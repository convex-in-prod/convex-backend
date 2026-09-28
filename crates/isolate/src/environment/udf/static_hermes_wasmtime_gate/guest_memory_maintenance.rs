use std::ops::Range;

use anyhow::Context as _;
use wasmtime::{
    Instance,
    Memory,
    Store,
    TypedFunc,
};

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use super::initialized_memory::ReclaimMode;
use super::wasmtime_anyhow;

const EXPORT: &str = "convex_wasm_udf_memory_maintenance";
const HEADER_BYTES: usize = 24;
const MAX_RANGES: usize = 128;
const RECORD_BYTES: usize = HEADER_BYTES + MAX_RANGES * 8;
const MAX_VISITED: u32 = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MaintenanceOperation {
    PrepareImage,
    FreeAllocatorPayload { page_size: u32 },
    ReclaimYoung,
    FreeDynamicPayload { page_size: u32 },
    EmptyYoungPayload,
    CollectForPressure { occupied_bytes: u32 },
    TrimWeakSlots,
}

impl MaintenanceOperation {
    fn arguments(self) -> (i32, i32) {
        match self {
            Self::PrepareImage => (0, 0),
            Self::FreeAllocatorPayload { page_size } => (1, page_size as i32),
            Self::ReclaimYoung => (2, 0),
            Self::FreeDynamicPayload { page_size } => (3, page_size as i32),
            Self::EmptyYoungPayload => (4, 0),
            Self::CollectForPressure { occupied_bytes } => (5, occupied_bytes as i32),
            Self::TrimWeakSlots => (6, 0),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PressureCollection {
    Unnecessary,
    Young,
    Full,
}

#[derive(Debug)]
pub(super) enum MaintenanceResult {
    Complete(Vec<Range<usize>>),
    Partial(Vec<Range<usize>>),
    Deferred,
    Unsupported,
    // Collection reports progress, never payload authority. The caller must
    // export fresh ranges after any operation that can run finalizers.
    Collection(PressureCollection),
    WeakSlotsTrimmed,
}

/// Optional guest support, discovered from the authenticated base instance.
/// Range authority is temporary: the owning lifecycle must retain exclusive
/// Store access and avoid allocation/collection until discard is complete.
pub(super) struct GuestMemoryMaintenance {
    call: TypedFunc<(i32, i32), i32>,
    pub(super) version: Option<u32>,
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) enum ReclamationOutcome {
    Retained {
        reclaimed_bytes: usize,
        collection: PressureCollection,
    },
    Deferred,
    Unsupported,
    InsufficientPayload {
        reclaimed_bytes: usize,
        collection: PressureCollection,
    },
}

impl GuestMemoryMaintenance {
    pub(super) fn from_instance<T>(
        store: &mut Store<T>,
        instance: Instance,
    ) -> anyhow::Result<Option<Self>> {
        let Some(export) = instance.get_export(&mut *store, EXPORT) else {
            return Ok(None);
        };
        let function = export
            .into_func()
            .context("guest memory maintenance export is not a function")?;
        let call = function
            .typed::<(i32, i32), i32>(&*store)
            .map_err(wasmtime_anyhow)?;
        Ok(Some(Self {
            call,
            version: None,
        }))
    }

    /// Uses the caller's existing fuel/deadline and memory admission. The
    /// caller must poison/dispose the Store on any error, including
    /// malformed output.
    pub(super) async fn run<T: Send>(
        &mut self,
        store: &mut Store<T>,
        memory: Memory,
        operation: MaintenanceOperation,
    ) -> anyhow::Result<MaintenanceResult> {
        if let MaintenanceOperation::FreeAllocatorPayload { page_size }
        | MaintenanceOperation::FreeDynamicPayload { page_size } = operation
        {
            anyhow::ensure!(
                (4096..=65536).contains(&page_size) && page_size.is_power_of_two(),
                "unsupported guest reclamation page size"
            );
        }
        if operation.arguments().0 > 2 {
            anyhow::ensure!(
                self.version == Some(3),
                "guest does not support lifetime maintenance"
            );
        }
        if let MaintenanceOperation::CollectForPressure { occupied_bytes } = operation {
            anyhow::ensure!(occupied_bytes > 0, "invalid collection pressure threshold");
        }
        let pointer = self
            .call
            .call_async(&mut *store, operation.arguments())
            .await
            .map_err(wasmtime_anyhow)? as u32 as usize;
        // A fixed-size copy bounds decoding and ends the borrowed guest slice
        // before any host mapping operation can discard or replace pages.
        let mut record = [0u8; RECORD_BYTES];
        memory
            .read(&*store, pointer, &mut record)
            .context("guest memory maintenance result lies outside memory")?;
        let result = decode_result(&record, operation, memory.data_size(&*store))?;
        let version = u32::from_le_bytes(record[..4].try_into().unwrap());
        anyhow::ensure!(
            self.version.is_none_or(|known| known == version),
            "guest memory maintenance version changed"
        );
        self.version = Some(version);
        Ok(result)
    }

    /// Maintain allocation lifetimes at a successful, admitted reuse boundary.
    /// `required_target` is aggregate pressure; the lower release target below
    /// is an amortized reuse policy, not an active-memory cap or admission
    /// credit. The caller owns this exact Store/recorder exclusively with
    /// both write hooks installed, live finalizer resources and the
    /// existing call budget.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub(super) async unsafe fn reclaim_for_reuse<T: Send>(
        &mut self,
        store: &mut Store<T>,
        memory: Memory,
        pages: &super::initialized_memory::PrivatePages,
        required_target: Option<usize>,
    ) -> anyhow::Result<ReclamationOutcome> {
        // Restored Stores have not called preparation themselves. Discover the
        // authenticated guest's protocol with an operation old guests support;
        // never probe an unknown operation and recover from its trap.
        if self.version.is_none() {
            self.run(
                store,
                memory,
                MaintenanceOperation::FreeAllocatorPayload { page_size: 4096 },
            )
            .await?;
        }
        if self.version != Some(3) {
            return match required_target {
                Some(target) => unsafe {
                    self.reclaim_legacy_for_reuse(store, memory, pages, target)
                        .await
                },
                None => Ok(ReclamationOutcome::Retained {
                    reclaimed_bytes: 0,
                    collection: PressureCollection::Unnecessary,
                }),
            };
        }

        const COLLECTION_BYTES: u32 = 2 * 1024 * 1024;
        const RELEASE_HIGH: usize = 27 * 1024 * 1024 / 4;
        const RELEASE_TARGET: usize = 25 * 1024 * 1024 / 4;
        anyhow::ensure!(
            !pages.observation().poisoned,
            "cannot maintain a poisoned initialized instance"
        );
        // The common path is a constant-time occupied-byte check in the guest.
        // Collection is driven by allocation pressure, not by request count.
        let MaintenanceResult::Collection(mut collection) = self
            .run(
                store,
                memory,
                MaintenanceOperation::CollectForPressure {
                    occupied_bytes: COLLECTION_BYTES,
                },
            )
            .await?
        else {
            anyhow::bail!("pressure collection returned payload authority");
        };
        let exposure = pages.observation().exposed_bytes;
        // Collection can free payload inside unchanged high-water backing.
        // Require allocation-pressure progress, not increased exposure, before
        // retrying release after changing request demand.
        if required_target.is_none()
            && (collection == PressureCollection::Unnecessary || exposure <= RELEASE_HIGH)
        {
            return Ok(ReclamationOutcome::Retained {
                reclaimed_bytes: 0,
                collection,
            });
        }
        let target = required_target.map_or(RELEASE_TARGET, |target| target.min(RELEASE_TARGET));
        let mut ranges = self
            .free_lifetime_payload(store, memory, collection)
            .await?;
        if let Some(reclaimed_bytes) =
            unsafe { pages.reclaim(&ranges, target, ReclaimMode::RequireTarget)? }
        {
            return Ok(ReclamationOutcome::Retained {
                reclaimed_bytes,
                collection,
            });
        }
        if required_target.is_some() && collection != PressureCollection::Full {
            // A proactive target can be below the live floor. Only actual
            // admission pressure justifies a lower-threshold collection retry;
            // otherwise release proven-free payload and charge the remainder.
            // Keep the retry bounded and retain this invocation's fuel/deadline.
            let MaintenanceResult::Collection(second) = self
                .run(
                    store,
                    memory,
                    MaintenanceOperation::CollectForPressure {
                        occupied_bytes: 512 * 1024,
                    },
                )
                .await?
            else {
                anyhow::bail!("pressure collection returned payload authority");
            };
            if second != PressureCollection::Unnecessary {
                collection = second;
                // All previous ranges expired during collection/finalization.
                ranges = self
                    .free_lifetime_payload(store, memory, collection)
                    .await?;
            }
        }
        let reclaimed_bytes =
            unsafe { pages.reclaim(&ranges, target, ReclaimMode::AvailablePayload)? }
                .context("partial reclamation did not report released bytes")?;
        if required_target.is_some_and(|target| pages.observation().exposed_bytes > target) {
            return Ok(ReclamationOutcome::InsufficientPayload {
                reclaimed_bytes,
                collection,
            });
        }
        // A live floor need not cause disposal under the proactive policy.
        // Ordinary ready-pool admission still charges all retained backing.
        Ok(ReclamationOutcome::Retained {
            reclaimed_bytes,
            collection,
        })
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    async fn free_lifetime_payload<T: Send>(
        &mut self,
        store: &mut Store<T>,
        memory: Memory,
        collection: PressureCollection,
    ) -> anyhow::Result<Vec<Range<usize>>> {
        if collection == PressureCollection::Full {
            match self
                .run(store, memory, MaintenanceOperation::TrimWeakSlots)
                .await?
            {
                MaintenanceResult::WeakSlotsTrimmed | MaintenanceResult::Deferred => {},
                MaintenanceResult::Unsupported => {
                    anyhow::bail!("prepared guest lacks weak-slot maintenance")
                },
                MaintenanceResult::Complete(_)
                | MaintenanceResult::Partial(_)
                | MaintenanceResult::Collection(_) => {
                    anyhow::bail!("invalid weak-slot maintenance result")
                },
            }
        }
        let mut ranges = Vec::new();
        // These exports do not allocate or advance collection. Their authorities
        // can coexist; trimming weak slots or running any collection cannot be
        // inserted between them and the host's discard.
        for operation in [
            MaintenanceOperation::FreeAllocatorPayload { page_size: 4096 },
            MaintenanceOperation::FreeDynamicPayload { page_size: 4096 },
        ] {
            match self.run(store, memory, operation).await? {
                MaintenanceResult::Complete(payload) | MaintenanceResult::Partial(payload) => {
                    ranges.extend(payload)
                },
                MaintenanceResult::Deferred | MaintenanceResult::Unsupported => {},
                MaintenanceResult::Collection(_) | MaintenanceResult::WeakSlotsTrimmed => {
                    anyhow::bail!("free-payload export returned collection progress")
                },
            }
        }
        let nursery = match self
            .run(store, memory, MaintenanceOperation::EmptyYoungPayload)
            .await?
        {
            MaintenanceResult::Complete(mut payload) => Some(payload.remove(0)),
            MaintenanceResult::Deferred | MaintenanceResult::Unsupported => None,
            MaintenanceResult::Partial(_)
            | MaintenanceResult::Collection(_)
            | MaintenanceResult::WeakSlotsTrimmed => anyhow::bail!("invalid empty-nursery result"),
        };
        // Validate complete ownership before retaining a nursery prefix. An
        // overlap inside that prefix is still contradictory guest evidence.
        if let Some(young) = &nursery {
            anyhow::ensure!(
                ranges
                    .iter()
                    .all(|range| range.end <= young.start || young.end <= range.start),
                "free payload overlaps the nursery"
            );
        }
        if let Some(mut young) = nursery {
            // Keep reusable nursery backing; only the tail of an already empty
            // nursery is disposable. This grants no authority over live cells.
            young.start = young
                .start
                .saturating_add(5 * 1024 * 1024 / 2)
                .min(young.end);
            if young.start < young.end {
                ranges.push(young);
            }
        }
        ranges.sort_unstable_by_key(|range| range.start);
        anyhow::ensure!(
            ranges.windows(2).all(|pair| pair[0].end <= pair[1].start),
            "allocator and collector free payload overlap"
        );
        Ok(ranges)
    }

    /// Reclaim only at a successful, admitted reuse boundary. The lifecycle
    /// keeps finalizer resources and the active memory/CPU allowance alive for
    /// this whole sequence. Any error or cancellation requires disposal; an
    /// expected inability to reclaim requires restoration or disposal, never a
    /// retry loop that prevents ordinary old-generation collector progress.
    /// The caller must prove that `pages` records this `memory` in this Store,
    /// with both write hooks installed and no outstanding guest-memory borrows.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    async unsafe fn reclaim_legacy_for_reuse<T: Send>(
        &mut self,
        store: &mut Store<T>,
        memory: Memory,
        pages: &super::initialized_memory::PrivatePages,
        retain_bytes: usize,
    ) -> anyhow::Result<ReclamationOutcome> {
        let before = pages.observation();
        anyhow::ensure!(
            !before.poisoned,
            "cannot reclaim a poisoned initialized instance"
        );
        if before.exposed_bytes <= retain_bytes {
            return Ok(ReclamationOutcome::Retained {
                reclaimed_bytes: 0,
                collection: PressureCollection::Unnecessary,
            });
        }
        let operation = MaintenanceOperation::FreeAllocatorPayload { page_size: 4096 };
        let native = self.run(store, memory, operation).await?;
        let ranges = match native {
            MaintenanceResult::Complete(ranges) | MaintenanceResult::Partial(ranges) => ranges,
            MaintenanceResult::Deferred
            | MaintenanceResult::Unsupported
            | MaintenanceResult::Collection(_)
            | MaintenanceResult::WeakSlotsTrimmed => {
                anyhow::bail!("allocator enumeration returned a collection status");
            },
        };
        // No guest call occurs between enumeration and discard. Partial
        // enumeration grants authority for exactly the returned ranges.
        if let Some(reclaimed_bytes) =
            unsafe { pages.reclaim(&ranges, retain_bytes, ReclaimMode::RequireTarget)? }
        {
            return Ok(ReclamationOutcome::Retained {
                reclaimed_bytes,
                collection: PressureCollection::Unnecessary,
            });
        }
        let nursery = match self
            .run(store, memory, MaintenanceOperation::ReclaimYoung)
            .await?
        {
            MaintenanceResult::Complete(ranges) => ranges,
            MaintenanceResult::Deferred => return Ok(ReclamationOutcome::Deferred),
            MaintenanceResult::Unsupported => return Ok(ReclamationOutcome::Unsupported),
            MaintenanceResult::Partial(_)
            | MaintenanceResult::Collection(_)
            | MaintenanceResult::WeakSlotsTrimmed => {
                anyhow::bail!("invalid nursery reclamation result")
            },
        };
        // Collection/finalizers invalidate the earlier native ranges. This
        // allocation-free probe does not invalidate the newly emptied nursery.
        let mut ranges = match self.run(store, memory, operation).await? {
            MaintenanceResult::Complete(ranges) | MaintenanceResult::Partial(ranges) => ranges,
            MaintenanceResult::Deferred
            | MaintenanceResult::Unsupported
            | MaintenanceResult::Collection(_)
            | MaintenanceResult::WeakSlotsTrimmed => {
                anyhow::bail!("allocator enumeration returned a collection status");
            },
        };
        // Validate both authorities before either can change the mapping. Even
        // if native payload alone meets the bound, an alias with the allocated
        // nursery means the guest returned contradictory ownership evidence.
        anyhow::ensure!(
            ranges.iter().all(|native| nursery
                .iter()
                .all(|young| { native.end <= young.start || young.end <= native.start })),
            "allocator reclamation overlaps the nursery"
        );
        if let Some(reclaimed_bytes) =
            unsafe { pages.reclaim(&ranges, retain_bytes, ReclaimMode::RequireTarget)? }
        {
            return Ok(ReclamationOutcome::Retained {
                reclaimed_bytes,
                collection: PressureCollection::Young,
            });
        }
        // Native-free payload and the allocated nursery must be disjoint. Their
        // union can meet the bound even when neither region alone is sufficient.
        // The recorder excludes partial boundary pages and touches no heap data.
        ranges.extend(nursery);
        ranges.sort_unstable_by_key(|range| range.start);
        match unsafe { pages.reclaim(&ranges, retain_bytes, ReclaimMode::RequireTarget)? } {
            Some(reclaimed_bytes) => Ok(ReclamationOutcome::Retained {
                reclaimed_bytes,
                collection: PressureCollection::Young,
            }),
            None => Ok(ReclamationOutcome::InsufficientPayload {
                reclaimed_bytes: 0,
                collection: PressureCollection::Young,
            }),
        }
    }
}

fn decode_result(
    record: &[u8; RECORD_BYTES],
    operation: MaintenanceOperation,
    memory_bytes: usize,
) -> anyhow::Result<MaintenanceResult> {
    let word = |offset: usize| u32::from_le_bytes(record[offset..offset + 4].try_into().unwrap());
    let version = word(0);
    anyhow::ensure!(
        matches!(version, 1..=3) && word(4) as usize == RECORD_BYTES,
        "unsupported guest memory maintenance result layout"
    );
    anyhow::ensure!(
        word(8) == operation.arguments().0 as u32,
        "guest memory maintenance result has a different operation"
    );
    anyhow::ensure!(
        operation.arguments().0 <= 2 || version == 3,
        "lifetime maintenance requires version 3"
    );
    let status = word(12);
    let count = word(16) as usize;
    let visited = word(20);
    anyhow::ensure!(
        count <= MAX_RANGES && visited <= MAX_VISITED,
        "guest memory maintenance result exceeds enumeration bounds"
    );
    match operation {
        MaintenanceOperation::FreeAllocatorPayload { .. } => {
            anyhow::ensure!(status <= 1, "invalid allocator reclamation status");
            anyhow::ensure!(
                count <= visited as usize,
                "allocator ranges exceed visited blocks"
            );
        },
        MaintenanceOperation::PrepareImage if version >= 2 => {
            // Image certification preserves the entire heap. It does not
            // authorize discarding nursery pages, which may still be live.
            anyhow::ensure!(
                status == 0 && count == 0 && visited == 0,
                "invalid image preparation result"
            );
        },
        MaintenanceOperation::FreeDynamicPayload { .. } => {
            anyhow::ensure!(
                status <= 3 && count <= visited as usize,
                "invalid dynamic free-payload result"
            );
            if status >= 2 {
                anyhow::ensure!(
                    count == 0 && visited == 0,
                    "deferred dynamic payload grants authority"
                );
            }
        },
        MaintenanceOperation::CollectForPressure { .. } => {
            anyhow::ensure!(
                count == 0 && visited == 0,
                "pressure collection grants payload authority"
            );
            return Ok(MaintenanceResult::Collection(match status {
                0 => PressureCollection::Unnecessary,
                4 => PressureCollection::Young,
                5 => PressureCollection::Full,
                _ => anyhow::bail!("invalid pressure collection status"),
            }));
        },
        MaintenanceOperation::TrimWeakSlots => {
            anyhow::ensure!(
                count == 0 && visited == 0,
                "weak-slot maintenance grants payload authority"
            );
            return match status {
                0 => Ok(MaintenanceResult::WeakSlotsTrimmed),
                2 => Ok(MaintenanceResult::Deferred),
                3 => Ok(MaintenanceResult::Unsupported),
                _ => anyhow::bail!("invalid weak-slot maintenance status"),
            };
        },
        MaintenanceOperation::PrepareImage
        | MaintenanceOperation::ReclaimYoung
        | MaintenanceOperation::EmptyYoungPayload => {
            anyhow::ensure!(visited == 0, "nursery result contains allocator visits");
            match status {
                0 => anyhow::ensure!(count == 1, "empty successful nursery result"),
                2 | 3 => anyhow::ensure!(count == 0, "deferred nursery result grants ranges"),
                _ => anyhow::bail!("invalid nursery reclamation status"),
            }
        },
    }
    let mut ranges = Vec::with_capacity(count);
    for index in 0..count {
        let start = word(HEADER_BYTES + index * 8) as usize;
        let end = word(HEADER_BYTES + index * 8 + 4) as usize;
        anyhow::ensure!(
            start < end && end <= memory_bytes,
            "guest memory reclamation range lies outside memory"
        );
        if let MaintenanceOperation::FreeAllocatorPayload { page_size }
        | MaintenanceOperation::FreeDynamicPayload { page_size } = operation
        {
            anyhow::ensure!(
                start % page_size as usize == 0 && end % page_size as usize == 0,
                "allocator reclamation range is not page aligned"
            );
        }
        ranges.push(start..end);
    }
    // Allocator traversal order is not address order. Sorting at most 128 ranges
    // rejects aliases and lets the recorder intersect them with private backing
    // without a heap walk or any admission-lock work.
    ranges.sort_unstable_by_key(|range| range.start);
    anyhow::ensure!(
        ranges.windows(2).all(|pair| pair[0].end <= pair[1].start),
        "guest memory reclamation ranges overlap"
    );
    match status {
        0 => Ok(MaintenanceResult::Complete(ranges)),
        1 => Ok(MaintenanceResult::Partial(ranges)),
        2 => Ok(MaintenanceResult::Deferred),
        3 => Ok(MaintenanceResult::Unsupported),
        _ => unreachable!("validated maintenance status changed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        operation: MaintenanceOperation,
        status: u32,
        visited: u32,
        ranges: &[(u32, u32)],
    ) -> [u8; RECORD_BYTES] {
        let mut record = [0; RECORD_BYTES];
        let mut words = vec![
            1,
            RECORD_BYTES as u32,
            operation.arguments().0 as u32,
            status,
            ranges.len() as u32,
            visited,
        ];
        words.extend(ranges.iter().flat_map(|&(start, end)| [start, end]));
        for (index, word) in words.into_iter().enumerate() {
            record[index * 4..index * 4 + 4].copy_from_slice(&word.to_le_bytes());
        }
        record
    }

    #[test]
    fn partial_allocator_output_keeps_only_bounded_nonoverlapping_authority() -> anyhow::Result<()>
    {
        let operation = MaintenanceOperation::FreeAllocatorPayload { page_size: 4096 };
        let record = record(operation, 1, 4, &[(12288, 16384), (4096, 8192)]);
        let MaintenanceResult::Partial(ranges) = decode_result(&record, operation, 65536)? else {
            anyhow::bail!("partial allocator output lost its status");
        };
        assert_eq!(ranges, vec![4096..8192, 12288..16384]);
        assert!(decode_result(&record, MaintenanceOperation::ReclaimYoung, 65536).is_err());
        for ranges in [
            &[(4096, 12288), (8192, 16384)][..],
            &[(4097, 8192)],
            &[(4096, 69632)],
            &[(8192, 4096)],
        ] {
            assert!(
                decode_result(&self::record(operation, 0, 4, ranges), operation, 65536).is_err()
            );
        }
        assert!(decode_result(
            &self::record(operation, 0, 0, &[(4096, 8192)]),
            operation,
            65536
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn deferred_collection_cannot_grant_discard_authority() -> anyhow::Result<()> {
        for operation in [
            MaintenanceOperation::PrepareImage,
            MaintenanceOperation::ReclaimYoung,
        ] {
            assert!(matches!(
                decode_result(&record(operation, 2, 0, &[]), operation, 65536)?,
                MaintenanceResult::Deferred
            ));
            assert!(
                decode_result(&record(operation, 2, 0, &[(4096, 8192)]), operation, 65536).is_err()
            );
            assert!(decode_result(&record(operation, 0, 0, &[]), operation, 65536).is_err());
            assert!(
                decode_result(&record(operation, 0, 1, &[(4096, 8192)]), operation, 65536).is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn lifetime_protocol_separates_collection_from_discard_authority() -> anyhow::Result<()> {
        let pressure = MaintenanceOperation::CollectForPressure {
            occupied_bytes: 1024,
        };
        for (status, expected) in [
            (0, PressureCollection::Unnecessary),
            (4, PressureCollection::Young),
            (5, PressureCollection::Full),
        ] {
            let mut output = record(pressure, status, 0, &[]);
            assert!(decode_result(&output, pressure, 65536).is_err());
            output[..4].copy_from_slice(&3u32.to_le_bytes());
            assert!(matches!(decode_result(&output, pressure, 65536)?,
                MaintenanceResult::Collection(actual) if actual == expected));
            output[16..20].copy_from_slice(&1u32.to_le_bytes());
            assert!(decode_result(&output, pressure, 65536).is_err());
        }
        for operation in [
            MaintenanceOperation::FreeDynamicPayload { page_size: 4096 },
            MaintenanceOperation::TrimWeakSlots,
        ] {
            let mut output = record(operation, 2, 0, &[]);
            output[..4].copy_from_slice(&3u32.to_le_bytes());
            assert!(matches!(
                decode_result(&output, operation, 65536)?,
                MaintenanceResult::Deferred
            ));
            output[20..24].copy_from_slice(&1u32.to_le_bytes());
            assert!(decode_result(&output, operation, 65536).is_err());
        }
        let operation = MaintenanceOperation::FreeDynamicPayload { page_size: 4096 };
        let mut output = record(operation, 1, 2, &[(8192, 12288), (4096, 8192)]);
        output[..4].copy_from_slice(&3u32.to_le_bytes());
        assert!(matches!(decode_result(&output, operation, 65536)?,
            MaintenanceResult::Partial(ranges) if ranges == vec![4096..8192, 8192..12288]));
        output[24..28].copy_from_slice(&4096u32.to_le_bytes());
        assert!(decode_result(&output, operation, 65536).is_err());
        Ok(())
    }

    #[test]
    fn result_statuses_are_scoped_to_their_version_and_operation() {
        for version in 1u32..=3 {
            for operation in [
                MaintenanceOperation::PrepareImage,
                MaintenanceOperation::FreeAllocatorPayload { page_size: 4096 },
                MaintenanceOperation::ReclaimYoung,
                MaintenanceOperation::FreeDynamicPayload { page_size: 4096 },
                MaintenanceOperation::EmptyYoungPayload,
                MaintenanceOperation::CollectForPressure {
                    occupied_bytes: u32::MAX,
                },
                MaintenanceOperation::TrimWeakSlots,
            ] {
                for status in 0..=6 {
                    let (valid, ranges, visited) = match operation {
                        MaintenanceOperation::PrepareImage if version >= 2 => {
                            (status == 0, &[][..], 0)
                        },
                        MaintenanceOperation::PrepareImage
                        | MaintenanceOperation::ReclaimYoung
                        | MaintenanceOperation::EmptyYoungPayload => (
                            matches!(status, 0 | 2 | 3),
                            if status == 0 {
                                &[(4096, 8192)][..]
                            } else {
                                &[][..]
                            },
                            0,
                        ),
                        MaintenanceOperation::FreeAllocatorPayload { .. } => {
                            (status <= 1, &[][..], MAX_VISITED)
                        },
                        MaintenanceOperation::FreeDynamicPayload { .. } => {
                            (status <= 3, &[][..], 0)
                        },
                        MaintenanceOperation::CollectForPressure { .. } => {
                            (matches!(status, 0 | 4 | 5), &[][..], 0)
                        },
                        MaintenanceOperation::TrimWeakSlots => {
                            (matches!(status, 0 | 2 | 3), &[][..], 0)
                        },
                    };
                    let mut output = record(operation, status, visited, ranges);
                    output[..4].copy_from_slice(&version.to_le_bytes());
                    // Unused slots must neither add authority nor reject an
                    // otherwise valid partial record left by an earlier call.
                    output[HEADER_BYTES + ranges.len() * 8..].fill(0xff);
                    assert_eq!(
                        decode_result(&output, operation, 65536).is_ok(),
                        valid && (version == 3 || operation.arguments().0 <= 2),
                        "version {version}, operation {operation:?}, status {status}",
                    );
                }
            }
        }
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    #[test]
    fn pressure_collection_reexports_free_payload_before_discard() -> anyhow::Result<()> {
        use std::{
            future::Future as _,
            task::{
                Context,
                Poll,
                Waker,
            },
        };

        use super::super::initialized_memory::InitializedMemoryImage;
        let mut config = wasmtime::Config::new();
        config.memory_reservation(1u64 << 32);
        let engine = wasmtime::Engine::new(&config).map_err(wasmtime_anyhow)?;
        let module = wasmtime::Module::new(
            &engine,
            r#"
            (module
                (memory (export "memory") 1 1)
                (global $phase (mut i32) (i32.const 0))
                (func (export "convex_wasm_udf_memory_maintenance")
                    (param $operation i32) (param i32) (result i32)
                    (i32.store (i32.const 0) (i32.const 3))
                    (i32.store (i32.const 4) (i32.const 1048))
                    (i32.store (i32.const 8) (local.get $operation))
                    (i32.store (i32.const 12) (i32.const 0))
                    (i32.store (i32.const 16) (i32.const 0))
                    (i32.store (i32.const 20) (i32.const 0))
                    (if (i32.eq (local.get $operation) (i32.const 5)) (then
                        (global.set $phase (i32.add (global.get $phase) (i32.const 1)))
                        (i32.store (i32.const 12) (i32.add (global.get $phase) (i32.const 3)))))
                    (if (i32.eq (local.get $operation) (i32.const 4)) (then
                        (i32.store (i32.const 12) (i32.const 2))))
                    (if (i32.eq (local.get $operation) (i32.const 3)) (then
                        (i32.store (i32.const 16) (i32.const 1))
                        (i32.store (i32.const 20) (i32.const 1))
                        (i32.store (i32.const 24) (i32.mul (global.get $phase) (i32.const 4096)))
                        (i32.store (i32.const 28) (i32.mul (global.get $phase) (i32.const 8192)))))
                    (i32.const 0)))"#,
        )
        .map_err(wasmtime_anyhow)?;
        let operation = async {
            let mut store = Store::new(&engine, ());
            let instance = Instance::new_async(&mut store, &module, &[])
                .await
                .map_err(wasmtime_anyhow)?;
            let memory = instance.get_memory(&mut store, "memory").unwrap();
            let mut maintenance =
                GuestMemoryMaintenance::from_instance(&mut store, instance)?.unwrap();
            let image = InitializedMemoryImage::capture(memory.data(&store))?;
            let pages = unsafe { image.attach(&mut store, memory, 65536, 65536, None)? };
            memory.write(&mut store, 0, &[7; 16384])?;
            let result = unsafe {
                maintenance
                    .reclaim_for_reuse(&mut store, memory, &pages, Some(8192))
                    .await?
            };
            assert!(matches!(
                result,
                ReclamationOutcome::Retained {
                    reclaimed_bytes: 8192,
                    collection: PressureCollection::Full
                }
            ));
            assert_eq!(pages.observation().exposed_bytes, 8192);
            // The second collection revoked the first free range. Reusing it
            // would silently reset live bytes in this page to the image.
            assert!(memory.data(&store)[4096..8192]
                .iter()
                .all(|&byte| byte == 7));
            assert!(memory.data(&store)[8192..16384]
                .iter()
                .all(|&byte| byte == 0));
            Ok(())
        };
        match std::pin::pin!(operation).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => result,
            Poll::Pending => anyhow::bail!("reclamation fixture unexpectedly suspended"),
        }
    }

    #[test]
    fn image_preparation_v2_does_not_grant_nursery_discard_authority() -> anyhow::Result<()> {
        let operation = MaintenanceOperation::PrepareImage;
        let mut prepared = record(operation, 0, 0, &[]);
        prepared[..4].copy_from_slice(&2u32.to_le_bytes());
        assert!(matches!(
            decode_result(&prepared, operation, 65536)?,
            MaintenanceResult::Complete(ranges) if ranges.is_empty()
        ));
        // Version 1 certifies only with an empty-nursery range; keep accepting
        // existing artifacts without interpreting Deferred as image authority.
        assert!(matches!(
            decode_result(&record(operation, 0, 0, &[(4096, 8192)]), operation, 65536)?,
            MaintenanceResult::Complete(ranges) if ranges == vec![4096..8192]
        ));
        for (status, visits, ranges) in [
            (0, 0, &[(4096, 8192)][..]),
            (0, 1, &[]),
            (2, 0, &[]),
            (3, 0, &[]),
        ] {
            let mut invalid = record(operation, status, visits, ranges);
            invalid[..4].copy_from_slice(&2u32.to_le_bytes());
            assert!(decode_result(&invalid, operation, 65536).is_err());
        }
        let mut nursery = record(MaintenanceOperation::ReclaimYoung, 0, 0, &[]);
        nursery[..4].copy_from_slice(&2u32.to_le_bytes());
        assert!(decode_result(&nursery, MaintenanceOperation::ReclaimYoung, 65536).is_err());
        Ok(())
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    #[test]
    fn collection_retries_release_without_new_private_exposure() -> anyhow::Result<()> {
        use std::{
            future::Future as _,
            task::{
                Context,
                Poll,
                Waker,
            },
        };

        use super::super::initialized_memory::InitializedMemoryImage;

        let mut config = wasmtime::Config::new();
        config.memory_reservation(1u64 << 32);
        let engine = wasmtime::Engine::new(&config).map_err(wasmtime_anyhow)?;
        let module = wasmtime::Module::new(
            &engine,
            r#"
            (module
                (memory (export "memory") 128 128)
                (global $collections (mut i32) (i32.const 0))
                (func (export "convex_wasm_udf_memory_maintenance")
                    (param $operation i32) (param i32) (result i32)
                    (i32.store (i32.const 0) (i32.const 3))
                    (i32.store (i32.const 4) (i32.const 1048))
                    (i32.store (i32.const 8) (local.get $operation))
                    (i32.store (i32.const 12) (i32.const 0))
                    (i32.store (i32.const 16) (i32.const 0))
                    (i32.store (i32.const 20) (i32.const 0))
                    (if (i32.eq (local.get $operation) (i32.const 5)) (then
                        (global.set $collections (i32.add (global.get $collections) (i32.const 1)))
                        (i32.store (i32.const 12) (i32.const 4))))
                    (if (i32.eq (local.get $operation) (i32.const 4)) (then
                        (i32.store (i32.const 12) (i32.const 2))))
                    ;; A later collection frees storage inside the same backing.
                    (if (i32.and
                        (i32.eq (local.get $operation) (i32.const 3))
                        (i32.ge_u (global.get $collections) (i32.const 2))) (then
                        (i32.store (i32.const 16) (i32.const 1))
                        (i32.store (i32.const 20) (i32.const 1))
                        (i32.store (i32.const 24) (i32.const 1048576))
                        (i32.store (i32.const 28) (i32.const 2097152))))
                    (i32.const 0)))"#,
        )
        .map_err(wasmtime_anyhow)?;
        let operation = async {
            let mut store = Store::new(&engine, ());
            let instance = Instance::new_async(&mut store, &module, &[])
                .await
                .map_err(wasmtime_anyhow)?;
            let memory = instance.get_memory(&mut store, "memory").unwrap();
            let mut maintenance =
                GuestMemoryMaintenance::from_instance(&mut store, instance)?.unwrap();
            let image = InitializedMemoryImage::capture(memory.data(&store))?;
            let pages = unsafe {
                image.attach(&mut store, memory, 8 * 1024 * 1024, 8 * 1024 * 1024, None)?
            };
            memory.write(&mut store, 0, &vec![7; 7 * 1024 * 1024])?;
            let first = unsafe {
                maintenance
                    .reclaim_for_reuse(&mut store, memory, &pages, None)
                    .await?
            };
            assert!(matches!(
                first,
                ReclamationOutcome::Retained {
                    reclaimed_bytes: 0,
                    ..
                }
            ));
            assert_eq!(pages.observation().exposed_bytes, 7 * 1024 * 1024);
            let second = unsafe {
                maintenance
                    .reclaim_for_reuse(&mut store, memory, &pages, None)
                    .await?
            };
            assert!(matches!(
                second,
                ReclamationOutcome::Retained {
                    reclaimed_bytes: 786432,
                    collection: PressureCollection::Young,
                }
            ));
            assert_eq!(pages.observation().exposed_bytes, 25 * 1024 * 1024 / 4);
            Ok(())
        };
        match std::pin::pin!(operation).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => result,
            Poll::Pending => anyhow::bail!("release retry fixture unexpectedly suspended"),
        }
    }

    #[test]
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    fn native_nursery_alias_is_rejected_before_native_discard() -> anyhow::Result<()> {
        use std::{
            future::Future,
            task::{
                Context,
                Poll,
                Waker,
            },
        };

        use super::super::initialized_memory::InitializedMemoryImage;

        let mut config = wasmtime::Config::new();
        config.memory_reservation(1u64 << 32);
        let engine = wasmtime::Engine::new(&config).map_err(wasmtime_anyhow)?;
        // The first native probe is empty. Collection and the second native
        // probe then contradict each other, but native payload alone would
        // meet the target. No mapping change may precede that validation.
        let module = wasmtime::Module::new(
            &engine,
            r#"
            (module
                (memory (export "memory") 1 1)
                (global $calls (mut i32) (i32.const 0))
                (func (export "convex_wasm_udf_memory_maintenance")
                    (param $operation i32) (param i32) (result i32)
                    (global.set $calls (i32.add (global.get $calls) (i32.const 1)))
                    (i32.store (i32.const 8) (local.get $operation))
                    (i32.store (i32.const 16) (i32.const 0))
                    (i32.store (i32.const 20) (i32.const 0))
                    (if (i32.gt_u (global.get $calls) (i32.const 1)) (then
                        (i32.store (i32.const 16) (i32.const 1))
                        (i32.store (i32.const 24) (i32.const 4096))
                        (i32.store (i32.const 28) (i32.const 12288))
                        (if (i32.eq (local.get $operation) (i32.const 1)) (then
                            (i32.store (i32.const 20) (i32.const 1))))))
                    (i32.const 0)))
        "#,
        )
        .map_err(wasmtime_anyhow)?;
        let operation = async {
            let mut store = Store::new(&engine, ());
            let instance = Instance::new_async(&mut store, &module, &[])
                .await
                .map_err(wasmtime_anyhow)?;
            let memory = instance.get_memory(&mut store, "memory").unwrap();
            let mut maintenance =
                GuestMemoryMaintenance::from_instance(&mut store, instance)?.unwrap();
            // Preparation already established the protocol for this fixture;
            // keep its probe sequence focused on conflicting range authority.
            maintenance.version = Some(1);
            let image = InitializedMemoryImage::capture(memory.data(&store))?;
            let pages = unsafe { image.attach(&mut store, memory, 65536, 65536, None)? };
            memory.write(
                &mut store,
                0,
                &record(
                    MaintenanceOperation::FreeAllocatorPayload { page_size: 4096 },
                    0,
                    0,
                    &[],
                ),
            )?;
            memory.write(&mut store, 4096, &[7; 8192])?;
            let before = pages.observation().exposed_bytes;
            assert_eq!(before, 12288);
            let result = unsafe {
                maintenance
                    .reclaim_for_reuse(&mut store, memory, &pages, Some(4096))
                    .await
            };
            assert_eq!(
                result
                    .err()
                    .context("aliased payload was retained")?
                    .to_string(),
                "allocator reclamation overlaps the nursery"
            );
            assert_eq!(pages.observation().exposed_bytes, before);
            assert!(memory.data(&store)[4096..12288]
                .iter()
                .all(|&byte| byte == 7));
            Ok(())
        };
        match std::pin::pin!(operation).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => result,
            Poll::Pending => anyhow::bail!("reclamation fixture unexpectedly suspended"),
        }
    }
}
