//! Initialized application images on stable Wasmtime mappings. This module
//! owns private-page exposure, not JavaScript reuse authority or Store
//! snapshots. Callers must separately reconstruct tables/globals and validate
//! initialization dependencies before installing an image in an exclusively
//! owned idle Store.

use std::{
    fs::File,
    ops::Range,
    os::{
        fd::{
            AsRawFd,
            FromRawFd,
        },
        unix::fs::FileExt,
    },
    ptr,
    sync::{
        atomic::{
            AtomicBool,
            AtomicU32,
            AtomicU64,
            AtomicUsize,
            Ordering,
        },
        Arc,
    },
};

use wasmtime::{
    unix::StoreExt as _,
    Memory,
    Store,
};

use super::super::generated_wasm_memory::InitializedMemoryLoan;

const PAGE_BYTES: usize = 4096;
const RESERVATION_BYTES: usize = 1usize << 32;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum ReclaimMode {
    RequireTarget,
    AvailablePayload,
}

pub(super) struct InitializedMemoryImage {
    file: File,
    source: usize,
    pub(super) len: usize,
    /// Nonzero pages written at capture. Shared-image admission must reserve
    /// capacity before capture: reading sparse holes can instantiate file
    /// pages.
    pub(super) stored_bytes: usize,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ImageCaptureError {
    #[error("initialized image unavailable while {operation}: {source}")]
    Unavailable {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

impl ImageCaptureError {
    fn from_io(operation: &'static str, source: std::io::Error) -> Self {
        // Capture has not touched the Store. Explicit kernel capability and
        // resource limits can retain ordinary preparation; bad arguments,
        // invalid descriptors and unexpected I/O errors must still fail.
        match source.raw_os_error() {
            Some(
                libc::ENOSYS
                | libc::EOPNOTSUPP
                | libc::EPERM
                | libc::EACCES
                | libc::EMFILE
                | libc::ENFILE
                | libc::ENOMEM
                | libc::ENOSPC
                | libc::EDQUOT
                | libc::EFBIG,
            ) => Self::Unavailable { operation, source },
            _ => Self::Failed(anyhow::Error::new(source).context(operation)),
        }
    }
}

impl InitializedMemoryImage {
    /// Install only in an exclusively owned idle Store using Wasmtime's default
    /// on-demand memory allocator. Pooling and custom allocators have different
    /// mapping/reset rules and are not supported here. This must be the Store's
    /// only protected memory: attachment replaces all three Store hooks. The
    /// caller reconstructs non-linear state first, reserves the initial private
    /// allowance and maximum recorder metadata before attachment, retains the
    /// whole image-capacity charge until its last mapping owner is gone,
    /// and disposes on any error. No guest call or borrowed memory slice
    /// may overlap attachment.
    ///
    /// The growth hook identifies this linear memory by its base address; the
    /// internal GC heap does not invoke it. The hook reserves the whole newly
    /// writable tail before Wasmtime exposes it. Any subsequent growth failure
    /// must terminate the invocation and dispose the slot; a successful
    /// callback is not evidence that OS growth completed.
    pub(super) unsafe fn attach<T>(
        &self,
        store: &mut Store<T>,
        memory: Memory,
        maximum_private_bytes: usize,
        initial_private_bytes: usize,
        overflow: Option<InitializedMemoryLoan>,
    ) -> anyhow::Result<Arc<PrivatePages>> {
        let engine = store.engine();
        anyhow::ensure!(
            engine.get_memory_reservation() >= RESERVATION_BYTES as u64
                && engine.get_signals_based_traps()
                && !engine.is_pulley(),
            "initialized image requires native signals and a full wasm32 reservation"
        );
        let ty = memory.ty(&*store);
        anyhow::ensure!(
            unsafe { libc::sysconf(libc::_SC_PAGESIZE) } == PAGE_BYTES as i64
                && !ty.is_shared()
                && !ty.is_64()
                && ty.page_size() == 65536,
            "initialized image requires unshared wasm32 memory and 4 KiB host pages"
        );
        let maximum = ty.maximum().unwrap_or(65536) as usize * 65536;
        let address = memory.data_ptr(&*store);
        let len = memory.data_size(&*store);
        let mut pages = PrivatePages::new(address, len, maximum, maximum_private_bytes)?;
        pages.overflow = overflow;
        let pages = Arc::new(pages);
        // Capacity bounds recorder storage; only the admitted initial grant is
        // writable. Later invocations can reuse the storage with a new grant.
        unsafe { pages.begin_invocation(initial_private_bytes)? };
        // Prepare recorder storage before replacing bytes, then install the
        // image read-only so no unrecorded writable pages are exposed.
        unsafe { self.install(address, len)? };
        let signal_pages = Arc::clone(&pages);
        let native_pages = Arc::clone(&pages);
        let growth_pages = Arc::clone(&pages);
        unsafe {
            store
                .set_signal_handler_with_trap(move |signal, info, context| {
                    signal_pages.handle_fault(signal, info, context)
                })
                .map_err(super::wasmtime_anyhow)?;
            store.set_memory_write_handler(move |address, length| {
                native_pages.permit(address, length)
            });
            store.set_memory_grow_handler(move |address, current, desired| {
                growth_pages.permit_growth(address, current, desired)
            });
        }
        Ok(pages)
    }

    /// The caller reserves `bytes.len()` of shared-image capacity before this
    /// copy, even for zero pages omitted from the sparse file. This captures
    /// linear bytes only and grants no authority to reuse initialized state.
    pub(super) fn capture(bytes: &[u8]) -> Result<Self, ImageCaptureError> {
        if bytes.is_empty() || bytes.len() % PAGE_BYTES != 0 {
            return Err(ImageCaptureError::Failed(anyhow::anyhow!(
                "unaligned initialized image"
            )));
        }
        let fd = unsafe {
            libc::memfd_create(
                c"initialized-wasm".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        if fd < 0 {
            return Err(ImageCaptureError::from_io(
                "creating initialized image",
                std::io::Error::last_os_error(),
            ));
        }
        let file = unsafe { File::from_raw_fd(fd) };
        file.set_len(bytes.len() as u64)
            .map_err(|error| ImageCaptureError::from_io("sizing initialized image", error))?;
        let mut stored_bytes = 0;
        for (index, page) in bytes.chunks_exact(PAGE_BYTES).enumerate() {
            if page.iter().any(|&byte| byte != 0) {
                file.write_all_at(page, (index * PAGE_BYTES) as u64)
                    .map_err(|error| {
                        ImageCaptureError::from_io("writing initialized image", error)
                    })?;
                stored_bytes += PAGE_BYTES;
            }
        }
        if unsafe {
            libc::fcntl(
                fd,
                libc::F_ADD_SEALS,
                libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE | libc::F_SEAL_SEAL,
            )
        } != 0
        {
            return Err(ImageCaptureError::from_io(
                "sealing initialized image",
                std::io::Error::last_os_error(),
            ));
        }
        let source = unsafe {
            libc::mmap(
                ptr::null_mut(),
                bytes.len(),
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if source == libc::MAP_FAILED {
            return Err(ImageCaptureError::from_io(
                "mapping initialized image",
                std::io::Error::last_os_error(),
            ));
        }
        Ok(Self {
            file,
            source: source as usize,
            len: bytes.len(),
            stored_bytes,
        })
    }

    /// The range must be an on-demand, nonmoving Wasmtime mapping, with no
    /// active guest call or borrowed memory slice. Store-owned
    /// tables/references and native globals must already be reconstructed
    /// for this exact image.
    unsafe fn install(&self, address: *mut u8, length: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            address as usize % PAGE_BYTES == 0 && length == self.len,
            "initialized image differs from destination length"
        );
        let result = unsafe {
            libc::mmap(
                address.cast(),
                self.len,
                libc::PROT_READ,
                libc::MAP_PRIVATE | libc::MAP_FIXED,
                self.file.as_raw_fd(),
                0,
            )
        };
        anyhow::ensure!(
            result == address.cast(),
            "install initialized image: {}",
            std::io::Error::last_os_error()
        );
        Ok(())
    }

    /// Restore all recorded writes, retaining a bounded set of writable pages
    /// to avoid repeated faults. This restores linear bytes only; the
    /// caller still owns native globals and host resources. Any error
    /// requires disposal; a partial restore leaves the recorder poisoned.
    /// `pages` must describe a live mapping of this exact image,
    /// exclusively owned with no active guest call or borrowed memory
    /// slice. The mapping's grown length is retained; pages beyond the
    /// image are restored to zeros.
    pub(super) unsafe fn restore(
        &self,
        pages: &PrivatePages,
        retain_bytes: usize,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !pages.poisoned.load(Ordering::Acquire)
                && self.len <= pages.len.load(Ordering::Acquire)
                && retain_bytes % PAGE_BYTES == 0,
            "invalid initialized image restore"
        );
        pages.poisoned.store(true, Ordering::Release);
        let count = pages.count.load(Ordering::Acquire);
        let keep = count.min(retain_bytes / PAGE_BYTES);
        for index in 0..keep {
            let offset = pages.pages[index].load(Ordering::Relaxed) as usize * PAGE_BYTES;
            unsafe {
                if offset < self.len {
                    ptr::copy_nonoverlapping(
                        (self.source + offset) as *const u8,
                        (pages.address + offset) as *mut u8,
                        PAGE_BYTES,
                    );
                } else {
                    ptr::write_bytes((pages.address + offset) as *mut u8, 0, PAGE_BYTES);
                }
            }
        }
        for index in keep..count {
            let page = pages.pages[index].load(Ordering::Relaxed) as usize;
            unsafe {
                pages.discard_page(page)?;
            }
        }
        pages.count.store(keep, Ordering::Release);
        pages.poisoned.store(false, Ordering::Release);
        Ok(())
    }
}

impl Drop for InitializedMemoryImage {
    fn drop(&mut self) {
        assert_eq!(unsafe { libc::munmap(self.source as *mut _, self.len) }, 0);
    }
}

/// Atomics permit synchronous signal-handler access; they do not authorize
/// concurrent use. The Store, mapping and all callbacks have one exclusive
/// owner. This recorder does not keep the mapping alive: the Store must remain
/// live for every callback, unsafe recorder operation and guest-memory borrow.
pub(super) struct PrivatePages {
    address: usize,
    len: AtomicUsize,
    maximum: usize,
    count: AtomicUsize,
    peak: AtomicUsize,
    pages: Box<[AtomicU32]>,
    membership: Box<[AtomicU64]>,
    metadata_bytes: usize,
    limit: AtomicUsize,
    denied: AtomicUsize,
    poisoned: AtomicBool,
    // The caller retains this recorder through mapping destruction. Store
    // callbacks alone are insufficient: their drop order is not that contract.
    overflow: Option<InitializedMemoryLoan>,
}

/// Conservative writable-page exposure, not resident bytes. Preflight charges
/// include pages that have not been written and growth that may still fail.
#[derive(Clone, Copy, Debug)]
pub(super) struct PrivatePageObservation {
    pub(super) exposed_bytes: usize,
    pub(super) peak_bytes: usize,
    pub(super) allowance_bytes: usize,
    pub(super) metadata_bytes: usize,
    pub(super) denied_writes: usize,
    pub(super) poisoned: bool,
}

impl PrivatePages {
    /// Recorder storage to reserve before attachment, including its owning Arc.
    /// Store hooks and signal-code ranges retain their separate host
    /// accounting.
    pub(super) fn metadata_bytes(maximum: usize, allowance: usize) -> anyhow::Result<usize> {
        anyhow::ensure!(
            maximum % PAGE_BYTES == 0
                && maximum <= RESERVATION_BYTES
                && allowance % PAGE_BYTES == 0
                && allowance <= maximum,
            "invalid initialized memory recorder limits"
        );
        // The Arc adds two reference-count words. Both arrays have exact lengths
        // bounded by wasm32 capacity; reclamation uses these same allocations.
        Ok(std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<AtomicUsize>()
            + allowance / PAGE_BYTES * std::mem::size_of::<AtomicU32>()
            + (maximum / PAGE_BYTES).div_ceil(64) * std::mem::size_of::<AtomicU64>())
    }

    fn new(address: *mut u8, len: usize, maximum: usize, allowance: usize) -> anyhow::Result<Self> {
        let metadata_bytes = Self::metadata_bytes(maximum, allowance)?;
        anyhow::ensure!(
            address as usize % PAGE_BYTES == 0
                && len % PAGE_BYTES == 0
                && maximum >= len
                && (address as usize).checked_add(RESERVATION_BYTES).is_some(),
            "invalid initialized memory recorder limits"
        );
        let pages = (0..allowance / PAGE_BYTES)
            .map(|_| AtomicU32::new(0))
            .collect();
        let membership = (0..(maximum / PAGE_BYTES).div_ceil(64))
            .map(|_| AtomicU64::new(0))
            .collect();
        Ok(Self {
            address: address as usize,
            len: AtomicUsize::new(len),
            maximum,
            count: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            pages,
            membership,
            metadata_bytes,
            limit: AtomicUsize::new(allowance / PAGE_BYTES),
            denied: AtomicUsize::new(0),
            poisoned: AtomicBool::new(false),
            overflow: None,
        })
    }

    fn contains(&self, page: usize) -> bool {
        self.membership[page / 64].load(Ordering::Relaxed) & (1u64 << (page % 64)) != 0
    }

    fn deny(&self) -> bool {
        self.denied.fetch_add(1, Ordering::Relaxed);
        self.poisoned.store(true, Ordering::Release);
        false
    }

    // No allocation, locks or callbacks: this also runs inside the synchronous
    // fault handler. A denied operation terminates this slot's usable lifetime.
    fn reserve(&self, first: usize, end: usize) -> bool {
        if self.poisoned.load(Ordering::Acquire) {
            return false;
        }
        let count = self.count.load(Ordering::Acquire);
        let limit = self.limit.load(Ordering::Relaxed)
            + self
                .overflow
                .as_ref()
                .map_or(0, |loan| loan.borrowed_bytes() / PAGE_BYTES);
        let local_available = limit.saturating_sub(count).min(self.pages.len() - count);
        // Reject a large bulk write/grow as soon as it exceeds the allowance;
        // denial must not scan the rest of the virtual address space.
        let mut unseen = (first..end).filter(|&page| !self.contains(page));
        let mut new = unseen.by_ref().take(local_available + 1).count();
        if new > local_available {
            // Ordinary writes consume local allowance without touching the
            // controller's shared cache line. Inspect shared credit only when
            // this operation would actually need it.
            let available = local_available
                .saturating_add(
                    self.overflow
                        .as_ref()
                        .map_or(0, InitializedMemoryLoan::available_pages),
                )
                .min(self.pages.len() - count);
            if new > available {
                return self.deny();
            }
            new += unseen.take(available - new + 1).count();
            if new > available {
                return self.deny();
            }
        }
        let overflow = (count + new).saturating_sub(limit);
        if overflow > 0
            && !self
                .overflow
                .as_ref()
                .is_some_and(|loan| loan.try_borrow(overflow))
        {
            return self.deny();
        }
        let mut next = count;
        for page in first..end {
            if !self.contains(page) {
                self.pages[next].store(page as u32, Ordering::Relaxed);
                self.membership[page / 64].fetch_or(1u64 << (page % 64), Ordering::Relaxed);
                next += 1;
            }
        }
        self.count.store(next, Ordering::Release);
        self.peak.fetch_max(next, Ordering::Relaxed);
        true
    }

    /// Preflight a native write while exclusively owning the Store. Permission
    /// lasts only until that borrow ends; no reclamation may overlap it.
    pub(super) unsafe fn permit(&self, address: usize, length: usize) -> bool {
        if length == 0 {
            return true;
        }
        let Some(absolute_end) = address.checked_add(length) else {
            return self.deny();
        };
        // Runtime bulk helpers also write tables and other host allocations.
        // Those owners retain their independent admission/accounting contracts.
        if absolute_end <= self.address || address >= self.address + RESERVATION_BYTES {
            return true;
        }
        let Some(offset) = address.checked_sub(self.address) else {
            return self.deny();
        };
        let Some(end) = offset
            .checked_add(length)
            .filter(|&end| end <= self.len.load(Ordering::Acquire))
        else {
            return self.deny();
        };
        let first = offset / PAGE_BYTES;
        let end = end.div_ceil(PAGE_BYTES);
        if (first..end).all(|page| self.contains(page)) {
            return !self.poisoned.load(Ordering::Acquire);
        }
        if !self.reserve(first, end) {
            return false;
        }
        // Charge before changing protection, including a partially failed call.
        if unsafe {
            libc::mprotect(
                (self.address + first * PAGE_BYTES) as *mut _,
                (end - first) * PAGE_BYTES,
                libc::PROT_READ | libc::PROT_WRITE,
            )
        } != 0
        {
            return self.deny();
        }
        true
    }

    fn permit_growth(&self, address: usize, current: usize, desired: usize) -> bool {
        if address != self.address {
            return true;
        }
        if self.len.load(Ordering::Acquire) != current
            || desired < current
            || desired > self.maximum
            || desired % PAGE_BYTES != 0
        {
            return self.deny();
        }
        // Charge the whole newly writable tail before growth. Previously
        // allocated virtual capacity is not authority for unrecorded writes.
        if !self.reserve(current / PAGE_BYTES, desired / PAGE_BYTES) {
            return false;
        }
        self.len.store(desired, Ordering::Release);
        true
    }

    pub(super) fn observation(&self) -> PrivatePageObservation {
        PrivatePageObservation {
            exposed_bytes: self.count.load(Ordering::Acquire) * PAGE_BYTES,
            peak_bytes: self.peak.load(Ordering::Acquire) * PAGE_BYTES,
            allowance_bytes: self.limit.load(Ordering::Relaxed) * PAGE_BYTES
                + self
                    .overflow
                    .as_ref()
                    .map_or(0, InitializedMemoryLoan::borrowed_bytes),
            metadata_bytes: self.metadata_bytes,
            denied_writes: self.denied.load(Ordering::Relaxed),
            poisoned: self.poisoned.load(Ordering::Acquire),
        }
    }

    pub(super) fn overflow_loan(&self) -> Option<&InitializedMemoryLoan> {
        self.overflow.as_ref()
    }

    /// The controller has transferred this exact loan into the invocation's
    /// existing charge. No guest call may occur between transfer and adoption.
    pub(super) unsafe fn accept_overflow_transfer(&self, bytes: usize) {
        assert!(bytes % PAGE_BYTES == 0);
        assert!(self
            .overflow
            .as_ref()
            .is_some_and(|loan| loan.borrowed_bytes() == 0));
        let allowance = self.limit.load(Ordering::Relaxed) + bytes / PAGE_BYTES;
        assert!(allowance <= self.pages.len());
        self.limit.store(allowance, Ordering::Release);
    }

    /// Called at an invocation boundary after reserving the corresponding host
    /// budget. Existing private backing is already charged and cannot disappear
    /// merely because a smaller forecast was learned.
    pub(super) unsafe fn begin_invocation(&self, allowance: usize) -> anyhow::Result<()> {
        let count = self.count.load(Ordering::Acquire);
        anyhow::ensure!(
            !self.poisoned.load(Ordering::Acquire)
                && self
                    .overflow
                    .as_ref()
                    .is_none_or(|loan| loan.borrowed_bytes() == 0)
                && allowance % PAGE_BYTES == 0
                && allowance / PAGE_BYTES <= self.pages.len()
                && allowance / PAGE_BYTES >= count,
            "invalid initialized memory allowance"
        );
        self.limit.store(allowance / PAGE_BYTES, Ordering::Release);
        self.peak.store(count, Ordering::Release);
        Ok(())
    }

    unsafe fn discard_page(&self, page: usize) -> anyhow::Result<()> {
        let address = (self.address + page * PAGE_BYTES) as *mut libc::c_void;
        anyhow::ensure!(
            unsafe { libc::mprotect(address, PAGE_BYTES, libc::PROT_READ) } == 0
                && unsafe { libc::madvise(address, PAGE_BYTES, libc::MADV_DONTNEED) } == 0,
            "discard initialized memory page: {}",
            std::io::Error::last_os_error()
        );
        self.membership[page / 64].fetch_and(!(1u64 << (page % 64)), Ordering::Relaxed);
        Ok(())
    }

    /// Ranges must describe current dead payload, excluding metadata, roots and
    /// native resources. The guest must not allocate or collect between
    /// producing them and this call. No guest call or borrowed memory slice may
    /// overlap reclamation. RequireTarget leaves an insufficient set unchanged;
    /// AvailablePayload releases what is proven free even when the target
    /// cannot be reached. Neither mode grants credit before actual discard,
    /// and any error requires disposal.
    pub(super) unsafe fn reclaim(
        &self,
        ranges: &[Range<usize>],
        retain_bytes: usize,
        mode: ReclaimMode,
    ) -> anyhow::Result<Option<usize>> {
        anyhow::ensure!(
            !self.poisoned.load(Ordering::Acquire)
                && retain_bytes % PAGE_BYTES == 0
                && ranges.iter().all(|range| range.start < range.end
                    && range.end <= self.len.load(Ordering::Acquire))
                && ranges.windows(2).all(|pair| pair[0].end <= pair[1].start),
            "invalid initialized memory reclamation ranges"
        );
        let count = self.count.load(Ordering::Acquire);
        let excess = count.saturating_sub(retain_bytes / PAGE_BYTES);
        if excess == 0 {
            return Ok(Some(0));
        }
        let reclaimable = |page: u32| {
            let offset = page as usize * PAGE_BYTES;
            let interval = ranges.partition_point(|range| range.end <= offset);
            ranges
                .get(interval)
                .is_some_and(|range| range.start <= offset && offset + PAGE_BYTES <= range.end)
        };
        // Count authority before changing pages. Partial release is useful under
        // lifetime pressure even when live state prevents reaching the target.
        // Work stays bounded by the recorder; this runs outside admission locks.
        let available = (0..count)
            .filter(|&index| reclaimable(self.pages[index].load(Ordering::Relaxed)))
            .take(excess)
            .count();
        if available < excess && mode == ReclaimMode::RequireTarget {
            return Ok(None);
        }
        let excess = available;
        if excess == 0 {
            return Ok(Some(0));
        }
        self.poisoned.store(true, Ordering::Release);
        let mut discarded = 0;
        for index in (0..count).rev() {
            let page = self.pages[index].load(Ordering::Relaxed);
            if reclaimable(page) {
                unsafe {
                    self.discard_page(page as usize)?;
                }
                discarded += 1;
                if discarded == excess {
                    break;
                }
            }
        }
        let mut keep = 0;
        for index in 0..count {
            let page = self.pages[index].load(Ordering::Relaxed);
            if self.contains(page as usize) {
                self.pages[keep].store(page, Ordering::Relaxed);
                keep += 1;
            }
        }
        anyhow::ensure!(
            discarded == excess && count - keep == excess,
            "initialized memory reclamation count changed"
        );
        self.count.store(keep, Ordering::Release);
        self.poisoned.store(false, Ordering::Release);
        Ok(Some(excess * PAGE_BYTES))
    }

    /// Synchronous Linux x86-64 fault hook. No allocation, locking, formatting
    /// or unwinding is allowed here. Unrelated faults remain Wasmtime's
    /// concern.
    pub(super) unsafe fn handle_fault(
        &self,
        signal: i32,
        info: *const libc::siginfo_t,
        context: *const libc::c_void,
    ) -> Result<bool, wasmtime::Trap> {
        if signal != libc::SIGSEGV || unsafe { (*info).si_code } != 2 {
            return Ok(false);
        }
        let address = unsafe { (*info).si_addr() } as usize;
        let registers = unsafe { &*(context as *const libc::ucontext_t) };
        if registers.uc_mcontext.gregs[libc::REG_ERR as usize] & 2 == 0
            || address < self.address
            || address >= self.address + self.len.load(Ordering::Acquire)
        {
            return Ok(false);
        }
        if self.contains((address - self.address) / PAGE_BYTES) {
            // It was already made writable. Retrying an unexplained fault here
            // would livelock rather than repair the underlying failure.
            self.deny();
            return Err(wasmtime::Trap::AllocationTooLarge);
        }
        if unsafe { self.permit(address, 1) } {
            Ok(true)
        } else {
            Err(wasmtime::Trap::AllocationTooLarge)
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::*;

    #[tokio::test]
    async fn real_store_overflow_transfers_once_and_survives_cancelled_disposal(
    ) -> anyhow::Result<()> {
        use std::time::Duration;

        use crate::environment::udf::generated_wasm_memory::{
            FunctionMemoryIdentity,
            FunctionMemoryLineage,
            GeneratedMemoryController,
            GeneratedMemoryPolicy,
            TerminalMemoryOutcome,
        };

        static BUFFERS: packed_value::memory::BufferMemory =
            packed_value::memory::BufferMemory::new();
        let identity = FunctionMemoryIdentity {
            deployment: "image-overflow".into(),
            package_key: "image-overflow".into(),
            lineage: FunctionMemoryLineage::CapabilityPackage {
                package_key: "image-overflow".into(),
            },
            compiler_pipeline_sha256: "0".repeat(64),
            compiler_revision: "compiler".into(),
            static_hermes_revision: "runtime".into(),
            admitted_language_version: 1,
            manifest_schema_version: 1,
            effect_execution_mode: super::super::EffectExecutionMode::BlockingFiber,
            opaque_value_abi_version: 1,
            wasmtime_revision: "engine".into(),
            target_triple: "x86_64-unknown-linux-gnu".into(),
            target_cpu: "generic".into(),
            engine_configuration_sha256: "1".repeat(64),
            admission_policy_version: 1,
        };
        let mut config = wasmtime::Config::new();
        config.async_support(true).memory_reservation(1u64 << 32);
        let engine = wasmtime::Engine::new(&config).map_err(super::super::wasmtime_anyhow)?;
        let module = wasmtime::Module::new(
            &engine,
            r#"(module
                (import "test" "wait" (func $wait))
                (memory (export "memory") 1 1)
                (func (export "write") (param i32 i32)
                    local.get 0 local.get 1 i32.store8)
                (func (export "suspend")
                    i32.const 12288 i32.const 99 i32.store8 call $wait))"#,
        )
        .map_err(super::super::wasmtime_anyhow)?;

        for cancel in [false, true] {
            let controller = GeneratedMemoryController::with_shared_buffers(
                GeneratedMemoryPolicy {
                    hard_instance_ceiling: 2,
                    soft_budget_bytes: 8 * 1024 * 1024,
                    hard_budget_bytes: 16 * 1024 * 1024,
                    safety_reserve_bytes: PAGE_BYTES,
                    unattributed_bytes_per_slot: PAGE_BYTES,
                    cold_peak_growth_bytes: 65536,
                    warm_idle_target: 1,
                    maximum_idle_age: Duration::from_secs(60),
                    pressure_enter_headroom_bytes: 1,
                    pressure_exit_headroom_bytes: 2,
                },
                &BUFFERS,
            )?;
            let mut permit = controller.try_admit(identity.clone(), None).unwrap();
            permit.mark_execution_started(false);
            let mut store = Store::new(
                &engine,
                permit.limiter(wasmtime::StoreLimitsBuilder::new().build()),
            );
            store.limiter(|limiter| limiter);
            let mut linker = wasmtime::Linker::new(&engine);
            linker
                .func_wrap_async("test", "wait", |_, (): ()| {
                    Box::new(async { std::future::pending::<wasmtime::Result<()>>().await })
                })
                .map_err(super::super::wasmtime_anyhow)?;
            let instance = linker
                .instantiate_async(&mut store, &module)
                .await
                .map_err(super::super::wasmtime_anyhow)?;
            let memory = instance.get_memory(&mut store, "memory").unwrap();
            let charge = controller
                .try_charge_module(65536 + PrivatePages::metadata_bytes(65536, 65536)?)
                .unwrap();
            let image = InitializedMemoryImage::capture(memory.data(&store))?;
            let pages = unsafe {
                image.attach(
                    &mut store,
                    memory,
                    65536,
                    0,
                    Some(permit.initialized_memory_loan()),
                )?
            };
            permit.attach_initialized_memory(65536, 0);
            let before = controller.snapshot_for_test(&identity).projected_bytes;
            let write = instance
                .get_typed_func::<(i32, i32), ()>(&mut store, "write")
                .map_err(super::super::wasmtime_anyhow)?;
            write
                .call_async(&mut store, (0, 49))
                .await
                .map_err(super::super::wasmtime_anyhow)?;
            assert_eq!(pages.overflow_loan().unwrap().borrowed_bytes(), PAGE_BYTES);
            // This overlaps the scalar's page and crosses two new pages. The
            // native hook must borrow only for the new exposure.
            memory.write(&mut store, PAGE_BYTES - 1, &vec![82; PAGE_BYTES + 2])?;
            assert_eq!(memory.data(&store)[0], 49);
            assert_eq!(memory.data(&store)[2 * PAGE_BYTES], 82);
            assert_eq!(
                pages.overflow_loan().unwrap().borrowed_bytes(),
                3 * PAGE_BYTES
            );
            assert_eq!(
                controller.snapshot_for_test(&identity).projected_bytes,
                before + 3 * PAGE_BYTES
            );

            if cancel {
                let suspend = instance
                    .get_typed_func::<(), ()>(&mut store, "suspend")
                    .map_err(super::super::wasmtime_anyhow)?;
                let mut suspended = Box::pin(suspend.call_async(&mut store, ()));
                assert!(futures::poll!(&mut suspended).is_pending());
                assert_eq!(
                    pages.overflow_loan().unwrap().borrowed_bytes(),
                    4 * PAGE_BYTES
                );
                drop(suspended);
                permit.observe_initialized_memory_disposal(pages.observation().peak_bytes, false);
                // Cancellation drops the actual suspended Wasm future. Neither
                // the permit nor an extra recorder owner may refund a live loan.
                drop(store);
                permit.finish(TerminalMemoryOutcome::Cancellation, false);
                assert_eq!(
                    controller.snapshot_for_test(&identity).projected_bytes,
                    charge.fixed_bytes() + PAGE_BYTES + 4 * PAGE_BYTES
                );
                drop(pages);
                assert_eq!(
                    controller.snapshot_for_test(&identity).projected_bytes,
                    charge.fixed_bytes() + PAGE_BYTES
                );
            } else {
                let before_transfer = controller.snapshot_for_test(&identity).projected_bytes;
                let transferred = store
                    .data()
                    .absorb_initialized_loan(pages.overflow_loan().unwrap());
                assert_eq!(transferred, 3 * PAGE_BYTES);
                unsafe { pages.accept_overflow_transfer(transferred) };
                assert_eq!(
                    controller.snapshot_for_test(&identity).projected_bytes,
                    before_transfer
                );
                assert_eq!(
                    store
                        .data()
                        .absorb_initialized_loan(pages.overflow_loan().unwrap()),
                    0
                );
                let observation = pages.observation();
                store.data().reconcile_initialized_memory(
                    65536,
                    observation.exposed_bytes,
                    observation.peak_bytes,
                    observation.allowance_bytes,
                    false,
                );
                let slot = permit.slot_id();
                permit.finish(TerminalMemoryOutcome::Success, true);
                assert_eq!(
                    controller
                        .snapshot_for_test(&identity)
                        .retained_baseline_bytes,
                    3 * PAGE_BYTES
                );

                let mut next = controller.try_admit(identity.clone(), Some(slot)).unwrap();
                next.mark_execution_started(true);
                *store.data_mut() = next.limiter(wasmtime::StoreLimitsBuilder::new().build());
                unsafe { pages.begin_invocation(next.begin_initialized_memory(65536))? };
                write
                    .call_async(&mut store, (0, 115))
                    .await
                    .map_err(super::super::wasmtime_anyhow)?;
                assert_eq!(memory.data(&store)[0], 115);
                assert_eq!(pages.overflow_loan().unwrap().borrowed_bytes(), 0);
                assert_eq!(pages.observation().exposed_bytes, 3 * PAGE_BYTES);
                drop(store);
                drop(pages);
                next.finish(TerminalMemoryOutcome::Success, false);
            }
            drop(image);
            drop(charge);
            assert_eq!(
                controller.snapshot_for_test(&identity).projected_bytes,
                PAGE_BYTES
            );
        }
        Ok(())
    }

    fn image_view(
        image: &InitializedMemoryImage,
        maximum_pages: u32,
        allowance: usize,
    ) -> anyhow::Result<(Store<()>, Memory, Arc<PrivatePages>)> {
        let mut config = wasmtime::Config::new();
        config.memory_reservation(1u64 << 32);
        let engine = wasmtime::Engine::new(&config).map_err(super::super::wasmtime_anyhow)?;
        // Exercise replacement of Wasmtime's ordinary static COW image, whose
        // slot continues to manage growth after attachment.
        let module = wasmtime::Module::new(
            &engine,
            format!(
                r#"(module
                    (memory (export "memory") 1 {maximum_pages})
                    (data (i32.const 0) "ordinary static image"))"#
            ),
        )
        .map_err(super::super::wasmtime_anyhow)?;
        let mut store = Store::new(&engine, ());
        let instance = wasmtime::Instance::new(&mut store, &module, &[])
            .map_err(super::super::wasmtime_anyhow)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("missing test memory")?;
        let pages = unsafe { image.attach(&mut store, memory, allowance, allowance, None)? };
        Ok((store, memory, pages))
    }

    #[test]
    fn attachment_rejects_invalid_setup_before_replacing_bytes() -> anyhow::Result<()> {
        let image = InitializedMemoryImage::capture(&vec![0x31; 65536])?;
        for (reservation, signals, allowance) in [
            (65536, true, PAGE_BYTES),
            (1u64 << 32, false, PAGE_BYTES),
            (1u64 << 32, true, PAGE_BYTES + 1),
        ] {
            let mut config = wasmtime::Config::new();
            config
                .memory_reservation(reservation)
                .signals_based_traps(signals);
            let engine = wasmtime::Engine::new(&config).map_err(super::super::wasmtime_anyhow)?;
            let mut store = Store::new(&engine, ());
            let memory_type =
                wasmtime::MemoryType::new(1, Some(2)).map_err(super::super::wasmtime_anyhow)?;
            let memory =
                Memory::new(&mut store, memory_type).map_err(super::super::wasmtime_anyhow)?;
            memory.write(&mut store, 0, &[0x52])?;
            assert!(
                unsafe { image.attach(&mut store, memory, allowance, allowance, None) }.is_err()
            );
            assert_eq!(memory.data(&store)[0], 0x52);
        }
        Ok(())
    }

    #[test]
    fn guest_and_native_writes_share_one_terminal_private_page_budget() -> anyhow::Result<()> {
        let mut config = wasmtime::Config::new();
        config.memory_reservation(1u64 << 32);
        let engine = wasmtime::Engine::new(&config).map_err(super::super::wasmtime_anyhow)?;
        let module = wasmtime::Module::new(
            &engine,
            r#"(module
            (memory (export "memory") 1 2)
            (data (i32.const 0) "ordinary initialization image")
            (func (export "write") (param i32 i32)
                local.get 0 local.get 1 i32.store8)
            (func (export "fill") (param i32 i32)
                local.get 0 i32.const 49 local.get 1 memory.fill))"#,
        )
        .map_err(super::super::wasmtime_anyhow)?;
        let mut store = Store::new(&engine, ());
        let instance = wasmtime::Instance::new(&mut store, &module, &[])
            .map_err(super::super::wasmtime_anyhow)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("missing test memory")?;
        let image = InitializedMemoryImage::capture(memory.data(&store))?;
        let pages =
            unsafe { image.attach(&mut store, memory, 2 * PAGE_BYTES, 2 * PAGE_BYTES, None)? };
        let write = instance
            .get_typed_func::<(i32, i32), ()>(&mut store, "write")
            .map_err(super::super::wasmtime_anyhow)?;
        let fill = instance
            .get_typed_func::<(i32, i32), ()>(&mut store, "fill")
            .map_err(super::super::wasmtime_anyhow)?;
        write
            .call(&mut store, (0, 82))
            .map_err(super::super::wasmtime_anyhow)?;
        fill.call(&mut store, (PAGE_BYTES as i32, PAGE_BYTES as i32))
            .map_err(super::super::wasmtime_anyhow)?;
        assert_eq!(memory.data(&store)[0], 82);
        assert_eq!(memory.data(&store)[PAGE_BYTES], 49);
        assert_eq!(pages.observation().exposed_bytes, 2 * PAGE_BYTES);
        // Memory::write uses the native preflight. It cannot write a new page
        // once the same allowance has been consumed by scalar/bulk guest writes.
        assert!(memory.write(&mut store, 2 * PAGE_BYTES, &[115]).is_err());
        assert_eq!(memory.data(&store)[2 * PAGE_BYTES], 0);
        assert!(pages.observation().poisoned);
        assert_eq!(pages.observation().exposed_bytes, 2 * PAGE_BYTES);
        drop(store);
        Ok(())
    }

    #[test]
    fn image_views_isolate_writes_and_restore_grown_capacity() -> anyhow::Result<()> {
        let original = vec![0x31; 65536];
        let image = InitializedMemoryImage::capture(&original)?;
        let (mut store, memory, pages) = image_view(&image, 3, 196608)?;
        let (other_store, other_memory, other_pages) = image_view(&image, 2, 131072)?;
        let address = memory.data_ptr(&store);
        memory.write(&mut store, 0, &[0x52])?;
        assert_eq!(other_memory.data(&other_store)[0], 0x31);
        assert_eq!(pages.observation().exposed_bytes, PAGE_BYTES);
        memory
            .grow(&mut store, 1)
            .map_err(super::super::wasmtime_anyhow)?;
        assert_eq!(memory.data_ptr(&store), address);
        // The entire tail is charged before it becomes writable, even before
        // the next native or guest write has touched its physical pages.
        assert_eq!(pages.observation().exposed_bytes, PAGE_BYTES + 65536);
        memory.write(&mut store, 65536, &[0x73])?;
        unsafe { image.restore(&pages, 131072)? };
        assert_eq!(&memory.data(&store)[..65536], original);
        assert!(memory.data(&store)[65536..].iter().all(|&byte| byte == 0));
        unsafe { image.restore(&pages, 0)? };
        assert_eq!(pages.observation().exposed_bytes, 0);
        memory.write(&mut store, 65536, &[0x94])?;
        unsafe { image.restore(&pages, 0)? };
        assert_eq!(memory.data(&store)[65536], 0);
        assert_eq!(memory.data_size(&store), 131072);

        // A later image-slot growth must not make the previously restored tail
        // writable again without recording it. Use a scalar guest store so the
        // write is charged only if that page is still protected.
        memory
            .grow(&mut store, 1)
            .map_err(super::super::wasmtime_anyhow)?;
        assert_eq!(pages.observation().exposed_bytes, 65536);
        let writer = wasmtime::Module::new(
            store.engine(),
            r#"(module
                (import "" "memory" (memory 1 3))
                (func (export "write")
                    i32.const 65536 i32.const 49 i32.store8))"#,
        )
        .map_err(super::super::wasmtime_anyhow)?;
        let writer = wasmtime::Instance::new(&mut store, &writer, &[memory.into()])
            .map_err(super::super::wasmtime_anyhow)?;
        writer
            .get_typed_func::<(), ()>(&mut store, "write")
            .map_err(super::super::wasmtime_anyhow)?
            .call(&mut store, ())
            .map_err(super::super::wasmtime_anyhow)?;
        assert_eq!(memory.data_ptr(&store), address);
        assert_eq!(memory.data(&store)[65536], 49);
        assert_eq!(pages.observation().exposed_bytes, 65536 + PAGE_BYTES);
        assert_eq!(other_pages.observation().exposed_bytes, 0);
        Ok(())
    }

    #[test]
    fn reclaim_requires_whole_owned_pages_and_preserves_live_neighbors() -> anyhow::Result<()> {
        let image = InitializedMemoryImage::capture(&vec![0x31; 65536])?;
        let (mut store, memory, pages) = image_view(&image, 1, 65536)?;
        memory.write(&mut store, 0, &vec![0x52; 3 * PAGE_BYTES])?;
        unsafe {
            // Neither boundary page belongs wholly to the returned payload.
            assert_eq!(
                pages.reclaim(
                    &[1..3 * PAGE_BYTES - 1],
                    PAGE_BYTES,
                    ReclaimMode::RequireTarget
                )?,
                None
            );
            assert_eq!(pages.observation().exposed_bytes, 3 * PAGE_BYTES);
            assert_eq!(
                pages.reclaim(
                    &[1..3 * PAGE_BYTES - 1],
                    PAGE_BYTES,
                    ReclaimMode::AvailablePayload
                )?,
                Some(PAGE_BYTES)
            );
        }
        // Partial release preserves both live boundary pages even though that
        // leaves exposure above the requested one-page target.
        assert_eq!(pages.observation().exposed_bytes, 2 * PAGE_BYTES);
        assert_eq!(memory.data(&store)[0], 0x52);
        assert_eq!(memory.data(&store)[PAGE_BYTES], 0x31);
        assert_eq!(memory.data(&store)[2 * PAGE_BYTES], 0x52);
        memory.write(&mut store, PAGE_BYTES, &[0x73])?;
        assert_eq!(pages.observation().exposed_bytes, 3 * PAGE_BYTES);
        assert_eq!(pages.observation().peak_bytes, 3 * PAGE_BYTES);
        Ok(())
    }

    #[test]
    fn invalid_native_ranges_poison_without_releasing_existing_pages() -> anyhow::Result<()> {
        let image = InitializedMemoryImage::capture(&vec![0; 65536])?;
        for case in 0..3 {
            let (mut store, memory, pages) = image_view(&image, 2, 2 * PAGE_BYTES)?;
            let base = memory.data_ptr(&store) as usize;
            let mut unrelated = [0u8; 8];
            unsafe {
                assert!(pages.permit(unrelated.as_mut_ptr() as usize, unrelated.len()));
                assert!(pages.permit(usize::MAX, 0));
            }
            assert_eq!(pages.observation().exposed_bytes, 0);
            memory
                .data_mut_range(&mut store, PAGE_BYTES - 1..PAGE_BYTES + 1)?
                .copy_from_slice(&[0x31, 0x52]);
            let address = match case {
                0 => base - 1,
                1 => base + 65535,
                2 => usize::MAX,
                _ => unreachable!(),
            };
            unsafe {
                assert!(!pages.permit(address, 2));
                assert!(!pages.permit(base, 1));
                assert!(pages.begin_invocation(2 * PAGE_BYTES).is_err());
            }
            assert_eq!(
                &memory.data(&store)[PAGE_BYTES - 1..PAGE_BYTES + 1],
                &[0x31, 0x52]
            );
            let observation = pages.observation();
            assert_eq!(observation.exposed_bytes, 2 * PAGE_BYTES);
            assert_eq!(observation.denied_writes, 1);
            assert!(observation.poisoned);
        }
        Ok(())
    }

    #[test]
    fn reclamation_compacts_sparse_records_and_records_discarded_pages_again() -> anyhow::Result<()>
    {
        let image = InitializedMemoryImage::capture(&vec![0x31; 65536])?;
        let (mut store, memory, pages) = image_view(&image, 1, 4 * PAGE_BYTES)?;
        for page in [3, 0, 2, 1] {
            memory.write(&mut store, page * PAGE_BYTES, &[0x52])?;
        }
        unsafe {
            assert_eq!(
                pages.reclaim(
                    &[0..2 * PAGE_BYTES, 3 * PAGE_BYTES..4 * PAGE_BYTES],
                    2 * PAGE_BYTES,
                    ReclaimMode::RequireTarget,
                )?,
                Some(2 * PAGE_BYTES)
            );
        }
        assert_eq!(memory.data(&store)[2 * PAGE_BYTES], 0x52);
        assert_eq!(
            (0..4)
                .filter(|&page| memory.data(&store)[page * PAGE_BYTES] == 0x52)
                .count(),
            2
        );
        memory.write(&mut store, 0, &vec![0x73; 4 * PAGE_BYTES])?;
        assert_eq!(pages.observation().exposed_bytes, 4 * PAGE_BYTES);
        unsafe { image.restore(&pages, PAGE_BYTES)? };
        assert!(memory.data(&store)[..4 * PAGE_BYTES]
            .iter()
            .all(|&byte| byte == 0x31));
        assert_eq!(pages.observation().exposed_bytes, PAGE_BYTES);
        Ok(())
    }

    #[test]
    fn growth_denial_is_terminal_and_keeps_previous_exposure_charged() -> anyhow::Result<()> {
        let image = InitializedMemoryImage::capture(&vec![0; 65536])?;
        let (mut store, memory, pages) = image_view(&image, 2, PAGE_BYTES)?;
        memory.write(&mut store, 0, &[0x31])?;
        let error = memory.grow(&mut store, 1).unwrap_err();
        assert_eq!(
            error.downcast_ref::<wasmtime::Trap>(),
            Some(&wasmtime::Trap::AllocationTooLarge)
        );
        assert_eq!(memory.data_size(&store), 65536);
        let observation = pages.observation();
        assert_eq!(observation.exposed_bytes, PAGE_BYTES);
        assert_eq!(observation.denied_writes, 1);
        assert!(observation.poisoned);
        unsafe {
            assert!(!pages.permit(memory.data_ptr(&store) as usize, 1));
            assert!(pages.begin_invocation(PAGE_BYTES).is_err());
            assert!(pages
                .reclaim(&[0..PAGE_BYTES], 0, ReclaimMode::RequireTarget)
                .is_err());
        }
        Ok(())
    }
}
