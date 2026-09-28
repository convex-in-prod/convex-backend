//! Request-independent images owned by the existing preparation group. The
//! cache never contains a request-warmed Store, and no Store-owned reference
//! crosses the image boundary.

use value::heap_size::HeapSize;

use super::*;
use crate::environment::udf::static_hermes_wasmtime_gate::{
    guest_memory_maintenance::{
        MaintenanceOperation,
        MaintenanceResult,
    },
    initialized_memory::{
        ImageCaptureError,
        InitializedMemoryImage,
        PrivatePages,
    },
    initialized_state::CapturedModuleState,
};

pub(super) struct GeneratedPreparedImage {
    memory: InitializedMemoryImage,
    modules: Vec<CapturedModuleState>,
    context_read_set: ContextReadSet,
    context_metadata_bytes: usize,
    wasi_monotonic_epoch: tokio::time::Instant,
    wasi_cpu_time: Option<u64>,
    pool_identity: Arc<GeneratedPoolIdentity>,
    generation: Weak<DeploymentGeneration>,
    identity_sha256: String,
    clone_host_bytes: usize,
    // A sparse image can acquire backing when a zero page is read. Its whole
    // capacity remains charged, including after the group releases its cache.
    charge: GeneratedModuleMemoryCharge,
}

impl GeneratedPreparedImage {
    pub(super) fn clone_construction_peak_bytes(&self) -> usize {
        // Instantiation and growth precede remapping, so reserve the complete
        // ordinary linear allocation, plus observed preparation host state.
        // This is a forecast: invalidated dependencies can require ordinary
        // initialization, whose actual allocations remain admission-checked.
        self.memory.len.saturating_add(self.clone_host_bytes)
    }
}

pub(super) struct GeneratedInitializedMemory {
    // None is permitted only during a failed attachment in a poisoned Store.
    pub(super) pages: Option<Arc<PrivatePages>>,
    maximum_private_bytes: usize,
    image: Arc<GeneratedPreparedImage>,
    // GeneratedReusableInstance drops the Store (and its hooks) before this
    // field. This last charge therefore outlives every recorder allocation.
    _metadata_charge: GeneratedModuleMemoryCharge,
}

impl GeneratedPreparedImageCache {
    pub(super) fn publish(&self, image: Arc<GeneratedPreparedImage>) {
        if image
            .generation
            .upgrade()
            .is_none_or(|generation| generation.is_retired())
        {
            return;
        }
        image.charge.describe(
            "initialized_image",
            image.identity_sha256.clone(),
            Some(image.memory.stored_bytes),
            true,
        );
        let previous = self.image.lock().replace(image);
        // Charge destruction and unmapping must never hold the cache mutex.
        if let Some(previous) = previous {
            previous.charge.mark_uncached();
        }
    }

    fn remove(&self, expected: &Arc<GeneratedPreparedImage>) {
        let removed = {
            let mut cached = self.image.lock();
            if cached
                .as_ref()
                .is_some_and(|image| Arc::ptr_eq(image, expected))
            {
                cached.take()
            } else {
                None
            }
        };
        if let Some(image) = removed {
            image.charge.mark_uncached();
        }
    }
}

impl Drop for GeneratedPreparedImageCache {
    fn drop(&mut self) {
        if let Some(image) = self.image.get_mut().take() {
            image.charge.mark_uncached();
        }
    }
}

impl<RT: Runtime> GeneratedReusableInstance<RT> {
    fn image_controller(&self) -> anyhow::Result<Arc<GeneratedMemoryController>> {
        Ok(Arc::clone(
            generated_state(self.store.data())
                .map_err(wasmtime_anyhow)?
                .memory_permit
                .as_ref()
                .context("initialized image lost its memory admission")?
                .controller(),
        ))
    }

    async fn install_prepared_image(
        &mut self,
        image: Arc<GeneratedPreparedImage>,
    ) -> anyhow::Result<bool> {
        anyhow::ensure!(
            self.initialized_memory.is_none() && !self.store.has_pending_exception(),
            "initialized image requires an unattached runtime without a pending exception"
        );
        let states = self
            .initialized_state
            .as_ref()
            .context("initialized image lost its authenticated scalar layout")?;
        anyhow::ensure!(
            states.len() == image.modules.len(),
            "initialized image graph changed"
        );
        let ty = self.memory.ty(&self.store);
        let maximum = usize::try_from(ty.maximum().unwrap_or(65536))?
            .checked_mul(65536)
            .context("initialized memory maximum overflow")?;
        let allowance =
            usize::try_from(self.routed.manifest.limits().max_guest_memory_bytes())?.min(maximum);
        // Each view owns a mutable copy of the dependency evidence for later
        // ordinary reuse. Its storage must outlive invalidation of the cache.
        let metadata_bytes = PrivatePages::metadata_bytes(maximum, allowance)?
            .checked_add(image.context_metadata_bytes)
            .context("initialized instance metadata charge overflow")?;
        let metadata_charge = match self.image_controller()?.try_charge_module(metadata_bytes) {
            Ok(charge) => charge,
            Err(
                ModuleMemoryAdmissionError::Pressure
                | ModuleMemoryAdmissionError::SoftBudget
                | ModuleMemoryAdmissionError::HardBudget
                | ModuleMemoryAdmissionError::FixedBytesExceedHardBudget,
            ) => return Ok(false),
        };
        metadata_charge.describe(
            "initialized_instance_metadata",
            image.identity_sha256.clone(),
            None,
            false,
        );
        let current = self.memory.data_size(&self.store);
        anyhow::ensure!(
            current <= image.memory.len && image.memory.len % 65536 == 0,
            "initialized image is smaller than its linked graph"
        );
        if current < image.memory.len {
            self.memory
                .grow_async(
                    &mut self.store,
                    u64::try_from((image.memory.len - current) / 65536)?,
                )
                .await
                .map_err(wasmtime_anyhow)?;
        }
        // All table placements/GOT bindings were reconstructed by ordinary
        // instantiation. Only authenticated numeric state is restored here.
        for (state, captured) in states.iter().zip(&image.modules) {
            state.restore(&mut self.store, captured).await?;
        }
        let permit = generated_state(self.store.data())
            .map_err(wasmtime_anyhow)?
            .memory_permit
            .as_ref()
            .context("initialized instance lost its memory permit")?;
        let initial_allowance = permit.initial_private_allowance(image.memory.len, allowance);
        let overflow = permit.initialized_memory_loan();
        // From this point any error must use host-only disposal: memory may
        // already have been replaced, or only some hooks may be installed.
        self.runtime_state = GeneratedRuntimeState::Poisoned;
        self.initialized_memory = Some(GeneratedInitializedMemory {
            pages: None,
            maximum_private_bytes: allowance,
            image,
            _metadata_charge: metadata_charge,
        });
        let initialized = self.initialized_memory.as_mut().unwrap();
        let pages = unsafe {
            initialized.image.memory.attach(
                &mut self.store,
                self.memory,
                allowance,
                initial_allowance,
                Some(overflow),
            )?
        };
        let mut charges = self.routed.memory_charge_ids();
        charges.push(initialized.image.charge.inspection_id());
        charges.push(initialized._metadata_charge.inspection_id());
        let permit = generated_state(self.store.data())
            .map_err(wasmtime_anyhow)?
            .memory_permit
            .as_ref()
            .context("initialized instance lost its memory permit")?;
        permit.attach_initialized_memory(initialized.image.memory.len, initial_allowance);
        initialized.pages = Some(pages);
        permit.describe_modules(charges, Some(initialized.image.charge.inspection_id()));
        self.runtime_state = GeneratedRuntimeState::Live;
        Ok(true)
    }

    pub(super) fn begin_initialized_memory(&mut self) -> anyhow::Result<()> {
        let Some(initialized) = &self.initialized_memory else {
            return Ok(());
        };
        let pages = initialized
            .pages
            .as_ref()
            .context("initialized runtime has no recorder")?;
        let allowance = generated_state(self.store.data())
            .map_err(wasmtime_anyhow)?
            .memory_permit
            .as_ref()
            .context("initialized runtime lost its memory permit")?
            .begin_initialized_memory(initialized.maximum_private_bytes);
        // Exclusive Store ownership is held, and the controller assigned this
        // grant from the checkout reservation before any guest entry.
        unsafe { pages.begin_invocation(allowance) }
    }

    pub(super) async fn restore_prepared_image(
        &mut self,
        cache: &GeneratedPreparedImageCache,
    ) -> anyhow::Result<bool> {
        let Some(image) = cache.image.lock().clone() else {
            return Ok(false);
        };
        let same_generation = self.routed.generation.as_ref().is_some_and(|generation| {
            std::ptr::eq(image.generation.as_ptr(), Arc::as_ptr(generation))
        });
        anyhow::ensure!(
            same_generation && image.pool_identity == self.pool_identity,
            "initialized image cache crossed a generation or graph boundary"
        );
        anyhow::ensure!(
            self.runtime_state == GeneratedRuntimeState::Inactive && !self.all_entries_prepared,
            "initialized image may only replace a newly linked Store"
        );
        let state = self.store.data_mut();
        let validation = ContextCache::validate_and_apply_context_read_set(
            state.provider.tx_for_initialization()?,
            &image.context_read_set,
        );
        let valid = state
            .timeout
            .as_mut()
            .context("image validation lost its preparation timeout")?
            .with_release_permit(PauseReason::UdfInitialize, validation)
            .await
            .map_err(|error| wasmtime_anyhow(generated_host_operation_error(error)))?;
        if !valid {
            // A newer or older transaction can invalidate the same source's
            // configuration. Reprepare against this transaction; existing views
            // keep their image owner until their own ordinary validation/drop.
            cache.remove(&image);
            return Ok(false);
        }
        if !self.install_prepared_image(Arc::clone(&image)).await? {
            return Ok(false);
        }
        // Native runtime timestamps are part of the captured bytes. A view
        // must keep their clock origin even though its HostState is fresh.
        self.store.data_mut().wasi_monotonic_epoch = image.wasi_monotonic_epoch;
        self.store
            .data()
            .wasi_cpu_clock
            .advance_to(image.wasi_cpu_time);
        // No module initializer runs on this path, so end the fresh provider's
        // snoop without replacing the image's complete dependency evidence.
        self.store
            .data_mut()
            .provider
            .finish_initialization_reads()?;
        self.context_read_set = Some(image.context_read_set.clone());
        self.all_entries_prepared = true;
        Ok(true)
    }

    pub(super) async fn capture_prepared_image(
        &mut self,
    ) -> anyhow::Result<Option<Arc<GeneratedPreparedImage>>> {
        let (Some(maintenance), Some(states), Some(read_set), Some(generation)) = (
            self.memory_maintenance.as_mut(),
            self.initialized_state.as_ref(),
            self.context_read_set.as_ref(),
            self.routed.generation.as_ref(),
        ) else {
            // Older and non-image-compatible guests retain ordinary preparation.
            return Ok(None);
        };
        let generated = generated_state(self.store.data()).map_err(wasmtime_anyhow)?;
        anyhow::ensure!(
            self.all_entries_prepared
                && self.runtime_state == GeneratedRuntimeState::Live
                && self.store.data().provider.is_preparation()
                && generated.capability_bridge.is_unissued()
                && generated.request_handle.is_none()
                && generated.values.live_handle_count() == 0
                && generated.async_operations.is_empty()
                && !generated.runtime_reuse_contaminated
                && !generated.discard_after_caught_initialization_failure
                && !self.store.data().guest_developer_error
                && self.store.data().developer_error.is_none()
                && !self.store.has_pending_exception(),
            "initialized image capture requires completed request-independent preparation"
        );
        if generation.retired.load(Ordering::Acquire) {
            return Ok(None);
        }
        self.runtime_state = GeneratedRuntimeState::Poisoned;
        let prepared = maintenance
            .run(
                &mut self.store,
                self.memory,
                MaintenanceOperation::PrepareImage,
            )
            .await?;
        self.runtime_state = GeneratedRuntimeState::Live;
        match prepared {
            MaintenanceResult::Complete(_) => {},
            MaintenanceResult::Deferred | MaintenanceResult::Unsupported => return Ok(None),
            MaintenanceResult::Partial(_)
            | MaintenanceResult::Collection(_)
            | MaintenanceResult::WeakSlotsTrimmed => {
                anyhow::bail!("invalid initialized image preparation result")
            },
        }
        // The authenticated layouts exclude reference globals, table mutation,
        // GC aggregates and reference-valued function interfaces. The registered
        // host imports create no Wasmtime GC roots, and no guest call remains
        // suspended here. Numeric host handles are checked separately below.
        // Caught exceptions can leave unreachable objects and allocated GC
        // capacity; neither is image state. A pending exception is still a
        // Store-owned root. The separate heap stays private and charged; it is
        // neither copied into the image nor restored in another Store.
        anyhow::ensure!(
            !self.store.has_pending_exception(),
            "image preparation left a pending Wasm exception"
        );
        let generated = generated_state(self.store.data()).map_err(wasmtime_anyhow)?;
        // A collection retry can catch a failed allocation and still return a
        // successful maintenance record. That cannot certify an image.
        generated
            .memory_limiter
            .ensure_memory_available()
            .map_err(wasmtime_anyhow)?;
        anyhow::ensure!(
            generated.capability_bridge.is_unissued()
                && generated.values.live_handle_count() == 0
                && generated.async_operations.is_empty()
                && !generated.runtime_reuse_contaminated
                && !generated.discard_after_caught_initialization_failure
                && !self.store.data().guest_developer_error
                && self.store.data().developer_error.is_none(),
            "image maintenance retained request resources or failed preparation"
        );
        let memory_bytes = self.memory.data_size(&self.store);
        let range_bytes = std::mem::size_of_val(read_set.range_hashes.as_slice()).saturating_add(
            read_set
                .range_hashes
                .iter()
                .map(|(_, index, table, intervals, _)| {
                    index
                        .heap_size()
                        .saturating_add(table.heap_size())
                        .saturating_add(intervals.heap_size())
                })
                .fold(0usize, usize::saturating_add),
        );
        let GeneratedPoolIdentity::Graph(identity) = self.pool_identity.as_ref() else {
            anyhow::bail!("initialized image requires an authenticated graph identity");
        };
        let identity_bytes = std::mem::size_of::<GeneratedPoolIdentity>()
            .saturating_add(2 * std::mem::size_of::<usize>())
            .saturating_add(identity.generation_sha256.capacity())
            .saturating_add(identity.graph_sha256.capacity())
            .saturating_add(identity.engine_compatibility_sha256.capacity())
            .saturating_add(
                identity
                    .shared
                    .capacity()
                    .saturating_mul(std::mem::size_of::<GeneratedAotModuleIdentity>()),
            )
            .saturating_add(
                std::iter::once(&identity.base)
                    .chain(&identity.shared)
                    .chain(std::iter::once(&identity.leaf))
                    .map(|module| {
                        module
                            .engine_compatibility_sha256
                            .capacity()
                            .saturating_add(module.serialized_module_sha256.capacity())
                    })
                    .fold(0usize, usize::saturating_add),
            );
        let context_metadata_bytes = read_set
            .read_set
            .read_set()
            .heap_size()
            .saturating_add(range_bytes);
        let metadata_bytes = states
            .iter()
            .map(|state| state.captured_bytes())
            .fold(0usize, usize::saturating_add)
            .saturating_add(std::mem::size_of::<GeneratedPreparedImage>())
            .saturating_add(2 * std::mem::size_of::<usize>())
            .saturating_add(identity_bytes)
            .saturating_add(64)
            .saturating_add(context_metadata_bytes);
        let charge = match self.image_controller()?.try_charge_module(
            memory_bytes
                .checked_add(metadata_bytes)
                .context("initialized image charge overflow")?,
        ) {
            Ok(charge) => charge,
            Err(
                ModuleMemoryAdmissionError::Pressure
                | ModuleMemoryAdmissionError::SoftBudget
                | ModuleMemoryAdmissionError::HardBudget
                | ModuleMemoryAdmissionError::FixedBytesExceedHardBudget,
            ) => return Ok(None),
        };
        self.runtime_state = GeneratedRuntimeState::Poisoned;
        let mut modules = Vec::with_capacity(states.len());
        for state in states {
            modules.push(state.capture(&mut self.store).await?);
        }
        let memory = match InitializedMemoryImage::capture(self.memory.data(&self.store)) {
            Ok(memory) => memory,
            Err(ImageCaptureError::Unavailable { .. }) => {
                // Capture has not attached any mapping or hook. The initialized
                // ordinary Store is still usable; do not retry capture here.
                self.runtime_state = GeneratedRuntimeState::Live;
                return Ok(None);
            },
            Err(error) => return Err(error.into()),
        };
        self.runtime_state = GeneratedRuntimeState::Live;
        let identity_sha256 = format!("{:x}", Sha256::digest(format!("{:?}", self.pool_identity)));
        charge.describe(
            "initialized_image",
            identity_sha256.clone(),
            Some(memory.stored_bytes),
            false,
        );
        let clone_host_bytes = generated_state(self.store.data())
            .map_err(wasmtime_anyhow)?
            .memory_permit
            .as_ref()
            .context("prepared image lost its memory admission")?
            .current_bytes()
            .1;
        let image = Arc::new(GeneratedPreparedImage {
            memory,
            modules,
            context_read_set: read_set.clone(),
            context_metadata_bytes,
            wasi_monotonic_epoch: self.store.data().wasi_monotonic_epoch,
            wasi_cpu_time: self.store.data().wasi_cpu_clock.current(),
            pool_identity: Arc::clone(&self.pool_identity),
            generation: Arc::downgrade(generation),
            identity_sha256,
            clone_host_bytes,
            charge,
        });
        if !self.install_prepared_image(Arc::clone(&image)).await? {
            return Ok(None);
        }
        // The source itself becomes a COW view; retaining its original private
        // preparation heap would defeat sharing even before the first clone.
        // An attachment failure retains an uncached owner through Store drop.
        // Publication waits for the caller's reconciliation and successful
        // finalization. A failed or cancelled producer keeps no cache owner.
        Ok(Some(image))
    }
}
