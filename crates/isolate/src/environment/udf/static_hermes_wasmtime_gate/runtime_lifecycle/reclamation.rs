use super::{
    super::guest_memory_maintenance::{
        PressureCollection,
        ReclamationOutcome,
    },
    *,
};
use crate::environment::udf::generated_wasm_memory::InitializedRetention;

register_convex_counter!(
    GENERATED_WASM_RECLAIMED_PRIVATE_BYTES_TOTAL,
    "Private initialized-memory bytes discarded at successful invocation boundaries",
    &["operation"]
);

impl<RT: Runtime> GeneratedReusableInstance<RT> {
    /// Run inside the current measured call, after a successful handler and
    /// Store reconciliation, before capability revocation, opaque-value cleanup
    /// or timeout removal.
    /// False means dispose after normal result finalization; it is not a retry.
    pub(super) async fn reclaim_after_success(&mut self) -> wasmtime::Result<bool> {
        if self.initialized_memory.is_none() {
            return Ok(true);
        }
        let state = self.store.data();
        let generated = generated_state(state)?;
        if self.runtime_state != GeneratedRuntimeState::Live
            || generated.runtime_reuse_contaminated
            || generated.discard_after_caught_initialization_failure
            || state.guest_developer_error
            || state.developer_error.is_some()
        {
            return Ok(false);
        }
        generated.memory_limiter.ensure_memory_available()?;
        let decision = generated
            .memory_permit
            .as_ref()
            .context("reclamation lost its invocation memory permit")
            .map_err(WasmtimeError::from_anyhow)?
            .initialized_retention();
        let target = match decision {
            InitializedRetention::Retain => {
                if self.memory_maintenance.as_ref().is_some_and(|maintenance| {
                    maintenance.version.is_some_and(|version| version < 3)
                }) {
                    return Ok(true);
                }
                None
            },
            InitializedRetention::Dispose => return Ok(false),
            InitializedRetention::Reclaim { target_bytes } => Some(target_bytes),
        };
        let initialized = self.initialized_memory.as_ref().unwrap();
        let pages = initialized
            .pages
            .as_ref()
            .context("reclamation requires an attached recorder")
            .map_err(WasmtimeError::from_anyhow)?;
        let maintenance = self
            .memory_maintenance
            .as_mut()
            .context("initialized image lost guest memory maintenance")
            .map_err(WasmtimeError::from_anyhow)?;
        let _timer = GatePhaseTimer::new("initialized_memory_reclamation");
        // Exclusive Store ownership covers enumeration, collection and discard.
        // No controller/pool guard crosses this await, and no new fuel or
        // deadline is supplied. Finalizer resources still belong to this call.
        let result = unsafe {
            maintenance
                .reclaim_for_reuse(&mut self.store, self.memory, pages, target)
                .await
        };
        // Only an actual maintenance operation needs another sample. The
        // ordinary successful path already reconciled once before selection.
        let reconciliation = self.reconcile_memory();
        // Reconciliation still samples exposure after a trap. Preserve the
        // original typed fuel/interrupt/memory failure if both operations fail.
        let (reclaimed_bytes, collection, reusable) = match result {
            Ok(ReclamationOutcome::Retained {
                reclaimed_bytes,
                collection,
            }) => (reclaimed_bytes, collection, true),
            Ok(ReclamationOutcome::InsufficientPayload {
                reclaimed_bytes,
                collection,
            }) => {
                log_instance_pool_event("generated_reclamation_insufficient");
                (reclaimed_bytes, collection, false)
            },
            Ok(ReclamationOutcome::Deferred) => {
                log_instance_pool_event("generated_reclamation_deferred");
                (0, PressureCollection::Unnecessary, false)
            },
            Ok(ReclamationOutcome::Unsupported) => {
                log_instance_pool_event("generated_reclamation_unsupported");
                (0, PressureCollection::Unnecessary, false)
            },
            Err(error) => {
                self.runtime_state = GeneratedRuntimeState::Poisoned;
                return Err(WasmtimeError::from_anyhow(error)
                    .context(StaticHermesWasmExecutionFailure::RuntimeCleanup));
            },
        };
        // Partial release still discarded real pages even if aggregate pressure
        // or subsequent reconciliation requires disposal. Count it before
        // propagating that failure; the zero-release path allocates no labels.
        if reclaimed_bytes != 0 {
            log_counter_with_labels(
                &GENERATED_WASM_RECLAIMED_PRIVATE_BYTES_TOTAL,
                reclaimed_bytes as u64,
                vec![StaticMetricLabel::new(
                    "operation",
                    match collection {
                        PressureCollection::Unnecessary => "free_payload",
                        PressureCollection::Young => "young",
                        PressureCollection::Full => "full",
                    },
                )],
            );
        }
        reconciliation.map_err(WasmtimeError::from_anyhow)?;
        Ok(reusable)
    }
}
