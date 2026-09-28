use std::io::{
    self,
    Write,
};

use super::*;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod prepared_image;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod reclamation;

#[derive(Default)]
pub(super) struct GeneratedPreparedImageCache {
    // The existing generation preparation flight serializes producers. This
    // lock only copies/removes ownership; capture, validation and mapping run
    // outside it, and instances retain their image after cache invalidation.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    image: parking_lot::Mutex<Option<Arc<prepared_image::GeneratedPreparedImage>>>,
}

impl GeneratedPreparedImageCache {
    pub(super) fn clone_construction_peak_bytes(&self) -> Option<usize> {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            self.image
                .lock()
                .as_ref()
                .map(|image| image.clone_construction_peak_bytes())
        }
        #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
        {
            None
        }
    }
}

const DETACHED_GENERATED_RUNTIME_CLEANUP_CPU_ADMISSION_TIMEOUT: Duration =
    GENERATED_RUNTIME_DESTROY_TIMEOUT;
const MAX_RETAINED_WASM_TRAP_FRAMES: usize = 64;
const MAX_RETAINED_WASM_TRAP_HOST_OPERATIONS: usize = 32;
const STATIC_HERMES_UNCAUGHT_EXCEPTION_STDERR: &[u8] = b"SH: uncaught exception";
const STATIC_HERMES_HEAP_OOM_MARKER: &[u8] = b"hermes::GCBase::oom";

struct JsonSizeWriter {
    bytes: usize,
}

impl Write for JsonSizeWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_json_size(value: &impl serde::Serialize) -> anyhow::Result<usize> {
    let mut writer = JsonSizeWriter { bytes: 0 };
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.bytes)
}

fn wasmtime_trap_stderr_classification(stderr: &[u8]) -> StaticHermesWasmTrapStderrClassification {
    if stderr.is_empty() {
        StaticHermesWasmTrapStderrClassification::Empty
    } else if stderr
        .windows(STATIC_HERMES_HEAP_OOM_MARKER.len())
        .any(|window| window == STATIC_HERMES_HEAP_OOM_MARKER)
    {
        StaticHermesWasmTrapStderrClassification::HermesHeapOutOfMemory
    } else if stderr == STATIC_HERMES_UNCAUGHT_EXCEPTION_STDERR {
        StaticHermesWasmTrapStderrClassification::StaticHermesUncaughtException
    } else {
        StaticHermesWasmTrapStderrClassification::Unclassified
    }
}

fn same_wasmtime_module(left: &Module, right: &Module) -> bool {
    left.image_range() == right.image_range()
}

struct GeneratedModuleError {
    // A Wasmtime backtrace owns Module clones. Destroy the error before the
    // charged owners, including when a retired Store has already been dropped.
    error: anyhow::Error,
    _routed: Arc<GeneratedRoutedModule>,
}

impl std::fmt::Debug for GeneratedModuleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.error, f)
    }
}

impl std::fmt::Display for GeneratedModuleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for GeneratedModuleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

fn retain_generated_error_modules(
    error: anyhow::Error,
    routed: &Arc<GeneratedRoutedModule>,
) -> anyhow::Error {
    if !error.is::<WasmBacktrace>() {
        return error;
    }
    let failure = error
        .downcast_ref::<StaticHermesWasmExecutionFailure>()
        .cloned();
    let export_diagnostic = error
        .downcast_ref::<StaticHermesGeneratedExportDiagnostic>()
        .copied();
    let metadata = error.downcast_ref::<ErrorMetadata>().cloned();
    let host_diagnostic = error
        .downcast_ref::<StaticHermesWasmHostInvariantDiagnostic>()
        .copied();
    let non_retryable = error
        .downcast_ref::<GeneratedNonRetryableFailure>()
        .copied();
    let memory_failure = error.downcast_ref::<GeneratedGuestMemoryLimit>().copied();
    let trap = error.downcast_ref::<Trap>().copied();
    let initialization = error.is::<GeneratedEngineInitializationFailure>();
    let mut error = anyhow::Error::new(GeneratedModuleError {
        error,
        _routed: Arc::clone(routed),
    });
    // Preserve the public classification's direct anyhow downcast as well as
    // the original source chain. Raw Wasmtime traps have already been classified.
    if let Some(metadata) = metadata {
        error = error.context(metadata);
    }
    if let Some(diagnostic) = host_diagnostic {
        error = error.context(diagnostic);
    }
    if let Some(non_retryable) = non_retryable {
        error = error.context(non_retryable);
    }
    if let Some(memory_failure) = memory_failure {
        error = error.context(memory_failure);
    }
    if let Some(trap) = trap {
        error = error.context(trap);
    }
    if initialization {
        error = error.context(GeneratedEngineInitializationFailure);
    }
    if let Some(export_diagnostic) = export_diagnostic {
        error = error.context(export_diagnostic);
    }
    if let Some(failure) = failure {
        error = error.context(failure);
    }
    error
}

fn account_generated_initial_static_memory<RT: Runtime>(
    store: &mut Store<HostState<RT>>,
    module: &Module,
    evidence: &OnceLock<super::immutable_static::VerifiedStaticMemory>,
    memory: Memory,
) -> anyhow::Result<()> {
    let observed;
    let verified = match evidence.get() {
        Some(verified) => verified,
        None => {
            // Compatibility graphs may give the provider's memory another
            // export name. Only a certificate requires the canonical name;
            // uncertified graphs keep their existing layout contract.
            let ty = memory.ty(&*store);
            observed = super::immutable_static::VerifiedStaticMemory {
                initial_bytes: usize::try_from(
                    ty.minimum()
                        .checked_mul(ty.page_size())
                        .context("generated initial memory size overflow")?,
                )?,
                shared_bytes: super::immutable_static::shared_bytes(store, module, memory)?,
            };
            // Never block other constructors behind a procfs scan or hold a
            // pool/cache/controller lock while collecting mapping evidence.
            // Concurrent first constructors may each observe their own mapping.
            // No get_or_init closure or verifier lock lies on this path.
            let _ = evidence.set(observed);
            &observed
        },
    };
    generated_state_mut(store.data_mut())?
        .memory_permit
        .as_ref()
        .context("generated memory permit disappeared before static accounting")?
        .credit_shared_static(verified.shared_bytes)
}

fn wasmtime_trap_module_role(
    routed: &GeneratedRoutedModule,
    module: &Module,
) -> (StaticHermesWasmTrapModuleRole, Option<u32>) {
    if let Some(graph) = &routed.emscripten_graph {
        if same_wasmtime_module(module, &graph.base.module) {
            return (StaticHermesWasmTrapModuleRole::Base, Some(0));
        }
        if let Some(ordinal) = graph
            .shared
            .iter()
            .position(|shared| same_wasmtime_module(module, &shared.module))
        {
            return (
                StaticHermesWasmTrapModuleRole::Shared,
                Some(u32::try_from(ordinal).expect("Wasm graph has more than u32 shared modules")),
            );
        }
        if same_wasmtime_module(module, &graph.leaf.module) {
            return (StaticHermesWasmTrapModuleRole::Leaf, Some(0));
        }
    } else if let Some(graph) = &routed.graph {
        if let Some(ordinal) = graph
            .dependencies
            .iter()
            .position(|dependency| same_wasmtime_module(module, &dependency.module.module))
        {
            return (
                StaticHermesWasmTrapModuleRole::Dependency,
                Some(
                    u32::try_from(ordinal)
                        .expect("Wasm graph has more than u32 dependency modules"),
                ),
            );
        }
        if same_wasmtime_module(module, &routed.module) {
            return (StaticHermesWasmTrapModuleRole::Leaf, Some(0));
        }
    } else if same_wasmtime_module(module, &routed.module) {
        return (StaticHermesWasmTrapModuleRole::Route, Some(0));
    }
    (StaticHermesWasmTrapModuleRole::Unknown, None)
}

fn wasmtime_trap_diagnostic<RT: Runtime>(
    error: &WasmtimeError,
    routed: &GeneratedRoutedModule,
    store: &Store<HostState<RT>>,
    reused_instance: bool,
    execution_fuel: u64,
) -> Option<StaticHermesWasmTrapDiagnostic> {
    let trap = error.downcast_ref::<Trap>()?;
    let backtrace = error.downcast_ref::<WasmBacktrace>();
    let frame_count = backtrace.map_or(0, |backtrace| backtrace.frames().len());
    let frames = backtrace
        .into_iter()
        .flat_map(WasmBacktrace::frames)
        .take(MAX_RETAINED_WASM_TRAP_FRAMES)
        .map(|frame| {
            let (module_role, module_ordinal) = wasmtime_trap_module_role(routed, frame.module());
            StaticHermesWasmTrapFrame {
                module_role,
                module_ordinal,
                function_index: frame.func_index(),
                function_offset: frame
                    .func_offset()
                    .and_then(|offset| u64::try_from(offset).ok()),
                module_offset: frame
                    .module_offset()
                    .and_then(|offset| u64::try_from(offset).ok()),
            }
        })
        .collect();
    let state = store.data();
    let generated = generated_state(state)
        .expect("generated invocation state disappeared while retaining a Wasmtime trap");
    let memory_permit = generated
        .memory_permit
        .as_ref()
        .expect("generated memory permit disappeared while retaining a Wasmtime trap");
    let (current_guest_bytes, current_host_bytes) = memory_permit.current_bytes();
    let trace = state
        .provider
        .host_operation_trace_entries()
        .expect("generated invocation context disappeared while retaining a Wasmtime trap");
    let host_operation_count = trace.map_or(0, <[_]>::len);
    let host_operations = trace
        .into_iter()
        .flat_map(|entries| {
            entries[entries
                .len()
                .saturating_sub(MAX_RETAINED_WASM_TRAP_HOST_OPERATIONS)..]
                .iter()
        })
        .map(|entry| StaticHermesWasmTrapHostOperationDiagnostic {
            operation: entry.operation().diagnostic_key(),
            status: entry.status().diagnostic_key(),
        })
        .collect();
    let limits = generated.manifest.limits();
    let stderr_byte_count =
        u64::try_from(state.stderr.len()).expect("generated stderr length exceeds u64");
    Some(StaticHermesWasmTrapDiagnostic::from_capsule(
        trap.into(),
        frames,
        frame_count > MAX_RETAINED_WASM_TRAP_FRAMES,
        state
            .provider
            .invocation_correlation()
            .expect("generated invocation context disappeared while retaining a Wasmtime trap"),
        reused_instance,
        StaticHermesWasmTrapFuelDiagnostic {
            limit: execution_fuel,
            remaining: store.get_fuel().ok(),
        },
        StaticHermesWasmTrapResourceDiagnostic {
            operation_count: generated.operation_count,
            operation_limit: limits.max_operation_count(),
            value_handle_count: u64::try_from(generated.values.live_handle_count())
                .expect("generated value handle count exceeds u64"),
            value_handle_limit: u64::from(limits.max_value_handles()),
            current_guest_bytes: u64::try_from(current_guest_bytes)
                .expect("generated guest memory exceeds u64"),
            guest_byte_limit: limits.max_guest_memory_bytes(),
            current_host_bytes: u64::try_from(current_host_bytes)
                .expect("generated host memory exceeds u64"),
            host_byte_limit: limits.max_host_owned_bytes(),
        },
        trace.is_some(),
        host_operations,
        host_operation_count > MAX_RETAINED_WASM_TRAP_HOST_OPERATIONS,
        StaticHermesWasmTrapStderrDiagnostic {
            classification: wasmtime_trap_stderr_classification(&state.stderr),
            byte_count: stderr_byte_count,
            sha256: format!("{:x}", Sha256::digest(&state.stderr)),
        },
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GeneratedRuntimeState {
    Inactive,
    Live,
    // A failed guest call may leave native allocator or VM invariants broken.
    // Only host-side resource cleanup and Store destruction are safe afterward.
    Poisoned,
}

// Wasmtime drops Store data before deallocating linear memory and the GC heap.
// The data owns the invocation permit on early errors and cancellation, so use
// into_data to destroy those allocations before releasing the ledger entry.
pub(super) struct GeneratedStore<RT: Runtime>(Option<Store<HostState<RT>>>);

impl<RT: Runtime> From<Store<HostState<RT>>> for GeneratedStore<RT> {
    fn from(store: Store<HostState<RT>>) -> Self {
        Self(Some(store))
    }
}

impl<RT: Runtime> std::ops::Deref for GeneratedStore<RT> {
    type Target = Store<HostState<RT>>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("generated Store already destroyed")
    }
}

impl<RT: Runtime> std::ops::DerefMut for GeneratedStore<RT> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("generated Store already destroyed")
    }
}

impl<RT: Runtime> wasmtime::AsContext for GeneratedStore<RT> {
    type Data = HostState<RT>;

    fn as_context(&self) -> wasmtime::StoreContext<'_, Self::Data> {
        wasmtime::AsContext::as_context(&**self)
    }
}

impl<RT: Runtime> wasmtime::AsContextMut for GeneratedStore<RT> {
    fn as_context_mut(&mut self) -> wasmtime::StoreContextMut<'_, Self::Data> {
        wasmtime::AsContextMut::as_context_mut(&mut **self)
    }
}

impl<RT: Runtime> Drop for GeneratedStore<RT> {
    fn drop(&mut self) {
        drop(
            self.0
                .take()
                .expect("generated Store destroyed twice")
                .into_data(),
        );
    }
}

pub(super) struct GeneratedReusableInstance<RT: Runtime> {
    pub(super) route_identity: GeneratedRouteIdentity,
    pub(super) pool_identity: Arc<GeneratedPoolIdentity>,
    pub(super) memory_slot_id: GeneratedSlotId,
    #[cfg(any(test, feature = "testing"))]
    pub(super) memory_controller: Arc<GeneratedMemoryController>,
    pub(super) store: GeneratedStore<RT>,
    memory: Memory,
    execution_hook_installed: bool,
    pub(super) select_entry: Option<TypedFunc<i64, i32>>,
    pub(super) prepare_selected_entry: Option<TypedFunc<(), i32>>,
    pub(super) memory_maintenance: Option<super::guest_memory_maintenance::GuestMemoryMaintenance>,
    initialized_state: Option<Vec<super::initialized_state::BoundModuleState>>,
    pub(super) run: TypedFunc<(), i32>,
    pub(super) destroy: TypedFunc<(), ()>,
    #[cfg(test)]
    pub(super) arm_formatter_initialization_failure: Option<TypedFunc<(), i32>>,
    #[cfg(test)]
    pub(super) partial_initialization_trace: Option<TypedFunc<(), i64>>,
    pub(super) context_read_set: Option<ContextReadSet>,
    pub(super) all_entries_prepared: bool,
    pub(super) pending_initialization_logs: LogLines,
    pub(super) id: u64,
    pub(super) runtime_state: GeneratedRuntimeState,
    pub(super) pool_checked_out: bool,
    pub(super) idle_since: Instant,
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    initialized_memory: Option<prepared_image::GeneratedInitializedMemory>,
    // Rust drops fields in declaration order. The Store retains executable
    // modules and COW images, so their charge owners must outlive its teardown.
    pub(super) routed: Arc<GeneratedRoutedModule>,
}

impl<RT: Runtime> GeneratedReusableInstance<RT> {
    fn requires_guest_destruction(&self) -> bool {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        if self.initialized_memory.is_some() {
            // External resources belong to HostState and its opaque tables;
            // guest wrappers contain handles, not independent host owners.
            // Store disposal frees guest allocations after ordinary host cleanup.
            // No guest destructor may spend a released return-boundary grant.
            return false;
        }
        self.runtime_state == GeneratedRuntimeState::Live
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    fn observe_initialized_memory_disposal(&self) {
        if let Some(pages) = self
            .initialized_memory
            .as_ref()
            .and_then(|initialized| initialized.pages.as_ref())
            && let Some(permit) = self
                .store
                .data()
                .generated
                .as_ref()
                .and_then(|generated| generated.memory_permit.as_ref())
        {
            let observed = pages.observation();
            permit.observe_initialized_memory_disposal(observed.peak_bytes, observed.poisoned);
        }
    }

    fn reconcile_memory(&mut self) -> anyhow::Result<()> {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        if let Some(initialized) = &self.initialized_memory {
            // Native write denial can poison the mapping without passing
            // through the Store limiter. Even if guest code catches a trap,
            // teardown must not reenter this mapping or return it to the pool.
            if initialized
                .pages
                .as_ref()
                .is_none_or(|pages| pages.observation().poisoned)
            {
                self.runtime_state = GeneratedRuntimeState::Poisoned;
                // Private-page credit is shared admission, not an engine
                // heap allocation limit. Its denial cannot authorize V8.
                generated_state_mut(self.store.data_mut())
                    .map_err(wasmtime_anyhow)?
                    .memory_limiter
                    .growth_failure = Some(GeneratedGuestMemoryLimit::Aggregate);
            }
        }
        let linear_bytes = self.memory.data_size(&self.store);
        let gc_heap_bytes = self.store.gc_heap_capacity();
        let limiter = &mut generated_state_mut(self.store.data_mut())
            .map_err(wasmtime_anyhow)?
            .memory_limiter;
        let result = limiter.reconcile(linear_bytes, gc_heap_bytes);
        if result.is_err() {
            self.runtime_state = GeneratedRuntimeState::Poisoned;
        }
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        if result.is_ok()
            && let Some(pages) = self
                .initialized_memory
                .as_ref()
                .and_then(|initialized| initialized.pages.as_ref())
        {
            if let Some(loan) = pages.overflow_loan() {
                let transferred = limiter.absorb_initialized_loan(loan);
                // The Store is exclusive and no guest entry separates the
                // accounting transfer from its recorder allowance update.
                unsafe { pages.accept_overflow_transfer(transferred) };
            }
            let observed = pages.observation();
            limiter.reconcile_initialized_memory(
                linear_bytes,
                observed.exposed_bytes,
                observed.peak_bytes,
                observed.allowance_bytes,
                observed.poisoned,
            );
        }
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        if result.is_err() {
            // Handled failures detach the permit before Store disposal. Sample
            // now because Drop can no longer publish this unsettled exposure.
            self.observe_initialized_memory_disposal();
        }
        result
    }
}

impl<RT: Runtime> Drop for GeneratedReusableInstance<RT> {
    fn drop(&mut self) {
        // Cancellation can drop a suspended call before reconciliation. Sample
        // before into_data frees memory and publishes the permit's observation.
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        self.observe_initialized_memory_disposal();
        if self.requires_guest_destruction() {
            log_instance_pool_event("generated_undestroyed");
        } else if self.pool_checked_out {
            log_instance_pool_event("generated_discarded");
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ReadAccounting {
    pub(super) documents: usize,
    pub(super) bytes: usize,
    pub(super) intervals: usize,
}

pub(super) struct InvocationOutput<RT: Runtime> {
    pub(super) outcome: InvocationOutcome,
    pub(super) function_outcome: Option<FunctionOutcome>,
    // Preparation has no function outcome to carry its module-initialization logs.
    pub(super) preparation_log_lines: Option<LogLines>,
    pub(super) transaction: Transaction<RT>,
    #[cfg(any(test, feature = "testing"))]
    pub(super) host_operation_error: Option<HostOperationErrorV1>,
    #[cfg(any(test, feature = "testing"))]
    pub(super) function_result: Option<JsonValue>,
    #[cfg(any(test, feature = "testing"))]
    pub(super) opaque_live_handles: usize,
    #[cfg(any(test, feature = "testing"))]
    pub(super) opaque_current_bytes: usize,
    #[cfg(any(test, feature = "testing"))]
    pub(super) operation_count: u64,
    #[cfg(any(test, feature = "testing"))]
    pub(super) capability_revoked: bool,
    #[cfg(any(test, feature = "testing"))]
    pub(super) runtime_reuse_contaminated: bool,
    #[cfg(test)]
    pub(super) first_developer_error_index: Option<usize>,
    #[cfg(test)]
    pub(super) read_accounting: ReadAccounting,
    #[cfg(any(test, feature = "testing"))]
    pub(super) active_time: Duration,
    #[cfg(any(test, feature = "testing"))]
    pub(super) observed_identity: bool,
    #[cfg(any(test, feature = "testing"))]
    pub(super) observed_rng: bool,
    #[cfg(any(test, feature = "testing"))]
    pub(super) observed_time: bool,
    #[cfg(any(test, feature = "testing"))]
    pub(super) log_lines: LogLines,
    #[cfg(any(test, feature = "testing"))]
    pub(super) journal: QueryJournal,
    #[cfg(any(test, feature = "testing"))]
    pub(super) audit_log_lines: AuditLogLines,
    #[cfg(any(test, feature = "testing"))]
    pub(super) syscall_trace: udf::SyscallTrace,
}

pub(super) struct GeneratedExecutionOutput<RT: Runtime> {
    pub(super) invocation: InvocationOutput<RT>,
    pub(super) reusable_instance: Option<GeneratedReusableInstance<RT>>,
    pub(super) memory_permit: Option<InvocationMemoryPermit>,
    pub(super) terminal_memory_outcome: TerminalMemoryOutcome,
    pub(super) system_error: Option<anyhow::Error>,
    #[cfg(test)]
    pub(super) runtime_id: u64,
    #[cfg(test)]
    pub(super) partial_initialization_trace: Option<i64>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum GeneratedExecutionPurpose {
    Invocation,
    Preparation,
}

impl<RT: Runtime> InvocationOutput<RT> {
    pub(super) fn into_routed_result(
        self,
        system_error: Option<anyhow::Error>,
        shadow: bool,
    ) -> anyhow::Result<(Transaction<RT>, FunctionOutcome)> {
        match self.outcome {
            InvocationOutcome::SystemTimeout => {
                return Err(system_error
                    .context("generated Wasm system timeout lost its classified error")?);
            },
            InvocationOutcome::SystemError => {
                return Err(system_error
                    .context("generated Wasm system outcome lost its internal error")?
                    .context("generated Wasm execution failed"));
            },
            // Resource-limited verification did not establish semantic agreement
            // or divergence. In particular, initialization can end before handler
            // read capture begins. Preserve the typed cause before observation
            // validation; never infer it from a user-controlled JS error message.
            // These outcomes come from the host timer, never from guest error
            // text. Keep them typed for a disposed primary's bounded V8 retry.
            InvocationOutcome::InitializationTimeout => {
                return Err(StaticHermesWasmExecutionFailure::InitializationTimeout.into());
            },
            InvocationOutcome::ActiveTimeout => {
                return Err(StaticHermesWasmExecutionFailure::ExecutionTimeout.into());
            },
            InvocationOutcome::FuelExhausted if shadow => {
                return Err(StaticHermesWasmExecutionFailure::InstructionBudget.into());
            },
            InvocationOutcome::Success
            | InvocationOutcome::DeveloperError(_)
            | InvocationOutcome::FuelExhausted => {},
        }
        // Application errors and instruction limits retain their canonical
        // function result and cannot acquire runtime-retry authority.
        Ok((
            self.transaction,
            self.function_outcome
                .context("generated Wasm invocation lost its canonical function outcome")?,
        ))
    }
}

fn merge_generated_execution_error(
    primary: &mut Option<anyhow::Error>,
    secondary: anyhow::Error,
    secondary_context: &'static str,
) {
    // A secondary finalization/cleanup failure invalidates retry authority even
    // when the original diagnostic remains the useful primary failure.
    *primary = Some(
        match primary.take() {
            Some(primary) => primary.context(format!("{secondary_context}: {secondary:#}")),
            None => secondary.context(secondary_context),
        }
        .context(GeneratedNonRetryableFailure::Cleanup),
    );
}

fn generated_error_timeout_reason(
    reason: Option<GeneratedTimeoutReason>,
    error: &anyhow::Error,
) -> Option<GeneratedTimeoutReason> {
    // The timer can fire after the call already failed, or while it is
    // returning from a host operation. Only its own interruption may replace
    // that call's error; independent failures retain their authority.
    if (!error.is::<crate::timeout::TimeoutExpired>()
        && error.downcast_ref::<Trap>() != Some(&Trap::Interrupt))
        || error.is::<GeneratedNonRetryableFailure>()
        || error.is::<ErrorMetadata>()
        || error.is::<GeneratedGuestMemoryLimit>()
    {
        return None;
    }
    match error.downcast_ref::<StaticHermesWasmExecutionFailure>() {
        Some(
            StaticHermesWasmExecutionFailure::RuntimeInitialization
            | StaticHermesWasmExecutionFailure::GeneratedExportDispatch
            | StaticHermesWasmExecutionFailure::GuestExecution
            | StaticHermesWasmExecutionFailure::WasmtimeTrap { .. },
        )
        | None => {},
        Some(
            StaticHermesWasmExecutionFailure::InitializationTimeout
            | StaticHermesWasmExecutionFailure::ExecutionTimeout
            | StaticHermesWasmExecutionFailure::InstructionBudget
            | StaticHermesWasmExecutionFailure::GuestMemoryLimit
            | StaticHermesWasmExecutionFailure::HostOwnedBytesLimit
            | StaticHermesWasmExecutionFailure::OpaqueHandleLimit
            | StaticHermesWasmExecutionFailure::OpaqueHandleSpaceExhausted
            | StaticHermesWasmExecutionFailure::AggregateMemoryLimit
            | StaticHermesWasmExecutionFailure::OperationLimit
            | StaticHermesWasmExecutionFailure::HostAbiInvariant
            | StaticHermesWasmExecutionFailure::ResultFinalization
            | StaticHermesWasmExecutionFailure::RuntimeCleanup,
        ) => return None,
    }
    reason
}

fn generated_system_timeout_error(max_duration: Duration) -> anyhow::Error {
    anyhow::anyhow!("Hit maximum total syscall duration (maximum duration: {max_duration:?})")
        .context(ErrorMetadata::bad_request(
            "SystemTimeoutError",
            SYSTEM_TIMEOUT_ERROR_MESSAGE,
        ))
}

fn generated_outcome_allows_runtime_reuse(
    outcome: &InvocationOutcome,
    guest_developer_error: bool,
    developer_error_reported: bool,
) -> bool {
    match outcome {
        InvocationOutcome::Success => true,
        InvocationOutcome::DeveloperError(_) => guest_developer_error || !developer_error_reported,
        InvocationOutcome::InitializationTimeout
        | InvocationOutcome::ActiveTimeout
        | InvocationOutcome::FuelExhausted
        | InvocationOutcome::SystemTimeout
        | InvocationOutcome::SystemError => false,
    }
}

fn arm_generated_store_epoch<RT: Runtime>(store: &mut Store<HostState<RT>>) {
    let continuous_poll_limit = *STATIC_HERMES_WASM_CONTINUOUS_POLL_LIMIT;
    store.epoch_deadline_callback(move |store| {
        let state = store.data();
        // The watchdog can be ready but unpolled while executor workers are in
        // guest code. Check its shared deadline before deciding to continue.
        // Idle destruction has already consumed the invocation's Timeout.
        if let Some(timeout) = &state.timeout {
            timeout.check_timeout();
        }
        let generated = state
            .generated
            .as_ref()
            .ok_or_else(|| host_invariant_error())?;
        generated.memory_limiter.ensure_memory_available()?;
        Ok(
            if generated
                .interrupt
                .should_interrupt(&generated.cancellation)
            {
                UpdateDeadline::Interrupt
            } else if state.wasi_cpu_clock.continuous_poll_elapsed() < continuous_poll_limit {
                UpdateDeadline::Continue(1)
            } else {
                UpdateDeadline::Yield(1)
            },
        )
    });
    store.set_epoch_deadline(1);
}

async fn destroy_generated_runtime<RT: Runtime>(
    routed: &GeneratedRoutedModule,
    instance: &mut GeneratedReusableInstance<RT>,
) -> anyhow::Result<()> {
    if !instance.requires_guest_destruction() {
        return Ok(());
    }
    #[cfg(any(test, feature = "testing"))]
    {
        let hooks = TEST_HOOKS.lock().as_ref().and_then(Weak::upgrade);
        let barrier = hooks.and_then(|hooks| {
            if Arc::ptr_eq(&hooks.metrics, &instance.store.data().metrics) {
                hooks.runtime_destruction_barrier.lock().take()
            } else {
                None
            }
        });
        if let Some((entered, release)) = barrier {
            // Keep the real worker and its work guard alive while the test
            // observes cancellation, before the cancelled Store is destroyed.
            // An assertion failure must not skip guest teardown. Abandoning
            // either channel releases this test-only hold, as does its deadline.
            if entered.send(()).is_ok() {
                let _ = tokio::time::timeout(Duration::from_secs(120), release).await;
            }
        }
    }
    let interrupt = Arc::clone(
        &instance
            .store
            .data()
            .generated
            .as_ref()
            .context("generated invocation state disappeared before runtime teardown")?
            .interrupt,
    );
    let destruction_deadline = tokio::time::Instant::now() + GENERATED_RUNTIME_DESTROY_TIMEOUT;
    interrupt.begin_destruction(destruction_deadline);
    instance.store.set_epoch_deadline(1);
    instance
        .store
        .set_fuel(routed.manifest.limits().execution_fuel())
        .map_err(wasmtime_anyhow)?;
    let result = {
        let cpu_clock = instance.store.data().wasi_cpu_clock.clone();
        let destroy_call = cpu_clock.within_poll_budget(
            cpu_clock.measure(instance.destroy.call_async(&mut instance.store, ())),
        );
        tokio::pin!(destroy_call);
        tokio::select! {
            result = &mut destroy_call => result,
            _ = tokio::time::sleep_until(destruction_deadline) => {
                match tokio::time::timeout(
                    GENERATED_RUNTIME_DESTROY_TIMEOUT,
                    &mut destroy_call,
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(host_invariant_error()),
                }
            },
        }
    };
    // A failed destroy still discards the Store. It must never enter the pool.
    instance.runtime_state = GeneratedRuntimeState::Inactive;
    record_runtime_destruction(if result.is_ok() {
        RuntimeDestructionOutcome::Completed
    } else {
        RuntimeDestructionOutcome::Failed
    });
    #[cfg(any(test, feature = "testing"))]
    if result.is_ok() {
        instance
            .store
            .data()
            .metrics
            .teardowns
            .fetch_add(1, Ordering::SeqCst);
    }
    result
        .map_err(wasmtime_anyhow)
        .context("generated Wasm runtime teardown failed")
}

pub(super) async fn discard_generated_instance_while_cpu_permit_held<RT: Runtime>(
    mut instance: GeneratedReusableInstance<RT>,
    _cpu_permit: &ConcurrencyPermit,
) -> anyhow::Result<()> {
    let routed = Arc::clone(&instance.routed);
    let result = destroy_generated_runtime(&routed, &mut instance)
        .await
        .map_err(|error| retain_generated_error_modules(error, &routed));
    instance.pool_checked_out = false;
    drop(instance);
    result
}

enum GeneratedRuntimeTeardownCpuAdmission {
    Held(ConcurrencyPermit),
    Acquire(ConcurrencyLimiter),
}

fn take_generated_runtime_teardown_cpu_admission<RT: Runtime>(
    instance: &mut GeneratedReusableInstance<RT>,
) -> anyhow::Result<GeneratedRuntimeTeardownCpuAdmission> {
    let state = instance.store.data_mut();
    if let Some(permit) = state.teardown_cpu_permit.take() {
        Ok(GeneratedRuntimeTeardownCpuAdmission::Held(permit))
    } else if let Some(timeout) = state.timeout.take() {
        Ok(GeneratedRuntimeTeardownCpuAdmission::Held(
            timeout
                .finish_with_permit()
                .context("generated Wasm teardown lost its active CPU permit")?,
        ))
    } else {
        Ok(GeneratedRuntimeTeardownCpuAdmission::Acquire(
            state.active_wasm_cpu_limiter.clone(),
        ))
    }
}

async fn acquire_generated_runtime_teardown_cpu_permit(
    limiter: &ConcurrencyLimiter,
) -> ConcurrencyPermit {
    // Idle teardown is maintenance, not an invocation resuming after a host
    // wait. It must queue with new invocations instead of repeatedly entering
    // the limiter's high-priority reacquisition lane.
    limiter
        .acquire(
            Arc::new("Static Hermes Wasm active CPU teardown".to_owned()),
            false,
        )
        .await
}

pub(super) async fn discard_generated_instance<RT: Runtime>(
    mut instance: GeneratedReusableInstance<RT>,
) -> anyhow::Result<()> {
    if !instance.requires_guest_destruction() {
        instance.pool_checked_out = false;
        drop(instance);
        return Ok(());
    }
    let admission = take_generated_runtime_teardown_cpu_admission(&mut instance)?;
    // Teardown invokes a guest export. A completed invocation carries its
    // original permit here; idle maintenance reacquires the same shared
    // limiter before it runs guest code.
    let _cpu_permit = match admission {
        GeneratedRuntimeTeardownCpuAdmission::Held(permit) => permit,
        GeneratedRuntimeTeardownCpuAdmission::Acquire(limiter) => {
            acquire_generated_runtime_teardown_cpu_permit(&limiter).await
        },
    };
    discard_generated_instance_while_cpu_permit_held(instance, &_cpu_permit).await
}

/// Discard a Store checked out before CPU admission failed without allowing its
/// detached cleanup to retain invocation resources forever. If active Wasm CPU
/// does not become available promptly, dropping the Store is the fail-closed
/// outcome: it has never re-entered the pool and guest teardown is not safe to
/// run without the shared CPU permit.
pub(super) async fn discard_generated_instance_after_cpu_admission_rejection<RT: Runtime>(
    mut instance: GeneratedReusableInstance<RT>,
) -> anyhow::Result<()> {
    if !instance.requires_guest_destruction() {
        instance.pool_checked_out = false;
        drop(instance);
        return Ok(());
    }
    let admission = take_generated_runtime_teardown_cpu_admission(&mut instance)?;
    let _cpu_permit = match admission {
        GeneratedRuntimeTeardownCpuAdmission::Held(permit) => permit,
        GeneratedRuntimeTeardownCpuAdmission::Acquire(limiter) => {
            match tokio::time::timeout(
                DETACHED_GENERATED_RUNTIME_CLEANUP_CPU_ADMISSION_TIMEOUT,
                acquire_generated_runtime_teardown_cpu_permit(&limiter),
            )
            .await
            {
                Ok(permit) => permit,
                Err(_) => {
                    // This Store is dirty and checked out. Dropping it cannot
                    // re-pool it, and prevents the detached task from keeping
                    // the invocation memory slot or application shadow guard.
                    drop(instance);
                    anyhow::bail!(
                        "generated Wasm detached teardown timed out waiting for active CPU \
                         capacity"
                    );
                },
            }
        },
    };
    discard_generated_instance_while_cpu_permit_held(instance, &_cpu_permit).await
}

pub(super) async fn discard_optional_generated_instance<RT: Runtime>(
    instance: Option<GeneratedReusableInstance<RT>>,
    error: anyhow::Error,
) -> anyhow::Error {
    let Some(instance) = instance else {
        return error;
    };
    match discard_generated_instance(instance).await {
        Ok(()) => error,
        Err(cleanup_error) => error
            .context(format!(
                "generated Wasm runtime cleanup also failed: {cleanup_error:#}"
            ))
            .context(GeneratedNonRetryableFailure::Cleanup),
    }
}

pub(super) async fn discard_optional_generated_instance_while_cpu_permit_held<RT: Runtime>(
    instance: Option<GeneratedReusableInstance<RT>>,
    cpu_permit: &ConcurrencyPermit,
    error: anyhow::Error,
) -> anyhow::Error {
    let Some(instance) = instance else {
        return error;
    };
    match discard_generated_instance_while_cpu_permit_held(instance, cpu_permit).await {
        Ok(()) => error,
        Err(cleanup_error) => error
            .context(format!(
                "generated Wasm runtime cleanup also failed: {cleanup_error:#}"
            ))
            .context(GeneratedNonRetryableFailure::Cleanup),
    }
}

#[cfg(any(test, feature = "testing"))]
pub async fn cleanup_static_hermes_test_instances<RT: Runtime>() -> anyhow::Result<usize> {
    let instances = {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        let selected = pool.take_all::<RT>();
        for instance in &selected {
            instance.memory_controller.mark_idle_slot_for_eviction(
                instance.memory_slot_id,
                IdleEvictionReason::TestCleanup,
            );
        }
        selected
    };
    let count = instances.len();
    let route_identities = instances
        .iter()
        .map(|instance| instance.route_identity.clone())
        .collect::<BTreeSet<_>>();
    let mut cleanup_error = None;
    for instance in instances {
        #[cfg(any(test, feature = "testing"))]
        instance.store.data().metrics.record_generated_pool_event(
            Some(instance.id),
            Some(instance.memory_slot_id.test_identity()),
            StaticHermesGeneratedPoolEvent::Evicted {
                trigger: StaticHermesGeneratedPoolEvictionTrigger::TestCleanup,
                reason: StaticHermesGeneratedPoolEvictionReason::TestCleanup,
            },
        );
        let controller = Arc::clone(&instance.memory_controller);
        let slot_id = instance.memory_slot_id;
        let discard_result = discard_generated_instance(instance).await;
        controller.finish_idle_eviction(slot_id, IdleEvictionReason::TestCleanup);
        if cleanup_error.is_none() {
            cleanup_error = discard_result.err();
        }
    }
    let removed = {
        let mut modules = GENERATED_ROUTED_MODULES.lock();
        route_identities
            .iter()
            .filter_map(|identity| {
                modules.remove(identity, "module_cache_evicted_generated_test_cleanup")
            })
            .collect::<Vec<_>>()
    };
    drop(removed);
    GENERATED_ROUTED_MODULES
        .lock()
        .evict_all_idle_shared_aot_modules();
    match cleanup_error {
        Some(error) => Err(error),
        None => Ok(count),
    }
}

pub(super) async fn execute_generated<RT: Runtime>(
    routed: Arc<GeneratedRoutedModule>,
    state: HostState<RT>,
    reusable_instance: Option<GeneratedReusableInstance<RT>>,
    reuse_instances: bool,
) -> anyhow::Result<GeneratedExecutionOutput<RT>> {
    let entry_selector = routed.entry_selector;
    execute_generated_with_entry_selector(
        routed,
        entry_selector,
        state,
        reusable_instance,
        reuse_instances,
    )
    .await
}

pub(super) async fn execute_generated_with_entry_selector<RT: Runtime>(
    routed: Arc<GeneratedRoutedModule>,
    entry_selector: Option<u64>,
    state: HostState<RT>,
    reusable_instance: Option<GeneratedReusableInstance<RT>>,
    reuse_instances: bool,
) -> anyhow::Result<GeneratedExecutionOutput<RT>> {
    let lifecycle_barrier = None;
    execute_generated_with_lifecycle_barrier_and_entry_selector(
        routed,
        entry_selector,
        GeneratedExecutionPurpose::Invocation,
        state,
        reusable_instance,
        reuse_instances,
        lifecycle_barrier,
        None,
    )
    .await
}

async fn wait_at_generated_lifecycle_barrier<RT: Runtime>(
    barrier: &GeneratedLifecycleBarrier,
    instance: &mut GeneratedReusableInstance<RT>,
) -> anyhow::Result<GeneratedLifecycleBarrierOutcome> {
    let (generation, invocation_id, cancellation) = {
        let state = instance.store.data();
        if !barrier.claim(state.provider.udf_path()) {
            return Ok(GeneratedLifecycleBarrierOutcome::Continue);
        }
        let generation = instance
            .routed
            .generation
            .clone()
            .context("generated Wasm lifecycle barrier route has no registry generation")?;
        let cancellation = generated_state(state)
            .map_err(wasmtime_anyhow)?
            .cancellation
            .clone();
        (generation, state.instance_id, cancellation)
    };
    let wait = barrier.wait(&generation, invocation_id, &cancellation);
    instance
        .store
        .data_mut()
        .timeout
        .as_mut()
        .context("generated Wasm lifecycle barrier timeout disappeared")?
        .with_release_permit(PauseReason::GeneratedLifecycleBarrier, wait)
        .await
        .context("generated Wasm lifecycle barrier wait failed")
}

pub(super) async fn execute_generated_with_lifecycle_barrier<RT: Runtime>(
    routed: Arc<GeneratedRoutedModule>,
    state: HostState<RT>,
    reusable_instance: Option<GeneratedReusableInstance<RT>>,
    reuse_instances: bool,
    lifecycle_barrier: Option<Arc<GeneratedLifecycleBarrier>>,
) -> anyhow::Result<GeneratedExecutionOutput<RT>> {
    let entry_selector = routed.entry_selector;
    execute_generated_with_lifecycle_barrier_and_entry_selector(
        routed,
        entry_selector,
        GeneratedExecutionPurpose::Invocation,
        state,
        reusable_instance,
        reuse_instances,
        lifecycle_barrier,
        None,
    )
    .await
}

async fn select_generated_entry<RT: Runtime>(
    store: &mut Store<HostState<RT>>,
    entry_selector: Option<u64>,
    select_entry: Option<&TypedFunc<i64, i32>>,
) -> Result<i32, WasmtimeError> {
    match (entry_selector, select_entry) {
        (None, None) => Ok(0),
        (Some(selector), Some(select_entry)) => {
            // Wasm i64 carries the selector's canonical unsigned 64-bit bit
            // pattern; the signed Rust value is only the call ABI.
            select_entry.call_async(store, selector as i64).await
        },
        _ => Err(host_invariant_error()),
    }
}

pub(super) async fn execute_generated_with_lifecycle_barrier_and_entry_selector<RT: Runtime>(
    routed: Arc<GeneratedRoutedModule>,
    entry_selector: Option<u64>,
    purpose: GeneratedExecutionPurpose,
    state: HostState<RT>,
    reusable_instance: Option<GeneratedReusableInstance<RT>>,
    reuse_instances: bool,
    lifecycle_barrier: Option<Arc<GeneratedLifecycleBarrier>>,
    prepared_images: Option<Arc<GeneratedPreparedImageCache>>,
) -> anyhow::Result<GeneratedExecutionOutput<RT>> {
    // A warm Store can retain an older descriptor than the newly resolved
    // selected entry. Backtrace ownership must follow the Store's actual code.
    let module_owner = Arc::clone(
        reusable_instance
            .as_ref()
            .map_or(&routed, |instance| &instance.routed),
    );
    let generated = generated_state(&state).map_err(wasmtime_anyhow)?;
    let interrupt = Arc::clone(&generated.interrupt);
    let cancellation = generated.cancellation.clone();
    let poll_clock = reusable_instance
        .as_ref()
        .map_or(&state.wasi_cpu_clock, |instance| {
            &instance.store.data().wasi_cpu_clock
        })
        .clone();
    // Cover the whole lifecycle: separate guest calls that complete Ready in
    // one task poll must share a slice, including instantiation and activation.
    let mut output = poll_clock
        .within_poll_budget(execute_generated_with_module_owner(
            routed,
            entry_selector,
            purpose,
            state,
            reusable_instance,
            reuse_instances,
            lifecycle_barrier,
            prepared_images,
        ))
        .await
        .map_err(|error| {
            // The lifecycle future has returned, so its Store and transaction are
            // gone even on direct exits before an InvocationOutput is available.
            // Instantiation can be interrupted before the ordinary outcome path.
            let error = if cancellation.is_cancelled() {
                error.context(GeneratedNonRetryableFailure::Cancelled)
            } else {
                match generated_error_timeout_reason(interrupt.timeout_reason(), &error) {
                    Some(GeneratedTimeoutReason::Initialization) => {
                        error.context(StaticHermesWasmExecutionFailure::InitializationTimeout)
                    },
                    Some(GeneratedTimeoutReason::Active) => {
                        error.context(StaticHermesWasmExecutionFailure::ExecutionTimeout)
                    },
                    Some(GeneratedTimeoutReason::System(_)) => {
                        error.context(ErrorMetadata::bad_request(
                            "SystemTimeoutError",
                            SYSTEM_TIMEOUT_ERROR_MESSAGE,
                        ))
                    },
                    Some(GeneratedTimeoutReason::Invalid) | None => error,
                }
            };
            let error = retain_generated_error_modules(error, &module_owner);
            let error = if error.is::<StaticHermesWasmExecutionFailure>() {
                error
            } else {
                error.context(StaticHermesWasmExecutionFailure::RuntimeInitialization)
            };
            allow_disposed_wasm_primary_fallback(error)
        })?;
    output.system_error = output
        .system_error
        .map(|error| retain_generated_error_modules(error, &module_owner));
    Ok(output)
}

async fn execute_generated_with_module_owner<RT: Runtime>(
    routed: Arc<GeneratedRoutedModule>,
    entry_selector: Option<u64>,
    purpose: GeneratedExecutionPurpose,
    mut state: HostState<RT>,
    mut reusable_instance: Option<GeneratedReusableInstance<RT>>,
    reuse_instances: bool,
    lifecycle_barrier: Option<Arc<GeneratedLifecycleBarrier>>,
    prepared_images: Option<Arc<GeneratedPreparedImageCache>>,
) -> anyhow::Result<GeneratedExecutionOutput<RT>> {
    macro_rules! try_generated {
        ($instance:expr, $result:expr) => {{
            match $result {
                Ok(value) => value,
                Err(error) => {
                    let error: anyhow::Error = error.into();
                    return Err(discard_optional_generated_instance($instance, error).await);
                },
            }
        }};
    }

    // Checkout has already transferred the reusable invocation resources out of the
    // old HostState. Install their new owner before any fallible setup:
    // teardown needs its interrupt and must consume the CPU permit this call
    // already holds.
    let mut fresh_state = if let Some(instance) = reusable_instance.as_mut() {
        state.wasi_monotonic_epoch = instance.store.data().wasi_monotonic_epoch;
        state.wasi_cpu_clock = instance.store.data().wasi_cpu_clock.clone();
        let previous_state = std::mem::replace(instance.store.data_mut(), state);
        drop(previous_state);
        None
    } else {
        Some(state)
    };

    if entry_selector.is_some() != routed.package_identity.requires_entry_selector() {
        return Err(discard_optional_generated_instance(
            reusable_instance,
            anyhow::anyhow!("generated Wasm route selector presence differs from its package ABI"),
        )
        .await);
    }
    let prepare_all_entries = purpose == GeneratedExecutionPurpose::Preparation
        || (reuse_instances
            && !matches!(
                &*routed.package_identity,
                ValidatedWasmUdfPackageIdentity::Legacy(_)
            )
            && reusable_instance
                .as_ref()
                .is_none_or(|instance| !instance.all_entries_prepared));
    let additional_preparation_selectors = if prepare_all_entries {
        try_generated!(
            reusable_instance,
            routed.package_identity.entry_preparation_selectors()
        )
    } else {
        Vec::new()
    };
    if prepared_images.is_some()
        && (purpose != GeneratedExecutionPurpose::Preparation
            || reusable_instance.is_some()
            || !reuse_instances)
    {
        return Err(discard_optional_generated_instance(
            reusable_instance,
            anyhow::anyhow!("initialized image cache requires fresh reusable preparation"),
        )
        .await);
    }
    let state = match reusable_instance.as_mut() {
        Some(instance) => instance.store.data_mut(),
        None => fresh_state
            .as_mut()
            .expect("fresh generated HostState missing"),
    };
    let has_request = try_generated!(
        reusable_instance,
        generated_state(state).map_err(wasmtime_anyhow)
    )
    .request_handle
    .is_some();
    if has_request != (purpose == GeneratedExecutionPurpose::Invocation) {
        return Err(discard_optional_generated_instance(
            reusable_instance,
            anyhow::anyhow!("generated Wasm request arguments differ from the lifecycle purpose"),
        )
        .await);
    }
    if state.provider.is_preparation() != (purpose == GeneratedExecutionPurpose::Preparation) {
        return Err(discard_optional_generated_instance(
            reusable_instance,
            anyhow::anyhow!("generated Wasm database context differs from the lifecycle purpose"),
        )
        .await);
    }
    if purpose == GeneratedExecutionPurpose::Preparation && lifecycle_barrier.is_some() {
        return Err(discard_optional_generated_instance(
            reusable_instance,
            anyhow::anyhow!(
                "generated Wasm preparation cannot use an invocation lifecycle barrier"
            ),
        )
        .await);
    }
    let timeout = try_generated!(
        reusable_instance,
        state
            .timeout
            .as_mut()
            .context("Static Hermes invocation timeout missing before execution")
    );
    try_generated!(
        reusable_instance,
        state
            .provider
            .initialize_static_hermes(timeout)
            .await
            .map_err(|error| wasmtime_anyhow(generated_host_operation_error(error)))
    );
    #[cfg(any(test, feature = "testing"))]
    let wall_start = Instant::now();
    #[cfg(any(test, feature = "testing"))]
    let metrics = Arc::clone(&state.metrics);
    let cancellation = try_generated!(
        reusable_instance,
        generated_state(state).map_err(wasmtime_anyhow)
    )
    .cancellation
    .clone();
    #[cfg(any(test, feature = "testing"))]
    let interrupt = Arc::clone(
        &try_generated!(
            reusable_instance,
            generated_state(state).map_err(wasmtime_anyhow)
        )
        .interrupt,
    );
    let memory_permit = try_generated!(
        reusable_instance,
        generated_state(state).map_err(wasmtime_anyhow)
    )
    .memory_permit
    .as_ref();
    let memory_permit = try_generated!(
        reusable_instance,
        memory_permit.context("generated invocation memory permit disappeared")
    );
    let memory_slot_id = memory_permit.slot_id();
    #[cfg(any(test, feature = "testing"))]
    let memory_controller = Arc::clone(memory_permit.controller());
    #[cfg(any(test, feature = "testing"))]
    let invocation_id = state.instance_id;
    #[cfg(any(test, feature = "testing"))]
    let mut phase_trace = GeneratedExecutionPhaseTrace::new(
        Arc::clone(&metrics),
        interrupt,
        cancellation.clone(),
        wall_start,
        reusable_instance.is_none(),
        routed.package_identity.requires_entry_selector(),
        routed.package_identity.requires_selected_entry_prepare(),
    );
    let manifest_execution_fuel = routed.manifest.limits().execution_fuel();
    #[cfg(any(test, feature = "testing"))]
    let execution_fuel = TEST_HOOKS
        .lock()
        .as_ref()
        .and_then(Weak::upgrade)
        .and_then(|hooks| hooks.generated_execution_fuel_override())
        .unwrap_or(manifest_execution_fuel);
    #[cfg(not(any(test, feature = "testing")))]
    let execution_fuel = manifest_execution_fuel;
    #[cfg(any(test, feature = "testing"))]
    {
        phase_trace.initial_fuel = Some(GENERATED_INITIALIZATION_FUEL);
    }
    let (mut reusable_instance, initialize) = match reusable_instance {
        Some(mut reusable_instance) => {
            let _timer = GatePhaseTimer::new(GATE_PHASE_WARM_STORE_CHECKOUT_RESET);
            if reusable_instance.pool_identity.as_ref() != &routed.pool_identity {
                discard_generated_instance(reusable_instance)
                    .await
                    .context(StaticHermesWasmExecutionFailure::RuntimeCleanup)?;
                anyhow::bail!("generated Wasm instance route changed while pooled");
            }
            if !reusable_instance
                .routed
                .has_same_generation_incarnation(&routed)
            {
                discard_generated_instance(reusable_instance)
                    .await
                    .context(StaticHermesWasmExecutionFailure::RuntimeCleanup)?;
                anyhow::bail!(
                    "generated Wasm instance generation incarnation changed while pooled"
                );
            }
            if reusable_instance.memory_slot_id != memory_slot_id {
                return Err(discard_optional_generated_instance(
                    Some(reusable_instance),
                    anyhow::anyhow!("generated Wasm instance memory slot changed while pooled"),
                )
                .await);
            }
            #[cfg(any(test, feature = "testing"))]
            if !Arc::ptr_eq(&reusable_instance.memory_controller, &memory_controller) {
                return Err(discard_optional_generated_instance(
                    Some(reusable_instance),
                    anyhow::anyhow!("generated Wasm memory controller changed while pooled"),
                )
                .await);
            }
            if purpose == GeneratedExecutionPurpose::Invocation {
                let logs = std::mem::take(&mut reusable_instance.pending_initialization_logs);
                try_generated!(
                    Some(reusable_instance),
                    reusable_instance
                        .store
                        .data_mut()
                        .provider
                        .append_initialization_log_lines(logs)
                );
            }
            if !reusable_instance.execution_hook_installed {
                reusable_instance.execution_hook_installed =
                    crate::execution_observation::install_wasm_hook_if_observed(
                        &mut reusable_instance.store,
                    );
            }
            try_generated!(
                Some(reusable_instance),
                reusable_instance
                    .store
                    .set_fuel(GENERATED_INITIALIZATION_FUEL)
                    .map_err(wasmtime_anyhow)
            );
            reusable_instance.store.set_epoch_deadline(1);
            reusable_instance.pool_checked_out = true;
            generated_state_mut(reusable_instance.store.data_mut())?
                .memory_permit
                .as_mut()
                .ok_or_else(|| host_invariant_error())?
                .mark_execution_started(true);
            (reusable_instance, None)
        },
        None => {
            let _timer = GatePhaseTimer::new(GATE_PHASE_FRESH_STORE_GRAPH_INSTANTIATION);
            let (memory_module, static_evidence) = if let Some(graph) = &routed.emscripten_graph {
                (&*graph.base.module, &graph.base.verified_static_memory)
            } else if let Some(graph) = &routed.graph {
                let owner = graph
                    .dependencies
                    .iter()
                    .find(|dependency| dependency.module_id == graph.layout.memory.module_id)
                    .context("generated graph lost its memory-owning dependency")?;
                (&*owner.module.module, &owner.module.verified_static_memory)
            } else {
                (&*routed.module, &routed.verified_static_memory)
            };
            let mut linker = Linker::new(&routed.engine);
            add_generated_convex_imports(&mut linker)
                .map_err(wasmtime_anyhow)
                .context(GeneratedEngineInitializationFailure)?;
            add_wasi_imports(&mut linker)
                .map_err(wasmtime_anyhow)
                .context(GeneratedEngineInitializationFailure)?;
            let mut store = GeneratedStore::from(Store::new(
                &routed.engine,
                fresh_state
                    .take()
                    .expect("fresh generated HostState missing"),
            ));
            let execution_hook_installed =
                crate::execution_observation::install_wasm_hook_if_observed(&mut store);
            store
                .set_fuel(GENERATED_INITIALIZATION_FUEL)
                .map_err(wasmtime_anyhow)
                .context(GeneratedEngineInitializationFailure)?;
            store.limiter(|state| {
                &mut state
                    .generated
                    .as_mut()
                    .expect("generated Store lost its invocation state")
                    .memory_limiter
            });
            arm_generated_store_epoch(&mut store);
            generated_state_mut(store.data_mut())?
                .memory_permit
                .as_mut()
                .ok_or_else(|| host_invariant_error())?
                .mark_execution_started(false);
            // The evidence belongs to this exact retained Module, not an
            // artifact name or generation. Only its first linear allocation
            // can consume the credit. Wasmtime may allocate its internal GC
            // heap first; the limiter keeps that capacity fully charged.
            let memory_owner_is_first = routed.graph.as_ref().is_none_or(|graph| {
                graph.dependencies.first().is_some_and(|dependency| {
                    Module::same(&dependency.module.module, memory_module)
                })
            });
            if memory_owner_is_first {
                if let Some(verified) = static_evidence.get().filter(|v| v.shared_bytes != 0) {
                    generated_state_mut(store.data_mut())?
                        .memory_limiter
                        .prepare_initial_static_memory(
                            verified.initial_bytes,
                            verified.shared_bytes,
                        );
                }
            }
            if routed.graph.is_none() {
                generated_state_mut(store.data_mut())?
                    .memory_permit
                    .as_ref()
                    .context("fresh Store lost its memory permit")?
                    .begin_static_memory_instantiation()?;
            }
            let cpu_clock = store.data().wasi_cpu_clock.clone();
            let (instance, selector_export, emscripten_graph_instances) = cpu_clock
                .measure(async {
                    Ok::<_, anyhow::Error>(match &routed.emscripten_graph {
                        Some(graph) => {
                            let instances = instantiate_generated_emscripten_graph(
                                &linker,
                                &mut store,
                                graph,
                                |store, memory| {
                                    account_generated_initial_static_memory(
                                        store,
                                        memory_module,
                                        static_evidence,
                                        memory,
                                    )
                                },
                            )
                            .await?;
                            (
                                instances.modules[0],
                                "convex_wasm_graph_select_entry",
                                Some((instances, graph.initialization.clone())),
                            )
                        },
                        None => {
                            let graph_instances = match &routed.graph {
                                Some(graph) => Some(
                                    instantiate_generated_graph_dependencies(
                                        &mut linker,
                                        &mut store,
                                        graph,
                                        |store| {
                                            generated_state_mut(store.data_mut())
                                                .map_err(wasmtime_anyhow)?
                                                .memory_permit
                                                .as_ref()
                                                .context(
                                                    "graph memory owner lost its memory permit",
                                                )?
                                                .begin_static_memory_instantiation()
                                        },
                                    )
                                    .await?,
                                ),
                                None => None,
                            };
                            let instance = linker
                                .instantiate_async(&mut store, &routed.module)
                                .await
                                .map_err(wasmtime_anyhow)
                                .context(GeneratedEngineInitializationFailure)?;
                            if let (Some(graph), Some(instances)) =
                                (&routed.graph, graph_instances.as_ref())
                            {
                                validate_generated_graph_instance_layout(
                                    &mut store,
                                    instances,
                                    &graph.layout,
                                )?;
                            }
                            (instance, "convex_wasm_select_entry", None)
                        },
                    })
                })
                .await
                .context(GeneratedEngineInitializationFailure)?;
            let initialize = instance
                .get_typed_func::<(), ()>(&mut store, "_initialize")
                .map_err(wasmtime_anyhow)
                .context(GeneratedEngineInitializationFailure)?;
            let (initialization, initialized_state) = match emscripten_graph_instances {
                Some((mut instances, initialization)) => {
                    let initialized_state = instances.initialized_state.take();
                    (
                        GeneratedFreshInitialization::EmscriptenGraph {
                            instances,
                            base_initialize: initialize,
                            initialization,
                        },
                        initialized_state,
                    )
                },
                None => (GeneratedFreshInitialization::Monolithic(initialize), None),
            };
            let run = instance
                .get_typed_func::<(), i32>(&mut store, "convex_wasm_udf_run")
                .map_err(wasmtime_anyhow)
                .context(GeneratedEngineInitializationFailure)?;
            let select_entry = routed
                .package_identity
                .requires_entry_selector()
                .then(|| instance.get_typed_func::<i64, i32>(&mut store, selector_export))
                .transpose()
                .map_err(wasmtime_anyhow)
                .context(GeneratedEngineInitializationFailure)?;
            let prepare_selected_entry = routed
                .package_identity
                .requires_selected_entry_prepare()
                .then(|| {
                    instance.get_typed_func::<(), i32>(
                        &mut store,
                        "convex_wasm_udf_prepare_selected_entry",
                    )
                })
                .transpose()
                .map_err(wasmtime_anyhow)
                .context(GeneratedEngineInitializationFailure)?;
            let destroy = instance
                .get_typed_func::<(), ()>(&mut store, "convex_wasm_udf_destroy_runtime")
                .map_err(wasmtime_anyhow)
                .context(GeneratedEngineInitializationFailure)?;
            let memory_maintenance =
                super::guest_memory_maintenance::GuestMemoryMaintenance::from_instance(
                    &mut store, instance,
                )
                .context(GeneratedEngineInitializationFailure)?;
            #[cfg(test)]
            let arm_formatter_initialization_failure = instance
                .get_typed_func::<(), i32>(
                    &mut store,
                    NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_ARM_EXPORT,
                )
                .ok();
            #[cfg(test)]
            let partial_initialization_trace = instance
                .get_typed_func::<(), i64>(
                    &mut store,
                    NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_TRACE_EXPORT,
                )
                .ok();
            let memory = instance
                .get_memory(&mut store, "memory")
                .context("generated Wasm instance lost its memory export")
                .context(GeneratedEngineInitializationFailure)?;
            if routed.emscripten_graph.is_none() {
                account_generated_initial_static_memory(
                    &mut store,
                    memory_module,
                    static_evidence,
                    memory,
                )?;
            }
            (
                GeneratedReusableInstance {
                    routed: Arc::clone(&routed),
                    route_identity: routed.route_identity.clone(),
                    pool_identity: Arc::new(routed.pool_identity.clone()),
                    memory_slot_id,
                    #[cfg(any(test, feature = "testing"))]
                    memory_controller,
                    store,
                    memory,
                    execution_hook_installed,
                    select_entry,
                    prepare_selected_entry,
                    memory_maintenance,
                    initialized_state,
                    run,
                    destroy,
                    #[cfg(test)]
                    arm_formatter_initialization_failure,
                    #[cfg(test)]
                    partial_initialization_trace,
                    context_read_set: None,
                    all_entries_prepared: false,
                    pending_initialization_logs: LogLines::default(),
                    id: NEXT_GENERATED_REUSABLE_INSTANCE_ID.fetch_add(1, Ordering::SeqCst),
                    runtime_state: GeneratedRuntimeState::Inactive,
                    pool_checked_out: true,
                    idle_since: Instant::now(),
                    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
                    initialized_memory: None,
                },
                Some(initialization),
            )
        },
    };
    // A Store retains these module owners throughout reuse. Record the links
    // once at construction rather than acquiring module locks on every call.
    if initialize.is_some() {
        let module_charge_ids = reusable_instance.routed.memory_charge_ids();
        let inspection_result = generated_state_mut(reusable_instance.store.data_mut())
            .map_err(wasmtime_anyhow)
            .and_then(|state| {
                state
                    .memory_permit
                    .as_ref()
                    .context("generated memory permit missing at inspection boundary")?
                    .describe_modules(module_charge_ids, None);
                Ok(())
            });
        try_generated!(Some(reusable_instance), inspection_result);
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    if initialize.is_none() {
        let grant = reusable_instance.begin_initialized_memory();
        if grant.is_err() {
            reusable_instance.runtime_state = GeneratedRuntimeState::Poisoned;
        }
        try_generated!(Some(reusable_instance), grant);
    }
    try_generated!(
        Some(reusable_instance),
        reusable_instance.reconcile_memory()
    );
    let reused_instance = initialize.is_none();
    if purpose == GeneratedExecutionPurpose::Invocation {
        crate::execution_observation::record_runtime_reuse(reused_instance);
    }
    #[cfg(any(test, feature = "testing"))]
    {
        if initialize.is_some() {
            phase_trace.instantiation_completed = Some(phase_trace.elapsed());
        }
    }
    #[cfg(any(test, feature = "testing"))]
    let runtime_was_created = initialize.is_some();
    #[cfg(test)]
    let runtime_id = reusable_instance.id;
    if reusable_instance.prepare_selected_entry.is_some()
        != routed.package_identity.requires_selected_entry_prepare()
    {
        return Err(discard_optional_generated_instance(
            Some(reusable_instance),
            anyhow::anyhow!(
                "generated Wasm selected-entry preparation export differs from its package ABI"
            ),
        )
        .await);
    }
    if reusable_instance.select_entry.is_some() != routed.package_identity.requires_entry_selector()
    {
        return Err(discard_optional_generated_instance(
            Some(reusable_instance),
            anyhow::anyhow!("generated Wasm selector export differs from its package ABI"),
        )
        .await);
    }

    #[cfg(any(test, feature = "testing"))]
    if TEST_HOOKS.lock().as_ref().and_then(Weak::upgrade).is_some() {
        metrics.generated_runtimes.lock().push(reusable_instance.id);
    }

    // The active instance owns its routed module, which owns the resolved
    // generation. Waiting here lets retirement proceed without changing the
    // generation this invocation will execute.
    let barrier_outcome = match lifecycle_barrier {
        Some(barrier) => {
            wait_at_generated_lifecycle_barrier(&barrier, &mut reusable_instance).await
        },
        None => Ok(GeneratedLifecycleBarrierOutcome::Continue),
    };
    let mut cancelled = matches!(
        &barrier_outcome,
        Ok(GeneratedLifecycleBarrierOutcome::Cancelled)
    );
    let mut failure_subphase = StaticHermesWasmExecutionFailure::RuntimeInitialization;
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    let mut memory_reuse_allowed = true;
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    let mut captured_image = None;
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    let memory_reuse_allowed = true;
    let call_result = match barrier_outcome {
        Ok(GeneratedLifecycleBarrierOutcome::Cancelled) => Ok(0),
        Ok(GeneratedLifecycleBarrierOutcome::Continue) => {
            let initialization_started = initialize.is_some();
            let call_result = {
                let cpu_clock = reusable_instance.store.data().wasi_cpu_clock.clone();
                let call = async {
                    use StaticHermesGeneratedExportDiagnostic::FirstSelectorRejected;

                    let invocation = async {
                        generated_state(reusable_instance.store.data())?
                            .memory_limiter
                            .ensure_memory_available()?;
                        let preparation_timer = GatePhaseTimer::new(
                            GATE_PHASE_GUEST_INITIALIZATION_SELECTED_ENTRY_PREPARATION,
                        );
                        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
                        if let Some(cache) = prepared_images.as_ref()
                            && reusable_instance
                                .restore_prepared_image(cache)
                                .await
                                .map_err(|error| {
                                    reusable_instance.runtime_state =
                                        GeneratedRuntimeState::Poisoned;
                                    WasmtimeError::from_anyhow(error)
                                })?
                        {
                            return Ok(0);
                        }
                        // A pooled graph can prepare a previously unused entry. Its
                        // module initializers need the same configuration-only
                        // authority and dependency capture as a fresh Store.
                        let mut initialization_authority_active = initialization_started
                            || reusable_instance.prepare_selected_entry.is_some();
                        if initialization_authority_active {
                            generated_state_mut(reusable_instance.store.data_mut())?
                                .capability_bridge
                                .begin_initialization()
                                .map_err(|_| host_invariant_error())?;
                            if !initialization_started
                                && generated_state(reusable_instance.store.data())?
                                    .context_read_set_required
                            {
                                reusable_instance
                                    .store
                                    .data_mut()
                                    .provider
                                    .snoop_initialization_reads()
                                    .map_err(|_| host_invariant_error())?;
                            }
                        }
                        if let Some(initialization) = initialize {
                            #[cfg(any(test, feature = "testing"))]
                            {
                                phase_trace.initialization_started = Some(phase_trace.elapsed());
                            }
                            #[cfg(test)]
                            metrics
                                .fresh_initialization_attempts
                                .fetch_add(1, Ordering::SeqCst);
                            // A fresh Store cancelled before initialization needs only a
                            // host-side drop: guest teardown may depend on relocations
                            // and constructors. Failed initialization must also drop
                            // the Store without entering potentially broken native state.
                            reusable_instance.runtime_state = GeneratedRuntimeState::Live;
                            run_generated_fresh_initialization(
                                &mut reusable_instance.store,
                                initialization,
                            )
                            .await
                            .inspect_err(|_| {
                                reusable_instance.runtime_state = GeneratedRuntimeState::Poisoned;
                            })?;
                            generated_state(reusable_instance.store.data())?
                                .memory_limiter
                                .ensure_memory_available()?;
                            #[cfg(any(test, feature = "testing"))]
                            {
                                phase_trace.initialization_completed = Some(phase_trace.elapsed());
                            }
                            if !routed.package_identity.requires_selected_entry_prepare() {
                                generated_state_mut(reusable_instance.store.data_mut())?
                                    .capability_bridge
                                    .finish_initialization()
                                    .map_err(|_| host_invariant_error())?;
                                initialization_authority_active = false;
                            }
                        }
                        #[cfg(test)]
                        if metrics
                            .formatter_initialization_failure_arms
                            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                                remaining.checked_sub(1)
                            })
                            .is_ok()
                        {
                            let arm = reusable_instance
                                .arm_formatter_initialization_failure
                                .clone()
                                .ok_or_else(|| host_invariant_error())?;
                            if arm
                                .call_async(&mut reusable_instance.store, ())
                                .await
                                .inspect_err(|_| {
                                    reusable_instance.runtime_state =
                                        GeneratedRuntimeState::Poisoned;
                                })?
                                != 0
                            {
                                return Err(host_invariant_error());
                            }
                        }
                        let mut selected_entry_prepared = false;
                        for preparation_selector in std::iter::once(entry_selector)
                            .filter(|_| additional_preparation_selectors.is_empty())
                            .chain(additional_preparation_selectors.iter().copied().map(Some))
                        {
                            generated_state(reusable_instance.store.data())?
                                .memory_limiter
                                .ensure_memory_available()?;
                            if preparation_selector.is_some() {
                                failure_subphase =
                                    StaticHermesWasmExecutionFailure::GeneratedExportDispatch;
                                #[cfg(any(test, feature = "testing"))]
                                {
                                    phase_trace.entry_selection_started =
                                        Some(phase_trace.elapsed());
                                }
                                let status = select_generated_entry(
                                    &mut reusable_instance.store,
                                    preparation_selector,
                                    reusable_instance.select_entry.as_ref(),
                                )
                                .await
                                .map_err(|error| {
                                    reusable_instance.runtime_state =
                                        GeneratedRuntimeState::Poisoned;
                                    error.context(
                                        StaticHermesGeneratedExportDiagnostic::FirstSelectorTrap,
                                    )
                                })?;
                                #[cfg(any(test, feature = "testing"))]
                                {
                                    phase_trace.entry_selection_status = Some(status);
                                }
                                if status != 0 {
                                    return Err(WasmtimeError::new(FirstSelectorRejected {
                                        status,
                                    }));
                                }
                                #[cfg(any(test, feature = "testing"))]
                                {
                                    phase_trace.entry_selection_completed =
                                        Some(phase_trace.elapsed());
                                }
                            }
                            selected_entry_prepared = if let Some(prepare_selected_entry) =
                                reusable_instance.prepare_selected_entry.as_ref()
                            {
                                generated_state(reusable_instance.store.data())?
                                    .memory_limiter
                                    .ensure_memory_available()?;
                                failure_subphase =
                                    StaticHermesWasmExecutionFailure::GeneratedExportDispatch;
                                #[cfg(any(test, feature = "testing"))]
                                {
                                    phase_trace.prepare_started = Some(phase_trace.elapsed());
                                    phase_trace.prepare_start_fuel =
                                        reusable_instance.store.get_fuel().ok();
                                }
                                let status = prepare_selected_entry
                                    .call_async(&mut reusable_instance.store, ())
                                    .await
                                    .map_err(|error| {
                                        reusable_instance.runtime_state =
                                            GeneratedRuntimeState::Poisoned;
                                        error.context(
                                        StaticHermesGeneratedExportDiagnostic::
                                            SelectedEntryPreparationTrap,
                                    )
                                    })?;
                                #[cfg(any(test, feature = "testing"))]
                                {
                                    phase_trace.prepare_status = Some(status);
                                }
                                if status != 0 {
                                    return Err(WasmtimeError::new(
                                    StaticHermesGeneratedExportDiagnostic::
                                        SelectedEntryPreparationRejected { status },
                                ));
                                }
                                #[cfg(any(test, feature = "testing"))]
                                {
                                    phase_trace.prepare_end_fuel =
                                        reusable_instance.store.get_fuel().ok();
                                    phase_trace.prepare_completed = Some(phase_trace.elapsed());
                                }
                                true
                            } else {
                                false
                            };
                            if selected_entry_prepared
                                != routed.package_identity.requires_selected_entry_prepare()
                            {
                                return Err(host_invariant_error());
                            }
                        }
                        generated_state(reusable_instance.store.data())?
                            .memory_limiter
                            .ensure_memory_available()?;
                        failure_subphase = StaticHermesWasmExecutionFailure::RuntimeInitialization;

                        // Selected-entry preparation evaluates the capability entry's
                        // module graph. Keep initialization read snooping active until
                        // that phase completes so reads made by module-scope code are
                        // part of the reusable runtime's context read set.
                        if (initialization_started || selected_entry_prepared)
                            && generated_state(reusable_instance.store.data())?
                                .context_read_set_required
                        {
                            let state = reusable_instance.store.data_mut();
                            let reads = state
                                .provider
                                .finish_initialization_reads()
                                .map_err(|_| host_invariant_error())?;
                            let capture = ContextCache::capture_context_read_set(
                                reads,
                                state
                                    .provider
                                    .tx_for_initialization()
                                    .map_err(|_| host_invariant_error())?,
                            );
                            let captured = state
                                .timeout
                                .as_mut()
                                .ok_or_else(|| host_invariant_error())?
                                .with_release_permit(PauseReason::UdfInitialize, capture)
                                .await
                                .map_err(generated_host_operation_error)?;
                            reusable_instance.context_read_set =
                                match (reusable_instance.context_read_set.take(), captured) {
                                    (Some(mut previous), Some(additional)) => {
                                        // Keep earlier modules' dependencies without
                                        // rehashing them after every warm preparation.
                                        let intervals = additional.read_set.num_intervals();
                                        let user_size = additional.read_set.user_tx_size().clone();
                                        let system_size =
                                            additional.read_set.system_tx_size().clone();
                                        previous.read_set.merge(
                                            additional.read_set.into_read_set(),
                                            intervals,
                                            user_size,
                                            system_size,
                                        );
                                        for range in additional.range_hashes {
                                            if !previous.range_hashes.contains(&range) {
                                                previous.range_hashes.push(range);
                                            }
                                        }
                                        Some(previous)
                                    },
                                    (None, captured) => captured,
                                    (Some(_), None) => None,
                                };
                        }

                        if initialization_authority_active {
                            generated_state_mut(reusable_instance.store.data_mut())?
                                .capability_bridge
                                .finish_initialization()
                                .map_err(|_| host_invariant_error())?;
                        }

                        {
                            let generated = generated_state(reusable_instance.store.data())?;
                            if !generated.capability_bridge.is_unissued()
                                || !generated.async_operations.is_empty()
                            {
                                return Err(host_invariant_error());
                            }
                        }
                        if prepare_all_entries {
                            reusable_instance.all_entries_prepared = true;
                        }
                        if purpose == GeneratedExecutionPurpose::Preparation {
                            // A prepared Store has configuration dependencies but no
                            // handler authority, result, or execution-phase provider.
                            #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
                            if prepared_images.is_some() {
                                captured_image = reusable_instance
                                    .capture_prepared_image()
                                    .await
                                    .map_err(|error| {
                                    reusable_instance.runtime_state =
                                        GeneratedRuntimeState::Poisoned;
                                    WasmtimeError::from_anyhow(error)
                                })?;
                            }
                            return Ok(0);
                        }
                        reusable_instance
                            .store
                            .data_mut()
                            .provider
                            .begin_execution()
                            .map_err(|_| host_invariant_error())?;
                        reusable_instance.store.set_fuel(execution_fuel)?;
                        reusable_instance.store.set_epoch_deadline(1);
                        let profile_fuel = reusable_instance.store.get_fuel().ok();
                        {
                            let generated =
                                generated_state_mut(reusable_instance.store.data_mut())?;
                            generated.performance_runtime_available = false;
                        }
                        reusable_instance.store.data_mut().profile_last_mark =
                            Some((Instant::now(), profile_fuel));
                        if entry_selector.is_some() {
                            failure_subphase =
                                StaticHermesWasmExecutionFailure::GeneratedExportDispatch;
                            #[cfg(any(test, feature = "testing"))]
                            {
                                phase_trace.second_entry_selection_started =
                                    Some(phase_trace.elapsed());
                            }
                            let status = select_generated_entry(
                                &mut reusable_instance.store,
                                entry_selector,
                                reusable_instance.select_entry.as_ref(),
                            )
                            .await
                            .map_err(|error| {
                                reusable_instance.runtime_state = GeneratedRuntimeState::Poisoned;
                                error.context(
                                    StaticHermesGeneratedExportDiagnostic::SecondSelectorTrap,
                                )
                            })?;
                            #[cfg(any(test, feature = "testing"))]
                            {
                                phase_trace.second_entry_selection_status = Some(status);
                            }
                            if status != 0 {
                                return Err(WasmtimeError::new(
                                    StaticHermesGeneratedExportDiagnostic::SecondSelectorRejected {
                                        status,
                                    },
                                ));
                            }
                            #[cfg(any(test, feature = "testing"))]
                            {
                                phase_trace.second_entry_selection_completed =
                                    Some(phase_trace.elapsed());
                            }
                            failure_subphase =
                                StaticHermesWasmExecutionFailure::RuntimeInitialization;
                        }
                        generated_state(reusable_instance.store.data())?
                            .memory_limiter
                            .ensure_memory_available()?;
                        rearm_generated_active_timeout(
                            reusable_instance.store.data_mut(),
                            Duration::from_millis(routed.manifest.limits().timeout_milliseconds()),
                        )
                        .map_err(WasmtimeError::from_anyhow)?;
                        let performance_monotonic_start =
                            reusable_instance.store.data().provider.rt().monotonic_now();
                        {
                            let generated =
                                generated_state_mut(reusable_instance.store.data_mut())?;
                            generated.performance_monotonic_start = performance_monotonic_start;
                            generated
                                .capability_bridge
                                .issue()
                                .map_err(|_| host_invariant_error())?;
                            generated.performance_runtime_available = true;
                        }
                        let capture_handler_reads = reusable_instance
                            .store
                            .data()
                            .provider
                            .handler_read_capture_enabled()
                            .map_err(|_| host_invariant_error())?;
                        if capture_handler_reads {
                            reusable_instance
                                .store
                                .data_mut()
                                .provider
                                .start_handler_read_capture()
                                .map_err(|_| host_invariant_error())?;
                        }
                        drop(preparation_timer);
                        #[cfg(any(test, feature = "testing"))]
                        {
                            phase_trace.handler_started = Some(phase_trace.elapsed());
                            phase_trace.handler_start_fuel =
                                reusable_instance.store.get_fuel().ok();
                        }
                        failure_subphase = StaticHermesWasmExecutionFailure::GuestExecution;
                        crate::execution_observation::set_phase(
                            crate::execution_observation::Phase::Handler,
                        );
                        let handler_timer =
                            GatePhaseTimer::new(GATE_PHASE_HANDLER_EXPORT_EXECUTION);
                        let result = reusable_instance
                            .run
                            .call_async(&mut reusable_instance.store, ())
                            .await
                            .inspect_err(|_| {
                                reusable_instance.runtime_state = GeneratedRuntimeState::Poisoned;
                            });
                        drop(handler_timer);
                        crate::execution_observation::set_phase(
                            crate::execution_observation::Phase::Finalization,
                        );
                        // A trapped nested call may no longer own its transaction.
                        // A trapped handler cannot publish a read comparison,
                        // and finishing its capture could replace the original
                        // host error with a retryable ABI failure.
                        if capture_handler_reads && result.is_ok() {
                            reusable_instance
                                .store
                                .data_mut()
                                .provider
                                .finish_handler_read_capture()
                                .map_err(|_| host_invariant_error())?;
                        }
                        #[cfg(any(test, feature = "testing"))]
                        {
                            phase_trace.handler_end_fuel = reusable_instance.store.get_fuel().ok();
                            phase_trace.handler_completion = Some(match &result {
                                Ok(_) => "returned",
                                Err(error) => match error.downcast_ref::<Trap>() {
                                    Some(Trap::OutOfFuel) => "trap:out-of-fuel",
                                    Some(Trap::Interrupt) => "trap:interrupt",
                                    Some(_) => "trap:other",
                                    None => "error",
                                },
                            });
                            if result.is_ok() {
                                phase_trace.handler_completed = Some(phase_trace.elapsed());
                            }
                        }
                        result
                    };
                    let result = invocation.await;
                    let reconciliation = reusable_instance.reconcile_memory();
                    // Preserve the trap, but a failed accounting check still
                    // vetoes recovery after disposal. Poisoning alone does not
                    // carry that veto through the error result.
                    if let Err(error) = reconciliation {
                        let mut primary = result.err().map(wasmtime_anyhow);
                        merge_generated_execution_error(
                            &mut primary,
                            error.context(StaticHermesWasmExecutionFailure::ResultFinalization),
                            "generated Wasm memory reconciliation failed",
                        );
                        return Err(WasmtimeError::from_anyhow(
                            primary.expect("memory reconciliation error disappeared"),
                        ));
                    }
                    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
                    if matches!(&result, Ok(0))
                        && purpose == GeneratedExecutionPurpose::Invocation
                        && reuse_instances
                        && !cancellation.is_cancelled()
                    {
                        // Reclamation may run finalizers. Keep it inside the
                        // measured call with its original fuel, deadline and
                        // host resources, before revoking request authority.
                        failure_subphase = StaticHermesWasmExecutionFailure::RuntimeCleanup;
                        memory_reuse_allowed = reusable_instance.reclaim_after_success().await?;
                    }
                    result
                };
                let call = cpu_clock.measure(call);
                tokio::pin!(call);
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        cancelled = true;
                        call.await
                    },
                    result = &mut call => result,
                }
            };
            let growth_failure = try_generated!(
                Some(reusable_instance),
                generated_state(reusable_instance.store.data()).map_err(wasmtime_anyhow)
            )
            .memory_limiter
            .growth_failure;
            // GC retry can swallow a limiter error and satisfy the allocation
            // after collection. Keep the normal failure/finalization path, but
            // never accept that result or reenter the guest during teardown.
            let call_result = if let Some(failure) = growth_failure {
                reusable_instance.runtime_state = GeneratedRuntimeState::Poisoned;
                call_result.and_then(|_| Err(WasmtimeError::new(failure)))
            } else {
                call_result
            };
            call_result.map_err(|error| {
                let failure =
                    if matches!(growth_failure, Some(GeneratedGuestMemoryLimit::Aggregate))
                        || error.downcast_ref::<GeneratedGuestMemoryLimit>()
                            == Some(&GeneratedGuestMemoryLimit::Aggregate)
                    {
                        StaticHermesWasmExecutionFailure::AggregateMemoryLimit
                    } else if error.is::<GeneratedGuestMemoryLimit>() {
                        StaticHermesWasmExecutionFailure::GuestMemoryLimit
                    } else if let Some(failure) = generated_state(reusable_instance.store.data())
                        .ok()
                        .and_then(|generated| generated.terminal_failure.clone())
                    {
                        failure
                    } else if error.is::<HostInvariant>() {
                        StaticHermesWasmExecutionFailure::HostAbiInvariant
                    } else if failure_subphase == StaticHermesWasmExecutionFailure::GuestExecution
                        && let Some(diagnostic) = wasmtime_trap_diagnostic(
                            &error,
                            &routed,
                            &reusable_instance.store,
                            reused_instance,
                            execution_fuel,
                        )
                    {
                        StaticHermesWasmExecutionFailure::WasmtimeTrap { diagnostic }
                    } else {
                        failure_subphase.clone()
                    };
                let error = error.context(failure);
                if failure_subphase == StaticHermesWasmExecutionFailure::RuntimeCleanup {
                    // A finalizer can trap with an otherwise recoverable engine
                    // diagnostic. Failure during cleanup still forbids retry.
                    error.context(GeneratedNonRetryableFailure::Cleanup)
                } else {
                    error
                }
            })
        },
        Err(error) => Err(generated_host_operation_error(error)
            .context(StaticHermesWasmExecutionFailure::RuntimeInitialization)),
    };
    #[cfg(any(test, feature = "testing"))]
    {
        phase_trace.final_fuel = reusable_instance.store.get_fuel().ok();
    }
    #[cfg(test)]
    let (call_result, partial_initialization_trace) = match reusable_instance
        .partial_initialization_trace
        .clone()
        .filter(|_| reusable_instance.runtime_state == GeneratedRuntimeState::Live)
    {
        Some(trace) => match trace.call_async(&mut reusable_instance.store, ()).await {
            Ok(trace) => (call_result, Some(trace)),
            Err(error) => {
                reusable_instance.runtime_state = GeneratedRuntimeState::Poisoned;
                (Err(error), None)
            },
        },
        None => (call_result, None),
    };
    try_generated!(
        Some(reusable_instance),
        generated_state_mut(reusable_instance.store.data_mut())
            .map_err(wasmtime_anyhow)
            .context(StaticHermesWasmExecutionFailure::RuntimeCleanup)
    )
    .performance_runtime_available = false;
    #[cfg(test)]
    try_generated!(
        Some(reusable_instance),
        reusable_instance
            .store
            .data_mut()
            .provider
            .cancel_nested_call_for_test()
    );
    cancelled |= cancellation.is_cancelled();
    let _finalization_timer = GatePhaseTimer::new(GATE_PHASE_RESULT_FINALIZATION_CLEANUP);
    #[cfg(any(test, feature = "testing"))]
    {
        let completion_hooks = TEST_HOOKS.lock().as_ref().and_then(Weak::upgrade);
        if let Some(hooks) = completion_hooks
            && hooks
                .held_guest_completions_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            let instance_id = reusable_instance.store.data().instance_id;
            let (release, release_rx) = oneshot::channel();
            assert!(hooks
                .guest_releases
                .lock()
                .insert(instance_id, release)
                .is_none());
            try_generated!(
                Some(reusable_instance),
                hooks
                    .guest_completed_tx
                    .send(instance_id)
                    .map_err(|_| host_invariant_error())
            );
            tokio::select! {
                biased;
                release = release_rx => {
                    try_generated!(
                        Some(reusable_instance),
                        release.map_err(|_| host_invariant_error())
                    );
                },
                _ = cancellation.cancelled() => {
                    cancelled = true;
                },
            }
        }
    }

    #[cfg(any(test, feature = "testing"))]
    let (capability_identity, forged_capability_rejected, prior_capability_rejected) = {
        let bridge = &try_generated!(
            Some(reusable_instance),
            generated_state(reusable_instance.store.data()).map_err(wasmtime_anyhow)
        )
        .capability_bridge;
        if !bridge.is_active() {
            (0, false, false)
        } else {
            let capability_identity = bridge.handle().map_err(|_| host_invariant_error());
            let capability_identity =
                try_generated!(Some(reusable_instance), capability_identity) as u64;
            let forged_identity = if capability_identity == u64::MAX {
                1
            } else {
                capability_identity + 1
            };
            let forged_identity = InvocationCapabilityIdentity::from_abi(forged_identity as i64)
                .map_err(|_| host_invariant_error());
            let forged_identity = try_generated!(Some(reusable_instance), forged_identity);
            let forged_capability_rejected = matches!(
                bridge.authorize(forged_identity),
                Err(CapabilityBridgeError::ForgedIdentity)
            );
            let prior_identity = metrics
                .previous_capability_identity
                .swap(capability_identity, Ordering::SeqCst);
            let prior_capability_rejected = if prior_identity == 0 {
                false
            } else {
                let prior_identity = InvocationCapabilityIdentity::from_abi(prior_identity as i64)
                    .map_err(|_| host_invariant_error());
                let prior_identity = try_generated!(Some(reusable_instance), prior_identity);
                matches!(
                    bridge.authorize(prior_identity),
                    Err(CapabilityBridgeError::ForgedIdentity)
                )
            };
            (
                capability_identity,
                forged_capability_rejected,
                prior_capability_rejected,
            )
        }
    };

    let capability_cleanup_error = if try_generated!(
        Some(reusable_instance),
        generated_state_mut(reusable_instance.store.data_mut())
            .map_err(wasmtime_anyhow)
            .context(StaticHermesWasmExecutionFailure::RuntimeCleanup)
    )
    .capability_bridge
    .revoke()
    .is_err()
    {
        Some(
            wasmtime_anyhow(host_invariant_error())
                .context(StaticHermesWasmExecutionFailure::RuntimeCleanup),
        )
    } else {
        None
    };
    #[cfg(any(test, feature = "testing"))]
    let revoked_capability_rejected = if capability_identity == 0 {
        false
    } else {
        let presented = InvocationCapabilityIdentity::from_abi(capability_identity as i64)
            .map_err(|_| host_invariant_error());
        let presented = try_generated!(Some(reusable_instance), presented);
        matches!(
            try_generated!(
                Some(reusable_instance),
                generated_state(reusable_instance.store.data()).map_err(wasmtime_anyhow)
            )
            .capability_bridge
            .authorize(presented),
            Err(CapabilityBridgeError::Revoked)
        )
    };

    #[cfg(any(test, feature = "testing"))]
    if let Err(error) = &call_result
        && TEST_HOOKS.lock().as_ref().and_then(Weak::upgrade).is_some()
    {
        metrics.execution_errors.lock().push(format!("{error:#}"));
    }
    let (timeout_reason, caught_initialization_failure) = {
        let generated = try_generated!(
            Some(reusable_instance),
            generated_state(reusable_instance.store.data())
                .map_err(wasmtime_anyhow)
                .context(StaticHermesWasmExecutionFailure::ResultFinalization)
        );
        (
            generated.interrupt.timeout_reason(),
            generated.discard_after_caught_initialization_failure,
        )
    };
    let call_result = call_result.map_err(wasmtime_anyhow);
    // Compatibility entries may continue after a caught initialization failure.
    // Its retained diagnostic must not hide a later host-owned interruption.
    let timeout_reason = if reusable_instance.store.data().developer_error.is_some()
        && !caught_initialization_failure
    {
        None
    } else {
        match &call_result {
            Ok(0) => timeout_reason,
            Ok(_) => None,
            Err(error) => generated_error_timeout_reason(timeout_reason, error),
        }
    };
    // An active timer can fire just before a host call pauses user time. If
    // that call moved the transaction into a nested UDF, interrupting it drops
    // the transaction too. Disposal must not require a canonical result from
    // that abandoned provider. Independent host errors never take this path.
    let interrupted_host_call = matches!(
        timeout_reason,
        Some(GeneratedTimeoutReason::Initialization | GeneratedTimeoutReason::Active)
    ) && call_result
        .as_ref()
        .is_err_and(|error| error.is::<crate::timeout::TimeoutExpired>());
    let (mut outcome, mut system_error) = match timeout_reason {
        Some(GeneratedTimeoutReason::Initialization) => {
            (InvocationOutcome::InitializationTimeout, None)
        },
        Some(GeneratedTimeoutReason::Active) => (InvocationOutcome::ActiveTimeout, None),
        Some(GeneratedTimeoutReason::System(max_duration)) => (
            InvocationOutcome::SystemTimeout,
            Some(generated_system_timeout_error(max_duration).context(failure_subphase.clone())),
        ),
        Some(GeneratedTimeoutReason::Invalid) => (
            InvocationOutcome::SystemError,
            Some(
                anyhow::anyhow!("generated timeout returned an unsupported termination reason")
                    .context(failure_subphase.clone()),
            ),
        ),
        None => match call_result {
            Ok(0)
                if try_generated!(
                    Some(reusable_instance),
                    generated_state(reusable_instance.store.data())
                        .map_err(wasmtime_anyhow)
                        .context(StaticHermesWasmExecutionFailure::ResultFinalization)
                )
                .discard_after_caught_initialization_failure =>
            {
                (InvocationOutcome::Success, None)
            },
            Ok(0 | 1) if reusable_instance.store.data().developer_error.is_some() => (
                InvocationOutcome::DeveloperError(
                    reusable_instance
                        .store
                        .data()
                        .developer_error
                        .clone()
                        .expect("checked generated developer error disappeared")
                        .message,
                ),
                None,
            ),
            Ok(0) => (InvocationOutcome::Success, None),
            Ok(1) => (
                InvocationOutcome::DeveloperError("UncaughtWasmException".to_owned()),
                None,
            ),
            Ok(status) => {
                let hooks = TEST_HOOKS.lock().as_ref().and_then(Weak::upgrade);
                let diagnostic = reusable_instance
                    .store
                    .data()
                    .developer_error
                    .as_ref()
                    .map(|error| format!(" after reporting developer error {:?}", error.message))
                    .unwrap_or_default();
                let error = match hooks {
                    Some(hooks) => anyhow::anyhow!(
                        "generated Wasm guest returned unexpected status {status}{diagnostic} \
                         after fixed completion stages {:?}",
                        hooks.guest_native_completion_stages()
                    ),
                    None => anyhow::anyhow!(
                        "generated Wasm guest returned unexpected status {status}{diagnostic}"
                    ),
                };
                (
                    InvocationOutcome::SystemError,
                    // Only 0/1 are valid guest ABI statuses. This is an engine
                    // contract failure, not an application exception.
                    Some(error.context(StaticHermesWasmExecutionFailure::HostAbiInvariant)),
                )
            },
            Err(error)
                if matches!(error.downcast_ref::<Trap>(), Some(Trap::OutOfFuel))
                    && !error.is::<GeneratedNonRetryableFailure>() =>
            {
                // Cleanup can fail with fuel exhaustion or retain an earlier
                // fuel trap. Its typed failure must remain a system error.
                (InvocationOutcome::FuelExhausted, None)
            },
            Err(error)
                if reusable_instance.store.data().guest_developer_error
                    && !error.is::<GeneratedNonRetryableFailure>()
                    && !error.is::<ErrorMetadata>()
                    && !error.is::<GeneratedGuestMemoryLimit>()
                    && matches!(
                        error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
                        Some(
                            StaticHermesWasmExecutionFailure::RuntimeInitialization
                                | StaticHermesWasmExecutionFailure::GeneratedExportDispatch
                        )
                    ) =>
            {
                // Module evaluation may report its exception before the
                // initialization export fails. It is still a function error.
                (
                    InvocationOutcome::DeveloperError(
                        reusable_instance
                            .store
                            .data()
                            .developer_error
                            .as_ref()
                            .expect("reported guest exception disappeared")
                            .message
                            .clone(),
                    ),
                    None,
                )
            },
            Err(error) => {
                let error = if error.is::<StaticHermesWasmExecutionFailure>() {
                    error
                } else {
                    error.context(failure_subphase)
                };
                (
                    InvocationOutcome::SystemError,
                    Some(error.context("generated Wasm guest invocation failed")),
                )
            },
        },
    };
    let capability_cleanup_failed = capability_cleanup_error.is_some();
    if let Some(error) = capability_cleanup_error {
        outcome = InvocationOutcome::SystemError;
        merge_generated_execution_error(
            &mut system_error,
            error,
            "generated Wasm capability revocation failed",
        );
    }

    let state = reusable_instance.store.data_mut();
    let function_result = if outcome == InvocationOutcome::Success && !cancelled {
        let result = (|| -> anyhow::Result<Option<PendingValue>> {
            let generated = state
                .generated
                .as_mut()
                .context("generated invocation state disappeared")?;
            anyhow::ensure!(
                generated.async_operations.has_no_active_operations(),
                "generated guest left an async operation pending"
            );
            if let Some(request_handle) = generated.request_handle.take() {
                generated
                    .values
                    .release(
                        request_handle,
                        match generated.manifest.value_mode() {
                            ValueMode::Opaque => OpaqueValueKind::ConvexJson,
                            ValueMode::GuestNativeJson => OpaqueValueKind::Bytes,
                        },
                    )
                    .context("generated request handle was not retained")?;
            }
            if purpose == GeneratedExecutionPurpose::Preparation {
                generated
                    .values
                    .finish_invocation()
                    .context("generated preparation retained values or transferred a result")?;
                generated.async_operations.clear();
                return Ok(None);
            }
            let transferred = generated
                .values
                .take_final_result()
                .context("generated guest did not transfer a valid result")?;
            let maximum_result_bytes =
                usize::try_from(generated.manifest.limits().max_result_bytes())?;
            let result = match transferred {
                FinalGuestValue::Json(value) => PendingValue::from_uncommitted_json(value)
                    .context("generated Wasm guest returned an invalid Convex value")?,
                FinalGuestValue::Typed(value) => value,
            };
            let result_byte_count =
                serialized_json_size(&result.to_uncommitted_json_serializable())?;
            anyhow::ensure!(
                result_byte_count <= maximum_result_bytes
                    && result_byte_count <= *FUNCTION_MAX_RESULT_SIZE,
                "generated guest result exceeded its byte limit"
            );
            generated
                .values
                .finish_invocation()
                .context("generated guest leaked an opaque value handle")?;
            generated.async_operations.clear();
            Ok(Some(result))
        })();
        match result {
            Ok(result) => result,
            Err(error) => {
                outcome = InvocationOutcome::SystemError;
                let opaque_value_failure = error.downcast_ref::<OpaqueValueError>().map(|opaque| {
                    let failure = StaticHermesWasmExecutionFailure::from(opaque);
                    let diagnostic = StaticHermesWasmHostInvariantDiagnostic::from_opaque_at(
                        opaque,
                        std::panic::Location::caller(),
                    );
                    anyhow::Error::new(HostInvariant)
                        .context(diagnostic)
                        .context(failure)
                });
                let error = match opaque_value_failure {
                    Some(error) => error,
                    None => error.context(StaticHermesWasmExecutionFailure::ResultFinalization),
                };
                system_error = Some(error.context("generated Wasm result finalization failed"));
                state
                    .generated
                    .as_mut()
                    .expect("generated invocation state disappeared during cleanup")
                    .async_operations
                    .clear();
                state
                    .generated
                    .as_mut()
                    .expect("generated invocation state disappeared during value cleanup")
                    .values
                    .cleanup();
                None
            },
        }
    } else {
        try_generated!(
            Some(reusable_instance),
            state
                .generated
                .as_mut()
                .context("generated invocation state disappeared")
                .context(StaticHermesWasmExecutionFailure::RuntimeCleanup)
        )
        .async_operations
        .clear();
        try_generated!(
            Some(reusable_instance),
            state
                .generated
                .as_mut()
                .context("generated invocation state disappeared")
                .context(StaticHermesWasmExecutionFailure::RuntimeCleanup)
        )
        .values
        .cleanup();
        None
    };
    #[cfg(not(any(test, feature = "testing")))]
    let mut function_result = function_result;

    // A failed, cancelled, or trapped guest may abandon a query cursor before
    // queryStreamNext observes `done`. Those queries belong only to this
    // invocation. Remove them before finalization while retaining strict leak
    // detection for a guest that otherwise reports success.
    if outcome != InvocationOutcome::Success || cancelled {
        state.provider.clear_queries();
    }

    let finalized = (|| -> anyhow::Result<Option<InvocationOutput<RT>>> {
        anyhow::ensure!(
            !state.provider.has_active_queries(),
            "generated guest left a database query active"
        );
        let opaque_values = &state
            .generated
            .as_ref()
            .context("generated invocation state disappeared after cleanup")?
            .values;
        anyhow::ensure!(
            state
                .generated
                .as_ref()
                .context("generated invocation state disappeared after cleanup")?
                .async_operations
                .is_empty(),
            "generated invocation retained async operation state after cleanup"
        );
        anyhow::ensure!(
            opaque_values.live_handle_count() == 0 && opaque_values.accounting().current_bytes == 0,
            "generated invocation retained opaque resources after cleanup"
        );
        if cancelled || interrupted_host_call {
            // A cancelled or interrupted nested call may have taken the
            // transaction with it.
            // There is no result to publish; finish resource cleanup and dispose
            // the Store below without requiring transaction-backed finalization.
            // Disposal consumes Timeout's CPU permit if guest destruction is safe.
            return Ok(None);
        }
        #[cfg(any(test, feature = "testing"))]
        let read_accounting =
            read_accounting(&state.provider.tx_for_initialization()?.execution_size());
        #[cfg(any(test, feature = "testing"))]
        {
            metrics
                .read_documents
                .fetch_add(read_accounting.documents, Ordering::SeqCst);
            metrics
                .read_bytes
                .fetch_add(read_accounting.bytes, Ordering::SeqCst);
            metrics
                .read_intervals
                .fetch_add(read_accounting.intervals, Ordering::SeqCst);
        }
        #[cfg(any(test, feature = "testing"))]
        let observed_identity = state.provider.observed_identity();
        #[cfg(any(test, feature = "testing"))]
        let observed_rng = state.provider.observed_rng();
        #[cfg(any(test, feature = "testing"))]
        let observed_time = state.provider.observed_time();
        let host_operation_error = match &outcome {
            InvocationOutcome::DeveloperError(_) => state
                .developer_error
                .as_ref()
                .and_then(|error| error.host_operation_error),
            InvocationOutcome::Success
            | InvocationOutcome::InitializationTimeout
            | InvocationOutcome::ActiveTimeout
            | InvocationOutcome::FuelExhausted
            | InvocationOutcome::SystemTimeout
            | InvocationOutcome::SystemError => None,
        };
        let timeout = state.timeout.take().context("gate timeout already taken")?;
        let (execution_time, teardown_cpu_permit) = match purpose {
            GeneratedExecutionPurpose::Invocation => {
                let (time, permit) =
                    timeout.into_function_execution_time_with_permit(state.provider.udf_type())?;
                (Some(time), permit)
            },
            // Background preparation must not contribute a synthetic function
            // execution to user latency metrics.
            GeneratedExecutionPurpose::Preparation => (None, timeout.finish_with_permit()?),
        };
        state.teardown_cpu_permit = Some(teardown_cpu_permit);
        #[cfg(any(test, feature = "testing"))]
        let active_time = match &execution_time {
            Some(time) => time.elapsed,
            None => Duration::ZERO,
        };
        #[cfg(any(test, feature = "testing"))]
        let wall_time = wall_start.elapsed();
        #[cfg(any(test, feature = "testing"))]
        {
            if purpose == GeneratedExecutionPurpose::Invocation {
                metrics.active_times.lock().push(active_time);
                metrics.wall_times.lock().push(wall_time);
            }
        }
        if let Some(host_operation_error) = host_operation_error {
            if purpose == GeneratedExecutionPurpose::Invocation {
                state
                    .provider
                    .set_terminal_host_operation_error(host_operation_error);
            }
        }
        #[cfg(any(test, feature = "testing"))]
        let observed_function_result = function_result
            .as_ref()
            .map(PendingValue::to_uncommitted_json);
        let canonical_output = if purpose == GeneratedExecutionPurpose::Preparation {
            // Preparation returns its transaction and lifecycle outcome directly;
            // it must never manufacture a query or mutation result.
            None
        } else {
            let execution_time =
                execution_time.context("generated invocation has no execution time")?;
            match &outcome {
                InvocationOutcome::Success => {
                    #[cfg(any(test, feature = "testing"))]
                    let result = function_result
                        .clone()
                        .context("generated Wasm guest did not provide a function result")?;
                    #[cfg(not(any(test, feature = "testing")))]
                    let result = function_result
                        .take()
                        .context("generated Wasm guest did not provide a function result")?;
                    Some(state.provider.into_outcome(Ok(result), execution_time)?)
                },
                InvocationOutcome::DeveloperError(message) => Some(state.provider.into_outcome(
                    Err(common::errors::JsError::from_message(message.clone())),
                    execution_time,
                )?),
                InvocationOutcome::InitializationTimeout => Some(state.provider.into_outcome(
                    Err(common::errors::JsError::from_message(
                        "Function initialization timed out (maximum duration: 5s)".to_owned(),
                    )),
                    execution_time,
                )?),
                InvocationOutcome::ActiveTimeout => Some(state.provider.into_outcome(
                    Err(common::errors::JsError::from_message(format!(
                        "Function execution timed out (maximum duration: {:?})",
                        Duration::from_millis(
                            state
                                .generated
                                .as_ref()
                                .context(
                                    "generated invocation state disappeared during timeout \
                                     reporting"
                                )?
                                .manifest
                                .limits()
                                .timeout_milliseconds(),
                        )
                    ))),
                    execution_time,
                )?),
                InvocationOutcome::FuelExhausted => Some(state.provider.into_outcome(
                    Err(common::errors::JsError::from_message(
                        "Function execution exhausted its Wasm instruction budget".to_owned(),
                    )),
                    execution_time,
                )?),
                InvocationOutcome::SystemTimeout | InvocationOutcome::SystemError => None,
            }
        };
        let (transaction, function_outcome) = match canonical_output {
            Some((transaction, outcome)) => (transaction, Some(outcome)),
            None => {
                let preparation_log_lines = match purpose {
                    GeneratedExecutionPurpose::Preparation => {
                        Some(state.provider.take_preparation_log_lines()?)
                    },
                    GeneratedExecutionPurpose::Invocation => None,
                };
                let transaction = state.provider.take_transaction_for_system()?;
                return Ok(Some(InvocationOutput {
                    outcome: outcome.clone(),
                    function_outcome: None,
                    preparation_log_lines,
                    transaction,
                    #[cfg(any(test, feature = "testing"))]
                    host_operation_error,
                    #[cfg(any(test, feature = "testing"))]
                    function_result: observed_function_result,
                    #[cfg(any(test, feature = "testing"))]
                    opaque_live_handles: opaque_values.live_handle_count(),
                    #[cfg(any(test, feature = "testing"))]
                    opaque_current_bytes: opaque_values.accounting().current_bytes,
                    #[cfg(any(test, feature = "testing"))]
                    operation_count: state
                        .generated
                        .as_ref()
                        .context("generated invocation state disappeared during test accounting")?
                        .operation_count,
                    #[cfg(any(test, feature = "testing"))]
                    capability_revoked: state
                        .generated
                        .as_ref()
                        .context("generated invocation state disappeared during test accounting")?
                        .capability_bridge
                        .is_revoked(),
                    #[cfg(any(test, feature = "testing"))]
                    runtime_reuse_contaminated: state
                        .generated
                        .as_ref()
                        .context("generated invocation state disappeared during test accounting")?
                        .runtime_reuse_contaminated,
                    #[cfg(test)]
                    first_developer_error_index: metrics
                        .first_developer_error_index
                        .load(Ordering::SeqCst)
                        .checked_sub(1),
                    #[cfg(test)]
                    read_accounting,
                    #[cfg(any(test, feature = "testing"))]
                    active_time,
                    #[cfg(any(test, feature = "testing"))]
                    observed_identity,
                    #[cfg(any(test, feature = "testing"))]
                    observed_rng,
                    #[cfg(any(test, feature = "testing"))]
                    observed_time,
                    #[cfg(any(test, feature = "testing"))]
                    log_lines: vec![].into(),
                    #[cfg(any(test, feature = "testing"))]
                    journal: QueryJournal::new(),
                    #[cfg(any(test, feature = "testing"))]
                    audit_log_lines: vec![].into(),
                    #[cfg(any(test, feature = "testing"))]
                    syscall_trace: udf::SyscallTrace::new(),
                }));
            },
        };
        #[cfg(any(test, feature = "testing"))]
        let canonical_outcome = function_outcome
            .as_ref()
            .expect("canonical Static Hermes outcome disappeared");
        #[cfg(any(test, feature = "testing"))]
        let (host_operation_error, log_lines, journal, audit_log_lines, syscall_trace) =
            match canonical_outcome {
                FunctionOutcome::Query(outcome) | FunctionOutcome::Mutation(outcome) => (
                    outcome.host_operation_error,
                    outcome.log_lines.clone(),
                    outcome.journal.clone(),
                    outcome.audit_log_lines.clone(),
                    outcome.syscall_trace.clone(),
                ),
                FunctionOutcome::Action(_) | FunctionOutcome::HttpAction(_) => {
                    anyhow::bail!("Static Hermes generated execution produced an action outcome")
                },
            };
        #[cfg(any(test, feature = "testing"))]
        let (
            opaque_live_handles,
            opaque_current_bytes,
            operation_count,
            capability_revoked,
            runtime_reuse_contaminated,
        ) = {
            let generated = state
                .generated
                .as_ref()
                .context("generated invocation state disappeared during test accounting")?;
            (
                opaque_values.live_handle_count(),
                opaque_values.accounting().current_bytes,
                generated.operation_count,
                generated.capability_bridge.is_revoked(),
                generated.runtime_reuse_contaminated,
            )
        };
        #[cfg(test)]
        let first_developer_error_index = metrics
            .first_developer_error_index
            .load(Ordering::SeqCst)
            .checked_sub(1);
        Ok(Some(InvocationOutput {
            outcome: outcome.clone(),
            function_outcome,
            preparation_log_lines: None,
            transaction,
            #[cfg(any(test, feature = "testing"))]
            host_operation_error,
            #[cfg(any(test, feature = "testing"))]
            function_result: observed_function_result,
            #[cfg(any(test, feature = "testing"))]
            opaque_live_handles,
            #[cfg(any(test, feature = "testing"))]
            opaque_current_bytes,
            #[cfg(any(test, feature = "testing"))]
            operation_count,
            #[cfg(any(test, feature = "testing"))]
            capability_revoked,
            #[cfg(any(test, feature = "testing"))]
            runtime_reuse_contaminated,
            #[cfg(test)]
            first_developer_error_index,
            #[cfg(test)]
            read_accounting,
            #[cfg(any(test, feature = "testing"))]
            active_time,
            #[cfg(any(test, feature = "testing"))]
            observed_identity,
            #[cfg(any(test, feature = "testing"))]
            observed_rng,
            #[cfg(any(test, feature = "testing"))]
            observed_time,
            #[cfg(any(test, feature = "testing"))]
            log_lines,
            #[cfg(any(test, feature = "testing"))]
            journal,
            #[cfg(any(test, feature = "testing"))]
            audit_log_lines,
            #[cfg(any(test, feature = "testing"))]
            syscall_trace,
        }))
    })();

    let finalized_failed = finalized.is_err();
    let (runtime_reuse_contaminated, discard_after_caught_initialization_failure) = {
        let generated = try_generated!(
            Some(reusable_instance),
            generated_state(reusable_instance.store.data())
                .map_err(wasmtime_anyhow)
                .context(StaticHermesWasmExecutionFailure::RuntimeCleanup)
        );
        (
            generated.runtime_reuse_contaminated,
            generated.discard_after_caught_initialization_failure,
        )
    };
    let context_read_set_ready = !try_generated!(
        Some(reusable_instance),
        generated_state(reusable_instance.store.data())
            .map_err(wasmtime_anyhow)
            .context(StaticHermesWasmExecutionFailure::RuntimeCleanup)
    )
    .context_read_set_required
        || reusable_instance.context_read_set.is_some();
    let retain_instance = finalized.as_ref().is_ok_and(|output| {
        let Some(output) = output else {
            return false;
        };
        let state = reusable_instance.store.data();
        let outcome_can_reuse = generated_outcome_allows_runtime_reuse(
            &output.outcome,
            state.guest_developer_error,
            state.developer_error.is_some(),
        );
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        let outcome_can_reuse = outcome_can_reuse
            && (reusable_instance.initialized_memory.is_none()
                || output.outcome == InvocationOutcome::Success);
        reuse_instances
            && memory_reuse_allowed
            && reusable_instance.runtime_state == GeneratedRuntimeState::Live
            && outcome_can_reuse
            && !runtime_reuse_contaminated
            && !discard_after_caught_initialization_failure
            && context_read_set_ready
    });
    #[cfg(any(test, feature = "testing"))]
    if let Ok(output) = finalized.as_ref() {
        let transaction_write_count = match output {
            Some(output) => Some(output.transaction.writes().coalesced_writes().count()),
            // Cancellation or host interruption can leave no transaction to
            // inspect. Keep that absence distinct from a zero-write result.
            None => reusable_instance
                .store
                .data_mut()
                .provider
                .tx_for_initialization()
                .ok()
                .map(|transaction| transaction.writes().coalesced_writes().count()),
        };
        let state = reusable_instance.store.data();
        let generated = state
            .generated
            .as_ref()
            .expect("finalized generated invocation state disappeared");
        let outcome_allows_reuse = !cancelled
            && generated_outcome_allows_runtime_reuse(
                &outcome,
                state.guest_developer_error,
                state.developer_error.is_some(),
            );
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        let outcome_allows_reuse = outcome_allows_reuse
            && (reusable_instance.initialized_memory.is_none()
                || outcome == InvocationOutcome::Success);
        let (deployment_sha256, package_key) = match &reusable_instance.route_identity {
            GeneratedRouteIdentity::Singleton { package_key } => (None, package_key.clone()),
            GeneratedRouteIdentity::DeploymentExport {
                deployment_sha256,
                package_key,
                ..
            } => (Some(deployment_sha256.clone()), package_key.clone()),
        };
        let entry_id = match &*reusable_instance.routed.package_identity {
            ValidatedWasmUdfPackageIdentity::Legacy(_) => None,
            ValidatedWasmUdfPackageIdentity::CapabilityEntry(identity) => {
                entry_selector.and_then(|selector| {
                    identity
                        .routes()
                        .iter()
                        .find(|route| {
                            u64::from_str_radix(route.entry_selector_id(), 16) == Ok(selector)
                        })
                        .map(|route| route.entry_id().to_owned())
                })
            },
            ValidatedWasmUdfPackageIdentity::ModuleGraphCohort(identity) => entry_selector
                .and_then(|selector| {
                    identity
                        .routes()
                        .iter()
                        .find(|route| {
                            u64::from_str_radix(route.entry_selector_id(), 16) == Ok(selector)
                        })
                        .map(|route| route.entry_id().to_owned())
                }),
        };
        metrics
            .generated_invocations
            .lock()
            .push(StaticHermesGeneratedInvocationObservation {
                invocation_id,
                runtime_id: reusable_instance.id,
                memory_slot_id: memory_slot_id.test_identity(),
                deployment_sha256,
                package_key,
                entry_id,
                entry_selector_id: entry_selector
                    .map(|entry_selector| format!("{entry_selector:016x}")),
                capability_identity,
                runtime_was_created,
                // Module-graph invocations can only reach this point after each
                // authenticated graph member was deserialized as Wasmtime AOT.
                serialized_aot_module_loaded: reusable_instance.routed.emscripten_graph.is_some(),
                operation_count: generated.operation_count,
                transaction_write_count,
                capability_revoked: generated.capability_bridge.is_revoked(),
                forged_capability_rejected,
                prior_capability_rejected,
                revoked_capability_rejected,
                opaque_live_handles: generated.values.live_handle_count(),
                opaque_current_bytes: generated.values.accounting().current_bytes,
                runtime_reuse_contaminated: generated.runtime_reuse_contaminated,
                retain_instance,
                reuse_instances,
                outcome_allows_reuse,
                guest_developer_error: state.guest_developer_error,
                developer_error_reported: state.developer_error.is_some(),
                discard_after_caught_initialization_failure,
                context_read_set_ready,
                cancelled,
            });
    }
    let terminal_memory_outcome = if finalized_failed || capability_cleanup_failed {
        TerminalMemoryOutcome::SystemError
    } else if cancelled {
        TerminalMemoryOutcome::Cancellation
    } else {
        match outcome {
            InvocationOutcome::Success => TerminalMemoryOutcome::Success,
            InvocationOutcome::DeveloperError(_) => TerminalMemoryOutcome::DeveloperError,
            InvocationOutcome::InitializationTimeout
            | InvocationOutcome::ActiveTimeout
            | InvocationOutcome::SystemTimeout => TerminalMemoryOutcome::Timeout,
            InvocationOutcome::FuelExhausted => TerminalMemoryOutcome::ResourceLimit,
            InvocationOutcome::SystemError => TerminalMemoryOutcome::SystemError,
        }
    };
    let invocation = match finalized {
        Ok(invocation) => invocation,
        Err(error) => {
            merge_generated_execution_error(
                &mut system_error,
                error.context(StaticHermesWasmExecutionFailure::ResultFinalization),
                "generated Wasm invocation finalization failed",
            );
            None
        },
    };
    let memory_permit = reusable_instance
        .store
        .data_mut()
        .generated
        .as_mut()
        .context("generated invocation state disappeared before memory completion")
        .and_then(|generated| {
            generated
                .memory_permit
                .take()
                .context("generated invocation memory permit disappeared before completion")
        });
    let secret_cleanup = {
        let completed_state = reusable_instance.store.data_mut();
        completed_state
            .generated
            .as_mut()
            .context("generated invocation state disappeared before secret cleanup")
            .map(|generated| generated.host_secret_values.clear())
    };
    if let Err(error) = secret_cleanup {
        merge_generated_execution_error(
            &mut system_error,
            error.context(StaticHermesWasmExecutionFailure::RuntimeCleanup),
            "generated Wasm secret cleanup failed",
        );
        let error = system_error
            .take()
            .context("generated Wasm secret cleanup lost its internal error")?;
        return Err(discard_optional_generated_instance(Some(reusable_instance), error).await);
    }
    let (mut memory_permit, mut cleanup_failed) = match memory_permit {
        Ok(memory_permit) => (Some(memory_permit), false),
        Err(error) => {
            merge_generated_execution_error(
                &mut system_error,
                error.context(StaticHermesWasmExecutionFailure::RuntimeCleanup),
                "generated Wasm memory completion failed",
            );
            (None, true)
        },
    };
    let retain_instance = retain_instance && memory_permit.is_some();
    assert!(
        !retain_instance || reusable_instance.store.data().provider.is_finished(),
        "generated pooled runtime retained a database invocation provider"
    );
    let (reusable_instance, memory_permit) = if retain_instance {
        (Some(reusable_instance), memory_permit)
    } else {
        let discard_result = discard_generated_instance(reusable_instance).await;
        if let Some(memory_permit) = memory_permit.take() {
            memory_permit.finish(
                if discard_result.is_err() {
                    TerminalMemoryOutcome::SystemError
                } else {
                    terminal_memory_outcome
                },
                false,
            );
        }
        if let Err(error) = discard_result {
            cleanup_failed = true;
            merge_generated_execution_error(
                &mut system_error,
                error.context(StaticHermesWasmExecutionFailure::RuntimeCleanup),
                "generated Wasm runtime cleanup failed",
            );
        }
        (None, None)
    };
    if cleanup_failed || finalized_failed {
        return Err(system_error
            .context("generated Wasm execution failed without a retained internal error")?);
    }
    if cancelled {
        #[cfg(any(test, feature = "testing"))]
        if terminal_memory_outcome == TerminalMemoryOutcome::Cancellation {
            metrics
                .terminal_cancellations
                .fetch_add(1, Ordering::SeqCst);
        }
        // The Result API has no transaction-bearing output for a cancelled
        // call. All disposal and memory accounting have completed above.
        return Err(system_error
            .unwrap_or_else(|| anyhow::anyhow!("generated Wasm execution cancelled"))
            .context(GeneratedNonRetryableFailure::Cancelled));
    }
    if interrupted_host_call {
        // Cleanup errors returned above keep their veto even when the host
        // interruption itself would otherwise authorize recovery.
        return Err(match outcome {
            InvocationOutcome::InitializationTimeout => {
                StaticHermesWasmExecutionFailure::InitializationTimeout.into()
            },
            InvocationOutcome::ActiveTimeout => {
                StaticHermesWasmExecutionFailure::ExecutionTimeout.into()
            },
            InvocationOutcome::SystemError => {
                system_error.context("interrupted host call lost its cleanup error")?
            },
            InvocationOutcome::Success
            | InvocationOutcome::DeveloperError(_)
            | InvocationOutcome::FuelExhausted
            | InvocationOutcome::SystemTimeout => {
                unreachable!("interrupted host call lost its timeout outcome")
            },
        });
    }
    let invocation = invocation.expect("checked generated invocation disappeared");
    let system_error = match invocation.outcome {
        InvocationOutcome::SystemTimeout | InvocationOutcome::SystemError => {
            Some(system_error.context("generated Wasm system outcome lost its internal error")?)
        },
        _ => {
            anyhow::ensure!(
                system_error.is_none(),
                "generated Wasm non-system outcome retained an internal error"
            );
            None
        },
    };

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    if retain_instance
        && !cancellation.is_cancelled()
        && let Some(image) = captured_image
    {
        prepared_images
            .as_ref()
            .expect("captured image lost its preparation cache")
            .publish(image);
    }
    Ok(GeneratedExecutionOutput {
        invocation,
        reusable_instance,
        memory_permit,
        terminal_memory_outcome,
        system_error,
        #[cfg(test)]
        runtime_id,
        #[cfg(test)]
        partial_initialization_trace,
    })
}

fn read_accounting(size: &FunctionExecutionSize) -> ReadAccounting {
    ReadAccounting {
        documents: size.read_size.total_document_count,
        bytes: size.read_size.total_document_size,
        intervals: size.num_intervals,
    }
}

pub(super) struct GeneratedLifecycleBarrier {
    claimed: AtomicBool,
    directory: PathBuf,
    udf_path: CanonicalizedUdfPath,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GeneratedLifecycleBarrierArrival<'a> {
    current_sha256: &'a str,
    deployment_sha256: &'a str,
    generation_sha256: &'a str,
    invocation_id: u64,
    kind: &'static str,
    udf_path: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GeneratedLifecycleBarrierOutcome {
    Continue,
    Cancelled,
}

impl GeneratedLifecycleBarrier {
    pub(super) fn load(directory: PathBuf, udf_path: String) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(
            directory.is_absolute(),
            "{LIFECYCLE_BARRIER_DIRECTORY_ENV} must be an absolute path"
        );
        let canonical = directory
            .canonicalize()
            .with_context(|| format!("failed to resolve {LIFECYCLE_BARRIER_DIRECTORY_ENV}"))?;
        anyhow::ensure!(
            canonical == directory,
            "{LIFECYCLE_BARRIER_DIRECTORY_ENV} must be a canonical path"
        );
        let metadata = fs::symlink_metadata(&directory)
            .with_context(|| format!("failed to inspect {LIFECYCLE_BARRIER_DIRECTORY_ENV}"))?;
        anyhow::ensure!(
            metadata.file_type().is_dir() && has_exact_control_permissions(&metadata, 0o700),
            "{LIFECYCLE_BARRIER_DIRECTORY_ENV} must be a mode-0700 directory"
        );
        anyhow::ensure!(
            fs::read_dir(&directory)
                .with_context(|| format!("failed to read {LIFECYCLE_BARRIER_DIRECTORY_ENV}"))?
                .next()
                .is_none(),
            "{LIFECYCLE_BARRIER_DIRECTORY_ENV} must be empty at startup"
        );
        let udf_path = udf_path
            .parse::<CanonicalizedUdfPath>()
            .with_context(|| format!("failed to parse {LIFECYCLE_BARRIER_UDF_PATH_ENV}"))?;
        Ok(Arc::new(Self {
            claimed: AtomicBool::new(false),
            directory,
            udf_path,
        }))
    }

    fn claim(&self, udf_path: &CanonicalizedUdfPath) -> bool {
        udf_path == &self.udf_path
            && self
                .claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    async fn wait(
        &self,
        generation: &DeploymentGeneration,
        invocation_id: u64,
        cancellation: &CancellationSignal,
    ) -> anyhow::Result<GeneratedLifecycleBarrierOutcome> {
        if cancellation.is_cancelled() {
            return Ok(GeneratedLifecycleBarrierOutcome::Cancelled);
        }
        anyhow::ensure!(
            read_generated_lifecycle_control_file(
                &self
                    .directory
                    .join(GENERATED_LIFECYCLE_BARRIER_RELEASE_FILE),
            )?
            .is_none(),
            "generated Wasm lifecycle barrier release exists before arrival"
        );
        let udf_path = self.udf_path.to_string();
        let mut arrival = serde_json::to_vec(&GeneratedLifecycleBarrierArrival {
            current_sha256: &generation.current_sha256,
            deployment_sha256: generation.deployment_sha256(),
            generation_sha256: &generation.generation_sha256,
            invocation_id,
            kind: GENERATED_LIFECYCLE_BARRIER_PROTOCOL,
            udf_path: &udf_path,
        })?;
        arrival.push(b'\n');
        publish_generated_lifecycle_control_file(
            &self.directory,
            GENERATED_LIFECYCLE_BARRIER_ARRIVAL_FILE,
            &arrival,
        )?;

        loop {
            match read_generated_lifecycle_control_file(
                &self
                    .directory
                    .join(GENERATED_LIFECYCLE_BARRIER_RELEASE_FILE),
            )? {
                Some(release) => {
                    anyhow::ensure!(
                        release == arrival,
                        "generated Wasm lifecycle barrier release does not match its arrival"
                    );
                    return Ok(GeneratedLifecycleBarrierOutcome::Continue);
                },
                None => {},
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    return Ok(GeneratedLifecycleBarrierOutcome::Cancelled);
                },
                _ = tokio::time::sleep(GENERATED_LIFECYCLE_BARRIER_POLL_INTERVAL) => {},
            }
        }
    }
}

pub(super) fn publish_generated_lifecycle_control_file(
    directory: &Path,
    name: &str,
    bytes: &[u8],
) -> anyhow::Result<()> {
    let temporary = directory.join(format!(".{name}.tmp"));
    let published = directory.join(name);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let result = (|| -> anyhow::Result<()> {
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("failed to create generated Wasm lifecycle {name}"))?;
        file.write_all(bytes)
            .with_context(|| format!("failed to write generated Wasm lifecycle {name}"))?;
        file.sync_all()
            .with_context(|| format!("failed to sync generated Wasm lifecycle {name}"))?;
        fs::hard_link(&temporary, &published)
            .with_context(|| format!("failed to publish generated Wasm lifecycle {name}"))?;
        fs::remove_file(&temporary)
            .with_context(|| format!("failed to remove generated Wasm lifecycle {name} staging"))?;
        File::open(directory)
            .and_then(|directory| directory.sync_all())
            .with_context(|| format!("failed to sync generated Wasm lifecycle {name} directory"))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub(super) fn read_generated_lifecycle_control_file(
    path: &Path,
) -> anyhow::Result<Option<Vec<u8>>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to open generated Wasm lifecycle control file {path:?}")
            });
        },
    };
    let metadata = file.metadata().with_context(|| {
        format!("failed to inspect generated Wasm lifecycle control file {path:?}")
    })?;
    anyhow::ensure!(
        metadata.file_type().is_file() && has_exact_control_permissions(&metadata, 0o600),
        "generated Wasm lifecycle control file must be a mode-0600 regular file"
    );
    anyhow::ensure!(
        metadata.len() <= 4096,
        "generated Wasm lifecycle control file exceeds 4096 bytes"
    );
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len())?);
    file.read_to_end(&mut bytes).with_context(|| {
        format!("failed to read generated Wasm lifecycle control file {path:?}")
    })?;
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use futures::poll;

    use super::*;

    #[test]
    fn generated_epoch_checks_timeout_and_poll_budget_without_watchdog() -> anyhow::Result<()> {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let database = new_test_database(rt.clone()).await?;
            let mut state = generated_test_state(
                rt.clone(),
                database.begin_system().await?,
                generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?,
                JsonValue::Null,
                UdfType::Query,
                UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                CancellationSignal::new_for_test(),
                None,
                Arc::new(GateMetrics::default()),
                None,
            )
            .await?;
            let engine = new_generated_engine(WasmMeteringMode::EpochChecks)?;
            let module = Module::new(
                &engine,
                r#"
                (module
                    (import "test" "tick" (func $tick))
                    (import "test" "wait" (func $wait))
                    (func $start (call $tick))
                    (start $start)
                    (func (export "step") (call $tick))
                    (func (export "spin") (loop (call $tick) (br 0)))
                    (func (export "wait_then_step") (call $wait) (call $tick)))
            "#,
            )
            .map_err(wasmtime_anyhow)?;
            let mut linker = Linker::new(&engine);
            let ticker = Arc::clone(&engine);
            linker.func_wrap("test", "tick", move || ticker.increment_epoch())?;
            let resumed = Arc::new(AtomicBool::new(false));
            let resume = Arc::clone(&resumed);
            linker.func_wrap_async(
                "test",
                "wait",
                move |_: Caller<'_, HostState<ProdRuntime>>, (): ()| {
                    let resumed = Arc::clone(&resumed);
                    Box::new(std::future::poll_fn(move |_| {
                        if resumed.load(Ordering::Acquire) {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    }))
                },
            )?;
            arm_generated_timeout(
                rt.clone(),
                &mut state,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )?;
            let clock = state.wasi_cpu_clock.clone();
            let interrupt = Arc::clone(&generated_state(&state)?.interrupt);
            let mut store = Store::new(&engine, state);
            store.set_fuel(u64::MAX)?;
            arm_generated_store_epoch(&mut store);
            engine.increment_epoch();
            let instance = clock
                .within_poll_budget(linker.instantiate_async(&mut store, &module))
                .await?;
            let step = instance.get_typed_func::<(), ()>(&mut store, "step")?;
            let spin = instance.get_typed_func::<(), ()>(&mut store, "spin")?;
            let wait = instance.get_typed_func::<(), ()>(&mut store, "wait_then_step")?;

            // Manual polls on this current-thread runtime cannot run the
            // watchdog. A healthy epoch inside a short call must Continue.
            engine.increment_epoch();
            let mut call = Box::pin(clock.within_poll_budget(step.call_async(&mut store, ())));
            assert!(matches!(poll!(call.as_mut()), Poll::Ready(Ok(()))));
            drop(call);

            engine.increment_epoch();
            let mut call = Box::pin(clock.within_poll_budget(async {
                std::thread::sleep(*STATIC_HERMES_WASM_CONTINUOUS_POLL_LIMIT);
                // Ready transitions and nested teardown scopes must inherit
                // the outer poll's already consumed allowance.
                clock
                    .within_poll_budget(step.call_async(&mut store, ()))
                    .await
            }));
            assert!(poll!(call.as_mut()).is_pending());
            assert!(matches!(poll!(call.as_mut()), Poll::Ready(Ok(()))));
            drop(call);

            let started = Instant::now();
            let mut call = Box::pin(clock.within_poll_budget(spin.call_async(&mut store, ())));
            assert!(poll!(call.as_mut()).is_pending());
            assert!(started.elapsed() >= *STATIC_HERMES_WASM_CONTINUOUS_POLL_LIMIT);
            drop(call);
            assert!(interrupt.timeout_reason().is_none());

            let mut call = Box::pin(clock.within_poll_budget(async {
                wait.call_async(&mut store, ()).await?;
                // Force a callback after the real host suspension even if the
                // guest return path contains no epoch check of its own.
                engine.increment_epoch();
                step.call_async(&mut store, ()).await
            }));
            assert!(poll!(call.as_mut()).is_pending());
            std::thread::sleep(
                *STATIC_HERMES_WASM_CONTINUOUS_POLL_LIMIT + *STATIC_HERMES_WASM_EPOCH_TICK_INTERVAL,
            );
            resume.store(true, Ordering::Release);
            // Host suspension did not use up the resumed poll's allowance.
            assert!(matches!(poll!(call.as_mut()), Poll::Ready(Ok(()))));
            drop(call);

            // The callback must trap in this first poll, before a Yield could
            // give the watchdog an opportunity to publish the timeout.
            arm_generated_timeout(
                rt.clone(),
                store.data_mut(),
                Duration::ZERO,
                Duration::from_secs(60),
            )?;
            engine.increment_epoch();
            let mut call = Box::pin(clock.within_poll_budget(spin.call_async(&mut store, ())));
            let Poll::Ready(Err(error)) = poll!(call.as_mut()) else {
                panic!("expired guest did not synchronously interrupt");
            };
            assert_eq!(error.downcast_ref::<Trap>(), Some(&Trap::Interrupt));
            assert!(matches!(
                interrupt.timeout_reason(),
                Some(GeneratedTimeoutReason::Active)
            ));
            drop(call);

            // Destruction ignores the old invocation reason, but uses the
            // exact deadline also supplied to its suspended-operation timer.
            interrupt.begin_destruction(tokio::time::Instant::now() + Duration::from_secs(1));
            engine.increment_epoch();
            let mut call = Box::pin(clock.within_poll_budget(step.call_async(&mut store, ())));
            assert!(matches!(poll!(call.as_mut()), Poll::Ready(Ok(()))));
            drop(call);
            interrupt.begin_destruction(tokio::time::Instant::now());
            engine.increment_epoch();
            let mut call = Box::pin(clock.within_poll_budget(spin.call_async(&mut store, ())));
            let Poll::Ready(Err(error)) = poll!(call.as_mut()) else {
                panic!("expired destruction did not synchronously interrupt");
            };
            assert_eq!(error.downcast_ref::<Trap>(), Some(&Trap::Interrupt));
            drop(call);

            let preparation = Arc::new(GeneratedInterruptState::default());
            preparation.set_execution_phase(GeneratedExecutionPhase::Preparing);
            generated_state_mut(store.data_mut())?.interrupt = Arc::clone(&preparation);
            arm_generated_timeout(
                rt,
                store.data_mut(),
                Duration::ZERO,
                Duration::from_secs(60),
            )?;
            // Rearming is a synchronous lifecycle transition: it must reject
            // expiration without entering a handler or waiting for an epoch.
            let error = rearm_generated_active_timeout(store.data_mut(), Duration::from_secs(60))
                .map_err(WasmtimeError::from_anyhow)
                .unwrap_err();
            assert!(!preparation.is_active());
            assert!(matches!(
                generated_error_timeout_reason(
                    preparation.timeout_reason(),
                    &wasmtime_anyhow(error),
                ),
                Some(GeneratedTimeoutReason::Initialization)
            ));
            Ok(())
        })
    }

    #[test]
    fn generated_wasm_memory_failed_gc_growth_blocks_host_work() -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on("failed_gc_growth_host_work", async move {
            let database = new_test_database(rt.clone()).await?;
            let mut state = generated_test_state(
                rt,
                database.begin_system().await?,
                generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?,
                JsonValue::Null,
                UdfType::Query,
                UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                CancellationSignal::new_for_test(),
                None,
                Arc::new(GateMetrics::default()),
                None,
            )
            .await?;
            generated_state_mut(&mut state)?.capability_bridge.issue()?;
            generated_state_mut(&mut state)?
                .interrupt
                .set_execution_phase(GeneratedExecutionPhase::Active);
            let capability = generated_state(&state)?.capability_bridge.handle()?;
            assert!(authorize_generated_capability(&mut state, capability)?);
            require_active_generated_execution(&state)?;
            let generated = generated_state_mut(&mut state)?;
            let handles = generated.values.live_handle_count();
            // The GC can swallow this error and satisfy its allocation after
            // collection. The host must still refuse the next operation.
            assert!(wasmtime::ResourceLimiter::memory_grow_failed(
                &mut generated.memory_limiter,
                WasmtimeError::msg("allocation failed"),
            )
            .is_err());
            assert!(generated
                .count_operation()
                .unwrap_err()
                .is::<GeneratedGuestMemoryLimit>());
            assert_eq!(generated.operation_count, 0);
            assert!(generated
                .opaque_value_operation(|values| values.insert_null())
                .unwrap_err()
                .is::<GeneratedGuestMemoryLimit>());
            assert_eq!(generated.values.live_handle_count(), handles);
            assert!(authorize_generated_capability(&mut state, capability)
                .unwrap_err()
                .is::<GeneratedGuestMemoryLimit>());
            assert!(require_active_generated_execution(&state)
                .unwrap_err()
                .is::<GeneratedGuestMemoryLimit>());
            Ok(())
        })
    }

    #[test]
    fn generated_wasm_memory_uncertified_provider_allows_named_memory() -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on("uncertified_memory_provider", async move {
            let database = new_test_database(rt.clone()).await?;
            let state = generated_test_state(
                rt,
                database.begin_system().await?,
                generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?,
                JsonValue::Null,
                UdfType::Query,
                UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                CancellationSignal::new_for_test(),
                None,
                Arc::new(GateMetrics::default()),
                None,
            )
            .await?;
            let engine = Engine::default();
            let mut memories = EncodedMemorySection::new();
            memories.memory(EncodedMemoryType {
                minimum: 1,
                maximum: Some(1),
                memory64: false,
                shared: false,
                page_size_log2: None,
            });
            let mut exports = EncodedExportSection::new();
            exports.export("shared_memory", EncodedExportKind::Memory, 0);
            let mut encoded = EncodedModule::new();
            encoded.section(&memories).section(&exports);
            let module = Module::new(&engine, encoded.finish()).map_err(wasmtime_anyhow)?;
            let mut store = GeneratedStore::from(Store::new(&engine, state));
            store.limiter(|state| &mut state.generated.as_mut().unwrap().memory_limiter);
            generated_state_mut(store.data_mut())?
                .memory_permit
                .as_mut()
                .unwrap()
                .mark_execution_started(false);
            generated_state_mut(store.data_mut())?
                .memory_permit
                .as_ref()
                .unwrap()
                .begin_static_memory_instantiation()?;
            let instance =
                wasmtime::Instance::new(&mut store, &module, &[]).map_err(wasmtime_anyhow)?;
            let memory = instance.get_memory(&mut store, "shared_memory").unwrap();
            let evidence = OnceLock::new();
            account_generated_initial_static_memory(&mut store, &module, &evidence, memory)?;
            assert_eq!(evidence.get().unwrap().shared_bytes, 0);
            assert_eq!(
                generated_state_mut(store.data_mut())?
                    .memory_permit
                    .as_ref()
                    .unwrap()
                    .current_bytes()
                    .0,
                65_536,
            );
            Ok(())
        })
    }

    #[test]
    fn generated_wasm_memory_backtrace_retains_module_owner_and_failure() -> anyhow::Result<()> {
        let routed = generated_memory_test_routed_module(
            generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?,
            GeneratedMemoryTestOperation {
                additional_pages: 0,
                destroy_infinite_loop: false,
                loop_iterations: Some(0),
                maximum_pages: 1,
            },
            GeneratedRouteIdentity::Singleton {
                package_key: "backtrace-ownership".to_owned(),
            },
        )?;
        let retained = Arc::downgrade(&routed);
        let store = Store::new(&routed.engine, ());
        let diagnostic = StaticHermesWasmHostInvariantDiagnostic::at_current_boundary();
        let error = anyhow::Error::new(Trap::OutOfFuel)
            .context(WasmBacktrace::capture(&store))
            .context(diagnostic)
            .context(StaticHermesWasmExecutionFailure::RuntimeInitialization);
        let error = retain_generated_error_modules(error, &routed);
        assert_eq!(
            error.downcast_ref::<StaticHermesWasmHostInvariantDiagnostic>(),
            Some(&diagnostic)
        );
        let error = allow_disposed_wasm_primary_fallback(error);
        assert!(!error.is::<StaticHermesWasmFallbackReason>());
        drop(error);
        let error = anyhow::Error::new(Trap::Interrupt)
            .context(WasmBacktrace::capture(&store))
            .context(StaticHermesWasmExecutionFailure::InitializationTimeout);
        let error =
            allow_disposed_wasm_primary_fallback(retain_generated_error_modules(error, &routed));
        assert_eq!(
            error.downcast_ref::<StaticHermesWasmFallbackReason>(),
            Some(&StaticHermesWasmFallbackReason::InitializationTimeout)
        );
        drop(error);
        let error = anyhow::anyhow!("guest failed")
            .context(WasmBacktrace::capture(&store))
            .context(ErrorMetadata::bad_request(
                "InvalidFixture",
                "fixture failed",
            ))
            .context(StaticHermesGeneratedExportDiagnostic::FirstSelectorTrap)
            .context(StaticHermesWasmExecutionFailure::GuestExecution);
        let error = retain_generated_error_modules(error, &routed);
        drop(store);
        drop(routed);
        assert!(retained.upgrade().is_some());
        assert!(error.is::<ErrorMetadata>());
        assert_eq!(
            error.downcast_ref::<StaticHermesGeneratedExportDiagnostic>(),
            Some(&StaticHermesGeneratedExportDiagnostic::FirstSelectorTrap),
        );
        assert_eq!(
            error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
            Some(&StaticHermesWasmExecutionFailure::GuestExecution),
        );
        drop(error);
        assert!(retained.upgrade().is_none());
        Ok(())
    }

    #[test]
    fn guest_reported_developer_error_allows_runtime_reuse() {
        assert!(generated_outcome_allows_runtime_reuse(
            &InvocationOutcome::DeveloperError("TypeError".to_owned()),
            true,
            true,
        ));
    }

    #[test]
    fn host_reported_deterministic_developer_error_retires_runtime() {
        assert!(!generated_outcome_allows_runtime_reuse(
            &InvocationOutcome::DeveloperError("InvalidArgument".to_owned()),
            false,
            true,
        ));
    }

    #[test]
    fn fired_timer_does_not_replace_an_independent_failure() {
        let interrupt = GeneratedInterruptState::default();
        interrupt.terminate(IsolateTerminationReason::UserTimeout(Duration::from_secs(1)).into());
        let provider = wasmtime_anyhow(generated_host_operation_error(
            ErrorMetadata::overloaded("DatabaseUnavailable", "database unavailable").into(),
        ));
        assert!(generated_error_timeout_reason(interrupt.timeout_reason(), &provider).is_none());
        assert_eq!(provider.short_msg(), "DatabaseUnavailable");
        assert!(
            !allow_disposed_wasm_primary_fallback(provider).is::<StaticHermesWasmFallbackReason>()
        );

        let fuel = anyhow::Error::new(Trap::OutOfFuel);
        assert!(generated_error_timeout_reason(interrupt.timeout_reason(), &fuel).is_none());
        let mut cleanup = Some(anyhow::Error::new(Trap::Interrupt));
        merge_generated_execution_error(
            &mut cleanup,
            anyhow::anyhow!("accounting failed"),
            "memory reconciliation failed",
        );
        let cleanup = cleanup.unwrap();
        assert!(generated_error_timeout_reason(interrupt.timeout_reason(), &cleanup).is_none());
        assert!(
            !allow_disposed_wasm_primary_fallback(cleanup).is::<StaticHermesWasmFallbackReason>()
        );

        let timeout = wasmtime_anyhow(generated_host_operation_error(
            anyhow::Error::new(crate::timeout::TimeoutExpired).context(ErrorMetadata::bad_request(
                "SystemTimeoutError",
                SYSTEM_TIMEOUT_ERROR_MESSAGE,
            )),
        ));
        assert!(matches!(
            generated_error_timeout_reason(interrupt.timeout_reason(), &timeout),
            Some(GeneratedTimeoutReason::Active)
        ));
        assert!(
            generated_error_timeout_reason(None, &anyhow::Error::new(Trap::Interrupt)).is_none()
        );
    }

    #[test]
    fn cleanup_preserves_the_primary_diagnostic_but_prevents_fallback() {
        let cleanup =
            anyhow::anyhow!("cleanup").context(StaticHermesWasmExecutionFailure::RuntimeCleanup);
        let mut error = None;
        merge_generated_execution_error(&mut error, cleanup, "cleanup failed");
        assert_eq!(
            error
                .as_ref()
                .and_then(|error| error.downcast_ref::<StaticHermesWasmExecutionFailure>()),
            Some(&StaticHermesWasmExecutionFailure::RuntimeCleanup)
        );

        let mut error = Some(
            anyhow::anyhow!("guest").context(StaticHermesWasmExecutionFailure::GuestMemoryLimit),
        );
        let cleanup =
            anyhow::anyhow!("cleanup").context(StaticHermesWasmExecutionFailure::RuntimeCleanup);
        merge_generated_execution_error(&mut error, cleanup, "cleanup also failed");
        let error = error.expect("merged execution error disappeared");
        assert_eq!(
            error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
            Some(&StaticHermesWasmExecutionFailure::GuestMemoryLimit)
        );
        assert!(format!("{error:#}").contains("cleanup also failed"));
        let error = allow_disposed_wasm_primary_fallback(error);
        assert!(!error.is::<StaticHermesWasmFallbackReason>());
    }

    #[test]
    fn serialized_json_size_matches_materialized_json_bytes() -> anyhow::Result<()> {
        let values = [
            json!(null),
            json!({"key":"value","nested":[true, "\u{1f980}"]}),
            json!("x".repeat(65_536)),
        ];
        for value in &values {
            assert_eq!(
                serialized_json_size(value)?,
                serde_json::to_vec(value)?.len()
            );
        }
        Ok(())
    }

    #[test]
    fn trap_stderr_classification_recognizes_fixed_runtime_marker() {
        assert_eq!(
            wasmtime_trap_stderr_classification(b""),
            StaticHermesWasmTrapStderrClassification::Empty
        );
        assert_eq!(
            wasmtime_trap_stderr_classification(STATIC_HERMES_UNCAUGHT_EXCEPTION_STDERR),
            StaticHermesWasmTrapStderrClassification::StaticHermesUncaughtException
        );
        assert_eq!(
            wasmtime_trap_stderr_classification(b"fatal hermes::GCBase::oom"),
            StaticHermesWasmTrapStderrClassification::HermesHeapOutOfMemory
        );
        assert_eq!(
            wasmtime_trap_stderr_classification(b"SH: uncaught exception\n"),
            StaticHermesWasmTrapStderrClassification::Unclassified
        );
    }

    #[tokio::test]
    async fn idle_teardown_does_not_overtake_primary_cpu_admission() {
        let limiter = ConcurrencyLimiter::new(1);
        let held = limiter.acquire(Arc::new("held".to_owned()), false).await;
        let cancellation = CancellationSignal::new_for_test();
        let mut primary = Box::pin(acquire_active_wasm_cpu_permit(
            &limiter,
            &cancellation,
            false,
            tokio::time::Instant::now() + Duration::from_secs(5),
            common::types::ActiveJavascriptClass::Protected,
        ));
        assert!(matches!(poll!(primary.as_mut()), Poll::Pending));
        let mut teardown = Box::pin(acquire_generated_runtime_teardown_cpu_permit(&limiter));
        assert!(matches!(poll!(teardown.as_mut()), Poll::Pending));

        drop(held);
        let Poll::Ready(primary_permit) = poll!(primary.as_mut()) else {
            panic!("primary CPU admission was not notified before idle teardown");
        };
        assert!(matches!(poll!(teardown.as_mut()), Poll::Pending));

        drop(primary_permit);
        let Poll::Ready(teardown_permit) = poll!(teardown.as_mut()) else {
            panic!("idle teardown did not receive CPU capacity after the primary invocation");
        };
        drop(teardown_permit);
    }
}

#[cfg(unix)]
fn has_exact_control_permissions(metadata: &fs::Metadata, expected: u32) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o7777 == expected
}

#[cfg(not(unix))]
fn has_exact_control_permissions(_metadata: &fs::Metadata, _expected: u32) -> bool {
    true
}
