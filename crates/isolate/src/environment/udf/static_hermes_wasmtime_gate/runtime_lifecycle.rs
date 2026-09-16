use std::io::{
    self,
    Write,
};

use super::*;

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

fn serialized_json_size(value: &JsonValue) -> anyhow::Result<usize> {
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

pub(super) struct GeneratedReusableInstance<RT: Runtime> {
    pub(super) routed: Arc<GeneratedRoutedModule>,
    pub(super) route_identity: GeneratedRouteIdentity,
    pub(super) pool_identity: Arc<GeneratedPoolIdentity>,
    pub(super) memory_slot_id: GeneratedSlotId,
    #[cfg(any(test, feature = "testing"))]
    pub(super) memory_controller: Arc<GeneratedMemoryController>,
    pub(super) store: Store<HostState<RT>>,
    pub(super) select_entry: Option<TypedFunc<i64, i32>>,
    pub(super) prepare_selected_entry: Option<TypedFunc<(), i32>>,
    pub(super) run: TypedFunc<(), i32>,
    pub(super) destroy: TypedFunc<(), ()>,
    #[cfg(test)]
    pub(super) arm_formatter_initialization_failure: Option<TypedFunc<(), i32>>,
    #[cfg(test)]
    pub(super) partial_initialization_trace: Option<TypedFunc<(), i64>>,
    pub(super) context_read_set: Option<ContextReadSet>,
    #[cfg(any(test, feature = "testing"))]
    pub(super) id: u64,
    pub(super) runtime_live: bool,
    pub(super) pool_checked_out: bool,
    pub(super) idle_since: Instant,
}

impl<RT: Runtime> Drop for GeneratedReusableInstance<RT> {
    fn drop(&mut self) {
        if self.runtime_live {
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
    pub(super) cancelled: bool,
    pub(super) reusable_instance: Option<GeneratedReusableInstance<RT>>,
    pub(super) memory_permit: Option<InvocationMemoryPermit>,
    pub(super) terminal_memory_outcome: TerminalMemoryOutcome,
    pub(super) system_error: Option<anyhow::Error>,
    #[cfg(test)]
    pub(super) runtime_id: u64,
    #[cfg(test)]
    pub(super) partial_initialization_trace: Option<i64>,
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
            InvocationOutcome::InitializationTimeout if shadow => {
                return Err(StaticHermesWasmExecutionFailure::InitializationTimeout.into());
            },
            InvocationOutcome::ActiveTimeout if shadow => {
                return Err(StaticHermesWasmExecutionFailure::ExecutionTimeout.into());
            },
            InvocationOutcome::FuelExhausted if shadow => {
                return Err(StaticHermesWasmExecutionFailure::InstructionBudget.into());
            },
            InvocationOutcome::Success
            | InvocationOutcome::DeveloperError(_)
            | InvocationOutcome::InitializationTimeout
            | InvocationOutcome::ActiveTimeout
            | InvocationOutcome::FuelExhausted => {},
        }
        // Authoritative invocations keep the canonical function error, including
        // resource limits; changing a verifier outcome must not change that API.
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
    *primary = Some(match primary.take() {
        Some(primary) => primary.context(format!("{secondary_context}: {secondary:#}")),
        None => secondary.context(secondary_context),
    });
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
    store.epoch_deadline_callback(|store| {
        let generated = store
            .data()
            .generated
            .as_ref()
            .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
        Ok(
            if generated
                .interrupt
                .should_interrupt(&generated.cancellation)
            {
                UpdateDeadline::Interrupt
            } else {
                // A healthy tick still yields so the request-local timeout and
                // cancellation futures can update their interruption state.
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
    if !instance.runtime_live {
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
    interrupt.begin_destruction();
    instance.store.set_epoch_deadline(1);
    instance
        .store
        .set_fuel(routed.manifest.limits().execution_fuel())
        .map_err(wasmtime_anyhow)?;
    let result = {
        let destroy_call = instance.destroy.call_async(&mut instance.store, ());
        tokio::pin!(destroy_call);
        tokio::select! {
            result = &mut destroy_call => result,
            _ = tokio::time::sleep(GENERATED_RUNTIME_DESTROY_TIMEOUT) => {
                interrupt
                    .destruction_timed_out
                    .store(true, Ordering::Release);
                match tokio::time::timeout(
                    GENERATED_RUNTIME_DESTROY_TIMEOUT,
                    &mut destroy_call,
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(WasmtimeError::new(HostInvariant)),
                }
            },
        }
    };
    // A failed destroy still discards the Store. It must never enter the pool.
    instance.runtime_live = false;
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
    let result = destroy_generated_runtime(&routed, &mut instance).await;
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
    if !instance.runtime_live {
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
    if !instance.runtime_live {
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
        Err(cleanup_error) => error.context(format!(
            "generated Wasm runtime cleanup also failed: {cleanup_error:#}"
        )),
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
        Err(cleanup_error) => error.context(format!(
            "generated Wasm runtime cleanup also failed: {cleanup_error:#}"
        )),
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
        state,
        reusable_instance,
        reuse_instances,
        lifecycle_barrier,
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
        state,
        reusable_instance,
        reuse_instances,
        lifecycle_barrier,
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
        _ => Err(WasmtimeError::new(HostInvariant)),
    }
}

pub(super) async fn execute_generated_with_lifecycle_barrier_and_entry_selector<RT: Runtime>(
    routed: Arc<GeneratedRoutedModule>,
    entry_selector: Option<u64>,
    mut state: HostState<RT>,
    reusable_instance: Option<GeneratedReusableInstance<RT>>,
    reuse_instances: bool,
    lifecycle_barrier: Option<Arc<GeneratedLifecycleBarrier>>,
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

    if entry_selector.is_some() != routed.package_identity.requires_entry_selector() {
        return Err(discard_optional_generated_instance(
            reusable_instance,
            anyhow::anyhow!("generated Wasm route selector presence differs from its package ABI"),
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
        state.provider.prepare_execution(timeout).await
    );
    #[cfg(any(test, feature = "testing"))]
    let wall_start = Instant::now();
    #[cfg(any(test, feature = "testing"))]
    let metrics = Arc::clone(&state.metrics);
    let cancellation = try_generated!(
        reusable_instance,
        generated_state(&state).map_err(wasmtime_anyhow)
    )
    .cancellation
    .clone();
    #[cfg(any(test, feature = "testing"))]
    let interrupt = Arc::clone(
        &try_generated!(
            reusable_instance,
            generated_state(&state).map_err(wasmtime_anyhow)
        )
        .interrupt,
    );
    let memory_permit = try_generated!(
        reusable_instance,
        generated_state(&state).map_err(wasmtime_anyhow)
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
            let mut state = state;
            state.wasi_monotonic_epoch = reusable_instance.store.data().wasi_monotonic_epoch;
            let previous_state = std::mem::replace(reusable_instance.store.data_mut(), state);
            drop(previous_state);
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
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?
                .mark_execution_started(true);
            (reusable_instance, None)
        },
        None => {
            let _timer = GatePhaseTimer::new(GATE_PHASE_FRESH_STORE_GRAPH_INSTANTIATION);
            let mut linker = Linker::new(&routed.engine);
            add_generated_convex_imports(&mut linker).map_err(wasmtime_anyhow)?;
            add_wasi_imports(&mut linker).map_err(wasmtime_anyhow)?;
            let mut store = Store::new(&routed.engine, state);
            store
                .set_fuel(GENERATED_INITIALIZATION_FUEL)
                .map_err(wasmtime_anyhow)?;
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
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?
                .mark_execution_started(false);
            let (instance, selector_export, emscripten_graph_instances) = match &routed
                .emscripten_graph
            {
                Some(graph) => {
                    let instances =
                        instantiate_generated_emscripten_graph(&linker, &mut store, graph).await?;
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
                            )
                            .await?,
                        ),
                        None => None,
                    };
                    let instance = linker
                        .instantiate_async(&mut store, &routed.module)
                        .await
                        .map_err(wasmtime_anyhow)?;
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
            };
            let initialize = instance
                .get_typed_func::<(), ()>(&mut store, "_initialize")
                .map_err(wasmtime_anyhow)?;
            let initialization = match emscripten_graph_instances {
                Some((instances, initialization)) => {
                    GeneratedFreshInitialization::EmscriptenGraph {
                        instances,
                        base_initialize: initialize,
                        initialization,
                    }
                },
                None => GeneratedFreshInitialization::Monolithic(initialize),
            };
            let run = instance
                .get_typed_func::<(), i32>(&mut store, "convex_wasm_udf_run")
                .map_err(wasmtime_anyhow)?;
            let select_entry = routed
                .package_identity
                .requires_entry_selector()
                .then(|| instance.get_typed_func::<i64, i32>(&mut store, selector_export))
                .transpose()
                .map_err(wasmtime_anyhow)?;
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
                .map_err(wasmtime_anyhow)?;
            let destroy = instance
                .get_typed_func::<(), ()>(&mut store, "convex_wasm_udf_destroy_runtime")
                .map_err(wasmtime_anyhow)?;
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
            (
                GeneratedReusableInstance {
                    routed: Arc::clone(&routed),
                    route_identity: routed.route_identity.clone(),
                    pool_identity: Arc::new(routed.pool_identity.clone()),
                    memory_slot_id,
                    #[cfg(any(test, feature = "testing"))]
                    memory_controller,
                    store,
                    select_entry,
                    prepare_selected_entry,
                    run,
                    destroy,
                    #[cfg(test)]
                    arm_formatter_initialization_failure,
                    #[cfg(test)]
                    partial_initialization_trace,
                    context_read_set: None,
                    #[cfg(any(test, feature = "testing"))]
                    id: NEXT_GENERATED_REUSABLE_INSTANCE_ID.fetch_add(1, Ordering::SeqCst),
                    runtime_live: false,
                    pool_checked_out: true,
                    idle_since: Instant::now(),
                },
                Some(initialization),
            )
        },
    };
    let reused_instance = initialize.is_none();
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
    let call_result = match barrier_outcome {
        Ok(GeneratedLifecycleBarrierOutcome::Cancelled) => Ok(0),
        Ok(GeneratedLifecycleBarrierOutcome::Continue) => {
            let initialization_started = initialize.is_some();
            let call_result = {
                let call = async {
                    let preparation_timer = GatePhaseTimer::new(
                        GATE_PHASE_GUEST_INITIALIZATION_SELECTED_ENTRY_PREPARATION,
                    );
                    // A pooled graph can prepare a previously unused entry. Its
                    // module initializers need the same configuration-only
                    // authority and dependency capture as a fresh Store.
                    let mut initialization_authority_active = initialization_started
                        || reusable_instance.prepare_selected_entry.is_some();
                    if initialization_authority_active {
                        generated_state_mut(reusable_instance.store.data_mut())?
                            .capability_bridge
                            .begin_initialization()
                            .map_err(|_| WasmtimeError::new(HostInvariant))?;
                        if !initialization_started
                            && generated_state(reusable_instance.store.data())?
                                .context_read_set_required
                        {
                            reusable_instance
                                .store
                                .data_mut()
                                .provider
                                .snoop_initialization_reads()
                                .map_err(|_| WasmtimeError::new(HostInvariant))?;
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
                        // and constructors. Once initialization starts, even partial
                        // initialization retains the bounded guest teardown obligation.
                        reusable_instance.runtime_live = true;
                        run_generated_fresh_initialization(
                            &mut reusable_instance.store,
                            initialization,
                        )
                        .await?;
                        #[cfg(any(test, feature = "testing"))]
                        {
                            phase_trace.initialization_completed = Some(phase_trace.elapsed());
                        }
                        if !routed.package_identity.requires_selected_entry_prepare() {
                            generated_state_mut(reusable_instance.store.data_mut())?
                                .capability_bridge
                                .finish_initialization()
                                .map_err(|_| WasmtimeError::new(HostInvariant))?;
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
                            .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
                        if arm.call_async(&mut reusable_instance.store, ()).await? != 0 {
                            return Err(WasmtimeError::new(HostInvariant));
                        }
                    }
                    if entry_selector.is_some() {
                        failure_subphase =
                            StaticHermesWasmExecutionFailure::GeneratedExportDispatch;
                        #[cfg(any(test, feature = "testing"))]
                        {
                            phase_trace.entry_selection_started = Some(phase_trace.elapsed());
                        }
                        let status = select_generated_entry(
                            &mut reusable_instance.store,
                            entry_selector,
                            reusable_instance.select_entry.as_ref(),
                        )
                        .await
                        .map_err(|error| {
                            error.context(StaticHermesGeneratedExportDiagnostic::FirstSelectorTrap)
                        })?;
                        #[cfg(any(test, feature = "testing"))]
                        {
                            phase_trace.entry_selection_status = Some(status);
                        }
                        if status != 0 {
                            return Err(WasmtimeError::new(
                                StaticHermesGeneratedExportDiagnostic::FirstSelectorRejected {
                                    status,
                                },
                            ));
                        }
                        #[cfg(any(test, feature = "testing"))]
                        {
                            phase_trace.entry_selection_completed = Some(phase_trace.elapsed());
                        }
                    }
                    let selected_entry_prepared = if let Some(prepare_selected_entry) =
                        reusable_instance.prepare_selected_entry.as_ref()
                    {
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
                            phase_trace.prepare_end_fuel = reusable_instance.store.get_fuel().ok();
                            phase_trace.prepare_completed = Some(phase_trace.elapsed());
                        }
                        true
                    } else {
                        false
                    };
                    if selected_entry_prepared
                        != routed.package_identity.requires_selected_entry_prepare()
                    {
                        return Err(WasmtimeError::new(HostInvariant));
                    }
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
                            .map_err(|_| WasmtimeError::new(HostInvariant))?;
                        let capture = ContextCache::capture_context_read_set(
                            reads,
                            state
                                .provider
                                .tx()
                                .map_err(|_| WasmtimeError::new(HostInvariant))?,
                        );
                        let captured = state
                            .timeout
                            .as_mut()
                            .ok_or_else(|| WasmtimeError::new(HostInvariant))?
                            .with_release_permit(PauseReason::UdfInitialize, capture)
                            .await
                            .map_err(|_| WasmtimeError::new(HostInvariant))?;
                        reusable_instance.context_read_set =
                            match (reusable_instance.context_read_set.take(), captured) {
                                (Some(mut previous), Some(additional)) => {
                                    // Keep earlier modules' dependencies without
                                    // rehashing them after every warm preparation.
                                    let intervals = additional.read_set.num_intervals();
                                    let user_size = additional.read_set.user_tx_size().clone();
                                    let system_size = additional.read_set.system_tx_size().clone();
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
                            .map_err(|_| WasmtimeError::new(HostInvariant))?;
                    }

                    {
                        let generated = generated_state(reusable_instance.store.data())?;
                        if !generated.capability_bridge.is_unissued()
                            || !generated.async_operations.is_empty()
                        {
                            return Err(WasmtimeError::new(HostInvariant));
                        }
                    }
                    reusable_instance.store.set_fuel(execution_fuel)?;
                    reusable_instance.store.set_epoch_deadline(1);
                    let profile_fuel = reusable_instance.store.get_fuel().ok();
                    {
                        let generated = generated_state_mut(reusable_instance.store.data_mut())?;
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
                            error.context(StaticHermesGeneratedExportDiagnostic::SecondSelectorTrap)
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
                        failure_subphase = StaticHermesWasmExecutionFailure::RuntimeInitialization;
                    }
                    {
                        let generated = generated_state_mut(reusable_instance.store.data_mut())?;
                        generated
                            .interrupt
                            .set_execution_phase(GeneratedExecutionPhase::Active);
                    }
                    rearm_generated_active_timeout(
                        reusable_instance.store.data_mut(),
                        Duration::from_millis(routed.manifest.limits().timeout_milliseconds()),
                    )
                    .map_err(|error| WasmtimeError::msg(format!("{error:#}")))?;
                    let performance_monotonic_start =
                        reusable_instance.store.data().provider.rt().monotonic_now();
                    {
                        let generated = generated_state_mut(reusable_instance.store.data_mut())?;
                        generated.performance_monotonic_start = performance_monotonic_start;
                        generated
                            .capability_bridge
                            .issue()
                            .map_err(|_| WasmtimeError::new(HostInvariant))?;
                        generated.performance_runtime_available = true;
                    }
                    let capture_handler_reads = reusable_instance
                        .store
                        .data()
                        .provider
                        .handler_read_capture_enabled()
                        .map_err(|_| WasmtimeError::new(HostInvariant))?;
                    if capture_handler_reads {
                        reusable_instance
                            .store
                            .data_mut()
                            .provider
                            .start_handler_read_capture()
                            .map_err(|_| WasmtimeError::new(HostInvariant))?;
                    }
                    drop(preparation_timer);
                    #[cfg(any(test, feature = "testing"))]
                    {
                        phase_trace.handler_started = Some(phase_trace.elapsed());
                        phase_trace.handler_start_fuel = reusable_instance.store.get_fuel().ok();
                    }
                    failure_subphase = StaticHermesWasmExecutionFailure::GuestExecution;
                    let handler_timer = GatePhaseTimer::new(GATE_PHASE_HANDLER_EXPORT_EXECUTION);
                    let result = reusable_instance
                        .run
                        .call_async(&mut reusable_instance.store, ())
                        .await;
                    drop(handler_timer);
                    if capture_handler_reads {
                        reusable_instance
                            .store
                            .data_mut()
                            .provider
                            .finish_handler_read_capture()
                            .map_err(|_| WasmtimeError::new(HostInvariant))?;
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
            call_result.map_err(|error| {
                let failure = if error.is::<GeneratedGuestMemoryLimit>() {
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
                error.context(failure)
            })
        },
        Err(error) => Err(WasmtimeError::msg(format!(
            "generated Wasm lifecycle barrier failed: {error:#}"
        ))
        .context(StaticHermesWasmExecutionFailure::RuntimeInitialization)),
    };
    #[cfg(any(test, feature = "testing"))]
    {
        phase_trace.final_fuel = reusable_instance.store.get_fuel().ok();
    }
    #[cfg(test)]
    let (call_result, partial_initialization_trace) =
        match reusable_instance.partial_initialization_trace.clone() {
            Some(trace) => match trace.call_async(&mut reusable_instance.store, ()).await {
                Ok(trace) => (call_result, Some(trace)),
                Err(error) => (Err(error), None),
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
                    .map_err(|_| WasmtimeError::new(HostInvariant))
            );
            tokio::select! {
                biased;
                release = release_rx => {
                    try_generated!(
                        Some(reusable_instance),
                        release.map_err(|_| WasmtimeError::new(HostInvariant))
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
            let capability_identity = bridge
                .handle()
                .map_err(|_| WasmtimeError::new(HostInvariant));
            let capability_identity =
                try_generated!(Some(reusable_instance), capability_identity) as u64;
            let forged_identity = if capability_identity == u64::MAX {
                1
            } else {
                capability_identity + 1
            };
            let forged_identity = InvocationCapabilityIdentity::from_abi(forged_identity as i64)
                .map_err(|_| WasmtimeError::new(HostInvariant));
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
                    .map_err(|_| WasmtimeError::new(HostInvariant));
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
            anyhow::Error::new(HostInvariant)
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
            .map_err(|_| WasmtimeError::new(HostInvariant));
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
    let timeout_reason = try_generated!(
        Some(reusable_instance),
        generated_state(reusable_instance.store.data())
            .map_err(wasmtime_anyhow)
            .context(StaticHermesWasmExecutionFailure::ResultFinalization)
    )
    .interrupt
    .timeout_reason();
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
                    Some(error.context(failure_subphase)),
                )
            },
            Err(error) if matches!(error.downcast_ref::<Trap>(), Some(Trap::OutOfFuel)) => {
                (InvocationOutcome::FuelExhausted, None)
            },
            Err(error) if matches!(error.downcast_ref::<Trap>(), Some(Trap::Interrupt)) => {
                match try_generated!(
                    Some(reusable_instance),
                    generated_state(reusable_instance.store.data())
                        .map_err(wasmtime_anyhow)
                        .context(StaticHermesWasmExecutionFailure::ResultFinalization)
                )
                .interrupt
                .execution_phase()
                {
                    GeneratedExecutionPhase::Preparing => {
                        (InvocationOutcome::InitializationTimeout, None)
                    },
                    GeneratedExecutionPhase::Active => (InvocationOutcome::ActiveTimeout, None),
                }
            },
            Err(error) => {
                let error = wasmtime_anyhow(error);
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
        let result = (|| -> anyhow::Result<JsonValue> {
            let generated = state
                .generated
                .as_mut()
                .context("generated invocation state disappeared")?;
            anyhow::ensure!(
                generated.async_operations.has_no_active_operations(),
                "generated guest left an async operation pending"
            );
            let result = generated
                .values
                .take_final_result()
                .context("generated guest did not transfer a valid result")?;
            generated
                .values
                .release(
                    generated.request_handle,
                    match generated.manifest.value_mode() {
                        ValueMode::Opaque => OpaqueValueKind::ConvexJson,
                        ValueMode::GuestNativeJson => OpaqueValueKind::Bytes,
                    },
                )
                .context("generated request handle was not retained")?;
            let result_byte_count = serialized_json_size(&result)?;
            let maximum_result_bytes =
                usize::try_from(generated.manifest.limits().max_result_bytes())?;
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
            Ok(result)
        })();
        match result {
            Ok(result) => Some(result),
            Err(error) => {
                outcome = InvocationOutcome::SystemError;
                let opaque_value_failure = error
                    .downcast_ref::<OpaqueValueError>()
                    .map(StaticHermesWasmExecutionFailure::from);
                let error = match opaque_value_failure {
                    Some(failure) => error.context(failure),
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

    let finalized = (|| -> anyhow::Result<InvocationOutput<RT>> {
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
        #[cfg(any(test, feature = "testing"))]
        let read_accounting = read_accounting(&state.provider.tx()?.execution_size());
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
        let (execution_time, teardown_cpu_permit) =
            timeout.into_function_execution_time_with_permit(state.provider.udf_type())?;
        state.teardown_cpu_permit = Some(teardown_cpu_permit);
        #[cfg(any(test, feature = "testing"))]
        let active_time = execution_time.elapsed;
        #[cfg(any(test, feature = "testing"))]
        let wall_time = wall_start.elapsed();
        #[cfg(any(test, feature = "testing"))]
        {
            metrics.active_times.lock().push(active_time);
            metrics.wall_times.lock().push(wall_time);
        }
        if let Some(host_operation_error) = host_operation_error {
            state
                .provider
                .set_terminal_host_operation_error(host_operation_error);
        }
        let canonical_output = match &outcome {
            // Cancellation can skip guest execution entirely. Preserve its transaction
            // for the caller without requiring a result or fabricating a successful UDF.
            InvocationOutcome::Success if cancelled => None,
            InvocationOutcome::Success => {
                #[cfg(any(test, feature = "testing"))]
                let result = function_result
                    .clone()
                    .context("generated Wasm guest did not provide a function result")?;
                #[cfg(not(any(test, feature = "testing")))]
                let result = function_result
                    .take()
                    .context("generated Wasm guest did not provide a function result")?;
                let result = PendingValue::from_uncommitted_json(result)
                    .context("generated Wasm guest returned an invalid Convex value")?;
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
        };
        let (transaction, function_outcome) = match canonical_output {
            Some((transaction, outcome)) => (transaction, Some(outcome)),
            None => {
                let transaction = state.provider.take_transaction_for_system()?;
                return Ok(InvocationOutput {
                    outcome: outcome.clone(),
                    function_outcome: None,
                    transaction,
                    #[cfg(any(test, feature = "testing"))]
                    host_operation_error,
                    #[cfg(any(test, feature = "testing"))]
                    function_result,
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
                });
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
        Ok(InvocationOutput {
            outcome: outcome.clone(),
            function_outcome,
            transaction,
            #[cfg(any(test, feature = "testing"))]
            host_operation_error,
            #[cfg(any(test, feature = "testing"))]
            function_result,
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
        })
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
        let state = reusable_instance.store.data();
        let outcome_can_reuse = generated_outcome_allows_runtime_reuse(
            &output.outcome,
            state.guest_developer_error,
            state.developer_error.is_some(),
        );
        reuse_instances
            && !cancelled
            && outcome_can_reuse
            && !runtime_reuse_contaminated
            && !discard_after_caught_initialization_failure
            && context_read_set_ready
    });
    #[cfg(any(test, feature = "testing"))]
    if let (Some(hooks), Ok(output)) = (
        TEST_HOOKS.lock().as_ref().and_then(Weak::upgrade),
        finalized.as_ref(),
    ) {
        let state = reusable_instance.store.data();
        let outcome_allows_reuse = generated_outcome_allows_runtime_reuse(
            &output.outcome,
            state.guest_developer_error,
            state.developer_error.is_some(),
        );
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
        hooks.metrics.generated_invocations.lock().push(
            StaticHermesGeneratedInvocationObservation {
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
                operation_count: output.operation_count,
                transaction_write_count: output.transaction.writes().coalesced_writes().count(),
                capability_revoked: output.capability_revoked,
                forged_capability_rejected,
                prior_capability_rejected,
                revoked_capability_rejected,
                opaque_live_handles: output.opaque_live_handles,
                opaque_current_bytes: output.opaque_current_bytes,
                runtime_reuse_contaminated: output.runtime_reuse_contaminated,
                retain_instance,
                reuse_instances,
                outcome_allows_reuse,
                guest_developer_error: state.guest_developer_error,
                developer_error_reported: state.developer_error.is_some(),
                discard_after_caught_initialization_failure,
                context_read_set_ready,
                cancelled,
            },
        );
    }
    let terminal_memory_outcome = if finalized_failed {
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
    #[cfg(any(test, feature = "testing"))]
    if terminal_memory_outcome == TerminalMemoryOutcome::Cancellation {
        metrics
            .terminal_cancellations
            .fetch_add(1, Ordering::SeqCst);
    }
    let invocation = match finalized {
        Ok(invocation) => Some(invocation),
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
            memory_permit.finish(terminal_memory_outcome, false);
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
    if cleanup_failed || invocation.is_none() {
        return Err(system_error
            .context("generated Wasm execution failed without a retained internal error")?);
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

    Ok(GeneratedExecutionOutput {
        invocation,
        cancelled,
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
    fn cleanup_classification_is_primary_only_without_an_execution_failure() {
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
            anyhow::anyhow!("guest").context(StaticHermesWasmExecutionFailure::GuestExecution),
        );
        let cleanup =
            anyhow::anyhow!("cleanup").context(StaticHermesWasmExecutionFailure::RuntimeCleanup);
        merge_generated_execution_error(&mut error, cleanup, "cleanup also failed");
        let error = error.expect("merged execution error disappeared");
        assert_eq!(
            error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
            Some(&StaticHermesWasmExecutionFailure::GuestExecution)
        );
        assert!(format!("{error:#}").contains("cleanup also failed"));
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
