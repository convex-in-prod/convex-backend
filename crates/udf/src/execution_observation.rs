use serde::Serialize;

/// Exclusive time inside synchronous polls. Wall time can exceed thread CPU
/// when the operating system deschedules the polling thread.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionOwnerTime {
    pub cpu_nanos: u64,
    pub poll_wall_nanos: u64,
}

/// Off-poll intervals include executor scheduling delay, not just I/O.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionSuspensionTime {
    pub provider_nanos: u64,
    pub permit_nanos: u64,
    pub other_nanos: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionPhaseObservation {
    pub guest: ExecutionOwnerTime,
    /// Runtime work outside marked guest entries and provider polls. V8
    /// preparation also includes module initialization not split by this
    /// observer.
    pub host: ExecutionOwnerTime,
    pub provider: ExecutionOwnerTime,
    pub suspension: ExecutionSuspensionTime,
}

/// Synchronous module-loading spans are included in the enclosing execution
/// phase (dynamic imports can occur in the handler). Do not add these subspans
/// to the enclosing phase when computing total execution time.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModuleInitializationObservation {
    pub compilation: ExecutionOwnerTime,
    pub serialization: ExecutionOwnerTime,
    pub instantiation: ExecutionOwnerTime,
    pub evaluation: ExecutionOwnerTime,
    pub export_inspection: ExecutionOwnerTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ExecutionObservationInvalid {
    CpuClockUnavailable,
    OwnerStackMismatch,
    OwnerStackLimit,
    NestedRuntime,
}

/// Process-local diagnostic evidence. Lane suspension overlaps runtime work
/// and must not be added to the runtime phases. CPU excludes remote database
/// work and separately spawned engine/provider tasks.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionObservation {
    /// Counts can exceed one when an observed invocation includes nested work.
    pub context_lookup: ContextLookupObservation,
    pub module_initialization: ModuleInitializationObservation,
    pub lane: ExecutionPhaseObservation,
    pub preparation: ExecutionPhaseObservation,
    pub handler: ExecutionPhaseObservation,
    pub finalization: ExecutionPhaseObservation,
    pub runtime_queue_nanos: u64,
    /// Reused Wasm Store/runtime or validated reused V8 context. This does not
    /// prove that every selected guest module was already initialized.
    pub reused_runtime: Option<bool>,
    pub poll_count: u64,
    /// Clock/accounting boundaries, including Wasm transitions. Multiplying
    /// this by a separately measured per-boundary cost estimates observer
    /// perturbation; the recorded owner CPU includes that cost.
    pub accounting_flushes: u64,
    pub wasm_transitions: u64,
    pub provider_batches: u64,
    pub provider_operations: u64,
    pub provider_response_bytes: u64,
    pub runtime_tasks: u32,
    pub active_tasks: u32,
    pub completed_tasks: u32,
    pub cancelled_tasks: u32,
    pub invalid: Option<ExecutionObservationInvalid>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextLookupObservation {
    pub absent: u32,
    pub validated: u32,
    pub validation_failed: u32,
    pub validation_error: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PairedExecutionObservation {
    pub primary: ExecutionObservation,
    pub wasm: ExecutionObservation,
    pub comparison_matches: Option<bool>,
    pub primary_wall_nanos: Option<u64>,
    pub wasm_wall_nanos: Option<u64>,
    /// This precedes observer selection. Its wall duration is present in the
    /// primary lane timer, but its CPU is not measured by this diagnostic.
    pub primary_prestarted_transaction_nanos: u64,
}
