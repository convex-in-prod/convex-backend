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
    /// Subset of suspension after the first observed wake, not additional time.
    /// Self-wakes during the preceding poll start at that poll's end. This is
    /// executor delay for this wrapper, not remote provider service time.
    pub wake_to_poll_nanos: u64,
    pub resumed_polls: u64,
    pub resumed_polls_without_wake: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectLayoutObservation {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub fallbacks: u64,
}

/// Synchronous V8 GC callback spans during observed polls, not background
/// worker CPU or a count of complete collections. Included in owner time.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GcCallbackObservation {
    pub callbacks: u64,
    pub time: ExecutionOwnerTime,
}

/// Hermes runtime counters sampled around handler execution and cleanup.
/// This excludes preparation and is not additive to phase CPU/wall time.
/// Allocated bytes are cumulative allocation traffic, not retained memory.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HermesHeapObservation {
    pub collections: u64,
    pub gc_wall_nanos: u64,
    /// Absent when the host cannot provide the WASI thread CPU clock.
    pub gc_cpu_nanos: Option<u64>,
    pub allocated_bytes: u64,
    pub heap_before_bytes: u64,
    pub heap_after_bytes: u64,
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
    /// Native document-layout counters; absent when the engine has no such
    /// path.
    pub object_layouts: Option<ObjectLayoutObservation>,
    pub v8_gc_callbacks: Option<GcCallbackObservation>,
    /// Absent on a trap or other exit before the guest could report counters.
    pub hermes_handler_heap: Option<HermesHeapObservation>,
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
