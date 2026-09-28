use std::sync::Arc;

use serde::Serialize;

/// Invocation-local controller accounting, not process RSS. Peaks count
/// admitted allocations; a later allocator failure can roll back their current
/// charge. Growth requests reported to the controller as denied are recorded
/// separately and never inflate admitted peaks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WasmMemoryObservation {
    /// Whether execution used a checked-out pooled runtime. This can be false
    /// with a nonzero checkout baseline when a stale runtime was discarded and
    /// replaced with a fresh Store before execution.
    pub reused_instance: bool,
    /// Controller-accounted bytes attached to the admitted memory slot at
    /// checkout, not a measurement of currently resident process memory.
    pub checkout_baseline_bytes: u64,
    /// Peak controller-accounted bytes admitted for this invocation. This can
    /// include an allocation that the allocator later fails.
    pub peak_admitted_bytes: u64,
    /// Peak admitted guest linear-memory accounting. This is not process RSS
    /// and can include an allocation that the allocator later fails.
    pub peak_guest_bytes: u64,
    /// Peak admitted host-owned invocation accounting, including values that
    /// can remain attached to a reusable runtime.
    pub peak_host_bytes: u64,
    /// Peak requested accounting used for learning, including growth reported
    /// to the controller as denied.
    pub requested_peak_bytes: u64,
    /// Admission forecast above the checkout baseline. This establishes the
    /// invocation's initial controller liability; it is not measured memory.
    pub forecast_growth_bytes: u64,
    /// Last combined controller accounting associated with the execution.
    pub completion_bytes: u64,
    /// Bytes returned to the idle pool, not a promise of future retention.
    pub returned_to_pool_bytes: u64,
    pub returned_to_pool: bool,
    pub forecast_overrun: bool,
    pub growth_denied: bool,
    /// A successful execution was discarded despite instance reuse being
    /// enabled. Routine fresh-instance policy does not set this anomaly.
    pub successful_execution_discarded: bool,
    pub terminal: WasmMemoryTerminal,
}

/// Quantiles are conservative upper bounds from the generated-memory
/// controller's logarithmic histogram. `maximum` is the exact largest value
/// observed while the controller record is retained.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WasmMemoryBytesStatistics {
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub maximum: u64,
}

/// Exact maxima for one execution population. Keeping these values separate
/// from the histogram quantiles preserves rare allocations without retaining
/// individual invocation samples.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WasmMemoryMaximums {
    pub checkout_baseline_bytes: u64,
    pub peak_admitted_bytes: u64,
    pub peak_guest_bytes: u64,
    pub peak_host_bytes: u64,
    pub requested_peak_bytes: u64,
    pub forecast_growth_bytes: u64,
    pub completion_bytes: u64,
    pub returned_to_pool_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WasmMemoryTerminalCounts {
    pub success: u64,
    pub developer_error: u64,
    pub system_error: u64,
    pub cancellation: u64,
    pub timeout: u64,
    pub resource_limit: u64,
    pub memory_limit: u64,
}

/// Cumulative controller statistics for a nonempty execution population.
///
/// These values describe controller accounting rather than process RSS. The
/// generated-memory controller updates them for every started Wasm execution;
/// semantic comparison sampling does not control this population.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WasmMemoryExecutionStatistics {
    pub observation_count: u64,
    pub reused_count: u64,
    pub returned_to_pool_count: u64,
    pub forecast_overrun_count: u64,
    pub growth_denied_count: u64,
    pub successful_execution_discarded_count: u64,
    pub terminal_counts: WasmMemoryTerminalCounts,
    pub checkout_baseline_bytes: WasmMemoryBytesStatistics,
    pub peak_admitted_bytes: WasmMemoryBytesStatistics,
    pub peak_guest_bytes: WasmMemoryBytesStatistics,
    pub peak_host_bytes: WasmMemoryBytesStatistics,
    pub requested_peak_bytes: WasmMemoryBytesStatistics,
    pub forecast_growth_bytes: WasmMemoryBytesStatistics,
    pub completion_bytes: WasmMemoryBytesStatistics,
    pub returned_to_pool_bytes: WasmMemoryBytesStatistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fresh_maximums: Option<WasmMemoryMaximums>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reused_maximums: Option<WasmMemoryMaximums>,
}

impl WasmMemoryObservation {
    pub fn has_anomaly(&self) -> bool {
        self.forecast_overrun || self.growth_denied || self.successful_execution_discarded
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum WasmMemoryTerminal {
    Success,
    DeveloperError,
    SystemError,
    Cancellation,
    Timeout,
    ResourceLimit,
    MemoryLimit,
}

/// Independent of comparison completion so detached cleanup and failed
/// executions can still publish memory evidence. Must not control execution.
pub type WasmMemoryObserver = Arc<dyn Fn(WasmMemoryObservation) + Send + Sync>;
