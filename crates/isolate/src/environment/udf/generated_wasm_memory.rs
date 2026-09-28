use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fs,
    path::Path,
    sync::{
        atomic::{
            AtomicBool,
            AtomicUsize,
            Ordering,
        },
        Arc,
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::Context as _;
use metrics::{
    log_counter_with_labels,
    log_gauge,
    log_gauge_with_labels,
    register_convex_counter,
    register_convex_gauge,
    StaticMetricLabel,
};
use packed_value::memory::{
    BufferMemory,
    PACKED_BUFFER_MEMORY,
};
use parking_lot::Mutex;
use tokio::sync::Notify;
use udf::wasm_memory::{
    WasmMemoryBytesStatistics,
    WasmMemoryExecutionStatistics,
    WasmMemoryMaximums,
    WasmMemoryObservation,
    WasmMemoryObserver,
    WasmMemoryTerminal,
    WasmMemoryTerminalCounts,
};
use wasmtime::{
    ResourceLimiter,
    StoreLimits,
};

mod inspection;
use inspection::ModuleChargeInspection;
pub use inspection::StaticHermesMemoryInspection;

use super::{
    wasm_udf_abi::{
        HostOwnedByteAccounting,
        HostOwnedByteObserver,
    },
    wasm_udf_manifest::{
        EffectExecutionMode,
        UdfKind,
    },
};

const SUB_BUCKETS_PER_POWER_OF_TWO: usize = 4;
// Cover individual growth through the configured 6 GiB default hard budget.
// The final bucket remains a catch-all so a larger operator budget cannot make
// the learned forecast underestimate an out-of-range observation.
const LOGARITHMIC_BUCKETS: usize = 132;
const MINIMUM_USEFUL_SAMPLES: u64 = 32;
const MINIMUM_WARM_GROWTH_BYTES: usize = 1024 * 1024;
const MAX_FUNCTION_RECORDS: usize = 4096;
const MAX_ROUTE_STATISTICS_RECORDS: usize = 4096;
const CGROUP_ROOT: &str = "/sys/fs/cgroup";

register_convex_counter!(
    GENERATED_WASM_MEMORY_EVENTS_TOTAL,
    "Generated Wasm memory controller events",
    &["event"]
);
register_convex_gauge!(
    GENERATED_WASM_MEMORY_INSTANCES_INFO,
    "Generated Wasm instances tracked by the memory controller",
    &["state"]
);
register_convex_gauge!(
    GENERATED_WASM_MEMORY_BYTES,
    "Generated Wasm memory controller byte accounting",
    &["component"]
);
register_convex_gauge!(
    GENERATED_WASM_MEMORY_CGROUP_HEADROOM_BYTES,
    "Latest finite cgroup memory headroom visible to generated Wasm admission"
);
register_convex_gauge!(
    GENERATED_WASM_MEMORY_CGROUP_SAMPLE_AVAILABLE_INFO,
    "Whether generated Wasm admission has a current finite cgroup memory sample"
);
register_convex_gauge!(
    GENERATED_WASM_MEMORY_INSTANCE_POLICY_TOTAL,
    "Configured generated Wasm instance policy values",
    &["setting"]
);

fn log_memory_event(event: &'static str) {
    log_counter_with_labels(
        &GENERATED_WASM_MEMORY_EVENTS_TOTAL,
        1,
        vec![StaticMetricLabel::new("event", event)],
    );
}

pub(crate) fn record_module_cache_event(event: &'static str) {
    log_memory_event(event);
}

pub(crate) struct PreparationInstanceCapacity {
    pub(crate) available: usize,
    pub(crate) limit: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct PreparationMemoryCost {
    pub(crate) peak_bytes: usize,
    pub(crate) retained_bytes: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct GeneratedMemoryPolicy {
    pub(crate) hard_instance_ceiling: usize,
    pub(crate) soft_budget_bytes: usize,
    pub(crate) hard_budget_bytes: usize,
    pub(crate) safety_reserve_bytes: usize,
    pub(crate) unattributed_bytes_per_slot: usize,
    pub(crate) cold_peak_growth_bytes: usize,
    pub(crate) warm_idle_target: usize,
    pub(crate) maximum_idle_age: Duration,
    pub(crate) pressure_enter_headroom_bytes: usize,
    pub(crate) pressure_exit_headroom_bytes: usize,
}

impl GeneratedMemoryPolicy {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.hard_instance_ceiling > 0,
            "generated Wasm instance ceiling must be greater than zero"
        );
        anyhow::ensure!(
            self.soft_budget_bytes < self.hard_budget_bytes,
            "generated Wasm soft memory budget must be smaller than its hard budget"
        );
        anyhow::ensure!(
            self.safety_reserve_bytes < self.soft_budget_bytes,
            "generated Wasm safety reserve must be smaller than its soft budget"
        );
        anyhow::ensure!(
            self.cold_peak_growth_bytes
                .checked_add(self.safety_reserve_bytes)
                .is_some_and(|bytes| bytes <= self.hard_budget_bytes),
            "generated Wasm cold forecast and safety reserve exceed its hard budget"
        );
        anyhow::ensure!(
            self.warm_idle_target <= self.hard_instance_ceiling,
            "generated Wasm warm idle target exceeds its instance ceiling"
        );
        anyhow::ensure!(
            self.pressure_enter_headroom_bytes < self.pressure_exit_headroom_bytes,
            "generated Wasm pressure exit headroom must exceed enter headroom"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct FunctionMemoryIdentity {
    pub(crate) deployment: String,
    pub(crate) package_key: String,
    pub(crate) lineage: FunctionMemoryLineage,
    pub(crate) compiler_pipeline_sha256: String,
    pub(crate) compiler_revision: String,
    pub(crate) static_hermes_revision: String,
    pub(crate) admitted_language_version: u32,
    pub(crate) manifest_schema_version: u32,
    pub(crate) effect_execution_mode: EffectExecutionMode,
    pub(crate) opaque_value_abi_version: u32,
    pub(crate) wasmtime_revision: String,
    pub(crate) target_triple: String,
    pub(crate) target_cpu: String,
    pub(crate) engine_configuration_sha256: String,
    pub(crate) admission_policy_version: u32,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum FunctionMemoryLineage {
    LegacyExport {
        module_path: String,
        export_name: String,
        udf_kind: UdfKind,
        resolved_graph_sha256: String,
        export_sha256: String,
    },
    CapabilityPackage {
        package_key: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GeneratedMemoryExecutionRole {
    Primary,
    Shadow,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct GeneratedMemoryRouteIdentity {
    pub(crate) generation_sha256: String,
    pub(crate) route_sha256: String,
    pub(crate) udf_kind: UdfKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GeneratedMemoryRouteContext {
    pub(crate) identity: GeneratedMemoryRouteIdentity,
    pub(crate) role: GeneratedMemoryExecutionRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticHermesGeneratedMemoryRouteStatistics {
    pub route_sha256: String,
    pub aggregate: WasmMemoryExecutionStatistics,
    pub primary: Option<WasmMemoryExecutionStatistics>,
    pub shadow: Option<WasmMemoryExecutionStatistics>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticHermesGeneratedMemoryStatistics {
    pub generation_sha256: String,
    pub selected_route_count: usize,
    pub observed_route_count: usize,
    pub route_record_capacity: usize,
    pub route_record_eviction_count: u64,
    pub aggregate: Option<WasmMemoryExecutionStatistics>,
    pub routes: Vec<StaticHermesGeneratedMemoryRouteStatistics>,
}

impl FunctionMemoryIdentity {
    fn same_lineage(&self, other: &Self) -> bool {
        self.lineage == other.lineage
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalMemoryOutcome {
    Success,
    DeveloperError,
    SystemError,
    Cancellation,
    Timeout,
    ResourceLimit,
    MemoryLimit,
}

impl TerminalMemoryOutcome {
    fn index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::DeveloperError => 1,
            Self::SystemError => 2,
            Self::Cancellation => 3,
            Self::Timeout => 4,
            Self::ResourceLimit => 5,
            Self::MemoryLimit => 6,
        }
    }

    fn metric_name(self) -> &'static str {
        match self {
            Self::Success => "terminal_success",
            Self::DeveloperError => "terminal_developer_error",
            Self::SystemError => "terminal_system_error",
            Self::Cancellation => "terminal_cancellation",
            Self::Timeout => "terminal_timeout",
            Self::ResourceLimit => "terminal_resource_limit",
            Self::MemoryLimit => "terminal_memory_limit",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct GeneratedSlotId(u64);

impl GeneratedSlotId {
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn test_identity(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeDestructionOutcome {
    Completed,
    Failed,
}

pub(crate) fn record_runtime_destruction(outcome: RuntimeDestructionOutcome) {
    log_memory_event(match outcome {
        RuntimeDestructionOutcome::Completed => "destruction_completed",
        RuntimeDestructionOutcome::Failed => "destruction_failed",
    });
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdleEvictionReason {
    IdleAge,
    MemoryPressure,
    InstanceCeiling,
    AdmissionBudget,
    GenerationRetirement,
    InitializationInvalidation,
    #[cfg(any(test, feature = "testing"))]
    TestCleanup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdleEvictionTrigger {
    Maintenance,
    NewSlot,
    AdmissionBudget,
    GenerationRetirement,
    InitializationInvalidation,
}

impl IdleEvictionReason {
    fn metric_name(self) -> &'static str {
        match self {
            Self::IdleAge => "idle_eviction_age",
            Self::MemoryPressure => "idle_eviction_pressure",
            Self::InstanceCeiling => "idle_eviction_ceiling",
            Self::AdmissionBudget => "idle_eviction_admission_budget",
            Self::GenerationRetirement => "idle_eviction_generation_retirement",
            Self::InitializationInvalidation => "idle_eviction_initialization_invalidation",
            #[cfg(any(test, feature = "testing"))]
            Self::TestCleanup => "idle_eviction_test_cleanup",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionRejection {
    ForecastExceedsHardBudget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ModuleMemoryAdmissionError {
    #[error("generated Wasm module fixed-byte estimate exceeds its hard memory budget")]
    FixedBytesExceedHardBudget,
    #[error("generated Wasm module cache admission is blocked by memory pressure")]
    Pressure,
    #[error("generated Wasm module cache admission exceeds its soft memory budget")]
    SoftBudget,
    #[error("generated Wasm module cache admission exceeds its hard memory budget")]
    HardBudget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendPressure {
    Unavailable,
    Healthy { headroom_bytes: usize },
    Active { headroom_bytes: usize },
}

#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GeneratedMemoryTestSnapshot {
    pub(crate) active_instances: usize,
    pub(crate) idle_instances: usize,
    pub(crate) evicting_instances: usize,
    pub(crate) fixed_module_bytes: usize,
    pub(crate) retained_baseline_bytes: usize,
    pub(crate) active_checkout_baseline_bytes: usize,
    pub(crate) active_guest_bytes: usize,
    pub(crate) active_host_bytes: usize,
    pub(crate) active_current_growth_bytes: usize,
    pub(crate) active_forecast_bytes: usize,
    pub(crate) active_liability_bytes: usize,
    pub(crate) unattributed_allowance_bytes: usize,
    pub(crate) projected_bytes: usize,
    pub(crate) function_samples: u64,
    pub(crate) developer_error_samples: u64,
    pub(crate) system_error_samples: u64,
    pub(crate) timeout_samples: u64,
    pub(crate) resource_limit_samples: u64,
    pub(crate) memory_limit_samples: u64,
}

#[derive(Clone, Copy, Debug)]
struct AggregateMemoryMetrics {
    active_instances: usize,
    idle_instances: usize,
    evicting_instances: usize,
    retained_baseline_bytes: usize,
    active_checkout_baseline_bytes: usize,
    active_guest_bytes: usize,
    active_host_bytes: usize,
    active_current_growth_bytes: usize,
    active_forecast_bytes: usize,
    active_liability_bytes: usize,
    unattributed_allowance_bytes: usize,
    projected_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
struct LogarithmicHistogram {
    buckets: [u64; LOGARITHMIC_BUCKETS],
    samples: u64,
    maximum: usize,
}

impl Default for LogarithmicHistogram {
    fn default() -> Self {
        Self {
            buckets: [0; LOGARITHMIC_BUCKETS],
            samples: 0,
            maximum: 0,
        }
    }
}

impl LogarithmicHistogram {
    fn observe(&mut self, bytes: usize) {
        let bucket = logarithmic_bucket(bytes);
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        self.samples = self.samples.saturating_add(1);
        self.maximum = self.maximum.max(bytes);
    }

    fn merge(&mut self, other: &Self) {
        for (count, other_count) in self.buckets.iter_mut().zip(other.buckets) {
            *count = count.saturating_add(other_count);
        }
        self.samples = self.samples.saturating_add(other.samples);
        self.maximum = self.maximum.max(other.maximum);
    }

    fn quantile(&self, numerator: u64, denominator: u64) -> Option<usize> {
        if self.samples == 0 {
            return None;
        }
        let target = self
            .samples
            .saturating_mul(numerator)
            .saturating_add(denominator - 1)
            / denominator;
        let mut seen = 0u64;
        for (bucket, count) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= target {
                return Some(logarithmic_bucket_upper_bound(bucket));
            }
        }
        Some(logarithmic_bucket_upper_bound(LOGARITHMIC_BUCKETS - 1))
    }

    fn statistics(&self) -> Option<WasmMemoryBytesStatistics> {
        Some(WasmMemoryBytesStatistics {
            p50: u64::try_from(self.quantile(50, 100)?).unwrap_or(u64::MAX),
            p90: u64::try_from(self.quantile(90, 100)?).unwrap_or(u64::MAX),
            p99: u64::try_from(self.quantile(99, 100)?).unwrap_or(u64::MAX),
            maximum: u64::try_from(self.maximum).unwrap_or(u64::MAX),
        })
    }
}

fn logarithmic_bucket(bytes: usize) -> usize {
    if bytes <= 1 {
        return 0;
    }
    let exponent = usize::BITS as usize - 1 - bytes.leading_zeros() as usize;
    let base = 1usize << exponent;
    let offset = bytes - base;
    let sub_bucket = offset
        .saturating_mul(SUB_BUCKETS_PER_POWER_OF_TWO)
        .div_ceil(base)
        .min(SUB_BUCKETS_PER_POWER_OF_TWO - 1);
    (exponent * SUB_BUCKETS_PER_POWER_OF_TWO + sub_bucket).min(LOGARITHMIC_BUCKETS - 1)
}

fn logarithmic_bucket_upper_bound(bucket: usize) -> usize {
    if bucket == LOGARITHMIC_BUCKETS - 1 {
        return usize::MAX;
    }
    let exponent = bucket / SUB_BUCKETS_PER_POWER_OF_TWO;
    let sub_bucket = bucket % SUB_BUCKETS_PER_POWER_OF_TWO;
    let Some(base) = 1usize.checked_shl(u32::try_from(exponent).unwrap_or(u32::MAX)) else {
        return usize::MAX;
    };
    base.saturating_add(
        base.saturating_mul(sub_bucket + 1)
            .div_ceil(SUB_BUCKETS_PER_POWER_OF_TWO),
    )
}

#[derive(Clone, Copy)]
enum MemoryStatisticField {
    CheckoutBaseline,
    PeakAdmitted,
    PeakGuest,
    PeakHost,
    RequestedPeak,
    ForecastGrowth,
    Completion,
    ReturnedToPool,
}

impl MemoryStatisticField {
    const ALL: [Self; 8] = [
        Self::CheckoutBaseline,
        Self::PeakAdmitted,
        Self::PeakGuest,
        Self::PeakHost,
        Self::RequestedPeak,
        Self::ForecastGrowth,
        Self::Completion,
        Self::ReturnedToPool,
    ];

    const fn index(self) -> usize {
        match self {
            Self::CheckoutBaseline => 0,
            Self::PeakAdmitted => 1,
            Self::PeakGuest => 2,
            Self::PeakHost => 3,
            Self::RequestedPeak => 4,
            Self::ForecastGrowth => 5,
            Self::Completion => 6,
            Self::ReturnedToPool => 7,
        }
    }

    fn value(self, observation: &WasmMemoryObservation) -> usize {
        let value = match self {
            Self::CheckoutBaseline => observation.checkout_baseline_bytes,
            Self::PeakAdmitted => observation.peak_admitted_bytes,
            Self::PeakGuest => observation.peak_guest_bytes,
            Self::PeakHost => observation.peak_host_bytes,
            Self::RequestedPeak => observation.requested_peak_bytes,
            Self::ForecastGrowth => observation.forecast_growth_bytes,
            Self::Completion => observation.completion_bytes,
            Self::ReturnedToPool => observation.returned_to_pool_bytes,
        };
        usize::try_from(value).unwrap_or(usize::MAX)
    }
}

#[derive(Clone, Debug)]
struct ExecutionMemoryStatistics {
    distributions: [LogarithmicHistogram; MemoryStatisticField::ALL.len()],
    observation_count: u64,
    reused_count: u64,
    returned_to_pool_count: u64,
    forecast_overrun_count: u64,
    growth_denied_count: u64,
    successful_execution_discarded_count: u64,
    terminal_counts: [u64; 7],
    fresh_maximums: Option<[usize; MemoryStatisticField::ALL.len()]>,
    reused_maximums: Option<[usize; MemoryStatisticField::ALL.len()]>,
}

impl Default for ExecutionMemoryStatistics {
    fn default() -> Self {
        Self {
            distributions: [LogarithmicHistogram::default(); MemoryStatisticField::ALL.len()],
            observation_count: 0,
            reused_count: 0,
            returned_to_pool_count: 0,
            forecast_overrun_count: 0,
            growth_denied_count: 0,
            successful_execution_discarded_count: 0,
            terminal_counts: [0; 7],
            fresh_maximums: None,
            reused_maximums: None,
        }
    }
}

impl ExecutionMemoryStatistics {
    fn observe(&mut self, observation: &WasmMemoryObservation) {
        self.observation_count = self.observation_count.saturating_add(1);
        self.reused_count = self
            .reused_count
            .saturating_add(u64::from(observation.reused_instance));
        self.returned_to_pool_count = self
            .returned_to_pool_count
            .saturating_add(u64::from(observation.returned_to_pool));
        self.forecast_overrun_count = self
            .forecast_overrun_count
            .saturating_add(u64::from(observation.forecast_overrun));
        self.growth_denied_count = self
            .growth_denied_count
            .saturating_add(u64::from(observation.growth_denied));
        self.successful_execution_discarded_count = self
            .successful_execution_discarded_count
            .saturating_add(u64::from(observation.successful_execution_discarded));
        let terminal_index = match observation.terminal {
            WasmMemoryTerminal::Success => 0,
            WasmMemoryTerminal::DeveloperError => 1,
            WasmMemoryTerminal::SystemError => 2,
            WasmMemoryTerminal::Cancellation => 3,
            WasmMemoryTerminal::Timeout => 4,
            WasmMemoryTerminal::ResourceLimit => 5,
            WasmMemoryTerminal::MemoryLimit => 6,
        };
        self.terminal_counts[terminal_index] =
            self.terminal_counts[terminal_index].saturating_add(1);
        let values = MemoryStatisticField::ALL.map(|field| field.value(observation));
        for field in MemoryStatisticField::ALL {
            let value = values[field.index()];
            self.distributions[field.index()].observe(value);
        }
        let maxima = if observation.reused_instance {
            &mut self.reused_maximums
        } else {
            &mut self.fresh_maximums
        }
        .get_or_insert([0; MemoryStatisticField::ALL.len()]);
        for field in MemoryStatisticField::ALL {
            let value = values[field.index()];
            maxima[field.index()] = maxima[field.index()].max(value);
        }
    }

    fn merge(&mut self, other: &Self) {
        self.observation_count = self
            .observation_count
            .saturating_add(other.observation_count);
        self.reused_count = self.reused_count.saturating_add(other.reused_count);
        self.returned_to_pool_count = self
            .returned_to_pool_count
            .saturating_add(other.returned_to_pool_count);
        self.forecast_overrun_count = self
            .forecast_overrun_count
            .saturating_add(other.forecast_overrun_count);
        self.growth_denied_count = self
            .growth_denied_count
            .saturating_add(other.growth_denied_count);
        self.successful_execution_discarded_count = self
            .successful_execution_discarded_count
            .saturating_add(other.successful_execution_discarded_count);
        for (count, other_count) in self.terminal_counts.iter_mut().zip(other.terminal_counts) {
            *count = count.saturating_add(other_count);
        }
        for (distribution, other_distribution) in
            self.distributions.iter_mut().zip(&other.distributions)
        {
            distribution.merge(other_distribution);
        }
        merge_maximums(&mut self.fresh_maximums, other.fresh_maximums);
        merge_maximums(&mut self.reused_maximums, other.reused_maximums);
    }

    fn snapshot(&self) -> Option<WasmMemoryExecutionStatistics> {
        if self.observation_count == 0 {
            return None;
        }
        let distribution = |field: MemoryStatisticField| {
            self.distributions[field.index()]
                .statistics()
                .expect("nonempty memory statistics lost their histogram samples")
        };
        Some(WasmMemoryExecutionStatistics {
            observation_count: self.observation_count,
            reused_count: self.reused_count,
            returned_to_pool_count: self.returned_to_pool_count,
            forecast_overrun_count: self.forecast_overrun_count,
            growth_denied_count: self.growth_denied_count,
            successful_execution_discarded_count: self.successful_execution_discarded_count,
            terminal_counts: WasmMemoryTerminalCounts {
                success: self.terminal_counts[0],
                developer_error: self.terminal_counts[1],
                system_error: self.terminal_counts[2],
                cancellation: self.terminal_counts[3],
                timeout: self.terminal_counts[4],
                resource_limit: self.terminal_counts[5],
                memory_limit: self.terminal_counts[6],
            },
            checkout_baseline_bytes: distribution(MemoryStatisticField::CheckoutBaseline),
            peak_admitted_bytes: distribution(MemoryStatisticField::PeakAdmitted),
            peak_guest_bytes: distribution(MemoryStatisticField::PeakGuest),
            peak_host_bytes: distribution(MemoryStatisticField::PeakHost),
            requested_peak_bytes: distribution(MemoryStatisticField::RequestedPeak),
            forecast_growth_bytes: distribution(MemoryStatisticField::ForecastGrowth),
            completion_bytes: distribution(MemoryStatisticField::Completion),
            returned_to_pool_bytes: distribution(MemoryStatisticField::ReturnedToPool),
            fresh_maximums: self.fresh_maximums.map(memory_maximums),
            reused_maximums: self.reused_maximums.map(memory_maximums),
        })
    }
}

fn merge_maximums(
    target: &mut Option<[usize; MemoryStatisticField::ALL.len()]>,
    source: Option<[usize; MemoryStatisticField::ALL.len()]>,
) {
    let Some(source) = source else {
        return;
    };
    let target = target.get_or_insert([0; MemoryStatisticField::ALL.len()]);
    for (value, source_value) in target.iter_mut().zip(source) {
        *value = (*value).max(source_value);
    }
}

fn memory_maximums(values: [usize; MemoryStatisticField::ALL.len()]) -> WasmMemoryMaximums {
    let value =
        |field: MemoryStatisticField| u64::try_from(values[field.index()]).unwrap_or(u64::MAX);
    WasmMemoryMaximums {
        checkout_baseline_bytes: value(MemoryStatisticField::CheckoutBaseline),
        peak_admitted_bytes: value(MemoryStatisticField::PeakAdmitted),
        peak_guest_bytes: value(MemoryStatisticField::PeakGuest),
        peak_host_bytes: value(MemoryStatisticField::PeakHost),
        requested_peak_bytes: value(MemoryStatisticField::RequestedPeak),
        forecast_growth_bytes: value(MemoryStatisticField::ForecastGrowth),
        completion_bytes: value(MemoryStatisticField::Completion),
        returned_to_pool_bytes: value(MemoryStatisticField::ReturnedToPool),
    }
}

#[derive(Clone, Debug, Default)]
struct RouteMemoryStatistics {
    primary: ExecutionMemoryStatistics,
    shadow: ExecutionMemoryStatistics,
    last_used: u64,
    memory_identity: Option<FunctionMemoryIdentity>,
}

impl RouteMemoryStatistics {
    fn observe(
        &mut self,
        role: GeneratedMemoryExecutionRole,
        observation: &WasmMemoryObservation,
        clock: u64,
    ) {
        match role {
            GeneratedMemoryExecutionRole::Primary => self.primary.observe(observation),
            GeneratedMemoryExecutionRole::Shadow => self.shadow.observe(observation),
        }
        self.last_used = clock;
    }

    fn aggregate(&self) -> ExecutionMemoryStatistics {
        let mut aggregate = self.primary.clone();
        aggregate.merge(&self.shadow);
        aggregate
    }

    fn snapshot(&self, route_sha256: String) -> StaticHermesGeneratedMemoryRouteStatistics {
        StaticHermesGeneratedMemoryRouteStatistics {
            route_sha256,
            aggregate: self
                .aggregate()
                .snapshot()
                .expect("retained route memory statistics are empty"),
            primary: self.primary.snapshot(),
            shadow: self.shadow.snapshot(),
        }
    }
}

#[derive(Clone, Debug)]
struct FunctionMemoryStats {
    peak_growth: LogarithmicHistogram,
    preparation_peak_growth: LogarithmicHistogram,
    warm_peak_growth: LogarithmicHistogram,
    warm_sample_count: u64,
    retained_memory: LogarithmicHistogram,
    sample_count: u64,
    peak_growth_mean: f64,
    peak_growth_m2: f64,
    observed_maximum: usize,
    prediction_overruns: u64,
    retained_change_sum: i128,
    minimum_retained_change: i64,
    maximum_retained_change: i64,
    terminal_samples: [u64; 7],
    inherited_forecast_bytes: usize,
    last_used: u64,
}

impl FunctionMemoryStats {
    fn new(inherited_forecast_bytes: usize, last_used: u64) -> Self {
        Self {
            peak_growth: LogarithmicHistogram::default(),
            preparation_peak_growth: LogarithmicHistogram::default(),
            warm_peak_growth: LogarithmicHistogram::default(),
            warm_sample_count: 0,
            retained_memory: LogarithmicHistogram::default(),
            sample_count: 0,
            peak_growth_mean: 0.0,
            peak_growth_m2: 0.0,
            observed_maximum: 0,
            prediction_overruns: 0,
            retained_change_sum: 0,
            minimum_retained_change: 0,
            maximum_retained_change: 0,
            terminal_samples: [0; 7],
            inherited_forecast_bytes,
            last_used,
        }
    }

    fn forecast(&self, cold_prior: usize) -> usize {
        let learned = self.warm_peak_growth.quantile(99, 100);
        if self.warm_sample_count < MINIMUM_USEFUL_SAMPLES {
            return learned
                .unwrap_or(0)
                .max(cold_prior)
                .max(self.inherited_forecast_bytes);
        }
        let learned = learned.expect("warm samples lost their growth histogram");
        // The histogram already rounds upward. Add headroom for variation not
        // represented by those samples, including a zero-growth population.
        learned
            .saturating_add(learned.div_ceil(4))
            .max(MINIMUM_WARM_GROWTH_BYTES.min(cold_prior))
    }

    fn preparation_forecast(&self, cold_prior: usize) -> usize {
        // Initialization observations must neither inflate warm forecasts nor
        // be diluted by frequent warm calls. A new Store also needs enough
        // headroom for a handler before its next return to the pool.
        self.preparation_peak_growth
            .quantile(99, 100)
            .unwrap_or(cold_prior)
            .max(cold_prior)
            .max(self.forecast(cold_prior))
    }

    fn observe(
        &mut self,
        peak_growth: usize,
        retained_memory: usize,
        retained_change: i64,
        forecast: usize,
        terminal: TerminalMemoryOutcome,
        clock: u64,
    ) {
        self.sample_count = self.sample_count.saturating_add(1);
        let delta = peak_growth as f64 - self.peak_growth_mean;
        self.peak_growth_mean += delta / self.sample_count as f64;
        self.peak_growth_m2 += delta * (peak_growth as f64 - self.peak_growth_mean);
        self.observed_maximum = self.observed_maximum.max(peak_growth);
        if peak_growth > forecast {
            self.prediction_overruns = self.prediction_overruns.saturating_add(1);
        }
        self.peak_growth.observe(peak_growth);
        self.retained_memory.observe(retained_memory);
        self.retained_change_sum = self
            .retained_change_sum
            .saturating_add(i128::from(retained_change));
        if self.sample_count == 1 {
            self.minimum_retained_change = retained_change;
            self.maximum_retained_change = retained_change;
        } else {
            self.minimum_retained_change = self.minimum_retained_change.min(retained_change);
            self.maximum_retained_change = self.maximum_retained_change.max(retained_change);
        }
        self.terminal_samples[terminal.index()] =
            self.terminal_samples[terminal.index()].saturating_add(1);
        self.last_used = clock;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotStatus {
    Idle,
    Active { invocation_id: u64 },
    Evicting,
}

#[derive(Clone, Debug)]
struct SlotMemory {
    identity: FunctionMemoryIdentity,
    module_charge_ids: Vec<u64>,
    guest_bytes: usize,
    shared_static_bytes: usize,
    retained_host_bytes: usize,
    // An initialized idle Store reserves the growth needed for its next
    // checkout. Admission transfers this liability instead of adding it twice.
    idle_growth_bytes: usize,
    status: SlotStatus,
}

impl SlotMemory {
    fn retained_baseline(&self) -> usize {
        self.guest_bytes
            .checked_sub(self.shared_static_bytes)
            .expect("shared static bytes exceed retained guest capacity")
            .saturating_add(self.retained_host_bytes)
    }
}

#[derive(Clone, Debug)]
struct PreStaticMemoryPeaks {
    requested_bytes: usize,
    admitted_bytes: usize,
}

#[derive(Clone, Debug)]
struct ActiveInvocation {
    slot_id: GeneratedSlotId,
    identity: FunctionMemoryIdentity,
    checkout_baseline_bytes: usize,
    current_guest_bytes: usize,
    shared_static_bytes: usize,
    current_host_bytes: usize,
    peak_accounted_bytes: usize,
    peak_admitted_bytes: usize,
    peak_guest_bytes: usize,
    peak_host_bytes: usize,
    peaks_before_static_memory: Option<PreStaticMemoryPeaks>,
    forecast_bytes: usize,
    liability_bytes: usize,
    preparation_headroom_bytes: usize,
    cold_start: bool,
    prediction_overrun_recorded: bool,
    growth_denied: bool,
}

#[derive(Debug)]
struct ControllerState {
    slots: BTreeMap<GeneratedSlotId, SlotMemory>,
    active: BTreeMap<u64, ActiveInvocation>,
    functions: BTreeMap<FunctionMemoryIdentity, FunctionMemoryStats>,
    routes: BTreeMap<GeneratedMemoryRouteIdentity, RouteMemoryStatistics>,
    route_record_eviction_count: u64,
    global_peak_growth: LogarithmicHistogram,
    fixed_module_bytes: usize,
    module_charges: BTreeMap<u64, ModuleChargeInspection>,
    next_module_charge_id: u64,
    next_slot_id: u64,
    next_invocation_id: u64,
    clock: u64,
    pressure: BackendPressure,
    pressure_sample_available: bool,
}

impl Default for ControllerState {
    fn default() -> Self {
        Self {
            slots: BTreeMap::new(),
            active: BTreeMap::new(),
            functions: BTreeMap::new(),
            routes: BTreeMap::new(),
            route_record_eviction_count: 0,
            global_peak_growth: LogarithmicHistogram::default(),
            fixed_module_bytes: 0,
            module_charges: BTreeMap::new(),
            next_module_charge_id: 1,
            next_slot_id: 1,
            next_invocation_id: 1,
            clock: 0,
            pressure: BackendPressure::Unavailable,
            pressure_sample_available: false,
        }
    }
}

pub(crate) struct GeneratedMemoryController {
    inspection_id: uuid::Uuid,
    inspection_started: Instant,
    policy: GeneratedMemoryPolicy,
    state: Mutex<ControllerState>,
    shared_buffers: &'static BufferMemory,
    changed: &'static Notify,
}

impl GeneratedMemoryController {
    pub(crate) fn new(policy: GeneratedMemoryPolicy) -> anyhow::Result<Arc<Self>> {
        Self::with_shared_buffers(policy, &PACKED_BUFFER_MEMORY)
    }

    pub(super) fn with_shared_buffers(
        policy: GeneratedMemoryPolicy,
        shared_buffers: &'static BufferMemory,
    ) -> anyhow::Result<Arc<Self>> {
        policy.validate()?;
        let controller = Arc::new(Self {
            inspection_id: uuid::Uuid::new_v4(),
            inspection_started: Instant::now(),
            policy,
            state: Mutex::new(ControllerState::default()),
            shared_buffers,
            changed: shared_buffers.changed(),
        });
        controller.log_policy_metrics();
        {
            let state = controller.state.lock();
            controller.log_state_metrics_locked(&state);
        }
        Ok(controller)
    }

    pub(crate) fn refresh_pressure_from_cgroup(&self) {
        self.update_pressure_sample(read_cgroup_headroom(Path::new(CGROUP_ROOT)));
        // Packed backing can change on database threads without any Wasm event.
        // Refresh its aggregate gauge on the existing sampler cadence as well.
        self.log_state_metrics_locked(&self.state.lock());
    }

    pub(crate) fn pressure(&self) -> BackendPressure {
        self.state.lock().pressure
    }

    pub(crate) fn statistics_for_routes(
        &self,
        generation_sha256: &str,
        udf_kind: UdfKind,
        selected_route_sha256s: &[String],
    ) -> StaticHermesGeneratedMemoryStatistics {
        let selected: BTreeSet<&str> = selected_route_sha256s.iter().map(String::as_str).collect();
        let state = self.state.lock();
        let routes = state
            .routes
            .iter()
            .filter(|(identity, _)| {
                identity.generation_sha256 == generation_sha256
                    && identity.udf_kind == udf_kind
                    && selected.contains(identity.route_sha256.as_str())
            })
            .map(|(identity, statistics)| statistics.snapshot(identity.route_sha256.clone()))
            .collect::<Vec<_>>();
        let mut aggregate = ExecutionMemoryStatistics::default();
        for route in state.routes.iter().filter_map(|(identity, statistics)| {
            (identity.generation_sha256 == generation_sha256
                && identity.udf_kind == udf_kind
                && selected.contains(identity.route_sha256.as_str()))
            .then_some(statistics)
        }) {
            aggregate.merge(&route.aggregate());
        }
        StaticHermesGeneratedMemoryStatistics {
            generation_sha256: generation_sha256.to_owned(),
            selected_route_count: selected_route_sha256s.len(),
            observed_route_count: routes.len(),
            route_record_capacity: MAX_ROUTE_STATISTICS_RECORDS,
            route_record_eviction_count: state.route_record_eviction_count,
            aggregate: aggregate.snapshot(),
            routes,
        }
    }

    pub(crate) fn try_charge_module(
        self: &Arc<Self>,
        fixed_bytes: usize,
    ) -> Result<GeneratedModuleMemoryCharge, ModuleMemoryAdmissionError> {
        assert!(
            fixed_bytes > 0,
            "generated Wasm module charge must be greater than zero"
        );
        let mut state = self.state.lock();
        self.check_module_charge_locked(&state, 0, fixed_bytes)?;
        state.fixed_module_bytes = state
            .fixed_module_bytes
            .checked_add(fixed_bytes)
            .expect("generated Wasm module byte accounting overflow");
        let charge_id = state.next_module_charge_id;
        state.next_module_charge_id = charge_id.checked_add(1).expect("module charge ID overflow");
        state.module_charges.insert(
            charge_id,
            ModuleChargeInspection::loading(charge_id, fixed_bytes),
        );
        self.log_state_metrics_locked(&state);
        drop(state);
        log_memory_event("module_charge_reserved");
        Ok(GeneratedModuleMemoryCharge {
            controller: Arc::clone(self),
            fixed_bytes,
            charge_id,
        })
    }

    fn resize_module_charge(
        &self,
        charge_id: u64,
        old_fixed_bytes: usize,
        new_fixed_bytes: usize,
    ) -> Result<(), ModuleMemoryAdmissionError> {
        assert!(
            new_fixed_bytes > 0,
            "generated Wasm module charge must be greater than zero"
        );
        if old_fixed_bytes == new_fixed_bytes {
            return Ok(());
        }
        let mut state = self.state.lock();
        assert!(
            state.fixed_module_bytes >= old_fixed_bytes,
            "generated Wasm module byte accounting underflow"
        );
        if new_fixed_bytes > old_fixed_bytes {
            self.check_module_charge_locked(&state, old_fixed_bytes, new_fixed_bytes)?;
        }
        state.fixed_module_bytes = state
            .fixed_module_bytes
            .checked_sub(old_fixed_bytes)
            .and_then(|bytes| bytes.checked_add(new_fixed_bytes))
            .expect("generated Wasm module byte accounting overflow");
        state
            .module_charges
            .get_mut(&charge_id)
            .expect("module charge record disappeared")
            .bytes = new_fixed_bytes;
        self.log_state_metrics_locked(&state);
        drop(state);
        log_memory_event("module_charge_resized");
        if new_fixed_bytes < old_fixed_bytes {
            self.changed.notify_waiters();
        }
        Ok(())
    }

    fn check_module_charge_locked(
        &self,
        state: &ControllerState,
        replaced_fixed_bytes: usize,
        proposed_fixed_bytes: usize,
    ) -> Result<(), ModuleMemoryAdmissionError> {
        if self
            .policy
            .safety_reserve_bytes
            .checked_add(proposed_fixed_bytes)
            .is_none_or(|bytes| bytes > self.policy.hard_budget_bytes)
        {
            return Err(ModuleMemoryAdmissionError::FixedBytesExceedHardBudget);
        }
        if matches!(state.pressure, BackendPressure::Active { .. }) {
            return Err(ModuleMemoryAdmissionError::Pressure);
        }
        let Some(projected) = self
            .checked_projected_bytes_locked(state)
            .and_then(|bytes| bytes.checked_sub(replaced_fixed_bytes))
            .and_then(|bytes| bytes.checked_add(proposed_fixed_bytes))
        else {
            return Err(ModuleMemoryAdmissionError::HardBudget);
        };
        if projected > self.policy.hard_budget_bytes {
            return Err(ModuleMemoryAdmissionError::HardBudget);
        }
        if projected > self.policy.soft_budget_bytes {
            return Err(ModuleMemoryAdmissionError::SoftBudget);
        }
        Ok(())
    }

    pub(crate) fn module_reclaim_bytes_needed(
        &self,
        replaced_fixed_bytes: usize,
        proposed_fixed_bytes: usize,
    ) -> Option<usize> {
        // This is a reclamation target, not an admission grant. Drop the
        // controller lock before cache eviction destroys owners, then retry
        // actual admission against the current packed-buffer population.
        let state = self.state.lock();
        self.checked_projected_bytes_locked(&state)
            .and_then(|bytes| bytes.checked_sub(replaced_fixed_bytes))
            .and_then(|bytes| bytes.checked_add(proposed_fixed_bytes))
            .map(|bytes| bytes.saturating_sub(self.policy.soft_budget_bytes))
    }

    fn release_module_charge(&self, charge_id: u64, fixed_bytes: usize) {
        let mut state = self.state.lock();
        let removed = state
            .module_charges
            .remove(&charge_id)
            .expect("module charge record released twice");
        assert_eq!(
            removed.bytes, fixed_bytes,
            "module charge inspection drifted"
        );
        state.fixed_module_bytes = state
            .fixed_module_bytes
            .checked_sub(fixed_bytes)
            .expect("generated Wasm module charge released more than once");
        self.log_state_metrics_locked(&state);
        drop(state);
        log_memory_event("module_charge_released");
        self.changed.notify_waiters();
    }

    fn update_pressure_sample(&self, headroom_bytes: anyhow::Result<Option<usize>>) {
        let mut state = self.state.lock();
        let previous = state.pressure;
        let sample = headroom_bytes.ok().flatten();
        let previous_sample_available = state.pressure_sample_available;
        state.pressure_sample_available = sample.is_some();
        state.pressure = match sample {
            Some(headroom_bytes) => {
                let active = match previous {
                    BackendPressure::Active { .. } => {
                        headroom_bytes < self.policy.pressure_exit_headroom_bytes
                    },
                    BackendPressure::Unavailable | BackendPressure::Healthy { .. } => {
                        headroom_bytes <= self.policy.pressure_enter_headroom_bytes
                    },
                };
                if active {
                    BackendPressure::Active { headroom_bytes }
                } else {
                    BackendPressure::Healthy { headroom_bytes }
                }
            },
            None => match previous {
                // Losing the external pressure sample must not turn a known
                // overload into admission authority. Keep pressure active
                // until a later finite sample crosses the exit threshold.
                BackendPressure::Active { .. } => previous,
                BackendPressure::Unavailable | BackendPressure::Healthy { .. } => {
                    BackendPressure::Unavailable
                },
            },
        };
        match sample {
            None => {
                log_gauge(&GENERATED_WASM_MEMORY_CGROUP_SAMPLE_AVAILABLE_INFO, 0.0);
                if previous_sample_available {
                    log_memory_event("cgroup_sample_unavailable");
                }
            },
            Some(headroom_bytes) => {
                log_gauge(&GENERATED_WASM_MEMORY_CGROUP_SAMPLE_AVAILABLE_INFO, 1.0);
                log_gauge(
                    &GENERATED_WASM_MEMORY_CGROUP_HEADROOM_BYTES,
                    headroom_bytes as f64,
                );
            },
        }
        let pressure_changed = matches!(previous, BackendPressure::Active { .. })
            != matches!(state.pressure, BackendPressure::Active { .. });
        drop(state);
        if pressure_changed {
            log_memory_event(
                if matches!(self.pressure(), BackendPressure::Active { .. }) {
                    "pressure_enter"
                } else {
                    "pressure_exit"
                },
            );
            self.changed.notify_waiters();
        }
    }

    pub(crate) async fn admit(
        self: &Arc<Self>,
        identity: FunctionMemoryIdentity,
        existing_slot: Option<GeneratedSlotId>,
    ) -> Result<InvocationMemoryPermit, AdmissionRejection> {
        let mut waited = false;
        loop {
            let notification = self.changed.notified();
            tokio::pin!(notification);
            let _ = notification.as_mut().enable();
            match self.try_admit(identity.clone(), existing_slot) {
                Ok(permit) => {
                    if waited {
                        self.record_admission_wait_completed();
                    }
                    return Ok(permit);
                },
                Err(TryAdmissionError::Permanent(reason)) => {
                    self.record_admission_rejection();
                    return Err(reason);
                },
                Err(TryAdmissionError::Wait(reason)) => {
                    if !waited {
                        self.record_admission_wait(reason);
                        waited = true;
                    }
                    notification.await;
                },
            }
        }
    }

    pub(crate) fn change_notification(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }

    pub(crate) fn try_admit(
        self: &Arc<Self>,
        identity: FunctionMemoryIdentity,
        existing_slot: Option<GeneratedSlotId>,
    ) -> Result<InvocationMemoryPermit, TryAdmissionError> {
        self.try_admit_inner(identity, existing_slot, false)
    }

    pub(crate) fn try_admit_preparation(
        self: &Arc<Self>,
        identity: FunctionMemoryIdentity,
    ) -> Result<InvocationMemoryPermit, TryAdmissionError> {
        self.try_admit_inner(identity, None, true)
    }

    fn try_admit_inner(
        self: &Arc<Self>,
        identity: FunctionMemoryIdentity,
        existing_slot: Option<GeneratedSlotId>,
        preparation: bool,
    ) -> Result<InvocationMemoryPermit, TryAdmissionError> {
        let mut state = self.state.lock();
        state.clock = state
            .clock
            .checked_add(1)
            .expect("memory controller clock overflow");
        let clock = state.clock;
        let learned_forecast = self.forecast_locked(&mut state, &identity, clock)?;
        let forecast_bytes = if existing_slot.is_none() {
            state.functions[&identity].preparation_forecast(self.policy.cold_peak_growth_bytes)
        } else {
            learned_forecast
        };
        // Preparation retains its baseline and a separate allowance for the
        // next warm invocation. Do not reserve initialization twice.
        let preparation_headroom_bytes = if preparation { learned_forecast } else { 0 };
        let proposed_fixed_bytes = if existing_slot.is_some() {
            0
        } else {
            self.policy.unattributed_bytes_per_slot
        };
        let permanent_bytes = self
            .policy
            .safety_reserve_bytes
            .checked_add(forecast_bytes)
            .and_then(|bytes| bytes.checked_add(preparation_headroom_bytes))
            .and_then(|bytes| bytes.checked_add(proposed_fixed_bytes));
        if permanent_bytes.is_none_or(|bytes| bytes > self.policy.hard_budget_bytes) {
            return Err(TryAdmissionError::Permanent(
                AdmissionRejection::ForecastExceedsHardBudget,
            ));
        }
        if matches!(state.pressure, BackendPressure::Active { .. }) {
            return Err(TryAdmissionError::Wait(AdmissionWaitReason::Pressure));
        }
        let active_count = state.active.len();
        if active_count >= self.policy.hard_instance_ceiling {
            return Err(TryAdmissionError::Wait(
                AdmissionWaitReason::ActiveCountCeiling,
            ));
        }
        let checkout_baseline_bytes = match existing_slot {
            Some(slot_id) => {
                let slot = state
                    .slots
                    .get(&slot_id)
                    .expect("generated Wasm pool referenced an unknown memory slot");
                if slot.status != SlotStatus::Idle {
                    return Err(TryAdmissionError::Wait(
                        AdmissionWaitReason::SlotUnavailable,
                    ));
                }
                slot.retained_baseline()
            },
            None if state.slots.len() < self.policy.hard_instance_ceiling => 0,
            None => {
                return Err(TryAdmissionError::Wait(
                    AdmissionWaitReason::InstanceCountCeiling,
                ));
            },
        };
        let reserved_growth = existing_slot.map_or(0, |slot| state.slots[&slot].idle_growth_bytes);
        let Some(projected) = self
            .checked_projected_bytes_locked(&state)
            .and_then(|bytes| bytes.checked_sub(reserved_growth))
            .and_then(|bytes| bytes.checked_add(forecast_bytes))
            .and_then(|bytes| bytes.checked_add(preparation_headroom_bytes))
            .and_then(|bytes| bytes.checked_add(proposed_fixed_bytes))
        else {
            return Err(TryAdmissionError::Wait(AdmissionWaitReason::HardBudget));
        };
        if projected > self.policy.hard_budget_bytes {
            return Err(TryAdmissionError::Wait(AdmissionWaitReason::HardBudget));
        }
        if projected > self.policy.soft_budget_bytes {
            return Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget));
        }

        let slot_id = match existing_slot {
            Some(slot_id) => slot_id,
            None => {
                let slot_id = GeneratedSlotId(state.next_slot_id);
                state.next_slot_id = state
                    .next_slot_id
                    .checked_add(1)
                    .expect("generated Wasm slot ID overflow");
                state.slots.insert(
                    slot_id,
                    SlotMemory {
                        identity: identity.clone(),
                        module_charge_ids: Vec::new(),
                        guest_bytes: 0,
                        shared_static_bytes: 0,
                        retained_host_bytes: 0,
                        idle_growth_bytes: 0,
                        status: SlotStatus::Idle,
                    },
                );
                slot_id
            },
        };
        let invocation_id = state.next_invocation_id;
        state.next_invocation_id = state
            .next_invocation_id
            .checked_add(1)
            .expect("generated Wasm invocation ID overflow");
        let slot = state
            .slots
            .get_mut(&slot_id)
            .expect("admitted generated Wasm slot disappeared");
        slot.identity = identity.clone();
        let current_guest_bytes = slot.guest_bytes;
        let shared_static_bytes = slot.shared_static_bytes;
        let current_host_bytes = slot.retained_host_bytes;
        slot.status = SlotStatus::Active { invocation_id };
        state.active.insert(
            invocation_id,
            ActiveInvocation {
                slot_id,
                identity: identity.clone(),
                checkout_baseline_bytes,
                current_guest_bytes,
                shared_static_bytes,
                current_host_bytes,
                peak_accounted_bytes: checkout_baseline_bytes,
                peak_admitted_bytes: checkout_baseline_bytes,
                peak_guest_bytes: current_guest_bytes,
                peak_host_bytes: current_host_bytes,
                peaks_before_static_memory: None,
                forecast_bytes,
                liability_bytes: forecast_bytes,
                preparation_headroom_bytes,
                cold_start: existing_slot.is_none(),
                prediction_overrun_recorded: false,
                growth_denied: false,
            },
        );
        self.log_state_metrics_locked(&state);
        drop(state);
        log_memory_event("admitted");
        Ok(InvocationMemoryPermit {
            observation: None,
            observation_started: false,
            reused_instance: false,
            reuse_enabled: false,
            route_context: None,
            route_statistics_configured: false,
            account: Arc::new(InvocationMemoryAccount {
                controller: Arc::clone(self),
                identity,
                invocation_id,
                slot_id,
                current_guest_bytes: AtomicUsize::new(current_guest_bytes),
                current_host_bytes: AtomicUsize::new(current_host_bytes),
                finished: AtomicBool::new(false),
            }),
        })
    }

    pub(crate) fn record_admission_wait(&self, reason: AdmissionWaitReason) {
        log_memory_event("admission_wait");
        log_memory_event(reason.metric_name());
    }

    pub(crate) fn record_admission_wait_completed(&self) {
        log_memory_event("admission_wait_completed");
    }

    pub(crate) fn record_admission_rejection(&self) {
        log_memory_event("admission_rejected");
    }

    fn forecast_locked(
        &self,
        state: &mut ControllerState,
        identity: &FunctionMemoryIdentity,
        clock: u64,
    ) -> Result<usize, TryAdmissionError> {
        if let Some(stats) = state.functions.get_mut(identity) {
            stats.last_used = clock;
            return Ok(stats.forecast(self.policy.cold_peak_growth_bytes));
        }
        if state.functions.len() == MAX_FUNCTION_RECORDS {
            let retained_identities = state
                .slots
                .values()
                .map(|slot| &slot.identity)
                .collect::<BTreeSet<_>>();
            let evicted = state
                .functions
                .iter()
                .filter(|(candidate, _)| !retained_identities.contains(candidate))
                .min_by_key(|(_, stats)| stats.last_used)
                .map(|(identity, _)| identity.clone());
            let Some(evicted) = evicted else {
                return Err(TryAdmissionError::Wait(
                    AdmissionWaitReason::FunctionRecordCapacity,
                ));
            };
            state.functions.remove(&evicted);
            log_memory_event("function_record_evicted");
        }
        let inherited_forecast_bytes = state
            .functions
            .iter()
            .filter(|(candidate, _)| candidate.same_lineage(identity))
            .map(|(_, stats)| stats.forecast(self.policy.cold_peak_growth_bytes))
            .max()
            .or_else(|| state.global_peak_growth.quantile(99, 100))
            .unwrap_or(self.policy.cold_peak_growth_bytes)
            .max(self.policy.cold_peak_growth_bytes);
        state.functions.insert(
            identity.clone(),
            FunctionMemoryStats::new(inherited_forecast_bytes, clock),
        );
        log_memory_event(
            if inherited_forecast_bytes > self.policy.cold_peak_growth_bytes {
                "inherited_prior"
            } else {
                "cold_prior"
            },
        );
        Ok(inherited_forecast_bytes)
    }

    fn checked_projected_bytes_locked(&self, state: &ControllerState) -> Option<usize> {
        self.checked_projection_with_shared_buffers(state, self.shared_buffers.bytes())
    }

    // Inspection must use the same equation with its one captured backing
    // count: database owners can allocate or release buffers concurrently.
    fn checked_projection_with_shared_buffers(
        &self,
        state: &ControllerState,
        shared_buffers: usize,
    ) -> Option<usize> {
        let retained = state.slots.values().try_fold(0usize, |total, slot| {
            total.checked_add(
                slot.guest_bytes
                    .checked_sub(slot.shared_static_bytes)?
                    .checked_add(slot.retained_host_bytes)?,
            )
        })?;
        let liabilities = state
            .active
            .values()
            .try_fold(0usize, |total, invocation| {
                total
                    .checked_add(invocation.liability_bytes)?
                    .checked_add(invocation.preparation_headroom_bytes)
            })?;
        let idle_growth = state
            .slots
            .values()
            .filter(|slot| slot.status == SlotStatus::Idle)
            .try_fold(0usize, |total, slot| {
                total.checked_add(slot.idle_growth_bytes)
            })?;
        retained
            .checked_add(liabilities)?
            .checked_add(idle_growth)?
            .checked_add(state.fixed_module_bytes)?
            // Cache retention limits are not additional live allocations. Count
            // all packed backing here once, including bytes still pinned after
            // eviction and bytes held by non-Wasm readers. Invocation liabilities
            // exclude these bytes but retain private containers and guest copies.
            .checked_add(shared_buffers)?
            .checked_add(
                state
                    .slots
                    .len()
                    .checked_mul(self.policy.unattributed_bytes_per_slot)?,
            )
            .and_then(|bytes| bytes.checked_add(self.policy.safety_reserve_bytes))
    }

    #[cfg(test)]
    fn projected_bytes_locked(&self, state: &ControllerState) -> usize {
        // A saturated metric remains conservative, while admission paths use
        // the checked form above so overflow can never grant capacity.
        self.checked_projected_bytes_locked(state)
            .unwrap_or(usize::MAX)
    }

    fn update_accounted_bytes(
        &self,
        invocation_id: u64,
        guest_bytes: usize,
        host_bytes: usize,
        denied_growth_bytes: Option<usize>,
        allocation_committed: bool,
        initial_static_credit: Option<usize>,
    ) -> bool {
        let mut state = self.state.lock();
        let Some(invocation) = state.active.get(&invocation_id) else {
            return false;
        };
        let old_liability = invocation.liability_bytes;
        let checkout_baseline_bytes = invocation.checkout_baseline_bytes;
        let forecast_bytes = invocation.forecast_bytes;
        if let Some(credit) = initial_static_credit {
            assert!(
                invocation.cold_start
                    && invocation.current_guest_bytes == 0
                    && invocation.shared_static_bytes == 0
                    && invocation.checkout_baseline_bytes == 0
                    && credit <= guest_bytes
                    && denied_growth_bytes.is_none()
                    && !allocation_committed,
                "initial static credit requires a fresh guest allocation"
            );
        }
        let shared_static_bytes = initial_static_credit.unwrap_or(invocation.shared_static_bytes);
        let private_guest_bytes = guest_bytes
            .checked_sub(shared_static_bytes)
            .expect("guest capacity is smaller than its shared static region");
        let accounted_bytes = private_guest_bytes.checked_add(host_bytes);
        let observed_peak = denied_growth_bytes
            .map(|denied_guest| {
                denied_guest
                    .saturating_sub(shared_static_bytes)
                    .saturating_add(host_bytes)
            })
            .unwrap_or_else(|| private_guest_bytes.saturating_add(host_bytes));
        let growth_bytes =
            accounted_bytes.map(|bytes| bytes.saturating_sub(checkout_baseline_bytes));
        let proposed_liability = growth_bytes.map(|bytes| bytes.max(forecast_bytes));
        // Database owners can move the shared counter above the hard budget
        // independently. A release (or the commit of an unchanged preflight)
        // must still replace the old charge; rejecting it strands freed bytes.
        let nonincreasing = accounted_bytes.is_some_and(|bytes| {
            bytes
                <= invocation
                    .current_guest_bytes
                    .saturating_sub(invocation.shared_static_bytes)
                    .saturating_add(invocation.current_host_bytes)
        }) && proposed_liability.is_some_and(|bytes| bytes <= old_liability);
        let projected = self
            .checked_projected_bytes_locked(&state)
            .and_then(|bytes| bytes.checked_sub(old_liability))
            .and_then(|bytes| bytes.checked_add(proposed_liability?));
        let invocation = state
            .active
            .get_mut(&invocation_id)
            .expect("generated Wasm invocation disappeared during accounting");
        invocation.peak_accounted_bytes = invocation.peak_accounted_bytes.max(observed_peak);
        let denied = denied_growth_bytes.is_some()
            || accounted_bytes.is_none()
            || (!nonincreasing
                && projected.is_none_or(|bytes| bytes > self.policy.hard_budget_bytes));
        if denied {
            invocation.growth_denied = true;
            log_memory_event("growth_denied");
            if !allocation_committed || accounted_bytes.is_none() {
                return false;
            }
        }
        let growth_bytes = growth_bytes.expect("checked invocation growth disappeared");
        let proposed_liability =
            proposed_liability.expect("checked invocation liability disappeared");
        invocation.current_guest_bytes = guest_bytes;
        // Install the verified credit and raw allocation together. A rejected
        // allocation must leave neither capacity nor a phantom credit behind.
        invocation.shared_static_bytes = shared_static_bytes;
        invocation.current_host_bytes = host_bytes;
        // A post-allocation denial still retains real container capacity. Later
        // cleanup decreases must not reclassify the rejected capacity as an
        // admitted peak after terminal denial has been latched.
        if !invocation.growth_denied {
            invocation.peak_admitted_bytes = invocation
                .peak_admitted_bytes
                .max(accounted_bytes.expect("admitted invocation accounting overflowed"));
            invocation.peak_guest_bytes = invocation.peak_guest_bytes.max(guest_bytes);
            invocation.peak_host_bytes = invocation.peak_host_bytes.max(host_bytes);
        }
        invocation.liability_bytes = proposed_liability;
        if growth_bytes > forecast_bytes && !invocation.prediction_overrun_recorded {
            invocation.prediction_overrun_recorded = true;
            log_memory_event("prediction_overrun");
        }
        self.log_state_metrics_locked(&state);
        drop(state);
        if proposed_liability < old_liability {
            // Releasing transient host buffers can admit another Store while
            // this invocation remains active or suspended on a database read.
            self.changed.notify_waiters();
        }
        !denied
    }

    fn record_route_statistics_locked(
        state: &mut ControllerState,
        context: &GeneratedMemoryRouteContext,
        memory_identity: &FunctionMemoryIdentity,
        observation: &WasmMemoryObservation,
        clock: u64,
    ) -> bool {
        let mut evicted = false;
        if !state.routes.contains_key(&context.identity)
            && state.routes.len() == MAX_ROUTE_STATISTICS_RECORDS
        {
            let oldest = state
                .routes
                .iter()
                .min_by_key(|(_, statistics)| statistics.last_used)
                .map(|(identity, _)| identity.clone())
                .expect("full generated Wasm route statistics store is empty");
            state.routes.remove(&oldest);
            state.route_record_eviction_count = state.route_record_eviction_count.saturating_add(1);
            evicted = true;
        }
        let statistics = state.routes.entry(context.identity.clone()).or_default();
        if statistics.memory_identity.as_ref() != Some(memory_identity) {
            if statistics.memory_identity.is_some() {
                // A changed engine/policy identity is a new population. Do not
                // relabel old histograms with the latest invocation's identity.
                *statistics = RouteMemoryStatistics::default();
                state.route_record_eviction_count =
                    state.route_record_eviction_count.saturating_add(1);
                evicted = true;
            }
            statistics.memory_identity = Some(memory_identity.clone());
        }
        statistics.observe(context.role, observation, clock);
        evicted
    }

    fn complete(
        &self,
        invocation_id: u64,
        terminal: TerminalMemoryOutcome,
        retain_slot: bool,
        reused_instance: bool,
        reuse_enabled: bool,
        route_context: Option<&GeneratedMemoryRouteContext>,
    ) -> WasmMemoryObservation {
        let mut state = self.state.lock();
        state.clock = state
            .clock
            .checked_add(1)
            .expect("memory controller clock overflow");
        let clock = state.clock;
        let invocation = state
            .active
            .remove(&invocation_id)
            .expect("generated Wasm invocation completed more than once");
        let terminal = if invocation.growth_denied {
            TerminalMemoryOutcome::MemoryLimit
        } else {
            terminal
        };
        let terminal_metric = terminal.metric_name();
        let current_accounted_bytes = invocation
            .current_guest_bytes
            .checked_sub(invocation.shared_static_bytes)
            .expect("shared static bytes exceed completed guest capacity")
            .saturating_add(invocation.current_host_bytes);
        let observation = WasmMemoryObservation {
            reused_instance,
            checkout_baseline_bytes: invocation.checkout_baseline_bytes as u64,
            peak_admitted_bytes: invocation.peak_admitted_bytes as u64,
            peak_guest_bytes: invocation.peak_guest_bytes as u64,
            peak_host_bytes: invocation.peak_host_bytes as u64,
            requested_peak_bytes: invocation.peak_accounted_bytes as u64,
            forecast_growth_bytes: invocation.forecast_bytes as u64,
            completion_bytes: current_accounted_bytes as u64,
            returned_to_pool_bytes: if retain_slot {
                current_accounted_bytes as u64
            } else {
                0
            },
            returned_to_pool: retain_slot,
            forecast_overrun: invocation.prediction_overrun_recorded,
            growth_denied: invocation.growth_denied,
            successful_execution_discarded: terminal == TerminalMemoryOutcome::Success
                && !retain_slot
                && reuse_enabled,
            terminal: match terminal {
                TerminalMemoryOutcome::Success => WasmMemoryTerminal::Success,
                TerminalMemoryOutcome::DeveloperError => WasmMemoryTerminal::DeveloperError,
                TerminalMemoryOutcome::SystemError => WasmMemoryTerminal::SystemError,
                TerminalMemoryOutcome::Cancellation => WasmMemoryTerminal::Cancellation,
                TerminalMemoryOutcome::Timeout => WasmMemoryTerminal::Timeout,
                TerminalMemoryOutcome::ResourceLimit => WasmMemoryTerminal::ResourceLimit,
                TerminalMemoryOutcome::MemoryLimit => WasmMemoryTerminal::MemoryLimit,
            },
        };
        let checkout_baseline_bytes =
            usize::try_from(observation.checkout_baseline_bytes).unwrap_or(usize::MAX);
        let peak_growth = usize::try_from(
            observation
                .requested_peak_bytes
                .saturating_sub(observation.checkout_baseline_bytes),
        )
        .unwrap_or(usize::MAX);
        let retained_memory = usize::try_from(observation.completion_bytes).unwrap_or(usize::MAX);
        let retained_change = signed_difference(retained_memory, checkout_baseline_bytes);
        let stats = state
            .functions
            .get_mut(&invocation.identity)
            .expect("generated Wasm function statistics disappeared while active");
        let previous_warm_forecast = stats.forecast(self.policy.cold_peak_growth_bytes);
        let previous_cold_forecast = stats.preparation_forecast(self.policy.cold_peak_growth_bytes);
        // A canceled, limited or failed execution only supplies a lower bound
        // on its demand. It can raise a forecast, but a truncated low sample
        // cannot train it down. Route distributions retain every actual sample.
        let complete_sample = match terminal {
            TerminalMemoryOutcome::Success | TerminalMemoryOutcome::DeveloperError => true,
            TerminalMemoryOutcome::SystemError
            | TerminalMemoryOutcome::Cancellation
            | TerminalMemoryOutcome::Timeout
            | TerminalMemoryOutcome::ResourceLimit
            | TerminalMemoryOutcome::MemoryLimit => false,
        };
        if invocation.cold_start {
            // Fresh Stores can be interrupted partway through initialization,
            // just as warm handlers can be interrupted before their peak.
            if complete_sample || peak_growth >= previous_cold_forecast {
                stats.preparation_peak_growth.observe(peak_growth);
            }
        } else if complete_sample || peak_growth >= previous_warm_forecast {
            stats.warm_peak_growth.observe(peak_growth);
            stats.warm_sample_count = stats.warm_sample_count.saturating_add(1);
        }
        stats.observe(
            peak_growth,
            retained_memory,
            retained_change,
            usize::try_from(observation.forecast_growth_bytes).unwrap_or(usize::MAX),
            terminal,
            clock,
        );
        let warm_forecast = stats.forecast(self.policy.cold_peak_growth_bytes);
        state.global_peak_growth.observe(peak_growth);
        if retain_slot {
            let slot = state
                .slots
                .get_mut(&invocation.slot_id)
                .expect("completed generated Wasm slot disappeared");
            assert_eq!(
                slot.status,
                SlotStatus::Active { invocation_id },
                "generated Wasm slot active invocation drifted"
            );
            slot.guest_bytes = invocation.current_guest_bytes;
            slot.shared_static_bytes = invocation.shared_static_bytes;
            slot.retained_host_bytes = invocation.current_host_bytes;
            slot.idle_growth_bytes = slot.idle_growth_bytes.min(warm_forecast);
            slot.status = SlotStatus::Idle;
        } else {
            let removed = state
                .slots
                .remove(&invocation.slot_id)
                .expect("discarded generated Wasm slot disappeared");
            assert_eq!(
                removed.status,
                SlotStatus::Active { invocation_id },
                "discarded generated Wasm slot active invocation drifted"
            );
        }
        // Idle Stores can otherwise retain their original cold allowance
        // indefinitely while another Store supplies all warm observations.
        // Shrinking releases only reservations, never retained memory. Larger
        // forecasts are admitted on checkout and reserved again on return.
        if warm_forecast < previous_warm_forecast {
            for slot in state.slots.values_mut() {
                if slot.identity == invocation.identity && slot.status == SlotStatus::Idle {
                    slot.idle_growth_bytes = slot.idle_growth_bytes.min(warm_forecast);
                }
            }
        }
        let route_record_evicted = route_context.is_some_and(|context| {
            Self::record_route_statistics_locked(
                &mut state,
                context,
                &invocation.identity,
                &observation,
                clock,
            )
        });
        self.log_state_metrics_locked(&state);
        drop(state);
        log_memory_event(terminal_metric);
        if route_record_evicted {
            log_memory_event("route_statistics_record_evicted");
        }
        self.changed.notify_waiters();
        observation
    }

    fn complete_unstarted(&self, invocation_id: u64, retain_slot: bool) {
        let mut state = self.state.lock();
        let invocation = state
            .active
            .remove(&invocation_id)
            .expect("generated Wasm unstarted invocation completed more than once");
        if retain_slot {
            let forecast =
                state.functions[&invocation.identity].forecast(self.policy.cold_peak_growth_bytes);
            let slot = state
                .slots
                .get_mut(&invocation.slot_id)
                .expect("unstarted generated Wasm slot disappeared");
            assert_eq!(
                slot.status,
                SlotStatus::Active { invocation_id },
                "unstarted generated Wasm slot active invocation drifted"
            );
            // Another Store may have trained the forecast down while this
            // checkout waited for CPU. It was active during idle refresh.
            slot.idle_growth_bytes = slot.idle_growth_bytes.min(forecast);
            slot.status = SlotStatus::Idle;
        } else {
            let removed = state
                .slots
                .remove(&invocation.slot_id)
                .expect("discarded unstarted generated Wasm slot disappeared");
            assert_eq!(
                removed.status,
                SlotStatus::Active { invocation_id },
                "discarded unstarted generated Wasm slot active invocation drifted"
            );
        }
        self.log_state_metrics_locked(&state);
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) fn preparation_instance_capacity(
        &self,
        owned_slots: usize,
    ) -> PreparationInstanceCapacity {
        PreparationInstanceCapacity {
            available: self
                .policy
                .hard_instance_ceiling
                .saturating_sub(self.state.lock().slots.len())
                .saturating_add(owned_slots)
                .min(self.policy.hard_instance_ceiling),
            limit: self.policy.hard_instance_ceiling,
        }
    }

    pub(crate) fn estimated_preparation_memory_cost(
        &self,
        identity: &FunctionMemoryIdentity,
        observed: PreparationMemoryCost,
    ) -> Option<PreparationMemoryCost> {
        let state = self.state.lock();
        // An evicted forecast is unknown again; preparation orders unknown
        // groups first and ordinary admission recreates their conservative prior.
        let stats = state.functions.get(identity)?;
        let forecast = stats.preparation_forecast(self.policy.cold_peak_growth_bytes);
        let warm_forecast = stats.forecast(self.policy.cold_peak_growth_bytes);
        let retained_bytes = observed.retained_bytes.saturating_add(warm_forecast);
        Some(PreparationMemoryCost {
            peak_bytes: observed
                .peak_bytes
                .max(forecast)
                .saturating_add(warm_forecast)
                .max(retained_bytes),
            retained_bytes,
        })
    }

    pub(crate) fn preparation_eviction_candidates(
        &self,
        identity: &FunctionMemoryIdentity,
        candidates: &[(GeneratedSlotId, Duration)],
    ) -> Vec<(GeneratedSlotId, IdleEvictionReason)> {
        let mut state = self.state.lock();
        if matches!(state.pressure, BackendPressure::Active { .. }) {
            return Vec::new();
        }
        let clock = state.clock;
        if self.forecast_locked(&mut state, identity, clock).is_err() {
            return Vec::new();
        }
        let forecast =
            state.functions[identity].preparation_forecast(self.policy.cold_peak_growth_bytes);
        let warm_forecast = state.functions[identity].forecast(self.policy.cold_peak_growth_bytes);
        let projected = self
            .checked_projected_bytes_locked(&state)
            .and_then(|bytes| bytes.checked_add(forecast))
            .and_then(|bytes| bytes.checked_add(warm_forecast))
            .and_then(|bytes| bytes.checked_add(self.policy.unattributed_bytes_per_slot));
        self.select_budget_evictions_locked(&mut state, projected, None, candidates)
    }

    pub(crate) fn invocation_eviction_candidates(
        &self,
        identity: &FunctionMemoryIdentity,
        existing_slot: Option<GeneratedSlotId>,
        candidates: &[(GeneratedSlotId, Duration)],
    ) -> Vec<(GeneratedSlotId, IdleEvictionReason)> {
        let mut state = self.state.lock();
        if matches!(state.pressure, BackendPressure::Active { .. }) {
            return Vec::new();
        }
        let clock = state.clock;
        let Ok(warm_forecast) = self.forecast_locked(&mut state, identity, clock) else {
            return Vec::new();
        };
        let projected = if let Some(slot_id) = existing_slot {
            let slot = &state.slots[&slot_id];
            assert_eq!(slot.status, SlotStatus::Idle);
            self.checked_projected_bytes_locked(&state)
                .and_then(|bytes| bytes.checked_sub(slot.idle_growth_bytes))
                .and_then(|bytes| bytes.checked_add(warm_forecast))
        } else {
            let cold_forecast =
                state.functions[identity].preparation_forecast(self.policy.cold_peak_growth_bytes);
            self.checked_projected_bytes_locked(&state)
                .and_then(|bytes| bytes.checked_add(cold_forecast))
                .and_then(|bytes| bytes.checked_add(self.policy.unattributed_bytes_per_slot))
        };
        self.select_budget_evictions_locked(&mut state, projected, existing_slot, candidates)
    }

    fn select_budget_evictions_locked(
        &self,
        state: &mut ControllerState,
        mut projected: Option<usize>,
        existing_slot: Option<GeneratedSlotId>,
        candidates: &[(GeneratedSlotId, Duration)],
    ) -> Vec<(GeneratedSlotId, IdleEvictionReason)> {
        let mut remaining_slots = state.slots.len();
        let mut reclaimed = BTreeSet::new();
        let mut selected = Vec::new();
        let fits = |projected: Option<usize>, remaining_slots| {
            (existing_slot.is_some() || remaining_slots < self.policy.hard_instance_ceiling)
                && projected.is_some_and(|bytes| bytes <= self.policy.soft_budget_bytes)
        };
        for (slot_id, _) in candidates {
            if fits(projected, remaining_slots) {
                break;
            }
            // Preserve the Store the waiting invocation can actually reuse.
            if Some(*slot_id) == existing_slot {
                continue;
            }
            assert!(reclaimed.insert(*slot_id), "duplicate reclaim candidate");
            let slot = state
                .slots
                .get(slot_id)
                .expect("reclaim candidate lost its slot");
            assert_eq!(
                slot.status,
                SlotStatus::Idle,
                "reclaim candidate is not idle"
            );
            remaining_slots = remaining_slots
                .checked_sub(1)
                .expect("reclaim candidate count exceeds live slots");
            projected = projected
                .and_then(|bytes| bytes.checked_sub(slot.retained_baseline()))
                .and_then(|bytes| bytes.checked_sub(slot.idle_growth_bytes))
                .and_then(|bytes| bytes.checked_sub(self.policy.unattributed_bytes_per_slot));
            selected.push((*slot_id, IdleEvictionReason::AdmissionBudget));
        }
        if !fits(projected, remaining_slots) {
            return Vec::new();
        }
        // If external packed owners are the limiting population, destroying
        // useful Stores cannot make this admission fit. Reclaim only a complete
        // sufficient prefix before preparation resumes, and keep capacity intact
        // otherwise. Removing just one baseline lets maintenance rebuild it
        // before the next removal, preventing a group needing two removals from
        // making progress.
        for (slot_id, _) in &selected {
            state
                .slots
                .get_mut(slot_id)
                .expect("budget eviction lost its validated slot")
                .status = SlotStatus::Evicting;
        }
        if !selected.is_empty() {
            self.log_state_metrics_locked(&state);
        }
        selected
    }

    fn reserve_idle_growth(&self, invocation_id: u64) -> bool {
        let mut state = self.state.lock();
        let invocation = state
            .active
            .get(&invocation_id)
            .expect("idle reservation lost its active invocation");
        let forecast = state.functions[&invocation.identity]
            .forecast(self.policy.cold_peak_growth_bytes)
            .max(if invocation.cold_start {
                0
            } else {
                invocation
                    .peak_accounted_bytes
                    .saturating_sub(invocation.checkout_baseline_bytes)
            });
        // Active slots retain their checkout baseline in the slot ledger;
        // growth is charged through the invocation liability until completion.
        // Reserve both that growth and the next checkout's allowance.
        let liability = invocation
            .current_guest_bytes
            .checked_sub(invocation.shared_static_bytes)
            .expect("shared static bytes exceed returning guest capacity")
            .saturating_add(invocation.current_host_bytes)
            .saturating_sub(invocation.checkout_baseline_bytes)
            .checked_add(forecast);
        let projected = self
            .checked_projected_bytes_locked(&state)
            .and_then(|bytes| bytes.checked_sub(invocation.liability_bytes))
            .and_then(|bytes| bytes.checked_sub(invocation.preparation_headroom_bytes))
            .and_then(|bytes| bytes.checked_add(liability?));
        // External packed owners can cross the soft budget during execution.
        // Moving an already admitted liability back to idle needs no new
        // capacity; rejecting that move destroys useful Stores without freeing
        // the external backing that blocked admission.
        let nonincreasing = liability.is_some_and(|bytes| {
            invocation
                .liability_bytes
                .checked_add(invocation.preparation_headroom_bytes)
                .is_some_and(|reserved| bytes <= reserved)
        });
        if matches!(state.pressure, BackendPressure::Active { .. })
            || (!nonincreasing
                && projected.is_none_or(|bytes| bytes > self.policy.soft_budget_bytes))
        {
            return false;
        }
        let slot_id = invocation.slot_id;
        let invocation = state
            .active
            .get_mut(&invocation_id)
            .expect("reservation invocation disappeared");
        invocation.liability_bytes = liability.expect("validated idle liability disappeared");
        invocation.preparation_headroom_bytes = 0;
        state
            .slots
            .get_mut(&slot_id)
            .expect("reservation slot disappeared")
            .idle_growth_bytes = forecast;
        self.log_state_metrics_locked(&state);
        true
    }

    pub(crate) fn record_admission_timeout(&self) {
        log_memory_event("admission_timeout");
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn set_pressure_for_test(&self, pressure: BackendPressure) {
        if pressure == BackendPressure::Unavailable {
            let mut state = self.state.lock();
            state.pressure = pressure;
            state.pressure_sample_available = false;
            return;
        }
        let headroom = match pressure {
            BackendPressure::Unavailable => unreachable!("handled unavailable pressure above"),
            BackendPressure::Healthy { headroom_bytes }
            | BackendPressure::Active { headroom_bytes } => Ok(Some(headroom_bytes)),
        };
        self.update_pressure_sample(headroom);
        assert_eq!(self.pressure(), pressure);
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn snapshot_for_test(
        &self,
        identity: &FunctionMemoryIdentity,
    ) -> GeneratedMemoryTestSnapshot {
        let state = self.state.lock();
        let aggregate = self.aggregate_metrics_locked(&state, self.shared_buffers.bytes());
        let stats = state.functions.get(identity);
        GeneratedMemoryTestSnapshot {
            active_instances: aggregate.active_instances,
            idle_instances: aggregate.idle_instances,
            evicting_instances: aggregate.evicting_instances,
            fixed_module_bytes: state.fixed_module_bytes,
            retained_baseline_bytes: aggregate.retained_baseline_bytes,
            active_checkout_baseline_bytes: aggregate.active_checkout_baseline_bytes,
            active_guest_bytes: aggregate.active_guest_bytes,
            active_host_bytes: aggregate.active_host_bytes,
            active_current_growth_bytes: aggregate.active_current_growth_bytes,
            active_forecast_bytes: aggregate.active_forecast_bytes,
            active_liability_bytes: aggregate.active_liability_bytes,
            unattributed_allowance_bytes: aggregate.unattributed_allowance_bytes,
            projected_bytes: aggregate.projected_bytes,
            function_samples: stats.map_or(0, |stats| stats.sample_count),
            developer_error_samples: stats.map_or(0, |stats| {
                stats.terminal_samples[TerminalMemoryOutcome::DeveloperError.index()]
            }),
            system_error_samples: stats.map_or(0, |stats| {
                stats.terminal_samples[TerminalMemoryOutcome::SystemError.index()]
            }),
            timeout_samples: stats.map_or(0, |stats| {
                stats.terminal_samples[TerminalMemoryOutcome::Timeout.index()]
            }),
            resource_limit_samples: stats.map_or(0, |stats| {
                stats.terminal_samples[TerminalMemoryOutcome::ResourceLimit.index()]
            }),
            memory_limit_samples: stats.map_or(0, |stats| {
                stats.terminal_samples[TerminalMemoryOutcome::MemoryLimit.index()]
            }),
        }
    }

    pub(crate) fn idle_eviction_candidates(
        &self,
        protected_from_age: &BTreeSet<GeneratedSlotId>,
        idle_slots_oldest_first: &[(GeneratedSlotId, Duration)],
        trigger: IdleEvictionTrigger,
    ) -> Vec<(GeneratedSlotId, IdleEvictionReason)> {
        let mut state = self.state.lock();
        let forced_reason = match trigger {
            IdleEvictionTrigger::GenerationRetirement => {
                Some(IdleEvictionReason::GenerationRetirement)
            },
            IdleEvictionTrigger::InitializationInvalidation => {
                Some(IdleEvictionReason::InitializationInvalidation)
            },
            IdleEvictionTrigger::Maintenance
            | IdleEvictionTrigger::NewSlot
            | IdleEvictionTrigger::AdmissionBudget => None,
        };
        if let Some(reason) = forced_reason {
            // The caller holds the generated instance-pool lock and supplies
            // only retired or invalidated slots. Validate the complete set
            // before changing ledger state so failure cannot mark a partial set.
            for (index, (slot_id, _)) in idle_slots_oldest_first.iter().enumerate() {
                assert!(
                    !idle_slots_oldest_first[..index]
                        .iter()
                        .any(|(candidate, _)| candidate == slot_id),
                    "retired generated Wasm slot was supplied more than once"
                );
                let slot = state
                    .slots
                    .get(slot_id)
                    .expect("retired generated Wasm slot disappeared");
                assert_eq!(
                    slot.status,
                    SlotStatus::Idle,
                    "retired generated Wasm slot was not idle"
                );
            }
            let candidates = idle_slots_oldest_first
                .iter()
                .map(|(slot_id, _)| {
                    state
                        .slots
                        .get_mut(slot_id)
                        .expect("validated retired generated Wasm slot disappeared")
                        .status = SlotStatus::Evicting;
                    (*slot_id, reason)
                })
                .collect::<Vec<_>>();
            if !candidates.is_empty() {
                self.log_state_metrics_locked(&state);
            }
            return candidates;
        }
        let pressure = matches!(state.pressure, BackendPressure::Active { .. });
        let mut candidates = Vec::new();
        let mut remaining_idle = state
            .slots
            .values()
            .filter(|slot| slot.status == SlotStatus::Idle)
            .count();
        for (slot_id, idle_age) in idle_slots_oldest_first {
            let slots_after_selected = state.slots.len().saturating_sub(candidates.len());
            let Some(slot) = state.slots.get_mut(slot_id) else {
                continue;
            };
            if slot.status != SlotStatus::Idle {
                continue;
            }
            let reason = if pressure {
                Some(IdleEvictionReason::MemoryPressure)
            } else if trigger == IdleEvictionTrigger::AdmissionBudget {
                Some(IdleEvictionReason::AdmissionBudget)
            } else if !protected_from_age.contains(slot_id)
                && *idle_age >= self.policy.maximum_idle_age
                && remaining_idle > self.policy.warm_idle_target
            {
                Some(IdleEvictionReason::IdleAge)
            } else if trigger == IdleEvictionTrigger::NewSlot
                && slots_after_selected >= self.policy.hard_instance_ceiling
            {
                Some(IdleEvictionReason::InstanceCeiling)
            } else {
                None
            };
            let Some(reason) = reason else {
                continue;
            };
            slot.status = SlotStatus::Evicting;
            candidates.push((*slot_id, reason));
            remaining_idle = remaining_idle.saturating_sub(1);
            if matches!(
                reason,
                IdleEvictionReason::InstanceCeiling | IdleEvictionReason::AdmissionBudget
            ) && !pressure
            {
                break;
            }
        }
        if !candidates.is_empty() {
            self.log_state_metrics_locked(&state);
        }
        candidates
    }

    pub(crate) fn finish_idle_eviction(
        &self,
        slot_id: GeneratedSlotId,
        reason: IdleEvictionReason,
    ) {
        let mut state = self.state.lock();
        let slot = state
            .slots
            .remove(&slot_id)
            .expect("evicted generated Wasm slot disappeared");
        assert_eq!(
            slot.status,
            SlotStatus::Evicting,
            "evicted generated Wasm slot was not marked for eviction"
        );
        self.log_state_metrics_locked(&state);
        drop(state);
        log_memory_event(reason.metric_name());
        self.changed.notify_waiters();
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn mark_idle_slot_for_eviction(
        &self,
        slot_id: GeneratedSlotId,
        _reason: IdleEvictionReason,
    ) {
        let mut state = self.state.lock();
        let slot = state
            .slots
            .get_mut(&slot_id)
            .expect("generated Wasm cleanup slot disappeared");
        assert_eq!(
            slot.status,
            SlotStatus::Idle,
            "generated Wasm cleanup slot was not idle"
        );
        slot.status = SlotStatus::Evicting;
        self.log_state_metrics_locked(&state);
    }

    fn log_policy_metrics(&self) {
        for (setting, value) in [
            ("hard_instance_ceiling", self.policy.hard_instance_ceiling),
            ("warm_idle_target", self.policy.warm_idle_target),
        ] {
            log_gauge_with_labels(
                &GENERATED_WASM_MEMORY_INSTANCE_POLICY_TOTAL,
                value as f64,
                vec![StaticMetricLabel::new("setting", setting)],
            );
        }
    }

    fn log_state_metrics_locked(&self, state: &ControllerState) {
        let shared_buffers = self.shared_buffers.bytes();
        let aggregate = self.aggregate_metrics_locked(state, shared_buffers);
        for (name, value) in [
            ("active", aggregate.active_instances),
            ("idle", aggregate.idle_instances),
            ("evicting", aggregate.evicting_instances),
        ] {
            log_gauge_with_labels(
                &GENERATED_WASM_MEMORY_INSTANCES_INFO,
                value as f64,
                vec![StaticMetricLabel::new("state", name)],
            );
        }
        for (component, bytes) in [
            ("projected", aggregate.projected_bytes),
            ("modules", state.fixed_module_bytes),
            ("shared_packed_buffers", shared_buffers),
            ("retained_baseline", aggregate.retained_baseline_bytes),
            (
                "active_checkout_baseline",
                aggregate.active_checkout_baseline_bytes,
            ),
            ("active_guest", aggregate.active_guest_bytes),
            (
                "shared_static_discount",
                state
                    .slots
                    .values()
                    .filter(|slot| !matches!(slot.status, SlotStatus::Active { .. }))
                    .map(|slot| slot.shared_static_bytes)
                    .chain(
                        state
                            .active
                            .values()
                            .map(|invocation| invocation.shared_static_bytes),
                    )
                    .fold(0usize, usize::saturating_add),
            ),
            ("active_host_owned", aggregate.active_host_bytes),
            (
                "active_current_growth",
                aggregate.active_current_growth_bytes,
            ),
            ("active_forecast", aggregate.active_forecast_bytes),
            ("active_liability", aggregate.active_liability_bytes),
            (
                "unattributed_allowance",
                aggregate.unattributed_allowance_bytes,
            ),
            (
                "idle_growth_reservation",
                state
                    .slots
                    .values()
                    .filter(|slot| slot.status == SlotStatus::Idle)
                    .fold(0usize, |bytes, slot| {
                        bytes.saturating_add(slot.idle_growth_bytes)
                    }),
            ),
            ("soft_budget", self.policy.soft_budget_bytes),
            ("hard_budget", self.policy.hard_budget_bytes),
            ("safety_reserve", self.policy.safety_reserve_bytes),
        ] {
            log_gauge_with_labels(
                &GENERATED_WASM_MEMORY_BYTES,
                bytes as f64,
                vec![StaticMetricLabel::new("component", component)],
            );
        }
    }

    fn aggregate_metrics_locked(
        &self,
        state: &ControllerState,
        shared_buffers: usize,
    ) -> AggregateMemoryMetrics {
        let idle_instances = state
            .slots
            .values()
            .filter(|slot| slot.status == SlotStatus::Idle)
            .count();
        let evicting_instances = state
            .slots
            .values()
            .filter(|slot| slot.status == SlotStatus::Evicting)
            .count();
        let retained_baseline_bytes = state.slots.values().fold(0usize, |total, slot| {
            total.saturating_add(slot.retained_baseline())
        });
        let active_checkout_baseline_bytes =
            state.active.values().fold(0usize, |total, invocation| {
                total.saturating_add(invocation.checkout_baseline_bytes)
            });
        let active_guest_bytes = state.active.values().fold(0usize, |total, invocation| {
            total.saturating_add(invocation.current_guest_bytes)
        });
        let active_host_bytes = state.active.values().fold(0usize, |total, invocation| {
            total.saturating_add(invocation.current_host_bytes)
        });
        let active_current_growth_bytes =
            state.active.values().fold(0usize, |total, invocation| {
                total.saturating_add(
                    invocation
                        .current_guest_bytes
                        .checked_sub(invocation.shared_static_bytes)
                        .expect("shared static bytes exceed active guest capacity")
                        .saturating_add(invocation.current_host_bytes)
                        .saturating_sub(invocation.checkout_baseline_bytes),
                )
            });
        let active_forecast_bytes = state.active.values().fold(0usize, |total, invocation| {
            total.saturating_add(invocation.forecast_bytes)
        });
        let active_liability_bytes = state.active.values().fold(0usize, |total, invocation| {
            total
                .saturating_add(invocation.liability_bytes)
                .saturating_add(invocation.preparation_headroom_bytes)
        });
        let unattributed_allowance_bytes = state
            .slots
            .len()
            .saturating_mul(self.policy.unattributed_bytes_per_slot);
        AggregateMemoryMetrics {
            active_instances: state.active.len(),
            idle_instances,
            evicting_instances,
            retained_baseline_bytes,
            active_checkout_baseline_bytes,
            active_guest_bytes,
            active_host_bytes,
            active_current_growth_bytes,
            active_forecast_bytes,
            active_liability_bytes,
            unattributed_allowance_bytes,
            projected_bytes: self
                .checked_projection_with_shared_buffers(state, shared_buffers)
                .unwrap_or(usize::MAX),
        }
    }
}

pub(crate) struct GeneratedModuleMemoryCharge {
    controller: Arc<GeneratedMemoryController>,
    fixed_bytes: usize,
    charge_id: u64,
}

impl GeneratedModuleMemoryCharge {
    pub(crate) fn fixed_bytes(&self) -> usize {
        self.fixed_bytes
    }

    pub(crate) fn reclaim_bytes_needed(&self, fixed_bytes: usize) -> Option<usize> {
        self.controller
            .module_reclaim_bytes_needed(self.fixed_bytes, fixed_bytes)
    }

    pub(crate) fn try_resize(
        &mut self,
        fixed_bytes: usize,
    ) -> Result<(), ModuleMemoryAdmissionError> {
        self.controller
            .resize_module_charge(self.charge_id, self.fixed_bytes, fixed_bytes)?;
        self.fixed_bytes = fixed_bytes;
        Ok(())
    }
}

impl Drop for GeneratedModuleMemoryCharge {
    fn drop(&mut self) {
        self.controller
            .release_module_charge(self.charge_id, self.fixed_bytes);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TryAdmissionError {
    Wait(AdmissionWaitReason),
    Permanent(AdmissionRejection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionWaitReason {
    Pressure,
    ActiveCountCeiling,
    InstanceCountCeiling,
    SlotUnavailable,
    SoftBudget,
    HardBudget,
    FunctionRecordCapacity,
}

impl AdmissionWaitReason {
    fn metric_name(self) -> &'static str {
        match self {
            Self::Pressure => "admission_wait_pressure",
            Self::ActiveCountCeiling => "admission_wait_active_count",
            Self::InstanceCountCeiling => "admission_wait_instance_count",
            Self::SlotUnavailable => "admission_wait_slot",
            Self::SoftBudget => "admission_wait_soft_budget",
            Self::HardBudget => "admission_wait_hard_budget",
            Self::FunctionRecordCapacity => "admission_wait_function_records",
        }
    }
}

pub(crate) struct InvocationMemoryPermit {
    account: Arc<InvocationMemoryAccount>,
    observation: Option<WasmMemoryObserver>,
    observation_started: bool,
    reused_instance: bool,
    reuse_enabled: bool,
    route_context: Option<GeneratedMemoryRouteContext>,
    route_statistics_configured: bool,
}

impl InvocationMemoryPermit {
    pub(crate) fn configure_route_statistics(
        &mut self,
        route_context: Option<GeneratedMemoryRouteContext>,
        reuse_enabled: bool,
    ) {
        assert!(
            !self.route_statistics_configured,
            "generated Wasm memory route statistics configured twice"
        );
        self.route_statistics_configured = true;
        self.route_context = route_context;
        self.reuse_enabled = reuse_enabled;
    }

    pub(crate) fn observe_completion(&mut self, observer: WasmMemoryObserver) {
        assert!(
            self.observation.is_none(),
            "memory completion observer attached twice"
        );
        self.observation = Some(observer);
    }

    /// Mark the point where the invocation first enters Wasmtime. Setup that
    /// fails before this boundary must not publish an invented zero sample.
    /// Record actual Store reuse here because checkout can reject a stale
    /// pooled Store and replace it before execution.
    pub(crate) fn mark_execution_started(&mut self, reused_instance: bool) {
        assert!(
            !self.observation_started,
            "generated Wasm memory execution started twice"
        );
        self.reused_instance = reused_instance;
        self.observation_started = true;
    }

    fn publish(&self, observation: WasmMemoryObservation) {
        if !self.observation_started {
            return;
        }
        if let Some(observer) = &self.observation {
            observer(observation);
        }
    }

    pub(crate) fn slot_id(&self) -> GeneratedSlotId {
        self.account.slot_id
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn controller(&self) -> &Arc<GeneratedMemoryController> {
        &self.account.controller
    }

    pub(crate) fn observer(&self) -> Arc<dyn HostOwnedByteObserver> {
        self.account.clone()
    }

    pub(crate) fn reserve_idle_growth(&self) -> bool {
        self.account
            .controller
            .reserve_idle_growth(self.account.invocation_id)
    }

    pub(crate) fn preparation_memory_cost(&self) -> PreparationMemoryCost {
        let controller = &self.account.controller;
        let state = controller.state.lock();
        let invocation = state
            .active
            .get(&self.account.invocation_id)
            .expect("preparation cost lost its active invocation");
        // Keep admitted capacity separate from forecasts, which may change
        // after warm handler calls or before this group needs reconstruction.
        PreparationMemoryCost {
            peak_bytes: invocation.peak_admitted_bytes,
            retained_bytes: invocation
                .current_guest_bytes
                .checked_sub(invocation.shared_static_bytes)
                .expect("shared static bytes exceed preparation capacity")
                .saturating_add(invocation.current_host_bytes),
        }
    }

    pub(crate) fn begin_static_memory_instantiation(&self) -> anyhow::Result<()> {
        let mut state = self.account.controller.state.lock();
        let invocation = state
            .active
            .get_mut(&self.account.invocation_id)
            .context("static memory instantiation lost its active invocation")?;
        anyhow::ensure!(
            invocation.cold_start
                && invocation.checkout_baseline_bytes == 0
                && invocation.shared_static_bytes == 0
                && invocation.peaks_before_static_memory.is_none(),
            "static memory instantiation requires a fresh memory owner"
        );
        // Earlier dependencies may already have allocated a GC heap and host
        // buffers. Their combined peaks precede the shared pages and must not
        // receive the later credit. Capture once, just before the owner starts.
        invocation.peaks_before_static_memory = Some(PreStaticMemoryPeaks {
            requested_bytes: invocation.peak_accounted_bytes,
            admitted_bytes: invocation.peak_admitted_bytes,
        });
        Ok(())
    }

    /// Install a newly verified COW credit, or confirm the same credit applied
    /// during initial allocation using retained Module evidence. Raw capacity
    /// and per-Store limits remain unchanged. The Module owns the backing
    /// charge.
    pub(crate) fn credit_shared_static(&self, bytes: usize) -> anyhow::Result<()> {
        let controller = &self.account.controller;
        let mut state = controller.state.lock();
        let invocation = state
            .active
            .get_mut(&self.account.invocation_id)
            .context("immutable static credit lost its active invocation")?;
        anyhow::ensure!(
            invocation.cold_start
                && invocation.checkout_baseline_bytes == 0
                && bytes <= invocation.current_guest_bytes,
            "immutable static credit requires a fresh Store with sufficient capacity"
        );
        if invocation.shared_static_bytes != 0 {
            anyhow::ensure!(
                invocation.shared_static_bytes == bytes,
                "instantiated static backing differs from initial admission credit"
            );
            invocation.peaks_before_static_memory = None;
            return Ok(());
        }
        if bytes == 0 {
            invocation.peaks_before_static_memory = None;
            return Ok(());
        }
        let before = invocation
            .peaks_before_static_memory
            .take()
            .context("static memory credit is missing its instantiation boundary")?;
        invocation.shared_static_bytes = bytes;
        // Only the population after the owner's allocation contained these
        // shared pages. Preserve both earlier peaks, including simultaneous
        // host and GC capacity, rather than clamping to host-only demand.
        invocation.peak_accounted_bytes = invocation
            .peak_accounted_bytes
            .saturating_sub(bytes)
            .max(before.requested_bytes);
        invocation.peak_admitted_bytes = invocation
            .peak_admitted_bytes
            .saturating_sub(bytes)
            .max(before.admitted_bytes);
        invocation.liability_bytes = (invocation.current_guest_bytes - bytes)
            .checked_add(invocation.current_host_bytes)
            .context("immutable static admission accounting overflow")?
            .max(invocation.forecast_bytes);
        controller.log_state_metrics_locked(&state);
        drop(state);
        controller.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn current_bytes(&self) -> (usize, usize) {
        (
            self.account.current_guest_bytes.load(Ordering::Acquire),
            self.account.current_host_bytes.load(Ordering::Acquire),
        )
    }

    pub(crate) fn limiter(&self, store_limits: StoreLimits) -> GeneratedStoreLimiter {
        GeneratedStoreLimiter {
            store_limits,
            account: Arc::clone(&self.account),
            growth_failed: false,
            initial_static_memory: None,
        }
    }

    pub(crate) fn finish(self, terminal: TerminalMemoryOutcome, retain_slot: bool) {
        assert!(
            self.observation.is_none() || self.observation_started,
            "observed generated Wasm invocation completed before execution"
        );
        assert!(
            !self.account.finished.swap(true, Ordering::AcqRel),
            "generated Wasm memory permit completed more than once"
        );
        let observation = self.account.controller.complete(
            self.account.invocation_id,
            terminal,
            retain_slot,
            self.reused_instance,
            self.reuse_enabled,
            self.route_context.as_ref(),
        );
        self.publish(observation);
    }

    /// Complete admission before any guest execution begins.
    ///
    /// This returns the memory slot to its prior idle state without recording a
    /// function-memory observation, because the invocation did not run.
    pub(crate) fn finish_unstarted(self, retain_slot: bool) {
        assert!(
            !self.observation_started,
            "started generated Wasm invocation completed as unstarted"
        );
        assert!(
            !self.account.finished.swap(true, Ordering::AcqRel),
            "generated Wasm memory permit completed more than once"
        );
        self.account
            .controller
            .complete_unstarted(self.account.invocation_id, retain_slot);
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn finish_with_snapshot_for_test(
        self,
        terminal: TerminalMemoryOutcome,
        retain_slot: bool,
    ) -> GeneratedMemoryTestSnapshot {
        let controller = Arc::clone(&self.account.controller);
        let identity = self.account.identity.clone();
        self.finish(terminal, retain_slot);
        controller.snapshot_for_test(&identity)
    }
}

impl Drop for InvocationMemoryPermit {
    fn drop(&mut self) {
        if !self.account.finished.swap(true, Ordering::AcqRel) {
            if self.observation_started {
                let observation = self.account.controller.complete(
                    self.account.invocation_id,
                    TerminalMemoryOutcome::SystemError,
                    false,
                    self.reused_instance,
                    self.reuse_enabled,
                    self.route_context.as_ref(),
                );
                self.publish(observation);
            } else {
                self.account
                    .controller
                    .complete_unstarted(self.account.invocation_id, false);
            }
        }
    }
}

struct InvocationMemoryAccount {
    controller: Arc<GeneratedMemoryController>,
    identity: FunctionMemoryIdentity,
    invocation_id: u64,
    slot_id: GeneratedSlotId,
    current_guest_bytes: AtomicUsize,
    current_host_bytes: AtomicUsize,
    finished: AtomicBool,
}

impl InvocationMemoryAccount {
    fn update_guest(&self, desired_bytes: usize, denied_growth_bytes: Option<usize>) -> bool {
        let host_bytes = self.current_host_bytes.load(Ordering::Acquire);
        let accepted = self.controller.update_accounted_bytes(
            self.invocation_id,
            desired_bytes,
            host_bytes,
            denied_growth_bytes,
            false,
            None,
        );
        if accepted {
            self.current_guest_bytes
                .store(desired_bytes, Ordering::Release);
        }
        accepted
    }

    fn update_host(&self, accounting: HostOwnedByteAccounting, allocation_committed: bool) -> bool {
        let host_bytes = accounting
            .current_bytes
            .checked_sub(accounting.shared_packed_bytes)
            .expect("shared packed bytes exceed logical host bytes")
            .saturating_add(accounting.retained_bytes);
        let guest_bytes = self.current_guest_bytes.load(Ordering::Acquire);
        let accepted = self.controller.update_accounted_bytes(
            self.invocation_id,
            guest_bytes,
            host_bytes,
            None,
            allocation_committed,
            None,
        );
        if accepted || allocation_committed {
            self.current_host_bytes.store(host_bytes, Ordering::Release);
        }
        accepted
    }
}

impl HostOwnedByteObserver for InvocationMemoryAccount {
    fn host_owned_bytes_changing(&self, accounting: HostOwnedByteAccounting) -> bool {
        self.update_host(accounting, false)
    }

    fn host_owned_bytes_changed(&self, accounting: HostOwnedByteAccounting) -> bool {
        self.update_host(accounting, true)
    }
}

pub(crate) struct GeneratedStoreLimiter {
    store_limits: StoreLimits,
    account: Arc<InvocationMemoryAccount>,
    pub(crate) growth_failed: bool,
    initial_static_memory: Option<(usize, usize)>,
}

#[derive(Debug, thiserror::Error)]
#[error("generated Wasm guest memory limit exceeded")]
pub(crate) struct GeneratedGuestMemoryLimit;

impl GeneratedStoreLimiter {
    pub(crate) fn ensure_memory_available(&self) -> wasmtime::Result<()> {
        // Wasmtime may collect and retry a GC allocation after swallowing the
        // limiter error. Recovery of that allocation does not restore this
        // invocation's authority to admit host work or enter preparation or
        // handler execution.
        if self.growth_failed {
            return Err(wasmtime::Error::new(GeneratedGuestMemoryLimit));
        }
        Ok(())
    }

    /// The caller has retained evidence from an actual mapping of the same
    /// Module and is about to instantiate its sole defined linear memory.
    /// Arming this does not reduce liability; the first allocation must pass
    /// both raw limits and aggregate admission before the credit takes effect.
    pub(crate) fn prepare_initial_static_memory(&mut self, initial: usize, shared: usize) {
        assert!(
            self.initial_static_memory.is_none()
                && self.account.current_guest_bytes.load(Ordering::Acquire) == 0
                && initial > 0
                && shared <= initial,
            "invalid initial static memory admission"
        );
        self.initial_static_memory = Some((initial, shared));
    }

    pub(crate) fn reconcile(
        &mut self,
        linear_bytes: usize,
        gc_heap_bytes: usize,
    ) -> anyhow::Result<()> {
        let actual = linear_bytes
            .checked_add(gc_heap_bytes)
            .context("generated Wasm memory capacity overflow")?;
        let accounted = self.account.current_guest_bytes.load(Ordering::Acquire);
        if actual == accounted {
            return Ok(());
        }
        anyhow::ensure!(
            self.growth_failed && actual < accounted,
            "generated Wasm memory accounting differs from Store capacities"
        );
        anyhow::ensure!(
            self.account.update_guest(actual, None),
            "generated Wasm memory reconciliation was rejected"
        );
        Ok(())
    }
}

impl ResourceLimiter for GeneratedStoreLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let total = self.account.current_guest_bytes.load(Ordering::Acquire);
        // Wasmtime uses this callback for linear memories and its internal GC
        // heap. `current` and `desired` describe only the allocation growing.
        let growth = desired
            .checked_sub(current)
            .expect("Wasm memory growth shrank");
        let Some(proposed) = total.checked_add(growth) else {
            self.growth_failed = true;
            self.account.update_guest(total, Some(usize::MAX));
            return Err(wasmtime::Error::new(GeneratedGuestMemoryLimit));
        };
        if self.growth_failed {
            return Err(wasmtime::Error::new(GeneratedGuestMemoryLimit));
        }
        let admitted = self
            .store_limits
            .memory_growing(current, desired, maximum)
            .and_then(|allowed| {
                if allowed {
                    self.store_limits.memory_growing(total, proposed, None)
                } else {
                    Ok(false)
                }
            });
        match admitted {
            Ok(true) => {},
            Ok(false) => {
                self.growth_failed = true;
                self.account.update_guest(total, Some(proposed));
                return Err(wasmtime::Error::new(GeneratedGuestMemoryLimit));
            },
            Err(error) => {
                self.growth_failed = true;
                self.account.update_guest(total, Some(proposed));
                return Err(error.context(GeneratedGuestMemoryLimit));
            },
        }
        let accepted = match self.initial_static_memory.take() {
            Some((initial, shared)) => {
                // Wasmtime allocates defined linear memories before tables,
                // globals or guest startup. Never let a different allocation
                // consume evidence intended for that initial linear memory.
                if current != 0 || total != 0 || desired != initial {
                    self.growth_failed = true;
                    return Err(wasmtime::Error::msg(
                        "initial static credit reached an unexpected allocation",
                    ));
                }
                let accepted = self.account.controller.update_accounted_bytes(
                    self.account.invocation_id,
                    proposed,
                    self.account.current_host_bytes.load(Ordering::Acquire),
                    None,
                    false,
                    Some(shared),
                );
                if accepted {
                    self.account
                        .current_guest_bytes
                        .store(proposed, Ordering::Release);
                }
                accepted
            },
            None => self.account.update_guest(proposed, None),
        };
        if !accepted {
            self.growth_failed = true;
            return Err(wasmtime::Error::new(GeneratedGuestMemoryLimit));
        }
        Ok(true)
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        // A type-limit rejection can arrive without memory_growing. Rolling
        // back the previous callback would then release successful growth.
        // Fail the invocation and keep its reservation until actual capacities
        // are reconciled or the Store is destroyed.
        self.growth_failed = true;
        let total = self.account.current_guest_bytes.load(Ordering::Acquire);
        self.account.update_guest(total, Some(total));
        Err(error.context(GeneratedGuestMemoryLimit))
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.store_limits.table_growing(current, desired, maximum)
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.store_limits.table_grow_failed(error)
    }

    fn instances(&self) -> usize {
        self.store_limits.instances()
    }

    fn tables(&self) -> usize {
        self.store_limits.tables()
    }

    fn memories(&self) -> usize {
        self.store_limits.memories()
    }
}

fn signed_difference(current: usize, baseline: usize) -> i64 {
    if current >= baseline {
        i64::try_from(current - baseline).unwrap_or(i64::MAX)
    } else {
        -i64::try_from(baseline - current).unwrap_or(i64::MAX)
    }
}

fn read_cgroup_headroom(root: &Path) -> anyhow::Result<Option<usize>> {
    let current_path = root.join("memory.current");
    let maximum_path = root.join("memory.max");
    let current = match fs::read_to_string(&current_path) {
        Ok(current) => current,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let maximum = match fs::read_to_string(&maximum_path) {
        Ok(maximum) => maximum,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let maximum = maximum.trim();
    if maximum == "max" {
        return Ok(None);
    }
    let current = current.trim().parse::<usize>()?;
    let maximum = maximum.parse::<usize>()?;
    Ok(Some(maximum.saturating_sub(current)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Isolate these policy tests from packed allocations made by other tests.
    fn test_controller(
        policy: GeneratedMemoryPolicy,
    ) -> anyhow::Result<Arc<GeneratedMemoryController>> {
        static BUFFERS: BufferMemory = BufferMemory::new();
        GeneratedMemoryController::with_shared_buffers(policy, &BUFFERS)
    }

    const MIB: usize = 1024 * 1024;

    #[tokio::test]
    async fn shared_buffer_release_wakes_admission_after_cache_eviction() {
        static BUFFERS: BufferMemory = BufferMemory::new();
        let mut limits = policy();
        limits.soft_budget_bytes = 1024;
        limits.hard_budget_bytes = 1536;
        limits.safety_reserve_bytes = 64;
        limits.cold_peak_growth_bytes = 64;
        limits.unattributed_bytes_per_slot = 16;
        let controller = GeneratedMemoryController::with_shared_buffers(limits, &BUFFERS).unwrap();
        let function = identity("shared-buffers", "a", "read");
        let cache = BUFFERS.track(vec![0; 900]);
        let reader = cache.clone();
        assert_eq!(controller.snapshot_for_test(&function).projected_bytes, 964);
        drop(cache);
        let admission = controller.admit(function.clone(), None);
        tokio::pin!(admission);
        assert!(futures::poll!(&mut admission).is_pending());
        // No invocation completes here: the backing's last external reader is
        // sufficient to make this admission feasible and wake it.
        std::thread::spawn(move || drop(reader)).join().unwrap();
        let admitted = tokio::time::timeout(Duration::from_secs(1), admission)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(controller.snapshot_for_test(&function).projected_bytes, 144);

        let backing = BUFFERS.track(vec![0; 128]);
        assert!(admitted
            .account
            .host_owned_bytes_changed(HostOwnedByteAccounting {
                current_bytes: 256 + 8,
                shared_packed_bytes: 256,
                ..Default::default()
            }));
        let snapshot = controller.snapshot_for_test(&function);
        assert_eq!(snapshot.active_host_bytes, 8);
        assert_eq!(snapshot.projected_bytes, 144 + 128);
        drop(backing);
        drop(admitted);
        assert_eq!(BUFFERS.bytes(), 0);
    }

    #[test]
    fn shared_buffers_participate_in_hard_growth_admission() {
        static BUFFERS: BufferMemory = BufferMemory::new();
        let mut limits = policy();
        limits.soft_budget_bytes = 1024;
        limits.hard_budget_bytes = 1536;
        limits.safety_reserve_bytes = 64;
        limits.cold_peak_growth_bytes = 64;
        limits.unattributed_bytes_per_slot = 16;
        let controller = GeneratedMemoryController::with_shared_buffers(limits, &BUFFERS).unwrap();
        let permit = controller
            .try_admit(identity("shared-growth", "a", "read"), None)
            .unwrap();
        let backing = BUFFERS.track(vec![0; 1400]);
        assert!(!permit.account.update_guest(100, None));
        drop(backing);
        drop(permit);
        assert_eq!(BUFFERS.bytes(), 0);
    }

    #[test]
    fn external_backing_growth_cannot_reject_releases_or_hide_committed_host_bytes() {
        static BUFFERS: BufferMemory = BufferMemory::new();
        let controller = GeneratedMemoryController::with_shared_buffers(
            GeneratedMemoryPolicy {
                soft_budget_bytes: 512,
                hard_budget_bytes: 640,
                safety_reserve_bytes: 64,
                cold_peak_growth_bytes: 32,
                unattributed_bytes_per_slot: 1,
                ..policy()
            },
            &BUFFERS,
        )
        .unwrap();
        let function = identity("external-growth", "a", "read");
        let permit = controller.try_admit(function.clone(), None).unwrap();
        let host = |bytes| HostOwnedByteAccounting {
            current_bytes: bytes,
            ..Default::default()
        };
        assert!(permit.account.host_owned_bytes_changing(host(80)));
        let backing = BUFFERS.track(vec![0; 600]);
        assert!(permit.account.host_owned_bytes_changed(host(80)));
        assert!(!permit.account.host_owned_bytes_changed(host(82)));
        assert_eq!(permit.current_bytes().1, 82);
        assert_eq!(
            controller.snapshot_for_test(&function).active_host_bytes,
            82
        );
        assert!(permit.account.host_owned_bytes_changed(host(81)));
        assert_eq!(
            controller.state.lock().active[&permit.account.invocation_id].peak_admitted_bytes,
            80,
        );
        assert!(permit.account.host_owned_bytes_changed(host(0)));
        assert_eq!(controller.snapshot_for_test(&function).active_host_bytes, 0);
        drop(backing);
        drop(permit);
    }

    #[test]
    fn external_backing_growth_preserves_admitted_idle_reservations() {
        static BUFFERS: BufferMemory = BufferMemory::new();
        let controller = GeneratedMemoryController::with_shared_buffers(
            GeneratedMemoryPolicy {
                soft_budget_bytes: 512,
                hard_budget_bytes: 640,
                safety_reserve_bytes: 64,
                cold_peak_growth_bytes: 32,
                unattributed_bytes_per_slot: 1,
                ..policy()
            },
            &BUFFERS,
        )
        .unwrap();
        let function = identity("external-return", "a", "read");
        let prepared = controller.try_admit_preparation(function.clone()).unwrap();
        assert!(prepared.account.update_guest(16, None));
        assert!(prepared.reserve_idle_growth());
        let slot = prepared.slot_id();
        prepared.finish(TerminalMemoryOutcome::Success, true);

        let warm = controller.try_admit(function.clone(), Some(slot)).unwrap();
        let backing = BUFFERS.track(vec![0; 400]);
        let before_return = controller.snapshot_for_test(&function).projected_bytes;
        assert_eq!(before_return, 513);
        assert!(warm.reserve_idle_growth());
        warm.finish(TerminalMemoryOutcome::Success, true);
        assert_eq!(
            controller.snapshot_for_test(&function).projected_bytes,
            before_return,
        );
        assert_eq!(controller.snapshot_for_test(&function).idle_instances, 1);

        drop(backing);
        let growing = controller.try_admit(function.clone(), Some(slot)).unwrap();
        assert!(growing.account.update_guest(17, None));
        let backing = BUFFERS.track(vec![0; 400]);
        // Retaining the extra capacity plus the next forecast is an increase
        // over the active liability and still requires soft-budget admission.
        assert!(!growing.reserve_idle_growth());
        drop(backing);
        growing.finish(TerminalMemoryOutcome::Success, false);
    }

    #[test]
    fn failed_allocation_reconciles_under_external_pressure() -> anyhow::Result<()> {
        static BUFFERS: BufferMemory = BufferMemory::new();
        let controller = GeneratedMemoryController::with_shared_buffers(
            GeneratedMemoryPolicy {
                soft_budget_bytes: 512,
                hard_budget_bytes: 640,
                safety_reserve_bytes: 64,
                cold_peak_growth_bytes: 32,
                unattributed_bytes_per_slot: 1,
                ..policy()
            },
            &BUFFERS,
        )?;
        let permit = controller
            .try_admit(identity("reconcile", "a", "read"), None)
            .unwrap();
        let mut limiter = permit.limiter(wasmtime::StoreLimitsBuilder::new().build());
        limiter.memory_growing(0, 1, None)?;
        limiter.memory_growing(1, 2, None)?;
        assert!(limiter
            .memory_grow_failed(wasmtime::Error::msg("allocation failed"))
            .is_err());
        let backing = BUFFERS.track(vec![0; 640]);
        limiter.reconcile(1, 0)?;
        assert_eq!(permit.current_bytes().0, 1);
        drop(backing);
        drop(permit);
        Ok(())
    }

    #[test]
    fn truncated_warm_executions_do_not_train_down_the_forecast() {
        let controller = test_controller(policy()).unwrap();
        let function = identity("truncated", "a", "read");
        for _ in 0..MINIMUM_USEFUL_SAMPLES {
            let cold = controller.try_admit(function.clone(), None).unwrap();
            let slot = cold.slot_id();
            assert!(cold.account.update_guest(MIB, None));
            cold.finish(TerminalMemoryOutcome::Success, true);
            let warm = controller.try_admit(function.clone(), Some(slot)).unwrap();
            warm.finish(TerminalMemoryOutcome::Cancellation, false);
        }
        let state = controller.state.lock();
        assert_eq!(state.functions[&function].warm_sample_count, 0);
        assert_eq!(state.functions[&function].forecast(32 * MIB), 32 * MIB);
    }

    #[test]
    fn truncated_initializations_do_not_erase_a_learned_cold_requirement() {
        let controller = test_controller(policy()).unwrap();
        let function = identity("truncated-initialization", "a", "read");
        let cold = controller.try_admit(function.clone(), None).unwrap();
        assert!(cold.account.update_guest(96 * MIB, None));
        cold.finish(TerminalMemoryOutcome::Success, false);
        for _ in 0..200 {
            let interrupted = controller.try_admit(function.clone(), None).unwrap();
            assert!(interrupted.account.update_guest(16 * MIB, None));
            interrupted.finish(TerminalMemoryOutcome::Cancellation, false);
        }
        let state = controller.state.lock();
        let stats = &state.functions[&function];
        assert_eq!(stats.preparation_peak_growth.samples, 1);
        assert_eq!(stats.preparation_forecast(32 * MIB), 112 * MIB);
        assert_eq!(stats.sample_count, 201);
    }

    #[test]
    fn external_backing_does_not_trigger_futile_invocation_eviction() {
        static BUFFERS: BufferMemory = BufferMemory::new();
        let controller = GeneratedMemoryController::with_shared_buffers(
            GeneratedMemoryPolicy {
                soft_budget_bytes: 512,
                hard_budget_bytes: 640,
                safety_reserve_bytes: 64,
                cold_peak_growth_bytes: 32,
                unattributed_bytes_per_slot: 1,
                ..policy()
            },
            &BUFFERS,
        )
        .unwrap();
        let function = identity("retained", "a", "read");
        let permit = controller.try_admit(function.clone(), None).unwrap();
        let slot = permit.slot_id();
        assert!(permit.account.update_guest(8, None));
        assert!(permit.reserve_idle_growth());
        permit.finish(TerminalMemoryOutcome::Success, true);
        let backing = BUFFERS.track(vec![0; 480]);
        assert!(controller
            .invocation_eviction_candidates(&function, None, &[(slot, Duration::ZERO)])
            .is_empty());
        assert_eq!(controller.snapshot_for_test(&function).idle_instances, 1);
        drop(backing);
        controller
            .try_admit(function, Some(slot))
            .unwrap()
            .finish_unstarted(false);
    }

    fn route_context(
        route_byte: u8,
        role: GeneratedMemoryExecutionRole,
    ) -> GeneratedMemoryRouteContext {
        GeneratedMemoryRouteContext {
            identity: GeneratedMemoryRouteIdentity {
                generation_sha256: "a".repeat(64),
                route_sha256: format!("{route_byte:064x}"),
                udf_kind: UdfKind::Query,
            },
            role,
        }
    }

    fn memory_observation(
        bytes: usize,
        reused_instance: bool,
        terminal: WasmMemoryTerminal,
    ) -> WasmMemoryObservation {
        WasmMemoryObservation {
            reused_instance,
            checkout_baseline_bytes: u64::try_from(bytes / 2).unwrap(),
            peak_admitted_bytes: u64::try_from(bytes).unwrap(),
            peak_guest_bytes: u64::try_from(bytes.saturating_sub(3)).unwrap(),
            peak_host_bytes: 3,
            requested_peak_bytes: u64::try_from(bytes.saturating_add(7)).unwrap(),
            forecast_growth_bytes: 11,
            completion_bytes: u64::try_from(bytes.saturating_sub(1)).unwrap(),
            returned_to_pool_bytes: u64::try_from(bytes.saturating_sub(1)).unwrap(),
            returned_to_pool: true,
            forecast_overrun: bytes >= 1_000,
            growth_denied: terminal == WasmMemoryTerminal::MemoryLimit,
            successful_execution_discarded: false,
            terminal,
        }
    }

    fn record_route_observation(
        controller: &GeneratedMemoryController,
        context: &GeneratedMemoryRouteContext,
        observation: &WasmMemoryObservation,
    ) -> bool {
        let mut state = controller.state.lock();
        state.clock += 1;
        let clock = state.clock;
        GeneratedMemoryController::record_route_statistics_locked(
            &mut state,
            context,
            &identity("observed", "a", "observed"),
            observation,
            clock,
        )
    }

    #[test]
    fn route_statistics_cover_started_executions_but_not_unstarted_admissions() {
        let controller = test_controller(policy()).unwrap();
        let context = route_context(1, GeneratedMemoryExecutionRole::Shadow);
        let mut started = controller
            .try_admit(identity("started", "a", "started"), None)
            .unwrap();
        started.configure_route_statistics(Some(context.clone()), true);
        started.mark_execution_started(false);
        assert!(started.account.update_guest(8 * MIB, None));
        started.finish(TerminalMemoryOutcome::Success, true);

        let mut unstarted = controller
            .try_admit(identity("unstarted-statistics", "a", "unstarted"), None)
            .unwrap();
        unstarted.configure_route_statistics(Some(context), true);
        unstarted.finish_unstarted(false);

        let statistics = controller.statistics_for_routes(
            &"a".repeat(64),
            UdfKind::Query,
            &[format!("{:064x}", 1)],
        );
        assert_eq!(statistics.observed_route_count, 1);
        let shadow = statistics.routes[0].shadow.as_ref().unwrap();
        assert_eq!(shadow.observation_count, 1);
        assert_eq!(shadow.terminal_counts.success, 1);
        assert_eq!(shadow.peak_guest_bytes.maximum, 8 * MIB as u64);
    }

    #[test]
    fn route_memory_identity_change_starts_a_new_population() {
        let controller = test_controller(policy()).unwrap();
        let context = route_context(1, GeneratedMemoryExecutionRole::Shadow);
        record_route_observation(
            &controller,
            &context,
            &memory_observation(100, true, WasmMemoryTerminal::Success),
        );
        let changed_identity = identity("replacement", "b", "observed");
        {
            let mut state = controller.state.lock();
            let clock = state.clock + 1;
            assert!(GeneratedMemoryController::record_route_statistics_locked(
                &mut state,
                &context,
                &changed_identity,
                &memory_observation(10, false, WasmMemoryTerminal::Success),
                clock,
            ));
            assert_eq!(
                state.routes[&context.identity].memory_identity.as_ref(),
                Some(&changed_identity)
            );
        }
        let statistics = controller.statistics_for_routes(
            &"a".repeat(64),
            UdfKind::Query,
            &[format!("{:064x}", 1)],
        );
        assert_eq!(statistics.route_record_eviction_count, 1);
        let shadow = statistics.routes[0].shadow.as_ref().unwrap();
        assert_eq!(shadow.observation_count, 1);
        assert_eq!(shadow.peak_admitted_bytes.maximum, 10);
    }

    #[test]
    fn memory_inspection_reconciles_live_charges_and_retained_route_histograms() {
        let controller = test_controller(policy()).unwrap();
        let mut charge = controller.try_charge_module(16 * MIB).unwrap();
        charge.describe("sharedAot", "f".repeat(64), Some(8 * MIB), true);
        let mut permit = controller
            .try_admit(identity("inspected", "a", "read"), None)
            .unwrap();
        permit.describe_modules(vec![charge.inspection_id(), charge.inspection_id()]);
        permit.configure_route_statistics(
            Some(route_context(1, GeneratedMemoryExecutionRole::Shadow)),
            true,
        );
        permit.mark_execution_started(false);
        assert!(permit.account.update_guest(8 * MIB, None));
        let snapshot = || {
            serde_json::to_value(controller.inspect(
                Some(&"a".repeat(64)),
                UdfKind::Query,
                &[format!("{:064x}", 1)],
                None,
                100,
            ))
            .unwrap()
        };
        let active = snapshot();
        assert_eq!(active["slots"][0]["state"], "active");
        assert_eq!(
            active["slots"][0]["moduleChargeIds"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            active["ledgerBytes"]["projected"].as_u64().unwrap() as usize,
            controller.projected_bytes_locked(&controller.state.lock())
        );
        permit.finish(TerminalMemoryOutcome::Success, true);
        charge.mark_uncached();
        charge.try_resize(20 * MIB).unwrap();
        let idle = snapshot();
        assert_eq!(idle["slots"][0]["state"], "idle");
        assert_eq!(idle["moduleCharges"][0]["cached"], false);
        assert_eq!(idle["moduleCharges"][0]["bytes"], 20 * MIB);
        assert_eq!(idle["ledgerBytes"]["modules"], 20 * MIB);
        let route = &idle["routes"][0];
        assert_eq!(
            route["memoryIdentitySha256"],
            idle["slots"][0]["memoryIdentitySha256"]
        );
        assert_eq!(
            route["shadow"]["distributions"]["peakGuestBytes"]["maximum"],
            8 * MIB
        );
        assert_eq!(
            route["shadow"]["distributions"]["peakGuestBytes"]["counts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .sum::<u64>(),
            1
        );
        // No cache owner remains, but the live charge is still visible. Only
        // its destruction releases the inventory and the global byte total.
        drop(charge);
        assert_eq!(snapshot()["moduleChargeCount"], 0);
    }

    #[test]
    fn route_statistics_are_cumulative_and_preserve_roles_reuse_and_exact_maxima() {
        let controller = test_controller(policy()).unwrap();
        let shadow = route_context(2, GeneratedMemoryExecutionRole::Shadow);
        for index in 0..100 {
            let bytes = if index < 10 { 1_000 } else { 10 };
            record_route_observation(
                &controller,
                &shadow,
                &memory_observation(bytes, index != 0, WasmMemoryTerminal::Success),
            );
        }
        let primary = route_context(2, GeneratedMemoryExecutionRole::Primary);
        let mut discarded = memory_observation(2_000, false, WasmMemoryTerminal::Success);
        discarded.returned_to_pool = false;
        discarded.returned_to_pool_bytes = 0;
        discarded.successful_execution_discarded = true;
        record_route_observation(&controller, &primary, &discarded);
        record_route_observation(
            &controller,
            &primary,
            &memory_observation(20, true, WasmMemoryTerminal::MemoryLimit),
        );

        let statistics = controller.statistics_for_routes(
            &"a".repeat(64),
            UdfKind::Query,
            &[format!("{:064x}", 2)],
        );
        assert_eq!(
            statistics.route_record_capacity,
            MAX_ROUTE_STATISTICS_RECORDS
        );
        assert_eq!(statistics.route_record_eviction_count, 0);
        let route = &statistics.routes[0];
        let shadow = route.shadow.as_ref().unwrap();
        assert_eq!(shadow.observation_count, 100);
        assert_eq!(shadow.reused_count, 99);
        assert_eq!(
            shadow.fresh_maximums.as_ref().unwrap().peak_admitted_bytes,
            1_000
        );
        assert_eq!(
            shadow.reused_maximums.as_ref().unwrap().peak_admitted_bytes,
            1_000
        );
        assert_eq!(shadow.peak_admitted_bytes.maximum, 1_000);
        assert_eq!(
            shadow.peak_admitted_bytes.p99,
            logarithmic_bucket_upper_bound(logarithmic_bucket(1_000)) as u64
        );
        let primary = route.primary.as_ref().unwrap();
        assert_eq!(primary.observation_count, 2);
        assert_eq!(primary.reused_count, 1);
        assert_eq!(primary.successful_execution_discarded_count, 1);
        assert_eq!(primary.terminal_counts.success, 1);
        assert_eq!(primary.terminal_counts.memory_limit, 1);
        assert_eq!(route.aggregate.observation_count, 102);
        assert_eq!(route.aggregate.peak_admitted_bytes.maximum, 2_000);
        assert_eq!(statistics.aggregate.as_ref().unwrap(), &route.aggregate);
    }

    #[test]
    fn route_statistics_eviction_is_explicit_and_does_not_consume_admission_records() {
        let controller = test_controller(policy()).unwrap();
        for route in 0..=MAX_ROUTE_STATISTICS_RECORDS {
            let context = GeneratedMemoryRouteContext {
                identity: GeneratedMemoryRouteIdentity {
                    generation_sha256: "a".repeat(64),
                    route_sha256: format!("{route:064x}"),
                    udf_kind: UdfKind::Query,
                },
                role: GeneratedMemoryExecutionRole::Shadow,
            };
            let evicted = record_route_observation(
                &controller,
                &context,
                &memory_observation(10, true, WasmMemoryTerminal::Success),
            );
            assert_eq!(evicted, route == MAX_ROUTE_STATISTICS_RECORDS);
        }
        let statistics = controller.statistics_for_routes(
            &"a".repeat(64),
            UdfKind::Query,
            &[
                format!("{:064x}", 0),
                format!("{:064x}", MAX_ROUTE_STATISTICS_RECORDS),
            ],
        );
        assert_eq!(statistics.selected_route_count, 2);
        assert_eq!(statistics.observed_route_count, 1);
        assert_eq!(statistics.route_record_eviction_count, 1);
        assert_eq!(
            statistics.routes[0].route_sha256,
            format!("{:064x}", MAX_ROUTE_STATISTICS_RECORDS)
        );
        assert!(controller.state.lock().functions.is_empty());
    }

    #[test]
    fn logarithmic_histogram_catch_all_never_understates_an_out_of_range_sample() {
        let mut histogram = LogarithmicHistogram::default();
        histogram.observe(usize::MAX);
        let statistics = histogram.statistics().unwrap();
        assert_eq!(
            statistics.p50,
            u64::try_from(usize::MAX).unwrap_or(u64::MAX)
        );
        assert_eq!(
            statistics.p99,
            u64::try_from(usize::MAX).unwrap_or(u64::MAX)
        );
        assert_eq!(
            statistics.maximum,
            u64::try_from(usize::MAX).unwrap_or(u64::MAX)
        );
    }

    #[test]
    fn memory_observation_separates_admitted_and_denied_peaks_and_warm_baseline() {
        let controller = test_controller(policy()).unwrap();
        let function = identity("observed", "a", "observed");
        let observations = Arc::new(Mutex::new(Vec::new()));
        let capture = Arc::clone(&observations);
        let observer: WasmMemoryObserver = Arc::new(move |value| capture.lock().push(value));
        let mut cold = controller.try_admit(function.clone(), None).unwrap();
        cold.configure_route_statistics(None, true);
        cold.observe_completion(Arc::clone(&observer));
        cold.mark_execution_started(false);
        let slot = cold.slot_id();
        assert!(cold.account.update_guest(48 * MIB, None));
        cold.finish(TerminalMemoryOutcome::Success, true);
        let cold = observations.lock()[0].clone();
        assert!(!cold.reused_instance);
        assert_eq!(cold.checkout_baseline_bytes, 0);
        assert_eq!(cold.peak_admitted_bytes, 48 * MIB as u64);
        assert!(cold.forecast_overrun);
        assert!(cold.returned_to_pool);
        assert!(!cold.successful_execution_discarded);

        // A stale checked-out Store is destroyed before a fresh permit is
        // admitted; its retained capacity cannot become the new cold baseline.
        let stale = controller.try_admit(function.clone(), Some(slot)).unwrap();
        stale.finish_unstarted(false);
        assert_eq!(observations.lock().len(), 1);
        let mut replaced = controller.try_admit(function.clone(), None).unwrap();
        let replacement_slot = replaced.slot_id();
        assert_ne!(replacement_slot, slot);
        replaced.configure_route_statistics(None, true);
        replaced.observe_completion(Arc::clone(&observer));
        replaced.mark_execution_started(false);
        assert!(replaced.account.update_guest(48 * MIB, None));
        replaced.finish(TerminalMemoryOutcome::Success, true);
        let replaced = observations.lock()[1].clone();
        assert!(!replaced.reused_instance);
        assert_eq!(replaced.checkout_baseline_bytes, 0);
        assert_eq!(replaced.peak_admitted_bytes, 48 * MIB as u64);

        let mut warm = controller
            .try_admit(function, Some(replacement_slot))
            .unwrap();
        warm.configure_route_statistics(None, true);
        warm.observe_completion(observer);
        warm.mark_execution_started(true);
        assert!(!warm.account.update_guest(48 * MIB, Some(700 * MIB)));
        warm.finish(TerminalMemoryOutcome::DeveloperError, false);
        let warm = observations.lock()[2].clone();
        assert!(warm.reused_instance);
        assert_eq!(warm.checkout_baseline_bytes, 48 * MIB as u64);
        assert_eq!(warm.peak_admitted_bytes, 48 * MIB as u64);
        assert_eq!(warm.requested_peak_bytes, 700 * MIB as u64);
        assert_eq!(warm.returned_to_pool_bytes, 0);
        assert_eq!(warm.terminal, WasmMemoryTerminal::MemoryLimit);
        assert!(warm.growth_denied);
    }

    #[test]
    fn memory_observation_survives_error_drop_but_excludes_unstarted_admission() {
        let controller = test_controller(policy()).unwrap();
        let observations = Arc::new(Mutex::new(Vec::new()));
        let capture = Arc::clone(&observations);
        let observer: WasmMemoryObserver = Arc::new(move |value| capture.lock().push(value));
        for reuse_enabled in [false, true] {
            let mut permit = controller
                .try_admit(identity("discard", "a", "discard"), None)
                .unwrap();
            permit.configure_route_statistics(None, reuse_enabled);
            permit.observe_completion(Arc::clone(&observer));
            permit.mark_execution_started(false);
            permit.finish(TerminalMemoryOutcome::Success, false);
        }
        assert!(!observations.lock()[0].successful_execution_discarded);
        assert!(observations.lock()[1].successful_execution_discarded);
        let mut permit = controller
            .try_admit(identity("drop", "a", "drop"), None)
            .unwrap();
        permit.configure_route_statistics(None, true);
        permit.observe_completion(Arc::clone(&observer));
        permit.mark_execution_started(false);
        drop(permit);
        assert_eq!(
            observations.lock()[2].terminal,
            WasmMemoryTerminal::SystemError
        );
        let mut permit = controller
            .try_admit(identity("unstarted", "a", "unstarted"), None)
            .unwrap();
        permit.configure_route_statistics(None, true);
        permit.observe_completion(observer);
        drop(permit);
        assert_eq!(observations.lock().len(), 3);
    }

    fn policy() -> GeneratedMemoryPolicy {
        GeneratedMemoryPolicy {
            hard_instance_ceiling: 200,
            soft_budget_bytes: 512 * MIB,
            hard_budget_bytes: 640 * MIB,
            safety_reserve_bytes: 64 * MIB,
            unattributed_bytes_per_slot: MIB,
            cold_peak_growth_bytes: 32 * MIB,
            warm_idle_target: 2,
            maximum_idle_age: Duration::from_secs(60),
            pressure_enter_headroom_bytes: 128 * MIB,
            pressure_exit_headroom_bytes: 256 * MIB,
        }
    }

    #[test]
    fn shared_static_credit_preserves_capacity_limits_and_pool_ownership() {
        let controller = test_controller(policy()).unwrap();
        let function = identity("shared", "a", "shared");
        let module_charge = controller.try_charge_module(40 * MIB).unwrap();
        let mut slots = Vec::new();
        for _ in 0..2 {
            let permit = controller.try_admit(function.clone(), None).unwrap();
            assert!(permit
                .account
                .host_owned_bytes_changed(HostOwnedByteAccounting {
                    shared_packed_bytes: 0,
                    current_bytes: 80 * MIB,
                    peak_bytes: 80 * MIB,
                    retained_bytes: 0,
                }));
            assert!(permit
                .account
                .host_owned_bytes_changed(HostOwnedByteAccounting::default()));
            permit.begin_static_memory_instantiation().unwrap();
            assert!(permit.account.update_guest(48 * MIB, None));
            permit.credit_shared_static(32 * MIB).unwrap();
            assert_eq!(permit.current_bytes().0, 48 * MIB);
            assert_eq!(permit.preparation_memory_cost().retained_bytes, 16 * MIB);
            assert_eq!(permit.preparation_memory_cost().peak_bytes, 80 * MIB);
            permit.credit_shared_static(32 * MIB).unwrap();
            assert!(permit.credit_shared_static(31 * MIB).is_err());
            slots.push(permit.slot_id());
            permit.finish(TerminalMemoryOutcome::Success, true);
        }
        let snapshot = controller.snapshot_for_test(&function);
        assert_eq!(snapshot.retained_baseline_bytes, 32 * MIB);
        assert_eq!(snapshot.fixed_module_bytes, 40 * MIB);
        let warm = controller
            .try_admit(function.clone(), Some(slots[0]))
            .unwrap();
        assert!(warm.credit_shared_static(32 * MIB).is_err());
        let mut limiter = warm.limiter(
            wasmtime::StoreLimitsBuilder::new()
                .memory_size(50 * MIB)
                .build(),
        );
        assert!(limiter.memory_growing(48 * MIB, 51 * MIB, None).is_err());
        assert_eq!(warm.current_bytes().0, 48 * MIB);
        warm.finish(TerminalMemoryOutcome::MemoryLimit, false);
        assert_eq!(
            controller
                .snapshot_for_test(&function)
                .retained_baseline_bytes,
            16 * MIB
        );
        controller
            .try_admit(function.clone(), Some(slots[1]))
            .unwrap()
            .finish_unstarted(false);
        assert_eq!(
            controller.snapshot_for_test(&function).fixed_module_bytes,
            40 * MIB
        );
        drop(module_charge);
        assert_eq!(
            controller.snapshot_for_test(&function).fixed_module_bytes,
            0
        );
    }

    #[test]
    fn delayed_static_credit_preserves_earlier_combined_private_peak() {
        for scale in [1, 3] {
            let controller = test_controller(policy()).unwrap();
            let function = identity("delayed-static", "a", "delayed-static");
            let permit = controller.try_admit(function.clone(), None).unwrap();
            let mut limiter = permit.limiter(
                wasmtime::StoreLimitsBuilder::new()
                    .memory_size(128 * MIB)
                    .build(),
            );
            // The same limiter callback accounts for a preceding dependency's
            // GC heap, then the memory owner's initial linear allocation.
            limiter.memory_growing(0, 8 * scale * MIB, None).unwrap();
            let host = |bytes| HostOwnedByteAccounting {
                current_bytes: bytes * scale * MIB,
                peak_bytes: 10 * scale * MIB,
                shared_packed_bytes: 0,
                retained_bytes: 0,
            };
            assert!(permit.account.host_owned_bytes_changed(host(10)));
            assert!(permit.account.host_owned_bytes_changed(host(2)));
            permit.begin_static_memory_instantiation().unwrap();
            limiter.memory_growing(0, 20 * scale * MIB, None).unwrap();
            permit.credit_shared_static(20 * scale * MIB).unwrap();
            let cost = permit.preparation_memory_cost();
            assert_eq!(cost.peak_bytes, 18 * scale * MIB);
            assert_eq!(cost.retained_bytes, 10 * scale * MIB);
            let state = controller.state.lock();
            let invocation = &state.active[&permit.account.invocation_id];
            assert_eq!(invocation.peak_accounted_bytes, 18 * scale * MIB);
            drop(state);
            drop(limiter);
            permit.finish(TerminalMemoryOutcome::Success, false);
            // The scaled case exceeds the cold prior: losing the earlier
            // private peak would incorrectly leave initialization at that prior.
            assert!(
                controller.state.lock().functions[&function]
                    .preparation_forecast(policy().cold_peak_growth_bytes)
                    >= 18 * scale * MIB
            );
        }
    }

    #[test]
    fn verified_initial_static_credit_admits_private_capacity_and_preserves_raw_limits() {
        for (verified, raw_limit, succeeds) in [
            (true, 64 * MIB, true),
            (false, 64 * MIB, false),
            (true, 40 * MIB, false),
        ] {
            let mut limits = policy();
            limits.soft_budget_bytes = 124 * MIB;
            limits.hard_budget_bytes = 128 * MIB;
            limits.safety_reserve_bytes = 8 * MIB;
            limits.cold_peak_growth_bytes = 4 * MIB;
            let controller = test_controller(limits).unwrap();
            let function = identity("initial-static", "a", "initial-static");
            let module_charge = controller.try_charge_module(100 * MIB).unwrap();
            let permit = controller.try_admit(function.clone(), None).unwrap();
            let mut limiter = permit.limiter(
                wasmtime::StoreLimitsBuilder::new()
                    .memory_size(raw_limit)
                    .build(),
            );
            if verified {
                limiter.prepare_initial_static_memory(48 * MIB, 32 * MIB);
            }
            assert_eq!(limiter.memory_growing(0, 48 * MIB, None).is_ok(), succeeds);
            let snapshot = controller.snapshot_for_test(&function);
            if succeeds {
                assert_eq!(snapshot.active_guest_bytes, 48 * MIB);
                assert_eq!(snapshot.active_current_growth_bytes, 16 * MIB);
                assert_eq!(snapshot.projected_bytes, 125 * MIB);
                permit.credit_shared_static(32 * MIB).unwrap();
                // An allocator failure keeps the preflight charge until the
                // failed Store is destroyed; it must never return to the pool.
                assert!(limiter
                    .memory_grow_failed(wasmtime::Error::msg("allocation failed"))
                    .is_err());
            } else {
                assert_eq!(snapshot.active_guest_bytes, 0);
                assert_eq!(snapshot.active_current_growth_bytes, 0);
            }
            drop(limiter);
            drop(permit);
            assert_eq!(
                controller.snapshot_for_test(&function).projected_bytes,
                108 * MIB
            );
            drop(module_charge);
        }
    }

    fn identity(name: &str, deployment: &str, fingerprint: &str) -> FunctionMemoryIdentity {
        FunctionMemoryIdentity {
            deployment: deployment.to_owned(),
            package_key: format!("package-{deployment}"),
            lineage: FunctionMemoryLineage::LegacyExport {
                module_path: "module.js".to_owned(),
                export_name: name.to_owned(),
                udf_kind: UdfKind::Query,
                resolved_graph_sha256: fingerprint.to_owned(),
                export_sha256: fingerprint.to_owned(),
            },
            compiler_pipeline_sha256: "pipeline".to_owned(),
            compiler_revision: "compiler".to_owned(),
            static_hermes_revision: "hermes".to_owned(),
            admitted_language_version: 1,
            manifest_schema_version: 1,
            effect_execution_mode: EffectExecutionMode::BlockingFiber,
            opaque_value_abi_version: 1,
            wasmtime_revision: "wasmtime".to_owned(),
            target_triple: "target".to_owned(),
            target_cpu: "cpu".to_owned(),
            engine_configuration_sha256: "engine".to_owned(),
            admission_policy_version: 1,
        }
    }

    #[test]
    fn preparation_order_estimates_follow_later_handler_memory_observations() {
        let mut limits = policy();
        limits.hard_instance_ceiling = 2;
        limits.soft_budget_bytes = 1216 * MIB;
        limits.hard_budget_bytes = 1536 * MIB;
        let controller = test_controller(limits).unwrap();
        let first = identity("first", "a", "first");
        let second = identity("second", "a", "second");
        {
            let mut state = controller.state.lock();
            state
                .functions
                .insert(first.clone(), FunctionMemoryStats::new(128 * MIB, 0));
            state
                .functions
                .insert(second.clone(), FunctionMemoryStats::new(256 * MIB, 0));
        }
        let mut observed = Vec::new();
        for (function, retained) in [(&first, 64 * MIB), (&second, 128 * MIB)] {
            let prepared = controller.try_admit_preparation(function.clone()).unwrap();
            assert!(prepared.account.update_guest(retained, None));
            assert!(prepared.reserve_idle_growth());
            observed.push((prepared.slot_id(), prepared.preparation_memory_cost()));
            prepared.finish(TerminalMemoryOutcome::Success, true);
        }
        let transient = |function: &FunctionMemoryIdentity, measured| {
            let estimate = controller
                .estimated_preparation_memory_cost(function, measured)
                .unwrap();
            estimate.peak_bytes - estimate.retained_bytes
        };
        assert!(transient(&first, observed[0].1) < transient(&second, observed[1].1));
        let warm = controller
            .try_admit(first.clone(), Some(observed[0].0))
            .unwrap();
        assert!(warm
            .account
            .host_owned_bytes_changed(HostOwnedByteAccounting {
                shared_packed_bytes: 0,
                current_bytes: 448 * MIB,
                peak_bytes: 448 * MIB,
                retained_bytes: 0,
            }));
        assert!(warm
            .account
            .host_owned_bytes_changed(HostOwnedByteAccounting {
                shared_packed_bytes: 0,
                current_bytes: 0,
                peak_bytes: 448 * MIB,
                retained_bytes: 0,
            }));
        assert!(warm.reserve_idle_growth());
        warm.finish(TerminalMemoryOutcome::Success, true);
        for (slot, reason) in controller.idle_eviction_candidates(
            &BTreeSet::new(),
            &[(observed[0].0, Duration::ZERO)],
            IdleEvictionTrigger::InitializationInvalidation,
        ) {
            controller.finish_idle_eviction(slot, reason);
        }
        assert!(matches!(
            controller.try_admit_preparation(first.clone()),
            Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget))
        ));
        // The old order protects the second group's baseline forever. Current
        // forecasts put the missing first group ahead, allowing that baseline
        // to be reclaimed and both groups to be rebuilt within the same budget.
        assert!(transient(&first, observed[0].1) > transient(&second, observed[1].1));
        let evicted =
            controller.preparation_eviction_candidates(&first, &[(observed[1].0, Duration::ZERO)]);
        assert_eq!(evicted.len(), 1);
        for (slot, reason) in evicted {
            controller.finish_idle_eviction(slot, reason);
        }
        for (function, retained) in [(&first, 64 * MIB), (&second, 128 * MIB)] {
            let prepared = controller.try_admit_preparation(function.clone()).unwrap();
            assert!(prepared.account.update_guest(retained, None));
            assert!(prepared.reserve_idle_growth());
            prepared.finish(TerminalMemoryOutcome::Success, true);
        }
        assert_eq!(controller.snapshot_for_test(&first).idle_instances, 2);
    }

    #[test]
    fn preparation_forecast_is_not_diluted_by_warm_handler_samples() {
        let mut limits = policy();
        limits.soft_budget_bytes = 144 * MIB;
        let controller = test_controller(limits).unwrap();
        let function = identity("prepared", "a", "prepared");
        let retained = controller.try_admit(function.clone(), None).unwrap();
        assert!(retained.account.update_guest(8 * MIB, None));
        assert!(retained.reserve_idle_growth());
        let slot = retained.slot_id();
        retained.finish(TerminalMemoryOutcome::Success, true);
        for _ in 0..200 {
            let warm = controller.try_admit(function.clone(), Some(slot)).unwrap();
            assert!(warm.reserve_idle_growth());
            warm.finish(TerminalMemoryOutcome::Success, true);
        }
        let prepared = controller.try_admit_preparation(function.clone()).unwrap();
        assert!(prepared.account.update_guest(80 * MIB, None));
        assert!(prepared.reserve_idle_growth());
        // Learned warm headroom lets this Store fit. Discard it deliberately
        // so the next cold admission must use the completed preparation sample.
        prepared.finish(TerminalMemoryOutcome::Success, false);
        // The ordinary 99th percentile still describes small warm growth.
        // Replenishment must retain its cold observation independently.
        for _ in 0..200 {
            controller
                .try_admit(function.clone(), Some(slot))
                .unwrap()
                .finish(TerminalMemoryOutcome::Success, true);
        }
        assert!(matches!(
            controller.try_admit_preparation(function.clone()),
            Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget))
        ));
        // Reallocation must use the same preparation forecast as admission;
        // otherwise it destroys useful capacity for a Store that still fails.
        assert!(controller
            .preparation_eviction_candidates(&function, &[(slot, Duration::ZERO)])
            .is_empty());
        controller
            .try_admit(function, Some(slot))
            .unwrap()
            .finish_unstarted(false);
    }

    #[test]
    fn preparation_rejects_a_store_without_room_for_warm_execution() {
        let mut limits = policy();
        limits.cold_peak_growth_bytes = 64 * MIB;
        limits.unattributed_bytes_per_slot = 2 * MIB;
        limits.soft_budget_bytes = limits.safety_reserve_bytes + 100 * MIB;
        let controller = test_controller(limits).unwrap();
        let function = identity("prepared", "a", "prepared");
        assert!(matches!(
            controller.try_admit_preparation(function.clone()),
            Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget))
        ));
        // Cold invocation admission alone would succeed, but retaining its
        // initialized 48 MiB would not leave the 64 MiB checkout allowance.
        let permit = controller.try_admit(function, None).unwrap();
        assert!(permit.account.update_guest(48 * MIB, None));
        assert!(!permit.reserve_idle_growth());
        permit.finish(TerminalMemoryOutcome::Success, false);
    }

    #[test]
    fn preparation_reclamation_keeps_useful_capacity_when_the_pending_store_still_cannot_fit() {
        let mut limits = policy();
        limits.soft_budget_bytes = 224 * MIB;
        let controller = test_controller(limits).unwrap();
        let healthy = identity("healthy", "a", "healthy");
        // Seed the small group's own forecast before another group's larger
        // observation becomes the global prior for newly encountered groups.
        let seed = controller.try_admit(healthy.clone(), None).unwrap();
        seed.finish_unstarted(false);
        let large = identity("large", "a", "large");
        let observation = controller.try_admit(large.clone(), None).unwrap();
        assert!(observation.account.update_guest(128 * MIB, None));
        observation.finish(TerminalMemoryOutcome::Success, false);
        let retained = controller.try_admit_preparation(healthy.clone()).unwrap();
        assert!(retained.account.update_guest(16 * MIB, None));
        assert!(retained.reserve_idle_growth());
        let slot = retained.slot_id();
        retained.finish(TerminalMemoryOutcome::Success, true);
        let before = controller.snapshot_for_test(&healthy).projected_bytes;
        assert!(matches!(
            controller.try_admit_preparation(large.clone()),
            Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget))
        ));
        assert!(controller
            .preparation_eviction_candidates(&large, &[(slot, Duration::ZERO)])
            .is_empty());
        assert_eq!(
            controller.snapshot_for_test(&healthy).projected_bytes,
            before
        );
        // The failed reallocation leaves real usable capacity for the healthy
        // group, rather than publishing an eviction and rebuilding it forever.
        controller
            .try_admit(healthy, Some(slot))
            .unwrap()
            .finish_unstarted(false);
    }

    #[test]
    fn preparation_reclamation_counts_idle_reservations_and_preserves_occupied_slots() {
        for soft_budget_bytes in [160 * MIB, 208 * MIB] {
            let mut limits = policy();
            limits.soft_budget_bytes = soft_budget_bytes;
            limits.hard_instance_ceiling = 2;
            let controller = test_controller(limits).unwrap();
            let function = identity("prepared", "a", "prepared");
            let retained = controller.try_admit_preparation(function.clone()).unwrap();
            assert!(retained.account.update_guest(16 * MIB, None));
            assert!(retained.reserve_idle_growth());
            let slot = retained.slot_id();
            retained.finish(TerminalMemoryOutcome::Success, true);
            let occupied = controller.try_admit(function.clone(), None).unwrap();
            assert!(occupied.account.update_guest(80 * MIB, None));
            let before = controller.snapshot_for_test(&function);
            assert_eq!(before.projected_bytes, 194 * MIB);
            assert!(controller
                .preparation_eviction_candidates(&function, &[])
                .is_empty());
            // Reclaiming the idle Store still leaves 210 MiB including the
            // new preparation, so neither budget permits admission yet.
            assert!(controller
                .preparation_eviction_candidates(&function, &[(slot, Duration::ZERO)])
                .is_empty());
            assert_eq!(controller.snapshot_for_test(&function), before);
            occupied.finish_unstarted(false);
            let released = controller.snapshot_for_test(&function);
            assert_eq!(released.projected_bytes, 113 * MIB);
            assert_eq!(released.function_samples, before.function_samples);
            // Unstarted completion leaves the forecast at 32 MiB. Preparation
            // needs 178 MiB with the idle Store, or 129 MiB after releasing its
            // baseline, growth reservation and slot allowance together.
            let needs_reclamation = soft_budget_bytes == 160 * MIB;
            if needs_reclamation {
                assert!(matches!(
                    controller.try_admit_preparation(function.clone()),
                    Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget))
                ));
            }
            let evicted =
                controller.preparation_eviction_candidates(&function, &[(slot, Duration::ZERO)]);
            if needs_reclamation {
                assert_eq!(evicted, vec![(slot, IdleEvictionReason::AdmissionBudget)]);
            } else {
                assert!(evicted.is_empty());
                assert_eq!(controller.snapshot_for_test(&function), released);
            }
            for (slot, reason) in evicted {
                controller.finish_idle_eviction(slot, reason);
            }
            controller
                .try_admit_preparation(function)
                .unwrap()
                .finish_unstarted(false);
        }
    }

    #[test]
    fn preparation_reclaims_multiple_baselines_before_rebuilding_any() {
        let mut limits = policy();
        limits.soft_budget_bytes = 288 * MIB;
        let controller = test_controller(limits).unwrap();
        let small_groups = [
            identity("first", "a", "first"),
            identity("last", "a", "last"),
        ];
        // Establish the small groups before the large group's observation can
        // become their inherited forecast. Its learned allowance is 96 MiB.
        for group in &small_groups {
            controller
                .try_admit(group.clone(), None)
                .unwrap()
                .finish_unstarted(false);
        }
        let large = identity("large", "a", "large");
        let observed = controller.try_admit(large.clone(), None).unwrap();
        assert!(observed.account.update_guest(80 * MIB, None));
        let large_slot = observed.slot_id();
        observed.finish(TerminalMemoryOutcome::Success, true);
        let warm = controller
            .try_admit(large.clone(), Some(large_slot))
            .unwrap();
        assert!(warm
            .account
            .host_owned_bytes_changed(HostOwnedByteAccounting {
                shared_packed_bytes: 0,
                current_bytes: 80 * MIB,
                peak_bytes: 80 * MIB,
                retained_bytes: 0,
            }));
        warm.finish(TerminalMemoryOutcome::Success, false);
        let mut slots = Vec::new();
        for group in &small_groups {
            let prepared = controller.try_admit_preparation(group.clone()).unwrap();
            assert!(prepared.account.update_guest(8 * MIB, None));
            assert!(prepared.reserve_idle_growth());
            slots.push((prepared.slot_id(), Duration::ZERO));
            prepared.finish(TerminalMemoryOutcome::Success, true);
        }
        assert!(matches!(
            controller.try_admit_preparation(large.clone()),
            Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget))
        ));
        // One removal is insufficient. Rebuilding it before removing the other
        // used to prevent this feasible three-group baseline on every pass.
        assert!(controller
            .preparation_eviction_candidates(&large, &slots[..1])
            .is_empty());
        let evicted = controller.preparation_eviction_candidates(&large, &slots);
        assert_eq!(evicted.len(), 2);
        assert_eq!(controller.snapshot_for_test(&large).evicting_instances, 2);
        for (slot, reason) in evicted {
            controller.finish_idle_eviction(slot, reason);
        }
        for group in std::iter::once(&large).chain(&small_groups) {
            let prepared = controller.try_admit_preparation(group.clone()).unwrap();
            assert!(prepared.account.update_guest(8 * MIB, None));
            assert!(prepared.reserve_idle_growth());
            prepared.finish(TerminalMemoryOutcome::Success, true);
        }
        let ready = controller.snapshot_for_test(&large);
        assert_eq!(ready.idle_instances, 3);
        assert_eq!(ready.projected_bytes, 251 * MIB);
    }

    #[test]
    fn prepared_growth_reservation_transfers_to_checkout_without_double_charge() {
        let mut limits = policy();
        limits.cold_peak_growth_bytes = 64 * MIB;
        limits.unattributed_bytes_per_slot = 2 * MIB;
        limits.soft_budget_bytes = 256 * MIB;
        let controller = test_controller(limits).unwrap();
        let function = identity("prepared", "a", "prepared");
        let permit = controller.try_admit_preparation(function.clone()).unwrap();
        assert!(permit.account.update_guest(48 * MIB, None));
        assert!(permit.reserve_idle_growth());
        let slot = permit.slot_id();
        let reserved = controller.snapshot_for_test(&function).projected_bytes;
        permit.finish(TerminalMemoryOutcome::Success, true);
        assert_eq!(
            controller.snapshot_for_test(&function).projected_bytes,
            reserved
        );
        let before = controller.snapshot_for_test(&function).projected_bytes;
        assert!(matches!(
            controller.try_admit_preparation(function.clone()),
            Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget))
        ));
        let warm = controller.try_admit(function.clone(), Some(slot)).unwrap();
        assert_eq!(
            controller.snapshot_for_test(&function).projected_bytes,
            before
        );
        warm.finish_unstarted(true);
        assert_eq!(
            controller.snapshot_for_test(&function).projected_bytes,
            before
        );
        let warm = controller.try_admit(function, Some(slot)).unwrap();
        warm.finish(TerminalMemoryOutcome::Success, false);
        assert!(controller.state.lock().slots.is_empty());
    }

    #[test]
    fn mixed_small_and_large_forecasts_share_only_safe_capacity() {
        let controller = test_controller(policy()).unwrap();
        let small = identity("small", "a", "small");
        let large = identity("large", "a", "large");
        {
            let mut state = controller.state.lock();
            state
                .functions
                .insert(small.clone(), FunctionMemoryStats::new(4 * MIB, 1));
            state
                .functions
                .get_mut(&small)
                .unwrap()
                .peak_growth
                .observe(4 * MIB);
            state.functions.get_mut(&small).unwrap().sample_count = MINIMUM_USEFUL_SAMPLES;
            state
                .functions
                .insert(large.clone(), FunctionMemoryStats::new(192 * MIB, 2));
            state
                .functions
                .get_mut(&large)
                .unwrap()
                .peak_growth
                .observe(192 * MIB);
            state.functions.get_mut(&large).unwrap().sample_count = MINIMUM_USEFUL_SAMPLES;
        }
        let first = controller.try_admit(large, None).unwrap();
        let second = controller.try_admit(small.clone(), None).unwrap();
        assert!(matches!(
            controller.try_admit(small, None),
            Ok(_) | Err(TryAdmissionError::Wait(_))
        ));
        drop(first);
        drop(second);
    }

    #[test]
    fn steep_cold_burst_is_bounded_below_count_ceiling() {
        let controller = test_controller(policy()).unwrap();
        let function = identity("burst", "a", "burst");
        let mut permits = Vec::new();
        while let Ok(permit) = controller.try_admit(function.clone(), None) {
            permits.push(permit);
        }
        assert!(permits.len() < 200);
        assert_eq!(permits.len(), 13);
    }

    #[test]
    fn prediction_overrun_raises_live_liability() {
        let controller = test_controller(policy()).unwrap();
        let permit = controller
            .try_admit(identity("overrun", "a", "overrun"), None)
            .unwrap();
        assert!(permit.account.update_guest(96 * MIB, None));
        let state = controller.state.lock();
        let active = state.active.values().next().unwrap();
        assert_eq!(active.liability_bytes, 96 * MIB);
        assert!(active.prediction_overrun_recorded);
    }

    #[tokio::test]
    async fn admission_resumes_when_an_active_invocation_releases_transient_memory() {
        let mut limits = policy();
        limits.soft_budget_bytes = 192 * MIB;
        let controller = test_controller(limits).unwrap();
        let active = controller
            .try_admit(identity("active", "a", "active"), None)
            .unwrap();
        assert!(active
            .account
            .host_owned_bytes_changed(HostOwnedByteAccounting {
                shared_packed_bytes: 0,
                current_bytes: 128 * MIB,
                peak_bytes: 128 * MIB,
                retained_bytes: 0,
            }));
        let admission = controller.admit(identity("waiting", "a", "waiting"), None);
        tokio::pin!(admission);
        assert!(futures::poll!(&mut admission).is_pending());
        assert!(active
            .account
            .host_owned_bytes_changed(HostOwnedByteAccounting {
                shared_packed_bytes: 0,
                current_bytes: 0,
                peak_bytes: 128 * MIB,
                retained_bytes: 0,
            }));
        let admitted = tokio::time::timeout(Duration::from_secs(1), &mut admission)
            .await
            .expect("admission waited for completion after memory was released")
            .unwrap();
        assert_eq!(controller.state.lock().active.len(), 2);
        admitted.finish_unstarted(false);
        active.finish_unstarted(false);
    }

    #[test]
    fn resource_limit_terminal_preserves_memory_learning() {
        let controller = test_controller(policy()).unwrap();
        let function = identity("resource-limit", "a", "resource-limit");
        let permit = controller.try_admit(function.clone(), None).unwrap();
        assert!(permit.account.update_guest(96 * MIB, None));
        permit.finish(TerminalMemoryOutcome::ResourceLimit, false);

        let completed = controller.snapshot_for_test(&function);
        assert_eq!(completed.active_instances, 0);
        assert_eq!(completed.idle_instances, 0);
        assert_eq!(completed.function_samples, 1);
        assert_eq!(completed.timeout_samples, 0);
        assert_eq!(completed.resource_limit_samples, 1);
        assert_eq!(completed.memory_limit_samples, 0);

        let next = controller.try_admit(function, None).unwrap();
        let forecast = controller
            .state
            .lock()
            .active
            .get(&next.account.invocation_id)
            .unwrap()
            .forecast_bytes;
        // Forecasts use the conservative upper bound of a logarithmic bucket.
        assert_eq!(forecast, 112 * MIB);
    }

    #[test]
    fn aggregate_metrics_track_active_idle_and_evicting_state() {
        let controller = test_controller(policy()).unwrap();
        let function = identity("metrics", "a", "metrics");
        let permit = controller.try_admit(function.clone(), None).unwrap();
        let slot_id = permit.slot_id();
        assert!(permit.account.update_guest(48 * MIB, None));
        assert!(permit
            .account
            .host_owned_bytes_changed(HostOwnedByteAccounting {
                shared_packed_bytes: 0,
                current_bytes: 3 * MIB,
                peak_bytes: 3 * MIB,
                retained_bytes: 5 * MIB,
            }));

        let active = controller.snapshot_for_test(&function);
        assert_eq!(active.active_instances, 1);
        assert_eq!(active.idle_instances, 0);
        assert_eq!(active.evicting_instances, 0);
        assert_eq!(active.retained_baseline_bytes, 0);
        assert_eq!(active.active_checkout_baseline_bytes, 0);
        assert_eq!(active.active_guest_bytes, 48 * MIB);
        assert_eq!(active.active_host_bytes, 8 * MIB);
        assert_eq!(active.active_current_growth_bytes, 56 * MIB);
        assert_eq!(active.active_forecast_bytes, 32 * MIB);
        assert_eq!(active.active_liability_bytes, 56 * MIB);
        assert_eq!(active.unattributed_allowance_bytes, MIB);
        assert_eq!(
            active.projected_bytes,
            active
                .retained_baseline_bytes
                .saturating_add(active.active_liability_bytes)
                .saturating_add(active.fixed_module_bytes)
                .saturating_add(active.unattributed_allowance_bytes)
                .saturating_add(policy().safety_reserve_bytes)
        );

        permit.finish(TerminalMemoryOutcome::Success, true);
        let idle = controller.snapshot_for_test(&function);
        assert_eq!(idle.active_instances, 0);
        assert_eq!(idle.idle_instances, 1);
        assert_eq!(idle.evicting_instances, 0);
        assert_eq!(idle.retained_baseline_bytes, 56 * MIB);
        assert_eq!(idle.active_guest_bytes, 0);
        assert_eq!(idle.active_host_bytes, 0);
        assert_eq!(idle.active_liability_bytes, 0);
        assert_eq!(idle.unattributed_allowance_bytes, MIB);

        controller.set_pressure_for_test(BackendPressure::Active {
            headroom_bytes: 64 * MIB,
        });
        assert_eq!(
            controller.idle_eviction_candidates(
                &BTreeSet::new(),
                &[(slot_id, Duration::ZERO)],
                IdleEvictionTrigger::Maintenance,
            ),
            vec![(slot_id, IdleEvictionReason::MemoryPressure)]
        );
        let evicting = controller.snapshot_for_test(&function);
        assert_eq!(evicting.active_instances, 0);
        assert_eq!(evicting.idle_instances, 0);
        assert_eq!(evicting.evicting_instances, 1);
        assert_eq!(evicting.retained_baseline_bytes, 56 * MIB);

        controller.finish_idle_eviction(slot_id, IdleEvictionReason::MemoryPressure);
        let evicted = controller.snapshot_for_test(&function);
        assert_eq!(evicted.active_instances, 0);
        assert_eq!(evicted.idle_instances, 0);
        assert_eq!(evicted.evicting_instances, 0);
        assert_eq!(evicted.retained_baseline_bytes, 0);
        assert_eq!(evicted.unattributed_allowance_bytes, 0);
        assert_eq!(evicted.projected_bytes, policy().safety_reserve_bytes);
    }

    #[test]
    fn unavailable_sample_does_not_release_active_pressure() {
        let controller = test_controller(policy()).unwrap();
        controller.update_pressure_sample(Ok(Some(policy().pressure_enter_headroom_bytes)));
        let active = controller.pressure();
        assert!(matches!(active, BackendPressure::Active { .. }));

        controller.update_pressure_sample(Ok(None));
        assert_eq!(controller.pressure(), active);
        assert!(matches!(
            controller.try_admit(identity("pressure", "a", "pressure"), None),
            Err(TryAdmissionError::Wait(AdmissionWaitReason::Pressure))
        ));

        controller.update_pressure_sample(Ok(Some(policy().pressure_exit_headroom_bytes)));
        assert!(matches!(
            controller.pressure(),
            BackendPressure::Healthy { .. }
        ));
        controller
            .try_admit(identity("pressure", "a", "pressure"), None)
            .unwrap()
            .finish_unstarted(false);
    }

    #[test]
    fn changed_deployment_uses_conservative_prior_until_warm() {
        let controller = test_controller(policy()).unwrap();
        let old = identity("function", "old", "old");
        {
            let mut state = controller.state.lock();
            let mut stats = FunctionMemoryStats::new(128 * MIB, 1);
            for _ in 0..MINIMUM_USEFUL_SAMPLES {
                stats.warm_peak_growth.observe(128 * MIB);
            }
            stats.warm_sample_count = MINIMUM_USEFUL_SAMPLES;
            state.functions.insert(old, stats);
        }
        let new = identity("function", "new", "old");
        let permit = controller.try_admit(new.clone(), None).unwrap();
        assert_eq!(
            controller
                .state
                .lock()
                .active
                .get(&permit.account.invocation_id)
                .unwrap()
                .forecast_bytes,
            200 * MIB
        );
        drop(permit);
    }

    #[test]
    fn aggregate_hard_limit_denies_memory_growth() {
        let mut constrained = policy();
        constrained.hard_budget_bytes = 160 * MIB;
        constrained.soft_budget_bytes = 128 * MIB;
        constrained.safety_reserve_bytes = 32 * MIB;
        let controller = test_controller(constrained).unwrap();
        let permit = controller
            .try_admit(identity("grow", "a", "grow"), None)
            .unwrap();
        let mut limiter = permit.limiter(
            wasmtime::StoreLimitsBuilder::new()
                .memory_size(512 * MIB)
                .instances(1)
                .memories(1)
                .trap_on_grow_failure(true)
                .build(),
        );
        let error = limiter.memory_growing(0, 144 * MIB, None).unwrap_err();
        assert!(error.is::<GeneratedGuestMemoryLimit>());
        assert!(
            controller
                .state
                .lock()
                .active
                .values()
                .next()
                .unwrap()
                .growth_denied
        );
    }

    #[test]
    fn fixed_module_charges_affect_admission_and_release_once() {
        let controller = test_controller(policy()).unwrap();
        let function = identity("module-accounting", "a", "module-accounting");
        let first = controller.try_charge_module(215 * MIB).unwrap();
        let second = controller.try_charge_module(215 * MIB).unwrap();
        assert_eq!(
            controller.snapshot_for_test(&function).fixed_module_bytes,
            430 * MIB
        );
        assert!(matches!(
            controller.try_admit(function.clone(), None),
            Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget))
        ));

        drop(first);
        assert_eq!(
            controller.snapshot_for_test(&function).fixed_module_bytes,
            215 * MIB
        );
        let permit = controller.try_admit(function.clone(), None).unwrap();
        drop(permit);
        drop(second);
        assert_eq!(
            controller.snapshot_for_test(&function).fixed_module_bytes,
            0
        );
    }

    #[test]
    fn accounting_overflow_cannot_grant_hard_budget_authority() {
        let mut maximum_budget = policy();
        maximum_budget.hard_instance_ceiling = 1;
        maximum_budget.warm_idle_target = 1;
        maximum_budget.soft_budget_bytes = usize::MAX - 1;
        maximum_budget.hard_budget_bytes = usize::MAX;
        maximum_budget.safety_reserve_bytes = 1;
        maximum_budget.unattributed_bytes_per_slot = 1;
        maximum_budget.cold_peak_growth_bytes = 1;
        let controller = test_controller(maximum_budget).unwrap();
        let permit = controller
            .try_admit(identity("overflow", "a", "overflow"), None)
            .unwrap();

        assert!(!permit.account.update_guest(usize::MAX, None));
        assert!(
            controller
                .state
                .lock()
                .active
                .values()
                .next()
                .unwrap()
                .growth_denied
        );
        drop(permit);

        assert!(matches!(
            controller.try_charge_module(usize::MAX),
            Err(ModuleMemoryAdmissionError::FixedBytesExceedHardBudget)
        ));
    }

    #[test]
    fn per_store_memory_grow_limit_is_terminal() {
        let controller = test_controller(policy()).unwrap();
        let permit = controller
            .try_admit(identity("store-limit", "a", "store-limit"), None)
            .unwrap();
        let mut limiter = permit.limiter(
            wasmtime::StoreLimitsBuilder::new()
                .memory_size(2 * MIB)
                .instances(1)
                .memories(1)
                .trap_on_grow_failure(true)
                .build(),
        );
        let error = limiter.memory_growing(0, 3 * MIB, None).unwrap_err();
        assert!(error.is::<GeneratedGuestMemoryLimit>());
        assert!(
            controller
                .state
                .lock()
                .active
                .values()
                .next()
                .unwrap()
                .growth_denied
        );
    }

    #[test]
    fn per_store_limit_accounts_total_linear_memory() {
        let controller = test_controller(policy()).unwrap();
        let function = identity("linear-memory", "a", "linear-memory");
        let permit = controller.try_admit(function.clone(), None).unwrap();
        let mut limiter = permit.limiter(
            wasmtime::StoreLimitsBuilder::new()
                .memory_size(64 * MIB)
                .instances(1)
                .memories(1)
                .trap_on_grow_failure(true)
                .build(),
        );

        assert!(limiter.memory_growing(0, 36 * MIB, None).is_ok());
        assert_eq!(
            controller.snapshot_for_test(&function).active_guest_bytes,
            36 * MIB
        );
        assert!(limiter
            .memory_growing(36 * MIB, 64 * MIB + 1, None)
            .is_err());
        assert_eq!(
            controller.snapshot_for_test(&function).active_guest_bytes,
            36 * MIB
        );
    }

    #[test]
    fn real_store_accounts_linear_memory_and_gc_heap_across_reuse() -> anyhow::Result<()> {
        const PAGE: usize = 65_536;
        let controller = test_controller(policy())?;
        let function = identity("two-heaps", "a", "two-heaps");
        let permit = controller.try_admit(function.clone(), None).unwrap();
        let slot = permit.slot_id();
        let limits = wasmtime::StoreLimitsBuilder::new()
            .memory_size(64 * MIB)
            .memories(1)
            .trap_on_grow_failure(true)
            .build();
        let mut store =
            wasmtime::Store::new(&wasmtime::Engine::default(), permit.limiter(limits.clone()));
        store.limiter(|limiter| limiter);
        let memory = wasmtime::Memory::new(&mut store, wasmtime::MemoryType::new(768, None))?;
        store.gc_heap_grow(PAGE as u64)?;
        let actual = memory.data_size(&store) + store.gc_heap_capacity();
        assert_eq!(actual, 48 * MIB + PAGE);
        assert_eq!(permit.current_bytes().0, actual);
        memory.grow(&mut store, 1)?;
        let gc_bytes = store.gc_heap_capacity();
        let linear_bytes = memory.data_size(&store);
        store.data_mut().reconcile(linear_bytes, gc_bytes)?;
        permit.finish(TerminalMemoryOutcome::Success, true);

        let warm = controller.try_admit(function.clone(), Some(slot)).unwrap();
        *store.data_mut() = warm.limiter(limits);
        store.gc_heap_grow(PAGE as u64)?;
        memory.grow(&mut store, 1)?;
        let actual = memory.data_size(&store) + store.gc_heap_capacity();
        assert_eq!(warm.current_bytes().0, actual);
        // Each heap is below the per-Store limit; their sum exceeds it.
        assert!(memory
            .grow(&mut store, (16 * MIB / PAGE - 2) as u64)
            .is_err());
        assert_eq!(warm.current_bytes().0, actual);
        drop(store);
        warm.finish(TerminalMemoryOutcome::MemoryLimit, false);
        assert_eq!(
            controller
                .snapshot_for_test(&function)
                .retained_baseline_bytes,
            0
        );
        Ok(())
    }

    #[test]
    fn failed_growth_cannot_roll_back_an_earlier_success() -> anyhow::Result<()> {
        let controller = test_controller(policy())?;
        let function = identity("growth-failure", "a", "growth-failure");
        let permit = controller.try_admit(function.clone(), None).unwrap();
        let limiter = permit.limiter(
            wasmtime::StoreLimitsBuilder::new()
                .memory_size(64 * MIB)
                .memories(1)
                .trap_on_grow_failure(true)
                .build(),
        );
        let mut config = wasmtime::Config::new();
        config.wasm_custom_page_sizes(true);
        let mut store = wasmtime::Store::new(&wasmtime::Engine::new(&config)?, limiter);
        store.limiter(|limiter| limiter);
        let memory = wasmtime::Memory::new(
            &mut store,
            wasmtime::MemoryType::builder()
                .page_size_log2(0)
                .min(MIB as u64)
                .build()?,
        )?;
        memory.grow(&mut store, 1)?;
        let actual = memory.data_size(&store);
        // Wasmtime rejects this before calling memory_growing, but still calls
        // memory_grow_failed. There is no pending allocation to roll back.
        assert!(memory.grow(&mut store, u64::MAX).is_err());
        assert_eq!(permit.current_bytes().0, actual);
        store.data_mut().reconcile(actual, 0)?;
        drop(store);
        permit.finish(TerminalMemoryOutcome::MemoryLimit, false);

        let permit = controller.try_admit(function.clone(), None).unwrap();
        let mut limiter = permit.limiter(wasmtime::StoreLimitsBuilder::new().build());
        limiter.memory_growing(0, MIB, None)?;
        limiter.memory_growing(MIB, 2 * MIB, None)?;
        assert!(limiter
            .memory_grow_failed(wasmtime::Error::msg("allocation failed"))
            .is_err());
        // Only a measured capacity, not an ambiguous callback, releases bytes.
        assert_eq!(permit.current_bytes().0, 2 * MIB);
        limiter.reconcile(MIB, 0)?;
        assert_eq!(permit.current_bytes().0, MIB);
        assert!(limiter.memory_growing(MIB, 2 * MIB, None).is_err());
        permit.finish(TerminalMemoryOutcome::MemoryLimit, false);
        Ok(())
    }

    #[test]
    fn warm_learning_releases_cold_reservations_on_all_retained_slots() {
        let mut limits = policy();
        limits.cold_peak_growth_bytes = 512 * MIB;
        limits.soft_budget_bytes = 4 * 1024 * MIB;
        limits.hard_budget_bytes = 6 * 1024 * MIB;
        let controller = test_controller(limits).unwrap();
        let function = identity("prepared", "a", "prepared");
        let mut slots = Vec::new();
        for _ in 0..3 {
            let prepared = controller.try_admit_preparation(function.clone()).unwrap();
            assert!(prepared.account.update_guest(48 * MIB, None));
            assert!(prepared.reserve_idle_growth());
            slots.push(prepared.slot_id());
            prepared.finish(TerminalMemoryOutcome::Success, true);
        }
        let waiting_for_cpu = controller
            .try_admit(function.clone(), Some(slots[2]))
            .unwrap();
        for sample in 0..MINIMUM_USEFUL_SAMPLES {
            let warm = controller
                .try_admit(function.clone(), Some(slots[0]))
                .unwrap();
            assert_eq!(
                controller.state.lock().active[&warm.account.invocation_id].forecast_bytes,
                512 * MIB
            );
            assert!(warm
                .account
                .host_owned_bytes_changed(HostOwnedByteAccounting {
                    shared_packed_bytes: 0,
                    current_bytes: 64 * 1024,
                    peak_bytes: 64 * 1024,
                    retained_bytes: 0,
                }));
            assert!(warm
                .account
                .host_owned_bytes_changed(HostOwnedByteAccounting {
                    shared_packed_bytes: 0,
                    current_bytes: 0,
                    peak_bytes: 64 * 1024,
                    retained_bytes: 0,
                }));
            assert!(warm.reserve_idle_growth());
            warm.finish(TerminalMemoryOutcome::Success, true);
            if sample + 1 < MINIMUM_USEFUL_SAMPLES {
                assert_eq!(
                    controller.state.lock().slots[&slots[1]].idle_growth_bytes,
                    512 * MIB
                );
            }
        }
        waiting_for_cpu.finish_unstarted(true);
        {
            let state = controller.state.lock();
            for slot in &slots {
                assert_eq!(state.slots[slot].guest_bytes, 48 * MIB);
                assert_eq!(state.slots[slot].idle_growth_bytes, MIB);
            }
        }
        let warm = controller
            .try_admit(function.clone(), Some(slots[1]))
            .unwrap();
        assert_eq!(
            controller.state.lock().active[&warm.account.invocation_id].forecast_bytes,
            MIB
        );
        warm.finish_unstarted(true);
        let cold = controller.try_admit_preparation(function).unwrap();
        {
            let state = controller.state.lock();
            let invocation = &state.active[&cold.account.invocation_id];
            assert_eq!(invocation.forecast_bytes, 512 * MIB);
            assert_eq!(invocation.preparation_headroom_bytes, MIB);
        }
        cold.finish_unstarted(false);
    }

    #[test]
    fn idle_age_and_pressure_select_bounded_evictions() {
        let controller = test_controller(policy()).unwrap();
        let first = controller
            .try_admit(identity("first", "a", "first"), None)
            .unwrap();
        let first_slot = first.slot_id();
        first.finish(TerminalMemoryOutcome::Success, true);
        let second = controller
            .try_admit(identity("second", "a", "second"), None)
            .unwrap();
        let second_slot = second.slot_id();
        second.finish(TerminalMemoryOutcome::Success, true);
        let third = controller
            .try_admit(identity("third", "a", "third"), None)
            .unwrap();
        let third_slot = third.slot_id();
        third.finish(TerminalMemoryOutcome::Success, true);

        assert!(controller
            .idle_eviction_candidates(
                &BTreeSet::from([first_slot, second_slot, third_slot]),
                &[
                    (first_slot, Duration::from_secs(120)),
                    (second_slot, Duration::from_secs(90)),
                    (third_slot, Duration::from_secs(1)),
                ],
                IdleEvictionTrigger::Maintenance,
            )
            .is_empty());
        assert_eq!(
            controller.idle_eviction_candidates(
                &BTreeSet::new(),
                &[
                    (first_slot, Duration::from_secs(120)),
                    (second_slot, Duration::from_secs(90)),
                    (third_slot, Duration::from_secs(1)),
                ],
                IdleEvictionTrigger::Maintenance,
            ),
            vec![(first_slot, IdleEvictionReason::IdleAge)]
        );
        controller.finish_idle_eviction(first_slot, IdleEvictionReason::IdleAge);
        controller.update_pressure_sample(Ok(Some(64 * MIB)));
        assert_eq!(
            controller.idle_eviction_candidates(
                &BTreeSet::from([second_slot, third_slot]),
                &[
                    (second_slot, Duration::from_secs(1)),
                    (third_slot, Duration::from_secs(1)),
                ],
                IdleEvictionTrigger::Maintenance,
            ),
            vec![
                (second_slot, IdleEvictionReason::MemoryPressure),
                (third_slot, IdleEvictionReason::MemoryPressure),
            ]
        );
    }

    #[test]
    fn admission_budget_selects_one_oldest_idle_at_the_warm_target() {
        let controller = test_controller(policy()).unwrap();
        let first = controller
            .try_admit(identity("first", "a", "first"), None)
            .unwrap();
        let first_slot = first.slot_id();
        first.finish(TerminalMemoryOutcome::Success, true);
        let second = controller
            .try_admit(identity("second", "a", "second"), None)
            .unwrap();
        let second_slot = second.slot_id();
        second.finish(TerminalMemoryOutcome::Success, true);

        assert_eq!(
            controller.idle_eviction_candidates(
                &BTreeSet::new(),
                &[
                    (first_slot, Duration::from_secs(2)),
                    (second_slot, Duration::from_secs(1)),
                ],
                IdleEvictionTrigger::AdmissionBudget,
            ),
            vec![(first_slot, IdleEvictionReason::AdmissionBudget)]
        );
        let selected = controller.snapshot_for_test(&identity("first", "a", "first"));
        assert_eq!(selected.idle_instances, 1);
        assert_eq!(selected.evicting_instances, 1);
    }

    #[test]
    fn admission_reclaims_one_slot_even_when_all_candidates_exceed_the_idle_age() {
        let mut policy = policy();
        policy.warm_idle_target = 0;
        let controller = test_controller(policy).unwrap();
        let mut slots = Vec::new();
        for name in ["first", "second", "third"] {
            let permit = controller
                .try_admit(identity(name, "a", name), None)
                .unwrap();
            slots.push((permit.slot_id(), Duration::from_secs(120)));
            permit.finish(TerminalMemoryOutcome::Success, true);
        }
        assert_eq!(
            controller.idle_eviction_candidates(
                &BTreeSet::new(),
                &slots,
                IdleEvictionTrigger::AdmissionBudget
            ),
            vec![(slots[0].0, IdleEvictionReason::AdmissionBudget)]
        );
        let state = controller.snapshot_for_test(&identity("first", "a", "first"));
        assert_eq!(state.idle_instances, 2);
        assert_eq!(state.evicting_instances, 1);
    }

    #[test]
    fn generation_retirement_marks_only_supplied_idle_slots() {
        let controller = test_controller(policy()).unwrap();
        let first = controller
            .try_admit(identity("first", "a", "first"), None)
            .unwrap();
        let first_slot = first.slot_id();
        first.finish(TerminalMemoryOutcome::Success, true);
        let second = controller
            .try_admit(identity("second", "a", "second"), None)
            .unwrap();
        let second_slot = second.slot_id();
        second.finish(TerminalMemoryOutcome::Success, true);
        controller.update_pressure_sample(Ok(Some(64 * MIB)));

        assert_eq!(
            controller.idle_eviction_candidates(
                &BTreeSet::new(),
                &[(first_slot, Duration::ZERO)],
                IdleEvictionTrigger::GenerationRetirement,
            ),
            vec![(first_slot, IdleEvictionReason::GenerationRetirement)]
        );
        let selected = controller.snapshot_for_test(&identity("first", "a", "first"));
        assert_eq!(selected.idle_instances, 1);
        assert_eq!(selected.evicting_instances, 1);

        controller.finish_idle_eviction(first_slot, IdleEvictionReason::GenerationRetirement);
        let retained = controller.snapshot_for_test(&identity("second", "a", "second"));
        assert_eq!(retained.idle_instances, 1);
        assert_eq!(retained.evicting_instances, 0);
        assert_eq!(
            controller.idle_eviction_candidates(
                &BTreeSet::new(),
                &[(second_slot, Duration::ZERO)],
                IdleEvictionTrigger::GenerationRetirement,
            ),
            vec![(second_slot, IdleEvictionReason::GenerationRetirement)]
        );
        controller.finish_idle_eviction(second_slot, IdleEvictionReason::GenerationRetirement);
    }
}
