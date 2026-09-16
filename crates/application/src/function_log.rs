use std::{
    cell::Cell,
    collections::{
        BTreeMap,
        HashMap,
        VecDeque,
    },
    str::FromStr,
    sync::Arc,
    time::{
        Duration,
        SystemTime,
    },
};

use anyhow::Context as _;
use common::{
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentPath,
    },
    errors::{
        report_error_sync,
        JsError,
    },
    execution_context::ExecutionContext,
    identity::InertIdentity,
    knobs::{
        self,
    },
    log_lines::{
        LogLine,
        LogLines,
    },
    log_streaming::{
        self,
        FunctionConcurrencyStats,
        FunctionEventSource,
        FunctionRunReason,
        LogEvent,
        LogSender,
        SchedulerInfo,
        StructuredLogEvent,
    },
    runtime::{
        Runtime,
        SpawnHandle,
        UnixTimestamp,
    },
    types::{
        CursorMs,
        FunctionCaller,
        HttpActionRoute,
        ModuleEnvironment,
        QueryInvocation,
        TableName,
        TableStats,
        UdfIdentifier,
        UdfType,
    },
};
use function_runner::{
    QueryShadowAdmissionEvent,
    QueryShadowComparison,
    QueryShadowDirection,
    QueryShadowReport,
    QueryShadowReportSink,
    QueryShadowRouteKey,
    QueryShadowTerminal,
};
#[cfg(test)]
use function_runner::{
    QueryShadowFailureReason,
    QueryShadowFailureStage,
    QueryShadowInsertedDocumentIdentity,
};
use http::StatusCode;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use isolate::{
    IsolateClient,
    StaticHermesQueryShadowRegistry,
};
use itertools::Itertools;
use parking_lot::Mutex;
use serde_json::{
    json,
    Value as JsonValue,
};
use sync_types::types::SerializedArgs;
use tokio::sync::oneshot;
use udf::{
    validation::{
        ValidatedActionOutcome,
        ValidatedUdfOutcome,
    },
    HttpActionOutcome,
    HttpActionRequestHead,
    SyscallTrace,
    UdfOutcome,
};
use udf_metrics::{
    CounterBucket,
    GaugeBucket,
    HistogramBucket,
    MetricName,
    MetricStore,
    MetricStoreConfig,
    MetricType,
    MetricsWindow,
    Percentile,
    Timeseries,
    UdfMetricsError,
};
use usage_tracking::{
    AggregatedFunctionUsageStats,
    CallType,
    FunctionUsageTracker,
    OccInfo,
    UsageCounter,
};
use value::{
    heap_size::{
        HeapSize,
        WithHeapSize,
    },
    sha256::Sha256Digest,
};

#[cfg(feature = "static-hermes-wasmtime-gate")]
use crate::metrics::log_query_shadow_route_detail_capacity_drop;

const SCHEDULED_JOB_METRICS_BUCKET_WIDTH: Duration = Duration::from_secs(15);
const SCHEDULED_JOB_METRICS_MAX_BUCKETS: usize = 240;
const QUERY_SHADOW_METRICS_BUCKET_WIDTH: Duration = Duration::from_secs(15 * 60);
const QUERY_SHADOW_METRICS_MAX_BUCKETS: usize = 4;
const QUERY_SHADOW_METRICS_SIGNIFICANT_FIGURES: u8 = 1;
const QUERY_SHADOW_TIMING_PERCENTILES: [Percentile; 3] = [90, 99, 100];
const QUERY_SHADOW_DIAGNOSTICS_PER_ROUTE: usize = 8;
const QUERY_SHADOW_DIRECTIONS: usize = 2;

#[derive(Clone, Copy)]
enum QueryShadowMetricScope<'a> {
    Route(UdfType, &'a QueryShadowRouteKey),
    Generation(UdfType, &'a str),
}

#[derive(Clone, Copy)]
enum QueryShadowEvidenceScope<'a> {
    Routes(UdfType, &'a [QueryShadowRouteKey]),
    Generation(UdfType, &'a str),
}

#[derive(Clone, Copy)]
enum QueryShadowReportRetention {
    RouteAndGeneration,
    GenerationOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryShadowReportAcceptance {
    RouteDetail,
    GenerationAggregateOnly,
    InactiveGenerationAggregateOnly,
    Ignored,
}

/// Windowed query-shadow timing percentiles backed by the UDF metric store.
///
/// `wasm_to_primary_ratio` uses numeric ratio scaling: `1.0` is stored as a
/// one-second histogram value and returned as `1.0`. Values outside the metric
/// store's configured histogram range are clamped to that range.
/// `completed_samples` is the exact number of paired executions represented by
/// the timing histograms in each returned bucket.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryShadowTimingPercentiles {
    pub primary_seconds: BTreeMap<Percentile, Timeseries>,
    pub wasm_seconds: BTreeMap<Percentile, Timeseries>,
    pub wasm_to_primary_ratio: BTreeMap<Percentile, Timeseries>,
    pub completed_samples: Vec<(SystemTime, Option<u64>)>,
}

/// Timing statistics for one authenticated registry-generation route and the
/// aggregate population.
///
/// Every percentile map contains exactly the keys `90`, `99`, and `100`.
/// Percentile `100` is the retained-window HDR histogram maximum, subject to
/// histogram quantization; it is not a process-lifetime maximum.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryShadowTimingStats {
    pub route: QueryShadowTimingPercentiles,
    pub aggregate: QueryShadowTimingPercentiles,
}

/// Bounded query-shadow comparison and terminal counters.
///
/// Map keys are fixed internal classifications such as
/// `comparison:result:match` and `terminal:capacity_drop`. They never contain a
/// function path, arguments, identity, values, journal contents, or error text.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryShadowOutcomeStats {
    pub route: BTreeMap<String, Timeseries>,
    pub aggregate: BTreeMap<String, Timeseries>,
}

/// One retained query-shadow timing distribution.
///
/// Percentiles and maximum are in seconds. For the Wasm-to-primary ratio,
/// seconds encode the numeric ratio, so `1.0` means equal execution time.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryShadowTimingSummary {
    pub exact_sample_count: u64,
    pub p50: Option<f64>,
    pub p90: Option<f64>,
    pub p99: Option<f64>,
    pub retained_max: Option<f64>,
}

/// Retained terminal classifications for query-shadow reports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryShadowTerminalCounts {
    pub capacity_drop: u64,
    pub shadow_failure: u64,
    pub timeout: u64,
    pub invalid_shadow: u64,
    pub invalid_primary: u64,
    pub primary_failure: u64,
    pub primary_snapshot_rejected: u64,
    pub unexpected_task_exit: u64,
}

/// Retained fixed stages for terminal verifier failures.
///
/// These counts classify only the backend stage. They do not include error
/// text, function paths, arguments, identities, values, or host-operation
/// data.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryShadowFailureStageCounts {
    pub transaction_setup: u64,
    pub environment_setup: u64,
    pub route_authentication: u64,
    pub module_loading: u64,
    pub wasm_execution: u64,
    pub verifier_execution: u64,
    pub transaction_finalization: u64,
    pub unclassified: u64,
}

/// Retained data-free reasons for terminal verifier failures.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryShadowFailureReasonCounts {
    pub infrastructure: u64,
    pub route_authentication: u64,
    pub module_loading: u64,
    pub runtime_initialization: u64,
    pub initialization_timeout: u64,
    pub execution_timeout: u64,
    pub instruction_budget: u64,
    pub generated_export_dispatch: u64,
    pub guest_memory_limit: u64,
    pub host_owned_bytes_limit: u64,
    pub opaque_handle_limit: u64,
    pub opaque_handle_space_exhausted: u64,
    pub aggregate_memory_limit: u64,
    pub operation_limit: u64,
    pub host_abi_invariant: u64,
    pub hermes_heap_out_of_memory: u64,
    pub wasmtime_trap: u64,
    pub guest_execution: u64,
    pub result_finalization: u64,
    pub runtime_cleanup: u64,
    pub verifier_execution: u64,
    pub transaction_finalization: u64,
    pub unclassified: u64,
}

/// Retained fixed comparison dimensions with one or more mismatches.
///
/// Counts are not mutually exclusive: one completed comparison can increment
/// several dimensions. Field names are fixed and never contain execution data.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryShadowMismatchCounts {
    pub snapshot: u64,
    pub result: u64,
    pub query_journal: u64,
    pub read_dependencies: u64,
    pub invocation_inputs: u64,
    pub host_operation_trace: u64,
    pub host_operation_error: u64,
    pub observed_identity: u64,
    pub observed_time: u64,
    pub observed_rng: u64,
    pub log_lines: u64,
    pub audit_log_lines: u64,
    pub write_set: u64,
}

/// Fixed attribution for lane-local inserted-document identity comparison.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryShadowInsertedDocumentIdentityCounts {
    pub not_applicable: u64,
    pub not_required: u64,
    pub matched: u64,
    pub inconsistent: u64,
    pub unresolved: u64,
}

impl QueryShadowInsertedDocumentIdentityCounts {
    fn total(&self) -> anyhow::Result<u64> {
        self.not_applicable
            .checked_add(self.not_required)
            .and_then(|count| count.checked_add(self.matched))
            .and_then(|count| count.checked_add(self.inconsistent))
            .and_then(|count| count.checked_add(self.unresolved))
            .context("query-shadow inserted-document identity count overflow")
    }
}

impl QueryShadowTerminalCounts {
    fn total(&self) -> anyhow::Result<u64> {
        self.capacity_drop
            .checked_add(self.shadow_failure)
            .and_then(|count| count.checked_add(self.timeout))
            .and_then(|count| count.checked_add(self.invalid_shadow))
            .and_then(|count| count.checked_add(self.invalid_primary))
            .and_then(|count| count.checked_add(self.primary_failure))
            .and_then(|count| count.checked_add(self.primary_snapshot_rejected))
            .and_then(|count| count.checked_add(self.unexpected_task_exit))
            .context("query-shadow terminal count overflow")
    }
}

impl QueryShadowFailureStageCounts {
    fn total(&self) -> anyhow::Result<u64> {
        self.transaction_setup
            .checked_add(self.environment_setup)
            .and_then(|count| count.checked_add(self.route_authentication))
            .and_then(|count| count.checked_add(self.module_loading))
            .and_then(|count| count.checked_add(self.wasm_execution))
            .and_then(|count| count.checked_add(self.verifier_execution))
            .and_then(|count| count.checked_add(self.transaction_finalization))
            .and_then(|count| count.checked_add(self.unclassified))
            .context("query-shadow failure-stage count overflow")
    }
}

impl QueryShadowFailureReasonCounts {
    fn total(&self) -> anyhow::Result<u64> {
        self.infrastructure
            .checked_add(self.route_authentication)
            .and_then(|count| count.checked_add(self.module_loading))
            .and_then(|count| count.checked_add(self.runtime_initialization))
            .and_then(|count| count.checked_add(self.initialization_timeout))
            .and_then(|count| count.checked_add(self.execution_timeout))
            .and_then(|count| count.checked_add(self.instruction_budget))
            .and_then(|count| count.checked_add(self.generated_export_dispatch))
            .and_then(|count| count.checked_add(self.guest_memory_limit))
            .and_then(|count| count.checked_add(self.host_owned_bytes_limit))
            .and_then(|count| count.checked_add(self.opaque_handle_limit))
            .and_then(|count| count.checked_add(self.opaque_handle_space_exhausted))
            .and_then(|count| count.checked_add(self.aggregate_memory_limit))
            .and_then(|count| count.checked_add(self.operation_limit))
            .and_then(|count| count.checked_add(self.host_abi_invariant))
            .and_then(|count| count.checked_add(self.hermes_heap_out_of_memory))
            .and_then(|count| count.checked_add(self.wasmtime_trap))
            .and_then(|count| count.checked_add(self.guest_execution))
            .and_then(|count| count.checked_add(self.result_finalization))
            .and_then(|count| count.checked_add(self.runtime_cleanup))
            .and_then(|count| count.checked_add(self.verifier_execution))
            .and_then(|count| count.checked_add(self.transaction_finalization))
            .and_then(|count| count.checked_add(self.unclassified))
            .context("query-shadow failure-reason count overflow")
    }
}

/// A retention-bounded query-shadow outcome and timing summary.
///
/// `evidence_count` is the number of completed comparisons and terminal
/// execution outcomes. Admission events do not make a route semantically
/// observed. Generated-memory statistics are read separately from the runtime
/// controller and do not depend on comparison sampling.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryShadowEvidenceSummary {
    pub attempt_count: u64,
    pub admitted_count: u64,
    pub capacity_drop_count: u64,
    pub evidence_count: u64,
    pub completed_count: u64,
    pub mismatch_count: u64,
    pub mismatch_counts: QueryShadowMismatchCounts,
    pub inserted_document_identity_counts: QueryShadowInsertedDocumentIdentityCounts,
    pub terminal_count: u64,
    pub terminal_counts: QueryShadowTerminalCounts,
    pub shadow_failure_stage_counts: QueryShadowFailureStageCounts,
    pub shadow_failure_reason_counts: QueryShadowFailureReasonCounts,
    pub primary_seconds: QueryShadowTimingSummary,
    pub wasm_seconds: QueryShadowTimingSummary,
    pub wasm_to_primary_ratio: QueryShadowTimingSummary,
    /// Timing for Wasm-authoritative executions. Kept separate from the
    /// legacy V8-primary fields so a rollout-direction change cannot silently
    /// mix unlike populations.
    pub wasm_primary_seconds: QueryShadowTimingSummary,
    pub v8_verifier_seconds: QueryShadowTimingSummary,
    pub v8_verifier_to_wasm_primary_ratio: QueryShadowTimingSummary,
}

/// One bounded, data-free shadow outcome retained with its comparison
/// dimensions still correlated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryShadowDiagnosticEvidence {
    pub sequence: u64,
    pub recorded_at_unix_milliseconds: u64,
    pub direction: QueryShadowDirection,
    pub outcome: QueryShadowDiagnosticOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryShadowDiagnosticOutcome {
    Memory(udf::wasm_memory::WasmMemoryObservation),
    /// Admission was rejected before a verifier lane started.
    Admission(QueryShadowAdmissionEvent),
    Mismatch(QueryShadowComparison),
    Terminal(QueryShadowTerminal),
}

/// One authenticated registry-generation route with retained query-shadow
/// evidence.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryShadowRouteEvidence {
    pub route_key: QueryShadowRouteKey,
    pub selected_count: u64,
    pub unobserved_count: u64,
    pub summary: QueryShadowEvidenceSummary,
    pub diagnostics: Vec<QueryShadowDiagnosticEvidence>,
}

#[derive(Clone)]
struct RetainedQueryShadowDiagnostic {
    sequence: u64,
    recorded_at: SystemTime,
    udf_type: UdfType,
    route_key: QueryShadowRouteKey,
    direction: QueryShadowDirection,
    outcome: QueryShadowDiagnosticOutcome,
}

/// Route selection from the authenticated active query-shadow registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryShadowRegistrySelection {
    pub udf_type: UdfType,
    pub generation_sha256: String,
    pub selected_route_count: u64,
    pub routes_with_retained_evidence_count: u64,
    pub routes_with_completed_comparison_count: u64,
    pub routes_with_mismatch_count: u64,
    pub routes_with_terminal_count: u64,
    pub unobserved_route_count: u64,
}

/// A page of aggregate and per-route query-shadow evidence.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryShadowEvidencePage {
    pub udf_type: UdfType,
    pub direction: QueryShadowDirection,
    pub aggregate: QueryShadowEvidenceSummary,
    pub selection: Option<QueryShadowRegistrySelection>,
    pub routes: Vec<QueryShadowRouteEvidence>,
    pub next_cursor: Option<QueryShadowRouteKey>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct ShadowMetricRouteKey {
    udf_type: UdfType,
    route_key: QueryShadowRouteKey,
    direction: QueryShadowDirection,
}

impl ShadowMetricRouteKey {
    #[cfg(test)]
    fn new(udf_type: UdfType, route_key: &QueryShadowRouteKey) -> Self {
        Self::in_direction(
            udf_type,
            route_key,
            QueryShadowDirection::V8PrimaryWasmShadow,
        )
    }

    fn in_direction(
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        direction: QueryShadowDirection,
    ) -> Self {
        Self {
            udf_type,
            route_key: route_key.clone(),
            direction,
        }
    }
}

#[derive(Clone)]
struct QueryShadowMetrics {
    store: MetricStore<u32>,
    // Separate stores keep every counter and histogram scoped to its runtime
    // direction, including reports that finish after a policy switch.
    wasm_primary_store: MetricStore<u32>,
    active_generation_sha256: Option<Box<str>>,
    active_routes: HashMap<ShadowMetricRouteKey, SystemTime>,
    route_retention: Duration,
    max_active_routes: usize,
    diagnostics: VecDeque<RetainedQueryShadowDiagnostic>,
    max_diagnostics: usize,
    next_diagnostic_sequence: u64,
}

impl QueryShadowMetrics {
    fn new(base_ts: SystemTime, config: MetricStoreConfig, max_active_routes: usize) -> Self {
        let retention_buckets: u32 = config
            .max_buckets
            .try_into()
            .expect("query-shadow metric retention exceeds u32 buckets");
        let max_diagnostics = max_active_routes
            .checked_mul(QUERY_SHADOW_DIRECTIONS)
            .and_then(|count| count.checked_mul(QUERY_SHADOW_DIAGNOSTICS_PER_ROUTE))
            .expect("query-shadow diagnostic capacity overflow");
        Self {
            store: MetricStore::new(base_ts, config),
            wasm_primary_store: MetricStore::new(base_ts, config),
            active_generation_sha256: None,
            active_routes: HashMap::new(),
            route_retention: config.bucket_width.saturating_mul(retention_buckets),
            max_active_routes,
            diagnostics: VecDeque::with_capacity(max_diagnostics),
            max_diagnostics,
            next_diagnostic_sequence: 0,
        }
    }

    fn store_for_direction(&self, direction: QueryShadowDirection) -> &MetricStore<u32> {
        match direction {
            QueryShadowDirection::V8PrimaryWasmShadow => &self.store,
            QueryShadowDirection::WasmPrimaryV8Shadow => &self.wasm_primary_store,
        }
    }

    fn store_for_direction_mut(
        &mut self,
        direction: QueryShadowDirection,
    ) -> &mut MetricStore<u32> {
        match direction {
            QueryShadowDirection::V8PrimaryWasmShadow => &mut self.store,
            QueryShadowDirection::WasmPrimaryV8Shadow => &mut self.wasm_primary_store,
        }
    }

    fn prune_expired_routes(&mut self, now: SystemTime) {
        self.active_routes
            .retain(|_, last_seen| match now.duration_since(*last_seen) {
                Ok(age) => age < self.route_retention,
                Err(_) => true,
            });
        self.diagnostics.retain(
            |diagnostic| match now.duration_since(diagnostic.recorded_at) {
                Ok(age) => age < self.route_retention,
                Err(_) => true,
            },
        );
    }

    fn record_diagnostic(&mut self, report: &QueryShadowReport, recorded_at: SystemTime) {
        let outcome = match report {
            QueryShadowReport::Memory { observation, .. } if observation.has_anomaly() => {
                QueryShadowDiagnosticOutcome::Memory(observation.clone())
            },
            QueryShadowReport::Admission {
                event:
                    event @ (QueryShadowAdmissionEvent::PendingDrop
                    | QueryShadowAdmissionEvent::CapacityDrop),
                ..
            } => QueryShadowDiagnosticOutcome::Admission(*event),
            QueryShadowReport::Compared { comparison, .. } if !comparison.matches() => {
                QueryShadowDiagnosticOutcome::Mismatch(comparison.clone())
            },
            QueryShadowReport::Terminal { terminal, .. } => {
                QueryShadowDiagnosticOutcome::Terminal(terminal.clone())
            },
            QueryShadowReport::Admission {
                event: QueryShadowAdmissionEvent::Attempt | QueryShadowAdmissionEvent::Admitted,
                ..
            }
            | QueryShadowReport::Compared { .. }
            | QueryShadowReport::Memory { .. } => return,
        };
        self.next_diagnostic_sequence = self
            .next_diagnostic_sequence
            .checked_add(1)
            .expect("query-shadow diagnostic sequence overflow");
        self.diagnostics.push_back(RetainedQueryShadowDiagnostic {
            sequence: self.next_diagnostic_sequence,
            recorded_at,
            udf_type: report.udf_type(),
            route_key: report.route_key().clone(),
            direction: report.direction(),
            outcome,
        });

        let route_diagnostic_count = self
            .diagnostics
            .iter()
            .filter(|diagnostic| {
                diagnostic.udf_type == report.udf_type()
                    && diagnostic.route_key == *report.route_key()
                    && diagnostic.direction == report.direction()
            })
            .count();
        if route_diagnostic_count > QUERY_SHADOW_DIAGNOSTICS_PER_ROUTE {
            let same_route = |diagnostic: &RetainedQueryShadowDiagnostic| {
                diagnostic.udf_type == report.udf_type()
                    && diagnostic.route_key == *report.route_key()
                    && diagnostic.direction == report.direction()
            };
            // Admission and memory pressure are useful context, but must not
            // evict the only retained terminal or semantic-mismatch explanation
            // for a route. Keep the fixed bound by evicting the oldest admission
            // first, then memory diagnostics; if only semantic outcomes remain,
            // evict the oldest outcome as usual.
            let position = self
                .diagnostics
                .iter()
                .position(|diagnostic| {
                    same_route(diagnostic)
                        && matches!(
                            diagnostic.outcome,
                            QueryShadowDiagnosticOutcome::Admission(_)
                        )
                })
                .or_else(|| {
                    self.diagnostics.iter().position(|diagnostic| {
                        same_route(diagnostic)
                            && matches!(diagnostic.outcome, QueryShadowDiagnosticOutcome::Memory(_))
                    })
                })
                .or_else(|| self.diagnostics.iter().position(same_route))
                .expect("query-shadow route diagnostic disappeared");
            self.diagnostics.remove(position);
        }
        while self.diagnostics.len() > self.max_diagnostics {
            self.diagnostics.pop_front();
        }
    }

    fn diagnostics_for_route(
        &self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        direction: QueryShadowDirection,
    ) -> Vec<QueryShadowDiagnosticEvidence> {
        self.diagnostics
            .iter()
            .filter(|diagnostic| {
                diagnostic.udf_type == udf_type
                    && diagnostic.route_key == *route_key
                    && diagnostic.direction == direction
            })
            .map(|diagnostic| QueryShadowDiagnosticEvidence {
                sequence: diagnostic.sequence,
                recorded_at_unix_milliseconds: u64::try_from(
                    diagnostic
                        .recorded_at
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("query-shadow diagnostic predates the Unix epoch")
                        .as_millis(),
                )
                .expect("query-shadow diagnostic timestamp exceeds u64 milliseconds"),
                direction: diagnostic.direction,
                outcome: diagnostic.outcome.clone(),
            })
            .collect()
    }

    fn activate_generation(&mut self, generation_sha256: &str, ts: SystemTime) {
        if self.active_generation_sha256.as_deref() == Some(generation_sha256) {
            return;
        }
        self.active_generation_sha256 = Some(generation_sha256.into());
        // Only the authenticated current generation is observable. Starting a
        // new metric timeline on rotation also keeps retained route names and
        // histograms within the configured route-detail bound.
        self.reset_timeline(ts);
    }

    fn reset_timeline(&mut self, ts: SystemTime) {
        self.active_routes.clear();
        self.diagnostics.clear();
        self.store.reset_timeline(ts);
        self.wasm_primary_store.reset_timeline(ts);
    }

    fn has_route_detail_capacity(
        &mut self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        direction: QueryShadowDirection,
        ts: SystemTime,
    ) -> bool {
        self.prune_expired_routes(ts);
        self.active_routes
            .contains_key(&ShadowMetricRouteKey::in_direction(
                udf_type, route_key, direction,
            ))
            || self
                .active_routes
                .keys()
                .filter(|key| key.direction == direction)
                .count()
                < self.max_active_routes
    }

    fn accept_route(
        &mut self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        direction: QueryShadowDirection,
        ts: SystemTime,
    ) {
        let route_key = ShadowMetricRouteKey::in_direction(udf_type, route_key, direction);
        if let Some(last_seen) = self.active_routes.get_mut(&route_key) {
            *last_seen = (*last_seen).max(ts);
            return;
        }
        self.active_routes.insert(route_key, ts);
    }

    fn retained_window(&self, now: SystemTime) -> anyhow::Result<MetricsWindow> {
        let start = now
            .checked_sub(self.route_retention)
            .context("query-shadow retained window precedes the system clock epoch")?;
        Ok(MetricsWindow {
            start,
            end: now,
            num_buckets: QUERY_SHADOW_METRICS_MAX_BUCKETS,
        })
    }
}

fn query_shadow_metrics_config() -> MetricStoreConfig {
    MetricStoreConfig {
        bucket_width: QUERY_SHADOW_METRICS_BUCKET_WIDTH,
        max_buckets: QUERY_SHADOW_METRICS_MAX_BUCKETS,
        histogram_min_duration: *knobs::UDF_METRICS_MIN_DURATION,
        histogram_max_duration: *knobs::UDF_METRICS_MAX_DURATION,
        histogram_significant_figures: QUERY_SHADOW_METRICS_SIGNIFICANT_FIGURES,
    }
}

pub enum OutstandingFunctionState {
    Running,
    Queued,
}

/// A function's execution is summarized by this structure and stored in the
/// UdfExecutionLog
#[derive(Debug, Clone)]
pub struct FunctionExecution {
    pub params: UdfParams,

    /// When we return the function result and log it. For cached
    // queries this can be long after the query was actually
    // executed.
    pub unix_timestamp: UnixTimestamp,

    /// When the function ran. For cached queries this can be long before
    // this execution is requested and is logged.
    pub execution_timestamp: UnixTimestamp,

    /// How this UDF was executed, with read-only or read-write permissions
    pub udf_type: UdfType,

    /// Log lines that the UDF emitted via the `Console` API.
    pub log_lines: LogLines,

    /// Which tables were read or written to in the UDF execution?
    pub tables_touched: WithHeapSize<BTreeMap<TableName, TableStats>>,

    /// Was this UDF execution computed from scratch or cached?
    pub cached_result: bool,
    /// How long (in seconds) did executing this UDF take?
    pub execution_time: f64,
    /// How long (in seconds) did the user's code execute?
    pub user_execution_time: Option<Duration>,

    /// Who called this UDF?
    pub caller: FunctionCaller,

    /// What type of environment was this UDF run in? This can be `Invalid` if
    /// the user specified an invalid path for an action, and then we won't
    /// know whether the action was intended to run in V8 or Node.
    pub environment: ModuleEnvironment,

    /// What syscalls did this function execute?
    pub syscall_trace: SyscallTrace,

    /// Usage statistics for this instance
    pub usage_stats: AggregatedFunctionUsageStats,
    pub memory_used_mb: u64,
    /// Size of the serialized arguments in bytes, excluding HTTP actions.
    pub args_bytes: Option<u64>,
    /// Size of the returned value in bytes if the function execution was
    /// successful, excluding HTTP actions.
    pub return_bytes: Option<u64>,

    /// The Convex NPM package version pushed with the module version executed.
    pub udf_server_version: Option<semver::Version>,

    /// The identity under which the udf was executed. Must be inert - since it
    /// can only be for logging purposes - not giving any authorization
    /// power.
    pub identity: InertIdentity,

    pub context: ExecutionContext,

    /// The length of the mutation queue at the time the mutation was executed.
    /// Only applicable for mutations.
    pub mutation_queue_length: Option<usize>,

    // Number of retries prior to a successful execution. Only applicable for mutations.
    pub mutation_retry_count: Option<usize>,

    // If this execution resulted in an OCC error, this will be Some.
    pub occ_info: Option<OccInfo>,

    /// Whether this function will be retried (e.g. a mutation that OCCs or hits
    /// write throughput limits)
    pub will_retry: bool,

    // Whether this was a fresh call or a rerun. Only applicable for queries.
    pub query_invocation: Option<QueryInvocation>,
}

impl HeapSize for FunctionExecution {
    fn heap_size(&self) -> usize {
        self.params.heap_size()
            + self.log_lines.heap_size()
            + self.tables_touched.heap_size()
            + self.syscall_trace.heap_size()
            + self.context.heap_size()
    }
}

impl FunctionExecution {
    fn identifier(&self) -> UdfIdentifier {
        match &self.params {
            UdfParams::Function { identifier, .. } => UdfIdentifier::Function(identifier.clone()),
            UdfParams::Http { identifier, .. } => UdfIdentifier::Http(identifier.clone()),
        }
    }

    fn event_source(
        &self,
        sub_function_path: Option<&CanonicalizedComponentFunctionPath>,
    ) -> FunctionEventSource {
        let cached = if self.udf_type == UdfType::Query {
            Some(self.cached_result)
        } else {
            None
        };
        let (component_path, udf_path) = match sub_function_path {
            Some(path) => (path.component.clone(), path.udf_path.to_string()),
            None => {
                let udf_id = self.params.identifier_str();
                let component_path = match &self.params {
                    UdfParams::Function { identifier, .. } => identifier.component.clone(),
                    // TODO(ENG-7612): Support HTTP actions in components.
                    UdfParams::Http { .. } => ComponentPath::root(),
                };
                (component_path, udf_id)
            },
        };

        FunctionEventSource {
            component_path,
            udf_path,
            udf_type: self.udf_type,
            module_environment: self.environment,
            cached,
            context: self.context.clone(),
            mutation_queue_length: self.mutation_queue_length,
            mutation_retry_count: self.mutation_retry_count,
        }
    }

    fn console_log_events_for_log_line(
        &self,
        log_line: &LogLine,
        sub_function_path: Option<&CanonicalizedComponentFunctionPath>,
    ) -> Vec<LogEvent> {
        match log_line {
            LogLine::Structured(log_line) => {
                vec![LogEvent {
                    timestamp: self.unix_timestamp,
                    event: StructuredLogEvent::Console {
                        source: self.event_source(sub_function_path),
                        log_line: log_line.clone(),
                    },
                }]
            },
            LogLine::SubFunction { path, log_lines } => log_lines
                .iter()
                .flat_map(|log_line| self.console_log_events_for_log_line(log_line, Some(path)))
                .collect(),
        }
    }

    fn console_log_events(&self) -> Vec<LogEvent> {
        self.log_lines
            .iter()
            .flat_map(|line| self.console_log_events_for_log_line(line, None))
            .collect()
    }

    fn udf_execution_record_log_events(&self) -> anyhow::Result<Vec<LogEvent>> {
        let execution_time = Duration::from_secs_f64(self.execution_time);

        let mut events = vec![LogEvent {
            timestamp: self.unix_timestamp,
            event: StructuredLogEvent::FunctionExecution {
                source: self.event_source(None),
                error: self.params.err().cloned(),
                execution_time,
                user_execution_time: self.user_execution_time,
                occ_info: self.occ_info.clone(),
                will_retry: self.will_retry,
                scheduler_info: match self.caller {
                    FunctionCaller::Scheduler { job_id, .. } => Some(SchedulerInfo {
                        job_id: job_id.to_string(),
                    }),
                    _ => None,
                },
                run_reason: FunctionRunReason::new(&self.caller, self.query_invocation),
                usage_stats: log_streaming::AggregatedFunctionUsageStats {
                    database_read_bytes: self.usage_stats.database_read_bytes,
                    database_write_bytes: self.usage_stats.database_write_bytes,
                    database_io_read_bytes: self.usage_stats.database_io_read_bytes,
                    database_io_write_bytes: self.usage_stats.database_io_write_bytes,
                    database_read_documents: self.usage_stats.database_read_documents,
                    database_write_documents: self.usage_stats.database_write_documents,
                    database_write_index_rows: self.usage_stats.database_write_index_rows,
                    storage_read_bytes: self.usage_stats.storage_read_bytes,
                    storage_write_bytes: self.usage_stats.storage_write_bytes,
                    vector_index_read_bytes: self.usage_stats.vector_index_read_bytes,
                    vector_index_write_bytes: self.usage_stats.vector_index_write_bytes,
                    text_index_write_query_bytes: self.usage_stats.text_index_write_query_bytes,
                    text_index_query_bytes: self.usage_stats.text_index_query_bytes,
                    vector_index_read_query_bytes: self.usage_stats.vector_index_read_query_bytes,
                    vector_index_write_query_bytes: self.usage_stats.vector_index_write_query_bytes,
                    network_egress_bytes: self.usage_stats.network_egress_bytes,
                    memory_used_mb: self.memory_used_mb,
                    args_bytes: self.args_bytes,
                    return_bytes: self.return_bytes,
                    audit_log_egress_bytes: self.usage_stats.audit_log_egress_bytes,
                },
            },
        }];

        if let Some(err) = self.params.err() {
            events.push(LogEvent {
                timestamp: self.unix_timestamp,
                event: StructuredLogEvent::Exception {
                    error: err.clone(),
                    user_identifier: self.identity.user_identifier().cloned(),
                    source: self.event_source(None),
                    udf_server_version: self.udf_server_version.clone(),
                    request_metadata: self.context.request_metadata.clone(),
                },
            });
        }

        Ok(events)
    }
}

#[derive(Debug, Clone)]
pub struct FunctionExecutionProgress {
    /// Log lines that the UDF emitted via the `Console` API.
    pub log_lines: LogLines,

    pub event_source: FunctionEventSource,
    pub function_start_timestamp: UnixTimestamp,
}

impl HeapSize for FunctionExecutionProgress {
    fn heap_size(&self) -> usize {
        self.log_lines.heap_size() + self.event_source.heap_size()
    }
}

impl FunctionExecutionProgress {
    fn console_log_events_for_log_line(
        &self,
        log_line: &LogLine,
        sub_function_path: Option<&CanonicalizedComponentFunctionPath>,
    ) -> Vec<LogEvent> {
        match log_line {
            LogLine::Structured(log_line) => {
                let mut event_source = self.event_source.clone();
                if let Some(sub_function_path) = sub_function_path {
                    event_source.component_path = sub_function_path.component.clone();
                    event_source.udf_path = sub_function_path.udf_path.to_string();
                };
                vec![LogEvent {
                    timestamp: log_line.timestamp,
                    event: StructuredLogEvent::Console {
                        source: event_source,
                        log_line: log_line.clone(),
                    },
                }]
            },
            LogLine::SubFunction { path, log_lines } => log_lines
                .iter()
                .flat_map(|log_line| self.console_log_events_for_log_line(log_line, Some(path)))
                .collect(),
        }
    }

    fn console_log_events(&self) -> Vec<LogEvent> {
        self.log_lines
            .iter()
            .flat_map(|line| self.console_log_events_for_log_line(line, None))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub enum FunctionExecutionPart {
    Completion(FunctionExecution),
    Progress(FunctionExecutionProgress),
}

impl HeapSize for FunctionExecutionPart {
    fn heap_size(&self) -> usize {
        match self {
            FunctionExecutionPart::Completion(i) => i.heap_size(),
            FunctionExecutionPart::Progress(i) => i.heap_size(),
        }
    }
}

#[derive(Clone)]
pub struct ActionCompletion {
    pub outcome: ValidatedActionOutcome,
    pub execution_time: Duration,
    pub environment: ModuleEnvironment,
    pub memory_in_mb: u64,
    pub context: ExecutionContext,
    pub unix_timestamp: UnixTimestamp,
    pub caller: FunctionCaller,
    pub log_lines: LogLines,
}

impl ActionCompletion {
    pub fn log_lines(&self) -> &LogLines {
        &self.log_lines
    }
}

#[derive(Debug, Clone)]
pub enum UdfParams {
    Function {
        // Avoid storing the actual result because the json can be quite large.
        // Instead only store the error if there was one. If error is None, the
        // function succeeded.
        error: Option<JsError>,
        /// Path of the component and UDF that was executed.
        identifier: CanonicalizedComponentFunctionPath,
    },
    Http {
        result: Result<HttpActionStatusCode, JsError>,
        identifier: HttpActionRoute,
    },
}

impl HeapSize for UdfParams {
    fn heap_size(&self) -> usize {
        match self {
            UdfParams::Function { error, identifier } => error.heap_size() + identifier.heap_size(),
            UdfParams::Http { result, identifier } => result.heap_size() + identifier.heap_size(),
        }
    }
}

impl UdfParams {
    pub fn is_err(&self) -> bool {
        match self {
            UdfParams::Function { error, .. } => error.is_some(),
            UdfParams::Http { result, .. } => result.is_err(),
        }
    }

    fn err(&self) -> Option<&JsError> {
        match self {
            UdfParams::Function { error: Some(e), .. } => Some(e),
            UdfParams::Http { result: Err(e), .. } => Some(e),
            _ => None,
        }
    }

    pub fn identifier_str(&self) -> String {
        match self {
            Self::Function { identifier, .. } => identifier.udf_path.clone().strip().to_string(),
            Self::Http { identifier, .. } => identifier.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpActionStatusCode(pub StatusCode);

impl HeapSize for HttpActionStatusCode {
    fn heap_size(&self) -> usize {
        // StatusCode is a wrapper around u16
        0
    }
}

impl From<HttpActionStatusCode> for serde_json::Value {
    fn from(value: HttpActionStatusCode) -> Self {
        json!({
            "status": value.0.as_u16().to_string(),
        })
    }
}

pub enum TrackUsage {
    Track(FunctionUsageTracker),
    // We don't count usage for system errors since they're not the user's fault
    SystemError,
}

#[derive(PartialEq)]
pub enum UdfRate {
    Invocations,
    Errors,
    CacheHits,
    CacheMisses,
    SubscriptionInvalidations,
}

impl FromStr for UdfRate {
    type Err = anyhow::Error;

    fn from_str(r: &str) -> anyhow::Result<Self> {
        let udf_rate = match r {
            "invocations" => UdfRate::Invocations,
            "errors" => UdfRate::Errors,
            "cacheHits" => UdfRate::CacheHits,
            "cacheMisses" => UdfRate::CacheMisses,
            "subscriptionInvalidations" => UdfRate::SubscriptionInvalidations,
            _ => anyhow::bail!("Invalid UDF rate: {}", r),
        };
        Ok(udf_rate)
    }
}

pub enum TableRate {
    RowsRead,
    RowsWritten,
}

impl FromStr for TableRate {
    type Err = anyhow::Error;

    fn from_str(r: &str) -> anyhow::Result<Self> {
        let table_rate = match r {
            "rowsRead" => TableRate::RowsRead,
            "rowsWritten" => TableRate::RowsWritten,
            _ => anyhow::bail!("Invalid table rate: {}", r),
        };
        Ok(table_rate)
    }
}

#[derive(Clone)]
pub struct FunctionExecutionLog<RT: Runtime> {
    inner: Arc<Mutex<Inner<RT>>>,
    scheduled_job_metrics: Arc<Mutex<MetricStore>>,
    query_shadow_metrics: Arc<Mutex<QueryShadowMetrics>>,
    usage_tracking: UsageCounter,
    rt: RT,
    concurrency_stats_logger: Arc<Mutex<Box<dyn SpawnHandle>>>,
}

impl<RT: Runtime> FunctionExecutionLog<RT> {
    pub fn new(rt: RT, usage_tracking: UsageCounter, log_manager: Arc<dyn LogSender>) -> Self {
        let base_ts = rt.system_time();
        let metrics_config = MetricStoreConfig {
            bucket_width: *knobs::UDF_METRICS_BUCKET_WIDTH,
            max_buckets: *knobs::UDF_METRICS_MAX_BUCKETS,
            histogram_min_duration: *knobs::UDF_METRICS_MIN_DURATION,
            histogram_max_duration: *knobs::UDF_METRICS_MAX_DURATION,
            histogram_significant_figures: *knobs::UDF_METRICS_SIGNIFICANT_FIGURES,
        };
        let scheduled_job_metrics_config = MetricStoreConfig {
            bucket_width: SCHEDULED_JOB_METRICS_BUCKET_WIDTH,
            max_buckets: SCHEDULED_JOB_METRICS_MAX_BUCKETS,
            ..metrics_config
        };
        let scheduled_job_metrics = Arc::new(Mutex::new(MetricStore::new(
            base_ts,
            scheduled_job_metrics_config,
        )));
        let query_shadow_metrics = Arc::new(Mutex::new(QueryShadowMetrics::new(
            base_ts,
            query_shadow_metrics_config(),
            *knobs::APPLICATION_STATIC_HERMES_SHADOW_MAX_ACTIVE_ROUTES,
        )));
        let inner = Arc::new(Mutex::new(Inner {
            rt: rt.clone(),
            num_execution_completions: 0,
            log: WithHeapSize::default(),
            log_waiters: vec![].into(),
            log_manager,
            metrics: MetricStore::new(base_ts, metrics_config),
        }));

        let inner_for_task = inner.clone();

        // Spawn a background task to periodically send concurrency stats
        let concurrency_stats_logger = Arc::new(Mutex::new(rt.spawn(
            "concurrency_stats_logger",
            async move {
                let runtime = inner_for_task.lock().rt.clone();
                loop {
                    let bucket_width = *knobs::UDF_METRICS_BUCKET_WIDTH;
                    runtime.wait(bucket_width).await;

                    let now = runtime.system_time();

                    // Query all outstanding_functions gauges to get current concurrency stats
                    let inner = inner_for_task.lock();
                    let metrics = inner.metrics.clone();
                    drop(inner);

                    let start_time = now - bucket_width;

                    // Get stats for current and previous buckets
                    let current_stats =
                        Self::query_all_concurrency_stats(&metrics, start_time, now);
                    let prev_start_time = start_time - bucket_width;
                    let previous_stats =
                        Self::query_all_concurrency_stats(&metrics, prev_start_time, start_time);

                    // Only send event if stats have changed
                    if current_stats != previous_stats {
                        let (query, mutation, action, node_action, http_action) = current_stats;
                        let event = LogEvent {
                            timestamp: UnixTimestamp::from_system_time(start_time)
                                .unwrap_or_else(|| UnixTimestamp::from_system_time(now).unwrap()),
                            event: StructuredLogEvent::ConcurrencyStats {
                                query,
                                mutation,
                                action,
                                node_action,
                                http_action,
                            },
                        };

                        let inner = inner_for_task.lock();
                        inner.log_manager.send_logs(vec![event]);
                    }
                }
            },
        )));

        Self {
            inner,
            scheduled_job_metrics,
            query_shadow_metrics,
            rt,
            usage_tracking,
            concurrency_stats_logger,
        }
    }

    pub fn shutdown(&self) {
        self.concurrency_stats_logger.lock().shutdown();
    }

    pub async fn log_query(
        &self,
        outcome: &UdfOutcome,
        tables_touched: BTreeMap<TableName, TableStats>,
        was_cached: bool,
        execution_time: Duration,
        caller: FunctionCaller,
        usage_tracking: FunctionUsageTracker,
        context: ExecutionContext,
        query_invocation: QueryInvocation,
    ) {
        self._log_query(
            outcome,
            tables_touched,
            was_cached,
            execution_time,
            caller,
            TrackUsage::Track(usage_tracking),
            context,
            query_invocation,
        )
        .await
    }

    pub async fn log_query_system_error(
        &self,
        e: &anyhow::Error,
        path: CanonicalizedComponentFunctionPath,
        arguments: SerializedArgs,
        identity: InertIdentity,
        start: tokio::time::Instant,
        caller: FunctionCaller,
        context: ExecutionContext,
        query_invocation: QueryInvocation,
    ) -> anyhow::Result<()> {
        // TODO: We currently synthesize a `UdfOutcome` for
        // an internal system error. If we decide we want to keep internal system errors
        // in the UDF execution log, we may want to plumb through stuff like log lines.
        let outcome = UdfOutcome::from_error(
            JsError::from_error_ref(e),
            path,
            arguments,
            identity,
            self.rt.clone(),
            None,
        )?;
        self._log_query(
            &outcome,
            BTreeMap::new(),
            false,
            start.elapsed(),
            caller,
            TrackUsage::SystemError,
            context,
            query_invocation,
        )
        .await;
        Ok(())
    }

    #[fastrace::trace]
    async fn _log_query(
        &self,
        outcome: &UdfOutcome,
        tables_touched: BTreeMap<TableName, TableStats>,
        was_cached: bool,
        execution_time: Duration,
        caller: FunctionCaller,
        usage: TrackUsage,
        context: ExecutionContext,
        query_invocation: QueryInvocation,
    ) {
        let aggregated = match usage {
            TrackUsage::Track(usage_tracker) => {
                let usage_stats = usage_tracker.gather_user_stats();
                let aggregated = usage_stats.aggregate();
                self.usage_tracking
                    .track_call(
                        UdfIdentifier::Function(outcome.path.clone()),
                        context.execution_id,
                        context.request_id.clone(),
                        if was_cached {
                            CallType::CachedQuery
                        } else {
                            CallType::UncachedQuery {
                                duration: execution_time,
                                user_execution_time: outcome.user_execution_time,
                                memory_in_mb: outcome.memory_in_mb,
                            }
                        },
                        outcome.result.is_ok(),
                        usage_stats,
                    )
                    .await;
                aggregated
            },
            TrackUsage::SystemError => AggregatedFunctionUsageStats::default(),
        };
        if outcome.path.is_system() {
            return;
        }
        let args_bytes = Some(outcome.arguments.heap_size() as u64);
        let return_bytes = outcome.result.as_ref().ok().map(|v| v.heap_size() as u64);
        let execution = FunctionExecution {
            params: UdfParams::Function {
                error: match &outcome.result {
                    Ok(_) => None,
                    Err(e) => Some(e.clone()),
                },
                identifier: outcome.path.clone(),
            },
            unix_timestamp: self.rt.unix_timestamp(),
            execution_timestamp: outcome.unix_timestamp,
            mutation_queue_length: None,
            udf_type: UdfType::Query,
            log_lines: outcome.log_lines.clone(),
            tables_touched: tables_touched.into(),
            cached_result: was_cached,
            execution_time: execution_time.as_secs_f64(),
            user_execution_time: outcome.user_execution_time,
            caller,
            environment: ModuleEnvironment::Isolate,
            syscall_trace: outcome.syscall_trace.clone(),
            usage_stats: aggregated,
            memory_used_mb: outcome.memory_in_mb,
            args_bytes,
            return_bytes,
            udf_server_version: outcome.udf_server_version.clone(),
            identity: outcome.identity.clone(),
            context,
            mutation_retry_count: None,
            occ_info: None,
            will_retry: false,
            query_invocation: Some(query_invocation),
        };
        self.log_execution(execution, true, true);
    }

    pub async fn log_mutation(
        &self,
        outcome: ValidatedUdfOutcome,
        tables_touched: BTreeMap<TableName, TableStats>,
        execution_time: Duration,
        caller: FunctionCaller,
        usage: FunctionUsageTracker,
        context: ExecutionContext,
        mutation_queue_length: Option<usize>,
        mutation_retry_count: usize,
    ) {
        self._log_mutation(
            outcome,
            tables_touched,
            execution_time,
            caller,
            TrackUsage::Track(usage),
            context,
            None,
            mutation_queue_length,
            mutation_retry_count,
            false,
        )
        .await
    }

    pub async fn log_mutation_system_error(
        &self,
        e: &anyhow::Error,
        path: CanonicalizedComponentFunctionPath,
        arguments: SerializedArgs,
        identity: InertIdentity,
        start: tokio::time::Instant,
        caller: FunctionCaller,
        context: ExecutionContext,
        mutation_queue_length: Option<usize>,
        mutation_retry_count: usize,
    ) -> anyhow::Result<()> {
        // TODO: We currently synthesize a `UdfOutcome` for
        // an internal system error. If we decide we want to keep internal system errors
        // in the UDF execution log, we may want to plumb through stuff like log lines.
        let outcome = ValidatedUdfOutcome::from_error(
            JsError::from_error_ref(e),
            path,
            arguments,
            identity,
            self.rt.clone(),
            None,
        )?;
        self._log_mutation(
            outcome,
            BTreeMap::new(),
            start.elapsed(),
            caller,
            TrackUsage::SystemError,
            context,
            None,
            mutation_queue_length,
            mutation_retry_count,
            false,
        )
        .await;
        Ok(())
    }

    pub async fn log_mutation_write_throughput_error(
        &self,
        e: &anyhow::Error,
        path: CanonicalizedComponentFunctionPath,
        arguments: SerializedArgs,
        identity: InertIdentity,
        start: tokio::time::Instant,
        caller: FunctionCaller,
        context: ExecutionContext,
        mutation_queue_length: Option<usize>,
        mutation_retry_count: usize,
        will_retry: bool,
    ) -> anyhow::Result<()> {
        let outcome = ValidatedUdfOutcome::from_error(
            JsError::from_error_ref(e),
            path,
            arguments,
            identity,
            self.rt.clone(),
            None,
        )?;
        self._log_mutation(
            outcome,
            Default::default(),
            start.elapsed(),
            caller,
            TrackUsage::SystemError,
            context,
            None,
            mutation_queue_length,
            mutation_retry_count,
            will_retry,
        )
        .await;
        Ok(())
    }

    pub async fn log_mutation_occ_error(
        &self,
        outcome: ValidatedUdfOutcome,
        tables_touched: BTreeMap<TableName, TableStats>,
        execution_time: Duration,
        caller: FunctionCaller,
        usage: FunctionUsageTracker,
        context: ExecutionContext,
        mut occ_info: OccInfo,
        mutation_queue_length: Option<usize>,
        mutation_retry_count: usize,
        will_retry: bool,
    ) {
        occ_info.retry_count = Some(mutation_retry_count as u64);
        self._log_mutation(
            outcome,
            tables_touched,
            execution_time,
            caller,
            TrackUsage::Track(usage),
            context,
            Some(occ_info),
            mutation_queue_length,
            mutation_retry_count,
            will_retry,
        )
        .await;
    }

    async fn _log_mutation(
        &self,
        outcome: ValidatedUdfOutcome,
        tables_touched: BTreeMap<TableName, TableStats>,
        execution_time: Duration,
        caller: FunctionCaller,
        usage: TrackUsage,
        context: ExecutionContext,
        occ_info: Option<OccInfo>,
        mutation_queue_length: Option<usize>,
        mutation_retry_count: usize,
        will_retry: bool,
    ) {
        let aggregated = match usage {
            TrackUsage::Track(usage_tracker) => {
                let usage_stats = usage_tracker.gather_user_stats();
                let aggregated = usage_stats.aggregate();
                self.usage_tracking
                    .track_call(
                        UdfIdentifier::Function(outcome.path.clone()),
                        context.execution_id,
                        context.request_id.clone(),
                        CallType::Mutation {
                            duration: execution_time,
                            user_execution_time: outcome.user_execution_time,
                            memory_in_mb: outcome.memory_in_mb,
                            occ_info: occ_info.clone(),
                        },
                        outcome.result.is_ok(),
                        usage_stats,
                    )
                    .await;
                aggregated
            },
            TrackUsage::SystemError => AggregatedFunctionUsageStats::default(),
        };
        if outcome.path.udf_path.is_system() {
            return;
        }
        let args_bytes = Some(outcome.arguments.heap_size() as u64);
        let return_bytes = outcome.result.as_ref().ok().map(|v| v.heap_size() as u64);
        let execution = FunctionExecution {
            params: UdfParams::Function {
                error: outcome.result.err(),
                identifier: outcome.path,
            },
            unix_timestamp: self.rt.unix_timestamp(),
            mutation_queue_length,
            execution_timestamp: outcome.unix_timestamp,
            udf_type: UdfType::Mutation,
            log_lines: outcome.log_lines,
            tables_touched: tables_touched.into(),
            cached_result: false,
            execution_time: execution_time.as_secs_f64(),
            user_execution_time: outcome.user_execution_time,
            caller,
            environment: ModuleEnvironment::Isolate,
            syscall_trace: outcome.syscall_trace,
            usage_stats: aggregated,
            memory_used_mb: outcome.memory_in_mb,
            args_bytes,
            return_bytes,
            udf_server_version: outcome.udf_server_version,
            identity: outcome.identity,
            context,
            mutation_retry_count: Some(mutation_retry_count),
            occ_info,
            will_retry,
            query_invocation: None,
        };
        self.log_execution(execution, true, true);
    }

    pub async fn log_action(&self, completion: ActionCompletion, usage: FunctionUsageTracker) {
        self._log_action(completion, TrackUsage::Track(usage)).await
    }

    pub async fn log_action_system_error(
        &self,
        e: &anyhow::Error,
        path: CanonicalizedComponentFunctionPath,
        arguments: SerializedArgs,
        identity: InertIdentity,
        start: tokio::time::Instant,
        caller: FunctionCaller,
        log_lines: LogLines,
        context: ExecutionContext,
    ) -> anyhow::Result<()> {
        // Synthesize an `ActionCompletion` for system errors.
        let unix_timestamp = self.rt.unix_timestamp();
        let completion = ActionCompletion {
            outcome: ValidatedActionOutcome::from_system_error(
                path,
                arguments,
                identity,
                unix_timestamp,
                e,
            ),
            execution_time: start.elapsed(),
            environment: ModuleEnvironment::Invalid,
            memory_in_mb: 0,
            context,
            unix_timestamp: self.rt.unix_timestamp(),
            caller,
            log_lines,
        };
        self._log_action(completion, TrackUsage::SystemError).await;
        Ok(())
    }

    async fn _log_action(&self, completion: ActionCompletion, usage: TrackUsage) {
        let outcome = completion.outcome;
        let log_lines = completion.log_lines;
        let aggregated = match usage {
            TrackUsage::Track(usage_tracker) => {
                let usage_stats = usage_tracker.gather_user_stats();
                let aggregated = usage_stats.aggregate();
                self.usage_tracking
                    .track_call(
                        UdfIdentifier::Function(outcome.path.clone()),
                        completion.context.execution_id,
                        completion.context.request_id.clone(),
                        CallType::Action {
                            env: completion.environment,
                            duration: completion.execution_time,
                            user_execution_time: outcome.user_execution_time,
                            memory_in_mb: completion.memory_in_mb,
                        },
                        outcome.result.is_ok(),
                        usage_stats,
                    )
                    .await;
                aggregated
            },
            TrackUsage::SystemError => AggregatedFunctionUsageStats::default(),
        };
        if outcome.path.udf_path.is_system() {
            return;
        }
        let args_bytes = Some(outcome.arguments.heap_size() as u64);
        let return_bytes = outcome.result.as_ref().ok().map(|v| v.heap_size() as u64);
        let execution = FunctionExecution {
            params: UdfParams::Function {
                error: outcome.result.err(),
                identifier: outcome.path,
            },
            unix_timestamp: self.rt.unix_timestamp(),
            mutation_queue_length: None,
            execution_timestamp: outcome.unix_timestamp,
            udf_type: UdfType::Action,
            log_lines,
            tables_touched: WithHeapSize::default(),
            cached_result: false,
            execution_time: completion.execution_time.as_secs_f64(),
            user_execution_time: outcome.user_execution_time,
            caller: completion.caller,
            environment: completion.environment,
            syscall_trace: outcome.syscall_trace,
            usage_stats: aggregated,
            memory_used_mb: completion.memory_in_mb,
            args_bytes,
            return_bytes,
            udf_server_version: outcome.udf_server_version,
            identity: outcome.identity,
            context: completion.context,
            mutation_retry_count: None,
            occ_info: None,
            will_retry: false,
            query_invocation: None,
        };
        self.log_execution(execution, /* send_console_events */ false, true)
    }

    pub fn log_action_progress(
        &self,
        path: CanonicalizedComponentFunctionPath,
        unix_timestamp: UnixTimestamp,
        context: ExecutionContext,
        log_lines: LogLines,
        module_environment: ModuleEnvironment,
    ) {
        if path.is_system() {
            return;
        }
        let event_source = FunctionEventSource {
            component_path: path.component,
            udf_path: path.udf_path.strip().to_string(),
            udf_type: UdfType::Action,
            module_environment,
            cached: Some(false),
            context,
            mutation_queue_length: None,
            mutation_retry_count: None,
        };

        self.log_execution_progress(log_lines, event_source, unix_timestamp)
    }

    pub async fn log_http_action(
        &self,
        outcome: HttpActionOutcome,
        result: Result<HttpActionStatusCode, JsError>,
        log_lines: LogLines,
        execution_time: Duration,
        caller: FunctionCaller,
        usage: FunctionUsageTracker,
        context: ExecutionContext,
        response_sha256: Sha256Digest,
    ) {
        self._log_http_action(
            outcome,
            result,
            log_lines,
            execution_time,
            caller,
            TrackUsage::Track(usage),
            context,
            response_sha256,
        )
        .await
    }

    pub async fn log_http_action_system_error(
        &self,
        error: &anyhow::Error,
        http_request: HttpActionRequestHead,
        identity: InertIdentity,
        start: tokio::time::Instant,
        caller: FunctionCaller,
        log_lines: LogLines,
        context: ExecutionContext,
        response_sha256: Sha256Digest,
    ) {
        let js_err = JsError::from_error_ref(error);
        let outcome = HttpActionOutcome::new(
            None,
            http_request,
            identity,
            self.rt.unix_timestamp(),
            udf::HttpActionResult::Error(js_err.clone()),
            None,
            None,
            Duration::ZERO,
        );
        self._log_http_action(
            outcome,
            Err(js_err),
            log_lines,
            start.elapsed(),
            caller,
            TrackUsage::SystemError,
            context,
            response_sha256,
        )
        .await
    }

    async fn _log_http_action(
        &self,
        outcome: HttpActionOutcome,
        result: Result<HttpActionStatusCode, JsError>,
        log_lines: LogLines,
        execution_time: Duration,
        caller: FunctionCaller,
        usage: TrackUsage,
        context: ExecutionContext,
        response_sha256: Sha256Digest,
    ) {
        // For usage tracking, substitute "[unmatched]" when the JS router
        // didn't match a route. This avoids route explosion from every unique
        // 404 URL becoming a separate billing entry. Function execution logs
        // preserve the actual URL for debugging.
        let usage_route = if outcome.route.matched {
            outcome.route.clone()
        } else {
            HttpActionRoute {
                method: outcome.route.method,
                path: "[unmatched]".to_string(),
                matched: false,
            }
        };
        let aggregated = match usage {
            TrackUsage::Track(usage_tracker) => {
                let usage_stats = usage_tracker.gather_user_stats();
                let aggregated = usage_stats.aggregate();
                self.usage_tracking
                    .track_call(
                        UdfIdentifier::Http(usage_route),
                        context.execution_id,
                        context.request_id.clone(),
                        CallType::HttpAction {
                            duration: execution_time,
                            user_execution_time: outcome.user_execution_time,
                            memory_in_mb: outcome.memory_in_mb(),
                            response_sha256,
                        },
                        result.clone().is_ok_and(|code| code.0.as_u16() < 400),
                        usage_stats,
                    )
                    .await;
                aggregated
            },
            TrackUsage::SystemError => AggregatedFunctionUsageStats::default(),
        };
        let execution = FunctionExecution {
            params: UdfParams::Http {
                result,
                identifier: outcome.route.clone(),
            },
            unix_timestamp: self.rt.unix_timestamp(),
            execution_timestamp: outcome.unix_timestamp,
            mutation_queue_length: None,
            udf_type: UdfType::HttpAction,
            log_lines,
            tables_touched: WithHeapSize::default(),
            cached_result: false,
            execution_time: execution_time.as_secs_f64(),
            user_execution_time: outcome.user_execution_time,
            caller,
            environment: ModuleEnvironment::Isolate,
            usage_stats: aggregated,
            memory_used_mb: outcome.memory_in_mb(),
            args_bytes: None,
            return_bytes: None,
            syscall_trace: outcome.syscall_trace,
            udf_server_version: outcome.udf_server_version,
            identity: outcome.identity,
            context,
            mutation_retry_count: None,
            occ_info: None,
            will_retry: false,
            query_invocation: None,
        };
        self.log_execution(
            execution,
            /* send_console_events */ false,
            /* log_app_metrics */ outcome.route.matched,
        );
    }

    pub fn log_http_action_progress(
        &self,
        identifier: HttpActionRoute,
        unix_timestamp: UnixTimestamp,
        context: ExecutionContext,
        log_lines: LogLines,
        module_environment: ModuleEnvironment,
    ) {
        let event_source = FunctionEventSource {
            // TODO(ENG-7612): Support HTTP actions in components.
            component_path: ComponentPath::root(),
            udf_path: identifier.to_string(),
            udf_type: UdfType::HttpAction,
            module_environment,
            cached: Some(false),
            context,
            mutation_queue_length: None,
            mutation_retry_count: None,
        };

        self.log_execution_progress(log_lines, event_source, unix_timestamp)
    }

    fn log_execution(
        &self,
        execution: FunctionExecution,
        send_console_events: bool,
        log_app_metrics: bool,
    ) {
        if let Err(mut e) =
            self.inner
                .lock()
                .log_execution(execution, send_console_events, log_app_metrics)
        {
            report_error_sync(&mut e);
        }
    }

    fn log_execution_progress(
        &self,
        log_lines: LogLines,
        event_source: FunctionEventSource,
        timestamp: UnixTimestamp,
    ) {
        if let Err(mut e) =
            self.inner
                .lock()
                .log_execution_progress(log_lines, event_source, timestamp)
        {
            report_error_sync(&mut e);
        }
    }

    /// Records that as of `timestamp`, the next scheduled job is at
    /// `next_job_ts` (None if there are no pending jobs).
    ///
    /// Scheduler lag is extrapolated from this state, so callers must record
    /// every observation rather than applying structured-log rate limits
    /// here.
    pub fn record_scheduled_job_lag(&self, next_job_ts: Option<SystemTime>, timestamp: SystemTime) {
        let result = record_scheduled_job_lag_sample(
            &mut self.scheduled_job_metrics.lock(),
            next_job_ts,
            timestamp,
        );
        if let Err(e) = result {
            Inner::<RT>::log_metrics_error(e);
        }
    }

    /// Emits rate-limited structured scheduler statistics.
    pub fn log_scheduled_job_stats(
        &self,
        next_job_ts: Option<SystemTime>,
        timestamp: SystemTime,
        num_running_jobs: u64,
    ) {
        if let Err(mut e) =
            self.inner
                .lock()
                .log_scheduled_job_stats(next_job_ts, timestamp, num_running_jobs)
        {
            report_error_sync(&mut e);
        }
    }

    pub fn record_subscription_invalidations(&self, events: Vec<database::InvalidationEvent>) {
        let ts = self.rt.system_time();
        let mut inner = self.inner.lock();
        for event in &events {
            if let Some(display_name) = event.write_source.as_ref().and_then(|ws| ws.display_name())
            {
                let name: MetricName = format!(
                    "subscription_invalidations:{display_name}:{}",
                    event.tablet_id
                );
                if let Err(e) = inner.metrics.add_counter(&name, ts, event.count as f32) {
                    Inner::<RT>::log_metrics_error(e);
                }
            }
        }
    }

    /// Record a privacy-safe outcome from an authenticated query-shadow
    /// registry-generation route.
    ///
    /// Returns true only when route-level evidence accepts the report. Metric
    /// recording is an observability boundary: rejection never affects the
    /// authoritative primary result.
    pub fn record_query_shadow_report(&self, report: QueryShadowReport) -> bool {
        #[cfg(not(feature = "static-hermes-wasmtime-gate"))]
        {
            let _ = report;
            return false;
        }
        #[cfg(feature = "static-hermes-wasmtime-gate")]
        {
            let mut metrics = self.query_shadow_metrics.lock();
            // Resolve the registry while holding the evidence lock. This preserves
            // generation order when a report races a live registry rotation: an
            // older report cannot acquire the lock after newer evidence and clear
            // the newer route-detail generation.
            let registry = match IsolateClient::<RT>::active_static_hermes_query_shadow_registry() {
                Ok(Some(registry)) => registry,
                Ok(None) | Err(_) => return false,
            };
            if !registry.authenticates_route(
                report.udf_type(),
                report.route_key().generation_sha256(),
                report.route_key().route_sha256(),
            ) {
                return false;
            }
            // Capture time while holding the dedicated lock so concurrent detached
            // completions cannot submit metric buckets in reverse order.
            let ts = self.rt.system_time();
            match record_authenticated_query_shadow_report(
                &mut metrics,
                &report,
                registry.generation_sha256(),
                ts,
            ) {
                Ok(QueryShadowReportAcceptance::RouteDetail) => true,
                Ok(QueryShadowReportAcceptance::GenerationAggregateOnly) => {
                    log_query_shadow_route_detail_capacity_drop();
                    false
                },
                Ok(
                    QueryShadowReportAcceptance::InactiveGenerationAggregateOnly
                    | QueryShadowReportAcceptance::Ignored,
                ) => false,
                Err(error) => {
                    Inner::<RT>::log_metrics_error(error);
                    false
                },
            }
        }
    }

    /// Query p90, p99, retained-window max timings, and exact sample counts for
    /// one authenticated registry-generation route and the aggregate shadow
    /// population.
    pub fn query_shadow_timing_stats(
        &self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        window: MetricsWindow,
    ) -> anyhow::Result<QueryShadowTimingStats> {
        let metrics = self.query_shadow_metrics.lock();
        query_shadow_timing_stats(&metrics.store, udf_type, route_key, &window)
    }

    /// Query bounded correctness and terminal counters for one authenticated
    /// registry-generation route and the aggregate shadow population.
    pub fn query_shadow_outcome_stats(
        &self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        window: MetricsWindow,
    ) -> anyhow::Result<QueryShadowOutcomeStats> {
        let metrics = self.query_shadow_metrics.lock();
        query_shadow_outcome_stats(&metrics.store, udf_type, route_key, &window)
    }

    /// Return a bounded page of query-shadow evidence for the authenticated
    /// active registry generation.
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub fn query_shadow_evidence(
        &self,
        udf_type: UdfType,
        cursor: Option<&QueryShadowRouteKey>,
        limit: usize,
    ) -> anyhow::Result<QueryShadowEvidencePage> {
        self.query_shadow_evidence_for_direction(
            udf_type,
            QueryShadowDirection::V8PrimaryWasmShadow,
            cursor,
            limit,
        )
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub fn query_shadow_evidence_for_direction(
        &self,
        udf_type: UdfType,
        direction: QueryShadowDirection,
        cursor: Option<&QueryShadowRouteKey>,
        limit: usize,
    ) -> anyhow::Result<QueryShadowEvidencePage> {
        query_shadow_evidence_with_active_registry(
            &self.query_shadow_metrics,
            udf_type,
            direction,
            IsolateClient::<RT>::active_static_hermes_query_shadow_registry,
            || self.rt.system_time(),
            cursor,
            limit,
        )
    }

    pub fn udf_rate(
        &self,
        identifier: UdfIdentifier,
        metric: UdfRate,
        window: MetricsWindow,
    ) -> anyhow::Result<Timeseries> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };
        let name = match metric {
            UdfRate::Invocations => udf_invocations_metric(&identifier),
            UdfRate::Errors => udf_errors_metric(&identifier),
            UdfRate::CacheHits => udf_cache_hits_metric(&identifier),
            UdfRate::CacheMisses => udf_cache_misses_metric(&identifier),
            UdfRate::SubscriptionInvalidations => {
                // Aggregate across all tablets for this mutation.
                let mutation_name = udf_metric_name(&identifier);
                let by_table = Self::get_subscription_invalidation_counter(
                    &window,
                    &metrics,
                    Some(&mutation_name),
                )?;
                if by_table.is_empty() {
                    return window.resample_counters(&metrics, vec![], true);
                }
                return sum_timeseries(&window, by_table.values());
            },
        };
        let buckets = metrics.query_counter(&name, window.start..window.end)?;
        window.resample_counters(&metrics, buckets, true)
    }

    pub fn cache_hit_percentage(
        &self,
        identifier: UdfIdentifier,
        window: MetricsWindow,
    ) -> anyhow::Result<Timeseries> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };
        let hits = metrics.query_counter(
            &udf_cache_hits_metric(&identifier),
            window.start..window.end,
        )?;
        let hits = window.resample_counters(&metrics, hits, false /* is_rate */)?;
        let misses = metrics.query_counter(
            &udf_cache_misses_metric(&identifier),
            window.start..window.end,
        )?;
        let misses = window.resample_counters(&metrics, misses, false /* is_rate */)?;

        merge_series(&hits, &misses, cache_hit_percentage)
    }

    pub fn failure_percentage_top_k(
        &self,
        window: MetricsWindow,
        k: usize,
    ) -> anyhow::Result<Vec<(String, Timeseries)>> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };

        // Get the invocations and errors
        let invocations = Self::get_udf_metric_counter(&window, &metrics, "invocations")?;
        let errors = Self::get_udf_metric_counter(&window, &metrics, "errors")?;

        Self::top_k_for_rate(&window, errors, invocations, k, percentage, false)
    }

    pub fn cache_hit_percentage_top_k(
        &self,
        window: MetricsWindow,
        k: usize,
    ) -> anyhow::Result<Vec<(String, Timeseries)>> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };

        // Get the invocations and hits
        let hits = Self::get_udf_metric_counter(&window, &metrics, "cache_hits")?;
        let misses = Self::get_udf_metric_counter(&window, &metrics, "cache_misses")?;

        Self::top_k_for_rate(&window, hits, misses, k, cache_hit_percentage, true)
    }

    pub fn get_all_function_calls(
        &self,
        window: &MetricsWindow,
    ) -> anyhow::Result<HashMap<String, Timeseries>> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };
        Self::get_udf_metric_counter(window, &metrics, "invocations")
    }

    pub fn function_call_count_top_k(
        &self,
        window: MetricsWindow,
        k: usize,
    ) -> anyhow::Result<Vec<(String, Timeseries)>> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };

        let mut invocations = Self::get_udf_metric_counter(&window, &metrics, "invocations")?;

        let mut overall: HashMap<&str, f64> = HashMap::new();
        for (udf_id, series) in invocations.iter() {
            let sum = series.iter().filter_map(|&(_, value)| value).sum1();
            if let Some(total) = sum {
                overall.insert(udf_id, total);
            }
        }

        let top_k = Self::top_k(&overall, k, false);

        let mut ret = vec![];
        for udf_id in top_k {
            let series = invocations
                .remove(&udf_id)
                .expect("everything in topk came from invocations");
            ret.push((udf_id.to_string(), series));
        }

        if !invocations.is_empty() {
            let rest = sum_timeseries(&window, invocations.values())?;
            ret.push(("_rest".to_string(), rest));
        }

        Ok(ret)
    }

    /// Returns the top-k subscription invalidation pairs.
    /// If `identifier` is provided, filters to that mutation and returns
    /// top-k by tablet. Otherwise returns top-k by (mutation, tablet) pairs.
    pub fn subscription_invalidations_top_k(
        &self,
        window: MetricsWindow,
        k: usize,
        identifier: Option<&UdfIdentifier>,
    ) -> anyhow::Result<Vec<(String, Timeseries)>> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };
        let mutation_filter = identifier.map(udf_metric_name);
        let mut counters = Self::get_subscription_invalidation_counter(
            &window,
            &metrics,
            mutation_filter.as_deref(),
        )?;

        let mut overall: HashMap<&str, f64> = HashMap::new();
        for (key, series) in counters.iter() {
            let sum = series.iter().filter_map(|&(_, value)| value).sum1();
            if let Some(total) = sum {
                overall.insert(key, total);
            }
        }

        let top_k = Self::top_k(&overall, k, false);

        let mut ret = vec![];
        for key in top_k {
            let series = counters
                .remove(&key)
                .expect("everything in topk came from counters");
            ret.push((key.to_string(), series));
        }

        if !counters.is_empty() {
            let rest = sum_timeseries(&window, counters.values())?;
            ret.push(("_rest".to_string(), rest));
        }

        Ok(ret)
    }

    /// Queries `subscription_invalidations:{mutation}:{tablet_id}` metrics.
    /// If `mutation_filter` is Some, only returns metrics for that mutation
    /// (with the mutation prefix stripped, so keys are tablet IDs).
    /// If None, returns all pairs (keys are `{mutation}:{tablet_id}`).
    fn get_subscription_invalidation_counter(
        window: &MetricsWindow,
        metrics: &MetricStore,
        mutation_filter: Option<&str>,
    ) -> anyhow::Result<HashMap<String, Timeseries>> {
        let metric_names = metrics.metric_names_for_type(MetricType::Counter);
        let prefix = match mutation_filter {
            Some(mutation) => format!("subscription_invalidations:{mutation}:"),
            None => "subscription_invalidations:".to_string(),
        };

        let filtered: Vec<_> = metric_names
            .iter()
            .filter(|name| name.starts_with(&prefix))
            .collect();

        let mut results: HashMap<String, Vec<&CounterBucket>> = HashMap::new();
        for name in filtered {
            let result = metrics.query_counter(name, window.start..window.end)?;
            let key = if mutation_filter.is_some() {
                // Strip prefix → tablet_id
                name[prefix.len()..].to_string()
            } else {
                // Strip "subscription_invalidations:" → "{mutation}:{tablet_id}"
                name["subscription_invalidations:".len()..].to_string()
            };
            results.insert(key, result);
        }

        results
            .into_iter()
            .map(|(k, v)| {
                Ok((
                    k,
                    window.resample_counters(metrics, v, false /* is_rate */)?,
                ))
            })
            .collect()
    }

    fn get_udf_metric_counter(
        window: &MetricsWindow,
        metrics: &MetricStore,
        metric_name: &str,
    ) -> anyhow::Result<HashMap<String, Timeseries>> {
        let metric_names = metrics.metric_names_for_type(MetricType::Counter);

        let filtered_metric_names: Vec<_> = metric_names
            .iter()
            .filter(|name| name.starts_with("udf") && name.ends_with(metric_name))
            .collect();

        let mut results: HashMap<String, Vec<&CounterBucket>> = HashMap::new();
        for metric_name in filtered_metric_names {
            let result = metrics.query_counter(metric_name, window.start..window.end)?;
            let metric_name_parts: Vec<&str> = metric_name.split(':').collect();
            // UDF names can have colons in them, so we need to only exclude
            // the metric type and metric name
            let metric_name = metric_name_parts[1..metric_name_parts.len() - 1].join(":");
            results.insert(metric_name, result);
        }

        results
            .into_iter()
            .map(|(k, v)| {
                Ok((
                    k,
                    window.resample_counters(metrics, v, false /* is_rate */)?,
                ))
            })
            .collect()
    }

    fn top_k(map: &HashMap<&str, f64>, k: usize, ascending: bool) -> Vec<String> {
        let mut top_k: Vec<_> = map.iter().map(|(&key, &sum)| (key, sum)).collect();
        top_k.sort_unstable_by(|(name1, a), (name2, b)| {
            if ascending {
                a.total_cmp(b)
            } else {
                b.total_cmp(a)
            }
            .then(name1.cmp(name2)) // tiebreak by name
        });
        top_k.truncate(k);
        top_k.into_iter().map(|(name, _)| name.to_owned()).collect()
    }

    // Compute the top k rates for the given UDFs
    fn top_k_for_rate(
        window: &MetricsWindow,
        mut ts1: HashMap<String, Timeseries>,
        mut ts2: HashMap<String, Timeseries>,
        k: usize,
        merge: impl Fn(Option<f64>, Option<f64>) -> Option<f64> + Copy,
        ascending: bool,
    ) -> anyhow::Result<Vec<(String, Timeseries)>> {
        // First, calculate the overall rates summed over the entire time window
        let mut overall: HashMap<&str, f64> = HashMap::new();
        for (udf_id, ts1_series) in ts1.iter() {
            let ts1_sum = ts1_series.iter().filter_map(|&(_, value)| value).sum1();
            let ts2_sum = ts2
                .get(udf_id)
                .and_then(|ts2_series| ts2_series.iter().filter_map(|&(_, value)| value).sum1());
            if let Some(ratio) = merge(ts1_sum, ts2_sum) {
                overall.insert(udf_id, ratio);
            }
        }
        let top_k = Self::top_k(&overall, k, ascending);

        let mut ret = vec![];

        for udf_id in top_k {
            // Remove the top k from the hits and misses timeseries
            // so we can sum up everything that's left over.
            let ts1_series = ts1
                .remove(&udf_id)
                .expect("everything in topk came from ts1");
            let ts2_series = ts2
                .remove(&udf_id)
                .unwrap_or_else(|| ts1_series.iter().map(|&(ts, _)| (ts, None)).collect());
            let merged: Timeseries = merge_series(&ts1_series, &ts2_series, merge)?;
            ret.push((udf_id.to_string(), merged));
        }

        // Sum up the rest of the rates
        if !ts2.is_empty() || !ts1.is_empty() {
            let rest_ts1 = sum_timeseries(window, ts1.values())?;
            let rest_ts2 = sum_timeseries(window, ts2.values())?;
            let rest = merge_series(&rest_ts1, &rest_ts2, merge)?;
            ret.push(("_rest".to_string(), rest));
        }

        Ok(ret)
    }

    pub fn latency_percentiles(
        &self,
        identifier: UdfIdentifier,
        percentiles: Vec<Percentile>,
        window: MetricsWindow,
    ) -> anyhow::Result<BTreeMap<Percentile, Timeseries>> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };
        let buckets = metrics.query_histogram(
            &udf_execution_time_metric(&identifier),
            window.start..window.end,
        )?;
        window.resample_histograms(&metrics, buckets, &percentiles)
    }

    pub fn table_rate(
        &self,
        table_name: TableName,
        metric: TableRate,
        window: MetricsWindow,
    ) -> anyhow::Result<Timeseries> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };
        let name = match metric {
            TableRate::RowsRead => table_rows_read_metric(&table_name),
            TableRate::RowsWritten => table_rows_written_metric(&table_name),
        };
        let buckets = metrics.query_counter(&name, window.start..window.end)?;
        window.resample_counters(&metrics, buckets, true)
    }

    pub fn udf_summary(
        &self,
        cursor: Option<CursorMs>,
    ) -> (Option<UdfMetricSummary>, Option<CursorMs>) {
        let inner = self.inner.lock();
        let new_cursor = inner.log.back().map(|(ts, _)| *ts);

        let first_entry_ix = inner.log.partition_point(|(ts, _)| Some(*ts) <= cursor);
        if first_entry_ix >= inner.log.len() {
            return (None, new_cursor);
        }
        let mut summary = UdfMetricSummary::default();
        for i in first_entry_ix..inner.log.len() {
            let (_, entry) = &inner.log[i];
            let FunctionExecutionPart::Completion(entry) = entry else {
                continue;
            };
            let function_summary = summary
                .function_calls
                .entry(entry.caller.clone())
                .or_default()
                .entry(entry.udf_type)
                .or_default()
                .entry(entry.environment)
                .or_default();
            let error_count = if entry.params.is_err() { 1 } else { 0 };
            let entry_duration = Duration::from_secs_f64(entry.execution_time);

            function_summary.invocations += 1;
            function_summary.errors += error_count;
            function_summary.execution_time += entry_duration;
            function_summary.syscalls.merge(&entry.syscall_trace);

            summary.invocations += 1;
            summary.errors += error_count;
            summary.execution_time += entry_duration;
        }

        (Some(summary), new_cursor)
    }

    pub async fn stream(&self, cursor: CursorMs) -> (Vec<FunctionExecution>, CursorMs) {
        loop {
            let rx = {
                let mut inner = self.inner.lock();
                let first_entry_ix = inner.log.partition_point(|(ts, _)| *ts <= cursor);
                if first_entry_ix < inner.log.len() {
                    let entries = (first_entry_ix..inner.log.len())
                        .map(|i| &inner.log[i])
                        .filter_map(|(_, entry)| match entry {
                            FunctionExecutionPart::Completion(completion) => {
                                Some(completion.clone())
                            },
                            _ => None,
                        })
                        .collect();
                    let (new_cursor, _) = inner.log.back().unwrap();
                    return (entries, *new_cursor);
                }
                let (tx, rx) = oneshot::channel();
                inner.log_waiters.push(tx);
                rx
            };
            let _ = rx.await;
        }
    }

    pub async fn stream_parts(&self, cursor: CursorMs) -> (Vec<FunctionExecutionPart>, CursorMs) {
        loop {
            let rx = {
                let mut inner = self.inner.lock();
                let first_entry_ix = inner.log.partition_point(|(ts, _)| *ts <= cursor);
                if first_entry_ix < inner.log.len() {
                    let entries = (first_entry_ix..inner.log.len())
                        .map(|i| &inner.log[i])
                        .map(|(_, entry)| match entry {
                            FunctionExecutionPart::Completion(c) => {
                                let with_stripped_log_lines = match c.udf_type {
                                    UdfType::Query | UdfType::Mutation => c.clone(),
                                    UdfType::Action | UdfType::HttpAction => {
                                        let mut cloned = c.clone();
                                        cloned.log_lines = vec![].into();
                                        cloned
                                    },
                                };
                                FunctionExecutionPart::Completion(with_stripped_log_lines)
                            },
                            FunctionExecutionPart::Progress(c) => {
                                FunctionExecutionPart::Progress(c.clone())
                            },
                        })
                        .collect();
                    let (new_cursor, _) = inner.log.back().unwrap();
                    return (entries, *new_cursor);
                }
                let (tx, rx) = oneshot::channel();
                inner.log_waiters.push(tx);
                rx
            };
            let _ = rx.await;
        }
    }

    pub fn latest_cursor(&self) -> CursorMs {
        let inner = self.inner.lock();
        if let Some((new_cursor, _)) = inner.log.back() {
            *new_cursor
        } else {
            0.0
        }
    }

    /// Get the gauge value from the first complete bucket in the time range
    fn get_gauge_value(
        metrics: &MetricStore,
        env: &ModuleEnvironment,
        udf_type: &UdfType,
        state: &OutstandingFunctionState,
        start_time: SystemTime,
        now: SystemTime,
    ) -> u64 {
        let metric_name = outstanding_functions_metric(env, udf_type, state);
        metrics
            .query_gauge(&metric_name, start_time..now)
            .ok()
            .and_then(|buckets| buckets.first().map(|b| b.value.max(0.0) as u64))
            .unwrap_or(0)
    }

    /// Get both running and queued stats for a function type
    fn get_concurrency_stats(
        metrics: &MetricStore,
        env: ModuleEnvironment,
        udf_type: UdfType,
        start_time: SystemTime,
        now: SystemTime,
    ) -> FunctionConcurrencyStats {
        FunctionConcurrencyStats {
            num_running: Self::get_gauge_value(
                metrics,
                &env,
                &udf_type,
                &OutstandingFunctionState::Running,
                start_time,
                now,
            ),
            num_queued: Self::get_gauge_value(
                metrics,
                &env,
                &udf_type,
                &OutstandingFunctionState::Queued,
                start_time,
                now,
            ),
        }
    }

    /// Query concurrency stats for all function types within a time window.
    /// Returns a tuple of (query, mutation, action, node_action, http_action)
    /// stats.
    fn query_all_concurrency_stats(
        metrics: &MetricStore,
        start_time: SystemTime,
        end_time: SystemTime,
    ) -> (
        FunctionConcurrencyStats,
        FunctionConcurrencyStats,
        FunctionConcurrencyStats,
        FunctionConcurrencyStats,
        FunctionConcurrencyStats,
    ) {
        (
            Self::get_concurrency_stats(
                metrics,
                ModuleEnvironment::Isolate,
                UdfType::Query,
                start_time,
                end_time,
            ),
            Self::get_concurrency_stats(
                metrics,
                ModuleEnvironment::Isolate,
                UdfType::Mutation,
                start_time,
                end_time,
            ),
            Self::get_concurrency_stats(
                metrics,
                ModuleEnvironment::Isolate,
                UdfType::Action,
                start_time,
                end_time,
            ),
            Self::get_concurrency_stats(
                metrics,
                ModuleEnvironment::Node,
                UdfType::Action,
                start_time,
                end_time,
            ),
            Self::get_concurrency_stats(
                metrics,
                ModuleEnvironment::Isolate,
                UdfType::HttpAction,
                start_time,
                end_time,
            ),
        )
    }

    pub fn scheduled_job_lag(&self, window: MetricsWindow) -> anyhow::Result<Timeseries> {
        let metrics = self.scheduled_job_metrics.lock().clone();
        query_scheduled_job_lag(&window, &metrics)
    }

    /// Log the current number of outstanding functions as a gauge that tracks
    /// the maximum.
    pub fn log_outstanding_functions(
        &self,
        total: usize,
        env: ModuleEnvironment,
        udf_type: UdfType,
        state: OutstandingFunctionState,
    ) {
        let now = self.rt.system_time();
        let name = outstanding_functions_metric(&env, &udf_type, &state);
        // Use gauge with max operation to track maximum value within each bucket.
        let _ = self
            .inner
            .lock()
            .metrics
            .add_gauge_max(&name, now, total as f32);
    }

    pub fn function_concurrency(
        &self,
        window: MetricsWindow,
    ) -> anyhow::Result<BTreeMap<String, Timeseries>> {
        let metrics = {
            let inner = self.inner.lock();
            inner.metrics.clone()
        };

        let mut result = BTreeMap::new();

        // Query all outstanding_functions metrics
        let metric_names = metrics.metric_names_for_type(MetricType::Gauge);
        for metric_name in metric_names {
            if metric_name.starts_with("outstanding_functions:") {
                let buckets = metrics.query_gauge(&metric_name, window.start..window.end)?;
                let timeseries = window.resample_gauges(&metrics, buckets)?;
                result.insert(metric_name.to_string(), timeseries);
            }
        }

        Ok(result)
    }
}

impl<RT: Runtime> QueryShadowReportSink for FunctionExecutionLog<RT> {
    fn record_query_shadow_report(&self, report: QueryShadowReport) -> bool {
        FunctionExecutionLog::record_query_shadow_report(self, report)
    }
}

fn record_query_shadow_report_with_capacity(
    metrics: &mut QueryShadowMetrics,
    report: &QueryShadowReport,
    ts: SystemTime,
) -> Result<QueryShadowReportAcceptance, UdfMetricsError> {
    if matches!(
        report,
        QueryShadowReport::Memory { observation, .. } if !observation.has_anomaly()
    ) {
        return Ok(QueryShadowReportAcceptance::Ignored);
    }
    if !metrics.has_route_detail_capacity(
        report.udf_type(),
        report.route_key(),
        report.direction(),
        ts,
    ) {
        record_query_shadow_report_sample(
            metrics.store_for_direction_mut(report.direction()),
            report,
            ts,
            QueryShadowReportRetention::GenerationOnly,
        )?;
        return Ok(QueryShadowReportAcceptance::GenerationAggregateOnly);
    }
    record_query_shadow_report_sample(
        metrics.store_for_direction_mut(report.direction()),
        report,
        ts,
        QueryShadowReportRetention::RouteAndGeneration,
    )?;
    metrics.record_diagnostic(report, ts);
    metrics.accept_route(
        report.udf_type(),
        report.route_key(),
        report.direction(),
        ts,
    );
    Ok(QueryShadowReportAcceptance::RouteDetail)
}

fn record_authenticated_query_shadow_report(
    metrics: &mut QueryShadowMetrics,
    report: &QueryShadowReport,
    active_generation_sha256: &str,
    ts: SystemTime,
) -> Result<QueryShadowReportAcceptance, UdfMetricsError> {
    // The registry selection is authoritative even when the report belongs to
    // a retained older generation. Rotate before recording that late aggregate
    // so the first current-generation report cannot erase it afterward.
    metrics.activate_generation(active_generation_sha256, ts);
    if report.route_key().generation_sha256() != active_generation_sha256 {
        // A transaction that began on a preceding source snapshot can finish
        // after a newer snapshot becomes the displayed selection. Retain its
        // factual generation aggregate, but do not move the displayed
        // selection backward or consume route-detail capacity owned by the
        // newer generation. Returning false also prevents a late admission
        // attempt from starting new shadow work after that rotation.
        // Do not apply the active timeline's backward-clock recovery here. A
        // late report may predate the current generation's base timestamp, but
        // resetting that shared timeline would erase current route evidence and
        // its capacity reservations. The observability boundary can reject this
        // stale sample while leaving the active generation untouched.
        record_query_shadow_report_sample(
            metrics.store_for_direction_mut(report.direction()),
            report,
            ts,
            QueryShadowReportRetention::GenerationOnly,
        )?;
        return Ok(QueryShadowReportAcceptance::InactiveGenerationAggregateOnly);
    }
    record_query_shadow_report_with_clock_recovery(metrics, report, ts)
}

fn record_query_shadow_report_with_clock_recovery(
    metrics: &mut QueryShadowMetrics,
    report: &QueryShadowReport,
    ts: SystemTime,
) -> Result<QueryShadowReportAcceptance, UdfMetricsError> {
    match record_query_shadow_report_with_capacity(metrics, report, ts) {
        Ok(acceptance) => Ok(acceptance),
        Err(
            UdfMetricsError::SamplePrecedesBaseTimestamp { .. }
            | UdfMetricsError::SamplePrecedesCutoff { .. },
        ) => {
            // A backward wall-clock step must not reject every sampled route
            // until real time catches up. The old and new bucket timelines
            // cannot be combined, so discard the dedicated evidence window and
            // its route-detail reservations before retrying this report once.
            metrics.reset_timeline(ts);
            record_query_shadow_report_with_capacity(metrics, report, ts)
        },
        Err(error) => Err(error),
    }
}

fn record_query_shadow_report_sample(
    metrics: &mut MetricStore<u32>,
    report: &QueryShadowReport,
    ts: SystemTime,
    retention: QueryShadowReportRetention,
) -> Result<(), UdfMetricsError> {
    // One report owns several correlated counters and histograms. Preserve the
    // previous persistent store if any write fails so rejected evidence cannot
    // leave a partial report behind.
    let previous_metrics = metrics.clone();
    let result = (|| match report {
        QueryShadowReport::Memory { .. } => Ok(()),
        QueryShadowReport::Admission {
            udf_type,
            route_key,
            direction,
            event,
        } => record_query_shadow_counter(
            metrics,
            *udf_type,
            route_key,
            *direction,
            &format!("admission:{}", event.metric_key()),
            ts,
            retention,
        ),
        QueryShadowReport::Compared {
            udf_type,
            route_key,
            direction,
            primary_duration,
            wasm_duration,
            verifier_duration,
            comparison,
        } => {
            record_query_shadow_timing_sample(
                metrics,
                *udf_type,
                route_key,
                *direction,
                *primary_duration,
                *wasm_duration,
                *verifier_duration,
                ts,
                retention,
            )?;
            record_query_shadow_comparison(
                metrics, *udf_type, route_key, *direction, comparison, ts, retention,
            )
        },
        QueryShadowReport::Terminal {
            udf_type,
            route_key,
            direction,
            terminal,
        } => {
            record_query_shadow_counter(
                metrics,
                *udf_type,
                route_key,
                *direction,
                &format!("terminal:{}", terminal.metric_key()),
                ts,
                retention,
            )?;
            if let Some(stage) = terminal.shadow_failure_stage() {
                record_query_shadow_counter(
                    metrics,
                    *udf_type,
                    route_key,
                    *direction,
                    &format!("terminal:shadow_failure_stage:{}", stage.metric_key()),
                    ts,
                    retention,
                )?;
            }
            if let Some(reason) = terminal.shadow_failure_reason() {
                record_query_shadow_counter(
                    metrics,
                    *udf_type,
                    route_key,
                    *direction,
                    &format!("terminal:shadow_failure_reason:{}", reason.metric_key()),
                    ts,
                    retention,
                )?;
            }
            Ok(())
        },
    })();
    if result.is_err() {
        *metrics = previous_metrics;
    }
    result
}

fn record_query_shadow_timing_sample(
    metrics: &mut MetricStore<u32>,
    udf_type: UdfType,
    route_key: &QueryShadowRouteKey,
    direction: QueryShadowDirection,
    primary_duration: Duration,
    wasm_duration: Duration,
    verifier_duration: Option<Duration>,
    ts: SystemTime,
    retention: QueryShadowReportRetention,
) -> Result<(), UdfMetricsError> {
    if matches!(direction, QueryShadowDirection::WasmPrimaryV8Shadow) {
        let verifier_duration = verifier_duration.ok_or_else(|| {
            UdfMetricsError::InternalError(anyhow::anyhow!(
                "Wasm-primary comparison is missing verifier duration"
            ))
        })?;
        return record_wasm_primary_v8_shadow_timing_sample(
            metrics,
            udf_type,
            route_key,
            primary_duration,
            verifier_duration,
            ts,
            retention,
        );
    }

    // MetricStore histograms contain durations. Encoding 1.0 ratio as one second
    // lets its existing HDR implementation provide ratio percentiles without a
    // second sketch implementation. A zero primary duration is retained in the
    // timing histograms but has no defined ratio. Clamp before Duration
    // conversion so an enormous finite ratio cannot panic this boundary.
    let ratio = (!primary_duration.is_zero()).then(|| {
        let ratio_seconds = (wasm_duration.as_secs_f64() / primary_duration.as_secs_f64())
            .min(knobs::UDF_METRICS_MAX_DURATION.as_secs_f64());
        Duration::from_secs_f64(ratio_seconds)
    });

    if matches!(retention, QueryShadowReportRetention::RouteAndGeneration) {
        metrics.add_histogram(
            &query_shadow_metric(
                QueryShadowMetricScope::Route(udf_type, route_key),
                "primary_seconds",
            ),
            ts,
            primary_duration,
        )?;
        metrics.add_histogram(
            &query_shadow_metric(
                QueryShadowMetricScope::Route(udf_type, route_key),
                "wasm_seconds",
            ),
            ts,
            wasm_duration,
        )?;
        if let Some(ratio) = ratio {
            metrics.add_histogram(
                &query_shadow_metric(
                    QueryShadowMetricScope::Route(udf_type, route_key),
                    "wasm_to_primary_ratio",
                ),
                ts,
                ratio,
            )?;
        }
    }
    metrics.add_histogram(
        &query_shadow_metric(
            QueryShadowMetricScope::Generation(udf_type, route_key.generation_sha256()),
            "primary_seconds",
        ),
        ts,
        primary_duration,
    )?;
    metrics.add_histogram(
        &query_shadow_metric(
            QueryShadowMetricScope::Generation(udf_type, route_key.generation_sha256()),
            "wasm_seconds",
        ),
        ts,
        wasm_duration,
    )?;
    if let Some(ratio) = ratio {
        metrics.add_histogram(
            &query_shadow_metric(
                QueryShadowMetricScope::Generation(udf_type, route_key.generation_sha256()),
                "wasm_to_primary_ratio",
            ),
            ts,
            ratio,
        )?;
    }
    Ok(())
}

fn record_wasm_primary_v8_shadow_timing_sample(
    metrics: &mut MetricStore<u32>,
    udf_type: UdfType,
    route_key: &QueryShadowRouteKey,
    wasm_primary_duration: Duration,
    v8_verifier_duration: Duration,
    ts: SystemTime,
    retention: QueryShadowReportRetention,
) -> Result<(), UdfMetricsError> {
    // Keep inverse-direction timing in a separate namespace. The legacy
    // primary/wasm fields describe V8-primary comparisons and must not become
    // a mixture of V8 and Wasm authoritative timings after a runtime-policy
    // change.
    let ratio = (!wasm_primary_duration.is_zero()).then(|| {
        let ratio_seconds = (v8_verifier_duration.as_secs_f64()
            / wasm_primary_duration.as_secs_f64())
        .min(knobs::UDF_METRICS_MAX_DURATION.as_secs_f64());
        Duration::from_secs_f64(ratio_seconds)
    });

    let record = |metrics: &mut MetricStore<u32>, scope: QueryShadowMetricScope<'_>| {
        metrics.add_histogram(
            &query_shadow_metric(scope, "wasm_primary_seconds"),
            ts,
            wasm_primary_duration,
        )?;
        metrics.add_histogram(
            &query_shadow_metric(scope, "v8_verifier_seconds"),
            ts,
            v8_verifier_duration,
        )?;
        if let Some(ratio) = ratio {
            metrics.add_histogram(
                &query_shadow_metric(scope, "v8_verifier_to_wasm_primary_ratio"),
                ts,
                ratio,
            )?;
        }
        Ok::<(), UdfMetricsError>(())
    };

    if matches!(retention, QueryShadowReportRetention::RouteAndGeneration) {
        record(metrics, QueryShadowMetricScope::Route(udf_type, route_key))?;
    }
    record(
        metrics,
        QueryShadowMetricScope::Generation(udf_type, route_key.generation_sha256()),
    )?;
    Ok(())
}

fn record_query_shadow_comparison(
    metrics: &mut MetricStore<u32>,
    udf_type: UdfType,
    route_key: &QueryShadowRouteKey,
    direction: QueryShadowDirection,
    comparison: &QueryShadowComparison,
    ts: SystemTime,
    retention: QueryShadowReportRetention,
) -> Result<(), UdfMetricsError> {
    if !comparison.matches() {
        record_query_shadow_counter(
            metrics,
            udf_type,
            route_key,
            direction,
            "comparison:mismatch",
            ts,
            retention,
        )?;
    }
    record_query_shadow_counter(
        metrics,
        udf_type,
        route_key,
        direction,
        &format!(
            "comparison:inserted_document_identity:{}",
            comparison.inserted_document_identity.metric_key()
        ),
        ts,
        retention,
    )?;
    for (dimension, matches) in [
        ("snapshot", comparison.snapshot_matches),
        ("result", comparison.result_matches),
        ("query_journal", comparison.journal_matches),
        ("read_dependencies", comparison.read_dependencies_match),
        ("invocation_inputs", comparison.invocation_inputs_match),
        (
            "host_operation_trace",
            comparison.host_operation_trace_matches,
        ),
        (
            "host_operation_error",
            comparison.host_operation_error_matches,
        ),
        ("observed_identity", comparison.observed_identity_matches),
        ("observed_time", comparison.observed_time_matches),
        ("observed_rng", comparison.observed_rng_matches),
        ("log_lines", comparison.log_lines_match),
        ("audit_log_lines", comparison.audit_log_lines_match),
        ("write_set", comparison.write_set_matches),
    ] {
        record_query_shadow_counter(
            metrics,
            udf_type,
            route_key,
            direction,
            &format!(
                "comparison:{dimension}:{}",
                if matches { "match" } else { "mismatch" }
            ),
            ts,
            retention,
        )?;
    }
    Ok(())
}

fn record_query_shadow_counter(
    metrics: &mut MetricStore<u32>,
    udf_type: UdfType,
    route_key: &QueryShadowRouteKey,
    _direction: QueryShadowDirection,
    classification: &str,
    ts: SystemTime,
    retention: QueryShadowReportRetention,
) -> Result<(), UdfMetricsError> {
    if matches!(retention, QueryShadowReportRetention::RouteAndGeneration) {
        metrics.add_counter(
            &query_shadow_metric(
                QueryShadowMetricScope::Route(udf_type, route_key),
                classification,
            ),
            ts,
            1.0,
        )?;
    }
    metrics.add_counter(
        &query_shadow_metric(
            QueryShadowMetricScope::Generation(udf_type, route_key.generation_sha256()),
            classification,
        ),
        ts,
        1.0,
    )
}

fn query_shadow_timing_stats(
    metrics: &MetricStore<u32>,
    udf_type: UdfType,
    route_key: &QueryShadowRouteKey,
    window: &MetricsWindow,
) -> anyhow::Result<QueryShadowTimingStats> {
    Ok(QueryShadowTimingStats {
        route: query_shadow_timing_percentiles(
            metrics,
            QueryShadowMetricScope::Route(udf_type, route_key),
            window,
        )?,
        aggregate: query_shadow_timing_percentiles(
            metrics,
            QueryShadowMetricScope::Generation(udf_type, route_key.generation_sha256()),
            window,
        )?,
    })
}

fn query_shadow_outcome_stats(
    metrics: &MetricStore<u32>,
    udf_type: UdfType,
    route_key: &QueryShadowRouteKey,
    window: &MetricsWindow,
) -> anyhow::Result<QueryShadowOutcomeStats> {
    Ok(QueryShadowOutcomeStats {
        route: query_shadow_outcome_counters(
            metrics,
            QueryShadowMetricScope::Route(udf_type, route_key),
            window,
        )?,
        aggregate: query_shadow_outcome_counters(
            metrics,
            QueryShadowMetricScope::Generation(udf_type, route_key.generation_sha256()),
            window,
        )?,
    })
}

fn query_shadow_outcome_counters(
    metrics: &MetricStore<u32>,
    scope: QueryShadowMetricScope<'_>,
    window: &MetricsWindow,
) -> anyhow::Result<BTreeMap<String, Timeseries>> {
    let prefix = query_shadow_metric(scope, "");
    metrics
        .metric_names_for_type(MetricType::Counter)
        .into_iter()
        .filter(|name| name.starts_with(&prefix))
        .map(|name| {
            let classification = name[prefix.len()..].to_owned();
            let buckets = metrics.query_counter(&name, window.start..window.end)?;
            let timeseries = window.resample_counters(metrics, buckets, false)?;
            Ok((classification, timeseries))
        })
        .collect()
}

fn query_shadow_timing_percentiles(
    metrics: &MetricStore<u32>,
    scope: QueryShadowMetricScope<'_>,
    window: &MetricsWindow,
) -> anyhow::Result<QueryShadowTimingPercentiles> {
    Ok(QueryShadowTimingPercentiles {
        primary_seconds: query_shadow_timing_histogram(metrics, scope, "primary_seconds", window)?,
        wasm_seconds: query_shadow_timing_histogram(metrics, scope, "wasm_seconds", window)?,
        wasm_to_primary_ratio: query_shadow_timing_histogram(
            metrics,
            scope,
            "wasm_to_primary_ratio",
            window,
        )?,
        completed_samples: query_shadow_completed_samples(metrics, scope, window)?,
    })
}

fn query_shadow_timing_histogram(
    metrics: &MetricStore<u32>,
    scope: QueryShadowMetricScope<'_>,
    metric: &str,
    window: &MetricsWindow,
) -> anyhow::Result<BTreeMap<Percentile, Timeseries>> {
    let buckets = metrics.query_histogram(
        &query_shadow_metric(scope, metric),
        window.start..window.end,
    )?;
    if buckets.is_empty() {
        let timeseries = empty_query_shadow_timeseries(window)?;
        return Ok(QUERY_SHADOW_TIMING_PERCENTILES
            .into_iter()
            .map(|percentile| (percentile, timeseries.clone()))
            .collect());
    }
    window.resample_histograms(metrics, buckets, &QUERY_SHADOW_TIMING_PERCENTILES)
}

fn query_shadow_completed_samples(
    metrics: &MetricStore<u32>,
    scope: QueryShadowMetricScope<'_>,
    window: &MetricsWindow,
) -> anyhow::Result<Vec<(SystemTime, Option<u64>)>> {
    let buckets = metrics.query_histogram(
        &query_shadow_metric(scope, "primary_seconds"),
        window.start..window.end,
    )?;
    let mut result = (0..window.num_buckets)
        .map(|index| Ok((window.bucket_start(index)?, None)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    for bucket in buckets {
        let bucket_start = metrics.bucket_start(bucket.index);
        if (window.start..window.end).contains(&bucket_start) {
            let output_index = window.bucket_index(bucket_start)?;
            let count = &mut result[output_index].1;
            *count = Some(
                count
                    .unwrap_or(0u64)
                    .checked_add(bucket.histogram.len())
                    .context("query-shadow completed sample count overflow")?,
            );
        }
    }
    Ok(result)
}

fn empty_query_shadow_timeseries(window: &MetricsWindow) -> anyhow::Result<Timeseries> {
    (0..window.num_buckets)
        .map(|index| Ok((window.bucket_start(index)?, None)))
        .collect()
}

fn query_shadow_metric(scope: QueryShadowMetricScope<'_>, metric: &str) -> MetricName {
    match scope {
        QueryShadowMetricScope::Route(udf_type, route_key) => {
            format!(
                "query_shadow:{}:route:{}:{metric}",
                udf_type.to_lowercase_string(),
                route_key.as_str()
            )
        },
        QueryShadowMetricScope::Generation(udf_type, generation_sha256) => {
            format!(
                "query_shadow:{}:generation:{generation_sha256}:{metric}",
                udf_type.to_lowercase_string()
            )
        },
    }
}

fn query_shadow_metric_names(scope: QueryShadowEvidenceScope<'_>, metric: &str) -> Vec<MetricName> {
    match scope {
        QueryShadowEvidenceScope::Routes(udf_type, route_keys) => route_keys
            .iter()
            .map(|route_key| {
                query_shadow_metric(QueryShadowMetricScope::Route(udf_type, route_key), metric)
            })
            .collect(),
        QueryShadowEvidenceScope::Generation(udf_type, generation_sha256) => {
            vec![query_shadow_metric(
                QueryShadowMetricScope::Generation(udf_type, generation_sha256),
                metric,
            )]
        },
    }
}

trait QueryShadowRegistryView {
    fn generation_sha256(&self) -> &str;
    fn route_sha256s(&self, udf_type: UdfType) -> &[String];
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl QueryShadowRegistryView for StaticHermesQueryShadowRegistry {
    fn generation_sha256(&self) -> &str {
        StaticHermesQueryShadowRegistry::generation_sha256(self)
    }

    fn route_sha256s(&self, udf_type: UdfType) -> &[String] {
        StaticHermesQueryShadowRegistry::route_sha256s(self, udf_type)
    }
}

#[cfg(test)]
struct TestQueryShadowRegistrySnapshot {
    udf_type: UdfType,
    generation_sha256: String,
    route_sha256s: Vec<String>,
}

#[cfg(test)]
impl QueryShadowRegistryView for TestQueryShadowRegistrySnapshot {
    fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }

    fn route_sha256s(&self, udf_type: UdfType) -> &[String] {
        if udf_type == self.udf_type {
            &self.route_sha256s
        } else {
            &[]
        }
    }
}

fn query_shadow_evidence_with_active_registry<R: QueryShadowRegistryView>(
    metrics: &Mutex<QueryShadowMetrics>,
    udf_type: UdfType,
    direction: QueryShadowDirection,
    read_active_registry: impl FnOnce() -> anyhow::Result<Option<R>>,
    system_time: impl FnOnce() -> SystemTime,
    cursor: Option<&QueryShadowRouteKey>,
    limit: usize,
) -> anyhow::Result<QueryShadowEvidencePage> {
    // Reports resolve the active registry while holding this lock before
    // resetting a generation timeline. Matching that order makes the evidence
    // page linearize before or after a rotation, never across one.
    let (registry, now, mut metrics) = {
        let mut metrics = metrics.lock();
        let registry = read_active_registry()?;
        let now = system_time();
        metrics.prune_expired_routes(now);
        // MetricStore uses persistent collections. The ordinary route and
        // diagnostic collections have explicit caps. Keep
        // the generation-consistent snapshot, then release the report mutex
        // before route paging and metric queries.
        (registry, now, metrics.clone())
    };
    query_shadow_evidence_page_for_direction(
        &mut metrics,
        now,
        udf_type,
        registry.as_ref().map(|registry| {
            (
                udf_type,
                registry.generation_sha256(),
                registry.route_sha256s(udf_type),
            )
        }),
        direction,
        cursor,
        limit,
    )
}

#[cfg(test)]
fn query_shadow_evidence_page(
    metrics: &mut QueryShadowMetrics,
    now: SystemTime,
    requested_udf_type: UdfType,
    registry: Option<(UdfType, &str, &[String])>,
    cursor: Option<&QueryShadowRouteKey>,
    limit: usize,
) -> anyhow::Result<QueryShadowEvidencePage> {
    query_shadow_evidence_page_for_direction(
        metrics,
        now,
        requested_udf_type,
        registry,
        QueryShadowDirection::V8PrimaryWasmShadow,
        cursor,
        limit,
    )
}

fn query_shadow_evidence_page_for_direction(
    metrics: &mut QueryShadowMetrics,
    now: SystemTime,
    requested_udf_type: UdfType,
    registry: Option<(UdfType, &str, &[String])>,
    direction: QueryShadowDirection,
    cursor: Option<&QueryShadowRouteKey>,
    limit: usize,
) -> anyhow::Result<QueryShadowEvidencePage> {
    anyhow::ensure!(
        limit > 0,
        "query-shadow evidence page limit must be positive"
    );
    metrics.prune_expired_routes(now);
    let window = metrics.retained_window(now)?;

    let Some((udf_type, generation_sha256, route_sha256s)) = registry else {
        anyhow::ensure!(
            cursor.is_none(),
            "query-shadow evidence cursor requires an active registry"
        );
        return Ok(QueryShadowEvidencePage {
            udf_type: requested_udf_type,
            direction,
            aggregate: query_shadow_evidence_summary_for_routes(
                metrics.store_for_direction(direction),
                requested_udf_type,
                &[],
                &window,
            )?,
            selection: None,
            routes: vec![],
            next_cursor: None,
        });
    };

    let mut route_keys = route_sha256s
        .iter()
        .map(|route_sha256| {
            QueryShadowRouteKey::new(generation_sha256, route_sha256)
                .context("active query-shadow registry contains an invalid route digest")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    route_keys.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    anyhow::ensure!(
        route_keys
            .windows(2)
            .all(|routes| routes[0].as_str() != routes[1].as_str()),
        "active query-shadow registry contains duplicate route digests"
    );
    let aggregate = query_shadow_evidence_summary_for_generation(
        metrics.store_for_direction(direction),
        udf_type,
        generation_sha256,
        &window,
    )?;
    let selected_route_count =
        u64::try_from(route_keys.len()).context("query-shadow selected route count exceeds u64")?;
    let mut routes_with_retained_evidence_count = 0u64;
    let mut routes_with_completed_comparison_count = 0u64;
    let mut routes_with_mismatch_count = 0u64;
    let mut routes_with_terminal_count = 0u64;
    for route_key in route_keys.iter().filter(|route_key| {
        metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::in_direction(
                udf_type, route_key, direction,
            ))
    }) {
        let summary = query_shadow_evidence_summary_for_routes(
            metrics.store_for_direction(direction),
            udf_type,
            std::slice::from_ref(route_key),
            &window,
        )?;
        if summary.evidence_count > 0 {
            routes_with_retained_evidence_count = routes_with_retained_evidence_count
                .checked_add(1)
                .context("query-shadow retained evidence route count overflow")?;
        }
        if summary.completed_count > 0 {
            routes_with_completed_comparison_count = routes_with_completed_comparison_count
                .checked_add(1)
                .context("query-shadow completed comparison route count overflow")?;
        }
        if summary.mismatch_count > 0 {
            routes_with_mismatch_count = routes_with_mismatch_count
                .checked_add(1)
                .context("query-shadow mismatch route count overflow")?;
        }
        if summary.terminal_count > 0 {
            routes_with_terminal_count = routes_with_terminal_count
                .checked_add(1)
                .context("query-shadow terminal route count overflow")?;
        }
    }
    let unobserved_route_count = selected_route_count
        .checked_sub(routes_with_retained_evidence_count)
        .context("query-shadow evidence route count exceeds selected route count")?;
    let selection = QueryShadowRegistrySelection {
        udf_type,
        generation_sha256: generation_sha256.to_owned(),
        selected_route_count,
        routes_with_retained_evidence_count,
        routes_with_completed_comparison_count,
        routes_with_mismatch_count,
        routes_with_terminal_count,
        unobserved_route_count,
    };

    let start = match cursor {
        None => 0,
        Some(cursor) => {
            anyhow::ensure!(
                cursor.generation_sha256() == generation_sha256,
                "query-shadow evidence cursor does not match the active registry generation"
            );
            route_keys
                .binary_search_by(|route_key| route_key.as_str().cmp(cursor.as_str()))
                .map(|index| index + 1)
                .map_err(|_| anyhow::anyhow!("query-shadow evidence cursor is not selected"))?
        },
    };
    let has_more = route_keys.len().saturating_sub(start) > limit;
    let page_route_keys = route_keys
        .into_iter()
        .skip(start)
        .take(limit)
        .collect::<Vec<_>>();
    let next_cursor = has_more.then(|| {
        page_route_keys
            .last()
            .expect("query-shadow evidence page has a next cursor without a route")
            .clone()
    });
    let routes = page_route_keys
        .into_iter()
        .map(|route_key| {
            let summary = query_shadow_evidence_summary_for_routes(
                metrics.store_for_direction(direction),
                udf_type,
                std::slice::from_ref(&route_key),
                &window,
            )?;
            Ok(QueryShadowRouteEvidence {
                selected_count: 1,
                unobserved_count: u64::from(summary.evidence_count == 0),
                diagnostics: metrics.diagnostics_for_route(udf_type, &route_key, direction),
                route_key,
                summary,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(QueryShadowEvidencePage {
        udf_type,
        direction,
        aggregate,
        selection: Some(selection),
        routes,
        next_cursor,
    })
}

fn query_shadow_evidence_summary_for_routes(
    metrics: &MetricStore<u32>,
    udf_type: UdfType,
    route_keys: &[QueryShadowRouteKey],
    window: &MetricsWindow,
) -> anyhow::Result<QueryShadowEvidenceSummary> {
    query_shadow_evidence_summary(
        metrics,
        QueryShadowEvidenceScope::Routes(udf_type, route_keys),
        window,
    )
}

fn query_shadow_evidence_summary_for_generation(
    metrics: &MetricStore<u32>,
    udf_type: UdfType,
    generation_sha256: &str,
    window: &MetricsWindow,
) -> anyhow::Result<QueryShadowEvidenceSummary> {
    query_shadow_evidence_summary(
        metrics,
        QueryShadowEvidenceScope::Generation(udf_type, generation_sha256),
        window,
    )
}

fn query_shadow_evidence_summary(
    metrics: &MetricStore<u32>,
    route_keys: QueryShadowEvidenceScope<'_>,
    window: &MetricsWindow,
) -> anyhow::Result<QueryShadowEvidenceSummary> {
    let primary_seconds =
        query_shadow_timing_summary_for_routes(metrics, route_keys, "primary_seconds", window)?;
    let wasm_seconds =
        query_shadow_timing_summary_for_routes(metrics, route_keys, "wasm_seconds", window)?;
    let wasm_to_primary_ratio = query_shadow_timing_summary_for_routes(
        metrics,
        route_keys,
        "wasm_to_primary_ratio",
        window,
    )?;
    let wasm_primary_seconds = query_shadow_timing_summary_for_routes(
        metrics,
        route_keys,
        "wasm_primary_seconds",
        window,
    )?;
    let v8_verifier_seconds =
        query_shadow_timing_summary_for_routes(metrics, route_keys, "v8_verifier_seconds", window)?;
    let v8_verifier_to_wasm_primary_ratio = query_shadow_timing_summary_for_routes(
        metrics,
        route_keys,
        "v8_verifier_to_wasm_primary_ratio",
        window,
    )?;
    let terminal_counts = QueryShadowTerminalCounts {
        capacity_drop: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:capacity_drop",
            window,
        )?,
        shadow_failure: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure",
            window,
        )?,
        timeout: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:timeout",
            window,
        )?,
        invalid_shadow: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:invalid_shadow",
            window,
        )?,
        invalid_primary: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:invalid_primary",
            window,
        )?,
        primary_failure: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:primary_failure",
            window,
        )?,
        primary_snapshot_rejected: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:primary_snapshot_rejected",
            window,
        )?,
        unexpected_task_exit: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:unexpected_task_exit",
            window,
        )?,
    };
    let shadow_failure_stage_counts = QueryShadowFailureStageCounts {
        transaction_setup: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_stage:transaction_setup",
            window,
        )?,
        environment_setup: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_stage:environment_setup",
            window,
        )?,
        route_authentication: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_stage:route_authentication",
            window,
        )?,
        module_loading: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_stage:module_loading",
            window,
        )?,
        wasm_execution: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_stage:wasm_execution",
            window,
        )?,
        verifier_execution: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_stage:verifier_execution",
            window,
        )?,
        transaction_finalization: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_stage:transaction_finalization",
            window,
        )?,
        unclassified: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_stage:unclassified",
            window,
        )?,
    };
    anyhow::ensure!(
        shadow_failure_stage_counts.total()? == terminal_counts.shadow_failure,
        "query-shadow failure-stage counts do not match terminal shadow-failure count"
    );
    let shadow_failure_reason_counts = QueryShadowFailureReasonCounts {
        infrastructure: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:infrastructure",
            window,
        )?,
        route_authentication: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:route_authentication",
            window,
        )?,
        module_loading: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:module_loading",
            window,
        )?,
        runtime_initialization: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:runtime_initialization",
            window,
        )?,
        initialization_timeout: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:initialization_timeout",
            window,
        )?,
        execution_timeout: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:execution_timeout",
            window,
        )?,
        instruction_budget: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:instruction_budget",
            window,
        )?,
        generated_export_dispatch: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:generated_export_dispatch",
            window,
        )?,
        guest_memory_limit: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:guest_memory_limit",
            window,
        )?,
        host_owned_bytes_limit: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:host_owned_bytes_limit",
            window,
        )?,
        opaque_handle_limit: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:opaque_handle_limit",
            window,
        )?,
        opaque_handle_space_exhausted: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:opaque_handle_space_exhausted",
            window,
        )?,
        aggregate_memory_limit: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:aggregate_memory_limit",
            window,
        )?,
        operation_limit: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:operation_limit",
            window,
        )?,
        host_abi_invariant: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:host_abi_invariant",
            window,
        )?,
        hermes_heap_out_of_memory: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:hermes_heap_out_of_memory",
            window,
        )?,
        wasmtime_trap: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:wasmtime_trap",
            window,
        )?,
        guest_execution: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:guest_execution",
            window,
        )?,
        result_finalization: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:result_finalization",
            window,
        )?,
        runtime_cleanup: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:runtime_cleanup",
            window,
        )?,
        verifier_execution: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:verifier_execution",
            window,
        )?,
        transaction_finalization: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:transaction_finalization",
            window,
        )?,
        unclassified: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "terminal:shadow_failure_reason:unclassified",
            window,
        )?,
    };
    anyhow::ensure!(
        shadow_failure_reason_counts.total()? == terminal_counts.shadow_failure,
        "query-shadow failure-reason counts do not match terminal shadow-failure count"
    );
    let completed_count = primary_seconds
        .exact_sample_count
        .checked_add(wasm_primary_seconds.exact_sample_count)
        .context("query-shadow completed count overflow")?;
    let terminal_count = terminal_counts.total()?;
    let evidence_count = completed_count
        .checked_add(terminal_count)
        .context("query-shadow evidence count overflow")?;
    let inserted_document_identity_counts = QueryShadowInsertedDocumentIdentityCounts {
        not_applicable: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:inserted_document_identity:not_applicable",
            window,
        )?,
        not_required: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:inserted_document_identity:not_required",
            window,
        )?,
        matched: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:inserted_document_identity:matched",
            window,
        )?,
        inconsistent: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:inserted_document_identity:inconsistent",
            window,
        )?,
        unresolved: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:inserted_document_identity:unresolved",
            window,
        )?,
    };
    anyhow::ensure!(
        inserted_document_identity_counts.total()? == completed_count,
        "query-shadow inserted-document identity counts do not match completed comparisons"
    );
    let mismatch_counts = QueryShadowMismatchCounts {
        snapshot: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:snapshot:mismatch",
            window,
        )?,
        result: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:result:mismatch",
            window,
        )?,
        query_journal: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:query_journal:mismatch",
            window,
        )?,
        read_dependencies: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:read_dependencies:mismatch",
            window,
        )?,
        invocation_inputs: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:invocation_inputs:mismatch",
            window,
        )?,
        host_operation_trace: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:host_operation_trace:mismatch",
            window,
        )?,
        host_operation_error: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:host_operation_error:mismatch",
            window,
        )?,
        observed_identity: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:observed_identity:mismatch",
            window,
        )?,
        observed_time: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:observed_time:mismatch",
            window,
        )?,
        observed_rng: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:observed_rng:mismatch",
            window,
        )?,
        log_lines: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:log_lines:mismatch",
            window,
        )?,
        audit_log_lines: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:audit_log_lines:mismatch",
            window,
        )?,
        write_set: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:write_set:mismatch",
            window,
        )?,
    };
    Ok(QueryShadowEvidenceSummary {
        attempt_count: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "admission:attempt",
            window,
        )?,
        admitted_count: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "admission:admitted",
            window,
        )?,
        capacity_drop_count: terminal_counts
            .capacity_drop
            .checked_add(query_shadow_counter_total_for_routes(
                metrics,
                route_keys,
                "admission:capacity_drop",
                window,
            )?)
            .context("query-shadow capacity-drop count overflow")?,
        evidence_count,
        completed_count,
        mismatch_count: query_shadow_counter_total_for_routes(
            metrics,
            route_keys,
            "comparison:mismatch",
            window,
        )?,
        mismatch_counts,
        inserted_document_identity_counts,
        terminal_count,
        terminal_counts,
        shadow_failure_stage_counts,
        shadow_failure_reason_counts,
        primary_seconds,
        wasm_seconds,
        wasm_to_primary_ratio,
        wasm_primary_seconds,
        v8_verifier_seconds,
        v8_verifier_to_wasm_primary_ratio,
    })
}

fn query_shadow_timing_summary_for_routes(
    metrics: &MetricStore<u32>,
    route_keys: QueryShadowEvidenceScope<'_>,
    metric: &str,
    window: &MetricsWindow,
) -> anyhow::Result<QueryShadowTimingSummary> {
    let mut histogram: Option<HistogramBucket<u32>> = None;
    for metric_name in query_shadow_metric_names(route_keys, metric) {
        for bucket in metrics.query_histogram(&metric_name, window.start..window.end)? {
            if let Some(histogram) = &mut histogram {
                histogram
                    .histogram
                    .add(&bucket.histogram)
                    .context("query-shadow timing histogram merge failed")?;
            } else {
                histogram = Some((*bucket).clone());
            }
        }
    }
    let Some(histogram) = histogram else {
        return Ok(QueryShadowTimingSummary {
            exact_sample_count: 0,
            p50: None,
            p90: None,
            p99: None,
            retained_max: None,
        });
    };
    let to_seconds =
        |percentile| histogram.histogram.value_at_percentile(percentile) as f64 / 1000.0;
    Ok(QueryShadowTimingSummary {
        exact_sample_count: histogram.histogram.len(),
        p50: Some(to_seconds(50.0)),
        p90: Some(to_seconds(90.0)),
        p99: Some(to_seconds(99.0)),
        retained_max: Some(to_seconds(100.0)),
    })
}

fn query_shadow_counter_total_for_routes(
    metrics: &MetricStore<u32>,
    route_keys: QueryShadowEvidenceScope<'_>,
    classification: &str,
    window: &MetricsWindow,
) -> anyhow::Result<u64> {
    let count = query_shadow_metric_names(route_keys, classification)
        .into_iter()
        .try_fold(0.0, |total, metric_name| {
            let route_count = metrics
                .query_counter(&metric_name, window.start..window.end)?
                .into_iter()
                .map(|bucket| f64::from(bucket.value))
                .sum::<f64>();
            Ok::<_, UdfMetricsError>(total + route_count)
        })?;
    anyhow::ensure!(
        count.is_finite() && count >= 0.0 && count.fract() == 0.0 && count <= u64::MAX as f64,
        "query-shadow counter is not a non-negative whole-number count"
    );
    Ok(count as u64)
}

fn sum_timeseries<'a>(
    window: &MetricsWindow,
    series: impl Iterator<Item = &'a Timeseries>,
) -> anyhow::Result<Timeseries> {
    let mut result = (0..window.num_buckets)
        .map(|i| Ok((window.bucket_start(i)?, None)))
        .collect::<anyhow::Result<Timeseries>>()?;
    for timeseries in series {
        anyhow::ensure!(timeseries.len() == result.len());
        for (i, &(ts, value)) in timeseries.iter().enumerate() {
            anyhow::ensure!(ts == result[i].0);
            if let Some(value) = value {
                *result[i].1.get_or_insert(0.) += value;
            }
        }
    }
    Ok(result)
}

fn merge_series(
    ts1: &Timeseries,
    ts2: &Timeseries,
    merge: impl Fn(Option<f64>, Option<f64>) -> Option<f64>,
) -> anyhow::Result<Timeseries> {
    anyhow::ensure!(ts2.len() == ts1.len());
    ts1.iter()
        .zip(ts2)
        .map(|(&(t1, t1_value), &(t2, t2_value))| {
            anyhow::ensure!(t1 == t2);
            Ok((t1, merge(t1_value, t2_value)))
        })
        .collect()
}

/// numerator / denominator * 100
fn percentage(numerator: Option<f64>, denominator: Option<f64>) -> Option<f64> {
    match (numerator, denominator) {
        (Some(numerator), Some(denominator)) => Some(numerator / denominator * 100.),
        (None, Some(_denominator)) => Some(0.),
        // This doesn't make sense, but could happen with mismatched timeseries or missing data
        (Some(_numerator), None) => Some(100.),
        (None, None) => None,
    }
}

/// hits / (hits + misses) * 100
fn cache_hit_percentage(hits: Option<f64>, misses: Option<f64>) -> Option<f64> {
    match (hits, misses) {
        (Some(hits), Some(misses)) => Some(hits / (hits + misses) * 100.),
        // There are hits but not misses, so the hit rate is 100.
        (Some(_hits), None) => Some(100.),
        // There are misses but not hits, so the hit rate is 0.
        (None, Some(_misses)) => Some(0.),
        (None, None) => None,
    }
}

fn resample_scheduled_job_lag(
    window: &MetricsWindow,
    metrics: &MetricStore,
    buckets: Vec<&GaugeBucket>,
) -> anyhow::Result<Timeseries> {
    let samples = buckets
        .into_iter()
        .map(|bucket| (metrics.bucket_start(bucket.index), f64::from(bucket.value)))
        .collect_vec();
    let mut next_sample = 0;
    let mut latest_sample = None;
    let mut result = Vec::with_capacity(window.num_buckets);

    for output_index in 0..window.num_buckets {
        let output_time = window.bucket_start(output_index)?;
        while next_sample < samples.len() && samples[next_sample].0 <= output_time {
            latest_sample = Some(samples[next_sample]);
            next_sample += 1;
        }
        let value = match latest_sample {
            Some((sample_time, sample_value)) => {
                let elapsed = output_time.duration_since(sample_time)?;
                Some((sample_value + elapsed.as_secs_f64()).max(0.0))
            },
            None => None,
        };
        result.push((output_time, value));
    }
    Ok(result)
}

fn query_scheduled_job_lag(
    window: &MetricsWindow,
    metrics: &MetricStore,
) -> anyhow::Result<Timeseries> {
    // A scheduler that errors before obtaining another queue decision has no new
    // sample. Seed the window with its latest retained state so a known overdue
    // queue does not disappear merely because the query's start moved past it.
    let query_start = metrics.base_ts().min(window.start);
    let buckets = metrics.query_gauge(scheduled_job_next_ts_metric(), query_start..window.end)?;
    resample_scheduled_job_lag(window, metrics, buckets)
}

fn record_scheduled_job_lag_sample(
    metrics: &mut MetricStore,
    next_job_ts: Option<SystemTime>,
    now: SystemTime,
) -> Result<(), UdfMetricsError> {
    let observed_value = next_job_ts.map_or(-f32::INFINITY, |ts| signed_duration_since(now, ts));
    // MetricStore retains one gauge value per source bucket and associates it with
    // the bucket start. Store lag at that timestamp so later interpolation
    // reconstructs `output_time - next_job_ts` instead of treating a
    // mid-bucket sample as if it occurred at the start and manufacturing lag
    // after the scheduler caught up.
    let bucket_start = metrics.bucket_start_containing(now);
    let value = next_job_ts.map_or(-f32::INFINITY, |ts| signed_duration_since(bucket_start, ts));
    match metrics.add_gauge(scheduled_job_next_ts_metric(), now, value) {
        Ok(()) => Ok(()),
        // Timestamp order no longer represents observation order after a backward
        // wall-clock step. Reset this dedicated timeline so no pre-jump ready time
        // remains available for extrapolation after the new observation.
        Err(
            UdfMetricsError::SamplePrecedesBaseTimestamp { .. }
            | UdfMetricsError::SamplePrecedesCutoff { .. },
        ) => {
            metrics.reset_timeline(now);
            metrics.add_gauge(scheduled_job_next_ts_metric(), now, observed_value)
        },
        Err(err) => Err(err),
    }
}

struct Inner<RT: Runtime> {
    rt: RT,

    log: WithHeapSize<VecDeque<(CursorMs, FunctionExecutionPart)>>,
    num_execution_completions: usize,
    log_waiters: WithHeapSize<Vec<oneshot::Sender<()>>>,
    log_manager: Arc<dyn LogSender>,
    metrics: MetricStore,
}

impl<RT: Runtime> Inner<RT> {
    fn log_execution(
        &mut self,
        execution: FunctionExecution,
        send_console_events: bool,
        log_app_metrics: bool,
    ) -> anyhow::Result<()> {
        if log_app_metrics && let Err(e) = self.log_execution_app_metrics(&execution) {
            Self::log_metrics_error(e);
        }
        let next_time = self.next_time()?;

        // Gather log lines
        let mut log_events = if send_console_events {
            execution.console_log_events()
        } else {
            vec![]
        };
        // Gather UDF execution record
        match execution.udf_execution_record_log_events() {
            Ok(records) => log_events.extend(records),
            Err(mut e) => {
                // Don't let failing to construct the UDF execution record block sending
                // the other log events
                tracing::error!("failed to create UDF execution record: {}", e);
                report_error_sync(&mut e);
            },
        }

        self.log_manager.send_logs(log_events);

        self.log
            .push_back((next_time, FunctionExecutionPart::Completion(execution)));
        self.num_execution_completions += 1;
        while self.num_execution_completions > *knobs::MAX_UDF_EXECUTION {
            let front = self.log.pop_front();
            if let Some((_, FunctionExecutionPart::Completion(_))) = front {
                self.num_execution_completions -= 1;
            }
        }
        for waiter in self.log_waiters.drain(..) {
            let _ = waiter.send(());
        }
        Ok(())
    }

    fn log_metrics_error(error: UdfMetricsError) {
        // Only log an error to tracing and/or Sentry at most once every 10 seconds per
        // thread.
        thread_local! {
            static LAST_LOGGED_ERROR: Cell<Option<SystemTime>> = const { Cell::new(None) };
        }
        let now = SystemTime::now();
        let should_log = LAST_LOGGED_ERROR.get().is_none_or(|last_logged| {
            now.duration_since(last_logged).unwrap_or(Duration::ZERO) >= Duration::from_secs(10)
        });
        if !should_log {
            return;
        }
        LAST_LOGGED_ERROR.set(Some(now));
        tracing::error!("Failed to log application metrics: {}", error);
        if let UdfMetricsError::InternalError(mut e) = error {
            report_error_sync(&mut e);
        }
    }

    fn log_execution_app_metrics(
        &mut self,
        execution: &FunctionExecution,
    ) -> Result<(), UdfMetricsError> {
        let ts = execution.unix_timestamp.as_system_time();

        let identifier = execution.identifier();

        let name = udf_invocations_metric(&identifier);
        self.metrics.add_counter(&name, ts, 1.0)?;

        let is_err = match &execution.params {
            UdfParams::Function { error, .. } => error.is_some(),
            UdfParams::Http { result, .. } => result.is_err(),
        };
        if is_err {
            let name = udf_errors_metric(&identifier);
            self.metrics.add_counter(&name, ts, 1.0)?;
        }
        if execution.udf_type == UdfType::Query {
            if execution.cached_result {
                let name = udf_cache_hits_metric(&identifier);
                self.metrics.add_counter(&name, ts, 1.0)?;
            } else {
                let name = udf_cache_misses_metric(&identifier);
                self.metrics.add_counter(&name, ts, 1.0)?;
            }
        }

        let name = udf_execution_time_metric(&identifier);
        self.metrics
            .add_histogram(&name, ts, Duration::from_secs_f64(execution.execution_time))?;

        for (table_name, table_stats) in &execution.tables_touched {
            let name = table_rows_read_metric(table_name);
            self.metrics
                .add_counter(&name, ts, table_stats.rows_read as f32)?;
            let name = table_rows_written_metric(table_name);
            self.metrics
                .add_counter(&name, ts, table_stats.rows_written as f32)?;
        }
        Ok(())
    }

    fn log_execution_progress(
        &mut self,
        log_lines: LogLines,
        event_source: FunctionEventSource,
        function_start_timestamp: UnixTimestamp,
    ) -> anyhow::Result<()> {
        let next_time = self.next_time()?;
        let progress = FunctionExecutionProgress {
            log_lines,
            event_source,
            function_start_timestamp,
        };

        let log_events = progress.console_log_events();
        self.log_manager.send_logs(log_events);
        self.log
            .push_back((next_time, FunctionExecutionPart::Progress(progress)));
        for waiter in self.log_waiters.drain(..) {
            let _ = waiter.send(());
        }
        Ok(())
    }

    fn log_scheduled_job_stats(
        &mut self,
        next_job_ts: Option<SystemTime>,
        now: SystemTime,
        num_running_jobs: u64,
    ) -> anyhow::Result<()> {
        // -Infinity means there is no scheduled job
        let value = next_job_ts.map_or(-f32::INFINITY, |ts| signed_duration_since(now, ts));
        if value > 0.0 {
            self.log_manager.send_logs(vec![LogEvent {
                timestamp: UnixTimestamp::from_system_time(now).context("now < UNIX_EPOCH?")?,
                event: StructuredLogEvent::ScheduledJobLag {
                    lag_seconds: Duration::from_secs_f32(value.max(0.0)),
                },
            }]);
        }
        if value > 0.0 || num_running_jobs > 0 {
            self.log_manager.send_logs(vec![LogEvent {
                timestamp: UnixTimestamp::from_system_time(now).context("now < UNIX_EPOCH?")?,
                event: StructuredLogEvent::SchedulerStats {
                    lag_seconds: Duration::from_secs_f32(value.max(0.0)),
                    num_running_jobs,
                },
            }]);
        }
        Ok(())
    }

    fn next_time(&self) -> anyhow::Result<CursorMs> {
        let since_epoch = self
            .rt
            .system_time()
            .duration_since(SystemTime::UNIX_EPOCH)?;
        let mut next_time =
            (since_epoch.as_secs() as f64 * 1e3) + (since_epoch.subsec_nanos() as f64 * 1e-6);
        if let Some((last_time, _)) = self.log.back() {
            let lower_bound = last_time.next_up();
            if lower_bound > next_time {
                next_time = lower_bound;
            }
        }
        Ok(next_time)
    }
}

/// `t1 - t2` in seconds, possibly negative
fn signed_duration_since(t1: SystemTime, t2: SystemTime) -> f32 {
    match t1.duration_since(t2) {
        Ok(d) => d.as_secs_f32(),
        Err(e) => -e.duration().as_secs_f32(),
    }
}

#[cfg(test)]
mod query_shadow_metrics_tests {
    use std::sync::mpsc;

    use function_runner::{
        QueryShadowAdmissionEvent,
        QueryShadowGeneratedExportDiagnostic,
        QueryShadowTerminal,
    };

    use super::*;

    struct BlockingTestQueryShadowRegistrySnapshot {
        generation_sha256: String,
        route_sha256s: Vec<String>,
        scan_started: mpsc::SyncSender<()>,
        resume_scan: mpsc::Receiver<()>,
    }

    impl QueryShadowRegistryView for BlockingTestQueryShadowRegistrySnapshot {
        fn generation_sha256(&self) -> &str {
            &self.generation_sha256
        }

        fn route_sha256s(&self, _udf_type: UdfType) -> &[String] {
            self.scan_started.send(()).unwrap();
            self.resume_scan.recv().unwrap();
            &self.route_sha256s
        }
    }

    fn metric_store(
        base_ts: SystemTime,
        bucket_width: Duration,
        max_buckets: usize,
    ) -> MetricStore<u32> {
        MetricStore::new(
            base_ts,
            MetricStoreConfig {
                bucket_width,
                max_buckets,
                histogram_min_duration: Duration::from_millis(1),
                histogram_max_duration: Duration::from_secs(15 * 60),
                histogram_significant_figures: 3,
            },
        )
    }

    fn percentile_value(
        percentiles: &BTreeMap<Percentile, Timeseries>,
        percentile: Percentile,
    ) -> f64 {
        percentiles[&percentile][0]
            .1
            .expect("timing percentile should contain a sample")
    }

    fn matching_comparison() -> QueryShadowComparison {
        QueryShadowComparison {
            snapshot_matches: true,
            result_matches: true,
            journal_matches: true,
            read_dependencies_match: true,
            invocation_inputs_match: true,
            host_operation_trace_matches: true,
            host_operation_error_matches: true,
            observed_identity_matches: true,
            observed_time_matches: true,
            observed_rng_matches: true,
            log_lines_match: true,
            audit_log_lines_match: true,
            write_set_matches: true,
            inserted_document_identity: QueryShadowInsertedDocumentIdentity::NotApplicable,
            host_operation_trace_diagnostic: None,
            result_diagnostic: None,
        }
    }

    #[test]
    fn shadow_memory_anomalies_are_independent_of_matching_comparisons() {
        use udf::wasm_memory::{
            WasmMemoryObservation,
            WasmMemoryTerminal,
        };
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(20_000);
        let route = query_shadow_route_key('d', '1');
        let second_route = query_shadow_route_key('d', '2');
        let mut metrics = QueryShadowMetrics::new(base_ts, query_shadow_metrics_config(), 2);
        metrics.activate_generation(route.generation_sha256(), base_ts);
        for direction in [
            QueryShadowDirection::V8PrimaryWasmShadow,
            QueryShadowDirection::WasmPrimaryV8Shadow,
        ] {
            let comparison = QueryShadowReport::new_for_type_and_direction(
                UdfType::Query,
                route.clone(),
                direction,
                Duration::from_millis(1),
                Duration::from_millis(2),
                Some(Duration::from_millis(3)),
                matching_comparison(),
            );
            record_query_shadow_report_with_capacity(&mut metrics, &comparison, base_ts).unwrap();
            for index in 0..70 {
                let observation = WasmMemoryObservation {
                    reused_instance: index > 0,
                    checkout_baseline_bytes: 0,
                    peak_admitted_bytes: index,
                    peak_guest_bytes: index,
                    peak_host_bytes: 0,
                    requested_peak_bytes: index,
                    forecast_growth_bytes: 1,
                    completion_bytes: index,
                    returned_to_pool_bytes: index,
                    returned_to_pool: true,
                    forecast_overrun: true,
                    growth_denied: false,
                    successful_execution_discarded: false,
                    terminal: WasmMemoryTerminal::Success,
                };
                record_query_shadow_report_with_capacity(
                    &mut metrics,
                    &QueryShadowReport::Memory {
                        udf_type: UdfType::Query,
                        route_key: route.clone(),
                        direction,
                        observation,
                    },
                    base_ts,
                )
                .unwrap();
            }
            for index in 70..80 {
                record_query_shadow_report_with_capacity(
                    &mut metrics,
                    &QueryShadowReport::Memory {
                        udf_type: UdfType::Query,
                        route_key: second_route.clone(),
                        direction,
                        observation: WasmMemoryObservation {
                            reused_instance: true,
                            checkout_baseline_bytes: 0,
                            peak_admitted_bytes: index,
                            peak_guest_bytes: index,
                            peak_host_bytes: 0,
                            requested_peak_bytes: index,
                            forecast_growth_bytes: 1,
                            completion_bytes: index,
                            returned_to_pool_bytes: index,
                            returned_to_pool: true,
                            forecast_overrun: true,
                            growth_denied: false,
                            successful_execution_discarded: false,
                            terminal: WasmMemoryTerminal::Success,
                        },
                    },
                    base_ts,
                )
                .unwrap();
            }
            let routes = vec![
                route.route_sha256().to_owned(),
                second_route.route_sha256().to_owned(),
            ];
            let page = query_shadow_evidence_page_for_direction(
                &mut metrics,
                base_ts + Duration::from_secs(1),
                UdfType::Query,
                Some((UdfType::Query, route.generation_sha256(), &routes)),
                direction,
                None,
                2,
            )
            .unwrap();
            let summary = &page.routes[0].summary;
            assert_eq!(summary.completed_count, 1);
            assert_eq!(summary.mismatch_count, 0);
            assert_eq!(summary.terminal_count, 0);
            assert!(page.routes[0]
                .diagnostics
                .iter()
                .all(|d| matches!(d.outcome, QueryShadowDiagnosticOutcome::Memory(_))));
            assert!(!page.routes[0].diagnostics.is_empty());
            assert!(page.routes[0].diagnostics.len() <= QUERY_SHADOW_DIAGNOSTICS_PER_ROUTE);
            assert_eq!(page.aggregate.completed_count, 1);
            assert_eq!(page.routes[1].unobserved_count, 1);
            assert!(!page.routes[1].diagnostics.is_empty());
            assert!(page.routes[1]
                .diagnostics
                .iter()
                .all(|d| matches!(d.outcome, QueryShadowDiagnosticOutcome::Memory(_))));
            assert_eq!(
                page.selection.unwrap().routes_with_retained_evidence_count,
                1
            );
        }
        metrics.prune_expired_routes(base_ts + metrics.route_retention);
        assert!(metrics.diagnostics.is_empty());
    }

    #[test]
    fn ordinary_memory_observations_do_not_consume_shadow_evidence_retention() {
        use udf::wasm_memory::{
            WasmMemoryObservation,
            WasmMemoryTerminal,
        };
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(20_000);
        let route = query_shadow_route_key('d', '1');
        let mut metrics = QueryShadowMetrics::new(base_ts, query_shadow_metrics_config(), 1);
        metrics.activate_generation(route.generation_sha256(), base_ts);
        let acceptance = record_query_shadow_report_with_capacity(
            &mut metrics,
            &QueryShadowReport::Memory {
                udf_type: UdfType::Query,
                route_key: route,
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                observation: WasmMemoryObservation {
                    reused_instance: true,
                    checkout_baseline_bytes: 1,
                    peak_admitted_bytes: 1,
                    peak_guest_bytes: 1,
                    peak_host_bytes: 0,
                    requested_peak_bytes: 1,
                    forecast_growth_bytes: 1,
                    completion_bytes: 1,
                    returned_to_pool_bytes: 1,
                    returned_to_pool: true,
                    forecast_overrun: false,
                    growth_denied: false,
                    successful_execution_discarded: false,
                    terminal: WasmMemoryTerminal::Success,
                },
            },
            base_ts,
        )
        .unwrap();
        assert_eq!(acceptance, QueryShadowReportAcceptance::Ignored);
        assert!(metrics.active_routes.is_empty());
        assert!(metrics.diagnostics.is_empty());
    }

    #[test]
    fn query_shadow_diagnostics_retain_bounded_correlated_reports() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(20_000);
        let route = query_shadow_route_key('d', '1');
        let mut metrics = QueryShadowMetrics::new(base_ts, query_shadow_metrics_config(), 1);
        metrics.activate_generation(route.generation_sha256(), base_ts);

        record_query_shadow_report_with_capacity(
            &mut metrics,
            &QueryShadowReport::new(
                route.clone(),
                Duration::from_millis(1),
                Duration::from_millis(2),
                matching_comparison(),
            ),
            base_ts,
        )
        .unwrap();
        for index in 0..10 {
            let mut comparison = matching_comparison();
            comparison.result_matches = false;
            comparison.read_dependencies_match = index % 2 == 0;
            record_query_shadow_report_with_capacity(
                &mut metrics,
                &QueryShadowReport::new(
                    route.clone(),
                    Duration::from_millis(1),
                    Duration::from_millis(2),
                    comparison,
                ),
                base_ts,
            )
            .unwrap();
        }
        record_query_shadow_report_with_capacity(
            &mut metrics,
            &QueryShadowReport::Terminal {
                udf_type: UdfType::Query,
                route_key: route.clone(),
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                terminal: QueryShadowTerminal::ShadowFailure {
                    stage: QueryShadowFailureStage::WasmExecution,
                    reason: QueryShadowFailureReason::GuestExecution,
                    generated_export_diagnostic: None,
                    wasmtime_trap_diagnostic: None,
                },
            },
            base_ts,
        )
        .unwrap();

        let route_sha256s = vec![route.route_sha256().to_owned()];
        let page = query_shadow_evidence_page(
            &mut metrics,
            base_ts,
            UdfType::Query,
            Some((UdfType::Query, route.generation_sha256(), &route_sha256s)),
            None,
            1,
        )
        .unwrap();
        let diagnostics = &page.routes[0].diagnostics;
        assert_eq!(diagnostics.len(), QUERY_SHADOW_DIAGNOSTICS_PER_ROUTE);
        assert_eq!(diagnostics.first().unwrap().sequence, 4);
        assert_eq!(diagnostics.last().unwrap().sequence, 11);
        assert!(matches!(
            diagnostics.first().unwrap().outcome,
            QueryShadowDiagnosticOutcome::Mismatch(QueryShadowComparison {
                result_matches: false,
                ..
            })
        ));
        assert_eq!(
            diagnostics.last().unwrap().outcome,
            QueryShadowDiagnosticOutcome::Terminal(QueryShadowTerminal::ShadowFailure {
                stage: QueryShadowFailureStage::WasmExecution,
                reason: QueryShadowFailureReason::GuestExecution,
                generated_export_diagnostic: None,
                wasmtime_trap_diagnostic: None,
            })
        );
    }

    #[test]
    fn query_shadow_diagnostics_preserve_terminal_outcome_under_admission_pressure() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(20_050);
        let route = query_shadow_route_key('d', '1');
        let mut metrics = QueryShadowMetrics::new(base_ts, query_shadow_metrics_config(), 1);
        metrics.activate_generation(route.generation_sha256(), base_ts);

        record_query_shadow_report_with_capacity(
            &mut metrics,
            &QueryShadowReport::Terminal {
                udf_type: UdfType::Query,
                route_key: route.clone(),
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                terminal: QueryShadowTerminal::ShadowFailure {
                    stage: QueryShadowFailureStage::WasmExecution,
                    reason: QueryShadowFailureReason::GeneratedExportDispatch,
                    generated_export_diagnostic: Some(
                        QueryShadowGeneratedExportDiagnostic::SelectedEntryPreparationRejected {
                            status: 65,
                        },
                    ),
                    wasmtime_trap_diagnostic: None,
                },
            },
            base_ts,
        )
        .unwrap();
        for _ in 0..(QUERY_SHADOW_DIAGNOSTICS_PER_ROUTE + 4) {
            record_query_shadow_report_with_capacity(
                &mut metrics,
                &QueryShadowReport::Admission {
                    udf_type: UdfType::Query,
                    route_key: route.clone(),
                    direction: QueryShadowDirection::V8PrimaryWasmShadow,
                    event: QueryShadowAdmissionEvent::CapacityDrop,
                },
                base_ts,
            )
            .unwrap();
        }

        let route_sha256s = vec![route.route_sha256().to_owned()];
        let page = query_shadow_evidence_page(
            &mut metrics,
            base_ts,
            UdfType::Query,
            Some((UdfType::Query, route.generation_sha256(), &route_sha256s)),
            None,
            1,
        )
        .unwrap();
        let diagnostics = &page.routes[0].diagnostics;
        assert_eq!(diagnostics.len(), QUERY_SHADOW_DIAGNOSTICS_PER_ROUTE);
        assert!(diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.outcome,
                QueryShadowDiagnosticOutcome::Terminal(QueryShadowTerminal::ShadowFailure {
                    reason: QueryShadowFailureReason::GeneratedExportDispatch,
                    generated_export_diagnostic: Some(
                        QueryShadowGeneratedExportDiagnostic::SelectedEntryPreparationRejected {
                            status: 65
                        }
                    ),
                    ..
                })
            )
        }));
    }

    #[test]
    fn query_shadow_diagnostics_retain_admission_rejections() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(20_100);
        let route = query_shadow_route_key('d', '2');
        let mut metrics = QueryShadowMetrics::new(base_ts, query_shadow_metrics_config(), 1);
        metrics.activate_generation(route.generation_sha256(), base_ts);

        record_query_shadow_report_with_capacity(
            &mut metrics,
            &QueryShadowReport::Admission {
                udf_type: UdfType::Query,
                route_key: route.clone(),
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                event: QueryShadowAdmissionEvent::CapacityDrop,
            },
            base_ts,
        )
        .unwrap();
        record_query_shadow_report_with_capacity(
            &mut metrics,
            &QueryShadowReport::Admission {
                udf_type: UdfType::Query,
                route_key: route.clone(),
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                event: QueryShadowAdmissionEvent::PendingDrop,
            },
            base_ts,
        )
        .unwrap();

        let route_sha256s = vec![route.route_sha256().to_owned()];
        let page = query_shadow_evidence_page(
            &mut metrics,
            base_ts,
            UdfType::Query,
            Some((UdfType::Query, route.generation_sha256(), &route_sha256s)),
            None,
            1,
        )
        .unwrap();
        assert_eq!(page.aggregate.capacity_drop_count, 1);
        assert_eq!(page.routes[0].diagnostics.len(), 2);
        assert_eq!(
            page.routes[0].diagnostics[0].outcome,
            QueryShadowDiagnosticOutcome::Admission(QueryShadowAdmissionEvent::CapacityDrop)
        );
        assert_eq!(
            page.routes[0].diagnostics[1].outcome,
            QueryShadowDiagnosticOutcome::Admission(QueryShadowAdmissionEvent::PendingDrop)
        );
    }

    fn query_shadow_route_key(generation: char, route: char) -> QueryShadowRouteKey {
        QueryShadowRouteKey::new(
            &generation.to_string().repeat(64),
            &route.to_string().repeat(64),
        )
        .unwrap()
    }

    fn assert_percentile_close(actual: f64, expected: f64) {
        let tolerance = (expected.abs() * 0.001).max(0.002);
        assert!(
            (actual - expected).abs() < tolerance,
            "expected {expected}, got {actual}"
        );
    }

    fn counter_value(stats: &BTreeMap<String, Timeseries>, classification: &str) -> f64 {
        stats[classification][0]
            .1
            .expect("query-shadow counter should contain a sample")
    }

    #[test]
    fn timings_are_isolated_per_route_and_recorded_in_aggregate() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut metrics = metric_store(base_ts, Duration::from_secs(60), 2);
        let route_a = query_shadow_route_key('1', 'a');
        let route_b = query_shadow_route_key('1', 'b');
        for millis in 1u64..=100 {
            let report = QueryShadowReport::new(
                route_a.clone(),
                Duration::from_millis(millis),
                Duration::from_millis(millis * millis),
                matching_comparison(),
            );
            record_query_shadow_report_sample(
                &mut metrics,
                &report,
                base_ts + Duration::from_secs(1),
                QueryShadowReportRetention::RouteAndGeneration,
            )
            .unwrap();
        }
        let route_b_report = QueryShadowReport::new(
            route_b.clone(),
            Duration::from_secs(50),
            Duration::from_secs(50),
            matching_comparison(),
        );
        record_query_shadow_report_sample(
            &mut metrics,
            &route_b_report,
            base_ts + Duration::from_secs(1),
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };

        let route_a_stats =
            query_shadow_timing_stats(&metrics, UdfType::Query, &route_a, &window).unwrap();
        assert_eq!(
            route_a_stats
                .route
                .primary_seconds
                .keys()
                .copied()
                .collect_vec(),
            QUERY_SHADOW_TIMING_PERCENTILES
        );
        assert!((percentile_value(&route_a_stats.route.primary_seconds, 90) - 0.09).abs() < 0.002);
        assert!((percentile_value(&route_a_stats.route.primary_seconds, 99) - 0.099).abs() < 0.002);
        assert_percentile_close(
            percentile_value(&route_a_stats.route.primary_seconds, 100),
            0.1,
        );
        assert!(
            (percentile_value(&route_a_stats.route.wasm_to_primary_ratio, 90) - 90.0).abs() < 1.0
        );
        assert_percentile_close(
            percentile_value(&route_a_stats.route.wasm_to_primary_ratio, 100),
            100.0,
        );
        assert_percentile_close(
            percentile_value(&route_a_stats.aggregate.primary_seconds, 100),
            50.0,
        );
        assert_percentile_close(
            percentile_value(&route_a_stats.aggregate.wasm_seconds, 100),
            50.0,
        );
        assert_percentile_close(
            percentile_value(&route_a_stats.aggregate.wasm_to_primary_ratio, 100),
            100.0,
        );
        assert_eq!(route_a_stats.route.completed_samples[0].1, Some(100));
        assert_eq!(route_a_stats.aggregate.completed_samples[0].1, Some(101));

        let route_b_stats =
            query_shadow_timing_stats(&metrics, UdfType::Query, &route_b, &window).unwrap();
        assert_percentile_close(
            percentile_value(&route_b_stats.route.primary_seconds, 100),
            50.0,
        );
        assert_percentile_close(
            percentile_value(&route_b_stats.route.wasm_to_primary_ratio, 100),
            1.0,
        );
        assert_eq!(route_b_stats.route.completed_samples[0].1, Some(1));

        let terminal = QueryShadowReport::Terminal {
            udf_type: UdfType::Query,
            route_key: route_b.clone(),
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            terminal: QueryShadowTerminal::CapacityDrop,
        };
        record_query_shadow_report_sample(
            &mut metrics,
            &terminal,
            base_ts + Duration::from_secs(1),
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        let route_a_outcomes =
            query_shadow_outcome_stats(&metrics, UdfType::Query, &route_a, &window).unwrap();
        assert_eq!(
            counter_value(&route_a_outcomes.route, "comparison:result:match"),
            100.0
        );
        assert_eq!(
            counter_value(&route_a_outcomes.aggregate, "comparison:result:match"),
            101.0
        );
        assert!(!route_a_outcomes
            .route
            .contains_key("terminal:capacity_drop"));
        assert_eq!(
            counter_value(&route_a_outcomes.aggregate, "terminal:capacity_drop"),
            1.0
        );
        let route_b_outcomes =
            query_shadow_outcome_stats(&metrics, UdfType::Query, &route_b, &window).unwrap();
        assert_eq!(
            counter_value(&route_b_outcomes.route, "terminal:capacity_drop"),
            1.0
        );
    }

    #[test]
    fn aggregate_metrics_do_not_mix_registry_generations() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(1_500);
        let mut metrics = metric_store(base_ts, Duration::from_secs(60), 2);
        let old_route = query_shadow_route_key('1', 'a');
        let new_route = query_shadow_route_key('2', 'a');
        for (route_key, primary_millis) in [(&old_route, 10), (&new_route, 50)] {
            record_query_shadow_report_sample(
                &mut metrics,
                &QueryShadowReport::new(
                    route_key.clone(),
                    Duration::from_millis(primary_millis),
                    Duration::from_millis(primary_millis),
                    matching_comparison(),
                ),
                base_ts,
                QueryShadowReportRetention::RouteAndGeneration,
            )
            .unwrap();
        }
        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };

        let old_stats =
            query_shadow_timing_stats(&metrics, UdfType::Query, &old_route, &window).unwrap();
        let new_stats =
            query_shadow_timing_stats(&metrics, UdfType::Query, &new_route, &window).unwrap();
        assert_eq!(old_stats.aggregate.completed_samples[0].1, Some(1));
        assert_eq!(new_stats.aggregate.completed_samples[0].1, Some(1));
        assert_percentile_close(
            percentile_value(&old_stats.aggregate.primary_seconds, 100),
            0.01,
        );
        assert_percentile_close(
            percentile_value(&new_stats.aggregate.primary_seconds, 100),
            0.05,
        );
    }

    #[test]
    fn route_histograms_follow_metric_store_retention() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
        let mut metrics = metric_store(base_ts, Duration::from_secs(1), 2);
        let old_route = query_shadow_route_key('3', 'c');
        let new_route = query_shadow_route_key('3', 'd');
        let old_report = QueryShadowReport::new(
            old_route.clone(),
            Duration::from_millis(10),
            Duration::from_millis(20),
            matching_comparison(),
        );
        record_query_shadow_report_sample(
            &mut metrics,
            &old_report,
            base_ts + Duration::from_millis(100),
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        let new_report = QueryShadowReport::new(
            new_route,
            Duration::from_millis(20),
            Duration::from_millis(40),
            matching_comparison(),
        );
        record_query_shadow_report_sample(
            &mut metrics,
            &new_report,
            base_ts + Duration::from_secs(2),
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        let old_window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(1),
            num_buckets: 1,
        };

        let old_stats =
            query_shadow_timing_stats(&metrics, UdfType::Query, &old_route, &old_window).unwrap();
        for percentiles in [
            old_stats.route.primary_seconds,
            old_stats.route.wasm_seconds,
            old_stats.route.wasm_to_primary_ratio,
        ] {
            assert!(percentiles
                .values()
                .all(|timeseries| timeseries[0].1.is_none()));
        }
    }

    #[test]
    fn active_route_cap_rejects_untracked_evidence_and_releases_expired_routes() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(3_000);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(1),
            max_buckets: 1,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 1);
        let first_route = query_shadow_route_key('4', 'e');
        let omitted_route = query_shadow_route_key('4', 'f');
        assert!(metrics.has_route_detail_capacity(
            UdfType::Query,
            &first_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts
        ));
        metrics.accept_route(
            UdfType::Query,
            &first_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts,
        );
        assert!(!metrics.has_route_detail_capacity(
            UdfType::Query,
            &omitted_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts
        ));
        let after_retention = base_ts + Duration::from_secs(1);
        assert!(metrics.has_route_detail_capacity(
            UdfType::Query,
            &omitted_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            after_retention
        ));
    }

    #[test]
    fn active_route_cap_is_independent_per_direction() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(3_250);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(1),
            max_buckets: 1,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 1);
        let route = query_shadow_route_key('4', 'd');
        metrics.accept_route(
            UdfType::Query,
            &route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts,
        );
        assert!(metrics.has_route_detail_capacity(
            UdfType::Query,
            &route,
            QueryShadowDirection::WasmPrimaryV8Shadow,
            base_ts,
        ));
    }

    #[test]
    fn route_detail_cap_retains_generation_aggregate_but_rejects_the_route() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(3_500);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(60),
            max_buckets: 2,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 1);
        let retained_route = query_shadow_route_key('4', 'a');
        let omitted_route = query_shadow_route_key('4', 'b');
        let report = |route_key| QueryShadowReport::Admission {
            udf_type: UdfType::Query,
            route_key,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            event: QueryShadowAdmissionEvent::Attempt,
        };

        assert_eq!(
            record_query_shadow_report_with_capacity(
                &mut metrics,
                &report(retained_route.clone()),
                base_ts,
            )
            .unwrap(),
            QueryShadowReportAcceptance::RouteDetail
        );
        assert_eq!(
            record_query_shadow_report_with_capacity(
                &mut metrics,
                &report(retained_route.clone()),
                base_ts,
            )
            .unwrap(),
            QueryShadowReportAcceptance::RouteDetail
        );
        assert_eq!(
            record_query_shadow_report_with_capacity(
                &mut metrics,
                &report(omitted_route.clone()),
                base_ts,
            )
            .unwrap(),
            QueryShadowReportAcceptance::GenerationAggregateOnly
        );

        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };
        assert_eq!(
            query_shadow_evidence_summary_for_generation(
                &metrics.store,
                UdfType::Query,
                retained_route.generation_sha256(),
                &window,
            )
            .unwrap()
            .attempt_count,
            3
        );
        assert_eq!(
            query_shadow_evidence_summary_for_routes(
                &metrics.store,
                UdfType::Query,
                std::slice::from_ref(&retained_route),
                &window,
            )
            .unwrap()
            .attempt_count,
            2
        );
        assert_eq!(
            query_shadow_evidence_summary_for_routes(
                &metrics.store,
                UdfType::Query,
                std::slice::from_ref(&omitted_route),
                &window,
            )
            .unwrap()
            .attempt_count,
            0
        );
    }

    #[test]
    fn active_generation_reclaims_route_detail_capacity_after_rotation() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(4_000);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(60),
            max_buckets: 2,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 1);
        let old_route = query_shadow_route_key('5', 'a');
        let active_route = query_shadow_route_key('6', 'a');

        assert!(metrics.has_route_detail_capacity(
            UdfType::Query,
            &old_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts
        ));
        metrics.accept_route(
            UdfType::Query,
            &old_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts,
        );
        assert!(!metrics.has_route_detail_capacity(
            UdfType::Query,
            &active_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts
        ));

        metrics.activate_generation(active_route.generation_sha256(), base_ts);
        assert!(!metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::new(UdfType::Query, &old_route)));
        assert!(metrics.has_route_detail_capacity(
            UdfType::Query,
            &active_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts
        ));
    }

    #[test]
    fn late_authenticated_generation_report_preserves_active_route_capacity() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(4_250);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(60),
            max_buckets: 2,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 1);
        let active_route = query_shadow_route_key('6', 'a');
        let late_route = query_shadow_route_key('5', 'b');
        let report = |route_key| QueryShadowReport::Admission {
            udf_type: UdfType::Query,
            route_key,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            event: QueryShadowAdmissionEvent::Attempt,
        };

        assert_eq!(
            record_authenticated_query_shadow_report(
                &mut metrics,
                &report(active_route.clone()),
                active_route.generation_sha256(),
                base_ts,
            )
            .unwrap(),
            QueryShadowReportAcceptance::RouteDetail,
        );
        assert_eq!(metrics.active_routes.len(), 1);
        assert_eq!(
            record_authenticated_query_shadow_report(
                &mut metrics,
                &report(late_route.clone()),
                active_route.generation_sha256(),
                base_ts + Duration::from_secs(1),
            )
            .unwrap(),
            QueryShadowReportAcceptance::InactiveGenerationAggregateOnly,
        );
        assert!(metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::new(UdfType::Query, &active_route)));
        assert!(!metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::new(UdfType::Query, &late_route)));
        assert_eq!(
            metrics.active_generation_sha256.as_deref(),
            Some(active_route.generation_sha256()),
        );

        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };
        assert_eq!(
            query_shadow_evidence_summary_for_generation(
                &metrics.store,
                UdfType::Query,
                active_route.generation_sha256(),
                &window,
            )
            .unwrap()
            .attempt_count,
            1,
        );
        assert_eq!(
            query_shadow_evidence_summary_for_generation(
                &metrics.store,
                UdfType::Query,
                late_route.generation_sha256(),
                &window,
            )
            .unwrap()
            .attempt_count,
            1,
        );
        assert_eq!(
            query_shadow_evidence_summary_for_routes(
                &metrics.store,
                UdfType::Query,
                std::slice::from_ref(&late_route),
                &window,
            )
            .unwrap()
            .attempt_count,
            0,
        );
    }

    #[test]
    fn late_authenticated_generation_report_survives_initial_active_report() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(4_300);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(60),
            max_buckets: 2,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 1);
        let active_route = query_shadow_route_key('6', 'a');
        let late_route = query_shadow_route_key('5', 'b');
        let report = |route_key| QueryShadowReport::Admission {
            udf_type: UdfType::Query,
            route_key,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            event: QueryShadowAdmissionEvent::Attempt,
        };

        assert_eq!(
            record_authenticated_query_shadow_report(
                &mut metrics,
                &report(late_route.clone()),
                late_route.generation_sha256(),
                base_ts,
            )
            .unwrap(),
            QueryShadowReportAcceptance::RouteDetail,
        );
        assert_eq!(
            record_authenticated_query_shadow_report(
                &mut metrics,
                &report(late_route.clone()),
                active_route.generation_sha256(),
                base_ts + Duration::from_secs(1),
            )
            .unwrap(),
            QueryShadowReportAcceptance::InactiveGenerationAggregateOnly,
        );
        assert_eq!(
            metrics.active_generation_sha256.as_deref(),
            Some(active_route.generation_sha256()),
        );
        assert!(metrics.active_routes.is_empty());
        assert_eq!(
            record_authenticated_query_shadow_report(
                &mut metrics,
                &report(active_route.clone()),
                active_route.generation_sha256(),
                base_ts + Duration::from_secs(2),
            )
            .unwrap(),
            QueryShadowReportAcceptance::RouteDetail,
        );

        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };
        for generation_sha256 in [
            late_route.generation_sha256(),
            active_route.generation_sha256(),
        ] {
            assert_eq!(
                query_shadow_evidence_summary_for_generation(
                    &metrics.store,
                    UdfType::Query,
                    generation_sha256,
                    &window,
                )
                .unwrap()
                .attempt_count,
                1,
            );
        }
        assert!(!metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::new(UdfType::Query, &late_route)));
        assert!(metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::new(UdfType::Query, &active_route)));
    }

    #[test]
    fn late_authenticated_generation_report_rejects_backward_clock_without_resetting_active_state()
    {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(4_350);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(60),
            max_buckets: 2,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 1);
        let active_route = query_shadow_route_key('6', 'a');
        let late_route = query_shadow_route_key('5', 'b');
        let report = |route_key| QueryShadowReport::Admission {
            udf_type: UdfType::Query,
            route_key,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            event: QueryShadowAdmissionEvent::Attempt,
        };

        assert_eq!(
            record_authenticated_query_shadow_report(
                &mut metrics,
                &report(active_route.clone()),
                active_route.generation_sha256(),
                base_ts + Duration::from_secs(61),
            )
            .unwrap(),
            QueryShadowReportAcceptance::RouteDetail,
        );
        let active_base_ts = metrics.store.base_ts();
        assert!(matches!(
            record_authenticated_query_shadow_report(
                &mut metrics,
                &report(late_route.clone()),
                active_route.generation_sha256(),
                base_ts + Duration::from_secs(1),
            ),
            Err(UdfMetricsError::SamplePrecedesBaseTimestamp { .. }
                | UdfMetricsError::SamplePrecedesCutoff { .. })
        ));
        assert_eq!(metrics.store.base_ts(), active_base_ts);
        assert!(metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::new(UdfType::Query, &active_route)));
        assert!(!metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::new(UdfType::Query, &late_route)));

        let window = MetricsWindow {
            start: base_ts + Duration::from_secs(1),
            end: base_ts + Duration::from_secs(61),
            num_buckets: 1,
        };
        assert_eq!(
            query_shadow_evidence_summary_for_generation(
                &metrics.store,
                UdfType::Query,
                active_route.generation_sha256(),
                &window,
            )
            .unwrap()
            .attempt_count,
            1,
        );
        assert_eq!(
            query_shadow_evidence_summary_for_generation(
                &metrics.store,
                UdfType::Query,
                late_route.generation_sha256(),
                &window,
            )
            .unwrap()
            .attempt_count,
            0,
        );
    }

    #[test]
    fn backward_clock_step_restarts_evidence_and_route_capacity() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(4_500);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(60),
            max_buckets: 2,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 1);
        let first_route = query_shadow_route_key('6', 'a');
        let recovered_route = query_shadow_route_key('6', 'b');
        let report = |route_key| QueryShadowReport::Admission {
            udf_type: UdfType::Query,
            route_key,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            event: QueryShadowAdmissionEvent::Attempt,
        };
        metrics.activate_generation(first_route.generation_sha256(), base_ts);

        assert_eq!(
            record_query_shadow_report_with_clock_recovery(
                &mut metrics,
                &report(first_route.clone()),
                base_ts + Duration::from_secs(61),
            )
            .unwrap(),
            QueryShadowReportAcceptance::RouteDetail
        );
        assert_eq!(metrics.active_routes.len(), 1);

        assert_eq!(
            record_query_shadow_report_with_clock_recovery(
                &mut metrics,
                &report(recovered_route.clone()),
                base_ts + Duration::from_secs(1),
            )
            .unwrap(),
            QueryShadowReportAcceptance::RouteDetail
        );
        assert_eq!(metrics.store.base_ts(), base_ts + Duration::from_secs(1));
        assert!(!metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::new(UdfType::Query, &first_route)));
        assert!(metrics
            .active_routes
            .contains_key(&ShadowMetricRouteKey::new(UdfType::Query, &recovered_route)));

        let window = MetricsWindow {
            start: base_ts + Duration::from_secs(1),
            end: base_ts + Duration::from_secs(61),
            num_buckets: 1,
        };
        assert_eq!(
            query_shadow_evidence_summary_for_generation(
                &metrics.store,
                UdfType::Query,
                recovered_route.generation_sha256(),
                &window,
            )
            .unwrap()
            .attempt_count,
            1
        );
    }

    #[test]
    fn zero_primary_duration_records_completed_evidence_without_a_ratio() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(5_000);
        let mut metrics = metric_store(base_ts, Duration::from_secs(60), 2);
        let route = query_shadow_route_key('7', 'a');
        let report = QueryShadowReport::new(
            route.clone(),
            Duration::ZERO,
            Duration::from_millis(20),
            matching_comparison(),
        );
        record_query_shadow_report_sample(
            &mut metrics,
            &report,
            base_ts,
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };

        let summary = query_shadow_evidence_summary_for_routes(
            &metrics,
            UdfType::Query,
            std::slice::from_ref(&route),
            &window,
        )
        .unwrap();
        assert_eq!(summary.completed_count, 1);
        assert_eq!(summary.primary_seconds.exact_sample_count, 1);
        assert_eq!(summary.wasm_to_primary_ratio.exact_sample_count, 0);
        assert_eq!(summary.wasm_to_primary_ratio.p90, None);
    }

    #[test]
    fn wasm_primary_comparison_counts_as_completed_evidence() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(5_500);
        let config = query_shadow_metrics_config();
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 2);
        let route = query_shadow_route_key('7', 'b');
        let report = QueryShadowReport::new_for_type_and_direction(
            UdfType::Query,
            route.clone(),
            QueryShadowDirection::WasmPrimaryV8Shadow,
            Duration::from_millis(20),
            Duration::from_millis(20),
            Some(Duration::from_millis(5)),
            matching_comparison(),
        );
        assert_eq!(
            record_query_shadow_report_with_capacity(&mut metrics, &report, base_ts).unwrap(),
            QueryShadowReportAcceptance::RouteDetail
        );
        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };
        let summary = query_shadow_evidence_summary_for_routes(
            metrics.store_for_direction(QueryShadowDirection::WasmPrimaryV8Shadow),
            UdfType::Query,
            std::slice::from_ref(&route),
            &window,
        )
        .unwrap();
        assert_eq!(summary.completed_count, 1);
        assert_eq!(summary.evidence_count, 1);
        assert_eq!(summary.wasm_primary_seconds.exact_sample_count, 1);
    }

    #[test]
    fn invalid_primary_terminal_is_retained_separately_from_primary_failure() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(6_000);
        let mut metrics = metric_store(base_ts, Duration::from_secs(60), 2);
        let route = query_shadow_route_key('8', 'a');
        let report = QueryShadowReport::Terminal {
            udf_type: UdfType::Query,
            route_key: route.clone(),
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            terminal: QueryShadowTerminal::InvalidPrimary,
        };
        record_query_shadow_report_sample(
            &mut metrics,
            &report,
            base_ts,
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };

        let summary = query_shadow_evidence_summary_for_routes(
            &metrics,
            UdfType::Query,
            std::slice::from_ref(&route),
            &window,
        )
        .unwrap();
        assert_eq!(summary.evidence_count, 1);
        assert_eq!(summary.terminal_count, 1);
        assert_eq!(
            summary.terminal_counts,
            QueryShadowTerminalCounts {
                invalid_primary: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn unexpected_task_exit_terminal_is_retained_as_fixed_evidence() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(6_500);
        let mut metrics = metric_store(base_ts, Duration::from_secs(60), 2);
        let route = query_shadow_route_key('8', 'b');
        let report = QueryShadowReport::Terminal {
            udf_type: UdfType::Query,
            route_key: route.clone(),
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            terminal: QueryShadowTerminal::UnexpectedTaskExit,
        };
        record_query_shadow_report_sample(
            &mut metrics,
            &report,
            base_ts,
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };

        let summary = query_shadow_evidence_summary_for_routes(
            &metrics,
            UdfType::Query,
            std::slice::from_ref(&route),
            &window,
        )
        .unwrap();
        assert_eq!(summary.completed_count, 0);
        assert_eq!(summary.terminal_count, 1);
        assert_eq!(summary.terminal_counts.unexpected_task_exit, 1);
    }

    #[test]
    fn shadow_failure_stages_preserve_parent_stage_equality_for_each_retention_scope() {
        let expected_stages = QueryShadowFailureStageCounts {
            transaction_setup: 1,
            environment_setup: 1,
            route_authentication: 1,
            module_loading: 1,
            wasm_execution: 4,
            verifier_execution: 1,
            transaction_finalization: 1,
            unclassified: 1,
        };
        let expected_reasons = QueryShadowFailureReasonCounts {
            initialization_timeout: 1,
            execution_timeout: 1,
            instruction_budget: 1,
            verifier_execution: 1,
            unclassified: 7,
            ..Default::default()
        };
        for (retention, route_is_retained) in [
            (QueryShadowReportRetention::RouteAndGeneration, true),
            (QueryShadowReportRetention::GenerationOnly, false),
        ] {
            let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(6_750);
            let mut metrics = metric_store(base_ts, Duration::from_secs(60), 2);
            let route = query_shadow_route_key('8', 'c');
            for stage in [
                QueryShadowFailureStage::TransactionSetup,
                QueryShadowFailureStage::EnvironmentSetup,
                QueryShadowFailureStage::RouteAuthentication,
                QueryShadowFailureStage::ModuleLoading,
                QueryShadowFailureStage::WasmExecution,
                QueryShadowFailureStage::VerifierExecution,
                QueryShadowFailureStage::TransactionFinalization,
                QueryShadowFailureStage::Unclassified,
            ] {
                let reason = match stage {
                    QueryShadowFailureStage::VerifierExecution => {
                        QueryShadowFailureReason::VerifierExecution
                    },
                    QueryShadowFailureStage::TransactionSetup
                    | QueryShadowFailureStage::EnvironmentSetup
                    | QueryShadowFailureStage::RouteAuthentication
                    | QueryShadowFailureStage::ModuleLoading
                    | QueryShadowFailureStage::WasmExecution
                    | QueryShadowFailureStage::TransactionFinalization
                    | QueryShadowFailureStage::Unclassified => {
                        QueryShadowFailureReason::Unclassified
                    },
                };
                record_query_shadow_report_sample(
                    &mut metrics,
                    &QueryShadowReport::Terminal {
                        udf_type: UdfType::Query,
                        route_key: route.clone(),
                        direction: QueryShadowDirection::V8PrimaryWasmShadow,
                        terminal: QueryShadowTerminal::ShadowFailure {
                            stage,
                            reason,
                            generated_export_diagnostic: None,
                            wasmtime_trap_diagnostic: None,
                        },
                    },
                    base_ts,
                    retention,
                )
                .unwrap();
            }
            for reason in [
                QueryShadowFailureReason::InitializationTimeout,
                QueryShadowFailureReason::ExecutionTimeout,
                QueryShadowFailureReason::InstructionBudget,
            ] {
                record_query_shadow_report_sample(
                    &mut metrics,
                    &QueryShadowReport::Terminal {
                        udf_type: UdfType::Query,
                        route_key: route.clone(),
                        direction: QueryShadowDirection::V8PrimaryWasmShadow,
                        terminal: QueryShadowTerminal::ShadowFailure {
                            stage: QueryShadowFailureStage::WasmExecution,
                            reason,
                            generated_export_diagnostic: None,
                            wasmtime_trap_diagnostic: None,
                        },
                    },
                    base_ts,
                    retention,
                )
                .unwrap();
            }
            let window = MetricsWindow {
                start: base_ts,
                end: base_ts + Duration::from_secs(60),
                num_buckets: 1,
            };

            let aggregate = query_shadow_evidence_summary_for_generation(
                &metrics,
                UdfType::Query,
                route.generation_sha256(),
                &window,
            )
            .unwrap();
            assert_eq!(aggregate.evidence_count, 11);
            assert_eq!(aggregate.terminal_count, 11);
            assert_eq!(aggregate.terminal_counts.shadow_failure, 11);
            assert_eq!(aggregate.shadow_failure_reason_counts.total().unwrap(), 11);
            assert_eq!(aggregate.shadow_failure_stage_counts, expected_stages);
            assert_eq!(aggregate.shadow_failure_reason_counts, expected_reasons);

            let route_summary = query_shadow_evidence_summary_for_routes(
                &metrics,
                UdfType::Query,
                std::slice::from_ref(&route),
                &window,
            )
            .unwrap();
            assert_eq!(
                route_summary.terminal_count,
                route_is_retained.then_some(11).unwrap_or(0)
            );
            assert_eq!(
                route_summary.shadow_failure_stage_counts,
                route_is_retained
                    .then_some(expected_stages.clone())
                    .unwrap_or_default()
            );
            assert_eq!(
                route_summary.shadow_failure_reason_counts,
                route_is_retained
                    .then_some(expected_reasons.clone())
                    .unwrap_or_default()
            );
        }
    }

    #[test]
    fn shadow_failure_stage_write_error_rolls_back_the_parent_terminal() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(6_875);
        let mut metrics = metric_store(base_ts, Duration::from_secs(60), 2);
        let route = query_shadow_route_key('8', 'd');
        let stage_metric = query_shadow_metric(
            QueryShadowMetricScope::Generation(UdfType::Query, route.generation_sha256()),
            "terminal:shadow_failure_stage:wasm_execution",
        );
        metrics
            .add_histogram(&stage_metric, base_ts, Duration::from_millis(1))
            .unwrap();

        let error = record_query_shadow_report_sample(
            &mut metrics,
            &QueryShadowReport::Terminal {
                udf_type: UdfType::Query,
                route_key: route.clone(),
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                terminal: QueryShadowTerminal::ShadowFailure {
                    stage: QueryShadowFailureStage::WasmExecution,
                    reason: QueryShadowFailureReason::Unclassified,
                    generated_export_diagnostic: None,
                    wasmtime_trap_diagnostic: None,
                },
            },
            base_ts,
            QueryShadowReportRetention::GenerationOnly,
        )
        .expect_err("a conflicting stage metric must reject the terminal report");
        assert!(error.to_string().contains("Metric type mismatch"));

        assert!(
            metrics
                .query_counter(
                    &query_shadow_metric(
                        QueryShadowMetricScope::Generation(
                            UdfType::Query,
                            route.generation_sha256(),
                        ),
                        "terminal:shadow_failure",
                    ),
                    base_ts..base_ts + Duration::from_secs(60),
                )
                .unwrap()
                .is_empty(),
            "rejected stage evidence retained its parent terminal"
        );

        let window = MetricsWindow {
            start: base_ts,
            end: base_ts + Duration::from_secs(60),
            num_buckets: 1,
        };
        assert!(query_shadow_evidence_summary_for_generation(
            &metrics,
            UdfType::Query,
            route.generation_sha256(),
            &window,
        )
        .is_err());
    }

    #[test]
    fn evidence_snapshot_serializes_registry_read_with_generation_rotation() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(7_000);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(60),
            max_buckets: 2,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let metrics = Mutex::new(QueryShadowMetrics::new(base_ts, config, 1));
        let old_route = query_shadow_route_key('9', 'a');
        let active_route = query_shadow_route_key('a', 'b');
        let report = |route_key| QueryShadowReport::Terminal {
            udf_type: UdfType::Query,
            route_key,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            terminal: QueryShadowTerminal::ShadowFailure {
                stage: QueryShadowFailureStage::Unclassified,
                reason: QueryShadowFailureReason::Unclassified,
                generated_export_diagnostic: None,
                wasmtime_trap_diagnostic: None,
            },
        };

        {
            let mut metrics = metrics.lock();
            metrics.activate_generation(old_route.generation_sha256(), base_ts);
            record_query_shadow_report_with_capacity(&mut metrics, &report(old_route), base_ts)
                .unwrap();

            // This models a report that wins the evidence lock during a registry
            // rotation and resets the retained timeline for the new generation.
            metrics.activate_generation(active_route.generation_sha256(), base_ts);
            record_query_shadow_report_with_capacity(
                &mut metrics,
                &report(active_route.clone()),
                base_ts,
            )
            .unwrap();
        }

        let active_generation_sha256 = active_route.generation_sha256().to_owned();
        let active_route_sha256 = active_route.route_sha256().to_owned();
        let page = query_shadow_evidence_with_active_registry(
            &metrics,
            UdfType::Query,
            QueryShadowDirection::V8PrimaryWasmShadow,
            || {
                // This makes the synchronization invariant deterministic: the
                // registry read must occur while the evidence lock is held.
                assert!(metrics.try_lock().is_none());
                Ok(Some(TestQueryShadowRegistrySnapshot {
                    udf_type: UdfType::Query,
                    generation_sha256: active_generation_sha256,
                    route_sha256s: vec![active_route_sha256],
                }))
            },
            || {
                assert!(metrics.try_lock().is_none());
                base_ts + Duration::from_secs(1)
            },
            None,
            1,
        )
        .unwrap();

        assert_eq!(page.aggregate.evidence_count, 1);
        assert_eq!(page.routes.len(), 1);
        assert_eq!(page.routes[0].route_key, active_route);
        assert_eq!(page.selection.unwrap().generation_sha256, "a".repeat(64));
    }

    #[test]
    fn evidence_route_scan_does_not_hold_metrics_lock() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(8_000);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(60),
            max_buckets: 2,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let metrics = Mutex::new(QueryShadowMetrics::new(base_ts, config, 1));
        let (scan_started_tx, scan_started_rx) = mpsc::sync_channel(0);
        let (resume_scan_tx, resume_scan_rx) = mpsc::sync_channel(0);

        std::thread::scope(|scope| {
            let evidence = scope.spawn(|| {
                query_shadow_evidence_with_active_registry(
                    &metrics,
                    UdfType::Query,
                    QueryShadowDirection::V8PrimaryWasmShadow,
                    || {
                        Ok(Some(BlockingTestQueryShadowRegistrySnapshot {
                            generation_sha256: "a".repeat(64),
                            route_sha256s: vec!["b".repeat(64)],
                            scan_started: scan_started_tx,
                            resume_scan: resume_scan_rx,
                        }))
                    },
                    || base_ts + Duration::from_secs(1),
                    None,
                    1,
                )
            });

            scan_started_rx.recv().unwrap();
            let admission_can_lock = metrics.try_lock().is_some();
            resume_scan_tx.send(()).unwrap();
            let page = evidence.join().unwrap().unwrap();

            assert!(admission_can_lock);
            assert_eq!(page.routes.len(), 1);
        });
    }

    #[test]
    fn evidence_page_uses_active_registry_selection_and_retained_summaries() {
        let base_ts = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let config = MetricStoreConfig {
            bucket_width: Duration::from_secs(60),
            max_buckets: 2,
            histogram_min_duration: Duration::from_millis(1),
            histogram_max_duration: Duration::from_secs(15 * 60),
            histogram_significant_figures: 3,
        };
        let mut metrics = QueryShadowMetrics::new(base_ts, config, 3);
        let generation_sha256 = "1".repeat(64);
        let route_b_sha256 = "b".repeat(64);
        let route_c_sha256 = "c".repeat(64);
        let route_d_sha256 = "d".repeat(64);
        let old_generation_route =
            QueryShadowRouteKey::new(&"0".repeat(64), &"a".repeat(64)).unwrap();
        let route_b = QueryShadowRouteKey::new(&generation_sha256, &route_b_sha256).unwrap();
        let route_c = QueryShadowRouteKey::new(&generation_sha256, &route_c_sha256).unwrap();
        let mut old_generation_comparison = matching_comparison();
        old_generation_comparison.result_matches = false;
        let old_generation_report = QueryShadowReport::new(
            old_generation_route.clone(),
            Duration::from_millis(30),
            Duration::from_millis(30),
            old_generation_comparison,
        );
        assert!(metrics.has_route_detail_capacity(
            UdfType::Query,
            &old_generation_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts
        ));
        metrics.accept_route(
            UdfType::Query,
            &old_generation_route,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts,
        );
        record_query_shadow_report_sample(
            &mut metrics.store,
            &old_generation_report,
            base_ts,
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        let matching = QueryShadowReport::new(
            route_b.clone(),
            Duration::from_millis(10),
            Duration::from_millis(20),
            matching_comparison(),
        );
        assert!(metrics.has_route_detail_capacity(
            UdfType::Query,
            &route_b,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts
        ));
        metrics.accept_route(
            UdfType::Query,
            &route_b,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts,
        );
        for event in [
            QueryShadowAdmissionEvent::Attempt,
            QueryShadowAdmissionEvent::Admitted,
        ] {
            record_query_shadow_report_sample(
                &mut metrics.store,
                &QueryShadowReport::Admission {
                    udf_type: UdfType::Query,
                    route_key: route_b.clone(),
                    direction: QueryShadowDirection::V8PrimaryWasmShadow,
                    event,
                },
                base_ts,
                QueryShadowReportRetention::RouteAndGeneration,
            )
            .unwrap();
        }
        record_query_shadow_report_sample(
            &mut metrics.store,
            &matching,
            base_ts,
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();

        let mut mismatching_comparison = matching_comparison();
        mismatching_comparison.invocation_inputs_match = false;
        mismatching_comparison.write_set_matches = false;
        let mismatching = QueryShadowReport::new(
            route_b.clone(),
            Duration::from_millis(20),
            Duration::from_millis(40),
            mismatching_comparison,
        );
        record_query_shadow_report_sample(
            &mut metrics.store,
            &mismatching,
            base_ts,
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();

        let terminal = QueryShadowReport::Terminal {
            udf_type: UdfType::Query,
            route_key: route_c.clone(),
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            terminal: QueryShadowTerminal::ShadowFailure {
                stage: QueryShadowFailureStage::WasmExecution,
                reason: QueryShadowFailureReason::Unclassified,
                generated_export_diagnostic: None,
                wasmtime_trap_diagnostic: None,
            },
        };
        assert!(metrics.has_route_detail_capacity(
            UdfType::Query,
            &route_c,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts
        ));
        metrics.accept_route(
            UdfType::Query,
            &route_c,
            QueryShadowDirection::V8PrimaryWasmShadow,
            base_ts,
        );
        record_query_shadow_report_sample(
            &mut metrics.store,
            &QueryShadowReport::Admission {
                udf_type: UdfType::Query,
                route_key: route_c.clone(),
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                event: QueryShadowAdmissionEvent::CapacityDrop,
            },
            base_ts,
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        record_query_shadow_report_sample(
            &mut metrics.store,
            &terminal,
            base_ts,
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();
        record_query_shadow_report_sample(
            &mut metrics.store,
            &QueryShadowReport::Terminal {
                udf_type: UdfType::Query,
                route_key: route_c.clone(),
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                terminal: QueryShadowTerminal::Timeout,
            },
            base_ts,
            QueryShadowReportRetention::RouteAndGeneration,
        )
        .unwrap();

        let route_sha256s = vec![
            route_d_sha256.clone(),
            route_b_sha256.clone(),
            route_c_sha256.clone(),
        ];
        let first_page = query_shadow_evidence_page(
            &mut metrics,
            base_ts + Duration::from_secs(1),
            UdfType::Query,
            Some((UdfType::Query, &generation_sha256, &route_sha256s)),
            None,
            2,
        )
        .unwrap();

        assert_eq!(first_page.aggregate.evidence_count, 4);
        assert_eq!(first_page.aggregate.attempt_count, 1);
        assert_eq!(first_page.aggregate.admitted_count, 1);
        assert_eq!(first_page.aggregate.capacity_drop_count, 1);
        assert_eq!(first_page.aggregate.completed_count, 2);
        assert_eq!(first_page.aggregate.mismatch_count, 1);
        assert_eq!(
            first_page.aggregate.mismatch_counts,
            QueryShadowMismatchCounts {
                invocation_inputs: 1,
                write_set: 1,
                ..Default::default()
            }
        );
        assert_eq!(first_page.aggregate.terminal_counts.shadow_failure, 1);
        assert_eq!(first_page.aggregate.terminal_counts.timeout, 1);
        assert_eq!(
            first_page.aggregate.shadow_failure_stage_counts,
            QueryShadowFailureStageCounts {
                wasm_execution: 1,
                ..Default::default()
            }
        );
        assert_eq!(first_page.aggregate.primary_seconds.exact_sample_count, 2);
        assert_eq!(first_page.aggregate.primary_seconds.p90, Some(0.02));
        assert_eq!(first_page.aggregate.primary_seconds.p99, Some(0.02));
        assert_eq!(
            first_page.aggregate.primary_seconds.retained_max,
            Some(0.02)
        );
        assert_eq!(
            first_page.selection,
            Some(QueryShadowRegistrySelection {
                udf_type: UdfType::Query,
                generation_sha256: generation_sha256.clone(),
                selected_route_count: 3,
                routes_with_retained_evidence_count: 2,
                routes_with_completed_comparison_count: 1,
                routes_with_mismatch_count: 1,
                routes_with_terminal_count: 1,
                unobserved_route_count: 1,
            })
        );
        assert_eq!(first_page.routes.len(), 2);
        assert_eq!(
            first_page.routes[0].route_key.route_sha256(),
            route_b_sha256
        );
        assert_eq!(first_page.routes[0].unobserved_count, 0);
        assert_eq!(first_page.routes[0].summary.completed_count, 2);
        assert_eq!(first_page.routes[0].summary.mismatch_count, 1);
        assert_eq!(
            first_page.routes[0].summary.mismatch_counts,
            QueryShadowMismatchCounts {
                invocation_inputs: 1,
                write_set: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            first_page.routes[1].route_key.route_sha256(),
            route_c_sha256
        );
        assert_eq!(
            first_page.routes[1].summary.terminal_counts.shadow_failure,
            1
        );
        assert_eq!(
            first_page.routes[1].summary.shadow_failure_stage_counts,
            QueryShadowFailureStageCounts {
                wasm_execution: 1,
                ..Default::default()
            }
        );
        assert_eq!(first_page.routes[1].summary.terminal_counts.timeout, 1);
        let cursor = first_page.next_cursor.unwrap();

        let second_page = query_shadow_evidence_page(
            &mut metrics,
            base_ts + Duration::from_secs(1),
            UdfType::Query,
            Some((UdfType::Query, &generation_sha256, &route_sha256s)),
            Some(&cursor),
            2,
        )
        .unwrap();
        assert_eq!(second_page.next_cursor, None);
        assert_eq!(second_page.routes.len(), 1);
        assert_eq!(
            second_page.routes[0].route_key.route_sha256(),
            route_d_sha256
        );
        assert_eq!(second_page.routes[0].unobserved_count, 1);
        assert_eq!(second_page.routes[0].summary.evidence_count, 0);
        assert_eq!(
            second_page.routes[0]
                .summary
                .primary_seconds
                .exact_sample_count,
            0
        );
        assert_eq!(second_page.routes[0].summary.primary_seconds.p90, None);

        let stale_generation_cursor =
            QueryShadowRouteKey::new(&"e".repeat(64), &route_b_sha256).unwrap();
        assert!(query_shadow_evidence_page(
            &mut metrics,
            base_ts + Duration::from_secs(1),
            UdfType::Query,
            Some((UdfType::Query, &generation_sha256, &route_sha256s)),
            Some(&stale_generation_cursor),
            2,
        )
        .is_err());

        let unselected_cursor =
            QueryShadowRouteKey::new(&generation_sha256, &"e".repeat(64)).unwrap();
        assert!(query_shadow_evidence_page(
            &mut metrics,
            base_ts + Duration::from_secs(1),
            UdfType::Query,
            Some((UdfType::Query, &generation_sha256, &route_sha256s)),
            Some(&unselected_cursor),
            2,
        )
        .is_err());
    }

    #[test]
    fn dedicated_config_bounds_retained_route_histograms() {
        let config = query_shadow_metrics_config();
        assert_eq!(config.bucket_width, Duration::from_secs(15 * 60));
        assert_eq!(config.max_buckets, 4);
        assert_eq!(config.histogram_significant_figures, 1);

        // Each route and the aggregate have exactly three timing histograms.
        // Therefore a configured cap of R retains at most (R + 1) * 3 * 4
        // histogram buckets, each using the one-significant-digit HDR layout.
        let max_active_routes = 2_048;
        assert_eq!((max_active_routes + 1) * 3 * config.max_buckets, 24_588);
    }
}

#[cfg(test)]
mod scheduled_job_lag_tests {
    use super::*;

    fn scheduled_job_metrics(base: SystemTime) -> MetricStore {
        MetricStore::new(
            base,
            MetricStoreConfig {
                bucket_width: SCHEDULED_JOB_METRICS_BUCKET_WIDTH,
                max_buckets: SCHEDULED_JOB_METRICS_MAX_BUCKETS,
                histogram_min_duration: Duration::from_millis(1),
                histogram_max_duration: Duration::from_secs(15 * 60),
                histogram_significant_figures: 3,
            },
        )
    }

    #[test]
    fn resampling_uses_lag_at_the_source_bucket_start() -> anyhow::Result<()> {
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        let mut metrics = scheduled_job_metrics(base);
        let sample_time = SystemTime::UNIX_EPOCH + Duration::from_secs(45);
        let next_job_time = SystemTime::UNIX_EPOCH + Duration::from_secs(46);
        record_scheduled_job_lag_sample(&mut metrics, Some(next_job_time), sample_time)?;

        let window = MetricsWindow {
            start: SystemTime::UNIX_EPOCH + Duration::from_secs(30),
            end: SystemTime::UNIX_EPOCH + Duration::from_secs(75),
            num_buckets: 3,
        };
        let buckets =
            metrics.query_gauge(scheduled_job_next_ts_metric(), window.start..window.end)?;
        let result = resample_scheduled_job_lag(&window, &metrics, buckets)?;

        assert_eq!(result[0].1, None);
        assert_eq!(result[1].1, Some(0.0));
        assert_eq!(result[2].1, Some(14.0));
        Ok(())
    }

    #[test]
    fn backward_clock_step_before_the_store_base_discards_pre_jump_state() -> anyhow::Result<()> {
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let mut metrics = scheduled_job_metrics(base);
        let latest_bucket_start = base + Duration::from_secs(30);
        record_scheduled_job_lag_sample(
            &mut metrics,
            Some(base + Duration::from_secs(20)),
            latest_bucket_start,
        )?;

        record_scheduled_job_lag_sample(
            &mut metrics,
            // Future relative to the post-jump clock, but earlier than the
            // pre-jump store timeline.
            Some(base - Duration::from_secs(5)),
            base - Duration::from_secs(10),
        )?;

        let window = MetricsWindow {
            start: base - Duration::from_secs(10),
            end: base + Duration::from_secs(5),
            num_buckets: 1,
        };
        let result = query_scheduled_job_lag(&window, &metrics)?;

        assert_eq!(metrics.base_ts(), base - Duration::from_secs(10));
        assert_eq!(result, vec![(base - Duration::from_secs(10), Some(0.0))]);
        Ok(())
    }

    #[test]
    fn backward_clock_step_before_the_latest_bucket_removes_stale_ready_time() -> anyhow::Result<()>
    {
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let mut metrics = scheduled_job_metrics(base);
        let previous_ready_time = base + Duration::from_secs(5);
        record_scheduled_job_lag_sample(&mut metrics, Some(previous_ready_time), base)?;
        record_scheduled_job_lag_sample(
            &mut metrics,
            Some(previous_ready_time),
            base + Duration::from_secs(30),
        )?;

        let post_jump_now = base + Duration::from_secs(10);
        record_scheduled_job_lag_sample(
            &mut metrics,
            Some(base + Duration::from_secs(20)),
            post_jump_now,
        )?;

        let window = MetricsWindow {
            start: post_jump_now,
            end: post_jump_now + SCHEDULED_JOB_METRICS_BUCKET_WIDTH,
            num_buckets: 1,
        };
        let result = query_scheduled_job_lag(&window, &metrics)?;

        // Retaining the pre-jump bucket would extrapolate five seconds of lag.
        assert_eq!(metrics.base_ts(), post_jump_now);
        assert_eq!(result, vec![(post_jump_now, Some(0.0))]);
        Ok(())
    }

    #[test]
    fn sub_interval_future_change_does_not_extrapolate_stale_lag() -> anyhow::Result<()> {
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        let mut metrics = scheduled_job_metrics(base);
        let previous_ready_time = SystemTime::UNIX_EPOCH + Duration::from_secs(46);
        record_scheduled_job_lag_sample(
            &mut metrics,
            Some(previous_ready_time),
            SystemTime::UNIX_EPOCH + Duration::from_secs(40),
        )?;

        let next_ready_time = previous_ready_time + Duration::from_secs(10);
        record_scheduled_job_lag_sample(
            &mut metrics,
            Some(next_ready_time),
            SystemTime::UNIX_EPOCH + Duration::from_secs(41),
        )?;

        let window = MetricsWindow {
            start: SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            end: SystemTime::UNIX_EPOCH + Duration::from_secs(65),
            num_buckets: 3,
        };
        let buckets =
            metrics.query_gauge(scheduled_job_next_ts_metric(), window.start..window.end)?;
        let result = resample_scheduled_job_lag(&window, &metrics, buckets)?;

        // Extrapolating the previous ready time would report four seconds here.
        assert_eq!(
            result[2],
            (SystemTime::UNIX_EPOCH + Duration::from_secs(50), Some(0.0))
        );
        Ok(())
    }

    #[test]
    fn empty_queue_does_not_extrapolate_stale_lag() -> anyhow::Result<()> {
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        let mut metrics = scheduled_job_metrics(base);
        record_scheduled_job_lag_sample(
            &mut metrics,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(46)),
            SystemTime::UNIX_EPOCH + Duration::from_secs(40),
        )?;
        record_scheduled_job_lag_sample(
            &mut metrics,
            None,
            SystemTime::UNIX_EPOCH + Duration::from_secs(41),
        )?;

        let window = MetricsWindow {
            start: SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            end: SystemTime::UNIX_EPOCH + Duration::from_secs(65),
            num_buckets: 3,
        };
        let buckets =
            metrics.query_gauge(scheduled_job_next_ts_metric(), window.start..window.end)?;
        let result = resample_scheduled_job_lag(&window, &metrics, buckets)?;

        assert_eq!(
            result[2],
            (SystemTime::UNIX_EPOCH + Duration::from_secs(50), Some(0.0))
        );
        Ok(())
    }

    #[test]
    fn retained_state_before_the_window_continues_extrapolating() -> anyhow::Result<()> {
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        let mut metrics = scheduled_job_metrics(base);
        let next_job_time = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        record_scheduled_job_lag_sample(
            &mut metrics,
            Some(next_job_time),
            SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        )?;

        let window = MetricsWindow {
            start: SystemTime::UNIX_EPOCH + Duration::from_secs(3705),
            end: SystemTime::UNIX_EPOCH + Duration::from_secs(3750),
            num_buckets: 3,
        };
        let result = query_scheduled_job_lag(&window, &metrics)?;

        assert_eq!(
            result[0],
            (
                SystemTime::UNIX_EPOCH + Duration::from_secs(3705),
                Some(3685.0)
            )
        );
        Ok(())
    }
}

#[derive(Default)]
pub struct UdfMetricSummary {
    // Aggregated metrics for backwards compatibility.
    pub invocations: u32,
    pub errors: u32,
    pub execution_time: Duration,

    pub function_calls:
        BTreeMap<FunctionCaller, BTreeMap<UdfType, BTreeMap<ModuleEnvironment, FunctionSummary>>>,
}

impl From<UdfMetricSummary> for JsonValue {
    fn from(value: UdfMetricSummary) -> Self {
        json!({
            "invocations": value.invocations,
            "errors": value.errors,
            "executionTime": value.execution_time.as_secs_f64(),

            "functionCalls": value
                .function_calls
                .into_iter()
                .map(|(caller, v)| {
                    let map1 = v.into_iter()
                        .map(|(udf_type, v)| {
                            let map2 = v.into_iter()
                                .map(|(environment, summary)| {
                                    let key = environment.to_string();
                                    let value = JsonValue::from(summary);
                                    (key, value)
                                })
                                .collect::<serde_json::Map<_, _>>();
                            (format!("{udf_type}"), JsonValue::Object(map2))
                        })
                        .collect::<serde_json::Map<_, _>>();
                    (format!("{caller}"), JsonValue::Object(map1))
                })
                .collect::<serde_json::Map<_, _>>(),
        })
    }
}

#[derive(Default)]
pub struct FunctionSummary {
    pub invocations: u32,
    pub errors: u32,
    pub execution_time: Duration,
    pub syscalls: SyscallTrace,
}

impl From<FunctionSummary> for JsonValue {
    fn from(value: FunctionSummary) -> Self {
        json!({
            "invocations": value.invocations,
            "errors": value.errors,
            "executionTime": value.execution_time.as_secs_f64(),
            "syscalls": JsonValue::from(value.syscalls),
        })
    }
}

fn udf_invocations_metric(identifier: &UdfIdentifier) -> MetricName {
    format!("udf:{}:invocations", udf_metric_name(identifier))
}

fn udf_errors_metric(identifier: &UdfIdentifier) -> MetricName {
    format!("udf:{}:errors", udf_metric_name(identifier))
}

fn udf_cache_hits_metric(identifier: &UdfIdentifier) -> MetricName {
    format!("udf:{}:cache_hits", udf_metric_name(identifier))
}

fn udf_cache_misses_metric(identifier: &UdfIdentifier) -> MetricName {
    format!("udf:{}:cache_misses", udf_metric_name(identifier))
}

fn udf_execution_time_metric(identifier: &UdfIdentifier) -> MetricName {
    format!("udf:{}:execution_time", udf_metric_name(identifier))
}

// TODO: Thread component path through here.
fn table_rows_read_metric(table_name: &TableName) -> MetricName {
    format!("table:{table_name}:rows_read")
}

fn table_rows_written_metric(table_name: &TableName) -> MetricName {
    format!("table:{table_name}:rows_written")
}

fn scheduled_job_next_ts_metric() -> &'static str {
    "scheduled_jobs:next_ts"
}

fn outstanding_functions_metric(
    env: &ModuleEnvironment,
    udf_type: &UdfType,
    state: &OutstandingFunctionState,
) -> MetricName {
    let env_str = match env {
        ModuleEnvironment::Isolate => "isolate",
        ModuleEnvironment::Node => "node",
        ModuleEnvironment::Invalid => "invalid",
    };
    let state_str = match state {
        OutstandingFunctionState::Running => "running",
        OutstandingFunctionState::Queued => "queued",
    };
    format!("outstanding_functions:{env_str}:{udf_type}:{state_str}")
}

/// View over `FunctionExecutionLog` that only exposes metrics methods.
/// Obtained by checking `DeploymentOp::ViewMetrics`.
pub struct FunctionMetricsLog<'a, RT: Runtime> {
    log: &'a FunctionExecutionLog<RT>,
}

impl<'a, RT: Runtime> FunctionMetricsLog<'a, RT> {
    pub(crate) fn new(log: &'a FunctionExecutionLog<RT>) -> Self {
        Self { log }
    }

    pub fn udf_rate(
        &self,
        identifier: UdfIdentifier,
        metric: UdfRate,
        window: MetricsWindow,
    ) -> anyhow::Result<Timeseries> {
        self.log.udf_rate(identifier, metric, window)
    }

    pub fn cache_hit_percentage(
        &self,
        identifier: UdfIdentifier,
        window: MetricsWindow,
    ) -> anyhow::Result<Timeseries> {
        self.log.cache_hit_percentage(identifier, window)
    }

    pub fn failure_percentage_top_k(
        &self,
        window: MetricsWindow,
        k: usize,
    ) -> anyhow::Result<Vec<(String, Timeseries)>> {
        self.log.failure_percentage_top_k(window, k)
    }

    pub fn cache_hit_percentage_top_k(
        &self,
        window: MetricsWindow,
        k: usize,
    ) -> anyhow::Result<Vec<(String, Timeseries)>> {
        self.log.cache_hit_percentage_top_k(window, k)
    }

    pub fn get_all_function_calls(
        &self,
        window: &MetricsWindow,
    ) -> anyhow::Result<HashMap<String, Timeseries>> {
        self.log.get_all_function_calls(window)
    }

    pub fn function_call_count_top_k(
        &self,
        window: MetricsWindow,
        k: usize,
    ) -> anyhow::Result<Vec<(String, Timeseries)>> {
        self.log.function_call_count_top_k(window, k)
    }

    pub fn subscription_invalidations_top_k(
        &self,
        window: MetricsWindow,
        k: usize,
        identifier: Option<&UdfIdentifier>,
    ) -> anyhow::Result<Vec<(String, Timeseries)>> {
        self.log
            .subscription_invalidations_top_k(window, k, identifier)
    }

    pub fn latency_percentiles(
        &self,
        identifier: UdfIdentifier,
        percentiles: Vec<Percentile>,
        window: MetricsWindow,
    ) -> anyhow::Result<BTreeMap<Percentile, Timeseries>> {
        self.log
            .latency_percentiles(identifier, percentiles, window)
    }

    pub fn query_shadow_timing_stats(
        &self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        window: MetricsWindow,
    ) -> anyhow::Result<QueryShadowTimingStats> {
        self.log
            .query_shadow_timing_stats(udf_type, route_key, window)
    }

    pub fn query_shadow_outcome_stats(
        &self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        window: MetricsWindow,
    ) -> anyhow::Result<QueryShadowOutcomeStats> {
        self.log
            .query_shadow_outcome_stats(udf_type, route_key, window)
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub fn query_shadow_evidence(
        &self,
        udf_type: UdfType,
        cursor: Option<&QueryShadowRouteKey>,
        limit: usize,
    ) -> anyhow::Result<QueryShadowEvidencePage> {
        self.log.query_shadow_evidence(udf_type, cursor, limit)
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub fn query_shadow_evidence_for_direction(
        &self,
        udf_type: UdfType,
        direction: QueryShadowDirection,
        cursor: Option<&QueryShadowRouteKey>,
        limit: usize,
    ) -> anyhow::Result<QueryShadowEvidencePage> {
        self.log
            .query_shadow_evidence_for_direction(udf_type, direction, cursor, limit)
    }

    pub fn table_rate(
        &self,
        table_name: TableName,
        metric: TableRate,
        window: MetricsWindow,
    ) -> anyhow::Result<Timeseries> {
        self.log.table_rate(table_name, metric, window)
    }

    pub fn scheduled_job_lag(&self, window: MetricsWindow) -> anyhow::Result<Timeseries> {
        self.log.scheduled_job_lag(window)
    }

    pub fn function_concurrency(
        &self,
        window: MetricsWindow,
    ) -> anyhow::Result<BTreeMap<String, Timeseries>> {
        self.log.function_concurrency(window)
    }

    pub fn udf_summary(
        &self,
        cursor: Option<CursorMs>,
    ) -> (Option<UdfMetricSummary>, Option<CursorMs>) {
        self.log.udf_summary(cursor)
    }
}

/// View over `FunctionExecutionLog` that only exposes log entry streaming
/// methods. Obtained by checking `DeploymentOp::ViewLogs`.
pub struct FunctionEntriesLog<'a, RT: Runtime> {
    log: &'a FunctionExecutionLog<RT>,
}

impl<'a, RT: Runtime> FunctionEntriesLog<'a, RT> {
    pub(crate) fn new(log: &'a FunctionExecutionLog<RT>) -> Self {
        Self { log }
    }

    pub async fn stream(&self, cursor: CursorMs) -> (Vec<FunctionExecution>, CursorMs) {
        self.log.stream(cursor).await
    }

    pub async fn stream_parts(&self, cursor: CursorMs) -> (Vec<FunctionExecutionPart>, CursorMs) {
        self.log.stream_parts(cursor).await
    }

    pub fn latest_cursor(&self) -> CursorMs {
        self.log.latest_cursor()
    }
}

fn udf_metric_name(identifier: &UdfIdentifier) -> String {
    let (component, id) = identifier.clone().into_component_and_udf_path();
    match component {
        Some(component) => {
            format!("{component}/{id}")
        },
        None => id,
    }
}
