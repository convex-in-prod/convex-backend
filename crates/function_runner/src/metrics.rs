use fastrace::{
    local::LocalSpan,
    Event,
};
use metrics::{
    log_counter_with_labels,
    log_distribution,
    log_distribution_with_labels,
    log_gauge,
    register_convex_counter,
    register_convex_gauge,
    register_convex_histogram,
    MetricLabel,
    StaticMetricLabel,
    Timer,
};
use prometheus::{
    VMHistogram,
    VMHistogramVec,
};

fn cache_metric_labels<'a>(table_name: &'a str) -> Vec<MetricLabel<'a>> {
    vec![MetricLabel::new("table", table_name)]
}

register_convex_counter!(
    MEMORY_INDEX_CACHE_GET_TOTAL,
    "Number of funrun index cache gets (hits + misses)",
    &["table"]
);
pub fn log_funrun_index_cache_get(table_name: &str) {
    log_counter_with_labels(
        &MEMORY_INDEX_CACHE_GET_TOTAL,
        1,
        cache_metric_labels(table_name),
    );
}

register_convex_histogram!(
    MEMORY_INDEX_CACHE_LOAD_INDEX_SECONDS,
    "Time to load an in-memory index for funrun",
    &["table"]
);
pub fn load_index_timer(table_name: &str) -> Timer<VMHistogramVec> {
    let mut t = Timer::new_with_labels(&MEMORY_INDEX_CACHE_LOAD_INDEX_SECONDS);
    t.add_label(StaticMetricLabel::new("table", table_name.to_owned()));
    t
}

register_convex_histogram!(
    MEMORY_INDEX_CACHE_LOADED_ROWS,
    "Number of rows loaded for an index",
    &["table"]
);
pub fn log_funrun_index_load_rows(rows: u64, table_name: &str) {
    log_distribution_with_labels(
        &MEMORY_INDEX_CACHE_LOADED_ROWS,
        rows as f64,
        cache_metric_labels(table_name),
    );
}

register_convex_histogram!(
    FUNCTION_RUNNER_BEGIN_TX_SECONDS,
    "Time to begin a transaction",
);
pub fn begin_tx_timer() -> Timer<VMHistogram> {
    Timer::new(&FUNCTION_RUNNER_BEGIN_TX_SECONDS)
}

register_convex_histogram!(MODULE_LOAD_SECONDS, "Time to load modules", &["source"]);
pub fn module_load_timer(source: &'static str) -> Timer<VMHistogramVec> {
    let mut timer = Timer::new_with_labels(&MODULE_LOAD_SECONDS);
    timer.add_label(MetricLabel::new_const("source", source));
    timer
}

register_convex_histogram!(
    MODULE_CACHE_SOURCE_SIZE_BYTES_TOTAL,
    "Size in bytes of module source code retrieved from cache",
);

register_convex_histogram!(
    MODULE_CACHE_SOURCE_MAP_SIZE_BYTES_TOTAL,
    "Size in bytes of module source maps retrieved from cache",
);
pub fn record_module_sizes(source_size: usize, source_map_size: Option<usize>) {
    log_distribution(&MODULE_CACHE_SOURCE_SIZE_BYTES_TOTAL, source_size as f64);
    if let Some(map_size) = source_map_size {
        log_distribution(&MODULE_CACHE_SOURCE_MAP_SIZE_BYTES_TOTAL, map_size as f64);
    }
    LocalSpan::add_event(Event::new("module_cache_get_module").with_properties(|| {
        [
            ("module_cache_source_size", source_size.to_string()),
            (
                "module_cache_source_map_size",
                source_map_size
                    .map(|s| s.to_string())
                    .unwrap_or("None".to_string()),
            ),
        ]
    }));
}

register_convex_gauge!(
    CODE_CACHE_SIZE_BYTES_TOTAL,
    "Size in bytes of the function runner V8 code cache",
);
pub fn record_code_cache_size(size: u64) {
    log_gauge(&CODE_CACHE_SIZE_BYTES_TOTAL, size as f64);
}

register_convex_gauge!(
    DATABASE_EXECUTION_IN_FLIGHT_REQUESTS,
    "Database function-runner attempts whose caller is still waiting",
    &["runtime", "role", "active_javascript_class"]
);
register_convex_counter!(
    DATABASE_EXECUTION_ATTEMPTS_TOTAL,
    "One terminal outcome per database function-runner attempt, before transaction commit",
    &[
        "runtime",
        "role",
        "active_javascript_class",
        "outcome",
        "reason"
    ],
    std::time::Duration::MAX,
);
register_convex_histogram!(
    DATABASE_EXECUTION_ATTEMPT_SECONDS,
    "Database function-runner time including source selection, setup and admission",
    &["runtime", "role", "active_javascript_class", "outcome"]
);

#[cfg(feature = "static-hermes-wasmtime-gate")]
register_convex_counter!(
    DATABASE_WASM_PRIMARY_FALLBACKS_TOTAL,
    "Wasm primary failures retried once through ordinary V8 admission",
    &["reason"],
);

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) fn log_wasm_primary_fallback(reason: isolate::StaticHermesWasmFallbackReason) {
    DATABASE_WASM_PRIMARY_FALLBACKS_TOTAL
        .with_label_values(&[reason.label()])
        .inc();
}

pub(crate) enum DatabaseAttemptRole {
    Primary,
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    Verifier,
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    Fallback,
}

pub(crate) fn v8_capacity_rejection_reason(error: &anyhow::Error) -> Option<&'static str> {
    use errors::ErrorMetadataAnyhowExt;
    // V8 preserves retry-before-execution metadata instead of using Overloaded.
    // IsolateNotClean has the same error code but is a real execution failure.
    if !error.is_rejected_before_execution() {
        return None;
    }
    match error.short_msg() {
        "ExecuteFullError" => Some("backlog"),
        "ExpiredInQueue" => Some("queue_expired"),
        "InitialPermitTimeoutError" => Some("cpu"),
        _ => None,
    }
}

/// Owns the caller's attempt, not its worker or memory reservation.
/// Cancellation finishes this observation while detached cleanup retains its
/// own resource gauges. Resource retries and CPU resumptions must not create
/// another attempt.
pub(crate) struct DatabaseExecutionAttempt {
    runtime: &'static str,
    role: &'static str,
    class: &'static str,
    started: std::time::Instant,
    outcome: &'static str,
    reason: &'static str,
}

impl DatabaseExecutionAttempt {
    pub(crate) fn new(
        class: common::types::ActiveJavascriptClass,
        role: DatabaseAttemptRole,
    ) -> Self {
        use common::types::ActiveJavascriptClass;
        let class = match class {
            ActiveJavascriptClass::Protected => "protected",
            ActiveJavascriptClass::Degradable => "degradable",
            ActiveJavascriptClass::HighPriorityMutation => "high_priority_mutation",
            ActiveJavascriptClass::Dependency => "dependency",
            ActiveJavascriptClass::ControlPlane => "control_plane",
        };
        let role = match role {
            DatabaseAttemptRole::Primary => "primary",
            #[cfg(feature = "static-hermes-wasmtime-gate")]
            DatabaseAttemptRole::Verifier => "verifier",
            #[cfg(feature = "static-hermes-wasmtime-gate")]
            DatabaseAttemptRole::Fallback => "fallback",
        };
        let attempt = Self {
            runtime: "selecting",
            role,
            class,
            started: std::time::Instant::now(),
            outcome: "cancelled",
            reason: "none",
        };
        DATABASE_EXECUTION_IN_FLIGHT_REQUESTS
            .with_label_values(&[attempt.runtime, role, class])
            .inc();
        attempt
    }

    pub(crate) fn select_runtime(&mut self, runtime: common::types::UdfRuntime) {
        let runtime = match runtime {
            common::types::UdfRuntime::V8 => "v8",
            common::types::UdfRuntime::Wasm => "wasm",
        };
        if self.runtime != runtime {
            DATABASE_EXECUTION_IN_FLIGHT_REQUESTS
                .with_label_values(&[self.runtime, self.role, self.class])
                .dec();
            self.runtime = runtime;
            DATABASE_EXECUTION_IN_FLIGHT_REQUESTS
                .with_label_values(&[self.runtime, self.role, self.class])
                .inc();
        }
    }

    pub(crate) fn finish(&mut self, result: Result<Option<&udf::FunctionOutcome>, &anyhow::Error>) {
        use errors::ErrorMetadataAnyhowExt;
        self.outcome = match result {
            Ok(Some(
                udf::FunctionOutcome::Query(outcome) | udf::FunctionOutcome::Mutation(outcome),
            )) => {
                if outcome.result.is_err() {
                    "function_error"
                } else {
                    "success"
                }
            },
            // Only query/mutation requests construct this guard. Preserve a
            // distinct diagnostic if the runner violates that result contract.
            Ok(Some(udf::FunctionOutcome::Action(_) | udf::FunctionOutcome::HttpAction(_))) => {
                "unexpected_outcome"
            },
            Ok(None) => "runtime_unavailable",
            Err(error) => {
                if let Some(reason) = v8_capacity_rejection_reason(error) {
                    self.reason = reason;
                    self.outcome = "overloaded";
                    return;
                }
                #[cfg(feature = "static-hermes-wasmtime-gate")]
                if let Some(failure) =
                    error.downcast_ref::<isolate::StaticHermesWasmAdmissionFailure>()
                {
                    use isolate::StaticHermesWasmAdmissionFailure;
                    self.reason = match failure {
                        StaticHermesWasmAdmissionFailure::Backlog => "backlog",
                        StaticHermesWasmAdmissionFailure::QueueDelay => "queue_delay",
                        StaticHermesWasmAdmissionFailure::Deadline => "setup_deadline",
                        StaticHermesWasmAdmissionFailure::Module => "module",
                        StaticHermesWasmAdmissionFailure::Memory => "memory",
                        StaticHermesWasmAdmissionFailure::Cpu => "cpu",
                    };
                    self.outcome = "overloaded";
                    return;
                }
                #[cfg(feature = "static-hermes-wasmtime-gate")]
                if error.is::<isolate::StaticHermesQueryShadowCapacityUnavailable>() {
                    self.reason = "verifier_capacity";
                    self.outcome = "overloaded";
                    return;
                }
                if error.is_degradable_query_capacity() {
                    self.reason = "degradable_capacity";
                    "overloaded"
                } else if error.is_overloaded() {
                    self.reason = "other_capacity";
                    "overloaded"
                } else {
                    "system_error"
                }
            },
        };
    }
}

impl Drop for DatabaseExecutionAttempt {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.outcome = "system_error";
            self.reason = "panic";
        }
        DATABASE_EXECUTION_IN_FLIGHT_REQUESTS
            .with_label_values(&[self.runtime, self.role, self.class])
            .dec();
        DATABASE_EXECUTION_ATTEMPTS_TOTAL
            .with_label_values(&[
                self.runtime,
                self.role,
                self.class,
                self.outcome,
                self.reason,
            ])
            .inc();
        DATABASE_EXECUTION_ATTEMPT_SECONDS
            .with_label_values(&[self.runtime, self.role, self.class, self.outcome])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod database_attempt_tests {
    use super::{
        DatabaseAttemptRole,
        DatabaseExecutionAttempt,
    };

    #[test]
    fn v8_capacity_rejections_are_overload_without_masking_dirty_isolates() {
        for (code, outcome, reason) in [
            ("ExecuteFullError", "overloaded", "backlog"),
            ("ExpiredInQueue", "overloaded", "queue_expired"),
            ("InitialPermitTimeoutError", "overloaded", "cpu"),
            ("IsolateNotClean", "system_error", "none"),
        ] {
            let error = errors::ErrorMetadata::rejected_before_execution(code, "rejected").into();
            let mut attempt = DatabaseExecutionAttempt::new(
                common::types::ActiveJavascriptClass::Protected,
                DatabaseAttemptRole::Primary,
            );
            attempt.select_runtime(common::types::UdfRuntime::V8);
            attempt.finish(Err(&error));
            assert_eq!((attempt.outcome, attempt.reason), (outcome, reason));
        }
    }
}
