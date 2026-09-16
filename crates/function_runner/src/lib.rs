#![feature(iterator_try_collect)]
#![feature(impl_trait_in_assoc_type)]
#![feature(try_blocks)]
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use common::{
    auth::AuthConfig,
    bootstrap_model::components::definition::ComponentDefinitionMetadata,
    components::{
        ComponentDefinitionPath,
        ComponentName,
        Resource,
    },
    document::PendingDocumentUpdate,
    errors::JsError,
    execution_context::ExecutionContext,
    log_lines::LogLine,
    query_analysis_admission::QueryAnalysisAdmission,
    runtime::{
        Runtime,
        UnixTimestamp,
    },
    schemas::DatabaseSchema,
    types::{
        IndexId,
        RepeatableTimestamp,
        SchedulerDependencyClass,
        UdfType,
    },
};
use database::{
    ReadSet,
    Transaction,
    TransactionReadSet,
    TransactionReadSize,
};
use keybroker::Identity;
pub use metrics::record_module_sizes;
use model::{
    config::types::ModuleConfig,
    environment_variables::types::{
        EnvVarName,
        EnvVarValue,
    },
    modules::module_versions::{
        AnalyzedModule,
        ModuleSource,
        SourceMap,
    },
    udf_config::types::UdfConfig,
};
use server::{
    FunctionExecutionStartGate,
    FunctionMetadata,
    HttpActionMetadata,
};
use sync_types::{
    CanonicalizedModulePath,
    Timestamp,
};
use tokio::sync::{
    mpsc,
    OwnedSemaphorePermit,
};
use udf::{
    ActionCallbacks,
    EvaluateAppDefinitionsResult,
    FunctionOutcome,
};
use usage_tracking::FunctionUsageStats;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use value::TableMapping;
use value::{
    identifier::Identifier,
    TabletId,
};

mod in_memory_indexes;
pub mod in_process_function_runner;
mod metrics;
mod module_cache;
pub mod server;

mod query_shadow;
#[cfg(feature = "testing")]
pub use query_shadow::{
    take_query_shadow_test_trace_diagnostics,
    QueryShadowTestTraceDiagnostic,
};
pub use query_shadow::{
    QueryShadowAdmissionEvent,
    QueryShadowComparison,
    QueryShadowDirection,
    QueryShadowErrorDiagnostic,
    QueryShadowErrorKind,
    QueryShadowFailureReason,
    QueryShadowFailureStage,
    QueryShadowGeneratedExportDiagnostic,
    QueryShadowHostOperationCountDifference,
    QueryShadowHostOperationTraceDiagnostic,
    QueryShadowInsertedDocumentIdentity,
    QueryShadowInvalidReason,
    QueryShadowReport,
    QueryShadowReportSink,
    QueryShadowResultDiagnostic,
    QueryShadowResultDifference,
    QueryShadowRouteKey,
    QueryShadowRouteKeyError,
    QueryShadowTerminal,
};

/// Route-aware admission policy for an authenticated shadow route.
///
/// The function runner resolves the authenticated route before asking for a
/// permit so admission state never needs a source path or request data.
pub trait QueryShadowAdmission: Send + Sync + 'static {
    /// Return the validated wall-clock deadline for an admitted shadow lane.
    fn shadow_timeout(&self) -> Duration;

    fn try_admit(
        &self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
    ) -> Option<QueryShadowPermit>;
}

/// Records whether an admitted query shadow's comparison was accepted into
/// route-level evidence or ended without an accepted comparison.
///
/// Implementations use this to retain only routes with accepted comparison
/// evidence. Rejected reports and terminal outcomes leave a route eligible for
/// a later retry.
pub trait QueryShadowObservationTracker: Send + Sync + 'static {
    fn comparison_completed(&self);

    fn terminal(&self);
}

impl<F> QueryShadowObservationTracker for F
where
    F: Fn() + Send + Sync + 'static,
{
    fn comparison_completed(&self) {
        self();
    }

    fn terminal(&self) {
        self();
    }
}

pub struct QueryShadowPermit {
    permit: Option<Arc<OwnedSemaphorePermit>>,
    udf_type: UdfType,
    route_key: QueryShadowRouteKey,
    direction: QueryShadowDirection,
    report_sink: Arc<dyn QueryShadowReportSink>,
    observation_tracker: Arc<dyn QueryShadowObservationTracker>,
}

#[derive(Clone, Copy)]
enum QueryShadowObservationOutcome {
    ComparisonCompleted,
    Terminal,
}

impl QueryShadowPermit {
    /// Memory evidence survives semantic success and detached lane cleanup. It
    /// neither releases admission nor marks a correctness comparison complete.
    pub fn memory_observer(&self) -> udf::wasm_memory::WasmMemoryObserver {
        let sink = Arc::clone(&self.report_sink);
        let udf_type = self.udf_type;
        let route_key = self.route_key.clone();
        let direction = self.direction;
        Arc::new(move |observation| {
            sink.record_query_shadow_report(QueryShadowReport::Memory {
                udf_type,
                route_key: route_key.clone(),
                direction,
                observation,
            });
        })
    }

    pub fn new(
        permit: OwnedSemaphorePermit,
        udf_type: UdfType,
        route_key: QueryShadowRouteKey,
        report_sink: Arc<dyn QueryShadowReportSink>,
        observation_tracker: Arc<dyn QueryShadowObservationTracker>,
    ) -> Self {
        Self::new_with_direction(
            permit,
            udf_type,
            route_key,
            QueryShadowDirection::V8PrimaryWasmShadow,
            report_sink,
            observation_tracker,
        )
    }

    pub fn new_with_direction(
        permit: OwnedSemaphorePermit,
        udf_type: UdfType,
        route_key: QueryShadowRouteKey,
        direction: QueryShadowDirection,
        report_sink: Arc<dyn QueryShadowReportSink>,
        observation_tracker: Arc<dyn QueryShadowObservationTracker>,
    ) -> Self {
        Self {
            permit: Some(Arc::new(permit)),
            udf_type,
            route_key,
            direction,
            report_sink,
            observation_tracker,
        }
    }

    /// Retain the admission reservation while detached shadow work cleans up.
    pub fn work_guard(&self) -> Arc<OwnedSemaphorePermit> {
        Arc::clone(
            self.permit
                .as_ref()
                .expect("query shadow permit cannot create a work guard after finalization starts"),
        )
    }

    /// Record the terminal comparison after the authoritative primary finishes.
    pub fn record_comparison(
        &mut self,
        primary_duration: Duration,
        wasm_duration: Duration,
        comparison: QueryShadowComparison,
    ) {
        self.record_comparison_with_verifier(primary_duration, wasm_duration, None, comparison);
    }

    /// Record a comparison while retaining the verifier duration and runtime
    /// direction for Wasm-primary verification.
    pub fn record_comparison_with_verifier(
        &mut self,
        primary_duration: Duration,
        wasm_duration: Duration,
        verifier_duration: Option<Duration>,
        comparison: QueryShadowComparison,
    ) {
        if self.permit.is_none() {
            return;
        }
        self.begin_finalization();
        let report = QueryShadowReport::new_for_type_and_direction(
            self.udf_type,
            self.route_key.clone(),
            self.direction,
            primary_duration,
            wasm_duration,
            verifier_duration,
            comparison,
        );
        let outcome = if self.report_sink.record_query_shadow_report(report) {
            QueryShadowObservationOutcome::ComparisonCompleted
        } else {
            QueryShadowObservationOutcome::Terminal
        };
        self.finish_finalization(outcome);
    }

    /// Record a terminal shadow outcome before releasing the route observation
    /// reservation.
    pub fn record_terminal(&mut self, terminal: QueryShadowTerminal) {
        if self.permit.is_none() {
            return;
        }
        self.begin_finalization();
        let _ = self
            .report_sink
            .record_query_shadow_report(QueryShadowReport::Terminal {
                udf_type: self.udf_type,
                route_key: self.route_key.clone(),
                direction: self.direction,
                terminal,
            });
        self.finish_finalization(QueryShadowObservationOutcome::Terminal);
    }

    fn begin_finalization(&mut self) {
        // Release this owner's capacity before publishing outcome evidence.
        // A waiter can react to that evidence immediately, so publishing first
        // could make its non-queued admission fail against work that is already
        // complete. A detached work guard, when present, continues to hold the
        // same permit through cleanup.
        drop(
            self.permit
                .take()
                .expect("query shadow admission permit finalized twice"),
        );
    }

    fn finish_finalization(&self, outcome: QueryShadowObservationOutcome) {
        match outcome {
            QueryShadowObservationOutcome::ComparisonCompleted => {
                self.observation_tracker.comparison_completed();
            },
            QueryShadowObservationOutcome::Terminal => self.observation_tracker.terminal(),
        }
    }
}

impl Drop for QueryShadowPermit {
    fn drop(&mut self) {
        if self.permit.is_some() {
            // Detached work can end outside an explicit terminal path. Emit a
            // fixed route-keyed terminal before releasing its route observation
            // reservation.
            self.record_terminal(QueryShadowTerminal::UnexpectedTaskExit);
        }
    }
}

#[cfg(test)]
mod query_shadow_report_tests {
    use std::{
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            Arc,
            Barrier,
        },
        thread,
        time::Duration,
    };

    use common::types::UdfType;
    use parking_lot::Mutex;
    use tokio::sync::Semaphore;
    use udf::wasm_memory::{
        WasmMemoryObservation,
        WasmMemoryTerminal,
    };

    use super::{
        QueryShadowComparison,
        QueryShadowDirection,
        QueryShadowFailureReason,
        QueryShadowFailureStage,
        QueryShadowInsertedDocumentIdentity,
        QueryShadowObservationTracker,
        QueryShadowPermit,
        QueryShadowReport,
        QueryShadowRouteKey,
        QueryShadowTerminal,
    };

    struct ObservationTracker {
        completed: AtomicUsize,
        terminal: AtomicUsize,
    }

    impl QueryShadowObservationTracker for ObservationTracker {
        fn comparison_completed(&self) {
            self.completed.fetch_add(1, Ordering::Relaxed);
        }

        fn terminal(&self) {
            self.terminal.fetch_add(1, Ordering::Relaxed);
        }
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
    fn permit_forwards_only_the_safe_report() {
        let reports = Arc::new(Mutex::new(Vec::new()));
        let reports_for_sink = Arc::clone(&reports);
        let sink = Arc::new(move |report: QueryShadowReport| {
            reports_for_sink.lock().push(report);
            true
        });
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.try_acquire_owned().unwrap();
        let route_key = QueryShadowRouteKey::new(&"1".repeat(64), &"a".repeat(64)).unwrap();
        let mut permit = QueryShadowPermit::new(
            permit,
            UdfType::Query,
            route_key.clone(),
            sink,
            Arc::new(|| {}),
        );
        let comparison = matching_comparison();

        permit.record_comparison(
            Duration::from_millis(10),
            Duration::from_millis(20),
            comparison.clone(),
        );

        assert_eq!(
            *reports.lock(),
            vec![QueryShadowReport::new(
                route_key,
                Duration::from_millis(10),
                Duration::from_millis(20),
                comparison,
            )]
        );
    }

    #[test]
    fn memory_report_does_not_finalize_shadow_observation() {
        let reports = Arc::new(Mutex::new(Vec::new()));
        let reports_for_sink = Arc::clone(&reports);
        let semaphore = Arc::new(Semaphore::new(1));
        let route_key = QueryShadowRouteKey::new(&"1".repeat(64), &"b".repeat(64)).unwrap();
        let tracker = Arc::new(ObservationTracker {
            completed: AtomicUsize::new(0),
            terminal: AtomicUsize::new(0),
        });
        let mut permit = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            route_key.clone(),
            Arc::new(move |report| {
                reports_for_sink.lock().push(report);
                true
            }),
            tracker.clone(),
        );
        let observation = WasmMemoryObservation {
            reused_instance: false,
            checkout_baseline_bytes: 0,
            peak_admitted_bytes: 10,
            peak_guest_bytes: 10,
            peak_host_bytes: 0,
            requested_peak_bytes: 10,
            forecast_growth_bytes: 10,
            completion_bytes: 10,
            returned_to_pool_bytes: 10,
            returned_to_pool: true,
            forecast_overrun: false,
            growth_denied: false,
            successful_execution_discarded: false,
            terminal: WasmMemoryTerminal::Success,
        };

        permit.memory_observer()(observation.clone());

        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(tracker.completed.load(Ordering::Relaxed), 0);
        assert_eq!(tracker.terminal.load(Ordering::Relaxed), 0);
        assert_eq!(
            *reports.lock(),
            vec![QueryShadowReport::Memory {
                udf_type: UdfType::Query,
                route_key,
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                observation,
            }]
        );

        permit.record_terminal(QueryShadowTerminal::Timeout);
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(tracker.terminal.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn completed_comparison_releases_shadow_budget_before_permit_drop() {
        let semaphore = Arc::new(Semaphore::new(1));
        let mut first = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            QueryShadowRouteKey::new(&"1".repeat(64), &"a".repeat(64)).unwrap(),
            Arc::new(|_| true),
            Arc::new(|| {}),
        );

        first.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());

        let second = semaphore
            .clone()
            .try_acquire_owned()
            .expect("completed comparison retained shadow admission capacity");
        drop(second);
        assert_eq!(semaphore.available_permits(), 1);
        drop(first);
    }

    #[test]
    fn completed_comparison_releases_owner_before_publishing_evidence() {
        let semaphore = Arc::new(Semaphore::new(1));
        let semaphore_for_sink = Arc::clone(&semaphore);
        let mut permit = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            QueryShadowRouteKey::new(&"1".repeat(64), &"a".repeat(64)).unwrap(),
            Arc::new(move |report| {
                if matches!(report, QueryShadowReport::Compared { .. }) {
                    let immediate_repeat = semaphore_for_sink
                        .clone()
                        .try_acquire_owned()
                        .expect("published comparison still retained its owner permit");
                    drop(immediate_repeat);
                }
                true
            }),
            Arc::new(|| {}),
        );

        permit.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());

        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn detached_work_guard_holds_capacity_during_comparison_publication() {
        let semaphore = Arc::new(Semaphore::new(1));
        let semaphore_for_sink = Arc::clone(&semaphore);
        let mut permit = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            QueryShadowRouteKey::new(&"1".repeat(64), &"b".repeat(64)).unwrap(),
            Arc::new(move |report| {
                if matches!(report, QueryShadowReport::Compared { .. }) {
                    assert!(semaphore_for_sink.clone().try_acquire_owned().is_err());
                }
                true
            }),
            Arc::new(|| {}),
        );
        let work_guard = permit.work_guard();

        permit.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());

        assert_eq!(semaphore.available_permits(), 0);
        drop(work_guard);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn shadow_budget_is_released_before_observation_tracker_runs() {
        let semaphore = Arc::new(Semaphore::new(1));
        let tracker_entered = Arc::new(Barrier::new(2));
        let release_tracker = Arc::new(Barrier::new(2));
        let tracker_entered_for_thread = Arc::clone(&tracker_entered);
        let release_tracker_for_thread = Arc::clone(&release_tracker);
        let mut permit = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            QueryShadowRouteKey::new(&"1".repeat(64), &"f".repeat(64)).unwrap(),
            Arc::new(|_| true),
            Arc::new(move || {
                tracker_entered_for_thread.wait();
                release_tracker_for_thread.wait();
            }),
        );
        let finalizer = thread::spawn(move || {
            permit.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());
        });

        tracker_entered.wait();
        let next = semaphore.clone().try_acquire_owned();
        release_tracker.wait();
        finalizer.join().unwrap();

        let next = next.expect("observation finalized before shadow capacity was released");
        drop(next);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn terminal_and_comparison_update_route_observation_state_differently() {
        let semaphore = Arc::new(Semaphore::new(2));
        let tracker = Arc::new(ObservationTracker {
            completed: AtomicUsize::new(0),
            terminal: AtomicUsize::new(0),
        });
        let route_key = QueryShadowRouteKey::new(&"1".repeat(64), &"b".repeat(64)).unwrap();
        let mut terminal_permit = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            route_key.clone(),
            Arc::new(|_| true),
            tracker.clone(),
        );
        terminal_permit.record_terminal(QueryShadowTerminal::Timeout);

        let mut comparison_permit = QueryShadowPermit::new(
            semaphore.try_acquire_owned().unwrap(),
            UdfType::Query,
            route_key,
            Arc::new(|_| true),
            tracker.clone(),
        );
        comparison_permit.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());

        assert_eq!(tracker.terminal.load(Ordering::Relaxed), 1);
        assert_eq!(tracker.completed.load(Ordering::Relaxed), 1);
        assert_eq!(QueryShadowTerminal::Timeout.metric_key(), "timeout");
    }

    #[test]
    fn terminal_report_precedes_observation_release() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_for_sink = Arc::clone(&events);
        let events_for_tracker = Arc::clone(&events);
        let semaphore = Arc::new(Semaphore::new(1));
        let semaphore_for_sink = Arc::clone(&semaphore);
        let mut permit = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            QueryShadowRouteKey::new(&"1".repeat(64), &"c".repeat(64)).unwrap(),
            Arc::new(move |_| {
                let immediate_repeat = semaphore_for_sink
                    .clone()
                    .try_acquire_owned()
                    .expect("terminal report retained its owner permit");
                drop(immediate_repeat);
                events_for_sink.lock().push("report");
                true
            }),
            Arc::new(move || events_for_tracker.lock().push("release")),
        );

        permit.record_terminal(QueryShadowTerminal::Timeout);

        assert_eq!(*events.lock(), vec!["report", "release"]);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn rejected_comparison_releases_the_first_observation_reservation() {
        let semaphore = Arc::new(Semaphore::new(1));
        let tracker = Arc::new(ObservationTracker {
            completed: AtomicUsize::new(0),
            terminal: AtomicUsize::new(0),
        });
        let route_key = QueryShadowRouteKey::new(&"1".repeat(64), &"c".repeat(64)).unwrap();
        let semaphore_for_sink = Arc::clone(&semaphore);
        let mut permit = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            route_key,
            Arc::new(move |_| {
                let immediate_repeat = semaphore_for_sink
                    .clone()
                    .try_acquire_owned()
                    .expect("rejected comparison retained its owner permit");
                drop(immediate_repeat);
                false
            }),
            tracker.clone(),
        );

        permit.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());

        assert_eq!(tracker.completed.load(Ordering::Relaxed), 0);
        assert_eq!(tracker.terminal.load(Ordering::Relaxed), 1);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn dropping_unfinalized_permit_reports_one_fixed_terminal() {
        let semaphore = Arc::new(Semaphore::new(1));
        let tracker = Arc::new(ObservationTracker {
            completed: AtomicUsize::new(0),
            terminal: AtomicUsize::new(0),
        });
        let reports = Arc::new(Mutex::new(Vec::new()));
        let reports_for_sink = Arc::clone(&reports);
        let route_key = QueryShadowRouteKey::new(&"1".repeat(64), &"d".repeat(64)).unwrap();
        let semaphore_for_sink = Arc::clone(&semaphore);
        let permit = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            route_key.clone(),
            Arc::new(move |report| {
                let immediate_repeat = semaphore_for_sink
                    .clone()
                    .try_acquire_owned()
                    .expect("unexpected-exit report retained its owner permit");
                drop(immediate_repeat);
                reports_for_sink.lock().push(report);
                true
            }),
            tracker.clone(),
        );

        drop(permit);

        assert_eq!(tracker.completed.load(Ordering::Relaxed), 0);
        assert_eq!(tracker.terminal.load(Ordering::Relaxed), 1);
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(
            *reports.lock(),
            vec![QueryShadowReport::Terminal {
                udf_type: UdfType::Query,
                route_key,
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                terminal: QueryShadowTerminal::UnexpectedTaskExit,
            }]
        );
    }

    #[test]
    fn stage_bearing_terminal_is_not_repeated_when_the_permit_is_dropped() {
        let semaphore = Arc::new(Semaphore::new(1));
        let reports = Arc::new(Mutex::new(Vec::new()));
        let reports_for_sink = Arc::clone(&reports);
        let route_key = QueryShadowRouteKey::new(&"1".repeat(64), &"f".repeat(64)).unwrap();
        let mut permit = QueryShadowPermit::new(
            semaphore.try_acquire_owned().unwrap(),
            UdfType::Query,
            route_key.clone(),
            Arc::new(move |report| {
                reports_for_sink.lock().push(report);
                true
            }),
            Arc::new(|| {}),
        );

        permit.record_terminal(QueryShadowTerminal::ShadowFailure {
            stage: QueryShadowFailureStage::RouteAuthentication,
            reason: QueryShadowFailureReason::RouteAuthentication,
            generated_export_diagnostic: None,
            wasmtime_trap_diagnostic: None,
        });
        drop(permit);

        assert_eq!(
            *reports.lock(),
            vec![QueryShadowReport::Terminal {
                udf_type: UdfType::Query,
                route_key,
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                terminal: QueryShadowTerminal::ShadowFailure {
                    stage: QueryShadowFailureStage::RouteAuthentication,
                    reason: QueryShadowFailureReason::RouteAuthentication,
                    generated_export_diagnostic: None,
                    wasmtime_trap_diagnostic: None,
                },
            }]
        );
    }

    #[test]
    fn work_guard_holds_shadow_budget_after_caller_cancellation() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let permit = QueryShadowPermit::new(
            permit,
            UdfType::Query,
            QueryShadowRouteKey::new(&"1".repeat(64), &"e".repeat(64)).unwrap(),
            Arc::new(|_| true),
            Arc::new(|| {}),
        );
        let work_guard = permit.work_guard();

        drop(permit);
        assert_eq!(semaphore.available_permits(), 0);
        drop(work_guard);
        assert_eq!(semaphore.available_permits(), 1);
    }
}

pub enum FunctionExecutionMode {
    /// Preserve the deployment's configured runtime selection.
    Configured,
    /// Execute in V8 even when a Wasm route exists.
    V8,
    /// Execute in V8 and retain the data-free host-operation trace for a
    /// local comparison consumer.
    V8WithHostOperationTrace,
    /// Execute only through an authenticated Static Hermes Wasm route.
    StaticHermesWasm,
    /// Execute through an authenticated Static Hermes Wasm route and run a
    /// sampled V8 verifier in bounded background work. Wasm remains
    /// authoritative and the verifier cannot affect the caller's result.
    StaticHermesWasmWithV8Shadow(Arc<dyn QueryShadowAdmission>),
    /// Execute through an authenticated Static Hermes Wasm route and retain
    /// the data-free host-operation trace for a local comparison consumer.
    StaticHermesWasmWithHostOperationTrace,
    /// Execute V8 authoritatively and ask the authenticated route-aware policy
    /// whether a Wasm shadow should run.
    V8WithStaticHermesWasmShadow(Arc<dyn QueryShadowAdmission>),
    /// Test-only mutation pairing with caller-supplied fixed inputs. V8 remains
    /// authoritative; the matching authenticated Wasm invocation is discarded
    /// after exact local comparison. Production uses route-policy sampling.
    #[cfg(feature = "testing")]
    V8WithStaticHermesWasmMutationShadowForTest { inputs: DatabaseUdfExecutionInputs },
}

impl FunctionExecutionMode {
    pub fn trace_host_operations(&self) -> bool {
        match self {
            Self::V8WithHostOperationTrace
            | Self::StaticHermesWasmWithHostOperationTrace
            | Self::StaticHermesWasmWithV8Shadow(_) => true,
            #[cfg(feature = "testing")]
            Self::V8WithStaticHermesWasmMutationShadowForTest { .. } => true,
            _ => false,
        }
    }
}

#[derive(Clone, Copy)]
pub struct DatabaseUdfExecutionInputs {
    pub rng_seed: [u8; 32],
    pub unix_timestamp: UnixTimestamp,
}

pub enum FunctionExecutionResult {
    Completed {
        transaction: Option<FunctionFinalTransaction>,
        outcome: FunctionOutcome,
        usage_stats: FunctionUsageStats,
    },
    StaticHermesWasmUnavailable,
}

#[async_trait]
pub trait FunctionRunner<RT: Runtime>: Send + Sync + 'static {
    async fn run_function(
        &self,
        execution_mode: FunctionExecutionMode,
        udf_type: UdfType,
        identity: Identity,
        ts: RepeatableTimestamp,
        existing_writes: FunctionWrites,
        log_line_sender: Option<mpsc::UnboundedSender<LogLine>>,
        function_metadata: Option<FunctionMetadata>,
        http_action_metadata: Option<HttpActionMetadata>,
        default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
        in_memory_index_last_modified: BTreeMap<IndexId, Timestamp>,
        context: ExecutionContext,
        function_execution_start: Option<FunctionExecutionStartGate>,
        scheduler_dependency: SchedulerDependencyClass,
    ) -> anyhow::Result<FunctionExecutionResult>;

    async fn analyze(
        &self,
        udf_config: UdfConfig,
        modules: BTreeMap<CanonicalizedModulePath, ModuleConfig>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        query_analysis_admission: Option<QueryAnalysisAdmission>,
    ) -> anyhow::Result<Result<BTreeMap<CanonicalizedModulePath, AnalyzedModule>, JsError>>;

    async fn evaluate_app_definitions(
        &self,
        app_definition: ModuleConfig,
        component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    ) -> anyhow::Result<EvaluateAppDefinitionsResult>;

    async fn evaluate_component_initializer(
        &self,
        evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        path: ComponentDefinitionPath,
        definition: ModuleConfig,
        args: BTreeMap<Identifier, Resource>,
        name: ComponentName,
    ) -> anyhow::Result<BTreeMap<Identifier, Resource>>;

    async fn evaluate_schema(
        &self,
        schema_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        rng_seed: [u8; 32],
        unix_timestamp: UnixTimestamp,
    ) -> anyhow::Result<DatabaseSchema>;

    async fn evaluate_auth_config(
        &self,
        auth_config_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        explanation: &str,
    ) -> anyhow::Result<AuthConfig>;

    /// Set the action callbacks. Only used for InProcessFunctionRunner to break
    /// a reference cycle between ApplicationFunctionRunner and dyn
    /// FunctionRunner.
    fn set_action_callbacks(&self, action_callbacks: Arc<dyn ActionCallbacks>);
}

/// Reads and writes from a UDF that executed in Funrun
pub struct FunctionFinalTransaction {
    pub begin_timestamp: Timestamp,
    pub reads: FunctionReads,
    pub handler_reads: Option<FunctionReads>,
    pub writes: FunctionWrites,
    pub rows_read_by_tablet: BTreeMap<TabletId, u64>,
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub table_mapping: TableMapping,
}

impl<RT: Runtime> TryFrom<Transaction<RT>> for FunctionFinalTransaction {
    type Error = anyhow::Error;

    fn try_from(mut tx: Transaction<RT>) -> anyhow::Result<Self> {
        tx.require_not_nested()?;
        let begin_timestamp = *tx.begin_timestamp();
        let rows_read_by_tablet = tx
            .stats_by_tablet()
            .iter()
            .map(|(table, stats)| (*table, stats.rows_read))
            .collect();
        let handler_reads = tx.take_handler_read_set().map(Into::into);
        #[cfg(feature = "static-hermes-wasmtime-gate")]
        let (reads, writes, table_mapping) = tx.into_reads_writes_and_table_mapping();
        #[cfg(not(feature = "static-hermes-wasmtime-gate"))]
        let (reads, writes) = tx.into_reads_and_writes();
        Ok(Self {
            begin_timestamp,
            reads: reads.into(),
            handler_reads,
            writes: FunctionWrites {
                updates: writes
                    .into_flat()?
                    .into_coalesced_writes()
                    .map(Arc::unwrap_or_clone)
                    .collect(),
            },
            rows_read_by_tablet,
            #[cfg(feature = "static-hermes-wasmtime-gate")]
            table_mapping,
        })
    }
}

pub struct FunctionReads {
    pub reads: ReadSet,
    pub num_intervals: usize,
    pub user_tx_size: TransactionReadSize,
    pub system_tx_size: TransactionReadSize,
}

impl From<TransactionReadSet> for FunctionReads {
    fn from(read_set: TransactionReadSet) -> Self {
        let num_intervals = read_set.num_intervals();
        let user_tx_size = read_set.user_tx_size().clone();
        let system_tx_size = read_set.system_tx_size().clone();
        let reads = read_set.into_read_set();
        Self {
            reads,
            num_intervals,
            user_tx_size,
            system_tx_size,
        }
    }
}

/// Subset of [`Writes`] that is returned by [FunctionRunner] after a function
/// has executed.
#[derive(Clone, Default)]
pub struct FunctionWrites {
    /// N.B.: these are expected to have unique `id`s
    pub updates: Vec<PendingDocumentUpdate>,
}
