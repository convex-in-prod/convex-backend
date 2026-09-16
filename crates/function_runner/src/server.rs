#[cfg(feature = "static-hermes-wasmtime-gate")]
use std::sync::Mutex;
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fmt::Debug,
    sync::Arc,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use std::{
    future::Future,
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
pub use common::execution_start::{
    function_execution_start_barrier,
    FunctionExecutionStartController,
    FunctionExecutionStartGate,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use common::runtime::WithTimeout;
use common::{
    auth::AuthConfig,
    bootstrap_model::components::definition::ComponentDefinitionMetadata,
    components::{
        ComponentDefinitionPath,
        ComponentName,
        Resource,
    },
    document::udf_unix_timestamp,
    errors::JsError,
    execution_context::ExecutionContext,
    http::{
        fetch::FetchClient,
        RoutedHttpPath,
    },
    knobs::MAX_ISOLATE_WORKERS,
    log_lines::LogLine,
    persistence::RetentionValidator,
    query_analysis_admission::QueryAnalysisAdmission,
    query_journal::QueryJournal,
    runtime::{
        Runtime,
        UnixTimestamp,
    },
    schemas::DatabaseSchema,
    types::{
        ActiveJavascriptClass,
        ConvexOrigin,
        DeploymentMetadata,
        IndexId,
        ModuleEnvironment,
        SchedulerDependencyClass,
        UdfType,
    },
};
use database::{
    BootstrapMetadata,
    TableCountSnapshot,
    Transaction,
    TransactionTextSnapshot,
};
use file_storage::TransactionalFileStorage;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use futures::future::Either;
use futures::FutureExt;
use indexing::index_reader::IndexReader;
#[cfg(all(test, feature = "static-hermes-wasmtime-gate"))]
use isolate::StaticHermesWasmTrapCode;
use isolate::{
    client::{
        EnvironmentData,
        IsolateWorker,
    },
    IsolateClient,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use isolate::{
    StaticHermesGeneratedExportDiagnostic,
    StaticHermesQueryShadowCapacityUnavailable as ShadowCapacityError,
    StaticHermesWasmExecutionFailure,
    StaticHermesWasmModuleLoadingFailure,
    StaticHermesWasmTrapDiagnostic,
    StaticHermesWasmtimePreparationMode,
};
use keybroker::{
    FunctionRunnerKeyBroker,
    Identity,
};
use model::{
    components::auth::propagate_component_auth,
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
use rand::Rng;
use storage::{
    Storage,
    StorageUseCase,
};
use sync_types::{
    CanonicalizedModulePath,
    Timestamp,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::{
    mpsc,
    oneshot,
};
use udf::{
    validation::{
        ValidatedHttpPath,
        ValidatedPathAndArgs,
    },
    ActionCallbacks,
    EvaluateAppDefinitionsResult,
    FunctionOutcome,
    HttpActionRequest as HttpActionRequestInner,
    HttpActionResponseStreamer,
};
use usage_tracking::{
    FunctionUsageStats,
    FunctionUsageTracker,
};
use value::identifier::Identifier;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use value::ResolvedDocumentId;

use super::in_memory_indexes::InMemoryIndexCache;
#[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
use crate::query_shadow::{
    compare_mutation_for_test as compare_mutation_shadow_for_test,
    MutationShadowTestObservation,
};
use crate::{
    module_cache::{
        CodeCache,
        FunctionRunnerModuleLoader,
        ModuleCache,
    },
    FunctionExecutionMode,
    FunctionExecutionResult,
    FunctionFinalTransaction,
    FunctionWrites,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use crate::{
    query_shadow::{
        compare_mutation as compare_mutation_shadow,
        compare_query as compare_query_shadow,
        log_terminal as log_query_shadow_terminal,
        MutationShadowObservation,
        QueryShadowFailureReason,
        QueryShadowFailureStage,
        QueryShadowGeneratedExportDiagnostic,
        QueryShadowInvalidReason,
        QueryShadowObservation,
    },
    DatabaseUdfExecutionInputs,
    QueryShadowPermit,
    QueryShadowRouteKey,
    QueryShadowTerminal,
};

/// Validate execution modes that depend on optional backend capabilities.
///
/// Keep this check at the function-runner boundary: callers may construct a
/// production shadow mode from runtime configuration even when this binary was
/// built without the Static Hermes Wasmtime gate. In that configuration,
/// continuing into the ordinary V8 path would silently disable shadowing.
fn validate_execution_mode_for_build(execution_mode: &FunctionExecutionMode) -> anyhow::Result<()> {
    #[cfg(not(feature = "static-hermes-wasmtime-gate"))]
    if matches!(
        execution_mode,
        FunctionExecutionMode::V8WithStaticHermesWasmShadow(_)
            | FunctionExecutionMode::StaticHermesWasmWithV8Shadow(_)
    ) {
        anyhow::bail!("Static Hermes production shadow requires static-hermes-wasmtime-gate");
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    let _ = execution_mode;

    Ok(())
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
struct DatabaseLaneRequest {
    key_broker: FunctionRunnerKeyBroker,
    index_reader: Arc<dyn IndexReader>,
    convex_origin: ConvexOrigin,
    bootstrap_metadata: BootstrapMetadata,
    table_count_snapshot: Arc<dyn TableCountSnapshot>,
    text_index_snapshot: Arc<dyn TransactionTextSnapshot>,
    identity: Identity,
    existing_writes: FunctionWrites,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    in_memory_index_last_modified: BTreeMap<IndexId, Timestamp>,
    context: ExecutionContext,
    scheduler_dependency: SchedulerDependencyClass,
    subfunctions_in_same_isolate: bool,
    deployment: DeploymentMetadata,
    udf_type: UdfType,
    function_metadata: FunctionMetadata,
    function_started_sender: Option<oneshot::Sender<()>>,
    trace_host_operations: bool,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
struct DatabaseLaneTransaction<RT: Runtime> {
    transaction: Transaction<RT>,
    usage_tracker: FunctionUsageTracker,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
enum DatabaseLaneMode {
    Primary,
    ObservedPrimary(udf::wasm_memory::WasmMemoryObserver),
    WasmShadow(
        Option<Arc<OwnedSemaphorePermit>>,
        Option<udf::wasm_memory::WasmMemoryObserver>,
    ),
    V8Verifier(Arc<OwnedSemaphorePermit>),
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Debug, thiserror::Error)]
#[error("Static Hermes shadow lane failed during {stage:?}")]
struct ShadowLaneFailureStage {
    stage: QueryShadowFailureStage,
    reason: QueryShadowFailureReason,
    generated_export_diagnostic: Option<QueryShadowGeneratedExportDiagnostic>,
    wasmtime_trap_diagnostic: Option<StaticHermesWasmTrapDiagnostic>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn shadow_failure_reason(
    error: &anyhow::Error,
    stage: QueryShadowFailureStage,
) -> QueryShadowFailureReason {
    if error.is::<StaticHermesWasmModuleLoadingFailure>() {
        return QueryShadowFailureReason::ModuleLoading;
    }
    if let Some(failure) = error.downcast_ref::<StaticHermesWasmExecutionFailure>() {
        return match failure {
            StaticHermesWasmExecutionFailure::RuntimeInitialization => {
                QueryShadowFailureReason::RuntimeInitialization
            },
            StaticHermesWasmExecutionFailure::InitializationTimeout => {
                QueryShadowFailureReason::InitializationTimeout
            },
            StaticHermesWasmExecutionFailure::ExecutionTimeout => {
                QueryShadowFailureReason::ExecutionTimeout
            },
            StaticHermesWasmExecutionFailure::InstructionBudget => {
                QueryShadowFailureReason::InstructionBudget
            },
            StaticHermesWasmExecutionFailure::GeneratedExportDispatch => {
                QueryShadowFailureReason::GeneratedExportDispatch
            },
            StaticHermesWasmExecutionFailure::GuestMemoryLimit => {
                QueryShadowFailureReason::GuestMemoryLimit
            },
            StaticHermesWasmExecutionFailure::HostOwnedBytesLimit => {
                QueryShadowFailureReason::HostOwnedBytesLimit
            },
            StaticHermesWasmExecutionFailure::OpaqueHandleLimit => {
                QueryShadowFailureReason::OpaqueHandleLimit
            },
            StaticHermesWasmExecutionFailure::OpaqueHandleSpaceExhausted => {
                QueryShadowFailureReason::OpaqueHandleSpaceExhausted
            },
            StaticHermesWasmExecutionFailure::AggregateMemoryLimit => {
                QueryShadowFailureReason::AggregateMemoryLimit
            },
            StaticHermesWasmExecutionFailure::OperationLimit => {
                QueryShadowFailureReason::OperationLimit
            },
            StaticHermesWasmExecutionFailure::HostAbiInvariant => {
                QueryShadowFailureReason::HostAbiInvariant
            },
            StaticHermesWasmExecutionFailure::WasmtimeTrap { diagnostic } => {
                if diagnostic.stderr_classification()
                    == isolate::StaticHermesWasmTrapStderrClassification::HermesHeapOutOfMemory
                {
                    QueryShadowFailureReason::HermesHeapOutOfMemory
                } else {
                    QueryShadowFailureReason::WasmtimeTrap
                }
            },
            StaticHermesWasmExecutionFailure::GuestExecution => {
                QueryShadowFailureReason::GuestExecution
            },
            StaticHermesWasmExecutionFailure::ResultFinalization => {
                QueryShadowFailureReason::ResultFinalization
            },
            StaticHermesWasmExecutionFailure::RuntimeCleanup => {
                QueryShadowFailureReason::RuntimeCleanup
            },
        };
    }
    match stage {
        QueryShadowFailureStage::TransactionSetup | QueryShadowFailureStage::EnvironmentSetup => {
            QueryShadowFailureReason::Infrastructure
        },
        QueryShadowFailureStage::RouteAuthentication => {
            QueryShadowFailureReason::RouteAuthentication
        },
        QueryShadowFailureStage::ModuleLoading => QueryShadowFailureReason::ModuleLoading,
        QueryShadowFailureStage::VerifierExecution => QueryShadowFailureReason::VerifierExecution,
        QueryShadowFailureStage::WasmExecution | QueryShadowFailureStage::Unclassified => {
            QueryShadowFailureReason::Unclassified
        },
        QueryShadowFailureStage::TransactionFinalization => {
            QueryShadowFailureReason::TransactionFinalization
        },
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn classify_shadow_lane_error(
    error: anyhow::Error,
    is_shadow_lane: bool,
    stage: QueryShadowFailureStage,
) -> anyhow::Error {
    // Module construction can be deferred from preparation into execution.
    // Preserve its marker instead of reporting the enclosing execution stage.
    let stage = if error.is::<StaticHermesWasmModuleLoadingFailure>() {
        QueryShadowFailureStage::ModuleLoading
    } else {
        stage
    };
    let reason = shadow_failure_reason(&error, stage);
    let generated_export_diagnostic = error
        .downcast_ref::<StaticHermesGeneratedExportDiagnostic>()
        .map(|diagnostic| match diagnostic {
            StaticHermesGeneratedExportDiagnostic::FirstSelectorTrap => {
                QueryShadowGeneratedExportDiagnostic::FirstSelectorTrap
            },
            StaticHermesGeneratedExportDiagnostic::FirstSelectorRejected { status } => {
                QueryShadowGeneratedExportDiagnostic::FirstSelectorRejected { status: *status }
            },
            StaticHermesGeneratedExportDiagnostic::SelectedEntryPreparationTrap => {
                QueryShadowGeneratedExportDiagnostic::SelectedEntryPreparationTrap
            },
            StaticHermesGeneratedExportDiagnostic::SelectedEntryPreparationRejected { status } => {
                QueryShadowGeneratedExportDiagnostic::SelectedEntryPreparationRejected {
                    status: *status,
                }
            },
            StaticHermesGeneratedExportDiagnostic::SecondSelectorTrap => {
                QueryShadowGeneratedExportDiagnostic::SecondSelectorTrap
            },
            StaticHermesGeneratedExportDiagnostic::SecondSelectorRejected { status } => {
                QueryShadowGeneratedExportDiagnostic::SecondSelectorRejected { status: *status }
            },
        });
    let wasmtime_trap_diagnostic = error
        .downcast_ref::<StaticHermesWasmExecutionFailure>()
        .and_then(|failure| match failure {
            StaticHermesWasmExecutionFailure::WasmtimeTrap { diagnostic } => {
                Some(diagnostic.clone())
            },
            _ => None,
        });
    if is_shadow_lane && !error.is::<ShadowCapacityError>() {
        error.context(ShadowLaneFailureStage {
            stage,
            reason,
            generated_export_diagnostic,
            wasmtime_trap_diagnostic,
        })
    } else {
        error
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn classify_static_hermes_preparation_error(
    error: anyhow::Error,
    is_shadow_lane: bool,
) -> anyhow::Error {
    classify_shadow_lane_error(
        error,
        is_shadow_lane,
        QueryShadowFailureStage::RouteAuthentication,
    )
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn shadow_failure(
    error: &anyhow::Error,
) -> (
    QueryShadowFailureStage,
    QueryShadowFailureReason,
    Option<QueryShadowGeneratedExportDiagnostic>,
    Option<StaticHermesWasmTrapDiagnostic>,
) {
    error
        .downcast_ref::<ShadowLaneFailureStage>()
        .map(|failure| {
            (
                failure.stage,
                failure.reason,
                failure.generated_export_diagnostic,
                failure.wasmtime_trap_diagnostic.clone(),
            )
        })
        .unwrap_or((
            QueryShadowFailureStage::Unclassified,
            QueryShadowFailureReason::Unclassified,
            None,
            None,
        ))
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
enum PrimaryShadowObservation {
    Observation {
        observation: ShadowObservation,
        duration: Duration,
    },
    Terminal(QueryShadowTerminal),
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
enum ShadowObservation {
    Query(QueryShadowObservation),
    Mutation(MutationShadowObservation),
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Clone)]
pub struct QueryShadowPrimaryPublication(Arc<QueryShadowPrimaryPublicationState>);

#[cfg(feature = "static-hermes-wasmtime-gate")]
struct QueryShadowPrimaryPublicationState {
    pending: Mutex<Option<PendingQueryShadowPrimaryPublication>>,
    snapshot_validation: ShadowSnapshotValidation,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Clone)]
struct ShadowSnapshotValidation {
    timestamp: Timestamp,
    retention_validator: Arc<dyn RetentionValidator>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl ShadowSnapshotValidation {
    async fn validate(&self) -> Result<(), QueryShadowTerminal> {
        // A detached verifier may finish after the primary's retention check.
        // An unavailable historical snapshot cannot support a comparison.
        self.retention_validator
            .validate_snapshot(self.timestamp)
            .await
            .map_err(|_| QueryShadowTerminal::InvalidShadow {
                reason: QueryShadowInvalidReason::SnapshotValidation,
            })
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
struct PendingQueryShadowPrimaryPublication {
    sender: oneshot::Sender<PrimaryShadowObservation>,
    primary: Option<PrimaryShadowObservation>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl QueryShadowPrimaryPublication {
    pub(crate) fn new(
        timestamp: Timestamp,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> Self {
        Self(Arc::new(QueryShadowPrimaryPublicationState {
            pending: Mutex::new(None),
            snapshot_validation: ShadowSnapshotValidation {
                timestamp,
                retention_validator,
            },
        }))
    }

    fn arm(&self, sender: oneshot::Sender<PrimaryShadowObservation>) {
        assert!(
            self.0
                .pending
                .lock()
                .expect("query-shadow primary publication lock was poisoned")
                .replace(PendingQueryShadowPrimaryPublication {
                    sender,
                    primary: None,
                })
                .is_none(),
            "query-shadow primary publication was armed twice"
        );
    }

    fn record_primary(&self, primary: PrimaryShadowObservation) {
        let mut pending = self
            .0
            .pending
            .lock()
            .expect("query-shadow primary publication lock was poisoned");
        let pending = pending
            .as_mut()
            .expect("query-shadow primary result arrived without an admitted shadow");
        assert!(
            pending.primary.replace(primary).is_none(),
            "query-shadow primary result was recorded twice"
        );
    }

    pub(crate) fn publish_after_retention_validation(&self) {
        let Some(PendingQueryShadowPrimaryPublication { sender, primary }) = self
            .0
            .pending
            .lock()
            .expect("query-shadow primary publication lock was poisoned")
            .take()
        else {
            return;
        };
        let primary = primary.expect("retention passed before the primary result was recorded");
        let _ = sender.send(primary);
    }

    pub(crate) fn reject_after_retention_validation(&self) {
        self.send_terminal(QueryShadowTerminal::PrimarySnapshotRejected);
    }

    pub(crate) fn primary_failed(&self) {
        self.send_terminal(QueryShadowTerminal::PrimaryFailure);
    }

    fn send_terminal(&self, terminal: QueryShadowTerminal) {
        let pending = self
            .0
            .pending
            .lock()
            .expect("query-shadow primary publication lock was poisoned")
            .take();
        if let Some(PendingQueryShadowPrimaryPublication { sender, .. }) = pending {
            let _ = sender.send(PrimaryShadowObservation::Terminal(terminal));
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl PrimaryShadowObservation {
    fn from_execution(
        udf_type: UdfType,
        transaction: &FunctionFinalTransaction,
        outcome: &FunctionOutcome,
        duration: Duration,
        preexisting_write_ids: &BTreeSet<ResolvedDocumentId>,
    ) -> Self {
        let observation = match udf_type {
            UdfType::Query => {
                QueryShadowObservation::new(transaction, outcome).map(ShadowObservation::Query)
            },
            UdfType::Mutation => {
                MutationShadowObservation::new(transaction, outcome, preexisting_write_ids)
                    .map(ShadowObservation::Mutation)
            },
            UdfType::Action | UdfType::HttpAction => {
                return Self::Terminal(QueryShadowTerminal::InvalidPrimary);
            },
        };
        match observation {
            Ok(observation) => Self::Observation {
                observation,
                duration,
            },
            Err(_) => Self::Terminal(QueryShadowTerminal::InvalidPrimary),
        }
    }
}

/// Wait for the two shadow lanes without allowing shadow execution to govern
/// the authoritative V8 result.
///
/// If the V8 lane is cancelled or cannot produce a comparable observation,
/// return immediately and drop the still-running shadow future. Otherwise,
/// retain a completed shadow result until V8 has produced the result that the
/// caller receives.
#[cfg(feature = "static-hermes-wasmtime-gate")]
async fn await_shadow_lanes<Shadow>(
    primary_receiver: &mut oneshot::Receiver<PrimaryShadowObservation>,
    shadow: impl Future<Output = Shadow>,
    snapshot_validation: &ShadowSnapshotValidation,
) -> Result<Option<((ShadowObservation, Duration), Shadow)>, QueryShadowTerminal> {
    tokio::pin!(shadow);
    let first = tokio::select! {
        primary = &mut *primary_receiver => Either::Left(primary),
        shadow = &mut shadow => Either::Right(shadow),
    };
    let (primary, shadow) = match first {
        Either::Left(Ok(PrimaryShadowObservation::Observation {
            observation,
            duration,
        })) => ((observation, duration), shadow.await),
        Either::Left(Ok(PrimaryShadowObservation::Terminal(terminal))) => return Err(terminal),
        Either::Left(Err(_)) => return Ok(None),
        Either::Right(shadow) => match primary_receiver.await {
            Ok(PrimaryShadowObservation::Observation {
                observation,
                duration,
            }) => ((observation, duration), shadow),
            Ok(PrimaryShadowObservation::Terminal(terminal)) => return Err(terminal),
            Err(_) => return Ok(None),
        },
    };
    snapshot_validation.validate().await?;
    Ok(Some((primary, shadow)))
}

/// Bound the complete lifecycle of an admitted verifier lane. Dropping the lane
/// future on timeout cancels live execution; its detached worker retains the
/// permit work guard until cancellation cleanup has completed.
#[cfg(feature = "static-hermes-wasmtime-gate")]
async fn run_admitted_shadow_lane<RT: Runtime, T>(
    rt: &RT,
    timeout: Duration,
    lane: impl Future<Output = anyhow::Result<T>> + Send,
) -> anyhow::Result<T> {
    rt.with_timeout("Static Hermes shadow", timeout, lane).await
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
enum V8VerifierOutcome {
    Observation {
        observation: ShadowObservation,
        duration: Duration,
    },
    Terminal(QueryShadowTerminal),
}

/// Pair comparable Wasm-primary and V8-verifier observations.
///
/// Do not poll the verifier until the authoritative Wasm observation has
/// passed retention validation. V8 verifier work uses the ordinary isolate
/// scheduler, so speculative execution could consume capacity needed by
/// authoritative V8 requests.
#[cfg(feature = "static-hermes-wasmtime-gate")]
async fn await_v8_verifier_and_primary<Verifier>(
    primary_receiver: &mut oneshot::Receiver<PrimaryShadowObservation>,
    verifier: Verifier,
    snapshot_validation: &ShadowSnapshotValidation,
) -> Result<((ShadowObservation, Duration), (ShadowObservation, Duration)), QueryShadowTerminal>
where
    Verifier: Future<Output = V8VerifierOutcome>,
{
    let primary = match primary_receiver.await {
        Ok(PrimaryShadowObservation::Observation {
            observation,
            duration,
        }) => (observation, duration),
        Ok(PrimaryShadowObservation::Terminal(terminal)) => return Err(terminal),
        Err(_) => return Err(QueryShadowTerminal::PrimaryFailure),
    };
    let verifier = verifier.await;
    // Failed reads may also have observed expired history. Match the forward
    // lane's retention-first classification before reporting execution failure.
    snapshot_validation.validate().await?;
    let verifier = match verifier {
        V8VerifierOutcome::Observation {
            observation,
            duration,
        } => (observation, duration),
        V8VerifierOutcome::Terminal(terminal) => return Err(terminal),
    };
    Ok((primary, verifier))
}

/// Start one admitted V8 verifier, then return the authoritative Wasm result
/// without waiting for verifier execution or comparison. The verifier receives
/// the primary observation only after the caller's normal retention check.
#[cfg(feature = "static-hermes-wasmtime-gate")]
async fn run_wasm_primary_with_v8_verifier<RT, Primary, PrimaryFuture, VerifierFuture, Observe>(
    rt: &RT,
    shadow_timeout: Duration,
    mut shadow_permit: QueryShadowPermit,
    primary_publication: QueryShadowPrimaryPublication,
    primary: PrimaryFuture,
    verifier: VerifierFuture,
    observe_primary: Observe,
) -> anyhow::Result<Primary>
where
    RT: Runtime,
    PrimaryFuture: Future<Output = anyhow::Result<Primary>>,
    VerifierFuture: Future<Output = V8VerifierOutcome> + Send + 'static,
    Observe: FnOnce(&Primary) -> PrimaryShadowObservation,
{
    let (primary_sender, mut primary_receiver) = oneshot::channel::<PrimaryShadowObservation>();
    primary_publication.arm(primary_sender);
    // Retain validation without retaining the primary sender. Caller
    // cancellation must still close the observation channel promptly.
    let snapshot_validation = primary_publication.0.snapshot_validation.clone();
    let background_rt = rt.clone();
    rt.spawn_background("static_hermes_v8_shadow", async move {
        let result = run_admitted_shadow_lane(&background_rt, shadow_timeout, async {
            let (primary, (shadow, verifier_duration)) = match await_v8_verifier_and_primary(
                &mut primary_receiver,
                verifier,
                &snapshot_validation,
            )
            .await
            {
                Ok(observations) => observations,
                Err(terminal) => {
                    shadow_permit.record_terminal(terminal);
                    return Ok(());
                },
            };
            let comparison = match (primary.0, shadow) {
                (ShadowObservation::Query(primary), ShadowObservation::Query(shadow)) => {
                    compare_query_shadow(primary, shadow)
                },
                (ShadowObservation::Mutation(primary), ShadowObservation::Mutation(shadow)) => {
                    compare_mutation_shadow(primary, shadow)
                },
                _ => {
                    shadow_permit.record_terminal(QueryShadowTerminal::InvalidShadow {
                        reason: QueryShadowInvalidReason::LaneTypeMismatch,
                    });
                    return Ok(());
                },
            };
            shadow_permit.record_comparison_with_verifier(
                primary.1,
                primary.1,
                Some(verifier_duration),
                comparison,
            );
            Ok(())
        })
        .await;
        if result.is_err() {
            shadow_permit.record_terminal(QueryShadowTerminal::Timeout);
        }
    });

    let primary = primary.await?;
    primary_publication.record_primary(observe_primary(&primary));
    Ok(primary)
}

pub struct RunRequestArgs {
    pub execution_mode: FunctionExecutionMode,
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub query_shadow_primary_publication: Option<QueryShadowPrimaryPublication>,
    pub key_broker: FunctionRunnerKeyBroker,
    pub index_reader: Arc<dyn IndexReader>,
    pub convex_origin: ConvexOrigin,
    pub bootstrap_metadata: BootstrapMetadata,
    pub table_count_snapshot: Arc<dyn TableCountSnapshot>,
    pub text_index_snapshot: Arc<dyn TransactionTextSnapshot>,
    pub action_callbacks: Arc<dyn ActionCallbacks>,
    pub fetch_client: Arc<dyn FetchClient>,
    pub log_line_sender: Option<mpsc::UnboundedSender<LogLine>>,
    pub function_started_sender: Option<oneshot::Sender<()>>,
    pub function_execution_start: Option<FunctionExecutionStartGate>,
    pub udf_type: UdfType,
    pub identity: Identity,
    pub existing_writes: FunctionWrites,
    pub default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    pub in_memory_index_last_modified: BTreeMap<IndexId, Timestamp>,
    pub context: ExecutionContext,
    pub scheduler_dependency: SchedulerDependencyClass,
    pub subfunctions_in_same_isolate: bool,
    pub deployment: DeploymentMetadata,
}

#[derive(Clone)]
pub enum FunctionMetadata {
    Query {
        path_and_args: ValidatedPathAndArgs,
        journal: QueryJournal,
        active_javascript_class: ActiveJavascriptClass,
    },
    Mutation {
        path_and_args: ValidatedPathAndArgs,
        journal: QueryJournal,
    },
    Action {
        path_and_args: ValidatedPathAndArgs,
    },
}

impl FunctionMetadata {
    fn path_and_args(&self) -> &ValidatedPathAndArgs {
        match self {
            Self::Query { path_and_args, .. }
            | Self::Mutation { path_and_args, .. }
            | Self::Action { path_and_args } => path_and_args,
        }
    }
}

pub struct HttpActionMetadata {
    pub http_response_streamer: HttpActionResponseStreamer,
    pub http_module_path: ValidatedHttpPath,
    pub routed_path: RoutedHttpPath,
    pub http_request: HttpActionRequestInner,
}

#[async_trait]
pub trait StorageForDeployment<RT: Runtime>: Debug + Clone + Send + Sync + 'static {
    /// Gets a storage impl for a deployment. Agnostic to what kind of storage -
    /// local or s3, or how it was loaded (e.g. passed directly within backend,
    /// loaded from a transaction created in Funrun)
    async fn storage_for_deployment(
        &self,
        transaction: &mut Transaction<RT>,
        use_case: StorageUseCase,
    ) -> anyhow::Result<Arc<dyn Storage>>;
}

#[derive(Clone, Debug)]
pub struct DeploymentStorage {
    pub files_storage: Arc<dyn Storage>,
    pub modules_storage: Arc<dyn Storage>,
}

#[async_trait]
impl<RT: Runtime> StorageForDeployment<RT> for DeploymentStorage {
    async fn storage_for_deployment(
        &self,
        _transaction: &mut Transaction<RT>,
        use_case: StorageUseCase,
    ) -> anyhow::Result<Arc<dyn Storage>> {
        match use_case {
            StorageUseCase::Files => Ok(self.files_storage.clone()),
            StorageUseCase::Modules => Ok(self.modules_storage.clone()),
            _ => anyhow::bail!("function runner storage does not support {use_case}"),
        }
    }
}

pub struct FunctionRunnerCore<RT: Runtime, S: StorageForDeployment<RT>> {
    rt: RT,
    storage: S,
    index_cache: InMemoryIndexCache<RT>,
    module_cache: ModuleCache<RT>,
    code_cache: CodeCache,
    isolate_client: IsolateClient<RT>,
}

impl<RT: Runtime, S: StorageForDeployment<RT>> Clone for FunctionRunnerCore<RT, S> {
    fn clone(&self) -> Self {
        Self {
            rt: self.rt.clone(),
            storage: self.storage.clone(),
            index_cache: self.index_cache.clone(),
            module_cache: self.module_cache.clone(),
            code_cache: self.code_cache.clone(),
            isolate_client: self.isolate_client.clone(),
        }
    }
}

#[fastrace::trace]
pub async fn validate_run_function_result(
    udf_type: UdfType,
    ts: Timestamp,
    retention_validator: Arc<dyn RetentionValidator>,
) -> anyhow::Result<()> {
    match udf_type {
        // Since queries and mutations have no side effects, we perform the
        // retention check here, when validating the result.
        UdfType::Query | UdfType::Mutation => retention_validator
            .validate_snapshot(ts)
            .await
            .context("Function runner retention check changed"),
        // Since Actions can have side effects, we have to validate their
        // retention while we run them. We can't perform an additional check
        // here since actions can run longer than the retention.
        UdfType::Action | UdfType::HttpAction => Ok(()),
    }
}

impl<RT: Runtime, S: StorageForDeployment<RT>> FunctionRunnerCore<RT, S> {
    pub fn new<W: IsolateWorker<RT>>(
        rt: RT,
        storage: S,
        max_percent_per_client: usize,
        isolate_worker: W,
    ) -> anyhow::Result<Self> {
        let max_isolate_workers = *MAX_ISOLATE_WORKERS;
        let isolate_client = IsolateClient::new(
            rt.clone(),
            max_percent_per_client,
            max_isolate_workers,
            isolate_worker,
        )?;
        let index_cache = InMemoryIndexCache::new(rt.clone());
        let module_cache = ModuleCache::new(rt.clone());
        let code_cache = CodeCache::new();

        Ok(Self {
            rt,
            storage,
            index_cache,
            module_cache,
            code_cache,
            isolate_client,
        })
    }

    pub fn active_isolate_workers(&self) -> usize {
        self.isolate_client.active_workers()
    }

    pub fn max_isolate_workers(&self) -> usize {
        self.isolate_client.max_workers()
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.isolate_client.shutdown().await
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    async fn begin_database_lane_transaction(
        &self,
        request: &DatabaseLaneRequest,
        is_shadow_lane: bool,
    ) -> anyhow::Result<DatabaseLaneTransaction<RT>> {
        let usage_tracker = FunctionUsageTracker::new();
        let transaction = self
            .index_cache
            .begin_tx(
                request.identity.clone(),
                request.existing_writes.clone(),
                Arc::clone(&request.index_reader),
                request.deployment.name.clone(),
                request.in_memory_index_last_modified.clone(),
                request.bootstrap_metadata.clone(),
                Arc::clone(&request.table_count_snapshot),
                Arc::clone(&request.text_index_snapshot),
                usage_tracker.clone(),
            )
            .await
            .map_err(|error| {
                classify_shadow_lane_error(
                    error,
                    is_shadow_lane,
                    QueryShadowFailureStage::TransactionSetup,
                )
            })?;
        Ok(DatabaseLaneTransaction {
            transaction,
            usage_tracker,
        })
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    async fn run_database_lane(
        &self,
        request: DatabaseLaneRequest,
        inputs: DatabaseUdfExecutionInputs,
        static_hermes_route: Option<isolate::StaticHermesWasmtimeRouteHandle>,
        mode: DatabaseLaneMode,
        prestarted: Option<DatabaseLaneTransaction<RT>>,
    ) -> anyhow::Result<(
        FunctionFinalTransaction,
        FunctionOutcome,
        FunctionUsageStats,
    )> {
        let (is_shadow_lane, shadow_work_guard, execution_failure_stage, memory_observer) =
            match mode {
                DatabaseLaneMode::Primary => {
                    (false, None, QueryShadowFailureStage::WasmExecution, None)
                },
                DatabaseLaneMode::ObservedPrimary(observer) => (
                    false,
                    None,
                    QueryShadowFailureStage::WasmExecution,
                    Some(observer),
                ),
                DatabaseLaneMode::WasmShadow(work_guard, observer) => (
                    true,
                    work_guard,
                    QueryShadowFailureStage::WasmExecution,
                    observer,
                ),
                DatabaseLaneMode::V8Verifier(work_guard) => (
                    true,
                    Some(work_guard),
                    QueryShadowFailureStage::VerifierExecution,
                    None,
                ),
            };
        let DatabaseLaneTransaction {
            mut transaction,
            usage_tracker,
        } = match prestarted {
            Some(prestarted) => prestarted,
            None => {
                self.begin_database_lane_transaction(&request, is_shadow_lane)
                    .await?
            },
        };
        let DatabaseLaneRequest {
            key_broker,
            convex_origin,
            default_system_env_vars,
            context,
            scheduler_dependency,
            subfunctions_in_same_isolate,
            deployment,
            udf_type,
            function_metadata,
            function_started_sender,
            trace_host_operations,
            ..
        } = request;
        let DatabaseUdfExecutionInputs {
            rng_seed,
            unix_timestamp,
        } = inputs;
        let (path_and_args, journal, active_javascript_class) = match (udf_type, function_metadata)
        {
            (
                UdfType::Query,
                FunctionMetadata::Query {
                    path_and_args,
                    journal,
                    active_javascript_class,
                },
            ) => (path_and_args, journal, active_javascript_class),
            (
                UdfType::Mutation,
                FunctionMetadata::Mutation {
                    path_and_args,
                    journal,
                },
            ) => (path_and_args, journal, ActiveJavascriptClass::Protected),
            (UdfType::Query | UdfType::Mutation, FunctionMetadata::Action { .. })
            | (UdfType::Query, FunctionMetadata::Mutation { .. })
            | (UdfType::Mutation, FunctionMetadata::Query { .. })
            | (UdfType::Action | UdfType::HttpAction, _) => {
                anyhow::bail!("Function metadata does not match {udf_type}")
            },
        };
        let deployment_name = deployment.name.clone();
        let static_hermes_route = match static_hermes_route {
            Some(route) => IsolateClient::<RT>::select_static_hermes_wasmtime_route_for_invocation(
                route,
                udf_type,
                &path_and_args,
                &mut transaction,
            )
            .await
            .map_err(|error| classify_static_hermes_preparation_error(error, is_shadow_lane))?,
            None => None,
        };
        let storage = self
            .storage
            .storage_for_deployment(&mut transaction, StorageUseCase::Files)
            .await
            .map_err(|error| {
                classify_shadow_lane_error(
                    error,
                    is_shadow_lane,
                    QueryShadowFailureStage::EnvironmentSetup,
                )
            })?;
        let file_storage = TransactionalFileStorage::new(self.rt.clone(), storage, convex_origin);
        let modules_storage = self
            .storage
            .storage_for_deployment(&mut transaction, StorageUseCase::Modules)
            .await
            .map_err(|error| {
                classify_shadow_lane_error(
                    error,
                    is_shadow_lane,
                    QueryShadowFailureStage::EnvironmentSetup,
                )
            })?;
        let mut environment_data = EnvironmentData {
            key_broker,
            default_system_env_vars,
            file_storage,
            module_loader: Arc::new(FunctionRunnerModuleLoader {
                deployment_name: deployment_name.clone(),
                cache: self.module_cache.clone(),
                code_cache: self.code_cache.clone(),
                modules_storage,
            }),
            deployment,
            host_secret_values: Some(BTreeMap::new()),
        };
        let (transaction, outcome) = if let Some(route) = static_hermes_route {
            let mut prepared = IsolateClient::<RT>::prepare_static_hermes_wasmtime_invocation(
                route,
                udf_type,
                path_and_args,
                transaction,
                if is_shadow_lane {
                    StaticHermesWasmtimePreparationMode::Shadow
                } else {
                    StaticHermesWasmtimePreparationMode::Primary
                },
            )
            .await
            .map_err(|error| classify_static_hermes_preparation_error(error, is_shadow_lane))?;
            if let Some(observer) = memory_observer {
                prepared.observe_memory(observer);
            }
            let host_secret_values = prepared
                .load_authorized_host_secret_values()
                .await
                .map_err(|error| {
                    classify_shadow_lane_error(
                        error,
                        is_shadow_lane,
                        QueryShadowFailureStage::EnvironmentSetup,
                    )
                })?;
            let destination = environment_data
                .host_secret_values
                .as_mut()
                .expect("top-level function lost its host-secret state");
            assert!(
                destination.is_empty(),
                "top-level function received pre-populated host-secret state"
            );
            *destination = host_secret_values;
            self.isolate_client
                .execute_static_hermes_wasmtime_gate(
                    prepared,
                    deployment_name,
                    udf_type,
                    journal,
                    context,
                    environment_data,
                    rng_seed,
                    unix_timestamp,
                    function_started_sender,
                    trace_host_operations,
                    shadow_work_guard,
                    trace_host_operations,
                )
                .await
                .map_err(|error| {
                    classify_shadow_lane_error(error, is_shadow_lane, execution_failure_stage)
                })?
        } else {
            self.isolate_client
                .execute_udf(
                    udf_type,
                    path_and_args,
                    transaction,
                    journal,
                    context,
                    environment_data,
                    rng_seed,
                    unix_timestamp,
                    0,
                    deployment_name,
                    function_started_sender,
                    subfunctions_in_same_isolate,
                    trace_host_operations,
                    shadow_work_guard,
                    scheduler_dependency,
                    active_javascript_class,
                )
                .await
                .map_err(|error| {
                    classify_shadow_lane_error(error, is_shadow_lane, execution_failure_stage)
                })?
        };
        Ok((
            transaction.try_into().map_err(|error| {
                classify_shadow_lane_error(
                    error,
                    is_shadow_lane,
                    QueryShadowFailureStage::TransactionFinalization,
                )
            })?,
            outcome,
            usage_tracker.gather_user_stats(),
        ))
    }

    // Runs a function given the information for the backend as well as arguments
    // to the function itself.
    // NOTE: The caller of this is responsible of checking retention by calling
    // `validate_function_runner_result`. If the retention check fails, we should
    // ignore any results or errors returned by this method.
    #[fastrace::trace]
    pub async fn run_function_no_retention_check(
        &self,
        run_request_args: RunRequestArgs,
        function_metadata: Option<FunctionMetadata>,
        http_action_metadata: Option<HttpActionMetadata>,
    ) -> anyhow::Result<FunctionExecutionResult> {
        self.run_function_no_retention_check_inner(
            run_request_args,
            function_metadata,
            http_action_metadata,
        )
        .boxed()
        .await
    }

    pub async fn run_function_no_retention_check_inner(
        &self,
        RunRequestArgs {
            execution_mode,
            #[cfg(feature = "static-hermes-wasmtime-gate")]
            query_shadow_primary_publication,
            key_broker,
            index_reader,
            convex_origin,
            bootstrap_metadata,
            table_count_snapshot,
            text_index_snapshot,
            action_callbacks,
            fetch_client,
            log_line_sender,
            function_started_sender,
            function_execution_start,
            udf_type,
            identity,
            existing_writes,
            default_system_env_vars,
            in_memory_index_last_modified,
            context,
            scheduler_dependency,
            subfunctions_in_same_isolate,
            deployment,
        }: RunRequestArgs,
        function_metadata: Option<FunctionMetadata>,
        http_action_metadata: Option<HttpActionMetadata>,
    ) -> anyhow::Result<FunctionExecutionResult> {
        validate_execution_mode_for_build(&execution_mode)?;
        #[cfg(feature = "static-hermes-wasmtime-gate")]
        let configured_static_hermes_route =
            match (&execution_mode, udf_type, function_metadata.as_ref()) {
                (
                    FunctionExecutionMode::V8 | FunctionExecutionMode::V8WithHostOperationTrace,
                    ..,
                ) => Ok(None),
                (
                    FunctionExecutionMode::V8WithStaticHermesWasmShadow(_),
                    UdfType::Query | UdfType::Mutation,
                    Some(function_metadata),
                ) => IsolateClient::<RT>::resolve_static_hermes_wasmtime_shadow_route(
                    udf_type,
                    function_metadata.path_and_args(),
                ),
                #[cfg(feature = "testing")]
                (
                    FunctionExecutionMode::V8WithStaticHermesWasmMutationShadowForTest { .. },
                    UdfType::Mutation,
                    Some(function_metadata),
                ) => IsolateClient::<RT>::resolve_static_hermes_wasmtime_shadow_route(
                    udf_type,
                    function_metadata.path_and_args(),
                ),
                (_, UdfType::Query | UdfType::Mutation, Some(function_metadata)) => {
                    // Pin the route before constructing the function-runner transaction.
                    // Any later Wasm failure must fail that Wasm lane.
                    IsolateClient::<RT>::resolve_static_hermes_wasmtime_route(
                        udf_type,
                        function_metadata.path_and_args(),
                    )
                },
                _ => Ok(None),
            };
        #[cfg(feature = "static-hermes-wasmtime-gate")]
        let execution_mode = match (&execution_mode, &configured_static_hermes_route) {
            // Sampling happens before path-aware route resolution. An absent
            // route therefore means that this is an unselected function, which
            // must retain the ordinary V8 behavior. Registry/authentication
            // errors remain errors and never take this downgrade.
            (FunctionExecutionMode::StaticHermesWasmWithV8Shadow(_), Ok(None)) => {
                FunctionExecutionMode::Configured
            },
            _ => execution_mode,
        };
        let trace_host_operations = execution_mode.trace_host_operations();
        #[cfg(feature = "static-hermes-wasmtime-gate")]
        let static_hermes_wasm_required = matches!(
            &execution_mode,
            FunctionExecutionMode::StaticHermesWasm
                | FunctionExecutionMode::StaticHermesWasmWithV8Shadow(_)
                | FunctionExecutionMode::StaticHermesWasmWithHostOperationTrace
        );
        #[cfg(feature = "static-hermes-wasmtime-gate")]
        let static_hermes_route = match execution_mode {
            FunctionExecutionMode::Configured => configured_static_hermes_route?,
            FunctionExecutionMode::V8 | FunctionExecutionMode::V8WithHostOperationTrace => None,
            FunctionExecutionMode::StaticHermesWasm
            | FunctionExecutionMode::StaticHermesWasmWithHostOperationTrace => {
                let Some(route) = configured_static_hermes_route? else {
                    return Ok(FunctionExecutionResult::StaticHermesWasmUnavailable);
                };
                Some(route)
            },
            FunctionExecutionMode::V8WithStaticHermesWasmShadow(shadow_admission) => {
                let query_shadow_primary_publication = query_shadow_primary_publication
                    .as_ref()
                    .context("Static Hermes shadow request lacked primary publication state")?;
                anyhow::ensure!(
                    matches!(udf_type, UdfType::Query | UdfType::Mutation),
                    "Static Hermes production shadow supports queries and mutations only"
                );
                let shadow_timeout = shadow_admission.shadow_timeout();
                let function_metadata =
                    function_metadata.context("Missing database function metadata")?;
                let mut primary_request = DatabaseLaneRequest {
                    key_broker,
                    index_reader,
                    convex_origin,
                    bootstrap_metadata,
                    table_count_snapshot,
                    text_index_snapshot,
                    identity,
                    existing_writes,
                    default_system_env_vars,
                    in_memory_index_last_modified,
                    context,
                    scheduler_dependency,
                    subfunctions_in_same_isolate,
                    deployment,
                    udf_type,
                    function_metadata,
                    function_started_sender,
                    trace_host_operations: false,
                };
                let primary_transaction_started = self.rt.monotonic_now();
                let primary_prestarted = self
                    .begin_database_lane_transaction(&primary_request, false)
                    .await?;
                let primary_transaction_duration = self
                    .rt
                    .monotonic_now()
                    .duration_since(primary_transaction_started);
                // Both lanes share the primary transaction's Date.now() input.
                // Derive it from the primary creation-time cursor before
                // starting the shadow transaction.
                let inputs = DatabaseUdfExecutionInputs {
                    rng_seed: self.rt.rng().random(),
                    unix_timestamp: udf_unix_timestamp(
                        primary_prestarted.transaction.next_creation_time(),
                    ),
                };
                let preexisting_write_ids: BTreeSet<_> = primary_request
                    .existing_writes
                    .updates
                    .iter()
                    .map(|update| update.id())
                    .collect();
                let shadow_candidate = match configured_static_hermes_route {
                    Ok(Some(route_candidate)) => {
                        let shadow_request = DatabaseLaneRequest {
                            key_broker: primary_request.key_broker.clone(),
                            index_reader: primary_request.index_reader.clone(),
                            convex_origin: primary_request.convex_origin.clone(),
                            bootstrap_metadata: primary_request.bootstrap_metadata.clone(),
                            table_count_snapshot: primary_request.table_count_snapshot.clone(),
                            text_index_snapshot: primary_request.text_index_snapshot.clone(),
                            identity: primary_request.identity.clone(),
                            existing_writes: primary_request.existing_writes.clone(),
                            default_system_env_vars: primary_request
                                .default_system_env_vars
                                .clone(),
                            in_memory_index_last_modified: primary_request
                                .in_memory_index_last_modified
                                .clone(),
                            context: primary_request.context.clone(),
                            scheduler_dependency,
                            subfunctions_in_same_isolate,
                            deployment: primary_request.deployment.clone(),
                            udf_type,
                            function_metadata: primary_request.function_metadata.clone(),
                            function_started_sender: None,
                            trace_host_operations: true,
                        };
                        let shadow_transaction = self
                            .begin_database_lane_transaction(&shadow_request, true)
                            .await
                            .and_then(|mut prestarted| {
                                prestarted.transaction.apply_document_creation_state(
                                    &primary_prestarted.transaction.document_creation_state(),
                                )?;
                                Ok(prestarted)
                            });
                        match shadow_transaction {
                            Ok(mut prestarted) => {
                                let selected_route = IsolateClient::<RT>::
                                    select_static_hermes_wasmtime_route_for_invocation(
                                        route_candidate,
                                        udf_type,
                                        shadow_request.function_metadata.path_and_args(),
                                        &mut prestarted.transaction,
                                    )
                                    .await;
                                match selected_route {
                                    Ok(Some(shadow_route)) => {
                                        let route_key = match (
                                            shadow_route.authenticated_generation_sha256(),
                                            shadow_route.authenticated_route_id(),
                                        ) {
                                            (Some(generation_sha256), Some(route_id)) => {
                                                QueryShadowRouteKey::new(
                                                    generation_sha256,
                                                    route_id,
                                                )
                                                .ok()
                                            },
                                            (None, None) | (Some(_), None) | (None, Some(_)) => {
                                                None
                                            },
                                        };
                                        match route_key {
                                            Some(route_key) => shadow_admission
                                                .try_admit(udf_type, &route_key)
                                                .map(|shadow_permit| {
                                                    (
                                                        shadow_request,
                                                        shadow_route,
                                                        shadow_permit,
                                                        prestarted,
                                                    )
                                                }),
                                            None => {
                                                log_query_shadow_terminal(
                                                    udf_type,
                                                    "invalid_route_key",
                                                );
                                                None
                                            },
                                        }
                                    },
                                    Ok(None) => {
                                        log_query_shadow_terminal(udf_type, "route_unavailable");
                                        None
                                    },
                                    Err(_) => {
                                        log_query_shadow_terminal(
                                            udf_type,
                                            "route_resolution_failure",
                                        );
                                        None
                                    },
                                }
                            },
                            Err(_) => {
                                log_query_shadow_terminal(udf_type, "route_resolution_failure");
                                None
                            },
                        }
                    },
                    Ok(None) => {
                        log_query_shadow_terminal(udf_type, "route_unavailable");
                        None
                    },
                    Err(_) => {
                        // Route resolution is shadow-only. Its error details may contain
                        // operator context and must not affect or enter telemetry for V8.
                        log_query_shadow_terminal(udf_type, "route_resolution_failure");
                        None
                    },
                };
                primary_request.trace_host_operations = shadow_candidate.is_some();
                let primary_sender =
                    if let Some((shadow_request, shadow_route, mut shadow_permit, prestarted)) =
                        shadow_candidate
                    {
                        let (primary_sender, mut primary_receiver) =
                            oneshot::channel::<PrimaryShadowObservation>();
                        query_shadow_primary_publication.arm(primary_sender);
                        let snapshot_validation = query_shadow_primary_publication
                            .0
                            .snapshot_validation
                            .clone();
                        let shadow_runner = self.clone();
                        let shadow_preexisting_write_ids = preexisting_write_ids.clone();
                        self.rt
                            .spawn_background("static_hermes_shadow", async move {
                                let result = run_admitted_shadow_lane(
                                    &shadow_runner.rt,
                                    shadow_timeout,
                                    async {
                                        let shadow = async {
                                            let shadow_started = shadow_runner.rt.monotonic_now();
                                            let result = shadow_runner
                                                .run_database_lane(
                                                    shadow_request,
                                                    inputs,
                                                    Some(shadow_route),
                                                    DatabaseLaneMode::WasmShadow(
                                                        Some(shadow_permit.work_guard()),
                                                        Some(shadow_permit.memory_observer()),
                                                    ),
                                                    Some(prestarted),
                                                )
                                                .await;
                                            let duration = shadow_runner
                                                .rt
                                                .monotonic_now()
                                                .duration_since(shadow_started);
                                            (result, duration)
                                        };
                                        let ((primary, primary_duration), (shadow, wasm_duration)) =
                                            match await_shadow_lanes(
                                                &mut primary_receiver,
                                                shadow,
                                                &snapshot_validation,
                                            )
                                            .await
                                            {
                                                Ok(Some(paired)) => paired,
                                                Ok(None) => {
                                                    let terminal =
                                                        QueryShadowTerminal::PrimaryFailure;
                                                    log_query_shadow_terminal(
                                                        udf_type,
                                                        terminal.metric_key(),
                                                    );
                                                    shadow_permit.record_terminal(terminal);
                                                    return Ok(());
                                                },
                                                Err(terminal) => {
                                                    log_query_shadow_terminal(
                                                        udf_type,
                                                        terminal.metric_key(),
                                                    );
                                                    shadow_permit.record_terminal(terminal);
                                                    return Ok(());
                                                },
                                            };
                                        let shadow = match shadow {
                                            Ok((transaction, outcome, _)) => {
                                                let observation = match udf_type {
                                                    UdfType::Query => {
                                                        QueryShadowObservation::from_owned(
                                                            transaction,
                                                            outcome,
                                                        )
                                                        .map(ShadowObservation::Query)
                                                    },
                                                    UdfType::Mutation => {
                                                        let observation =
                                                            MutationShadowObservation::from_owned(
                                                                transaction,
                                                                outcome,
                                                                &shadow_preexisting_write_ids,
                                                            );
                                                        observation.map(ShadowObservation::Mutation)
                                                    },
                                                    UdfType::Action | UdfType::HttpAction => {
                                                        unreachable!(
                                                            "admitted an unsupported shadow UDF \
                                                             type"
                                                        )
                                                    },
                                                };
                                                match observation {
                                                    Ok(observation) => observation,
                                                    Err(reason) => {
                                                        let terminal =
                                                            QueryShadowTerminal::InvalidShadow {
                                                                reason,
                                                            };
                                                        log_query_shadow_terminal(
                                                            udf_type,
                                                            terminal.metric_key(),
                                                        );
                                                        shadow_permit.record_terminal(terminal);
                                                        return Ok(());
                                                    },
                                                }
                                            },
                                            Err(error) => {
                                                let terminal = if error.is::<ShadowCapacityError>()
                                                {
                                                    QueryShadowTerminal::CapacityDrop
                                                } else {
                                                    let (
                                                        stage,
                                                        reason,
                                                        generated_export_diagnostic,
                                                        wasmtime_trap_diagnostic,
                                                    ) = shadow_failure(&error);
                                                    QueryShadowTerminal::ShadowFailure {
                                                        stage,
                                                        reason,
                                                        generated_export_diagnostic,
                                                        wasmtime_trap_diagnostic,
                                                    }
                                                };
                                                log_query_shadow_terminal(
                                                    udf_type,
                                                    terminal.metric_key(),
                                                );
                                                shadow_permit.record_terminal(terminal);
                                                return Ok(());
                                            },
                                        };
                                        let comparison = match (primary, shadow) {
                                            (
                                                ShadowObservation::Query(primary),
                                                ShadowObservation::Query(shadow),
                                            ) => compare_query_shadow(primary, shadow),
                                            (
                                                ShadowObservation::Mutation(primary),
                                                ShadowObservation::Mutation(shadow),
                                            ) => compare_mutation_shadow(primary, shadow),
                                            _ => {
                                                let terminal = QueryShadowTerminal::InvalidShadow {
                                                    reason:
                                                        QueryShadowInvalidReason::LaneTypeMismatch,
                                                };
                                                log_query_shadow_terminal(
                                                    udf_type,
                                                    terminal.metric_key(),
                                                );
                                                shadow_permit.record_terminal(terminal);
                                                return Ok(());
                                            },
                                        };
                                        shadow_permit.record_comparison(
                                            primary_duration,
                                            wasm_duration,
                                            comparison,
                                        );
                                        Ok(())
                                    },
                                )
                                .await;
                                if result.is_err() {
                                    let terminal = QueryShadowTerminal::Timeout;
                                    log_query_shadow_terminal(udf_type, terminal.metric_key());
                                    shadow_permit.record_terminal(terminal);
                                }
                            });
                        true
                    } else {
                        false
                    };
                let primary_started = self.rt.monotonic_now();
                let (transaction, outcome, usage_stats) = self
                    .run_database_lane(
                        primary_request,
                        inputs,
                        None,
                        DatabaseLaneMode::Primary,
                        Some(primary_prestarted),
                    )
                    .await?;
                let primary_duration = primary_transaction_duration
                    + self.rt.monotonic_now().duration_since(primary_started);
                if primary_sender {
                    let primary = PrimaryShadowObservation::from_execution(
                        udf_type,
                        &transaction,
                        &outcome,
                        primary_duration,
                        &preexisting_write_ids,
                    );
                    query_shadow_primary_publication.record_primary(primary);
                }
                return Ok(FunctionExecutionResult::Completed {
                    transaction: Some(transaction),
                    outcome,
                    usage_stats,
                });
            },
            FunctionExecutionMode::StaticHermesWasmWithV8Shadow(shadow_admission) => {
                let query_shadow_primary_publication =
                    query_shadow_primary_publication.as_ref().context(
                        "Static Hermes Wasm-primary request lacked primary publication state",
                    )?;
                anyhow::ensure!(
                    matches!(udf_type, UdfType::Query | UdfType::Mutation),
                    "Static Hermes Wasm-primary verification supports queries and mutations only"
                );
                let shadow_timeout = shadow_admission.shadow_timeout();
                let function_metadata =
                    function_metadata.context("Missing database function metadata")?;
                let mut primary_request = DatabaseLaneRequest {
                    key_broker,
                    index_reader,
                    convex_origin,
                    bootstrap_metadata,
                    table_count_snapshot,
                    text_index_snapshot,
                    identity,
                    existing_writes,
                    default_system_env_vars,
                    in_memory_index_last_modified,
                    context,
                    scheduler_dependency,
                    subfunctions_in_same_isolate,
                    deployment,
                    udf_type,
                    function_metadata,
                    function_started_sender,
                    trace_host_operations: true,
                };
                let primary_transaction_started = self.rt.monotonic_now();
                let mut primary_prestarted = self
                    .begin_database_lane_transaction(&primary_request, false)
                    .await?;
                let primary_transaction_duration = self
                    .rt
                    .monotonic_now()
                    .duration_since(primary_transaction_started);
                // Both lanes share the Wasm-primary lane's deterministic
                // Date.now() and RNG inputs.
                let inputs = DatabaseUdfExecutionInputs {
                    rng_seed: self.rt.rng().random(),
                    unix_timestamp: udf_unix_timestamp(
                        primary_prestarted.transaction.next_creation_time(),
                    ),
                };
                let route_candidate = configured_static_hermes_route?
                    .context("Wasm-primary verification route is unavailable")?;
                let primary_route =
                    IsolateClient::<RT>::select_static_hermes_wasmtime_route_for_invocation(
                        route_candidate,
                        udf_type,
                        primary_request.function_metadata.path_and_args(),
                        &mut primary_prestarted.transaction,
                    )
                    .await?;
                let Some(primary_route) = primary_route else {
                    // Source selection and quarantine may exclude a configured
                    // route after the primary transaction has started. This is
                    // ordinary V8-only routing, not strict Wasm unavailability.
                    primary_request.trace_host_operations = false;
                    let (transaction, outcome, usage_stats) = self
                        .run_database_lane(
                            primary_request,
                            inputs,
                            None,
                            DatabaseLaneMode::Primary,
                            Some(primary_prestarted),
                        )
                        .await?;
                    return Ok(FunctionExecutionResult::Completed {
                        transaction: Some(transaction),
                        outcome,
                        usage_stats,
                    });
                };
                let route_key = match (
                    primary_route.authenticated_generation_sha256(),
                    primary_route.authenticated_route_id(),
                ) {
                    (Some(generation_sha256), Some(route_id)) => {
                        QueryShadowRouteKey::new(generation_sha256, route_id).ok()
                    },
                    (None, None) | (Some(_), None) | (None, Some(_)) => None,
                };
                let Some(route_key) = route_key else {
                    // The primary remains Wasm even when verifier admission is
                    // unavailable for a singleton route. Do not perform any V8
                    // work without authenticated route evidence.
                    let (transaction, outcome, usage_stats) = self
                        .run_database_lane(
                            primary_request,
                            inputs,
                            Some(primary_route),
                            DatabaseLaneMode::Primary,
                            Some(primary_prestarted),
                        )
                        .await?;
                    return Ok(FunctionExecutionResult::Completed {
                        transaction: Some(transaction),
                        outcome,
                        usage_stats,
                    });
                };
                let Some(shadow_permit) = shadow_admission.try_admit(udf_type, &route_key) else {
                    // The primary remains Wasm even when verifier admission is
                    // dropped. Do not perform any V8 work in that case.
                    let (transaction, outcome, usage_stats) = self
                        .run_database_lane(
                            primary_request,
                            inputs,
                            Some(primary_route),
                            DatabaseLaneMode::Primary,
                            Some(primary_prestarted),
                        )
                        .await?;
                    return Ok(FunctionExecutionResult::Completed {
                        transaction: Some(transaction),
                        outcome,
                        usage_stats,
                    });
                };
                let shadow_request = DatabaseLaneRequest {
                    key_broker: primary_request.key_broker.clone(),
                    index_reader: primary_request.index_reader.clone(),
                    convex_origin: primary_request.convex_origin.clone(),
                    bootstrap_metadata: primary_request.bootstrap_metadata.clone(),
                    table_count_snapshot: primary_request.table_count_snapshot.clone(),
                    text_index_snapshot: primary_request.text_index_snapshot.clone(),
                    identity: primary_request.identity.clone(),
                    existing_writes: primary_request.existing_writes.clone(),
                    default_system_env_vars: primary_request.default_system_env_vars.clone(),
                    in_memory_index_last_modified: primary_request
                        .in_memory_index_last_modified
                        .clone(),
                    context: primary_request.context.clone(),
                    // Verification is detached from the caller after the Wasm
                    // result is published. It cannot unblock that caller and
                    // must not consume the isolate scheduler's dependency
                    // reserve.
                    scheduler_dependency: SchedulerDependencyClass::Independent,
                    subfunctions_in_same_isolate,
                    deployment: primary_request.deployment.clone(),
                    udf_type,
                    function_metadata: primary_request.function_metadata.clone(),
                    function_started_sender: None,
                    trace_host_operations: true,
                };
                let shadow_work_guard = shadow_permit.work_guard();
                let shadow_runner = self.clone();
                let preexisting_write_ids: BTreeSet<_> = primary_request
                    .existing_writes
                    .updates
                    .iter()
                    .map(|update| update.id())
                    .collect();
                let shadow_preexisting_write_ids = preexisting_write_ids.clone();
                let document_creation_state =
                    primary_prestarted.transaction.document_creation_state();
                let verifier = async move {
                    let shadow_started = shadow_runner.rt.monotonic_now();
                    let shadow = async {
                        let mut prestarted = shadow_runner
                            .begin_database_lane_transaction(&shadow_request, true)
                            .await?;
                        prestarted
                            .transaction
                            .apply_document_creation_state(&document_creation_state)?;
                        shadow_runner
                            .run_database_lane(
                                shadow_request,
                                inputs,
                                None,
                                DatabaseLaneMode::V8Verifier(shadow_work_guard),
                                Some(prestarted),
                            )
                            .await
                    }
                    .await;
                    let duration = shadow_runner
                        .rt
                        .monotonic_now()
                        .duration_since(shadow_started);
                    let (shadow_transaction, shadow_outcome, _) = match shadow {
                        Ok(result) => result,
                        Err(error) => {
                            let (
                                stage,
                                reason,
                                generated_export_diagnostic,
                                wasmtime_trap_diagnostic,
                            ) = shadow_failure(&error);
                            return V8VerifierOutcome::Terminal(
                                QueryShadowTerminal::ShadowFailure {
                                    stage,
                                    reason,
                                    generated_export_diagnostic,
                                    wasmtime_trap_diagnostic,
                                },
                            );
                        },
                    };
                    let observation = match udf_type {
                        UdfType::Query => {
                            QueryShadowObservation::from_owned(shadow_transaction, shadow_outcome)
                                .map(ShadowObservation::Query)
                        },
                        UdfType::Mutation => MutationShadowObservation::from_owned(
                            shadow_transaction,
                            shadow_outcome,
                            &shadow_preexisting_write_ids,
                        )
                        .map(ShadowObservation::Mutation),
                        UdfType::Action | UdfType::HttpAction => unreachable!(),
                    };
                    match observation {
                        Ok(observation) => V8VerifierOutcome::Observation {
                            observation,
                            duration,
                        },
                        Err(reason) => {
                            V8VerifierOutcome::Terminal(QueryShadowTerminal::InvalidShadow {
                                reason,
                            })
                        },
                    }
                };
                let primary_started = self.rt.monotonic_now();
                let memory_observer = shadow_permit.memory_observer();
                let primary = async {
                    let result = self
                        .run_database_lane(
                            primary_request,
                            inputs,
                            Some(primary_route),
                            DatabaseLaneMode::ObservedPrimary(memory_observer),
                            Some(primary_prestarted),
                        )
                        .await?;
                    let duration = primary_transaction_duration
                        + self.rt.monotonic_now().duration_since(primary_started);
                    Ok((result, duration))
                };
                let ((transaction, outcome, usage_stats), _) = run_wasm_primary_with_v8_verifier(
                    &self.rt,
                    shadow_timeout,
                    shadow_permit,
                    query_shadow_primary_publication.clone(),
                    primary,
                    verifier,
                    |((transaction, outcome, _), duration)| {
                        PrimaryShadowObservation::from_execution(
                            udf_type,
                            transaction,
                            outcome,
                            *duration,
                            &preexisting_write_ids,
                        )
                    },
                )
                .await?;
                return Ok(FunctionExecutionResult::Completed {
                    transaction: Some(transaction),
                    outcome,
                    usage_stats,
                });
            },
            #[cfg(feature = "testing")]
            FunctionExecutionMode::V8WithStaticHermesWasmMutationShadowForTest { inputs } => {
                anyhow::ensure!(
                    udf_type == UdfType::Mutation,
                    "test-only Static Hermes mutation shadow supports mutations only"
                );
                let static_hermes_route = configured_static_hermes_route?
                    .context("test-only Static Hermes mutation shadow route is unavailable")?;
                let function_metadata =
                    function_metadata.context("Missing mutation function metadata")?;
                let shadow_request = DatabaseLaneRequest {
                    key_broker: key_broker.clone(),
                    index_reader: index_reader.clone(),
                    convex_origin: convex_origin.clone(),
                    bootstrap_metadata: bootstrap_metadata.clone(),
                    table_count_snapshot: table_count_snapshot.clone(),
                    text_index_snapshot: text_index_snapshot.clone(),
                    identity: identity.clone(),
                    existing_writes: existing_writes.clone(),
                    default_system_env_vars: default_system_env_vars.clone(),
                    in_memory_index_last_modified: in_memory_index_last_modified.clone(),
                    context: context.clone(),
                    scheduler_dependency,
                    subfunctions_in_same_isolate,
                    deployment: deployment.clone(),
                    udf_type,
                    function_metadata: function_metadata.clone(),
                    function_started_sender: None,
                    trace_host_operations: true,
                };
                let primary_request = DatabaseLaneRequest {
                    key_broker,
                    index_reader,
                    convex_origin,
                    bootstrap_metadata,
                    table_count_snapshot,
                    text_index_snapshot,
                    identity,
                    existing_writes,
                    default_system_env_vars,
                    in_memory_index_last_modified,
                    context,
                    scheduler_dependency,
                    subfunctions_in_same_isolate,
                    deployment,
                    udf_type,
                    function_metadata,
                    function_started_sender,
                    trace_host_operations: true,
                };
                let primary_runner = self.clone();
                let shadow_runner = self.clone();
                let preexisting_write_ids: BTreeSet<_> = primary_request
                    .existing_writes
                    .updates
                    .iter()
                    .map(|update| update.id())
                    .collect();
                let primary_prestarted = self
                    .begin_database_lane_transaction(&primary_request, false)
                    .await?;
                let mut shadow_prestarted = self
                    .begin_database_lane_transaction(&shadow_request, true)
                    .await?;
                shadow_prestarted
                    .transaction
                    .apply_document_creation_state(
                        &primary_prestarted.transaction.document_creation_state(),
                    )?;
                let (primary, shadow) = tokio::try_join!(
                    async move {
                        primary_runner
                            .run_database_lane(
                                primary_request,
                                inputs,
                                None,
                                DatabaseLaneMode::Primary,
                                Some(primary_prestarted),
                            )
                            .await
                    },
                    async move {
                        shadow_runner
                            .run_database_lane(
                                shadow_request,
                                inputs,
                                Some(static_hermes_route),
                                DatabaseLaneMode::WasmShadow(None, None),
                                Some(shadow_prestarted),
                            )
                            .await
                    },
                )?;
                let (transaction, outcome, usage_stats) = primary;
                let (shadow_transaction, shadow_outcome, _) = shadow;
                compare_mutation_shadow_for_test(
                    MutationShadowTestObservation::new(
                        &transaction,
                        &outcome,
                        &preexisting_write_ids,
                    )?,
                    MutationShadowTestObservation::new(
                        &shadow_transaction,
                        &shadow_outcome,
                        &preexisting_write_ids,
                    )?,
                )?;
                return Ok(FunctionExecutionResult::Completed {
                    transaction: Some(transaction),
                    outcome,
                    usage_stats,
                });
            },
        };
        #[cfg(not(feature = "static-hermes-wasmtime-gate"))]
        match execution_mode {
            FunctionExecutionMode::Configured
            | FunctionExecutionMode::V8
            | FunctionExecutionMode::V8WithHostOperationTrace => {},
            FunctionExecutionMode::V8WithStaticHermesWasmShadow(_) => {
                anyhow::bail!(
                    "Static Hermes production shadow requires static-hermes-wasmtime-gate"
                );
            },
            FunctionExecutionMode::StaticHermesWasmWithV8Shadow(_) => {
                anyhow::bail!(
                    "Static Hermes Wasm-primary verification requires static-hermes-wasmtime-gate"
                );
            },
            #[cfg(feature = "testing")]
            FunctionExecutionMode::V8WithStaticHermesWasmMutationShadowForTest { .. } => {
                anyhow::bail!(
                    "test-only Static Hermes mutation shadow requires static-hermes-wasmtime-gate"
                );
            },
            FunctionExecutionMode::StaticHermesWasm
            | FunctionExecutionMode::StaticHermesWasmWithHostOperationTrace => {
                return Ok(FunctionExecutionResult::StaticHermesWasmUnavailable);
            },
        }
        let deployment_name = deployment.name.clone();
        let usage_tracker = FunctionUsageTracker::new();
        let mut transaction = self
            .index_cache
            .begin_tx(
                identity.clone(),
                existing_writes,
                index_reader,
                deployment_name.clone(),
                in_memory_index_last_modified,
                bootstrap_metadata,
                table_count_snapshot,
                text_index_snapshot,
                usage_tracker.clone(),
            )
            .await?;
        #[cfg(feature = "static-hermes-wasmtime-gate")]
        let static_hermes_route = match static_hermes_route {
            Some(route) => {
                IsolateClient::<RT>::select_static_hermes_wasmtime_route_for_invocation(
                    route,
                    udf_type,
                    &function_metadata
                        .as_ref()
                        .context("Missing function metadata for Wasm route selection")?
                        .path_and_args(),
                    &mut transaction,
                )
                .await?
            },
            None => None,
        };
        let storage = self
            .storage
            .storage_for_deployment(&mut transaction, StorageUseCase::Files)
            .await?;
        let file_storage = TransactionalFileStorage::new(self.rt.clone(), storage, convex_origin);
        let modules_storage = self
            .storage
            .storage_for_deployment(&mut transaction, StorageUseCase::Modules)
            .await?;

        #[cfg(feature = "static-hermes-wasmtime-gate")]
        let mut environment_data = EnvironmentData {
            key_broker,
            default_system_env_vars,
            file_storage,
            module_loader: Arc::new(FunctionRunnerModuleLoader {
                deployment_name: deployment_name.clone(),
                cache: self.module_cache.clone(),
                code_cache: self.code_cache.clone(),
                modules_storage,
            }),
            deployment,
            host_secret_values: Some(BTreeMap::new()),
        };
        #[cfg(not(feature = "static-hermes-wasmtime-gate"))]
        let environment_data = EnvironmentData {
            key_broker,
            default_system_env_vars,
            file_storage,
            module_loader: Arc::new(FunctionRunnerModuleLoader {
                deployment_name: deployment_name.clone(),
                cache: self.module_cache.clone(),
                code_cache: self.code_cache.clone(),
                modules_storage,
            }),
            deployment,
        };

        let execution: anyhow::Result<(
            Option<FunctionFinalTransaction>,
            FunctionOutcome,
            FunctionUsageStats,
        )> = match udf_type {
            UdfType::Query | UdfType::Mutation => {
                anyhow::ensure!(
                    function_execution_start.is_none(),
                    "Database functions cannot have an action execution start barrier"
                );
                let function_metadata =
                    function_metadata.context("Missing function metadata for query or mutation")?;
                let (path_and_args, journal, active_javascript_class) = match udf_type {
                    UdfType::Query => match function_metadata {
                        FunctionMetadata::Query {
                            path_and_args,
                            journal,
                            active_javascript_class,
                        } => (path_and_args, journal, active_javascript_class),
                        FunctionMetadata::Mutation { .. } | FunctionMetadata::Action { .. } => {
                            anyhow::bail!("Function metadata does not match {udf_type}")
                        },
                    },
                    UdfType::Mutation => match function_metadata {
                        FunctionMetadata::Mutation {
                            path_and_args,
                            journal,
                        } => (path_and_args, journal, ActiveJavascriptClass::Protected),
                        FunctionMetadata::Query { .. } | FunctionMetadata::Action { .. } => {
                            anyhow::bail!("Function metadata does not match {udf_type}")
                        },
                    },
                    UdfType::Action | UdfType::HttpAction => {
                        unreachable!("outer match restricts this arm to queries and mutations")
                    },
                };
                // Initialize the UDF's RNG from some high-quality entropy. As with
                // `unix_timestamp` below, the UDF is only deterministic modulo this
                // system-generated input.
                let rng_seed = self.rt.rng().random();
                let unix_timestamp = udf_unix_timestamp(transaction.next_creation_time());
                #[cfg(feature = "static-hermes-wasmtime-gate")]
                if let Some(static_hermes_route) = static_hermes_route {
                    let mut prepared =
                        IsolateClient::<RT>::prepare_static_hermes_wasmtime_invocation(
                            static_hermes_route,
                            udf_type,
                            path_and_args,
                            transaction,
                            StaticHermesWasmtimePreparationMode::Primary,
                        )
                        .await?;
                    let host_secret_values = prepared.load_authorized_host_secret_values().await?;
                    let destination = environment_data
                        .host_secret_values
                        .as_mut()
                        .expect("top-level function lost its host-secret state");
                    assert!(
                        destination.is_empty(),
                        "top-level function received pre-populated host-secret state"
                    );
                    *destination = host_secret_values;
                    let (tx, outcome) = self
                        .isolate_client
                        .execute_static_hermes_wasmtime_gate(
                            prepared,
                            deployment_name,
                            udf_type,
                            journal,
                            context,
                            environment_data,
                            rng_seed,
                            unix_timestamp,
                            function_started_sender,
                            trace_host_operations,
                            None,
                            trace_host_operations,
                        )
                        .await?;
                    return Ok(FunctionExecutionResult::Completed {
                        transaction: Some(tx.try_into()?),
                        outcome,
                        usage_stats: usage_tracker.gather_user_stats(),
                    });
                }
                #[cfg(feature = "static-hermes-wasmtime-gate")]
                if static_hermes_wasm_required {
                    return Ok(FunctionExecutionResult::StaticHermesWasmUnavailable);
                }
                let (tx, outcome) = self
                    .isolate_client
                    .execute_udf(
                        udf_type,
                        path_and_args,
                        transaction,
                        journal,
                        context,
                        environment_data,
                        rng_seed,
                        unix_timestamp,
                        0,
                        deployment_name,
                        function_started_sender,
                        subfunctions_in_same_isolate,
                        trace_host_operations,
                        #[cfg(feature = "static-hermes-wasmtime-gate")]
                        None,
                        scheduler_dependency,
                        active_javascript_class,
                    )
                    .await?;
                Ok((
                    Some(tx.try_into()?),
                    outcome,
                    usage_tracker.gather_user_stats(),
                ))
            },
            UdfType::Action => {
                let path_and_args =
                    match function_metadata.context("Missing function metadata for action")? {
                        FunctionMetadata::Action { path_and_args } => path_and_args,
                        FunctionMetadata::Query { .. } | FunctionMetadata::Mutation { .. } => {
                            anyhow::bail!("Function metadata does not match {udf_type}")
                        },
                    };
                let log_line_sender =
                    log_line_sender.context("Missing log line sender for action")?;
                let function_execution_start =
                    function_execution_start.map(FunctionExecutionStartGate::into_channels);
                let outcome = self
                    .isolate_client
                    .execute_action(
                        path_and_args,
                        transaction,
                        action_callbacks,
                        fetch_client,
                        log_line_sender,
                        context,
                        environment_data,
                        deployment_name,
                        function_started_sender,
                        function_execution_start,
                        scheduler_dependency,
                    )
                    .await?;
                Ok((
                    None,
                    FunctionOutcome::Action(outcome),
                    usage_tracker.gather_user_stats(),
                ))
            },
            UdfType::HttpAction => {
                anyhow::ensure!(
                    function_execution_start.is_none(),
                    "HTTP actions cannot have a scheduled action execution start barrier"
                );
                anyhow::ensure!(
                    scheduler_dependency == SchedulerDependencyClass::Independent,
                    "HTTP actions cannot be dependency-unblocking scheduler requests"
                );
                let HttpActionMetadata {
                    http_response_streamer,
                    http_module_path,
                    routed_path,
                    http_request,
                } = http_action_metadata.context("Missing http action metadata")?;
                let log_line_sender =
                    log_line_sender.context("Missing log line sender for http action")?;
                // Set the proper identity for component HTTP actions. Note that for HTTP,
                // the component is both the caller and the callee.
                let component_id = http_module_path.path().component;
                let identity =
                    propagate_component_auth(&identity, component_id, component_id.is_root());
                let outcome = self
                    .isolate_client
                    .execute_http_action(
                        http_module_path,
                        routed_path,
                        http_request,
                        identity,
                        action_callbacks,
                        fetch_client,
                        log_line_sender,
                        http_response_streamer,
                        transaction,
                        context,
                        environment_data,
                        deployment_name,
                        function_started_sender,
                    )
                    .await?;
                Ok((
                    None,
                    FunctionOutcome::HttpAction(outcome),
                    usage_tracker.gather_user_stats(),
                ))
            },
        };
        let (transaction, outcome, usage_stats) = execution?;
        Ok(FunctionExecutionResult::Completed {
            transaction,
            outcome,
            usage_stats,
        })
    }

    pub async fn analyze(
        &self,
        udf_config: UdfConfig,
        modules: BTreeMap<CanonicalizedModulePath, ModuleConfig>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        deployment_name: String,
        query_analysis_admission: Option<QueryAnalysisAdmission>,
    ) -> anyhow::Result<Result<BTreeMap<CanonicalizedModulePath, AnalyzedModule>, JsError>> {
        anyhow::ensure!(
            modules
                .values()
                .all(|m| m.environment == ModuleEnvironment::Isolate),
            "Can only analyze Isolate modules"
        );

        self.isolate_client
            .analyze(
                udf_config,
                modules,
                environment_variables,
                deployment_name,
                query_analysis_admission,
            )
            .await
    }

    #[fastrace::trace]
    pub async fn evaluate_app_definitions(
        &self,
        app_definition: ModuleConfig,
        component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
        deployment_name: String,
    ) -> anyhow::Result<EvaluateAppDefinitionsResult> {
        anyhow::ensure!(
            app_definition.environment == ModuleEnvironment::Isolate,
            "Can only evaluate Isolate modules"
        );
        anyhow::ensure!(
            component_definitions
                .values()
                .all(|m| m.environment == ModuleEnvironment::Isolate),
            "Can only evaluate Isolate modules"
        );

        self.isolate_client
            .evaluate_app_definitions(
                app_definition,
                component_definitions,
                dependency_graph,
                user_environment_variables,
                system_env_vars,
                deployment_name,
            )
            .await
    }

    #[fastrace::trace]
    pub async fn evaluate_component_initializer(
        &self,
        evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        path: ComponentDefinitionPath,
        definition: ModuleConfig,
        args: BTreeMap<Identifier, Resource>,
        name: ComponentName,
        deployment_name: String,
    ) -> anyhow::Result<BTreeMap<Identifier, Resource>> {
        self.isolate_client
            .evaluate_component_initializer(
                evaluated_definitions,
                path,
                definition,
                args,
                name,
                deployment_name,
            )
            .await
    }

    #[fastrace::trace]
    pub async fn evaluate_schema(
        &self,
        schema_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        rng_seed: [u8; 32],
        unix_timestamp: UnixTimestamp,
        deployment_name: String,
    ) -> anyhow::Result<DatabaseSchema> {
        self.isolate_client
            .evaluate_schema(
                schema_bundle,
                source_map,
                rng_seed,
                unix_timestamp,
                deployment_name,
            )
            .await
    }

    #[fastrace::trace]
    pub async fn evaluate_auth_config(
        &self,
        auth_config_bundle: ModuleSource,
        source_map: Option<SourceMap>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        explanation: &str,
        deployment_name: String,
    ) -> anyhow::Result<AuthConfig> {
        self.isolate_client
            .evaluate_auth_config(
                auth_config_bundle,
                source_map,
                environment_variables,
                explanation,
                deployment_name,
            )
            .await
    }
}
#[cfg(all(test, feature = "static-hermes-wasmtime-gate"))]
mod shadow_capacity_classification_tests {
    use anyhow::Context as _;
    use isolate::StaticHermesQueryShadowCapacityUnavailable;

    use super::{
        classify_shadow_lane_error,
        shadow_failure,
        QueryShadowFailureReason,
        QueryShadowFailureStage,
        ShadowCapacityError,
    };

    #[test]
    fn lane_stage_context_does_not_hide_shadow_capacity() {
        let module_load_error: anyhow::Error = StaticHermesQueryShadowCapacityUnavailable.into();
        let error = classify_shadow_lane_error(
            module_load_error.context(super::StaticHermesWasmModuleLoadingFailure),
            true,
            QueryShadowFailureStage::RouteAuthentication,
        );

        assert!(error.is::<ShadowCapacityError>());
        assert_eq!(
            shadow_failure(&error),
            (
                QueryShadowFailureStage::Unclassified,
                QueryShadowFailureReason::Unclassified,
                None,
                None,
            )
        );
    }
}

#[cfg(test)]
mod execution_mode_build_contract_tests {
    use std::{
        sync::Arc,
        time::Duration,
    };

    use common::types::UdfType;

    use super::validate_execution_mode_for_build;
    use crate::{
        FunctionExecutionMode,
        QueryShadowAdmission,
        QueryShadowPermit,
        QueryShadowRouteKey,
    };

    struct NeverAdmitShadow;

    impl QueryShadowAdmission for NeverAdmitShadow {
        fn shadow_timeout(&self) -> Duration {
            Duration::from_secs(1)
        }

        fn try_admit(
            &self,
            _udf_type: UdfType,
            _route_key: &QueryShadowRouteKey,
        ) -> Option<QueryShadowPermit> {
            None
        }
    }

    fn production_shadow_mode() -> FunctionExecutionMode {
        FunctionExecutionMode::V8WithStaticHermesWasmShadow(Arc::new(NeverAdmitShadow))
    }

    #[cfg(not(feature = "static-hermes-wasmtime-gate"))]
    #[test]
    fn production_shadow_requires_static_hermes_wasmtime_gate() {
        let error = validate_execution_mode_for_build(&production_shadow_mode()).unwrap_err();

        assert_eq!(
            error.to_string(),
            "Static Hermes production shadow requires static-hermes-wasmtime-gate"
        );
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn production_shadow_is_accepted_with_static_hermes_wasmtime_gate() -> anyhow::Result<()> {
        validate_execution_mode_for_build(&production_shadow_mode())
    }
}

#[cfg(all(test, feature = "static-hermes-wasmtime-gate"))]
mod query_shadow_lane_contract_tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{
                AtomicBool,
                AtomicUsize,
                Ordering,
            },
            Arc,
        },
        task::{
            Context,
            Poll,
        },
        time::{
            Duration,
            SystemTime,
        },
    };

    use anyhow::Context as _;
    use common::{
        components::{
            ExportPath,
            PublicFunctionPath,
        },
        identity::InertIdentity,
        pause::PauseClient,
        persistence::{
            NoopRetentionValidator,
            RetentionValidator,
        },
        query_journal::QueryJournal,
        runtime::{
            Runtime,
            SpawnHandle,
            UnixTimestamp,
        },
        types::{
            RepeatableTimestamp,
            UdfType,
        },
    };
    use database::{
        ReadSet,
        TransactionReadSize,
    };
    use futures::{
        future,
        FutureExt,
    };
    use rand::SeedableRng;
    use sync_types::{
        types::SerializedArgs,
        Timestamp,
    };
    use tokio::sync::{
        oneshot,
        Semaphore,
    };
    use udf::{
        FunctionOutcome,
        HostOperationTrace,
        SyscallTrace,
        UdfOutcome,
    };
    use value::{
        ConvexValue,
        JsonPackedValue,
        PendingValue,
    };

    use super::{
        await_shadow_lanes,
        await_v8_verifier_and_primary,
        classify_shadow_lane_error,
        compare_query_shadow,
        run_admitted_shadow_lane,
        shadow_failure,
        MutationShadowObservation,
        PrimaryShadowObservation,
        QueryShadowFailureReason,
        QueryShadowFailureStage,
        QueryShadowGeneratedExportDiagnostic,
        QueryShadowInvalidReason,
        QueryShadowObservation,
        QueryShadowPrimaryPublication,
        ShadowObservation,
        ShadowSnapshotValidation,
        StaticHermesGeneratedExportDiagnostic,
        StaticHermesWasmExecutionFailure,
        StaticHermesWasmModuleLoadingFailure,
        StaticHermesWasmTrapCode,
        StaticHermesWasmTrapDiagnostic,
        V8VerifierOutcome,
    };
    use crate::{
        FunctionFinalTransaction,
        FunctionReads,
        FunctionWrites,
        QueryShadowPermit,
        QueryShadowRouteKey,
        QueryShadowTerminal,
    };

    #[derive(Clone)]
    struct ImmediateDeadlineRuntime;

    fn valid_test_snapshot() -> ShadowSnapshotValidation {
        ShadowSnapshotValidation {
            timestamp: Timestamp::MIN,
            retention_validator: Arc::new(NoopRetentionValidator),
        }
    }

    struct ExpiringTestSnapshot {
        valid: AtomicBool,
        validations: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl RetentionValidator for ExpiringTestSnapshot {
        fn optimistic_validate_snapshot(&self, _ts: Timestamp) -> anyhow::Result<()> {
            anyhow::bail!("test expects validation after execution")
        }

        async fn validate_snapshot(&self, ts: Timestamp) -> anyhow::Result<()> {
            assert_eq!(ts, Timestamp::MIN);
            self.validations.fetch_add(1, Ordering::SeqCst);
            anyhow::ensure!(self.valid.load(Ordering::SeqCst), "test snapshot expired");
            Ok(())
        }

        async fn validate_document_snapshot(&self, _ts: Timestamp) -> anyhow::Result<()> {
            anyhow::bail!("test does not read the documents log")
        }

        async fn min_snapshot_ts(&self) -> anyhow::Result<RepeatableTimestamp> {
            anyhow::bail!("test expects validation of the admitted timestamp")
        }

        async fn min_document_snapshot_ts(&self) -> anyhow::Result<RepeatableTimestamp> {
            anyhow::bail!("test does not read the documents log")
        }
    }

    impl Runtime for ImmediateDeadlineRuntime {
        fn wait(
            &self,
            _duration: Duration,
        ) -> Pin<Box<dyn futures::future::FusedFuture<Output = ()> + Send + 'static>> {
            Box::pin(future::ready(()).fuse())
        }

        fn spawn(
            &self,
            _name: &'static str,
            _f: impl Future<Output = ()> + Send + 'static,
        ) -> Box<dyn SpawnHandle> {
            panic!("ImmediateDeadlineRuntime::spawn is not used by these tests")
        }

        fn spawn_thread<Fut: Future<Output = ()>, F: FnOnce() -> Fut + Send + 'static>(
            &self,
            _name: &str,
            _f: F,
        ) -> Box<dyn SpawnHandle> {
            panic!("ImmediateDeadlineRuntime::spawn_thread is not used by these tests")
        }

        fn system_time(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH
        }

        fn monotonic_now(&self) -> tokio::time::Instant {
            tokio::time::Instant::now()
        }

        fn rng(&self) -> Box<dyn rand::RngCore> {
            Box::new(rand::rngs::StdRng::seed_from_u64(0))
        }

        fn pause_client(&self) -> PauseClient {
            PauseClient::new()
        }
    }

    struct PendingShadow {
        started: Option<oneshot::Sender<()>>,
        cancelled: Option<oneshot::Sender<()>>,
    }

    impl Future for PendingShadow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
            }
            Poll::Pending
        }
    }

    impl Drop for PendingShadow {
        fn drop(&mut self) {
            if let Some(cancelled) = self.cancelled.take() {
                let _ = cancelled.send(());
            }
        }
    }

    fn query_transaction(
        begin_timestamp: Timestamp,
        capture_handler_reads: bool,
    ) -> FunctionFinalTransaction {
        let reads = || FunctionReads {
            reads: ReadSet::empty(),
            num_intervals: 0,
            user_tx_size: TransactionReadSize::default(),
            system_tx_size: TransactionReadSize::default(),
        };
        FunctionFinalTransaction {
            begin_timestamp,
            reads: reads(),
            handler_reads: capture_handler_reads.then(reads),
            writes: FunctionWrites::default(),
            rows_read_by_tablet: Default::default(),
            table_mapping: value::TableMapping::new(),
        }
    }

    fn query_outcome() -> anyhow::Result<FunctionOutcome> {
        Ok(FunctionOutcome::Query(UdfOutcome {
            path: PublicFunctionPath::RootExport("test:default".parse::<ExportPath>()?)
                .debug_into_component_path(),
            arguments: SerializedArgs::from_args(vec![])?,
            identity: InertIdentity::Unknown,
            observed_identity: false,
            rng_seed: [0; 32],
            observed_rng: false,
            unix_timestamp: UnixTimestamp::from_nanos(0),
            observed_time: false,
            log_lines: vec![].into(),
            journal: QueryJournal::new(),
            audit_log_lines: vec![].into(),
            result: Ok(JsonPackedValue::pack(PendingValue::Concrete(
                ConvexValue::Null,
            ))),
            host_operation_error: None,
            host_operation_trace: HostOperationTrace::from(vec![]),
            syscall_trace: SyscallTrace::new(),
            udf_server_version: None,
            memory_in_mb: 0,
            user_execution_time: Some(Duration::ZERO),
        }))
    }

    fn query_primary_observation() -> anyhow::Result<PrimaryShadowObservation> {
        Ok(PrimaryShadowObservation::from_execution(
            UdfType::Query,
            &query_transaction(Timestamp::MIN, true),
            &query_outcome()?,
            Duration::ZERO,
            &Default::default(),
        ))
    }

    #[test]
    fn missing_primary_handler_reads_are_an_invalid_primary_terminal() -> anyhow::Result<()> {
        let transaction = query_transaction(Timestamp::MIN, false);
        let outcome = query_outcome()?;

        assert!(QueryShadowObservation::new(&transaction, &outcome).is_err());
        assert!(matches!(
            PrimaryShadowObservation::from_execution(
                UdfType::Query,
                &transaction,
                &outcome,
                Duration::ZERO,
                &Default::default(),
            ),
            PrimaryShadowObservation::Terminal(QueryShadowTerminal::InvalidPrimary),
        ));
        Ok(())
    }

    #[test]
    fn rejected_shadow_observations_preserve_fixed_causes() -> anyhow::Result<()> {
        let query = query_outcome()?;
        let FunctionOutcome::Query(outcome) = query.clone() else {
            unreachable!("query fixture returned another UDF type");
        };
        let mutation = FunctionOutcome::Mutation(outcome);
        for owned in [false, true] {
            let transaction = query_transaction(Timestamp::MIN, false);
            let result = if owned {
                QueryShadowObservation::from_owned(transaction, query.clone())
            } else {
                QueryShadowObservation::new(&transaction, &query)
            };
            assert!(matches!(
                result,
                Err(QueryShadowInvalidReason::MissingHandlerReads)
            ));
            let transaction = query_transaction(Timestamp::MIN, true);
            let result = if owned {
                QueryShadowObservation::from_owned(transaction, mutation.clone())
            } else {
                QueryShadowObservation::new(&transaction, &mutation)
            };
            assert!(matches!(result, Err(QueryShadowInvalidReason::OutcomeType)));
            let transaction = query_transaction(Timestamp::MIN, false);
            let result = if owned {
                MutationShadowObservation::from_owned(
                    transaction,
                    mutation.clone(),
                    &Default::default(),
                )
            } else {
                MutationShadowObservation::new(&transaction, &mutation, &Default::default())
            };
            assert!(matches!(
                result,
                Err(QueryShadowInvalidReason::MissingHandlerReads)
            ));
            let mut incomplete = mutation.clone();
            let FunctionOutcome::Mutation(outcome) = &mut incomplete else {
                unreachable!("mutation fixture returned another UDF type");
            };
            outcome.host_operation_trace = HostOperationTrace::default();
            let transaction = query_transaction(Timestamp::MIN, true);
            let result = if owned {
                MutationShadowObservation::from_owned(transaction, incomplete, &Default::default())
            } else {
                MutationShadowObservation::new(&transaction, &incomplete, &Default::default())
            };
            assert!(matches!(
                result,
                Err(QueryShadowInvalidReason::IncompleteHostTrace)
            ));
        }
        Ok(())
    }

    #[test]
    fn production_query_shadow_comparison_uses_final_transaction_snapshot() -> anyhow::Result<()> {
        let outcome = query_outcome()?;
        let primary_transaction = query_transaction(Timestamp::MIN, true);
        let same_snapshot_transaction = query_transaction(Timestamp::MIN, true);
        let different_snapshot_transaction = query_transaction(Timestamp::MIN.succ()?, true);

        let comparison = compare_query_shadow(
            QueryShadowObservation::new(&primary_transaction, &outcome)?,
            QueryShadowObservation::new(&same_snapshot_transaction, &outcome)?,
        );
        assert!(comparison.snapshot_matches);
        assert!(comparison.matches());

        let comparison = compare_query_shadow(
            QueryShadowObservation::new(&primary_transaction, &outcome)?,
            QueryShadowObservation::new(&different_snapshot_transaction, &outcome)?,
        );
        assert_eq!(
            comparison,
            crate::QueryShadowComparison {
                snapshot_matches: false,
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
                inserted_document_identity:
                    crate::QueryShadowInsertedDocumentIdentity::NotApplicable,
                host_operation_trace_diagnostic: None,
                result_diagnostic: None,
            }
        );
        assert!(!comparison.matches());
        Ok(())
    }

    #[test]
    fn consuming_shadow_observation_preserves_query_comparison_evidence() -> anyhow::Result<()> {
        let borrowed = QueryShadowObservation::new(
            &query_transaction(Timestamp::MIN, true),
            &query_outcome()?,
        )?;
        let consumed = QueryShadowObservation::from_owned(
            query_transaction(Timestamp::MIN, true),
            query_outcome()?,
        )?;

        assert!(compare_query_shadow(borrowed, consumed).matches());
        Ok(())
    }

    #[test]
    fn primary_shadow_publication_waits_for_retention_validation() {
        let publication =
            QueryShadowPrimaryPublication::new(Timestamp::MIN, Arc::new(NoopRetentionValidator));
        let (sender, mut receiver) = oneshot::channel();
        publication.arm(sender);
        publication.record_primary(PrimaryShadowObservation::Terminal(
            QueryShadowTerminal::InvalidPrimary,
        ));

        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        publication.publish_after_retention_validation();
        assert!(matches!(
            receiver.try_recv(),
            Ok(PrimaryShadowObservation::Terminal(
                QueryShadowTerminal::InvalidPrimary
            ))
        ));
    }

    #[test]
    fn primary_shadow_publication_rejects_failed_retention_validation() {
        let publication =
            QueryShadowPrimaryPublication::new(Timestamp::MIN, Arc::new(NoopRetentionValidator));
        let (sender, mut receiver) = oneshot::channel();
        publication.arm(sender);
        publication.record_primary(PrimaryShadowObservation::Terminal(
            QueryShadowTerminal::InvalidPrimary,
        ));

        publication.reject_after_retention_validation();
        assert!(matches!(
            receiver.try_recv(),
            Ok(PrimaryShadowObservation::Terminal(
                QueryShadowTerminal::PrimarySnapshotRejected
            ))
        ));
    }

    #[test]
    fn shadow_lane_failures_retain_each_real_stage_boundary() {
        for stage in [
            QueryShadowFailureStage::TransactionSetup,
            QueryShadowFailureStage::EnvironmentSetup,
            QueryShadowFailureStage::RouteAuthentication,
            QueryShadowFailureStage::ModuleLoading,
            QueryShadowFailureStage::WasmExecution,
            QueryShadowFailureStage::VerifierExecution,
            QueryShadowFailureStage::TransactionFinalization,
        ] {
            let shadow_error = classify_shadow_lane_error(
                anyhow::anyhow!("test failure").context("outer test context"),
                true,
                stage,
            )
            .context("later test context");
            assert_eq!(shadow_failure(&shadow_error).0, stage);
        }

        let module_load_error = anyhow::anyhow!("test module load failure")
            .context(StaticHermesWasmModuleLoadingFailure);
        let classified_module_load_error = classify_shadow_lane_error(
            module_load_error,
            true,
            QueryShadowFailureStage::WasmExecution,
        );
        assert_eq!(
            shadow_failure(&classified_module_load_error),
            (
                QueryShadowFailureStage::ModuleLoading,
                QueryShadowFailureReason::ModuleLoading,
                None,
                None,
            )
        );

        let primary_error = classify_shadow_lane_error(
            anyhow::anyhow!("test failure"),
            false,
            QueryShadowFailureStage::WasmExecution,
        );
        assert_eq!(
            shadow_failure(&primary_error),
            (
                QueryShadowFailureStage::Unclassified,
                QueryShadowFailureReason::Unclassified,
                None,
                None,
            )
        );
    }

    #[test]
    fn shadow_lane_failures_retain_data_free_execution_subphases() {
        let trap_diagnostic = StaticHermesWasmTrapDiagnostic::new(
            StaticHermesWasmTrapCode::UnreachableCodeReached,
            Some(3),
            Some(5),
        );
        for (failure, expected_reason) in [
            (
                StaticHermesWasmExecutionFailure::InitializationTimeout,
                QueryShadowFailureReason::InitializationTimeout,
            ),
            (
                StaticHermesWasmExecutionFailure::ExecutionTimeout,
                QueryShadowFailureReason::ExecutionTimeout,
            ),
            (
                StaticHermesWasmExecutionFailure::InstructionBudget,
                QueryShadowFailureReason::InstructionBudget,
            ),
            (
                StaticHermesWasmExecutionFailure::RuntimeInitialization,
                QueryShadowFailureReason::RuntimeInitialization,
            ),
            (
                StaticHermesWasmExecutionFailure::GeneratedExportDispatch,
                QueryShadowFailureReason::GeneratedExportDispatch,
            ),
            (
                StaticHermesWasmExecutionFailure::GuestMemoryLimit,
                QueryShadowFailureReason::GuestMemoryLimit,
            ),
            (
                StaticHermesWasmExecutionFailure::HostOwnedBytesLimit,
                QueryShadowFailureReason::HostOwnedBytesLimit,
            ),
            (
                StaticHermesWasmExecutionFailure::OpaqueHandleLimit,
                QueryShadowFailureReason::OpaqueHandleLimit,
            ),
            (
                StaticHermesWasmExecutionFailure::OpaqueHandleSpaceExhausted,
                QueryShadowFailureReason::OpaqueHandleSpaceExhausted,
            ),
            (
                StaticHermesWasmExecutionFailure::AggregateMemoryLimit,
                QueryShadowFailureReason::AggregateMemoryLimit,
            ),
            (
                StaticHermesWasmExecutionFailure::OperationLimit,
                QueryShadowFailureReason::OperationLimit,
            ),
            (
                StaticHermesWasmExecutionFailure::HostAbiInvariant,
                QueryShadowFailureReason::HostAbiInvariant,
            ),
            (
                StaticHermesWasmExecutionFailure::WasmtimeTrap {
                    diagnostic: trap_diagnostic.clone(),
                },
                QueryShadowFailureReason::WasmtimeTrap,
            ),
            (
                StaticHermesWasmExecutionFailure::GuestExecution,
                QueryShadowFailureReason::GuestExecution,
            ),
            (
                StaticHermesWasmExecutionFailure::ResultFinalization,
                QueryShadowFailureReason::ResultFinalization,
            ),
            (
                StaticHermesWasmExecutionFailure::RuntimeCleanup,
                QueryShadowFailureReason::RuntimeCleanup,
            ),
        ] {
            let shadow_error = classify_shadow_lane_error(
                anyhow::anyhow!("private failure details").context(failure),
                true,
                QueryShadowFailureStage::WasmExecution,
            );
            assert_eq!(
                shadow_failure(&shadow_error),
                (
                    QueryShadowFailureStage::WasmExecution,
                    expected_reason,
                    None,
                    (expected_reason == QueryShadowFailureReason::WasmtimeTrap)
                        .then(|| trap_diagnostic.clone()),
                )
            );
        }
    }

    #[test]
    fn generated_export_failure_diagnostics_survive_lane_classification() {
        let cases = [
            (
                StaticHermesGeneratedExportDiagnostic::FirstSelectorTrap,
                QueryShadowGeneratedExportDiagnostic::FirstSelectorTrap,
            ),
            (
                StaticHermesGeneratedExportDiagnostic::FirstSelectorRejected { status: 64 },
                QueryShadowGeneratedExportDiagnostic::FirstSelectorRejected { status: 64 },
            ),
            (
                StaticHermesGeneratedExportDiagnostic::SelectedEntryPreparationTrap,
                QueryShadowGeneratedExportDiagnostic::SelectedEntryPreparationTrap,
            ),
            (
                StaticHermesGeneratedExportDiagnostic::SelectedEntryPreparationRejected {
                    status: 65,
                },
                QueryShadowGeneratedExportDiagnostic::SelectedEntryPreparationRejected {
                    status: 65,
                },
            ),
            (
                StaticHermesGeneratedExportDiagnostic::SecondSelectorTrap,
                QueryShadowGeneratedExportDiagnostic::SecondSelectorTrap,
            ),
            (
                StaticHermesGeneratedExportDiagnostic::SecondSelectorRejected { status: 66 },
                QueryShadowGeneratedExportDiagnostic::SecondSelectorRejected { status: 66 },
            ),
        ];

        for (backend_diagnostic, expected_diagnostic) in cases {
            let error = classify_shadow_lane_error(
                anyhow::anyhow!("private generated-export failure").context(backend_diagnostic),
                true,
                QueryShadowFailureStage::WasmExecution,
            );

            assert_eq!(
                shadow_failure(&error),
                (
                    QueryShadowFailureStage::WasmExecution,
                    QueryShadowFailureReason::Unclassified,
                    Some(expected_diagnostic),
                    None,
                )
            );
        }
    }

    #[test]
    fn v8_verifier_execution_has_fixed_failure_attribution() {
        let verifier_error = classify_shadow_lane_error(
            anyhow::anyhow!("private verifier failure details"),
            true,
            QueryShadowFailureStage::VerifierExecution,
        );

        assert_eq!(
            shadow_failure(&verifier_error),
            (
                QueryShadowFailureStage::VerifierExecution,
                QueryShadowFailureReason::VerifierExecution,
                None,
                None,
            )
        );
    }

    #[tokio::test]
    async fn query_shadow_lane_contract_preserves_v8_authority() -> anyhow::Result<()> {
        let (primary_sender, mut primary_receiver) = oneshot::channel();
        let (shadow_started_sender, shadow_started_receiver) = oneshot::channel();
        let (shadow_release_sender, shadow_release_receiver) = oneshot::channel();
        let paired_lanes = tokio::spawn(async move {
            await_shadow_lanes(
                &mut primary_receiver,
                async move {
                    shadow_started_sender
                        .send(())
                        .expect("shadow start receiver was dropped");
                    shadow_release_receiver
                        .await
                        .expect("shadow release sender was dropped");
                    "wasm"
                },
                &valid_test_snapshot(),
            )
            .await
        });

        // The shadow starts before V8 finishes, but does not delay the V8 result.
        shadow_started_receiver.await?;
        assert!(primary_sender.send(query_primary_observation()?).is_ok());
        tokio::task::yield_now().await;
        assert!(!paired_lanes.is_finished());
        shadow_release_sender
            .send(())
            .expect("shadow release receiver was dropped");
        let Some(((primary, duration), shadow)) = paired_lanes
            .await?
            .expect("valid primary observation was rejected")
        else {
            anyhow::bail!("primary observation sender was cancelled");
        };
        assert!(matches!(primary, ShadowObservation::Query(_)));
        assert_eq!(duration, Duration::ZERO);
        assert_eq!(shadow, "wasm");

        let (primary_sender, mut primary_receiver) = oneshot::channel::<PrimaryShadowObservation>();
        let (shadow_started_sender, shadow_started_receiver) = oneshot::channel();
        let (shadow_cancelled_sender, shadow_cancelled_receiver) = oneshot::channel();
        let cancelled_primary = tokio::spawn(async move {
            await_shadow_lanes(
                &mut primary_receiver,
                PendingShadow {
                    started: Some(shadow_started_sender),
                    cancelled: Some(shadow_cancelled_sender),
                },
                &valid_test_snapshot(),
            )
            .await
        });
        shadow_started_receiver.await?;
        drop(primary_sender);
        assert!(matches!(cancelled_primary.await?, Ok(None)));
        shadow_cancelled_receiver.await?;

        let (primary_sender, mut primary_receiver) = oneshot::channel();
        let shadow_failure = tokio::spawn(async move {
            await_shadow_lanes(
                &mut primary_receiver,
                async { Err::<(), _>("failed") },
                &valid_test_snapshot(),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!shadow_failure.is_finished());
        assert!(primary_sender.send(query_primary_observation()?).is_ok());
        let Some(((_, duration), shadow)) = shadow_failure
            .await?
            .expect("valid primary observation was rejected")
        else {
            anyhow::bail!("primary observation sender was cancelled");
        };
        assert_eq!(duration, Duration::ZERO);
        assert_eq!(shadow, Err("failed"));

        let (primary_sender, mut primary_receiver) = oneshot::channel();
        let (shadow_started_sender, shadow_started_receiver) = oneshot::channel();
        let (shadow_cancelled_sender, shadow_cancelled_receiver) = oneshot::channel();
        let invalid_primary = tokio::spawn(async move {
            await_shadow_lanes(
                &mut primary_receiver,
                PendingShadow {
                    started: Some(shadow_started_sender),
                    cancelled: Some(shadow_cancelled_sender),
                },
                &valid_test_snapshot(),
            )
            .await
        });
        shadow_started_receiver.await?;
        assert!(primary_sender
            .send(PrimaryShadowObservation::Terminal(
                QueryShadowTerminal::InvalidPrimary,
            ))
            .is_ok());
        assert!(matches!(
            invalid_primary.await?,
            Err(QueryShadowTerminal::InvalidPrimary)
        ));
        shadow_cancelled_receiver.await?;

        let publication =
            QueryShadowPrimaryPublication::new(Timestamp::MIN, Arc::new(NoopRetentionValidator));
        let snapshot_validation = publication.0.snapshot_validation.clone();
        let (primary_sender, mut primary_receiver) = oneshot::channel();
        publication.arm(primary_sender);
        let (shadow_started_sender, shadow_started_receiver) = oneshot::channel();
        let (shadow_cancelled_sender, shadow_cancelled_receiver) = oneshot::channel();
        let cancelled_publication = tokio::spawn(async move {
            await_shadow_lanes(
                &mut primary_receiver,
                PendingShadow {
                    started: Some(shadow_started_sender),
                    cancelled: Some(shadow_cancelled_sender),
                },
                &snapshot_validation,
            )
            .await
        });
        shadow_started_receiver.await?;
        drop(publication);
        assert!(matches!(cancelled_publication.await?, Ok(None)));
        shadow_cancelled_receiver.await?;

        Ok(())
    }

    #[tokio::test]
    async fn detached_shadows_validate_retention_after_execution() -> anyhow::Result<()> {
        for wasm_primary in [false, true] {
            for (expire_during_shadow, shadow_failed) in
                [(false, false), (true, false), (false, true), (true, true)]
            {
                let validator = Arc::new(ExpiringTestSnapshot {
                    valid: AtomicBool::new(true),
                    validations: AtomicUsize::new(0),
                });
                let publication =
                    QueryShadowPrimaryPublication::new(Timestamp::MIN, validator.clone());
                let snapshot_validation = publication.0.snapshot_validation.clone();
                let (primary_sender, mut primary_receiver) = oneshot::channel();
                publication.arm(primary_sender);
                publication.record_primary(query_primary_observation()?);
                let (started_sender, started_receiver) = oneshot::channel();
                let (release_sender, release_receiver) = oneshot::channel();
                let PrimaryShadowObservation::Observation {
                    observation,
                    duration,
                } = query_primary_observation()?
                else {
                    anyhow::bail!("test fixture did not produce an observation")
                };
                let pairing = tokio::spawn(async move {
                    let shadow = async move {
                        started_sender
                            .send(())
                            .expect("shadow start receiver dropped");
                        release_receiver
                            .await
                            .expect("shadow release sender dropped");
                        if shadow_failed {
                            V8VerifierOutcome::Terminal(QueryShadowTerminal::ShadowFailure {
                                stage: QueryShadowFailureStage::VerifierExecution,
                                reason: QueryShadowFailureReason::VerifierExecution,
                                generated_export_diagnostic: None,
                                wasmtime_trap_diagnostic: None,
                            })
                        } else {
                            V8VerifierOutcome::Observation {
                                observation,
                                duration,
                            }
                        }
                    };
                    if wasm_primary {
                        await_v8_verifier_and_primary(
                            &mut primary_receiver,
                            shadow,
                            &snapshot_validation,
                        )
                        .await
                        .map(|_| ())
                    } else {
                        await_shadow_lanes(&mut primary_receiver, shadow, &snapshot_validation)
                            .await
                            .and_then(|paired| match paired {
                                Some((_, V8VerifierOutcome::Terminal(terminal))) => Err(terminal),
                                Some((_, V8VerifierOutcome::Observation { .. })) => Ok(()),
                                None => Err(QueryShadowTerminal::PrimaryFailure),
                            })
                    }
                });

                validator.validate_snapshot(Timestamp::MIN).await?;
                publication.publish_after_retention_validation();
                drop(publication);
                started_receiver.await?;
                assert_eq!(validator.validations.load(Ordering::SeqCst), 1);
                assert!(!pairing.is_finished());
                validator
                    .valid
                    .store(!expire_during_shadow, Ordering::SeqCst);
                release_sender
                    .send(())
                    .expect("shadow release receiver dropped");
                let result = pairing.await?;
                if expire_during_shadow {
                    assert!(matches!(
                        result,
                        Err(QueryShadowTerminal::InvalidShadow {
                            reason: QueryShadowInvalidReason::SnapshotValidation,
                        })
                    ));
                } else if shadow_failed {
                    assert!(matches!(
                        result,
                        Err(QueryShadowTerminal::ShadowFailure { .. })
                    ));
                } else {
                    assert!(result.is_ok());
                }
                assert_eq!(validator.validations.load(Ordering::SeqCst), 2);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn wasm_primary_terminal_does_not_start_v8_verifier() -> anyhow::Result<()> {
        let (primary_sender, mut primary_receiver) = oneshot::channel();
        let (verifier_started_sender, mut verifier_started_receiver) = oneshot::channel();
        let pairing = tokio::spawn(async move {
            await_v8_verifier_and_primary(
                &mut primary_receiver,
                async move {
                    let _ = verifier_started_sender.send(());
                    unreachable!("pending V8 verifier completed")
                },
                &valid_test_snapshot(),
            )
            .await
        });

        tokio::task::yield_now().await;
        assert!(matches!(
            verifier_started_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(primary_sender
            .send(PrimaryShadowObservation::Terminal(
                QueryShadowTerminal::PrimarySnapshotRejected,
            ))
            .is_ok());
        assert!(matches!(
            pairing.await?,
            Err(QueryShadowTerminal::PrimarySnapshotRejected)
        ));
        assert!(matches!(
            verifier_started_receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn query_shadow_deadline_cancels_pending_setup_or_execution() -> anyhow::Result<()> {
        let (started_sender, started_receiver) = oneshot::channel();
        let (cancelled_sender, cancelled_receiver) = oneshot::channel();

        let result = run_admitted_shadow_lane(
            &ImmediateDeadlineRuntime,
            Duration::from_secs(1),
            async move {
                PendingShadow {
                    started: Some(started_sender),
                    cancelled: Some(cancelled_sender),
                }
                .await;
                Ok(())
            },
        )
        .await;

        assert!(result.is_err());
        started_receiver.await?;
        cancelled_receiver.await?;
        Ok(())
    }

    #[tokio::test]
    async fn query_shadow_deadline_ends_primary_wait_without_comparison() -> anyhow::Result<()> {
        let (_primary_sender, mut primary_receiver) =
            oneshot::channel::<PrimaryShadowObservation>();
        let comparison_attempted = Arc::new(AtomicBool::new(false));
        let comparison_attempted_for_lane = Arc::clone(&comparison_attempted);

        let result = run_admitted_shadow_lane(
            &ImmediateDeadlineRuntime,
            Duration::from_secs(1),
            async move {
                let _ =
                    await_shadow_lanes(&mut primary_receiver, async { () }, &valid_test_snapshot())
                        .await;
                comparison_attempted_for_lane.store(true, Ordering::Relaxed);
                Ok(())
            },
        )
        .await;

        assert!(result.is_err());
        assert!(!comparison_attempted.load(Ordering::Relaxed));
        Ok(())
    }

    #[tokio::test]
    async fn query_shadow_timeout_waits_for_cleanup() -> anyhow::Result<()> {
        let semaphore = Arc::new(Semaphore::new(1));
        let mut permit = QueryShadowPermit::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            UdfType::Query,
            QueryShadowRouteKey::new(&"1".repeat(64), &"a".repeat(64))?,
            Arc::new(|_| true),
            Arc::new(|| {}),
        );
        let work_guard = permit.work_guard();
        let (shadow_cancelled_sender, shadow_cancelled_receiver) = oneshot::channel();
        let (cancellation_observed_sender, cancellation_observed_receiver) = oneshot::channel();
        let (cleanup_release_sender, cleanup_release_receiver) = oneshot::channel();
        let cleanup = tokio::spawn(async move {
            shadow_cancelled_receiver.await?;
            cancellation_observed_sender.send(()).ok();
            cleanup_release_receiver.await?;
            drop(work_guard);
            Ok::<_, anyhow::Error>(())
        });

        let result = run_admitted_shadow_lane(
            &ImmediateDeadlineRuntime,
            Duration::from_secs(1),
            async move {
                PendingShadow {
                    started: None,
                    cancelled: Some(shadow_cancelled_sender),
                }
                .await;
                Ok(())
            },
        )
        .await;
        assert!(result.is_err());
        cancellation_observed_receiver.await?;

        permit.record_terminal(QueryShadowTerminal::Timeout);
        drop(permit);
        assert_eq!(semaphore.available_permits(), 0);

        cleanup_release_sender.send(()).ok();
        cleanup.await??;
        assert_eq!(semaphore.available_permits(), 1);
        Ok(())
    }
}
