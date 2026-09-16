#[cfg(feature = "static-hermes-wasmtime-gate")]
use application::function_log::{
    QueryShadowDiagnosticEvidence,
    QueryShadowDiagnosticOutcome,
    QueryShadowEvidencePage,
    QueryShadowEvidenceSummary,
    QueryShadowFailureReasonCounts,
    QueryShadowFailureStageCounts,
    QueryShadowInsertedDocumentIdentityCounts,
    QueryShadowMismatchCounts,
    QueryShadowRegistrySelection,
    QueryShadowRouteEvidence,
    QueryShadowTerminalCounts,
    QueryShadowTimingSummary,
};
use axum::response::IntoResponse;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use common::http::PaginationMetadata;
use common::{
    components::{
        ComponentFunctionPath,
        ComponentPath,
    },
    http::{
        extract::{
            Json,
            MtState,
            Query,
        },
        HttpResponseError,
    },
    types::{
        UdfIdentifier,
        UdfType,
    },
};
use errors::ErrorMetadata;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use function_runner::{
    QueryShadowComparison,
    QueryShadowDirection,
    QueryShadowErrorDiagnostic,
    QueryShadowGeneratedExportDiagnostic,
    QueryShadowHostOperationCountDifference,
    QueryShadowHostOperationTraceDiagnostic,
    QueryShadowResultDiagnostic,
    QueryShadowRouteKey,
    QueryShadowTerminal,
};
#[cfg(all(test, feature = "static-hermes-wasmtime-gate"))]
use isolate::StaticHermesWasmTrapCode;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use isolate::{
    IsolateClient,
    StaticHermesGeneratedMemoryRouteStatistics,
    StaticHermesGeneratedMemoryStatistics,
    StaticHermesWasmTrapDiagnostic,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use runtime::prod::ProdRuntime;
use serde::Deserialize;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use serde::Serialize;
use sync_types::UdfPath;
use value::{
    TableMapping,
    TabletId,
};

use crate::{
    authentication::ExtractIdentity,
    LocalAppState,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UdfRateQueryArgs {
    component_path: Option<String>,
    #[serde(alias = "path")]
    udf_path: String,
    metric: String,
    window: String,
    udf_type: Option<String>,
}

pub(crate) async fn udf_rate(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(UdfRateQueryArgs {
        component_path,
        udf_path,
        metric,
        window,
        udf_type,
    }): Query<UdfRateQueryArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let window_json: serde_json::Value =
        serde_json::from_str(&window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;
    let udf_identifier = parse_udf_identifier(udf_type, component_path, udf_path)?;

    let timeseries =
        st.application
            .metrics_log(&identity)?
            .udf_rate(udf_identifier, metric.parse()?, window)?;
    Ok(Json(timeseries))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopKQueryArgs {
    window: String,
    k: Option<usize>,
}

pub(crate) async fn failure_percentage_top_k(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(TopKQueryArgs { window, k }): Query<TopKQueryArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let window_json: serde_json::Value =
        serde_json::from_str(&window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;

    let k = validate_k(k)?;

    let timeseries = st
        .application
        .metrics_log(&identity)?
        .failure_percentage_top_k(window, k)?;
    Ok(Json(timeseries))
}

pub(crate) async fn cache_hit_percentage_top_k(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(TopKQueryArgs { window, k }): Query<TopKQueryArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let window_json: serde_json::Value =
        serde_json::from_str(&window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;

    let k = validate_k(k)?;

    let timeseries = st
        .application
        .metrics_log(&identity)?
        .cache_hit_percentage_top_k(window, k)?;
    Ok(Json(timeseries))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SubscriptionInvalidationsTopKArgs {
    component_path: Option<String>,
    #[serde(alias = "path")]
    udf_path: Option<String>,
    window: String,
    udf_type: Option<String>,
    k: Option<usize>,
}

pub(crate) async fn subscription_invalidations_top_k(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(args): Query<SubscriptionInvalidationsTopKArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let window_json: serde_json::Value =
        serde_json::from_str(&args.window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;
    let k = validate_k(args.k)?;
    let table_mapping = st.application.latest_snapshot()?.table_mapping().clone();

    let udf_identifier = args
        .udf_path
        .map(|path| parse_udf_identifier(args.udf_type, args.component_path, path))
        .transpose()?;

    let timeseries = st
        .application
        .metrics_log(&identity)?
        .subscription_invalidations_top_k(window, k, udf_identifier.as_ref())?;

    // When filtered to a specific function, keys are tablet IDs.
    // Otherwise, keys are "{mutation}:{tablet_id}".
    let timeseries = if udf_identifier.is_some() {
        resolve_tablet_keys(timeseries, &table_mapping)
    } else {
        resolve_mutation_tablet_keys(timeseries, &table_mapping)
    };
    Ok(Json(timeseries))
}

pub(crate) async fn function_call_count_top_k(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(TopKQueryArgs { window, k }): Query<TopKQueryArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let window_json: serde_json::Value =
        serde_json::from_str(&window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;

    let k = validate_k(k)?;

    let timeseries = st
        .application
        .metrics_log(&identity)?
        .function_call_count_top_k(window, k)?;
    Ok(Json(timeseries))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CacheHitPercentageQueryArgs {
    component_path: Option<String>,
    #[serde(alias = "path")]
    udf_path: String,
    window: String,
    udf_type: Option<String>,
}
pub(crate) async fn cache_hit_percentage(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(query_args): Query<CacheHitPercentageQueryArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let window_json: serde_json::Value =
        serde_json::from_str(&query_args.window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;
    let udf_identifier = parse_udf_identifier(
        query_args.udf_type,
        query_args.component_path,
        query_args.udf_path,
    )?;
    let timeseries = st
        .application
        .metrics_log(&identity)?
        .cache_hit_percentage(udf_identifier, window)?;
    Ok(Json(timeseries))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LatencyPercentilesQueryArgs {
    component_path: Option<String>,
    #[serde(alias = "path")]
    udf_path: String,
    percentiles: String,
    window: String,
    udf_type: Option<String>,
}
pub(crate) async fn latency_percentiles(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(query_args): Query<LatencyPercentilesQueryArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let udf_identifier = parse_udf_identifier(
        query_args.udf_type,
        query_args.component_path,
        query_args.udf_path,
    )?;
    let percentiles: Vec<usize> =
        serde_json::from_str(&query_args.percentiles).map_err(anyhow::Error::new)?;
    let window_json: serde_json::Value =
        serde_json::from_str(&query_args.window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;
    let timeseries: Vec<_> = st
        .application
        .metrics_log(&identity)?
        .latency_percentiles(udf_identifier, percentiles, window)?
        .into_iter()
        .collect();
    Ok(Json(timeseries))
}

#[derive(Deserialize)]
pub(crate) struct TableRateQueryArgs {
    name: String,
    metric: String,
    window: String,
}
pub(crate) async fn table_rate(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(query_args): Query<TableRateQueryArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let name = query_args.name.parse()?;
    let metric = query_args.metric.parse()?;
    let window_json: serde_json::Value =
        serde_json::from_str(&query_args.window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;
    let timeseries = st
        .application
        .metrics_log(&identity)?
        .table_rate(name, metric, window)?;
    Ok(Json(timeseries))
}

fn parse_udf_identifier(
    udf_type: Option<String>,
    component_path: Option<String>,
    identifier: String,
) -> anyhow::Result<UdfIdentifier> {
    let component = ComponentPath::deserialize(component_path.as_deref())?;
    let udf_identifier = match udf_type {
        Some(udf_type) => {
            let udf_type: UdfType = udf_type.parse()?;
            match udf_type {
                UdfType::HttpAction => UdfIdentifier::Http(identifier.parse()?),
                _ => {
                    let udf_path: UdfPath = identifier.parse()?;
                    let path = ComponentFunctionPath {
                        component,
                        udf_path,
                    };
                    UdfIdentifier::Function(path.canonicalize())
                },
            }
        },
        None => {
            let udf_path: UdfPath = identifier.parse()?;
            let path = ComponentFunctionPath {
                component,
                udf_path,
            };
            UdfIdentifier::Function(path.canonicalize())
        },
    };
    Ok(udf_identifier)
}

#[derive(Deserialize)]
pub(crate) struct ScheduledJobLagArgs {
    window: String,
}
pub(crate) async fn scheduled_job_lag(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(query_args): Query<ScheduledJobLagArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let window_json: serde_json::Value =
        serde_json::from_str(&query_args.window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;
    let timeseries = st
        .application
        .metrics_log(&identity)?
        .scheduled_job_lag(window)?;
    Ok(Json(timeseries))
}

#[derive(Deserialize)]
pub(crate) struct FunctionConcurrencyArgs {
    window: String,
}
pub(crate) async fn function_concurrency(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(query_args): Query<FunctionConcurrencyArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let window_json: serde_json::Value =
        serde_json::from_str(&query_args.window).map_err(anyhow::Error::new)?;
    let window = window_json.try_into()?;
    let metrics = st
        .application
        .metrics_log(&identity)?
        .function_concurrency(window)?;
    Ok(Json(metrics))
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueryShadowEvidenceArgs {
    limit: Option<usize>,
    cursor: Option<String>,
    udf_type: Option<String>,
    direction: Option<String>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GeneratedWasmMemoryStatisticsArgs {
    limit: Option<usize>,
    cursor: Option<String>,
    udf_type: Option<String>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowGeneratedExportDiagnosticResponse {
    phase: &'static str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<i32>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowGeneratedExportDiagnostic> for QueryShadowGeneratedExportDiagnosticResponse {
    fn from(diagnostic: QueryShadowGeneratedExportDiagnostic) -> Self {
        Self {
            phase: diagnostic.phase_key(),
            outcome: diagnostic.outcome_key(),
            status: diagnostic.status(),
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowTimingResponse {
    exact_sample_count: u64,
    p50: Option<f64>,
    p90: Option<f64>,
    p99: Option<f64>,
    retained_max: Option<f64>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowTimingSummary> for QueryShadowTimingResponse {
    fn from(summary: QueryShadowTimingSummary) -> Self {
        Self {
            exact_sample_count: summary.exact_sample_count,
            p50: summary.p50,
            p90: summary.p90,
            p99: summary.p99,
            retained_max: summary.retained_max,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowTerminalResponse {
    capacity_drop: u64,
    shadow_failure: u64,
    timeout: u64,
    invalid_shadow: u64,
    invalid_primary: u64,
    primary_failure: u64,
    primary_snapshot_rejected: u64,
    unexpected_task_exit: u64,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowTerminalCounts> for QueryShadowTerminalResponse {
    fn from(counts: QueryShadowTerminalCounts) -> Self {
        Self {
            capacity_drop: counts.capacity_drop,
            shadow_failure: counts.shadow_failure,
            timeout: counts.timeout,
            invalid_shadow: counts.invalid_shadow,
            invalid_primary: counts.invalid_primary,
            primary_failure: counts.primary_failure,
            primary_snapshot_rejected: counts.primary_snapshot_rejected,
            unexpected_task_exit: counts.unexpected_task_exit,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowFailureStageResponse {
    transaction_setup: u64,
    environment_setup: u64,
    route_authentication: u64,
    module_loading: u64,
    wasm_execution: u64,
    verifier_execution: u64,
    transaction_finalization: u64,
    unclassified: u64,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowFailureReasonResponse {
    infrastructure: u64,
    route_authentication: u64,
    module_loading: u64,
    runtime_initialization: u64,
    initialization_timeout: u64,
    execution_timeout: u64,
    instruction_budget: u64,
    generated_export_dispatch: u64,
    guest_memory_limit: u64,
    host_owned_bytes_limit: u64,
    opaque_handle_limit: u64,
    opaque_handle_space_exhausted: u64,
    aggregate_memory_limit: u64,
    operation_limit: u64,
    host_abi_invariant: u64,
    hermes_heap_out_of_memory: u64,
    wasmtime_trap: u64,
    guest_execution: u64,
    result_finalization: u64,
    runtime_cleanup: u64,
    verifier_execution: u64,
    transaction_finalization: u64,
    unclassified: u64,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowFailureReasonCounts> for QueryShadowFailureReasonResponse {
    fn from(counts: QueryShadowFailureReasonCounts) -> Self {
        Self {
            infrastructure: counts.infrastructure,
            route_authentication: counts.route_authentication,
            module_loading: counts.module_loading,
            runtime_initialization: counts.runtime_initialization,
            initialization_timeout: counts.initialization_timeout,
            execution_timeout: counts.execution_timeout,
            instruction_budget: counts.instruction_budget,
            generated_export_dispatch: counts.generated_export_dispatch,
            guest_memory_limit: counts.guest_memory_limit,
            host_owned_bytes_limit: counts.host_owned_bytes_limit,
            opaque_handle_limit: counts.opaque_handle_limit,
            opaque_handle_space_exhausted: counts.opaque_handle_space_exhausted,
            aggregate_memory_limit: counts.aggregate_memory_limit,
            operation_limit: counts.operation_limit,
            host_abi_invariant: counts.host_abi_invariant,
            hermes_heap_out_of_memory: counts.hermes_heap_out_of_memory,
            wasmtime_trap: counts.wasmtime_trap,
            guest_execution: counts.guest_execution,
            result_finalization: counts.result_finalization,
            runtime_cleanup: counts.runtime_cleanup,
            verifier_execution: counts.verifier_execution,
            transaction_finalization: counts.transaction_finalization,
            unclassified: counts.unclassified,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowFailureStageCounts> for QueryShadowFailureStageResponse {
    fn from(counts: QueryShadowFailureStageCounts) -> Self {
        Self {
            transaction_setup: counts.transaction_setup,
            environment_setup: counts.environment_setup,
            route_authentication: counts.route_authentication,
            module_loading: counts.module_loading,
            wasm_execution: counts.wasm_execution,
            verifier_execution: counts.verifier_execution,
            transaction_finalization: counts.transaction_finalization,
            unclassified: counts.unclassified,
        }
    }
}

#[cfg(all(test, feature = "static-hermes-wasmtime-gate"))]
mod query_shadow_terminal_response_tests {
    use axum::{
        body::to_bytes,
        http::StatusCode,
    };
    use function_runner::{
        QueryShadowFailureReason,
        QueryShadowFailureStage,
        QueryShadowGeneratedExportDiagnostic,
        QueryShadowInsertedDocumentIdentity,
        QueryShadowInvalidReason,
    };
    use keybroker::{
        DeploymentOp,
        Identity,
    };
    use roles::RequireDeploymentOp;
    use serde_json::json;
    use udf::wasm_memory::{
        WasmMemoryBytesStatistics,
        WasmMemoryExecutionStatistics,
        WasmMemoryMaximums,
        WasmMemoryTerminalCounts,
    };

    use super::*;

    #[test]
    fn invalid_shadow_response_preserves_fixed_rejection_reasons() {
        for reason in [
            QueryShadowInvalidReason::SnapshotValidation,
            QueryShadowInvalidReason::LaneTypeMismatch,
            QueryShadowInvalidReason::QueryWrites,
            QueryShadowInvalidReason::OutcomeType,
            QueryShadowInvalidReason::MissingHandlerReads,
            QueryShadowInvalidReason::IncompleteHostTrace,
            QueryShadowInvalidReason::WriteNormalization,
        ] {
            for direction in [
                QueryShadowDirection::V8PrimaryWasmShadow,
                QueryShadowDirection::WasmPrimaryV8Shadow,
            ] {
                let response = QueryShadowDiagnosticResponse::from(QueryShadowDiagnosticEvidence {
                    sequence: 1,
                    recorded_at_unix_milliseconds: 1_001,
                    direction,
                    outcome: QueryShadowDiagnosticOutcome::Terminal(
                        QueryShadowTerminal::InvalidShadow { reason },
                    ),
                });
                assert_eq!(
                    serde_json::to_value(response).unwrap(),
                    json!({
                        "kind": "terminal",
                        "sequence": 1,
                        "recordedAtUnixMilliseconds": 1001,
                        "direction": direction.metric_key(),
                        "terminal": "invalid_shadow",
                        "invalidReason": reason.diagnostic_key(),
                    })
                );
            }
        }
    }

    #[test]
    fn memory_response_preserves_controller_accounting() {
        use udf::wasm_memory::{
            WasmMemoryObservation,
            WasmMemoryTerminal,
        };

        let observation = WasmMemoryObservation {
            reused_instance: true,
            checkout_baseline_bytes: 10,
            peak_admitted_bytes: 40,
            peak_guest_bytes: 30,
            peak_host_bytes: 10,
            requested_peak_bytes: 50,
            forecast_growth_bytes: 20,
            completion_bytes: 40,
            returned_to_pool_bytes: 0,
            returned_to_pool: false,
            forecast_overrun: true,
            growth_denied: true,
            successful_execution_discarded: false,
            terminal: WasmMemoryTerminal::MemoryLimit,
        };
        let response = QueryShadowDiagnosticResponse::from(QueryShadowDiagnosticEvidence {
            sequence: 9,
            recorded_at_unix_milliseconds: 9_009,
            direction: QueryShadowDirection::WasmPrimaryV8Shadow,
            outcome: QueryShadowDiagnosticOutcome::Memory(observation),
        });
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "kind": "memory",
                "sequence": 9,
                "recordedAtUnixMilliseconds": 9009,
                "direction": "wasm_primary_v8_shadow",
                "observation": {
                    "reusedInstance": true,
                    "checkoutBaselineBytes": 10,
                    "peakAdmittedBytes": 40,
                    "peakGuestBytes": 30,
                    "peakHostBytes": 10,
                    "requestedPeakBytes": 50,
                    "forecastGrowthBytes": 20,
                    "completionBytes": 40,
                    "returnedToPoolBytes": 0,
                    "returnedToPool": false,
                    "forecastOverrun": true,
                    "growthDenied": true,
                    "successfulExecutionDiscarded": false,
                    "terminal": "memoryLimit",
                },
            })
        );
    }

    #[test]
    fn response_preserves_error_fingerprints_without_raw_messages() {
        let error = common::errors::JsError::from_message("TypeError: private-value".to_owned());
        let response = serde_json::to_value(QueryShadowResultDiagnosticResponse::from(
            QueryShadowResultDiagnostic {
                reason: function_runner::QueryShadowResultDifference::OutcomeType,
                path: vec![],
                primary_type: "value",
                shadow_type: "error",
                primary_error: None,
                shadow_error: Some(QueryShadowErrorDiagnostic::from(&error)),
            },
        ))
        .unwrap();
        assert_eq!(
            response["shadowError"],
            json!({
                "kind": "type_error",
                "messageSha256": common::sha256::Sha256::hash(error.message.as_bytes()).as_hex(),
                "hasCustomData": false,
            })
        );
        assert!(response.get("primaryError").is_none());
        assert!(!response.to_string().contains("private-value"));
        let oversized = common::errors::JsError::from_message("x".repeat(65537));
        let response = serde_json::to_value(QueryShadowErrorDiagnosticResponse::from(
            QueryShadowErrorDiagnostic::from(&oversized),
        ))
        .unwrap();
        assert!(response["messageSha256"].is_null());
    }

    #[test]
    fn response_preserves_every_terminal_class() {
        let response = serde_json::to_value(QueryShadowTerminalResponse::from(
            QueryShadowTerminalCounts {
                capacity_drop: 1,
                shadow_failure: 2,
                timeout: 3,
                invalid_shadow: 4,
                invalid_primary: 5,
                primary_failure: 6,
                primary_snapshot_rejected: 7,
                unexpected_task_exit: 8,
            },
        ))
        .unwrap();

        assert_eq!(
            response,
            json!({
                "capacityDrop": 1,
                "shadowFailure": 2,
                "timeout": 3,
                "invalidShadow": 4,
                "invalidPrimary": 5,
                "primaryFailure": 6,
                "primarySnapshotRejected": 7,
                "unexpectedTaskExit": 8,
            })
        );
    }

    #[test]
    fn response_preserves_every_shadow_failure_stage() {
        let response = serde_json::to_value(QueryShadowFailureStageResponse::from(
            QueryShadowFailureStageCounts {
                transaction_setup: 1,
                environment_setup: 2,
                route_authentication: 3,
                module_loading: 4,
                wasm_execution: 5,
                verifier_execution: 6,
                transaction_finalization: 7,
                unclassified: 8,
            },
        ))
        .unwrap();

        assert_eq!(
            response,
            json!({
                "transactionSetup": 1,
                "environmentSetup": 2,
                "routeAuthentication": 3,
                "moduleLoading": 4,
                "wasmExecution": 5,
                "verifierExecution": 6,
                "transactionFinalization": 7,
                "unclassified": 8,
            })
        );
    }

    fn full_summary() -> QueryShadowEvidenceSummary {
        let timing = QueryShadowTimingSummary {
            exact_sample_count: 13,
            p50: Some(0.125),
            p90: Some(0.25),
            p99: Some(0.5),
            retained_max: Some(1.0),
        };
        QueryShadowEvidenceSummary {
            attempt_count: 1,
            admitted_count: 2,
            capacity_drop_count: 3,
            evidence_count: 4,
            completed_count: 5,
            mismatch_count: 6,
            mismatch_counts: QueryShadowMismatchCounts {
                snapshot: 1,
                result: 2,
                query_journal: 3,
                read_dependencies: 4,
                invocation_inputs: 5,
                host_operation_trace: 6,
                host_operation_error: 7,
                observed_identity: 8,
                observed_time: 9,
                observed_rng: 10,
                log_lines: 11,
                audit_log_lines: 12,
                write_set: 13,
            },
            inserted_document_identity_counts: QueryShadowInsertedDocumentIdentityCounts {
                not_applicable: 14,
                not_required: 15,
                matched: 16,
                inconsistent: 17,
                unresolved: 18,
            },
            terminal_count: 7,
            terminal_counts: QueryShadowTerminalCounts {
                capacity_drop: 1,
                shadow_failure: 2,
                timeout: 3,
                invalid_shadow: 4,
                invalid_primary: 5,
                primary_failure: 6,
                primary_snapshot_rejected: 7,
                unexpected_task_exit: 8,
            },
            shadow_failure_stage_counts: QueryShadowFailureStageCounts {
                transaction_setup: 1,
                environment_setup: 2,
                route_authentication: 3,
                module_loading: 4,
                wasm_execution: 5,
                verifier_execution: 6,
                transaction_finalization: 7,
                unclassified: 8,
            },
            shadow_failure_reason_counts: QueryShadowFailureReasonCounts {
                infrastructure: 1,
                route_authentication: 2,
                module_loading: 3,
                runtime_initialization: 4,
                initialization_timeout: 13,
                execution_timeout: 14,
                instruction_budget: 15,
                generated_export_dispatch: 5,
                guest_memory_limit: 6,
                host_owned_bytes_limit: 16,
                opaque_handle_limit: 17,
                opaque_handle_space_exhausted: 18,
                aggregate_memory_limit: 19,
                operation_limit: 20,
                host_abi_invariant: 21,
                hermes_heap_out_of_memory: 23,
                wasmtime_trap: 22,
                guest_execution: 7,
                result_finalization: 8,
                runtime_cleanup: 9,
                verifier_execution: 10,
                transaction_finalization: 11,
                unclassified: 12,
            },
            primary_seconds: timing.clone(),
            wasm_seconds: timing.clone(),
            wasm_to_primary_ratio: timing.clone(),
            wasm_primary_seconds: timing.clone(),
            v8_verifier_seconds: timing.clone(),
            v8_verifier_to_wasm_primary_ratio: timing,
        }
    }

    fn full_summary_json() -> serde_json::Value {
        json!({
            "attemptCount": 1,
            "admittedCount": 2,
            "capacityDropCount": 3,
            "evidenceCount": 4,
            "completedCount": 5,
            "mismatchCount": 6,
            "mismatchCounts": {
                "snapshot": 1,
                "result": 2,
                "queryJournal": 3,
                "readDependencies": 4,
                "invocationInputs": 5,
                "hostOperationTrace": 6,
                "hostOperationError": 7,
                "observedIdentity": 8,
                "observedTime": 9,
                "observedRng": 10,
                "logLines": 11,
                "auditLogLines": 12,
                "writeSet": 13,
            },
            "insertedDocumentIdentityCounts": {
                "notApplicable": 14,
                "notRequired": 15,
                "matched": 16,
                "inconsistent": 17,
                "unresolved": 18,
            },
            "terminalCount": 7,
            "terminalCounts": {
                "capacityDrop": 1,
                "shadowFailure": 2,
                "timeout": 3,
                "invalidShadow": 4,
                "invalidPrimary": 5,
                "primaryFailure": 6,
                "primarySnapshotRejected": 7,
                "unexpectedTaskExit": 8,
            },
            "shadowFailureStageCounts": {
                "transactionSetup": 1,
                "environmentSetup": 2,
                "routeAuthentication": 3,
                "moduleLoading": 4,
                "wasmExecution": 5,
                "verifierExecution": 6,
                "transactionFinalization": 7,
                "unclassified": 8,
            },
            "shadowFailureReasonCounts": {
                "infrastructure": 1,
                "routeAuthentication": 2,
                "moduleLoading": 3,
                "runtimeInitialization": 4,
                "initializationTimeout": 13,
                "executionTimeout": 14,
                "instructionBudget": 15,
                "generatedExportDispatch": 5,
                "guestMemoryLimit": 6,
                "hostOwnedBytesLimit": 16,
                "opaqueHandleLimit": 17,
                "opaqueHandleSpaceExhausted": 18,
                "aggregateMemoryLimit": 19,
                "operationLimit": 20,
                "hostAbiInvariant": 21,
                "hermesHeapOutOfMemory": 23,
                "wasmtimeTrap": 22,
                "guestExecution": 7,
                "resultFinalization": 8,
                "runtimeCleanup": 9,
                "verifierExecution": 10,
                "transactionFinalization": 11,
                "unclassified": 12,
            },
            "primarySeconds": {
                "exactSampleCount": 13,
                "p50": 0.125,
                "p90": 0.25,
                "p99": 0.5,
                "retainedMax": 1.0,
            },
            "wasmSeconds": {
                "exactSampleCount": 13,
                "p50": 0.125,
                "p90": 0.25,
                "p99": 0.5,
                "retainedMax": 1.0,
            },
            "wasmToPrimaryRatio": {
                "exactSampleCount": 13,
                "p50": 0.125,
                "p90": 0.25,
                "p99": 0.5,
                "retainedMax": 1.0,
            },
            "wasmPrimarySeconds": {
                "exactSampleCount": 13,
                "p50": 0.125,
                "p90": 0.25,
                "p99": 0.5,
                "retainedMax": 1.0,
            },
            "v8VerifierSeconds": {
                "exactSampleCount": 13,
                "p50": 0.125,
                "p90": 0.25,
                "p99": 0.5,
                "retainedMax": 1.0,
            },
            "v8VerifierToWasmPrimaryRatio": {
                "exactSampleCount": 13,
                "p50": 0.125,
                "p90": 0.25,
                "p99": 0.5,
                "retainedMax": 1.0,
            },
        })
    }

    #[test]
    fn evidence_response_preserves_full_envelope_and_flattened_route_summary() {
        let generation_sha256 = "a".repeat(64);
        let route_sha256 = "b".repeat(64);
        let next_route_sha256 = "c".repeat(64);
        let route_key = QueryShadowRouteKey::new(&generation_sha256, &route_sha256).unwrap();
        let next_cursor = QueryShadowRouteKey::new(&generation_sha256, &next_route_sha256).unwrap();
        let generated_export_diagnostic =
            QueryShadowGeneratedExportDiagnostic::SelectedEntryPreparationRejected { status: 65 };
        let response =
            serde_json::to_value(QueryShadowEvidenceResponse::from(QueryShadowEvidencePage {
                udf_type: UdfType::Query,
                direction: QueryShadowDirection::V8PrimaryWasmShadow,
                aggregate: full_summary(),
                selection: Some(QueryShadowRegistrySelection {
                    udf_type: UdfType::Query,
                    generation_sha256: generation_sha256.clone(),
                    selected_route_count: 8,
                    routes_with_retained_evidence_count: 9,
                    routes_with_completed_comparison_count: 10,
                    routes_with_mismatch_count: 11,
                    routes_with_terminal_count: 12,
                    unobserved_route_count: 13,
                }),
                routes: vec![QueryShadowRouteEvidence {
                    route_key,
                    selected_count: 14,
                    unobserved_count: 15,
                    summary: full_summary(),
                    diagnostics: vec![
                        QueryShadowDiagnosticEvidence {
                            sequence: 16,
                            recorded_at_unix_milliseconds: 16_000,
                            direction: QueryShadowDirection::V8PrimaryWasmShadow,
                            outcome: QueryShadowDiagnosticOutcome::Mismatch(
                                QueryShadowComparison {
                                    snapshot_matches: true,
                                    result_matches: false,
                                    result_diagnostic: Some(QueryShadowResultDiagnostic {
                                        reason: function_runner::QueryShadowResultDifference::Type,
                                        path: vec![0, 2],
                                        primary_type: "String",
                                        shadow_type: "Null",
                                        primary_error: None,
                                        shadow_error: None,
                                    }),
                                    journal_matches: true,
                                    read_dependencies_match: false,
                                    invocation_inputs_match: true,
                                    host_operation_trace_matches: false,
                                    host_operation_error_matches: true,
                                    observed_identity_matches: true,
                                    observed_time_matches: true,
                                    observed_rng_matches: true,
                                    log_lines_match: true,
                                    audit_log_lines_match: true,
                                    write_set_matches: false,
                                    inserted_document_identity:
                                        QueryShadowInsertedDocumentIdentity::Unresolved,
                                    host_operation_trace_diagnostic: Some(
                                        QueryShadowHostOperationTraceDiagnostic {
                                            primary_comparison_eligible: true,
                                            shadow_comparison_eligible: true,
                                            differing_counts: vec![
                                                QueryShadowHostOperationCountDifference {
                                                    operation:
                                                        udf::LogicalHostOperation::UserIdentity,
                                                    status:
                                                        udf::LogicalHostOperationStatus::Success,
                                                    primary_count: 1,
                                                    shadow_count: 2,
                                                },
                                            ],
                                            omitted_differing_count: 0,
                                        },
                                    ),
                                },
                            ),
                        },
                        QueryShadowDiagnosticEvidence {
                            sequence: 17,
                            recorded_at_unix_milliseconds: 17_000,
                            direction: QueryShadowDirection::WasmPrimaryV8Shadow,
                            outcome: QueryShadowDiagnosticOutcome::Terminal(
                                QueryShadowTerminal::ShadowFailure {
                                    stage: QueryShadowFailureStage::VerifierExecution,
                                    reason: QueryShadowFailureReason::VerifierExecution,
                                    generated_export_diagnostic: Some(generated_export_diagnostic),
                                    wasmtime_trap_diagnostic: None,
                                },
                            ),
                        },
                        QueryShadowDiagnosticEvidence {
                            sequence: 18,
                            recorded_at_unix_milliseconds: 18_000,
                            direction: QueryShadowDirection::V8PrimaryWasmShadow,
                            outcome: QueryShadowDiagnosticOutcome::Admission(
                                function_runner::QueryShadowAdmissionEvent::CapacityDrop,
                            ),
                        },
                        QueryShadowDiagnosticEvidence {
                            sequence: 19,
                            recorded_at_unix_milliseconds: 19_000,
                            direction: QueryShadowDirection::V8PrimaryWasmShadow,
                            outcome: QueryShadowDiagnosticOutcome::Terminal(
                                QueryShadowTerminal::ShadowFailure {
                                    stage: QueryShadowFailureStage::WasmExecution,
                                    reason: QueryShadowFailureReason::WasmtimeTrap,
                                    generated_export_diagnostic: None,
                                    wasmtime_trap_diagnostic: Some(
                                        StaticHermesWasmTrapDiagnostic::new(
                                            StaticHermesWasmTrapCode::UnreachableCodeReached,
                                            Some(37),
                                            Some(91),
                                        ),
                                    ),
                                },
                            ),
                        },
                    ],
                }],
                next_cursor: Some(next_cursor.clone()),
            }))
            .unwrap();
        let summary = full_summary_json();
        let mut route = serde_json::Map::from_iter([
            ("generationSha256".to_owned(), json!(generation_sha256)),
            ("routeSha256".to_owned(), json!(route_sha256)),
            ("selectedCount".to_owned(), json!(14)),
            ("unobservedCount".to_owned(), json!(15)),
            (
                "diagnostics".to_owned(),
                json!([
                    {
                        "kind": "mismatch",
                        "sequence": 16,
                        "recordedAtUnixMilliseconds": 16000,
                        "direction": "v8_primary_wasm_shadow",
                        "comparison": {
                            "snapshotMatches": true,
                            "resultMatches": false,
                            "resultDiagnostic": {
                                "reason": "type",
                                "path": [0, 2],
                                "primaryType": "String",
                                "shadowType": "Null",
                            },
                            "journalMatches": true,
                            "readDependenciesMatch": false,
                            "invocationInputsMatch": true,
                            "hostOperationTraceMatches": false,
                            "hostOperationErrorMatches": true,
                            "observedIdentityMatches": true,
                            "observedTimeMatches": true,
                            "observedRngMatches": true,
                            "logLinesMatch": true,
                            "auditLogLinesMatch": true,
                            "writeSetMatches": false,
                            "insertedDocumentIdentity": "unresolved",
                            "hostOperationTraceDiagnostic": {
                                "primaryComparisonEligible": true,
                                "shadowComparisonEligible": true,
                                "differingCounts": [
                                    {
                                        "operation": "user_identity",
                                        "status": "success",
                                        "primaryCount": 1,
                                        "shadowCount": 2,
                                    },
                                ],
                                "omittedDifferingCount": 0,
                            },
                        },
                    },
                    {
                        "kind": "terminal",
                        "sequence": 17,
                        "recordedAtUnixMilliseconds": 17000,
                        "direction": "wasm_primary_v8_shadow",
                        "terminal": "shadow_failure",
                        "failureStage": "verifier_execution",
                        "failureReason": "verifier_execution",
                        "generatedExportDiagnostic": {
                            "phase": "selected_entry_preparation",
                            "outcome": "rejected",
                            "status": 65,
                        },
                    },
                    {
                        "kind": "admission",
                        "sequence": 18,
                        "recordedAtUnixMilliseconds": 18000,
                        "direction": "v8_primary_wasm_shadow",
                        "event": "capacity_drop",
                    },
                    {
                        "kind": "terminal",
                        "sequence": 19,
                        "recordedAtUnixMilliseconds": 19000,
                        "direction": "v8_primary_wasm_shadow",
                        "terminal": "shadow_failure",
                        "failureStage": "wasm_execution",
                        "failureReason": "wasmtime_trap",
                        "wasmtimeTrapDiagnostic": {
                            "code": "unreachable_code_reached",
                            "frames": [{
                                "moduleRole": "unknown",
                                "functionIndex": 37,
                                "functionOffset": 91,
                            }],
                            "framesTruncated": false,
                            "invocationCorrelation": "00000000-0000-4000-8000-000000000000",
                            "reusedInstance": false,
                            "fuel": {"limit": 0, "remaining": null},
                            "resources": {
                                "operationCount": 0,
                                "operationLimit": 0,
                                "valueHandleCount": 0,
                                "valueHandleLimit": 0,
                                "currentGuestBytes": 0,
                                "guestByteLimit": 0,
                                "currentHostBytes": 0,
                                "hostByteLimit": 0,
                            },
                            "hostOperationTraceAvailable": false,
                            "hostOperations": [],
                            "hostOperationsTruncated": false,
                            "stderr": {
                                "classification": "empty",
                                "byteCount": 0,
                                "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                            },
                        },
                    },
                ]),
            ),
        ]);
        route.extend(summary.as_object().unwrap().clone());

        assert_eq!(
            response,
            json!({
                "udfType": "query",
                "direction": "v8_primary_wasm_shadow",
                "aggregate": summary,
                "selection": {
                    "udfType": "query",
                    "generationSha256": "a".repeat(64),
                    "selectedRouteCount": 8,
                    "observedRouteCount": 9,
                    "completedRouteCount": 10,
                    "mismatchRouteCount": 11,
                    "terminalRouteCount": 12,
                    "unobservedRouteCount": 13,
                },
                "routes": [serde_json::Value::Object(route)],
                "pagination": {
                    "hasMore": true,
                    "nextCursor": next_cursor.as_str(),
                },
            })
        );
    }

    #[test]
    fn query_shadow_evidence_requires_view_metrics_permission() {
        assert!(Identity::system()
            .require_operation(DeploymentOp::ViewMetrics)
            .is_ok());
        assert!(Identity::Unknown(None)
            .require_operation(DeploymentOp::ViewMetrics)
            .is_err());
    }

    fn generated_memory_execution_statistics(
        observation_count: u64,
        reused_count: u64,
        maximum: u64,
    ) -> WasmMemoryExecutionStatistics {
        let distribution = WasmMemoryBytesStatistics {
            p50: 64,
            p90: 80,
            p99: 160,
            maximum,
        };
        let maximums = WasmMemoryMaximums {
            checkout_baseline_bytes: maximum,
            peak_admitted_bytes: maximum,
            peak_guest_bytes: maximum,
            peak_host_bytes: maximum,
            requested_peak_bytes: maximum,
            forecast_growth_bytes: maximum,
            completion_bytes: maximum,
            returned_to_pool_bytes: maximum,
        };
        WasmMemoryExecutionStatistics {
            observation_count,
            reused_count,
            returned_to_pool_count: observation_count,
            forecast_overrun_count: 0,
            growth_denied_count: 0,
            successful_execution_discarded_count: 0,
            terminal_counts: WasmMemoryTerminalCounts {
                success: observation_count,
                ..Default::default()
            },
            checkout_baseline_bytes: distribution.clone(),
            peak_admitted_bytes: distribution.clone(),
            peak_guest_bytes: distribution.clone(),
            peak_host_bytes: distribution.clone(),
            requested_peak_bytes: distribution.clone(),
            forecast_growth_bytes: distribution.clone(),
            completion_bytes: distribution.clone(),
            returned_to_pool_bytes: distribution,
            fresh_maximums: (observation_count > reused_count).then(|| maximums.clone()),
            reused_maximums: (reused_count > 0).then_some(maximums),
        }
    }

    #[test]
    fn generated_memory_statistics_response_preserves_scope_roles_and_pagination() {
        let generation_sha256 = "a".repeat(64);
        let first_route_sha256 = "1".repeat(64);
        let second_route_sha256 = "2".repeat(64);
        let first = generated_memory_execution_statistics(1, 0, 200);
        let second = generated_memory_execution_statistics(2, 2, 100);
        let aggregate = generated_memory_execution_statistics(3, 2, 200);
        let statistics = StaticHermesGeneratedMemoryStatistics {
            generation_sha256: generation_sha256.clone(),
            selected_route_count: 3,
            observed_route_count: 2,
            route_record_capacity: 4096,
            route_record_eviction_count: 7,
            aggregate: Some(aggregate),
            routes: vec![
                StaticHermesGeneratedMemoryRouteStatistics {
                    route_sha256: first_route_sha256.clone(),
                    aggregate: first.clone(),
                    primary: Some(first),
                    shadow: None,
                },
                StaticHermesGeneratedMemoryRouteStatistics {
                    route_sha256: second_route_sha256.clone(),
                    aggregate: second.clone(),
                    primary: None,
                    shadow: Some(second),
                },
            ],
        };
        let first_page = serde_json::to_value(
            generated_wasm_memory_statistics_response(UdfType::Query, statistics.clone(), None, 1)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first_page["scope"], "backend_process_route_record_lifetime");
        assert_eq!(first_page["routeRecordCapacity"], 4096);
        assert_eq!(first_page["routeRecordEvictionCount"], 7);
        assert_eq!(first_page["selection"]["selectedRouteCount"], 3);
        assert_eq!(first_page["selection"]["observedRouteCount"], 2);
        assert_eq!(first_page["selection"]["unobservedRouteCount"], 1);
        assert_eq!(first_page["aggregate"]["observationCount"], 3);
        assert_eq!(first_page["aggregate"]["peakAdmittedBytes"]["p99"], 160);
        assert_eq!(first_page["aggregate"]["peakAdmittedBytes"]["maximum"], 200);
        assert_eq!(first_page["routes"][0]["routeSha256"], first_route_sha256);
        assert!(first_page["routes"][0].get("primary").is_some());
        assert!(first_page["routes"][0].get("shadow").is_none());
        assert_eq!(first_page["pagination"]["hasMore"], true);

        let cursor =
            QueryShadowRouteKey::parse(first_page["pagination"]["nextCursor"].as_str().unwrap())
                .unwrap();
        let second_page = serde_json::to_value(
            generated_wasm_memory_statistics_response(UdfType::Query, statistics, Some(&cursor), 1)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second_page["routes"][0]["routeSha256"], second_route_sha256);
        assert!(second_page["routes"][0].get("primary").is_none());
        assert!(second_page["routes"][0].get("shadow").is_some());
        assert_eq!(second_page["pagination"]["hasMore"], false);
        assert!(second_page["pagination"].get("nextCursor").is_none());
    }

    #[tokio::test]
    async fn query_shadow_evidence_registry_errors_have_a_generic_private_response(
    ) -> anyhow::Result<()> {
        let private_registry_error = "private registry path /functions/secret identity 123";
        let evidence = Err::<(), _>(anyhow::anyhow!(private_registry_error));
        let error = evidence
            .map_err(|_| anyhow::anyhow!(ErrorMetadata::operational_internal_server_error()))
            .unwrap_err();
        let response = HttpResponseError::from(error).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body)?,
            json!({
                "code": "InternalServerError",
                "message": "Your request couldn't be completed. Try again later.",
            })
        );
        assert!(!String::from_utf8(body.to_vec())?.contains(private_registry_error));
        Ok(())
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowMismatchResponse {
    snapshot: u64,
    result: u64,
    query_journal: u64,
    read_dependencies: u64,
    invocation_inputs: u64,
    host_operation_trace: u64,
    host_operation_error: u64,
    observed_identity: u64,
    observed_time: u64,
    observed_rng: u64,
    log_lines: u64,
    audit_log_lines: u64,
    write_set: u64,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowInsertedDocumentIdentityResponse {
    not_applicable: u64,
    not_required: u64,
    matched: u64,
    inconsistent: u64,
    unresolved: u64,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowInsertedDocumentIdentityCounts>
    for QueryShadowInsertedDocumentIdentityResponse
{
    fn from(counts: QueryShadowInsertedDocumentIdentityCounts) -> Self {
        Self {
            not_applicable: counts.not_applicable,
            not_required: counts.not_required,
            matched: counts.matched,
            inconsistent: counts.inconsistent,
            unresolved: counts.unresolved,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowMismatchCounts> for QueryShadowMismatchResponse {
    fn from(counts: QueryShadowMismatchCounts) -> Self {
        Self {
            snapshot: counts.snapshot,
            result: counts.result,
            query_journal: counts.query_journal,
            read_dependencies: counts.read_dependencies,
            invocation_inputs: counts.invocation_inputs,
            host_operation_trace: counts.host_operation_trace,
            host_operation_error: counts.host_operation_error,
            observed_identity: counts.observed_identity,
            observed_time: counts.observed_time,
            observed_rng: counts.observed_rng,
            log_lines: counts.log_lines,
            audit_log_lines: counts.audit_log_lines,
            write_set: counts.write_set,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowSummaryResponse {
    attempt_count: u64,
    admitted_count: u64,
    capacity_drop_count: u64,
    evidence_count: u64,
    completed_count: u64,
    mismatch_count: u64,
    mismatch_counts: QueryShadowMismatchResponse,
    inserted_document_identity_counts: QueryShadowInsertedDocumentIdentityResponse,
    terminal_count: u64,
    terminal_counts: QueryShadowTerminalResponse,
    shadow_failure_stage_counts: QueryShadowFailureStageResponse,
    shadow_failure_reason_counts: QueryShadowFailureReasonResponse,
    primary_seconds: QueryShadowTimingResponse,
    wasm_seconds: QueryShadowTimingResponse,
    wasm_to_primary_ratio: QueryShadowTimingResponse,
    wasm_primary_seconds: QueryShadowTimingResponse,
    v8_verifier_seconds: QueryShadowTimingResponse,
    v8_verifier_to_wasm_primary_ratio: QueryShadowTimingResponse,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowEvidenceSummary> for QueryShadowSummaryResponse {
    fn from(summary: QueryShadowEvidenceSummary) -> Self {
        Self {
            attempt_count: summary.attempt_count,
            admitted_count: summary.admitted_count,
            capacity_drop_count: summary.capacity_drop_count,
            evidence_count: summary.evidence_count,
            completed_count: summary.completed_count,
            mismatch_count: summary.mismatch_count,
            mismatch_counts: summary.mismatch_counts.into(),
            inserted_document_identity_counts: summary.inserted_document_identity_counts.into(),
            terminal_count: summary.terminal_count,
            terminal_counts: summary.terminal_counts.into(),
            shadow_failure_stage_counts: summary.shadow_failure_stage_counts.into(),
            shadow_failure_reason_counts: summary.shadow_failure_reason_counts.into(),
            primary_seconds: summary.primary_seconds.into(),
            wasm_seconds: summary.wasm_seconds.into(),
            wasm_to_primary_ratio: summary.wasm_to_primary_ratio.into(),
            wasm_primary_seconds: summary.wasm_primary_seconds.into(),
            v8_verifier_seconds: summary.v8_verifier_seconds.into(),
            v8_verifier_to_wasm_primary_ratio: summary.v8_verifier_to_wasm_primary_ratio.into(),
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowComparisonResponse {
    snapshot_matches: bool,
    result_matches: bool,
    journal_matches: bool,
    read_dependencies_match: bool,
    invocation_inputs_match: bool,
    host_operation_trace_matches: bool,
    host_operation_error_matches: bool,
    observed_identity_matches: bool,
    observed_time_matches: bool,
    observed_rng_matches: bool,
    log_lines_match: bool,
    audit_log_lines_match: bool,
    write_set_matches: bool,
    inserted_document_identity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    host_operation_trace_diagnostic: Option<QueryShadowHostOperationTraceDiagnosticResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_diagnostic: Option<QueryShadowResultDiagnosticResponse>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowResultDiagnosticResponse {
    reason: &'static str,
    path: Vec<usize>,
    primary_type: &'static str,
    shadow_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_error: Option<QueryShadowErrorDiagnosticResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shadow_error: Option<QueryShadowErrorDiagnosticResponse>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowErrorDiagnosticResponse {
    kind: &'static str,
    message_sha256: Option<String>,
    has_custom_data: bool,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowErrorDiagnostic> for QueryShadowErrorDiagnosticResponse {
    fn from(diagnostic: QueryShadowErrorDiagnostic) -> Self {
        Self {
            kind: diagnostic.kind.diagnostic_key(),
            message_sha256: diagnostic.message_sha256.map(|digest| digest.as_hex()),
            has_custom_data: diagnostic.has_custom_data,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowResultDiagnostic> for QueryShadowResultDiagnosticResponse {
    fn from(diagnostic: QueryShadowResultDiagnostic) -> Self {
        Self {
            reason: diagnostic.reason.diagnostic_key(),
            path: diagnostic.path,
            primary_type: diagnostic.primary_type,
            shadow_type: diagnostic.shadow_type,
            primary_error: diagnostic.primary_error.map(Into::into),
            shadow_error: diagnostic.shadow_error.map(Into::into),
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowHostOperationCountDifferenceResponse {
    operation: &'static str,
    status: &'static str,
    primary_count: u64,
    shadow_count: u64,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowHostOperationCountDifference>
    for QueryShadowHostOperationCountDifferenceResponse
{
    fn from(difference: QueryShadowHostOperationCountDifference) -> Self {
        Self {
            operation: difference.operation.diagnostic_key(),
            status: difference.status.diagnostic_key(),
            primary_count: difference.primary_count,
            shadow_count: difference.shadow_count,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowHostOperationTraceDiagnosticResponse {
    primary_comparison_eligible: bool,
    shadow_comparison_eligible: bool,
    differing_counts: Vec<QueryShadowHostOperationCountDifferenceResponse>,
    omitted_differing_count: u64,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowHostOperationTraceDiagnostic>
    for QueryShadowHostOperationTraceDiagnosticResponse
{
    fn from(diagnostic: QueryShadowHostOperationTraceDiagnostic) -> Self {
        Self {
            primary_comparison_eligible: diagnostic.primary_comparison_eligible,
            shadow_comparison_eligible: diagnostic.shadow_comparison_eligible,
            differing_counts: diagnostic
                .differing_counts
                .into_iter()
                .map(Into::into)
                .collect(),
            omitted_differing_count: diagnostic.omitted_differing_count,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowComparison> for QueryShadowComparisonResponse {
    fn from(comparison: QueryShadowComparison) -> Self {
        Self {
            snapshot_matches: comparison.snapshot_matches,
            result_matches: comparison.result_matches,
            result_diagnostic: comparison.result_diagnostic.map(Into::into),
            journal_matches: comparison.journal_matches,
            read_dependencies_match: comparison.read_dependencies_match,
            invocation_inputs_match: comparison.invocation_inputs_match,
            host_operation_trace_matches: comparison.host_operation_trace_matches,
            host_operation_error_matches: comparison.host_operation_error_matches,
            observed_identity_matches: comparison.observed_identity_matches,
            observed_time_matches: comparison.observed_time_matches,
            observed_rng_matches: comparison.observed_rng_matches,
            log_lines_match: comparison.log_lines_match,
            audit_log_lines_match: comparison.audit_log_lines_match,
            write_set_matches: comparison.write_set_matches,
            inserted_document_identity: comparison.inserted_document_identity.metric_key(),
            host_operation_trace_diagnostic: comparison
                .host_operation_trace_diagnostic
                .map(Into::into),
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "kind"
)]
enum QueryShadowDiagnosticResponse {
    Memory {
        sequence: u64,
        recorded_at_unix_milliseconds: u64,
        direction: &'static str,
        observation: udf::wasm_memory::WasmMemoryObservation,
    },
    Admission {
        sequence: u64,
        recorded_at_unix_milliseconds: u64,
        direction: &'static str,
        event: &'static str,
    },
    Mismatch {
        sequence: u64,
        recorded_at_unix_milliseconds: u64,
        direction: &'static str,
        comparison: QueryShadowComparisonResponse,
    },
    Terminal {
        sequence: u64,
        recorded_at_unix_milliseconds: u64,
        direction: &'static str,
        terminal: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        invalid_reason: Option<&'static str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure_stage: Option<&'static str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failure_reason: Option<&'static str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        generated_export_diagnostic: Option<QueryShadowGeneratedExportDiagnosticResponse>,
        #[serde(skip_serializing_if = "Option::is_none")]
        wasmtime_trap_diagnostic: Option<StaticHermesWasmTrapDiagnostic>,
    },
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowDiagnosticEvidence> for QueryShadowDiagnosticResponse {
    fn from(evidence: QueryShadowDiagnosticEvidence) -> Self {
        match evidence.outcome {
            QueryShadowDiagnosticOutcome::Memory(observation) => Self::Memory {
                sequence: evidence.sequence,
                recorded_at_unix_milliseconds: evidence.recorded_at_unix_milliseconds,
                direction: evidence.direction.metric_key(),
                observation,
            },
            QueryShadowDiagnosticOutcome::Admission(event) => Self::Admission {
                sequence: evidence.sequence,
                recorded_at_unix_milliseconds: evidence.recorded_at_unix_milliseconds,
                direction: evidence.direction.metric_key(),
                event: event.metric_key(),
            },
            QueryShadowDiagnosticOutcome::Mismatch(comparison) => Self::Mismatch {
                sequence: evidence.sequence,
                recorded_at_unix_milliseconds: evidence.recorded_at_unix_milliseconds,
                direction: evidence.direction.metric_key(),
                comparison: comparison.into(),
            },
            QueryShadowDiagnosticOutcome::Terminal(terminal) => Self::Terminal {
                sequence: evidence.sequence,
                recorded_at_unix_milliseconds: evidence.recorded_at_unix_milliseconds,
                direction: evidence.direction.metric_key(),
                terminal: terminal.metric_key(),
                invalid_reason: if let QueryShadowTerminal::InvalidShadow { reason } = terminal {
                    Some(reason.diagnostic_key())
                } else {
                    None
                },
                failure_stage: terminal
                    .shadow_failure_stage()
                    .map(|stage| stage.metric_key()),
                failure_reason: terminal
                    .shadow_failure_reason()
                    .map(|reason| reason.metric_key()),
                generated_export_diagnostic: terminal.generated_export_diagnostic().map(Into::into),
                wasmtime_trap_diagnostic: terminal.wasmtime_trap_diagnostic().map(Into::into),
            },
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowRouteResponse {
    generation_sha256: String,
    route_sha256: String,
    selected_count: u64,
    unobserved_count: u64,
    diagnostics: Vec<QueryShadowDiagnosticResponse>,
    #[serde(flatten)]
    summary: QueryShadowSummaryResponse,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowRouteEvidence> for QueryShadowRouteResponse {
    fn from(evidence: QueryShadowRouteEvidence) -> Self {
        Self {
            generation_sha256: evidence.route_key.generation_sha256().to_owned(),
            route_sha256: evidence.route_key.route_sha256().to_owned(),
            selected_count: evidence.selected_count,
            unobserved_count: evidence.unobserved_count,
            diagnostics: evidence.diagnostics.into_iter().map(Into::into).collect(),
            summary: evidence.summary.into(),
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowSelectionResponse {
    udf_type: String,
    generation_sha256: String,
    selected_route_count: u64,
    /// A route is observed after the metrics store retains either a completed
    /// comparison or a terminal report for it.
    observed_route_count: u64,
    completed_route_count: u64,
    mismatch_route_count: u64,
    terminal_route_count: u64,
    unobserved_route_count: u64,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowRegistrySelection> for QueryShadowSelectionResponse {
    fn from(selection: QueryShadowRegistrySelection) -> Self {
        Self {
            udf_type: selection.udf_type.to_lowercase_string().to_owned(),
            generation_sha256: selection.generation_sha256,
            selected_route_count: selection.selected_route_count,
            observed_route_count: selection.routes_with_retained_evidence_count,
            completed_route_count: selection.routes_with_completed_comparison_count,
            mismatch_route_count: selection.routes_with_mismatch_count,
            terminal_route_count: selection.routes_with_terminal_count,
            unobserved_route_count: selection.unobserved_route_count,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryShadowEvidenceResponse {
    udf_type: String,
    direction: String,
    aggregate: QueryShadowSummaryResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    selection: Option<QueryShadowSelectionResponse>,
    routes: Vec<QueryShadowRouteResponse>,
    pagination: PaginationMetadata,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GeneratedWasmMemorySelectionResponse {
    selected_route_count: usize,
    observed_route_count: usize,
    unobserved_route_count: usize,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GeneratedWasmMemoryRouteResponse {
    route_sha256: String,
    aggregate: udf::wasm_memory::WasmMemoryExecutionStatistics,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary: Option<udf::wasm_memory::WasmMemoryExecutionStatistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shadow: Option<udf::wasm_memory::WasmMemoryExecutionStatistics>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<StaticHermesGeneratedMemoryRouteStatistics> for GeneratedWasmMemoryRouteResponse {
    fn from(statistics: StaticHermesGeneratedMemoryRouteStatistics) -> Self {
        Self {
            route_sha256: statistics.route_sha256,
            aggregate: statistics.aggregate,
            primary: statistics.primary,
            shadow: statistics.shadow,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GeneratedWasmMemoryStatisticsResponse {
    udf_type: String,
    generation_sha256: String,
    scope: &'static str,
    route_record_capacity: usize,
    route_record_eviction_count: u64,
    selection: GeneratedWasmMemorySelectionResponse,
    aggregate: Option<udf::wasm_memory::WasmMemoryExecutionStatistics>,
    routes: Vec<GeneratedWasmMemoryRouteResponse>,
    pagination: PaginationMetadata,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn generated_wasm_memory_statistics_response(
    udf_type: UdfType,
    statistics: StaticHermesGeneratedMemoryStatistics,
    cursor: Option<&QueryShadowRouteKey>,
    limit: usize,
) -> anyhow::Result<GeneratedWasmMemoryStatisticsResponse> {
    let start = match cursor {
        None => 0,
        Some(cursor) => {
            anyhow::ensure!(
                cursor.generation_sha256() == statistics.generation_sha256,
                "generated Wasm memory statistics cursor does not match the active generation"
            );
            statistics
                .routes
                .binary_search_by(|route| route.route_sha256.as_str().cmp(cursor.route_sha256()))
                .map(|index| index + 1)
                .map_err(|_| {
                    anyhow::anyhow!("generated Wasm memory statistics cursor is not observed")
                })?
        },
    };
    let has_more = statistics.routes.len().saturating_sub(start) > limit;
    let routes = statistics
        .routes
        .into_iter()
        .skip(start)
        .take(limit)
        .map(GeneratedWasmMemoryRouteResponse::from)
        .collect::<Vec<_>>();
    let next_cursor = has_more
        .then(|| {
            QueryShadowRouteKey::new(
                &statistics.generation_sha256,
                &routes
                    .last()
                    .expect("generated Wasm memory page has a next cursor without a route")
                    .route_sha256,
            )
        })
        .transpose()?
        .map(|cursor| cursor.as_str().to_owned());
    let unobserved_route_count = statistics
        .selected_route_count
        .checked_sub(statistics.observed_route_count)
        .ok_or_else(|| {
            anyhow::anyhow!("generated Wasm observed route count exceeds its selection")
        })?;
    Ok(GeneratedWasmMemoryStatisticsResponse {
        udf_type: udf_type.to_lowercase_string().to_owned(),
        generation_sha256: statistics.generation_sha256,
        scope: "backend_process_route_record_lifetime",
        route_record_capacity: statistics.route_record_capacity,
        route_record_eviction_count: statistics.route_record_eviction_count,
        selection: GeneratedWasmMemorySelectionResponse {
            selected_route_count: statistics.selected_route_count,
            observed_route_count: statistics.observed_route_count,
            unobserved_route_count,
        },
        aggregate: statistics.aggregate,
        routes,
        pagination: PaginationMetadata {
            has_more,
            next_cursor,
        },
    })
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<QueryShadowEvidencePage> for QueryShadowEvidenceResponse {
    fn from(page: QueryShadowEvidencePage) -> Self {
        let next_cursor = page.next_cursor.map(|cursor| cursor.as_str().to_owned());
        Self {
            udf_type: page.udf_type.to_lowercase_string().to_owned(),
            direction: page.direction.metric_key().to_owned(),
            aggregate: page.aggregate.into(),
            selection: page.selection.map(Into::into),
            routes: page.routes.into_iter().map(Into::into).collect(),
            pagination: PaginationMetadata {
                has_more: next_cursor.is_some(),
                next_cursor,
            },
        }
    }
}

/// Return retention-bounded, digest-only query-shadow evidence.
///
/// Requires `DeploymentOp::ViewMetrics`. The active authenticated registry
/// supplies selected, observed, attempted, admitted, capacity-drop, completed,
/// mismatch, terminal, unobserved, and bounded generated-memory anomaly
/// evidence. Route rows are ordered by digest and paginated. The optional
/// direction selects the independently retained V8-primary or Wasm-primary
/// population. No application paths or execution payloads are read.
#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) async fn query_shadow_evidence(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(QueryShadowEvidenceArgs {
        limit,
        cursor,
        udf_type,
        direction,
    }): Query<QueryShadowEvidenceArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    const DEFAULT_LIMIT: usize = 25;
    const MAX_LIMIT: usize = 100;

    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
            "InvalidQueryShadowEvidenceLimit",
            "The query-shadow evidence limit must be between 1 and 100"
        ))
        .into());
    }
    let cursor = cursor
        .as_deref()
        .map(QueryShadowRouteKey::parse)
        .transpose()
        .map_err(|_| {
            anyhow::anyhow!(ErrorMetadata::bad_request(
                "InvalidQueryShadowEvidenceCursor",
                "The query-shadow evidence cursor is invalid"
            ))
        })?;
    let udf_type = match udf_type.as_deref().unwrap_or("query") {
        "query" => UdfType::Query,
        "mutation" => UdfType::Mutation,
        _ => {
            return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
                "InvalidQueryShadowEvidenceUdfType",
                "The query-shadow evidence UDF type must be query or mutation"
            ))
            .into());
        },
    };
    let direction = match direction.as_deref().unwrap_or("v8_primary_wasm_shadow") {
        "v8_primary_wasm_shadow" => QueryShadowDirection::V8PrimaryWasmShadow,
        "wasm_primary_v8_shadow" => QueryShadowDirection::WasmPrimaryV8Shadow,
        _ => {
            return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
                "InvalidQueryShadowEvidenceDirection",
                "The query-shadow evidence direction is invalid"
            ))
            .into());
        },
    };
    let metrics_log = st.application.metrics_log(&identity)?;
    // The registry can fail while reading deployment configuration. Do not
    // allow its diagnostic context to become an operator-facing response.
    let evidence = metrics_log
        .query_shadow_evidence_for_direction(udf_type, direction, cursor.as_ref(), limit)
        .map_err(|_| anyhow::anyhow!(ErrorMetadata::operational_internal_server_error()))?;
    Ok(Json(QueryShadowEvidenceResponse::from(evidence)))
}

/// Return cumulative generated-Wasm memory-controller statistics.
///
/// Requires `DeploymentOp::ViewMetrics`. Statistics cover every started
/// authenticated execution retained by the current backend process. Route rows
/// contain only authenticated digests and are ordered by route digest. A
/// process restart or bounded route-record eviction removes earlier history.
#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) async fn generated_wasm_memory_statistics(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Query(GeneratedWasmMemoryStatisticsArgs {
        limit,
        cursor,
        udf_type,
    }): Query<GeneratedWasmMemoryStatisticsArgs>,
) -> Result<impl IntoResponse, HttpResponseError> {
    const DEFAULT_LIMIT: usize = 25;
    const MAX_LIMIT: usize = 100;

    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
            "InvalidGeneratedWasmMemoryStatisticsLimit",
            "The generated-Wasm memory statistics limit must be between 1 and 100"
        ))
        .into());
    }
    let cursor = cursor
        .as_deref()
        .map(QueryShadowRouteKey::parse)
        .transpose()
        .map_err(|_| {
            anyhow::anyhow!(ErrorMetadata::bad_request(
                "InvalidGeneratedWasmMemoryStatisticsCursor",
                "The generated-Wasm memory statistics cursor is invalid"
            ))
        })?;
    let udf_type = match udf_type.as_deref().unwrap_or("query") {
        "query" => UdfType::Query,
        "mutation" => UdfType::Mutation,
        _ => {
            return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
                "InvalidGeneratedWasmMemoryStatisticsUdfType",
                "The generated-Wasm memory statistics UDF type must be query or mutation"
            ))
            .into());
        },
    };
    st.application.metrics_log(&identity)?;
    // Registry and controller errors can contain local paths or deployment
    // context. Keep that context out of the operator-facing response.
    let statistics =
        IsolateClient::<ProdRuntime>::static_hermes_generated_memory_statistics(udf_type)
            .map_err(|_| anyhow::anyhow!(ErrorMetadata::operational_internal_server_error()))?
            .ok_or_else(|| anyhow::anyhow!(ErrorMetadata::operational_internal_server_error()))?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.generation_sha256() != statistics.generation_sha256
            || statistics
                .routes
                .binary_search_by(|route| route.route_sha256.as_str().cmp(cursor.route_sha256()))
                .is_err()
    }) {
        return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
            "InvalidGeneratedWasmMemoryStatisticsCursor",
            "The generated-Wasm memory statistics cursor is not an observed route in the active \
             generation"
        ))
        .into());
    }
    let response =
        generated_wasm_memory_statistics_response(udf_type, statistics, cursor.as_ref(), limit)
            .map_err(|_| anyhow::anyhow!(ErrorMetadata::operational_internal_server_error()))?;
    Ok(Json(response))
}

fn validate_k(k: Option<usize>) -> anyhow::Result<usize> {
    const MIN_K: usize = 1;
    const MAX_K: usize = 25;
    const DEFAULT_K: usize = 5;

    let k = k.unwrap_or(DEFAULT_K);
    if !(MIN_K..=MAX_K).contains(&k) {
        anyhow::bail!(ErrorMetadata::bad_request(
            "InvalidTopKParameter",
            format!("k must be between {MIN_K} and {MAX_K}, got {k}")
        ));
    }
    Ok(k)
}

fn resolve_tablet_id(tablet_id_str: &str, table_mapping: &TableMapping) -> String {
    tablet_id_str
        .parse::<TabletId>()
        .ok()
        .and_then(|id| table_mapping.tablet_name(id).ok())
        .map(|name| name.to_string())
        .unwrap_or_else(|| tablet_id_str.to_string())
}

/// Resolve keys of the form "{tablet_id}" to table names.
fn resolve_tablet_keys<T>(
    timeseries: Vec<(String, T)>,
    table_mapping: &TableMapping,
) -> Vec<(String, T)> {
    timeseries
        .into_iter()
        .map(|(key, ts)| {
            if key == "_rest" {
                return (key, ts);
            }
            (resolve_tablet_id(&key, table_mapping), ts)
        })
        .collect()
}

/// Resolve keys of the form "{mutation}:{tablet_id}" to
/// "{mutation}:{table_name}".
fn resolve_mutation_tablet_keys<T>(
    timeseries: Vec<(String, T)>,
    table_mapping: &TableMapping,
) -> Vec<(String, T)> {
    timeseries
        .into_iter()
        .map(|(key, ts)| {
            if key == "_rest" {
                return (key, ts);
            }
            // The key is "{mutation}:{tablet_id}". The mutation path can
            // contain colons, so split from the right.
            if let Some(pos) = key.rfind(':') {
                let mutation = &key[..pos];
                let tablet_id_str = &key[pos + 1..];
                let table_name = resolve_tablet_id(tablet_id_str, table_mapping);
                (format!("{mutation}:{table_name}"), ts)
            } else {
                (key, ts)
            }
        })
        .collect()
}
