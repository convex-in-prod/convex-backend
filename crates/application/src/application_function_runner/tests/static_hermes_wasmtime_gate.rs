use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fs::OpenOptions,
    future::Future,
    io::Write,
    path::{
        Path,
        PathBuf,
    },
    pin::Pin,
    sync::{
        Arc,
        LazyLock,
        Mutex,
    },
    time::{
        Duration,
        Instant,
        SystemTime,
    },
};

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use common::{
    bootstrap_model::index::{
        database_index::{
            DatabaseIndexState,
            IndexedFields,
        },
        IndexConfig,
        IndexMetadata,
    },
    components::{
        CanonicalizedComponentFunctionPath,
        CanonicalizedComponentModulePath,
        ComponentId,
        ComponentPath,
        PublicFunctionPath,
    },
    execution_context::{
        ExecutionContext,
        RequestContext,
    },
    http::fetch::StaticFetchClient,
    knobs::{
        APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS,
        APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS,
    },
    log_streaming::{
        LogEvent,
        LogSender,
    },
    persistence::PersistenceReader,
    query_journal::{
        QueryJournal,
        QueryJournalLogicalIdentity,
    },
    runtime::{
        new_unlimited_rate_limiter,
        UnixTimestamp,
    },
    schemas::DatabaseSchema,
    shutdown::ShutdownSignal,
    types::{
        ActiveJavascriptClass,
        ConvexOrigin,
        DeploymentClass,
        DeploymentMetadata,
        FunctionCaller,
        IndexDescriptor,
        IndexName,
        ModuleEnvironment,
        ObjectKey,
        QueryInvocation,
        SchedulerDependencyClass,
        StorageUuid,
        UdfType,
    },
    virtual_system_mapping::VirtualSystemDocMapper,
    RequestId,
};
use database::{
    Database,
    IndexModel,
    PatchValue,
    ResolvedQuery,
    SystemMetadataModel,
    TableModel,
    Transaction,
    UserFacingModel,
};
use events::usage::NoOpUsageEventLogger;
use file_storage::TransactionalFileStorage;
use function_runner::{
    in_process_function_runner::InProcessFunctionRunner,
    server::DeploymentStorage,
    FunctionExecutionMode,
    FunctionRunner,
    QueryShadowDirection,
    QueryShadowRouteKey,
};
use futures::TryStreamExt;
use indexing::index_cache::IndexCache;
use isolate::{
    cleanup_static_hermes_test_instances,
    install_static_hermes_gate_test_hooks,
    StaticHermesGateTestHooks,
    StaticHermesGeneratedExecutionPhaseObservation,
    StaticHermesGeneratedInvocationObservation,
};
use keybroker::{
    DeploymentSecret,
    Identity,
    KeyBroker,
    UserIdentity,
};
use model::{
    config::{
        module_loader::ModuleLoader,
        types::{
            node_executor_pool_topology,
            ModuleConfig,
        },
    },
    file_storage::{
        types::FileStorageEntry,
        FileStorageId,
        FileStorageModel,
    },
    initialize_application_system_tables,
    modules::{
        function_validators::{
            ArgsValidator,
            ReturnsValidator,
        },
        module_versions::{
            AnalyzedFunction,
            AnalyzedModule,
            FullModuleSource,
            ModuleSource,
            SourceMap,
            Visibility,
        },
        types::ModuleMetadata,
        ModuleModel,
    },
    scheduled_jobs::{
        types::ScheduledJobState,
        virtual_table::{
            PublicScheduledJob,
            ScheduledJobsDocMapper,
        },
    },
    source_packages::{
        types::{
            NodeExecutorPoolTopology,
            PackageSize,
            SourcePackage,
            SourcePackageRuntimeGeneration,
        },
        upload_download::download_package,
        SourcePackageModel,
    },
    udf_config::{
        types::UdfConfig,
        UdfConfigModel,
    },
};
use node_executor::{
    noop::NoopNodeExecutor,
    NodeActions,
};
use runtime::prod::ProdRuntime;
use search::searcher::SearcherStub;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    json,
    Value as JsonValue,
};
use sqlite::SqlitePersistence;
use storage::{
    LocalDirStorage,
    Storage,
    StorageExt,
    Upload,
};
use sync_types::{
    types::SerializedArgs,
    CanonicalizedModulePath,
    CanonicalizedUdfPath,
};
use usage_tracking::UsageCounter;
use value::{
    obj,
    sha256::{
        Sha256,
        Sha256Digest,
    },
    DeveloperDocumentId,
    InternalId,
    TableName,
    TableNamespace,
};

use super::super::{
    ApplicationFunctionRunner,
    DatabaseFunctionKind,
    QueryCache,
};
use crate::{
    audit_logging::AuditLogClient,
    deploy_config::ModuleJson,
    function_log::{
        FunctionExecutionLog,
        QueryShadowEvidencePage,
        QueryShadowEvidenceSummary,
        QueryShadowMismatchCounts,
        QueryShadowRegistrySelection,
        QueryShadowRouteEvidence,
        QueryShadowTimingSummary,
    },
};

const DEPLOYMENT_MANIFEST_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_DEPLOYMENT_MANIFEST";
const REAL_RUNNER_TEST_EXPECTATION_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_RUNNER_TEST_EXPECTATION";
const NORMAL_ROUTE_TEST_EXPECTATION_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_NORMAL_ROUTE_TEST_EXPECTATION";
const NORMAL_ROUTE_TEST_EXPECTATION_FILE_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_NORMAL_ROUTE_TEST_EXPECTATION_FILE";
const NORMAL_ROUTE_TEST_EXPECTATION_FILE_SHA256_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_NORMAL_ROUTE_TEST_EXPECTATION_FILE_SHA256";
const NORMAL_ROUTE_TEST_EXPECTATION_FILE_SIZE_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_NORMAL_ROUTE_TEST_EXPECTATION_FILE_SIZE";
// This applies only to the ignored local diagnostic test. It changes
// a Store budget after artifact authentication and is never read by
// the production route configuration.
const NORMAL_ROUTE_TEST_EXECUTION_FUEL_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_TEST_EXECUTION_FUEL";
// This applies only to the ignored local diagnostic test. It preserves
// the authenticated generated-runtime platform limit while allowing
// that test process to give its V8 primary more cold-start time.
const NORMAL_ROUTE_TEST_GENERATED_RUNTIME_EXECUTION_TIME_MS_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_TEST_GENERATED_RUNTIME_EXECUTION_TIME_MS";
const NORMAL_ROUTE_WARM_BENCHMARK_ITERATIONS_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_WARM_BENCHMARK_ITERATIONS";
const NORMAL_ROUTE_WARM_BENCHMARK_REPORT_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_WARM_BENCHMARK_REPORT";
const NORMAL_ROUTE_WARM_BENCHMARK_ITERATIONS: usize = 1_000;
const NORMAL_ROUTE_TEST_AUTHENTICATED_EXECUTION_TIME_MS: u64 = 1_000;
const NORMAL_ROUTE_TEST_PHASE_TIMEOUT: Duration = Duration::from_secs(120);
const NORMAL_ROUTE_SHADOW_PAGE_LIMIT: usize = 256;
const STATIC_HERMES_GATE_TEST_INSTANCE_NAME: &str = "static-hermes-gate-test";
const NORMAL_ROUTE_TEST_DEPLOYMENT_SECRET_DOMAIN: &[u8] =
    b"convex-static-hermes-normal-route-deployment-secret-v1\0";
static GATE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static NORMAL_ROUTE_SCHEMA_CACHE: LazyLock<Mutex<BTreeMap<String, Arc<DatabaseSchema>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

struct NormalRouteWarmBenchmark {
    iterations: usize,
    report_path: PathBuf,
}

fn normal_route_warm_benchmark(
    expectation: &NormalRouteTestExpectation,
) -> anyhow::Result<Option<NormalRouteWarmBenchmark>> {
    let iterations = std::env::var(NORMAL_ROUTE_WARM_BENCHMARK_ITERATIONS_ENV);
    let report_path = std::env::var(NORMAL_ROUTE_WARM_BENCHMARK_REPORT_ENV);
    let (iterations, report_path) = match (iterations, report_path) {
        (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => {
            return Ok(None);
        },
        (Ok(iterations), Ok(report_path)) => (iterations, report_path),
        (Err(std::env::VarError::NotPresent), Ok(_))
        | (Ok(_), Err(std::env::VarError::NotPresent)) => anyhow::bail!(
            "{NORMAL_ROUTE_WARM_BENCHMARK_ITERATIONS_ENV} and \
             {NORMAL_ROUTE_WARM_BENCHMARK_REPORT_ENV} must be set together"
        ),
        (Err(error), _) => {
            return Err(error).with_context(|| {
                format!("failed to read {NORMAL_ROUTE_WARM_BENCHMARK_ITERATIONS_ENV}")
            })
        },
        (_, Err(error)) => {
            return Err(error).with_context(|| {
                format!("failed to read {NORMAL_ROUTE_WARM_BENCHMARK_REPORT_ENV}")
            })
        },
    };
    let iterations = iterations
        .parse::<usize>()
        .with_context(|| format!("{NORMAL_ROUTE_WARM_BENCHMARK_ITERATIONS_ENV} must be a usize"))?;
    anyhow::ensure!(
        iterations == NORMAL_ROUTE_WARM_BENCHMARK_ITERATIONS,
        "{NORMAL_ROUTE_WARM_BENCHMARK_ITERATIONS_ENV} must equal \
         {NORMAL_ROUTE_WARM_BENCHMARK_ITERATIONS}"
    );
    anyhow::ensure!(
        matches!(
            expectation.lane,
            NormalRouteTestLane::V8 | NormalRouteTestLane::Wasm
        ),
        "warm benchmark is only valid for the V8 or Wasm lane"
    );
    anyhow::ensure!(
        expectation.route.runtime_module_path == "API/apiKeys.js"
            && expectation.route.export_name == "validateApiKey"
            && matches!(
                &expectation.setup,
                NormalRouteTestSetup::ApiKeyValidation { .. }
            ),
        "warm benchmark requires the API/apiKeys.js:validateApiKey fixture"
    );
    let report_path = PathBuf::from(report_path);
    anyhow::ensure!(
        report_path.is_absolute(),
        "{NORMAL_ROUTE_WARM_BENCHMARK_REPORT_ENV} must be an absolute path"
    );
    Ok(Some(NormalRouteWarmBenchmark {
        iterations,
        report_path,
    }))
}

fn configure_normal_route_execution_fuel(hooks: &StaticHermesGateTestHooks) -> anyhow::Result<()> {
    let value = match std::env::var(NORMAL_ROUTE_TEST_EXECUTION_FUEL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read {NORMAL_ROUTE_TEST_EXECUTION_FUEL_ENV}"));
        },
    };
    let fuel = value.parse::<u64>().with_context(|| {
        format!("{NORMAL_ROUTE_TEST_EXECUTION_FUEL_ENV} must be a positive u64")
    })?;
    hooks.set_generated_execution_fuel_override(fuel)
}

fn configure_normal_route_generated_runtime_execution_time(
    lane: NormalRouteTestLane,
    hooks: &StaticHermesGateTestHooks,
) -> anyhow::Result<()> {
    let value = std::env::var(NORMAL_ROUTE_TEST_GENERATED_RUNTIME_EXECUTION_TIME_MS_ENV);
    match (lane, value) {
        (NormalRouteTestLane::Shadow, Ok(value)) => {
            let milliseconds = value.parse::<u64>().with_context(|| {
                format!(
                    "{NORMAL_ROUTE_TEST_GENERATED_RUNTIME_EXECUTION_TIME_MS_ENV} must be a \
                     positive u64"
                )
            })?;
            anyhow::ensure!(
                milliseconds == NORMAL_ROUTE_TEST_AUTHENTICATED_EXECUTION_TIME_MS,
                "{NORMAL_ROUTE_TEST_GENERATED_RUNTIME_EXECUTION_TIME_MS_ENV} must equal the \
                 authenticated execution-time limit"
            );
            hooks.set_generated_runtime_execution_time_limit_override(Duration::from_millis(
                milliseconds,
            ))
        },
        (NormalRouteTestLane::Shadow, Err(std::env::VarError::NotPresent)) => {
            anyhow::bail!(
                "{NORMAL_ROUTE_TEST_GENERATED_RUNTIME_EXECUTION_TIME_MS_ENV} is required for the \
                 shadow test lane"
            )
        },
        (
            NormalRouteTestLane::V8 | NormalRouteTestLane::Wasm,
            Err(std::env::VarError::NotPresent),
        ) => Ok(()),
        (_, Err(error)) => Err(error).with_context(|| {
            format!("failed to read {NORMAL_ROUTE_TEST_GENERATED_RUNTIME_EXECUTION_TIME_MS_ENV}")
        }),
        (NormalRouteTestLane::V8 | NormalRouteTestLane::Wasm, Ok(_)) => anyhow::bail!(
            "{NORMAL_ROUTE_TEST_GENERATED_RUNTIME_EXECUTION_TIME_MS_ENV} is only valid for the \
             shadow test lane"
        ),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealRunnerDeploymentManifest {
    deployment_sha256: String,
    exports: Vec<RealRunnerDeploymentExport>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealRunnerDeploymentExport {
    export_name: String,
    package_reference: Option<RealRunnerPackageReference>,
    routing: RealRunnerRouting,
    runtime_module_path: String,
    udf_kind: String,
    visibility: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealRunnerPackageReference {
    cache_key: String,
}

#[derive(Deserialize)]
struct RealRunnerRouting {
    decision: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealRunnerTestExpectation {
    arguments: Vec<JsonValue>,
    deployment_sha256: String,
    expected_error_contains: String,
    package_key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteTestExpectation {
    deployment_sha256: String,
    generation_manifest_sha256: String,
    generation_sha256: String,
    lane: NormalRouteTestLane,
    observation_path: PathBuf,
    #[serde(default)]
    paired_fixture_sha256: Option<String>,
    package_key: String,
    route: NormalRouteTestRoute,
    routing: NormalRouteTestRouting,
    setup: NormalRouteTestSetup,
    source: NormalRouteTestSource,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRoutePairedLaneBatchExpectation {
    cases: Vec<NormalRoutePairedLaneBatchCase>,
    kind: String,
    observation_path: PathBuf,
    progress_path: PathBuf,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRoutePairedLaneBatchCase {
    case_id: String,
    expectation: NormalRouteTestExpectation,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneBatchProgress {
    active_case: Option<NormalRoutePairedLaneBatchProgressCase>,
    batch_kind: &'static str,
    completed_case_ids: Vec<String>,
    expected_case_ids: Vec<String>,
    kind: &'static str,
    lane: NormalRouteTestLane,
    schema_version: u16,
    status: &'static str,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneBatchProgressCase {
    actual_package_key: Option<String>,
    case_id: String,
    expected_cohort_case_ids: Vec<String>,
    expected_package_key: String,
    route: NormalRouteTestRoute,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteSequenceTestExpectation {
    deployment_sha256: String,
    generation_manifest_sha256: String,
    generation_sha256: String,
    lane: NormalRouteTestLane,
    observation_path: PathBuf,
    sequence: NormalRouteTestSequence,
    source: NormalRouteSequenceTestSource,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum NormalRouteTestLane {
    V8,
    Wasm,
    Shadow,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteTestRoute {
    export_name: String,
    runtime_module_path: String,
    udf_kind: String,
    visibility: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteTestRouting {
    entry_id: String,
    entry_selector_id: String,
    route_id: String,
    route_identity: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteTestSequence {
    application_indexes: Vec<NormalRouteSequenceApplicationIndex>,
    expected_capability_stages: Vec<String>,
    expected_committed_state: NormalRouteSequenceExpectedCommittedState,
    #[serde(skip_serializing_if = "Option::is_none")]
    invocation_identity: Option<NormalRouteSequenceInvocationIdentity>,
    kind: String,
    steps: Vec<NormalRouteSequenceStep>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum NormalRouteSequenceInvocationIdentity {
    SeededUser {
        role: String,
        user_id: String,
        user_table: String,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
enum NormalRouteSequenceExpectedCommittedState {
    ApplicationProbePatch {
        #[serde(rename = "applicationTable")]
        application_table: String,
        normalized: JsonValue,
        #[serde(rename = "scheduledFunctionAddress")]
        scheduled_function_address: String,
    },
    Tagged(NormalRouteSequenceTaggedExpectedCommittedState),
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum NormalRouteSequenceTaggedExpectedCommittedState {
    EmptyDatabase {
        normalized: JsonValue,
        tables: Vec<String>,
    },
    EmptyTables {
        normalized: JsonValue,
        result_fields: BTreeMap<String, String>,
        synthetic_rows: Vec<JsonValue>,
    },
    TableDocuments {
        documents: Vec<JsonValue>,
        normalized: JsonValue,
        table: String,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteSequenceApplicationIndex {
    fields: Vec<String>,
    index: String,
    table: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteSequenceStep {
    arguments: JsonValue,
    entry_id: String,
    entry_selector_id: String,
    expected_result: JsonValue,
    package_key: String,
    route: NormalRouteTestRoute,
    route_id: String,
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum NormalRouteTestSetup {
    ApplicationProbe {
        application_index: String,
        application_index_fields: Vec<String>,
        application_table: String,
        user_table: String,
    },
    ApplicationProbePatch {
        application_index: String,
        application_index_fields: Vec<String>,
        application_table: String,
        scheduled_function_address: String,
    },
    ApiKeyValidation {
        application_index: String,
        application_index_fields: Vec<String>,
        application_table: String,
        first_api_key: String,
        second_api_key: String,
    },
    CollectDelete {
        expected_counts: Vec<u64>,
        result_fields: BTreeMap<String, String>,
        synthetic_rows: Vec<JsonValue>,
    },
    EmptyIndexedQueryV1 {
        application_index: String,
        application_index_fields: Vec<String>,
        application_table: String,
        expected_result: JsonValue,
        invocation_arguments: JsonValue,
    },
    SeededDocumentsQueryV1 {
        application_index: String,
        application_index_fields: Vec<String>,
        application_table: String,
        documents: Vec<JsonValue>,
        expected_result: JsonValue,
        invocation_arguments: JsonValue,
    },
    FileStorageGetUrlV1 {
        content_base64: String,
        content_type: String,
        expected_result: String,
        invocation_arguments: JsonValue,
        storage_uuid: String,
    },
    IndexedPaginationQueryV1 {
        application_index: String,
        application_index_fields: Vec<String>,
        application_table: String,
        invocations: Vec<NormalRouteIndexedPaginationInvocation>,
        seeded_rows: Vec<NormalRouteObjectFixtureRow>,
        warmup_invocation_count: usize,
    },
    ObjectPatchDeleteV1 {
        deleted_fields: Vec<String>,
        expected_results: Vec<JsonValue>,
        invocation_arguments: JsonValue,
        restore_fields_before_measured_invocation: Option<JsonValue>,
        seeded_rows: Vec<NormalRouteObjectFixtureRow>,
        target_alias: String,
    },
    SeededDocumentsMutationV1 {
        application_index: String,
        application_index_fields: Vec<String>,
        application_table: String,
        documents: Vec<JsonValue>,
        expected_result: JsonValue,
        invocation_arguments: JsonValue,
        #[serde(default)]
        expected_read_accounting: Option<NormalRouteExpectedReadAccounting>,
    },
    PairedLaneV1 {
        invocations: Vec<JsonValue>,
        #[serde(
            default,
            deserialize_with = "deserialize_normal_route_paired_lane_missing_ids"
        )]
        missing_ids: Option<Vec<NormalRoutePairedLaneMissingId>>,
        #[serde(default)]
        opaque_cursors: Vec<NormalRoutePairedLaneOpaqueCursor>,
        seeded_rows: Vec<NormalRouteObjectFixtureRow>,
    },
    StatefulQueryPatchV1 {
        invocations: Vec<NormalRouteStatefulQueryPatchInvocation>,
        post_state_field_policies:
            BTreeMap<String, BTreeMap<String, NormalRoutePostStateFieldPolicy>>,
        seeded_rows: Vec<NormalRouteObjectFixtureRow>,
        warmup_invocation_count: usize,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteObjectFixtureRow {
    alias: String,
    table: String,
    value: JsonValue,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRoutePairedLaneMissingId {
    path: Vec<String>,
    table_name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRoutePairedLaneOpaqueCursor {
    capture_invocation: usize,
    capture_result_path: Vec<String>,
    id: String,
}

fn deserialize_normal_route_paired_lane_missing_ids<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<NormalRoutePairedLaneMissingId>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let missing_ids = Vec::<NormalRoutePairedLaneMissingId>::deserialize(deserializer)?;
    if missing_ids.is_empty() {
        return Err(serde::de::Error::custom(
            "paired-lane missing ID metadata must not be empty",
        ));
    }
    Ok(Some(missing_ids))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteStatefulQueryPatchInvocation {
    arguments: JsonValue,
    expected_capability_stages: Vec<String>,
    expected_result: JsonValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteIndexedPaginationInvocation {
    arguments: JsonValue,
    expected_result: JsonValue,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum NormalRoutePostStateFieldPolicy {
    Absent,
    IncreasedNumber,
    Preserved,
}

enum NormalRouteFixtureState {
    ApplicationProbe {
        user_id: value::DeveloperDocumentId,
    },
    ApplicationProbePatch,
    ApiKeyValidation {
        first_document_id: value::DeveloperDocumentId,
        second_document_id: value::DeveloperDocumentId,
        first_api_key: String,
        second_api_key: String,
    },
    CollectDelete {
        expected_counts: Vec<u64>,
        result_fields: BTreeMap<String, TableName>,
        synthetic_rows: Vec<JsonValue>,
    },
    EmptyIndexedQueryV1(NormalRouteEmptyIndexedQueryState),
    SeededDocumentsQueryV1(NormalRouteSeededDocumentsQueryState),
    FileStorageGetUrlV1(NormalRouteFileStorageState),
    IndexedPaginationQueryV1(NormalRouteIndexedPaginationState),
    ObjectPatchDeleteV1(NormalRouteObjectPatchDeleteState),
    SeededDocumentsMutationV1(NormalRouteSeededDocumentsMutationState),
    PairedLaneV1(NormalRoutePairedLaneState),
    StatefulQueryPatchV1(NormalRouteStatefulQueryPatchState),
}

struct NormalRouteValidatedFileStorageSetup {
    content: Bytes,
    content_type: String,
    expected_result: JsonValue,
    invocation_arguments: JsonValue,
    storage_uuid: StorageUuid,
}

struct NormalRouteFileStorageState {
    content: Bytes,
    developer_id: value::DeveloperDocumentId,
    entry: FileStorageEntry,
    expected_result: JsonValue,
    invocation_arguments: JsonValue,
}

struct NormalRouteEmptyIndexedQueryState {
    application_table: TableName,
    expected_result: JsonValue,
    invocation_arguments: JsonValue,
    normalized_committed_state: JsonValue,
}

struct NormalRouteSeededDocumentsQueryState {
    expected_result: JsonValue,
    invocation_arguments: JsonValue,
}

struct NormalRouteIndexedPaginationState {
    invocations: Vec<NormalRouteIndexedPaginationInvocationState>,
    normalized_committed_state: JsonValue,
    rows: Vec<NormalRouteObjectFixtureRowState>,
    warmup_invocation_count: usize,
}

struct NormalRouteIndexedPaginationInvocationState {
    arguments: JsonValue,
    expected_result: JsonValue,
    normalized_expected_result: JsonValue,
}

struct NormalRouteObjectPatchDeleteState {
    deleted_fields: BTreeSet<String>,
    expected_results: Vec<JsonValue>,
    invocation_arguments: JsonValue,
    normalized_committed_state: JsonValue,
    normalized_expected_results: Vec<JsonValue>,
    restore_fields_before_measured_invocation: Option<value::ConvexObject>,
    rows: Vec<NormalRouteObjectFixtureRowState>,
    target_document_id: value::DeveloperDocumentId,
}

struct NormalRouteSeededDocumentsMutationState {
    document_ids: Vec<value::DeveloperDocumentId>,
    expected_result: JsonValue,
    expected_read_accounting: Option<NormalRouteExpectedReadAccounting>,
    invocation_arguments: JsonValue,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum NormalRouteExpectedReadAccounting {
    None,
}

struct NormalRouteObjectFixtureRowState {
    alias: String,
    document_id: value::DeveloperDocumentId,
    expected_value: value::ConvexObject,
}

struct NormalRoutePairedLaneState {
    fixture_ids: BTreeMap<String, value::DeveloperDocumentId>,
    invocations: Vec<JsonValue>,
    missing_id_bindings: Vec<NormalRoutePairedLaneMissingId>,
    opaque_cursors: Vec<NormalRoutePairedLaneOpaqueCursor>,
}

struct NormalRoutePairedLaneExecutionOptions {
    case_id: Option<String>,
    cleanup_after: bool,
    require_cold_start: bool,
    verify_provenance: bool,
}

impl NormalRoutePairedLaneExecutionOptions {
    const ONE_CASE: Self = Self {
        case_id: None,
        cleanup_after: true,
        require_cold_start: true,
        verify_provenance: true,
    };
}

struct NormalRoutePairedLaneHookCounts {
    capability_operation_stages: usize,
    database_query_starts: usize,
    execution_errors: usize,
    instance_ids: usize,
    pool_hits: usize,
    pool_misses: usize,
    pool_events: usize,
    read_bytes: usize,
    read_documents: usize,
    read_intervals: usize,
    reads_cancelled: usize,
    reads_completed: usize,
    runtime_ids: usize,
    runtime_invocations: usize,
    store_drops: usize,
    teardowns: usize,
    transaction_drops: usize,
}

fn normal_route_paired_lane_hook_counts(
    hooks: &StaticHermesGateTestHooks,
) -> NormalRoutePairedLaneHookCounts {
    NormalRoutePairedLaneHookCounts {
        capability_operation_stages: hooks.guest_native_completion_stages().len(),
        database_query_starts: hooks.database_query_starts(),
        execution_errors: hooks.execution_errors().len(),
        instance_ids: hooks.instance_ids().len(),
        pool_hits: hooks.generated_pool_hits(),
        pool_misses: hooks.generated_pool_misses(),
        pool_events: hooks.generated_pool_events().len(),
        read_bytes: hooks.read_bytes(),
        read_documents: hooks.read_documents(),
        read_intervals: hooks.read_intervals(),
        reads_cancelled: hooks.read_cancelled(),
        reads_completed: hooks.read_completed(),
        runtime_ids: hooks.generated_runtime_ids().len(),
        runtime_invocations: hooks.generated_invocations().len(),
        store_drops: hooks.store_drops(),
        teardowns: hooks.teardowns(),
        transaction_drops: hooks.transaction_drops(),
    }
}

fn normal_route_paired_lane_counter_delta(
    after: usize,
    before: usize,
    counter: &str,
) -> anyhow::Result<usize> {
    after
        .checked_sub(before)
        .with_context(|| format!("paired-lane {counter} counter moved backwards"))
}

fn normal_route_paired_lane_observation_counter(
    observation: &JsonValue,
    field: &str,
) -> anyhow::Result<usize> {
    observation
        .get(field)
        .and_then(JsonValue::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .with_context(|| format!("paired-lane observation omitted {field}"))
}

fn normal_route_paired_lane_observation_runtime_ids(
    observation: &JsonValue,
) -> anyhow::Result<BTreeSet<u64>> {
    observation
        .get("runtimeInvocations")
        .and_then(JsonValue::as_array)
        .context("paired-lane observation omitted runtime invocations")?
        .iter()
        .map(|invocation| {
            invocation
                .get("runtimeId")
                .and_then(JsonValue::as_u64)
                .context("paired-lane runtime invocation omitted its runtime identity")
        })
        .collect()
}

fn normal_route_paired_lane_observed_package_key(
    observation: &JsonValue,
    lane: NormalRouteTestLane,
) -> anyhow::Result<Option<String>> {
    match lane {
        NormalRouteTestLane::V8 => Ok(None),
        NormalRouteTestLane::Wasm => {
            let invocations = observation
                .get("runtimeInvocations")
                .and_then(JsonValue::as_array)
                .context("paired-lane observation omitted runtime invocations")?;
            let mut package_keys = invocations.iter().map(|invocation| {
                invocation
                    .get("packageKey")
                    .and_then(JsonValue::as_str)
                    .context("paired-lane runtime invocation omitted packageKey")
            });
            let first = package_keys
                .next()
                .transpose()?
                .context("paired-lane observation has no observed runtime package")?;
            for package_key in package_keys {
                anyhow::ensure!(
                    package_key? == first,
                    "paired-lane runtime invocations observed different packages"
                );
            }
            let _ = normal_route_sha256(first, "paired-lane observed package identity")?;
            Ok(Some(first.to_owned()))
        },
        NormalRouteTestLane::Shadow => {
            anyhow::bail!("query-shadow normal-route test does not support paired batches")
        },
    }
}

struct NormalRouteStatefulQueryPatchState {
    invocations: Vec<NormalRouteStatefulQueryPatchInvocationState>,
    normalized_committed_state: JsonValue,
    rows: Vec<NormalRouteStatefulQueryPatchRowState>,
    warmup_invocation_count: usize,
}

struct NormalRouteStatefulQueryPatchInvocationState {
    arguments: JsonValue,
    expected_capability_stages: Vec<String>,
    expected_result: JsonValue,
    normalized_expected_result: JsonValue,
}

struct NormalRouteStatefulQueryPatchRowState {
    alias: String,
    document_id: value::DeveloperDocumentId,
    initial_value: value::ConvexObject,
    post_state_field_policies: BTreeMap<String, NormalRoutePostStateFieldPolicy>,
}

struct NormalRouteApplicationProbePatchState {
    owner_id: String,
    scheduled_function_id: String,
    normalized: JsonValue,
}

enum NormalRouteSequenceFixtureState {
    ApplicationProbePatch {
        user_document_id: Option<value::DeveloperDocumentId>,
    },
    EmptyTables {
        result_fields: BTreeMap<String, TableName>,
        seeded_document_ids: Vec<value::DeveloperDocumentId>,
        user_document_id: Option<value::DeveloperDocumentId>,
    },
    EmptyDatabase {
        tables: Vec<TableName>,
        user_document_id: Option<value::DeveloperDocumentId>,
    },
    TableDocuments {
        table: TableName,
        user_document_id: Option<value::DeveloperDocumentId>,
    },
}

struct NormalRouteScheduledModule {
    export_name: String,
    module_path: CanonicalizedModulePath,
    source: Arc<FullModuleSource>,
    udf_type: UdfType,
    visibility: Visibility,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteTestSource {
    #[serde(default)]
    additional_analyzed_exports: Vec<NormalRouteAdditionalAnalyzedExport>,
    bundle: NormalRouteSourceBundleIdentity,
    module_sha256: String,
    module_source: String,
    package: NormalRouteSourcePackage,
    scheduled_module: Option<NormalRouteScheduledSourceModule>,
    source_map: Option<SourceMap>,
    source_package_sha256: String,
    source_package_runtime_content_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteAdditionalAnalyzedExport {
    export_name: String,
    runtime_module_path: String,
    udf_kind: String,
    visibility: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteSequenceTestSource {
    bundle: NormalRouteSourceBundleIdentity,
    modules: Vec<NormalRouteSequenceSourceModule>,
    package: NormalRouteSourcePackage,
    scheduled_module: Option<NormalRouteScheduledSourceModule>,
    source_package_sha256: String,
    source_package_runtime_content_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteScheduledSourceModule {
    environment: String,
    export_name: String,
    module_sha256: String,
    module_source: String,
    runtime_module_path: String,
    source_map: Option<SourceMap>,
    udf_kind: String,
    visibility: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteSequenceSourceModule {
    module_sha256: String,
    module_source: String,
    runtime_module_path: String,
    source_map: Option<SourceMap>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteSourcePackage {
    path: PathBuf,
    sha256: String,
    size: usize,
}

// A paired batch may reuse the immutable, authenticated package bytes, but
// never the database or runner that consumes them. Each case still
// installs its own source-package metadata and fixture into a new
// database.
struct NormalRouteSharedSourcePackage {
    source_package: SourcePackage,
    storage: Arc<dyn Storage>,
}

// A paired batch can also share its parsed, immutable source bundle. The module
// map keeps Arc values, while each case retains its own runner and
// database state.
struct NormalRouteSharedSourceBundle {
    modules: Arc<BTreeMap<CanonicalizedModulePath, Arc<FullModuleSource>>>,
    sha256: Sha256Digest,
    size: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NormalRouteSourceBundleIdentity {
    path: PathBuf,
    sha256: String,
    size: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NormalRouteSourceBundle {
    app_definition: NormalRouteSourceBundleApplication,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NormalRouteSourceBundleApplication {
    changed_modules: Vec<ModuleJson>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRouteStructuralObservation {
    kind: &'static str,
    schema_version: u16,
    deployment_sha256: String,
    generation_sha256: String,
    lane: NormalRouteTestLane,
    package_key: String,
    route: NormalRouteTestRoute,
    warmup_invocation_count: usize,
    invocation_count: usize,
    query_journals: Vec<NormalRouteQueryJournalObservation>,
    normalized_results: Vec<JsonValue>,
    normalized_committed_state: Option<JsonValue>,
    database_query_starts: usize,
    reads_completed: usize,
    read_documents: usize,
    read_bytes: usize,
    read_intervals: usize,
    runtime_observation_count: usize,
    unique_runtime_count: usize,
    unique_memory_slot_count: usize,
    runtime_invocations: Vec<NormalRouteRuntimeInvocation>,
    pool_hits: usize,
    pool_misses: usize,
    measured_pool_hits: usize,
    measured_pool_misses: usize,
    measured_fresh_runtime_admissions: usize,
    capability_operation_stages: Vec<&'static str>,
    provenance_rejections: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_shadow: Option<NormalRouteShadowObservation>,
    cleaned_up_instances: usize,
    store_drops_after_cleanup: usize,
    teardowns_after_cleanup: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRouteShadowObservation {
    generation_sha256: String,
    route_id: String,
    primary_wasm_routing_enabled: bool,
    attempt_count: u64,
    admitted_count: u64,
    completed_comparison_count: u64,
    mismatch_count: u64,
    mismatch_counts: NormalRouteShadowMismatchObservation,
    terminal_count: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRouteShadowMismatchObservation {
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
}

impl From<QueryShadowMismatchCounts> for NormalRouteShadowMismatchObservation {
    fn from(counts: QueryShadowMismatchCounts) -> Self {
        Self {
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
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneObservation {
    kind: &'static str,
    schema_version: u16,
    deployment_sha256: String,
    generation_sha256: String,
    lane: NormalRouteTestLane,
    paired_fixture_sha256: String,
    table_mapping_sha256: String,
    package_key: String,
    route: NormalRouteTestRoute,
    comparison_evidence: NormalRoutePairedLaneComparisonEvidence,
    comparison_dimensions: NormalRoutePairedLaneComparisonDimensions,
    invocation_count: usize,
    outcomes: Vec<NormalRoutePairedLaneOutcome>,
    query_journals: Vec<NormalRouteQueryJournalObservation>,
    normalized_committed_state: JsonValue,
    database_query_starts: usize,
    reads_completed: usize,
    reads_cancelled: usize,
    read_documents: usize,
    read_bytes: usize,
    read_intervals: usize,
    transaction_drops: usize,
    execution_error_count: usize,
    runtime_observation_count: usize,
    unique_runtime_count: usize,
    unique_memory_slot_count: usize,
    runtime_invocations: Vec<NormalRouteRuntimeInvocation>,
    pool_hits: usize,
    pool_misses: usize,
    measured_pool_hits: usize,
    measured_pool_misses: usize,
    measured_fresh_runtime_admissions: usize,
    capability_operation_stages: Vec<&'static str>,
    provenance_rejections: usize,
    cleaned_up_instances: usize,
    store_drops_after_cleanup: usize,
    teardowns_after_cleanup: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneComparisonDimensions {
    committed_root_user_table_post_state: &'static str,
    external_effects: &'static str,
    outcomes: &'static str,
    query_journal_end_cursor: &'static str,
    query_journal_full: &'static str,
    scheduler_system_state: &'static str,
    transaction_write_set: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneComparisonEvidence {
    external_effects: NormalRoutePairedLaneNotApplicableEvidence,
    scheduler_system_state: NormalRoutePairedLaneNotApplicableEvidence,
    transaction_write_sets: Vec<Vec<NormalRoutePairedLaneWriteIntent>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneNotApplicableEvidence {
    host_operation_traces: Vec<NormalRoutePairedLaneHostOperationTrace>,
    kind: &'static str,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneHostOperationTrace {
    entries: Vec<NormalRoutePairedLaneHostOperationTraceEntry>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneHostOperationTraceEntry {
    operation: NormalRoutePairedLaneHostOperation,
    status: NormalRoutePairedLaneHostOperationStatus,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum NormalRoutePairedLaneHostOperation {
    AuditLog,
    CancelJob,
    ComponentArgument,
    CreateFunctionHandle,
    DatabaseCount,
    DatabaseDelete,
    DatabaseGet,
    DatabaseInsert,
    DatabaseNormalizeId,
    DatabasePatch,
    DatabaseQueryPage,
    DatabaseQueryCleanup,
    DatabaseQueryStream,
    DatabaseQueryStreamNext,
    DatabaseReplace,
    DeploymentMetadata,
    FunctionMetadata,
    RequestMetadata,
    RequireOperation,
    RunUdf,
    Schedule,
    SnapshotTimestamp,
    StorageDelete,
    StorageGenerateUploadUrl,
    StorageGetMetadata,
    StorageGetUrl,
    ThrowOcc,
    ThrowOverloaded,
    TransactionMetrics,
    UserIdentity,
    WriteDeploymentAuditLog,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
enum NormalRoutePairedLaneHostOperationStatus {
    Success,
    Failure,
}

const PAIRED_LANE_EXTERNAL_EFFECT_OPERATIONS: &[NormalRoutePairedLaneHostOperation] = &[
    NormalRoutePairedLaneHostOperation::AuditLog,
    NormalRoutePairedLaneHostOperation::StorageDelete,
    NormalRoutePairedLaneHostOperation::StorageGenerateUploadUrl,
    NormalRoutePairedLaneHostOperation::WriteDeploymentAuditLog,
];
const PAIRED_LANE_SCHEDULER_SYSTEM_STATE_OPERATIONS: &[NormalRoutePairedLaneHostOperation] = &[
    NormalRoutePairedLaneHostOperation::CancelJob,
    NormalRoutePairedLaneHostOperation::Schedule,
];

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneWriteIntent {
    new_document_sha256: Option<String>,
    old_document_sha256: Option<String>,
    operation: NormalRoutePairedLaneWriteOperation,
    table: String,
}

#[derive(Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
enum NormalRoutePairedLaneWriteOperation {
    Delete,
    Insert,
    Update,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneBatchObservation {
    kind: &'static str,
    schema_version: u16,
    lane: NormalRouteTestLane,
    cases: Vec<NormalRoutePairedLaneBatchObservationCase>,
    cohort_mechanisms: Vec<NormalRoutePairedLaneCohortMechanism>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneBatchObservationCase {
    case_id: String,
    observation: JsonValue,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRoutePairedLaneCohortMechanism {
    case_ids: Vec<String>,
    deployment_sha256: String,
    generation_sha256: String,
    invocation_count: usize,
    lane: NormalRouteTestLane,
    package_key: String,
    pooled_runtime_count: usize,
    provenance_rejections: usize,
    runtime_admissions: usize,
    teardown_count: usize,
}

#[derive(Serialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
enum NormalRoutePairedLaneOutcome {
    Error { error: NormalRoutePairedLaneError },
    Returned { value: JsonValue },
}

#[derive(Serialize)]
#[serde(
    tag = "category",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
enum NormalRoutePairedLaneError {
    MissingDocument {
        operation: NormalRoutePairedLaneMissingDocumentOperation,
        missing_id_bindings: Vec<NormalRoutePairedLaneMissingId>,
    },
    Unclassified {
        source: NormalRoutePairedLaneUnclassifiedErrorSource,
    },
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
enum NormalRoutePairedLaneMissingDocumentOperation {
    Patch,
    Replace,
    Delete,
}

impl From<udf::HostOperation> for NormalRoutePairedLaneMissingDocumentOperation {
    fn from(operation: udf::HostOperation) -> Self {
        match operation {
            udf::HostOperation::Patch => Self::Patch,
            udf::HostOperation::Replace => Self::Replace,
            udf::HostOperation::Delete => Self::Delete,
        }
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
enum NormalRoutePairedLaneUnclassifiedErrorSource {
    Developer,
    System,
}

#[test]
fn normal_route_paired_lane_missing_document_error_serializes_structurally() {
    for (host_operation, operation) in [
        (udf::HostOperation::Patch, "patch"),
        (udf::HostOperation::Replace, "replace"),
        (udf::HostOperation::Delete, "delete"),
    ] {
        let outcome = NormalRoutePairedLaneOutcome::Error {
            error: NormalRoutePairedLaneError::MissingDocument {
                operation: host_operation.into(),
                missing_id_bindings: vec![NormalRoutePairedLaneMissingId {
                    path: vec!["targetId".to_owned()],
                    table_name: "accounts".to_owned(),
                }],
            },
        };
        assert_eq!(
            serde_json::to_value(outcome).expect("paired-lane outcome must serialize"),
            json!({
                "error": {
                    "category": "missing-document",
                    "operation": operation,
                    "missingIdBindings": [{
                        "path": ["targetId"],
                        "tableName": "accounts",
                    }],
                },
                "kind": "error",
            })
        );
    }
}

#[test]
fn normal_route_paired_lane_unclassified_error_serializes_structurally() {
    let outcome = NormalRoutePairedLaneOutcome::Error {
        error: NormalRoutePairedLaneError::Unclassified {
            source: NormalRoutePairedLaneUnclassifiedErrorSource::Developer,
        },
    };
    assert_eq!(
        serde_json::to_value(outcome).expect("paired-lane outcome must serialize"),
        json!({
            "error": { "category": "unclassified", "source": "developer" },
            "kind": "error",
        })
    );
}

#[test]
fn normal_route_paired_lane_missing_id_bindings_require_canonical_user_metadata() {
    let first = NormalRoutePairedLaneMissingId {
        path: vec!["firstId".to_owned()],
        table_name: "accounts".to_owned(),
    };
    let second = NormalRoutePairedLaneMissingId {
        path: vec!["secondId".to_owned()],
        table_name: "profiles".to_owned(),
    };
    assert_eq!(
        normalize_normal_route_paired_lane_missing_id_bindings(&[first.clone(), second.clone(),])
            .expect("canonical user bindings must be accepted"),
        vec![first.clone(), second.clone()]
    );
    for bindings in [
        Vec::new(),
        vec![first.clone(), first.clone()],
        vec![second.clone(), first.clone()],
        vec![NormalRoutePairedLaneMissingId {
            path: Vec::new(),
            table_name: "accounts".to_owned(),
        }],
        vec![NormalRoutePairedLaneMissingId {
            path: vec!["targetId".to_owned()],
            table_name: "".to_owned(),
        }],
        vec![NormalRoutePairedLaneMissingId {
            path: vec!["targetId".to_owned()],
            table_name: "_storage".to_owned(),
        }],
    ] {
        assert!(
            normalize_normal_route_paired_lane_missing_id_bindings(&bindings).is_err(),
            "invalid binding metadata must fail"
        );
    }
}

#[test]
fn normal_route_paired_lane_developer_error_classifies_exact_typed_missing_document() {
    let document_id = DeveloperDocumentId::MAX;
    let outcome = paired_lane_developer_error(
        Some(udf::HostOperationErrorV1::NonexistentDocument {
            operation: udf::HostOperation::Patch,
            document_id,
        }),
        &json!({ "target": { "id": document_id.encode() } }),
        &[NormalRoutePairedLaneMissingId {
            path: vec!["target".to_owned(), "id".to_owned()],
            table_name: "accounts".to_owned(),
        }],
    );
    assert_eq!(
        serde_json::to_value(outcome).expect("paired-lane outcome must serialize"),
        json!({
            "error": {
                "category": "missing-document",
                "operation": "patch",
                "missingIdBindings": [{
                    "path": ["target", "id"],
                    "tableName": "accounts",
                }],
            },
            "kind": "error",
        })
    );
}

#[test]
fn normal_route_paired_lane_developer_error_rejects_mismatched_missing_document_id() {
    let document_id = DeveloperDocumentId::MAX;
    let outcome = paired_lane_developer_error(
        Some(udf::HostOperationErrorV1::NonexistentDocument {
            operation: udf::HostOperation::Patch,
            document_id,
        }),
        &json!({ "targetId": "different-document-id" }),
        &[NormalRoutePairedLaneMissingId {
            path: vec!["targetId".to_owned()],
            table_name: "accounts".to_owned(),
        }],
    );
    assert_eq!(
        serde_json::to_value(outcome).expect("paired-lane outcome must serialize"),
        json!({
            "error": { "category": "unclassified", "source": "developer" },
            "kind": "error",
        })
    );
}

#[test]
fn normal_route_paired_lane_developer_error_returns_only_authenticated_matching_bindings() {
    let document_id = DeveloperDocumentId::MAX;
    let outcome = paired_lane_developer_error(
        Some(udf::HostOperationErrorV1::NonexistentDocument {
            operation: udf::HostOperation::Replace,
            document_id,
        }),
        &json!({
            "firstId": document_id.encode(),
            "secondId": "different-document-id",
        }),
        &[
            NormalRoutePairedLaneMissingId {
                path: vec!["firstId".to_owned()],
                table_name: "accounts".to_owned(),
            },
            NormalRoutePairedLaneMissingId {
                path: vec!["secondId".to_owned()],
                table_name: "profiles".to_owned(),
            },
        ],
    );
    assert_eq!(
        serde_json::to_value(outcome).expect("paired-lane outcome must serialize"),
        json!({
            "error": {
                "category": "missing-document",
                "operation": "replace",
                "missingIdBindings": [{
                    "path": ["firstId"],
                    "tableName": "accounts",
                }],
            },
            "kind": "error",
        })
    );
}

#[test]
fn normal_route_paired_lane_developer_error_does_not_infer_missing_document() {
    let outcome = paired_lane_developer_error(
        None,
        &json!({ "targetId": DeveloperDocumentId::MAX.encode() }),
        &[NormalRoutePairedLaneMissingId {
            path: vec!["targetId".to_owned()],
            table_name: "accounts".to_owned(),
        }],
    );
    assert_eq!(
        serde_json::to_value(outcome).expect("paired-lane outcome must serialize"),
        json!({
            "error": { "category": "unclassified", "source": "developer" },
            "kind": "error",
        })
    );
}

#[test]
fn normal_route_paired_lane_developer_error_rejects_unauthenticated_path() {
    let document_id = DeveloperDocumentId::MAX;
    let outcome = paired_lane_developer_error(
        Some(udf::HostOperationErrorV1::NonexistentDocument {
            operation: udf::HostOperation::Delete,
            document_id,
        }),
        &json!({ "differentId": document_id.encode() }),
        &[NormalRoutePairedLaneMissingId {
            path: vec!["targetId".to_owned()],
            table_name: "accounts".to_owned(),
        }],
    );
    assert_eq!(
        serde_json::to_value(outcome).expect("paired-lane outcome must serialize"),
        json!({
            "error": { "category": "unclassified", "source": "developer" },
            "kind": "error",
        })
    );
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRouteSequenceObservation {
    kind: &'static str,
    schema_version: u16,
    deployment_sha256: String,
    generation_sha256: String,
    lane: NormalRouteTestLane,
    sequence: NormalRouteTestSequence,
    invocation_count: usize,
    query_journals: Vec<NormalRouteQueryJournalObservation>,
    results: Vec<JsonValue>,
    normalized_committed_state: JsonValue,
    database_query_starts: usize,
    reads_completed: usize,
    read_documents: usize,
    read_bytes: usize,
    read_intervals: usize,
    runtime_observation_count: usize,
    unique_runtime_count: usize,
    unique_memory_slot_count: usize,
    runtime_invocations: Vec<NormalRouteSequenceInvocation>,
    execution_phases: Vec<NormalRouteExecutionPhaseObservation>,
    pool_hits: usize,
    pool_misses: usize,
    measured_pool_hits: usize,
    measured_pool_misses: usize,
    measured_fresh_runtime_admissions: usize,
    capability_operation_stages: Vec<&'static str>,
    cleaned_up_instances: usize,
    store_drops_after_cleanup: usize,
    teardowns_after_cleanup: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRouteExecutionPhaseObservation {
    fresh_runtime: bool,
    entry_selection_required: bool,
    selected_entry_preparation_required: bool,
    total_nanoseconds: u64,
    instantiation_completed_nanoseconds: Option<u64>,
    initialization_started_nanoseconds: Option<u64>,
    initialization_completed_nanoseconds: Option<u64>,
    entry_selection_started_nanoseconds: Option<u64>,
    entry_selection_completed_nanoseconds: Option<u64>,
    prepare_started_nanoseconds: Option<u64>,
    prepare_completed_nanoseconds: Option<u64>,
    handler_started_nanoseconds: Option<u64>,
    handler_completed_nanoseconds: Option<u64>,
    handler_completion: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
enum NormalRouteQueryJournalObservation {
    None,
    End {
        query_fingerprint_sha256: String,
    },
    After {
        query_fingerprint_sha256: String,
        after_key_sha256: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        after_key_continuation: Option<NormalRouteQueryJournalAfterKeyContinuation>,
    },
}

// The raw after-key digest remains lane-local diagnostic evidence. This marker
// records the authenticated same-lane opaque cursor that carries that
// continuation to a later invocation, so paired lanes with
// independently allocated document IDs can compare the continuation
// rather than the lane-local key bytes.
#[derive(Debug, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
enum NormalRouteQueryJournalAfterKeyContinuation {
    SameLaneOpaqueCursor { opaque_cursor_id: String },
}

impl From<&QueryJournal> for NormalRouteQueryJournalObservation {
    fn from(journal: &QueryJournal) -> Self {
        match journal.logical_identity() {
            QueryJournalLogicalIdentity::None => Self::None,
            QueryJournalLogicalIdentity::End {
                query_fingerprint_sha256,
            } => Self::End {
                query_fingerprint_sha256: query_fingerprint_sha256.as_hex(),
            },
            QueryJournalLogicalIdentity::After {
                query_fingerprint_sha256,
                after_key_sha256,
            } => Self::After {
                query_fingerprint_sha256: query_fingerprint_sha256.as_hex(),
                after_key_sha256: after_key_sha256.as_hex(),
                after_key_continuation: None,
            },
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRouteRuntimeInvocation {
    invocation_id: u64,
    runtime_id: u64,
    memory_slot_id: u64,
    deployment_sha256: String,
    package_key: String,
    entry_id: String,
    entry_selector_id: String,
    capability_identity: u64,
    runtime_was_created: bool,
    operation_count: u64,
    capability_revoked: bool,
    forged_capability_rejected: bool,
    prior_capability_rejected: bool,
    revoked_capability_rejected: bool,
    opaque_live_handles: usize,
    opaque_current_bytes: usize,
    runtime_reuse_contaminated: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalRouteSequenceInvocation {
    invocation_id: u64,
    runtime_id: u64,
    memory_slot_id: u64,
    deployment_sha256: String,
    package_key: String,
    entry_id: String,
    entry_selector_id: String,
    capability_identity: u64,
    runtime_was_created: bool,
    serialized_aot_module_loaded: bool,
    operation_count: u64,
    capability_revoked: bool,
    forged_capability_rejected: bool,
    prior_capability_rejected: bool,
    revoked_capability_rejected: bool,
    opaque_live_handles: usize,
    opaque_current_bytes: usize,
    runtime_reuse_contaminated: bool,
}

impl TryFrom<StaticHermesGeneratedInvocationObservation> for NormalRouteRuntimeInvocation {
    type Error = anyhow::Error;

    fn try_from(
        observation: StaticHermesGeneratedInvocationObservation,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            invocation_id: observation.invocation_id,
            runtime_id: observation.runtime_id,
            memory_slot_id: observation.memory_slot_id,
            deployment_sha256: observation
                .deployment_sha256
                .context("normal route invocation omitted its routed deployment identity")?,
            package_key: observation.package_key,
            entry_id: observation
                .entry_id
                .context("normal route invocation omitted its routed entry identity")?,
            entry_selector_id: observation
                .entry_selector_id
                .context("normal route invocation omitted its routed selector identity")?,
            capability_identity: observation.capability_identity,
            runtime_was_created: observation.runtime_was_created,
            operation_count: observation.operation_count,
            capability_revoked: observation.capability_revoked,
            forged_capability_rejected: observation.forged_capability_rejected,
            prior_capability_rejected: observation.prior_capability_rejected,
            revoked_capability_rejected: observation.revoked_capability_rejected,
            opaque_live_handles: observation.opaque_live_handles,
            opaque_current_bytes: observation.opaque_current_bytes,
            runtime_reuse_contaminated: observation.runtime_reuse_contaminated,
        })
    }
}

impl TryFrom<StaticHermesGeneratedInvocationObservation> for NormalRouteSequenceInvocation {
    type Error = anyhow::Error;

    fn try_from(
        observation: StaticHermesGeneratedInvocationObservation,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            invocation_id: observation.invocation_id,
            runtime_id: observation.runtime_id,
            memory_slot_id: observation.memory_slot_id,
            deployment_sha256: observation
                .deployment_sha256
                .context("sequence invocation omitted its routed deployment identity")?,
            package_key: observation.package_key,
            entry_id: observation
                .entry_id
                .context("sequence invocation omitted its routed entry identity")?,
            entry_selector_id: observation
                .entry_selector_id
                .context("sequence invocation omitted its routed selector identity")?,
            capability_identity: observation.capability_identity,
            runtime_was_created: observation.runtime_was_created,
            serialized_aot_module_loaded: observation.serialized_aot_module_loaded,
            operation_count: observation.operation_count,
            capability_revoked: observation.capability_revoked,
            forged_capability_rejected: observation.forged_capability_rejected,
            prior_capability_rejected: observation.prior_capability_rejected,
            revoked_capability_rejected: observation.revoked_capability_rejected,
            opaque_live_handles: observation.opaque_live_handles,
            opaque_current_bytes: observation.opaque_current_bytes,
            runtime_reuse_contaminated: observation.runtime_reuse_contaminated,
        })
    }
}

fn normal_route_phase_duration_nanoseconds(
    duration: Duration,
    field: &'static str,
) -> anyhow::Result<u64> {
    duration
        .as_nanos()
        .try_into()
        .with_context(|| format!("normal route execution phase {field} duration is out of range"))
}

fn normal_route_optional_phase_duration_nanoseconds(
    duration: Option<Duration>,
    field: &'static str,
) -> anyhow::Result<Option<u64>> {
    duration
        .map(|duration| normal_route_phase_duration_nanoseconds(duration, field))
        .transpose()
}

impl TryFrom<StaticHermesGeneratedExecutionPhaseObservation>
    for NormalRouteExecutionPhaseObservation
{
    type Error = anyhow::Error;

    fn try_from(
        observation: StaticHermesGeneratedExecutionPhaseObservation,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            fresh_runtime: observation.fresh_runtime,
            entry_selection_required: observation.entry_selection_required,
            selected_entry_preparation_required: observation.selected_entry_preparation_required,
            total_nanoseconds: normal_route_phase_duration_nanoseconds(observation.total, "total")?,
            instantiation_completed_nanoseconds: normal_route_optional_phase_duration_nanoseconds(
                observation.instantiation_completed,
                "instantiation completion",
            )?,
            initialization_started_nanoseconds: normal_route_optional_phase_duration_nanoseconds(
                observation.initialization_started,
                "initialization start",
            )?,
            initialization_completed_nanoseconds: normal_route_optional_phase_duration_nanoseconds(
                observation.initialization_completed,
                "initialization completion",
            )?,
            entry_selection_started_nanoseconds: normal_route_optional_phase_duration_nanoseconds(
                observation.entry_selection_started,
                "entry selection start",
            )?,
            entry_selection_completed_nanoseconds:
                normal_route_optional_phase_duration_nanoseconds(
                    observation.entry_selection_completed,
                    "entry selection completion",
                )?,
            prepare_started_nanoseconds: normal_route_optional_phase_duration_nanoseconds(
                observation.prepare_started,
                "selected-entry preparation start",
            )?,
            prepare_completed_nanoseconds: normal_route_optional_phase_duration_nanoseconds(
                observation.prepare_completed,
                "selected-entry preparation completion",
            )?,
            handler_started_nanoseconds: normal_route_optional_phase_duration_nanoseconds(
                observation.handler_started,
                "handler start",
            )?,
            handler_completed_nanoseconds: normal_route_optional_phase_duration_nanoseconds(
                observation.handler_completed,
                "handler completion",
            )?,
            handler_completion: observation.handler_completion,
        })
    }
}

fn normal_route_execution_phase_is_valid(
    observation: &StaticHermesGeneratedExecutionPhaseObservation,
) -> bool {
    let mut phase_boundaries = Vec::with_capacity(10);
    match (
        observation.fresh_runtime,
        observation.instantiation_completed,
        observation.initialization_started,
        observation.initialization_completed,
    ) {
        (
            true,
            Some(instantiation),
            Some(initialization_started),
            Some(initialization_completed),
        ) => {
            phase_boundaries.extend([
                instantiation,
                initialization_started,
                initialization_completed,
            ]);
        },
        (false, None, None, None) => {},
        _ => return false,
    }
    let entry_selection_boundaries = match (
        observation.entry_selection_required,
        observation.entry_selection_started,
        observation.entry_selection_completed,
    ) {
        (true, Some(started), Some(completed)) => Some((started, completed)),
        (false, None, None) => None,
        _ => return false,
    };
    let preparation_boundaries = match (
        observation.selected_entry_preparation_required,
        observation.prepare_started,
        observation.prepare_completed,
    ) {
        (true, Some(started), Some(completed)) => Some((started, completed)),
        (false, None, None) => None,
        _ => return false,
    };
    let second_entry_selection_boundaries = match (
        observation.second_entry_selection_started,
        observation.second_entry_selection_completed,
    ) {
        (Some(started), Some(completed)) => Some((started, completed)),
        (None, None) => None,
        _ => return false,
    };
    let (Some(handler_started), Some(handler_completed)) =
        (observation.handler_started, observation.handler_completed)
    else {
        return false;
    };
    if let Some((entry_selection_started, entry_selection_completed)) = entry_selection_boundaries {
        phase_boundaries.extend([entry_selection_started, entry_selection_completed]);
    }
    if let Some((prepare_started, prepare_completed)) = preparation_boundaries {
        phase_boundaries.extend([prepare_started, prepare_completed]);
    }
    if let Some((second_entry_selection_started, second_entry_selection_completed)) =
        second_entry_selection_boundaries
    {
        phase_boundaries.extend([
            second_entry_selection_started,
            second_entry_selection_completed,
        ]);
    }
    phase_boundaries.extend([handler_started, handler_completed, observation.total]);
    observation.handler_completion == Some("returned")
        && !observation.cancellation_observed
        && phase_boundaries
            .windows(2)
            .all(|boundary| boundary[0] <= boundary[1])
}

fn normal_route_execution_phase_observation() -> StaticHermesGeneratedExecutionPhaseObservation {
    StaticHermesGeneratedExecutionPhaseObservation {
        fresh_runtime: false,
        entry_selection_required: true,
        selected_entry_preparation_required: true,
        total: Duration::from_nanos(9),
        timeout_armed_before_execution: None,
        timeout_reason: None,
        timeout_after_execution_start: None,
        cancellation_observed: false,
        instantiation_completed: None,
        initialization_started: None,
        initialization_completed: None,
        entry_selection_started: Some(Duration::from_nanos(1)),
        entry_selection_status: Some(0),
        entry_selection_completed: Some(Duration::from_nanos(2)),
        prepare_started: Some(Duration::from_nanos(3)),
        prepare_status: Some(0),
        prepare_completed: Some(Duration::from_nanos(4)),
        second_entry_selection_started: Some(Duration::from_nanos(5)),
        second_entry_selection_status: Some(0),
        second_entry_selection_completed: Some(Duration::from_nanos(6)),
        handler_started: Some(Duration::from_nanos(7)),
        handler_completed: Some(Duration::from_nanos(8)),
        handler_completion: Some("returned"),
        initial_fuel: None,
        prepare_start_fuel: None,
        prepare_end_fuel: None,
        handler_start_fuel: None,
        handler_end_fuel: None,
        final_fuel: None,
        function_entry_marks: Vec::new(),
        host_call_marks: Vec::new(),
        completion_stages: Vec::new(),
    }
}

#[test]
fn normal_route_execution_phase_preserves_and_validates_selected_entry_preparation(
) -> anyhow::Result<()> {
    let observation = normal_route_execution_phase_observation();
    assert!(normal_route_execution_phase_is_valid(&observation));

    let serialized = serde_json::to_value(NormalRouteExecutionPhaseObservation::try_from(
        observation.clone(),
    )?)?;
    assert_eq!(serialized["entrySelectionRequired"], json!(true));
    assert_eq!(serialized["prepareStartedNanoseconds"], json!(3));
    assert_eq!(serialized["prepareCompletedNanoseconds"], json!(4));

    let mut missing_prepare = observation.clone();
    missing_prepare.prepare_started = None;
    assert!(!normal_route_execution_phase_is_valid(&missing_prepare));

    let mut out_of_order_prepare = observation.clone();
    out_of_order_prepare.prepare_completed = Some(Duration::from_nanos(6));
    assert!(!normal_route_execution_phase_is_valid(
        &out_of_order_prepare
    ));

    let mut no_prepare_required = observation;
    no_prepare_required.selected_entry_preparation_required = false;
    no_prepare_required.prepare_started = None;
    no_prepare_required.prepare_completed = None;
    assert!(normal_route_execution_phase_is_valid(&no_prepare_required));

    let mut missing_required_selection = normal_route_execution_phase_observation();
    missing_required_selection.entry_selection_started = None;
    missing_required_selection.entry_selection_completed = None;
    assert!(!normal_route_execution_phase_is_valid(
        &missing_required_selection
    ));

    let mut fixed_entry = normal_route_execution_phase_observation();
    fixed_entry.entry_selection_required = false;
    fixed_entry.entry_selection_started = None;
    fixed_entry.entry_selection_completed = None;
    assert!(normal_route_execution_phase_is_valid(&fixed_entry));

    let mut incomplete_selection = fixed_entry;
    incomplete_selection.entry_selection_started = Some(Duration::from_nanos(1));
    assert!(!normal_route_execution_phase_is_valid(
        &incomplete_selection
    ));
    Ok(())
}

#[derive(Debug)]
struct NoopLogSender;

#[async_trait]
impl LogSender for NoopLogSender {
    fn send_logs(&self, _logs: Vec<LogEvent>) {}

    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

struct UnusedModuleLoader;

#[async_trait]
impl ModuleLoader<ProdRuntime> for UnusedModuleLoader {
    async fn get_module_with_metadata(
        &self,
        _module_metadata: &common::document::ParsedDocument<ModuleMetadata>,
        _source_package: &common::document::ParsedDocument<SourcePackage>,
    ) -> anyhow::Result<Arc<FullModuleSource>> {
        anyhow::bail!("Static Hermes route must not load a JavaScript module")
    }
}

// Module loading moved into `InProcessFunctionRunner` with the upstream
// source-map cache refactor. Keep the existing fixture argument at this
// boundary while the test manifests still construct the same source bundle.
struct NormalRouteModuleLoader {
    modules: Arc<BTreeMap<CanonicalizedModulePath, Arc<FullModuleSource>>>,
}

#[async_trait]
impl ModuleLoader<ProdRuntime> for NormalRouteModuleLoader {
    async fn get_module_with_metadata(
        &self,
        module_metadata: &common::document::ParsedDocument<ModuleMetadata>,
        _source_package: &common::document::ParsedDocument<SourcePackage>,
    ) -> anyhow::Result<Arc<FullModuleSource>> {
        let module = self
            .modules
            .get(&module_metadata.path)
            .context("normal route source bundle omitted a loaded module")?;
        anyhow::ensure!(
            model::modules::hash_module_source(&module.source, module.source_map.as_ref())
                == module_metadata.sha256,
            "normal route source bundle module differs from its metadata"
        );
        Ok(Arc::clone(module))
    }
}

async fn new_database(
    rt: ProdRuntime,
    persistence: Arc<SqlitePersistence>,
) -> anyhow::Result<Database<ProdRuntime>> {
    let (deleted_tablet_sender, _deleted_tablet_receiver) = tokio::sync::mpsc::channel(16);
    Database::load(
        persistence,
        rt.clone(),
        Arc::new(SearcherStub),
        ShutdownSignal::panic(),
        model::virtual_system_mapping().clone(),
        IndexCache::new(1 << 20).new_handle(),
        Arc::new(new_unlimited_rate_limiter(rt)),
        deleted_tablet_sender,
        "static_hermes_gate_tests".to_owned(),
    )
    .await
}

async fn install_gate_module(
    database: &Database<ProdRuntime>,
    runtime_module_path: &str,
    export_name: &str,
    udf_type: UdfType,
    visibility: Visibility,
) -> anyhow::Result<()> {
    initialize_application_system_tables(database).await?;
    let mut tx = database.begin_system().await?;
    UdfConfigModel::new(&mut tx, TableNamespace::root_component())
        .set(UdfConfig {
            server_version: semver::Version::new(1, 99, 0),
            import_phase_rng_seed: [0; 32],
            import_phase_unix_timestamp: UnixTimestamp::from_nanos(0),
        })
        .await?;
    let source_package_id = SourcePackageModel::new(&mut tx, TableNamespace::root_component())
        .put(SourcePackage {
            storage_key: ObjectKey::try_from("static-hermes-gate-test")?,
            sha256: Sha256Digest::from([0; 32]),
            runtime_content_sha256: None,
            runtime_generation: None,
            external_deps_package_id: None,
            package_size: PackageSize::default(),
            node_version: None,
            node_executor_pool_topology: Default::default(),
        })
        .await?;
    let analyzed_function = AnalyzedFunction::new(
        export_name.parse()?,
        None,
        udf_type,
        Some(visibility),
        ArgsValidator::Unvalidated,
        ReturnsValidator::Unvalidated,
    )?;
    ModuleModel::new(&mut tx)
        .put(
            None,
            CanonicalizedComponentModulePath {
                component: ComponentId::Root,
                module_path: runtime_module_path.parse()?,
            },
            ModuleSource::from(""),
            source_package_id,
            None,
            Some(AnalyzedModule {
                functions: vec![analyzed_function].into(),
                ..Default::default()
            }),
            ModuleEnvironment::Isolate,
            None,
        )
        .await?;
    database
        .commit_with_write_source(tx, "static_hermes_gate_test_module")
        .await?;
    Ok(())
}

async fn install_gate_module_and_document(
    database: &Database<ProdRuntime>,
) -> anyhow::Result<(TableName, value::DeveloperDocumentId)> {
    install_gate_module(
        database,
        "generatedWasmTest.js",
        "run",
        UdfType::Mutation,
        Visibility::Public,
    )
    .await?;
    let mut tx = database.begin_system().await?;
    let table: TableName = "static_hermes_gate_documents".parse()?;
    let id = SystemMetadataModel::new(&mut tx, TableNamespace::root_component())
        .insert_metadata(
            &table,
            obj!(
                "marker" => "backend-gate-document",
                "sequence" => 1_i64,
            )?,
        )
        .await?;
    database
        .commit_with_write_source(tx, "static_hermes_gate_test_setup")
        .await?;
    Ok((table, id.developer_id))
}

async fn install_gate_query_module_and_documents(
    database: &Database<ProdRuntime>,
) -> anyhow::Result<TableName> {
    install_gate_module(
        database,
        "generatedWasmQueryTest.js",
        "run",
        UdfType::Query,
        Visibility::Public,
    )
    .await?;
    let mut tx = database.begin_system().await?;
    let table: TableName = "static_hermes_gate_query_documents".parse()?;
    let index_name = IndexName::new(table.clone(), IndexDescriptor::new("by_tenant")?)?;
    IndexModel::new(&mut tx)
        .add_application_index(
            TableNamespace::root_component(),
            IndexMetadata::new_enabled(
                index_name,
                IndexedFields::try_from(vec!["tenant".parse()?])?,
            ),
        )
        .await?;
    for (tenant, marker) in [("tenant-a", "first"), ("tenant-b", "second")] {
        SystemMetadataModel::new(&mut tx, TableNamespace::root_component())
            .insert_metadata(&table, obj!("tenant" => tenant, "marker" => marker)?)
            .await?;
    }
    database
        .commit_with_write_source(tx, "static_hermes_gate_query_test_setup")
        .await?;
    Ok(table)
}

async fn new_runner(
    rt: ProdRuntime,
    database: Database<ProdRuntime>,
    persistence: Arc<SqlitePersistence>,
    storage: Arc<dyn Storage>,
    _module_loader: Arc<dyn ModuleLoader<ProdRuntime>>,
    query_cache: QueryCache,
    key_broker: KeyBroker,
) -> anyhow::Result<Arc<ApplicationFunctionRunner<ProdRuntime>>> {
    let deployment = DeploymentMetadata {
        name: STATIC_HERMES_GATE_TEST_INSTANCE_NAME.to_owned(),
        region: None,
        class: DeploymentClass::S16,
    };
    let convex_origin = ConvexOrigin::from("http://127.0.0.1:3210".to_owned());
    let function_runner = Arc::new(InProcessFunctionRunner::new(
        deployment.clone(),
        key_broker.function_runner_keybroker(),
        convex_origin.clone(),
        rt.clone(),
        persistence as Arc<dyn PersistenceReader>,
        DeploymentStorage {
            files_storage: Arc::clone(&storage),
            modules_storage: Arc::clone(&storage),
        },
        database.clone(),
        Arc::new(StaticFetchClient::new()),
    )?);
    let usage_counter = UsageCounter::new(Arc::new(NoOpUsageEventLogger));
    let function_log =
        FunctionExecutionLog::new(rt.clone(), usage_counter, Arc::new(NoopLogSender));
    let source_map_cache = crate::source_map_cache::SourceMapCache::new(rt.clone());
    let node_actions = NodeActions::new(
        Arc::new(NoopNodeExecutor::new()),
        convex_origin.clone(),
        Duration::from_secs(1),
        rt.clone(),
        deployment.clone(),
    );
    let runner = Arc::new(ApplicationFunctionRunner::new(
        rt.clone(),
        database,
        key_broker,
        function_runner.clone(),
        node_actions,
        TransactionalFileStorage::new(rt, Arc::clone(&storage), convex_origin),
        storage,
        source_map_cache,
        function_log,
        AuditLogClient::for_static_hermes_gate_test(),
        Default::default(),
        query_cache,
        None,
        deployment,
    ));
    function_runner.set_action_callbacks(runner.clone());
    Ok(runner)
}

fn real_runner_test_fixture(
) -> anyhow::Result<(RealRunnerDeploymentExport, RealRunnerTestExpectation)> {
    let deployment_manifest_path = std::env::var_os(DEPLOYMENT_MANIFEST_ENV).context(format!(
        "{DEPLOYMENT_MANIFEST_ENV} must identify a deployment manifest"
    ))?;
    let manifest: RealRunnerDeploymentManifest = serde_json::from_slice(
        &std::fs::read(&deployment_manifest_path)
            .context("failed to read real runner deployment manifest")?,
    )
    .context("failed to parse real runner deployment manifest")?;
    let expectation: RealRunnerTestExpectation = serde_json::from_str(
        &std::env::var(REAL_RUNNER_TEST_EXPECTATION_ENV)
            .with_context(|| format!("failed to read {REAL_RUNNER_TEST_EXPECTATION_ENV}"))?,
    )
    .with_context(|| format!("failed to parse {REAL_RUNNER_TEST_EXPECTATION_ENV}"))?;
    anyhow::ensure!(
        manifest.deployment_sha256 == expectation.deployment_sha256,
        "real runner deployment digest differs from the expected producer identity"
    );
    let [selected] = <[_; 1]>::try_from(
        manifest
            .exports
            .into_iter()
            .filter(|export| export.routing.decision == "wasm")
            .collect::<Vec<_>>(),
    )
    .map_err(|exports| {
        anyhow::anyhow!(
            "real runner deployment must contain exactly one Wasm export, found {}",
            exports.len()
        )
    })?;
    let package_reference = selected
        .package_reference
        .as_ref()
        .context("real runner Wasm export omitted its package reference")?;
    anyhow::ensure!(
        package_reference.cache_key == expectation.package_key,
        "real runner package key differs from the expected producer identity"
    );
    anyhow::ensure!(
        !expectation.expected_error_contains.is_empty(),
        "real runner expected error must not be empty"
    );
    Ok((selected, expectation))
}

fn generated_mutation_path() -> anyhow::Result<PublicFunctionPath> {
    Ok(PublicFunctionPath::RootExport(
        "generatedWasmTest:run".parse()?,
    ))
}

fn generated_mutation_args(id: JsonValue) -> anyhow::Result<SerializedArgs> {
    Ok(SerializedArgs::from_args(vec![json!({
        "id": id,
        "value": {
            "marker": "committed-effect",
        },
    })])?)
}

type GateMutationFuture<'a> = Pin<
    Box<
        dyn Future<Output = anyhow::Result<Result<crate::MutationReturn, crate::MutationError>>>
            + 'a,
    >,
>;

fn generated_mutation_future<'a>(
    runner: &'a ApplicationFunctionRunner<ProdRuntime>,
    id: JsonValue,
) -> GateMutationFuture<'a> {
    Box::pin(runner.retry_mutation(
        RequestContext::new_for_system_request(RequestId::new()),
        generated_mutation_path().expect("invalid generated mutation path"),
        generated_mutation_args(id).expect("invalid generated mutation arguments"),
        Identity::system(),
        None,
        FunctionCaller::Cron,
        None,
    ))
}

async fn replace_document(
    database: &Database<ProdRuntime>,
    id: value::DeveloperDocumentId,
    sequence: i64,
) -> anyhow::Result<()> {
    let mut tx = database.begin_system().await?;
    UserFacingModel::new(&mut tx, TableNamespace::root_component())
        .replace(
            id,
            obj!(
                "marker" => "backend-gate-document",
                "sequence" => sequence,
            )?,
        )
        .await?;
    database
        .commit_with_write_source(tx, "static_hermes_gate_test_conflict")
        .await?;
    Ok(())
}

async fn table_count(database: &Database<ProdRuntime>, table: &TableName) -> anyhow::Result<u64> {
    let mut tx = database.begin_system().await?;
    let mut query = ResolvedQuery::new(
        &mut tx,
        TableNamespace::root_component(),
        common::query::Query::full_table_scan(table.clone(), common::query::Order::Asc),
    )?;
    let mut count = 0;
    while query.next(&mut tx, None).await?.is_some() {
        count += 1;
    }
    Ok(count)
}

fn resolve_normal_route_fixture_references(
    value: &JsonValue,
    fixture_ids: &BTreeMap<String, value::DeveloperDocumentId>,
    referenced_aliases: &mut BTreeSet<String>,
) -> anyhow::Result<JsonValue> {
    match value {
        JsonValue::Array(values) => Ok(JsonValue::Array(
            values
                .iter()
                .map(|value| {
                    resolve_normal_route_fixture_references(value, fixture_ids, referenced_aliases)
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        )),
        JsonValue::Object(fields) if fields.contains_key("$fixtureId") => {
            anyhow::ensure!(
                fields.len() == 1,
                "fixture ID reference must contain exactly $fixtureId"
            );
            let alias = fields
                .get("$fixtureId")
                .and_then(JsonValue::as_str)
                .context("fixture ID reference alias must be a string")?;
            let document_id = fixture_ids
                .get(alias)
                .with_context(|| format!("fixture ID reference alias is unavailable: {alias}"))?;
            referenced_aliases.insert(alias.to_owned());
            Ok(json!(document_id.encode()))
        },
        JsonValue::Object(fields) => Ok(JsonValue::Object(
            fields
                .iter()
                .map(|(field, value)| {
                    Ok((
                        field.clone(),
                        resolve_normal_route_fixture_references(
                            value,
                            fixture_ids,
                            referenced_aliases,
                        )?,
                    ))
                })
                .collect::<anyhow::Result<serde_json::Map<_, _>>>()?,
        )),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Ok(value.clone())
        },
    }
}

fn resolve_normal_route_seeded_document_references(
    value: &JsonValue,
    document_ids: &[value::DeveloperDocumentId],
) -> anyhow::Result<JsonValue> {
    match value {
        JsonValue::Array(values) => Ok(JsonValue::Array(
            values
                .iter()
                .map(|value| resolve_normal_route_seeded_document_references(value, document_ids))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )),
        JsonValue::Object(fields) if fields.contains_key("$documentId") => {
            anyhow::ensure!(
                fields.len() == 1,
                "seeded document reference must contain exactly $documentId"
            );
            let index = fields
                .get("$documentId")
                .and_then(JsonValue::as_u64)
                .context("seeded document reference index must be a nonnegative integer")?;
            let index =
                usize::try_from(index).context("seeded document reference index is too large")?;
            let document_id = document_ids.get(index).with_context(|| {
                format!("seeded document reference index is unavailable: {index}")
            })?;
            Ok(json!(document_id.encode()))
        },
        JsonValue::Object(fields) => Ok(JsonValue::Object(
            fields
                .iter()
                .map(|(field, value)| {
                    Ok((
                        field.clone(),
                        resolve_normal_route_seeded_document_references(value, document_ids)?,
                    ))
                })
                .collect::<anyhow::Result<serde_json::Map<_, _>>>()?,
        )),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Ok(value.clone())
        },
    }
}

fn normal_route_paired_lane_missing_id_placeholders(
    value: &JsonValue,
) -> anyhow::Result<Vec<NormalRoutePairedLaneMissingId>> {
    fn collect(
        value: &JsonValue,
        path: &mut Vec<String>,
    ) -> anyhow::Result<Vec<NormalRoutePairedLaneMissingId>> {
        match value {
            JsonValue::Array(values) => {
                let mut placeholders = Vec::new();
                for value in values {
                    placeholders.extend(collect(value, path)?);
                }
                anyhow::ensure!(
                    placeholders.is_empty(),
                    "paired-lane missing ID placeholder must not occur in an array"
                );
                Ok(placeholders)
            },
            JsonValue::Object(fields) if fields.contains_key("$missingId") => {
                anyhow::ensure!(
                    fields.len() == 1,
                    "paired-lane missing ID placeholder must contain exactly $missingId"
                );
                let table_name = fields
                    .get("$missingId")
                    .and_then(JsonValue::as_str)
                    .filter(|table_name| !table_name.is_empty())
                    .context("paired-lane missing ID placeholder table name is invalid")?;
                Ok(vec![NormalRoutePairedLaneMissingId {
                    path: path.clone(),
                    table_name: table_name.to_owned(),
                }])
            },
            JsonValue::Object(fields) => {
                let mut placeholders = Vec::new();
                for (field, value) in fields {
                    path.push(field.clone());
                    placeholders.extend(collect(value, path)?);
                    path.pop();
                }
                Ok(placeholders)
            },
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                Ok(Vec::new())
            },
        }
    }

    collect(value, &mut Vec::new())
}

fn authenticate_normal_route_paired_lane_missing_ids(
    invocations: &[JsonValue],
    missing_ids: Option<&[NormalRoutePairedLaneMissingId]>,
) -> anyhow::Result<BTreeSet<NormalRoutePairedLaneMissingId>> {
    let expected = match missing_ids {
        Some(missing_ids) => normalize_normal_route_paired_lane_missing_id_bindings(missing_ids)?
            .into_iter()
            .collect(),
        None => BTreeSet::new(),
    };
    for invocation in invocations {
        let placeholders = normal_route_paired_lane_missing_id_placeholders(invocation)?;
        let actual = placeholders.iter().cloned().collect::<BTreeSet<_>>();
        anyhow::ensure!(
            actual.len() == placeholders.len() && actual == expected,
            "paired-lane missing ID placeholders do not match their metadata"
        );
    }
    Ok(expected)
}

fn normalize_normal_route_paired_lane_missing_id_bindings(
    missing_ids: &[NormalRoutePairedLaneMissingId],
) -> anyhow::Result<Vec<NormalRoutePairedLaneMissingId>> {
    anyhow::ensure!(
        !missing_ids.is_empty(),
        "paired-lane missing ID metadata must not be empty"
    );
    let canonical = missing_ids.iter().cloned().collect::<BTreeSet<_>>();
    anyhow::ensure!(
        canonical.len() == missing_ids.len(),
        "paired-lane missing ID metadata is repeated"
    );
    let canonical = canonical.into_iter().collect::<Vec<_>>();
    anyhow::ensure!(
        canonical.as_slice() == missing_ids,
        "paired-lane missing ID metadata is not in canonical order"
    );
    for missing_id in &canonical {
        anyhow::ensure!(
            !missing_id.path.is_empty() && missing_id.path.iter().all(|field| !field.is_empty()),
            "paired-lane missing ID metadata path is invalid"
        );
        let table: TableName = missing_id.table_name.parse()?;
        anyhow::ensure!(
            !table.is_system(),
            "paired-lane missing ID metadata must name a user table"
        );
    }
    Ok(canonical)
}

fn substitute_normal_route_paired_lane_missing_ids(
    value: &JsonValue,
    missing_ids: &BTreeMap<String, DeveloperDocumentId>,
) -> anyhow::Result<JsonValue> {
    match value {
        JsonValue::Array(values) => Ok(JsonValue::Array(
            values
                .iter()
                .map(|value| substitute_normal_route_paired_lane_missing_ids(value, missing_ids))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )),
        JsonValue::Object(fields) if fields.contains_key("$missingId") => {
            let table_name = fields
                .get("$missingId")
                .and_then(JsonValue::as_str)
                .context("paired-lane missing ID placeholder table name is invalid")?;
            let document_id = missing_ids.get(table_name).with_context(|| {
                format!("paired-lane missing ID table is unavailable: {table_name}")
            })?;
            Ok(json!(document_id.encode()))
        },
        JsonValue::Object(fields) => Ok(JsonValue::Object(
            fields
                .iter()
                .map(|(field, value)| {
                    Ok((
                        field.clone(),
                        substitute_normal_route_paired_lane_missing_ids(value, missing_ids)?,
                    ))
                })
                .collect::<anyhow::Result<serde_json::Map<_, _>>>()?,
        )),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Ok(value.clone())
        },
    }
}

async fn materialize_normal_route_paired_lane_missing_ids(
    tx: &mut database::Transaction<ProdRuntime>,
    invocations: &[JsonValue],
    missing_ids: Option<&[NormalRoutePairedLaneMissingId]>,
) -> anyhow::Result<(Vec<JsonValue>, Vec<NormalRoutePairedLaneMissingId>)> {
    let authenticated_missing_ids =
        authenticate_normal_route_paired_lane_missing_ids(invocations, missing_ids)?;
    if authenticated_missing_ids.is_empty() {
        return Ok((invocations.to_vec(), Vec::new()));
    }

    let mut ids_by_table = BTreeMap::new();
    for (table_name, table) in authenticated_missing_ids
        .iter()
        .map(|missing_id| {
            Ok((
                missing_id.table_name.clone(),
                missing_id.table_name.parse::<TableName>()?,
            ))
        })
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?
    {
        if !TableModel::new(tx).table_exists(TableNamespace::root_component(), &table) {
            TableModel::new(tx)
                .insert_table_metadata(TableNamespace::root_component(), &table)
                .await?;
        }
        let table_number = tx
            .table_mapping()
            .namespace(TableNamespace::root_component())
            .id(&table)?
            .table_number;
        let document_id = DeveloperDocumentId::new(table_number, InternalId::MAX);
        let resolved_document_id =
            tx.resolve_developer_id(&document_id, TableNamespace::root_component())?;
        anyhow::ensure!(
            tx.get(resolved_document_id).await?.is_none(),
            "paired-lane missing ID placeholder resolves to an existing document"
        );
        ids_by_table.insert(table_name, document_id);
    }

    // This supplies validator-valid arguments for both lanes. It does not make the
    // selected route routing-ready; the paired-lane observation remains
    // execution-only.
    Ok((
        invocations
            .iter()
            .map(|invocation| {
                substitute_normal_route_paired_lane_missing_ids(invocation, &ids_by_table)
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        authenticated_missing_ids.into_iter().collect(),
    ))
}

fn normal_route_paired_lane_opaque_cursor_id_is_valid(id: &str) -> bool {
    let mut bytes = id.bytes();
    matches!(bytes.next(), Some(byte) if byte.is_ascii_alphabetic())
        && id.len() <= 64
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn normal_route_paired_lane_opaque_cursor_references(
    value: &JsonValue,
) -> anyhow::Result<Vec<String>> {
    match value {
        JsonValue::Array(values) => values
            .iter()
            .map(normal_route_paired_lane_opaque_cursor_references)
            .collect::<anyhow::Result<Vec<_>>>()
            .map(|references| references.into_iter().flatten().collect()),
        JsonValue::Object(fields) if fields.contains_key("$opaqueCursor") => {
            anyhow::ensure!(
                fields.len() == 1,
                "paired-lane opaque cursor reference must contain exactly $opaqueCursor"
            );
            let id = fields
                .get("$opaqueCursor")
                .and_then(JsonValue::as_str)
                .filter(|id| normal_route_paired_lane_opaque_cursor_id_is_valid(id))
                .context("paired-lane opaque cursor reference ID is invalid")?;
            Ok(vec![id.to_owned()])
        },
        JsonValue::Object(fields) => fields
            .values()
            .map(normal_route_paired_lane_opaque_cursor_references)
            .collect::<anyhow::Result<Vec<_>>>()
            .map(|references| references.into_iter().flatten().collect()),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Ok(Vec::new())
        },
    }
}

fn normal_route_paired_lane_opaque_cursor_paths_overlap(left: &[String], right: &[String]) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn normalize_normal_route_paired_lane_opaque_cursors(
    invocations: &[JsonValue],
    opaque_cursors: &[NormalRoutePairedLaneOpaqueCursor],
) -> anyhow::Result<Vec<NormalRoutePairedLaneOpaqueCursor>> {
    let mut ids = BTreeSet::new();
    let mut cursor_locations = Vec::<(usize, Vec<String>)>::new();
    for cursor in opaque_cursors {
        anyhow::ensure!(
            normal_route_paired_lane_opaque_cursor_id_is_valid(&cursor.id)
                && cursor.capture_invocation < invocations.len()
                && !cursor.capture_result_path.is_empty()
                && cursor
                    .capture_result_path
                    .iter()
                    .all(|field| !field.is_empty()),
            "paired-lane opaque cursor metadata is invalid"
        );
        anyhow::ensure!(
            ids.insert(cursor.id.clone()),
            "paired-lane opaque cursor ID is repeated"
        );
        anyhow::ensure!(
            !cursor_locations.iter().any(|(invocation, path)| {
                *invocation == cursor.capture_invocation
                    && normal_route_paired_lane_opaque_cursor_paths_overlap(
                        path,
                        &cursor.capture_result_path,
                    )
            }),
            "paired-lane opaque cursor capture locations overlap"
        );
        cursor_locations.push((
            cursor.capture_invocation,
            cursor.capture_result_path.clone(),
        ));
    }
    let cursors_by_id = opaque_cursors
        .iter()
        .map(|cursor| (cursor.id.as_str(), cursor))
        .collect::<BTreeMap<_, _>>();
    let mut reference_counts = opaque_cursors
        .iter()
        .map(|cursor| (cursor.id.as_str(), 0usize))
        .collect::<BTreeMap<_, _>>();
    for (invocation_index, invocation) in invocations.iter().enumerate() {
        for id in normal_route_paired_lane_opaque_cursor_references(invocation)? {
            let cursor = cursors_by_id
                .get(id.as_str())
                .context("paired-lane opaque cursor reference is undeclared")?;
            anyhow::ensure!(
                invocation_index > cursor.capture_invocation,
                "paired-lane opaque cursor reference does not follow its capture"
            );
            *reference_counts
                .get_mut(id.as_str())
                .expect("declared paired-lane opaque cursor lost its reference count") += 1;
        }
    }
    anyhow::ensure!(
        reference_counts.values().all(|count| *count == 1),
        "paired-lane opaque cursors must each be referenced exactly once"
    );
    Ok(opaque_cursors.to_vec())
}

fn normal_route_paired_lane_query_journal_with_opaque_cursor_continuation(
    journal: NormalRouteQueryJournalObservation,
    invocation_index: usize,
    opaque_cursors: &[NormalRoutePairedLaneOpaqueCursor],
) -> NormalRouteQueryJournalObservation {
    let mut captures = opaque_cursors
        .iter()
        .filter(|cursor| cursor.capture_invocation == invocation_index);
    let Some(cursor) = captures.next() else {
        return journal;
    };
    if captures.next().is_some() {
        return journal;
    }
    match journal {
        NormalRouteQueryJournalObservation::After {
            query_fingerprint_sha256,
            after_key_sha256,
            after_key_continuation: None,
        } => NormalRouteQueryJournalObservation::After {
            query_fingerprint_sha256,
            after_key_sha256,
            after_key_continuation: Some(
                NormalRouteQueryJournalAfterKeyContinuation::SameLaneOpaqueCursor {
                    opaque_cursor_id: cursor.id.clone(),
                },
            ),
        },
        journal => journal,
    }
}

fn resolve_normal_route_paired_lane_opaque_cursors(
    value: &JsonValue,
    captured_cursors: &BTreeMap<String, String>,
) -> anyhow::Result<JsonValue> {
    match value {
        JsonValue::Array(values) => Ok(JsonValue::Array(
            values
                .iter()
                .map(|value| {
                    resolve_normal_route_paired_lane_opaque_cursors(value, captured_cursors)
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        )),
        JsonValue::Object(fields) if fields.contains_key("$opaqueCursor") => {
            anyhow::ensure!(
                fields.len() == 1,
                "paired-lane opaque cursor reference must contain exactly $opaqueCursor"
            );
            let id = fields
                .get("$opaqueCursor")
                .and_then(JsonValue::as_str)
                .filter(|id| normal_route_paired_lane_opaque_cursor_id_is_valid(id))
                .context("paired-lane opaque cursor reference ID is invalid")?;
            Ok(json!(captured_cursors.get(id).context(
                "paired-lane opaque cursor was used before capture"
            )?))
        },
        JsonValue::Object(fields) => Ok(JsonValue::Object(
            fields
                .iter()
                .map(|(field, value)| {
                    Ok((
                        field.clone(),
                        resolve_normal_route_paired_lane_opaque_cursors(value, captured_cursors)?,
                    ))
                })
                .collect::<anyhow::Result<serde_json::Map<_, _>>>()?,
        )),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Ok(value.clone())
        },
    }
}

fn capture_normal_route_paired_lane_opaque_cursors(
    value: &JsonValue,
    invocation_index: usize,
    opaque_cursors: &[NormalRoutePairedLaneOpaqueCursor],
    captured_cursors: &mut BTreeMap<String, String>,
) -> anyhow::Result<JsonValue> {
    let mut normalized = value.clone();
    for cursor in opaque_cursors
        .iter()
        .filter(|cursor| cursor.capture_invocation == invocation_index)
    {
        let captured = cursor
            .capture_result_path
            .iter()
            .try_fold(value, |current, field| current.as_object()?.get(field))
            .and_then(JsonValue::as_str)
            .filter(|cursor| !cursor.is_empty())
            .context("paired-lane opaque cursor capture did not return a nonempty string")?
            .to_owned();
        anyhow::ensure!(
            captured_cursors
                .insert(cursor.id.clone(), captured)
                .is_none(),
            "paired-lane opaque cursor was captured more than once"
        );
        let (last, ancestors) = cursor
            .capture_result_path
            .split_last()
            .context("paired-lane opaque cursor capture path is empty")?;
        let mut target = &mut normalized;
        for field in ancestors {
            target = target
                .as_object_mut()
                .and_then(|fields| fields.get_mut(field))
                .context(
                    "paired-lane opaque cursor capture path disappeared during normalization",
                )?;
        }
        let target = target
            .as_object_mut()
            .and_then(|fields| fields.get_mut(last))
            .context("paired-lane opaque cursor capture path disappeared during normalization")?;
        *target = json!({ "kind": "opaque-cursor" });
    }
    Ok(normalized)
}

#[derive(Default)]
struct NormalRoutePaginationTemplateCounts {
    capture_cursor_count: usize,
    fixture_reference_counts: BTreeMap<String, usize>,
    previous_cursor_count: usize,
}

fn count_normal_route_pagination_template(
    value: &JsonValue,
    counts: &mut NormalRoutePaginationTemplateCounts,
) -> anyhow::Result<()> {
    match value {
        JsonValue::Array(values) => {
            for value in values {
                count_normal_route_pagination_template(value, counts)?;
            }
        },
        JsonValue::Object(fields) if fields.contains_key("$paginationCursor") => {
            anyhow::ensure!(
                fields.len() == 1,
                "pagination cursor reference must contain exactly $paginationCursor"
            );
            match fields.get("$paginationCursor").and_then(JsonValue::as_str) {
                Some("capture") => counts.capture_cursor_count += 1,
                Some("previous") => counts.previous_cursor_count += 1,
                _ => anyhow::bail!("pagination cursor reference is invalid"),
            }
        },
        JsonValue::Object(fields) if fields.contains_key("$fixtureId") => {
            anyhow::ensure!(
                fields.len() == 1,
                "pagination fixture ID reference must contain exactly $fixtureId"
            );
            let alias = fields
                .get("$fixtureId")
                .and_then(JsonValue::as_str)
                .context("pagination fixture ID reference alias must be a string")?;
            *counts
                .fixture_reference_counts
                .entry(alias.to_owned())
                .or_default() += 1;
        },
        JsonValue::Object(fields) => {
            for value in fields.values() {
                count_normal_route_pagination_template(value, counts)?;
            }
        },
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {},
    }
    Ok(())
}

fn normalize_normal_route_pagination_cursor_marker(value: &JsonValue) -> anyhow::Result<JsonValue> {
    match value {
        JsonValue::Array(values) => Ok(JsonValue::Array(
            values
                .iter()
                .map(normalize_normal_route_pagination_cursor_marker)
                .collect::<anyhow::Result<Vec<_>>>()?,
        )),
        JsonValue::Object(fields) if fields.contains_key("$paginationCursor") => {
            anyhow::ensure!(
                fields.len() == 1 && fields.get("$paginationCursor") == Some(&json!("capture")),
                "pagination result contains an invalid cursor marker"
            );
            Ok(json!({ "kind": "nonempty-pagination-cursor" }))
        },
        JsonValue::Object(fields) => Ok(JsonValue::Object(
            fields
                .iter()
                .map(|(field, value)| {
                    Ok((
                        field.clone(),
                        normalize_normal_route_pagination_cursor_marker(value)?,
                    ))
                })
                .collect::<anyhow::Result<serde_json::Map<_, _>>>()?,
        )),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Ok(value.clone())
        },
    }
}

fn resolve_normal_route_pagination_cursor_argument(
    value: &JsonValue,
    captured_cursor: Option<&str>,
) -> anyhow::Result<JsonValue> {
    match value {
        JsonValue::Array(values) => Ok(JsonValue::Array(
            values
                .iter()
                .map(|value| {
                    resolve_normal_route_pagination_cursor_argument(value, captured_cursor)
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        )),
        JsonValue::Object(fields) if fields.contains_key("$paginationCursor") => {
            anyhow::ensure!(
                fields.len() == 1 && fields.get("$paginationCursor") == Some(&json!("previous")),
                "pagination invocation contains an invalid cursor marker"
            );
            Ok(json!(captured_cursor.context(
                "pagination invocation requested a cursor before page 1 completed"
            )?))
        },
        JsonValue::Object(fields) => Ok(JsonValue::Object(
            fields
                .iter()
                .map(|(field, value)| {
                    Ok((
                        field.clone(),
                        resolve_normal_route_pagination_cursor_argument(value, captured_cursor)?,
                    ))
                })
                .collect::<anyhow::Result<serde_json::Map<_, _>>>()?,
        )),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Ok(value.clone())
        },
    }
}

fn capture_normal_route_pagination_cursor(
    value: &JsonValue,
    description: &str,
) -> anyhow::Result<String> {
    value
        .as_str()
        .filter(|cursor| !cursor.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("{description} returned an empty or non-string cursor"))
}

fn verify_normal_route_pagination_result(
    actual: &JsonValue,
    expected: &JsonValue,
    captured_cursor: &mut Option<String>,
) -> anyhow::Result<()> {
    match expected {
        JsonValue::Array(expected_values) => {
            let actual_values = actual
                .as_array()
                .context("pagination result array changed to another value kind")?;
            anyhow::ensure!(
                actual_values.len() == expected_values.len(),
                "pagination result array length changed"
            );
            for (actual, expected) in actual_values.iter().zip(expected_values) {
                verify_normal_route_pagination_result(actual, expected, captured_cursor)?;
            }
        },
        JsonValue::Object(expected_fields) if expected_fields.contains_key("$paginationCursor") => {
            anyhow::ensure!(
                expected_fields.len() == 1
                    && expected_fields.get("$paginationCursor") == Some(&json!("capture")),
                "pagination expected result contains an invalid cursor marker"
            );
            let cursor = capture_normal_route_pagination_cursor(actual, "pagination page 1")?;
            anyhow::ensure!(
                captured_cursor.is_none(),
                "pagination result attempted to replace its captured cursor"
            );
            *captured_cursor = Some(cursor);
        },
        JsonValue::Object(expected_fields) => {
            let actual_fields = actual
                .as_object()
                .context("pagination result object changed to another value kind")?;
            anyhow::ensure!(
                actual_fields.len() == expected_fields.len(),
                "pagination result object field set changed"
            );
            for (field, expected) in expected_fields {
                let actual = actual_fields
                    .get(field)
                    .with_context(|| format!("pagination result omitted expected field {field}"))?;
                verify_normal_route_pagination_result(actual, expected, captured_cursor)?;
            }
        },
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            anyhow::ensure!(
                actual == expected,
                "pagination result differs from its exact expectation"
            )
        },
    }
    Ok(())
}

fn normalized_normal_route_unchanged_seeded_state(
    seeded_rows: &[NormalRouteObjectFixtureRow],
) -> anyhow::Result<JsonValue> {
    let rows = seeded_rows
        .iter()
        .map(|row| {
            let mut document = row
                .value
                .as_object()
                .context("pagination fixture row value is not an object")?
                .clone();
            document.insert(
                "_creationTime".to_owned(),
                json!({ "kind": "creation-time" }),
            );
            document.insert("_id".to_owned(), json!({ "$fixtureId": row.alias }));
            Ok(json!({
                "alias": row.alias,
                "document": document,
                "table": row.table,
            }))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(json!({ "rows": rows }))
}

fn normalized_normal_route_object_patch_delete_state(
    seeded_rows: &[NormalRouteObjectFixtureRow],
    target_alias: &str,
    deleted_fields: &BTreeSet<String>,
) -> anyhow::Result<JsonValue> {
    let rows = seeded_rows
        .iter()
        .map(|row| {
            let mut document = row
                .value
                .as_object()
                .context("object patch fixture row value is not an object")?
                .clone();
            if row.alias == target_alias {
                for field in deleted_fields {
                    document.remove(field);
                }
            }
            document.insert(
                "_creationTime".to_owned(),
                json!({ "kind": "creation-time" }),
            );
            document.insert("_id".to_owned(), json!({ "$fixtureId": row.alias }));
            Ok(json!({
                "alias": row.alias,
                "document": document,
                "table": row.table,
            }))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(json!({
        "rows": rows,
        "targetAlias": target_alias,
    }))
}

fn normalized_normal_route_stateful_query_patch_state(
    seeded_rows: &[NormalRouteObjectFixtureRow],
    post_state_field_policies: &BTreeMap<String, BTreeMap<String, NormalRoutePostStateFieldPolicy>>,
) -> anyhow::Result<JsonValue> {
    let rows = seeded_rows
        .iter()
        .map(|row| {
            let policies = post_state_field_policies.get(&row.alias).with_context(|| {
                format!(
                    "stateful query patch fixture omitted post-state policies for {}",
                    row.alias
                )
            })?;
            let mut document = row
                .value
                .as_object()
                .context("stateful query patch fixture row value is not an object")?
                .clone();
            for (field, policy) in policies {
                match policy {
                    NormalRoutePostStateFieldPolicy::Absent => {
                        document.remove(field);
                    },
                    NormalRoutePostStateFieldPolicy::IncreasedNumber => {
                        document.insert(field.clone(), json!({ "kind": "increased-number" }));
                    },
                    NormalRoutePostStateFieldPolicy::Preserved => {},
                }
            }
            document.insert(
                "_creationTime".to_owned(),
                json!({ "kind": "creation-time" }),
            );
            document.insert("_id".to_owned(), json!({ "$fixtureId": row.alias }));
            Ok(json!({
                "alias": row.alias,
                "document": document,
                "table": row.table,
            }))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(json!({ "rows": rows }))
}

fn normal_route_numeric_value(value: &value::ConvexValue) -> Option<f64> {
    match value {
        value::ConvexValue::Float64(value) => Some(*value),
        value::ConvexValue::Int64(value) => Some(*value as f64),
        _ => None,
    }
}

async fn restore_normal_route_object_patch_delete_fields(
    database: &Database<ProdRuntime>,
    fixture: &NormalRouteObjectPatchDeleteState,
) -> anyhow::Result<()> {
    let Some(fields) = &fixture.restore_fields_before_measured_invocation else {
        return Ok(());
    };
    let mut tx = database.begin_system().await?;
    UserFacingModel::new(&mut tx, TableNamespace::root_component())
        .patch(
            fixture.target_document_id.clone(),
            PatchValue::from(fields.clone()),
        )
        .await?;
    database
        .commit_with_write_source(tx, "static_hermes_normal_route_object_fixture_restore")
        .await?;
    Ok(())
}

fn verify_normal_route_object_patch_delete_row(
    row: &NormalRouteObjectFixtureRowState,
    target_document_id: &value::DeveloperDocumentId,
    deleted_fields: &BTreeSet<String>,
    actual_document_id: &value::DeveloperDocumentId,
    actual_value: &value::ConvexObject,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        actual_document_id == &row.document_id,
        "object patch fixture row changed identity: alias={}",
        row.alias
    );
    let actual_body = actual_value
        .clone()
        .filter_fields(|field| !matches!(&**field, "_id" | "_creationTime"));
    if &row.document_id != target_document_id {
        anyhow::ensure!(
            actual_body == row.expected_value,
            "object patch fixture row changed outside its declared fields: alias={}",
            row.alias
        );
        return Ok(());
    }

    let undeclared_value = actual_body
        .clone()
        .filter_fields(|field| !deleted_fields.contains(&**field));
    anyhow::ensure!(
        undeclared_value == row.expected_value,
        "object patch fixture row changed outside its declared fields: alias={}",
        row.alias
    );
    anyhow::ensure!(
        deleted_fields
            .iter()
            .all(|field| actual_body.get(field.as_str()).is_none()),
        "object patch fixture retained a declared deleted field"
    );
    Ok(())
}

async fn normal_route_object_patch_delete_post_state(
    database: &Database<ProdRuntime>,
    fixture: &NormalRouteObjectPatchDeleteState,
) -> anyhow::Result<JsonValue> {
    let mut tx = database.begin_system().await?;
    for row in &fixture.rows {
        let document = UserFacingModel::new(&mut tx, TableNamespace::root_component())
            .get_with_ts(row.document_id.clone(), None)
            .await?
            .with_context(|| format!("object patch fixture row is absent: {}", row.alias))?
            .0;
        verify_normal_route_object_patch_delete_row(
            row,
            &fixture.target_document_id,
            &fixture.deleted_fields,
            &document.id(),
            &document.value().0,
        )?;
    }
    Ok(fixture.normalized_committed_state.clone())
}

async fn normal_route_indexed_pagination_post_state(
    database: &Database<ProdRuntime>,
    fixture: &NormalRouteIndexedPaginationState,
) -> anyhow::Result<JsonValue> {
    let mut tx = database.begin_system().await?;
    for row in &fixture.rows {
        let document = UserFacingModel::new(&mut tx, TableNamespace::root_component())
            .get_with_ts(row.document_id.clone(), None)
            .await?
            .with_context(|| format!("indexed pagination fixture row is absent: {}", row.alias))?
            .0;
        anyhow::ensure!(
            document.id() == row.document_id && document.value().0 == row.expected_value,
            "indexed pagination query changed a fixture row: alias={}",
            row.alias
        );
    }
    Ok(fixture.normalized_committed_state.clone())
}

async fn normal_route_stateful_query_patch_post_state(
    database: &Database<ProdRuntime>,
    fixture: &NormalRouteStatefulQueryPatchState,
) -> anyhow::Result<JsonValue> {
    let mut tx = database.begin_system().await?;
    for row in &fixture.rows {
        let document = UserFacingModel::new(&mut tx, TableNamespace::root_component())
            .get_with_ts(row.document_id.clone(), None)
            .await?
            .with_context(|| format!("stateful query patch fixture row is absent: {}", row.alias))?
            .0;
        let actual = &document.value().0;
        // Policies cover application fields. UserFacingModel also returns the two
        // Convex system fields in the document value.
        let expected_field_count = row
            .post_state_field_policies
            .values()
            .filter(|policy| !matches!(policy, NormalRoutePostStateFieldPolicy::Absent))
            .count()
            + 2;
        anyhow::ensure!(
            document.id() == row.document_id,
            "stateful query patch fixture row changed identity: alias={}",
            row.alias
        );
        anyhow::ensure!(
            actual.len() == expected_field_count,
            "stateful query patch fixture row has an unexpected field set: alias={}, \
             expected_count={expected_field_count}, actual_count={}, actual_fields={:?}",
            row.alias,
            actual.len(),
            actual.keys().collect::<Vec<_>>()
        );
        for (field, policy) in &row.post_state_field_policies {
            let actual_value = actual.get(field.as_str());
            let initial_value = row
                .initial_value
                .get(field.as_str())
                .context("stateful query patch policy names an unseeded field")?;
            let policy_matches = match policy {
                NormalRoutePostStateFieldPolicy::Absent => actual_value.is_none(),
                NormalRoutePostStateFieldPolicy::IncreasedNumber => actual_value
                    .and_then(normal_route_numeric_value)
                    .zip(normal_route_numeric_value(initial_value))
                    .is_some_and(|(actual, initial)| actual > initial),
                NormalRoutePostStateFieldPolicy::Preserved => actual_value == Some(initial_value),
            };
            anyhow::ensure!(
                policy_matches,
                "stateful query patch post-state policy failed: alias={}, field={field}",
                row.alias
            );
        }
    }
    Ok(fixture.normalized_committed_state.clone())
}

async fn seed_normal_route_collect_delete_rows(
    database: &Database<ProdRuntime>,
    result_fields: &BTreeMap<String, TableName>,
    synthetic_rows: &[JsonValue],
) -> anyhow::Result<Vec<value::DeveloperDocumentId>> {
    let mut tx = database.begin_system().await?;
    let mut document_ids = Vec::with_capacity(result_fields.len() * synthetic_rows.len());
    for table in result_fields.values() {
        for row in synthetic_rows {
            let value = value::ConvexObject::try_from(row.clone())?;
            let document_id = UserFacingModel::new(&mut tx, TableNamespace::root_component())
                .insert(table.clone(), value)
                .await?;
            document_ids.push(document_id);
        }
    }
    database
        .commit_with_write_source(tx, "static_hermes_normal_route_collect_delete_seed")
        .await?;
    anyhow::ensure!(
        document_ids.len() == result_fields.len() * synthetic_rows.len(),
        "collect-delete fixture did not seed every declared table"
    );
    for table in result_fields.values() {
        anyhow::ensure!(
            table_count(database, table).await? == synthetic_rows.len() as u64,
            "collect-delete fixture table has an unexpected seeded row count"
        );
    }
    Ok(document_ids)
}

async fn normal_route_collect_delete_post_state(
    database: &Database<ProdRuntime>,
    result_fields: &BTreeMap<String, TableName>,
    seeded_document_ids: &[value::DeveloperDocumentId],
) -> anyhow::Result<JsonValue> {
    let mut tx = database.begin_system().await?;
    let mut seeded_ids_absent = true;
    for document_id in seeded_document_ids {
        seeded_ids_absent &= UserFacingModel::new(&mut tx, TableNamespace::root_component())
            .get_with_ts(document_id.clone(), None)
            .await?
            .is_none();
    }
    anyhow::ensure!(
        seeded_ids_absent,
        "collect-delete route retained a seeded document"
    );
    drop(tx);

    let mut table_counts = serde_json::Map::new();
    for table in result_fields.values() {
        let count = table_count(database, table).await?;
        anyhow::ensure!(count == 0, "collect-delete route retained table rows");
        table_counts.insert(table.to_string(), json!(count));
    }
    Ok(json!({
        "seededIdsAbsent": seeded_ids_absent,
        "tableCounts": table_counts,
    }))
}

async fn normal_route_empty_database_post_state(
    database: &Database<ProdRuntime>,
    tables: &[TableName],
) -> anyhow::Result<JsonValue> {
    let mut table_counts = serde_json::Map::new();
    for table in tables {
        let count = table_count(database, table).await?;
        anyhow::ensure!(
            count == 0,
            "normal route sequence empty-database table retained rows"
        );
        table_counts.insert(table.to_string(), json!(count));
    }
    Ok(json!({ "tableCounts": table_counts }))
}

fn normal_route_sequence_json_canonical_bytes(value: &JsonValue) -> anyhow::Result<Vec<u8>> {
    fn write(value: &JsonValue, output: &mut Vec<u8>) -> anyhow::Result<()> {
        match value {
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                serde_json::to_writer(output, value)?
            },
            JsonValue::Array(values) => {
                output.push(b'[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(b',');
                    }
                    write(value, output)?;
                }
                output.push(b']');
            },
            JsonValue::Object(values) => {
                output.push(b'{');
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        output.push(b',');
                    }
                    serde_json::to_writer(&mut *output, key)?;
                    output.push(b':');
                    write(
                        values
                            .get(key)
                            .context("canonical JSON object key disappeared")?,
                        output,
                    )?;
                }
                output.push(b'}');
            },
        }
        Ok(())
    }

    let mut bytes = Vec::new();
    write(value, &mut bytes)?;
    Ok(bytes)
}

fn normal_route_sequence_sort_documents(
    mut documents: Vec<JsonValue>,
) -> anyhow::Result<Vec<JsonValue>> {
    let mut keyed = documents
        .drain(..)
        .map(|document| {
            Ok((
                normal_route_sequence_json_canonical_bytes(&document)?,
                document,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    keyed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    Ok(keyed.into_iter().map(|(_, document)| document).collect())
}

fn normal_route_table_document_expected_normalized(
    document: &JsonValue,
) -> anyhow::Result<JsonValue> {
    let fields = document
        .as_object()
        .context("normal route sequence table-document expected row is not an object")?;
    anyhow::ensure!(
        !fields.contains_key("_id") && !fields.contains_key("_creationTime"),
        "normal route sequence table-document expected row must omit system fields"
    );
    let mut normalized = fields.clone();
    normalized.insert(
        "_creationTime".to_owned(),
        json!({ "kind": "creation-time" }),
    );
    normalized.insert("_id".to_owned(), json!({ "kind": "stable-document-id" }));
    Ok(JsonValue::Object(normalized))
}

fn normal_route_table_documents_expected_normalized(
    table: &TableName,
    documents: &[JsonValue],
) -> anyhow::Result<JsonValue> {
    anyhow::ensure!(
        !documents.is_empty(),
        "normal route sequence table-documents state must contain at least one row"
    );
    let documents = documents
        .iter()
        .map(normal_route_table_document_expected_normalized)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let documents = normal_route_sequence_sort_documents(documents)?;
    let document_count = documents.len();
    Ok(json!({
        "documents": documents,
        "tableCounts": { table.to_string(): document_count },
    }))
}

async fn normal_route_table_documents_post_state(
    database: &Database<ProdRuntime>,
    table: &TableName,
    expected_document_count: usize,
) -> anyhow::Result<JsonValue> {
    let mut tx = database.begin_system().await?;
    let mut query = ResolvedQuery::new(
        &mut tx,
        TableNamespace::root_component(),
        common::query::Query::full_table_scan(table.clone(), common::query::Order::Asc),
    )?;
    let mut documents = Vec::with_capacity(expected_document_count);
    while let Some(document) = query.next(&mut tx, None).await? {
        let mut value = document
            .to_internal_json()
            .as_object()
            .context("normal route sequence table-documents row is not an object")?
            .clone();
        let creation_time = value
            .remove("_creationTime")
            .context("normal route sequence table-documents row omitted its creation time")?;
        anyhow::ensure!(
            creation_time.is_number(),
            "normal route sequence table-documents row has an invalid creation time"
        );
        let document_id = value
            .remove("_id")
            .context("normal route sequence table-documents row omitted its document ID")?;
        anyhow::ensure!(
            document_id.is_string(),
            "normal route sequence table-documents row has an invalid document ID"
        );
        value.insert(
            "_creationTime".to_owned(),
            json!({ "kind": "creation-time" }),
        );
        value.insert("_id".to_owned(), json!({ "kind": "stable-document-id" }));
        documents.push(JsonValue::Object(value));
    }
    drop(query);
    drop(tx);
    anyhow::ensure!(
        documents.len() == expected_document_count,
        "normal route sequence table-documents row count differs from its expectation"
    );
    let documents = normal_route_sequence_sort_documents(documents)?;
    let document_count = documents.len();
    Ok(json!({
        "documents": documents,
        "tableCounts": { table.to_string(): document_count },
    }))
}

async fn normal_route_application_probe_patch_state(
    database: &Database<ProdRuntime>,
    table: &TableName,
    scheduled_function_address: &str,
) -> anyhow::Result<NormalRouteApplicationProbePatchState> {
    let mut tx = database.begin_system().await?;
    let mut query = ResolvedQuery::new(
        &mut tx,
        TableNamespace::root_component(),
        common::query::Query::full_table_scan(table.clone(), common::query::Order::Asc),
    )?;
    let document = query
        .next(&mut tx, None)
        .await?
        .context("application patch route did not commit its singleton state")?;
    anyhow::ensure!(
        query.next(&mut tx, None).await?.is_none(),
        "application patch route committed more than one probe state"
    );
    drop(query);

    let value = document.to_internal_json();
    let fields = value
        .as_object()
        .context("application patch route committed a non-object probe state")?;
    let owner_id = fields
        .get("_id")
        .and_then(JsonValue::as_str)
        .context("application patch route state omitted its document ID")?;
    let scheduled_function_id = fields
        .get("nodeProbeScheduledFnId")
        .and_then(JsonValue::as_str)
        .context("application patch route state omitted its scheduled-function ID")?;
    let scheduled_function_id_encoded = scheduled_function_id.to_owned();
    let scheduler_observed_at = fields
        .get("schedulerObservedAt")
        .and_then(JsonValue::as_f64)
        .context("application patch route state omitted its scheduler observation time")?;
    anyhow::ensure!(
        fields.len() == 5
            && scheduler_observed_at >= 0.0
            && scheduler_observed_at <= ((1_u64 << 53) - 1) as f64
            && scheduler_observed_at.fract() == 0.0
            && fields
                .get("_creationTime")
                .is_some_and(JsonValue::is_number)
            && fields.get("key") == Some(&json!("singleton"))
            && fields.get("lastNodeProbeSuccessAt").is_none(),
        "application patch route committed an unexpected probe state shape"
    );
    let scheduled_function_id = value::DeveloperDocumentId::decode(scheduled_function_id)?;
    let table_mapping = tx.table_mapping().clone();
    let virtual_system_mapping = tx.virtual_system_mapping().clone();
    let system_scheduled_function_id = virtual_system_mapping
        .virtual_id_v6_to_system_resolved_doc_id(
            TableNamespace::root_component(),
            &scheduled_function_id,
            &table_mapping,
        )?;
    let system_scheduled_function = tx
        .get(system_scheduled_function_id)
        .await?
        .context("application patch route state does not own an existing scheduled function")?;
    let scheduled_function = ScheduledJobsDocMapper
        .system_to_virtual_doc(
            &mut tx,
            &virtual_system_mapping,
            system_scheduled_function,
            &table_mapping,
            semver::Version::new(1, 99, 0),
        )
        .await?;
    anyhow::ensure!(
        scheduled_function.id().to_string() == scheduled_function_id_encoded
            && f64::from(scheduled_function.creation_time()) >= 0.0,
        "application patch route resolved an unexpected scheduled function document"
    );
    let scheduled_job = PublicScheduledJob::try_from(scheduled_function.value().0.clone())
        .map_err(|_| {
            anyhow::anyhow!("application patch route owns an invalid public scheduled function")
        })?;
    let scheduled_args = scheduled_job.args.to_internal_json();
    let scheduled_nonce = scheduled_args
        .as_array()
        .and_then(|args| args.first())
        .and_then(JsonValue::as_object)
        .and_then(|arg| arg.get("nonce"))
        .and_then(JsonValue::as_str)
        .context("application patch route scheduled function omitted its nonce")?;
    let scheduled_nonce_time = scheduled_nonce
        .strip_prefix("scheduled-node-probe-")
        .context("application patch route scheduled function has an invalid nonce prefix")?
        .parse::<u64>()
        .context("application patch route scheduled function has an invalid nonce time")?;
    anyhow::ensure!(
        scheduled_nonce_time <= (1_u64 << 53) - 1
            && scheduled_nonce == format!("scheduled-node-probe-{scheduled_nonce_time}"),
        "application patch route scheduled function has a non-canonical nonce"
    );
    let scheduled_nonce_time = scheduled_nonce_time as f64;
    let scheduled_time = scheduled_job.scheduled_time;
    anyhow::ensure!(
        scheduled_time.is_finite()
            && scheduled_time >= scheduled_nonce_time
            && scheduled_time < scheduled_nonce_time + 1.0
            && scheduled_nonce_time <= scheduler_observed_at,
        "application patch route scheduled function has an unexpected scheduled time"
    );
    let expected_args = json!([{
        "cpuIterations": 0.0,
        "delayMs": 0.0,
        "nonce": scheduled_nonce,
        "sequence": 0.0,
    }]);
    anyhow::ensure!(
        String::from(scheduled_job.name) == scheduled_function_address
            && scheduled_args == expected_args
            && matches!(scheduled_job.state, ScheduledJobState::Pending)
            && scheduled_job.completed_time.is_none(),
        "application patch route committed an unexpected scheduled function"
    );
    Ok(NormalRouteApplicationProbePatchState {
        owner_id: owner_id.to_owned(),
        scheduled_function_id: scheduled_function_id_encoded,
        normalized: json!({
            "_creationTime": { "kind": "creation-time" },
            "_id": { "kind": "stable-document-id" },
            "key": "singleton",
            "nodeProbeScheduledFnId": {
                "kind": "stable-scheduled-function-id",
                "ownerKey": "singleton",
            },
            "schedulerObservedAt": { "kind": "invocation-time" },
        }),
    })
}

fn normal_route_sha256(value: &str, description: &str) -> anyhow::Result<Sha256Digest> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{description} is not a SHA-256 digest"
    );
    let bytes = const_hex::decode(value)
        .with_context(|| format!("{description} is not lowercase hexadecimal"))?;
    Sha256Digest::try_from(bytes)
}

fn normal_route_test_key_broker(deployment_sha256: &str) -> anyhow::Result<KeyBroker> {
    let deployment_digest =
        normal_route_sha256(deployment_sha256, "normal route deployment identity")?;
    // Each lane has an isolated test database but represents this same
    // authenticated deployment. Bind its deterministic cursor key to
    // that identity.
    let mut secret_hasher = Sha256::new();
    secret_hasher.update(NORMAL_ROUTE_TEST_DEPLOYMENT_SECRET_DOMAIN);
    secret_hasher.update(deployment_digest.as_ref());
    let deployment_secret = DeploymentSecret::try_from(secret_hasher.finalize().to_vec())?;
    KeyBroker::new(STATIC_HERMES_GATE_TEST_INSTANCE_NAME, deployment_secret)
}

#[test]
fn normal_route_key_broker_matches_cursor_ciphertext_by_deployment_identity() -> anyhow::Result<()>
{
    use common::query::{
        Cursor,
        CursorPosition,
    };

    let deployment_sha256 = "a".repeat(64);
    let other_deployment_sha256 = "b".repeat(64);
    let cursor = Cursor {
        position: CursorPosition::End,
        query_fingerprint: b"logical-query".to_vec(),
    };
    let changed_cursor = Cursor {
        position: CursorPosition::End,
        query_fingerprint: b"changed-logical-query".to_vec(),
    };
    let first = normal_route_test_key_broker(&deployment_sha256)?.encrypt_cursor(&cursor);
    let same = normal_route_test_key_broker(&deployment_sha256)?.encrypt_cursor(&cursor);
    let changed = normal_route_test_key_broker(&deployment_sha256)?.encrypt_cursor(&changed_cursor);
    let different_deployment =
        normal_route_test_key_broker(&other_deployment_sha256)?.encrypt_cursor(&cursor);
    assert_eq!(first, same);
    assert_ne!(first, changed);
    assert_ne!(first, different_deployment);
    Ok(())
}

fn read_normal_route_test_expectation() -> anyhow::Result<String> {
    let file_handoff = match (
        std::env::var_os(NORMAL_ROUTE_TEST_EXPECTATION_FILE_ENV),
        std::env::var_os(NORMAL_ROUTE_TEST_EXPECTATION_FILE_SHA256_ENV),
        std::env::var_os(NORMAL_ROUTE_TEST_EXPECTATION_FILE_SIZE_ENV),
    ) {
        (None, None, None) => None,
        (Some(path), Some(_), Some(_)) => Some(path),
        _ => anyhow::bail!("normal route test expectation file handoff is incomplete"),
    };
    match (
        std::env::var(NORMAL_ROUTE_TEST_EXPECTATION_ENV),
        file_handoff,
    ) {
        (Ok(_), Some(_)) => anyhow::bail!(
            "normal route test expectation inline and file handoffs are mutually exclusive"
        ),
        (Ok(expectation), None) => Ok(expectation),
        (Err(std::env::VarError::NotPresent), Some(path)) => {
            let expected_sha256 = normal_route_sha256(
                &std::env::var(NORMAL_ROUTE_TEST_EXPECTATION_FILE_SHA256_ENV).with_context(
                    || format!("failed to read {NORMAL_ROUTE_TEST_EXPECTATION_FILE_SHA256_ENV}"),
                )?,
                "normal route test expectation file identity",
            )?;
            let expected_size = std::env::var(NORMAL_ROUTE_TEST_EXPECTATION_FILE_SIZE_ENV)
                .with_context(|| {
                    format!("failed to read {NORMAL_ROUTE_TEST_EXPECTATION_FILE_SIZE_ENV}")
                })?
                .parse::<usize>()
                .with_context(|| {
                    format!("{NORMAL_ROUTE_TEST_EXPECTATION_FILE_SIZE_ENV} is not a byte count")
                })?;
            let metadata = std::fs::symlink_metadata(&path)
                .context("failed to inspect normal route test expectation file")?;
            anyhow::ensure!(
                metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
                "normal route test expectation file must be a regular file"
            );
            let bytes = std::fs::read(&path)
                .context("failed to read normal route test expectation file")?;
            anyhow::ensure!(
                bytes.len() == expected_size,
                "normal route test expectation file size differs from its authenticated identity"
            );
            anyhow::ensure!(
                Sha256::hash(&bytes) == expected_sha256,
                "normal route test expectation file differs from its authenticated identity"
            );
            String::from_utf8(bytes).context("normal route test expectation file is not UTF-8")
        },
        (Err(std::env::VarError::NotPresent), None) => Err(anyhow::anyhow!(
            "failed to read {NORMAL_ROUTE_TEST_EXPECTATION_ENV} or its file handoff"
        )),
        (Err(error), _) => Err(error)
            .with_context(|| format!("failed to read {NORMAL_ROUTE_TEST_EXPECTATION_ENV}")),
    }
}

fn normal_route_user_identity(subject: String) -> anyhow::Result<UserIdentity> {
    UserIdentity::from_proto_unchecked(pb::convex_identity::UserIdentity {
        subject: Some(subject.clone()),
        issuer: Some("https://normal-route.invalid".to_owned()),
        expiration: Some((SystemTime::now() + Duration::from_secs(3_600)).into()),
        attributes: Some(pb::convex_identity::UserIdentityAttributes {
            token_identifier: Some(format!("https://normal-route.invalid|{subject}")),
            issuer: Some("https://normal-route.invalid".to_owned()),
            subject: Some(subject),
            ..Default::default()
        }),
        original_token: Some("normal-route-test-token".to_owned()),
    })
}

fn normal_route_source_bundle_sha256(
    bundle_identity: &NormalRouteSourceBundleIdentity,
) -> anyhow::Result<Sha256Digest> {
    normal_route_sha256(
        &bundle_identity.sha256,
        "normal route source-bundle identity",
    )
}

fn read_normal_route_source_bundle(
    bundle_identity: &NormalRouteSourceBundleIdentity,
) -> anyhow::Result<NormalRouteSourceBundle> {
    let bundle_bytes = std::fs::read(&bundle_identity.path)
        .context("failed to read normal route source bundle")?;
    let expected_bundle_sha256 = normal_route_source_bundle_sha256(bundle_identity)?;
    anyhow::ensure!(
        bundle_bytes.len() == bundle_identity.size
            && Sha256::hash(&bundle_bytes) == expected_bundle_sha256,
        "normal route source bundle differs from its authenticated identity"
    );
    serde_json::from_slice(&bundle_bytes).context("failed to parse normal route source bundle")
}

fn load_normal_route_source_modules(
    bundle_identity: &NormalRouteSourceBundleIdentity,
) -> anyhow::Result<BTreeMap<CanonicalizedModulePath, Arc<FullModuleSource>>> {
    let bundle = read_normal_route_source_bundle(bundle_identity)?;
    let mut modules = BTreeMap::new();
    for module in bundle.app_definition.changed_modules {
        if module.environment.as_deref() != Some("isolate") {
            continue;
        }
        let path: CanonicalizedModulePath = module.path.parse()?;
        let prior = modules.insert(
            path,
            Arc::new(FullModuleSource {
                source: ModuleSource::new(&module.source),
                source_map: module.source_map,
            }),
        );
        anyhow::ensure!(
            prior.is_none(),
            "normal route source bundle repeats a module"
        );
    }
    Ok(modules)
}

fn normal_route_selected_source_module_is_authenticated(
    expectation: &NormalRouteTestExpectation,
    modules: &BTreeMap<CanonicalizedModulePath, Arc<FullModuleSource>>,
) -> anyhow::Result<()> {
    let selected_path: CanonicalizedModulePath = expectation.route.runtime_module_path.parse()?;
    let selected = modules
        .get(&selected_path)
        .context("normal route source bundle omitted the selected module")?;
    anyhow::ensure!(
        &*selected.source == expectation.source.module_source.as_str()
            && selected.source_map == expectation.source.source_map,
        "normal route selected module differs from its authenticated source"
    );
    Ok(())
}

fn normal_route_source_modules(
    expectation: &NormalRouteTestExpectation,
) -> anyhow::Result<BTreeMap<CanonicalizedModulePath, Arc<FullModuleSource>>> {
    let modules = load_normal_route_source_modules(&expectation.source.bundle)?;
    normal_route_selected_source_module_is_authenticated(expectation, &modules)?;
    Ok(modules)
}

fn new_normal_route_shared_source_bundle(
    expectation: &NormalRouteTestExpectation,
) -> anyhow::Result<NormalRouteSharedSourceBundle> {
    let sha256 = normal_route_source_bundle_sha256(&expectation.source.bundle)?;
    let size = expectation.source.bundle.size;
    let modules = Arc::new(normal_route_source_modules(expectation)?);
    Ok(NormalRouteSharedSourceBundle {
        modules,
        sha256,
        size,
    })
}

fn normal_route_shared_source_bundle_is_authenticated(
    expectation: &NormalRouteTestExpectation,
    source_bundle: &NormalRouteSharedSourceBundle,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        normal_route_source_bundle_sha256(&expectation.source.bundle)? == source_bundle.sha256
            && expectation.source.bundle.size == source_bundle.size,
        "normal route batch cohort source bundle differs from its authenticated identity"
    );
    normal_route_selected_source_module_is_authenticated(expectation, &source_bundle.modules)
}

fn normal_route_source_package_sha256(
    expectation: &NormalRouteTestExpectation,
) -> anyhow::Result<Sha256Digest> {
    let source_package_sha256 = normal_route_sha256(
        &expectation.source.package.sha256,
        "normal route source-package file identity",
    )?;
    let expected_source_package_sha256 = normal_route_sha256(
        &expectation.source.source_package_sha256,
        "normal route source-package identity",
    )?;
    anyhow::ensure!(
        source_package_sha256 == expected_source_package_sha256,
        "normal route source package differs from its authenticated identity"
    );
    Ok(source_package_sha256)
}

fn normal_route_shared_source_package_is_authenticated(
    expectation: &NormalRouteTestExpectation,
    source_package: &SourcePackage,
) -> anyhow::Result<()> {
    let source_package_sha256 = normal_route_source_package_sha256(expectation)?;
    let runtime_content_sha256 = normal_route_sha256(
        &expectation.source.source_package_runtime_content_sha256,
        "normal route source-package runtime-content identity",
    )?;
    let runtime_generation = normal_route_runtime_generation(
        &expectation.deployment_sha256,
        &expectation.generation_manifest_sha256,
        &expectation.generation_sha256,
    )?;
    anyhow::ensure!(
        source_package.sha256 == source_package_sha256
            && source_package.runtime_content_sha256.as_ref() == Some(&runtime_content_sha256)
            && source_package.runtime_generation.as_ref() == Some(&runtime_generation)
            && source_package.package_size.zipped_size_bytes == expectation.source.package.size,
        "normal route batch cohort source package differs from its authenticated identity"
    );
    Ok(())
}

fn normal_route_runtime_generation(
    deployment: &str,
    generation_manifest: &str,
    generation: &str,
) -> anyhow::Result<SourcePackageRuntimeGeneration> {
    Ok(SourcePackageRuntimeGeneration {
        deployment_sha256: normal_route_sha256(deployment, "normal route deployment identity")?,
        generation_manifest_sha256: normal_route_sha256(
            generation_manifest,
            "normal route generation manifest identity",
        )?,
        generation_sha256: normal_route_sha256(generation, "normal route generation identity")?,
    })
}

fn normal_route_source_bundle_topology(
    bundle: &NormalRouteSourceBundleIdentity,
) -> anyhow::Result<NodeExecutorPoolTopology> {
    let modules = read_normal_route_source_bundle(bundle)?
        .app_definition
        .changed_modules
        .into_iter()
        .map(ModuleConfig::try_from)
        .collect::<anyhow::Result<Vec<_>>>()?;
    node_executor_pool_topology(&modules)
}

async fn upload_normal_route_source_package(
    expectation: &NormalRouteTestExpectation,
    storage: &Arc<dyn Storage>,
) -> anyhow::Result<SourcePackage> {
    let source_package_sha256 = normal_route_source_package_sha256(expectation)?;
    let runtime_content_sha256 = normal_route_sha256(
        &expectation.source.source_package_runtime_content_sha256,
        "normal route source-package runtime-content identity",
    )?;
    let source_package_bytes = std::fs::read(&expectation.source.package.path)
        .context("failed to read normal route source package")?;
    anyhow::ensure!(
        source_package_bytes.len() == expectation.source.package.size,
        "normal route source package size differs from its authenticated identity"
    );
    anyhow::ensure!(
        Sha256::hash(&source_package_bytes) == source_package_sha256,
        "normal route source package differs from its authenticated identity"
    );
    let source_package_size = source_package_bytes.len();
    // The full archive also contains Node actions. Preserve their pool topology
    // even when this test invokes only an isolate query or mutation.
    let node_executor_pool_topology =
        normal_route_source_bundle_topology(&expectation.source.bundle)?;
    let runtime_generation = normal_route_runtime_generation(
        &expectation.deployment_sha256,
        &expectation.generation_manifest_sha256,
        &expectation.generation_sha256,
    )?;
    let mut source_package_upload = storage.start_upload().await?;
    source_package_upload
        .write(Bytes::from(source_package_bytes))
        .await?;
    let storage_key = source_package_upload.complete().await?;
    Ok(SourcePackage {
        storage_key,
        sha256: source_package_sha256,
        runtime_content_sha256: Some(runtime_content_sha256),
        runtime_generation: Some(runtime_generation),
        external_deps_package_id: None,
        package_size: PackageSize {
            zipped_size_bytes: source_package_size,
            unzipped_size_bytes: 0,
        },
        node_version: None,
        node_executor_pool_topology,
    })
}

async fn new_normal_route_shared_source_package(
    rt: ProdRuntime,
    expectation: &NormalRouteTestExpectation,
) -> anyhow::Result<NormalRouteSharedSourcePackage> {
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt)?);
    let source_package = upload_normal_route_source_package(expectation, &storage).await?;
    Ok(NormalRouteSharedSourcePackage {
        source_package,
        storage,
    })
}

async fn normal_route_authenticated_schema(
    runner: &ApplicationFunctionRunner<ProdRuntime>,
    expectation: &NormalRouteTestExpectation,
    storage: &Arc<dyn Storage>,
    source_package: &SourcePackage,
) -> anyhow::Result<Arc<DatabaseSchema>> {
    let source_bundle_sha256 = &expectation.source.bundle.sha256;
    let _ = normal_route_sha256(source_bundle_sha256, "normal route source-bundle identity")?;
    let source_package_sha256 = &expectation.source.source_package_sha256;
    normal_route_shared_source_package_is_authenticated(expectation, source_package)?;
    if let Some(schema) = NORMAL_ROUTE_SCHEMA_CACHE
        .lock()
        .expect("normal route schema cache lock was poisoned")
        .get(source_package_sha256)
    {
        return Ok(Arc::clone(schema));
    }
    let source_modules = download_package(Arc::clone(storage), source_package).await?;
    let schema_path: CanonicalizedModulePath = "schema.js".parse()?;
    let schema_module = source_modules
        .get(&schema_path)
        .context("normal route source package omitted isolate schema.js")?;
    anyhow::ensure!(
        schema_module.environment == ModuleEnvironment::Isolate
            && schema_module.node_pool.is_none(),
        "normal route source package schema.js must be an isolate module without a Node pool"
    );
    let schema = Arc::new(
        runner
            .evaluate_schema(
                schema_module.source.clone(),
                schema_module.source_map.clone(),
                [0; 32],
                UnixTimestamp::from_nanos(0),
            )
            .await?,
    );
    NORMAL_ROUTE_SCHEMA_CACHE
        .lock()
        .expect("normal route schema cache lock was poisoned")
        .insert(source_package_sha256.clone(), Arc::clone(&schema));
    Ok(schema)
}

async fn materialize_normal_route_schema(
    tx: &mut database::Transaction<ProdRuntime>,
    schema: &DatabaseSchema,
) -> anyhow::Result<()> {
    for table_definition in schema.tables.values() {
        let table = &table_definition.table_name;
        if !TableModel::new(tx).table_exists(TableNamespace::root_component(), table) {
            TableModel::new(tx)
                .insert_table_metadata(TableNamespace::root_component(), table)
                .await?;
        }
        for index_schema in table_definition.indexes.values() {
            IndexModel::new(tx)
                .add_application_index(
                    TableNamespace::root_component(),
                    IndexMetadata::new_enabled(
                        IndexName::new(table.clone(), index_schema.index_descriptor.clone())?,
                        index_schema.fields.clone(),
                    ),
                )
                .await?;
        }
    }
    Ok(())
}

fn normal_route_table_mapping_sha256(
    table_entries: impl IntoIterator<Item = (String, u32)>,
) -> anyhow::Result<String> {
    let mut tables = BTreeMap::new();
    let mut table_numbers = BTreeSet::new();
    for (table, table_number) in table_entries {
        anyhow::ensure!(
            tables.insert(table, table_number).is_none() && table_numbers.insert(table_number),
            "normal route table mapping contains a duplicate table name or number"
        );
    }
    Ok(Sha256::hash(&serde_json::to_vec(&tables)?).as_hex())
}

async fn normal_route_paired_lane_initial_table_mapping_sha256(
    database: &Database<ProdRuntime>,
) -> anyhow::Result<String> {
    let mut tx = database.begin_system().await?;
    normal_route_table_mapping_sha256(tx.table_mapping().iter_active_user_tables().filter_map(
        |(_, namespace, table_number, table)| {
            (namespace == TableNamespace::root_component())
                .then(|| (table.to_string(), u32::from(table_number)))
        },
    ))
}

fn normal_route_sequence_source_modules(
    expectation: &NormalRouteSequenceTestExpectation,
) -> anyhow::Result<BTreeMap<CanonicalizedModulePath, Arc<FullModuleSource>>> {
    let modules = load_normal_route_source_modules(&expectation.source.bundle)?;
    let mut selected_paths = BTreeSet::new();
    for expected in &expectation.source.modules {
        let path: CanonicalizedModulePath = expected.runtime_module_path.parse()?;
        anyhow::ensure!(
            selected_paths.insert(path.clone()),
            "normal route sequence repeats an authenticated source module"
        );
        let selected = modules
            .get(&path)
            .context("normal route source bundle omitted a sequence module")?;
        anyhow::ensure!(
            &*selected.source == expected.module_source.as_str()
                && selected.source_map == expected.source_map,
            "normal route sequence module differs from its authenticated source"
        );
        let expected_sha256 = normal_route_sha256(
            &expected.module_sha256,
            "normal route sequence module identity",
        )?;
        anyhow::ensure!(
            model::modules::hash_module_source(&selected.source, selected.source_map.as_ref())
                == expected_sha256,
            "normal route sequence module bytes do not match the producer identity"
        );
    }
    let routed_paths = expectation
        .sequence
        .steps
        .iter()
        .map(|step| step.route.runtime_module_path.parse())
        .collect::<anyhow::Result<BTreeSet<CanonicalizedModulePath>>>()?;
    anyhow::ensure!(
        !selected_paths.is_empty() && selected_paths == routed_paths,
        "normal route sequence authenticated source modules differ from its routed modules"
    );
    Ok(modules)
}

fn normal_route_scheduled_module(
    bundle_identity: &NormalRouteSourceBundleIdentity,
    source: &NormalRouteScheduledSourceModule,
    scheduled_function_address: &str,
) -> anyhow::Result<NormalRouteScheduledModule> {
    let (module_path, export_name) = scheduled_function_address
        .split_once(':')
        .context("normal route scheduled-function address has no export")?;
    anyhow::ensure!(
        module_path.ends_with(".js")
            && !export_name.is_empty()
            && !export_name.contains(':')
            && source.environment == "node"
            && source.runtime_module_path == module_path
            && source.export_name == export_name,
        "normal route scheduled-function identity is invalid"
    );
    let bundle = read_normal_route_source_bundle(bundle_identity)?;
    let mut matching_modules = bundle
        .app_definition
        .changed_modules
        .into_iter()
        .filter(|module| {
            module.path == module_path && module.environment.as_deref() == Some("node")
        });
    let module = matching_modules
        .next()
        .context("normal route source bundle omitted its scheduled Node module")?;
    anyhow::ensure!(
        matching_modules.next().is_none(),
        "normal route source bundle repeats its scheduled Node module"
    );
    let expected_sha256 = normal_route_sha256(
        &source.module_sha256,
        "normal route scheduled Node module identity",
    )?;
    let full_source = Arc::new(FullModuleSource {
        source: ModuleSource::new(&module.source),
        source_map: module.source_map,
    });
    anyhow::ensure!(
        &*full_source.source == source.module_source.as_str()
            && full_source.source_map == source.source_map
            && model::modules::hash_module_source(
                &full_source.source,
                full_source.source_map.as_ref()
            ) == expected_sha256,
        "normal route scheduled Node module differs from its authenticated source"
    );
    let visibility = match source.visibility.as_str() {
        "internal" => Visibility::Internal,
        "public" => Visibility::Public,
        _ => anyhow::bail!("normal route scheduled function has unsupported visibility"),
    };
    let udf_type: UdfType = source.udf_kind.parse()?;
    anyhow::ensure!(
        matches!(udf_type, UdfType::Action),
        "normal route scheduled function must be an action"
    );
    Ok(NormalRouteScheduledModule {
        export_name: export_name.to_owned(),
        module_path: module_path.parse()?,
        source: full_source,
        udf_type,
        visibility,
    })
}

fn resolve_normal_route_file_storage_reference(
    value: &JsonValue,
    developer_id: &str,
    reference_count: &mut usize,
) -> anyhow::Result<JsonValue> {
    match value {
        JsonValue::Array(values) => Ok(JsonValue::Array(
            values
                .iter()
                .map(|value| {
                    resolve_normal_route_file_storage_reference(
                        value,
                        developer_id,
                        reference_count,
                    )
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        )),
        JsonValue::Object(fields) if fields.contains_key("$fileStorageId") => {
            anyhow::ensure!(
                fields.len() == 1 && fields.get("$fileStorageId") == Some(&json!("file")),
                "file-storage reference must contain exactly the declared file alias"
            );
            *reference_count += 1;
            Ok(json!(developer_id))
        },
        JsonValue::Object(fields) => Ok(JsonValue::Object(
            fields
                .iter()
                .map(|(field, value)| {
                    Ok((
                        field.clone(),
                        resolve_normal_route_file_storage_reference(
                            value,
                            developer_id,
                            reference_count,
                        )?,
                    ))
                })
                .collect::<anyhow::Result<serde_json::Map<_, _>>>()?,
        )),
        _ => Ok(value.clone()),
    }
}

fn substitute_normal_route_file_storage_id(
    invocation_arguments: &JsonValue,
    developer_id: &str,
) -> anyhow::Result<JsonValue> {
    anyhow::ensure!(
        invocation_arguments.is_object(),
        "file-storage invocation arguments must be an object"
    );
    let mut reference_count = 0;
    let resolved = resolve_normal_route_file_storage_reference(
        invocation_arguments,
        developer_id,
        &mut reference_count,
    )?;
    anyhow::ensure!(
        reference_count == 1,
        "file-storage invocation arguments must contain exactly one file reference"
    );
    Ok(resolved)
}

fn validate_normal_route_file_storage_setup(
    content_base64: &str,
    content_type: &str,
    expected_result: &str,
    invocation_arguments: &JsonValue,
    storage_uuid: &str,
) -> anyhow::Result<NormalRouteValidatedFileStorageSetup> {
    let content =
        base64::decode(content_base64).context("file-storage content is not valid base64")?;
    anyhow::ensure!(
        !content.is_empty()
            && content.len() <= 65_536
            && base64::encode(&content) == content_base64,
        "file-storage content must be nonempty canonical base64 under 64 KiB"
    );
    anyhow::ensure!(
        !content_type.is_empty()
            && content_type.encode_utf16().count() <= 255
            && content_type
                .chars()
                .all(|character| !matches!(character as u32, 0..=31 | 127)),
        "file-storage content type is invalid"
    );
    let parsed_storage_uuid: StorageUuid = storage_uuid
        .parse()
        .context("file-storage UUID is invalid")?;
    anyhow::ensure!(
        parsed_storage_uuid.to_string() == storage_uuid,
        "file-storage UUID must use canonical lowercase form"
    );
    let expected_url =
        url::Url::parse(expected_result).context("file-storage expected result is not a URL")?;
    anyhow::ensure!(
        matches!(expected_url.scheme(), "http" | "https")
            && expected_url.host_str().is_some()
            && expected_url.username().is_empty()
            && expected_url.password().is_none()
            && expected_url.query().is_none()
            && expected_url.fragment().is_none()
            && expected_url.path() == format!("/api/storage/{parsed_storage_uuid}")
            && expected_url.as_str() == expected_result,
        "file-storage expected result must be the exact storage URL for its UUID"
    );
    substitute_normal_route_file_storage_id(invocation_arguments, "validated-storage-id")?;
    Ok(NormalRouteValidatedFileStorageSetup {
        content: Bytes::from(content),
        content_type: content_type.to_owned(),
        expected_result: json!(expected_result),
        invocation_arguments: invocation_arguments.clone(),
        storage_uuid: parsed_storage_uuid,
    })
}

async fn install_normal_route_file_storage_fixture(
    tx: &mut database::Transaction<ProdRuntime>,
    storage: &Arc<dyn Storage>,
    setup: &NormalRouteValidatedFileStorageSetup,
) -> anyhow::Result<NormalRouteFileStorageState> {
    let mut upload = storage.start_upload().await?;
    upload.write(setup.content.clone()).await?;
    let storage_key = upload.complete().await?;
    let entry = FileStorageEntry {
        storage_id: setup.storage_uuid.clone(),
        storage_key,
        sha256: Sha256::hash(&setup.content),
        size: setup.content.len().try_into()?,
        content_type: Some(setup.content_type.clone()),
    };
    let system_document_id = FileStorageModel::new(tx, TableNamespace::root_component())
        .store_file(entry.clone())
        .await?;
    let developer_id = tx
        .virtual_system_mapping()
        .system_resolved_id_to_virtual_developer_id(system_document_id)?;
    let invocation_arguments = substitute_normal_route_file_storage_id(
        &setup.invocation_arguments,
        &developer_id.encode(),
    )?;
    Ok(NormalRouteFileStorageState {
        content: setup.content.clone(),
        developer_id,
        entry,
        expected_result: setup.expected_result.clone(),
        invocation_arguments,
    })
}

async fn verify_normal_route_file_storage_fixture(
    database: &Database<ProdRuntime>,
    storage: &Arc<dyn Storage>,
    fixture: &NormalRouteFileStorageState,
) -> anyhow::Result<()> {
    let mut tx = database.begin_system().await?;
    let document_entry = FileStorageModel::new(&mut tx, TableNamespace::root_component())
        .get_file(FileStorageId::DocumentId(fixture.developer_id.clone()))
        .await?
        .context("file-storage fixture metadata is unavailable by developer ID")?;
    let uuid_entry = FileStorageModel::new(&mut tx, TableNamespace::root_component())
        .get_file(FileStorageId::LegacyStorageId(
            fixture.entry.storage_id.clone(),
        ))
        .await?
        .context("file-storage fixture metadata is unavailable by storage UUID")?;
    anyhow::ensure!(
        document_entry.id() == uuid_entry.id()
            && document_entry.into_value() == fixture.entry
            && uuid_entry.into_value() == fixture.entry,
        "file-storage fixture metadata changed"
    );
    drop(tx);
    let stored = storage
        .get(&fixture.entry.storage_key)
        .await?
        .context("file-storage fixture object is unavailable")?;
    anyhow::ensure!(
        stored.content_length == fixture.content.len() as i64,
        "file-storage fixture object size changed"
    );
    let stored_content = stored
        .stream
        .try_fold(Vec::new(), |mut content, chunk| async move {
            content.extend_from_slice(&chunk);
            Ok(content)
        })
        .await?;
    anyhow::ensure!(
        stored_content == fixture.content,
        "file-storage fixture object content changed"
    );
    Ok(())
}

async fn install_normal_route_fixture(
    database: &Database<ProdRuntime>,
    expectation: &NormalRouteTestExpectation,
    paired_lane_schema: Option<&DatabaseSchema>,
    modules: &BTreeMap<CanonicalizedModulePath, Arc<FullModuleSource>>,
    source_package: SourcePackage,
    storage: &Arc<dyn Storage>,
) -> anyhow::Result<NormalRouteFixtureState> {
    let _ = normal_route_sha256(&expectation.routing.entry_id, "normal route entry identity")?;
    let _ = normal_route_sha256(&expectation.routing.route_id, "normal route route identity")?;
    anyhow::ensure!(
        expectation.routing.entry_selector_id.len() == 16
            && expectation
                .routing
                .entry_selector_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            && expectation.routing.route_identity
                == serde_json::to_string(&[
                    expectation.route.runtime_module_path.as_str(),
                    expectation.route.export_name.as_str(),
                ])?,
        "normal route routing identity is invalid"
    );
    let validated_file_storage_setup = match &expectation.setup {
        NormalRouteTestSetup::FileStorageGetUrlV1 {
            content_base64,
            content_type,
            expected_result,
            invocation_arguments,
            storage_uuid,
        } => Some(validate_normal_route_file_storage_setup(
            content_base64,
            content_type,
            expected_result,
            invocation_arguments,
            storage_uuid,
        )?),
        _ => None,
    };
    let visibility = match &expectation.setup {
        NormalRouteTestSetup::ApplicationProbe { .. } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "query" && expectation.route.visibility == "public",
                "application probe fixture must select a public query"
            );
            Visibility::Public
        },
        NormalRouteTestSetup::ApplicationProbePatch { .. } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "mutation"
                    && expectation.route.visibility == "internal",
                "application probe patch fixture must select an internal mutation"
            );
            Visibility::Internal
        },
        NormalRouteTestSetup::ApiKeyValidation {
            first_api_key,
            second_api_key,
            ..
        } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "query" && expectation.route.visibility == "internal",
                "API-key validation fixture must select an internal query"
            );
            anyhow::ensure!(
                !first_api_key.is_empty()
                    && !second_api_key.is_empty()
                    && first_api_key != second_api_key,
                "API-key validation fixture inputs must be nonempty and distinct"
            );
            Visibility::Internal
        },
        NormalRouteTestSetup::CollectDelete {
            expected_counts,
            result_fields,
            synthetic_rows,
        } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "mutation"
                    && matches!(expectation.route.visibility.as_str(), "internal" | "public"),
                "collect-delete fixture must select an authenticated mutation"
            );
            anyhow::ensure!(
                expected_counts.as_slice() == [2, 0]
                    && synthetic_rows.len() == 2
                    && synthetic_rows.iter().all(|row| {
                        row.as_object().is_some_and(|fields| {
                            !fields.contains_key("_id") && !fields.contains_key("_creationTime")
                        })
                    })
                    && synthetic_rows[0] != synthetic_rows[1],
                "collect-delete fixture must declare two distinct synthetic rows and counts 2 \
                 then 0"
            );
            anyhow::ensure!(
                !result_fields.is_empty()
                    && result_fields.keys().all(|field| !field.is_empty())
                    && result_fields.values().all(|table| !table.is_empty())
                    && result_fields.values().collect::<BTreeSet<_>>().len() == result_fields.len(),
                "collect-delete fixture must map result fields to distinct tables"
            );
            match expectation.route.visibility.as_str() {
                "internal" => Visibility::Internal,
                "public" => Visibility::Public,
                _ => unreachable!("visibility was checked above"),
            }
        },
        NormalRouteTestSetup::EmptyIndexedQueryV1 {
            application_index,
            application_index_fields,
            application_table,
            expected_result: _,
            invocation_arguments,
        } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "query"
                    && matches!(expectation.route.visibility.as_str(), "internal" | "public"),
                "empty indexed query fixture must select an authenticated query"
            );
            anyhow::ensure!(
                !application_index.is_empty()
                    && !application_table.is_empty()
                    && !application_index_fields.is_empty()
                    && application_index_fields.iter().all(|field| {
                        !field.is_empty() && field.parse::<value::FieldPath>().is_ok()
                    })
                    && application_index_fields
                        .iter()
                        .collect::<BTreeSet<_>>()
                        .len()
                        == application_index_fields.len()
                    && invocation_arguments.is_object(),
                "empty indexed query fixture contract is invalid"
            );
            match expectation.route.visibility.as_str() {
                "internal" => Visibility::Internal,
                "public" => Visibility::Public,
                _ => unreachable!("visibility was checked above"),
            }
        },
        NormalRouteTestSetup::SeededDocumentsQueryV1 {
            application_index,
            application_index_fields,
            application_table,
            documents,
            invocation_arguments,
            ..
        } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "query"
                    && matches!(expectation.route.visibility.as_str(), "internal" | "public"),
                "seeded-documents query fixture must select an authenticated query"
            );
            anyhow::ensure!(
                !application_index.is_empty()
                    && !application_table.is_empty()
                    && !application_index_fields.is_empty()
                    && application_index_fields.iter().all(|field| {
                        !field.is_empty() && field.parse::<value::FieldPath>().is_ok()
                    })
                    && application_index_fields
                        .iter()
                        .collect::<BTreeSet<_>>()
                        .len()
                        == application_index_fields.len()
                    && !documents.is_empty()
                    && invocation_arguments.is_object(),
                "seeded-documents query fixture contract is invalid"
            );
            for document in documents {
                let fields = document
                    .as_object()
                    .context("seeded-documents query document is not an object")?;
                anyhow::ensure!(
                    !fields.contains_key("_id") && !fields.contains_key("_creationTime"),
                    "seeded-documents query document must omit system fields"
                );
            }
            match expectation.route.visibility.as_str() {
                "internal" => Visibility::Internal,
                "public" => Visibility::Public,
                _ => unreachable!("visibility was checked above"),
            }
        },
        NormalRouteTestSetup::FileStorageGetUrlV1 { .. } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "query" && expectation.route.visibility == "internal",
                "file-storage fixture must select an internal query"
            );
            Visibility::Internal
        },
        NormalRouteTestSetup::IndexedPaginationQueryV1 {
            application_index,
            application_index_fields,
            application_table,
            invocations,
            seeded_rows,
            warmup_invocation_count,
        } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "query"
                    && matches!(expectation.route.visibility.as_str(), "internal" | "public"),
                "indexed pagination fixture must select an authenticated query"
            );
            anyhow::ensure!(
                *warmup_invocation_count == 0
                    && !application_index.is_empty()
                    && !application_table.is_empty()
                    && !application_index_fields.is_empty()
                    && application_index_fields.iter().all(|field| {
                        !field.is_empty() && field.parse::<value::FieldPath>().is_ok()
                    })
                    && application_index_fields
                        .iter()
                        .collect::<BTreeSet<_>>()
                        .len()
                        == application_index_fields.len()
                    && invocations.len() == 2
                    && !seeded_rows.is_empty(),
                "indexed pagination fixture contract is invalid"
            );
            let mut aliases = BTreeSet::new();
            let mut indexed_row_count = 0;
            for row in seeded_rows {
                anyhow::ensure!(
                    !row.alias.is_empty()
                        && row.alias.len() <= 64
                        && row
                            .alias
                            .bytes()
                            .next()
                            .is_some_and(|byte| byte.is_ascii_alphabetic())
                        && row.alias.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
                        })
                        && aliases.insert(row.alias.clone())
                        && !row.table.is_empty(),
                    "indexed pagination fixture has an invalid or repeated row alias"
                );
                let fields = row
                    .value
                    .as_object()
                    .context("indexed pagination fixture row value is not an object")?;
                anyhow::ensure!(
                    !fields.contains_key("_id") && !fields.contains_key("_creationTime"),
                    "indexed pagination fixture row must omit system fields"
                );
                if row.table == *application_table {
                    indexed_row_count += 1;
                    anyhow::ensure!(
                        application_index_fields
                            .iter()
                            .all(|field| fields.contains_key(field)),
                        "indexed pagination row omits an application index field"
                    );
                }
            }
            anyhow::ensure!(
                indexed_row_count > 0,
                "indexed pagination fixture has no row in its indexed table"
            );
            let mut argument_counts = Vec::with_capacity(invocations.len());
            let mut result_counts = Vec::with_capacity(invocations.len());
            for invocation in invocations {
                anyhow::ensure!(
                    invocation.arguments.is_object(),
                    "indexed pagination invocation arguments must be an object"
                );
                let mut arguments = NormalRoutePaginationTemplateCounts::default();
                count_normal_route_pagination_template(&invocation.arguments, &mut arguments)?;
                let mut result = NormalRoutePaginationTemplateCounts::default();
                count_normal_route_pagination_template(&invocation.expected_result, &mut result)?;
                anyhow::ensure!(
                    arguments
                        .fixture_reference_counts
                        .keys()
                        .chain(result.fixture_reference_counts.keys())
                        .all(|alias| aliases.contains(alias)),
                    "indexed pagination template names an unseeded fixture alias"
                );
                argument_counts.push(arguments);
                result_counts.push(result);
            }
            anyhow::ensure!(
                argument_counts[0].capture_cursor_count == 0
                    && argument_counts[0].previous_cursor_count == 0
                    && result_counts[0].capture_cursor_count == 1
                    && result_counts[0].previous_cursor_count == 0
                    && argument_counts[1].capture_cursor_count == 0
                    && argument_counts[1].previous_cursor_count == 1
                    && result_counts[1].capture_cursor_count == 0
                    && result_counts[1].previous_cursor_count == 0,
                "indexed pagination fixture must capture page 1 cursor and pass it once to page 2"
            );
            let first_result_aliases = result_counts[0]
                .fixture_reference_counts
                .keys()
                .collect::<BTreeSet<_>>();
            let second_result_aliases = result_counts[1]
                .fixture_reference_counts
                .keys()
                .collect::<BTreeSet<_>>();
            anyhow::ensure!(
                !first_result_aliases.is_empty()
                    && !second_result_aliases.is_empty()
                    && result_counts[0]
                        .fixture_reference_counts
                        .values()
                        .all(|count| *count == 1)
                    && result_counts[1]
                        .fixture_reference_counts
                        .values()
                        .all(|count| *count == 1)
                    && first_result_aliases.is_disjoint(&second_result_aliases),
                "indexed pagination expected pages contain duplicate fixture IDs"
            );
            match expectation.route.visibility.as_str() {
                "internal" => Visibility::Internal,
                "public" => Visibility::Public,
                _ => unreachable!("visibility was checked above"),
            }
        },
        NormalRouteTestSetup::ObjectPatchDeleteV1 {
            deleted_fields,
            expected_results,
            invocation_arguments,
            restore_fields_before_measured_invocation,
            seeded_rows,
            target_alias,
        } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "mutation"
                    && matches!(expectation.route.visibility.as_str(), "internal" | "public"),
                "object patch fixture must select an authenticated mutation"
            );
            anyhow::ensure!(
                invocation_arguments.is_object()
                    && expected_results.len() == 2
                    && !seeded_rows.is_empty(),
                "object patch fixture must declare arguments, seeded rows, and two measured \
                 results"
            );
            let mut aliases = BTreeSet::new();
            for row in seeded_rows {
                anyhow::ensure!(
                    !row.alias.is_empty()
                        && row.alias.len() <= 64
                        && row
                            .alias
                            .bytes()
                            .next()
                            .is_some_and(|byte| byte.is_ascii_alphabetic())
                        && row.alias.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
                        })
                        && aliases.insert(row.alias.clone())
                        && !row.table.is_empty(),
                    "object patch fixture has an invalid or repeated row alias"
                );
                let fields = row
                    .value
                    .as_object()
                    .context("object patch fixture row value is not an object")?;
                anyhow::ensure!(
                    !fields.contains_key("_id") && !fields.contains_key("_creationTime"),
                    "object patch fixture row must omit system fields"
                );
            }
            let target = seeded_rows
                .iter()
                .find(|row| row.alias == *target_alias)
                .context("object patch fixture target alias is unavailable")?;
            let target_fields = target
                .value
                .as_object()
                .context("object patch fixture target value is not an object")?;
            let deleted_field_set = deleted_fields.iter().collect::<BTreeSet<_>>();
            anyhow::ensure!(
                !deleted_fields.is_empty()
                    && deleted_field_set.len() == deleted_fields.len()
                    && deleted_fields.iter().all(|field| {
                        field.parse::<value::FieldName>().is_ok()
                            && target_fields.contains_key(field)
                    }),
                "object patch fixture deleted fields are invalid"
            );
            if let Some(restore_fields) = restore_fields_before_measured_invocation {
                let fields = restore_fields
                    .as_object()
                    .context("object patch fixture restore fields are not an object")?;
                anyhow::ensure!(
                    !fields.is_empty()
                        && fields.keys().all(|field| deleted_field_set.contains(field)),
                    "object patch fixture restore fields are not deleted fields"
                );
            }
            match expectation.route.visibility.as_str() {
                "internal" => Visibility::Internal,
                "public" => Visibility::Public,
                _ => unreachable!("visibility was checked above"),
            }
        },
        NormalRouteTestSetup::SeededDocumentsMutationV1 {
            application_index,
            application_index_fields,
            application_table,
            documents,
            expected_read_accounting,
            invocation_arguments,
            ..
        } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "mutation"
                    && matches!(expectation.route.visibility.as_str(), "internal" | "public"),
                "seeded-documents mutation fixture must select an authenticated mutation"
            );
            anyhow::ensure!(
                expected_read_accounting.is_none()
                    || matches!(
                        expected_read_accounting,
                        Some(NormalRouteExpectedReadAccounting::None)
                    ),
                "seeded-documents mutation fixture read accounting is invalid"
            );
            anyhow::ensure!(
                !application_index.is_empty()
                    && !application_table.is_empty()
                    && !application_index_fields.is_empty()
                    && application_index_fields.iter().all(|field| {
                        !field.is_empty() && field.parse::<value::FieldPath>().is_ok()
                    })
                    && application_index_fields
                        .iter()
                        .collect::<BTreeSet<_>>()
                        .len()
                        == application_index_fields.len()
                    && !documents.is_empty()
                    && invocation_arguments.is_object(),
                "seeded-documents mutation fixture contract is invalid"
            );
            for document in documents {
                let fields = document
                    .as_object()
                    .context("seeded-documents mutation document is not an object")?;
                anyhow::ensure!(
                    !fields.contains_key("_id") && !fields.contains_key("_creationTime"),
                    "seeded-documents mutation document must omit system fields"
                );
            }
            match expectation.route.visibility.as_str() {
                "internal" => Visibility::Internal,
                "public" => Visibility::Public,
                _ => unreachable!("visibility was checked above"),
            }
        },
        NormalRouteTestSetup::PairedLaneV1 {
            invocations,
            seeded_rows,
            ..
        } => {
            anyhow::ensure!(
                matches!(expectation.route.udf_kind.as_str(), "query" | "mutation")
                    && matches!(expectation.route.visibility.as_str(), "internal" | "public")
                    && (2..=16).contains(&invocations.len())
                    && seeded_rows.len() <= 128,
                "paired-lane fixture route or bounds are invalid"
            );
            let mut aliases = BTreeSet::new();
            for row in seeded_rows {
                anyhow::ensure!(
                    !row.alias.is_empty()
                        && row.alias.len() <= 64
                        && row
                            .alias
                            .bytes()
                            .next()
                            .is_some_and(|byte| byte.is_ascii_alphabetic())
                        && row.alias.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
                        })
                        && aliases.insert(row.alias.clone())
                        && !row.table.is_empty()
                        && row.value.as_object().is_some_and(|fields| {
                            !fields.contains_key("_id") && !fields.contains_key("_creationTime")
                        }),
                    "paired-lane fixture seeded row is invalid"
                );
            }
            anyhow::ensure!(
                invocations.iter().all(JsonValue::is_object),
                "paired-lane fixture invocation arguments must be objects"
            );
            for invocation in invocations {
                let mut counts = NormalRoutePaginationTemplateCounts::default();
                count_normal_route_pagination_template(invocation, &mut counts)?;
                anyhow::ensure!(
                    counts.capture_cursor_count == 0
                        && counts.previous_cursor_count == 0
                        && counts
                            .fixture_reference_counts
                            .keys()
                            .all(|alias| aliases.contains(alias)),
                    "paired-lane invocation contains an invalid cursor or fixture reference"
                );
            }
            match expectation.route.visibility.as_str() {
                "internal" => Visibility::Internal,
                "public" => Visibility::Public,
                _ => unreachable!("visibility was checked above"),
            }
        },
        NormalRouteTestSetup::StatefulQueryPatchV1 {
            invocations,
            post_state_field_policies,
            seeded_rows,
            warmup_invocation_count,
        } => {
            anyhow::ensure!(
                expectation.route.udf_kind == "mutation"
                    && matches!(expectation.route.visibility.as_str(), "internal" | "public"),
                "stateful query patch fixture must select an authenticated mutation"
            );
            anyhow::ensure!(
                *warmup_invocation_count == 0
                    && invocations.len() == 2
                    && invocations.iter().all(|invocation| {
                        invocation.arguments.is_object()
                            && !invocation.expected_capability_stages.is_empty()
                            && invocation.expected_capability_stages.iter().all(|stage| {
                                matches!(stage.as_str(), "start:db-query" | "start:db-patch")
                            })
                            && invocation
                                .expected_capability_stages
                                .iter()
                                .any(|stage| stage == "start:db-query")
                    })
                    && invocations.iter().any(|invocation| {
                        invocation
                            .expected_capability_stages
                            .iter()
                            .any(|stage| stage == "start:db-patch")
                    })
                    && !seeded_rows.is_empty()
                    && post_state_field_policies.len() == seeded_rows.len(),
                "stateful query patch fixture contract is invalid"
            );
            let mut aliases = BTreeSet::new();
            for row in seeded_rows {
                anyhow::ensure!(
                    !row.alias.is_empty()
                        && row.alias.len() <= 64
                        && row
                            .alias
                            .bytes()
                            .next()
                            .is_some_and(|byte| byte.is_ascii_alphabetic())
                        && row.alias.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
                        })
                        && aliases.insert(row.alias.clone())
                        && !row.table.is_empty(),
                    "stateful query patch fixture has an invalid or repeated row alias"
                );
                let fields = row
                    .value
                    .as_object()
                    .context("stateful query patch fixture row value is not an object")?;
                let policies = post_state_field_policies
                    .get(&row.alias)
                    .context("stateful query patch fixture omitted a seeded row policy")?;
                anyhow::ensure!(
                    !fields.contains_key("_id")
                        && !fields.contains_key("_creationTime")
                        && fields.len() == policies.len()
                        && policies.keys().all(|field| {
                            field.parse::<value::FieldName>().is_ok() && fields.contains_key(field)
                        })
                        && policies.iter().all(|(field, policy)| {
                            !matches!(policy, NormalRoutePostStateFieldPolicy::IncreasedNumber)
                                || fields.get(field).is_some_and(JsonValue::is_number)
                        }),
                    "stateful query patch fixture field policies are invalid"
                );
            }
            anyhow::ensure!(
                post_state_field_policies
                    .keys()
                    .all(|alias| aliases.contains(alias)),
                "stateful query patch fixture policy names an unseeded alias"
            );
            match expectation.route.visibility.as_str() {
                "internal" => Visibility::Internal,
                "public" => Visibility::Public,
                _ => unreachable!("visibility was checked above"),
            }
        },
    };
    let scheduled_module = match &expectation.setup {
        NormalRouteTestSetup::ApplicationProbePatch {
            scheduled_function_address,
            ..
        } => Some(normal_route_scheduled_module(
            &expectation.source.bundle,
            expectation
                .source
                .scheduled_module
                .as_ref()
                .context("application patch fixture omitted its scheduled Node module")?,
            scheduled_function_address,
        )?),
        _ => {
            anyhow::ensure!(
                expectation.source.scheduled_module.is_none(),
                "normal route fixture supplied an unused scheduled Node module"
            );
            None
        },
    };
    initialize_application_system_tables(database).await?;
    let mut tx = database.begin_system().await?;
    match (&expectation.setup, paired_lane_schema) {
        (NormalRouteTestSetup::PairedLaneV1 { .. }, Some(schema)) => {
            materialize_normal_route_schema(&mut tx, schema).await?;
        },
        (NormalRouteTestSetup::PairedLaneV1 { .. }, None) => {
            anyhow::bail!("paired-lane fixture is missing its authenticated schema")
        },
        (_, Some(_)) => {
            anyhow::bail!("non-paired fixture supplied an authenticated schema")
        },
        (_, None) => {},
    }
    UdfConfigModel::new(&mut tx, TableNamespace::root_component())
        .set(UdfConfig {
            server_version: semver::Version::new(1, 99, 0),
            import_phase_rng_seed: [0; 32],
            import_phase_unix_timestamp: UnixTimestamp::from_nanos(0),
        })
        .await?;

    let source_package_id = SourcePackageModel::new(&mut tx, TableNamespace::root_component())
        .put(source_package)
        .await?;
    let module_path: CanonicalizedModulePath = expectation.route.runtime_module_path.parse()?;
    let udf_type = match expectation.route.udf_kind.as_str() {
        "query" => UdfType::Query,
        "mutation" => UdfType::Mutation,
        _ => anyhow::bail!("normal route fixture selected an unsupported UDF kind"),
    };
    for (path, module) in modules {
        let analyze_result = if path.is_deps() {
            None
        } else {
            let mut functions = Vec::new();
            let mut names = BTreeSet::new();
            if path == &module_path {
                names.insert(expectation.route.export_name.clone());
                functions.push(AnalyzedFunction::new(
                    expectation.route.export_name.parse()?,
                    None,
                    udf_type,
                    Some(visibility.clone()),
                    ArgsValidator::Unvalidated,
                    ReturnsValidator::Unvalidated,
                )?);
            }
            for sibling in &expectation.source.additional_analyzed_exports {
                let sibling_path: CanonicalizedModulePath = sibling.runtime_module_path.parse()?;
                if &sibling_path != path {
                    continue;
                }
                anyhow::ensure!(
                    names.insert(sibling.export_name.clone()),
                    "normal route fixture repeats an analyzed export"
                );
                let sibling_type = match sibling.udf_kind.as_str() {
                    "query" => UdfType::Query,
                    "mutation" => UdfType::Mutation,
                    _ => {
                        anyhow::bail!("normal route additional export has an unsupported UDF kind")
                    },
                };
                let sibling_visibility = match sibling.visibility.as_str() {
                    "internal" => Visibility::Internal,
                    "public" => Visibility::Public,
                    _ => anyhow::bail!(
                        "normal route additional export has an unsupported visibility"
                    ),
                };
                functions.push(AnalyzedFunction::new(
                    sibling.export_name.parse()?,
                    None,
                    sibling_type,
                    Some(sibling_visibility),
                    ArgsValidator::Unvalidated,
                    ReturnsValidator::Unvalidated,
                )?);
            }
            Some(AnalyzedModule {
                functions: functions.into(),
                ..Default::default()
            })
        };
        ModuleModel::new(&mut tx)
            .put(
                None,
                CanonicalizedComponentModulePath {
                    component: ComponentId::Root,
                    module_path: path.clone(),
                },
                module.source.clone(),
                source_package_id,
                module.source_map.clone(),
                analyze_result,
                ModuleEnvironment::Isolate,
                None,
            )
            .await?;
    }
    if let Some(scheduled_module) = scheduled_module {
        anyhow::ensure!(
            !modules.contains_key(&scheduled_module.module_path),
            "normal route scheduled Node module overlaps a routed isolate module"
        );
        ModuleModel::new(&mut tx)
            .put(
                None,
                CanonicalizedComponentModulePath {
                    component: ComponentId::Root,
                    module_path: scheduled_module.module_path,
                },
                scheduled_module.source.source.clone(),
                source_package_id,
                scheduled_module.source.source_map.clone(),
                Some(AnalyzedModule {
                    functions: vec![AnalyzedFunction::new(
                        scheduled_module.export_name.parse()?,
                        None,
                        scheduled_module.udf_type,
                        Some(scheduled_module.visibility),
                        ArgsValidator::Unvalidated,
                        ReturnsValidator::Unvalidated,
                    )?]
                    .into(),
                    ..Default::default()
                }),
                ModuleEnvironment::Node,
                None,
            )
            .await?;
    }
    let component_module_path = CanonicalizedComponentModulePath {
        component: ComponentId::Root,
        module_path: module_path.clone(),
    };
    let module = ModuleModel::new(&mut tx)
        .get_metadata(component_module_path)
        .await?
        .context("normal route module was not installed")?;
    anyhow::ensure!(
        module.sha256.as_hex() == expectation.source.module_sha256,
        "normal route module bytes do not match the producer identity"
    );

    let application_index = match &expectation.setup {
        NormalRouteTestSetup::ApplicationProbe {
            application_index,
            application_index_fields,
            application_table,
            ..
        }
        | NormalRouteTestSetup::ApplicationProbePatch {
            application_index,
            application_index_fields,
            application_table,
            ..
        }
        | NormalRouteTestSetup::ApiKeyValidation {
            application_index,
            application_index_fields,
            application_table,
            ..
        }
        | NormalRouteTestSetup::IndexedPaginationQueryV1 {
            application_index,
            application_index_fields,
            application_table,
            ..
        }
        | NormalRouteTestSetup::EmptyIndexedQueryV1 {
            application_index,
            application_index_fields,
            application_table,
            ..
        }
        | NormalRouteTestSetup::SeededDocumentsQueryV1 {
            application_index,
            application_index_fields,
            application_table,
            ..
        } => Some((
            application_index,
            application_index_fields,
            application_table,
        )),
        NormalRouteTestSetup::CollectDelete { .. } => None,
        NormalRouteTestSetup::FileStorageGetUrlV1 { .. } => None,
        NormalRouteTestSetup::ObjectPatchDeleteV1 { .. } => None,
        NormalRouteTestSetup::SeededDocumentsMutationV1 {
            application_index,
            application_index_fields,
            application_table,
            ..
        } => Some((
            application_index,
            application_index_fields,
            application_table,
        )),
        NormalRouteTestSetup::PairedLaneV1 { .. } => None,
        NormalRouteTestSetup::StatefulQueryPatchV1 { .. } => None,
    };
    if let Some((application_index, application_index_fields, application_table)) =
        application_index
    {
        let table: TableName = application_table.parse()?;
        let index_name = IndexName::new(table, IndexDescriptor::new(application_index.clone())?)?;
        let index_fields = application_index_fields
            .iter()
            .map(|field| field.parse())
            .collect::<anyhow::Result<Vec<_>>>()?;
        IndexModel::new(&mut tx)
            .add_application_index(
                TableNamespace::root_component(),
                IndexMetadata::new_enabled(index_name, IndexedFields::try_from(index_fields)?),
            )
            .await?;
    }
    let fixture = match &expectation.setup {
        NormalRouteTestSetup::ApplicationProbe { user_table, .. } => {
            let user_table: TableName = user_table.parse()?;
            let user = SystemMetadataModel::new(&mut tx, TableNamespace::root_component())
                .insert_metadata(
                    &user_table,
                    obj!("userId" => "normal-route-user", "role" => "user")?,
                )
                .await?;
            NormalRouteFixtureState::ApplicationProbe {
                user_id: user.developer_id,
            }
        },
        NormalRouteTestSetup::ApplicationProbePatch { .. } => {
            NormalRouteFixtureState::ApplicationProbePatch
        },
        NormalRouteTestSetup::ApiKeyValidation {
            application_table,
            first_api_key,
            second_api_key,
            ..
        } => {
            let table: TableName = application_table.parse()?;
            let first_document_id = UserFacingModel::new(&mut tx, TableNamespace::root_component())
                .insert(
                    table.clone(),
                    obj!(
                        "keyHash" => Sha256::hash(first_api_key.as_bytes()).as_hex(),
                        "keyPrefix" => "normal-route",
                        "name" => "normal route validation",
                        "isActive" => true,
                    )?,
                )
                .await?;
            let second_document_id =
                UserFacingModel::new(&mut tx, TableNamespace::root_component())
                    .insert(
                        table,
                        obj!(
                            "keyHash" => Sha256::hash(second_api_key.as_bytes()).as_hex(),
                            "keyPrefix" => "normal-route",
                            "name" => "normal route validation",
                            "isActive" => true,
                        )?,
                    )
                    .await?;
            NormalRouteFixtureState::ApiKeyValidation {
                first_document_id,
                second_document_id,
                first_api_key: first_api_key.clone(),
                second_api_key: second_api_key.clone(),
            }
        },
        NormalRouteTestSetup::CollectDelete {
            expected_counts,
            result_fields,
            synthetic_rows,
        } => {
            let result_fields = result_fields
                .iter()
                .map(|(field, table)| Ok((field.clone(), table.parse::<TableName>()?)))
                .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
            for table in result_fields.values() {
                TableModel::new(&mut tx)
                    .insert_table_metadata(TableNamespace::root_component(), table)
                    .await?;
            }
            NormalRouteFixtureState::CollectDelete {
                expected_counts: expected_counts.clone(),
                result_fields,
                synthetic_rows: synthetic_rows.clone(),
            }
        },
        NormalRouteTestSetup::EmptyIndexedQueryV1 {
            application_table,
            expected_result,
            invocation_arguments,
            ..
        } => {
            let application_table: TableName = application_table.parse()?;
            let mut table_counts = serde_json::Map::new();
            table_counts.insert(application_table.to_string(), json!(0));
            NormalRouteFixtureState::EmptyIndexedQueryV1(NormalRouteEmptyIndexedQueryState {
                normalized_committed_state: json!({ "tableCounts": table_counts }),
                application_table,
                expected_result: expected_result.clone(),
                invocation_arguments: invocation_arguments.clone(),
            })
        },
        NormalRouteTestSetup::FileStorageGetUrlV1 { .. } => {
            NormalRouteFixtureState::FileStorageGetUrlV1(
                install_normal_route_file_storage_fixture(
                    &mut tx,
                    storage,
                    validated_file_storage_setup
                        .as_ref()
                        .context("file-storage setup was not validated")?,
                )
                .await?,
            )
        },
        NormalRouteTestSetup::IndexedPaginationQueryV1 {
            invocations,
            seeded_rows,
            warmup_invocation_count,
            ..
        } => {
            anyhow::ensure!(
                *warmup_invocation_count == 0 && invocations.len() == 2,
                "indexed pagination fixture requires two measured invocations and no warmup"
            );
            let mut fixture_ids = BTreeMap::new();
            let mut rows = Vec::with_capacity(seeded_rows.len());
            for row in seeded_rows {
                let resolved_value = resolve_normal_route_fixture_references(
                    &row.value,
                    &fixture_ids,
                    &mut BTreeSet::new(),
                )?;
                let expected_value = value::ConvexObject::try_from(resolved_value)?;
                let table: TableName = row.table.parse()?;
                let document_id = UserFacingModel::new(&mut tx, TableNamespace::root_component())
                    .insert(table, expected_value.clone())
                    .await?;
                anyhow::ensure!(
                    fixture_ids
                        .insert(row.alias.clone(), document_id.clone())
                        .is_none(),
                    "indexed pagination fixture repeated a seeded alias"
                );
                rows.push(NormalRouteObjectFixtureRowState {
                    alias: row.alias.clone(),
                    document_id,
                    expected_value,
                });
            }
            let invocations = invocations
                .iter()
                .map(|invocation| {
                    Ok(NormalRouteIndexedPaginationInvocationState {
                        arguments: resolve_normal_route_fixture_references(
                            &invocation.arguments,
                            &fixture_ids,
                            &mut BTreeSet::new(),
                        )?,
                        expected_result: resolve_normal_route_fixture_references(
                            &invocation.expected_result,
                            &fixture_ids,
                            &mut BTreeSet::new(),
                        )?,
                        normalized_expected_result:
                            normalize_normal_route_pagination_cursor_marker(
                                &invocation.expected_result,
                            )?,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            NormalRouteFixtureState::IndexedPaginationQueryV1(NormalRouteIndexedPaginationState {
                invocations,
                normalized_committed_state: normalized_normal_route_unchanged_seeded_state(
                    seeded_rows,
                )?,
                rows,
                warmup_invocation_count: *warmup_invocation_count,
            })
        },
        NormalRouteTestSetup::ObjectPatchDeleteV1 {
            deleted_fields,
            expected_results,
            invocation_arguments,
            restore_fields_before_measured_invocation,
            seeded_rows,
            target_alias,
        } => {
            let mut fixture_ids = BTreeMap::new();
            let mut rows = Vec::with_capacity(seeded_rows.len());
            for row in seeded_rows {
                let mut referenced_aliases = BTreeSet::new();
                let resolved_value = resolve_normal_route_fixture_references(
                    &row.value,
                    &fixture_ids,
                    &mut referenced_aliases,
                )?;
                let expected_value = value::ConvexObject::try_from(resolved_value)?;
                let table: TableName = row.table.parse()?;
                let document_id = UserFacingModel::new(&mut tx, TableNamespace::root_component())
                    .insert(table.clone(), expected_value.clone())
                    .await?;
                anyhow::ensure!(
                    fixture_ids
                        .insert(row.alias.clone(), document_id.clone())
                        .is_none(),
                    "object patch fixture repeated a seeded alias"
                );
                rows.push(NormalRouteObjectFixtureRowState {
                    alias: row.alias.clone(),
                    document_id,
                    expected_value,
                });
            }
            let target_document_id = fixture_ids
                .get(target_alias)
                .context("object patch fixture target alias was not seeded")?
                .clone();
            let deleted_fields = deleted_fields.iter().cloned().collect::<BTreeSet<_>>();
            for row in &mut rows {
                if row.document_id == target_document_id {
                    row.expected_value = row
                        .expected_value
                        .clone()
                        .filter_fields(|field| !deleted_fields.contains(&**field));
                }
            }
            let mut argument_references = BTreeSet::new();
            let invocation_arguments = resolve_normal_route_fixture_references(
                invocation_arguments,
                &fixture_ids,
                &mut argument_references,
            )?;
            anyhow::ensure!(
                argument_references.contains(target_alias),
                "object patch fixture invocation arguments do not reference the target"
            );
            let normalized_expected_results = expected_results.clone();
            let expected_results = expected_results
                .iter()
                .map(|result| {
                    resolve_normal_route_fixture_references(
                        result,
                        &fixture_ids,
                        &mut BTreeSet::new(),
                    )
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let restore_fields_before_measured_invocation =
                restore_fields_before_measured_invocation
                    .as_ref()
                    .map(|fields| {
                        let resolved = resolve_normal_route_fixture_references(
                            fields,
                            &fixture_ids,
                            &mut BTreeSet::new(),
                        )?;
                        value::ConvexObject::try_from(resolved)
                    })
                    .transpose()?;
            NormalRouteFixtureState::ObjectPatchDeleteV1(NormalRouteObjectPatchDeleteState {
                deleted_fields: deleted_fields.clone(),
                expected_results,
                invocation_arguments,
                normalized_committed_state: normalized_normal_route_object_patch_delete_state(
                    seeded_rows,
                    target_alias,
                    &deleted_fields,
                )?,
                normalized_expected_results,
                restore_fields_before_measured_invocation,
                rows,
                target_document_id,
            })
        },
        NormalRouteTestSetup::SeededDocumentsMutationV1 {
            application_table,
            documents,
            expected_result,
            expected_read_accounting,
            invocation_arguments,
            ..
        } => {
            let table: TableName = application_table.parse()?;
            let mut document_ids = Vec::with_capacity(documents.len());
            for document in documents {
                let resolved_document =
                    resolve_normal_route_seeded_document_references(document, &document_ids)?;
                let document = value::ConvexObject::try_from(resolved_document)?;
                let document_id = UserFacingModel::new(&mut tx, TableNamespace::root_component())
                    .insert(table.clone(), document)
                    .await?;
                document_ids.push(document_id);
            }
            let invocation_arguments = resolve_normal_route_seeded_document_references(
                invocation_arguments,
                &document_ids,
            )?;
            let expected_result =
                resolve_normal_route_seeded_document_references(expected_result, &document_ids)?;
            NormalRouteFixtureState::SeededDocumentsMutationV1(
                NormalRouteSeededDocumentsMutationState {
                    document_ids,
                    expected_result,
                    expected_read_accounting: *expected_read_accounting,
                    invocation_arguments,
                },
            )
        },
        NormalRouteTestSetup::SeededDocumentsQueryV1 {
            application_table,
            documents,
            expected_result,
            invocation_arguments,
            ..
        } => {
            let table: TableName = application_table.parse()?;
            let mut document_ids = Vec::with_capacity(documents.len());
            for document in documents {
                let resolved_document =
                    resolve_normal_route_seeded_document_references(document, &document_ids)?;
                let document = value::ConvexObject::try_from(resolved_document)?;
                let document_id = UserFacingModel::new(&mut tx, TableNamespace::root_component())
                    .insert(table.clone(), document)
                    .await?;
                document_ids.push(document_id);
            }
            let invocation_arguments = resolve_normal_route_seeded_document_references(
                invocation_arguments,
                &document_ids,
            )?;
            let expected_result =
                resolve_normal_route_seeded_document_references(expected_result, &document_ids)?;
            NormalRouteFixtureState::SeededDocumentsQueryV1(NormalRouteSeededDocumentsQueryState {
                expected_result,
                invocation_arguments,
            })
        },
        NormalRouteTestSetup::PairedLaneV1 {
            invocations,
            missing_ids,
            opaque_cursors,
            seeded_rows,
        } => {
            let mut fixture_ids = BTreeMap::new();
            for row in seeded_rows {
                let resolved_value = resolve_normal_route_fixture_references(
                    &row.value,
                    &fixture_ids,
                    &mut BTreeSet::new(),
                )?;
                let value = value::ConvexObject::try_from(resolved_value)?;
                let table: TableName = row.table.parse()?;
                let document_id = UserFacingModel::new(&mut tx, TableNamespace::root_component())
                    .insert(table, value)
                    .await?;
                anyhow::ensure!(
                    fixture_ids.insert(row.alias.clone(), document_id).is_none(),
                    "paired-lane fixture repeated a seeded alias"
                );
            }
            let invocations = invocations
                .iter()
                .map(|arguments| {
                    resolve_normal_route_fixture_references(
                        arguments,
                        &fixture_ids,
                        &mut BTreeSet::new(),
                    )
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let (invocations, missing_id_bindings) =
                materialize_normal_route_paired_lane_missing_ids(
                    &mut tx,
                    &invocations,
                    missing_ids.as_deref(),
                )
                .await?;
            let opaque_cursors =
                normalize_normal_route_paired_lane_opaque_cursors(&invocations, &opaque_cursors)?;
            NormalRouteFixtureState::PairedLaneV1(NormalRoutePairedLaneState {
                fixture_ids,
                invocations,
                missing_id_bindings,
                opaque_cursors,
            })
        },
        NormalRouteTestSetup::StatefulQueryPatchV1 {
            invocations,
            post_state_field_policies,
            seeded_rows,
            warmup_invocation_count,
        } => {
            anyhow::ensure!(
                *warmup_invocation_count == 0,
                "stateful query patch fixture requires zero warmup invocations"
            );
            anyhow::ensure!(
                invocations.len() == 2,
                "stateful query patch fixture requires exactly two invocations"
            );
            anyhow::ensure!(
                !seeded_rows.is_empty() && post_state_field_policies.len() == seeded_rows.len(),
                "stateful query patch fixture policies must cover every seeded row"
            );
            let mut fixture_ids = BTreeMap::new();
            let mut rows = Vec::with_capacity(seeded_rows.len());
            for row in seeded_rows {
                let policies = post_state_field_policies.get(&row.alias).with_context(|| {
                    format!(
                        "stateful query patch fixture omitted policies for {}",
                        row.alias
                    )
                })?;
                let resolved_value = resolve_normal_route_fixture_references(
                    &row.value,
                    &fixture_ids,
                    &mut BTreeSet::new(),
                )?;
                let initial_value = value::ConvexObject::try_from(resolved_value)?;
                anyhow::ensure!(
                    policies.len() == initial_value.len()
                        && policies
                            .keys()
                            .all(|field| initial_value.get(field.as_str()).is_some()),
                    "stateful query patch fixture policies must cover every seeded field"
                );
                anyhow::ensure!(
                    policies.iter().all(|(field, policy)| {
                        !matches!(policy, NormalRoutePostStateFieldPolicy::IncreasedNumber)
                            || initial_value
                                .get(field.as_str())
                                .and_then(normal_route_numeric_value)
                                .is_some()
                    }),
                    "stateful query patch increased-number policy requires a numeric seed"
                );
                let table: TableName = row.table.parse()?;
                let document_id = UserFacingModel::new(&mut tx, TableNamespace::root_component())
                    .insert(table, initial_value.clone())
                    .await?;
                anyhow::ensure!(
                    fixture_ids
                        .insert(row.alias.clone(), document_id.clone())
                        .is_none(),
                    "stateful query patch fixture repeated a seeded alias"
                );
                rows.push(NormalRouteStatefulQueryPatchRowState {
                    alias: row.alias.clone(),
                    document_id,
                    initial_value,
                    post_state_field_policies: policies.clone(),
                });
            }
            anyhow::ensure!(
                post_state_field_policies
                    .keys()
                    .all(|alias| fixture_ids.contains_key(alias)),
                "stateful query patch fixture policies name an unseeded alias"
            );
            let mut expected_patch = false;
            let invocations = invocations
                .iter()
                .enumerate()
                .map(|(index, invocation)| {
                    anyhow::ensure!(
                        invocation.arguments.is_object(),
                        "stateful query patch invocation {index} arguments are not an object"
                    );
                    anyhow::ensure!(
                        !invocation.expected_capability_stages.is_empty()
                            && invocation.expected_capability_stages.iter().all(|stage| {
                                matches!(stage.as_str(), "start:db-query" | "start:db-patch")
                            })
                            && invocation
                                .expected_capability_stages
                                .iter()
                                .any(|stage| stage == "start:db-query"),
                        "stateful query patch invocation {index} has invalid capability stages"
                    );
                    expected_patch |= invocation
                        .expected_capability_stages
                        .iter()
                        .any(|stage| stage == "start:db-patch");
                    Ok(NormalRouteStatefulQueryPatchInvocationState {
                        arguments: resolve_normal_route_fixture_references(
                            &invocation.arguments,
                            &fixture_ids,
                            &mut BTreeSet::new(),
                        )?,
                        expected_capability_stages: invocation.expected_capability_stages.clone(),
                        expected_result: resolve_normal_route_fixture_references(
                            &invocation.expected_result,
                            &fixture_ids,
                            &mut BTreeSet::new(),
                        )?,
                        normalized_expected_result: invocation.expected_result.clone(),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            anyhow::ensure!(
                expected_patch,
                "stateful query patch fixture must expect at least one patch"
            );
            NormalRouteFixtureState::StatefulQueryPatchV1(NormalRouteStatefulQueryPatchState {
                invocations,
                normalized_committed_state: normalized_normal_route_stateful_query_patch_state(
                    seeded_rows,
                    post_state_field_policies,
                )?,
                rows,
                warmup_invocation_count: *warmup_invocation_count,
            })
        },
    };
    database
        .commit_with_write_source(tx, "static_hermes_normal_route_test_setup")
        .await?;
    Ok(fixture)
}

fn normal_route_sequence_route_key(route: &NormalRouteTestRoute) -> String {
    format!("{}:{}", route.runtime_module_path, route.export_name)
}

fn normal_route_sequence_observation_protocol(kind: &str) -> anyhow::Result<(&'static str, u16)> {
    match kind {
        "convex-wasm-normal-route-sequence-v1" => {
            Ok(("convex-wasm-normal-route-sequence-observation-v5", 5))
        },
        "convex-wasm-normal-route-sequence-v2" => {
            Ok(("convex-wasm-normal-route-sequence-observation-v6", 6))
        },
        _ => anyhow::bail!("normal route sequence kind is unsupported"),
    }
}

async fn install_normal_route_sequence_fixture(
    database: &Database<ProdRuntime>,
    expectation: &NormalRouteSequenceTestExpectation,
    modules: &BTreeMap<CanonicalizedModulePath, Arc<FullModuleSource>>,
    source_package: SourcePackage,
) -> anyhow::Result<NormalRouteSequenceFixtureState> {
    let is_legacy_aba = expectation.sequence.kind == "convex-wasm-normal-route-sequence-v1";
    anyhow::ensure!(
        (is_legacy_aba || expectation.sequence.kind == "convex-wasm-normal-route-sequence-v2")
            && !expectation.sequence.steps.is_empty()
            && (!is_legacy_aba || expectation.sequence.steps.len() == 3),
        "normal route sequence must be a nonempty supported sequence"
    );
    let route_keys = expectation
        .sequence
        .steps
        .iter()
        .map(|step| normal_route_sequence_route_key(&step.route))
        .collect::<Vec<_>>();
    if is_legacy_aba {
        let uses_one_package = expectation
            .sequence
            .steps
            .iter()
            .all(|step| step.package_key == expectation.sequence.steps[0].package_key);
        anyhow::ensure!(
            route_keys[0] == route_keys[2]
                && route_keys[0] != route_keys[1]
                && expectation.sequence.steps[0].package_key
                    == expectation.sequence.steps[2].package_key
                && (uses_one_package
                    || expectation.sequence.steps[0].package_key
                        != expectation.sequence.steps[1].package_key)
                && expectation.sequence.steps[0].entry_id == expectation.sequence.steps[2].entry_id
                && expectation.sequence.steps[0].entry_id != expectation.sequence.steps[1].entry_id
                && expectation.sequence.steps[0].entry_selector_id
                    == expectation.sequence.steps[2].entry_selector_id
                && expectation.sequence.steps[0].entry_selector_id
                    != expectation.sequence.steps[1].entry_selector_id,
            "normal route sequence does not identify distinct A and B entries in one or two \
             packages"
        );
    }
    if let Some(NormalRouteSequenceInvocationIdentity::SeededUser {
        role,
        user_id,
        user_table,
    }) = &expectation.sequence.invocation_identity
    {
        anyhow::ensure!(
            !role.is_empty()
                && !user_id.is_empty()
                && !user_table.is_empty()
                && expectation.sequence.steps.iter().all(|step| {
                    (step.route.udf_kind == "query" && step.route.visibility == "public")
                        || (step.route.udf_kind == "mutation"
                            && step.route.visibility == "internal")
                }),
            "seeded-user normal route sequence requires public queries or internal mutations"
        );
    }

    let mut analyzed_functions =
        BTreeMap::<CanonicalizedModulePath, BTreeMap<String, AnalyzedFunction>>::new();
    for step in &expectation.sequence.steps {
        let _ = normal_route_sha256(&step.package_key, "normal route sequence package identity")?;
        let _ = normal_route_sha256(&step.entry_id, "normal route sequence entry identity")?;
        let _ = normal_route_sha256(&step.route_id, "normal route sequence route identity")?;
        anyhow::ensure!(
            step.entry_selector_id.len() == 16
                && step
                    .entry_selector_id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "normal route sequence selector identity is invalid"
        );
        let udf_type: UdfType = step.route.udf_kind.parse()?;
        anyhow::ensure!(
            matches!(udf_type, UdfType::Query | UdfType::Mutation),
            "normal route sequence selected an unsupported UDF kind"
        );
        let visibility = match step.route.visibility.as_str() {
            "internal" => Visibility::Internal,
            "public" => Visibility::Public,
            _ => anyhow::bail!("normal route sequence selected an unsupported visibility"),
        };
        let module_path: CanonicalizedModulePath = step.route.runtime_module_path.parse()?;
        let functions = analyzed_functions.entry(module_path).or_default();
        functions
            .entry(step.route.export_name.clone())
            .or_insert(AnalyzedFunction::new(
                step.route.export_name.parse()?,
                None,
                udf_type,
                Some(visibility),
                ArgsValidator::Unvalidated,
                ReturnsValidator::Unvalidated,
            )?);
    }
    anyhow::ensure!(
        !analyzed_functions.is_empty()
            && analyzed_functions.len()
                == expectation
                    .sequence
                    .steps
                    .iter()
                    .map(|step| step.route.runtime_module_path.as_str())
                    .collect::<BTreeSet<_>>()
                    .len(),
        "normal route sequence installed modules differ from its routed modules"
    );
    let scheduled_module = match &expectation.sequence.expected_committed_state {
        NormalRouteSequenceExpectedCommittedState::ApplicationProbePatch {
            scheduled_function_address,
            ..
        } => {
            let source = expectation.source.scheduled_module.as_ref().context(
                "normal route sequence application-probe state omitted its scheduled source module",
            )?;
            let scheduled_module = normal_route_scheduled_module(
                &expectation.source.bundle,
                source,
                scheduled_function_address,
            )?;
            anyhow::ensure!(
                !modules.contains_key(&scheduled_module.module_path),
                "normal route sequence scheduled Node module overlaps a routed isolate module"
            );
            Some(scheduled_module)
        },
        NormalRouteSequenceExpectedCommittedState::Tagged(
            NormalRouteSequenceTaggedExpectedCommittedState::EmptyTables { .. }
            | NormalRouteSequenceTaggedExpectedCommittedState::EmptyDatabase { .. },
        ) => {
            anyhow::ensure!(
                expectation.source.scheduled_module.is_none(),
                "normal route sequence empty-database state included a scheduled source module"
            );
            None
        },
        NormalRouteSequenceExpectedCommittedState::Tagged(
            NormalRouteSequenceTaggedExpectedCommittedState::TableDocuments { .. },
        ) => {
            anyhow::ensure!(
                expectation.source.scheduled_module.is_none(),
                "normal route sequence table-documents state included a scheduled source module"
            );
            None
        },
    };

    initialize_application_system_tables(database).await?;
    let mut tx = database.begin_system().await?;
    UdfConfigModel::new(&mut tx, TableNamespace::root_component())
        .set(UdfConfig {
            server_version: semver::Version::new(1, 99, 0),
            import_phase_rng_seed: [0; 32],
            import_phase_unix_timestamp: UnixTimestamp::from_nanos(0),
        })
        .await?;
    let source_package_id = SourcePackageModel::new(&mut tx, TableNamespace::root_component())
        .put(source_package)
        .await?;
    for (path, module) in modules {
        let analyze_result = if path.is_deps() {
            None
        } else {
            Some(AnalyzedModule {
                functions: analyzed_functions
                    .remove(path)
                    .unwrap_or_default()
                    .into_values()
                    .collect::<Vec<_>>()
                    .into(),
                ..Default::default()
            })
        };
        ModuleModel::new(&mut tx)
            .put(
                None,
                CanonicalizedComponentModulePath {
                    component: ComponentId::Root,
                    module_path: path.clone(),
                },
                module.source.clone(),
                source_package_id,
                module.source_map.clone(),
                analyze_result,
                ModuleEnvironment::Isolate,
                None,
            )
            .await?;
    }
    anyhow::ensure!(
        analyzed_functions.is_empty(),
        "normal route sequence source bundle omitted an analyzed module"
    );
    if let Some(scheduled_module) = scheduled_module {
        ModuleModel::new(&mut tx)
            .put(
                None,
                CanonicalizedComponentModulePath {
                    component: ComponentId::Root,
                    module_path: scheduled_module.module_path,
                },
                scheduled_module.source.source.clone(),
                source_package_id,
                scheduled_module.source.source_map.clone(),
                Some(AnalyzedModule {
                    functions: vec![AnalyzedFunction::new(
                        scheduled_module.export_name.parse()?,
                        None,
                        scheduled_module.udf_type,
                        Some(scheduled_module.visibility),
                        ArgsValidator::Unvalidated,
                        ReturnsValidator::Unvalidated,
                    )?]
                    .into(),
                    ..Default::default()
                }),
                ModuleEnvironment::Node,
                None,
            )
            .await?;
    }
    let mut installed_indexes = BTreeSet::new();
    for application_index in &expectation.sequence.application_indexes {
        let table: TableName = application_index.table.parse()?;
        let index_name = IndexName::new(
            table.clone(),
            IndexDescriptor::new(application_index.index.clone())?,
        )?;
        anyhow::ensure!(
            installed_indexes.insert(index_name.clone()),
            "normal route sequence repeats an application index"
        );
        let fields = application_index
            .fields
            .iter()
            .map(|field| field.parse())
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            !fields.is_empty(),
            "normal route sequence application index has no fields"
        );
        IndexModel::new(&mut tx)
            .add_application_index(
                TableNamespace::root_component(),
                IndexMetadata::new_enabled(index_name, IndexedFields::try_from(fields)?),
            )
            .await?;
    }
    anyhow::ensure!(
        !installed_indexes.is_empty(),
        "normal route sequence must install application indexes"
    );
    let user_document_id = match &expectation.sequence.invocation_identity {
        Some(NormalRouteSequenceInvocationIdentity::SeededUser {
            role,
            user_id,
            user_table,
        }) => {
            let user_table: TableName = user_table.parse()?;
            let user = SystemMetadataModel::new(&mut tx, TableNamespace::root_component())
                .insert_metadata(
                    &user_table,
                    obj!("userId" => user_id.clone(), "role" => role.clone())?,
                )
                .await?;
            Some(user.developer_id)
        },
        None => None,
    };
    database
        .commit_with_write_source(tx, "static_hermes_normal_route_sequence_test_setup")
        .await?;
    match &expectation.sequence.expected_committed_state {
        NormalRouteSequenceExpectedCommittedState::ApplicationProbePatch { .. } => {
            Ok(NormalRouteSequenceFixtureState::ApplicationProbePatch { user_document_id })
        },
        NormalRouteSequenceExpectedCommittedState::Tagged(
            NormalRouteSequenceTaggedExpectedCommittedState::EmptyTables {
                normalized,
                result_fields,
                synthetic_rows,
            },
        ) => {
            anyhow::ensure!(
                result_fields.len() > 0
                    && result_fields.values().collect::<BTreeSet<_>>().len() == result_fields.len()
                    && synthetic_rows.len() == 2
                    && synthetic_rows[0] != synthetic_rows[1]
                    && synthetic_rows.iter().all(|row| {
                        row.as_object().is_some_and(|fields| {
                            !fields.contains_key("_id") && !fields.contains_key("_creationTime")
                        })
                    }),
                "normal route sequence empty-table fixture is invalid"
            );
            let result_fields = result_fields
                .iter()
                .map(|(field, table)| Ok((field.clone(), table.parse()?)))
                .collect::<anyhow::Result<BTreeMap<String, TableName>>>()?;
            let expected_normalized = json!({
                "seededIdsAbsent": true,
                "tableCounts": result_fields
                    .values()
                    .map(|table| (table.to_string(), json!(0)))
                    .collect::<serde_json::Map<_, _>>(),
            });
            anyhow::ensure!(
                normalized == &expected_normalized,
                "normal route sequence empty-table normalized state is invalid"
            );
            let seeded_document_ids =
                seed_normal_route_collect_delete_rows(database, &result_fields, synthetic_rows)
                    .await?;
            Ok(NormalRouteSequenceFixtureState::EmptyTables {
                result_fields,
                seeded_document_ids,
                user_document_id,
            })
        },
        NormalRouteSequenceExpectedCommittedState::Tagged(
            NormalRouteSequenceTaggedExpectedCommittedState::EmptyDatabase { normalized, tables },
        ) => {
            anyhow::ensure!(
                !tables.is_empty()
                    && tables.iter().all(|table| !table.is_empty())
                    && tables.iter().collect::<BTreeSet<_>>().len() == tables.len(),
                "normal route sequence empty-database tables are invalid"
            );
            let tables = tables
                .iter()
                .map(|table| table.parse::<TableName>())
                .collect::<anyhow::Result<Vec<_>>>()?;
            let expected_normalized = json!({
                "tableCounts": tables
                    .iter()
                    .map(|table| (table.to_string(), json!(0)))
                    .collect::<serde_json::Map<_, _>>(),
            });
            anyhow::ensure!(
                normalized == &expected_normalized,
                "normal route sequence empty-database normalized state is invalid"
            );
            for table in &tables {
                anyhow::ensure!(
                    table_count(database, table).await? == 0,
                    "normal route sequence empty-database table is not empty before execution"
                );
            }
            Ok(NormalRouteSequenceFixtureState::EmptyDatabase {
                tables,
                user_document_id,
            })
        },
        NormalRouteSequenceExpectedCommittedState::Tagged(
            NormalRouteSequenceTaggedExpectedCommittedState::TableDocuments {
                documents,
                normalized,
                table,
            },
        ) => {
            let table = table.parse::<TableName>()?;
            let expected_normalized =
                normal_route_table_documents_expected_normalized(&table, documents)?;
            anyhow::ensure!(
                normalized == &expected_normalized,
                "normal route sequence table-documents normalized state is invalid"
            );
            anyhow::ensure!(
                table_count(database, &table).await? == 0,
                "normal route sequence table-documents table is not empty before execution"
            );
            Ok(NormalRouteSequenceFixtureState::TableDocuments {
                table,
                user_document_id,
            })
        },
    }
}

async fn invalidate_normal_route_query_cache(
    database: &Database<ProdRuntime>,
    user_id: value::DeveloperDocumentId,
) -> anyhow::Result<()> {
    let mut tx = database.begin_system().await?;
    UserFacingModel::new(&mut tx, TableNamespace::root_component())
        .replace(
            user_id,
            obj!(
                "userId" => "normal-route-user",
                "role" => "user",
                "normalRouteIteration" => 2_i64,
            )?,
        )
        .await?;
    database
        .commit_with_write_source(tx, "static_hermes_normal_route_test_invalidation")
        .await?;
    Ok(())
}

fn normalize_normal_route_result(
    result: &JsonValue,
    fixture: &NormalRouteFixtureState,
    invocation_index: usize,
    captured_pagination_cursor: &mut Option<String>,
) -> anyhow::Result<JsonValue> {
    match fixture {
        NormalRouteFixtureState::ApplicationProbe { .. } => {
            let fields = result
                .as_object()
                .context("normal route result is not an object")?;
            anyhow::ensure!(
                fields.get("configured") == Some(&json!(false))
                    && fields
                        .get("serverObservedAt")
                        .is_some_and(JsonValue::is_number)
                    && fields.len() == 2,
                "application probe result structure changed: {result:?}"
            );
            Ok(json!({
                "configured": false,
                "serverObservedAt": { "kind": "invocation-time" },
            }))
        },
        NormalRouteFixtureState::ApplicationProbePatch => {
            anyhow::ensure!(
                result.is_null(),
                "application probe patch result changed: {result:?}"
            );
            Ok(JsonValue::Null)
        },
        NormalRouteFixtureState::ApiKeyValidation {
            first_document_id,
            second_document_id,
            first_api_key,
            second_api_key,
        } => {
            let fields = result
                .as_object()
                .context("normal route result is not an object")?;
            let (api_key, expected_id) = if invocation_index % 2 == 0 {
                (first_api_key, first_document_id.encode())
            } else {
                (second_api_key, second_document_id.encode())
            };
            let expected_hash = Sha256::hash(api_key.as_bytes()).as_hex();
            anyhow::ensure!(
                fields.get("_id").and_then(JsonValue::as_str) == Some(expected_id.as_str())
                    && fields
                        .get("_creationTime")
                        .is_some_and(JsonValue::is_number)
                    && fields.get("isActive") == Some(&json!(true))
                    && fields.get("keyHash").and_then(JsonValue::as_str)
                        == Some(expected_hash.as_str())
                    && fields.get("keyPrefix") == Some(&json!("normal-route"))
                    && fields.get("name") == Some(&json!("normal route validation"))
                    && fields.len() == 6,
                "API-key validation result structure changed"
            );
            Ok(json!({
                "_creationTime": { "kind": "creation-time" },
                "_id": { "kind": "document-id" },
                "isActive": true,
                "keyHash": expected_hash,
                "keyPrefix": "normal-route",
                "name": "normal route validation",
            }))
        },
        NormalRouteFixtureState::CollectDelete {
            expected_counts,
            result_fields,
            ..
        } => {
            let expected_count = *expected_counts
                .get(invocation_index)
                .context("collect-delete invocation has no expected count")?;
            let fields = result
                .as_object()
                .context("collect-delete result is not an object")?;
            // JavaScript array lengths are Convex Float64 values, so packed JSON keeps a
            // float-backed number even when the count is integral.
            let actual_counts = result_fields
                .keys()
                .map(|field| {
                    (
                        field.as_str(),
                        fields.get(field).and_then(JsonValue::as_f64),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            anyhow::ensure!(
                fields.len() == result_fields.len()
                    && actual_counts
                        .values()
                        .all(|count| *count == Some(expected_count as f64)),
                "collect-delete result counts changed: expected_count={expected_count}, \
                 actual_field_count={}, actual_counts={actual_counts:?}",
                fields.len()
            );
            Ok(JsonValue::Object(
                result_fields
                    .keys()
                    .map(|field| (field.clone(), json!(expected_count)))
                    .collect(),
            ))
        },
        NormalRouteFixtureState::EmptyIndexedQueryV1(fixture) => {
            anyhow::ensure!(
                result == &fixture.expected_result,
                "empty indexed query result differs from its exact expectation"
            );
            Ok(fixture.expected_result.clone())
        },
        NormalRouteFixtureState::FileStorageGetUrlV1(fixture) => {
            anyhow::ensure!(
                result == &fixture.expected_result,
                "file-storage result differs from its exact expectation"
            );
            Ok(fixture.expected_result.clone())
        },
        NormalRouteFixtureState::IndexedPaginationQueryV1(fixture) => {
            let invocation = fixture
                .invocations
                .get(invocation_index)
                .context("indexed pagination invocation is unavailable")?;
            verify_normal_route_pagination_result(
                result,
                &invocation.expected_result,
                captured_pagination_cursor,
            )?;
            Ok(invocation.normalized_expected_result.clone())
        },
        NormalRouteFixtureState::ObjectPatchDeleteV1(fixture) => {
            let expected_result = fixture
                .expected_results
                .get(invocation_index)
                .context("object patch invocation has no expected result")?;
            anyhow::ensure!(
                result == expected_result,
                "object patch result differs from its exact expectation"
            );
            Ok(fixture
                .normalized_expected_results
                .get(invocation_index)
                .context("object patch invocation has no normalized expected result")?
                .clone())
        },
        NormalRouteFixtureState::SeededDocumentsMutationV1(fixture) => {
            anyhow::ensure!(
                result == &fixture.expected_result,
                "seeded-documents mutation result differs from its exact expectation"
            );
            Ok(fixture.expected_result.clone())
        },
        NormalRouteFixtureState::SeededDocumentsQueryV1(fixture) => {
            anyhow::ensure!(
                result == &fixture.expected_result,
                "seeded-documents query result differs from its exact expectation"
            );
            Ok(fixture.expected_result.clone())
        },
        NormalRouteFixtureState::PairedLaneV1(_) => Ok(result.clone()),
        NormalRouteFixtureState::StatefulQueryPatchV1(fixture) => {
            let invocation = fixture
                .invocations
                .get(invocation_index)
                .context("stateful query patch invocation is unavailable")?;
            anyhow::ensure!(
                result == &invocation.expected_result,
                "stateful query patch result differs from its exact expectation"
            );
            Ok(invocation.normalized_expected_result.clone())
        },
    }
}

#[test]
fn normal_route_patch_result_normalization_accepts_only_null() -> anyhow::Result<()> {
    let fixture = NormalRouteFixtureState::ApplicationProbePatch;
    assert_eq!(
        normalize_normal_route_result(&JsonValue::Null, &fixture, 0, &mut None)?,
        JsonValue::Null
    );
    let error = normalize_normal_route_result(&json!({}), &fixture, 0, &mut None)
        .expect_err("application patch result normalization accepted a non-null result");
    assert!(error
        .to_string()
        .contains("application probe patch result changed"));
    Ok(())
}

#[test]
fn normal_route_pagination_cursor_handoff_preserves_the_raw_cursor() -> anyhow::Result<()> {
    let raw_cursor = "opaque/raw+cursor==";
    let expected_page = json!({
        "continueCursor": { "$paginationCursor": "capture" },
        "page": ["A", "B"],
    });
    let actual_page = json!({
        "continueCursor": raw_cursor,
        "page": ["A", "B"],
    });
    let mut captured_cursor = None;
    verify_normal_route_pagination_result(&actual_page, &expected_page, &mut captured_cursor)?;
    assert_eq!(captured_cursor.as_deref(), Some(raw_cursor));
    assert_eq!(
        resolve_normal_route_pagination_cursor_argument(
            &json!({ "cursor": { "$paginationCursor": "previous" } }),
            captured_cursor.as_deref(),
        )?,
        json!({ "cursor": raw_cursor })
    );
    assert_eq!(
        normalize_normal_route_pagination_cursor_marker(&expected_page)?,
        json!({
            "continueCursor": { "kind": "nonempty-pagination-cursor" },
            "page": ["A", "B"],
        })
    );
    verify_normal_route_pagination_result(
        &json!({ "continueCursor": null, "page": ["C"] }),
        &json!({ "continueCursor": null, "page": ["C"] }),
        &mut captured_cursor,
    )?;

    let mut missing_cursor = None;
    verify_normal_route_pagination_result(
        &json!({ "continueCursor": "", "page": ["A", "B"] }),
        &expected_page,
        &mut missing_cursor,
    )
    .expect_err("pagination result accepted an empty captured cursor");
    resolve_normal_route_pagination_cursor_argument(
        &json!({ "cursor": { "$paginationCursor": "previous" } }),
        None,
    )
    .expect_err("pagination invocation accepted a missing previous cursor");
    Ok(())
}

#[test]
fn normal_route_paired_lane_opaque_cursor_handoff_is_same_lane() -> anyhow::Result<()> {
    let invocations = vec![
        json!({ "cursor": null }),
        json!({ "cursor": { "$opaqueCursor": "next-page" } }),
    ];
    let opaque_cursors = normalize_normal_route_paired_lane_opaque_cursors(
        &invocations,
        &[NormalRoutePairedLaneOpaqueCursor {
            capture_invocation: 0,
            capture_result_path: vec!["continueCursor".to_owned()],
            id: "next-page".to_owned(),
        }],
    )?;
    let raw_cursor = "opaque/raw+cursor==";
    let mut captured_cursors = BTreeMap::new();
    let normalized_first_page = capture_normal_route_paired_lane_opaque_cursors(
        &json!({ "continueCursor": raw_cursor, "page": ["A", "B"] }),
        0,
        &opaque_cursors,
        &mut captured_cursors,
    )?;
    assert_eq!(
        normalized_first_page,
        json!({
            "continueCursor": { "kind": "opaque-cursor" },
            "page": ["A", "B"],
        })
    );
    assert_eq!(
        resolve_normal_route_paired_lane_opaque_cursors(&invocations[1], &captured_cursors,)?,
        json!({ "cursor": raw_cursor })
    );
    assert_eq!(
        serde_json::to_value(
            normal_route_paired_lane_query_journal_with_opaque_cursor_continuation(
                NormalRouteQueryJournalObservation::After {
                    query_fingerprint_sha256: "a".repeat(64),
                    after_key_sha256: "b".repeat(64),
                    after_key_continuation: None,
                },
                0,
                &opaque_cursors,
            ),
        )?,
        json!({
            "afterKeyContinuation": {
                "kind": "same-lane-opaque-cursor",
                "opaqueCursorId": "next-page",
            },
            "afterKeySha256": "b".repeat(64),
            "kind": "after",
            "queryFingerprintSha256": "a".repeat(64),
        })
    );

    let used_before_capture = normalize_normal_route_paired_lane_opaque_cursors(
        &[
            json!({ "cursor": { "$opaqueCursor": "next-page" } }),
            json!({}),
        ],
        &opaque_cursors,
    );
    assert!(used_before_capture.is_err());
    Ok(())
}

fn normal_route_paired_lane_test_write_intents(
    writes: Vec<NormalRoutePairedLaneRawWrite>,
) -> anyhow::Result<Vec<JsonValue>> {
    let mut document_id_markers = BTreeMap::new();
    normal_route_paired_lane_write_id_markers(&writes, &mut document_id_markers)?;
    let mut intents = writes
        .iter()
        .map(|write| {
            let intent = normal_route_paired_lane_write_intent_from_documents(
                write.table.clone(),
                write.old_document.as_ref(),
                write.new_document.as_ref(),
                &document_id_markers,
            )?;
            Ok(serde_json::to_value(intent)?)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    intents.sort_unstable_by(|left, right| {
        normal_route_sequence_json_canonical_bytes(left)
            .expect("test write intent must canonicalize")
            .cmp(
                &normal_route_sequence_json_canonical_bytes(right)
                    .expect("test write intent must canonicalize"),
            )
    });
    Ok(intents)
}

#[test]
fn normal_route_paired_lane_write_intents_hash_normalized_lane_local_fields() -> anyhow::Result<()>
{
    let first = normal_route_paired_lane_test_write_intents(vec![NormalRoutePairedLaneRawWrite {
        id: "lane-a-document".to_owned(),
        old_document: None,
        new_document: Some(json!({
            "_creationTime": 1,
            "_id": "lane-a-document",
            "owner": "lane-a-document",
            "secret": "not-emitted-as-raw-evidence",
        })),
        table: "accounts".to_owned(),
    }])?;
    let second =
        normal_route_paired_lane_test_write_intents(vec![NormalRoutePairedLaneRawWrite {
            id: "lane-b-document".to_owned(),
            old_document: None,
            new_document: Some(json!({
                "_creationTime": 9_999,
                "_id": "lane-b-document",
                "owner": "lane-b-document",
                "secret": "not-emitted-as-raw-evidence",
            })),
            table: "accounts".to_owned(),
        }])?;

    assert_eq!(first, second);
    let serialized = serde_json::to_string(&first)?;
    assert!(!serialized.contains("lane-a-document"));
    assert!(!serialized.contains("not-emitted-as-raw-evidence"));
    Ok(())
}

#[test]
fn normal_route_paired_lane_write_intents_preserve_same_table_id_relations() -> anyhow::Result<()> {
    let self_references = normal_route_paired_lane_test_write_intents(vec![
        NormalRoutePairedLaneRawWrite {
            id: "first".to_owned(),
            old_document: None,
            new_document: Some(json!({
                "_creationTime": 1,
                "_id": "first",
                "peer": "first",
            })),
            table: "accounts".to_owned(),
        },
        NormalRoutePairedLaneRawWrite {
            id: "second".to_owned(),
            old_document: None,
            new_document: Some(json!({
                "_creationTime": 2,
                "_id": "second",
                "peer": "second",
            })),
            table: "accounts".to_owned(),
        },
    ])?;
    let forged_cross_references = normal_route_paired_lane_test_write_intents(vec![
        NormalRoutePairedLaneRawWrite {
            id: "first".to_owned(),
            old_document: None,
            new_document: Some(json!({
                "_creationTime": 1,
                "_id": "first",
                "peer": "second",
            })),
            table: "accounts".to_owned(),
        },
        NormalRoutePairedLaneRawWrite {
            id: "second".to_owned(),
            old_document: None,
            new_document: Some(json!({
                "_creationTime": 2,
                "_id": "second",
                "peer": "first",
            })),
            table: "accounts".to_owned(),
        },
    ])?;

    assert_ne!(self_references, forged_cross_references);
    Ok(())
}

#[test]
fn normal_route_paired_lane_not_applicable_evidence_requires_a_clean_trace() -> anyhow::Result<()> {
    let clean_trace =
        normal_route_paired_lane_host_operation_trace(&udf::HostOperationTrace::from(vec![
            udf::LogicalHostOperation::DatabaseGet,
        ]))?;
    assert!(normal_route_paired_lane_not_applicable_evidence(
        vec![clean_trace.clone()],
        PAIRED_LANE_EXTERNAL_EFFECT_OPERATIONS,
        "external effects",
    )
    .is_ok());
    assert!(normal_route_paired_lane_not_applicable_evidence(
        vec![clean_trace],
        PAIRED_LANE_SCHEDULER_SYSTEM_STATE_OPERATIONS,
        "scheduler system state",
    )
    .is_ok());

    for forbidden_operation in [
        udf::LogicalHostOperation::AuditLog,
        udf::LogicalHostOperation::StorageDelete,
        udf::LogicalHostOperation::StorageGenerateUploadUrl,
        udf::LogicalHostOperation::WriteDeploymentAuditLog,
    ] {
        let trace =
            normal_route_paired_lane_host_operation_trace(&udf::HostOperationTrace::from(vec![
                forbidden_operation,
            ]))?;
        assert!(normal_route_paired_lane_not_applicable_evidence(
            vec![trace],
            PAIRED_LANE_EXTERNAL_EFFECT_OPERATIONS,
            "external effects",
        )
        .is_err());
    }
    for forbidden_operation in [
        udf::LogicalHostOperation::CancelJob,
        udf::LogicalHostOperation::Schedule,
    ] {
        let trace =
            normal_route_paired_lane_host_operation_trace(&udf::HostOperationTrace::from(vec![
                forbidden_operation,
            ]))?;
        assert!(normal_route_paired_lane_not_applicable_evidence(
            vec![trace],
            PAIRED_LANE_SCHEDULER_SYSTEM_STATE_OPERATIONS,
            "scheduler system state",
        )
        .is_err());
    }
    assert!(
        normal_route_paired_lane_host_operation_trace(&udf::HostOperationTrace::default()).is_err()
    );
    Ok(())
}

#[test]
fn normal_route_collect_delete_normalizes_float_counts() -> anyhow::Result<()> {
    let fixture = NormalRouteFixtureState::CollectDelete {
        expected_counts: vec![2, 0],
        result_fields: BTreeMap::from([
            ("first".to_owned(), "normalRouteFirst".parse()?),
            ("second".to_owned(), "normalRouteSecond".parse()?),
        ]),
        synthetic_rows: vec![
            json!({ "fixtureOrdinal": 1 }),
            json!({ "fixtureOrdinal": 2 }),
        ],
    };
    assert_eq!(
        normalize_normal_route_result(
            &json!({
                "first": 2.0,
                "second": 2.0,
            }),
            &fixture,
            0,
            &mut None,
        )?,
        json!({
            "first": 2,
            "second": 2,
        })
    );
    assert_eq!(
        normalize_normal_route_result(
            &json!({
                "first": 0.0,
                "second": 0.0,
            }),
            &fixture,
            1,
            &mut None,
        )?,
        json!({
            "first": 0,
            "second": 0,
        })
    );
    let error = normalize_normal_route_result(
        &json!({
            "first": 1.0,
            "second": 2.0,
        }),
        &fixture,
        0,
        &mut None,
    )
    .expect_err("collect-delete result normalization accepted a wrong count");
    assert!(error.to_string().contains(
        "expected_count=2, actual_field_count=2, actual_counts={\"first\": Some(1.0), \"second\": \
         Some(2.0)}"
    ));
    Ok(())
}

#[test]
fn normal_route_object_patch_state_removes_only_declared_target_fields() -> anyhow::Result<()> {
    let rows = vec![
        NormalRouteObjectFixtureRow {
            alias: "related".to_owned(),
            table: "fixtureRelatedRows".to_owned(),
            value: json!({ "marker": "related" }),
        },
        NormalRouteObjectFixtureRow {
            alias: "target".to_owned(),
            table: "fixtureTargetRows".to_owned(),
            value: json!({
                "relatedId": { "$fixtureId": "related" },
                "removed": "value",
                "retained": "value",
            }),
        },
    ];
    assert_eq!(
        normalized_normal_route_object_patch_delete_state(
            &rows,
            "target",
            &BTreeSet::from(["removed".to_owned()]),
        )?,
        json!({
            "rows": [
                {
                    "alias": "related",
                    "document": {
                        "_creationTime": { "kind": "creation-time" },
                        "_id": { "$fixtureId": "related" },
                        "marker": "related",
                    },
                    "table": "fixtureRelatedRows",
                },
                {
                    "alias": "target",
                    "document": {
                        "_creationTime": { "kind": "creation-time" },
                        "_id": { "$fixtureId": "target" },
                        "relatedId": { "$fixtureId": "related" },
                        "retained": "value",
                    },
                    "table": "fixtureTargetRows",
                },
            ],
            "targetAlias": "target",
        })
    );
    Ok(())
}

#[test]
fn normal_route_object_patch_delete_post_state_requires_absence_and_preserves_fields(
) -> anyhow::Result<()> {
    let target_document_id = DeveloperDocumentId::MIN;
    let deleted_fields = BTreeSet::from(["optionalField".to_owned()]);
    let initial_value = value::ConvexObject::try_from(json!({
        "optionalField": "present",
        "retainedField": "unchanged",
    }))?;
    let row = NormalRouteObjectFixtureRowState {
        alias: "target".to_owned(),
        document_id: target_document_id,
        expected_value: initial_value
            .clone()
            .filter_fields(|field| !deleted_fields.contains(&**field)),
    };
    let deleted_value = value::ConvexObject::try_from(json!({
        "_creationTime": 123,
        "_id": "fixture-id",
        "retainedField": "unchanged",
    }))?;
    verify_normal_route_object_patch_delete_row(
        &row,
        &target_document_id,
        &deleted_fields,
        &target_document_id,
        &deleted_value,
    )?;

    let retained_deleted_field = verify_normal_route_object_patch_delete_row(
        &row,
        &target_document_id,
        &deleted_fields,
        &target_document_id,
        &initial_value,
    )
    .expect_err("object patch post-state accepted a retained declared field");
    assert!(retained_deleted_field
        .to_string()
        .contains("retained a declared deleted field"));

    let unrelated_change = value::ConvexObject::try_from(json!({
        "retainedField": "changed",
    }))?;
    let unrelated_change = verify_normal_route_object_patch_delete_row(
        &row,
        &target_document_id,
        &deleted_fields,
        &target_document_id,
        &unrelated_change,
    )
    .expect_err("object patch post-state accepted an undeclared field change");
    assert!(unrelated_change
        .to_string()
        .contains("changed outside its declared fields"));
    Ok(())
}

#[test]
fn normal_route_fixture_reference_rejects_unknown_and_ambiguous_aliases() {
    let fixture_ids = BTreeMap::new();
    let unknown = resolve_normal_route_fixture_references(
        &json!({ "$fixtureId": "missing" }),
        &fixture_ids,
        &mut BTreeSet::new(),
    )
    .expect_err("unknown fixture alias was accepted");
    assert!(unknown.to_string().contains("alias is unavailable"));
    let ambiguous = resolve_normal_route_fixture_references(
        &json!({ "$fixtureId": "missing", "extra": true }),
        &fixture_ids,
        &mut BTreeSet::new(),
    )
    .expect_err("ambiguous fixture reference was accepted");
    assert!(ambiguous
        .to_string()
        .contains("must contain exactly $fixtureId"));
}

#[test]
fn normal_route_file_storage_setup_parsing_remains_closed() -> anyhow::Result<()> {
    let setup = json!({
        "contentBase64": "ZmlsZS1jb250ZW50",
        "contentType": "image/png",
        "expectedResult":
            "http://127.0.0.1:3210/api/storage/11111111-1111-4111-8111-111111111111",
        "invocationArguments": {
            "nested": [{ "$fileStorageId": "file" }],
        },
        "kind": "file-storage-get-url-v1",
        "storageUuid": "11111111-1111-4111-8111-111111111111",
    });
    let parsed: NormalRouteTestSetup = serde_json::from_value(setup.clone())?;
    let NormalRouteTestSetup::FileStorageGetUrlV1 {
        content_base64,
        content_type,
        expected_result,
        invocation_arguments,
        storage_uuid,
    } = parsed
    else {
        anyhow::bail!("file-storage setup parsed as a different fixture")
    };
    let validated = validate_normal_route_file_storage_setup(
        &content_base64,
        &content_type,
        &expected_result,
        &invocation_arguments,
        &storage_uuid,
    )?;
    assert_eq!(validated.content, Bytes::from_static(b"file-content"));
    assert_eq!(validated.content_type, "image/png");

    let mut setup_with_unknown_field = setup;
    setup_with_unknown_field
        .as_object_mut()
        .context("test setup is not an object")?
        .insert("unexpected".to_owned(), json!(true));
    assert!(
        serde_json::from_value::<NormalRouteTestSetup>(setup_with_unknown_field).is_err(),
        "file-storage setup accepted an unknown field"
    );
    Ok(())
}

#[test]
fn normal_route_file_storage_setup_rejects_invalid_values() {
    let expected_result = "http://127.0.0.1:3210/api/storage/11111111-1111-4111-8111-111111111111";
    let invocation_arguments = json!({ "id": { "$fileStorageId": "file" } });
    let storage_uuid = "11111111-1111-4111-8111-111111111111";
    assert!(validate_normal_route_file_storage_setup(
        "not base64",
        "image/png",
        expected_result,
        &invocation_arguments,
        storage_uuid,
    )
    .is_err());
    assert!(validate_normal_route_file_storage_setup(
        "ZmlsZQ==",
        "image/png\ninvalid",
        expected_result,
        &invocation_arguments,
        storage_uuid,
    )
    .is_err());
    assert!(validate_normal_route_file_storage_setup(
        "ZmlsZQ==",
        "image/png",
        expected_result,
        &invocation_arguments,
        "11111111-1111-4111-8111-11111111111A",
    )
    .is_err());
    assert!(validate_normal_route_file_storage_setup(
        "ZmlsZQ==",
        "image/png",
        &format!("{expected_result}?unexpected=1"),
        &invocation_arguments,
        storage_uuid,
    )
    .is_err());
    assert!(validate_normal_route_file_storage_setup(
        "ZmlsZQ==",
        "image/png",
        expected_result,
        &json!({ "id": "literal" }),
        storage_uuid,
    )
    .is_err());
}

#[test]
fn normal_route_file_storage_substitution_requires_one_closed_reference() -> anyhow::Result<()> {
    assert_eq!(
        substitute_normal_route_file_storage_id(
            &json!({
                "nested": [
                    { "marker": true },
                    { "$fileStorageId": "file" },
                ],
            }),
            "dynamic-storage-id",
        )?,
        json!({
            "nested": [
                { "marker": true },
                "dynamic-storage-id",
            ],
        })
    );
    for invalid in [
        json!({ "id": "literal" }),
        json!({
            "first": { "$fileStorageId": "file" },
            "second": { "$fileStorageId": "file" },
        }),
        json!({ "id": { "$fileStorageId": "other" } }),
        json!({ "id": { "$fileStorageId": "file", "extra": true } }),
    ] {
        substitute_normal_route_file_storage_id(&invalid, "dynamic-storage-id")
            .expect_err("invalid file-storage reference was accepted");
    }
    Ok(())
}

#[test]
fn normal_route_table_mapping_sha256_is_stable_and_captures_added_tables() -> anyhow::Result<()> {
    let first = normal_route_table_mapping_sha256([
        ("counterpartyReviews".to_owned(), 10057),
        ("counterparties".to_owned(), 10058),
    ])?;
    let reordered = normal_route_table_mapping_sha256([
        ("counterparties".to_owned(), 10058),
        ("counterpartyReviews".to_owned(), 10057),
    ])?;
    let added = normal_route_table_mapping_sha256([
        ("counterpartyReviews".to_owned(), 10057),
        ("counterparties".to_owned(), 10058),
        ("counterpartyReviewActivity".to_owned(), 10059),
    ])?;
    assert_eq!(first, reordered);
    assert_ne!(first, added);
    Ok(())
}

async fn run_normal_route_schema_materialization_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
    let database = new_database(rt, persistence).await?;
    initialize_application_system_tables(&database).await?;
    let schema = common::db_schema!(
        "normalRouteSchemaMaterializedFirst" => common::schemas::DocumentSchema::Any,
        "normalRouteSchemaMaterializedSecond" => common::schemas::DocumentSchema::Any,
    );
    let indexed_table: TableName = "normalRouteSchemaMaterializedFirst".parse()?;
    let index_descriptor = IndexDescriptor::new("by_value")?;
    let index_fields = IndexedFields::try_from(vec!["value".parse()?])?;
    let index_name = IndexName::new(indexed_table.clone(), index_descriptor.clone())?;
    let mut schema = schema;
    schema
        .tables
        .get_mut(&indexed_table)
        .context("materialized schema omitted its indexed table")?
        .indexes
        .insert(
            index_descriptor.clone(),
            common::schemas::IndexSchema {
                index_descriptor,
                fields: index_fields.clone(),
            },
        );
    let expected_tables = schema.tables.keys().cloned().collect::<BTreeSet<_>>();
    let mut setup_tx = database.begin_system().await?;
    materialize_normal_route_schema(&mut setup_tx, &schema).await?;
    database
        .commit_with_write_source(
            setup_tx,
            "normal_route_schema_table_materialization_test_setup",
        )
        .await?;
    let mut check_tx = database.begin_system().await?;
    let actual_tables = check_tx
        .table_mapping()
        .iter_active_user_tables()
        .filter_map(|(_, namespace, _, table)| {
            (namespace == TableNamespace::root_component()).then(|| table.clone())
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_tables, expected_tables);
    let actual_indexes = IndexModel::new(&mut check_tx)
        .get_application_indexes(TableNamespace::root_component())
        .await?
        .into_iter()
        .map(|index| index.into_value())
        .collect::<Vec<_>>();
    let has_expected_index = actual_indexes.iter().any(|index| {
        index.name == index_name
            && matches!(
                &index.config,
                IndexConfig::Database {
                    spec,
                    on_disk_state: DatabaseIndexState::Enabled,
                    ..
                } if spec.fields == index_fields
            )
    });
    assert!(
        has_expected_index,
        "materialized application index differs from its public contract: {actual_indexes:?}"
    );
    drop(check_tx);
    database.shutdown().await?;
    Ok(())
}

#[test]
fn normal_route_schema_tables_and_indexes_are_materialized_before_paired_execution(
) -> anyhow::Result<()> {
    let _gate_test_guard = GATE_TEST_LOCK.lock().expect("gate test lock was poisoned");
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "normal_route_schema_table_materialization",
        run_normal_route_schema_materialization_test(rt),
    )
}

async fn run_normal_route_file_storage_lifecycle_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
    let database = new_database(rt.clone(), persistence).await?;
    initialize_application_system_tables(&database).await?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt)?);
    let setup = validate_normal_route_file_storage_setup(
        "ZmlsZS1jb250ZW50",
        "image/png",
        "http://127.0.0.1:3210/api/storage/11111111-1111-4111-8111-111111111111",
        &json!({ "id": { "$fileStorageId": "file" } }),
        "11111111-1111-4111-8111-111111111111",
    )?;
    let mut tx = database.begin_system().await?;
    let fixture = install_normal_route_file_storage_fixture(&mut tx, &storage, &setup).await?;
    database
        .commit_with_write_source(tx, "normal_route_file_storage_test_setup")
        .await?;
    assert_eq!(
        fixture.invocation_arguments,
        json!({ "id": fixture.developer_id.encode() })
    );
    verify_normal_route_file_storage_fixture(&database, &storage, &fixture).await?;
    verify_normal_route_file_storage_fixture(&database, &storage, &fixture).await?;
    database.shutdown().await?;
    Ok(())
}

#[test]
fn normal_route_file_storage_fixture_preserves_object_and_metadata() -> anyhow::Result<()> {
    let _gate_test_guard = GATE_TEST_LOCK.lock().expect("gate test lock was poisoned");
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "normal_route_file_storage_lifecycle",
        run_normal_route_file_storage_lifecycle_test(rt),
    )
}

fn write_normal_route_observation(path: &Path, observation: &impl Serialize) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec(observation)?;
    bytes.push(b'\n');
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .context("failed to create normal route structural observation")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .context("failed to set normal route structural observation permissions")?;
    }
    file.write_all(&bytes)
        .context("failed to write normal route structural observation")?;
    file.sync_all()
        .context("failed to sync normal route structural observation")?;
    Ok(())
}

fn write_normal_route_batch_progress(
    path: &Path,
    progress: &NormalRoutePairedLaneBatchProgress,
) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec(progress)?;
    bytes.push(b'\n');
    let file_name = path
        .file_name()
        .context("paired-lane batch progress path has no file name")?
        .to_str()
        .context("paired-lane batch progress path is not UTF-8")?;
    let temporary_path = path.with_file_name(format!(".{file_name}.tmp"));
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary_path)
        .context("failed to create paired-lane batch progress temporary file")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .context("failed to set paired-lane batch progress permissions")?;
    }
    file.write_all(&bytes)
        .context("failed to write paired-lane batch progress")?;
    file.sync_all()
        .context("failed to sync paired-lane batch progress")?;
    drop(file);
    std::fs::rename(&temporary_path, path)
        .context("failed to publish paired-lane batch progress")?;
    let parent = path
        .parent()
        .context("paired-lane batch progress path has no parent")?;
    std::fs::File::open(parent)
        .context("failed to open paired-lane batch progress parent")?
        .sync_all()
        .context("failed to sync paired-lane batch progress parent")?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn normal_route_observation_writer_creates_owner_private_file() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("observation.json");
    write_normal_route_observation(&path, &json!({ "kind": "test" }))?;
    let mode = std::fs::metadata(&path)?.permissions().mode() & 0o7777;
    anyhow::ensure!(
        mode == 0o600,
        "normal route observation mode is {mode:o}, expected 600"
    );
    let error = write_normal_route_observation(&path, &json!({ "kind": "test" }))
        .expect_err("normal route observation writer overwrote an existing file");
    assert!(error
        .to_string()
        .contains("failed to create normal route structural observation"));
    Ok(())
}

fn normal_route_shadow_primary_wasm_routing_enabled(udf_type: UdfType) -> anyhow::Result<bool> {
    anyhow::ensure!(
        std::env::var("CONVEX_STATIC_HERMES_WASM_GATE_ENABLED")? == "0",
        "shadow test requires normal Static Hermes Wasm routing to remain disabled"
    );
    let sampling_bps = match udf_type {
        UdfType::Query => *APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS,
        UdfType::Mutation => *APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS,
        UdfType::Action | UdfType::HttpAction => {
            anyhow::bail!("shadow test selected an unsupported UDF type")
        },
    };
    anyhow::ensure!(
        sampling_bps == 10_000,
        "shadow test requires complete sampling for the selected UDF type"
    );
    Ok(false)
}

fn normal_route_shadow_summary_text(summary: &QueryShadowEvidenceSummary) -> String {
    format!(
        concat!(
            "attempts={}, admitted={}, completed={}, mismatches={}, terminals={} ",
            "(capacity_drop={}, shadow_failure={}, timeout={}, invalid_shadow={}, ",
            "invalid_primary={}, primary_failure={}, primary_snapshot_rejected={}, ",
            "unexpected_task_exit={})"
        ),
        summary.attempt_count,
        summary.admitted_count,
        summary.completed_count,
        summary.mismatch_count,
        summary.terminal_count,
        summary.terminal_counts.capacity_drop,
        summary.terminal_counts.shadow_failure,
        summary.terminal_counts.timeout,
        summary.terminal_counts.invalid_shadow,
        summary.terminal_counts.invalid_primary,
        summary.terminal_counts.primary_failure,
        summary.terminal_counts.primary_snapshot_rejected,
        summary.terminal_counts.unexpected_task_exit,
    )
}

fn normal_route_shadow_hook_summary(hooks: &StaticHermesGateTestHooks) -> String {
    format!(
        "generated_route_preflight_stages={:?}, generated_pool_misses={}, instances={}, \
         runtime_ids={}, runtime_invocations={}",
        hooks.generated_route_preflight_stages(),
        hooks.generated_pool_misses(),
        hooks.instance_ids().len(),
        hooks.generated_runtime_ids().len(),
        hooks.generated_invocations().len(),
    )
}

fn normal_route_shadow_evidence_pages<F>(
    route_key: &QueryShadowRouteKey,
    mut query_page: F,
) -> anyhow::Result<(
    QueryShadowEvidenceSummary,
    Option<QueryShadowEvidenceSummary>,
)>
where
    F: FnMut(Option<&QueryShadowRouteKey>) -> anyhow::Result<QueryShadowEvidencePage>,
{
    let mut cursor = None;
    loop {
        // Evidence cursors are exclusive, so begin at None and walk pages until the
        // target route is included.
        let evidence = query_page(cursor.as_ref())?;
        let selection = evidence
            .selection
            .context("query-shadow test has no authenticated registry selection")?;
        anyhow::ensure!(
            selection.generation_sha256.as_str() == route_key.generation_sha256()
                && selection.selected_route_count > 0,
            "query-shadow test registry selection differs from the authenticated route"
        );
        let aggregate = evidence.aggregate;
        if let Some(route) = evidence
            .routes
            .into_iter()
            .find(|route| route.route_key.as_str() == route_key.as_str())
        {
            return Ok((aggregate, Some(route.summary)));
        }
        let Some(next_cursor) = evidence.next_cursor else {
            return Ok((aggregate, None));
        };
        cursor = Some(next_cursor);
    }
}

fn normal_route_shadow_evidence(
    function_log: &FunctionExecutionLog<ProdRuntime>,
    udf_type: UdfType,
    route_key: &QueryShadowRouteKey,
) -> anyhow::Result<(
    QueryShadowEvidenceSummary,
    Option<QueryShadowEvidenceSummary>,
)> {
    normal_route_shadow_evidence_pages(route_key, |cursor| {
        function_log.query_shadow_evidence(udf_type, cursor, NORMAL_ROUTE_SHADOW_PAGE_LIMIT)
    })
}

#[test]
fn normal_route_shadow_evidence_walks_exclusive_cursors() -> anyhow::Result<()> {
    let generation_sha256 = "a".repeat(64);
    let first_route = QueryShadowRouteKey::new(&generation_sha256, &"b".repeat(64))?;
    let target_route = QueryShadowRouteKey::new(&generation_sha256, &"c".repeat(64))?;
    let timing = QueryShadowTimingSummary {
        exact_sample_count: 0,
        p50: None,
        p90: None,
        p99: None,
        retained_max: None,
    };
    let summary = QueryShadowEvidenceSummary {
        attempt_count: 0,
        admitted_count: 0,
        capacity_drop_count: 0,
        evidence_count: 0,
        completed_count: 0,
        mismatch_count: 0,
        mismatch_counts: Default::default(),
        inserted_document_identity_counts: Default::default(),
        terminal_count: 0,
        terminal_counts: Default::default(),
        shadow_failure_stage_counts: Default::default(),
        shadow_failure_reason_counts: Default::default(),
        primary_seconds: timing.clone(),
        wasm_seconds: timing.clone(),
        wasm_to_primary_ratio: timing.clone(),
        wasm_primary_seconds: timing.clone(),
        v8_verifier_seconds: timing.clone(),
        v8_verifier_to_wasm_primary_ratio: timing,
    };
    let selection = QueryShadowRegistrySelection {
        udf_type: UdfType::Query,
        generation_sha256: generation_sha256.clone(),
        selected_route_count: 2,
        routes_with_retained_evidence_count: 0,
        routes_with_completed_comparison_count: 0,
        routes_with_mismatch_count: 0,
        routes_with_terminal_count: 0,
        unobserved_route_count: 2,
    };
    let route_summary = summary.clone();
    let route_evidence = move |route_key| QueryShadowRouteEvidence {
        route_key,
        selected_count: 1,
        unobserved_count: 1,
        summary: route_summary.clone(),
        diagnostics: vec![],
    };
    let pages = vec![
        QueryShadowEvidencePage {
            udf_type: UdfType::Query,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            aggregate: summary.clone(),
            selection: Some(selection.clone()),
            routes: vec![route_evidence(first_route.clone())],
            next_cursor: Some(first_route.clone()),
        },
        QueryShadowEvidencePage {
            udf_type: UdfType::Query,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            aggregate: summary,
            selection: Some(selection),
            routes: vec![route_evidence(target_route.clone())],
            next_cursor: None,
        },
    ];
    let mut pages = pages.into_iter();
    let mut cursors = Vec::new();
    let (_, found) = normal_route_shadow_evidence_pages(&target_route, |cursor| {
        cursors.push(cursor.cloned());
        pages
            .next()
            .context("query-shadow test page is unavailable")
    })?;
    assert_eq!(cursors, vec![None, Some(first_route)]);
    assert!(found.is_some());
    Ok(())
}

fn normal_route_shadow_timeout_summary(
    function_log: &FunctionExecutionLog<ProdRuntime>,
    udf_type: UdfType,
    route_key: &QueryShadowRouteKey,
) -> anyhow::Result<String> {
    let (aggregate, route) = normal_route_shadow_evidence(function_log, udf_type, route_key)?;
    if let Some(route) = route {
        return Ok(format!(
            "retained route evidence: {}",
            normal_route_shadow_summary_text(&route)
        ));
    }
    Ok(format!(
        "target route has no retained evidence; aggregate evidence: {}",
        normal_route_shadow_summary_text(&aggregate)
    ))
}

async fn wait_for_normal_route_shadow_observation(
    function_log: &FunctionExecutionLog<ProdRuntime>,
    expectation: &NormalRouteTestExpectation,
    hooks: &StaticHermesGateTestHooks,
    udf_type: UdfType,
    expected_completed_comparison_count: u64,
) -> anyhow::Result<NormalRouteShadowObservation> {
    let route_key = QueryShadowRouteKey::new(
        &expectation.generation_sha256,
        &expectation.routing.route_id,
    )?;
    match tokio::time::timeout(NORMAL_ROUTE_TEST_PHASE_TIMEOUT, async {
        loop {
            let (_, summary) = normal_route_shadow_evidence(function_log, udf_type, &route_key)?;
            if let Some(summary) = summary {
                let completed_or_terminal = summary
                    .completed_count
                    .checked_add(summary.terminal_count)
                    .context("query-shadow evidence count overflow")?;
                if summary.terminal_count > 0 {
                    anyhow::bail!(
                        concat!(
                            "query-shadow route reached a terminal outcome before the ",
                            "expected comparison: {}; {}"
                        ),
                        normal_route_shadow_summary_text(&summary),
                        normal_route_shadow_hook_summary(hooks),
                    );
                }
                if completed_or_terminal > 0
                    && summary.completed_count >= expected_completed_comparison_count
                {
                    return Ok(NormalRouteShadowObservation {
                        generation_sha256: expectation.generation_sha256.clone(),
                        route_id: expectation.routing.route_id.clone(),
                        primary_wasm_routing_enabled:
                            normal_route_shadow_primary_wasm_routing_enabled(udf_type)?,
                        attempt_count: summary.attempt_count,
                        admitted_count: summary.admitted_count,
                        completed_comparison_count: summary.completed_count,
                        mismatch_count: summary.mismatch_count,
                        mismatch_counts: summary.mismatch_counts.into(),
                        terminal_count: summary.terminal_count,
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    {
        Ok(result) => result,
        Err(_) => {
            let evidence = normal_route_shadow_timeout_summary(function_log, udf_type, &route_key)
                .context("failed to collect query-shadow timeout evidence")?;
            anyhow::bail!(
                "query-shadow comparison did not finish before the normal-route timeout: \
                 {evidence}; {}",
                normal_route_shadow_hook_summary(hooks),
            );
        },
    }
}

fn run_normal_route_invocation<'a>(
    runner: &'a ApplicationFunctionRunner<ProdRuntime>,
    database: &'a Database<ProdRuntime>,
    path: PublicFunctionPath,
    identity: Identity,
    udf_kind: &'a str,
    argument: JsonValue,
) -> Pin<
    Box<
        dyn Future<Output = anyhow::Result<(JsonValue, NormalRouteQueryJournalObservation)>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let arguments = SerializedArgs::from_args(vec![argument])?;
        match udf_kind {
            "query" => {
                let query = runner
                    .run_query_at_ts(
                        RequestContext::new_for_system_request(RequestId::new()),
                        path,
                        arguments,
                        identity,
                        *database.now_ts_for_reads(),
                        None,
                        FunctionCaller::Cron,
                        QueryInvocation::Fresh,
                    )
                    .await?;
                let journal = (&query.journal).into();
                let result = query
                    .result
                    .map_err(|error| anyhow::anyhow!("normal registry route failed: {error}"))?;
                Ok((result.json_value(), journal))
            },
            "mutation" => {
                let caller = FunctionCaller::Cron;
                let request_context = RequestContext::new_for_system_request(RequestId::new());
                let context = ExecutionContext::new(request_context, &caller);
                let transaction = database.begin(identity).await?;
                let (transaction, outcome) = runner
                    .run_mutation_no_udf_log(
                        transaction,
                        path,
                        arguments,
                        caller.allowed_visibility(),
                        context,
                        None,
                        SchedulerDependencyClass::Independent,
                    )
                    .await?;
                let journal = (&outcome.journal).into();
                let result = outcome
                    .result
                    .map_err(|error| anyhow::anyhow!("normal registry route failed: {error}"))?;
                let commit_ts = database
                    .commit_with_write_source(
                        transaction,
                        "static_hermes_normal_route_test_mutation",
                    )
                    .await?;
                Ok((
                    result.resolve_commit_ts(i64::from(commit_ts))?.json_value(),
                    journal,
                ))
            },
            _ => anyhow::bail!("normal route fixture selected an unsupported UDF kind"),
        }
    })
}

async fn install_normal_route_provenance_mismatch(
    database: &Database<ProdRuntime>,
) -> anyhow::Result<()> {
    let mismatched_sha256 = Sha256::hash(b"normal-route-provenance-mismatch");
    let mut transaction = database.begin_system().await?;
    let active_source_package =
        SourcePackageModel::new(&mut transaction, TableNamespace::root_component())
            .get_latest()
            .await?
            .context("normal route provenance fixture omitted its active source package")?;
    anyhow::ensure!(
        active_source_package.sha256 != mismatched_sha256
            && active_source_package.runtime_content_sha256.as_ref() != Some(&mismatched_sha256),
        "normal route provenance mismatch unexpectedly matches the active source package"
    );
    SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
        .replace(
            active_source_package.id(),
            SourcePackage {
                storage_key: active_source_package.storage_key.clone(),
                sha256: mismatched_sha256.clone(),
                runtime_content_sha256: active_source_package
                    .runtime_content_sha256
                    .as_ref()
                    .map(|_| mismatched_sha256.clone()),
                runtime_generation: active_source_package.runtime_generation.clone(),
                external_deps_package_id: active_source_package.external_deps_package_id.clone(),
                package_size: active_source_package.package_size,
                node_version: active_source_package.node_version,
                node_executor_pool_topology: active_source_package
                    .node_executor_pool_topology
                    .clone(),
            }
            .try_into()?,
        )
        .await?;
    database
        .commit_with_write_source(
            transaction,
            "static_hermes_normal_route_test_provenance_mismatch",
        )
        .await?;
    let mut verification_transaction = database.begin_system().await?;
    let active_source_package = SourcePackageModel::new(
        &mut verification_transaction,
        TableNamespace::root_component(),
    )
    .get_latest()
    .await?
    .context("normal route provenance fixture lost its active source package")?;
    anyhow::ensure!(
        active_source_package.sha256 == mismatched_sha256
            && active_source_package
                .runtime_content_sha256
                .as_ref()
                .is_none_or(|sha256| sha256 == &mismatched_sha256),
        "normal route provenance fixture did not install an active mismatch"
    );
    Ok(())
}

async fn run_generated_occ_retry_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
    let database = new_database(rt.clone(), Arc::clone(&persistence)).await?;
    let (table, id) = install_gate_module_and_document(&database).await?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
    let runner = new_runner(
        rt,
        database.clone(),
        persistence,
        storage,
        Arc::new(UnusedModuleLoader),
        QueryCache::new(1 << 20),
        KeyBroker::dev(),
    )
    .await?;

    let effects: TableName = "static_hermes_gate_effects".parse()?;
    let retry_hooks = StaticHermesGateTestHooks::new_generated_occ_mutation(
        &table.to_string(),
        &effects.to_string(),
        1,
    )?;
    let retry_guard = install_static_hermes_gate_test_hooks(&retry_hooks)?;
    let mut retried = generated_mutation_future(&runner, json!(id.encode()));
    let first_instance = tokio::select! {
        result = &mut retried => {
            anyhow::bail!("routed mutation completed before OCC conflict: {result:?}")
        },
        instance = retry_hooks.wait_for_guest_completion() => instance?,
    };
    replace_document(&database, id, 3).await?;
    retry_hooks.release_guest_completion(first_instance)?;
    let mutation_return = retried.await??;
    let effect_id = value::DeveloperDocumentId::decode(
        mutation_return
            .value
            .json_value()
            .as_str()
            .context("generated mutation did not return its inserted document ID")?,
    )?;
    let mut effect_tx = database.begin_system().await?;
    let effect = UserFacingModel::new(&mut effect_tx, TableNamespace::root_component())
        .get_with_ts(effect_id, None)
        .await?
        .context("committed generated mutation effect was not readable")?
        .0
        .to_internal_json();
    assert_eq!(effect.get("marker"), Some(&json!("committed-effect")));
    drop(effect_tx);
    assert_eq!(table_count(&database, &effects).await?, 1);
    assert_eq!(retry_hooks.read_completed(), 4);
    assert_eq!(retry_hooks.read_cancelled(), 0);
    assert_eq!(retry_hooks.read_documents(), 2);
    assert!(retry_hooks.read_bytes() > 0);
    assert!(retry_hooks.read_intervals() > 0);
    assert_eq!(retry_hooks.store_drops(), 1);
    assert_eq!(retry_hooks.teardowns(), 0);
    let instances = retry_hooks.instance_ids();
    assert_eq!(instances.len(), 2);
    assert_eq!(instances[0], first_instance);
    assert_ne!(instances[0], instances[1]);
    let generated_runtimes = retry_hooks.generated_runtime_ids();
    assert_eq!(generated_runtimes.len(), 2);
    assert_eq!(generated_runtimes[0], generated_runtimes[1]);

    let invalid = generated_mutation_future(&runner, json!("not-a-document-id")).await?;
    let invalid = invalid.expect_err("invalid generated mutation ID should fail");
    assert!(invalid.error.message.contains("InvalidArgument"));
    assert_eq!(table_count(&database, &effects).await?, 1);
    assert_eq!(retry_hooks.read_completed(), 5);
    assert_eq!(retry_hooks.read_documents(), 2);
    assert_eq!(retry_hooks.teardowns(), 2);
    let generated_runtimes = retry_hooks.generated_runtime_ids();
    assert_eq!(generated_runtimes.len(), 3);
    assert_eq!(generated_runtimes[0], generated_runtimes[1]);
    assert_ne!(generated_runtimes[1], generated_runtimes[2]);
    drop(retry_guard);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn generated_wasm_mutation_retries_occ_through_application_boundary() -> anyhow::Result<()> {
    let _gate_test_guard = GATE_TEST_LOCK.lock().expect("gate test lock was poisoned");
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_application_occ_retry",
        run_generated_occ_retry_test(rt),
    )
}

async fn run_generated_query_terminal_cancellation_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
    let database = new_database(rt.clone(), Arc::clone(&persistence)).await?;
    let table = install_gate_query_module_and_documents(&database).await?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
    let runner = new_runner(
        rt,
        database.clone(),
        persistence,
        storage,
        Arc::new(UnusedModuleLoader),
        QueryCache::new(1 << 20),
        KeyBroker::dev(),
    )
    .await?;
    let hooks =
        StaticHermesGateTestHooks::new_generated_query_terminal_pair(&table.to_string(), 1)?;
    let hooks_guard = install_static_hermes_gate_test_hooks(&hooks)?;
    let path = CanonicalizedComponentFunctionPath {
        component: ComponentPath::root(),
        udf_path: CanonicalizedUdfPath::new("generatedWasmQueryTest.js".parse()?, "run".parse()?),
    };
    let mut execution = Box::pin(runner.run_query_without_caching(
        RequestContext::new_for_system_request(RequestId::new()),
        database.begin_system().await?,
        path,
        SerializedArgs::from_args(vec![json!({
            "firstTenant": "tenant-a",
            "secondTenant": "tenant-b",
        })])?,
        FunctionCaller::Cron,
    ));
    let instance_id = tokio::select! {
        result = &mut execution => {
            anyhow::bail!(
                "query-terminal route completed before public-runner cancellation: {result:?}"
            )
        },
        instance_id = hooks.wait_for_pending_read() => instance_id?,
    };
    assert_eq!(hooks.database_query_starts(), 2);

    // Dropping the public runner future is its cancellation contract. The
    // IsolateClient drop guard must terminate the generated invocation.
    drop(execution);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if hooks.read_cancelled() == 1 && hooks.teardowns() == 1 && hooks.store_drops() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("public-runner query-terminal cancellation did not clean up")?;
    assert_eq!(hooks.read_completed(), 0);
    assert_eq!(hooks.transaction_drops(), 0);
    assert_eq!(hooks.terminal_cancellations(), 1);
    assert_eq!(hooks.instance_ids(), vec![instance_id]);
    let execution_errors = hooks.execution_errors();
    assert_eq!(execution_errors.len(), 1);
    assert!(execution_errors[0].contains("generated async syscall cancelled"));
    hooks
        .release_read(instance_id)
        .expect_err("cancelled query-terminal read remained live");
    drop(hooks_guard);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn generated_query_terminals_cancel_through_application_boundary() -> anyhow::Result<()> {
    let _gate_test_guard = GATE_TEST_LOCK.lock().expect("gate test lock was poisoned");
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_application_query_terminal_cancellation",
        run_generated_query_terminal_cancellation_test(rt),
    )
}

async fn run_real_deployment_function_runner_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let (selected, expectation) = real_runner_test_fixture()?;
    let udf_type: UdfType = selected.udf_kind.parse()?;
    anyhow::ensure!(
        udf_type == UdfType::Query,
        "real runner fixture currently requires a query export"
    );
    let visibility = match selected.visibility.as_str() {
        "internal" => Visibility::Internal,
        "public" => Visibility::Public,
        _ => anyhow::bail!("real runner export has an unsupported visibility"),
    };

    let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
    let database = new_database(rt.clone(), Arc::clone(&persistence)).await?;
    install_gate_module(
        &database,
        &selected.runtime_module_path,
        &selected.export_name,
        udf_type,
        visibility,
    )
    .await?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
    let runner = new_runner(
        rt,
        database.clone(),
        persistence,
        storage,
        Arc::new(UnusedModuleLoader),
        QueryCache::new(1 << 20),
        KeyBroker::dev(),
    )
    .await?;
    let path = CanonicalizedComponentFunctionPath {
        component: ComponentPath::root(),
        udf_path: CanonicalizedUdfPath::new(
            selected.runtime_module_path.parse()?,
            selected.export_name.parse()?,
        ),
    };
    let hooks = StaticHermesGateTestHooks::new(0, 0);
    configure_normal_route_execution_fuel(&hooks)?;
    let hooks_guard = install_static_hermes_gate_test_hooks(&hooks)?;
    let tx = database.begin_system().await?;
    let execution = runner
        .run_query_without_caching(
            RequestContext::new_for_system_request(RequestId::new()),
            tx,
            path,
            SerializedArgs::from_args(expectation.arguments)?,
            FunctionCaller::Cron,
        )
        .await;
    anyhow::ensure!(
        hooks.database_query_starts() == 1,
        "real runner did not reach its generic imported database query: {:?}",
        hooks.execution_errors()
    );
    let error = match execution {
        Ok((Ok(_), _)) => {
            anyhow::bail!("real runner fixture unexpectedly completed without its database fixture")
        },
        Ok((Err(error), _)) => error.message,
        Err(error) => error.to_string(),
    };
    anyhow::ensure!(
        error.contains(&expectation.expected_error_contains),
        "real runner stopped at an unexpected boundary: {error}"
    );
    drop(hooks_guard);
    database.shutdown().await?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn normal_route_linux_process_rss_bytes() -> anyhow::Result<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm")
        .context("failed to read /proc/self/statm for the warm benchmark")?;
    let resident_pages = statm
        .split_ascii_whitespace()
        .nth(1)
        .context("/proc/self/statm omitted the resident-page count")?
        .parse::<u64>()
        .context("/proc/self/statm resident-page count is invalid")?;
    // SAFETY: sysconf with _SC_PAGESIZE has no memory-safety precondition.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    anyhow::ensure!(page_size > 0, "sysconf(_SC_PAGESIZE) returned {page_size}");
    resident_pages
        .checked_mul(u64::try_from(page_size).context("page size is negative")?)
        .context("Linux process RSS byte count overflowed")
}

#[cfg(not(target_os = "linux"))]
fn normal_route_linux_process_rss_bytes() -> anyhow::Result<u64> {
    anyhow::bail!("warm benchmark requires Linux /proc/self/statm RSS accounting")
}

fn normal_route_warm_benchmark_percentile(
    sorted_nanoseconds: &[u64],
    percentage: usize,
) -> anyhow::Result<u64> {
    anyhow::ensure!(
        !sorted_nanoseconds.is_empty() && percentage > 0 && percentage <= 100,
        "warm benchmark percentile input is invalid"
    );
    // Nearest-rank: percentile p selects sorted[ceil(p * N / 100) - 1].
    let rank = percentage
        .checked_mul(sorted_nanoseconds.len())
        .context("warm benchmark percentile rank overflow")?
        .checked_add(99)
        .context("warm benchmark percentile rank overflow")?
        / 100;
    Ok(sorted_nanoseconds[rank - 1])
}

async fn run_normal_route_warm_benchmark(
    runner: &ApplicationFunctionRunner<ProdRuntime>,
    database: &Database<ProdRuntime>,
    path: PublicFunctionPath,
    identity: Identity,
    expectation: &NormalRouteTestExpectation,
    fixture: &NormalRouteFixtureState,
    hooks: &StaticHermesGateTestHooks,
    benchmark: &NormalRouteWarmBenchmark,
) -> anyhow::Result<()> {
    let NormalRouteFixtureState::ApiKeyValidation { first_api_key, .. } = fixture else {
        anyhow::bail!("warm benchmark fixture changed after validation");
    };
    let invocation_arguments = json!({ "apiKey": first_api_key });
    let rss_before_prime = normal_route_linux_process_rss_bytes()?;
    let (prime_result, _) = run_normal_route_invocation(
        runner,
        database,
        path.clone(),
        identity.clone(),
        &expectation.route.udf_kind,
        invocation_arguments.clone(),
    )
    .await?;
    normalize_normal_route_result(&prime_result, fixture, 0, &mut None)?;
    let rss_after_prime_return = normal_route_linux_process_rss_bytes()?;

    let mut durations_nanoseconds = Vec::with_capacity(benchmark.iterations);
    for _ in 0..benchmark.iterations {
        let started = Instant::now();
        let (result, _) = run_normal_route_invocation(
            runner,
            database,
            path.clone(),
            identity.clone(),
            &expectation.route.udf_kind,
            invocation_arguments.clone(),
        )
        .await?;
        let elapsed_nanoseconds = u64::try_from(started.elapsed().as_nanos())
            .context("warm benchmark invocation duration is out of range")?;
        normalize_normal_route_result(&result, fixture, 0, &mut None)?;
        durations_nanoseconds.push(elapsed_nanoseconds);
    }
    let rss_after_measured_returns = normal_route_linux_process_rss_bytes()?;
    let total_nanoseconds = durations_nanoseconds
        .iter()
        .try_fold(0_u64, |total, value| {
            total
                .checked_add(*value)
                .context("warm benchmark total duration overflow")
        })?;
    anyhow::ensure!(
        total_nanoseconds > 0,
        "warm benchmark measured zero total duration"
    );
    let mut sorted_nanoseconds = durations_nanoseconds;
    sorted_nanoseconds.sort_unstable();
    let runtime_ids = hooks.generated_runtime_ids();
    let instance_ids = hooks.instance_ids();
    let runtime_invocations = hooks.generated_invocations();
    let pool_hits = hooks.generated_pool_hits();
    let pool_misses = hooks.generated_pool_misses();
    let memory_snapshots = hooks.generated_memory_snapshots();
    let unique_runtime_count = runtime_ids.iter().copied().collect::<BTreeSet<_>>().len();
    let unique_memory_slot_count = runtime_invocations
        .iter()
        .map(|observation| observation.memory_slot_id)
        .collect::<BTreeSet<_>>()
        .len();

    let memory_controller = match expectation.lane {
        NormalRouteTestLane::Wasm => {
            let total_invocation_count = benchmark
                .iterations
                .checked_add(1)
                .context("warm benchmark invocation count overflow")?;
            let final_snapshot = memory_snapshots
                .last()
                .context("warm benchmark Wasm lane recorded no post-return memory snapshot")?;
            anyhow::ensure!(
                runtime_ids.len() == total_invocation_count
                    && instance_ids.len() == total_invocation_count
                    && runtime_invocations.len() == total_invocation_count
                    && unique_runtime_count == 1
                    && unique_memory_slot_count == 1
                    && runtime_invocations[0].runtime_was_created
                    && runtime_invocations
                        .iter()
                        .skip(1)
                        .all(|observation| !observation.runtime_was_created)
                    && runtime_invocations.iter().all(|observation| {
                        observation.deployment_sha256.as_deref()
                            == Some(expectation.deployment_sha256.as_str())
                            && observation.package_key == expectation.package_key
                            && observation.entry_id.as_deref()
                                == Some(expectation.routing.entry_id.as_str())
                            && observation.entry_selector_id.as_deref()
                                == Some(expectation.routing.entry_selector_id.as_str())
                            && observation.capability_revoked
                            && observation.forged_capability_rejected
                            && observation.revoked_capability_rejected
                            && observation.opaque_live_handles == 0
                            && observation.opaque_current_bytes == 0
                            && !observation.runtime_reuse_contaminated
                    })
                    && runtime_invocations
                        .iter()
                        .skip(1)
                        .all(|observation| observation.prior_capability_rejected)
                    && pool_misses == 1
                    && pool_hits == benchmark.iterations
                    && memory_snapshots.len() == total_invocation_count
                    && final_snapshot.active_instances == 0
                    && final_snapshot.idle_instances == 1
                    && final_snapshot.evicting_instances == 0
                    && final_snapshot.function_samples
                        == u64::try_from(total_invocation_count)
                            .context("warm benchmark sample count is out of range")?
                    && final_snapshot.developer_error_samples == 0
                    && final_snapshot.system_error_samples == 0
                    && final_snapshot.timeout_samples == 0
                    && final_snapshot.resource_limit_samples == 0
                    && final_snapshot.memory_limit_samples == 0,
                "warm benchmark did not retain exactly one clean Wasm runtime and memory slot: \
                 runtime_ids={runtime_ids:?}, runtime_invocations={}, pool_hits={pool_hits}, \
                 pool_misses={pool_misses}, memory_snapshots={}, final_snapshot={final_snapshot:?}",
                runtime_invocations.len(),
                memory_snapshots.len(),
            );
            json!({
                "postReturnSnapshotCount": memory_snapshots.len(),
                "finalPostReturnSnapshot": {
                    "activeInstances": final_snapshot.active_instances,
                    "idleInstances": final_snapshot.idle_instances,
                    "evictingInstances": final_snapshot.evicting_instances,
                    "fixedModuleBytes": final_snapshot.fixed_module_bytes,
                    "retainedBaselineBytes": final_snapshot.retained_baseline_bytes,
                    "activeCheckoutBaselineBytes": final_snapshot.active_checkout_baseline_bytes,
                    "activeGuestBytes": final_snapshot.active_guest_bytes,
                    "activeHostBytes": final_snapshot.active_host_bytes,
                    "activeCurrentGrowthBytes": final_snapshot.active_current_growth_bytes,
                    "activeForecastBytes": final_snapshot.active_forecast_bytes,
                    "activeLiabilityBytes": final_snapshot.active_liability_bytes,
                    "unattributedAllowanceBytes": final_snapshot.unattributed_allowance_bytes,
                    "projectedBytes": final_snapshot.projected_bytes,
                    "functionSamples": final_snapshot.function_samples,
                    "developerErrorSamples": final_snapshot.developer_error_samples,
                    "systemErrorSamples": final_snapshot.system_error_samples,
                    "timeoutSamples": final_snapshot.timeout_samples,
                    "resourceLimitSamples": final_snapshot.resource_limit_samples,
                    "memoryLimitSamples": final_snapshot.memory_limit_samples,
                },
            })
        },
        NormalRouteTestLane::V8 => {
            anyhow::ensure!(
                runtime_ids.is_empty()
                    && instance_ids.is_empty()
                    && runtime_invocations.is_empty()
                    && pool_hits == 0
                    && pool_misses == 0
                    && memory_snapshots.is_empty(),
                "V8 warm benchmark reached generated Wasm state: runtime_ids={runtime_ids:?}, \
                 pool_hits={pool_hits}, pool_misses={pool_misses}, memory_snapshots={}",
                memory_snapshots.len(),
            );
            JsonValue::Null
        },
        NormalRouteTestLane::Shadow => {
            unreachable!("warm benchmark lane was validated before fixture setup")
        },
    };

    let cleaned_up_instances = cleanup_static_hermes_test_instances::<ProdRuntime>().await?;
    let rss_after_cleanup = normal_route_linux_process_rss_bytes()?;
    match expectation.lane {
        NormalRouteTestLane::Wasm => anyhow::ensure!(
            cleaned_up_instances == 1
                && hooks.teardowns() == 1
                && hooks.store_drops() == instance_ids.len(),
            "warm benchmark cleanup did not destroy the retained Wasm runtime: \
             cleaned_up_instances={cleaned_up_instances}, teardowns={}, store_drops={}",
            hooks.teardowns(),
            hooks.store_drops(),
        ),
        NormalRouteTestLane::V8 => anyhow::ensure!(
            cleaned_up_instances == 0 && hooks.teardowns() == 0 && hooks.store_drops() == 0,
            "V8 warm benchmark unexpectedly retained a generated runtime"
        ),
        NormalRouteTestLane::Shadow => unreachable!(),
    }
    let rss_delta = i64::try_from(rss_after_measured_returns)
        .context("warm benchmark RSS is out of range")?
        .checked_sub(i64::try_from(rss_before_prime).context("warm benchmark RSS is out of range")?)
        .context("warm benchmark RSS delta overflow")?;
    let throughput_per_second =
        benchmark.iterations as f64 * 1_000_000_000_f64 / total_nanoseconds as f64;
    write_normal_route_observation(
        &benchmark.report_path,
        &json!({
            "kind": "convex-wasm-normal-route-warm-benchmark-v1",
            "schemaVersion": 1,
            "lane": expectation.lane,
            "deploymentSha256": &expectation.deployment_sha256,
            "generationSha256": &expectation.generation_sha256,
            "packageKey": &expectation.package_key,
            "route": &expectation.route,
            "coldPrimeInvocationCount": 1,
            "measuredInvocationCount": benchmark.iterations,
            "correctResultCount": benchmark.iterations,
            "timing": {
                "unit": "nanoseconds",
                "percentileConvention": "nearest-rank: sorted[ceil(p*N/100)-1]",
                "min": sorted_nanoseconds[0],
                "p50": normal_route_warm_benchmark_percentile(&sorted_nanoseconds, 50)?,
                "p90": normal_route_warm_benchmark_percentile(&sorted_nanoseconds, 90)?,
                "p99": normal_route_warm_benchmark_percentile(&sorted_nanoseconds, 99)?,
                "max": *sorted_nanoseconds.last().expect("nonempty benchmark timings"),
                "total": total_nanoseconds,
                "throughputPerSecond": throughput_per_second,
            },
            "reuse": {
                "generatedRuntimeInvocationCount": runtime_invocations.len(),
                "uniqueRuntimeCount": unique_runtime_count,
                "uniqueMemorySlotCount": unique_memory_slot_count,
                "poolMisses": pool_misses,
                "poolHits": pool_hits,
            },
            "generatedMemoryController": memory_controller,
            "linuxProcessRssBytes": {
                "beforePrime": rss_before_prime,
                "afterPrimeReturn": rss_after_prime_return,
                "afterMeasuredReturns": rss_after_measured_returns,
                "afterCleanup": rss_after_cleanup,
                "deltaBeforePrimeToAfterMeasuredReturns": rss_delta,
                "scope": "whole Linux process; not attributed to Wasm memory",
            },
        }),
    )?;
    Ok(())
}

async fn run_normal_registry_route_test(
    rt: ProdRuntime,
    expectation: NormalRouteTestExpectation,
) -> anyhow::Result<()> {
    run_normal_registry_route_test_with_paired_options(
        rt,
        expectation,
        NormalRoutePairedLaneExecutionOptions::ONE_CASE,
        None,
        None,
        None,
    )
    .await
}

async fn run_normal_registry_route_test_with_paired_options(
    rt: ProdRuntime,
    expectation: NormalRouteTestExpectation,
    paired_options: NormalRoutePairedLaneExecutionOptions,
    paired_hooks: Option<Arc<StaticHermesGateTestHooks>>,
    shared_source_package: Option<&NormalRouteSharedSourcePackage>,
    shared_source_bundle: Option<&NormalRouteSharedSourceBundle>,
) -> anyhow::Result<()> {
    let warm_benchmark = normal_route_warm_benchmark(&expectation)?;
    if matches!(expectation.lane, NormalRouteTestLane::Shadow) {
        anyhow::ensure!(
            expectation.route.udf_kind == "query"
                && matches!(
                    &expectation.setup,
                    NormalRouteTestSetup::ApiKeyValidation { .. }
                ),
            "query-shadow normal-route test requires the API-key validation query fixture"
        );
    }
    let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
    let database = new_database(rt.clone(), Arc::clone(&persistence)).await?;
    let storage: Arc<dyn Storage> = match shared_source_package {
        Some(shared_source_package) => Arc::clone(&shared_source_package.storage),
        None => Arc::new(LocalDirStorage::new(rt.clone())?),
    };
    let modules = match shared_source_bundle {
        Some(shared_source_bundle) => {
            normal_route_shared_source_bundle_is_authenticated(&expectation, shared_source_bundle)?;
            Arc::clone(&shared_source_bundle.modules)
        },
        None => Arc::new(normal_route_source_modules(&expectation)?),
    };
    let source_package = match shared_source_package {
        Some(shared_source_package) => {
            normal_route_shared_source_package_is_authenticated(
                &expectation,
                &shared_source_package.source_package,
            )?;
            shared_source_package.source_package.clone()
        },
        None => upload_normal_route_source_package(&expectation, &storage).await?,
    };
    let runner = new_runner(
        rt.clone(),
        database.clone(),
        Arc::clone(&persistence),
        Arc::clone(&storage),
        Arc::new(NormalRouteModuleLoader {
            modules: Arc::clone(&modules),
        }),
        // Every requested invocation must enter the function runner so the
        // structural observation measures retained-runtime reuse. Queries with
        // empty journals would otherwise remain in the application query cache.
        QueryCache::new(0),
        normal_route_test_key_broker(&expectation.deployment_sha256)?,
    )
    .await?;
    let paired_lane_schema = match &expectation.setup {
        NormalRouteTestSetup::PairedLaneV1 { .. } => Some(
            normal_route_authenticated_schema(&runner, &expectation, &storage, &source_package)
                .await?,
        ),
        _ => None,
    };
    let fixture = install_normal_route_fixture(
        &database,
        &expectation,
        paired_lane_schema.as_deref(),
        &modules,
        source_package,
        &storage,
    )
    .await?;
    if let NormalRouteFixtureState::PairedLaneV1(fixture) = fixture {
        anyhow::ensure!(
            !matches!(expectation.lane, NormalRouteTestLane::Shadow),
            "query-shadow normal-route test does not support paired-lane fixtures"
        );
        let observation_path = expectation.observation_path.clone();
        let install_hooks = paired_hooks.is_none();
        let hooks = paired_hooks.unwrap_or_else(|| StaticHermesGateTestHooks::new(0, 0));
        let hooks_guard = install_hooks
            .then(|| install_static_hermes_gate_test_hooks(&hooks))
            .transpose()?;
        let observation = run_normal_registry_paired_lane_test(
            runner,
            database,
            expectation,
            fixture,
            paired_options,
            hooks,
        )
        .await?;
        write_normal_route_observation(&observation_path, &observation)?;
        drop(hooks_guard);
        return Ok(());
    }
    if let NormalRouteTestSetup::ApiKeyValidation {
        application_table, ..
    } = &expectation.setup
    {
        let table: TableName = application_table.parse()?;
        anyhow::ensure!(
            table_count(&database, &table).await? == 2,
            "API-key validation fixture must contain exactly two application rows"
        );
    }
    if let NormalRouteTestSetup::CollectDelete { result_fields, .. } = &expectation.setup {
        for table in result_fields.values() {
            let table: TableName = table.parse()?;
            anyhow::ensure!(
                table_count(&database, &table).await? == 0,
                "collect-delete fixture table must be empty before warmup"
            );
        }
    }
    if let NormalRouteFixtureState::EmptyIndexedQueryV1(fixture) = &fixture {
        anyhow::ensure!(
            table_count(&database, &fixture.application_table).await? == 0,
            "empty indexed query fixture table must be empty before invocation"
        );
    }
    if let NormalRouteFixtureState::FileStorageGetUrlV1(fixture) = &fixture {
        verify_normal_route_file_storage_fixture(&database, &storage, fixture).await?;
    }
    let function_log = runner.function_log.clone();
    let module_name = expectation
        .route
        .runtime_module_path
        .strip_suffix(".js")
        .context("normal route runtime module path must end in .js")?;
    let path = PublicFunctionPath::RootExport(
        format!("{module_name}:{}", expectation.route.export_name).parse()?,
    );
    let identity = match &fixture {
        NormalRouteFixtureState::ApplicationProbe { user_id } => {
            Identity::user(normal_route_user_identity(user_id.encode())?)
        },
        NormalRouteFixtureState::ApplicationProbePatch
        | NormalRouteFixtureState::ApiKeyValidation { .. }
        | NormalRouteFixtureState::CollectDelete { .. }
        | NormalRouteFixtureState::EmptyIndexedQueryV1(_)
        | NormalRouteFixtureState::SeededDocumentsQueryV1(_)
        | NormalRouteFixtureState::FileStorageGetUrlV1(_)
        | NormalRouteFixtureState::IndexedPaginationQueryV1(_)
        | NormalRouteFixtureState::ObjectPatchDeleteV1(_)
        | NormalRouteFixtureState::SeededDocumentsMutationV1(_)
        | NormalRouteFixtureState::StatefulQueryPatchV1(_) => Identity::system(),
        NormalRouteFixtureState::PairedLaneV1(_) => {
            unreachable!("paired-lane fixtures return before legacy route execution")
        },
    };
    let hooks = StaticHermesGateTestHooks::new(0, 0);
    configure_normal_route_execution_fuel(&hooks)?;
    configure_normal_route_generated_runtime_execution_time(expectation.lane, &hooks)?;
    let hooks_guard = install_static_hermes_gate_test_hooks(&hooks)?;
    if let Some(warm_benchmark) = &warm_benchmark {
        let benchmark_result = run_normal_route_warm_benchmark(
            &runner,
            &database,
            path,
            identity,
            &expectation,
            &fixture,
            &hooks,
            warm_benchmark,
        )
        .await;
        drop(hooks_guard);
        let shutdown_result = database.shutdown().await;
        benchmark_result?;
        shutdown_result?;
        return Ok(());
    }
    let mut query_journals = Vec::new();
    let mut normalized_results = Vec::new();
    let (warmup_invocation_count, invocation_count) = match &fixture {
        NormalRouteFixtureState::ApplicationProbe { .. } => (0, 2),
        NormalRouteFixtureState::ApplicationProbePatch => (1, 2),
        NormalRouteFixtureState::ApiKeyValidation { .. } => (
            0,
            if matches!(expectation.lane, NormalRouteTestLane::Shadow) {
                2
            } else {
                3
            },
        ),
        NormalRouteFixtureState::CollectDelete { .. } => (1, 2),
        NormalRouteFixtureState::EmptyIndexedQueryV1(_) => (0, 2),
        NormalRouteFixtureState::SeededDocumentsQueryV1(_) => (0, 2),
        NormalRouteFixtureState::FileStorageGetUrlV1(_) => (0, 2),
        NormalRouteFixtureState::IndexedPaginationQueryV1(fixture) => {
            (fixture.warmup_invocation_count, fixture.invocations.len())
        },
        NormalRouteFixtureState::ObjectPatchDeleteV1(_) => (1, 2),
        NormalRouteFixtureState::SeededDocumentsMutationV1(_) => (0, 1),
        NormalRouteFixtureState::PairedLaneV1(_) => {
            unreachable!("paired-lane fixtures return before legacy route execution")
        },
        NormalRouteFixtureState::StatefulQueryPatchV1(fixture) => {
            (fixture.warmup_invocation_count, fixture.invocations.len())
        },
    };
    let total_invocation_count = warmup_invocation_count + invocation_count;
    let mut measured_pool_hits_baseline = 0;
    let mut measured_pool_misses_baseline = 0;
    let patch_application_setup = match &expectation.setup {
        NormalRouteTestSetup::ApplicationProbePatch {
            application_table,
            scheduled_function_address,
            ..
        } => Some((
            application_table.parse::<TableName>()?,
            scheduled_function_address.as_str(),
        )),
        _ => None,
    };
    let mut patch_state_identity = None;
    let mut collect_delete_seeded_document_ids = Vec::new();
    let mut normalized_committed_state = None;
    let mut captured_pagination_cursor = None;
    for execution_index in 0..total_invocation_count {
        if execution_index >= warmup_invocation_count {
            if let NormalRouteFixtureState::ObjectPatchDeleteV1(fixture) = &fixture {
                restore_normal_route_object_patch_delete_fields(&database, fixture).await?;
            }
        }
        let args = match &fixture {
            NormalRouteFixtureState::ApplicationProbe { .. }
            | NormalRouteFixtureState::ApplicationProbePatch
            | NormalRouteFixtureState::CollectDelete { .. } => json!({}),
            NormalRouteFixtureState::EmptyIndexedQueryV1(fixture) => {
                fixture.invocation_arguments.clone()
            },
            NormalRouteFixtureState::SeededDocumentsQueryV1(fixture) => {
                fixture.invocation_arguments.clone()
            },
            NormalRouteFixtureState::FileStorageGetUrlV1(fixture) => {
                fixture.invocation_arguments.clone()
            },
            NormalRouteFixtureState::IndexedPaginationQueryV1(fixture) => {
                let invocation = fixture
                    .invocations
                    .get(execution_index)
                    .context("indexed pagination invocation arguments are unavailable")?;
                resolve_normal_route_pagination_cursor_argument(
                    &invocation.arguments,
                    captured_pagination_cursor.as_deref(),
                )?
            },
            NormalRouteFixtureState::ObjectPatchDeleteV1(fixture) => {
                fixture.invocation_arguments.clone()
            },
            NormalRouteFixtureState::SeededDocumentsMutationV1(fixture) => {
                fixture.invocation_arguments.clone()
            },
            NormalRouteFixtureState::PairedLaneV1(_) => {
                unreachable!("paired-lane fixtures return before legacy route execution")
            },
            NormalRouteFixtureState::StatefulQueryPatchV1(fixture) => fixture
                .invocations
                .get(execution_index)
                .context("stateful query patch invocation arguments are unavailable")?
                .arguments
                .clone(),
            NormalRouteFixtureState::ApiKeyValidation {
                first_api_key,
                second_api_key,
                ..
            } => json!({
                "apiKey": if execution_index % 2 == 0 {
                    first_api_key
                } else {
                    second_api_key
                }
            }),
        };
        let capability_stage_offset = hooks
            .guest_native_completion_stages()
            .into_iter()
            .filter(|stage| stage.starts_with("start:"))
            .count();
        let (result, journal) = run_normal_route_invocation(
            &runner,
            &database,
            path.clone(),
            identity.clone(),
            &expectation.route.udf_kind,
            args,
        )
        .await?;
        if matches!(expectation.lane, NormalRouteTestLane::Shadow)
            && execution_index + 1 < total_invocation_count
        {
            let expected_completed_comparison_count = u64::try_from(execution_index + 1)
                .context("query-shadow invocation count exceeds u64")?;
            let query_shadow = wait_for_normal_route_shadow_observation(
                &function_log,
                &expectation,
                &hooks,
                UdfType::Query,
                expected_completed_comparison_count,
            )
            .await?;
            anyhow::ensure!(
                query_shadow.completed_comparison_count == expected_completed_comparison_count
                    && query_shadow.terminal_count == 0,
                "query-shadow route did not finish its prior comparison before the next invocation"
            );
        }
        if let (NormalRouteTestLane::Wasm, NormalRouteFixtureState::StatefulQueryPatchV1(fixture)) =
            (&expectation.lane, &fixture)
        {
            let expected_stages = &fixture
                .invocations
                .get(execution_index)
                .context("stateful query patch capability expectation is unavailable")?
                .expected_capability_stages;
            let actual_stages = hooks
                .guest_native_completion_stages()
                .into_iter()
                .filter(|stage| stage.starts_with("start:"))
                .skip(capability_stage_offset)
                .collect::<Vec<_>>();
            anyhow::ensure!(
                actual_stages
                    == expected_stages
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                "stateful query patch invocation {execution_index} capability stages differ from \
                 their exact expectation"
            );
        }
        if matches!(
            (&expectation.lane, &fixture),
            (
                NormalRouteTestLane::Wasm,
                NormalRouteFixtureState::IndexedPaginationQueryV1(_)
            )
        ) {
            let actual_stages = hooks
                .guest_native_completion_stages()
                .into_iter()
                .filter(|stage| stage.starts_with("start:"))
                .skip(capability_stage_offset)
                .collect::<Vec<_>>();
            anyhow::ensure!(
                actual_stages == ["start:db-query"],
                "indexed pagination invocation {execution_index} did not issue exactly one \
                 database query: {actual_stages:?}"
            );
        }
        if execution_index >= warmup_invocation_count {
            let invocation_index = execution_index - warmup_invocation_count;
            query_journals.push(journal);
            normalized_results.push(normalize_normal_route_result(
                &result,
                &fixture,
                invocation_index,
                &mut captured_pagination_cursor,
            )?);
        } else if matches!(&fixture, NormalRouteFixtureState::CollectDelete { .. }) {
            normalize_normal_route_result(&result, &fixture, 1, &mut None)?;
        } else if matches!(&fixture, NormalRouteFixtureState::ObjectPatchDeleteV1(_)) {
            normalize_normal_route_result(&result, &fixture, 0, &mut None)?;
        }
        if execution_index == 0 {
            if let NormalRouteFixtureState::ApplicationProbe { user_id } = &fixture {
                invalidate_normal_route_query_cache(&database, user_id.clone()).await?;
            }
        }
        if execution_index + 1 == warmup_invocation_count {
            measured_pool_hits_baseline = hooks.generated_pool_hits();
            measured_pool_misses_baseline = hooks.generated_pool_misses();
            if let NormalRouteFixtureState::CollectDelete {
                result_fields,
                synthetic_rows,
                ..
            } = &fixture
            {
                collect_delete_seeded_document_ids =
                    seed_normal_route_collect_delete_rows(&database, result_fields, synthetic_rows)
                        .await?;
            }
        }
        if let Some((table, scheduled_function_address)) = &patch_application_setup {
            let state = normal_route_application_probe_patch_state(
                &database,
                table,
                scheduled_function_address,
            )
            .await?;
            if let Some((owner_id, scheduled_function_id)) = &patch_state_identity {
                anyhow::ensure!(
                    owner_id == &state.owner_id
                        && scheduled_function_id == &state.scheduled_function_id,
                    "application patch route changed singleton or scheduled-function ownership \
                     across retained-runtime invocations"
                );
            } else {
                patch_state_identity =
                    Some((state.owner_id.clone(), state.scheduled_function_id.clone()));
            }
            normalized_committed_state = Some(state.normalized);
        }
        if let NormalRouteFixtureState::ObjectPatchDeleteV1(fixture) = &fixture {
            normalized_committed_state =
                Some(normal_route_object_patch_delete_post_state(&database, fixture).await?);
        }
        if execution_index + 1 == total_invocation_count {
            if let NormalRouteFixtureState::EmptyIndexedQueryV1(fixture) = &fixture {
                let committed_state = normal_route_empty_database_post_state(
                    &database,
                    &[fixture.application_table.clone()],
                )
                .await?;
                anyhow::ensure!(
                    committed_state == fixture.normalized_committed_state,
                    "empty indexed query fixture committed state changed"
                );
                normalized_committed_state = Some(committed_state);
            }
            if let NormalRouteFixtureState::IndexedPaginationQueryV1(fixture) = &fixture {
                normalized_committed_state =
                    Some(normal_route_indexed_pagination_post_state(&database, fixture).await?);
            }
            if let NormalRouteFixtureState::StatefulQueryPatchV1(fixture) = &fixture {
                normalized_committed_state =
                    Some(normal_route_stateful_query_patch_post_state(&database, fixture).await?);
            }
        }
        if execution_index >= warmup_invocation_count {
            if let NormalRouteFixtureState::CollectDelete { result_fields, .. } = &fixture {
                normalized_committed_state = Some(
                    normal_route_collect_delete_post_state(
                        &database,
                        result_fields,
                        &collect_delete_seeded_document_ids,
                    )
                    .await?,
                );
            }
        }
    }
    anyhow::ensure!(
        normalized_committed_state.is_some()
            == matches!(
                &fixture,
                NormalRouteFixtureState::ApplicationProbePatch
                    | NormalRouteFixtureState::CollectDelete { .. }
                    | NormalRouteFixtureState::EmptyIndexedQueryV1(_)
                    | NormalRouteFixtureState::IndexedPaginationQueryV1(_)
                    | NormalRouteFixtureState::ObjectPatchDeleteV1(_)
                    | NormalRouteFixtureState::StatefulQueryPatchV1(_)
            ),
        "normal route committed-state observation differs from its fixture"
    );
    if matches!(
        &fixture,
        NormalRouteFixtureState::IndexedPaginationQueryV1(_)
    ) {
        anyhow::ensure!(
            captured_pagination_cursor.is_some()
                && query_journals.len() == 2
                && query_journals.iter().all(|journal| {
                    !matches!(journal, NormalRouteQueryJournalObservation::None)
                }),
            "indexed pagination route did not preserve its raw cursor handoff and two complete \
             query journals"
        );
    }

    let mut provenance_rejections = 0;
    if matches!(expectation.lane, NormalRouteTestLane::Wasm) {
        let runtime_observations_before = hooks.generated_runtime_ids().len();
        let pool_hits_before = hooks.generated_pool_hits();
        let pool_misses_before = hooks.generated_pool_misses();
        install_normal_route_provenance_mismatch(&database).await?;
        let error = run_normal_route_invocation(
            &runner,
            &database,
            path.clone(),
            identity.clone(),
            &expectation.route.udf_kind,
            json!({}),
        )
        .await
        .expect_err("source-package provenance mismatch unexpectedly executed");
        anyhow::ensure!(
            error.to_string().contains("source package")
                && error
                    .to_string()
                    .contains("identity does not match the active transaction"),
            "normal route provenance mismatch failed at an unexpected boundary: {error:#}"
        );
        anyhow::ensure!(
            hooks.generated_runtime_ids().len() == runtime_observations_before
                && hooks.generated_pool_hits() == pool_hits_before
                && hooks.generated_pool_misses() == pool_misses_before,
            "normal route provenance rejection reached runtime admission"
        );
        provenance_rejections = 1;
    }
    if let NormalRouteFixtureState::FileStorageGetUrlV1(fixture) = &fixture {
        verify_normal_route_file_storage_fixture(&database, &storage, fixture).await?;
    }

    let query_shadow = if matches!(expectation.lane, NormalRouteTestLane::Shadow) {
        Some(
            wait_for_normal_route_shadow_observation(
                &function_log,
                &expectation,
                &hooks,
                UdfType::Query,
                u64::try_from(total_invocation_count)
                    .context("query-shadow invocation count exceeds u64")?,
            )
            .await?,
        )
    } else {
        None
    };

    let runtime_ids = hooks.generated_runtime_ids();
    let instance_ids = hooks.instance_ids();
    let runtime_invocations = hooks.generated_invocations();
    let pool_hits = hooks.generated_pool_hits();
    let pool_misses = hooks.generated_pool_misses();
    let measured_pool_hits = pool_hits - measured_pool_hits_baseline;
    let measured_pool_misses = pool_misses - measured_pool_misses_baseline;
    let measured_fresh_runtime_admissions = runtime_invocations
        .iter()
        .skip(warmup_invocation_count)
        .filter(|observation| observation.runtime_was_created)
        .count();
    let capability_operation_stages = hooks
        .guest_native_completion_stages()
        .into_iter()
        .filter(|stage| stage.starts_with("start:"))
        .collect::<Vec<_>>();
    let database_query_starts = hooks.database_query_starts();
    let reads_completed = hooks.read_completed();
    let reads_cancelled = hooks.read_cancelled();
    let read_documents = hooks.read_documents();
    let read_bytes = hooks.read_bytes();
    let read_intervals = hooks.read_intervals();
    let transaction_drops = hooks.transaction_drops();
    let execution_errors = hooks.execution_errors();
    match expectation.lane {
        NormalRouteTestLane::Wasm => {
            let unique_invocation_ids = runtime_invocations
                .iter()
                .map(|observation| observation.invocation_id)
                .collect::<BTreeSet<_>>();
            let unique_capability_identities = runtime_invocations
                .iter()
                .map(|observation| observation.capability_identity)
                .collect::<BTreeSet<_>>();
            let unique_memory_slot_count = runtime_invocations
                .iter()
                .map(|observation| observation.memory_slot_id)
                .collect::<BTreeSet<_>>()
                .len();
            anyhow::ensure!(
                runtime_ids.len() == total_invocation_count
                    && runtime_invocations.len() == total_invocation_count
                    && runtime_ids
                        .iter()
                        .all(|runtime_id| runtime_id == &runtime_ids[0])
                    && unique_memory_slot_count == 1
                    && unique_invocation_ids.len() == total_invocation_count
                    && unique_capability_identities.len() == total_invocation_count
                    && runtime_invocations[0].runtime_was_created
                    && runtime_invocations
                        .iter()
                        .skip(1)
                        .all(|observation| !observation.runtime_was_created)
                    && runtime_invocations.iter().all(|observation| {
                        observation.deployment_sha256.as_deref()
                            == Some(expectation.deployment_sha256.as_str())
                            && observation.package_key == expectation.package_key
                            && observation.entry_id.as_deref()
                                == Some(expectation.routing.entry_id.as_str())
                            && observation.entry_selector_id.as_deref()
                                == Some(expectation.routing.entry_selector_id.as_str())
                            && observation.capability_revoked
                            && observation.forged_capability_rejected
                            && observation.revoked_capability_rejected
                            && observation.opaque_live_handles == 0
                            && observation.opaque_current_bytes == 0
                            && !observation.runtime_reuse_contaminated
                    })
                    && runtime_invocations
                        .iter()
                        .skip(1)
                        .all(|observation| observation.prior_capability_rejected)
                    && pool_misses == 1
                    && pool_hits + pool_misses == total_invocation_count,
                "normal registry route did not preserve one runtime with fresh clean invocation \
                 authority: runtimes={runtime_ids:?}, invocations={runtime_invocations:?}, \
                 pool_hits={pool_hits}, pool_misses={pool_misses}"
            );
            if matches!(&fixture, NormalRouteFixtureState::ApplicationProbePatch) {
                let patch_operations = capability_operation_stages
                    .iter()
                    .filter(|stage| **stage == "start:db-patch")
                    .count();
                let insert_operations = capability_operation_stages
                    .iter()
                    .filter(|stage| **stage == "start:db-insert")
                    .count();
                let schedule_operations = capability_operation_stages
                    .iter()
                    .filter(|stage| **stage == "start:scheduler-run-after")
                    .count();
                anyhow::ensure!(
                    measured_pool_hits == invocation_count
                        && measured_pool_misses == 0
                        && measured_fresh_runtime_admissions == 0
                        && patch_operations == invocation_count
                        && insert_operations == warmup_invocation_count
                        && schedule_operations == warmup_invocation_count,
                    "application patch route did not execute only on retained-runtime admissions: \
                     measured_pool_hits={measured_pool_hits}, \
                     measured_pool_misses={measured_pool_misses}, \
                     measured_fresh_runtime_admissions={measured_fresh_runtime_admissions}, \
                     stages={capability_operation_stages:?}"
                );
            }
            if let NormalRouteFixtureState::CollectDelete {
                result_fields,
                synthetic_rows,
                ..
            } = &fixture
            {
                let query_operations = capability_operation_stages
                    .iter()
                    .filter(|stage| **stage == "start:db-query")
                    .count();
                let delete_operations = capability_operation_stages
                    .iter()
                    .filter(|stage| **stage == "start:db-delete")
                    .count();
                let expected_query_operations = result_fields.len() * total_invocation_count;
                let expected_delete_operations = result_fields.len() * synthetic_rows.len();
                anyhow::ensure!(
                    measured_pool_hits == invocation_count
                        && measured_pool_misses == 0
                        && measured_fresh_runtime_admissions == 0
                        && query_operations == expected_query_operations
                        && delete_operations == expected_delete_operations
                        && capability_operation_stages.len()
                            == expected_query_operations + expected_delete_operations,
                    concat!(
                        "collect-delete route did not preserve retained-runtime admission ",
                        "or exact generic operation counts: ",
                        "measured_pool_hits={measured_pool_hits}, ",
                        "measured_pool_misses={measured_pool_misses}, ",
                        "measured_fresh_runtime_admissions=",
                        "{measured_fresh_runtime_admissions}, ",
                        "stages={capability_operation_stages:?}"
                    )
                );
            }
            if matches!(&fixture, NormalRouteFixtureState::FileStorageGetUrlV1(_)) {
                anyhow::ensure!(
                    measured_pool_hits == invocation_count - 1
                        && measured_pool_misses == 1
                        && measured_fresh_runtime_admissions == 1
                        && capability_operation_stages
                            == vec!["start:storage-get-url"; invocation_count],
                    concat!(
                        "file-storage route did not preserve exact retained-runtime ",
                        "admission or capability operations: ",
                        "measured_pool_hits={measured_pool_hits}, ",
                        "measured_pool_misses={measured_pool_misses}, ",
                        "measured_fresh_runtime_admissions=",
                        "{measured_fresh_runtime_admissions}, ",
                        "stages={capability_operation_stages:?}"
                    )
                );
            }
            if matches!(
                &fixture,
                NormalRouteFixtureState::EmptyIndexedQueryV1(_)
                    | NormalRouteFixtureState::SeededDocumentsQueryV1(_)
                    | NormalRouteFixtureState::IndexedPaginationQueryV1(_)
            ) {
                anyhow::ensure!(
                    measured_pool_hits == invocation_count - 1
                        && measured_pool_misses == 1
                        && measured_fresh_runtime_admissions == 1
                        && capability_operation_stages == vec!["start:db-query"; invocation_count],
                    concat!(
                        "indexed query route did not preserve exact retained-runtime ",
                        "admission or read-only capability operations: ",
                        "measured_pool_hits={measured_pool_hits}, ",
                        "measured_pool_misses={measured_pool_misses}, ",
                        "measured_fresh_runtime_admissions=",
                        "{measured_fresh_runtime_admissions}, ",
                        "stages={capability_operation_stages:?}"
                    )
                );
            }
            if matches!(&fixture, NormalRouteFixtureState::ObjectPatchDeleteV1(_)) {
                let patch_operations = capability_operation_stages
                    .iter()
                    .filter(|stage| **stage == "start:db-patch")
                    .count();
                anyhow::ensure!(
                    measured_pool_hits == invocation_count
                        && measured_pool_misses == 0
                        && measured_fresh_runtime_admissions == 0
                        && patch_operations == total_invocation_count
                        && capability_operation_stages.len() == total_invocation_count,
                    concat!(
                        "object patch route did not preserve retained-runtime admission ",
                        "or exact patch operation count: ",
                        "measured_pool_hits={measured_pool_hits}, ",
                        "measured_pool_misses={measured_pool_misses}, ",
                        "measured_fresh_runtime_admissions=",
                        "{measured_fresh_runtime_admissions}, ",
                        "stages={capability_operation_stages:?}"
                    )
                );
            }
            if let NormalRouteFixtureState::StatefulQueryPatchV1(fixture) = &fixture {
                let expected_capability_operation_stages = fixture
                    .invocations
                    .iter()
                    .flat_map(|invocation| {
                        invocation
                            .expected_capability_stages
                            .iter()
                            .map(String::as_str)
                    })
                    .collect::<Vec<_>>();
                anyhow::ensure!(
                    measured_pool_hits == invocation_count - 1
                        && measured_pool_misses == 1
                        && measured_fresh_runtime_admissions == 1
                        && capability_operation_stages == expected_capability_operation_stages,
                    concat!(
                        "stateful query patch route did not preserve exact retained-runtime ",
                        "admission or per-invocation effects: ",
                        "measured_pool_hits={measured_pool_hits}, ",
                        "measured_pool_misses={measured_pool_misses}, ",
                        "measured_fresh_runtime_admissions=",
                        "{measured_fresh_runtime_admissions}, ",
                        "stages={capability_operation_stages:?}"
                    )
                );
            }
            let read_accounting_matches_fixture =
                if matches!(&fixture, NormalRouteFixtureState::FileStorageGetUrlV1(_)) {
                    true
                } else if matches!(
                    &fixture,
                    NormalRouteFixtureState::IndexedPaginationQueryV1(_)
                ) {
                    database_query_starts == invocation_count
                        && reads_completed >= database_query_starts
                        && read_documents > 0
                        && read_bytes > 0
                        && read_intervals > 0
                } else if matches!(&fixture, NormalRouteFixtureState::EmptyIndexedQueryV1(_)) {
                    // queryPage completes a read without opening a query stream.
                    (database_query_starts == invocation_count
                        || (database_query_starts == 0 && reads_completed == invocation_count))
                        && reads_completed >= invocation_count
                        && read_documents == 0
                        && read_bytes == 0
                } else if matches!(&fixture, NormalRouteFixtureState::StatefulQueryPatchV1(_)) {
                    database_query_starts > 0
                        && reads_completed >= database_query_starts
                        && read_documents > 0
                        && read_bytes > 0
                        && read_intervals > 0
                } else if matches!(&fixture, NormalRouteFixtureState::ObjectPatchDeleteV1(_)) {
                    database_query_starts == 0
                        && reads_completed == total_invocation_count
                        && read_documents == total_invocation_count
                        && read_bytes > 0
                        && read_intervals > 0
                } else if matches!(
                    &fixture,
                    NormalRouteFixtureState::SeededDocumentsMutationV1(_)
                ) {
                    match &fixture {
                        NormalRouteFixtureState::SeededDocumentsMutationV1(fixture)
                            if matches!(
                                fixture.expected_read_accounting,
                                Some(NormalRouteExpectedReadAccounting::None)
                            ) =>
                        {
                            database_query_starts == 0
                                && reads_completed > 0
                                && read_documents == 0
                                && read_bytes == 0
                                && read_intervals > 0
                        },
                        NormalRouteFixtureState::SeededDocumentsMutationV1(_) => {
                            reads_completed > 0
                                && reads_completed >= database_query_starts
                                && read_documents > 0
                                && read_bytes > 0
                                && read_intervals > 0
                        },
                        _ => unreachable!("seeded mutation read accounting branch lost fixture"),
                    }
                } else if matches!(&fixture, NormalRouteFixtureState::SeededDocumentsQueryV1(_)) {
                    // An indexed page whose predicate excludes every seeded row still records
                    // completed read intervals, while query-page accounting has no returned
                    // document payload to charge.
                    reads_completed > 0
                        && reads_completed >= database_query_starts
                        && read_intervals > 0
                        && ((read_documents > 0 && read_bytes > 0)
                            || (read_documents == 0 && read_bytes == 0))
                } else {
                    // One database query stream can require several completed async
                    // batches.
                    database_query_starts > 0
                        && reads_completed >= database_query_starts
                        && read_documents > 0
                        && read_bytes > 0
                        && read_intervals > 0
                };
            anyhow::ensure!(
                instance_ids.len() == total_invocation_count
                    && read_accounting_matches_fixture
                    && reads_cancelled == 0
                    && transaction_drops == 0
                    && execution_errors.is_empty(),
                "normal registry route did not complete cleanly: instances={instance_ids:?}, \
                 database_query_starts={database_query_starts}, \
                 reads_completed={reads_completed}, reads_cancelled={reads_cancelled}, \
                 read_documents={read_documents}, read_bytes={read_bytes}, \
                 read_intervals={read_intervals}, transaction_drops={transaction_drops}, \
                 execution_errors={execution_errors:?}"
            );
        },
        NormalRouteTestLane::V8 => {
            anyhow::ensure!(
                runtime_ids.is_empty()
                    && instance_ids.is_empty()
                    && runtime_invocations.is_empty()
                    && pool_hits == 0
                    && pool_misses == 0
                    && capability_operation_stages.is_empty()
                    && database_query_starts == 0
                    && reads_completed == 0
                    && reads_cancelled == 0
                    && read_documents == 0
                    && read_bytes == 0
                    && read_intervals == 0
                    && transaction_drops == 0
                    && execution_errors.is_empty(),
                "V8 control unexpectedly reached the generated runtime: \
                 instances={instance_ids:?}, database_query_starts={database_query_starts}, \
                 reads_completed={reads_completed}, reads_cancelled={reads_cancelled}, \
                 read_documents={read_documents}, read_bytes={read_bytes}, \
                 read_intervals={read_intervals}, transaction_drops={transaction_drops}, \
                 execution_errors={execution_errors:?}"
            );
        },
        NormalRouteTestLane::Shadow => {
            let query_shadow = query_shadow
                .as_ref()
                .context("query-shadow normal-route test omitted comparison evidence")?;
            let trace_diagnostics = function_runner::take_query_shadow_test_trace_diagnostics();
            anyhow::ensure!(
                query_shadow.generation_sha256 == expectation.generation_sha256
                    && query_shadow.route_id == expectation.routing.route_id
                    && !query_shadow.primary_wasm_routing_enabled
                    && query_shadow.attempt_count == 2
                    && query_shadow.admitted_count == 2
                    && query_shadow.completed_comparison_count == 2
                    && query_shadow.mismatch_count == 0
                    && query_shadow.terminal_count == 0,
                "query-shadow route did not retain two successful V8-primary/Wasm-shadow \
                 comparisons: generation_matches={}, route_matches={}, \
                 primary_wasm_routing_enabled={}, attempts={}, admitted={}, \
                 completed_comparisons={}, mismatches={}, mismatch_counts={:?}, terminals={}, \
                 trace_diagnostics={:?}",
                query_shadow.generation_sha256 == expectation.generation_sha256,
                query_shadow.route_id == expectation.routing.route_id,
                query_shadow.primary_wasm_routing_enabled,
                query_shadow.attempt_count,
                query_shadow.admitted_count,
                query_shadow.completed_comparison_count,
                query_shadow.mismatch_count,
                query_shadow.mismatch_counts,
                query_shadow.terminal_count,
                trace_diagnostics
            );
            anyhow::ensure!(
                runtime_ids.len() == 2
                    && instance_ids.len() == 2
                    && runtime_invocations.len() == 2
                    && runtime_invocations[0].runtime_was_created
                    && !runtime_invocations[1].runtime_was_created
                    && runtime_ids[0] == runtime_ids[1]
                    && runtime_invocations[0].memory_slot_id
                        == runtime_invocations[1].memory_slot_id
                    && runtime_invocations[1].prior_capability_rejected
                    && runtime_invocations.iter().all(|observation| {
                        observation.deployment_sha256.as_deref()
                            == Some(expectation.deployment_sha256.as_str())
                            && observation.package_key == expectation.package_key
                            && observation.entry_id.as_deref()
                                == Some(expectation.routing.entry_id.as_str())
                            && observation.entry_selector_id.as_deref()
                                == Some(expectation.routing.entry_selector_id.as_str())
                            && observation.capability_revoked
                            && observation.forged_capability_rejected
                            && observation.revoked_capability_rejected
                            && observation.opaque_live_handles == 0
                            && observation.opaque_current_bytes == 0
                            && !observation.runtime_reuse_contaminated
                    })
                    && pool_hits == 1
                    && pool_misses == 1
                    && capability_operation_stages
                        == vec!["start:db-query-stream", "start:db-query-stream"]
                    && database_query_starts == 2
                    && reads_completed >= database_query_starts
                    && read_documents > 0
                    && read_bytes > 0
                    && read_intervals > 0
                    && reads_cancelled == 0
                    && transaction_drops == 0
                    && execution_errors.is_empty(),
                "query-shadow route did not execute two authenticated guest-promise database \
                 query streams: runtimes={runtime_ids:?}, invocations={runtime_invocations:?}, \
                 stages={capability_operation_stages:?}, \
                 database_query_starts={database_query_starts}, \
                 reads_completed={reads_completed}, reads_cancelled={reads_cancelled}, \
                 read_documents={read_documents}, read_bytes={read_bytes}, \
                 read_intervals={read_intervals}, transaction_drops={transaction_drops}, \
                 execution_errors={execution_errors:?}"
            );
        },
    }
    let unique_runtime_count = runtime_ids.iter().copied().collect::<BTreeSet<_>>().len();
    let unique_memory_slot_count = runtime_invocations
        .iter()
        .map(|observation| observation.memory_slot_id)
        .collect::<BTreeSet<_>>()
        .len();
    let cleaned_up_instances = cleanup_static_hermes_test_instances::<ProdRuntime>().await?;
    let teardowns_after_cleanup = hooks.teardowns();
    let store_drops_after_cleanup = hooks.store_drops();
    match expectation.lane {
        // This hook counts per-invocation HostState drops, including the state replaced
        // when a retained Wasmtime Store receives its next invocation.
        NormalRouteTestLane::Wasm => anyhow::ensure!(
            cleaned_up_instances == 1
                && teardowns_after_cleanup == 1
                && store_drops_after_cleanup == instance_ids.len(),
            "normal registry route cleanup did not destroy its retained runtime: \
             cleaned_up_instances={cleaned_up_instances}, \
             teardowns_after_cleanup={teardowns_after_cleanup}, \
             store_drops_after_cleanup={store_drops_after_cleanup}"
        ),
        NormalRouteTestLane::V8 => anyhow::ensure!(
            cleaned_up_instances == 0
                && teardowns_after_cleanup == 0
                && store_drops_after_cleanup == 0,
            "V8 control unexpectedly retained a generated runtime: \
             cleaned_up_instances={cleaned_up_instances}, \
             teardowns_after_cleanup={teardowns_after_cleanup}, \
             store_drops_after_cleanup={store_drops_after_cleanup}"
        ),
        NormalRouteTestLane::Shadow => anyhow::ensure!(
            cleaned_up_instances == 1
                && teardowns_after_cleanup == 1
                && store_drops_after_cleanup == instance_ids.len(),
            "query-shadow route cleanup did not destroy its retained runtime: \
             cleaned_up_instances={cleaned_up_instances}, \
             teardowns_after_cleanup={teardowns_after_cleanup}, \
             store_drops_after_cleanup={store_drops_after_cleanup}"
        ),
    }
    let runtime_invocations = runtime_invocations
        .into_iter()
        .map(TryInto::try_into)
        .collect::<anyhow::Result<Vec<NormalRouteRuntimeInvocation>>>()?;
    let observation = NormalRouteStructuralObservation {
        kind: "convex-wasm-normal-route-structural-observation-v5",
        schema_version: 5,
        deployment_sha256: expectation.deployment_sha256,
        generation_sha256: expectation.generation_sha256,
        lane: expectation.lane,
        package_key: expectation.package_key,
        route: expectation.route,
        warmup_invocation_count,
        invocation_count: normalized_results.len(),
        query_journals,
        normalized_results,
        normalized_committed_state,
        database_query_starts,
        reads_completed,
        read_documents,
        read_bytes,
        read_intervals,
        runtime_observation_count: runtime_ids.len(),
        unique_runtime_count,
        unique_memory_slot_count,
        runtime_invocations,
        pool_hits,
        pool_misses,
        measured_pool_hits,
        measured_pool_misses,
        measured_fresh_runtime_admissions,
        capability_operation_stages,
        provenance_rejections,
        query_shadow,
        cleaned_up_instances,
        store_drops_after_cleanup,
        teardowns_after_cleanup,
    };
    write_normal_route_observation(&expectation.observation_path, &observation)?;
    drop(hooks_guard);
    database.shutdown().await?;
    Ok(())
}

fn normalize_paired_lane_value(
    value: &JsonValue,
    document_id_markers: &BTreeMap<String, JsonValue>,
) -> JsonValue {
    normalize_paired_lane_value_with_self(value, document_id_markers, None)
}

fn normalize_paired_lane_value_with_self(
    value: &JsonValue,
    document_id_markers: &BTreeMap<String, JsonValue>,
    self_id: Option<&str>,
) -> JsonValue {
    match value {
        JsonValue::Array(values) => JsonValue::Array(
            values
                .iter()
                .map(|value| {
                    normalize_paired_lane_value_with_self(value, document_id_markers, self_id)
                })
                .collect(),
        ),
        JsonValue::Object(fields) => JsonValue::Object(
            fields
                .iter()
                .map(|(field, value)| {
                    (
                        field.clone(),
                        normalize_paired_lane_value_with_self(value, document_id_markers, self_id),
                    )
                })
                .collect(),
        ),
        JsonValue::String(value) if self_id == Some(value) => {
            json!({ "$writeId": { "kind": "self" } })
        },
        JsonValue::String(value) => document_id_markers
            .get(value)
            .cloned()
            .unwrap_or_else(|| JsonValue::String(value.clone())),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => value.clone(),
    }
}

fn normalize_paired_lane_document(
    document: &JsonValue,
    document_id_markers: &BTreeMap<String, JsonValue>,
) -> JsonValue {
    normalize_paired_lane_document_with_self(document, document_id_markers, None)
}

fn normalize_paired_lane_document_with_self(
    document: &JsonValue,
    document_id_markers: &BTreeMap<String, JsonValue>,
    self_id: Option<&str>,
) -> JsonValue {
    // Only document roots have lane-local system metadata; nested values are user
    // data.
    let fields = document
        .as_object()
        .expect("paired-lane document is not an object");
    JsonValue::Object(
        fields
            .iter()
            .map(|(field, value)| {
                let normalized = if field == "_creationTime" {
                    json!({ "kind": "creation-time" })
                } else {
                    normalize_paired_lane_value_with_self(value, document_id_markers, self_id)
                };
                (field.clone(), normalized)
            })
            .collect(),
    )
}

fn normal_route_paired_lane_document_sha256(
    document: &JsonValue,
    document_id_markers: &BTreeMap<String, JsonValue>,
) -> anyhow::Result<String> {
    let normalized = normalize_paired_lane_document(document, document_id_markers);
    Ok(Sha256::hash(&normal_route_sequence_json_canonical_bytes(&normalized)?).as_hex())
}

struct NormalRoutePairedLaneRawWrite {
    id: String,
    new_document: Option<JsonValue>,
    old_document: Option<JsonValue>,
    table: String,
}

fn normal_route_paired_lane_write_marker_signature(
    write: &NormalRoutePairedLaneRawWrite,
    document_id_markers: &BTreeMap<String, JsonValue>,
) -> anyhow::Result<String> {
    let value = json!({
        "new": write.new_document.as_ref().map(|document| {
            normalize_paired_lane_document_with_self(
                document,
                document_id_markers,
                Some(write.id.as_str()),
            )
        }),
        "old": write.old_document.as_ref().map(|document| {
            normalize_paired_lane_document_with_self(
                document,
                document_id_markers,
                Some(write.id.as_str()),
            )
        }),
        "table": write.table,
    });
    Ok(Sha256::hash(&normal_route_sequence_json_canonical_bytes(&value)?).as_hex())
}

fn normal_route_paired_lane_write_id_markers(
    writes: &[NormalRoutePairedLaneRawWrite],
    document_id_markers: &mut BTreeMap<String, JsonValue>,
) -> anyhow::Result<()> {
    let generated_writes = writes
        .iter()
        .filter(|write| !document_id_markers.contains_key(&write.id))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        generated_writes
            .iter()
            .map(|write| write.id.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            == generated_writes.len(),
        "paired-lane transaction write set contains duplicate document IDs"
    );
    // Refining the generated-ID partition from the complete old/new document graph
    // keeps same-table IDs and their reference relationships distinct without
    // retaining a lane-local ID or raw document value in the emitted evidence.
    let mut classes = generated_writes
        .iter()
        .map(|write| (write.id.as_str(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    for _ in 0..=generated_writes.len() {
        for write in &generated_writes {
            document_id_markers.insert(
                write.id.clone(),
                json!({
                    "$writeId": {
                        "class": classes[write.id.as_str()],
                        "table": write.table,
                    },
                }),
            );
        }
        let mut grouped_signatures = BTreeMap::<usize, Vec<(String, &str)>>::new();
        for write in &generated_writes {
            grouped_signatures
                .entry(classes[write.id.as_str()])
                .or_default()
                .push((
                    normal_route_paired_lane_write_marker_signature(write, document_id_markers)?,
                    write.id.as_str(),
                ));
        }
        let mut next_classes = BTreeMap::new();
        let mut next_class = 0;
        for signatures in grouped_signatures.values_mut() {
            signatures.sort_unstable();
            let mut previous_signature = None;
            for (signature, id) in signatures {
                let signature = signature.as_str();
                if previous_signature.as_deref() != Some(signature) {
                    if previous_signature.is_some() {
                        next_class += 1;
                    }
                    previous_signature = Some(signature.to_owned());
                }
                next_classes.insert(*id, next_class);
            }
            next_class += 1;
        }
        let changed = next_classes != classes;
        classes = next_classes;
        if !changed {
            for write in &generated_writes {
                document_id_markers.insert(
                    write.id.clone(),
                    json!({
                        "$writeId": {
                            "class": normal_route_paired_lane_write_marker_signature(
                                write,
                                document_id_markers,
                            )?,
                            "table": write.table,
                        },
                    }),
                );
            }
            return Ok(());
        }
    }
    anyhow::bail!("paired-lane generated document-ID normalization did not converge")
}

fn normal_route_paired_lane_write_intent_from_documents(
    table: String,
    old_document: Option<&JsonValue>,
    new_document: Option<&JsonValue>,
    document_id_markers: &BTreeMap<String, JsonValue>,
) -> anyhow::Result<NormalRoutePairedLaneWriteIntent> {
    let (operation, old_document_sha256, new_document_sha256) = match (old_document, new_document) {
        (None, Some(new_document)) => (
            NormalRoutePairedLaneWriteOperation::Insert,
            None,
            Some(normal_route_paired_lane_document_sha256(
                new_document,
                document_id_markers,
            )?),
        ),
        (Some(old_document), None) => (
            NormalRoutePairedLaneWriteOperation::Delete,
            Some(normal_route_paired_lane_document_sha256(
                old_document,
                document_id_markers,
            )?),
            None,
        ),
        (Some(old_document), Some(new_document)) => (
            NormalRoutePairedLaneWriteOperation::Update,
            Some(normal_route_paired_lane_document_sha256(
                old_document,
                document_id_markers,
            )?),
            Some(normal_route_paired_lane_document_sha256(
                new_document,
                document_id_markers,
            )?),
        ),
        (None, None) => {
            anyhow::bail!("paired-lane transaction write omitted both the old and new document")
        },
    };
    Ok(NormalRoutePairedLaneWriteIntent {
        new_document_sha256,
        old_document_sha256,
        operation,
        table,
    })
}

fn normal_route_paired_lane_transaction_write_set(
    transaction: &mut Transaction<ProdRuntime>,
    document_id_markers: &mut BTreeMap<String, JsonValue>,
) -> anyhow::Result<Vec<NormalRoutePairedLaneWriteIntent>> {
    let table_mapping = transaction.table_mapping().clone();
    let updates = transaction
        .writes()
        .as_flat()?
        .coalesced_writes()
        .collect::<Vec<_>>();

    let writes = updates
        .into_iter()
        .map(|update| {
            let table = table_mapping
                .tablet_name(update.id().tablet_id)?
                .to_string();
            let old_document = update
                .old_document()
                .map(|(document, _)| document.to_internal_json());
            let new_document = update
                .new_document_internal_json()
                .map(serde_json::to_value)
                .transpose()?;
            Ok(NormalRoutePairedLaneRawWrite {
                id: update.id().developer_id.encode(),
                new_document,
                old_document,
                table,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    normal_route_paired_lane_write_id_markers(&writes, document_id_markers)?;
    let mut intents = writes
        .iter()
        .map(|write| {
            normal_route_paired_lane_write_intent_from_documents(
                write.table.clone(),
                write.old_document.as_ref(),
                write.new_document.as_ref(),
                document_id_markers,
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    intents.sort_by(|left, right| {
        (
            &left.table,
            &left.operation,
            &left.old_document_sha256,
            &left.new_document_sha256,
        )
            .cmp(&(
                &right.table,
                &right.operation,
                &right.old_document_sha256,
                &right.new_document_sha256,
            ))
    });
    Ok(intents)
}

fn normal_route_paired_lane_host_operation(
    operation: udf::LogicalHostOperation,
) -> anyhow::Result<NormalRoutePairedLaneHostOperation> {
    Ok(match operation {
        udf::LogicalHostOperation::AuditLog => NormalRoutePairedLaneHostOperation::AuditLog,
        udf::LogicalHostOperation::CancelJob => NormalRoutePairedLaneHostOperation::CancelJob,
        udf::LogicalHostOperation::ComponentArgument => {
            NormalRoutePairedLaneHostOperation::ComponentArgument
        },
        udf::LogicalHostOperation::CreateFunctionHandle => {
            NormalRoutePairedLaneHostOperation::CreateFunctionHandle
        },
        udf::LogicalHostOperation::DatabaseCount => {
            NormalRoutePairedLaneHostOperation::DatabaseCount
        },
        udf::LogicalHostOperation::DatabaseDelete => {
            NormalRoutePairedLaneHostOperation::DatabaseDelete
        },
        udf::LogicalHostOperation::DatabaseGet => NormalRoutePairedLaneHostOperation::DatabaseGet,
        udf::LogicalHostOperation::DatabaseInsert => {
            NormalRoutePairedLaneHostOperation::DatabaseInsert
        },
        udf::LogicalHostOperation::DatabaseNormalizeId => {
            NormalRoutePairedLaneHostOperation::DatabaseNormalizeId
        },
        udf::LogicalHostOperation::DatabasePatch => {
            NormalRoutePairedLaneHostOperation::DatabasePatch
        },
        udf::LogicalHostOperation::DatabaseQueryPage => {
            NormalRoutePairedLaneHostOperation::DatabaseQueryPage
        },
        udf::LogicalHostOperation::DatabaseQueryCleanup => {
            NormalRoutePairedLaneHostOperation::DatabaseQueryCleanup
        },
        udf::LogicalHostOperation::DatabaseQueryStream => {
            NormalRoutePairedLaneHostOperation::DatabaseQueryStream
        },
        udf::LogicalHostOperation::DatabaseQueryStreamNext => {
            NormalRoutePairedLaneHostOperation::DatabaseQueryStreamNext
        },
        udf::LogicalHostOperation::DatabaseReplace => {
            NormalRoutePairedLaneHostOperation::DatabaseReplace
        },
        udf::LogicalHostOperation::DeploymentMetadata => {
            NormalRoutePairedLaneHostOperation::DeploymentMetadata
        },
        udf::LogicalHostOperation::FunctionMetadata => {
            NormalRoutePairedLaneHostOperation::FunctionMetadata
        },
        udf::LogicalHostOperation::RequestMetadata => {
            NormalRoutePairedLaneHostOperation::RequestMetadata
        },
        udf::LogicalHostOperation::RequireOperation => {
            NormalRoutePairedLaneHostOperation::RequireOperation
        },
        udf::LogicalHostOperation::RunUdf => NormalRoutePairedLaneHostOperation::RunUdf,
        udf::LogicalHostOperation::Schedule => NormalRoutePairedLaneHostOperation::Schedule,
        udf::LogicalHostOperation::SnapshotTimestamp => {
            NormalRoutePairedLaneHostOperation::SnapshotTimestamp
        },
        udf::LogicalHostOperation::StorageDelete => {
            NormalRoutePairedLaneHostOperation::StorageDelete
        },
        udf::LogicalHostOperation::StorageGenerateUploadUrl => {
            NormalRoutePairedLaneHostOperation::StorageGenerateUploadUrl
        },
        udf::LogicalHostOperation::StorageGetMetadata => {
            NormalRoutePairedLaneHostOperation::StorageGetMetadata
        },
        udf::LogicalHostOperation::StorageGetUrl => {
            NormalRoutePairedLaneHostOperation::StorageGetUrl
        },
        udf::LogicalHostOperation::ThrowOcc => NormalRoutePairedLaneHostOperation::ThrowOcc,
        udf::LogicalHostOperation::ThrowOverloaded => {
            NormalRoutePairedLaneHostOperation::ThrowOverloaded
        },
        udf::LogicalHostOperation::TransactionMetrics => {
            NormalRoutePairedLaneHostOperation::TransactionMetrics
        },
        udf::LogicalHostOperation::UserIdentity => NormalRoutePairedLaneHostOperation::UserIdentity,
        udf::LogicalHostOperation::WriteDeploymentAuditLog => {
            NormalRoutePairedLaneHostOperation::WriteDeploymentAuditLog
        },
        udf::LogicalHostOperation::UnknownAsyncSyscall
        | udf::LogicalHostOperation::UnknownSyncSyscall => {
            anyhow::bail!("paired-lane host-operation trace contains an unknown operation")
        },
    })
}

fn normal_route_paired_lane_host_operation_trace(
    trace: &udf::HostOperationTrace,
) -> anyhow::Result<NormalRoutePairedLaneHostOperationTrace> {
    anyhow::ensure!(
        trace.is_comparison_eligible(),
        "paired-lane host-operation trace is disabled, incomplete, or unknown"
    );
    let entries = trace
        .entries()
        .context("paired-lane host-operation trace is disabled")?
        .iter()
        .map(|entry| {
            let status = match entry.status() {
                udf::LogicalHostOperationStatus::Success => {
                    NormalRoutePairedLaneHostOperationStatus::Success
                },
                udf::LogicalHostOperationStatus::Failure => {
                    NormalRoutePairedLaneHostOperationStatus::Failure
                },
                udf::LogicalHostOperationStatus::Pending => {
                    anyhow::bail!("paired-lane host-operation trace contains a pending operation")
                },
            };
            Ok(NormalRoutePairedLaneHostOperationTraceEntry {
                operation: normal_route_paired_lane_host_operation(entry.operation())?,
                status,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(NormalRoutePairedLaneHostOperationTrace { entries })
}

fn normal_route_paired_lane_not_applicable_evidence(
    host_operation_traces: Vec<NormalRoutePairedLaneHostOperationTrace>,
    forbidden_operations: &[NormalRoutePairedLaneHostOperation],
    dimension: &'static str,
) -> anyhow::Result<NormalRoutePairedLaneNotApplicableEvidence> {
    anyhow::ensure!(
        host_operation_traces.iter().all(|trace| trace
            .entries
            .iter()
            .all(|entry| { !forbidden_operations.contains(&entry.operation) })),
        "paired-lane {dimension} cannot be marked not-applicable because its host-operation trace \
         proves an effect"
    );
    Ok(NormalRoutePairedLaneNotApplicableEvidence {
        host_operation_traces,
        kind: "not-applicable",
    })
}

fn normalize_normal_route_paired_lane_outcome(
    outcome: NormalRoutePairedLaneOutcome,
    document_id_markers: &BTreeMap<String, JsonValue>,
) -> NormalRoutePairedLaneOutcome {
    match outcome {
        NormalRoutePairedLaneOutcome::Error { error } => {
            NormalRoutePairedLaneOutcome::Error { error }
        },
        NormalRoutePairedLaneOutcome::Returned { value } => {
            NormalRoutePairedLaneOutcome::Returned {
                value: normalize_paired_lane_value(&value, document_id_markers),
            }
        },
    }
}

async fn normal_route_paired_lane_post_state(
    database: &Database<ProdRuntime>,
    mut document_id_markers: BTreeMap<String, JsonValue>,
) -> anyhow::Result<(JsonValue, BTreeMap<String, JsonValue>)> {
    let mut tx = database.begin_system().await?;
    let mut observed_tables = tx
        .table_mapping()
        .iter_active_user_tables()
        .filter_map(|(_, namespace, _, table)| {
            (namespace == TableNamespace::root_component()).then(|| table.clone())
        })
        .collect::<Vec<_>>();
    // Table metadata order is backend-internal; sort it for lane-independent
    // snapshots.
    observed_tables.sort_unstable_by_key(|table| table.to_string());
    let mut raw_tables = Vec::with_capacity(observed_tables.len());
    for table in observed_tables {
        let mut query = ResolvedQuery::new(
            &mut tx,
            TableNamespace::root_component(),
            common::query::Query::full_table_scan(table.clone(), common::query::Order::Asc),
        )?;
        let mut documents = Vec::new();
        while let Some(document) = query.next(&mut tx, None).await? {
            let value = document.to_internal_json();
            let document_id = value
                .get("_id")
                .and_then(JsonValue::as_str)
                .context("paired-lane committed document omitted its ID")?
                .to_owned();
            let ordinal = documents.len() + 1;
            document_id_markers
                .entry(document_id)
                .or_insert_with(|| json!({ "$observedId": format!("{}:{ordinal}", table) }));
            documents.push(value);
        }
        raw_tables.push((table.to_string(), documents));
    }
    let tables = raw_tables
        .into_iter()
        .map(|(table, documents)| {
            json!({
                "documents": documents
                    .iter()
                    .map(|document| {
                        normalize_paired_lane_document(document, &document_id_markers)
                    })
                    .collect::<Vec<_>>(),
                "table": table,
            })
        })
        .collect::<Vec<_>>();
    Ok((json!({ "tables": tables }), document_id_markers))
}

fn paired_lane_unclassified_error(
    source: NormalRoutePairedLaneUnclassifiedErrorSource,
) -> NormalRoutePairedLaneOutcome {
    NormalRoutePairedLaneOutcome::Error {
        error: NormalRoutePairedLaneError::Unclassified { source },
    }
}

fn paired_lane_developer_error(
    host_operation_error: Option<udf::HostOperationErrorV1>,
    argument: &JsonValue,
    missing_id_bindings: &[NormalRoutePairedLaneMissingId],
) -> NormalRoutePairedLaneOutcome {
    let Some(udf::HostOperationErrorV1::NonexistentDocument {
        operation,
        document_id,
    }) = host_operation_error
    else {
        return paired_lane_unclassified_error(
            NormalRoutePairedLaneUnclassifiedErrorSource::Developer,
        );
    };
    let document_id = document_id.encode();
    let missing_id_bindings = missing_id_bindings
        .iter()
        .filter(|binding| {
            binding
                .path
                .iter()
                .try_fold(argument, |value, field| value.as_object()?.get(field))
                .and_then(JsonValue::as_str)
                == Some(document_id.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    if missing_id_bindings.is_empty() {
        paired_lane_unclassified_error(NormalRoutePairedLaneUnclassifiedErrorSource::Developer)
    } else {
        NormalRoutePairedLaneOutcome::Error {
            error: NormalRoutePairedLaneError::MissingDocument {
                operation: operation.into(),
                missing_id_bindings,
            },
        }
    }
}

struct NormalRoutePairedLaneInvocation {
    host_operation_trace: NormalRoutePairedLaneHostOperationTrace,
    outcome: NormalRoutePairedLaneOutcome,
    query_journal: NormalRouteQueryJournalObservation,
    transaction_write_set: Vec<NormalRoutePairedLaneWriteIntent>,
}

fn normal_route_paired_lane_execution_mode(
    lane: NormalRouteTestLane,
) -> anyhow::Result<FunctionExecutionMode> {
    match lane {
        NormalRouteTestLane::V8 => Ok(FunctionExecutionMode::V8WithHostOperationTrace),
        NormalRouteTestLane::Wasm => {
            Ok(FunctionExecutionMode::StaticHermesWasmWithHostOperationTrace)
        },
        NormalRouteTestLane::Shadow => {
            anyhow::bail!("query-shadow normal-route test does not support paired fixtures")
        },
    }
}

async fn run_normal_route_paired_lane_invocation(
    runner: &ApplicationFunctionRunner<ProdRuntime>,
    database: &Database<ProdRuntime>,
    path: PublicFunctionPath,
    lane: NormalRouteTestLane,
    udf_kind: &str,
    argument: JsonValue,
    missing_id_bindings: &[NormalRoutePairedLaneMissingId],
    document_id_markers: &mut BTreeMap<String, JsonValue>,
) -> anyhow::Result<NormalRoutePairedLaneInvocation> {
    let (udf_type, function_kind) = match udf_kind {
        "query" => (
            UdfType::Query,
            DatabaseFunctionKind::Query(ActiveJavascriptClass::Protected),
        ),
        "mutation" => (UdfType::Mutation, DatabaseFunctionKind::Mutation),
        _ => anyhow::bail!("paired-lane fixture selected an unsupported UDF kind"),
    };
    let caller = FunctionCaller::Cron;
    let request_context = RequestContext::new_for_system_request(RequestId::new());
    let context = ExecutionContext::new(request_context, &caller);
    let arguments = SerializedArgs::from_args(vec![argument.clone()])?;
    let mut transaction = database.begin(Identity::system()).await?;
    let path_and_args = udf::validation::ValidatedPathAndArgs::new(
        caller.allowed_visibility(),
        &mut transaction,
        path,
        arguments,
        udf_type,
    )
    .await??;
    let (mut transaction, outcome) = runner
        .isolate_functions
        .execute_query_or_mutation_with_mode(
            transaction,
            path_and_args,
            function_kind,
            QueryJournal::new(),
            context,
            SchedulerDependencyClass::Independent,
            normal_route_paired_lane_execution_mode(lane)?,
        )
        .await?;
    let outcome = match (udf_type, outcome) {
        (UdfType::Query, udf::FunctionOutcome::Query(outcome))
        | (UdfType::Mutation, udf::FunctionOutcome::Mutation(outcome)) => outcome,
        _ => anyhow::bail!("paired-lane function runner returned the wrong UDF outcome"),
    };
    let query_journal = (&outcome.journal).into();
    let host_operation_trace =
        normal_route_paired_lane_host_operation_trace(&outcome.host_operation_trace)?;
    // Do not commit a paired mutation unless the trace proves both N/A
    // dimensions. The final observation repeats these complete traces for
    // authenticated comparison evidence.
    normal_route_paired_lane_not_applicable_evidence(
        vec![host_operation_trace.clone()],
        PAIRED_LANE_EXTERNAL_EFFECT_OPERATIONS,
        "external effects",
    )?;
    normal_route_paired_lane_not_applicable_evidence(
        vec![host_operation_trace.clone()],
        PAIRED_LANE_SCHEDULER_SYSTEM_STATE_OPERATIONS,
        "scheduler system state",
    )?;
    let transaction_write_set =
        normal_route_paired_lane_transaction_write_set(&mut transaction, document_id_markers)?;
    let host_operation_error = outcome.host_operation_error;
    let outcome = match outcome.result {
        Ok(result) if udf_type == UdfType::Query => {
            anyhow::ensure!(
                transaction_write_set.is_empty(),
                "paired-lane query produced a transaction write intent"
            );
            NormalRoutePairedLaneOutcome::Returned {
                value: result.json_value(),
            }
        },
        Ok(result) if udf_type == UdfType::Mutation => {
            let commit_ts = database
                .commit_with_write_source(transaction, "static_hermes_paired_lane_mutation")
                .await?;
            NormalRoutePairedLaneOutcome::Returned {
                value: result.resolve_commit_ts(i64::from(commit_ts))?.json_value(),
            }
        },
        Err(_) => paired_lane_developer_error(host_operation_error, &argument, missing_id_bindings),
        Ok(_) => unreachable!("paired-lane UDF type is restricted to query or mutation"),
    };
    Ok(NormalRoutePairedLaneInvocation {
        host_operation_trace,
        outcome,
        query_journal,
        transaction_write_set,
    })
}

async fn run_normal_registry_paired_lane_batch_test(
    rt: ProdRuntime,
    batch: NormalRoutePairedLaneBatchExpectation,
) -> anyhow::Result<()> {
    normal_route_paired_lane_batch_header_is_valid(&batch.kind, batch.cases.len())?;
    let progress_path = batch.progress_path.clone();
    let mut case_ids = BTreeSet::new();
    let mut cohorts = BTreeMap::<String, Vec<NormalRoutePairedLaneBatchCase>>::new();
    for case in batch.cases {
        anyhow::ensure!(
            !case.case_id.is_empty()
                && case.case_id.len() <= 128
                && case_ids.insert(case.case_id.clone())
                && matches!(
                    case.expectation.setup,
                    NormalRouteTestSetup::PairedLaneV1 { .. }
                ),
            "paired-lane batch case identity or fixture is invalid"
        );
        cohorts
            .entry(case.expectation.package_key.clone())
            .or_default()
            .push(case);
    }
    let expected_case_ids = case_ids.iter().cloned().collect::<Vec<_>>();
    let mut batch_cases = Vec::new();
    let mut completed_case_ids = Vec::with_capacity(case_ids.len());
    let mut cohort_mechanisms = Vec::with_capacity(cohorts.len());
    for (package_key, cohort_cases) in cohorts {
        let first_expectation = &cohort_cases[0].expectation;
        anyhow::ensure!(
            cohort_cases.iter().all(|case| {
                case.expectation.deployment_sha256 == first_expectation.deployment_sha256
                    && case.expectation.generation_manifest_sha256
                        == first_expectation.generation_manifest_sha256
                    && case.expectation.generation_sha256 == first_expectation.generation_sha256
                    && case.expectation.lane == first_expectation.lane
                    && case.expectation.package_key == package_key
                    && case.expectation.source.package.sha256
                        == first_expectation.source.package.sha256
                    && case.expectation.source.package.size == first_expectation.source.package.size
                    && case.expectation.source.source_package_sha256
                        == first_expectation.source.source_package_sha256
                    && case
                        .expectation
                        .source
                        .source_package_runtime_content_sha256
                        == first_expectation
                            .source
                            .source_package_runtime_content_sha256
                    && case.expectation.source.bundle.sha256
                        == first_expectation.source.bundle.sha256
                    && case.expectation.source.bundle.size == first_expectation.source.bundle.size
            }),
            "paired-lane batch cohort does not share one deployment, generation, lane, package, \
             source-package identity, and source-bundle identity"
        );
        let deployment_sha256 = first_expectation.deployment_sha256.clone();
        let generation_sha256 = first_expectation.generation_sha256.clone();
        let lane = first_expectation.lane;
        let cohort_invocation_count = cohort_cases
            .iter()
            .map(|case| match &case.expectation.setup {
                NormalRouteTestSetup::PairedLaneV1 { invocations, .. } => invocations.len(),
                _ => 0,
            })
            .sum::<usize>();
        anyhow::ensure!(
            cohort_invocation_count >= 2,
            "paired-lane batch cohort invocation count is invalid"
        );
        let shared_source_package =
            new_normal_route_shared_source_package(rt.clone(), first_expectation).await?;
        let shared_source_bundle = new_normal_route_shared_source_bundle(first_expectation)?;
        let hooks = StaticHermesGateTestHooks::new(0, 0);
        let hooks_guard = install_static_hermes_gate_test_hooks(&hooks)?;
        let cohort_len = cohort_cases.len();
        let expected_cohort_case_ids = cohort_cases
            .iter()
            .map(|case| case.case_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut cohort_case_ids = Vec::with_capacity(cohort_len);
        let mut cohort_observations = Vec::with_capacity(cohort_len);
        for case in cohort_cases {
            let observation_path = case.expectation.observation_path.clone();
            let case_id = case.case_id;
            let route = case.expectation.route.clone();
            write_normal_route_batch_progress(
                &progress_path,
                &NormalRoutePairedLaneBatchProgress {
                    active_case: Some(NormalRoutePairedLaneBatchProgressCase {
                        actual_package_key: None,
                        case_id: case_id.clone(),
                        expected_cohort_case_ids: expected_cohort_case_ids.clone(),
                        expected_package_key: package_key.clone(),
                        route: route.clone(),
                    }),
                    batch_kind: "convex-wasm-normal-route-paired-batch-v1",
                    completed_case_ids: completed_case_ids.clone(),
                    expected_case_ids: expected_case_ids.clone(),
                    kind: "convex-wasm-normal-route-paired-batch-progress-v1",
                    lane,
                    schema_version: 1,
                    status: "running",
                },
            )?;
            run_normal_registry_route_test_with_paired_options(
                rt.clone(),
                case.expectation,
                NormalRoutePairedLaneExecutionOptions {
                    case_id: Some(case_id.clone()),
                    cleanup_after: true,
                    require_cold_start: true,
                    verify_provenance: true,
                },
                Some(Arc::clone(&hooks)),
                Some(&shared_source_package),
                Some(&shared_source_bundle),
            )
            .await?;
            let observation = serde_json::from_slice::<JsonValue>(
                &std::fs::read(&observation_path).with_context(|| {
                    format!("failed to read paired-lane case observation {case_id}")
                })?,
            )
            .with_context(|| format!("failed to parse paired-lane case observation {case_id}"))?;
            let actual_package_key =
                normal_route_paired_lane_observed_package_key(&observation, lane)?;
            cohort_case_ids.push(case_id.clone());
            cohort_observations.push(observation.clone());
            batch_cases.push(NormalRoutePairedLaneBatchObservationCase {
                case_id: case_id.clone(),
                observation,
            });
            completed_case_ids.push(case_id.clone());
            write_normal_route_batch_progress(
                &progress_path,
                &NormalRoutePairedLaneBatchProgress {
                    active_case: Some(NormalRoutePairedLaneBatchProgressCase {
                        actual_package_key,
                        case_id,
                        expected_cohort_case_ids: expected_cohort_case_ids.clone(),
                        expected_package_key: package_key.clone(),
                        route,
                    }),
                    batch_kind: "convex-wasm-normal-route-paired-batch-v1",
                    completed_case_ids: completed_case_ids.clone(),
                    expected_case_ids: expected_case_ids.clone(),
                    kind: "convex-wasm-normal-route-paired-batch-progress-v1",
                    lane,
                    schema_version: 1,
                    status: "observed",
                },
            )?;
        }
        drop(hooks_guard);
        let runtime_admissions = cohort_observations
            .iter()
            .map(|observation| {
                normal_route_paired_lane_observation_counter(
                    observation,
                    "measuredFreshRuntimeAdmissions",
                )
            })
            .sum::<anyhow::Result<usize>>()?;
        let pool_hits = cohort_observations
            .iter()
            .map(|observation| {
                normal_route_paired_lane_observation_counter(observation, "poolHits")
            })
            .sum::<anyhow::Result<usize>>()?;
        let pool_misses = cohort_observations
            .iter()
            .map(|observation| {
                normal_route_paired_lane_observation_counter(observation, "poolMisses")
            })
            .sum::<anyhow::Result<usize>>()?;
        let provenance_rejections = cohort_observations
            .iter()
            .map(|observation| {
                normal_route_paired_lane_observation_counter(observation, "provenanceRejections")
            })
            .sum::<anyhow::Result<usize>>()?;
        let teardown_count = cohort_observations
            .iter()
            .map(|observation| {
                normal_route_paired_lane_observation_counter(observation, "teardownsAfterCleanup")
            })
            .sum::<anyhow::Result<usize>>()?;
        let pooled_runtime_ids = cohort_observations
            .iter()
            .map(normal_route_paired_lane_observation_runtime_ids)
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>();
        match lane {
            NormalRouteTestLane::Wasm => anyhow::ensure!(
                runtime_admissions == cohort_len
                    && pool_misses == cohort_len
                    && pool_hits == cohort_invocation_count - cohort_len
                    && provenance_rejections == cohort_len
                    && teardown_count == cohort_len
                    && pooled_runtime_ids.len() == cohort_len,
                "paired-lane batch cohort did not preserve per-fixture Wasm lifecycle controls"
            ),
            NormalRouteTestLane::V8 => anyhow::ensure!(
                runtime_admissions == 0
                    && pool_misses == 0
                    && pool_hits == 0
                    && provenance_rejections == 0
                    && teardown_count == 0
                    && pooled_runtime_ids.is_empty(),
                "paired-lane V8 batch cohort reached generated-runtime accounting"
            ),
            NormalRouteTestLane::Shadow => {
                anyhow::bail!("query-shadow normal-route test does not support paired batches")
            },
        }
        cohort_mechanisms.push(NormalRoutePairedLaneCohortMechanism {
            case_ids: cohort_case_ids,
            deployment_sha256,
            generation_sha256,
            invocation_count: cohort_invocation_count,
            lane,
            package_key,
            pooled_runtime_count: pooled_runtime_ids.len(),
            provenance_rejections,
            runtime_admissions,
            teardown_count,
        });
    }
    batch_cases.sort_by(|left, right| left.case_id.cmp(&right.case_id));
    let first_mechanism = cohort_mechanisms
        .first()
        .context("paired-lane batch produced no cohorts")?;
    anyhow::ensure!(
        cohort_mechanisms.iter().all(|mechanism| {
            mechanism.deployment_sha256 == first_mechanism.deployment_sha256
                && mechanism.generation_sha256 == first_mechanism.generation_sha256
                && mechanism.lane == first_mechanism.lane
        }),
        "paired-lane batch does not share one deployment, generation, and lane"
    );
    let batch_lane = first_mechanism.lane;
    write_normal_route_observation(
        &batch.observation_path,
        &NormalRoutePairedLaneBatchObservation {
            kind: "convex-wasm-normal-route-paired-batch-observation-v7",
            schema_version: 7,
            lane: batch_lane,
            cases: batch_cases,
            cohort_mechanisms,
        },
    )?;
    write_normal_route_batch_progress(
        &progress_path,
        &NormalRoutePairedLaneBatchProgress {
            active_case: None,
            batch_kind: "convex-wasm-normal-route-paired-batch-v1",
            completed_case_ids,
            expected_case_ids,
            kind: "convex-wasm-normal-route-paired-batch-progress-v1",
            lane: batch_lane,
            schema_version: 1,
            status: "completed",
        },
    )?;
    Ok(())
}

fn normal_route_paired_lane_batch_header_is_valid(
    kind: &str,
    case_count: usize,
) -> anyhow::Result<()> {
    // The authenticated expectation handoff determines batch length; validate every
    // case below instead of imposing a local cardinality cap.
    anyhow::ensure!(
        kind == "convex-wasm-normal-route-paired-batch-v1" && case_count > 0,
        "paired-lane batch kind or case bounds are invalid"
    );
    Ok(())
}

async fn run_normal_registry_paired_lane_test(
    runner: Arc<ApplicationFunctionRunner<ProdRuntime>>,
    database: Database<ProdRuntime>,
    expectation: NormalRouteTestExpectation,
    fixture: NormalRoutePairedLaneState,
    options: NormalRoutePairedLaneExecutionOptions,
    hooks: Arc<StaticHermesGateTestHooks>,
) -> anyhow::Result<NormalRoutePairedLaneObservation> {
    let paired_fixture_sha256 = expectation
        .paired_fixture_sha256
        .as_deref()
        .context("paired-lane fixture identity is missing")?;
    let _ = normal_route_sha256(paired_fixture_sha256, "paired-lane fixture identity")?;
    let table_mapping_sha256 =
        normal_route_paired_lane_initial_table_mapping_sha256(&database).await?;
    let module_name = expectation
        .route
        .runtime_module_path
        .strip_suffix(".js")
        .context("paired-lane runtime module path must end in .js")?;
    let path = PublicFunctionPath::RootExport(
        format!("{module_name}:{}", expectation.route.export_name).parse()?,
    );
    configure_normal_route_execution_fuel(&hooks)?;
    let hook_counts_before = normal_route_paired_lane_hook_counts(&hooks);
    let mut outcomes = Vec::with_capacity(fixture.invocations.len());
    let mut query_journals = Vec::with_capacity(fixture.invocations.len());
    let mut transaction_write_sets = Vec::with_capacity(fixture.invocations.len());
    let mut host_operation_traces = Vec::with_capacity(fixture.invocations.len());
    let mut document_id_markers = fixture
        .fixture_ids
        .iter()
        .map(|(alias, document_id)| (document_id.encode(), json!({ "$fixtureId": alias })))
        .collect::<BTreeMap<_, _>>();
    let mut captured_opaque_cursors = BTreeMap::new();
    for (invocation_index, arguments) in fixture.invocations.iter().enumerate() {
        let arguments =
            resolve_normal_route_paired_lane_opaque_cursors(arguments, &captured_opaque_cursors)?;
        let invocation = run_normal_route_paired_lane_invocation(
            &runner,
            &database,
            path.clone(),
            expectation.lane,
            &expectation.route.udf_kind,
            arguments,
            &fixture.missing_id_bindings,
            &mut document_id_markers,
        )
        .await?;
        let (outcome, query_journal) = match invocation.outcome {
            NormalRoutePairedLaneOutcome::Returned { value } => (
                NormalRoutePairedLaneOutcome::Returned {
                    value: capture_normal_route_paired_lane_opaque_cursors(
                        &value,
                        invocation_index,
                        &fixture.opaque_cursors,
                        &mut captured_opaque_cursors,
                    )?,
                },
                normal_route_paired_lane_query_journal_with_opaque_cursor_continuation(
                    invocation.query_journal,
                    invocation_index,
                    &fixture.opaque_cursors,
                ),
            ),
            NormalRoutePairedLaneOutcome::Error { error } => (
                NormalRoutePairedLaneOutcome::Error { error },
                invocation.query_journal,
            ),
        };
        outcomes.push(outcome);
        query_journals.push(query_journal);
        transaction_write_sets.push(invocation.transaction_write_set);
        host_operation_traces.push(invocation.host_operation_trace);
    }
    let mut provenance_rejections = 0;
    if matches!(expectation.lane, NormalRouteTestLane::Wasm) && options.verify_provenance {
        let runtime_observations_before = hooks.generated_runtime_ids().len();
        let pool_hits_before = hooks.generated_pool_hits();
        let pool_misses_before = hooks.generated_pool_misses();
        install_normal_route_provenance_mismatch(&database).await?;
        let rejection = run_normal_route_paired_lane_invocation(
            &runner,
            &database,
            path.clone(),
            expectation.lane,
            &expectation.route.udf_kind,
            fixture
                .invocations
                .first()
                .context("paired-lane fixture has no invocation")?
                .clone(),
            &fixture.missing_id_bindings,
            &mut document_id_markers,
        )
        .await;
        anyhow::ensure!(
            rejection.is_err()
                && hooks.generated_runtime_ids().len() == runtime_observations_before
                && hooks.generated_pool_hits() == pool_hits_before
                && hooks.generated_pool_misses() == pool_misses_before,
            "paired-lane provenance rejection reached runtime admission"
        );
        provenance_rejections = 1;
    }
    let (normalized_committed_state, document_id_markers) =
        normal_route_paired_lane_post_state(&database, document_id_markers).await?;
    let outcomes = outcomes
        .into_iter()
        .map(|outcome| normalize_normal_route_paired_lane_outcome(outcome, &document_id_markers))
        .collect::<Vec<_>>();
    let comparison_evidence = NormalRoutePairedLaneComparisonEvidence {
        external_effects: normal_route_paired_lane_not_applicable_evidence(
            host_operation_traces.clone(),
            PAIRED_LANE_EXTERNAL_EFFECT_OPERATIONS,
            "external effects",
        )?,
        scheduler_system_state: normal_route_paired_lane_not_applicable_evidence(
            host_operation_traces,
            PAIRED_LANE_SCHEDULER_SYSTEM_STATE_OPERATIONS,
            "scheduler system state",
        )?,
        transaction_write_sets,
    };
    let runtime_ids = hooks
        .generated_runtime_ids()
        .into_iter()
        .skip(hook_counts_before.runtime_ids)
        .collect::<Vec<_>>();
    let instance_ids = hooks
        .instance_ids()
        .into_iter()
        .skip(hook_counts_before.instance_ids)
        .collect::<Vec<_>>();
    let runtime_invocations = hooks
        .generated_invocations()
        .into_iter()
        .skip(hook_counts_before.runtime_invocations)
        .collect::<Vec<_>>();
    let pool_hits = normal_route_paired_lane_counter_delta(
        hooks.generated_pool_hits(),
        hook_counts_before.pool_hits,
        "pool-hit",
    )?;
    let pool_misses = normal_route_paired_lane_counter_delta(
        hooks.generated_pool_misses(),
        hook_counts_before.pool_misses,
        "pool-miss",
    )?;
    let pool_events = hooks
        .generated_pool_events()
        .into_iter()
        .skip(hook_counts_before.pool_events)
        .collect::<Vec<_>>();
    let capability_operation_stages = hooks
        .guest_native_completion_stages()
        .into_iter()
        .skip(hook_counts_before.capability_operation_stages)
        .filter(|stage| stage.starts_with("start:"))
        .collect::<Vec<_>>();
    let execution_errors = hooks
        .execution_errors()
        .into_iter()
        .skip(hook_counts_before.execution_errors)
        .collect::<Vec<_>>();
    let execution_error_count = execution_errors.len();
    let database_query_starts = normal_route_paired_lane_counter_delta(
        hooks.database_query_starts(),
        hook_counts_before.database_query_starts,
        "database-query-start",
    )?;
    let reads_completed = normal_route_paired_lane_counter_delta(
        hooks.read_completed(),
        hook_counts_before.reads_completed,
        "read-completion",
    )?;
    let reads_cancelled = normal_route_paired_lane_counter_delta(
        hooks.read_cancelled(),
        hook_counts_before.reads_cancelled,
        "read-cancellation",
    )?;
    let read_documents = normal_route_paired_lane_counter_delta(
        hooks.read_documents(),
        hook_counts_before.read_documents,
        "read-document",
    )?;
    let read_bytes = normal_route_paired_lane_counter_delta(
        hooks.read_bytes(),
        hook_counts_before.read_bytes,
        "read-byte",
    )?;
    let read_intervals = normal_route_paired_lane_counter_delta(
        hooks.read_intervals(),
        hook_counts_before.read_intervals,
        "read-interval",
    )?;
    let transaction_drops = normal_route_paired_lane_counter_delta(
        hooks.transaction_drops(),
        hook_counts_before.transaction_drops,
        "transaction-drop",
    )?;
    let outcome_error_count = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, NormalRoutePairedLaneOutcome::Error { .. }))
        .count();
    let invocation_count = outcomes.len();
    match expectation.lane {
        NormalRouteTestLane::Wasm => {
            let expected_pool_misses = usize::from(options.require_cold_start);
            let mut failed_invariants = [
                ("runtime-count", runtime_ids.len() == invocation_count),
                ("instance-count", instance_ids.len() == invocation_count),
                (
                    "runtime-invocation-count",
                    runtime_invocations.len() == invocation_count,
                ),
                (
                    "single-runtime",
                    runtime_ids.first().is_some_and(|first| {
                        runtime_ids.iter().all(|runtime_id| runtime_id == first)
                    }),
                ),
                (
                    "deployment-identity",
                    runtime_invocations.iter().all(|observation| {
                        observation.deployment_sha256.as_deref()
                            == Some(expectation.deployment_sha256.as_str())
                    }),
                ),
                (
                    "package-identity",
                    runtime_invocations
                        .iter()
                        .all(|observation| observation.package_key == expectation.package_key),
                ),
                (
                    "entry-identity",
                    runtime_invocations.iter().all(|observation| {
                        observation.entry_id.as_deref()
                            == Some(expectation.routing.entry_id.as_str())
                            && observation.entry_selector_id.as_deref()
                                == Some(expectation.routing.entry_selector_id.as_str())
                    }),
                ),
                (
                    "capability-revoked",
                    runtime_invocations
                        .iter()
                        .all(|observation| observation.capability_revoked),
                ),
                (
                    "forged-capability-rejected",
                    runtime_invocations
                        .iter()
                        .all(|observation| observation.forged_capability_rejected),
                ),
                (
                    "revoked-capability-rejected",
                    runtime_invocations
                        .iter()
                        .all(|observation| observation.revoked_capability_rejected),
                ),
                (
                    "opaque-live-handles",
                    runtime_invocations
                        .iter()
                        .all(|observation| observation.opaque_live_handles == 0),
                ),
                (
                    "opaque-current-bytes",
                    runtime_invocations
                        .iter()
                        .all(|observation| observation.opaque_current_bytes == 0),
                ),
                (
                    "reuse-contamination",
                    runtime_invocations
                        .iter()
                        .all(|observation| !observation.runtime_reuse_contaminated),
                ),
                (
                    "cold-start",
                    runtime_invocations.first().is_some_and(|observation| {
                        observation.runtime_was_created == options.require_cold_start
                    }),
                ),
                (
                    "subsequent-runtime-reuse",
                    runtime_invocations
                        .iter()
                        .skip(1)
                        .all(|observation| !observation.runtime_was_created),
                ),
                (
                    "subsequent-prior-capability-rejected",
                    runtime_invocations
                        .iter()
                        .skip(1)
                        .all(|observation| observation.prior_capability_rejected),
                ),
                ("cancelled-reads", reads_cancelled == 0),
                ("transaction-drops", transaction_drops == 0),
            ]
            .into_iter()
            .filter_map(|(name, passed)| (!passed).then_some(name.to_owned()))
            .collect::<Vec<_>>();
            if pool_misses != expected_pool_misses {
                failed_invariants.push(format!(
                    "pool-misses(actual={pool_misses}, expected={expected_pool_misses})"
                ));
            }
            let expected_pool_hits = invocation_count - expected_pool_misses;
            if pool_hits != expected_pool_hits {
                failed_invariants.push(format!(
                    "pool-hits(actual={pool_hits}, expected={expected_pool_hits})"
                ));
            }
            if execution_error_count != 0 {
                failed_invariants.push(format!(
                    "execution-errors(actual={execution_error_count}, expected=0)"
                ));
            }
            anyhow::ensure!(
                failed_invariants.is_empty(),
                "paired-lane Wasm execution did not preserve retained runtime reuse and cleanup \
                 (pool-hits={pool_hits}, pool-misses={pool_misses}, \
                 execution-errors={execution_error_count}, outcome-errors={outcome_error_count}): \
                 {}; execution-error-details={execution_errors:?}; invocations={:?}; \
                 pool-events={pool_events:?}",
                failed_invariants.join(", "),
                runtime_invocations
            );
        },
        NormalRouteTestLane::V8 => anyhow::ensure!(
            runtime_ids.is_empty()
                && instance_ids.is_empty()
                && runtime_invocations.is_empty()
                && pool_hits == 0
                && pool_misses == 0
                && capability_operation_stages.is_empty()
                && execution_error_count == 0
                && database_query_starts == 0
                && reads_completed == 0
                && reads_cancelled == 0
                && read_documents == 0
                && read_bytes == 0
                && read_intervals == 0
                && transaction_drops == 0,
            "paired-lane V8 control reached generated-runtime accounting"
        ),
        NormalRouteTestLane::Shadow => {
            anyhow::bail!("query-shadow normal-route test does not support paired fixtures")
        },
    }
    let runtime_invocations = runtime_invocations
        .into_iter()
        .map(TryInto::try_into)
        .collect::<anyhow::Result<Vec<NormalRouteRuntimeInvocation>>>()?;
    let unique_runtime_count = runtime_ids.iter().copied().collect::<BTreeSet<_>>().len();
    let unique_memory_slot_count = runtime_invocations
        .iter()
        .map(|observation| observation.memory_slot_id)
        .collect::<BTreeSet<_>>()
        .len();
    let cleaned_up_instances = if options.cleanup_after {
        cleanup_static_hermes_test_instances::<ProdRuntime>().await?
    } else {
        0
    };
    let teardowns_after_cleanup = normal_route_paired_lane_counter_delta(
        hooks.teardowns(),
        hook_counts_before.teardowns,
        "teardown",
    )?;
    let store_drops_after_cleanup = normal_route_paired_lane_counter_delta(
        hooks.store_drops(),
        hook_counts_before.store_drops,
        "store-drop",
    )?;
    // Every pool hit replaces the previous HostState, and every
    // runtime teardown drops the final state from its Store.
    let expected_store_drops = pool_hits + teardowns_after_cleanup;
    let case_id = options.case_id.as_deref().unwrap_or("single-case");
    let route = &expectation.route;
    match expectation.lane {
        NormalRouteTestLane::Wasm if options.cleanup_after => anyhow::ensure!(
            cleaned_up_instances == 1
                && teardowns_after_cleanup == 1
                && store_drops_after_cleanup == expected_store_drops,
            "paired-lane Wasm cleanup did not destroy its retained runtime: case_id={case_id}, \
             route={}:{} ({}, {}), package_key={}, cleaned_up_instances={cleaned_up_instances}, \
             teardowns_after_cleanup={teardowns_after_cleanup}, \
             store_drops_after_cleanup={store_drops_after_cleanup}, \
             expected_store_drops={expected_store_drops}, \
             runtime_invocations={runtime_invocations:?}",
            route.runtime_module_path,
            route.export_name,
            route.udf_kind,
            route.visibility,
            expectation.package_key,
        ),
        NormalRouteTestLane::Wasm => anyhow::ensure!(
            cleaned_up_instances == 0
                && teardowns_after_cleanup == 0
                && store_drops_after_cleanup == expected_store_drops,
            "paired-lane Wasm non-final cohort lifecycle accounting is invalid: \
             case_id={case_id}, route={}:{} ({}, {}), package_key={}, \
             cleaned_up_instances={cleaned_up_instances}, \
             teardowns_after_cleanup={teardowns_after_cleanup}, \
             store_drops_after_cleanup={store_drops_after_cleanup}, \
             expected_store_drops={expected_store_drops}, \
             runtime_invocations={runtime_invocations:?}",
            route.runtime_module_path,
            route.export_name,
            route.udf_kind,
            route.visibility,
            expectation.package_key,
        ),
        NormalRouteTestLane::V8 => anyhow::ensure!(
            cleaned_up_instances == 0
                && teardowns_after_cleanup == 0
                && store_drops_after_cleanup == 0,
            "paired-lane V8 control retained a generated runtime"
        ),
        NormalRouteTestLane::Shadow => {
            anyhow::bail!("query-shadow normal-route test does not support paired fixtures")
        },
    }
    let observation = NormalRoutePairedLaneObservation {
        kind: "convex-wasm-normal-route-paired-observation-v7",
        schema_version: 7,
        deployment_sha256: expectation.deployment_sha256,
        generation_sha256: expectation.generation_sha256,
        lane: expectation.lane,
        paired_fixture_sha256: paired_fixture_sha256.to_owned(),
        table_mapping_sha256,
        package_key: expectation.package_key,
        route: expectation.route,
        comparison_evidence,
        comparison_dimensions: NormalRoutePairedLaneComparisonDimensions {
            committed_root_user_table_post_state: "compared",
            external_effects: "not-applicable",
            outcomes: "compared",
            query_journal_end_cursor: "compared",
            query_journal_full: "compared",
            scheduler_system_state: "not-applicable",
            transaction_write_set: "compared",
        },
        invocation_count,
        outcomes,
        query_journals,
        normalized_committed_state,
        database_query_starts,
        reads_completed,
        reads_cancelled,
        read_documents,
        read_bytes,
        read_intervals,
        transaction_drops,
        execution_error_count,
        runtime_observation_count: runtime_ids.len(),
        unique_runtime_count,
        unique_memory_slot_count,
        runtime_invocations,
        pool_hits,
        pool_misses,
        measured_pool_hits: pool_hits,
        measured_pool_misses: pool_misses,
        measured_fresh_runtime_admissions: usize::from(
            matches!(expectation.lane, NormalRouteTestLane::Wasm) && options.require_cold_start,
        ),
        capability_operation_stages,
        provenance_rejections,
        cleaned_up_instances,
        store_drops_after_cleanup,
        teardowns_after_cleanup,
    };
    database.shutdown().await?;
    Ok(observation)
}

fn normal_route_sequence_json_values_are_equivalent(
    actual: &JsonValue,
    expected: &JsonValue,
) -> bool {
    match (actual, expected) {
        (JsonValue::Array(actual_values), JsonValue::Array(expected_values)) => {
            actual_values.len() == expected_values.len()
                && actual_values
                    .iter()
                    .zip(expected_values)
                    .all(|(actual, expected)| {
                        normal_route_sequence_json_values_are_equivalent(actual, expected)
                    })
        },
        (JsonValue::Object(actual_fields), JsonValue::Object(expected_fields)) => {
            actual_fields.len() == expected_fields.len()
                && expected_fields.iter().all(|(field, expected)| {
                    actual_fields.get(field).is_some_and(|actual| {
                        normal_route_sequence_json_values_are_equivalent(actual, expected)
                    })
                })
        },
        (JsonValue::Number(actual), JsonValue::Number(expected)) => {
            matches!(
                (actual.as_f64(), expected.as_f64()),
                (Some(actual), Some(expected)) if actual == expected
            )
        },
        _ => actual == expected,
    }
}

fn normalize_normal_route_sequence_result(
    result: &JsonValue,
    expected_result: &JsonValue,
) -> anyhow::Result<JsonValue> {
    if expected_result
        == &json!({
            "configured": false,
            "serverObservedAt": { "kind": "invocation-time" },
        })
    {
        let fields = result
            .as_object()
            .context("normal route sequence invocation-time result is not an object")?;
        anyhow::ensure!(
            fields.len() == 2
                && fields.get("configured") == Some(&json!(false))
                && fields
                    .get("serverObservedAt")
                    .is_some_and(JsonValue::is_number),
            "normal route sequence invocation-time result changed"
        );
        return Ok(expected_result.clone());
    }
    // Expected results are ordinary JSON, while Convex JavaScript numbers return
    // through JsonPackedValue as Float64. serde_json compares number
    // representations, so 0 and 0.0 differ even though they have the
    // same JavaScript/Convex Number value. Results may nest arrays and
    // objects, so preserve those semantics recursively.
    anyhow::ensure!(
        normal_route_sequence_json_values_are_equivalent(result, expected_result),
        "normal route sequence result differs from its expectation"
    );
    Ok(expected_result.clone())
}

async fn normal_route_sequence_phase<T>(
    phase: &'static str,
    future: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    eprintln!("normal route sequence phase started: {phase}");
    let result = tokio::time::timeout(NORMAL_ROUTE_TEST_PHASE_TIMEOUT, future)
        .await
        .with_context(|| {
            format!(
                "normal route sequence phase {phase} exceeded the {} second limit",
                NORMAL_ROUTE_TEST_PHASE_TIMEOUT.as_secs()
            )
        })??;
    eprintln!("normal route sequence phase completed: {phase}");
    Ok(result)
}

#[test]
fn normal_route_sequence_result_normalizes_equivalent_numeric_json() -> anyhow::Result<()> {
    let expected = json!({
        "topLevel": 0,
        "nested": [
            { "negative": -2 },
            9007199254740993_u64,
        ],
    });
    let actual = serde_json::from_str::<JsonValue>(
        r#"{"topLevel":0.0,"nested":[{"negative":-2.0},9007199254740992.0]}"#,
    )?;
    assert_ne!(actual, expected);
    assert_eq!(
        normalize_normal_route_sequence_result(&actual, &expected)?,
        expected
    );
    assert!(
        !normal_route_sequence_json_values_are_equivalent(
            &json!({ "topLevel": 1.0, "nested": [{ "negative": -2.0 }, 0.0] }),
            &expected,
        ),
        "numeric equivalence accepted a changed nested result"
    );
    assert!(
        !normal_route_sequence_json_values_are_equivalent(
            &json!({ "topLevel": 0.0, "nested": [{ "negative": -2.0 }] }),
            &expected,
        ),
        "numeric equivalence accepted a changed nested shape"
    );
    Ok(())
}

#[test]
fn normal_route_paired_lane_document_normalizes_only_root_creation_time() {
    let document = json!({
        "_id": "root-id",
        "_creationTime": 101,
        "items": [{ "_creationTime": 303 }],
        "nested": { "_creationTime": 202 },
    });

    assert_eq!(
        normalize_paired_lane_document(&document, &BTreeMap::new()),
        json!({
            "_id": "root-id",
            "_creationTime": { "kind": "creation-time" },
            "items": [{ "_creationTime": 303 }],
            "nested": { "_creationTime": 202 },
        })
    );
}

#[test]
fn normal_route_paired_lane_document_hash_preserves_nested_creation_time() -> anyhow::Result<()> {
    let first_lane_document = json!({
        "_id": "root-id",
        "_creationTime": 101,
        "nested": { "_creationTime": 202 },
    });
    let second_lane_document = json!({
        "_id": "root-id",
        "_creationTime": 102,
        "nested": { "_creationTime": 202 },
    });
    let nested_creation_time_changed = json!({
        "_id": "root-id",
        "_creationTime": 102,
        "nested": { "_creationTime": 203 },
    });
    let document_id_markers = BTreeMap::new();

    let first_lane_hash =
        normal_route_paired_lane_document_sha256(&first_lane_document, &document_id_markers)?;
    assert_eq!(
        first_lane_hash,
        normal_route_paired_lane_document_sha256(&second_lane_document, &document_id_markers,)?
    );
    assert_ne!(
        first_lane_hash,
        normal_route_paired_lane_document_sha256(
            &nested_creation_time_changed,
            &document_id_markers,
        )?
    );
    Ok(())
}

#[test]
fn normal_route_paired_lane_return_value_preserves_creation_time() {
    let value = json!({
        "_creationTime": 101,
        "nested": { "_creationTime": 202 },
    });
    let normalized = normalize_normal_route_paired_lane_outcome(
        NormalRoutePairedLaneOutcome::Returned {
            value: value.clone(),
        },
        &BTreeMap::new(),
    );
    let NormalRoutePairedLaneOutcome::Returned {
        value: normalized_value,
    } = normalized
    else {
        panic!("paired-lane returned outcome was not preserved");
    };

    assert_eq!(normalized_value, value);
}

#[test]
fn normal_route_paired_lane_batch_header_accepts_authenticated_full_case_count() {
    assert!(normal_route_paired_lane_batch_header_is_valid(
        "convex-wasm-normal-route-paired-batch-v1",
        1_240,
    )
    .is_ok());
    assert!(normal_route_paired_lane_batch_header_is_valid(
        "convex-wasm-normal-route-paired-batch-v1",
        0,
    )
    .is_err());
    assert!(normal_route_paired_lane_batch_header_is_valid(
        "convex-wasm-normal-route-paired-batch-v2",
        1_240,
    )
    .is_err());
}

#[test]
fn normal_route_paired_lane_shared_source_identity_accepts_two_routes() -> anyhow::Result<()> {
    // This covers the structural cohort handoff without the external Wasm fixture.
    // The ignored post-wave four-case route test covers package upload and
    // shared source handoff across actual invocations within a cohort.
    let temp_dir = tempfile::tempdir()?;
    let package_bytes = b"normal-route-shared-source-package";
    let package_path = temp_dir.path().join("source-package");
    std::fs::write(&package_path, package_bytes)?;
    let package_sha256 = Sha256::hash(package_bytes).as_hex();

    let first_module_source = "export default 1;";
    let second_module_source = "export default 2;";
    let source_bundle = json!({
        "appDefinition": {
            "changedModules": [
                {
                    "environment": "isolate",
                    "path": "first.js",
                    "source": first_module_source,
                    "sourceMap": null,
                },
                {
                    "environment": "isolate",
                    "path": "second.js",
                    "source": second_module_source,
                    "sourceMap": null,
                },
                {
                    "environment": "node:pool:batch",
                    "nodePool": "batch",
                    "path": "pooled.js",
                    "source": "\"use node\"; \"use node pool:batch\"; export default 3;",
                    "sourceMap": null,
                },
                {
                    "environment": "node",
                    "path": "default_node.js",
                    "source": "\"use node\"; export default 4;",
                    "sourceMap": null,
                },
            ],
        },
    });
    let source_bundle_bytes = serde_json::to_vec(&source_bundle)?;
    let source_bundle_path = temp_dir.path().join("source-bundle.json");
    std::fs::write(&source_bundle_path, &source_bundle_bytes)?;
    let source_bundle_sha256 = Sha256::hash(&source_bundle_bytes).as_hex();

    let first_expectation = json!({
        "deploymentSha256": "a".repeat(64),
        "generationManifestSha256": "d".repeat(64),
        "generationSha256": "b".repeat(64),
        "lane": "wasm",
        "observationPath": temp_dir.path().join("first-observation.json"),
        "packageKey": "shared-package-key",
        "route": {
            "exportName": "first",
            "runtimeModulePath": "first.js",
            "udfKind": "query",
            "visibility": "public",
        },
        "routing": {
            "entryId": "first-entry",
            "entrySelectorId": "first-selector",
            "routeId": "first-route",
            "routeIdentity": "first-route-identity",
        },
        "setup": {
            "kind": "paired-lane-v1",
            "invocations": [],
            "opaqueCursors": [],
            "seededRows": [],
        },
        "source": {
            "bundle": {
                "path": source_bundle_path,
                "sha256": source_bundle_sha256.clone(),
                "size": source_bundle_bytes.len(),
            },
            "moduleSha256": Sha256::hash(first_module_source.as_bytes()).as_hex(),
            "moduleSource": first_module_source,
            "package": {
                "path": package_path,
                "sha256": package_sha256.clone(),
                "size": package_bytes.len(),
            },
            "scheduledModule": null,
            "sourceMap": null,
            "sourcePackageSha256": package_sha256.clone(),
            "sourcePackageRuntimeContentSha256": "c".repeat(64),
        },
    });
    let mut second_expectation = first_expectation.clone();
    second_expectation["observationPath"] = json!(temp_dir.path().join("second-observation.json"));
    second_expectation["route"]["exportName"] = json!("second");
    second_expectation["route"]["runtimeModulePath"] = json!("second.js");
    second_expectation["routing"]["entryId"] = json!("second-entry");
    second_expectation["routing"]["entrySelectorId"] = json!("second-selector");
    second_expectation["routing"]["routeId"] = json!("second-route");
    second_expectation["routing"]["routeIdentity"] = json!("second-route-identity");
    second_expectation["source"]["moduleSha256"] =
        json!(Sha256::hash(second_module_source.as_bytes()).as_hex());
    second_expectation["source"]["moduleSource"] = json!(second_module_source);
    let second_expectation_for_rejections = second_expectation.clone();

    let batch = serde_json::from_value::<NormalRoutePairedLaneBatchExpectation>(json!({
        "cases": [
            { "caseId": "first-case", "expectation": first_expectation },
            { "caseId": "second-case", "expectation": second_expectation },
        ],
        "kind": "convex-wasm-normal-route-paired-batch-v1",
        "observationPath": temp_dir.path().join("batch-observation.json"),
        "progressPath": temp_dir.path().join("batch-progress.json"),
    }))?;
    normal_route_paired_lane_batch_header_is_valid(&batch.kind, batch.cases.len())?;
    assert!(normal_route_paired_lane_batch_header_is_valid(&batch.kind, 1).is_ok());
    let [first_case, second_case] = batch.cases.as_slice() else {
        anyhow::bail!("paired-lane source identity fixture did not contain two cases");
    };
    assert_eq!(
        first_case.expectation.package_key,
        second_case.expectation.package_key
    );
    assert_eq!(
        first_case.expectation.source.bundle.sha256,
        second_case.expectation.source.bundle.sha256
    );
    assert_eq!(
        first_case.expectation.source.package.sha256,
        second_case.expectation.source.package.sha256
    );
    assert_ne!(
        first_case.expectation.route.runtime_module_path,
        second_case.expectation.route.runtime_module_path
    );

    let shared_source_bundle = new_normal_route_shared_source_bundle(&first_case.expectation)?;
    let topology = normal_route_source_bundle_topology(&first_case.expectation.source.bundle)?;
    assert_eq!(topology.get(&"pooled.js".parse()?), Some(&"batch".parse()?));
    assert_eq!(
        topology.default_routes(),
        Some(&BTreeSet::from(["default_node.js".parse()?]))
    );
    let shared_source_package = SourcePackage {
        storage_key: ObjectKey::try_from("normal-route-shared-source-package")?,
        sha256: normal_route_source_package_sha256(&first_case.expectation)?,
        runtime_content_sha256: Some(normal_route_sha256(
            &first_case
                .expectation
                .source
                .source_package_runtime_content_sha256,
            "shared source fixture runtime-content identity",
        )?),
        runtime_generation: Some(normal_route_runtime_generation(
            &first_case.expectation.deployment_sha256,
            &first_case.expectation.generation_manifest_sha256,
            &first_case.expectation.generation_sha256,
        )?),
        external_deps_package_id: None,
        package_size: PackageSize {
            zipped_size_bytes: package_bytes.len(),
            unzipped_size_bytes: 0,
        },
        node_version: None,
        node_executor_pool_topology: topology,
    };
    for expectation in [&first_case.expectation, &second_case.expectation] {
        normal_route_shared_source_bundle_is_authenticated(expectation, &shared_source_bundle)?;
        normal_route_shared_source_package_is_authenticated(expectation, &shared_source_package)?;
    }

    for runtime_content in ["f".repeat(64), "malformed".to_owned()] {
        let mut mismatched_runtime_content = serde_json::from_value::<NormalRouteTestExpectation>(
            second_expectation_for_rejections.clone(),
        )?;
        mismatched_runtime_content
            .source
            .source_package_runtime_content_sha256 = runtime_content;
        assert!(normal_route_shared_source_package_is_authenticated(
            &mismatched_runtime_content,
            &shared_source_package,
        )
        .is_err());
    }
    let mut other_generation = serde_json::from_value::<NormalRouteTestExpectation>(
        second_expectation_for_rejections.clone(),
    )?;
    other_generation.generation_manifest_sha256 = "e".repeat(64);
    assert!(normal_route_shared_source_package_is_authenticated(
        &other_generation,
        &shared_source_package,
    )
    .is_err());
    let mut source_without_runtime_content = second_expectation_for_rejections["source"].clone();
    source_without_runtime_content
        .as_object_mut()
        .context("source fixture is not an object")?
        .remove("sourcePackageRuntimeContentSha256");
    assert!(
        serde_json::from_value::<NormalRouteTestSource>(source_without_runtime_content).is_err()
    );

    let mut sequence_source = second_expectation_for_rejections["source"].clone();
    let sequence_source_fields = sequence_source
        .as_object_mut()
        .context("source fixture is not an object")?;
    let module_sha256 = sequence_source_fields
        .remove("moduleSha256")
        .context("source fixture omitted module identity")?;
    let module_source = sequence_source_fields
        .remove("moduleSource")
        .context("source fixture omitted module source")?;
    let source_map = sequence_source_fields
        .remove("sourceMap")
        .context("source fixture omitted source map")?;
    sequence_source_fields.insert(
        "modules".to_owned(),
        json!([{
            "runtimeModulePath": "second.js", "moduleSha256": module_sha256,
            "moduleSource": module_source, "sourceMap": source_map,
        }]),
    );
    let parsed_sequence =
        serde_json::from_value::<NormalRouteSequenceTestSource>(sequence_source.clone())?;
    assert_eq!(
        parsed_sequence.source_package_runtime_content_sha256,
        "c".repeat(64)
    );
    sequence_source
        .as_object_mut()
        .context("sequence source fixture is not an object")?
        .remove("sourcePackageRuntimeContentSha256");
    assert!(serde_json::from_value::<NormalRouteSequenceTestSource>(sequence_source).is_err());

    let mut mismatched_bundle_digest = serde_json::from_value::<NormalRouteTestExpectation>(
        second_expectation_for_rejections.clone(),
    )?;
    mismatched_bundle_digest.source.bundle.sha256 = "f".repeat(64);
    assert!(normal_route_shared_source_bundle_is_authenticated(
        &mismatched_bundle_digest,
        &shared_source_bundle,
    )
    .is_err());

    let mut mismatched_bundle_size = serde_json::from_value::<NormalRouteTestExpectation>(
        second_expectation_for_rejections.clone(),
    )?;
    mismatched_bundle_size.source.bundle.size += 1;
    assert!(normal_route_shared_source_bundle_is_authenticated(
        &mismatched_bundle_size,
        &shared_source_bundle,
    )
    .is_err());

    let mut mismatched_module_source = serde_json::from_value::<NormalRouteTestExpectation>(
        second_expectation_for_rejections.clone(),
    )?;
    mismatched_module_source.source.module_source = "export default 3;".to_owned();
    assert!(normal_route_shared_source_bundle_is_authenticated(
        &mismatched_module_source,
        &shared_source_bundle,
    )
    .is_err());

    let mut mismatched_package_digest = serde_json::from_value::<NormalRouteTestExpectation>(
        second_expectation_for_rejections.clone(),
    )?;
    mismatched_package_digest.source.package.sha256 = "f".repeat(64);
    assert!(normal_route_shared_source_package_is_authenticated(
        &mismatched_package_digest,
        &shared_source_package,
    )
    .is_err());

    let mut mismatched_package_size =
        serde_json::from_value::<NormalRouteTestExpectation>(second_expectation_for_rejections)?;
    mismatched_package_size.source.package.size += 1;
    assert!(normal_route_shared_source_package_is_authenticated(
        &mismatched_package_size,
        &shared_source_package,
    )
    .is_err());
    Ok(())
}

#[test]
fn normal_route_sequence_empty_database_state_is_non_seeding() -> anyhow::Result<()> {
    let expected = json!({
        "kind": "empty-database",
        "normalized": {
            "tableCounts": {
                "firstTable": 0,
                "secondTable": 0,
            },
        },
        "tables": ["firstTable", "secondTable"],
    });
    let state =
        serde_json::from_value::<NormalRouteSequenceExpectedCommittedState>(expected.clone())?;
    assert!(matches!(
        state,
        NormalRouteSequenceExpectedCommittedState::Tagged(
            NormalRouteSequenceTaggedExpectedCommittedState::EmptyDatabase { .. }
        )
    ));
    assert_eq!(serde_json::to_value(&state)?, expected);

    let seeded = json!({
        "kind": "empty-database",
        "normalized": { "tableCounts": { "firstTable": 0 } },
        "syntheticRows": [{ "fixtureOrdinal": 1 }],
        "tables": ["firstTable"],
    });
    assert!(serde_json::from_value::<NormalRouteSequenceExpectedCommittedState>(seeded).is_err());
    Ok(())
}

#[test]
fn normal_route_sequence_table_documents_state_keeps_raw_rows() -> anyhow::Result<()> {
    let expected = json!({
        "kind": "table-documents",
        "normalized": {
            "documents": [{
                "_creationTime": { "kind": "creation-time" },
                "_id": { "kind": "stable-document-id" },
                "enabled": false,
                "groupId": "",
                "kind": "channel",
            }],
            "tableCounts": { "telegramGroups": 1 },
        },
        "documents": [{
            "enabled": false,
            "groupId": "",
            "kind": "channel",
        }],
        "table": "telegramGroups",
    });
    let state =
        serde_json::from_value::<NormalRouteSequenceExpectedCommittedState>(expected.clone())?;
    assert_eq!(serde_json::to_value(&state)?, expected);
    let NormalRouteSequenceExpectedCommittedState::Tagged(
        NormalRouteSequenceTaggedExpectedCommittedState::TableDocuments {
            documents,
            normalized,
            ..
        },
    ) = state
    else {
        anyhow::bail!("table-documents state selected the wrong committed-state variant");
    };
    assert_eq!(
        documents,
        expected["documents"].as_array().unwrap().as_slice()
    );
    assert_eq!(normalized, expected["normalized"]);

    let table: TableName = "telegramGroups".parse()?;
    let normalized = normal_route_table_documents_expected_normalized(
        &table,
        &[json!({
            "enabled": false,
            "groupId": "",
            "kind": "channel",
        })],
    )?;
    assert_eq!(normalized, expected["normalized"]);
    assert!(normal_route_table_documents_expected_normalized(
        &table,
        &[json!({ "_id": "forbidden" })],
    )
    .is_err());
    Ok(())
}

async fn run_normal_registry_route_sequence_test(
    rt: ProdRuntime,
    expectation: NormalRouteSequenceTestExpectation,
) -> anyhow::Result<()> {
    let (database, runner, fixture) = normal_route_sequence_phase("setup", async {
        let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
        let database = new_database(rt.clone(), Arc::clone(&persistence)).await?;
        let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
        let modules = normal_route_sequence_source_modules(&expectation)?;
        let source_package_bytes = std::fs::read(&expectation.source.package.path)
            .context("failed to read normal route sequence source package")?;
        anyhow::ensure!(
            source_package_bytes.len() == expectation.source.package.size,
            "normal route sequence source package size differs from its authenticated identity"
        );
        let source_package_sha256 = normal_route_sha256(
            &expectation.source.package.sha256,
            "normal route sequence source-package file identity",
        )?;
        let expected_source_package_sha256 = normal_route_sha256(
            &expectation.source.source_package_sha256,
            "normal route sequence source-package identity",
        )?;
        let runtime_content_sha256 = normal_route_sha256(
            &expectation.source.source_package_runtime_content_sha256,
            "normal route sequence source-package runtime-content identity",
        )?;
        anyhow::ensure!(
            source_package_sha256 == expected_source_package_sha256
                && Sha256::hash(&source_package_bytes) == source_package_sha256,
            "normal route sequence source package differs from its authenticated identity"
        );
        let source_package_size = source_package_bytes.len();
        let runtime_generation = normal_route_runtime_generation(
            &expectation.deployment_sha256,
            &expectation.generation_manifest_sha256,
            &expectation.generation_sha256,
        )?;
        let node_executor_pool_topology =
            normal_route_source_bundle_topology(&expectation.source.bundle)?;
        let mut source_package_upload = storage.start_upload().await?;
        source_package_upload
            .write(Bytes::from(source_package_bytes))
            .await?;
        let storage_key = source_package_upload.complete().await?;
        let fixture = install_normal_route_sequence_fixture(
            &database,
            &expectation,
            &modules,
            SourcePackage {
                storage_key,
                sha256: source_package_sha256,
                runtime_content_sha256: Some(runtime_content_sha256),
                runtime_generation: Some(runtime_generation),
                external_deps_package_id: None,
                package_size: PackageSize {
                    zipped_size_bytes: source_package_size,
                    unzipped_size_bytes: 0,
                },
                node_version: None,
                node_executor_pool_topology,
            },
        )
        .await?;

        let module_loader = Arc::new(NormalRouteModuleLoader {
            modules: Arc::new(modules),
        });
        let runner = new_runner(
            rt,
            database.clone(),
            persistence,
            Arc::clone(&storage),
            module_loader,
            // Every sequence step must enter the function runner. Otherwise
            // the final A query can be served from the application query cache
            // and cannot prove post-mutation retained-runtime execution.
            QueryCache::new(0),
            normal_route_test_key_broker(&expectation.deployment_sha256)?,
        )
        .await?;
        Ok((database, runner, fixture))
    })
    .await?;
    let hooks = StaticHermesGateTestHooks::new(0, 0);
    configure_normal_route_execution_fuel(&hooks)?;
    let hooks_guard = install_static_hermes_gate_test_hooks(&hooks)?;
    let mut query_journals = Vec::with_capacity(expectation.sequence.steps.len());
    let mut results = Vec::with_capacity(expectation.sequence.steps.len());
    let mut invocation_time_results = Vec::new();
    let user_document_id = match &fixture {
        NormalRouteSequenceFixtureState::ApplicationProbePatch { user_document_id }
        | NormalRouteSequenceFixtureState::EmptyTables {
            user_document_id, ..
        }
        | NormalRouteSequenceFixtureState::EmptyDatabase {
            user_document_id, ..
        }
        | NormalRouteSequenceFixtureState::TableDocuments {
            user_document_id, ..
        } => *user_document_id,
    };
    for (step_index, step) in expectation.sequence.steps.iter().enumerate() {
        let module_name = step
            .route
            .runtime_module_path
            .strip_suffix(".js")
            .context("normal route sequence module path must end in .js")?;
        let path = PublicFunctionPath::RootExport(
            format!("{module_name}:{}", step.route.export_name).parse()?,
        );
        let identity = match &expectation.sequence.invocation_identity {
            Some(NormalRouteSequenceInvocationIdentity::SeededUser { .. }) => {
                match (step.route.udf_kind.as_str(), step.route.visibility.as_str()) {
                    ("query", "public") => Identity::user(normal_route_user_identity(
                        user_document_id
                            .context("normal route sequence omitted its seeded user")?
                            .encode(),
                    )?),
                    ("mutation", "internal") => Identity::system(),
                    _ => anyhow::bail!(
                        "seeded-user normal route sequence selected an unsupported route"
                    ),
                }
            },
            None => Identity::system(),
        };
        let phase = if expectation.sequence.kind == "convex-wasm-normal-route-sequence-v1" {
            ["invoke-a-first", "invoke-b", "invoke-a-final"][step_index]
        } else {
            "invoke-step"
        };
        let invocation = normal_route_sequence_phase(
            phase,
            run_normal_route_invocation(
                &runner,
                &database,
                path,
                identity,
                &step.route.udf_kind,
                step.arguments.clone(),
            ),
        )
        .await;
        let (result, journal) = match invocation {
            Ok(output) => output,
            Err(error) => {
                eprintln!(
                    "normal route sequence phase {phase} failed; generated execution phases: \
                     {:?}; generated execution errors: {:?}",
                    hooks.generated_execution_phases(),
                    hooks.execution_errors(),
                );
                return Err(error);
            },
        };
        if step.expected_result
            == json!({
                "configured": false,
                "serverObservedAt": { "kind": "invocation-time" },
            })
        {
            invocation_time_results.push(
                result
                    .get("serverObservedAt")
                    .and_then(JsonValue::as_f64)
                    .context("normal route sequence invocation-time result omitted its marker")?,
            );
        }
        results.push(normalize_normal_route_sequence_result(
            &result,
            &step.expected_result,
        )?);
        query_journals.push(journal);
    }
    if expectation.sequence.kind == "convex-wasm-normal-route-sequence-v1" {
        anyhow::ensure!(
            invocation_time_results.is_empty()
                || (invocation_time_results.len() == 2
                    && invocation_time_results[1] > invocation_time_results[0]),
            "normal route sequence final A did not execute after its first invocation"
        );
    }
    let committed_state = normal_route_sequence_phase("post-state", async {
        let committed_state = match (&fixture, &expectation.sequence.expected_committed_state) {
            (
                NormalRouteSequenceFixtureState::ApplicationProbePatch { .. },
                NormalRouteSequenceExpectedCommittedState::ApplicationProbePatch {
                    application_table,
                    scheduled_function_address,
                    ..
                },
            ) => {
                normal_route_application_probe_patch_state(
                    &database,
                    &application_table.parse()?,
                    scheduled_function_address,
                )
                .await?
                .normalized
            },
            (
                NormalRouteSequenceFixtureState::EmptyTables {
                    result_fields,
                    seeded_document_ids,
                    ..
                },
                NormalRouteSequenceExpectedCommittedState::Tagged(
                    NormalRouteSequenceTaggedExpectedCommittedState::EmptyTables { .. },
                ),
            ) => {
                normal_route_collect_delete_post_state(
                    &database,
                    result_fields,
                    seeded_document_ids,
                )
                .await?
            },
            (
                NormalRouteSequenceFixtureState::EmptyDatabase { tables, .. },
                NormalRouteSequenceExpectedCommittedState::Tagged(
                    NormalRouteSequenceTaggedExpectedCommittedState::EmptyDatabase { .. },
                ),
            ) => normal_route_empty_database_post_state(&database, tables).await?,
            (
                NormalRouteSequenceFixtureState::TableDocuments { table, .. },
                NormalRouteSequenceExpectedCommittedState::Tagged(
                    NormalRouteSequenceTaggedExpectedCommittedState::TableDocuments {
                        documents,
                        ..
                    },
                ),
            ) => normal_route_table_documents_post_state(&database, table, documents.len()).await?,
            _ => anyhow::bail!(
                "normal route sequence fixture state differs from its committed-state variant"
            ),
        };
        Ok(committed_state)
    })
    .await?;
    let expected_committed_state = match &expectation.sequence.expected_committed_state {
        NormalRouteSequenceExpectedCommittedState::ApplicationProbePatch { normalized, .. }
        | NormalRouteSequenceExpectedCommittedState::Tagged(
            NormalRouteSequenceTaggedExpectedCommittedState::EmptyTables { normalized, .. },
        )
        | NormalRouteSequenceExpectedCommittedState::Tagged(
            NormalRouteSequenceTaggedExpectedCommittedState::EmptyDatabase { normalized, .. },
        )
        | NormalRouteSequenceExpectedCommittedState::Tagged(
            NormalRouteSequenceTaggedExpectedCommittedState::TableDocuments { normalized, .. },
        ) => normalized,
    };
    anyhow::ensure!(
        &committed_state == expected_committed_state,
        "normal route sequence committed state differs from its expectation"
    );

    let runtime_ids = hooks.generated_runtime_ids();
    let instance_ids = hooks.instance_ids();
    let runtime_invocations = hooks.generated_invocations();
    let pool_hits = hooks.generated_pool_hits();
    let pool_misses = hooks.generated_pool_misses();
    let capability_operation_stages = hooks
        .guest_native_completion_stages()
        .into_iter()
        .filter(|stage| stage.starts_with("start:"))
        .collect::<Vec<_>>();
    let expected_capability_operation_stages = expectation
        .sequence
        .expected_capability_stages
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let database_query_starts = hooks.database_query_starts();
    let reads_completed = hooks.read_completed();
    let reads_cancelled = hooks.read_cancelled();
    let read_documents = hooks.read_documents();
    let read_bytes = hooks.read_bytes();
    let read_intervals = hooks.read_intervals();
    let transaction_drops = hooks.transaction_drops();
    let execution_errors = hooks.execution_errors();
    let execution_phases = hooks.generated_execution_phases();
    let expected_unique_runtime_count = expectation
        .sequence
        .steps
        .iter()
        .map(|step| step.package_key.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let expected_pool_misses = expected_unique_runtime_count;
    let expected_pool_hits = expectation.sequence.steps.len() - expected_pool_misses;
    let mut runtime_by_package = BTreeMap::new();
    let runtime_reuse_is_valid = runtime_invocations
        .iter()
        .zip(&expectation.sequence.steps)
        .all(
            |(observation, step)| match runtime_by_package.get(&step.package_key) {
                Some((runtime_id, memory_slot_id)) => {
                    !observation.runtime_was_created
                        && observation.runtime_id == *runtime_id
                        && observation.memory_slot_id == *memory_slot_id
                },
                None => {
                    runtime_by_package.insert(
                        step.package_key.clone(),
                        (observation.runtime_id, observation.memory_slot_id),
                    );
                    observation.runtime_was_created
                },
            },
        );
    match expectation.lane {
        NormalRouteTestLane::Wasm => anyhow::ensure!(
            runtime_ids.len() == expectation.sequence.steps.len()
                && instance_ids.len() == expectation.sequence.steps.len()
                && runtime_invocations.len() == expectation.sequence.steps.len()
                && execution_phases.len() == expectation.sequence.steps.len()
                && execution_phases
                    .iter()
                    .zip(&runtime_invocations)
                    .all(|(phase, invocation)| {
                        phase.fresh_runtime == invocation.runtime_was_created
                            && normal_route_execution_phase_is_valid(phase)
                    })
                && runtime_reuse_is_valid
                && runtime_by_package.len() == expected_unique_runtime_count
                && runtime_by_package
                    .values()
                    .map(|(runtime_id, _)| runtime_id)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == expected_unique_runtime_count
                && runtime_by_package
                    .values()
                    .map(|(_, memory_slot_id)| memory_slot_id)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == expected_unique_runtime_count
                && !runtime_invocations[0].prior_capability_rejected
                && runtime_invocations
                    .iter()
                    .skip(1)
                    .all(|observation| observation.prior_capability_rejected)
                && runtime_invocations
                    .iter()
                    .map(|observation| observation.invocation_id)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == expectation.sequence.steps.len()
                && runtime_invocations
                    .iter()
                    .map(|observation| observation.capability_identity)
                    .collect::<BTreeSet<_>>()
                    .len()
                    == expectation.sequence.steps.len()
                && runtime_invocations.iter().all(|observation| {
                    observation.serialized_aot_module_loaded
                        && observation.capability_revoked
                        && observation.forged_capability_rejected
                        && observation.revoked_capability_rejected
                        && observation.opaque_live_handles == 0
                        && observation.opaque_current_bytes == 0
                        && !observation.runtime_reuse_contaminated
                })
                && runtime_invocations
                    .iter()
                    .zip(&expectation.sequence.steps)
                    .all(|(observation, step)| {
                        observation.deployment_sha256.as_deref()
                            == Some(expectation.deployment_sha256.as_str())
                            && observation.package_key == step.package_key
                            && observation.entry_id.as_deref() == Some(step.entry_id.as_str())
                            && observation.entry_selector_id.as_deref()
                                == Some(step.entry_selector_id.as_str())
                    })
                && pool_hits == expected_pool_hits
                && pool_misses == expected_pool_misses
                && capability_operation_stages == expected_capability_operation_stages
                && reads_cancelled == 0
                && transaction_drops == 0
                && execution_errors.is_empty(),
            "normal route sequence did not preserve authenticated retained routing and clean \
             invocation authority: runtimes={runtime_ids:?}, invocations={runtime_invocations:?}, \
             pool_hits={pool_hits}, pool_misses={pool_misses}, \
             stages={capability_operation_stages:?}, reads_cancelled={reads_cancelled}, \
             transaction_drops={transaction_drops}, execution_errors={execution_errors:?}, \
             execution_phases={execution_phases:?}"
        ),
        NormalRouteTestLane::V8 => anyhow::ensure!(
            runtime_ids.is_empty()
                && instance_ids.is_empty()
                && runtime_invocations.is_empty()
                && execution_phases.is_empty()
                && pool_hits == 0
                && pool_misses == 0
                && capability_operation_stages.is_empty()
                && database_query_starts == 0
                && reads_completed == 0
                && read_documents == 0
                && read_bytes == 0
                && read_intervals == 0
                && reads_cancelled == 0
                && transaction_drops == 0
                && execution_errors.is_empty(),
            "V8 normal route sequence unexpectedly produced generated-runtime state: \
             runtimes={runtime_ids:?}, invocations={runtime_invocations:?}, \
             pool_hits={pool_hits}, pool_misses={pool_misses}, \
             stages={capability_operation_stages:?}, \
             database_query_starts={database_query_starts}, reads_completed={reads_completed}, \
             read_documents={read_documents}, read_bytes={read_bytes}, \
             read_intervals={read_intervals}, reads_cancelled={reads_cancelled}, \
             transaction_drops={transaction_drops}, execution_errors={execution_errors:?}"
        ),
        NormalRouteTestLane::Shadow => {
            anyhow::bail!("query-shadow normal-route test does not support sequence fixtures")
        },
    }
    let unique_runtime_count = runtime_ids.iter().copied().collect::<BTreeSet<_>>().len();
    let unique_memory_slot_count = runtime_invocations
        .iter()
        .map(|observation| observation.memory_slot_id)
        .collect::<BTreeSet<_>>()
        .len();
    let measured_fresh_runtime_admissions = runtime_invocations
        .iter()
        .filter(|observation| observation.runtime_was_created)
        .count();
    let cleaned_up_instances = normal_route_sequence_phase(
        "cleanup-instances",
        cleanup_static_hermes_test_instances::<ProdRuntime>(),
    )
    .await?;
    let teardowns_after_cleanup = hooks.teardowns();
    let store_drops_after_cleanup = hooks.store_drops();
    let expected_cleanup_count = match expectation.lane {
        NormalRouteTestLane::Wasm => expected_unique_runtime_count,
        NormalRouteTestLane::V8 => 0,
        NormalRouteTestLane::Shadow => {
            anyhow::bail!("query-shadow normal-route test does not support sequence fixtures")
        },
    };
    anyhow::ensure!(
        cleaned_up_instances == expected_cleanup_count
            && teardowns_after_cleanup == expected_cleanup_count
            && store_drops_after_cleanup == instance_ids.len(),
        "normal route sequence cleanup did not match its lane: \
         cleaned_up_instances={cleaned_up_instances}, \
         teardowns_after_cleanup={teardowns_after_cleanup}, \
         store_drops_after_cleanup={store_drops_after_cleanup}"
    );
    let runtime_invocations = runtime_invocations
        .into_iter()
        .map(TryInto::try_into)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let execution_phases = execution_phases
        .into_iter()
        .map(TryInto::try_into)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let (observation_kind, observation_schema_version) =
        normal_route_sequence_observation_protocol(&expectation.sequence.kind)?;
    let observation = NormalRouteSequenceObservation {
        kind: observation_kind,
        schema_version: observation_schema_version,
        deployment_sha256: expectation.deployment_sha256,
        generation_sha256: expectation.generation_sha256,
        lane: expectation.lane,
        sequence: expectation.sequence,
        invocation_count: results.len(),
        query_journals,
        results,
        normalized_committed_state: committed_state,
        database_query_starts,
        reads_completed,
        read_documents,
        read_bytes,
        read_intervals,
        runtime_observation_count: runtime_ids.len(),
        unique_runtime_count,
        unique_memory_slot_count,
        runtime_invocations,
        execution_phases,
        pool_hits,
        pool_misses,
        measured_pool_hits: pool_hits,
        measured_pool_misses: pool_misses,
        measured_fresh_runtime_admissions,
        capability_operation_stages,
        cleaned_up_instances,
        store_drops_after_cleanup,
        teardowns_after_cleanup,
    };
    write_normal_route_observation(&expectation.observation_path, &observation)?;
    drop(hooks_guard);
    normal_route_sequence_phase("cleanup-database", database.shutdown()).await?;
    Ok(())
}

async fn run_normal_registry_mutation_shadow_test(
    rt: ProdRuntime,
    expectation: NormalRouteTestExpectation,
    cancel_after_writes: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        expectation.lane == NormalRouteTestLane::Shadow
            && expectation.route.udf_kind == "mutation"
            && expectation.route.visibility == "internal",
        "mutation shadow requires the shadow lane and an authenticated internal mutation route"
    );
    let _ = normal_route_shadow_primary_wasm_routing_enabled(UdfType::Mutation)?;
    let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
    let database = new_database(rt.clone(), Arc::clone(&persistence)).await?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
    let modules = normal_route_source_modules(&expectation)?;
    let source_package = upload_normal_route_source_package(&expectation, &storage).await?;
    let runner = new_runner(
        rt.clone(),
        database.clone(),
        persistence,
        Arc::clone(&storage),
        Arc::new(NormalRouteModuleLoader {
            modules: Arc::new(modules.clone()),
        }),
        QueryCache::new(0),
        normal_route_test_key_broker(&expectation.deployment_sha256)?,
    )
    .await?;
    let paired_lane_schema = match &expectation.setup {
        NormalRouteTestSetup::PairedLaneV1 { .. } => Some(
            normal_route_authenticated_schema(&runner, &expectation, &storage, &source_package)
                .await?,
        ),
        _ => None,
    };
    let fixture = install_normal_route_fixture(
        &database,
        &expectation,
        paired_lane_schema.as_deref(),
        &modules,
        source_package,
        &storage,
    )
    .await?;
    let (invocation_arguments, expected_result) = match &fixture {
        NormalRouteFixtureState::ObjectPatchDeleteV1(fixture) => {
            restore_normal_route_object_patch_delete_fields(&database, fixture).await?;
            (
                fixture.invocation_arguments.clone(),
                Some(
                    fixture
                        .expected_results
                        .first()
                        .context("mutation shadow fixture has no expected result")?
                        .clone(),
                ),
            )
        },
        NormalRouteFixtureState::SeededDocumentsMutationV1(fixture) => (
            fixture.invocation_arguments.clone(),
            Some(fixture.expected_result.clone()),
        ),
        NormalRouteFixtureState::PairedLaneV1(fixture) => (
            fixture
                .invocations
                .first()
                .context("paired mutation-shadow fixture has no invocation")?
                .clone(),
            None,
        ),
        _ => anyhow::bail!(
            "mutation shadow requires an existing-document or paired mutation fixture"
        ),
    };
    let hooks = StaticHermesGateTestHooks::new(0, usize::from(cancel_after_writes));
    configure_normal_route_execution_fuel(&hooks)?;
    configure_normal_route_generated_runtime_execution_time(NormalRouteTestLane::Shadow, &hooks)?;
    let cleanup_control = if cancel_after_writes {
        let permits = runner.mutation_shadow.detached_cleanup_test_permits()?;
        let (entered, release) = hooks.hold_next_runtime_destruction();
        let initial_state = normal_route_paired_lane_post_state(&database, BTreeMap::new()).await?;
        Some((permits, entered, release, initial_state))
    } else {
        None
    };
    let hooks_guard = install_static_hermes_gate_test_hooks(&hooks)?;
    let caller = FunctionCaller::Cron;
    let request_context = RequestContext::new_for_system_request(RequestId::new());
    let context = ExecutionContext::new(request_context, &caller);
    let module_name = expectation
        .route
        .runtime_module_path
        .strip_suffix(".js")
        .context("mutation-shadow runtime module path must end in .js")?;
    let path = PublicFunctionPath::RootExport(
        format!("{module_name}:{}", expectation.route.export_name).parse()?,
    );
    let transaction = database.begin(Identity::system()).await?;
    let initial_write_commits = database.write_commits_since_load();
    let mutation = runner.run_mutation_no_udf_log(
        transaction,
        path.clone(),
        SerializedArgs::from_args(vec![invocation_arguments.clone()])?,
        caller.allowed_visibility(),
        context,
        None,
        SchedulerDependencyClass::Independent,
    );
    let (transaction, outcome) = if cancel_after_writes {
        let completion = tokio::time::timeout(NORMAL_ROUTE_TEST_PHASE_TIMEOUT, async {
            let primary = async {
                let (transaction, outcome) = mutation.await?;
                anyhow::ensure!(
                    outcome.result.is_ok(),
                    "V8 fixture returned a developer error before shadow guest completion"
                );
                Ok::<_, anyhow::Error>((transaction, outcome))
            };
            tokio::try_join!(primary, hooks.wait_for_guest_completion())
        })
        .await;
        let (primary, instance_id) = match completion {
            Ok(result) => result?,
            Err(deadline) => {
                // Selection can itself have failed. Keep evidence-reader errors
                // in the diagnostic rather than replacing the original deadline.
                let evidence = runner.function_log.query_shadow_evidence(
                    UdfType::Mutation,
                    None,
                    NORMAL_ROUTE_SHADOW_PAGE_LIMIT,
                );
                return Err(deadline).with_context(|| {
                    format!(
                        "mutation shadow did not reach guest completion alongside V8; \
                         evidence={evidence:?}; {}; phases={:?}; execution_errors={}",
                        normal_route_shadow_hook_summary(&hooks),
                        hooks.generated_execution_phases(),
                        hooks.execution_errors().len(),
                    )
                });
            },
        };
        assert_eq!(hooks.instance_ids(), vec![instance_id]);
        primary
    } else {
        mutation.await?
    };
    let expected_result_matches = match &expected_result {
        Some(expected_result) => outcome
            .result
            .as_ref()
            .is_ok_and(|value| &value.json_value() == expected_result),
        None => outcome.result.is_ok(),
    };
    anyhow::ensure!(
        expected_result_matches,
        "mutation shadow V8 primary returned an unexpected result"
    );
    if let Some((permits, cleanup_entered, cleanup_release, (initial_state, initial_markers))) =
        cleanup_control
    {
        // The generated lane has already produced its write set, but remains
        // suspended before finalization. V8 can commit without awaiting it.
        assert!(hooks.guest_native_completion_stages().iter().any(|stage| {
            matches!(
                *stage,
                "start:db-insert" | "start:db-patch" | "start:db-replace" | "start:db-delete"
            )
        }));
        let (before_primary_commit, _) =
            normal_route_paired_lane_post_state(&database, initial_markers).await?;
        assert_eq!(
            before_primary_commit, initial_state,
            "shadow writes became visible before V8 committed"
        );
        assert_eq!(database.write_commits_since_load(), initial_write_commits);
        assert!(
            transaction
                .writes()
                .as_flat()?
                .coalesced_writes()
                .next()
                .is_some(),
            "V8 mutation did not produce a write set"
        );
        database
            .commit_with_write_source(transaction, "static_hermes_cancelled_shadow_v8_commit")
            .await?;
        let (committed_state, document_markers) =
            normal_route_paired_lane_post_state(&database, BTreeMap::new()).await?;
        if let NormalRouteFixtureState::ObjectPatchDeleteV1(fixture) = &fixture {
            assert_eq!(
                normal_route_object_patch_delete_post_state(&database, fixture).await?,
                fixture.normalized_committed_state
            );
        }
        tokio::time::timeout(NORMAL_ROUTE_TEST_PHASE_TIMEOUT, cleanup_entered)
            .await
            .context("cancelled shadow did not reach real worker destruction")??;
        let route_key = QueryShadowRouteKey::new(
            &expectation.generation_sha256,
            &expectation.routing.route_id,
        )?;
        tokio::time::timeout(NORMAL_ROUTE_TEST_PHASE_TIMEOUT, async {
            loop {
                let (_, summary) = normal_route_shadow_evidence(
                    &runner.function_log,
                    UdfType::Mutation,
                    &route_key,
                )?;
                if let Some(summary) = summary
                    && summary.terminal_count == 1
                {
                    assert_eq!(summary.terminal_counts.timeout, 1);
                    assert_eq!(summary.admitted_count, 1);
                    assert_eq!(summary.completed_count, 0);
                    assert_eq!(summary.mismatch_count, 0);
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("cancelled shadow did not publish its timeout")??;
        assert_eq!(permits.available_permits(), 0);
        assert!(Arc::clone(&permits).try_acquire_owned().is_err());
        assert_eq!(hooks.store_drops(), 0);
        hooks
            .release_guest_completion(hooks.instance_ids()[0])
            .expect_err("cancelled shadow retained its guest completion receiver");
        cleanup_release
            .send(())
            .map_err(|_| anyhow::anyhow!("real worker abandoned destruction barrier"))?;
        tokio::time::timeout(NORMAL_ROUTE_TEST_PHASE_TIMEOUT, async {
            loop {
                if permits.available_permits() == 1 && hooks.store_drops() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("cancelled worker did not destroy its Store and release admission")?;
        assert_eq!(hooks.teardowns(), 1);
        assert_eq!(hooks.terminal_cancellations(), 1);
        let cancelled = hooks.generated_invocations();
        assert_eq!(cancelled.len(), 1);
        assert!(cancelled[0].cancelled);
        assert!(cancelled[0].capability_revoked);
        assert!(!cancelled[0].retain_instance);
        // Start events also occur for rejected writes. Inspect the actual
        // transaction retained across the post-guest cancellation boundary.
        assert!(cancelled[0].transaction_write_count > 0);
        assert!(!cancelled[0].developer_error_reported);
        assert!(hooks.execution_errors().is_empty());
        assert_eq!(cancelled[0].opaque_live_handles, 0);
        assert_eq!(cancelled[0].opaque_current_bytes, 0);
        assert_eq!(
            cleanup_static_hermes_test_instances::<ProdRuntime>().await?,
            0
        );
        let (after_cleanup, _) =
            normal_route_paired_lane_post_state(&database, document_markers).await?;
        assert_eq!(after_cleanup, committed_state);
        // A duplicate idempotent commit can leave document values unchanged.
        assert_eq!(
            database.write_commits_since_load(),
            initial_write_commits + 1
        );

        let context = ExecutionContext::new(
            RequestContext::new_for_system_request(RequestId::new()),
            &caller,
        );
        let recovery_arguments = match &fixture {
            NormalRouteFixtureState::PairedLaneV1(fixture) => fixture
                .invocations
                .get(1)
                .context("paired mutation-shadow fixture has no recovery invocation")?
                .clone(),
            _ => invocation_arguments,
        };
        let (recovery_transaction, recovery) = runner
            .run_mutation_no_udf_log(
                database.begin(Identity::system()).await?,
                path,
                SerializedArgs::from_args(vec![recovery_arguments])?,
                caller.allowed_visibility(),
                context,
                None,
                SchedulerDependencyClass::Independent,
            )
            .await?;
        assert!(recovery.result.is_ok(), "recovery V8 mutation failed");
        tokio::time::timeout(NORMAL_ROUTE_TEST_PHASE_TIMEOUT, async {
            loop {
                let (_, summary) = normal_route_shadow_evidence(
                    &runner.function_log,
                    UdfType::Mutation,
                    &route_key,
                )?;
                if let Some(summary) = summary
                    && summary.completed_count == 1
                {
                    assert_eq!(summary.admitted_count, 2);
                    assert_eq!(summary.terminal_count, 1);
                    assert_eq!(summary.mismatch_count, 0);
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("fresh shadow did not compare successfully after cancelled worker cleanup")??;
        assert_eq!(
            database.write_commits_since_load(),
            initial_write_commits + 1
        );
        let expected_write_commits =
            initial_write_commits + 1 + usize::from(!recovery_transaction.is_readonly());
        database
            .commit_with_write_source(
                recovery_transaction,
                "static_hermes_cancelled_shadow_recovery_commit",
            )
            .await?;
        let runtimes = hooks.generated_runtime_ids();
        assert_eq!(runtimes.len(), 2);
        assert_ne!(runtimes[0], runtimes[1]);
        let invocations = hooks.generated_invocations();
        assert_eq!(invocations.len(), 2);
        assert!(invocations[1].runtime_was_created);
        assert!(!invocations[1].cancelled);
        assert!(invocations[1].capability_revoked);
        assert_eq!(invocations[1].opaque_live_handles, 0);
        assert_eq!(invocations[1].opaque_current_bytes, 0);
        assert_eq!(permits.available_permits(), 1);
        assert_eq!(
            cleanup_static_hermes_test_instances::<ProdRuntime>().await?,
            1
        );
        assert_eq!(hooks.teardowns(), 2);
        assert_eq!(hooks.store_drops(), 2);
        assert_eq!(database.write_commits_since_load(), expected_write_commits);
        drop(hooks_guard);
        database.shutdown().await?;
        return Ok(());
    }
    let mutation_shadow = wait_for_normal_route_shadow_observation(
        &runner.function_log,
        &expectation,
        &hooks,
        UdfType::Mutation,
        1,
    )
    .await?;
    anyhow::ensure!(
        mutation_shadow.generation_sha256 == expectation.generation_sha256
            && mutation_shadow.route_id == expectation.routing.route_id
            && !mutation_shadow.primary_wasm_routing_enabled
            && mutation_shadow.attempt_count == 1
            && mutation_shadow.admitted_count == 1
            && mutation_shadow.completed_comparison_count == 1
            && mutation_shadow.mismatch_count == 0
            && mutation_shadow.terminal_count == 0,
        "mutation shadow did not retain one successful V8-primary/Wasm-shadow comparison"
    );
    assert_eq!(database.write_commits_since_load(), initial_write_commits);
    database
        .commit_with_write_source(
            transaction,
            "static_hermes_production_mutation_shadow_v8_commit",
        )
        .await?;
    let normalized_state = match &fixture {
        NormalRouteFixtureState::ObjectPatchDeleteV1(fixture) => {
            Some(normal_route_object_patch_delete_post_state(&database, fixture).await?)
        },
        NormalRouteFixtureState::SeededDocumentsMutationV1(_) => None,
        NormalRouteFixtureState::PairedLaneV1(_) => None,
        _ => unreachable!("mutation-shadow fixture was restricted above"),
    };
    let database_write_start_count = hooks
        .guest_native_completion_stages()
        .into_iter()
        .filter(|stage| {
            matches!(
                *stage,
                "start:db-insert" | "start:db-patch" | "start:db-replace" | "start:db-delete"
            )
        })
        .count();
    let generated_invocations = hooks.generated_invocations();
    let database_query_starts = hooks.database_query_starts();
    let reads_cancelled = hooks.read_cancelled();
    let execution_errors = hooks.execution_errors();
    let (expected_state_matches, expected_read_accounting_matches) = match &fixture {
        NormalRouteFixtureState::ObjectPatchDeleteV1(fixture) => (
            normalized_state.as_ref() == Some(&fixture.normalized_committed_state),
            database_query_starts == 0 && reads_cancelled == 0,
        ),
        NormalRouteFixtureState::SeededDocumentsMutationV1(_) => {
            (true, database_query_starts > 0 && reads_cancelled == 0)
        },
        NormalRouteFixtureState::PairedLaneV1(_) => (true, true),
        _ => unreachable!("mutation-shadow fixture was restricted above"),
    };
    anyhow::ensure!(
        expected_state_matches
            && expected_read_accounting_matches
            && database_write_start_count > 0
            && generated_invocations.len() == 1
            && generated_invocations[0].transaction_write_count > 0
            && execution_errors.is_empty(),
        "mutation shadow did not preserve one V8 commit and one discarded Wasm write set"
    );
    let cleaned_up_instances = cleanup_static_hermes_test_instances::<ProdRuntime>().await?;
    anyhow::ensure!(
        cleaned_up_instances == 1,
        "mutation shadow did not clean up its retained Wasm runtime"
    );
    // An extra idempotent shadow commit can preserve the expected document values.
    assert_eq!(
        database.write_commits_since_load(),
        initial_write_commits + 1
    );
    drop(hooks_guard);
    database.shutdown().await?;
    Ok(())
}

#[test]
#[ignore = "requires an operator-provided generated deployment and artifact cache"]
fn static_hermes_real_deployment_reaches_function_runner_import_boundary() -> anyhow::Result<()> {
    let _gate_test_guard = GATE_TEST_LOCK.lock().expect("gate test lock was poisoned");
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "static_hermes_real_deployment_function_runner",
        run_real_deployment_function_runner_test(rt),
    )
}

#[test]
#[ignore = "requires an operator-provided runtime registry route fixture"]
fn static_hermes_registry_route_reuses_application_function_runner() -> anyhow::Result<()> {
    let _gate_test_guard = GATE_TEST_LOCK.lock().expect("gate test lock was poisoned");
    // This ignored test builds a deeply nested async future from its external
    // fixture, which exceeds Rust's default test-thread stack.
    let test_thread = std::thread::Builder::new()
        .name("static-hermes-normal-route-test".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| -> anyhow::Result<()> {
            let expectation_json = read_normal_route_test_expectation()?;
            let expectation_value = serde_json::from_str::<JsonValue>(&expectation_json)
                .context("failed to parse normal route test expectation")?;
            let tokio = ProdRuntime::init_tokio()?;
            let rt = ProdRuntime::new(&tokio);
            let block_rt = rt.clone();
            if expectation_value.get("kind")
                == Some(&JsonValue::String(
                    "convex-wasm-normal-route-paired-batch-v1".to_owned(),
                ))
            {
                let batch: NormalRoutePairedLaneBatchExpectation =
                    serde_json::from_str(&expectation_json)
                        .context("failed to parse normal route test expectation")?;
                block_rt.block_on(
                    "static_hermes_normal_registry_paired_lane_batch",
                    run_normal_registry_paired_lane_batch_test(rt, batch),
                )
            } else if expectation_value.get("sequence").is_some() {
                let expectation: NormalRouteSequenceTestExpectation =
                    serde_json::from_str(&expectation_json)
                        .context("failed to parse normal route test expectation")?;
                block_rt.block_on(
                    "static_hermes_normal_registry_application_function_runner_sequence",
                    run_normal_registry_route_sequence_test(rt, expectation),
                )
            } else {
                let expectation: NormalRouteTestExpectation =
                    serde_json::from_str(&expectation_json)
                        .context("failed to parse normal route test expectation")?;
                block_rt.block_on(
                    "static_hermes_normal_registry_application_function_runner",
                    run_normal_registry_route_test(rt, expectation),
                )
            }
        })
        .context("failed to spawn static Hermes normal route test thread")?;
    match test_thread.join() {
        Ok(result) => result,
        Err(_) => anyhow::bail!("static Hermes normal route test thread panicked"),
    }
}

#[test]
#[ignore = "requires an operator-provided existing-document mutation registry and fixture"]
fn static_hermes_registry_mutation_shadow_commits_v8_only() -> anyhow::Result<()> {
    run_registry_mutation_shadow_test_thread(false)
}

#[test]
#[ignore = "requires an authenticated mutation registry fixture, full mutation sampling and one \
            shadow permit"]
fn static_hermes_registry_mutation_shadow_timeout_holds_worker_budget() -> anyhow::Result<()> {
    run_registry_mutation_shadow_test_thread(true)
}

fn run_registry_mutation_shadow_test_thread(cancel_after_writes: bool) -> anyhow::Result<()> {
    let _gate_test_guard = GATE_TEST_LOCK.lock().expect("gate test lock was poisoned");
    let test_thread = std::thread::Builder::new()
        .name("static-hermes-mutation-shadow-test".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || -> anyhow::Result<()> {
            let expectation: NormalRouteTestExpectation =
                serde_json::from_str(&read_normal_route_test_expectation()?)
                    .context("failed to parse mutation-shadow route expectation")?;
            let tokio = ProdRuntime::init_tokio()?;
            let rt = ProdRuntime::new(&tokio);
            let block_rt = rt.clone();
            block_rt.block_on("static_hermes_normal_registry_mutation_shadow", async {
                tokio::time::timeout(
                    Duration::from_secs(600),
                    run_normal_registry_mutation_shadow_test(rt, expectation, cancel_after_writes),
                )
                .await
                .context("mutation-shadow test exceeded its overall deadline")?
            })
        })
        .context("failed to spawn static Hermes mutation-shadow test thread")?;
    match test_thread.join() {
        Ok(result) => result,
        Err(_) => anyhow::bail!("static Hermes mutation-shadow test thread panicked"),
    }
}
