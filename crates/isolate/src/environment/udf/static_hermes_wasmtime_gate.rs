use std::{
    any::Any,
    collections::{
        BTreeMap,
        BTreeSet,
        HashSet,
        VecDeque,
    },
    error::Error as StdError,
    fmt,
    fs::{
        self,
        File,
        OpenOptions,
    },
    future::Future,
    io::{
        Read,
        Write,
    },
    path::{
        Path,
        PathBuf,
    },
    sync::{
        atomic::{
            AtomicBool,
            AtomicU64,
            AtomicU8,
            AtomicUsize,
            Ordering,
        },
        Arc,
        LazyLock,
        OnceLock,
        Weak,
    },
    task::Poll,
    time::{
        Duration,
        Instant,
    },
};
#[cfg(test)]
use std::{
    hash::{
        Hash,
        Hasher,
    },
    time::SystemTime,
};

use anyhow::Context as _;
#[cfg(test)]
use common::knobs::MAX_SYSCALL_BATCH_SIZE;
use common::{
    audit_log_lines::{
        AuditLogLine,
        AuditLogLines,
    },
    components::{
        CanonicalizedComponentFunctionPath,
        CanonicalizedComponentModulePath,
        ComponentId,
        ComponentPath,
        PublicFunctionPath,
        Reference,
        ResolvedComponentFunctionPath,
        Resource,
    },
    execution_context::ExecutionContext,
    knobs::{
        static_hermes_wasm_primary_directions,
        validate_static_hermes_verifier_directions,
        APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS,
        APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_V8_SHADOW_BPS,
        APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS,
        APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_V8_SHADOW_BPS,
        DATABASE_UDF_SYSTEM_TIMEOUT,
        FUNCTION_MAX_RESULT_SIZE,
        LOCAL_BACKEND_MEMORY_PRESSURE_ENTER_HEADROOM_BYTES,
        LOCAL_BACKEND_MEMORY_PRESSURE_EXIT_HEADROOM_BYTES,
        MAX_REACTOR_CALL_DEPTH,
    },
    log_lines::{
        LogLevel,
        LogLine,
        LogLines,
    },
    query::{
        Cursor,
        Query,
    },
    query_journal::QueryJournal,
    runtime::{
        tokio_spawn_blocking,
        Runtime,
        UnixTimestamp,
    },
    types::{
        AllowedVisibility,
        DeploymentMetadata,
        EnvVarName,
        EnvVarValue,
        UdfType,
    },
    version::Version,
};
#[cfg(test)]
use common::{
    bootstrap_model::index::{
        database_index::IndexedFields,
        IndexMetadata,
    },
    execution_context::RequestContext,
    persistence::Persistence,
    runtime::new_unlimited_rate_limiter,
    shutdown::ShutdownSignal,
    types::{
        ConvexOrigin,
        DeploymentClass,
        EnvironmentVariable,
        FunctionCaller,
        IndexDescriptor,
        IndexName,
        ModuleEnvironment,
        ObjectKey,
    },
    RequestId,
};
use database::{
    query::{
        query_batch_next,
        TableFilter,
    },
    DeveloperQuery,
    FunctionExecutionSize,
    SnoopedTransaction,
    Transaction,
    TransactionLimits,
};
#[cfg(test)]
use database::{
    Database,
    IndexModel,
    SystemMetadataModel,
    UserFacingModel,
};
use errors::{
    ErrorCode,
    ErrorMetadata,
    ErrorMetadataAnyhowExt as _,
};
#[cfg(test)]
use file_storage::TransactionalFileStorage;
#[cfg(test)]
use indexing::index_cache::IndexCache;
use keybroker::{
    DeploymentOp,
    FunctionRunnerKeyBroker,
};
#[cfg(test)]
use keybroker::{
    Identity,
    KeyBroker,
    UserIdentity,
};
use metrics::{
    log_counter_with_labels,
    log_distribution,
    log_gauge,
    register_convex_counter,
    register_convex_gauge,
    register_convex_histogram,
    StaticMetricLabel,
};
use model::{
    components::{
        handles::FunctionHandlesModel,
        ComponentsModel,
    },
    environment_variables::EnvironmentVariablesModel,
    file_storage::{
        types::FileStorageEntry,
        BatchKey,
        FileStorageId,
    },
    modules::ModuleModel,
    source_packages::SourcePackageModel,
    virtual_system_mapping,
};
#[cfg(test)]
use model::{
    initialize_application_system_tables,
    modules::{
        function_validators::{
            ArgsValidator,
            ReturnsValidator,
        },
        module_versions::{
            AnalyzedFunction,
            AnalyzedModule,
            ModuleSource,
            Visibility,
        },
    },
    source_packages::types::{
        PackageSize,
        SourcePackage,
    },
};
use rand::{
    Rng,
    SeedableRng,
};
use rand_chacha::ChaCha12Rng;
#[cfg(test)]
use runtime::prod::ProdRuntime;
#[cfg(test)]
use search::searcher::SearcherStub;
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
};
use serde_json::{
    json,
    Value as JsonValue,
};
use sha2::{
    Digest,
    Sha256,
};
#[cfg(test)]
use sqlite::SqlitePersistence;
#[cfg(test)]
use storage::{
    LocalDirStorage,
    Storage,
};
use sync_types::{
    types::SerializedArgs,
    udf_path::CanonicalizedUdfPath,
    CanonicalizedModulePath,
};
use tokio::sync::{
    mpsc,
    oneshot,
};
#[cfg(test)]
use udf::HostOperation;
use udf::{
    validation::{
        validate_schedule_args,
        PendingArgsPolicy,
        ValidatedPathAndArgs,
    },
    FunctionOutcome,
    HostOperationErrorV1,
    HostOperationTraceEntryHandle,
    LogicalHostOperation,
    LogicalHostOperationStatus,
    NestedUdfOutcome,
    UdfOutcome,
};
#[cfg(test)]
use value::{
    id_v6::DeveloperDocumentId,
    obj,
    sha256::Sha256Digest,
    ConvexObject,
    ResolvedDocumentId,
    MAX_COMMIT_TS,
};
use value::{
    ConvexArray,
    ConvexValue,
    JsonPackedValue,
    PendingValue,
    TableName,
    TableNamespace,
    TableNumber,
    TabletIdAndTableNumber,
};
#[cfg(any(test, feature = "testing"))]
use wasm_encoder::{
    BlockType as EncodedBlockType,
    CodeSection as EncodedCodeSection,
    ConstExpr as EncodedConstExpr,
    DataSection as EncodedDataSection,
    EntityType as EncodedEntityType,
    ExportKind as EncodedExportKind,
    ExportSection as EncodedExportSection,
    Function as EncodedFunction,
    FunctionSection as EncodedFunctionSection,
    GlobalSection as EncodedGlobalSection,
    GlobalType as EncodedGlobalType,
    ImportSection as EncodedImportSection,
    Instruction as EncodedInstruction,
    MemorySection as EncodedMemorySection,
    MemoryType as EncodedMemoryType,
    Module as EncodedModule,
    RefType as EncodedRefType,
    TableSection as EncodedTableSection,
    TableType as EncodedTableType,
    TypeSection as EncodedTypeSection,
    ValType as EncodedValType,
};
use wasmtime::{
    Caller,
    Config,
    Engine,
    Error as WasmtimeError,
    Extern,
    ExternType,
    HeapType,
    Linker,
    Memory,
    Module,
    Mutability,
    Precompiled,
    ProfilingStrategy,
    Store,
    StoreLimitsBuilder,
    Trap,
    TypedFunc,
    UpdateDeadline,
    ValType,
    WasmBacktrace,
};

#[cfg(test)]
const SUCCESS_FUEL: u64 = 2_000_000_000;

#[cfg(test)]
mod canonical_module_graph_execution_tests;

#[cfg(any(test, feature = "testing"))]
use super::generated_wasm_memory::GeneratedMemoryTestSnapshot;
#[cfg(any(test, feature = "testing"))]
use super::wasm_udf_manifest::MANIFEST_SCHEMA_VERSION;
#[cfg(test)]
use super::wasm_udf_package::{
    canonical_json_sha256,
    load_capability_entry_package_for_compatibility_test,
    CapabilityGraphExportReference,
};
use super::{
    async_syscall::{
        developer_document_to_json,
        AsyncSyscallBatch,
        NestedUdfType,
        QueryId,
    },
    generated_wasm_memory::{
        record_module_cache_event,
        record_runtime_destruction,
        AdmissionRejection,
        AdmissionWaitReason,
        BackendPressure,
        FunctionMemoryIdentity,
        FunctionMemoryLineage,
        GeneratedGuestMemoryLimit,
        GeneratedMemoryController,
        GeneratedMemoryExecutionRole,
        GeneratedMemoryPolicy,
        GeneratedMemoryRouteContext,
        GeneratedMemoryRouteIdentity,
        GeneratedModuleMemoryCharge,
        GeneratedSlotId,
        GeneratedStoreLimiter,
        IdleEvictionReason,
        IdleEvictionTrigger,
        InvocationMemoryPermit,
        ModuleMemoryAdmissionError,
        RuntimeDestructionOutcome,
        TerminalMemoryOutcome,
        TryAdmissionError,
    },
    module_graph_registry::{
        AuthenticatedModuleGraphCatalog,
        AuthenticatedModuleGraphExecution,
        AuthenticatedModuleGraphExecutionModule,
        GraphInitialization,
        GraphModuleContract,
        GraphModuleLayout,
        GraphModuleProvider,
        RegistryUse,
        RuntimeRegistryAdmission,
    },
    syscall::{
        parse_query_stream_request,
        syscall_impl,
        SyscallProviderInternal,
    },
    wasm_udf_abi::{
        GuestCapabilityRequestCodec,
        GuestNativeValueCodec,
        OpaqueHandle,
        OpaqueJsonKind,
        OpaqueValue,
        OpaqueValueError,
        OpaqueValueKind,
        OpaqueValueTable,
        INVOCATION_UNIX_TIMESTAMP_MS_IMPORT,
        MAX_EXACT_JAVASCRIPT_INTEGER,
    },
    wasm_udf_manifest::{
        permitted_conditional_convex_imports,
        EffectExecutionMode,
        ImportedOperationDescriptor,
        PlatformLimits,
        QueryConstraintOperator,
        QueryOrder,
        QueryTerminal,
        RuntimeCompatibility,
        UdfKind as ManifestUdfKind,
        ValueMode,
        WasmUdfExecutionManifest,
        WasmUdfExecutionPolicy,
        COHORT_MANIFEST_SCHEMA_VERSION,
        COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION,
        CONDITIONAL_CONVEX_IMPORTS,
        EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION,
        OPAQUE_VALUE_ABI_VERSION,
    },
    wasm_udf_package::{
        load_runtime_registry_current,
        load_runtime_registry_generation,
        load_runtime_registry_generation_with_retained_graphs,
        load_runtime_registry_source_catalog,
        load_validated_package,
        runtime_registry_is_empty,
        snapshot_serialized_module_bytes,
        validate_serialized_module_snapshot_directory,
        AuthenticatedModuleGraphRouteMaterial,
        CapabilityGraphExternType,
        CapabilityGraphImportProvider,
        CapabilityGraphLayout,
        CapabilityGraphModuleContract,
        CapabilityGraphValueType,
        DeployedRuntimeIdentity,
        DeploymentExportPackage,
        DeploymentExportRouting,
        DeploymentRouteLease,
        DeploymentRuntimeEntry,
        ValidatedCapabilityGraph,
        ValidatedCapabilityGraphDependency,
        ValidatedCapabilityGraphModule,
        ValidatedDeploymentManifest,
        ValidatedRuntimeRegistryGeneration,
        ValidatedRuntimeRegistryGenerationDescriptor,
        ValidatedWasmUdfPackage,
        ValidatedWasmUdfPackageIdentity,
    },
    DatabaseUdfEnvironment,
};
#[cfg(test)]
use crate::module_cache::{
    ModuleCache,
    V8ModuleSource,
};
use crate::{
    client::{
        CancellationSignal,
        EnvironmentData,
        HostSecretValue,
        IsolateClient,
        UdfCallback,
        UdfRequest,
    },
    context_cache::{
        ContextCache,
        ContextReadSet,
    },
    environment::helpers::{
        remove_rejected_before_execution,
        MAX_LOG_LINES,
    },
    metrics::log_run_udf,
    termination::{
        IsolateTerminationReason,
        TerminationReason,
    },
    timeout::{
        FunctionExecutionTime,
        PauseReason,
        Timeout,
        SYSTEM_TIMEOUT_ERROR_MESSAGE,
    },
    ConcurrencyLimiter,
    ConcurrencyPermit,
};

mod async_operations;
mod capability_bridge;
mod graph_runtime;
mod host_imports;
mod host_state;
mod module_contract;
mod query_operations;
mod routed_module_cache;
mod routing_runtime;
mod runtime_lifecycle;
mod source_keyed_deployment_catalog;
#[cfg(any(test, feature = "testing"))]
mod test_support;

#[cfg(test)]
use self::async_operations::{
    run_generated_capability_sync_operation,
    start_generated_capability_operation,
};
#[cfg(any(test, feature = "testing"))]
use self::capability_bridge::{
    CapabilityBridgeError,
    InvocationCapabilityIdentity,
};
#[cfg(test)]
use self::graph_runtime::GeneratedEmscriptenGraphInstances;
#[cfg(any(test, feature = "testing"))]
pub(crate) use self::routing_runtime::resolve_compatibility_shadow_route;
#[cfg(test)]
pub(crate) use self::routing_runtime::run_routed;
pub(crate) use self::routing_runtime::{
    active_query_shadow_registry,
    generated_memory_statistics,
    initialize_memory_pressure_sampler,
    initialize_route_configuration,
    prepare_routed_invocation,
    resolve_route,
    resolve_shadow_route,
    run_prepared_routed,
    select_route_for_invocation,
};
pub use self::routing_runtime::{
    source_keyed_paired_deployment_guard_enabled,
    source_keyed_runtime_generation_identity,
    source_keyed_runtime_readiness,
    PreparedStaticHermesWasmtimeInvocation,
    SourceKeyedRuntimeGenerationIdentity,
    SourceKeyedRuntimeReadiness,
    StaticHermesQueryShadowCapacityUnavailable,
    StaticHermesQueryShadowRegistry,
    StaticHermesWasmModuleLoadingFailure,
    StaticHermesWasmtimePreparationMode,
    StaticHermesWasmtimeRouteHandle,
};
#[cfg(any(test, feature = "testing"))]
pub use self::runtime_lifecycle::cleanup_static_hermes_test_instances;
#[cfg(test)]
use self::test_support::{
    generated_async_batch_test_routed_module,
    generated_capability_async_operation_test_module,
    generated_database_normalize_id_test_module,
    generated_database_normalize_id_test_routed_module,
    generated_fixed_entry_prepare_test_module,
    generated_guest_promise_database_get_test_module,
    generated_guest_promise_host_operation_error_test_module,
    generated_guest_promise_test_manifest_with_runtime_limits,
    generated_host_secret_verify_test_module,
    generated_host_secret_verify_test_routed_module,
    generated_memory_test_module,
    generated_memory_test_routed_module,
    generated_memory_test_routed_module_with_generation,
    generated_routed_test_state,
    generated_schema_five_test_manifest_with_runtime_limits,
    generated_selected_entry_dispatch_fault_test_module,
    generated_sequential_query_collect_test_routed_module,
    generated_test_execution_context,
    generated_test_manifest,
    generated_test_manifest_with_limits,
    generated_test_manifest_with_limits_and_fuel,
    generated_test_manifest_with_runtime_limits,
    generated_test_manifest_with_runtime_limits_and_schema,
    generated_test_manifest_with_timeout,
    generated_test_module,
    generated_test_routed_module,
    generated_test_routed_module_from_bytes,
    generated_test_state,
    generated_test_state_with_rng_seed,
    GeneratedAsyncBatchTestOperation,
    GeneratedDatabaseWriteHandleFault,
    GeneratedDatabaseWriteKind,
    GeneratedDatabaseWriteTestOperation,
    GeneratedFunctionHandleTestOperation,
    GeneratedGuestPromiseHostOperationErrorMode,
    GeneratedMemoryTestOperation,
    GeneratedRoutedTestMemorySetup,
    GeneratedSelectedEntryDispatchFault,
    GeneratedTestMemorySetup,
    GeneratedTestOperation,
    RetainedGeneratedTestRuntime,
};
#[cfg(any(test, feature = "testing"))]
use self::test_support::{
    generated_occ_test_routed_module,
    generated_query_terminal_pair_routed_module,
};
#[cfg(feature = "testing")]
pub(crate) use self::test_support::{
    generated_query_shadow_observation_module,
    query_shadow_observation_imported_operations,
};
use self::{
    async_operations::{
        add_generated_async_operation_imports,
        authorize_generated_capability,
        capability_function_address_syscall_args,
        parse_query_next,
        record_generated_developer_error,
        run_generated_async_syscall,
        run_generated_direct_async_batch,
        AsyncOperationHandle,
        AsyncOperationState,
        QueryNext,
    },
    capability_bridge::{
        decode_function_address,
        InvocationCapabilityBridge,
    },
    graph_runtime::{
        instantiate_generated_emscripten_graph,
        instantiate_generated_graph_dependencies,
        run_generated_fresh_initialization,
        validate_generated_graph_instance_layout,
        GeneratedFreshInitialization,
    },
    host_imports::{
        add_generated_convex_imports,
        add_wasi_imports,
    },
    host_state::*,
    module_contract::*,
    query_operations::generated_query_syscall_args,
    routed_module_cache::*,
    routing_runtime::*,
    runtime_lifecycle::*,
};
pub use super::generated_wasm_memory::{
    StaticHermesGeneratedMemoryRouteStatistics,
    StaticHermesGeneratedMemoryStatistics,
};

#[cfg(test)]
mod application_capability_entry_tests;
#[cfg(test)]
mod artifact_registry_contract_tests;
#[cfg(test)]
use self::artifact_registry_contract_tests::{
    generated_module_cache_test_controller,
    NativeCapabilityTestAbiReport,
    RealCapabilityEntryPackageTestExpectation,
    NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_ARM_EXPORT,
    NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_TRACE_EXPORT,
};
#[cfg(test)]
mod console_logging_tests;
#[cfg(test)]
mod crypto_subtle_digest_tests;
#[cfg(test)]
mod generic_query_application_tests;
#[cfg(test)]
mod invocation_timestamp_tests;
#[cfg(test)]
mod performance_now_tests;
#[cfg(test)]
mod process_env_application_tests;
#[cfg(test)]
mod query_operations_tests;
#[cfg(test)]
mod randomness_tests;
#[cfg(test)]
mod runtime_acceptance_tests;
#[cfg(test)]
mod runtime_hotpath_benchmark;
#[cfg(test)]
mod wasi_clock_tests;

#[cfg(test)]
use self::runtime_acceptance_tests::{
    backend_gate_document,
    execute_generated_sequential_query_collect_test_module,
    execute_generated_test_module,
    execute_reusable_generated_async_batch_test_module,
    insert_document,
    insert_document_with_sequence,
    new_test_database,
    retain_generated_test_runtime,
};

const ENABLED_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_ENABLED";
const WASI_CLOCK_ID_REALTIME: i32 = 0;
const WASI_CLOCK_ID_MONOTONIC: i32 = 1;
const WASI_ERRNO_NOTSUP: i32 = 58;
const NANOS_PER_MILLISECOND: u64 = 1_000_000;
const GENERATED_INSTANCE_HARD_CEILING_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_INSTANCE_HARD_CEILING";
const GENERATED_MEMORY_SOFT_BUDGET_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_SOFT_BUDGET_BYTES";
const GENERATED_MEMORY_HARD_BUDGET_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_HARD_BUDGET_BYTES";
const GENERATED_MEMORY_SAFETY_RESERVE_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_SAFETY_RESERVE_BYTES";
const GENERATED_MEMORY_UNATTRIBUTED_PER_SLOT_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_UNATTRIBUTED_PER_SLOT_BYTES";
const GENERATED_MEMORY_COLD_FORECAST_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_COLD_FORECAST_BYTES";
const GENERATED_MEMORY_WARM_IDLE_TARGET_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_WARM_IDLE_TARGET";
const GENERATED_MEMORY_MAX_IDLE_SECONDS_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_MAX_IDLE_SECONDS";
const GENERATED_MEMORY_ADMISSION_WAIT_MILLISECONDS_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERATED_MEMORY_ADMISSION_WAIT_MILLISECONDS";
const ACTIVE_WASM_CPU_CONCURRENCY_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_ACTIVE_CPU_CONCURRENCY";
const PACKAGE_DIRECTORY_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_PACKAGE_DIRECTORY";
const SERIALIZED_MODULE_SNAPSHOT_DIRECTORY_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_SERIALIZED_MODULE_SNAPSHOT_DIRECTORY";
const ARTIFACT_CACHE_ROOT_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_ARTIFACT_CACHE_ROOT";
const DEPLOYMENT_MANIFEST_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_DEPLOYMENT_MANIFEST";
const RUNTIME_REGISTRY_ROOT_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_RUNTIME_REGISTRY_ROOT";
const SOURCE_KEYED_DEPLOYMENT_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_SOURCE_KEYED_DEPLOYMENT";
const LIFECYCLE_BARRIER_DIRECTORY_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_LIFECYCLE_BARRIER_DIRECTORY";
const LIFECYCLE_BARRIER_UDF_PATH_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_LIFECYCLE_BARRIER_UDF_PATH";
const REUSE_INSTANCES_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_REUSE_INSTANCES";
const HOST_SECRET_SELECTORS_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_HOST_SECRET_SELECTORS";
const ROUTED_UDF_PATH_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_UDF_PATH";
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_RESULT_BYTES: usize = 16 * 1024 * 1024;
const GENERATED_ENGINE_CONFIGURATION_SHA256: &str =
    "bebac200e042d7c2678b574ba75645f1bf06bb9295982d46184384b0376874b6";
// Wasmtime exposes compatibility hashing only when a compiler backend is
// linked. Package generation, destination AOT reconstruction, and the contract
// tests use Cranelift; supplied artifacts still rely on Wasmtime's
// deserialization compatibility check.
pub(super) const GENERATED_ENGINE_COMPATIBILITY_SHA256: &str =
    "89880ad9de64f2bc9e218bf19bac3fb9963b23c56f02b302f1bf1f69d48606f8";
const GENERATED_LIFECYCLE_BARRIER_ARRIVAL_FILE: &str = "arrived";
const GENERATED_LIFECYCLE_BARRIER_RELEASE_FILE: &str = "release";
const GENERATED_LIFECYCLE_BARRIER_POLL_INTERVAL: Duration = Duration::from_millis(10);
const GENERATED_LIFECYCLE_BARRIER_PROTOCOL: &str = "convex-generated-wasm-lifecycle-barrier-v1";
const GENERATED_TARGET_CPU: &str = "baseline";
const GENERATED_TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";
pub(super) const GENERATED_WASMTIME_REVISION: &str = "7ad2e732ab9ca8665d3cdd91f9c395315eeafc81";
const GENERATED_RUNTIME_DESTROY_TIMEOUT: Duration = Duration::from_millis(250);
const GENERATED_EPOCH_TICK_INTERVAL: Duration = Duration::from_millis(10);
const GENERATED_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(5);
const GENERATED_INITIALIZATION_FUEL: u64 = 20_000_000_000;
const GENERATED_POOL_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
const GENERATED_MEMORY_ADMISSION_POLICY_VERSION: u32 = 1;
const MAX_GENERATED_FIELD_NAME_BYTES: usize = 4 * 1024;
const MAX_HOST_SECRET_INPUT_BYTES: usize = 8 * 1024;
const MAX_CRYPTO_SUBTLE_DIGEST_INPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_CRYPTO_GET_RANDOM_VALUES_BYTES: usize = 65_536;
const SHA256_DIGEST_BYTES: usize = 32;
const RANDOM_UUID_BYTES: usize = 36;
const RANDOM_UUID_ENTROPY_BYTES: u64 = 16;
const MATH_RANDOM_ENTROPY_BYTES: u64 = 8;
// Host digest work bypasses Wasm instrumentation, so charge deterministic
// fuel in proportion to the input processed through the guest ABI.
const SHA256_DIGEST_BASE_FUEL: u64 = 1;
const SHA256_DIGEST_FUEL_PER_INPUT_BYTE: u64 = 1;
const RANDOM_BASE_FUEL: u64 = 1;
const RANDOM_FUEL_PER_ENTROPY_BYTE: u64 = 1;

static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
static GENERATED_POOL_MAINTENANCE_STARTED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
#[derive(Default)]
struct Sha256CompatibilityHasher(Sha256);

#[cfg(test)]
impl Sha256CompatibilityHasher {
    fn finalize(self) -> String {
        format!("{:x}", self.0.finalize())
    }
}

#[cfg(test)]
impl Hasher for Sha256CompatibilityHasher {
    fn finish(&self) -> u64 {
        let digest = self.0.clone().finalize();
        u64::from_le_bytes(
            digest[..8]
                .try_into()
                .expect("SHA-256 digest prefix has eight bytes"),
        )
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn write_u8(&mut self, value: u8) {
        self.write(&value.to_le_bytes());
    }

    fn write_u16(&mut self, value: u16) {
        self.write(&value.to_le_bytes());
    }

    fn write_u32(&mut self, value: u32) {
        self.write(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.write(&value.to_le_bytes());
    }

    fn write_u128(&mut self, value: u128) {
        self.write(&value.to_le_bytes());
    }

    fn write_usize(&mut self, value: usize) {
        self.write(
            &u64::try_from(value)
                .expect("generated Wasm compatibility hashing requires at most 64-bit usize")
                .to_le_bytes(),
        );
    }

    fn write_i8(&mut self, value: i8) {
        self.write(&value.to_le_bytes());
    }

    fn write_i16(&mut self, value: i16) {
        self.write(&value.to_le_bytes());
    }

    fn write_i32(&mut self, value: i32) {
        self.write(&value.to_le_bytes());
    }

    fn write_i64(&mut self, value: i64) {
        self.write(&value.to_le_bytes());
    }

    fn write_i128(&mut self, value: i128) {
        self.write(&value.to_le_bytes());
    }

    fn write_isize(&mut self, value: isize) {
        self.write(
            &i64::try_from(value)
                .expect("generated Wasm compatibility hashing requires at most 64-bit isize")
                .to_le_bytes(),
        );
    }
}

#[cfg(test)]
fn calculated_precompile_compatibility_sha256(engine: &Engine) -> String {
    let mut hasher = Sha256CompatibilityHasher::default();
    engine.precompile_compatibility_hash().hash(&mut hasher);
    hasher.finalize()
}
#[cfg(any(test, feature = "testing"))]
static NEXT_GENERATED_REUSABLE_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

register_convex_histogram!(
    STATIC_HERMES_WASMTIME_GATE_PHASE_SECONDS,
    "Duration of a Static Hermes/Wasmtime gate phase",
    &["phase"]
);
const GATE_PHASE_AUTHENTICATED_GRAPH_SHARED_AOT_RESOLUTION: &str =
    "authenticated_graph_shared_aot_resolution";
const GATE_PHASE_INVOCATION_PREPARATION_SOURCE_AUTHENTICATION: &str =
    "invocation_preparation_source_authentication";
const GATE_PHASE_RETAINED_POOL_ADMISSION_CHECKOUT: &str = "retained_pool_admission_checkout";
const GATE_PHASE_ROUTE_LEAF_AOT_RESOLUTION: &str = "route_leaf_aot_resolution";
const GATE_PHASE_FRESH_STORE_GRAPH_INSTANTIATION: &str = "fresh_store_graph_instantiation";
const GATE_PHASE_WARM_STORE_CHECKOUT_RESET: &str = "warm_store_checkout_reset";
const GATE_PHASE_RETAINED_CONTEXT_READ_SET_VALIDATION: &str =
    "retained_context_read_set_validation";
const GATE_PHASE_GUEST_INITIALIZATION_SELECTED_ENTRY_PREPARATION: &str =
    "guest_initialization_selected_entry_preparation";
const GATE_PHASE_HANDLER_EXPORT_EXECUTION: &str = "handler_export_execution";
const GATE_PHASE_RESULT_FINALIZATION_CLEANUP: &str = "result_finalization_cleanup";
register_convex_histogram!(
    STATIC_HERMES_WASMTIME_GATE_GUEST_FUEL_OPERATIONS,
    "Fuel consumed by a Static Hermes/Wasmtime guest phase",
    &["phase"]
);
register_convex_counter!(
    STATIC_HERMES_WASMTIME_GATE_INSTANCE_POOL_TOTAL,
    "Static Hermes/Wasmtime instance pool events",
    &["event"]
);
register_convex_counter!(
    STATIC_HERMES_WASMTIME_GATE_REGISTRY_RELOAD_TOTAL,
    "Generated Wasm runtime registry reload events",
    &["event"]
);
register_convex_gauge!(
    STATIC_HERMES_WASMTIME_GATE_ACTIVE_CPU_CAPACITY_INFO,
    "Configured shared active CPU capacity for Static Hermes/Wasmtime"
);
register_convex_counter!(
    STATIC_HERMES_WASMTIME_GATE_ACTIVE_CPU_ADMISSION_TOTAL,
    "Static Hermes/Wasmtime active CPU admission outcomes",
    &["outcome"]
);
register_convex_histogram!(
    STATIC_HERMES_WASMTIME_GATE_ACTIVE_CPU_ADMISSION_WAIT_SECONDS,
    "Time Static Hermes/Wasmtime waits for shared active CPU capacity"
);

fn log_instance_pool_event(event: &'static str) {
    log_counter_with_labels(
        &STATIC_HERMES_WASMTIME_GATE_INSTANCE_POOL_TOTAL,
        1,
        vec![StaticMetricLabel::new("event", event)],
    );
}

fn log_registry_reload_event(event: &'static str) {
    log_counter_with_labels(
        &STATIC_HERMES_WASMTIME_GATE_REGISTRY_RELOAD_TOTAL,
        1,
        vec![StaticMetricLabel::new("event", event)],
    );
}

fn log_active_wasm_cpu_capacity(capacity: usize) {
    log_gauge(
        &STATIC_HERMES_WASMTIME_GATE_ACTIVE_CPU_CAPACITY_INFO,
        capacity as f64,
    );
}

fn log_active_wasm_cpu_admission(outcome: &'static str, wait: Duration) {
    log_counter_with_labels(
        &STATIC_HERMES_WASMTIME_GATE_ACTIVE_CPU_ADMISSION_TOTAL,
        1,
        vec![StaticMetricLabel::new("outcome", outcome)],
    );
    log_distribution(
        &STATIC_HERMES_WASMTIME_GATE_ACTIVE_CPU_ADMISSION_WAIT_SECONDS,
        wait.as_secs_f64(),
    );
}

fn log_gate_phase(phase: &'static str, duration: Duration) {
    // Phase marks are on the guest hot path. `with_label_values` performs the
    // fixed one-label lookup directly; routing through the generic helper
    // would allocate a label vector and a temporary map for every mark.
    STATIC_HERMES_WASMTIME_GATE_PHASE_SECONDS
        .with_label_values(&[phase])
        .observe(duration.as_secs_f64());
}

pub(super) struct GatePhaseTimer {
    phase: &'static str,
    started: Instant,
}

impl GatePhaseTimer {
    pub(super) fn new(phase: &'static str) -> Self {
        Self {
            phase,
            started: Instant::now(),
        }
    }
}

impl Drop for GatePhaseTimer {
    fn drop(&mut self) {
        log_gate_phase(self.phase, self.started.elapsed());
    }
}

fn guest_phase(phase: i32) -> Result<&'static str, WasmtimeError> {
    match phase {
        1 => Ok("guest_runtime_init"),
        2 => Ok("guest_unit_finalize"),
        3 => Ok("guest_runtime_done"),
        4 => Ok("guest_runtime_reuse"),
        10 => Ok("guest_unit_init_and_request_utf"),
        11 => Ok("guest_request_json_parse"),
        12 => Ok("guest_application"),
        13 => Ok("guest_result_json_stringify"),
        14 => Ok("guest_result_utf"),
        15 => Ok("guest_unit_init_and_typed_request"),
        16 => Ok("guest_opaque_application"),
        17 => Ok("guest_opaque_result_transfer"),
        _ => Err(WasmtimeError::new(HostInvariant)),
    }
}

#[derive(Debug)]
struct WasiExit(i32);

impl fmt::Display for WasiExit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "guest exited with status {}", self.0)
    }
}

impl StdError for WasiExit {}

#[derive(Debug)]
struct HostInvariant;

impl fmt::Display for HostInvariant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Static Hermes host ABI invariant failed")
    }
}

impl StdError for HostInvariant {}

/// Fixed Wasmtime trap code retained without the underlying error text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StaticHermesWasmTrapCode {
    StackOverflow,
    MemoryOutOfBounds,
    HeapMisaligned,
    TableOutOfBounds,
    IndirectCallToNull,
    BadSignature,
    IntegerOverflow,
    IntegerDivisionByZero,
    BadConversionToInteger,
    UnreachableCodeReached,
    Interrupt,
    OutOfFuel,
    AtomicWaitNonSharedMemory,
    NullReference,
    ArrayOutOfBounds,
    AllocationTooLarge,
    CastFailure,
    CannotEnterComponent,
    NoAsyncResult,
    UnhandledTag,
    ContinuationAlreadyConsumed,
    DisabledOpcode,
    Other,
}

impl StaticHermesWasmTrapCode {
    pub const fn diagnostic_key(self) -> &'static str {
        match self {
            Self::StackOverflow => "stack_overflow",
            Self::MemoryOutOfBounds => "memory_out_of_bounds",
            Self::HeapMisaligned => "heap_misaligned",
            Self::TableOutOfBounds => "table_out_of_bounds",
            Self::IndirectCallToNull => "indirect_call_to_null",
            Self::BadSignature => "bad_signature",
            Self::IntegerOverflow => "integer_overflow",
            Self::IntegerDivisionByZero => "integer_division_by_zero",
            Self::BadConversionToInteger => "bad_conversion_to_integer",
            Self::UnreachableCodeReached => "unreachable_code_reached",
            Self::Interrupt => "interrupt",
            Self::OutOfFuel => "out_of_fuel",
            Self::AtomicWaitNonSharedMemory => "atomic_wait_non_shared_memory",
            Self::NullReference => "null_reference",
            Self::ArrayOutOfBounds => "array_out_of_bounds",
            Self::AllocationTooLarge => "allocation_too_large",
            Self::CastFailure => "cast_failure",
            Self::CannotEnterComponent => "cannot_enter_component",
            Self::NoAsyncResult => "no_async_result",
            Self::UnhandledTag => "unhandled_tag",
            Self::ContinuationAlreadyConsumed => "continuation_already_consumed",
            Self::DisabledOpcode => "disabled_opcode",
            Self::Other => "other",
        }
    }
}

impl From<&Trap> for StaticHermesWasmTrapCode {
    fn from(trap: &Trap) -> Self {
        match trap {
            Trap::StackOverflow => Self::StackOverflow,
            Trap::MemoryOutOfBounds => Self::MemoryOutOfBounds,
            Trap::HeapMisaligned => Self::HeapMisaligned,
            Trap::TableOutOfBounds => Self::TableOutOfBounds,
            Trap::IndirectCallToNull => Self::IndirectCallToNull,
            Trap::BadSignature => Self::BadSignature,
            Trap::IntegerOverflow => Self::IntegerOverflow,
            Trap::IntegerDivisionByZero => Self::IntegerDivisionByZero,
            Trap::BadConversionToInteger => Self::BadConversionToInteger,
            Trap::UnreachableCodeReached => Self::UnreachableCodeReached,
            Trap::Interrupt => Self::Interrupt,
            Trap::OutOfFuel => Self::OutOfFuel,
            Trap::AtomicWaitNonSharedMemory => Self::AtomicWaitNonSharedMemory,
            Trap::NullReference => Self::NullReference,
            Trap::ArrayOutOfBounds => Self::ArrayOutOfBounds,
            Trap::AllocationTooLarge => Self::AllocationTooLarge,
            Trap::CastFailure => Self::CastFailure,
            Trap::CannotEnterComponent => Self::CannotEnterComponent,
            Trap::NoAsyncResult => Self::NoAsyncResult,
            Trap::UnhandledTag => Self::UnhandledTag,
            Trap::ContinuationAlreadyConsumed => Self::ContinuationAlreadyConsumed,
            Trap::DisabledOpcode => Self::DisabledOpcode,
            _ => Self::Other,
        }
    }
}

/// Authenticated module role for one retained Wasm trap frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StaticHermesWasmTrapModuleRole {
    Route,
    Base,
    Shared,
    Leaf,
    Dependency,
    Unknown,
}

/// One data-free frame from a Wasmtime guest backtrace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticHermesWasmTrapFrame {
    pub module_role: StaticHermesWasmTrapModuleRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_ordinal: Option<u32>,
    pub function_index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_offset: Option<u64>,
}

/// Bounded fuel state captured at the trap boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticHermesWasmTrapFuelDiagnostic {
    pub limit: u64,
    pub remaining: Option<u64>,
}

/// Bounded invocation resource state captured at the trap boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticHermesWasmTrapResourceDiagnostic {
    pub operation_count: u64,
    pub operation_limit: u64,
    pub value_handle_count: u64,
    pub value_handle_limit: u64,
    pub current_guest_bytes: u64,
    pub guest_byte_limit: u64,
    pub current_host_bytes: u64,
    pub host_byte_limit: u64,
}

/// One fixed, data-free logical host-operation outcome preceding a trap.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticHermesWasmTrapHostOperationDiagnostic {
    pub operation: &'static str,
    pub status: &'static str,
}

/// Closed classification for guest stderr retained at a trap boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StaticHermesWasmTrapStderrClassification {
    Empty,
    StaticHermesUncaughtException,
    HermesHeapOutOfMemory,
    Unclassified,
}

/// Data-free stderr evidence. Raw stderr is never retained.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticHermesWasmTrapStderrDiagnostic {
    pub classification: StaticHermesWasmTrapStderrClassification,
    pub byte_count: u64,
    pub sha256: String,
}

/// One bounded, data-free diagnostic capsule for a Wasmtime guest trap.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticHermesWasmTrapDiagnostic {
    code: StaticHermesWasmTrapCode,
    frames: Vec<StaticHermesWasmTrapFrame>,
    frames_truncated: bool,
    invocation_correlation: String,
    reused_instance: bool,
    fuel: StaticHermesWasmTrapFuelDiagnostic,
    resources: StaticHermesWasmTrapResourceDiagnostic,
    host_operation_trace_available: bool,
    host_operations: Vec<StaticHermesWasmTrapHostOperationDiagnostic>,
    host_operations_truncated: bool,
    stderr: StaticHermesWasmTrapStderrDiagnostic,
}

impl StaticHermesWasmTrapDiagnostic {
    pub fn new(
        code: StaticHermesWasmTrapCode,
        function_index: Option<u32>,
        function_offset: Option<u64>,
    ) -> Self {
        Self {
            code,
            frames: function_index
                .map(|function_index| StaticHermesWasmTrapFrame {
                    module_role: StaticHermesWasmTrapModuleRole::Unknown,
                    module_ordinal: None,
                    function_index,
                    function_offset,
                    module_offset: None,
                })
                .into_iter()
                .collect(),
            frames_truncated: false,
            invocation_correlation: "00000000-0000-4000-8000-000000000000".to_owned(),
            reused_instance: false,
            fuel: StaticHermesWasmTrapFuelDiagnostic {
                limit: 0,
                remaining: None,
            },
            resources: StaticHermesWasmTrapResourceDiagnostic {
                operation_count: 0,
                operation_limit: 0,
                value_handle_count: 0,
                value_handle_limit: 0,
                current_guest_bytes: 0,
                guest_byte_limit: 0,
                current_host_bytes: 0,
                host_byte_limit: 0,
            },
            host_operation_trace_available: false,
            host_operations: Vec::new(),
            host_operations_truncated: false,
            stderr: StaticHermesWasmTrapStderrDiagnostic {
                classification: StaticHermesWasmTrapStderrClassification::Empty,
                byte_count: 0,
                sha256: format!("{:x}", Sha256::digest([])),
            },
        }
    }

    fn from_capsule(
        code: StaticHermesWasmTrapCode,
        frames: Vec<StaticHermesWasmTrapFrame>,
        frames_truncated: bool,
        invocation_correlation: String,
        reused_instance: bool,
        fuel: StaticHermesWasmTrapFuelDiagnostic,
        resources: StaticHermesWasmTrapResourceDiagnostic,
        host_operation_trace_available: bool,
        host_operations: Vec<StaticHermesWasmTrapHostOperationDiagnostic>,
        host_operations_truncated: bool,
        stderr: StaticHermesWasmTrapStderrDiagnostic,
    ) -> Self {
        Self {
            code,
            frames,
            frames_truncated,
            invocation_correlation,
            reused_instance,
            fuel,
            resources,
            host_operation_trace_available,
            host_operations,
            host_operations_truncated,
            stderr,
        }
    }

    /// Returns the bounded stderr classification without exposing stderr bytes.
    pub fn stderr_classification(&self) -> StaticHermesWasmTrapStderrClassification {
        self.stderr.classification
    }
}

/// Data-free execution subphase retained when a Wasm shadow lane fails.
///
/// This marker is carried through `anyhow` only so the function runner can
/// publish a fixed diagnostic classification. It must never contain source
/// paths, arguments, identities, values, or underlying error text.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StaticHermesWasmExecutionFailure {
    #[error("Static Hermes Wasm runtime initialization failed")]
    RuntimeInitialization,
    #[error("Static Hermes Wasm initialization timed out")]
    InitializationTimeout,
    #[error("Static Hermes Wasm execution timed out")]
    ExecutionTimeout,
    #[error("Static Hermes Wasm instruction budget exhausted")]
    InstructionBudget,
    #[error("Static Hermes Wasm generated export dispatch failed")]
    GeneratedExportDispatch,
    #[error("Static Hermes Wasm guest memory limit exceeded")]
    GuestMemoryLimit,
    #[error("Static Hermes Wasm host-owned byte limit exceeded")]
    HostOwnedBytesLimit,
    #[error("Static Hermes Wasm opaque handle limit exceeded")]
    OpaqueHandleLimit,
    #[error("Static Hermes Wasm opaque handle space exhausted")]
    OpaqueHandleSpaceExhausted,
    #[error("Static Hermes Wasm aggregate memory limit exceeded")]
    AggregateMemoryLimit,
    #[error("Static Hermes Wasm operation limit exceeded")]
    OperationLimit,
    #[error("Static Hermes Wasm host ABI invariant failed")]
    HostAbiInvariant,
    #[error("Static Hermes Wasm trapped")]
    WasmtimeTrap {
        diagnostic: StaticHermesWasmTrapDiagnostic,
    },
    #[error("Static Hermes Wasm guest execution failed")]
    GuestExecution,
    #[error("Static Hermes Wasm result finalization failed")]
    ResultFinalization,
    #[error("Static Hermes Wasm runtime cleanup failed")]
    RuntimeCleanup,
}

impl From<&OpaqueValueError> for StaticHermesWasmExecutionFailure {
    fn from(error: &OpaqueValueError) -> Self {
        match error {
            OpaqueValueError::HostOwnedBytesLimitExceeded { .. } => Self::HostOwnedBytesLimit,
            OpaqueValueError::HandleLimitExceeded { .. } => Self::OpaqueHandleLimit,
            OpaqueValueError::HandleSpaceExhausted => Self::OpaqueHandleSpaceExhausted,
            OpaqueValueError::AggregateMemoryLimitExceeded => Self::AggregateMemoryLimit,
            OpaqueValueError::InvalidHandle
            | OpaqueValueError::KindMismatch { .. }
            | OpaqueValueError::JsonShapeMismatch { .. }
            | OpaqueValueError::NonFiniteNumber
            | OpaqueValueError::InvalidFieldName
            | OpaqueValueError::DuplicateObjectField { .. }
            | OpaqueValueError::ArrayIndexOutOfBounds
            | OpaqueValueError::FinalResultAlreadySet
            | OpaqueValueError::FinalResultMissing
            | OpaqueValueError::LeakedValues { .. } => Self::HostAbiInvariant,
        }
    }
}

#[cfg(test)]
#[test]
fn opaque_value_failures_have_bounded_execution_classifications() {
    let cases = [
        (
            OpaqueValueError::HostOwnedBytesLimitExceeded { maximum_bytes: 1 },
            StaticHermesWasmExecutionFailure::HostOwnedBytesLimit,
        ),
        (
            OpaqueValueError::HandleLimitExceeded { maximum: 1 },
            StaticHermesWasmExecutionFailure::OpaqueHandleLimit,
        ),
        (
            OpaqueValueError::HandleSpaceExhausted,
            StaticHermesWasmExecutionFailure::OpaqueHandleSpaceExhausted,
        ),
        (
            OpaqueValueError::AggregateMemoryLimitExceeded,
            StaticHermesWasmExecutionFailure::AggregateMemoryLimit,
        ),
        (
            OpaqueValueError::InvalidHandle,
            StaticHermesWasmExecutionFailure::HostAbiInvariant,
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(StaticHermesWasmExecutionFailure::from(&error), expected);
    }
}

/// Bounded, data-free detail for a generated-export dispatch failure.
///
/// This marker is carried through the `anyhow` chain so the function runner can
/// expose which fixed ABI boundary failed without retaining route identities,
/// arguments, values, or the underlying Wasmtime error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StaticHermesGeneratedExportDiagnostic {
    #[error("Static Hermes Wasm first selector trapped")]
    FirstSelectorTrap,
    #[error("Static Hermes Wasm first selector returned status {status}")]
    FirstSelectorRejected { status: i32 },
    #[error("Static Hermes Wasm selected-entry preparation trapped")]
    SelectedEntryPreparationTrap,
    #[error("Static Hermes Wasm selected-entry preparation returned status {status}")]
    SelectedEntryPreparationRejected { status: i32 },
    #[error("Static Hermes Wasm second selector trapped")]
    SecondSelectorTrap,
    #[error("Static Hermes Wasm second selector returned status {status}")]
    SecondSelectorRejected { status: i32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InvocationOutcome {
    Success,
    DeveloperError(String),
    InitializationTimeout,
    ActiveTimeout,
    FuelExhausted,
    SystemTimeout,
    SystemError,
}

#[derive(Default)]
struct GateMetrics {
    #[cfg(test)]
    async_operation_cancel_all_calls: AtomicUsize,
    #[cfg(test)]
    async_operations_abandoned: AtomicUsize,
    #[cfg(test)]
    formatter_initialization_failure_arms: AtomicUsize,
    #[cfg(test)]
    fresh_initialization_attempts: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    database_query_starts: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    execution_errors: parking_lot::Mutex<Vec<String>>,
    #[cfg(any(test, feature = "testing"))]
    read_completed: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    read_cancelled: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    store_drops: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    teardowns: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    transaction_drops: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    instances: parking_lot::Mutex<Vec<u64>>,
    #[cfg(any(test, feature = "testing"))]
    generated_runtimes: parking_lot::Mutex<Vec<u64>>,
    #[cfg(any(test, feature = "testing"))]
    guest_native_completion_stages: parking_lot::Mutex<Vec<&'static str>>,
    #[cfg(any(test, feature = "testing"))]
    generated_profile_marks: parking_lot::Mutex<Vec<(i32, Instant)>>,
    #[cfg(any(test, feature = "testing"))]
    generated_host_call_marks: parking_lot::Mutex<Vec<(&'static str, Instant)>>,
    #[cfg(any(test, feature = "testing"))]
    generated_pool_hits: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    generated_pool_misses: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    generated_pool_events: parking_lot::Mutex<Vec<StaticHermesGeneratedPoolEventObservation>>,
    #[cfg(any(test, feature = "testing"))]
    generated_invocations: parking_lot::Mutex<Vec<StaticHermesGeneratedInvocationObservation>>,
    #[cfg(any(test, feature = "testing"))]
    generated_memory_snapshots: parking_lot::Mutex<Vec<StaticHermesGeneratedMemorySnapshot>>,
    #[cfg(any(test, feature = "testing"))]
    generated_execution_phases:
        parking_lot::Mutex<Vec<StaticHermesGeneratedExecutionPhaseObservation>>,
    #[cfg(any(test, feature = "testing"))]
    generated_route_preflight_stages:
        parking_lot::Mutex<Vec<StaticHermesGeneratedRoutePreflightStage>>,
    #[cfg(any(test, feature = "testing"))]
    previous_capability_identity: AtomicU64,
    #[cfg(any(test, feature = "testing"))]
    active_times: parking_lot::Mutex<Vec<Duration>>,
    #[cfg(any(test, feature = "testing"))]
    wall_times: parking_lot::Mutex<Vec<Duration>>,
    #[cfg(any(test, feature = "testing"))]
    read_documents: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    read_bytes: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    read_intervals: AtomicUsize,
    #[cfg(test)]
    first_developer_error_index: AtomicUsize,
    #[cfg(any(test, feature = "testing"))]
    terminal_cancellations: AtomicUsize,
    #[cfg(test)]
    profile_marks: AtomicUsize,
}

// Production exposes phase metrics through the registered histograms. The
// detailed collectors below are only populated by deterministic tests, so
// production invocations share this inactive handle rather than allocating a
// fresh collector on the request path.
#[cfg(not(any(test, feature = "testing")))]
static PRODUCTION_GATE_METRICS: LazyLock<Arc<GateMetrics>> =
    LazyLock::new(|| Arc::new(GateMetrics::default()));

// The concurrency limiter retains this value for the lifetime of an active
// invocation, but all generated Wasm requests share the same client label.
static ACTIVE_WASM_CPU_ADMISSION_CLIENT_ID: LazyLock<Arc<String>> =
    LazyLock::new(|| Arc::new("Static Hermes Wasm active CPU".to_owned()));

#[cfg(any(test, feature = "testing"))]
impl GateMetrics {
    fn record_generated_pool_event(
        &self,
        runtime_id: Option<u64>,
        memory_slot_id: Option<u64>,
        event: StaticHermesGeneratedPoolEvent,
    ) {
        self.generated_pool_events
            .lock()
            .push(StaticHermesGeneratedPoolEventObservation {
                runtime_id,
                memory_slot_id,
                event,
            });
    }

    fn record_generated_memory_snapshot(&self, snapshot: GeneratedMemoryTestSnapshot) {
        self.generated_memory_snapshots.lock().push(snapshot.into());
    }
}

#[cfg(any(test, feature = "testing"))]
struct GeneratedExecutionPhaseTrace {
    metrics: Arc<GateMetrics>,
    interrupt: Arc<GeneratedInterruptState>,
    cancellation: CancellationSignal,
    started: Instant,
    fresh_runtime: bool,
    entry_selection_required: bool,
    selected_entry_preparation_required: bool,
    instantiation_completed: Option<Duration>,
    initialization_started: Option<Duration>,
    initialization_completed: Option<Duration>,
    entry_selection_started: Option<Duration>,
    entry_selection_status: Option<i32>,
    entry_selection_completed: Option<Duration>,
    prepare_started: Option<Duration>,
    prepare_status: Option<i32>,
    prepare_completed: Option<Duration>,
    second_entry_selection_started: Option<Duration>,
    second_entry_selection_status: Option<i32>,
    second_entry_selection_completed: Option<Duration>,
    handler_started: Option<Duration>,
    handler_completed: Option<Duration>,
    handler_completion: Option<&'static str>,
    initial_fuel: Option<u64>,
    prepare_start_fuel: Option<u64>,
    prepare_end_fuel: Option<u64>,
    handler_start_fuel: Option<u64>,
    handler_end_fuel: Option<u64>,
    final_fuel: Option<u64>,
    profile_mark_start: usize,
    completion_stage_start: usize,
}

#[cfg(any(test, feature = "testing"))]
impl GeneratedExecutionPhaseTrace {
    fn new(
        metrics: Arc<GateMetrics>,
        interrupt: Arc<GeneratedInterruptState>,
        cancellation: CancellationSignal,
        started: Instant,
        fresh_runtime: bool,
        entry_selection_required: bool,
        selected_entry_preparation_required: bool,
    ) -> Self {
        let profile_mark_start = metrics.generated_profile_marks.lock().len();
        let completion_stage_start = metrics.guest_native_completion_stages.lock().len();
        Self {
            metrics,
            interrupt,
            cancellation,
            started,
            fresh_runtime,
            entry_selection_required,
            selected_entry_preparation_required,
            instantiation_completed: None,
            initialization_started: None,
            initialization_completed: None,
            entry_selection_started: None,
            entry_selection_status: None,
            entry_selection_completed: None,
            prepare_started: None,
            prepare_status: None,
            prepare_completed: None,
            second_entry_selection_started: None,
            second_entry_selection_status: None,
            second_entry_selection_completed: None,
            handler_started: None,
            handler_completed: None,
            handler_completion: None,
            initial_fuel: None,
            prepare_start_fuel: None,
            prepare_end_fuel: None,
            handler_start_fuel: None,
            handler_end_fuel: None,
            final_fuel: None,
            profile_mark_start,
            completion_stage_start,
        }
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }
}

#[cfg(any(test, feature = "testing"))]
impl Drop for GeneratedExecutionPhaseTrace {
    fn drop(&mut self) {
        let timeout_diagnostic = self.interrupt.diagnostic(self.started);
        self.metrics.generated_execution_phases.lock().push(
            StaticHermesGeneratedExecutionPhaseObservation {
                fresh_runtime: self.fresh_runtime,
                entry_selection_required: self.entry_selection_required,
                selected_entry_preparation_required: self.selected_entry_preparation_required,
                total: self.elapsed(),
                timeout_armed_before_execution: timeout_diagnostic.armed_before_execution,
                timeout_reason: timeout_diagnostic.reason,
                timeout_after_execution_start: timeout_diagnostic.after_execution_start,
                cancellation_observed: self.cancellation.is_cancelled(),
                instantiation_completed: self.instantiation_completed,
                initialization_started: self.initialization_started,
                initialization_completed: self.initialization_completed,
                entry_selection_started: self.entry_selection_started,
                entry_selection_status: self.entry_selection_status,
                entry_selection_completed: self.entry_selection_completed,
                prepare_started: self.prepare_started,
                prepare_status: self.prepare_status,
                prepare_completed: self.prepare_completed,
                second_entry_selection_started: self.second_entry_selection_started,
                second_entry_selection_status: self.second_entry_selection_status,
                second_entry_selection_completed: self.second_entry_selection_completed,
                handler_started: self.handler_started,
                handler_completed: self.handler_completed,
                handler_completion: self.handler_completion,
                initial_fuel: self.initial_fuel,
                prepare_start_fuel: self.prepare_start_fuel,
                prepare_end_fuel: self.prepare_end_fuel,
                handler_start_fuel: self.handler_start_fuel,
                handler_end_fuel: self.handler_end_fuel,
                final_fuel: self.final_fuel,
                function_entry_marks: self
                    .metrics
                    .generated_profile_marks
                    .lock()
                    .iter()
                    .skip(self.profile_mark_start)
                    .map(|(phase, marked_at)| {
                        (*phase, marked_at.saturating_duration_since(self.started))
                    })
                    .collect(),
                host_call_marks: self
                    .metrics
                    .generated_host_call_marks
                    .lock()
                    .iter()
                    .filter(|(_, marked_at)| *marked_at >= self.started)
                    .map(|(stage, marked_at)| {
                        (*stage, marked_at.saturating_duration_since(self.started))
                    })
                    .collect(),
                completion_stages: self
                    .metrics
                    .guest_native_completion_stages
                    .lock()
                    .iter()
                    .skip(self.completion_stage_start)
                    .copied()
                    .collect(),
            },
        );
    }
}

#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Debug)]
pub struct StaticHermesGeneratedInvocationObservation {
    pub invocation_id: u64,
    pub runtime_id: u64,
    pub memory_slot_id: u64,
    pub deployment_sha256: Option<String>,
    pub package_key: String,
    pub entry_id: Option<String>,
    pub entry_selector_id: Option<String>,
    pub capability_identity: u64,
    pub runtime_was_created: bool,
    pub serialized_aot_module_loaded: bool,
    pub operation_count: u64,
    pub transaction_write_count: usize,
    pub capability_revoked: bool,
    pub forged_capability_rejected: bool,
    pub prior_capability_rejected: bool,
    pub revoked_capability_rejected: bool,
    pub opaque_live_handles: usize,
    pub opaque_current_bytes: usize,
    pub runtime_reuse_contaminated: bool,
    pub retain_instance: bool,
    pub reuse_instances: bool,
    pub outcome_allows_reuse: bool,
    pub guest_developer_error: bool,
    pub developer_error_reported: bool,
    pub discard_after_caught_initialization_failure: bool,
    pub context_read_set_ready: bool,
    pub cancelled: bool,
}

/// A test-only view of the generated-runtime controller immediately after a
/// reusable runtime has returned its slot to the pool.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug)]
pub struct StaticHermesGeneratedMemorySnapshot {
    pub active_instances: usize,
    pub idle_instances: usize,
    pub evicting_instances: usize,
    pub fixed_module_bytes: usize,
    pub retained_baseline_bytes: usize,
    pub active_checkout_baseline_bytes: usize,
    pub active_guest_bytes: usize,
    pub active_host_bytes: usize,
    pub active_current_growth_bytes: usize,
    pub active_forecast_bytes: usize,
    pub active_liability_bytes: usize,
    pub unattributed_allowance_bytes: usize,
    pub projected_bytes: usize,
    pub function_samples: u64,
    pub developer_error_samples: u64,
    pub system_error_samples: u64,
    pub timeout_samples: u64,
    pub resource_limit_samples: u64,
    pub memory_limit_samples: u64,
}

#[cfg(any(test, feature = "testing"))]
impl From<GeneratedMemoryTestSnapshot> for StaticHermesGeneratedMemorySnapshot {
    fn from(snapshot: GeneratedMemoryTestSnapshot) -> Self {
        Self {
            active_instances: snapshot.active_instances,
            idle_instances: snapshot.idle_instances,
            evicting_instances: snapshot.evicting_instances,
            fixed_module_bytes: snapshot.fixed_module_bytes,
            retained_baseline_bytes: snapshot.retained_baseline_bytes,
            active_checkout_baseline_bytes: snapshot.active_checkout_baseline_bytes,
            active_guest_bytes: snapshot.active_guest_bytes,
            active_host_bytes: snapshot.active_host_bytes,
            active_current_growth_bytes: snapshot.active_current_growth_bytes,
            active_forecast_bytes: snapshot.active_forecast_bytes,
            active_liability_bytes: snapshot.active_liability_bytes,
            unattributed_allowance_bytes: snapshot.unattributed_allowance_bytes,
            projected_bytes: snapshot.projected_bytes,
            function_samples: snapshot.function_samples,
            developer_error_samples: snapshot.developer_error_samples,
            system_error_samples: snapshot.system_error_samples,
            timeout_samples: snapshot.timeout_samples,
            resource_limit_samples: snapshot.resource_limit_samples,
            memory_limit_samples: snapshot.memory_limit_samples,
        }
    }
}

// Pool events are deliberately identity-free. They let retained-runtime tests
// distinguish an unavailable route match from a lifecycle or memory eviction
// without exposing routed paths or request data through test diagnostics.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticHermesGeneratedPoolEventObservation {
    pub runtime_id: Option<u64>,
    pub memory_slot_id: Option<u64>,
    pub event: StaticHermesGeneratedPoolEvent,
}

#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StaticHermesGeneratedPoolEvent {
    Checkout {
        matching_pool_identity_available: bool,
        reused_runtime: bool,
    },
    AdmissionWait {
        matching_pool_identity_available: bool,
        reason: StaticHermesGeneratedPoolAdmissionWaitReason,
    },
    AdmissionRejected {
        matching_pool_identity_available: bool,
    },
    Returned,
    ReturnedForGenerationRetirement,
    ContextReadSetRejected,
    Evicted {
        trigger: StaticHermesGeneratedPoolEvictionTrigger,
        reason: StaticHermesGeneratedPoolEvictionReason,
    },
}

#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticHermesGeneratedPoolAdmissionWaitReason {
    Pressure,
    ActiveCountCeiling,
    InstanceCountCeiling,
    SlotUnavailable,
    SoftBudget,
    HardBudget,
    FunctionRecordCapacity,
}

#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticHermesGeneratedPoolEvictionTrigger {
    Maintenance,
    NewSlot,
    AdmissionBudget,
    GenerationRetirement,
    TestCleanup,
}

#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticHermesGeneratedPoolEvictionReason {
    IdleAge,
    MemoryPressure,
    InstanceCeiling,
    AdmissionBudget,
    GenerationRetirement,
    TestCleanup,
}

// Retained-route tests need this even when the guest is interrupted before it
// can produce a normal invocation observation.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Debug)]
pub struct StaticHermesGeneratedExecutionPhaseObservation {
    pub fresh_runtime: bool,
    pub entry_selection_required: bool,
    pub selected_entry_preparation_required: bool,
    pub total: Duration,
    pub timeout_armed_before_execution: Option<Duration>,
    pub timeout_reason: Option<&'static str>,
    pub timeout_after_execution_start: Option<Duration>,
    pub cancellation_observed: bool,
    pub instantiation_completed: Option<Duration>,
    pub initialization_started: Option<Duration>,
    pub initialization_completed: Option<Duration>,
    pub entry_selection_started: Option<Duration>,
    pub entry_selection_status: Option<i32>,
    pub entry_selection_completed: Option<Duration>,
    pub prepare_started: Option<Duration>,
    pub prepare_status: Option<i32>,
    pub prepare_completed: Option<Duration>,
    pub second_entry_selection_started: Option<Duration>,
    pub second_entry_selection_status: Option<i32>,
    pub second_entry_selection_completed: Option<Duration>,
    pub handler_started: Option<Duration>,
    pub handler_completed: Option<Duration>,
    pub handler_completion: Option<&'static str>,
    pub initial_fuel: Option<u64>,
    pub prepare_start_fuel: Option<u64>,
    pub prepare_end_fuel: Option<u64>,
    pub handler_start_fuel: Option<u64>,
    pub handler_end_fuel: Option<u64>,
    pub final_fuel: Option<u64>,
    pub function_entry_marks: Vec<(i32, Duration)>,
    pub host_call_marks: Vec<(&'static str, Duration)>,
    pub completion_stages: Vec<&'static str>,
}

// This deliberately contains only fixed execution boundaries. It never carries
// a route identity, request data, artifact name, or error detail.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticHermesGeneratedRoutePreflightStage {
    InvocationPreparationEntered,
    RoutedWorkerEntered,
    InvocationValidated,
    SourceIdentityVerified,
    RoutedModuleLoadStarted,
    RoutedRegistryRouteValidated,
    RoutedSharedEngineAcquisitionStarted,
    RoutedSharedEngineAcquired,
    RoutedRuntimeCompatibilityResolutionStarted,
    RoutedRuntimeCompatibilityResolved,
    RoutedGraphRouteMaterialResolutionStarted,
    RoutedGraphRouteMaterialResolved,
    RoutedRouteLeaseAuthenticationStarted,
    RoutedRouteLeaseAuthenticated,
    RoutedNongraphPackageResolutionStarted,
    RoutedNongraphPackageResolved,
    RoutedArtifactResolved,
    RoutedSharedAotPrepared,
    RoutedModuleCapacityRejected,
    RoutedModuleCacheBuilt,
    RoutedModuleLoaded,
    InvocationStateCreated,
    MemoryAdmitted,
    WarmContextReadSetValidated,
    ActiveCpuAdmitted,
    ExecutionEntered,
}

pub struct StaticHermesGateTestHooks {
    metrics: Arc<GateMetrics>,
    held_reads_remaining: AtomicUsize,
    held_guest_completions_remaining: AtomicUsize,
    entered_tx: mpsc::UnboundedSender<u64>,
    entered_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<u64>>,
    releases: parking_lot::Mutex<BTreeMap<u64, oneshot::Sender<()>>>,
    guest_completed_tx: mpsc::UnboundedSender<u64>,
    guest_completed_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<u64>>,
    guest_releases: parking_lot::Mutex<BTreeMap<u64, oneshot::Sender<()>>>,
    #[cfg(any(test, feature = "testing"))]
    runtime_destruction_barrier:
        parking_lot::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    // This is deliberately test-only diagnostic state. It changes the Store's
    // per-invocation fuel, never the authenticated manifest or artifact.
    #[cfg(any(test, feature = "testing"))]
    generated_execution_fuel_override: AtomicU64,
    // This is deliberately test-only diagnostic state. It changes only the
    // runtime compatibility presented to artifact and route validation, never
    // the manifest policy that later arms the active execution timeout.
    #[cfg(any(test, feature = "testing"))]
    generated_runtime_execution_time_limit_override_ms: AtomicU64,
    // This only controls synthetic routed test modules. Production routes
    // always use the configured admission limiter.
    #[cfg(any(test, feature = "testing"))]
    generated_active_wasm_cpu_limiter: parking_lot::Mutex<Option<ConcurrencyLimiter>>,
    #[cfg(any(test, feature = "testing"))]
    generated_route: Option<Arc<GeneratedRoutedModule>>,
}

impl StaticHermesGateTestHooks {
    pub fn new(held_read_count: usize, held_guest_completion_count: usize) -> Arc<Self> {
        let (entered_tx, entered_rx) = mpsc::unbounded_channel();
        let (guest_completed_tx, guest_completed_rx) = mpsc::unbounded_channel();
        Arc::new(Self {
            metrics: Arc::new(GateMetrics::default()),
            held_reads_remaining: AtomicUsize::new(held_read_count),
            held_guest_completions_remaining: AtomicUsize::new(held_guest_completion_count),
            entered_tx,
            entered_rx: tokio::sync::Mutex::new(entered_rx),
            releases: parking_lot::Mutex::new(BTreeMap::new()),
            guest_completed_tx,
            guest_completed_rx: tokio::sync::Mutex::new(guest_completed_rx),
            guest_releases: parking_lot::Mutex::new(BTreeMap::new()),
            #[cfg(any(test, feature = "testing"))]
            runtime_destruction_barrier: parking_lot::Mutex::new(None),
            #[cfg(any(test, feature = "testing"))]
            generated_execution_fuel_override: AtomicU64::new(0),
            #[cfg(any(test, feature = "testing"))]
            generated_runtime_execution_time_limit_override_ms: AtomicU64::new(0),
            #[cfg(any(test, feature = "testing"))]
            generated_active_wasm_cpu_limiter: parking_lot::Mutex::new(None),
            #[cfg(any(test, feature = "testing"))]
            generated_route: None,
        })
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn new_generated_occ_mutation(
        read_table_name: &str,
        insert_table_name: &str,
        held_guest_completion_count: usize,
    ) -> anyhow::Result<Arc<Self>> {
        let mut hooks = Self::new(0, held_guest_completion_count);
        Arc::get_mut(&mut hooks)
            .context("new generated test hooks were unexpectedly shared")?
            .generated_route = Some(generated_occ_test_routed_module(
            read_table_name,
            insert_table_name,
        )?);
        Ok(hooks)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn new_generated_query_terminal_pair(
        table_name: &str,
        held_read_count: usize,
    ) -> anyhow::Result<Arc<Self>> {
        let mut hooks = Self::new(held_read_count, 0);
        Arc::get_mut(&mut hooks)
            .context("new generated test hooks were unexpectedly shared")?
            .generated_route = Some(generated_query_terminal_pair_routed_module(table_name)?);
        Ok(hooks)
    }

    #[cfg(any(test, feature = "testing"))]
    /// Dropping the release sender or abandoning arrival lets cleanup proceed.
    pub fn hold_next_runtime_destruction(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let mut barrier = self.runtime_destruction_barrier.lock();
        assert!(
            barrier.is_none(),
            "runtime destruction barrier already armed"
        );
        *barrier = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    pub async fn wait_for_pending_read(&self) -> anyhow::Result<u64> {
        self.entered_rx
            .lock()
            .await
            .recv()
            .await
            .context("Static Hermes gate read event channel closed")
    }

    pub fn release_read(&self, instance_id: u64) -> anyhow::Result<()> {
        self.releases
            .lock()
            .remove(&instance_id)
            .context("Static Hermes gate read release missing")?
            .send(())
            .map_err(|_| anyhow::anyhow!("Static Hermes gate read already dropped"))
    }

    pub async fn wait_for_guest_completion(&self) -> anyhow::Result<u64> {
        self.guest_completed_rx
            .lock()
            .await
            .recv()
            .await
            .context("Static Hermes gate guest completion channel closed")
    }

    pub fn release_guest_completion(&self, instance_id: u64) -> anyhow::Result<()> {
        self.guest_releases
            .lock()
            .remove(&instance_id)
            .context("Static Hermes gate guest completion release missing")?
            .send(())
            .map_err(|_| anyhow::anyhow!("Static Hermes gate attempt already dropped"))
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn read_completed(&self) -> usize {
        self.metrics.read_completed.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn database_query_starts(&self) -> usize {
        self.metrics.database_query_starts.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn execution_errors(&self) -> Vec<String> {
        self.metrics.execution_errors.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn read_cancelled(&self) -> usize {
        self.metrics.read_cancelled.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn store_drops(&self) -> usize {
        self.metrics.store_drops.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn transaction_drops(&self) -> usize {
        self.metrics.transaction_drops.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn teardowns(&self) -> usize {
        self.metrics.teardowns.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn terminal_cancellations(&self) -> usize {
        self.metrics.terminal_cancellations.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn instance_ids(&self) -> Vec<u64> {
        self.metrics.instances.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn generated_runtime_ids(&self) -> Vec<u64> {
        self.metrics.generated_runtimes.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn generated_pool_hits(&self) -> usize {
        self.metrics.generated_pool_hits.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn generated_pool_misses(&self) -> usize {
        self.metrics.generated_pool_misses.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn generated_pool_events(&self) -> Vec<StaticHermesGeneratedPoolEventObservation> {
        self.metrics.generated_pool_events.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn generated_invocations(&self) -> Vec<StaticHermesGeneratedInvocationObservation> {
        self.metrics.generated_invocations.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn generated_memory_snapshots(&self) -> Vec<StaticHermesGeneratedMemorySnapshot> {
        self.metrics.generated_memory_snapshots.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn generated_execution_phases(
        &self,
    ) -> Vec<StaticHermesGeneratedExecutionPhaseObservation> {
        self.metrics.generated_execution_phases.lock().clone()
    }

    /// Returns opaque route-execution boundaries in their process-wide emission
    /// order. Tests using this hook must serialize the instrumented invocation.
    #[cfg(any(test, feature = "testing"))]
    pub fn generated_route_preflight_stages(
        &self,
    ) -> Vec<StaticHermesGeneratedRoutePreflightStage> {
        self.metrics.generated_route_preflight_stages.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_generated_active_wasm_cpu_limiter(&self, limiter: ConcurrencyLimiter) {
        *self.generated_active_wasm_cpu_limiter.lock() = Some(limiter);
    }

    #[cfg(any(test, feature = "testing"))]
    fn generated_active_wasm_cpu_limiter(&self) -> Option<ConcurrencyLimiter> {
        self.generated_active_wasm_cpu_limiter.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    fn new_generated_route(routed: Arc<GeneratedRoutedModule>) -> Arc<StaticHermesGateTestHooks> {
        let mut hooks = Self::new(0, 0);
        Arc::get_mut(&mut hooks)
            .expect("new generated test hooks were unexpectedly shared")
            .generated_route = Some(routed);
        hooks
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_generated_execution_fuel_override(&self, fuel: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            fuel > 0,
            "generated execution fuel override must be positive"
        );
        self.generated_execution_fuel_override
            .store(fuel, Ordering::Release);
        Ok(())
    }

    #[cfg(any(test, feature = "testing"))]
    fn generated_execution_fuel_override(&self) -> Option<u64> {
        let fuel = self
            .generated_execution_fuel_override
            .load(Ordering::Acquire);
        (fuel > 0).then_some(fuel)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_generated_runtime_execution_time_limit_override(
        &self,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let milliseconds = u64::try_from(timeout.as_millis())
            .context("generated runtime execution-time override exceeds u64 milliseconds")?;
        anyhow::ensure!(
            milliseconds > 0,
            "generated runtime execution-time override must be positive"
        );
        self.generated_runtime_execution_time_limit_override_ms
            .store(milliseconds, Ordering::Release);
        Ok(())
    }

    #[cfg(any(test, feature = "testing"))]
    fn generated_runtime_execution_time_limit_override(&self) -> Option<u64> {
        let milliseconds = self
            .generated_runtime_execution_time_limit_override_ms
            .load(Ordering::Acquire);
        (milliseconds > 0).then_some(milliseconds)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn guest_native_completion_stages(&self) -> Vec<&'static str> {
        self.metrics.guest_native_completion_stages.lock().clone()
    }

    #[cfg(not(any(test, feature = "testing")))]
    pub fn guest_native_completion_stages(&self) -> Vec<&'static str> {
        Vec::new()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn active_times(&self) -> Vec<Duration> {
        self.metrics.active_times.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn wall_times(&self) -> Vec<Duration> {
        self.metrics.wall_times.lock().clone()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn read_documents(&self) -> usize {
        self.metrics.read_documents.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn read_bytes(&self) -> usize {
        self.metrics.read_bytes.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn read_intervals(&self) -> usize {
        self.metrics.read_intervals.load(Ordering::SeqCst)
    }
}

static TEST_HOOKS: LazyLock<parking_lot::Mutex<Option<Weak<StaticHermesGateTestHooks>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(None));

// This hook is available only to tests that have explicitly installed the
// process-local test hooks. Production runtime compatibility always derives
// every platform limit from the active backend policy.
#[cfg(any(test, feature = "testing"))]
pub(super) fn generated_runtime_execution_time_limit_override() -> Option<u64> {
    TEST_HOOKS
        .lock()
        .as_ref()
        .and_then(Weak::upgrade)
        .and_then(|hooks| hooks.generated_runtime_execution_time_limit_override())
}

#[cfg(test)]
static TEST_HOOKS_TEST_LOCK: LazyLock<parking_lot::Mutex<()>> =
    LazyLock::new(|| parking_lot::Mutex::new(()));

#[cfg(any(test, feature = "testing"))]
fn record_guest_native_completion_stage(metrics: &GateMetrics, stage: &'static str) {
    metrics.guest_native_completion_stages.lock().push(stage);
}

#[cfg(not(any(test, feature = "testing")))]
fn record_guest_native_completion_stage(_metrics: &GateMetrics, _stage: &'static str) {}

#[cfg(any(test, feature = "testing"))]
fn record_generated_host_call_mark(metrics: &GateMetrics, stage: &'static str) {
    metrics
        .generated_host_call_marks
        .lock()
        .push((stage, Instant::now()));
}

#[cfg(not(any(test, feature = "testing")))]
fn record_generated_host_call_mark(_metrics: &GateMetrics, _stage: &'static str) {}

#[cfg(any(test, feature = "testing"))]
pub(super) fn record_generated_route_preflight_stage(
    stage: StaticHermesGeneratedRoutePreflightStage,
) {
    let Some(hooks) = TEST_HOOKS.lock().as_ref().and_then(Weak::upgrade) else {
        return;
    };
    hooks
        .metrics
        .generated_route_preflight_stages
        .lock()
        .push(stage);
}

pub struct StaticHermesGateTestHooksGuard;

impl Drop for StaticHermesGateTestHooksGuard {
    fn drop(&mut self) {
        *TEST_HOOKS.lock() = None;
    }
}

pub fn install_test_hooks(
    hooks: &Arc<StaticHermesGateTestHooks>,
) -> anyhow::Result<StaticHermesGateTestHooksGuard> {
    let mut active = TEST_HOOKS.lock();
    anyhow::ensure!(
        active.as_ref().and_then(Weak::upgrade).is_none(),
        "Static Hermes gate test hooks already installed"
    );
    *active = Some(Arc::downgrade(hooks));
    Ok(StaticHermesGateTestHooksGuard)
}

#[cfg(test)]
#[test]
fn route_preflight_stages_preserve_emission_order() -> anyhow::Result<()> {
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    let hooks = StaticHermesGateTestHooks::new(0, 0);
    let _test_hooks_guard = install_test_hooks(&hooks)?;

    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::InvocationPreparationEntered,
    );
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::SourceIdentityVerified,
    );
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedModuleLoadStarted,
    );
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedModuleLoaded,
    );
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedWorkerEntered,
    );

    assert_eq!(
        hooks.generated_route_preflight_stages(),
        vec![
            StaticHermesGeneratedRoutePreflightStage::InvocationPreparationEntered,
            StaticHermesGeneratedRoutePreflightStage::SourceIdentityVerified,
            StaticHermesGeneratedRoutePreflightStage::RoutedModuleLoadStarted,
            StaticHermesGeneratedRoutePreflightStage::RoutedModuleLoaded,
            StaticHermesGeneratedRoutePreflightStage::RoutedWorkerEntered,
        ]
    );
    Ok(())
}

#[cfg(test)]
#[test]
fn generated_runtime_compatibility_test_override_changes_only_execution_time() -> anyhow::Result<()>
{
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    let unmodified = module_contract::generated_runtime_compatibility("test-engine")?;
    let hooks = StaticHermesGateTestHooks::new(0, 0);
    hooks.set_generated_runtime_execution_time_limit_override(Duration::from_secs(2))?;
    {
        let _test_hooks_guard = install_test_hooks(&hooks)?;
        let overridden = module_contract::generated_runtime_compatibility("test-engine")?;
        let mut expected_platform_limits = unmodified.platform_limits;
        expected_platform_limits.execution_time_ms = 2_000;
        assert_eq!(overridden.platform_limits, expected_platform_limits);
    }
    let restored = module_contract::generated_runtime_compatibility("test-engine")?;
    assert_eq!(restored.platform_limits, unmodified.platform_limits);
    Ok(())
}

#[cfg(test)]
#[test]
fn module_memory_admission_errors_keep_typed_shadow_classification() -> anyhow::Result<()> {
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    let hooks = StaticHermesGateTestHooks::new(0, 0);
    let _test_hooks_guard = install_test_hooks(&hooks)?;

    for (error, transient) in [
        (
            ModuleMemoryAdmissionError::FixedBytesExceedHardBudget,
            false,
        ),
        (ModuleMemoryAdmissionError::Pressure, true),
        (ModuleMemoryAdmissionError::SoftBudget, true),
        (ModuleMemoryAdmissionError::HardBudget, true),
    ] {
        let primary = routing_runtime::classify_generated_module_load_error(
            routed_module_cache::module_memory_admission_error(error)
                .context("module loader context"),
            false,
        );
        assert_eq!(
            primary.downcast_ref::<ModuleMemoryAdmissionError>(),
            Some(&error)
        );

        let shadow = routing_runtime::classify_generated_module_load_error(
            routed_module_cache::module_memory_admission_error(error)
                .context("module loader context"),
            true,
        );
        assert_eq!(
            shadow.is::<StaticHermesQueryShadowCapacityUnavailable>(),
            transient
        );
        assert_eq!(
            shadow.downcast_ref::<ModuleMemoryAdmissionError>(),
            (!transient).then_some(&error)
        );
    }

    assert_eq!(
        hooks.generated_route_preflight_stages(),
        vec![StaticHermesGeneratedRoutePreflightStage::RoutedModuleCapacityRejected; 8]
    );
    Ok(())
}

struct PendingReadLease {
    metrics: Arc<GateMetrics>,
    completed: bool,
}

impl PendingReadLease {
    fn complete(&mut self) {
        self.completed = true;
        #[cfg(any(test, feature = "testing"))]
        self.metrics.read_completed.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for PendingReadLease {
    fn drop(&mut self) {
        if !self.completed {
            #[cfg(any(test, feature = "testing"))]
            self.metrics.read_cancelled.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct ReadControl {
    entered: ReadEntered,
    release: oneshot::Receiver<()>,
}

enum ReadEntered {
    #[cfg(test)]
    Direct(oneshot::Sender<()>),
    TestHooks {
        sender: mpsc::UnboundedSender<u64>,
        instance_id: u64,
    },
}

#[cfg(test)]
struct ReadHostSide {
    entered: oneshot::Receiver<()>,
    release: Option<oneshot::Sender<()>>,
}

#[cfg(test)]
fn read_control() -> (ReadControl, ReadHostSide) {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    (
        ReadControl {
            entered: ReadEntered::Direct(entered_tx),
            release: release_rx,
        },
        ReadHostSide {
            entered: entered_rx,
            release: Some(release_tx),
        },
    )
}

fn is_official_output_chunk_initialization_failure(message: &str) -> bool {
    // The generated lowering renders a non-Error guest throw as `Uncaught `
    // before it calls the developer-error import.
    let Some((unit_slot, error_class)) = message
        .strip_prefix("Uncaught ")
        .unwrap_or(message)
        .strip_prefix("Guest initialization failed: unit_slot=")
        .and_then(|message| message.split_once(" error_class="))
    else {
        return false;
    };
    !unit_slot.is_empty()
        && unit_slot.bytes().all(|byte| byte.is_ascii_digit())
        && unit_slot.parse::<u32>().is_ok()
        && matches!(
            error_class,
            "non-error"
                | "error"
                | "type-error"
                | "range-error"
                | "reference-error"
                | "syntax-error"
                | "eval-error"
                | "uri-error"
        )
}

fn new_invocation_state<RT: Runtime>(
    rt: RT,
    provider: DatabaseUdfWasmInvocation<RT>,
    udf_callback: StaticHermesUdfCallback<RT>,
    read_control: Option<ReadControl>,
    metrics: Arc<GateMetrics>,
    permit: ConcurrencyPermit,
) -> HostState<RT> {
    let instance_id = NEXT_INSTANCE_ID.fetch_add(1, Ordering::SeqCst);
    let wasi_monotonic_epoch = rt.monotonic_now();
    let active_wasm_cpu_limiter = permit.limiter().clone();
    #[cfg(any(test, feature = "testing"))]
    let read_control = match read_control {
        Some(read_control) => Some(read_control),
        None => TEST_HOOKS
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .and_then(|hooks| {
                hooks
                    .held_reads_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .ok()
                    .map(|_| {
                        let (release, release_rx) = oneshot::channel();
                        assert!(hooks.releases.lock().insert(instance_id, release).is_none());
                        ReadControl {
                            entered: ReadEntered::TestHooks {
                                sender: hooks.entered_tx.clone(),
                                instance_id,
                            },
                            release: release_rx,
                        }
                    })
            }),
    };
    #[cfg(not(any(test, feature = "testing")))]
    let read_control = read_control;
    #[cfg(any(test, feature = "testing"))]
    metrics.instances.lock().push(instance_id);
    HostState {
        provider,
        udf_callback,
        wasi_monotonic_epoch,
        timeout: Some(Timeout::for_static_hermes_gate(rt, permit)),
        active_wasm_cpu_limiter,
        teardown_cpu_permit: None,
        read_control,
        developer_error: None,
        guest_developer_error: false,
        stdout: Vec::new(),
        stderr: Vec::new(),
        metrics,
        instance_id,
        backend_timeout_armed: false,
        profile_last_mark: None,
        generated: None,
    }
}

#[cfg(test)]
async fn new_state(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    read_control: Option<ReadControl>,
    metrics: Arc<GateMetrics>,
    journal: QueryJournal,
) -> anyhow::Result<HostState<ProdRuntime>> {
    let provider = DatabaseUdfWasmInvocation::<ProdRuntime>::new_for_test(
        rt.clone(),
        transaction,
        journal,
        false,
    )?;
    let limiter = ConcurrencyLimiter::new_for_wasm(usize::MAX);
    let permit = limiter
        .acquire(
            Arc::new("Static Hermes Wasm test active CPU".to_owned()),
            false,
        )
        .await;
    Ok(new_invocation_state(
        rt,
        provider,
        StaticHermesUdfCallback::Unavailable,
        read_control,
        metrics,
        permit,
    ))
}

fn wasmtime_anyhow(error: WasmtimeError) -> anyhow::Error {
    error.into()
}

fn detect_precompiled_snapshot_file(snapshot: &File) -> anyhow::Result<Option<Precompiled>> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;

        // Wasmtime's detector parses the ELF section table, which can live at
        // the end of a large AOT file. Read it through the immutable snapshot
        // descriptor instead of retaining the entire artifact in memory.
        return Ok(Engine::detect_precompiled_file(format!(
            "/proc/self/fd/{}",
            snapshot.as_raw_fd()
        ))?);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let mut copy = snapshot.try_clone()?;
        let mut bytes = Vec::new();
        copy.read_to_end(&mut bytes)?;
        Ok(Engine::detect_precompiled(&bytes))
    }
}
