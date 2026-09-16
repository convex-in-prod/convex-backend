#![feature(const_type_name)]
#![feature(error_generic_member_access)]
#![feature(exclusive_wrapper)]
#![feature(try_blocks)]
#![feature(try_blocks_heterogeneous)]
#![feature(iterator_try_collect)]
#![feature(type_alias_impl_trait)]
#![feature(never_type)]
#![feature(impl_trait_in_assoc_type)]
#![feature(impl_trait_in_fn_trait_return)]
#![feature(ptr_metadata)]
#![feature(exit_status_error)]
#![feature(vec_deque_extract_if)]

#[cfg(all(
    feature = "static-hermes-wasmtime-gate",
    not(all(target_arch = "x86_64", target_os = "linux", target_env = "gnu"))
))]
compile_error!(
    "static-hermes-wasmtime-gate requires target x86_64-unknown-linux-gnu because its \
     authenticated AOT contract is target-specific"
);

#[cfg(feature = "static-hermes-wasmtime-gate")]
use crate::environment::udf::wasm_udf_package::{
    MODULE_GRAPH_RUNTIME_SURFACE_INVENTORY_SHA256,
    MODULE_GRAPH_RUNTIME_SURFACE_POLICY_IDENTITY_KIND,
    MODULE_GRAPH_RUNTIME_SURFACE_POLICY_SHA256,
};

#[cfg(feature = "static-hermes-wasmtime-gate")]
mod static_hermes_wasmtime_quarantine;

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StaticHermesWasmtimeGateRuntimeSurfacePolicyIdentity {
    pub inventory_sha256: &'static str,
    pub kind: &'static str,
    pub runtime_surface_policy_sha256: &'static str,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub const STATIC_HERMES_WASMTIME_GATE_RUNTIME_SURFACE_POLICY_IDENTITY:
    StaticHermesWasmtimeGateRuntimeSurfacePolicyIdentity =
    StaticHermesWasmtimeGateRuntimeSurfacePolicyIdentity {
        inventory_sha256: MODULE_GRAPH_RUNTIME_SURFACE_INVENTORY_SHA256,
        kind: MODULE_GRAPH_RUNTIME_SURFACE_POLICY_IDENTITY_KIND,
        runtime_surface_policy_sha256: MODULE_GRAPH_RUNTIME_SURFACE_POLICY_SHA256,
    };

mod array_buffer_allocator;
pub mod bundled_js;
pub mod client;
mod concurrency_limiter;
pub mod context_cache;
mod context_local_state;
pub mod convert_v8;
pub mod environment;
pub mod error;
mod execution_scope;
pub mod helpers;
mod http;
mod is_instance_of_error;
pub mod isolate;
mod isolate_queue;
pub mod isolate_worker;
pub mod metrics;
pub mod module_cache;
pub mod module_map;
pub mod ops;
mod request_scope;
pub mod strings;
mod termination;
pub mod timeout;
mod udf_runtime;

#[cfg(all(
    feature = "static-hermes-wasmtime-gate",
    any(test, feature = "testing")
))]
pub use self::environment::udf::static_hermes_wasmtime_gate::{
    cleanup_static_hermes_test_instances,
    install_test_hooks as install_static_hermes_gate_test_hooks,
    StaticHermesGateTestHooks,
    StaticHermesGateTestHooksGuard,
    StaticHermesGeneratedExecutionPhaseObservation,
    StaticHermesGeneratedInvocationObservation,
    StaticHermesGeneratedMemorySnapshot,
    StaticHermesGeneratedRoutePreflightStage,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
pub use self::environment::udf::static_hermes_wasmtime_gate::{
    source_keyed_paired_deployment_guard_enabled,
    source_keyed_runtime_generation_identity,
    source_keyed_runtime_readiness,
    SourceKeyedRuntimeGenerationIdentity,
    SourceKeyedRuntimeReadiness,
    StaticHermesGeneratedMemoryRouteStatistics,
    StaticHermesGeneratedMemoryStatistics,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
pub use self::environment::udf::static_hermes_wasmtime_gate::{
    PreparedStaticHermesWasmtimeInvocation,
    StaticHermesGeneratedExportDiagnostic,
    StaticHermesQueryShadowCapacityUnavailable,
    StaticHermesQueryShadowRegistry,
    StaticHermesWasmExecutionFailure,
    StaticHermesWasmModuleLoadingFailure,
    StaticHermesWasmTrapCode,
    StaticHermesWasmTrapDiagnostic,
    StaticHermesWasmTrapStderrClassification,
    StaticHermesWasmtimePreparationMode,
    StaticHermesWasmtimeRouteHandle,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
pub use self::static_hermes_wasmtime_quarantine::{
    initialize_static_hermes_wasmtime_quarantine,
    static_hermes_wasmtime_quarantine,
    static_hermes_wasmtime_quarantine_path_for_database_spec,
    StaticHermesWasmtimeQuarantine,
    StaticHermesWasmtimeQuarantineAction,
    StaticHermesWasmtimeQuarantineEntry,
    StaticHermesWasmtimeQuarantineMutationError,
    StaticHermesWasmtimeQuarantineSelector,
    StaticHermesWasmtimeQuarantineSnapshot,
    StaticHermesWasmtimeQuarantineUpdate,
    StaticHermesWasmtimeRouteDecision,
    StaticHermesWasmtimeSourceIdentity,
};
pub use self::{
    client::{
        ActionRequest,
        ActionRequestParams,
        IsolateClient,
        IsolateConfig,
    },
    concurrency_limiter::{
        ConcurrencyLimiter,
        ConcurrencyPermit,
    },
    execution_scope::ExecutionScope,
    helpers::{
        deserialize_udf_custom_error,
        deserialize_udf_result,
        format_uncaught_error,
    },
    isolate::IsolateHeapStats,
    metrics::{
        log_source_map_missing,
        log_source_map_token_lookup_failed,
    },
    request_scope::RequestScope,
    termination::{
        ContextId,
        ExecutionHandle,
    },
    timeout::{
        start_cooperative_request,
        Timeout,
    },
};
