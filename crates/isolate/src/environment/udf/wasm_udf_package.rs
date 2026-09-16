use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fmt,
    fs::{
        self,
        File,
        OpenOptions,
    },
    io::{
        BufReader,
        Read,
        Seek,
        Write,
    },
    path::{
        Path,
        PathBuf,
    },
};

use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value as JsonValue;
use sha2::{
    Digest,
    Sha256,
};
use thiserror::Error;

use super::{
    module_graph_registry::{
        authenticate_module_graph_catalog_v10,
        authenticate_module_graph_catalog_v4,
        authenticate_module_graph_catalog_v5,
        authenticate_module_graph_catalog_v6,
        authenticate_module_graph_catalog_v9,
        parse_generation_v10,
        parse_generation_v4,
        parse_generation_v5,
        parse_generation_v6,
        parse_generation_v9,
        AuthenticatedModuleGraphCatalog,
        DeploymentModuleGraphBinding,
        DeploymentModuleGraphRoute,
        RuntimeRegistryAdmission,
        RuntimeRegistryGenerationV10,
        RuntimeRegistryGenerationV4,
        RuntimeRegistryGenerationV5,
        RuntimeRegistryGenerationV6,
        RuntimeRegistryGenerationV9,
    },
    wasm_udf_manifest::{
        permitted_conditional_convex_imports,
        validate_imported_operations,
        AotArtifactIdentity,
        CompilerIdentity,
        EffectExecutionMode,
        ImportedOperation,
        ManifestError,
        PlatformLimits,
        RuntimeCompatibility,
        UdfKind,
        WasmUdfExecutionManifest,
        WasmUdfExecutionPolicy,
        COHORT_MANIFEST_SCHEMA_VERSION,
        COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION,
        EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION,
        MAX_SERIALIZED_MODULE_BYTES,
        OPAQUE_VALUE_ABI_VERSION,
    },
};

const PACKAGE_ENTRY_KIND: &str = "convex-wasm-artifact-package-v3";
const COHORT_PACKAGE_ENTRY_KIND: &str = "convex-wasm-cohort-package-v1";
const COHORT_MANIFEST_KIND: &str = "convex-wasm-cohort-manifest-v1";
const COHORT_PROVENANCE_KIND: &str = "convex-wasm-cohort-provenance-v1";
const COHORT_ROUTE_REFERENCE_KIND: &str = "convex-wasm-cohort-route-reference-v1";
const COHORT_ENTRY_SELECTOR_ABI_VERSION: u32 = 1;
const DEPLOYMENT_MANIFEST_KIND_V2: &str = "convex-wasm-deployment-v2";
const DEPLOYMENT_MANIFEST_KIND_V3: &str = "convex-wasm-deployment-v3";
const DEPLOYMENT_MANIFEST_KIND_V4: &str = "convex-wasm-deployment-v4";
const DEPLOYMENT_MANIFEST_KIND_V5: &str = "convex-wasm-deployment-v5";
const DEPLOYMENT_MANIFEST_KIND_V6: &str = "convex-wasm-deployment-v6";
const DEPLOYMENT_MANIFEST_KIND_V8: &str = "convex-wasm-deployment-v8";
const CONTEXT_REUSE_POLICY_IDENTITY_KIND: &str = "convex-wasm-context-reuse-selection";
const CONTEXT_REUSE_ANALYSIS_KIND: &str = "convex-context-reuse-analysis";
const CONTEXT_REUSE_COHORT_ANALYSIS_KIND: &str = "convex-context-reuse-cohort-analysis";
const CAPABILITY_ENTRY_PACKAGE_KIND: &str = "convex-wasm-capability-entry-package-v1";
#[cfg(test)]
const CAPABILITY_GRAPH_PACKAGE_KIND: &str = "convex-wasm-capability-graph-package-v1";
#[cfg(test)]
const CAPABILITY_GRAPH_MANIFEST_KIND: &str = "convex-wasm-capability-graph-manifest-v1";
#[cfg(test)]
const CAPABILITY_GRAPH_MANIFEST_SCHEMA_VERSION: u32 = 1;
const CAPABILITY_ENTRY_MANIFEST_KIND: &str = "convex-wasm-capability-entry-manifest-v1";
const CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION: u32 = 3;
const CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION: u32 = 4;
const CAPABILITY_ENTRY_PROVENANCE_KIND_V1: &str = "convex-wasm-capability-entry-provenance-v1";
const CAPABILITY_ENTRY_PROVENANCE_KIND_V2: &str = "convex-wasm-capability-entry-provenance-v2";
const CAPABILITY_NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION: u64 = 2;
const CAPABILITY_ENTRY_ROUTE_REFERENCE_KIND: &str =
    "convex-wasm-capability-entry-route-reference-v1";
const MODULE_GRAPH_COHORT_CONTRACT_KIND: &str = "convex-wasm-module-graph-cohort-contract-v1";
const MODULE_GRAPH_COHORT_CONTRACT_SCHEMA_VERSION: u32 = 1;
const MODULE_GRAPH_COHORT_CONTRACT_KIND_V2: &str = "convex-wasm-module-graph-cohort-contract-v2";
const MODULE_GRAPH_COHORT_CONTRACT_SCHEMA_VERSION_V2: u32 = 2;
const MODULE_GRAPH_ROUTE_REFERENCE_KIND: &str = "convex-wasm-module-graph-route-reference-v1";
pub(crate) const MODULE_GRAPH_RUNTIME_SURFACE_POLICY_IDENTITY_KIND: &str =
    "convex-wasm-runtime-surface-policy-identity";
pub(crate) const MODULE_GRAPH_RUNTIME_SURFACE_INVENTORY_SHA256: &str =
    "80146a1d8d1440f10526f8065997fa07c0a8ae735f3b88778448b2a093639509";
pub(crate) const MODULE_GRAPH_RUNTIME_SURFACE_POLICY_SHA256: &str =
    "7f037ee1a61058ee36d7f1e851e4cb7da3fda41634ca745596be263c7102d042";
const LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION: u32 = 1;
const CAPABILITY_ENTRY_SELECTOR_ABI_VERSION: u32 = 2;
const CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND: &str =
    "convex-wasm-artifact-producer-identity-v1";
const CAPABILITY_STATIC_HERMES_C_BUNDLE_KIND: &str = "static-hermes-c-bundle-v1";
const CAPABILITY_STATIC_HERMES_C_BUNDLE_FLAG: &str = "-Xemit-c-bundle";
const CAPABILITY_STATIC_HERMES_C_BUNDLE_SHARD_SIZE_FLAG_PREFIX: &str = "-Xemit-c-shard-size=";
const CAPABILITY_STATIC_HERMES_C_BUNDLE_MAX_MEMBERS: usize = 65_536;
const CAPABILITY_STATIC_HERMES_PRECOMPILE_PROCESS_KIND: &str =
    "convex-wasm-static-hermes-precompile-process-identity-v1";
const CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMPILATION_POLICY_KIND: &str =
    "convex-wasm-static-hermes-c-bundle-member-compilation-v1";
const CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMMAND_KIND: &str =
    "convex-wasm-static-hermes-c-bundle-member-command-identity-v1";
const CAPABILITY_STATIC_HERMES_C_BUNDLE_PACKAGE_COMPILATION_KIND: &str =
    "convex-wasm-static-hermes-c-bundle-package-compilation-v1";
const CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG: &str = "-O2";
const CAPABILITY_STATIC_HERMES_C_BUNDLE_COMPACT_OPTIMIZATION_FLAG: &str = "-Oz";
const CAPABILITY_OFFICIAL_OUTPUT_CHUNK_DESCRIPTOR_KIND: &str =
    "convex-wasm-official-output-chunk-native-application-descriptor-v2";
const CAPABILITY_OFFICIAL_OUTPUT_CHUNK_UNIT_KIND: &str =
    "convex-wasm-official-output-chunk-unit-v1";
const CAPABILITY_OFFICIAL_OUTPUT_CHUNK_ENTRY_PUBLICATION_UNIT_KIND: &str =
    "convex-wasm-official-output-chunk-entry-publication-unit-v2";
const CAPABILITY_OFFICIAL_OUTPUT_CHUNK_INITIALIZATION_KIND: &str =
    "closed-numbered-chunk-slots-with-per-entry-publication-v1";
const CAPABILITY_OFFICIAL_OUTPUT_CHUNK_NATIVE_DESCRIPTOR_KIND: &str =
    "convex-wasm-official-output-chunk-native-descriptor-abi-v2";
const CAPABILITY_OFFICIAL_OUTPUT_CHUNK_TOPOLOGY_KIND: &str =
    "convex-wasm-capability-unit-topology-v5";
const CAPABILITY_OFFICIAL_OUTPUT_CHUNK_MAXIMUM_UNITS: usize = 1024;
const CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL: &str = "convex_wasm_application_entry_count";
const CAPABILITY_APPLICATION_FACTORY_BY_SLOT_SYMBOL: &str =
    "convex_wasm_application_factory_by_slot";
const CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL: &str = "convex_wasm_application_unit_count";
const CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL: &str =
    "convex_wasm_application_factory_by_unit_slot";
const CAPABILITY_SELECTED_ENTRY_PREPARATION: &str = "selected-entry-preparation-v1";
const MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG: &str =
    "-DCONVEX_WASM_MULTI_ENTRY_APPLICATION_UNIT=1";
const CHUNK_APPLICATION_UNIT_COMPILE_FLAG: &str = "-DCONVEX_WASM_CHUNK_APPLICATION_UNIT=1";
const LEGACY_CAPABILITY_REQUEST_ABI_VERSION: u32 = 1;
const PENDING_VALUE_CAPABILITY_REQUEST_ABI_VERSION: u32 = 2;
const PAGINATION_CAPABILITY_REQUEST_ABI_VERSION: u32 = 3;
const CAPABILITY_REQUEST_ABI_VERSION: u32 = 4;
const LEGACY_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND: &str =
    "convex-wasm-capability-request-envelope-identity-v1";
const PAGINATION_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND: &str =
    "convex-wasm-capability-request-envelope-identity-v2";
const PRE_STORAGE_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND: &str =
    "convex-wasm-capability-request-envelope-identity-v3";
const CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND: &str =
    "convex-wasm-capability-request-envelope-identity-v4";
const LEGACY_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND: &str =
    "convex-wasm-canonical-capability-request-envelope-vector-corpus-v1";
const PAGINATION_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND: &str =
    "convex-wasm-canonical-capability-request-envelope-vector-corpus-v2";
const PRE_STORAGE_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND: &str =
    "convex-wasm-canonical-capability-request-envelope-vector-corpus-v3";
const CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND: &str =
    "convex-wasm-canonical-capability-request-envelope-vector-corpus-v4";
const LEGACY_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND: &str =
    "convex-sdk-backend-capability-request-envelope-producer-v1";
const PAGINATION_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND: &str =
    "convex-sdk-backend-capability-request-envelope-producer-v2";
const PRE_STORAGE_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND: &str =
    "convex-sdk-backend-capability-request-envelope-producer-v3";
const CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND: &str =
    "convex-sdk-backend-capability-request-envelope-producer-v4";
const GUEST_NATIVE_JSON_CODEC_IDENTITY_KIND: &str = "convex-wasm-guest-native-json-codec-identity";
const CANONICAL_CONVEX_VALUE_VECTOR_CORPUS_KIND: &str =
    "convex-wasm-canonical-convex-value-vector-corpus-v1";
const CANONICAL_CONVEX_VALUE_VECTOR_PRODUCER_KIND: &str =
    "convex-sdk-backend-canonical-value-producer-v1";
const COMPILE_SELECTION_KIND: &str = "convex-wasm-explicit-export-selection-v1";
const DEVELOPMENT_COMPILE_SELECTION_KIND: &str = "convex-wasm-development-export-selection-v1";
const CAPABILITY_ENTRY_COMPILE_SELECTION_KIND: &str =
    "convex-wasm-explicit-capability-entry-selection-v1";
const ALL_ELIGIBLE_COMPILE_SELECTION_KIND: &str = "convex-wasm-all-eligible-selection-v1";
const VERIFIED_PRECOMPILER_PACKAGE_KIND: &str = "convex-wasm-verified-precompiler-package";
const PRECOMPILER_PACKAGE_MANIFEST_KIND: &str = "convex-wasm-precompiler-package";
const PRECOMPILER_PACKAGE_MANIFEST_SCHEMA_VERSION: u64 = 1;
const BUILD_PROVENANCE_KIND: &str = "convex-wasm-artifact-provenance-v1";
const ENGINE_IDENTITY_KIND: &str = "convex-wasm-wasmtime-engine-identity";
const DEPLOYED_RUNTIME_IDENTITY_KIND_V1: &str = "convex-deployed-runtime-module-v1";
const DEPLOYED_RUNTIME_IDENTITY_KIND_V2: &str = "convex-deployed-runtime-module-v2";
const DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND: &str =
    "convex-deployed-runtime-identity-diagnostic-v1";
const RUNTIME_REGISTRY_GENERATION_KIND_V1: &str = "convex-wasm-runtime-registry-generation-v1";
const RUNTIME_REGISTRY_GENERATION_KIND_V2: &str = "convex-wasm-runtime-registry-generation-v2";
const RUNTIME_REGISTRY_GENERATION_KIND_V3: &str = "convex-wasm-runtime-registry-generation-v3";
const RUNTIME_REGISTRY_GENERATION_KIND_V4: &str = "convex-wasm-runtime-registry-generation-v4";
const RUNTIME_REGISTRY_GENERATION_KIND_V5: &str = "convex-wasm-runtime-registry-generation-v5";
const RUNTIME_REGISTRY_GENERATION_KIND_V6_SHADOW_ONLY: &str =
    "convex-wasm-runtime-registry-generation-v6-shadow-only";
const RUNTIME_REGISTRY_GENERATION_KIND_V9: &str = "convex-wasm-runtime-registry-generation-v9";
const RUNTIME_REGISTRY_GENERATION_KIND_V10_SHADOW_ONLY: &str =
    "convex-wasm-runtime-registry-generation-v10-shadow-only";
const RUNTIME_REGISTRY_CURRENT_KIND: &str = "convex-wasm-runtime-registry-current-v1";
const RUNTIME_REGISTRY_SOURCE_CATALOG_KIND_V1: &str =
    "convex-wasm-runtime-registry-source-catalog-v1";
const RUNTIME_REGISTRY_SOURCE_CATALOG_KIND_V2: &str =
    "convex-wasm-runtime-registry-source-catalog-v2";
const RUNTIME_REGISTRY_SOURCE_CATALOG_FILE: &str = "source-catalog.json";
const RUNTIME_REGISTRY_SOURCE_CATALOG_COMPLETE_FILE: &str = "SOURCE_CATALOG_COMPLETE";
const MAX_DEPLOYMENT_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;
const MAX_COMPLETE_MARKER_BYTES: u64 = 65;
const MAX_PACKAGE_ENTRY_BYTES: u64 = 64 * 1024;
const MAX_EXECUTION_MANIFEST_BYTES: u64 = 256 * 1024;
const MAX_BUILD_PROVENANCE_BYTES: u64 = 256 * 1024;
const MAX_COHORT_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_COHORT_PROVENANCE_BYTES: u64 = 8 * 1024 * 1024;
#[cfg(test)]
const MAX_CAPABILITY_GRAPH_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_RUNTIME_REGISTRY_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_RUNTIME_REGISTRY_SOURCE_CATALOG_ENTRIES: usize = 4096;
const MAX_CORE_WASM_BYTES: u64 = 320 * 1024 * 1024;
const MAX_DEPLOYED_RUNTIME_IDENTITY_REASONS: usize = 64;
const MAX_DEPLOYED_RUNTIME_IDENTITY_REASON_CODE_BYTES: usize = 128;
const MAX_DEPLOYED_RUNTIME_IDENTITY_REASON_DETAIL_BYTES: usize = 4 * 1024;
const PACKAGE_FILES: [&str; 6] = [
    "COMPLETE",
    "build-provenance.json",
    "execution-manifest.json",
    "module.cwasm",
    "module.wasm",
    "package-entry.json",
];
const COHORT_PACKAGE_FILES: [&str; 6] = [
    "COMPLETE",
    "build-provenance.json",
    "cohort-manifest.json",
    "module.cwasm",
    "module.wasm",
    "package-entry.json",
];
const CAPABILITY_ENTRY_PACKAGE_FILES: [&str; 6] = [
    "COMPLETE",
    "build-provenance.json",
    "entry-manifest.json",
    "module.cwasm",
    "module.wasm",
    "package-entry.json",
];
#[cfg(test)]
const MAX_CAPABILITY_GRAPH_SHARED_MODULES: usize = 1024;

#[derive(Debug, Error)]
pub(crate) enum WasmUdfPackageError {
    #[error("failed to inspect Wasm UDF package path {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Wasm UDF package path {0} is not a private directory")]
    InvalidDirectory(PathBuf),
    #[error("Wasm UDF package contains an unexpected directory entry {0}")]
    UnexpectedEntry(String),
    #[error("Wasm UDF package file {0} is missing")]
    MissingFile(&'static str),
    #[error("Wasm UDF package file {path} is not a private regular file")]
    InvalidFile { path: PathBuf },
    #[error("Wasm UDF package file {path} exceeds its {maximum_bytes}-byte limit")]
    FileTooLarge { path: PathBuf, maximum_bytes: u64 },
    #[error("Wasm UDF package completion marker is invalid")]
    InvalidCompletionMarker,
    #[error("Wasm UDF package metadata is malformed: {0}")]
    MalformedPackageEntry(#[source] serde_json::Error),
    #[error("Wasm UDF package build provenance is malformed: {0}")]
    MalformedBuildProvenance(#[source] serde_json::Error),
    #[error("Wasm UDF package JSON file {0} is not in canonical form")]
    NonCanonicalJson(&'static str),
    #[error("Wasm UDF package metadata is invalid: {0}")]
    InvalidPackageEntry(String),
    #[error("Wasm UDF package build provenance is invalid: {0}")]
    InvalidBuildProvenance(String),
    #[error(transparent)]
    InvalidExecutionManifest(#[from] ManifestError),
    #[error("Wasm UDF package is not executable because its routing decision is V8 fallback")]
    V8FallbackPackage,
    #[error("Wasm UDF package artifact {name} has size {actual_bytes}, expected {expected_bytes}")]
    ArtifactSize {
        name: &'static str,
        actual_bytes: u64,
        expected_bytes: u64,
    },
    #[error("Wasm UDF package artifact {0} does not match its SHA-256 identity")]
    ArtifactDigest(&'static str),
    #[error("Wasm UDF deployment manifest is malformed: {0}")]
    MalformedDeploymentManifest(#[source] serde_json::Error),
    #[error("Wasm UDF deployment manifest is invalid: {0}")]
    InvalidDeploymentManifest(String),
    #[error("Wasm UDF runtime registry manifest is malformed: {0}")]
    MalformedRuntimeRegistry(#[source] serde_json::Error),
    #[error("Wasm UDF runtime registry is invalid: {0}")]
    InvalidRuntimeRegistry(String),
    #[error("Wasm UDF deployed-runtime identity is not bound: {0}")]
    UnboundDeployedRuntimeIdentity(DeployedRuntimeIdentityDiagnostic),
}

#[derive(Debug)]
pub(crate) struct ValidatedWasmUdfPackage {
    pub(crate) execution: WasmUdfExecutionPolicy,
    pub(crate) identity: ValidatedWasmUdfPackageIdentity,
    pub(crate) package_key: String,
    pub(crate) serialized_module_snapshot: File,
    pub(crate) entry_selector: Option<u64>,
    pub(crate) permitted_conditional_convex_imports: BTreeSet<&'static str>,
    pub(crate) core_wasm_bytes: u64,
    pub(crate) serialized_module_bytes: u64,
    pub(crate) graph: Option<ValidatedCapabilityGraph>,
}

#[derive(Debug)]
pub(crate) struct ValidatedCapabilityGraph {
    pub(crate) graph_sha256: String,
    pub(crate) engine_compatibility_sha256: String,
    pub(crate) base: ValidatedCapabilityGraphDependency,
    pub(crate) shared: Vec<ValidatedCapabilityGraphDependency>,
    pub(crate) leaf: ValidatedCapabilityGraphLeaf,
    pub(crate) layout: CapabilityGraphLayout,
}

#[derive(Debug)]
pub(crate) struct ValidatedCapabilityGraphDependency {
    pub(crate) module: ValidatedCapabilityGraphModule,
    pub(crate) serialized_module_snapshot: File,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedCapabilityGraphLeaf {
    pub(crate) module: ValidatedCapabilityGraphModule,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedCapabilityGraphModule {
    pub(crate) module_id: String,
    pub(crate) provider_namespace: String,
    pub(crate) core_wasm_sha256: String,
    pub(crate) core_wasm_bytes: u64,
    pub(crate) serialized_module_sha256: String,
    pub(crate) serialized_module_bytes: u64,
    pub(crate) contract: CapabilityGraphModuleContract,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityGraphPackageEntry {
    artifacts: BTreeMap<String, PackageArtifact>,
    graph_manifest: PackageArtifact,
    key: String,
    kind: String,
    manifest_sha256: String,
    provenance: PackageArtifact,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityGraphManifest {
    base: CapabilityGraphModule,
    engine: CohortEngineIdentity,
    entry_manifest: PackageArtifact,
    graph_sha256: String,
    kind: String,
    layout: CapabilityGraphLayout,
    leaf: CapabilityGraphModule,
    provenance: PackageArtifact,
    schema_version: u32,
    shared: Vec<CapabilityGraphModule>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum CapabilityGraphModuleRole {
    #[serde(rename = "base")]
    Base,
    #[serde(rename = "shared")]
    Shared,
    #[serde(rename = "leaf")]
    Leaf,
}

#[cfg(test)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityGraphModule {
    artifacts: CapabilityGraphModuleArtifacts,
    contract: CapabilityGraphModuleContract,
    module_id: String,
    provider_namespace: String,
    role: CapabilityGraphModuleRole,
}

#[cfg(test)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityGraphModuleArtifacts {
    core_wasm: CapabilityGraphArtifact,
    serialized_module: CapabilityGraphArtifact,
}

#[cfg(test)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityGraphArtifact {
    path: String,
    sha256: String,
    size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CapabilityGraphModuleContract {
    pub(crate) imports: Vec<CapabilityGraphImportContract>,
    pub(crate) exports: Vec<CapabilityGraphExportContract>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CapabilityGraphImportContract {
    pub(crate) module: String,
    pub(crate) name: String,
    pub(crate) provider: CapabilityGraphImportProvider,
    #[serde(rename = "type")]
    pub(crate) ty: CapabilityGraphExternType,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum CapabilityGraphImportProvider {
    #[serde(rename = "host")]
    Host,
    #[serde(rename = "module")]
    Module { module_id: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CapabilityGraphExportContract {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) ty: CapabilityGraphExternType,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum CapabilityGraphExternType {
    #[serde(rename = "function")]
    Function {
        parameters: Vec<CapabilityGraphValueType>,
        results: Vec<CapabilityGraphValueType>,
    },
    #[serde(rename = "global")]
    Global {
        mutable: bool,
        value: CapabilityGraphValueType,
    },
    #[serde(rename = "memory")]
    Memory {
        maximum_pages: Option<u64>,
        memory64: bool,
        minimum_pages: u64,
        page_size_bytes: u64,
        shared: bool,
    },
    #[serde(rename = "table")]
    Table {
        element: CapabilityGraphValueType,
        maximum_elements: Option<u64>,
        minimum_elements: u64,
        table64: bool,
    },
    #[serde(rename = "tag")]
    Tag {
        parameters: Vec<CapabilityGraphValueType>,
        results: Vec<CapabilityGraphValueType>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum CapabilityGraphValueType {
    #[serde(rename = "i32")]
    I32,
    #[serde(rename = "i64")]
    I64,
    #[serde(rename = "f32")]
    F32,
    #[serde(rename = "f64")]
    F64,
    #[serde(rename = "v128")]
    V128,
    #[serde(rename = "funcref")]
    FuncRef,
    #[serde(rename = "externref")]
    ExternRef,
    #[serde(rename = "exnref")]
    ExnRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CapabilityGraphLayout {
    pub(crate) memory: CapabilityGraphExportReference,
    pub(crate) stack: CapabilityGraphStackLayout,
    pub(crate) table: CapabilityGraphExportReference,
    pub(crate) tags: Vec<CapabilityGraphExportReference>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CapabilityGraphExportReference {
    pub(crate) export_name: String,
    pub(crate) module_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CapabilityGraphStackLayout {
    pub(crate) alignment_bytes: u64,
    pub(crate) initial_pointer: u64,
    pub(crate) lower_bound_bytes: u64,
    pub(crate) pointer: CapabilityGraphExportReference,
    pub(crate) upper_bound_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedWasmUdfPackageIdentity {
    Legacy(WasmUdfExecutionManifest),
    CapabilityEntry(CapabilityEntryRuntimeIdentity),
    ModuleGraphCohort(ModuleGraphCohortRuntimeIdentity),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapabilityEntryRuntimeIdentity {
    entries: Vec<CapabilityEntryRuntimeUnitIdentity>,
    manifest_schema_version: u32,
    artifact_pipeline_sha256: String,
    compiler: CapabilityCompilerIdentity,
    official_output_chunk_entry_slots: Option<BTreeMap<String, u32>>,
    opaque_value_abi_version: u32,
    routes: Vec<CapabilityEntryRoute>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapabilityEntryRuntimeUnitIdentity {
    local_profile_sha256: String,
    entry_id: String,
    entry_path: String,
    entry_symbol: String,
    invocation_abi: Option<CapabilityInvocationAbi>,
    module_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ModuleGraphCohortRuntimeIdentity {
    cohort_contract_sha256: String,
    compiler: ModuleGraphCohortCompilerIdentity,
    entries: Vec<CapabilityEntryRuntimeUnitIdentity>,
    precompiler_material_identity: PrecompilerMaterialIdentity,
    routes: Vec<CapabilityEntryRoute>,
}

impl CapabilityEntryRuntimeIdentity {
    #[cfg(test)]
    pub(crate) fn entry_id(&self) -> &str {
        &self
            .entries
            .first()
            .expect("validated capability package has no entries")
            .entry_id
    }

    #[cfg(test)]
    pub(crate) fn entry_path(&self) -> &str {
        &self
            .entries
            .first()
            .expect("validated capability package has no entries")
            .entry_path
    }

    #[cfg(test)]
    pub(crate) fn module_path(&self) -> &str {
        &self
            .entries
            .first()
            .expect("validated capability package has no entries")
            .module_path
    }

    pub(crate) fn entries(&self) -> &[CapabilityEntryRuntimeUnitIdentity] {
        &self.entries
    }

    pub(crate) fn manifest_schema_version(&self) -> u32 {
        self.manifest_schema_version
    }

    pub(crate) fn artifact_pipeline_sha256(&self) -> &str {
        &self.artifact_pipeline_sha256
    }

    pub(crate) fn compiler_revision(&self) -> &str {
        &self.compiler.compiler_revision
    }

    pub(crate) fn static_hermes_revision(&self) -> &str {
        &self.compiler.static_hermes_revision
    }

    pub(crate) fn admitted_language_version(&self) -> u32 {
        self.compiler.admitted_language_version
    }

    pub(crate) fn opaque_value_abi_version(&self) -> u32 {
        self.opaque_value_abi_version
    }

    pub(crate) fn routes(&self) -> &[CapabilityEntryRoute] {
        &self.routes
    }

    pub(crate) fn official_output_chunk_entry_slot(&self, entry_selector: u64) -> Option<u32> {
        let entry_slots = self.official_output_chunk_entry_slots.as_ref()?;
        let entry_selector_id = format!("{entry_selector:016x}");
        let route = self
            .routes
            .iter()
            .find(|route| route.entry_selector_id == entry_selector_id)
            .expect("validated official-output chunk selector is missing its route");
        Some(
            *entry_slots
                .get(&route.entry_id)
                .expect("validated official-output chunk route is missing its entry slot"),
        )
    }
}

impl ModuleGraphCohortRuntimeIdentity {
    pub(crate) fn cohort_contract_sha256(&self) -> &str {
        &self.cohort_contract_sha256
    }

    pub(crate) fn compiler_revision(&self) -> &str {
        &self.compiler.compiler_revision
    }

    pub(crate) fn admitted_language_version(&self) -> u32 {
        self.compiler.admitted_language_version
    }

    pub(crate) fn artifact_pipeline_sha256(&self) -> &str {
        &self.compiler.artifact_pipeline_sha256
    }

    pub(crate) fn static_hermes_revision(&self) -> &str {
        &self.compiler.static_hermes_revision
    }

    pub(crate) fn routes(&self) -> &[CapabilityEntryRoute] {
        &self.routes
    }
}

impl CapabilityEntryRuntimeUnitIdentity {
    pub(crate) fn local_profile_sha256(&self) -> &str {
        &self.local_profile_sha256
    }

    pub(crate) fn entry_id(&self) -> &str {
        &self.entry_id
    }

    pub(crate) fn entry_path(&self) -> &str {
        &self.entry_path
    }

    pub(crate) fn entry_symbol(&self) -> &str {
        &self.entry_symbol
    }

    pub(crate) fn module_path(&self) -> &str {
        &self.module_path
    }
}

impl ValidatedWasmUdfPackageIdentity {
    pub(crate) fn legacy_udf_kind(&self) -> Option<UdfKind> {
        match self {
            Self::Legacy(manifest) => Some(manifest.source().udf_kind()),
            Self::CapabilityEntry(_) | Self::ModuleGraphCohort(_) => None,
        }
    }

    pub(crate) fn authenticates_route_lease(&self, lease: &DeploymentRouteLease) -> bool {
        match self {
            Self::Legacy(_) => true,
            Self::CapabilityEntry(identity) => identity.routes.iter().any(|route| {
                lease.entry_id.as_deref() == Some(route.entry_id.as_str())
                    && lease.route_id.as_deref() == Some(route.route_id.as_str())
                    && lease.entry_selector_id.as_deref() == Some(route.entry_selector_id.as_str())
                    && lease.export_name == route.export_name
                    && lease.udf_kind == route.udf_kind
                    && lease.visibility == route.visibility
            }),
            Self::ModuleGraphCohort(identity) => identity.routes.iter().any(|route| {
                lease.entry_id.as_deref() == Some(route.entry_id.as_str())
                    && lease.route_id.as_deref() == Some(route.route_id.as_str())
                    && lease.entry_selector_id.as_deref() == Some(route.entry_selector_id.as_str())
                    && lease.export_name == route.export_name
                    && lease.udf_kind == route.udf_kind
                    && lease.visibility == route.visibility
            }),
        }
    }

    pub(crate) fn opaque_value_abi_version(&self) -> u32 {
        match self {
            Self::Legacy(manifest) => manifest.opaque_value_abi_version(),
            Self::CapabilityEntry(identity) => identity.opaque_value_abi_version,
            Self::ModuleGraphCohort(_) => OPAQUE_VALUE_ABI_VERSION,
        }
    }

    pub(crate) fn requires_entry_selector(&self) -> bool {
        match self {
            Self::Legacy(manifest) => matches!(
                manifest.manifest_schema_version(),
                COHORT_MANIFEST_SCHEMA_VERSION
                    | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
                    | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION
            ),
            Self::CapabilityEntry(_) | Self::ModuleGraphCohort(_) => true,
        }
    }

    pub(crate) fn requires_selected_entry_prepare(&self) -> bool {
        true
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PackageEntry {
    kind: String,
    key: String,
    manifest_sha256: String,
    provenance: PackageArtifact,
    artifacts: BTreeMap<String, PackageArtifact>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PackageArtifact {
    sha256: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortPackageEntry {
    artifacts: BTreeMap<String, PackageArtifact>,
    key: String,
    kind: String,
    manifest_sha256: String,
    provenance: PackageArtifact,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BuildProvenance {
    artifact_pipeline_sha256: String,
    engine_identity: ProvenanceEngineIdentity,
    identities: JsonValue,
    kind: String,
    lowering_pipeline_sha256: String,
    materials: JsonValue,
    semantic_environment: JsonValue,
    source_pipeline_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProvenanceEngineIdentity {
    engine_compatibility_sha256: String,
    engine_config: ProvenanceEngineConfig,
    kind: String,
    target: ProvenanceEngineTarget,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProvenanceEngineConfig {
    consume_fuel: bool,
    epoch_interruption: bool,
    profiling_strategy: String,
    wasm_exceptions: bool,
}

#[derive(Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ProvenanceEngineTarget {
    cpu: String,
    triple: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeploymentArtifactPrecompiler {
    material_identity: PrecompilerMaterialIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PrecompilerMaterialIdentity {
    binary: PrecompilerBinaryIdentity,
    kind: String,
    manifest_kind: String,
    manifest_schema_version: u64,
    manifest_sha256: String,
    package_id: String,
    source_tree_sha256: String,
    target_triple: String,
    wasmtime_revision: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PrecompilerBinaryIdentity {
    sha256: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortManifest {
    abi: CohortAbi,
    artifacts: CohortArtifacts,
    bucket: u32,
    compiler: CohortCompilerIdentity,
    engine: CohortEngineIdentity,
    kind: String,
    members: Vec<CohortMember>,
    partition_policy: CohortPartitionPolicy,
    pipeline: CohortPipelineIdentity,
    runtime: JsonValue,
    schema_version: u32,
    toolchain: JsonValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortAbi {
    cohort_entry_selector_abi_version: u32,
    opaque_value_abi_version: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortArtifacts {
    core_wasm: PackageArtifact,
    serialized_module: PackageArtifact,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortCompilerIdentity {
    admitted_language_version: u32,
    compiler_revision: String,
    lowering_pipeline_sha256: String,
    source_pipeline_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityEntryManifest {
    abi: CapabilityEntryAbi,
    artifacts: CohortArtifacts,
    compiler: CapabilityCompilerIdentity,
    engine: CohortEngineIdentity,
    entry: Option<CapabilityEntryIdentity>,
    entries: Option<Vec<CapabilityEntryIdentity>>,
    execution: WasmUdfExecutionPolicy,
    kind: String,
    pipeline: CapabilityPipelineIdentity,
    routes: Vec<CapabilityEntryManifestRoute>,
    runtime: CapabilityRuntimeIdentity,
    schema_version: u32,
    toolchain: CapabilityToolchainIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GuestNativeJsonCodecIdentity {
    canonical_vector_corpus: CanonicalConvexValueVectorCorpus,
    generated_source: CapabilityValueCodecSource,
    kind: String,
    lowering_pipeline_sha256: String,
    schema_version: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CanonicalConvexValueVectorCorpus {
    kind: String,
    producer: CanonicalConvexValueVectorCorpusProducer,
    schema_version: u32,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CanonicalConvexValueVectorCorpusProducer {
    kind: String,
    source_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityRequestEnvelopeIdentity {
    capability_request_abi_version: u32,
    canonical_vector_corpus: CanonicalCapabilityRequestEnvelopeVectorCorpus,
    generated_source: CapabilityRequestEnvelopeSource,
    kind: String,
    lowering_pipeline_sha256: String,
    schema_version: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CanonicalCapabilityRequestEnvelopeVectorCorpus {
    kind: String,
    producer: CanonicalCapabilityRequestEnvelopeVectorCorpusProducer,
    schema_version: u32,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CanonicalCapabilityRequestEnvelopeVectorCorpusProducer {
    kind: String,
    source_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityRequestEnvelopeSource {
    sha256: String,
    size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityValueCodecSource {
    sha256: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityEntryAbi {
    capability_request_abi_version: u32,
    entry_selector_abi_version: u32,
    opaque_value_abi_version: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityEntryIdentity {
    local_profile: CapabilityLocalProfile,
    entry_id: String,
    entry_path: String,
    entry_symbol: Option<String>,
    invocation_abi: Option<CapabilityInvocationAbi>,
    module_path: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum CapabilityInvocationAbi {
    #[serde(rename = "convex-wasm-legacy-custom-context-handler-v1")]
    LegacyHandler,
    #[serde(rename = "convex-sdk-registration-wrapper-tagged-json-v1")]
    OfficialWrapper,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityLocalProfile {
    dependency_graph_sha256: String,
    javascript: PackageArtifact,
    metafile_sha256: String,
    sha256: String,
    source_map: PackageArtifact,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityCompilerIdentity {
    admitted_language_version: u32,
    compiler_revision: String,
    lowering_pipeline_sha256: String,
    source_pipeline_sha256: String,
    static_hermes_global_policy: CapabilityStaticHermesGlobalPolicyIdentity,
    static_hermes_revision: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesGlobalPolicyIdentity {
    inventory_sha256: String,
    kind: String,
    runtime_surface_policy_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ModuleGraphCohortContract {
    cohort_contract_sha256: String,
    cohort_id: String,
    compiler: ModuleGraphCohortCompilerIdentity,
    compiler_source_envelope_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_reuse_analysis: Option<ContextReuseCohortAnalysisIdentity>,
    descriptor_identity_sha256: String,
    engine: ModuleGraphCohortEngineIdentity,
    entries: Vec<ModuleGraphCohortEntryIdentity>,
    execution: WasmUdfExecutionPolicy,
    kind: String,
    precompiler_material_identity: PrecompilerMaterialIdentity,
    producer_implementation: CapabilityProducerImplementationIdentity,
    routes: Vec<ModuleGraphCohortRoute>,
    runtime_surface_policy_sha256: String,
    schedule_sha256: String,
    schema_version: u32,
    source_envelope_sha256: String,
    source_pipeline_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ContextReuseAnalysisIdentity {
    entries: Vec<String>,
    kind: String,
    policy_fingerprint: String,
    result_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ContextReuseCohortAnalysisIdentity {
    entries: Vec<String>,
    entry_graph_sha256s: Vec<String>,
    kind: String,
    policy_fingerprint: String,
    result_sha256: String,
    shared_analysis_sha256: String,
    third_party_material_fingerprints: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModuleGraphCohortEntryIdentity {
    entry_id: String,
    entry_path: String,
    entry_symbol: String,
    invocation_abi: CapabilityInvocationAbi,
    local_profile: CapabilityLocalProfile,
    module_path: String,
    source: JsonValue,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModuleGraphCohortCompilerIdentity {
    admitted_language_version: u32,
    artifact_pipeline_sha256: String,
    compiler_revision: String,
    lowering_pipeline_sha256: String,
    source_pipeline_sha256: String,
    static_hermes_global_policy: CapabilityStaticHermesGlobalPolicyIdentity,
    static_hermes_revision: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModuleGraphCohortEngineIdentity {
    compatibility_sha256: String,
    config: ProvenanceEngineConfig,
    configuration_sha256: String,
    package: PrecompilerMaterialIdentity,
    revision: String,
    target: GraphCohortEngineTarget,
    wasmtime_materials_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphCohortEngineTarget {
    cpu: String,
    triple: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ModuleGraphCohortRoute {
    entry_id: String,
    entry_selector_id: String,
    entry_symbol: String,
    export_name: String,
    route_id: String,
    udf_kind: UdfKind,
    visibility: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityPipelineIdentity {
    artifact_pipeline_sha256: String,
    kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    runtime_surface_policy_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityProducerImplementationIdentity {
    kind: String,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityProducerIdentity {
    kind: String,
    manifest: CapabilityProducerSourceIdentity,
    node_version: String,
    #[serde(default)]
    operational_sources: Vec<CapabilityProducerSourceIdentity>,
    sha256: String,
    sources: Vec<CapabilityProducerSourceIdentity>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityProducerSourceIdentity {
    path: String,
    sha256: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityRuntimeIdentity {
    archives_material_sha256: String,
    headers_material_sha256: String,
    main_material_sha256: String,
    main_object: PackageArtifact,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityToolchainIdentity {
    emscripten: CapabilityEmscriptenIdentity,
    static_hermes: CapabilityStaticHermesIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityEmscriptenIdentity {
    llvm_revision: String,
    materials_sha256: String,
    revision: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesIdentity {
    c_bundle: Option<CapabilityStaticHermesCBundleIdentity>,
    flags: Vec<String>,
    materials_sha256: String,
    revision: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesCBundleIdentity {
    compilation_sha256: String,
    kind: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapabilityEntryRoute {
    entry_id: String,
    entry_selector_id: String,
    entry_symbol: String,
    export_name: String,
    invocation_abi: Option<CapabilityInvocationAbi>,
    route_id: String,
    udf_kind: UdfKind,
    visibility: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityEntryManifestRoute {
    entry_id: Option<String>,
    entry_selector_id: String,
    entry_symbol: Option<String>,
    export_name: String,
    route_id: String,
    udf_kind: UdfKind,
    visibility: String,
}

impl CapabilityEntryRoute {
    pub(crate) fn entry_id(&self) -> &str {
        &self.entry_id
    }

    pub(crate) fn entry_selector_id(&self) -> &str {
        &self.entry_selector_id
    }

    pub(crate) fn export_name(&self) -> &str {
        &self.export_name
    }

    pub(crate) fn entry_symbol(&self) -> &str {
        &self.entry_symbol
    }

    pub(crate) fn route_id(&self) -> &str {
        &self.route_id
    }

    pub(crate) fn udf_kind(&self) -> UdfKind {
        self.udf_kind
    }

    pub(crate) fn visibility(&self) -> &str {
        &self.visibility
    }
}

enum CapabilityEntryBuildProvenance {
    V1(CapabilityEntryBuildProvenanceV1),
    V2(CapabilityEntryBuildProvenanceV2),
}

impl<'de> Deserialize<'de> for CapabilityEntryBuildProvenance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        let kind = value.get("kind").and_then(JsonValue::as_str);
        match kind {
            Some(CAPABILITY_ENTRY_PROVENANCE_KIND_V1) => serde_json::from_value(value)
                .map(Self::V1)
                .map_err(serde::de::Error::custom),
            Some(CAPABILITY_ENTRY_PROVENANCE_KIND_V2) => {
                match serde_json::from_value::<CapabilityEntryBuildProvenanceV2>(value.clone()) {
                    Ok(provenance) => Ok(Self::V2(provenance)),
                    Err(_) => normalize_capability_native_artifact_provenance_v2(value)
                        .and_then(|value| {
                            serde_json::from_value::<CapabilityEntryBuildProvenanceV2>(value)
                                .map_err(|error| error.to_string())
                        })
                        .map(Self::V2)
                        .map_err(serde::de::Error::custom),
                }
            },
            _ => Err(serde::de::Error::custom(
                "capability-entry provenance kind is unsupported",
            )),
        }
    }
}

// Native artifact identities version their producer binding independently.
// Version 2 moves the repeated producer implementation to the authenticated
// package-level producer identity.
fn normalize_capability_native_artifact_provenance_v2(
    mut provenance: JsonValue,
) -> Result<JsonValue, String> {
    let producer_implementation = {
        let producer_identity = provenance
            .get("producerIdentity")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| {
                "native artifact provenance producerIdentity must be an object".to_owned()
            })?;
        let kind = producer_identity
            .get("kind")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                "native artifact provenance producerIdentity.kind must be a string".to_owned()
            })?;
        let sha256 = producer_identity
            .get("sha256")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                "native artifact provenance producerIdentity.sha256 must be a string".to_owned()
            })?;
        serde_json::json!({ "kind": kind, "sha256": sha256 })
    };

    let normalize_application_unit = |unit: &mut JsonValue, description: &str| {
        let identities = unit
            .get_mut("identities")
            .and_then(JsonValue::as_object_mut)
            .ok_or_else(|| format!("{description}.identities must be an object"))?;
        normalize_capability_native_artifact_stage(
            identities
                .get_mut("generatedC")
                .ok_or_else(|| format!("{description}.identities.generatedC is missing"))?,
            &producer_implementation,
            &format!("{description}.identities.generatedC"),
        )?;
        normalize_capability_native_artifact_stage(
            identities
                .get_mut("exportObject")
                .ok_or_else(|| format!("{description}.identities.exportObject is missing"))?,
            &producer_implementation,
            &format!("{description}.identities.exportObject"),
        )
    };

    let application_units = provenance
        .get_mut("applicationUnits")
        .and_then(JsonValue::as_array_mut)
        .ok_or_else(|| "native artifact provenance applicationUnits must be an array".to_owned())?;
    for (index, unit) in application_units.iter_mut().enumerate() {
        normalize_application_unit(unit, &format!("applicationUnits[{index}]"))?;
    }

    let bridge = provenance
        .get_mut("bridge")
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| "native artifact provenance bridge must be an object".to_owned())?;
    normalize_capability_native_artifact_stage(
        bridge
            .get_mut("generatedC")
            .ok_or_else(|| "native artifact provenance bridge.generatedC is missing".to_owned())?,
        &producer_implementation,
        "bridge.generatedC",
    )?;
    normalize_capability_native_artifact_stage(
        bridge
            .get_mut("object")
            .ok_or_else(|| "native artifact provenance bridge.object is missing".to_owned())?,
        &producer_implementation,
        "bridge.object",
    )?;

    if let Some(formatter) = provenance.get_mut("formatter") {
        if !formatter.is_null() {
            let formatter = formatter.as_object_mut().ok_or_else(|| {
                "native artifact provenance formatter must be an object".to_owned()
            })?;
            {
                let generated_c = formatter.get("generatedC").ok_or_else(|| {
                    "native artifact provenance formatter.generatedC is missing".to_owned()
                })?;
                let generated_c_identity_sha256 = formatter
                    .get("object")
                    .and_then(JsonValue::as_object)
                    .and_then(|object| object.get("generatedCIdentitySha256"))
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| {
                        "native artifact provenance formatter.object.generatedCIdentitySha256 is \
                         missing"
                            .to_owned()
                    })?;
                if generated_c_identity_sha256 != canonical_json_sha256(generated_c)? {
                    return Err(
                        "native artifact provenance formatter generated-C identity hash is \
                         inconsistent"
                            .to_owned(),
                    );
                }
            }
            normalize_capability_native_artifact_stage(
                formatter.get_mut("generatedC").ok_or_else(|| {
                    "native artifact provenance formatter.generatedC is missing".to_owned()
                })?,
                &producer_implementation,
                "formatter.generatedC",
            )?;
            let normalized_generated_c_identity_sha256 = canonical_json_sha256(
                formatter
                    .get("generatedC")
                    .expect("formatter generated-C remains present"),
            )?;
            formatter
                .get_mut("object")
                .and_then(JsonValue::as_object_mut)
                .expect("formatter object remains present")
                .insert(
                    "generatedCIdentitySha256".to_owned(),
                    JsonValue::String(normalized_generated_c_identity_sha256),
                );
            normalize_capability_native_artifact_stage(
                formatter.get_mut("object").ok_or_else(|| {
                    "native artifact provenance formatter.object is missing".to_owned()
                })?,
                &producer_implementation,
                "formatter.object",
            )?;
        }
    }

    let identities = provenance
        .get_mut("identities")
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| "native artifact provenance identities must be an object".to_owned())?;
    for field in [
        "coreWasm",
        "runtimeMainObject",
        "selectorObject",
        "wasmtimeAot",
    ] {
        normalize_capability_native_artifact_stage(
            identities.get_mut(field).ok_or_else(|| {
                format!("native artifact provenance identities.{field} is missing")
            })?,
            &producer_implementation,
            &format!("identities.{field}"),
        )?;
    }

    if let Some(chunk_application) = provenance.get_mut("chunkApplication") {
        if !chunk_application.is_null() {
            let units = chunk_application
                .get_mut("units")
                .and_then(JsonValue::as_array_mut)
                .ok_or_else(|| {
                    "native artifact provenance chunkApplication.units must be an array".to_owned()
                })?;
            for (index, unit) in units.iter_mut().enumerate() {
                normalize_application_unit(unit, &format!("chunkApplication.units[{index}]"))?;
            }
        }
    }

    Ok(provenance)
}

fn normalize_capability_native_artifact_stage(
    stage: &mut JsonValue,
    producer_implementation: &JsonValue,
    description: &str,
) -> Result<(), String> {
    let stage = stage
        .as_object_mut()
        .ok_or_else(|| format!("native artifact provenance {description} must be an object"))?;
    if stage.contains_key("producerImplementation") {
        return Err(format!(
            "native artifact provenance {description} must not repeat producerImplementation"
        ));
    }
    if stage
        .remove("nativeArtifactIdentitySchemaVersion")
        .and_then(|value| value.as_u64())
        != Some(CAPABILITY_NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION)
    {
        return Err(format!(
            "native artifact provenance {description} has an unsupported identity schema version"
        ));
    }
    stage.insert(
        "producerImplementation".to_owned(),
        producer_implementation.clone(),
    );
    Ok(())
}

pub(super) fn canonical_json_sha256(value: &JsonValue) -> Result<String, String> {
    let mut bytes = Vec::new();
    write_canonical_json(value, &mut bytes)
        .map_err(|error| format!("native artifact provenance does not canonicalize: {error}"))?;
    Ok(sha256(&bytes))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityEntryBuildProvenanceV1 {
    artifact_pipeline_sha256: String,
    engine_identity: ProvenanceEngineIdentity,
    identities: CapabilityEntryProvenanceIdentities,
    kind: String,
    materials: JsonValue,
    semantic_environment: JsonValue,
    request_envelope: CapabilityRequestEnvelopeIdentity,
    value_codec: GuestNativeJsonCodecIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityEntryBuildProvenanceV2 {
    application_units: Vec<CapabilityApplicationUnitProvenance>,
    artifact_pipeline_sha256: String,
    bridge: CapabilityBridgeProvenance,
    chunk_application: Option<CapabilityOfficialOutputChunkApplicationProvenance>,
    engine_identity: ProvenanceEngineIdentity,
    formatter: Option<CapabilityFormatterProvenance>,
    identities: CapabilityEntryProvenanceIdentities,
    kind: String,
    materials: JsonValue,
    producer_identity: CapabilityProducerIdentity,
    semantic_environment: JsonValue,
    request_envelope: CapabilityRequestEnvelopeIdentity,
    value_codec: GuestNativeJsonCodecIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityApplicationUnitProvenance {
    application_unit_slot: Option<u32>,
    entry_id: String,
    entry_slot: u32,
    identities: CapabilityApplicationUnitIdentities,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityApplicationUnitIdentities {
    export_object: CapabilityApplicationObjectIdentity,
    generated_c: CapabilityApplicationGeneratedCIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityApplicationObjectIdentity {
    capability_chunk_application_unit: Option<CapabilityOfficialOutputChunkObjectUnitIdentity>,
    compile_flags: Vec<String>,
    effect_execution_mode: EffectExecutionMode,
    emscripten: CapabilityProvenanceEmscriptenIdentity,
    generated_c: PackageArtifact,
    generated_c_identity_sha256: Option<String>,
    opaque_value_abi_version: u32,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    runtime_headers: String,
    semantic_environment: JsonValue,
    static_hermes_c_bundle_member_compilation:
        Option<CapabilityStaticHermesCBundleMemberCompilationIdentity>,
    unit_role: String,
    value_mode: super::wasm_udf_manifest::ValueMode,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityApplicationGeneratedCIdentity {
    #[serde(skip_serializing_if = "Option::is_none")]
    capability_application_unit: Option<CapabilityApplicationGeneratedCPhysicalIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capability_chunk_application_unit: Option<CapabilityOfficialOutputChunkGeneratedCUnitIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capability_entry: Option<CapabilityApplicationGeneratedCEntryIdentity>,
    exported_unit_name: String,
    flags: Vec<String>,
    generated_source: PackageArtifact,
    #[serde(skip_serializing_if = "Option::is_none")]
    guest_source_provenance: Option<JsonValue>,
    opaque_value_abi_version: u32,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    semantic_environment: JsonValue,
    static_hermes: CapabilityProvenanceStaticHermesIdentity,
    unit_role: String,
    value_mode: super::wasm_udf_manifest::ValueMode,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityApplicationGeneratedCEntryIdentity {
    local_profile: CapabilityLocalProfile,
    entry_id: String,
    invocation_abi: Option<CapabilityInvocationAbi>,
    runtime_surface_policy_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityApplicationGeneratedCPhysicalIdentity {
    application_unit: CapabilityPhysicalApplicationUnitIdentity,
    entries: Vec<CapabilityApplicationGeneratedCPhysicalEntryIdentity>,
    runtime_surface_policy_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityApplicationGeneratedCPhysicalEntryIdentity {
    entry_id: String,
    entry_path: String,
    invocation_abi: Option<CapabilityInvocationAbi>,
    local_profile_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityPhysicalApplicationUnitIdentity {
    compiler_mode: String,
    entries: Vec<CapabilityPhysicalApplicationUnitEntryIdentity>,
    entry_symbol: String,
    exported_unit_name: String,
    identity_sha256: String,
    kind: String,
    unit_count: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityPhysicalApplicationUnitEntryIdentity {
    entry_path: String,
    handoff_slot: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityBridgeProvenance {
    generated_c: CapabilityBridgeGeneratedCIdentity,
    object: CapabilityBridgeObjectIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityBridgeGeneratedCIdentity {
    effect_execution_mode: EffectExecutionMode,
    exported_unit_name: String,
    flags: Vec<String>,
    generated_source: PackageArtifact,
    opaque_value_abi_version: u32,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    request_envelope: CapabilityRequestEnvelopeIdentity,
    semantic_environment: JsonValue,
    static_hermes: CapabilityProvenanceStaticHermesIdentity,
    unit_role: String,
    value_codec: GuestNativeJsonCodecIdentity,
    value_mode: super::wasm_udf_manifest::ValueMode,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityBridgeObjectIdentity {
    compile_flags: Vec<String>,
    emscripten: CapabilityProvenanceEmscriptenIdentity,
    generated_c: PackageArtifact,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    runtime_headers: String,
    semantic_environment: JsonValue,
    static_hermes_c_bundle_member_compilation:
        Option<CapabilityStaticHermesCBundleMemberCompilationIdentity>,
    unit_role: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityFormatterProvenance {
    generated_c: CapabilityFormatterGeneratedCIdentity,
    generated_source: PackageArtifact,
    object: CapabilityFormatterObjectIdentity,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityFormatterGeneratedCIdentity {
    exported_unit_name: String,
    flags: Vec<String>,
    generated_source: PackageArtifact,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    semantic_environment: JsonValue,
    static_hermes: CapabilityProvenanceStaticHermesIdentity,
    unit_role: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityFormatterObjectIdentity {
    compile_flags: Vec<String>,
    emscripten: CapabilityProvenanceEmscriptenIdentity,
    generated_c: PackageArtifact,
    generated_c_identity_sha256: String,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    runtime_headers: String,
    semantic_environment: JsonValue,
    static_hermes_c_bundle_member_compilation:
        Option<CapabilityStaticHermesCBundleMemberCompilationIdentity>,
    unit_role: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityProvenanceEmscriptenIdentity {
    llvm_revision: String,
    materials: String,
    revision: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityProvenanceStaticHermesIdentity {
    materials: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    precompile_process: Option<CapabilityStaticHermesPrecompileProcessIdentity>,
    revision: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesPrecompileProcessIdentity {
    c_bundle_member_compilation_policy:
        Option<CapabilityStaticHermesCBundleMemberCompilationPolicy>,
    compiler_arguments_sha256: String,
    kind: String,
    launcher_arguments_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesCBundleMemberCompilationPolicy {
    c_optimization_level_zero: CapabilityStaticHermesCBundleOptimizationLevelZero,
    kind: String,
    normal_optimization_flag: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesCBundleOptimizationLevelZero {
    c_optimization_level: u32,
    function_count: u32,
    optimization_flag: String,
    role: String,
    stage: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesCBundleMemberCompilationIdentity {
    kind: String,
    member_compilations: Vec<CapabilityStaticHermesCBundleMemberCompilation>,
    relocatable_link: CapabilityStaticHermesCBundleRelocatableLink,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesCBundleRelocatableLink {
    arguments: Vec<String>,
    executable: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesCBundleMemberCompilation {
    effective_arguments: Vec<String>,
    member: CapabilityStaticHermesCBundleMemberIdentity,
    optimization: String,
    stage: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
enum CapabilityStaticHermesCBundleMemberIdentity {
    Metadata(CapabilityStaticHermesCBundleMetadataMemberIdentity),
    Function(CapabilityStaticHermesCBundleFunctionMemberIdentity),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesCBundleMetadataMemberIdentity {
    path: String,
    role: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityStaticHermesCBundleFunctionMemberIdentity {
    #[serde(skip_serializing_if = "Option::is_none")]
    c_optimization_level: Option<u32>,
    first_function_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    function_fragment_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    function_fragment_index: Option<u32>,
    function_count: u32,
    last_function_id: u32,
    oversize: bool,
    path: String,
    role: String,
    target_bytes: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkApplicationProvenance {
    descriptor: CapabilityOfficialOutputChunkApplicationDescriptor,
    units: Vec<CapabilityOfficialOutputChunkCompiledUnit>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkApplicationDescriptor {
    entries: Vec<CapabilityOfficialOutputChunkEntry>,
    identity_sha256: String,
    initialization: CapabilityOfficialOutputChunkInitialization,
    kind: String,
    native_descriptor: CapabilityOfficialOutputChunkNativeDescriptor,
    units: Vec<CapabilityOfficialOutputChunkUnit>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkEntry {
    dependency_graph_sha256: String,
    entry_module_path: String,
    entry_path: String,
    entry_publication_unit_slot: u32,
    entry_slot: u32,
    handoff_slot: u32,
    module_path: String,
    routes: Vec<CapabilityOfficialOutputChunkRoute>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkRoute {
    export_name: String,
    udf_kind: UdfKind,
    visibility: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkInitialization {
    chunk_slot_count: u32,
    entry_publication_unit_slots: Vec<u32>,
    kind: String,
    namespace_slot_count: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkNativeDescriptor {
    destruction: String,
    initialization: String,
    kind: String,
    publication: String,
    slots: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkUnit {
    application_unit_slot: u32,
    chunk_slot: i32,
    dependencies: Vec<CapabilityOfficialOutputChunkDependency>,
    entry_publication: bool,
    entry_symbol: String,
    exported_unit_name: String,
    identity_sha256: String,
    javascript: PackageArtifact,
    kind: String,
    publication_handoff_slot: i32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkDependency {
    kind: String,
    path: String,
    slot: u32,
    specifier: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkCompiledUnit {
    application_unit_slot: u32,
    identities: CapabilityApplicationUnitIdentities,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkGeneratedCUnitIdentity {
    entry_publication: bool,
    identity_sha256: String,
    kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkObjectUnitIdentity {
    identity_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkCoreWasmIdentity {
    abi: CapabilitySplitCoreWasmAbi,
    application_units: Vec<CapabilityOfficialOutputChunkCoreWasmUnit>,
    bucket: u32,
    emscripten: CapabilitySplitCoreWasmEmscripten,
    link_flags: Vec<String>,
    members: Vec<CapabilitySplitCoreWasmMember>,
    partition_policy: CohortPartitionPolicy,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    runtime: CapabilitySplitCoreWasmRuntime,
    selector_object: PackageArtifact,
    semantic_environment: JsonValue,
    static_hermes: CapabilitySplitCoreWasmStaticHermes,
    target: CohortEngineTarget,
    unit_topology: CapabilityOfficialOutputChunkCoreWasmTopology,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkCoreWasmUnit {
    application_unit: CapabilityOfficialOutputChunkUnit,
    application_unit_slot: u32,
    generated_c: PackageArtifact,
    identities: CapabilityApplicationUnitIdentities,
    object: PackageArtifact,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkCoreWasmTopology {
    application_entry_count: u32,
    application_entry_count_symbol: String,
    application_factory_by_unit_slot_symbol: String,
    application_flags: Vec<String>,
    application_unit_count: u32,
    application_unit_count_symbol: String,
    application_unit_maximum: u32,
    bridge: CapabilitySplitCoreWasmBridge,
    bridge_flags: Vec<String>,
    chunk_application: CapabilityOfficialOutputChunkApplicationDescriptor,
    formatter: CapabilitySplitCoreWasmFormatter,
    formatter_flags: Vec<String>,
    initialization_order: Vec<String>,
    kind: String,
    selected_entry_preparation: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkRuntimeMainIdentity {
    cohort_entry_selector_abi_version: u32,
    compile_flags: Vec<String>,
    effect_execution_mode: EffectExecutionMode,
    emscripten: CapabilityProvenanceEmscriptenIdentity,
    entry_selector_symbol: String,
    opaque_value_abi_version: u32,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    runtime_headers: String,
    runtime_main: String,
    runtime_main_language: String,
    semantic_environment: JsonValue,
    unit_topology: CapabilityOfficialOutputChunkRuntimeMainTopology,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkRuntimeMainTopology {
    application_entry_count: u32,
    application_entry_count_symbol: String,
    application_factory_by_unit_slot_symbol: String,
    application_selector_symbol: String,
    application_unit_count: u32,
    application_unit_count_symbol: String,
    application_unit_maximum: u32,
    bridge_entry_symbol: String,
    bridge_exported_unit_name: String,
    formatter_entry_symbol: String,
    formatter_exported_unit_name: String,
    initialization_order: Vec<String>,
    kind: String,
    native_descriptor: CapabilityOfficialOutputChunkNativeDescriptor,
    selected_entry_preparation: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityOfficialOutputChunkSelectorIdentity {
    abi_version: u32,
    application_entry_count: u32,
    application_entry_count_symbol: String,
    application_factory_by_unit_slot_symbol: String,
    application_unit_count: u32,
    application_unit_count_symbol: String,
    chunk_application: CapabilityOfficialOutputChunkApplicationDescriptor,
    kind: String,
    members: Vec<CapabilitySelectorMember>,
    partition_policy: CohortPartitionPolicy,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    semantic_environment: JsonValue,
    source_sha256: String,
    toolchain: CapabilitySelectorToolchainIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySelectorToolchainIdentity {
    compile_flags: Vec<String>,
    emscripten_materials: String,
    emscripten_revision: String,
    llvm_revision: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityPerEntrySelectorIdentity {
    abi_version: u32,
    application_entry_count: u32,
    application_entry_count_symbol: String,
    application_factory_by_slot_symbol: String,
    kind: String,
    members: Vec<CapabilitySelectorMember>,
    partition_policy: CohortPartitionPolicy,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    semantic_environment: JsonValue,
    source_sha256: String,
    toolchain: CapabilitySelectorToolchainIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityMultiEntryUnitSelectorIdentity {
    abi_version: u32,
    application_entry_count: u32,
    application_entry_count_symbol: String,
    application_factory_by_unit_slot_symbol: String,
    application_unit: CapabilityPhysicalApplicationUnitIdentity,
    application_unit_count: u32,
    application_unit_count_symbol: String,
    kind: String,
    members: Vec<CapabilitySelectorMember>,
    partition_policy: CohortPartitionPolicy,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    semantic_environment: JsonValue,
    source_sha256: String,
    toolchain: CapabilitySelectorToolchainIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitSchema3SelectorIdentity {
    abi_version: u32,
    bucket: u32,
    entry_id: String,
    entry_slot: u32,
    entry_symbol: String,
    kind: String,
    members: Vec<JsonValue>,
    partition_policy: CohortPartitionPolicy,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    semantic_environment: JsonValue,
    source_sha256: String,
    toolchain: CapabilitySelectorToolchainIdentity,
    unit_role: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySelectorMember {
    entry_selector_id: String,
    entry_symbol: String,
    handler_export_name: String,
    handler_udf_kind: UdfKind,
    invocation_abi: CapabilityInvocationAbi,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityEntryProvenanceIdentities {
    core_wasm: JsonValue,
    runtime_main_object: JsonValue,
    selector_object: JsonValue,
    wasmtime_aot: JsonValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmIdentity {
    abi: CapabilitySplitCoreWasmAbi,
    application_units: Option<Vec<CapabilitySplitCoreWasmApplicationUnit>>,
    bucket: u32,
    emscripten: CapabilitySplitCoreWasmEmscripten,
    link_flags: Vec<String>,
    members: Vec<CapabilitySplitCoreWasmMember>,
    partition_policy: CohortPartitionPolicy,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    runtime: CapabilitySplitCoreWasmRuntime,
    selector_object: PackageArtifact,
    semantic_environment: JsonValue,
    static_hermes: CapabilitySplitCoreWasmStaticHermes,
    target: CohortEngineTarget,
    unit_topology: CapabilitySplitCoreWasmTopology,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmAbi {
    capability_entry_selector_abi_version: u32,
    capability_request_abi_version: u32,
    opaque_value_abi_version: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmEmscripten {
    compile_flags: Vec<String>,
    llvm_revision: String,
    materials: String,
    revision: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmMember {
    application_unit_slot: Option<u32>,
    effect_execution_mode: EffectExecutionMode,
    entry_id: String,
    entry_slot: u32,
    entry_symbol: String,
    invocation_abi: Option<CapabilityInvocationAbi>,
    object: PackageArtifact,
    routes: Vec<CapabilitySplitCoreWasmRoute>,
    runtime_surface_policy_sha256: String,
    value_mode: super::wasm_udf_manifest::ValueMode,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmApplicationUnit {
    application_unit: CapabilityPhysicalApplicationUnitIdentity,
    application_unit_slot: u32,
    object: PackageArtifact,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmRoute {
    entry_selector_id: String,
    export_name: String,
    route_id: String,
    udf_kind: UdfKind,
    visibility: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmRuntime {
    archives: String,
    main_object: PackageArtifact,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmStaticHermes {
    application_flags: Vec<String>,
    bridge_flags: Vec<String>,
    formatter_flags: Option<Vec<String>>,
    materials: String,
    revision: String,
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum CapabilitySplitCoreWasmTopology {
    #[serde(rename = "convex-wasm-capability-unit-topology-v1")]
    V1(CapabilitySplitCoreWasmTopologyV1),
    #[serde(rename = "convex-wasm-capability-unit-topology-v2")]
    V2(CapabilitySplitCoreWasmTopologyV2),
    #[serde(rename = "convex-wasm-capability-unit-topology-v3")]
    V3(CapabilitySplitCoreWasmTopologyV3),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmTopologyV1 {
    application_entry_count: Option<u32>,
    application_entry_count_symbol: Option<String>,
    application_factory_by_slot_symbol: Option<String>,
    application_flags: Vec<String>,
    bridge: CapabilitySplitCoreWasmBridge,
    bridge_flags: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmTopologyV2 {
    application_entry_count: Option<u32>,
    application_entry_count_symbol: Option<String>,
    application_factory_by_slot_symbol: Option<String>,
    application_flags: Vec<String>,
    bridge: CapabilitySplitCoreWasmBridge,
    bridge_flags: Vec<String>,
    formatter: CapabilitySplitCoreWasmFormatter,
    formatter_flags: Vec<String>,
    initialization_order: Vec<String>,
    selected_entry_preparation: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmTopologyV3 {
    application_entry_count: u32,
    application_entry_count_symbol: String,
    application_factory_by_unit_slot_symbol: String,
    application_flags: Vec<String>,
    application_unit: CapabilityPhysicalApplicationUnitIdentity,
    application_unit_count: u32,
    application_unit_count_symbol: String,
    bridge: CapabilitySplitCoreWasmBridge,
    bridge_flags: Vec<String>,
    formatter: CapabilitySplitCoreWasmFormatter,
    formatter_flags: Vec<String>,
    initialization_order: Vec<String>,
    selected_entry_preparation: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmBridge {
    entry_symbol: String,
    generated_source: PackageArtifact,
    object: PackageArtifact,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitCoreWasmFormatter {
    entry_symbol: String,
    generated_source: PackageArtifact,
    object: PackageArtifact,
}

struct CapabilitySplitCoreWasmTopologyFields<'a> {
    application_entry_count: Option<u32>,
    application_entry_count_symbol: Option<&'a str>,
    application_factory_by_slot_symbol: Option<&'a str>,
    application_factory_by_unit_slot_symbol: Option<&'a str>,
    application_unit: Option<&'a CapabilityPhysicalApplicationUnitIdentity>,
    application_unit_count: Option<u32>,
    application_unit_count_symbol: Option<&'a str>,
    selected_entry_preparation: Option<&'a str>,
    application_flags: &'a [String],
    bridge: &'a CapabilitySplitCoreWasmBridge,
    bridge_flags: &'a [String],
}

impl CapabilitySplitCoreWasmTopology {
    fn fields(&self) -> CapabilitySplitCoreWasmTopologyFields<'_> {
        let (
            application_entry_count,
            application_entry_count_symbol,
            application_factory_by_slot_symbol,
            application_factory_by_unit_slot_symbol,
            application_unit,
            application_unit_count,
            application_unit_count_symbol,
            selected_entry_preparation,
            application_flags,
            bridge,
            bridge_flags,
        ) = match self {
            Self::V1(topology) => (
                topology.application_entry_count,
                topology.application_entry_count_symbol.as_deref(),
                topology.application_factory_by_slot_symbol.as_deref(),
                None,
                None,
                None,
                None,
                None,
                topology.application_flags.as_slice(),
                &topology.bridge,
                topology.bridge_flags.as_slice(),
            ),
            Self::V2(topology) => (
                topology.application_entry_count,
                topology.application_entry_count_symbol.as_deref(),
                topology.application_factory_by_slot_symbol.as_deref(),
                None,
                None,
                None,
                None,
                Some(topology.selected_entry_preparation.as_str()),
                topology.application_flags.as_slice(),
                &topology.bridge,
                topology.bridge_flags.as_slice(),
            ),
            Self::V3(topology) => (
                Some(topology.application_entry_count),
                Some(topology.application_entry_count_symbol.as_str()),
                None,
                Some(topology.application_factory_by_unit_slot_symbol.as_str()),
                Some(&topology.application_unit),
                Some(topology.application_unit_count),
                Some(topology.application_unit_count_symbol.as_str()),
                Some(topology.selected_entry_preparation.as_str()),
                topology.application_flags.as_slice(),
                &topology.bridge,
                topology.bridge_flags.as_slice(),
            ),
        };
        CapabilitySplitCoreWasmTopologyFields {
            application_entry_count,
            application_entry_count_symbol,
            application_factory_by_slot_symbol,
            application_factory_by_unit_slot_symbol,
            application_unit,
            application_unit_count,
            application_unit_count_symbol,
            selected_entry_preparation,
            application_flags,
            bridge,
            bridge_flags,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitRuntimeMainIdentity {
    cohort_entry_selector_abi_version: u32,
    compile_flags: Vec<String>,
    effect_execution_mode: EffectExecutionMode,
    emscripten: CapabilityProvenanceEmscriptenIdentity,
    entry_selector_symbol: String,
    opaque_value_abi_version: u32,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    runtime_headers: String,
    runtime_main: String,
    runtime_main_language: String,
    semantic_environment: JsonValue,
    unit_topology: CapabilitySplitRuntimeMainTopology,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityWasmtimeAotIdentity {
    core_wasm: PackageArtifact,
    engine_config: ProvenanceEngineConfig,
    pipeline_kind: String,
    producer_implementation: CapabilityProducerImplementationIdentity,
    semantic_environment: JsonValue,
    target: CohortEngineTarget,
    wasmtime: CapabilityWasmtimeIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityWasmtimeIdentity {
    materials: String,
    package: PrecompilerMaterialIdentity,
    revision: String,
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum CapabilitySplitRuntimeMainTopology {
    #[serde(rename = "convex-wasm-capability-unit-topology-v1")]
    V1(CapabilitySplitRuntimeMainTopologyV1),
    #[serde(rename = "convex-wasm-capability-unit-topology-v2")]
    V2(CapabilitySplitRuntimeMainTopologyV2),
    #[serde(rename = "convex-wasm-capability-unit-topology-v3")]
    V3(CapabilitySplitRuntimeMainTopologyV3),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitRuntimeMainTopologyV1 {
    application_entry_count: Option<u32>,
    application_entry_count_symbol: Option<String>,
    application_factory_by_slot_symbol: Option<String>,
    application_selector_symbol: String,
    bridge_entry_symbol: String,
    bridge_exported_unit_name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitRuntimeMainTopologyV2 {
    application_entry_count: Option<u32>,
    application_entry_count_symbol: Option<String>,
    application_factory_by_slot_symbol: Option<String>,
    application_selector_symbol: String,
    bridge_entry_symbol: String,
    bridge_exported_unit_name: String,
    formatter_entry_symbol: String,
    formatter_exported_unit_name: String,
    initialization_order: Vec<String>,
    selected_entry_preparation: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilitySplitRuntimeMainTopologyV3 {
    application_entry_count: u32,
    application_entry_count_symbol: String,
    application_factory_by_unit_slot_symbol: String,
    application_selector_symbol: String,
    application_unit: CapabilityPhysicalApplicationUnitIdentity,
    application_unit_count: u32,
    application_unit_count_symbol: String,
    bridge_entry_symbol: String,
    bridge_exported_unit_name: String,
    formatter_entry_symbol: String,
    formatter_exported_unit_name: String,
    initialization_order: Vec<String>,
    selected_entry_preparation: String,
}

struct CapabilitySplitRuntimeMainTopologyFields<'a> {
    application_entry_count: Option<u32>,
    application_entry_count_symbol: Option<&'a str>,
    application_factory_by_slot_symbol: Option<&'a str>,
    application_factory_by_unit_slot_symbol: Option<&'a str>,
    application_unit: Option<&'a CapabilityPhysicalApplicationUnitIdentity>,
    application_unit_count: Option<u32>,
    application_unit_count_symbol: Option<&'a str>,
    selected_entry_preparation: Option<&'a str>,
    application_selector_symbol: &'a str,
    bridge_entry_symbol: &'a str,
    bridge_exported_unit_name: &'a str,
}

impl CapabilitySplitRuntimeMainTopology {
    fn fields(&self) -> CapabilitySplitRuntimeMainTopologyFields<'_> {
        let (
            application_entry_count,
            application_entry_count_symbol,
            application_factory_by_slot_symbol,
            application_factory_by_unit_slot_symbol,
            application_unit,
            application_unit_count,
            application_unit_count_symbol,
            selected_entry_preparation,
            application_selector_symbol,
            bridge_entry_symbol,
            bridge_exported_unit_name,
        ) = match self {
            Self::V1(topology) => (
                topology.application_entry_count,
                topology.application_entry_count_symbol.as_deref(),
                topology.application_factory_by_slot_symbol.as_deref(),
                None,
                None,
                None,
                None,
                None,
                topology.application_selector_symbol.as_str(),
                topology.bridge_entry_symbol.as_str(),
                topology.bridge_exported_unit_name.as_str(),
            ),
            Self::V2(topology) => (
                topology.application_entry_count,
                topology.application_entry_count_symbol.as_deref(),
                topology.application_factory_by_slot_symbol.as_deref(),
                None,
                None,
                None,
                None,
                Some(topology.selected_entry_preparation.as_str()),
                topology.application_selector_symbol.as_str(),
                topology.bridge_entry_symbol.as_str(),
                topology.bridge_exported_unit_name.as_str(),
            ),
            Self::V3(topology) => (
                Some(topology.application_entry_count),
                Some(topology.application_entry_count_symbol.as_str()),
                None,
                Some(topology.application_factory_by_unit_slot_symbol.as_str()),
                Some(&topology.application_unit),
                Some(topology.application_unit_count),
                Some(topology.application_unit_count_symbol.as_str()),
                Some(topology.selected_entry_preparation.as_str()),
                topology.application_selector_symbol.as_str(),
                topology.bridge_entry_symbol.as_str(),
                topology.bridge_exported_unit_name.as_str(),
            ),
        };
        CapabilitySplitRuntimeMainTopologyFields {
            application_entry_count,
            application_entry_count_symbol,
            application_factory_by_slot_symbol,
            application_factory_by_unit_slot_symbol,
            application_unit,
            application_unit_count,
            application_unit_count_symbol,
            selected_entry_preparation,
            application_selector_symbol,
            bridge_entry_symbol,
            bridge_exported_unit_name,
        }
    }
}

fn capability_runtime_support_unit_role(
    entry_selector_abi_version: u32,
) -> Result<&'static str, WasmUdfPackageError> {
    match entry_selector_abi_version {
        LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION => Ok("shared-untyped-console-formatter"),
        CAPABILITY_ENTRY_SELECTOR_ABI_VERSION => Ok("shared-untyped-runtime-support"),
        _ => Err(invalid_entry(
            "capability-entry selector ABI has no runtime-support contract",
        )),
    }
}

fn has_capability_unit_initialization_order(order: &[String], runtime_support_role: &str) -> bool {
    order.iter().map(String::as_str).eq([
        "shared-typed-capability-bridge",
        runtime_support_role,
        "untyped-applications-by-entry-slot",
    ])
}

fn has_capability_multi_entry_unit_initialization_order(
    order: &[String],
    runtime_support_role: &str,
) -> bool {
    order.iter().map(String::as_str).eq([
        "shared-typed-capability-bridge",
        runtime_support_role,
        "untyped-application-units-by-unit-slot",
    ])
}

fn capability_formatter_topology_versions_match(
    core: &CapabilitySplitCoreWasmTopology,
    runtime: &CapabilitySplitRuntimeMainTopology,
    formatter_present: bool,
) -> bool {
    match (core, runtime, formatter_present) {
        (
            CapabilitySplitCoreWasmTopology::V1(_),
            CapabilitySplitRuntimeMainTopology::V1(_),
            false,
        ) => true,
        (
            CapabilitySplitCoreWasmTopology::V2(core),
            CapabilitySplitRuntimeMainTopology::V2(runtime),
            true,
        ) => {
            core.selected_entry_preparation == CAPABILITY_SELECTED_ENTRY_PREPARATION
                && runtime.selected_entry_preparation == core.selected_entry_preparation
        },
        (
            CapabilitySplitCoreWasmTopology::V3(core),
            CapabilitySplitRuntimeMainTopology::V3(runtime),
            true,
        ) => {
            core.selected_entry_preparation == CAPABILITY_SELECTED_ENTRY_PREPARATION
                && runtime.selected_entry_preparation == core.selected_entry_preparation
        },
        _ => false,
    }
}

fn capability_official_output_chunk_selected_entry_preparation_matches(
    core: &CapabilityOfficialOutputChunkCoreWasmTopology,
    runtime: &CapabilityOfficialOutputChunkRuntimeMainTopology,
) -> bool {
    core.selected_entry_preparation == CAPABILITY_SELECTED_ENTRY_PREPARATION
        && runtime.selected_entry_preparation == core.selected_entry_preparation
}

fn is_capability_runtime_main_topology_compile_flag(flag: &str) -> bool {
    matches!(
        flag,
        MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG | CHUNK_APPLICATION_UNIT_COMPILE_FLAG
    )
}

fn capability_runtime_main_compile_flags_match(
    core_compile_flags: &[String],
    runtime_compile_flags: &[String],
    required_topology_flag: Option<&str>,
) -> bool {
    // The authenticated runtime-main stage identity records its complete flag
    // vector and the manifest binds the resulting object. Main-only flags are
    // therefore an authenticated suffix; the consumer-owned cross-object
    // contract is the exact core prefix and a closed topology mode. ABI and
    // topology fields remain authenticated and checked independently.
    if core_compile_flags
        .iter()
        .any(|flag| is_capability_runtime_main_topology_compile_flag(flag))
    {
        return false;
    }
    let Some(runtime_main_flags) = runtime_compile_flags.strip_prefix(core_compile_flags) else {
        return false;
    };
    let main_only_flags = match required_topology_flag {
        Some(required_flag) => {
            let Some((actual_flag, main_only_flags)) = runtime_main_flags.split_last() else {
                return false;
            };
            if actual_flag != required_flag {
                return false;
            }
            main_only_flags
        },
        None => runtime_main_flags,
    };
    !main_only_flags
        .iter()
        .any(|flag| is_capability_runtime_main_topology_compile_flag(flag))
}

#[derive(Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortEngineIdentity {
    compatibility_sha256: String,
    config: ProvenanceEngineConfig,
    configuration_sha256: String,
    package: PrecompilerMaterialIdentity,
    revision: String,
    target: CohortEngineTarget,
}

#[derive(Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct CohortEngineTarget {
    cpu: String,
    platform: JsonValue,
    triple: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortPartitionPolicy {
    bucket_count: u32,
    hash: String,
    kind: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortPipelineIdentity {
    artifact_pipeline_sha256: String,
    kind: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortMember {
    #[serde(default)]
    effect_execution_mode: Option<EffectExecutionMode>,
    entry_id: String,
    entry_selector_id: String,
    entry_symbol: String,
    generated_c: PackageArtifact,
    imported_operations: Vec<ImportedOperation>,
    lowering: CohortMemberLowering,
    object: PackageArtifact,
    opaque_value_abi_version: u32,
    route: CohortRoute,
    route_id: String,
    selection_index: u32,
    source: CohortMemberSource,
    value_mode: super::wasm_udf_manifest::ValueMode,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortMemberLowering {
    admitted_language_version: u32,
    lowering_pipeline_sha256: String,
    source_pipeline_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortRoute {
    export_name: String,
    kind: String,
    runtime_module_path: String,
    udf_kind: UdfKind,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortMemberSource {
    export_sha256: String,
    generated_java_script: PackageArtifact,
    resolved_graph_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortBuildProvenance {
    artifact_pipeline_sha256: String,
    engine_identity: ProvenanceEngineIdentity,
    identities: JsonValue,
    kind: String,
    materials: JsonValue,
    members: Vec<CohortProvenanceMember>,
    semantic_environment: JsonValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortProvenanceMember {
    identities: JsonValue,
    route_id: String,
}

#[cfg(test)]
fn test_precompiler_material_identity() -> PrecompilerMaterialIdentity {
    PrecompilerMaterialIdentity {
        binary: PrecompilerBinaryIdentity {
            sha256: "3333333333333333333333333333333333333333333333333333333333333333".to_owned(),
            size: 1,
        },
        kind: VERIFIED_PRECOMPILER_PACKAGE_KIND.to_owned(),
        manifest_kind: PRECOMPILER_PACKAGE_MANIFEST_KIND.to_owned(),
        manifest_schema_version: PRECOMPILER_PACKAGE_MANIFEST_SCHEMA_VERSION,
        manifest_sha256: "4444444444444444444444444444444444444444444444444444444444444444"
            .to_owned(),
        package_id: "5555555555555555555555555555555555555555555555555555555555555555".to_owned(),
        source_tree_sha256: "6666666666666666666666666666666666666666666666666666666666666666"
            .to_owned(),
        target_triple: "x86_64-unknown-linux-gnu".to_owned(),
        wasmtime_revision: "wasmtime-test-revision".to_owned(),
    }
}

#[cfg(test)]
pub(crate) fn test_precompiler_material_identity_json() -> JsonValue {
    serde_json::to_value(test_precompiler_material_identity())
        .expect("test precompiler material identity must serialize")
}

pub(crate) enum DeploymentExportPackage {
    Wasm(ValidatedWasmUdfPackage),
    ExistingRuntime,
    V8Fallback,
}

pub(crate) struct AuthenticatedModuleGraphRouteMaterial {
    pub(crate) execution: WasmUdfExecutionPolicy,
    pub(crate) identity: ValidatedWasmUdfPackageIdentity,
    pub(crate) package_key: String,
    pub(crate) entry_selector: u64,
    pub(crate) permitted_conditional_convex_imports: BTreeSet<&'static str>,
}

pub(crate) enum DeploymentExportRouting<'a> {
    Wasm {
        package_key: &'a str,
        runtime_entry: DeploymentRuntimeEntry<'a>,
        route_lease: DeploymentRouteLease,
        deployed_runtime_identity: &'a DeployedRuntimeIdentity,
    },
    ExistingRuntime,
    V8Fallback,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeploymentRuntimeEntry<'a> {
    LegacyExport {
        runtime_module_path: &'a str,
        export_name: &'a str,
    },
    CapabilityPackage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeploymentRouteLease {
    entry_id: Option<String>,
    entry_selector_id: Option<String>,
    export_name: String,
    route_id: Option<String>,
    udf_kind: UdfKind,
    visibility: String,
}

impl DeploymentRouteLease {
    pub(crate) fn entry_selector(&self) -> Option<u64> {
        self.entry_selector_id.as_ref().map(|selector| {
            u64::from_str_radix(selector, 16)
                .expect("validated deployment route selector became invalid")
        })
    }

    pub(crate) fn entry_id(&self) -> Option<&str> {
        self.entry_id.as_deref()
    }

    pub(crate) fn export_name(&self) -> &str {
        &self.export_name
    }

    pub(crate) fn route_id(&self) -> Option<&str> {
        self.route_id.as_deref()
    }

    pub(crate) fn udf_kind(&self) -> UdfKind {
        self.udf_kind
    }

    #[cfg(test)]
    pub(crate) fn visibility(&self) -> &str {
        &self.visibility
    }

    #[cfg(test)]
    pub(crate) fn capability_entry_for_test(
        entry_id: String,
        entry_selector_id: String,
        export_name: String,
        route_id: String,
        udf_kind: UdfKind,
        visibility: String,
    ) -> Self {
        Self {
            entry_id: Some(entry_id),
            entry_selector_id: Some(entry_selector_id),
            export_name,
            route_id: Some(route_id),
            udf_kind,
            visibility,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeRegistryArtifact {
    sha256: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeRegistryPackageFile {
    name: String,
    sha256: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeRegistryPackage {
    files: Vec<RuntimeRegistryPackageFile>,
    package_key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeRegistryCohortPackage {
    cohort_package_id: String,
    files: Vec<RuntimeRegistryPackageFile>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeRegistryDeploymentManifest {
    deployment_sha256: String,
    sha256: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum RuntimeRegistryGenerationManifest {
    #[serde(rename = "convex-wasm-runtime-registry-generation-v1")]
    V1 {
        #[serde(rename = "deploymentManifest")]
        deployment_manifest: RuntimeRegistryDeploymentManifest,
        #[serde(rename = "generationSha256")]
        generation_sha256: String,
        packages: Vec<RuntimeRegistryPackage>,
    },
    #[serde(rename = "convex-wasm-runtime-registry-generation-v2")]
    V2 {
        #[serde(rename = "cohortPackages")]
        cohort_packages: Vec<RuntimeRegistryCohortPackage>,
        #[serde(rename = "deploymentManifest")]
        deployment_manifest: RuntimeRegistryDeploymentManifest,
        #[serde(rename = "generationSha256")]
        generation_sha256: String,
    },
    #[serde(rename = "convex-wasm-runtime-registry-generation-v3")]
    V3 {
        #[serde(rename = "capabilityEntryPackages")]
        capability_entry_packages: Vec<RuntimeRegistryPackage>,
        #[serde(rename = "deploymentManifest")]
        deployment_manifest: RuntimeRegistryDeploymentManifest,
        #[serde(rename = "generationSha256")]
        generation_sha256: String,
    },
}

impl RuntimeRegistryGenerationManifest {
    fn deployment_manifest(&self) -> &RuntimeRegistryDeploymentManifest {
        match self {
            Self::V1 {
                deployment_manifest,
                ..
            }
            | Self::V2 {
                deployment_manifest,
                ..
            }
            | Self::V3 {
                deployment_manifest,
                ..
            } => deployment_manifest,
        }
    }

    fn generation_sha256(&self) -> &str {
        match self {
            Self::V1 {
                generation_sha256, ..
            }
            | Self::V2 {
                generation_sha256, ..
            }
            | Self::V3 {
                generation_sha256, ..
            } => generation_sha256,
        }
    }
}

enum ParsedRuntimeRegistryGeneration {
    Legacy(RuntimeRegistryGenerationManifest),
    V4(RuntimeRegistryGenerationV4),
    V5(RuntimeRegistryGenerationV5),
    V6ShadowOnly(RuntimeRegistryGenerationV6),
    V9(RuntimeRegistryGenerationV9),
    V10ShadowOnly(RuntimeRegistryGenerationV10),
}

impl ParsedRuntimeRegistryGeneration {
    fn deployment_manifest(&self) -> RuntimeRegistryDeploymentManifestRef<'_> {
        match self {
            Self::Legacy(generation) => {
                let deployment = generation.deployment_manifest();
                RuntimeRegistryDeploymentManifestRef {
                    deployment_sha256: &deployment.deployment_sha256,
                    sha256: &deployment.sha256,
                    size: deployment.size,
                }
            },
            Self::V4(generation) => {
                let deployment = generation.deployment_manifest();
                RuntimeRegistryDeploymentManifestRef {
                    deployment_sha256: deployment.deployment_sha256(),
                    sha256: deployment.sha256(),
                    size: deployment.size(),
                }
            },
            Self::V5(generation) => {
                let deployment = generation.deployment_manifest();
                RuntimeRegistryDeploymentManifestRef {
                    deployment_sha256: deployment.deployment_sha256(),
                    sha256: deployment.sha256(),
                    size: deployment.size(),
                }
            },
            Self::V6ShadowOnly(generation) => {
                let deployment = generation.deployment_manifest();
                RuntimeRegistryDeploymentManifestRef {
                    deployment_sha256: deployment.deployment_sha256(),
                    sha256: deployment.sha256(),
                    size: deployment.size(),
                }
            },
            Self::V9(generation) => {
                let deployment = generation.deployment_manifest();
                RuntimeRegistryDeploymentManifestRef {
                    deployment_sha256: deployment.deployment_sha256(),
                    sha256: deployment.sha256(),
                    size: deployment.size(),
                }
            },
            Self::V10ShadowOnly(generation) => {
                let deployment = generation.deployment_manifest();
                RuntimeRegistryDeploymentManifestRef {
                    deployment_sha256: deployment.deployment_sha256(),
                    sha256: deployment.sha256(),
                    size: deployment.size(),
                }
            },
        }
    }

    fn generation_sha256(&self) -> &str {
        match self {
            Self::Legacy(generation) => generation.generation_sha256(),
            Self::V4(generation) => generation.generation_sha256(),
            Self::V5(generation) => generation.generation_sha256(),
            Self::V6ShadowOnly(generation) => generation.generation_sha256(),
            Self::V9(generation) => generation.generation_sha256(),
            Self::V10ShadowOnly(generation) => generation.generation_sha256(),
        }
    }

    fn admission(&self) -> RuntimeRegistryAdmission {
        match self {
            Self::V6ShadowOnly(generation) => generation.admission(),
            Self::V10ShadowOnly(generation) => generation.admission(),
            Self::Legacy(_) | Self::V4(_) | Self::V5(_) => {
                RuntimeRegistryAdmission::PrimaryAdmitted
            },
            Self::V9(_) => RuntimeRegistryAdmission::PrimaryAdmitted,
        }
    }
}

struct RuntimeRegistryDeploymentManifestRef<'a> {
    deployment_sha256: &'a str,
    sha256: &'a str,
    size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeRegistryCurrentManifest {
    current_sha256: String,
    deployment_sha256: String,
    generation: RuntimeRegistryArtifact,
    generation_sha256: String,
    kind: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeRegistrySourceCatalogEntry {
    deployment_sha256: String,
    generation: RuntimeRegistryArtifact,
    generation_sha256: String,
    source_package_runtime_content_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeRegistrySourceCatalogManifest {
    catalog_sha256: String,
    entries: Vec<RuntimeRegistrySourceCatalogEntry>,
    kind: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ValidatedRuntimeRegistryGenerationDescriptor {
    current_sha256: String,
    deployment_sha256: String,
    generation_file_sha256: String,
    generation_file_size: u64,
    generation_sha256: String,
    source_package_runtime_content_sha256: Option<String>,
}

impl PartialEq for ValidatedRuntimeRegistryGenerationDescriptor {
    fn eq(&self, other: &Self) -> bool {
        self.same_authenticated_identity(other)
    }
}

impl Eq for ValidatedRuntimeRegistryGenerationDescriptor {}

impl ValidatedRuntimeRegistryGenerationDescriptor {
    pub(crate) fn current_sha256(&self) -> &str {
        &self.current_sha256
    }

    pub(crate) fn deployment_sha256(&self) -> &str {
        &self.deployment_sha256
    }

    pub(crate) fn generation_manifest_sha256(&self) -> &str {
        &self.generation_file_sha256
    }

    pub(crate) fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }

    pub(crate) fn source_package_runtime_content_sha256(&self) -> Option<&str> {
        self.source_package_runtime_content_sha256.as_deref()
    }

    pub(crate) fn same_authenticated_identity(&self, other: &Self) -> bool {
        self.deployment_sha256 == other.deployment_sha256
            && self.generation_file_sha256 == other.generation_file_sha256
            && self.generation_file_size == other.generation_file_size
            && self.generation_sha256 == other.generation_sha256
            && self.source_package_runtime_content_sha256
                == other.source_package_runtime_content_sha256
    }
}

pub(crate) struct ValidatedRuntimeRegistrySourceCatalog {
    catalog_sha256: String,
    generations: Vec<ValidatedRuntimeRegistryGenerationDescriptor>,
}

impl ValidatedRuntimeRegistrySourceCatalog {
    pub(crate) fn catalog_sha256(&self) -> &str {
        &self.catalog_sha256
    }

    pub(crate) fn into_generations(self) -> Vec<ValidatedRuntimeRegistryGenerationDescriptor> {
        self.generations
    }
}

pub(crate) struct ValidatedRuntimeRegistryGeneration {
    descriptor: ValidatedRuntimeRegistryGenerationDescriptor,
    admission: RuntimeRegistryAdmission,
    current_sha256: String,
    generation_manifest_sha256: String,
    generation_sha256: String,
    packages_root: PathBuf,
    registry: ValidatedDeploymentManifest,
    module_graph_catalog: Option<AuthenticatedModuleGraphCatalog>,
}

pub(crate) struct SelectedDeploymentWasmExport {
    pub(crate) runtime_module_path: String,
    pub(crate) export_name: String,
    pub(crate) udf_kind: UdfKind,
}

impl ValidatedRuntimeRegistryGeneration {
    pub(crate) fn current_sha256(&self) -> &str {
        &self.current_sha256
    }

    pub(crate) fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }

    pub(crate) fn generation_manifest_sha256(&self) -> &str {
        &self.generation_manifest_sha256
    }

    pub(crate) fn packages_root(&self) -> &Path {
        &self.packages_root
    }

    pub(crate) fn registry(&self) -> &ValidatedDeploymentManifest {
        &self.registry
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        String,
        String,
        PathBuf,
        ValidatedDeploymentManifest,
        Option<AuthenticatedModuleGraphCatalog>,
        RuntimeRegistryAdmission,
        ValidatedRuntimeRegistryGenerationDescriptor,
    ) {
        (
            self.current_sha256,
            self.generation_manifest_sha256,
            self.generation_sha256,
            self.packages_root,
            self.registry,
            self.module_graph_catalog,
            self.admission,
            self.descriptor,
        )
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DeploymentExportKey {
    runtime_module_path: String,
    export_name: String,
}

impl DeploymentExportKey {
    fn new(runtime_module_path: &str, export_name: &str) -> Self {
        Self {
            runtime_module_path: runtime_module_path.to_owned(),
            export_name: export_name.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeployedRuntimeIdentity {
    module_sha256: String,
    source_package: DeployedSourcePackageIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeployedSourcePackageIdentity {
    ArchiveSha256(String),
    RuntimeContentSha256(String),
}

impl DeployedRuntimeIdentity {
    pub(crate) fn module_sha256(&self) -> &str {
        &self.module_sha256
    }

    pub(crate) fn source_package_archive_sha256(&self) -> Option<&str> {
        match &self.source_package {
            DeployedSourcePackageIdentity::ArchiveSha256(sha256) => Some(sha256),
            DeployedSourcePackageIdentity::RuntimeContentSha256(_) => None,
        }
    }

    pub(crate) fn source_package_runtime_content_sha256(&self) -> Option<&str> {
        match &self.source_package {
            DeployedSourcePackageIdentity::ArchiveSha256(_) => None,
            DeployedSourcePackageIdentity::RuntimeContentSha256(sha256) => Some(sha256),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(module_sha256: String, source_package_sha256: String) -> Self {
        Self {
            module_sha256,
            source_package: DeployedSourcePackageIdentity::ArchiveSha256(source_package_sha256),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_runtime_content_test(
        module_sha256: String,
        source_package_runtime_content_sha256: String,
    ) -> Self {
        Self {
            module_sha256,
            source_package: DeployedSourcePackageIdentity::RuntimeContentSha256(
                source_package_runtime_content_sha256,
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeployedRuntimeIdentityVerdict {
    Unprovable,
    Mismatch,
}

impl fmt::Display for DeployedRuntimeIdentityVerdict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unprovable => "unprovable",
            Self::Mismatch => "mismatch",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeployedRuntimeIdentityReason {
    code: String,
    detail: String,
}

impl DeployedRuntimeIdentityReason {
    pub(crate) fn code(&self) -> &str {
        &self.code
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeployedRuntimeIdentityDiagnostic {
    verdict: DeployedRuntimeIdentityVerdict,
    reasons: Vec<DeployedRuntimeIdentityReason>,
}

impl DeployedRuntimeIdentityDiagnostic {
    pub(crate) fn verdict(&self) -> DeployedRuntimeIdentityVerdict {
        self.verdict
    }

    pub(crate) fn reasons(&self) -> &[DeployedRuntimeIdentityReason] {
        &self.reasons
    }
}

impl fmt::Display for DeployedRuntimeIdentityDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.verdict)?;
        for (index, reason) in self.reasons.iter().enumerate() {
            if index == 0 {
                formatter.write_str(" (")?;
            } else {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{}: {}", reason.code, reason.detail)?;
        }
        formatter.write_str(")")
    }
}

enum DeploymentRuntimeBinding {
    Bound(DeployedRuntimeIdentity),
    Diagnostic(DeployedRuntimeIdentityDiagnostic),
}

impl DeploymentRuntimeBinding {
    fn identity(&self) -> Result<&DeployedRuntimeIdentity, WasmUdfPackageError> {
        match self {
            Self::Bound(identity) => Ok(identity),
            Self::Diagnostic(diagnostic) => Err(
                WasmUdfPackageError::UnboundDeployedRuntimeIdentity(diagnostic.clone()),
            ),
        }
    }
}

struct DeploymentExportSource {
    export_name: String,
    export_sha256: String,
    module_path: String,
    resolved_graph_sha256: String,
    udf_kind: UdfKind,
    visibility: String,
    deployed_runtime_binding: DeploymentRuntimeBinding,
}

enum ValidatedDeploymentExport {
    Wasm {
        package_reference: DeploymentWasmPackageReference,
        artifact: DeploymentWasmArtifact,
        source: DeploymentExportSource,
    },
    ExistingRuntime {
        udf_kind: UdfKind,
    },
    V8Fallback {
        udf_kind: UdfKind,
    },
}

enum DeploymentWasmArtifact {
    LegacyExecutionManifest(JsonValue),
    CapabilityEntryManifest {
        manifest_sha256: String,
        compiler_limits: JsonValue,
    },
    ModuleGraphCohortContract {
        cohort_contract_sha256: String,
    },
}

enum DeploymentWasmPackageReference {
    Singleton {
        package_key: String,
    },
    Cohort {
        cohort_package_id: String,
        entry_id: String,
        entry_selector_id: String,
    },
    CapabilityEntry {
        capability_entry_package_id: String,
        entry_id: String,
        route_id: String,
        entry_selector_id: String,
    },
    ModuleGraphCohort {
        cohort_contract_sha256: String,
        entry_id: String,
        route_id: String,
        entry_selector_id: String,
    },
}

impl DeploymentWasmPackageReference {
    fn package_key(&self) -> &str {
        match self {
            Self::Singleton { package_key } => package_key,
            Self::Cohort {
                cohort_package_id, ..
            } => cohort_package_id,
            Self::CapabilityEntry {
                capability_entry_package_id,
                ..
            } => capability_entry_package_id,
            Self::ModuleGraphCohort {
                cohort_contract_sha256,
                ..
            } => cohort_contract_sha256,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum DeploymentManifestVersion {
    V2,
    V3,
    V4,
    V5,
    V6,
    V8,
}

enum DeploymentCompileSelection {
    HistoricalExplicit(BTreeSet<(String, String)>),
    CapabilityEntryExplicit(BTreeSet<(String, String)>),
    AllEligible,
}

pub(crate) struct ValidatedDeploymentManifest {
    artifact_precompiler: PrecompilerMaterialIdentity,
    deployment_sha256: String,
    exports: BTreeMap<DeploymentExportKey, ValidatedDeploymentExport>,
    module_graph_binding: Option<DeploymentModuleGraphBinding>,
    module_graph_cohorts: Vec<ModuleGraphCohortContract>,
    #[cfg(test)]
    existing_runtime_actions: BTreeSet<DeploymentExportKey>,
    version: DeploymentManifestVersion,
}

impl ValidatedDeploymentManifest {
    pub(crate) fn load(manifest_path: &Path) -> Result<Self, WasmUdfPackageError> {
        let manifest_bytes = read_bounded_file(manifest_path, MAX_DEPLOYMENT_MANIFEST_BYTES)?;
        Self::parse(&manifest_bytes)
    }

    #[cfg(test)]
    pub(crate) fn load_for_development_test(
        manifest_path: &Path,
    ) -> Result<Self, WasmUdfPackageError> {
        let manifest_bytes = read_bounded_file(manifest_path, MAX_DEPLOYMENT_MANIFEST_BYTES)?;
        Self::parse_with_development_selection(&manifest_bytes, true)
    }

    fn parse(manifest_bytes: &[u8]) -> Result<Self, WasmUdfPackageError> {
        Self::parse_with_development_selection(manifest_bytes, false)
    }

    fn parse_with_development_selection(
        manifest_bytes: &[u8],
        allow_development_selection: bool,
    ) -> Result<Self, WasmUdfPackageError> {
        let mut manifest: JsonValue = serde_json::from_slice(&manifest_bytes)
            .map_err(WasmUdfPackageError::MalformedDeploymentManifest)?;
        let manifest_object = manifest
            .as_object_mut()
            .ok_or_else(|| invalid_deployment_manifest("top level must be an object"))?;
        let version = match require_string_field(manifest_object, "kind", "top level")? {
            DEPLOYMENT_MANIFEST_KIND_V2 => DeploymentManifestVersion::V2,
            DEPLOYMENT_MANIFEST_KIND_V3 => DeploymentManifestVersion::V3,
            DEPLOYMENT_MANIFEST_KIND_V4 => DeploymentManifestVersion::V4,
            DEPLOYMENT_MANIFEST_KIND_V5 => DeploymentManifestVersion::V5,
            DEPLOYMENT_MANIFEST_KIND_V6 => DeploymentManifestVersion::V6,
            DEPLOYMENT_MANIFEST_KIND_V8 => DeploymentManifestVersion::V8,
            _ => {
                return Err(invalid_deployment_manifest(
                    "unsupported deployment manifest kind",
                ));
            },
        };
        let mut expected_top_level_keys = vec![
            "artifactPrecompiler",
            "compileSelection",
            "compiler",
            "counts",
            "deploymentSha256",
            "diagnosticCensus",
            "existingRuntimeActions",
            "exports",
            "graph",
            "inventoryAuthority",
            "kind",
            "mode",
            "policy",
            "sourceInventory",
        ];
        if manifest_object.contains_key("effectExecutionMode") {
            expected_top_level_keys.push("effectExecutionMode");
        }
        if matches!(
            version,
            DeploymentManifestVersion::V5
                | DeploymentManifestVersion::V6
                | DeploymentManifestVersion::V8
        ) {
            expected_top_level_keys.push("contextReusePolicyIdentity");
        }
        if matches!(
            version,
            DeploymentManifestVersion::V6 | DeploymentManifestVersion::V8
        ) {
            expected_top_level_keys.push("moduleGraphBinding");
            expected_top_level_keys.push("moduleGraphCohorts");
        }
        if version == DeploymentManifestVersion::V8 {
            expected_top_level_keys.push("contextReuseAnalysis");
        }
        require_exact_keys(
            manifest_object.keys().map(String::as_str),
            &expected_top_level_keys,
            "top level",
        )?;
        if matches!(
            version,
            DeploymentManifestVersion::V5
                | DeploymentManifestVersion::V6
                | DeploymentManifestVersion::V8
        ) {
            let context_reuse_policy_identity = manifest_object
                .get("contextReusePolicyIdentity")
                .and_then(JsonValue::as_object)
                .ok_or_else(|| {
                    invalid_deployment_manifest(
                        "contextReusePolicyIdentity must be an object for deployment manifest v5",
                    )
                })?;
            require_exact_keys(
                context_reuse_policy_identity.keys().map(String::as_str),
                &["kind", "sha256"],
                "contextReusePolicyIdentity",
            )?;
            if require_string_field(
                context_reuse_policy_identity,
                "kind",
                "contextReusePolicyIdentity",
            )? != CONTEXT_REUSE_POLICY_IDENTITY_KIND
            {
                return Err(invalid_deployment_manifest(
                    "contextReusePolicyIdentity kind is invalid",
                ));
            }
            validate_deployment_sha256(
                "contextReusePolicyIdentity sha256",
                require_string_field(
                    context_reuse_policy_identity,
                    "sha256",
                    "contextReusePolicyIdentity",
                )?,
            )?;
        }
        let effect_execution_mode = manifest_object
            .get("effectExecutionMode")
            .map(|value| {
                serde_json::from_value::<EffectExecutionMode>(value.clone())
                    .map_err(|_| invalid_deployment_manifest("effectExecutionMode is invalid"))
            })
            .transpose()?;
        if effect_execution_mode.is_some()
            && !matches!(
                version,
                DeploymentManifestVersion::V4
                    | DeploymentManifestVersion::V5
                    | DeploymentManifestVersion::V6
                    | DeploymentManifestVersion::V8
            )
        {
            return Err(invalid_deployment_manifest(
                "effectExecutionMode requires deployment manifest v4",
            ));
        }
        require_string_field(manifest_object, "mode", "top level").and_then(|mode| {
            if mode == "compile" {
                Ok(())
            } else {
                Err(invalid_deployment_manifest(
                    "deployment manifest is not compile mode",
                ))
            }
        })?;
        let deployment_sha256 =
            require_string_field(manifest_object, "deploymentSha256", "top level")?.to_owned();
        validate_deployment_sha256("deploymentSha256", &deployment_sha256)?;
        manifest_object.remove("deploymentSha256");
        let mut canonical = Vec::new();
        write_canonical_json(&manifest, &mut canonical)
            .expect("writing canonical deployment JSON to a byte vector cannot fail");
        if sha256(&canonical) != deployment_sha256 {
            return Err(invalid_deployment_manifest(
                "deploymentSha256 does not authenticate the manifest",
            ));
        }

        let manifest_object = manifest
            .as_object()
            .expect("validated deployment manifest object changed shape");
        let artifact_precompiler = validate_deployment_artifact_precompiler(
            manifest_object
                .get("artifactPrecompiler")
                .ok_or_else(|| invalid_deployment_manifest("artifactPrecompiler is missing"))?,
        )?;
        let context_reuse_analysis = if version == DeploymentManifestVersion::V8 {
            let identity = serde_json::from_value::<ContextReuseAnalysisIdentity>(
                manifest_object
                    .get("contextReuseAnalysis")
                    .expect("deployment-v8 exact fields require contextReuseAnalysis")
                    .clone(),
            )
            .map_err(|error| {
                invalid_deployment_manifest(format!(
                    "contextReuseAnalysis has missing, unknown, or invalid fields: {error}"
                ))
            })?;
            identity.validate()?;
            Some(identity)
        } else {
            None
        };
        let module_graph_binding = if matches!(
            version,
            DeploymentManifestVersion::V6 | DeploymentManifestVersion::V8
        ) {
            Some(
                DeploymentModuleGraphBinding::parse(
                    manifest_object
                        .get("moduleGraphBinding")
                        .expect("module-graph deployment exact fields require moduleGraphBinding")
                        .clone(),
                )
                .map_err(|error| invalid_deployment_manifest(error.to_string()))?,
            )
        } else {
            None
        };
        let module_graph_cohorts = if matches!(
            version,
            DeploymentManifestVersion::V6 | DeploymentManifestVersion::V8
        ) {
            let contracts = serde_json::from_value::<Vec<ModuleGraphCohortContract>>(
                manifest_object
                    .get("moduleGraphCohorts")
                    .expect("module-graph deployment exact fields require moduleGraphCohorts")
                    .clone(),
            )
            .map_err(|error| {
                invalid_deployment_manifest(format!(
                    "moduleGraphCohorts have missing, unknown, or invalid fields: {error}"
                ))
            })?;
            validate_module_graph_cohort_contracts(&contracts)?;
            contracts
        } else {
            Vec::new()
        };
        if version == DeploymentManifestVersion::V6
            && (module_graph_binding
                .as_ref()
                .and_then(DeploymentModuleGraphBinding::context_reuse_analysis)
                .is_some()
                || module_graph_cohorts
                    .iter()
                    .any(|contract| contract.context_reuse_analysis().is_some()))
        {
            return Err(invalid_deployment_manifest(
                "deployment-v6 contains deployment-v8 context-reuse graph authority",
            ));
        }
        if version == DeploymentManifestVersion::V8 {
            let context_reuse_analysis = context_reuse_analysis
                .as_ref()
                .expect("deployment-v8 exact fields require contextReuseAnalysis");
            let cohort_analyses = module_graph_cohorts
                .iter()
                .map(ModuleGraphCohortContract::context_reuse_analysis)
                .collect::<Option<Vec<_>>>();
            if module_graph_binding
                .as_ref()
                .and_then(DeploymentModuleGraphBinding::context_reuse_analysis)
                != Some(context_reuse_analysis)
                || cohort_analyses.is_none()
            {
                return Err(invalid_deployment_manifest(
                    "deployment-v8 context-reuse analysis differs from its graph authority",
                ));
            }
            validate_context_reuse_cohort_analysis_set(
                context_reuse_analysis,
                &cohort_analyses.expect("deployment-v8 cohort analyses were checked above"),
            )?;
        }
        let counts = manifest_object
            .get("counts")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| invalid_deployment_manifest("counts must be an object"))?;
        let mut expected_count_keys = vec![
            "actionsOnExistingRuntime",
            "eligible",
            "ineligible",
            "mutations",
            "queries",
            "selectedWasm",
            "total",
            "unselectedEligible",
        ];
        if matches!(
            version,
            DeploymentManifestVersion::V4
                | DeploymentManifestVersion::V5
                | DeploymentManifestVersion::V6
                | DeploymentManifestVersion::V8
        ) {
            expected_count_keys.push("artifactFallback");
        }
        require_exact_keys(
            counts.keys().map(String::as_str),
            &expected_count_keys,
            "counts",
        )?;
        let compile_selection = manifest_object
            .get("compileSelection")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| invalid_deployment_manifest("compileSelection must be an object"))?;
        let compile_selection_kind =
            require_string_field(compile_selection, "kind", "compileSelection")?;
        let compile_selection = match compile_selection_kind {
            COMPILE_SELECTION_KIND | DEVELOPMENT_COMPILE_SELECTION_KIND => {
                if compile_selection_kind == DEVELOPMENT_COMPILE_SELECTION_KIND {
                    if !matches!(
                        version,
                        DeploymentManifestVersion::V3
                            | DeploymentManifestVersion::V4
                            | DeploymentManifestVersion::V5
                            | DeploymentManifestVersion::V6
                            | DeploymentManifestVersion::V8
                    ) {
                        return Err(invalid_deployment_manifest(
                            "development-only compileSelection requires deployment manifest v3, \
                             v4, or v5",
                        ));
                    }
                    if !allow_development_selection {
                        return Err(invalid_deployment_manifest(
                            "development-only compileSelection is not accepted by the runtime \
                             registry",
                        ));
                    }
                    require_exact_keys(
                        compile_selection.keys().map(String::as_str),
                        &["developmentOnly", "exports", "kind"],
                        "compileSelection",
                    )?;
                    if compile_selection.get("developmentOnly") != Some(&JsonValue::Bool(true)) {
                        return Err(invalid_deployment_manifest(
                            "compileSelection developmentOnly must be true",
                        ));
                    }
                } else {
                    if version != DeploymentManifestVersion::V2 {
                        return Err(invalid_deployment_manifest(
                            "historical explicit compileSelection requires deployment manifest v2",
                        ));
                    }
                    require_exact_keys(
                        compile_selection.keys().map(String::as_str),
                        &["exports", "kind"],
                        "compileSelection",
                    )?;
                }
                let selected_exports = compile_selection
                    .get("exports")
                    .and_then(JsonValue::as_array)
                    .ok_or_else(|| {
                        invalid_deployment_manifest("compileSelection exports must be an array")
                    })?;
                let mut selected_identities = BTreeSet::new();
                for selected_export in selected_exports {
                    let selected_export = selected_export.as_object().ok_or_else(|| {
                        invalid_deployment_manifest(
                            "compileSelection exports entries must be objects",
                        )
                    })?;
                    require_exact_keys(
                        selected_export.keys().map(String::as_str),
                        &["exportName", "modulePath"],
                        "compileSelection exports entry",
                    )?;
                    let module_path = require_canonical_module_path(
                        selected_export,
                        "modulePath",
                        "compileSelection exports entry",
                    )?;
                    let export_name = require_identity_field(
                        selected_export,
                        "exportName",
                        "compileSelection exports entry",
                    )?;
                    if !selected_identities.insert((module_path.to_owned(), export_name.to_owned()))
                    {
                        return Err(invalid_deployment_manifest(
                            "compileSelection contains a duplicate function identity",
                        ));
                    }
                }
                DeploymentCompileSelection::HistoricalExplicit(selected_identities)
            },
            CAPABILITY_ENTRY_COMPILE_SELECTION_KIND => {
                if !matches!(
                    version,
                    DeploymentManifestVersion::V5
                        | DeploymentManifestVersion::V6
                        | DeploymentManifestVersion::V8
                ) {
                    return Err(invalid_deployment_manifest(
                        "capability-entry compileSelection requires deployment manifest v5",
                    ));
                }
                require_exact_keys(
                    compile_selection.keys().map(String::as_str),
                    &["exports", "kind"],
                    "compileSelection",
                )?;
                let selected_exports = compile_selection
                    .get("exports")
                    .and_then(JsonValue::as_array)
                    .ok_or_else(|| {
                        invalid_deployment_manifest("compileSelection exports must be an array")
                    })?;
                let mut selected_identities = BTreeSet::new();
                for selected_export in selected_exports {
                    let selected_export = selected_export.as_object().ok_or_else(|| {
                        invalid_deployment_manifest(
                            "compileSelection exports entries must be objects",
                        )
                    })?;
                    require_exact_keys(
                        selected_export.keys().map(String::as_str),
                        &["entryPath", "exportName"],
                        "compileSelection exports entry",
                    )?;
                    let entry_path = require_identity_field(
                        selected_export,
                        "entryPath",
                        "compileSelection exports entry",
                    )?;
                    let export_name = require_identity_field(
                        selected_export,
                        "exportName",
                        "compileSelection exports entry",
                    )?;
                    if !selected_identities.insert((entry_path.to_owned(), export_name.to_owned()))
                    {
                        return Err(invalid_deployment_manifest(
                            "compileSelection contains a duplicate capability-entry route identity",
                        ));
                    }
                }
                DeploymentCompileSelection::CapabilityEntryExplicit(selected_identities)
            },
            ALL_ELIGIBLE_COMPILE_SELECTION_KIND => {
                require_exact_keys(
                    compile_selection.keys().map(String::as_str),
                    &["kind"],
                    "compileSelection",
                )?;
                DeploymentCompileSelection::AllEligible
            },
            _ => {
                return Err(invalid_deployment_manifest(
                    "compileSelection kind is invalid",
                ));
            },
        };
        let diagnostic_census = manifest_object
            .get("diagnosticCensus")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| invalid_deployment_manifest("diagnosticCensus must be an array"))?;
        let mut diagnostic_census_ids = BTreeSet::new();
        for diagnostic in diagnostic_census {
            let diagnostic = diagnostic.as_object().ok_or_else(|| {
                invalid_deployment_manifest("diagnosticCensus entries must be objects")
            })?;
            require_exact_keys(
                diagnostic.keys().map(String::as_str),
                &[
                    "code",
                    "column",
                    "construct",
                    "exportCount",
                    "file",
                    "id",
                    "line",
                    "message",
                    "occurrenceCount",
                    "source",
                ],
                "diagnosticCensus entry",
            )?;
            for field in ["code", "construct", "file", "message", "source"] {
                require_string_field(diagnostic, field, "diagnosticCensus entry")?;
            }
            for field in ["column", "exportCount", "line", "occurrenceCount"] {
                require_usize_field(diagnostic, field, "diagnosticCensus entry")?;
            }
            let id = require_identity_field(diagnostic, "id", "diagnosticCensus entry")?;
            if !diagnostic_census_ids.insert(id) {
                return Err(invalid_deployment_manifest(
                    "diagnosticCensus contains a duplicate identity",
                ));
            }
        }
        let exports = manifest_object
            .get("exports")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| invalid_deployment_manifest("exports must be an array"))?;
        let actions = manifest_object
            .get("existingRuntimeActions")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| {
                invalid_deployment_manifest("existingRuntimeActions must be an array")
            })?;
        let mut action_identities = BTreeSet::new();
        for action in actions {
            let action = action.as_object().ok_or_else(|| {
                invalid_deployment_manifest("existingRuntimeActions entries must be objects")
            })?;
            require_exact_keys(
                action.keys().map(String::as_str),
                &[
                    "entryPath",
                    "exportName",
                    "routing",
                    "runtimeModulePath",
                    "udfKind",
                    "visibility",
                ],
                "existingRuntimeActions entry",
            )?;
            let _entry_path =
                require_identity_field(action, "entryPath", "existingRuntimeActions entry")?;
            let runtime_module_path = require_runtime_module_path(
                action,
                "runtimeModulePath",
                "existingRuntimeActions entry",
            )?;
            let export_name =
                require_identity_field(action, "exportName", "existingRuntimeActions entry")?;
            if !action_identities.insert(DeploymentExportKey::new(runtime_module_path, export_name))
            {
                return Err(invalid_deployment_manifest(
                    "existingRuntimeActions contains a duplicate function identity",
                ));
            }
            if require_string_field(action, "udfKind", "existingRuntimeActions entry")? != "action"
            {
                return Err(invalid_deployment_manifest(
                    "existing-runtime action UDF kind is invalid",
                ));
            }
            validate_visibility(require_string_field(
                action,
                "visibility",
                "existingRuntimeActions entry",
            )?)?;
            let routing = action
                .get("routing")
                .and_then(JsonValue::as_object)
                .ok_or_else(|| {
                    invalid_deployment_manifest("existing-runtime action routing must be an object")
                })?;
            require_exact_keys(
                routing.keys().map(String::as_str),
                &["decision", "reason"],
                "existing-runtime action routing",
            )?;
            if require_string_field(routing, "decision", "existing-runtime action routing")?
                != "existingRuntime"
                || require_string_field(routing, "reason", "existing-runtime action routing")?
                    != "action-runtime"
            {
                return Err(invalid_deployment_manifest(
                    "existing-runtime action routing is invalid",
                ));
            }
        }
        let mut validated_exports = BTreeMap::new();
        let mut export_identities = BTreeSet::new();
        let mut eligible_count = 0;
        let mut ineligible_count = 0;
        let mut artifact_fallback_count = 0;
        let mut mutation_count = 0;
        let mut query_count = 0;
        let mut selected_wasm_count = 0;
        let mut unselected_eligible_count = 0;
        let mut artifact_fallback_identities = BTreeSet::new();
        let mut capability_entry_wasm_identities = BTreeSet::new();
        let module_graph_cohorts_by_contract = module_graph_cohorts
            .iter()
            .map(|contract| (contract.cohort_contract_sha256.as_str(), contract))
            .collect::<BTreeMap<_, _>>();
        for export in exports {
            let export = export
                .as_object()
                .ok_or_else(|| invalid_deployment_manifest("exports entries must be objects"))?;
            let mut expected_export_keys = vec![
                "artifact",
                "compiler",
                "dependencies",
                "diagnostics",
                "entryPath",
                "exportName",
                "packageReference",
                "routing",
                "runtimeModulePath",
                "source",
                "sourceCrossCheck",
                "udfKind",
                "visibility",
            ];
            if export.contains_key("diagnosticCensusIds") {
                expected_export_keys.push("diagnosticCensusIds");
            }
            if export.contains_key("dependencyGraphSha256") {
                expected_export_keys.push("dependencyGraphSha256");
            }
            if export.contains_key("compilerLimits") {
                expected_export_keys.push("compilerLimits");
            }
            if export.contains_key("operationSummary") {
                expected_export_keys.push("operationSummary");
            }
            if matches!(
                version,
                DeploymentManifestVersion::V3
                    | DeploymentManifestVersion::V4
                    | DeploymentManifestVersion::V5
                    | DeploymentManifestVersion::V6
                    | DeploymentManifestVersion::V8
            ) && export.contains_key("appliedDependencyAdapters")
            {
                expected_export_keys.push("appliedDependencyAdapters");
            }
            if matches!(
                version,
                DeploymentManifestVersion::V3
                    | DeploymentManifestVersion::V4
                    | DeploymentManifestVersion::V5
                    | DeploymentManifestVersion::V6
                    | DeploymentManifestVersion::V8
            ) && export.contains_key("appliedRegistrationAdapter")
            {
                expected_export_keys.push("appliedRegistrationAdapter");
            }
            require_exact_keys(
                export.keys().map(String::as_str),
                &expected_export_keys,
                "exports entry",
            )?;
            if export
                .get("appliedDependencyAdapters")
                .is_some_and(|value| !value.is_array())
            {
                return Err(invalid_deployment_manifest(
                    "export appliedDependencyAdapters must be an array",
                ));
            }
            if export
                .get("appliedRegistrationAdapter")
                .is_some_and(|value| !value.is_object())
            {
                return Err(invalid_deployment_manifest(
                    "export appliedRegistrationAdapter must be an object",
                ));
            }
            let entry_path = require_identity_field(export, "entryPath", "exports entry")?;
            let runtime_module_path =
                require_runtime_module_path(export, "runtimeModulePath", "exports entry")?;
            let row_export_name = require_identity_field(export, "exportName", "exports entry")?;
            let row_udf_kind =
                parse_udf_kind(require_string_field(export, "udfKind", "exports entry")?)?;
            match row_udf_kind {
                UdfKind::Query => query_count += 1,
                UdfKind::Mutation => mutation_count += 1,
            }
            let row_visibility = require_string_field(export, "visibility", "exports entry")?;
            validate_visibility(row_visibility)?;
            let public_identity = DeploymentExportKey::new(runtime_module_path, row_export_name);
            if action_identities.contains(&public_identity) {
                return Err(invalid_deployment_manifest(
                    "function identity appears as both an export and an existing-runtime action",
                ));
            }
            if !export_identities.insert(public_identity) {
                return Err(invalid_deployment_manifest(
                    "exports contains a duplicate function identity",
                ));
            }
            let dependency_graph_sha256 = export
                .contains_key("dependencyGraphSha256")
                .then(|| require_string_field(export, "dependencyGraphSha256", "exports entry"))
                .transpose()?;
            if let Some(dependency_graph_sha256) = dependency_graph_sha256 {
                validate_deployment_sha256("dependencyGraphSha256", dependency_graph_sha256)?;
            }
            let export_diagnostic_ids = export
                .contains_key("diagnosticCensusIds")
                .then(|| require_string_array_field(export, "diagnosticCensusIds", "exports entry"))
                .transpose()?;
            if export_diagnostic_ids
                .as_ref()
                .is_some_and(|ids| !ids.is_subset(&diagnostic_census_ids))
            {
                return Err(invalid_deployment_manifest(
                    "exports entry references an unknown diagnostic census identity",
                ));
            }
            if let Some(operation_summary) = export.get("operationSummary") {
                let operation_summary = operation_summary.as_object().ok_or_else(|| {
                    invalid_deployment_manifest("export operationSummary must be an object")
                })?;
                require_exact_keys(
                    operation_summary.keys().map(String::as_str),
                    &[
                        "argumentFieldCount",
                        "documentPropertyCount",
                        "intrinsicCount",
                        "operationCount",
                        "operationsByKind",
                    ],
                    "export operationSummary",
                )?;
                for field in [
                    "argumentFieldCount",
                    "documentPropertyCount",
                    "intrinsicCount",
                    "operationCount",
                ] {
                    require_usize_field(operation_summary, field, "export operationSummary")?;
                }
                let operations_by_kind = operation_summary
                    .get("operationsByKind")
                    .and_then(JsonValue::as_object)
                    .ok_or_else(|| {
                        invalid_deployment_manifest(
                            "export operationSummary operationsByKind must be an object",
                        )
                    })?;
                let mut described_operation_count = 0usize;
                for (kind, value) in operations_by_kind {
                    if !matches!(
                        kind.as_str(),
                        "authenticationGetUserIdentity"
                            | "databaseGet"
                            | "databaseNormalizeId"
                            | "functionHandleCreate"
                            | "databaseInsert"
                            | "databasePatch"
                            | "databaseReplace"
                            | "databaseDelete"
                            | "databaseIndexQuery"
                            | "hostSecretVerify"
                            | "schedulerRunAfter"
                            | "schedulerRunAt"
                            | "sha256"
                    ) {
                        return Err(invalid_deployment_manifest(
                            "export operationSummary contains an unknown operation kind",
                        ));
                    }
                    described_operation_count = described_operation_count
                        .checked_add(
                            value
                                .as_u64()
                                .and_then(|value| usize::try_from(value).ok())
                                .ok_or_else(|| {
                                    invalid_deployment_manifest(
                                        "export operationSummary operation count must be a \
                                         nonnegative integer",
                                    )
                                })?,
                        )
                        .ok_or_else(|| {
                            invalid_deployment_manifest(
                                "export operationSummary operation count overflowed",
                            )
                        })?;
                }
                if described_operation_count
                    != require_usize_field(
                        operation_summary,
                        "operationCount",
                        "export operationSummary",
                    )?
                {
                    return Err(invalid_deployment_manifest(
                        "export operationSummary operationCount does not match operationsByKind",
                    ));
                }
            }
            let compiler_is_object = export.get("compiler").is_some_and(JsonValue::is_object);
            let compiler_is_null = export.get("compiler").is_some_and(JsonValue::is_null);
            if !compiler_is_object && !compiler_is_null {
                return Err(invalid_deployment_manifest(
                    "export compiler identity must be an object or null",
                ));
            }
            let routing = export
                .get("routing")
                .and_then(JsonValue::as_object)
                .ok_or_else(|| invalid_deployment_manifest("export routing must be an object"))?;
            require_exact_keys(
                routing.keys().map(String::as_str),
                &["decision", "reason"],
                "export routing",
            )?;
            let decision = require_string_field(routing, "decision", "export routing")?;
            let reason = require_string_field(routing, "reason", "export routing")?;
            let v5_not_analyzed_route = matches!(
                version,
                DeploymentManifestVersion::V5
                    | DeploymentManifestVersion::V6
                    | DeploymentManifestVersion::V8
            ) && decision == "existingRuntime"
                && reason == "not-analyzed";
            if v5_not_analyzed_route != dependency_graph_sha256.is_none() {
                return Err(invalid_deployment_manifest(
                    "only v5 not-analyzed exports may omit dependencyGraphSha256",
                ));
            }
            let v5_source_not_analyzed = matches!(
                version,
                DeploymentManifestVersion::V5
                    | DeploymentManifestVersion::V6
                    | DeploymentManifestVersion::V8
            ) && decision == "existingRuntime"
                && matches!(reason, "not-analyzed" | "not-selected")
                && export
                    .get("compiler")
                    .and_then(JsonValue::as_object)
                    .and_then(|compiler| compiler.get("kind"))
                    .and_then(JsonValue::as_str)
                    == Some("not-analyzed");
            if v5_not_analyzed_route && !v5_source_not_analyzed {
                return Err(invalid_deployment_manifest(
                    "v5 not-analyzed export compiler kind is invalid",
                ));
            }
            if v5_source_not_analyzed {
                let compiler = export
                    .get("compiler")
                    .and_then(JsonValue::as_object)
                    .ok_or_else(|| {
                        invalid_deployment_manifest(
                            "v5 not-analyzed export compiler must be an object",
                        )
                    })?;
                require_exact_keys(
                    compiler.keys().map(String::as_str),
                    &["kind"],
                    "v5 not-analyzed export compiler",
                )?;
                if require_string_field(compiler, "kind", "v5 not-analyzed export compiler")?
                    != "not-analyzed"
                {
                    return Err(invalid_deployment_manifest(
                        "v5 not-analyzed export compiler kind is invalid",
                    ));
                }
            }
            let source = validate_export_source(
                export,
                entry_path,
                row_export_name,
                row_udf_kind,
                row_visibility,
                compiler_is_object && !v5_source_not_analyzed,
                decision == "wasm",
            )?;
            let export_key = DeploymentExportKey::new(runtime_module_path, row_export_name);

            let validated_export = match decision {
                "wasm" => {
                    eligible_count += 1;
                    selected_wasm_count += 1;
                    if version == DeploymentManifestVersion::V8
                        && context_reuse_analysis
                            .as_ref()
                            .is_none_or(|analysis| !analysis.contains_entry(entry_path))
                    {
                        return Err(invalid_deployment_manifest(
                            "deployment-v8 selects an entry outside its context-reuse analysis",
                        ));
                    }
                    let expected_reason = match version {
                        DeploymentManifestVersion::V5
                        | DeploymentManifestVersion::V6
                        | DeploymentManifestVersion::V8 => "runtimeCapability",
                        DeploymentManifestVersion::V2
                        | DeploymentManifestVersion::V3
                        | DeploymentManifestVersion::V4 => "staticEligibility",
                    };
                    if reason != expected_reason {
                        return Err(invalid_deployment_manifest(
                            "Wasm export routing reason is invalid",
                        ));
                    }
                    if matches!(
                        version,
                        DeploymentManifestVersion::V5
                            | DeploymentManifestVersion::V6
                            | DeploymentManifestVersion::V8
                    ) && !capability_entry_wasm_identities
                        .insert((entry_path.to_owned(), row_export_name.to_owned()))
                    {
                        return Err(invalid_deployment_manifest(
                            "exports contains a duplicate routed capability-entry identity",
                        ));
                    }
                    if !export.get("compiler").is_some_and(JsonValue::is_object) {
                        return Err(invalid_deployment_manifest(
                            "Wasm export compiler identity must be an object",
                        ));
                    }
                    if export_diagnostic_ids.is_none() {
                        return Err(invalid_deployment_manifest(
                            "Wasm export diagnostic census identities are missing",
                        ));
                    }
                    let package_reference = export
                        .get("packageReference")
                        .and_then(JsonValue::as_object)
                        .ok_or_else(|| {
                            invalid_deployment_manifest(
                                "Wasm export packageReference must be an object",
                            )
                        })?;
                    let package_reference = match version {
                        DeploymentManifestVersion::V2 => {
                            require_exact_keys(
                                package_reference.keys().map(String::as_str),
                                &["cacheKey", "kind"],
                                "Wasm export packageReference",
                            )?;
                            if require_string_field(
                                package_reference,
                                "kind",
                                "Wasm export packageReference",
                            )? != PACKAGE_ENTRY_KIND
                            {
                                return Err(invalid_deployment_manifest(
                                    "Wasm export packageReference kind is invalid",
                                ));
                            }
                            let cache_key = require_string_field(
                                package_reference,
                                "cacheKey",
                                "Wasm export packageReference",
                            )?;
                            validate_deployment_sha256("packageReference.cacheKey", cache_key)?;
                            DeploymentWasmPackageReference::Singleton {
                                package_key: cache_key.to_owned(),
                            }
                        },
                        DeploymentManifestVersion::V3 | DeploymentManifestVersion::V4 => {
                            require_exact_keys(
                                package_reference.keys().map(String::as_str),
                                &["cohortPackageId", "entryId", "entrySelectorId", "kind"],
                                "Wasm export packageReference",
                            )?;
                            if require_string_field(
                                package_reference,
                                "kind",
                                "Wasm export packageReference",
                            )? != COHORT_ROUTE_REFERENCE_KIND
                            {
                                return Err(invalid_deployment_manifest(
                                    "Wasm export cohort packageReference kind is invalid",
                                ));
                            }
                            let cohort_package_id = require_string_field(
                                package_reference,
                                "cohortPackageId",
                                "Wasm export packageReference",
                            )?;
                            let entry_id = require_string_field(
                                package_reference,
                                "entryId",
                                "Wasm export packageReference",
                            )?;
                            let entry_selector_id = require_string_field(
                                package_reference,
                                "entrySelectorId",
                                "Wasm export packageReference",
                            )?;
                            validate_deployment_sha256(
                                "packageReference.cohortPackageId",
                                cohort_package_id,
                            )?;
                            validate_deployment_sha256("packageReference.entryId", entry_id)?;
                            validate_entry_selector_id(
                                "packageReference.entrySelectorId",
                                entry_selector_id,
                            )?;
                            DeploymentWasmPackageReference::Cohort {
                                cohort_package_id: cohort_package_id.to_owned(),
                                entry_id: entry_id.to_owned(),
                                entry_selector_id: entry_selector_id.to_owned(),
                            }
                        },
                        DeploymentManifestVersion::V5 => {
                            require_exact_keys(
                                package_reference.keys().map(String::as_str),
                                &[
                                    "capabilityEntryPackageId",
                                    "entryId",
                                    "entrySelectorId",
                                    "kind",
                                    "routeId",
                                ],
                                "Wasm export packageReference",
                            )?;
                            if require_string_field(
                                package_reference,
                                "kind",
                                "Wasm export packageReference",
                            )? != CAPABILITY_ENTRY_ROUTE_REFERENCE_KIND
                            {
                                return Err(invalid_deployment_manifest(
                                    "Wasm export capability-entry packageReference kind is invalid",
                                ));
                            }
                            let capability_entry_package_id = require_string_field(
                                package_reference,
                                "capabilityEntryPackageId",
                                "Wasm export packageReference",
                            )?;
                            let entry_id = require_string_field(
                                package_reference,
                                "entryId",
                                "Wasm export packageReference",
                            )?;
                            let route_id = require_string_field(
                                package_reference,
                                "routeId",
                                "Wasm export packageReference",
                            )?;
                            let entry_selector_id = require_string_field(
                                package_reference,
                                "entrySelectorId",
                                "Wasm export packageReference",
                            )?;
                            validate_deployment_sha256(
                                "packageReference.capabilityEntryPackageId",
                                capability_entry_package_id,
                            )?;
                            validate_deployment_sha256("packageReference.entryId", entry_id)?;
                            validate_deployment_sha256("packageReference.routeId", route_id)?;
                            validate_entry_selector_id(
                                "packageReference.entrySelectorId",
                                entry_selector_id,
                            )?;
                            DeploymentWasmPackageReference::CapabilityEntry {
                                capability_entry_package_id: capability_entry_package_id.to_owned(),
                                entry_id: entry_id.to_owned(),
                                route_id: route_id.to_owned(),
                                entry_selector_id: entry_selector_id.to_owned(),
                            }
                        },
                        DeploymentManifestVersion::V6 | DeploymentManifestVersion::V8 => {
                            require_exact_keys(
                                package_reference.keys().map(String::as_str),
                                &[
                                    "cohortContractSha256",
                                    "entryId",
                                    "entrySelectorId",
                                    "kind",
                                    "routeId",
                                ],
                                "Wasm export packageReference",
                            )?;
                            if require_string_field(
                                package_reference,
                                "kind",
                                "Wasm export packageReference",
                            )? != MODULE_GRAPH_ROUTE_REFERENCE_KIND
                            {
                                return Err(invalid_deployment_manifest(
                                    "Wasm export module graph packageReference kind is invalid",
                                ));
                            }
                            let cohort_contract_sha256 = require_string_field(
                                package_reference,
                                "cohortContractSha256",
                                "Wasm export packageReference",
                            )?;
                            let entry_id = require_string_field(
                                package_reference,
                                "entryId",
                                "Wasm export packageReference",
                            )?;
                            let route_id = require_string_field(
                                package_reference,
                                "routeId",
                                "Wasm export packageReference",
                            )?;
                            let entry_selector_id = require_string_field(
                                package_reference,
                                "entrySelectorId",
                                "Wasm export packageReference",
                            )?;
                            validate_deployment_sha256(
                                "packageReference.cohortContractSha256",
                                cohort_contract_sha256,
                            )?;
                            validate_deployment_sha256("packageReference.entryId", entry_id)?;
                            validate_deployment_sha256("packageReference.routeId", route_id)?;
                            validate_entry_selector_id(
                                "packageReference.entrySelectorId",
                                entry_selector_id,
                            )?;
                            DeploymentWasmPackageReference::ModuleGraphCohort {
                                cohort_contract_sha256: cohort_contract_sha256.to_owned(),
                                entry_id: entry_id.to_owned(),
                                route_id: route_id.to_owned(),
                                entry_selector_id: entry_selector_id.to_owned(),
                            }
                        },
                    };
                    let artifact = export
                        .get("artifact")
                        .and_then(JsonValue::as_object)
                        .ok_or_else(|| {
                            invalid_deployment_manifest("Wasm export artifact must be an object")
                        })?;
                    let compiler_limits = export.get("compilerLimits").ok_or_else(|| {
                        invalid_deployment_manifest("Wasm export compilerLimits are missing")
                    })?;
                    if let DeploymentWasmPackageReference::ModuleGraphCohort {
                        cohort_contract_sha256,
                        entry_id,
                        route_id,
                        entry_selector_id,
                    } = &package_reference
                    {
                        require_exact_keys(
                            artifact.keys().map(String::as_str),
                            &["cohortContractSha256"],
                            "Wasm export artifact",
                        )?;
                        let artifact_contract_sha256 = require_string_field(
                            artifact,
                            "cohortContractSha256",
                            "Wasm export artifact",
                        )?;
                        validate_deployment_sha256(
                            "artifact.cohortContractSha256",
                            artifact_contract_sha256,
                        )?;
                        if artifact_contract_sha256 != cohort_contract_sha256
                            || effect_execution_mode
                                != Some(EffectExecutionMode::GuestPromiseEventLoop)
                        {
                            return Err(invalid_deployment_manifest(
                                "module graph artifact or effect-execution identity differs from \
                                 its route reference",
                            ));
                        }
                        let contract = module_graph_cohorts_by_contract
                            .get(cohort_contract_sha256.as_str())
                            .ok_or_else(|| {
                                invalid_deployment_manifest(
                                    "module graph route reference selects no embedded cohort \
                                     contract",
                                )
                            })?;
                        contract.validate_deployment_route(
                            entry_id,
                            route_id,
                            entry_selector_id,
                            entry_path,
                            runtime_module_path,
                            row_export_name,
                            row_udf_kind,
                            row_visibility,
                            export
                                .get("compiler")
                                .expect("validated export compiler disappeared"),
                            compiler_limits,
                            &artifact_precompiler,
                        )?;
                        ValidatedDeploymentExport::Wasm {
                            package_reference,
                            artifact: DeploymentWasmArtifact::ModuleGraphCohortContract {
                                cohort_contract_sha256: artifact_contract_sha256.to_owned(),
                            },
                            source: source.ok_or_else(|| {
                                invalid_deployment_manifest(
                                    "Wasm export source identity is incomplete",
                                )
                            })?,
                        }
                    } else if let DeploymentWasmPackageReference::CapabilityEntry {
                        capability_entry_package_id,
                        ..
                    } = &package_reference
                    {
                        require_exact_keys(
                            artifact.keys().map(String::as_str),
                            &["entryManifestSha256"],
                            "Wasm export artifact",
                        )?;
                        let manifest_sha256 = require_string_field(
                            artifact,
                            "entryManifestSha256",
                            "Wasm export artifact",
                        )?;
                        validate_deployment_sha256(
                            "artifact.entryManifestSha256",
                            manifest_sha256,
                        )?;
                        if manifest_sha256 != capability_entry_package_id {
                            return Err(invalid_deployment_manifest(
                                "capability-entry artifact identity differs from its package \
                                 reference",
                            ));
                        }
                        if effect_execution_mode != Some(EffectExecutionMode::GuestPromiseEventLoop)
                        {
                            return Err(invalid_deployment_manifest(
                                "capability-entry deployment requires guest-promise-event-loop",
                            ));
                        }
                        ValidatedDeploymentExport::Wasm {
                            package_reference,
                            artifact: DeploymentWasmArtifact::CapabilityEntryManifest {
                                manifest_sha256: manifest_sha256.to_owned(),
                                compiler_limits: compiler_limits.clone(),
                            },
                            source: source.ok_or_else(|| {
                                invalid_deployment_manifest(
                                    "Wasm export source identity is incomplete",
                                )
                            })?,
                        }
                    } else {
                        require_exact_keys(
                            artifact.keys().map(String::as_str),
                            &["executionManifest"],
                            "Wasm export artifact",
                        )?;
                        let execution_manifest =
                            artifact.get("executionManifest").ok_or_else(|| {
                                invalid_deployment_manifest(
                                    "Wasm export artifact executionManifest is missing",
                                )
                            })?;
                        validate_precompiler_execution_manifest(
                            &artifact_precompiler,
                            execution_manifest,
                            version,
                            &package_reference,
                        )?;
                        let route_effect_execution_mode = execution_manifest
                            .get("effectExecutionMode")
                            .map(|value| {
                                serde_json::from_value::<EffectExecutionMode>(value.clone())
                                    .map_err(|_| {
                                        invalid_deployment_manifest(
                                            "Wasm export effectExecutionMode is invalid",
                                        )
                                    })
                            })
                            .transpose()?;
                        if route_effect_execution_mode.is_some() && effect_execution_mode.is_none()
                        {
                            return Err(invalid_deployment_manifest(
                                "Wasm export effectExecutionMode is missing from the deployment",
                            ));
                        }
                        if let Some(effect_execution_mode) = effect_execution_mode
                            && route_effect_execution_mode.unwrap_or_default()
                                != effect_execution_mode
                        {
                            return Err(invalid_deployment_manifest(
                                "Wasm export effectExecutionMode differs from the deployment",
                            ));
                        }
                        let manifest_platform_limits =
                            execution_manifest.get("platformLimits").ok_or_else(|| {
                                invalid_deployment_manifest(
                                    "Wasm export execution manifest platformLimits are missing",
                                )
                            })?;
                        if compiler_limits != manifest_platform_limits {
                            return Err(invalid_deployment_manifest(
                                "Wasm export compilerLimits differ from execution manifest \
                                 platformLimits",
                            ));
                        }
                        let mut execution_manifest_bytes = Vec::new();
                        write_canonical_json(execution_manifest, &mut execution_manifest_bytes)
                            .expect(
                                "writing canonical execution manifest to a byte vector cannot fail",
                            );
                        if let DeploymentWasmPackageReference::Singleton { package_key } =
                            &package_reference
                        {
                            if sha256(&execution_manifest_bytes) != *package_key {
                                return Err(invalid_deployment_manifest(
                                    "Wasm export execution manifest does not match its cache key",
                                ));
                            }
                        }
                        ValidatedDeploymentExport::Wasm {
                            package_reference,
                            artifact: DeploymentWasmArtifact::LegacyExecutionManifest(
                                execution_manifest.clone(),
                            ),
                            source: source.ok_or_else(|| {
                                invalid_deployment_manifest(
                                    "Wasm export source identity is incomplete",
                                )
                            })?,
                        }
                    }
                },
                "existingRuntime" => {
                    match reason {
                        "not-selected" => {
                            eligible_count += 1;
                            unselected_eligible_count += 1;
                        },
                        "not-analyzed"
                            if matches!(
                                version,
                                DeploymentManifestVersion::V5
                                    | DeploymentManifestVersion::V6
                                    | DeploymentManifestVersion::V8
                            ) =>
                        {
                            ineligible_count += 1;
                        },
                        _ => {
                            return Err(invalid_deployment_manifest(
                                "existing-runtime export routing reason is invalid",
                            ));
                        },
                    }
                    if !export.get("artifact").is_some_and(JsonValue::is_null)
                        || !export
                            .get("packageReference")
                            .is_some_and(JsonValue::is_null)
                    {
                        return Err(invalid_deployment_manifest(
                            "existing-runtime export must not reference a package",
                        ));
                    }
                    if !export.get("compiler").is_some_and(JsonValue::is_object) {
                        return Err(invalid_deployment_manifest(
                            "existing-runtime export compiler identity must be an object",
                        ));
                    }
                    if export_diagnostic_ids.is_none() {
                        return Err(invalid_deployment_manifest(
                            "existing-runtime export diagnostic census identities are missing",
                        ));
                    }
                    if export.contains_key("compilerLimits") {
                        return Err(invalid_deployment_manifest(
                            "existing-runtime export must not contain compilerLimits",
                        ));
                    }
                    ValidatedDeploymentExport::ExistingRuntime {
                        udf_kind: row_udf_kind,
                    }
                },
                "v8Fallback" => {
                    let artifact_fallback = reason == "static-hermes-source-incompatibility-v1";
                    if artifact_fallback {
                        if !matches!(
                            version,
                            DeploymentManifestVersion::V4
                                | DeploymentManifestVersion::V5
                                | DeploymentManifestVersion::V6
                                | DeploymentManifestVersion::V8
                        ) {
                            return Err(invalid_deployment_manifest(
                                "artifact V8 fallback requires deployment manifest v4",
                            ));
                        }
                        eligible_count += 1;
                        artifact_fallback_count += 1;
                        artifact_fallback_identities.insert((
                            runtime_module_path
                                .strip_suffix(".js")
                                .expect("validated runtime module path must end in .js")
                                .to_owned(),
                            row_export_name.to_owned(),
                        ));
                    } else {
                        ineligible_count += 1;
                    }
                    if !matches!(
                        reason,
                        "staticIneligibility"
                            | "static-hermes-source-incompatibility-v1"
                            | "registration-reexport"
                            | "unsupported-registration-builder"
                            | "generated-source-kind-mismatch"
                            | "generated-source-registration-mismatch"
                    ) {
                        return Err(invalid_deployment_manifest(
                            "V8 fallback routing reason is invalid",
                        ));
                    }
                    if !export.get("artifact").is_some_and(JsonValue::is_null)
                        || !export
                            .get("packageReference")
                            .is_some_and(JsonValue::is_null)
                    {
                        return Err(invalid_deployment_manifest(
                            "V8 fallback export must not reference a package",
                        ));
                    }
                    let compiler_is_valid = if matches!(
                        reason,
                        "staticIneligibility" | "static-hermes-source-incompatibility-v1"
                    ) {
                        export.get("compiler").is_some_and(JsonValue::is_object)
                    } else {
                        export.get("compiler").is_some_and(JsonValue::is_null)
                    };
                    if !compiler_is_valid {
                        return Err(invalid_deployment_manifest(
                            "V8 fallback compiler identity is inconsistent with its reason",
                        ));
                    }
                    let diagnostics_are_valid = if matches!(
                        reason,
                        "staticIneligibility" | "static-hermes-source-incompatibility-v1"
                    ) {
                        export_diagnostic_ids.is_some()
                    } else {
                        export_diagnostic_ids.is_none() && !export.contains_key("operationSummary")
                    };
                    if !diagnostics_are_valid {
                        return Err(invalid_deployment_manifest(
                            "V8 fallback analysis fields are inconsistent with its reason",
                        ));
                    }
                    if artifact_fallback {
                        let compiler_limits = export.get("compilerLimits").ok_or_else(|| {
                            invalid_deployment_manifest(
                                "artifact V8 fallback compilerLimits are missing",
                            )
                        })?;
                        serde_json::from_value::<PlatformLimits>(compiler_limits.clone()).map_err(
                            |_| {
                                invalid_deployment_manifest(
                                    "artifact V8 fallback compilerLimits are invalid",
                                )
                            },
                        )?;
                    } else if export.contains_key("compilerLimits") {
                        return Err(invalid_deployment_manifest(
                            "V8 fallback export must not contain compilerLimits",
                        ));
                    }
                    ValidatedDeploymentExport::V8Fallback {
                        udf_kind: row_udf_kind,
                    }
                },
                _ => {
                    return Err(invalid_deployment_manifest(
                        "export routing decision is invalid",
                    ));
                },
            };
            if validated_exports
                .insert(export_key, validated_export)
                .is_some()
            {
                return Err(invalid_deployment_manifest(
                    "exports contains a duplicate function identity",
                ));
            }
        }
        let mut selected_artifact_identities = validated_exports
            .iter()
            .filter_map(|(key, export)| {
                matches!(export, ValidatedDeploymentExport::Wasm { .. }).then(|| {
                    (
                        key.runtime_module_path
                            .strip_suffix(".js")
                            .expect("validated runtime module path must end in .js")
                            .to_owned(),
                        key.export_name.clone(),
                    )
                })
            })
            .collect::<BTreeSet<_>>();
        selected_artifact_identities.extend(artifact_fallback_identities);
        match compile_selection {
            DeploymentCompileSelection::HistoricalExplicit(selected_identities)
                if selected_identities != selected_artifact_identities =>
            {
                return Err(invalid_deployment_manifest(
                    "compileSelection does not match the selected artifact exports",
                ));
            },
            DeploymentCompileSelection::CapabilityEntryExplicit(selected_identities)
                if selected_identities != capability_entry_wasm_identities =>
            {
                return Err(invalid_deployment_manifest(
                    "compileSelection does not match the routed capability-entry Wasm exports",
                ));
            },
            DeploymentCompileSelection::AllEligible
                if unselected_eligible_count != 0
                    || selected_wasm_count + artifact_fallback_count != eligible_count =>
            {
                return Err(invalid_deployment_manifest(
                    "all-eligible compileSelection does not process every eligible export",
                ));
            },
            DeploymentCompileSelection::HistoricalExplicit(_)
            | DeploymentCompileSelection::CapabilityEntryExplicit(_)
            | DeploymentCompileSelection::AllEligible => {},
        }
        let total = exports.len();
        for (field, actual) in [
            ("actionsOnExistingRuntime", actions.len()),
            ("eligible", eligible_count),
            ("ineligible", ineligible_count),
            ("mutations", mutation_count),
            ("queries", query_count),
            ("selectedWasm", selected_wasm_count),
            ("total", total),
            ("unselectedEligible", unselected_eligible_count),
        ] {
            if require_usize_field(counts, field, "counts")? != actual {
                return Err(invalid_deployment_manifest(format!(
                    "counts.{field} does not match the deployment entries"
                )));
            }
        }
        if matches!(
            version,
            DeploymentManifestVersion::V4
                | DeploymentManifestVersion::V5
                | DeploymentManifestVersion::V6
                | DeploymentManifestVersion::V8
        ) && require_usize_field(counts, "artifactFallback", "counts")?
            != artifact_fallback_count
        {
            return Err(invalid_deployment_manifest(
                "counts.artifactFallback does not match the deployment entries",
            ));
        }
        if selected_wasm_count + artifact_fallback_count + unselected_eligible_count
            != eligible_count
            || eligible_count + ineligible_count != total
            || mutation_count + query_count != total
        {
            return Err(invalid_deployment_manifest(
                "deployment counts do not partition the exports",
            ));
        }
        Ok(Self {
            artifact_precompiler,
            deployment_sha256,
            exports: validated_exports,
            module_graph_binding,
            module_graph_cohorts,
            #[cfg(test)]
            existing_runtime_actions: action_identities,
            version,
        })
    }

    pub(crate) fn deployment_sha256(&self) -> &str {
        &self.deployment_sha256
    }

    pub(crate) fn selected_wasm_exports(&self) -> Vec<SelectedDeploymentWasmExport> {
        self.exports
            .iter()
            .filter_map(|(key, export)| {
                let ValidatedDeploymentExport::Wasm { source, .. } = export else {
                    return None;
                };
                Some(SelectedDeploymentWasmExport {
                    runtime_module_path: key.runtime_module_path.clone(),
                    export_name: key.export_name.clone(),
                    udf_kind: source.udf_kind,
                })
            })
            .collect()
    }

    /// Return the single source-package identity authenticated by every
    /// selected Wasm export in this deployment.
    pub(crate) fn deployed_source_package_runtime_content_sha256(
        &self,
    ) -> Result<&str, WasmUdfPackageError> {
        let mut source_package_runtime_content_sha256 = None;
        for export in self.exports.values() {
            let ValidatedDeploymentExport::Wasm { source, .. } = export else {
                continue;
            };
            let candidate = source
                .deployed_runtime_binding
                .identity()?
                .source_package_runtime_content_sha256()
                .ok_or_else(|| {
                    invalid_deployment_manifest(
                        "source-keyed generation requires runtime-content deployment identity v2",
                    )
                })?;
            if source_package_runtime_content_sha256.is_some_and(|expected| expected != candidate) {
                return Err(invalid_deployment_manifest(
                    "selected Wasm exports do not bind one source package",
                ));
            }
            source_package_runtime_content_sha256 = Some(candidate);
        }
        source_package_runtime_content_sha256.ok_or_else(|| {
            invalid_deployment_manifest(
                "source-keyed generation contains no source-bound Wasm export",
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn contains_existing_runtime_action(
        &self,
        runtime_module_path: &str,
        export_name: &str,
    ) -> bool {
        self.existing_runtime_actions
            .contains(&DeploymentExportKey::new(runtime_module_path, export_name))
    }

    pub(crate) fn export_routing<'a>(
        &'a self,
        runtime_module_path: &'a str,
        export_name: &'a str,
        udf_kind: UdfKind,
    ) -> Result<DeploymentExportRouting<'a>, WasmUdfPackageError> {
        match self.lookup_export(runtime_module_path, export_name, udf_kind)? {
            ValidatedDeploymentExport::Wasm {
                package_reference,
                source,
                ..
            } => {
                let deployed_runtime_identity = source.deployed_runtime_binding.identity()?;
                let (runtime_entry, entry_id, entry_selector_id, route_id) = match package_reference
                {
                    DeploymentWasmPackageReference::Singleton { .. } => (
                        DeploymentRuntimeEntry::LegacyExport {
                            runtime_module_path,
                            export_name,
                        },
                        None,
                        None,
                        None,
                    ),
                    DeploymentWasmPackageReference::Cohort {
                        entry_selector_id, ..
                    } => (
                        DeploymentRuntimeEntry::LegacyExport {
                            runtime_module_path,
                            export_name,
                        },
                        None,
                        Some(entry_selector_id.clone()),
                        None,
                    ),
                    DeploymentWasmPackageReference::CapabilityEntry {
                        entry_id,
                        route_id,
                        entry_selector_id,
                        ..
                    } => (
                        DeploymentRuntimeEntry::CapabilityPackage,
                        Some(entry_id.clone()),
                        Some(entry_selector_id.clone()),
                        Some(route_id.clone()),
                    ),
                    DeploymentWasmPackageReference::ModuleGraphCohort {
                        entry_id,
                        route_id,
                        entry_selector_id,
                        ..
                    } => (
                        DeploymentRuntimeEntry::CapabilityPackage,
                        Some(entry_id.clone()),
                        Some(entry_selector_id.clone()),
                        Some(route_id.clone()),
                    ),
                };
                Ok(DeploymentExportRouting::Wasm {
                    package_key: package_reference.package_key(),
                    runtime_entry,
                    route_lease: DeploymentRouteLease {
                        entry_id,
                        entry_selector_id,
                        export_name: source.export_name.clone(),
                        route_id,
                        udf_kind: source.udf_kind,
                        visibility: source.visibility.clone(),
                    },
                    deployed_runtime_identity,
                })
            },
            ValidatedDeploymentExport::ExistingRuntime { .. } => {
                Ok(DeploymentExportRouting::ExistingRuntime)
            },
            ValidatedDeploymentExport::V8Fallback { .. } => Ok(DeploymentExportRouting::V8Fallback),
        }
    }

    pub(crate) fn deployed_runtime_identity(
        &self,
        runtime_module_path: &str,
        export_name: &str,
        udf_kind: UdfKind,
    ) -> Result<Option<&DeployedRuntimeIdentity>, WasmUdfPackageError> {
        match self.lookup_export(runtime_module_path, export_name, udf_kind)? {
            ValidatedDeploymentExport::Wasm { source, .. } => {
                Ok(Some(source.deployed_runtime_binding.identity()?))
            },
            ValidatedDeploymentExport::ExistingRuntime { .. }
            | ValidatedDeploymentExport::V8Fallback { .. } => Ok(None),
        }
    }

    pub(crate) fn load_export_package(
        &self,
        artifact_cache_root: &Path,
        runtime_module_path: &str,
        export_name: &str,
        udf_kind: UdfKind,
        runtime: &RuntimeCompatibility<'_>,
        serialized_module_snapshot_directory: &Path,
    ) -> Result<DeploymentExportPackage, WasmUdfPackageError> {
        let packages_root = self.package_registry_root(artifact_cache_root);
        self.load_export_package_from_packages_root(
            &packages_root,
            runtime_module_path,
            export_name,
            udf_kind,
            runtime,
            serialized_module_snapshot_directory,
        )
    }

    pub(crate) fn load_export_package_from_packages_root(
        &self,
        packages_root: &Path,
        runtime_module_path: &str,
        export_name: &str,
        udf_kind: UdfKind,
        runtime: &RuntimeCompatibility<'_>,
        serialized_module_snapshot_directory: &Path,
    ) -> Result<DeploymentExportPackage, WasmUdfPackageError> {
        self.load_export_package_from_packages_root_impl(
            packages_root,
            runtime_module_path,
            export_name,
            udf_kind,
            runtime,
            serialized_module_snapshot_directory,
            true,
        )
    }

    fn load_export_package_from_packages_root_impl(
        &self,
        packages_root: &Path,
        runtime_module_path: &str,
        export_name: &str,
        udf_kind: UdfKind,
        runtime: &RuntimeCompatibility<'_>,
        serialized_module_snapshot_directory: &Path,
        require_bound_runtime_identity: bool,
    ) -> Result<DeploymentExportPackage, WasmUdfPackageError> {
        let (package_reference, artifact, source) =
            match self.lookup_export(runtime_module_path, export_name, udf_kind)? {
                ValidatedDeploymentExport::Wasm {
                    package_reference,
                    artifact,
                    source,
                } => {
                    if require_bound_runtime_identity {
                        source.deployed_runtime_binding.identity()?;
                    }
                    (package_reference, artifact, source)
                },
                ValidatedDeploymentExport::ExistingRuntime { .. } => {
                    return Ok(DeploymentExportPackage::ExistingRuntime);
                },
                ValidatedDeploymentExport::V8Fallback { .. } => {
                    return Ok(DeploymentExportPackage::V8Fallback);
                },
            };
        let package_key = package_reference.package_key();
        let package_path = packages_root.join(package_key);
        let package = match package_reference {
            DeploymentWasmPackageReference::Singleton { .. } => {
                load_validated_package_with_precompiler(
                    &package_path,
                    runtime,
                    Some(&self.artifact_precompiler),
                    serialized_module_snapshot_directory,
                )?
            },
            DeploymentWasmPackageReference::Cohort {
                entry_id,
                entry_selector_id,
                ..
            } => load_validated_cohort_package(
                &package_path,
                match artifact {
                    DeploymentWasmArtifact::LegacyExecutionManifest(manifest) => manifest,
                    DeploymentWasmArtifact::CapabilityEntryManifest { .. } => {
                        return Err(invalid_deployment_manifest(
                            "cohort package reference used a capability-entry artifact",
                        ));
                    },
                    DeploymentWasmArtifact::ModuleGraphCohortContract { .. } => {
                        return Err(invalid_deployment_manifest(
                            "cohort package reference used a module graph artifact",
                        ));
                    },
                },
                entry_id,
                entry_selector_id,
                runtime,
                &self.artifact_precompiler,
                serialized_module_snapshot_directory,
            )?,
            DeploymentWasmPackageReference::CapabilityEntry {
                entry_id,
                route_id,
                entry_selector_id,
                ..
            } => {
                let DeploymentWasmArtifact::CapabilityEntryManifest {
                    manifest_sha256,
                    compiler_limits,
                } = artifact
                else {
                    return Err(invalid_deployment_manifest(
                        "capability-entry package reference used a legacy artifact",
                    ));
                };
                load_validated_capability_package(
                    &package_path,
                    manifest_sha256,
                    compiler_limits,
                    entry_id,
                    route_id,
                    entry_selector_id,
                    &source.module_path,
                    runtime_module_path,
                    export_name,
                    udf_kind,
                    &source.visibility,
                    runtime,
                    &self.artifact_precompiler,
                    serialized_module_snapshot_directory,
                )?
            },
            DeploymentWasmPackageReference::ModuleGraphCohort { .. } => {
                return Err(invalid_deployment_manifest(
                    "module graph routes do not have monolithic packages",
                ));
            },
        };
        if package.package_key != package_key {
            return Err(invalid_deployment_manifest(
                "loaded package key does not match the deployment export",
            ));
        }
        if let (
            ValidatedWasmUdfPackageIdentity::Legacy(package_manifest),
            DeploymentWasmArtifact::LegacyExecutionManifest(execution_manifest),
        ) = (&package.identity, artifact)
        {
            let mut execution_manifest_bytes = Vec::new();
            write_canonical_json(execution_manifest, &mut execution_manifest_bytes)
                .expect("writing canonical execution manifest cannot fail");
            let deployment_execution_manifest =
                WasmUdfExecutionManifest::parse_for_runtime(&execution_manifest_bytes, runtime)?;
            if package_manifest != &deployment_execution_manifest {
                return Err(invalid_deployment_manifest(
                    "package execution manifest differs from the deployment export",
                ));
            }
            let package_source = package_manifest.source();
            if package_source.module_path() != source.module_path
                || package_source.runtime_module_path() != runtime_module_path
                || package_source.export_name() != source.export_name
                || package_source.udf_kind() != source.udf_kind
                || package_source.resolved_graph_sha256().as_str() != source.resolved_graph_sha256
                || package_source.export_sha256().as_str() != source.export_sha256
            {
                return Err(invalid_deployment_manifest(
                    "package source identity differs from the deployment export",
                ));
            }
        }
        Ok(DeploymentExportPackage::Wasm(package))
    }

    #[cfg(test)]
    pub(crate) fn load_export_package_for_compatibility_test(
        &self,
        packages_root: &Path,
        runtime_module_path: &str,
        export_name: &str,
        udf_kind: UdfKind,
        runtime: &RuntimeCompatibility<'_>,
        serialized_module_snapshot_directory: &Path,
    ) -> Result<DeploymentExportPackage, WasmUdfPackageError> {
        self.load_export_package_from_packages_root_impl(
            packages_root,
            runtime_module_path,
            export_name,
            udf_kind,
            runtime,
            serialized_module_snapshot_directory,
            false,
        )
    }

    #[cfg(test)]
    pub(crate) fn package_key_for_compatibility_test(
        &self,
        runtime_module_path: &str,
        export_name: &str,
        udf_kind: UdfKind,
    ) -> Result<&str, WasmUdfPackageError> {
        match self.lookup_export(runtime_module_path, export_name, udf_kind)? {
            ValidatedDeploymentExport::Wasm {
                package_reference, ..
            } => Ok(package_reference.package_key()),
            ValidatedDeploymentExport::ExistingRuntime { .. }
            | ValidatedDeploymentExport::V8Fallback { .. } => Err(invalid_deployment_manifest(
                "compatibility-test export is not routed to Wasm",
            )),
        }
    }

    pub(crate) fn authenticated_module_graph_route_material(
        &self,
        runtime_module_path: &str,
        export_name: &str,
        udf_kind: UdfKind,
        runtime: &RuntimeCompatibility<'_>,
    ) -> Result<AuthenticatedModuleGraphRouteMaterial, WasmUdfPackageError> {
        let (cohort_contract_sha256, entry_id, route_id, entry_selector_id, artifact, source) =
            match self.lookup_export(runtime_module_path, export_name, udf_kind)? {
                ValidatedDeploymentExport::Wasm {
                    package_reference:
                        DeploymentWasmPackageReference::ModuleGraphCohort {
                            cohort_contract_sha256,
                            entry_id,
                            route_id,
                            entry_selector_id,
                        },
                    artifact,
                    source,
                } => (
                    cohort_contract_sha256,
                    entry_id,
                    route_id,
                    entry_selector_id,
                    artifact,
                    source,
                ),
                ValidatedDeploymentExport::Wasm { .. } => {
                    return Err(invalid_deployment_manifest(
                        "selected Wasm export is not a module graph route",
                    ));
                },
                ValidatedDeploymentExport::ExistingRuntime { .. }
                | ValidatedDeploymentExport::V8Fallback { .. } => {
                    return Err(invalid_deployment_manifest(
                        "selected export is not routed to a module graph",
                    ));
                },
            };
        source.deployed_runtime_binding.identity()?;
        let DeploymentWasmArtifact::ModuleGraphCohortContract {
            cohort_contract_sha256: artifact_contract_sha256,
        } = artifact
        else {
            return Err(invalid_deployment_manifest(
                "module graph route has a non-graph artifact identity",
            ));
        };
        if artifact_contract_sha256 != cohort_contract_sha256 {
            return Err(invalid_deployment_manifest(
                "module graph route and artifact contract identities differ",
            ));
        }
        let contract = self
            .module_graph_cohorts
            .iter()
            .find(|contract| contract.cohort_contract_sha256 == *cohort_contract_sha256)
            .ok_or_else(|| {
                invalid_deployment_manifest(
                    "module graph route selects no authenticated cohort contract",
                )
            })?;
        let validated = contract.validate()?;
        contract
            .execution
            .validate_capability_entry(OPAQUE_VALUE_ABI_VERSION, runtime)?;
        if contract.precompiler_material_identity != self.artifact_precompiler
            || contract.precompiler_material_identity.target_triple != runtime.target_triple
            || contract.precompiler_material_identity.wasmtime_revision != runtime.wasmtime_revision
            || contract.engine.compatibility_sha256 != runtime.engine_compatibility_sha256
            || contract.engine.configuration_sha256 != runtime.engine_configuration_sha256
            || contract.engine.revision != runtime.wasmtime_revision
            || contract.engine.target.triple != runtime.target_triple
            || contract.engine.target.cpu != runtime.target_cpu
        {
            return Err(invalid_deployment_manifest(
                "module graph cohort precompiler identity is incompatible with the runtime",
            ));
        }
        let route = validated
            .routes
            .iter()
            .find(|route| route.route_id == *route_id)
            .ok_or_else(|| {
                invalid_deployment_manifest(
                    "module graph route selects no authenticated cohort route",
                )
            })?;
        if route.entry_id != *entry_id || route.entry_selector_id != *entry_selector_id {
            return Err(invalid_deployment_manifest(
                "module graph route reference differs from its cohort route",
            ));
        }
        let entry_selector = u64::from_str_radix(entry_selector_id, 16)
            .map_err(|_| invalid_deployment_manifest("module graph route selector is invalid"))?;
        let permitted_conditional_convex_imports = permitted_conditional_convex_imports(
            contract.execution.value_mode(),
            contract.execution.effect_execution_mode(),
            contract.execution.imported_operations(),
        );
        Ok(AuthenticatedModuleGraphRouteMaterial {
            execution: contract.execution.clone(),
            identity: ValidatedWasmUdfPackageIdentity::ModuleGraphCohort(
                ModuleGraphCohortRuntimeIdentity {
                    cohort_contract_sha256: contract.cohort_contract_sha256.clone(),
                    compiler: contract.compiler.clone(),
                    entries: validated.entries,
                    precompiler_material_identity: contract.precompiler_material_identity.clone(),
                    routes: validated.routes,
                },
            ),
            package_key: cohort_contract_sha256.clone(),
            entry_selector,
            permitted_conditional_convex_imports,
        })
    }

    fn package_keys(&self) -> BTreeSet<&str> {
        self.exports
            .values()
            .filter_map(|export| match export {
                ValidatedDeploymentExport::Wasm {
                    package_reference, ..
                } => match package_reference {
                    DeploymentWasmPackageReference::ModuleGraphCohort { .. } => None,
                    DeploymentWasmPackageReference::Singleton { .. }
                    | DeploymentWasmPackageReference::Cohort { .. }
                    | DeploymentWasmPackageReference::CapabilityEntry { .. } => {
                        Some(package_reference.package_key())
                    },
                },
                ValidatedDeploymentExport::ExistingRuntime { .. }
                | ValidatedDeploymentExport::V8Fallback { .. } => None,
            })
            .collect()
    }

    fn capability_entry_route_ids(&self) -> BTreeSet<String> {
        self.exports
            .values()
            .filter_map(|export| match export {
                ValidatedDeploymentExport::Wasm {
                    package_reference:
                        DeploymentWasmPackageReference::CapabilityEntry { route_id, .. },
                    ..
                } => Some(route_id.clone()),
                ValidatedDeploymentExport::Wasm { .. }
                | ValidatedDeploymentExport::ExistingRuntime { .. }
                | ValidatedDeploymentExport::V8Fallback { .. } => None,
            })
            .collect()
    }

    /// Return the authenticated route digests eligible for V8-primary shadow
    /// execution of the requested UDF kind.
    ///
    /// The manifest validates every returned route ID as a SHA-256 digest.
    /// This deliberately omits module paths, export names, and all other
    /// deployment metadata.
    pub(crate) fn shadow_route_ids(&self, udf_kind: UdfKind) -> BTreeSet<String> {
        self.exports
            .values()
            .filter_map(|export| match export {
                ValidatedDeploymentExport::Wasm {
                    package_reference:
                        DeploymentWasmPackageReference::CapabilityEntry { route_id, .. }
                        | DeploymentWasmPackageReference::ModuleGraphCohort { route_id, .. },
                    source,
                    ..
                } if source.udf_kind == udf_kind => Some(route_id.clone()),
                ValidatedDeploymentExport::Wasm { .. }
                | ValidatedDeploymentExport::ExistingRuntime { .. }
                | ValidatedDeploymentExport::V8Fallback { .. } => None,
            })
            .collect()
    }

    fn module_graph_routes(&self) -> BTreeMap<String, DeploymentModuleGraphRoute> {
        self.exports
            .values()
            .filter_map(|export| match export {
                ValidatedDeploymentExport::Wasm {
                    package_reference:
                        DeploymentWasmPackageReference::ModuleGraphCohort {
                            cohort_contract_sha256,
                            entry_id,
                            entry_selector_id,
                            route_id,
                        },
                    source,
                    ..
                } => Some((
                    route_id.clone(),
                    DeploymentModuleGraphRoute {
                        cohort_contract_sha256: cohort_contract_sha256.clone(),
                        entry_id: entry_id.clone(),
                        entry_selector_id: entry_selector_id.clone(),
                        export_name: source.export_name.clone(),
                        route_id: route_id.clone(),
                        udf_kind: match source.udf_kind {
                            UdfKind::Query => "query",
                            UdfKind::Mutation => "mutation",
                        }
                        .to_owned(),
                        visibility: source.visibility.clone(),
                    },
                )),
                ValidatedDeploymentExport::Wasm { .. }
                | ValidatedDeploymentExport::ExistingRuntime { .. }
                | ValidatedDeploymentExport::V8Fallback { .. } => None,
            })
            .collect()
    }

    pub(crate) fn package_registry_root(&self, artifact_cache_root: &Path) -> PathBuf {
        artifact_cache_root.join(self.package_registry_directory())
    }

    fn package_registry_directory(&self) -> &'static str {
        match self.version {
            DeploymentManifestVersion::V2 => "packages",
            DeploymentManifestVersion::V3 | DeploymentManifestVersion::V4 => "cohort-packages",
            DeploymentManifestVersion::V5 => "v5/capability-entry-packages",
            DeploymentManifestVersion::V6 | DeploymentManifestVersion::V8 => "packages",
        }
    }

    #[cfg(test)]
    pub(crate) fn empty_for_test(deployment_sha256: &str) -> Self {
        validate_deployment_sha256("deploymentSha256", deployment_sha256)
            .expect("test deployment SHA-256 must be valid");
        Self {
            artifact_precompiler: test_precompiler_material_identity(),
            deployment_sha256: deployment_sha256.to_owned(),
            exports: BTreeMap::new(),
            module_graph_binding: None,
            module_graph_cohorts: Vec::new(),
            existing_runtime_actions: BTreeSet::new(),
            version: DeploymentManifestVersion::V2,
        }
    }

    fn lookup_export(
        &self,
        runtime_module_path: &str,
        export_name: &str,
        udf_kind: UdfKind,
    ) -> Result<&ValidatedDeploymentExport, WasmUdfPackageError> {
        let export = self
            .exports
            .get(&DeploymentExportKey::new(runtime_module_path, export_name))
            .ok_or_else(|| invalid_deployment_manifest("selected export is missing"))?;
        let export_udf_kind = match export {
            ValidatedDeploymentExport::Wasm { source, .. } => source.udf_kind,
            ValidatedDeploymentExport::ExistingRuntime { udf_kind } => *udf_kind,
            ValidatedDeploymentExport::V8Fallback { udf_kind } => *udf_kind,
        };
        if export_udf_kind != udf_kind {
            return Err(invalid_deployment_manifest(
                "selected export UDF kind differs from the invocation",
            ));
        }
        Ok(export)
    }
}

fn validate_deployment_artifact_precompiler(
    value: &JsonValue,
) -> Result<PrecompilerMaterialIdentity, WasmUdfPackageError> {
    let artifact_precompiler: DeploymentArtifactPrecompiler = serde_json::from_value(value.clone())
        .map_err(|_| {
            invalid_deployment_manifest(
                "artifactPrecompiler has missing, unknown, or invalid fields",
            )
        })?;
    let identity = artifact_precompiler.material_identity;
    validate_precompiler_material_identity(&identity, "artifactPrecompiler.materialIdentity")?;
    Ok(identity)
}

fn validate_precompiler_material_identity(
    identity: &PrecompilerMaterialIdentity,
    description: &str,
) -> Result<(), WasmUdfPackageError> {
    if identity.kind != VERIFIED_PRECOMPILER_PACKAGE_KIND
        || identity.manifest_kind != PRECOMPILER_PACKAGE_MANIFEST_KIND
        || identity.manifest_schema_version != PRECOMPILER_PACKAGE_MANIFEST_SCHEMA_VERSION
    {
        return Err(invalid_deployment_manifest(
            "artifactPrecompiler material identity kind or schema is unsupported",
        ));
    }
    for (field, digest) in [
        ("binary.sha256", identity.binary.sha256.as_str()),
        ("manifestSha256", identity.manifest_sha256.as_str()),
        ("packageId", identity.package_id.as_str()),
        ("sourceTreeSha256", identity.source_tree_sha256.as_str()),
    ] {
        validate_deployment_sha256(&format!("{description}.{field}"), digest)?;
    }
    if identity.binary.size == 0 {
        return Err(invalid_deployment_manifest(format!(
            "{description} binary size must be positive"
        )));
    }
    for (field, value) in [
        ("targetTriple", identity.target_triple.as_str()),
        ("wasmtimeRevision", identity.wasmtime_revision.as_str()),
    ] {
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(invalid_deployment_manifest(format!(
                "{description}.{field} is invalid"
            )));
        }
    }
    Ok(())
}

fn validate_precompiler_execution_manifest(
    precompiler: &PrecompilerMaterialIdentity,
    execution_manifest: &JsonValue,
    version: DeploymentManifestVersion,
    package_reference: &DeploymentWasmPackageReference,
) -> Result<(), WasmUdfPackageError> {
    let manifest_schema_version = execution_manifest
        .get("manifestSchemaVersion")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            invalid_deployment_manifest(
                "Wasm export execution manifest schema version must be an integer",
            )
        })?;
    let schema_matches_deployment = match version {
        DeploymentManifestVersion::V2 => manifest_schema_version == 3,
        DeploymentManifestVersion::V3 => manifest_schema_version == 4,
        DeploymentManifestVersion::V4 => matches!(manifest_schema_version, 4 | 5 | 6),
        DeploymentManifestVersion::V5
        | DeploymentManifestVersion::V6
        | DeploymentManifestVersion::V8 => false,
    };
    if !schema_matches_deployment {
        return Err(invalid_deployment_manifest(
            "deployment kind and execution manifest schema version disagree",
        ));
    }
    let artifact = execution_manifest
        .get("artifact")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| {
            invalid_deployment_manifest("Wasm export execution manifest artifact must be an object")
        })?;
    if require_string_field(
        artifact,
        "targetTriple",
        "Wasm export execution manifest artifact",
    )? != precompiler.target_triple
        || require_string_field(
            artifact,
            "wasmtimeRevision",
            "Wasm export execution manifest artifact",
        )? != precompiler.wasmtime_revision
    {
        return Err(invalid_deployment_manifest(
            "artifactPrecompiler identity differs from a Wasm execution manifest",
        ));
    }
    match package_reference {
        DeploymentWasmPackageReference::Singleton { .. } => {
            if artifact.contains_key("cohort")
                || artifact.contains_key("entryId")
                || artifact.contains_key("entrySelectorId")
            {
                return Err(invalid_deployment_manifest(
                    "singleton package execution manifest contains cohort identity",
                ));
            }
        },
        DeploymentWasmPackageReference::Cohort {
            cohort_package_id,
            entry_id,
            entry_selector_id,
        } => {
            let cohort = artifact
                .get("cohort")
                .and_then(JsonValue::as_object)
                .ok_or_else(|| {
                    invalid_deployment_manifest(
                        "cohort execution manifest artifact cohort must be an object",
                    )
                })?;
            if require_string_field(cohort, "packageId", "cohort execution manifest artifact")?
                != cohort_package_id
                || require_string_field(
                    cohort,
                    "manifestSha256",
                    "cohort execution manifest artifact",
                )? != cohort_package_id
                || require_string_field(artifact, "entryId", "cohort execution manifest artifact")?
                    != entry_id
                || require_string_field(
                    artifact,
                    "entrySelectorId",
                    "cohort execution manifest artifact",
                )? != entry_selector_id
            {
                return Err(invalid_deployment_manifest(
                    "cohort packageReference differs from the execution manifest artifact",
                ));
            }
        },
        DeploymentWasmPackageReference::CapabilityEntry { .. } => {
            return Err(invalid_deployment_manifest(
                "capability-entry package used the legacy execution-manifest validator",
            ));
        },
        DeploymentWasmPackageReference::ModuleGraphCohort { .. } => {
            return Err(invalid_deployment_manifest(
                "module graph route used the legacy execution-manifest validator",
            ));
        },
    }
    Ok(())
}

fn validate_export_source(
    export: &serde_json::Map<String, JsonValue>,
    entry_path: &str,
    export_name: &str,
    udf_kind: UdfKind,
    visibility: &str,
    analyzed: bool,
    selected_wasm: bool,
) -> Result<Option<DeploymentExportSource>, WasmUdfPackageError> {
    let source = export
        .get("source")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| invalid_deployment_manifest("export source must be an object"))?;
    if !analyzed {
        require_exact_keys(
            source.keys().map(String::as_str),
            &["exportName", "modulePath", "udfKind"],
            "export source",
        )?;
        if require_string_field(source, "modulePath", "export source")? != entry_path
            || require_string_field(source, "exportName", "export source")? != export_name
            || parse_udf_kind(require_string_field(source, "udfKind", "export source")?)?
                != udf_kind
        {
            return Err(invalid_deployment_manifest(
                "export row and source identity disagree",
            ));
        }
        return Ok(None);
    }
    let deployed_runtime_binding = if selected_wasm {
        Some(parse_deployed_runtime_binding(source)?)
    } else {
        require_exact_keys(
            source.keys().map(String::as_str),
            &[
                "exportName",
                "exportSha256",
                "modulePath",
                "resolvedGraphSha256",
                "udfKind",
            ],
            "export source",
        )?;
        None
    };
    let parsed_export_name =
        require_string_field(source, "exportName", "export source")?.to_owned();
    let parsed_export_sha256 =
        require_string_field(source, "exportSha256", "export source")?.to_owned();
    let parsed_module_path =
        require_string_field(source, "modulePath", "export source")?.to_owned();
    let parsed_resolved_graph_sha256 =
        require_string_field(source, "resolvedGraphSha256", "export source")?.to_owned();
    let parsed_udf_kind =
        parse_udf_kind(require_string_field(source, "udfKind", "export source")?)?;
    validate_deployment_sha256("source.exportSha256", &parsed_export_sha256)?;
    validate_deployment_sha256("source.resolvedGraphSha256", &parsed_resolved_graph_sha256)?;
    if parsed_module_path != entry_path
        || parsed_export_name != export_name
        || parsed_udf_kind != udf_kind
    {
        return Err(invalid_deployment_manifest(
            "export row and source identity disagree",
        ));
    }
    Ok(
        deployed_runtime_binding.map(|deployed_runtime_binding| DeploymentExportSource {
            export_name: parsed_export_name,
            export_sha256: parsed_export_sha256,
            module_path: parsed_module_path,
            resolved_graph_sha256: parsed_resolved_graph_sha256,
            udf_kind: parsed_udf_kind,
            visibility: visibility.to_owned(),
            deployed_runtime_binding,
        }),
    )
}

fn parse_deployed_runtime_binding(
    source: &serde_json::Map<String, JsonValue>,
) -> Result<DeploymentRuntimeBinding, WasmUdfPackageError> {
    let has_identity = source.contains_key("deployedRuntimeIdentity");
    let has_diagnostic = source.contains_key("deployedRuntimeIdentityDiagnostic");
    let binding_field = match (has_identity, has_diagnostic) {
        (true, false) => "deployedRuntimeIdentity",
        (false, true) => "deployedRuntimeIdentityDiagnostic",
        (true, true) => {
            return Err(invalid_deployment_manifest(
                "selected Wasm export source must not contain both deployedRuntimeIdentity and \
                 deployedRuntimeIdentityDiagnostic",
            ));
        },
        (false, false) => {
            return Err(invalid_deployment_manifest(
                "selected Wasm export source must contain exactly one deployed-runtime identity \
                 or diagnostic",
            ));
        },
    };
    require_exact_keys(
        source.keys().map(String::as_str),
        &[
            binding_field,
            "exportName",
            "exportSha256",
            "modulePath",
            "resolvedGraphSha256",
            "udfKind",
        ],
        "selected Wasm export source",
    )?;
    if has_identity {
        let identity = source
            .get(binding_field)
            .and_then(JsonValue::as_object)
            .ok_or_else(|| {
                invalid_deployment_manifest("source deployedRuntimeIdentity must be an object")
            })?;
        let kind = require_string_field(identity, "kind", "source deployedRuntimeIdentity")?;
        let (source_package_field, source_package): (
            &str,
            fn(String) -> DeployedSourcePackageIdentity,
        ) = match kind {
            DEPLOYED_RUNTIME_IDENTITY_KIND_V1 => (
                "sourcePackageSha256",
                DeployedSourcePackageIdentity::ArchiveSha256,
            ),
            DEPLOYED_RUNTIME_IDENTITY_KIND_V2 => (
                "sourcePackageRuntimeContentSha256",
                DeployedSourcePackageIdentity::RuntimeContentSha256,
            ),
            _ => {
                return Err(invalid_deployment_manifest(
                    "source deployedRuntimeIdentity kind is invalid",
                ));
            },
        };
        require_exact_keys(
            identity.keys().map(String::as_str),
            &["kind", "moduleSha256", source_package_field],
            "source deployedRuntimeIdentity",
        )?;
        let module_sha256 =
            require_string_field(identity, "moduleSha256", "source deployedRuntimeIdentity")?
                .to_owned();
        let source_package_sha256 = require_string_field(
            identity,
            source_package_field,
            "source deployedRuntimeIdentity",
        )?
        .to_owned();
        validate_deployment_sha256(
            "source.deployedRuntimeIdentity.moduleSha256",
            &module_sha256,
        )?;
        validate_deployment_sha256(
            &format!("source.deployedRuntimeIdentity.{source_package_field}"),
            &source_package_sha256,
        )?;
        return Ok(DeploymentRuntimeBinding::Bound(DeployedRuntimeIdentity {
            module_sha256,
            source_package: source_package(source_package_sha256),
        }));
    }

    let diagnostic = source
        .get(binding_field)
        .and_then(JsonValue::as_object)
        .ok_or_else(|| {
            invalid_deployment_manifest(
                "source deployedRuntimeIdentityDiagnostic must be an object",
            )
        })?;
    require_exact_keys(
        diagnostic.keys().map(String::as_str),
        &["kind", "reasons", "verdict"],
        "source deployedRuntimeIdentityDiagnostic",
    )?;
    if require_string_field(
        diagnostic,
        "kind",
        "source deployedRuntimeIdentityDiagnostic",
    )? != DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND
    {
        return Err(invalid_deployment_manifest(
            "source deployedRuntimeIdentityDiagnostic kind is invalid",
        ));
    }
    let verdict = match require_string_field(
        diagnostic,
        "verdict",
        "source deployedRuntimeIdentityDiagnostic",
    )? {
        "unprovable" => DeployedRuntimeIdentityVerdict::Unprovable,
        "mismatch" => DeployedRuntimeIdentityVerdict::Mismatch,
        _ => {
            return Err(invalid_deployment_manifest(
                "source deployedRuntimeIdentityDiagnostic verdict is invalid",
            ));
        },
    };
    let reasons = diagnostic
        .get("reasons")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            invalid_deployment_manifest(
                "source deployedRuntimeIdentityDiagnostic reasons must be an array",
            )
        })?;
    if reasons.is_empty() || reasons.len() > MAX_DEPLOYED_RUNTIME_IDENTITY_REASONS {
        return Err(invalid_deployment_manifest(format!(
            "source deployedRuntimeIdentityDiagnostic must contain between 1 and \
             {MAX_DEPLOYED_RUNTIME_IDENTITY_REASONS} reasons"
        )));
    }
    let mut parsed_reasons = Vec::with_capacity(reasons.len());
    for reason in reasons {
        let reason = reason.as_object().ok_or_else(|| {
            invalid_deployment_manifest(
                "source deployedRuntimeIdentityDiagnostic reasons entries must be objects",
            )
        })?;
        require_exact_keys(
            reason.keys().map(String::as_str),
            &["code", "detail"],
            "source deployedRuntimeIdentityDiagnostic reason",
        )?;
        let code = require_bounded_diagnostic_field(
            reason,
            "code",
            MAX_DEPLOYED_RUNTIME_IDENTITY_REASON_CODE_BYTES,
        )?;
        if !code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(invalid_deployment_manifest(
                "source deployedRuntimeIdentityDiagnostic reason code must contain only uppercase \
                 ASCII letters, digits, and underscores",
            ));
        }
        let detail = require_bounded_diagnostic_field(
            reason,
            "detail",
            MAX_DEPLOYED_RUNTIME_IDENTITY_REASON_DETAIL_BYTES,
        )?;
        parsed_reasons.push(DeployedRuntimeIdentityReason { code, detail });
    }
    Ok(DeploymentRuntimeBinding::Diagnostic(
        DeployedRuntimeIdentityDiagnostic {
            verdict,
            reasons: parsed_reasons,
        },
    ))
}

fn require_bounded_diagnostic_field(
    object: &serde_json::Map<String, JsonValue>,
    field: &str,
    maximum_bytes: usize,
) -> Result<String, WasmUdfPackageError> {
    let value = require_string_field(
        object,
        field,
        "source deployedRuntimeIdentityDiagnostic reason",
    )?;
    if value.trim().is_empty() || value.len() > maximum_bytes || value.chars().any(char::is_control)
    {
        return Err(invalid_deployment_manifest(format!(
            "source deployedRuntimeIdentityDiagnostic reason {field} must be nonempty, contain no \
             control characters, and use at most {maximum_bytes} bytes"
        )));
    }
    Ok(value.to_owned())
}

fn parse_udf_kind(value: &str) -> Result<UdfKind, WasmUdfPackageError> {
    match value {
        "query" => Ok(UdfKind::Query),
        "mutation" => Ok(UdfKind::Mutation),
        _ => Err(invalid_deployment_manifest("export UDF kind is invalid")),
    }
}

fn require_exact_keys<'a>(
    actual: impl Iterator<Item = &'a str>,
    expected: &[&str],
    description: &str,
) -> Result<(), WasmUdfPackageError> {
    if actual.collect::<BTreeSet<_>>() != expected.iter().copied().collect::<BTreeSet<_>>() {
        return Err(invalid_deployment_manifest(format!(
            "{description} has missing or unknown fields"
        )));
    }
    Ok(())
}

fn require_string_field<'a>(
    object: &'a serde_json::Map<String, JsonValue>,
    field: &str,
    description: &str,
) -> Result<&'a str, WasmUdfPackageError> {
    object
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            invalid_deployment_manifest(format!("{description} field {field} must be a string"))
        })
}

fn require_identity_field<'a>(
    object: &'a serde_json::Map<String, JsonValue>,
    field: &str,
    description: &str,
) -> Result<&'a str, WasmUdfPackageError> {
    let value = require_string_field(object, field, description)?;
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(invalid_deployment_manifest(format!(
            "{description} field {field} is invalid"
        )));
    }
    Ok(value)
}

fn require_runtime_module_path<'a>(
    object: &'a serde_json::Map<String, JsonValue>,
    field: &str,
    description: &str,
) -> Result<&'a str, WasmUdfPackageError> {
    let value = require_identity_field(object, field, description)?;
    if !value.ends_with(".js")
        || value.starts_with('/')
        || value.contains('\\')
        || value
            .split('/')
            .any(|component| matches!(component, "" | "." | ".."))
    {
        return Err(invalid_deployment_manifest(format!(
            "{description} field {field} is not a canonical runtime module path"
        )));
    }
    Ok(value)
}

fn require_canonical_module_path<'a>(
    object: &'a serde_json::Map<String, JsonValue>,
    field: &str,
    description: &str,
) -> Result<&'a str, WasmUdfPackageError> {
    let value = require_identity_field(object, field, description)?;
    if value.starts_with('/')
        || value.contains('\\')
        || value
            .split('/')
            .any(|component| matches!(component, "" | "." | ".."))
    {
        return Err(invalid_deployment_manifest(format!(
            "{description} field {field} is not a canonical module path"
        )));
    }
    Ok(value)
}

fn require_string_array_field<'a>(
    object: &'a serde_json::Map<String, JsonValue>,
    field: &str,
    description: &str,
) -> Result<BTreeSet<&'a str>, WasmUdfPackageError> {
    let values = object
        .get(field)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            invalid_deployment_manifest(format!("{description} field {field} must be an array"))
        })?;
    let mut unique_values = BTreeSet::new();
    for value in values {
        let value = value.as_str().ok_or_else(|| {
            invalid_deployment_manifest(format!(
                "{description} field {field} entries must be strings"
            ))
        })?;
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(invalid_deployment_manifest(format!(
                "{description} field {field} contains an invalid entry"
            )));
        }
        if !unique_values.insert(value) {
            return Err(invalid_deployment_manifest(format!(
                "{description} field {field} contains a duplicate entry"
            )));
        }
    }
    Ok(unique_values)
}

fn require_usize_field(
    object: &serde_json::Map<String, JsonValue>,
    field: &str,
    description: &str,
) -> Result<usize, WasmUdfPackageError> {
    object
        .get(field)
        .and_then(JsonValue::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            invalid_deployment_manifest(format!(
                "{description} field {field} must be a nonnegative integer"
            ))
        })
}

fn validate_visibility(value: &str) -> Result<(), WasmUdfPackageError> {
    if matches!(value, "public" | "internal") {
        Ok(())
    } else {
        Err(invalid_deployment_manifest(
            "function visibility is invalid",
        ))
    }
}

fn validate_deployment_sha256(field: &str, value: &str) -> Result<(), WasmUdfPackageError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_deployment_manifest(format!(
            "{field} must be exactly 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn validate_entry_selector_id(field: &str, value: &str) -> Result<(), WasmUdfPackageError> {
    if value.len() != 16
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_deployment_manifest(format!(
            "{field} must be exactly 16 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

pub(crate) fn load_runtime_registry_current(
    registry_root: &Path,
) -> Result<ValidatedRuntimeRegistryGenerationDescriptor, WasmUdfPackageError> {
    validate_runtime_registry_directory(registry_root)?;
    let (_, _, has_module_graph_cache, _) = validate_runtime_registry_root_entries(registry_root)?;
    validate_runtime_registry_directory(&registry_root.join("generations"))?;
    validate_runtime_registry_directory(&registry_root.join("packages"))?;
    if has_module_graph_cache {
        validate_runtime_registry_directory(&registry_root.join("module-graph-cache"))?;
    }

    let (_, value) = read_runtime_registry_json(
        &registry_root.join("current"),
        MAX_RUNTIME_REGISTRY_MANIFEST_BYTES,
        "current",
    )?;
    let current: RuntimeRegistryCurrentManifest = serde_json::from_value(value.clone())
        .map_err(WasmUdfPackageError::MalformedRuntimeRegistry)?;
    if current.kind != RUNTIME_REGISTRY_CURRENT_KIND {
        return Err(invalid_runtime_registry("current manifest kind is invalid"));
    }
    for (field, digest) in [
        ("current.currentSha256", current.current_sha256.as_str()),
        (
            "current.deploymentSha256",
            current.deployment_sha256.as_str(),
        ),
        (
            "current.generation.sha256",
            current.generation.sha256.as_str(),
        ),
        (
            "current.generationSha256",
            current.generation_sha256.as_str(),
        ),
    ] {
        validate_runtime_registry_sha256(field, digest)?;
    }
    validate_runtime_registry_size(
        "current.generation.size",
        current.generation.size,
        MAX_RUNTIME_REGISTRY_MANIFEST_BYTES,
    )?;
    if runtime_registry_identity(&value, "currentSha256")? != current.current_sha256 {
        return Err(invalid_runtime_registry(
            "currentSha256 does not authenticate the current manifest",
        ));
    }
    Ok(ValidatedRuntimeRegistryGenerationDescriptor {
        current_sha256: current.current_sha256,
        deployment_sha256: current.deployment_sha256,
        generation_file_sha256: current.generation.sha256,
        generation_file_size: current.generation.size,
        generation_sha256: current.generation_sha256,
        source_package_runtime_content_sha256: None,
    })
}

pub(crate) fn load_runtime_registry_source_catalog(
    registry_root: &Path,
) -> Result<ValidatedRuntimeRegistrySourceCatalog, WasmUdfPackageError> {
    validate_runtime_registry_directory(registry_root)?;
    let (_, _, has_module_graph_cache, has_source_catalog) =
        validate_runtime_registry_root_entries(registry_root)?;
    if !has_source_catalog {
        return Err(invalid_runtime_registry(
            "source catalog files are missing from the registry root",
        ));
    }
    validate_runtime_registry_directory(&registry_root.join("generations"))?;
    validate_runtime_registry_directory(&registry_root.join("packages"))?;
    if has_module_graph_cache {
        validate_runtime_registry_directory(&registry_root.join("module-graph-cache"))?;
    }

    let catalog_path = registry_root.join(RUNTIME_REGISTRY_SOURCE_CATALOG_FILE);
    let (catalog_bytes, value) = read_runtime_registry_json(
        &catalog_path,
        MAX_RUNTIME_REGISTRY_MANIFEST_BYTES,
        RUNTIME_REGISTRY_SOURCE_CATALOG_FILE,
    )?;
    let catalog: RuntimeRegistrySourceCatalogManifest = serde_json::from_value(value.clone())
        .map_err(WasmUdfPackageError::MalformedRuntimeRegistry)?;
    if !matches!(
        catalog.kind.as_str(),
        RUNTIME_REGISTRY_SOURCE_CATALOG_KIND_V1 | RUNTIME_REGISTRY_SOURCE_CATALOG_KIND_V2
    ) {
        return Err(invalid_runtime_registry(
            "source catalog manifest kind is invalid",
        ));
    }
    validate_runtime_registry_sha256("sourceCatalog.catalogSha256", &catalog.catalog_sha256)?;
    if runtime_registry_identity(&value, "catalogSha256")? != catalog.catalog_sha256 {
        return Err(invalid_runtime_registry(
            "catalogSha256 does not authenticate the source catalog manifest",
        ));
    }
    if catalog.entries.is_empty()
        || catalog.entries.len() > MAX_RUNTIME_REGISTRY_SOURCE_CATALOG_ENTRIES
    {
        return Err(invalid_runtime_registry(format!(
            "source catalog must contain between 1 and \
             {MAX_RUNTIME_REGISTRY_SOURCE_CATALOG_ENTRIES} entries"
        )));
    }

    let legacy_catalog = catalog.kind == RUNTIME_REGISTRY_SOURCE_CATALOG_KIND_V1;
    let mut previous_key: Option<(String, String, String, String)> = None;
    let mut generations = Vec::with_capacity(catalog.entries.len());
    for entry in catalog.entries {
        for (field, digest) in [
            (
                "sourceCatalog entry sourcePackageRuntimeContentSha256",
                entry.source_package_runtime_content_sha256.as_str(),
            ),
            (
                "sourceCatalog entry deploymentSha256",
                entry.deployment_sha256.as_str(),
            ),
            (
                "sourceCatalog entry generation.sha256",
                entry.generation.sha256.as_str(),
            ),
            (
                "sourceCatalog entry generationSha256",
                entry.generation_sha256.as_str(),
            ),
        ] {
            validate_runtime_registry_sha256(field, digest)?;
        }
        validate_runtime_registry_size(
            "sourceCatalog entry generation.size",
            entry.generation.size,
            MAX_RUNTIME_REGISTRY_MANIFEST_BYTES,
        )?;
        let key = (
            entry.source_package_runtime_content_sha256.clone(),
            entry.deployment_sha256.clone(),
            entry.generation.sha256.clone(),
            entry.generation_sha256.clone(),
        );
        if let Some(previous) = previous_key.replace(key.clone()) {
            let duplicate_legacy_source = legacy_catalog && previous.0 == key.0;
            if previous >= key || duplicate_legacy_source {
                return Err(invalid_runtime_registry(
                    "source catalog entries must be unique and sorted by exact source/generation \
                     pair",
                ));
            }
        }
        generations.push(ValidatedRuntimeRegistryGenerationDescriptor {
            current_sha256: catalog.catalog_sha256.clone(),
            deployment_sha256: entry.deployment_sha256,
            generation_file_sha256: entry.generation.sha256,
            generation_file_size: entry.generation.size,
            generation_sha256: entry.generation_sha256,
            source_package_runtime_content_sha256: Some(
                entry.source_package_runtime_content_sha256,
            ),
        });
    }

    if legacy_catalog {
        let complete = read_runtime_registry_file(
            &registry_root.join(RUNTIME_REGISTRY_SOURCE_CATALOG_COMPLETE_FILE),
            MAX_COMPLETE_MARKER_BYTES,
            "source catalog completion marker",
        )?;
        if complete != format!("{}\n", catalog.catalog_sha256).as_bytes() {
            return Err(invalid_runtime_registry(
                "source catalog completion marker is invalid",
            ));
        }
    }
    let (confirmed_catalog_bytes, _) = read_runtime_registry_json(
        &catalog_path,
        MAX_RUNTIME_REGISTRY_MANIFEST_BYTES,
        RUNTIME_REGISTRY_SOURCE_CATALOG_FILE,
    )?;
    if confirmed_catalog_bytes != catalog_bytes {
        return Err(invalid_runtime_registry(
            "source catalog changed while it was being authenticated",
        ));
    }

    Ok(ValidatedRuntimeRegistrySourceCatalog {
        catalog_sha256: catalog.catalog_sha256,
        generations,
    })
}

pub(crate) fn load_runtime_registry_generation(
    registry_root: &Path,
    current: &ValidatedRuntimeRegistryGenerationDescriptor,
) -> Result<ValidatedRuntimeRegistryGeneration, WasmUdfPackageError> {
    load_runtime_registry_generation_with_retained_graphs(registry_root, current, &[])
}

pub(crate) fn load_runtime_registry_generation_with_retained_graphs(
    registry_root: &Path,
    current: &ValidatedRuntimeRegistryGenerationDescriptor,
    retained_catalogs: &[&AuthenticatedModuleGraphCatalog],
) -> Result<ValidatedRuntimeRegistryGeneration, WasmUdfPackageError> {
    let deployment_root = registry_root
        .join("generations")
        .join(&current.deployment_sha256);
    validate_runtime_registry_directory(&deployment_root)?;
    let keyed_generation_root = deployment_root.join(&current.generation_sha256);
    let (generation_root, keyed_generation) = match fs::symlink_metadata(&keyed_generation_root) {
        Ok(_) => (keyed_generation_root, true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (deployment_root, false),
        Err(error) => return Err(io_error(&keyed_generation_root, error)),
    };
    validate_runtime_registry_directory(&generation_root)?;
    validate_runtime_registry_directory_entries(
        &generation_root,
        &["COMPLETE", "deployment.json", "generation.json"],
        "generation",
    )?;

    let generation_path = generation_root.join("generation.json");
    let (generation_bytes, generation_value) = read_runtime_registry_json(
        &generation_path,
        MAX_RUNTIME_REGISTRY_MANIFEST_BYTES,
        "generation.json",
    )?;
    validate_runtime_registry_artifact_bytes(
        "generation.json",
        &generation_bytes,
        &RuntimeRegistryArtifact {
            sha256: current.generation_file_sha256.clone(),
            size: current.generation_file_size,
        },
    )?;
    let generation_kind = generation_value.get("kind").and_then(JsonValue::as_str);
    if !keyed_generation
        && matches!(
            generation_kind,
            Some(
                RUNTIME_REGISTRY_GENERATION_KIND_V5
                    | RUNTIME_REGISTRY_GENERATION_KIND_V6_SHADOW_ONLY
                    | RUNTIME_REGISTRY_GENERATION_KIND_V9
                    | RUNTIME_REGISTRY_GENERATION_KIND_V10_SHADOW_ONLY
            )
        )
    {
        return Err(invalid_runtime_registry(
            "module-graph generations require immutable generation-keyed storage",
        ));
    }
    let generation = match generation_kind {
        Some(RUNTIME_REGISTRY_GENERATION_KIND_V4) => ParsedRuntimeRegistryGeneration::V4(
            parse_generation_v4(generation_value.clone())
                .map_err(|error| invalid_runtime_registry(error.to_string()))?,
        ),
        Some(RUNTIME_REGISTRY_GENERATION_KIND_V5) => ParsedRuntimeRegistryGeneration::V5(
            parse_generation_v5(generation_value.clone())
                .map_err(|error| invalid_runtime_registry(error.to_string()))?,
        ),
        Some(RUNTIME_REGISTRY_GENERATION_KIND_V6_SHADOW_ONLY) => {
            ParsedRuntimeRegistryGeneration::V6ShadowOnly(
                parse_generation_v6(generation_value.clone())
                    .map_err(|error| invalid_runtime_registry(error.to_string()))?,
            )
        },
        Some(RUNTIME_REGISTRY_GENERATION_KIND_V9) => ParsedRuntimeRegistryGeneration::V9(
            parse_generation_v9(generation_value.clone())
                .map_err(|error| invalid_runtime_registry(error.to_string()))?,
        ),
        Some(RUNTIME_REGISTRY_GENERATION_KIND_V10_SHADOW_ONLY) => {
            ParsedRuntimeRegistryGeneration::V10ShadowOnly(
                parse_generation_v10(generation_value.clone())
                    .map_err(|error| invalid_runtime_registry(error.to_string()))?,
            )
        },
        _ => ParsedRuntimeRegistryGeneration::Legacy(
            serde_json::from_value(generation_value.clone())
                .map_err(WasmUdfPackageError::MalformedRuntimeRegistry)?,
        ),
    };
    validate_runtime_registry_sha256(
        "generation.generationSha256",
        generation.generation_sha256(),
    )?;
    if runtime_registry_identity(&generation_value, "generationSha256")?
        != generation.generation_sha256()
    {
        return Err(invalid_runtime_registry(
            "generationSha256 does not authenticate the generation manifest",
        ));
    }
    if generation.generation_sha256() != current.generation_sha256 {
        return Err(invalid_runtime_registry(
            "current generationSha256 differs from generation.json",
        ));
    }
    let deployment = generation.deployment_manifest();
    for (field, digest) in [
        (
            "generation.deploymentManifest.deploymentSha256",
            deployment.deployment_sha256,
        ),
        ("generation.deploymentManifest.sha256", deployment.sha256),
    ] {
        validate_runtime_registry_sha256(field, digest)?;
    }
    validate_runtime_registry_size(
        "generation.deploymentManifest.size",
        deployment.size,
        MAX_DEPLOYMENT_MANIFEST_BYTES,
    )?;
    if deployment.deployment_sha256 != current.deployment_sha256 {
        return Err(invalid_runtime_registry(
            "current deploymentSha256 differs from generation.json",
        ));
    }

    let complete = read_runtime_registry_file(
        &generation_root.join("COMPLETE"),
        MAX_COMPLETE_MARKER_BYTES,
        "generation COMPLETE",
    )?;
    if complete != format!("{}\n", generation.generation_sha256()).as_bytes() {
        return Err(invalid_runtime_registry(
            "generation completion marker is invalid",
        ));
    }

    let deployment_path = generation_root.join("deployment.json");
    let (deployment_bytes, _) = read_runtime_registry_json(
        &deployment_path,
        MAX_DEPLOYMENT_MANIFEST_BYTES,
        "deployment.json",
    )?;
    validate_runtime_registry_artifact_bytes(
        "deployment.json",
        &deployment_bytes,
        &RuntimeRegistryArtifact {
            sha256: deployment.sha256.to_owned(),
            size: deployment.size,
        },
    )?;
    let registry = ValidatedDeploymentManifest::parse(&deployment_bytes)?;
    if registry.deployment_sha256() != deployment.deployment_sha256 {
        return Err(invalid_runtime_registry(
            "deployment manifest identity differs from generation.json",
        ));
    }

    let (packages_root, generation_package_keys, module_graph_catalog) = match &generation {
        ParsedRuntimeRegistryGeneration::Legacy(RuntimeRegistryGenerationManifest::V1 {
            packages,
            ..
        }) => {
            if registry.version != DeploymentManifestVersion::V2 {
                return Err(invalid_runtime_registry(
                    "generation-v1 requires a deployment-v2 singleton package set",
                ));
            }
            if packages.is_empty() {
                return Err(invalid_runtime_registry(
                    "generation packages must not be empty",
                ));
            }
            let packages_root = registry_root.join("packages");
            let mut previous_package_key = None;
            for package in packages {
                validate_runtime_registry_sha256("generation packageKey", &package.package_key)?;
                if previous_package_key
                    .replace(package.package_key.as_str())
                    .is_some_and(|previous| previous >= package.package_key.as_str())
                {
                    return Err(invalid_runtime_registry(
                        "generation packages must be unique and sorted by packageKey",
                    ));
                }
                validate_runtime_registry_package(
                    &packages_root,
                    &package.package_key,
                    &package.files,
                    &PACKAGE_FILES,
                )?;
            }
            let keys = packages
                .iter()
                .map(|package| package.package_key.as_str())
                .collect::<BTreeSet<_>>();
            (packages_root, keys, None)
        },
        ParsedRuntimeRegistryGeneration::Legacy(RuntimeRegistryGenerationManifest::V2 {
            cohort_packages,
            ..
        }) => {
            if !matches!(
                registry.version,
                DeploymentManifestVersion::V3 | DeploymentManifestVersion::V4
            ) {
                return Err(invalid_runtime_registry(
                    "generation-v2 requires a cohort deployment manifest",
                ));
            }
            let packages_root = registry_root.join("cohort-packages");
            validate_runtime_registry_directory(&packages_root)?;
            let mut previous_package_id = None;
            for package in cohort_packages {
                validate_runtime_registry_sha256(
                    "generation cohortPackageId",
                    &package.cohort_package_id,
                )?;
                if previous_package_id
                    .replace(package.cohort_package_id.as_str())
                    .is_some_and(|previous| previous >= package.cohort_package_id.as_str())
                {
                    return Err(invalid_runtime_registry(
                        "generation cohortPackages must be unique and sorted by cohortPackageId",
                    ));
                }
                validate_runtime_registry_package(
                    &packages_root,
                    &package.cohort_package_id,
                    &package.files,
                    &COHORT_PACKAGE_FILES,
                )?;
            }
            let keys = cohort_packages
                .iter()
                .map(|package| package.cohort_package_id.as_str())
                .collect::<BTreeSet<_>>();
            (packages_root, keys, None)
        },
        ParsedRuntimeRegistryGeneration::Legacy(RuntimeRegistryGenerationManifest::V3 {
            capability_entry_packages,
            ..
        }) => {
            if registry.version != DeploymentManifestVersion::V5 {
                return Err(invalid_runtime_registry(
                    "generation-v3 requires a capability-entry deployment manifest",
                ));
            }
            if capability_entry_packages.is_empty() {
                return Err(invalid_runtime_registry(
                    "generation capabilityEntryPackages must not be empty",
                ));
            }
            let packages_root = registry_root.join("capability-entry-packages");
            validate_runtime_registry_directory(&packages_root)?;
            let mut previous_package_key = None;
            for package in capability_entry_packages {
                validate_runtime_registry_sha256(
                    "generation capability-entry packageKey",
                    &package.package_key,
                )?;
                if previous_package_key
                    .replace(package.package_key.as_str())
                    .is_some_and(|previous| previous >= package.package_key.as_str())
                {
                    return Err(invalid_runtime_registry(
                        "generation capabilityEntryPackages must be unique and sorted by \
                         packageKey",
                    ));
                }
                validate_runtime_registry_package(
                    &packages_root,
                    &package.package_key,
                    &package.files,
                    &CAPABILITY_ENTRY_PACKAGE_FILES,
                )?;
            }
            let keys = capability_entry_packages
                .iter()
                .map(|package| package.package_key.as_str())
                .collect::<BTreeSet<_>>();
            (packages_root, keys, None)
        },
        ParsedRuntimeRegistryGeneration::V4(generation_v4) => {
            if registry.version != DeploymentManifestVersion::V5 {
                return Err(invalid_runtime_registry(
                    "generation-v4 requires a capability-entry deployment manifest",
                ));
            }
            if generation_v4.capability_entry_packages().is_empty() {
                return Err(invalid_runtime_registry(
                    "generation capabilityEntryPackages must not be empty",
                ));
            }
            let packages_root = registry_root.join("capability-entry-packages");
            validate_runtime_registry_directory(&packages_root)?;
            let mut previous_package_key = None;
            for package in generation_v4.capability_entry_packages() {
                validate_runtime_registry_sha256(
                    "generation capability-entry packageKey",
                    package.package_key(),
                )?;
                if previous_package_key
                    .replace(package.package_key())
                    .is_some_and(|previous| previous >= package.package_key())
                {
                    return Err(invalid_runtime_registry(
                        "generation capabilityEntryPackages must be unique and sorted by \
                         packageKey",
                    ));
                }
                validate_runtime_registry_package_v4(
                    &packages_root,
                    package.package_key(),
                    package.files(),
                )?;
            }
            let keys = generation_v4
                .capability_entry_packages()
                .iter()
                .map(|package| package.package_key())
                .collect::<BTreeSet<_>>();
            let route_ids = registry.capability_entry_route_ids();
            let catalog = authenticate_module_graph_catalog_v4(
                registry_root,
                retained_catalogs,
                generation_v4,
                &route_ids,
            )
            .map_err(|error| invalid_runtime_registry(error.to_string()))?;
            (packages_root, keys, Some(catalog))
        },
        ParsedRuntimeRegistryGeneration::V5(generation_v5) => {
            validate_module_graph_runtime_registry_root(registry_root)?;
            if registry.version != DeploymentManifestVersion::V6 {
                return Err(invalid_runtime_registry(
                    "generation-v5 requires a deployment-v6 manifest",
                ));
            }
            let packages_root = registry_root.join("packages");
            let deployment_binding = registry.module_graph_binding.as_ref().ok_or_else(|| {
                invalid_runtime_registry("deployment-v6 module graph binding is missing")
            })?;
            let deployment_routes = registry.module_graph_routes();
            let catalog = authenticate_module_graph_catalog_v5(
                registry_root,
                retained_catalogs,
                generation_v5,
                deployment_binding,
                &registry.module_graph_cohorts,
                &deployment_routes,
            )
            .map_err(|error| invalid_runtime_registry(error.to_string()))?;
            (packages_root, BTreeSet::new(), Some(catalog))
        },
        ParsedRuntimeRegistryGeneration::V6ShadowOnly(generation_v6) => {
            validate_module_graph_runtime_registry_root(registry_root)?;
            if registry.version != DeploymentManifestVersion::V6 {
                return Err(invalid_runtime_registry(
                    "generation-v6-shadow-only requires a deployment-v6 manifest",
                ));
            }
            let packages_root = registry_root.join("packages");
            let deployment_binding = registry.module_graph_binding.as_ref().ok_or_else(|| {
                invalid_runtime_registry("deployment-v6 module graph binding is missing")
            })?;
            let deployment_routes = registry.module_graph_routes();
            let catalog = authenticate_module_graph_catalog_v6(
                registry_root,
                retained_catalogs,
                generation_v6,
                deployment_binding,
                &registry.module_graph_cohorts,
                &deployment_routes,
            )
            .map_err(|error| invalid_runtime_registry(error.to_string()))?;
            (packages_root, BTreeSet::new(), Some(catalog))
        },
        ParsedRuntimeRegistryGeneration::V9(generation_v9) => {
            validate_module_graph_runtime_registry_root(registry_root)?;
            if registry.version != DeploymentManifestVersion::V8 {
                return Err(invalid_runtime_registry(
                    "generation-v9 requires a deployment-v8 manifest",
                ));
            }
            let packages_root = registry_root.join("packages");
            let deployment_binding = registry.module_graph_binding.as_ref().ok_or_else(|| {
                invalid_runtime_registry("deployment-v8 module graph binding is missing")
            })?;
            let deployment_routes = registry.module_graph_routes();
            let catalog = authenticate_module_graph_catalog_v9(
                registry_root,
                retained_catalogs,
                generation_v9,
                deployment_binding,
                &registry.module_graph_cohorts,
                &deployment_routes,
            )
            .map_err(|error| invalid_runtime_registry(error.to_string()))?;
            (packages_root, BTreeSet::new(), Some(catalog))
        },
        ParsedRuntimeRegistryGeneration::V10ShadowOnly(generation_v10) => {
            validate_module_graph_runtime_registry_root(registry_root)?;
            if registry.version != DeploymentManifestVersion::V8 {
                return Err(invalid_runtime_registry(
                    "generation-v10-shadow-only requires a deployment-v8 manifest",
                ));
            }
            let packages_root = registry_root.join("packages");
            let deployment_binding = registry.module_graph_binding.as_ref().ok_or_else(|| {
                invalid_runtime_registry("deployment-v8 module graph binding is missing")
            })?;
            let deployment_routes = registry.module_graph_routes();
            let catalog = authenticate_module_graph_catalog_v10(
                registry_root,
                retained_catalogs,
                generation_v10,
                deployment_binding,
                &registry.module_graph_cohorts,
                &deployment_routes,
            )
            .map_err(|error| invalid_runtime_registry(error.to_string()))?;
            (packages_root, BTreeSet::new(), Some(catalog))
        },
    };
    if registry.package_keys() != generation_package_keys {
        return Err(invalid_runtime_registry(
            "generation packages differ from the deployment Wasm package set",
        ));
    }
    if let Some(expected_source) = &current.source_package_runtime_content_sha256 {
        let actual_source = registry.deployed_source_package_runtime_content_sha256()?;
        if actual_source != expected_source {
            return Err(invalid_runtime_registry(
                "source catalog entry differs from the generation source package",
            ));
        }
    }

    Ok(ValidatedRuntimeRegistryGeneration {
        descriptor: current.clone(),
        admission: generation.admission(),
        current_sha256: current.current_sha256.clone(),
        generation_manifest_sha256: current.generation_file_sha256.clone(),
        generation_sha256: generation.generation_sha256().to_owned(),
        packages_root,
        registry,
        module_graph_catalog,
    })
}

fn validate_runtime_registry_package(
    packages_root: &Path,
    package_key: &str,
    files: &[RuntimeRegistryPackageFile],
    expected_files: &[&str],
) -> Result<(), WasmUdfPackageError> {
    if files.len() != expected_files.len()
        || files
            .iter()
            .map(|file| file.name.as_str())
            .ne(expected_files.iter().copied())
    {
        return Err(invalid_runtime_registry(
            "generation package files differ from the required ordered package file set",
        ));
    }
    let package_root = packages_root.join(package_key);
    validate_runtime_registry_directory(&package_root)?;
    validate_runtime_registry_directory_entries(&package_root, expected_files, "package")?;
    for file in files {
        validate_runtime_registry_sha256("generation package file sha256", &file.sha256)?;
        let maximum_bytes = match file.name.as_str() {
            "COMPLETE" => 128,
            "build-provenance.json" if expected_files == PACKAGE_FILES => {
                MAX_BUILD_PROVENANCE_BYTES
            },
            "build-provenance.json" => MAX_COHORT_PROVENANCE_BYTES,
            "cohort-manifest.json" => MAX_COHORT_MANIFEST_BYTES,
            "entry-manifest.json" => MAX_COHORT_MANIFEST_BYTES,
            "execution-manifest.json" => MAX_EXECUTION_MANIFEST_BYTES,
            "module.cwasm" => MAX_SERIALIZED_MODULE_BYTES,
            "module.wasm" => MAX_CORE_WASM_BYTES,
            "package-entry.json" => MAX_EXECUTION_MANIFEST_BYTES,
            _ => unreachable!("validated package file set changed"),
        };
        validate_runtime_registry_size("generation package file size", file.size, maximum_bytes)?;
        validate_runtime_registry_artifact_file(
            &package_root.join(&file.name),
            maximum_bytes,
            "package file",
            &RuntimeRegistryArtifact {
                sha256: file.sha256.clone(),
                size: file.size,
            },
        )?;
    }
    Ok(())
}

fn validate_runtime_registry_package_v4(
    packages_root: &Path,
    package_key: &str,
    files: &[super::module_graph_registry::FileRecord],
) -> Result<(), WasmUdfPackageError> {
    let expected = CAPABILITY_ENTRY_PACKAGE_FILES;
    if files.len() != expected.len()
        || files
            .iter()
            .map(|file| file.name())
            .ne(expected.iter().copied())
    {
        return Err(invalid_runtime_registry(
            "generation capability-entry package files differ from the exact file set",
        ));
    }
    let package_root = packages_root.join(package_key);
    validate_runtime_registry_directory(&package_root)?;
    validate_runtime_registry_directory_entries(&package_root, &expected, "package")?;
    for file in files {
        validate_runtime_registry_sha256("generation package file sha256", file.sha256())?;
        let maximum_bytes = match file.name() {
            "COMPLETE" => 128,
            "build-provenance.json" => MAX_COHORT_PROVENANCE_BYTES,
            "entry-manifest.json" => MAX_COHORT_MANIFEST_BYTES,
            "module.cwasm" => MAX_SERIALIZED_MODULE_BYTES,
            "module.wasm" => MAX_CORE_WASM_BYTES,
            "package-entry.json" => MAX_EXECUTION_MANIFEST_BYTES,
            _ => unreachable!("validated capability-entry package file name changed"),
        };
        validate_runtime_registry_size("generation package file size", file.size(), maximum_bytes)?;
        validate_runtime_registry_artifact_file(
            &package_root.join(file.name()),
            maximum_bytes,
            "package file",
            &RuntimeRegistryArtifact {
                sha256: file.sha256().to_owned(),
                size: file.size(),
            },
        )?;
    }
    Ok(())
}

fn read_runtime_registry_json(
    path: &Path,
    maximum_bytes: u64,
    name: &'static str,
) -> Result<(Vec<u8>, JsonValue), WasmUdfPackageError> {
    let bytes = read_runtime_registry_file(path, maximum_bytes, name)?;
    let payload = json_payload(&bytes);
    if bytes.len() != payload.len() + 1 || !bytes.ends_with(b"\n") {
        return Err(invalid_runtime_registry(format!(
            "{name} must contain canonical JSON followed by one newline"
        )));
    }
    let value: JsonValue =
        serde_json::from_slice(payload).map_err(WasmUdfPackageError::MalformedRuntimeRegistry)?;
    let mut canonical = Vec::with_capacity(payload.len());
    write_canonical_json(&value, &mut canonical)
        .expect("writing canonical runtime registry JSON to a byte vector cannot fail");
    if canonical != payload {
        return Err(invalid_runtime_registry(format!(
            "{name} is not canonical JSON"
        )));
    }
    Ok((bytes, value))
}

fn read_runtime_registry_file(
    path: &Path,
    maximum_bytes: u64,
    name: &str,
) -> Result<Vec<u8>, WasmUdfPackageError> {
    let file = open_runtime_registry_file(path)?;
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    if metadata.len() == 0 || metadata.len() > maximum_bytes {
        return Err(invalid_runtime_registry(format!(
            "{name} size must be between 1 and {maximum_bytes} bytes"
        )));
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| {
        invalid_runtime_registry(format!("{name} size exceeds the host address space"))
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(maximum_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| io_error(path, source))?;
    if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > maximum_bytes {
        return Err(invalid_runtime_registry(format!(
            "{name} changed while it was read"
        )));
    }
    Ok(bytes)
}

fn runtime_registry_identity(
    value: &JsonValue,
    identity_field: &str,
) -> Result<String, WasmUdfPackageError> {
    let mut without_identity = value.clone();
    let object = without_identity
        .as_object_mut()
        .ok_or_else(|| invalid_runtime_registry("registry manifest must be an object"))?;
    if object.remove(identity_field).is_none() {
        return Err(invalid_runtime_registry(format!(
            "registry manifest is missing {identity_field}"
        )));
    }
    let mut canonical = Vec::new();
    write_canonical_json(&without_identity, &mut canonical)
        .expect("writing runtime registry identity to a byte vector cannot fail");
    Ok(sha256(&canonical))
}

fn validate_runtime_registry_artifact_bytes(
    name: &str,
    bytes: &[u8],
    expected: &RuntimeRegistryArtifact,
) -> Result<(), WasmUdfPackageError> {
    if bytes.len() as u64 != expected.size || sha256(bytes) != expected.sha256 {
        return Err(invalid_runtime_registry(format!(
            "{name} differs from its authenticated size or SHA-256"
        )));
    }
    Ok(())
}

fn validate_runtime_registry_artifact_file(
    path: &Path,
    maximum_bytes: u64,
    name: &str,
    expected: &RuntimeRegistryArtifact,
) -> Result<(), WasmUdfPackageError> {
    let file = open_runtime_registry_file(path)?;
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    validate_runtime_registry_size(name, metadata.len(), maximum_bytes)?;
    if metadata.len() != expected.size {
        return Err(invalid_runtime_registry(format!(
            "{name} differs from its authenticated size"
        )));
    }
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    std::io::copy(&mut reader, &mut digest).map_err(|source| io_error(path, source))?;
    if format!("{:x}", digest.finalize()) != expected.sha256 {
        return Err(invalid_runtime_registry(format!(
            "{name} differs from its authenticated SHA-256"
        )));
    }
    Ok(())
}

fn validate_runtime_registry_size(
    field: &str,
    size: u64,
    maximum_bytes: u64,
) -> Result<(), WasmUdfPackageError> {
    if size == 0 || size > maximum_bytes {
        return Err(invalid_runtime_registry(format!(
            "{field} must be between 1 and {maximum_bytes}"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn runtime_registry_core_wasm_size_limit_is_320_mib() {
    validate_runtime_registry_size("module.wasm", MAX_CORE_WASM_BYTES, MAX_CORE_WASM_BYTES)
        .expect("320 MiB Core Wasm artifact was rejected");
    assert!(validate_runtime_registry_size(
        "module.wasm",
        MAX_CORE_WASM_BYTES + 1,
        MAX_CORE_WASM_BYTES,
    )
    .is_err());
}

fn validate_runtime_registry_sha256(field: &str, digest: &str) -> Result<(), WasmUdfPackageError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_runtime_registry(format!(
            "{field} must be exactly 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

/// Only an existing, private, empty directory represents an unpublished
/// registry. Missing paths and partial or malformed publications are not empty
/// registries.
pub(crate) fn runtime_registry_is_empty(path: &Path) -> Result<bool, WasmUdfPackageError> {
    validate_runtime_registry_directory(path)?;
    let first = fs::read_dir(path)
        .map_err(|source| io_error(path, source))?
        .next()
        .transpose()
        .map_err(|source| io_error(path, source))?;
    Ok(first.is_none())
}

fn validate_runtime_registry_directory(path: &Path) -> Result<(), WasmUdfPackageError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| io_error(path, source))?;
    if !metadata.file_type().is_dir() || !has_exact_permissions(&metadata, 0o700) {
        return Err(invalid_runtime_registry(format!(
            "{} is not a private registry directory",
            path.display()
        )));
    }
    Ok(())
}

fn validate_runtime_registry_directory_entries(
    path: &Path,
    expected: &[&str],
    name: &str,
) -> Result<(), WasmUdfPackageError> {
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(path).map_err(|source| io_error(path, source))? {
        let entry = entry.map_err(|source| io_error(path, source))?;
        let entry_name = entry.file_name().into_string().map_err(|entry_name| {
            invalid_runtime_registry(format!(
                "{name} contains a non-UTF-8 entry {}",
                entry_name.to_string_lossy()
            ))
        })?;
        actual.insert(entry_name);
    }
    if actual.iter().map(String::as_str).collect::<BTreeSet<_>>() != expected {
        return Err(invalid_runtime_registry(format!(
            "{name} directory has missing or unexpected entries"
        )));
    }
    Ok(())
}

fn validate_runtime_registry_root_entries(
    path: &Path,
) -> Result<(bool, bool, bool, bool), WasmUdfPackageError> {
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(path).map_err(|source| io_error(path, source))? {
        let entry = entry.map_err(|source| io_error(path, source))?;
        let entry_name = entry.file_name().into_string().map_err(|entry_name| {
            invalid_runtime_registry(format!(
                "root contains a non-UTF-8 entry {}",
                entry_name.to_string_lossy()
            ))
        })?;
        actual.insert(entry_name);
    }
    let old_root = BTreeSet::from([
        "current".to_owned(),
        "generations".to_owned(),
        "packages".to_owned(),
    ]);
    let new_root = BTreeSet::from([
        "cohort-packages".to_owned(),
        "current".to_owned(),
        "generations".to_owned(),
        "packages".to_owned(),
    ]);
    let capability_root = BTreeSet::from([
        "capability-entry-packages".to_owned(),
        "current".to_owned(),
        "generations".to_owned(),
        "packages".to_owned(),
    ]);
    let module_graph_root = BTreeSet::from([
        "capability-entry-packages".to_owned(),
        "current".to_owned(),
        "generations".to_owned(),
        "module-graph-cache".to_owned(),
        "packages".to_owned(),
    ]);
    let graph_only_root = BTreeSet::from([
        "current".to_owned(),
        "generations".to_owned(),
        "module-graph-cache".to_owned(),
        "packages".to_owned(),
    ]);
    let has_source_catalog = actual.contains(RUNTIME_REGISTRY_SOURCE_CATALOG_FILE);
    let has_completion_marker = actual.contains(RUNTIME_REGISTRY_SOURCE_CATALOG_COMPLETE_FILE);
    if has_completion_marker && !has_source_catalog {
        return Err(invalid_runtime_registry(
            "source catalog completion marker exists without a catalog",
        ));
    }
    let without_source_catalog = actual
        .into_iter()
        .filter(|name| {
            name != RUNTIME_REGISTRY_SOURCE_CATALOG_FILE
                && name != RUNTIME_REGISTRY_SOURCE_CATALOG_COMPLETE_FILE
        })
        .collect::<BTreeSet<_>>();
    if without_source_catalog == old_root {
        Ok((false, false, false, has_source_catalog))
    } else if without_source_catalog == new_root {
        Ok((true, false, false, has_source_catalog))
    } else if without_source_catalog == capability_root {
        Ok((false, true, false, has_source_catalog))
    } else if without_source_catalog == module_graph_root {
        Ok((false, true, true, has_source_catalog))
    } else if without_source_catalog == graph_only_root {
        Ok((false, false, true, has_source_catalog))
    } else {
        Err(invalid_runtime_registry(
            "root directory has missing or unexpected entries",
        ))
    }
}

fn validate_module_graph_runtime_registry_root(path: &Path) -> Result<(), WasmUdfPackageError> {
    let (has_cohort_packages, has_capability_entry_packages, has_module_graph_cache, _) =
        validate_runtime_registry_root_entries(path)?;
    if (
        has_cohort_packages,
        has_capability_entry_packages,
        has_module_graph_cache,
    ) != (false, false, true)
    {
        return Err(invalid_runtime_registry(
            "module-graph generations require the graph-only registry root layout",
        ));
    }
    Ok(())
}

fn open_runtime_registry_file(path: &Path) -> Result<File, WasmUdfPackageError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|source| io_error(path, source))?;
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    if !metadata.file_type().is_file() || !has_exact_permissions(&metadata, 0o600) {
        return Err(invalid_runtime_registry(format!(
            "{} is not a private registry file",
            path.display()
        )));
    }
    Ok(file)
}

#[cfg(unix)]
fn has_exact_permissions(metadata: &fs::Metadata, expected: u32) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o7777 == expected
}

#[cfg(not(unix))]
fn has_exact_permissions(_metadata: &fs::Metadata, _expected: u32) -> bool {
    true
}

fn invalid_runtime_registry(message: impl Into<String>) -> WasmUdfPackageError {
    WasmUdfPackageError::InvalidRuntimeRegistry(message.into())
}

pub(crate) fn load_validated_package(
    package_path: &Path,
    runtime: &RuntimeCompatibility<'_>,
    serialized_module_snapshot_directory: &Path,
) -> Result<ValidatedWasmUdfPackage, WasmUdfPackageError> {
    load_validated_package_with_precompiler(
        package_path,
        runtime,
        None,
        serialized_module_snapshot_directory,
    )
}

fn load_validated_package_with_precompiler(
    package_path: &Path,
    runtime: &RuntimeCompatibility<'_>,
    expected_precompiler: Option<&PrecompilerMaterialIdentity>,
    serialized_module_snapshot_directory: &Path,
) -> Result<ValidatedWasmUdfPackage, WasmUdfPackageError> {
    validate_directory(package_path)?;
    validate_directory_entries(package_path)?;

    let complete = read_bounded_file(&package_path.join("COMPLETE"), MAX_COMPLETE_MARKER_BYTES)?;
    let package_entry_bytes = read_bounded_file(
        &package_path.join("package-entry.json"),
        MAX_PACKAGE_ENTRY_BYTES,
    )?;
    let manifest_bytes = read_bounded_file(
        &package_path.join("execution-manifest.json"),
        MAX_EXECUTION_MANIFEST_BYTES,
    )?;
    let provenance_bytes = read_bounded_file(
        &package_path.join("build-provenance.json"),
        MAX_BUILD_PROVENANCE_BYTES,
    )?;

    require_canonical_json("package-entry.json", &package_entry_bytes)?;
    require_canonical_json("execution-manifest.json", &manifest_bytes)?;
    let package_entry: PackageEntry = serde_json::from_slice(json_payload(&package_entry_bytes))
        .map_err(WasmUdfPackageError::MalformedPackageEntry)?;
    validate_sha256("package-entry.key", &package_entry.key)?;
    validate_sha256(
        "package-entry.manifestSha256",
        &package_entry.manifest_sha256,
    )?;
    validate_sha256(
        "package-entry.provenance.sha256",
        &package_entry.provenance.sha256,
    )?;
    if package_entry.kind != PACKAGE_ENTRY_KIND {
        return Err(invalid_entry(format!(
            "unsupported package kind {}",
            package_entry.kind
        )));
    }
    if complete != format!("{}\n", package_entry.key).as_bytes() {
        return Err(WasmUdfPackageError::InvalidCompletionMarker);
    }
    let directory_key = package_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_entry("package directory name is not valid UTF-8"))?;
    if directory_key != package_entry.key {
        return Err(invalid_entry(
            "package directory name does not match its key",
        ));
    }

    let manifest_sha256 = sha256(json_payload(&manifest_bytes));
    if package_entry.key != manifest_sha256 || package_entry.manifest_sha256 != manifest_sha256 {
        return Err(invalid_entry(
            "manifest digest does not match the package key",
        ));
    }
    let manifest =
        WasmUdfExecutionManifest::parse_for_runtime(json_payload(&manifest_bytes), runtime)?;
    if !manifest.routing().is_wasm() {
        return Err(WasmUdfPackageError::V8FallbackPackage);
    }
    let manifest_artifact = manifest
        .artifact()
        .ok_or_else(|| invalid_entry("Wasm routing did not identify an artifact"))?;
    validate_artifact_identity(
        "build-provenance.json",
        &package_entry.provenance,
        manifest_artifact
            .provenance_bytes()
            .ok_or_else(|| invalid_entry("singleton artifact omitted provenance size"))?,
        manifest_artifact
            .provenance_sha256()
            .ok_or_else(|| invalid_entry("singleton artifact omitted provenance digest"))?
            .as_str(),
        MAX_BUILD_PROVENANCE_BYTES,
    )?;
    validate_artifact_bytes(
        "build-provenance.json",
        &provenance_bytes,
        &package_entry.provenance,
    )?;
    require_canonical_json("build-provenance.json", &provenance_bytes)?;
    validate_build_provenance(
        json_payload(&provenance_bytes),
        manifest.compiler(),
        manifest_artifact,
        expected_precompiler,
    )?;
    if package_entry
        .artifacts
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != BTreeSet::from(["module.cwasm", "module.wasm"])
    {
        return Err(invalid_entry(
            "artifact metadata must contain exactly module.cwasm and module.wasm",
        ));
    }

    let core_wasm = package_entry
        .artifacts
        .get("module.wasm")
        .expect("validated package artifact disappeared");
    validate_artifact_identity(
        "module.wasm",
        core_wasm,
        manifest_artifact.core_wasm_bytes(),
        manifest_artifact.core_wasm_sha256().as_str(),
        MAX_CORE_WASM_BYTES,
    )?;
    let serialized_module = package_entry
        .artifacts
        .get("module.cwasm")
        .expect("validated package artifact disappeared");
    validate_artifact_identity(
        "module.cwasm",
        serialized_module,
        manifest_artifact.serialized_module_bytes(),
        manifest_artifact.serialized_module_sha256().as_str(),
        MAX_SERIALIZED_MODULE_BYTES,
    )?;

    validate_artifact_file(&package_path.join("module.wasm"), "module.wasm", core_wasm)?;
    validate_artifact_file(
        &package_path.join("build-provenance.json"),
        "build-provenance.json",
        &package_entry.provenance,
    )?;
    let serialized_module_snapshot = snapshot_validated_serialized_module_file(
        &package_path.join("module.cwasm"),
        "module.cwasm",
        serialized_module,
        serialized_module_snapshot_directory,
    )?;
    validate_directory_entries(package_path)?;
    if read_bounded_file(&package_path.join("COMPLETE"), MAX_COMPLETE_MARKER_BYTES)? != complete {
        return Err(WasmUdfPackageError::InvalidCompletionMarker);
    }

    let permitted_conditional_convex_imports = permitted_conditional_convex_imports(
        manifest.value_mode(),
        manifest.effect_execution_mode(),
        manifest.imported_operations(),
    );
    Ok(ValidatedWasmUdfPackage {
        execution: WasmUdfExecutionPolicy::from_legacy(&manifest),
        identity: ValidatedWasmUdfPackageIdentity::Legacy(manifest),
        package_key: package_entry.key,
        serialized_module_snapshot,
        entry_selector: None,
        permitted_conditional_convex_imports,
        core_wasm_bytes: core_wasm.size,
        serialized_module_bytes: serialized_module.size,
        graph: None,
    })
}

#[cfg(test)]
mod legacy_capability_graph_v1 {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    fn load_validated_capability_graph_package(
        package_path: &Path,
        package_entry_bytes: &[u8],
        expected_manifest_sha256: &str,
        expected_platform_limits: &JsonValue,
        expected_entry_id: &str,
        expected_route_id: &str,
        expected_entry_selector_id: &str,
        expected_entry_path: &str,
        expected_runtime_module_path: &str,
        expected_export_name: &str,
        expected_udf_kind: UdfKind,
        expected_visibility: &str,
        runtime: &RuntimeCompatibility<'_>,
        expected_precompiler: &PrecompilerMaterialIdentity,
        serialized_module_snapshot_directory: &Path,
    ) -> Result<ValidatedWasmUdfPackage, WasmUdfPackageError> {
        let complete =
            read_bounded_file(&package_path.join("COMPLETE"), MAX_COMPLETE_MARKER_BYTES)?;
        let manifest_bytes = read_bounded_file(
            &package_path.join("entry-manifest.json"),
            MAX_COHORT_MANIFEST_BYTES,
        )?;
        let provenance_bytes = read_bounded_file(
            &package_path.join("build-provenance.json"),
            MAX_COHORT_PROVENANCE_BYTES,
        )?;
        let graph_manifest_bytes = read_bounded_file(
            &package_path.join("graph-manifest.json"),
            MAX_CAPABILITY_GRAPH_MANIFEST_BYTES,
        )?;
        for (name, bytes) in [
            ("entry-manifest.json", manifest_bytes.as_slice()),
            ("build-provenance.json", provenance_bytes.as_slice()),
            ("graph-manifest.json", graph_manifest_bytes.as_slice()),
        ] {
            require_canonical_json(name, bytes)?;
        }

        let package_entry: CapabilityGraphPackageEntry =
            serde_json::from_slice(json_payload(package_entry_bytes))
                .map_err(WasmUdfPackageError::MalformedPackageEntry)?;
        if package_entry.kind != CAPABILITY_GRAPH_PACKAGE_KIND {
            return Err(invalid_entry("unsupported capability graph package kind"));
        }
        for (field, value) in [
            ("package-entry.key", package_entry.key.as_str()),
            (
                "package-entry.manifestSha256",
                package_entry.manifest_sha256.as_str(),
            ),
            (
                "package-entry.provenance.sha256",
                package_entry.provenance.sha256.as_str(),
            ),
            (
                "package-entry.graphManifest.sha256",
                package_entry.graph_manifest.sha256.as_str(),
            ),
        ] {
            validate_sha256(field, value)?;
        }
        let manifest_sha256 = sha256(json_payload(&manifest_bytes));
        if package_entry.key != manifest_sha256
            || package_entry.manifest_sha256 != manifest_sha256
            || manifest_sha256 != expected_manifest_sha256
            || complete != format!("{}\n", package_entry.key).as_bytes()
            || package_path.file_name().and_then(|name| name.to_str())
                != Some(package_entry.key.as_str())
        {
            return Err(invalid_entry(
                "capability graph package path, completion marker, and entry-manifest identity \
                 disagree",
            ));
        }
        validate_artifact_bytes(
            "graph-manifest.json",
            &graph_manifest_bytes,
            &package_entry.graph_manifest,
        )?;
        validate_artifact_bytes(
            "build-provenance.json",
            &provenance_bytes,
            &package_entry.provenance,
        )?;

        let graph_manifest_value: JsonValue =
            serde_json::from_slice(json_payload(&graph_manifest_bytes))
                .map_err(WasmUdfPackageError::MalformedPackageEntry)?;
        let graph_manifest: CapabilityGraphManifest =
            serde_json::from_value(graph_manifest_value.clone()).map_err(|error| {
                invalid_entry(format!(
                    "capability graph manifest has missing, unknown, or invalid fields: {error}"
                ))
            })?;
        validate_capability_graph_manifest_identity(
            &graph_manifest,
            &graph_manifest_value,
            &package_entry,
            &manifest_bytes,
            &provenance_bytes,
            runtime,
            expected_precompiler,
        )?;

        let legacy_package_entry = CohortPackageEntry {
            artifacts: BTreeMap::from([
                (
                    "module.cwasm".to_owned(),
                    package_entry
                        .artifacts
                        .get("module.cwasm")
                        .cloned()
                        .expect("validated graph leaf AOT artifact disappeared"),
                ),
                (
                    "module.wasm".to_owned(),
                    package_entry
                        .artifacts
                        .get("module.wasm")
                        .cloned()
                        .expect("validated graph leaf Core-Wasm artifact disappeared"),
                ),
            ]),
            key: package_entry.key.clone(),
            kind: CAPABILITY_ENTRY_PACKAGE_KIND.to_owned(),
            manifest_sha256: package_entry.manifest_sha256.clone(),
            provenance: package_entry.provenance.clone(),
        };
        let manifest: CapabilityEntryManifest =
            serde_json::from_slice(json_payload(&manifest_bytes)).map_err(|error| {
                invalid_entry(format!(
                    "capability-entry manifest has missing, unknown, or invalid fields: {error}"
                ))
            })?;
        validate_capability_graph_leaf_engine(&graph_manifest.engine, &manifest.engine)?;
        let validated_identity = validate_capability_entry_manifest(
            &manifest,
            &legacy_package_entry,
            expected_platform_limits,
            expected_entry_id,
            expected_route_id,
            expected_entry_selector_id,
            expected_entry_path,
            expected_runtime_module_path,
            expected_export_name,
            expected_udf_kind,
            expected_visibility,
            runtime,
            expected_precompiler,
        )?;
        let provenance: CapabilityEntryBuildProvenance =
            serde_json::from_slice(json_payload(&provenance_bytes)).map_err(|error| {
                invalid_provenance(format!(
                    "capability-entry provenance has missing, unknown, or invalid fields: {error}"
                ))
            })?;
        validate_capability_entry_provenance(
            &provenance,
            &manifest,
            &validated_identity.entries,
            &validated_identity.routes,
        )?;
        let official_output_chunk_entry_slots =
            official_output_chunk_entry_slots(&provenance, &validated_identity.entries);

        let base = load_validated_capability_graph_dependency(
            package_path,
            &graph_manifest.base,
            serialized_module_snapshot_directory,
        )?;
        let mut shared = Vec::with_capacity(graph_manifest.shared.len());
        for module in &graph_manifest.shared {
            shared.push(load_validated_capability_graph_dependency(
                package_path,
                module,
                serialized_module_snapshot_directory,
            )?);
        }
        validate_capability_graph_core_artifact(package_path, &graph_manifest.leaf)?;
        let leaf_serialized =
            graph_artifact_package_identity(&graph_manifest.leaf.artifacts.serialized_module);
        let serialized_module_snapshot = snapshot_validated_serialized_module_file(
            &package_path.join(&graph_manifest.leaf.artifacts.serialized_module.path),
            "graph leaf AOT module",
            &leaf_serialized,
            serialized_module_snapshot_directory,
        )?;

        let mut expected_files = package_entry
            .artifacts
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        expected_files.extend([
            "COMPLETE",
            "build-provenance.json",
            "entry-manifest.json",
            "graph-manifest.json",
            "package-entry.json",
        ]);
        validate_directory_entries_dynamic(package_path, &expected_files)?;
        if read_bounded_file(&package_path.join("COMPLETE"), MAX_COMPLETE_MARKER_BYTES)? != complete
        {
            return Err(WasmUdfPackageError::InvalidCompletionMarker);
        }

        let selector =
            u64::from_str_radix(&validated_identity.selected_route.entry_selector_id, 16)
                .map_err(|_| invalid_entry("capability graph route selector is invalid"))?;
        let permitted_conditional_convex_imports = permitted_conditional_convex_imports(
            manifest.execution.value_mode(),
            manifest.execution.effect_execution_mode(),
            manifest.execution.imported_operations(),
        );
        let leaf = validated_capability_graph_module(&graph_manifest.leaf);
        let leaf_core_wasm_bytes = leaf.core_wasm_bytes;
        let leaf_serialized_module_bytes = leaf.serialized_module_bytes;
        Ok(ValidatedWasmUdfPackage {
            execution: manifest.execution,
            identity: ValidatedWasmUdfPackageIdentity::CapabilityEntry(
                CapabilityEntryRuntimeIdentity {
                    entries: validated_identity.entries,
                    manifest_schema_version: manifest.schema_version,
                    artifact_pipeline_sha256: manifest.pipeline.artifact_pipeline_sha256,
                    compiler: manifest.compiler,
                    official_output_chunk_entry_slots,
                    opaque_value_abi_version: manifest.abi.opaque_value_abi_version,
                    routes: validated_identity.routes,
                },
            ),
            package_key: package_entry.key,
            serialized_module_snapshot,
            entry_selector: Some(selector),
            permitted_conditional_convex_imports,
            core_wasm_bytes: leaf_core_wasm_bytes,
            serialized_module_bytes: leaf_serialized_module_bytes,
            graph: Some(ValidatedCapabilityGraph {
                graph_sha256: graph_manifest.graph_sha256,
                engine_compatibility_sha256: graph_manifest.engine.compatibility_sha256,
                base,
                shared,
                leaf: ValidatedCapabilityGraphLeaf { module: leaf },
                layout: graph_manifest.layout,
            }),
        })
    }

    fn load_validated_capability_graph_dependency(
        package_path: &Path,
        module: &CapabilityGraphModule,
        serialized_module_snapshot_directory: &Path,
    ) -> Result<ValidatedCapabilityGraphDependency, WasmUdfPackageError> {
        validate_capability_graph_core_artifact(package_path, module)?;
        let serialized = graph_artifact_package_identity(&module.artifacts.serialized_module);
        let serialized_module_snapshot = snapshot_validated_serialized_module_file(
            &package_path.join(&module.artifacts.serialized_module.path),
            "graph dependency AOT module",
            &serialized,
            serialized_module_snapshot_directory,
        )?;
        Ok(ValidatedCapabilityGraphDependency {
            module: validated_capability_graph_module(module),
            serialized_module_snapshot,
        })
    }

    fn validate_capability_graph_core_artifact(
        package_path: &Path,
        module: &CapabilityGraphModule,
    ) -> Result<(), WasmUdfPackageError> {
        let core_wasm = graph_artifact_package_identity(&module.artifacts.core_wasm);
        validate_artifact_file(
            &package_path.join(&module.artifacts.core_wasm.path),
            "graph module Core-Wasm",
            &core_wasm,
        )
    }

    fn graph_artifact_package_identity(artifact: &CapabilityGraphArtifact) -> PackageArtifact {
        PackageArtifact {
            sha256: artifact.sha256.clone(),
            size: artifact.size,
        }
    }

    fn validated_capability_graph_module(
        module: &CapabilityGraphModule,
    ) -> ValidatedCapabilityGraphModule {
        ValidatedCapabilityGraphModule {
            module_id: module.module_id.clone(),
            provider_namespace: module.provider_namespace.clone(),
            core_wasm_sha256: module.artifacts.core_wasm.sha256.clone(),
            core_wasm_bytes: module.artifacts.core_wasm.size,
            serialized_module_sha256: module.artifacts.serialized_module.sha256.clone(),
            serialized_module_bytes: module.artifacts.serialized_module.size,
            contract: module.contract.clone(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_capability_graph_manifest_identity(
        manifest: &CapabilityGraphManifest,
        manifest_value: &JsonValue,
        package_entry: &CapabilityGraphPackageEntry,
        entry_manifest_bytes: &[u8],
        provenance_bytes: &[u8],
        runtime: &RuntimeCompatibility<'_>,
        expected_precompiler: &PrecompilerMaterialIdentity,
    ) -> Result<(), WasmUdfPackageError> {
        validate_capability_graph_manifest_schema(&manifest.kind, manifest.schema_version)?;
        validate_sha256("graph.graphSha256", &manifest.graph_sha256)?;
        if capability_graph_manifest_sha256(manifest_value)? != manifest.graph_sha256 {
            return Err(invalid_entry("capability graph manifest digest is invalid"));
        }
        if manifest.entry_manifest
            != (PackageArtifact {
                sha256: sha256(entry_manifest_bytes),
                size: entry_manifest_bytes.len() as u64,
            })
            || manifest.provenance
                != (PackageArtifact {
                    sha256: sha256(provenance_bytes),
                    size: provenance_bytes.len() as u64,
                })
            || manifest.provenance != package_entry.provenance
        {
            return Err(invalid_entry(
                "capability graph metadata artifacts differ from their authenticated files",
            ));
        }
        validate_capability_graph_engine(&manifest.engine, runtime, expected_precompiler)?;
        if manifest.shared.len() > MAX_CAPABILITY_GRAPH_SHARED_MODULES {
            return Err(invalid_entry(
                "capability graph contains too many shared modules",
            ));
        }

        let mut ordered_modules = Vec::with_capacity(manifest.shared.len() + 2);
        ordered_modules.push(&manifest.base);
        ordered_modules.extend(manifest.shared.iter());
        ordered_modules.push(&manifest.leaf);
        let mut module_ids = BTreeSet::new();
        let mut provider_namespaces = BTreeSet::new();
        let mut artifact_paths = BTreeSet::new();
        let mut available_exports =
            BTreeMap::<String, (String, BTreeMap<String, CapabilityGraphExternType>)>::new();
        for (index, module) in ordered_modules.iter().enumerate() {
            let expected_role = if index == 0 {
                CapabilityGraphModuleRole::Base
            } else if index + 1 == ordered_modules.len() {
                CapabilityGraphModuleRole::Leaf
            } else {
                CapabilityGraphModuleRole::Shared
            };
            if module.role != expected_role {
                return Err(invalid_entry(
                    "capability graph module role differs from its ordered position",
                ));
            }
            validate_capability_graph_identifier("moduleId", &module.module_id)?;
            validate_capability_graph_identifier("providerNamespace", &module.provider_namespace)?;
            if matches!(
                module.provider_namespace.as_str(),
                "convex" | "env" | "wasi_snapshot_preview1"
            ) || !module_ids.insert(module.module_id.clone())
                || !provider_namespaces.insert(module.provider_namespace.clone())
            {
                return Err(invalid_entry(
                    "capability graph module identities or provider namespaces are duplicated or \
                     reserved",
                ));
            }
            validate_capability_graph_module_artifacts(
                module,
                index,
                ordered_modules.len(),
                package_entry,
                &mut artifact_paths,
            )?;
            let exports = validate_capability_graph_module_contract(
                module,
                &available_exports,
                index + 1 == ordered_modules.len(),
            )?;
            available_exports.insert(
                module.module_id.clone(),
                (module.provider_namespace.clone(), exports),
            );
        }
        if package_entry
            .artifacts
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != artifact_paths
        {
            return Err(invalid_entry(
                "capability graph package artifacts differ from the graph module set",
            ));
        }
        validate_capability_graph_layout(manifest, &available_exports)?;
        Ok(())
    }

    pub(super) fn validate_capability_graph_manifest_schema(
        kind: &str,
        schema_version: u32,
    ) -> Result<(), WasmUdfPackageError> {
        if kind != CAPABILITY_GRAPH_MANIFEST_KIND
            || schema_version != CAPABILITY_GRAPH_MANIFEST_SCHEMA_VERSION
        {
            return Err(invalid_entry(
                "unsupported capability graph manifest kind or schema",
            ));
        }
        Ok(())
    }

    pub(super) fn capability_graph_manifest_sha256(
        manifest_value: &JsonValue,
    ) -> Result<String, WasmUdfPackageError> {
        let mut identity_value = manifest_value.clone();
        identity_value
            .as_object_mut()
            .and_then(|value| value.remove("graphSha256"))
            .ok_or_else(|| invalid_entry("capability graph manifest identity field is missing"))?;
        let mut identity_bytes = Vec::new();
        write_canonical_json(&identity_value, &mut identity_bytes)
            .expect("writing capability graph identity cannot fail");
        Ok(sha256(&identity_bytes))
    }

    fn validate_capability_graph_engine(
        engine: &CohortEngineIdentity,
        runtime: &RuntimeCompatibility<'_>,
        expected_precompiler: &PrecompilerMaterialIdentity,
    ) -> Result<(), WasmUdfPackageError> {
        for (field, value) in [
            (
                "graph.engine.compatibilitySha256",
                engine.compatibility_sha256.as_str(),
            ),
            (
                "graph.engine.configurationSha256",
                engine.configuration_sha256.as_str(),
            ),
        ] {
            validate_sha256(field, value)?;
        }
        if engine.package != *expected_precompiler
            || engine.revision != runtime.wasmtime_revision
            || engine.target.triple != runtime.target_triple
            || engine.target.cpu != runtime.target_cpu
            || engine.compatibility_sha256 != runtime.engine_compatibility_sha256
            || engine.configuration_sha256 != runtime.engine_configuration_sha256
            || !engine.target.platform.is_object()
        {
            return Err(invalid_entry(
                "capability graph engine identity is incompatible with the runtime",
            ));
        }
        let mut config_bytes = Vec::new();
        write_canonical_json(
            &serde_json::to_value(&engine.config).expect("graph engine config serializes"),
            &mut config_bytes,
        )
        .expect("writing graph engine config cannot fail");
        if sha256(&config_bytes) != engine.configuration_sha256 {
            return Err(invalid_entry(
                "capability graph engine configuration digest is invalid",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_capability_graph_leaf_engine(
        graph: &CohortEngineIdentity,
        leaf: &CohortEngineIdentity,
    ) -> Result<(), WasmUdfPackageError> {
        if graph != leaf {
            return Err(invalid_entry(
                "capability graph engine identity differs from its leaf entry manifest",
            ));
        }
        Ok(())
    }

    fn validate_capability_graph_module_artifacts(
        module: &CapabilityGraphModule,
        index: usize,
        module_count: usize,
        package_entry: &CapabilityGraphPackageEntry,
        artifact_paths: &mut BTreeSet<String>,
    ) -> Result<(), WasmUdfPackageError> {
        let (expected_core_wasm_path, expected_serialized_module_path) = if index == 0 {
            ("base.wasm".to_owned(), "base.cwasm".to_owned())
        } else if index + 1 == module_count {
            ("module.wasm".to_owned(), "module.cwasm".to_owned())
        } else {
            let shared_index = index - 1;
            (
                format!("shared-{shared_index:04}.wasm"),
                format!("shared-{shared_index:04}.cwasm"),
            )
        };
        for (description, artifact, expected_path, maximum_bytes) in [
            (
                "Core-Wasm",
                &module.artifacts.core_wasm,
                expected_core_wasm_path,
                MAX_CORE_WASM_BYTES,
            ),
            (
                "AOT",
                &module.artifacts.serialized_module,
                expected_serialized_module_path,
                MAX_SERIALIZED_MODULE_BYTES,
            ),
        ] {
            if artifact.path != expected_path
                || artifact.size == 0
                || artifact.size > maximum_bytes
                || !artifact_paths.insert(artifact.path.clone())
            {
                return Err(invalid_entry(format!(
                    "capability graph {description} artifact path or size is invalid"
                )));
            }
            validate_sha256("graph module artifact sha256", &artifact.sha256)?;
            if package_entry.artifacts.get(&artifact.path)
                != Some(&graph_artifact_package_identity(artifact))
            {
                return Err(invalid_entry(format!(
                    "capability graph {description} artifact differs from package metadata"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn validate_capability_graph_module_contract(
        module: &CapabilityGraphModule,
        available_exports: &BTreeMap<String, (String, BTreeMap<String, CapabilityGraphExternType>)>,
        is_leaf: bool,
    ) -> Result<BTreeMap<String, CapabilityGraphExternType>, WasmUdfPackageError> {
        let mut previous_import: Option<(&str, &str)> = None;
        let mut imports = BTreeSet::new();
        for import in &module.contract.imports {
            validate_capability_graph_identifier("import module", &import.module)?;
            validate_capability_graph_identifier("import name", &import.name)?;
            validate_capability_graph_extern_type(&import.ty)?;
            let key = (import.module.as_str(), import.name.as_str());
            if previous_import.is_some_and(|previous| previous >= key) || !imports.insert(key) {
                return Err(invalid_entry(
                    "capability graph module imports are unsorted or duplicated",
                ));
            }
            previous_import = Some(key);
            match &import.provider {
                CapabilityGraphImportProvider::Host => {
                    if !matches!(
                        import.module.as_str(),
                        "convex" | "env" | "wasi_snapshot_preview1"
                    ) {
                        return Err(invalid_entry(
                            "capability graph host import uses an unsupported provider namespace",
                        ));
                    }
                },
                CapabilityGraphImportProvider::Module { module_id } => {
                    let (provider_namespace, exports) =
                        available_exports.get(module_id).ok_or_else(|| {
                            invalid_entry(
                                "capability graph import provider is missing or appears after its \
                                 consumer",
                            )
                        })?;
                    if provider_namespace != &import.module
                        || exports.get(&import.name) != Some(&import.ty)
                    {
                        return Err(invalid_entry(
                            "capability graph import provider or exact type differs from its \
                             export",
                        ));
                    }
                },
            }
        }
        let mut previous_export = None;
        let mut exports = BTreeMap::new();
        for export in &module.contract.exports {
            validate_capability_graph_identifier("export name", &export.name)?;
            validate_capability_graph_extern_type(&export.ty)?;
            if previous_export
                .replace(export.name.as_str())
                .is_some_and(|previous| previous >= export.name.as_str())
                || exports
                    .insert(export.name.clone(), export.ty.clone())
                    .is_some()
            {
                return Err(invalid_entry(
                    "capability graph module exports are unsorted or duplicated",
                ));
            }
        }
        if is_leaf {
            for name in [
                "_initialize",
                "convex_wasm_udf_destroy_runtime",
                "convex_wasm_udf_run",
                "convex_wasm_select_entry",
                "convex_wasm_udf_prepare_selected_entry",
            ] {
                if !exports.contains_key(name) {
                    return Err(invalid_entry(
                        "capability graph leaf contract omits a required runtime export",
                    ));
                }
            }
        }
        Ok(exports)
    }

    fn validate_capability_graph_extern_type(
        ty: &CapabilityGraphExternType,
    ) -> Result<(), WasmUdfPackageError> {
        match ty {
            CapabilityGraphExternType::Function { .. }
            | CapabilityGraphExternType::Global { .. }
            | CapabilityGraphExternType::Tag { .. } => {},
            CapabilityGraphExternType::Memory {
                maximum_pages,
                memory64,
                minimum_pages,
                page_size_bytes,
                shared,
            } => {
                if !matches!(page_size_bytes, 1 | 65_536)
                    || maximum_pages.is_some_and(|maximum| maximum < *minimum_pages)
                    || (*shared && maximum_pages.is_none())
                    || (!*memory64 && *minimum_pages > u32::MAX.into())
                {
                    return Err(invalid_entry("capability graph memory contract is invalid"));
                }
            },
            CapabilityGraphExternType::Table {
                element,
                maximum_elements,
                minimum_elements,
                table64,
            } => {
                if !matches!(
                    element,
                    CapabilityGraphValueType::FuncRef
                        | CapabilityGraphValueType::ExternRef
                        | CapabilityGraphValueType::ExnRef
                ) || maximum_elements.is_some_and(|maximum| maximum < *minimum_elements)
                    || (!*table64 && *minimum_elements > u32::MAX.into())
                {
                    return Err(invalid_entry("capability graph table contract is invalid"));
                }
            },
        }
        Ok(())
    }

    fn validate_capability_graph_layout(
        manifest: &CapabilityGraphManifest,
        exports: &BTreeMap<String, (String, BTreeMap<String, CapabilityGraphExternType>)>,
    ) -> Result<(), WasmUdfPackageError> {
        let memory = capability_graph_layout_export(&manifest.layout.memory, exports)?;
        let CapabilityGraphExternType::Memory {
            minimum_pages,
            page_size_bytes,
            ..
        } = memory
        else {
            return Err(invalid_entry(
                "capability graph shared memory layout does not reference a memory export",
            ));
        };
        if !matches!(
            capability_graph_layout_export(&manifest.layout.table, exports)?,
            CapabilityGraphExternType::Table { .. }
        ) {
            return Err(invalid_entry(
                "capability graph shared table layout does not reference a table export",
            ));
        }
        let stack_pointer =
            capability_graph_layout_export(&manifest.layout.stack.pointer, exports)?;
        if !matches!(
            stack_pointer,
            CapabilityGraphExternType::Global {
                mutable: true,
                value: CapabilityGraphValueType::I32 | CapabilityGraphValueType::I64,
            }
        ) {
            return Err(invalid_entry(
                "capability graph stack pointer layout does not reference a mutable integer global",
            ));
        }
        let stack = &manifest.layout.stack;
        let memory_bytes = minimum_pages
            .checked_mul(*page_size_bytes)
            .ok_or_else(|| invalid_entry("capability graph shared memory byte size overflows"))?;
        if stack.alignment_bytes == 0
            || !stack.alignment_bytes.is_power_of_two()
            || stack.lower_bound_bytes >= stack.upper_bound_bytes
            || stack.upper_bound_bytes > memory_bytes
            || !(stack.lower_bound_bytes..=stack.upper_bound_bytes).contains(&stack.initial_pointer)
            || stack.lower_bound_bytes % stack.alignment_bytes != 0
            || stack.upper_bound_bytes % stack.alignment_bytes != 0
            || stack.initial_pointer % stack.alignment_bytes != 0
        {
            return Err(invalid_entry(
                "capability graph stack layout bounds or alignment are invalid",
            ));
        }
        let mut previous_tag = None;
        let mut layout_tags = BTreeSet::new();
        for tag in &manifest.layout.tags {
            if previous_tag
                .as_ref()
                .is_some_and(|previous| previous >= tag)
                || !layout_tags.insert(tag.clone())
                || !matches!(
                    capability_graph_layout_export(tag, exports)?,
                    CapabilityGraphExternType::Tag { .. }
                )
            {
                return Err(invalid_entry(
                    "capability graph tag layout is unsorted, duplicated, or not a tag",
                ));
            }
            previous_tag = Some(tag.clone());
        }

        let mut imported_memory = false;
        let mut imported_table = false;
        let mut imported_tags = BTreeSet::new();
        for module in std::iter::once(&manifest.base)
            .chain(manifest.shared.iter())
            .chain(std::iter::once(&manifest.leaf))
        {
            for import in &module.contract.imports {
                let CapabilityGraphImportProvider::Module { module_id } = &import.provider else {
                    continue;
                };
                let reference = CapabilityGraphExportReference {
                    export_name: import.name.clone(),
                    module_id: module_id.clone(),
                };
                match &import.ty {
                    CapabilityGraphExternType::Memory { .. } => {
                        if reference != manifest.layout.memory {
                            return Err(invalid_entry(
                                "capability graph module imports an undeclared shared memory",
                            ));
                        }
                        imported_memory = true;
                    },
                    CapabilityGraphExternType::Table { .. } => {
                        if reference != manifest.layout.table {
                            return Err(invalid_entry(
                                "capability graph module imports an undeclared shared table",
                            ));
                        }
                        imported_table = true;
                    },
                    CapabilityGraphExternType::Tag { .. } => {
                        imported_tags.insert(reference);
                    },
                    CapabilityGraphExternType::Function { .. }
                    | CapabilityGraphExternType::Global { .. } => {},
                }
            }
        }
        if !imported_memory
            || !imported_table
            || imported_tags != layout_tags
            || manifest.layout.memory.module_id == manifest.leaf.module_id
            || manifest.layout.table.module_id == manifest.leaf.module_id
            || manifest.layout.stack.pointer.module_id == manifest.leaf.module_id
            || manifest
                .layout
                .tags
                .iter()
                .any(|tag| tag.module_id == manifest.leaf.module_id)
        {
            return Err(invalid_entry(
                "capability graph shared layout does not exactly cover its dependency imports",
            ));
        }
        Ok(())
    }

    fn capability_graph_layout_export<'a>(
        reference: &CapabilityGraphExportReference,
        exports: &'a BTreeMap<String, (String, BTreeMap<String, CapabilityGraphExternType>)>,
    ) -> Result<&'a CapabilityGraphExternType, WasmUdfPackageError> {
        exports
            .get(&reference.module_id)
            .and_then(|(_, exports)| exports.get(&reference.export_name))
            .ok_or_else(|| invalid_entry("capability graph layout references a missing export"))
    }

    fn validate_capability_graph_identifier(
        field: &str,
        value: &str,
    ) -> Result<(), WasmUdfPackageError> {
        if value.is_empty()
            || value.len() > 256
            || value == "."
            || value == ".."
            || value.chars().any(char::is_control)
        {
            return Err(invalid_entry(format!(
                "capability graph {field} is invalid"
            )));
        }
        Ok(())
    }
}

fn load_validated_cohort_package(
    package_path: &Path,
    execution_manifest: &JsonValue,
    expected_entry_id: &str,
    expected_entry_selector_id: &str,
    runtime: &RuntimeCompatibility<'_>,
    expected_precompiler: &PrecompilerMaterialIdentity,
    serialized_module_snapshot_directory: &Path,
) -> Result<ValidatedWasmUdfPackage, WasmUdfPackageError> {
    validate_directory(package_path)?;
    validate_directory_entries_exact(package_path, &COHORT_PACKAGE_FILES)?;

    let complete = read_bounded_file(&package_path.join("COMPLETE"), MAX_COMPLETE_MARKER_BYTES)?;
    let package_entry_bytes = read_bounded_file(
        &package_path.join("package-entry.json"),
        MAX_PACKAGE_ENTRY_BYTES,
    )?;
    let cohort_manifest_bytes = read_bounded_file(
        &package_path.join("cohort-manifest.json"),
        MAX_COHORT_MANIFEST_BYTES,
    )?;
    let provenance_bytes = read_bounded_file(
        &package_path.join("build-provenance.json"),
        MAX_COHORT_PROVENANCE_BYTES,
    )?;
    for (name, bytes) in [
        ("package-entry.json", package_entry_bytes.as_slice()),
        ("cohort-manifest.json", cohort_manifest_bytes.as_slice()),
        ("build-provenance.json", provenance_bytes.as_slice()),
    ] {
        require_canonical_json(name, bytes)?;
    }

    let package_entry: CohortPackageEntry =
        serde_json::from_slice(json_payload(&package_entry_bytes))
            .map_err(WasmUdfPackageError::MalformedPackageEntry)?;
    if package_entry.kind != COHORT_PACKAGE_ENTRY_KIND {
        return Err(invalid_entry("unsupported cohort package kind"));
    }
    for (field, value) in [
        ("package-entry.key", package_entry.key.as_str()),
        (
            "package-entry.manifestSha256",
            package_entry.manifest_sha256.as_str(),
        ),
        (
            "package-entry.provenance.sha256",
            package_entry.provenance.sha256.as_str(),
        ),
    ] {
        validate_sha256(field, value)?;
    }
    if package_entry.provenance.size == 0 {
        return Err(invalid_entry("cohort provenance size must be positive"));
    }
    if package_entry
        .artifacts
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != BTreeSet::from(["module.cwasm", "module.wasm"])
    {
        return Err(invalid_entry(
            "cohort artifact metadata must contain exactly module.cwasm and module.wasm",
        ));
    }
    if complete != format!("{}\n", package_entry.key).as_bytes() {
        return Err(WasmUdfPackageError::InvalidCompletionMarker);
    }
    let directory_key = package_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_entry("cohort package directory name is not valid UTF-8"))?;
    if directory_key != package_entry.key {
        return Err(invalid_entry(
            "cohort package directory name does not match its key",
        ));
    }
    let cohort_manifest_sha256 = sha256(json_payload(&cohort_manifest_bytes));
    if cohort_manifest_sha256 != package_entry.key
        || cohort_manifest_sha256 != package_entry.manifest_sha256
    {
        return Err(invalid_entry(
            "cohort manifest digest does not match the package key",
        ));
    }

    let cohort_manifest: CohortManifest =
        serde_json::from_slice(json_payload(&cohort_manifest_bytes)).map_err(|error| {
            invalid_entry(format!(
                "cohort manifest has missing, unknown, or invalid fields: {error}"
            ))
        })?;
    let permitted_conditional_convex_imports = validate_cohort_manifest_identity(
        &cohort_manifest,
        &package_entry,
        execution_manifest,
        expected_entry_id,
        expected_entry_selector_id,
        runtime,
        expected_precompiler,
    )?;

    validate_artifact_bytes(
        "build-provenance.json",
        &provenance_bytes,
        &package_entry.provenance,
    )?;
    let provenance: CohortBuildProvenance = serde_json::from_slice(json_payload(&provenance_bytes))
        .map_err(|error| {
            invalid_provenance(format!(
                "cohort provenance has missing, unknown, or invalid fields: {error}"
            ))
        })?;
    validate_cohort_provenance(&provenance, &cohort_manifest)?;

    let core_wasm = package_entry
        .artifacts
        .get("module.wasm")
        .expect("validated cohort package artifact disappeared");
    let serialized_module = package_entry
        .artifacts
        .get("module.cwasm")
        .expect("validated cohort package artifact disappeared");
    validate_artifact_file(&package_path.join("module.wasm"), "module.wasm", core_wasm)?;
    validate_artifact_file(
        &package_path.join("build-provenance.json"),
        "build-provenance.json",
        &package_entry.provenance,
    )?;
    let serialized_module_snapshot = snapshot_validated_serialized_module_file(
        &package_path.join("module.cwasm"),
        "module.cwasm",
        serialized_module,
        serialized_module_snapshot_directory,
    )?;

    let mut execution_manifest_bytes = Vec::new();
    write_canonical_json(execution_manifest, &mut execution_manifest_bytes)
        .expect("writing canonical execution manifest cannot fail");
    let manifest = WasmUdfExecutionManifest::parse_for_runtime(&execution_manifest_bytes, runtime)?;
    if !manifest.routing().is_wasm() {
        return Err(WasmUdfPackageError::V8FallbackPackage);
    }
    let selector = u64::from_str_radix(expected_entry_selector_id, 16)
        .map_err(|_| invalid_entry("cohort entry selector is invalid"))?;

    validate_directory_entries_exact(package_path, &COHORT_PACKAGE_FILES)?;
    if read_bounded_file(&package_path.join("COMPLETE"), MAX_COMPLETE_MARKER_BYTES)? != complete {
        return Err(WasmUdfPackageError::InvalidCompletionMarker);
    }
    Ok(ValidatedWasmUdfPackage {
        execution: WasmUdfExecutionPolicy::from_legacy(&manifest),
        identity: ValidatedWasmUdfPackageIdentity::Legacy(manifest),
        package_key: package_entry.key,
        serialized_module_snapshot,
        entry_selector: Some(selector),
        permitted_conditional_convex_imports,
        core_wasm_bytes: core_wasm.size,
        serialized_module_bytes: serialized_module.size,
        graph: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn load_validated_capability_package(
    package_path: &Path,
    expected_manifest_sha256: &str,
    expected_platform_limits: &JsonValue,
    expected_entry_id: &str,
    expected_route_id: &str,
    expected_entry_selector_id: &str,
    expected_entry_path: &str,
    expected_runtime_module_path: &str,
    expected_export_name: &str,
    expected_udf_kind: UdfKind,
    expected_visibility: &str,
    runtime: &RuntimeCompatibility<'_>,
    expected_precompiler: &PrecompilerMaterialIdentity,
    serialized_module_snapshot_directory: &Path,
) -> Result<ValidatedWasmUdfPackage, WasmUdfPackageError> {
    validate_directory(package_path)?;
    let package_entry_bytes = read_bounded_file(
        &package_path.join("package-entry.json"),
        MAX_PACKAGE_ENTRY_BYTES,
    )?;
    require_canonical_json("package-entry.json", &package_entry_bytes)?;
    let package_entry: JsonValue = serde_json::from_slice(json_payload(&package_entry_bytes))
        .map_err(WasmUdfPackageError::MalformedPackageEntry)?;
    let kind = package_entry
        .get("kind")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| invalid_entry("capability package kind is missing or invalid"))?;
    match kind {
        CAPABILITY_ENTRY_PACKAGE_KIND => load_validated_capability_entry_package(
            package_path,
            expected_manifest_sha256,
            expected_platform_limits,
            expected_entry_id,
            expected_route_id,
            expected_entry_selector_id,
            expected_entry_path,
            expected_runtime_module_path,
            expected_export_name,
            expected_udf_kind,
            expected_visibility,
            runtime,
            expected_precompiler,
            serialized_module_snapshot_directory,
        ),
        _ => Err(invalid_entry("unsupported capability package kind")),
    }
}

#[allow(clippy::too_many_arguments)]
fn load_validated_capability_entry_package(
    package_path: &Path,
    expected_manifest_sha256: &str,
    expected_platform_limits: &JsonValue,
    expected_entry_id: &str,
    expected_route_id: &str,
    expected_entry_selector_id: &str,
    expected_entry_path: &str,
    expected_runtime_module_path: &str,
    expected_export_name: &str,
    expected_udf_kind: UdfKind,
    expected_visibility: &str,
    runtime: &RuntimeCompatibility<'_>,
    expected_precompiler: &PrecompilerMaterialIdentity,
    serialized_module_snapshot_directory: &Path,
) -> Result<ValidatedWasmUdfPackage, WasmUdfPackageError> {
    validate_directory(package_path)?;
    validate_directory_entries_exact(package_path, &CAPABILITY_ENTRY_PACKAGE_FILES)?;

    let complete = read_bounded_file(&package_path.join("COMPLETE"), MAX_COMPLETE_MARKER_BYTES)?;
    let package_entry_bytes = read_bounded_file(
        &package_path.join("package-entry.json"),
        MAX_PACKAGE_ENTRY_BYTES,
    )?;
    let manifest_bytes = read_bounded_file(
        &package_path.join("entry-manifest.json"),
        MAX_COHORT_MANIFEST_BYTES,
    )?;
    let provenance_bytes = read_bounded_file(
        &package_path.join("build-provenance.json"),
        MAX_COHORT_PROVENANCE_BYTES,
    )?;
    for (name, bytes) in [
        ("package-entry.json", package_entry_bytes.as_slice()),
        ("entry-manifest.json", manifest_bytes.as_slice()),
        ("build-provenance.json", provenance_bytes.as_slice()),
    ] {
        require_canonical_json(name, bytes)?;
    }

    let package_entry: CohortPackageEntry =
        serde_json::from_slice(json_payload(&package_entry_bytes))
            .map_err(WasmUdfPackageError::MalformedPackageEntry)?;
    if package_entry.kind != CAPABILITY_ENTRY_PACKAGE_KIND {
        return Err(invalid_entry("unsupported capability-entry package kind"));
    }
    for (field, value) in [
        ("package-entry.key", package_entry.key.as_str()),
        (
            "package-entry.manifestSha256",
            package_entry.manifest_sha256.as_str(),
        ),
        (
            "package-entry.provenance.sha256",
            package_entry.provenance.sha256.as_str(),
        ),
    ] {
        validate_sha256(field, value)?;
    }
    if package_entry
        .artifacts
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != BTreeSet::from(["module.cwasm", "module.wasm"])
    {
        return Err(invalid_entry(
            "capability-entry artifacts must contain exactly module.cwasm and module.wasm",
        ));
    }
    let manifest_sha256 = sha256(json_payload(&manifest_bytes));
    if package_entry.key != manifest_sha256
        || package_entry.manifest_sha256 != manifest_sha256
        || manifest_sha256 != expected_manifest_sha256
        || complete != format!("{}\n", package_entry.key).as_bytes()
        || package_path.file_name().and_then(|name| name.to_str())
            != Some(package_entry.key.as_str())
    {
        return Err(invalid_entry(
            "capability-entry package path, completion marker, and manifest identity disagree",
        ));
    }

    let manifest: CapabilityEntryManifest = serde_json::from_slice(json_payload(&manifest_bytes))
        .map_err(|error| {
        invalid_entry(format!(
            "capability-entry manifest has missing, unknown, or invalid fields: {error}"
        ))
    })?;
    let validated_identity = validate_capability_entry_manifest(
        &manifest,
        &package_entry,
        expected_platform_limits,
        expected_entry_id,
        expected_route_id,
        expected_entry_selector_id,
        expected_entry_path,
        expected_runtime_module_path,
        expected_export_name,
        expected_udf_kind,
        expected_visibility,
        runtime,
        expected_precompiler,
    )?;

    validate_artifact_bytes(
        "build-provenance.json",
        &provenance_bytes,
        &package_entry.provenance,
    )?;
    let provenance: CapabilityEntryBuildProvenance =
        serde_json::from_slice(json_payload(&provenance_bytes)).map_err(|error| {
            invalid_provenance(format!(
                "capability-entry provenance has missing, unknown, or invalid fields: {error}"
            ))
        })?;
    validate_capability_entry_provenance(
        &provenance,
        &manifest,
        &validated_identity.entries,
        &validated_identity.routes,
    )?;
    let official_output_chunk_entry_slots =
        official_output_chunk_entry_slots(&provenance, &validated_identity.entries);

    let core_wasm = package_entry
        .artifacts
        .get("module.wasm")
        .expect("validated capability-entry artifact disappeared");
    let serialized_module = package_entry
        .artifacts
        .get("module.cwasm")
        .expect("validated capability-entry artifact disappeared");
    validate_artifact_file(&package_path.join("module.wasm"), "module.wasm", core_wasm)?;
    validate_artifact_file(
        &package_path.join("build-provenance.json"),
        "build-provenance.json",
        &package_entry.provenance,
    )?;
    let serialized_module_snapshot = snapshot_validated_serialized_module_file(
        &package_path.join("module.cwasm"),
        "module.cwasm",
        serialized_module,
        serialized_module_snapshot_directory,
    )?;
    validate_directory_entries_exact(package_path, &CAPABILITY_ENTRY_PACKAGE_FILES)?;
    if read_bounded_file(&package_path.join("COMPLETE"), MAX_COMPLETE_MARKER_BYTES)? != complete {
        return Err(WasmUdfPackageError::InvalidCompletionMarker);
    }

    let selector = u64::from_str_radix(&validated_identity.selected_route.entry_selector_id, 16)
        .map_err(|_| invalid_entry("capability-entry route selector is invalid"))?;
    let permitted_conditional_convex_imports = permitted_conditional_convex_imports(
        manifest.execution.value_mode(),
        manifest.execution.effect_execution_mode(),
        manifest.execution.imported_operations(),
    );
    Ok(ValidatedWasmUdfPackage {
        execution: manifest.execution,
        identity: ValidatedWasmUdfPackageIdentity::CapabilityEntry(
            CapabilityEntryRuntimeIdentity {
                entries: validated_identity.entries,
                manifest_schema_version: manifest.schema_version,
                artifact_pipeline_sha256: manifest.pipeline.artifact_pipeline_sha256,
                compiler: manifest.compiler,
                official_output_chunk_entry_slots,
                opaque_value_abi_version: manifest.abi.opaque_value_abi_version,
                routes: validated_identity.routes,
            },
        ),
        package_key: package_entry.key,
        serialized_module_snapshot,
        entry_selector: Some(selector),
        permitted_conditional_convex_imports,
        core_wasm_bytes: core_wasm.size,
        serialized_module_bytes: serialized_module.size,
        graph: None,
    })
}

#[cfg(test)]
fn create_private_serialized_module_snapshot_tempdir(
    error_path: &Path,
) -> Result<tempfile::TempDir, WasmUdfPackageError> {
    #[cfg(unix)]
    let directory = {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()
            .map_err(|source| io_error(error_path, source))?;
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .map_err(|source| io_error(directory.path(), source))?;
        directory
    };
    #[cfg(not(unix))]
    let directory = tempfile::tempdir().map_err(|source| io_error(error_path, source))?;
    validate_serialized_module_snapshot_directory_layout(directory.path())?;
    Ok(directory)
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn load_capability_entry_package_for_compatibility_test(
    package_path: &Path,
    expected_package_id: &str,
    expected_entry_id: &str,
    expected_route_id: &str,
    expected_entry_selector_id: &str,
    expected_entry_path: &str,
    expected_runtime_module_path: &str,
    expected_export_name: &str,
    expected_udf_kind: UdfKind,
    expected_visibility: &str,
    runtime: &RuntimeCompatibility<'_>,
) -> Result<ValidatedWasmUdfPackage, WasmUdfPackageError> {
    // The standalone compatibility fixture has no deployment manifest. Route and
    // source identities remain independent test inputs, while the deployment-owned
    // platform and precompiler identities come from the artifact being checked.
    let manifest_bytes = read_bounded_file(
        &package_path.join("entry-manifest.json"),
        MAX_COHORT_MANIFEST_BYTES,
    )?;
    require_canonical_json("entry-manifest.json", &manifest_bytes)?;
    let manifest: CapabilityEntryManifest = serde_json::from_slice(json_payload(&manifest_bytes))
        .map_err(|error| {
        invalid_entry(format!(
            "capability-entry compatibility manifest is invalid: {error}"
        ))
    })?;
    let expected_platform_limits = serde_json::to_value(manifest.execution.platform_limits())
        .expect("platform limits serialize");
    let expected_precompiler = manifest.engine.package;
    let serialized_module_snapshot_directory =
        create_private_serialized_module_snapshot_tempdir(package_path)?;
    load_validated_capability_entry_package(
        package_path,
        expected_package_id,
        &expected_platform_limits,
        expected_entry_id,
        expected_route_id,
        expected_entry_selector_id,
        expected_entry_path,
        expected_runtime_module_path,
        expected_export_name,
        expected_udf_kind,
        expected_visibility,
        runtime,
        &expected_precompiler,
        serialized_module_snapshot_directory.path(),
    )
}

struct ValidatedCapabilityEntryManifestIdentity {
    entries: Vec<CapabilityEntryRuntimeUnitIdentity>,
    routes: Vec<CapabilityEntryRoute>,
    selected_route: CapabilityEntryRoute,
}

fn capability_entry_symbol(entry_id: &str) -> String {
    format!("sh_export_convex_wasm_entry_{entry_id}")
}

fn capability_manifest_entry_units<'a>(
    schema_version: u32,
    entry: Option<&'a CapabilityEntryIdentity>,
    entries: Option<&'a [CapabilityEntryIdentity]>,
) -> Result<Vec<&'a CapabilityEntryIdentity>, WasmUdfPackageError> {
    match schema_version {
        CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION => {
            let entry = entry.ok_or_else(|| {
                invalid_entry("capability-entry schema 3 requires exactly one entry")
            })?;
            if entries.is_some() {
                return Err(invalid_entry(
                    "capability-entry schema 3 forbids a generic entry table",
                ));
            }
            Ok(vec![entry])
        },
        CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION => {
            if entry.is_some() {
                return Err(invalid_entry(
                    "capability-entry schema 4 forbids the singleton entry field",
                ));
            }
            let entries = entries.ok_or_else(|| {
                invalid_entry("capability-entry schema 4 requires an entry-unit table")
            })?;
            if !(2..=8).contains(&entries.len()) {
                return Err(invalid_entry(
                    "capability-entry schema 4 requires between two and eight entry units",
                ));
            }
            Ok(entries.iter().collect())
        },
        _ => Err(invalid_entry(
            "capability-entry manifest uses an unsupported schema",
        )),
    }
}

fn validate_capability_entry_manifest_unit(
    entry: &CapabilityEntryIdentity,
    requires_explicit_symbol: bool,
    entry_selector_abi_version: u32,
) -> Result<CapabilityEntryRuntimeUnitIdentity, WasmUdfPackageError> {
    for (field, value) in [
        ("entry.entryId", entry.entry_id.as_str()),
        (
            "entry.localProfile.sha256",
            entry.local_profile.sha256.as_str(),
        ),
        (
            "entry.localProfile.metafileSha256",
            entry.local_profile.metafile_sha256.as_str(),
        ),
        (
            "entry.localProfile.dependencyGraphSha256",
            entry.local_profile.dependency_graph_sha256.as_str(),
        ),
        (
            "entry.localProfile.javascript.sha256",
            entry.local_profile.javascript.sha256.as_str(),
        ),
        (
            "entry.localProfile.sourceMap.sha256",
            entry.local_profile.source_map.sha256.as_str(),
        ),
    ] {
        validate_sha256(field, value)?;
    }
    if entry.entry_path.is_empty()
        || entry.entry_path.chars().any(char::is_control)
        || entry.module_path.is_empty()
        || entry.module_path.chars().any(char::is_control)
        || entry.local_profile.javascript.size == 0
        || entry.local_profile.source_map.size == 0
    {
        return Err(invalid_entry(
            "capability-entry source or local-profile identity is invalid",
        ));
    }
    let entry_identity = match (entry_selector_abi_version, entry.invocation_abi) {
        (LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION, None) => serde_json::json!({
            "domain": "convex-wasm-capability-entry-v1",
            "localProfileSha256": entry.local_profile.sha256,
            "selectedEntry": {
                "entryPath": entry.entry_path,
                "modulePath": entry.module_path,
            },
        }),
        (CAPABILITY_ENTRY_SELECTOR_ABI_VERSION, Some(invocation_abi)) => serde_json::json!({
            "domain": "convex-wasm-capability-entry-v1",
            "invocationAbi": invocation_abi,
            "localProfileSha256": entry.local_profile.sha256,
            "selectedEntry": {
                "entryPath": entry.entry_path,
                "modulePath": entry.module_path,
            },
        }),
        _ => {
            return Err(invalid_entry(
                "capability-entry invocation ABI disagrees with its selector ABI",
            ));
        },
    };
    let mut entry_identity_bytes = Vec::new();
    write_canonical_json(&entry_identity, &mut entry_identity_bytes)
        .expect("writing capability entry identity cannot fail");
    if sha256(&entry_identity_bytes) != entry.entry_id {
        return Err(invalid_entry(
            "capability-entry ID does not authenticate its local profile and selected entry",
        ));
    }
    let expected_symbol = capability_entry_symbol(&entry.entry_id);
    let entry_symbol = match (requires_explicit_symbol, entry.entry_symbol.as_deref()) {
        (false, None) => expected_symbol,
        (true, Some(symbol)) if symbol == expected_symbol => symbol.to_owned(),
        _ => {
            return Err(invalid_entry(
                "capability-entry schema and entry-unit factory identity disagree",
            ));
        },
    };
    Ok(CapabilityEntryRuntimeUnitIdentity {
        local_profile_sha256: entry.local_profile.sha256.clone(),
        entry_id: entry.entry_id.clone(),
        entry_path: entry.entry_path.clone(),
        entry_symbol,
        invocation_abi: entry.invocation_abi,
        module_path: entry.module_path.clone(),
    })
}

fn capability_source_pipeline_sha256(entries: &[CapabilityEntryRuntimeUnitIdentity]) -> String {
    if let [entry] = entries {
        return entry.local_profile_sha256.clone();
    }
    let identity = serde_json::json!({
        "domain": "convex-wasm-capability-package-source-pipeline-v2",
        "profiles": entries
            .iter()
            .map(|entry| entry.local_profile_sha256.as_str())
            .collect::<Vec<_>>(),
    });
    let mut identity_bytes = Vec::new();
    write_canonical_json(&identity, &mut identity_bytes)
        .expect("writing capability source-pipeline identity cannot fail");
    sha256(&identity_bytes)
}

fn capability_selector_id(
    entry_selector_abi_version: u32,
    entry_symbol: &str,
    export_name: &str,
    udf_kind: UdfKind,
    invocation_abi: Option<CapabilityInvocationAbi>,
) -> Result<String, WasmUdfPackageError> {
    let selector_identity = match (entry_selector_abi_version, invocation_abi) {
        (LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION, None) => serde_json::json!({
            "domain": "convex-wasm-capability-selector-member-v1",
            "entrySymbol": entry_symbol,
            "handlerExportName": export_name,
            "handlerUdfKind": udf_kind,
        }),
        (CAPABILITY_ENTRY_SELECTOR_ABI_VERSION, Some(invocation_abi)) => serde_json::json!({
            "domain": "convex-wasm-capability-selector-member-v1",
            "entrySymbol": entry_symbol,
            "handlerExportName": export_name,
            "handlerUdfKind": udf_kind,
            "invocationAbi": invocation_abi,
        }),
        _ => {
            return Err(invalid_entry(
                "capability-entry invocation ABI disagrees with its selector ABI",
            ));
        },
    };
    let mut selector_identity_bytes = Vec::new();
    write_canonical_json(&selector_identity, &mut selector_identity_bytes)
        .expect("writing capability selector identity cannot fail");
    Ok(sha256(&selector_identity_bytes)[..16].to_owned())
}

fn capability_route_selector_id(
    schema_version: u32,
    entry_selector_abi_version: u32,
    route_id: &str,
    entry_symbol: &str,
    export_name: &str,
    udf_kind: UdfKind,
    invocation_abi: Option<CapabilityInvocationAbi>,
) -> Result<String, WasmUdfPackageError> {
    match schema_version {
        CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION => route_id
            .get(..16)
            .map(str::to_owned)
            .ok_or_else(|| invalid_entry("capability-entry schema 3 route ID is invalid")),
        CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION => capability_selector_id(
            entry_selector_abi_version,
            entry_symbol,
            export_name,
            udf_kind,
            invocation_abi,
        ),
        _ => Err(invalid_entry(
            "capability-entry route uses an unsupported manifest schema",
        )),
    }
}

fn capability_manifest_route_entry<'a>(
    schema_version: u32,
    entries: &'a [CapabilityEntryRuntimeUnitIdentity],
    route: &CapabilityEntryManifestRoute,
) -> Result<&'a CapabilityEntryRuntimeUnitIdentity, WasmUdfPackageError> {
    match schema_version {
        CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION => {
            if route.entry_id.is_some() || route.entry_symbol.is_some() {
                return Err(invalid_entry(
                    "capability-entry schema 3 route contains generic selector fields",
                ));
            }
            entries
                .first()
                .ok_or_else(|| invalid_entry("capability-entry schema 3 has no entry"))
        },
        CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION => {
            let route_entry_id = route.entry_id.as_deref().ok_or_else(|| {
                invalid_entry("capability-entry schema 4 route omits its entry unit")
            })?;
            let entry = entries
                .iter()
                .find(|entry| entry.entry_id == route_entry_id)
                .ok_or_else(|| {
                    invalid_entry("capability-entry route selects an unknown entry unit")
                })?;
            if route.entry_symbol.as_deref() != Some(entry.entry_symbol.as_str()) {
                return Err(invalid_entry(
                    "capability-entry route and entry-unit factory identity disagree",
                ));
            }
            Ok(entry)
        },
        _ => Err(invalid_entry(
            "capability-entry route uses an unsupported manifest schema",
        )),
    }
}

fn validate_capability_entry_route_coverage(
    entries: &[CapabilityEntryRuntimeUnitIdentity],
    routes: &[CapabilityEntryRoute],
) -> Result<(), WasmUdfPackageError> {
    let expected_entry_ids = entries
        .iter()
        .map(|entry| entry.entry_id.as_str())
        .collect::<BTreeSet<_>>();
    let routed_entry_ids = routes
        .iter()
        .map(|route| route.entry_id.as_str())
        .collect::<BTreeSet<_>>();
    if routed_entry_ids != expected_entry_ids {
        return Err(invalid_entry(
            "capability-entry route table does not cover every entry unit",
        ));
    }
    Ok(())
}

struct ValidatedModuleGraphCohortIdentity {
    entries: Vec<CapabilityEntryRuntimeUnitIdentity>,
    routes: Vec<CapabilityEntryRoute>,
}

impl ContextReuseAnalysisIdentity {
    pub(super) fn validate(&self) -> Result<(), WasmUdfPackageError> {
        if self.kind != CONTEXT_REUSE_ANALYSIS_KIND {
            return Err(invalid_deployment_manifest(
                "context-reuse analysis identity kind is unsupported",
            ));
        }
        validate_deployment_sha256(
            "contextReuseAnalysis.policyFingerprint",
            &self.policy_fingerprint,
        )?;
        validate_deployment_sha256("contextReuseAnalysis.resultSha256", &self.result_sha256)?;
        validate_sorted_context_reuse_paths(
            &self.entries,
            "contextReuseAnalysis.entries",
            false,
            compare_rust_strings,
        )
    }

    fn contains_entry(&self, entry_path: &str) -> bool {
        self.entries
            .binary_search_by(|candidate| candidate.as_str().cmp(entry_path))
            .is_ok()
    }
}

impl ContextReuseCohortAnalysisIdentity {
    pub(super) fn validate(&self) -> Result<(), WasmUdfPackageError> {
        if self.kind != CONTEXT_REUSE_COHORT_ANALYSIS_KIND
            || self.entries.len() != self.entry_graph_sha256s.len()
        {
            return Err(invalid_deployment_manifest(
                "context-reuse cohort analysis identity is unsupported",
            ));
        }
        validate_sorted_context_reuse_paths(
            &self.entries,
            "contextReuseAnalysis.entries",
            true,
            compare_javascript_strings,
        )?;
        for digest in &self.entry_graph_sha256s {
            validate_deployment_sha256("contextReuseAnalysis.entryGraphSha256s", digest)?;
        }
        for (path, digest) in &self.third_party_material_fingerprints {
            // This map is embedded in JavaScript-authored enclosing identities. ASCII keeps
            // JavaScript UTF-16 and Rust scalar key ordering identical at every outer
            // digest.
            if !path.starts_with("node_modules/")
                || !is_normalized_repository_relative_posix_path(path)
                || !path.is_ascii()
            {
                return Err(invalid_deployment_manifest(
                    "context-reuse third-party material path must be normalized ASCII",
                ));
            }
            validate_deployment_sha256(
                "contextReuseAnalysis.thirdPartyMaterialFingerprints",
                digest,
            )?;
        }
        for (field, digest) in [
            (
                "contextReuseAnalysis.policyFingerprint",
                &self.policy_fingerprint,
            ),
            (
                "contextReuseAnalysis.sharedAnalysisSha256",
                &self.shared_analysis_sha256,
            ),
            ("contextReuseAnalysis.resultSha256", &self.result_sha256),
        ] {
            validate_deployment_sha256(field, digest)?;
        }
        let payload = serde_json::json!({
            "entries": self.entries,
            "entryGraphSha256s": self.entry_graph_sha256s,
            "kind": self.kind,
            "policyFingerprint": self.policy_fingerprint,
            "sharedAnalysisSha256": self.shared_analysis_sha256,
            "thirdPartyMaterialFingerprints": self.third_party_material_fingerprints,
        });
        let mut canonical = Vec::new();
        write_javascript_canonical_json(&payload, &mut canonical)
            .expect("writing canonical context-reuse cohort analysis cannot fail");
        if sha256(&canonical) != self.result_sha256 {
            return Err(invalid_deployment_manifest(
                "context-reuse cohort analysis identity digest is invalid",
            ));
        }
        Ok(())
    }
}

pub(super) fn validate_context_reuse_cohort_analysis_set(
    application_analysis: &ContextReuseAnalysisIdentity,
    cohort_analyses: &[&ContextReuseCohortAnalysisIdentity],
) -> Result<(), WasmUdfPackageError> {
    application_analysis.validate()?;
    let mut shared_analysis_sha256 = None;
    for cohort_analysis in cohort_analyses {
        cohort_analysis.validate()?;
        if cohort_analysis.policy_fingerprint != application_analysis.policy_fingerprint
            || shared_analysis_sha256
                .replace(cohort_analysis.shared_analysis_sha256.as_str())
                .is_some_and(|previous| previous != cohort_analysis.shared_analysis_sha256.as_str())
        {
            return Err(invalid_deployment_manifest(
                "context-reuse cohort analyses do not share one application authority",
            ));
        }
    }
    if shared_analysis_sha256.is_none() {
        return Err(invalid_deployment_manifest(
            "context-reuse cohort analysis set must not be empty",
        ));
    }
    Ok(())
}

fn validate_sorted_context_reuse_paths(
    paths: &[String],
    description: &str,
    require_nonempty: bool,
    compare: fn(&str, &str) -> std::cmp::Ordering,
) -> Result<(), WasmUdfPackageError> {
    if (require_nonempty && paths.is_empty())
        || paths
            .iter()
            .any(|path| !is_normalized_repository_relative_posix_path(path))
        || paths
            .windows(2)
            .any(|pair| compare(&pair[0], &pair[1]) != std::cmp::Ordering::Less)
    {
        return Err(invalid_deployment_manifest(format!(
            "{description} must contain unique sorted normalized paths"
        )));
    }
    Ok(())
}

fn compare_rust_strings(left: &str, right: &str) -> std::cmp::Ordering {
    left.cmp(right)
}

fn compare_javascript_strings(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn write_javascript_canonical_json(value: &JsonValue, output: &mut Vec<u8>) -> std::io::Result<()> {
    // The cohort result identity is authored by JSON.stringify after UTF-16 key
    // sorting.
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            serde_json::to_writer(output, value)?;
        },
        JsonValue::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_javascript_canonical_json(value, output)?;
            }
            output.push(b']');
        },
        JsonValue::Object(values) => {
            output.push(b'{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable_by(|left, right| compare_javascript_strings(left, right));
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');
                write_javascript_canonical_json(
                    values
                        .get(key)
                        .expect("canonical JSON object key disappeared"),
                    output,
                )?;
            }
            output.push(b'}');
        },
    }
    Ok(())
}

impl ModuleGraphCohortContract {
    pub(super) fn cohort_contract_sha256(&self) -> &str {
        &self.cohort_contract_sha256
    }

    pub(super) fn cohort_id(&self) -> &str {
        &self.cohort_id
    }

    pub(super) fn schedule_sha256(&self) -> &str {
        &self.schedule_sha256
    }

    pub(super) fn source_envelope_sha256(&self) -> &str {
        &self.source_envelope_sha256
    }

    pub(super) fn producer_implementation(&self) -> (&str, &str) {
        (
            &self.producer_implementation.kind,
            &self.producer_implementation.sha256,
        )
    }

    pub(super) fn context_reuse_analysis(&self) -> Option<&ContextReuseCohortAnalysisIdentity> {
        self.context_reuse_analysis.as_ref()
    }

    pub(super) fn routes(&self) -> &[ModuleGraphCohortRoute] {
        &self.routes
    }

    pub(super) fn engine_compatibility_sha256(&self) -> &str {
        &self.engine.compatibility_sha256
    }

    pub(super) fn engine_configuration_sha256(&self) -> &str {
        &self.engine.configuration_sha256
    }

    pub(super) fn engine_revision(&self) -> &str {
        &self.engine.revision
    }

    pub(super) fn engine_target_cpu(&self) -> &str {
        &self.engine.target.cpu
    }

    pub(super) fn engine_target_triple(&self) -> &str {
        &self.engine.target.triple
    }

    pub(super) fn precompiler_material_identity_json(&self) -> JsonValue {
        serde_json::to_value(&self.precompiler_material_identity)
            .expect("precompiler material identity serializes")
    }

    fn validate(&self) -> Result<ValidatedModuleGraphCohortIdentity, WasmUdfPackageError> {
        let context_reuse_contract = match (self.kind.as_str(), self.schema_version) {
            (MODULE_GRAPH_COHORT_CONTRACT_KIND, MODULE_GRAPH_COHORT_CONTRACT_SCHEMA_VERSION) => {
                self.context_reuse_analysis.is_none()
            },
            (
                MODULE_GRAPH_COHORT_CONTRACT_KIND_V2,
                MODULE_GRAPH_COHORT_CONTRACT_SCHEMA_VERSION_V2,
            ) => self.context_reuse_analysis.is_some(),
            _ => false,
        };
        if !context_reuse_contract || self.entries.is_empty() || self.routes.is_empty() {
            return Err(invalid_deployment_manifest(
                "module graph cohort contract kind, schema, entries, or routes are unsupported",
            ));
        }
        if let Some(context_reuse_analysis) = &self.context_reuse_analysis {
            context_reuse_analysis.validate()?;
            let cohort_entries = self
                .entries
                .iter()
                .map(|entry| entry.entry_path.as_str())
                .collect::<Vec<_>>();
            let analyzed_entries = context_reuse_analysis
                .entries
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let cohort_graphs = self
                .entries
                .iter()
                .map(|entry| entry.local_profile.dependency_graph_sha256.as_str())
                .collect::<Vec<_>>();
            let analyzed_graphs = context_reuse_analysis
                .entry_graph_sha256s
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            if cohort_entries != analyzed_entries || cohort_graphs != analyzed_graphs {
                return Err(invalid_deployment_manifest(
                    "module graph cohort entries differ from their context-reuse analysis",
                ));
            }
        }
        for (field, digest) in [
            ("cohortContractSha256", self.cohort_contract_sha256.as_str()),
            ("cohortId", self.cohort_id.as_str()),
            ("scheduleSha256", self.schedule_sha256.as_str()),
            ("sourceEnvelopeSha256", self.source_envelope_sha256.as_str()),
            (
                "compiler.loweringPipelineSha256",
                self.compiler.lowering_pipeline_sha256.as_str(),
            ),
            (
                "compiler.artifactPipelineSha256",
                self.compiler.artifact_pipeline_sha256.as_str(),
            ),
            (
                "compiler.sourcePipelineSha256",
                self.compiler.source_pipeline_sha256.as_str(),
            ),
            (
                "compiler.staticHermesGlobalPolicy.inventorySha256",
                self.compiler
                    .static_hermes_global_policy
                    .inventory_sha256
                    .as_str(),
            ),
            (
                "compiler.staticHermesGlobalPolicy.runtimeSurfacePolicySha256",
                self.compiler
                    .static_hermes_global_policy
                    .runtime_surface_policy_sha256
                    .as_str(),
            ),
            (
                "compilerSourceEnvelopeSha256",
                self.compiler_source_envelope_sha256.as_str(),
            ),
            (
                "descriptorIdentitySha256",
                self.descriptor_identity_sha256.as_str(),
            ),
            (
                "engine.compatibilitySha256",
                self.engine.compatibility_sha256.as_str(),
            ),
            (
                "engine.configurationSha256",
                self.engine.configuration_sha256.as_str(),
            ),
            (
                "engine.wasmtimeMaterialsSha256",
                self.engine.wasmtime_materials_sha256.as_str(),
            ),
            (
                "runtimeSurfacePolicySha256",
                self.runtime_surface_policy_sha256.as_str(),
            ),
            ("sourcePipelineSha256", self.source_pipeline_sha256.as_str()),
        ] {
            validate_deployment_sha256(field, digest)?;
        }
        if self.compiler.admitted_language_version == 0
            || self.compiler.compiler_revision.is_empty()
            || self
                .compiler
                .compiler_revision
                .chars()
                .any(char::is_control)
            || self.compiler.static_hermes_revision.is_empty()
            || self
                .compiler
                .static_hermes_revision
                .chars()
                .any(char::is_control)
            || self.compiler.static_hermes_global_policy.kind
                != MODULE_GRAPH_RUNTIME_SURFACE_POLICY_IDENTITY_KIND
            || self.compiler.static_hermes_global_policy.inventory_sha256
                != MODULE_GRAPH_RUNTIME_SURFACE_INVENTORY_SHA256
            || self
                .compiler
                .static_hermes_global_policy
                .runtime_surface_policy_sha256
                != self.runtime_surface_policy_sha256
            || self.runtime_surface_policy_sha256 != MODULE_GRAPH_RUNTIME_SURFACE_POLICY_SHA256
            || self.compiler.source_pipeline_sha256 != self.source_pipeline_sha256
            || self.engine.package != self.precompiler_material_identity
            || self.engine.revision != self.precompiler_material_identity.wasmtime_revision
            || self.engine.target.triple != self.precompiler_material_identity.target_triple
            || self.engine.target.cpu.is_empty()
            || self.engine.target.cpu.chars().any(char::is_control)
        {
            return Err(invalid_deployment_manifest(
                "module graph cohort compiler, engine, or source identity is invalid",
            ));
        }
        validate_precompiler_material_identity(
            &self.precompiler_material_identity,
            "moduleGraphCohort.precompilerMaterialIdentity",
        )?;
        validate_capability_producer_implementation_identity(&self.producer_implementation)?;
        let mut engine_config_bytes = Vec::new();
        write_canonical_json(
            &serde_json::to_value(&self.engine.config).expect("engine config serializes"),
            &mut engine_config_bytes,
        )
        .expect("writing engine config cannot fail");
        if sha256(&engine_config_bytes) != self.engine.configuration_sha256 {
            return Err(invalid_deployment_manifest(
                "module graph cohort engine configuration identity is invalid",
            ));
        }
        self.execution
            .validate_capability_entry_contract(OPAQUE_VALUE_ABI_VERSION)?;
        capability_entry_value_codec(&self.execution, &self.compiler.lowering_pipeline_sha256)?;
        let request_envelope = capability_entry_request_envelope(
            &self.execution,
            &self.compiler.lowering_pipeline_sha256,
        )?;
        if request_envelope.capability_request_abi_version != CAPABILITY_REQUEST_ABI_VERSION {
            return Err(invalid_deployment_manifest(
                "module graph cohort request-envelope ABI is unsupported",
            ));
        }

        let mut entries = Vec::with_capacity(self.entries.len());
        let mut previous_entry_path: Option<&str> = None;
        let mut entry_ids = BTreeSet::new();
        let mut entry_symbols = BTreeSet::new();
        let mut module_paths = BTreeSet::new();
        for entry in &self.entries {
            if !entry.source.is_object() {
                return Err(invalid_deployment_manifest(
                    "module graph cohort entry source identity must be an object",
                ));
            }
            let capability_entry = CapabilityEntryIdentity {
                entry_id: entry.entry_id.clone(),
                entry_path: entry.entry_path.clone(),
                entry_symbol: Some(entry.entry_symbol.clone()),
                invocation_abi: Some(entry.invocation_abi),
                local_profile: entry.local_profile.clone(),
                module_path: entry.module_path.clone(),
            };
            let validated = validate_capability_entry_manifest_unit(
                &capability_entry,
                true,
                CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            )?;
            if previous_entry_path.is_some_and(|previous| {
                compare_javascript_strings(previous, validated.entry_path.as_str())
                    != std::cmp::Ordering::Less
            }) || !entry_ids.insert(validated.entry_id.clone())
                || !entry_symbols.insert(validated.entry_symbol.clone())
                || !module_paths.insert(validated.module_path.clone())
            {
                return Err(invalid_deployment_manifest(
                    "module graph cohort entries are unsorted or duplicated",
                ));
            }
            previous_entry_path = Some(&entry.entry_path);
            entries.push(validated);
        }
        if self.compiler.source_pipeline_sha256 != capability_source_pipeline_sha256(&entries) {
            return Err(invalid_deployment_manifest(
                "module graph cohort source pipeline differs from its local profiles",
            ));
        }

        let entries_by_id = entries
            .iter()
            .map(|entry| (entry.entry_id.as_str(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut previous_route_id: Option<&str> = None;
        let mut route_ids = BTreeSet::new();
        let mut selector_ids = BTreeSet::new();
        let mut routes = Vec::with_capacity(self.routes.len());
        for route in &self.routes {
            validate_deployment_sha256("module graph cohort routeId", &route.route_id)?;
            validate_entry_selector_id(
                "module graph cohort entrySelectorId",
                &route.entry_selector_id,
            )?;
            validate_visibility(&route.visibility)?;
            let entry = entries_by_id.get(route.entry_id.as_str()).ok_or_else(|| {
                invalid_deployment_manifest("module graph cohort route selects an unknown entry")
            })?;
            let route_identity = serde_json::json!({
                "domain": "convex-wasm-capability-route-v1",
                "entryId": entry.entry_id,
                "exportName": route.export_name,
                "udfKind": route.udf_kind,
                "visibility": route.visibility,
            });
            let mut route_identity_bytes = Vec::new();
            write_canonical_json(&route_identity, &mut route_identity_bytes)
                .expect("writing module graph cohort route identity cannot fail");
            let expected_route_id = sha256(&route_identity_bytes);
            let expected_selector_id = match entry.invocation_abi {
                Some(CapabilityInvocationAbi::LegacyHandler) => expected_route_id[..16].to_owned(),
                Some(CapabilityInvocationAbi::OfficialWrapper) => capability_selector_id(
                    CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
                    &entry.entry_symbol,
                    &route.export_name,
                    route.udf_kind,
                    entry.invocation_abi,
                )?,
                None => {
                    return Err(invalid_deployment_manifest(
                        "module graph cohort entry omits its invocation ABI",
                    ));
                },
            };
            if route.export_name.is_empty()
                || route.export_name.chars().any(char::is_control)
                || route.entry_symbol != entry.entry_symbol
                || route.route_id != expected_route_id
                || route.entry_selector_id != expected_selector_id
                || previous_route_id.is_some_and(|previous| previous >= route.route_id.as_str())
                || !route_ids.insert(route.route_id.as_str())
                || !selector_ids.insert(route.entry_selector_id.as_str())
            {
                return Err(invalid_deployment_manifest(
                    "module graph cohort routes are invalid, unsorted, or duplicated",
                ));
            }
            previous_route_id = Some(&route.route_id);
            routes.push(CapabilityEntryRoute {
                entry_id: entry.entry_id.clone(),
                entry_selector_id: route.entry_selector_id.clone(),
                entry_symbol: entry.entry_symbol.clone(),
                export_name: route.export_name.clone(),
                invocation_abi: entry.invocation_abi,
                route_id: route.route_id.clone(),
                udf_kind: route.udf_kind,
                visibility: route.visibility.clone(),
            });
        }
        validate_capability_entry_route_coverage(&entries, &routes)?;

        let mut value = serde_json::to_value(self).expect("module graph cohort serializes");
        value
            .as_object_mut()
            .expect("module graph cohort serializes as an object")
            .remove("cohortContractSha256");
        let mut canonical = Vec::new();
        write_canonical_json(&value, &mut canonical)
            .expect("writing canonical module graph cohort cannot fail");
        if sha256(&canonical) != self.cohort_contract_sha256 {
            return Err(invalid_deployment_manifest(
                "module graph cohort contract identity is invalid",
            ));
        }
        Ok(ValidatedModuleGraphCohortIdentity { entries, routes })
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_deployment_route(
        &self,
        expected_entry_id: &str,
        expected_route_id: &str,
        expected_entry_selector_id: &str,
        expected_entry_path: &str,
        expected_runtime_module_path: &str,
        expected_export_name: &str,
        expected_udf_kind: UdfKind,
        expected_visibility: &str,
        deployment_compiler: &JsonValue,
        deployment_platform_limits: &JsonValue,
        deployment_precompiler: &PrecompilerMaterialIdentity,
    ) -> Result<(), WasmUdfPackageError> {
        let identity = self.validate()?;
        let entry = identity
            .entries
            .iter()
            .find(|entry| entry.entry_id == expected_entry_id)
            .ok_or_else(|| {
                invalid_deployment_manifest("module graph route reference selects no cohort entry")
            })?;
        let route = identity
            .routes
            .iter()
            .find(|route| route.route_id == expected_route_id)
            .ok_or_else(|| {
                invalid_deployment_manifest("module graph route reference selects no cohort route")
            })?;
        if route.entry_id != expected_entry_id
            || route.entry_selector_id != expected_entry_selector_id
            || route.export_name != expected_export_name
            || route.udf_kind != expected_udf_kind
            || route.visibility != expected_visibility
            || entry.entry_path != expected_entry_path
            || expected_runtime_module_path.strip_suffix(".js") != Some(entry.module_path.as_str())
            || serde_json::to_value(&self.compiler).expect("cohort compiler serializes")
                != *deployment_compiler
            || serde_json::to_value(self.execution.platform_limits())
                .expect("cohort platform limits serialize")
                != *deployment_platform_limits
            || self.precompiler_material_identity != *deployment_precompiler
        {
            return Err(invalid_deployment_manifest(
                "module graph cohort contract differs from its deployment route",
            ));
        }
        Ok(())
    }
}

impl ModuleGraphCohortRoute {
    pub(super) fn entry_id(&self) -> &str {
        &self.entry_id
    }

    pub(super) fn entry_selector_id(&self) -> &str {
        &self.entry_selector_id
    }

    pub(super) fn entry_symbol(&self) -> &str {
        &self.entry_symbol
    }

    pub(super) fn export_name(&self) -> &str {
        &self.export_name
    }

    pub(super) fn route_id(&self) -> &str {
        &self.route_id
    }

    pub(super) fn udf_kind(&self) -> UdfKind {
        self.udf_kind
    }

    pub(super) fn visibility(&self) -> &str {
        &self.visibility
    }
}

pub(super) fn validate_module_graph_cohort_contracts(
    contracts: &[ModuleGraphCohortContract],
) -> Result<(), WasmUdfPackageError> {
    if contracts.is_empty() {
        return Err(invalid_deployment_manifest(
            "module graph cohort contracts must not be empty",
        ));
    }
    let mut previous_cohort_id: Option<&str> = None;
    let mut contract_ids = BTreeSet::new();
    let mut route_ids = BTreeSet::new();
    for contract in contracts {
        let identity = contract.validate()?;
        if previous_cohort_id.is_some_and(|previous| previous >= contract.cohort_id.as_str())
            || !contract_ids.insert(contract.cohort_contract_sha256.as_str())
            || identity
                .routes
                .iter()
                .any(|route| !route_ids.insert(route.route_id.clone()))
        {
            return Err(invalid_deployment_manifest(
                "module graph cohort contracts are unsorted or duplicated",
            ));
        }
        previous_cohort_id = Some(&contract.cohort_id);
    }
    Ok(())
}

fn validate_capability_producer_implementation_identity(
    identity: &CapabilityProducerImplementationIdentity,
) -> Result<(), WasmUdfPackageError> {
    validate_sha256("pipeline.producerImplementation.sha256", &identity.sha256)?;
    if identity.kind != CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND {
        return Err(invalid_entry(
            "capability-entry producer implementation identity is unsupported",
        ));
    }
    Ok(())
}

fn validate_capability_static_hermes_identity(
    identity: &CapabilityStaticHermesIdentity,
) -> Result<(), WasmUdfPackageError> {
    let c_bundle_enabled = capability_static_hermes_c_bundle_enabled(&identity.flags)?;
    if c_bundle_enabled {
        let c_bundle = identity.c_bundle.as_ref().ok_or_else(|| {
            invalid_entry("capability-entry Static Hermes C bundle identity is missing")
        })?;
        if c_bundle.kind != CAPABILITY_STATIC_HERMES_C_BUNDLE_KIND {
            return Err(invalid_entry(
                "capability-entry Static Hermes C bundle identity kind is unsupported",
            ));
        }
        validate_sha256(
            "toolchain.staticHermes.cBundle.compilationSha256",
            &c_bundle.compilation_sha256,
        )?;
    } else if identity.c_bundle.is_some() {
        return Err(invalid_entry(
            "capability-entry Static Hermes C bundle identity has no matching flags",
        ));
    }
    Ok(())
}

fn capability_static_hermes_c_bundle_enabled(
    flags: &[String],
) -> Result<bool, WasmUdfPackageError> {
    Ok(capability_static_hermes_c_bundle_shard_size(flags)?.is_some())
}

fn capability_static_hermes_c_bundle_shard_size(
    flags: &[String],
) -> Result<Option<u32>, WasmUdfPackageError> {
    let c_bundle_flag_count = flags
        .iter()
        .filter(|flag| flag.as_str() == CAPABILITY_STATIC_HERMES_C_BUNDLE_FLAG)
        .count();
    let c_bundle_shard_sizes = flags
        .iter()
        .filter_map(|flag| {
            flag.strip_prefix(CAPABILITY_STATIC_HERMES_C_BUNDLE_SHARD_SIZE_FLAG_PREFIX)
        })
        .collect::<Vec<_>>();
    match (c_bundle_flag_count, c_bundle_shard_sizes.len()) {
        (0, 0) => Ok(None),
        (1, 1) => {
            let shard_size = c_bundle_shard_sizes[0];
            let shard_size = shard_size.parse::<u32>().ok().filter(|size| *size > 0);
            if shard_size.is_none() || !flags.iter().any(|flag| flag == "-emit-c") {
                return Err(invalid_entry(
                    "capability-entry Static Hermes C bundle flags are invalid",
                ));
            }
            Ok(shard_size)
        },
        _ => Err(invalid_entry(
            "capability-entry Static Hermes C bundle flags are incomplete or duplicated",
        )),
    }
}

fn is_normalized_repository_relative_posix_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.ends_with('/')
        && !path.contains('\\')
        && !path.contains('\0')
        && path
            .split('/')
            .all(|component| !matches!(component, "" | "." | ".."))
}

fn validate_capability_producer_identity(
    identity: &CapabilityProducerIdentity,
) -> Result<CapabilityProducerImplementationIdentity, WasmUdfPackageError> {
    const JAVASCRIPT_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

    if identity.kind != CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND
        || identity.node_version.is_empty()
        || identity.sources.is_empty()
        || !is_normalized_repository_relative_posix_path(&identity.manifest.path)
        || identity.manifest.size > JAVASCRIPT_MAX_SAFE_INTEGER
        || validate_sha256("producerIdentity.sha256", &identity.sha256).is_err()
        || validate_sha256(
            "producerIdentity.manifest.sha256",
            &identity.manifest.sha256,
        )
        .is_err()
    {
        return Err(invalid_provenance(
            "capability-entry producer identity is invalid",
        ));
    }
    for (sources, field) in [
        (&identity.sources, "sources"),
        (&identity.operational_sources, "operationalSources"),
    ] {
        let mut previous_path: Option<&str> = None;
        for source in sources {
            if !is_normalized_repository_relative_posix_path(&source.path)
                || source.size > JAVASCRIPT_MAX_SAFE_INTEGER
                || validate_sha256(&format!("producerIdentity.{field}.sha256"), &source.sha256)
                    .is_err()
                || previous_path.is_some_and(|previous| previous >= source.path.as_str())
            {
                return Err(invalid_provenance(format!(
                    "capability-entry producer identity {field} table is invalid"
                )));
            }
            previous_path = Some(&source.path);
        }
    }
    Ok(CapabilityProducerImplementationIdentity {
        kind: identity.kind.clone(),
        sha256: identity.sha256.clone(),
    })
}

pub(super) fn normalized_capability_producer_implementation_identity(
    value: &JsonValue,
) -> Result<(String, String), WasmUdfPackageError> {
    let identity: CapabilityProducerIdentity = serde_json::from_value(value.clone())
        .map_err(|_| invalid_provenance("capability-entry producer identity is malformed"))?;
    let implementation = validate_capability_producer_identity(&identity)?;
    Ok((implementation.kind, implementation.sha256))
}

#[allow(clippy::too_many_arguments)]
fn validate_capability_entry_manifest(
    manifest: &CapabilityEntryManifest,
    package_entry: &CohortPackageEntry,
    expected_platform_limits: &JsonValue,
    expected_entry_id: &str,
    expected_route_id: &str,
    expected_entry_selector_id: &str,
    expected_entry_path: &str,
    expected_runtime_module_path: &str,
    expected_export_name: &str,
    expected_udf_kind: UdfKind,
    expected_visibility: &str,
    runtime: &RuntimeCompatibility<'_>,
    expected_precompiler: &PrecompilerMaterialIdentity,
) -> Result<ValidatedCapabilityEntryManifestIdentity, WasmUdfPackageError> {
    if manifest.kind != CAPABILITY_ENTRY_MANIFEST_KIND
        || !matches!(
            manifest.schema_version,
            CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION | CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION
        )
        || !is_supported_capability_request_abi_version(manifest.abi.capability_request_abi_version)
        || !matches!(
            manifest.abi.entry_selector_abi_version,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION | CAPABILITY_ENTRY_SELECTOR_ABI_VERSION
        )
    {
        return Err(invalid_entry(
            "unsupported capability-entry manifest kind, schema, ABI, or runtime identity",
        ));
    }
    manifest
        .execution
        .validate_capability_entry(manifest.abi.opaque_value_abi_version, runtime)?;
    capability_entry_value_codec(
        &manifest.execution,
        &manifest.compiler.lowering_pipeline_sha256,
    )?;
    let request_envelope = capability_entry_request_envelope(
        &manifest.execution,
        &manifest.compiler.lowering_pipeline_sha256,
    )?;
    if manifest.abi.capability_request_abi_version
        != request_envelope.capability_request_abi_version
    {
        return Err(invalid_entry(
            "capability-entry request-envelope ABI differs from the manifest ABI",
        ));
    }
    if serde_json::to_value(manifest.execution.platform_limits())
        .expect("platform limits serialize")
        != *expected_platform_limits
    {
        return Err(invalid_entry(
            "capability-entry platform limits differ from the deployment export",
        ));
    }
    for (field, value) in [
        (
            "compiler.loweringPipelineSha256",
            manifest.compiler.lowering_pipeline_sha256.as_str(),
        ),
        (
            "compiler.sourcePipelineSha256",
            manifest.compiler.source_pipeline_sha256.as_str(),
        ),
        (
            "compiler.staticHermesGlobalPolicy.inventorySha256",
            manifest
                .compiler
                .static_hermes_global_policy
                .inventory_sha256
                .as_str(),
        ),
        (
            "compiler.staticHermesGlobalPolicy.runtimeSurfacePolicySha256",
            manifest
                .compiler
                .static_hermes_global_policy
                .runtime_surface_policy_sha256
                .as_str(),
        ),
        (
            "pipeline.artifactPipelineSha256",
            manifest.pipeline.artifact_pipeline_sha256.as_str(),
        ),
        (
            "pipeline.runtimeSurfacePolicySha256",
            manifest.pipeline.runtime_surface_policy_sha256.as_str(),
        ),
        (
            "runtime.archivesMaterialSha256",
            manifest.runtime.archives_material_sha256.as_str(),
        ),
        (
            "runtime.headersMaterialSha256",
            manifest.runtime.headers_material_sha256.as_str(),
        ),
        (
            "runtime.mainMaterialSha256",
            manifest.runtime.main_material_sha256.as_str(),
        ),
        (
            "runtime.mainObject.sha256",
            manifest.runtime.main_object.sha256.as_str(),
        ),
        (
            "toolchain.emscripten.materialsSha256",
            manifest.toolchain.emscripten.materials_sha256.as_str(),
        ),
        (
            "toolchain.staticHermes.materialsSha256",
            manifest.toolchain.static_hermes.materials_sha256.as_str(),
        ),
    ] {
        validate_sha256(field, value)?;
    }
    validate_capability_static_hermes_identity(&manifest.toolchain.static_hermes)?;
    validate_capability_producer_implementation_identity(
        &manifest.pipeline.producer_implementation,
    )?;
    if manifest.runtime.main_object.size == 0
        || manifest.compiler.admitted_language_version == 0
        || manifest.compiler.compiler_revision.is_empty()
        || manifest.compiler.static_hermes_revision.is_empty()
        || manifest.compiler.static_hermes_global_policy.kind
            != "convex-wasm-runtime-surface-policy-identity"
        || manifest
            .compiler
            .static_hermes_global_policy
            .runtime_surface_policy_sha256
            != manifest.pipeline.runtime_surface_policy_sha256
        || manifest.toolchain.emscripten.llvm_revision.is_empty()
        || manifest.toolchain.emscripten.revision.is_empty()
        || manifest.toolchain.static_hermes.revision.is_empty()
        || manifest.toolchain.static_hermes.flags.is_empty()
        || manifest.compiler.static_hermes_revision != manifest.toolchain.static_hermes.revision
    {
        return Err(invalid_entry(
            "capability-entry source, compiler, or local-profile identity is invalid",
        ));
    }

    let manifest_entries = capability_manifest_entry_units(
        manifest.schema_version,
        manifest.entry.as_ref(),
        manifest.entries.as_deref(),
    )?;
    let requires_explicit_symbol =
        manifest.schema_version == CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION;
    let mut entries = Vec::with_capacity(manifest_entries.len());
    let mut previous_entry_path: Option<&str> = None;
    let mut entry_ids = BTreeSet::new();
    let mut entry_symbols = BTreeSet::new();
    let mut entry_paths = BTreeSet::new();
    let mut module_paths = BTreeSet::new();
    for entry in manifest_entries {
        let validated = validate_capability_entry_manifest_unit(
            entry,
            requires_explicit_symbol,
            manifest.abi.entry_selector_abi_version,
        )?;
        if previous_entry_path.is_some_and(|previous| previous >= validated.entry_path.as_str())
            || !entry_ids.insert(validated.entry_id.clone())
            || !entry_symbols.insert(validated.entry_symbol.clone())
            || !entry_paths.insert(validated.entry_path.clone())
            || !module_paths.insert(validated.module_path.clone())
        {
            return Err(invalid_entry(
                "capability-entry entry units are unsorted or duplicated",
            ));
        }
        previous_entry_path = Some(&entry.entry_path);
        entries.push(validated);
    }
    if manifest.compiler.source_pipeline_sha256 != capability_source_pipeline_sha256(&entries) {
        return Err(invalid_entry(
            "capability-entry source pipeline differs from its local profiles",
        ));
    }
    let selected_entry = entries
        .iter()
        .find(|entry| entry.entry_id == expected_entry_id)
        .ok_or_else(|| invalid_entry("capability-entry package reference selects no entry unit"))?;
    if selected_entry.entry_path != expected_entry_path
        || expected_runtime_module_path.strip_suffix(".js")
            != Some(selected_entry.module_path.as_str())
    {
        return Err(invalid_entry(
            "capability-entry selected entry unit differs from the deployment export",
        ));
    }
    if manifest.engine.package != *expected_precompiler
        || manifest.engine.revision != runtime.wasmtime_revision
        || manifest.engine.target.triple != runtime.target_triple
        || manifest.engine.target.cpu != runtime.target_cpu
        || manifest.engine.compatibility_sha256 != runtime.engine_compatibility_sha256
        || manifest.engine.configuration_sha256 != runtime.engine_configuration_sha256
        || !manifest.engine.target.platform.is_object()
    {
        return Err(invalid_entry(
            "capability-entry engine identity is incompatible with the runtime",
        ));
    }
    let mut engine_config_bytes = Vec::new();
    write_canonical_json(
        &serde_json::to_value(&manifest.engine.config).expect("engine config serializes"),
        &mut engine_config_bytes,
    )
    .expect("writing engine config cannot fail");
    if sha256(&engine_config_bytes) != manifest.engine.configuration_sha256
        || validate_artifact_pipeline_kind(&manifest.pipeline.kind).is_err()
    {
        return Err(invalid_entry(
            "capability-entry engine configuration or pipeline identity is invalid",
        ));
    }
    let core_wasm = package_entry
        .artifacts
        .get("module.wasm")
        .expect("validated capability-entry artifact disappeared");
    let serialized_module = package_entry
        .artifacts
        .get("module.cwasm")
        .expect("validated capability-entry artifact disappeared");
    if manifest.artifacts.core_wasm.sha256 != core_wasm.sha256
        || manifest.artifacts.core_wasm.size != core_wasm.size
        || manifest.artifacts.serialized_module.sha256 != serialized_module.sha256
        || manifest.artifacts.serialized_module.size != serialized_module.size
    {
        return Err(invalid_entry(
            "capability-entry artifact identity differs from package metadata",
        ));
    }

    let route_entries =
        validate_capability_entry_route_table(manifest.schema_version, &entries, &manifest.routes)?;
    let mut selected_route = None;
    let mut routes = Vec::with_capacity(manifest.routes.len());
    for (route, entry) in manifest.routes.iter().zip(route_entries) {
        let route_identity = serde_json::json!({
            "domain": "convex-wasm-capability-route-v1",
            "entryId": entry.entry_id,
            "exportName": route.export_name,
            "udfKind": route.udf_kind,
            "visibility": route.visibility,
        });
        let mut route_identity_bytes = Vec::new();
        write_canonical_json(&route_identity, &mut route_identity_bytes)
            .expect("writing capability route identity cannot fail");
        let route_id = sha256(&route_identity_bytes);
        let entry_selector_id = capability_route_selector_id(
            manifest.schema_version,
            manifest.abi.entry_selector_abi_version,
            &route_id,
            &entry.entry_symbol,
            &route.export_name,
            route.udf_kind,
            entry.invocation_abi,
        )?;
        if route.route_id != route_id || route.entry_selector_id != entry_selector_id {
            return Err(invalid_entry(
                "capability-entry route or selector identity is invalid",
            ));
        }
        let route = CapabilityEntryRoute {
            entry_id: entry.entry_id.clone(),
            entry_selector_id,
            entry_symbol: entry.entry_symbol.clone(),
            export_name: route.export_name.clone(),
            invocation_abi: entry.invocation_abi,
            route_id,
            udf_kind: route.udf_kind,
            visibility: route.visibility.clone(),
        };
        if route.route_id == expected_route_id {
            if selected_route.replace(route.clone()).is_some() {
                return Err(invalid_entry(
                    "capability-entry package reference selects more than one route",
                ));
            }
        }
        routes.push(route);
    }
    if manifest.schema_version == CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION {
        validate_capability_entry_route_coverage(&entries, &routes)?;
    }
    let selected_route = selected_route
        .ok_or_else(|| invalid_entry("capability-entry package reference selects no route"))?;
    if selected_route.entry_id != expected_entry_id
        || selected_route.entry_selector_id != expected_entry_selector_id
        || selected_route.export_name != expected_export_name
        || selected_route.udf_kind != expected_udf_kind
        || selected_route.visibility != expected_visibility
    {
        return Err(invalid_entry(
            "capability-entry selected route differs from the deployment export",
        ));
    }
    Ok(ValidatedCapabilityEntryManifestIdentity {
        entries,
        routes,
        selected_route,
    })
}

fn validate_capability_entry_route_table<'a>(
    schema_version: u32,
    entries: &'a [CapabilityEntryRuntimeUnitIdentity],
    routes: &[CapabilityEntryManifestRoute],
) -> Result<Vec<&'a CapabilityEntryRuntimeUnitIdentity>, WasmUdfPackageError> {
    if routes.is_empty() {
        return Err(invalid_entry(
            "capability-entry manifest contains no routes",
        ));
    }
    let mut previous_route_key: Option<(&str, &str)> = None;
    let mut route_ids = BTreeSet::new();
    let mut selector_ids = BTreeSet::new();
    let mut route_entries = Vec::with_capacity(routes.len());
    for route in routes {
        validate_deployment_sha256("route.routeId", &route.route_id)?;
        validate_entry_selector_id("route.entrySelectorId", &route.entry_selector_id)?;
        validate_visibility(&route.visibility)?;
        let entry = capability_manifest_route_entry(schema_version, entries, route)?;
        let route_key = (entry.entry_path.as_str(), route.export_name.as_str());
        if route.export_name.is_empty()
            || route.export_name.chars().any(char::is_control)
            || previous_route_key.is_some_and(|previous| previous >= route_key)
            || !route_ids.insert(route.route_id.as_str())
            || !selector_ids.insert(route.entry_selector_id.as_str())
        {
            return Err(invalid_entry(
                "capability-entry routes are invalid, unsorted, or duplicated",
            ));
        }
        previous_route_key = Some(route_key);
        route_entries.push(entry);
    }
    Ok(route_entries)
}

fn capability_entry_value_codec(
    execution: &WasmUdfExecutionPolicy,
    expected_lowering_pipeline_sha256: &str,
) -> Result<GuestNativeJsonCodecIdentity, WasmUdfPackageError> {
    let value = execution
        .value_codec()
        .ok_or_else(|| invalid_entry("capability-entry value codec is missing"))?;
    let identity: GuestNativeJsonCodecIdentity =
        serde_json::from_value(value.clone()).map_err(|error| {
            invalid_entry(format!("capability-entry value codec is invalid: {error}"))
        })?;
    validate_guest_native_json_codec_identity(&identity, expected_lowering_pipeline_sha256)?;
    Ok(identity)
}

fn capability_entry_request_envelope(
    execution: &WasmUdfExecutionPolicy,
    expected_lowering_pipeline_sha256: &str,
) -> Result<CapabilityRequestEnvelopeIdentity, WasmUdfPackageError> {
    let value = execution
        .request_envelope()
        .ok_or_else(|| invalid_entry("capability-entry request envelope is missing"))?;
    let identity: CapabilityRequestEnvelopeIdentity = serde_json::from_value(value.clone())
        .map_err(|error| {
            invalid_entry(format!(
                "capability-entry request envelope is invalid: {error}"
            ))
        })?;
    validate_capability_request_envelope_identity(&identity, expected_lowering_pipeline_sha256)?;
    Ok(identity)
}

fn validate_capability_request_envelope_identity(
    identity: &CapabilityRequestEnvelopeIdentity,
    expected_lowering_pipeline_sha256: &str,
) -> Result<(), WasmUdfPackageError> {
    for (field, value) in [
        (
            "capability-entry request envelope canonical vector corpus SHA-256",
            identity.canonical_vector_corpus.sha256.as_str(),
        ),
        (
            "capability-entry request envelope canonical vector producer source SHA-256",
            identity
                .canonical_vector_corpus
                .producer
                .source_sha256
                .as_str(),
        ),
        (
            "capability-entry request envelope generated source SHA-256",
            identity.generated_source.sha256.as_str(),
        ),
        (
            "capability-entry request envelope lowering pipeline SHA-256",
            identity.lowering_pipeline_sha256.as_str(),
        ),
    ] {
        validate_sha256(field, value)?;
    }
    let identity_contract_matches = match identity.capability_request_abi_version {
        LEGACY_CAPABILITY_REQUEST_ABI_VERSION | PENDING_VALUE_CAPABILITY_REQUEST_ABI_VERSION => {
            identity.kind == LEGACY_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND
                && identity.canonical_vector_corpus.kind
                    == LEGACY_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND
                && identity.canonical_vector_corpus.producer.kind
                    == LEGACY_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND
        },
        PAGINATION_CAPABILITY_REQUEST_ABI_VERSION => {
            identity.kind == PAGINATION_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND
                && identity.canonical_vector_corpus.kind
                    == PAGINATION_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND
                && identity.canonical_vector_corpus.producer.kind
                    == PAGINATION_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND
        },
        CAPABILITY_REQUEST_ABI_VERSION => {
            (identity.kind == PRE_STORAGE_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND
                && identity.canonical_vector_corpus.kind
                    == PRE_STORAGE_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND
                && identity.canonical_vector_corpus.producer.kind
                    == PRE_STORAGE_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND)
                || (identity.kind == CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND
                    && identity.canonical_vector_corpus.kind
                        == CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND
                    && identity.canonical_vector_corpus.producer.kind
                        == CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND)
        },
        _ => false,
    };
    if identity.schema_version != 1
        || identity.canonical_vector_corpus.schema_version != 1
        || !identity_contract_matches
        || identity.generated_source.size == 0
        || identity.lowering_pipeline_sha256 != expected_lowering_pipeline_sha256
    {
        return Err(invalid_entry(
            "capability-entry request-envelope identity is unsupported or disagrees with the \
             compiler",
        ));
    }
    Ok(())
}

fn is_supported_capability_request_abi_version(version: u32) -> bool {
    matches!(
        version,
        LEGACY_CAPABILITY_REQUEST_ABI_VERSION
            | PENDING_VALUE_CAPABILITY_REQUEST_ABI_VERSION
            | PAGINATION_CAPABILITY_REQUEST_ABI_VERSION
            | CAPABILITY_REQUEST_ABI_VERSION
    )
}

fn validate_guest_native_json_codec_identity(
    identity: &GuestNativeJsonCodecIdentity,
    expected_lowering_pipeline_sha256: &str,
) -> Result<(), WasmUdfPackageError> {
    for (field, value) in [
        (
            "capability-entry value codec canonical vector corpus SHA-256",
            identity.canonical_vector_corpus.sha256.as_str(),
        ),
        (
            "capability-entry value codec canonical vector producer source SHA-256",
            identity
                .canonical_vector_corpus
                .producer
                .source_sha256
                .as_str(),
        ),
        (
            "capability-entry value codec generated source SHA-256",
            identity.generated_source.sha256.as_str(),
        ),
        (
            "capability-entry value codec lowering pipeline SHA-256",
            identity.lowering_pipeline_sha256.as_str(),
        ),
    ] {
        validate_sha256(field, value)?;
    }
    if identity.kind != GUEST_NATIVE_JSON_CODEC_IDENTITY_KIND
        || identity.schema_version != 1
        || identity.canonical_vector_corpus.kind != CANONICAL_CONVEX_VALUE_VECTOR_CORPUS_KIND
        || identity.canonical_vector_corpus.schema_version != 1
        || identity.canonical_vector_corpus.producer.kind
            != CANONICAL_CONVEX_VALUE_VECTOR_PRODUCER_KIND
        || identity.generated_source.size == 0
        || identity.lowering_pipeline_sha256 != expected_lowering_pipeline_sha256
    {
        return Err(invalid_entry(
            "capability-entry value codec identity is unsupported or disagrees with the compiler",
        ));
    }
    Ok(())
}

fn validate_capability_entry_value_codec_provenance(
    provenance: &GuestNativeJsonCodecIdentity,
    manifest: &GuestNativeJsonCodecIdentity,
) -> Result<(), WasmUdfPackageError> {
    if provenance != manifest {
        return Err(invalid_provenance(
            "capability-entry provenance value codec differs from the manifest",
        ));
    }
    Ok(())
}

fn validate_capability_entry_request_envelope_provenance(
    provenance: &CapabilityRequestEnvelopeIdentity,
    manifest: &CapabilityRequestEnvelopeIdentity,
) -> Result<(), WasmUdfPackageError> {
    if provenance != manifest {
        return Err(invalid_provenance(
            "capability-entry provenance request envelope differs from the manifest",
        ));
    }
    Ok(())
}

fn capability_selector_member_provenance(
    entry_selector_abi_version: u32,
    route: &CapabilityEntryRoute,
    include_entry_symbol: bool,
) -> Result<JsonValue, WasmUdfPackageError> {
    let invocation_abi = match (entry_selector_abi_version, route.invocation_abi) {
        (LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION, None) => None,
        (CAPABILITY_ENTRY_SELECTOR_ABI_VERSION, Some(invocation_abi)) => Some(invocation_abi),
        _ => {
            return Err(invalid_provenance(
                "capability selector invocation ABI disagrees with its selector ABI",
            ));
        },
    };
    let mut member = serde_json::Map::new();
    member.insert(
        "entrySelectorId".to_owned(),
        JsonValue::String(route.entry_selector_id.clone()),
    );
    if include_entry_symbol {
        member.insert(
            "entrySymbol".to_owned(),
            JsonValue::String(route.entry_symbol.clone()),
        );
    }
    member.insert(
        "handlerExportName".to_owned(),
        JsonValue::String(route.export_name.clone()),
    );
    member.insert(
        "handlerUdfKind".to_owned(),
        serde_json::to_value(route.udf_kind).expect("UDF kind serializes"),
    );
    if let Some(invocation_abi) = invocation_abi {
        member.insert(
            "invocationAbi".to_owned(),
            serde_json::to_value(invocation_abi).expect("invocation ABI serializes"),
        );
    }
    Ok(JsonValue::Object(member))
}

fn validate_legacy_capability_selector_provenance(
    selector_object: &JsonValue,
    entry_selector_abi_version: u32,
    application_entry_count: usize,
    routes: &[CapabilityEntryRoute],
    producer_implementation: &CapabilityProducerImplementationIdentity,
) -> Result<(), WasmUdfPackageError> {
    let selector = selector_object
        .as_object()
        .ok_or_else(|| invalid_provenance("capability selector identity is not an object"))?;
    let expected_members = routes
        .iter()
        .map(|route| capability_selector_member_provenance(entry_selector_abi_version, route, true))
        .collect::<Result<Vec<_>, _>>()?;
    let source_sha256 = selector
        .get("sourceSha256")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            invalid_provenance("capability selector identity omits its source SHA-256")
        })?;
    validate_sha256(
        "capability-entry provenance selector sourceSha256",
        source_sha256,
    )?;
    let selector_producer: CapabilityProducerImplementationIdentity =
        serde_json::from_value(selector.get("producerImplementation").cloned().ok_or_else(
            || invalid_provenance("capability selector identity omits its producer implementation"),
        )?)
        .map_err(|error| {
            invalid_provenance(format!(
                "capability selector producer implementation is invalid: {error}"
            ))
        })?;
    if selector.len() != 8
        || selector.get("abiVersion").and_then(JsonValue::as_u64)
            != Some(u64::from(entry_selector_abi_version))
        || selector
            .get("applicationEntryCount")
            .and_then(JsonValue::as_u64)
            != u64::try_from(application_entry_count).ok()
        || selector
            .get("applicationEntryCountSymbol")
            .and_then(JsonValue::as_str)
            != Some(CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL)
        || selector
            .get("applicationFactoryBySlotSymbol")
            .and_then(JsonValue::as_str)
            != Some(CAPABILITY_APPLICATION_FACTORY_BY_SLOT_SYMBOL)
        || selector.get("kind").and_then(JsonValue::as_str) != Some(CAPABILITY_ENTRY_MANIFEST_KIND)
        || selector.get("members") != Some(&JsonValue::Array(expected_members))
        || selector_producer != *producer_implementation
    {
        return Err(invalid_provenance(
            "capability selector identity differs from the authenticated entry-unit routes",
        ));
    }
    Ok(())
}

struct CapabilitySelectorExpectedIdentity<'a> {
    abi_version: u32,
    compile_flags: &'a [String],
    emscripten_llvm_revision: &'a str,
    emscripten_materials: &'a str,
    emscripten_revision: &'a str,
    partition_policy: &'a CohortPartitionPolicy,
    pipeline_kind: &'a str,
    producer_implementation: &'a CapabilityProducerImplementationIdentity,
    semantic_environment: &'a JsonValue,
}

fn capability_selector_shared_identity_matches(
    abi_version: u32,
    partition_policy: &CohortPartitionPolicy,
    pipeline_kind: &str,
    producer_implementation: &CapabilityProducerImplementationIdentity,
    semantic_environment: &JsonValue,
    toolchain: &CapabilitySelectorToolchainIdentity,
    expected: &CapabilitySelectorExpectedIdentity<'_>,
) -> bool {
    abi_version == expected.abi_version
        && partition_policy == expected.partition_policy
        && pipeline_kind == expected.pipeline_kind
        && producer_implementation == expected.producer_implementation
        && semantic_environment == expected.semantic_environment
        && toolchain.compile_flags == expected.compile_flags
        && toolchain.emscripten_materials == expected.emscripten_materials
        && toolchain.emscripten_revision == expected.emscripten_revision
        && toolchain.llvm_revision == expected.emscripten_llvm_revision
}

fn capability_selector_members_match(
    members: &[CapabilitySelectorMember],
    routes: &[CapabilityEntryRoute],
    entry_selector_abi_version: u32,
) -> bool {
    members.len() == routes.len()
        && members.iter().zip(routes).all(|(actual, expected)| {
            let invocation_abi = match (entry_selector_abi_version, expected.invocation_abi) {
                (CAPABILITY_ENTRY_SELECTOR_ABI_VERSION, Some(invocation_abi)) => invocation_abi,
                _ => return false,
            };
            actual.entry_selector_id == expected.entry_selector_id
                && actual.entry_symbol == expected.entry_symbol
                && actual.handler_export_name == expected.export_name
                && actual.handler_udf_kind == expected.udf_kind
                && actual.invocation_abi == invocation_abi
        })
}

fn validate_capability_per_entry_selector_provenance(
    selector_object: &JsonValue,
    application_entry_count: usize,
    routes: &[CapabilityEntryRoute],
    expected: &CapabilitySelectorExpectedIdentity<'_>,
) -> Result<(), WasmUdfPackageError> {
    let selector: CapabilityPerEntrySelectorIdentity =
        serde_json::from_value(selector_object.clone()).map_err(|error| {
            invalid_provenance(format!(
                "capability per-entry selector identity is invalid: {error}"
            ))
        })?;
    validate_sha256(
        "capability per-entry selector source SHA-256",
        &selector.source_sha256,
    )?;
    if selector.application_entry_count
        != u32::try_from(application_entry_count).unwrap_or(u32::MAX)
        || selector.application_entry_count_symbol != CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL
        || selector.application_factory_by_slot_symbol
            != CAPABILITY_APPLICATION_FACTORY_BY_SLOT_SYMBOL
        || selector.kind != CAPABILITY_ENTRY_MANIFEST_KIND
        || !capability_selector_members_match(&selector.members, routes, expected.abi_version)
        || !capability_selector_shared_identity_matches(
            selector.abi_version,
            &selector.partition_policy,
            &selector.pipeline_kind,
            &selector.producer_implementation,
            &selector.semantic_environment,
            &selector.toolchain,
            expected,
        )
    {
        return Err(invalid_provenance(
            "capability per-entry selector identity is inconsistent",
        ));
    }
    Ok(())
}

fn validate_capability_multi_entry_unit_selector_provenance(
    selector_object: &JsonValue,
    entries: &[CapabilityEntryRuntimeUnitIdentity],
    routes: &[CapabilityEntryRoute],
    application_unit: &CapabilityPhysicalApplicationUnitIdentity,
    expected: &CapabilitySelectorExpectedIdentity<'_>,
) -> Result<(), WasmUdfPackageError> {
    let selector: CapabilityMultiEntryUnitSelectorIdentity =
        serde_json::from_value(selector_object.clone()).map_err(|error| {
            invalid_provenance(format!(
                "capability physical application-unit selector identity is invalid: {error}"
            ))
        })?;
    validate_sha256(
        "capability physical application-unit selector source SHA-256",
        &selector.source_sha256,
    )?;
    if selector.application_entry_count != u32::try_from(entries.len()).unwrap_or(u32::MAX)
        || selector.application_entry_count_symbol != CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL
        || selector.application_factory_by_unit_slot_symbol
            != CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL
        || selector.application_unit != *application_unit
        || selector.application_unit_count != 1
        || selector.application_unit_count_symbol != CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL
        || selector.kind != CAPABILITY_ENTRY_MANIFEST_KIND
        || !capability_selector_members_match(&selector.members, routes, expected.abi_version)
        || !capability_selector_shared_identity_matches(
            selector.abi_version,
            &selector.partition_policy,
            &selector.pipeline_kind,
            &selector.producer_implementation,
            &selector.semantic_environment,
            &selector.toolchain,
            expected,
        )
    {
        return Err(invalid_provenance(
            "capability selector identity differs from the authenticated physical application unit",
        ));
    }
    Ok(())
}

struct CapabilityEntryProvenanceFields<'a> {
    artifact_pipeline_sha256: &'a str,
    engine_identity: &'a ProvenanceEngineIdentity,
    identities: &'a CapabilityEntryProvenanceIdentities,
    kind: &'a str,
    materials: &'a JsonValue,
    request_envelope: &'a CapabilityRequestEnvelopeIdentity,
    semantic_environment: &'a JsonValue,
    value_codec: &'a GuestNativeJsonCodecIdentity,
}

fn validate_capability_provenance_artifact(
    field: &str,
    artifact: &PackageArtifact,
) -> Result<(), WasmUdfPackageError> {
    if validate_sha256(field, &artifact.sha256).is_err() || artifact.size == 0 {
        return Err(invalid_provenance(format!(
            "{field} must contain a nonempty artifact with a valid SHA-256"
        )));
    }
    Ok(())
}

struct CapabilityFormatterExpectedIdentity<'a> {
    compile_flags: &'a [String],
    emscripten_llvm_revision: &'a str,
    emscripten_materials: &'a str,
    emscripten_revision: &'a str,
    pipeline_kind: &'a str,
    producer_implementation: &'a CapabilityProducerImplementationIdentity,
    runtime_headers: &'a str,
    semantic_environment: &'a JsonValue,
    static_hermes_materials: &'a str,
    static_hermes_revision: &'a str,
    unit_role: &'a str,
}

fn validate_capability_formatter_provenance(
    formatter: &CapabilityFormatterProvenance,
    expected: CapabilityFormatterExpectedIdentity<'_>,
) -> Result<(), WasmUdfPackageError> {
    validate_capability_provenance_artifact(
        "capability-entry formatter generated source",
        &formatter.generated_c.generated_source,
    )?;
    validate_capability_provenance_artifact(
        "capability-entry formatter source",
        &formatter.generated_source,
    )?;
    validate_capability_provenance_artifact(
        "capability-entry formatter generated C",
        &formatter.object.generated_c,
    )?;
    let generated_c_identity = serde_json::to_value(&formatter.generated_c)
        .expect("capability formatter generated C identity serializes");
    let mut generated_c_identity_bytes = Vec::new();
    write_canonical_json(&generated_c_identity, &mut generated_c_identity_bytes)
        .expect("writing capability formatter generated C identity cannot fail");
    if formatter.generated_c.exported_unit_name != "convex_wasm_console_formatter"
        || formatter.generated_c.flags.is_empty()
        || formatter
            .generated_c
            .flags
            .iter()
            .any(|flag| flag == "-typed")
        || formatter.generated_c.pipeline_kind != expected.pipeline_kind
        || formatter.object.pipeline_kind != expected.pipeline_kind
        || formatter.generated_c.producer_implementation != *expected.producer_implementation
        || formatter.object.producer_implementation != *expected.producer_implementation
        || formatter.object.compile_flags.as_slice() != expected.compile_flags
        || formatter.generated_c.semantic_environment != *expected.semantic_environment
        || formatter.object.semantic_environment != *expected.semantic_environment
        || formatter.generated_c.static_hermes.materials != expected.static_hermes_materials
        || formatter.generated_c.static_hermes.revision != expected.static_hermes_revision
        || formatter.object.emscripten.materials != expected.emscripten_materials
        || formatter.object.emscripten.llvm_revision != expected.emscripten_llvm_revision
        || formatter.object.emscripten.revision != expected.emscripten_revision
        || formatter.object.runtime_headers != expected.runtime_headers
        || formatter.generated_source != formatter.generated_c.generated_source
        || formatter.generated_c.unit_role != expected.unit_role
        || formatter.object.unit_role != expected.unit_role
        || validate_sha256(
            "capability-entry formatter generated C identity SHA-256",
            &formatter.object.generated_c_identity_sha256,
        )
        .is_err()
        || formatter.object.generated_c_identity_sha256 != sha256(&generated_c_identity_bytes)
    {
        return Err(invalid_provenance(
            "capability-entry console formatter provenance is inconsistent",
        ));
    }
    Ok(())
}

fn validate_capability_entry_provenance_fields(
    provenance: CapabilityEntryProvenanceFields<'_>,
    expected_kind: &str,
    manifest: &CapabilityEntryManifest,
) -> Result<(), WasmUdfPackageError> {
    let value_codec = capability_entry_value_codec(
        &manifest.execution,
        &manifest.compiler.lowering_pipeline_sha256,
    )?;
    validate_capability_entry_value_codec_provenance(provenance.value_codec, &value_codec)?;
    let request_envelope = capability_entry_request_envelope(
        &manifest.execution,
        &manifest.compiler.lowering_pipeline_sha256,
    )?;
    validate_capability_entry_request_envelope_provenance(
        provenance.request_envelope,
        &request_envelope,
    )?;
    validate_sha256(
        "capability-entry provenance artifactPipelineSha256",
        provenance.artifact_pipeline_sha256,
    )?;
    validate_sha256(
        "capability-entry provenance engineCompatibilitySha256",
        &provenance.engine_identity.engine_compatibility_sha256,
    )?;
    if provenance.kind != expected_kind
        || provenance.artifact_pipeline_sha256 != manifest.pipeline.artifact_pipeline_sha256
        || provenance.engine_identity.engine_compatibility_sha256
            != manifest.engine.compatibility_sha256
        || provenance.engine_identity.engine_config != manifest.engine.config
        || provenance.engine_identity.target.cpu != manifest.engine.target.cpu
        || provenance.engine_identity.target.triple != manifest.engine.target.triple
        || !provenance.identities.core_wasm.is_object()
        || !provenance.identities.runtime_main_object.is_object()
        || !provenance.identities.selector_object.is_object()
        || !provenance.identities.wasmtime_aot.is_object()
        || !provenance.materials.is_object()
        || !provenance.semantic_environment.is_object()
    {
        return Err(invalid_provenance(
            "capability-entry provenance differs from the authenticated manifest",
        ));
    }
    Ok(())
}

fn validate_split_schema_3_selector_provenance(
    selector: &JsonValue,
    application_unit: &CapabilityApplicationUnitProvenance,
    core_wasm: &CapabilitySplitCoreWasmIdentity,
    routes: &[CapabilityEntryRoute],
    pipeline_kind: &str,
    producer_implementation: &CapabilityProducerImplementationIdentity,
    semantic_environment: &JsonValue,
) -> Result<(), WasmUdfPackageError> {
    let selector: CapabilitySplitSchema3SelectorIdentity = serde_json::from_value(selector.clone())
        .map_err(|error| {
            invalid_provenance(format!(
                "capability singleton selector identity is invalid: {error}"
            ))
        })?;
    let expected_members = routes
        .iter()
        .map(|route| {
            capability_selector_member_provenance(
                core_wasm.abi.capability_entry_selector_abi_version,
                route,
                false,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_sha256(
        "capability-entry provenance selector sourceSha256",
        &selector.source_sha256,
    )?;
    if selector.abi_version != core_wasm.abi.capability_entry_selector_abi_version
        || selector.bucket != core_wasm.bucket
        || selector.entry_id != application_unit.entry_id
        || selector.entry_slot != application_unit.entry_slot
        || selector.entry_symbol != capability_entry_symbol(&application_unit.entry_id)
        || selector.kind != CAPABILITY_ENTRY_MANIFEST_KIND
        || selector.members != expected_members
        || selector.partition_policy != core_wasm.partition_policy
        || selector.pipeline_kind != pipeline_kind
        || selector.producer_implementation != *producer_implementation
        || selector.semantic_environment != *semantic_environment
        || selector.toolchain.compile_flags != core_wasm.emscripten.compile_flags
        || selector.toolchain.emscripten_materials != core_wasm.emscripten.materials
        || selector.toolchain.emscripten_revision != core_wasm.emscripten.revision
        || selector.toolchain.llvm_revision != core_wasm.emscripten.llvm_revision
        || selector.unit_role != "untyped-application-selector"
    {
        return Err(invalid_provenance(
            "capability selector identity differs from the authenticated singleton entry",
        ));
    }
    Ok(())
}

fn validate_capability_split_unit_table(
    provenance: &CapabilityEntryBuildProvenanceV2,
    entries: &[CapabilityEntryRuntimeUnitIdentity],
    runtime_support_role: &str,
) -> Result<(), WasmUdfPackageError> {
    let multi_entry_application_unit = provenance
        .application_units
        .first()
        .is_some_and(|unit| unit.application_unit_slot.is_some());
    if provenance.application_units.len() != entries.len()
        || provenance.bridge.generated_c.unit_role != "shared-typed-capability-bridge"
        || provenance.bridge.object.unit_role != "shared-typed-capability-bridge"
        || !provenance
            .bridge
            .generated_c
            .flags
            .iter()
            .any(|flag| flag == "-typed")
        || provenance.formatter.as_ref().is_some_and(|formatter| {
            formatter.generated_c.unit_role != runtime_support_role
                || formatter.object.unit_role != runtime_support_role
        })
    {
        return Err(invalid_provenance(
            "capability-entry split unit table has invalid cardinality or roles",
        ));
    }
    for (entry_slot, (application_unit, entry)) in
        provenance.application_units.iter().zip(entries).enumerate()
    {
        let generated_c = &application_unit.identities.generated_c;
        if usize::try_from(application_unit.entry_slot) != Ok(entry_slot)
            || application_unit.entry_id != entry.entry_id
            || (if multi_entry_application_unit {
                application_unit.application_unit_slot != Some(0)
                    || generated_c.capability_application_unit.is_none()
                    || generated_c.capability_entry.is_some()
            } else {
                application_unit.application_unit_slot.is_some()
                    || generated_c.capability_application_unit.is_some()
                    || generated_c.capability_entry.is_none()
            })
            || generated_c.unit_role != "untyped-application"
            || application_unit.identities.export_object.unit_role != "untyped-application"
            || generated_c.flags.is_empty()
            || generated_c.flags.iter().any(|flag| flag == "-typed")
        {
            return Err(invalid_provenance(
                "capability-entry application-unit slot, ID, or role is invalid",
            ));
        }
    }
    Ok(())
}

fn validate_physical_application_unit_identity(
    identity: &CapabilityPhysicalApplicationUnitIdentity,
    entries: &[CapabilityEntryRuntimeUnitIdentity],
) -> Result<(), WasmUdfPackageError> {
    validate_sha256(
        "capability physical application-unit identity SHA-256",
        &identity.identity_sha256,
    )?;
    let expected_exported_unit_name =
        format!("convex_wasm_application_unit_{}", identity.identity_sha256);
    if identity.kind != "convex-wasm-multi-entry-application-unit-v1"
        || identity.compiler_mode != "static-hermes-untyped-application"
        || identity.unit_count != 1
        || identity.entries.len() != entries.len()
        || identity.exported_unit_name != expected_exported_unit_name
        || identity.entry_symbol != format!("sh_export_{expected_exported_unit_name}")
        || identity.entries.iter().zip(entries).enumerate().any(
            |(handoff_slot, (physical_entry, logical_entry))| {
                usize::try_from(physical_entry.handoff_slot) != Ok(handoff_slot)
                    || physical_entry.entry_path != logical_entry.entry_path
            },
        )
    {
        return Err(invalid_provenance(
            "capability physical application-unit identity is inconsistent",
        ));
    }
    Ok(())
}

fn capability_application_static_hermes_flags(
    flags: &[String],
) -> Result<Vec<String>, WasmUdfPackageError> {
    let application_flags = flags
        .iter()
        .filter(|flag| flag.as_str() != "-typed")
        .cloned()
        .collect::<Vec<_>>();
    if application_flags.len() != flags.len().saturating_sub(1) {
        return Err(invalid_provenance(
            "capability application Static Hermes flags require exactly one typed marker",
        ));
    }
    Ok(application_flags)
}

fn validate_capability_static_hermes_precompile_process(
    process: &CapabilityStaticHermesPrecompileProcessIdentity,
    expected_normal_optimization_flag: &str,
) -> Result<(), WasmUdfPackageError> {
    if process.kind != CAPABILITY_STATIC_HERMES_PRECOMPILE_PROCESS_KIND
        || process
            .c_bundle_member_compilation_policy
            .as_ref()
            .is_none_or(|policy| {
                policy.kind != CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMPILATION_POLICY_KIND
                    || policy.normal_optimization_flag != expected_normal_optimization_flag
                    || policy.c_optimization_level_zero.c_optimization_level != 0
                    || policy.c_optimization_level_zero.function_count != 1
                    || policy.c_optimization_level_zero.optimization_flag != "-O0"
                    || policy.c_optimization_level_zero.role != "function"
                    || policy.c_optimization_level_zero.stage
                        != "c-optimization-level-zero-c-bundle-member-object"
            })
    {
        return Err(invalid_provenance(
            "capability-entry Static Hermes C bundle precompile process is invalid",
        ));
    }
    validate_sha256(
        "capability-entry Static Hermes C bundle compiler arguments SHA-256",
        &process.compiler_arguments_sha256,
    )?;
    validate_sha256(
        "capability-entry Static Hermes C bundle launcher arguments SHA-256",
        &process.launcher_arguments_sha256,
    )?;
    Ok(())
}

fn validate_capability_static_hermes_provenance_identity(
    identity: &CapabilityProvenanceStaticHermesIdentity,
    expected_materials: &str,
    expected_revision: &str,
    flags: &[String],
    expected_normal_optimization_flag: &str,
) -> Result<(), WasmUdfPackageError> {
    let c_bundle_enabled = capability_static_hermes_c_bundle_enabled(flags)?;
    if identity.materials != expected_materials
        || identity.revision != expected_revision
        || (c_bundle_enabled
            && identity
                .precompile_process
                .as_ref()
                .map(|process| {
                    validate_capability_static_hermes_precompile_process(
                        process,
                        expected_normal_optimization_flag,
                    )
                })
                .transpose()?
                .is_none())
        || (!c_bundle_enabled && identity.precompile_process.is_some())
    {
        return Err(invalid_provenance(
            "capability-entry Static Hermes provenance identity is inconsistent",
        ));
    }
    Ok(())
}

fn validate_capability_static_hermes_c_bundle_member_compilation(
    compilation: &CapabilityStaticHermesCBundleMemberCompilationIdentity,
    expected_stage: &str,
    expected_target_bytes: u32,
    expected_normal_optimization_flag: &str,
) -> Result<(), WasmUdfPackageError> {
    if compilation.kind != CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMMAND_KIND
        || compilation.member_compilations.len() < 2
        || compilation.member_compilations.len() >= CAPABILITY_STATIC_HERMES_C_BUNDLE_MAX_MEMBERS
    {
        return Err(invalid_provenance(
            "capability-entry Static Hermes C bundle member compilation is invalid",
        ));
    }
    let mut expected_link_arguments = vec!["-r".to_owned()];
    let mut member_paths = BTreeSet::new();
    let mut previous_function: Option<&CapabilityStaticHermesCBundleFunctionMemberIdentity> = None;
    for (index, member_compilation) in compilation.member_compilations.iter().enumerate() {
        let object_name = format!("member-{index:05}.o");
        expected_link_arguments.push(object_name.clone());
        let effective_arguments = &member_compilation.effective_arguments;
        let c_indexes = effective_arguments
            .iter()
            .enumerate()
            .filter_map(|(index, argument)| (argument == "-c").then_some(index))
            .collect::<Vec<_>>();
        let object_indexes = effective_arguments
            .iter()
            .enumerate()
            .filter_map(|(index, argument)| (argument == "-o").then_some(index))
            .collect::<Vec<_>>();
        let [c_index] = c_indexes.as_slice() else {
            return Err(invalid_provenance(
                "capability-entry Static Hermes C bundle member command must contain one -c",
            ));
        };
        let [object_index] = object_indexes.as_slice() else {
            return Err(invalid_provenance(
                "capability-entry Static Hermes C bundle member command must contain one -o",
            ));
        };
        let expected_path = match &member_compilation.member {
            CapabilityStaticHermesCBundleMemberIdentity::Metadata(member)
                if index == 0
                    && member.role == "metadata"
                    && member_compilation.optimization == expected_normal_optimization_flag
                    && member_compilation.stage == expected_stage =>
            {
                &member.path
            },
            CapabilityStaticHermesCBundleMemberIdentity::Function(member) => {
                let function_fragment = match (
                    member.function_fragment_count,
                    member.function_fragment_index,
                ) {
                    (None, None) => None,
                    (Some(count), Some(index))
                        if member.function_count == 1
                            && count > 0
                            && usize::try_from(count).ok().is_some_and(|count| {
                                count < CAPABILITY_STATIC_HERMES_C_BUNDLE_MAX_MEMBERS - 1
                            })
                            && index < count =>
                    {
                        Some((count, index))
                    },
                    _ => {
                        return Err(invalid_provenance(
                            "capability-entry Static Hermes C bundle function fragment is invalid",
                        ));
                    },
                };
                let expected_optimization = if member.c_optimization_level == Some(0) {
                    if member.function_count != 1 || function_fragment.is_some() {
                        return Err(invalid_provenance(
                            "capability-entry Static Hermes C bundle O0 function member is invalid",
                        ));
                    }
                    ("-O0", "c-optimization-level-zero-c-bundle-member-object")
                } else if member.c_optimization_level.is_none() {
                    (expected_normal_optimization_flag, expected_stage)
                } else {
                    return Err(invalid_provenance(
                        "capability-entry Static Hermes C bundle function optimization is invalid",
                    ));
                };
                let compiler_optimization_flags = effective_arguments
                    .iter()
                    .filter(|argument| {
                        matches!(
                            argument.as_str(),
                            "-O" | "-O0"
                                | "-O1"
                                | "-O2"
                                | "-O3"
                                | "-O4"
                                | "-O5"
                                | "-O6"
                                | "-O7"
                                | "-O8"
                                | "-O9"
                                | "-Og"
                                | "-Os"
                                | "-Oz"
                                | "-Ofast"
                        )
                    })
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                if index == 0
                    || member.role != "function"
                    || member.function_count == 0
                    || member.last_function_id
                        != member
                            .first_function_id
                            .checked_add(member.function_count - 1)
                            .ok_or_else(|| {
                                invalid_provenance(
                                    "capability-entry Static Hermes C bundle function range \
                                     overflows",
                                )
                            })?
                    || member.target_bytes != expected_target_bytes
                    || (member.oversize && member.function_count != 1)
                    || member_compilation.optimization != expected_optimization.0
                    || member_compilation.stage != expected_optimization.1
                    || compiler_optimization_flags != [expected_optimization.0]
                {
                    return Err(invalid_provenance(
                        "capability-entry Static Hermes C bundle function member is invalid",
                    ));
                }
                match previous_function {
                    None if member.first_function_id != 0
                        || function_fragment.is_some_and(|(_, index)| index != 0) =>
                    {
                        return Err(invalid_provenance(
                            "capability-entry Static Hermes C bundle function sequence does not \
                             start at zero",
                        ));
                    },
                    None => {},
                    Some(previous)
                        if previous.last_function_id.checked_add(1)
                            == Some(member.first_function_id) =>
                    {
                        if previous.function_fragment_index.is_some()
                            && previous.function_fragment_index
                                != previous.function_fragment_count.map(|count| count - 1)
                        {
                            return Err(invalid_provenance(
                                "capability-entry Static Hermes C bundle function fragment \
                                 sequence is incomplete",
                            ));
                        }
                        if function_fragment.is_some_and(|(_, index)| index != 0) {
                            return Err(invalid_provenance(
                                "capability-entry Static Hermes C bundle function fragment \
                                 sequence does not start at zero",
                            ));
                        }
                    },
                    Some(previous)
                        if member.first_function_id == previous.first_function_id
                            && member.last_function_id == previous.last_function_id =>
                    {
                        let Some((count, fragment_index)) = function_fragment else {
                            return Err(invalid_provenance(
                                "capability-entry Static Hermes C bundle repeated function range \
                                 lacks a fragment identity",
                            ));
                        };
                        if previous.function_fragment_count != Some(count)
                            || previous.function_fragment_index != fragment_index.checked_sub(1)
                        {
                            return Err(invalid_provenance(
                                "capability-entry Static Hermes C bundle function fragment \
                                 sequence is invalid",
                            ));
                        }
                    },
                    Some(_) => {
                        return Err(invalid_provenance(
                            "capability-entry Static Hermes C bundle function ranges are not \
                             contiguous and ordered",
                        ));
                    },
                }
                previous_function = Some(member);
                &member.path
            },
            _ => {
                return Err(invalid_provenance(
                    "capability-entry Static Hermes C bundle member identity is invalid",
                ));
            },
        };
        if !member_paths.insert(expected_path)
            || c_index >= object_index
            || effective_arguments.get(c_index + 1) != Some(expected_path)
            || effective_arguments.get(object_index + 1) != Some(&object_name)
        {
            return Err(invalid_provenance(
                "capability-entry Static Hermes C bundle member command differs from its identity",
            ));
        }
    }
    if previous_function
        .and_then(|member| {
            member
                .function_fragment_count
                .zip(member.function_fragment_index)
        })
        .is_some_and(|(count, index)| index != count - 1)
    {
        return Err(invalid_provenance(
            "capability-entry Static Hermes C bundle function fragment sequence is incomplete",
        ));
    }
    expected_link_arguments.extend(["-o".to_owned(), "unit.o".to_owned()]);
    if compilation.relocatable_link.executable.is_empty()
        || compilation.relocatable_link.arguments != expected_link_arguments
    {
        return Err(invalid_provenance(
            "capability-entry Static Hermes C bundle link command is invalid",
        ));
    }
    Ok(())
}

fn validate_capability_static_hermes_c_bundle_object_identity(
    compilation: Option<&CapabilityStaticHermesCBundleMemberCompilationIdentity>,
    flags: &[String],
    expected_stage: &str,
    expected_normal_optimization_flag: &str,
) -> Result<(), WasmUdfPackageError> {
    match (
        capability_static_hermes_c_bundle_shard_size(flags)?,
        compilation,
    ) {
        (Some(shard_size), Some(compilation)) => {
            validate_capability_static_hermes_c_bundle_member_compilation(
                compilation,
                expected_stage,
                shard_size,
                expected_normal_optimization_flag,
            )
        },
        (None, None) => Ok(()),
        _ => Err(invalid_provenance(
            "capability-entry Static Hermes C bundle object identity disagrees with flags",
        )),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CapabilityStaticHermesCBundleCompilationMember<'a> {
    generated_c: &'a CapabilityStaticHermesPrecompileProcessIdentity,
    object: &'a CapabilityStaticHermesCBundleMemberCompilationIdentity,
}

struct CapabilityStaticHermesCBundleCompilationApplication<'a> {
    generated_c: &'a CapabilityProvenanceStaticHermesIdentity,
    object: Option<&'a CapabilityStaticHermesCBundleMemberCompilationIdentity>,
    route_id: String,
}

fn capability_static_hermes_c_bundle_compilation_member<'a>(
    generated_c: &'a CapabilityProvenanceStaticHermesIdentity,
    object: Option<&'a CapabilityStaticHermesCBundleMemberCompilationIdentity>,
    component: &str,
) -> Result<CapabilityStaticHermesCBundleCompilationMember<'a>, WasmUdfPackageError> {
    Ok(CapabilityStaticHermesCBundleCompilationMember {
        generated_c: generated_c.precompile_process.as_ref().ok_or_else(|| {
            invalid_provenance(format!(
                "capability-entry Static Hermes C bundle {component} generated-C identity is \
                 missing its precompile process"
            ))
        })?,
        object: object.ok_or_else(|| {
            invalid_provenance(format!(
                "capability-entry Static Hermes C bundle {component} object identity is missing \
                 its member compilation"
            ))
        })?,
    })
}

fn validate_capability_static_hermes_c_bundle_compilation(
    static_hermes: &CapabilityStaticHermesIdentity,
    bridge: Option<CapabilityStaticHermesCBundleCompilationMember<'_>>,
    formatter: Option<CapabilityStaticHermesCBundleCompilationMember<'_>>,
    applications: &[CapabilityStaticHermesCBundleCompilationApplication<'_>],
) -> Result<(), WasmUdfPackageError> {
    let Some(c_bundle) = static_hermes.c_bundle.as_ref() else {
        return Ok(());
    };
    let applications = applications
        .iter()
        .map(|application| {
            let member = capability_static_hermes_c_bundle_compilation_member(
                application.generated_c,
                application.object,
                "application",
            )?;
            Ok(serde_json::json!({
                "generatedC": member.generated_c,
                "object": member.object,
                "routeId": application.route_id,
            }))
        })
        .collect::<Result<Vec<_>, WasmUdfPackageError>>()?;
    let mut identity = serde_json::Map::new();
    identity.insert("applications".to_owned(), JsonValue::Array(applications));
    if let Some(bridge) = bridge {
        identity.insert(
            "bridge".to_owned(),
            serde_json::json!({
                "generatedC": bridge.generated_c,
                "object": bridge.object,
            }),
        );
    }
    if let Some(formatter) = formatter {
        identity.insert(
            "formatter".to_owned(),
            serde_json::json!({
                "generatedC": formatter.generated_c,
                "object": formatter.object,
            }),
        );
    }
    identity.insert(
        "kind".to_owned(),
        JsonValue::String(CAPABILITY_STATIC_HERMES_C_BUNDLE_PACKAGE_COMPILATION_KIND.to_owned()),
    );
    if canonical_identity_sha256(&JsonValue::Object(identity))? != c_bundle.compilation_sha256 {
        return Err(invalid_provenance(
            "capability-entry Static Hermes C bundle compilation identity differs from the \
             manifest",
        ));
    }
    Ok(())
}

fn canonical_identity_sha256(value: &impl Serialize) -> Result<String, WasmUdfPackageError> {
    let value = serde_json::to_value(value).map_err(|error| {
        invalid_provenance(format!(
            "capability-entry identity does not serialize: {error}"
        ))
    })?;
    let mut bytes = Vec::new();
    write_canonical_json(&value, &mut bytes).map_err(|error| {
        invalid_provenance(format!(
            "capability-entry identity does not canonicalize: {error}"
        ))
    })?;
    Ok(sha256(&bytes))
}

fn is_normalized_official_output_chunk_dependency_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|component| !matches!(component, "" | "." | ".."))
}

fn is_normalized_official_output_chunk_dependency_specifier(specifier: &str) -> bool {
    if specifier.contains('\\') {
        return false;
    }
    let path = if let Some(path) = specifier.strip_prefix("./") {
        path
    } else if specifier.starts_with("../") {
        let mut components = specifier.split('/').peekable();
        while components.next_if_eq(&"..").is_some() {}
        return components.next().is_some_and(|first| {
            !matches!(first, "" | "." | "..")
                && !first.chars().any(char::is_control)
                && components.all(|component| {
                    !matches!(component, "" | "." | "..")
                        && !component.chars().any(char::is_control)
                })
        });
    } else {
        return false;
    };
    !path.is_empty()
        && !path.contains('\\')
        && path.split('/').all(|component| {
            !matches!(component, "" | "." | "..") && !component.chars().any(char::is_control)
        })
}

fn validate_capability_official_output_chunk_descriptor(
    descriptor: &CapabilityOfficialOutputChunkApplicationDescriptor,
    manifest_entries: &[&CapabilityEntryIdentity],
    entries: &[CapabilityEntryRuntimeUnitIdentity],
    routes: &[CapabilityEntryRoute],
) -> Result<(), WasmUdfPackageError> {
    let chunk_slot_count = descriptor.initialization.chunk_slot_count;
    let entry_count = u32::try_from(descriptor.entries.len()).unwrap_or(u32::MAX);
    if descriptor.kind != CAPABILITY_OFFICIAL_OUTPUT_CHUNK_DESCRIPTOR_KIND
        || descriptor.initialization.kind != CAPABILITY_OFFICIAL_OUTPUT_CHUNK_INITIALIZATION_KIND
        || descriptor.native_descriptor.kind
            != CAPABILITY_OFFICIAL_OUTPUT_CHUNK_NATIVE_DESCRIPTOR_KIND
        || descriptor.native_descriptor.destruction != "destroy-store-on-any-initialization-failure"
        || descriptor.native_descriptor.initialization
            != "recursive-closed-literal-require-with-provisional-cjs-namespaces"
        || descriptor.native_descriptor.publication
            != "authenticated-selected-entry-wrapper-validation-after-selected-closure-initialization"
        || descriptor.native_descriptor.slots
            != "closed-numbered-namespace-slots-with-per-entry-publication-units"
        || descriptor.entries.is_empty()
        || descriptor.entries.len() != entries.len()
        || descriptor.units.len() < 2
        || descriptor.units.len() > CAPABILITY_OFFICIAL_OUTPUT_CHUNK_MAXIMUM_UNITS
        || chunk_slot_count == 0
        || descriptor.initialization.namespace_slot_count != chunk_slot_count
        || descriptor.initialization.entry_publication_unit_slots.len() != descriptor.entries.len()
        || descriptor.units.len()
            != usize::try_from(chunk_slot_count)
                .unwrap_or(usize::MAX)
                .saturating_add(descriptor.entries.len())
    {
        return Err(invalid_provenance(
            "capability-entry official-output chunk descriptor is invalid",
        ));
    }
    validate_sha256(
        "capability-entry official-output chunk descriptor identity SHA-256",
        &descriptor.identity_sha256,
    )?;
    let mut entry_chunk_slots = BTreeSet::new();
    for (slot, unit) in descriptor.units.iter().enumerate() {
        let slot = u32::try_from(slot).map_err(|_| {
            invalid_provenance("capability-entry official-output chunk slot exceeds u32")
        })?;
        validate_capability_provenance_artifact(
            "capability-entry official-output chunk JavaScript",
            &unit.javascript,
        )?;
        let entry_publication = slot >= chunk_slot_count;
        let publication_handoff_slot = if entry_publication {
            slot.checked_sub(chunk_slot_count).ok_or_else(|| {
                invalid_provenance("capability-entry official-output publication slot underflowed")
            })?
        } else {
            0
        };
        if unit.application_unit_slot != slot
            || unit.entry_symbol
                != format!(
                    "sh_export_convex_wasm_official_chunk_{}",
                    unit.identity_sha256
                )
            || unit.exported_unit_name
                != format!("convex_wasm_official_chunk_{}", unit.identity_sha256)
            || (entry_publication
                && (!unit.entry_publication
                    || unit.chunk_slot != -1
                    || !unit.dependencies.is_empty()
                    || unit.publication_handoff_slot
                        != i32::try_from(publication_handoff_slot).unwrap_or(i32::MAX)
                    || unit.kind != CAPABILITY_OFFICIAL_OUTPUT_CHUNK_ENTRY_PUBLICATION_UNIT_KIND))
            || (!entry_publication
                && (unit.entry_publication
                    || unit.chunk_slot != i32::try_from(slot).unwrap_or(i32::MAX)
                    || unit.publication_handoff_slot != -1
                    || unit.kind != CAPABILITY_OFFICIAL_OUTPUT_CHUNK_UNIT_KIND))
        {
            return Err(invalid_provenance(
                "capability-entry official-output chunk unit is invalid",
            ));
        }
        let mut dependency_specifier_slots = BTreeMap::new();
        for dependency in &unit.dependencies {
            if !matches!(
                dependency.kind.as_str(),
                "dynamic-import" | "import-statement"
            ) || dependency.slot >= chunk_slot_count
                || dependency.slot == slot
                || !is_normalized_official_output_chunk_dependency_path(&dependency.path)
                || !is_normalized_official_output_chunk_dependency_specifier(&dependency.specifier)
                || dependency_specifier_slots
                    .insert(dependency.specifier.as_str(), dependency.slot)
                    .is_some()
            {
                return Err(invalid_provenance(
                    "capability-entry official-output chunk dependency is invalid",
                ));
            }
        }
    }
    for (handoff_slot, ((descriptor_entry, manifest_entry), entry)) in descriptor
        .entries
        .iter()
        .zip(manifest_entries)
        .zip(entries)
        .enumerate()
    {
        let expected_routes = routes
            .iter()
            .filter(|route| route.entry_id == entry.entry_id)
            .collect::<Vec<_>>();
        if descriptor_entry.handoff_slot != u32::try_from(handoff_slot).unwrap_or(u32::MAX)
            || descriptor_entry.entry_path != entry.entry_path
            || descriptor_entry.module_path != entry.module_path
            || descriptor_entry.entry_module_path != format!("{}.js", entry.module_path)
            || descriptor_entry.dependency_graph_sha256
                != manifest_entry.local_profile.dependency_graph_sha256
            || descriptor_entry.entry_slot >= chunk_slot_count
            || !entry_chunk_slots.insert(descriptor_entry.entry_slot)
            || descriptor_entry.entry_publication_unit_slot
                != descriptor
                    .initialization
                    .entry_publication_unit_slots
                    .get(handoff_slot)
                    .copied()
                    .unwrap_or(u32::MAX)
            || descriptor_entry.entry_publication_unit_slot
                != chunk_slot_count.saturating_add(u32::try_from(handoff_slot).unwrap_or(u32::MAX))
            || descriptor_entry.routes.len() != expected_routes.len()
            || descriptor_entry
                .routes
                .iter()
                .zip(expected_routes)
                .any(|(actual, expected)| {
                    actual.export_name != expected.export_name
                        || actual.udf_kind != expected.udf_kind
                        || actual.visibility != expected.visibility
                })
        {
            return Err(invalid_provenance(
                "capability-entry official-output chunk entry is inconsistent",
            ));
        }
        let publication_unit = descriptor
            .units
            .get(
                usize::try_from(descriptor_entry.entry_publication_unit_slot).unwrap_or(usize::MAX),
            )
            .ok_or_else(|| {
                invalid_provenance(
                    "capability-entry official-output entry publication unit is missing",
                )
            })?;
        if !publication_unit.entry_publication
            || publication_unit.publication_handoff_slot
                != i32::try_from(descriptor_entry.handoff_slot).unwrap_or(i32::MAX)
        {
            return Err(invalid_provenance(
                "capability-entry official-output entry publication unit is inconsistent",
            ));
        }
    }
    if descriptor
        .initialization
        .entry_publication_unit_slots
        .iter()
        .enumerate()
        .any(|(handoff_slot, publication_slot)| {
            *publication_slot
                != chunk_slot_count.saturating_add(u32::try_from(handoff_slot).unwrap_or(u32::MAX))
                || *publication_slot >= chunk_slot_count.saturating_add(entry_count)
        })
    {
        return Err(invalid_provenance(
            "capability-entry official-output publication slots are inconsistent",
        ));
    }
    Ok(())
}

fn validate_capability_official_output_chunk_physical_unit_binding<'a>(
    compiled_application_unit_slot: u32,
    generated_unit: &CapabilityOfficialOutputChunkGeneratedCUnitIdentity,
    object_unit: &CapabilityOfficialOutputChunkObjectUnitIdentity,
    descriptor: &'a CapabilityOfficialOutputChunkApplicationDescriptor,
) -> Result<&'a CapabilityOfficialOutputChunkUnit, WasmUdfPackageError> {
    let unit = descriptor
        .units
        .get(usize::try_from(compiled_application_unit_slot).unwrap_or(usize::MAX))
        .ok_or_else(|| {
            invalid_provenance("official-output chunk compiled unit has no descriptor")
        })?;
    if unit.application_unit_slot != compiled_application_unit_slot
        || generated_unit.entry_publication != unit.entry_publication
        || generated_unit.identity_sha256 != unit.identity_sha256
        || generated_unit.kind != unit.kind
        || object_unit.identity_sha256 != unit.identity_sha256
    {
        return Err(invalid_provenance(
            "capability-entry official-output chunk unit provenance is inconsistent",
        ));
    }
    Ok(unit)
}

fn validate_capability_official_output_chunk_compiled_unit(
    compiled: &CapabilityOfficialOutputChunkCompiledUnit,
    descriptor: &CapabilityOfficialOutputChunkApplicationDescriptor,
    expected_flags: &[String],
    expected_materials: &str,
    expected_revision: &str,
    expected_compile_flags: &[String],
    expected_emscripten: &CapabilityEmscriptenIdentity,
    expected_runtime_headers: &str,
    expected_pipeline_kind: &str,
    expected_producer: &CapabilityProducerImplementationIdentity,
    expected_semantic_environment: &JsonValue,
    expected_execution: &WasmUdfExecutionPolicy,
    expected_opaque_value_abi_version: u32,
) -> Result<(), WasmUdfPackageError> {
    let generated_c = &compiled.identities.generated_c;
    let object = &compiled.identities.export_object;
    let generated_unit = generated_c
        .capability_chunk_application_unit
        .as_ref()
        .ok_or_else(|| {
            invalid_provenance("official-output chunk generated-C identity omits its unit")
        })?;
    let object_unit = object
        .capability_chunk_application_unit
        .as_ref()
        .ok_or_else(|| {
            invalid_provenance("official-output chunk object identity omits its unit")
        })?;
    let unit = validate_capability_official_output_chunk_physical_unit_binding(
        compiled.application_unit_slot,
        generated_unit,
        object_unit,
        descriptor,
    )?;
    let expected_unit_role = if unit.entry_publication {
        "untyped-official-entry-publication"
    } else {
        "untyped-official-chunk"
    };
    if generated_c.capability_application_unit.is_some()
        || generated_c.capability_entry.is_some()
        || generated_c.guest_source_provenance.is_some()
        || generated_c.exported_unit_name != unit.exported_unit_name
        || generated_c.generated_source != unit.javascript
        || generated_c.flags != expected_flags
        || generated_c.opaque_value_abi_version != expected_opaque_value_abi_version
        || object.opaque_value_abi_version != expected_opaque_value_abi_version
        || generated_c.value_mode != expected_execution.value_mode()
        || object.value_mode != expected_execution.value_mode()
        || object.effect_execution_mode != expected_execution.effect_execution_mode()
        || generated_c.pipeline_kind != expected_pipeline_kind
        || object.pipeline_kind != expected_pipeline_kind
        || generated_c.producer_implementation != *expected_producer
        || object.producer_implementation != *expected_producer
        || generated_c.semantic_environment != *expected_semantic_environment
        || object.semantic_environment != *expected_semantic_environment
        || object.compile_flags != expected_compile_flags
        || object.emscripten.llvm_revision != expected_emscripten.llvm_revision
        || object.emscripten.materials != expected_emscripten.materials_sha256
        || object.emscripten.revision != expected_emscripten.revision
        || object.runtime_headers != expected_runtime_headers
        || generated_c.unit_role != expected_unit_role
        || object.unit_role != expected_unit_role
    {
        return Err(invalid_provenance(
            "capability-entry official-output chunk unit provenance is inconsistent",
        ));
    }
    validate_capability_provenance_artifact(
        "capability-entry official-output chunk generated C",
        &object.generated_c,
    )?;
    validate_capability_provenance_artifact(
        "capability-entry official-output chunk source",
        &generated_c.generated_source,
    )?;
    validate_capability_static_hermes_provenance_identity(
        &generated_c.static_hermes,
        expected_materials,
        expected_revision,
        expected_flags,
        CAPABILITY_STATIC_HERMES_C_BUNDLE_COMPACT_OPTIMIZATION_FLAG,
    )?;
    validate_capability_static_hermes_c_bundle_object_identity(
        object.static_hermes_c_bundle_member_compilation.as_ref(),
        expected_flags,
        "export-object",
        CAPABILITY_STATIC_HERMES_C_BUNDLE_COMPACT_OPTIMIZATION_FLAG,
    )?;
    let generated_c_identity_sha256 =
        object
            .generated_c_identity_sha256
            .as_deref()
            .ok_or_else(|| {
                invalid_provenance(
                    "official-output chunk object identity omits generated-C identity SHA-256",
                )
            })?;
    validate_sha256(
        "capability-entry official-output chunk generated-C identity SHA-256",
        generated_c_identity_sha256,
    )?;
    if generated_c_identity_sha256 != canonical_identity_sha256(generated_c)? {
        return Err(invalid_provenance(
            "capability-entry official-output chunk generated-C identity is inconsistent",
        ));
    }
    Ok(())
}

fn validate_capability_official_output_chunk_entry_physical_provenance(
    application_units: &[CapabilityApplicationUnitProvenance],
    descriptor_entries: &[CapabilityOfficialOutputChunkEntry],
    entries: &[CapabilityEntryRuntimeUnitIdentity],
    compiled_units: &[CapabilityOfficialOutputChunkCompiledUnit],
) -> Result<(), WasmUdfPackageError> {
    if application_units.len() != entries.len() || descriptor_entries.len() != entries.len() {
        return Err(invalid_provenance(
            "capability-entry official-output entry publication provenance is inconsistent",
        ));
    }
    for (entry_slot, ((application_unit, descriptor_entry), entry)) in application_units
        .iter()
        .zip(descriptor_entries)
        .zip(entries)
        .enumerate()
    {
        let physical_unit_slot = usize::try_from(descriptor_entry.entry_slot).map_err(|_| {
            invalid_provenance(
                "capability-entry official-output entry publication provenance is inconsistent",
            )
        })?;
        let physical_unit = compiled_units.get(physical_unit_slot).ok_or_else(|| {
            invalid_provenance(
                "capability-entry official-output entry publication provenance is inconsistent",
            )
        })?;
        if physical_unit.application_unit_slot != descriptor_entry.entry_slot
            || usize::try_from(application_unit.entry_slot) != Ok(entry_slot)
            || application_unit.entry_id != entry.entry_id
            || application_unit.application_unit_slot != Some(descriptor_entry.entry_slot)
            || application_unit.identities != physical_unit.identities
        {
            return Err(invalid_provenance(
                "capability-entry official-output entry publication provenance is inconsistent",
            ));
        }
    }
    Ok(())
}

fn validate_capability_official_output_chunk_provenance(
    provenance: &CapabilityEntryBuildProvenanceV2,
    manifest: &CapabilityEntryManifest,
    entries: &[CapabilityEntryRuntimeUnitIdentity],
    routes: &[CapabilityEntryRoute],
) -> Result<(), WasmUdfPackageError> {
    let chunk_application = provenance.chunk_application.as_ref().ok_or_else(|| {
        invalid_provenance("capability-entry official-output chunk provenance is missing")
    })?;
    let producer_implementation =
        validate_capability_producer_identity(&provenance.producer_identity)?;
    let manifest_entries = capability_manifest_entry_units(
        manifest.schema_version,
        manifest.entry.as_ref(),
        manifest.entries.as_deref(),
    )?;
    let application_flags =
        capability_application_static_hermes_flags(&manifest.toolchain.static_hermes.flags)?;
    if manifest_entries.len() != entries.len()
        || producer_implementation != manifest.pipeline.producer_implementation
        || chunk_application.units.len() != chunk_application.descriptor.units.len()
    {
        return Err(invalid_provenance(
            "capability-entry official-output chunk provenance has inconsistent cardinality",
        ));
    }
    validate_capability_official_output_chunk_descriptor(
        &chunk_application.descriptor,
        &manifest_entries,
        entries,
        routes,
    )?;

    let bridge = &provenance.bridge;
    if bridge.generated_c.effect_execution_mode != manifest.execution.effect_execution_mode()
        || bridge.generated_c.exported_unit_name != "convex_wasm_capability_bridge"
        || bridge.generated_c.flags != manifest.toolchain.static_hermes.flags
        || bridge.generated_c.opaque_value_abi_version != manifest.abi.opaque_value_abi_version
        || bridge.generated_c.value_mode != manifest.execution.value_mode()
        || bridge.generated_c.pipeline_kind != manifest.pipeline.kind
        || bridge.object.pipeline_kind != manifest.pipeline.kind
        || bridge.generated_c.producer_implementation != producer_implementation
        || bridge.object.producer_implementation != producer_implementation
        || bridge.generated_c.request_envelope != provenance.request_envelope
        || bridge.generated_c.value_codec != provenance.value_codec
        || bridge.generated_c.semantic_environment != provenance.semantic_environment
        || bridge.object.semantic_environment != provenance.semantic_environment
        || bridge.object.emscripten.llvm_revision != manifest.toolchain.emscripten.llvm_revision
        || bridge.object.emscripten.materials != manifest.toolchain.emscripten.materials_sha256
        || bridge.object.emscripten.revision != manifest.toolchain.emscripten.revision
        || bridge.object.runtime_headers != manifest.runtime.headers_material_sha256
        || bridge.generated_c.unit_role != "shared-typed-capability-bridge"
        || bridge.object.unit_role != "shared-typed-capability-bridge"
    {
        return Err(invalid_provenance(
            "capability-entry official-output chunk bridge provenance is inconsistent",
        ));
    }
    validate_capability_provenance_artifact(
        "capability-entry official-output chunk bridge generated C",
        &bridge.object.generated_c,
    )?;
    validate_capability_provenance_artifact(
        "capability-entry official-output chunk bridge source",
        &bridge.generated_c.generated_source,
    )?;
    validate_capability_static_hermes_provenance_identity(
        &bridge.generated_c.static_hermes,
        &manifest.toolchain.static_hermes.materials_sha256,
        &manifest.toolchain.static_hermes.revision,
        &manifest.toolchain.static_hermes.flags,
        CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
    )?;
    validate_capability_static_hermes_c_bundle_object_identity(
        bridge
            .object
            .static_hermes_c_bundle_member_compilation
            .as_ref(),
        &manifest.toolchain.static_hermes.flags,
        "capability-bridge-object",
        CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
    )?;

    let formatter = provenance.formatter.as_ref().ok_or_else(|| {
        invalid_provenance("capability-entry official-output chunk formatter provenance is missing")
    })?;
    validate_capability_formatter_provenance(
        formatter,
        CapabilityFormatterExpectedIdentity {
            compile_flags: &bridge.object.compile_flags,
            emscripten_llvm_revision: &manifest.toolchain.emscripten.llvm_revision,
            emscripten_materials: &manifest.toolchain.emscripten.materials_sha256,
            emscripten_revision: &manifest.toolchain.emscripten.revision,
            pipeline_kind: &manifest.pipeline.kind,
            producer_implementation: &producer_implementation,
            runtime_headers: &manifest.runtime.headers_material_sha256,
            semantic_environment: &provenance.semantic_environment,
            static_hermes_materials: &manifest.toolchain.static_hermes.materials_sha256,
            static_hermes_revision: &manifest.toolchain.static_hermes.revision,
            unit_role: "shared-untyped-runtime-support",
        },
    )?;
    validate_capability_static_hermes_provenance_identity(
        &formatter.generated_c.static_hermes,
        &manifest.toolchain.static_hermes.materials_sha256,
        &manifest.toolchain.static_hermes.revision,
        &application_flags,
        CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
    )?;
    validate_capability_static_hermes_c_bundle_object_identity(
        formatter
            .object
            .static_hermes_c_bundle_member_compilation
            .as_ref(),
        &application_flags,
        "console-formatter-object",
        CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
    )?;

    for (slot, compiled) in chunk_application.units.iter().enumerate() {
        if compiled.application_unit_slot != u32::try_from(slot).unwrap_or(u32::MAX) {
            return Err(invalid_provenance(
                "capability-entry official-output chunk compiled units are unordered",
            ));
        }
        validate_capability_official_output_chunk_compiled_unit(
            compiled,
            &chunk_application.descriptor,
            &application_flags,
            &manifest.toolchain.static_hermes.materials_sha256,
            &manifest.toolchain.static_hermes.revision,
            &bridge.object.compile_flags,
            &manifest.toolchain.emscripten,
            &manifest.runtime.headers_material_sha256,
            &manifest.pipeline.kind,
            &producer_implementation,
            &provenance.semantic_environment,
            &manifest.execution,
            manifest.abi.opaque_value_abi_version,
        )?;
    }
    if manifest.toolchain.static_hermes.c_bundle.is_some() {
        let bridge_compilation = capability_static_hermes_c_bundle_compilation_member(
            &bridge.generated_c.static_hermes,
            bridge
                .object
                .static_hermes_c_bundle_member_compilation
                .as_ref(),
            "bridge",
        )?;
        let formatter_compilation = capability_static_hermes_c_bundle_compilation_member(
            &formatter.generated_c.static_hermes,
            formatter
                .object
                .static_hermes_c_bundle_member_compilation
                .as_ref(),
            "formatter",
        )?;
        let application_compilations = chunk_application
            .units
            .iter()
            .zip(&chunk_application.descriptor.units)
            .map(
                |(compiled, unit)| CapabilityStaticHermesCBundleCompilationApplication {
                    generated_c: &compiled.identities.generated_c.static_hermes,
                    object: compiled
                        .identities
                        .export_object
                        .static_hermes_c_bundle_member_compilation
                        .as_ref(),
                    route_id: format!("official-output-physical-unit:{}", unit.identity_sha256),
                },
            )
            .collect::<Vec<_>>();
        validate_capability_static_hermes_c_bundle_compilation(
            &manifest.toolchain.static_hermes,
            Some(bridge_compilation),
            Some(formatter_compilation),
            &application_compilations,
        )?;
    }

    validate_capability_official_output_chunk_entry_physical_provenance(
        &provenance.application_units,
        &chunk_application.descriptor.entries,
        entries,
        &chunk_application.units,
    )?;

    let core_wasm: CapabilityOfficialOutputChunkCoreWasmIdentity =
        serde_json::from_value(provenance.identities.core_wasm.clone()).map_err(|error| {
            invalid_provenance(format!(
                "capability-entry official-output core Wasm identity is invalid: {error}"
            ))
        })?;
    validate_capability_partition_policy(&core_wasm.partition_policy, core_wasm.bucket)?;
    if core_wasm.abi.capability_entry_selector_abi_version
        != manifest.abi.entry_selector_abi_version
        || core_wasm.abi.capability_request_abi_version
            != manifest.abi.capability_request_abi_version
        || core_wasm.abi.opaque_value_abi_version != manifest.abi.opaque_value_abi_version
        || core_wasm.pipeline_kind != manifest.pipeline.kind
        || core_wasm.producer_implementation != producer_implementation
        || core_wasm.semantic_environment != provenance.semantic_environment
        || core_wasm.target.cpu != manifest.engine.target.cpu
        || core_wasm.target.triple != manifest.engine.target.triple
        || core_wasm.emscripten.compile_flags != bridge.object.compile_flags
        || core_wasm.emscripten.llvm_revision != manifest.toolchain.emscripten.llvm_revision
        || core_wasm.emscripten.materials != manifest.toolchain.emscripten.materials_sha256
        || core_wasm.emscripten.revision != manifest.toolchain.emscripten.revision
        || core_wasm.runtime.archives != manifest.runtime.archives_material_sha256
        || core_wasm.runtime.main_object != manifest.runtime.main_object
        || core_wasm.link_flags.is_empty()
        || core_wasm.static_hermes.application_flags != application_flags
        || core_wasm.static_hermes.bridge_flags != manifest.toolchain.static_hermes.flags
        || core_wasm.static_hermes.formatter_flags.as_deref() != Some(application_flags.as_slice())
        || core_wasm.static_hermes.materials != manifest.toolchain.static_hermes.materials_sha256
        || core_wasm.static_hermes.revision != manifest.toolchain.static_hermes.revision
        || core_wasm.unit_topology.kind != CAPABILITY_OFFICIAL_OUTPUT_CHUNK_TOPOLOGY_KIND
        || core_wasm.unit_topology.application_entry_count
            != u32::try_from(entries.len()).unwrap_or(u32::MAX)
        || core_wasm.unit_topology.application_entry_count_symbol
            != CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL
        || core_wasm
            .unit_topology
            .application_factory_by_unit_slot_symbol
            != CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL
        || core_wasm.unit_topology.application_flags != application_flags
        || core_wasm.unit_topology.application_unit_count
            != u32::try_from(chunk_application.units.len()).unwrap_or(u32::MAX)
        || core_wasm.unit_topology.application_unit_count_symbol
            != CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL
        || core_wasm.unit_topology.application_unit_maximum
            != u32::try_from(CAPABILITY_OFFICIAL_OUTPUT_CHUNK_MAXIMUM_UNITS).unwrap_or(u32::MAX)
        || core_wasm.unit_topology.chunk_application != chunk_application.descriptor
        || core_wasm.unit_topology.bridge.entry_symbol != "sh_export_convex_wasm_capability_bridge"
        || core_wasm.unit_topology.bridge.generated_source != bridge.generated_c.generated_source
        || core_wasm.unit_topology.bridge_flags != bridge.generated_c.flags
        || core_wasm.unit_topology.formatter.entry_symbol
            != "sh_export_convex_wasm_console_formatter"
        || core_wasm.unit_topology.formatter.generated_source
            != formatter.generated_c.generated_source
        || core_wasm.unit_topology.formatter_flags != formatter.generated_c.flags
        || core_wasm.unit_topology.selected_entry_preparation
            != CAPABILITY_SELECTED_ENTRY_PREPARATION
        || core_wasm.unit_topology.initialization_order
            != [
                "shared-typed-capability-bridge",
                "shared-untyped-runtime-support",
                "recursive-authenticated-selected-entry-chunk-closure",
                "authenticated-selected-entry-publication-after-selected-closure-initialization",
            ]
    {
        return Err(invalid_provenance(
            "capability-entry official-output core Wasm topology is inconsistent",
        ));
    }
    validate_capability_provenance_artifact(
        "capability-entry official-output core Wasm bridge object",
        &core_wasm.unit_topology.bridge.object,
    )?;
    validate_capability_provenance_artifact(
        "capability-entry official-output core Wasm formatter object",
        &core_wasm.unit_topology.formatter.object,
    )?;
    if core_wasm.application_units.len() != chunk_application.units.len()
        || core_wasm.members.len() != entries.len()
    {
        return Err(invalid_provenance(
            "capability-entry official-output core Wasm unit cardinality is inconsistent",
        ));
    }
    for ((core_unit, compiled), descriptor_unit) in core_wasm
        .application_units
        .iter()
        .zip(&chunk_application.units)
        .zip(&chunk_application.descriptor.units)
    {
        if core_unit.application_unit_slot != compiled.application_unit_slot
            || core_unit.application_unit != *descriptor_unit
            || core_unit.generated_c != compiled.identities.export_object.generated_c
            || core_unit.identities != compiled.identities
        {
            return Err(invalid_provenance(
                "capability-entry official-output core Wasm unit identity is inconsistent",
            ));
        }
        validate_capability_provenance_artifact(
            "capability-entry official-output core Wasm application object",
            &core_unit.object,
        )?;
    }
    for (entry_slot, (member, entry)) in core_wasm.members.iter().zip(entries).enumerate() {
        let descriptor_entry = &chunk_application.descriptor.entries[entry_slot];
        let expected_routes = routes
            .iter()
            .filter(|route| route.entry_id == entry.entry_id)
            .collect::<Vec<_>>();
        if member.application_unit_slot != Some(descriptor_entry.entry_slot)
            || usize::try_from(member.entry_slot) != Ok(entry_slot)
            || member.entry_id != entry.entry_id
            || member.entry_symbol != entry.entry_symbol
            || member.invocation_abi != entry.invocation_abi
            || member.effect_execution_mode != manifest.execution.effect_execution_mode()
            || member.value_mode != manifest.execution.value_mode()
            || member.runtime_surface_policy_sha256
                != manifest.pipeline.runtime_surface_policy_sha256
            || member.routes.len() != expected_routes.len()
            || member
                .routes
                .iter()
                .zip(expected_routes)
                .any(|(actual, expected)| {
                    actual.entry_selector_id != expected.entry_selector_id
                        || actual.export_name != expected.export_name
                        || actual.route_id != expected.route_id
                        || actual.udf_kind != expected.udf_kind
                        || actual.visibility != expected.visibility
                })
        {
            return Err(invalid_provenance(
                "capability-entry official-output core Wasm member is inconsistent",
            ));
        }
        validate_capability_provenance_artifact(
            "capability-entry official-output core Wasm member object",
            &member.object,
        )?;
    }

    let runtime_main: CapabilityOfficialOutputChunkRuntimeMainIdentity = serde_json::from_value(
        provenance.identities.runtime_main_object.clone(),
    )
    .map_err(|error| {
        invalid_provenance(format!(
            "capability-entry official-output runtime main identity is invalid: {error}"
        ))
    })?;
    if runtime_main.cohort_entry_selector_abi_version != COHORT_ENTRY_SELECTOR_ABI_VERSION
        || !capability_runtime_main_compile_flags_match(
            &core_wasm.emscripten.compile_flags,
            &runtime_main.compile_flags,
            Some(CHUNK_APPLICATION_UNIT_COMPILE_FLAG),
        )
        || runtime_main.effect_execution_mode != manifest.execution.effect_execution_mode()
        || runtime_main.emscripten.llvm_revision != manifest.toolchain.emscripten.llvm_revision
        || runtime_main.emscripten.materials != manifest.toolchain.emscripten.materials_sha256
        || runtime_main.emscripten.revision != manifest.toolchain.emscripten.revision
        || runtime_main.entry_selector_symbol != "convex_wasm_selected_exported_unit"
        || runtime_main.opaque_value_abi_version != manifest.abi.opaque_value_abi_version
        || runtime_main.pipeline_kind != manifest.pipeline.kind
        || runtime_main.producer_implementation != producer_implementation
        || runtime_main.runtime_headers != manifest.runtime.headers_material_sha256
        || runtime_main.runtime_main != manifest.runtime.main_material_sha256
        || runtime_main.runtime_main_language != "c++"
        || runtime_main.semantic_environment != provenance.semantic_environment
        || runtime_main.unit_topology.kind != CAPABILITY_OFFICIAL_OUTPUT_CHUNK_TOPOLOGY_KIND
        || runtime_main.unit_topology.application_entry_count
            != core_wasm.unit_topology.application_entry_count
        || runtime_main.unit_topology.application_entry_count_symbol
            != core_wasm.unit_topology.application_entry_count_symbol
        || runtime_main
            .unit_topology
            .application_factory_by_unit_slot_symbol
            != core_wasm
                .unit_topology
                .application_factory_by_unit_slot_symbol
        || runtime_main.unit_topology.application_selector_symbol
            != "convex_wasm_selected_exported_unit"
        || runtime_main.unit_topology.application_unit_count
            != core_wasm.unit_topology.application_unit_count
        || runtime_main.unit_topology.application_unit_count_symbol
            != core_wasm.unit_topology.application_unit_count_symbol
        || runtime_main.unit_topology.application_unit_maximum
            != u32::try_from(CAPABILITY_OFFICIAL_OUTPUT_CHUNK_MAXIMUM_UNITS).unwrap_or(u32::MAX)
        || runtime_main.unit_topology.bridge_entry_symbol
            != core_wasm.unit_topology.bridge.entry_symbol
        || runtime_main.unit_topology.bridge_exported_unit_name != "convex_wasm_capability_bridge"
        || runtime_main.unit_topology.formatter_entry_symbol
            != core_wasm.unit_topology.formatter.entry_symbol
        || runtime_main.unit_topology.formatter_exported_unit_name
            != "convex_wasm_console_formatter"
        || !capability_official_output_chunk_selected_entry_preparation_matches(
            &core_wasm.unit_topology,
            &runtime_main.unit_topology,
        )
        || runtime_main.unit_topology.initialization_order
            != core_wasm.unit_topology.initialization_order
        || runtime_main.unit_topology.native_descriptor
            != chunk_application.descriptor.native_descriptor
    {
        return Err(invalid_provenance(
            "capability-entry official-output runtime main topology is inconsistent",
        ));
    }

    let selector: CapabilityOfficialOutputChunkSelectorIdentity =
        serde_json::from_value(provenance.identities.selector_object.clone()).map_err(|error| {
            invalid_provenance(format!(
                "capability-entry official-output selector identity is invalid: {error}"
            ))
        })?;
    let selector_expected = CapabilitySelectorExpectedIdentity {
        abi_version: manifest.abi.entry_selector_abi_version,
        compile_flags: &core_wasm.emscripten.compile_flags,
        emscripten_llvm_revision: &manifest.toolchain.emscripten.llvm_revision,
        emscripten_materials: &manifest.toolchain.emscripten.materials_sha256,
        emscripten_revision: &manifest.toolchain.emscripten.revision,
        partition_policy: &core_wasm.partition_policy,
        pipeline_kind: &manifest.pipeline.kind,
        producer_implementation: &producer_implementation,
        semantic_environment: &provenance.semantic_environment,
    };
    if selector.application_entry_count != u32::try_from(entries.len()).unwrap_or(u32::MAX)
        || selector.application_entry_count_symbol != CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL
        || selector.application_factory_by_unit_slot_symbol
            != CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL
        || selector.application_unit_count
            != u32::try_from(chunk_application.units.len()).unwrap_or(u32::MAX)
        || selector.application_unit_count_symbol != CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL
        || selector.chunk_application != chunk_application.descriptor
        || selector.kind != CAPABILITY_ENTRY_MANIFEST_KIND
        || !capability_selector_members_match(
            &selector.members,
            routes,
            selector_expected.abi_version,
        )
        || !capability_selector_shared_identity_matches(
            selector.abi_version,
            &selector.partition_policy,
            &selector.pipeline_kind,
            &selector.producer_implementation,
            &selector.semantic_environment,
            &selector.toolchain,
            &selector_expected,
        )
    {
        return Err(invalid_provenance(
            "capability-entry official-output selector identity is inconsistent",
        ));
    }
    validate_sha256(
        "capability-entry official-output selector source SHA-256",
        &selector.source_sha256,
    )?;

    let wasmtime_aot: CapabilityWasmtimeAotIdentity =
        serde_json::from_value(provenance.identities.wasmtime_aot.clone()).map_err(|error| {
            invalid_provenance(format!(
                "capability-entry official-output Wasmtime AOT identity is invalid: {error}"
            ))
        })?;
    validate_capability_provenance_artifact(
        "capability-entry official-output Wasmtime AOT core Wasm",
        &wasmtime_aot.core_wasm,
    )?;
    if validate_sha256(
        "capability-entry official-output Wasmtime AOT materials SHA-256",
        &wasmtime_aot.wasmtime.materials,
    )
    .is_err()
        || wasmtime_aot.core_wasm != manifest.artifacts.core_wasm
        || wasmtime_aot.engine_config != manifest.engine.config
        || wasmtime_aot.pipeline_kind != manifest.pipeline.kind
        || wasmtime_aot.producer_implementation != producer_implementation
        || wasmtime_aot.semantic_environment != provenance.semantic_environment
        || wasmtime_aot.target.cpu != manifest.engine.target.cpu
        || wasmtime_aot.target.platform != manifest.engine.target.platform
        || wasmtime_aot.target.triple != manifest.engine.target.triple
        || wasmtime_aot.wasmtime.package != manifest.engine.package
        || wasmtime_aot.wasmtime.revision != manifest.engine.revision
    {
        return Err(invalid_provenance(
            "capability-entry official-output Wasmtime AOT provenance is inconsistent",
        ));
    }
    Ok(())
}

fn validate_capability_split_provenance(
    provenance: &CapabilityEntryBuildProvenanceV2,
    manifest: &CapabilityEntryManifest,
    entries: &[CapabilityEntryRuntimeUnitIdentity],
    routes: &[CapabilityEntryRoute],
) -> Result<(), WasmUdfPackageError> {
    if provenance.chunk_application.is_some() {
        return validate_capability_official_output_chunk_provenance(
            provenance, manifest, entries, routes,
        );
    }
    let producer_implementation =
        validate_capability_producer_identity(&provenance.producer_identity)?;
    if producer_implementation != manifest.pipeline.producer_implementation {
        return Err(invalid_provenance(
            "capability-entry producer identity differs from the manifest",
        ));
    }
    let manifest_entries = capability_manifest_entry_units(
        manifest.schema_version,
        manifest.entry.as_ref(),
        manifest.entries.as_deref(),
    )?;
    let runtime_support_role =
        capability_runtime_support_unit_role(manifest.abi.entry_selector_abi_version)?;
    validate_capability_split_unit_table(provenance, entries, runtime_support_role)?;
    let multi_entry_application_unit = provenance
        .application_units
        .first()
        .is_some_and(|unit| unit.application_unit_slot.is_some());
    if manifest_entries.len() != entries.len() {
        return Err(invalid_provenance(
            "capability-entry application-unit provenance has the wrong cardinality",
        ));
    }

    let bridge = &provenance.bridge;
    validate_capability_provenance_artifact(
        "capability-entry bridge generated C",
        &bridge.object.generated_c,
    )?;
    validate_capability_provenance_artifact(
        "capability-entry bridge generated source",
        &bridge.generated_c.generated_source,
    )?;
    if bridge.generated_c.unit_role != "shared-typed-capability-bridge"
        || bridge.object.unit_role != "shared-typed-capability-bridge"
        || bridge.generated_c.exported_unit_name != "convex_wasm_capability_bridge"
        || bridge.generated_c.flags != manifest.toolchain.static_hermes.flags
        || !bridge.generated_c.flags.iter().any(|flag| flag == "-typed")
        || bridge.generated_c.opaque_value_abi_version != manifest.abi.opaque_value_abi_version
        || bridge.generated_c.value_mode != manifest.execution.value_mode()
        || bridge.generated_c.effect_execution_mode != manifest.execution.effect_execution_mode()
        || bridge.generated_c.pipeline_kind != manifest.pipeline.kind
        || bridge.object.pipeline_kind != manifest.pipeline.kind
        || bridge.generated_c.producer_implementation != producer_implementation
        || bridge.object.producer_implementation != producer_implementation
        || bridge.generated_c.request_envelope != provenance.request_envelope
        || bridge.generated_c.value_codec != provenance.value_codec
        || bridge.generated_c.semantic_environment != provenance.semantic_environment
        || bridge.object.semantic_environment != provenance.semantic_environment
        || bridge.generated_c.static_hermes.materials
            != manifest.toolchain.static_hermes.materials_sha256
        || bridge.generated_c.static_hermes.revision != manifest.toolchain.static_hermes.revision
        || bridge.object.emscripten.materials != manifest.toolchain.emscripten.materials_sha256
        || bridge.object.emscripten.llvm_revision != manifest.toolchain.emscripten.llvm_revision
        || bridge.object.emscripten.revision != manifest.toolchain.emscripten.revision
        || bridge.object.runtime_headers != manifest.runtime.headers_material_sha256
    {
        return Err(invalid_provenance(
            "capability-entry bridge provenance is inconsistent",
        ));
    }
    if let Some(formatter) = &provenance.formatter {
        validate_capability_formatter_provenance(
            formatter,
            CapabilityFormatterExpectedIdentity {
                compile_flags: &bridge.object.compile_flags,
                emscripten_llvm_revision: &manifest.toolchain.emscripten.llvm_revision,
                emscripten_materials: &manifest.toolchain.emscripten.materials_sha256,
                emscripten_revision: &manifest.toolchain.emscripten.revision,
                pipeline_kind: &manifest.pipeline.kind,
                producer_implementation: &producer_implementation,
                runtime_headers: &manifest.runtime.headers_material_sha256,
                semantic_environment: &provenance.semantic_environment,
                static_hermes_materials: &manifest.toolchain.static_hermes.materials_sha256,
                static_hermes_revision: &manifest.toolchain.static_hermes.revision,
                unit_role: runtime_support_role,
            },
        )?;
    }

    for (entry_slot, ((application_unit, manifest_entry), entry)) in provenance
        .application_units
        .iter()
        .zip(manifest_entries)
        .zip(entries)
        .enumerate()
    {
        let generated_c = &application_unit.identities.generated_c;
        let object = &application_unit.identities.export_object;
        validate_capability_provenance_artifact(
            "capability-entry application generated C",
            &object.generated_c,
        )?;
        validate_capability_provenance_artifact(
            "capability-entry application generated source",
            &generated_c.generated_source,
        )?;
        let application_source_is_consistent = match (
            &generated_c.capability_entry,
            &generated_c.capability_application_unit,
        ) {
            (Some(singleton), None) => {
                singleton.entry_id == entry.entry_id
                    && singleton.invocation_abi == entry.invocation_abi
                    && singleton.local_profile == manifest_entry.local_profile
                    && singleton.runtime_surface_policy_sha256
                        == manifest.pipeline.runtime_surface_policy_sha256
                    && generated_c.exported_unit_name
                        == format!("convex_wasm_entry_{}", entry.entry_id)
            },
            (None, Some(physical)) => {
                validate_physical_application_unit_identity(&physical.application_unit, entries)?;
                physical.entries.len() == entries.len()
                    && physical.runtime_surface_policy_sha256
                        == manifest.pipeline.runtime_surface_policy_sha256
                    && generated_c.exported_unit_name
                        == physical.application_unit.exported_unit_name
                    && physical
                        .entries
                        .get(entry_slot)
                        .is_some_and(|physical_entry| {
                            physical_entry.entry_id == entry.entry_id
                                && physical_entry.entry_path == entry.entry_path
                                && physical_entry.invocation_abi == entry.invocation_abi
                                && physical_entry.local_profile_sha256
                                    == manifest_entry.local_profile.sha256
                        })
            },
            _ => false,
        };
        if usize::try_from(application_unit.entry_slot) != Ok(entry_slot)
            || application_unit.entry_id != entry.entry_id
            || !application_source_is_consistent
            || generated_c.unit_role != "untyped-application"
            || object.unit_role != "untyped-application"
            || generated_c.flags.iter().any(|flag| flag == "-typed")
            || generated_c.flags.is_empty()
            || generated_c.opaque_value_abi_version != manifest.abi.opaque_value_abi_version
            || object.opaque_value_abi_version != manifest.abi.opaque_value_abi_version
            || generated_c.value_mode != manifest.execution.value_mode()
            || object.value_mode != manifest.execution.value_mode()
            || object.effect_execution_mode != manifest.execution.effect_execution_mode()
            || generated_c.pipeline_kind != manifest.pipeline.kind
            || object.pipeline_kind != manifest.pipeline.kind
            || generated_c.producer_implementation != producer_implementation
            || object.producer_implementation != producer_implementation
            || generated_c.semantic_environment != provenance.semantic_environment
            || object.semantic_environment != provenance.semantic_environment
            || !generated_c
                .guest_source_provenance
                .as_ref()
                .is_some_and(JsonValue::is_object)
            || generated_c.static_hermes.materials
                != manifest.toolchain.static_hermes.materials_sha256
            || generated_c.static_hermes.revision != manifest.toolchain.static_hermes.revision
            || object.emscripten.materials != manifest.toolchain.emscripten.materials_sha256
            || object.emscripten.llvm_revision != manifest.toolchain.emscripten.llvm_revision
            || object.emscripten.revision != manifest.toolchain.emscripten.revision
            || object.runtime_headers != manifest.runtime.headers_material_sha256
        {
            return Err(invalid_provenance(
                "capability-entry application-unit provenance is inconsistent",
            ));
        }
    }

    let core_wasm: CapabilitySplitCoreWasmIdentity =
        serde_json::from_value(provenance.identities.core_wasm.clone()).map_err(|error| {
            invalid_provenance(format!(
                "capability-entry split core Wasm identity is invalid: {error}"
            ))
        })?;
    validate_capability_partition_policy(&core_wasm.partition_policy, core_wasm.bucket)?;
    let core_topology = core_wasm.unit_topology.fields();
    validate_capability_provenance_artifact(
        "capability-entry core selector object",
        &core_wasm.selector_object,
    )?;
    validate_capability_provenance_artifact(
        "capability-entry core runtime main object",
        &core_wasm.runtime.main_object,
    )?;
    validate_capability_provenance_artifact(
        "capability-entry bridge object",
        &core_topology.bridge.object,
    )?;
    let formatter_topology_is_consistent = match (&core_wasm.unit_topology, &provenance.formatter) {
        (CapabilitySplitCoreWasmTopology::V1(_), None) => {
            core_wasm.static_hermes.formatter_flags.is_none()
        },
        (CapabilitySplitCoreWasmTopology::V2(topology), Some(formatter)) => {
            validate_capability_provenance_artifact(
                "capability-entry formatter object",
                &topology.formatter.object,
            )?;
            core_wasm.static_hermes.formatter_flags.as_deref()
                == Some(formatter.generated_c.flags.as_slice())
                && formatter.object.compile_flags == core_wasm.emscripten.compile_flags
                && formatter.object.compile_flags == bridge.object.compile_flags
                && topology.formatter_flags == formatter.generated_c.flags
                && topology.application_flags == formatter.generated_c.flags
                && topology.formatter.entry_symbol == "sh_export_convex_wasm_console_formatter"
                && topology.formatter.generated_source == formatter.generated_c.generated_source
                && topology.selected_entry_preparation == CAPABILITY_SELECTED_ENTRY_PREPARATION
                && has_capability_unit_initialization_order(
                    &topology.initialization_order,
                    runtime_support_role,
                )
        },
        (CapabilitySplitCoreWasmTopology::V3(topology), Some(formatter)) => {
            validate_capability_provenance_artifact(
                "capability-entry formatter object",
                &topology.formatter.object,
            )?;
            core_wasm.static_hermes.formatter_flags.as_deref()
                == Some(formatter.generated_c.flags.as_slice())
                && formatter.object.compile_flags == core_wasm.emscripten.compile_flags
                && formatter.object.compile_flags == bridge.object.compile_flags
                && topology.formatter_flags == formatter.generated_c.flags
                && topology.application_flags == formatter.generated_c.flags
                && topology.formatter.entry_symbol == "sh_export_convex_wasm_console_formatter"
                && topology.formatter.generated_source == formatter.generated_c.generated_source
                && topology.selected_entry_preparation == CAPABILITY_SELECTED_ENTRY_PREPARATION
                && has_capability_multi_entry_unit_initialization_order(
                    &topology.initialization_order,
                    runtime_support_role,
                )
        },
        _ => false,
    };
    let application_topology_is_consistent = if multi_entry_application_unit {
        let Some(physical_source) = provenance.application_units.first().and_then(|unit| {
            unit.identities
                .generated_c
                .capability_application_unit
                .as_ref()
        }) else {
            return Err(invalid_provenance(
                "capability physical application-unit source identity is missing",
            ));
        };
        validate_physical_application_unit_identity(&physical_source.application_unit, entries)?;
        let Some(core_application_units) = &core_wasm.application_units else {
            return Err(invalid_provenance(
                "capability physical application-unit core table is missing",
            ));
        };
        core_topology.application_entry_count == u32::try_from(entries.len()).ok()
            && core_topology.application_entry_count_symbol
                == Some(CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL)
            && core_topology.application_factory_by_slot_symbol.is_none()
            && core_topology.application_factory_by_unit_slot_symbol
                == Some(CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL)
            && core_topology.application_unit == Some(&physical_source.application_unit)
            && core_topology.application_unit_count == Some(1)
            && core_topology.application_unit_count_symbol
                == Some(CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL)
            && core_application_units.len() == 1
            && core_application_units[0].application_unit_slot == 0
            && core_application_units[0].application_unit == physical_source.application_unit
            && core_wasm
                .members
                .first()
                .is_some_and(|member| core_application_units[0].object == member.object)
    } else {
        core_wasm.application_units.is_none()
            && match manifest.schema_version {
                CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION => {
                    core_topology.application_entry_count.is_none()
                        && core_topology.application_entry_count_symbol.is_none()
                        && core_topology.application_factory_by_slot_symbol.is_none()
                },
                CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION => {
                    core_topology.application_entry_count == u32::try_from(entries.len()).ok()
                        && core_topology.application_entry_count_symbol
                            == Some(CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL)
                        && core_topology.application_factory_by_slot_symbol
                            == Some(CAPABILITY_APPLICATION_FACTORY_BY_SLOT_SYMBOL)
                },
                _ => false,
            }
            && core_topology
                .application_factory_by_unit_slot_symbol
                .is_none()
            && core_topology.application_unit.is_none()
            && core_topology.application_unit_count.is_none()
            && core_topology.application_unit_count_symbol.is_none()
    };
    if core_wasm.abi.capability_entry_selector_abi_version
        != manifest.abi.entry_selector_abi_version
        || core_wasm.abi.capability_request_abi_version
            != manifest.abi.capability_request_abi_version
        || core_wasm.abi.opaque_value_abi_version != manifest.abi.opaque_value_abi_version
        || core_wasm.pipeline_kind != manifest.pipeline.kind
        || core_wasm.producer_implementation != producer_implementation
        || core_wasm.semantic_environment != provenance.semantic_environment
        || core_wasm.target.cpu != manifest.engine.target.cpu
        || core_wasm.target.triple != manifest.engine.target.triple
        || core_wasm.emscripten.compile_flags != bridge.object.compile_flags
        || core_wasm.emscripten.llvm_revision != manifest.toolchain.emscripten.llvm_revision
        || core_wasm.emscripten.materials != manifest.toolchain.emscripten.materials_sha256
        || core_wasm.emscripten.revision != manifest.toolchain.emscripten.revision
        || core_wasm.runtime.archives != manifest.runtime.archives_material_sha256
        || core_wasm.runtime.main_object != manifest.runtime.main_object
        || core_wasm.link_flags.is_empty()
        || core_wasm.static_hermes.application_flags != core_topology.application_flags
        || core_wasm.static_hermes.bridge_flags != core_topology.bridge_flags
        || core_wasm.static_hermes.bridge_flags != bridge.generated_c.flags
        || core_wasm.static_hermes.materials != manifest.toolchain.static_hermes.materials_sha256
        || core_wasm.static_hermes.revision != manifest.toolchain.static_hermes.revision
        || core_topology.bridge.entry_symbol != "sh_export_convex_wasm_capability_bridge"
        || core_topology.bridge.generated_source != bridge.generated_c.generated_source
        || !formatter_topology_is_consistent
        || core_wasm.members.len() != entries.len()
        || !application_topology_is_consistent
    {
        return Err(invalid_provenance(
            "capability-entry core Wasm topology is inconsistent",
        ));
    }
    for (entry_slot, ((member, application_unit), entry)) in core_wasm
        .members
        .iter()
        .zip(&provenance.application_units)
        .zip(entries)
        .enumerate()
    {
        validate_capability_provenance_artifact(
            "capability-entry application object",
            &member.object,
        )?;
        let expected_routes = routes
            .iter()
            .filter(|route| route.entry_id == entry.entry_id)
            .collect::<Vec<_>>();
        if usize::try_from(member.entry_slot) != Ok(entry_slot)
            || member.entry_slot != application_unit.entry_slot
            || member.application_unit_slot != application_unit.application_unit_slot
            || (multi_entry_application_unit
                && core_wasm
                    .application_units
                    .as_ref()
                    .and_then(|units| units.first())
                    .is_none_or(|physical| physical.object != member.object))
            || member.entry_id != entry.entry_id
            || member.entry_symbol != entry.entry_symbol
            || member.invocation_abi != entry.invocation_abi
            || member.effect_execution_mode != manifest.execution.effect_execution_mode()
            || member.value_mode != manifest.execution.value_mode()
            || member.runtime_surface_policy_sha256
                != manifest.pipeline.runtime_surface_policy_sha256
            || member.routes.len() != expected_routes.len()
            || member
                .routes
                .iter()
                .zip(expected_routes)
                .any(|(actual, expected)| {
                    actual.entry_selector_id != expected.entry_selector_id
                        || actual.export_name != expected.export_name
                        || actual.route_id != expected.route_id
                        || actual.udf_kind != expected.udf_kind
                        || actual.visibility != expected.visibility
                })
            || application_unit.identities.generated_c.flags
                != core_wasm.static_hermes.application_flags
            || application_unit.identities.export_object.compile_flags
                != core_wasm.emscripten.compile_flags
        {
            return Err(invalid_provenance(
                "capability-entry core Wasm member identity is inconsistent",
            ));
        }
    }

    let runtime_main: CapabilitySplitRuntimeMainIdentity = serde_json::from_value(
        provenance.identities.runtime_main_object.clone(),
    )
    .map_err(|error| {
        invalid_provenance(format!(
            "capability-entry split runtime main identity is invalid: {error}"
        ))
    })?;
    let runtime_topology = runtime_main.unit_topology.fields();
    let runtime_topology_is_consistent = capability_formatter_topology_versions_match(
        &core_wasm.unit_topology,
        &runtime_main.unit_topology,
        provenance.formatter.is_some(),
    ) && match (
        &core_wasm.unit_topology,
        &runtime_main.unit_topology,
        &provenance.formatter,
    ) {
        (
            CapabilitySplitCoreWasmTopology::V1(_),
            CapabilitySplitRuntimeMainTopology::V1(_),
            None,
        ) => true,
        (
            CapabilitySplitCoreWasmTopology::V2(core),
            CapabilitySplitRuntimeMainTopology::V2(runtime),
            Some(_),
        ) => {
            runtime.formatter_entry_symbol == core.formatter.entry_symbol
                && runtime.formatter_exported_unit_name == "convex_wasm_console_formatter"
                && runtime.initialization_order == core.initialization_order
                && core.selected_entry_preparation == CAPABILITY_SELECTED_ENTRY_PREPARATION
                && runtime.selected_entry_preparation == core.selected_entry_preparation
        },
        (
            CapabilitySplitCoreWasmTopology::V3(core),
            CapabilitySplitRuntimeMainTopology::V3(runtime),
            Some(_),
        ) => {
            runtime.formatter_entry_symbol == core.formatter.entry_symbol
                && runtime.formatter_exported_unit_name == "convex_wasm_console_formatter"
                && runtime.initialization_order == core.initialization_order
                && core.selected_entry_preparation == CAPABILITY_SELECTED_ENTRY_PREPARATION
                && runtime.selected_entry_preparation == core.selected_entry_preparation
        },
        _ => false,
    };
    if runtime_main.cohort_entry_selector_abi_version != COHORT_ENTRY_SELECTOR_ABI_VERSION
        || !capability_runtime_main_compile_flags_match(
            &core_wasm.emscripten.compile_flags,
            &runtime_main.compile_flags,
            multi_entry_application_unit.then_some(MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG),
        )
        || runtime_main.effect_execution_mode != manifest.execution.effect_execution_mode()
        || runtime_main.emscripten
            != (CapabilityProvenanceEmscriptenIdentity {
                llvm_revision: manifest.toolchain.emscripten.llvm_revision.clone(),
                materials: manifest.toolchain.emscripten.materials_sha256.clone(),
                revision: manifest.toolchain.emscripten.revision.clone(),
            })
        || runtime_main.entry_selector_symbol != "convex_wasm_selected_exported_unit"
        || runtime_main.opaque_value_abi_version != manifest.abi.opaque_value_abi_version
        || runtime_main.pipeline_kind != manifest.pipeline.kind
        || runtime_main.producer_implementation != producer_implementation
        || runtime_main.runtime_headers != manifest.runtime.headers_material_sha256
        || runtime_main.runtime_main != manifest.runtime.main_material_sha256
        || runtime_main.runtime_main_language != "c++"
        || runtime_main.semantic_environment != provenance.semantic_environment
        || runtime_topology.application_selector_symbol != "convex_wasm_selected_exported_unit"
        || runtime_topology.bridge_entry_symbol != "sh_export_convex_wasm_capability_bridge"
        || runtime_topology.bridge_exported_unit_name != "convex_wasm_capability_bridge"
        || runtime_topology.application_entry_count != core_topology.application_entry_count
        || runtime_topology.application_entry_count_symbol
            != core_topology.application_entry_count_symbol
        || runtime_topology.application_factory_by_slot_symbol
            != core_topology.application_factory_by_slot_symbol
        || runtime_topology.application_factory_by_unit_slot_symbol
            != core_topology.application_factory_by_unit_slot_symbol
        || runtime_topology.application_unit != core_topology.application_unit
        || runtime_topology.application_unit_count != core_topology.application_unit_count
        || runtime_topology.application_unit_count_symbol
            != core_topology.application_unit_count_symbol
        || runtime_topology.selected_entry_preparation != core_topology.selected_entry_preparation
        || !runtime_topology_is_consistent
    {
        return Err(invalid_provenance(
            "capability-entry runtime main topology is inconsistent",
        ));
    }

    let wasmtime_aot: CapabilityWasmtimeAotIdentity =
        serde_json::from_value(provenance.identities.wasmtime_aot.clone()).map_err(|error| {
            invalid_provenance(format!(
                "capability-entry Wasmtime AOT identity is invalid: {error}"
            ))
        })?;
    validate_capability_provenance_artifact(
        "capability-entry Wasmtime AOT core Wasm",
        &wasmtime_aot.core_wasm,
    )?;
    if validate_sha256(
        "capability-entry Wasmtime AOT materials SHA-256",
        &wasmtime_aot.wasmtime.materials,
    )
    .is_err()
        || wasmtime_aot.core_wasm != manifest.artifacts.core_wasm
        || wasmtime_aot.engine_config != manifest.engine.config
        || wasmtime_aot.pipeline_kind != manifest.pipeline.kind
        || wasmtime_aot.producer_implementation != producer_implementation
        || wasmtime_aot.semantic_environment != provenance.semantic_environment
        || wasmtime_aot.target.cpu != manifest.engine.target.cpu
        || wasmtime_aot.target.platform != manifest.engine.target.platform
        || wasmtime_aot.target.triple != manifest.engine.target.triple
        || wasmtime_aot.wasmtime.package != manifest.engine.package
        || wasmtime_aot.wasmtime.revision != manifest.engine.revision
    {
        return Err(invalid_provenance(
            "capability-entry Wasmtime AOT provenance is inconsistent",
        ));
    }

    match manifest.schema_version {
        CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION => {
            validate_split_schema_3_selector_provenance(
                &provenance.identities.selector_object,
                provenance.application_units.first().ok_or_else(|| {
                    invalid_provenance("capability-entry singleton application unit is missing")
                })?,
                &core_wasm,
                routes,
                &manifest.pipeline.kind,
                &producer_implementation,
                &provenance.semantic_environment,
            )?;
        },
        CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION => {
            let selector_expected = CapabilitySelectorExpectedIdentity {
                abi_version: manifest.abi.entry_selector_abi_version,
                compile_flags: &core_wasm.emscripten.compile_flags,
                emscripten_llvm_revision: &manifest.toolchain.emscripten.llvm_revision,
                emscripten_materials: &manifest.toolchain.emscripten.materials_sha256,
                emscripten_revision: &manifest.toolchain.emscripten.revision,
                partition_policy: &core_wasm.partition_policy,
                pipeline_kind: &manifest.pipeline.kind,
                producer_implementation: &producer_implementation,
                semantic_environment: &provenance.semantic_environment,
            };
            if multi_entry_application_unit {
                let application_unit = core_topology.application_unit.ok_or_else(|| {
                    invalid_provenance(
                        "capability physical application-unit selector identity is missing",
                    )
                })?;
                validate_capability_multi_entry_unit_selector_provenance(
                    &provenance.identities.selector_object,
                    entries,
                    routes,
                    application_unit,
                    &selector_expected,
                )?;
            } else {
                validate_capability_per_entry_selector_provenance(
                    &provenance.identities.selector_object,
                    entries.len(),
                    routes,
                    &selector_expected,
                )?;
            }
        },
        _ => {
            return Err(invalid_provenance(
                "capability-entry split provenance uses an unsupported manifest schema",
            ));
        },
    }
    Ok(())
}

fn validate_capability_entry_provenance_shape(
    provenance: &CapabilityEntryBuildProvenance,
    pipeline_version: u32,
) -> Result<(), WasmUdfPackageError> {
    match provenance {
        CapabilityEntryBuildProvenance::V1(provenance)
            if provenance.kind == CAPABILITY_ENTRY_PROVENANCE_KIND_V1 && pipeline_version < 9 =>
        {
            Ok(())
        },
        CapabilityEntryBuildProvenance::V2(provenance)
            if provenance.kind == CAPABILITY_ENTRY_PROVENANCE_KIND_V2 && pipeline_version >= 9 =>
        {
            Ok(())
        },
        CapabilityEntryBuildProvenance::V1(_) => Err(invalid_provenance(
            "capability-entry provenance v1 cannot describe split application units",
        )),
        CapabilityEntryBuildProvenance::V2(_) => Err(invalid_provenance(
            "capability-entry provenance v2 requires the split-unit pipeline",
        )),
    }
}

fn validate_capability_entry_provenance(
    provenance: &CapabilityEntryBuildProvenance,
    manifest: &CapabilityEntryManifest,
    entries: &[CapabilityEntryRuntimeUnitIdentity],
    routes: &[CapabilityEntryRoute],
) -> Result<(), WasmUdfPackageError> {
    let pipeline_version = artifact_pipeline_version(&manifest.pipeline.kind)?;
    validate_capability_entry_provenance_shape(provenance, pipeline_version)?;
    match provenance {
        CapabilityEntryBuildProvenance::V1(provenance) => {
            validate_capability_entry_provenance_fields(
                CapabilityEntryProvenanceFields {
                    artifact_pipeline_sha256: &provenance.artifact_pipeline_sha256,
                    engine_identity: &provenance.engine_identity,
                    identities: &provenance.identities,
                    kind: &provenance.kind,
                    materials: &provenance.materials,
                    request_envelope: &provenance.request_envelope,
                    semantic_environment: &provenance.semantic_environment,
                    value_codec: &provenance.value_codec,
                },
                CAPABILITY_ENTRY_PROVENANCE_KIND_V1,
                manifest,
            )?;
            if manifest.schema_version == CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION {
                validate_legacy_capability_selector_provenance(
                    &provenance.identities.selector_object,
                    manifest.abi.entry_selector_abi_version,
                    entries.len(),
                    routes,
                    &manifest.pipeline.producer_implementation,
                )?;
            }
            Ok(())
        },
        CapabilityEntryBuildProvenance::V2(provenance) => {
            validate_capability_entry_provenance_fields(
                CapabilityEntryProvenanceFields {
                    artifact_pipeline_sha256: &provenance.artifact_pipeline_sha256,
                    engine_identity: &provenance.engine_identity,
                    identities: &provenance.identities,
                    kind: &provenance.kind,
                    materials: &provenance.materials,
                    request_envelope: &provenance.request_envelope,
                    semantic_environment: &provenance.semantic_environment,
                    value_codec: &provenance.value_codec,
                },
                CAPABILITY_ENTRY_PROVENANCE_KIND_V2,
                manifest,
            )?;
            validate_capability_split_provenance(provenance, manifest, entries, routes)
        },
    }
}

fn official_output_chunk_entry_slots(
    provenance: &CapabilityEntryBuildProvenance,
    entries: &[CapabilityEntryRuntimeUnitIdentity],
) -> Option<BTreeMap<String, u32>> {
    let CapabilityEntryBuildProvenance::V2(provenance) = provenance else {
        return None;
    };
    let chunk_application = provenance.chunk_application.as_ref()?;
    let mut entry_slots = BTreeMap::new();
    assert_eq!(
        chunk_application.descriptor.entries.len(),
        entries.len(),
        "validated official-output chunk descriptor has inconsistent entry cardinality"
    );
    for (descriptor_entry, entry) in chunk_application.descriptor.entries.iter().zip(entries) {
        assert!(
            entry_slots
                .insert(entry.entry_id.clone(), descriptor_entry.entry_slot)
                .is_none(),
            "validated official-output chunk descriptor has duplicate entry IDs"
        );
    }
    Some(entry_slots)
}

fn validate_cohort_manifest_identity(
    cohort: &CohortManifest,
    package_entry: &CohortPackageEntry,
    execution_manifest: &JsonValue,
    expected_entry_id: &str,
    expected_entry_selector_id: &str,
    runtime: &RuntimeCompatibility<'_>,
    expected_precompiler: &PrecompilerMaterialIdentity,
) -> Result<BTreeSet<&'static str>, WasmUdfPackageError> {
    if cohort.kind != COHORT_MANIFEST_KIND
        || cohort.schema_version != 1
        || cohort.abi.cohort_entry_selector_abi_version != COHORT_ENTRY_SELECTOR_ABI_VERSION
        || cohort.abi.opaque_value_abi_version != runtime.opaque_value_abi_version
    {
        return Err(invalid_entry(
            "unsupported cohort manifest kind, schema, or ABI",
        ));
    }
    validate_cohort_partition_policy(&cohort.partition_policy, cohort.bucket)?;
    if !cohort.runtime.is_object() || !cohort.toolchain.is_object() {
        return Err(invalid_entry(
            "cohort runtime and toolchain identities must be objects",
        ));
    }
    for (field, value) in [
        (
            "cohort.compiler.loweringPipelineSha256",
            cohort.compiler.lowering_pipeline_sha256.as_str(),
        ),
        (
            "cohort.compiler.sourcePipelineSha256",
            cohort.compiler.source_pipeline_sha256.as_str(),
        ),
        (
            "cohort.engine.compatibilitySha256",
            cohort.engine.compatibility_sha256.as_str(),
        ),
        (
            "cohort.engine.configurationSha256",
            cohort.engine.configuration_sha256.as_str(),
        ),
        (
            "cohort.pipeline.artifactPipelineSha256",
            cohort.pipeline.artifact_pipeline_sha256.as_str(),
        ),
    ] {
        validate_sha256(field, value)?;
    }
    validate_artifact_pipeline_kind(&cohort.pipeline.kind)?;
    if cohort.engine.package != *expected_precompiler
        || cohort.engine.revision != runtime.wasmtime_revision
        || cohort.engine.target.triple != runtime.target_triple
        || cohort.engine.target.cpu != runtime.target_cpu
        || cohort.engine.compatibility_sha256 != runtime.engine_compatibility_sha256
        || cohort.engine.configuration_sha256 != runtime.engine_configuration_sha256
        || !cohort.engine.target.platform.is_object()
    {
        return Err(invalid_entry(
            "cohort compiler engine identity is incompatible with the runtime",
        ));
    }
    let engine_config = serde_json::to_value(&cohort.engine.config)
        .expect("cohort engine configuration serializes");
    let mut engine_config_bytes = Vec::new();
    write_canonical_json(&engine_config, &mut engine_config_bytes)
        .expect("writing cohort engine configuration cannot fail");
    if sha256(&engine_config_bytes) != cohort.engine.configuration_sha256 {
        return Err(invalid_entry(
            "cohort engine configuration digest is invalid",
        ));
    }

    let package_core_wasm = package_entry
        .artifacts
        .get("module.wasm")
        .expect("validated cohort package artifact disappeared");
    let package_serialized_module = package_entry
        .artifacts
        .get("module.cwasm")
        .expect("validated cohort package artifact disappeared");
    for (field, artifact) in [
        ("cohort.artifacts.coreWasm", &cohort.artifacts.core_wasm),
        (
            "cohort.artifacts.serializedModule",
            &cohort.artifacts.serialized_module,
        ),
    ] {
        validate_sha256(field, &artifact.sha256)?;
        if artifact.size == 0 {
            return Err(invalid_entry(format!("{field} size must be positive")));
        }
    }
    if cohort.artifacts.core_wasm.sha256 != package_core_wasm.sha256
        || cohort.artifacts.core_wasm.size != package_core_wasm.size
        || cohort.artifacts.serialized_module.sha256 != package_serialized_module.sha256
        || cohort.artifacts.serialized_module.size != package_serialized_module.size
    {
        return Err(invalid_entry(
            "cohort manifest artifact identity differs from package metadata",
        ));
    }

    let mut execution_manifest_bytes = Vec::new();
    write_canonical_json(execution_manifest, &mut execution_manifest_bytes)
        .expect("writing canonical execution manifest cannot fail");
    let execution =
        WasmUdfExecutionManifest::parse_for_runtime(&execution_manifest_bytes, runtime)?;
    let artifact = execution
        .artifact()
        .ok_or_else(|| invalid_entry("cohort execution manifest omitted its artifact"))?;
    let execution_cohort = artifact
        .cohort()
        .ok_or_else(|| invalid_entry("cohort execution artifact omitted package identity"))?;
    if execution_cohort.package_id().as_str() != package_entry.key
        || execution_cohort.manifest_sha256().as_str() != package_entry.manifest_sha256
        || execution_cohort.bucket() != cohort.bucket
        || artifact.core_wasm_sha256().as_str() != cohort.artifacts.core_wasm.sha256
        || artifact.core_wasm_bytes() != cohort.artifacts.core_wasm.size
        || artifact.serialized_module_sha256().as_str() != cohort.artifacts.serialized_module.sha256
        || artifact.serialized_module_bytes() != cohort.artifacts.serialized_module.size
    {
        return Err(invalid_entry(
            "cohort execution artifact identity differs from the authenticated package",
        ));
    }
    if cohort.compiler.admitted_language_version != execution.compiler().admitted_language_version()
        || cohort.compiler.compiler_revision != execution.compiler().compiler_revision()
        || cohort.compiler.lowering_pipeline_sha256
            != execution.compiler().lowering_pipeline_sha256().as_str()
        || cohort.compiler.source_pipeline_sha256
            != execution.compiler().source_pipeline_sha256().as_str()
        || cohort.pipeline.artifact_pipeline_sha256
            != execution.compiler().artifact_pipeline_sha256().as_str()
    {
        return Err(invalid_entry(
            "cohort compiler identity differs from the route execution manifest",
        ));
    }

    if cohort.members.is_empty() {
        return Err(invalid_entry("cohort manifest contains no members"));
    }
    let mut route_ids = BTreeSet::new();
    let mut selector_ids = BTreeSet::new();
    let mut selected_member = None;
    let mut permitted_conditional_imports = BTreeSet::new();
    let mut previous_route_id: Option<&str> = None;
    for (index, member) in cohort.members.iter().enumerate() {
        match execution.manifest_schema_version() {
            EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION if member.effect_execution_mode.is_none() => {
                return Err(invalid_entry(
                    "schema-6 cohort member omitted effect execution mode",
                ));
            },
            COHORT_MANIFEST_SCHEMA_VERSION | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
                if member.effect_execution_mode.is_some() =>
            {
                return Err(invalid_entry(
                    "legacy cohort member contains an effect execution mode",
                ));
            },
            COHORT_MANIFEST_SCHEMA_VERSION
            | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
            | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION => {},
            _ => {
                return Err(invalid_entry(
                    "cohort member uses an unsupported execution manifest schema",
                ));
            },
        }
        if member.effect_execution_mode.unwrap_or_default() != execution.effect_execution_mode() {
            return Err(invalid_entry(
                "cohort member effect execution mode differs from the route manifest",
            ));
        }
        validate_cohort_member(member, index, cohort, &execution)?;
        validate_imported_operations(
            &member.imported_operations,
            member.route.udf_kind,
            execution.manifest_schema_version(),
        )?;
        permitted_conditional_imports.extend(permitted_conditional_convex_imports(
            member.value_mode,
            member.effect_execution_mode.unwrap_or_default(),
            &member.imported_operations,
        ));
        if previous_route_id.is_some_and(|previous| previous >= member.route_id.as_str()) {
            return Err(invalid_entry(
                "cohort members are not strictly sorted by route identity",
            ));
        }
        previous_route_id = Some(&member.route_id);
        if !route_ids.insert(member.route_id.as_str())
            || !selector_ids.insert(member.entry_selector_id.as_str())
        {
            return Err(invalid_entry(
                "cohort manifest contains a duplicate route or selector identity",
            ));
        }
        if member.entry_id == expected_entry_id
            && member.entry_selector_id == expected_entry_selector_id
        {
            if selected_member.replace(member).is_some() {
                return Err(invalid_entry(
                    "cohort package reference selects more than one member",
                ));
            }
        }
    }
    let selected_member = selected_member
        .ok_or_else(|| invalid_entry("cohort package reference selects no member"))?;
    let source = execution.source();
    if selected_member.route.runtime_module_path != source.runtime_module_path()
        || selected_member.route.export_name != source.export_name()
        || selected_member.route.udf_kind != source.udf_kind()
        || selected_member.entry_id != artifact.entry_id().expect("schema 4 entry ID").as_str()
        || selected_member.entry_selector_id
            != artifact
                .entry_selector_id()
                .expect("schema 4 entry selector")
        || selected_member.entry_symbol != artifact.entry_symbol().expect("schema 4 entry symbol")
        || selected_member.selection_index
            != artifact
                .entry_selection_index()
                .expect("schema 4 selection index")
    {
        return Err(invalid_entry(
            "cohort selected member differs from the route execution manifest",
        ));
    }
    Ok(permitted_conditional_imports)
}

fn validate_artifact_pipeline_kind(kind: &str) -> Result<(), WasmUdfPackageError> {
    artifact_pipeline_version(kind).map(|_| ())
}

fn artifact_pipeline_version(kind: &str) -> Result<u32, WasmUdfPackageError> {
    let version = kind
        .strip_prefix("convex-wasm-artifact-pipeline-v")
        .ok_or_else(|| invalid_entry("cohort artifact pipeline kind is invalid"))?;
    let parsed_version = version
        .parse::<u32>()
        .map_err(|_| invalid_entry("cohort artifact pipeline kind is invalid"))?;
    if parsed_version == 0 || parsed_version.to_string() != version {
        return Err(invalid_entry("cohort artifact pipeline kind is invalid"));
    }
    Ok(parsed_version)
}

fn validate_cohort_member(
    member: &CohortMember,
    index: usize,
    cohort: &CohortManifest,
    selected_execution: &WasmUdfExecutionManifest,
) -> Result<(), WasmUdfPackageError> {
    for (field, value) in [
        ("cohort member routeId", member.route_id.as_str()),
        ("cohort member entryId", member.entry_id.as_str()),
        (
            "cohort member source.exportSha256",
            member.source.export_sha256.as_str(),
        ),
        (
            "cohort member source.resolvedGraphSha256",
            member.source.resolved_graph_sha256.as_str(),
        ),
        (
            "cohort member generatedC.sha256",
            member.generated_c.sha256.as_str(),
        ),
        ("cohort member object.sha256", member.object.sha256.as_str()),
        (
            "cohort member generatedJavaScript.sha256",
            member.source.generated_java_script.sha256.as_str(),
        ),
    ] {
        validate_sha256(field, value)?;
    }
    validate_entry_selector_id("cohort member entrySelectorId", &member.entry_selector_id)?;
    if member.generated_c.size == 0
        || member.object.size == 0
        || member.source.generated_java_script.size == 0
        || member.selection_index as usize != index
        || member.opaque_value_abi_version != cohort.abi.opaque_value_abi_version
        || member.lowering.admitted_language_version != cohort.compiler.admitted_language_version
        || member.lowering.lowering_pipeline_sha256 != cohort.compiler.lowering_pipeline_sha256
        || member.lowering.source_pipeline_sha256 != cohort.compiler.source_pipeline_sha256
        || member.route.kind != "convex-wasm-runtime-route-v1"
    {
        return Err(invalid_entry("cohort member metadata is inconsistent"));
    }

    let route = serde_json::json!({
        "exportName": member.route.export_name,
        "kind": member.route.kind,
        "runtimeModulePath": member.route.runtime_module_path,
        "udfKind": member.route.udf_kind,
    });
    let route_identity = serde_json::json!({
        "domain": "convex-wasm-route-id-v1",
        "route": route,
    });
    let mut route_identity_bytes = Vec::new();
    write_canonical_json(&route_identity, &mut route_identity_bytes)
        .expect("writing cohort route identity cannot fail");
    let route_id = sha256(&route_identity_bytes);
    let entry_identity = serde_json::json!({
        "domain": "convex-wasm-cohort-entry-v1",
        "routeId": route_id,
    });
    let mut entry_identity_bytes = Vec::new();
    write_canonical_json(&entry_identity, &mut entry_identity_bytes)
        .expect("writing cohort entry identity cannot fail");
    let entry_id = sha256(&entry_identity_bytes);
    let partition_sha256 =
        sha256(format!("{}\0{}", cohort.partition_policy.kind, member.route_id).as_bytes());
    let bucket = u32::from_str_radix(&partition_sha256[..8], 16)
        .expect("validated route identity has eight hexadecimal characters")
        % cohort.partition_policy.bucket_count;
    if member.route_id != route_id
        || member.entry_id != entry_id
        || member.entry_selector_id != entry_id[..16]
        || member.entry_symbol != format!("sh_export_convex_wasm_udf_{route_id}")
        || bucket != cohort.bucket
    {
        return Err(invalid_entry(
            "cohort member derived route or selector identity is invalid",
        ));
    }

    if member.entry_id
        == selected_execution
            .artifact()
            .and_then(|artifact| artifact.entry_id())
            .expect("selected schema 4 execution manifest entry ID")
            .as_str()
    {
        if member.imported_operations != selected_execution.imported_operations()
            || member.value_mode != selected_execution.value_mode()
            || member.effect_execution_mode.unwrap_or_default()
                != selected_execution.effect_execution_mode()
            || member.source.export_sha256 != selected_execution.source().export_sha256().as_str()
            || member.source.resolved_graph_sha256
                != selected_execution.source().resolved_graph_sha256().as_str()
        {
            return Err(invalid_entry(
                "cohort member execution identity differs from the selected route manifest",
            ));
        }
    }
    Ok(())
}

fn validate_cohort_partition_policy(
    policy: &CohortPartitionPolicy,
    bucket: u32,
) -> Result<(), WasmUdfPackageError> {
    if !cohort_partition_policy_is_supported(policy, bucket) {
        return Err(invalid_entry(
            "unsupported cohort partition policy or bucket",
        ));
    }
    Ok(())
}

fn validate_capability_partition_policy(
    policy: &CohortPartitionPolicy,
    bucket: u32,
) -> Result<(), WasmUdfPackageError> {
    if !cohort_partition_policy_is_supported(policy, bucket) {
        return Err(invalid_provenance(
            "capability-entry cohort partition policy or bucket is unsupported",
        ));
    }
    Ok(())
}

fn cohort_partition_policy_is_supported(policy: &CohortPartitionPolicy, bucket: u32) -> bool {
    policy.bucket_count == 16
        && policy.hash == "sha256-route-id-domain-first-u32-be-modulo"
        && policy.kind == "convex-wasm-cohort-partition-v1"
        && bucket < policy.bucket_count
}

fn validate_cohort_provenance(
    provenance: &CohortBuildProvenance,
    cohort: &CohortManifest,
) -> Result<(), WasmUdfPackageError> {
    validate_sha256(
        "cohort provenance artifactPipelineSha256",
        &provenance.artifact_pipeline_sha256,
    )?;
    validate_sha256(
        "cohort provenance engineCompatibilitySha256",
        &provenance.engine_identity.engine_compatibility_sha256,
    )?;
    if provenance.kind != COHORT_PROVENANCE_KIND
        || provenance.artifact_pipeline_sha256 != cohort.pipeline.artifact_pipeline_sha256
        || provenance.engine_identity.engine_compatibility_sha256
            != cohort.engine.compatibility_sha256
        || provenance.engine_identity.engine_config != cohort.engine.config
        || provenance.engine_identity.target.cpu != cohort.engine.target.cpu
        || provenance.engine_identity.target.triple != cohort.engine.target.triple
        || !provenance.identities.is_object()
        || !provenance.materials.is_object()
        || !provenance.semantic_environment.is_object()
        || provenance.members.len() != cohort.members.len()
    {
        return Err(invalid_provenance(
            "cohort provenance differs from the authenticated cohort manifest",
        ));
    }
    for (provenance_member, cohort_member) in provenance.members.iter().zip(&cohort.members) {
        if !provenance_member.identities.is_object()
            || provenance_member.route_id != cohort_member.route_id
        {
            return Err(invalid_provenance(
                "cohort provenance member identity is invalid",
            ));
        }
    }
    Ok(())
}

fn validate_build_provenance(
    bytes: &[u8],
    compiler: &CompilerIdentity,
    artifact: &AotArtifactIdentity,
    expected_precompiler: Option<&PrecompilerMaterialIdentity>,
) -> Result<(), WasmUdfPackageError> {
    let provenance: BuildProvenance =
        serde_json::from_slice(bytes).map_err(WasmUdfPackageError::MalformedBuildProvenance)?;
    validate_sha256(
        "build-provenance.artifactPipelineSha256",
        &provenance.artifact_pipeline_sha256,
    )?;
    validate_sha256(
        "build-provenance.loweringPipelineSha256",
        &provenance.lowering_pipeline_sha256,
    )?;
    validate_sha256(
        "build-provenance.sourcePipelineSha256",
        &provenance.source_pipeline_sha256,
    )?;
    validate_sha256(
        "build-provenance.engineIdentity.engineCompatibilitySha256",
        &provenance.engine_identity.engine_compatibility_sha256,
    )?;
    if provenance.kind != BUILD_PROVENANCE_KIND {
        return Err(invalid_provenance("unsupported build provenance kind"));
    }
    if !provenance.identities.is_object()
        || !provenance.materials.is_object()
        || !provenance.semantic_environment.is_object()
    {
        return Err(invalid_provenance(
            "build provenance identities, materials, and semanticEnvironment must be objects",
        ));
    }
    if let Some(expected_precompiler) = expected_precompiler {
        let package_identity = provenance
            .identities
            .pointer("/wasmtimeAot/wasmtime/package")
            .ok_or_else(|| {
                invalid_provenance(
                    "build provenance does not identify its verified precompiler package",
                )
            })?;
        let expected_identity =
            serde_json::to_value(expected_precompiler).expect("precompiler identity serializes");
        if package_identity != &expected_identity {
            return Err(invalid_provenance(
                "build provenance precompiler identity differs from the deployment manifest",
            ));
        }
    }
    if provenance.artifact_pipeline_sha256 != compiler.artifact_pipeline_sha256().as_str()
        || provenance.lowering_pipeline_sha256 != compiler.lowering_pipeline_sha256().as_str()
        || provenance.source_pipeline_sha256 != compiler.source_pipeline_sha256().as_str()
    {
        return Err(invalid_provenance(
            "build provenance pipeline identities differ from the execution manifest",
        ));
    }
    let engine_identity = provenance.engine_identity;
    if engine_identity.kind != ENGINE_IDENTITY_KIND {
        return Err(invalid_provenance(
            "build provenance engine identity kind is invalid",
        ));
    }
    let engine_config = engine_identity.engine_config;
    if !engine_config.consume_fuel
        || !engine_config.epoch_interruption
        || engine_config.profiling_strategy != "perf-map"
        || !engine_config.wasm_exceptions
    {
        return Err(invalid_provenance(
            "build provenance engine configuration is invalid",
        ));
    }
    let engine_config_identity = serde_json::json!({
        "consumeFuel": engine_config.consume_fuel,
        "epochInterruption": engine_config.epoch_interruption,
        "profilingStrategy": engine_config.profiling_strategy,
        "wasmExceptions": engine_config.wasm_exceptions,
    });
    let mut engine_config_bytes = Vec::new();
    write_canonical_json(&engine_config_identity, &mut engine_config_bytes)
        .expect("writing engine configuration identity to a byte vector cannot fail");
    if sha256(&engine_config_bytes) != artifact.engine_configuration_sha256().as_str() {
        return Err(invalid_provenance(
            "build provenance engine configuration differs from the execution manifest",
        ));
    }
    if engine_identity.engine_compatibility_sha256
        != artifact.engine_compatibility_sha256().as_str()
        || engine_identity.target.cpu != artifact.target_cpu()
        || engine_identity.target.triple != artifact.target_triple()
    {
        return Err(invalid_provenance(
            "build provenance engine identity differs from the execution manifest",
        ));
    }
    Ok(())
}

fn validate_directory(path: &Path) -> Result<(), WasmUdfPackageError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| io_error(path, source))?;
    if !metadata.file_type().is_dir() || !has_private_permissions(&metadata) {
        return Err(WasmUdfPackageError::InvalidDirectory(path.to_owned()));
    }
    Ok(())
}

fn validate_directory_entries(path: &Path) -> Result<(), WasmUdfPackageError> {
    validate_directory_entries_exact(path, &PACKAGE_FILES)
}

fn validate_directory_entries_exact(
    path: &Path,
    expected_names: &[&'static str],
) -> Result<(), WasmUdfPackageError> {
    let expected = expected_names.iter().copied().collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(path).map_err(|source| io_error(path, source))? {
        let entry = entry.map_err(|source| io_error(path, source))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|name| WasmUdfPackageError::UnexpectedEntry(name.to_string_lossy().into()))?;
        if !expected.contains(name.as_str()) {
            return Err(WasmUdfPackageError::UnexpectedEntry(name));
        }
        actual.insert(name);
    }
    for expected_name in expected {
        if !actual.contains(expected_name) {
            return Err(WasmUdfPackageError::MissingFile(expected_name));
        }
    }
    Ok(())
}

fn validate_directory_entries_dynamic(
    path: &Path,
    expected_names: &[&str],
) -> Result<(), WasmUdfPackageError> {
    let expected = expected_names.iter().copied().collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(path).map_err(|source| io_error(path, source))? {
        let entry = entry.map_err(|source| io_error(path, source))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|name| WasmUdfPackageError::UnexpectedEntry(name.to_string_lossy().into()))?;
        if !expected.contains(name.as_str()) {
            return Err(WasmUdfPackageError::UnexpectedEntry(name));
        }
        actual.insert(name);
    }
    if actual.iter().map(String::as_str).collect::<BTreeSet<_>>() != expected {
        return Err(invalid_entry(
            "capability graph package directory omits an authenticated file",
        ));
    }
    Ok(())
}

fn read_bounded_file(path: &Path, maximum_bytes: u64) -> Result<Vec<u8>, WasmUdfPackageError> {
    let file = open_private_regular_file(path)?;
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    if metadata.len() > maximum_bytes {
        return Err(WasmUdfPackageError::FileTooLarge {
            path: path.to_owned(),
            maximum_bytes,
        });
    }
    let capacity =
        usize::try_from(metadata.len()).map_err(|_| WasmUdfPackageError::FileTooLarge {
            path: path.to_owned(),
            maximum_bytes,
        })?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(maximum_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| io_error(path, source))?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(WasmUdfPackageError::FileTooLarge {
            path: path.to_owned(),
            maximum_bytes,
        });
    }
    Ok(bytes)
}

fn open_private_regular_file(path: &Path) -> Result<File, WasmUdfPackageError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|source| io_error(path, source))?;
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    if !metadata.file_type().is_file() || !has_private_permissions(&metadata) {
        return Err(WasmUdfPackageError::InvalidFile {
            path: path.to_owned(),
        });
    }
    Ok(file)
}

#[cfg(unix)]
fn has_private_permissions(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o077 == 0
}

#[cfg(not(unix))]
fn has_private_permissions(_metadata: &fs::Metadata) -> bool {
    true
}

fn json_payload(bytes: &[u8]) -> &[u8] {
    bytes.strip_suffix(b"\n").unwrap_or(bytes)
}

fn require_canonical_json(name: &'static str, bytes: &[u8]) -> Result<(), WasmUdfPackageError> {
    let payload = json_payload(bytes);
    if bytes.len() != payload.len() + 1 || !bytes.ends_with(b"\n") {
        return Err(WasmUdfPackageError::NonCanonicalJson(name));
    }
    let value: JsonValue =
        serde_json::from_slice(payload).map_err(WasmUdfPackageError::MalformedPackageEntry)?;
    let mut canonical = Vec::with_capacity(payload.len());
    write_canonical_json(&value, &mut canonical)
        .expect("writing canonical JSON to a byte vector cannot fail");
    if canonical != payload {
        return Err(WasmUdfPackageError::NonCanonicalJson(name));
    }
    Ok(())
}

fn write_canonical_json(value: &JsonValue, output: &mut Vec<u8>) -> std::io::Result<()> {
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            serde_json::to_writer(output, value)?;
        },
        JsonValue::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output)?;
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
                write_canonical_json(
                    values
                        .get(key)
                        .expect("canonical JSON object key disappeared"),
                    output,
                )?;
            }
            output.push(b'}');
        },
    }
    Ok(())
}

fn validate_artifact_identity(
    name: &'static str,
    package_artifact: &PackageArtifact,
    manifest_size: u64,
    manifest_sha256: &str,
    maximum_bytes: u64,
) -> Result<(), WasmUdfPackageError> {
    validate_sha256("package-entry.artifact.sha256", &package_artifact.sha256)?;
    if package_artifact.size > maximum_bytes {
        return Err(invalid_entry(format!(
            "{name} exceeds its {maximum_bytes}-byte limit"
        )));
    }
    if package_artifact.size != manifest_size {
        return Err(WasmUdfPackageError::ArtifactSize {
            name,
            actual_bytes: package_artifact.size,
            expected_bytes: manifest_size,
        });
    }
    if package_artifact.sha256 != manifest_sha256 {
        return Err(WasmUdfPackageError::ArtifactDigest(name));
    }
    Ok(())
}

fn validate_artifact_file(
    path: &Path,
    name: &'static str,
    expected: &PackageArtifact,
) -> Result<(), WasmUdfPackageError> {
    let file = open_private_regular_file(path)?;
    validate_open_artifact_file(path, name, expected, &file)
}

fn validate_open_artifact_file(
    path: &Path,
    name: &'static str,
    expected: &PackageArtifact,
    file: &File,
) -> Result<(), WasmUdfPackageError> {
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    if metadata.len() != expected.size {
        return Err(WasmUdfPackageError::ArtifactSize {
            name,
            actual_bytes: metadata.len(),
            expected_bytes: expected.size,
        });
    }
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let actual_bytes = std::io::copy(
        &mut reader.by_ref().take(expected.size.saturating_add(1)),
        &mut digest,
    )
    .map_err(|source| io_error(path, source))?;
    if actual_bytes != expected.size {
        return Err(WasmUdfPackageError::ArtifactSize {
            name,
            actual_bytes,
            expected_bytes: expected.size,
        });
    }
    if format!("{:x}", digest.finalize()) != expected.sha256 {
        return Err(WasmUdfPackageError::ArtifactDigest(name));
    }
    Ok(())
}

fn validate_artifact_bytes(
    name: &'static str,
    bytes: &[u8],
    expected: &PackageArtifact,
) -> Result<(), WasmUdfPackageError> {
    if bytes.len() as u64 != expected.size {
        return Err(WasmUdfPackageError::ArtifactSize {
            name,
            actual_bytes: bytes.len() as u64,
            expected_bytes: expected.size,
        });
    }
    if sha256(bytes) != expected.sha256 {
        return Err(WasmUdfPackageError::ArtifactDigest(name));
    }
    Ok(())
}

fn snapshot_validated_serialized_module_file(
    path: &Path,
    name: &'static str,
    expected: &PackageArtifact,
    serialized_module_snapshot_directory: &Path,
) -> Result<File, WasmUdfPackageError> {
    let mut source_file = open_private_regular_file(path)?;
    let source_metadata = source_file
        .metadata()
        .map_err(|source| io_error(path, source))?;
    if source_metadata.len() != expected.size {
        return Err(WasmUdfPackageError::ArtifactSize {
            name,
            actual_bytes: source_metadata.len(),
            expected_bytes: expected.size,
        });
    }

    validate_serialized_module_snapshot_directory_layout(serialized_module_snapshot_directory)?;
    let mut snapshot = tempfile::tempfile_in(serialized_module_snapshot_directory)
        .map_err(|source| io_error(serialized_module_snapshot_directory, source))?;
    validate_serialized_module_snapshot_file(&snapshot, serialized_module_snapshot_directory)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    let mut actual_bytes = 0;
    loop {
        let remaining = expected.size.saturating_add(1).saturating_sub(actual_bytes);
        if remaining == 0 {
            break;
        }
        let buffer_len = remaining.min(buffer.len() as u64) as usize;
        let bytes_read = source_file
            .read(&mut buffer[..buffer_len])
            .map_err(|source| io_error(path, source))?;
        if bytes_read == 0 {
            break;
        }
        let bytes = &buffer[..bytes_read];
        digest.update(bytes);
        snapshot
            .write_all(bytes)
            .map_err(|source| io_error(path, source))?;
        actual_bytes = actual_bytes
            .checked_add(bytes_read as u64)
            .expect("bounded serialized module size overflowed while snapshotting");
    }
    if actual_bytes != expected.size {
        return Err(WasmUdfPackageError::ArtifactSize {
            name,
            actual_bytes,
            expected_bytes: expected.size,
        });
    }
    if format!("{:x}", digest.finalize()) != expected.sha256 {
        return Err(WasmUdfPackageError::ArtifactDigest(name));
    }
    snapshot.rewind().map_err(|source| io_error(path, source))?;
    Ok(snapshot)
}

pub(crate) fn snapshot_serialized_module_bytes(
    bytes: &[u8],
    serialized_module_snapshot_directory: &Path,
) -> Result<File, WasmUdfPackageError> {
    validate_serialized_module_snapshot_directory_layout(serialized_module_snapshot_directory)?;
    let mut snapshot = tempfile::tempfile_in(serialized_module_snapshot_directory)
        .map_err(|source| io_error(serialized_module_snapshot_directory, source))?;
    validate_serialized_module_snapshot_file(&snapshot, serialized_module_snapshot_directory)?;
    snapshot
        .set_len(0)
        .map_err(|source| io_error(serialized_module_snapshot_directory, source))?;
    snapshot
        .write_all(bytes)
        .map_err(|source| io_error(serialized_module_snapshot_directory, source))?;
    snapshot
        .rewind()
        .map_err(|source| io_error(serialized_module_snapshot_directory, source))?;
    Ok(snapshot)
}

pub(crate) fn validate_serialized_module_snapshot_directory(
    path: &Path,
) -> Result<(), WasmUdfPackageError> {
    validate_serialized_module_snapshot_directory_layout(path)?;
    let snapshot = tempfile::tempfile_in(path).map_err(|source| io_error(path, source))?;
    validate_serialized_module_snapshot_file(&snapshot, path)?;
    Ok(())
}

fn validate_serialized_module_snapshot_directory_layout(
    path: &Path,
) -> Result<(), WasmUdfPackageError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| io_error(path, source))?;
    if !is_owner_private_serialized_module_snapshot_directory(&metadata) {
        return Err(WasmUdfPackageError::InvalidDirectory(path.to_owned()));
    }
    Ok(())
}

#[cfg(unix)]
fn is_owner_private_serialized_module_snapshot_directory(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::{
        MetadataExt,
        PermissionsExt,
    };

    // SAFETY: `geteuid` reads the calling process's effective UID and requires
    // no pointers, shared state, or preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    metadata.file_type().is_dir()
        && metadata.permissions().mode() & 0o7777 == 0o700
        // The loader will create the anonymous file as the backend's effective
        // user. Requiring that owner prevents a privileged process from using
        // another principal's otherwise mode-0700 directory for executable AOT.
        && metadata.uid() == effective_uid
}

#[cfg(not(unix))]
fn is_owner_private_serialized_module_snapshot_directory(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_dir() && has_exact_permissions(metadata, 0o700)
}

fn validate_serialized_module_snapshot_file(
    snapshot: &File,
    directory: &Path,
) -> Result<(), WasmUdfPackageError> {
    snapshot
        .set_len(1)
        .map_err(|source| io_error(directory, source))?;

    #[cfg(all(target_os = "linux", not(test)))]
    {
        use std::os::fd::AsRawFd;

        let mut filesystem = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // `snapshot` remains open while fstatfs reads the filesystem that will
        // hold Wasmtime's file-backed mapping.
        if unsafe { libc::fstatfs(snapshot.as_raw_fd(), filesystem.as_mut_ptr()) } != 0 {
            return Err(io_error(directory, std::io::Error::last_os_error()));
        }
        // A serialized module can be 1 GiB. Keeping its snapshot on tmpfs
        // would turn an AOT load into an unaccounted shmem allocation.
        if unsafe { filesystem.assume_init() }.f_type == libc::TMPFS_MAGIC {
            return Err(WasmUdfPackageError::InvalidDirectory(directory.to_owned()));
        }
    }

    #[cfg(all(unix, not(test)))]
    {
        use std::os::fd::AsRawFd;

        // Match Wasmtime's private writable file mapping, then prove that the
        // mount permits its code pages to become executable before loading a
        // real artifact from the configured snapshot directory.
        let mapping = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                1,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE,
                snapshot.as_raw_fd(),
                0,
            )
        };
        if mapping == libc::MAP_FAILED {
            return Err(io_error(directory, std::io::Error::last_os_error()));
        }
        let mprotect_error =
            (unsafe { libc::mprotect(mapping, 1, libc::PROT_READ | libc::PROT_EXEC) } != 0)
                .then(std::io::Error::last_os_error);
        // The mapping is no longer needed after the mount-capability probe.
        let munmap_error =
            (unsafe { libc::munmap(mapping, 1) } != 0).then(std::io::Error::last_os_error);
        if let Some(source) = mprotect_error.or(munmap_error) {
            return Err(io_error(directory, source));
        }
    }

    snapshot
        .set_len(0)
        .map_err(|source| io_error(directory, source))?;
    Ok(())
}

fn validate_sha256(field: &str, value: &str) -> Result<(), WasmUdfPackageError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_entry(format!(
            "{field} must be exactly 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn invalid_entry(reason: impl Into<String>) -> WasmUdfPackageError {
    WasmUdfPackageError::InvalidPackageEntry(reason.into())
}

fn invalid_provenance(reason: impl Into<String>) -> WasmUdfPackageError {
    WasmUdfPackageError::InvalidBuildProvenance(reason.into())
}

fn invalid_deployment_manifest(reason: impl Into<String>) -> WasmUdfPackageError {
    WasmUdfPackageError::InvalidDeploymentManifest(reason.into())
}

fn io_error(path: &Path, source: std::io::Error) -> WasmUdfPackageError {
    WasmUdfPackageError::Io {
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::os::unix::fs::PermissionsExt;

    use anyhow::Context;
    use serde_json::json;

    use super::{
        legacy_capability_graph_v1::{
            capability_graph_manifest_sha256,
            validate_capability_graph_leaf_engine,
            validate_capability_graph_manifest_schema,
            validate_capability_graph_module_contract,
        },
        *,
    };
    use crate::environment::udf::{
        module_graph_registry::tests::module_graph_cohort_contract,
        static_hermes_wasmtime_gate::{
            GENERATED_ENGINE_COMPATIBILITY_SHA256,
            GENERATED_WASMTIME_REVISION,
        },
        wasm_udf_manifest::{
            MANIFEST_SCHEMA_VERSION,
            OPAQUE_VALUE_ABI_VERSION,
        },
    };

    const WASMTIME_REVISION: &str = "wasmtime-test-revision";
    const TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";
    const TARGET_CPU: &str = "baseline";
    const ENGINE_CONFIGURATION_SHA256: &str =
        "bebac200e042d7c2678b574ba75645f1bf06bb9295982d46184384b0376874b6";
    const DEPLOYED_MODULE_SHA256: &str =
        "1111111111111111111111111111111111111111111111111111111111111111";
    const DEPLOYED_SOURCE_PACKAGE_SHA256: &str =
        "2222222222222222222222222222222222222222222222222222222222222222";
    const REAL_DEPLOYMENT_MANIFEST_ENV: &str =
        "CONVEX_STATIC_HERMES_WASM_GATE_REAL_DEPLOYMENT_MANIFEST";
    const REAL_RUNTIME_REGISTRY_ROOT_ENV: &str =
        "CONVEX_STATIC_HERMES_WASM_GATE_REAL_RUNTIME_REGISTRY_ROOT";

    #[test]
    fn capability_graph_manifest_digest_rejects_tampering() -> anyhow::Result<()> {
        let mut manifest = json!({
            "base": {"moduleId": "base"},
            "graphSha256": "0".repeat(64),
            "kind": CAPABILITY_GRAPH_MANIFEST_KIND,
            "schemaVersion": CAPABILITY_GRAPH_MANIFEST_SCHEMA_VERSION,
        });
        let identity = capability_graph_manifest_sha256(&manifest)?;
        manifest["graphSha256"] = JsonValue::String(identity.clone());
        assert_eq!(capability_graph_manifest_sha256(&manifest)?, identity);

        manifest["base"]["moduleId"] = JsonValue::String("tampered".to_owned());
        assert_ne!(capability_graph_manifest_sha256(&manifest)?, identity);
        Ok(())
    }

    #[test]
    fn capability_graph_requires_its_explicit_schema() {
        assert!(validate_capability_graph_manifest_schema(
            CAPABILITY_GRAPH_MANIFEST_KIND,
            CAPABILITY_GRAPH_MANIFEST_SCHEMA_VERSION,
        )
        .is_ok());
        for legacy_schema in [
            CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION,
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
        ] {
            assert!(validate_capability_graph_manifest_schema(
                CAPABILITY_GRAPH_MANIFEST_KIND,
                legacy_schema,
            )
            .is_err());
        }
        assert!(validate_capability_graph_manifest_schema(
            CAPABILITY_ENTRY_MANIFEST_KIND,
            CAPABILITY_GRAPH_MANIFEST_SCHEMA_VERSION,
        )
        .is_err());
    }

    #[test]
    fn capability_graph_rejects_leaf_engine_identity_drift() -> anyhow::Result<()> {
        let graph_engine = json!({
            "compatibilitySha256": "a".repeat(64),
            "config": {
                "consumeFuel": true,
                "epochInterruption": true,
                "profilingStrategy": "perfmap",
                "wasmExceptions": true,
            },
            "configurationSha256": "b".repeat(64),
            "package": {
                "binary": {"sha256": "c".repeat(64), "size": 1},
                "kind": "precompiler-material",
                "manifestKind": "precompiler-manifest",
                "manifestSchemaVersion": 1,
                "manifestSha256": "d".repeat(64),
                "packageId": "e".repeat(64),
                "sourceTreeSha256": "f".repeat(64),
                "targetTriple": TARGET_TRIPLE,
                "wasmtimeRevision": WASMTIME_REVISION,
            },
            "revision": WASMTIME_REVISION,
            "target": {
                "cpu": TARGET_CPU,
                "platform": {"objectFormat": "elf"},
                "triple": TARGET_TRIPLE,
            },
        });
        let graph: CohortEngineIdentity = serde_json::from_value(graph_engine.clone())?;
        let leaf: CohortEngineIdentity = serde_json::from_value(graph_engine.clone())?;
        assert!(validate_capability_graph_leaf_engine(&graph, &leaf).is_ok());

        let mut drifted_leaf = graph_engine;
        drifted_leaf["target"]["platform"]["objectFormat"] =
            JsonValue::String("different".to_owned());
        let drifted_leaf: CohortEngineIdentity = serde_json::from_value(drifted_leaf)?;
        assert!(validate_capability_graph_leaf_engine(&graph, &drifted_leaf).is_err());
        Ok(())
    }

    #[test]
    fn capability_graph_contract_rejects_provider_and_type_drift() -> anyhow::Result<()> {
        let memory_type = CapabilityGraphExternType::Memory {
            maximum_pages: Some(8),
            memory64: false,
            minimum_pages: 4,
            page_size_bytes: 65_536,
            shared: false,
        };
        let available = BTreeMap::from([(
            "base".to_owned(),
            (
                "graph-base".to_owned(),
                BTreeMap::from([("memory".to_owned(), memory_type.clone())]),
            ),
        )]);
        let module = |provider: &str, ty: CapabilityGraphExternType| CapabilityGraphModule {
            artifacts: CapabilityGraphModuleArtifacts {
                core_wasm: CapabilityGraphArtifact {
                    path: "shared-0000.wasm".to_owned(),
                    sha256: "a".repeat(64),
                    size: 1,
                },
                serialized_module: CapabilityGraphArtifact {
                    path: "shared-0000.cwasm".to_owned(),
                    sha256: "b".repeat(64),
                    size: 1,
                },
            },
            contract: CapabilityGraphModuleContract {
                imports: vec![CapabilityGraphImportContract {
                    module: "graph-base".to_owned(),
                    name: "memory".to_owned(),
                    provider: CapabilityGraphImportProvider::Module {
                        module_id: provider.to_owned(),
                    },
                    ty,
                }],
                exports: vec![],
            },
            module_id: "shared".to_owned(),
            provider_namespace: "graph-shared".to_owned(),
            role: CapabilityGraphModuleRole::Shared,
        };
        assert!(validate_capability_graph_module_contract(
            &module("base", memory_type.clone()),
            &available,
            false,
        )
        .is_ok());
        assert!(validate_capability_graph_module_contract(
            &module("replacement", memory_type.clone()),
            &available,
            false,
        )
        .is_err());
        assert!(validate_capability_graph_module_contract(
            &module(
                "base",
                CapabilityGraphExternType::Memory {
                    maximum_pages: Some(8),
                    memory64: false,
                    minimum_pages: 5,
                    page_size_bytes: 65_536,
                    shared: false,
                },
            ),
            &available,
            false,
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn artifact_pipeline_provenance_revision_is_not_a_runtime_compatibility_gate() {
        for kind in [
            "convex-wasm-artifact-pipeline-v7",
            "convex-wasm-artifact-pipeline-v8",
            "convex-wasm-artifact-pipeline-v9",
        ] {
            validate_artifact_pipeline_kind(kind)
                .expect("valid authenticated pipeline provenance revision was rejected");
        }
        for kind in [
            "convex-wasm-artifact-pipeline-v",
            "convex-wasm-artifact-pipeline-v0",
            "convex-wasm-artifact-pipeline-v08",
            "other-pipeline-v8",
        ] {
            assert!(validate_artifact_pipeline_kind(kind).is_err());
        }
    }

    #[test]
    fn capability_runtime_main_compile_flags_bind_core_prefix_and_closed_topology_mode() {
        let core_flags = vec!["-O2".to_owned(), "-DNDEBUG".to_owned()];
        let runtime_main_flags = vec![
            "-O2".to_owned(),
            "-DNDEBUG".to_owned(),
            "-DRUNTIME_MAIN_DIAGNOSTICS=1".to_owned(),
            "-DRUNTIME_MAIN_TEST_CONTROL=1".to_owned(),
        ];
        assert!(capability_runtime_main_compile_flags_match(
            &core_flags,
            &runtime_main_flags,
            None,
        ));

        let missing_core_flag = vec!["-O2".to_owned(), "-DRUNTIME_MAIN_DIAGNOSTICS=1".to_owned()];
        assert!(!capability_runtime_main_compile_flags_match(
            &core_flags,
            &missing_core_flag,
            None,
        ));
        let wrong_core_flag = vec!["-O3".to_owned(), "-DNDEBUG".to_owned()];
        assert!(!capability_runtime_main_compile_flags_match(
            &core_flags,
            &wrong_core_flag,
            None,
        ));
        let extra_flag_inside_core_prefix = vec![
            "-O2".to_owned(),
            "-DRUNTIME_MAIN_DIAGNOSTICS=1".to_owned(),
            "-DNDEBUG".to_owned(),
        ];
        assert!(!capability_runtime_main_compile_flags_match(
            &core_flags,
            &extra_flag_inside_core_prefix,
            None,
        ));

        let mut multi_entry_flags = runtime_main_flags.clone();
        multi_entry_flags.push(MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG.to_owned());
        assert!(capability_runtime_main_compile_flags_match(
            &core_flags,
            &multi_entry_flags,
            Some(MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG),
        ));
        let mut chunk_flags = runtime_main_flags.clone();
        chunk_flags.push(CHUNK_APPLICATION_UNIT_COMPILE_FLAG.to_owned());
        assert!(capability_runtime_main_compile_flags_match(
            &core_flags,
            &chunk_flags,
            Some(CHUNK_APPLICATION_UNIT_COMPILE_FLAG),
        ));

        let missing_topology_flag = runtime_main_flags.clone();
        assert!(!capability_runtime_main_compile_flags_match(
            &core_flags,
            &missing_topology_flag,
            Some(MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG),
        ));
        let mut wrong_topology_flag = runtime_main_flags.clone();
        wrong_topology_flag.push(CHUNK_APPLICATION_UNIT_COMPILE_FLAG.to_owned());
        assert!(!capability_runtime_main_compile_flags_match(
            &core_flags,
            &wrong_topology_flag,
            Some(MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG),
        ));
        let mut flag_after_topology = multi_entry_flags.clone();
        flag_after_topology.push("-DRUNTIME_MAIN_TEST_CONTROL=1".to_owned());
        assert!(!capability_runtime_main_compile_flags_match(
            &core_flags,
            &flag_after_topology,
            Some(MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG),
        ));
        let mut duplicated_topology_flag = multi_entry_flags;
        duplicated_topology_flag.insert(
            core_flags.len(),
            MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG.to_owned(),
        );
        assert!(!capability_runtime_main_compile_flags_match(
            &core_flags,
            &duplicated_topology_flag,
            Some(MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG),
        ));
        let reserved_topology_flag_in_v2 = vec![
            "-O2".to_owned(),
            "-DNDEBUG".to_owned(),
            CHUNK_APPLICATION_UNIT_COMPILE_FLAG.to_owned(),
        ];
        assert!(!capability_runtime_main_compile_flags_match(
            &core_flags,
            &reserved_topology_flag_in_v2,
            None,
        ));
        let core_flags_with_reserved_mode = vec![
            "-O2".to_owned(),
            MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG.to_owned(),
        ];
        let mut runtime_flags_with_duplicated_mode = core_flags_with_reserved_mode.clone();
        runtime_flags_with_duplicated_mode
            .push(MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG.to_owned());
        assert!(!capability_runtime_main_compile_flags_match(
            &core_flags_with_reserved_mode,
            &runtime_flags_with_duplicated_mode,
            Some(MULTI_ENTRY_APPLICATION_UNIT_COMPILE_FLAG),
        ));
    }

    fn capability_split_provenance_json_for_test() -> JsonValue {
        let entry = capability_manifest_entry_for_test("convex/example.ts", "example", None);
        let entry_id = entry.entry_id.clone();
        let pipeline_kind = "convex-wasm-artifact-pipeline-v9";
        let semantic_environment = json!({
            "LANG": "C",
            "LC_ALL": "C",
            "SOURCE_DATE_EPOCH": "0",
            "TZ": "UTC",
        });
        let emscripten = json!({
            "llvmRevision": "e".repeat(40),
            "materials": "1".repeat(64),
            "revision": "f".repeat(40),
        });
        let static_hermes = json!({
            "materials": "2".repeat(64),
            "revision": "a".repeat(40),
        });
        let request_envelope =
            serde_json::to_value(capability_request_envelope_identity_for_test())
                .expect("request-envelope fixture serializes");
        let value_codec = serde_json::to_value(guest_native_json_codec_identity_for_test())
            .expect("value-codec fixture serializes");
        let producer_implementation = json!({
            "kind": CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND,
            "sha256": "9".repeat(64),
        });
        let producer_identity = json!({
            "kind": CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND,
            "manifest": {
                "path": "scripts/producer-manifest.json",
                "sha256": "8".repeat(64),
                "size": 1,
            },
            "nodeVersion": "v24.0.0",
            "sha256": "9".repeat(64),
            "sources": [{
                "path": "scripts/producer.mjs",
                "sha256": "7".repeat(64),
                "size": 1,
            }],
        });
        let generated_source = json!({"sha256": "3".repeat(64), "size": 1});
        let generated_c = json!({"sha256": "4".repeat(64), "size": 1});
        let formatter_generated_c = json!({
            "exportedUnitName": "convex_wasm_console_formatter",
            "flags": ["-O", "-Xenable-tdz", "-emit-c"],
            "generatedSource": generated_source,
            "pipelineKind": pipeline_kind,
            "producerImplementation": producer_implementation,
            "semanticEnvironment": semantic_environment,
            "staticHermes": static_hermes,
            "unitRole": "shared-untyped-console-formatter",
        });
        json!({
            "applicationUnits": [{
                "entryId": entry_id,
                "entrySlot": 0,
                "identities": {
                    "exportObject": {
                        "compileFlags": ["-O2"],
                        "effectExecutionMode": "guest-promise-event-loop",
                        "emscripten": emscripten,
                        "generatedC": generated_c,
                        "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
                        "pipelineKind": pipeline_kind,
                        "producerImplementation": producer_implementation,
                        "runtimeHeaders": "5".repeat(64),
                        "semanticEnvironment": semantic_environment,
                        "unitRole": "untyped-application",
                        "valueMode": "guest-native-json",
                    },
                    "generatedC": {
                        "capabilityEntry": {
                            "localProfile": entry.local_profile,
                            "entryId": entry_id,
                            "runtimeSurfacePolicySha256": "6".repeat(64),
                        },
                        "exportedUnitName": format!("convex_wasm_entry_{entry_id}"),
                        "flags": ["-O", "-Xenable-tdz", "-emit-c"],
                        "generatedSource": generated_source,
                        "guestSourceProvenance": {},
                        "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
                        "pipelineKind": pipeline_kind,
                        "producerImplementation": producer_implementation,
                        "semanticEnvironment": semantic_environment,
                        "staticHermes": static_hermes,
                        "unitRole": "untyped-application",
                        "valueMode": "guest-native-json",
                    },
                },
            }],
            "artifactPipelineSha256": "7".repeat(64),
            "bridge": {
                "generatedC": {
                    "effectExecutionMode": "guest-promise-event-loop",
                    "exportedUnitName": "convex_wasm_capability_bridge",
                    "flags": ["-typed", "-O", "-Xenable-tdz", "-emit-c"],
                    "generatedSource": generated_source,
                    "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
                    "pipelineKind": pipeline_kind,
                    "producerImplementation": producer_implementation,
                    "requestEnvelope": request_envelope,
                    "semanticEnvironment": semantic_environment,
                    "staticHermes": static_hermes,
                    "unitRole": "shared-typed-capability-bridge",
                    "valueCodec": value_codec,
                    "valueMode": "guest-native-json",
                },
                "object": {
                    "compileFlags": ["-O2"],
                    "emscripten": emscripten,
                    "generatedC": generated_c,
                    "pipelineKind": pipeline_kind,
                    "producerImplementation": producer_implementation,
                    "runtimeHeaders": "5".repeat(64),
                    "semanticEnvironment": semantic_environment,
                    "unitRole": "shared-typed-capability-bridge",
                },
            },
            "engineIdentity": {
                "engineCompatibilitySha256": "8".repeat(64),
                "engineConfig": {
                    "consumeFuel": true,
                    "epochInterruption": true,
                    "profilingStrategy": "perf-map",
                    "wasmExceptions": true,
                },
                "kind": ENGINE_IDENTITY_KIND,
                "target": {"cpu": TARGET_CPU, "triple": TARGET_TRIPLE},
            },
            "formatter": {
                "generatedC": formatter_generated_c,
                "generatedSource": generated_source,
                "object": {
                    "compileFlags": ["-O2"],
                    "emscripten": emscripten,
                    "generatedC": generated_c,
                    "generatedCIdentitySha256": sha256(&canonical_bytes(&formatter_generated_c)),
                    "pipelineKind": pipeline_kind,
                    "producerImplementation": producer_implementation,
                    "runtimeHeaders": "5".repeat(64),
                    "semanticEnvironment": semantic_environment,
                    "unitRole": "shared-untyped-console-formatter",
                },
            },
            "identities": {
                "coreWasm": {},
                "runtimeMainObject": {},
                "selectorObject": {},
                "wasmtimeAot": {},
            },
            "kind": CAPABILITY_ENTRY_PROVENANCE_KIND_V2,
            "materials": {},
            "producerIdentity": producer_identity,
            "requestEnvelope": request_envelope,
            "semanticEnvironment": semantic_environment,
            "valueCodec": value_codec,
        })
    }

    fn capability_native_artifact_provenance_json_for_test() -> JsonValue {
        let mut provenance = capability_split_provenance_json_for_test();
        let producer_implementation = provenance["producerIdentity"]
            .as_object()
            .map(|identity| {
                json!({
                    "kind": identity["kind"].clone(),
                    "sha256": identity["sha256"].clone(),
                })
            })
            .expect("producer identity fixture must be an object");
        for field in [
            "coreWasm",
            "runtimeMainObject",
            "selectorObject",
            "wasmtimeAot",
        ] {
            provenance["identities"][field] = json!({
                "producerImplementation": producer_implementation,
            });
        }
        let native_stage = |stage: &mut JsonValue| {
            let stage = stage
                .as_object_mut()
                .expect("native artifact fixture stage must be an object");
            assert!(stage.remove("producerImplementation").is_some());
            stage.insert(
                "nativeArtifactIdentitySchemaVersion".to_owned(),
                json!(CAPABILITY_NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION),
            );
        };
        for unit in provenance["applicationUnits"]
            .as_array_mut()
            .expect("application units fixture must be an array")
        {
            native_stage(&mut unit["identities"]["generatedC"]);
            native_stage(&mut unit["identities"]["exportObject"]);
        }
        native_stage(&mut provenance["bridge"]["generatedC"]);
        native_stage(&mut provenance["bridge"]["object"]);
        native_stage(&mut provenance["formatter"]["generatedC"]);
        native_stage(&mut provenance["formatter"]["object"]);
        provenance["formatter"]["object"]["generatedCIdentitySha256"] = json!(sha256(
            &canonical_bytes(&provenance["formatter"]["generatedC"])
        ));
        for field in [
            "coreWasm",
            "runtimeMainObject",
            "selectorObject",
            "wasmtimeAot",
        ] {
            native_stage(&mut provenance["identities"][field]);
        }
        provenance
    }

    fn capability_official_output_chunk_descriptor_json_for_test() -> JsonValue {
        let chunk_identity = "a".repeat(64);
        let publication_identity = "b".repeat(64);
        json!({
            "entries": [{
                "dependencyGraphSha256": "c".repeat(64),
                "entryModulePath": "example.js",
                "entryPath": "convex/example.ts",
                "entryPublicationUnitSlot": 1,
                "entrySlot": 0,
                "handoffSlot": 0,
                "modulePath": "example",
                "routes": [{
                    "exportName": "run",
                    "udfKind": "query",
                    "visibility": "public",
                }],
            }],
            "identitySha256": "d".repeat(64),
            "initialization": {
                "chunkSlotCount": 1,
                "entryPublicationUnitSlots": [1],
                "kind": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_INITIALIZATION_KIND,
                "namespaceSlotCount": 1,
            },
            "kind": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_DESCRIPTOR_KIND,
            "nativeDescriptor": {
                "destruction": "destroy-store-on-any-initialization-failure",
                "initialization": "recursive-closed-literal-require-with-provisional-cjs-namespaces",
                "kind": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_NATIVE_DESCRIPTOR_KIND,
                "publication": "authenticated-selected-entry-wrapper-validation-after-selected-closure-initialization",
                "slots": "closed-numbered-namespace-slots-with-per-entry-publication-units",
            },
            "units": [
                {
                    "applicationUnitSlot": 0,
                    "chunkSlot": 0,
                    "dependencies": [],
                    "entryPublication": false,
                    "entrySymbol": format!("sh_export_convex_wasm_official_chunk_{chunk_identity}"),
                    "exportedUnitName": format!("convex_wasm_official_chunk_{chunk_identity}"),
                    "identitySha256": chunk_identity,
                    "javascript": {"sha256": "e".repeat(64), "size": 1},
                    "kind": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_UNIT_KIND,
                    "publicationHandoffSlot": -1,
                },
                {
                    "applicationUnitSlot": 1,
                    "chunkSlot": -1,
                    "dependencies": [],
                    "entryPublication": true,
                    "entrySymbol": format!("sh_export_convex_wasm_official_chunk_{publication_identity}"),
                    "exportedUnitName": format!("convex_wasm_official_chunk_{publication_identity}"),
                    "identitySha256": publication_identity,
                    "javascript": {"sha256": "f".repeat(64), "size": 1},
                    "kind": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_ENTRY_PUBLICATION_UNIT_KIND,
                    "publicationHandoffSlot": 0,
                },
            ],
        })
    }

    #[test]
    fn capability_entry_v2_provenance_accepts_closed_per_entry_publication_descriptor() {
        let mut provenance = capability_split_provenance_json_for_test();
        provenance["chunkApplication"] = json!({
            "descriptor": capability_official_output_chunk_descriptor_json_for_test(),
            "units": [],
        });
        let parsed: CapabilityEntryBuildProvenance = serde_json::from_value(provenance.clone())
            .expect("per-entry publication provenance failed to parse");
        assert!(matches!(parsed, CapabilityEntryBuildProvenance::V2(_)));

        let mut unknown = provenance.clone();
        unknown["chunkApplication"]["descriptor"]["initialization"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<CapabilityEntryBuildProvenance>(unknown).is_err());

        let mut missing_publication = provenance.clone();
        missing_publication["chunkApplication"]["descriptor"]["entries"][0]
            .as_object_mut()
            .expect("descriptor entry must be an object")
            .remove("entryPublicationUnitSlot");
        assert!(
            serde_json::from_value::<CapabilityEntryBuildProvenance>(missing_publication).is_err()
        );

        let mut missing_handoff = provenance;
        missing_handoff["chunkApplication"]["descriptor"]["units"][1]
            .as_object_mut()
            .expect("descriptor unit must be an object")
            .remove("publicationHandoffSlot");
        assert!(serde_json::from_value::<CapabilityEntryBuildProvenance>(missing_handoff).is_err());
    }

    #[test]
    fn capability_entry_provenance_discriminator_separates_historical_and_split_shapes() {
        let split_json = capability_split_provenance_json_for_test();
        let parsed_split: CapabilityEntryBuildProvenanceV2 =
            serde_json::from_value(split_json.clone()).expect("split provenance failed to parse");
        let producer_implementation =
            validate_capability_producer_identity(&parsed_split.producer_identity)
                .expect("split provenance producer identity was rejected");
        assert_eq!(
            producer_implementation,
            parsed_split.bridge.generated_c.producer_implementation
        );
        let split = CapabilityEntryBuildProvenance::V2(parsed_split);
        assert!(matches!(split, CapabilityEntryBuildProvenance::V2(_)));
        validate_capability_entry_provenance_shape(&split, 9)
            .expect("split provenance discriminator was rejected");
        assert!(validate_capability_entry_provenance_shape(&split, 8).is_err());

        let mut historical_json = split_json.clone();
        let historical = historical_json
            .as_object_mut()
            .expect("provenance fixture must be an object");
        historical.remove("applicationUnits");
        historical.remove("bridge");
        historical.remove("formatter");
        historical.remove("producerIdentity");
        historical.insert(
            "kind".to_owned(),
            json!(CAPABILITY_ENTRY_PROVENANCE_KIND_V1),
        );
        let historical: CapabilityEntryBuildProvenance =
            serde_json::from_value(historical_json).expect("historical provenance failed to parse");
        assert!(matches!(historical, CapabilityEntryBuildProvenance::V1(_)));
        validate_capability_entry_provenance_shape(&historical, 8)
            .expect("historical provenance discriminator was rejected");
        assert!(validate_capability_entry_provenance_shape(&historical, 9).is_err());

        let mut mislabeled_split = split_json;
        mislabeled_split["kind"] = json!(CAPABILITY_ENTRY_PROVENANCE_KIND_V1);
        let mislabeled_split: CapabilityEntryBuildProvenance =
            serde_json::from_value(mislabeled_split).expect("split shape failed to parse");
        assert!(validate_capability_entry_provenance_shape(&mislabeled_split, 9).is_err());
    }

    #[test]
    fn capability_entry_native_artifact_provenance_reuses_package_producer_identity() {
        let provenance = capability_native_artifact_provenance_json_for_test();
        let parsed: CapabilityEntryBuildProvenance =
            serde_json::from_value(provenance).expect("native artifact provenance failed to parse");
        let CapabilityEntryBuildProvenance::V2(parsed) = parsed else {
            panic!("native artifact provenance did not parse as v2");
        };
        assert_eq!(
            parsed.bridge.generated_c.producer_implementation,
            CapabilityProducerImplementationIdentity {
                kind: CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND.to_owned(),
                sha256: "9".repeat(64),
            }
        );
        let formatter = parsed
            .formatter
            .as_ref()
            .expect("native artifact provenance formatter is missing");
        assert_eq!(
            formatter.object.generated_c_identity_sha256,
            canonical_identity_sha256(&formatter.generated_c)
                .expect("normalized formatter generated-C identity does not canonicalize")
        );

        let mut missing_version = capability_native_artifact_provenance_json_for_test();
        missing_version["bridge"]["generatedC"]
            .as_object_mut()
            .expect("bridge generated-C fixture must be an object")
            .remove("nativeArtifactIdentitySchemaVersion");
        assert!(serde_json::from_value::<CapabilityEntryBuildProvenance>(missing_version).is_err());

        let mut unsupported_version = capability_native_artifact_provenance_json_for_test();
        unsupported_version["identities"]["wasmtimeAot"]["nativeArtifactIdentitySchemaVersion"] =
            json!(CAPABILITY_NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION + 1);
        assert!(
            serde_json::from_value::<CapabilityEntryBuildProvenance>(unsupported_version).is_err()
        );

        let mut repeated_producer = capability_native_artifact_provenance_json_for_test();
        repeated_producer["formatter"]["object"]["producerImplementation"] = json!({
            "kind": CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND,
            "sha256": "9".repeat(64),
        });
        assert!(
            serde_json::from_value::<CapabilityEntryBuildProvenance>(repeated_producer).is_err()
        );

        let mut inconsistent_formatter_identity =
            capability_native_artifact_provenance_json_for_test();
        inconsistent_formatter_identity["formatter"]["object"]["generatedCIdentitySha256"] =
            json!("0".repeat(64));
        assert!(serde_json::from_value::<CapabilityEntryBuildProvenance>(
            inconsistent_formatter_identity
        )
        .is_err());

        let mut unknown_stage_field = capability_native_artifact_provenance_json_for_test();
        unknown_stage_field["applicationUnits"][0]["identities"]["exportObject"]["unexpected"] =
            json!(true);
        assert!(
            serde_json::from_value::<CapabilityEntryBuildProvenance>(unknown_stage_field).is_err()
        );
    }

    #[test]
    fn capability_entry_split_provenance_requires_exact_topology_fields() {
        for field in ["applicationUnits", "bridge", "producerIdentity"] {
            let mut missing = capability_split_provenance_json_for_test();
            missing
                .as_object_mut()
                .expect("provenance fixture must be an object")
                .remove(field);
            assert!(serde_json::from_value::<CapabilityEntryBuildProvenance>(missing).is_err());
        }

        let mut unknown_bridge_field = capability_split_provenance_json_for_test();
        unknown_bridge_field["bridge"]["unexpected"] = json!(true);
        assert!(
            serde_json::from_value::<CapabilityEntryBuildProvenance>(unknown_bridge_field).is_err()
        );

        let mut unknown_application_identity = capability_split_provenance_json_for_test();
        unknown_application_identity["applicationUnits"][0]["identities"]["unexpected"] =
            json!(true);
        assert!(serde_json::from_value::<CapabilityEntryBuildProvenance>(
            unknown_application_identity
        )
        .is_err());

        let mut unknown_producer_field = capability_split_provenance_json_for_test();
        unknown_producer_field["producerIdentity"]["unexpected"] = json!(true);
        assert!(
            serde_json::from_value::<CapabilityEntryBuildProvenance>(unknown_producer_field)
                .is_err()
        );

        for pointer in [
            "/applicationUnits/0/identities/exportObject",
            "/applicationUnits/0/identities/generatedC",
            "/bridge/generatedC",
            "/bridge/object",
            "/formatter/generatedC",
            "/formatter/object",
        ] {
            let mut missing = capability_split_provenance_json_for_test();
            missing
                .pointer_mut(pointer)
                .and_then(JsonValue::as_object_mut)
                .expect("producer identity fixture path must be an object")
                .remove("producerImplementation");
            assert!(serde_json::from_value::<CapabilityEntryBuildProvenance>(missing).is_err());
        }
    }

    #[test]
    fn capability_entry_formatter_topology_v2_requires_exact_ordered_fields() {
        let artifact = json!({"sha256": "a".repeat(64), "size": 1});
        let core = json!({
            "applicationEntryCount": 2,
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryBySlotSymbol": CAPABILITY_APPLICATION_FACTORY_BY_SLOT_SYMBOL,
            "applicationFlags": ["-O", "-Xenable-tdz", "-emit-c"],
            "bridge": {
                "entrySymbol": "sh_export_convex_wasm_capability_bridge",
                "generatedSource": artifact,
                "object": artifact,
            },
            "bridgeFlags": ["-typed", "-O", "-Xenable-tdz", "-emit-c"],
            "formatter": {
                "entrySymbol": "sh_export_convex_wasm_console_formatter",
                "generatedSource": artifact,
                "object": artifact,
            },
            "formatterFlags": ["-O", "-Xenable-tdz", "-emit-c"],
            "initializationOrder": [
                "shared-typed-capability-bridge",
                "shared-untyped-console-formatter",
                "untyped-applications-by-entry-slot",
            ],
            "kind": "convex-wasm-capability-unit-topology-v2",
            "selectedEntryPreparation": CAPABILITY_SELECTED_ENTRY_PREPARATION,
        });
        let parsed_core: CapabilitySplitCoreWasmTopology =
            serde_json::from_value(core.clone()).expect("schema-4 topology-v2 failed to parse");
        let CapabilitySplitCoreWasmTopology::V2(parsed) = &parsed_core else {
            panic!("schema-4 topology-v2 parsed as the legacy topology");
        };
        assert!(has_capability_unit_initialization_order(
            &parsed.initialization_order,
            "shared-untyped-console-formatter",
        ));
        assert_eq!(
            parsed_core.fields().selected_entry_preparation,
            Some(CAPABILITY_SELECTED_ENTRY_PREPARATION)
        );

        for field in [
            "formatter",
            "formatterFlags",
            "initializationOrder",
            "selectedEntryPreparation",
        ] {
            let mut missing = core.clone();
            missing
                .as_object_mut()
                .expect("core topology fixture must be an object")
                .remove(field);
            assert!(serde_json::from_value::<CapabilitySplitCoreWasmTopology>(missing).is_err());
        }
        let mut extra = core.clone();
        extra["formatter"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<CapabilitySplitCoreWasmTopology>(extra).is_err());
        let mut reordered = parsed.initialization_order.clone();
        reordered.swap(0, 1);
        assert!(!has_capability_unit_initialization_order(
            &reordered,
            "shared-untyped-console-formatter",
        ));

        let runtime = json!({
            "applicationEntryCount": 2,
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryBySlotSymbol": CAPABILITY_APPLICATION_FACTORY_BY_SLOT_SYMBOL,
            "applicationSelectorSymbol": "convex_wasm_selected_exported_unit",
            "bridgeEntrySymbol": "sh_export_convex_wasm_capability_bridge",
            "bridgeExportedUnitName": "convex_wasm_capability_bridge",
            "formatterEntrySymbol": "sh_export_convex_wasm_console_formatter",
            "formatterExportedUnitName": "convex_wasm_console_formatter",
            "initializationOrder": [
                "shared-typed-capability-bridge",
                "shared-untyped-console-formatter",
                "untyped-applications-by-entry-slot",
            ],
            "kind": "convex-wasm-capability-unit-topology-v2",
            "selectedEntryPreparation": CAPABILITY_SELECTED_ENTRY_PREPARATION,
        });
        let parsed_runtime =
            serde_json::from_value::<CapabilitySplitRuntimeMainTopology>(runtime.clone())
                .expect("schema-4 runtime topology-v2 failed to parse");
        assert!(capability_formatter_topology_versions_match(
            &parsed_core,
            &parsed_runtime,
            true,
        ));
        assert!(!capability_formatter_topology_versions_match(
            &parsed_core,
            &parsed_runtime,
            false,
        ));
        let mut wrong_core_preparation = core.clone();
        wrong_core_preparation["selectedEntryPreparation"] = json!("implicit-run-initialization");
        let wrong_core_preparation: CapabilitySplitCoreWasmTopology =
            serde_json::from_value(wrong_core_preparation)
                .expect("wrong core preparation shape failed to parse");
        assert!(!capability_formatter_topology_versions_match(
            &wrong_core_preparation,
            &parsed_runtime,
            true,
        ));

        let mut wrong_runtime_preparation = runtime.clone();
        wrong_runtime_preparation["selectedEntryPreparation"] =
            json!("implicit-run-initialization");
        let wrong_runtime_preparation: CapabilitySplitRuntimeMainTopology =
            serde_json::from_value(wrong_runtime_preparation)
                .expect("wrong runtime preparation shape failed to parse");
        assert!(!capability_formatter_topology_versions_match(
            &parsed_core,
            &wrong_runtime_preparation,
            true,
        ));
        for field in [
            "formatterEntrySymbol",
            "formatterExportedUnitName",
            "initializationOrder",
            "selectedEntryPreparation",
        ] {
            let mut missing = runtime.clone();
            missing
                .as_object_mut()
                .expect("runtime topology fixture must be an object")
                .remove(field);
            assert!(serde_json::from_value::<CapabilitySplitRuntimeMainTopology>(missing).is_err());
        }
    }

    #[test]
    fn capability_multi_entry_application_unit_topology_binds_scalar_handoff_order() {
        let artifact = json!({"sha256": "a".repeat(64), "size": 1});
        let application_unit = json!({
            "compilerMode": "static-hermes-untyped-application",
            "entries": [
                {"entryPath": "convex/f.ts", "handoffSlot": 0},
                {"entryPath": "convex/s.ts", "handoffSlot": 1},
            ],
            "entrySymbol": format!("sh_export_convex_wasm_application_unit_{}", "b".repeat(64)),
            "exportedUnitName": format!("convex_wasm_application_unit_{}", "b".repeat(64)),
            "identitySha256": "b".repeat(64),
            "kind": "convex-wasm-multi-entry-application-unit-v1",
            "unitCount": 1,
        });
        let initialization_order = json!([
            "shared-typed-capability-bridge",
            "shared-untyped-runtime-support",
            "untyped-application-units-by-unit-slot",
        ]);
        let core = json!({
            "applicationEntryCount": 2,
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryByUnitSlotSymbol":
                CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL,
            "applicationFlags": ["-O", "-Xenable-tdz", "-emit-c"],
            "applicationUnit": application_unit,
            "applicationUnitCount": 1,
            "applicationUnitCountSymbol": CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL,
            "bridge": {
                "entrySymbol": "sh_export_convex_wasm_capability_bridge",
                "generatedSource": artifact,
                "object": artifact,
            },
            "bridgeFlags": ["-typed", "-O", "-Xenable-tdz", "-emit-c"],
            "formatter": {
                "entrySymbol": "sh_export_convex_wasm_console_formatter",
                "generatedSource": artifact,
                "object": artifact,
            },
            "formatterFlags": ["-O", "-Xenable-tdz", "-emit-c"],
            "initializationOrder": initialization_order,
            "kind": "convex-wasm-capability-unit-topology-v3",
            "selectedEntryPreparation": CAPABILITY_SELECTED_ENTRY_PREPARATION,
        });
        let parsed_core: CapabilitySplitCoreWasmTopology =
            serde_json::from_value(core.clone()).expect("physical core topology failed to parse");
        let core_fields = parsed_core.fields();
        assert_eq!(core_fields.application_entry_count, Some(2));
        assert_eq!(core_fields.application_unit_count, Some(1));
        assert_eq!(
            core_fields.selected_entry_preparation,
            Some(CAPABILITY_SELECTED_ENTRY_PREPARATION)
        );
        assert_eq!(
            core_fields.application_factory_by_unit_slot_symbol,
            Some(CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL)
        );
        assert_eq!(
            core_fields
                .application_unit
                .expect("physical application unit missing")
                .entries
                .iter()
                .map(|entry| (entry.entry_path.as_str(), entry.handoff_slot))
                .collect::<Vec<_>>(),
            vec![("convex/f.ts", 0), ("convex/s.ts", 1)]
        );

        let runtime = json!({
            "applicationEntryCount": 2,
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryByUnitSlotSymbol":
                CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL,
            "applicationSelectorSymbol": "convex_wasm_selected_exported_unit",
            "applicationUnit": application_unit,
            "applicationUnitCount": 1,
            "applicationUnitCountSymbol": CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL,
            "bridgeEntrySymbol": "sh_export_convex_wasm_capability_bridge",
            "bridgeExportedUnitName": "convex_wasm_capability_bridge",
            "formatterEntrySymbol": "sh_export_convex_wasm_console_formatter",
            "formatterExportedUnitName": "convex_wasm_console_formatter",
            "initializationOrder": initialization_order,
            "kind": "convex-wasm-capability-unit-topology-v3",
            "selectedEntryPreparation": CAPABILITY_SELECTED_ENTRY_PREPARATION,
        });
        let parsed_runtime: CapabilitySplitRuntimeMainTopology =
            serde_json::from_value(runtime.clone())
                .expect("physical runtime topology failed to parse");
        assert!(capability_formatter_topology_versions_match(
            &parsed_core,
            &parsed_runtime,
            true,
        ));

        for field in ["selectedEntryPreparation"] {
            let mut missing = core.clone();
            missing
                .as_object_mut()
                .expect("physical core topology must be an object")
                .remove(field);
            assert!(serde_json::from_value::<CapabilitySplitCoreWasmTopology>(missing).is_err());

            let mut missing = runtime.clone();
            missing
                .as_object_mut()
                .expect("physical runtime topology must be an object")
                .remove(field);
            assert!(serde_json::from_value::<CapabilitySplitRuntimeMainTopology>(missing).is_err());
        }

        let mut wrong_core_preparation = core.clone();
        wrong_core_preparation["selectedEntryPreparation"] = json!("implicit-run-initialization");
        let wrong_core_preparation: CapabilitySplitCoreWasmTopology =
            serde_json::from_value(wrong_core_preparation)
                .expect("wrong physical core preparation shape failed to parse");
        assert!(!capability_formatter_topology_versions_match(
            &wrong_core_preparation,
            &parsed_runtime,
            true,
        ));

        let mut mismatched_runtime_preparation = runtime.clone();
        mismatched_runtime_preparation["selectedEntryPreparation"] =
            json!("implicit-run-initialization");
        let mismatched_runtime_preparation: CapabilitySplitRuntimeMainTopology =
            serde_json::from_value(mismatched_runtime_preparation)
                .expect("mismatched physical runtime preparation shape failed to parse");
        assert!(!capability_formatter_topology_versions_match(
            &parsed_core,
            &mismatched_runtime_preparation,
            true,
        ));

        let mut tampered = core;
        tampered["applicationUnit"]["entries"][1]["handoffSlot"] = json!(0);
        let tampered: CapabilitySplitCoreWasmTopology =
            serde_json::from_value(tampered).expect("tampered topology shape failed to parse");
        assert!(validate_physical_application_unit_identity(
            tampered
                .fields()
                .application_unit
                .expect("tampered physical application unit missing"),
            &[
                capability_runtime_entry_for_test('f'),
                capability_runtime_entry_for_test('s'),
            ],
        )
        .is_err());
    }

    #[test]
    fn capability_official_output_chunk_topology_requires_selected_entry_preparation() {
        let artifact = json!({"sha256": "a".repeat(64), "size": 1});
        let descriptor = capability_official_output_chunk_descriptor_json_for_test();
        let core = json!({
            "applicationEntryCount": 1,
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryByUnitSlotSymbol": CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL,
            "applicationFlags": ["-O", "-Xenable-tdz", "-emit-c"],
            "applicationUnitCount": 2,
            "applicationUnitCountSymbol": CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL,
            "applicationUnitMaximum": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_MAXIMUM_UNITS,
            "bridge": {
                "entrySymbol": "sh_export_convex_wasm_capability_bridge",
                "generatedSource": artifact,
                "object": artifact,
            },
            "bridgeFlags": ["-typed", "-O", "-Xenable-tdz", "-emit-c"],
            "chunkApplication": descriptor,
            "formatter": {
                "entrySymbol": "sh_export_convex_wasm_console_formatter",
                "generatedSource": artifact,
                "object": artifact,
            },
            "formatterFlags": ["-O", "-Xenable-tdz", "-emit-c"],
            "initializationOrder": [
                "shared-typed-capability-bridge",
                "shared-untyped-runtime-support",
                "recursive-authenticated-selected-entry-chunk-closure",
                "authenticated-selected-entry-publication-after-selected-closure-initialization",
            ],
            "kind": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_TOPOLOGY_KIND,
            "selectedEntryPreparation": CAPABILITY_SELECTED_ENTRY_PREPARATION,
        });
        let parsed_core: CapabilityOfficialOutputChunkCoreWasmTopology =
            serde_json::from_value(core.clone())
                .expect("official-output core topology failed to parse");
        let runtime = json!({
            "applicationEntryCount": 1,
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryByUnitSlotSymbol": CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL,
            "applicationSelectorSymbol": "convex_wasm_selected_exported_unit",
            "applicationUnitCount": 2,
            "applicationUnitCountSymbol": CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL,
            "applicationUnitMaximum": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_MAXIMUM_UNITS,
            "bridgeEntrySymbol": "sh_export_convex_wasm_capability_bridge",
            "bridgeExportedUnitName": "convex_wasm_capability_bridge",
            "formatterEntrySymbol": "sh_export_convex_wasm_console_formatter",
            "formatterExportedUnitName": "convex_wasm_console_formatter",
            "initializationOrder": core["initializationOrder"].clone(),
            "kind": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_TOPOLOGY_KIND,
            "nativeDescriptor": core["chunkApplication"]["nativeDescriptor"].clone(),
            "selectedEntryPreparation": CAPABILITY_SELECTED_ENTRY_PREPARATION,
        });
        let parsed_runtime: CapabilityOfficialOutputChunkRuntimeMainTopology =
            serde_json::from_value(runtime.clone())
                .expect("official-output runtime topology failed to parse");
        assert!(
            capability_official_output_chunk_selected_entry_preparation_matches(
                &parsed_core,
                &parsed_runtime,
            )
        );

        let mut missing_core = core.clone();
        missing_core
            .as_object_mut()
            .expect("official-output core topology must be an object")
            .remove("selectedEntryPreparation");
        assert!(
            serde_json::from_value::<CapabilityOfficialOutputChunkCoreWasmTopology>(missing_core)
                .is_err()
        );
        let mut missing_runtime = runtime.clone();
        missing_runtime
            .as_object_mut()
            .expect("official-output runtime topology must be an object")
            .remove("selectedEntryPreparation");
        assert!(
            serde_json::from_value::<CapabilityOfficialOutputChunkRuntimeMainTopology>(
                missing_runtime
            )
            .is_err()
        );

        let mut wrong_core_preparation = core;
        wrong_core_preparation["selectedEntryPreparation"] = json!("implicit-run-initialization");
        let wrong_core_preparation: CapabilityOfficialOutputChunkCoreWasmTopology =
            serde_json::from_value(wrong_core_preparation)
                .expect("wrong official-output core preparation shape failed to parse");
        assert!(
            !capability_official_output_chunk_selected_entry_preparation_matches(
                &wrong_core_preparation,
                &parsed_runtime,
            )
        );

        let mut mismatched_runtime_preparation = runtime;
        mismatched_runtime_preparation["selectedEntryPreparation"] =
            json!("implicit-run-initialization");
        let mismatched_runtime_preparation: CapabilityOfficialOutputChunkRuntimeMainTopology =
            serde_json::from_value(mismatched_runtime_preparation)
                .expect("mismatched official-output runtime preparation shape failed to parse");
        assert!(
            !capability_official_output_chunk_selected_entry_preparation_matches(
                &parsed_core,
                &mismatched_runtime_preparation,
            )
        );
    }

    #[test]
    fn capability_entry_formatter_provenance_rejects_identity_tampering() {
        let parse = || {
            let provenance: CapabilityEntryBuildProvenanceV2 =
                serde_json::from_value(capability_split_provenance_json_for_test())
                    .expect("formatter provenance fixture failed to parse");
            provenance
                .formatter
                .expect("formatter provenance fixture omitted the formatter")
        };
        let semantic_environment = json!({
            "LANG": "C",
            "LC_ALL": "C",
            "SOURCE_DATE_EPOCH": "0",
            "TZ": "UTC",
        });
        let emscripten_llvm_revision = "e".repeat(40);
        let emscripten_materials = "1".repeat(64);
        let emscripten_revision = "f".repeat(40);
        let runtime_headers = "5".repeat(64);
        let static_hermes_materials = "2".repeat(64);
        let static_hermes_revision = "a".repeat(40);
        let compile_flags = ["-O2".to_owned()];
        let producer_implementation = CapabilityProducerImplementationIdentity {
            kind: CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND.to_owned(),
            sha256: "9".repeat(64),
        };
        let expected = || CapabilityFormatterExpectedIdentity {
            compile_flags: &compile_flags,
            emscripten_llvm_revision: &emscripten_llvm_revision,
            emscripten_materials: &emscripten_materials,
            emscripten_revision: &emscripten_revision,
            pipeline_kind: "convex-wasm-artifact-pipeline-v9",
            producer_implementation: &producer_implementation,
            runtime_headers: &runtime_headers,
            semantic_environment: &semantic_environment,
            static_hermes_materials: &static_hermes_materials,
            static_hermes_revision: &static_hermes_revision,
            unit_role: "shared-untyped-console-formatter",
        };
        validate_capability_formatter_provenance(&parse(), expected())
            .expect("valid formatter provenance was rejected");

        let mut typed = parse();
        typed.generated_c.flags.insert(0, "-typed".to_owned());
        assert!(validate_capability_formatter_provenance(&typed, expected()).is_err());

        let mut wrong_generated_source = parse();
        wrong_generated_source.generated_c.generated_source.sha256 = "invalid".to_owned();
        assert!(
            validate_capability_formatter_provenance(&wrong_generated_source, expected()).is_err()
        );

        let mut wrong_generated_c = parse();
        wrong_generated_c.object.generated_c.size = 0;
        assert!(validate_capability_formatter_provenance(&wrong_generated_c, expected()).is_err());

        let mut wrong_generated_c_identity = parse();
        wrong_generated_c_identity
            .object
            .generated_c_identity_sha256 = "b".repeat(64);
        assert!(
            validate_capability_formatter_provenance(&wrong_generated_c_identity, expected())
                .is_err()
        );

        let mut wrong_object_flags = parse();
        wrong_object_flags
            .object
            .compile_flags
            .push("-g".to_owned());
        assert!(validate_capability_formatter_provenance(&wrong_object_flags, expected()).is_err());

        let mut wrong_role = parse();
        wrong_role.object.unit_role = "untyped-application".to_owned();
        assert!(validate_capability_formatter_provenance(&wrong_role, expected()).is_err());
    }

    #[test]
    fn capability_entry_split_provenance_rejects_slot_id_and_role_drift() {
        let parse = || {
            serde_json::from_value::<CapabilityEntryBuildProvenanceV2>(
                capability_split_provenance_json_for_test(),
            )
            .expect("split provenance fixture failed to parse")
        };
        let provenance = parse();
        let entry_id = provenance.application_units[0].entry_id.clone();
        let entries = vec![CapabilityEntryRuntimeUnitIdentity {
            local_profile_sha256: "a".repeat(64),
            entry_id: entry_id.clone(),
            entry_path: "convex/example.ts".to_owned(),
            entry_symbol: capability_entry_symbol(&entry_id),
            invocation_abi: None,
            module_path: "example".to_owned(),
        }];
        validate_capability_split_unit_table(
            &provenance,
            &entries,
            "shared-untyped-console-formatter",
        )
        .expect("valid split unit table was rejected");

        let mut wrong_slot = parse();
        wrong_slot.application_units[0].entry_slot = 1;
        assert!(validate_capability_split_unit_table(
            &wrong_slot,
            &entries,
            "shared-untyped-console-formatter",
        )
        .is_err());

        let mut wrong_id = parse();
        wrong_id.application_units[0].entry_id = "f".repeat(64);
        assert!(validate_capability_split_unit_table(
            &wrong_id,
            &entries,
            "shared-untyped-console-formatter",
        )
        .is_err());

        let mut wrong_application_role = parse();
        wrong_application_role.application_units[0]
            .identities
            .generated_c
            .unit_role = "shared-typed-capability-bridge".to_owned();
        assert!(validate_capability_split_unit_table(
            &wrong_application_role,
            &entries,
            "shared-untyped-console-formatter",
        )
        .is_err());

        let mut wrong_bridge_role = parse();
        wrong_bridge_role.bridge.object.unit_role = "untyped-application".to_owned();
        assert!(validate_capability_split_unit_table(
            &wrong_bridge_role,
            &entries,
            "shared-untyped-console-formatter",
        )
        .is_err());

        let mut typed_application = parse();
        typed_application.application_units[0]
            .identities
            .generated_c
            .flags
            .insert(0, "-typed".to_owned());
        assert!(validate_capability_split_unit_table(
            &typed_application,
            &entries,
            "shared-untyped-console-formatter",
        )
        .is_err());
    }

    #[test]
    fn capability_schema_3_selector_binds_producer_identity_fields() {
        let provenance = serde_json::from_value::<CapabilityEntryBuildProvenanceV2>(
            capability_split_provenance_json_for_test(),
        )
        .expect("split provenance fixture failed to parse");
        let application_unit = provenance
            .application_units
            .first()
            .expect("split provenance fixture has no application unit");
        let producer_implementation = CapabilityProducerImplementationIdentity {
            kind: CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND.to_owned(),
            sha256: "9".repeat(64),
        };
        let semantic_environment = json!({
            "LANG": "C",
            "LC_ALL": "C",
            "SOURCE_DATE_EPOCH": "0",
            "TZ": "UTC",
        });
        let artifact = json!({"sha256": "a".repeat(64), "size": 1});
        let core_wasm: CapabilitySplitCoreWasmIdentity = serde_json::from_value(json!({
            "abi": {
                "capabilityEntrySelectorAbiVersion": CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
                "capabilityRequestAbiVersion": CAPABILITY_REQUEST_ABI_VERSION,
                "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
            },
            "bucket": 0,
            "emscripten": {
                "compileFlags": ["-O2"],
                "llvmRevision": "e".repeat(40),
                "materials": "1".repeat(64),
                "revision": "f".repeat(40),
            },
            "linkFlags": ["-Wl,--no-entry"],
            "members": [],
            "partitionPolicy": {
                "bucketCount": 16,
                "hash": "sha256-route-id-domain-first-u32-be-modulo",
                "kind": "convex-wasm-cohort-partition-v1",
            },
            "pipelineKind": "convex-wasm-artifact-pipeline-v9",
            "producerImplementation": producer_implementation,
            "runtime": {"archives": "b".repeat(64), "mainObject": artifact},
            "selectorObject": artifact,
            "semanticEnvironment": semantic_environment,
            "staticHermes": {
                "applicationFlags": [],
                "bridgeFlags": [],
                "materials": "c".repeat(64),
                "revision": "d".repeat(40),
            },
            "target": {"cpu": TARGET_CPU, "platform": {}, "triple": TARGET_TRIPLE},
            "unitTopology": {
                "applicationFlags": [],
                "bridge": {
                    "entrySymbol": "sh_export_convex_wasm_capability_bridge",
                    "generatedSource": artifact,
                    "object": artifact,
                },
                "bridgeFlags": [],
                "kind": "convex-wasm-capability-unit-topology-v1",
            },
        }))
        .expect("schema 3 core Wasm fixture failed to parse");
        validate_capability_partition_policy(&core_wasm.partition_policy, core_wasm.bucket)
            .expect("producer partition policy was rejected");

        let route = CapabilityEntryRoute {
            entry_id: application_unit.entry_id.clone(),
            entry_selector_id: "1".repeat(16),
            entry_symbol: capability_entry_symbol(&application_unit.entry_id),
            export_name: "example".to_owned(),
            invocation_abi: Some(CapabilityInvocationAbi::OfficialWrapper),
            route_id: "2".repeat(64),
            udf_kind: UdfKind::Query,
            visibility: "public".to_owned(),
        };
        let selector = json!({
            "abiVersion": CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            "bucket": core_wasm.bucket,
            "entryId": application_unit.entry_id,
            "entrySlot": application_unit.entry_slot,
            "entrySymbol": capability_entry_symbol(&application_unit.entry_id),
            "kind": CAPABILITY_ENTRY_MANIFEST_KIND,
            "members": [{
                "entrySelectorId": route.entry_selector_id,
                "handlerExportName": route.export_name,
                "handlerUdfKind": route.udf_kind,
                "invocationAbi": CapabilityInvocationAbi::OfficialWrapper,
            }],
            "partitionPolicy": {
                "bucketCount": 16,
                "hash": "sha256-route-id-domain-first-u32-be-modulo",
                "kind": "convex-wasm-cohort-partition-v1",
            },
            "pipelineKind": "convex-wasm-artifact-pipeline-v9",
            "producerImplementation": producer_implementation,
            "semanticEnvironment": semantic_environment,
            "sourceSha256": "e".repeat(64),
            "toolchain": {
                "compileFlags": ["-O2"],
                "emscriptenMaterials": "1".repeat(64),
                "emscriptenRevision": "f".repeat(40),
                "llvmRevision": "e".repeat(40),
            },
            "unitRole": "untyped-application-selector",
        });
        let validate = |selector: &JsonValue| {
            validate_split_schema_3_selector_provenance(
                selector,
                application_unit,
                &core_wasm,
                std::slice::from_ref(&route),
                "convex-wasm-artifact-pipeline-v9",
                &producer_implementation,
                &semantic_environment,
            )
        };
        validate(&selector).expect("valid producer selector identity was rejected");

        let mut changed_partition_policy = selector.clone();
        changed_partition_policy["partitionPolicy"]["bucketCount"] = json!(15);
        assert!(validate(&changed_partition_policy).is_err());

        let mut missing_partition_policy_field = selector.clone();
        missing_partition_policy_field["partitionPolicy"]
            .as_object_mut()
            .expect("selector partition policy must be an object")
            .remove("hash");
        assert!(validate(&missing_partition_policy_field).is_err());

        let mut changed_pipeline_kind = selector.clone();
        changed_pipeline_kind["pipelineKind"] = json!("convex-wasm-artifact-pipeline-v8");
        assert!(validate(&changed_pipeline_kind).is_err());

        let mut changed_semantic_environment = selector.clone();
        changed_semantic_environment["semanticEnvironment"]["TZ"] = json!("PST8PDT");
        assert!(validate(&changed_semantic_environment).is_err());

        for (field, value) in [
            ("compileFlags", json!(["-O3"])),
            ("emscriptenMaterials", json!("f".repeat(64))),
            ("emscriptenRevision", json!("a".repeat(40))),
            ("llvmRevision", json!("b".repeat(40))),
        ] {
            let mut changed = selector.clone();
            changed["toolchain"][field] = value;
            assert!(validate(&changed).is_err(), "accepted changed {field}");
        }

        let mut missing_toolchain_field = selector.clone();
        missing_toolchain_field["toolchain"]
            .as_object_mut()
            .expect("selector toolchain must be an object")
            .remove("llvmRevision");
        assert!(validate(&missing_toolchain_field).is_err());

        let mut extra_toolchain_field = selector;
        extra_toolchain_field["toolchain"]["unexpected"] = json!(true);
        assert!(validate(&extra_toolchain_field).is_err());

        let unsupported_policy = CohortPartitionPolicy {
            bucket_count: 16,
            hash: "sha256-route-id-domain-first-u32-be-modulo".to_owned(),
            kind: "convex-wasm-cohort-partition-v2".to_owned(),
        };
        assert!(validate_capability_partition_policy(&unsupported_policy, 0).is_err());
    }

    fn guest_native_json_codec_identity_for_test() -> GuestNativeJsonCodecIdentity {
        GuestNativeJsonCodecIdentity {
            canonical_vector_corpus: CanonicalConvexValueVectorCorpus {
                kind: CANONICAL_CONVEX_VALUE_VECTOR_CORPUS_KIND.to_owned(),
                producer: CanonicalConvexValueVectorCorpusProducer {
                    kind: CANONICAL_CONVEX_VALUE_VECTOR_PRODUCER_KIND.to_owned(),
                    source_sha256: "a".repeat(64),
                },
                schema_version: 1,
                sha256: "b".repeat(64),
            },
            generated_source: CapabilityValueCodecSource {
                sha256: "c".repeat(64),
                size: 1,
            },
            kind: GUEST_NATIVE_JSON_CODEC_IDENTITY_KIND.to_owned(),
            lowering_pipeline_sha256: "d".repeat(64),
            schema_version: 1,
        }
    }

    #[test]
    fn capability_entry_value_codec_binds_corpus_source_and_provenance() {
        let identity = guest_native_json_codec_identity_for_test();
        validate_guest_native_json_codec_identity(&identity, &identity.lowering_pipeline_sha256)
            .expect("valid capability-entry codec identity was rejected");
        validate_capability_entry_value_codec_provenance(&identity, &identity)
            .expect("matching capability-entry codec provenance was rejected");

        let mut unsupported_schema = identity.clone();
        unsupported_schema.schema_version = 2;
        assert!(matches!(
            validate_guest_native_json_codec_identity(
                &unsupported_schema,
                &identity.lowering_pipeline_sha256
            ),
            Err(WasmUdfPackageError::InvalidPackageEntry(_))
        ));

        let mut changed_provenance = identity.clone();
        changed_provenance
            .canonical_vector_corpus
            .producer
            .source_sha256 = "e".repeat(64);
        assert!(matches!(
            validate_capability_entry_value_codec_provenance(&changed_provenance, &identity),
            Err(WasmUdfPackageError::InvalidBuildProvenance(_))
        ));
    }

    fn capability_request_envelope_identity_for_test() -> CapabilityRequestEnvelopeIdentity {
        CapabilityRequestEnvelopeIdentity {
            capability_request_abi_version: CAPABILITY_REQUEST_ABI_VERSION,
            canonical_vector_corpus: CanonicalCapabilityRequestEnvelopeVectorCorpus {
                kind: CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND.to_owned(),
                producer: CanonicalCapabilityRequestEnvelopeVectorCorpusProducer {
                    kind: CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND.to_owned(),
                    source_sha256: "a".repeat(64),
                },
                schema_version: 1,
                sha256: "b".repeat(64),
            },
            generated_source: CapabilityRequestEnvelopeSource {
                sha256: "c".repeat(64),
                size: 1,
            },
            kind: CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND.to_owned(),
            lowering_pipeline_sha256: "d".repeat(64),
            schema_version: 1,
        }
    }

    #[test]
    fn capability_entry_request_envelope_binds_abi_corpus_source_and_provenance() {
        let identity = capability_request_envelope_identity_for_test();
        assert_eq!(identity.capability_request_abi_version, 4);
        assert_eq!(
            identity.kind,
            "convex-wasm-capability-request-envelope-identity-v4"
        );
        assert_eq!(
            identity.canonical_vector_corpus.kind,
            "convex-wasm-canonical-capability-request-envelope-vector-corpus-v4"
        );
        assert_eq!(
            identity.canonical_vector_corpus.producer.kind,
            "convex-sdk-backend-capability-request-envelope-producer-v4"
        );
        validate_capability_request_envelope_identity(
            &identity,
            &identity.lowering_pipeline_sha256,
        )
        .expect("valid capability request-envelope identity was rejected");
        validate_capability_entry_request_envelope_provenance(&identity, &identity)
            .expect("matching capability request-envelope provenance was rejected");

        let mut pre_storage = identity.clone();
        pre_storage.kind = PRE_STORAGE_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND.to_owned();
        pre_storage.canonical_vector_corpus.kind =
            PRE_STORAGE_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND.to_owned();
        pre_storage.canonical_vector_corpus.producer.kind =
            PRE_STORAGE_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND.to_owned();
        validate_capability_request_envelope_identity(
            &pre_storage,
            &identity.lowering_pipeline_sha256,
        )
        .expect("producer-supported pre-storage ABI 4 request-envelope identity was rejected");

        let mut mixed_pre_storage = pre_storage.clone();
        mixed_pre_storage.kind = CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND.to_owned();
        assert!(matches!(
            validate_capability_request_envelope_identity(
                &mixed_pre_storage,
                &identity.lowering_pipeline_sha256,
            ),
            Err(WasmUdfPackageError::InvalidPackageEntry(_))
        ));

        for legacy_version in [
            LEGACY_CAPABILITY_REQUEST_ABI_VERSION,
            PENDING_VALUE_CAPABILITY_REQUEST_ABI_VERSION,
        ] {
            let mut legacy = identity.clone();
            legacy.capability_request_abi_version = legacy_version;
            legacy.kind = LEGACY_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND.to_owned();
            legacy.canonical_vector_corpus.kind =
                LEGACY_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND.to_owned();
            legacy.canonical_vector_corpus.producer.kind =
                LEGACY_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND.to_owned();
            validate_capability_request_envelope_identity(
                &legacy,
                &identity.lowering_pipeline_sha256,
            )
            .expect("legacy capability request-envelope identity was rejected");
        }

        let mut pagination = identity.clone();
        pagination.capability_request_abi_version = PAGINATION_CAPABILITY_REQUEST_ABI_VERSION;
        pagination.kind = PAGINATION_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND.to_owned();
        pagination.canonical_vector_corpus.kind =
            PAGINATION_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND.to_owned();
        pagination.canonical_vector_corpus.producer.kind =
            PAGINATION_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND.to_owned();
        validate_capability_request_envelope_identity(
            &pagination,
            &identity.lowering_pipeline_sha256,
        )
        .expect("pagination capability request-envelope identity was rejected");

        let mut changed_abi = identity.clone();
        changed_abi.capability_request_abi_version = 5;
        assert!(matches!(
            validate_capability_request_envelope_identity(
                &changed_abi,
                &identity.lowering_pipeline_sha256,
            ),
            Err(WasmUdfPackageError::InvalidPackageEntry(_))
        ));

        let mut stale_identity = identity.clone();
        stale_identity.kind = LEGACY_CAPABILITY_REQUEST_ENVELOPE_IDENTITY_KIND.to_owned();
        stale_identity.canonical_vector_corpus.kind =
            LEGACY_CANONICAL_CAPABILITY_REQUEST_ENVELOPE_VECTOR_CORPUS_KIND.to_owned();
        stale_identity.canonical_vector_corpus.producer.kind =
            LEGACY_CAPABILITY_REQUEST_ENVELOPE_VECTOR_PRODUCER_KIND.to_owned();
        assert!(matches!(
            validate_capability_request_envelope_identity(
                &stale_identity,
                &identity.lowering_pipeline_sha256,
            ),
            Err(WasmUdfPackageError::InvalidPackageEntry(_))
        ));

        let mut changed_provenance = identity.clone();
        changed_provenance.canonical_vector_corpus.sha256 = "e".repeat(64);
        assert!(matches!(
            validate_capability_entry_request_envelope_provenance(&changed_provenance, &identity,),
            Err(WasmUdfPackageError::InvalidBuildProvenance(_))
        ));
    }

    #[test]
    fn package_registry_root_does_not_guess_cache_version() {
        let mut registry = ValidatedDeploymentManifest::empty_for_test(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        assert_eq!(
            registry.package_registry_root(Path::new("/cache/v3")),
            Path::new("/cache/v3/packages"),
        );

        registry.version = DeploymentManifestVersion::V4;
        assert_eq!(
            registry.package_registry_root(Path::new("/cache/v5")),
            Path::new("/cache/v5/cohort-packages"),
        );
    }

    struct PackageFixture {
        _root: tempfile::TempDir,
        path: PathBuf,
        manifest: JsonValue,
    }

    pub(crate) struct RuntimeRegistryReloadFixture {
        package: PackageFixture,
        manifest: JsonValue,
        root: PathBuf,
    }

    pub(crate) struct RuntimeRegistryTestGeneration {
        pub(crate) deployment_sha256: String,
        pub(crate) generation_sha256: String,
    }

    pub(crate) struct RuntimeRegistryQueryShadowFixture {
        _package: PackageFixture,
        root: PathBuf,
        generation_sha256: String,
        query_route_sha256: String,
        mutation_route_sha256: String,
    }

    pub(crate) struct RuntimeRegistrySourceKeyedCapabilityFixture {
        _root: tempfile::TempDir,
        additive_route: RuntimeRegistrySourceKeyedCapabilityRoute,
        generations: Vec<RuntimeRegistrySourceKeyedCapabilityGeneration>,
        package_key: String,
        root: PathBuf,
        serialized_module_snapshot_directory: PathBuf,
    }

    #[derive(Clone)]
    pub(crate) struct RuntimeRegistrySourceKeyedCapabilityGeneration {
        pub(crate) source_package_runtime_content_sha256: String,
        pub(crate) deployment_sha256: String,
        pub(crate) generation_sha256: String,
        pub(crate) package_key: String,
        pub(crate) selector_count: usize,
        pub(crate) generation_file_sha256: String,
        generation_file_size: u64,
    }

    #[derive(Clone)]
    struct RuntimeRegistrySourceKeyedCapabilityRoute {
        entry_id: String,
        entry_selector_id: String,
        export_name: String,
        route_id: String,
    }

    impl PackageFixture {
        fn create() -> anyhow::Result<Self> {
            let root = tempfile::tempdir()?;
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))?;
            let core_wasm = b"core-wasm-fixture";
            let serialized_module = b"serialized-module-fixture";
            let provenance = json!({
                "artifactPipelineSha256":
                    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "engineIdentity": {
                    "engineCompatibilitySha256":
                        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    "engineConfig": {
                        "consumeFuel": true,
                        "epochInterruption": true,
                        "profilingStrategy": "perf-map",
                        "wasmExceptions": true,
                    },
                    "kind": ENGINE_IDENTITY_KIND,
                    "target": {
                        "cpu": TARGET_CPU,
                        "triple": TARGET_TRIPLE,
                    },
                },
                "identities": {
                    "wasmtimeAot": {
                        "wasmtime": {
                            "package": test_precompiler_material_identity(),
                        },
                    },
                },
                "kind": "convex-wasm-artifact-provenance-v1",
                "loweringPipelineSha256":
                    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "materials": {},
                "semanticEnvironment": {},
                "sourcePipelineSha256":
                    "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            });
            let provenance_bytes = canonical_file_bytes(&provenance);
            let manifest = json!({
                "artifact": {
                    "coreWasmBytes": core_wasm.len(),
                    "coreWasmSha256": sha256(core_wasm),
                    "engineConfigurationSha256": ENGINE_CONFIGURATION_SHA256,
                    "engineCompatibilitySha256":
                        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    "provenanceBytes": provenance_bytes.len(),
                    "provenanceSha256": sha256(&provenance_bytes),
                    "serializedModuleBytes": serialized_module.len(),
                    "serializedModuleSha256": sha256(serialized_module),
                    "targetCpu": TARGET_CPU,
                    "targetTriple": TARGET_TRIPLE,
                    "wasmtimeRevision": WASMTIME_REVISION,
                },
                "compiler": {
                    "admittedLanguageVersion": 1,
                    "artifactPipelineSha256":
                        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                    "compilerRevision": "compiler-test-revision",
                    "loweringPipelineSha256":
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                    "sourcePipelineSha256":
                        "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                    "staticHermesRevision": "static-hermes-test-revision",
                },
                "importedOperations": [],
                "limits": {
                    "executionFuel": 1,
                    "maxGuestMemoryBytes": 1,
                    "maxHostOwnedBytes": 1,
                    "maxOperationCount": 1,
                    "maxResultBytes": 1,
                    "maxValueHandles": 1,
                    "timeoutMilliseconds": 1,
                },
                "platformLimits": PlatformLimits::authoritative()?,
                "manifestSchemaVersion": MANIFEST_SCHEMA_VERSION,
                "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
                "routing": {
                    "decision": "wasm",
                },
                "source": {
                    "exportName": "run",
                    "exportSha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "modulePath": "convex/example.ts",
                    "runtimeModulePath": "example.js",
                    "resolvedGraphSha256":
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "udfKind": "query",
                },
            });
            let manifest_bytes = canonical_bytes(&manifest);
            let key = sha256(&manifest_bytes);
            let package_entry = json!({
                "artifacts": {
                    "module.cwasm": {
                        "sha256": sha256(serialized_module),
                        "size": serialized_module.len(),
                    },
                    "module.wasm": {
                        "sha256": sha256(core_wasm),
                        "size": core_wasm.len(),
                    },
                },
                "key": key,
                "kind": PACKAGE_ENTRY_KIND,
                "manifestSha256": key,
                "provenance": {
                    "sha256": sha256(&provenance_bytes),
                    "size": provenance_bytes.len(),
                },
            });
            let path = root.path().join("packages").join(&key);
            fs::create_dir_all(&path)?;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
            write_private(&path.join("module.wasm"), core_wasm)?;
            write_private(&path.join("module.cwasm"), serialized_module)?;
            write_private(
                &path.join("execution-manifest.json"),
                &canonical_file_bytes(&manifest),
            )?;
            write_private(&path.join("build-provenance.json"), &provenance_bytes)?;
            write_private(
                &path.join("package-entry.json"),
                &canonical_file_bytes(&package_entry),
            )?;
            write_private(&path.join("COMPLETE"), format!("{key}\n").as_bytes())?;
            Ok(Self {
                _root: root,
                path,
                manifest,
            })
        }

        fn cache_root(&self) -> &Path {
            self._root.path()
        }

        fn package_key(&self) -> &str {
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("package fixture key missing")
        }
    }

    impl RuntimeRegistryReloadFixture {
        pub(crate) fn create() -> anyhow::Result<Self> {
            let package = PackageFixture::create()?;
            let manifest = signed_deployment_manifest(vec![deployment_export(&package)]);
            let root = create_runtime_registry(&package, &manifest)?;
            Ok(Self {
                package,
                manifest,
                root,
            })
        }

        pub(crate) fn root(&self) -> &Path {
            &self.root
        }

        pub(crate) fn publish_generation(
            &self,
            marker: &str,
            complete: bool,
        ) -> anyhow::Result<RuntimeRegistryTestGeneration> {
            let mut manifest = self.manifest.clone();
            manifest["compiler"] = json!({ "testGeneration": marker });
            sign_deployment_manifest(&mut manifest);
            let generation =
                write_runtime_registry_generation(&self.root, &self.package, &manifest)?;
            let deployment_sha256 = manifest["deploymentSha256"]
                .as_str()
                .context("runtime registry test deployment digest missing")?
                .to_owned();
            let generation_sha256 = generation["generationSha256"]
                .as_str()
                .context("runtime registry test generation digest missing")?
                .to_owned();
            if !complete {
                fs::remove_file(
                    self.root
                        .join("generations")
                        .join(&deployment_sha256)
                        .join(&generation_sha256)
                        .join("COMPLETE"),
                )?;
            }
            write_runtime_registry_current(&self.root, &generation)?;
            Ok(RuntimeRegistryTestGeneration {
                deployment_sha256,
                generation_sha256,
            })
        }

        pub(crate) fn complete_generation(
            &self,
            generation: &RuntimeRegistryTestGeneration,
        ) -> anyhow::Result<()> {
            write_private(
                &self
                    .root
                    .join("generations")
                    .join(&generation.deployment_sha256)
                    .join(&generation.generation_sha256)
                    .join("COMPLETE"),
                format!("{}\n", generation.generation_sha256).as_bytes(),
            )
        }
    }

    impl RuntimeRegistryQueryShadowFixture {
        pub(crate) fn create() -> anyhow::Result<Self> {
            let package = PackageFixture::create()?;
            let query = capability_entry_deployment_export(&package);
            let query_route_sha256 = query["packageReference"]["routeId"]
                .as_str()
                .context("query-shadow fixture query route digest missing")?
                .to_owned();
            let package_key = query["packageReference"]["capabilityEntryPackageId"]
                .as_str()
                .context("query-shadow fixture capability package key missing")?
                .to_owned();

            let mut mutation = query.clone();
            mutation["entryPath"] = json!("convex/mutation.ts");
            mutation["exportName"] = json!("mutate");
            mutation["runtimeModulePath"] = json!("mutation.js");
            mutation["source"]["exportName"] = json!("mutate");
            mutation["source"]["modulePath"] = json!("convex/mutation.ts");
            mutation["source"]["udfKind"] = json!("mutation");
            mutation["udfKind"] = json!("mutation");
            mutation["packageReference"]["entrySelectorId"] = json!("7".repeat(16));
            mutation["packageReference"]["routeId"] = json!("7".repeat(64));
            let mutation_route_sha256 = mutation["packageReference"]["routeId"]
                .as_str()
                .context("query-shadow fixture mutation route digest missing")?
                .to_owned();
            let manifest = signed_v5_capability_selection_manifest(vec![query, mutation]);

            let root = package.cache_root().join("query-shadow-runtime-registry");
            create_private_directory(&root)?;
            create_private_directory(&root.join("capability-entry-packages"))?;
            create_private_directory(&root.join("generations"))?;
            create_private_directory(&root.join("packages"))?;
            let capability_package = root.join("capability-entry-packages").join(&package_key);
            create_private_directory(&capability_package)?;
            for (source, destination) in [
                ("COMPLETE", "COMPLETE"),
                ("build-provenance.json", "build-provenance.json"),
                ("execution-manifest.json", "entry-manifest.json"),
                ("module.cwasm", "module.cwasm"),
                ("module.wasm", "module.wasm"),
                ("package-entry.json", "package-entry.json"),
            ] {
                fs::hard_link(
                    package.path.join(source),
                    capability_package.join(destination),
                )?;
            }
            let generation = write_runtime_registry_generation_files(
                &root,
                &manifest,
                json!({
                    "capabilityEntryPackages": [{
                        "files": runtime_registry_capability_entry_package_files(
                            &capability_package,
                        )?,
                        "packageKey": package_key,
                    }],
                    "kind": RUNTIME_REGISTRY_GENERATION_KIND_V3,
                }),
            )?;
            write_runtime_registry_current(&root, &generation)?;
            let generation_sha256 = generation["generationSha256"]
                .as_str()
                .context("query-shadow fixture generation digest missing")?
                .to_owned();
            Ok(Self {
                _package: package,
                root,
                generation_sha256,
                query_route_sha256,
                mutation_route_sha256,
            })
        }

        pub(crate) fn generation_sha256(&self) -> &str {
            &self.generation_sha256
        }

        pub(crate) fn mutation_route_sha256(&self) -> &str {
            &self.mutation_route_sha256
        }

        pub(crate) fn query_route_sha256(&self) -> &str {
            &self.query_route_sha256
        }

        pub(crate) fn root(&self) -> &Path {
            &self.root
        }
    }

    impl RuntimeRegistrySourceKeyedCapabilityFixture {
        pub(crate) fn create(
            core_wasm: &[u8],
            serialized_module: &[u8],
            selector_count: usize,
        ) -> anyhow::Result<Self> {
            anyhow::ensure!(
                selector_count > 0,
                "source-keyed fixture requires a selector"
            );
            let total_selector_count = selector_count
                .checked_add(1)
                .context("source-keyed fixture selector count overflow")?;
            let root_owner = tempfile::tempdir()?;
            fs::set_permissions(root_owner.path(), fs::Permissions::from_mode(0o700))?;
            let root = root_owner.path().join("runtime-registry");
            let serialized_module_snapshot_directory = root_owner.path().join("snapshots");
            for path in [
                root.clone(),
                root.join("capability-entry-packages"),
                root.join("generations"),
                root.join("packages"),
                serialized_module_snapshot_directory.clone(),
            ] {
                create_private_directory(&path)?;
            }

            let (package_key, routes) = write_source_keyed_capability_package(
                &root,
                core_wasm,
                serialized_module,
                total_selector_count,
            )?;
            let initial_routes = routes[..selector_count].to_vec();
            let additive_route = routes
                .last()
                .context("source-keyed fixture additive route missing")?
                .clone();
            let source_package_runtime_content_sha256 = "1".repeat(64);
            let manifest = signed_source_keyed_capability_manifest(
                &package_key,
                &initial_routes,
                &source_package_runtime_content_sha256,
            )?;
            let initial = write_source_keyed_capability_generation(
                &root,
                &package_key,
                &manifest,
                source_package_runtime_content_sha256,
                selector_count,
            )?;
            let generation = read_source_keyed_generation_json(&root, &initial)?;
            write_runtime_registry_current(&root, &generation)?;
            write_runtime_registry_source_catalog_fixture(&root, &[initial.clone()])?;

            Ok(Self {
                _root: root_owner,
                additive_route,
                generations: vec![initial],
                package_key,
                root,
                serialized_module_snapshot_directory,
            })
        }

        pub(crate) fn root(&self) -> &Path {
            &self.root
        }

        pub(crate) fn serialized_module_snapshot_directory(&self) -> &Path {
            &self.serialized_module_snapshot_directory
        }

        pub(crate) fn initial(&self) -> &RuntimeRegistrySourceKeyedCapabilityGeneration {
            self.generations
                .first()
                .expect("source-keyed fixture initial generation disappeared")
        }

        pub(crate) fn publish_additive_generation(
            &mut self,
        ) -> anyhow::Result<RuntimeRegistrySourceKeyedCapabilityGeneration> {
            anyhow::ensure!(
                self.generations.len() == 1,
                "source-keyed fixture additive generation was published twice"
            );
            let source_package_runtime_content_sha256 = "2".repeat(64);
            let manifest = signed_source_keyed_capability_manifest(
                &self.package_key,
                std::slice::from_ref(&self.additive_route),
                &source_package_runtime_content_sha256,
            )?;
            let generation = write_source_keyed_capability_generation(
                &self.root,
                &self.package_key,
                &manifest,
                source_package_runtime_content_sha256,
                1,
            )?;
            self.generations.push(generation.clone());
            write_runtime_registry_source_catalog_fixture(&self.root, &self.generations)?;
            Ok(generation)
        }

        pub(crate) fn publish_latest_only_catalog(&self) -> anyhow::Result<()> {
            let latest = self
                .generations
                .last()
                .context("source-keyed fixture has no generation")?;
            write_runtime_registry_source_catalog_fixture(&self.root, std::slice::from_ref(latest))
        }
    }

    #[test]
    fn source_keyed_generation_descriptor_reuse_requires_the_exact_manifest() {
        let retained = ValidatedRuntimeRegistryGenerationDescriptor {
            current_sha256: "1".repeat(64),
            deployment_sha256: "2".repeat(64),
            generation_file_sha256: "3".repeat(64),
            generation_file_size: 123,
            generation_sha256: "4".repeat(64),
            source_package_runtime_content_sha256: Some("5".repeat(64)),
        };
        let mut additive = retained.clone();
        additive.current_sha256 = "6".repeat(64);
        assert!(additive.same_authenticated_identity(&retained));

        let mut changed_source = additive.clone();
        changed_source.source_package_runtime_content_sha256 = Some("7".repeat(64));
        let mut missing_source = additive.clone();
        missing_source.source_package_runtime_content_sha256 = None;
        let mut changed_deployment = additive.clone();
        changed_deployment.deployment_sha256 = "7".repeat(64);
        let mut changed_generation = additive.clone();
        changed_generation.generation_sha256 = "7".repeat(64);
        let mut changed_manifest = additive.clone();
        changed_manifest.generation_file_sha256 = "7".repeat(64);
        let mut changed_size = additive.clone();
        changed_size.generation_file_size += 1;
        for changed in [
            changed_source,
            missing_source,
            changed_deployment,
            changed_generation,
            changed_manifest,
            changed_size,
        ] {
            assert!(!changed.same_authenticated_identity(&retained));
        }
    }

    #[test]
    fn source_keyed_capability_fixture_publishes_an_additive_authenticated_catalog(
    ) -> anyhow::Result<()> {
        let mut fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(
            b"source-keyed-core-wasm-fixture",
            b"source-keyed-aot-fixture",
            3,
        )?;
        let initial_catalog = load_runtime_registry_source_catalog(fixture.root())?;
        let initial_descriptors = initial_catalog.into_generations();
        assert_eq!(initial_descriptors.len(), 1);
        let initial = load_runtime_registry_generation(
            fixture.root(),
            initial_descriptors
                .first()
                .context("source-keyed fixture initial descriptor missing")?,
        )?;
        assert_eq!(initial.registry().selected_wasm_exports().len(), 3);
        assert_eq!(
            initial.generation_manifest_sha256(),
            fixture.initial().generation_file_sha256,
        );
        assert!(initial
            .descriptor
            .same_authenticated_identity(&initial_descriptors[0]));
        let mut wrong_size = initial_descriptors[0].clone();
        wrong_size.generation_file_size += 1;
        assert!(load_runtime_registry_generation(fixture.root(), &wrong_size).is_err());

        let additive = fixture.publish_additive_generation()?;
        let additive_catalog = load_runtime_registry_source_catalog(fixture.root())?;
        let additive_descriptors = additive_catalog.into_generations();
        assert_eq!(additive_descriptors.len(), 2);
        assert_ne!(
            additive_descriptors[0].current_sha256(),
            initial.descriptor.current_sha256(),
        );
        assert!(additive_descriptors[0].same_authenticated_identity(&initial.descriptor));
        let loaded = additive_descriptors
            .iter()
            .map(|descriptor| load_runtime_registry_generation(fixture.root(), descriptor))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(loaded[0].registry().selected_wasm_exports().len(), 3);
        assert_eq!(loaded[1].registry().selected_wasm_exports().len(), 1);
        assert_eq!(additive.selector_count, 1);
        Ok(())
    }

    fn compatibility() -> RuntimeCompatibility<'static> {
        RuntimeCompatibility {
            opaque_value_abi_version: OPAQUE_VALUE_ABI_VERSION,
            platform_limits: PlatformLimits::authoritative()
                .expect("test backend platform limits are representable"),
            wasmtime_revision: WASMTIME_REVISION,
            target_triple: TARGET_TRIPLE,
            target_cpu: TARGET_CPU,
            engine_configuration_sha256: ENGINE_CONFIGURATION_SHA256,
            engine_compatibility_sha256:
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        }
    }

    fn canonical_bytes(value: &JsonValue) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_canonical_json(value, &mut bytes).expect("canonical JSON fixture");
        bytes
    }

    fn canonical_file_bytes(value: &JsonValue) -> Vec<u8> {
        let mut bytes = canonical_bytes(value);
        bytes.push(b'\n');
        bytes
    }

    fn capability_manifest_entry_for_test(
        entry_path: &str,
        module_path: &str,
        invocation_abi: Option<CapabilityInvocationAbi>,
    ) -> CapabilityEntryIdentity {
        let local_profile_sha256 = "a".repeat(64);
        let mut entry_identity = json!({
            "domain": "convex-wasm-capability-entry-v1",
            "localProfileSha256": local_profile_sha256,
            "selectedEntry": {
                "entryPath": entry_path,
                "modulePath": module_path,
            },
        });
        if let Some(invocation_abi) = invocation_abi {
            entry_identity["invocationAbi"] =
                serde_json::to_value(invocation_abi).expect("invocation ABI fixture serializes");
        }
        CapabilityEntryIdentity {
            local_profile: CapabilityLocalProfile {
                dependency_graph_sha256: "b".repeat(64),
                javascript: PackageArtifact {
                    sha256: "e".repeat(64),
                    size: 1,
                },
                metafile_sha256: "f".repeat(64),
                sha256: local_profile_sha256,
                source_map: PackageArtifact {
                    sha256: "1".repeat(64),
                    size: 1,
                },
            },
            entry_id: sha256(&canonical_bytes(&entry_identity)),
            entry_path: entry_path.to_owned(),
            entry_symbol: None,
            invocation_abi,
            module_path: module_path.to_owned(),
        }
    }

    fn capability_runtime_entry_for_test(marker: char) -> CapabilityEntryRuntimeUnitIdentity {
        let entry_id = marker.to_string().repeat(64);
        CapabilityEntryRuntimeUnitIdentity {
            local_profile_sha256: "9".repeat(64),
            entry_id: entry_id.clone(),
            entry_path: format!("convex/{marker}.ts"),
            entry_symbol: capability_entry_symbol(&entry_id),
            invocation_abi: None,
            module_path: marker.to_string(),
        }
    }

    fn capability_runtime_route_for_test(
        entry: &CapabilityEntryRuntimeUnitIdentity,
        selector: &str,
        export_name: &str,
        route_marker: char,
        udf_kind: UdfKind,
    ) -> CapabilityEntryRoute {
        CapabilityEntryRoute {
            entry_id: entry.entry_id.clone(),
            entry_selector_id: selector.to_owned(),
            entry_symbol: entry.entry_symbol.clone(),
            export_name: export_name.to_owned(),
            invocation_abi: entry.invocation_abi,
            route_id: route_marker.to_string().repeat(64),
            udf_kind,
            visibility: "public".to_owned(),
        }
    }

    #[test]
    fn capability_manifest_schema_controls_entry_representation() {
        let singleton = capability_manifest_entry_for_test("convex/a.ts", "a", None);
        assert_eq!(
            capability_manifest_entry_units(
                CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION,
                Some(&singleton),
                None,
            )
            .expect("schema 3 singleton entry was rejected")
            .len(),
            1
        );
        assert!(capability_manifest_entry_units(
            CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION,
            None,
            None,
        )
        .is_err());

        let generic_entries = vec![
            capability_manifest_entry_for_test("convex/a.ts", "a", None),
            capability_manifest_entry_for_test("convex/b.ts", "b", None),
        ];
        assert!(capability_manifest_entry_units(
            CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION,
            Some(&singleton),
            Some(&generic_entries),
        )
        .is_err());
        assert!(capability_manifest_entry_units(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            Some(&singleton),
            Some(&generic_entries),
        )
        .is_err());
        assert!(capability_manifest_entry_units(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            None,
            None,
        )
        .is_err());
        assert!(capability_manifest_entry_units(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            None,
            Some(&generic_entries[..1]),
        )
        .is_err());
        assert_eq!(
            capability_manifest_entry_units(
                CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
                None,
                Some(&generic_entries),
            )
            .expect("schema 4 generic entry table was rejected")
            .len(),
            2
        );
    }

    #[test]
    fn capability_local_profiles_authenticate_entries_and_source_pipeline() {
        let mut entry = capability_manifest_entry_for_test("convex/a.ts", "a", None);
        entry.entry_symbol = Some(capability_entry_symbol(&entry.entry_id));
        let local_profile =
            serde_json::to_value(&entry.local_profile).expect("local-profile fixture serializes");
        let encoded = json!({
            "entryId": entry.entry_id,
            "entryPath": entry.entry_path,
            "entrySymbol": entry.entry_symbol,
            "localProfile": local_profile,
            "modulePath": entry.module_path,
        });
        let parsed: CapabilityEntryIdentity = serde_json::from_value(encoded.clone())
            .expect("current local-profile entry failed to parse");
        let validated = validate_capability_entry_manifest_unit(
            &parsed,
            true,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
        )
        .expect("current local-profile entry failed authentication");
        assert_eq!(validated.local_profile_sha256, parsed.local_profile.sha256);

        let mut legacy = encoded;
        let legacy_object = legacy
            .as_object_mut()
            .expect("entry fixture is not an object");
        let local_profile = legacy_object
            .remove("localProfile")
            .expect("entry fixture has no local profile");
        legacy_object.insert("compileProfile".to_owned(), local_profile);
        assert!(serde_json::from_value::<CapabilityEntryIdentity>(legacy).is_err());

        let first = capability_runtime_entry_for_test('1');
        assert_eq!(
            capability_source_pipeline_sha256(std::slice::from_ref(&first)),
            first.local_profile_sha256
        );
        let second = capability_runtime_entry_for_test('2');
        let expected_identity = json!({
            "domain": "convex-wasm-capability-package-source-pipeline-v2",
            "profiles": [first.local_profile_sha256, second.local_profile_sha256],
        });
        assert_eq!(
            capability_source_pipeline_sha256(&[first, second]),
            sha256(&canonical_bytes(&expected_identity))
        );
    }

    #[test]
    fn capability_selector_abi_v2_authenticates_entry_invocation_abi() {
        let mut entry = capability_manifest_entry_for_test(
            "convex/images.ts",
            "images",
            Some(CapabilityInvocationAbi::OfficialWrapper),
        );
        entry.entry_symbol = Some(capability_entry_symbol(&entry.entry_id));
        let validated = validate_capability_entry_manifest_unit(
            &entry,
            true,
            CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
        )
        .expect("selector ABI v2 entry was rejected");
        assert_eq!(
            validated.invocation_abi,
            Some(CapabilityInvocationAbi::OfficialWrapper)
        );

        let mut changed_invocation_abi = entry;
        changed_invocation_abi.invocation_abi = Some(CapabilityInvocationAbi::LegacyHandler);
        assert!(validate_capability_entry_manifest_unit(
            &changed_invocation_abi,
            true,
            CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
        )
        .is_err());

        changed_invocation_abi.invocation_abi = None;
        assert!(validate_capability_entry_manifest_unit(
            &changed_invocation_abi,
            true,
            CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
        )
        .is_err());
    }

    #[test]
    fn capability_producer_implementation_identity_is_closed_and_authenticated() {
        let current = json!({
            "kind": "convex-wasm-artifact-producer-identity-v1",
            "sha256": "8ce2ceb63cb22aceb3735fb0a215c0f39e4c197610088e09361b64cba67b8eea",
        });
        let parsed: CapabilityProducerImplementationIdentity =
            serde_json::from_value(current.clone())
                .expect("current producer implementation identity failed to parse");
        validate_capability_producer_implementation_identity(&parsed)
            .expect("current producer implementation identity failed authentication");

        let mut wrong_kind = current.clone();
        wrong_kind["kind"] = json!("convex-wasm-artifact-producer-identity-v2");
        let parsed: CapabilityProducerImplementationIdentity =
            serde_json::from_value(wrong_kind).expect("wrong producer kind failed to parse");
        assert!(validate_capability_producer_implementation_identity(&parsed).is_err());

        let mut malformed_sha256 = current.clone();
        malformed_sha256["sha256"] = json!("not-a-sha256");
        let parsed: CapabilityProducerImplementationIdentity =
            serde_json::from_value(malformed_sha256)
                .expect("malformed producer SHA-256 failed to parse");
        assert!(validate_capability_producer_implementation_identity(&parsed).is_err());

        let mut extra_field = current;
        extra_field["unexpected"] = json!(true);
        assert!(
            serde_json::from_value::<CapabilityProducerImplementationIdentity>(extra_field)
                .is_err()
        );
    }

    #[test]
    fn capability_static_hermes_c_bundle_identity_is_closed_and_matches_flags() {
        let current = json!({
            "cBundle": {
                "compilationSha256": "a".repeat(64),
                "kind": CAPABILITY_STATIC_HERMES_C_BUNDLE_KIND,
            },
            "flags": [
                "-typed",
                "-O",
                "-Xenable-tdz",
                "-emit-c",
                CAPABILITY_STATIC_HERMES_C_BUNDLE_FLAG,
                "-Xemit-c-shard-size=2097152",
            ],
            "materialsSha256": "b".repeat(64),
            "revision": "c".repeat(40),
        });
        let parsed: CapabilityStaticHermesIdentity = serde_json::from_value(current.clone())
            .expect("Static Hermes C bundle identity failed to parse");
        validate_capability_static_hermes_identity(&parsed)
            .expect("Static Hermes C bundle identity failed authentication");

        let mut wrong_kind = current.clone();
        wrong_kind["cBundle"]["kind"] = json!("static-hermes-c-bundle-v2");
        let parsed: CapabilityStaticHermesIdentity = serde_json::from_value(wrong_kind)
            .expect("wrong Static Hermes C bundle kind failed to parse");
        assert!(validate_capability_static_hermes_identity(&parsed).is_err());

        let mut malformed_sha256 = current.clone();
        malformed_sha256["cBundle"]["compilationSha256"] = json!("not-a-sha256");
        let parsed: CapabilityStaticHermesIdentity = serde_json::from_value(malformed_sha256)
            .expect("malformed Static Hermes C bundle digest failed to parse");
        assert!(validate_capability_static_hermes_identity(&parsed).is_err());

        let mut unknown_field = current.clone();
        unknown_field["cBundle"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<CapabilityStaticHermesIdentity>(unknown_field).is_err());

        let mut missing_identity = current.clone();
        missing_identity
            .as_object_mut()
            .expect("Static Hermes identity fixture must be an object")
            .remove("cBundle");
        let parsed: CapabilityStaticHermesIdentity = serde_json::from_value(missing_identity)
            .expect("bundle flags without an identity failed to parse");
        assert!(validate_capability_static_hermes_identity(&parsed).is_err());

        let mut unexpected_identity = current.clone();
        unexpected_identity["flags"] = json!(["-typed", "-O", "-Xenable-tdz", "-emit-c"]);
        let parsed: CapabilityStaticHermesIdentity = serde_json::from_value(unexpected_identity)
            .expect("identity without bundle flags failed to parse");
        assert!(validate_capability_static_hermes_identity(&parsed).is_err());

        let mut partial_flags = current.clone();
        partial_flags["flags"] = json!([
            "-typed",
            "-O",
            "-Xenable-tdz",
            "-emit-c",
            CAPABILITY_STATIC_HERMES_C_BUNDLE_FLAG,
        ]);
        let parsed: CapabilityStaticHermesIdentity =
            serde_json::from_value(partial_flags).expect("partial bundle flags failed to parse");
        assert!(validate_capability_static_hermes_identity(&parsed).is_err());

        let mut duplicate_flags = current;
        duplicate_flags["flags"] = json!([
            "-typed",
            "-O",
            "-Xenable-tdz",
            "-emit-c",
            CAPABILITY_STATIC_HERMES_C_BUNDLE_FLAG,
            CAPABILITY_STATIC_HERMES_C_BUNDLE_FLAG,
            "-Xemit-c-shard-size=2097152",
        ]);
        let parsed: CapabilityStaticHermesIdentity = serde_json::from_value(duplicate_flags)
            .expect("duplicate bundle flags failed to parse");
        assert!(validate_capability_static_hermes_identity(&parsed).is_err());
    }

    #[test]
    fn capability_static_hermes_c_bundle_member_compilation_binds_function_fragments() {
        let current = json!({
            "kind": CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMMAND_KIND,
            "memberCompilations": [
                {
                    "effectiveArguments": ["-O2", "-c", "metadata.c", "-o", "member-00000.o"],
                    "member": {"path": "metadata.c", "role": "metadata"},
                    "optimization": "-O2",
                    "stage": "export-object",
                },
                {
                    "effectiveArguments": ["-O2", "-c", "functions-00000.c", "-o", "member-00001.o"],
                    "member": {
                        "firstFunctionId": 0,
                        "functionCount": 1,
                        "lastFunctionId": 0,
                        "oversize": false,
                        "path": "functions-00000.c",
                        "role": "function",
                        "targetBytes": 2097152,
                    },
                    "optimization": "-O2",
                    "stage": "export-object",
                },
                {
                    "effectiveArguments": ["-O2", "-c", "functions-00001.c", "-o", "member-00002.o"],
                    "member": {
                        "firstFunctionId": 1,
                        "functionFragmentCount": 2,
                        "functionFragmentIndex": 0,
                        "functionCount": 1,
                        "lastFunctionId": 1,
                        "oversize": false,
                        "path": "functions-00001.c",
                        "role": "function",
                        "targetBytes": 2097152,
                    },
                    "optimization": "-O2",
                    "stage": "export-object",
                },
                {
                    "effectiveArguments": ["-O2", "-c", "functions-00002.c", "-o", "member-00003.o"],
                    "member": {
                        "firstFunctionId": 1,
                        "functionFragmentCount": 2,
                        "functionFragmentIndex": 1,
                        "functionCount": 1,
                        "lastFunctionId": 1,
                        "oversize": false,
                        "path": "functions-00002.c",
                        "role": "function",
                        "targetBytes": 2097152,
                    },
                    "optimization": "-O2",
                    "stage": "export-object",
                },
                {
                    "effectiveArguments": ["-O2", "-c", "functions-00003.c", "-o", "member-00004.o"],
                    "member": {
                        "firstFunctionId": 2,
                        "functionCount": 1,
                        "lastFunctionId": 2,
                        "oversize": false,
                        "path": "functions-00003.c",
                        "role": "function",
                        "targetBytes": 2097152,
                    },
                    "optimization": "-O2",
                    "stage": "export-object",
                },
            ],
            "relocatableLink": {
                "arguments": [
                    "-r",
                    "member-00000.o",
                    "member-00001.o",
                    "member-00002.o",
                    "member-00003.o",
                    "member-00004.o",
                    "-o",
                    "unit.o",
                ],
                "executable": "wasm-ld",
            },
        });
        let parse = |value: JsonValue| {
            serde_json::from_value::<CapabilityStaticHermesCBundleMemberCompilationIdentity>(value)
                .expect("C bundle member-compilation fixture failed to parse")
        };
        validate_capability_static_hermes_c_bundle_member_compilation(
            &parse(current.clone()),
            "export-object",
            2_097_152,
            CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
        )
        .expect("valid C bundle function-fragment layout was rejected");

        let mut fragmented_first_function = current.clone();
        fragmented_first_function["memberCompilations"][1]["member"]["functionFragmentCount"] =
            json!(2);
        fragmented_first_function["memberCompilations"][1]["member"]["functionFragmentIndex"] =
            json!(0);
        let mut second_fragment = fragmented_first_function["memberCompilations"][1].clone();
        second_fragment["effectiveArguments"][2] = json!("functions-00000-fragment-1.c");
        second_fragment["member"]["functionFragmentIndex"] = json!(1);
        second_fragment["member"]["path"] = json!("functions-00000-fragment-1.c");
        let member_compilations = fragmented_first_function["memberCompilations"]
            .as_array_mut()
            .expect("member compilations must be an array");
        member_compilations.insert(2, second_fragment);
        for (index, compilation) in member_compilations.iter_mut().enumerate() {
            let arguments = compilation["effectiveArguments"]
                .as_array_mut()
                .expect("member compilation arguments must be an array");
            let object_index = arguments
                .iter()
                .position(|argument| argument == "-o")
                .expect("member compilation must have an output argument");
            arguments[object_index + 1] = json!(format!("member-{index:05}.o"));
        }
        fragmented_first_function["relocatableLink"]["arguments"] = json!([
            "-r",
            "member-00000.o",
            "member-00001.o",
            "member-00002.o",
            "member-00003.o",
            "member-00004.o",
            "member-00005.o",
            "-o",
            "unit.o",
        ]);
        validate_capability_static_hermes_c_bundle_member_compilation(
            &parse(fragmented_first_function.clone()),
            "export-object",
            2_097_152,
            CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
        )
        .expect("fragmented first function ID zero was rejected");

        let mut incomplete_first_function = fragmented_first_function;
        incomplete_first_function["memberCompilations"][2]["member"]["functionFragmentIndex"] =
            json!(0);
        assert!(
            validate_capability_static_hermes_c_bundle_member_compilation(
                &parse(incomplete_first_function),
                "export-object",
                2_097_152,
                CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
            )
            .is_err()
        );

        let mut missing_fragment_identity = current.clone();
        missing_fragment_identity["memberCompilations"][3]["member"]
            .as_object_mut()
            .expect("fragment member must be an object")
            .remove("functionFragmentCount");
        assert!(
            validate_capability_static_hermes_c_bundle_member_compilation(
                &parse(missing_fragment_identity),
                "export-object",
                2_097_152,
                CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
            )
            .is_err()
        );

        let mut out_of_order_fragment = current.clone();
        out_of_order_fragment["memberCompilations"][3]["member"]["functionFragmentIndex"] =
            json!(0);
        assert!(
            validate_capability_static_hermes_c_bundle_member_compilation(
                &parse(out_of_order_fragment),
                "export-object",
                2_097_152,
                CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
            )
            .is_err()
        );

        let mut incorrect_target_bytes = current.clone();
        incorrect_target_bytes["memberCompilations"][4]["member"]["targetBytes"] = json!(1);
        assert!(
            validate_capability_static_hermes_c_bundle_member_compilation(
                &parse(incorrect_target_bytes),
                "export-object",
                2_097_152,
                CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
            )
            .is_err()
        );

        let mut duplicate_compiler_flag = current;
        duplicate_compiler_flag["memberCompilations"][1]["effectiveArguments"]
            .as_array_mut()
            .expect("C bundle member arguments must be an array")
            .insert(1, json!("-O0"));
        assert!(
            validate_capability_static_hermes_c_bundle_member_compilation(
                &parse(duplicate_compiler_flag),
                "export-object",
                2_097_152,
                CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
            )
            .is_err()
        );
    }

    #[test]
    fn capability_static_hermes_c_bundle_compilation_authenticates_all_components() {
        let precompile_process = |marker: char| CapabilityStaticHermesPrecompileProcessIdentity {
            c_bundle_member_compilation_policy: Some(
                CapabilityStaticHermesCBundleMemberCompilationPolicy {
                    c_optimization_level_zero: CapabilityStaticHermesCBundleOptimizationLevelZero {
                        c_optimization_level: 0,
                        function_count: 1,
                        optimization_flag: "-O0".to_owned(),
                        role: "function".to_owned(),
                        stage: "c-optimization-level-zero-c-bundle-member-object".to_owned(),
                    },
                    kind: CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMPILATION_POLICY_KIND
                        .to_owned(),
                    normal_optimization_flag: "-O2".to_owned(),
                },
            ),
            compiler_arguments_sha256: marker.to_string().repeat(64),
            kind: CAPABILITY_STATIC_HERMES_PRECOMPILE_PROCESS_KIND.to_owned(),
            launcher_arguments_sha256: marker.to_ascii_uppercase().to_string().repeat(64),
        };
        let member_compilation =
            |marker: char| CapabilityStaticHermesCBundleMemberCompilationIdentity {
                kind: CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMMAND_KIND.to_owned(),
                member_compilations: vec![],
                relocatable_link: CapabilityStaticHermesCBundleRelocatableLink {
                    arguments: vec![marker.to_string()],
                    executable: "wasm-ld".to_owned(),
                },
            };
        let bridge_generated_c = CapabilityProvenanceStaticHermesIdentity {
            materials: "a".repeat(64),
            precompile_process: Some(precompile_process('a')),
            revision: "b".repeat(40),
        };
        let bridge_object = member_compilation('b');
        let formatter_generated_c = CapabilityProvenanceStaticHermesIdentity {
            materials: "a".repeat(64),
            precompile_process: Some(precompile_process('c')),
            revision: "b".repeat(40),
        };
        let formatter_object = member_compilation('d');
        let application_generated_c = CapabilityProvenanceStaticHermesIdentity {
            materials: "a".repeat(64),
            precompile_process: Some(precompile_process('e')),
            revision: "b".repeat(40),
        };
        let application_object = member_compilation('f');
        let route_id = format!("official-output-physical-unit:{}", "c".repeat(64));
        let expected_identity = json!({
            "applications": [{
                "generatedC": application_generated_c.precompile_process,
                "object": application_object,
                "routeId": route_id,
            }],
            "bridge": {
                "generatedC": bridge_generated_c.precompile_process,
                "object": bridge_object,
            },
            "formatter": {
                "generatedC": formatter_generated_c.precompile_process,
                "object": formatter_object,
            },
            "kind": CAPABILITY_STATIC_HERMES_C_BUNDLE_PACKAGE_COMPILATION_KIND,
        });
        let static_hermes = CapabilityStaticHermesIdentity {
            c_bundle: Some(CapabilityStaticHermesCBundleIdentity {
                compilation_sha256: canonical_identity_sha256(&expected_identity)
                    .expect("C bundle compilation identity must canonicalize"),
                kind: CAPABILITY_STATIC_HERMES_C_BUNDLE_KIND.to_owned(),
            }),
            flags: vec![],
            materials_sha256: "a".repeat(64),
            revision: "b".repeat(40),
        };
        let bridge = capability_static_hermes_c_bundle_compilation_member(
            &bridge_generated_c,
            Some(&bridge_object),
            "bridge",
        )
        .expect("bridge C bundle compilation identity is complete");
        let formatter = capability_static_hermes_c_bundle_compilation_member(
            &formatter_generated_c,
            Some(&formatter_object),
            "formatter",
        )
        .expect("formatter C bundle compilation identity is complete");
        let applications = [CapabilityStaticHermesCBundleCompilationApplication {
            generated_c: &application_generated_c,
            object: Some(&application_object),
            route_id: format!("official-output-physical-unit:{}", "c".repeat(64)),
        }];
        validate_capability_static_hermes_c_bundle_compilation(
            &static_hermes,
            Some(bridge),
            Some(formatter),
            &applications,
        )
        .expect("matching C bundle compilation identity was rejected");

        let mut tampered_application_generated_c = application_generated_c.clone();
        tampered_application_generated_c
            .precompile_process
            .as_mut()
            .expect("application precompile process is present")
            .compiler_arguments_sha256 = "f".repeat(64);
        let tampered_applications = [CapabilityStaticHermesCBundleCompilationApplication {
            generated_c: &tampered_application_generated_c,
            object: Some(&application_object),
            route_id: format!("official-output-physical-unit:{}", "c".repeat(64)),
        }];
        let bridge = capability_static_hermes_c_bundle_compilation_member(
            &bridge_generated_c,
            Some(&bridge_object),
            "bridge",
        )
        .expect("bridge C bundle compilation identity is complete");
        let formatter = capability_static_hermes_c_bundle_compilation_member(
            &formatter_generated_c,
            Some(&formatter_object),
            "formatter",
        )
        .expect("formatter C bundle compilation identity is complete");
        assert!(validate_capability_static_hermes_c_bundle_compilation(
            &static_hermes,
            Some(bridge),
            Some(formatter),
            &tampered_applications,
        )
        .is_err());
    }

    #[test]
    fn capability_static_hermes_per_entry_chunk_precompile_policy_is_compact() {
        let process: CapabilityStaticHermesPrecompileProcessIdentity = serde_json::from_value(
            json!({
                "cBundleMemberCompilationPolicy": {
                    "cOptimizationLevelZero": {
                        "cOptimizationLevel": 0,
                        "functionCount": 1,
                        "optimizationFlag": "-O0",
                        "role": "function",
                        "stage": "c-optimization-level-zero-c-bundle-member-object",
                    },
                    "kind": CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMPILATION_POLICY_KIND,
                    "normalOptimizationFlag": CAPABILITY_STATIC_HERMES_C_BUNDLE_COMPACT_OPTIMIZATION_FLAG,
                },
                "compilerArgumentsSha256": "a".repeat(64),
                "kind": CAPABILITY_STATIC_HERMES_PRECOMPILE_PROCESS_KIND,
                "launcherArgumentsSha256": "b".repeat(64),
            }),
        )
        .expect("per-entry chunk precompile policy failed to parse");
        validate_capability_static_hermes_precompile_process(
            &process,
            CAPABILITY_STATIC_HERMES_C_BUNDLE_COMPACT_OPTIMIZATION_FLAG,
        )
        .expect("compact per-entry chunk precompile policy was rejected");
        assert!(validate_capability_static_hermes_precompile_process(
            &process,
            CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
        )
        .is_err());

        let compilation: CapabilityStaticHermesCBundleMemberCompilationIdentity =
            serde_json::from_value(json!({
                "kind": CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMMAND_KIND,
                "memberCompilations": [
                    {
                        "effectiveArguments": ["-Oz", "-c", "metadata.c", "-o", "member-00000.o"],
                        "member": {"path": "metadata.c", "role": "metadata"},
                        "optimization": CAPABILITY_STATIC_HERMES_C_BUNDLE_COMPACT_OPTIMIZATION_FLAG,
                        "stage": "export-object",
                    },
                    {
                        "effectiveArguments": ["-Oz", "-c", "functions-00000.c", "-o", "member-00001.o"],
                        "member": {
                            "firstFunctionId": 0,
                            "functionCount": 1,
                            "lastFunctionId": 0,
                            "oversize": false,
                            "path": "functions-00000.c",
                            "role": "function",
                            "targetBytes": 2097152,
                        },
                        "optimization": CAPABILITY_STATIC_HERMES_C_BUNDLE_COMPACT_OPTIMIZATION_FLAG,
                        "stage": "export-object",
                    },
                ],
                "relocatableLink": {
                    "arguments": ["-r", "member-00000.o", "member-00001.o", "-o", "unit.o"],
                    "executable": "wasm-ld",
                },
            }))
            .expect("compact per-entry chunk member compilation failed to parse");
        validate_capability_static_hermes_c_bundle_member_compilation(
            &compilation,
            "export-object",
            2_097_152,
            CAPABILITY_STATIC_HERMES_C_BUNDLE_COMPACT_OPTIMIZATION_FLAG,
        )
        .expect("compact per-entry chunk member compilation was rejected");
        assert!(
            validate_capability_static_hermes_c_bundle_member_compilation(
                &compilation,
                "export-object",
                2_097_152,
                CAPABILITY_STATIC_HERMES_C_BUNDLE_BASELINE_OPTIMIZATION_FLAG,
            )
            .is_err()
        );
    }

    #[test]
    fn capability_static_hermes_c_bundle_compilation_preserves_omitted_optimization() {
        let precompile_process = json!({
            "cBundleMemberCompilationPolicy": {
                "cOptimizationLevelZero": {
                    "cOptimizationLevel": 0,
                    "functionCount": 1,
                    "optimizationFlag": "-O0",
                    "role": "function",
                    "stage": "c-optimization-level-zero-c-bundle-member-object",
                },
                "kind": CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMPILATION_POLICY_KIND,
                "normalOptimizationFlag": "-O2",
            },
            "compilerArgumentsSha256": "a".repeat(64),
            "kind": CAPABILITY_STATIC_HERMES_PRECOMPILE_PROCESS_KIND,
            "launcherArgumentsSha256": "b".repeat(64),
        });
        let generated_c: CapabilityProvenanceStaticHermesIdentity = serde_json::from_value(json!({
            "materials": "c".repeat(64),
            "precompileProcess": precompile_process,
            "revision": "d".repeat(40),
        }))
        .expect("Static Hermes generated-C identity fixture parses");
        let raw_member_compilation = json!({
            "kind": CAPABILITY_STATIC_HERMES_C_BUNDLE_MEMBER_COMMAND_KIND,
            "memberCompilations": [{
                "effectiveArguments": ["-O2", "-c", "functions-00000.c", "-o", "member-00000.o"],
                "member": {
                    "firstFunctionId": 0,
                    "functionCount": 1,
                    "lastFunctionId": 0,
                    "oversize": false,
                    "path": "functions-00000.c",
                    "role": "function",
                    "targetBytes": 2097152,
                },
                "optimization": "-O2",
                "stage": "export-object",
            }],
            "relocatableLink": {
                "arguments": ["-r", "member-00000.o", "-o", "unit.o"],
                "executable": "wasm-ld",
            },
        });
        let member_compilation: CapabilityStaticHermesCBundleMemberCompilationIdentity =
            serde_json::from_value(raw_member_compilation.clone())
                .expect("ordinary function C bundle member fixture parses");
        assert_eq!(
            serde_json::to_value(&member_compilation)
                .expect("ordinary function C bundle member fixture serializes"),
            raw_member_compilation,
        );
        let route_id = format!("official-output-physical-unit:{}", "e".repeat(64));
        let expected_identity = json!({
            "applications": [{
                "generatedC": generated_c.precompile_process,
                "object": raw_member_compilation,
                "routeId": route_id,
            }],
            "kind": CAPABILITY_STATIC_HERMES_C_BUNDLE_PACKAGE_COMPILATION_KIND,
        });
        let static_hermes = CapabilityStaticHermesIdentity {
            c_bundle: Some(CapabilityStaticHermesCBundleIdentity {
                compilation_sha256: canonical_identity_sha256(&expected_identity)
                    .expect("C bundle package identity must canonicalize"),
                kind: CAPABILITY_STATIC_HERMES_C_BUNDLE_KIND.to_owned(),
            }),
            flags: vec![],
            materials_sha256: "c".repeat(64),
            revision: "d".repeat(40),
        };
        let applications = [CapabilityStaticHermesCBundleCompilationApplication {
            generated_c: &generated_c,
            object: Some(&member_compilation),
            route_id: format!("official-output-physical-unit:{}", "e".repeat(64)),
        }];
        validate_capability_static_hermes_c_bundle_compilation(
            &static_hermes,
            None,
            None,
            &applications,
        )
        .expect("C bundle package identity changed an omitted ordinary-function optimization");
    }

    #[test]
    fn capability_official_output_chunk_dependency_and_unit_limits_are_closed() {
        let mut manifest_entry = capability_manifest_entry_for_test(
            "convex/example.ts",
            "example",
            Some(CapabilityInvocationAbi::OfficialWrapper),
        );
        manifest_entry.entry_symbol = Some(capability_entry_symbol(&manifest_entry.entry_id));
        let entry = validate_capability_entry_manifest_unit(
            &manifest_entry,
            true,
            CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
        )
        .expect("official-output descriptor entry fixture is valid");
        let routes = vec![capability_runtime_route_for_test(
            &entry,
            "1".repeat(16).as_str(),
            "run",
            'a',
            UdfKind::Query,
        )];
        let manifest_entries = [&manifest_entry];
        let unit = |slot: u32,
                    entry_publication: bool,
                    dependencies: Vec<CapabilityOfficialOutputChunkDependency>| {
            let identity_sha256 = format!("{slot:064x}");
            CapabilityOfficialOutputChunkUnit {
                application_unit_slot: slot,
                chunk_slot: if entry_publication {
                    -1
                } else {
                    i32::try_from(slot).expect("test slot fits i32")
                },
                dependencies,
                entry_publication,
                entry_symbol: format!("sh_export_convex_wasm_official_chunk_{identity_sha256}"),
                exported_unit_name: format!("convex_wasm_official_chunk_{identity_sha256}"),
                identity_sha256,
                javascript: PackageArtifact {
                    sha256: "b".repeat(64),
                    size: 1,
                },
                kind: if entry_publication {
                    CAPABILITY_OFFICIAL_OUTPUT_CHUNK_ENTRY_PUBLICATION_UNIT_KIND.to_owned()
                } else {
                    CAPABILITY_OFFICIAL_OUTPUT_CHUNK_UNIT_KIND.to_owned()
                },
                publication_handoff_slot: if entry_publication { 0 } else { -1 },
            }
        };
        let mut descriptor = CapabilityOfficialOutputChunkApplicationDescriptor {
            entries: vec![CapabilityOfficialOutputChunkEntry {
                dependency_graph_sha256: manifest_entry
                    .local_profile
                    .dependency_graph_sha256
                    .clone(),
                entry_module_path: format!("{}.js", entry.module_path),
                entry_path: entry.entry_path.clone(),
                entry_publication_unit_slot: 3,
                entry_slot: 0,
                handoff_slot: 0,
                module_path: entry.module_path.clone(),
                routes: vec![CapabilityOfficialOutputChunkRoute {
                    export_name: "run".to_owned(),
                    udf_kind: UdfKind::Query,
                    visibility: "public".to_owned(),
                }],
            }],
            identity_sha256: "c".repeat(64),
            initialization: CapabilityOfficialOutputChunkInitialization {
                chunk_slot_count: 3,
                entry_publication_unit_slots: vec![3],
                kind: CAPABILITY_OFFICIAL_OUTPUT_CHUNK_INITIALIZATION_KIND.to_owned(),
                namespace_slot_count: 3,
            },
            kind: CAPABILITY_OFFICIAL_OUTPUT_CHUNK_DESCRIPTOR_KIND.to_owned(),
            native_descriptor: CapabilityOfficialOutputChunkNativeDescriptor {
                destruction: "destroy-store-on-any-initialization-failure".to_owned(),
                initialization: "recursive-closed-literal-require-with-provisional-cjs-namespaces"
                    .to_owned(),
                kind: CAPABILITY_OFFICIAL_OUTPUT_CHUNK_NATIVE_DESCRIPTOR_KIND.to_owned(),
                publication:
                    "authenticated-selected-entry-wrapper-validation-after-selected-closure-initialization"
                        .to_owned(),
                slots: "closed-numbered-namespace-slots-with-per-entry-publication-units"
                    .to_owned(),
            },
            units: vec![
                unit(
                    0,
                    false,
                    vec![
                        CapabilityOfficialOutputChunkDependency {
                            kind: "dynamic-import".to_owned(),
                            path: "chunks/shared.js".to_owned(),
                            slot: 1,
                            specifier: "../shared.js".to_owned(),
                        },
                        CapabilityOfficialOutputChunkDependency {
                            kind: "import-statement".to_owned(),
                            path: "chunks/shared.js".to_owned(),
                            slot: 1,
                            specifier: "../compat/shared.js".to_owned(),
                        },
                    ],
                ),
                unit(
                    1,
                    false,
                    vec![CapabilityOfficialOutputChunkDependency {
                        kind: "import-statement".to_owned(),
                        path: "chunks/root.js".to_owned(),
                        slot: 0,
                        specifier: "./root.js".to_owned(),
                    }],
                ),
                unit(2, false, vec![]),
                unit(3, true, vec![]),
            ],
        };
        validate_capability_official_output_chunk_descriptor(
            &descriptor,
            &manifest_entries,
            std::slice::from_ref(&entry),
            &routes,
        )
        .expect("dynamic, parent-relative, cyclic official-output dependencies were rejected");

        let mut unsupported_kind = descriptor.clone();
        unsupported_kind.units[0].dependencies[0].kind = "require-call".to_owned();
        assert!(validate_capability_official_output_chunk_descriptor(
            &unsupported_kind,
            &manifest_entries,
            std::slice::from_ref(&entry),
            &routes,
        )
        .is_err());

        let mut ambiguous_specifier = descriptor.clone();
        ambiguous_specifier.units[0].dependencies[1].slot = 2;
        ambiguous_specifier.units[0].dependencies[1].specifier = "../shared.js".to_owned();
        assert!(validate_capability_official_output_chunk_descriptor(
            &ambiguous_specifier,
            &manifest_entries,
            std::slice::from_ref(&entry),
            &routes,
        )
        .is_err());

        let mut unnormalized_path = descriptor.clone();
        unnormalized_path.units[0].dependencies[0].path = "chunks/../shared.js".to_owned();
        assert!(validate_capability_official_output_chunk_descriptor(
            &unnormalized_path,
            &manifest_entries,
            std::slice::from_ref(&entry),
            &routes,
        )
        .is_err());

        let mut unnormalized_specifier = descriptor.clone();
        unnormalized_specifier.units[0].dependencies[0].specifier =
            "./chunks/../shared.js".to_owned();
        assert!(validate_capability_official_output_chunk_descriptor(
            &unnormalized_specifier,
            &manifest_entries,
            std::slice::from_ref(&entry),
            &routes,
        )
        .is_err());

        let maximum_units = u32::try_from(CAPABILITY_OFFICIAL_OUTPUT_CHUNK_MAXIMUM_UNITS)
            .expect("maximum unit count fits u32");
        descriptor.units = (0..maximum_units - 1)
            .map(|slot| unit(slot, false, vec![]))
            .collect();
        descriptor.units.push(unit(maximum_units - 1, true, vec![]));
        descriptor.initialization.chunk_slot_count = maximum_units - 1;
        descriptor.initialization.entry_publication_unit_slots = vec![maximum_units - 1];
        descriptor.initialization.namespace_slot_count = maximum_units - 1;
        validate_capability_official_output_chunk_descriptor(
            &descriptor,
            &manifest_entries,
            std::slice::from_ref(&entry),
            &routes,
        )
        .expect("the authenticated official-output chunk unit maximum was rejected");

        descriptor.units[usize::try_from(maximum_units - 1).expect("test slot fits usize")] =
            unit(maximum_units - 1, false, vec![]);
        descriptor.units.push(unit(maximum_units, true, vec![]));
        descriptor.initialization.chunk_slot_count = maximum_units;
        descriptor.initialization.entry_publication_unit_slots = vec![maximum_units];
        descriptor.initialization.namespace_slot_count = maximum_units;
        assert!(validate_capability_official_output_chunk_descriptor(
            &descriptor,
            &manifest_entries,
            std::slice::from_ref(&entry),
            &routes,
        )
        .is_err());
    }

    #[test]
    fn capability_official_output_chunk_physical_unit_identity_is_minimal_and_slot_bound() {
        let unit =
            |slot: u32, marker: char, entry_publication: bool| CapabilityOfficialOutputChunkUnit {
                application_unit_slot: slot,
                chunk_slot: if entry_publication {
                    -1
                } else {
                    i32::try_from(slot).expect("test slot fits i32")
                },
                dependencies: vec![],
                entry_publication,
                entry_symbol: format!(
                    "sh_export_convex_wasm_official_chunk_{}",
                    marker.to_string().repeat(64)
                ),
                exported_unit_name: format!(
                    "convex_wasm_official_chunk_{}",
                    marker.to_string().repeat(64)
                ),
                identity_sha256: marker.to_string().repeat(64),
                javascript: PackageArtifact {
                    sha256: "d".repeat(64),
                    size: 1,
                },
                kind: if entry_publication {
                    CAPABILITY_OFFICIAL_OUTPUT_CHUNK_ENTRY_PUBLICATION_UNIT_KIND.to_owned()
                } else {
                    CAPABILITY_OFFICIAL_OUTPUT_CHUNK_UNIT_KIND.to_owned()
                },
                publication_handoff_slot: if entry_publication { 0 } else { -1 },
            };
        let descriptor = CapabilityOfficialOutputChunkApplicationDescriptor {
            entries: vec![],
            identity_sha256: "e".repeat(64),
            initialization: CapabilityOfficialOutputChunkInitialization {
                chunk_slot_count: 2,
                entry_publication_unit_slots: vec![2],
                kind: CAPABILITY_OFFICIAL_OUTPUT_CHUNK_INITIALIZATION_KIND.to_owned(),
                namespace_slot_count: 2,
            },
            kind: CAPABILITY_OFFICIAL_OUTPUT_CHUNK_DESCRIPTOR_KIND.to_owned(),
            native_descriptor: CapabilityOfficialOutputChunkNativeDescriptor {
                destruction: "destroy-store-on-any-initialization-failure".to_owned(),
                initialization: "recursive-closed-literal-require-with-provisional-cjs-namespaces"
                    .to_owned(),
                kind: CAPABILITY_OFFICIAL_OUTPUT_CHUNK_NATIVE_DESCRIPTOR_KIND.to_owned(),
                publication:
                    "authenticated-selected-entry-wrapper-validation-after-selected-closure-initialization"
                        .to_owned(),
                slots: "closed-numbered-namespace-slots-with-per-entry-publication-units"
                    .to_owned(),
            },
            units: vec![unit(0, 'a', false), unit(1, 'b', false), unit(2, 'c', true)],
        };
        let generated_json = json!({
            "entryPublication": false,
            "identitySha256": "a".repeat(64),
            "kind": CAPABILITY_OFFICIAL_OUTPUT_CHUNK_UNIT_KIND,
        });
        let generated: CapabilityOfficialOutputChunkGeneratedCUnitIdentity =
            serde_json::from_value(generated_json.clone())
                .expect("minimal generated-C physical unit identity failed to parse");
        assert_eq!(
            serde_json::to_value(&generated)
                .expect("minimal generated-C physical unit identity failed to serialize"),
            generated_json,
        );
        let object_json = json!({"identitySha256": "a".repeat(64)});
        let object: CapabilityOfficialOutputChunkObjectUnitIdentity =
            serde_json::from_value(object_json.clone())
                .expect("minimal object physical unit identity failed to parse");
        assert_eq!(
            serde_json::to_value(&object)
                .expect("minimal object physical unit identity failed to serialize"),
            object_json,
        );
        validate_capability_official_output_chunk_physical_unit_binding(
            0,
            &generated,
            &object,
            &descriptor,
        )
        .expect("physical unit identity matching its compiled slot was rejected");

        let mut swapped_identity = generated.clone();
        swapped_identity.identity_sha256 = "b".repeat(64);
        assert!(
            validate_capability_official_output_chunk_physical_unit_binding(
                0,
                &swapped_identity,
                &object,
                &descriptor,
            )
            .is_err()
        );

        let mut swapped_topology = descriptor.clone();
        swapped_topology.units.swap(0, 1);
        assert!(
            validate_capability_official_output_chunk_physical_unit_binding(
                0,
                &generated,
                &object,
                &swapped_topology,
            )
            .is_err()
        );

        let mut generated_with_unknown_field = generated_json;
        generated_with_unknown_field["applicationIdentitySha256"] = json!("f".repeat(64));
        assert!(
            serde_json::from_value::<CapabilityOfficialOutputChunkGeneratedCUnitIdentity>(
                generated_with_unknown_field,
            )
            .is_err()
        );

        let mut object_with_unknown_field = object_json;
        object_with_unknown_field["applicationUnitSlot"] = json!(0);
        assert!(
            serde_json::from_value::<CapabilityOfficialOutputChunkObjectUnitIdentity>(
                object_with_unknown_field,
            )
            .is_err()
        );
    }

    #[test]
    fn capability_official_output_chunk_entries_bind_selected_physical_units() {
        let identities = |marker: char| {
            let provenance = serde_json::from_value::<CapabilityEntryBuildProvenanceV2>(
                capability_split_provenance_json_for_test(),
            )
            .expect("physical-unit provenance fixture failed to parse");
            let mut identities = provenance
                .application_units
                .into_iter()
                .next()
                .expect("physical-unit provenance fixture has no application unit")
                .identities;
            identities.export_object.generated_c.sha256 = marker.to_string().repeat(64);
            identities
        };
        let entries = vec![
            capability_runtime_entry_for_test('a'),
            capability_runtime_entry_for_test('b'),
        ];
        let descriptor_entries = entries
            .iter()
            .enumerate()
            .map(|(entry_slot, entry)| CapabilityOfficialOutputChunkEntry {
                dependency_graph_sha256: "d".repeat(64),
                entry_module_path: format!("{}.js", entry.module_path),
                entry_path: entry.entry_path.clone(),
                entry_publication_unit_slot: u32::try_from(entry_slot + 4)
                    .expect("test slot fits u32"),
                entry_slot: u32::try_from(entry_slot + 2).expect("test slot fits u32"),
                handoff_slot: u32::try_from(entry_slot).expect("test slot fits u32"),
                module_path: entry.module_path.clone(),
                routes: vec![],
            })
            .collect::<Vec<_>>();
        let first_physical = identities('a');
        let second_physical = identities('b');
        let publication = identities('p');
        let compiled_units = vec![
            CapabilityOfficialOutputChunkCompiledUnit {
                application_unit_slot: 0,
                identities: identities('0'),
            },
            CapabilityOfficialOutputChunkCompiledUnit {
                application_unit_slot: 1,
                identities: identities('1'),
            },
            CapabilityOfficialOutputChunkCompiledUnit {
                application_unit_slot: 2,
                identities: first_physical.clone(),
            },
            CapabilityOfficialOutputChunkCompiledUnit {
                application_unit_slot: 3,
                identities: second_physical.clone(),
            },
            CapabilityOfficialOutputChunkCompiledUnit {
                application_unit_slot: 4,
                identities: publication.clone(),
            },
        ];
        let mut application_units = vec![
            CapabilityApplicationUnitProvenance {
                application_unit_slot: Some(2),
                entry_id: entries[0].entry_id.clone(),
                entry_slot: 0,
                identities: first_physical,
            },
            CapabilityApplicationUnitProvenance {
                application_unit_slot: Some(3),
                entry_id: entries[1].entry_id.clone(),
                entry_slot: 1,
                identities: second_physical,
            },
        ];
        validate_capability_official_output_chunk_entry_physical_provenance(
            &application_units,
            &descriptor_entries,
            &entries,
            &compiled_units,
        )
        .expect("entries bound to distinct selected physical units were rejected");

        application_units[1].identities = publication;
        assert!(
            validate_capability_official_output_chunk_entry_physical_provenance(
                &application_units,
                &descriptor_entries,
                &entries,
                &compiled_units,
            )
            .is_err()
        );
    }

    #[test]
    fn capability_producer_identity_is_closed_sorted_and_authenticated() {
        let current = json!({
            "kind": CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND,
            "manifest": {
                "path": "scripts/convex-wasm-artifact-producer-source-manifest.json",
                "sha256": "a".repeat(64),
                "size": 1,
            },
            "nodeVersion": "v24.15.0",
            "sha256": "b".repeat(64),
            "sources": [
                {"path": "scripts/a.mjs", "sha256": "c".repeat(64), "size": 1},
                {"path": "scripts/b.mjs", "sha256": "d".repeat(64), "size": 2},
            ],
        });
        let parsed: CapabilityProducerIdentity = serde_json::from_value(current.clone())
            .expect("current producer identity failed to parse");
        assert_eq!(
            validate_capability_producer_identity(&parsed)
                .expect("current producer identity failed authentication"),
            CapabilityProducerImplementationIdentity {
                kind: CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND.to_owned(),
                sha256: "b".repeat(64),
            }
        );

        let mut unsorted = current.clone();
        unsorted["sources"]
            .as_array_mut()
            .expect("producer source fixture must be an array")
            .swap(0, 1);
        let parsed: CapabilityProducerIdentity =
            serde_json::from_value(unsorted).expect("unsorted producer identity failed to parse");
        assert!(validate_capability_producer_identity(&parsed).is_err());

        let mut non_normalized_path = current.clone();
        non_normalized_path["sources"][0]["path"] = json!("scripts/../scripts/a.mjs");
        let parsed: CapabilityProducerIdentity = serde_json::from_value(non_normalized_path)
            .expect("non-normalized producer path failed to parse");
        assert!(validate_capability_producer_identity(&parsed).is_err());

        let mut extra_field = current;
        extra_field["manifest"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<CapabilityProducerIdentity>(extra_field).is_err());
    }

    #[test]
    fn capability_schema_4_requires_deterministic_entry_factory_symbols() {
        let mut entry = capability_manifest_entry_for_test("convex/a.ts", "a", None);
        validate_capability_entry_manifest_unit(
            &entry,
            false,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
        )
        .expect("schema 3 singleton entry without a factory symbol was rejected");
        assert!(validate_capability_entry_manifest_unit(
            &entry,
            true,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
        )
        .is_err());

        entry.entry_symbol = Some("sh_export_convex_wasm_entry_wrong".to_owned());
        assert!(validate_capability_entry_manifest_unit(
            &entry,
            true,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
        )
        .is_err());

        entry.entry_symbol = Some(capability_entry_symbol(&entry.entry_id));
        validate_capability_entry_manifest_unit(
            &entry,
            true,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
        )
        .expect("schema 4 deterministic entry factory symbol was rejected");
    }

    #[test]
    fn capability_selector_identity_is_generic_and_schema_3_is_compatible() {
        let route_id = "a".repeat(64);
        let legacy = capability_route_selector_id(
            CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            &route_id,
            "ignored",
            "ignored",
            UdfKind::Query,
            None,
        )
        .expect("schema 3 selector identity was rejected");
        assert_eq!(legacy, &route_id[..16]);

        let base = capability_route_selector_id(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            &route_id,
            "sh_export_convex_wasm_entry_a",
            "handler",
            UdfKind::Query,
            None,
        )
        .expect("schema 4 selector identity was rejected");
        for changed in [
            capability_route_selector_id(
                CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
                LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
                &route_id,
                "sh_export_convex_wasm_entry_b",
                "handler",
                UdfKind::Query,
                None,
            ),
            capability_route_selector_id(
                CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
                LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
                &route_id,
                "sh_export_convex_wasm_entry_a",
                "otherHandler",
                UdfKind::Query,
                None,
            ),
            capability_route_selector_id(
                CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
                LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
                &route_id,
                "sh_export_convex_wasm_entry_a",
                "handler",
                UdfKind::Mutation,
                None,
            ),
        ] {
            assert_ne!(
                base,
                changed.expect("changed schema 4 selector was rejected")
            );
        }
    }

    #[test]
    fn capability_schema_4_routes_bind_known_entry_units() {
        let entries = vec![
            capability_runtime_entry_for_test('a'),
            capability_runtime_entry_for_test('b'),
        ];
        let mut route = CapabilityEntryManifestRoute {
            entry_id: Some(entries[0].entry_id.clone()),
            entry_selector_id: "1".repeat(16),
            entry_symbol: Some(entries[0].entry_symbol.clone()),
            export_name: "handler".to_owned(),
            route_id: "c".repeat(64),
            udf_kind: UdfKind::Query,
            visibility: "public".to_owned(),
        };
        assert_eq!(
            capability_manifest_route_entry(
                CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
                &entries,
                &route,
            )
            .expect("schema 4 route rejected its entry unit")
            .entry_id,
            entries[0].entry_id
        );

        route.entry_id = None;
        assert!(capability_manifest_route_entry(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            &entries,
            &route,
        )
        .is_err());
        route.entry_id = Some("f".repeat(64));
        assert!(capability_manifest_route_entry(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            &entries,
            &route,
        )
        .is_err());
        route.entry_id = Some(entries[0].entry_id.clone());
        route.entry_symbol = Some("wrong".to_owned());
        assert!(capability_manifest_route_entry(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            &entries,
            &route,
        )
        .is_err());
    }

    #[test]
    fn capability_schema_4_routes_follow_entry_path_order() {
        let mut entries = vec![
            capability_runtime_entry_for_test('a'),
            capability_runtime_entry_for_test('b'),
        ];
        entries[0].entry_id = "f".repeat(64);
        entries[0].entry_symbol = capability_entry_symbol(&entries[0].entry_id);
        entries[1].entry_id = "0".repeat(64);
        entries[1].entry_symbol = capability_entry_symbol(&entries[1].entry_id);
        let route = |entry: &CapabilityEntryRuntimeUnitIdentity,
                     entry_selector_id: String,
                     export_name: &str,
                     route_id: String| CapabilityEntryManifestRoute {
            entry_id: Some(entry.entry_id.clone()),
            entry_selector_id,
            entry_symbol: Some(entry.entry_symbol.clone()),
            export_name: export_name.to_owned(),
            route_id,
            udf_kind: UdfKind::Query,
            visibility: "public".to_owned(),
        };
        let routes = vec![
            route(&entries[0], "1".repeat(16), "first", "c".repeat(64)),
            route(&entries[1], "2".repeat(16), "second", "d".repeat(64)),
        ];
        let route_entries = validate_capability_entry_route_table(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            &entries,
            &routes,
        )
        .expect("entry-path-ordered schema 4 routes were rejected");
        assert_eq!(route_entries[0].entry_id, "f".repeat(64));
        assert_eq!(route_entries[1].entry_id, "0".repeat(64));

        let reversed = [routes[1].clone(), routes[0].clone()];
        assert!(validate_capability_entry_route_table(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            &entries,
            &reversed,
        )
        .is_err());

        let mut duplicate_selector = routes;
        duplicate_selector[1].entry_selector_id = duplicate_selector[0].entry_selector_id.clone();
        assert!(validate_capability_entry_route_table(
            CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
            &entries,
            &duplicate_selector,
        )
        .is_err());
    }

    #[test]
    fn capability_schema_4_routes_cover_every_entry_unit() {
        let entries = vec![
            capability_runtime_entry_for_test('a'),
            capability_runtime_entry_for_test('b'),
        ];
        let mut routes = vec![capability_runtime_route_for_test(
            &entries[0],
            "1111111111111111",
            "first",
            'c',
            UdfKind::Query,
        )];
        assert!(validate_capability_entry_route_coverage(&entries, &routes).is_err());

        routes.push(capability_runtime_route_for_test(
            &entries[1],
            "2222222222222222",
            "second",
            'd',
            UdfKind::Mutation,
        ));
        validate_capability_entry_route_coverage(&entries, &routes)
            .expect("schema 4 route table covering every entry unit was rejected");
    }

    #[test]
    fn capability_schema_4_selector_provenance_is_exact() {
        let entries = [
            capability_runtime_entry_for_test('a'),
            capability_runtime_entry_for_test('b'),
        ];
        let routes = vec![
            capability_runtime_route_for_test(
                &entries[0],
                "1111111111111111",
                "first",
                'c',
                UdfKind::Query,
            ),
            capability_runtime_route_for_test(
                &entries[1],
                "2222222222222222",
                "second",
                'd',
                UdfKind::Mutation,
            ),
        ];
        let producer_implementation = CapabilityProducerImplementationIdentity {
            kind: CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND.to_owned(),
            sha256: "9".repeat(64),
        };
        let selector = json!({
            "abiVersion": LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            "applicationEntryCount": entries.len(),
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryBySlotSymbol": CAPABILITY_APPLICATION_FACTORY_BY_SLOT_SYMBOL,
            "kind": CAPABILITY_ENTRY_MANIFEST_KIND,
            "members": routes.iter().map(|route| json!({
                "entrySelectorId": route.entry_selector_id,
                "entrySymbol": route.entry_symbol,
                "handlerExportName": route.export_name,
                "handlerUdfKind": route.udf_kind,
            })).collect::<Vec<_>>(),
            "producerImplementation": producer_implementation,
            "sourceSha256": "e".repeat(64),
        });
        validate_legacy_capability_selector_provenance(
            &selector,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            entries.len(),
            &routes,
            &producer_implementation,
        )
        .expect("valid schema 4 selector provenance was rejected");

        let mut singleton = selector.clone();
        singleton["entryId"] = json!(entries[0].entry_id);
        assert!(validate_legacy_capability_selector_provenance(
            &singleton,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            entries.len(),
            &routes,
            &producer_implementation,
        )
        .is_err());

        for field in ["entrySymbol", "handlerExportName", "handlerUdfKind"] {
            let mut changed = selector.clone();
            changed["members"][0][field] = json!("changed");
            assert!(validate_legacy_capability_selector_provenance(
                &changed,
                LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
                entries.len(),
                &routes,
                &producer_implementation,
            )
            .is_err());

            let mut missing = selector.clone();
            missing["members"][0]
                .as_object_mut()
                .expect("selector member fixture must be an object")
                .remove(field);
            assert!(validate_legacy_capability_selector_provenance(
                &missing,
                LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
                entries.len(),
                &routes,
                &producer_implementation,
            )
            .is_err());
        }

        for field in [
            "applicationEntryCount",
            "applicationEntryCountSymbol",
            "applicationFactoryBySlotSymbol",
        ] {
            let mut changed = selector.clone();
            changed[field] = json!("changed");
            assert!(validate_legacy_capability_selector_provenance(
                &changed,
                LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
                entries.len(),
                &routes,
                &producer_implementation,
            )
            .is_err());
        }

        let mut changed_producer = selector;
        changed_producer["producerImplementation"]["sha256"] = json!("a".repeat(64));
        assert!(validate_legacy_capability_selector_provenance(
            &changed_producer,
            LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            entries.len(),
            &routes,
            &producer_implementation,
        )
        .is_err());
    }

    #[test]
    fn current_capability_selectors_bind_complete_shared_and_topology_identities() {
        let mut entries = [
            capability_runtime_entry_for_test('a'),
            capability_runtime_entry_for_test('b'),
        ];
        for entry in &mut entries {
            entry.invocation_abi = Some(CapabilityInvocationAbi::LegacyHandler);
        }
        let routes = vec![
            capability_runtime_route_for_test(
                &entries[0],
                "1111111111111111",
                "first",
                'c',
                UdfKind::Query,
            ),
            capability_runtime_route_for_test(
                &entries[1],
                "2222222222222222",
                "second",
                'd',
                UdfKind::Mutation,
            ),
        ];
        let compile_flags = vec!["-O2".to_owned(), "-DNDEBUG".to_owned()];
        let partition_policy = CohortPartitionPolicy {
            bucket_count: 16,
            hash: "sha256-route-id-domain-first-u32-be-modulo".to_owned(),
            kind: "convex-wasm-cohort-partition-v1".to_owned(),
        };
        let producer_implementation = CapabilityProducerImplementationIdentity {
            kind: CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND.to_owned(),
            sha256: "9".repeat(64),
        };
        let semantic_environment = json!({
            "LANG": "C",
            "LC_ALL": "C",
            "SOURCE_DATE_EPOCH": "0",
            "TZ": "UTC",
        });
        let expected = CapabilitySelectorExpectedIdentity {
            abi_version: CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            compile_flags: &compile_flags,
            emscripten_llvm_revision: "llvm-revision",
            emscripten_materials: "1",
            emscripten_revision: "emscripten-revision",
            partition_policy: &partition_policy,
            pipeline_kind: "convex-wasm-artifact-pipeline-v9",
            producer_implementation: &producer_implementation,
            semantic_environment: &semantic_environment,
        };
        let members = routes
            .iter()
            .map(|route| {
                json!({
                    "entrySelectorId": route.entry_selector_id,
                    "entrySymbol": route.entry_symbol,
                    "handlerExportName": route.export_name,
                    "handlerUdfKind": route.udf_kind,
                    "invocationAbi": CapabilityInvocationAbi::LegacyHandler,
                })
            })
            .collect::<Vec<_>>();
        let per_entry_selector = json!({
            "abiVersion": CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            "applicationEntryCount": entries.len(),
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryBySlotSymbol": CAPABILITY_APPLICATION_FACTORY_BY_SLOT_SYMBOL,
            "kind": CAPABILITY_ENTRY_MANIFEST_KIND,
            "members": members,
            "partitionPolicy": {
                "bucketCount": 16,
                "hash": "sha256-route-id-domain-first-u32-be-modulo",
                "kind": "convex-wasm-cohort-partition-v1",
            },
            "pipelineKind": "convex-wasm-artifact-pipeline-v9",
            "producerImplementation": producer_implementation,
            "semanticEnvironment": semantic_environment,
            "sourceSha256": "e".repeat(64),
            "toolchain": {
                "compileFlags": compile_flags,
                "emscriptenMaterials": "1",
                "emscriptenRevision": "emscripten-revision",
                "llvmRevision": "llvm-revision",
            },
        });
        let validate_per_entry = |selector: &JsonValue| {
            validate_capability_per_entry_selector_provenance(
                selector,
                entries.len(),
                &routes,
                &expected,
            )
        };
        validate_per_entry(&per_entry_selector)
            .expect("complete current per-entry selector identity was rejected");

        let mut missing_shared_field = per_entry_selector.clone();
        missing_shared_field
            .as_object_mut()
            .expect("per-entry selector fixture must be an object")
            .remove("toolchain");
        assert!(validate_per_entry(&missing_shared_field).is_err());
        let mut extra_field = per_entry_selector.clone();
        extra_field["unexpected"] = json!(true);
        assert!(validate_per_entry(&extra_field).is_err());
        let mut wrong_partition = per_entry_selector.clone();
        wrong_partition["partitionPolicy"]["bucketCount"] = json!(15);
        assert!(validate_per_entry(&wrong_partition).is_err());
        let mut wrong_pipeline = per_entry_selector.clone();
        wrong_pipeline["pipelineKind"] = json!("convex-wasm-artifact-pipeline-v8");
        assert!(validate_per_entry(&wrong_pipeline).is_err());
        let mut wrong_environment = per_entry_selector.clone();
        wrong_environment["semanticEnvironment"]["TZ"] = json!("PST8PDT");
        assert!(validate_per_entry(&wrong_environment).is_err());
        for (field, value) in [
            ("compileFlags", json!(["-O3"])),
            ("emscriptenMaterials", json!("2")),
            ("emscriptenRevision", json!("other-emscripten")),
            ("llvmRevision", json!("other-llvm")),
        ] {
            let mut wrong_toolchain = per_entry_selector.clone();
            wrong_toolchain["toolchain"][field] = value;
            assert!(
                validate_per_entry(&wrong_toolchain).is_err(),
                "accepted changed selector toolchain {field}"
            );
        }
        let mut wrong_invocation_abi = per_entry_selector.clone();
        wrong_invocation_abi["members"][0]["invocationAbi"] =
            json!(CapabilityInvocationAbi::OfficialWrapper);
        assert!(validate_per_entry(&wrong_invocation_abi).is_err());

        let application_unit = CapabilityPhysicalApplicationUnitIdentity {
            compiler_mode: "whole-application".to_owned(),
            entries: entries
                .iter()
                .enumerate()
                .map(
                    |(handoff_slot, entry)| CapabilityPhysicalApplicationUnitEntryIdentity {
                        entry_path: entry.entry_path.clone(),
                        handoff_slot: u32::try_from(handoff_slot).expect("fixture slot fits u32"),
                    },
                )
                .collect(),
            entry_symbol: "sh_export_convex_wasm_application".to_owned(),
            exported_unit_name: "convex_wasm_application".to_owned(),
            identity_sha256: "f".repeat(64),
            kind: "convex-wasm-physical-application-unit-v1".to_owned(),
            unit_count: 1,
        };
        let mut multi_entry_selector = per_entry_selector.clone();
        let multi_entry_selector_object = multi_entry_selector
            .as_object_mut()
            .expect("multi-entry selector fixture must be an object");
        multi_entry_selector_object.remove("applicationFactoryBySlotSymbol");
        multi_entry_selector_object.insert(
            "applicationFactoryByUnitSlotSymbol".to_owned(),
            json!(CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL),
        );
        multi_entry_selector_object.insert(
            "applicationUnit".to_owned(),
            serde_json::to_value(&application_unit).expect("application-unit fixture serializes"),
        );
        multi_entry_selector_object.insert("applicationUnitCount".to_owned(), json!(1));
        multi_entry_selector_object.insert(
            "applicationUnitCountSymbol".to_owned(),
            json!(CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL),
        );
        let validate_multi_entry = |selector: &JsonValue| {
            validate_capability_multi_entry_unit_selector_provenance(
                selector,
                &entries,
                &routes,
                &application_unit,
                &expected,
            )
        };
        validate_multi_entry(&multi_entry_selector)
            .expect("complete current multi-entry selector identity was rejected");

        let mut wrong_application_unit = multi_entry_selector.clone();
        wrong_application_unit["applicationUnit"]["identitySha256"] = json!("0".repeat(64));
        assert!(validate_multi_entry(&wrong_application_unit).is_err());
        let mut wrong_application_unit_count = multi_entry_selector.clone();
        wrong_application_unit_count["applicationUnitCount"] = json!(2);
        assert!(validate_multi_entry(&wrong_application_unit_count).is_err());
        let mut missing_application_unit = multi_entry_selector;
        missing_application_unit
            .as_object_mut()
            .expect("multi-entry selector fixture must be an object")
            .remove("applicationUnit");
        assert!(validate_multi_entry(&missing_application_unit).is_err());
    }

    #[test]
    fn official_output_selector_schema_is_closed() {
        let descriptor = capability_official_output_chunk_descriptor_json_for_test();
        let selector = json!({
            "abiVersion": CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            "applicationEntryCount": 1,
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryByUnitSlotSymbol": CAPABILITY_APPLICATION_FACTORY_BY_UNIT_SLOT_SYMBOL,
            "applicationUnitCount": 2,
            "applicationUnitCountSymbol": CAPABILITY_APPLICATION_UNIT_COUNT_SYMBOL,
            "chunkApplication": descriptor,
            "kind": CAPABILITY_ENTRY_MANIFEST_KIND,
            "members": [{
                "entrySelectorId": "1".repeat(16),
                "entrySymbol": "sh_export_convex_wasm_entry_a",
                "handlerExportName": "run",
                "handlerUdfKind": "query",
                "invocationAbi": CapabilityInvocationAbi::OfficialWrapper,
            }],
            "partitionPolicy": {
                "bucketCount": 16,
                "hash": "sha256-route-id-domain-first-u32-be-modulo",
                "kind": "convex-wasm-cohort-partition-v1",
            },
            "pipelineKind": "convex-wasm-artifact-pipeline-v9",
            "producerImplementation": {
                "kind": CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND,
                "sha256": "9".repeat(64),
            },
            "semanticEnvironment": {},
            "sourceSha256": "e".repeat(64),
            "toolchain": {
                "compileFlags": ["-O2"],
                "emscriptenMaterials": "1".repeat(64),
                "emscriptenRevision": "a".repeat(40),
                "llvmRevision": "b".repeat(40),
            },
        });
        serde_json::from_value::<CapabilityOfficialOutputChunkSelectorIdentity>(selector.clone())
            .expect("complete official-output selector identity failed to parse");

        let mut missing = selector.clone();
        missing
            .as_object_mut()
            .expect("official-output selector fixture must be an object")
            .remove("partitionPolicy");
        assert!(
            serde_json::from_value::<CapabilityOfficialOutputChunkSelectorIdentity>(missing)
                .is_err()
        );
        let mut extra = selector;
        extra["unexpected"] = json!(true);
        assert!(
            serde_json::from_value::<CapabilityOfficialOutputChunkSelectorIdentity>(extra).is_err()
        );
    }

    #[test]
    fn capability_selector_abi_v2_provenance_requires_exact_invocation_abi() {
        let mut entries = [
            capability_runtime_entry_for_test('a'),
            capability_runtime_entry_for_test('b'),
        ];
        for entry in &mut entries {
            entry.invocation_abi = Some(CapabilityInvocationAbi::OfficialWrapper);
        }
        let routes = vec![
            capability_runtime_route_for_test(
                &entries[0],
                "1111111111111111",
                "first",
                'c',
                UdfKind::Query,
            ),
            capability_runtime_route_for_test(
                &entries[1],
                "2222222222222222",
                "second",
                'd',
                UdfKind::Mutation,
            ),
        ];
        let producer_implementation = CapabilityProducerImplementationIdentity {
            kind: CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND.to_owned(),
            sha256: "9".repeat(64),
        };
        let selector = json!({
            "abiVersion": CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            "applicationEntryCount": entries.len(),
            "applicationEntryCountSymbol": CAPABILITY_APPLICATION_ENTRY_COUNT_SYMBOL,
            "applicationFactoryBySlotSymbol": CAPABILITY_APPLICATION_FACTORY_BY_SLOT_SYMBOL,
            "kind": CAPABILITY_ENTRY_MANIFEST_KIND,
            "members": routes.iter().map(|route| json!({
                "entrySelectorId": route.entry_selector_id,
                "entrySymbol": route.entry_symbol,
                "handlerExportName": route.export_name,
                "handlerUdfKind": route.udf_kind,
                "invocationAbi": CapabilityInvocationAbi::OfficialWrapper,
            })).collect::<Vec<_>>(),
            "producerImplementation": producer_implementation,
            "sourceSha256": "e".repeat(64),
        });
        validate_legacy_capability_selector_provenance(
            &selector,
            CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            entries.len(),
            &routes,
            &producer_implementation,
        )
        .expect("valid selector ABI v2 provenance was rejected");

        let mut changed = selector.clone();
        changed["members"][0]["invocationAbi"] = json!(CapabilityInvocationAbi::LegacyHandler);
        assert!(validate_legacy_capability_selector_provenance(
            &changed,
            CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            entries.len(),
            &routes,
            &producer_implementation,
        )
        .is_err());

        let mut missing = selector;
        missing["members"][0]
            .as_object_mut()
            .expect("selector member fixture must be an object")
            .remove("invocationAbi");
        assert!(validate_legacy_capability_selector_provenance(
            &missing,
            CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
            entries.len(),
            &routes,
            &producer_implementation,
        )
        .is_err());
    }

    #[test]
    fn capability_runtime_main_keeps_the_cohort_selector_abi() {
        assert_eq!(COHORT_ENTRY_SELECTOR_ABI_VERSION, 1);
        assert_eq!(CAPABILITY_ENTRY_SELECTOR_ABI_VERSION, 2);
        assert_ne!(
            COHORT_ENTRY_SELECTOR_ABI_VERSION,
            CAPABILITY_ENTRY_SELECTOR_ABI_VERSION
        );

        let runtime_main: CapabilitySplitRuntimeMainIdentity = serde_json::from_value(json!({
            "cohortEntrySelectorAbiVersion": COHORT_ENTRY_SELECTOR_ABI_VERSION,
            "compileFlags": [],
            "effectExecutionMode": "guest-promise-event-loop",
            "emscripten": {
                "llvmRevision": "llvm",
                "materials": "materials",
                "revision": "emscripten",
            },
            "entrySelectorSymbol": "convex_wasm_selected_exported_unit",
            "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
            "pipelineKind": "convex-wasm-artifact-pipeline-v9",
            "producerImplementation": {
                "kind": CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND,
                "sha256": "9".repeat(64),
            },
            "runtimeHeaders": "headers",
            "runtimeMain": "main",
            "runtimeMainLanguage": "c++",
            "semanticEnvironment": {},
            "unitTopology": {
                "applicationSelectorSymbol": "convex_wasm_selected_exported_unit",
                "bridgeEntrySymbol": "sh_export_convex_wasm_capability_bridge",
                "bridgeExportedUnitName": "convex_wasm_capability_bridge",
                "kind": "convex-wasm-capability-unit-topology-v1",
            },
        }))
        .expect("runtime-main identity failed to parse");
        assert_eq!(
            runtime_main.cohort_entry_selector_abi_version,
            COHORT_ENTRY_SELECTOR_ABI_VERSION
        );
        assert_ne!(
            runtime_main.cohort_entry_selector_abi_version,
            CAPABILITY_ENTRY_SELECTOR_ABI_VERSION
        );
    }

    #[test]
    fn capability_package_identity_authenticates_distinct_route_leases() {
        let entries = vec![
            capability_runtime_entry_for_test('a'),
            capability_runtime_entry_for_test('b'),
        ];
        let routes = vec![
            capability_runtime_route_for_test(
                &entries[0],
                "1111111111111111",
                "first",
                'c',
                UdfKind::Query,
            ),
            capability_runtime_route_for_test(
                &entries[1],
                "2222222222222222",
                "second",
                'd',
                UdfKind::Mutation,
            ),
        ];
        let identity =
            ValidatedWasmUdfPackageIdentity::CapabilityEntry(CapabilityEntryRuntimeIdentity {
                entries,
                manifest_schema_version: CAPABILITY_PACKAGE_MANIFEST_SCHEMA_VERSION,
                artifact_pipeline_sha256: "3".repeat(64),
                compiler: CapabilityCompilerIdentity {
                    admitted_language_version: 1,
                    compiler_revision: "compiler".to_owned(),
                    lowering_pipeline_sha256: "4".repeat(64),
                    source_pipeline_sha256: "5".repeat(64),
                    static_hermes_global_policy: CapabilityStaticHermesGlobalPolicyIdentity {
                        inventory_sha256: "6".repeat(64),
                        kind: "convex-wasm-runtime-surface-policy-identity".to_owned(),
                        runtime_surface_policy_sha256: "7".repeat(64),
                    },
                    static_hermes_revision: "static-hermes".to_owned(),
                },
                official_output_chunk_entry_slots: None,
                opaque_value_abi_version: OPAQUE_VALUE_ABI_VERSION,
                routes: routes.clone(),
            });
        let leases = routes
            .iter()
            .map(|route| {
                DeploymentRouteLease::capability_entry_for_test(
                    route.entry_id.clone(),
                    route.entry_selector_id.clone(),
                    route.export_name.clone(),
                    route.route_id.clone(),
                    route.udf_kind,
                    route.visibility.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_ne!(leases[0], leases[1]);
        assert!(leases
            .iter()
            .all(|lease| identity.authenticates_route_lease(lease)));

        let mut forged = leases[0].clone();
        forged.entry_id = Some("f".repeat(64));
        assert!(!identity.authenticates_route_lease(&forged));
    }

    fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        fs::write(path, bytes)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(())
    }

    fn source_keyed_capability_precompiler_material_identity() -> PrecompilerMaterialIdentity {
        PrecompilerMaterialIdentity {
            binary: PrecompilerBinaryIdentity {
                sha256: "3".repeat(64),
                size: 1,
            },
            kind: VERIFIED_PRECOMPILER_PACKAGE_KIND.to_owned(),
            manifest_kind: PRECOMPILER_PACKAGE_MANIFEST_KIND.to_owned(),
            manifest_schema_version: PRECOMPILER_PACKAGE_MANIFEST_SCHEMA_VERSION,
            manifest_sha256: "4".repeat(64),
            package_id: "5".repeat(64),
            source_tree_sha256: "6".repeat(64),
            target_triple: "x86_64-unknown-linux-gnu".to_owned(),
            wasmtime_revision: GENERATED_WASMTIME_REVISION.to_owned(),
        }
    }

    fn source_keyed_capability_route(
        entry_id: &str,
        export_name: String,
    ) -> RuntimeRegistrySourceKeyedCapabilityRoute {
        let route_id = sha256(&canonical_bytes(&json!({
            "domain": "convex-wasm-capability-route-v1",
            "entryId": entry_id,
            "exportName": export_name,
            "udfKind": UdfKind::Query,
            "visibility": "public",
        })));
        RuntimeRegistrySourceKeyedCapabilityRoute {
            entry_id: entry_id.to_owned(),
            entry_selector_id: route_id[..16].to_owned(),
            export_name,
            route_id,
        }
    }

    fn write_source_keyed_capability_package(
        root: &Path,
        core_wasm: &[u8],
        serialized_module: &[u8],
        selector_count: usize,
    ) -> anyhow::Result<(String, Vec<RuntimeRegistrySourceKeyedCapabilityRoute>)> {
        anyhow::ensure!(
            !core_wasm.is_empty() && !serialized_module.is_empty(),
            "source-keyed fixture requires nonempty Core Wasm and AOT artifacts"
        );
        let entry =
            capability_manifest_entry_for_test("convex/sourceKeyed.ts", "sourceKeyed", None);
        let routes = (0..selector_count)
            .map(|index| {
                source_keyed_capability_route(&entry.entry_id, format!("route_{index:08}"))
            })
            .collect::<Vec<_>>();
        let route_manifest = routes
            .iter()
            .map(|route| {
                json!({
                    "entrySelectorId": route.entry_selector_id,
                    "exportName": route.export_name,
                    "routeId": route.route_id,
                    "udfKind": "query",
                    "visibility": "public",
                })
            })
            .collect::<Vec<_>>();
        let engine_config = json!({
            "consumeFuel": true,
            "epochInterruption": true,
            "profilingStrategy": "perf-map",
            "wasmExceptions": true,
        });
        let engine_configuration_sha256 = sha256(&canonical_bytes(&engine_config));
        anyhow::ensure!(
            engine_configuration_sha256
                == "bebac200e042d7c2678b574ba75645f1bf06bb9295982d46184384b0376874b6",
            "source-keyed fixture engine configuration drifted"
        );
        let value_codec = guest_native_json_codec_identity_for_test();
        let request_envelope = capability_request_envelope_identity_for_test();
        let precompiler = source_keyed_capability_precompiler_material_identity();
        let artifact_pipeline_sha256 = "7".repeat(64);
        let producer_implementation = json!({
            "kind": CAPABILITY_PRODUCER_IMPLEMENTATION_IDENTITY_KIND,
            "sha256": "8".repeat(64),
        });
        let platform_limits = PlatformLimits::authoritative()?;
        let manifest = json!({
            "abi": {
                "capabilityRequestAbiVersion": CAPABILITY_REQUEST_ABI_VERSION,
                "entrySelectorAbiVersion": LEGACY_CAPABILITY_ENTRY_SELECTOR_ABI_VERSION,
                "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
            },
            "artifacts": {
                "coreWasm": {
                    "sha256": sha256(core_wasm),
                    "size": core_wasm.len(),
                },
                "serializedModule": {
                    "sha256": sha256(serialized_module),
                    "size": serialized_module.len(),
                },
            },
            "compiler": {
                "admittedLanguageVersion": 1,
                "compilerRevision": "source-keyed-fixture-compiler",
                "loweringPipelineSha256": value_codec.lowering_pipeline_sha256,
                "sourcePipelineSha256": entry.local_profile.sha256,
                "staticHermesGlobalPolicy": {
                    "inventorySha256": "9".repeat(64),
                    "kind": "convex-wasm-runtime-surface-policy-identity",
                    "runtimeSurfacePolicySha256": "a".repeat(64),
                },
                "staticHermesRevision": "source-keyed-fixture-static-hermes",
            },
            "engine": {
                "compatibilitySha256": GENERATED_ENGINE_COMPATIBILITY_SHA256,
                "config": engine_config,
                "configurationSha256": engine_configuration_sha256,
                "package": precompiler,
                "revision": GENERATED_WASMTIME_REVISION,
                "target": {
                    "cpu": "baseline",
                    "platform": {},
                    "triple": "x86_64-unknown-linux-gnu",
                },
            },
            "entry": entry,
            "entries": null,
            "execution": {
                "effectExecutionMode": "guest-promise-event-loop",
                "importedOperations": [],
                "limits": {
                    "executionFuel": 1,
                    "maxGuestMemoryBytes": 1024 * 1024,
                    "maxHostOwnedBytes": 1,
                    "maxOperationCount": 1,
                    "maxResultBytes": 1,
                    "maxValueHandles": 1,
                    "timeoutMilliseconds": 1,
                },
                "platformLimits": platform_limits,
                "requestEnvelope": request_envelope,
                "valueCodec": value_codec,
                "valueMode": "guest-native-json",
            },
            "kind": CAPABILITY_ENTRY_MANIFEST_KIND,
            "pipeline": {
                "artifactPipelineSha256": artifact_pipeline_sha256,
                "kind": "convex-wasm-artifact-pipeline-v8",
                "producerImplementation": producer_implementation,
                "runtimeSurfacePolicySha256": "a".repeat(64),
            },
            "routes": route_manifest,
            "runtime": {
                "archivesMaterialSha256": "b".repeat(64),
                "headersMaterialSha256": "c".repeat(64),
                "mainMaterialSha256": "d".repeat(64),
                "mainObject": {"sha256": "e".repeat(64), "size": 1},
            },
            "schemaVersion": CAPABILITY_ENTRY_MANIFEST_SCHEMA_VERSION,
            "toolchain": {
                "emscripten": {
                    "llvmRevision": "llvm-source-keyed-fixture",
                    "materialsSha256": "f".repeat(64),
                    "revision": "emscripten-source-keyed-fixture",
                },
                "staticHermes": {
                    "cBundle": null,
                    "flags": ["-O"],
                    "materialsSha256": "0".repeat(64),
                    "revision": "source-keyed-fixture-static-hermes",
                },
            },
        });
        let manifest_bytes = canonical_file_bytes(&manifest);
        let package_key = sha256(&canonical_bytes(&manifest));
        let provenance = json!({
            "artifactPipelineSha256": "7".repeat(64),
            "engineIdentity": {
                "engineCompatibilitySha256": GENERATED_ENGINE_COMPATIBILITY_SHA256,
                "engineConfig": engine_config,
                "kind": ENGINE_IDENTITY_KIND,
                "target": {
                    "cpu": "baseline",
                    "triple": "x86_64-unknown-linux-gnu",
                },
            },
            "identities": {
                "coreWasm": {},
                "runtimeMainObject": {},
                "selectorObject": {},
                "wasmtimeAot": {},
            },
            "kind": CAPABILITY_ENTRY_PROVENANCE_KIND_V1,
            "materials": {},
            "requestEnvelope": request_envelope,
            "semanticEnvironment": {},
            "valueCodec": value_codec,
        });
        let provenance_bytes = canonical_file_bytes(&provenance);
        let package_entry = json!({
            "artifacts": {
                "module.cwasm": {
                    "sha256": sha256(serialized_module),
                    "size": serialized_module.len(),
                },
                "module.wasm": {
                    "sha256": sha256(core_wasm),
                    "size": core_wasm.len(),
                },
            },
            "key": package_key,
            "kind": CAPABILITY_ENTRY_PACKAGE_KIND,
            "manifestSha256": package_key,
            "provenance": {
                "sha256": sha256(&provenance_bytes),
                "size": provenance_bytes.len(),
            },
        });
        let package_path = root.join("capability-entry-packages").join(&package_key);
        create_private_directory(&package_path)?;
        write_private(&package_path.join("module.wasm"), core_wasm)?;
        write_private(&package_path.join("module.cwasm"), serialized_module)?;
        write_private(&package_path.join("entry-manifest.json"), &manifest_bytes)?;
        write_private(
            &package_path.join("build-provenance.json"),
            &provenance_bytes,
        )?;
        write_private(
            &package_path.join("package-entry.json"),
            &canonical_file_bytes(&package_entry),
        )?;
        write_private(
            &package_path.join("COMPLETE"),
            format!("{package_key}\n").as_bytes(),
        )?;
        Ok((package_key, routes))
    }

    fn source_keyed_capability_deployment_export(
        package_key: &str,
        route: &RuntimeRegistrySourceKeyedCapabilityRoute,
        source_package_runtime_content_sha256: &str,
    ) -> anyhow::Result<JsonValue> {
        Ok(json!({
            "artifact": {
                "entryManifestSha256": package_key,
            },
            "compiler": {},
            "compilerLimits": PlatformLimits::authoritative()?,
            "dependencies": [],
            "dependencyGraphSha256": "b".repeat(64),
            "diagnosticCensusIds": [],
            "diagnostics": [],
            "entryPath": "convex/sourceKeyed.ts",
            "exportName": route.export_name,
            "packageReference": {
                "capabilityEntryPackageId": package_key,
                "entryId": route.entry_id,
                "entrySelectorId": route.entry_selector_id,
                "kind": CAPABILITY_ENTRY_ROUTE_REFERENCE_KIND,
                "routeId": route.route_id,
            },
            "routing": {
                "decision": "wasm",
                "reason": "runtimeCapability",
            },
            "runtimeModulePath": "sourceKeyed.js",
            "source": {
                "deployedRuntimeIdentity": {
                    "kind": DEPLOYED_RUNTIME_IDENTITY_KIND_V2,
                    "moduleSha256": DEPLOYED_MODULE_SHA256,
                    "sourcePackageRuntimeContentSha256":
                        source_package_runtime_content_sha256,
                },
                "exportName": route.export_name,
                "exportSha256": "e".repeat(64),
                "modulePath": "convex/sourceKeyed.ts",
                "resolvedGraphSha256": "b".repeat(64),
                "udfKind": "query",
            },
            "sourceCrossCheck": {},
            "udfKind": "query",
            "visibility": "public",
        }))
    }

    fn signed_source_keyed_capability_manifest(
        package_key: &str,
        routes: &[RuntimeRegistrySourceKeyedCapabilityRoute],
        source_package_runtime_content_sha256: &str,
    ) -> anyhow::Result<JsonValue> {
        let exports = routes
            .iter()
            .map(|route| {
                source_keyed_capability_deployment_export(
                    package_key,
                    route,
                    source_package_runtime_content_sha256,
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut manifest = signed_v5_capability_selection_manifest(exports);
        manifest["artifactPrecompiler"]["materialIdentity"] =
            serde_json::to_value(source_keyed_capability_precompiler_material_identity())?;
        sign_deployment_manifest(&mut manifest);
        Ok(manifest)
    }

    fn write_source_keyed_capability_generation(
        root: &Path,
        package_key: &str,
        manifest: &JsonValue,
        source_package_runtime_content_sha256: String,
        selector_count: usize,
    ) -> anyhow::Result<RuntimeRegistrySourceKeyedCapabilityGeneration> {
        let package_path = root.join("capability-entry-packages").join(package_key);
        let generation = write_runtime_registry_generation_files(
            root,
            manifest,
            json!({
                "capabilityEntryPackages": [{
                    "files": runtime_registry_capability_entry_package_files(&package_path)?,
                    "packageKey": package_key,
                }],
                "kind": RUNTIME_REGISTRY_GENERATION_KIND_V3,
            }),
        )?;
        let deployment_sha256 = manifest["deploymentSha256"]
            .as_str()
            .context("source-keyed fixture deployment digest missing")?
            .to_owned();
        let generation_sha256 = generation["generationSha256"]
            .as_str()
            .context("source-keyed fixture generation digest missing")?
            .to_owned();
        let generation_bytes = fs::read(
            root.join("generations")
                .join(&deployment_sha256)
                .join(&generation_sha256)
                .join("generation.json"),
        )?;
        Ok(RuntimeRegistrySourceKeyedCapabilityGeneration {
            source_package_runtime_content_sha256,
            deployment_sha256,
            generation_sha256,
            package_key: package_key.to_owned(),
            selector_count,
            generation_file_sha256: sha256(&generation_bytes),
            generation_file_size: u64::try_from(generation_bytes.len())
                .context("source-keyed fixture generation file is too large")?,
        })
    }

    fn read_source_keyed_generation_json(
        root: &Path,
        generation: &RuntimeRegistrySourceKeyedCapabilityGeneration,
    ) -> anyhow::Result<JsonValue> {
        let bytes = fs::read(
            root.join("generations")
                .join(&generation.deployment_sha256)
                .join(&generation.generation_sha256)
                .join("generation.json"),
        )?;
        serde_json::from_slice(&bytes).context("source-keyed fixture generation is malformed")
    }

    fn write_runtime_registry_source_catalog_fixture(
        root: &Path,
        generations: &[RuntimeRegistrySourceKeyedCapabilityGeneration],
    ) -> anyhow::Result<()> {
        let mut entries = generations
            .iter()
            .map(|generation| {
                json!({
                    "deploymentSha256": generation.deployment_sha256,
                    "generation": {
                        "sha256": generation.generation_file_sha256,
                        "size": generation.generation_file_size,
                    },
                    "generationSha256": generation.generation_sha256,
                    "sourcePackageRuntimeContentSha256":
                        generation.source_package_runtime_content_sha256,
                })
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left["sourcePackageRuntimeContentSha256"]
                .as_str()
                .cmp(&right["sourcePackageRuntimeContentSha256"].as_str())
        });
        let mut catalog = json!({
            "entries": entries,
            "kind": RUNTIME_REGISTRY_SOURCE_CATALOG_KIND_V2,
        });
        let catalog_sha256 = sha256(&canonical_bytes(&catalog));
        catalog
            .as_object_mut()
            .context("source catalog fixture must be an object")?
            .insert(
                "catalogSha256".to_owned(),
                JsonValue::String(catalog_sha256.clone()),
            );

        let catalog_path = root.join(RUNTIME_REGISTRY_SOURCE_CATALOG_FILE);
        let catalog_staged_path = root.join("source-catalog.json.next");
        write_private(&catalog_staged_path, &canonical_file_bytes(&catalog))?;
        fs::rename(catalog_staged_path, catalog_path)?;
        Ok(())
    }

    fn deployment_export(fixture: &PackageFixture) -> JsonValue {
        json!({
            "artifact": {
                "executionManifest": fixture.manifest,
            },
            "compiler": {},
            "compilerLimits": fixture.manifest["platformLimits"],
            "dependencies": [],
            "dependencyGraphSha256":
                fixture.manifest["source"]["resolvedGraphSha256"],
            "diagnosticCensusIds": [],
            "diagnostics": [],
            "entryPath": fixture.manifest["source"]["modulePath"],
            "exportName": fixture.manifest["source"]["exportName"],
            "packageReference": {
                "cacheKey": fixture.package_key(),
                "kind": PACKAGE_ENTRY_KIND,
            },
            "routing": {
                "decision": "wasm",
                "reason": "staticEligibility",
            },
            "runtimeModulePath": "example.js",
            "source": {
                "deployedRuntimeIdentity": {
                    "kind": DEPLOYED_RUNTIME_IDENTITY_KIND_V1,
                    "moduleSha256": DEPLOYED_MODULE_SHA256,
                    "sourcePackageSha256": DEPLOYED_SOURCE_PACKAGE_SHA256,
                },
                "exportName": fixture.manifest["source"]["exportName"],
                "exportSha256": fixture.manifest["source"]["exportSha256"],
                "modulePath": fixture.manifest["source"]["modulePath"],
                "resolvedGraphSha256": fixture.manifest["source"]["resolvedGraphSha256"],
                "udfKind": fixture.manifest["source"]["udfKind"],
            },
            "sourceCrossCheck": {},
            "udfKind": fixture.manifest["source"]["udfKind"],
            "visibility": "public",
        })
    }

    fn capability_entry_deployment_export(fixture: &PackageFixture) -> JsonValue {
        let mut export = deployment_export(fixture);
        export["artifact"] = json!({
            "entryManifestSha256":
                "3333333333333333333333333333333333333333333333333333333333333333",
        });
        export["routing"]["reason"] = json!("runtimeCapability");
        export["packageReference"] = json!({
            "capabilityEntryPackageId":
                "3333333333333333333333333333333333333333333333333333333333333333",
            "entryId":
                "4444444444444444444444444444444444444444444444444444444444444444",
            "entrySelectorId": "5555555555555555",
            "kind": CAPABILITY_ENTRY_ROUTE_REFERENCE_KIND,
            "routeId":
                "6666666666666666666666666666666666666666666666666666666666666666",
        });
        export
    }

    fn not_analyzed_v5_deployment_export(fixture: &PackageFixture) -> JsonValue {
        let mut export = deployment_export(fixture);
        export["artifact"] = JsonValue::Null;
        export["compiler"] = json!({ "kind": "not-analyzed" });
        export["dependencies"] = json!({});
        export["entryPath"] = json!("convex/notAnalyzed.ts");
        export["exportName"] = json!("notAnalyzed");
        export["packageReference"] = JsonValue::Null;
        export["routing"] = json!({
            "decision": "existingRuntime",
            "reason": "not-analyzed",
        });
        export["runtimeModulePath"] = json!("notAnalyzed.js");
        export["source"] = json!({
            "exportName": "notAnalyzed",
            "modulePath": "convex/notAnalyzed.ts",
            "udfKind": "query",
        });
        export["sourceCrossCheck"] = JsonValue::Null;
        let export_object = export
            .as_object_mut()
            .expect("deployment export fixture must be an object");
        export_object.remove("compilerLimits");
        export_object.remove("dependencyGraphSha256");
        export
    }

    fn not_selected_v5_deployment_export(fixture: &PackageFixture) -> JsonValue {
        let mut export = capability_entry_deployment_export(fixture);
        export["artifact"] = JsonValue::Null;
        export["packageReference"] = JsonValue::Null;
        export
            .as_object_mut()
            .expect("deployment export fixture must be an object")
            .remove("compilerLimits");
        export["routing"] = json!({
            "decision": "existingRuntime",
            "reason": "not-selected",
        });
        remove_deployed_runtime_binding(&mut export);
        export
    }

    fn not_analyzed_not_selected_v5_deployment_export(fixture: &PackageFixture) -> JsonValue {
        let mut export = not_selected_v5_deployment_export(fixture);
        export["compiler"] = json!({ "kind": "not-analyzed" });
        export["exportName"] = json!("notSelectedCompanion");
        export["source"] = json!({
            "exportName": "notSelectedCompanion",
            "modulePath": fixture.manifest["source"]["modulePath"],
            "udfKind": fixture.manifest["source"]["udfKind"],
        });
        export
    }

    fn remove_deployed_runtime_binding(export: &mut JsonValue) {
        let source = export["source"]
            .as_object_mut()
            .expect("deployment export source fixture must be an object");
        source.remove("deployedRuntimeIdentity");
        source.remove("deployedRuntimeIdentityDiagnostic");
    }

    fn signed_deployment_manifest(exports: Vec<JsonValue>) -> JsonValue {
        let selected_wasm = exports
            .iter()
            .filter(|export| export["routing"]["decision"] == "wasm")
            .count();
        let unselected_eligible = exports
            .iter()
            .filter(|export| export["routing"]["decision"] == "existingRuntime")
            .count();
        let ineligible = exports
            .iter()
            .filter(|export| export["routing"]["decision"] == "v8Fallback")
            .count();
        let eligible = selected_wasm + unselected_eligible;
        let mutations = exports
            .iter()
            .filter(|export| export["udfKind"] == "mutation")
            .count();
        let queries = exports.len() - mutations;
        let compile_selection_exports = exports
            .iter()
            .filter(|export| export["routing"]["decision"] == "wasm")
            .map(|export| {
                let runtime_module_path = export["runtimeModulePath"]
                    .as_str()
                    .expect("fixture runtime module path");
                json!({
                    "exportName": export["exportName"],
                    "modulePath": runtime_module_path
                        .strip_suffix(".js")
                        .expect("fixture runtime module path suffix"),
                })
            })
            .collect::<Vec<_>>();
        let mut manifest = json!({
            "artifactPrecompiler": {
                "materialIdentity": test_precompiler_material_identity(),
            },
            "compileSelection": {
                "exports": compile_selection_exports,
                "kind": COMPILE_SELECTION_KIND,
            },
            "compiler": {},
            "counts": {
                "actionsOnExistingRuntime": 0,
                "eligible": eligible,
                "ineligible": ineligible,
                "mutations": mutations,
                "queries": queries,
                "selectedWasm": selected_wasm,
                "total": exports.len(),
                "unselectedEligible": unselected_eligible,
            },
            "diagnosticCensus": [],
            "existingRuntimeActions": [],
            "exports": exports,
            "graph": {},
            "inventoryAuthority": {},
            "kind": DEPLOYMENT_MANIFEST_KIND_V2,
            "mode": "compile",
            "policy": {},
            "sourceInventory": {},
        });
        sign_deployment_manifest(&mut manifest);
        manifest
    }

    fn signed_v5_capability_selection_manifest(exports: Vec<JsonValue>) -> JsonValue {
        let mut manifest = signed_deployment_manifest(exports);
        let exports = manifest["exports"]
            .as_array()
            .expect("deployment exports fixture must be an array");
        let selected_exports = exports
            .iter()
            .filter(|export| export["routing"]["decision"] == "wasm")
            .map(|export| {
                json!({
                    "entryPath": export["entryPath"],
                    "exportName": export["exportName"],
                })
            })
            .collect::<Vec<_>>();
        let selected_wasm = selected_exports.len();
        let unselected_eligible = exports
            .iter()
            .filter(|export| {
                export["routing"]
                    == json!({
                        "decision": "existingRuntime",
                        "reason": "not-selected",
                    })
            })
            .count();
        let artifact_fallback = exports
            .iter()
            .filter(|export| {
                export["routing"]
                    == json!({
                        "decision": "v8Fallback",
                        "reason": "static-hermes-source-incompatibility-v1",
                    })
            })
            .count();
        let total = exports.len();
        manifest["kind"] = json!(DEPLOYMENT_MANIFEST_KIND_V5);
        manifest["effectExecutionMode"] = json!("guest-promise-event-loop");
        manifest["contextReusePolicyIdentity"] = json!({
            "kind": CONTEXT_REUSE_POLICY_IDENTITY_KIND,
            "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        });
        manifest["counts"]["artifactFallback"] = json!(artifact_fallback);
        manifest["counts"]["eligible"] =
            json!(selected_wasm + unselected_eligible + artifact_fallback);
        manifest["counts"]["ineligible"] =
            json!(total - selected_wasm - unselected_eligible - artifact_fallback);
        manifest["counts"]["selectedWasm"] = json!(selected_wasm);
        manifest["counts"]["unselectedEligible"] = json!(unselected_eligible);
        manifest["compileSelection"] = json!({
            "exports": selected_exports,
            "kind": CAPABILITY_ENTRY_COMPILE_SELECTION_KIND,
        });
        sign_deployment_manifest(&mut manifest);
        manifest
    }

    fn signed_v6_module_graph_manifest(fixture: &PackageFixture) -> anyhow::Result<JsonValue> {
        let cohort_id = "2".repeat(64);
        let schedule_sha256 = "3".repeat(64);
        let source_envelope_sha256 = "4".repeat(64);
        let contract =
            module_graph_cohort_contract(&cohort_id, &schedule_sha256, &source_envelope_sha256)?;
        let contract_sha256 = contract["cohortContractSha256"]
            .as_str()
            .context("cohort contract identity")?;
        let entry = &contract["entries"][0];
        let route = &contract["routes"][0];
        let dependency_graph_sha256 = entry["localProfile"]["dependencyGraphSha256"].clone();

        let mut export = capability_entry_deployment_export(fixture);
        export["artifact"] = json!({"cohortContractSha256": contract_sha256});
        export["compiler"] = contract["compiler"].clone();
        export["compilerLimits"] = contract["execution"]["platformLimits"].clone();
        export["dependencyGraphSha256"] = dependency_graph_sha256.clone();
        export["entryPath"] = entry["entryPath"].clone();
        export["exportName"] = route["exportName"].clone();
        export["packageReference"] = json!({
            "cohortContractSha256": contract_sha256,
            "entryId": route["entryId"],
            "entrySelectorId": route["entrySelectorId"],
            "kind": MODULE_GRAPH_ROUTE_REFERENCE_KIND,
            "routeId": route["routeId"],
        });
        export["runtimeModulePath"] = json!(format!(
            "{}.js",
            entry["modulePath"]
                .as_str()
                .context("cohort entry module path")?
        ));
        export["source"]["exportName"] = route["exportName"].clone();
        export["source"]["modulePath"] = entry["entryPath"].clone();
        export["source"]["resolvedGraphSha256"] = dependency_graph_sha256;

        let mut binding = json!({
            "cohorts": [{
                "cohortContractSha256": contract_sha256,
                "cohortId": cohort_id,
                "graphManifestSha256": "e".repeat(64),
                "routeIds": [route["routeId"].clone()],
            }],
            "kind": "convex-wasm-deployment-module-graph-binding-v1",
            "scheduleSha256": schedule_sha256,
            "schemaVersion": 1,
            "sourceEnvelopeSha256": source_envelope_sha256,
        });
        let binding_sha256 = sha256(&canonical_bytes(&binding));
        binding
            .as_object_mut()
            .context("module graph binding")?
            .insert("bindingSha256".into(), json!(binding_sha256));

        let mut manifest = signed_v5_capability_selection_manifest(vec![export]);
        manifest["artifactPrecompiler"]["materialIdentity"] =
            contract["precompilerMaterialIdentity"].clone();
        manifest["kind"] = json!(DEPLOYMENT_MANIFEST_KIND_V6);
        manifest["moduleGraphBinding"] = binding;
        manifest["moduleGraphCohorts"] = json!([contract]);
        sign_deployment_manifest(&mut manifest);
        Ok(manifest)
    }

    fn signed_v4_artifact_fallback_manifest(fixture: &PackageFixture) -> JsonValue {
        let mut export = deployment_export(fixture);
        export["artifact"] = JsonValue::Null;
        export["packageReference"] = JsonValue::Null;
        export["routing"] = json!({
            "decision": "v8Fallback",
            "reason": "static-hermes-source-incompatibility-v1",
        });
        remove_deployed_runtime_binding(&mut export);

        let mut manifest = signed_deployment_manifest(vec![export]);
        manifest["kind"] = json!(DEPLOYMENT_MANIFEST_KIND_V4);
        manifest["compileSelection"] = json!({
            "kind": ALL_ELIGIBLE_COMPILE_SELECTION_KIND,
        });
        manifest["counts"]["artifactFallback"] = json!(1);
        manifest["counts"]["eligible"] = json!(1);
        manifest["counts"]["ineligible"] = json!(0);
        sign_deployment_manifest(&mut manifest);
        manifest
    }

    fn sign_deployment_manifest(manifest: &mut JsonValue) {
        manifest
            .as_object_mut()
            .expect("deployment manifest object")
            .remove("deploymentSha256");
        let digest = sha256(&canonical_bytes(&manifest));
        manifest
            .as_object_mut()
            .expect("deployment manifest object")
            .insert("deploymentSha256".to_owned(), JsonValue::String(digest));
    }

    #[test]
    fn deployment_v6_authenticates_graph_only_cohort_routes() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_v6_module_graph_manifest(&fixture)?;
        let registry = ValidatedDeploymentManifest::parse(&canonical_file_bytes(&manifest))?;
        assert!(registry.package_keys().is_empty());

        let contract = registry
            .module_graph_cohorts
            .first()
            .context("deployment-v6 cohort contract")?;
        let runtime = RuntimeCompatibility {
            opaque_value_abi_version: OPAQUE_VALUE_ABI_VERSION,
            platform_limits: PlatformLimits {
                execution_time_ms: 1,
                argument_bytes: 1,
                result_bytes: 1,
                documents_read: 1,
                read_bytes: 1,
                documents_written: 1,
                write_bytes: 1,
                scheduled_functions: 1,
                scheduled_argument_bytes: 1,
            },
            wasmtime_revision: &contract.engine.revision,
            target_triple: &contract.engine.target.triple,
            target_cpu: &contract.engine.target.cpu,
            engine_configuration_sha256: &contract.engine.configuration_sha256,
            engine_compatibility_sha256: &contract.engine.compatibility_sha256,
        };
        let material = registry.authenticated_module_graph_route_material(
            "example.js",
            "lookup",
            UdfKind::Query,
            &runtime,
        )?;
        assert_eq!(material.package_key, contract.cohort_contract_sha256);
        assert!(matches!(
            material.identity,
            ValidatedWasmUdfPackageIdentity::ModuleGraphCohort(_)
        ));

        let mut stale_runtime_surface_policy = manifest.clone();
        stale_runtime_surface_policy["moduleGraphCohorts"][0]["compiler"]
            ["staticHermesGlobalPolicy"]["runtimeSurfacePolicySha256"] =
            json!("9aa82597aab7c351ded460ff792ef09b983c2566c443f3b16473a9f8cf9a99a4");
        stale_runtime_surface_policy["moduleGraphCohorts"][0]["runtimeSurfacePolicySha256"] =
            json!("9aa82597aab7c351ded460ff792ef09b983c2566c443f3b16473a9f8cf9a99a4");
        sign_deployment_manifest(&mut stale_runtime_surface_policy);
        assert!(ValidatedDeploymentManifest::parse(&canonical_file_bytes(
            &stale_runtime_surface_policy
        ))
        .is_err());

        let mut stale_runtime_surface_inventory = manifest.clone();
        stale_runtime_surface_inventory["moduleGraphCohorts"][0]["compiler"]
            ["staticHermesGlobalPolicy"]["inventorySha256"] = json!("8".repeat(64));
        sign_deployment_manifest(&mut stale_runtime_surface_inventory);
        assert!(ValidatedDeploymentManifest::parse(&canonical_file_bytes(
            &stale_runtime_surface_inventory
        ))
        .is_err());

        let mut changed_contract = manifest.clone();
        changed_contract["moduleGraphCohorts"][0]["compiler"]["compilerRevision"] =
            json!("changed-compiler-revision");
        sign_deployment_manifest(&mut changed_contract);
        assert!(
            ValidatedDeploymentManifest::parse(&canonical_file_bytes(&changed_contract)).is_err()
        );

        let mut laundered_context_authority = manifest.clone();
        laundered_context_authority["moduleGraphBinding"]["kind"] =
            json!("convex-wasm-deployment-module-graph-binding-v2");
        laundered_context_authority["moduleGraphBinding"]["schemaVersion"] = json!(2);
        laundered_context_authority["moduleGraphBinding"]["contextReuseAnalysis"] = json!({
            "entries": ["convex/example.ts"],
            "kind": CONTEXT_REUSE_ANALYSIS_KIND,
            "policyFingerprint": "9".repeat(64),
            "resultSha256": "8".repeat(64),
        });
        laundered_context_authority["moduleGraphBinding"]
            .as_object_mut()
            .context("module graph binding")?
            .remove("bindingSha256");
        let binding_sha256 = sha256(&canonical_bytes(
            &laundered_context_authority["moduleGraphBinding"],
        ));
        laundered_context_authority["moduleGraphBinding"]["bindingSha256"] = json!(binding_sha256);
        sign_deployment_manifest(&mut laundered_context_authority);
        assert!(ValidatedDeploymentManifest::parse(&canonical_file_bytes(
            &laundered_context_authority
        ))
        .is_err());

        let mut legacy_route_reference = manifest;
        legacy_route_reference["exports"][0]["packageReference"]["kind"] =
            json!(CAPABILITY_ENTRY_ROUTE_REFERENCE_KIND);
        sign_deployment_manifest(&mut legacy_route_reference);
        assert!(
            ValidatedDeploymentManifest::parse(&canonical_file_bytes(&legacy_route_reference))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn context_reuse_cohorts_require_one_shared_analysis_authority() -> anyhow::Result<()> {
        let policy_fingerprint = "9".repeat(64);
        let application_analysis = serde_json::from_value::<ContextReuseAnalysisIdentity>(json!({
            "entries": ["convex/alpha.ts", "convex/beta.ts"],
            "kind": CONTEXT_REUSE_ANALYSIS_KIND,
            "policyFingerprint": policy_fingerprint,
            "resultSha256": "8".repeat(64),
        }))?;
        let cohort_analysis = |entry_path: &str,
                               entry_graph_sha256: &str,
                               shared_analysis_sha256: &str|
         -> anyhow::Result<ContextReuseCohortAnalysisIdentity> {
            let mut value = json!({
                "entries": [entry_path],
                "entryGraphSha256s": [entry_graph_sha256],
                "kind": CONTEXT_REUSE_COHORT_ANALYSIS_KIND,
                    "policyFingerprint": application_analysis.policy_fingerprint.as_str(),
                "sharedAnalysisSha256": shared_analysis_sha256,
                "thirdPartyMaterialFingerprints": {},
            });
            let result_sha256 = sha256(&canonical_bytes(&value));
            value
                .as_object_mut()
                .context("cohort analysis object")?
                .insert("resultSha256".into(), json!(result_sha256));
            Ok(serde_json::from_value(value)?)
        };
        let alpha = cohort_analysis("convex/alpha.ts", &"a".repeat(64), &"7".repeat(64))?;
        let beta = cohort_analysis("convex/beta.ts", &"b".repeat(64), &"7".repeat(64))?;
        validate_context_reuse_cohort_analysis_set(&application_analysis, &[&alpha, &beta])?;

        let different_shared = cohort_analysis("convex/beta.ts", &"b".repeat(64), &"6".repeat(64))?;
        assert!(validate_context_reuse_cohort_analysis_set(
            &application_analysis,
            &[&alpha, &different_shared],
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn context_reuse_cohort_identity_uses_javascript_string_order() -> anyhow::Result<()> {
        let identity = serde_json::from_value::<ContextReuseCohortAnalysisIdentity>(json!({
            "entries": ["convex/\u{10000}.ts", "convex/\u{e000}.ts"],
            "entryGraphSha256s": ["a".repeat(64), "b".repeat(64)],
            "kind": CONTEXT_REUSE_COHORT_ANALYSIS_KIND,
            "policyFingerprint": "9".repeat(64),
            "resultSha256": "e7b29af9545941fabe8ceffbe7eabf44d4f9c8b4e04d4b1edb397fea2cf2ac7d",
            "sharedAnalysisSha256": "7".repeat(64),
            "thirdPartyMaterialFingerprints": {
                "node_modules/a.js": "c".repeat(64),
                "node_modules/b.js": "d".repeat(64),
            },
        }))?;
        identity.validate()?;
        Ok(())
    }

    fn write_deployment_manifest(
        fixture: &PackageFixture,
        manifest: &JsonValue,
    ) -> anyhow::Result<PathBuf> {
        let path = fixture.cache_root().join("deployment-manifest.json");
        write_private(&path, &serde_json::to_vec_pretty(manifest)?)?;
        Ok(path)
    }

    fn create_private_directory(path: &Path) -> anyhow::Result<()> {
        fs::create_dir(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        Ok(())
    }

    fn runtime_registry_package_files(package_path: &Path) -> anyhow::Result<Vec<JsonValue>> {
        PACKAGE_FILES
            .iter()
            .map(|name| {
                let bytes = fs::read(package_path.join(name))?;
                Ok(json!({
                    "name": name,
                    "sha256": sha256(&bytes),
                    "size": bytes.len(),
                }))
            })
            .collect()
    }

    fn runtime_registry_capability_entry_package_files(
        package_path: &Path,
    ) -> anyhow::Result<Vec<JsonValue>> {
        CAPABILITY_ENTRY_PACKAGE_FILES
            .iter()
            .map(|name| {
                let bytes = fs::read(package_path.join(name))?;
                Ok(json!({
                    "name": name,
                    "sha256": sha256(&bytes),
                    "size": bytes.len(),
                }))
            })
            .collect()
    }

    fn write_runtime_registry_generation(
        root: &Path,
        fixture: &PackageFixture,
        manifest: &JsonValue,
    ) -> anyhow::Result<JsonValue> {
        write_runtime_registry_generation_files(
            root,
            manifest,
            json!({
                "kind": RUNTIME_REGISTRY_GENERATION_KIND_V1,
                "packages": [{
                    "files": runtime_registry_package_files(&fixture.path)?,
                    "packageKey": fixture.package_key(),
                }],
            }),
        )
    }

    fn write_runtime_registry_generation_files(
        root: &Path,
        manifest: &JsonValue,
        mut generation: JsonValue,
    ) -> anyhow::Result<JsonValue> {
        let deployment_sha256 = manifest["deploymentSha256"]
            .as_str()
            .context("deployment fixture digest missing")?;
        let deployment_bytes = canonical_file_bytes(manifest);

        generation
            .as_object_mut()
            .context("generation fixture object")?
            .insert(
                "deploymentManifest".to_owned(),
                json!({
                "deploymentSha256": deployment_sha256,
                "sha256": sha256(&deployment_bytes),
                "size": deployment_bytes.len(),
                }),
            );
        let generation_sha256 = sha256(&canonical_bytes(&generation));
        generation
            .as_object_mut()
            .context("generation fixture object")?
            .insert(
                "generationSha256".to_owned(),
                JsonValue::String(generation_sha256.clone()),
            );
        let deployment_root = root.join("generations").join(deployment_sha256);
        match fs::symlink_metadata(&deployment_root) {
            Ok(_) => validate_runtime_registry_directory(&deployment_root)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_private_directory(&deployment_root)?;
            },
            Err(error) => return Err(error.into()),
        }
        let generation_root = deployment_root.join(&generation_sha256);
        create_private_directory(&generation_root)?;
        write_private(&generation_root.join("deployment.json"), &deployment_bytes)?;
        write_private(
            &generation_root.join("generation.json"),
            &canonical_file_bytes(&generation),
        )?;
        write_private(
            &generation_root.join("COMPLETE"),
            format!("{generation_sha256}\n").as_bytes(),
        )?;
        Ok(generation)
    }

    fn write_runtime_registry_current(root: &Path, generation: &JsonValue) -> anyhow::Result<()> {
        let deployment_sha256 = generation["deploymentManifest"]["deploymentSha256"]
            .as_str()
            .context("generation fixture deployment digest missing")?;
        let generation_sha256 = generation["generationSha256"]
            .as_str()
            .context("generation fixture digest missing")?;
        let generation_bytes = fs::read(
            root.join("generations")
                .join(deployment_sha256)
                .join(generation_sha256)
                .join("generation.json"),
        )?;
        let without_identity = json!({
            "deploymentSha256": deployment_sha256,
            "generation": {
                "sha256": sha256(&generation_bytes),
                "size": generation_bytes.len(),
            },
            "generationSha256": generation_sha256,
            "kind": RUNTIME_REGISTRY_CURRENT_KIND,
        });
        let current_sha256 = sha256(&canonical_bytes(&without_identity));
        let mut current = without_identity;
        current
            .as_object_mut()
            .context("current fixture object")?
            .insert(
                "currentSha256".to_owned(),
                JsonValue::String(current_sha256),
            );
        write_private(&root.join("current"), &canonical_file_bytes(&current))
    }

    fn create_runtime_registry(
        fixture: &PackageFixture,
        manifest: &JsonValue,
    ) -> anyhow::Result<PathBuf> {
        let root = fixture.cache_root().join("runtime-registry");
        create_private_directory(&root)?;
        create_private_directory(&root.join("generations"))?;
        create_private_directory(&root.join("packages"))?;
        let package_root = root.join("packages").join(fixture.package_key());
        create_private_directory(&package_root)?;
        for name in PACKAGE_FILES {
            fs::hard_link(fixture.path.join(name), package_root.join(name))?;
        }
        let generation = write_runtime_registry_generation(&root, fixture, manifest)?;
        write_runtime_registry_current(&root, &generation)?;
        Ok(root)
    }

    #[test]
    fn module_graph_generation_root_is_graph_only_and_does_not_open_legacy_packages(
    ) -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))?;
        for name in ["generations", "module-graph-cache", "packages"] {
            create_private_directory(&root.path().join(name))?;
        }
        let mut current = json!({
            "deploymentSha256": "a".repeat(64),
            "generation": {"sha256": "b".repeat(64), "size": 1},
            "generationSha256": "c".repeat(64),
            "kind": RUNTIME_REGISTRY_CURRENT_KIND,
        });
        let current_sha256 = sha256(&canonical_bytes(&current));
        current
            .as_object_mut()
            .context("current manifest")?
            .insert("currentSha256".into(), json!(current_sha256));
        write_private(
            &root.path().join("current"),
            &canonical_file_bytes(&current),
        )?;

        load_runtime_registry_current(root.path())?;
        validate_module_graph_runtime_registry_root(root.path())?;

        let legacy_root = root.path().join("capability-entry-packages");
        create_private_directory(&legacy_root)?;
        fs::set_permissions(&legacy_root, fs::Permissions::from_mode(0o000))?;
        load_runtime_registry_current(root.path())?;
        assert!(validate_module_graph_runtime_registry_root(root.path()).is_err());
        fs::set_permissions(&legacy_root, fs::Permissions::from_mode(0o700))?;
        Ok(())
    }

    #[test]
    fn loads_authenticated_runtime_registry_generation() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        let root = create_runtime_registry(&fixture, &manifest)?;

        let current = load_runtime_registry_current(&root)?;
        let generation = load_runtime_registry_generation(&root, &current)?;
        assert!(root
            .join("generations")
            .join(current.deployment_sha256.as_str())
            .join(current.generation_sha256.as_str())
            .is_dir());
        assert_eq!(
            generation.registry().deployment_sha256(),
            manifest["deploymentSha256"]
                .as_str()
                .context("deployment fixture digest missing")?
        );
        assert_eq!(
            generation.packages_root().join(fixture.package_key()),
            root.join("packages").join(fixture.package_key())
        );
        let package = generation
            .registry()
            .load_export_package_from_packages_root(
                generation.packages_root(),
                "example.js",
                "run",
                UdfKind::Query,
                &compatibility(),
                fixture.cache_root(),
            )?;
        assert!(matches!(package, DeploymentExportPackage::Wasm(_)));
        Ok(())
    }

    #[test]
    fn loads_flat_runtime_registry_generation_for_compatibility() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        let root = create_runtime_registry(&fixture, &manifest)?;
        let current = load_runtime_registry_current(&root)?;
        let deployment_root = root.join("generations").join(&current.deployment_sha256);
        let keyed_generation_root = deployment_root.join(&current.generation_sha256);
        let staged_flat_generation_root = root.join("generations").join("staged-flat-generation");

        fs::rename(&keyed_generation_root, &staged_flat_generation_root)?;
        fs::remove_dir(&deployment_root)?;
        fs::rename(&staged_flat_generation_root, &deployment_root)?;

        let generation = load_runtime_registry_generation(&root, &current)?;
        assert_eq!(generation.generation_sha256(), current.generation_sha256);
        Ok(())
    }

    #[test]
    fn rejects_flat_module_graph_runtime_registry_generations() -> anyhow::Result<()> {
        for generation_kind in [
            RUNTIME_REGISTRY_GENERATION_KIND_V5,
            RUNTIME_REGISTRY_GENERATION_KIND_V6_SHADOW_ONLY,
            RUNTIME_REGISTRY_GENERATION_KIND_V9,
            RUNTIME_REGISTRY_GENERATION_KIND_V10_SHADOW_ONLY,
        ] {
            let fixture = PackageFixture::create()?;
            let manifest = signed_deployment_manifest(vec![deployment_export(&fixture)]);
            let root = fixture.cache_root().join(format!("flat-{generation_kind}"));
            create_private_directory(&root)?;
            create_private_directory(&root.join("generations"))?;
            create_private_directory(&root.join("packages"))?;
            let generation = write_runtime_registry_generation_files(
                &root,
                &manifest,
                json!({"kind": generation_kind}),
            )?;
            write_runtime_registry_current(&root, &generation)?;

            let deployment_sha256 = generation["deploymentManifest"]["deploymentSha256"]
                .as_str()
                .context("flat module-graph fixture deployment digest missing")?;
            let deployment_root = root.join("generations").join(deployment_sha256);
            let generation_sha256 = generation["generationSha256"]
                .as_str()
                .context("flat module-graph fixture generation digest missing")?;
            let keyed_generation_root = deployment_root.join(generation_sha256);
            let staged_flat_generation_root =
                root.join("generations").join("staged-flat-generation");
            fs::rename(&keyed_generation_root, &staged_flat_generation_root)?;
            fs::remove_dir(&deployment_root)?;
            fs::rename(&staged_flat_generation_root, &deployment_root)?;

            let current = load_runtime_registry_current(&root)?;
            assert!(matches!(
                load_runtime_registry_generation(&root, &current),
                Err(WasmUdfPackageError::InvalidRuntimeRegistry(message))
                    if message
                        == "module-graph generations require immutable generation-keyed storage"
            ));
        }
        Ok(())
    }

    #[test]
    fn loads_empty_cohort_registry_for_all_fallback_v4_deployment() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_v4_artifact_fallback_manifest(&fixture);
        let root = fixture.cache_root().join("empty-cohort-runtime-registry");
        create_private_directory(&root)?;
        create_private_directory(&root.join("cohort-packages"))?;
        create_private_directory(&root.join("generations"))?;
        create_private_directory(&root.join("packages"))?;
        let generation = write_runtime_registry_generation_files(
            &root,
            &manifest,
            json!({
                "cohortPackages": [],
                "kind": RUNTIME_REGISTRY_GENERATION_KIND_V2,
            }),
        )?;
        write_runtime_registry_current(&root, &generation)?;

        let current = load_runtime_registry_current(&root)?;
        let generation = load_runtime_registry_generation(&root, &current)?;
        assert!(generation.registry().package_keys().is_empty());
        assert_eq!(generation.packages_root(), root.join("cohort-packages"),);
        Ok(())
    }

    #[test]
    #[ignore = "requires an externally published runtime registry"]
    fn loads_externally_published_runtime_registry_generation() -> anyhow::Result<()> {
        let root = std::env::var_os(REAL_RUNTIME_REGISTRY_ROOT_ENV)
            .map(PathBuf::from)
            .with_context(|| {
                format!("{REAL_RUNTIME_REGISTRY_ROOT_ENV} must identify the runtime registry root")
            })?;
        let current = load_runtime_registry_current(&root)?;
        let generation = load_runtime_registry_generation(&root, &current)?;
        assert_eq!(current.current_sha256(), generation.current_sha256());
        assert!(!generation.generation_sha256().is_empty());
        let has_legacy_packages = !generation.registry().package_keys().is_empty();
        let has_module_graph_catalog = generation.into_parts().5.is_some();
        assert!(has_legacy_packages || has_module_graph_catalog);
        Ok(())
    }

    #[test]
    #[ignore = "requires an externally published deployment manifest"]
    fn loads_externally_published_deployment_manifest() -> anyhow::Result<()> {
        let path = std::env::var_os(REAL_DEPLOYMENT_MANIFEST_ENV)
            .map(PathBuf::from)
            .with_context(|| {
                format!("{REAL_DEPLOYMENT_MANIFEST_ENV} must identify the deployment manifest")
            })?;
        let deployment = ValidatedDeploymentManifest::load(&path)?;
        assert!(!deployment.package_keys().is_empty());
        Ok(())
    }

    #[test]
    fn rejects_runtime_registry_package_tampering_before_activation() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        let root = create_runtime_registry(&fixture, &manifest)?;
        let current = load_runtime_registry_current(&root)?;
        write_private(
            &root
                .join("packages")
                .join(fixture.package_key())
                .join("module.wasm"),
            b"tampered-module",
        )?;

        assert!(matches!(
            load_runtime_registry_generation(&root, &current),
            Err(WasmUdfPackageError::InvalidRuntimeRegistry(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_noncanonical_runtime_registry_current_pointer() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        let root = create_runtime_registry(&fixture, &manifest)?;
        let current: JsonValue = serde_json::from_slice(&fs::read(root.join("current"))?)?;
        write_private(
            &root.join("current"),
            format!("{}\n", serde_json::to_string_pretty(&current)?).as_bytes(),
        )?;

        assert!(matches!(
            load_runtime_registry_current(&root),
            Err(WasmUdfPackageError::InvalidRuntimeRegistry(_))
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_runtime_registry_special_permission_bits() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        let root = create_runtime_registry(&fixture, &manifest)?;

        fs::set_permissions(root.join("current"), fs::Permissions::from_mode(0o4600))?;
        assert!(matches!(
            load_runtime_registry_current(&root),
            Err(WasmUdfPackageError::InvalidRuntimeRegistry(_))
        ));

        fs::set_permissions(root.join("current"), fs::Permissions::from_mode(0o600))?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o1700))?;
        assert!(matches!(
            load_runtime_registry_current(&root),
            Err(WasmUdfPackageError::InvalidRuntimeRegistry(_))
        ));
        Ok(())
    }

    #[test]
    fn resolves_authenticated_deployment_export_to_its_package() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        let registry = ValidatedDeploymentManifest::load(&manifest_path)?;
        assert_eq!(
            registry.deployment_sha256(),
            manifest["deploymentSha256"]
                .as_str()
                .context("deployment digest missing")?
        );
        assert!(matches!(
            registry.export_routing("example.js", "run", UdfKind::Query)?,
            DeploymentExportRouting::Wasm { package_key, .. }
                if package_key == fixture.package_key()
        ));
        assert!(matches!(
            registry.export_routing("convex/example.ts", "run", UdfKind::Query),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        assert!(matches!(
            registry.export_routing("example.js", "run", UdfKind::Mutation),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        let deployed_runtime_identity = registry
            .deployed_runtime_identity("example.js", "run", UdfKind::Query)?
            .context("bound Wasm export must retain its deployed-runtime identity")?;
        assert_eq!(
            deployed_runtime_identity.module_sha256(),
            DEPLOYED_MODULE_SHA256
        );
        assert_eq!(
            deployed_runtime_identity
                .source_package_archive_sha256()
                .context("legacy fixture must bind the source-package archive")?,
            DEPLOYED_SOURCE_PACKAGE_SHA256
        );
        let resolved = registry.load_export_package(
            fixture.cache_root(),
            "example.js",
            "run",
            UdfKind::Query,
            &compatibility(),
            fixture.cache_root(),
        )?;
        let DeploymentExportPackage::Wasm(package) = resolved else {
            anyhow::bail!("eligible export routed to V8")
        };
        assert_eq!(package.package_key, fixture.package_key());
        let ValidatedWasmUdfPackageIdentity::Legacy(manifest) = &package.identity else {
            anyhow::bail!("legacy deployment loaded a capability-entry package")
        };
        assert_eq!(manifest.source().module_path(), "convex/example.ts");
        Ok(())
    }

    #[test]
    fn validates_deployment_precompiler_identity_and_package_binding() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let valid = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        let valid_path = write_deployment_manifest(&fixture, &valid)?;
        ValidatedDeploymentManifest::load(&valid_path)?;

        let mut missing = valid.clone();
        missing
            .as_object_mut()
            .context("deployment fixture must be an object")?
            .remove("artifactPrecompiler");
        sign_deployment_manifest(&mut missing);
        let missing_path = write_deployment_manifest(&fixture, &missing)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&missing_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        for mut invalid in [
            {
                let mut manifest = valid.clone();
                manifest["artifactPrecompiler"]["unexpected"] = json!(true);
                manifest
            },
            {
                let mut manifest = valid.clone();
                manifest["artifactPrecompiler"]["materialIdentity"]["unexpected"] = json!(true);
                manifest
            },
            {
                let mut manifest = valid.clone();
                manifest["artifactPrecompiler"]["materialIdentity"]["packageId"] =
                    json!("not-a-digest");
                manifest
            },
            {
                let mut manifest = valid.clone();
                manifest["artifactPrecompiler"]["materialIdentity"]["binary"]["size"] = json!(0);
                manifest
            },
            {
                let mut manifest = valid.clone();
                manifest["artifactPrecompiler"]["materialIdentity"]["targetTriple"] =
                    json!("other-target");
                manifest
            },
            {
                let mut manifest = valid.clone();
                manifest["artifactPrecompiler"]["materialIdentity"]["wasmtimeRevision"] =
                    json!("other-revision");
                manifest
            },
        ] {
            sign_deployment_manifest(&mut invalid);
            let invalid_path = write_deployment_manifest(&fixture, &invalid)?;
            assert!(matches!(
                ValidatedDeploymentManifest::load(&invalid_path),
                Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
            ));
        }

        let mut substituted = valid;
        substituted["artifactPrecompiler"]["materialIdentity"]["binary"]["sha256"] =
            json!("7777777777777777777777777777777777777777777777777777777777777777");
        sign_deployment_manifest(&mut substituted);
        let substituted_path = write_deployment_manifest(&fixture, &substituted)?;
        let registry = ValidatedDeploymentManifest::load(&substituted_path)?;
        assert!(matches!(
            registry.load_export_package(
                fixture.cache_root(),
                "example.js",
                "run",
                UdfKind::Query,
                &compatibility(),
                fixture.cache_root(),
            ),
            Err(WasmUdfPackageError::InvalidBuildProvenance(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_malformed_deployed_runtime_identities() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let invalid_identities = [
            json!({
                "kind": "wrong-kind",
                "moduleSha256": DEPLOYED_MODULE_SHA256,
                "sourcePackageSha256": DEPLOYED_SOURCE_PACKAGE_SHA256,
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_KIND_V1,
                "moduleSha256": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "sourcePackageSha256": DEPLOYED_SOURCE_PACKAGE_SHA256,
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_KIND_V1,
                "moduleSha256": DEPLOYED_MODULE_SHA256,
                "sourcePackageSha256": "short",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_KIND_V1,
                "moduleSha256": DEPLOYED_MODULE_SHA256,
                "sourcePackageSha256": DEPLOYED_SOURCE_PACKAGE_SHA256,
                "unexpected": true,
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_KIND_V1,
                "moduleSha256": DEPLOYED_MODULE_SHA256,
                "sourcePackageRuntimeContentSha256": DEPLOYED_SOURCE_PACKAGE_SHA256,
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_KIND_V2,
                "moduleSha256": DEPLOYED_MODULE_SHA256,
                "sourcePackageSha256": DEPLOYED_SOURCE_PACKAGE_SHA256,
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_KIND_V2,
                "moduleSha256": DEPLOYED_MODULE_SHA256,
                "sourcePackageRuntimeContentSha256": "short",
            }),
        ];
        for invalid_identity in invalid_identities {
            let mut export = deployment_export(&fixture);
            export["source"]["deployedRuntimeIdentity"] = invalid_identity;
            let manifest = signed_deployment_manifest(vec![export]);
            let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
            assert!(matches!(
                ValidatedDeploymentManifest::load(&manifest_path),
                Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn accepts_runtime_content_deployed_runtime_identity() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut export = deployment_export(&fixture);
        export["source"]["deployedRuntimeIdentity"] = json!({
            "kind": DEPLOYED_RUNTIME_IDENTITY_KIND_V2,
            "moduleSha256": DEPLOYED_MODULE_SHA256,
            "sourcePackageRuntimeContentSha256": DEPLOYED_SOURCE_PACKAGE_SHA256,
        });
        let manifest = signed_deployment_manifest(vec![export]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        let registry = ValidatedDeploymentManifest::load(&manifest_path)?;
        let identity = registry
            .deployed_runtime_identity("example.js", "run", UdfKind::Query)?
            .context("runtime-content fixture must retain its deployed-runtime identity")?;
        assert_eq!(identity.module_sha256(), DEPLOYED_MODULE_SHA256);
        assert_eq!(identity.source_package_archive_sha256(), None);
        assert_eq!(
            identity.source_package_runtime_content_sha256(),
            Some(DEPLOYED_SOURCE_PACKAGE_SHA256)
        );
        Ok(())
    }

    #[test]
    fn requires_exactly_one_deployed_runtime_binding_for_selected_wasm() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let diagnostic = json!({
            "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
            "reasons": [{
                "code": "DEPLOYED_RUNTIME_AUTHORITY_NOT_PROVIDED",
                "detail": "no authenticated deployed-runtime material was provided",
            }],
            "verdict": "unprovable",
        });

        let mut both = deployment_export(&fixture);
        both["source"]["deployedRuntimeIdentityDiagnostic"] = diagnostic;
        let manifest = signed_deployment_manifest(vec![both]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut neither = deployment_export(&fixture);
        remove_deployed_runtime_binding(&mut neither);
        let manifest = signed_deployment_manifest(vec![neither]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_malformed_deployed_runtime_identity_diagnostics() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let valid_reason = json!({
            "code": "DEPLOYED_RUNTIME_AUTHORITY_NOT_PROVIDED",
            "detail": "no authenticated deployed-runtime material was provided",
        });
        let too_many_reasons =
            vec![valid_reason.clone(); MAX_DEPLOYED_RUNTIME_IDENTITY_REASONS + 1];
        let invalid_diagnostics = [
            json!({
                "kind": "wrong-kind",
                "reasons": [valid_reason],
                "verdict": "unprovable",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": [],
                "verdict": "unprovable",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": [valid_reason],
                "verdict": "unknown",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": [{
                    "code": "",
                    "detail": "actionable detail",
                }],
                "verdict": "mismatch",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": [{
                    "code": "not stable",
                    "detail": "actionable detail",
                }],
                "verdict": "mismatch",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": [{
                    "code": "STABLE_CODE",
                    "detail": "",
                }],
                "verdict": "mismatch",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": [{
                    "code": "X".repeat(MAX_DEPLOYED_RUNTIME_IDENTITY_REASON_CODE_BYTES + 1),
                    "detail": "actionable detail",
                }],
                "verdict": "mismatch",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": [{
                    "code": "STABLE_CODE",
                    "detail":
                        "x".repeat(MAX_DEPLOYED_RUNTIME_IDENTITY_REASON_DETAIL_BYTES + 1),
                }],
                "verdict": "mismatch",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": [{
                    "code": "STABLE_CODE",
                    "detail": "actionable detail",
                    "unexpected": true,
                }],
                "verdict": "mismatch",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": [valid_reason],
                "unexpected": true,
                "verdict": "mismatch",
            }),
            json!({
                "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
                "reasons": too_many_reasons,
                "verdict": "unprovable",
            }),
        ];
        for invalid_diagnostic in invalid_diagnostics {
            let mut export = deployment_export(&fixture);
            remove_deployed_runtime_binding(&mut export);
            export["source"]["deployedRuntimeIdentityDiagnostic"] = invalid_diagnostic;
            let manifest = signed_deployment_manifest(vec![export]);
            let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
            assert!(matches!(
                ValidatedDeploymentManifest::load(&manifest_path),
                Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn unbound_selected_wasm_fails_before_package_access() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut export = deployment_export(&fixture);
        remove_deployed_runtime_binding(&mut export);
        export["source"]["deployedRuntimeIdentityDiagnostic"] = json!({
            "kind": DEPLOYED_RUNTIME_IDENTITY_DIAGNOSTIC_KIND,
            "reasons": [{
                "code": "AUTHORITATIVE_BUNDLE_MODULE_MISMATCH",
                "detail": "the authoritative bundle does not match the active module",
            }],
            "verdict": "mismatch",
        });
        let manifest = signed_deployment_manifest(vec![export]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        let registry = ValidatedDeploymentManifest::load(&manifest_path)?;

        for result in [
            registry.export_routing("example.js", "run", UdfKind::Query),
            registry
                .deployed_runtime_identity("example.js", "run", UdfKind::Query)
                .map(|_| DeploymentExportRouting::ExistingRuntime),
        ] {
            let Err(WasmUdfPackageError::UnboundDeployedRuntimeIdentity(diagnostic)) = result
            else {
                anyhow::bail!("unbound selected Wasm export did not fail closed")
            };
            assert_eq!(
                diagnostic.verdict(),
                DeployedRuntimeIdentityVerdict::Mismatch
            );
            assert_eq!(
                diagnostic.reasons()[0].code(),
                "AUTHORITATIVE_BUNDLE_MODULE_MISMATCH"
            );
            assert_eq!(
                diagnostic.reasons()[0].detail(),
                "the authoritative bundle does not match the active module"
            );
        }

        assert!(matches!(
            registry.load_export_package(
                Path::new("/package-access-must-not-occur"),
                "example.js",
                "run",
                UdfKind::Query,
                &compatibility(),
                fixture.cache_root(),
            ),
            Err(WasmUdfPackageError::UnboundDeployedRuntimeIdentity(_))
        ));
        Ok(())
    }

    #[test]
    fn deployment_binds_compiler_limits_to_package_and_deployment_identities() -> anyhow::Result<()>
    {
        let fixture = PackageFixture::create()?;
        let original_package_key = fixture.package_key().to_owned();
        let original_deployment = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        let original_deployment_sha256 = original_deployment["deploymentSha256"]
            .as_str()
            .context("deployment digest missing")?
            .to_owned();

        let mut changed_manifest = fixture.manifest.clone();
        changed_manifest["platformLimits"]["documentsRead"] = json!(
            changed_manifest["platformLimits"]["documentsRead"]
                .as_u64()
                .context("documentsRead fixture missing")?
                - 1
        );
        let changed_package_key = sha256(&canonical_bytes(&changed_manifest));
        assert_ne!(changed_package_key, original_package_key);

        let mut changed_export = deployment_export(&fixture);
        changed_export["artifact"]["executionManifest"] = changed_manifest.clone();
        changed_export["compilerLimits"] = changed_manifest["platformLimits"].clone();
        changed_export["packageReference"]["cacheKey"] = json!(changed_package_key);
        let changed_deployment = signed_deployment_manifest(vec![changed_export]);
        assert_ne!(
            changed_deployment["deploymentSha256"]
                .as_str()
                .context("changed deployment digest missing")?,
            original_deployment_sha256
        );
        Ok(())
    }

    #[test]
    fn rejects_missing_or_inconsistent_deployment_compiler_limits() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut missing = deployment_export(&fixture);
        missing
            .as_object_mut()
            .context("deployment export fixture must be an object")?
            .remove("compilerLimits");
        let manifest = signed_deployment_manifest(vec![missing]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut inconsistent = deployment_export(&fixture);
        inconsistent["compilerLimits"]["documentsRead"] = json!(1);
        let manifest = signed_deployment_manifest(vec![inconsistent]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        Ok(())
    }

    #[test]
    fn returns_authenticated_v8_fallback_without_loading_a_package() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut export = deployment_export(&fixture);
        export["artifact"] = JsonValue::Null;
        export["packageReference"] = JsonValue::Null;
        export
            .as_object_mut()
            .context("deployment export fixture must be an object")?
            .remove("compilerLimits");
        export["routing"] = json!({
            "decision": "v8Fallback",
            "reason": "staticIneligibility",
        });
        remove_deployed_runtime_binding(&mut export);
        let manifest = signed_deployment_manifest(vec![export]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        let registry = ValidatedDeploymentManifest::load(&manifest_path)?;
        assert!(matches!(
            registry.export_routing("example.js", "run", UdfKind::Query)?,
            DeploymentExportRouting::V8Fallback
        ));
        assert!(registry
            .deployed_runtime_identity("example.js", "run", UdfKind::Query)?
            .is_none());
        assert!(matches!(
            registry.load_export_package(
                fixture.cache_root(),
                "example.js",
                "run",
                UdfKind::Query,
                &compatibility(),
                fixture.cache_root(),
            )?,
            DeploymentExportPackage::V8Fallback
        ));
        Ok(())
    }

    #[test]
    fn keeps_unselected_eligible_export_on_existing_runtime() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut export = deployment_export(&fixture);
        export["artifact"] = JsonValue::Null;
        export["packageReference"] = JsonValue::Null;
        export
            .as_object_mut()
            .context("deployment export fixture must be an object")?
            .remove("compilerLimits");
        export["routing"] = json!({
            "decision": "existingRuntime",
            "reason": "not-selected",
        });
        remove_deployed_runtime_binding(&mut export);
        let manifest = signed_deployment_manifest(vec![export]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        let registry = ValidatedDeploymentManifest::load(&manifest_path)?;
        assert!(matches!(
            registry.export_routing("example.js", "run", UdfKind::Query)?,
            DeploymentExportRouting::ExistingRuntime
        ));
        assert!(registry
            .deployed_runtime_identity("example.js", "run", UdfKind::Query)?
            .is_none());
        assert!(matches!(
            registry.load_export_package(
                fixture.cache_root(),
                "example.js",
                "run",
                UdfKind::Query,
                &compatibility(),
                fixture.cache_root(),
            )?,
            DeploymentExportPackage::ExistingRuntime
        ));
        Ok(())
    }

    #[test]
    fn validates_reason_specific_diagnostic_census_identity_presence() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut compiler_null_fallback = deployment_export(&fixture);
        compiler_null_fallback["artifact"] = JsonValue::Null;
        compiler_null_fallback["compiler"] = JsonValue::Null;
        compiler_null_fallback["packageReference"] = JsonValue::Null;
        compiler_null_fallback["routing"] = json!({
            "decision": "v8Fallback",
            "reason": "unsupported-registration-builder",
        });
        compiler_null_fallback
            .as_object_mut()
            .context("deployment export fixture must be an object")?
            .remove("diagnosticCensusIds");
        compiler_null_fallback
            .as_object_mut()
            .context("deployment export fixture must be an object")?
            .remove("compilerLimits");
        let compiler_null_source = compiler_null_fallback["source"]
            .as_object_mut()
            .context("deployment export source fixture must be an object")?;
        compiler_null_source.remove("deployedRuntimeIdentity");
        compiler_null_source.remove("exportSha256");
        compiler_null_source.remove("resolvedGraphSha256");
        let manifest = signed_v5_capability_selection_manifest(vec![compiler_null_fallback]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        ValidatedDeploymentManifest::load(&manifest_path)?;

        let mut missing_context_reuse_identity = manifest.clone();
        missing_context_reuse_identity
            .as_object_mut()
            .context("v5 deployment manifest fixture must be an object")?
            .remove("contextReusePolicyIdentity");
        sign_deployment_manifest(&mut missing_context_reuse_identity);
        let manifest_path = write_deployment_manifest(&fixture, &missing_context_reuse_identity)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut invalid_context_reuse_kind = manifest.clone();
        invalid_context_reuse_kind["contextReusePolicyIdentity"]["kind"] = json!("other");
        sign_deployment_manifest(&mut invalid_context_reuse_kind);
        let manifest_path = write_deployment_manifest(&fixture, &invalid_context_reuse_kind)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut invalid_context_reuse_sha256 = manifest.clone();
        invalid_context_reuse_sha256["contextReusePolicyIdentity"]["sha256"] = json!("invalid");
        sign_deployment_manifest(&mut invalid_context_reuse_sha256);
        let manifest_path = write_deployment_manifest(&fixture, &invalid_context_reuse_sha256)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        for routing in [
            json!({
                "decision": "wasm",
                "reason": "staticEligibility",
            }),
            json!({
                "decision": "existingRuntime",
                "reason": "not-selected",
            }),
            json!({
                "decision": "v8Fallback",
                "reason": "staticIneligibility",
            }),
        ] {
            let mut analyzed_export = deployment_export(&fixture);
            if routing["decision"] != "wasm" {
                analyzed_export["artifact"] = JsonValue::Null;
                analyzed_export["packageReference"] = JsonValue::Null;
                remove_deployed_runtime_binding(&mut analyzed_export);
                analyzed_export
                    .as_object_mut()
                    .context("deployment export fixture must be an object")?
                    .remove("compilerLimits");
            }
            analyzed_export["routing"] = routing;
            analyzed_export
                .as_object_mut()
                .context("deployment export fixture must be an object")?
                .remove("diagnosticCensusIds");
            let manifest = signed_deployment_manifest(vec![analyzed_export]);
            let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
            assert!(matches!(
                ValidatedDeploymentManifest::load(&manifest_path),
                Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn validates_reason_specific_source_identity_shape() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut compiler_null_fallback = deployment_export(&fixture);
        compiler_null_fallback["artifact"] = JsonValue::Null;
        compiler_null_fallback["compiler"] = JsonValue::Null;
        compiler_null_fallback["packageReference"] = JsonValue::Null;
        compiler_null_fallback["routing"] = json!({
            "decision": "v8Fallback",
            "reason": "unsupported-registration-builder",
        });
        compiler_null_fallback
            .as_object_mut()
            .context("deployment export fixture must be an object")?
            .remove("diagnosticCensusIds");
        compiler_null_fallback
            .as_object_mut()
            .context("deployment export fixture must be an object")?
            .remove("compilerLimits");
        let source = compiler_null_fallback["source"]
            .as_object_mut()
            .context("deployment export source fixture must be an object")?;
        source.remove("deployedRuntimeIdentity");
        source.remove("exportSha256");
        source.remove("resolvedGraphSha256");
        let manifest = signed_deployment_manifest(vec![compiler_null_fallback.clone()]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        ValidatedDeploymentManifest::load(&manifest_path)?;

        let mut compiler_null_with_analyzed_source = compiler_null_fallback;
        compiler_null_with_analyzed_source["source"]["exportSha256"] =
            fixture.manifest["source"]["exportSha256"].clone();
        compiler_null_with_analyzed_source["source"]["resolvedGraphSha256"] =
            fixture.manifest["source"]["resolvedGraphSha256"].clone();
        let manifest = signed_deployment_manifest(vec![compiler_null_with_analyzed_source]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        for routing in [
            json!({
                "decision": "wasm",
                "reason": "staticEligibility",
            }),
            json!({
                "decision": "existingRuntime",
                "reason": "not-selected",
            }),
            json!({
                "decision": "v8Fallback",
                "reason": "staticIneligibility",
            }),
        ] {
            let mut analyzed_export = deployment_export(&fixture);
            if routing["decision"] != "wasm" {
                analyzed_export["artifact"] = JsonValue::Null;
                analyzed_export["packageReference"] = JsonValue::Null;
                remove_deployed_runtime_binding(&mut analyzed_export);
                analyzed_export
                    .as_object_mut()
                    .context("deployment export fixture must be an object")?
                    .remove("compilerLimits");
            }
            analyzed_export["routing"] = routing;
            analyzed_export["source"]
                .as_object_mut()
                .context("deployment export source fixture must be an object")?
                .remove("exportSha256");
            let manifest = signed_deployment_manifest(vec![analyzed_export]);
            let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
            assert!(matches!(
                ValidatedDeploymentManifest::load(&manifest_path),
                Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn validates_explicit_and_all_eligible_compile_selection_shapes() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let legacy_explicit = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        let manifest_path = write_deployment_manifest(&fixture, &legacy_explicit)?;
        ValidatedDeploymentManifest::load(&manifest_path)?;

        // Keep the v3 selection-policy cases independent of cohort-package validation.
        // The historical v2 fixture above intentionally uses a singleton
        // package reference, which is not a valid v3 export shape.
        let mut historical_explicit_v3 = signed_deployment_manifest(Vec::new());
        historical_explicit_v3["kind"] = json!(DEPLOYMENT_MANIFEST_KIND_V3);
        sign_deployment_manifest(&mut historical_explicit_v3);
        let manifest_path = write_deployment_manifest(&fixture, &historical_explicit_v3)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load_for_development_test(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut development_explicit = historical_explicit_v3;
        development_explicit["compileSelection"]["kind"] =
            json!(DEVELOPMENT_COMPILE_SELECTION_KIND);
        development_explicit["compileSelection"]["developmentOnly"] = json!(true);
        sign_deployment_manifest(&mut development_explicit);
        let manifest_path = write_deployment_manifest(&fixture, &development_explicit)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        ValidatedDeploymentManifest::load_for_development_test(&manifest_path)?;

        let mut development_explicit_v4 = development_explicit.clone();
        development_explicit_v4["kind"] = json!(DEPLOYMENT_MANIFEST_KIND_V4);
        development_explicit_v4["counts"]["artifactFallback"] = json!(0);
        sign_deployment_manifest(&mut development_explicit_v4);
        let manifest_path = write_deployment_manifest(&fixture, &development_explicit_v4)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        ValidatedDeploymentManifest::load_for_development_test(&manifest_path)?;

        let mut development_explicit_without_marker = development_explicit.clone();
        development_explicit_without_marker["compileSelection"]
            .as_object_mut()
            .context("development compileSelection fixture must be an object")?
            .remove("developmentOnly");
        sign_deployment_manifest(&mut development_explicit_without_marker);
        let manifest_path =
            write_deployment_manifest(&fixture, &development_explicit_without_marker)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load_for_development_test(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut all_eligible = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        all_eligible["compileSelection"] = json!({
            "kind": ALL_ELIGIBLE_COMPILE_SELECTION_KIND,
        });
        sign_deployment_manifest(&mut all_eligible);
        let manifest_path = write_deployment_manifest(&fixture, &all_eligible)?;
        ValidatedDeploymentManifest::load(&manifest_path)?;

        let mut all_eligible_with_exports = all_eligible.clone();
        all_eligible_with_exports["compileSelection"]["exports"] = json!([]);
        sign_deployment_manifest(&mut all_eligible_with_exports);
        let manifest_path = write_deployment_manifest(&fixture, &all_eligible_with_exports)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut explicit_without_exports = all_eligible.clone();
        explicit_without_exports["compileSelection"] = json!({
            "kind": COMPILE_SELECTION_KIND,
        });
        sign_deployment_manifest(&mut explicit_without_exports);
        let manifest_path = write_deployment_manifest(&fixture, &explicit_without_exports)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut existing_runtime_export = deployment_export(&fixture);
        existing_runtime_export["artifact"] = JsonValue::Null;
        existing_runtime_export["packageReference"] = JsonValue::Null;
        existing_runtime_export
            .as_object_mut()
            .context("deployment export fixture must be an object")?
            .remove("compilerLimits");
        existing_runtime_export["routing"] = json!({
            "decision": "existingRuntime",
            "reason": "not-selected",
        });
        remove_deployed_runtime_binding(&mut existing_runtime_export);
        let mut incomplete_all_eligible = signed_deployment_manifest(vec![existing_runtime_export]);
        incomplete_all_eligible["compileSelection"] = json!({
            "kind": ALL_ELIGIBLE_COMPILE_SELECTION_KIND,
        });
        sign_deployment_manifest(&mut incomplete_all_eligible);
        let manifest_path = write_deployment_manifest(&fixture, &incomplete_all_eligible)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut wrong_all_eligible_count = all_eligible;
        wrong_all_eligible_count["counts"]["selectedWasm"] = json!(0);
        sign_deployment_manifest(&mut wrong_all_eligible_count);
        let manifest_path = write_deployment_manifest(&fixture, &wrong_all_eligible_count)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        Ok(())
    }

    #[test]
    fn validates_v5_capability_entry_compile_selection() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_v5_capability_selection_manifest(vec![
            capability_entry_deployment_export(&fixture),
            not_analyzed_not_selected_v5_deployment_export(&fixture),
            not_analyzed_v5_deployment_export(&fixture),
        ]);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        ValidatedDeploymentManifest::load(&manifest_path)?;

        let mut static_eligibility_route = manifest.clone();
        static_eligibility_route["exports"][0]["routing"]["reason"] = json!("staticEligibility");
        sign_deployment_manifest(&mut static_eligibility_route);
        let manifest_path = write_deployment_manifest(&fixture, &static_eligibility_route)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut duplicate_selection = manifest.clone();
        let duplicate_identity = duplicate_selection["compileSelection"]["exports"][0].clone();
        duplicate_selection["compileSelection"]["exports"]
            .as_array_mut()
            .context("capability compileSelection exports fixture must be an array")?
            .push(duplicate_identity);
        sign_deployment_manifest(&mut duplicate_selection);
        let manifest_path = write_deployment_manifest(&fixture, &duplicate_selection)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut missing_wasm_route = manifest.clone();
        missing_wasm_route["compileSelection"]["exports"] = json!([]);
        sign_deployment_manifest(&mut missing_wasm_route);
        let manifest_path = write_deployment_manifest(&fixture, &missing_wasm_route)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let non_wasm_export = not_selected_v5_deployment_export(&fixture);
        let mut selected_non_wasm = signed_v5_capability_selection_manifest(vec![non_wasm_export]);
        selected_non_wasm["compileSelection"]["exports"] = json!([{
            "entryPath": "convex/example.ts",
            "exportName": "run",
        }]);
        sign_deployment_manifest(&mut selected_non_wasm);
        let manifest_path = write_deployment_manifest(&fixture, &selected_non_wasm)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut not_selected_without_graph = not_selected_v5_deployment_export(&fixture);
        not_selected_without_graph
            .as_object_mut()
            .context("deployment export fixture must be an object")?
            .remove("dependencyGraphSha256");
        let not_selected_without_graph =
            signed_v5_capability_selection_manifest(vec![not_selected_without_graph]);
        let manifest_path = write_deployment_manifest(&fixture, &not_selected_without_graph)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut not_analyzed_not_selected_with_rich_source =
            not_analyzed_not_selected_v5_deployment_export(&fixture);
        not_analyzed_not_selected_with_rich_source["source"]["exportSha256"] =
            json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let not_analyzed_not_selected_with_rich_source =
            signed_v5_capability_selection_manifest(vec![
                not_analyzed_not_selected_with_rich_source,
            ]);
        let manifest_path =
            write_deployment_manifest(&fixture, &not_analyzed_not_selected_with_rich_source)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut not_analyzed_not_selected_with_wrong_compiler =
            not_analyzed_not_selected_v5_deployment_export(&fixture);
        not_analyzed_not_selected_with_wrong_compiler["compiler"]["kind"] = json!("other");
        let not_analyzed_not_selected_with_wrong_compiler =
            signed_v5_capability_selection_manifest(vec![
                not_analyzed_not_selected_with_wrong_compiler,
            ]);
        let manifest_path =
            write_deployment_manifest(&fixture, &not_analyzed_not_selected_with_wrong_compiler)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut not_analyzed_with_graph = not_analyzed_v5_deployment_export(&fixture);
        not_analyzed_with_graph["dependencyGraphSha256"] =
            json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let not_analyzed_with_graph =
            signed_v5_capability_selection_manifest(vec![not_analyzed_with_graph]);
        let manifest_path = write_deployment_manifest(&fixture, &not_analyzed_with_graph)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut not_analyzed_with_wrong_compiler = not_analyzed_v5_deployment_export(&fixture);
        not_analyzed_with_wrong_compiler["compiler"]["kind"] = json!("other");
        let not_analyzed_with_wrong_compiler =
            signed_v5_capability_selection_manifest(vec![not_analyzed_with_wrong_compiler]);
        let manifest_path = write_deployment_manifest(&fixture, &not_analyzed_with_wrong_compiler)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut unknown_selection_field = manifest.clone();
        unknown_selection_field["compileSelection"]["unexpected"] = json!(true);
        sign_deployment_manifest(&mut unknown_selection_field);
        let manifest_path = write_deployment_manifest(&fixture, &unknown_selection_field)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut wrong_version = manifest.clone();
        wrong_version["kind"] = json!(DEPLOYMENT_MANIFEST_KIND_V4);
        sign_deployment_manifest(&mut wrong_version);
        let manifest_path = write_deployment_manifest(&fixture, &wrong_version)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let first_export = capability_entry_deployment_export(&fixture);
        let mut duplicate_routed_identity = first_export.clone();
        duplicate_routed_identity["runtimeModulePath"] = json!("other.js");
        let mut duplicate_routed_identities =
            signed_v5_capability_selection_manifest(vec![first_export, duplicate_routed_identity]);
        duplicate_routed_identities["compileSelection"]["exports"] = json!([{
            "entryPath": "convex/example.ts",
            "exportName": "run",
        }]);
        sign_deployment_manifest(&mut duplicate_routed_identities);
        let manifest_path = write_deployment_manifest(&fixture, &duplicate_routed_identities)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        Ok(())
    }

    #[test]
    fn validates_v4_artifact_fallback_counts_and_shape() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = signed_v4_artifact_fallback_manifest(&fixture);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        let registry = ValidatedDeploymentManifest::load(&manifest_path)?;
        assert!(matches!(
            registry.export_routing("example.js", "run", UdfKind::Query)?,
            DeploymentExportRouting::V8Fallback
        ));

        let mut wrong_count = manifest.clone();
        wrong_count["counts"]["artifactFallback"] = json!(0);
        sign_deployment_manifest(&mut wrong_count);
        let manifest_path = write_deployment_manifest(&fixture, &wrong_count)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut missing_count = manifest.clone();
        missing_count["counts"]
            .as_object_mut()
            .context("v4 counts fixture must be an object")?
            .remove("artifactFallback");
        sign_deployment_manifest(&mut missing_count);
        let manifest_path = write_deployment_manifest(&fixture, &missing_count)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut missing_limits = manifest.clone();
        missing_limits["exports"][0]
            .as_object_mut()
            .context("v4 artifact fallback fixture must be an object")?
            .remove("compilerLimits");
        sign_deployment_manifest(&mut missing_limits);
        let manifest_path = write_deployment_manifest(&fixture, &missing_limits)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut v3 = manifest;
        v3["kind"] = json!(DEPLOYMENT_MANIFEST_KIND_V3);
        v3["counts"]
            .as_object_mut()
            .context("v3 counts fixture must be an object")?
            .remove("artifactFallback");
        sign_deployment_manifest(&mut v3);
        let manifest_path = write_deployment_manifest(&fixture, &v3)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&manifest_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        Ok(())
    }

    #[test]
    fn accepts_authenticated_diagnostic_census_and_operation_summary() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut export = deployment_export(&fixture);
        export["diagnosticCensusIds"] = json!(["diagnostic_fixture"]);
        export["operationSummary"] = json!({
            "argumentFieldCount": 1,
            "documentPropertyCount": 1,
            "intrinsicCount": 1,
            "operationCount": 10,
            "operationsByKind": {
                "authenticationGetUserIdentity": 1,
                "databaseGet": 1,
                "databaseNormalizeId": 1,
                "databaseInsert": 1,
                "databasePatch": 1,
                "databaseReplace": 1,
                "databaseDelete": 1,
                "functionHandleCreate": 1,
                "hostSecretVerify": 1,
                "sha256": 1,
            },
        });
        let mut manifest = signed_deployment_manifest(vec![export]);
        manifest["diagnosticCensus"] = json!([{
            "code": "unsupported-construct",
            "column": 1,
            "construct": "ExampleExpression",
            "exportCount": 1,
            "file": "functions/example.ts",
            "id": "diagnostic_fixture",
            "line": 1,
            "message": "example diagnostic",
            "occurrenceCount": 1,
            "source": "example",
        }]);
        sign_deployment_manifest(&mut manifest);
        let manifest_path = write_deployment_manifest(&fixture, &manifest)?;
        ValidatedDeploymentManifest::load(&manifest_path)?;
        Ok(())
    }

    #[test]
    fn rejects_tampered_or_inconsistent_deployment_routing() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut tampered = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        tampered["exports"][0]["visibility"] = json!("internal");
        let tampered_path = write_deployment_manifest(&fixture, &tampered)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&tampered_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut inconsistent_export = deployment_export(&fixture);
        inconsistent_export["source"]["exportName"] = json!("other");
        let inconsistent = signed_deployment_manifest(vec![inconsistent_export]);
        let inconsistent_path = write_deployment_manifest(&fixture, &inconsistent)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&inconsistent_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut unknown_routing = deployment_export(&fixture);
        unknown_routing["routing"]["unexpected"] = json!(true);
        let unknown = signed_deployment_manifest(vec![unknown_routing]);
        let unknown_path = write_deployment_manifest(&fixture, &unknown)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&unknown_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut wrong_counts = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        wrong_counts["counts"]["total"] = json!(2);
        sign_deployment_manifest(&mut wrong_counts);
        let wrong_counts_path = write_deployment_manifest(&fixture, &wrong_counts)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&wrong_counts_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut wrong_partition = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        wrong_partition["counts"]["selectedWasm"] = json!(0);
        wrong_partition["counts"]["unselectedEligible"] = json!(1);
        sign_deployment_manifest(&mut wrong_partition);
        let wrong_partition_path = write_deployment_manifest(&fixture, &wrong_partition)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&wrong_partition_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut wrong_selection = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        wrong_selection["compileSelection"]["exports"][0]["exportName"] = json!("other");
        sign_deployment_manifest(&mut wrong_selection);
        let wrong_selection_path = write_deployment_manifest(&fixture, &wrong_selection)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&wrong_selection_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut malformed_census = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        malformed_census["exports"][0]["diagnosticCensusIds"] = json!([1]);
        sign_deployment_manifest(&mut malformed_census);
        let malformed_census_path = write_deployment_manifest(&fixture, &malformed_census)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&malformed_census_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));

        let mut duplicate_action = signed_deployment_manifest(vec![deployment_export(&fixture)]);
        duplicate_action["existingRuntimeActions"] = json!([{
            "entryPath": "convex/example.ts",
            "exportName": "run",
            "routing": {
                "decision": "existingRuntime",
                "reason": "action-runtime",
            },
            "runtimeModulePath": "example.js",
            "udfKind": "action",
            "visibility": "public",
        }]);
        duplicate_action["counts"]["actionsOnExistingRuntime"] = json!(1);
        sign_deployment_manifest(&mut duplicate_action);
        let duplicate_action_path = write_deployment_manifest(&fixture, &duplicate_action)?;
        assert!(matches!(
            ValidatedDeploymentManifest::load(&duplicate_action_path),
            Err(WasmUdfPackageError::InvalidDeploymentManifest(_))
        ));
        Ok(())
    }

    #[test]
    fn validates_complete_package_before_returning_aot_snapshot() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let package =
            load_validated_package(&fixture.path, &compatibility(), fixture.cache_root())?;
        let mut serialized_module_snapshot = package.serialized_module_snapshot;
        let mut serialized_module = Vec::new();
        serialized_module_snapshot.read_to_end(&mut serialized_module)?;
        assert_eq!(serialized_module, b"serialized-module-fixture".to_vec());
        assert_eq!(
            package.package_key,
            fixture
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .context("package fixture key missing")?
        );
        let ValidatedWasmUdfPackageIdentity::Legacy(manifest) = &package.identity else {
            anyhow::bail!("singleton fixture loaded a capability-entry package")
        };
        assert_eq!(manifest.source().export_name(), "run");
        Ok(())
    }

    #[test]
    fn normal_package_serialized_module_size_limit_is_one_gib() {
        assert_eq!(MAX_SERIALIZED_MODULE_BYTES, 1024 * 1024 * 1024);
        let sha256 = "a".repeat(64);
        let at_limit = PackageArtifact {
            sha256: sha256.clone(),
            size: MAX_SERIALIZED_MODULE_BYTES,
        };
        validate_artifact_identity(
            "module.cwasm",
            &at_limit,
            MAX_SERIALIZED_MODULE_BYTES,
            &sha256,
            MAX_SERIALIZED_MODULE_BYTES,
        )
        .expect("1 GiB serialized module metadata was rejected");

        let above_limit = PackageArtifact {
            sha256: sha256.clone(),
            size: MAX_SERIALIZED_MODULE_BYTES + 1,
        };
        assert!(matches!(
            validate_artifact_identity(
                "module.cwasm",
                &above_limit,
                MAX_SERIALIZED_MODULE_BYTES + 1,
                &sha256,
                MAX_SERIALIZED_MODULE_BYTES,
            ),
            Err(WasmUdfPackageError::InvalidPackageEntry(_))
        ));
    }

    #[test]
    fn validated_serialized_module_snapshot_ignores_source_file_mutation() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let package =
            load_validated_package(&fixture.path, &compatibility(), fixture.cache_root())?;
        let mut source_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(fixture.path.join("module.cwasm"))?;
        source_file.write_all(b"mutated-source-serialized-module")?;

        let mut serialized_module_snapshot = package.serialized_module_snapshot;
        let mut serialized_module = Vec::new();
        serialized_module_snapshot.read_to_end(&mut serialized_module)?;
        assert_eq!(serialized_module, b"serialized-module-fixture");
        Ok(())
    }

    #[test]
    fn serialized_module_snapshot_retains_authenticated_module_bytes() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let source_path = root.path().join("module.cwasm");
        let snapshot_directory = root.path().join("snapshots");
        let module_bytes = vec![0xa5; 65];
        write_private(&source_path, &module_bytes)?;
        create_private_directory(&snapshot_directory)?;

        let mut snapshot = snapshot_validated_serialized_module_file(
            &source_path,
            "module.cwasm",
            &PackageArtifact {
                sha256: sha256(&module_bytes),
                size: module_bytes.len() as u64,
            },
            &snapshot_directory,
        )?;

        let mut snapshot_bytes = Vec::new();
        snapshot.read_to_end(&mut snapshot_bytes)?;
        assert_eq!(snapshot_bytes, module_bytes);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn compatibility_snapshot_tempdir_is_owner_private() -> anyhow::Result<()> {
        let directory = create_private_serialized_module_snapshot_tempdir(Path::new(
            "compatibility serialized-module snapshot directory",
        ))?;
        let metadata = fs::symlink_metadata(directory.path())?;
        assert!(is_owner_private_serialized_module_snapshot_directory(
            &metadata
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_public_serialized_module_snapshot_directory() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let serialized_module_snapshot_directory = fixture.cache_root().join("snapshots");
        create_private_directory(&serialized_module_snapshot_directory)?;
        fs::set_permissions(
            &serialized_module_snapshot_directory,
            fs::Permissions::from_mode(0o755),
        )?;

        assert!(matches!(
            load_validated_package(
                &fixture.path,
                &compatibility(),
                &serialized_module_snapshot_directory,
            ),
            Err(WasmUdfPackageError::InvalidDirectory(path))
                if path == serialized_module_snapshot_directory
        ));
        Ok(())
    }

    #[test]
    fn rejects_artifact_corruption_before_returning_bytes() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        write_private(
            &fixture.path.join("module.cwasm"),
            b"serialized-module-corrupt",
        )?;
        assert!(matches!(
            load_validated_package(&fixture.path, &compatibility(), fixture.cache_root()),
            Err(WasmUdfPackageError::ArtifactDigest("module.cwasm"))
        ));
        Ok(())
    }

    #[test]
    fn rejects_provenance_corruption_before_returning_artifact_bytes() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut provenance: JsonValue = serde_json::from_slice(json_payload(&fs::read(
            fixture.path.join("build-provenance.json"),
        )?))?;
        provenance["sourcePipelineSha256"] =
            json!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        write_private(
            &fixture.path.join("build-provenance.json"),
            &canonical_file_bytes(&provenance),
        )?;
        assert!(matches!(
            load_validated_package(&fixture.path, &compatibility(), fixture.cache_root()),
            Err(WasmUdfPackageError::ArtifactDigest("build-provenance.json"))
        ));
        Ok(())
    }

    #[test]
    fn rejects_provenance_identities_that_differ_from_the_manifest() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let manifest = WasmUdfExecutionManifest::parse_for_runtime(
            &canonical_bytes(&fixture.manifest),
            &compatibility(),
        )?;
        let artifact = manifest
            .artifact()
            .context("Wasm fixture artifact is missing")?;
        let provenance: JsonValue = serde_json::from_slice(json_payload(&fs::read(
            fixture.path.join("build-provenance.json"),
        )?))?;

        for (field_path, replacement) in [
            (
                &["loweringPipelineSha256"][..],
                json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ),
            (
                &["engineIdentity", "engineCompatibilitySha256"][..],
                json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ),
            (&["engineIdentity", "target", "cpu"][..], json!("native")),
            (
                &["engineIdentity", "engineConfig", "consumeFuel"][..],
                json!(false),
            ),
        ] {
            let mut inconsistent = provenance.clone();
            let mut target = &mut inconsistent;
            for field in &field_path[..field_path.len() - 1] {
                target = &mut target[*field];
            }
            target[field_path[field_path.len() - 1]] = replacement;
            assert!(matches!(
                validate_build_provenance(
                    &canonical_bytes(&inconsistent),
                    manifest.compiler(),
                    artifact,
                    None,
                ),
                Err(WasmUdfPackageError::InvalidBuildProvenance(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn rejects_noncanonical_manifest_even_when_valid_json() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let noncanonical = serde_json::to_vec_pretty(&fixture.manifest)?;
        write_private(
            &fixture.path.join("execution-manifest.json"),
            &[noncanonical, b"\n".to_vec()].concat(),
        )?;
        assert!(matches!(
            load_validated_package(&fixture.path, &compatibility(), fixture.cache_root()),
            Err(WasmUdfPackageError::NonCanonicalJson(
                "execution-manifest.json"
            ))
        ));
        Ok(())
    }

    #[test]
    fn rejects_incompatible_engine_before_artifact_use() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let runtime = RuntimeCompatibility {
            opaque_value_abi_version: OPAQUE_VALUE_ABI_VERSION,
            platform_limits: PlatformLimits::authoritative()?,
            wasmtime_revision: "other-revision",
            target_triple: TARGET_TRIPLE,
            target_cpu: TARGET_CPU,
            engine_configuration_sha256: ENGINE_CONFIGURATION_SHA256,
            engine_compatibility_sha256:
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        };
        assert!(matches!(
            load_validated_package(&fixture.path, &runtime, fixture.cache_root()),
            Err(WasmUdfPackageError::InvalidExecutionManifest(
                ManifestError::IncompatibleEngine {
                    field: "artifact.wasmtimeRevision"
                }
            ))
        ));
        Ok(())
    }

    #[test]
    fn rejects_incompatible_platform_limits_before_artifact_use() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        let mut runtime = compatibility();
        runtime.platform_limits.documents_read += 1;
        assert!(matches!(
            load_validated_package(&fixture.path, &runtime, fixture.cache_root()),
            Err(WasmUdfPackageError::InvalidExecutionManifest(
                ManifestError::IncompatiblePlatformLimit {
                    field: "platformLimits.documentsRead"
                }
            ))
        ));
        Ok(())
    }

    #[test]
    fn rejects_partial_and_extended_packages() -> anyhow::Result<()> {
        let missing = PackageFixture::create()?;
        fs::remove_file(missing.path.join("module.wasm"))?;
        assert!(matches!(
            load_validated_package(&missing.path, &compatibility(), missing.cache_root()),
            Err(WasmUdfPackageError::MissingFile("module.wasm"))
        ));

        let extended = PackageFixture::create()?;
        write_private(&extended.path.join("unexpected"), b"unexpected")?;
        assert!(matches!(
            load_validated_package(&extended.path, &compatibility(), extended.cache_root()),
            Err(WasmUdfPackageError::UnexpectedEntry(name)) if name == "unexpected"
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinks_and_public_permissions() -> anyhow::Result<()> {
        use std::os::unix::fs::symlink;

        let symlinked = PackageFixture::create()?;
        fs::remove_file(symlinked.path.join("module.cwasm"))?;
        symlink(
            symlinked.path.join("module.wasm"),
            symlinked.path.join("module.cwasm"),
        )?;
        assert!(matches!(
            load_validated_package(&symlinked.path, &compatibility(), symlinked.cache_root()),
            Err(WasmUdfPackageError::Io { .. } | WasmUdfPackageError::InvalidFile { .. })
        ));

        let public_file = PackageFixture::create()?;
        fs::set_permissions(
            public_file.path.join("module.cwasm"),
            fs::Permissions::from_mode(0o644),
        )?;
        assert!(matches!(
            load_validated_package(
                &public_file.path,
                &compatibility(),
                public_file.cache_root(),
            ),
            Err(WasmUdfPackageError::InvalidFile { .. })
        ));

        let public_directory = PackageFixture::create()?;
        fs::set_permissions(&public_directory.path, fs::Permissions::from_mode(0o755))?;
        assert!(matches!(
            load_validated_package(
                &public_directory.path,
                &compatibility(),
                public_directory.cache_root(),
            ),
            Err(WasmUdfPackageError::InvalidDirectory(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_invalid_completion_marker() -> anyhow::Result<()> {
        let fixture = PackageFixture::create()?;
        write_private(&fixture.path.join("COMPLETE"), b"incomplete\n")?;
        assert!(matches!(
            load_validated_package(&fixture.path, &compatibility(), fixture.cache_root()),
            Err(WasmUdfPackageError::InvalidCompletionMarker)
        ));
        Ok(())
    }
}
