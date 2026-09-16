use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fs::{
        self,
        OpenOptions,
    },
    io::{
        BufReader,
        Read,
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
    wasm_udf_manifest::MAX_SERIALIZED_MODULE_BYTES,
    wasm_udf_package::{
        normalized_capability_producer_implementation_identity,
        validate_context_reuse_cohort_analysis_set,
        validate_module_graph_cohort_contracts,
        ContextReuseAnalysisIdentity,
        ContextReuseCohortAnalysisIdentity,
        ModuleGraphCohortContract,
    },
};

const GENERATION_KIND_V4: &str = "convex-wasm-runtime-registry-generation-v4";
const GENERATION_KIND_V5: &str = "convex-wasm-runtime-registry-generation-v5";
const GENERATION_KIND_V6_SHADOW_ONLY: &str =
    "convex-wasm-runtime-registry-generation-v6-shadow-only";
const GENERATION_KIND_V9: &str = "convex-wasm-runtime-registry-generation-v9";
const GENERATION_KIND_V10_SHADOW_ONLY: &str =
    "convex-wasm-runtime-registry-generation-v10-shadow-only";
const GRAPH_ROUTING_KIND: &str = "convex-wasm-runtime-registry-graph-routing-boundary-v1";
const SHADOW_ROUTING_KIND: &str = "convex-wasm-runtime-registry-shadow-routing-boundary-v1";
const DEPLOYMENT_BINDING_KIND: &str = "convex-wasm-deployment-module-graph-binding-v1";
const DEPLOYMENT_BINDING_KIND_V2: &str = "convex-wasm-deployment-module-graph-binding-v2";
const GRAPH_MANIFEST_KIND_V2: &str = "convex-wasm-module-graph-manifest-v2";
const GRAPH_MANIFEST_KIND_V3: &str = "convex-wasm-module-graph-manifest-v3";
const GRAPH_MANIFEST_KIND_V4: &str = "convex-wasm-module-graph-manifest-v4";
const GRAPH_MANIFEST_KIND_V5: &str = "convex-wasm-module-graph-manifest-v5";
const GRAPH_PACKAGE_KIND_V2: &str = "convex-wasm-module-graph-package-v2";
const GRAPH_PACKAGE_KIND_V3: &str = "convex-wasm-module-graph-package-v3";
const GRAPH_PACKAGE_KIND_V4: &str = "convex-wasm-module-graph-package-v4";
const GRAPH_PACKAGE_KIND_V5: &str = "convex-wasm-module-graph-package-v5";
const GRAPH_PROVENANCE_KIND_V2: &str = "convex-wasm-module-graph-provenance-v2";
const GRAPH_PROVENANCE_KIND_V3: &str = "convex-wasm-module-graph-provenance-v3";
const GRAPH_PROVENANCE_KIND_V4: &str = "convex-wasm-module-graph-provenance-v4";
const GRAPH_PROVENANCE_KIND_V5: &str = "convex-wasm-module-graph-provenance-v5";
const GRAPH_CORE_IDENTITY_AUTHORITY_KIND: &str =
    "convex-wasm-module-graph-core-identity-authority-v1";
const SHARED_MODULES_KIND: &str = "convex-wasm-module-graph-shared-modules-v1";
const LEGACY_SHARED_ROLE: &str = "common";
const SHARED_ROLE_PREFIX: &str = "shared-";
const CACHE_ENTRY_KIND: &str = "convex-wasm-artifact-cache-entry-v5";
const PIPELINE_KIND: &str = "convex-wasm-artifact-pipeline-v9";
const NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION: u64 = 2;
const MONOLITHIC_RUNTIME: &str = "monolithic-package";
const MODULE_GRAPH_RUNTIME: &str = "module-graph";
const GRAPH_CACHE_VERSION: &str = "v6";
const GRAPH_PACKAGE_FILES: [&str; 4] = [
    "COMPLETE",
    "build-provenance.json",
    "graph-manifest.json",
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

fn graph_format_contract(
    manifest_kind: &str,
    schema_version: u64,
) -> Option<(&'static str, &'static str)> {
    match (manifest_kind, schema_version) {
        (GRAPH_MANIFEST_KIND_V2, 2) => Some((GRAPH_PROVENANCE_KIND_V2, GRAPH_PACKAGE_KIND_V2)),
        (GRAPH_MANIFEST_KIND_V3, 3) => Some((GRAPH_PROVENANCE_KIND_V3, GRAPH_PACKAGE_KIND_V3)),
        (GRAPH_MANIFEST_KIND_V4, 4) => Some((GRAPH_PROVENANCE_KIND_V4, GRAPH_PACKAGE_KIND_V4)),
        (GRAPH_MANIFEST_KIND_V5, 5) => Some((GRAPH_PROVENANCE_KIND_V5, GRAPH_PACKAGE_KIND_V5)),
        _ => None,
    }
}

const MAX_COMPLETE_BYTES: u64 = 128;
const MAX_GRAPH_METADATA_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CORE_WASM_BYTES: u64 = 320 * 1024 * 1024;
const MAX_DEPLOYMENT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub(super) enum ModuleGraphRegistryError {
    #[error("module graph registry I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("module graph registry JSON is malformed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("module graph registry is invalid: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FileRecord {
    name: String,
    sha256: String,
    size: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CapabilityEntryPackageRecord {
    files: Vec<FileRecord>,
    package_key: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeploymentManifestRecord {
    deployment_sha256: String,
    sha256: String,
    size: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphRoutingBoundary {
    cutover_ready: bool,
    cutover_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_reuse_analysis: Option<ContextReuseAnalysisIdentity>,
    graph_backed_route_ids: Vec<String>,
    kind: String,
    missing_route_ids: Vec<String>,
    selected_route_ids: Vec<String>,
    selected_runtime: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ShadowOnlyRoutingBoundary {
    artifact_complete: bool,
    artifact_state: String,
    kind: String,
    route_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PackageFileRecords {
    files: Vec<FileRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphArtifactRecord {
    cache_key: String,
    files: Vec<FileRecord>,
    kind: String,
    role: String,
    sha256: String,
    size: u64,
    stage: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModuleGraphRecord {
    artifacts: Vec<GraphArtifactRecord>,
    graph_manifest_sha256: String,
    package: PackageFileRecords,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RuntimeRegistryGenerationV4 {
    capability_entry_packages: Vec<CapabilityEntryPackageRecord>,
    deployment_manifest: DeploymentManifestRecord,
    generation_sha256: String,
    graph_routing: GraphRoutingBoundary,
    kind: String,
    module_graphs: Vec<ModuleGraphRecord>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RuntimeRegistryGenerationV5 {
    deployment_manifest: DeploymentManifestRecord,
    generation_sha256: String,
    graph_routing: GraphRoutingBoundary,
    kind: String,
    module_graph_binding: DeploymentModuleGraphBinding,
    module_graph_cohorts: Vec<ModuleGraphCohortContract>,
    module_graphs: Vec<ModuleGraphRecord>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RuntimeRegistryGenerationV6 {
    admission: ShadowOnlyRuntimeRegistryAdmission,
    deployment_manifest: DeploymentManifestRecord,
    generation_sha256: String,
    kind: String,
    module_graph_binding: DeploymentModuleGraphBinding,
    module_graph_cohorts: Vec<ModuleGraphCohortContract>,
    module_graphs: Vec<ModuleGraphRecord>,
    shadow_routing: ShadowOnlyRoutingBoundary,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RuntimeRegistryGenerationV9 {
    context_reuse_analysis: ContextReuseAnalysisIdentity,
    deployment_manifest: DeploymentManifestRecord,
    generation_sha256: String,
    graph_routing: GraphRoutingBoundary,
    kind: String,
    module_graph_binding: DeploymentModuleGraphBinding,
    module_graph_cohorts: Vec<ModuleGraphCohortContract>,
    module_graphs: Vec<ModuleGraphRecord>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RuntimeRegistryGenerationV10 {
    admission: ShadowOnlyRuntimeRegistryAdmission,
    context_reuse_analysis: ContextReuseAnalysisIdentity,
    deployment_manifest: DeploymentManifestRecord,
    generation_sha256: String,
    kind: String,
    module_graph_binding: DeploymentModuleGraphBinding,
    module_graph_cohorts: Vec<ModuleGraphCohortContract>,
    module_graphs: Vec<ModuleGraphRecord>,
    shadow_routing: ShadowOnlyRoutingBoundary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeRegistryAdmission {
    PrimaryAdmitted,
    ShadowOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistryUse {
    Primary,
    Shadow,
    #[cfg(any(test, feature = "testing"))]
    CompatibilityShadow,
}

impl RegistryUse {
    pub(crate) fn is_shadow(self) -> bool {
        match self {
            Self::Primary => false,
            Self::Shadow => true,
            #[cfg(any(test, feature = "testing"))]
            Self::CompatibilityShadow => true,
        }
    }
}

impl RuntimeRegistryAdmission {
    pub(crate) fn allows(self, registry_use: RegistryUse) -> bool {
        match registry_use {
            RegistryUse::Primary => self == Self::PrimaryAdmitted,
            // A primary-admitted generation has already passed the stronger
            // publication contract and can safely serve non-authoritative
            // shadow verification. Shadow-only generations remain restricted
            // to the shadow lane.
            RegistryUse::Shadow => {
                matches!(self, Self::PrimaryAdmitted | Self::ShadowOnly)
            },
            #[cfg(any(test, feature = "testing"))]
            RegistryUse::CompatibilityShadow => true,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
enum ShadowOnlyRuntimeRegistryAdmission {
    #[serde(rename = "shadow-only")]
    ShadowOnly,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeploymentModuleGraphBinding {
    binding_sha256: String,
    cohorts: Vec<DeploymentModuleGraphCohort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_reuse_analysis: Option<ContextReuseAnalysisIdentity>,
    kind: String,
    schedule_sha256: String,
    schema_version: u64,
    source_envelope_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeploymentModuleGraphCohort {
    cohort_contract_sha256: String,
    cohort_id: String,
    graph_manifest_sha256: String,
    route_ids: Vec<String>,
}

impl RuntimeRegistryGenerationV4 {
    pub(super) fn capability_entry_packages(&self) -> &[CapabilityEntryPackageRecord] {
        &self.capability_entry_packages
    }

    pub(super) fn deployment_manifest(&self) -> &DeploymentManifestRecord {
        &self.deployment_manifest
    }

    pub(super) fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }
}

impl RuntimeRegistryGenerationV5 {
    pub(super) fn deployment_manifest(&self) -> &DeploymentManifestRecord {
        &self.deployment_manifest
    }

    pub(super) fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }

    pub(super) fn module_graph_cohorts(&self) -> &[ModuleGraphCohortContract] {
        &self.module_graph_cohorts
    }

    #[cfg(test)]
    fn module_graph_binding(&self) -> &DeploymentModuleGraphBinding {
        &self.module_graph_binding
    }
}

impl RuntimeRegistryGenerationV6 {
    pub(super) fn admission(&self) -> RuntimeRegistryAdmission {
        match self.admission {
            ShadowOnlyRuntimeRegistryAdmission::ShadowOnly => RuntimeRegistryAdmission::ShadowOnly,
        }
    }

    pub(super) fn deployment_manifest(&self) -> &DeploymentManifestRecord {
        &self.deployment_manifest
    }

    pub(super) fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }
}

impl RuntimeRegistryGenerationV9 {
    pub(super) fn deployment_manifest(&self) -> &DeploymentManifestRecord {
        &self.deployment_manifest
    }

    pub(super) fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }
}

impl RuntimeRegistryGenerationV10 {
    pub(super) fn admission(&self) -> RuntimeRegistryAdmission {
        match self.admission {
            ShadowOnlyRuntimeRegistryAdmission::ShadowOnly => RuntimeRegistryAdmission::ShadowOnly,
        }
    }

    pub(super) fn deployment_manifest(&self) -> &DeploymentManifestRecord {
        &self.deployment_manifest
    }

    pub(super) fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }
}

impl DeploymentModuleGraphBinding {
    pub(super) fn parse(value: JsonValue) -> Result<Self, ModuleGraphRegistryError> {
        let binding: Self = serde_json::from_value(value.clone())?;
        validate_deployment_binding(&binding, &value)?;
        Ok(binding)
    }

    pub(super) fn context_reuse_analysis(&self) -> Option<&ContextReuseAnalysisIdentity> {
        self.context_reuse_analysis.as_ref()
    }
}

impl CapabilityEntryPackageRecord {
    pub(super) fn files(&self) -> &[FileRecord] {
        &self.files
    }

    pub(super) fn package_key(&self) -> &str {
        &self.package_key
    }
}

impl FileRecord {
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn sha256(&self) -> &str {
        &self.sha256
    }

    pub(super) fn size(&self) -> u64 {
        self.size
    }
}

impl DeploymentManifestRecord {
    pub(super) fn deployment_sha256(&self) -> &str {
        &self.deployment_sha256
    }

    pub(super) fn sha256(&self) -> &str {
        &self.sha256
    }

    pub(super) fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug)]
pub(crate) struct AuthenticatedModuleGraphCatalog {
    graphs: BTreeMap<String, AuthenticatedModuleGraph>,
    route_graphs: BTreeMap<String, String>,
    registry_root: PathBuf,
    records: Vec<ModuleGraphRecord>,
}

impl AuthenticatedModuleGraphCatalog {
    pub(crate) fn execution_graph(
        &self,
        route_id: &str,
    ) -> Option<AuthenticatedModuleGraphExecution<'_>> {
        let graph_sha256 = self.route_graphs.get(route_id)?;
        let graph = self.graphs.get(graph_sha256)?;
        Some(AuthenticatedModuleGraphExecution {
            engine_compatibility_sha256: &graph.engine_compatibility_sha256,
            graph_sha256,
            host_abi: &graph.host_abi,
            initialization: &graph.initialization,
            modules: graph
                .modules
                .iter()
                .zip(graph.artifacts.chunks_exact(2))
                .map(|(manifest, artifacts)| {
                    let [core_wasm, aot] = artifacts else {
                        unreachable!("authenticated graph artifact pairs lost their exact shape")
                    };
                    AuthenticatedModuleGraphExecutionModule {
                        aot,
                        core_wasm,
                        manifest,
                    }
                })
                .collect(),
        })
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.graphs.len()
    }
}

#[derive(Clone, Debug)]
struct AuthenticatedModuleGraph {
    _package_root: PathBuf,
    context_reuse_analysis: Option<ContextReuseCohortAnalysisIdentity>,
    engine: GraphEngine,
    engine_compatibility_sha256: String,
    host_abi: GraphHostAbi,
    initialization: GraphInitialization,
    modules: Vec<GraphModule>,
    producer_implementation: GraphProducerImplementationIdentity,
    routing: GraphManifestRouting,
    artifacts: Vec<AuthenticatedGraphArtifact>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphProducerImplementationIdentity {
    kind: String,
    sha256: String,
}

pub(crate) struct AuthenticatedModuleGraphExecution<'a> {
    engine_compatibility_sha256: &'a str,
    graph_sha256: &'a str,
    host_abi: &'a GraphHostAbi,
    initialization: &'a GraphInitialization,
    modules: Vec<AuthenticatedModuleGraphExecutionModule<'a>>,
}

impl AuthenticatedModuleGraphExecution<'_> {
    pub(crate) fn engine_compatibility_sha256(&self) -> &str {
        self.engine_compatibility_sha256
    }

    pub(crate) fn graph_sha256(&self) -> &str {
        self.graph_sha256
    }

    pub(crate) fn host_abi(&self) -> &GraphHostAbi {
        self.host_abi
    }

    pub(crate) fn initialization(&self) -> &GraphInitialization {
        self.initialization
    }

    pub(crate) fn modules(&self) -> &[AuthenticatedModuleGraphExecutionModule<'_>] {
        &self.modules
    }
}

pub(crate) struct AuthenticatedModuleGraphExecutionModule<'a> {
    aot: &'a AuthenticatedGraphArtifact,
    core_wasm: &'a AuthenticatedGraphArtifact,
    manifest: &'a GraphModule,
}

impl AuthenticatedModuleGraphExecutionModule<'_> {
    pub(crate) fn aot_payload_available(&self) -> bool {
        self.aot.payload_available
    }

    pub(crate) fn aot_path(&self) -> &Path {
        &self.aot.artifact_path
    }

    pub(crate) fn aot_sha256(&self) -> &str {
        &self.aot.sha256
    }

    pub(crate) fn aot_size(&self) -> u64 {
        self.aot.size
    }

    pub(crate) fn core_wasm_path(&self) -> &Path {
        &self.core_wasm.artifact_path
    }

    pub(crate) fn core_wasm_size(&self) -> u64 {
        self.core_wasm.size
    }

    pub(crate) fn core_wasm_sha256(&self) -> &str {
        &self.core_wasm.sha256
    }

    pub(crate) fn role(&self) -> &str {
        &self.aot.role
    }

    pub(crate) fn contract(&self) -> &GraphModuleContract {
        &self.manifest.contract
    }

    pub(crate) fn layout(&self) -> &GraphModuleLayout {
        &self.manifest.layout
    }

    pub(crate) fn providers(&self) -> &[GraphModuleProvider] {
        &self.manifest.providers
    }
}

#[derive(Clone, Debug)]
struct AuthenticatedGraphArtifact {
    artifact_path: PathBuf,
    _cache_key: String,
    _kind: ArtifactKind,
    payload_available: bool,
    role: String,
    sha256: String,
    size: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactKind {
    CoreWasm,
    Aot,
}

impl ArtifactKind {
    fn parse(value: &str) -> Result<Self, ModuleGraphRegistryError> {
        match value {
            "coreWasm" => Ok(Self::CoreWasm),
            "aot" => Ok(Self::Aot),
            _ => invalid(format!("unsupported module graph artifact kind {value}")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::CoreWasm => "coreWasm",
            Self::Aot => "aot",
        }
    }

    fn file(self) -> &'static str {
        match self {
            Self::CoreWasm => "artifact.wasm",
            Self::Aot => "artifact.cwasm",
        }
    }

    fn maximum_bytes(self) -> u64 {
        match self {
            Self::CoreWasm => MAX_CORE_WASM_BYTES,
            Self::Aot => MAX_SERIALIZED_MODULE_BYTES,
        }
    }

    fn identity_kind(self) -> &'static str {
        match self {
            Self::CoreWasm => "convex-wasm-module-graph-core-wasm-identity-v1",
            Self::Aot => "convex-wasm-module-graph-wasmtime-aot-identity-v1",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphManifest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_reuse_analysis: Option<ContextReuseCohortAnalysisIdentity>,
    engine: GraphEngine,
    graph_manifest_sha256: String,
    host_abi: GraphHostAbi,
    initialization: GraphInitialization,
    kind: String,
    modules: Vec<GraphModule>,
    producer_implementation: GraphProducerImplementationIdentity,
    replacement: JsonValue,
    routing: GraphManifestRouting,
    schema_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    shared_shards: Option<GraphSharedShards>,
    toolchain: JsonValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphManifestV4 {
    #[serde(default)]
    context_reuse_analysis: Option<ContextReuseCohortAnalysisIdentity>,
    engine: GraphEngine,
    graph_manifest_sha256: String,
    host_abi: GraphHostAbi,
    initialization: GraphInitialization,
    kind: String,
    module_references: Vec<GraphModuleReferenceV4>,
    producer_implementation: GraphProducerImplementationIdentity,
    replacement: JsonValue,
    routing: GraphManifestRouting,
    schema_version: u64,
    shared_shards: Option<GraphSharedShards>,
    toolchain: JsonValue,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum GraphModuleReferenceV4 {
    Authority(GraphModuleAuthorityReference),
    Inline(GraphModule),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphModuleAuthorityReference {
    artifacts: GraphArtifactPair,
    authority: GraphCoreIdentityAuthorityReference,
    role: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphCoreIdentityAuthorityReference {
    kind: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphCoreIdentityAuthority {
    contract: GraphModuleContract,
    kind: String,
    layout: GraphModuleLayout,
    link: JsonValue,
    native_artifact_identity_schema_version: u64,
    object_compilation: JsonValue,
    ownership: JsonValue,
    pipeline_kind: String,
    providers: Vec<GraphModuleProvider>,
    role: String,
    shared_shard_sha256: Option<String>,
    source_provenance: JsonValue,
    toolchain: JsonValue,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphModule {
    artifacts: GraphArtifactPair,
    contract: GraphModuleContract,
    layout: GraphModuleLayout,
    link: JsonValue,
    object_compilation: JsonValue,
    ownership: JsonValue,
    providers: Vec<GraphModuleProvider>,
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    shared_shard_sha256: Option<String>,
    source_provenance: JsonValue,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphSharedShards {
    kind: String,
    shards: Vec<GraphSharedShard>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphSharedShard {
    role: String,
    shard_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphEngine {
    compatibility_sha256: String,
    config: GraphEngineConfig,
    configuration_sha256: String,
    package: JsonValue,
    revision: String,
    target: GraphEngineTarget,
    wasmtime_materials_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphEngineConfig {
    consume_fuel: bool,
    epoch_interruption: bool,
    profiling_strategy: String,
    wasm_exceptions: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphEngineTarget {
    cpu: String,
    triple: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphHostAbi {
    pub(crate) imports: Vec<GraphHostImport>,
    pub(crate) kind: String,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphHostImport {
    pub(crate) module: String,
    pub(crate) name: String,
    pub(crate) type_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphInitialization {
    pub(crate) base_heap_base: u32,
    pub(crate) base_table_size: u64,
    pub(crate) constructor_order: Vec<String>,
    pub(crate) final_memory_cursor: u32,
    pub(crate) final_table_cursor: u64,
    pub(crate) module_order: Vec<String>,
    pub(crate) relocation_order: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphModuleContract {
    pub(crate) authority: GraphModuleContractAuthority,
    pub(crate) contract_sha256: String,
    pub(crate) dylink: GraphDylinkContract,
    pub(crate) exports: Vec<GraphModuleExport>,
    pub(crate) imports: Vec<GraphModuleImport>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphModuleContractAuthority {
    pub(crate) engine_compatibility_sha256: String,
    pub(crate) inspection_sha256: String,
    pub(crate) kind: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphDylinkContract {
    pub(crate) first: bool,
    pub(crate) sha256: String,
    pub(crate) weak_imports: Vec<GraphWeakImport>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphWeakImport {
    pub(crate) module: String,
    pub(crate) name: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphModuleImport {
    pub(crate) index: usize,
    pub(crate) module: String,
    pub(crate) name: String,
    pub(crate) r#type: GraphExternalType,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphModuleExport {
    pub(crate) index: usize,
    pub(crate) name: String,
    pub(crate) r#type: GraphExternalType,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphExternalType {
    pub(crate) canonical: String,
    pub(crate) kind: String,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphModuleLayout {
    pub(crate) memory_align: u32,
    pub(crate) memory_base: u32,
    pub(crate) memory_size: u32,
    pub(crate) table_align: u32,
    pub(crate) table_base: u64,
    pub(crate) table_size: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GraphModuleProvider {
    pub(crate) consumer: String,
    pub(crate) import_index: usize,
    pub(crate) imported_module: String,
    pub(crate) imported_name: String,
    pub(crate) provider: String,
    pub(crate) provider_export: Option<String>,
    pub(crate) type_sha256: String,
    pub(crate) weak: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphArtifactPair {
    aot: GraphArtifactReference,
    core_wasm: GraphArtifactReference,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphArtifactReference {
    cache_key: String,
    sha256: String,
    size: u64,
    stage: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphManifestRouting {
    cohort_id: String,
    kind: String,
    routes: Vec<GraphRoute>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schedule_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_envelope_sha256: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphRoute {
    entry_id: String,
    entry_selector_id: String,
    entry_symbol: String,
    export_name: String,
    route_id: String,
    udf_kind: String,
    visibility: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DeploymentModuleGraphRoute {
    pub(super) cohort_contract_sha256: String,
    pub(super) entry_id: String,
    pub(super) entry_selector_id: String,
    pub(super) export_name: String,
    pub(super) route_id: String,
    pub(super) udf_kind: String,
    pub(super) visibility: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphPackageEntry {
    artifacts: BTreeMap<String, GraphArtifactPair>,
    #[serde(default)]
    context_reuse_analysis: Option<ContextReuseCohortAnalysisIdentity>,
    key: String,
    kind: String,
    manifest: FileIdentity,
    provenance: FileIdentity,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FileIdentity {
    sha256: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphProvenance {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_reuse_analysis: Option<ContextReuseCohortAnalysisIdentity>,
    engine: GraphEngine,
    host_abi: GraphHostAbi,
    identities: BTreeMap<String, GraphArtifactIdentities>,
    kind: String,
    producer_identity: JsonValue,
    routing: JsonValue,
    schema_version: u64,
    shared_shards: Option<GraphSharedShards>,
    toolchain: JsonValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphProvenanceV4 {
    #[serde(default)]
    context_reuse_analysis: Option<ContextReuseCohortAnalysisIdentity>,
    kind: String,
    producer_identity: JsonValue,
    schema_version: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GraphArtifactIdentities {
    aot: JsonValue,
    core_wasm: JsonValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CacheEntry {
    // Compiler-local physical cache hints never authorize runtime payload reuse.
    #[serde(default, rename = "admission")]
    _admission: Option<serde::de::IgnoredAny>,
    artifact_file: String,
    artifact_sha256: String,
    artifact_size: u64,
    identity: JsonValue,
    key: String,
    kind: String,
    metadata: JsonValue,
    stage: String,
}

struct AuthenticatedGraphArtifactEntry {
    record: GraphArtifactRecord,
    entry: CacheEntry,
    payload_available: bool,
    // Only stable Core references request this projection; inline leaves and AOT
    // entries do not carry Core authority. It lives for this generation load.
    core_identity_authority: Option<GraphCoreIdentityAuthority>,
}

pub(super) fn parse_generation_v4(
    value: JsonValue,
) -> Result<RuntimeRegistryGenerationV4, ModuleGraphRegistryError> {
    let generation: RuntimeRegistryGenerationV4 = serde_json::from_value(value.clone())?;
    validate_sha256("generationSha256", &generation.generation_sha256)?;
    if generation.kind != GENERATION_KIND_V4 {
        return invalid("runtime registry generation kind is not generation-v4");
    }
    if generation.graph_routing.context_reuse_analysis.is_some() {
        return invalid("generation-v4 graph routing contains a newer analysis authority");
    }
    if canonical_identity(&value, "generationSha256")? != generation.generation_sha256 {
        return invalid("generationSha256 does not authenticate the canonical generation");
    }
    validate_generation_records(
        &generation.capability_entry_packages,
        &generation.deployment_manifest,
        &generation.module_graphs,
        "generation-v4",
    )?;
    Ok(generation)
}

pub(super) fn parse_generation_v5(
    value: JsonValue,
) -> Result<RuntimeRegistryGenerationV5, ModuleGraphRegistryError> {
    let generation: RuntimeRegistryGenerationV5 = serde_json::from_value(value.clone())?;
    validate_sha256("generationSha256", &generation.generation_sha256)?;
    if generation.kind != GENERATION_KIND_V5 {
        return invalid("runtime registry generation kind is not generation-v5");
    }
    if generation.graph_routing.context_reuse_analysis.is_some() {
        return invalid("generation-v5 graph routing contains a newer analysis authority");
    }
    if canonical_identity(&value, "generationSha256")? != generation.generation_sha256 {
        return invalid("generationSha256 does not authenticate the canonical generation");
    }
    validate_module_graph_generation_records(
        &generation.deployment_manifest,
        &generation.module_graphs,
        "generation-v5",
    )?;
    validate_module_graph_cohort_contracts(&generation.module_graph_cohorts)
        .map_err(|error| ModuleGraphRegistryError::Invalid(error.to_string()))?;
    validate_deployment_binding(
        &generation.module_graph_binding,
        &serde_json::to_value(&generation.module_graph_binding)?,
    )?;
    if generation
        .module_graph_binding
        .context_reuse_analysis
        .is_some()
        || generation
            .module_graph_cohorts
            .iter()
            .any(|contract| contract.context_reuse_analysis().is_some())
    {
        return invalid("generation-v5 contains generation-v9 context-reuse authority");
    }
    Ok(generation)
}

pub(super) fn parse_generation_v6(
    value: JsonValue,
) -> Result<RuntimeRegistryGenerationV6, ModuleGraphRegistryError> {
    let generation: RuntimeRegistryGenerationV6 = serde_json::from_value(value.clone())?;
    validate_sha256("generationSha256", &generation.generation_sha256)?;
    if generation.kind != GENERATION_KIND_V6_SHADOW_ONLY {
        return invalid("runtime registry generation kind is not generation-v6-shadow-only");
    }
    if canonical_identity(&value, "generationSha256")? != generation.generation_sha256 {
        return invalid("generationSha256 does not authenticate the canonical generation");
    }
    validate_module_graph_generation_records(
        &generation.deployment_manifest,
        &generation.module_graphs,
        "generation-v6-shadow-only",
    )?;
    validate_shadow_only_routing_shape(&generation.shadow_routing)?;
    validate_module_graph_cohort_contracts(&generation.module_graph_cohorts)
        .map_err(|error| ModuleGraphRegistryError::Invalid(error.to_string()))?;
    validate_deployment_binding(
        &generation.module_graph_binding,
        &serde_json::to_value(&generation.module_graph_binding)?,
    )?;
    if generation
        .module_graph_binding
        .context_reuse_analysis
        .is_some()
        || generation
            .module_graph_cohorts
            .iter()
            .any(|contract| contract.context_reuse_analysis().is_some())
    {
        return invalid(
            "generation-v6-shadow-only contains generation-v10 context-reuse authority",
        );
    }
    Ok(generation)
}

pub(super) fn parse_generation_v9(
    value: JsonValue,
) -> Result<RuntimeRegistryGenerationV9, ModuleGraphRegistryError> {
    let generation: RuntimeRegistryGenerationV9 = serde_json::from_value(value.clone())?;
    validate_sha256("generationSha256", &generation.generation_sha256)?;
    if generation.kind != GENERATION_KIND_V9 {
        return invalid("runtime registry generation kind is not generation-v9");
    }
    if canonical_identity(&value, "generationSha256")? != generation.generation_sha256 {
        return invalid("generationSha256 does not authenticate the canonical generation");
    }
    validate_module_graph_generation_records(
        &generation.deployment_manifest,
        &generation.module_graphs,
        "generation-v9",
    )?;
    if generation.graph_routing.context_reuse_analysis.is_none() {
        return invalid("generation-v9 graph routing omits context-reuse analysis authority");
    }
    validate_context_reuse_generation_authority(
        &generation.context_reuse_analysis,
        generation.graph_routing.context_reuse_analysis.as_ref(),
        &generation.module_graph_binding,
        &generation.module_graph_cohorts,
    )?;
    Ok(generation)
}

pub(super) fn parse_generation_v10(
    value: JsonValue,
) -> Result<RuntimeRegistryGenerationV10, ModuleGraphRegistryError> {
    let generation: RuntimeRegistryGenerationV10 = serde_json::from_value(value.clone())?;
    validate_sha256("generationSha256", &generation.generation_sha256)?;
    if generation.kind != GENERATION_KIND_V10_SHADOW_ONLY {
        return invalid("runtime registry generation kind is not generation-v10-shadow-only");
    }
    if canonical_identity(&value, "generationSha256")? != generation.generation_sha256 {
        return invalid("generationSha256 does not authenticate the canonical generation");
    }
    validate_module_graph_generation_records(
        &generation.deployment_manifest,
        &generation.module_graphs,
        "generation-v10-shadow-only",
    )?;
    validate_shadow_only_routing_shape(&generation.shadow_routing)?;
    validate_context_reuse_generation_authority(
        &generation.context_reuse_analysis,
        None,
        &generation.module_graph_binding,
        &generation.module_graph_cohorts,
    )?;
    Ok(generation)
}

fn validate_context_reuse_generation_authority(
    context_reuse_analysis: &ContextReuseAnalysisIdentity,
    routing_context_reuse_analysis: Option<&ContextReuseAnalysisIdentity>,
    binding: &DeploymentModuleGraphBinding,
    contracts: &[ModuleGraphCohortContract],
) -> Result<(), ModuleGraphRegistryError> {
    context_reuse_analysis
        .validate()
        .map_err(|error| ModuleGraphRegistryError::Invalid(error.to_string()))?;
    if routing_context_reuse_analysis.is_some_and(|routing| routing != context_reuse_analysis)
        || binding.context_reuse_analysis.as_ref() != Some(context_reuse_analysis)
    {
        return invalid(
            "runtime generation context-reuse analysis differs from its routing or binding",
        );
    }
    validate_module_graph_cohort_contracts(contracts)
        .map_err(|error| ModuleGraphRegistryError::Invalid(error.to_string()))?;
    let cohort_analyses = contracts
        .iter()
        .map(ModuleGraphCohortContract::context_reuse_analysis)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            ModuleGraphRegistryError::Invalid(
                "runtime generation cohort omits context-reuse analysis authority".into(),
            )
        })?;
    validate_context_reuse_cohort_analysis_set(context_reuse_analysis, &cohort_analyses)
        .map_err(|error| ModuleGraphRegistryError::Invalid(error.to_string()))?;
    validate_deployment_binding(binding, &serde_json::to_value(binding)?)?;
    Ok(())
}

pub(super) fn authenticate_module_graph_catalog_v4(
    registry_root: &Path,
    retained_catalogs: &[&AuthenticatedModuleGraphCatalog],
    generation: &RuntimeRegistryGenerationV4,
    deployment_route_ids: &BTreeSet<String>,
) -> Result<AuthenticatedModuleGraphCatalog, ModuleGraphRegistryError> {
    validate_graph_routing_v4(&generation.graph_routing, deployment_route_ids)?;
    let (graphs, route_graphs) =
        authenticate_graph_records(registry_root, &generation.module_graphs, retained_catalogs)?;
    let backed = generation
        .graph_routing
        .graph_backed_route_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if route_graphs.keys().cloned().collect::<BTreeSet<_>>() != backed {
        return invalid("graph routing does not match the authenticated graph route closure");
    }
    // Generation-v4 authenticates availability only. Its monolithic routing
    // boundary must never produce an executable route-to-graph authority.
    Ok(AuthenticatedModuleGraphCatalog {
        graphs,
        route_graphs: BTreeMap::new(),
        registry_root: registry_root.to_owned(),
        records: generation.module_graphs.clone(),
    })
}

pub(super) fn authenticate_module_graph_catalog_v5(
    registry_root: &Path,
    retained_catalogs: &[&AuthenticatedModuleGraphCatalog],
    generation: &RuntimeRegistryGenerationV5,
    deployment_binding: &DeploymentModuleGraphBinding,
    deployment_cohorts: &[ModuleGraphCohortContract],
    deployment_routes: &BTreeMap<String, DeploymentModuleGraphRoute>,
) -> Result<AuthenticatedModuleGraphCatalog, ModuleGraphRegistryError> {
    authenticate_module_graph_catalog(
        registry_root,
        retained_catalogs,
        &generation.graph_routing,
        &generation.module_graph_binding,
        &generation.module_graph_cohorts,
        &generation.module_graphs,
        deployment_binding,
        deployment_cohorts,
        deployment_routes,
        "generation-v5",
    )
}

pub(super) fn authenticate_module_graph_catalog_v6(
    registry_root: &Path,
    retained_catalogs: &[&AuthenticatedModuleGraphCatalog],
    generation: &RuntimeRegistryGenerationV6,
    deployment_binding: &DeploymentModuleGraphBinding,
    deployment_cohorts: &[ModuleGraphCohortContract],
    deployment_routes: &BTreeMap<String, DeploymentModuleGraphRoute>,
) -> Result<AuthenticatedModuleGraphCatalog, ModuleGraphRegistryError> {
    let deployment_route_ids = deployment_routes.keys().cloned().collect::<BTreeSet<_>>();
    validate_shadow_only_routing(&generation.shadow_routing, &deployment_route_ids)?;
    if &generation.module_graph_binding != deployment_binding {
        return invalid(
            "generation-v6-shadow-only module graph binding differs from the authenticated \
             deployment-v6",
        );
    }
    if generation.module_graph_cohorts != deployment_cohorts {
        return invalid(
            "generation-v6-shadow-only cohort contracts differ from the authenticated \
             deployment-v6",
        );
    }
    let (graphs, route_graphs) =
        authenticate_graph_records(registry_root, &generation.module_graphs, retained_catalogs)?;
    let authenticated_route_ids = route_graphs.keys().cloned().collect::<BTreeSet<_>>();
    if authenticated_route_ids
        != generation
            .shadow_routing
            .route_ids
            .iter()
            .cloned()
            .collect()
    {
        return invalid(
            "shadow routing route IDs do not match the authenticated graph route closure",
        );
    }
    validate_execution_closure(
        &graphs,
        &route_graphs,
        deployment_binding,
        deployment_cohorts,
        deployment_routes,
    )?;
    Ok(AuthenticatedModuleGraphCatalog {
        graphs,
        route_graphs,
        registry_root: registry_root.to_owned(),
        records: generation.module_graphs.clone(),
    })
}

pub(super) fn authenticate_module_graph_catalog_v9(
    registry_root: &Path,
    retained_catalogs: &[&AuthenticatedModuleGraphCatalog],
    generation: &RuntimeRegistryGenerationV9,
    deployment_binding: &DeploymentModuleGraphBinding,
    deployment_cohorts: &[ModuleGraphCohortContract],
    deployment_routes: &BTreeMap<String, DeploymentModuleGraphRoute>,
) -> Result<AuthenticatedModuleGraphCatalog, ModuleGraphRegistryError> {
    authenticate_module_graph_catalog(
        registry_root,
        retained_catalogs,
        &generation.graph_routing,
        &generation.module_graph_binding,
        &generation.module_graph_cohorts,
        &generation.module_graphs,
        deployment_binding,
        deployment_cohorts,
        deployment_routes,
        "generation-v9",
    )
}

pub(super) fn authenticate_module_graph_catalog_v10(
    registry_root: &Path,
    retained_catalogs: &[&AuthenticatedModuleGraphCatalog],
    generation: &RuntimeRegistryGenerationV10,
    deployment_binding: &DeploymentModuleGraphBinding,
    deployment_cohorts: &[ModuleGraphCohortContract],
    deployment_routes: &BTreeMap<String, DeploymentModuleGraphRoute>,
) -> Result<AuthenticatedModuleGraphCatalog, ModuleGraphRegistryError> {
    let deployment_route_ids = deployment_routes.keys().cloned().collect::<BTreeSet<_>>();
    validate_shadow_only_routing(&generation.shadow_routing, &deployment_route_ids)?;
    if &generation.module_graph_binding != deployment_binding
        || generation.module_graph_cohorts != deployment_cohorts
    {
        return invalid(
            "generation-v10-shadow-only authority differs from the authenticated deployment-v8",
        );
    }
    let (graphs, route_graphs) =
        authenticate_graph_records(registry_root, &generation.module_graphs, retained_catalogs)?;
    if route_graphs.keys().cloned().collect::<BTreeSet<_>>()
        != generation
            .shadow_routing
            .route_ids
            .iter()
            .cloned()
            .collect()
    {
        return invalid(
            "shadow routing route IDs do not match the authenticated graph route closure",
        );
    }
    validate_execution_closure(
        &graphs,
        &route_graphs,
        deployment_binding,
        deployment_cohorts,
        deployment_routes,
    )?;
    Ok(AuthenticatedModuleGraphCatalog {
        graphs,
        route_graphs,
        registry_root: registry_root.to_owned(),
        records: generation.module_graphs.clone(),
    })
}

#[allow(clippy::too_many_arguments)]
fn authenticate_module_graph_catalog(
    registry_root: &Path,
    retained_catalogs: &[&AuthenticatedModuleGraphCatalog],
    graph_routing: &GraphRoutingBoundary,
    module_graph_binding: &DeploymentModuleGraphBinding,
    module_graph_cohorts: &[ModuleGraphCohortContract],
    module_graphs: &[ModuleGraphRecord],
    deployment_binding: &DeploymentModuleGraphBinding,
    deployment_cohorts: &[ModuleGraphCohortContract],
    deployment_routes: &BTreeMap<String, DeploymentModuleGraphRoute>,
    generation_description: &str,
) -> Result<AuthenticatedModuleGraphCatalog, ModuleGraphRegistryError> {
    let deployment_route_ids = deployment_routes.keys().cloned().collect::<BTreeSet<_>>();
    validate_graph_routing_v5(graph_routing, &deployment_route_ids)?;
    if module_graph_binding != deployment_binding {
        return invalid(format!(
            "{generation_description} module graph binding differs from the authenticated \
             deployment authority"
        ));
    }
    if module_graph_cohorts != deployment_cohorts {
        return invalid(format!(
            "{generation_description} cohort contracts differ from the authenticated deployment \
             authority"
        ));
    }
    let (graphs, route_graphs) =
        authenticate_graph_records(registry_root, module_graphs, retained_catalogs)?;
    validate_execution_closure(
        &graphs,
        &route_graphs,
        deployment_binding,
        deployment_cohorts,
        deployment_routes,
    )?;
    Ok(AuthenticatedModuleGraphCatalog {
        graphs,
        route_graphs,
        registry_root: registry_root.to_owned(),
        records: module_graphs.to_vec(),
    })
}

fn authenticate_graph_records(
    registry_root: &Path,
    records: &[ModuleGraphRecord],
    retained_catalogs: &[&AuthenticatedModuleGraphCatalog],
) -> Result<
    (
        BTreeMap<String, AuthenticatedModuleGraph>,
        BTreeMap<String, String>,
    ),
    ModuleGraphRegistryError,
> {
    let cache_root = registry_root.join("module-graph-cache");
    let immutable_root = cache_root.join("immutable").join(GRAPH_CACHE_VERSION);
    let packages_root = immutable_root.join("packages");
    let artifacts_root = immutable_root.join("artifacts");
    let mut physical_roots_validated = false;
    let mut graphs = BTreeMap::new();
    let mut route_graphs = BTreeMap::new();
    // Shared members occur in multiple graphs. Authenticate their immutable
    // files once during this generation load, then check each graph's bindings.
    let mut authenticated_entries = BTreeMap::new();
    for record in records {
        let retained = retained_catalogs.iter().find_map(|catalog| {
            (catalog.registry_root == registry_root
                && catalog.records.iter().any(|retained| retained == record))
            .then(|| {
                catalog
                    .graphs
                    .get(&record.graph_manifest_sha256)
                    .expect("retained graph record lost its authenticated graph")
            })
        });
        // Reuse owned metadata, not a claim that these paths remain unchanged.
        // Cold module loads still authenticate payload bytes before deserialization.
        let graph = match retained {
            Some(graph) => graph.clone(),
            None => {
                // Retained metadata does not depend on physical cache availability.
                // Validate these roots only when admitting new physical material.
                if !physical_roots_validated {
                    for directory in [
                        &cache_root,
                        &cache_root.join("immutable"),
                        &immutable_root,
                        &packages_root,
                        &artifacts_root,
                    ] {
                        validate_private_directory(directory)?;
                    }
                    physical_roots_validated = true;
                }
                authenticate_graph(
                    &packages_root,
                    &artifacts_root,
                    record,
                    &mut authenticated_entries,
                )?
            },
        };
        for route in &graph.routing.routes {
            if route_graphs
                .insert(route.route_id.clone(), record.graph_manifest_sha256.clone())
                .is_some()
            {
                return invalid(format!(
                    "route {} is present in more than one module graph",
                    route.route_id
                ));
            }
        }
        if graphs
            .insert(record.graph_manifest_sha256.clone(), graph)
            .is_some()
        {
            return invalid("runtime registry contains a duplicate module graph");
        }
    }
    Ok((graphs, route_graphs))
}

fn validate_generation_records(
    capability_entry_packages: &[CapabilityEntryPackageRecord],
    deployment_manifest: &DeploymentManifestRecord,
    module_graphs: &[ModuleGraphRecord],
    generation_description: &str,
) -> Result<(), ModuleGraphRegistryError> {
    validate_sha256(
        "deploymentManifest.deploymentSha256",
        &deployment_manifest.deployment_sha256,
    )?;
    validate_sha256("deploymentManifest.sha256", &deployment_manifest.sha256)?;
    validate_size(
        "deploymentManifest.size",
        deployment_manifest.size,
        MAX_DEPLOYMENT_BYTES,
    )?;
    if capability_entry_packages.is_empty() {
        return invalid(format!(
            "{generation_description} must contain capability-entry packages"
        ));
    }
    let mut previous = None;
    for package in capability_entry_packages {
        validate_sha256("capability-entry package key", &package.package_key)?;
        require_strictly_sorted(
            &mut previous,
            &package.package_key,
            "capability-entry packages",
        )?;
        validate_file_records(
            &package.files,
            &CAPABILITY_ENTRY_PACKAGE_FILES,
            "capability-entry package",
        )?;
    }
    validate_module_graph_records(module_graphs, generation_description)
}

fn validate_module_graph_records(
    module_graphs: &[ModuleGraphRecord],
    generation_description: &str,
) -> Result<(), ModuleGraphRegistryError> {
    if module_graphs.is_empty() {
        return invalid(format!(
            "{generation_description} must contain module graphs"
        ));
    }
    let mut previous = None;
    for graph in module_graphs {
        validate_sha256("graphManifestSha256", &graph.graph_manifest_sha256)?;
        require_strictly_sorted(&mut previous, &graph.graph_manifest_sha256, "module graphs")?;
        validate_file_records(
            &graph.package.files,
            &GRAPH_PACKAGE_FILES,
            "module graph package",
        )?;
        if graph.artifacts.len() < 4 || graph.artifacts.len() % 2 != 0 {
            return invalid("module graph must contain base and leaf Core Wasm/AOT artifact pairs");
        }
        let mut roles = Vec::with_capacity(graph.artifacts.len() / 2);
        for artifacts in graph.artifacts.chunks_exact(2) {
            let [core_wasm, aot] = artifacts else {
                unreachable!("checked module graph artifact pairs lost their exact shape")
            };
            if ArtifactKind::parse(&core_wasm.kind)? != ArtifactKind::CoreWasm
                || ArtifactKind::parse(&aot.kind)? != ArtifactKind::Aot
                || core_wasm.role != aot.role
            {
                return invalid(
                    "module graph artifacts must be ordered as per-module Core Wasm/AOT pairs",
                );
            }
            roles.push(core_wasm.role.as_str());
        }
        validate_ordered_module_roles(&roles, "module graph artifact roles")?;
        for (index, artifact) in graph.artifacts.iter().enumerate() {
            let expected_role = roles[index / 2];
            let expected_kind = if index % 2 == 0 {
                ArtifactKind::CoreWasm
            } else {
                ArtifactKind::Aot
            };
            let kind = ArtifactKind::parse(&artifact.kind)?;
            if artifact.role != expected_role || kind != expected_kind {
                return invalid(
                    "module graph artifacts must be ordered as authenticated module pairs",
                );
            }
            let expected_stage = artifact_stage(&artifact.role, kind);
            if artifact.stage != expected_stage {
                return invalid(format!(
                    "{} {} artifact stage must be {expected_stage}",
                    artifact.role,
                    kind.as_str()
                ));
            }
            validate_sha256("module graph artifact cache key", &artifact.cache_key)?;
            validate_sha256("module graph artifact SHA-256", &artifact.sha256)?;
            validate_size(
                "module graph artifact size",
                artifact.size,
                kind.maximum_bytes(),
            )?;
            let expected_files = ["COMPLETE", kind.file(), "entry.json"];
            validate_file_records(&artifact.files, &expected_files, "module graph artifact")?;
            let file = &artifact.files[1];
            if file.sha256 != artifact.sha256 || file.size != artifact.size {
                return invalid("module graph artifact record disagrees with its artifact file");
            }
        }
    }
    Ok(())
}

fn validate_module_graph_generation_records(
    deployment_manifest: &DeploymentManifestRecord,
    module_graphs: &[ModuleGraphRecord],
    generation_description: &str,
) -> Result<(), ModuleGraphRegistryError> {
    validate_sha256(
        "deploymentManifest.deploymentSha256",
        &deployment_manifest.deployment_sha256,
    )?;
    validate_sha256("deploymentManifest.sha256", &deployment_manifest.sha256)?;
    validate_size(
        "deploymentManifest.size",
        deployment_manifest.size,
        MAX_DEPLOYMENT_BYTES,
    )?;
    validate_module_graph_records(module_graphs, generation_description)
}

fn validate_shadow_only_routing_shape(
    routing: &ShadowOnlyRoutingBoundary,
) -> Result<(), ModuleGraphRegistryError> {
    if routing.kind != SHADOW_ROUTING_KIND {
        return invalid("shadow routing kind is unsupported");
    }
    if !routing.artifact_complete || routing.artifact_state != "shadow-only" {
        return invalid("shadow routing must describe complete shadow-only artifacts");
    }
    validate_sorted_sha256s("shadowRouting.routeIds", &routing.route_ids)
}

fn validate_shadow_only_routing(
    routing: &ShadowOnlyRoutingBoundary,
    deployment_route_ids: &BTreeSet<String>,
) -> Result<(), ModuleGraphRegistryError> {
    validate_shadow_only_routing_shape(routing)?;
    if routing.route_ids.iter().cloned().collect::<BTreeSet<_>>() != *deployment_route_ids {
        return invalid("shadow routing routes differ from the deployment route closure");
    }
    Ok(())
}

fn validate_graph_routing_v4(
    routing: &GraphRoutingBoundary,
    deployment_route_ids: &BTreeSet<String>,
) -> Result<(), ModuleGraphRegistryError> {
    if routing.kind != GRAPH_ROUTING_KIND {
        return invalid("runtime registry graph routing kind is unsupported");
    }
    if routing.selected_runtime != MONOLITHIC_RUNTIME {
        return invalid("generation-v4 must retain the monolithic selected runtime");
    }
    for (field, route_ids) in [
        ("selectedRouteIds", &routing.selected_route_ids),
        ("graphBackedRouteIds", &routing.graph_backed_route_ids),
        ("missingRouteIds", &routing.missing_route_ids),
    ] {
        validate_sorted_sha256s(field, route_ids)?;
    }
    let selected = routing
        .selected_route_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let backed = routing
        .graph_backed_route_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let missing = routing
        .missing_route_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if &selected != deployment_route_ids {
        return invalid("graph routing selected routes differ from the deployment route closure");
    }
    if !backed.is_subset(&selected)
        || selected
            .difference(&backed)
            .cloned()
            .collect::<BTreeSet<_>>()
            != missing
    {
        return invalid("graph routing does not describe the exact selected-route closure");
    }
    let cutover_ready = missing.is_empty();
    let expected_state = if cutover_ready {
        "graph-complete-deployment-reference-required"
    } else {
        "graph-incomplete"
    };
    if routing.cutover_ready != cutover_ready || routing.cutover_state != expected_state {
        return invalid("graph routing cutover state is inconsistent with its missing routes");
    }
    Ok(())
}

fn validate_graph_routing_v5(
    routing: &GraphRoutingBoundary,
    deployment_route_ids: &BTreeSet<String>,
) -> Result<(), ModuleGraphRegistryError> {
    if routing.kind != GRAPH_ROUTING_KIND {
        return invalid("runtime registry graph routing kind is unsupported");
    }
    for (field, route_ids) in [
        ("selectedRouteIds", &routing.selected_route_ids),
        ("graphBackedRouteIds", &routing.graph_backed_route_ids),
        ("missingRouteIds", &routing.missing_route_ids),
    ] {
        validate_sorted_sha256s(field, route_ids)?;
    }
    let selected = routing
        .selected_route_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let backed = routing
        .graph_backed_route_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if selected.is_empty()
        || &selected != deployment_route_ids
        || backed != selected
        || !routing.missing_route_ids.is_empty()
        || !routing.cutover_ready
        || routing.cutover_state != "module-graph-authorized"
        || routing.selected_runtime != MODULE_GRAPH_RUNTIME
    {
        return invalid(
            "runtime registry graph routing does not authorize the exact deployment graph closure",
        );
    }
    Ok(())
}

fn validate_deployment_binding(
    binding: &DeploymentModuleGraphBinding,
    value: &JsonValue,
) -> Result<(), ModuleGraphRegistryError> {
    let context_reuse_format = match (binding.kind.as_str(), binding.schema_version) {
        (DEPLOYMENT_BINDING_KIND, 1) => binding.context_reuse_analysis.is_none(),
        (DEPLOYMENT_BINDING_KIND_V2, 2) => binding.context_reuse_analysis.is_some(),
        _ => false,
    };
    if !context_reuse_format || binding.cohorts.is_empty() {
        return invalid("deployment module graph binding kind, schema, or cohorts are unsupported");
    }
    if let Some(context_reuse_analysis) = &binding.context_reuse_analysis {
        context_reuse_analysis
            .validate()
            .map_err(|error| ModuleGraphRegistryError::Invalid(error.to_string()))?;
    }
    for (field, digest) in [
        ("bindingSha256", binding.binding_sha256.as_str()),
        ("scheduleSha256", binding.schedule_sha256.as_str()),
        (
            "sourceEnvelopeSha256",
            binding.source_envelope_sha256.as_str(),
        ),
    ] {
        validate_sha256(field, digest)?;
    }
    if canonical_identity(value, "bindingSha256")? != binding.binding_sha256 {
        return invalid("deployment module graph binding identity is invalid");
    }
    let mut previous_cohort = None;
    let mut graph_manifest_sha256s = BTreeSet::new();
    let mut route_ids = BTreeSet::new();
    for cohort in &binding.cohorts {
        validate_sha256(
            "module graph binding cohortContractSha256",
            &cohort.cohort_contract_sha256,
        )?;
        validate_sha256("module graph binding cohortId", &cohort.cohort_id)?;
        validate_sha256(
            "module graph binding graphManifestSha256",
            &cohort.graph_manifest_sha256,
        )?;
        require_strictly_sorted(
            &mut previous_cohort,
            &cohort.cohort_id,
            "module graph binding cohorts",
        )?;
        if !graph_manifest_sha256s.insert(&cohort.graph_manifest_sha256) {
            return invalid("deployment module graph binding contains a duplicate graph");
        }
        if cohort.route_ids.is_empty() {
            return invalid("deployment module graph binding cohort contains no routes");
        }
        validate_sorted_sha256s("module graph binding cohort routeIds", &cohort.route_ids)?;
        for route_id in &cohort.route_ids {
            if !route_ids.insert(route_id) {
                return invalid("deployment module graph binding contains a duplicate route");
            }
        }
    }
    Ok(())
}

fn validate_execution_closure(
    graphs: &BTreeMap<String, AuthenticatedModuleGraph>,
    route_graphs: &BTreeMap<String, String>,
    binding: &DeploymentModuleGraphBinding,
    contracts: &[ModuleGraphCohortContract],
    deployment_routes: &BTreeMap<String, DeploymentModuleGraphRoute>,
) -> Result<(), ModuleGraphRegistryError> {
    let binding_route_ids = binding
        .cohorts
        .iter()
        .flat_map(|cohort| cohort.route_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let deployment_route_ids = deployment_routes.keys().cloned().collect::<BTreeSet<_>>();
    if binding_route_ids != deployment_route_ids {
        return invalid(
            "deployment module graph binding does not cover the exact selected-route closure",
        );
    }
    if route_graphs.keys().cloned().collect::<BTreeSet<_>>() != deployment_route_ids {
        return invalid("authenticated module graphs do not cover the selected deployment routes");
    }
    if graphs.len() != binding.cohorts.len() {
        return invalid("deployment binding and authenticated graph counts differ");
    }

    let cohorts = binding
        .cohorts
        .iter()
        .map(|cohort| (cohort.cohort_id.as_str(), cohort))
        .collect::<BTreeMap<_, _>>();
    let contracts = contracts
        .iter()
        .map(|contract| (contract.cohort_id(), contract))
        .collect::<BTreeMap<_, _>>();
    if contracts.len() != cohorts.len() {
        return invalid("deployment binding and cohort contract counts differ");
    }
    let mut authenticated_cohorts = BTreeSet::new();
    for (graph_manifest_sha256, graph) in graphs {
        let routing = &graph.routing;
        let cohort = cohorts.get(routing.cohort_id.as_str()).ok_or_else(|| {
            ModuleGraphRegistryError::Invalid(
                "authenticated module graph is outside the deployment binding".into(),
            )
        })?;
        let contract = contracts.get(routing.cohort_id.as_str()).ok_or_else(|| {
            ModuleGraphRegistryError::Invalid(
                "authenticated module graph has no cohort contract".into(),
            )
        })?;
        if !authenticated_cohorts.insert(routing.cohort_id.as_str()) {
            return invalid("authenticated module graphs contain a duplicate cohort");
        }
        let (contract_producer_kind, contract_producer_sha256) = contract.producer_implementation();
        if cohort.cohort_contract_sha256 != contract.cohort_contract_sha256()
            || cohort.graph_manifest_sha256 != *graph_manifest_sha256
            || (routing.kind == "convex-wasm-module-graph-routing-v1"
                && (routing.schedule_sha256.as_deref() != Some(binding.schedule_sha256.as_str())
                    || routing.source_envelope_sha256.as_deref()
                        != Some(binding.source_envelope_sha256.as_str())))
            || contract.schedule_sha256() != binding.schedule_sha256
            || contract.source_envelope_sha256() != binding.source_envelope_sha256
            || graph.producer_implementation.kind.as_str() != contract_producer_kind
            || graph.producer_implementation.sha256.as_str() != contract_producer_sha256
            || graph.engine.compatibility_sha256 != contract.engine_compatibility_sha256()
            || graph.engine.configuration_sha256 != contract.engine_configuration_sha256()
            || graph.engine.revision != contract.engine_revision()
            || graph.engine.target.cpu != contract.engine_target_cpu()
            || graph.engine.target.triple != contract.engine_target_triple()
            || graph.engine.package != contract.precompiler_material_identity_json()
            || graph.context_reuse_analysis.as_ref() != contract.context_reuse_analysis()
        {
            return invalid(
                "authenticated module graph identity or provenance differs from deployment \
                 authority",
            );
        }
        let graph_route_ids = routing
            .routes
            .iter()
            .map(|route| route.route_id.clone())
            .collect::<Vec<_>>();
        if graph_route_ids != cohort.route_ids {
            return invalid("authenticated graph routes differ from their bound cohort closure");
        }
        let contract_route_ids = contract
            .routes()
            .iter()
            .map(|route| route.route_id().to_owned())
            .collect::<Vec<_>>();
        if contract_route_ids != cohort.route_ids {
            return invalid("authenticated cohort contract routes differ from their bound closure");
        }
        for (graph_route, contract_route) in routing.routes.iter().zip(contract.routes()) {
            let deployment_route =
                deployment_routes
                    .get(&graph_route.route_id)
                    .ok_or_else(|| {
                        ModuleGraphRegistryError::Invalid(
                            "authenticated graph route is outside the selected deployment".into(),
                        )
                    })?;
            if graph_route.entry_id != deployment_route.entry_id
                || graph_route.entry_selector_id != deployment_route.entry_selector_id
                || graph_route.entry_symbol != contract_route.entry_symbol()
                || graph_route.export_name != deployment_route.export_name
                || graph_route.route_id != deployment_route.route_id
                || graph_route.udf_kind != deployment_route.udf_kind
                || graph_route.visibility != deployment_route.visibility
                || deployment_route.cohort_contract_sha256 != contract.cohort_contract_sha256()
                || contract_route.entry_id() != deployment_route.entry_id
                || contract_route.entry_selector_id() != deployment_route.entry_selector_id
                || contract_route.export_name() != deployment_route.export_name
                || contract_route.route_id() != deployment_route.route_id
                || match contract_route.udf_kind() {
                    super::wasm_udf_manifest::UdfKind::Query => "query",
                    super::wasm_udf_manifest::UdfKind::Mutation => "mutation",
                } != deployment_route.udf_kind
                || contract_route.visibility() != deployment_route.visibility
            {
                return invalid(
                    "authenticated graph route differs from its selected deployment route",
                );
            }
        }
    }
    Ok(())
}

fn graph_artifact_reference_matches_record(
    reference: &GraphArtifactReference,
    record: &GraphArtifactRecord,
) -> bool {
    reference.cache_key == record.cache_key
        && reference.sha256 == record.sha256
        && reference.size == record.size
        && reference.stage == record.stage
}

fn load_graph_core_identity_authority<'a>(
    artifacts_root: &Path,
    record: &GraphArtifactRecord,
    reference: &GraphArtifactReference,
    role: &str,
    authenticated_entries: &'a mut BTreeMap<(String, String), AuthenticatedGraphArtifactEntry>,
) -> Result<&'a GraphCoreIdentityAuthority, ModuleGraphRegistryError> {
    if record.kind != "coreWasm"
        || record.role != role
        || !graph_artifact_reference_matches_record(reference, record)
    {
        return invalid("module graph Core Wasm authority disagrees with its registry record");
    }
    let admitted = authenticate_artifact_entry(
        artifacts_root,
        record,
        ArtifactKind::CoreWasm,
        authenticated_entries,
    )?;
    if admitted.core_identity_authority.is_none() {
        let identity: GraphCoreIdentityAuthority =
            serde_json::from_value(admitted.entry.identity.clone())?;
        if identity.kind != ArtifactKind::CoreWasm.identity_kind()
            || identity.role != role
            || identity.pipeline_kind != PIPELINE_KIND
            || identity.native_artifact_identity_schema_version
                != NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION
            || admitted.entry.metadata != serde_json::json!({"contract": &identity.contract})
        {
            return invalid("module graph Core Wasm authority identity contract is invalid");
        }
        admitted.core_identity_authority = Some(identity);
    }
    Ok(admitted
        .core_identity_authority
        .as_ref()
        .expect("Core authority was admitted before reuse"))
}

fn parse_graph_manifest(
    artifacts_root: &Path,
    record: &ModuleGraphRecord,
    value: JsonValue,
    authenticated_entries: &mut BTreeMap<(String, String), AuthenticatedGraphArtifactEntry>,
) -> Result<GraphManifest, ModuleGraphRegistryError> {
    let kind = value.get("kind").and_then(JsonValue::as_str);
    let schema_version = value.get("schemaVersion").and_then(JsonValue::as_u64);
    let reference_manifest = matches!(
        (kind, schema_version),
        (Some(GRAPH_MANIFEST_KIND_V4), Some(4)) | (Some(GRAPH_MANIFEST_KIND_V5), Some(5))
    );
    if !reference_manifest {
        return Ok(serde_json::from_value(value)?);
    }
    let stored: GraphManifestV4 = serde_json::from_value(value)?;
    let context_reuse_format = match (stored.kind.as_str(), stored.schema_version) {
        (GRAPH_MANIFEST_KIND_V4, 4) => stored.context_reuse_analysis.is_none(),
        (GRAPH_MANIFEST_KIND_V5, 5) => stored.context_reuse_analysis.is_some(),
        _ => false,
    };
    if !context_reuse_format {
        return invalid("module graph reference manifest kind or schema is unsupported");
    }
    if stored.module_references.len() * 2 != record.artifacts.len() {
        return invalid("module graph manifest-v4 does not cover its registry artifacts");
    }
    let reference_count = stored.module_references.len();
    let mut modules = Vec::with_capacity(reference_count);
    for (index, reference) in stored.module_references.into_iter().enumerate() {
        let core_record = &record.artifacts[index * 2];
        let aot_record = &record.artifacts[index * 2 + 1];
        let module = match reference {
            GraphModuleReferenceV4::Authority(reference) => {
                if index + 1 == reference_count
                    || reference.authority.kind != GRAPH_CORE_IDENTITY_AUTHORITY_KIND
                    || reference.role == "leaf"
                    || !graph_artifact_reference_matches_record(
                        &reference.artifacts.core_wasm,
                        core_record,
                    )
                    || !graph_artifact_reference_matches_record(
                        &reference.artifacts.aot,
                        aot_record,
                    )
                {
                    return invalid("module graph stable-module authority reference is invalid");
                }
                let identity = load_graph_core_identity_authority(
                    artifacts_root,
                    core_record,
                    &reference.artifacts.core_wasm,
                    &reference.role,
                    authenticated_entries,
                )?;
                GraphModule {
                    artifacts: reference.artifacts,
                    contract: identity.contract.clone(),
                    layout: identity.layout.clone(),
                    link: identity.link.clone(),
                    object_compilation: identity.object_compilation.clone(),
                    ownership: identity.ownership.clone(),
                    providers: identity.providers.clone(),
                    role: identity.role.clone(),
                    shared_shard_sha256: identity.shared_shard_sha256.clone(),
                    source_provenance: identity.source_provenance.clone(),
                }
            },
            GraphModuleReferenceV4::Inline(module) => {
                if index + 1 != reference_count
                    || module.role != "leaf"
                    || !graph_artifact_reference_matches_record(
                        &module.artifacts.core_wasm,
                        core_record,
                    )
                    || !graph_artifact_reference_matches_record(&module.artifacts.aot, aot_record)
                {
                    return invalid("module graph inline leaf reference is invalid");
                }
                module
            },
        };
        modules.push(module);
    }
    Ok(GraphManifest {
        context_reuse_analysis: stored.context_reuse_analysis,
        engine: stored.engine,
        graph_manifest_sha256: stored.graph_manifest_sha256,
        host_abi: stored.host_abi,
        initialization: stored.initialization,
        kind: stored.kind,
        modules,
        producer_implementation: stored.producer_implementation,
        replacement: stored.replacement,
        routing: stored.routing,
        schema_version: stored.schema_version,
        shared_shards: stored.shared_shards,
        toolchain: stored.toolchain,
    })
}

fn parse_graph_provenance(
    value: JsonValue,
    manifest: &GraphManifest,
) -> Result<GraphProvenance, ModuleGraphRegistryError> {
    if !matches!(
        (manifest.kind.as_str(), manifest.schema_version),
        (GRAPH_MANIFEST_KIND_V4, 4) | (GRAPH_MANIFEST_KIND_V5, 5)
    ) {
        return Ok(serde_json::from_value(value)?);
    }
    let stored: GraphProvenanceV4 = serde_json::from_value(value)?;
    let context_reuse_format = match (stored.kind.as_str(), stored.schema_version) {
        (GRAPH_PROVENANCE_KIND_V4, 4) => stored.context_reuse_analysis.is_none(),
        (GRAPH_PROVENANCE_KIND_V5, 5) => stored.context_reuse_analysis.is_some(),
        _ => false,
    };
    if !context_reuse_format || stored.context_reuse_analysis != manifest.context_reuse_analysis {
        return invalid("module graph reference provenance kind or authority is unsupported");
    }
    let identities = manifest
        .modules
        .iter()
        .map(|module| {
            Ok((
                module.role.clone(),
                GraphArtifactIdentities {
                    aot: expected_artifact_identity(manifest, module, ArtifactKind::Aot)?,
                    core_wasm: expected_artifact_identity(
                        manifest,
                        module,
                        ArtifactKind::CoreWasm,
                    )?,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, ModuleGraphRegistryError>>()?;
    Ok(GraphProvenance {
        context_reuse_analysis: stored.context_reuse_analysis,
        engine: manifest.engine.clone(),
        host_abi: manifest.host_abi.clone(),
        identities,
        kind: stored.kind,
        producer_identity: stored.producer_identity,
        routing: serde_json::to_value(&manifest.routing)?,
        schema_version: stored.schema_version,
        shared_shards: manifest.shared_shards.clone(),
        toolchain: manifest.toolchain.clone(),
    })
}

fn authenticate_graph(
    packages_root: &Path,
    artifacts_root: &Path,
    record: &ModuleGraphRecord,
    authenticated_entries: &mut BTreeMap<(String, String), AuthenticatedGraphArtifactEntry>,
) -> Result<AuthenticatedModuleGraph, ModuleGraphRegistryError> {
    let package_root = packages_root.join(&record.graph_manifest_sha256);
    validate_private_directory(&package_root)?;
    validate_directory_entries(&package_root, &GRAPH_PACKAGE_FILES, "module graph package")?;
    authenticate_file_records(&package_root, &record.package.files, "module graph package")?;
    let complete = read_file(&package_root.join("COMPLETE"), MAX_COMPLETE_BYTES)?;
    if complete != format!("{}\n", record.graph_manifest_sha256).as_bytes() {
        return invalid("module graph package completion marker is invalid");
    }
    let (manifest_bytes, manifest_value) = read_canonical_json(
        &package_root.join("graph-manifest.json"),
        MAX_GRAPH_METADATA_BYTES,
    )?;
    let (provenance_bytes, provenance_value) = read_canonical_json(
        &package_root.join("build-provenance.json"),
        MAX_GRAPH_METADATA_BYTES,
    )?;
    let (_, package_entry_value) = read_canonical_json(
        &package_root.join("package-entry.json"),
        MAX_GRAPH_METADATA_BYTES,
    )?;
    let manifest = parse_graph_manifest(
        artifacts_root,
        record,
        manifest_value.clone(),
        authenticated_entries,
    )?;
    let provenance = parse_graph_provenance(provenance_value.clone(), &manifest)?;
    let package_entry: GraphPackageEntry = serde_json::from_value(package_entry_value)?;
    validate_graph_metadata(
        &record.graph_manifest_sha256,
        &manifest_bytes,
        &manifest_value,
        &provenance_bytes,
        &provenance_value,
        &manifest,
        &provenance,
        &package_entry,
    )?;
    let producer_implementation = manifest.producer_implementation.clone();

    let mut artifacts = Vec::with_capacity(record.artifacts.len());
    for (index, artifact_record) in record.artifacts.iter().enumerate() {
        let module = &manifest.modules[index / 2];
        let kind = ArtifactKind::parse(&artifact_record.kind)?;
        let reference = match kind {
            ArtifactKind::CoreWasm => &module.artifacts.core_wasm,
            ArtifactKind::Aot => &module.artifacts.aot,
        };
        if reference.cache_key != artifact_record.cache_key
            || reference.sha256 != artifact_record.sha256
            || reference.size != artifact_record.size
            || reference.stage != artifact_record.stage
        {
            return invalid(format!(
                "{} {} registry artifact disagrees with the graph manifest",
                artifact_record.role,
                kind.as_str()
            ));
        }
        let identity = match kind {
            ArtifactKind::CoreWasm => &provenance.identities[&artifact_record.role].core_wasm,
            ArtifactKind::Aot => &provenance.identities[&artifact_record.role].aot,
        };
        artifacts.push(authenticate_artifact(
            artifacts_root,
            artifact_record,
            &artifact_record.role,
            kind,
            identity,
            &module.contract,
            authenticated_entries,
        )?);
    }
    Ok(AuthenticatedModuleGraph {
        _package_root: package_root,
        context_reuse_analysis: manifest.context_reuse_analysis.clone(),
        engine: manifest.engine.clone(),
        engine_compatibility_sha256: manifest.engine.compatibility_sha256.clone(),
        host_abi: manifest.host_abi.clone(),
        initialization: manifest.initialization.clone(),
        modules: manifest.modules.clone(),
        producer_implementation,
        routing: manifest.routing,
        artifacts,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_graph_metadata(
    expected_graph_sha256: &str,
    manifest_bytes: &[u8],
    manifest_value: &JsonValue,
    provenance_bytes: &[u8],
    provenance_value: &JsonValue,
    manifest: &GraphManifest,
    provenance: &GraphProvenance,
    package_entry: &GraphPackageEntry,
) -> Result<(), ModuleGraphRegistryError> {
    let Some((expected_provenance_kind, expected_package_kind)) =
        graph_format_contract(&manifest.kind, manifest.schema_version)
    else {
        return invalid("module graph manifest kind or schema is unsupported");
    };
    validate_sha256("graphManifestSha256", &manifest.graph_manifest_sha256)?;
    if manifest.graph_manifest_sha256 != expected_graph_sha256
        || canonical_identity(manifest_value, "graphManifestSha256")? != expected_graph_sha256
    {
        return invalid("graphManifestSha256 does not authenticate the graph manifest");
    }
    let module_roles = manifest
        .modules
        .iter()
        .map(|module| module.role.as_str())
        .collect::<Vec<_>>();
    validate_ordered_module_roles(&module_roles, "module graph manifest roles")?;
    validate_shared_shard_declarations(
        &module_roles,
        manifest.shared_shards.as_ref(),
        provenance.shared_shards.as_ref(),
    )?;
    if manifest_value
        .get("sharedShards")
        .is_some_and(JsonValue::is_null)
        || provenance_value
            .get("sharedShards")
            .is_some_and(JsonValue::is_null)
    {
        return invalid("module graph shared shards must be omitted or an object");
    }
    for module in &manifest.modules {
        for (kind, reference) in [
            (ArtifactKind::CoreWasm, &module.artifacts.core_wasm),
            (ArtifactKind::Aot, &module.artifacts.aot),
        ] {
            validate_sha256("graph artifact cache key", &reference.cache_key)?;
            validate_sha256("graph artifact SHA-256", &reference.sha256)?;
            validate_size("graph artifact size", reference.size, kind.maximum_bytes())?;
            if reference.stage != artifact_stage(&module.role, kind) {
                return invalid("module graph manifest artifact stage is invalid");
            }
        }
        validate_shared_shard_module_identity(module)?;
        validate_source_provenance(&module.source_provenance, &module.role)?;
        validate_contract(
            &module.contract,
            &module.role,
            &manifest.engine.compatibility_sha256,
        )?;
        validate_module_layout(&module.layout, &module.role)?;
    }
    validate_graph_routes(&manifest.routing, manifest.schema_version)?;
    validate_graph_wide_metadata(manifest)?;
    validate_module_providers(&manifest.modules, &manifest.host_abi)?;
    if provenance.kind != expected_provenance_kind
        || provenance.schema_version != manifest.schema_version
    {
        return invalid("module graph provenance kind or schema is unsupported");
    }
    let context_reuse_format = match (manifest.kind.as_str(), manifest.schema_version) {
        (GRAPH_MANIFEST_KIND_V5, 5) => manifest.context_reuse_analysis.is_some(),
        (GRAPH_MANIFEST_KIND_V2, 2) | (GRAPH_MANIFEST_KIND_V3, 3) | (GRAPH_MANIFEST_KIND_V4, 4) => {
            manifest.context_reuse_analysis.is_none()
        },
        _ => false,
    };
    if !context_reuse_format {
        return invalid("module graph context-reuse authority requires manifest-v5");
    }
    match &manifest.context_reuse_analysis {
        Some(context_reuse_analysis) => {
            context_reuse_analysis
                .validate()
                .map_err(|error| ModuleGraphRegistryError::Invalid(error.to_string()))?;
            if provenance.context_reuse_analysis.as_ref() != Some(context_reuse_analysis)
                || package_entry.context_reuse_analysis.as_ref() != Some(context_reuse_analysis)
            {
                return invalid(
                    "module graph context-reuse analysis differs across package authorities",
                );
            }
        },
        None => {
            if provenance.context_reuse_analysis.is_some()
                || package_entry.context_reuse_analysis.is_some()
            {
                return invalid("legacy module graph contains context-reuse analysis authority");
            }
        },
    }
    let role_set = module_roles.into_iter().collect::<BTreeSet<_>>();
    if provenance
        .identities
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != role_set
    {
        return invalid("module graph provenance identities do not cover the exact graph roles");
    }
    let manifest_routing = serde_json::to_value(&manifest.routing)?;
    if provenance.engine != manifest.engine
        || provenance.host_abi != manifest.host_abi
        || provenance.routing != manifest_routing
        || provenance.shared_shards != manifest.shared_shards
        || provenance.toolchain != manifest.toolchain
    {
        return invalid("module graph provenance disagrees with graph-wide manifest identities");
    }
    if normalized_graph_producer_implementation(&provenance.producer_identity)?
        != manifest.producer_implementation
    {
        return invalid(
            "module graph provenance producer identity disagrees with manifest producer \
             implementation",
        );
    }
    let _ = (
        &manifest.initialization,
        &manifest.replacement,
        &provenance.producer_identity,
    );
    if package_entry.kind != expected_package_kind || package_entry.key != expected_graph_sha256 {
        return invalid("module graph package entry kind or key is invalid");
    }
    if package_entry.artifacts.len() != manifest.modules.len() {
        return invalid("module graph package entry artifacts do not cover every role");
    }
    for module in &manifest.modules {
        if package_entry.artifacts.get(&module.role) != Some(&module.artifacts) {
            return invalid("module graph package entry artifacts disagree with the manifest");
        }
        let identities = &provenance.identities[&module.role];
        let expected_core = expected_artifact_identity(manifest, module, ArtifactKind::CoreWasm)?;
        let expected_aot = expected_artifact_identity(manifest, module, ArtifactKind::Aot)?;
        if identities.core_wasm != expected_core || identities.aot != expected_aot {
            return invalid(format!(
                "{} module provenance does not match its producer-v2 artifact identities",
                module.role
            ));
        }
    }
    validate_file_identity("manifest", &package_entry.manifest, manifest_bytes)?;
    validate_file_identity("provenance", &package_entry.provenance, provenance_bytes)?;
    Ok(())
}

fn expected_artifact_identity(
    manifest: &GraphManifest,
    module: &GraphModule,
    kind: ArtifactKind,
) -> Result<JsonValue, ModuleGraphRegistryError> {
    let toolchain = require_object_keys(
        &manifest.toolchain,
        &["aot", "core", "staticHermesCBundleMemberCompilation"],
        "module graph toolchain",
    )?;
    match kind {
        ArtifactKind::CoreWasm => {
            let mut identity = serde_json::json!({
                "contract": module.contract,
                "kind": kind.identity_kind(),
                "layout": module.layout,
                "link": module.link,
                "nativeArtifactIdentitySchemaVersion": NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION,
                "objectCompilation": module.object_compilation,
                "ownership": module.ownership,
                "pipelineKind": PIPELINE_KIND,
                "providers": module.providers,
                "role": module.role,
                "sourceProvenance": module.source_provenance,
                "toolchain": {
                    "core": toolchain["core"],
                    "staticHermesCBundleMemberCompilation":
                        toolchain["staticHermesCBundleMemberCompilation"],
                },
            });
            if let Some(shared_shard_sha256) = &module.shared_shard_sha256 {
                let object = identity
                    .as_object_mut()
                    .expect("constructed Core Wasm identity is an object");
                object.insert(
                    "sharedShardSha256".into(),
                    JsonValue::String(shared_shard_sha256.clone()),
                );
            }
            if module.role == "leaf" {
                let replacement = require_object_keys(
                    &manifest.replacement,
                    &[
                        "leafInvalidation",
                        "leafInvalidationSha256",
                        "previousGraphManifestSha256",
                        "replace",
                        "stable",
                    ],
                    "module graph replacement",
                )?;
                let object = identity
                    .as_object_mut()
                    .expect("constructed Core Wasm identity is an object");
                object.insert(
                    "leafInvalidation".into(),
                    replacement["leafInvalidation"].clone(),
                );
                object.insert(
                    "leafInvalidationSha256".into(),
                    replacement["leafInvalidationSha256"].clone(),
                );
                object.insert("routing".into(), serde_json::to_value(&manifest.routing)?);
            }
            Ok(identity)
        },
        ArtifactKind::Aot => Ok(serde_json::json!({
            "coreWasm": {
                "cacheKey": module.artifacts.core_wasm.cache_key,
                "sha256": module.artifacts.core_wasm.sha256,
                "size": module.artifacts.core_wasm.size,
            },
            "engine": manifest.engine,
            "kind": kind.identity_kind(),
            "nativeArtifactIdentitySchemaVersion": NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION,
            "pipelineKind": PIPELINE_KIND,
            "role": module.role,
            "toolchain": toolchain["aot"],
        })),
    }
}

fn normalized_graph_producer_implementation(
    producer_identity: &JsonValue,
) -> Result<GraphProducerImplementationIdentity, ModuleGraphRegistryError> {
    let (kind, sha256) = normalized_capability_producer_implementation_identity(producer_identity)
        .map_err(|_| {
            ModuleGraphRegistryError::Invalid("module graph producer identity is invalid".into())
        })?;
    Ok(GraphProducerImplementationIdentity { kind, sha256 })
}

fn validate_graph_wide_metadata(manifest: &GraphManifest) -> Result<(), ModuleGraphRegistryError> {
    let engine = &manifest.engine;
    for (field, digest) in [
        ("compatibilitySha256", engine.compatibility_sha256.as_str()),
        ("configurationSha256", engine.configuration_sha256.as_str()),
        (
            "wasmtimeMaterialsSha256",
            engine.wasmtime_materials_sha256.as_str(),
        ),
    ] {
        validate_sha256(&format!("module graph engine {field}"), digest)?;
    }
    if canonical_sha256(&serde_json::to_value(&engine.config)?)? != engine.configuration_sha256 {
        return invalid("module graph engine configuration SHA-256 is invalid");
    }
    if !engine.config.consume_fuel
        || !engine.config.epoch_interruption
        || engine.config.profiling_strategy != "perf-map"
        || !engine.config.wasm_exceptions
        || engine.target.cpu != "baseline"
        || engine.target.triple != "x86_64-unknown-linux-gnu"
        || engine.revision.is_empty()
    {
        return invalid("module graph engine is not executable by the pinned runtime");
    }
    let host_abi = &manifest.host_abi;
    if host_abi.kind != "convex-wasm-module-graph-host-abi-v1" {
        return invalid("module graph host ABI kind is unsupported");
    }
    validate_sha256("module graph host ABI SHA-256", &host_abi.sha256)?;
    let mut previous = None;
    for imported in &host_abi.imports {
        if imported.module.is_empty() || imported.name.is_empty() {
            return invalid("module graph host ABI contains an empty import name");
        }
        validate_sha256("module graph host ABI type SHA-256", &imported.type_sha256)?;
        let key = format!("{}\0{}", imported.module, imported.name);
        if previous
            .replace(key.clone())
            .is_some_and(|value| value >= key)
        {
            return invalid("module graph host ABI imports must be unique and sorted");
        }
    }
    if canonical_sha256(&serde_json::json!({
        "imports": host_abi.imports,
        "kind": host_abi.kind,
    }))? != host_abi.sha256
    {
        return invalid("module graph host ABI SHA-256 is invalid");
    }
    validate_initialization(&manifest.initialization, &manifest.modules)?;
    let replacement = require_object_keys(
        &manifest.replacement,
        &[
            "leafInvalidation",
            "leafInvalidationSha256",
            "previousGraphManifestSha256",
            "replace",
            "stable",
        ],
        "module graph replacement",
    )?;
    let leaf_sha256 = string_field(
        replacement,
        "leafInvalidationSha256",
        "module graph replacement",
    )?;
    validate_sha256("leaf invalidation SHA-256", leaf_sha256)?;
    if canonical_sha256(&replacement["leafInvalidation"])? != leaf_sha256 {
        return invalid("module graph leaf invalidation SHA-256 is invalid");
    }
    require_object_keys(
        &manifest.toolchain,
        &["aot", "core", "staticHermesCBundleMemberCompilation"],
        "module graph toolchain",
    )?;
    Ok(())
}

fn validate_source_provenance(
    value: &JsonValue,
    role: &str,
) -> Result<(), ModuleGraphRegistryError> {
    let provenance = require_object_keys(
        value,
        &["kind", "payload", "sha256"],
        "module source provenance",
    )?;
    if string_field(provenance, "kind", "module source provenance")?
        != "convex-wasm-module-role-source-provenance-v1"
    {
        return invalid("module source provenance kind is unsupported");
    }
    let digest = string_field(provenance, "sha256", "module source provenance")?;
    validate_sha256("module source provenance SHA-256", digest)?;
    if canonical_sha256(&serde_json::json!({
        "kind": provenance["kind"],
        "payload": provenance["payload"],
    }))? != digest
    {
        return invalid(format!(
            "{role} module source provenance SHA-256 is invalid"
        ));
    }
    Ok(())
}

fn shared_shard_sha256_for_role(role: &str) -> Option<&str> {
    let shard_sha256 = role.strip_prefix(SHARED_ROLE_PREFIX)?;
    (shard_sha256.len() == 64
        && shard_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some(shard_sha256)
}

fn validate_shared_shard_module_identity(
    module: &GraphModule,
) -> Result<(), ModuleGraphRegistryError> {
    validate_shared_shard_identity(&module.role, module.shared_shard_sha256.as_deref())
}

fn validate_shared_shard_identity(
    role: &str,
    shared_shard_sha256: Option<&str>,
) -> Result<(), ModuleGraphRegistryError> {
    match shared_shard_sha256_for_role(role) {
        Some(expected) if shared_shard_sha256 == Some(expected) => Ok(()),
        Some(_) => {
            invalid("shared module shard identity does not match its content-addressed role")
        },
        None if shared_shard_sha256.is_some() => {
            invalid("only content-addressed shared modules may declare a shard identity")
        },
        None => Ok(()),
    }
}

fn validate_shared_shard_declarations(
    module_roles: &[&str],
    shared_shards: Option<&GraphSharedShards>,
    provenance_shared_shards: Option<&GraphSharedShards>,
) -> Result<(), ModuleGraphRegistryError> {
    if shared_shards != provenance_shared_shards {
        return invalid("module graph provenance shared shards disagree with the graph manifest");
    }
    let shared_roles = &module_roles[1..module_roles.len() - 1];
    let Some(shared_shards) = shared_shards else {
        if module_roles != ["base", LEGACY_SHARED_ROLE, "leaf"] {
            return invalid(
                "variable shared modules require authenticated shared-shard declarations",
            );
        }
        return Ok(());
    };
    if shared_shards.kind != SHARED_MODULES_KIND {
        return invalid("module graph shared-shard declaration kind is unsupported");
    }
    if shared_shards.shards.len() != shared_roles.len() {
        return invalid("module graph shared-shard declarations do not cover the shared modules");
    }
    for (shard, role) in shared_shards.shards.iter().zip(shared_roles) {
        validate_sha256("module graph shared shard SHA-256", &shard.shard_sha256)?;
        if shard.role != format!("{SHARED_ROLE_PREFIX}{}", shard.shard_sha256)
            || shard.role != *role
        {
            return invalid(
                "module graph shared-shard declaration does not bind its content identity",
            );
        }
    }
    Ok(())
}

fn validate_contract(
    contract: &GraphModuleContract,
    role: &str,
    engine_compatibility_sha256: &str,
) -> Result<(), ModuleGraphRegistryError> {
    validate_sha256("module contract SHA-256", &contract.contract_sha256)?;
    validate_sha256(
        "module contract inspection SHA-256",
        &contract.authority.inspection_sha256,
    )?;
    if contract.authority.kind != "convex-wasm-wasmtime-module-contract-v1"
        || contract.authority.engine_compatibility_sha256 != engine_compatibility_sha256
        || !contract.dylink.first
    {
        return invalid(format!("{role} module contract authority is invalid"));
    }
    validate_sha256("module contract dylink SHA-256", &contract.dylink.sha256)?;
    let mut previous_weak: Option<String> = None;
    for imported in &contract.dylink.weak_imports {
        let key = format!("{}\0{}", imported.module, imported.name);
        if previous_weak
            .replace(key.clone())
            .is_some_and(|value| value >= key)
        {
            return invalid("module dylink weak imports must be unique and sorted");
        }
    }
    let mut export_names = BTreeSet::new();
    for (index, imported) in contract.imports.iter().enumerate() {
        if imported.index != index || imported.module.is_empty() || imported.name.is_empty() {
            return invalid(format!("{role} module imports lost their exact order"));
        }
        validate_external_type(&imported.r#type, role)?;
    }
    for (index, exported) in contract.exports.iter().enumerate() {
        if exported.index != index
            || exported.name.is_empty()
            || !export_names.insert(&exported.name)
        {
            return invalid(format!("{role} module exports are not uniquely ordered"));
        }
        validate_external_type(&exported.r#type, role)?;
    }
    if canonical_sha256(&serde_json::json!({
        "authority": contract.authority,
        "dylink": contract.dylink,
        "exports": contract.exports,
        "imports": contract.imports,
    }))? != contract.contract_sha256
    {
        return invalid("module contract SHA-256 is invalid");
    }
    Ok(())
}

fn validate_external_type(
    r#type: &GraphExternalType,
    role: &str,
) -> Result<(), ModuleGraphRegistryError> {
    if r#type.canonical.is_empty()
        || !matches!(
            r#type.kind.as_str(),
            "func" | "global" | "memory" | "table" | "tag"
        )
    {
        return invalid(format!(
            "{role} module contains an unsupported external type"
        ));
    }
    validate_sha256("module external type SHA-256", &r#type.sha256)?;
    if sha256(r#type.canonical.as_bytes()) != r#type.sha256 {
        return invalid(format!("{role} module external type SHA-256 is invalid"));
    }
    Ok(())
}

fn validate_module_layout(
    layout: &GraphModuleLayout,
    role: &str,
) -> Result<(), ModuleGraphRegistryError> {
    if layout.memory_align > 31 || layout.table_align > 31 {
        return invalid(format!("{role} module layout alignment is unsupported"));
    }
    if role == "base"
        && (layout.memory_base != 0
            || layout.memory_size != 0
            || layout.table_base != 0
            || layout.table_size != 0)
    {
        return invalid("base module layout contains side-module reservations");
    }
    Ok(())
}

fn validate_initialization(
    initialization: &GraphInitialization,
    modules: &[GraphModule],
) -> Result<(), ModuleGraphRegistryError> {
    let module_order = modules
        .iter()
        .map(|module| module.role.as_str())
        .collect::<Vec<_>>();
    validate_ordered_module_roles(&module_order, "module graph initialization modules")?;
    if initialization.base_heap_base == 0
        || initialization.base_table_size == 0
        || !initialization
            .module_order
            .iter()
            .map(String::as_str)
            .eq(module_order.iter().copied())
        || !initialization
            .constructor_order
            .iter()
            .map(String::as_str)
            .eq(module_order[1..].iter().copied())
        || !initialization
            .relocation_order
            .iter()
            .map(String::as_str)
            .eq(module_order[1..].iter().copied())
    {
        return invalid("module graph initialization order or base layout is invalid");
    }
    let mut memory_cursor = initialization.base_heap_base;
    let mut table_cursor = initialization.base_table_size;
    for module in &modules[1..] {
        memory_cursor = align_u32(memory_cursor, module.layout.memory_align)?;
        table_cursor = align_u64(table_cursor, module.layout.table_align)?;
        if module.layout.memory_base != memory_cursor || module.layout.table_base != table_cursor {
            return invalid(format!(
                "{} module layout is not the deterministic next graph placement",
                module.role
            ));
        }
        memory_cursor = memory_cursor
            .checked_add(module.layout.memory_size)
            .ok_or_else(|| {
                ModuleGraphRegistryError::Invalid("graph memory cursor overflowed".into())
            })?;
        table_cursor = table_cursor
            .checked_add(module.layout.table_size)
            .ok_or_else(|| {
                ModuleGraphRegistryError::Invalid("graph table cursor overflowed".into())
            })?;
    }
    if initialization.final_memory_cursor != memory_cursor
        || initialization.final_table_cursor != table_cursor
    {
        return invalid("module graph initialization final cursors are invalid");
    }
    Ok(())
}

fn align_u32(value: u32, exponent: u32) -> Result<u32, ModuleGraphRegistryError> {
    let alignment = 1u32 << exponent;
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| {
            ModuleGraphRegistryError::Invalid("graph memory alignment overflowed".into())
        })
}

fn align_u64(value: u64, exponent: u32) -> Result<u64, ModuleGraphRegistryError> {
    let alignment = 1u64 << exponent;
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| ModuleGraphRegistryError::Invalid("graph table alignment overflowed".into()))
}

fn validate_module_providers(
    modules: &[GraphModule],
    host_abi: &GraphHostAbi,
) -> Result<(), ModuleGraphRegistryError> {
    let module_roles = modules
        .iter()
        .map(|module| module.role.as_str())
        .collect::<Vec<_>>();
    validate_ordered_module_roles(&module_roles, "module graph providers")?;
    let host_imports = host_abi
        .imports
        .iter()
        .map(|imported| ((imported.module.as_str(), imported.name.as_str()), imported))
        .collect::<BTreeMap<_, _>>();
    for (consumer_index, module) in modules.iter().enumerate() {
        if module.providers.len() != module.contract.imports.len() {
            return invalid(format!(
                "{} module providers do not cover every exact import",
                module.role
            ));
        }
        for (import_index, (provider, imported)) in module
            .providers
            .iter()
            .zip(&module.contract.imports)
            .enumerate()
        {
            if provider.consumer != module.role
                || provider.import_index != import_index
                || provider.imported_module != imported.module
                || provider.imported_name != imported.name
                || provider.type_sha256 != imported.r#type.sha256
            {
                return invalid(format!(
                    "{} module provider {import_index} disagrees with its exact import",
                    module.role
                ));
            }
            validate_sha256("module provider type SHA-256", &provider.type_sha256)?;
            match provider.provider.as_str() {
                "host" => {
                    let allowed =
                        host_imports.get(&(imported.module.as_str(), imported.name.as_str()));
                    if consumer_index != 0
                        || provider.provider_export.is_some()
                        || provider.weak
                        || allowed
                            .is_none_or(|allowed| allowed.type_sha256 != imported.r#type.sha256)
                    {
                        return invalid("only base imports may bind the closed host ABI");
                    }
                },
                "loader" => {
                    if consumer_index == 0
                        || imported.module != "env"
                        || !matches!(imported.name.as_str(), "__memory_base" | "__table_base")
                        || imported.r#type.canonical != "global(i32,const)"
                        || provider.provider_export.is_some()
                        || provider.weak
                    {
                        return invalid("module graph contains an invalid relocation-base binding");
                    }
                },
                "weak-zero" => {
                    let declared = module.contract.dylink.weak_imports.iter().any(|candidate| {
                        candidate.module == imported.module && candidate.name == imported.name
                    });
                    if consumer_index == 0
                        || provider.provider_export.is_some()
                        || !provider.weak
                        || !matches!(imported.module.as_str(), "GOT.func" | "GOT.mem")
                        || imported.r#type.canonical != "global(i32,var)"
                        || !declared
                    {
                        return invalid("module graph contains an invalid weak-zero binding");
                    }
                },
                role => {
                    let provider_index = module_roles
                        .iter()
                        .position(|candidate| *candidate == role)
                        .ok_or_else(|| {
                            ModuleGraphRegistryError::Invalid(format!(
                                "module graph provider {role} is not an authenticated module"
                            ))
                        })?;
                    let self_got = provider_index == consumer_index
                        && matches!(imported.module.as_str(), "GOT.func" | "GOT.mem");
                    if provider_index > consumer_index
                        || (provider_index == consumer_index && !self_got)
                        || provider.provider_export.is_none()
                        || provider.weak
                    {
                        return invalid("module graph provider creates a cycle");
                    }
                    let export_name = provider
                        .provider_export
                        .as_deref()
                        .expect("validated provider export is present");
                    let exported = modules[provider_index]
                        .contract
                        .exports
                        .iter()
                        .find(|exported| exported.name == export_name)
                        .ok_or_else(|| {
                            ModuleGraphRegistryError::Invalid(
                                "module graph provider export is missing".into(),
                            )
                        })?;
                    let direct = exported.r#type.sha256 == imported.r#type.sha256;
                    let got_function = imported.module == "GOT.func"
                        && imported.r#type.canonical == "global(i32,var)"
                        && exported.r#type.kind == "func";
                    let got_memory = imported.module == "GOT.mem"
                        && imported.r#type.canonical == "global(i32,var)"
                        && exported.r#type.kind == "global"
                        && exported.r#type.canonical.starts_with("global(i32,");
                    let base_resource = provider_index == 0
                        && imported.module == "env"
                        && matches!(
                            imported.name.as_str(),
                            "memory" | "__indirect_function_table"
                        )
                        && exported.r#type.kind == imported.r#type.kind;
                    if !direct && !got_function && !got_memory && !base_resource {
                        return invalid("module graph provider export type does not match");
                    }
                },
            }
        }
        // dylink.0 records linker weak-symbol metadata, not only live Core
        // Wasm imports. A weak symbol may resolve to a concrete provider, or
        // the final link may remove its import. The weak-zero branch above is
        // the only binding that requires matching dylink evidence.
    }
    Ok(())
}

fn validate_graph_routes(
    routing: &GraphManifestRouting,
    graph_schema_version: u64,
) -> Result<(), ModuleGraphRegistryError> {
    if routing.routes.is_empty() {
        return invalid("module graph routing kind is unsupported or has no routes");
    }
    validate_sha256("routing.cohortId", &routing.cohort_id)?;
    match (graph_schema_version, routing.kind.as_str()) {
        (2, "convex-wasm-module-graph-routing-v1") => {
            let schedule_sha256 = routing.schedule_sha256.as_deref().ok_or_else(|| {
                ModuleGraphRegistryError::Invalid(
                    "module graph routing-v1 is missing scheduleSha256".into(),
                )
            })?;
            let source_envelope_sha256 =
                routing.source_envelope_sha256.as_deref().ok_or_else(|| {
                    ModuleGraphRegistryError::Invalid(
                        "module graph routing-v1 is missing sourceEnvelopeSha256".into(),
                    )
                })?;
            validate_sha256("routing.scheduleSha256", schedule_sha256)?;
            validate_sha256("routing.sourceEnvelopeSha256", source_envelope_sha256)?;
        },
        (3 | 4 | 5, "convex-wasm-module-graph-routing-v2") => {
            // Routing-v2 omits deployment-global identity so exact packages can be reused.
            // The deployment binding and cohort contract still authenticate
            // both values.
            if routing.schedule_sha256.is_some() || routing.source_envelope_sha256.is_some() {
                return invalid("module graph routing-v2 contains deployment-global identity");
            }
        },
        _ => return invalid("module graph routing kind does not match its graph schema"),
    }
    let mut previous = None;
    for route in &routing.routes {
        validate_sha256("routing route entryId", &route.entry_id)?;
        validate_sha256("routing routeId", &route.route_id)?;
        require_strictly_sorted(&mut previous, &route.route_id, "module graph routes")?;
        if route.entry_selector_id.len() != 16
            || !route
                .entry_selector_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || route.entry_symbol.is_empty()
            || route.export_name.is_empty()
            || !matches!(route.udf_kind.as_str(), "query" | "mutation")
            || !matches!(route.visibility.as_str(), "public" | "internal")
        {
            return invalid("module graph route contains an unsupported field value");
        }
    }
    Ok(())
}

fn authenticate_artifact_entry<'a>(
    artifacts_root: &Path,
    record: &GraphArtifactRecord,
    kind: ArtifactKind,
    authenticated_entries: &'a mut BTreeMap<(String, String), AuthenticatedGraphArtifactEntry>,
) -> Result<&'a mut AuthenticatedGraphArtifactEntry, ModuleGraphRegistryError> {
    if record.kind != kind.as_str() {
        return invalid("module graph artifact record has the wrong kind");
    }
    let entry_root = artifacts_root.join(&record.stage).join(&record.cache_key);
    let expected_files = ["COMPLETE", kind.file(), "entry.json"];
    validate_file_records(&record.files, &expected_files, "module graph artifact")?;
    if record.files[1].sha256 != record.sha256 || record.files[1].size != record.size {
        return invalid("module graph artifact record disagrees with its artifact file");
    }
    validate_size(
        "module graph artifact size",
        record.size,
        kind.maximum_bytes(),
    )?;
    match authenticated_entries.entry((record.stage.clone(), record.cache_key.clone())) {
        std::collections::btree_map::Entry::Occupied(retained) => {
            if retained.get().record != *record {
                return invalid(
                    "shared module graph artifact records disagree within a generation",
                );
            }
            Ok(retained.into_mut())
        },
        std::collections::btree_map::Entry::Vacant(pending) => {
            validate_private_directory(&artifacts_root.join(&record.stage))?;
            validate_private_directory(&entry_root)?;
            let payload_available = match validate_directory_entries(
                &entry_root,
                &expected_files,
                "module graph artifact",
            ) {
                Ok(()) => true,
                Err(error) if kind == ArtifactKind::Aot => {
                    validate_directory_entries(
                        &entry_root,
                        &["COMPLETE", "entry.json"],
                        "module graph artifact without reconstructible AOT payload",
                    )
                    .map_err(|_| error)?;
                    false
                },
                Err(error) => return Err(error),
            };
            // A missing AOT payload is reconstructed from the authenticated
            // Core Wasm before generation readiness. Every present file remains
            // authenticated here, so corruption is not treated as absence.
            for file in &record.files {
                if payload_available || file.name != kind.file() {
                    authenticate_file_record(&entry_root, file, "module graph artifact")?;
                }
            }
            let complete = read_file(&entry_root.join("COMPLETE"), MAX_COMPLETE_BYTES)?;
            if complete != format!("{}\n", record.cache_key).as_bytes() {
                return invalid("module graph artifact completion marker is invalid");
            }
            let (_, entry_value) =
                read_canonical_json(&entry_root.join("entry.json"), MAX_GRAPH_METADATA_BYTES)?;
            let entry: CacheEntry = serde_json::from_value(entry_value)?;
            if entry.kind != CACHE_ENTRY_KIND
                || entry.key != record.cache_key
                || entry.stage != record.stage
                || entry.artifact_file != kind.file()
                || entry.artifact_sha256 != record.sha256
                || entry.artifact_size != record.size
            {
                return invalid(
                    "module graph cache entry does not authenticate its registry record",
                );
            }
            let identity_object = entry.identity.as_object().ok_or_else(|| {
                ModuleGraphRegistryError::Invalid("artifact identity is not an object".into())
            })?;
            if identity_object.get("kind").and_then(JsonValue::as_str) != Some(kind.identity_kind())
                || identity_object.get("role").and_then(JsonValue::as_str)
                    != Some(record.role.as_str())
            {
                return invalid("module graph artifact identity has the wrong kind or role");
            }
            // The immutable record is checked on every reuse; its identity key
            // needs authentication only when this generation first admits it.
            let cache_key_identity = serde_json::json!({
                "identity": entry.identity,
                "kind": PIPELINE_KIND,
                "stage": entry.stage,
            });
            if canonical_sha256(&cache_key_identity)? != record.cache_key {
                return invalid("module graph artifact identity does not match its cache key");
            }
            Ok(pending.insert(AuthenticatedGraphArtifactEntry {
                record: record.clone(),
                entry,
                payload_available,
                core_identity_authority: None,
            }))
        },
    }
}

fn authenticate_artifact(
    artifacts_root: &Path,
    record: &GraphArtifactRecord,
    role: &str,
    kind: ArtifactKind,
    expected_identity: &JsonValue,
    expected_contract: &GraphModuleContract,
    authenticated_entries: &mut BTreeMap<(String, String), AuthenticatedGraphArtifactEntry>,
) -> Result<AuthenticatedGraphArtifact, ModuleGraphRegistryError> {
    let authenticated =
        authenticate_artifact_entry(artifacts_root, record, kind, authenticated_entries)?;
    let entry = &authenticated.entry;
    if entry.identity != *expected_identity {
        return invalid("module graph cache entry does not authenticate its registry record");
    }
    if record.role != role {
        return invalid("module graph artifact identity has the wrong kind or role");
    }
    if kind == ArtifactKind::CoreWasm {
        let expected_metadata = serde_json::json!({"contract": expected_contract});
        if entry.metadata != expected_metadata {
            return invalid("Core Wasm artifact metadata disagrees with its module contract");
        }
    } else {
        let identity = entry
            .identity
            .as_object()
            .expect("validated identity object");
        let engine = identity
            .get("engine")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| {
                ModuleGraphRegistryError::Invalid("AOT identity has no engine object".into())
            })?;
        let expected_metadata = serde_json::json!({
            "engineCompatibilitySha256": engine.get("compatibilitySha256"),
            "engineConfig": engine.get("config"),
            "kind": "convex-wasm-wasmtime-engine-identity",
            "target": engine.get("target"),
        });
        if entry.metadata != expected_metadata {
            return invalid("AOT artifact metadata disagrees with its engine identity");
        }
    }
    let artifact_path = artifacts_root
        .join(&record.stage)
        .join(&record.cache_key)
        .join(kind.file());
    Ok(AuthenticatedGraphArtifact {
        artifact_path,
        _cache_key: record.cache_key.clone(),
        _kind: kind,
        payload_available: authenticated.payload_available,
        role: role.to_owned(),
        sha256: record.sha256.clone(),
        size: record.size,
    })
}

fn validate_file_records(
    files: &[FileRecord],
    expected_names: &[&str],
    description: &str,
) -> Result<(), ModuleGraphRegistryError> {
    if files.len() != expected_names.len()
        || files
            .iter()
            .map(|file| file.name.as_str())
            .ne(expected_names.iter().copied())
    {
        return invalid(format!(
            "{description} does not name its exact ordered file set"
        ));
    }
    for file in files {
        validate_sha256(&format!("{description} file SHA-256"), &file.sha256)?;
        let maximum = match file.name.as_str() {
            "COMPLETE" => MAX_COMPLETE_BYTES,
            "artifact.wasm" | "module.wasm" => MAX_CORE_WASM_BYTES,
            "artifact.cwasm" | "module.cwasm" => MAX_SERIALIZED_MODULE_BYTES,
            _ => MAX_GRAPH_METADATA_BYTES,
        };
        validate_size(&format!("{description} file size"), file.size, maximum)?;
    }
    Ok(())
}

fn authenticate_file_records(
    root: &Path,
    records: &[FileRecord],
    description: &str,
) -> Result<(), ModuleGraphRegistryError> {
    for record in records {
        authenticate_file_record(root, record, description)?;
    }
    Ok(())
}

fn authenticate_file_record(
    root: &Path,
    record: &FileRecord,
    description: &str,
) -> Result<(), ModuleGraphRegistryError> {
    let maximum = match record.name.as_str() {
        "COMPLETE" => MAX_COMPLETE_BYTES,
        "artifact.wasm" | "module.wasm" => MAX_CORE_WASM_BYTES,
        "artifact.cwasm" | "module.cwasm" => MAX_SERIALIZED_MODULE_BYTES,
        _ => MAX_GRAPH_METADATA_BYTES,
    };
    authenticate_file(
        &root.join(&record.name),
        maximum,
        record.size,
        &record.sha256,
        description,
    )
}

fn validate_file_identity(
    field: &str,
    identity: &FileIdentity,
    bytes: &[u8],
) -> Result<(), ModuleGraphRegistryError> {
    validate_sha256(&format!("package entry {field} SHA-256"), &identity.sha256)?;
    if identity.size != bytes.len() as u64 || identity.sha256 != sha256(bytes) {
        return invalid(format!(
            "package entry {field} does not authenticate its file"
        ));
    }
    Ok(())
}

fn validate_sorted_sha256s(field: &str, values: &[String]) -> Result<(), ModuleGraphRegistryError> {
    let mut previous = None;
    for value in values {
        validate_sha256(field, value)?;
        require_strictly_sorted(&mut previous, value, field)?;
    }
    Ok(())
}

fn require_strictly_sorted<'a>(
    previous: &mut Option<&'a str>,
    current: &'a str,
    field: &str,
) -> Result<(), ModuleGraphRegistryError> {
    if previous
        .replace(current)
        .is_some_and(|value| value >= current)
    {
        return invalid(format!("{field} must be unique and sorted"));
    }
    Ok(())
}

fn validate_ordered_module_roles(
    roles: &[&str],
    description: &str,
) -> Result<(), ModuleGraphRegistryError> {
    if roles.len() < 2 || roles.first() != Some(&"base") || roles.last() != Some(&"leaf") {
        return invalid(format!(
            "{description} must start with base and end with leaf"
        ));
    }
    let mut unique = BTreeSet::new();
    for (index, role) in roles.iter().enumerate() {
        if role.is_empty()
            || role.len() > 128
            || !role
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return invalid(format!(
                "{description} contains an invalid module identifier"
            ));
        }
        if !unique.insert(*role) {
            return invalid(format!(
                "{description} contains a duplicate module identifier"
            ));
        }
        if matches!(*role, "host" | "loader" | "weak-zero") {
            return invalid(format!("{description} uses a reserved provider identifier"));
        }
        if index > 0 && index + 1 < roles.len() && matches!(*role, "base" | "leaf") {
            return invalid(format!(
                "{description} places a boundary module among shared modules"
            ));
        }
        if index > 0
            && index + 1 < roles.len()
            && *role != LEGACY_SHARED_ROLE
            && shared_shard_sha256_for_role(role).is_none()
        {
            return invalid(format!(
                "{description} contains an unsupported shared module identifier"
            ));
        }
    }
    Ok(())
}

fn artifact_stage(role: &str, kind: ArtifactKind) -> String {
    match kind {
        ArtifactKind::CoreWasm => format!("module-graph-{role}-core-wasm"),
        ArtifactKind::Aot => format!("module-graph-{role}-wasmtime-aot"),
    }
}

fn validate_sha256(field: &str, value: &str) -> Result<(), ModuleGraphRegistryError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid(format!("{field} must be a lowercase SHA-256"));
    }
    Ok(())
}

fn require_object_keys<'a>(
    value: &'a JsonValue,
    expected: &[&str],
    description: &str,
) -> Result<&'a serde_json::Map<String, JsonValue>, ModuleGraphRegistryError> {
    let object = value.as_object().ok_or_else(|| {
        ModuleGraphRegistryError::Invalid(format!("{description} is not an object"))
    })?;
    let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return invalid(format!("{description} has missing or unexpected fields"));
    }
    Ok(object)
}

fn string_field<'a>(
    object: &'a serde_json::Map<String, JsonValue>,
    field: &str,
    description: &str,
) -> Result<&'a str, ModuleGraphRegistryError> {
    object
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            ModuleGraphRegistryError::Invalid(format!("{description}.{field} must be a string"))
        })
}

fn validate_size(field: &str, value: u64, maximum: u64) -> Result<(), ModuleGraphRegistryError> {
    if value == 0 || value > maximum {
        return invalid(format!("{field} must be between 1 and {maximum}"));
    }
    Ok(())
}

fn validate_private_directory(path: &Path) -> Result<(), ModuleGraphRegistryError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| io_error(path, source))?;
    if !metadata.file_type().is_dir() || !has_exact_permissions(&metadata, 0o700) {
        return invalid(format!("{} is not a private directory", path.display()));
    }
    Ok(())
}

fn validate_directory_entries(
    path: &Path,
    expected: &[&str],
    description: &str,
) -> Result<(), ModuleGraphRegistryError> {
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(path).map_err(|source| io_error(path, source))? {
        let entry = entry.map_err(|source| io_error(path, source))?;
        let name = entry.file_name().into_string().map_err(|_| {
            ModuleGraphRegistryError::Invalid(format!("{description} has a non-UTF-8 entry"))
        })?;
        actual.insert(name);
    }
    let expected = expected.iter().map(|name| (*name).to_owned()).collect();
    if actual != expected {
        return invalid(format!("{description} has missing or unexpected files"));
    }
    Ok(())
}

fn authenticate_file(
    path: &Path,
    maximum: u64,
    expected_size: u64,
    expected_sha256: &str,
    description: &str,
) -> Result<(), ModuleGraphRegistryError> {
    validate_sha256(&format!("{description} SHA-256"), expected_sha256)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|source| io_error(path, source))?;
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    if !metadata.file_type().is_file()
        || !has_exact_permissions(&metadata, 0o600)
        || metadata.len() == 0
        || metadata.len() > maximum
        || metadata.len() != expected_size
    {
        return invalid(format!(
            "{description} is not a bounded authenticated private file"
        ));
    }
    // Core Wasm artifacts can be hundreds of MiB. Registry authentication only
    // needs their digest, so do not make their full size an unadmitted heap
    // allocation during generation loading.
    let mut digest = Sha256::new();
    let bytes_read = std::io::copy(&mut BufReader::new(file), &mut digest)
        .map_err(|source| io_error(path, source))?;
    if bytes_read != expected_size || format!("{:x}", digest.finalize()) != expected_sha256 {
        return invalid(format!(
            "{description} differs from its authenticated bytes"
        ));
    }
    Ok(())
}

fn read_canonical_json(
    path: &Path,
    maximum: u64,
) -> Result<(Vec<u8>, JsonValue), ModuleGraphRegistryError> {
    let bytes = read_file(path, maximum)?;
    let payload = bytes.strip_suffix(b"\n").ok_or_else(|| {
        ModuleGraphRegistryError::Invalid(format!("{} has no final newline", path.display()))
    })?;
    let value: JsonValue = serde_json::from_slice(payload)?;
    if canonical_bytes(&value)? != payload {
        return invalid(format!("{} is not canonical JSON", path.display()));
    }
    Ok((bytes, value))
}

fn read_file(path: &Path, maximum: u64) -> Result<Vec<u8>, ModuleGraphRegistryError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|source| io_error(path, source))?;
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    if !metadata.file_type().is_file()
        || !has_exact_permissions(&metadata, 0o600)
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return invalid(format!("{} is not a bounded private file", path.display()));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| io_error(path, source))?;
    if bytes.len() as u64 != metadata.len() {
        return invalid(format!("{} changed while it was read", path.display()));
    }
    Ok(bytes)
}

fn canonical_identity(
    value: &JsonValue,
    identity_field: &str,
) -> Result<String, ModuleGraphRegistryError> {
    let mut value = value.clone();
    let object = value.as_object_mut().ok_or_else(|| {
        ModuleGraphRegistryError::Invalid("canonical identity is not an object".into())
    })?;
    if object.remove(identity_field).is_none() {
        return invalid(format!("canonical identity is missing {identity_field}"));
    }
    canonical_sha256(&value)
}

fn canonical_sha256(value: &JsonValue) -> Result<String, ModuleGraphRegistryError> {
    Ok(sha256(&canonical_bytes(value)?))
}

fn canonical_bytes(value: &JsonValue) -> Result<Vec<u8>, ModuleGraphRegistryError> {
    let mut bytes = Vec::new();
    write_canonical_json(value, &mut bytes).map_err(|source| ModuleGraphRegistryError::Io {
        path: PathBuf::from("<canonical-json>"),
        source,
    })?;
    Ok(bytes)
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
                write_canonical_json(&values[key], output)?;
            }
            output.push(b'}');
        },
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn invalid<T>(message: impl Into<String>) -> Result<T, ModuleGraphRegistryError> {
    Err(ModuleGraphRegistryError::Invalid(message.into()))
}

fn io_error(path: &Path, source: std::io::Error) -> ModuleGraphRegistryError {
    ModuleGraphRegistryError::Io {
        path: path.to_owned(),
        source,
    }
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

#[cfg(test)]
pub(crate) fn materialize_registry_v5_test_fixture(
    fixture: &JsonValue,
    root: &Path,
) -> anyhow::Result<()> {
    tests::materialize_fixture(fixture, root)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        collections::BTreeMap,
        env,
        fs::{
            self,
            OpenOptions,
        },
        io::Write,
        path::{
            Path,
            PathBuf,
        },
    };

    use anyhow::Context as _;
    use serde_json::{
        json,
        Value as JsonValue,
    };
    use tempfile::TempDir;

    use super::{
        authenticate_module_graph_catalog_v4,
        authenticate_module_graph_catalog_v5,
        canonical_bytes,
        expected_artifact_identity,
        normalized_graph_producer_implementation,
        parse_generation_v10,
        parse_generation_v4,
        parse_generation_v5,
        parse_generation_v6,
        parse_generation_v9,
        sha256,
        shared_shard_sha256_for_role,
        validate_execution_closure,
        validate_graph_routes,
        validate_shared_shard_declarations,
        validate_shared_shard_identity,
        ArtifactKind,
        AuthenticatedModuleGraph,
        DeploymentModuleGraphRoute,
        GraphEngine,
        GraphManifest,
        GraphManifestRouting,
        GraphModule,
        GraphProducerImplementationIdentity,
        GraphProvenance,
        GraphRoute,
        GraphSharedShard,
        GraphSharedShards,
        RuntimeRegistryAdmission,
        GENERATION_KIND_V10_SHADOW_ONLY,
        GENERATION_KIND_V6_SHADOW_ONLY,
        GENERATION_KIND_V9,
        GRAPH_ROUTING_KIND,
        NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION,
        SHADOW_ROUTING_KIND,
    };
    use crate::environment::udf::wasm_udf_package::{
        load_runtime_registry_current,
        load_runtime_registry_generation,
        load_runtime_registry_generation_with_retained_graphs,
        test_precompiler_material_identity_json,
        MODULE_GRAPH_RUNTIME_SURFACE_INVENTORY_SHA256,
        MODULE_GRAPH_RUNTIME_SURFACE_POLICY_SHA256,
    };

    const FIXTURE_ENV: &str = "CONVEX_WASM_MODULE_GRAPH_REGISTRY_V5_FIXTURE";

    #[test]
    fn graph_artifact_authentication_reuses_files_only_within_one_generation() -> anyhow::Result<()>
    {
        for (kind, with_admission, authority_first) in [
            (ArtifactKind::CoreWasm, false, true),
            (ArtifactKind::CoreWasm, true, true),
            (ArtifactKind::CoreWasm, false, false),
            (ArtifactKind::CoreWasm, true, false),
            (ArtifactKind::Aot, false, false),
            (ArtifactKind::Aot, true, false),
        ] {
            let root = TempDir::new()?;
            let role = match kind {
                ArtifactKind::CoreWasm => "base",
                ArtifactKind::Aot => "leaf",
            };
            let stage = super::artifact_stage(role, kind);
            let contract: super::GraphModuleContract = serde_json::from_value(json!({
                "authority": {"engineCompatibilitySha256": "e".repeat(64),
                    "inspectionSha256": "a".repeat(64), "kind": "test"},
                "contractSha256": "c".repeat(64),
                "dylink": {"first": true, "sha256": "d".repeat(64), "weakImports": []},
                "exports": [], "imports": [],
            }))?;
            let mut identity = json!({"kind": kind.identity_kind(), "role": role});
            let metadata = match kind {
                ArtifactKind::CoreWasm => {
                    identity = json!({
                        "kind": kind.identity_kind(), "role": role,
                        "contract": contract,
                        "layout": {"memoryAlign": 0, "memoryBase": 0, "memorySize": 0,
                            "tableAlign": 0, "tableBase": 0, "tableSize": 0},
                        "link": {}, "objectCompilation": {}, "ownership": {},
                        "nativeArtifactIdentitySchemaVersion": NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION,
                        "pipelineKind": super::PIPELINE_KIND, "providers": [],
                        "sourceProvenance": {}, "toolchain": {},
                    });
                    json!({"contract": contract})
                },
                ArtifactKind::Aot => {
                    identity["engine"] = json!({"compatibilitySha256": "e".repeat(64),
                        "config": {}, "target": "test"});
                    json!({"engineCompatibilitySha256": "e".repeat(64), "engineConfig": {},
                        "kind": "convex-wasm-wasmtime-engine-identity", "target": "test"})
                },
            };
            let key = super::canonical_sha256(&json!({
                "identity": identity, "kind": super::PIPELINE_KIND, "stage": stage,
            }))?;
            let payload = b"authenticated artifact payload";
            let digest = sha256(payload);
            let size = u64::try_from(payload.len())?;
            let mut entry = json!({
                "artifactFile": kind.file(), "artifactSha256": digest, "artifactSize": size,
                "identity": identity, "key": key, "kind": super::CACHE_ENTRY_KIND,
                "metadata": metadata, "stage": stage,
            });
            if with_admission {
                entry["admission"] = json!({
                    "entrySha256": super::canonical_sha256(&entry)?,
                    "payload": {"dev": "1", "ino": "2", "size": size.to_string(),
                        "mtimeNs": "3", "ctimeNs": "4", "nlink": "1",
                        "mode": "33152", "uid": "1000", "gid": "1000"},
                });
            }
            let entry_root = root.path().join(&stage).join(&key);
            create_private_directory(&root.path().join(&stage))?;
            create_private_directory(&entry_root)?;
            let mut entry_bytes = canonical_bytes(&entry)?;
            entry_bytes.push(b'\n');
            let complete = format!("{key}\n").into_bytes();
            let mut files = Vec::new();
            for (name, bytes) in [
                ("COMPLETE", complete.as_slice()),
                (kind.file(), payload.as_slice()),
                ("entry.json", entry_bytes.as_slice()),
            ] {
                write_private(&entry_root.join(name), bytes)?;
                files.push(super::FileRecord {
                    name: name.to_owned(),
                    sha256: sha256(bytes),
                    size: u64::try_from(bytes.len())?,
                });
            }
            let record = super::GraphArtifactRecord {
                cache_key: key,
                files,
                kind: kind.as_str().to_owned(),
                role: role.to_owned(),
                sha256: digest,
                size,
                stage,
            };
            let mut authenticated = BTreeMap::new();
            let reference = super::GraphArtifactReference {
                cache_key: record.cache_key.clone(),
                sha256: record.sha256.clone(),
                size: record.size,
                stage: record.stage.clone(),
            };
            let mut authority_pointer = if authority_first {
                Some(super::load_graph_core_identity_authority(
                    root.path(),
                    &record,
                    &reference,
                    role,
                    &mut authenticated,
                )?
                    as *const super::GraphCoreIdentityAuthority)
            } else {
                None
            };
            let first = super::authenticate_artifact(
                root.path(),
                &record,
                role,
                kind,
                &identity,
                &contract,
                &mut authenticated,
            )?;
            assert!(first.payload_available);
            let withheld = entry_root.join("withheld");
            fs::rename(&first.artifact_path, &withheld)?;
            for name in ["COMPLETE", "entry.json"] {
                fs::rename(
                    entry_root.join(name),
                    entry_root.join(format!("withheld-{name}")),
                )?;
            }
            if kind == ArtifactKind::CoreWasm && !authority_first {
                // A legacy inline graph can admit the entry before a reference
                // graph requests its typed authority, without reopening files.
                assert!(authenticated
                    .values()
                    .all(|entry| entry.core_identity_authority.is_none()));
                authority_pointer = Some(super::load_graph_core_identity_authority(
                    root.path(),
                    &record,
                    &reference,
                    role,
                    &mut authenticated,
                )?
                    as *const super::GraphCoreIdentityAuthority);
            }
            if let Some(first_authority) = authority_pointer {
                let repeated = super::load_graph_core_identity_authority(
                    root.path(),
                    &record,
                    &reference,
                    role,
                    &mut authenticated,
                )?;
                assert_eq!(first_authority, repeated as *const _);
                assert!(super::load_graph_core_identity_authority(
                    root.path(),
                    &record,
                    &reference,
                    "leaf",
                    &mut authenticated,
                )
                .is_err());
                let mut changed_reference = reference.clone();
                changed_reference.sha256 = "b".repeat(64);
                assert!(super::load_graph_core_identity_authority(
                    root.path(),
                    &record,
                    &changed_reference,
                    role,
                    &mut authenticated,
                )
                .is_err());
                assert!(super::load_graph_core_identity_authority(
                    root.path(),
                    &record,
                    &reference,
                    role,
                    &mut BTreeMap::new(),
                )
                .is_err());
            }
            let repeated = super::authenticate_artifact(
                root.path(),
                &record,
                role,
                kind,
                &identity,
                &contract,
                &mut authenticated,
            )?;
            assert_eq!(repeated.artifact_path, first.artifact_path);
            assert_eq!(authenticated.len(), 1);
            for name in ["COMPLETE", "entry.json"] {
                fs::rename(
                    entry_root.join(format!("withheld-{name}")),
                    entry_root.join(name),
                )?;
            }

            assert!(super::authenticate_artifact(
                root.path(),
                &record,
                "wrong-role",
                kind,
                &identity,
                &contract,
                &mut authenticated
            )
            .is_err());

            let mut changed_record = record.clone();
            changed_record.files[2].sha256 = "b".repeat(64);
            assert!(super::authenticate_artifact(
                root.path(),
                &changed_record,
                role,
                kind,
                &identity,
                &contract,
                &mut authenticated
            )
            .is_err());
            let mut changed_identity = identity.clone();
            changed_identity["unexpected"] = json!(true);
            assert!(super::authenticate_artifact(
                root.path(),
                &record,
                role,
                kind,
                &changed_identity,
                &contract,
                &mut authenticated
            )
            .is_err());
            if kind == ArtifactKind::CoreWasm {
                let mut changed_contract = contract.clone();
                changed_contract.contract_sha256 = "b".repeat(64);
                assert!(super::authenticate_artifact(
                    root.path(),
                    &record,
                    role,
                    kind,
                    &identity,
                    &changed_contract,
                    &mut authenticated
                )
                .is_err());
            }
            let mut oversized = record.clone();
            oversized.size = kind.maximum_bytes() + 1;
            oversized.files[1].size = oversized.size;
            let mut disagreement = record.clone();
            disagreement.sha256 = "b".repeat(64);
            // Reject invalid declarations before either file access or a
            // conflicting cached record can account for the failure.
            for entries in [&mut authenticated, &mut BTreeMap::new()] {
                let error = super::authenticate_artifact(
                    root.path(),
                    &oversized,
                    role,
                    kind,
                    &identity,
                    &contract,
                    entries,
                )
                .unwrap_err();
                assert!(error.to_string().contains("size"));
                let error = super::authenticate_artifact(
                    root.path(),
                    &disagreement,
                    role,
                    kind,
                    &identity,
                    &contract,
                    entries,
                )
                .unwrap_err();
                assert!(error
                    .to_string()
                    .contains("disagrees with its artifact file"));
            }

            assert!(super::authenticate_artifact(
                root.path(),
                &record,
                role,
                kind,
                &identity,
                &contract,
                &mut BTreeMap::new(),
            )
            .is_err());
            if kind == ArtifactKind::Aot {
                let omitted = root.path().join("omitted-artifact.cwasm");
                fs::rename(&withheld, &omitted)?;
                let without_payload = super::authenticate_artifact(
                    root.path(),
                    &record,
                    role,
                    kind,
                    &identity,
                    &contract,
                    &mut BTreeMap::new(),
                )?;
                assert!(!without_payload.payload_available);
                fs::rename(omitted, &withheld)?;
            }
            fs::rename(withheld, &first.artifact_path)?;
            fs::write(&first.artifact_path, vec![0; payload.len()])?;
            assert!(super::authenticate_artifact(
                root.path(),
                &record,
                role,
                kind,
                &identity,
                &contract,
                &mut BTreeMap::new()
            )
            .is_err());

            fs::write(&first.artifact_path, payload)?;
            let mut invalid_metadata = entry.clone();
            invalid_metadata["metadata"] = json!({});
            let mut invalid_metadata_bytes = canonical_bytes(&invalid_metadata)?;
            invalid_metadata_bytes.push(b'\n');
            fs::write(entry_root.join("entry.json"), &invalid_metadata_bytes)?;
            let mut invalid_metadata_record = record.clone();
            invalid_metadata_record.files[2].sha256 = sha256(&invalid_metadata_bytes);
            invalid_metadata_record.files[2].size = u64::try_from(invalid_metadata_bytes.len())?;
            let error = super::authenticate_artifact(
                root.path(),
                &invalid_metadata_record,
                role,
                kind,
                &identity,
                &contract,
                &mut BTreeMap::new(),
            )
            .unwrap_err();
            assert!(error.to_string().contains("metadata disagrees"));

            let mut unknown_field = entry.clone();
            unknown_field["unexpected"] = json!(true);
            let mut unknown_bytes = canonical_bytes(&unknown_field)?;
            unknown_bytes.push(b'\n');
            fs::write(entry_root.join("entry.json"), &unknown_bytes)?;
            let mut unknown_record = record.clone();
            unknown_record.files[2].sha256 = sha256(&unknown_bytes);
            unknown_record.files[2].size = u64::try_from(unknown_bytes.len())?;
            let error = super::authenticate_artifact(
                root.path(),
                &unknown_record,
                role,
                kind,
                &identity,
                &contract,
                &mut BTreeMap::new(),
            )
            .unwrap_err();
            assert!(error.to_string().contains("unknown field"));

            let mut wrong_key_entry = entry.clone();
            wrong_key_entry["identity"]["keyMismatch"] = json!(true);
            let mut wrong_key_bytes = canonical_bytes(&wrong_key_entry)?;
            wrong_key_bytes.push(b'\n');
            fs::write(entry_root.join("entry.json"), &wrong_key_bytes)?;
            let mut wrong_key_record = record.clone();
            wrong_key_record.files[2].sha256 = sha256(&wrong_key_bytes);
            wrong_key_record.files[2].size = u64::try_from(wrong_key_bytes.len())?;
            let mut rejected = BTreeMap::new();
            let error = super::authenticate_artifact_entry(
                root.path(),
                &wrong_key_record,
                kind,
                &mut rejected,
            )
            .err()
            .expect("first admission must authenticate the identity key");
            assert!(error.to_string().contains("does not match its cache key"));
            assert!(rejected.is_empty());
        }
        Ok(())
    }

    pub(crate) fn with_leaf_module_cache_test_records(
        engine: &wasmtime::Engine,
        run: impl FnOnce(
            super::AuthenticatedModuleGraphExecutionModule<'_>,
            super::AuthenticatedModuleGraphExecutionModule<'_>,
            super::AuthenticatedModuleGraphExecutionModule<'_>,
        ) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let core_wasm = wasm_encoder::Module::new().finish();
        let aot = engine
            .precompile_module(&core_wasm)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let artifact = |kind: ArtifactKind, bytes: &[u8]| -> anyhow::Result<_> {
            let path = root.path().join(kind.file());
            fs::write(&path, bytes)?;
            Ok(super::AuthenticatedGraphArtifact {
                artifact_path: path,
                _cache_key: sha256(bytes),
                _kind: kind,
                payload_available: true,
                role: "leaf".to_owned(),
                sha256: sha256(bytes),
                size: u64::try_from(bytes.len())?,
            })
        };
        let core_wasm = artifact(ArtifactKind::CoreWasm, &core_wasm)?;
        let aot = artifact(ArtifactKind::Aot, &aot)?;
        let reconstructible_aot = super::AuthenticatedGraphArtifact {
            artifact_path: root.path().join("omitted-artifact.cwasm"),
            payload_available: false,
            ..aot.clone()
        };
        let reference = |artifact: &super::AuthenticatedGraphArtifact| {
            json!({
                "cacheKey": artifact.sha256,
                "sha256": artifact.sha256,
                "size": artifact.size,
                "stage": artifact._kind.as_str(),
            })
        };
        let manifest: GraphModule = serde_json::from_value(json!({
            "artifacts": {"aot": reference(&aot), "coreWasm": reference(&core_wasm)},
            "contract": {
                "authority": {
                    "engineCompatibilitySha256": "e".repeat(64),
                    "inspectionSha256": "a".repeat(64),
                    "kind": "test",
                },
                "contractSha256": "c".repeat(64),
                "dylink": {"first": true, "sha256": "d".repeat(64), "weakImports": []},
                "exports": [], "imports": [],
            },
            "layout": {"memoryAlign": 0, "memoryBase": 0, "memorySize": 0,
                "tableAlign": 0, "tableBase": 0, "tableSize": 0},
            "link": {}, "objectCompilation": {}, "ownership": {}, "providers": [],
            "role": "leaf", "sourceProvenance": {},
        }))?;
        let mut changed = manifest.clone();
        changed.contract.exports.push(super::GraphModuleExport {
            index: 0,
            name: "unexpected".to_owned(),
            r#type: super::GraphExternalType {
                canonical: "func()->()".to_owned(),
                kind: "func".to_owned(),
                sha256: sha256(b"func()->()"),
            },
        });
        run(
            super::AuthenticatedModuleGraphExecutionModule {
                aot: &aot,
                core_wasm: &core_wasm,
                manifest: &manifest,
            },
            super::AuthenticatedModuleGraphExecutionModule {
                aot: &aot,
                core_wasm: &core_wasm,
                manifest: &changed,
            },
            super::AuthenticatedModuleGraphExecutionModule {
                aot: &reconstructible_aot,
                core_wasm: &core_wasm,
                manifest: &manifest,
            },
        )
    }

    fn graph_producer_identity_fixture(with_operational_sources: bool) -> JsonValue {
        let mut identity = json!({
            "kind": "convex-wasm-artifact-producer-identity-v1",
            "manifest": {
                "path": "scripts/convex-wasm-artifact-producer-source-manifest.json",
                "sha256": "a".repeat(64),
                "size": 1,
            },
            "nodeVersion": "v24.15.0",
            "sha256": "4".repeat(64),
            "sources": [
                {"path": "scripts/a.mjs", "sha256": "b".repeat(64), "size": 1},
                {"path": "scripts/b.mjs", "sha256": "c".repeat(64), "size": 2},
            ],
        });
        if with_operational_sources {
            identity["operationalSources"] = json!([
                {"path": "scripts/operational/a.mjs", "sha256": "d".repeat(64), "size": 1},
                {"path": "scripts/operational/b.mjs", "sha256": "e".repeat(64), "size": 2},
                {"path": "scripts/operational/c.mjs", "sha256": "f".repeat(64), "size": 3},
                {"path": "scripts/operational/d.mjs", "sha256": "0".repeat(64), "size": 4},
                {"path": "scripts/operational/e.mjs", "sha256": "1".repeat(64), "size": 5},
                {"path": "scripts/operational/f.mjs", "sha256": "2".repeat(64), "size": 6},
            ]);
        }
        identity
    }

    #[test]
    fn graph_routing_contract_accepts_only_its_paired_schema() {
        let route = GraphRoute {
            entry_id: "1".repeat(64),
            entry_selector_id: "2".repeat(16),
            entry_symbol: "entry".into(),
            export_name: "query".into(),
            route_id: "3".repeat(64),
            udf_kind: "query".into(),
            visibility: "public".into(),
        };
        let routing_v1 = GraphManifestRouting {
            cohort_id: "4".repeat(64),
            kind: "convex-wasm-module-graph-routing-v1".into(),
            routes: vec![route.clone()],
            schedule_sha256: Some("5".repeat(64)),
            source_envelope_sha256: Some("6".repeat(64)),
        };
        let routing_v2: GraphManifestRouting = serde_json::from_value(json!({
            "cohortId": "4".repeat(64),
            "kind": "convex-wasm-module-graph-routing-v2",
            "routes": [route],
        }))
        .expect("routing-v2 without deployment-global fields should deserialize");

        assert!(validate_graph_routes(&routing_v1, 2).is_ok());
        assert!(validate_graph_routes(&routing_v2, 3).is_ok());
        assert!(validate_graph_routes(&routing_v2, 4).is_ok());
        assert!(validate_graph_routes(&routing_v1, 3).is_err());
        assert!(validate_graph_routes(&routing_v1, 4).is_err());
        assert!(validate_graph_routes(&routing_v2, 2).is_err());

        let mut routing_v2_with_global_identity = routing_v2;
        routing_v2_with_global_identity.schedule_sha256 = Some("5".repeat(64));
        assert!(validate_graph_routes(&routing_v2_with_global_identity, 3).is_err());
        assert!(validate_graph_routes(&routing_v2_with_global_identity, 4).is_err());
    }

    #[test]
    fn shared_shard_contract_binds_module_roles_and_provenance() -> anyhow::Result<()> {
        let first_shard = "a".repeat(64);
        let second_shard = "b".repeat(64);
        let first_role = format!("shared-{first_shard}");
        let second_role = format!("shared-{second_shard}");
        let roles = ["base", first_role.as_str(), second_role.as_str(), "leaf"];
        let shared_shards = GraphSharedShards {
            kind: "convex-wasm-module-graph-shared-modules-v1".into(),
            shards: vec![
                GraphSharedShard {
                    role: first_role.clone(),
                    shard_sha256: first_shard.clone(),
                },
                GraphSharedShard {
                    role: second_role.clone(),
                    shard_sha256: second_shard.clone(),
                },
            ],
        };

        assert_eq!(
            shared_shard_sha256_for_role(&first_role),
            Some(first_shard.as_str())
        );
        assert!(validate_shared_shard_identity(&first_role, Some(&first_shard)).is_ok());
        assert!(validate_shared_shard_identity(&first_role, Some(&second_shard)).is_err());
        assert!(validate_shared_shard_identity("base", Some(&first_shard)).is_err());
        assert!(validate_shared_shard_declarations(
            &roles,
            Some(&shared_shards),
            Some(&shared_shards),
        )
        .is_ok());

        let mut changed_descriptor = shared_shards.clone();
        changed_descriptor.shards[0].shard_sha256 = second_shard;
        assert!(validate_shared_shard_declarations(
            &roles,
            Some(&changed_descriptor),
            Some(&changed_descriptor),
        )
        .is_err());

        assert!(validate_shared_shard_declarations(&roles, Some(&shared_shards), None,).is_err());
        assert!(validate_shared_shard_declarations(&roles, None, None,).is_err());
        assert!(
            validate_shared_shard_declarations(&["base", "common", "leaf"], None, None,).is_ok()
        );
        Ok(())
    }

    #[test]
    fn shared_shard_identity_is_bound_to_core_wasm_not_aot() -> anyhow::Result<()> {
        let shard_sha256 = "a".repeat(64);
        let role = format!("shared-{shard_sha256}");
        let module: GraphModule = serde_json::from_value(json!({
            "artifacts": {
                "aot": {
                    "cacheKey": "b".repeat(64),
                    "sha256": "c".repeat(64),
                    "size": 1,
                    "stage": "aot",
                },
                "coreWasm": {
                    "cacheKey": "d".repeat(64),
                    "sha256": "e".repeat(64),
                    "size": 1,
                    "stage": "core",
                },
            },
            "contract": {
                "authority": {
                    "engineCompatibilitySha256": "f".repeat(64),
                    "inspectionSha256": "0".repeat(64),
                    "kind": "convex-wasm-wasmtime-module-contract-v1",
                },
                "contractSha256": "1".repeat(64),
                "dylink": {"first": true, "sha256": "2".repeat(64), "weakImports": []},
                "exports": [],
                "imports": [],
            },
            "layout": {
                "memoryAlign": 0,
                "memoryBase": 0,
                "memorySize": 0,
                "tableAlign": 0,
                "tableBase": 0,
                "tableSize": 0,
            },
            "link": {},
            "objectCompilation": {},
            "ownership": {},
            "providers": [],
            "role": role,
            "sharedShardSha256": shard_sha256,
            "sourceProvenance": {},
        }))?;
        let shared_shards = GraphSharedShards {
            kind: "convex-wasm-module-graph-shared-modules-v1".into(),
            shards: vec![GraphSharedShard {
                role: module.role.clone(),
                shard_sha256: module
                    .shared_shard_sha256
                    .clone()
                    .context("shared shard identity")?,
            }],
        };
        let engine = json!({
            "compatibilitySha256": "3".repeat(64),
            "config": {
                "consumeFuel": true,
                "epochInterruption": true,
                "profilingStrategy": "perf-map",
                "wasmExceptions": true,
            },
            "configurationSha256": "4".repeat(64),
            "package": {},
            "revision": "test",
            "target": {"cpu": "baseline", "triple": "x86_64-unknown-linux-gnu"},
            "wasmtimeMaterialsSha256": "5".repeat(64),
        });
        let host_abi = json!({
            "imports": [],
            "kind": "convex-wasm-module-graph-host-abi-v1",
            "sha256": "6".repeat(64),
        });
        let toolchain = json!({
            "aot": {},
            "core": {},
            "staticHermesCBundleMemberCompilation": {},
        });
        let manifest: GraphManifest = serde_json::from_value(json!({
            "engine": engine,
            "graphManifestSha256": "7".repeat(64),
            "hostAbi": host_abi,
            "initialization": {
                "baseHeapBase": 1,
                "baseTableSize": 1,
                "constructorOrder": [],
                "finalMemoryCursor": 1,
                "finalTableCursor": 1,
                "moduleOrder": [],
                "relocationOrder": [],
            },
            "kind": "convex-wasm-module-graph-manifest-v2",
            "modules": [],
            "producerImplementation": {
                "kind": "convex-wasm-artifact-producer-identity-v1",
                "sha256": "4".repeat(64),
            },
            "replacement": {},
            "routing": {
                "cohortId": "8".repeat(64),
                "kind": "convex-wasm-module-graph-routing-v1",
                "routes": [],
                "scheduleSha256": "9".repeat(64),
                "sourceEnvelopeSha256": "a".repeat(64),
            },
            "schemaVersion": 2,
            "sharedShards": shared_shards,
            "toolchain": toolchain,
        }))?;
        let _provenance: GraphProvenance = serde_json::from_value(json!({
            "engine": manifest.engine,
            "hostAbi": manifest.host_abi,
            "identities": {},
            "kind": "convex-wasm-module-graph-provenance-v2",
            "producerIdentity": graph_producer_identity_fixture(false),
            "routing": manifest.routing,
            "schemaVersion": 2,
            "sharedShards": shared_shards,
            "toolchain": manifest.toolchain,
        }))?;

        let core = expected_artifact_identity(&manifest, &module, ArtifactKind::CoreWasm)?;
        assert_eq!(core["sharedShardSha256"], json!(module.shared_shard_sha256));
        assert_eq!(
            core["nativeArtifactIdentitySchemaVersion"],
            json!(NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION)
        );
        assert!(core.get("producerImplementation").is_none());

        let aot = expected_artifact_identity(&manifest, &module, ArtifactKind::Aot)?;
        assert!(aot.get("sharedShardSha256").is_none());
        assert_eq!(
            aot["nativeArtifactIdentitySchemaVersion"],
            json!(NATIVE_ARTIFACT_IDENTITY_SCHEMA_VERSION)
        );
        assert!(aot.get("producerImplementation").is_none());

        let mut descriptor_tampered = module.clone();
        descriptor_tampered.shared_shard_sha256 = Some("b".repeat(64));
        let tampered_core =
            expected_artifact_identity(&manifest, &descriptor_tampered, ArtifactKind::CoreWasm)?;
        assert_ne!(core, tampered_core);
        Ok(())
    }

    #[test]
    fn graph_producer_identity_accepts_supported_forms_and_rejects_malformed_extensions(
    ) -> anyhow::Result<()> {
        let historical = graph_producer_identity_fixture(false);
        assert_eq!(
            normalized_graph_producer_implementation(&historical)?,
            GraphProducerImplementationIdentity {
                kind: "convex-wasm-artifact-producer-identity-v1".into(),
                sha256: "4".repeat(64),
            }
        );

        let current = graph_producer_identity_fixture(true);
        assert_eq!(
            normalized_graph_producer_implementation(&current)?,
            GraphProducerImplementationIdentity {
                kind: "convex-wasm-artifact-producer-identity-v1".into(),
                sha256: "4".repeat(64),
            }
        );

        let mut unexpected = current.clone();
        unexpected["unexpected"] = json!(true);
        assert!(normalized_graph_producer_implementation(&unexpected).is_err());

        let mut malformed_operational_sources = current.clone();
        malformed_operational_sources["operationalSources"] = json!("not-an-array");
        assert!(normalized_graph_producer_implementation(&malformed_operational_sources).is_err());

        let mut malformed_operational_source = current;
        malformed_operational_source["operationalSources"][0]["unexpected"] = json!(true);
        assert!(normalized_graph_producer_implementation(&malformed_operational_source).is_err());
        Ok(())
    }

    #[test]
    #[ignore = "requires the cross-boundary producer fixture path"]
    fn accepts_javascript_generation_v5_fixture_and_rejects_tampering() -> anyhow::Result<()> {
        let path = env::var_os(FIXTURE_ENV).context("cross-boundary fixture path is not set")?;
        let fixture: JsonValue = serde_json::from_slice(&fs::read(path)?)?;
        anyhow::ensure!(
            fixture["expected"]["registryGenerationV5"]
                .get("capabilityEntryPackages")
                .is_none()
                && fixture["material"].get("capabilityEntryPackage").is_none(),
            "cross-boundary generation-v5 fixture contains legacy capability-entry packages"
        );
        let root = TempDir::new()?;
        materialize_fixture(&fixture, root.path())?;
        anyhow::ensure!(
            !root.path().join("capability-entry-packages").exists(),
            "graph-only fixture materialized a legacy capability-entry package root"
        );
        let generation_value = fixture["expected"]["registryGenerationV5"].clone();
        let generation = parse_generation_v5(generation_value.clone())?;
        let [selected_route_id] = generation.graph_routing.selected_route_ids.as_slice() else {
            anyhow::bail!("cross-boundary fixture must contain exactly one selected route")
        };
        let current = load_runtime_registry_current(root.path())?;
        let validated = load_runtime_registry_generation(root.path(), &current)?;
        let catalog = validated
            .into_parts()
            .5
            .context("generation-v5 did not produce a module graph catalog")?;
        assert_eq!(catalog.len(), 1);
        assert_eq!(
            catalog
                .execution_graph(selected_route_id)
                .map(|graph| graph.graph_sha256().to_owned()),
            Some(generation.module_graphs[0].graph_manifest_sha256.clone())
        );

        let mut changed_stage = generation_value.clone();
        changed_stage["moduleGraphs"][0]["artifacts"][0]["stage"] = json!("wrong-stage");
        resign_generation(&mut changed_stage)?;
        assert!(parse_generation_v5(changed_stage).is_err());

        let artifact_path = root
            .path()
            .join("module-graph-cache/immutable/v6/artifacts")
            .join(generation.module_graphs[0].artifacts[0].stage.as_str())
            .join(generation.module_graphs[0].artifacts[0].cache_key.as_str())
            .join("artifact.wasm");
        write_private(&artifact_path, b"tampered")?;
        assert!(load_runtime_registry_generation(root.path(), &current).is_err());
        let withheld = TempDir::new()?;
        fs::rename(
            root.path().join("module-graph-cache/immutable"),
            withheld.path().join("immutable"),
        )?;
        // A retained graph is an owned metadata snapshot. Reusing it does not
        // reread payloads, but any later cold module load must reject corruption.
        let reused = load_runtime_registry_generation_with_retained_graphs(
            root.path(),
            &current,
            &[&catalog],
        )?;
        let reused_catalog = reused.into_parts().5.context("retained graph catalog")?;
        assert_eq!(reused_catalog.route_graphs, catalog.route_graphs);
        drop(catalog);
        fs::rename(
            withheld.path().join("immutable"),
            root.path().join("module-graph-cache/immutable"),
        )?;
        let artifact = &reused_catalog
            .graphs
            .values()
            .next()
            .context("retained graph")?
            .artifacts[0];
        assert!(super::authenticate_file(
            &artifact.artifact_path,
            super::MAX_CORE_WASM_BYTES,
            artifact.size,
            &artifact.sha256,
            "retained Core Wasm",
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn rejects_unknown_generation_fields_and_graph_authority() -> anyhow::Result<()> {
        let mut value = minimal_generation();
        value["unexpected"] = json!(true);
        resign_generation(&mut value)?;
        assert!(parse_generation_v4(value).is_err());

        let mut value = minimal_generation();
        value["graphRouting"]["selectedRuntime"] = json!("module-graph");
        resign_generation(&mut value)?;
        let generation = parse_generation_v4(value)?;
        assert!(authenticate_module_graph_catalog_v4(
            Path::new("unused"),
            &[],
            &generation,
            &generation
                .graph_routing
                .selected_route_ids
                .iter()
                .cloned()
                .collect(),
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn module_graph_artifact_records_accept_ordered_shared_shards() -> anyhow::Result<()> {
        let mut no_shared = minimal_generation();
        let artifacts = no_shared["moduleGraphs"][0]["artifacts"]
            .as_array_mut()
            .context("module graph artifacts")?;
        artifacts.drain(2..4);
        resign_generation(&mut no_shared)?;
        assert!(parse_generation_v4(no_shared).is_ok());

        let mut multiple_shared = minimal_generation();
        let artifacts = multiple_shared["moduleGraphs"][0]["artifacts"]
            .as_array_mut()
            .context("module graph artifacts")?;
        let shared_role = format!("shared-{}", "a".repeat(64));
        artifacts.splice(
            4..4,
            [
                artifact(&shared_role, "coreWasm"),
                artifact(&shared_role, "aot"),
            ],
        );
        resign_generation(&mut multiple_shared)?;
        assert!(parse_generation_v4(multiple_shared).is_ok());

        let mut duplicate_shared = minimal_generation();
        let artifacts = duplicate_shared["moduleGraphs"][0]["artifacts"]
            .as_array_mut()
            .context("module graph artifacts")?;
        artifacts.splice(
            4..4,
            [artifact("common", "coreWasm"), artifact("common", "aot")],
        );
        resign_generation(&mut duplicate_shared)?;
        assert!(parse_generation_v4(duplicate_shared).is_err());

        let mut misplaced_leaf = minimal_generation();
        let artifacts = misplaced_leaf["moduleGraphs"][0]["artifacts"]
            .as_array_mut()
            .context("module graph artifacts")?;
        artifacts.splice(
            4..4,
            [artifact("leaf", "coreWasm"), artifact("leaf", "aot")],
        );
        resign_generation(&mut misplaced_leaf)?;
        assert!(parse_generation_v4(misplaced_leaf).is_err());
        Ok(())
    }

    #[test]
    fn generation_v5_and_v6_require_exact_authenticated_binding_and_route_closure(
    ) -> anyhow::Result<()> {
        let graph_sha256 = "e".repeat(64);
        let cohort_id = "2".repeat(64);
        let schedule_sha256 = "3".repeat(64);
        let source_envelope_sha256 = "4".repeat(64);
        let contract =
            module_graph_cohort_contract(&cohort_id, &schedule_sha256, &source_envelope_sha256)?;
        let contract_sha256 = contract["cohortContractSha256"]
            .as_str()
            .context("cohort contract identity")?
            .to_owned();
        let contract_route = &contract["routes"][0];
        let route_id = contract_route["routeId"]
            .as_str()
            .context("cohort contract route ID")?
            .to_owned();
        let mut value = minimal_generation();
        value["kind"] = json!("convex-wasm-runtime-registry-generation-v5");
        value
            .as_object_mut()
            .context("generation object")?
            .remove("capabilityEntryPackages");
        value
            .as_object_mut()
            .context("generation object")?
            .insert("moduleGraphCohorts".into(), json!([contract.clone()]));
        value["graphRouting"] = json!({
            "cutoverReady": true,
            "cutoverState": "module-graph-authorized",
            "graphBackedRouteIds": [route_id.clone()],
            "kind": "convex-wasm-runtime-registry-graph-routing-boundary-v1",
            "missingRouteIds": [],
            "selectedRouteIds": [route_id.clone()],
            "selectedRuntime": "module-graph",
        });
        let mut binding = json!({
            "bindingSha256": "0".repeat(64),
            "cohorts": [{
                "cohortContractSha256": contract_sha256.clone(),
                "cohortId": cohort_id.clone(),
                "graphManifestSha256": graph_sha256.clone(),
                "routeIds": [route_id.clone()],
            }],
            "kind": "convex-wasm-deployment-module-graph-binding-v1",
            "scheduleSha256": schedule_sha256.clone(),
            "schemaVersion": 1,
            "sourceEnvelopeSha256": source_envelope_sha256.clone(),
        });
        resign_identity(&mut binding, "bindingSha256")?;
        value
            .as_object_mut()
            .context("generation object")?
            .insert("moduleGraphBinding".into(), binding);
        resign_generation(&mut value)?;
        let generation = parse_generation_v5(value.clone())?;

        let mut shadow_only = value.clone();
        shadow_only["kind"] = json!(GENERATION_KIND_V6_SHADOW_ONLY);
        let shadow_only_object = shadow_only.as_object_mut().context("generation object")?;
        shadow_only_object.remove("graphRouting");
        shadow_only_object.insert("admission".into(), json!("shadow-only"));
        shadow_only_object.insert(
            "shadowRouting".into(),
            json!({
                "artifactComplete": true,
                "artifactState": "shadow-only",
                "kind": SHADOW_ROUTING_KIND,
                "routeIds": [route_id.clone()],
            }),
        );
        resign_generation(&mut shadow_only)?;
        assert_eq!(
            parse_generation_v6(shadow_only.clone())?.admission(),
            RuntimeRegistryAdmission::ShadowOnly
        );

        let mut missing_admission = shadow_only.clone();
        missing_admission
            .as_object_mut()
            .context("generation object")?
            .remove("admission");
        resign_generation(&mut missing_admission)?;
        assert!(parse_generation_v6(missing_admission).is_err());

        let mut primary_admitted = shadow_only.clone();
        primary_admitted["admission"] = json!("primary-admitted");
        resign_generation(&mut primary_admitted)?;
        assert!(parse_generation_v6(primary_admitted).is_err());

        let mut incomplete_artifacts = shadow_only.clone();
        incomplete_artifacts["shadowRouting"]["artifactComplete"] = json!(false);
        resign_generation(&mut incomplete_artifacts)?;
        assert!(parse_generation_v6(incomplete_artifacts).is_err());

        let mut tampered = shadow_only;
        tampered["shadowRouting"]["artifactState"] = json!("shadow-only-tampered");
        assert!(parse_generation_v6(tampered).is_err());

        let route = GraphRoute {
            entry_id: contract_route["entryId"]
                .as_str()
                .context("entry ID")?
                .into(),
            entry_selector_id: contract_route["entrySelectorId"]
                .as_str()
                .context("selector ID")?
                .into(),
            entry_symbol: contract_route["entrySymbol"]
                .as_str()
                .context("entry symbol")?
                .into(),
            export_name: contract_route["exportName"]
                .as_str()
                .context("export name")?
                .into(),
            route_id: route_id.clone(),
            udf_kind: "query".into(),
            visibility: "public".into(),
        };
        let engine: GraphEngine = serde_json::from_value(contract["engine"].clone())?;
        let mut graphs = BTreeMap::from([(
            graph_sha256.clone(),
            AuthenticatedModuleGraph {
                _package_root: PathBuf::from("unused"),
                context_reuse_analysis: None,
                engine_compatibility_sha256: engine.compatibility_sha256.clone(),
                engine,
                host_abi: super::GraphHostAbi {
                    imports: vec![],
                    kind: "convex-wasm-module-graph-host-abi-v1".into(),
                    sha256: "8".repeat(64),
                },
                initialization: super::GraphInitialization {
                    base_heap_base: 1,
                    base_table_size: 1,
                    constructor_order: vec!["common".into(), "leaf".into()],
                    final_memory_cursor: 1,
                    final_table_cursor: 1,
                    module_order: vec!["base".into(), "common".into(), "leaf".into()],
                    relocation_order: vec!["common".into(), "leaf".into()],
                },
                modules: vec![],
                producer_implementation: GraphProducerImplementationIdentity {
                    kind: "convex-wasm-artifact-producer-identity-v1".into(),
                    sha256: "4".repeat(64),
                },
                routing: GraphManifestRouting {
                    cohort_id,
                    kind: "convex-wasm-module-graph-routing-v1".into(),
                    routes: vec![route.clone()],
                    schedule_sha256: Some(schedule_sha256),
                    source_envelope_sha256: Some(source_envelope_sha256),
                },
                artifacts: vec![],
            },
        )]);
        let route_graphs = BTreeMap::from([(route_id.clone(), graph_sha256.clone())]);
        let deployment_routes = BTreeMap::from([(
            route_id.clone(),
            DeploymentModuleGraphRoute {
                cohort_contract_sha256: contract_sha256,
                entry_id: route.entry_id,
                entry_selector_id: route.entry_selector_id,
                export_name: route.export_name,
                route_id: route_id.clone(),
                udf_kind: route.udf_kind,
                visibility: route.visibility,
            },
        )]);
        validate_execution_closure(
            &graphs,
            &route_graphs,
            generation.module_graph_binding(),
            generation.module_graph_cohorts(),
            &deployment_routes,
        )?;

        // No physical cache directories exist: only the retained snapshot
        // can supply these graphs, while deployment closure is still checked.
        let retained_root = TempDir::new()?;
        let retained = super::AuthenticatedModuleGraphCatalog {
            graphs: graphs.clone(),
            route_graphs: route_graphs.clone(),
            registry_root: retained_root.path().to_owned(),
            records: generation.module_graphs.clone(),
        };
        let reused = authenticate_module_graph_catalog_v5(
            retained_root.path(),
            &[&retained],
            &generation,
            generation.module_graph_binding(),
            generation.module_graph_cohorts(),
            &deployment_routes,
        )?;
        assert_eq!(reused.route_graphs, route_graphs);
        assert_eq!(reused.graphs.len(), graphs.len());
        assert!(super::authenticate_graph_records(
            retained_root.path(),
            &generation.module_graphs,
            &[],
        )
        .is_err());
        let mut foreign = super::AuthenticatedModuleGraphCatalog {
            graphs: graphs.clone(),
            route_graphs: route_graphs.clone(),
            registry_root: PathBuf::from("different-registry"),
            records: generation.module_graphs.clone(),
        };
        assert!(super::authenticate_graph_records(
            retained_root.path(),
            &generation.module_graphs,
            &[&foreign],
        )
        .is_err());
        foreign.registry_root = retained_root.path().to_owned();
        foreign.records[0].package.files[0].size += 1;
        assert!(super::authenticate_graph_records(
            retained_root.path(),
            &generation.module_graphs,
            &[&foreign],
        )
        .is_err());
        foreign.records = generation.module_graphs.clone();
        foreign.records[0].artifacts[0].sha256 = "0".repeat(64);
        assert!(super::authenticate_graph_records(
            retained_root.path(),
            &generation.module_graphs,
            &[&foreign],
        )
        .is_err());
        let mut changed_routes = deployment_routes.clone();
        changed_routes
            .get_mut(&route_id)
            .context("deployment route")?
            .entry_id = "0".repeat(64);
        assert!(authenticate_module_graph_catalog_v5(
            retained_root.path(),
            &[&retained],
            &generation,
            generation.module_graph_binding(),
            generation.module_graph_cohorts(),
            &changed_routes,
        )
        .is_err());

        let mut legacy_package_field = value.clone();
        legacy_package_field
            .as_object_mut()
            .context("generation object")?
            .insert("capabilityEntryPackages".into(), json!([]));
        resign_generation(&mut legacy_package_field)?;
        assert!(parse_generation_v5(legacy_package_field).is_err());

        let mut missing_contract_identity = value.clone();
        missing_contract_identity["moduleGraphCohorts"][0]
            .as_object_mut()
            .context("cohort contract object")?
            .remove("cohortContractSha256");
        resign_generation(&mut missing_contract_identity)?;
        assert!(parse_generation_v5(missing_contract_identity).is_err());

        for (pointer, replacement) in [
            (
                "/moduleGraphCohorts/0/cohortContractSha256",
                json!("f".repeat(64)),
            ),
            (
                "/moduleGraphCohorts/0/compiler/compilerRevision",
                json!("changed-compiler-revision"),
            ),
            (
                "/moduleGraphCohorts/0/execution/limits/executionFuel",
                json!(2),
            ),
            (
                "/moduleGraphCohorts/0/precompilerMaterialIdentity/binary/sha256",
                json!("f".repeat(64)),
            ),
            (
                "/moduleGraphCohorts/0/entries/0/source/sha256",
                json!("f".repeat(64)),
            ),
            (
                "/moduleGraphCohorts/0/routes/0/exportName",
                json!("changedExport"),
            ),
        ] {
            let mut tampered = value.clone();
            *tampered
                .pointer_mut(pointer)
                .with_context(|| format!("missing tamper path {pointer}"))? = replacement;
            resign_generation(&mut tampered)?;
            assert!(
                parse_generation_v5(tampered).is_err(),
                "generation-v5 accepted cohort tampering at {pointer}"
            );
        }

        let mut changed_generation_contract = value.clone();
        changed_generation_contract["moduleGraphCohorts"][0]["descriptorIdentitySha256"] =
            json!("f".repeat(64));
        resign_identity(
            &mut changed_generation_contract["moduleGraphCohorts"][0],
            "cohortContractSha256",
        )?;
        resign_generation(&mut changed_generation_contract)?;
        let changed_generation_contract = parse_generation_v5(changed_generation_contract)?;
        assert!(authenticate_module_graph_catalog_v5(
            Path::new("unused"),
            &[],
            &changed_generation_contract,
            generation.module_graph_binding(),
            generation.module_graph_cohorts(),
            &deployment_routes,
        )
        .is_err());

        let mut changed_binding = value.clone();
        changed_binding["moduleGraphBinding"]["cohorts"][0]["cohortContractSha256"] =
            json!("f".repeat(64));
        resign_identity(&mut changed_binding["moduleGraphBinding"], "bindingSha256")?;
        resign_generation(&mut changed_binding)?;
        let changed_binding = parse_generation_v5(changed_binding)?;
        assert!(validate_execution_closure(
            &graphs,
            &route_graphs,
            changed_binding.module_graph_binding(),
            changed_binding.module_graph_cohorts(),
            &deployment_routes,
        )
        .is_err());

        assert!(validate_execution_closure(
            &graphs,
            &BTreeMap::new(),
            generation.module_graph_binding(),
            generation.module_graph_cohorts(),
            &deployment_routes,
        )
        .is_err());
        let mut changed_deployment_routes = deployment_routes.clone();
        changed_deployment_routes
            .get_mut(&route_id)
            .context("deployment route")?
            .export_name = "changedExport".into();
        assert!(validate_execution_closure(
            &graphs,
            &route_graphs,
            generation.module_graph_binding(),
            generation.module_graph_cohorts(),
            &changed_deployment_routes,
        )
        .is_err());
        let engine_revision = graphs
            .get(&graph_sha256)
            .context("authenticated graph")?
            .engine
            .revision
            .clone();
        graphs
            .get_mut(&graph_sha256)
            .context("authenticated graph")?
            .engine
            .revision = "changed-engine-revision".into();
        assert!(validate_execution_closure(
            &graphs,
            &route_graphs,
            generation.module_graph_binding(),
            generation.module_graph_cohorts(),
            &deployment_routes,
        )
        .is_err());
        graphs
            .get_mut(&graph_sha256)
            .context("authenticated graph")?
            .engine
            .revision = engine_revision;

        let producer_sha256 = graphs
            .get(&graph_sha256)
            .context("authenticated graph")?
            .producer_implementation
            .sha256
            .clone();
        graphs
            .get_mut(&graph_sha256)
            .context("authenticated graph")?
            .producer_implementation
            .sha256 = "f".repeat(64);
        assert!(validate_execution_closure(
            &graphs,
            &route_graphs,
            generation.module_graph_binding(),
            generation.module_graph_cohorts(),
            &deployment_routes,
        )
        .is_err());
        graphs
            .get_mut(&graph_sha256)
            .context("authenticated graph")?
            .producer_implementation
            .sha256 = producer_sha256;

        value["moduleGraphBinding"]["unexpected"] = json!(true);
        resign_generation(&mut value)?;
        assert!(parse_generation_v5(value).is_err());
        Ok(())
    }

    #[test]
    fn generation_v9_v10_bind_context_authority_and_admission_shape() -> anyhow::Result<()> {
        let graph_sha256 = "e".repeat(64);
        let cohort_id = "2".repeat(64);
        let schedule_sha256 = "3".repeat(64);
        let source_envelope_sha256 = "4".repeat(64);
        let mut contract =
            module_graph_cohort_contract(&cohort_id, &schedule_sha256, &source_envelope_sha256)?;
        let entry_path = contract["entries"][0]["entryPath"]
            .as_str()
            .context("cohort entry path")?
            .to_owned();
        let entry_graph_sha256 = contract["entries"][0]["localProfile"]["dependencyGraphSha256"]
            .as_str()
            .context("cohort entry graph identity")?
            .to_owned();
        let policy_fingerprint = "9".repeat(64);
        let application_analysis = json!({
            "entries": [entry_path.clone()],
            "kind": "convex-context-reuse-analysis",
            "policyFingerprint": policy_fingerprint.clone(),
            "resultSha256": "8".repeat(64),
        });
        let mut cohort_analysis = json!({
            "entries": [entry_path],
            "entryGraphSha256s": [entry_graph_sha256],
            "kind": "convex-context-reuse-cohort-analysis",
            "policyFingerprint": policy_fingerprint,
            "sharedAnalysisSha256": "7".repeat(64),
            "thirdPartyMaterialFingerprints": {},
        });
        resign_identity(&mut cohort_analysis, "resultSha256")?;
        contract["kind"] = json!("convex-wasm-module-graph-cohort-contract-v2");
        contract["schemaVersion"] = json!(2);
        contract
            .as_object_mut()
            .context("cohort contract object")?
            .insert("contextReuseAnalysis".into(), cohort_analysis);
        resign_identity(&mut contract, "cohortContractSha256")?;
        let contract_sha256 = contract["cohortContractSha256"]
            .as_str()
            .context("cohort contract identity")?
            .to_owned();
        let route_id = contract["routes"][0]["routeId"]
            .as_str()
            .context("cohort route ID")?
            .to_owned();

        let mut binding = json!({
            "bindingSha256": "0".repeat(64),
            "cohorts": [{
                "cohortContractSha256": contract_sha256,
                "cohortId": cohort_id,
                "graphManifestSha256": graph_sha256,
                "routeIds": [route_id.clone()],
            }],
            "contextReuseAnalysis": application_analysis.clone(),
            "kind": "convex-wasm-deployment-module-graph-binding-v2",
            "scheduleSha256": schedule_sha256,
            "schemaVersion": 2,
            "sourceEnvelopeSha256": source_envelope_sha256,
        });
        resign_identity(&mut binding, "bindingSha256")?;

        let mut primary = minimal_generation();
        let primary_object = primary.as_object_mut().context("generation object")?;
        primary_object.remove("capabilityEntryPackages");
        primary_object.insert("contextReuseAnalysis".into(), application_analysis.clone());
        primary_object.insert("kind".into(), json!(GENERATION_KIND_V9));
        primary_object.insert("moduleGraphBinding".into(), binding);
        primary_object.insert("moduleGraphCohorts".into(), json!([contract]));
        primary["graphRouting"] = json!({
            "contextReuseAnalysis": application_analysis,
            "cutoverReady": true,
            "cutoverState": "module-graph-authorized",
            "graphBackedRouteIds": [route_id.clone()],
            "kind": GRAPH_ROUTING_KIND,
            "missingRouteIds": [],
            "selectedRouteIds": [route_id.clone()],
            "selectedRuntime": "module-graph",
        });
        resign_generation(&mut primary)?;
        parse_generation_v9(primary.clone())?;

        let mut shadow = primary.clone();
        let shadow_object = shadow.as_object_mut().context("generation object")?;
        shadow_object.insert("kind".into(), json!(GENERATION_KIND_V10_SHADOW_ONLY));
        shadow_object.remove("graphRouting");
        shadow_object.insert("admission".into(), json!("shadow-only"));
        shadow_object.insert(
            "shadowRouting".into(),
            json!({
                "artifactComplete": true,
                "artifactState": "shadow-only",
                "kind": SHADOW_ROUTING_KIND,
                "routeIds": [route_id],
            }),
        );
        resign_generation(&mut shadow)?;
        assert_eq!(
            parse_generation_v10(shadow.clone())?.admission(),
            RuntimeRegistryAdmission::ShadowOnly
        );

        let mut mismatched_application_analysis = primary.clone();
        mismatched_application_analysis["contextReuseAnalysis"]["resultSha256"] =
            json!("6".repeat(64));
        resign_generation(&mut mismatched_application_analysis)?;
        assert!(parse_generation_v9(mismatched_application_analysis).is_err());

        let mut invalid_cohort_analysis = primary.clone();
        invalid_cohort_analysis["moduleGraphCohorts"][0]["contextReuseAnalysis"]
            ["sharedAnalysisSha256"] = json!("5".repeat(64));
        resign_identity(
            &mut invalid_cohort_analysis["moduleGraphCohorts"][0],
            "cohortContractSha256",
        )?;
        resign_generation(&mut invalid_cohort_analysis)?;
        assert!(parse_generation_v9(invalid_cohort_analysis).is_err());

        let mut laundered_primary = primary.clone();
        laundered_primary["kind"] = json!("convex-wasm-runtime-registry-generation-v5");
        laundered_primary
            .as_object_mut()
            .context("generation object")?
            .remove("contextReuseAnalysis");
        laundered_primary["graphRouting"]
            .as_object_mut()
            .context("graph routing object")?
            .remove("contextReuseAnalysis");
        resign_generation(&mut laundered_primary)?;
        assert!(parse_generation_v5(laundered_primary).is_err());

        let mut laundered_shadow = shadow.clone();
        laundered_shadow["kind"] = json!(GENERATION_KIND_V6_SHADOW_ONLY);
        laundered_shadow
            .as_object_mut()
            .context("generation object")?
            .remove("contextReuseAnalysis");
        resign_generation(&mut laundered_shadow)?;
        assert!(parse_generation_v6(laundered_shadow).is_err());

        let mut primary_with_shadow_shape = shadow;
        primary_with_shadow_shape["kind"] = json!(GENERATION_KIND_V9);
        resign_generation(&mut primary_with_shadow_shape)?;
        assert!(parse_generation_v9(primary_with_shadow_shape).is_err());

        let mut shadow_with_primary_shape = primary;
        shadow_with_primary_shape["kind"] = json!(GENERATION_KIND_V10_SHADOW_ONLY);
        resign_generation(&mut shadow_with_primary_shape)?;
        assert!(parse_generation_v10(shadow_with_primary_shape).is_err());
        Ok(())
    }

    pub(crate) fn module_graph_cohort_contract(
        cohort_id: &str,
        schedule_sha256: &str,
        source_envelope_sha256: &str,
    ) -> anyhow::Result<JsonValue> {
        let invocation_abi = "convex-sdk-registration-wrapper-tagged-json-v1";
        let local_profile_sha256 = "a".repeat(64);
        let entry_path = "convex/example.ts";
        let module_path = "example";
        let entry_id = sha256(&canonical_bytes(&json!({
            "domain": "convex-wasm-capability-entry-v1",
            "invocationAbi": invocation_abi,
            "localProfileSha256": local_profile_sha256,
            "selectedEntry": {
                "entryPath": entry_path,
                "modulePath": module_path,
            },
        }))?);
        let entry_symbol = format!("sh_export_convex_wasm_entry_{entry_id}");
        let export_name = "lookup";
        let udf_kind = "query";
        let visibility = "public";
        let route_id = sha256(&canonical_bytes(&json!({
            "domain": "convex-wasm-capability-route-v1",
            "entryId": entry_id,
            "exportName": export_name,
            "udfKind": udf_kind,
            "visibility": visibility,
        }))?);
        let selector_sha256 = sha256(&canonical_bytes(&json!({
            "domain": "convex-wasm-capability-selector-member-v1",
            "entrySymbol": entry_symbol,
            "handlerExportName": export_name,
            "handlerUdfKind": udf_kind,
            "invocationAbi": invocation_abi,
        }))?);
        let entry_selector_id = &selector_sha256[..16];
        let request_envelope = json!({
            "capabilityRequestAbiVersion": 4,
            "canonicalVectorCorpus": {
                "kind": "convex-wasm-canonical-capability-request-envelope-vector-corpus-v4",
                "producer": {
                    "kind": "convex-sdk-backend-capability-request-envelope-producer-v4",
                    "sourceSha256": "1".repeat(64),
                },
                "schemaVersion": 1,
                "sha256": "2".repeat(64),
            },
            "generatedSource": {"sha256": "3".repeat(64), "size": 1},
            "kind": "convex-wasm-capability-request-envelope-identity-v4",
            "loweringPipelineSha256": "d".repeat(64),
            "schemaVersion": 1,
        });
        let value_codec = json!({
            "canonicalVectorCorpus": {
                "kind": "convex-wasm-canonical-convex-value-vector-corpus-v1",
                "producer": {
                    "kind": "convex-sdk-backend-canonical-value-producer-v1",
                    "sourceSha256": "4".repeat(64),
                },
                "schemaVersion": 1,
                "sha256": "5".repeat(64),
            },
            "generatedSource": {"sha256": "6".repeat(64), "size": 1},
            "kind": "convex-wasm-guest-native-json-codec-identity",
            "loweringPipelineSha256": "d".repeat(64),
            "schemaVersion": 1,
        });
        let precompiler_material_identity = test_precompiler_material_identity_json();
        let engine_config = json!({
            "consumeFuel": true,
            "epochInterruption": true,
            "profilingStrategy": "perf-map",
            "wasmExceptions": true,
        });
        let payload = json!({
            "cohortId": cohort_id,
            "compiler": {
                "admittedLanguageVersion": 1,
                "artifactPipelineSha256": "9".repeat(64),
                "compilerRevision": "compiler-test-revision",
                "loweringPipelineSha256": "d".repeat(64),
                "sourcePipelineSha256": local_profile_sha256,
                "staticHermesGlobalPolicy": {
                    "inventorySha256": MODULE_GRAPH_RUNTIME_SURFACE_INVENTORY_SHA256,
                    "kind": "convex-wasm-runtime-surface-policy-identity",
                    "runtimeSurfacePolicySha256": MODULE_GRAPH_RUNTIME_SURFACE_POLICY_SHA256,
                },
                "staticHermesRevision": "static-hermes-test-revision",
            },
            "compilerSourceEnvelopeSha256": source_envelope_sha256,
            "descriptorIdentitySha256": "7".repeat(64),
            "engine": {
                "compatibilitySha256": "6".repeat(64),
                "config": engine_config,
                "configurationSha256": sha256(&canonical_bytes(&engine_config)?),
                "package": precompiler_material_identity,
                "revision": "wasmtime-test-revision",
                "target": {"cpu": "x86-64-v3", "triple": "x86_64-unknown-linux-gnu"},
                "wasmtimeMaterialsSha256": "5".repeat(64),
            },
            "entries": [{
                "entryId": entry_id,
                "entryPath": entry_path,
                "entrySymbol": entry_symbol,
                "invocationAbi": invocation_abi,
                "localProfile": {
                    "dependencyGraphSha256": "d".repeat(64),
                    "javascript": {"sha256": "e".repeat(64), "size": 1},
                    "metafileSha256": "f".repeat(64),
                    "sha256": local_profile_sha256,
                    "sourceMap": {"sha256": "1".repeat(64), "size": 1},
                },
                "modulePath": module_path,
                "source": {"sha256": "2".repeat(64)},
            }],
            "execution": {
                "effectExecutionMode": "guest-promise-event-loop",
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
                "platformLimits": {
                    "argumentBytes": 1,
                    "documentsRead": 1,
                    "documentsWritten": 1,
                    "executionTimeMs": 1,
                    "readBytes": 1,
                    "resultBytes": 1,
                    "scheduledArgumentBytes": 1,
                    "scheduledFunctions": 1,
                    "writeBytes": 1,
                },
                "requestEnvelope": request_envelope,
                "valueCodec": value_codec,
                "valueMode": "guest-native-json",
            },
            "kind": "convex-wasm-module-graph-cohort-contract-v1",
            "precompilerMaterialIdentity": precompiler_material_identity,
            "producerImplementation": {
                "kind": "convex-wasm-artifact-producer-identity-v1",
                "sha256": "4".repeat(64),
            },
            "routes": [{
                "entryId": entry_id,
                "entrySelectorId": entry_selector_id,
                "entrySymbol": entry_symbol,
                "exportName": export_name,
                "routeId": route_id,
                "udfKind": udf_kind,
                "visibility": visibility,
            }],
            "runtimeSurfacePolicySha256": MODULE_GRAPH_RUNTIME_SURFACE_POLICY_SHA256,
            "scheduleSha256": schedule_sha256,
            "schemaVersion": 1,
            "sourceEnvelopeSha256": source_envelope_sha256,
            "sourcePipelineSha256": local_profile_sha256,
        });
        let mut contract = payload.clone();
        contract
            .as_object_mut()
            .context("cohort contract object")?
            .insert(
                "cohortContractSha256".into(),
                json!(sha256(&canonical_bytes(&payload)?)),
            );
        Ok(contract)
    }

    fn minimal_generation() -> JsonValue {
        let route = "a".repeat(64);
        let mut value = json!({
            "capabilityEntryPackages": [{
                "files": [
                    file("COMPLETE"),
                    file("build-provenance.json"),
                    file("entry-manifest.json"),
                    file("module.cwasm"),
                    file("module.wasm"),
                    file("package-entry.json"),
                ],
                "packageKey": "b".repeat(64),
            }],
            "deploymentManifest": {
                "deploymentSha256": "c".repeat(64),
                "sha256": "d".repeat(64),
                "size": 1,
            },
            "generationSha256": "0".repeat(64),
            "graphRouting": {
                "cutoverReady": true,
                "cutoverState": "graph-complete-deployment-reference-required",
                "graphBackedRouteIds": [route.clone()],
                "kind": "convex-wasm-runtime-registry-graph-routing-boundary-v1",
                "missingRouteIds": [],
                "selectedRouteIds": [route],
                "selectedRuntime": "monolithic-package",
            },
            "kind": "convex-wasm-runtime-registry-generation-v4",
            "moduleGraphs": [{
                "artifacts": [
                    artifact("base", "coreWasm"), artifact("base", "aot"),
                    artifact("common", "coreWasm"), artifact("common", "aot"),
                    artifact("leaf", "coreWasm"), artifact("leaf", "aot"),
                ],
                "graphManifestSha256": "e".repeat(64),
                "package": {"files": [
                    file("COMPLETE"), file("build-provenance.json"),
                    file("graph-manifest.json"), file("package-entry.json"),
                ]},
            }],
        });
        resign_generation(&mut value).expect("minimal generation serializes");
        value
    }

    fn file(name: &str) -> JsonValue {
        json!({"name": name, "sha256": "f".repeat(64), "size": 1})
    }

    fn artifact(role: &str, kind: &str) -> JsonValue {
        let artifact_file = if kind == "coreWasm" {
            "artifact.wasm"
        } else {
            "artifact.cwasm"
        };
        let suffix = if kind == "coreWasm" {
            "core-wasm"
        } else {
            "wasmtime-aot"
        };
        json!({
            "cacheKey": "1".repeat(64),
            "files": [file("COMPLETE"), file(artifact_file), file("entry.json")],
            "kind": kind,
            "role": role,
            "sha256": "f".repeat(64),
            "size": 1,
            "stage": format!("module-graph-{role}-{suffix}"),
        })
    }

    fn resign_generation(value: &mut JsonValue) -> anyhow::Result<()> {
        resign_identity(value, "generationSha256")
    }

    fn resign_identity(value: &mut JsonValue, field: &str) -> anyhow::Result<()> {
        value
            .as_object_mut()
            .context("identity object")?
            .remove(field);
        let identity = sha256(&canonical_bytes(value)?);
        value
            .as_object_mut()
            .context("identity object")?
            .insert(field.into(), json!(identity));
        Ok(())
    }

    pub(super) fn materialize_fixture(fixture: &JsonValue, root: &Path) -> anyhow::Result<()> {
        create_private_directory(root)?;
        for name in [
            "generations",
            "packages",
            "module-graph-cache",
            "module-graph-cache/immutable",
            "module-graph-cache/immutable/v6",
            "module-graph-cache/immutable/v6/packages",
            "module-graph-cache/immutable/v6/artifacts",
        ] {
            create_private_directory(&root.join(name))?;
        }
        let material = &fixture["material"];
        anyhow::ensure!(
            material.get("capabilityEntryPackage").is_none(),
            "generation-v5 fixture material contains a legacy capability-entry package"
        );
        let graph = &material["moduleGraph"];
        materialize_files(
            &root
                .join("module-graph-cache/immutable/v6/packages")
                .join(graph["graphManifestSha256"].as_str().context("graph key")?),
            graph["packageFiles"]
                .as_array()
                .context("graph package files")?,
        )?;
        for artifact in graph["artifactEntries"]
            .as_array()
            .context("graph artifacts")?
        {
            let stage = artifact["stage"].as_str().context("artifact stage")?;
            let stage_root = root
                .join("module-graph-cache/immutable/v6/artifacts")
                .join(stage);
            create_private_directory(&stage_root)?;
            materialize_files(
                &stage_root.join(artifact["cacheKey"].as_str().context("artifact key")?),
                artifact["files"].as_array().context("artifact files")?,
            )?;
        }
        let generation = &fixture["expected"]["registryGenerationV5"];
        let deployment_sha256 = generation["deploymentManifest"]["deploymentSha256"]
            .as_str()
            .context("deployment SHA-256")?;
        let generation_sha256 = generation["generationSha256"]
            .as_str()
            .context("generation SHA-256")?;
        let deployment_root = root.join("generations").join(deployment_sha256);
        create_private_directory(&deployment_root)?;
        let generation_root = deployment_root.join(generation_sha256);
        create_private_directory(&generation_root)?;
        let deployment = base64::decode(
            material["deployment"]["base64"]
                .as_str()
                .context("deployment base64")?,
        )?;
        write_private(&generation_root.join("deployment.json"), &deployment)?;
        let mut generation_bytes = canonical_bytes(generation)?;
        generation_bytes.push(b'\n');
        write_private(&generation_root.join("generation.json"), &generation_bytes)?;
        write_private(
            &generation_root.join("COMPLETE"),
            format!("{generation_sha256}\n").as_bytes(),
        )?;
        let mut current = json!({
            "deploymentSha256": deployment_sha256,
            "generation": {
                "sha256": sha256(&generation_bytes),
                "size": generation_bytes.len(),
            },
            "generationSha256": generation_sha256,
            "kind": "convex-wasm-runtime-registry-current-v1",
        });
        let current_sha256 = sha256(&canonical_bytes(&current)?);
        current
            .as_object_mut()
            .context("current object")?
            .insert("currentSha256".into(), json!(current_sha256));
        let mut current_bytes = canonical_bytes(&current)?;
        current_bytes.push(b'\n');
        write_private(&root.join("current"), &current_bytes)?;
        Ok(())
    }

    fn materialize_files(root: &Path, files: &[JsonValue]) -> anyhow::Result<()> {
        create_private_directory(root)?;
        for file in files {
            let name = file["name"].as_str().context("material file name")?;
            let bytes = base64::decode(file["base64"].as_str().context("material base64")?)?;
            write_private(&root.join(name), &bytes)?;
        }
        Ok(())
    }

    fn create_private_directory(path: &Path) -> anyhow::Result<()> {
        fs::create_dir_all(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        let mut options = OpenOptions::new();
        options.create(true).write(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }
}
