use std::collections::BTreeSet;

use common::knobs::{
    DATABASE_UDF_USER_TIMEOUT,
    FUNCTION_MAX_ARGS_SIZE,
    FUNCTION_MAX_RESULT_SIZE,
};
use database::TransactionLimits;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value as JsonValue;
use thiserror::Error;
use value::FieldPath;

pub(crate) const MANIFEST_SCHEMA_VERSION: u32 = 3;
pub(crate) const COHORT_MANIFEST_SCHEMA_VERSION: u32 = 4;
pub(crate) const COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION: u32 = 5;
pub(crate) const EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION: u32 = 6;
// Version 3 adds the invocation Unix timestamp host import.
pub(crate) const OPAQUE_VALUE_ABI_VERSION: u32 = 3;

const MAX_MANIFEST_BYTES: usize = 256 * 1024;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_STRING_BYTES: usize = 4 * 1024;
const MAX_IMPORTED_OPERATIONS: usize = 256;
const MAX_IMPORTED_OPERATION_ID: u32 = u16::MAX as u32;
pub(crate) const MAX_QUERY_TAKE_LIMIT: u32 = 100_000;
const MAX_ADMISSION_DIAGNOSTICS: usize = 256;
const MAX_DEPENDENCY_CHAIN: usize = 64;
const MAX_GUEST_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
const MAX_HOST_OWNED_BYTES: u64 = 256 * 1024 * 1024;
const MAX_RESULT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_VALUE_HANDLES: u32 = 65_536;
const MAX_OPERATION_COUNT: u64 = 1_000_000;
const MAX_EXECUTION_FUEL: u64 = 10_000_000_000_000;
const MAX_TIMEOUT_MILLISECONDS: u64 = 60_000;
const MAX_CORE_WASM_BYTES: u64 = 320 * 1024 * 1024;
pub(crate) const MAX_SERIALIZED_MODULE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_PROVENANCE_BYTES: u64 = 256 * 1024;
const MAX_PLATFORM_LIMIT_VALUE: u64 = 9_007_199_254_740_991;

#[derive(Debug, Error)]
pub(crate) enum ManifestError {
    #[error("Wasm UDF execution manifest exceeds {maximum_bytes} bytes")]
    TooLarge { maximum_bytes: usize },
    #[error("Wasm UDF execution manifest is malformed: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("unsupported Wasm UDF execution manifest schema version {actual}")]
    UnsupportedSchemaVersion { actual: u32 },
    #[error("unsupported Wasm UDF opaque-value ABI version {actual}")]
    UnsupportedAbiVersion { actual: u32 },
    #[error("Wasm UDF execution manifest field {field} is invalid: {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error("Wasm UDF execution manifest is incompatible with this engine: {field}")]
    IncompatibleEngine { field: &'static str },
    #[error("Wasm UDF execution manifest is incompatible with backend platform limit {field}")]
    IncompatiblePlatformLimit { field: &'static str },
    #[error("Wasm UDF execution manifest has inconsistent routing data: {0}")]
    InconsistentRouting(&'static str),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct Sha256Fingerprint(String);

impl Sha256Fingerprint {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self, field: &'static str) -> Result<(), ManifestError> {
        if self.0.len() != 64
            || !self
                .0
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid(
                field,
                "expected exactly 64 lowercase hexadecimal characters",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WasmUdfExecutionManifest {
    manifest_schema_version: u32,
    opaque_value_abi_version: u32,
    #[serde(default)]
    value_mode: ValueMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    effect_execution_mode: Option<EffectExecutionMode>,
    source: SourceIdentity,
    compiler: CompilerIdentity,
    artifact: Option<AotArtifactIdentity>,
    limits: ExecutionLimits,
    platform_limits: PlatformLimits,
    imported_operations: Vec<ImportedOperation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    route: Option<RuntimeRouteIdentity>,
    routing: RoutingDecision,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WasmUdfExecutionPolicy {
    effect_execution_mode: EffectExecutionMode,
    imported_operations: Vec<ImportedOperation>,
    limits: ExecutionLimits,
    platform_limits: PlatformLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_envelope: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value_codec: Option<JsonValue>,
    value_mode: ValueMode,
}

impl WasmUdfExecutionPolicy {
    pub(crate) fn from_legacy(manifest: &WasmUdfExecutionManifest) -> Self {
        Self {
            effect_execution_mode: manifest.effect_execution_mode(),
            imported_operations: manifest.imported_operations.clone(),
            limits: manifest.limits.clone(),
            platform_limits: manifest.platform_limits,
            request_envelope: None,
            value_codec: None,
            value_mode: manifest.value_mode,
        }
    }

    pub(crate) fn validate_capability_entry(
        &self,
        opaque_value_abi_version: u32,
        runtime: &RuntimeCompatibility<'_>,
    ) -> Result<(), ManifestError> {
        self.validate_capability_entry_contract(opaque_value_abi_version)?;
        if opaque_value_abi_version != runtime.opaque_value_abi_version {
            return Err(ManifestError::UnsupportedAbiVersion {
                actual: opaque_value_abi_version,
            });
        }
        self.platform_limits
            .validate_compatible(&runtime.platform_limits)?;
        Ok(())
    }

    pub(crate) fn validate_capability_entry_contract(
        &self,
        opaque_value_abi_version: u32,
    ) -> Result<(), ManifestError> {
        if opaque_value_abi_version != OPAQUE_VALUE_ABI_VERSION {
            return Err(ManifestError::UnsupportedAbiVersion {
                actual: opaque_value_abi_version,
            });
        }
        if self.value_mode != ValueMode::GuestNativeJson {
            return Err(invalid(
                "execution.valueMode",
                "capability-entry invocation request and result values require guest-native-json",
            ));
        }
        if self.value_codec.is_none() {
            return Err(invalid(
                "execution.valueCodec",
                "capability entries require a guest-native codec identity",
            ));
        }
        if self.request_envelope.is_none() {
            return Err(invalid(
                "execution.requestEnvelope",
                "capability entries require a typed request-envelope identity",
            ));
        }
        if self.effect_execution_mode != EffectExecutionMode::GuestPromiseEventLoop {
            return Err(invalid(
                "execution.effectExecutionMode",
                "capability entries require guest-promise-event-loop",
            ));
        }
        if !self.imported_operations.is_empty() {
            return Err(invalid(
                "execution.importedOperations",
                "capability entries forbid compiler-assigned operations",
            ));
        }
        self.platform_limits.validate()?;
        self.limits.validate(&self.platform_limits)?;
        Ok(())
    }

    pub(crate) fn value_mode(&self) -> ValueMode {
        self.value_mode
    }

    pub(crate) fn value_codec(&self) -> Option<&JsonValue> {
        self.value_codec.as_ref()
    }

    pub(crate) fn request_envelope(&self) -> Option<&JsonValue> {
        self.request_envelope.as_ref()
    }

    pub(crate) fn effect_execution_mode(&self) -> EffectExecutionMode {
        self.effect_execution_mode
    }

    pub(crate) fn limits(&self) -> &ExecutionLimits {
        &self.limits
    }

    pub(crate) fn platform_limits(&self) -> &PlatformLimits {
        &self.platform_limits
    }

    pub(crate) fn imported_operations(&self) -> &[ImportedOperation] {
        &self.imported_operations
    }

    pub(crate) fn imported_operation(&self, id: u32) -> Option<&ImportedOperation> {
        self.imported_operations
            .iter()
            .find(|operation| operation.id == id)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeRouteIdentity {
    export_name: String,
    kind: String,
    runtime_module_path: String,
    udf_kind: UdfKind,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestSchema {
    manifest_schema_version: u32,
}

impl WasmUdfExecutionManifest {
    pub(crate) fn parse_for_runtime(
        bytes: &[u8],
        runtime: &RuntimeCompatibility<'_>,
    ) -> Result<Self, ManifestError> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(ManifestError::TooLarge {
                maximum_bytes: MAX_MANIFEST_BYTES,
            });
        }
        let schema: ManifestSchema = serde_json::from_slice(bytes)?;
        if !matches!(
            schema.manifest_schema_version,
            MANIFEST_SCHEMA_VERSION
                | COHORT_MANIFEST_SCHEMA_VERSION
                | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
                | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION
        ) {
            return Err(ManifestError::UnsupportedSchemaVersion {
                actual: schema.manifest_schema_version,
            });
        }
        let manifest: Self = serde_json::from_slice(bytes)?;
        manifest.validate(runtime)?;
        Ok(manifest)
    }

    pub(crate) fn source(&self) -> &SourceIdentity {
        &self.source
    }

    pub(crate) fn manifest_schema_version(&self) -> u32 {
        self.manifest_schema_version
    }

    pub(crate) fn opaque_value_abi_version(&self) -> u32 {
        self.opaque_value_abi_version
    }

    pub(crate) fn value_mode(&self) -> ValueMode {
        self.value_mode
    }

    pub(crate) fn effect_execution_mode(&self) -> EffectExecutionMode {
        self.effect_execution_mode.unwrap_or_default()
    }

    pub(crate) fn compiler(&self) -> &CompilerIdentity {
        &self.compiler
    }

    pub(crate) fn artifact(&self) -> Option<&AotArtifactIdentity> {
        self.artifact.as_ref()
    }

    pub(crate) fn limits(&self) -> &ExecutionLimits {
        &self.limits
    }

    pub(crate) fn platform_limits(&self) -> &PlatformLimits {
        &self.platform_limits
    }

    pub(crate) fn imported_operations(&self) -> &[ImportedOperation] {
        &self.imported_operations
    }

    pub(crate) fn imported_operation(&self, id: u32) -> Option<&ImportedOperation> {
        self.imported_operations
            .iter()
            .find(|operation| operation.id == id)
    }

    pub(crate) fn routing(&self) -> &RoutingDecision {
        &self.routing
    }

    fn validate(&self, runtime: &RuntimeCompatibility<'_>) -> Result<(), ManifestError> {
        if !matches!(
            self.manifest_schema_version,
            MANIFEST_SCHEMA_VERSION
                | COHORT_MANIFEST_SCHEMA_VERSION
                | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
                | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION
        ) {
            return Err(ManifestError::UnsupportedSchemaVersion {
                actual: self.manifest_schema_version,
            });
        }
        if self.opaque_value_abi_version != OPAQUE_VALUE_ABI_VERSION
            || self.opaque_value_abi_version != runtime.opaque_value_abi_version
        {
            return Err(ManifestError::UnsupportedAbiVersion {
                actual: self.opaque_value_abi_version,
            });
        }

        self.source.validate()?;
        self.compiler.validate()?;
        self.platform_limits.validate()?;
        self.platform_limits
            .validate_compatible(&runtime.platform_limits)?;
        self.limits.validate(&self.platform_limits)?;
        validate_imported_operations(
            &self.imported_operations,
            self.source.udf_kind,
            self.manifest_schema_version,
        )?;
        match self.manifest_schema_version {
            MANIFEST_SCHEMA_VERSION
            | COHORT_MANIFEST_SCHEMA_VERSION
            | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
                if self.effect_execution_mode.is_some() =>
            {
                return Err(invalid(
                    "effectExecutionMode",
                    "schemas 3 through 5 forbid effectExecutionMode",
                ));
            },
            EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION if self.effect_execution_mode.is_none() => {
                return Err(invalid(
                    "effectExecutionMode",
                    "schema 6 requires effectExecutionMode",
                ));
            },
            MANIFEST_SCHEMA_VERSION
            | COHORT_MANIFEST_SCHEMA_VERSION
            | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
            | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION => {},
            _ => unreachable!("manifest schema version was validated"),
        }
        if self.imported_operations.iter().any(|operation| {
            matches!(
                operation.operation(),
                ImportedOperationDescriptor::DatabaseIndexQuery {
                    terminal: QueryTerminal::Stream,
                    ..
                }
            )
        }) && self.effect_execution_mode() != EffectExecutionMode::GuestPromiseEventLoop
        {
            return Err(invalid(
                "importedOperations.operation.terminal",
                "query streams require guest-promise-event-loop",
            ));
        }
        self.routing.validate()?;
        match self.manifest_schema_version {
            MANIFEST_SCHEMA_VERSION if self.route.is_some() => {
                return Err(invalid(
                    "route",
                    "schema version 3 forbids cohort route identity",
                ));
            },
            COHORT_MANIFEST_SCHEMA_VERSION
            | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
            | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION => {
                let route = self
                    .route
                    .as_ref()
                    .ok_or_else(|| invalid("route", "cohort schemas require route identity"))?;
                if route.kind != "convex-wasm-runtime-route-v1"
                    || route.runtime_module_path != self.source.runtime_module_path
                    || route.export_name != self.source.export_name
                    || route.udf_kind != self.source.udf_kind
                {
                    return Err(invalid(
                        "route",
                        "route identity differs from the source identity",
                    ));
                }
            },
            MANIFEST_SCHEMA_VERSION => {},
            _ => unreachable!("manifest schema version was validated"),
        }

        match (&self.routing, &self.artifact) {
            (RoutingDecision::Wasm, Some(artifact)) => {
                artifact.validate(runtime, self.manifest_schema_version)?;
            },
            (RoutingDecision::Wasm, None) => {
                return Err(ManifestError::InconsistentRouting(
                    "an eligible export must identify an AOT artifact",
                ));
            },
            (RoutingDecision::V8Fallback { .. }, Some(_)) => {
                return Err(ManifestError::InconsistentRouting(
                    "an ineligible export must not identify an executable artifact",
                ));
            },
            (RoutingDecision::V8Fallback { .. }, None) => {
                if !self.imported_operations.is_empty() {
                    return Err(ManifestError::InconsistentRouting(
                        "an ineligible export must not declare executable imports",
                    ));
                }
            },
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum ValueMode {
    #[default]
    #[serde(rename = "opaque")]
    Opaque,
    #[serde(rename = "guest-native-json")]
    GuestNativeJson,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum EffectExecutionMode {
    #[default]
    #[serde(rename = "blocking-fiber")]
    BlockingFiber,
    #[serde(rename = "guest-promise-event-loop")]
    GuestPromiseEventLoop,
}

pub(crate) struct RuntimeCompatibility<'a> {
    pub(crate) opaque_value_abi_version: u32,
    pub(crate) platform_limits: PlatformLimits,
    pub(crate) wasmtime_revision: &'a str,
    pub(crate) target_triple: &'a str,
    pub(crate) target_cpu: &'a str,
    pub(crate) engine_configuration_sha256: &'a str,
    pub(crate) engine_compatibility_sha256: &'a str,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PlatformLimits {
    pub(crate) execution_time_ms: u64,
    pub(crate) argument_bytes: u64,
    pub(crate) result_bytes: u64,
    pub(crate) documents_read: u64,
    pub(crate) read_bytes: u64,
    pub(crate) documents_written: u64,
    pub(crate) write_bytes: u64,
    pub(crate) scheduled_functions: u64,
    pub(crate) scheduled_argument_bytes: u64,
}

impl PlatformLimits {
    pub(crate) fn authoritative() -> anyhow::Result<Self> {
        let transaction_limits = TransactionLimits::default();
        let as_u64 = |field: &'static str, value: usize| {
            u64::try_from(value)
                .map_err(|_| anyhow::anyhow!("backend platform limit {field} exceeds u64"))
        };
        Ok(Self {
            execution_time_ms: u64::try_from(DATABASE_UDF_USER_TIMEOUT.as_millis())
                .map_err(|_| anyhow::anyhow!("backend execution-time limit exceeds u64"))?,
            argument_bytes: as_u64("argumentBytes", *FUNCTION_MAX_ARGS_SIZE)?,
            result_bytes: as_u64("resultBytes", *FUNCTION_MAX_RESULT_SIZE)?,
            documents_read: as_u64("documentsRead", transaction_limits.documents_read)?,
            read_bytes: as_u64("readBytes", transaction_limits.bytes_read)?,
            documents_written: as_u64("documentsWritten", transaction_limits.documents_written)?,
            write_bytes: as_u64("writeBytes", transaction_limits.bytes_written)?,
            scheduled_functions: as_u64(
                "scheduledFunctions",
                transaction_limits.functions_scheduled,
            )?,
            scheduled_argument_bytes: as_u64(
                "scheduledArgumentBytes",
                transaction_limits.scheduled_function_args_bytes,
            )?,
        })
    }

    fn validate(&self) -> Result<(), ManifestError> {
        for (field, value) in self.fields() {
            validate_nonzero_max(field, value, MAX_PLATFORM_LIMIT_VALUE)?;
        }
        Ok(())
    }

    fn validate_compatible(&self, authoritative: &Self) -> Result<(), ManifestError> {
        for ((field, actual), (_, expected)) in
            self.fields().into_iter().zip(authoritative.fields())
        {
            if actual != expected {
                return Err(ManifestError::IncompatiblePlatformLimit { field });
            }
        }
        Ok(())
    }

    fn fields(&self) -> [(&'static str, u64); 9] {
        [
            ("platformLimits.executionTimeMs", self.execution_time_ms),
            ("platformLimits.argumentBytes", self.argument_bytes),
            ("platformLimits.resultBytes", self.result_bytes),
            ("platformLimits.documentsRead", self.documents_read),
            ("platformLimits.readBytes", self.read_bytes),
            ("platformLimits.documentsWritten", self.documents_written),
            ("platformLimits.writeBytes", self.write_bytes),
            (
                "platformLimits.scheduledFunctions",
                self.scheduled_functions,
            ),
            (
                "platformLimits.scheduledArgumentBytes",
                self.scheduled_argument_bytes,
            ),
        ]
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SourceIdentity {
    resolved_graph_sha256: Sha256Fingerprint,
    export_sha256: Sha256Fingerprint,
    module_path: String,
    runtime_module_path: String,
    export_name: String,
    udf_kind: UdfKind,
}

impl SourceIdentity {
    pub(crate) fn resolved_graph_sha256(&self) -> &Sha256Fingerprint {
        &self.resolved_graph_sha256
    }

    pub(crate) fn export_sha256(&self) -> &Sha256Fingerprint {
        &self.export_sha256
    }

    pub(crate) fn module_path(&self) -> &str {
        &self.module_path
    }

    pub(crate) fn export_name(&self) -> &str {
        &self.export_name
    }

    pub(crate) fn runtime_module_path(&self) -> &str {
        &self.runtime_module_path
    }

    pub(crate) fn udf_kind(&self) -> UdfKind {
        self.udf_kind
    }

    fn validate(&self) -> Result<(), ManifestError> {
        self.resolved_graph_sha256
            .validate("source.resolvedGraphSha256")?;
        self.export_sha256.validate("source.exportSha256")?;
        validate_string(
            "source.modulePath",
            &self.module_path,
            MAX_STRING_BYTES,
            true,
        )?;
        validate_string(
            "source.exportName",
            &self.export_name,
            MAX_IDENTIFIER_BYTES,
            false,
        )?;
        validate_string(
            "source.runtimeModulePath",
            &self.runtime_module_path,
            MAX_STRING_BYTES,
            false,
        )?;
        if !self.runtime_module_path.ends_with(".js")
            || self.runtime_module_path.starts_with('/')
            || self.runtime_module_path.contains('\\')
            || self
                .runtime_module_path
                .split('/')
                .any(|component| matches!(component, "" | "." | ".."))
        {
            return Err(invalid(
                "source.runtimeModulePath",
                "expected a normalized relative .js path",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum UdfKind {
    Query,
    Mutation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CompilerIdentity {
    artifact_pipeline_sha256: Sha256Fingerprint,
    compiler_revision: String,
    lowering_pipeline_sha256: Sha256Fingerprint,
    source_pipeline_sha256: Sha256Fingerprint,
    static_hermes_revision: String,
    admitted_language_version: u32,
}

impl CompilerIdentity {
    pub(crate) fn artifact_pipeline_sha256(&self) -> &Sha256Fingerprint {
        &self.artifact_pipeline_sha256
    }

    pub(crate) fn lowering_pipeline_sha256(&self) -> &Sha256Fingerprint {
        &self.lowering_pipeline_sha256
    }

    pub(crate) fn source_pipeline_sha256(&self) -> &Sha256Fingerprint {
        &self.source_pipeline_sha256
    }

    pub(crate) fn compiler_revision(&self) -> &str {
        &self.compiler_revision
    }

    pub(crate) fn static_hermes_revision(&self) -> &str {
        &self.static_hermes_revision
    }

    pub(crate) fn admitted_language_version(&self) -> u32 {
        self.admitted_language_version
    }

    fn validate(&self) -> Result<(), ManifestError> {
        self.artifact_pipeline_sha256
            .validate("compiler.artifactPipelineSha256")?;
        self.lowering_pipeline_sha256
            .validate("compiler.loweringPipelineSha256")?;
        self.source_pipeline_sha256
            .validate("compiler.sourcePipelineSha256")?;
        validate_string(
            "compiler.compilerRevision",
            &self.compiler_revision,
            MAX_IDENTIFIER_BYTES,
            false,
        )?;
        validate_string(
            "compiler.staticHermesRevision",
            &self.static_hermes_revision,
            MAX_IDENTIFIER_BYTES,
            false,
        )?;
        if self.admitted_language_version == 0 {
            return Err(invalid(
                "compiler.admittedLanguageVersion",
                "version must be nonzero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AotArtifactIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cohort: Option<CohortArtifactIdentity>,
    core_wasm_sha256: Sha256Fingerprint,
    engine_compatibility_sha256: Sha256Fingerprint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entry_id: Option<Sha256Fingerprint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entry_selection_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entry_selector_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entry_symbol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provenance_sha256: Option<Sha256Fingerprint>,
    serialized_module_sha256: Sha256Fingerprint,
    wasmtime_revision: String,
    target_triple: String,
    target_cpu: String,
    engine_configuration_sha256: Sha256Fingerprint,
    core_wasm_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provenance_bytes: Option<u64>,
    serialized_module_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CohortArtifactIdentity {
    bucket: u32,
    manifest_sha256: Sha256Fingerprint,
    package_id: Sha256Fingerprint,
    partition_policy: CohortPartitionPolicy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CohortPartitionPolicy {
    bucket_count: u32,
    hash: String,
    kind: String,
}

impl AotArtifactIdentity {
    pub(crate) fn core_wasm_sha256(&self) -> &Sha256Fingerprint {
        &self.core_wasm_sha256
    }

    pub(crate) fn engine_compatibility_sha256(&self) -> &Sha256Fingerprint {
        &self.engine_compatibility_sha256
    }

    pub(crate) fn engine_configuration_sha256(&self) -> &Sha256Fingerprint {
        &self.engine_configuration_sha256
    }

    pub(crate) fn serialized_module_sha256(&self) -> &Sha256Fingerprint {
        &self.serialized_module_sha256
    }

    pub(crate) fn provenance_sha256(&self) -> Option<&Sha256Fingerprint> {
        self.provenance_sha256.as_ref()
    }

    pub(crate) fn core_wasm_bytes(&self) -> u64 {
        self.core_wasm_bytes
    }

    pub(crate) fn serialized_module_bytes(&self) -> u64 {
        self.serialized_module_bytes
    }

    pub(crate) fn provenance_bytes(&self) -> Option<u64> {
        self.provenance_bytes
    }

    pub(crate) fn cohort(&self) -> Option<&CohortArtifactIdentity> {
        self.cohort.as_ref()
    }

    pub(crate) fn entry_id(&self) -> Option<&Sha256Fingerprint> {
        self.entry_id.as_ref()
    }

    pub(crate) fn entry_selection_index(&self) -> Option<u32> {
        self.entry_selection_index
    }

    pub(crate) fn entry_selector_id(&self) -> Option<&str> {
        self.entry_selector_id.as_deref()
    }

    pub(crate) fn entry_symbol(&self) -> Option<&str> {
        self.entry_symbol.as_deref()
    }

    pub(crate) fn target_cpu(&self) -> &str {
        &self.target_cpu
    }

    pub(crate) fn target_triple(&self) -> &str {
        &self.target_triple
    }

    fn validate(
        &self,
        runtime: &RuntimeCompatibility<'_>,
        manifest_schema_version: u32,
    ) -> Result<(), ManifestError> {
        self.core_wasm_sha256.validate("artifact.coreWasmSha256")?;
        self.engine_compatibility_sha256
            .validate("artifact.engineCompatibilitySha256")?;
        if let Some(provenance_sha256) = &self.provenance_sha256 {
            provenance_sha256.validate("artifact.provenanceSha256")?;
        }
        self.serialized_module_sha256
            .validate("artifact.serializedModuleSha256")?;
        self.engine_configuration_sha256
            .validate("artifact.engineConfigurationSha256")?;
        validate_string(
            "artifact.wasmtimeRevision",
            &self.wasmtime_revision,
            MAX_IDENTIFIER_BYTES,
            false,
        )?;
        validate_string(
            "artifact.targetTriple",
            &self.target_triple,
            MAX_IDENTIFIER_BYTES,
            false,
        )?;
        validate_string(
            "artifact.targetCpu",
            &self.target_cpu,
            MAX_IDENTIFIER_BYTES,
            false,
        )?;
        validate_nonzero_max(
            "artifact.coreWasmBytes",
            self.core_wasm_bytes,
            MAX_CORE_WASM_BYTES,
        )?;
        if let Some(provenance_bytes) = self.provenance_bytes {
            validate_nonzero_max(
                "artifact.provenanceBytes",
                provenance_bytes,
                MAX_PROVENANCE_BYTES,
            )?;
        }
        validate_nonzero_max(
            "artifact.serializedModuleBytes",
            self.serialized_module_bytes,
            MAX_SERIALIZED_MODULE_BYTES,
        )?;

        match manifest_schema_version {
            MANIFEST_SCHEMA_VERSION => {
                if self.cohort.is_some()
                    || self.entry_id.is_some()
                    || self.entry_selection_index.is_some()
                    || self.entry_selector_id.is_some()
                    || self.entry_symbol.is_some()
                    || self.provenance_sha256.is_none()
                    || self.provenance_bytes.is_none()
                {
                    return Err(invalid(
                        "artifact",
                        "schema version 3 requires singleton provenance and forbids cohort fields",
                    ));
                }
            },
            COHORT_MANIFEST_SCHEMA_VERSION
            | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
            | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION => {
                let cohort = self.cohort.as_ref().ok_or_else(|| {
                    invalid("artifact.cohort", "cohort schemas require cohort identity")
                })?;
                cohort.validate()?;
                self.entry_id
                    .as_ref()
                    .ok_or_else(|| invalid("artifact.entryId", "field is required"))?
                    .validate("artifact.entryId")?;
                let entry_selector_id = self
                    .entry_selector_id
                    .as_deref()
                    .ok_or_else(|| invalid("artifact.entrySelectorId", "field is required"))?;
                if entry_selector_id.len() != 16
                    || !entry_selector_id
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                {
                    return Err(invalid(
                        "artifact.entrySelectorId",
                        "expected exactly 16 lowercase hexadecimal characters",
                    ));
                }
                validate_string(
                    "artifact.entrySymbol",
                    self.entry_symbol
                        .as_deref()
                        .ok_or_else(|| invalid("artifact.entrySymbol", "field is required"))?,
                    MAX_IDENTIFIER_BYTES,
                    false,
                )?;
                if self.entry_selection_index.is_none() {
                    return Err(invalid("artifact.entrySelectionIndex", "field is required"));
                }
                if self.provenance_sha256.is_some() || self.provenance_bytes.is_some() {
                    return Err(invalid(
                        "artifact",
                        "cohort schemas forbid singleton provenance fields",
                    ));
                }
            },
            _ => unreachable!("manifest schema version was validated"),
        }

        for (matches, field) in [
            (
                self.wasmtime_revision == runtime.wasmtime_revision,
                "artifact.wasmtimeRevision",
            ),
            (
                self.target_triple == runtime.target_triple,
                "artifact.targetTriple",
            ),
            (self.target_cpu == runtime.target_cpu, "artifact.targetCpu"),
            (
                self.engine_configuration_sha256.as_str() == runtime.engine_configuration_sha256,
                "artifact.engineConfigurationSha256",
            ),
            (
                self.engine_compatibility_sha256.as_str() == runtime.engine_compatibility_sha256,
                "artifact.engineCompatibilitySha256",
            ),
        ] {
            if !matches {
                return Err(ManifestError::IncompatibleEngine { field });
            }
        }
        Ok(())
    }
}

impl CohortArtifactIdentity {
    pub(crate) fn bucket(&self) -> u32 {
        self.bucket
    }

    pub(crate) fn manifest_sha256(&self) -> &Sha256Fingerprint {
        &self.manifest_sha256
    }

    pub(crate) fn package_id(&self) -> &Sha256Fingerprint {
        &self.package_id
    }

    fn validate(&self) -> Result<(), ManifestError> {
        self.manifest_sha256
            .validate("artifact.cohort.manifestSha256")?;
        self.package_id.validate("artifact.cohort.packageId")?;
        if self.package_id != self.manifest_sha256 {
            return Err(invalid(
                "artifact.cohort",
                "packageId and manifestSha256 must match",
            ));
        }
        if self.partition_policy.bucket_count != 16
            || self.partition_policy.hash != "sha256-route-id-domain-first-u32-be-modulo"
            || self.partition_policy.kind != "convex-wasm-cohort-partition-v1"
            || self.bucket >= self.partition_policy.bucket_count
        {
            return Err(invalid(
                "artifact.cohort.partitionPolicy",
                "unsupported cohort partition policy or bucket",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ExecutionLimits {
    max_guest_memory_bytes: u64,
    max_host_owned_bytes: u64,
    max_value_handles: u32,
    max_operation_count: u64,
    max_result_bytes: u64,
    execution_fuel: u64,
    timeout_milliseconds: u64,
}

impl ExecutionLimits {
    pub(crate) fn max_guest_memory_bytes(&self) -> u64 {
        self.max_guest_memory_bytes
    }

    pub(crate) fn max_host_owned_bytes(&self) -> u64 {
        self.max_host_owned_bytes
    }

    pub(crate) fn max_value_handles(&self) -> u32 {
        self.max_value_handles
    }

    pub(crate) fn max_operation_count(&self) -> u64 {
        self.max_operation_count
    }

    pub(crate) fn max_result_bytes(&self) -> u64 {
        self.max_result_bytes
    }

    pub(crate) fn execution_fuel(&self) -> u64 {
        self.execution_fuel
    }

    pub(crate) fn timeout_milliseconds(&self) -> u64 {
        self.timeout_milliseconds
    }

    fn validate(&self, platform_limits: &PlatformLimits) -> Result<(), ManifestError> {
        validate_nonzero_max(
            "limits.maxGuestMemoryBytes",
            self.max_guest_memory_bytes,
            MAX_GUEST_MEMORY_BYTES,
        )?;
        validate_nonzero_max(
            "limits.maxHostOwnedBytes",
            self.max_host_owned_bytes,
            MAX_HOST_OWNED_BYTES,
        )?;
        validate_nonzero_max(
            "limits.maxValueHandles",
            u64::from(self.max_value_handles),
            u64::from(MAX_VALUE_HANDLES),
        )?;
        validate_nonzero_max(
            "limits.maxOperationCount",
            self.max_operation_count,
            MAX_OPERATION_COUNT,
        )?;
        validate_nonzero_max(
            "limits.maxResultBytes",
            self.max_result_bytes,
            MAX_RESULT_BYTES,
        )?;
        validate_nonzero_max(
            "limits.executionFuel",
            self.execution_fuel,
            MAX_EXECUTION_FUEL,
        )?;
        validate_nonzero_max(
            "limits.timeoutMilliseconds",
            self.timeout_milliseconds,
            MAX_TIMEOUT_MILLISECONDS,
        )?;
        if self.max_result_bytes > platform_limits.result_bytes {
            return Err(invalid(
                "limits.maxResultBytes",
                "artifact result budget exceeds the backend platform result limit",
            ));
        }
        if self.timeout_milliseconds > platform_limits.execution_time_ms {
            return Err(invalid(
                "limits.timeoutMilliseconds",
                "artifact timeout exceeds the backend platform execution-time limit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "decision", rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum RoutingDecision {
    Wasm,
    V8Fallback {
        diagnostics: Vec<AdmissionDiagnostic>,
    },
}

impl RoutingDecision {
    pub(crate) fn is_wasm(&self) -> bool {
        matches!(self, Self::Wasm)
    }

    fn validate(&self) -> Result<(), ManifestError> {
        let Self::V8Fallback { diagnostics } = self else {
            return Ok(());
        };
        if diagnostics.is_empty() {
            return Err(invalid(
                "routing.diagnostics",
                "V8 fallback requires at least one static admission diagnostic",
            ));
        }
        if diagnostics.len() > MAX_ADMISSION_DIAGNOSTICS {
            return Err(invalid(
                "routing.diagnostics",
                format!("at most {MAX_ADMISSION_DIAGNOSTICS} diagnostics are allowed"),
            ));
        }
        for diagnostic in diagnostics {
            diagnostic.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AdmissionDiagnostic {
    code: AdmissionDiagnosticCode,
    subject: String,
    message: String,
    module_path: String,
    source_span: Option<SourceSpan>,
    dependency_chain: Vec<String>,
}

impl AdmissionDiagnostic {
    fn validate(&self) -> Result<(), ManifestError> {
        validate_string(
            "routing.diagnostics.subject",
            &self.subject,
            MAX_STRING_BYTES,
            true,
        )?;
        validate_string(
            "routing.diagnostics.message",
            &self.message,
            MAX_STRING_BYTES,
            true,
        )?;
        validate_string(
            "routing.diagnostics.modulePath",
            &self.module_path,
            MAX_STRING_BYTES,
            true,
        )?;
        if self.dependency_chain.len() > MAX_DEPENDENCY_CHAIN {
            return Err(invalid(
                "routing.diagnostics.dependencyChain",
                format!("at most {MAX_DEPENDENCY_CHAIN} modules are allowed"),
            ));
        }
        for module_path in &self.dependency_chain {
            validate_string(
                "routing.diagnostics.dependencyChain",
                module_path,
                MAX_STRING_BYTES,
                true,
            )?;
        }
        if let Some(source_span) = &self.source_span {
            source_span.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum AdmissionDiagnosticCode {
    UnsupportedImport,
    UnsupportedConstruct,
    UnsupportedGlobal,
    UnsupportedConvexOperation,
    UnsupportedAsyncShape,
    DynamicModuleResolution,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceSpan {
    start_line: u32,
    start_column: u32,
    end_line: u32,
    end_column: u32,
}

impl SourceSpan {
    fn validate(&self) -> Result<(), ManifestError> {
        if self.start_line == 0
            || self.start_column == 0
            || self.end_line == 0
            || self.end_column == 0
            || (self.end_line, self.end_column) < (self.start_line, self.start_column)
        {
            return Err(invalid(
                "routing.diagnostics.sourceSpan",
                "positions must be one-based and ordered",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ImportedOperation {
    id: u32,
    debug_name: String,
    operation: ImportedOperationDescriptor,
}

impl ImportedOperation {
    pub(crate) fn operation(&self) -> &ImportedOperationDescriptor {
        &self.operation
    }

    fn validate(&self, manifest_schema_version: u32) -> Result<(), ManifestError> {
        if !(1..=MAX_IMPORTED_OPERATION_ID).contains(&self.id) {
            return Err(invalid(
                "importedOperations.id",
                format!("operation IDs must be between 1 and {MAX_IMPORTED_OPERATION_ID}"),
            ));
        }
        validate_string(
            "importedOperations.debugName",
            &self.debug_name,
            MAX_STRING_BYTES,
            true,
        )?;
        self.operation.validate(manifest_schema_version)
    }
}

pub(crate) fn validate_imported_operations(
    imported_operations: &[ImportedOperation],
    udf_kind: UdfKind,
    manifest_schema_version: u32,
) -> Result<(), ManifestError> {
    if imported_operations.len() > MAX_IMPORTED_OPERATIONS {
        return Err(invalid(
            "importedOperations",
            format!("at most {MAX_IMPORTED_OPERATIONS} operations are allowed"),
        ));
    }
    let mut ids = BTreeSet::new();
    for operation in imported_operations {
        operation.validate(manifest_schema_version)?;
        if !ids.insert(operation.id) {
            return Err(invalid(
                "importedOperations.id",
                format!("operation ID {} is duplicated", operation.id),
            ));
        }
    }
    if udf_kind == UdfKind::Query
        && imported_operations.iter().any(|operation| {
            matches!(
                &operation.operation,
                ImportedOperationDescriptor::DatabaseInsert { .. }
                    | ImportedOperationDescriptor::DatabasePatch { .. }
                    | ImportedOperationDescriptor::DatabaseReplace { .. }
                    | ImportedOperationDescriptor::DatabaseDelete { .. }
                    | ImportedOperationDescriptor::SchedulerRunAfter { .. }
                    | ImportedOperationDescriptor::SchedulerRunAt { .. }
            )
        })
    {
        return Err(invalid(
            "importedOperations.operation.kind",
            "queries cannot declare database writes or scheduler operations",
        ));
    }
    Ok(())
}

pub(crate) const CONDITIONAL_CONVEX_IMPORTS: &[&str] = &[
    "convex_async_operation_cancel_all",
    "convex_async_query_stream_close",
    "convex_async_query_stream_next",
    "convex_async_query_stream_open_take",
    "convex_async_operation_completion_status",
    "convex_async_operation_completion_take",
    "convex_async_operation_poll_ready",
    "convex_async_operation_start_take",
    "convex_async_operation_wait_any",
    "convex_capability_current",
    "convex_console_message",
    "convex_capability_request_decode",
    "convex_capability_request_release",
    "convex_capability_query_stream_open_take",
    "convex_crypto_subtle_digest_sha256",
    "convex_capability_start_take",
    "convex_capability_sync_take",
    "convex_async_batch_take",
    "convex_db_get",
    "convex_db_normalize_id",
    "convex_db_write",
    "convex_function_handle_create",
    "convex_function_result",
    "convex_guest_value_decode",
    "convex_guest_value_encode",
    "convex_guest_value_payload_copy",
    "convex_guest_value_payload_len",
    "convex_guest_value_payload_release",
    "convex_guest_value_request_copy",
    "convex_guest_value_request_len",
    "convex_guest_value_result",
    "convex_host_secret_verify",
    "convex_query_next",
    "convex_query_start_utf8",
    "convex_query_start_value",
    "convex_request_field",
    "convex_scheduler_schedule",
    "convex_sha256_value",
];

pub(crate) fn permitted_conditional_convex_imports(
    value_mode: ValueMode,
    effect_execution_mode: EffectExecutionMode,
    imported_operations: &[ImportedOperation],
) -> BTreeSet<&'static str> {
    let mut permitted = match value_mode {
        ValueMode::Opaque => BTreeSet::from([
            "convex_capability_current",
            "convex_capability_request_decode",
            "convex_capability_request_release",
            "convex_function_result",
            "convex_request_field",
        ]),
        ValueMode::GuestNativeJson => BTreeSet::from([
            "convex_guest_value_request_len",
            "convex_guest_value_request_copy",
            "convex_capability_current",
            "convex_capability_request_decode",
            "convex_capability_request_release",
            "convex_guest_value_decode",
            "convex_guest_value_encode",
            "convex_guest_value_payload_len",
            "convex_guest_value_payload_copy",
            "convex_guest_value_payload_release",
            "convex_guest_value_result",
        ]),
    };
    let mut has_async_completion = false;
    let mut has_async_start = false;
    if effect_execution_mode == EffectExecutionMode::GuestPromiseEventLoop {
        permitted.extend([
            "convex_async_operation_cancel_all",
            "convex_async_operation_wait_any",
            "convex_async_operation_poll_ready",
            "convex_async_operation_completion_status",
            "convex_async_operation_completion_take",
            "convex_console_message",
            "convex_capability_query_stream_open_take",
            "convex_capability_start_take",
            "convex_capability_sync_take",
            "convex_async_query_stream_next",
            "convex_async_query_stream_close",
            "convex_crypto_subtle_digest_sha256",
        ]);
    }
    for operation in imported_operations {
        match (effect_execution_mode, operation.operation()) {
            (_, ImportedOperationDescriptor::Sha256) => {
                permitted.insert("convex_sha256_value");
            },
            (_, ImportedOperationDescriptor::HostSecretVerify { .. }) => {
                permitted.insert("convex_host_secret_verify");
            },
            (_, ImportedOperationDescriptor::DatabaseNormalizeId { .. }) => {
                permitted.insert("convex_db_normalize_id");
            },
            (
                EffectExecutionMode::GuestPromiseEventLoop,
                ImportedOperationDescriptor::AuthenticationGetUserIdentity {}
                | ImportedOperationDescriptor::FunctionHandleCreate {}
                | ImportedOperationDescriptor::DatabaseGet { .. }
                | ImportedOperationDescriptor::DatabaseInsert { .. }
                | ImportedOperationDescriptor::DatabasePatch { .. }
                | ImportedOperationDescriptor::DatabaseReplace { .. }
                | ImportedOperationDescriptor::DatabaseDelete { .. }
                | ImportedOperationDescriptor::SchedulerRunAfter { .. }
                | ImportedOperationDescriptor::SchedulerRunAt { .. },
            ) => {
                has_async_completion = true;
                has_async_start = true;
            },
            (
                EffectExecutionMode::GuestPromiseEventLoop,
                ImportedOperationDescriptor::DatabaseIndexQuery {
                    terminal: QueryTerminal::Stream,
                    ..
                },
            ) => {
                has_async_completion = true;
                permitted.extend([
                    "convex_async_query_stream_open_take",
                    "convex_async_query_stream_next",
                    "convex_async_query_stream_close",
                ]);
            },
            (
                EffectExecutionMode::GuestPromiseEventLoop,
                ImportedOperationDescriptor::DatabaseIndexQuery { .. },
            ) => {
                has_async_completion = true;
                has_async_start = true;
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::AuthenticationGetUserIdentity {},
            ) => {
                permitted.insert("convex_async_batch_take");
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::FunctionHandleCreate {},
            ) => {
                permitted.extend(["convex_async_batch_take", "convex_function_handle_create"]);
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::DatabaseIndexQuery { constraints, .. },
            ) => {
                permitted.extend([
                    "convex_async_batch_take",
                    "convex_query_start_value",
                    "convex_query_next",
                ]);
                if constraints.is_none() {
                    permitted.insert("convex_query_start_utf8");
                }
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::DatabaseGet { .. },
            ) => {
                permitted.extend(["convex_async_batch_take", "convex_db_get"]);
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::DatabaseInsert { .. }
                | ImportedOperationDescriptor::DatabasePatch { .. }
                | ImportedOperationDescriptor::DatabaseReplace { .. }
                | ImportedOperationDescriptor::DatabaseDelete { .. },
            ) => {
                permitted.extend(["convex_async_batch_take", "convex_db_write"]);
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::SchedulerRunAfter { .. }
                | ImportedOperationDescriptor::SchedulerRunAt { .. },
            ) => {
                permitted.extend(["convex_async_batch_take", "convex_scheduler_schedule"]);
            },
        }
    }
    if has_async_start {
        permitted.insert("convex_async_operation_start_take");
    }
    if has_async_completion {
        permitted.extend([
            "convex_async_operation_cancel_all",
            "convex_async_operation_wait_any",
            "convex_async_operation_poll_ready",
            "convex_async_operation_completion_status",
            "convex_async_operation_completion_take",
        ]);
    }
    permitted
}

/// Each compiler-assigned ID selects a static descriptor while the dynamic
/// operand crosses the ABI as an opaque handle. The host therefore implements
/// generic authentication, database, function-handle, and scheduler operations
/// rather than matching IDs to application-specific Rust functions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum ImportedOperationDescriptor {
    Sha256,
    AuthenticationGetUserIdentity {},
    HostSecretVerify {
        contract_version: u32,
        selector: String,
    },
    FunctionHandleCreate {},
    DatabaseNormalizeId {
        table_name: String,
    },
    DatabaseGet {
        table_name: String,
    },
    DatabaseInsert {
        table_name: String,
    },
    DatabasePatch {
        table_name: String,
    },
    DatabaseReplace {
        table_name: String,
    },
    DatabaseDelete {
        table_name: String,
    },
    DatabaseIndexQuery {
        table_name: String,
        index_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        equality_field: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        constraints: Option<Vec<QueryConstraint>>,
        order: QueryOrder,
        terminal: QueryTerminal,
        limit: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit_argument_index: Option<u32>,
    },
    SchedulerRunAfter {
        function_reference: String,
    },
    SchedulerRunAt {
        function_reference: String,
    },
}

impl ImportedOperationDescriptor {
    fn validate(&self, manifest_schema_version: u32) -> Result<(), ManifestError> {
        match self {
            Self::Sha256
            | Self::AuthenticationGetUserIdentity {}
            | Self::FunctionHandleCreate {} => {},
            Self::HostSecretVerify {
                contract_version,
                selector,
            } => {
                if *contract_version != 1
                    || !selector
                        .bytes()
                        .next()
                        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
                    || !selector
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                {
                    return Err(invalid(
                        "importedOperations.operation.hostSecretVerify",
                        "contractVersion must be 1 and selector must be an opaque host-secret \
                         selector",
                    ));
                }
                validate_string(
                    "importedOperations.operation.selector",
                    selector,
                    MAX_IDENTIFIER_BYTES,
                    false,
                )?;
            },
            Self::DatabaseNormalizeId { table_name }
            | Self::DatabaseGet { table_name }
            | Self::DatabaseInsert { table_name }
            | Self::DatabasePatch { table_name }
            | Self::DatabaseReplace { table_name }
            | Self::DatabaseDelete { table_name } => {
                validate_string(
                    "importedOperations.operation.tableName",
                    table_name,
                    MAX_IDENTIFIER_BYTES,
                    false,
                )?;
            },
            Self::DatabaseIndexQuery {
                table_name,
                index_name,
                equality_field,
                constraints,
                terminal,
                limit,
                limit_argument_index,
                ..
            } => {
                if *terminal == QueryTerminal::Stream
                    && (manifest_schema_version != EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION
                        || limit.is_some()
                        || limit_argument_index.is_some())
                {
                    return Err(invalid(
                        "importedOperations.operation.terminal",
                        "query streams require schema 6 and cannot declare a limit",
                    ));
                }
                for (field, value) in [
                    ("importedOperations.operation.tableName", table_name),
                    ("importedOperations.operation.indexName", index_name),
                ] {
                    validate_string(field, value, MAX_IDENTIFIER_BYTES, false)?;
                }
                match manifest_schema_version {
                    MANIFEST_SCHEMA_VERSION | COHORT_MANIFEST_SCHEMA_VERSION => {
                        let equality_field = equality_field.as_deref().ok_or_else(|| {
                            invalid(
                                "importedOperations.operation.equalityField",
                                "schemas 3 and 4 require equalityField",
                            )
                        })?;
                        if constraints.is_some() {
                            return Err(invalid(
                                "importedOperations.operation.constraints",
                                "schemas 3 and 4 forbid constraints",
                            ));
                        }
                        validate_string(
                            "importedOperations.operation.equalityField",
                            equality_field,
                            MAX_IDENTIFIER_BYTES,
                            false,
                        )?;
                    },
                    COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
                    | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION => {
                        if equality_field.is_some() {
                            return Err(invalid(
                                "importedOperations.operation.equalityField",
                                "schemas 5 and 6 forbid equalityField",
                            ));
                        }
                        validate_query_constraints(constraints.as_deref().ok_or_else(|| {
                            invalid(
                                "importedOperations.operation.constraints",
                                "schemas 5 and 6 require constraints",
                            )
                        })?)?;
                    },
                    _ => unreachable!("manifest schema version was validated"),
                }
                if limit.is_some_and(|limit| limit == 0 || limit > MAX_QUERY_TAKE_LIMIT) {
                    return Err(invalid(
                        "importedOperations.operation.limit",
                        "limit must be between 1 and 100000 when present",
                    ));
                }
                if let Some(limit_argument_index) = limit_argument_index {
                    if manifest_schema_version != EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION {
                        return Err(invalid(
                            "importedOperations.operation.limitArgumentIndex",
                            "only schema 6 supports a dynamic query limit",
                        ));
                    }
                    if limit.is_some() {
                        return Err(invalid(
                            "importedOperations.operation.limitArgumentIndex",
                            "a dynamic query limit cannot be combined with a static limit",
                        ));
                    }
                    if *terminal != QueryTerminal::Collect {
                        return Err(invalid(
                            "importedOperations.operation.limitArgumentIndex",
                            "a dynamic query limit requires the collect terminal",
                        ));
                    }
                    let constraint_count = constraints
                        .as_ref()
                        .expect("schema 6 query constraints were validated")
                        .len();
                    if usize::try_from(*limit_argument_index).ok() != Some(constraint_count) {
                        return Err(invalid(
                            "importedOperations.operation.limitArgumentIndex",
                            "the dynamic query limit must follow every constraint argument",
                        ));
                    }
                }
            },
            Self::SchedulerRunAfter { function_reference }
            | Self::SchedulerRunAt { function_reference } => {
                validate_string(
                    "importedOperations.operation.functionReference",
                    function_reference,
                    MAX_STRING_BYTES,
                    false,
                )?;
            },
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueryConstraint {
    field_path: String,
    operator: QueryConstraintOperator,
}

impl QueryConstraint {
    pub(crate) fn field_path(&self) -> &str {
        &self.field_path
    }

    pub(crate) fn operator(&self) -> QueryConstraintOperator {
        self.operator
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum QueryConstraintOperator {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

fn validate_query_constraints(constraints: &[QueryConstraint]) -> Result<(), ManifestError> {
    if constraints.is_empty() {
        return Err(invalid(
            "importedOperations.operation.constraints",
            "at least one constraint is required",
        ));
    }
    let mut fields = BTreeSet::new();
    let mut saw_range = false;
    for (index, constraint) in constraints.iter().enumerate() {
        validate_string(
            "importedOperations.operation.constraints.fieldPath",
            &constraint.field_path,
            MAX_IDENTIFIER_BYTES,
            false,
        )?;
        let parsed = constraint.field_path.parse::<FieldPath>().map_err(|_| {
            invalid(
                "importedOperations.operation.constraints.fieldPath",
                "value must be a valid field path",
            )
        })?;
        if String::from(parsed) != constraint.field_path {
            return Err(invalid(
                "importedOperations.operation.constraints.fieldPath",
                "value must be a canonical field path",
            ));
        }
        if !fields.insert(constraint.field_path.as_str()) {
            return Err(invalid(
                "importedOperations.operation.constraints.fieldPath",
                "constraint fields must be unique",
            ));
        }
        match constraint.operator {
            QueryConstraintOperator::Eq if saw_range => {
                return Err(invalid(
                    "importedOperations.operation.constraints.operator",
                    "equality constraints cannot follow a range constraint",
                ));
            },
            QueryConstraintOperator::Eq => {},
            QueryConstraintOperator::Gt
            | QueryConstraintOperator::Gte
            | QueryConstraintOperator::Lt
            | QueryConstraintOperator::Lte => {
                if saw_range || index + 1 != constraints.len() {
                    return Err(invalid(
                        "importedOperations.operation.constraints.operator",
                        "at most one range constraint is allowed and it must be final",
                    ));
                }
                saw_range = true;
            },
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum QueryOrder {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum QueryTerminal {
    Collect,
    First,
    Stream,
    Unique,
}

fn validate_string(
    field: &'static str,
    value: &str,
    maximum_bytes: usize,
    allow_spaces: bool,
) -> Result<(), ManifestError> {
    if value.is_empty() {
        return Err(invalid(field, "value must not be empty"));
    }
    if value.len() > maximum_bytes {
        return Err(invalid(
            field,
            format!("value exceeds {maximum_bytes} bytes"),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(field, "control characters are not allowed"));
    }
    if !allow_spaces && value.chars().any(char::is_whitespace) {
        return Err(invalid(field, "whitespace is not allowed"));
    }
    Ok(())
}

fn validate_nonzero_max(
    field: &'static str,
    value: u64,
    maximum: u64,
) -> Result<(), ManifestError> {
    if !(1..=maximum).contains(&value) {
        return Err(invalid(
            field,
            format!("value must be between 1 and {maximum}"),
        ));
    }
    Ok(())
}

fn invalid(field: &'static str, reason: impl Into<String>) -> ManifestError {
    ManifestError::InvalidField {
        field,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{
        json,
        Value as JsonValue,
    };

    use super::*;

    const FINGERPRINT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OTHER_FINGERPRINT: &str =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn platform_limits() -> PlatformLimits {
        PlatformLimits {
            execution_time_ms: 1_000,
            argument_bytes: 16 * 1024 * 1024,
            result_bytes: 16 * 1024 * 1024,
            documents_read: 32_000,
            read_bytes: 16 * 1024 * 1024,
            documents_written: 16_000,
            write_bytes: 16 * 1024 * 1024,
            scheduled_functions: 1_000,
            scheduled_argument_bytes: 16 * 1024 * 1024,
        }
    }

    fn runtime() -> RuntimeCompatibility<'static> {
        RuntimeCompatibility {
            opaque_value_abi_version: OPAQUE_VALUE_ABI_VERSION,
            platform_limits: platform_limits(),
            wasmtime_revision: "wasmtime-main-revision",
            target_triple: "x86_64-unknown-linux-gnu",
            target_cpu: "x86-64-v3",
            engine_configuration_sha256: OTHER_FINGERPRINT,
            engine_compatibility_sha256: FINGERPRINT,
        }
    }

    fn eligible_manifest() -> JsonValue {
        json!({
            "manifestSchemaVersion": MANIFEST_SCHEMA_VERSION,
            "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
            "source": {
                "resolvedGraphSha256": FINGERPRINT,
                "exportSha256": OTHER_FINGERPRINT,
                "modulePath": "functions/example.ts",
                "runtimeModulePath": "functions/example.js",
                "exportName": "listItems",
                "udfKind": "query",
            },
            "compiler": {
                "artifactPipelineSha256": FINGERPRINT,
                "compilerRevision": "compiler-revision",
                "loweringPipelineSha256": OTHER_FINGERPRINT,
                "sourcePipelineSha256": FINGERPRINT,
                "staticHermesRevision": "static-hermes-revision",
                "admittedLanguageVersion": 1,
            },
            "artifact": {
                "coreWasmSha256": FINGERPRINT,
                "provenanceSha256": FINGERPRINT,
                "serializedModuleSha256": OTHER_FINGERPRINT,
                "wasmtimeRevision": "wasmtime-main-revision",
                "targetTriple": "x86_64-unknown-linux-gnu",
                "targetCpu": "x86-64-v3",
                "engineConfigurationSha256": OTHER_FINGERPRINT,
                "engineCompatibilitySha256": FINGERPRINT,
                "coreWasmBytes": 4_000_000,
                "provenanceBytes": 100_000,
                "serializedModuleBytes": 5_000_000,
            },
            "limits": {
                "maxGuestMemoryBytes": 33_554_432,
                "maxHostOwnedBytes": 16_777_216,
                "maxValueHandles": 1024,
                "maxOperationCount": 10_000,
                "maxResultBytes": 8_388_608,
                "executionFuel": 2_000_000_000_u64,
                "timeoutMilliseconds": 1000,
            },
            "platformLimits": platform_limits(),
            "importedOperations": [
                {
                    "id": 1,
                    "debugName": "itemsBySubject",
                    "operation": {
                        "kind": "databaseIndexQuery",
                        "tableName": "items",
                        "indexName": "by_subject",
                        "equalityField": "subject",
                        "order": "descending",
                        "terminal": "collect",
                        "limit": null,
                    },
                },
                {
                    "id": 2,
                    "debugName": "tokenByDigest",
                    "operation": {
                        "kind": "databaseIndexQuery",
                        "tableName": "accessTokens",
                        "indexName": "by_digest",
                        "equalityField": "digest",
                        "order": "ascending",
                        "terminal": "unique",
                        "limit": 2,
                    },
                },
            ],
            "routing": {
                "decision": "wasm",
            },
        })
    }

    fn cohort_manifest() -> JsonValue {
        let mut manifest = eligible_manifest();
        manifest["manifestSchemaVersion"] = json!(COHORT_MANIFEST_SCHEMA_VERSION);
        manifest["route"] = json!({
            "exportName": "listItems",
            "kind": "convex-wasm-runtime-route-v1",
            "runtimeModulePath": "functions/example.js",
            "udfKind": "query",
        });
        let artifact = manifest["artifact"]
            .as_object_mut()
            .expect("artifact fixture must be an object");
        artifact.remove("provenanceBytes");
        artifact.remove("provenanceSha256");
        artifact.insert(
            "cohort".to_owned(),
            json!({
                "bucket": 1,
                "manifestSha256": FINGERPRINT,
                "packageId": FINGERPRINT,
                "partitionPolicy": {
                    "bucketCount": 16,
                    "hash": "sha256-route-id-domain-first-u32-be-modulo",
                    "kind": "convex-wasm-cohort-partition-v1",
                },
            }),
        );
        artifact.insert("entryId".to_owned(), json!(FINGERPRINT));
        artifact.insert("entrySelectionIndex".to_owned(), json!(0));
        artifact.insert("entrySelectorId".to_owned(), json!("aaaaaaaaaaaaaaaa"));
        artifact.insert("entrySymbol".to_owned(), json!("sh_export_fixture"));
        manifest
    }

    fn compound_query_manifest() -> JsonValue {
        let mut manifest = cohort_manifest();
        manifest["manifestSchemaVersion"] = json!(COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION);
        let operations = manifest["importedOperations"]
            .as_array_mut()
            .expect("imported operation fixture must be an array");
        for operation in operations {
            let descriptor = operation["operation"]
                .as_object_mut()
                .expect("operation descriptor fixture must be an object");
            let equality_field = descriptor
                .remove("equalityField")
                .expect("query fixture must have an equality field");
            descriptor.insert(
                "constraints".to_owned(),
                json!([{
                    "fieldPath": equality_field,
                    "operator": "eq",
                }]),
            );
        }
        manifest
    }

    fn effect_execution_manifest(mode: &str) -> JsonValue {
        let mut manifest = compound_query_manifest();
        manifest["manifestSchemaVersion"] = json!(EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION);
        manifest["effectExecutionMode"] = json!(mode);
        manifest
    }

    fn parse(value: &JsonValue) -> Result<WasmUdfExecutionManifest, ManifestError> {
        WasmUdfExecutionManifest::parse_for_runtime(&serde_json::to_vec(value)?, &runtime())
    }

    #[test]
    fn capability_entries_require_guest_native_json_values() -> anyhow::Result<()> {
        let mut manifest = effect_execution_manifest("guest-promise-event-loop");
        manifest["valueMode"] = json!("guest-native-json");
        manifest["importedOperations"] = json!([]);
        let mut policy = WasmUdfExecutionPolicy::from_legacy(&parse(&manifest)?);
        assert!(matches!(
            policy.validate_capability_entry(OPAQUE_VALUE_ABI_VERSION, &runtime()),
            Err(ManifestError::InvalidField {
                field: "execution.valueCodec",
                ..
            })
        ));
        policy.value_codec = Some(json!({
            "kind": "test-capability-value-codec-identity",
        }));
        assert!(matches!(
            policy.validate_capability_entry(OPAQUE_VALUE_ABI_VERSION, &runtime()),
            Err(ManifestError::InvalidField {
                field: "execution.requestEnvelope",
                ..
            })
        ));
        policy.request_envelope = Some(json!({
            "kind": "test-capability-request-envelope-identity",
        }));
        policy.validate_capability_entry(OPAQUE_VALUE_ABI_VERSION, &runtime())?;

        let mut opaque_policy = policy;
        opaque_policy.value_mode = ValueMode::Opaque;
        opaque_policy.effect_execution_mode = EffectExecutionMode::BlockingFiber;
        assert!(matches!(
            opaque_policy.validate_capability_entry(OPAQUE_VALUE_ABI_VERSION, &runtime()),
            Err(ManifestError::InvalidField {
                field: "execution.valueMode",
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn accepts_engine_pinned_eligible_manifest() -> anyhow::Result<()> {
        let manifest = parse(&eligible_manifest())?;
        assert!(manifest.routing().is_wasm());
        assert_eq!(manifest.imported_operations().len(), 2);
        assert_eq!(manifest.platform_limits(), &platform_limits());
        assert_eq!(manifest.source().module_path(), "functions/example.ts");
        assert_eq!(
            manifest
                .artifact()
                .expect("eligible artifact missing")
                .serialized_module_sha256()
                .as_str(),
            OTHER_FINGERPRINT
        );
        Ok(())
    }

    #[test]
    fn enforces_core_wasm_size_limit_at_320_mib() -> anyhow::Result<()> {
        let mut at_limit = eligible_manifest();
        at_limit["artifact"]["coreWasmBytes"] = json!(MAX_CORE_WASM_BYTES);
        parse(&at_limit)?;

        let mut above_limit = at_limit;
        above_limit["artifact"]["coreWasmBytes"] = json!(MAX_CORE_WASM_BYTES + 1);
        assert!(matches!(
            parse(&above_limit),
            Err(ManifestError::InvalidField {
                field: "artifact.coreWasmBytes",
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn enforces_serialized_module_size_limit_at_one_gib() -> anyhow::Result<()> {
        assert_eq!(MAX_SERIALIZED_MODULE_BYTES, 1024 * 1024 * 1024);
        let mut at_limit = eligible_manifest();
        at_limit["artifact"]["serializedModuleBytes"] = json!(MAX_SERIALIZED_MODULE_BYTES);
        parse(&at_limit)?;

        let mut above_limit = at_limit;
        above_limit["artifact"]["serializedModuleBytes"] = json!(MAX_SERIALIZED_MODULE_BYTES + 1);
        assert!(matches!(
            parse(&above_limit),
            Err(ManifestError::InvalidField {
                field: "artifact.serializedModuleBytes",
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn schema_five_validates_ordered_query_constraints_and_static_limits() -> anyhow::Result<()> {
        let legacy_cohort = parse(&cohort_manifest())?;
        let permitted = permitted_conditional_convex_imports(
            legacy_cohort.value_mode(),
            legacy_cohort.effect_execution_mode(),
            std::slice::from_ref(&legacy_cohort.imported_operations()[0]),
        );
        assert!(permitted.contains("convex_query_start_value"));
        assert!(permitted.contains("convex_query_start_utf8"));

        let single_constraint = parse(&compound_query_manifest())?;
        let permitted = permitted_conditional_convex_imports(
            single_constraint.value_mode(),
            single_constraint.effect_execution_mode(),
            std::slice::from_ref(&single_constraint.imported_operations()[0]),
        );
        assert!(permitted.contains("convex_query_start_value"));
        assert!(!permitted.contains("convex_query_start_utf8"));

        let mut valid = compound_query_manifest();
        valid["importedOperations"][0]["operation"]["constraints"] = json!([
            { "fieldPath": "tenant", "operator": "eq" },
            { "fieldPath": "status", "operator": "eq" },
            { "fieldPath": "sequence", "operator": "gt" },
        ]);
        valid["importedOperations"][0]["operation"]["limit"] = json!(2);
        let manifest = parse(&valid)?;
        let ImportedOperationDescriptor::DatabaseIndexQuery {
            equality_field,
            constraints: Some(constraints),
            limit,
            ..
        } = manifest.imported_operations()[0].operation()
        else {
            anyhow::bail!("schema-5 query descriptor did not retain constraints");
        };
        assert!(equality_field.is_none());
        assert_eq!(*limit, Some(2));
        assert_eq!(
            constraints
                .iter()
                .map(|constraint| (constraint.field_path(), constraint.operator()))
                .collect::<Vec<_>>(),
            vec![
                ("tenant", QueryConstraintOperator::Eq),
                ("status", QueryConstraintOperator::Eq),
                ("sequence", QueryConstraintOperator::Gt),
            ]
        );
        let permitted = permitted_conditional_convex_imports(
            manifest.value_mode(),
            manifest.effect_execution_mode(),
            std::slice::from_ref(&manifest.imported_operations()[0]),
        );
        assert!(permitted.contains("convex_query_start_value"));
        assert!(!permitted.contains("convex_query_start_utf8"));

        for limit in [1, 100_000] {
            let mut boundary = valid.clone();
            boundary["importedOperations"][0]["operation"]["limit"] = json!(limit);
            parse(&boundary)?;
        }
        for limit in [0, 100_001] {
            let mut boundary = valid.clone();
            boundary["importedOperations"][0]["operation"]["limit"] = json!(limit);
            assert!(matches!(
                parse(&boundary),
                Err(ManifestError::InvalidField {
                    field: "importedOperations.operation.limit",
                    ..
                })
            ));
        }
        for limit in [json!(-1), json!(1.5)] {
            let mut non_integer = valid.clone();
            non_integer["importedOperations"][0]["operation"]["limit"] = limit;
            assert!(matches!(
                parse(&non_integer),
                Err(ManifestError::Malformed(_))
            ));
        }

        let invalid_constraints = [
            json!([]),
            json!([{ "fieldPath": ".tenant", "operator": "eq" }]),
            json!([
                { "fieldPath": "tenant", "operator": "eq" },
                { "fieldPath": "tenant", "operator": "eq" },
            ]),
            json!([
                { "fieldPath": "tenant", "operator": "gt" },
                { "fieldPath": "status", "operator": "eq" },
            ]),
            json!([
                { "fieldPath": "tenant", "operator": "gt" },
                { "fieldPath": "status", "operator": "lt" },
            ]),
        ];
        for constraints in invalid_constraints {
            let mut invalid = valid.clone();
            invalid["importedOperations"][0]["operation"]["constraints"] = constraints;
            assert!(matches!(
                parse(&invalid),
                Err(ManifestError::InvalidField { .. })
            ));
        }

        let mut unknown_operator = valid.clone();
        unknown_operator["importedOperations"][0]["operation"]["constraints"][0]["operator"] =
            json!("ne");
        assert!(matches!(
            parse(&unknown_operator),
            Err(ManifestError::Malformed(_))
        ));

        let mut old_with_constraints = cohort_manifest();
        old_with_constraints["importedOperations"][0]["operation"] =
            valid["importedOperations"][0]["operation"].clone();
        assert!(matches!(
            parse(&old_with_constraints),
            Err(ManifestError::InvalidField { .. })
        ));
        let mut new_with_equality = cohort_manifest();
        new_with_equality["manifestSchemaVersion"] = json!(COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION);
        assert!(matches!(
            parse(&new_with_equality),
            Err(ManifestError::InvalidField {
                field: "importedOperations.operation.equalityField",
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn schema_six_authenticates_dynamic_query_limit_argument() -> anyhow::Result<()> {
        let mut valid = effect_execution_manifest("blocking-fiber");
        valid["importedOperations"][0]["operation"]["limit"] = JsonValue::Null;
        valid["importedOperations"][0]["operation"]["limitArgumentIndex"] = json!(1);
        let manifest = parse(&valid)?;
        let ImportedOperationDescriptor::DatabaseIndexQuery {
            constraints: Some(constraints),
            limit,
            limit_argument_index,
            terminal,
            ..
        } = manifest.imported_operations()[0].operation()
        else {
            anyhow::bail!("schema-6 query descriptor did not retain its dynamic limit");
        };
        assert_eq!(constraints.len(), 1);
        assert_eq!(*limit, None);
        assert_eq!(*limit_argument_index, Some(1));
        assert_eq!(*terminal, QueryTerminal::Collect);

        let mut schema_five = compound_query_manifest();
        schema_five["importedOperations"][0]["operation"]["limitArgumentIndex"] = json!(1);
        let mut static_and_dynamic = valid.clone();
        static_and_dynamic["importedOperations"][0]["operation"]["limit"] = json!(2);
        let mut non_collect = valid.clone();
        non_collect["importedOperations"][0]["operation"]["terminal"] = json!("first");
        let mut wrong_argument_index = valid.clone();
        wrong_argument_index["importedOperations"][0]["operation"]["limitArgumentIndex"] = json!(0);
        for invalid_manifest in [
            schema_five,
            static_and_dynamic,
            non_collect,
            wrong_argument_index,
        ] {
            assert!(matches!(
                parse(&invalid_manifest),
                Err(ManifestError::InvalidField {
                    field: "importedOperations.operation.limitArgumentIndex",
                    ..
                })
            ));
        }
        Ok(())
    }

    #[test]
    fn schema_six_authenticates_guest_query_stream_imports() -> anyhow::Result<()> {
        let mut stream = effect_execution_manifest("guest-promise-event-loop");
        stream["importedOperations"][0]["operation"]["terminal"] = json!("stream");
        stream["importedOperations"][0]["operation"]["limit"] = JsonValue::Null;
        let manifest = parse(&stream)?;
        let permitted = permitted_conditional_convex_imports(
            manifest.value_mode(),
            manifest.effect_execution_mode(),
            std::slice::from_ref(&manifest.imported_operations()[0]),
        );
        for name in [
            "convex_async_operation_cancel_all",
            "convex_async_query_stream_open_take",
            "convex_async_query_stream_next",
            "convex_async_query_stream_close",
            "convex_async_operation_wait_any",
            "convex_async_operation_poll_ready",
            "convex_async_operation_completion_status",
            "convex_async_operation_completion_take",
        ] {
            assert!(
                permitted.contains(name),
                "query stream did not permit {name}"
            );
        }
        assert!(!permitted.contains("convex_async_operation_start_take"));
        assert!(matches!(
            manifest.imported_operations()[0].operation(),
            ImportedOperationDescriptor::DatabaseIndexQuery {
                terminal: QueryTerminal::Stream,
                ..
            }
        ));

        let mut blocking = stream.clone();
        blocking["effectExecutionMode"] = json!("blocking-fiber");
        assert!(matches!(
            parse(&blocking),
            Err(ManifestError::InvalidField {
                field: "importedOperations.operation.terminal",
                ..
            })
        ));

        let mut limited = stream.clone();
        limited["importedOperations"][0]["operation"]["limit"] = json!(1);
        assert!(matches!(
            parse(&limited),
            Err(ManifestError::InvalidField {
                field: "importedOperations.operation.terminal",
                ..
            })
        ));

        let mut schema_five = stream;
        schema_five["manifestSchemaVersion"] = json!(COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION);
        schema_five
            .as_object_mut()
            .unwrap()
            .remove("effectExecutionMode");
        assert!(matches!(
            parse(&schema_five),
            Err(ManifestError::InvalidField {
                field: "importedOperations.operation.terminal",
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn schema_six_requires_and_binds_effect_execution_mode() -> anyhow::Result<()> {
        for (wire, expected) in [
            ("blocking-fiber", EffectExecutionMode::BlockingFiber),
            (
                "guest-promise-event-loop",
                EffectExecutionMode::GuestPromiseEventLoop,
            ),
        ] {
            let manifest = parse(&effect_execution_manifest(wire))?;
            assert_eq!(manifest.effect_execution_mode(), expected);
        }

        let mut missing = compound_query_manifest();
        missing["manifestSchemaVersion"] = json!(EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION);
        assert!(matches!(
            parse(&missing),
            Err(ManifestError::InvalidField {
                field: "effectExecutionMode",
                ..
            })
        ));

        for mut legacy in [
            eligible_manifest(),
            cohort_manifest(),
            compound_query_manifest(),
        ] {
            legacy["effectExecutionMode"] = json!("blocking-fiber");
            assert!(matches!(
                parse(&legacy),
                Err(ManifestError::InvalidField {
                    field: "effectExecutionMode",
                    ..
                })
            ));
        }

        let mut unknown = effect_execution_manifest("unknown");
        let result = parse(&unknown);
        assert!(
            matches!(result, Err(ManifestError::Malformed(_))),
            "unexpected unknown execution-mode result: {result:?}"
        );
        unknown["effectExecutionMode"] = JsonValue::Null;
        let result = parse(&unknown);
        assert!(
            matches!(
                result,
                Err(ManifestError::InvalidField {
                    field: "effectExecutionMode",
                    ..
                })
            ),
            "unexpected null execution-mode result: {result:?}"
        );
        Ok(())
    }

    #[test]
    fn guest_promise_mode_permits_only_the_generic_async_imports() -> anyhow::Result<()> {
        let manifest = parse(&effect_execution_manifest("guest-promise-event-loop"))?;
        let permitted = permitted_conditional_convex_imports(
            manifest.value_mode(),
            manifest.effect_execution_mode(),
            manifest.imported_operations(),
        );
        for name in [
            "convex_async_operation_cancel_all",
            "convex_async_operation_start_take",
            "convex_async_operation_wait_any",
            "convex_async_operation_poll_ready",
            "convex_async_operation_completion_status",
            "convex_async_operation_completion_take",
            "convex_capability_current",
            "convex_console_message",
            "convex_capability_request_decode",
            "convex_capability_request_release",
            "convex_capability_query_stream_open_take",
            "convex_capability_start_take",
            "convex_capability_sync_take",
            "convex_async_query_stream_next",
            "convex_async_query_stream_close",
            "convex_crypto_subtle_digest_sha256",
        ] {
            assert!(permitted.contains(name));
        }
        for name in [
            "convex_async_batch_take",
            "convex_db_get",
            "convex_db_write",
            "convex_function_handle_create",
            "convex_query_next",
            "convex_query_start_utf8",
            "convex_query_start_value",
            "convex_scheduler_schedule",
        ] {
            assert!(!permitted.contains(name));
        }
        Ok(())
    }

    #[test]
    fn derives_conditional_imports_from_value_mode_and_operations() -> anyhow::Result<()> {
        let manifest = parse(&eligible_manifest())?;
        assert_eq!(
            permitted_conditional_convex_imports(
                manifest.value_mode(),
                manifest.effect_execution_mode(),
                manifest.imported_operations()
            ),
            BTreeSet::from([
                "convex_async_batch_take",
                "convex_capability_current",
                "convex_function_result",
                "convex_query_next",
                "convex_query_start_utf8",
                "convex_query_start_value",
                "convex_request_field",
            ])
        );

        let mut guest = eligible_manifest();
        guest["valueMode"] = json!("guest-native-json");
        let manifest = parse(&guest)?;
        let permitted = permitted_conditional_convex_imports(
            manifest.value_mode(),
            manifest.effect_execution_mode(),
            manifest.imported_operations(),
        );
        assert!(permitted.contains("convex_guest_value_request_len"));
        assert!(permitted.contains("convex_guest_value_payload_release"));
        assert!(!permitted.contains("convex_function_result"));
        assert!(!permitted.contains("convex_request_field"));
        Ok(())
    }

    #[test]
    fn rejects_unknown_fields_and_engine_mismatch() {
        let mut unknown = eligible_manifest();
        unknown
            .as_object_mut()
            .expect("manifest object")
            .insert("ignored".to_owned(), JsonValue::Bool(true));
        assert!(matches!(parse(&unknown), Err(ManifestError::Malformed(_))));

        let mut mismatch = eligible_manifest();
        mismatch["artifact"]["targetCpu"] = json!("native");
        assert!(matches!(
            parse(&mismatch),
            Err(ManifestError::IncompatibleEngine {
                field: "artifact.targetCpu"
            })
        ));

        let mut incompatible_aot = eligible_manifest();
        incompatible_aot["artifact"]["engineCompatibilitySha256"] = json!(OTHER_FINGERPRINT);
        assert!(matches!(
            parse(&incompatible_aot),
            Err(ManifestError::IncompatibleEngine {
                field: "artifact.engineCompatibilitySha256"
            })
        ));
    }

    #[test]
    fn rejects_stale_and_mistyped_fingerprints() {
        let mut stale_schema = eligible_manifest();
        stale_schema["manifestSchemaVersion"] = json!(2);
        stale_schema
            .as_object_mut()
            .expect("manifest object")
            .remove("platformLimits");
        assert!(matches!(
            parse(&stale_schema),
            Err(ManifestError::UnsupportedSchemaVersion { actual: 2 })
        ));

        let mut stale = eligible_manifest();
        stale["opaqueValueAbiVersion"] = json!(OPAQUE_VALUE_ABI_VERSION + 1);
        assert!(matches!(
            parse(&stale),
            Err(ManifestError::UnsupportedAbiVersion { .. })
        ));

        let mut uppercase = eligible_manifest();
        uppercase["source"]["exportSha256"] =
            json!("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
        assert!(matches!(
            parse(&uppercase),
            Err(ManifestError::InvalidField {
                field: "source.exportSha256",
                ..
            })
        ));
    }

    #[test]
    fn invocation_timestamp_import_rejects_the_previous_abi_version() {
        let mut previous_abi = eligible_manifest();
        previous_abi["opaqueValueAbiVersion"] = json!(OPAQUE_VALUE_ABI_VERSION - 1);
        assert!(matches!(
            parse(&previous_abi),
            Err(ManifestError::UnsupportedAbiVersion { actual })
                if actual == OPAQUE_VALUE_ABI_VERSION - 1
        ));
    }

    #[test]
    fn schema_versions_reject_singleton_and_cohort_cross_contamination() -> anyhow::Result<()> {
        parse(&cohort_manifest())?;

        let mut singleton_with_route = eligible_manifest();
        singleton_with_route["route"] = cohort_manifest()["route"].clone();
        assert!(matches!(
            parse(&singleton_with_route),
            Err(ManifestError::InvalidField { field: "route", .. })
        ));

        let mut cohort_with_singleton_provenance = cohort_manifest();
        cohort_with_singleton_provenance["artifact"]["provenanceSha256"] = json!(FINGERPRINT);
        cohort_with_singleton_provenance["artifact"]["provenanceBytes"] = json!(100_000);
        assert!(matches!(
            parse(&cohort_with_singleton_provenance),
            Err(ManifestError::InvalidField {
                field: "artifact",
                ..
            })
        ));

        let mut cohort_without_route = cohort_manifest();
        cohort_without_route
            .as_object_mut()
            .expect("cohort fixture must be an object")
            .remove("route");
        assert!(matches!(
            parse(&cohort_without_route),
            Err(ManifestError::InvalidField { field: "route", .. })
        ));

        let mut cohort_without_identity = cohort_manifest();
        cohort_without_identity["artifact"]
            .as_object_mut()
            .expect("artifact fixture must be an object")
            .remove("cohort");
        assert!(matches!(
            parse(&cohort_without_identity),
            Err(ManifestError::InvalidField {
                field: "artifact.cohort",
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn rejects_missing_unknown_zero_and_oversized_platform_limits() {
        let mut missing = eligible_manifest();
        missing
            .as_object_mut()
            .expect("manifest object")
            .remove("platformLimits");
        assert!(matches!(parse(&missing), Err(ManifestError::Malformed(_))));

        let mut unknown = eligible_manifest();
        unknown["platformLimits"]["unknown"] = json!(1);
        assert!(matches!(parse(&unknown), Err(ManifestError::Malformed(_))));

        let mut zero = eligible_manifest();
        zero["platformLimits"]["documentsRead"] = json!(0);
        assert!(matches!(
            parse(&zero),
            Err(ManifestError::InvalidField {
                field: "platformLimits.documentsRead",
                ..
            })
        ));

        let mut oversized = eligible_manifest();
        oversized["platformLimits"]["writeBytes"] = json!(MAX_PLATFORM_LIMIT_VALUE + 1);
        assert!(matches!(
            parse(&oversized),
            Err(ManifestError::InvalidField {
                field: "platformLimits.writeBytes",
                ..
            })
        ));
    }

    #[test]
    fn rejects_every_directional_platform_limit_mismatch() {
        for (field, compatible) in [
            ("executionTimeMs", 1_000_u64),
            ("argumentBytes", 16 * 1024 * 1024),
            ("resultBytes", 16 * 1024 * 1024),
            ("documentsRead", 32_000),
            ("readBytes", 16 * 1024 * 1024),
            ("documentsWritten", 16_000),
            ("writeBytes", 16 * 1024 * 1024),
            ("scheduledFunctions", 1_000),
            ("scheduledArgumentBytes", 16 * 1024 * 1024),
        ] {
            let compatibility_field = match field {
                "executionTimeMs" => "platformLimits.executionTimeMs",
                "argumentBytes" => "platformLimits.argumentBytes",
                "resultBytes" => "platformLimits.resultBytes",
                "documentsRead" => "platformLimits.documentsRead",
                "readBytes" => "platformLimits.readBytes",
                "documentsWritten" => "platformLimits.documentsWritten",
                "writeBytes" => "platformLimits.writeBytes",
                "scheduledFunctions" => "platformLimits.scheduledFunctions",
                "scheduledArgumentBytes" => "platformLimits.scheduledArgumentBytes",
                _ => unreachable!("complete platform-limit fixture"),
            };
            for incompatible in [compatible - 1, compatible + 1] {
                let mut manifest = eligible_manifest();
                manifest["platformLimits"][field] = json!(incompatible);
                assert!(matches!(
                    parse(&manifest),
                    Err(ManifestError::IncompatiblePlatformLimit {
                        field
                    }) if field == compatibility_field
                ));
            }
        }
    }

    #[test]
    fn artifact_budgets_cannot_exceed_platform_time_or_result_limits() {
        let mut timeout = eligible_manifest();
        timeout["limits"]["timeoutMilliseconds"] = json!(1_001);
        assert!(matches!(
            parse(&timeout),
            Err(ManifestError::InvalidField {
                field: "limits.timeoutMilliseconds",
                ..
            })
        ));

        let mut runtime = runtime();
        runtime.platform_limits.result_bytes = 8 * 1024 * 1024;
        let mut result = eligible_manifest();
        result["platformLimits"]["resultBytes"] = json!(8 * 1024 * 1024);
        result["limits"]["maxResultBytes"] = json!(8 * 1024 * 1024 + 1);
        assert!(matches!(
            WasmUdfExecutionManifest::parse_for_runtime(
                &serde_json::to_vec(&result).expect("manifest fixture serializes"),
                &runtime,
            ),
            Err(ManifestError::InvalidField {
                field: "limits.maxResultBytes",
                ..
            })
        ));
    }

    #[test]
    fn fallback_requires_exact_static_diagnostics_and_no_artifact() -> anyhow::Result<()> {
        let mut fallback = eligible_manifest();
        fallback["artifact"] = JsonValue::Null;
        fallback["importedOperations"] = json!([]);
        fallback["routing"] = json!({
            "decision": "v8Fallback",
            "diagnostics": [{
                "code": "unsupportedImport",
                "subject": "node:crypto",
                "message": "Import node:crypto is not available in the Wasm UDF runtime",
                "modulePath": "functions/example.ts",
                "sourceSpan": {
                    "startLine": 3,
                    "startColumn": 1,
                    "endLine": 3,
                    "endColumn": 34,
                },
                "dependencyChain": ["functions/example.ts"],
            }],
        });
        let manifest = parse(&fallback)?;
        assert!(!manifest.routing().is_wasm());

        fallback["routing"]["diagnostics"] = json!([]);
        assert!(matches!(
            parse(&fallback),
            Err(ManifestError::InvalidField {
                field: "routing.diagnostics",
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn fallback_cannot_encode_transient_pressure_or_executable_data() {
        let mut fallback = eligible_manifest();
        fallback["artifact"] = JsonValue::Null;
        fallback["routing"] = json!({
            "decision": "v8Fallback",
            "diagnostics": [{
                "code": "unsupportedConstruct",
                "subject": "runtime memory pressure",
                "message": "capacity is not a static eligibility reason",
                "modulePath": "functions/example.ts",
                "sourceSpan": null,
                "dependencyChain": [],
            }],
        });
        assert!(matches!(
            parse(&fallback),
            Err(ManifestError::InconsistentRouting(
                "an ineligible export must not declare executable imports"
            ))
        ));
    }

    #[test]
    fn rejects_duplicate_operation_ids_and_oversized_input() {
        let mut duplicate = eligible_manifest();
        duplicate["importedOperations"][1]["id"] = json!(1);
        assert!(matches!(
            parse(&duplicate),
            Err(ManifestError::InvalidField {
                field: "importedOperations.id",
                ..
            })
        ));

        let mut out_of_range = eligible_manifest();
        out_of_range["importedOperations"][0]["id"] =
            json!(u64::from(MAX_IMPORTED_OPERATION_ID) + 1);
        assert!(matches!(
            parse(&out_of_range),
            Err(ManifestError::InvalidField {
                field: "importedOperations.id",
                ..
            })
        ));

        let oversized = vec![b' '; MAX_MANIFEST_BYTES + 1];
        assert!(matches!(
            WasmUdfExecutionManifest::parse_for_runtime(&oversized, &runtime()),
            Err(ManifestError::TooLarge { .. })
        ));
    }

    #[test]
    fn rejects_effect_operations_in_query_manifests() {
        for operation in [
            json!({
                "kind": "databaseInsert",
                "tableName": "items",
            }),
            json!({
                "kind": "databasePatch",
                "tableName": "items",
            }),
            json!({
                "kind": "databaseReplace",
                "tableName": "items",
            }),
            json!({
                "kind": "databaseDelete",
                "tableName": "items",
            }),
            json!({
                "kind": "schedulerRunAfter",
                "functionReference": "_reference/function/tasks:run",
            }),
        ] {
            let mut manifest = eligible_manifest();
            manifest["importedOperations"] = json!([{
                "id": 1,
                "debugName": "writeEffect",
                "operation": operation,
            }]);
            assert!(matches!(
                parse(&manifest),
                Err(ManifestError::InvalidField {
                    field: "importedOperations.operation.kind",
                    ..
                })
            ));
        }
    }

    #[test]
    fn authentication_get_user_identity_is_canonical_and_read_only() -> anyhow::Result<()> {
        let operation = json!({
            "kind": "authenticationGetUserIdentity",
        });
        for udf_kind in ["query", "mutation"] {
            let mut manifest = eligible_manifest();
            manifest["source"]["udfKind"] = json!(udf_kind);
            manifest["importedOperations"] = json!([{
                "id": 1,
                "debugName": "currentUserIdentity",
                "operation": operation,
            }]);

            let parsed = parse(&manifest)?;
            assert!(matches!(
                parsed.imported_operations()[0].operation(),
                ImportedOperationDescriptor::AuthenticationGetUserIdentity {}
            ));
            let canonical = serde_json::to_value(&parsed)?;
            assert_eq!(canonical["importedOperations"][0]["operation"], operation);
            assert_eq!(parse(&canonical)?, parsed);
        }

        let mut unknown = eligible_manifest();
        unknown["importedOperations"] = json!([{
            "id": 1,
            "debugName": "currentUserIdentity",
            "operation": {
                "kind": "authenticationGetUserIdentity",
                "argument": null,
            },
        }]);
        assert!(matches!(parse(&unknown), Err(ManifestError::Malformed(_))));
        Ok(())
    }

    #[test]
    fn host_secret_verify_is_canonical_and_strict() -> anyhow::Result<()> {
        let operation = json!({
            "kind": "hostSecretVerify",
            "contractVersion": 1,
            "selector": "HOST_SECRET",
        });
        for udf_kind in ["query", "mutation"] {
            let mut manifest = eligible_manifest();
            manifest["source"]["udfKind"] = json!(udf_kind);
            manifest["importedOperations"] = json!([{
                "id": 1,
                "debugName": "verifyHostSecret",
                "operation": operation,
            }]);

            let parsed = parse(&manifest)?;
            assert!(matches!(
                parsed.imported_operations()[0].operation(),
                ImportedOperationDescriptor::HostSecretVerify {
                    contract_version: 1,
                    selector,
                } if selector == "HOST_SECRET"
            ));
            assert!(permitted_conditional_convex_imports(
                parsed.value_mode(),
                parsed.effect_execution_mode(),
                parsed.imported_operations(),
            )
            .contains("convex_host_secret_verify"));
            let canonical = serde_json::to_value(&parsed)?;
            assert_eq!(canonical["importedOperations"][0]["operation"], operation);
            assert_eq!(parse(&canonical)?, parsed);
        }

        for invalid_operation in [
            json!({
                "kind": "hostSecretVerify",
                "contractVersion": 2,
                "selector": "HOST_SECRET",
            }),
            json!({
                "kind": "hostSecretVerify",
                "contractVersion": 1,
                "selector": "",
            }),
            json!({
                "kind": "hostSecretVerify",
                "contractVersion": 1,
                "selector": "1HOST_SECRET",
            }),
            json!({
                "kind": "hostSecretVerify",
                "contractVersion": 1,
                "selector": "HOST-SECRET",
            }),
        ] {
            let mut manifest = eligible_manifest();
            manifest["importedOperations"] = json!([{
                "id": 1,
                "debugName": "verifyHostSecret",
                "operation": invalid_operation,
            }]);
            assert!(matches!(
                parse(&manifest),
                Err(ManifestError::InvalidField {
                    field: "importedOperations.operation.hostSecretVerify",
                    ..
                })
            ));
        }

        let mut unknown = eligible_manifest();
        unknown["importedOperations"] = json!([{
            "id": 1,
            "debugName": "verifyHostSecret",
            "operation": {
                "kind": "hostSecretVerify",
                "contractVersion": 1,
                "selector": "HOST_SECRET",
                "value": "must-not-be-declared",
            },
        }]);
        assert!(matches!(parse(&unknown), Err(ManifestError::Malformed(_))));
        Ok(())
    }

    #[test]
    fn database_normalize_id_is_canonical_for_queries_and_mutations() -> anyhow::Result<()> {
        let operation = json!({
            "kind": "databaseNormalizeId",
            "tableName": "items",
        });
        for udf_kind in ["query", "mutation"] {
            let mut manifest = eligible_manifest();
            manifest["source"]["udfKind"] = json!(udf_kind);
            manifest["importedOperations"] = json!([{
                "id": 1,
                "debugName": "normalizeItemId",
                "operation": operation,
            }]);

            let parsed = parse(&manifest)?;
            assert!(matches!(
                parsed.imported_operations()[0].operation(),
                ImportedOperationDescriptor::DatabaseNormalizeId { table_name }
                    if table_name == "items"
            ));
            let canonical = serde_json::to_value(&parsed)?;
            assert_eq!(canonical["importedOperations"][0]["operation"], operation);
            assert_eq!(parse(&canonical)?, parsed);
        }

        let mut unknown = eligible_manifest();
        unknown["importedOperations"] = json!([{
            "id": 1,
            "debugName": "normalizeItemId",
            "operation": {
                "kind": "databaseNormalizeId",
                "tableName": "items",
                "argument": null,
            },
        }]);
        assert!(matches!(parse(&unknown), Err(ManifestError::Malformed(_))));
        Ok(())
    }

    #[test]
    fn function_handle_create_is_canonical_for_queries_and_mutations() -> anyhow::Result<()> {
        let operation = json!({
            "kind": "functionHandleCreate",
        });
        for udf_kind in ["query", "mutation"] {
            let mut manifest = eligible_manifest();
            manifest["source"]["udfKind"] = json!(udf_kind);
            manifest["importedOperations"] = json!([{
                "id": 1,
                "debugName": "createHandle",
                "operation": operation,
            }]);

            let parsed = parse(&manifest)?;
            assert!(matches!(
                parsed.imported_operations()[0].operation(),
                ImportedOperationDescriptor::FunctionHandleCreate {}
            ));
            let canonical = serde_json::to_value(&parsed)?;
            assert_eq!(canonical["importedOperations"][0]["operation"], operation);
            assert_eq!(parse(&canonical)?, parsed);
        }

        let mut unknown = eligible_manifest();
        unknown["importedOperations"] = json!([{
            "id": 1,
            "debugName": "createHandle",
            "operation": {
                "kind": "functionHandleCreate",
                "functionReference": "_reference/function/example:run",
            },
        }]);
        assert!(matches!(parse(&unknown), Err(ManifestError::Malformed(_))));
        Ok(())
    }
}
