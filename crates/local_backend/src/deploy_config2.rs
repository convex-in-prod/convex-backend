use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    time::Duration,
};

use application::deploy_config::{
    ComponentSchemaPrediction,
    EvaluatePushResponse,
    EvaluateSchemaPredictionResponse,
    FinishPushDiff,
    IndexChangePrediction,
    IndexPrediction,
    NodeDependencyJson,
    SchemaStatusJson,
    SerializedExternalDepsPackageSelection,
    SourceKeyedRuntimeActivation,
    SourceKeyedRuntimeExpectedPrior,
    StartPushRequest,
    StartPushResponse,
    TablePrediction,
};
use axum::{
    body::{
        Body,
        Bytes,
    },
    debug_handler,
    extract::State,
    http::header,
    response::IntoResponse,
};
use common::{
    auth::{
        AuthInfo,
        SerializedAuthInfo,
    },
    bootstrap_model::components::definition::SerializedComponentDefinitionMetadata,
    execution_context::RequestMetadata,
    http::{
        extract::{
            Json,
            MtState,
        },
        ExtractRequestMetadata,
        HttpResponseError,
    },
    schemas::TableValidationOutcome,
    types::NodeDependency,
};
use errors::{
    ErrorMetadata,
    ErrorMetadataAnyhowExt,
};
use fastrace::{
    collector::EventRecord,
    prelude::{
        SpanId,
        SpanRecord,
        TraceId,
    },
};
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use model::{
    auth::types::AuthDiff,
    components::{
        config::{
            SerializedComponentDefinitionDiff,
            SerializedComponentDiff,
            SerializedSchemaChange,
        },
        type_checking::SerializedCheckedComponent,
        types::SerializedEvaluatedComponentDefinition,
    },
    deployment_audit_log::{
        developer_index_config::{
            SerializedDeveloperIndexConfig,
            SerializedNamedDeveloperIndexConfig,
        },
        types::PushMessage,
    },
    external_packages::types::{
        ExternalDepsPackageId,
        ExternalDepsPackageSelection,
    },
    modules::module_versions::SerializedAnalyzedModule,
    source_packages::types::{
        SourcePackage,
        SourcePackageRuntimeGeneration,
    },
};
use roles::RequireDeploymentOp;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value as JsonValue;
use storage::StorageGetStream;
use value::{
    base64,
    sha256::Sha256Digest,
    ConvexObject,
    DeveloperDocumentId,
};

use crate::{
    admin::must_be_admin_from_key,
    LocalAppState,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PrepareExternalDepsRequest {
    admin_key: String,
    node_dependencies: Vec<NodeDependencyJson>,
    // A frozen cross-target deployment imports its exact archive instead of
    // asking the destination to resolve and rebuild the dependencies again.
    #[serde(
        default,
        deserialize_with = "ImportedExternalDepsArchive::deserialize_present"
    )]
    archive: Option<ImportedExternalDepsArchive>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportedExternalDepsArchive {
    sha256: String,
    bytes: String,
}

impl ImportedExternalDepsArchive {
    fn deserialize_present<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Self>, D::Error> {
        Self::deserialize(deserializer).map(Some)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedExternalDepsPackage {
    kind: &'static str,
    id: String,
    sha256: String,
    storage_key: String,
    size: usize,
    dependencies: Vec<PreparedNodeDependency>,
}

#[derive(Serialize)]
pub struct PreparedNodeDependency {
    package: String,
    version: String,
}

pub async fn prepare_external_deps(
    State(st): State<LocalAppState>,
    Json(req): Json<PrepareExternalDepsRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let identity = must_be_admin_from_key(
        st.application.app_auth(),
        st.instance_name.clone(),
        req.admin_key,
    )
    .await?;
    identity.require_operation(keybroker::DeploymentOp::Deploy)?;
    let dependencies: Vec<NodeDependency> =
        req.node_dependencies.into_iter().map(Into::into).collect();
    if dependencies.is_empty()
        || dependencies
            .iter()
            .map(|dependency| &dependency.package)
            .collect::<BTreeSet<_>>()
            .len()
            != dependencies.len()
    {
        return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
            "InvalidExternalModules",
            "Dependency preparation requires nonempty dependencies with unique package names",
        ))
        .into());
    }
    let (id, package) = match req.archive {
        None => {
            st.application
                .build_external_node_deps(dependencies.clone())
                .await?
        },
        Some(archive) => {
            let sha256 = const_hex::decode(&archive.sha256)
                .ok()
                .and_then(|bytes| Sha256Digest::try_from(bytes).ok())
                .ok_or_else(|| {
                    anyhow::anyhow!(ErrorMetadata::bad_request(
                        "InvalidExternalDepsPackage",
                        "Imported dependency archive SHA-256 is invalid"
                    ))
                })?;
            let bytes = base64::decode_urlsafe(&archive.bytes).map_err(|_| {
                anyhow::anyhow!(ErrorMetadata::bad_request(
                    "InvalidExternalDepsPackage",
                    "Imported dependency archive encoding is invalid"
                ))
            })?;
            tokio::time::timeout(
                Duration::from_secs(300),
                st.application.import_external_node_deps(
                    dependencies.clone(),
                    bytes.into(),
                    sha256,
                ),
            )
            .await
            .map_err(|_| anyhow::anyhow!("External dependency import timed out"))??
        },
    };
    package.validate_dependencies(&dependencies)?;
    package.package_size.verify_size()?;
    let mut dependencies: Vec<_> = package
        .deps
        .into_iter()
        .map(|dependency| PreparedNodeDependency {
            package: dependency.package,
            version: dependency.version,
        })
        .collect();
    dependencies.sort_by(|left, right| {
        (&left.package, &left.version).cmp(&(&right.package, &right.version))
    });
    Ok(Json(PreparedExternalDepsPackage {
        kind: "convex-external-deps-package-v1",
        id: id.into(),
        sha256: package.sha256.as_hex(),
        storage_key: package.storage_key.to_string(),
        size: package.package_size.zipped_size_bytes,
        dependencies,
    }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DownloadExternalDepsRequest {
    admin_key: String,
    id: String,
    sha256: String,
}

pub async fn download_external_deps(
    State(st): State<LocalAppState>,
    Json(req): Json<DownloadExternalDepsRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let identity = must_be_admin_from_key(
        st.application.app_auth(),
        st.instance_name.clone(),
        req.admin_key,
    )
    .await?;
    identity.require_operation(keybroker::DeploymentOp::Deploy)?;
    let selection: ExternalDepsPackageSelection = SerializedExternalDepsPackageSelection {
        id: req.id,
        sha256: req.sha256,
    }
    .try_into()
    .map_err(|_| {
        anyhow::anyhow!(ErrorMetadata::bad_request(
            "InvalidExternalDepsPackage",
            "Invalid external dependency package selection",
        ))
    })?;
    // One deadline covers storage lookup and body streaming, including retries.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let archive = tokio::time::timeout_at(
        deadline,
        st.application.download_external_node_deps(&selection),
    )
    .await
    .map_err(|_| anyhow::anyhow!("External dependency download timed out"))??;
    Ok((
        [
            (header::CONTENT_TYPE, "application/zip".to_owned()),
            (header::CONTENT_LENGTH, archive.content_length.to_string()),
            (header::CACHE_CONTROL, "private, no-store".to_owned()),
        ],
        Body::from_stream(external_deps_download_stream(archive, deadline)),
    ))
}

#[try_stream(ok = Bytes, error = std::io::Error)]
async fn external_deps_download_stream(
    mut archive: StorageGetStream,
    deadline: tokio::time::Instant,
) {
    let mut remaining = usize::try_from(archive.content_length)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    loop {
        if tokio::time::Instant::now() >= deadline {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "External dependency download timed out",
            ))?;
        }
        let chunk = tokio::time::timeout_at(deadline, archive.stream.try_next())
            .await
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "External dependency download timed out",
                )
            })??;
        let Some(chunk) = chunk else {
            if remaining != 0 {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "External dependency archive ended before its declared size",
                ))?;
            }
            break;
        };
        // Storage streams may end successfully without yielding the requested range.
        // Enforce the admitted byte count here, not only the response header.
        remaining = remaining.checked_sub(chunk.len()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "External dependency archive exceeds its declared size",
            )
        })?;
        yield chunk;
    }
}

impl TryFrom<StartPushResponse> for SerializedStartPushResponse {
    type Error = anyhow::Error;

    fn try_from(value: StartPushResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            environment_variables: value
                .environment_variables
                .into_iter()
                .map(|(k, v)| Ok((String::from(k), String::from(v))))
                .collect::<anyhow::Result<_>>()?,
            external_deps_id: value
                .external_deps_id
                .map(|id| String::from(DeveloperDocumentId::from(id))),
            component_definition_packages: value
                .component_definition_packages
                .into_iter()
                .map(|(k, v)| Ok((String::from(k), JsonValue::from(ConvexObject::try_from(v)?))))
                .collect::<anyhow::Result<_>>()?,
            app_auth: value
                .app_auth
                .into_iter()
                .map(SerializedAuthInfo::try_from)
                .collect::<anyhow::Result<_>>()?,
            analysis: value
                .analysis
                .into_iter()
                .map(|(k, v)| Ok((String::from(k), v.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
            app: value.app.try_into()?,
            schema_change: value.schema_change.try_into()?,
            node_executor_cutover_protocol_version: Some(1),
        })
    }
}

impl TryFrom<SerializedStartPushResponse> for StartPushResponse {
    type Error = anyhow::Error;

    fn try_from(value: SerializedStartPushResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            environment_variables: value
                .environment_variables
                .into_iter()
                .map(|(k, v)| Ok((k.parse()?, v.parse()?)))
                .collect::<anyhow::Result<_>>()?,
            external_deps_id: value
                .external_deps_id
                .map(|id| {
                    anyhow::Ok(ExternalDepsPackageId::from(
                        id.parse::<DeveloperDocumentId>()?,
                    ))
                })
                .transpose()?,
            component_definition_packages: value
                .component_definition_packages
                .into_iter()
                .map(|(k, v)| {
                    Ok((
                        k.parse()?,
                        SourcePackage::try_from(ConvexObject::try_from(v)?)?,
                    ))
                })
                .collect::<anyhow::Result<_>>()?,
            app_auth: value
                .app_auth
                .into_iter()
                .map(AuthInfo::try_from)
                .collect::<anyhow::Result<_>>()?,
            analysis: value
                .analysis
                .into_iter()
                .map(|(k, v)| Ok((k.parse()?, v.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
            app: value.app.try_into()?,
            schema_change: value.schema_change.try_into()?,
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedStartPushResponse {
    environment_variables: BTreeMap<String, String>,

    // Pointers to uploaded code.
    external_deps_id: Option<String>,
    component_definition_packages: BTreeMap<String, JsonValue>,

    // Analysis results.
    app_auth: Vec<SerializedAuthInfo>,
    analysis: BTreeMap<String, SerializedEvaluatedComponentDefinition>,

    // Typechecking results.
    app: SerializedCheckedComponent,

    // Schema changes.
    schema_change: SerializedSchemaChange,

    node_executor_cutover_protocol_version: Option<u32>,
}

impl TryFrom<EvaluatePushResponse> for SerializedEvaluatePushResponse {
    type Error = anyhow::Error;

    fn try_from(value: EvaluatePushResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            analysis: value
                .analysis
                .map(|analysis| {
                    analysis
                        .into_iter()
                        .map(|(k, v)| Ok((String::from(k), v.try_into()?)))
                        .collect::<anyhow::Result<_>>()
                })
                .transpose()?,
            schema_change: value.schema_change.try_into()?,
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedEvaluatePushResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    analysis: Option<BTreeMap<String, SerializedEvaluatedComponentDefinition>>,
    schema_change: SerializedSchemaChange,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedEvaluateSchemaResponse {
    /// Keyed by component path; "" is the root component.
    component_schema_evaluations: BTreeMap<String, SerializedComponentSchemaPrediction>,
    new_component_definitions: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedComponentSchemaPrediction {
    definition_path: String,
    schema_validation: bool,
    tables: Vec<SerializedTablePrediction>,
    indexes: Vec<SerializedIndexPrediction>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedTablePrediction {
    name: String,
    outcome: TableValidationOutcome,
    num_docs: u64,
    size_bytes: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedIndexPrediction {
    #[serde(flatten)]
    index: SerializedNamedDeveloperIndexConfig,
    change: IndexChangePrediction,
    needs_backfill: bool,
    num_docs: u64,
}

impl TryFrom<EvaluateSchemaPredictionResponse> for SerializedEvaluateSchemaResponse {
    type Error = anyhow::Error;

    fn try_from(value: EvaluateSchemaPredictionResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            component_schema_evaluations: value
                .component_schema_evaluations
                .into_iter()
                .map(|(path, prediction)| Ok((String::from(path), prediction.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
            new_component_definitions: value
                .new_component_definitions
                .into_iter()
                .map(String::from)
                .collect(),
        })
    }
}

impl TryFrom<ComponentSchemaPrediction> for SerializedComponentSchemaPrediction {
    type Error = anyhow::Error;

    fn try_from(value: ComponentSchemaPrediction) -> Result<Self, Self::Error> {
        Ok(Self {
            definition_path: String::from(value.definition_path),
            schema_validation: value.schema_validation,
            tables: value.tables.into_iter().map(Into::into).collect(),
            indexes: value.indexes.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<TablePrediction> for SerializedTablePrediction {
    fn from(value: TablePrediction) -> Self {
        Self {
            name: String::from(value.name),
            outcome: value.outcome,
            num_docs: value.num_docs,
            size_bytes: value.size_bytes,
        }
    }
}

impl From<IndexPrediction> for SerializedIndexPrediction {
    fn from(value: IndexPrediction) -> Self {
        Self {
            index: SerializedNamedDeveloperIndexConfig {
                name: value.name.to_string(),
                index_config: SerializedDeveloperIndexConfig::from(value.config),
            },
            change: value.change,
            needs_backfill: value.needs_backfill,
            num_docs: value.num_docs,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyzedComponent {
    definition: SerializedComponentDefinitionMetadata,
    schema: Option<JsonValue>,
    modules: BTreeMap<String, SerializedAnalyzedModule>,
}

#[debug_handler]
pub async fn start_push(
    State(st): State<LocalAppState>,
    Json(req): Json<StartPushRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let _identity = must_be_admin_from_key(
        st.application.app_auth(),
        st.instance_name.clone(),
        req.admin_key.clone(),
    )
    .await?;
    _identity.require_operation(keybroker::DeploymentOp::Deploy)?;
    let config = req.into_project_config().map_err(|e| {
        anyhow::Error::new(ErrorMetadata::bad_request("InvalidConfig", e.to_string()))
    })?;
    let result =
        st.application.start_push(&config).await.map_err(|e| {
            e.wrap_error_message(|msg| format!("Hit an error while pushing:\n{msg}"))
        })?;
    Ok(Json(SerializedStartPushResponse::try_from(
        result.response,
    )?))
}

// This endpoint is similar to `start_push`, but it does not commit schema or
// index preparation, so it cannot start schema validation or index backfills.
// It always returns the schema diff and includes code generation analysis only
// when requested.
pub async fn evaluate_push(
    MtState(st): MtState<LocalAppState>,
    Json(req): Json<StartPushRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let _identity = must_be_admin_from_key(
        st.application.app_auth(),
        st.instance_name.clone(),
        req.admin_key.clone(),
    )
    .await?;
    _identity.require_operation(keybroker::DeploymentOp::Deploy)?;
    let config = req.into_project_config().map_err(|e| {
        anyhow::Error::new(ErrorMetadata::bad_request("InvalidConfig", e.to_string()))
    })?;
    let resp =
        st.application.evaluate_push(&config).await.map_err(|e| {
            e.wrap_error_message(|msg| format!("Hit an error while pushing:\n{msg}"))
        })?;

    Ok(Json(SerializedEvaluatePushResponse::try_from(resp)?))
}

// Predicts, without side effects, the schema validation and index backfill
// work that pushing this config would trigger: which tables must be walked
// (and why the others can skip), with document counts and sizes, and which
// indexes need backfill. Unlike `evaluate_push`, only the schema bundles are
// evaluated; modules are neither analyzed nor uploaded.
pub async fn evaluate_schema(
    MtState(st): MtState<LocalAppState>,
    Json(req): Json<StartPushRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let identity = must_be_admin_from_key(
        st.application.app_auth(),
        st.instance_name.clone(),
        req.admin_key.clone(),
    )
    .await?;
    identity.require_operation(keybroker::DeploymentOp::Deploy)?;
    let config = req.into_project_config().map_err(|e| {
        anyhow::Error::new(ErrorMetadata::bad_request("InvalidConfig", e.to_string()))
    })?;
    let resp = st.application.evaluate_schema_prediction(&config).await?;
    Ok(Json(SerializedEvaluateSchemaResponse::try_from(resp)?))
}

const DEFAULT_SCHEMA_TIMEOUT_MS: u32 = 10_000;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WaitForSchemaRequest {
    admin_key: String,
    schema_change: SerializedSchemaChange,
    timeout_ms: Option<u32>,
}

pub async fn wait_for_schema(
    MtState(st): MtState<LocalAppState>,
    Json(req): Json<WaitForSchemaRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let identity = must_be_admin_from_key(
        st.application.app_auth(),
        st.instance_name.clone(),
        req.admin_key,
    )
    .await?;
    identity.require_operation(keybroker::DeploymentOp::Deploy)?;
    let timeout = Duration::from_millis(req.timeout_ms.unwrap_or(DEFAULT_SCHEMA_TIMEOUT_MS) as u64);
    let schema_change = req.schema_change.try_into()?;

    // In dry_run mode, we commit the schema changes in start_push so we can
    // validate the schema against existing data.
    let resp = st
        .application
        .wait_for_schema(identity, schema_change, timeout)
        .await?;
    Ok(Json(SchemaStatusJson::from(resp)))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FinishPushRequest {
    pub admin_key: String,
    start_push: SerializedStartPushResponse,
    pub dry_run: bool,
    pub message: Option<String>,
    #[serde(default)]
    pub source_keyed_runtime_activation: Option<SerializedSourceKeyedRuntimeActivation>,
    #[serde(default)]
    pub force_node_cutover: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SerializedSourceKeyedRuntimeGeneration {
    deployment_sha256: String,
    generation_manifest_sha256: String,
    generation_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SerializedSourceKeyedRuntimeExpectedPrior {
    source_package_id: String,
    source_package_sha256: String,
    source_package_runtime_content_sha256: Option<String>,
    runtime_generation: Option<SerializedSourceKeyedRuntimeGeneration>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SerializedSourceKeyedRuntimeActivation {
    expected_prior: SerializedSourceKeyedRuntimeExpectedPrior,
    target_generation: SerializedSourceKeyedRuntimeGeneration,
}

fn parse_source_keyed_runtime_sha256(
    value: String,
    field_name: &'static str,
) -> anyhow::Result<Sha256Digest> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        ErrorMetadata::bad_request(
            "InvalidSourceKeyedRuntimeActivation",
            format!("{field_name} must be a lowercase 64-character SHA-256 digest"),
        )
    );
    let bytes = const_hex::decode(value).map_err(|_| {
        anyhow::Error::new(ErrorMetadata::bad_request(
            "InvalidSourceKeyedRuntimeActivation",
            format!("{field_name} is not a valid SHA-256 digest"),
        ))
    })?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
        anyhow::Error::new(ErrorMetadata::bad_request(
            "InvalidSourceKeyedRuntimeActivation",
            format!("{field_name} is not a valid SHA-256 digest"),
        ))
    })?;
    Ok(Sha256Digest::from(bytes))
}

impl TryFrom<SerializedSourceKeyedRuntimeGeneration> for SourcePackageRuntimeGeneration {
    type Error = anyhow::Error;

    fn try_from(value: SerializedSourceKeyedRuntimeGeneration) -> anyhow::Result<Self> {
        Ok(Self {
            deployment_sha256: parse_source_keyed_runtime_sha256(
                value.deployment_sha256,
                "deploymentSha256",
            )?,
            generation_manifest_sha256: parse_source_keyed_runtime_sha256(
                value.generation_manifest_sha256,
                "generationManifestSha256",
            )?,
            generation_sha256: parse_source_keyed_runtime_sha256(
                value.generation_sha256,
                "generationSha256",
            )?,
        })
    }
}

impl TryFrom<SerializedSourceKeyedRuntimeActivation> for SourceKeyedRuntimeActivation {
    type Error = anyhow::Error;

    fn try_from(value: SerializedSourceKeyedRuntimeActivation) -> anyhow::Result<Self> {
        let expected_prior = value.expected_prior;
        let source_package_id = DeveloperDocumentId::decode(&expected_prior.source_package_id)
            .map_err(|_| {
                anyhow::Error::new(ErrorMetadata::bad_request(
                    "InvalidSourceKeyedRuntimeActivation",
                    "sourcePackageId is invalid",
                ))
            })?;
        Ok(Self {
            expected_prior: SourceKeyedRuntimeExpectedPrior {
                source_package_id,
                source_package_sha256: parse_source_keyed_runtime_sha256(
                    expected_prior.source_package_sha256,
                    "sourcePackageSha256",
                )?,
                source_package_runtime_content_sha256: expected_prior
                    .source_package_runtime_content_sha256
                    .map(|sha256| {
                        parse_source_keyed_runtime_sha256(
                            sha256,
                            "sourcePackageRuntimeContentSha256",
                        )
                    })
                    .transpose()?,
                runtime_generation: expected_prior
                    .runtime_generation
                    .map(TryInto::try_into)
                    .transpose()?,
            },
            target_generation: value.target_generation.try_into()?,
        })
    }
}

#[cfg(test)]
mod source_keyed_runtime_activation_tests {
    use super::*;

    #[test]
    fn external_deps_archive_import_is_explicit_and_ordinary_preparation_is_unchanged() {
        let mut request = serde_json::json!({
            "adminKey": "test-only",
            "nodeDependencies": [{ "name": "example-package", "version": "1.0.0" }],
        });
        assert!(
            serde_json::from_value::<PrepareExternalDepsRequest>(request.clone())
                .unwrap()
                .archive
                .is_none()
        );
        request["archive"] = serde_json::json!({ "sha256": "a".repeat(64), "bytes": "ZXhhbXBsZQ" });
        assert!(
            serde_json::from_value::<PrepareExternalDepsRequest>(request.clone())
                .unwrap()
                .archive
                .is_some()
        );
        request["archive"] = JsonValue::Null;
        assert!(serde_json::from_value::<PrepareExternalDepsRequest>(request).is_err());
    }

    #[tokio::test]
    async fn external_deps_download_stream_preserves_bytes_and_propagates_storage_errors() {
        use futures::{
            stream,
            StreamExt,
        };

        for (content_length, chunks, expected_error) in [
            (
                3,
                vec![Ok(Bytes::from_static(b"a")), Ok(Bytes::from_static(b"bc"))],
                None,
            ),
            (
                3,
                vec![Err(std::io::Error::other("storage read failed"))],
                Some(std::io::ErrorKind::Other),
            ),
            (
                3,
                vec![Ok(Bytes::from_static(b"ab"))],
                Some(std::io::ErrorKind::UnexpectedEof),
            ),
            (
                3,
                vec![Ok(Bytes::from_static(b"abcd"))],
                Some(std::io::ErrorKind::InvalidData),
            ),
        ] {
            let archive = StorageGetStream {
                content_length,
                stream: stream::iter(chunks).boxed(),
            };
            let result = external_deps_download_stream(
                archive,
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .try_collect::<Vec<_>>()
            .await;
            match expected_error {
                None => assert_eq!(result.unwrap().concat(), b"abc"),
                Some(kind) => assert_eq!(result.unwrap_err().kind(), kind),
            }
        }
    }

    #[tokio::test]
    async fn external_deps_download_stream_times_out_and_drops_cancelled_reads() {
        use futures::{
            stream,
            FutureExt,
            StreamExt,
        };

        for (stream, timeout) in [
            (stream::pending().boxed(), Duration::ZERO),
            (
                stream::iter([Ok(Bytes::from_static(b"abc"))]).boxed(),
                Duration::ZERO,
            ),
            (stream::pending().boxed(), Duration::from_millis(1)),
        ] {
            let archive = StorageGetStream {
                content_length: 3,
                stream,
            };
            let error =
                external_deps_download_stream(archive, tokio::time::Instant::now() + timeout)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        }

        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let archive = StorageGetStream {
            content_length: 3,
            stream: stream::once(async move {
                let _sender = sender;
                std::future::pending::<std::io::Result<Bytes>>().await
            })
            .boxed(),
        };
        let mut download = external_deps_download_stream(
            archive,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .boxed();
        assert!(download.try_next().now_or_never().is_none());
        drop(download);
        assert!(receiver.await.is_err());
    }

    #[test]
    fn external_deps_selection_is_exact_and_ordinary_requests_may_omit_it() {
        let mut request = serde_json::json!({
            "adminKey": "test-only",
            "functions": "convex/",
            "appDefinition": {
                "definition": null, "dependencies": [], "schema": null,
                "changedModules": [], "unchangedModuleHashes": [], "udfServerVersion": "1.0.0",
            },
            "componentDefinitions": [],
            "nodeDependencies": [{ "name": "example-package", "version": "1.0.0" }],
            "nodeVersion": "24",
        });
        let ordinary: StartPushRequest = serde_json::from_value(request.clone()).unwrap();
        assert!(ordinary
            .into_project_config()
            .unwrap()
            .external_deps_package
            .is_none());
        request["externalDepsPackage"] = JsonValue::Null;
        assert!(serde_json::from_value::<StartPushRequest>(request.clone()).is_err());
        request["externalDepsPackage"] = serde_json::json!({
            "id": DeveloperDocumentId::MIN.encode(), "sha256": "a".repeat(64),
        });
        let selected: StartPushRequest = serde_json::from_value(request.clone()).unwrap();
        let selection = selected
            .into_project_config()
            .unwrap()
            .external_deps_package
            .unwrap();
        assert_eq!(selection.sha256, Sha256Digest::from([0xaa; 32]));
        assert_eq!(selection.id, DeveloperDocumentId::MIN.into());
        for invalid in ["A".repeat(64), "g".repeat(64), "a".repeat(63)] {
            request["externalDepsPackage"]["sha256"] = invalid.into();
            let parsed: StartPushRequest = serde_json::from_value(request.clone()).unwrap();
            assert!(parsed.into_project_config().is_err());
        }
        request["externalDepsPackage"]["storageKey"] = "unexpected".into();
        assert!(serde_json::from_value::<StartPushRequest>(request).is_err());
    }

    fn activation_json() -> JsonValue {
        serde_json::json!({
            "expectedPrior": {
                "sourcePackageId": DeveloperDocumentId::MIN.encode(),
                "sourcePackageSha256": "4".repeat(64),
                "sourcePackageRuntimeContentSha256": "5".repeat(64),
                "runtimeGeneration": {
                    "deploymentSha256": "1".repeat(64),
                    "generationManifestSha256": "2".repeat(64),
                    "generationSha256": "3".repeat(64),
                },
            },
            "targetGeneration": {
                "deploymentSha256": "6".repeat(64),
                "generationManifestSha256": "7".repeat(64),
                "generationSha256": "8".repeat(64),
            },
        })
    }

    #[test]
    fn parses_exact_source_keyed_runtime_activation() {
        let serialized: SerializedSourceKeyedRuntimeActivation =
            serde_json::from_value(activation_json()).unwrap();
        let activation = SourceKeyedRuntimeActivation::try_from(serialized).unwrap();

        assert_eq!(
            activation,
            SourceKeyedRuntimeActivation {
                expected_prior: SourceKeyedRuntimeExpectedPrior {
                    source_package_id: DeveloperDocumentId::MIN,
                    source_package_sha256: Sha256Digest::from([0x44; 32]),
                    source_package_runtime_content_sha256: Some(Sha256Digest::from([0x55; 32])),
                    runtime_generation: Some(SourcePackageRuntimeGeneration {
                        deployment_sha256: Sha256Digest::from([0x11; 32]),
                        generation_manifest_sha256: Sha256Digest::from([0x22; 32]),
                        generation_sha256: Sha256Digest::from([0x33; 32]),
                    }),
                },
                target_generation: SourcePackageRuntimeGeneration {
                    deployment_sha256: Sha256Digest::from([0x66; 32]),
                    generation_manifest_sha256: Sha256Digest::from([0x77; 32]),
                    generation_sha256: Sha256Digest::from([0x88; 32]),
                },
            }
        );
    }

    #[test]
    fn rejects_non_lowercase_source_keyed_runtime_digests() {
        for invalid_digest in ["A".repeat(64), "g".repeat(64), "1".repeat(63)] {
            let mut value = activation_json();
            value["targetGeneration"]["generationSha256"] = invalid_digest.into();
            let serialized: SerializedSourceKeyedRuntimeActivation =
                serde_json::from_value(value).unwrap();
            let error = SourceKeyedRuntimeActivation::try_from(serialized)
                .err()
                .expect("invalid digest must be rejected");
            assert!(error
                .to_string()
                .contains("must be a lowercase 64-character SHA-256 digest"));
        }
    }

    #[test]
    fn rejects_unknown_source_keyed_runtime_activation_fields() {
        for path in ["expectedPrior", "targetGeneration"] {
            let mut value = activation_json();
            value[path]["unexpected"] = true.into();
            let error = serde_json::from_value::<SerializedSourceKeyedRuntimeActivation>(value)
                .err()
                .expect("unknown nested field must be rejected");
            assert!(error.to_string().contains("unknown field `unexpected`"));
        }
    }
}

/// Internal version that returns the commit timestamp for use by conductor
pub async fn finish_push_internal(
    st: &LocalAppState,
    request_metadata: RequestMetadata,
    req: FinishPushRequest,
) -> anyhow::Result<(SerializedFinishPushDiff, Option<common::types::Timestamp>)> {
    let identity = must_be_admin_from_key(
        st.application.app_auth(),
        st.instance_name.clone(),
        req.admin_key.clone(),
    )
    .await?;
    identity.require_operation(keybroker::DeploymentOp::Deploy)?;

    if req.force_node_cutover && !req.dry_run {
        anyhow::ensure!(
            req.start_push.node_executor_cutover_protocol_version == Some(1),
            ErrorMetadata::bad_request(
                "NodeExecutorCutoverProtocolUnsupported",
                "This backend does not advertise forced Node executor cutover protocol version 1",
            )
        );
    }
    let start_push = StartPushResponse::try_from(req.start_push)?;
    let message = req.message.map(PushMessage::try_from).transpose()?;
    let source_keyed_runtime_activation = req
        .source_keyed_runtime_activation
        .map(TryInto::try_into)
        .transpose()?;

    // We can't actually run `finish_push` in a dry run, since we rolled back all of
    // our changes during start push.
    if req.dry_run {
        tracing::info!("Skipping finish_push in dry run");
        let empty_diff = FinishPushDiff::default();
        return Ok((SerializedFinishPushDiff::try_from(empty_diff)?, None));
    }

    let (resp, ts) = st
        .application
        .finish_push(
            identity,
            request_metadata,
            start_push,
            message,
            source_keyed_runtime_activation,
            req.force_node_cutover,
        )
        .await
        .map_err(|e| {
            if e.short_msg() == "NodeExecutorCutoverFailedAfterCommit" {
                e
            } else {
                e.wrap_error_message(|msg| format!("Hit an error while pushing:\n{msg}"))
            }
        })?;
    Ok((SerializedFinishPushDiff::try_from(resp)?, Some(ts)))
}

pub async fn finish_push(
    MtState(st): MtState<LocalAppState>,
    ExtractRequestMetadata(request_metadata): ExtractRequestMetadata,
    Json(req): Json<FinishPushRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let (diff, _ts) = finish_push_internal(&st, request_metadata, req).await?;
    Ok(Json(diff))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportPushCompletedRequest {
    admin_key: String,
    spans: Vec<SerializedCompletedSpan>,
}

pub async fn report_push_completed(
    st: LocalAppState,
    req: ReportPushCompletedRequest,
) -> anyhow::Result<Vec<SpanRecord>> {
    let identity = must_be_admin_from_key(
        st.application.app_auth(),
        st.instance_name.clone(),
        req.admin_key.clone(),
    )
    .await?;
    identity.require_operation(keybroker::DeploymentOp::Deploy)?;
    let spans = req
        .spans
        .into_iter()
        .map(|s| s.try_into())
        .collect::<anyhow::Result<Vec<SpanRecord>>>()?;
    Ok(spans)
}

#[debug_handler]
pub async fn report_push_completed_handler(
    State(st): State<LocalAppState>,
    Json(req): Json<ReportPushCompletedRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let spans = report_push_completed(st, req).await?;
    tracing::debug!("Received spans: {:?}", spans);
    Ok(Json(()))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedFinishPushDiff {
    auth_diff: AuthDiff,
    definition_diffs: BTreeMap<String, SerializedComponentDefinitionDiff>,
    component_diffs: BTreeMap<String, SerializedComponentDiff>,
}

impl TryFrom<FinishPushDiff> for SerializedFinishPushDiff {
    type Error = anyhow::Error;

    fn try_from(value: FinishPushDiff) -> Result<Self, Self::Error> {
        Ok(Self {
            auth_diff: value.auth_diff,
            definition_diffs: value
                .definition_diffs
                .into_iter()
                .map(|(k, v)| Ok((String::from(k), v.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
            component_diffs: value
                .component_diffs
                .into_iter()
                .map(|(k, v)| Ok((String::from(k), v.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerializedCompletedSpan {
    trace_id: String,
    parent_id: String,
    span_id: String,
    begin_time_unix_ns: String,
    duration_ns: String,
    name: String,
    properties: BTreeMap<String, String>,
    events: Vec<SerializedEventRecord>,
}

impl TryFrom<SerializedCompletedSpan> for SpanRecord {
    type Error = anyhow::Error;

    fn try_from(value: SerializedCompletedSpan) -> Result<Self, Self::Error> {
        let trace_id_buf = base64::decode_urlsafe(&value.trace_id)?;
        let trace_id = u128::from_le_bytes(trace_id_buf[..].try_into()?);

        let parent_id_buf = base64::decode_urlsafe(&value.parent_id)?;
        let parent_id = u64::from_le_bytes(parent_id_buf[..].try_into()?);

        let span_id_buf = base64::decode_urlsafe(&value.span_id)?;
        let span_id = u64::from_le_bytes(span_id_buf[..].try_into()?);

        let begin_time_unix_ns_buf = base64::decode_urlsafe(&value.begin_time_unix_ns)?;
        let begin_time_unix_ns = u64::from_le_bytes(begin_time_unix_ns_buf[..].try_into()?);

        let duration_ns_buf = base64::decode_urlsafe(&value.duration_ns)?;
        let duration_ns = u64::from_le_bytes(duration_ns_buf[..].try_into()?);

        let properties = value
            .properties
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();

        let events = value
            .events
            .into_iter()
            .map(|e| e.try_into())
            .collect::<anyhow::Result<_>>()?;

        Ok(Self {
            trace_id: TraceId(trace_id),
            parent_id: SpanId(parent_id),
            span_id: SpanId(span_id),
            begin_time_unix_ns,
            duration_ns,
            name: value.name.into(),
            properties,
            events,
            links: vec![],
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerializedEventRecord {
    name: String,
    timestamp_unix_ns: String,
    properties: BTreeMap<String, String>,
}

impl TryFrom<SerializedEventRecord> for EventRecord {
    type Error = anyhow::Error;

    fn try_from(value: SerializedEventRecord) -> Result<Self, Self::Error> {
        let timestamp_unix_ns_buf = base64::decode_urlsafe(&value.timestamp_unix_ns)?;
        let timestamp_unix_ns = u64::from_le_bytes(timestamp_unix_ns_buf[..].try_into()?);
        let properties = value
            .properties
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();
        Ok(Self {
            name: value.name.into(),
            timestamp_unix_ns,
            properties,
        })
    }
}
