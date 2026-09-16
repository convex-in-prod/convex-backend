use axum::response::IntoResponse;
use common::{
    http::{
        extract::MtState,
        HttpResponseError,
    },
    sha256::Sha256Digest,
};
use keybroker::DeploymentOp;
use model::source_packages::SourcePackageModel;
use roles::RequireDeploymentOp;
use serde::Serialize;
use utoipa::ToSchema;
use value::TableNamespace;

use crate::{
    authentication::ExtractHeaderOnlyDeployKeyIdentity,
    LocalAppState,
};

const SOURCE_KEYED_RUNTIME_ACTIVE_PAIR_KIND: &str =
    "convex-local-source-keyed-runtime-active-state-v2";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SourceKeyedRuntimeActivePairStatus {
    Paired,
    NotPaired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SourceKeyedRuntimeActiveSource {
    pub source_package_id: String,
    pub source_package_runtime_content_sha256: Option<String>,
    pub source_package_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SourceKeyedRuntimeActiveGeneration {
    pub deployment_sha256: String,
    pub generation_manifest_sha256: String,
    pub generation_sha256: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SourceKeyedRuntimeActivePairResponse {
    pub kind: &'static str,
    pub generation: Option<SourceKeyedRuntimeActiveGeneration>,
    pub source: Option<SourceKeyedRuntimeActiveSource>,
    pub status: SourceKeyedRuntimeActivePairStatus,
}

fn response(
    source: Option<SourceKeyedRuntimeActiveSource>,
    generation: Option<isolate::SourceKeyedRuntimeGenerationIdentity>,
) -> SourceKeyedRuntimeActivePairResponse {
    let generation = generation.map(|generation| SourceKeyedRuntimeActiveGeneration {
        deployment_sha256: generation.deployment_sha256,
        generation_manifest_sha256: generation.generation_manifest_sha256,
        generation_sha256: generation.generation_sha256,
    });
    let paired = source
        .as_ref()
        .is_some_and(|source| source.source_package_runtime_content_sha256.is_some())
        && generation.is_some();
    SourceKeyedRuntimeActivePairResponse {
        kind: SOURCE_KEYED_RUNTIME_ACTIVE_PAIR_KIND,
        status: if paired {
            SourceKeyedRuntimeActivePairStatus::Paired
        } else {
            SourceKeyedRuntimeActivePairStatus::NotPaired
        },
        generation,
        source,
    }
}

fn map_runtime_generation_error(error: anyhow::Error) -> anyhow::Error {
    error.context(errors::ErrorMetadata::operational_internal_server_error())
}

/// Return the database-committed source package and its load-verified runtime
/// generation. The source package is read from one database snapshot, and the
/// source-keyed registry ensures the exact immutable generation bound to that
/// runtime-content digest before reporting the pair.
#[utoipa::path(
    get,
    path = "/source_keyed_runtime_active_pair",
    tag = "Static Hermes Wasmtime",
    responses((status = 200, body = SourceKeyedRuntimeActivePairResponse)),
    security(("Deploy Key" = [])),
)]
pub async fn source_keyed_runtime_active_pair(
    MtState(st): MtState<LocalAppState>,
    ExtractHeaderOnlyDeployKeyIdentity(identity): ExtractHeaderOnlyDeployKeyIdentity,
) -> Result<impl IntoResponse, HttpResponseError> {
    identity.require_operation(DeploymentOp::Deploy)?;
    let mut tx = st.application.begin(identity).await?;
    let source_package = SourcePackageModel::new(&mut tx, TableNamespace::Global)
        .get_latest_record()
        .await?;
    let source_package_id = source_package
        .as_ref()
        .map(|package| package.developer_id().to_string());
    let source_package_sha256 = source_package
        .as_ref()
        .map(|package| package.sha256.as_hex());
    let source_package_runtime_content_sha256 = source_package
        .as_ref()
        .and_then(|package| package.runtime_content_sha256.as_ref())
        .map(Sha256Digest::as_hex);
    let selected_runtime_generation = source_package
        .as_ref()
        .and_then(|package| package.runtime_generation.as_ref())
        .map(|generation| isolate::SourceKeyedRuntimeGenerationIdentity {
            deployment_sha256: generation.deployment_sha256.as_hex(),
            generation_manifest_sha256: generation.generation_manifest_sha256.as_hex(),
            generation_sha256: generation.generation_sha256.as_hex(),
        });
    tx.into_token()?;

    let generation = match (
        source_package_runtime_content_sha256.as_deref(),
        selected_runtime_generation.as_ref(),
    ) {
        (Some(runtime_content_sha256), Some(selected_runtime_generation)) => {
            isolate::source_keyed_runtime_generation_identity(
                runtime_content_sha256,
                selected_runtime_generation,
            )
            .await
            .map_err(map_runtime_generation_error)?
        },
        _ => None,
    };
    let source = source_package_id.zip(source_package_sha256).map(
        |(source_package_id, source_package_sha256)| SourceKeyedRuntimeActiveSource {
            source_package_id,
            source_package_runtime_content_sha256,
            source_package_sha256,
        },
    );
    Ok(common::http::extract::Json(response(source, generation)))
}

#[cfg(test)]
mod tests {
    use keybroker::Identity;

    use super::*;

    fn generation() -> isolate::SourceKeyedRuntimeGenerationIdentity {
        isolate::SourceKeyedRuntimeGenerationIdentity {
            deployment_sha256: "1".repeat(64),
            generation_manifest_sha256: "2".repeat(64),
            generation_sha256: "3".repeat(64),
        }
    }

    fn source(runtime_content_sha256: Option<String>) -> SourceKeyedRuntimeActiveSource {
        SourceKeyedRuntimeActiveSource {
            source_package_id: "source-package-id".to_owned(),
            source_package_runtime_content_sha256: runtime_content_sha256,
            source_package_sha256: "4".repeat(64),
        }
    }

    #[test]
    fn exact_pair_requires_both_database_source_identities_and_a_ready_generation() {
        let paired = response(Some(source(Some("5".repeat(64)))), Some(generation()));
        assert_eq!(paired.status, SourceKeyedRuntimeActivePairStatus::Paired);
        assert_eq!(
            paired.generation,
            Some(SourceKeyedRuntimeActiveGeneration {
                deployment_sha256: "1".repeat(64),
                generation_manifest_sha256: "2".repeat(64),
                generation_sha256: "3".repeat(64),
            })
        );
        assert_eq!(paired.source, Some(source(Some("5".repeat(64)))));

        for not_paired in [
            response(None, Some(generation())),
            response(Some(source(None)), Some(generation())),
            response(Some(source(Some("5".repeat(64)))), None),
        ] {
            assert_eq!(
                not_paired.status,
                SourceKeyedRuntimeActivePairStatus::NotPaired
            );
        }
    }

    #[test]
    fn active_pair_operation_requires_deploy_permission() {
        assert!(Identity::system()
            .require_operation(DeploymentOp::Deploy)
            .is_ok());
        assert!(Identity::Unknown(None)
            .require_operation(DeploymentOp::Deploy)
            .is_err());
    }

    #[test]
    fn response_has_the_stable_wire_contract() {
        let response = response(Some(source(Some("5".repeat(64)))), Some(generation()));
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            serde_json::json!({
                "generation": {
                    "deploymentSha256": "1".repeat(64),
                    "generationManifestSha256": "2".repeat(64),
                    "generationSha256": "3".repeat(64),
                },
                "kind": SOURCE_KEYED_RUNTIME_ACTIVE_PAIR_KIND,
                "source": {
                    "sourcePackageId": "source-package-id",
                    "sourcePackageRuntimeContentSha256": "5".repeat(64),
                    "sourcePackageSha256": "4".repeat(64),
                },
                "status": "paired",
            })
        );
    }
}
