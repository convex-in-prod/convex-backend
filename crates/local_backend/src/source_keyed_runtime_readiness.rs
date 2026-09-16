use axum::response::IntoResponse;
use common::http::{
    extract::{
        Json,
        MtState,
    },
    HttpResponseError,
};
use errors::ErrorMetadata;
use keybroker::DeploymentOp;
use roles::RequireDeploymentOp;
use serde::{
    Deserialize,
    Serialize,
};
use utoipa::ToSchema;

use crate::{
    authentication::ExtractHeaderOnlyDeployKeyIdentity,
    LocalAppState,
};

const SOURCE_KEYED_RUNTIME_READINESS_KIND: &str = "convex-local-source-keyed-runtime-readiness-v2";

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceKeyedRuntimeReadinessRequest {
    pub deployment_sha256: String,
    pub generation_manifest_sha256: String,
    pub generation_sha256: String,
    pub source_package_runtime_content_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SourceKeyedRuntimeReadinessStatus {
    Ready,
    NotStaged,
}

impl From<isolate::SourceKeyedRuntimeReadiness> for SourceKeyedRuntimeReadinessStatus {
    fn from(value: isolate::SourceKeyedRuntimeReadiness) -> Self {
        match value {
            isolate::SourceKeyedRuntimeReadiness::Ready => Self::Ready,
            isolate::SourceKeyedRuntimeReadiness::NotStaged => Self::NotStaged,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SourceKeyedRuntimeReadinessResponse {
    pub deployment_sha256: String,
    pub generation_manifest_sha256: String,
    pub generation_sha256: String,
    pub kind: &'static str,
    pub source_package_runtime_content_sha256: String,
    pub status: SourceKeyedRuntimeReadinessStatus,
}

fn validate_sha256(value: &str, field: &'static str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        ErrorMetadata::bad_request(
            "InvalidSourceKeyedRuntimeReadinessSha256",
            format!("{field} must be a lowercase 64-character SHA-256 digest"),
        )
    );
    Ok(())
}

fn map_source_keyed_runtime_readiness_error(error: anyhow::Error) -> anyhow::Error {
    error.context(ErrorMetadata::operational_internal_server_error())
}

const MAX_LOGGED_READINESS_ERROR_BYTES: usize = 16 * 1024;

fn redact_absolute_paths(message: &str) -> String {
    let mut redacted = String::with_capacity(message.len());
    let mut chars = message.char_indices().peekable();
    let mut at_token_boundary = true;
    while let Some((_, character)) = chars.next() {
        if character != '/' || !at_token_boundary {
            redacted.push(character);
            at_token_boundary = character.is_ascii_whitespace() || "\"'(=".contains(character);
            continue;
        }
        while let Some((_, next)) = chars.peek() {
            if next.is_ascii_whitespace() {
                break;
            }
            chars.next();
        }
        redacted.push_str("<path>");
        at_token_boundary = false;
    }
    if redacted.len() > MAX_LOGGED_READINESS_ERROR_BYTES {
        let mut end = MAX_LOGGED_READINESS_ERROR_BYTES;
        while !redacted.is_char_boundary(end) {
            end -= 1;
        }
        redacted.truncate(end);
        redacted.push_str("...");
    }
    redacted
}

fn log_source_keyed_runtime_readiness_failure(
    request: &SourceKeyedRuntimeReadinessRequest,
    error: &anyhow::Error,
) {
    let error_chain = redact_absolute_paths(&format!("{error:#}"));
    tracing::error!(
        deployment_sha256 = %request.deployment_sha256,
        generation_manifest_sha256 = %request.generation_manifest_sha256,
        generation_sha256 = %request.generation_sha256,
        source_package_runtime_content_sha256 = %request.source_package_runtime_content_sha256,
        error_chain = %error_chain,
        "Source-keyed runtime readiness failed"
    );
}

/// Return whether an exact source/generation pair has a retained,
/// authenticated, load-verified source-keyed generation.
///
/// This endpoint is intentionally read-only. It ensures only the requested
/// exact generation; the response grants no activation authority and contains
/// no registry paths or loader details.
#[utoipa::path(
    post,
    path = "/source_keyed_runtime_readiness",
    tag = "Static Hermes Wasmtime",
    request_body = SourceKeyedRuntimeReadinessRequest,
    responses((status = 200, body = SourceKeyedRuntimeReadinessResponse)),
    security(("Deploy Key" = [])),
)]
pub async fn source_keyed_runtime_readiness(
    MtState(_st): MtState<LocalAppState>,
    ExtractHeaderOnlyDeployKeyIdentity(identity): ExtractHeaderOnlyDeployKeyIdentity,
    Json(request): Json<SourceKeyedRuntimeReadinessRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    identity.require_operation(DeploymentOp::Deploy)?;
    for (value, field) in [
        (&request.deployment_sha256, "deploymentSha256"),
        (
            &request.generation_manifest_sha256,
            "generationManifestSha256",
        ),
        (&request.generation_sha256, "generationSha256"),
        (
            &request.source_package_runtime_content_sha256,
            "sourcePackageRuntimeContentSha256",
        ),
    ] {
        validate_sha256(value, field)?;
    }

    let generation = isolate::SourceKeyedRuntimeGenerationIdentity {
        deployment_sha256: request.deployment_sha256.clone(),
        generation_manifest_sha256: request.generation_manifest_sha256.clone(),
        generation_sha256: request.generation_sha256.clone(),
    };

    let status = isolate::source_keyed_runtime_readiness(
        &request.source_package_runtime_content_sha256,
        &generation,
    )
    .await
    .map(SourceKeyedRuntimeReadinessStatus::from)
    .map_err(|error| {
        log_source_keyed_runtime_readiness_failure(&request, &error);
        map_source_keyed_runtime_readiness_error(error)
    })?;

    Ok(Json(SourceKeyedRuntimeReadinessResponse {
        deployment_sha256: request.deployment_sha256,
        generation_manifest_sha256: request.generation_manifest_sha256,
        generation_sha256: request.generation_sha256,
        kind: SOURCE_KEYED_RUNTIME_READINESS_KIND,
        source_package_runtime_content_sha256: request.source_package_runtime_content_sha256,
        status,
    }))
}

#[cfg(test)]
mod tests {
    use axum::{
        body::to_bytes,
        response::IntoResponse,
    };
    use http::StatusCode;
    use keybroker::Identity;

    use super::*;

    #[test]
    fn accepts_only_lowercase_sha256_digests() {
        assert!(validate_sha256(&"a".repeat(64), "digest").is_ok());
        assert!(validate_sha256(&"0".repeat(64), "digest").is_ok());
        assert!(validate_sha256(&"A".repeat(64), "digest").is_err());
        assert!(validate_sha256(&"g".repeat(64), "digest").is_err());
        assert!(validate_sha256(&"a".repeat(63), "digest").is_err());
        assert!(validate_sha256(&"a".repeat(65), "digest").is_err());
    }

    #[test]
    fn request_rejects_unknown_fields() {
        let request =
            serde_json::from_value::<SourceKeyedRuntimeReadinessRequest>(serde_json::json!({
                "deploymentSha256": "b".repeat(64),
                "generationManifestSha256": "c".repeat(64),
                "generationSha256": "d".repeat(64),
                "sourcePackageRuntimeContentSha256": "a".repeat(64),
                "unexpected": true,
            }));
        assert!(request.is_err());
    }

    #[test]
    fn readiness_operation_requires_deploy_permission() {
        assert!(Identity::system()
            .require_operation(DeploymentOp::Deploy)
            .is_ok());
        assert!(Identity::Unknown(None)
            .require_operation(DeploymentOp::Deploy)
            .is_err());
    }

    #[test]
    fn response_has_the_stable_wire_contract() {
        let response = SourceKeyedRuntimeReadinessResponse {
            deployment_sha256: "b".repeat(64),
            generation_manifest_sha256: "c".repeat(64),
            generation_sha256: "d".repeat(64),
            kind: SOURCE_KEYED_RUNTIME_READINESS_KIND,
            source_package_runtime_content_sha256: "a".repeat(64),
            status: SourceKeyedRuntimeReadinessStatus::NotStaged,
        };
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            serde_json::json!({
                "deploymentSha256": "b".repeat(64),
                "generationManifestSha256": "c".repeat(64),
                "generationSha256": "d".repeat(64),
                "kind": SOURCE_KEYED_RUNTIME_READINESS_KIND,
                "sourcePackageRuntimeContentSha256": "a".repeat(64),
                "status": "notStaged",
            })
        );
    }

    #[test]
    fn maps_internal_readiness_without_extra_states() {
        assert_eq!(
            SourceKeyedRuntimeReadinessStatus::from(isolate::SourceKeyedRuntimeReadiness::Ready),
            SourceKeyedRuntimeReadinessStatus::Ready,
        );
        assert_eq!(
            SourceKeyedRuntimeReadinessStatus::from(
                isolate::SourceKeyedRuntimeReadiness::NotStaged,
            ),
            SourceKeyedRuntimeReadinessStatus::NotStaged,
        );
    }

    #[test]
    fn readiness_error_logging_redacts_absolute_paths_and_bounds_length() {
        let message = format!(
            "failed to load /var/lib/convex-runtime/generations/abc: {}",
            "x".repeat(MAX_LOGGED_READINESS_ERROR_BYTES * 2),
        );
        let redacted = redact_absolute_paths(&message);
        assert!(!redacted.contains("/var/lib/convex-runtime"));
        assert!(redacted.starts_with("failed to load <path>"));
        assert!(redacted.ends_with("..."));
        assert!(redacted.len() <= MAX_LOGGED_READINESS_ERROR_BYTES + 3);
    }

    #[tokio::test]
    async fn readiness_failures_keep_the_operational_source_but_return_opaque_metadata() {
        let private_detail = "private readiness source detail";
        let error = map_source_keyed_runtime_readiness_error(anyhow::anyhow!(private_detail));
        assert!(error
            .chain()
            .any(|source| source.to_string() == private_detail));

        let response = HttpResponseError::from(error).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "code": "InternalServerError",
                "message": "Your request couldn't be completed. Try again later.",
            })
        );
        assert!(!String::from_utf8_lossy(&body).contains(private_detail));
    }
}
