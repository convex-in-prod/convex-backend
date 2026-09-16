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

#[derive(Clone, Copy, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum QuarantineAction {
    Quarantine,
    Clear,
    Initialize,
}

impl From<QuarantineAction> for isolate::StaticHermesWasmtimeQuarantineAction {
    fn from(action: QuarantineAction) -> Self {
        match action {
            QuarantineAction::Quarantine => Self::Quarantine,
            QuarantineAction::Clear => Self::Clear,
            QuarantineAction::Initialize => Self::Initialize,
        }
    }
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateStaticHermesWasmtimeQuarantineRequest {
    /// `quarantine` adds a sticky selector; `clear` removes only that exact
    /// selector; `initialize` repairs a missing or ambiguous policy as an
    /// empty valid snapshot.
    action: QuarantineAction,
    /// Required for `quarantine` and `clear`; omitted for `initialize`.
    module_path: Option<String>,
    function_name: Option<String>,
    /// Required operator-supplied change rationale. It is bounded and stored
    /// as policy evidence, not emitted with application values.
    reason: String,
    /// Required bounded operator/change reference for audit correlation.
    operator_reference: String,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct InFlightInvocationsResponse {
    existing_invocations_retain_captured_engine: bool,
    /// Aggregate isolate-worker visibility is unavailable at this control
    /// boundary, so no drain count is claimed.
    aggregate_isolate_workers: Option<usize>,
    static_hermes_route_specific: bool,
    drain_wait_supported: bool,
    observability_gap: &'static str,
}

fn in_flight_invocations_response() -> InFlightInvocationsResponse {
    InFlightInvocationsResponse {
        existing_invocations_retain_captured_engine: true,
        aggregate_isolate_workers: None,
        static_hermes_route_specific: false,
        drain_wait_supported: false,
        observability_gap: "the application control boundary has no route-specific drain count",
    }
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuarantineEntryResponse {
    module_path: String,
    function_name: Option<String>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateStaticHermesWasmtimeQuarantineResponse {
    action: String,
    changed: bool,
    snapshot_version: u64,
    entries: Vec<QuarantineEntryResponse>,
    /// Route resolution is invocation-scoped. This update does not reroute or
    /// interrupt invocations that already captured their engine choice.
    in_flight_invocations: InFlightInvocationsResponse,
}

fn map_quarantine_mutation_error(
    error: isolate::StaticHermesWasmtimeQuarantineMutationError,
) -> HttpResponseError {
    let metadata = match &error {
        isolate::StaticHermesWasmtimeQuarantineMutationError::InvalidSelector(_) => {
            ErrorMetadata::bad_request(
                "InvalidWasmQuarantineSelector",
                "The Wasm quarantine selector is invalid",
            )
        },
        isolate::StaticHermesWasmtimeQuarantineMutationError::InvalidEvidence(_) => {
            ErrorMetadata::bad_request(
                "InvalidWasmQuarantineEvidence",
                "The Wasm quarantine reason or operator reference is invalid",
            )
        },
        isolate::StaticHermesWasmtimeQuarantineMutationError::InitializationRequired => {
            ErrorMetadata::conflict(
                "WasmQuarantineInitializationRequired",
                "Initialize the Wasm quarantine policy before updating it",
            )
        },
        isolate::StaticHermesWasmtimeQuarantineMutationError::DurablePolicyRequired => {
            ErrorMetadata::conflict(
                "WasmQuarantineDurablePolicyRequired",
                "The Wasm quarantine policy has no durable storage path",
            )
        },
        isolate::StaticHermesWasmtimeQuarantineMutationError::PolicyCapacityReached => {
            ErrorMetadata::conflict(
                "WasmQuarantinePolicyCapacityReached",
                "The Wasm quarantine policy cannot accept another selector",
            )
        },
        isolate::StaticHermesWasmtimeQuarantineMutationError::Persistence(_)
        | isolate::StaticHermesWasmtimeQuarantineMutationError::Internal(_) => {
            ErrorMetadata::operational_internal_server_error()
        },
    };
    anyhow::Error::new(error).context(metadata).into()
}

/// Apply a subtractive Wasm route quarantine.
///
/// The durable policy is atomically replaced and an immutable snapshot is
/// published for newly resolved invocations. Selectors are exact module paths
/// with an optional exact export name. This handler is mounted only in the
/// local dashboard/admin router and authenticates a deployment key before
/// applying a change.
#[utoipa::path(
    post,
    path = "/static_hermes_wasmtime_quarantine",
    tag = "Static Hermes Wasmtime",
    request_body = UpdateStaticHermesWasmtimeQuarantineRequest,
    responses((status = 200, body = UpdateStaticHermesWasmtimeQuarantineResponse)),
    security(("Deploy Key" = [])),
)]
pub async fn update_static_hermes_wasmtime_quarantine(
    MtState(_st): MtState<LocalAppState>,
    ExtractHeaderOnlyDeployKeyIdentity(identity): ExtractHeaderOnlyDeployKeyIdentity,
    Json(request): Json<UpdateStaticHermesWasmtimeQuarantineRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    identity.require_operation(DeploymentOp::Deploy)?;
    let update = match request.action {
        QuarantineAction::Initialize => {
            if request.module_path.is_some() || request.function_name.is_some() {
                return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
                    "InvalidWasmQuarantineInitializeRequest",
                    "modulePath and functionName must be omitted for initialize",
                ))
                .into());
            }
            isolate::static_hermes_wasmtime_quarantine()
                .initialize(request.reason, request.operator_reference)
                .map_err(map_quarantine_mutation_error)?
        },
        QuarantineAction::Quarantine | QuarantineAction::Clear => {
            let selector = isolate::StaticHermesWasmtimeQuarantineSelector::new(
                request.module_path.ok_or_else(|| {
                    anyhow::anyhow!(ErrorMetadata::bad_request(
                        "MissingWasmQuarantineModulePath",
                        "modulePath is required for quarantine and clear",
                    ))
                })?,
                request.function_name,
            )
            .map_err(|error| {
                map_quarantine_mutation_error(
                    isolate::StaticHermesWasmtimeQuarantineMutationError::InvalidSelector(error),
                )
            })?;
            isolate::static_hermes_wasmtime_quarantine()
                .update(
                    request.action.into(),
                    selector,
                    request.reason,
                    request.operator_reference,
                )
                .map_err(map_quarantine_mutation_error)?
        },
    };
    let entries = update
        .snapshot
        .entries()
        .into_iter()
        .map(|entry| QuarantineEntryResponse {
            module_path: entry.selector.module_path().to_owned(),
            function_name: entry.selector.function_name().map(str::to_owned),
        })
        .collect();
    Ok(Json(UpdateStaticHermesWasmtimeQuarantineResponse {
        action: update.action.to_string(),
        changed: update.changed,
        snapshot_version: update.snapshot.version(),
        entries,
        in_flight_invocations: in_flight_invocations_response(),
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
    fn response_does_not_claim_route_specific_drain_observability() {
        let response = in_flight_invocations_response();
        assert!(response.existing_invocations_retain_captured_engine);
        assert_eq!(response.aggregate_isolate_workers, None);
        assert!(!response.static_hermes_route_specific);
        assert!(!response.drain_wait_supported);
    }

    #[test]
    fn updates_require_deploy_permission() {
        assert!(Identity::system()
            .require_operation(DeploymentOp::Deploy)
            .is_ok());
        assert!(Identity::Unknown(None)
            .require_operation(DeploymentOp::Deploy)
            .is_err());
    }

    #[tokio::test]
    async fn endpoint_errors_have_stable_statuses_and_opaque_persistence_failures() {
        for (error, expected_code) in [
            (
                isolate::StaticHermesWasmtimeQuarantineMutationError::InvalidSelector(
                    anyhow::anyhow!("private selector detail"),
                ),
                "InvalidWasmQuarantineSelector",
            ),
            (
                isolate::StaticHermesWasmtimeQuarantineMutationError::InvalidEvidence(
                    anyhow::anyhow!("private evidence detail"),
                ),
                "InvalidWasmQuarantineEvidence",
            ),
        ] {
            let response = map_quarantine_mutation_error(error).into_response();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["code"],
                expected_code
            );
        }
        for (error, expected_code) in [
            (
                isolate::StaticHermesWasmtimeQuarantineMutationError::InitializationRequired,
                "WasmQuarantineInitializationRequired",
            ),
            (
                isolate::StaticHermesWasmtimeQuarantineMutationError::DurablePolicyRequired,
                "WasmQuarantineDurablePolicyRequired",
            ),
            (
                isolate::StaticHermesWasmtimeQuarantineMutationError::PolicyCapacityReached,
                "WasmQuarantinePolicyCapacityReached",
            ),
        ] {
            let response = map_quarantine_mutation_error(error).into_response();
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["code"],
                expected_code
            );
        }

        let response = map_quarantine_mutation_error(
            isolate::StaticHermesWasmtimeQuarantineMutationError::Persistence(anyhow::anyhow!(
                "private persistence path and detail"
            )),
        )
        .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "code": "InternalServerError",
                "message": "Your request couldn't be completed. Try again later.",
            })
        );
        assert!(!String::from_utf8_lossy(&body).contains("private persistence"));
    }
}
