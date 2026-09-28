use application::deploy_config::SourceKeyedRuntimeExpectedPrior;
use axum::response::IntoResponse;
use common::{
    http::{
        extract::{
            Json,
            MtState,
        },
        HttpResponseError,
    },
    wasm_runtime_policy::{
        runtime_policy_store,
        PrimaryRuntime,
        RuntimePolicy,
        RuntimePolicySnapshot,
        RuntimePolicyUpdate,
        RUNTIME_PUBLICATION_VALIDATION_TIMEOUT,
    },
};
use errors::ErrorMetadata;
use keybroker::DeploymentOp;
use model::source_packages::SourcePackageModel;
use roles::RequireDeploymentOp;
use runtime::prod::ProdRuntime;
use serde::{
    Deserialize,
    Serialize,
};
use utoipa::ToSchema;
use value::TableNamespace;

use crate::{
    authentication::ExtractHeaderOnlyDeployKeyIdentity,
    source_keyed_runtime_readiness::SourceKeyedRuntimeGroupPopulation,
    LocalAppState,
};

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RuntimePolicyResponse {
    snapshot: RuntimePolicySnapshot,
    durable: bool,
    /// Quarantines still override this policy. A mode update never clears them.
    quarantined_selectors: usize,
    /// An uninitialized or ambiguous quarantine store blocks all Wasm routes.
    quarantine_initialized: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    population: Vec<SourceKeyedRuntimeGroupPopulation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    preparation_elapsed_ms: Option<f64>,
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateRuntimePolicyRequest {
    expected_revision: u64,
    policy: RuntimePolicy,
}

/// Read the live primary engine and opposite-engine shadow sampling policy.
#[utoipa::path(
    get,
    path = "/wasm_runtime_policy",
    tag = "Static Hermes Wasmtime",
    responses(
        (status = 200, body = RuntimePolicyResponse),
        (status = 400, description = "Invalid deployment key header"),
        (status = 401, description = "Deployment key authentication required"),
        (status = 403, description = "Deploy permission required"),
    ),
    security(("Deploy Key" = [])),
)]
pub async fn get_runtime_policy(
    ExtractHeaderOnlyDeployKeyIdentity(identity): ExtractHeaderOnlyDeployKeyIdentity,
) -> Result<impl IntoResponse, HttpResponseError> {
    identity.require_operation(DeploymentOp::Deploy)?;
    let store = runtime_policy_store();
    let quarantine = isolate::static_hermes_wasmtime_quarantine().snapshot();
    Ok(Json(RuntimePolicyResponse {
        snapshot: store.snapshot(),
        durable: store.has_durable_storage(),
        quarantined_selectors: quarantine.entries().len(),
        quarantine_initialized: !quarantine.is_ambiguous(),
        population: Vec::new(),
        preparation_elapsed_ms: None,
    }))
}

/// Persist and select the runtime policy for newly dispatched invocations.
///
/// Existing invocations keep their captured engine. Artifact admission and
/// route quarantines remain authoritative; selecting Wasm does not create a
/// pair or promote a shadow-only generation. Zero sampling selects primary
/// only.
#[utoipa::path(
    post,
    path = "/wasm_runtime_policy",
    tag = "Static Hermes Wasmtime",
    request_body = UpdateRuntimePolicyRequest,
    responses(
        (status = 200, body = RuntimePolicyResponse),
        (status = 400, description = "Invalid runtime policy or deployment key header"),
        (status = 401, description = "Deployment key authentication required"),
        (status = 403, description = "Deploy permission required"),
        (status = 409, description = "Stale source or policy, unavailable staging or baseline capacity, changed initialization dependencies, validation timeout, or unavailable durable storage"),
        (status = 500, description = "Update failed; read status before another update"),
    ),
    security(("Deploy Key" = [])),
)]
pub async fn update_runtime_policy(
    MtState(st): MtState<LocalAppState>,
    ExtractHeaderOnlyDeployKeyIdentity(identity): ExtractHeaderOnlyDeployKeyIdentity,
    Json(request): Json<UpdateRuntimePolicyRequest>,
) -> Result<impl IntoResponse, HttpResponseError> {
    identity.require_operation(DeploymentOp::Deploy)?;
    request.policy.validate().map_err(|error| {
        HttpResponseError::from(error.context(ErrorMetadata::bad_request(
            "InvalidWasmRuntimePolicy",
            "Shadow sampling must be between 0 and 10000 basis points",
        )))
    })?;
    let store = runtime_policy_store().clone();
    if !store.has_durable_storage() {
        return Err(anyhow::anyhow!(ErrorMetadata::conflict(
            "WasmRuntimePolicyStorageRequired",
            "Live runtime selection requires durable policy storage",
        ))
        .into());
    }
    let current = store.snapshot();
    let conflict = || {
        anyhow::anyhow!(ErrorMetadata::conflict(
            "WasmRuntimePolicyChanged",
            "Read the current runtime policy before updating it",
        ))
    };
    if current.revision != request.expected_revision {
        return Err(conflict().into());
    }
    let promotion = (current.policy.query.primary == PrimaryRuntime::V8
        && request.policy.query.primary == PrimaryRuntime::Wasm)
        || (current.policy.mutation.primary == PrimaryRuntime::V8
            && request.policy.mutation.primary == PrimaryRuntime::Wasm);
    let preparation_started = std::time::Instant::now();
    let expected_source = if promotion {
        let mut tx = st.application.begin(identity.clone()).await?;
        SourcePackageModel::new(&mut tx, TableNamespace::Global)
            .get_latest_record()
            .await?
            .map(|source| SourceKeyedRuntimeExpectedPrior {
                source_package_id: source.developer_id(),
                source_package_sha256: source.sha256.clone(),
                source_package_runtime_content_sha256: source.runtime_content_sha256.clone(),
                runtime_generation: source.runtime_generation.clone(),
            })
    } else {
        None
    };
    if let Some(source) = &expected_source
        && source.runtime_generation.is_some()
        && source.source_package_runtime_content_sha256.is_none()
    {
        return Err(anyhow::anyhow!(ErrorMetadata::conflict(
            "SourceKeyedRuntimeContentIdentityMissing",
            "The selected runtime generation has no source content identity"
        ))
        .into());
    }
    let preparation = if let Some(source) = expected_source.as_ref()
        && let (Some(content), Some(generation)) = (
            &source.source_package_runtime_content_sha256,
            &source.runtime_generation,
        ) {
        st.application
            .runner()
            .prepare_source_keyed_runtime_for_policy(
                &content.as_hex(),
                &isolate::SourceKeyedRuntimeGenerationIdentity {
                    deployment_sha256: generation.deployment_sha256.as_hex(),
                    generation_manifest_sha256: generation.generation_manifest_sha256.as_hex(),
                    generation_sha256: generation.generation_sha256.as_hex(),
                },
                RuntimePolicySnapshot {
                    revision: request.expected_revision,
                    policy: request.policy,
                },
            )
            .await?
    } else {
        None
    };
    let preparation = match preparation {
        Some(isolate::SourceKeyedRuntimePreparation::Prepared(prepared)) => Some(prepared),
        Some(isolate::SourceKeyedRuntimePreparation::CapacityUnavailable {
            capacity,
            population,
        }) => {
            return Err(anyhow::anyhow!(ErrorMetadata::conflict(
                "StaticHermesWasmBaselineCapacityUnavailable",
                format!(
                    "Wasm baseline preparation capacity is unavailable: {}; {} of {} discovered \
                     groups have initialized capacity",
                    capacity.reason,
                    population.iter().filter(|group| group.prepared > 0).count(),
                    population.len(),
                )
            ))
            .into());
        },
        Some(isolate::SourceKeyedRuntimePreparation::NotStaged) => {
            return Err(anyhow::anyhow!(ErrorMetadata::conflict(
                "StaticHermesWasmGenerationNotStaged",
                "The selected source/runtime pair is not staged"
            ))
            .into());
        },
        None => None,
    };
    let preparation_elapsed_ms =
        promotion.then(|| preparation_started.elapsed().as_secs_f64() * 1000.0);
    // Only publication owns this gate. A concurrent demotion can complete while
    // promotion warms, and its revision makes the older intent fail here.
    let publication = store.lock_publication().await;
    if store.snapshot().revision != request.expected_revision {
        return Err(conflict().into());
    }
    let reservation = tokio::time::timeout(RUNTIME_PUBLICATION_VALIDATION_TIMEOUT, async {
        let mut tx = if promotion {
            Some(st.application.begin(identity).await?)
        } else {
            None
        };
        if let Some(tx) = &mut tx {
            let actual = SourcePackageModel::new(tx, TableNamespace::Global)
                .get_latest_record()
                .await?
                .map(|source| SourceKeyedRuntimeExpectedPrior {
                    source_package_id: source.developer_id(),
                    source_package_sha256: source.sha256.clone(),
                    source_package_runtime_content_sha256: source.runtime_content_sha256.clone(),
                    runtime_generation: source.runtime_generation.clone(),
                });
            if actual != expected_source {
                return Err(anyhow::anyhow!(ErrorMetadata::conflict(
                    "SourceKeyedRuntimeExpectedPriorMismatch",
                    "The committed source/runtime state changed during Wasm preparation"
                )));
            }
        }
        let reservation = preparation
            .as_ref()
            .map(|prepared| prepared.reserve::<ProdRuntime>())
            .transpose()?;
        if let Some(reservation) = &reservation {
            reservation
                .validate_dependencies(tx.as_mut().expect("promotion lost its source transaction"))
                .await?;
        }
        Ok::<_, anyhow::Error>(reservation)
    })
    .await
    .map_err(|_| anyhow::anyhow!(ErrorMetadata::conflict(
        "StaticHermesWasmPublicationValidationTimeout",
        "Wasm source and initialization validation timed out before publication",
    )))??;
    let population = reservation
        .as_ref()
        .map(|reservation| {
            reservation
                .population()
                .iter()
                .cloned()
                .map(Into::into)
                .collect()
        })
        .unwrap_or_default();
    // The owned filesystem task retains inventory and the publication gate even
    // if its HTTP waiter disconnects. No warmup holds the policy writer lock.
    let update = tokio::task::spawn_blocking(move || {
        let _publication = publication;
        let _reservation = reservation;
        store.update(request.expected_revision, request.policy)
    })
    .await
    .map_err(anyhow::Error::from)?
    .map_err(|error| {
        HttpResponseError::from(error.context(ErrorMetadata::operational_internal_server_error()))
    })?;
    let snapshot = match update {
        RuntimePolicyUpdate::Applied(snapshot) => snapshot,
        RuntimePolicyUpdate::Conflict => {
            return Err(anyhow::anyhow!(ErrorMetadata::conflict(
                "WasmRuntimePolicyChanged",
                "Read the current runtime policy before updating it",
            ))
            .into());
        },
    };
    let quarantine = isolate::static_hermes_wasmtime_quarantine().snapshot();
    Ok(Json(RuntimePolicyResponse {
        snapshot,
        durable: true,
        quarantined_selectors: quarantine.entries().len(),
        quarantine_initialized: !quarantine.is_ambiguous(),
        population,
        preparation_elapsed_ms,
    }))
}
