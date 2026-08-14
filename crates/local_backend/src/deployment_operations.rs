//! Process-local ownership for opt-in deployment analysis. A subscriber may
//! disconnect without cancelling work; cancellation is an authenticated call.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{
        Duration,
        SystemTime,
        UNIX_EPOCH,
    },
};

use anyhow::Context;
use application::deploy_config::{
    PreparedPush,
    StartPushRequest,
};
use axum::{
    body::Bytes,
    extract::State,
    response::{
        IntoResponse,
        Response,
    },
};
use common::{
    http::{
        extract::Json,
        HttpResponseError,
    },
    sha256::{
        Sha256,
        Sha256Digest,
    },
};
use errors::{
    ErrorMetadata,
    ErrorMetadataAnyhowExt,
};
use parking_lot::Mutex;
use roles::RequireDeploymentOp;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    json,
    Value,
};
use tokio::sync::watch;

use crate::{
    deploy_config2::{
        SerializedEvaluatePushResponse,
        SerializedStartPushResponse,
    },
    LocalAppState,
};

const LIFETIME: Duration = Duration::from_secs(600);
const MAX_OPERATIONS: usize = 8;
const MAX_RETAINED_BYTES: usize = 384 * 1024 * 1024;
const MAX_RESULT_BYTES: usize = 32 * 1024 * 1024;

metrics::register_convex_counter!(
    DEPLOYMENT_OPERATION_RETENTION_REJECTIONS_TOTAL,
    "New deployment operations refused by the retained operation count or byte budget"
);

pub fn http_admission(st: LocalAppState) -> common::http::DeploymentHttpAdmission {
    common::http::DeploymentHttpAdmission {
        analysis_paths: &[
            // Deployment clients read the current configuration before they
            // submit analysis. That prerequisite must survive ordinary shedding too.
            "/api/get_config",
            "/api/get_config_hashes",
            "/api/deploy2/prepare_external_deps",
            "/api/deploy2/download_external_deps",
            "/api/deploy2/start_push",
            "/api/deploy2/evaluate_push",
            "/api/deploy2/evaluate_schema",
            "/api/push_config",
            "/api/prepare_schema",
            "/api/deploy2/operations/submit",
        ],
        completion_paths: &["/api/deploy2/wait_for_schema", "/api/deploy2/finish_push"],
        status_paths: &[
            "/api/deploy2/operations/status",
            "/api/deploy2/operations/cancel",
            "/api/deploy2/operations/capabilities",
        ],
        authenticate: Arc::new(move |headers| {
            let st = st.clone();
            Box::pin(async move {
                let Some(key) = headers
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.strip_prefix("Convex "))
                else {
                    anyhow::bail!(ErrorMetadata::unauthenticated(
                        "InvalidAdminKey",
                        "Expected a Convex authorization header"
                    ));
                };
                authorize(&st, key.to_owned()).await
            })
        }),
    }
}

#[derive(Clone)]
pub(crate) struct DeploymentOperations {
    operations: Arc<Mutex<BTreeMap<String, Operation>>>,
    clock: Arc<Mutex<SystemTime>>,
    session_id: String,
}

impl Default for DeploymentOperations {
    fn default() -> Self {
        Self {
            operations: Arc::default(),
            clock: Arc::new(Mutex::new(SystemTime::now())),
            session_id: common::execution_context::ExecutionId::new().to_string(),
        }
    }
}

struct Operation {
    digest: Sha256Digest,
    kind: OperationKind,
    expires: SystemTime,
    deadline: tokio::time::Instant,
    reserved_bytes: usize,
    response: Bytes,
    cancellation: Option<watch::Sender<bool>>,
    prepared: Option<PreparedOperation>,
    finish: FinishState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FinishState {
    Idle,
    Active,
}

struct PreparedOperation {
    sources: Arc<PreparedPush>,
    response_sha256: Sha256Digest,
}

pub(crate) struct PreparedPushLease {
    // Fields drop in declaration order: release this source reference before
    // returning its registry reservation, including during handler unwinding.
    pub(crate) sources: Arc<PreparedPush>,
    _reservation: PreparedPushReservation,
}

struct PreparedPushReservation {
    owner: DeploymentOperations,
    operation_id: String,
}

impl Drop for PreparedPushReservation {
    fn drop(&mut self) {
        let mut operations = self.owner.operations.lock();
        let operation = operations
            .get_mut(&self.operation_id)
            .expect("active finish lost its source reservation");
        assert_eq!(operation.finish, FinishState::Active);
        operation.finish = FinishState::Idle;
        if operation.prepared.is_none() {
            operation.reserved_bytes = operation.response.len();
        }
        self.owner.prune(&mut operations, self.owner.now());
    }
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    overflowed: bool,
}

impl std::io::Write for BoundedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_RESULT_BYTES.saturating_sub(self.bytes.len()) {
            self.overflowed = true;
            return Err(std::io::ErrorKind::FileTooLarge.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// None is the expected size-limit outcome. Serialization errors unrelated to
// the writer limit are internal failures: all values here are already JSON.
fn encode_result(value: &Value) -> Option<Bytes> {
    let mut writer = BoundedJsonWriter {
        bytes: Vec::new(),
        overflowed: false,
    };
    let result = serde_json::to_writer(&mut writer, value);
    if writer.overflowed {
        return None;
    }
    result.expect("JSON result serialization failed");
    Some(Bytes::from(writer.bytes))
}

pub(crate) fn normalized_digest(value: &mut Value) -> anyhow::Result<Sha256Digest> {
    value.sort_all_objects();
    // Hash directly: an oversized analysis response must not allocate an
    // unbounded encoded copy before the bounded response writer sees it.
    let mut hash = Sha256::new();
    serde_json::to_writer(&mut hash, value)?;
    Ok(hash.finalize())
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OperationKind {
    StartPush,
    EvaluatePush,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubmitOperation {
    admin_key: String,
    session_id: String,
    operation_id: String,
    kind: OperationKind,
    config: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OperationControl {
    admin_key: String,
    operation_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilitiesRequest {
    admin_key: String,
}

pub async fn capabilities(
    State(st): State<LocalAppState>,
    Json(req): Json<CapabilitiesRequest>,
) -> Result<Response, HttpResponseError> {
    authorize(&st, req.admin_key).await?;
    Ok(axum::Json(json!({
        "protocolVersion": 1,
        "sessionId": st.deployment_operations.session_id,
        "lifetimeSeconds": LIFETIME.as_secs(),
        "maxOperations": MAX_OPERATIONS,
        "maxRetainedBytes": MAX_RETAINED_BYTES,
        "maxResultBytes": MAX_RESULT_BYTES,
    }))
    .into_response())
}

// The creation time is part of identity. Removing an expired entry cannot let
// a late retry silently create new work with the same operation ID.
pub(crate) fn operation_expiry(id: &str, now: SystemTime) -> anyhow::Result<SystemTime> {
    anyhow::ensure!(
        id.len() <= 64,
        ErrorMetadata::bad_request("InvalidDeploymentOperation", "Operation ID is too long")
    );
    let (created, nonce) = id.split_once(':').context(ErrorMetadata::bad_request(
        "InvalidDeploymentOperation",
        "Expected seconds-since-epoch:32-lowercase-hex operation ID",
    ))?;
    anyhow::ensure!(
        nonce.len() == 32
            && nonce
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        ErrorMetadata::bad_request("InvalidDeploymentOperation", "Invalid operation nonce")
    );
    let created: u64 = created.parse().map_err(|_| {
        ErrorMetadata::bad_request(
            "InvalidDeploymentOperation",
            "Invalid operation creation time",
        )
    })?;
    let created = UNIX_EPOCH
        .checked_add(Duration::from_secs(created))
        .context(ErrorMetadata::bad_request(
            "InvalidDeploymentOperation",
            "Invalid operation creation time",
        ))?;
    anyhow::ensure!(
        created <= now + Duration::from_secs(5),
        ErrorMetadata::bad_request(
            "InvalidDeploymentOperation",
            "Operation creation time is in the future"
        )
    );
    let expires = created + LIFETIME;
    anyhow::ensure!(
        now < expires,
        ErrorMetadata::bad_request(
            "DeploymentOperationExpired",
            "This operation ID has expired. Inspect deployment state before starting a new \
             operation."
        )
    );
    Ok(expires)
}

async fn authorize(st: &LocalAppState, key: String) -> anyhow::Result<()> {
    let identity = crate::admin::must_be_admin_from_key(
        st.application.app_auth(),
        st.instance_name.clone(),
        key,
    )
    .await?;
    identity.require_operation(keybroker::DeploymentOp::Deploy)?;
    Ok(())
}

fn response(bytes: Bytes) -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        bytes,
    )
        .into_response()
}

impl DeploymentOperations {
    pub(crate) fn now(&self) -> SystemTime {
        // Remember expired identities across clock rollback, but let the wall
        // clock catch up instead of preserving a permanent offset from clients.
        let mut clock = self.clock.lock();
        *clock = (*clock).max(SystemTime::now());
        *clock
    }

    pub(crate) fn prepared(
        &self,
        id: &str,
        mut response: Value,
    ) -> anyhow::Result<Option<PreparedPushLease>> {
        let digest = normalized_digest(&mut response)?;
        let mut operations = self.operations.lock();
        self.prune(&mut operations, self.now());
        let Some(operation) = operations.get_mut(id) else {
            return Ok(None);
        };
        // Receipt lookup alone cannot serialize two finishes that both arrive
        // before commit. Only this lease may enter force-capable Node cutover
        // admission; a retry must wait for its owner to release it.
        anyhow::ensure!(
            operation.finish == FinishState::Idle,
            ErrorMetadata::overloaded(
                "DeploymentOperationFinishInProgress",
                "A finish for this operation is still in progress. Retry the same finish after it \
                 completes."
            )
        );
        // A cancelled or evicted preparation can still have a durable finish
        // receipt. Application finish checks that receipt before requiring it.
        let Some(prepared) = operation.prepared.as_ref() else {
            return Ok(None);
        };
        anyhow::ensure!(
            prepared.response_sha256 == digest,
            ErrorMetadata::bad_request(
                "DeploymentOperationInputMismatch",
                "Finish input differs from the server-owned preparation result"
            )
        );
        let sources = prepared.sources.clone();
        operation.finish = FinishState::Active;
        Ok(Some(PreparedPushLease {
            sources,
            _reservation: PreparedPushReservation {
                owner: self.clone(),
                operation_id: id.to_owned(),
            },
        }))
    }

    fn prune(&self, operations: &mut BTreeMap<String, Operation>, now: SystemTime) {
        // A monotonic deadline can elapse while wall time is behind. Advance
        // the remembered time before pruning so a removed ID stays unusable.
        let monotonic = tokio::time::Instant::now();
        let now = operations
            .values()
            .filter(|operation| monotonic >= operation.deadline)
            .fold(now, |now, operation| now.max(operation.expires));
        let now = {
            let mut clock = self.clock.lock();
            *clock = (*clock).max(now);
            *clock
        };
        operations.retain(|_, operation| {
            if now < operation.expires {
                return true;
            }
            if let Some(cancel) = &operation.cancellation {
                cancel.send_replace(true);
            }
            // Expiry revokes new finish authority. An already admitted finish
            // keeps these bytes charged until its handler drops its lease.
            operation.prepared = None;
            // An active owner releases its input reservation only after the
            // cancelled future has dropped. A status call cannot release it.
            operation.cancellation.is_some() || operation.finish == FinishState::Active
        });
    }

    fn cancel(&self, id: &str) -> anyhow::Result<Bytes> {
        let mut operations = self.operations.lock();
        self.prune(&mut operations, self.now());
        operation_expiry(id, self.now())?;
        let operation = operations.get_mut(id).context(ErrorMetadata::bad_request(
            "DeploymentOperationUnknown",
            "No retained operation exists in this process",
        ))?;
        if let Some(cancel) = &operation.cancellation {
            cancel.send_replace(true);
        } else if operation.prepared.take().is_some() {
            operation.response = encode_result(
                &json!({"operationId":id,"state":"failed","code":"DeploymentOperationCancelled"}),
            )
            .expect("fixed cancellation exceeds result limit");
            if operation.finish == FinishState::Idle {
                operation.reserved_bytes = operation.response.len();
            }
        }
        Ok(operation.response.clone())
    }
}

pub async fn submit(
    State(st): State<LocalAppState>,
    Json(mut req): Json<SubmitOperation>,
) -> Result<Response, HttpResponseError> {
    authorize(&st, req.admin_key).await?;
    if req.session_id != st.deployment_operations.session_id {
        return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
            "DeploymentOperationSessionChanged",
            "The backend restarted. Inspect deployment state and obtain current capabilities \
             before submitting a new operation."
        ))
        .into());
    }
    let now = st.deployment_operations.now();
    let expires = operation_expiry(&req.operation_id, now)?;
    let deadline = tokio::time::Instant::now()
        + expires
            .duration_since(now)
            .context("deployment operation expiry precedes submission time")?;
    let config = req
        .config
        .as_object_mut()
        .context(ErrorMetadata::bad_request(
            "InvalidConfig",
            "Expected a configuration object",
        ))?;
    if config.contains_key("adminKey") {
        return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
            "InvalidConfig",
            "Supply adminKey only in the operation envelope"
        ))
        .into());
    }
    // Normalize object order and whitespace before binding identity.
    req.config.sort_all_objects();
    let encoded = serde_json::to_vec(&req.config).context("serializing deployment input")?;
    let digest = Sha256::hash(&encoded);
    let reconstructed_bytes = if req
        .config
        .get("appDefinition")
        .and_then(|definition| definition.get("unchangedModuleHashes"))
        .and_then(Value::as_array)
        .is_some_and(|hashes| !hashes.is_empty())
    {
        model::source_packages::types::MAX_UNZIPPED_PACKAGES_SIZE
    } else {
        0
    };
    let reserved_bytes = encoded
        .len()
        .checked_mul(2)
        .and_then(|n| n.checked_add(MAX_RESULT_BYTES))
        .and_then(|n| n.checked_add(reconstructed_bytes))
        .context("operation size overflow")?;
    drop(encoded);
    let (cancel, mut cancelled) = watch::channel(false);
    let initial = Bytes::from(
        serde_json::to_vec(&json!({"operationId":req.operation_id,"state":"running"}))
            .context("serializing operation status")?,
    );
    {
        let mut operations = st.deployment_operations.operations.lock();
        let now = st.deployment_operations.now();
        st.deployment_operations.prune(&mut operations, now);
        operation_expiry(&req.operation_id, st.deployment_operations.now())?;
        if let Some(previous) = operations.get(&req.operation_id) {
            if previous.digest != digest || previous.kind != req.kind {
                return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
                    "DeploymentOperationInputMismatch",
                    "Operation ID was already used with different input"
                ))
                .into());
            }
            return Ok(response(previous.response.clone()));
        }
        let retained: usize = operations.values().map(|op| op.reserved_bytes).sum();
        if operations.len() >= MAX_OPERATIONS
            || reserved_bytes > MAX_RETAINED_BYTES.saturating_sub(retained)
        {
            DEPLOYMENT_OPERATION_RETENTION_REJECTIONS_TOTAL.inc();
            return Err(anyhow::anyhow!(ErrorMetadata::overloaded(
                "DeploymentOperationsFull",
                "Deployment operation retention is full. Retry after an existing operation \
                 completes or expires."
            ))
            .into());
        }
        operations.insert(
            req.operation_id.clone(),
            Operation {
                digest,
                kind: req.kind,
                expires,
                deadline,
                reserved_bytes,
                response: initial.clone(),
                cancellation: Some(cancel),
                prepared: None,
                finish: FinishState::Idle,
            },
        );
    }
    // Retain a server-owned task, not the HTTP subscriber future. The join
    // boundary handles unwinding work panics; aborting builds still restart.
    let owner = st.deployment_operations.clone();
    tokio::spawn(async move {
        let operation_id = req.operation_id;
        let task = tokio::spawn(async move {
            let work = async {
                req.config
                    .as_object_mut()
                    .context("validated configuration lost object shape")?
                    .insert("adminKey".to_owned(), Value::String(String::new()));
                let request: StartPushRequest = serde_json::from_value(req.config).context(
                    ErrorMetadata::bad_request("InvalidConfig", "Invalid deployment configuration"),
                )?;
                let config = request.into_project_config()?;
                match req.kind {
                    OperationKind::StartPush => {
                        let result = st
                            .application
                            .start_push_with_prepared_sources(&config)
                            .await?;
                        let sources = result
                            .prepared
                            .context("owned push omitted prepared sources")?;
                        let mut response = serde_json::to_value(
                            SerializedStartPushResponse::try_from(result.response)?,
                        )?;
                        let response_sha256 = normalized_digest(&mut response)?;
                        anyhow::Ok((
                            response,
                            Some(PreparedOperation {
                                sources,
                                response_sha256,
                            }),
                        ))
                    },
                    OperationKind::EvaluatePush => {
                        serde_json::to_value(SerializedEvaluatePushResponse::try_from(
                            st.application.evaluate_push(&config).await?,
                        )?)
                        .map(|response| (response, None))
                        .map_err(Into::into)
                    },
                }
            };
            let work = common::query_analysis_admission::DEPLOYMENT_OPERATION_CANCELLATION
                .scope(cancelled.clone(), work);
            let result: anyhow::Result<(Value, Option<PreparedOperation>)> = tokio::select! {
                // A cancellation received before first polling must prevent
                // configuration work from starting, including preparation writes.
                biased;
                changed = cancelled.changed() => {
                    changed.expect("active deployment owner lost its cancellation sender");
                    Err(ErrorMetadata::bad_request("DeploymentOperationCancelled", "Deployment analysis was cancelled").into())
                },
                _ = tokio::time::sleep_until(deadline) => Err(ErrorMetadata::bad_request("DeploymentOperationExpired", "Deployment analysis exceeded its operation lifetime").into()),
                result = work => result,
            };
            result
        });
        let (result, prepared) = match task.await {
            Ok(Ok((result, prepared))) => (
                json!({"operationId": operation_id, "state":"completed", "result":result}),
                prepared,
            ),
            Ok(Err(error)) => (
                json!({"operationId":operation_id,"state":"failed","code":error.short_msg(),"message":format!("{error:#}")}),
                None,
            ),
            Err(_) => (
                json!({"operationId":operation_id,"state":"failed","code":"DeploymentOperationInterrupted"}),
                None,
            ),
        };
        // JSON values can always be serialized. Bound the encoded result before
        // publishing it; the input reservation includes this output allowance.
        let encoded = encode_result(&result);
        drop(result);
        owner.publish(&operation_id, encoded, prepared);
        // Completed responses and prepared sources also have a finite lifetime
        // when no later HTTP request arrives to perform lazy pruning.
        tokio::time::sleep_until(deadline).await;
        owner.prune(&mut owner.operations.lock(), owner.now());
    });
    Ok(response(initial))
}

impl DeploymentOperations {
    fn publish(
        &self,
        operation_id: &str,
        mut encoded: Option<Bytes>,
        mut prepared: Option<PreparedOperation>,
    ) {
        let mut operations = self.operations.lock();
        let operation = operations
            .get_mut(operation_id)
            .expect("active deployment owner lost its reservation");
        // Work can finish before its timer is polled, and serialization can
        // cross the deadline. Neither may publish expired finish authority.
        if self.now() >= operation.expires || tokio::time::Instant::now() >= operation.deadline {
            let mut clock = self.clock.lock();
            *clock = (*clock).max(operation.expires);
            operations.remove(operation_id);
            return;
        }
        // Cancellation may race with successful preparation and publication.
        // Keep the terminal request even if the work receiver already exited.
        if operation
            .cancellation
            .as_ref()
            .is_some_and(|cancel| *cancel.borrow())
        {
            prepared = None;
            encoded = encode_result(
                &json!({"operationId":operation_id,"state":"failed","code":"DeploymentOperationCancelled"}),
            );
        }
        let prepared_bytes = prepared
            .as_ref()
            .map_or(0, |prepared| prepared.sources.retained_bytes());
        if encoded
            .as_ref()
            .is_none_or(|encoded| encoded.len() + prepared_bytes > operation.reserved_bytes)
        {
            prepared = None;
            encoded = encode_result(
                &json!({"operationId":operation_id,"state":"failed","code":"DeploymentOperationResultTooLarge"}),
            );
        }
        let encoded = encoded.expect("fixed operation failure exceeds result limit");
        operation.reserved_bytes = encoded.len()
            + prepared
                .as_ref()
                .map_or(0, |prepared| prepared.sources.retained_bytes());
        operation.response = encoded;
        operation.prepared = prepared;
        operation.cancellation = None;
    }
}

pub async fn status(
    State(st): State<LocalAppState>,
    Json(req): Json<OperationControl>,
) -> Result<Response, HttpResponseError> {
    authorize(&st, req.admin_key).await?;
    let mut operations = st.deployment_operations.operations.lock();
    let now = st.deployment_operations.now();
    st.deployment_operations.prune(&mut operations, now);
    operation_expiry(&req.operation_id, st.deployment_operations.now())?;
    let operation = operations
        .get(&req.operation_id)
        .context(ErrorMetadata::bad_request(
            "DeploymentOperationUnknown",
            "No retained operation exists in this process. Analysis is not resumed after restart.",
        ))?;
    Ok(response(operation.response.clone()))
}

pub async fn cancel(
    State(st): State<LocalAppState>,
    Json(req): Json<OperationControl>,
) -> Result<Response, HttpResponseError> {
    authorize(&st, req.admin_key).await?;
    Ok(response(
        st.deployment_operations.cancel(&req.operation_id)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_stays_final_after_clock_corrections() {
        let owner = DeploymentOperations::default();
        let wall = owner.now();
        let id = format!(
            "{}:0123456789abcdef0123456789abcdef",
            wall.duration_since(UNIX_EPOCH).unwrap().as_secs()
        );
        let expired = wall + LIFETIME;
        *owner.clock.lock() = expired;
        assert!(operation_expiry(&id, owner.now()).is_err());
        // Wall time is behind the remembered expiry. Repeated observations
        // keep a fixed floor, allowing that clock to catch up later.
        assert_eq!(owner.now(), expired);
        assert_eq!(owner.now(), expired);
    }

    #[test]
    fn terminal_publication_observes_late_cancel_and_expiry() {
        for expired in [false, true] {
            let owner = DeploymentOperations::default();
            let (cancel, receiver) = watch::channel(false);
            // The work receiver can disappear before its owner publishes.
            drop(receiver);
            cancel.send_replace(true);
            owner.operations.lock().insert(
                "operation".into(),
                Operation {
                    digest: Sha256::hash(b"config"),
                    kind: OperationKind::StartPush,
                    expires: if expired {
                        UNIX_EPOCH
                    } else {
                        owner.now() + LIFETIME
                    },
                    deadline: tokio::time::Instant::now() + LIFETIME,
                    reserved_bytes: MAX_RESULT_BYTES,
                    response: Bytes::new(),
                    cancellation: Some(cancel),
                    prepared: None,
                    finish: FinishState::Idle,
                },
            );
            owner.publish(
                "operation",
                encode_result(&json!({"state":"completed"})),
                None,
            );
            let operations = owner.operations.lock();
            if expired {
                assert!(operations.is_empty());
            } else {
                let operation = &operations["operation"];
                let status: Value = serde_json::from_slice(&operation.response).unwrap();
                assert_eq!(status["code"], "DeploymentOperationCancelled");
                assert!(operation.cancellation.is_none());
                assert_eq!(operation.reserved_bytes, operation.response.len());
            }
        }
    }

    #[test]
    fn finish_lease_excludes_duplicates_and_retains_revoked_sources_until_release() {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Revocation {
            None,
            Cancel,
            Expire,
        }
        for revoke in [Revocation::None, Revocation::Cancel, Revocation::Expire] {
            let owner = DeploymentOperations::default();
            let now = owner.now();
            let id = format!(
                "{}:0123456789abcdef0123456789abcdef",
                now.duration_since(UNIX_EPOCH).unwrap().as_secs()
            );
            let mut response = json!({"prepared":true});
            let sources = Arc::new(PreparedPush::default());
            let weak_sources = Arc::downgrade(&sources);
            owner.operations.lock().insert(
                id.clone(),
                Operation {
                    digest: Sha256::hash(b"config"),
                    kind: OperationKind::StartPush,
                    expires: operation_expiry(&id, now).unwrap(),
                    deadline: tokio::time::Instant::now() + LIFETIME,
                    reserved_bytes: 1234,
                    response: encode_result(&response).unwrap(),
                    cancellation: None,
                    prepared: Some(PreparedOperation {
                        sources,
                        response_sha256: normalized_digest(&mut response).unwrap(),
                    }),
                    finish: FinishState::Idle,
                },
            );
            let first = owner.prepared(&id, response.clone()).unwrap().unwrap();
            assert_eq!(
                owner
                    .prepared(&id, response.clone())
                    .err()
                    .unwrap()
                    .short_msg(),
                "DeploymentOperationFinishInProgress"
            );
            match revoke {
                Revocation::None => {},
                Revocation::Cancel => {
                    owner.cancel(&id).unwrap();
                },
                Revocation::Expire => {
                    owner.operations.lock().get_mut(&id).unwrap().deadline =
                        tokio::time::Instant::now();
                },
            }
            owner.prune(&mut owner.operations.lock(), owner.now());
            assert_eq!(owner.operations.lock()[&id].reserved_bytes, 1234);
            assert_eq!(
                owner
                    .prepared(&id, response.clone())
                    .err()
                    .unwrap()
                    .short_msg(),
                "DeploymentOperationFinishInProgress"
            );
            assert!(weak_sources.upgrade().is_some());
            drop(first);
            let next = owner.prepared(&id, response).unwrap();
            if revoke != Revocation::None {
                assert!(next.is_none());
                assert!(weak_sources.upgrade().is_none());
                let operations = owner.operations.lock();
                if revoke == Revocation::Expire {
                    assert!(operations.is_empty());
                } else {
                    assert_eq!(
                        operations[&id].reserved_bytes,
                        operations[&id].response.len()
                    );
                }
            } else {
                assert!(next.is_some());
                drop(next);
                assert_eq!(owner.operations.lock()[&id].finish, FinishState::Idle);
            }
        }
    }

    #[test]
    fn result_serialization_stops_at_the_encoded_limit() {
        let small = json!({"result": "quoted\"and\nnewlined"});
        let encoded = encode_result(&small).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), small);
        // Escaping doubles these bytes. The encoded limit must apply while
        // writing, not only after a full second allocation has been made.
        let large = Value::String("\n".repeat(MAX_RESULT_BYTES / 2));
        assert!(encode_result(&large).is_none());
        let mut writer = BoundedJsonWriter {
            bytes: Vec::new(),
            overflowed: false,
        };
        assert!(serde_json::to_writer(&mut writer, &large).is_err());
        assert!(writer.bytes.len() <= MAX_RESULT_BYTES);
    }

    #[test]
    fn cloning_keeps_the_session_but_restart_changes_it() {
        let first = DeploymentOperations::default();
        assert_eq!(first.session_id, first.clone().session_id);
        assert_ne!(first.session_id, DeploymentOperations::default().session_id);
    }

    #[test]
    fn expired_identity_cannot_be_reused_after_retention_is_removed() {
        let now = UNIX_EPOCH + Duration::from_secs(1000);
        let id = "1000:0123456789abcdef0123456789abcdef";
        assert_eq!(operation_expiry(id, now).unwrap(), now + LIFETIME);
        assert!(operation_expiry(id, now + LIFETIME).is_err());
        assert!(operation_expiry(id, now - Duration::from_secs(6)).is_err());
        assert!(operation_expiry("1000:bad", now).is_err());
    }

    #[test]
    fn pruning_requests_cancellation_without_releasing_active_reservation() {
        let owner = DeploymentOperations::default();
        let now = owner.now();
        let (cancel, cancellation) = watch::channel(false);
        let mut operations = BTreeMap::from([(
            "operation".to_owned(),
            Operation {
                digest: Sha256::hash(b"config"),
                kind: OperationKind::StartPush,
                expires: now + LIFETIME,
                deadline: tokio::time::Instant::now(),
                reserved_bytes: 1234,
                response: Bytes::new(),
                cancellation: Some(cancel),
                prepared: None,
                finish: FinishState::Idle,
            },
        )]);
        owner.prune(&mut operations, now);
        assert!(*cancellation.borrow());
        assert_eq!(*owner.clock.lock(), now + LIFETIME);
        assert_eq!(operations["operation"].reserved_bytes, 1234);
        operations.get_mut("operation").unwrap().cancellation = None;
        owner.prune(&mut operations, now);
        assert!(operations.is_empty());
    }
}
