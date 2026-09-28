//! Exercise the real sync worker with exhausted errors injected at its
//! application boundary. Database conflict execution and result retention are
//! separate integration properties.
use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use anyhow::Context;
use application::{
    api::{
        ApplicationApi,
        ExecuteQueryTimestamp,
        SubscriptionClient,
        SubscriptionTrait,
    },
    FunctionError,
    FunctionReturn,
    RedactedActionError,
    RedactedActionReturn,
    RedactedMutationError,
    RedactedMutationReturn,
    RedactedQueryReturn,
};
use async_trait::async_trait;
use bytes::Bytes;
use common::{
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentId,
        ExportPath,
    },
    http::{
        RequestDestination,
        ResolvedHostname,
        ResolvedHostnameSource,
    },
    types::{
        ConvexOrigin,
        FunctionCaller,
        QueryInvocation,
        RepeatableTimestamp,
    },
    value::{
        sha256::Sha256Digest,
        DeveloperDocumentId,
    },
    RequestContext,
    RequestId,
};
use database::Token;
use file_storage::FileStream;
use futures::stream::BoxStream;
use headers::{
    ContentLength,
    ContentType,
};
use model::file_storage::FileStorageId;
use runtime::prod::ProdRuntime;
use sync_types::{
    types::SerializedArgs,
    QueryWorkloadClass,
};
use udf::{
    HttpActionRequest,
    HttpActionResponseStreamer,
};

use super::*;

#[derive(Default)]
struct MutationApi {
    calls: Mutex<BTreeMap<u32, bool>>,
}

impl MutationApi {
    fn execute(
        &self,
        identifier: Option<SessionRequestIdentifier>,
        admin: bool,
    ) -> anyhow::Result<Result<RedactedMutationReturn, RedactedMutationError>> {
        let identifier = identifier.expect("worker omitted mutation identity");
        assert_eq!(
            identifier.session_id,
            "00000000-0000-0000-0000-000000000000"
                .parse::<SessionId>()
                .unwrap()
        );
        assert!(
            self.calls
                .lock()
                .unwrap()
                .insert(identifier.request_id, admin)
                .is_none(),
            "worker resubmitted a terminal mutation"
        );
        match identifier.request_id {
            0 => Err(ErrorMetadata::system_occ(None, Some("private diagnostic".into())).into()),
            1 => Err(ErrorMetadata::user_occ(None, Some("private diagnostic".into()), None).into()),
            2 => Ok(Ok(RedactedMutationReturn {
                value: JsonPackedValue::pack(common::value::ConvexValue::Int64(7)),
                ts: Timestamp::MIN,
                log_lines: RedactedLogLines::empty(),
            })),
            _ => panic!("unexpected mutation identity"),
        }
    }
}

#[async_trait]
#[allow(unused_variables)]
impl ApplicationApi for MutationApi {
    async fn authenticate(
        &self,
        host: &ResolvedHostname,
        request_context: RequestContext,
        auth_token: AuthenticationToken,
    ) -> anyhow::Result<Identity> {
        Ok(Identity::system())
    }

    async fn execute_public_query(
        &self,
        host: &ResolvedHostname,
        request_context: RequestContext,
        identity: Identity,
        path: ExportPath,
        args: SerializedArgs,
        caller: FunctionCaller,
        query_workload_class: Option<QueryWorkloadClass>,
        ts: ExecuteQueryTimestamp,
        journal: Option<SerializedQueryJournal>,
        invocation: Option<QueryInvocation>,
    ) -> anyhow::Result<RedactedQueryReturn> {
        panic!("unexpected application call")
    }

    async fn execute_admin_query(
        &self,
        host: &ResolvedHostname,
        request_context: RequestContext,
        identity: Identity,
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        caller: FunctionCaller,
        query_workload_class: Option<QueryWorkloadClass>,
        ts: ExecuteQueryTimestamp,
        journal: Option<SerializedQueryJournal>,
        invocation: Option<QueryInvocation>,
    ) -> anyhow::Result<RedactedQueryReturn> {
        panic!("unexpected application call")
    }

    async fn execute_public_mutation(
        &self,
        host: &ResolvedHostname,
        request_context: RequestContext,
        identity: Identity,
        path: ExportPath,
        args: SerializedArgs,
        caller: FunctionCaller,
        mutation_identifier: Option<SessionRequestIdentifier>,
        mutation_queue_length: Option<usize>,
    ) -> anyhow::Result<Result<RedactedMutationReturn, RedactedMutationError>> {
        self.execute(mutation_identifier, false)
    }

    async fn execute_admin_mutation(
        &self,
        host: &ResolvedHostname,
        request_context: RequestContext,
        identity: Identity,
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        caller: FunctionCaller,
        mutation_identifier: Option<SessionRequestIdentifier>,
        mutation_queue_length: Option<usize>,
    ) -> anyhow::Result<Result<RedactedMutationReturn, RedactedMutationError>> {
        self.execute(mutation_identifier, true)
    }

    async fn execute_public_action(
        &self,
        host: &ResolvedHostname,
        request_context: RequestContext,
        identity: Identity,
        path: ExportPath,
        args: SerializedArgs,
        caller: FunctionCaller,
    ) -> anyhow::Result<Result<RedactedActionReturn, RedactedActionError>> {
        panic!("unexpected application call")
    }

    async fn execute_admin_action(
        &self,
        host: &ResolvedHostname,
        request_context: RequestContext,
        identity: Identity,
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        caller: FunctionCaller,
    ) -> anyhow::Result<Result<RedactedActionReturn, RedactedActionError>> {
        panic!("unexpected application call")
    }

    async fn execute_http_action(
        &self,
        host: &ResolvedHostname,
        request_context: RequestContext,
        http_request_metadata: HttpActionRequest,
        identity: Identity,
        caller: FunctionCaller,
        response_streamer: HttpActionResponseStreamer,
    ) -> anyhow::Result<()> {
        panic!("unexpected application call")
    }

    async fn execute_any_function(
        &self,
        host: &ResolvedHostname,
        request_context: RequestContext,
        identity: Identity,
        path: CanonicalizedComponentFunctionPath,
        args: SerializedArgs,
        caller: FunctionCaller,
    ) -> anyhow::Result<Result<FunctionReturn, FunctionError>> {
        panic!("unexpected application call")
    }

    async fn latest_timestamp(
        &self,
        host: &ResolvedHostname,
        request_id: RequestId,
    ) -> anyhow::Result<RepeatableTimestamp> {
        Ok(RepeatableTimestamp::MIN)
    }

    async fn check_store_file_authorization(
        &self,
        host: &ResolvedHostname,
        request_id: RequestId,
        token: &str,
        validity: Duration,
    ) -> anyhow::Result<ComponentId> {
        panic!("unexpected application call")
    }

    async fn store_file(
        &self,
        host: &ResolvedHostname,
        request_id: RequestId,
        origin: ConvexOrigin,
        component: ComponentId,
        content_length: Option<ContentLength>,
        content_type: Option<ContentType>,
        expected_sha256: Option<Sha256Digest>,
        body: BoxStream<'_, anyhow::Result<Bytes>>,
    ) -> anyhow::Result<DeveloperDocumentId> {
        panic!("unexpected application call")
    }

    async fn get_file_range(
        &self,
        host: &ResolvedHostname,
        request_id: RequestId,
        origin: ConvexOrigin,
        component: ComponentId,
        file_storage_id: FileStorageId,
        range: (Bound<u64>, Bound<u64>),
    ) -> anyhow::Result<FileStream> {
        panic!("unexpected application call")
    }

    async fn get_file(
        &self,
        host: &ResolvedHostname,
        request_id: RequestId,
        origin: ConvexOrigin,
        component: ComponentId,
        file_storage_id: FileStorageId,
    ) -> anyhow::Result<FileStream> {
        panic!("unexpected application call")
    }

    async fn subscription_client(
        &self,
        host: &ResolvedHostname,
    ) -> anyhow::Result<Box<dyn SubscriptionClient>> {
        Ok(Box::new(EmptySubscriptions))
    }

    async fn partition_id(&self, host: &ResolvedHostname) -> anyhow::Result<u64> {
        Ok(0)
    }
}

struct EmptySubscriptions;
#[async_trait]
impl SubscriptionClient for EmptySubscriptions {
    async fn subscribe(&self, _token: Token) -> anyhow::Result<Arc<dyn SubscriptionTrait>> {
        panic!("empty query set unexpectedly subscribed");
    }
}

#[test]
fn exhausted_occ_keeps_public_and_admin_mutations_on_one_worker() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let worker_rt = rt.clone();
    rt.block_on("sync_terminal_occ", async move {
        tokio::time::timeout(Duration::from_secs(10), async move {
            for admin in [false, true] {
                let api = Arc::new(MutationApi::default());
                let (client_tx, client_rx) = mpsc::unbounded_channel();
                let (server_tx, mut server_rx) = measurable_unbounded_channel();
                let connected = Arc::new(AtomicUsize::new(0));
                let on_connect = connected.clone();
                let mut worker = SyncWorker::new(
                    api.clone(),
                    worker_rt.clone(),
                    ResolvedHostname {
                        deployment_name: String::new(),
                        destination: RequestDestination::ConvexCloud,
                        source: ResolvedHostnameSource::Local,
                    },
                    SyncWorkerConfig::default(),
                    client_rx,
                    server_tx,
                    Box::new(move |_| {
                        on_connect.fetch_add(1, Ordering::SeqCst);
                    }),
                    0,
                    RequestMetadata::system(),
                );
                let peer = async {
                    client_tx.send((
                        ClientMessage::Connect {
                            session_id: "00000000-0000-0000-0000-000000000000".parse()?,
                            connection_count: 0,
                            last_close_reason: "InitialConnect".to_owned(),
                            max_observed_timestamp: None,
                            client_ts: None,
                            query_workload_class: None,
                            degradable_query_pressure_version: None,
                        },
                        worker_rt.monotonic_now(),
                    ))?;
                    if admin {
                        client_tx.send((
                            ClientMessage::Authenticate {
                                base_version: 0.into(),
                                token: AuthenticationToken::Admin("fixture".into(), None),
                            },
                            worker_rt.monotonic_now(),
                        ))?;
                    }
                    for request_id in 0..3 {
                        client_tx.send((
                            ClientMessage::Mutation {
                                request_id,
                                udf_path: "fixture:write".parse()?,
                                args: SerializedArgs::from_args(vec![serde_json::json!({})])?,
                                component_path: admin.then(String::new),
                            },
                            worker_rt.monotonic_now(),
                        ))?;
                    }
                    let mut responses = Vec::new();
                    while responses.len() < 3 {
                        let (message, _) =
                            server_rx.next().await.context("worker closed on OCC")?;
                        match message {
                            ServerMessage::MutationResponse {
                                request_id,
                                result,
                                ts,
                                log_lines,
                            } => {
                                assert_eq!(request_id as usize, responses.len());
                                assert!(log_lines.0.is_empty());
                                if request_id < 2 {
                                    assert_eq!(ts, None);
                                    let Err(ErrorPayload::Message(message)) = result else {
                                        panic!("exhausted OCC did not become a terminal error");
                                    };
                                    assert_eq!(
                                        message,
                                        format!("{}: {}", errors::OCC_ERROR, errors::OCC_ERROR_MSG)
                                    );
                                } else {
                                    assert!(result.is_ok(), "following mutation failed");
                                    assert_eq!(ts, Some(Timestamp::MIN));
                                }
                                responses.push(request_id);
                            },
                            ServerMessage::Transition { .. } | ServerMessage::Ping => {},
                            other => panic!("unexpected worker response: {other:?}"),
                        }
                    }
                    // A query-set change after both rejections still reaches the same worker.
                    client_tx.send((
                        ClientMessage::ModifyQuerySet {
                            base_version: 0.into(),
                            new_version: 1.into(),
                            modifications: vec![],
                        },
                        worker_rt.monotonic_now(),
                    ))?;
                    loop {
                        let (message, _) = server_rx
                            .next()
                            .await
                            .context("worker stopped after mutation")?;
                        match message {
                            ServerMessage::Transition { end_version, .. }
                                if end_version.query_set == 1.into() =>
                            {
                                break
                            },
                            ServerMessage::Transition { .. } | ServerMessage::Ping => {},
                            other => panic!("unexpected extra response: {other:?}"),
                        }
                    }
                    assert_eq!(
                        api.calls.lock().unwrap().clone(),
                        BTreeMap::from([(0, admin), (1, admin), (2, admin)])
                    );
                    assert_eq!(connected.load(Ordering::SeqCst), 1);
                    drop(client_tx);
                    Ok::<_, anyhow::Error>(())
                };
                futures::try_join!(worker.go(), peer)?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?
    })
}

#[test]
fn terminal_occ_classification_does_not_change_other_failures_or_http() {
    let occ: anyhow::Error = ErrorMetadata::system_occ(None, None).into();
    assert_eq!(occ.http_status().as_u16(), 503);
    assert!(!occ.is_deterministic_user_error());
    for error in [
        anyhow::anyhow!(errors::OCC_ERROR),
        ErrorMetadata::overloaded("Overloaded", errors::OCC_ERROR).into(),
        ErrorMetadata::rejected_before_execution("ExpiredInQueue", "expired").into(),
    ] {
        assert!(mutation_response(9, Err(error)).is_err());
    }
}
