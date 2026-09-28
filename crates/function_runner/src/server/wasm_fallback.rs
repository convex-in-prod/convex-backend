use super::*;

pub(super) struct PreparedFallback {
    pub request: RunRequestArgs,
    pub metadata: Option<FunctionMetadata>,
    pub publication: Option<QueryShadowPrimaryPublication>,
    pub admission_budget: Duration,
    pub execution_deadline: tokio::time::Instant,
}

/// Replay state is captured before any handler work. A retry gets a new
/// transaction at the same snapshot, never the failed attempt's writes.
#[derive(Default)]
pub(super) struct PrimaryReplay {
    inputs: Option<(
        DatabaseUdfExecutionInputs,
        database::TransactionDocumentCreationState,
    )>,
}

impl PrimaryReplay {
    pub(super) fn inputs<RT: Runtime>(
        &mut self,
        rt: &RT,
        transaction: &mut Transaction<RT>,
    ) -> anyhow::Result<DatabaseUdfExecutionInputs> {
        if let Some((inputs, creation)) = &self.inputs {
            transaction.apply_document_creation_state(creation)?;
            return Ok(*inputs);
        }
        let inputs = DatabaseUdfExecutionInputs {
            rng_seed: rt.rng().random(),
            unix_timestamp: udf_unix_timestamp(transaction.next_creation_time()),
        };
        self.inputs = Some((inputs, transaction.document_creation_state()));
        Ok(inputs)
    }
}

pub(super) fn permits_fallback(mode: &FunctionExecutionMode) -> bool {
    match mode {
        FunctionExecutionMode::Configured
        | FunctionExecutionMode::ConfiguredWasm
        | FunctionExecutionMode::StaticHermesWasmWithV8Shadow(_) => true,
        FunctionExecutionMode::V8
        | FunctionExecutionMode::V8WithHostOperationTrace
        | FunctionExecutionMode::StaticHermesWasm
        | FunctionExecutionMode::StaticHermesWasmWithHostOperationTrace
        | FunctionExecutionMode::V8WithStaticHermesWasmShadow(_) => false,
        #[cfg(feature = "testing")]
        FunctionExecutionMode::V8WithStaticHermesWasmMutationShadowForTest { .. } => false,
    }
}

pub(super) fn resolve_route<RT: Runtime>(
    mode: &FunctionExecutionMode,
    udf_type: UdfType,
    metadata: Option<&FunctionMetadata>,
) -> anyhow::Result<Option<isolate::StaticHermesWasmtimeRouteHandle>> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    if !matches!(udf_type, UdfType::Query | UdfType::Mutation) {
        return Ok(None);
    }
    if matches!(mode, FunctionExecutionMode::Configured)
        && common::wasm_runtime_policy::runtime_policy_store()
            .snapshot()
            .policy
            .for_udf(udf_type)
            .is_none_or(|mode| mode.primary == common::wasm_runtime_policy::PrimaryRuntime::V8)
    {
        return Ok(None);
    }
    match mode {
        FunctionExecutionMode::V8 | FunctionExecutionMode::V8WithHostOperationTrace => Ok(None),
        FunctionExecutionMode::V8WithStaticHermesWasmShadow(_) => {
            IsolateClient::<RT>::resolve_static_hermes_wasmtime_shadow_route(
                udf_type,
                metadata.path_and_args(),
            )
        },
        #[cfg(feature = "testing")]
        FunctionExecutionMode::V8WithStaticHermesWasmMutationShadowForTest { .. } => {
            IsolateClient::<RT>::resolve_static_hermes_wasmtime_shadow_route(
                udf_type,
                metadata.path_and_args(),
            )
        },
        FunctionExecutionMode::Configured
        | FunctionExecutionMode::ConfiguredWasm
        | FunctionExecutionMode::StaticHermesWasm
        | FunctionExecutionMode::StaticHermesWasmWithHostOperationTrace
        | FunctionExecutionMode::StaticHermesWasmWithV8Shadow(_) => {
            IsolateClient::<RT>::resolve_static_hermes_wasmtime_route(
                udf_type,
                metadata.path_and_args(),
            )
        },
    }
}

/// Only selected configured primaries retain this copy. In particular, V8-only
/// requests do not copy their write sets to prepare for a retry they cannot
/// use.
pub(super) fn v8_request(request: &RunRequestArgs) -> RunRequestArgs {
    assert!(matches!(
        request.udf_type,
        UdfType::Query | UdfType::Mutation
    ));
    assert!(request.function_execution_start.is_none());
    RunRequestArgs {
        execution_mode: FunctionExecutionMode::V8,
        query_shadow_primary_publication: None,
        key_broker: request.key_broker.clone(),
        index_reader: request.index_reader.clone(),
        convex_origin: request.convex_origin.clone(),
        bootstrap_metadata: request.bootstrap_metadata.clone(),
        table_count_snapshot: request.table_count_snapshot.clone(),
        text_index_snapshot: request.text_index_snapshot.clone(),
        action_callbacks: request.action_callbacks.clone(),
        fetch_client: request.fetch_client.clone(),
        log_line_sender: request.log_line_sender.clone(),
        function_started_sender: None,
        function_execution_start: None,
        udf_type: request.udf_type,
        identity: request.identity.clone(),
        existing_writes: request.existing_writes.clone(),
        default_system_env_vars: request.default_system_env_vars.clone(),
        in_memory_index_last_modified: request.in_memory_index_last_modified.clone(),
        context: request.context.clone(),
        scheduler_dependency: request.scheduler_dependency,
        subfunctions_in_same_isolate: request.subfunctions_in_same_isolate,
        deployment: request.deployment.clone(),
    }
}

/// Forward the one request-level start notification across engine attempts.
/// A failure before execution must leave it available for the V8 retry.
pub(super) async fn forward_start<T>(
    future: impl Future<Output = T>,
    started: Option<oneshot::Receiver<()>>,
    sender: &mut Option<oneshot::Sender<()>>,
) -> T {
    let Some(mut started) = started else {
        return future.await;
    };
    tokio::pin!(future);
    tokio::select! {
        biased;
        notification = &mut started => {
            if notification.is_ok() && let Some(sender) = sender.take() {
                let _ = sender.send(());
            }
            future.await
        },
        result = &mut future => {
            // Completion can race the notification from a different executor.
            if matches!(started.try_recv(), Ok(())) && let Some(sender) = sender.take() {
                let _ = sender.send(());
            }
            result
        },
    }
}

/// V8 gets a bounded queue wait within the request's original wall deadline.
/// Time executing Wasm must not consume the retry's entire admission allowance;
/// otherwise an active Wasm timeout can make recovery impossible before V8 is
/// even submitted. Once V8 starts its normal user/system timers also apply.
pub(super) async fn within_retry_deadline<T>(
    future: impl Future<Output = anyhow::Result<T>>,
    mut started: oneshot::Receiver<()>,
    sender: &mut Option<oneshot::Sender<()>>,
    admission_deadline: tokio::time::Instant,
    execution_deadline: tokio::time::Instant,
) -> anyhow::Result<T> {
    if tokio::time::Instant::now() >= execution_deadline {
        anyhow::bail!(errors::ErrorMetadata::bad_request(
            "SystemTimeoutError",
            "V8 fallback exhausted the original request time budget"
        ));
    }
    if tokio::time::Instant::now() >= admission_deadline {
        anyhow::bail!(errors::ErrorMetadata::rejected_before_execution(
            "ExpiredInQueue",
            "V8 fallback exhausted its admission deadline"
        ));
    }
    tokio::pin!(future);
    let mut awaiting_start = true;
    let mut admitted = false;
    loop {
        tokio::select! {
            biased;
            // Outer expiry is an execution timeout even while V8 is queued.
            // Treating it as capacity would defer a leader with no retry budget.
            _ = tokio::time::sleep_until(execution_deadline) => {
                anyhow::bail!(errors::ErrorMetadata::bad_request(
                    "SystemTimeoutError", "V8 fallback exhausted the original request time budget"
                ));
            },
            notification = &mut started, if awaiting_start => {
                awaiting_start = false;
                admitted = notification.is_ok();
                if admitted && let Some(sender) = sender.take() {
                    let _ = sender.send(());
                }
            },
            // V8 runs on another executor. Consume an already published start
            // before queue expiry: a delayed runner poll must not classify an
            // executing recovery as rejected before execution.
            _ = tokio::time::sleep_until(admission_deadline), if !admitted => {
                anyhow::bail!(errors::ErrorMetadata::rejected_before_execution(
                    "ExpiredInQueue", "V8 fallback exhausted its admission deadline"
                ));
            },
            result = &mut future => {
                // The worker can publish both start and completion in this poll.
                if awaiting_start && matches!(started.try_recv(), Ok(()))
                    && let Some(sender) = sender.take()
                {
                    let _ = sender.send(());
                }
                return result;
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{
        AtomicBool,
        Ordering,
    };

    use errors::ErrorMetadataAnyhowExt;

    use super::*;

    #[tokio::test]
    async fn expired_retry_does_not_poll_or_acquire_v8() {
        let polled = AtomicBool::new(false);
        let (_sender, started) = oneshot::channel();
        let deadline = tokio::time::Instant::now();
        let error = within_retry_deadline(
            async {
                polled.store(true, Ordering::SeqCst);
                Ok(())
            },
            started,
            &mut None,
            deadline,
            deadline + Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.short_msg(), "ExpiredInQueue");
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn retry_outer_deadline_is_not_queue_overload() {
        for already_expired in [false, true] {
            let polled = AtomicBool::new(false);
            let (_sender, started) = oneshot::channel();
            let deadline = tokio::time::Instant::now()
                + if already_expired {
                    Duration::ZERO
                } else {
                    Duration::from_millis(1)
                };
            let error = within_retry_deadline(
                async {
                    polled.store(true, Ordering::SeqCst);
                    futures::future::pending::<anyhow::Result<()>>().await
                },
                started,
                &mut None,
                deadline,
                deadline,
            )
            .await
            .unwrap_err();
            assert_eq!(error.short_msg(), "SystemTimeoutError");
            assert!(!error.is_rejected_before_execution());
            if already_expired {
                assert!(!polled.load(Ordering::SeqCst));
            }
        }
    }

    #[tokio::test]
    async fn queued_retry_retains_v8_rejection_and_cancellation() {
        let (_sender, started) = oneshot::channel();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let error = within_retry_deadline(
            async {
                Err::<(), _>(
                    errors::ErrorMetadata::rejected_before_execution("ExecuteFullError", "full")
                        .into(),
                )
            },
            started,
            &mut None,
            deadline,
            deadline,
        )
        .await
        .unwrap_err();
        assert_eq!(error.short_msg(), "ExecuteFullError");
        let (owned_sender, owned_receiver) = oneshot::channel::<()>();
        let (_sender, started) = oneshot::channel();
        let mut start_notification = None;
        let future = within_retry_deadline(
            async move {
                let _owned = owned_sender;
                futures::future::pending::<anyhow::Result<()>>().await
            },
            started,
            &mut start_notification,
            deadline,
            deadline,
        );
        assert!(tokio::time::timeout(Duration::from_millis(1), future)
            .await
            .is_err());
        assert!(owned_receiver.await.is_err());
    }

    #[tokio::test]
    async fn start_notification_survives_a_failed_first_attempt() {
        let (sender, mut receiver) = oneshot::channel();
        let mut sender = Some(sender);
        let (first, started) = oneshot::channel();
        drop(first);
        forward_start(async {}, Some(started), &mut sender).await;
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let (second, started) = oneshot::channel();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        within_retry_deadline(
            async {
                let _ = second.send(());
                tokio::task::yield_now().await;
                Ok(())
            },
            started,
            &mut sender,
            deadline,
            deadline,
        )
        .await
        .unwrap();
        receiver.await.unwrap();
    }

    #[tokio::test]
    async fn completed_retry_forwards_its_start_notification() {
        let (sender, receiver) = oneshot::channel();
        let mut sender = Some(sender);
        let (started_sender, started) = oneshot::channel();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        within_retry_deadline(
            async {
                started_sender.send(()).unwrap();
                Ok(())
            },
            started,
            &mut sender,
            deadline,
            deadline,
        )
        .await
        .unwrap();
        receiver.await.unwrap();
    }

    #[tokio::test]
    async fn published_retry_start_survives_a_delayed_runner_poll() {
        let (sender, receiver) = oneshot::channel();
        let mut sender = Some(sender);
        let (started_sender, started) = oneshot::channel();
        let (completed, completion) = oneshot::channel::<()>();
        let admission_deadline = tokio::time::Instant::now() + Duration::from_millis(20);
        let retry = within_retry_deadline(
            async {
                started_sender.send(()).unwrap();
                completion.await.unwrap();
                Ok(())
            },
            started,
            &mut sender,
            admission_deadline,
            admission_deadline + Duration::from_secs(1),
        );
        tokio::pin!(retry);
        assert!(futures::poll!(&mut retry).is_pending());
        // The worker has started, but its caller is not polled until the queue
        // deadline has passed and the result is also ready.
        tokio::time::sleep_until(admission_deadline).await;
        completed.send(()).unwrap();
        retry.await.unwrap();
        receiver.await.unwrap();
    }
}
