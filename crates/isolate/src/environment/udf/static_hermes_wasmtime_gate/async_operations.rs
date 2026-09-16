#[cfg(test)]
use proptest::prelude::*;

use super::{
    super::async_syscall::AsyncSyscallResult,
    capability_bridge::{
        decode_function_address,
        AsyncCapabilityOperation,
        CapabilityFunctionAddress,
        CapabilityNestedUdfType,
        CapabilityQueryConstraint,
        CapabilityQueryOperator,
        CapabilityQueryPagination,
        CapabilityQuerySource,
        CapabilityQueryTerminal,
        CapabilitySearchFilter,
        InvocationCapabilityIdentity,
        SyncCapabilityOperation,
    },
    query_operations::canonical_query_terminal_limit,
    *,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct AsyncOperationHandle(u32);

impl AsyncOperationHandle {
    pub(super) fn from_abi(value: i32) -> Result<Self, WasmtimeError> {
        let value = u32::try_from(value).map_err(|_| WasmtimeError::new(HostInvariant))?;
        if value == 0 {
            return Err(WasmtimeError::new(HostInvariant));
        }
        Ok(Self(value))
    }

    pub(super) fn to_abi(self) -> i32 {
        i32::try_from(self.0).expect("async operation handle exceeded its ABI range")
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct QueryStreamHandle(u32);

impl QueryStreamHandle {
    fn from_abi(value: i32) -> Result<Self, WasmtimeError> {
        let value = u32::try_from(value).map_err(|_| WasmtimeError::new(HostInvariant))?;
        if value == 0 {
            return Err(WasmtimeError::new(HostInvariant));
        }
        Ok(Self(value))
    }

    fn to_abi(self) -> i32 {
        i32::try_from(self.0).expect("query stream handle exceeded its ABI range")
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum AsyncOperationCompletion {
    Success(Option<JsonValue>),
    DeveloperError(GeneratedDeveloperError),
}

/// Owns operation descriptors and completions without retaining a future that
/// borrows the invocation's transaction provider. Completion announcement
/// follows the order supplied by the executor, never numeric handle order.
pub(super) struct AsyncOperationQueue<O, C> {
    next_handle: Option<u32>,
    // Requeued work advances the compatible syscall wave that produced it
    // before the pending suffix can start a different kind of syscall.
    requeued: VecDeque<(AsyncOperationHandle, O)>,
    pending: VecDeque<(AsyncOperationHandle, O)>,
    in_flight: BTreeSet<AsyncOperationHandle>,
    completed: BTreeMap<AsyncOperationHandle, C>,
    ready: VecDeque<AsyncOperationHandle>,
    announced: Option<(AsyncOperationHandle, bool)>,
}

impl<O, C> Default for AsyncOperationQueue<O, C> {
    fn default() -> Self {
        Self {
            next_handle: Some(1),
            requeued: VecDeque::new(),
            pending: VecDeque::new(),
            in_flight: BTreeSet::new(),
            completed: BTreeMap::new(),
            ready: VecDeque::new(),
            announced: None,
        }
    }
}

impl<O, C> AsyncOperationQueue<O, C> {
    pub(super) fn enqueue(&mut self, operation: O) -> Result<AsyncOperationHandle, WasmtimeError> {
        let handle = self.enqueue_handle()?;
        self.pending.push_back((handle, operation));
        Ok(handle)
    }

    pub(super) fn pop_pending(&mut self) -> Option<(AsyncOperationHandle, O)> {
        let (handle, operation) = self
            .requeued
            .pop_front()
            .or_else(|| self.pending.pop_front())?;
        assert!(
            self.in_flight.insert(handle),
            "async operation entered flight more than once"
        );
        Some((handle, operation))
    }

    pub(super) fn requeue(
        &mut self,
        handle: AsyncOperationHandle,
        operation: O,
    ) -> Result<(), WasmtimeError> {
        if !self.in_flight.remove(&handle) || self.completed.contains_key(&handle) {
            return Err(WasmtimeError::new(HostInvariant));
        }
        self.requeued.push_back((handle, operation));
        Ok(())
    }

    fn has_pending(&self) -> bool {
        !self.requeued.is_empty() || !self.pending.is_empty()
    }

    fn front_pending(&self) -> Option<&(AsyncOperationHandle, O)> {
        self.requeued.front().or_else(|| self.pending.front())
    }

    pub(super) fn complete(
        &mut self,
        handle: AsyncOperationHandle,
        completion: C,
    ) -> Result<(), WasmtimeError> {
        if !self.in_flight.remove(&handle) || self.completed.insert(handle, completion).is_some() {
            return Err(WasmtimeError::new(HostInvariant));
        }
        self.ready.push_back(handle);
        Ok(())
    }

    pub(super) fn enqueue_completion(
        &mut self,
        completion: C,
    ) -> Result<AsyncOperationHandle, WasmtimeError> {
        let handle = self.enqueue_handle()?;
        if self.completed.insert(handle, completion).is_some() {
            return Err(WasmtimeError::new(HostInvariant));
        }
        self.ready.push_back(handle);
        Ok(handle)
    }

    pub(super) fn announce_completion(
        &mut self,
    ) -> Result<Option<AsyncOperationHandle>, WasmtimeError> {
        if self.announced.is_some() {
            return Err(WasmtimeError::new(HostInvariant));
        }
        let Some(handle) = self.ready.pop_front() else {
            return Ok(None);
        };
        if !self.completed.contains_key(&handle) {
            return Err(WasmtimeError::new(HostInvariant));
        }
        self.announced = Some((handle, false));
        Ok(Some(handle))
    }

    pub(super) fn observe_completion_status(
        &mut self,
        handle: AsyncOperationHandle,
    ) -> Result<&C, WasmtimeError> {
        let Some((announced, status_observed)) = &mut self.announced else {
            return Err(WasmtimeError::new(HostInvariant));
        };
        if *announced != handle || *status_observed {
            return Err(WasmtimeError::new(HostInvariant));
        }
        *status_observed = true;
        self.completed
            .get(&handle)
            .ok_or_else(|| WasmtimeError::new(HostInvariant))
    }

    pub(super) fn take_completion(
        &mut self,
        handle: AsyncOperationHandle,
    ) -> Result<C, WasmtimeError> {
        if self.announced != Some((handle, true)) {
            return Err(WasmtimeError::new(HostInvariant));
        }
        self.announced = None;
        self.completed
            .remove(&handle)
            .ok_or_else(|| WasmtimeError::new(HostInvariant))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.requeued.is_empty()
            && self.pending.is_empty()
            && self.in_flight.is_empty()
            && self.completed.is_empty()
            && self.ready.is_empty()
            && self.announced.is_none()
    }

    pub(super) fn clear(&mut self) {
        self.requeued.clear();
        self.pending.clear();
        self.in_flight.clear();
        self.completed.clear();
        self.ready.clear();
        self.announced = None;
    }

    fn unsettled_count(&self) -> Result<usize, WasmtimeError> {
        let ready_handles = self.ready.iter().copied().collect::<BTreeSet<_>>();
        let completed_handles = self.completed.keys().copied().collect::<BTreeSet<_>>();
        if !self.in_flight.is_empty()
            || self.announced.is_some()
            || self.ready.len() != self.completed.len()
            || ready_handles != completed_handles
        {
            return Err(WasmtimeError::new(HostInvariant));
        }
        self.requeued
            .len()
            .checked_add(self.pending.len())
            .and_then(|count| count.checked_add(self.completed.len()))
            .ok_or_else(|| WasmtimeError::new(HostInvariant))
    }

    fn enqueue_handle(&mut self) -> Result<AsyncOperationHandle, WasmtimeError> {
        let next_handle = self
            .next_handle
            .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
        let handle = AsyncOperationHandle(next_handle);
        self.next_handle = (next_handle < i32::MAX as u32).then_some(next_handle + 1);
        Ok(handle)
    }
}

pub(super) async fn run_generated_async_syscall<RT: Runtime>(
    state: &mut HostState<RT>,
    name: String,
    args: JsonValue,
) -> Result<Option<JsonValue>, WasmtimeError> {
    let results =
        run_generated_async_syscall_batch(state, AsyncSyscallBatch::new(name, args)).await?;
    let Some(results) = results else {
        return Ok(None);
    };
    let [result] = <[_; 1]>::try_from(results).map_err(|_| WasmtimeError::new(HostInvariant))?;
    Ok(Some(result))
}

async fn run_generated_async_syscall_batch<RT: Runtime>(
    state: &mut HostState<RT>,
    batch: AsyncSyscallBatch,
) -> Result<Option<Vec<JsonValue>>, WasmtimeError> {
    let outcomes = execute_generated_async_syscall_batch(state, batch).await?;
    let mut values = Vec::with_capacity(outcomes.len());
    for (index, outcome) in outcomes.into_iter().enumerate() {
        match outcome {
            GeneratedAsyncSyscallOutcome::Success(value) => values.push(value),
            GeneratedAsyncSyscallOutcome::DeveloperError(error) => {
                record_generated_developer_error(state, index, error)?;
                return Ok(None);
            },
        }
    }
    Ok(Some(values))
}

enum GeneratedAsyncSyscallOutcome {
    Success(JsonValue),
    DeveloperError(GeneratedDeveloperError),
}

async fn execute_generated_async_syscall_batch<RT: Runtime>(
    state: &mut HostState<RT>,
    batch: AsyncSyscallBatch,
) -> Result<Vec<GeneratedAsyncSyscallOutcome>, WasmtimeError> {
    record_generated_host_call_mark(&state.metrics, "async-syscall:prepare");
    let control = state.read_control.take();
    let metrics = Arc::clone(&state.metrics);
    let cancellation = generated_state(state)?.cancellation.clone();
    let name = batch.name().to_owned();
    let udf_callback = state.udf_callback.clone();
    let syscall = state.provider.run_async_syscall_batch(batch, udf_callback);
    let syscall = async move {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                Err(WasmtimeError::msg("generated async syscall cancelled"))
            },
            result = async {
                match control {
                    Some(control) => {
                        require_pending_read(syscall, control, Arc::clone(&metrics)).await
                    },
                    None => {
                        record_generated_host_call_mark(&metrics, "async-syscall:execute");
                        let result = syscall.await;
                        record_generated_host_call_mark(&metrics, "async-syscall:completed");
                        #[cfg(any(test, feature = "testing"))]
                        metrics.read_completed.fetch_add(1, Ordering::SeqCst);
                        Ok(result)
                    },
                }
            } => result,
        }
    };
    let results = state
        .timeout
        .as_mut()
        .context("gate timeout missing")
        .map_err(|_| WasmtimeError::new(HostInvariant))?
        .with_release_permit(PauseReason::DatabaseSyscall { name }, async move {
            syscall.await.map_err(|error| {
                anyhow::anyhow!(error).context("generated async syscall execution failed")
            })
        })
        .await;
    record_generated_host_call_mark(&state.metrics, "async-syscall:resumed");
    let results = results.map_err(|error| {
        WasmtimeError::msg(format!("generated async syscall wait failed: {error:#}"))
    })?;
    results
        .into_iter()
        .map(|result| {
            let AsyncSyscallResult {
                result,
                host_operation_error,
            } = result;
            match result {
                Ok(result) => serde_json::from_str(&result)
                    .map(GeneratedAsyncSyscallOutcome::Success)
                    .map_err(|_| WasmtimeError::new(HostInvariant)),
                Err(error) if error.is_deterministic_user_error() => Ok(
                    GeneratedAsyncSyscallOutcome::DeveloperError(GeneratedDeveloperError {
                        message: error.short_msg().to_owned(),
                        host_operation_error,
                    }),
                ),
                Err(error) => Err(WasmtimeError::msg(format!(
                    "generated async syscall failed: {error:#}"
                ))),
            }
        })
        .collect()
}

pub(super) fn record_generated_developer_error<RT: Runtime>(
    state: &mut HostState<RT>,
    source_index: usize,
    error: impl Into<GeneratedDeveloperError>,
) -> Result<(), WasmtimeError> {
    if state.developer_error.is_some() {
        return Err(WasmtimeError::new(HostInvariant));
    }
    #[cfg(test)]
    state
        .metrics
        .first_developer_error_index
        .store(source_index + 1, Ordering::SeqCst);
    #[cfg(not(test))]
    let _ = source_index;
    state.developer_error = Some(error.into());
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GeneratedAsyncBatchInvocation {
    #[serde(deserialize_with = "deserialize_generated_operation_id")]
    operation_id: u32,
    arguments: Vec<JsonValue>,
}

fn deserialize_generated_operation_id<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let operation_id = f64::deserialize(deserializer)?;
    if !operation_id.is_finite()
        || operation_id < 0.0
        || operation_id.fract() != 0.0
        || operation_id > f64::from(u32::MAX)
    {
        return Err(serde::de::Error::custom(
            "generated batch operation ID must be a finite u32 integer",
        ));
    }
    Ok(operation_id as u32)
}

enum GeneratedAsyncOperation {
    Immediate(AsyncOperationCompletion),
    Syscall {
        name: String,
        args: JsonValue,
        normalization: GeneratedAsyncNormalization,
    },
    Query {
        query: Query,
        version: Option<Version>,
        table_name: String,
        terminal: QueryTerminal,
    },
    QueryContinuation {
        query_id: QueryId,
        table_name: String,
        terminal: QueryTerminal,
        values: Vec<JsonValue>,
    },
    QueryStreamStart {
        stream_handle: QueryStreamHandle,
        query: Query,
        version: Option<Version>,
    },
    QueryStreamContinuation {
        stream_handle: QueryStreamHandle,
        query_id: QueryId,
    },
}

enum GeneratedQueryStream {
    Pending {
        query: Query,
        version: Option<Version>,
    },
    Immediate(AsyncOperationCompletion),
    Active(QueryId),
}

struct GeneratedQueryStreams {
    next_handle: Option<u32>,
    streams: BTreeMap<QueryStreamHandle, GeneratedQueryStream>,
}

impl Default for GeneratedQueryStreams {
    fn default() -> Self {
        Self {
            next_handle: Some(1),
            streams: BTreeMap::new(),
        }
    }
}

impl GeneratedQueryStreams {
    fn open(&mut self, stream: GeneratedQueryStream) -> Result<QueryStreamHandle, WasmtimeError> {
        let next_handle = self
            .next_handle
            .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
        let handle = QueryStreamHandle(next_handle);
        self.next_handle = (next_handle < i32::MAX as u32).then_some(next_handle + 1);
        if self.streams.insert(handle, stream).is_some() {
            return Err(WasmtimeError::new(HostInvariant));
        }
        Ok(handle)
    }

    fn take_next(
        &mut self,
        handle: QueryStreamHandle,
    ) -> Result<GeneratedAsyncOperation, WasmtimeError> {
        let stream = self
            .streams
            .remove(&handle)
            .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
        Ok(match stream {
            GeneratedQueryStream::Pending { query, version } => {
                GeneratedAsyncOperation::QueryStreamStart {
                    stream_handle: handle,
                    query,
                    version,
                }
            },
            GeneratedQueryStream::Immediate(completion) => {
                GeneratedAsyncOperation::Immediate(completion)
            },
            GeneratedQueryStream::Active(query_id) => {
                GeneratedAsyncOperation::QueryStreamContinuation {
                    stream_handle: handle,
                    query_id,
                }
            },
        })
    }

    fn complete_value(
        &mut self,
        handle: QueryStreamHandle,
        query_id: QueryId,
    ) -> Result<(), WasmtimeError> {
        if self
            .streams
            .insert(handle, GeneratedQueryStream::Active(query_id))
            .is_some()
        {
            return Err(WasmtimeError::new(HostInvariant));
        }
        Ok(())
    }

    fn close(&mut self, handle: QueryStreamHandle) -> Result<Option<QueryId>, WasmtimeError> {
        let stream = self
            .streams
            .remove(&handle)
            .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
        Ok(match stream {
            GeneratedQueryStream::Pending { .. } | GeneratedQueryStream::Immediate(_) => None,
            GeneratedQueryStream::Active(query_id) => Some(query_id),
        })
    }

    fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    fn clear(&mut self) {
        self.streams.clear();
    }
}

#[derive(Default)]
pub(super) struct AsyncOperationState {
    queue: AsyncOperationQueue<GeneratedAsyncOperation, AsyncOperationCompletion>,
    active_queries: BTreeSet<QueryId>,
    query_streams: GeneratedQueryStreams,
    host_operation_errors: BTreeMap<AsyncOperationHandle, HostOperationErrorV1>,
}

impl AsyncOperationState {
    pub(super) fn begin_invocation(&mut self) -> Result<(), WasmtimeError> {
        if !self.is_empty() {
            return Err(WasmtimeError::new(HostInvariant));
        }
        self.queue.next_handle = Some(1);
        self.query_streams.next_handle = Some(1);
        Ok(())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.has_no_active_operations() && self.host_operation_errors.is_empty()
    }

    // A caught host-operation failure may leave error metadata until result
    // finalization clears it, but it must not leave executable work behind.
    pub(super) fn has_no_active_operations(&self) -> bool {
        self.queue.is_empty() && self.active_queries.is_empty() && self.query_streams.is_empty()
    }

    pub(super) fn clear(&mut self) {
        self.queue.clear();
        self.active_queries.clear();
        self.query_streams.clear();
        self.host_operation_errors.clear();
    }

    fn record_host_operation_error(
        &mut self,
        handle: AsyncOperationHandle,
        host_operation_error: HostOperationErrorV1,
    ) -> Result<(), WasmtimeError> {
        if self
            .host_operation_errors
            .insert(handle, host_operation_error)
            .is_some()
        {
            return Err(WasmtimeError::new(HostInvariant));
        }
        Ok(())
    }

    pub(super) fn take_host_operation_error(
        &mut self,
        handle: AsyncOperationHandle,
    ) -> Option<HostOperationErrorV1> {
        self.host_operation_errors.remove(&handle)
    }

    #[cfg(test)]
    pub(super) fn host_operation_error_count(&self) -> usize {
        self.host_operation_errors.len()
    }
}

#[derive(Clone, Copy)]
enum GeneratedAsyncNormalization {
    Json,
    FunctionHandle,
    InsertId,
    PublicId,
    Undefined,
}

enum GeneratedDirectAsyncBatchResult {
    Json(JsonValue),
    Undefined,
}

impl GeneratedDirectAsyncBatchResult {
    fn into_json_array_value(self) -> JsonValue {
        match self {
            Self::Json(value) => value,
            Self::Undefined => JsonValue::Null,
        }
    }
}

struct GeneratedPendingAsyncSyscall {
    source_index: usize,
    name: String,
    args: JsonValue,
    continuation: GeneratedAsyncContinuation,
}

struct GeneratedQueuedAsyncSyscall {
    handle: AsyncOperationHandle,
    continuation: GeneratedAsyncContinuation,
}

enum PreparedGeneratedAsyncOperation {
    Completion(AsyncOperationCompletion),
    Syscall {
        name: String,
        args: JsonValue,
        continuation: GeneratedAsyncContinuation,
    },
}

enum SettledGeneratedAsyncOperation {
    Complete(AsyncOperationCompletion),
    Requeue(GeneratedAsyncOperation),
}

enum GeneratedQueryCleanup {
    ProviderAlreadyCleaned,
    CleanupProvider,
}

enum GeneratedAsyncContinuation {
    Complete(GeneratedAsyncNormalization),
    Query {
        query_id: QueryId,
        table_name: String,
        terminal: QueryTerminal,
        values: Vec<JsonValue>,
    },
    QueryStream {
        stream_handle: QueryStreamHandle,
        query_id: QueryId,
    },
}

pub(super) enum QueryNext {
    Done,
    Value(JsonValue),
}

fn generated_async_arguments<const N: usize>(
    arguments: Vec<JsonValue>,
) -> Result<[JsonValue; N], WasmtimeError> {
    <[_; N]>::try_from(arguments).map_err(|_| WasmtimeError::new(HostInvariant))
}

fn imported_operation_observes_time(descriptor: &ImportedOperationDescriptor) -> bool {
    matches!(
        descriptor,
        ImportedOperationDescriptor::SchedulerRunAfter { .. }
            | ImportedOperationDescriptor::SchedulerRunAt { .. }
    )
}

fn capability_operation_observes_time(operation: &AsyncCapabilityOperation) -> bool {
    matches!(
        operation,
        AsyncCapabilityOperation::SchedulerRunAfter { .. }
            | AsyncCapabilityOperation::SchedulerRunAt { .. }
    )
}

fn materialize_generated_async_operation(
    descriptor: ImportedOperationDescriptor,
    arguments: Vec<JsonValue>,
    udf_type: UdfType,
    npm_version: &Version,
    unix_timestamp: UnixTimestamp,
) -> Result<anyhow::Result<GeneratedAsyncOperation>, WasmtimeError> {
    let operation = match descriptor {
        ImportedOperationDescriptor::Sha256 => {
            return Err(WasmtimeError::new(HostInvariant));
        },
        ImportedOperationDescriptor::HostSecretVerify { .. } => {
            return Err(WasmtimeError::new(HostInvariant));
        },
        ImportedOperationDescriptor::AuthenticationGetUserIdentity {} => {
            let [] = generated_async_arguments(arguments)?;
            GeneratedAsyncOperation::Syscall {
                name: "1.0/getUserIdentity".to_owned(),
                args: json!({}),
                normalization: GeneratedAsyncNormalization::Json,
            }
        },
        ImportedOperationDescriptor::FunctionHandleCreate {} => {
            let [function_address] = generated_async_arguments(arguments)?;
            let function_address = match decode_function_address(function_address) {
                Ok(function_address) => function_address,
                Err(_) => {
                    return Ok(Err(ErrorMetadata::bad_request(
                        "InvalidArgument",
                        "Function reference must resolve to one exact address",
                    )
                    .into()));
                },
            };
            GeneratedAsyncOperation::Syscall {
                name: "1.0/createFunctionHandle".to_owned(),
                args: capability_function_address_syscall_args(function_address, npm_version),
                normalization: GeneratedAsyncNormalization::FunctionHandle,
            }
        },
        ImportedOperationDescriptor::DatabaseNormalizeId { .. } => {
            return Err(WasmtimeError::new(HostInvariant));
        },
        ImportedOperationDescriptor::DatabaseGet { table_name } => {
            let [id] = generated_async_arguments(arguments)?;
            GeneratedAsyncOperation::Syscall {
                name: "1.0/get".to_owned(),
                args: json!({
                    "id": id,
                    "table": table_name,
                }),
                normalization: GeneratedAsyncNormalization::Json,
            }
        },
        ref descriptor @ ImportedOperationDescriptor::DatabaseIndexQuery {
            ref table_name,
            terminal,
            ..
        } => {
            if terminal == QueryTerminal::Stream {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let query_args = match generated_query_syscall_args(descriptor, arguments, npm_version)?
            {
                Ok(query_args) => query_args,
                Err(error) => return Ok(Err(error)),
            };
            let request = match parse_query_stream_request(query_args) {
                Ok(request) => request,
                Err(error) => return Ok(Err(error)),
            };
            GeneratedAsyncOperation::Query {
                query: request.query,
                version: request.version,
                table_name: table_name.clone(),
                terminal,
            }
        },
        ImportedOperationDescriptor::DatabaseInsert { table_name } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let [value] = generated_async_arguments(arguments)?;
            GeneratedAsyncOperation::Syscall {
                name: "1.0/insert".to_owned(),
                args: json!({
                    "table": table_name,
                    "value": value,
                }),
                normalization: GeneratedAsyncNormalization::InsertId,
            }
        },
        ImportedOperationDescriptor::DatabasePatch { table_name } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let [id, value] = generated_async_arguments(arguments)?;
            GeneratedAsyncOperation::Syscall {
                name: "1.0/shallowMerge".to_owned(),
                args: json!({
                    "table": table_name,
                    "id": id,
                    "value": value,
                }),
                normalization: GeneratedAsyncNormalization::Undefined,
            }
        },
        ImportedOperationDescriptor::DatabaseReplace { table_name } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let [id, value] = generated_async_arguments(arguments)?;
            GeneratedAsyncOperation::Syscall {
                name: "1.0/replace".to_owned(),
                args: json!({
                    "table": table_name,
                    "id": id,
                    "value": value,
                }),
                normalization: GeneratedAsyncNormalization::Undefined,
            }
        },
        ImportedOperationDescriptor::DatabaseDelete { table_name } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let [id] = generated_async_arguments(arguments)?;
            GeneratedAsyncOperation::Syscall {
                name: "1.0/remove".to_owned(),
                args: json!({
                    "table": table_name,
                    "id": id,
                }),
                normalization: GeneratedAsyncNormalization::Undefined,
            }
        },
        ref descriptor @ (ImportedOperationDescriptor::SchedulerRunAfter {
            ref function_reference,
        }
        | ImportedOperationDescriptor::SchedulerRunAt {
            ref function_reference,
        }) => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let [time_milliseconds, args] = generated_async_arguments(arguments)?;
            let Some(time_milliseconds) = time_milliseconds.as_f64() else {
                return Ok(Err(ErrorMetadata::bad_request(
                    "InvalidArgument",
                    "The scheduler time must be a number",
                )
                .into()));
            };
            let timestamp = match scheduler_timestamp(descriptor, time_milliseconds, unix_timestamp)
            {
                Ok(timestamp) => timestamp,
                Err(error) => return Ok(Err(error)),
            };
            GeneratedAsyncOperation::Syscall {
                name: "1.0/schedule".to_owned(),
                args: scheduler_syscall_args(function_reference.clone(), timestamp, args),
                normalization: GeneratedAsyncNormalization::PublicId,
            }
        },
    };
    Ok(Ok(operation))
}

fn capability_query_stream_args(
    table: &str,
    source: CapabilityQuerySource,
    operators: Vec<CapabilityQueryOperator>,
    order: capability_bridge::CapabilityQueryOrder,
    npm_version: &Version,
) -> JsonValue {
    capability_query_stream_args_with_terminal_limit(
        table,
        source,
        operators,
        order,
        None,
        npm_version,
    )
}

fn capability_query_stream_args_with_terminal_limit(
    table: &str,
    source: CapabilityQuerySource,
    operators: Vec<CapabilityQueryOperator>,
    order: capability_bridge::CapabilityQueryOrder,
    terminal_limit: Option<u32>,
    npm_version: &Version,
) -> JsonValue {
    let source = match source {
        CapabilityQuerySource::FullTableScan => json!({
            "type": "FullTableScan",
            "tableName": table,
            "order": order.into_json(),
        }),
        CapabilityQuerySource::IndexRange { index, constraints } => {
            let range = constraints
                .into_iter()
                .map(|constraint| {
                    let kind = constraint.query_range_kind();
                    let field = constraint.field().to_owned();
                    let value = constraint.value();
                    json!({
                        "type": kind,
                        "fieldPath": field,
                        "value": value,
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "type": "IndexRange",
                "indexName": format!("{table}.{index}"),
                "range": range,
                "order": order.into_json(),
            })
        },
        CapabilityQuerySource::Search { index, filters } => {
            let filters = filters
                .into_iter()
                .map(|filter| match filter {
                    CapabilitySearchFilter::Search { field, value } => json!({
                        "type": "Search",
                        "fieldPath": field,
                        "value": value,
                    }),
                    CapabilitySearchFilter::Eq { field, value } => json!({
                        "type": "Eq",
                        "fieldPath": field,
                        "value": value,
                    }),
                })
                .collect::<Vec<_>>();
            json!({
                "type": "Search",
                "indexName": format!("{table}.{index}"),
                "filters": filters,
            })
        },
    };
    let mut operators = operators
        .into_iter()
        .map(|operator| match operator {
            CapabilityQueryOperator::Filter { expression } => json!({
                "filter": expression,
            }),
            CapabilityQueryOperator::Limit { limit } => json!({
                "limit": limit,
            }),
        })
        .collect::<Vec<_>>();
    if let Some(limit) = terminal_limit {
        operators.push(json!({ "limit": limit }));
    }
    json!({
        "query": {
            "source": source,
            "operators": operators,
        },
        "version": npm_version.to_string(),
    })
}

fn capability_query_page_args(
    table: &str,
    source: CapabilityQuerySource,
    operators: Vec<CapabilityQueryOperator>,
    order: capability_bridge::CapabilityQueryOrder,
    pagination: CapabilityQueryPagination,
    npm_version: &Version,
) -> JsonValue {
    let mut args = capability_query_stream_args(table, source, operators, order, npm_version);
    let object = args
        .as_object_mut()
        .expect("capability query arguments must be an object");
    object.insert("cursor".to_owned(), json!(pagination.cursor));
    object.insert("endCursor".to_owned(), json!(pagination.end_cursor));
    object.insert(
        "maximumBytesRead".to_owned(),
        json!(pagination.maximum_bytes_read),
    );
    object.insert(
        "maximumRowsRead".to_owned(),
        json!(pagination.maximum_rows_read),
    );
    object.insert("pageSize".to_owned(), json!(pagination.page_size));
    args
}

fn validate_generated_capability_query_source(
    source: &CapabilityQuerySource,
    udf_type: UdfType,
) -> Result<(), WasmtimeError> {
    match udf_type {
        UdfType::Mutation => return Ok(()),
        UdfType::Query => {},
        _ => return Err(WasmtimeError::new(HostInvariant)),
    }
    let CapabilityQuerySource::IndexRange { constraints, .. } = source else {
        return Ok(());
    };
    // Query UDFs have no commit timestamp to assign. Reject pending values before
    // the canonical query parser projects them to MAX_COMMIT_TS for
    // mutation-local reads.
    for constraint in constraints {
        let value = match constraint {
            CapabilityQueryConstraint::Eq { value, .. }
            | CapabilityQueryConstraint::Gt { value, .. }
            | CapabilityQueryConstraint::Gte { value, .. }
            | CapabilityQueryConstraint::Lt { value, .. }
            | CapabilityQueryConstraint::Lte { value, .. } => value,
        };
        if super::super::wasm_udf_abi::is_undefined_marker(value) {
            continue;
        }
        if PendingValue::from_uncommitted_json(value.clone())
            .map_err(|_| WasmtimeError::new(HostInvariant))?
            .is_pending()
        {
            return Err(WasmtimeError::new(HostInvariant));
        }
    }
    Ok(())
}

fn materialize_generated_capability_query_stream(
    operation: AsyncCapabilityOperation,
    udf_type: UdfType,
    npm_version: &Version,
) -> Result<GeneratedQueryStream, WasmtimeError> {
    let AsyncCapabilityOperation::DatabaseQuery {
        table,
        source,
        operators,
        order,
        terminal: CapabilityQueryTerminal::Stream,
    } = operation
    else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    validate_generated_capability_query_source(&source, udf_type)?;
    match parse_query_stream_request(capability_query_stream_args(
        &table,
        source,
        operators,
        order,
        npm_version,
    )) {
        Ok(request) => Ok(GeneratedQueryStream::Pending {
            query: request.query,
            version: request.version,
        }),
        Err(error) if error.is_deterministic_user_error() => Ok(GeneratedQueryStream::Immediate(
            AsyncOperationCompletion::DeveloperError(error.short_msg().to_owned().into()),
        )),
        Err(_) => Err(WasmtimeError::new(HostInvariant)),
    }
}

fn materialize_generated_capability_operation(
    operation: AsyncCapabilityOperation,
    udf_type: UdfType,
    npm_version: &Version,
    unix_timestamp: UnixTimestamp,
) -> Result<anyhow::Result<GeneratedAsyncOperation>, WasmtimeError> {
    if !matches!(udf_type, UdfType::Query | UdfType::Mutation) {
        return Err(WasmtimeError::new(HostInvariant));
    }
    let operation = match operation {
        AsyncCapabilityOperation::AuthenticationGetUserIdentity => {
            GeneratedAsyncOperation::Syscall {
                name: "1.0/getUserIdentity".to_owned(),
                args: json!({}),
                normalization: GeneratedAsyncNormalization::Json,
            }
        },
        AsyncCapabilityOperation::AuditLog { body } => GeneratedAsyncOperation::Syscall {
            name: "1.0/auditLog".to_owned(),
            args: json!({
                "body": body,
                "version": npm_version.to_string(),
            }),
            normalization: GeneratedAsyncNormalization::Undefined,
        },
        AsyncCapabilityOperation::GetFunctionMetadata => GeneratedAsyncOperation::Syscall {
            name: "1.0/getFunctionMetadata".to_owned(),
            args: json!({}),
            normalization: GeneratedAsyncNormalization::Json,
        },
        AsyncCapabilityOperation::GetDeploymentMetadata => GeneratedAsyncOperation::Syscall {
            name: "1.0/getDeploymentMetadata".to_owned(),
            args: json!({}),
            normalization: GeneratedAsyncNormalization::Json,
        },
        AsyncCapabilityOperation::GetTransactionMetrics => GeneratedAsyncOperation::Syscall {
            name: "1.0/getTransactionMetrics".to_owned(),
            args: json!({}),
            normalization: GeneratedAsyncNormalization::Json,
        },
        AsyncCapabilityOperation::GetRequestMetadata => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/getRequestMetadata".to_owned(),
                args: json!({}),
                normalization: GeneratedAsyncNormalization::Json,
            }
        },
        AsyncCapabilityOperation::FunctionHandleCreate { function_address } => {
            GeneratedAsyncOperation::Syscall {
                name: "1.0/createFunctionHandle".to_owned(),
                args: capability_function_address_syscall_args(function_address, npm_version),
                normalization: GeneratedAsyncNormalization::FunctionHandle,
            }
        },
        AsyncCapabilityOperation::DatabaseGet {
            table,
            id,
            is_system,
        } => {
            let mut args = json!({
                "id": id,
                "isSystem": is_system,
                "version": npm_version.to_string(),
            });
            if let Some(table) = table {
                args.as_object_mut()
                    .expect("database get syscall arguments must be an object")
                    .insert("table".to_owned(), JsonValue::String(table));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/get".to_owned(),
                args,
                normalization: GeneratedAsyncNormalization::Json,
            }
        },
        AsyncCapabilityOperation::DatabaseQuery {
            table,
            source,
            operators,
            order,
            terminal,
        } => {
            validate_generated_capability_query_source(&source, udf_type)?;
            match terminal {
                CapabilityQueryTerminal::Paginate(pagination) => GeneratedAsyncOperation::Syscall {
                    name: "1.0/queryPage".to_owned(),
                    args: capability_query_page_args(
                        &table,
                        source,
                        operators,
                        order,
                        pagination,
                        npm_version,
                    ),
                    normalization: GeneratedAsyncNormalization::Json,
                },
                CapabilityQueryTerminal::Stream => return Err(WasmtimeError::new(HostInvariant)),
                capability_terminal => {
                    let terminal = match capability_terminal {
                        CapabilityQueryTerminal::Collect => QueryTerminal::Collect,
                        CapabilityQueryTerminal::First => QueryTerminal::First,
                        CapabilityQueryTerminal::Unique => QueryTerminal::Unique,
                        CapabilityQueryTerminal::Paginate(_) | CapabilityQueryTerminal::Stream => {
                            unreachable!()
                        },
                    };
                    let request = match parse_query_stream_request(
                        capability_query_stream_args_with_terminal_limit(
                            &table,
                            source,
                            operators,
                            order,
                            canonical_query_terminal_limit(terminal),
                            npm_version,
                        ),
                    ) {
                        Ok(request) => request,
                        Err(error) => return Ok(Err(error)),
                    };
                    GeneratedAsyncOperation::Query {
                        query: request.query,
                        version: request.version,
                        table_name: table,
                        terminal,
                    }
                },
            }
        },
        AsyncCapabilityOperation::DatabaseInsert { table, value } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/insert".to_owned(),
                args: json!({
                    "table": table,
                    "value": value,
                }),
                normalization: GeneratedAsyncNormalization::InsertId,
            }
        },
        AsyncCapabilityOperation::DatabasePatch { table, id, patch } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/shallowMerge".to_owned(),
                args: json!({
                    "table": table,
                    "id": id,
                    "value": patch,
                }),
                normalization: GeneratedAsyncNormalization::Undefined,
            }
        },
        AsyncCapabilityOperation::DatabaseReplace { table, id, value } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/replace".to_owned(),
                args: json!({
                    "table": table,
                    "id": id,
                    "value": value,
                }),
                normalization: GeneratedAsyncNormalization::Undefined,
            }
        },
        AsyncCapabilityOperation::DatabaseDelete { table, id } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/remove".to_owned(),
                args: json!({
                    "table": table,
                    "id": id,
                }),
                normalization: GeneratedAsyncNormalization::Undefined,
            }
        },
        AsyncCapabilityOperation::StorageGetUrl { storage_id } => {
            GeneratedAsyncOperation::Syscall {
                name: "1.0/storageGetUrl".to_owned(),
                args: json!({ "storageId": storage_id }),
                normalization: GeneratedAsyncNormalization::Json,
            }
        },
        AsyncCapabilityOperation::StorageGetMetadata { storage_id } => {
            GeneratedAsyncOperation::Syscall {
                name: "1.0/storageGetMetadata".to_owned(),
                args: json!({ "storageId": storage_id }),
                normalization: GeneratedAsyncNormalization::Json,
            }
        },
        AsyncCapabilityOperation::StorageGenerateUploadUrl => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/storageGenerateUploadUrl".to_owned(),
                args: json!({}),
                normalization: GeneratedAsyncNormalization::Json,
            }
        },
        AsyncCapabilityOperation::StorageDelete { storage_id } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/storageDelete".to_owned(),
                args: json!({ "storageId": storage_id }),
                normalization: GeneratedAsyncNormalization::Undefined,
            }
        },
        AsyncCapabilityOperation::SchedulerRunAfter {
            delay_milliseconds,
            function_address,
            args,
        } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let Some(delay_milliseconds) = delay_milliseconds.as_f64() else {
                return Ok(Err(ErrorMetadata::bad_request(
                    "InvalidArgument",
                    "The scheduler time must be a number",
                )
                .into()));
            };
            let timestamp = match scheduler_run_after_timestamp(delay_milliseconds, unix_timestamp)
            {
                Ok(timestamp) => timestamp,
                Err(error) => return Ok(Err(error)),
            };
            GeneratedAsyncOperation::Syscall {
                name: "1.0/schedule".to_owned(),
                args: capability_scheduler_syscall_args(function_address, timestamp, args),
                normalization: GeneratedAsyncNormalization::PublicId,
            }
        },
        AsyncCapabilityOperation::SchedulerRunAt {
            timestamp_milliseconds,
            function_address,
            args,
        } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let Some(timestamp_milliseconds) = timestamp_milliseconds.as_f64() else {
                return Ok(Err(ErrorMetadata::bad_request(
                    "InvalidArgument",
                    "The scheduler time must be a number",
                )
                .into()));
            };
            if !timestamp_milliseconds.is_finite() {
                return Ok(Err(ErrorMetadata::bad_request(
                    "InvalidArgument",
                    "The scheduler time must be a finite number",
                )
                .into()));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/schedule".to_owned(),
                args: capability_scheduler_syscall_args(
                    function_address,
                    timestamp_milliseconds / 1000.0,
                    args,
                ),
                normalization: GeneratedAsyncNormalization::PublicId,
            }
        },
        AsyncCapabilityOperation::SchedulerCancel { id } => {
            if udf_type != UdfType::Mutation {
                return Err(WasmtimeError::new(HostInvariant));
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/cancel_job".to_owned(),
                args: json!({ "id": id }),
                normalization: GeneratedAsyncNormalization::Undefined,
            }
        },
        AsyncCapabilityOperation::RunUdf {
            udf_type: nested_udf_type,
            function_address,
            args,
            transaction_limits,
        } => {
            match (udf_type, nested_udf_type) {
                (UdfType::Query, CapabilityNestedUdfType::Query)
                | (
                    UdfType::Mutation,
                    CapabilityNestedUdfType::Query
                    | CapabilityNestedUdfType::Mutation
                    | CapabilityNestedUdfType::SnapshotQuery,
                ) => {},
                _ => return Err(WasmtimeError::new(HostInvariant)),
            }
            GeneratedAsyncOperation::Syscall {
                name: "1.0/runUdf".to_owned(),
                args: capability_run_udf_syscall_args(
                    nested_udf_type,
                    function_address,
                    args,
                    transaction_limits,
                ),
                normalization: GeneratedAsyncNormalization::Json,
            }
        },
    };
    Ok(Ok(operation))
}

fn capability_run_udf_syscall_args(
    udf_type: CapabilityNestedUdfType,
    function_address: CapabilityFunctionAddress,
    args: JsonValue,
    transaction_limits: Option<JsonValue>,
) -> JsonValue {
    let udf_type = match udf_type {
        CapabilityNestedUdfType::Query => "query",
        CapabilityNestedUdfType::Mutation => "mutation",
        CapabilityNestedUdfType::SnapshotQuery => "snapshotQuery",
    };
    let mut syscall_args = match function_address {
        CapabilityFunctionAddress::Name(name) => json!({
            "udfType": udf_type,
            "name": name,
            "args": args,
        }),
        CapabilityFunctionAddress::Reference(reference) => json!({
            "udfType": udf_type,
            "reference": reference,
            "args": args,
        }),
        CapabilityFunctionAddress::FunctionHandle(function_handle) => json!({
            "udfType": udf_type,
            "functionHandle": function_handle,
            "args": args,
        }),
    };
    if let Some(transaction_limits) = transaction_limits {
        syscall_args
            .as_object_mut()
            .expect("nested UDF syscall arguments must be an object")
            .insert("transactionLimits".to_owned(), transaction_limits);
    }
    syscall_args
}

fn capability_scheduler_syscall_args(
    function_address: CapabilityFunctionAddress,
    timestamp: f64,
    args: JsonValue,
) -> JsonValue {
    match function_address {
        CapabilityFunctionAddress::Name(name) => json!({
            "name": name,
            "ts": timestamp,
            "args": args,
        }),
        CapabilityFunctionAddress::Reference(reference) => json!({
            "reference": reference,
            "ts": timestamp,
            "args": args,
        }),
        CapabilityFunctionAddress::FunctionHandle(function_handle) => {
            json!({
                "functionHandle": function_handle,
                "ts": timestamp,
                "args": args,
            })
        },
    }
}

pub(super) fn capability_function_address_syscall_args(
    function_address: CapabilityFunctionAddress,
    npm_version: &Version,
) -> JsonValue {
    match function_address {
        CapabilityFunctionAddress::Name(name) => {
            json!({ "name": name, "version": npm_version.to_string() })
        },
        CapabilityFunctionAddress::Reference(reference) => {
            json!({ "reference": reference, "version": npm_version.to_string() })
        },
        CapabilityFunctionAddress::FunctionHandle(function_handle) => {
            json!({
                "functionHandle": function_handle,
                "version": npm_version.to_string(),
            })
        },
    }
}

fn normalize_generated_async_result(
    normalization: GeneratedAsyncNormalization,
    result: JsonValue,
) -> Result<JsonValue, WasmtimeError> {
    match normalization {
        GeneratedAsyncNormalization::Json => Ok(result),
        GeneratedAsyncNormalization::FunctionHandle => {
            if !result.is_string() {
                return Err(WasmtimeError::new(HostInvariant));
            }
            Ok(result)
        },
        GeneratedAsyncNormalization::InsertId => result
            .as_object()
            .and_then(|result| result.get("_id"))
            .and_then(JsonValue::as_str)
            .map(|id| JsonValue::String(id.to_owned()))
            .ok_or_else(|| WasmtimeError::new(HostInvariant)),
        GeneratedAsyncNormalization::PublicId => {
            if !result.is_string() {
                return Err(WasmtimeError::new(HostInvariant));
            }
            Ok(result)
        },
        GeneratedAsyncNormalization::Undefined => Err(WasmtimeError::new(HostInvariant)),
    }
}

fn normalize_generated_direct_async_batch_result(
    normalization: GeneratedAsyncNormalization,
    result: JsonValue,
) -> Result<GeneratedDirectAsyncBatchResult, WasmtimeError> {
    if matches!(normalization, GeneratedAsyncNormalization::Undefined) {
        return Ok(GeneratedDirectAsyncBatchResult::Undefined);
    }
    Ok(GeneratedDirectAsyncBatchResult::Json(
        normalize_generated_async_result(normalization, result)?,
    ))
}

pub(super) fn parse_query_next(result: JsonValue) -> Result<QueryNext, WasmtimeError> {
    let done = result
        .get("done")
        .and_then(JsonValue::as_bool)
        .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
    let value = result
        .get("value")
        .cloned()
        .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
    if done {
        if !value.is_null() {
            return Err(WasmtimeError::new(HostInvariant));
        }
        Ok(QueryNext::Done)
    } else {
        Ok(QueryNext::Value(value))
    }
}

fn cleanup_generated_async_queries<RT: Runtime>(
    state: &mut HostState<RT>,
    active_queries: &mut BTreeSet<QueryId>,
) {
    for query_id in std::mem::take(active_queries) {
        SyscallProviderInternal::cleanup_query(&mut state.provider, query_id);
    }
}

fn require_guest_promise_event_loop<RT: Runtime>(
    state: &HostState<RT>,
) -> Result<(), WasmtimeError> {
    if generated_state(state)?.manifest.effect_execution_mode()
        != EffectExecutionMode::GuestPromiseEventLoop
    {
        return Err(WasmtimeError::new(HostInvariant));
    }
    Ok(())
}

fn start_generated_async_operation<RT: Runtime>(
    state: &mut HostState<RT>,
    operation_id: i32,
    arguments_handle: i64,
) -> Result<i32, WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    let arguments = take_generated_json_value(state, arguments_handle)?;
    let JsonValue::Array(arguments) = arguments else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    let descriptor = generated_state_mut(state)?.operation(operation_id)?;
    let udf_type = state.provider.udf_type();
    let unix_timestamp = if imported_operation_observes_time(&descriptor) {
        state
            .provider
            .unix_timestamp()
            .map_err(|_| WasmtimeError::new(HostInvariant))?
    } else {
        state
            .provider
            .invocation_unix_timestamp()
            .map_err(|_| WasmtimeError::new(HostInvariant))?
    };
    let npm_version = state
        .provider
        .npm_version()
        .map_err(|_| WasmtimeError::new(HostInvariant))?
        .clone();
    let handle = match materialize_generated_async_operation(
        descriptor,
        arguments,
        udf_type,
        &npm_version,
        unix_timestamp,
    )? {
        Ok(operation) => generated_state_mut(state)?
            .async_operations
            .queue
            .enqueue(operation)?,
        Err(error) if error.is_deterministic_user_error() => generated_state_mut(state)?
            .async_operations
            .queue
            .enqueue(GeneratedAsyncOperation::Immediate(
                AsyncOperationCompletion::DeveloperError(error.short_msg().to_owned().into()),
            ))?,
        Err(_) => return Err(WasmtimeError::new(HostInvariant)),
    };
    Ok(handle.to_abi())
}

pub(super) fn start_generated_capability_operation<RT: Runtime>(
    state: &mut HostState<RT>,
    capability_handle: i64,
    request_handle: i64,
) -> Result<i32, WasmtimeError> {
    record_generated_host_call_mark(&state.metrics, "capability-start:entered");
    let metrics = Arc::clone(&state.metrics);
    require_guest_promise_event_loop(state)?;
    if !authorize_generated_capability(state, capability_handle)? {
        return Ok(-1);
    }
    let request_handle = opaque_handle(request_handle)?;
    let request = generated_state_mut(state)?
        .opaque_value_operation(|values| values.take_transferred_operand(request_handle))?;
    let OpaqueValue::CapabilityRequest(request) = request else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    let operation = generated_state(state)?
        .capability_bridge
        .decode_async(request.into_request())
        .map_err(|_| WasmtimeError::new(HostInvariant))?;
    let stage = match &operation {
        AsyncCapabilityOperation::AuthenticationGetUserIdentity => "start:auth",
        AsyncCapabilityOperation::AuditLog { .. } => "start:audit-log",
        AsyncCapabilityOperation::GetFunctionMetadata => "start:get-function-metadata",
        AsyncCapabilityOperation::GetDeploymentMetadata => "start:get-deployment-metadata",
        AsyncCapabilityOperation::GetTransactionMetrics => "start:get-transaction-metrics",
        AsyncCapabilityOperation::GetRequestMetadata => "start:get-request-metadata",
        AsyncCapabilityOperation::FunctionHandleCreate { .. } => "start:function-handle-create",
        AsyncCapabilityOperation::DatabaseGet {
            is_system: false, ..
        } => "start:db-get",
        AsyncCapabilityOperation::DatabaseGet {
            is_system: true, ..
        } => "start:db-system-get",
        AsyncCapabilityOperation::DatabaseQuery { .. } => "start:db-query",
        AsyncCapabilityOperation::DatabaseInsert { .. } => "start:db-insert",
        AsyncCapabilityOperation::DatabasePatch { .. } => "start:db-patch",
        AsyncCapabilityOperation::DatabaseReplace { .. } => "start:db-replace",
        AsyncCapabilityOperation::DatabaseDelete { .. } => "start:db-delete",
        AsyncCapabilityOperation::StorageGetUrl { .. } => "start:storage-get-url",
        AsyncCapabilityOperation::StorageGetMetadata { .. } => "start:storage-get-metadata",
        AsyncCapabilityOperation::StorageGenerateUploadUrl => "start:storage-generate-upload-url",
        AsyncCapabilityOperation::StorageDelete { .. } => "start:storage-delete",
        AsyncCapabilityOperation::SchedulerRunAfter { .. } => "start:scheduler-run-after",
        AsyncCapabilityOperation::SchedulerRunAt { .. } => "start:scheduler-run-at",
        AsyncCapabilityOperation::SchedulerCancel { .. } => "start:scheduler-cancel",
        AsyncCapabilityOperation::RunUdf {
            udf_type: CapabilityNestedUdfType::Query,
            ..
        } => "start:run-query",
        AsyncCapabilityOperation::RunUdf {
            udf_type: CapabilityNestedUdfType::Mutation,
            ..
        } => "start:run-mutation",
        AsyncCapabilityOperation::RunUdf {
            udf_type: CapabilityNestedUdfType::SnapshotQuery,
            ..
        } => "start:run-snapshot-query",
    };
    record_guest_native_completion_stage(&state.metrics, stage);
    let udf_type = state.provider.udf_type();
    let unix_timestamp = if capability_operation_observes_time(&operation) {
        state
            .provider
            .unix_timestamp()
            .map_err(|_| WasmtimeError::new(HostInvariant))?
    } else {
        state
            .provider
            .invocation_unix_timestamp()
            .map_err(|_| WasmtimeError::new(HostInvariant))?
    };
    let npm_version = state
        .provider
        .npm_version()
        .map_err(|_| WasmtimeError::new(HostInvariant))?
        .clone();
    let operation = match materialize_generated_capability_operation(
        operation,
        udf_type,
        &npm_version,
        unix_timestamp,
    )? {
        Ok(operation) => operation,
        Err(error) if error.is_deterministic_user_error() => GeneratedAsyncOperation::Immediate(
            AsyncOperationCompletion::DeveloperError(error.short_msg().to_owned().into()),
        ),
        Err(_) => return Err(WasmtimeError::new(HostInvariant)),
    };
    generated_state_mut(state)?.count_operation()?;
    generated_state_mut(state)?
        .async_operations
        .queue
        .enqueue(operation)
        .map(|handle| {
            record_generated_host_call_mark(&metrics, "capability-start:queued");
            handle.to_abi()
        })
}

pub(super) fn authorize_generated_capability<RT: Runtime>(
    state: &mut HostState<RT>,
    capability_handle: i64,
) -> Result<bool, WasmtimeError> {
    let Ok(capability_identity) = InvocationCapabilityIdentity::from_abi(capability_handle) else {
        return Ok(false);
    };
    if generated_state(state)?
        .capability_bridge
        .authorize(capability_identity)
        .is_ok()
    {
        return Ok(true);
    }
    // A nonzero rejected identity means retained guest state crossed an
    // invocation boundary. The guest may catch the rejection, but this
    // runtime must not return to the reuse pool. Authority is checked before
    // taking an operand so the rejected caller retains its value handle.
    generated_state_mut(state)?.runtime_reuse_contaminated = true;
    Ok(false)
}

fn generated_performance_now_milliseconds<RT: Runtime>(
    state: &mut HostState<RT>,
) -> Result<f64, WasmtimeError> {
    if !generated_state(state)?.performance_runtime_available {
        return Err(WasmtimeError::new(HostInvariant));
    }
    let elapsed = match state.provider.udf_type() {
        // Queries must remain deterministic within an invocation.
        UdfType::Query => Duration::ZERO,
        UdfType::Mutation => {
            let start = generated_state(state)?.performance_monotonic_start;
            state
                .provider
                .rt()
                .monotonic_now()
                .checked_duration_since(start)
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?
        },
        UdfType::Action | UdfType::HttpAction => {
            return Err(WasmtimeError::new(HostInvariant));
        },
    };
    state
        .provider
        .observe_time()
        .map_err(|_| WasmtimeError::new(HostInvariant))?;
    Ok(round_performance_duration_milliseconds(elapsed))
}

fn round_performance_duration_milliseconds(elapsed: Duration) -> f64 {
    (elapsed.as_secs_f64() * 10_000.0).floor() / 10.0
}

pub(super) fn run_generated_capability_sync_operation<RT: Runtime>(
    state: &mut HostState<RT>,
    capability_handle: i64,
    request_handle: i64,
) -> Result<i64, WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    let initialization_environment = if capability_handle == 0
        && generated_state(state)?
            .capability_bridge
            .allows_initialization_environment()
    {
        // Initialization deliberately exposes no invocation identity. Inspect
        // the request before consuming it so the zero-identity exception can
        // authorize only process.env, while every other request stays outside
        // the preparation capability.
        let request = generated_state(state)?
            .values
            .get(
                opaque_handle(request_handle)?,
                OpaqueValueKind::CapabilityRequest,
            )
            .map_err(|_| WasmtimeError::new(HostInvariant))?
            .clone();
        let OpaqueValue::CapabilityRequest(request) = request else {
            return Err(WasmtimeError::new(HostInvariant));
        };
        matches!(
            generated_state(state)?
                .capability_bridge
                .decode_sync(request.into_request())
                .map_err(|_| WasmtimeError::new(HostInvariant))?,
            SyncCapabilityOperation::EnvironmentVariableGet { .. }
        )
    } else {
        false
    };
    if !initialization_environment && !authorize_generated_capability(state, capability_handle)? {
        return Ok(-2);
    }
    let request_handle = opaque_handle(request_handle)?;
    let request = generated_state_mut(state)?
        .opaque_value_operation(|values| values.take_transferred_operand(request_handle))?;
    let OpaqueValue::CapabilityRequest(request) = request else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    let operation = generated_state(state)?
        .capability_bridge
        .decode_sync(request.into_request())
        .map_err(|_| WasmtimeError::new(HostInvariant))?;
    let stage = match &operation {
        SyncCapabilityOperation::DatabaseNormalizeId { .. } => "sync:db-normalize-id",
        SyncCapabilityOperation::EnvironmentVariableGet { .. } => "sync:environment-get",
        SyncCapabilityOperation::PerformanceNow => "sync:performance-now",
    };
    record_guest_native_completion_stage(&state.metrics, stage);
    if !matches!(
        state.provider.udf_type(),
        UdfType::Query | UdfType::Mutation
    ) {
        return Err(WasmtimeError::new(HostInvariant));
    }
    generated_state_mut(state)?.count_operation()?;
    let result = match operation {
        SyncCapabilityOperation::DatabaseNormalizeId { table, value } => {
            let result = syscall_impl(
                &mut state.provider,
                "1.0/db/normalizeId",
                json!({
                    "table": table,
                    "idString": value,
                }),
            );
            let Some(result) = classify_host_result(state, result)? else {
                return Ok(-1);
            };
            result
                .as_object()
                .and_then(|result| result.get("id"))
                .filter(|id| id.is_string() || id.is_null())
                .cloned()
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?
        },
        SyncCapabilityOperation::EnvironmentVariableGet { name } => {
            let result = name
                .parse::<EnvVarName>()
                .and_then(|name| state.provider.get_environment_variable(&name));
            match result {
                Ok(value) => json!({
                    "value": value.map(String::from),
                }),
                Err(error) if error.is_deterministic_user_error() => json!({
                    "error": error.short_msg(),
                }),
                Err(_) => return Err(WasmtimeError::new(HostInvariant)),
            }
        },
        SyncCapabilityOperation::PerformanceNow => {
            json!(generated_performance_now_milliseconds(state)?)
        },
    };
    generated_state_mut(state)?
        .opaque_value_operation(|values| values.insert_json(result))
        .map(OpaqueHandle::to_abi)
}

pub(super) fn current_generated_capability_identity<RT: Runtime>(
    state: &HostState<RT>,
) -> Result<i64, WasmtimeError> {
    // Invocation capability issuance is shared by blocking-fiber and
    // guest-promise executions. Reading the current identity does not access
    // the guest-promise queue, so both execution modes may use the
    // capability-protected randomness imports.
    generated_state(state)?
        .capability_bridge
        .handle()
        .map_err(|_| WasmtimeError::new(HostInvariant))
}

fn open_generated_async_query_stream<RT: Runtime>(
    state: &mut HostState<RT>,
    operation_id: i32,
    arguments_handle: i64,
) -> Result<i32, WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    let arguments = take_generated_json_value(state, arguments_handle)?;
    let JsonValue::Array(arguments) = arguments else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    let npm_version = state
        .provider
        .npm_version()
        .map_err(|_| WasmtimeError::new(HostInvariant))?
        .clone();
    let descriptor = generated_state_mut(state)?.operation(operation_id)?;
    let ImportedOperationDescriptor::DatabaseIndexQuery {
        terminal: QueryTerminal::Stream,
        ..
    } = &descriptor
    else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    let stream = match generated_query_syscall_args(&descriptor, arguments, &npm_version)? {
        Ok(args) => match parse_query_stream_request(args) {
            Ok(request) => GeneratedQueryStream::Pending {
                query: request.query,
                version: request.version,
            },
            Err(error) if error.is_deterministic_user_error() => GeneratedQueryStream::Immediate(
                AsyncOperationCompletion::DeveloperError(error.short_msg().to_owned().into()),
            ),
            Err(_) => return Err(WasmtimeError::new(HostInvariant)),
        },
        Err(error) if error.is_deterministic_user_error() => GeneratedQueryStream::Immediate(
            AsyncOperationCompletion::DeveloperError(error.short_msg().to_owned().into()),
        ),
        Err(_) => return Err(WasmtimeError::new(HostInvariant)),
    };
    generated_state_mut(state)?
        .async_operations
        .query_streams
        .open(stream)
        .map(QueryStreamHandle::to_abi)
}

fn open_generated_capability_query_stream<RT: Runtime>(
    state: &mut HostState<RT>,
    capability_handle: i64,
    request_handle: i64,
) -> Result<i32, WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    if !authorize_generated_capability(state, capability_handle)? {
        return Ok(-1);
    }
    let request_handle = opaque_handle(request_handle)?;
    let request = generated_state_mut(state)?
        .opaque_value_operation(|values| values.take_transferred_operand(request_handle))?;
    let OpaqueValue::CapabilityRequest(request) = request else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    let operation = generated_state(state)?
        .capability_bridge
        .decode_query_stream(request.into_request())
        .map_err(|_| WasmtimeError::new(HostInvariant))?;
    let udf_type = state.provider.udf_type();
    let npm_version = state
        .provider
        .npm_version()
        .map_err(|_| WasmtimeError::new(HostInvariant))?
        .clone();
    let stream = materialize_generated_capability_query_stream(operation, udf_type, &npm_version)?;
    record_guest_native_completion_stage(&state.metrics, "start:db-query-stream");
    generated_state_mut(state)?.count_operation()?;
    generated_state_mut(state)?
        .async_operations
        .query_streams
        .open(stream)
        .map(QueryStreamHandle::to_abi)
}

fn start_generated_async_query_stream_next<RT: Runtime>(
    state: &mut HostState<RT>,
    stream_handle: i32,
) -> Result<i32, WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    let operation = generated_state_mut(state)?
        .async_operations
        .query_streams
        .take_next(QueryStreamHandle::from_abi(stream_handle)?)?;
    generated_state_mut(state)?
        .async_operations
        .queue
        .enqueue(operation)
        .map(AsyncOperationHandle::to_abi)
}

fn close_generated_async_query_stream<RT: Runtime>(
    state: &mut HostState<RT>,
    stream_handle: i32,
) -> Result<(), WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    let query_id = generated_state_mut(state)?
        .async_operations
        .query_streams
        .close(QueryStreamHandle::from_abi(stream_handle)?)?;
    if let Some(query_id) = query_id {
        remove_generated_active_query(state, query_id, GeneratedQueryCleanup::CleanupProvider)?;
        record_generated_query_cleanup(state);
    }
    Ok(())
}

fn cancel_all_generated_async_operations<RT: Runtime>(
    state: &mut HostState<RT>,
) -> Result<i32, WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    // Root-Promise settlement is between host calls. Reject partial completion
    // observation, then let the guest compare this abandoned-handle count with
    // its own pending registrations before it releases those registrations.
    let abandoned_operation_count = generated_state(state)?
        .async_operations
        .queue
        .unsettled_count()?;

    // Query cursors borrow canonical transaction state. Remove them before
    // dropping the descriptors and retained completions that authenticate the
    // corresponding operations.
    let mut active_queries =
        std::mem::take(&mut generated_state_mut(state)?.async_operations.active_queries);
    for query_id in std::mem::take(&mut active_queries) {
        if !SyscallProviderInternal::cleanup_query(&mut state.provider, query_id) {
            return Err(WasmtimeError::new(HostInvariant));
        }
    }
    generated_state_mut(state)?.async_operations.clear();
    if state.provider.has_active_queries() {
        return Err(WasmtimeError::new(HostInvariant));
    }
    let abandoned_operation_count =
        i32::try_from(abandoned_operation_count).map_err(|_| WasmtimeError::new(HostInvariant))?;
    #[cfg(test)]
    {
        state
            .metrics
            .async_operation_cancel_all_calls
            .fetch_add(1, Ordering::SeqCst);
        state.metrics.async_operations_abandoned.fetch_add(
            usize::try_from(abandoned_operation_count)
                .expect("nonnegative async operation count stopped fitting usize"),
            Ordering::SeqCst,
        );
    }
    Ok(abandoned_operation_count)
}

fn generated_async_operation_syscall_name(operation: &GeneratedAsyncOperation) -> Option<&str> {
    match operation {
        GeneratedAsyncOperation::Immediate(_) => None,
        GeneratedAsyncOperation::Syscall { name, .. } => Some(name),
        GeneratedAsyncOperation::Query { .. }
        | GeneratedAsyncOperation::QueryContinuation { .. }
        | GeneratedAsyncOperation::QueryStreamStart { .. }
        | GeneratedAsyncOperation::QueryStreamContinuation { .. } => Some("1.0/queryStreamNext"),
    }
}

fn start_generated_query_stream<RT: Runtime>(
    state: &mut HostState<RT>,
    query: Query,
    version: Option<Version>,
) -> anyhow::Result<u32> {
    let trace_entry = SyscallProviderInternal::start_logical_host_operation(
        &mut state.provider,
        LogicalHostOperation::DatabaseQueryStream,
    );
    let result = state.provider.start_query(query, version);
    SyscallProviderInternal::complete_logical_host_operation(
        &mut state.provider,
        trace_entry,
        if result.is_ok() {
            LogicalHostOperationStatus::Success
        } else {
            LogicalHostOperationStatus::Failure
        },
    );
    result
}

fn prepare_generated_async_operation<RT: Runtime>(
    state: &mut HostState<RT>,
    operation: GeneratedAsyncOperation,
) -> Result<PreparedGeneratedAsyncOperation, WasmtimeError> {
    match operation {
        GeneratedAsyncOperation::Immediate(completion) => {
            Ok(PreparedGeneratedAsyncOperation::Completion(completion))
        },
        GeneratedAsyncOperation::Syscall {
            name,
            args,
            normalization,
        } => Ok(PreparedGeneratedAsyncOperation::Syscall {
            name,
            args,
            continuation: GeneratedAsyncContinuation::Complete(normalization),
        }),
        GeneratedAsyncOperation::Query {
            query,
            version,
            table_name,
            terminal,
        } => {
            generated_state_mut(state)?.count_operation()?;
            #[cfg(any(test, feature = "testing"))]
            state
                .metrics
                .database_query_starts
                .fetch_add(1, Ordering::SeqCst);
            let query_id = match start_generated_query_stream(state, query, version) {
                Ok(query_id) => query_id,
                Err(error) if error.is_deterministic_user_error() => {
                    return Ok(PreparedGeneratedAsyncOperation::Completion(
                        AsyncOperationCompletion::DeveloperError(
                            error.short_msg().to_owned().into(),
                        ),
                    ));
                },
                Err(_) => return Err(WasmtimeError::new(HostInvariant)),
            };
            if !generated_state_mut(state)?
                .async_operations
                .active_queries
                .insert(query_id)
            {
                SyscallProviderInternal::cleanup_query(&mut state.provider, query_id);
                return Err(WasmtimeError::new(HostInvariant));
            }
            Ok(PreparedGeneratedAsyncOperation::Syscall {
                name: "1.0/queryStreamNext".to_owned(),
                args: json!({ "queryId": query_id }),
                continuation: GeneratedAsyncContinuation::Query {
                    query_id,
                    table_name,
                    terminal,
                    values: Vec::new(),
                },
            })
        },
        GeneratedAsyncOperation::QueryContinuation {
            query_id,
            table_name,
            terminal,
            values,
        } => {
            if !generated_state(state)?
                .async_operations
                .active_queries
                .contains(&query_id)
            {
                return Err(WasmtimeError::new(HostInvariant));
            }
            generated_state_mut(state)?.count_operation()?;
            Ok(PreparedGeneratedAsyncOperation::Syscall {
                name: "1.0/queryStreamNext".to_owned(),
                args: json!({ "queryId": query_id }),
                continuation: GeneratedAsyncContinuation::Query {
                    query_id,
                    table_name,
                    terminal,
                    values,
                },
            })
        },
        GeneratedAsyncOperation::QueryStreamStart {
            stream_handle,
            query,
            version,
        } => {
            generated_state_mut(state)?.count_operation()?;
            #[cfg(any(test, feature = "testing"))]
            state
                .metrics
                .database_query_starts
                .fetch_add(1, Ordering::SeqCst);
            let query_id = match start_generated_query_stream(state, query, version) {
                Ok(query_id) => query_id,
                Err(error) if error.is_deterministic_user_error() => {
                    return Ok(PreparedGeneratedAsyncOperation::Completion(
                        AsyncOperationCompletion::DeveloperError(
                            error.short_msg().to_owned().into(),
                        ),
                    ));
                },
                Err(_) => return Err(WasmtimeError::new(HostInvariant)),
            };
            if !generated_state_mut(state)?
                .async_operations
                .active_queries
                .insert(query_id)
            {
                SyscallProviderInternal::cleanup_query(&mut state.provider, query_id);
                return Err(WasmtimeError::new(HostInvariant));
            }
            Ok(PreparedGeneratedAsyncOperation::Syscall {
                name: "1.0/queryStreamNext".to_owned(),
                args: json!({ "queryId": query_id }),
                continuation: GeneratedAsyncContinuation::QueryStream {
                    stream_handle,
                    query_id,
                },
            })
        },
        GeneratedAsyncOperation::QueryStreamContinuation {
            stream_handle,
            query_id,
        } => {
            if !generated_state(state)?
                .async_operations
                .active_queries
                .contains(&query_id)
            {
                return Err(WasmtimeError::new(HostInvariant));
            }
            generated_state_mut(state)?.count_operation()?;
            Ok(PreparedGeneratedAsyncOperation::Syscall {
                name: "1.0/queryStreamNext".to_owned(),
                args: json!({ "queryId": query_id }),
                continuation: GeneratedAsyncContinuation::QueryStream {
                    stream_handle,
                    query_id,
                },
            })
        },
    }
}

fn remove_generated_active_query<RT: Runtime>(
    state: &mut HostState<RT>,
    query_id: QueryId,
    cleanup: GeneratedQueryCleanup,
) -> Result<(), WasmtimeError> {
    if matches!(cleanup, GeneratedQueryCleanup::CleanupProvider) {
        SyscallProviderInternal::cleanup_query(&mut state.provider, query_id);
    }
    if !generated_state_mut(state)?
        .async_operations
        .active_queries
        .remove(&query_id)
    {
        return Err(WasmtimeError::new(HostInvariant));
    }
    Ok(())
}

/// Query termination is a synchronous `queryCleanup` syscall in the V8
/// implementation. Generated guests perform the provider cleanup directly, so
/// record the equivalent logical operation after the query cursor has been
/// removed. Abort and cancellation cleanup deliberately do not call this: V8
/// does not issue `queryCleanup` after a failed `queryStreamNext`.
fn record_generated_query_cleanup<RT: Runtime>(state: &mut HostState<RT>) {
    let trace_entry = SyscallProviderInternal::start_logical_host_operation(
        &mut state.provider,
        LogicalHostOperation::DatabaseQueryCleanup,
    );
    SyscallProviderInternal::complete_logical_host_operation(
        &mut state.provider,
        trace_entry,
        LogicalHostOperationStatus::Success,
    );
}

fn complete_generated_query_terminal(
    terminal: QueryTerminal,
    table_name: &str,
    mut values: Vec<JsonValue>,
) -> Result<AsyncOperationCompletion, WasmtimeError> {
    let result = match terminal {
        QueryTerminal::Collect => {
            return Ok(AsyncOperationCompletion::Success(Some(JsonValue::Array(
                values,
            ))));
        },
        QueryTerminal::First => values.into_iter().next().unwrap_or(JsonValue::Null),
        QueryTerminal::Unique => match values.len() {
            0 => JsonValue::Null,
            1 => values.pop().expect("single unique result disappeared"),
            _ => {
                let first_id = values
                    .first()
                    .and_then(|value| value.get("_id"))
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
                let second_id = values
                    .get(1)
                    .and_then(|value| value.get("_id"))
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
                return Ok(AsyncOperationCompletion::DeveloperError(
                    format!(
                        "unique() query returned more than one result from table {table_name}:\n \
                         [{first_id}, {second_id}, ...]"
                    )
                    .into(),
                ));
            },
        },
        QueryTerminal::Stream => return Err(WasmtimeError::new(HostInvariant)),
    };
    Ok(AsyncOperationCompletion::Success(Some(result)))
}

fn cleanup_generated_event_loop_queries<RT: Runtime>(
    state: &mut HostState<RT>,
) -> Result<(), WasmtimeError> {
    let mut active_queries =
        std::mem::take(&mut generated_state_mut(state)?.async_operations.active_queries);
    cleanup_generated_async_queries(state, &mut active_queries);
    Ok(())
}

fn settle_generated_async_syscall<RT: Runtime>(
    state: &mut HostState<RT>,
    continuation: GeneratedAsyncContinuation,
    outcome: GeneratedAsyncSyscallOutcome,
) -> Result<SettledGeneratedAsyncOperation, WasmtimeError> {
    match continuation {
        GeneratedAsyncContinuation::Complete(normalization) => match outcome {
            GeneratedAsyncSyscallOutcome::Success(_)
                if matches!(normalization, GeneratedAsyncNormalization::Undefined) =>
            {
                Ok(SettledGeneratedAsyncOperation::Complete(
                    AsyncOperationCompletion::Success(None),
                ))
            },
            GeneratedAsyncSyscallOutcome::Success(result) => Ok(
                SettledGeneratedAsyncOperation::Complete(AsyncOperationCompletion::Success(Some(
                    normalize_generated_async_result(normalization, result)?,
                ))),
            ),
            GeneratedAsyncSyscallOutcome::DeveloperError(error) => {
                Ok(SettledGeneratedAsyncOperation::Complete(
                    AsyncOperationCompletion::DeveloperError(error),
                ))
            },
        },
        GeneratedAsyncContinuation::Query {
            query_id,
            table_name,
            terminal,
            mut values,
        } => {
            let result = match outcome {
                GeneratedAsyncSyscallOutcome::Success(result) => result,
                GeneratedAsyncSyscallOutcome::DeveloperError(error) => {
                    remove_generated_active_query(
                        state,
                        query_id,
                        GeneratedQueryCleanup::CleanupProvider,
                    )?;
                    return Ok(SettledGeneratedAsyncOperation::Complete(
                        AsyncOperationCompletion::DeveloperError(error),
                    ));
                },
            };
            match parse_query_next(result)? {
                QueryNext::Done => {
                    remove_generated_active_query(
                        state,
                        query_id,
                        GeneratedQueryCleanup::ProviderAlreadyCleaned,
                    )?;
                    record_generated_query_cleanup(state);
                    Ok(SettledGeneratedAsyncOperation::Complete(
                        complete_generated_query_terminal(terminal, &table_name, values)?,
                    ))
                },
                QueryNext::Value(value) => match terminal {
                    QueryTerminal::Collect | QueryTerminal::First | QueryTerminal::Unique => {
                        values.push(value);
                        Ok(SettledGeneratedAsyncOperation::Requeue(
                            GeneratedAsyncOperation::QueryContinuation {
                                query_id,
                                table_name,
                                terminal,
                                values,
                            },
                        ))
                    },
                    QueryTerminal::Stream => Err(WasmtimeError::new(HostInvariant)),
                },
            }
        },
        GeneratedAsyncContinuation::QueryStream {
            stream_handle,
            query_id,
        } => {
            let result = match outcome {
                GeneratedAsyncSyscallOutcome::Success(result) => result,
                GeneratedAsyncSyscallOutcome::DeveloperError(error) => {
                    remove_generated_active_query(
                        state,
                        query_id,
                        GeneratedQueryCleanup::CleanupProvider,
                    )?;
                    return Ok(SettledGeneratedAsyncOperation::Complete(
                        AsyncOperationCompletion::DeveloperError(error),
                    ));
                },
            };
            match parse_query_next(result)? {
                QueryNext::Done => {
                    remove_generated_active_query(
                        state,
                        query_id,
                        GeneratedQueryCleanup::ProviderAlreadyCleaned,
                    )?;
                    record_generated_query_cleanup(state);
                    Ok(SettledGeneratedAsyncOperation::Complete(
                        AsyncOperationCompletion::Success(Some(json!({
                            "done": true,
                            "value": null,
                        }))),
                    ))
                },
                QueryNext::Value(value) => {
                    generated_state_mut(state)?
                        .async_operations
                        .query_streams
                        .complete_value(stream_handle, query_id)?;
                    Ok(SettledGeneratedAsyncOperation::Complete(
                        AsyncOperationCompletion::Success(Some(json!({
                            "done": false,
                            "value": value,
                        }))),
                    ))
                },
            }
        },
    }
}

// Each wait advances a compatible syscall wave before it starts the next
// incompatible pending syscall. The runtime announces every completion
// settled by that wave before its next microtask checkpoint, so guest
// continuations enqueue their follow-up work in promise order.
async fn wait_for_generated_async_operation<RT: Runtime>(
    state: &mut HostState<RT>,
) -> Result<i32, WasmtimeError> {
    record_generated_host_call_mark(&state.metrics, "async-wait:entered");
    require_guest_promise_event_loop(state)?;
    let outcome = async {
        loop {
            if let Some(handle) = generated_state_mut(state)?
                .async_operations
                .queue
                .announce_completion()?
            {
                record_generated_host_call_mark(&state.metrics, "async-wait:announced");
                return Ok(handle.to_abi());
            }

            let (handle, operation) = generated_state_mut(state)?
                .async_operations
                .queue
                .pop_pending()
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
            record_generated_host_call_mark(&state.metrics, "async-wait:dequeued");
            let prepared = prepare_generated_async_operation(state, operation)?;
            let PreparedGeneratedAsyncOperation::Syscall {
                name,
                args,
                continuation,
            } = prepared
            else {
                let PreparedGeneratedAsyncOperation::Completion(completion) = prepared else {
                    return Err(WasmtimeError::new(HostInvariant));
                };
                generated_state_mut(state)?
                    .async_operations
                    .queue
                    .complete(handle, completion)?;
                continue;
            };

            let mut batch = AsyncSyscallBatch::new(name, args);
            let mut entries = vec![GeneratedQueuedAsyncSyscall {
                handle,
                continuation,
            }];
            let mut deferred_completion = None;
            loop {
                let can_push = generated_state(state)?
                    .async_operations
                    .queue
                    .front_pending()
                    .and_then(|(_, operation)| generated_async_operation_syscall_name(operation))
                    .is_some_and(|name| batch.can_push(name, &JsonValue::Null));
                if !can_push {
                    break;
                }
                let (handle, operation) = generated_state_mut(state)?
                    .async_operations
                    .queue
                    .pop_pending()
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
                match prepare_generated_async_operation(state, operation)? {
                    PreparedGeneratedAsyncOperation::Completion(completion) => {
                        deferred_completion = Some((handle, completion));
                        break;
                    },
                    PreparedGeneratedAsyncOperation::Syscall {
                        name,
                        args,
                        continuation,
                    } => {
                        batch
                            .push(name, args)
                            .map_err(|_| WasmtimeError::new(HostInvariant))?;
                        entries.push(GeneratedQueuedAsyncSyscall {
                            handle,
                            continuation,
                        });
                    },
                }
            }

            let outcomes = execute_generated_async_syscall_batch(state, batch).await?;
            record_generated_host_call_mark(&state.metrics, "async-wait:settled");
            if outcomes.len() != entries.len() {
                return Err(WasmtimeError::new(HostInvariant));
            }
            for (entry, outcome) in entries.into_iter().zip(outcomes) {
                let GeneratedQueuedAsyncSyscall {
                    handle,
                    continuation,
                    ..
                } = entry;
                match settle_generated_async_syscall(state, continuation, outcome)? {
                    SettledGeneratedAsyncOperation::Complete(completion) => {
                        generated_state_mut(state)?
                            .async_operations
                            .queue
                            .complete(handle, completion)?
                    },
                    SettledGeneratedAsyncOperation::Requeue(operation) => {
                        generated_state_mut(state)?
                            .async_operations
                            .queue
                            .requeue(handle, operation)?;
                    },
                }
            }
            if let Some((handle, completion)) = deferred_completion {
                generated_state_mut(state)?
                    .async_operations
                    .queue
                    .complete(handle, completion)?;
            }
        }
    }
    .await;
    if outcome.is_err() {
        cleanup_generated_event_loop_queries(state)?;
    }
    outcome
}

fn generated_async_operation_completion_status<RT: Runtime>(
    state: &mut HostState<RT>,
    handle: i32,
) -> Result<i32, WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    let handle = AsyncOperationHandle::from_abi(handle)?;
    let completion = generated_state_mut(state)?
        .async_operations
        .queue
        .observe_completion_status(handle)?;
    let (status, stage) = match completion {
        AsyncOperationCompletion::Success(_) => (0, "completion-status:success"),
        AsyncOperationCompletion::DeveloperError(_) => (1, "completion-status:developer-error"),
    };
    record_guest_native_completion_stage(&state.metrics, stage);
    Ok(status)
}

/// Announces the next completion already settled by a previous wait. Unlike
/// `convex_async_operation_wait_any`, this must not start pending work: the
/// runtime uses it to settle a whole completed syscall wave before draining
/// guest microtasks.
fn poll_generated_async_operation_ready<RT: Runtime>(
    state: &mut HostState<RT>,
) -> Result<i32, WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    let Some(handle) = generated_state_mut(state)?
        .async_operations
        .queue
        .announce_completion()?
    else {
        return Ok(0);
    };
    Ok(handle.to_abi())
}

fn take_generated_async_operation_completion<RT: Runtime>(
    state: &mut HostState<RT>,
    handle: i32,
) -> Result<i64, WasmtimeError> {
    require_guest_promise_event_loop(state)?;
    let handle = AsyncOperationHandle::from_abi(handle)?;
    let completion = generated_state_mut(state)?
        .async_operations
        .queue
        .take_completion(handle)?;
    let (result, stage) = match completion {
        AsyncOperationCompletion::Success(None) => (Ok(0), "completion-take:none"),
        AsyncOperationCompletion::Success(Some(value)) => (
            generated_state_mut(state)?
                .opaque_value_operation(|values| values.insert_json(value))
                .map(OpaqueHandle::to_abi),
            "completion-take:json",
        ),
        AsyncOperationCompletion::DeveloperError(error) => {
            if let Some(host_operation_error) = error.host_operation_error {
                generated_state_mut(state)?
                    .async_operations
                    .record_host_operation_error(handle, host_operation_error)?;
            }
            (
                generated_state_mut(state)?
                    .opaque_value_operation(|values| values.insert_string(error.message))
                    .map(OpaqueHandle::to_abi),
                "completion-take:developer-error",
            )
        },
    };
    record_guest_native_completion_stage(&state.metrics, stage);
    result
}

pub(super) fn add_generated_async_operation_imports<RT: Runtime>(
    linker: &mut Linker<HostState<RT>>,
) -> Result<(), WasmtimeError> {
    linker.func_wrap(
        "convex",
        "convex_async_query_stream_open_take",
        |mut caller: Caller<'_, HostState<RT>>, operation_id: i32, arguments_handle: i64| {
            open_generated_async_query_stream(caller.data_mut(), operation_id, arguments_handle)
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_async_query_stream_next",
        |mut caller: Caller<'_, HostState<RT>>, stream_handle: i32| {
            start_generated_async_query_stream_next(caller.data_mut(), stream_handle)
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_async_query_stream_close",
        |mut caller: Caller<'_, HostState<RT>>, stream_handle: i32| {
            close_generated_async_query_stream(caller.data_mut(), stream_handle)
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_async_operation_start_take",
        |mut caller: Caller<'_, HostState<RT>>, operation_id: i32, arguments_handle: i64| {
            start_generated_async_operation(caller.data_mut(), operation_id, arguments_handle)
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_capability_current",
        |caller: Caller<'_, HostState<RT>>| current_generated_capability_identity(caller.data()),
    )?;
    linker.func_wrap(
        "convex",
        "convex_capability_start_take",
        |mut caller: Caller<'_, HostState<RT>>, capability_handle: i64, request_handle: i64| {
            start_generated_capability_operation(
                caller.data_mut(),
                capability_handle,
                request_handle,
            )
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_capability_query_stream_open_take",
        |mut caller: Caller<'_, HostState<RT>>, capability_handle: i64, request_handle: i64| {
            open_generated_capability_query_stream(
                caller.data_mut(),
                capability_handle,
                request_handle,
            )
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_capability_sync_take",
        |mut caller: Caller<'_, HostState<RT>>, capability_handle: i64, request_handle: i64| {
            run_generated_capability_sync_operation(
                caller.data_mut(),
                capability_handle,
                request_handle,
            )
        },
    )?;
    linker.func_wrap_async(
        "convex",
        "convex_async_operation_wait_any",
        |mut caller: Caller<'_, HostState<RT>>, (): ()| {
            Box::new(async move { wait_for_generated_async_operation(caller.data_mut()).await })
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_async_operation_poll_ready",
        |mut caller: Caller<'_, HostState<RT>>| {
            poll_generated_async_operation_ready(caller.data_mut())
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_async_operation_completion_status",
        |mut caller: Caller<'_, HostState<RT>>, handle: i32| {
            generated_async_operation_completion_status(caller.data_mut(), handle)
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_async_operation_completion_take",
        |mut caller: Caller<'_, HostState<RT>>, handle: i32| {
            take_generated_async_operation_completion(caller.data_mut(), handle)
        },
    )?;
    linker.func_wrap(
        "convex",
        "convex_async_operation_cancel_all",
        |mut caller: Caller<'_, HostState<RT>>| {
            cancel_all_generated_async_operations(caller.data_mut())
        },
    )?;
    Ok(())
}

pub(super) async fn run_generated_direct_async_batch<RT: Runtime>(
    state: &mut HostState<RT>,
    invocations_handle: i64,
) -> Result<Option<JsonValue>, WasmtimeError> {
    let invocations = take_generated_json_value(state, invocations_handle)?;
    let invocations: Vec<GeneratedAsyncBatchInvocation> =
        serde_json::from_value(invocations).map_err(|_| WasmtimeError::new(HostInvariant))?;
    if invocations.is_empty() {
        return Ok(Some(JsonValue::Array(Vec::new())));
    }
    let descriptors = generated_state_mut(state)?
        .operations(invocations.iter().map(|invocation| invocation.operation_id))?;

    let udf_type = state.provider.udf_type();
    let unix_timestamp = if descriptors.iter().any(imported_operation_observes_time) {
        state
            .provider
            .unix_timestamp()
            .map_err(|_| WasmtimeError::new(HostInvariant))?
    } else {
        state
            .provider
            .invocation_unix_timestamp()
            .map_err(|_| WasmtimeError::new(HostInvariant))?
    };
    let npm_version = state
        .provider
        .npm_version()
        .map_err(|_| WasmtimeError::new(HostInvariant))?
        .clone();

    // Resolve the complete wire and every descriptor before any query is
    // registered or transaction syscall is executed.
    let mut operations = Vec::with_capacity(invocations.len());
    let mut first_developer_error = None;
    for (source_index, (descriptor, invocation)) in
        descriptors.into_iter().zip(invocations).enumerate()
    {
        match materialize_generated_async_operation(
            descriptor,
            invocation.arguments,
            udf_type,
            &npm_version,
            unix_timestamp,
        )? {
            Ok(operation) => operations.push((source_index, operation)),
            Err(error) if error.is_deterministic_user_error() => {
                if first_developer_error.is_none() {
                    first_developer_error = Some((source_index, error.short_msg().to_owned()));
                }
            },
            Err(_) => return Err(WasmtimeError::new(HostInvariant)),
        }
    }
    if let Some((source_index, message)) = first_developer_error {
        record_generated_developer_error(state, source_index, message)?;
        return Ok(None);
    }

    let operation_count = operations.len();
    let mut pending = VecDeque::with_capacity(operation_count);
    let mut active_queries = BTreeSet::new();
    for (source_index, operation) in operations {
        match operation {
            GeneratedAsyncOperation::Immediate(_) => {
                return Err(WasmtimeError::new(HostInvariant));
            },
            GeneratedAsyncOperation::Syscall {
                name,
                args,
                normalization,
            } => pending.push_back(GeneratedPendingAsyncSyscall {
                source_index,
                name,
                args,
                continuation: GeneratedAsyncContinuation::Complete(normalization),
            }),
            GeneratedAsyncOperation::Query {
                query,
                version,
                table_name,
                terminal,
            } => {
                if let Err(error) =
                    generated_state_mut(state).and_then(GeneratedInvocationState::count_operation)
                {
                    cleanup_generated_async_queries(state, &mut active_queries);
                    return Err(error);
                }
                #[cfg(any(test, feature = "testing"))]
                state
                    .metrics
                    .database_query_starts
                    .fetch_add(1, Ordering::SeqCst);
                let start_result = start_generated_query_stream(state, query, version);
                let query_id = match classify_host_result(state, start_result) {
                    Ok(Some(query_id)) => query_id,
                    Ok(None) => {
                        cleanup_generated_async_queries(state, &mut active_queries);
                        #[cfg(test)]
                        state
                            .metrics
                            .first_developer_error_index
                            .store(source_index + 1, Ordering::SeqCst);
                        return Ok(None);
                    },
                    Err(error) => {
                        cleanup_generated_async_queries(state, &mut active_queries);
                        return Err(error);
                    },
                };
                active_queries.insert(query_id);
                pending.push_back(GeneratedPendingAsyncSyscall {
                    source_index,
                    name: "1.0/queryStreamNext".to_owned(),
                    args: json!({ "queryId": query_id }),
                    continuation: GeneratedAsyncContinuation::Query {
                        query_id,
                        table_name,
                        terminal,
                        values: Vec::new(),
                    },
                });
            },
            GeneratedAsyncOperation::QueryContinuation { .. }
            | GeneratedAsyncOperation::QueryStreamStart { .. }
            | GeneratedAsyncOperation::QueryStreamContinuation { .. } => {
                return Err(WasmtimeError::new(HostInvariant));
            },
        }
    }

    let outcome = async {
        let mut results = (0..operation_count).map(|_| None).collect::<Vec<_>>();
        while let Some(GeneratedPendingAsyncSyscall {
            source_index,
            name,
            args,
            continuation,
        }) = pending.pop_front()
        {
            let mut batch = AsyncSyscallBatch::new(name, args);
            let mut entries = vec![(source_index, continuation)];
            while let Some(next) = pending.front() {
                if !batch.can_push(&next.name, &next.args) {
                    break;
                }
                let GeneratedPendingAsyncSyscall {
                    source_index,
                    name,
                    args,
                    continuation,
                } = pending
                    .pop_front()
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
                batch
                    .push(name, args)
                    .map_err(|_| WasmtimeError::new(HostInvariant))?;
                entries.push((source_index, continuation));
            }

            let batch_outcome = match execute_generated_async_syscall_batch(state, batch).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    cleanup_generated_async_queries(state, &mut active_queries);
                    return Err(error);
                },
            };
            if batch_outcome.len() != entries.len() {
                cleanup_generated_async_queries(state, &mut active_queries);
                return Err(WasmtimeError::new(HostInvariant));
            }

            for ((source_index, continuation), result) in entries.into_iter().zip(batch_outcome) {
                let result = match result {
                    GeneratedAsyncSyscallOutcome::Success(result) => result,
                    GeneratedAsyncSyscallOutcome::DeveloperError(error) => {
                        cleanup_generated_async_queries(state, &mut active_queries);
                        record_generated_developer_error(state, source_index, error)?;
                        return Ok(None);
                    },
                };
                match continuation {
                    GeneratedAsyncContinuation::Complete(normalization) => {
                        results[source_index] = Some(
                            normalize_generated_direct_async_batch_result(normalization, result)?,
                        );
                    },
                    GeneratedAsyncContinuation::Query {
                        query_id,
                        table_name,
                        terminal,
                        mut values,
                    } => match parse_query_next(result)? {
                        QueryNext::Done => {
                            if !active_queries.remove(&query_id) {
                                return Err(WasmtimeError::new(HostInvariant));
                            }
                            record_generated_query_cleanup(state);
                            match complete_generated_query_terminal(terminal, &table_name, values)?
                            {
                                AsyncOperationCompletion::Success(Some(result)) => {
                                    results[source_index] =
                                        Some(GeneratedDirectAsyncBatchResult::Json(result));
                                },
                                AsyncOperationCompletion::DeveloperError(error) => {
                                    cleanup_generated_async_queries(state, &mut active_queries);
                                    record_generated_developer_error(state, source_index, error)?;
                                    return Ok(None);
                                },
                                AsyncOperationCompletion::Success(None) => {
                                    return Err(WasmtimeError::new(HostInvariant));
                                },
                            }
                        },
                        QueryNext::Value(value) => match terminal {
                            QueryTerminal::Collect
                            | QueryTerminal::First
                            | QueryTerminal::Unique => {
                                values.push(value);
                                generated_state_mut(state)?.count_operation()?;
                                pending.push_back(GeneratedPendingAsyncSyscall {
                                    source_index,
                                    name: "1.0/queryStreamNext".to_owned(),
                                    args: json!({ "queryId": query_id }),
                                    continuation: GeneratedAsyncContinuation::Query {
                                        query_id,
                                        table_name,
                                        terminal,
                                        values,
                                    },
                                });
                            },
                            QueryTerminal::Stream => {
                                return Err(WasmtimeError::new(HostInvariant));
                            },
                        },
                    },
                    GeneratedAsyncContinuation::QueryStream { .. } => {
                        return Err(WasmtimeError::new(HostInvariant));
                    },
                }
            }
        }
        if !active_queries.is_empty() {
            return Err(WasmtimeError::new(HostInvariant));
        }
        Ok(Some(JsonValue::Array(
            results
                .into_iter()
                .map(|result| result.ok_or_else(|| WasmtimeError::new(HostInvariant)))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(GeneratedDirectAsyncBatchResult::into_json_array_value)
                .collect(),
        )))
    }
    .await;
    cleanup_generated_async_queries(state, &mut active_queries);
    outcome
}

#[cfg(test)]
#[test]
fn async_operation_queue_requeues_before_later_pending_operations() -> anyhow::Result<()> {
    let mut queue = AsyncOperationQueue::<&str, &str>::default();
    let query = queue.enqueue("query")?;
    let later_effect = queue.enqueue("later-effect")?;
    assert_eq!(query.to_abi(), 1);
    assert_eq!(later_effect.to_abi(), 2);

    assert_eq!(queue.pop_pending(), Some((query, "query")));
    queue.requeue(query, "query-continuation")?;
    assert_eq!(queue.pop_pending(), Some((query, "query-continuation")));
    queue.complete(query, "query-result")?;
    assert_eq!(queue.pop_pending(), Some((later_effect, "later-effect")));
    queue.complete(later_effect, "later-effect-result")?;

    assert_eq!(queue.announce_completion()?, Some(query));
    assert_eq!(queue.observe_completion_status(query)?, &"query-result");
    assert_eq!(queue.take_completion(query)?, "query-result");
    assert_eq!(queue.announce_completion()?, Some(later_effect));
    assert_eq!(
        queue.observe_completion_status(later_effect)?,
        &"later-effect-result"
    );
    assert_eq!(queue.take_completion(later_effect)?, "later-effect-result");
    assert!(queue.is_empty());
    Ok(())
}

#[cfg(test)]
#[test]
fn async_operation_queue_polls_ready_completions_without_advancing_pending() -> anyhow::Result<()> {
    let mut queue = AsyncOperationQueue::<&str, &str>::default();
    let first = queue.enqueue("first")?;
    let second = queue.enqueue("second")?;
    let later = queue.enqueue("later")?;

    assert_eq!(queue.pop_pending(), Some((first, "first")));
    assert_eq!(queue.pop_pending(), Some((second, "second")));
    queue.complete(first, "first-result")?;
    queue.complete(second, "second-result")?;

    assert_eq!(queue.announce_completion()?, Some(first));
    assert_eq!(queue.observe_completion_status(first)?, &"first-result");
    assert_eq!(queue.take_completion(first)?, "first-result");
    assert_eq!(queue.announce_completion()?, Some(second));
    assert_eq!(queue.observe_completion_status(second)?, &"second-result");
    assert_eq!(queue.take_completion(second)?, "second-result");
    assert_eq!(queue.announce_completion()?, None);

    assert_eq!(queue.pop_pending(), Some((later, "later")));
    Ok(())
}

#[cfg(test)]
#[test]
fn query_stream_handles_enforce_single_flight_and_terminal_ownership() -> anyhow::Result<()> {
    let query_id = QueryId::try_from(17_u64)?;
    let mut streams = GeneratedQueryStreams::default();
    let handle = streams.open(GeneratedQueryStream::Active(query_id))?;
    assert_eq!(handle.to_abi(), 1);

    assert!(matches!(
        streams.take_next(handle)?,
        GeneratedAsyncOperation::QueryStreamContinuation {
            stream_handle,
            query_id: actual_query_id,
        } if stream_handle == handle && actual_query_id == query_id
    ));
    assert!(streams.take_next(handle).is_err());
    assert!(streams.close(handle).is_err());

    streams.complete_value(handle, query_id)?;
    assert_eq!(streams.close(handle)?, Some(query_id));
    assert!(streams.close(handle).is_err());
    assert!(streams.is_empty());

    let failed = streams.open(GeneratedQueryStream::Immediate(
        AsyncOperationCompletion::DeveloperError("rejected".to_owned().into()),
    ))?;
    assert_eq!(failed.to_abi(), 2);
    assert!(matches!(
        streams.take_next(failed)?,
        GeneratedAsyncOperation::Immediate(AsyncOperationCompletion::DeveloperError(error))
            if error.message == "rejected"
    ));
    assert!(streams.close(failed).is_err());
    assert!(streams.is_empty());
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelQueryCompletion {
    Success(usize),
    DeveloperError(usize),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
struct ModelQueryContinuation {
    source_index: usize,
    remaining_values: u8,
    error_after_values: Option<u8>,
    observed_values: u8,
}

#[cfg(test)]
fn run_model_query_waves(
    specifications: &[(u8, Option<u8>)],
    maximum_batch_size: usize,
) -> anyhow::Result<(
    Vec<ModelQueryCompletion>,
    Vec<Vec<usize>>,
    AsyncOperationQueue<ModelQueryContinuation, ModelQueryCompletion>,
)> {
    anyhow::ensure!(maximum_batch_size > 0);
    let mut queue = AsyncOperationQueue::default();
    let mut active = BTreeSet::new();
    for (source_index, (remaining_values, error_after_values)) in
        specifications.iter().copied().enumerate()
    {
        let handle = queue.enqueue(ModelQueryContinuation {
            source_index,
            remaining_values,
            error_after_values,
            observed_values: 0,
        })?;
        anyhow::ensure!(active.insert(handle));
    }

    let mut waves = Vec::new();
    while queue.has_pending() {
        let mut entries = Vec::new();
        while entries.len() < maximum_batch_size {
            let Some(entry) = queue.pop_pending() else {
                break;
            };
            entries.push(entry);
        }
        let mut wave = Vec::with_capacity(entries.len());
        let mut continuations = Vec::new();
        for (handle, mut continuation) in entries {
            wave.push(continuation.source_index);
            if continuation.error_after_values == Some(continuation.observed_values) {
                anyhow::ensure!(active.remove(&handle));
                queue.complete(
                    handle,
                    ModelQueryCompletion::DeveloperError(continuation.source_index),
                )?;
            } else if continuation.remaining_values == 0 {
                anyhow::ensure!(active.remove(&handle));
                queue.complete(
                    handle,
                    ModelQueryCompletion::Success(continuation.source_index),
                )?;
            } else {
                continuation.remaining_values -= 1;
                continuation.observed_values += 1;
                continuations.push((handle, continuation));
            }
        }
        for (handle, continuation) in continuations {
            queue.requeue(handle, continuation)?;
        }
        waves.push(wave);
    }
    anyhow::ensure!(active.is_empty());

    let mut completions = Vec::with_capacity(specifications.len());
    while let Some(handle) = queue.announce_completion()? {
        completions.push(*queue.observe_completion_status(handle)?);
        queue.take_completion(handle)?;
    }
    anyhow::ensure!(queue.is_empty());
    Ok((completions, waves, queue))
}

#[cfg(test)]
#[test]
fn query_wave_model_requeues_before_later_pending_operations_and_announces_deterministically(
) -> anyhow::Result<()> {
    let (completions, waves, _) = run_model_query_waves(&[(3, None), (0, None)], 16)?;
    assert_eq!(
        completions,
        [
            ModelQueryCompletion::Success(1),
            ModelQueryCompletion::Success(0),
        ]
    );
    assert_eq!(waves[0], [0, 1]);
    assert_eq!(waves[1], [0]);

    let (completions, waves, _) = run_model_query_waves(&[(0, None), (2, None), (1, None)], 1)?;
    assert_eq!(
        completions,
        [
            ModelQueryCompletion::Success(0),
            ModelQueryCompletion::Success(1),
            ModelQueryCompletion::Success(2),
        ]
    );
    assert_eq!(waves, [[0], [1], [1], [1], [2], [2]]);

    let (completions, ..) = run_model_query_waves(&[(2, None), (2, None)], 16)?;
    assert_eq!(
        completions,
        [
            ModelQueryCompletion::Success(0),
            ModelQueryCompletion::Success(1),
        ]
    );

    let (completions, ..) = run_model_query_waves(&[(3, None), (4, Some(0))], 16)?;
    assert_eq!(
        completions,
        [
            ModelQueryCompletion::DeveloperError(1),
            ModelQueryCompletion::Success(0),
        ]
    );
    Ok(())
}

#[cfg(test)]
#[test]
fn query_wave_model_cancellation_cleans_up_and_queue_is_reusable() -> anyhow::Result<()> {
    let mut queue = AsyncOperationQueue::default();
    let first = queue.enqueue(ModelQueryContinuation {
        source_index: 0,
        remaining_values: 2,
        error_after_values: None,
        observed_values: 0,
    })?;
    let second = queue.enqueue(ModelQueryContinuation {
        source_index: 1,
        remaining_values: 1,
        error_after_values: None,
        observed_values: 0,
    })?;
    let mut active = BTreeSet::from([first, second]);
    let mut continuations = Vec::new();
    for expected_handle in [first, second] {
        let (handle, mut continuation) = queue.pop_pending().context("query disappeared")?;
        assert_eq!(handle, expected_handle);
        continuation.remaining_values -= 1;
        continuation.observed_values += 1;
        continuations.push((handle, continuation));
    }
    for (handle, continuation) in continuations {
        queue.requeue(handle, continuation)?;
    }

    queue.clear();
    active.clear();
    assert!(queue.is_empty());
    assert!(active.is_empty());

    let reused = queue.enqueue(ModelQueryContinuation {
        source_index: 2,
        remaining_values: 0,
        error_after_values: None,
        observed_values: 0,
    })?;
    assert_eq!(reused.to_abi(), 3);
    let (handle, continuation) = queue.pop_pending().context("reused query disappeared")?;
    assert_eq!(handle, reused);
    queue.complete(
        handle,
        ModelQueryCompletion::Success(continuation.source_index),
    )?;
    assert_eq!(queue.announce_completion()?, Some(reused));
    assert_eq!(
        queue.observe_completion_status(reused)?,
        &ModelQueryCompletion::Success(2)
    );
    assert_eq!(
        queue.take_completion(reused)?,
        ModelQueryCompletion::Success(2)
    );
    assert!(queue.is_empty());
    Ok(())
}

#[cfg(test)]
#[test]
fn async_operation_queue_rejects_duplicate_transitions_and_clears() -> anyhow::Result<()> {
    let mut queue = AsyncOperationQueue::<(), ()>::default();
    let handle = queue.enqueue(())?;
    assert!(queue.complete(handle, ()).is_err());
    assert_eq!(queue.pop_pending(), Some((handle, ())));
    queue.complete(handle, ())?;
    assert!(queue.complete(handle, ()).is_err());
    assert!(queue.requeue(handle, ()).is_err());
    assert_eq!(queue.announce_completion()?, Some(handle));
    queue.observe_completion_status(handle)?;
    assert!(queue.take_completion(handle).is_ok());
    assert!(queue.take_completion(handle).is_err());

    let pending = queue.enqueue(())?;
    assert_eq!(pending.to_abi(), 2);
    let pending_capacity = queue.pending.capacity();
    queue.clear();
    assert!(queue.is_empty());
    assert_eq!(queue.pending.capacity(), pending_capacity);
    Ok(())
}

#[cfg(test)]
#[test]
fn runtime_hot_path_retained_async_state_restarts_handles_without_reallocating(
) -> anyhow::Result<()> {
    let mut state = AsyncOperationState::default();
    state.queue.pending.reserve(4);
    let pending_capacity = state.queue.pending.capacity();
    state.queue.next_handle = Some(9);
    state.query_streams.next_handle = Some(7);

    state.begin_invocation()?;

    assert_eq!(state.queue.pending.capacity(), pending_capacity);
    assert_eq!(state.queue.enqueue_handle()?.to_abi(), 1);
    assert_eq!(
        state
            .query_streams
            .open(GeneratedQueryStream::Immediate(
                AsyncOperationCompletion::Success(None),
            ))?
            .to_abi(),
        1,
    );
    Ok(())
}

#[cfg(test)]
#[test]
fn async_operation_queue_counts_only_quiescent_abandoned_operations() -> anyhow::Result<()> {
    let mut queue = AsyncOperationQueue::<&str, &str>::default();
    let requeued = queue.enqueue("requeued")?;
    let pending = queue.enqueue("pending")?;
    assert_eq!(queue.unsettled_count()?, 2);

    assert_eq!(queue.pop_pending(), Some((requeued, "requeued")));
    assert!(queue.unsettled_count().is_err());
    queue.requeue(requeued, "requeued")?;
    assert_eq!(queue.pop_pending(), Some((requeued, "requeued")));
    queue.complete(requeued, "result")?;
    assert_eq!(queue.unsettled_count()?, 2);
    assert_eq!(queue.announce_completion()?, Some(requeued));
    assert!(queue.unsettled_count().is_err());
    queue.observe_completion_status(requeued)?;
    assert!(queue.unsettled_count().is_err());
    assert_eq!(queue.take_completion(requeued)?, "result");
    assert_eq!(queue.unsettled_count()?, 1);
    assert_eq!(queue.pop_pending(), Some((pending, "pending")));
    queue.clear();
    assert!(queue.is_empty());
    Ok(())
}

#[cfg(test)]
#[test]
fn async_operation_queue_uses_full_positive_i32_handle_range_and_explicit_outcomes(
) -> anyhow::Result<()> {
    let mut queue = AsyncOperationQueue::<(), AsyncOperationCompletion>::default();
    queue.next_handle = Some(i32::MAX as u32);
    let handle = queue.enqueue(())?;
    assert_eq!(handle.to_abi(), i32::MAX);
    assert!(queue.enqueue(()).is_err());
    assert_eq!(queue.pop_pending(), Some((handle, ())));
    queue.complete(
        handle,
        AsyncOperationCompletion::DeveloperError("rejected".to_owned().into()),
    )?;
    assert_eq!(queue.announce_completion()?, Some(handle));
    assert_eq!(
        queue.observe_completion_status(handle)?,
        &AsyncOperationCompletion::DeveloperError("rejected".to_owned().into())
    );
    assert_eq!(
        queue.take_completion(handle)?,
        AsyncOperationCompletion::DeveloperError("rejected".to_owned().into())
    );
    assert!(queue.is_empty());

    let success = AsyncOperationCompletion::Success(Some(json!({ "ok": true })));
    assert_eq!(
        success,
        AsyncOperationCompletion::Success(Some(json!({ "ok": true })))
    );
    Ok(())
}

#[cfg(test)]
#[test]
fn async_operation_queue_requires_one_announced_status_take_sequence() -> anyhow::Result<()> {
    let mut queue = AsyncOperationQueue::<(), AsyncOperationCompletion>::default();
    let ready = queue.enqueue_completion(AsyncOperationCompletion::Success(None))?;
    let pending = queue.enqueue(())?;

    assert!(queue.observe_completion_status(ready).is_err());
    assert!(queue.take_completion(ready).is_err());
    assert_eq!(queue.announce_completion()?, Some(ready));
    assert!(queue.announce_completion().is_err());
    assert!(queue.observe_completion_status(pending).is_err());
    assert!(queue.take_completion(ready).is_err());
    assert_eq!(
        queue.observe_completion_status(ready)?,
        &AsyncOperationCompletion::Success(None)
    );
    assert!(queue.observe_completion_status(ready).is_err());
    assert_eq!(
        queue.take_completion(ready)?,
        AsyncOperationCompletion::Success(None)
    );
    assert_eq!(queue.pop_pending(), Some((pending, ())));
    queue.complete(
        pending,
        AsyncOperationCompletion::Success(Some(JsonValue::Null)),
    )?;
    assert_eq!(queue.announce_completion()?, Some(pending));
    queue.observe_completion_status(pending)?;
    assert_eq!(
        queue.take_completion(pending)?,
        AsyncOperationCompletion::Success(Some(JsonValue::Null))
    );
    assert!(queue.is_empty());
    Ok(())
}

#[cfg(test)]
proptest! {
    #[test]
    fn async_operation_queue_preserves_generated_completion_order(
        priorities in prop::collection::vec(any::<u16>(), 1..64),
    ) {
        let mut queue = AsyncOperationQueue::<usize, usize>::default();
        let mut handles = Vec::with_capacity(priorities.len());
        for index in 0..priorities.len() {
            handles.push(queue.enqueue(index).expect("generated handle must fit"));
        }
        for (index, expected_handle) in handles.iter().copied().enumerate() {
            let (handle, operation) = queue.pop_pending().expect("queued operation disappeared");
            prop_assert_eq!(handle, expected_handle);
            prop_assert_eq!(operation, index);
        }

        let mut completion_order = (0..priorities.len()).collect::<Vec<_>>();
        completion_order.sort_by_key(|index| (priorities[*index], *index));
        for index in completion_order.iter().copied() {
            queue
                .complete(handles[index], index)
                .expect("in-flight operation must complete once");
        }
        for index in completion_order {
            let handle = queue
                .announce_completion()
                .expect("completion announcement must be valid")
                .expect("ready completion disappeared");
            prop_assert_eq!(handle, handles[index]);
            prop_assert_eq!(
                *queue
                    .observe_completion_status(handle)
                    .expect("announced completion status disappeared"),
                index
            );
            prop_assert_eq!(
                queue
                    .take_completion(handle)
                    .expect("observed completion disappeared"),
                index
            );
        }
        prop_assert!(queue.is_empty());
    }

    #[test]
    fn query_wave_model_preserves_priority_queue_invariants(
        specifications in prop::collection::vec((0u8..8, prop::option::of(0u8..8)), 1..64),
        maximum_batch_size in 1usize..17,
    ) {
        let (completions, waves, _) = run_model_query_waves(
            &specifications,
            maximum_batch_size,
        ).expect("generated query wave schedule must be valid");
        let mut expected_completions = specifications
            .iter()
            .copied()
            .enumerate()
            .map(|(source_index, (value_count, error_after_values))| {
                if let Some(error_after_values) = error_after_values
                    && error_after_values <= value_count
                {
                    (
                        error_after_values + 1,
                        source_index,
                        ModelQueryCompletion::DeveloperError(source_index),
                    )
                } else {
                    (
                        value_count + 1,
                        source_index,
                        ModelQueryCompletion::Success(source_index),
                    )
                }
            })
            .collect::<Vec<_>>();
        expected_completions.sort_by_key(|(_, source_index, _)| *source_index);
        let mut actual_completions = completions;
        actual_completions.sort_by_key(|completion| match completion {
            ModelQueryCompletion::Success(source_index)
            | ModelQueryCompletion::DeveloperError(source_index) => *source_index,
        });
        prop_assert_eq!(
            actual_completions,
            expected_completions
                .into_iter()
                .map(|(_, _, completion)| completion)
                .collect::<Vec<_>>()
        );
        for wave in waves {
            prop_assert!(!wave.is_empty());
            prop_assert!(wave.len() <= maximum_batch_size);
            let unique = wave.iter().copied().collect::<BTreeSet<_>>();
            prop_assert_eq!(unique.len(), wave.len());
            prop_assert!(wave.windows(2).all(|pair| pair[0] < pair[1]));
        }
    }
}

#[cfg(test)]
#[test]
fn seeded_query_wave_model_covers_multistep_errors_and_batch_boundaries() -> anyhow::Result<()> {
    let mut seed = 0x8d58_32a7_6f91_c4e5_u64;
    for _ in 0..256 {
        let operation_count = usize::try_from((seed % 48) + 1)?;
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let maximum_batch_size = usize::try_from((seed % 16) + 1)?;
        let mut specifications = Vec::with_capacity(operation_count);
        for _ in 0..operation_count {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let value_count = u8::try_from(seed % 8)?;
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let error_after_values = (seed & 3 != 0)
                .then(|| u8::try_from((seed >> 8) % 9).expect("seeded error position must fit u8"));
            specifications.push((value_count, error_after_values));
        }

        let (completions, waves, _) = run_model_query_waves(&specifications, maximum_batch_size)?;
        anyhow::ensure!(completions.len() == operation_count);
        anyhow::ensure!(waves
            .iter()
            .all(|wave| !wave.is_empty() && wave.len() <= maximum_batch_size));
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn generated_async_batch_operation_id_accepts_only_u32_integers() -> anyhow::Result<()> {
    for (wire_value, expected) in [
        (json!(0.0), 0),
        (json!(1.0), 1),
        (json!(u32::MAX), u32::MAX),
    ] {
        let invocation: GeneratedAsyncBatchInvocation = serde_json::from_value(json!({
            "operationId": wire_value,
            "arguments": [],
        }))?;
        assert_eq!(invocation.operation_id, expected);
    }

    for wire_value in [json!(-1.0), json!(1.5), json!(4_294_967_296.0)] {
        let result = serde_json::from_value::<GeneratedAsyncBatchInvocation>(json!({
            "operationId": wire_value,
            "arguments": [],
        }));
        assert!(result.is_err());
    }
    for wire_value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let deserializer =
            serde::de::value::F64Deserializer::<serde::de::value::Error>::new(wire_value);
        assert!(deserialize_generated_operation_id(deserializer).is_err());
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn authentication_get_user_identity_materializes_exact_async_syscall() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 43, 0);
    for udf_type in [UdfType::Query, UdfType::Mutation] {
        let operation = materialize_generated_async_operation(
            ImportedOperationDescriptor::AuthenticationGetUserIdentity {},
            Vec::new(),
            udf_type,
            &npm_version,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        )??;
        let GeneratedAsyncOperation::Syscall {
            name,
            args,
            normalization,
        } = operation
        else {
            anyhow::bail!("authentication operation did not materialize as a syscall");
        };
        assert_eq!(name, "1.0/getUserIdentity");
        assert_eq!(args, json!({}));
        assert!(matches!(normalization, GeneratedAsyncNormalization::Json));
    }

    for result in [
        JsonValue::Null,
        json!({
            "tokenIdentifier": "https://issuer.invalid|subject",
            "issuer": "https://issuer.invalid",
            "subject": "subject",
        }),
    ] {
        assert_eq!(
            normalize_generated_async_result(GeneratedAsyncNormalization::Json, result.clone())?,
            result
        );
    }
    assert!(materialize_generated_async_operation(
        ImportedOperationDescriptor::AuthenticationGetUserIdentity {},
        vec![JsonValue::Null],
        UdfType::Query,
        &npm_version,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .is_err());
    Ok(())
}

#[cfg(test)]
#[test]
fn function_handle_create_materializes_exact_async_syscall() -> anyhow::Result<()> {
    let npm_version = Version::parse("1.42.3+convex-in-prod.f0ef82a1e9b9")?;
    for (address, expected) in [
        (
            json!({ "name": "tasks:run" }),
            json!({
                "name": "tasks:run",
                "version": "1.42.3+convex-in-prod.f0ef82a1e9b9",
            }),
        ),
        (
            json!({ "reference": "_reference/function/tasks:run" }),
            json!({
                "reference": "_reference/function/tasks:run",
                "version": "1.42.3+convex-in-prod.f0ef82a1e9b9",
            }),
        ),
        (
            json!({ "functionHandle": "function://handle" }),
            json!({
                "functionHandle": "function://handle",
                "version": "1.42.3+convex-in-prod.f0ef82a1e9b9",
            }),
        ),
    ] {
        for udf_type in [UdfType::Query, UdfType::Mutation] {
            let operation = materialize_generated_async_operation(
                ImportedOperationDescriptor::FunctionHandleCreate {},
                vec![address.clone()],
                udf_type,
                &npm_version,
                UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            )??;
            let GeneratedAsyncOperation::Syscall {
                name,
                args,
                normalization,
            } = operation
            else {
                anyhow::bail!("function-handle operation did not materialize as a syscall");
            };
            assert_eq!(name, "1.0/createFunctionHandle");
            assert_eq!(args, expected.clone());
            assert!(matches!(
                normalization,
                GeneratedAsyncNormalization::FunctionHandle
            ));
        }
    }

    assert!(materialize_generated_async_operation(
        ImportedOperationDescriptor::FunctionHandleCreate {},
        Vec::new(),
        UdfType::Query,
        &npm_version,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .is_err());
    assert!(materialize_generated_async_operation(
        ImportedOperationDescriptor::FunctionHandleCreate {},
        vec![
            json!({ "name": "tasks:run" }),
            json!({ "name": "tasks:run" })
        ],
        UdfType::Mutation,
        &npm_version,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .is_err());
    for invalid in [
        json!("tasks:run"),
        json!({}),
        json!({ "name": "" }),
        json!({ "name": "tasks:run", "reference": "_reference/function/tasks:run" }),
    ] {
        assert!(materialize_generated_async_operation(
            ImportedOperationDescriptor::FunctionHandleCreate {},
            vec![invalid],
            UdfType::Query,
            &npm_version,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        )?
        .is_err());
    }
    assert_eq!(
        normalize_generated_async_result(
            GeneratedAsyncNormalization::FunctionHandle,
            json!("function://handle"),
        )?,
        json!("function://handle")
    );
    assert!(normalize_generated_async_result(
        GeneratedAsyncNormalization::FunctionHandle,
        JsonValue::Null,
    )
    .is_err());
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_function_handle_create_materializes_for_database_udfs() -> anyhow::Result<()> {
    let npm_version = Version::parse("1.42.3+convex-in-prod.f0ef82a1e9b9")?;
    for (function_address, expected) in [
        (
            CapabilityFunctionAddress::Name("tasks:run".to_owned()),
            json!({
                "name": "tasks:run",
                "version": "1.42.3+convex-in-prod.f0ef82a1e9b9",
            }),
        ),
        (
            CapabilityFunctionAddress::Reference("_reference/function/tasks:run".to_owned()),
            json!({
                "reference": "_reference/function/tasks:run",
                "version": "1.42.3+convex-in-prod.f0ef82a1e9b9",
            }),
        ),
        (
            CapabilityFunctionAddress::FunctionHandle("function://handle".to_owned()),
            json!({
                "functionHandle": "function://handle",
                "version": "1.42.3+convex-in-prod.f0ef82a1e9b9",
            }),
        ),
    ] {
        for udf_type in [UdfType::Query, UdfType::Mutation] {
            let operation = materialize_generated_capability_operation(
                AsyncCapabilityOperation::FunctionHandleCreate {
                    function_address: function_address.clone(),
                },
                udf_type,
                &npm_version,
                UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            )??;
            let GeneratedAsyncOperation::Syscall {
                name,
                args,
                normalization,
            } = operation
            else {
                anyhow::bail!("function-handle capability did not materialize as a syscall");
            };
            assert_eq!(name, "1.0/createFunctionHandle");
            assert_eq!(args, expected);
            assert!(matches!(
                normalization,
                GeneratedAsyncNormalization::FunctionHandle
            ));
        }
    }

    for udf_type in [UdfType::Action, UdfType::HttpAction] {
        assert!(materialize_generated_capability_operation(
            AsyncCapabilityOperation::FunctionHandleCreate {
                function_address: CapabilityFunctionAddress::Reference(
                    "_reference/function/tasks:run".to_owned(),
                ),
            },
            udf_type,
            &npm_version,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        )
        .is_err());
    }
    assert_eq!(
        normalize_generated_async_result(
            GeneratedAsyncNormalization::FunctionHandle,
            json!("function://handle"),
        )?,
        json!("function://handle")
    );
    assert!(normalize_generated_async_result(
        GeneratedAsyncNormalization::FunctionHandle,
        JsonValue::Null,
    )
    .is_err());
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_reads_materialize_exact_current_sdk_syscalls() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let unix_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    for (is_system, table) in [
        (false, Some("documents")),
        (true, Some("_scheduled_functions")),
        (false, None),
        (true, None),
    ] {
        let operation = materialize_generated_capability_operation(
            AsyncCapabilityOperation::DatabaseGet {
                table: table.map(str::to_owned),
                id: json!("document-id"),
                is_system,
            },
            UdfType::Query,
            &npm_version,
            unix_timestamp,
        )??;
        let GeneratedAsyncOperation::Syscall {
            name,
            args,
            normalization,
        } = operation
        else {
            anyhow::bail!("capability db.get did not materialize as a syscall");
        };
        assert_eq!(name, "1.0/get");
        let mut expected = json!({
            "id": "document-id",
            "isSystem": is_system,
            "version": "1.42.3",
        });
        if let Some(table) = table {
            expected["table"] = json!(table);
        }
        assert_eq!(args, expected);
        assert!(matches!(normalization, GeneratedAsyncNormalization::Json));
    }

    let auth = materialize_generated_capability_operation(
        AsyncCapabilityOperation::AuthenticationGetUserIdentity,
        UdfType::Mutation,
        &npm_version,
        unix_timestamp,
    )??;
    let GeneratedAsyncOperation::Syscall {
        name,
        args,
        normalization,
    } = auth
    else {
        anyhow::bail!("capability auth operation did not materialize as a syscall");
    };
    assert_eq!(name, "1.0/getUserIdentity");
    assert_eq!(args, json!({}));
    assert!(matches!(normalization, GeneratedAsyncNormalization::Json));
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_metadata_operations_materialize_exact_canonical_syscalls() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let unix_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    for (udf_type, operation, expected_name) in [
        (
            UdfType::Query,
            AsyncCapabilityOperation::GetFunctionMetadata,
            "1.0/getFunctionMetadata",
        ),
        (
            UdfType::Mutation,
            AsyncCapabilityOperation::GetFunctionMetadata,
            "1.0/getFunctionMetadata",
        ),
        (
            UdfType::Query,
            AsyncCapabilityOperation::GetDeploymentMetadata,
            "1.0/getDeploymentMetadata",
        ),
        (
            UdfType::Mutation,
            AsyncCapabilityOperation::GetDeploymentMetadata,
            "1.0/getDeploymentMetadata",
        ),
        (
            UdfType::Query,
            AsyncCapabilityOperation::GetTransactionMetrics,
            "1.0/getTransactionMetrics",
        ),
        (
            UdfType::Mutation,
            AsyncCapabilityOperation::GetTransactionMetrics,
            "1.0/getTransactionMetrics",
        ),
        (
            UdfType::Mutation,
            AsyncCapabilityOperation::GetRequestMetadata,
            "1.0/getRequestMetadata",
        ),
    ] {
        let operation = materialize_generated_capability_operation(
            operation,
            udf_type,
            &npm_version,
            unix_timestamp,
        )??;
        let GeneratedAsyncOperation::Syscall {
            name,
            args,
            normalization,
        } = operation
        else {
            anyhow::bail!("metadata operation did not materialize as a syscall");
        };
        assert_eq!(name, expected_name);
        assert_eq!(args, json!({}));
        assert!(matches!(normalization, GeneratedAsyncNormalization::Json));
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_audit_log_materializes_exact_sdk_syscall_for_database_udfs() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 43, 0);
    let unix_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    let body = json!({
        "action": "document.viewed",
        "actor": { "id": "user-id" },
        "source": { "ip": { "$var": "ip" } },
    });
    for udf_type in [UdfType::Query, UdfType::Mutation] {
        let operation = materialize_generated_capability_operation(
            AsyncCapabilityOperation::AuditLog { body: body.clone() },
            udf_type,
            &npm_version,
            unix_timestamp,
        )??;
        let GeneratedAsyncOperation::Syscall {
            name,
            args,
            normalization,
        } = operation
        else {
            anyhow::bail!("audit-log operation did not materialize as a syscall");
        };
        assert_eq!(name, "1.0/auditLog");
        assert_eq!(
            args,
            json!({
                "body": body,
                "version": "1.43.0",
            })
        );
        assert!(matches!(
            normalization,
            GeneratedAsyncNormalization::Undefined
        ));
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_request_metadata_rejects_query_udfs_during_materialization() {
    assert!(materialize_generated_capability_operation(
        AsyncCapabilityOperation::GetRequestMetadata,
        UdfType::Query,
        &Version::new(1, 42, 3),
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .is_err());
}

#[cfg(test)]
#[test]
fn capability_queries_reuse_exact_query_stream_shape() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let constraints = vec![
        CapabilityQueryConstraint::Eq {
            field: "tenant".to_owned(),
            value: json!("tenant-a"),
        },
        CapabilityQueryConstraint::Gt {
            field: "sequence".to_owned(),
            value: json!(3),
        },
    ];
    let operators = vec![
        CapabilityQueryOperator::Filter {
            expression: json!({
                "$neq": [
                    { "$field": "disabled" },
                    { "$literal": true },
                ],
            }),
        },
        CapabilityQueryOperator::Limit { limit: 5 },
    ];
    let expected_args = json!({
        "query": {
            "source": {
                "type": "IndexRange",
                "indexName": "documents.by_tenant_sequence",
                "range": [
                    { "type": "Eq", "fieldPath": "tenant", "value": "tenant-a" },
                    { "type": "Gt", "fieldPath": "sequence", "value": 3 },
                ],
                "order": "asc",
            },
            "operators": [
                {
                    "filter": {
                        "$neq": [
                            { "$field": "disabled" },
                            { "$literal": true },
                        ],
                    },
                },
                { "limit": 5 },
            ],
        },
        "version": "1.42.3",
    });
    assert_eq!(
        capability_query_stream_args(
            "documents",
            CapabilityQuerySource::IndexRange {
                index: "by_tenant_sequence".to_owned(),
                constraints: constraints.clone(),
            },
            operators.clone(),
            capability_bridge::CapabilityQueryOrder::Asc,
            &npm_version,
        ),
        expected_args
    );
    let expected = parse_query_stream_request(expected_args)?;
    let operation = materialize_generated_capability_operation(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source: CapabilityQuerySource::IndexRange {
                index: "by_tenant_sequence".to_owned(),
                constraints,
            },
            operators,
            order: capability_bridge::CapabilityQueryOrder::Asc,
            terminal: CapabilityQueryTerminal::Collect,
        },
        UdfType::Query,
        &npm_version,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )??;
    let GeneratedAsyncOperation::Query {
        query,
        version,
        table_name,
        terminal,
    } = operation
    else {
        anyhow::bail!("capability query did not reuse query continuation state");
    };
    assert_eq!(query, expected.query);
    assert_eq!(version, expected.version);
    assert_eq!(table_name, "documents");
    assert_eq!(terminal, QueryTerminal::Collect);

    let default_order = capability_query_stream_args(
        "documents",
        CapabilityQuerySource::FullTableScan,
        Vec::new(),
        capability_bridge::CapabilityQueryOrder::Default,
        &npm_version,
    );
    assert_eq!(
        default_order
            .pointer("/query/source/order")
            .context("capability query omitted its order field")?,
        &JsonValue::Null
    );
    assert_eq!(
        default_order
            .pointer("/query/source/tableName")
            .context("capability full-table query omitted its table")?,
        &json!("documents")
    );
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_search_queries_reuse_exact_query_stream_shape() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let source = CapabilityQuerySource::Search {
        index: "by_content".to_owned(),
        filters: vec![
            CapabilitySearchFilter::Search {
                field: "body".to_owned(),
                value: "needle phrase".to_owned(),
            },
            CapabilitySearchFilter::Eq {
                field: "tenant".to_owned(),
                value: json!("tenant-a"),
            },
            CapabilitySearchFilter::Eq {
                field: "category".to_owned(),
                value: json!({ "$undefined": null }),
            },
        ],
    };
    let operators = vec![CapabilityQueryOperator::Limit { limit: 4 }];
    let expected_args = json!({
        "query": {
            "source": {
                "type": "Search",
                "indexName": "documents.by_content",
                "filters": [
                    { "type": "Search", "fieldPath": "body", "value": "needle phrase" },
                    { "type": "Eq", "fieldPath": "tenant", "value": "tenant-a" },
                    { "type": "Eq", "fieldPath": "category", "value": { "$undefined": null } },
                ],
            },
            "operators": [{ "limit": 4 }],
        },
        "version": "1.42.3",
    });
    assert_eq!(
        capability_query_stream_args(
            "documents",
            source.clone(),
            operators.clone(),
            capability_bridge::CapabilityQueryOrder::Default,
            &npm_version,
        ),
        expected_args
    );
    let expected = parse_query_stream_request(expected_args)?;
    let operation = materialize_generated_capability_operation(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source,
            operators,
            order: capability_bridge::CapabilityQueryOrder::Default,
            terminal: CapabilityQueryTerminal::Collect,
        },
        UdfType::Query,
        &npm_version,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )??;
    let GeneratedAsyncOperation::Query {
        query,
        version,
        table_name,
        terminal,
    } = operation
    else {
        anyhow::bail!("capability search query did not reuse query continuation state");
    };
    assert_eq!(query, expected.query);
    assert_eq!(version, expected.version);
    assert_eq!(table_name, "documents");
    assert_eq!(terminal, QueryTerminal::Collect);
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_query_rejects_pending_commit_timestamp_in_any_range_constraint() {
    let operation = materialize_generated_capability_operation(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source: CapabilityQuerySource::IndexRange {
                index: "by_tenant_commit_ts".to_owned(),
                constraints: vec![
                    CapabilityQueryConstraint::Eq {
                        field: "tenant".to_owned(),
                        value: json!("tenant-a"),
                    },
                    CapabilityQueryConstraint::Gt {
                        field: "commitTs".to_owned(),
                        value: json!({ "$commitTs": null }),
                    },
                ],
            },
            operators: vec![],
            order: capability_bridge::CapabilityQueryOrder::Default,
            terminal: CapabilityQueryTerminal::Collect,
        },
        UdfType::Query,
        &Version::new(1, 42, 3),
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    );
    assert!(operation.is_err());
}

#[cfg(test)]
#[test]
fn capability_mutation_pending_commit_timestamp_range_uses_canonical_max_projection(
) -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let operation = materialize_generated_capability_operation(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source: CapabilityQuerySource::IndexRange {
                index: "by_commit_ts".to_owned(),
                constraints: vec![CapabilityQueryConstraint::Eq {
                    field: "commitTs".to_owned(),
                    value: json!({ "$commitTs": null }),
                }],
            },
            operators: vec![],
            order: capability_bridge::CapabilityQueryOrder::Default,
            terminal: CapabilityQueryTerminal::Collect,
        },
        UdfType::Mutation,
        &npm_version,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )??;
    let GeneratedAsyncOperation::Query { query, .. } = operation else {
        anyhow::bail!("pending commit timestamp query did not materialize canonically");
    };
    let expected = parse_query_stream_request(capability_query_stream_args(
        "documents",
        CapabilityQuerySource::IndexRange {
            index: "by_commit_ts".to_owned(),
            constraints: vec![CapabilityQueryConstraint::Eq {
                field: "commitTs".to_owned(),
                value: ConvexValue::Int64(MAX_COMMIT_TS).to_internal_json(),
            }],
        },
        vec![],
        capability_bridge::CapabilityQueryOrder::Default,
        &npm_version,
    ))?;
    assert_eq!(query, expected.query);
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_query_stream_rejects_pending_commit_timestamp_in_any_range_constraint() {
    let stream = materialize_generated_capability_query_stream(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source: CapabilityQuerySource::IndexRange {
                index: "by_tenant_commit_ts".to_owned(),
                constraints: vec![
                    CapabilityQueryConstraint::Eq {
                        field: "tenant".to_owned(),
                        value: json!("tenant-a"),
                    },
                    CapabilityQueryConstraint::Gt {
                        field: "commitTs".to_owned(),
                        value: json!({ "$commitTs": null }),
                    },
                ],
            },
            operators: vec![],
            order: capability_bridge::CapabilityQueryOrder::Default,
            terminal: CapabilityQueryTerminal::Stream,
        },
        UdfType::Query,
        &Version::new(1, 42, 3),
    );
    assert!(stream.is_err());
}

#[cfg(test)]
#[test]
fn capability_query_stream_accepts_undefined_range_constraint() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let source = CapabilityQuerySource::IndexRange {
        index: "by_tenant_released_at".to_owned(),
        constraints: vec![
            CapabilityQueryConstraint::Eq {
                field: "tenant".to_owned(),
                value: json!("tenant-a"),
            },
            CapabilityQueryConstraint::Eq {
                field: "releasedAt".to_owned(),
                value: json!({ "$undefined": null }),
            },
        ],
    };
    let stream = materialize_generated_capability_query_stream(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source: source.clone(),
            operators: vec![],
            order: capability_bridge::CapabilityQueryOrder::Default,
            terminal: CapabilityQueryTerminal::Stream,
        },
        UdfType::Query,
        &npm_version,
    )?;
    let GeneratedQueryStream::Pending { query, version } = stream else {
        anyhow::bail!("undefined query constraint did not produce a pending query stream");
    };
    let expected = parse_query_stream_request(capability_query_stream_args(
        "documents",
        source,
        vec![],
        capability_bridge::CapabilityQueryOrder::Default,
        &npm_version,
    ))?;
    assert_eq!(query, expected.query);
    assert_eq!(version, expected.version);
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_mutation_query_stream_pending_commit_timestamp_range_uses_canonical_max_projection(
) -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let stream = materialize_generated_capability_query_stream(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source: CapabilityQuerySource::IndexRange {
                index: "by_commit_ts".to_owned(),
                constraints: vec![CapabilityQueryConstraint::Eq {
                    field: "commitTs".to_owned(),
                    value: json!({ "$commitTs": null }),
                }],
            },
            operators: vec![],
            order: capability_bridge::CapabilityQueryOrder::Default,
            terminal: CapabilityQueryTerminal::Stream,
        },
        UdfType::Mutation,
        &npm_version,
    )?;
    let GeneratedQueryStream::Pending { query, .. } = stream else {
        anyhow::bail!("pending commit timestamp query stream did not materialize canonically");
    };
    let expected = parse_query_stream_request(capability_query_stream_args(
        "documents",
        CapabilityQuerySource::IndexRange {
            index: "by_commit_ts".to_owned(),
            constraints: vec![CapabilityQueryConstraint::Eq {
                field: "commitTs".to_owned(),
                value: ConvexValue::Int64(MAX_COMMIT_TS).to_internal_json(),
            }],
        },
        vec![],
        capability_bridge::CapabilityQueryOrder::Default,
        &npm_version,
    ))?;
    assert_eq!(query, expected.query);
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_query_stream_reuses_canonical_query_state_without_pagination() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let expected = parse_query_stream_request(capability_query_stream_args(
        "documents",
        CapabilityQuerySource::IndexRange {
            index: "by_tenant".to_owned(),
            constraints: vec![CapabilityQueryConstraint::Eq {
                field: "tenant".to_owned(),
                value: json!("tenant-a"),
            }],
        },
        vec![CapabilityQueryOperator::Limit { limit: 7 }],
        capability_bridge::CapabilityQueryOrder::Desc,
        &npm_version,
    ))?;
    let stream = materialize_generated_capability_query_stream(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source: CapabilityQuerySource::IndexRange {
                index: "by_tenant".to_owned(),
                constraints: vec![CapabilityQueryConstraint::Eq {
                    field: "tenant".to_owned(),
                    value: json!("tenant-a"),
                }],
            },
            operators: vec![CapabilityQueryOperator::Limit { limit: 7 }],
            order: capability_bridge::CapabilityQueryOrder::Desc,
            terminal: CapabilityQueryTerminal::Stream,
        },
        UdfType::Query,
        &npm_version,
    )?;
    let GeneratedQueryStream::Pending { query, version } = stream else {
        anyhow::bail!("capability query stream did not retain canonical pending query state");
    };
    assert_eq!(query, expected.query);
    assert_eq!(version, expected.version);
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_paginate_materializes_the_canonical_query_page_syscall() -> anyhow::Result<()> {
    let operation = materialize_generated_capability_operation(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source: CapabilityQuerySource::IndexRange {
                index: "by_tenant_sequence".to_owned(),
                constraints: vec![CapabilityQueryConstraint::Eq {
                    field: "tenant".to_owned(),
                    value: json!("tenant-a"),
                }],
            },
            operators: vec![CapabilityQueryOperator::Filter {
                expression: json!({
                    "$gt": [{ "$field": "sequence" }, { "$literal": 3 }],
                }),
            }],
            order: capability_bridge::CapabilityQueryOrder::Desc,
            terminal: CapabilityQueryTerminal::Paginate(CapabilityQueryPagination {
                cursor: Some("current-cursor".to_owned()),
                end_cursor: None,
                maximum_bytes_read: Some(4096),
                maximum_rows_read: Some(7),
                page_size: 2,
            }),
        },
        UdfType::Query,
        &Version::new(1, 42, 3),
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )??;
    let GeneratedAsyncOperation::Syscall {
        name,
        args,
        normalization,
    } = operation
    else {
        anyhow::bail!("capability paginate did not materialize as one canonical syscall");
    };
    assert_eq!(name, "1.0/queryPage");
    assert!(matches!(normalization, GeneratedAsyncNormalization::Json));
    assert_eq!(
        args,
        json!({
            "query": {
                "source": {
                    "type": "IndexRange",
                    "indexName": "documents.by_tenant_sequence",
                    "range": [
                        { "type": "Eq", "fieldPath": "tenant", "value": "tenant-a" },
                    ],
                    "order": "desc",
                },
                "operators": [{
                    "filter": {
                        "$gt": [{ "$field": "sequence" }, { "$literal": 3 }],
                    },
                }],
            },
            "cursor": "current-cursor",
            "endCursor": null,
            "pageSize": 2,
            "maximumRowsRead": 7,
            "maximumBytesRead": 4096,
            "version": "1.42.3",
        })
    );
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_query_terminals_and_udf_kinds_materialize_exactly() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let unix_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    for (capability_terminal, query_terminal) in [
        (CapabilityQueryTerminal::Collect, QueryTerminal::Collect),
        (CapabilityQueryTerminal::First, QueryTerminal::First),
        (CapabilityQueryTerminal::Unique, QueryTerminal::Unique),
    ] {
        for udf_type in [UdfType::Query, UdfType::Mutation] {
            let operation = materialize_generated_capability_operation(
                AsyncCapabilityOperation::DatabaseQuery {
                    table: "documents".to_owned(),
                    source: CapabilityQuerySource::FullTableScan,
                    operators: Vec::new(),
                    order: capability_bridge::CapabilityQueryOrder::Default,
                    terminal: capability_terminal.clone(),
                },
                udf_type,
                &npm_version,
                unix_timestamp,
            )??;
            let GeneratedAsyncOperation::Query {
                query, terminal, ..
            } = operation
            else {
                anyhow::bail!("capability terminal did not materialize as a query");
            };
            assert_eq!(terminal, query_terminal);
            match canonical_query_terminal_limit(query_terminal) {
                Some(limit) => assert_eq!(
                    query.operators.last(),
                    Some(&common::query::QueryOperator::Limit(usize::try_from(
                        limit
                    )?)),
                ),
                None => assert!(query.operators.is_empty()),
            }
        }
    }

    assert!(materialize_generated_capability_operation(
        AsyncCapabilityOperation::DatabaseQuery {
            table: "documents".to_owned(),
            source: CapabilityQuerySource::FullTableScan,
            operators: Vec::new(),
            order: capability_bridge::CapabilityQueryOrder::Default,
            terminal: CapabilityQueryTerminal::Collect,
        },
        UdfType::Action,
        &npm_version,
        unix_timestamp,
    )
    .is_err());
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_query_continuation_requeues_before_later_operations_and_drains_before_clear(
) -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let unix_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    let materialize = |terminal| {
        materialize_generated_capability_operation(
            AsyncCapabilityOperation::DatabaseQuery {
                table: "documents".to_owned(),
                source: CapabilityQuerySource::FullTableScan,
                operators: Vec::new(),
                order: capability_bridge::CapabilityQueryOrder::Default,
                terminal,
            },
            UdfType::Query,
            &npm_version,
            unix_timestamp,
        )
    };
    let mut state = AsyncOperationState::default();
    let collect_handle = state
        .queue
        .enqueue(materialize(CapabilityQueryTerminal::Collect)??)?;
    let first_handle = state
        .queue
        .enqueue(materialize(CapabilityQueryTerminal::First)??)?;

    let (handle, operation) = state.queue.pop_pending().context("collect disappeared")?;
    assert_eq!(handle, collect_handle);
    let GeneratedAsyncOperation::Query {
        table_name,
        terminal,
        ..
    } = operation
    else {
        anyhow::bail!("capability collect did not materialize as a query");
    };
    let query_id = QueryId::try_from(17_u64)?;
    assert!(state.active_queries.insert(query_id));
    state.queue.requeue(
        handle,
        GeneratedAsyncOperation::QueryContinuation {
            query_id,
            table_name,
            terminal,
            values: vec![json!({ "_id": "document-1" })],
        },
    )?;

    let (handle, operation) = state
        .queue
        .pop_pending()
        .context("collect continuation disappeared")?;
    assert_eq!(handle, collect_handle);
    assert!(matches!(
        operation,
        GeneratedAsyncOperation::QueryContinuation {
            query_id: actual_query_id,
            terminal: QueryTerminal::Collect,
            ref values,
            ..
        } if actual_query_id == query_id && values.len() == 1
    ));
    let (handle, operation) = state.queue.pop_pending().context("first disappeared")?;
    assert_eq!(handle, first_handle);
    assert!(matches!(
        operation,
        GeneratedAsyncOperation::Query {
            terminal: QueryTerminal::First,
            ..
        }
    ));

    // Cancellation must drain provider-owned query IDs before it drops the
    // descriptors that authenticate their continuations.
    let active_queries = std::mem::take(&mut state.active_queries);
    assert_eq!(active_queries, BTreeSet::from([query_id]));
    state.clear();
    assert!(state.is_empty());
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_mutations_repeat_host_udf_kind_authorization() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let unix_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    let operations = [
        AsyncCapabilityOperation::DatabaseInsert {
            table: "documents".to_owned(),
            value: json!({ "tenant": "tenant-a" }),
        },
        AsyncCapabilityOperation::DatabasePatch {
            table: "documents".to_owned(),
            id: json!("document-id"),
            patch: json!({ "sequence": 4 }),
        },
        AsyncCapabilityOperation::DatabaseReplace {
            table: "documents".to_owned(),
            id: json!("document-id"),
            value: json!({ "sequence": 5 }),
        },
        AsyncCapabilityOperation::DatabaseDelete {
            table: "documents".to_owned(),
            id: json!("document-id"),
        },
        AsyncCapabilityOperation::StorageGenerateUploadUrl,
        AsyncCapabilityOperation::StorageDelete {
            storage_id: "storage-id".to_owned(),
        },
        AsyncCapabilityOperation::SchedulerRunAfter {
            delay_milliseconds: json!(0),
            function_address: CapabilityFunctionAddress::Reference(
                "_reference/function/tasks:run".to_owned(),
            ),
            args: json!({ "sequence": 4 }),
        },
        AsyncCapabilityOperation::SchedulerRunAt {
            timestamp_milliseconds: json!(1_700_000_000_250_u64),
            function_address: CapabilityFunctionAddress::Name("tasks:run".to_owned()),
            args: json!({ "sequence": 5 }),
        },
        AsyncCapabilityOperation::SchedulerCancel {
            id: json!("scheduled-id"),
        },
    ];
    for operation in operations {
        assert!(materialize_generated_capability_operation(
            operation,
            UdfType::Query,
            &npm_version,
            unix_timestamp,
        )
        .is_err());
    }

    let insert = materialize_generated_capability_operation(
        AsyncCapabilityOperation::DatabaseInsert {
            table: "documents".to_owned(),
            value: json!({ "tenant": "tenant-a" }),
        },
        UdfType::Mutation,
        &npm_version,
        unix_timestamp,
    )??;
    let GeneratedAsyncOperation::Syscall {
        name,
        args,
        normalization,
    } = insert
    else {
        anyhow::bail!("capability insert did not materialize as a syscall");
    };
    assert_eq!(name, "1.0/insert");
    assert_eq!(
        args,
        json!({ "table": "documents", "value": { "tenant": "tenant-a" } })
    );
    assert!(matches!(
        normalization,
        GeneratedAsyncNormalization::InsertId
    ));

    let patch = materialize_generated_capability_operation(
        AsyncCapabilityOperation::DatabasePatch {
            table: "documents".to_owned(),
            id: json!("document-id"),
            patch: json!({ "sequence": 4 }),
        },
        UdfType::Mutation,
        &npm_version,
        unix_timestamp,
    )??;
    let GeneratedAsyncOperation::Syscall {
        name,
        args,
        normalization,
    } = patch
    else {
        anyhow::bail!("capability patch did not materialize as a syscall");
    };
    assert_eq!(name, "1.0/shallowMerge");
    assert_eq!(
        args,
        json!({ "table": "documents", "id": "document-id", "value": { "sequence": 4 } })
    );
    assert!(matches!(
        normalization,
        GeneratedAsyncNormalization::Undefined
    ));

    for (operation, expected_name, expected_args) in [
        (
            AsyncCapabilityOperation::DatabaseReplace {
                table: "documents".to_owned(),
                id: json!("document-id"),
                value: json!({ "sequence": 5 }),
            },
            "1.0/replace",
            json!({ "table": "documents", "id": "document-id", "value": { "sequence": 5 } }),
        ),
        (
            AsyncCapabilityOperation::DatabaseDelete {
                table: "documents".to_owned(),
                id: json!("document-id"),
            },
            "1.0/remove",
            json!({ "table": "documents", "id": "document-id" }),
        ),
    ] {
        let operation = materialize_generated_capability_operation(
            operation,
            UdfType::Mutation,
            &npm_version,
            unix_timestamp,
        )??;
        let GeneratedAsyncOperation::Syscall {
            name,
            args,
            normalization,
        } = operation
        else {
            anyhow::bail!("capability database write did not materialize as a syscall");
        };
        assert_eq!(name, expected_name);
        assert_eq!(args, expected_args);
        assert!(matches!(
            normalization,
            GeneratedAsyncNormalization::Undefined
        ));
    }

    let schedule = materialize_generated_capability_operation(
        AsyncCapabilityOperation::SchedulerRunAfter {
            delay_milliseconds: json!(250),
            function_address: CapabilityFunctionAddress::Reference(
                "_reference/function/tasks:run".to_owned(),
            ),
            args: json!({ "sequence": 4 }),
        },
        UdfType::Mutation,
        &npm_version,
        unix_timestamp,
    )??;
    let GeneratedAsyncOperation::Syscall {
        name,
        args,
        normalization,
    } = schedule
    else {
        anyhow::bail!("capability scheduler operation did not materialize as a syscall");
    };
    assert_eq!(name, "1.0/schedule");
    assert_eq!(
        args,
        json!({
            "reference": "_reference/function/tasks:run",
            "ts": 1_700_000_000.25,
            "args": { "sequence": 4 },
        })
    );
    assert!(matches!(
        normalization,
        GeneratedAsyncNormalization::PublicId
    ));

    let run_at = materialize_generated_capability_operation(
        AsyncCapabilityOperation::SchedulerRunAt {
            timestamp_milliseconds: json!(1_700_000_000_250_u64),
            function_address: CapabilityFunctionAddress::Name("tasks:run".to_owned()),
            args: json!({ "sequence": 5 }),
        },
        UdfType::Mutation,
        &npm_version,
        unix_timestamp,
    )??;
    let GeneratedAsyncOperation::Syscall {
        name,
        args,
        normalization,
    } = run_at
    else {
        anyhow::bail!("capability scheduler runAt did not materialize as a syscall");
    };
    assert_eq!(name, "1.0/schedule");
    assert_eq!(
        args,
        json!({
            "name": "tasks:run",
            "ts": 1_700_000_000.25,
            "args": { "sequence": 5 },
        })
    );
    assert!(matches!(
        normalization,
        GeneratedAsyncNormalization::PublicId
    ));

    let cancel = materialize_generated_capability_operation(
        AsyncCapabilityOperation::SchedulerCancel {
            id: json!("scheduled-id"),
        },
        UdfType::Mutation,
        &npm_version,
        unix_timestamp,
    )??;
    let GeneratedAsyncOperation::Syscall {
        name,
        args,
        normalization,
    } = cancel
    else {
        anyhow::bail!("capability scheduler cancel did not materialize as a syscall");
    };
    assert_eq!(name, "1.0/cancel_job");
    assert_eq!(args, json!({ "id": "scheduled-id" }));
    assert!(matches!(
        normalization,
        GeneratedAsyncNormalization::Undefined
    ));

    for (function_address, expected_address) in [
        (
            CapabilityFunctionAddress::Name("tasks:run".to_owned()),
            json!({ "name": "tasks:run" }),
        ),
        (
            CapabilityFunctionAddress::Reference("_reference/function/tasks:run".to_owned()),
            json!({ "reference": "_reference/function/tasks:run" }),
        ),
        (
            CapabilityFunctionAddress::FunctionHandle("function://handle".to_owned()),
            json!({ "functionHandle": "function://handle" }),
        ),
    ] {
        let syscall_args = capability_scheduler_syscall_args(
            function_address,
            1_700_000_000.25,
            json!({ "sequence": 4 }),
        );
        let mut expected = expected_address;
        expected
            .as_object_mut()
            .expect("scheduler address fixture must be an object")
            .extend(
                json!({ "ts": 1_700_000_000.25, "args": { "sequence": 4 } })
                    .as_object()
                    .expect("scheduler fixture must be an object")
                    .clone(),
            );
        assert_eq!(syscall_args, expected);
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_storage_materializes_normal_database_udf_syscalls() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let unix_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    for (operation, udf_type, expected_name, expected_args, expects_undefined) in [
        (
            AsyncCapabilityOperation::StorageGetUrl {
                storage_id: "storage-id".to_owned(),
            },
            UdfType::Query,
            "1.0/storageGetUrl",
            json!({ "storageId": "storage-id" }),
            false,
        ),
        (
            AsyncCapabilityOperation::StorageGetUrl {
                storage_id: "storage-id".to_owned(),
            },
            UdfType::Mutation,
            "1.0/storageGetUrl",
            json!({ "storageId": "storage-id" }),
            false,
        ),
        (
            AsyncCapabilityOperation::StorageGetMetadata {
                storage_id: "storage-id".to_owned(),
            },
            UdfType::Query,
            "1.0/storageGetMetadata",
            json!({ "storageId": "storage-id" }),
            false,
        ),
        (
            AsyncCapabilityOperation::StorageGetMetadata {
                storage_id: "storage-id".to_owned(),
            },
            UdfType::Mutation,
            "1.0/storageGetMetadata",
            json!({ "storageId": "storage-id" }),
            false,
        ),
        (
            AsyncCapabilityOperation::StorageGenerateUploadUrl,
            UdfType::Mutation,
            "1.0/storageGenerateUploadUrl",
            json!({}),
            false,
        ),
        (
            AsyncCapabilityOperation::StorageDelete {
                storage_id: "storage-id".to_owned(),
            },
            UdfType::Mutation,
            "1.0/storageDelete",
            json!({ "storageId": "storage-id" }),
            true,
        ),
    ] {
        let materialized = materialize_generated_capability_operation(
            operation,
            udf_type,
            &npm_version,
            unix_timestamp,
        )??;
        let GeneratedAsyncOperation::Syscall {
            name,
            args,
            normalization,
        } = materialized
        else {
            anyhow::bail!("storage capability did not materialize as a syscall");
        };
        assert_eq!(name, expected_name);
        assert_eq!(args, expected_args);
        assert!(
            matches!(normalization, GeneratedAsyncNormalization::Undefined) == expects_undefined
        );
        assert!(matches!(normalization, GeneratedAsyncNormalization::Json) != expects_undefined);
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_nested_udfs_materialize_exact_current_sdk_syscalls() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let unix_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    for (parent_udf_type, nested_udf_type, expected_udf_type) in [
        (UdfType::Query, CapabilityNestedUdfType::Query, "query"),
        (UdfType::Mutation, CapabilityNestedUdfType::Query, "query"),
        (
            UdfType::Mutation,
            CapabilityNestedUdfType::Mutation,
            "mutation",
        ),
        (
            UdfType::Mutation,
            CapabilityNestedUdfType::SnapshotQuery,
            "snapshotQuery",
        ),
    ] {
        let operation = materialize_generated_capability_operation(
            AsyncCapabilityOperation::RunUdf {
                udf_type: nested_udf_type,
                function_address: CapabilityFunctionAddress::Reference(
                    "_reference/function/tasks:run".to_owned(),
                ),
                args: json!({ "commitTs": { "$commitTs": null } }),
                transaction_limits: Some(json!({
                    "documentsRead": 8,
                    "documentsWritten": 3,
                })),
            },
            parent_udf_type,
            &npm_version,
            unix_timestamp,
        )??;
        let GeneratedAsyncOperation::Syscall {
            name,
            args,
            normalization,
        } = operation
        else {
            anyhow::bail!("nested UDF capability did not materialize as a syscall");
        };
        assert_eq!(name, "1.0/runUdf");
        assert_eq!(
            args,
            json!({
                "udfType": expected_udf_type,
                "reference": "_reference/function/tasks:run",
                "args": { "commitTs": { "$commitTs": null } },
                "transactionLimits": {
                    "documentsRead": 8,
                    "documentsWritten": 3,
                },
            })
        );
        assert!(matches!(normalization, GeneratedAsyncNormalization::Json));
    }

    let no_limits = capability_run_udf_syscall_args(
        CapabilityNestedUdfType::Query,
        CapabilityFunctionAddress::FunctionHandle("function://handle".to_owned()),
        json!({}),
        None,
    );
    assert_eq!(
        no_limits,
        json!({
            "udfType": "query",
            "functionHandle": "function://handle",
            "args": {},
        })
    );
    for nested_udf_type in [
        CapabilityNestedUdfType::Mutation,
        CapabilityNestedUdfType::SnapshotQuery,
    ] {
        assert!(materialize_generated_capability_operation(
            AsyncCapabilityOperation::RunUdf {
                udf_type: nested_udf_type,
                function_address: CapabilityFunctionAddress::Name("tasks:run".to_owned()),
                args: json!({}),
                transaction_limits: None,
            },
            UdfType::Query,
            &npm_version,
            unix_timestamp,
        )
        .is_err());
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_results_use_variant_derived_normalization() -> anyhow::Result<()> {
    assert_eq!(
        normalize_generated_async_result(
            GeneratedAsyncNormalization::InsertId,
            json!({ "_id": "inserted-id" }),
        )?,
        json!("inserted-id")
    );
    assert_eq!(
        normalize_generated_async_result(
            GeneratedAsyncNormalization::PublicId,
            json!("scheduled-id"),
        )?,
        json!("scheduled-id")
    );
    let undefined = normalize_generated_direct_async_batch_result(
        GeneratedAsyncNormalization::Undefined,
        JsonValue::Null,
    )?;
    assert!(matches!(
        &undefined,
        GeneratedDirectAsyncBatchResult::Undefined
    ));
    assert_eq!(undefined.into_json_array_value(), JsonValue::Null);
    for (normalization, invalid) in [
        (GeneratedAsyncNormalization::InsertId, JsonValue::Null),
        (
            GeneratedAsyncNormalization::PublicId,
            json!({ "id": "forged" }),
        ),
    ] {
        assert!(normalize_generated_async_result(normalization, invalid).is_err());
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn capability_scheduler_times_are_validated_by_the_host() -> anyhow::Result<()> {
    let npm_version = Version::new(1, 42, 3);
    let unix_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    for operation in [
        AsyncCapabilityOperation::SchedulerRunAfter {
            delay_milliseconds: json!(-1),
            function_address: CapabilityFunctionAddress::Name("tasks:run".to_owned()),
            args: json!({}),
        },
        AsyncCapabilityOperation::SchedulerRunAt {
            timestamp_milliseconds: json!("not-a-timestamp"),
            function_address: CapabilityFunctionAddress::Name("tasks:run".to_owned()),
            args: json!({}),
        },
    ] {
        assert!(materialize_generated_capability_operation(
            operation,
            UdfType::Mutation,
            &npm_version,
            unix_timestamp,
        )?
        .is_err());
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn performance_duration_is_invocation_relative_and_floored_to_point_one_millisecond() {
    assert_eq!(round_performance_duration_milliseconds(Duration::ZERO), 0.0);
    assert_eq!(
        round_performance_duration_milliseconds(Duration::from_nanos(12_349_999)),
        12.3
    );
    assert_eq!(
        round_performance_duration_milliseconds(Duration::from_nanos(12_400_000)),
        12.4
    );
}
