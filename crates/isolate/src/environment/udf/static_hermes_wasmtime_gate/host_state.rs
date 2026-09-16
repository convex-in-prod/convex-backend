use super::{
    super::{
        syscall::SyscallProviderInternal,
        DatabaseUdfArgs,
        DatabaseUdfEnvironment,
        DatabaseUdfInnerProvider,
        DatabaseUdfSyscallProvider,
    },
    *,
};

pub(super) const STATIC_HERMES_WASI_ARGS: &[&[u8]] = &[b"static-hermes-backend-gate"];

/// Invocation-owned database state for a retained Static Hermes Store.
///
/// The Store may outlive this value, but the canonical provider and its
/// transaction are removed during finalization before a Store can return to
/// the pool.
pub(super) struct DatabaseUdfWasmInvocation<RT: Runtime> {
    provider: Option<DatabaseUdfSyscallProvider<RT>>,
    args: Option<DatabaseUdfArgs>,
    #[cfg(test)]
    invocation_cleared: bool,
}

impl<RT: Runtime> DatabaseUdfWasmInvocation<RT> {
    pub(super) fn new(environment: DatabaseUdfEnvironment<RT>, args: DatabaseUdfArgs) -> Self {
        Self {
            provider: Some(environment.syscall_provider),
            args: Some(args),
            #[cfg(test)]
            invocation_cleared: false,
        }
    }

    fn provider(&self) -> anyhow::Result<&DatabaseUdfSyscallProvider<RT>> {
        self.provider
            .as_ref()
            .context("Static Hermes database invocation provider missing")
    }

    fn provider_mut(&mut self) -> anyhow::Result<&mut DatabaseUdfSyscallProvider<RT>> {
        self.provider
            .as_mut()
            .context("Static Hermes database invocation provider missing")
    }

    fn args(&self) -> anyhow::Result<&DatabaseUdfArgs> {
        self.args
            .as_ref()
            .context("Static Hermes database invocation arguments missing")
    }

    fn args_mut(&mut self) -> anyhow::Result<&mut DatabaseUdfArgs> {
        self.args
            .as_mut()
            .context("Static Hermes database invocation arguments missing")
    }

    pub(super) async fn prepare_execution(
        &mut self,
        timeout: &mut Timeout<RT>,
    ) -> anyhow::Result<()> {
        #[cfg(test)]
        anyhow::ensure!(
            !self.invocation_cleared,
            "Static Hermes test invocation was cleared"
        );
        let rng_seed = self.args()?.rng_seed;
        let unix_timestamp = self.args()?.unix_timestamp;
        self.initialize_static_hermes(timeout).await?;
        self.provider_mut()?
            .begin_execution(rng_seed, unix_timestamp)
    }

    pub(super) async fn initialize_static_hermes(
        &mut self,
        timeout: &mut Timeout<RT>,
    ) -> anyhow::Result<()> {
        self.provider_mut()?.initialize_static_hermes(timeout).await
    }

    pub(super) fn tx_for_initialization(&mut self) -> anyhow::Result<&mut Transaction<RT>> {
        self.provider_mut()?.phase.tx_mut()
    }

    pub(super) fn snoop_initialization_reads(&mut self) -> anyhow::Result<()> {
        self.provider_mut()?.phase.snoop_reads()
    }

    pub(super) fn finish_initialization_reads(
        &mut self,
    ) -> anyhow::Result<database::TransactionReadSet> {
        self.provider_mut()?.phase.finish_snoop()
    }

    pub(super) fn start_handler_read_capture(&mut self) -> anyhow::Result<()> {
        self.provider_mut()?.phase.start_handler_read_capture()
    }

    pub(super) fn finish_handler_read_capture(&mut self) -> anyhow::Result<()> {
        self.provider_mut()?.phase.finish_handler_read_capture()
    }

    pub(super) fn handler_read_capture_enabled(&self) -> anyhow::Result<bool> {
        Ok(self.provider()?.phase.handler_read_capture_enabled())
    }

    pub(super) fn tx(&mut self) -> anyhow::Result<&mut Transaction<RT>> {
        self.provider_mut()?.phase.tx()
    }

    pub(super) fn rt(&self) -> &RT {
        &self
            .provider
            .as_ref()
            .expect("Static Hermes provider missing")
            .rt
    }

    pub(super) fn udf_type(&self) -> UdfType {
        self.provider
            .as_ref()
            .expect("Static Hermes provider missing")
            .udf_type
    }

    pub(super) fn udf_path(&self) -> &CanonicalizedUdfPath {
        &self
            .provider
            .as_ref()
            .expect("Static Hermes provider missing")
            .path
            .udf_path
    }

    pub(super) fn npm_version(&self) -> anyhow::Result<&Version> {
        self.provider()?
            .udf_server_version
            .as_ref()
            .context("Static Hermes invocation npm version missing")
    }

    pub(super) fn allows_pending_values(&self) -> bool {
        let provider = self
            .provider
            .as_ref()
            .expect("Static Hermes provider missing");
        provider.udf_type == UdfType::Mutation || provider.reactor_depth > 0
    }

    pub(super) fn observed_identity(&self) -> bool {
        self.provider
            .as_ref()
            .expect("Static Hermes provider missing")
            .phase
            .observed_identity()
    }

    pub(super) fn observed_rng(&self) -> bool {
        self.provider
            .as_ref()
            .expect("Static Hermes provider missing")
            .phase
            .observed_rng()
    }

    pub(super) fn observed_time(&self) -> bool {
        self.provider
            .as_ref()
            .expect("Static Hermes provider missing")
            .phase
            .observed_time()
    }

    pub(super) fn unix_timestamp(&mut self) -> anyhow::Result<UnixTimestamp> {
        self.provider_mut()?.phase.unix_timestamp()
    }

    pub(super) fn invocation_unix_timestamp(&self) -> anyhow::Result<UnixTimestamp> {
        self.provider()?.phase.execution_unix_timestamp()
    }

    pub(super) fn observe_time(&mut self) -> anyhow::Result<()> {
        self.provider_mut()?.phase.observe_time();
        Ok(())
    }

    pub(super) fn rng(&mut self) -> anyhow::Result<&mut ChaCha12Rng> {
        self.provider_mut()?.phase.rng()
    }

    pub(super) fn emit_log_line(
        &mut self,
        level: LogLevel,
        messages: Vec<String>,
    ) -> anyhow::Result<()> {
        let provider = self.provider_mut()?;
        provider.emit_log_line(LogLine::new_developer_log_line(
            level,
            messages,
            provider.rt.unix_timestamp(),
        ));
        Ok(())
    }

    pub(super) fn clear_queries(&mut self) {
        self.provider
            .as_mut()
            .expect("Static Hermes provider missing")
            .query_manager
            .clear_developer_queries();
    }

    pub(super) fn has_active_queries(&self) -> bool {
        self.provider
            .as_ref()
            .expect("Static Hermes provider missing")
            .query_manager
            .has_developer_queries()
    }

    pub(super) fn into_outcome(
        &mut self,
        result: Result<PendingValue, common::errors::JsError>,
        execution_time: FunctionExecutionTime,
    ) -> anyhow::Result<(Transaction<RT>, FunctionOutcome)> {
        let provider = self
            .provider
            .take()
            .context("Static Hermes database invocation provider already taken")?;
        let args = self
            .args
            .take()
            .context("Static Hermes database invocation arguments already taken")?;
        provider.into_outcome(args, result, execution_time)
    }

    pub(super) fn take_transaction_for_system(&mut self) -> anyhow::Result<Transaction<RT>> {
        let provider = self
            .provider
            .take()
            .context("Static Hermes database invocation provider already taken")?;
        self.args
            .take()
            .context("Static Hermes database invocation arguments already taken")?;
        provider.phase.into_transaction()
    }

    pub(super) fn is_finished(&self) -> bool {
        self.provider.is_none() && self.args.is_none()
    }

    pub(super) fn set_terminal_host_operation_error(&mut self, error: HostOperationErrorV1) {
        self.provider
            .as_mut()
            .expect("Static Hermes provider missing")
            .set_terminal_host_operation_error(error);
    }

    pub(super) async fn run_async_syscall_batch(
        &mut self,
        batch: super::super::async_syscall::AsyncSyscallBatch,
        udf_callback: impl UdfCallback<RT>,
    ) -> Vec<super::super::async_syscall::AsyncSyscallResult> {
        super::super::async_syscall::run_async_syscall_batch(
            self.provider_mut().expect("Static Hermes provider missing"),
            batch,
            udf_callback,
        )
        .await
    }

    pub(super) async fn query_stream_next(
        &mut self,
        query_id: QueryId,
    ) -> anyhow::Result<JsonValue> {
        let results = self
            .run_async_syscall_batch(
                super::super::async_syscall::AsyncSyscallBatch::new(
                    "1.0/queryStreamNext".to_owned(),
                    json!({ "queryId": query_id }),
                ),
                StaticHermesUdfCallback::Unavailable,
            )
            .await;
        let [result] = <[_; 1]>::try_from(results)
            .map_err(|_| anyhow::anyhow!("Static Hermes query stream batch changed size"))?;
        serde_json::from_str(&result.result?)
            .context("Static Hermes query stream result was not valid JSON")
    }

    pub(super) fn get_environment_variable(
        &mut self,
        name: &EnvVarName,
    ) -> anyhow::Result<Option<EnvVarValue>> {
        self.provider_mut()?
            .phase
            .get_environment_variable(name.clone())
    }

    #[cfg(test)]
    pub(super) fn set_udf_type(&mut self, udf_type: UdfType) {
        self.provider
            .as_mut()
            .expect("Static Hermes test invocation provider missing")
            .udf_type = udf_type;
    }

    #[cfg(test)]
    pub(super) fn enable_host_operation_trace(&mut self) {
        self.provider
            .as_mut()
            .expect("Static Hermes test invocation provider missing")
            .host_operation_trace = udf::HostOperationTrace::for_query_shadow();
    }

    pub(super) fn host_operation_trace_entries(
        &self,
    ) -> anyhow::Result<Option<&[udf::HostOperationTraceEntry]>> {
        Ok(self.provider()?.host_operation_trace.entries())
    }

    pub(super) fn invocation_correlation(&self) -> anyhow::Result<String> {
        Ok(self.provider()?.context.execution_id.to_string())
    }

    #[cfg(test)]
    pub(super) fn set_rng_seed(&mut self, rng_seed: [u8; 32]) {
        self.args_mut()
            .expect("Static Hermes test invocation arguments missing")
            .rng_seed = rng_seed;
    }

    #[cfg(test)]
    pub(super) fn set_invocation(
        &mut self,
        path: ResolvedComponentFunctionPath,
        context: ExecutionContext,
        deployment: DeploymentMetadata,
        npm_version: Version,
        unix_timestamp: UnixTimestamp,
    ) {
        let provider = self
            .provider_mut()
            .expect("Static Hermes test invocation provider missing");
        provider.path = path;
        provider.context = context;
        provider.deployment = deployment;
        provider.udf_server_version = Some(npm_version);
        let args = self
            .args_mut()
            .expect("Static Hermes test invocation arguments missing");
        args.unix_timestamp = unix_timestamp;
        self.invocation_cleared = false;
    }

    #[cfg(test)]
    pub(super) fn clear_invocation(&mut self) {
        self.invocation_cleared = true;
    }

    #[cfg(test)]
    pub(super) fn take_transaction(&mut self) -> anyhow::Result<Transaction<RT>> {
        self.take_transaction_for_system()
    }

    #[cfg(test)]
    pub(super) fn take_log_lines(&mut self) -> LogLines {
        std::mem::take(
            &mut self
                .provider
                .as_mut()
                .expect("Static Hermes test invocation provider missing")
                .log_lines,
        )
    }

    #[cfg(test)]
    pub(super) fn set_test_file_storage_context(
        &mut self,
        key_broker: FunctionRunnerKeyBroker,
        file_storage: TransactionalFileStorage<RT>,
        deployment: DeploymentMetadata,
    ) {
        let provider = self
            .provider_mut()
            .expect("Static Hermes test invocation provider missing");
        provider.key_broker = key_broker;
        provider.file_storage = file_storage;
        provider.deployment = deployment;
    }

    #[cfg(test)]
    pub(super) async fn file_storage_generate_upload_url(&mut self) -> anyhow::Result<String> {
        self.provider_mut()?
            .file_storage_generate_upload_url()
            .await
    }

    #[cfg(test)]
    pub(super) async fn file_storage_get_url_batch(
        &mut self,
        storage_ids: BTreeMap<BatchKey, FileStorageId>,
    ) -> BTreeMap<BatchKey, anyhow::Result<Option<String>>> {
        let provider = match self.provider_mut() {
            Ok(provider) => provider,
            Err(error) => {
                return storage_ids
                    .into_keys()
                    .map(|batch_key| (batch_key, Err(error.clone_error())))
                    .collect();
            },
        };
        provider.file_storage_get_url_batch(storage_ids).await
    }

    #[cfg(test)]
    pub(super) async fn file_storage_delete(
        &mut self,
        storage_id: FileStorageId,
    ) -> anyhow::Result<()> {
        self.provider_mut()?.file_storage_delete(storage_id).await
    }

    #[cfg(test)]
    pub(super) async fn file_storage_get_entry(
        &mut self,
        storage_id: FileStorageId,
    ) -> anyhow::Result<Option<FileStorageEntry>> {
        self.provider_mut()?
            .file_storage_get_entry(storage_id)
            .await
    }

    #[cfg(test)]
    pub(super) fn new_for_test(
        rt: ProdRuntime,
        transaction: Transaction<ProdRuntime>,
        journal: QueryJournal,
        capture_handler_reads: bool,
    ) -> anyhow::Result<DatabaseUdfWasmInvocation<ProdRuntime>> {
        let identity = transaction.inert_identity();
        let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
        let provider = DatabaseUdfSyscallProvider {
            rt: rt.clone(),
            udf_type: UdfType::Mutation,
            path: ResolvedComponentFunctionPath {
                component: ComponentId::Root,
                udf_path: "generated_test:run".parse()?,
                component_path: ComponentPath::root(),
            },
            udf_server_version: Some(Version::new(1, 43, 0)),
            deployment: DeploymentMetadata {
                name: "generated-test".to_owned(),
                region: None,
                class: DeploymentClass::S16,
            },
            client_id: "generated-test".to_owned(),
            phase: super::super::phase::UdfPhase::new(
                transaction,
                rt.clone(),
                Arc::new(UnusedGateModuleCache),
                BTreeMap::new(),
                ComponentId::Root,
                capture_handler_reads,
            ),
            file_storage: TransactionalFileStorage::new(
                rt,
                storage,
                ConvexOrigin::from("http://127.0.0.1:3210".to_owned()),
            ),
            query_manager: super::super::async_syscall::QueryManager::new(),
            key_broker: KeyBroker::dev().function_runner_keybroker(),
            log_lines: vec![].into(),
            audit_log_lines: vec![].into(),
            prev_journal: journal,
            next_journal: QueryJournal::new(),
            syscall_trace: udf::SyscallTrace::new(),
            host_operation_trace: udf::HostOperationTrace::default(),
            terminal_host_operation_error: None,
            handler_read_capture_started: false,
            context: generated_test_execution_context(),
            reactor_depth: 0,
            shadow_work_guard: None,
        };
        Ok(DatabaseUdfWasmInvocation::<ProdRuntime> {
            provider: Some(provider),
            args: Some(DatabaseUdfArgs {
                unix_timestamp: UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                rng_seed: [0; 32],
                udf_args: SerializedArgs::from_args(vec![])?,
                identity,
                reuse_context: false,
            }),
            invocation_cleared: false,
        })
    }
}

#[cfg(test)]
pub(super) struct UnusedGateModuleCache;

#[cfg(test)]
#[async_trait::async_trait]
impl ModuleCache<ProdRuntime> for UnusedGateModuleCache {
    async fn get_module_with_metadata(
        &self,
        _module_metadata: &common::document::ParsedDocument<model::modules::types::ModuleMetadata>,
        _source_package: &common::document::ParsedDocument<
            model::source_packages::types::SourcePackage,
        >,
    ) -> anyhow::Result<Arc<V8ModuleSource>> {
        anyhow::bail!("generated storage provider test must not load a JavaScript module")
    }

    fn put_cached_code(
        &self,
        _module_metadata: &model::modules::types::ModuleMetadata,
        _cached_data: Arc<[u8]>,
    ) {
    }

    fn get_cached_code(
        &self,
        _module_metadata: &model::modules::types::ModuleMetadata,
    ) -> Option<Arc<[u8]>> {
        None
    }
}

fn pending_values_allowed_for_invocation(udf_type: UdfType, reactor_depth: usize) -> bool {
    udf_type == UdfType::Mutation || reactor_depth > 0
}

#[cfg(test)]
#[test]
fn pending_value_policy_matches_mutation_and_nested_udf_boundaries() {
    assert!(!pending_values_allowed_for_invocation(UdfType::Query, 0));
    assert!(pending_values_allowed_for_invocation(UdfType::Mutation, 0));
    assert!(pending_values_allowed_for_invocation(UdfType::Query, 1));
}

#[cfg(test)]
#[test]
fn static_hermes_wasi_args_are_the_fixed_guest_program_name() {
    assert_eq!(
        STATIC_HERMES_WASI_ARGS,
        &[b"static-hermes-backend-gate" as &[u8]]
    );
}

#[derive(Clone)]
pub(super) enum StaticHermesUdfCallback<RT: Runtime> {
    Isolate(IsolateClient<RT>),
    Unavailable,
}

impl<RT: Runtime> UdfCallback<RT> for StaticHermesUdfCallback<RT> {
    async fn execute_nested_udf(
        self,
        client_id: String,
        udf_request: UdfRequest<RT>,
        rng_seed: [u8; 32],
        reactor_depth: usize,
    ) -> anyhow::Result<(Transaction<RT>, NestedUdfOutcome)> {
        match self {
            Self::Isolate(client) => {
                (&client)
                    .execute_nested_udf(client_id, udf_request, rng_seed, reactor_depth)
                    .await
            },
            Self::Unavailable => {
                anyhow::bail!("generated gate nested UDF callback missing")
            },
        }
    }
}

impl<RT: Runtime> SyscallProviderInternal<RT> for DatabaseUdfWasmInvocation<RT> {
    fn start_logical_host_operation(
        &mut self,
        operation: LogicalHostOperation,
    ) -> Option<HostOperationTraceEntryHandle> {
        SyscallProviderInternal::start_logical_host_operation(
            self.provider_mut().expect("Static Hermes provider missing"),
            operation,
        )
    }

    fn complete_logical_host_operation(
        &mut self,
        entry: Option<HostOperationTraceEntryHandle>,
        status: LogicalHostOperationStatus,
    ) {
        SyscallProviderInternal::complete_logical_host_operation(
            self.provider_mut().expect("Static Hermes provider missing"),
            entry,
            status,
        );
    }

    fn table_filter(&self) -> TableFilter {
        SyscallProviderInternal::table_filter(
            self.provider
                .as_ref()
                .expect("Static Hermes provider missing"),
        )
    }

    fn lookup_table(&mut self, name: &TableName) -> anyhow::Result<Option<TabletIdAndTableNumber>> {
        SyscallProviderInternal::lookup_table(self.provider_mut()?, name)
    }

    fn lookup_virtual_table(&mut self, name: &TableName) -> anyhow::Result<Option<TableNumber>> {
        SyscallProviderInternal::lookup_virtual_table(self.provider_mut()?, name)
    }

    fn component_argument(&self, _name: &str) -> anyhow::Result<Option<ConvexValue>> {
        SyscallProviderInternal::component_argument(self.provider()?, _name)
    }

    fn start_query(&mut self, query: Query, version: Option<Version>) -> anyhow::Result<u32> {
        SyscallProviderInternal::start_query(self.provider_mut()?, query, version)
    }

    fn cleanup_query(&mut self, query_id: u32) -> bool {
        SyscallProviderInternal::cleanup_query(
            self.provider_mut().expect("Static Hermes provider missing"),
            query_id,
        )
    }

    fn require_operation(&mut self, _op: DeploymentOp) -> anyhow::Result<()> {
        SyscallProviderInternal::require_operation(self.provider_mut()?, _op)
    }

    fn snapshot_ts(&mut self) -> anyhow::Result<ConvexValue> {
        SyscallProviderInternal::snapshot_ts(self.provider_mut()?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GeneratedDeveloperError {
    pub(super) message: String,
    pub(super) host_operation_error: Option<HostOperationErrorV1>,
}

impl From<String> for GeneratedDeveloperError {
    fn from(message: String) -> Self {
        Self {
            message,
            host_operation_error: None,
        }
    }
}

pub(super) struct HostState<RT: Runtime> {
    pub(super) provider: DatabaseUdfWasmInvocation<RT>,
    pub(super) udf_callback: StaticHermesUdfCallback<RT>,
    // WASI monotonic time has an arbitrary epoch, but retained guest state can
    // compare readings across invocations when its Store is reused.
    pub(super) wasi_monotonic_epoch: tokio::time::Instant,
    pub(super) timeout: Option<Timeout<RT>>,
    pub(super) active_wasm_cpu_limiter: ConcurrencyLimiter,
    pub(super) teardown_cpu_permit: Option<ConcurrencyPermit>,
    pub(super) read_control: Option<ReadControl>,
    pub(super) developer_error: Option<GeneratedDeveloperError>,
    // A developer error reported by the guest is safe to return to the pool
    // after the normal invocation cleanup. Host-reported deterministic errors
    // use the same `developer_error` field but must still retire the runtime.
    pub(super) guest_developer_error: bool,
    pub(super) stdout: Vec<u8>,
    pub(super) stderr: Vec<u8>,
    pub(super) metrics: Arc<GateMetrics>,
    pub(super) instance_id: u64,
    pub(super) backend_timeout_armed: bool,
    pub(super) profile_last_mark: Option<(Instant, Option<u64>)>,
    pub(super) generated: Option<GeneratedInvocationState>,
}

impl<RT: Runtime> Drop for HostState<RT> {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "testing"))]
        self.metrics.store_drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) struct GeneratedInvocationState {
    pub(super) manifest: Arc<WasmUdfExecutionPolicy>,
    pub(super) values: OpaqueValueTable,
    pub(super) async_operations: AsyncOperationState,
    pub(super) capability_bridge: InvocationCapabilityBridge,
    // Replacing GeneratedInvocationState on every attempt resets elapsed time
    // even when the Store and Static Hermes runtime come from the reuse pool.
    pub(super) performance_monotonic_start: tokio::time::Instant,
    pub(super) performance_runtime_available: bool,
    pub(super) runtime_reuse_contaminated: bool,
    pub(super) allows_caught_official_output_chunk_initialization_failure: bool,
    pub(super) discard_after_caught_initialization_failure: bool,
    pub(super) context_read_set_required: bool,
    pub(super) request_handle: OpaqueHandle,
    pub(super) operation_count: u64,
    // Host imports turn typed Rust failures into Wasmtime traps. Retain the
    // first bounded category separately so the trap wrapper cannot erase the
    // operator-visible cause.
    pub(super) terminal_failure: Option<StaticHermesWasmExecutionFailure>,
    pub(super) cancellation: CancellationSignal,
    pub(super) interrupt: Arc<GeneratedInterruptState>,
    pub(super) memory_limiter: GeneratedStoreLimiter,
    pub(super) memory_permit: Option<InvocationMemoryPermit>,
    pub(super) host_secret_values: BTreeMap<String, HostSecretValue>,
}

pub(super) struct GeneratedInterruptState {
    timeout_reason: parking_lot::Mutex<Option<GeneratedTimeoutReason>>,
    execution_phase: AtomicU8,
    destroying: AtomicBool,
    pub(super) destruction_timed_out: AtomicBool,
    #[cfg(any(test, feature = "testing"))]
    timeout_armed_at: parking_lot::Mutex<Option<Instant>>,
    #[cfg(any(test, feature = "testing"))]
    timeout_terminated_at: parking_lot::Mutex<Option<Instant>>,
}

#[derive(Clone, Copy)]
pub(super) enum GeneratedTimeoutReason {
    Initialization,
    Active,
    System(Duration),
    Invalid,
}

#[cfg(any(test, feature = "testing"))]
pub(super) struct GeneratedTimeoutDiagnostic {
    pub(super) armed_before_execution: Option<Duration>,
    pub(super) reason: Option<&'static str>,
    pub(super) after_execution_start: Option<Duration>,
}

impl Default for GeneratedInterruptState {
    fn default() -> Self {
        Self {
            timeout_reason: parking_lot::Mutex::new(None),
            execution_phase: AtomicU8::new(GeneratedExecutionPhase::Active as u8),
            destroying: AtomicBool::new(false),
            destruction_timed_out: AtomicBool::new(false),
            #[cfg(any(test, feature = "testing"))]
            timeout_armed_at: parking_lot::Mutex::new(None),
            #[cfg(any(test, feature = "testing"))]
            timeout_terminated_at: parking_lot::Mutex::new(None),
        }
    }
}

impl GeneratedInterruptState {
    pub(super) fn set_execution_phase(&self, phase: GeneratedExecutionPhase) {
        self.execution_phase.store(phase as u8, Ordering::Release);
    }

    pub(super) fn execution_phase(&self) -> GeneratedExecutionPhase {
        match self.execution_phase.load(Ordering::Acquire) {
            value if value == GeneratedExecutionPhase::Preparing as u8 => {
                GeneratedExecutionPhase::Preparing
            },
            value if value == GeneratedExecutionPhase::Active as u8 => {
                GeneratedExecutionPhase::Active
            },
            _ => unreachable!("generated execution phase is invalid"),
        }
    }

    pub(super) fn is_active(&self) -> bool {
        self.execution_phase() == GeneratedExecutionPhase::Active
    }

    #[cfg(any(test, feature = "testing"))]
    pub(super) fn record_timeout_arm(&self) {
        *self.timeout_armed_at.lock() = Some(Instant::now());
    }

    pub(super) fn terminate(&self, reason: TerminationReason) {
        let reason = match reason {
            TerminationReason::Isolate(IsolateTerminationReason::UserTimeout(_)) => {
                match self.execution_phase() {
                    GeneratedExecutionPhase::Preparing => GeneratedTimeoutReason::Initialization,
                    GeneratedExecutionPhase::Active => GeneratedTimeoutReason::Active,
                }
            },
            TerminationReason::Isolate(IsolateTerminationReason::SystemTimeout(duration)) => {
                GeneratedTimeoutReason::System(duration)
            },
            TerminationReason::Isolate(
                IsolateTerminationReason::SystemError(_) | IsolateTerminationReason::OutOfMemory,
            )
            | TerminationReason::Context(_) => GeneratedTimeoutReason::Invalid,
        };
        *self.timeout_reason.lock() = Some(reason);
        #[cfg(any(test, feature = "testing"))]
        {
            *self.timeout_terminated_at.lock() = Some(Instant::now());
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub(super) fn diagnostic(&self, execution_started: Instant) -> GeneratedTimeoutDiagnostic {
        let armed_before_execution = self
            .timeout_armed_at
            .lock()
            .as_ref()
            .map(|armed| execution_started.saturating_duration_since(*armed));
        let reason = self.timeout_reason().map(|reason| match reason {
            GeneratedTimeoutReason::Initialization => "initialization",
            GeneratedTimeoutReason::Active => "active",
            GeneratedTimeoutReason::System(_) => "system",
            GeneratedTimeoutReason::Invalid => "invalid",
        });
        let after_execution_start = self
            .timeout_terminated_at
            .lock()
            .as_ref()
            .map(|terminated| terminated.saturating_duration_since(execution_started));
        GeneratedTimeoutDiagnostic {
            armed_before_execution,
            reason,
            after_execution_start,
        }
    }

    pub(super) fn timeout_reason(&self) -> Option<GeneratedTimeoutReason> {
        *self.timeout_reason.lock()
    }

    pub(super) fn begin_destruction(&self) {
        self.destruction_timed_out.store(false, Ordering::Release);
        self.destroying.store(true, Ordering::Release);
    }

    pub(super) fn should_interrupt(&self, cancellation: &CancellationSignal) -> bool {
        if self.destroying.load(Ordering::Acquire) {
            self.destruction_timed_out.load(Ordering::Acquire)
        } else {
            self.timeout_reason.lock().is_some() || cancellation.is_cancelled()
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum GeneratedExecutionPhase {
    Preparing,
    Active,
}

impl GeneratedInvocationState {
    fn latch_terminal_failure(&mut self, failure: StaticHermesWasmExecutionFailure) {
        self.terminal_failure.get_or_insert(failure);
    }

    pub(super) fn opaque_value_operation<T>(
        &mut self,
        operation: impl FnOnce(&mut OpaqueValueTable) -> Result<T, OpaqueValueError>,
    ) -> Result<T, WasmtimeError> {
        operation(&mut self.values).map_err(|error| {
            let failure = StaticHermesWasmExecutionFailure::from(&error);
            self.latch_terminal_failure(failure);
            WasmtimeError::new(HostInvariant)
        })
    }

    fn next_operation_count(&mut self, additional_operations: u64) -> Result<u64, WasmtimeError> {
        let Some(operation_count) = self.operation_count.checked_add(additional_operations) else {
            self.latch_terminal_failure(StaticHermesWasmExecutionFailure::OperationLimit);
            return Err(WasmtimeError::new(HostInvariant));
        };
        if operation_count > self.manifest.limits().max_operation_count() {
            self.latch_terminal_failure(StaticHermesWasmExecutionFailure::OperationLimit);
            return Err(WasmtimeError::new(HostInvariant));
        }
        Ok(operation_count)
    }

    pub(super) fn count_operation(&mut self) -> Result<(), WasmtimeError> {
        self.operation_count = self.next_operation_count(1)?;
        Ok(())
    }

    pub(super) fn operations(
        &mut self,
        operation_ids: impl ExactSizeIterator<Item = u32>,
    ) -> Result<Vec<ImportedOperationDescriptor>, WasmtimeError> {
        let additional_operations =
            u64::try_from(operation_ids.len()).map_err(|_| WasmtimeError::new(HostInvariant))?;
        let operation_count = self.next_operation_count(additional_operations)?;
        let operations = operation_ids
            .map(|operation_id| {
                self.manifest
                    .imported_operation(operation_id)
                    .map(|operation| operation.operation().clone())
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.operation_count = operation_count;
        Ok(operations)
    }

    pub(super) fn operation(
        &mut self,
        operation_id: i32,
    ) -> Result<ImportedOperationDescriptor, WasmtimeError> {
        let operation_id =
            u32::try_from(operation_id).map_err(|_| WasmtimeError::new(HostInvariant))?;
        let operation_count = self.next_operation_count(1)?;
        let operation = self
            .manifest
            .imported_operation(operation_id)
            .map(|operation| operation.operation().clone())
            .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
        self.operation_count = operation_count;
        Ok(operation)
    }
}

pub(super) fn guest_memory<RT: Runtime>(
    caller: &mut Caller<'_, HostState<RT>>,
) -> Result<Memory, WasmtimeError> {
    caller
        .get_export("memory")
        .and_then(Extern::into_memory)
        .ok_or_else(|| WasmtimeError::new(HostInvariant))
}

pub(super) fn checked_offset(value: i32) -> Result<usize, WasmtimeError> {
    usize::try_from(value).map_err(|_| WasmtimeError::new(HostInvariant))
}

pub(super) fn checked_len(value: i32, maximum: usize) -> Result<usize, WasmtimeError> {
    let length = usize::try_from(value).map_err(|_| WasmtimeError::new(HostInvariant))?;
    if length > maximum {
        return Err(WasmtimeError::new(HostInvariant));
    }
    Ok(length)
}

pub(super) fn with_guest_bytes<RT: Runtime, T>(
    caller: &mut Caller<'_, HostState<RT>>,
    pointer: i32,
    length: i32,
    maximum: usize,
    read: impl FnOnce(&[u8]) -> Result<T, WasmtimeError>,
) -> Result<T, WasmtimeError> {
    let length = checked_len(length, maximum)?;
    let start = checked_offset(pointer)?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
    let memory = guest_memory(caller)?;
    let bytes = memory
        .data(caller)
        .get(start..end)
        .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
    // Keep the Wasmtime memory borrow confined to the decoder. Callers must
    // finish parsing or hashing before mutating HostState, which avoids a
    // temporary Vec for every guest-to-host payload.
    read(bytes)
}

pub(super) fn write_guest_bytes<RT: Runtime>(
    caller: &mut Caller<'_, HostState<RT>>,
    pointer: i32,
    bytes: &[u8],
) -> Result<(), WasmtimeError> {
    guest_memory(caller)?
        .write(caller, checked_offset(pointer)?, bytes)
        .map_err(WasmtimeError::from)
}

pub(super) fn write_guest_bytes_from_state<RT: Runtime, F>(
    caller: &mut Caller<'_, HostState<RT>>,
    pointer: i32,
    capacity: usize,
    source: F,
) -> Result<usize, WasmtimeError>
where
    F: for<'a> FnOnce(&'a HostState<RT>) -> Result<&'a [u8], WasmtimeError>,
{
    // Wasmtime can lend the guest allocation and store data at the same time
    // because the guest memory and HostState are disjoint. Keep the source
    // borrowed from the opaque table while copying directly into guest memory;
    // allocating a temporary Vec here would double every bridge payload and
    // string transfer. The capacity check must stay before the slice copy so an
    // undersized guest buffer remains a typed host invariant failure rather
    // than a partial transfer.
    let memory = guest_memory(caller)?;
    let offset = checked_offset(pointer)?;
    let (guest_bytes, state) = memory.data_and_store_mut(caller);
    let bytes = source(state)?;
    if capacity < bytes.len() {
        return Err(WasmtimeError::new(HostInvariant));
    }
    let destination = guest_bytes
        .get_mut(
            offset
                ..offset
                    .checked_add(bytes.len())
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))?,
        )
        .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
    destination.copy_from_slice(bytes);
    Ok(bytes.len())
}

pub(super) fn consume_generated_fuel<RT: Runtime>(
    caller: &mut Caller<'_, HostState<RT>>,
    fuel: u64,
) -> Result<(), WasmtimeError> {
    let remaining = caller.get_fuel()?;
    let Some(remaining) = remaining.checked_sub(fuel) else {
        caller.set_fuel(0)?;
        return Err(WasmtimeError::new(Trap::OutOfFuel));
    };
    caller.set_fuel(remaining)
}

pub(super) fn consume_generated_random_fuel<RT: Runtime>(
    caller: &mut Caller<'_, HostState<RT>>,
    entropy_bytes: u64,
) -> Result<(), WasmtimeError> {
    let fuel = entropy_bytes
        .checked_mul(RANDOM_FUEL_PER_ENTROPY_BYTE)
        .and_then(|fuel| fuel.checked_add(RANDOM_BASE_FUEL))
        .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
    consume_generated_fuel(caller, fuel)
}

pub(super) fn write_u32<RT: Runtime>(
    caller: &mut Caller<'_, HostState<RT>>,
    pointer: i32,
    value: u32,
) -> Result<(), WasmtimeError> {
    write_guest_bytes(caller, pointer, &value.to_le_bytes())
}

pub(super) fn write_u64<RT: Runtime>(
    caller: &mut Caller<'_, HostState<RT>>,
    pointer: i32,
    value: u64,
) -> Result<(), WasmtimeError> {
    write_guest_bytes(caller, pointer, &value.to_le_bytes())
}

pub(super) fn read_u32<RT: Runtime>(
    caller: &mut Caller<'_, HostState<RT>>,
    offset: usize,
) -> Result<u32, WasmtimeError> {
    let mut bytes = [0; 4];
    guest_memory(caller)?
        .read(caller, offset, &mut bytes)
        .map_err(WasmtimeError::from)?;
    Ok(u32::from_le_bytes(bytes))
}

pub(super) async fn require_pending_read<T>(
    future: impl Future<Output = T>,
    control: ReadControl,
    metrics: Arc<GateMetrics>,
) -> Result<T, WasmtimeError> {
    let mut future = Box::pin(future);
    let mut ready = None;
    let was_pending = std::future::poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(true),
        Poll::Ready(result) => {
            ready = Some(result);
            Poll::Ready(false)
        },
    })
    .await;
    if !was_pending {
        drop(ready);
        return Err(WasmtimeError::new(HostInvariant));
    }

    let mut lease = PendingReadLease {
        metrics,
        completed: false,
    };
    match control.entered {
        #[cfg(test)]
        ReadEntered::Direct(entered) => entered
            .send(())
            .map_err(|_| WasmtimeError::new(HostInvariant))?,
        ReadEntered::TestHooks {
            sender,
            instance_id,
        } => sender
            .send(instance_id)
            .map_err(|_| WasmtimeError::new(HostInvariant))?,
    }
    control
        .release
        .await
        .map_err(|_| WasmtimeError::new(HostInvariant))?;
    let result = future.await;
    lease.complete();
    Ok(result)
}

pub(super) fn classify_host_result<RT: Runtime, T>(
    state: &mut HostState<RT>,
    result: anyhow::Result<T>,
) -> Result<Option<T>, WasmtimeError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.is_deterministic_user_error() => {
            state.developer_error = Some(error.short_msg().to_owned().into());
            Ok(None)
        },
        Err(_) => Err(WasmtimeError::new(HostInvariant)),
    }
}

pub(super) fn generated_state<RT: Runtime>(
    state: &HostState<RT>,
) -> Result<&GeneratedInvocationState, WasmtimeError> {
    state
        .generated
        .as_ref()
        .ok_or_else(|| WasmtimeError::new(HostInvariant))
}

pub(super) fn generated_state_mut<RT: Runtime>(
    state: &mut HostState<RT>,
) -> Result<&mut GeneratedInvocationState, WasmtimeError> {
    state
        .generated
        .as_mut()
        .ok_or_else(|| WasmtimeError::new(HostInvariant))
}

pub(super) fn require_active_generated_execution<RT: Runtime>(
    state: &HostState<RT>,
) -> Result<(), WasmtimeError> {
    if !generated_state(state)?.interrupt.is_active() {
        return Err(WasmtimeError::new(HostInvariant));
    }
    Ok(())
}

pub(super) fn opaque_handle(value: i64) -> Result<OpaqueHandle, WasmtimeError> {
    OpaqueHandle::from_abi(value).map_err(|_| WasmtimeError::new(HostInvariant))
}

pub(super) fn read_generated_utf8<RT: Runtime>(
    caller: &mut Caller<'_, HostState<RT>>,
    pointer: i32,
    length: i32,
    maximum: usize,
) -> Result<String, WasmtimeError> {
    with_guest_bytes(caller, pointer, length, maximum, |bytes| {
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| WasmtimeError::new(HostInvariant))
    })
}

pub(super) fn start_generated_query<RT: Runtime>(
    state: &mut HostState<RT>,
    operation_id: i32,
    value: JsonValue,
) -> Result<i64, WasmtimeError> {
    let npm_version = state
        .provider
        .npm_version()
        .map_err(|_| WasmtimeError::new(HostInvariant))?
        .clone();
    let descriptor = generated_state_mut(state)?.operation(operation_id)?;
    let values = match &descriptor {
        ImportedOperationDescriptor::DatabaseIndexQuery {
            equality_field: Some(_),
            constraints: None,
            ..
        } => vec![value],
        ImportedOperationDescriptor::DatabaseIndexQuery {
            equality_field: None,
            constraints: Some(_),
            ..
        } => {
            let JsonValue::Array(values) = value else {
                return Err(WasmtimeError::new(HostInvariant));
            };
            values
        },
        _ => return Err(WasmtimeError::new(HostInvariant)),
    };
    let Some(args) = classify_host_result(
        state,
        generated_query_syscall_args(&descriptor, values, &npm_version)?,
    )?
    else {
        return Ok(-1);
    };
    #[cfg(any(test, feature = "testing"))]
    state
        .metrics
        .database_query_starts
        .fetch_add(1, Ordering::SeqCst);
    let result = syscall_impl(&mut state.provider, "1.0/queryStream", args);
    let Some(result) = classify_host_result(state, result)? else {
        return Ok(-1);
    };
    let query_id = query_id_from_start_result(&result)?;
    generated_state_mut(state)?
        .opaque_value_operation(|values| values.insert(OpaqueValue::QueryCursor(query_id)))
        .map(OpaqueHandle::to_abi)
}

fn query_id_from_start_result(result: &JsonValue) -> Result<QueryId, WasmtimeError> {
    result
        .get("queryId")
        .and_then(JsonValue::as_u64)
        .and_then(|query_id| QueryId::try_from(query_id).ok())
        .ok_or_else(|| WasmtimeError::new(HostInvariant))
}

pub(super) async fn run_query_next<RT: Runtime>(
    state: &mut HostState<RT>,
    query_id: QueryId,
    cancellation: Option<CancellationSignal>,
) -> Result<Option<QueryNext>, WasmtimeError> {
    let control = state.read_control.take();
    let metrics = Arc::clone(&state.metrics);
    let name = "1.0/queryStreamNext".to_owned();
    let read = state.provider.query_stream_next(query_id);
    let read = async move {
        let read = async move {
            match control {
                Some(control) => require_pending_read(read, control, Arc::clone(&metrics)).await,
                None => {
                    let result = read.await;
                    #[cfg(any(test, feature = "testing"))]
                    metrics.read_completed.fetch_add(1, Ordering::SeqCst);
                    Ok(result)
                },
            }
        };
        match cancellation {
            Some(cancellation) => {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        Err(WasmtimeError::new(HostInvariant))
                    },
                    result = read => result,
                }
            },
            None => read.await,
        }
    };
    let result = state
        .timeout
        .as_mut()
        .context("gate timeout missing")
        .map_err(|_| WasmtimeError::new(HostInvariant))?
        .with_release_permit(PauseReason::DatabaseSyscall { name }, async move {
            read.await
                .map_err(|_| anyhow::anyhow!("query next host invariant failed"))
        })
        .await
        .map_err(|_| WasmtimeError::new(HostInvariant))?;
    let Some(result) = classify_host_result(state, result)? else {
        return Ok(None);
    };
    parse_query_next(result).map(Some)
}

/// Takes a consuming ABI operand. The guest must not release or reuse the
/// handle after calling an import that accepts one of these values.
pub(super) fn take_generated_json_value<RT: Runtime>(
    state: &mut HostState<RT>,
    handle: i64,
) -> Result<JsonValue, WasmtimeError> {
    let handle = opaque_handle(handle)?;
    let value = generated_state_mut(state)?.opaque_value_operation(|values| {
        values.take_transferred(handle, OpaqueValueKind::ConvexJson)
    })?;
    let OpaqueValue::ConvexJson(value) = value else {
        unreachable!("validated generated JSON value changed kind")
    };
    Ok(value)
}

fn generated_database_normalize_id_syscall(
    descriptor: ImportedOperationDescriptor,
    id_string: JsonValue,
) -> Result<(&'static str, JsonValue), WasmtimeError> {
    let ImportedOperationDescriptor::DatabaseNormalizeId { table_name } = descriptor else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    Ok((
        "1.0/db/normalizeId",
        json!({
            "table": table_name,
            "idString": id_string,
        }),
    ))
}

#[cfg(test)]
#[test]
fn database_normalize_id_materializes_exact_sync_syscall() -> anyhow::Result<()> {
    let (name, args) = generated_database_normalize_id_syscall(
        ImportedOperationDescriptor::DatabaseNormalizeId {
            table_name: "items".to_owned(),
        },
        json!("legacy-or-canonical-id"),
    )?;
    assert_eq!(name, "1.0/db/normalizeId");
    assert_eq!(
        args,
        json!({
            "table": "items",
            "idString": "legacy-or-canonical-id",
        })
    );
    assert!(generated_database_normalize_id_syscall(
        ImportedOperationDescriptor::DatabaseGet {
            table_name: "items".to_owned(),
        },
        json!("id"),
    )
    .is_err());
    Ok(())
}

pub(super) fn run_generated_database_normalize_id<RT: Runtime>(
    state: &mut HostState<RT>,
    operation_id: i32,
    id_string_handle: i64,
) -> Result<i64, WasmtimeError> {
    // Consume a valid operand before checking the descriptor so every terminal
    // path invalidates guest ownership, including a wrong-kind operand.
    let id_string_handle = opaque_handle(id_string_handle)?;
    let id_string = generated_state_mut(state)?
        .opaque_value_operation(|values| values.take_transferred_operand(id_string_handle))?;
    let OpaqueValue::ConvexJson(id_string) = id_string else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    let descriptor = generated_state_mut(state)?.operation(operation_id)?;
    let (name, args) = generated_database_normalize_id_syscall(descriptor, id_string)?;
    let result = syscall_impl(&mut state.provider, name, args);
    let Some(result) = classify_host_result(state, result)? else {
        return Ok(-1);
    };
    let normalized_id = result
        .as_object()
        .and_then(|result| result.get("id"))
        .filter(|id| id.is_string() || id.is_null())
        .cloned()
        .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
    generated_state_mut(state)?
        .opaque_value_operation(|values| values.insert_json(normalized_id))
        .map(OpaqueHandle::to_abi)
}

pub(super) fn generated_database_update_args<RT: Runtime>(
    state: &mut HostState<RT>,
    table_name: String,
    id_handle: i64,
    value_handle: i64,
) -> Result<JsonValue, WasmtimeError> {
    let id = take_generated_json_value(state, id_handle)?;
    let value = take_generated_json_value(state, value_handle)?;
    Ok(json!({
        "table": table_name,
        "id": id,
        "value": value,
    }))
}

pub(super) fn scheduler_timestamp(
    descriptor: &ImportedOperationDescriptor,
    value_milliseconds: f64,
    unix_timestamp: UnixTimestamp,
) -> anyhow::Result<f64> {
    if !value_milliseconds.is_finite() {
        return Err(ErrorMetadata::bad_request(
            "InvalidArgument",
            "The scheduler time must be a finite number",
        )
        .into());
    }
    match descriptor {
        ImportedOperationDescriptor::SchedulerRunAfter { .. } => {
            scheduler_run_after_timestamp(value_milliseconds, unix_timestamp)
        },
        ImportedOperationDescriptor::SchedulerRunAt { .. } => Ok(value_milliseconds / 1000.0),
        ImportedOperationDescriptor::Sha256
        | ImportedOperationDescriptor::AuthenticationGetUserIdentity {}
        | ImportedOperationDescriptor::HostSecretVerify { .. }
        | ImportedOperationDescriptor::FunctionHandleCreate {}
        | ImportedOperationDescriptor::DatabaseNormalizeId { .. }
        | ImportedOperationDescriptor::DatabaseGet { .. }
        | ImportedOperationDescriptor::DatabaseIndexQuery { .. }
        | ImportedOperationDescriptor::DatabaseInsert { .. }
        | ImportedOperationDescriptor::DatabasePatch { .. }
        | ImportedOperationDescriptor::DatabaseReplace { .. }
        | ImportedOperationDescriptor::DatabaseDelete { .. } => {
            anyhow::bail!("scheduler import selected a non-scheduler operation")
        },
    }
}

pub(super) fn scheduler_run_after_timestamp(
    delay_milliseconds: f64,
    unix_timestamp: UnixTimestamp,
) -> anyhow::Result<f64> {
    if !delay_milliseconds.is_finite() {
        return Err(ErrorMetadata::bad_request(
            "InvalidArgument",
            "The scheduler time must be a finite number",
        )
        .into());
    }
    if delay_milliseconds < 0.0 {
        return Err(ErrorMetadata::bad_request(
            "InvalidArgument",
            "The scheduler delay must be non-negative",
        )
        .into());
    }
    Ok(unix_timestamp.as_secs_f64() + delay_milliseconds / 1000.0)
}

pub(super) fn scheduler_syscall_args(
    function_reference: String,
    timestamp: f64,
    args: JsonValue,
) -> JsonValue {
    json!({
        "reference": function_reference,
        "ts": timestamp,
        "args": args,
    })
}
