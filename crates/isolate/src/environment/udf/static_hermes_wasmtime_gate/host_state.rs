use super::{
    super::{
        append_log_line,
        phase::UdfPhase,
        syscall::SyscallProviderInternal,
        DatabaseUdfArgs,
        DatabaseUdfEnvironment,
        DatabaseUdfInnerProvider,
        DatabaseUdfSyscallProvider,
    },
    *,
};

pub(super) const STATIC_HERMES_WASI_ARGS: &[&[u8]] = &[b"static-hermes-backend-gate"];

/// Database state for preparation or invocation in a retained Static Hermes
/// Store.
///
/// The Store may outlive this value, but the canonical provider and its
/// transaction are removed during finalization before a Store can return to
/// the pool.
pub(super) struct DatabaseUdfWasmInvocation<RT: Runtime> {
    context: Option<WasmDatabaseContext<RT>>,
    #[cfg(test)]
    invocation_cleared: bool,
    #[cfg(test)]
    pub(super) cancel_nested_call_on_completion: Option<CancellationSignal>,
}

enum WasmDatabaseContext<RT: Runtime> {
    Invocation {
        provider: DatabaseUdfSyscallProvider<RT>,
        args: DatabaseUdfArgs,
    },
    Preparation {
        phase: UdfPhase<RT>,
        log_lines: LogLines,
    },
}

impl<RT: Runtime> DatabaseUdfWasmInvocation<RT> {
    pub(super) fn new(environment: DatabaseUdfEnvironment<RT>, args: DatabaseUdfArgs) -> Self {
        Self {
            context: Some(WasmDatabaseContext::Invocation {
                provider: environment.syscall_provider,
                args,
            }),
            #[cfg(test)]
            invocation_cleared: false,
            #[cfg(test)]
            cancel_nested_call_on_completion: None,
        }
    }

    pub(super) fn for_preparation(
        rt: RT,
        transaction: Transaction<RT>,
        default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    ) -> Self {
        Self {
            context: Some(WasmDatabaseContext::Preparation {
                phase: UdfPhase::for_static_hermes_preparation(
                    transaction,
                    rt,
                    default_system_env_vars,
                ),
                log_lines: vec![].into(),
            }),
            #[cfg(test)]
            invocation_cleared: false,
            #[cfg(test)]
            cancel_nested_call_on_completion: None,
        }
    }

    fn provider(&self) -> anyhow::Result<&DatabaseUdfSyscallProvider<RT>> {
        match self
            .context
            .as_ref()
            .context("Static Hermes database context missing")?
        {
            WasmDatabaseContext::Invocation { provider, .. } => Ok(provider),
            WasmDatabaseContext::Preparation { .. } => {
                anyhow::bail!("initialization has no invocation provider")
            },
        }
    }

    fn provider_mut(&mut self) -> anyhow::Result<&mut DatabaseUdfSyscallProvider<RT>> {
        match self
            .context
            .as_mut()
            .context("Static Hermes database context missing")?
        {
            WasmDatabaseContext::Invocation { provider, .. } => Ok(provider),
            WasmDatabaseContext::Preparation { .. } => {
                anyhow::bail!("initialization has no invocation provider")
            },
        }
    }

    fn args(&self) -> anyhow::Result<&DatabaseUdfArgs> {
        match self
            .context
            .as_ref()
            .context("Static Hermes database context missing")?
        {
            WasmDatabaseContext::Invocation { args, .. } => Ok(args),
            WasmDatabaseContext::Preparation { .. } => {
                anyhow::bail!("initialization has no invocation arguments")
            },
        }
    }

    fn args_mut(&mut self) -> anyhow::Result<&mut DatabaseUdfArgs> {
        match self
            .context
            .as_mut()
            .context("Static Hermes database context missing")?
        {
            WasmDatabaseContext::Invocation { args, .. } => Ok(args),
            WasmDatabaseContext::Preparation { .. } => {
                anyhow::bail!("initialization has no invocation arguments")
            },
        }
    }

    fn phase(&self) -> anyhow::Result<&UdfPhase<RT>> {
        match self
            .context
            .as_ref()
            .context("Static Hermes database context missing")?
        {
            WasmDatabaseContext::Invocation { provider, .. } => Ok(&provider.phase),
            WasmDatabaseContext::Preparation { phase, .. } => Ok(phase),
        }
    }

    fn phase_mut(&mut self) -> anyhow::Result<&mut UdfPhase<RT>> {
        match self
            .context
            .as_mut()
            .context("Static Hermes database context missing")?
        {
            WasmDatabaseContext::Invocation { provider, .. } => Ok(&mut provider.phase),
            WasmDatabaseContext::Preparation { phase, .. } => Ok(phase),
        }
    }

    #[cfg(test)]
    pub(super) async fn prepare_execution(
        &mut self,
        timeout: &mut Timeout<RT>,
    ) -> anyhow::Result<()> {
        self.initialize_static_hermes(timeout).await?;
        self.begin_execution()
    }

    pub(super) fn begin_execution(&mut self) -> anyhow::Result<()> {
        let rng_seed = self.args()?.rng_seed;
        let unix_timestamp = self.args()?.unix_timestamp;
        self.provider_mut()?
            .begin_execution(rng_seed, unix_timestamp)
    }

    #[cfg(test)]
    pub(super) fn cancel_nested_call_for_test(&mut self) -> anyhow::Result<()> {
        if let Some(cancellation) = self.cancel_nested_call_on_completion.take() {
            // Model the lost transaction after reaching the real execution phase.
            // Do not reset the phase or weaken initialization to construct it.
            let (transaction, ..) = self.phase_mut()?.start_nested_udf()?;
            drop(transaction);
            cancellation.cancel_for_test();
        }
        Ok(())
    }

    pub(super) async fn initialize_static_hermes(
        &mut self,
        timeout: &mut Timeout<RT>,
    ) -> anyhow::Result<()> {
        #[cfg(test)]
        anyhow::ensure!(
            !self.invocation_cleared,
            "Static Hermes test invocation was cleared"
        );
        self.phase_mut()?.initialize_static_hermes(timeout).await
    }

    pub(super) fn tx_for_initialization(&mut self) -> anyhow::Result<&mut Transaction<RT>> {
        self.phase_mut()?.tx_mut()
    }

    pub(super) fn snoop_initialization_reads(&mut self) -> anyhow::Result<()> {
        self.phase_mut()?.snoop_reads()
    }

    pub(super) fn finish_initialization_reads(
        &mut self,
    ) -> anyhow::Result<database::TransactionReadSet> {
        self.phase_mut()?.finish_snoop()
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
            .phase()
            .expect("Static Hermes database context missing")
            .rt
    }

    pub(super) fn udf_type(&self) -> UdfType {
        self.provider()
            .expect("Static Hermes provider missing")
            .udf_type
    }

    pub(super) fn udf_path(&self) -> &CanonicalizedUdfPath {
        &self
            .provider()
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
        match self
            .context
            .as_ref()
            .expect("Static Hermes database context missing")
        {
            WasmDatabaseContext::Invocation { provider, .. } => {
                pending_values_allowed_for_invocation(provider.udf_type, provider.reactor_depth)
            },
            WasmDatabaseContext::Preparation { .. } => false,
        }
    }

    pub(super) fn observed_identity(&self) -> bool {
        self.phase()
            .expect("Static Hermes database context missing")
            .observed_identity()
    }

    pub(super) fn observed_rng(&self) -> bool {
        self.phase()
            .expect("Static Hermes database context missing")
            .observed_rng()
    }

    pub(super) fn observed_time(&self) -> bool {
        self.phase()
            .expect("Static Hermes database context missing")
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

    pub(super) fn record_generated_uuid(&mut self, uuid: [u8; 16]) -> anyhow::Result<()> {
        self.provider_mut()?
            .host_operation_trace
            .record_generated_uuid(uuid);
        Ok(())
    }

    pub(super) fn emit_log_line(
        &mut self,
        level: LogLevel,
        messages: Vec<String>,
    ) -> anyhow::Result<()> {
        match self
            .context
            .as_mut()
            .context("Static Hermes database context missing")?
        {
            WasmDatabaseContext::Invocation { provider, .. } => {
                provider.emit_log_line(LogLine::new_developer_log_line(
                    level,
                    messages,
                    provider.rt.unix_timestamp(),
                ));
            },
            WasmDatabaseContext::Preparation { phase, log_lines } => {
                append_log_line(
                    &phase.rt,
                    log_lines,
                    LogLine::new_developer_log_line(level, messages, phase.rt.unix_timestamp()),
                );
            },
        }
        Ok(())
    }

    pub(super) fn is_preparation(&self) -> bool {
        matches!(
            self.context
                .as_ref()
                .expect("Static Hermes database context missing"),
            WasmDatabaseContext::Preparation { .. }
        )
    }

    pub(super) fn take_preparation_log_lines(&mut self) -> anyhow::Result<LogLines> {
        match self
            .context
            .as_mut()
            .context("Static Hermes database context missing")?
        {
            WasmDatabaseContext::Preparation { log_lines, .. } => Ok(std::mem::take(log_lines)),
            WasmDatabaseContext::Invocation { .. } => {
                anyhow::bail!("invocation has no preparation log buffer")
            },
        }
    }

    pub(super) fn append_initialization_log_lines(
        &mut self,
        lines: LogLines,
    ) -> anyhow::Result<()> {
        let provider = self.provider_mut()?;
        for line in lines {
            provider.emit_log_line(line);
        }
        Ok(())
    }

    pub(super) fn clear_queries(&mut self) {
        match self
            .context
            .as_mut()
            .expect("Static Hermes database context missing")
        {
            WasmDatabaseContext::Invocation { provider, .. } => {
                provider.query_manager.clear_developer_queries()
            },
            WasmDatabaseContext::Preparation { .. } => {},
        }
    }

    pub(super) fn has_active_queries(&self) -> bool {
        match self
            .context
            .as_ref()
            .expect("Static Hermes database context missing")
        {
            WasmDatabaseContext::Invocation { provider, .. } => {
                provider.query_manager.has_developer_queries()
            },
            WasmDatabaseContext::Preparation { .. } => false,
        }
    }

    pub(super) fn into_outcome(
        &mut self,
        result: Result<PendingValue, common::errors::JsError>,
        execution_time: FunctionExecutionTime,
    ) -> anyhow::Result<(Transaction<RT>, FunctionOutcome)> {
        match self
            .context
            .take()
            .context("Static Hermes database context already taken")?
        {
            WasmDatabaseContext::Invocation { provider, args } => {
                provider.into_outcome(args, result, execution_time)
            },
            WasmDatabaseContext::Preparation { .. } => {
                anyhow::bail!("initialization cannot produce a function outcome")
            },
        }
    }

    pub(super) fn take_transaction_for_system(&mut self) -> anyhow::Result<Transaction<RT>> {
        match self
            .context
            .take()
            .context("Static Hermes database context already taken")?
        {
            WasmDatabaseContext::Invocation { provider, .. } => provider.phase.into_transaction(),
            WasmDatabaseContext::Preparation { phase, .. } => phase.into_transaction(),
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.context.is_none()
    }

    pub(super) fn set_terminal_host_operation_error(&mut self, error: HostOperationErrorV1) {
        self.provider_mut()
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
        serde_json::from_str(&result.result?.into_json_string()?)
            .context("Static Hermes query stream result was not valid JSON")
    }

    pub(super) fn get_environment_variable(
        &mut self,
        name: &EnvVarName,
    ) -> anyhow::Result<Option<EnvVarValue>> {
        self.phase_mut()?.get_environment_variable(name.clone())
    }

    #[cfg(test)]
    pub(super) fn set_udf_type(&mut self, udf_type: UdfType) {
        self.provider_mut()
            .expect("Static Hermes test invocation provider missing")
            .udf_type = udf_type;
    }

    #[cfg(test)]
    pub(super) fn enable_host_operation_trace(&mut self) {
        self.provider_mut()
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
                .provider_mut()
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
            file_downloads: file_storage::FileDownloads::default(),
            rt: rt.clone(),
            udf_type: UdfType::Mutation,
            path: ResolvedComponentFunctionPath {
                component: ComponentId::Root,
                udf_path: "generated_test:run".parse()?,
                component_path: ComponentPath::root(),
            },
            context_initialization_module: None,
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
            context: Some(WasmDatabaseContext::Invocation {
                provider,
                args: DatabaseUdfArgs {
                    unix_timestamp: UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                    rng_seed: [0; 32],
                    udf_args: SerializedArgs::from_args(vec![])?,
                    identity,
                    reuse_context: false,
                },
            }),
            invocation_cleared: false,
            cancel_nested_call_on_completion: None,
        })
    }
}

#[cfg(test)]
pub(super) struct UnusedGateModuleCache;

#[cfg(test)]
#[test]
fn generated_cancellation_disposes_without_nested_transaction() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("cancelled_nested_transaction", async move {
        let database = new_test_database(rt.clone()).await?;
        let manifest =
            generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
        let routed = generated_memory_test_routed_module(
            Arc::clone(&manifest),
            GeneratedMemoryTestOperation {
                additional_pages: 0,
                destroy_infinite_loop: false,
                loop_iterations: Some(0),
                maximum_pages: 1,
            },
            GeneratedRouteIdentity::Singleton {
                package_key: "cancelled-nested-transaction".to_owned(),
            },
        )?;
        let cancellation = CancellationSignal::new_for_test();
        let metrics = Arc::new(GateMetrics::default());
        let mut state = generated_test_state(
            rt.clone(),
            database.begin_system().await?,
            manifest,
            JsonValue::Null,
            UdfType::Query,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            cancellation.clone(),
            None,
            Arc::clone(&metrics),
            None,
        )
        .await?;
        arm_generated_timeout(
            rt,
            &mut state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        state.provider.cancel_nested_call_on_completion = Some(cancellation);
        let cpu_limiter = state.active_wasm_cpu_limiter.clone();
        let memory = generated_state_mut(&mut state)?
            .memory_permit
            .as_mut()
            .context("test memory permit missing")?;
        let observations = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&observations);
        let disposal_metrics = Arc::clone(&metrics);
        memory.observe_completion(Arc::new(move |observation| {
            assert_eq!(disposal_metrics.store_drops.load(Ordering::SeqCst), 1);
            recorded.lock().push(observation);
        }));
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            execute_generated(routed, state, None, false),
        )
        .await
        .context("cancelled nested invocation did not dispose")?;
        let error = result
            .err()
            .context("cancelled nested invocation published a result")?;
        assert!(matches!(
            error.downcast_ref::<GeneratedNonRetryableFailure>(),
            Some(GeneratedNonRetryableFailure::Cancelled)
        ));
        assert!(!error.is::<StaticHermesWasmFallbackReason>());
        assert_eq!(metrics.terminal_cancellations.load(Ordering::SeqCst), 1);
        assert_eq!(
            metrics.fresh_initialization_attempts.load(Ordering::SeqCst),
            1
        );
        assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.store_drops.load(Ordering::SeqCst), 1);
        assert_eq!(cpu_limiter.active_permits(), 0);
        let invocations = metrics.generated_invocations.lock();
        assert_eq!(invocations.len(), 1);
        assert!(invocations[0].cancelled);
        assert_eq!(invocations[0].transaction_write_count, None);
        assert!(invocations[0].capability_revoked);
        assert_eq!(invocations[0].opaque_live_handles, 0);
        assert_eq!(invocations[0].opaque_current_bytes, 0);
        assert!(!invocations[0].retain_instance);
        drop(invocations);
        let observations = observations.lock();
        assert_eq!(observations.len(), 1);
        assert_eq!(
            observations[0].terminal,
            udf::wasm_memory::WasmMemoryTerminal::Cancellation
        );
        assert!(!observations[0].returned_to_pool);
        drop(observations);
        database.shutdown().await?;
        Ok(())
    })
}

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
    #[cfg(test)]
    InterruptedHostCall(Arc<GeneratedInterruptState>),
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
            #[cfg(test)]
            Self::InterruptedHostCall(interrupt) => {
                // Model the timer/pause race at the real transaction ownership
                // handoff. Timer expiration itself is covered by lifecycle tests.
                drop(udf_request);
                interrupt.terminate(
                    IsolateTerminationReason::UserTimeout(Duration::from_millis(50)).into(),
                );
                Err(crate::timeout::TimeoutExpired.into())
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
            self.provider().expect("Static Hermes provider missing"),
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
    pub(super) wasi_cpu_clock: WasiCpuClock,
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
    // Preparation owns no invocation arguments and cannot expose a request to JS.
    pub(super) request_handle: Option<OpaqueHandle>,
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
    destruction_deadline: parking_lot::Mutex<Option<tokio::time::Instant>>,
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
            destruction_deadline: parking_lot::Mutex::new(None),
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

    pub(super) fn begin_destruction(&self, deadline: tokio::time::Instant) {
        *self.destruction_deadline.lock() = Some(deadline);
        self.destroying.store(true, Ordering::Release);
    }

    pub(super) fn should_interrupt(&self, cancellation: &CancellationSignal) -> bool {
        if self.destroying.load(Ordering::Acquire) {
            // Share the destruction watchdog's absolute deadline; guest CPU
            // must not need that watchdog to run before it can be interrupted.
            tokio::time::Instant::now()
                >= self
                    .destruction_deadline
                    .lock()
                    .expect("destruction deadline missing")
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

    #[track_caller]
    pub(super) fn opaque_value_operation<T>(
        &mut self,
        operation: impl FnOnce(&mut OpaqueValueTable) -> Result<T, OpaqueValueError>,
    ) -> Result<T, WasmtimeError> {
        self.memory_limiter.ensure_memory_available()?;
        let location = std::panic::Location::caller();
        operation(&mut self.values).map_err(|error| {
            let failure = StaticHermesWasmExecutionFailure::from(&error);
            self.latch_terminal_failure(failure);
            WasmtimeError::new(HostInvariant).context(
                StaticHermesWasmHostInvariantDiagnostic::from_opaque_at(&error, location),
            )
        })
    }

    fn next_operation_count(&mut self, additional_operations: u64) -> Result<u64, WasmtimeError> {
        self.memory_limiter.ensure_memory_available()?;
        let Some(operation_count) = self.operation_count.checked_add(additional_operations) else {
            self.latch_terminal_failure(StaticHermesWasmExecutionFailure::OperationLimit);
            return Err(host_invariant_error());
        };
        if operation_count > self.manifest.limits().max_operation_count() {
            self.latch_terminal_failure(StaticHermesWasmExecutionFailure::OperationLimit);
            return Err(host_invariant_error());
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
            u64::try_from(operation_ids.len()).map_err(|_| host_invariant_error())?;
        let operation_count = self.next_operation_count(additional_operations)?;
        let operations = operation_ids
            .map(|operation_id| {
                self.manifest
                    .imported_operation(operation_id)
                    .map(|operation| operation.operation().clone())
                    .ok_or_else(|| host_invariant_error())
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.operation_count = operation_count;
        Ok(operations)
    }

    pub(super) fn operation(
        &mut self,
        operation_id: i32,
    ) -> Result<ImportedOperationDescriptor, WasmtimeError> {
        let operation_id = u32::try_from(operation_id).map_err(|_| host_invariant_error())?;
        let operation_count = self.next_operation_count(1)?;
        let operation = self
            .manifest
            .imported_operation(operation_id)
            .map(|operation| operation.operation().clone())
            .ok_or_else(|| host_invariant_error())?;
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
        .ok_or_else(|| host_invariant_error())
}

pub(super) fn checked_offset(value: i32) -> Result<usize, WasmtimeError> {
    usize::try_from(value).map_err(|_| host_invariant_error())
}

pub(super) fn checked_len(value: i32, maximum: usize) -> Result<usize, WasmtimeError> {
    let length = usize::try_from(value).map_err(|_| host_invariant_error())?;
    if length > maximum {
        return Err(host_invariant_error());
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
        .ok_or_else(|| host_invariant_error())?;
    let memory = guest_memory(caller)?;
    let bytes = memory
        .data(caller)
        .get(start..end)
        .ok_or_else(|| host_invariant_error())?;
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
    let memory = guest_memory(caller)?;
    let offset = checked_offset(pointer)?;
    let bytes = source(caller.data())?;
    let length = bytes.len();
    if capacity < length {
        return Err(host_invariant_error());
    }
    let source = bytes.as_ptr();
    let end = offset
        .checked_add(length)
        .ok_or_else(|| host_invariant_error())?;
    if memory.data(&*caller).get(offset..end).is_none() {
        return Err(host_invariant_error());
    }
    // A whole-memory mutable borrow grants and charges every COW page, even
    // for a tiny payload. Validate first, then admit only the bytes copied;
    // write denial remains an error rather than data_and_store_mut's panic.
    let destination = memory.data_mut_range(&mut *caller, offset..end)?;
    // SAFETY: all callers borrow a Vec's bytes or a JSON String from the
    // opaque table. Those heap allocations are disjoint from guest memory and
    // remain owned and unchanged through this copy. data_mut_range only checks
    // bounds and invokes the optional PrivatePages hook, which cannot access
    // HostState, reenter the Store or move either allocation. Exclusive Store
    // ownership keeps the admitted destination writable until this immediate
    // copy finishes, without another Store operation or an intermediate Vec.
    unsafe { std::ptr::copy_nonoverlapping(source, destination.as_mut_ptr(), length) };
    Ok(length)
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
        .ok_or_else(|| host_invariant_error())?;
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
        return Err(host_invariant_error());
    }

    let mut lease = PendingReadLease {
        metrics,
        completed: false,
    };
    match control.entered {
        #[cfg(test)]
        ReadEntered::Direct(entered) => entered.send(()).map_err(|_| host_invariant_error())?,
        ReadEntered::TestHooks {
            sender,
            instance_id,
        } => sender
            .send(instance_id)
            .map_err(|_| host_invariant_error())?,
    }
    control.release.await.map_err(|_| host_invariant_error())?;
    let result = future.await;
    lease.complete();
    Ok(result)
}

pub(super) fn generated_host_operation_error(error: anyhow::Error) -> WasmtimeError {
    // A host timer stopping a suspended operation is distinct from a provider
    // failure arriving at the same deadline. The lifecycle owner still requires
    // its typed timer reason and successful disposal before authorizing recovery.
    if error.is::<crate::timeout::TimeoutExpired>() {
        return WasmtimeError::new(crate::timeout::TimeoutExpired);
    }
    // Provider failures are not evidence that the guest engine failed. Preserve
    // public metadata without exposing the provider's raw error payload.
    let failure = WasmtimeError::new(GeneratedNonRetryableFailure::HostOperation);
    match error.downcast_ref::<ErrorMetadata>() {
        Some(metadata) => failure.context(metadata.clone()),
        None => failure,
    }
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
        Err(error) => Err(generated_host_operation_error(error)),
    }
}

pub(super) fn generated_state<RT: Runtime>(
    state: &HostState<RT>,
) -> Result<&GeneratedInvocationState, WasmtimeError> {
    state
        .generated
        .as_ref()
        .ok_or_else(|| host_invariant_error())
}

pub(super) fn generated_state_mut<RT: Runtime>(
    state: &mut HostState<RT>,
) -> Result<&mut GeneratedInvocationState, WasmtimeError> {
    state
        .generated
        .as_mut()
        .ok_or_else(|| host_invariant_error())
}

pub(super) fn require_active_generated_execution<RT: Runtime>(
    state: &HostState<RT>,
) -> Result<(), WasmtimeError> {
    let generated = generated_state(state)?;
    generated.memory_limiter.ensure_memory_available()?;
    if !generated.interrupt.is_active() {
        return Err(host_invariant_error());
    }
    Ok(())
}

pub(super) fn opaque_handle(value: i64) -> Result<OpaqueHandle, WasmtimeError> {
    OpaqueHandle::from_abi(value).map_err(|_| host_invariant_error())
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
            .map_err(|_| host_invariant_error())
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
        .map_err(|_| host_invariant_error())?
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
                return Err(host_invariant_error());
            };
            values
        },
        _ => return Err(host_invariant_error()),
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
        .ok_or_else(|| host_invariant_error())
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
                        Err(host_invariant_error())
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
        .map_err(|_| host_invariant_error())?
        .with_release_permit(PauseReason::DatabaseSyscall { name }, async move {
            read.await
                .map_err(|_| anyhow::anyhow!("query next host invariant failed"))
        })
        .await
        .map_err(generated_host_operation_error)?;
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
        return Err(host_invariant_error());
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
        return Err(host_invariant_error());
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
        .ok_or_else(|| host_invariant_error())?;
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
