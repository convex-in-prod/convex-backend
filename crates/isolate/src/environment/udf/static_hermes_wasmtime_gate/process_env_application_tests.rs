use super::*;

const PACKAGE_DIRECTORY_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_PROCESS_ENV_PACKAGE_DIRECTORY";
const EXPECTATION_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_PROCESS_ENV_EXPECTATION";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProcessEnvironmentExpectation {
    package_id: String,
    entry_id: String,
    entry_path: String,
    runtime_module_path: String,
    route: ProcessEnvironmentRoute,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProcessEnvironmentRoute {
    route_id: String,
    entry_selector_id: String,
    export_name: String,
    udf_kind: ManifestUdfKind,
    visibility: String,
}

struct ProcessEnvironmentRuntime {
    routed: Arc<GeneratedRoutedModule>,
    controller: Arc<GeneratedMemoryController>,
    memory_identity: FunctionMemoryIdentity,
    selector: u64,
}

fn load_process_environment_runtime(
    package_directory: &Path,
    expectation: &ProcessEnvironmentExpectation,
) -> anyhow::Result<ProcessEnvironmentRuntime> {
    anyhow::ensure!(
        expectation.route.udf_kind == ManifestUdfKind::Query,
        "process environment fixture route must be a query"
    );
    let engine = shared_generated_engine()?;
    let compatibility_sha256 = calculated_precompile_compatibility_sha256(&engine);
    let compatibility = generated_runtime_compatibility(&compatibility_sha256)?;
    let route = &expectation.route;
    let package = load_capability_entry_package_for_compatibility_test(
        package_directory,
        &expectation.package_id,
        &expectation.entry_id,
        &route.route_id,
        &route.entry_selector_id,
        &expectation.entry_path,
        &expectation.runtime_module_path,
        &route.export_name,
        route.udf_kind,
        &route.visibility,
        &compatibility,
    )?;
    let selector = u64::from_str_radix(&route.entry_selector_id, 16)
        .context("process environment fixture selector is invalid")?;
    anyhow::ensure!(
        package.package_key == expectation.package_id && package.entry_selector == Some(selector),
        "process environment package changed its route or execution policy"
    );
    let controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: expectation.package_id.clone(),
        generation_sha256: expectation.package_id.clone(),
        package_key: expectation.package_id.clone(),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    let routed = cache_generated_routed_module(
        &controller,
        package,
        route_identity,
        Arc::clone(&engine),
        None,
    )?;
    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &routed.route_identity,
    );
    Ok(ProcessEnvironmentRuntime {
        routed,
        controller,
        memory_identity,
        selector,
    })
}

#[allow(clippy::too_many_arguments)]
async fn process_environment_state(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    runtime: &ProcessEnvironmentRuntime,
    metrics: Arc<GateMetrics>,
    existing_slot: Option<GeneratedSlotId>,
    values: Option<OpaqueValueTable>,
    initialize_runtime: bool,
) -> anyhow::Result<HostState<ProdRuntime>> {
    let mut state = new_state(rt, transaction, None, metrics, QueryJournal::new()).await?;
    state.provider.set_udf_type(UdfType::Query);
    state.provider.set_invocation(
        ResolvedComponentFunctionPath {
            component: ComponentId::Root,
            udf_path: "processEnvironment:readEnvironment".parse()?,
            component_path: ComponentPath::root(),
        },
        generated_test_execution_context(),
        DeploymentMetadata {
            name: "process-environment-test".to_owned(),
            region: None,
            class: DeploymentClass::S16,
        },
        Version::new(1, 43, 0),
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    );
    if initialize_runtime {
        state.provider.snoop_initialization_reads()?;
    }
    let timeout = state
        .timeout
        .as_mut()
        .context("process environment test timeout missing")?;
    state.provider.initialize_static_hermes(timeout).await?;

    let memory_permit = runtime
        .controller
        .admit(runtime.memory_identity.clone(), existing_slot)
        .await
        .map_err(|_| anyhow::anyhow!("process environment memory admission was rejected"))?;
    let observer = memory_permit.observer();
    let mut values = match values {
        Some(mut values) => {
            values.begin_invocation(observer)?;
            values
        },
        None => OpaqueValueTable::new_with_observer(
            usize::try_from(runtime.routed.manifest.limits().max_value_handles())?,
            usize::try_from(runtime.routed.manifest.limits().max_host_owned_bytes())?,
            Some(observer),
        ),
    };
    let maximum = usize::try_from(runtime.routed.manifest.platform_limits().argument_bytes)?
        .min(MAX_REQUEST_BYTES);
    let request_handle = values.insert(OpaqueValue::Bytes(GuestNativeValueCodec::encode(
        json!({}),
        maximum,
    )?))?;
    let capability_bridge = InvocationCapabilityBridge::unissued();
    let performance_monotonic_start = state.provider.rt().monotonic_now();
    state.generated = Some(GeneratedInvocationState {
        manifest: Arc::clone(&runtime.routed.manifest),
        values,
        async_operations: AsyncOperationState::default(),
        capability_bridge,
        performance_monotonic_start,
        performance_runtime_available: false,
        runtime_reuse_contaminated: false,
        allows_caught_official_output_chunk_initialization_failure: false,
        discard_after_caught_initialization_failure: false,
        context_read_set_required: true,
        request_handle,
        operation_count: 0,
        terminal_failure: None,
        cancellation: CancellationSignal::new_for_test(),
        interrupt: Arc::new(GeneratedInterruptState::default()),
        memory_limiter: memory_permit.limiter(
            StoreLimitsBuilder::new()
                .memory_size(usize::try_from(
                    runtime.routed.manifest.limits().max_guest_memory_bytes(),
                )?)
                .instances(1)
                .memories(1)
                .trap_on_grow_failure(true)
                .build(),
        ),
        memory_permit: Some(memory_permit),
        host_secret_values: BTreeMap::new(),
    });
    Ok(state)
}

fn assert_process_environment_result(
    output: &GeneratedExecutionOutput<ProdRuntime>,
    expected_value: &EnvVarValue,
    expected_operation_count: u64,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        output.invocation.outcome == InvocationOutcome::Success,
        "process environment guest invocation failed"
    );
    let result = output
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_object)
        .context("process environment guest result is not an object")?;
    let expected_value = JsonValue::String(String::from(expected_value.clone()));
    anyhow::ensure!(
        result.len() == 3
            && result.get("initializedValue") == Some(&expected_value)
            && result.get("invocationValue") == Some(&expected_value)
            && result.get("missing") == Some(&JsonValue::Bool(true)),
        "process environment guest returned an unexpected value shape"
    );
    anyhow::ensure!(
        output.invocation.operation_count == expected_operation_count,
        "process environment guest operation count changed"
    );
    anyhow::ensure!(
        output.invocation.capability_revoked
            && !output.invocation.runtime_reuse_contaminated
            && output.invocation.opaque_live_handles == 0
            && output.invocation.opaque_current_bytes == 0,
        "process environment invocation did not reach the reusable lifecycle boundary"
    );
    let retained = output
        .reusable_instance
        .as_ref()
        .context("process environment invocation did not retain its runtime")?;
    anyhow::ensure!(
        retained.store.data().provider.is_finished(),
        "process environment runtime retained invocation-owned provider state"
    );
    Ok(())
}

async fn discard_idle_process_environment_runtime(
    controller: &GeneratedMemoryController,
    retained: RetainedGeneratedTestRuntime,
) -> anyhow::Result<()> {
    let [(slot_id, reason)] = <[_; 1]>::try_from(controller.idle_eviction_candidates(
        &[(
            retained.memory_slot_id,
            retained.instance.idle_since.elapsed(),
        )],
        IdleEvictionTrigger::GenerationRetirement,
    ))
    .map_err(|_| anyhow::anyhow!("process environment runtime was not selected for eviction"))?;
    anyhow::ensure!(slot_id == retained.memory_slot_id);
    discard_generated_instance(retained.instance).await?;
    controller.finish_idle_eviction(slot_id, reason);
    Ok(())
}

async fn run_process_environment_application_test(
    rt: ProdRuntime,
    package_directory: PathBuf,
    expectation: ProcessEnvironmentExpectation,
) -> anyhow::Result<()> {
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    let test_hooks = StaticHermesGateTestHooks::new(0, 0);
    let _test_hooks_guard = install_test_hooks(&test_hooks)?;
    let runtime = load_process_environment_runtime(&package_directory, &expectation)?;
    let database = new_test_database(rt.clone()).await?;
    initialize_application_system_tables(&database).await?;
    let name: EnvVarName = "APPLICATION_KEY".parse()?;
    let initial_value: EnvVarValue = "configured-value".parse()?;
    let updated_value: EnvVarValue = "updated-value".parse()?;

    let mut setup = database.begin_system().await?;
    EnvironmentVariablesModel::new(&mut setup)
        .create(
            EnvironmentVariable::new(name.clone(), initial_value.clone()),
            &Default::default(),
        )
        .await?;
    database
        .commit_with_write_source(setup, "process_environment_application_test_setup")
        .await?;

    let metrics = Arc::clone(&test_hooks.metrics);
    let mut first_state = process_environment_state(
        rt.clone(),
        database.begin_system().await?,
        &runtime,
        Arc::clone(&metrics),
        None,
        None,
        true,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut first_state,
        Duration::from_secs(10),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut first = execute_generated_with_entry_selector(
        Arc::clone(&runtime.routed),
        Some(runtime.selector),
        first_state,
        None,
        true,
    )
    .await?;
    assert_process_environment_result(&first, &initial_value, 3)?;
    let first_retained = retain_generated_test_runtime(&mut first)?;
    let first_instance_id = first_retained.instance.id;
    let first_slot_id = first_retained.memory_slot_id;
    let first_capability = metrics
        .generated_invocations
        .lock()
        .last()
        .context("first process environment invocation had no observation")?
        .capability_identity;
    let first_read_set = first_retained
        .instance
        .context_read_set
        .as_ref()
        .context("fresh process environment runtime has no initialization read set")?;
    anyhow::ensure!(
        first_retained
            .instance
            .store
            .data()
            .generated
            .as_ref()
            .context("retained process environment state disappeared")?
            .capability_bridge
            .handle()
            .is_err(),
        "completed process environment runtime retained invocation authority"
    );
    drop(first.invocation.transaction);

    let mut second_transaction = database.begin_system().await?;
    anyhow::ensure!(
        ContextCache::validate_and_apply_context_read_set(&mut second_transaction, first_read_set)
            .await?,
        "unchanged process environment invalidated its initialization read set"
    );
    let mut second_state = process_environment_state(
        rt.clone(),
        second_transaction,
        &runtime,
        Arc::clone(&metrics),
        Some(first_slot_id),
        Some(first_retained.values),
        false,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut second_state,
        Duration::from_secs(10),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut second = execute_generated_with_entry_selector(
        Arc::clone(&runtime.routed),
        Some(runtime.selector),
        second_state,
        Some(first_retained.instance),
        true,
    )
    .await?;
    assert_process_environment_result(&second, &initial_value, 2)?;
    let observations = metrics.generated_invocations.lock();
    let second_observation = observations
        .last()
        .context("second process environment invocation had no observation")?;
    anyhow::ensure!(
        second_observation.capability_identity != first_capability
            && second_observation.prior_capability_rejected
            && second_observation.revoked_capability_rejected,
        "reused process environment runtime accepted stale invocation authority"
    );
    // The changed invocation records its observation through this mutex.
    drop(observations);
    let second_retained = retain_generated_test_runtime(&mut second)?;
    anyhow::ensure!(
        second_retained.instance.id == first_instance_id
            && second_retained.memory_slot_id == first_slot_id,
        "unchanged process environment invocation did not reuse its runtime slot"
    );
    drop(second.invocation.transaction);

    let mut update = database.begin_system().await?;
    EnvironmentVariablesModel::new(&mut update)
        .delete(&name)
        .await?
        .context("process environment fixture variable disappeared before update")?;
    EnvironmentVariablesModel::new(&mut update)
        .create(
            EnvironmentVariable::new(name, updated_value.clone()),
            &Default::default(),
        )
        .await?;
    database
        .commit_with_write_source(update, "process_environment_application_test_update")
        .await?;

    let mut changed_transaction = database.begin_system().await?;
    let changed_read_set = second_retained
        .instance
        .context_read_set
        .as_ref()
        .context("reused process environment runtime lost its read set")?;
    anyhow::ensure!(
        !ContextCache::validate_and_apply_context_read_set(
            &mut changed_transaction,
            changed_read_set
        )
        .await?,
        "changed process environment did not invalidate its initialization read set"
    );
    discard_idle_process_environment_runtime(&runtime.controller, second_retained).await?;

    let mut changed_state = process_environment_state(
        rt.clone(),
        changed_transaction,
        &runtime,
        Arc::clone(&metrics),
        None,
        None,
        true,
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut changed_state,
        Duration::from_secs(10),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut changed = execute_generated_with_entry_selector(
        Arc::clone(&runtime.routed),
        Some(runtime.selector),
        changed_state,
        None,
        true,
    )
    .await?;
    assert_process_environment_result(&changed, &updated_value, 3)?;
    let changed_instance = changed
        .reusable_instance
        .take()
        .context("changed process environment invocation did not retain its new runtime")?;
    let changed_permit = changed
        .memory_permit
        .take()
        .context("changed process environment invocation lost its memory permit")?;
    anyhow::ensure!(
        changed_instance.id != first_instance_id && changed_permit.slot_id() != first_slot_id,
        "changed process environment did not replace its runtime slot"
    );
    drop(changed.invocation.transaction);
    discard_generated_instance(changed_instance).await?;
    changed_permit.finish(changed.terminal_memory_outcome, false);
    anyhow::ensure!(
        metrics.teardowns.load(Ordering::SeqCst) == 2,
        "process environment runtime teardown count changed"
    );

    let route_identity = runtime.routed.route_identity.clone();
    drop(runtime.routed);
    drop(
        GENERATED_ROUTED_MODULES
            .lock()
            .remove(
                &route_identity,
                "process_environment_application_test_complete",
            )
            .context("process environment routed module was not cached")?,
    );
    let completed = runtime
        .controller
        .snapshot_for_test(&runtime.memory_identity);
    anyhow::ensure!(completed.active_instances == 0 && completed.idle_instances == 0);
    database.shutdown().await?;
    Ok(())
}

#[test]
#[ignore = "requires a producer-built generic process.env capability-entry package"]
fn generated_wasm_process_env_proxy_reuses_and_invalidates_runtime() -> anyhow::Result<()> {
    let package_directory = std::env::var_os(PACKAGE_DIRECTORY_ENV)
        .map(PathBuf::from)
        .context(format!("{PACKAGE_DIRECTORY_ENV} must identify the package"))?;
    let expectation: ProcessEnvironmentExpectation = serde_json::from_str(
        &std::env::var(EXPECTATION_ENV)
            .with_context(|| format!("failed to read {EXPECTATION_ENV}"))?,
    )
    .with_context(|| format!("failed to parse {EXPECTATION_ENV}"))?;
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_process_env_proxy",
        run_process_environment_application_test(rt, package_directory, expectation),
    )
}
