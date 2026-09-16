use super::*;

const PACKAGE_DIRECTORY_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERIC_QUERY_APPLICATION_PACKAGE_DIRECTORY";
const EXPECTATION_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_GENERIC_QUERY_APPLICATION_EXPECTATION";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GenericQueryApplicationExpectation {
    package_id: String,
    entry_id: String,
    entry_path: String,
    runtime_module_path: String,
    route: GenericQueryApplicationRoute,
    table_name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GenericQueryApplicationRoute {
    route_id: String,
    entry_selector_id: String,
    export_name: String,
    udf_kind: ManifestUdfKind,
    visibility: String,
}

fn load_authenticated_application_package(
    package_directory: &Path,
    expectation: &GenericQueryApplicationExpectation,
) -> anyhow::Result<(ValidatedWasmUdfPackage, Arc<Engine>)> {
    let engine = shared_generated_engine()?;
    let compatibility_sha256 = calculated_precompile_compatibility_sha256(&engine);
    let runtime = generated_runtime_compatibility(&compatibility_sha256)?;
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
        &runtime,
    )?;
    anyhow::ensure!(
        package.package_key == expectation.package_id
            && package.entry_selector == Some(u64::from_str_radix(&route.entry_selector_id, 16)?),
        "authenticated capability-entry package changed its route or execution policy"
    );
    Ok((package, engine))
}

#[allow(clippy::too_many_arguments)]
async fn generic_query_application_state(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    routed: &Arc<GeneratedRoutedModule>,
    request: JsonValue,
    cancellation: CancellationSignal,
    metrics: Arc<GateMetrics>,
    controller: Arc<GeneratedMemoryController>,
    existing_slot: Option<GeneratedSlotId>,
    values: Option<OpaqueValueTable>,
) -> anyhow::Result<HostState<ProdRuntime>> {
    let mut state = new_state(rt, transaction, None, metrics, QueryJournal::new()).await?;
    state.provider.set_udf_type(UdfType::Query);
    state.provider.set_invocation(
        ResolvedComponentFunctionPath {
            component: ComponentId::Root,
            udf_path: "generated_test:run".parse()?,
            component_path: ComponentPath::root(),
        },
        generated_test_execution_context(),
        DeploymentMetadata {
            name: "generated-test".to_owned(),
            region: None,
            class: DeploymentClass::S16,
        },
        Version::new(1, 43, 0),
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    );
    let memory_permit = controller
        .admit(
            generated_memory_identity_for_package(
                &routed.manifest,
                &routed.package_identity,
                &routed.route_identity,
            ),
            existing_slot,
        )
        .await
        .map_err(|_| anyhow::anyhow!("application collect memory admission was rejected"))?;
    let observer = memory_permit.observer();
    let mut values = match values {
        Some(mut values) => {
            values.begin_invocation(observer)?;
            values
        },
        None => OpaqueValueTable::new_with_observer(
            usize::try_from(routed.manifest.limits().max_value_handles())?,
            usize::try_from(routed.manifest.limits().max_host_owned_bytes())?,
            Some(observer),
        ),
    };
    let maximum =
        usize::try_from(routed.manifest.platform_limits().argument_bytes)?.min(MAX_REQUEST_BYTES);
    let request_handle = values.insert(OpaqueValue::Bytes(GuestNativeValueCodec::encode(
        request, maximum,
    )?))?;
    let capability_bridge = InvocationCapabilityBridge::unissued();
    let performance_monotonic_start = state.provider.rt().monotonic_now();
    state.generated = Some(GeneratedInvocationState {
        manifest: Arc::clone(&routed.manifest),
        values,
        async_operations: AsyncOperationState::default(),
        capability_bridge,
        performance_monotonic_start,
        performance_runtime_available: false,
        runtime_reuse_contaminated: false,
        allows_caught_official_output_chunk_initialization_failure: false,
        discard_after_caught_initialization_failure: false,
        context_read_set_required: false,
        request_handle,
        operation_count: 0,
        terminal_failure: None,
        cancellation,
        interrupt: Arc::new(GeneratedInterruptState::default()),
        memory_limiter: memory_permit.limiter(
            StoreLimitsBuilder::new()
                .memory_size(usize::try_from(
                    routed.manifest.limits().max_guest_memory_bytes(),
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

fn assert_collect_invocation(output: &GeneratedExecutionOutput<ProdRuntime>) -> anyhow::Result<()> {
    anyhow::ensure!(
        output.invocation.outcome == InvocationOutcome::Success,
        "application collect invocation failed: {:?}",
        output.system_error
    );
    let rows = output
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .context("application collect result is not an array")?;
    anyhow::ensure!(
        rows.len() == 2,
        "application collect result has the wrong row count"
    );
    anyhow::ensure!(
        rows.iter()
            .all(|row| row.get("marker") == Some(&json!("backend-gate-document"))),
        "application collect returned an unexpected document"
    );
    let sequences = rows
        .iter()
        .map(|row| row.get("sequence").and_then(JsonValue::as_f64))
        .collect::<Option<Vec<_>>>()
        .context("application collect result has an invalid sequence")?;
    anyhow::ensure!(
        sequences == [1.0, 2.0],
        "application collect did not preserve the canonical default order"
    );
    anyhow::ensure!(
        output.invocation.read_accounting.documents == 2,
        "application collect invocation read {} documents instead of two",
        output.invocation.read_accounting.documents
    );
    anyhow::ensure!(
        output.invocation.read_accounting.bytes > 0,
        "application collect invocation did not account for read bytes"
    );
    anyhow::ensure!(
        output.invocation.read_accounting.intervals > 0,
        "application collect invocation did not account for read intervals"
    );
    // The generic query protocol counts the query start, one advance per
    // document, and the final advance that observes exhaustion.
    anyhow::ensure!(
        output.invocation.operation_count == 4,
        "application collect invocation used {} operations instead of four",
        output.invocation.operation_count
    );
    anyhow::ensure!(
        output.invocation.capability_revoked,
        "application collect invocation retained its capability"
    );
    anyhow::ensure!(
        !output.invocation.runtime_reuse_contaminated,
        "application collect invocation contaminated runtime reuse"
    );
    anyhow::ensure!(
        output.invocation.opaque_live_handles == 0,
        "application collect invocation retained {} opaque handles",
        output.invocation.opaque_live_handles
    );
    anyhow::ensure!(
        output.invocation.opaque_current_bytes == 0,
        "application collect invocation retained {} opaque bytes",
        output.invocation.opaque_current_bytes
    );
    Ok(())
}

async fn run_generic_query_application_test(
    rt: ProdRuntime,
    package_directory: PathBuf,
    expectation: GenericQueryApplicationExpectation,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        expectation.route.udf_kind == ManifestUdfKind::Query,
        "generic query application route must be a query"
    );
    let table: TableName = expectation.table_name.parse()?;
    let (package, engine) =
        load_authenticated_application_package(&package_directory, &expectation)?;
    let database = new_test_database(rt.clone()).await?;
    insert_document_with_sequence(&database, &table, ConvexValue::Float64(1.0)).await?;
    insert_document_with_sequence(&database, &table, ConvexValue::Float64(2.0)).await?;

    let controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: expectation.package_id.clone(),
        generation_sha256: expectation.package_id.clone(),
        package_key: expectation.package_id.clone(),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    let routed = cache_generated_routed_module(&controller, package, route_identity, engine, None)?;
    anyhow::ensure!(
        routed.entry_selector
            == Some(u64::from_str_radix(
                &expectation.route.entry_selector_id,
                16,
            )?),
        "authenticated AOT route changed its entry selector"
    );
    let metrics = Arc::new(GateMetrics::default());

    let mut first_state = generic_query_application_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        json!({}),
        CancellationSignal::new_for_test(),
        Arc::clone(&metrics),
        Arc::clone(&controller),
        None,
        None,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut first_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut first = execute_generated(Arc::clone(&routed), first_state, None, true).await?;
    assert_collect_invocation(&first)?;
    let retained = retain_generated_test_runtime(&mut first)?;
    let instance_id = retained.instance.id;
    let slot_id = retained.memory_slot_id;
    drop(first.invocation.transaction);

    let second_transaction = tokio::time::timeout(Duration::from_secs(5), database.begin_system())
        .await
        .context("timed out while opening the second application collect transaction")??;
    let mut second_state = tokio::time::timeout(
        Duration::from_secs(5),
        generic_query_application_state(
            rt.clone(),
            second_transaction,
            &routed,
            json!({}),
            CancellationSignal::new_for_test(),
            Arc::clone(&metrics),
            Arc::clone(&controller),
            Some(slot_id),
            Some(retained.values),
        ),
    )
    .await
    .context("timed out while constructing the second application collect state")??;
    arm_generated_timeout(
        rt,
        &mut second_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut second = tokio::time::timeout(
        Duration::from_secs(5),
        execute_generated(
            Arc::clone(&routed),
            second_state,
            Some(retained.instance),
            true,
        ),
    )
    .await
    .context("timed out while executing the second application collect invocation")??;
    assert_collect_invocation(&second)?;
    let observations = metrics.generated_invocations.lock();
    let first_capability = observations
        .first()
        .context("first application collect invocation had no observation")?
        .capability_identity;
    let second_capability = observations
        .get(1)
        .context("second application collect invocation had no observation")?
        .capability_identity;
    anyhow::ensure!(
        second_capability != first_capability,
        "reused application runtime did not receive fresh invocation authority"
    );
    let second_instance = second
        .reusable_instance
        .take()
        .context("second application collect invocation did not retain its runtime")?;
    anyhow::ensure!(
        second_instance.id == instance_id,
        "application collect invocation did not reuse its runtime instance"
    );
    let second_permit = second
        .memory_permit
        .take()
        .context("second application collect invocation lost its memory permit")?;
    anyhow::ensure!(
        second_permit.slot_id() == slot_id,
        "application collect invocation did not reuse its memory slot"
    );
    drop(second.invocation.transaction);
    tokio::time::timeout(
        Duration::from_secs(5),
        discard_generated_instance(second_instance),
    )
    .await
    .context("timed out while discarding the reused application collect instance")??;
    second_permit.finish(second.terminal_memory_outcome, false);
    anyhow::ensure!(
        metrics.teardowns.load(Ordering::SeqCst) == 1,
        "application collect reusable runtime teardown count is invalid"
    );
    let route_identity = routed.route_identity.clone();
    drop(routed);
    drop(
        GENERATED_ROUTED_MODULES
            .lock()
            .remove(&route_identity, "generic_query_application_test_complete")
            .context("application collect routed module was not cached")?,
    );
    tokio::time::timeout(Duration::from_secs(5), database.shutdown())
        .await
        .context("timed out while shutting down the application collect database")??;
    Ok(())
}

#[test]
#[ignore = "requires a producer-built downstream application capability-entry package"]
fn generated_wasm_unchanged_generic_collect_query_reuses_runtime() -> anyhow::Result<()> {
    let package_directory = std::env::var_os(PACKAGE_DIRECTORY_ENV)
        .map(PathBuf::from)
        .context(format!("{PACKAGE_DIRECTORY_ENV} must identify the package"))?;
    let expectation: GenericQueryApplicationExpectation = serde_json::from_str(
        &std::env::var(EXPECTATION_ENV)
            .with_context(|| format!("failed to read {EXPECTATION_ENV}"))?,
    )
    .with_context(|| format!("failed to parse {EXPECTATION_ENV}"))?;
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_unchanged_generic_collect_query",
        run_generic_query_application_test(rt, package_directory, expectation),
    )
}
