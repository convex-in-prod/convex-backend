use super::super::*;
#[cfg(test)]
use super::fixtures::{
    generated_async_batch_test_module,
    generated_database_normalize_id_test_module,
    generated_host_secret_verify_test_module,
    generated_memory_test_module,
    generated_sequential_query_collect_test_module,
    generated_test_module,
};

#[cfg(test)]
pub(in super::super) fn generated_test_execution_context() -> ExecutionContext {
    ExecutionContext::new(
        RequestContext::new_for_system_request(RequestId::new()),
        &FunctionCaller::Cron,
    )
}

#[cfg(test)]
pub(in super::super) struct GeneratedTestMemorySetup {
    pub(in super::super) controller: Arc<GeneratedMemoryController>,
    pub(in super::super) route_identity: GeneratedRouteIdentity,
    pub(in super::super) existing_slot: Option<GeneratedSlotId>,
    pub(in super::super) values: Option<OpaqueValueTable>,
    pub(in super::super) maximum_guest_memory_bytes: usize,
}

#[cfg(test)]
pub(in super::super) struct GeneratedRoutedTestMemorySetup {
    pub(in super::super) controller: Arc<GeneratedMemoryController>,
    pub(in super::super) existing_slot: Option<GeneratedSlotId>,
    pub(in super::super) memory_identity: FunctionMemoryIdentity,
    pub(in super::super) values: Option<OpaqueValueTable>,
}

#[cfg(test)]
pub(in super::super) struct RetainedGeneratedTestRuntime {
    pub(in super::super) instance: GeneratedReusableInstance<ProdRuntime>,
    pub(in super::super) memory_slot_id: GeneratedSlotId,
    pub(in super::super) values: OpaqueValueTable,
}

#[cfg(test)]
pub(in super::super) async fn generated_test_state(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    request: JsonValue,
    udf_type: UdfType,
    unix_timestamp: UnixTimestamp,
    cancellation: CancellationSignal,
    read_control: Option<ReadControl>,
    metrics: Arc<GateMetrics>,
    memory_setup: Option<GeneratedTestMemorySetup>,
) -> anyhow::Result<HostState<ProdRuntime>> {
    generated_test_state_with_rng_seed(
        rt,
        transaction,
        manifest,
        request,
        udf_type,
        unix_timestamp,
        cancellation,
        read_control,
        metrics,
        memory_setup,
        [0; 32],
    )
    .await
}

#[cfg(test)]
pub(in super::super) async fn generated_test_state_with_rng_seed(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    request: JsonValue,
    udf_type: UdfType,
    unix_timestamp: UnixTimestamp,
    cancellation: CancellationSignal,
    read_control: Option<ReadControl>,
    metrics: Arc<GateMetrics>,
    memory_setup: Option<GeneratedTestMemorySetup>,
    rng_seed: [u8; 32],
) -> anyhow::Result<HostState<ProdRuntime>> {
    let mut state = new_state(
        rt.clone(),
        transaction,
        read_control,
        metrics,
        QueryJournal::new(),
    )
    .await?;
    state.provider.set_udf_type(udf_type);
    state.provider.set_rng_seed(rng_seed);
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
        unix_timestamp,
    );
    let memory_setup = match memory_setup {
        Some(memory_setup) => memory_setup,
        None => GeneratedTestMemorySetup {
            controller: Arc::clone(&route_configuration()?.generated_memory_controller),
            route_identity: GeneratedRouteIdentity::Singleton {
                package_key: "generated-test".to_owned(),
            },
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 1 << 20,
        },
    };
    let memory_permit = memory_setup
        .controller
        .admit(
            generated_memory_identity(&manifest, &memory_setup.route_identity),
            memory_setup.existing_slot,
        )
        .await
        .map_err(|_| anyhow::anyhow!("generated test memory admission was rejected"))?;
    let observer = memory_permit.observer();
    let mut values = match memory_setup.values {
        Some(mut values) => {
            values.begin_invocation(observer)?;
            values
        },
        None => OpaqueValueTable::new_with_observer(
            usize::try_from(manifest.limits().max_value_handles())?,
            usize::try_from(manifest.limits().max_host_owned_bytes())?,
            Some(observer),
        ),
    };
    let request_value = match manifest.value_mode() {
        ValueMode::Opaque => OpaqueValue::ConvexJson(request),
        ValueMode::GuestNativeJson => {
            let maximum =
                usize::try_from(manifest.platform_limits().argument_bytes)?.min(MAX_REQUEST_BYTES);
            let request = if state.provider.allows_pending_values() {
                GuestNativeValueCodec::encode_pending(request, maximum)
            } else {
                GuestNativeValueCodec::encode(request, maximum)
            }?;
            OpaqueValue::Bytes(request)
        },
    };
    let request_handle = values.insert(request_value)?;
    let capability_bridge = InvocationCapabilityBridge::unissued();
    let performance_monotonic_start = state.provider.rt().monotonic_now();
    state.generated = Some(GeneratedInvocationState {
        manifest: Arc::new(WasmUdfExecutionPolicy::from_legacy(&manifest)),
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
        interrupt: {
            let interrupt = Arc::new(GeneratedInterruptState::default());
            interrupt.set_execution_phase(GeneratedExecutionPhase::Preparing);
            interrupt
        },
        memory_limiter: memory_permit.limiter(
            StoreLimitsBuilder::new()
                .memory_size(memory_setup.maximum_guest_memory_bytes)
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

#[cfg(test)]
pub(in super::super) async fn generated_routed_test_state(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    routed: &GeneratedRoutedModule,
    request: JsonValue,
    memory_setup: GeneratedRoutedTestMemorySetup,
    metrics: Arc<GateMetrics>,
) -> anyhow::Result<HostState<ProdRuntime>> {
    let mut state = new_state(rt.clone(), transaction, None, metrics, QueryJournal::new()).await?;
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
    let maximum_handles = usize::try_from(routed.manifest.limits().max_value_handles())?;
    let maximum_host_owned_bytes =
        usize::try_from(routed.manifest.limits().max_host_owned_bytes())?;
    let maximum_guest_memory_bytes =
        usize::try_from(routed.manifest.limits().max_guest_memory_bytes())?;
    let allows_caught_official_output_chunk_initialization_failure =
        routed.entry_selector.is_some_and(|entry_selector| {
            matches!(
                &*routed.package_identity,
                ValidatedWasmUdfPackageIdentity::CapabilityEntry(identity)
                    if identity.official_output_chunk_entry_slot(entry_selector).is_some()
            )
        });
    let memory_permit = memory_setup
        .controller
        .admit(memory_setup.memory_identity, memory_setup.existing_slot)
        .await
        .map_err(|_| anyhow::anyhow!("generated routed test memory admission was rejected"))?;
    let observer = memory_permit.observer();
    let mut values = match memory_setup.values {
        Some(mut values) => {
            values.begin_invocation(observer)?;
            values
        },
        None => OpaqueValueTable::new_with_observer(
            maximum_handles,
            maximum_host_owned_bytes,
            Some(observer),
        ),
    };
    let request = GuestNativeValueCodec::encode(
        request,
        usize::try_from(routed.manifest.platform_limits().argument_bytes)?.min(MAX_REQUEST_BYTES),
    )?;
    let request_handle = values.insert(OpaqueValue::Bytes(request))?;
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
        allows_caught_official_output_chunk_initialization_failure,
        discard_after_caught_initialization_failure: false,
        context_read_set_required: false,
        request_handle,
        operation_count: 0,
        terminal_failure: None,
        cancellation: CancellationSignal::new_for_test(),
        interrupt: {
            let interrupt = Arc::new(GeneratedInterruptState::default());
            interrupt.set_execution_phase(GeneratedExecutionPhase::Preparing);
            interrupt
        },
        memory_limiter: memory_permit.limiter(
            StoreLimitsBuilder::new()
                .memory_size(maximum_guest_memory_bytes)
                .instances(routed.store_instance_limit())
                .memories(1)
                .trap_on_grow_failure(true)
                .build(),
        ),
        memory_permit: Some(memory_permit),
        host_secret_values: BTreeMap::new(),
    });
    Ok(state)
}

#[cfg(test)]
pub(in super::super) fn generated_test_routed_module(
    manifest: Arc<WasmUdfExecutionManifest>,
    operation: GeneratedTestOperation,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    generated_test_routed_module_from_bytes(manifest, generated_test_module(operation))
}

#[cfg(test)]
pub(in super::super) fn generated_host_secret_verify_test_routed_module(
    manifest: Arc<WasmUdfExecutionManifest>,
    operation_id: i32,
    pointer: i32,
    length: i32,
    candidate: &[u8],
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    generated_test_routed_module_from_bytes(
        manifest,
        generated_host_secret_verify_test_module(operation_id, pointer, length, candidate),
    )
}

#[cfg(test)]
pub(in super::super) fn generated_database_normalize_id_test_routed_module(
    manifest: Arc<WasmUdfExecutionManifest>,
    operation_id: i32,
    stale_operand: bool,
    retain_operand_for_reuse: bool,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    generated_test_routed_module_from_bytes(
        manifest,
        generated_database_normalize_id_test_module(
            operation_id,
            stale_operand,
            retain_operand_for_reuse,
        ),
    )
}

#[cfg(test)]
pub(in super::super) fn generated_async_batch_test_routed_module(
    manifest: Arc<WasmUdfExecutionManifest>,
    operation: GeneratedAsyncBatchTestOperation,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    generated_test_routed_module_from_bytes(manifest, generated_async_batch_test_module(operation))
}

#[cfg(test)]
pub(in super::super) fn generated_sequential_query_collect_test_routed_module(
    manifest: Arc<WasmUdfExecutionManifest>,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    generated_test_routed_module_from_bytes(
        manifest,
        generated_sequential_query_collect_test_module(),
    )
}

#[cfg(any(test, feature = "testing"))]
pub(in super::super) fn generated_test_routed_module_from_bytes(
    manifest: Arc<WasmUdfExecutionManifest>,
    module_bytes: Vec<u8>,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    let engine = shared_generated_engine()?;
    let module = Module::new(&engine, module_bytes).map_err(wasmtime_anyhow)?;
    validate_generated_module_contract(&module, &manifest)?;
    let artifact = manifest
        .artifact()
        .context("generated test manifest omitted its artifact identity")?;
    let fixed_module_bytes = generated_module_fixed_bytes(
        artifact.core_wasm_bytes(),
        artifact.serialized_module_bytes(),
        &module,
    )?;
    let permitted_conditional_convex_imports = permitted_conditional_convex_imports(
        manifest.value_mode(),
        manifest.effect_execution_mode(),
        manifest.imported_operations(),
    );
    let execution = Arc::new(WasmUdfExecutionPolicy::from_legacy(&manifest));
    let package_identity = Arc::new(ValidatedWasmUdfPackageIdentity::Legacy((*manifest).clone()));
    let entry_selector = package_identity.requires_entry_selector().then_some(0);
    let route_identity = GeneratedRouteIdentity::Singleton {
        package_key: "generated-test".to_owned(),
    };
    Ok(Arc::new(GeneratedRoutedModule {
        emscripten_graph: None,
        engine,
        entry_selector,
        fixed_module_bytes,
        generation: None,
        module_charge: OnceLock::new(),
        module: Arc::new(module),
        manifest: execution,
        package_identity,
        permitted_conditional_convex_imports,
        graph: None,
        pool_identity: GeneratedPoolIdentity::Route(route_identity.clone()),
        route_identity,
    }))
}

#[cfg(test)]
pub(in super::super) fn generated_memory_test_routed_module(
    manifest: Arc<WasmUdfExecutionManifest>,
    operation: GeneratedMemoryTestOperation,
    route_identity: GeneratedRouteIdentity,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    generated_memory_test_routed_module_with_generation(manifest, operation, route_identity, None)
}

#[cfg(test)]
pub(in super::super) fn generated_memory_test_routed_module_with_generation(
    manifest: Arc<WasmUdfExecutionManifest>,
    operation: GeneratedMemoryTestOperation,
    route_identity: GeneratedRouteIdentity,
    generation: Option<Arc<DeploymentGeneration>>,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    let engine = shared_generated_engine()?;
    let module =
        Module::new(&engine, generated_memory_test_module(operation)).map_err(wasmtime_anyhow)?;
    validate_generated_module_contract(&module, &manifest)?;
    let artifact = manifest
        .artifact()
        .context("generated memory test manifest omitted its artifact identity")?;
    let fixed_module_bytes = generated_module_fixed_bytes(
        artifact.core_wasm_bytes(),
        artifact.serialized_module_bytes(),
        &module,
    )?;
    let permitted_conditional_convex_imports = permitted_conditional_convex_imports(
        manifest.value_mode(),
        manifest.effect_execution_mode(),
        manifest.imported_operations(),
    );
    let execution = Arc::new(WasmUdfExecutionPolicy::from_legacy(&manifest));
    let package_identity = Arc::new(ValidatedWasmUdfPackageIdentity::Legacy((*manifest).clone()));
    Ok(Arc::new(GeneratedRoutedModule {
        emscripten_graph: None,
        engine,
        entry_selector: None,
        fixed_module_bytes,
        generation,
        module_charge: OnceLock::new(),
        module: Arc::new(module),
        manifest: execution,
        package_identity,
        permitted_conditional_convex_imports,
        graph: None,
        pool_identity: GeneratedPoolIdentity::Route(route_identity.clone()),
        route_identity,
    }))
}
