use super::*;

fn insert_performance_now_request(
    state: &mut HostState<ProdRuntime>,
) -> anyhow::Result<OpaqueHandle> {
    let bytes = GuestNativeValueCodec::encode(
        json!({
            "version": 1,
            "kind": "performanceNow",
        }),
        MAX_REQUEST_BYTES,
    )?;
    let request = GuestCapabilityRequestCodec::decode(&bytes, bytes.len())?;
    Ok(generated_state_mut(state)?
        .values
        .insert(OpaqueValue::CapabilityRequest(request))?)
}

fn run_performance_now(state: &mut HostState<ProdRuntime>, capability: i64) -> anyhow::Result<f64> {
    let request = insert_performance_now_request(state)?;
    let result = run_generated_capability_sync_operation(state, capability, request.to_abi())?;
    let result = opaque_handle(result)?;
    let milliseconds = generated_state(state)?
        .values
        .get_json(result)?
        .as_f64()
        .context("performance.now host result is not a number")?;
    generated_state_mut(state)?
        .values
        .release(result, OpaqueValueKind::ConvexJson)?;
    Ok(milliseconds)
}

async fn run_performance_now_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let manifest = generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Mutation,
        Vec::new(),
        1 << 20,
        1_000_000,
        16,
    )?;
    let controller = Arc::clone(&route_configuration()?.generated_memory_controller);
    let route_identity = GeneratedRouteIdentity::Singleton {
        package_key: "generated-performance-now-test".to_owned(),
    };
    let metrics = Arc::new(GateMetrics::default());
    let mut first = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Mutation,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 1 << 20,
        }),
    )
    .await?;
    let first_capability = generated_state(&first)?.capability_bridge.handle()?;
    let import_phase_request = insert_performance_now_request(&mut first)?;
    assert!(run_generated_capability_sync_operation(
        &mut first,
        first_capability,
        import_phase_request.to_abi(),
    )
    .is_err());
    assert!(!first.provider.observed_time());
    {
        let generated = generated_state_mut(&mut first)?;
        generated.performance_runtime_available = true;
        generated.performance_monotonic_start = rt.monotonic_now() - Duration::from_millis(2_000);
    }
    let aged_milliseconds = run_performance_now(&mut first, first_capability)?;
    assert!(aged_milliseconds >= 2_000.0);
    assert!(first.provider.observed_time());

    let first_transaction = first.provider.take_transaction()?;
    let mut first_generated = first
        .generated
        .take()
        .context("first performance.now invocation state disappeared")?;
    first_generated.values.cleanup();
    let values = std::mem::replace(&mut first_generated.values, OpaqueValueTable::new(1, 1));
    let memory_permit = first_generated
        .memory_permit
        .take()
        .context("first performance.now memory permit disappeared")?;
    let memory_slot_id = memory_permit.slot_id();
    drop(first_generated);
    drop(first);
    memory_permit.finish(TerminalMemoryOutcome::Success, true);
    drop(first_transaction);

    let mut reused = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        JsonValue::Null,
        UdfType::Mutation,
        UnixTimestamp::from_nanos(1_800_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        metrics,
        Some(GeneratedTestMemorySetup {
            controller,
            route_identity,
            existing_slot: Some(memory_slot_id),
            values: Some(values),
            maximum_guest_memory_bytes: 1 << 20,
        }),
    )
    .await?;
    generated_state_mut(&mut reused)?.performance_runtime_available = true;
    let reused_capability = generated_state(&reused)?.capability_bridge.handle()?;
    let fresh_milliseconds = run_performance_now(&mut reused, reused_capability)?;
    assert!(fresh_milliseconds + 1_000.0 < aged_milliseconds);

    reused.provider.set_udf_type(UdfType::Query);
    generated_state_mut(&mut reused)?.performance_monotonic_start =
        rt.monotonic_now() - Duration::from_millis(2_000);
    assert_eq!(run_performance_now(&mut reused, reused_capability)?, 0.0);

    let zero_identity_request = insert_performance_now_request(&mut reused)?;
    assert_eq!(
        run_generated_capability_sync_operation(&mut reused, 0, zero_identity_request.to_abi(),)?,
        -2
    );
    assert!(generated_state(&reused)?
        .values
        .get(zero_identity_request, OpaqueValueKind::CapabilityRequest)
        .is_ok());

    generated_state_mut(&mut reused)?
        .capability_bridge
        .revoke()?;
    assert_eq!(
        run_generated_capability_sync_operation(
            &mut reused,
            reused_capability,
            zero_identity_request.to_abi(),
        )?,
        -2
    );
    assert!(generated_state(&reused)?.runtime_reuse_contaminated);
    assert!(generated_state(&reused)?
        .values
        .get(zero_identity_request, OpaqueValueKind::CapabilityRequest)
        .is_ok());

    generated_state_mut(&mut reused)?.values.cleanup();
    let reused_transaction = reused.provider.take_transaction()?;
    let memory_permit = generated_state_mut(&mut reused)?
        .memory_permit
        .take()
        .context("reused performance.now memory permit disappeared")?;
    drop(reused);
    memory_permit.finish(TerminalMemoryOutcome::SystemError, false);
    drop(reused_transaction);
    database.shutdown().await?;
    Ok(())
}

#[test]
fn performance_now_is_capability_scoped_and_resets_with_a_reused_slot() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_performance_now",
        run_performance_now_tests(rt),
    )
}
