use super::*;

const SAMPLE_COUNT: usize = 25;
const MIB: usize = 1024 * 1024;

async fn run_retained_fixture_samples(
    rt: ProdRuntime,
    database: &Database<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    routed: Arc<GeneratedRoutedModule>,
    request: JsonValue,
    controller: Arc<GeneratedMemoryController>,
    metrics: Arc<GateMetrics>,
) -> anyhow::Result<()> {
    let route_identity = routed.route_identity.clone();
    let maximum_guest_memory_bytes = usize::try_from(manifest.limits().max_guest_memory_bytes())?;
    let mut retained: Option<RetainedGeneratedTestRuntime> = None;

    for sample_index in 0..=SAMPLE_COUNT {
        let (reusable_instance, existing_slot, values) = match retained.take() {
            Some(retained) => (
                Some(retained.instance),
                Some(retained.memory_slot_id),
                Some(retained.values),
            ),
            None => (None, None, None),
        };
        let mut state = generated_test_state(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            request.clone(),
            UdfType::Query,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            CancellationSignal::new_for_test(),
            None,
            Arc::clone(&metrics),
            Some(GeneratedTestMemorySetup {
                controller: Arc::clone(&controller),
                route_identity: route_identity.clone(),
                existing_slot,
                values,
                maximum_guest_memory_bytes,
            }),
        )
        .await?;
        arm_generated_timeout(
            rt.clone(),
            &mut state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let mut output =
            execute_generated(Arc::clone(&routed), state, reusable_instance, true).await?;
        anyhow::ensure!(
            output.invocation.outcome == InvocationOutcome::Success,
            "retained latency fixture did not complete successfully: {:?}",
            output.system_error
        );
        if sample_index == SAMPLE_COUNT {
            let instance = output
                .reusable_instance
                .take()
                .context("retained latency fixture did not retain its final runtime")?;
            let memory_permit = output
                .memory_permit
                .take()
                .context("retained latency fixture lost its final memory permit")?;
            discard_generated_instance(instance).await?;
            memory_permit.finish(output.terminal_memory_outcome, false);
        } else {
            retained = Some(retain_generated_test_runtime(&mut output)?);
        }
        drop(output.invocation.transaction);
    }
    anyhow::ensure!(
        retained.is_none(),
        "retained latency fixture kept a runtime after final cleanup"
    );
    Ok(())
}

fn percentile(mut samples: Vec<Duration>, percentile: usize) -> anyhow::Result<Duration> {
    anyhow::ensure!(
        !samples.is_empty() && percentile > 0 && percentile <= 100,
        "latency percentile requires nonempty samples and a percentile in 1..=100"
    );
    samples.sort_unstable();
    let index = (samples.len() * percentile).div_ceil(100) - 1;
    Ok(samples[index])
}

fn microseconds(samples: Vec<Duration>) -> anyhow::Result<JsonValue> {
    let sample_count = samples.len();
    let mean = samples.iter().map(Duration::as_secs_f64).sum::<f64>() / sample_count as f64;
    Ok(json!({
        "mean": mean * 1_000_000.0,
        "p50": percentile(samples.clone(), 50)?.as_secs_f64() * 1_000_000.0,
        "p90": percentile(samples.clone(), 90)?.as_secs_f64() * 1_000_000.0,
        "p99": percentile(samples, 99)?.as_secs_f64() * 1_000_000.0,
    }))
}

fn summarize_warm_samples(
    name: &str,
    observations: &[StaticHermesGeneratedExecutionPhaseObservation],
) -> anyhow::Result<JsonValue> {
    anyhow::ensure!(
        observations.len() == SAMPLE_COUNT + 1,
        "{name} retained latency fixture emitted {} observations instead of {}",
        observations.len(),
        SAMPLE_COUNT + 1
    );
    anyhow::ensure!(
        observations[0].fresh_runtime,
        "{name} retained latency fixture did not begin with a fresh runtime"
    );

    let mut total = Vec::with_capacity(SAMPLE_COUNT);
    let mut before_handler = Vec::with_capacity(SAMPLE_COUNT);
    let mut handler = Vec::with_capacity(SAMPLE_COUNT);
    let mut after_handler = Vec::with_capacity(SAMPLE_COUNT);
    for observation in &observations[1..] {
        anyhow::ensure!(
            !observation.fresh_runtime,
            "{name} retained latency fixture recreated a supposedly warm runtime"
        );
        let handler_started = observation
            .handler_started
            .context("retained latency fixture did not mark handler start")?;
        let handler_completed = observation
            .handler_completed
            .context("retained latency fixture did not mark handler completion")?;
        total.push(observation.total);
        before_handler.push(handler_started);
        handler.push(
            handler_completed
                .checked_sub(handler_started)
                .context("retained latency fixture marked handler completion before start")?,
        );
        after_handler.push(
            observation
                .total
                .checked_sub(handler_completed)
                .context("retained latency fixture completed after its total duration")?,
        );
    }

    Ok(json!({
        "warmSamples": SAMPLE_COUNT,
        "totalUs": microseconds(total)?,
        "beforeHandlerUs": microseconds(before_handler)?,
        "handlerUs": microseconds(handler)?,
        "afterHandlerUs": microseconds(after_handler)?,
    }))
}

#[test]
#[ignore = "manual retained-runtime latency probe; run with --ignored --nocapture"]
fn retained_runtime_latency_breakdown() -> anyhow::Result<()> {
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    let hooks = StaticHermesGateTestHooks::new(0, 0);
    let _test_hooks_guard = install_test_hooks(&hooks)?;
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("static_hermes_retained_runtime_latency", async move {
        let database = new_test_database(rt.clone()).await?;
        let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
            hard_instance_ceiling: 2,
            soft_budget_bytes: 16 * MIB,
            hard_budget_bytes: 24 * MIB,
            safety_reserve_bytes: MIB,
            unattributed_bytes_per_slot: MIB,
            cold_peak_growth_bytes: 4 * MIB,
            warm_idle_target: 0,
            maximum_idle_age: Duration::from_secs(1),
            pressure_enter_headroom_bytes: 2 * MIB,
            pressure_exit_headroom_bytes: 4 * MIB,
        })?;
        controller.set_pressure_for_test(BackendPressure::Healthy {
            headroom_bytes: 32 * MIB,
        });

        let no_op_manifest =
            generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), MIB)?;
        let no_op_routed = generated_memory_test_routed_module(
            Arc::clone(&no_op_manifest),
            GeneratedMemoryTestOperation {
                additional_pages: 0,
                destroy_infinite_loop: false,
                loop_iterations: Some(0),
                maximum_pages: 1,
            },
            GeneratedRouteIdentity::Singleton {
                package_key: "retained-latency-null-result".to_owned(),
            },
        )?;
        let no_op_start = hooks.generated_execution_phases().len();
        run_retained_fixture_samples(
            rt.clone(),
            &database,
            no_op_manifest,
            no_op_routed,
            JsonValue::Null,
            Arc::clone(&controller),
            Arc::clone(&hooks.metrics),
        )
        .await?;
        let no_op_observations = hooks.generated_execution_phases();

        let table: TableName = "retained_latency_documents".parse()?;
        let id = insert_document(&database, &table).await?.developer_id;
        let database_manifest = generated_test_manifest(
            ManifestUdfKind::Query,
            json!({
                "kind": "databaseGet",
                "tableName": table,
            }),
        )?;
        let database_routed = generated_test_routed_module(
            Arc::clone(&database_manifest),
            GeneratedTestOperation::DatabaseGet,
        )?;
        let database_start = no_op_observations.len();
        run_retained_fixture_samples(
            rt.clone(),
            &database,
            database_manifest,
            database_routed,
            json!({ "id": id.encode() }),
            controller,
            Arc::clone(&hooks.metrics),
        )
        .await?;
        let observations = hooks.generated_execution_phases();
        let report = json!({
            "scope": "in-memory test database; warm Store execution only; excludes route preparation, memory admission, transaction construction, and persistent database transport",
            "nullResultNoDatabase": summarize_warm_samples(
                "null-result no-database",
                &no_op_observations[no_op_start..],
            )?,
            "oneDatabaseGet": summarize_warm_samples(
                "one database get",
                &observations[database_start..],
            )?,
        });
        eprintln!(
            "retained Static Hermes runtime latency breakdown:\n{}",
            serde_json::to_string_pretty(&report)?
        );
        database.shutdown().await?;
        Ok(())
    })
}
