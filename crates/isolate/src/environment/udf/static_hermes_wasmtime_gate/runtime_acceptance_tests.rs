use super::{
    artifact_registry_contract_tests::{
        generated_module_cache_test_controller,
        NativeCapabilityTestExpectation,
        NativeCapabilityTestSelector,
        OfficialOutputChunkNativeValidationReport,
        OfficialOutputChunkSmokeExpectation,
        OfficialOutputChunkSmokeSelector,
        NATIVE_CAPABILITY_TEST_EXPECTATION_ENV,
        NATIVE_CAPABILITY_TEST_PACKAGE_DIRECTORY_ENV,
        OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_EXPECTATION_ENV,
        OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_MODULE_ENV,
    },
    *,
};

#[cfg(test)]
mod database_get_conformance_boundary_tests;

#[cfg(test)]
async fn run_generated_fuel_acceptance(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);

    let default_manifest = generated_test_manifest_with_limits_and_fuel(
        ManifestUdfKind::Query,
        Vec::new(),
        1 << 20,
        100,
    )?;
    let default_routed = generated_memory_test_routed_module(
        Arc::clone(&default_manifest),
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: None,
            maximum_pages: 1,
        },
        GeneratedRouteIdentity::Singleton {
            package_key: "generated-fuel-default".to_owned(),
        },
    )?;
    let mut default_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        default_manifest,
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut default_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let default_output = execute_generated(default_routed, default_state, None, false).await?;
    assert_eq!(
        default_output.invocation.outcome,
        InvocationOutcome::FuelExhausted
    );
    assert_eq!(
        default_output.terminal_memory_outcome,
        TerminalMemoryOutcome::ResourceLimit
    );
    assert!(default_output.reusable_instance.is_none());
    assert!(default_output.memory_permit.is_none());
    let (transaction, outcome) = default_output
        .invocation
        .into_routed_result(default_output.system_error, false)?;
    let FunctionOutcome::Query(outcome) = outcome else {
        anyhow::bail!("expected query outcome");
    };
    assert!(outcome.result.is_err());
    drop(transaction);

    let test_hooks = StaticHermesGateTestHooks::new(0, 0);
    test_hooks.set_generated_execution_fuel_override(100)?;
    let _test_hooks_guard = install_test_hooks(&test_hooks)?;
    let overridden_manifest = generated_test_manifest_with_limits_and_fuel(
        ManifestUdfKind::Query,
        Vec::new(),
        1 << 20,
        1_000_000_000,
    )?;
    let overridden_routed = generated_memory_test_routed_module(
        Arc::clone(&overridden_manifest),
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: None,
            maximum_pages: 1,
        },
        GeneratedRouteIdentity::Singleton {
            package_key: "generated-fuel-override".to_owned(),
        },
    )?;
    let mut overridden_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        overridden_manifest,
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut overridden_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let overridden_output =
        execute_generated(overridden_routed, overridden_state, None, false).await?;
    assert_eq!(
        overridden_output.invocation.outcome,
        InvocationOutcome::FuelExhausted
    );
    assert_eq!(
        overridden_output.terminal_memory_outcome,
        TerminalMemoryOutcome::ResourceLimit
    );
    assert!(overridden_output.reusable_instance.is_none());
    assert!(overridden_output.memory_permit.is_none());
    let error = overridden_output
        .invocation
        .into_routed_result(overridden_output.system_error, true)
        .err()
        .context("fuel-limited shadow was accepted for comparison")?;
    assert_eq!(
        error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
        Some(&StaticHermesWasmExecutionFailure::InstructionBudget)
    );

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_fixed_entry_prepare_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
    let routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        generated_fixed_entry_prepare_test_module(),
    )?;
    anyhow::ensure!(
        !routed.package_identity.requires_entry_selector()
            && routed.package_identity.requires_selected_entry_prepare(),
        "fixed-entry test package did not keep selection and preparation contracts distinct"
    );
    let metrics = Arc::new(GateMetrics::default());
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
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
    let output = execute_generated(routed, state, None, false).await?;
    assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
    drop(output.invocation.transaction);
    let phases = metrics.generated_execution_phases.lock();
    anyhow::ensure!(
        matches!(
            phases.as_slice(),
            [phase]
                if phase.selected_entry_preparation_required
                    && phase.entry_selection_started.is_none()
                    && phase.entry_selection_completed.is_none()
                    && phase.prepare_started.is_some()
                    && phase.prepare_completed.is_some()
        ),
        "fixed-entry execution did not record preparation without entry selection"
    );
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_warm_entry_prepare_environment_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    initialize_application_system_tables(&database).await?;
    let manifest = generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        1 << 20,
        1_000_000_000,
        2,
    )?;
    let names: [EnvVarName; 2] = [
        "INITIALIZATION_FIRST_SETTING".parse()?,
        "INITIALIZATION_SECOND_SETTING".parse()?,
    ];
    let mut setup = database.begin_system().await?;
    for name in &names {
        EnvironmentVariablesModel::new(&mut setup)
            .create(
                EnvironmentVariable::new(name.clone(), "original".parse()?),
                &Default::default(),
            )
            .await?;
    }
    database
        .commit_with_write_source(setup, "preparation_environment_setup")
        .await?;
    let environment_requests = names
        .iter()
        .map(|name| {
            GuestNativeValueCodec::encode(
                json!({
                    "version": 4, "kind": "environmentVariableGet", "name": name.to_string(),
                }),
                MAX_REQUEST_BYTES,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let unauthorized_request = GuestNativeValueCodec::encode(
        json!({ "version": 4, "kind": "performanceNow" }),
        MAX_REQUEST_BYTES,
    )?;
    let routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        super::test_support::generated_entry_prepare_environment_test_module(
            [&environment_requests[0], &environment_requests[1]],
            &unauthorized_request,
        ),
    )?;
    anyhow::ensure!(
        routed.package_identity.requires_entry_selector()
            && routed.package_identity.requires_selected_entry_prepare(),
        "environment preparation test requires entry selection and preparation"
    );
    let metrics = Arc::new(GateMetrics::default());
    let controller = generated_module_cache_test_controller(8 << 20, 12 << 20)?;
    let mut retained: Option<RetainedGeneratedTestRuntime> = None;
    let mut first_runtime_id = None;
    let mut first_range_hashes = None;
    for (iteration, selector) in [0, 1].into_iter().enumerate() {
        let (instance, existing_slot, values) = match retained.take() {
            Some(retained) => (
                Some(retained.instance),
                Some(retained.memory_slot_id),
                Some(retained.values),
            ),
            None => (None, None, None),
        };
        let previous_interval_accounting = instance
            .as_ref()
            .and_then(|instance| instance.context_read_set.as_ref())
            .map(|read_set| read_set.read_set.num_intervals());
        let mut transaction = database.begin_system().await?;
        if let Some(instance) = &instance {
            assert!(
                ContextCache::validate_and_apply_context_read_set(
                    &mut transaction,
                    instance
                        .context_read_set
                        .as_ref()
                        .context("missing cold dependencies")?,
                )
                .await?
            );
        }
        let mut state = generated_test_state(
            rt.clone(),
            transaction,
            Arc::clone(&manifest),
            JsonValue::Null,
            UdfType::Query,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            CancellationSignal::new_for_test(),
            None,
            Arc::clone(&metrics),
            Some(GeneratedTestMemorySetup {
                controller: Arc::clone(&controller),
                route_identity: routed.route_identity.clone(),
                existing_slot,
                values,
                maximum_guest_memory_bytes: 1 << 20,
            }),
        )
        .await?;
        generated_state_mut(&mut state)?.context_read_set_required = true;
        if iteration == 0 {
            state.provider.snoop_initialization_reads()?;
        }
        arm_generated_timeout(
            rt.clone(),
            &mut state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let mut output = execute_generated_with_entry_selector(
            Arc::clone(&routed),
            Some(selector),
            state,
            instance,
            true,
        )
        .await?;
        assert_eq!(
            output.invocation.outcome,
            InvocationOutcome::Success,
            "iteration {iteration}: {:?}",
            output.system_error
        );
        assert_eq!(output.invocation.function_result, Some(JsonValue::Null));
        assert_eq!(
            output.invocation.operation_count,
            u64::try_from(iteration + 1)?
        );
        assert!(output.invocation.capability_revoked);
        if let Some(first_id) = first_runtime_id {
            assert_eq!(output.runtime_id, first_id);
        } else {
            first_runtime_id = Some(output.runtime_id);
        }
        let context_read_set = output
            .reusable_instance
            .as_ref()
            .and_then(|instance| instance.context_read_set.as_ref())
            .context("successful preparation omitted its context read set")?;
        let structural_intervals = context_read_set
            .read_set
            .read_set()
            .iter_indexed()
            .map(|(_, reads)| reads.intervals.len())
            .sum::<usize>();
        let interval_accounting = context_read_set.read_set.num_intervals();
        assert_eq!(
            output.invocation.read_accounting.intervals, interval_accounting,
            "iteration {iteration} transaction and retained context accounting diverged"
        );
        if let Some(previous_interval_accounting) = previous_interval_accounting {
            assert!(
                interval_accounting > previous_interval_accounting,
                "warm preparation did not add its read accounting"
            );
        } else {
            assert!(
                structural_intervals > interval_accounting,
                "cold preparation did not retain its unaccounted derived dependencies"
            );
        }
        assert_eq!(
            context_read_set.read_set.read_set().iter_search().count(),
            0,
            "preparation configuration reads unexpectedly captured search dependencies"
        );
        if iteration == 0 {
            assert!(!context_read_set.range_hashes.is_empty());
            first_range_hashes = Some(context_read_set.range_hashes.clone());
        } else {
            let first_range_hashes = first_range_hashes
                .as_ref()
                .context("cold preparation dependency hashes disappeared")?;
            assert!(
                first_range_hashes
                    .iter()
                    .all(|hash| context_read_set.range_hashes.contains(hash)),
                "warm preparation discarded an earlier initialization dependency hash"
            );
        }
        retained = Some(retain_generated_test_runtime(&mut output)?);
        drop(output.invocation.transaction);
    }
    let retained = retained.context("preparation test did not retain its runtime")?;
    let read_set = retained
        .instance
        .context_read_set
        .as_ref()
        .context("missing warm dependencies")?;
    for name in &names {
        let mut unchanged = database.begin_system().await?;
        assert!(ContextCache::validate_and_apply_context_read_set(&mut unchanged, read_set).await?);
        drop(unchanged);
        let mut changed = database.begin_system().await?;
        EnvironmentVariablesModel::new(&mut changed)
            .delete(name)
            .await?
            .context("missing fixture setting")?;
        EnvironmentVariablesModel::new(&mut changed)
            .create(
                EnvironmentVariable::new(name.clone(), "changed".parse()?),
                &Default::default(),
            )
            .await?;
        // An uncommitted configuration edit must invalidate either entry's
        // initialization dependencies, without affecting the next test case.
        assert!(!ContextCache::validate_and_apply_context_read_set(&mut changed, read_set).await?);
    }
    let [(slot_id, reason)] = <[_; 1]>::try_from(controller.idle_eviction_candidates(
        &[(
            retained.memory_slot_id,
            retained.instance.idle_since.elapsed(),
        )],
        IdleEvictionTrigger::GenerationRetirement,
    ))
    .map_err(|_| anyhow::anyhow!("preparation test runtime was not selected for eviction"))?;
    discard_generated_instance(retained.instance).await?;
    controller.finish_idle_eviction(slot_id, reason);
    let phases = metrics.generated_execution_phases.lock().clone();
    anyhow::ensure!(
        phases.len() == 2
            && phases
                .iter()
                .all(|phase| phase.selected_entry_preparation_required
                    && phase.entry_selection_started.is_some()
                    && phase.entry_selection_completed.is_some()
                    && phase.prepare_started.is_some()
                    && phase.prepare_completed.is_some()),
        "environment preparation execution did not record selection and preparation"
    );
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_selected_entry_dispatch_failure_test(rt: ProdRuntime) -> anyhow::Result<()> {
    const ENTRY_SELECTOR: u64 = 0xf123_4567_89ab_cdef;

    #[derive(Clone)]
    struct ExpectedPhase {
        fault: GeneratedSelectedEntryDispatchFault,
        failure: Option<StaticHermesWasmExecutionFailure>,
        entry_selection_status: Option<i32>,
        entry_selection_completed: bool,
        prepare_started: bool,
        prepare_status: Option<i32>,
        prepare_completed: bool,
        second_entry_selection_started: bool,
        second_entry_selection_status: Option<i32>,
        second_entry_selection_completed: bool,
        handler_started: bool,
        handler_completed: bool,
    }

    let cases = [
        ExpectedPhase {
            fault: GeneratedSelectedEntryDispatchFault::Success,
            failure: None,
            entry_selection_status: Some(0),
            entry_selection_completed: true,
            prepare_started: true,
            prepare_status: Some(0),
            prepare_completed: true,
            second_entry_selection_started: true,
            second_entry_selection_status: Some(0),
            second_entry_selection_completed: true,
            handler_started: true,
            handler_completed: true,
        },
        ExpectedPhase {
            fault: GeneratedSelectedEntryDispatchFault::HandlerTrap,
            failure: Some(StaticHermesWasmExecutionFailure::WasmtimeTrap {
                diagnostic: StaticHermesWasmTrapDiagnostic {
                    ..StaticHermesWasmTrapDiagnostic::new(
                        StaticHermesWasmTrapCode::UnreachableCodeReached,
                        Some(3),
                        Some(8),
                    )
                },
            }),
            entry_selection_status: Some(0),
            entry_selection_completed: true,
            prepare_started: true,
            prepare_status: Some(0),
            prepare_completed: true,
            second_entry_selection_started: true,
            second_entry_selection_status: Some(0),
            second_entry_selection_completed: true,
            handler_started: true,
            handler_completed: false,
        },
        ExpectedPhase {
            fault: GeneratedSelectedEntryDispatchFault::FirstSelectorReject,
            failure: Some(StaticHermesWasmExecutionFailure::GeneratedExportDispatch),
            entry_selection_status: Some(7),
            entry_selection_completed: false,
            prepare_started: false,
            prepare_status: None,
            prepare_completed: false,
            second_entry_selection_started: false,
            second_entry_selection_status: None,
            second_entry_selection_completed: false,
            handler_started: false,
            handler_completed: false,
        },
        ExpectedPhase {
            fault: GeneratedSelectedEntryDispatchFault::FirstSelectorTrap,
            failure: Some(StaticHermesWasmExecutionFailure::GeneratedExportDispatch),
            entry_selection_status: None,
            entry_selection_completed: false,
            prepare_started: false,
            prepare_status: None,
            prepare_completed: false,
            second_entry_selection_started: false,
            second_entry_selection_status: None,
            second_entry_selection_completed: false,
            handler_started: false,
            handler_completed: false,
        },
        ExpectedPhase {
            fault: GeneratedSelectedEntryDispatchFault::PreparationStatus(65),
            failure: Some(StaticHermesWasmExecutionFailure::GeneratedExportDispatch),
            entry_selection_status: Some(0),
            entry_selection_completed: true,
            prepare_started: true,
            prepare_status: Some(65),
            prepare_completed: false,
            second_entry_selection_started: false,
            second_entry_selection_status: None,
            second_entry_selection_completed: false,
            handler_started: false,
            handler_completed: false,
        },
        ExpectedPhase {
            fault: GeneratedSelectedEntryDispatchFault::PreparationTrap,
            failure: Some(StaticHermesWasmExecutionFailure::GeneratedExportDispatch),
            entry_selection_status: Some(0),
            entry_selection_completed: true,
            prepare_started: true,
            prepare_status: None,
            prepare_completed: false,
            second_entry_selection_started: false,
            second_entry_selection_status: None,
            second_entry_selection_completed: false,
            handler_started: false,
            handler_completed: false,
        },
        ExpectedPhase {
            fault: GeneratedSelectedEntryDispatchFault::SecondSelectorReject,
            failure: Some(StaticHermesWasmExecutionFailure::GeneratedExportDispatch),
            entry_selection_status: Some(0),
            entry_selection_completed: true,
            prepare_started: true,
            prepare_status: Some(0),
            prepare_completed: true,
            second_entry_selection_started: true,
            second_entry_selection_status: Some(9),
            second_entry_selection_completed: false,
            handler_started: false,
            handler_completed: false,
        },
        ExpectedPhase {
            fault: GeneratedSelectedEntryDispatchFault::SecondSelectorTrap,
            failure: Some(StaticHermesWasmExecutionFailure::GeneratedExportDispatch),
            entry_selection_status: Some(0),
            entry_selection_completed: true,
            prepare_started: true,
            prepare_status: Some(0),
            prepare_completed: true,
            second_entry_selection_started: true,
            second_entry_selection_status: None,
            second_entry_selection_completed: false,
            handler_started: false,
            handler_completed: false,
        },
    ];

    let database = new_test_database(rt.clone()).await?;
    for expected in cases {
        let manifest = generated_schema_five_test_manifest_with_runtime_limits(
            ManifestUdfKind::Query,
            Vec::new(),
            1 << 20,
            1_000_000,
            128,
        )?;
        let routed = generated_test_routed_module_from_bytes(
            Arc::clone(&manifest),
            generated_selected_entry_dispatch_fault_test_module(expected.fault, ENTRY_SELECTOR),
        )?;
        anyhow::ensure!(
            routed.package_identity.requires_entry_selector()
                && routed.package_identity.requires_selected_entry_prepare(),
            "selector dispatch test package omitted a required dispatch phase"
        );
        let metrics = Arc::new(GateMetrics::default());
        let mut state = generated_test_state(
            rt.clone(),
            database.begin_system().await?,
            manifest,
            JsonValue::Null,
            UdfType::Query,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            CancellationSignal::new_for_test(),
            None,
            Arc::clone(&metrics),
            None,
        )
        .await?;
        let expected_invocation_correlation = state.provider.invocation_correlation()?;
        arm_generated_timeout(
            rt.clone(),
            &mut state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        state.provider.enable_host_operation_trace();
        let mut output =
            execute_generated_with_entry_selector(routed, Some(ENTRY_SELECTOR), state, None, false)
                .await?;
        assert!(output.invocation.capability_revoked);
        if expected.failure.is_none() {
            assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
            assert_eq!(output.invocation.function_result, Some(JsonValue::Null));
            assert!(output.system_error.is_none());
        } else {
            assert_eq!(output.invocation.outcome, InvocationOutcome::SystemError);
            assert!(output.invocation.function_result.is_none());
            let error = output
                .system_error
                .take()
                .context("generated dispatch failure lost its classified error")?;
            let actual = error.downcast_ref::<StaticHermesWasmExecutionFailure>();
            match (expected.failure.as_ref(), actual) {
                (
                    Some(StaticHermesWasmExecutionFailure::WasmtimeTrap { .. }),
                    Some(StaticHermesWasmExecutionFailure::WasmtimeTrap { diagnostic }),
                ) => {
                    assert_eq!(
                        diagnostic.code,
                        StaticHermesWasmTrapCode::UnreachableCodeReached
                    );
                    assert!(!diagnostic.frames.is_empty());
                    assert!(diagnostic.frames.iter().any(|frame| {
                        frame.module_role == StaticHermesWasmTrapModuleRole::Route
                            && frame.module_ordinal == Some(0)
                            && frame.function_index == 3
                            && frame.function_offset == Some(8)
                    }));
                    assert!(!diagnostic.frames_truncated);
                    assert_eq!(
                        diagnostic.invocation_correlation,
                        expected_invocation_correlation
                    );
                    assert!(!diagnostic.reused_instance);
                    assert!(diagnostic.fuel.limit > 0);
                    assert!(diagnostic.fuel.remaining.is_some());
                    assert_eq!(diagnostic.resources.operation_count, 0);
                    assert!(diagnostic.resources.operation_limit > 0);
                    assert_eq!(diagnostic.resources.value_handle_count, 1);
                    assert!(diagnostic.resources.value_handle_limit > 1);
                    assert!(diagnostic.host_operation_trace_available);
                    assert!(diagnostic.host_operations.is_empty());
                    assert!(!diagnostic.host_operations_truncated);
                    assert_eq!(
                        diagnostic.stderr.classification,
                        StaticHermesWasmTrapStderrClassification::Empty
                    );
                    assert_eq!(diagnostic.stderr.byte_count, 0);
                },
                _ => assert_eq!(actual, expected.failure.as_ref()),
            }
        }
        drop(output.invocation.transaction);

        let phases = metrics.generated_execution_phases.lock();
        let [phase] = phases.as_slice() else {
            anyhow::bail!(
                "generated dispatch fault {:?} recorded {} phase traces",
                expected.fault,
                phases.len()
            );
        };
        assert!(phase.entry_selection_started.is_some());
        assert_eq!(
            phase.entry_selection_status, expected.entry_selection_status,
            "first selector status for {:?}",
            expected.fault
        );
        assert_eq!(
            phase.entry_selection_completed.is_some(),
            expected.entry_selection_completed,
            "first selector completion for {:?}",
            expected.fault
        );
        assert_eq!(
            phase.prepare_started.is_some(),
            expected.prepare_started,
            "preparation reach for {:?}",
            expected.fault
        );
        assert_eq!(
            phase.prepare_status, expected.prepare_status,
            "preparation status for {:?}",
            expected.fault
        );
        assert_eq!(
            phase.prepare_completed.is_some(),
            expected.prepare_completed,
            "preparation completion for {:?}",
            expected.fault
        );
        assert_eq!(
            phase.second_entry_selection_started.is_some(),
            expected.second_entry_selection_started,
            "second selector reach for {:?}",
            expected.fault
        );
        assert_eq!(
            phase.second_entry_selection_status, expected.second_entry_selection_status,
            "second selector status for {:?}",
            expected.fault
        );
        assert_eq!(
            phase.second_entry_selection_completed.is_some(),
            expected.second_entry_selection_completed,
            "second selector completion for {:?}",
            expected.fault
        );
        assert_eq!(
            phase.handler_started.is_some(),
            expected.handler_started,
            "handler reach for {:?}",
            expected.fault
        );
        assert_eq!(
            phase.handler_completed.is_some(),
            expected.handler_completed,
            "handler completion for {:?}",
            expected.fault
        );
    }
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_saturated_cpu_admission_preserves_warm_instance(
    rt: ProdRuntime,
) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let route_identity = GeneratedRouteIdentity::Singleton {
        package_key: "generated-saturated-cpu-admission".to_owned(),
    };
    let manifest = generated_test_manifest_with_limits_and_fuel(
        ManifestUdfKind::Query,
        Vec::new(),
        4 * WASM_PAGE_BYTES,
        SUCCESS_FUEL,
    )?;
    let generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/saturated-cpu-admission"),
        ValidatedDeploymentManifest::empty_for_test(&"f".repeat(64)),
    );
    let routed = generated_memory_test_routed_module_with_generation(
        Arc::clone(&manifest),
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: Some(0),
            maximum_pages: 4,
        },
        route_identity.clone(),
        Some(Arc::clone(&generation)),
    )?;
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 1,
        soft_budget_bytes: 16 * WASM_PAGE_BYTES,
        hard_budget_bytes: 20 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 1,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let database = new_test_database(rt.clone()).await?;
    let metrics = Arc::new(GateMetrics::default());
    let cpu_limiter = ConcurrencyLimiter::new(1);
    let timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);

    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    state.active_wasm_cpu_limiter = cpu_limiter.clone();
    arm_generated_timeout(
        rt.clone(),
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut warmed = execute_generated(Arc::clone(&routed), state, None, true).await?;
    let warm_runtime_id = warmed
        .reusable_instance
        .as_ref()
        .context("warm admission fixture did not retain its runtime")?
        .id;
    drop(warmed.invocation.transaction);
    return_generated_routed_instance(
        warmed
            .reusable_instance
            .take()
            .context("warm admission fixture lost its runtime")?,
        warmed
            .memory_permit
            .take()
            .context("warm admission fixture lost its memory permit")?,
        warmed.terminal_memory_outcome,
    )
    .await?;

    let mut checked_out_instance: Option<GeneratedReusableInstance<ProdRuntime>> = None;
    let cancellation = CancellationSignal::new_for_test();
    let memory_permit = acquire_generated_routed_memory_permit(
        &controller,
        memory_identity.clone(),
        &routed,
        true,
        &mut checked_out_instance,
        tokio::time::Instant::now() + Duration::from_secs(1),
        &cancellation,
        &metrics,
        true,
    )
    .await?;
    assert_eq!(
        checked_out_instance
            .as_ref()
            .context("saturated CPU admission did not check out the warm runtime")?
            .id,
        warm_runtime_id
    );

    let held_cpu_permit = cpu_limiter
        .acquire(
            Arc::new("generated saturated CPU admission fixture".to_owned()),
            false,
        )
        .await;
    let error = acquire_active_wasm_cpu_permit(
        &cpu_limiter,
        &cancellation,
        true,
        tokio::time::Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect_err("saturated query-shadow CPU admission unexpectedly succeeded");
    assert!(error.is::<StaticHermesQueryShadowCapacityUnavailable>());
    return_optional_generated_routed_instance_after_cpu_admission_rejection(
        rt.clone(),
        checked_out_instance,
        memory_permit,
        None,
    );
    drop(held_cpu_permit);

    let preserved = controller.snapshot_for_test(&memory_identity);
    assert_eq!(preserved.active_instances, 0);
    assert_eq!(preserved.idle_instances, 1);
    assert_eq!(preserved.function_samples, 1);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 0);

    let mut retired_instance: Option<GeneratedReusableInstance<ProdRuntime>> = None;
    let retired_memory_permit = acquire_generated_routed_memory_permit(
        &controller,
        memory_identity.clone(),
        &routed,
        true,
        &mut retired_instance,
        tokio::time::Instant::now() + Duration::from_secs(1),
        &cancellation,
        &metrics,
        true,
    )
    .await?;
    generation.retired.store(true, Ordering::Release);
    let held_retired_cpu_permit = cpu_limiter
        .acquire(
            Arc::new("generated retired shadow rejection fixture".to_owned()),
            false,
        )
        .await;
    let error = acquire_active_wasm_cpu_permit(
        &cpu_limiter,
        &cancellation,
        true,
        tokio::time::Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect_err("retired saturated query-shadow CPU admission unexpectedly succeeded");
    assert!(error.is::<StaticHermesQueryShadowCapacityUnavailable>());
    return_optional_generated_routed_instance_after_cpu_admission_rejection(
        rt.clone(),
        retired_instance,
        retired_memory_permit,
        None,
    );
    let waiting_for_teardown = controller.snapshot_for_test(&memory_identity);
    assert_eq!(waiting_for_teardown.active_instances, 1);
    assert_eq!(waiting_for_teardown.idle_instances, 0);
    assert_eq!(waiting_for_teardown.function_samples, 1);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 0);
    drop(held_retired_cpu_permit);
    tokio::time::timeout(Duration::from_secs(5), async {
        while metrics.teardowns.load(Ordering::SeqCst) == 0
            || controller
                .snapshot_for_test(&memory_identity)
                .active_instances
                != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("retired shadow rejection cleanup did not complete")?;
    let cleaned = controller.snapshot_for_test(&memory_identity);
    assert_eq!(cleaned.active_instances, 0);
    assert_eq!(cleaned.idle_instances, 0);
    assert_eq!(cleaned.function_samples, 1);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 1);
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
fn generated_routed_test_path_and_args(
    args: Vec<JsonValue>,
) -> anyhow::Result<ValidatedPathAndArgs> {
    let args = SerializedArgs::from_args(args)?;
    ValidatedPathAndArgs::from_proto(pb::common::ValidatedPathAndArgs {
        path: Some("generated_test:run".to_owned()),
        args: Some(args.into_bytes()),
        npm_version: Some(Version::new(1, 43, 0).to_string()),
        component_path: Some(ComponentPath::root().into()),
        component_id: ComponentId::Root.serialize_to_string(),
        reuse_context: Some(false),
        context_reuse: Some(Default::default()),
    })
}

#[cfg(test)]
async fn execute_generated_routed_test_invocation(
    rt: ProdRuntime,
    client: &IsolateClient<ProdRuntime>,
    database: &Database<ProdRuntime>,
    args: Vec<JsonValue>,
    shadow_work_guard: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
) -> anyhow::Result<()> {
    let shadow = shadow_work_guard.is_some();
    let path_and_args = generated_routed_test_path_and_args(args)?;
    let route = if shadow {
        resolve_compatibility_shadow_route(UdfType::Query, &path_and_args)?
    } else {
        resolve_route(UdfType::Query, &path_and_args)?
    }
    .context("generated routed test route was not resolved")?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
    let environment_data = EnvironmentData {
        key_broker: KeyBroker::dev().function_runner_keybroker(),
        default_system_env_vars: BTreeMap::new(),
        file_storage: TransactionalFileStorage::new(
            rt.clone(),
            storage,
            ConvexOrigin::from("http://127.0.0.1:3210".to_owned()),
        ),
        module_loader: Arc::new(UnusedGateModuleCache),
        deployment: DeploymentMetadata {
            name: "generated-routed-test".to_owned(),
            region: None,
            class: DeploymentClass::S16,
        },
        host_secret_values: Some(BTreeMap::new()),
    };
    let prepared = IsolateClient::<ProdRuntime>::prepare_static_hermes_wasmtime_invocation(
        route,
        UdfType::Query,
        path_and_args,
        database.begin_system().await?,
        if shadow {
            StaticHermesWasmtimePreparationMode::Shadow
        } else {
            StaticHermesWasmtimePreparationMode::Primary
        },
    )
    .await?;
    let (transaction, _outcome) = client
        .execute_static_hermes_wasmtime_gate(
            prepared,
            "generated-routed-test".to_owned(),
            UdfType::Query,
            QueryJournal::new(),
            generated_test_execution_context(),
            environment_data,
            [0; 32],
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            None,
            false,
            shadow_work_guard,
            false,
        )
        .await?;
    drop(transaction);
    Ok(())
}

#[cfg(test)]
async fn run_generated_routed_request_limit_classification(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;
    const MAXIMUM_HOST_OWNED_BYTES: usize = 64;

    let database = new_test_database(rt.clone()).await?;
    let manifest = generated_test_manifest_with_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        4 * WASM_PAGE_BYTES,
    )?;
    let mut limited_manifest = serde_json::to_value(manifest.as_ref())?;
    limited_manifest["limits"]["maxHostOwnedBytes"] = json!(MAXIMUM_HOST_OWNED_BYTES);
    let limited_manifest = Arc::new(WasmUdfExecutionManifest::parse_for_runtime(
        &serde_json::to_vec(&limited_manifest)?,
        &generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?,
    )?);
    let routed = generated_memory_test_routed_module(
        limited_manifest,
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: Some(0),
            maximum_pages: 4,
        },
        GeneratedRouteIdentity::Singleton {
            package_key: "generated-routed-request-limit".to_owned(),
        },
    )?;
    let test_hooks = StaticHermesGateTestHooks::new_generated_route(routed);
    let _test_hooks_guard = install_test_hooks(&test_hooks)?;
    let isolate_worker = crate::isolate_worker::FunctionRunnerIsolateWorker::new(
        rt.clone(),
        crate::IsolateConfig::new(
            "generated_routed_request_limit_test",
            ConcurrencyLimiter::unlimited(),
        ),
    );
    let client = IsolateClient::new(rt.clone(), 100, 2, isolate_worker)?;

    let error = execute_generated_routed_test_invocation(
        rt,
        &client,
        &database,
        vec![json!({ "value": "x".repeat(4 * 1024) })],
        None,
    )
    .await
    .expect_err("oversized generated request unexpectedly entered the guest");
    assert!(matches!(
        error.downcast_ref::<OpaqueValueError>(),
        Some(OpaqueValueError::HostOwnedBytesLimitExceeded {
            maximum_bytes: MAXIMUM_HOST_OWNED_BYTES,
        })
    ));
    assert_eq!(
        error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
        Some(&StaticHermesWasmExecutionFailure::HostOwnedBytesLimit)
    );
    let stages = test_hooks.generated_route_preflight_stages();
    assert!(stages.contains(&StaticHermesGeneratedRoutePreflightStage::InvocationStateCreated));
    assert!(!stages.contains(&StaticHermesGeneratedRoutePreflightStage::ExecutionEntered));

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[test]
fn shadow_timeout_keeps_admission_through_nested_v8_descendant() -> anyhow::Result<()> {
    let tokio_runtime = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio_runtime);
    rt.clone()
        .block_on("shadow_descendant_admission_test", async move {
            let database = new_test_database(rt.clone()).await?;
            let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
            let root_guard = Arc::new(semaphore.clone().try_acquire_owned()?);
            let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
            let environment_data = EnvironmentData {
                key_broker: KeyBroker::dev().function_runner_keybroker(),
                default_system_env_vars: BTreeMap::new(),
                file_storage: TransactionalFileStorage::new(
                    rt.clone(),
                    storage,
                    ConvexOrigin::from("http://127.0.0.1:3210".to_owned()),
                ),
                module_loader: Arc::new(UnusedGateModuleCache),
                deployment: DeploymentMetadata {
                    name: "shadow-descendant-test".to_owned(),
                    region: None,
                    class: DeploymentClass::S16,
                },
                host_secret_values: Some(BTreeMap::new()),
            };
            let (environment, _) = DatabaseUdfEnvironment::new(
                rt.clone(),
                UdfRequest {
                    path_and_args: generated_routed_test_path_and_args(vec![JsonValue::Null])?,
                    udf_type: UdfType::Query,
                    transaction: database.begin_system().await?,
                    unix_timestamp: UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                    journal: QueryJournal::new(),
                    context: generated_test_execution_context(),
                    environment_data,
                    trace_host_operations: false,
                    capture_handler_reads: false,
                    shadow_work_guard: Some(Arc::clone(&root_guard)),
                },
                0,
                "shadow-descendant-test".to_owned(),
                [0; 32],
            );
            let descendant_guard = environment
                .shadow_work_guard_for_test()
                .context("shadow guard was not retained in the V8 provider")?;
            let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
            let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
            let descendant = tokio::spawn(async move {
                started_sender
                    .send(())
                    .expect("descendant start receiver was dropped");
                release_receiver
                    .await
                    .expect("descendant release sender was dropped");
                drop(descendant_guard);
            });
            started_receiver.await?;

            // The parent shadow deadline expires while the separately scheduled V8
            // descendant is still synchronous. Dropping the parent owners must not
            // make another shadow admissible before that descendant exits.
            assert!(
                tokio::time::timeout(Duration::from_millis(1), std::future::pending::<()>())
                    .await
                    .is_err()
            );
            drop(environment);
            drop(root_guard);
            assert_eq!(semaphore.available_permits(), 0);

            release_sender
                .send(())
                .expect("descendant release receiver was dropped");
            descendant.await?;
            assert_eq!(semaphore.available_permits(), 1);
            database.shutdown().await?;
            Ok(())
        })
}

#[cfg(test)]
async fn run_cancelled_generated_routed_cpu_admission_test_invocation(
    rt: ProdRuntime,
    client: IsolateClient<ProdRuntime>,
    database: &Database<ProdRuntime>,
    cancellation: CancellationSignal,
) -> anyhow::Result<()> {
    let path_and_args = generated_routed_test_path_and_args(vec![JsonValue::Null])?;
    let route = resolve_route(UdfType::Query, &path_and_args)?
        .context("generated CPU-admission cancellation test route was not resolved")?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
    let environment_data = EnvironmentData {
        key_broker: KeyBroker::dev().function_runner_keybroker(),
        default_system_env_vars: BTreeMap::new(),
        file_storage: TransactionalFileStorage::new(
            rt.clone(),
            storage,
            ConvexOrigin::from("http://127.0.0.1:3210".to_owned()),
        ),
        module_loader: Arc::new(UnusedGateModuleCache),
        deployment: DeploymentMetadata {
            name: "generated-cpu-admission-test".to_owned(),
            region: None,
            class: DeploymentClass::S16,
        },
        host_secret_values: Some(BTreeMap::new()),
    };
    let (transaction, _outcome) = run_routed(
        rt,
        route,
        client,
        "generated-cpu-admission-cancellation-test".to_owned(),
        0,
        UdfType::Query,
        path_and_args,
        database.begin_system().await?,
        QueryJournal::new(),
        generated_test_execution_context(),
        environment_data,
        [0; 32],
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        cancellation,
        None,
        false,
        None,
        false,
    )
    .await?;
    drop(transaction);
    Ok(())
}

#[cfg(test)]
async fn replace_warm_cpu_admission_read_set_with_invalidated_environment_read(
    rt: ProdRuntime,
    database: &Database<ProdRuntime>,
    route_identity: &GeneratedRouteIdentity,
) -> anyhow::Result<()> {
    initialize_application_system_tables(database).await?;
    let name: EnvVarName = "CPU_ADMISSION_WARM_READ".parse()?;
    let initial_value: EnvVarValue = "before-invalidation".parse()?;
    let updated_value: EnvVarValue = "after-invalidation".parse()?;
    let mut setup = database.begin_system().await?;
    EnvironmentVariablesModel::new(&mut setup)
        .create(
            EnvironmentVariable::new(name.clone(), initial_value.clone()),
            &Default::default(),
        )
        .await?;
    database
        .commit_with_write_source(setup, "generated_cpu_admission_warm_read_setup")
        .await?;

    let mut capture_state = new_state(
        rt.clone(),
        database.begin_system().await?,
        None,
        Arc::new(GateMetrics::default()),
        QueryJournal::new(),
    )
    .await?;
    capture_state.provider.snoop_initialization_reads()?;
    let timeout = capture_state
        .timeout
        .as_mut()
        .context("CPU-admission warm fixture lost its initialization timeout")?;
    capture_state
        .provider
        .initialize_static_hermes(timeout)
        .await?;
    assert_eq!(
        capture_state.provider.get_environment_variable(&name)?,
        Some(initial_value.clone())
    );
    let initialization_reads = capture_state.provider.finish_initialization_reads()?;
    let context_read_set = ContextCache::capture_context_read_set(
        initialization_reads,
        capture_state.provider.tx_for_initialization()?,
    )
    .await?
    .context("CPU-admission warm fixture did not capture an environment read set")?;
    drop(capture_state);

    let mut update = database.begin_system().await?;
    let removed = EnvironmentVariablesModel::new(&mut update)
        .delete(&name)
        .await?
        .context("CPU-admission warm fixture environment variable disappeared")?;
    assert_eq!(removed.value(), &initial_value);
    EnvironmentVariablesModel::new(&mut update)
        .create(
            EnvironmentVariable::new(name, updated_value),
            &Default::default(),
        )
        .await?;
    database
        .commit_with_write_source(update, "generated_cpu_admission_warm_read_update")
        .await?;

    let mut validation_state = new_state(
        rt,
        database.begin_system().await?,
        None,
        Arc::new(GateMetrics::default()),
        QueryJournal::new(),
    )
    .await?;
    assert!(
        !ContextCache::validate_and_apply_context_read_set(
            validation_state.provider.tx_for_initialization()?,
            &context_read_set,
        )
        .await?
    );
    drop(validation_state);

    let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
    let instance = pool
        .iter_mut()
        .find_map(|instance| {
            instance
                .downcast_mut::<GeneratedReusableInstance<ProdRuntime>>()
                .filter(|instance| &instance.route_identity == route_identity)
        })
        .context("CPU-admission warm fixture disappeared from the generated pool")?;
    instance.context_read_set = Some(context_read_set);
    Ok(())
}

#[cfg(test)]
async fn run_generated_routed_cpu_admission_regression(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let database = new_test_database(rt.clone()).await?;
    let controller = Arc::clone(&route_configuration()?.generated_memory_controller);
    let generation_sha256 = "d".repeat(64);
    let registry = DeploymentRegistry::from_legacy(
        PathBuf::from("/unused/generated-routed-cpu-admission"),
        ValidatedDeploymentManifest::empty_for_test(&generation_sha256),
    );
    let generation = registry.current();
    let manifest = generated_test_manifest_with_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        4 * WASM_PAGE_BYTES,
    )?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: generation.deployment_sha256().to_owned(),
        generation_sha256: generation.generation_sha256.clone(),
        package_key: "generated-routed-cpu-admission".to_owned(),
        entry: DeploymentEntryIdentity::LegacyExport {
            runtime_module_path: "generated_test.js".to_owned(),
            export_name: "run".to_owned(),
        },
    };
    let routed = generated_memory_test_routed_module_with_generation(
        Arc::clone(&manifest),
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: Some(0),
            maximum_pages: 4,
        },
        route_identity,
        Some(Arc::clone(&generation)),
    )?;
    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &routed.route_identity,
    );
    let cpu_limiter = ConcurrencyLimiter::new(1);
    let test_hooks = StaticHermesGateTestHooks::new_generated_route(Arc::clone(&routed));
    test_hooks.set_generated_active_wasm_cpu_limiter(cpu_limiter.clone());
    let _test_hooks_guard = install_test_hooks(&test_hooks)?;
    let isolate_worker = crate::isolate_worker::FunctionRunnerIsolateWorker::new(
        rt.clone(),
        crate::IsolateConfig::new(
            "generated_routed_cpu_admission_test",
            ConcurrencyLimiter::unlimited(),
        ),
    );
    let client = IsolateClient::new(rt.clone(), 100, 2, isolate_worker)?;

    execute_generated_routed_test_invocation(
        rt.clone(),
        &client,
        &database,
        vec![JsonValue::Null],
        None,
    )
    .await?;
    let warmed = controller.snapshot_for_test(&memory_identity);
    assert_eq!(warmed.active_instances, 0);
    assert_eq!(warmed.idle_instances, 1);
    assert_eq!(warmed.function_samples, 1);
    assert_eq!(test_hooks.generated_pool_hits(), 0);

    replace_warm_cpu_admission_read_set_with_invalidated_environment_read(
        rt.clone(),
        &database,
        &routed.route_identity,
    )
    .await?;

    let held_cpu_permit = cpu_limiter
        .acquire(
            Arc::new("generated routed invalidated warm validation fixture".to_owned()),
            false,
        )
        .await;
    let preflight_start = test_hooks.generated_route_preflight_stages().len();
    let invalidated_application_shadow_permits = Arc::new(tokio::sync::Semaphore::new(1));
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        execute_generated_routed_test_invocation(
            rt.clone(),
            &client,
            &database,
            vec![JsonValue::Null],
            Some(Arc::new(
                Arc::clone(&invalidated_application_shadow_permits)
                    .try_acquire_owned()
                    .context(
                        "invalidated routed query shadow could not acquire application guard",
                    )?,
            )),
        ),
    )
    .await
    .context("invalidated routed query shadow waited for held CPU teardown")?
    .expect_err("saturated routed query shadow unexpectedly ran");
    assert!(error.is::<StaticHermesQueryShadowCapacityUnavailable>());
    assert_eq!(cpu_limiter.active_permits(), 1);
    let rejection_stages = test_hooks.generated_route_preflight_stages();
    let rejection_stages = &rejection_stages[preflight_start..];
    assert!(
        !rejection_stages.contains(&StaticHermesGeneratedRoutePreflightStage::ActiveCpuAdmitted)
    );
    assert!(!rejection_stages
        .contains(&StaticHermesGeneratedRoutePreflightStage::InvocationStateCreated));
    assert!(test_hooks
        .generated_pool_events()
        .iter()
        .any(|event| { event.event == StaticHermesGeneratedPoolEvent::ContextReadSetRejected }));
    let waiting_for_invalidated_teardown = controller.snapshot_for_test(&memory_identity);
    assert_eq!(waiting_for_invalidated_teardown.active_instances, 1);
    assert_eq!(waiting_for_invalidated_teardown.idle_instances, 0);
    assert_eq!(waiting_for_invalidated_teardown.function_samples, 1);
    assert_eq!(test_hooks.generated_pool_hits(), 1);
    assert_eq!(test_hooks.teardowns(), 0);
    assert_eq!(
        invalidated_application_shadow_permits.available_permits(),
        0
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = controller.snapshot_for_test(&memory_identity);
            if snapshot.active_instances == 0
                && invalidated_application_shadow_permits.available_permits() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("invalidated routed query-shadow cleanup did not fail closed without CPU")?;
    let invalidated_discarded = controller.snapshot_for_test(&memory_identity);
    assert_eq!(invalidated_discarded.active_instances, 0);
    assert_eq!(invalidated_discarded.idle_instances, 0);
    assert_eq!(invalidated_discarded.function_samples, 1);
    assert_eq!(test_hooks.teardowns(), 0);
    assert_eq!(
        invalidated_application_shadow_permits.available_permits(),
        1
    );
    drop(held_cpu_permit);

    execute_generated_routed_test_invocation(
        rt.clone(),
        &client,
        &database,
        vec![JsonValue::Null],
        None,
    )
    .await?;
    let finalized = controller.snapshot_for_test(&memory_identity);
    assert_eq!(finalized.active_instances, 0);
    assert_eq!(finalized.idle_instances, 1);
    assert_eq!(finalized.function_samples, 2);
    assert_eq!(test_hooks.generated_pool_hits(), 1);
    assert_eq!(test_hooks.teardowns(), 0);

    let held_cpu_permit = cpu_limiter
        .acquire(
            Arc::new("generated routed warm validation fixture".to_owned()),
            false,
        )
        .await;
    let preflight_start = test_hooks.generated_route_preflight_stages().len();
    let error = execute_generated_routed_test_invocation(
        rt.clone(),
        &client,
        &database,
        vec![JsonValue::Null],
        Some(Arc::new(
            Arc::new(tokio::sync::Semaphore::new(1))
                .try_acquire_owned()
                .context("live routed query shadow could not acquire application guard")?,
        )),
    )
    .await
    .expect_err("saturated routed query shadow unexpectedly ran");
    assert!(error.is::<StaticHermesQueryShadowCapacityUnavailable>());
    drop(held_cpu_permit);
    let rejection_stages = test_hooks.generated_route_preflight_stages();
    let rejection_stages = &rejection_stages[preflight_start..];
    assert!(rejection_stages
        .contains(&StaticHermesGeneratedRoutePreflightStage::WarmContextReadSetValidated));
    assert!(
        !rejection_stages.contains(&StaticHermesGeneratedRoutePreflightStage::ActiveCpuAdmitted)
    );
    let repooled = controller.snapshot_for_test(&memory_identity);
    assert_eq!(repooled.active_instances, 0);
    assert_eq!(repooled.idle_instances, 1);
    assert_eq!(repooled.function_samples, 2);
    assert_eq!(test_hooks.generated_pool_hits(), 2);
    assert_eq!(test_hooks.teardowns(), 0);

    let held_cpu_permit = cpu_limiter
        .acquire(
            Arc::new("generated routed cancellation fixture".to_owned()),
            false,
        )
        .await;
    let cancellation = CancellationSignal::new_for_test();
    let cancellation_for_invocation = cancellation.clone();
    let pool_hits_before_cancellation = test_hooks.generated_pool_hits();
    let cancellation_rt = rt.clone();
    let cancellation_client = client.clone();
    let cancellation_database = database.clone();
    let cancellation_invocation = tokio::spawn(async move {
        run_cancelled_generated_routed_cpu_admission_test_invocation(
            cancellation_rt,
            cancellation_client,
            &cancellation_database,
            cancellation_for_invocation,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while test_hooks.generated_pool_hits() == pool_hits_before_cancellation {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("cancelled routed invocation did not check out its warm Store")?;
    cancellation.cancel_for_test();
    let cancellation_result = tokio::time::timeout(Duration::from_secs(1), cancellation_invocation)
        .await
        .context("cancelled routed invocation waited for held CPU")?
        .context("cancelled routed invocation task failed")?;
    let error = cancellation_result.expect_err("cancelled routed invocation unexpectedly ran");
    assert!(format!("{error:#}").contains("generated Wasm"));
    let repooled_after_cancellation = controller.snapshot_for_test(&memory_identity);
    assert_eq!(repooled_after_cancellation.active_instances, 0);
    assert_eq!(repooled_after_cancellation.idle_instances, 1);
    assert_eq!(test_hooks.teardowns(), 0);
    drop(held_cpu_permit);

    {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        let instance = pool
            .iter_mut()
            .find_map(|instance| {
                instance
                    .downcast_mut::<GeneratedReusableInstance<ProdRuntime>>()
                    .filter(|instance| instance.route_identity == routed.route_identity.clone())
            })
            .context("pre-CPU cleanup fixture lost its warm runtime")?;
        instance.context_read_set = None;
    }
    let held_cpu_permit = cpu_limiter
        .acquire(
            Arc::new("generated routed corrupt warm cleanup fixture".to_owned()),
            false,
        )
        .await;
    let corrupt_application_shadow_permits = Arc::new(tokio::sync::Semaphore::new(1));
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        execute_generated_routed_test_invocation(
            rt.clone(),
            &client,
            &database,
            vec![JsonValue::Null],
            Some(Arc::new(
                Arc::clone(&corrupt_application_shadow_permits)
                    .try_acquire_owned()
                    .context("corrupt routed query shadow could not acquire application guard")?,
            )),
        ),
    )
    .await
    .context("corrupt warm rejection waited for held CPU teardown")?
    .expect_err("corrupt warm routed query shadow unexpectedly ran");
    assert!(format!("{error:#}").contains("no initialization read set"));
    let waiting_for_corrupt_cleanup = controller.snapshot_for_test(&memory_identity);
    assert_eq!(waiting_for_corrupt_cleanup.active_instances, 1);
    assert_eq!(waiting_for_corrupt_cleanup.idle_instances, 0);
    assert_eq!(waiting_for_corrupt_cleanup.function_samples, 2);
    assert_eq!(corrupt_application_shadow_permits.available_permits(), 0);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = controller.snapshot_for_test(&memory_identity);
            if snapshot.active_instances == 0
                && corrupt_application_shadow_permits.available_permits() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("corrupt warm cleanup did not fail closed without CPU")?;
    assert_eq!(test_hooks.teardowns(), 0);
    drop(held_cpu_permit);

    execute_generated_routed_test_invocation(
        rt.clone(),
        &client,
        &database,
        vec![JsonValue::Null],
        None,
    )
    .await?;
    let rewarmed = controller.snapshot_for_test(&memory_identity);
    assert_eq!(rewarmed.active_instances, 0);
    assert_eq!(rewarmed.idle_instances, 1);
    assert_eq!(rewarmed.function_samples, 3);

    generation.retired.store(true, Ordering::Release);
    let held_cpu_permit = cpu_limiter
        .acquire(
            Arc::new("generated routed retirement fixture".to_owned()),
            false,
        )
        .await;
    let retired_application_shadow_permits = Arc::new(tokio::sync::Semaphore::new(1));
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        execute_generated_routed_test_invocation(
            rt.clone(),
            &client,
            &database,
            vec![JsonValue::Null],
            Some(Arc::new(
                Arc::clone(&retired_application_shadow_permits)
                    .try_acquire_owned()
                    .context("retired routed query shadow could not acquire application guard")?,
            )),
        ),
    )
    .await
    .context("retired routed query shadow waited for held CPU teardown")?
    .expect_err("retired saturated routed query shadow unexpectedly ran");
    assert!(error.is::<StaticHermesQueryShadowCapacityUnavailable>());
    let waiting_for_teardown = controller.snapshot_for_test(&memory_identity);
    assert_eq!(waiting_for_teardown.active_instances, 1);
    assert_eq!(waiting_for_teardown.idle_instances, 0);
    assert_eq!(waiting_for_teardown.function_samples, 3);
    assert_eq!(test_hooks.teardowns(), 0);
    assert_eq!(retired_application_shadow_permits.available_permits(), 0);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = controller.snapshot_for_test(&memory_identity);
            if snapshot.active_instances == 0
                && retired_application_shadow_permits.available_permits() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("retired routed query-shadow cleanup did not fail closed without CPU")?;
    let retired_discarded = controller.snapshot_for_test(&memory_identity);
    assert_eq!(retired_discarded.active_instances, 0);
    assert_eq!(retired_discarded.idle_instances, 0);
    assert_eq!(retired_discarded.function_samples, 3);
    assert_eq!(test_hooks.teardowns(), 0);
    assert_eq!(retired_application_shadow_permits.available_permits(), 1);
    drop(held_cpu_permit);

    drop(client);
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn wait_for_generated_cpu_entry(metrics: &Arc<GateMetrics>) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while metrics.profile_marks.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("generated CPU-bound guest did not enter its run loop")
}

#[cfg(test)]
async fn run_generated_shared_epoch_isolation(rt: ProdRuntime) -> anyhow::Result<()> {
    let manifest = generated_test_manifest_with_limits_and_fuel(
        ManifestUdfKind::Query,
        Vec::new(),
        1 << 20,
        10_000_000_000_000,
    )?;
    let timeout_manifest = generated_test_manifest_with_timeout(&manifest, 50)?;
    let routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: None,
            maximum_pages: 1,
        },
        GeneratedRouteIdentity::Singleton {
            package_key: "generated-shared-epoch-isolation".to_owned(),
        },
    )?;
    anyhow::ensure!(
        Arc::ptr_eq(&routed.engine, &shared_generated_engine()?),
        "generated test module did not use the shared engine"
    );
    let database = new_test_database(rt.clone()).await?;
    let timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);

    let sibling_cancellation = CancellationSignal::new_for_test();
    let sibling_metrics = Arc::new(GateMetrics::default());
    let mut sibling_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        sibling_cancellation.clone(),
        None,
        Arc::clone(&sibling_metrics),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut sibling_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let sibling_routed = Arc::clone(&routed);
    let sibling_execution = tokio::spawn(execute_generated(
        sibling_routed,
        sibling_state,
        None,
        false,
    ));
    wait_for_generated_cpu_entry(&sibling_metrics).await?;

    let timeout_metrics = Arc::new(GateMetrics::default());
    let mut timeout_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&timeout_manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&timeout_metrics),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut timeout_state,
        Duration::from_millis(50),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let timeout_routed = generated_memory_test_routed_module(
        timeout_manifest,
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: None,
            maximum_pages: 1,
        },
        GeneratedRouteIdentity::Singleton {
            package_key: "generated-shared-epoch-timeout".to_owned(),
        },
    )?;
    let timeout_execution = tokio::spawn(execute_generated(
        timeout_routed,
        timeout_state,
        None,
        false,
    ));
    wait_for_generated_cpu_entry(&timeout_metrics).await?;
    let timeout_output = tokio::time::timeout(Duration::from_secs(5), timeout_execution)
        .await
        .context("generated CPU-bound timeout did not complete")???;
    assert!(!timeout_output.cancelled);
    assert_eq!(
        timeout_output.invocation.outcome,
        InvocationOutcome::ActiveTimeout
    );
    assert_eq!(
        timeout_output.terminal_memory_outcome,
        TerminalMemoryOutcome::Timeout
    );
    assert!(!sibling_execution.is_finished());
    let error = timeout_output
        .invocation
        .into_routed_result(timeout_output.system_error, true)
        .err()
        .context("timed-out shadow was accepted for comparison")?;
    assert_eq!(
        error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
        Some(&StaticHermesWasmExecutionFailure::ExecutionTimeout)
    );

    let cancelled_signal = CancellationSignal::new_for_test();
    let cancelled_metrics = Arc::new(GateMetrics::default());
    let mut cancelled_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        cancelled_signal.clone(),
        None,
        Arc::clone(&cancelled_metrics),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut cancelled_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let cancelled_execution = tokio::spawn(execute_generated(routed, cancelled_state, None, false));
    wait_for_generated_cpu_entry(&cancelled_metrics).await?;
    cancelled_signal.cancel_for_test();
    let cancelled_output = tokio::time::timeout(Duration::from_secs(5), cancelled_execution)
        .await
        .context("generated CPU-bound cancellation did not complete")???;
    assert!(cancelled_output.cancelled);
    assert!(!sibling_execution.is_finished());
    drop(cancelled_output.invocation.transaction);

    sibling_cancellation.cancel_for_test();
    let sibling_output = tokio::time::timeout(Duration::from_secs(5), sibling_execution)
        .await
        .context("generated CPU-bound sibling cleanup did not complete")???;
    assert!(sibling_output.cancelled);
    drop(sibling_output.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_bounded_runtime_destruction(rt: ProdRuntime) -> anyhow::Result<()> {
    let manifest = generated_test_manifest_with_limits_and_fuel(
        ManifestUdfKind::Query,
        Vec::new(),
        1 << 20,
        10_000_000_000_000,
    )?;
    let routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: true,
            loop_iterations: Some(0),
            maximum_pages: 1,
        },
        GeneratedRouteIdentity::Singleton {
            package_key: "generated-bounded-runtime-destruction".to_owned(),
        },
    )?;
    let database = new_test_database(rt.clone()).await?;
    let metrics = Arc::new(GateMetrics::default());
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
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
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        execute_generated(routed, state, None, false),
    )
    .await
    .context("generated runtime destruction exceeded its bounded allowance")?;
    let Err(error) = result else {
        anyhow::bail!("nonterminating generated runtime destruction succeeded");
    };
    anyhow::ensure!(
        format!("{error:#}").contains("generated Wasm runtime teardown failed"),
        "nonterminating generated runtime destruction returned an unexpected error: {error:#}"
    );
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 0);
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_system_error_preservation(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let manifest = generated_test_manifest_with_limits(
        ManifestUdfKind::Mutation,
        vec![json!({
            "id": 1,
            "debugName": "insertDocument",
            "operation": {
                "kind": "databaseInsert",
                "tableName": "backend_gate_documents",
            },
        })],
        4 * WASM_PAGE_BYTES,
    )?;
    let routed = generated_test_routed_module(
        Arc::clone(&manifest),
        GeneratedTestOperation::DatabaseWrite(GeneratedDatabaseWriteTestOperation {
            kind: GeneratedDatabaseWriteKind::Insert,
            operation_id: 2,
            handle_fault: GeneratedDatabaseWriteHandleFault::None,
            import_database_get: false,
        }),
    )?;
    let route_identity = routed.route_identity.clone();
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 1,
        soft_budget_bytes: 12 * WASM_PAGE_BYTES,
        hard_budget_bytes: 16 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let database = new_test_database(rt.clone()).await?;
    let metrics = Arc::new(GateMetrics::default());
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        json!({ "idvalueresult": { "marker": "system-error-test" } }),
        UdfType::Mutation,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity,
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;

    let mut output = execute_generated(routed, state, None, true).await?;
    assert_eq!(output.invocation.outcome, InvocationOutcome::SystemError);
    let error = output
        .system_error
        .take()
        .context("generated system outcome did not retain its error")?;
    assert_eq!(
        error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
        Some(&StaticHermesWasmExecutionFailure::HostAbiInvariant)
    );
    let error = format!("{error:#}");
    assert!(error.contains("generated Wasm guest invocation failed"));
    assert!(error.contains("Static Hermes host ABI invariant failed"));
    assert!(output.reusable_instance.is_none());
    assert!(output.memory_permit.is_none());
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.store_drops.load(Ordering::SeqCst), 1);
    let memory = controller.snapshot_for_test(&memory_identity);
    assert_eq!(memory.active_instances, 0);
    assert_eq!(memory.idle_instances, 0);
    assert_eq!(memory.function_samples, 1);
    assert_eq!(memory.developer_error_samples, 0);
    assert_eq!(memory.system_error_samples, 1);

    drop(output.invocation.transaction);
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn seed_generated_idle_admission_fixture(
    rt: ProdRuntime,
    database: &Database<ProdRuntime>,
    controller: &Arc<GeneratedMemoryController>,
    routed: &Arc<GeneratedRoutedModule>,
    manifest: &Arc<WasmUdfExecutionManifest>,
    route_identity: &GeneratedRouteIdentity,
    metrics: &Arc<GateMetrics>,
    cpu_limiter: &ConcurrencyLimiter,
) -> anyhow::Result<()> {
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(manifest),
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * 64 * 1024,
        }),
    )
    .await?;
    state.active_wasm_cpu_limiter = cpu_limiter.clone();
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut output = execute_generated(Arc::clone(routed), state, None, true).await?;
    assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
    drop(output.invocation.transaction);
    return_generated_routed_instance(
        output
            .reusable_instance
            .take()
            .context("admission cleanup fixture did not retain its runtime")?,
        output
            .memory_permit
            .take()
            .context("admission cleanup fixture lost its memory permit")?,
        output.terminal_memory_outcome,
    )
    .await
}

#[cfg(test)]
async fn run_generated_idle_cleanup_observes_admission_control(
    rt: ProdRuntime,
) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let idle_route = GeneratedRouteIdentity::Singleton {
        package_key: "generated-admission-cleanup-idle".to_owned(),
    };
    let demand_route = GeneratedRouteIdentity::Singleton {
        package_key: "generated-admission-cleanup-demand".to_owned(),
    };
    let manifest = generated_test_manifest_with_limits_and_fuel(
        ManifestUdfKind::Query,
        Vec::new(),
        4 * WASM_PAGE_BYTES,
        SUCCESS_FUEL,
    )?;
    let operation = GeneratedMemoryTestOperation {
        additional_pages: 1,
        destroy_infinite_loop: false,
        loop_iterations: Some(0),
        maximum_pages: 4,
    };
    let idle_routed =
        generated_memory_test_routed_module(Arc::clone(&manifest), operation, idle_route.clone())?;
    let demand_routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        operation,
        demand_route.clone(),
    )?;
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 4,
        soft_budget_bytes: 7 * WASM_PAGE_BYTES,
        hard_budget_bytes: 12 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 1,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let idle_identity = generated_memory_identity(&manifest, &idle_route);
    let demand_identity = generated_memory_identity(&manifest, &demand_route);
    let database = new_test_database(rt.clone()).await?;
    let metrics = Arc::new(GateMetrics::default());
    let cpu_limiter = ConcurrencyLimiter::new(1);
    let held_cpu_permit = cpu_limiter
        .acquire(
            Arc::new("generated admission cleanup fixture".to_owned()),
            false,
        )
        .await;

    seed_generated_idle_admission_fixture(
        rt.clone(),
        &database,
        &controller,
        &idle_routed,
        &manifest,
        &idle_route,
        &metrics,
        &cpu_limiter,
    )
    .await?;
    assert_eq!(
        controller.snapshot_for_test(&idle_identity).idle_instances,
        1
    );
    let cancellation = CancellationSignal::new_for_test();
    let mut reusable_instance: Option<GeneratedReusableInstance<ProdRuntime>> = None;
    let admission = acquire_generated_memory_permit(
        &controller,
        demand_identity.clone(),
        &demand_routed,
        true,
        &mut reusable_instance,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &cancellation,
        &metrics,
        false,
    );
    tokio::pin!(admission);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut admission)
            .await
            .is_err()
    );
    cancellation.cancel_for_test();
    let cancellation_result = tokio::time::timeout(Duration::from_secs(1), admission)
        .await
        .context("idle eviction ignored primary admission cancellation")?;
    let Err(cancellation_error) = cancellation_result else {
        anyhow::bail!("cancelled idle eviction unexpectedly admitted memory")
    };
    assert!(cancellation_error
        .to_string()
        .contains("generated Wasm admission cancelled"));
    let cancelled = controller.snapshot_for_test(&idle_identity);
    assert_eq!(cancelled.idle_instances, 0);
    assert_eq!(cancelled.evicting_instances, 0);

    seed_generated_idle_admission_fixture(
        rt.clone(),
        &database,
        &controller,
        &idle_routed,
        &manifest,
        &idle_route,
        &metrics,
        &cpu_limiter,
    )
    .await?;
    assert_eq!(
        controller.snapshot_for_test(&idle_identity).idle_instances,
        1
    );
    let cancellation = CancellationSignal::new_for_test();
    let mut reusable_instance: Option<GeneratedReusableInstance<ProdRuntime>> = None;
    let deadline_result = tokio::time::timeout(
        Duration::from_secs(1),
        acquire_generated_memory_permit(
            &controller,
            demand_identity,
            &demand_routed,
            true,
            &mut reusable_instance,
            tokio::time::Instant::now() + Duration::from_millis(50),
            &cancellation,
            &metrics,
            false,
        ),
    )
    .await
    .context("idle eviction exceeded the primary admission deadline")?;
    let Err(deadline_error) = deadline_result else {
        anyhow::bail!("expired idle eviction unexpectedly admitted memory")
    };
    assert!(deadline_error
        .to_string()
        .contains("generated Wasm memory admission timed out"));
    let timed_out = controller.snapshot_for_test(&idle_identity);
    assert_eq!(timed_out.idle_instances, 0);
    assert_eq!(timed_out.evicting_instances, 0);

    seed_generated_idle_admission_fixture(
        rt,
        &database,
        &controller,
        &idle_routed,
        &manifest,
        &idle_route,
        &metrics,
        &cpu_limiter,
    )
    .await?;
    controller.set_pressure_for_test(BackendPressure::Active {
        headroom_bytes: WASM_PAGE_BYTES,
    });
    let maintenance_error = tokio::time::timeout(
        Duration::from_secs(1),
        evict_generated_idle_instances::<ProdRuntime>(
            &controller,
            IdleEvictionTrigger::Maintenance,
            IdleEvictionLimit::maintenance(),
        ),
    )
    .await
    .context("idle teardown stalled the maintenance cleanup boundary")?
    .expect_err("saturated maintenance teardown unexpectedly acquired CPU");
    assert!(matches!(maintenance_error, IdleEvictionError::Deadline));
    let maintained = controller.snapshot_for_test(&idle_identity);
    assert_eq!(maintained.idle_instances, 0);
    assert_eq!(maintained.evicting_instances, 0);

    drop(held_cpu_permit);
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_memory_pool_acceptance(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1"
            .to_owned(),
        generation_sha256: "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1"
            .to_owned(),
        package_key: "generated-memory-pool-acceptance".to_owned(),
        entry: DeploymentEntryIdentity::LegacyExport {
            runtime_module_path: "functions/generated_test.js".to_owned(),
            export_name: "run".to_owned(),
        },
    };
    let manifest = generated_test_manifest_with_limits_and_fuel(
        ManifestUdfKind::Query,
        Vec::new(),
        4 * WASM_PAGE_BYTES,
        SUCCESS_FUEL,
    )?;
    assert_eq!(manifest.limits().execution_fuel(), SUCCESS_FUEL);
    let memory_operation = GeneratedMemoryTestOperation {
        additional_pages: 1,
        destroy_infinite_loop: false,
        loop_iterations: Some(0),
        maximum_pages: 4,
    };
    let routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        memory_operation,
        route_identity.clone(),
    )?;
    let source = manifest.source();
    let GeneratedRouteIdentity::DeploymentExport {
        entry:
            DeploymentEntryIdentity::LegacyExport {
                runtime_module_path,
                export_name,
            },
        ..
    } = &route_identity
    else {
        unreachable!("memory pool acceptance route must be a deployment export")
    };
    assert_eq!(runtime_module_path.as_str(), source.runtime_module_path());
    assert_eq!(export_name.as_str(), source.export_name());
    assert!(!GENERATED_ROUTED_MODULES
        .lock()
        .contains_key(&route_identity));
    assert!(!GENERATED_ROUTED_INSTANCE_POOL
        .lock()
        .iter()
        .any(|instance| {
            instance
                .downcast_ref::<GeneratedReusableInstance<ProdRuntime>>()
                .is_some_and(|instance| instance.route_identity == route_identity)
        }));

    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 4,
        soft_budget_bytes: 12 * WASM_PAGE_BYTES,
        hard_budget_bytes: 16 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let database = new_test_database(rt.clone()).await?;
    let metrics = Arc::new(GateMetrics::default());
    let timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);

    let mut first_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut first_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut first = execute_generated(Arc::clone(&routed), first_state, None, true).await?;
    assert!(!first.cancelled);
    assert_eq!(first.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(first.invocation.function_result, Some(JsonValue::Null));
    let first_active = controller.snapshot_for_test(&memory_identity);
    assert_eq!(first_active.active_instances, 1);
    assert_eq!(first_active.idle_instances, 0);
    assert_eq!(first_active.active_checkout_baseline_bytes, 0);
    assert_eq!(first_active.active_guest_bytes, 2 * WASM_PAGE_BYTES);
    assert!(first_active.active_host_bytes > 0);
    assert_eq!(
        first_active.active_liability_bytes,
        first_active
            .active_guest_bytes
            .saturating_add(first_active.active_host_bytes)
    );
    let mut reusable_instance = first
        .reusable_instance
        .take()
        .context("successful generated memory fixture was not reusable")?;
    let first_permit = first
        .memory_permit
        .take()
        .context("successful generated memory fixture lost its permit")?;
    let memory_slot_id = first_permit.slot_id();
    drop(first.invocation.transaction);
    first_permit.finish(first.terminal_memory_outcome, true);
    assert_eq!(metrics.store_drops.load(Ordering::SeqCst), 0);

    let first_idle = controller.snapshot_for_test(&memory_identity);
    assert_eq!(first_idle.active_instances, 0);
    assert_eq!(first_idle.idle_instances, 1);
    assert_eq!(first_idle.function_samples, 1);
    let completed_state = reusable_instance
        .store
        .data_mut()
        .generated
        .take()
        .context("completed generated Store lost its invocation state")?;
    assert!(completed_state.memory_permit.is_none());
    assert_eq!(completed_state.values.live_handle_count(), 0);
    let retained_values = completed_state.values;
    let retained_accounting = retained_values.accounting();
    assert_eq!(retained_accounting.current_bytes, 0);
    assert_eq!(
        first_idle.retained_baseline_bytes,
        2 * WASM_PAGE_BYTES + retained_accounting.retained_bytes
    );
    reusable_instance
        .store
        .set_fuel(0)
        .map_err(wasmtime_anyhow)?;

    let mut second_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: route_identity.clone(),
            existing_slot: Some(memory_slot_id),
            values: Some(retained_values),
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    let second_checkout = controller.snapshot_for_test(&memory_identity);
    assert_eq!(second_checkout.active_instances, 1);
    assert_eq!(
        second_checkout.active_checkout_baseline_bytes,
        first_idle.retained_baseline_bytes
    );
    assert_eq!(second_checkout.active_guest_bytes, 2 * WASM_PAGE_BYTES);
    arm_generated_timeout(
        rt.clone(),
        &mut second_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut second = execute_generated(
        Arc::clone(&routed),
        second_state,
        Some(reusable_instance),
        true,
    )
    .await?;
    assert!(!second.cancelled);
    assert_eq!(second.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(second.invocation.function_result, Some(JsonValue::Null));
    assert!(
        second
            .reusable_instance
            .as_ref()
            .context("reused generated memory fixture was not retained")?
            .store
            .get_fuel()
            .map_err(wasmtime_anyhow)?
            < manifest.limits().execution_fuel()
    );
    let second_active = controller.snapshot_for_test(&memory_identity);
    assert_eq!(second_active.active_instances, 1);
    assert_eq!(second_active.active_guest_bytes, 3 * WASM_PAGE_BYTES);
    assert_eq!(
        second_active.active_checkout_baseline_bytes,
        first_idle.retained_baseline_bytes
    );
    let reusable_instance = second
        .reusable_instance
        .take()
        .context("reused generated memory fixture was not retained")?;
    let completed_values = &reusable_instance
        .store
        .data()
        .generated
        .as_ref()
        .context("reused generated Store lost its invocation state")?
        .values;
    assert_eq!(completed_values.live_handle_count(), 0);
    assert_eq!(completed_values.accounting().current_bytes, 0);
    assert_eq!(
        second_active.active_host_bytes,
        completed_values.accounting().retained_bytes
    );
    let second_permit = second
        .memory_permit
        .take()
        .context("reused generated memory fixture lost its permit")?;
    drop(second.invocation.transaction);

    {
        let mut cache = GENERATED_ROUTED_MODULES.lock();
        let charge = cache
            .reserve(&controller, routed.fixed_module_bytes)
            .map_err(module_memory_admission_error)?;
        cache.insert(Arc::clone(&routed), charge);
    }
    return_generated_routed_instance(
        reusable_instance,
        second_permit,
        second.terminal_memory_outcome,
    )
    .await?;
    let second_idle = controller.snapshot_for_test(&memory_identity);
    assert_eq!(second_idle.active_instances, 0);
    assert_eq!(second_idle.idle_instances, 1);
    assert_eq!(second_idle.function_samples, 2);
    assert_eq!(
        second_idle.retained_baseline_bytes,
        3 * WASM_PAGE_BYTES + second_active.active_host_bytes
    );
    assert_eq!(second_idle.fixed_module_bytes, routed.fixed_module_bytes);
    let removed_routed = GENERATED_ROUTED_MODULES
        .lock()
        .remove(&route_identity, "module_cache_evicted_test")
        .context("memory acceptance routed module disappeared from its cache")?;
    assert!(Arc::ptr_eq(&removed_routed, &routed));
    drop(removed_routed);
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        routed.fixed_module_bytes
    );
    let fixed_module_bytes = routed.fixed_module_bytes;
    drop(routed);
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        fixed_module_bytes,
        "idle pooled instance did not retain its routed module charge"
    );

    let pressure_teardowns = metrics.teardowns.load(Ordering::SeqCst);
    let pressure_store_drops = metrics.store_drops.load(Ordering::SeqCst);
    controller.set_pressure_for_test(BackendPressure::Active {
        headroom_bytes: WASM_PAGE_BYTES,
    });
    evict_generated_idle_instances::<ProdRuntime>(
        &controller,
        IdleEvictionTrigger::Maintenance,
        IdleEvictionLimit {
            deadline: tokio::time::Instant::now() + Duration::from_secs(5),
            cancellation: None,
        },
    )
    .await?;
    let pressure_evicted = controller.snapshot_for_test(&memory_identity);
    assert_eq!(pressure_evicted.active_instances, 0);
    assert_eq!(pressure_evicted.idle_instances, 0);
    assert_eq!(pressure_evicted.evicting_instances, 0);
    assert_eq!(pressure_evicted.retained_baseline_bytes, 0);
    assert_eq!(
        pressure_evicted.fixed_module_bytes, 0,
        "destroyed idle pooled instance retained its routed module charge"
    );
    assert_eq!(
        metrics.teardowns.load(Ordering::SeqCst),
        pressure_teardowns + 1
    );
    assert_eq!(
        metrics.store_drops.load(Ordering::SeqCst),
        pressure_store_drops + 1
    );

    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        memory_operation,
        route_identity.clone(),
    )?;
    let mut aged_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut aged_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut aged = execute_generated(Arc::clone(&routed), aged_state, None, true).await?;
    assert_eq!(aged.invocation.outcome, InvocationOutcome::Success);
    let aged_instance = aged
        .reusable_instance
        .take()
        .context("idle-age fixture was not reusable")?;
    let aged_permit = aged
        .memory_permit
        .take()
        .context("idle-age fixture lost its memory permit")?;
    drop(aged.invocation.transaction);
    return_generated_routed_instance(aged_instance, aged_permit, aged.terminal_memory_outcome)
        .await?;
    {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        assert_eq!(
            pool.iter()
                .filter(|instance| {
                    instance
                        .downcast_ref::<GeneratedReusableInstance<ProdRuntime>>()
                        .is_some_and(|instance| instance.route_identity == route_identity)
                })
                .count(),
            1
        );
        let instance = pool
            .iter_mut()
            .find_map(|instance| {
                instance
                    .downcast_mut::<GeneratedReusableInstance<ProdRuntime>>()
                    .filter(|instance| instance.route_identity == route_identity)
            })
            .context("idle-age fixture disappeared from the generated pool")?;
        instance.idle_since = Instant::now()
            .checked_sub(Duration::from_secs(2))
            .context("failed to construct an old generated idle timestamp")?;
    }
    let age_teardowns = metrics.teardowns.load(Ordering::SeqCst);
    let age_store_drops = metrics.store_drops.load(Ordering::SeqCst);
    evict_generated_idle_instances::<ProdRuntime>(
        &controller,
        IdleEvictionTrigger::Maintenance,
        IdleEvictionLimit {
            deadline: tokio::time::Instant::now() + Duration::from_secs(5),
            cancellation: None,
        },
    )
    .await?;
    let age_evicted = controller.snapshot_for_test(&memory_identity);
    assert_eq!(age_evicted.active_instances, 0);
    assert_eq!(age_evicted.idle_instances, 0);
    assert_eq!(age_evicted.evicting_instances, 0);
    assert_eq!(age_evicted.retained_baseline_bytes, 0);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), age_teardowns + 1);
    assert_eq!(
        metrics.store_drops.load(Ordering::SeqCst),
        age_store_drops + 1
    );
    assert!(!GENERATED_ROUTED_MODULES
        .lock()
        .contains_key(&route_identity));

    let budget_idle_route = GeneratedRouteIdentity::Singleton {
        package_key: "generated-budget-idle".to_owned(),
    };
    let budget_demand_route = GeneratedRouteIdentity::Singleton {
        package_key: "generated-budget-demand".to_owned(),
    };
    let budget_idle_routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        memory_operation,
        budget_idle_route.clone(),
    )?;
    let budget_demand_routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        memory_operation,
        budget_demand_route.clone(),
    )?;
    let budget_controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 4,
        soft_budget_bytes: 7 * WASM_PAGE_BYTES,
        hard_budget_bytes: 12 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 1,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    budget_controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let budget_idle_identity = generated_memory_identity(&manifest, &budget_idle_route);
    let budget_demand_identity = generated_memory_identity(&manifest, &budget_demand_route);
    let mut budget_idle_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&budget_controller),
            route_identity: budget_idle_route.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut budget_idle_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut budget_idle = execute_generated(
        Arc::clone(&budget_idle_routed),
        budget_idle_state,
        None,
        true,
    )
    .await?;
    assert_eq!(budget_idle.invocation.outcome, InvocationOutcome::Success);
    let budget_idle_instance = budget_idle
        .reusable_instance
        .take()
        .context("budget fixture did not retain its idle runtime")?;
    let budget_idle_permit = budget_idle
        .memory_permit
        .take()
        .context("budget fixture lost its idle memory permit")?;
    drop(budget_idle.invocation.transaction);
    return_generated_routed_instance(
        budget_idle_instance,
        budget_idle_permit,
        budget_idle.terminal_memory_outcome,
    )
    .await?;
    let budget_blocked = budget_controller.snapshot_for_test(&budget_idle_identity);
    assert_eq!(budget_blocked.idle_instances, 1);
    assert!(matches!(
        budget_controller.try_admit(budget_demand_identity.clone(), None),
        Err(TryAdmissionError::Wait(AdmissionWaitReason::SoftBudget))
    ));

    let budget_teardowns = metrics.teardowns.load(Ordering::SeqCst);
    let mut budget_reusable: Option<GeneratedReusableInstance<ProdRuntime>> = None;
    let budget_cancellation = CancellationSignal::new_for_test();
    let budget_permit = acquire_generated_memory_permit(
        &budget_controller,
        budget_demand_identity.clone(),
        &budget_demand_routed,
        true,
        &mut budget_reusable,
        tokio::time::Instant::now() + Duration::from_secs(2),
        &budget_cancellation,
        &metrics,
        false,
    )
    .await?;
    assert!(
        budget_reusable.is_none(),
        "budget admission reused a route-mismatched runtime"
    );
    assert_eq!(
        metrics.teardowns.load(Ordering::SeqCst),
        budget_teardowns + 1,
        "budget admission did not destroy the retained idle runtime"
    );
    assert!(
        !GENERATED_ROUTED_INSTANCE_POOL
            .lock()
            .iter()
            .any(|instance| {
                instance
                    .downcast_ref::<GeneratedReusableInstance<ProdRuntime>>()
                    .is_some_and(|instance| instance.route_identity == budget_idle_route)
            }),
        "budget admission left the route-mismatched idle runtime in the pool"
    );
    let budget_admitted = budget_controller.snapshot_for_test(&budget_demand_identity);
    assert_eq!(budget_admitted.active_instances, 1);
    assert_eq!(budget_admitted.idle_instances, 0);
    assert_eq!(budget_admitted.evicting_instances, 0);
    budget_permit.finish(TerminalMemoryOutcome::Success, false);

    let denied_teardowns = metrics.teardowns.load(Ordering::SeqCst);
    let denied_store_drops = metrics.store_drops.load(Ordering::SeqCst);
    let mut denied_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut denied_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let denied = execute_generated(Arc::clone(&routed), denied_state, None, true).await?;
    assert!(!denied.cancelled);
    assert_eq!(denied.invocation.outcome, InvocationOutcome::SystemError);
    assert!(denied.reusable_instance.is_none());
    assert!(denied.memory_permit.is_none());
    drop(denied.invocation.transaction);
    let denied_snapshot = controller.snapshot_for_test(&memory_identity);
    assert_eq!(denied_snapshot.active_instances, 0);
    assert_eq!(denied_snapshot.idle_instances, 0);
    assert_eq!(denied_snapshot.evicting_instances, 0);
    assert_eq!(denied_snapshot.function_samples, 4);
    assert_eq!(denied_snapshot.memory_limit_samples, 1);
    assert_eq!(
        metrics.teardowns.load(Ordering::SeqCst),
        denied_teardowns + 1
    );
    assert_eq!(
        metrics.store_drops.load(Ordering::SeqCst),
        denied_store_drops + 1
    );

    // The fixture's two-page retained runtime rounds to a three-page learned
    // forecast. This budget admits two active slots, rejects a third new slot,
    // and still admits a returned physical slot after that forecast is learned.
    let handoff_controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 4,
        soft_budget_bytes: 12 * WASM_PAGE_BYTES,
        hard_budget_bytes: 16 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: 3 * WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    handoff_controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let handoff_identity = generated_memory_identity(&manifest, &route_identity);
    let mut held = Vec::new();
    for _ in 0..2 {
        let mut state = generated_test_state(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            JsonValue::Null,
            UdfType::Query,
            timestamp,
            CancellationSignal::new_for_test(),
            None,
            Arc::clone(&metrics),
            Some(GeneratedTestMemorySetup {
                controller: Arc::clone(&handoff_controller),
                route_identity: route_identity.clone(),
                existing_slot: None,
                values: None,
                maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
            }),
        )
        .await?;
        arm_generated_timeout(
            rt.clone(),
            &mut state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let output = execute_generated(Arc::clone(&routed), state, None, true).await?;
        assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
        assert!(output.reusable_instance.is_some());
        assert!(output.memory_permit.is_some());
        held.push(output);
    }
    let held_snapshot = handoff_controller.snapshot_for_test(&handoff_identity);
    assert_eq!(held_snapshot.active_instances, 2);
    assert_eq!(held_snapshot.idle_instances, 0);

    let handoff_teardowns = metrics.teardowns.load(Ordering::SeqCst);
    let queued_cancellation = CancellationSignal::new_for_test();
    let mut queued_instance: Option<GeneratedReusableInstance<ProdRuntime>> = None;
    let queued_permit = {
        let queued = acquire_generated_memory_permit(
            &handoff_controller,
            handoff_identity.clone(),
            &routed,
            true,
            &mut queued_instance,
            tokio::time::Instant::now() + Duration::from_secs(2),
            &queued_cancellation,
            &metrics,
            false,
        );
        tokio::pin!(queued);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut queued)
                .await
                .is_err(),
            "queued pool miss acquired a third memory slot"
        );

        let mut returned = held.remove(0);
        let returned_slot = returned
            .memory_permit
            .as_ref()
            .context("handoff fixture lost its first permit")?
            .slot_id();
        drop(returned.invocation.transaction);
        return_generated_routed_instance(
            returned
                .reusable_instance
                .take()
                .context("handoff fixture lost its first reusable instance")?,
            returned
                .memory_permit
                .take()
                .context("handoff fixture lost its first memory permit")?,
            returned.terminal_memory_outcome,
        )
        .await?;

        let permit = tokio::time::timeout(Duration::from_secs(1), &mut queued)
            .await
            .context("queued generated pool miss did not adopt the returned slot")??;
        assert_eq!(permit.slot_id(), returned_slot);
        permit
    };
    assert_eq!(
        queued_instance
            .as_ref()
            .context("queued admission did not check out the returned physical instance")?
            .memory_slot_id,
        queued_permit.slot_id()
    );
    let handed_off = handoff_controller.snapshot_for_test(&handoff_identity);
    assert_eq!(handed_off.active_instances, 2);
    assert_eq!(handed_off.idle_instances, 0);
    assert_eq!(handed_off.evicting_instances, 0);
    assert_eq!(
        metrics.teardowns.load(Ordering::SeqCst),
        handoff_teardowns,
        "queued handoff destroyed an idle runtime instead of reusing it"
    );

    let queued_instance = queued_instance
        .take()
        .context("queued handoff instance disappeared during cleanup")?;
    discard_generated_instance(queued_instance).await?;
    queued_permit.finish(TerminalMemoryOutcome::Success, false);
    let mut second = held
        .pop()
        .context("handoff fixture lost its second active output")?;
    drop(second.invocation.transaction);
    discard_generated_instance(
        second
            .reusable_instance
            .take()
            .context("handoff fixture lost its second reusable instance")?,
    )
    .await?;
    second
        .memory_permit
        .take()
        .context("handoff fixture lost its second memory permit")?
        .finish(second.terminal_memory_outcome, false);
    let handoff_clean = handoff_controller.snapshot_for_test(&handoff_identity);
    assert_eq!(handoff_clean.active_instances, 0);
    assert_eq!(handoff_clean.idle_instances, 0);
    assert_eq!(handoff_clean.evicting_instances, 0);

    // At the physical slot ceiling, concurrent callers must serialize through
    // the returned route-matched runtime. Evicting before checkout turns every
    // first caller in a burst into a needless destroy-and-recreate cycle.
    let ceiling_controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 1,
        soft_budget_bytes: 12 * WASM_PAGE_BYTES,
        hard_budget_bytes: 16 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 1,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    ceiling_controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let ceiling_identity = generated_memory_identity(&manifest, &route_identity);
    let ceiling_hits_before = metrics.generated_pool_hits.load(Ordering::SeqCst);
    let ceiling_misses_before = metrics.generated_pool_misses.load(Ordering::SeqCst);
    let mut cold_instance: Option<GeneratedReusableInstance<ProdRuntime>> = None;
    let cold_cancellation = CancellationSignal::new_for_test();
    let cold_permit = acquire_generated_routed_memory_permit(
        &ceiling_controller,
        ceiling_identity.clone(),
        &routed,
        true,
        &mut cold_instance,
        tokio::time::Instant::now() + Duration::from_secs(2),
        &cold_cancellation,
        &metrics,
        false,
    )
    .await?;
    assert!(cold_instance.is_none());
    assert_eq!(
        metrics.generated_pool_misses.load(Ordering::SeqCst),
        ceiling_misses_before + 1,
        "initial slot admission did not record exactly one generated pool miss"
    );
    cold_permit.finish(TerminalMemoryOutcome::Success, false);

    let mut ceiling_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&ceiling_controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut ceiling_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut ceiling_seed =
        execute_generated(Arc::clone(&routed), ceiling_state, None, true).await?;
    assert_eq!(ceiling_seed.invocation.outcome, InvocationOutcome::Success);
    drop(ceiling_seed.invocation.transaction);
    return_generated_routed_instance(
        ceiling_seed
            .reusable_instance
            .take()
            .context("ceiling reuse fixture did not retain its runtime")?,
        ceiling_seed
            .memory_permit
            .take()
            .context("ceiling reuse fixture lost its memory permit")?,
        ceiling_seed.terminal_memory_outcome,
    )
    .await?;
    let ceiling_seeded = ceiling_controller.snapshot_for_test(&ceiling_identity);
    assert_eq!(ceiling_seeded.active_instances, 0);
    assert_eq!(ceiling_seeded.idle_instances, 1);

    const CONCURRENT_CEILING_ADMISSIONS: usize = 8;
    let ceiling_teardowns = metrics.teardowns.load(Ordering::SeqCst);
    let start = Arc::new(tokio::sync::Barrier::new(CONCURRENT_CEILING_ADMISSIONS));
    let mut concurrent_admissions = Vec::new();
    for _ in 0..CONCURRENT_CEILING_ADMISSIONS {
        let controller = Arc::clone(&ceiling_controller);
        let identity = ceiling_identity.clone();
        let routed = Arc::clone(&routed);
        let start = Arc::clone(&start);
        let metrics = Arc::clone(&metrics);
        concurrent_admissions.push(tokio::spawn(async move {
            start.wait().await;
            let cancellation = CancellationSignal::new_for_test();
            let mut reusable_instance: Option<GeneratedReusableInstance<ProdRuntime>> = None;
            let permit = acquire_generated_routed_memory_permit(
                &controller,
                identity,
                &routed,
                true,
                &mut reusable_instance,
                tokio::time::Instant::now() + Duration::from_secs(2),
                &cancellation,
                &metrics,
                false,
            )
            .await?;
            tokio::task::yield_now().await;
            return_generated_routed_instance(
                reusable_instance
                    .take()
                    .context("ceiling admission did not reuse the returned runtime")?,
                permit,
                TerminalMemoryOutcome::Success,
            )
            .await
        }));
    }
    for admission in concurrent_admissions {
        admission
            .await
            .context("concurrent ceiling admission task failed")??;
    }
    assert_eq!(
        metrics.teardowns.load(Ordering::SeqCst),
        ceiling_teardowns,
        "concurrent ceiling reuse destroyed a route-matched idle runtime"
    );
    assert_eq!(
        metrics.generated_pool_misses.load(Ordering::SeqCst),
        ceiling_misses_before + 1,
        "busy ceiling waiters recorded false generated pool misses"
    );
    assert_eq!(
        metrics.generated_pool_hits.load(Ordering::SeqCst),
        ceiling_hits_before + CONCURRENT_CEILING_ADMISSIONS,
        "concurrent ceiling admissions did not all reuse the warmed runtime"
    );
    let ceiling_reused = ceiling_controller.snapshot_for_test(&ceiling_identity);
    assert_eq!(ceiling_reused.active_instances, 0);
    assert_eq!(ceiling_reused.idle_instances, 1);
    assert_eq!(ceiling_reused.evicting_instances, 0);

    // A background shadow must not let idle stores for unrelated packages
    // monopolize a bounded pool. It may destroy one idle mismatch within its
    // existing deadline, but active or genuinely unavailable capacity still
    // returns the immediate capacity outcome.
    let alternate_route_identity = GeneratedRouteIdentity::Singleton {
        package_key: "generated-ceiling-alternate".to_owned(),
    };
    let alternate_routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        memory_operation,
        alternate_route_identity.clone(),
    )?;
    let alternate_identity = generated_memory_identity(&manifest, &alternate_route_identity);
    let immediate_teardowns = metrics.teardowns.load(Ordering::SeqCst);
    let mut immediate_instance: Option<GeneratedReusableInstance<ProdRuntime>> = None;
    let immediate_cancellation = CancellationSignal::new_for_test();
    let immediate_permit = acquire_generated_routed_memory_permit(
        &ceiling_controller,
        alternate_identity.clone(),
        &alternate_routed,
        true,
        &mut immediate_instance,
        tokio::time::Instant::now() + Duration::from_secs(2),
        &immediate_cancellation,
        &metrics,
        true,
    )
    .await?;
    assert!(immediate_instance.is_none());
    assert_eq!(
        metrics.teardowns.load(Ordering::SeqCst),
        immediate_teardowns + 1,
        "immediate admission did not evict the idle mismatched runtime"
    );
    let immediate_admitted = ceiling_controller.snapshot_for_test(&alternate_identity);
    assert_eq!(immediate_admitted.active_instances, 1);
    assert_eq!(immediate_admitted.idle_instances, 0);
    assert_eq!(immediate_admitted.evicting_instances, 0);
    immediate_permit.finish_unstarted(false);
    let ceiling_clean = ceiling_controller.snapshot_for_test(&alternate_identity);
    assert_eq!(ceiling_clean.active_instances, 0);
    assert_eq!(ceiling_clean.idle_instances, 0);
    assert_eq!(ceiling_clean.evicting_instances, 0);

    let admission_controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 4,
        soft_budget_bytes: 3 * WASM_PAGE_BYTES,
        hard_budget_bytes: 6 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: 0,
        cold_peak_growth_bytes: 2 * WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    let held_permit = admission_controller
        .admit(memory_identity.clone(), None)
        .await
        .map_err(|_| anyhow::anyhow!("held acceptance permit was rejected"))?;
    let held_snapshot = admission_controller.snapshot_for_test(&memory_identity);
    assert_eq!(held_snapshot.active_instances, 1);
    assert_eq!(held_snapshot.idle_instances, 0);

    let cancelled_signal = CancellationSignal::new_for_test();
    cancelled_signal.cancel_for_test();
    let mut blocked_reusable: Option<GeneratedReusableInstance<ProdRuntime>> = None;
    assert!(acquire_generated_memory_permit(
        &admission_controller,
        memory_identity.clone(),
        &routed,
        true,
        &mut blocked_reusable,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &cancelled_signal,
        &metrics,
        false,
    )
    .await
    .is_err());
    let timeout_signal = CancellationSignal::new_for_test();
    assert!(acquire_generated_memory_permit(
        &admission_controller,
        memory_identity.clone(),
        &routed,
        true,
        &mut blocked_reusable,
        tokio::time::Instant::now(),
        &timeout_signal,
        &metrics,
        false,
    )
    .await
    .is_err());
    let blocked_snapshot = admission_controller.snapshot_for_test(&memory_identity);
    assert_eq!(blocked_snapshot.active_instances, 1);
    assert_eq!(blocked_snapshot.idle_instances, 0);
    assert_eq!(blocked_snapshot.evicting_instances, 0);
    held_permit.finish(TerminalMemoryOutcome::Success, false);
    let released_snapshot = admission_controller.snapshot_for_test(&memory_identity);
    assert_eq!(released_snapshot.active_instances, 0);
    assert_eq!(released_snapshot.idle_instances, 0);
    assert_eq!(released_snapshot.evicting_instances, 0);

    // Test cleanup removes an idle physical Store. Its memory-slot ledger must
    // leave the controller with it so later fixtures do not accumulate ghost
    // idle capacity and spuriously evict their own returned runtime.
    let cleanup_route_identity = GeneratedRouteIdentity::Singleton {
        package_key: "generated-memory-pool-test-cleanup".to_owned(),
    };
    let cleanup_memory_identity = generated_memory_identity(&manifest, &cleanup_route_identity);
    let cleanup_routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        memory_operation,
        cleanup_route_identity.clone(),
    )?;
    let mut cleanup_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        JsonValue::Null,
        UdfType::Query,
        timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: cleanup_route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut cleanup_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut cleanup_output =
        execute_generated(Arc::clone(&cleanup_routed), cleanup_state, None, true).await?;
    assert_eq!(
        cleanup_output.invocation.outcome,
        InvocationOutcome::Success
    );
    drop(cleanup_output.invocation.transaction);
    return_generated_routed_instance(
        cleanup_output
            .reusable_instance
            .take()
            .context("test-cleanup fixture did not retain its runtime")?,
        cleanup_output
            .memory_permit
            .take()
            .context("test-cleanup fixture lost its memory permit")?,
        cleanup_output.terminal_memory_outcome,
    )
    .await?;
    let cleanup_idle = controller.snapshot_for_test(&cleanup_memory_identity);
    assert_eq!(cleanup_idle.active_instances, 0);
    assert_eq!(cleanup_idle.idle_instances, 1);
    assert_eq!(
        cleanup_static_hermes_test_instances::<ProdRuntime>().await?,
        1
    );
    let cleanup_released = controller.snapshot_for_test(&cleanup_memory_identity);
    assert_eq!(cleanup_released.active_instances, 0);
    assert_eq!(cleanup_released.idle_instances, 0);
    assert_eq!(cleanup_released.evicting_instances, 0);
    assert_eq!(cleanup_released.retained_baseline_bytes, 0);
    assert!(metrics.generated_pool_events.lock().iter().any(|event| {
        matches!(
            event.event,
            StaticHermesGeneratedPoolEvent::Evicted {
                trigger: StaticHermesGeneratedPoolEvictionTrigger::TestCleanup,
                reason: StaticHermesGeneratedPoolEvictionReason::TestCleanup,
            }
        )
    }));

    assert!(!GENERATED_ROUTED_INSTANCE_POOL
        .lock()
        .iter()
        .any(|instance| {
            instance
                .downcast_ref::<GeneratedReusableInstance<ProdRuntime>>()
                .is_some_and(|instance| instance.route_identity == route_identity)
        }));
    assert!(!GENERATED_ROUTED_MODULES
        .lock()
        .contains_key(&route_identity));
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_function_started_after_memory_admission_test() -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let route_identity = GeneratedRouteIdentity::Singleton {
        package_key: "generated-function-started-admission".to_owned(),
    };
    let manifest = generated_test_manifest_with_limits_and_fuel(
        ManifestUdfKind::Query,
        Vec::new(),
        4 * WASM_PAGE_BYTES,
        SUCCESS_FUEL,
    )?;
    let routed = generated_memory_test_routed_module(
        Arc::clone(&manifest),
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: Some(0),
            maximum_pages: 4,
        },
        route_identity.clone(),
    )?;
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 1,
        soft_budget_bytes: 12 * WASM_PAGE_BYTES,
        hard_budget_bytes: 16 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let identity = generated_memory_identity(&manifest, &route_identity);
    let held_permit = controller
        .admit(identity.clone(), None)
        .await
        .map_err(|_| anyhow::anyhow!("failed to hold generated route admission"))?;
    let cpu_limiter = ConcurrencyLimiter::new(1);
    let held_cpu_permit = cpu_limiter
        .acquire(
            Arc::new("Static Hermes Wasm function-start test".to_owned()),
            false,
        )
        .await;
    let (started_sender, mut started_receiver) = oneshot::channel();
    let admission = tokio::spawn({
        let controller = Arc::clone(&controller);
        let cpu_limiter = cpu_limiter.clone();
        let identity = identity.clone();
        let routed = Arc::clone(&routed);
        async move {
            let cancellation = CancellationSignal::new_for_test();
            let metrics = GateMetrics::default();
            let mut reusable_instance: Option<GeneratedReusableInstance<ProdRuntime>> = None;
            let permit = acquire_generated_routed_memory_permit(
                &controller,
                identity,
                &routed,
                true,
                &mut reusable_instance,
                tokio::time::Instant::now() + Duration::from_secs(5),
                &cancellation,
                &metrics,
                false,
            )
            .await?;
            anyhow::ensure!(
                reusable_instance.is_none(),
                "function-start admission unexpectedly checked out a reusable runtime"
            );
            let cpu_permit = acquire_active_wasm_cpu_permit(
                &cpu_limiter,
                &cancellation,
                false,
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await?;
            _ = started_sender.send(());
            Ok::<_, anyhow::Error>((permit, cpu_permit))
        }
    });

    tokio::task::yield_now().await;
    assert!(matches!(
        started_receiver.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(
        !admission.is_finished(),
        "generated route completed before memory admission became available"
    );

    held_permit.finish(TerminalMemoryOutcome::Success, false);
    tokio::task::yield_now().await;
    assert!(matches!(
        started_receiver.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(
        !admission.is_finished(),
        "generated route completed before active CPU capacity was available"
    );

    drop(held_cpu_permit);
    tokio::time::timeout(Duration::from_secs(5), &mut started_receiver)
        .await
        .context("generated route did not notify function start after capacity admission")?
        .context("generated route closed function-start notification without success")?;
    let (memory_permit, cpu_permit) = tokio::time::timeout(Duration::from_secs(5), admission)
        .await
        .context("generated route admission did not finish after capacity was released")???;
    drop(cpu_permit);
    memory_permit.finish(TerminalMemoryOutcome::Success, false);
    let snapshot = controller.snapshot_for_test(&identity);
    assert_eq!(snapshot.active_instances, 0);
    assert_eq!(snapshot.idle_instances, 0);
    Ok(())
}

#[cfg(test)]
fn generated_lifecycle_test_directory() -> anyhow::Result<tempfile::TempDir> {
    let directory = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
    }
    Ok(directory)
}

#[cfg(test)]
async fn wait_for_generated_lifecycle_arrival(directory: &Path) -> anyhow::Result<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(arrival) = read_generated_lifecycle_control_file(
                &directory.join(GENERATED_LIFECYCLE_BARRIER_ARRIVAL_FILE),
            )? {
                return Ok(arrival);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("generated lifecycle test did not observe arrival")?
}

#[cfg(test)]
fn generated_lifecycle_test_controller() -> anyhow::Result<Arc<GeneratedMemoryController>> {
    const MIB: usize = 1024 * 1024;

    GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 4,
        soft_budget_bytes: 16 * MIB,
        hard_budget_bytes: 24 * MIB,
        safety_reserve_bytes: MIB,
        unattributed_bytes_per_slot: MIB,
        cold_peak_growth_bytes: 4 * MIB,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * MIB,
        pressure_exit_headroom_bytes: 4 * MIB,
    })
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn spawn_generated_lifecycle_test_invocation(
    rt: ProdRuntime,
    database: &Database<ProdRuntime>,
    controller: Arc<GeneratedMemoryController>,
    generation: Arc<DeploymentGeneration>,
    cancellation: CancellationSignal,
    system_timeout: Duration,
    barrier: Option<Arc<GeneratedLifecycleBarrier>>,
    reuse_instances: bool,
) -> anyhow::Result<(
    tokio::task::JoinHandle<anyhow::Result<GeneratedExecutionOutput<ProdRuntime>>>,
    FunctionMemoryIdentity,
    Arc<GateMetrics>,
)> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let manifest = generated_test_manifest_with_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        4 * WASM_PAGE_BYTES,
    )?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: generation.deployment_sha256().to_owned(),
        generation_sha256: generation.generation_sha256.clone(),
        package_key: format!("lifecycle-{}", generation.deployment_sha256()),
        entry: DeploymentEntryIdentity::LegacyExport {
            runtime_module_path: "functions/generated_test.js".to_owned(),
            export_name: "run".to_owned(),
        },
    };
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let routed = generated_memory_test_routed_module_with_generation(
        Arc::clone(&manifest),
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: Some(0),
            maximum_pages: 4,
        },
        route_identity.clone(),
        Some(generation),
    )?;
    let metrics = Arc::new(GateMetrics::default());
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        cancellation,
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller,
            route_identity,
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(rt, &mut state, Duration::from_secs(5), system_timeout)?;
    Ok((
        tokio::spawn(execute_generated_with_lifecycle_barrier(
            routed,
            state,
            None,
            reuse_instances,
            barrier,
        )),
        memory_identity,
        metrics,
    ))
}

#[cfg(test)]
async fn run_generated_lifecycle_barrier_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 65_536;

    let database = new_test_database(rt.clone()).await?;

    let success_directory = generated_lifecycle_test_directory()?;
    let success_barrier = GeneratedLifecycleBarrier::load(
        success_directory.path().to_owned(),
        "generated_test:run".to_owned(),
    )?;
    let original_sha256 = "a".repeat(64);
    let registry = DeploymentRegistry::from_legacy(
        PathBuf::from("/unused/lifecycle-original"),
        ValidatedDeploymentManifest::empty_for_test(&original_sha256),
    );
    let original_generation = registry.current();
    let success_controller = generated_lifecycle_test_controller()?;
    let success_cancellation = CancellationSignal::new_for_test();
    let (success, success_identity, success_metrics) = spawn_generated_lifecycle_test_invocation(
        rt.clone(),
        &database,
        Arc::clone(&success_controller),
        original_generation,
        success_cancellation,
        Duration::from_secs(5),
        Some(success_barrier),
        false,
    )
    .await?;
    let success_arrival = wait_for_generated_lifecycle_arrival(success_directory.path()).await?;
    let arrival: JsonValue = serde_json::from_slice(&success_arrival)?;
    assert_eq!(
        arrival.get("kind"),
        Some(&JsonValue::String(
            GENERATED_LIFECYCLE_BARRIER_PROTOCOL.to_owned()
        ))
    );
    assert_eq!(
        arrival.get("generationSha256"),
        Some(&JsonValue::String(original_sha256.clone()))
    );
    let suspended = success_controller.snapshot_for_test(&success_identity);
    assert_eq!(suspended.active_instances, 1);
    assert_eq!(suspended.idle_instances, 0);
    assert_eq!(
        success_metrics
            .fresh_initialization_attempts
            .load(Ordering::SeqCst),
        0
    );

    let replacement_sha256 = "b".repeat(64);
    let replacement = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/lifecycle-replacement"),
        ValidatedDeploymentManifest::empty_for_test(&replacement_sha256),
    );
    let retired = registry
        .activate(replacement)?
        .expect("previous generation");
    assert_eq!(retired.generation_sha256, original_sha256);
    drop(retired);
    publish_generated_lifecycle_control_file(
        success_directory.path(),
        GENERATED_LIFECYCLE_BARRIER_RELEASE_FILE,
        &success_arrival,
    )?;
    let success = tokio::time::timeout(Duration::from_secs(5), success)
        .await
        .context("released generated lifecycle invocation did not finish")???;
    assert!(!success.cancelled);
    assert_eq!(success.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(success.invocation.function_result, Some(JsonValue::Null));
    assert_eq!(
        success_metrics
            .fresh_initialization_attempts
            .load(Ordering::SeqCst),
        1
    );
    assert!(success.reusable_instance.is_none());
    assert!(success.memory_permit.is_none());
    assert_eq!(success_metrics.teardowns.load(Ordering::SeqCst), 1);
    drop(success.invocation.transaction);
    let completed = success_controller.snapshot_for_test(&success_identity);
    assert_eq!(completed.active_instances, 0);
    assert_eq!(completed.idle_instances, 0);

    let cancellation_directory = generated_lifecycle_test_directory()?;
    let cancellation_barrier = GeneratedLifecycleBarrier::load(
        cancellation_directory.path().to_owned(),
        "generated_test:run".to_owned(),
    )?;
    let cancellation_sha256 = "c".repeat(64);
    let cancellation_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/lifecycle-cancellation"),
        ValidatedDeploymentManifest::empty_for_test(&cancellation_sha256),
    );
    let cancellation_generation_weak = Arc::downgrade(&cancellation_generation);
    let cancellation_controller = generated_lifecycle_test_controller()?;
    let cancellation_signal = CancellationSignal::new_for_test();
    let (cancellation, cancellation_identity, cancellation_metrics) =
        spawn_generated_lifecycle_test_invocation(
            rt.clone(),
            &database,
            Arc::clone(&cancellation_controller),
            cancellation_generation,
            cancellation_signal.clone(),
            Duration::from_secs(5),
            Some(cancellation_barrier),
            false,
        )
        .await?;
    wait_for_generated_lifecycle_arrival(cancellation_directory.path()).await?;
    assert_eq!(
        cancellation_controller
            .snapshot_for_test(&cancellation_identity)
            .active_instances,
        1
    );
    cancellation_signal.cancel_for_test();
    let cancellation = tokio::time::timeout(Duration::from_secs(5), cancellation)
        .await
        .context("cancelled generated lifecycle invocation did not finish")???;
    assert!(cancellation.cancelled);
    assert!(cancellation.system_error.is_none());
    assert!(cancellation.invocation.function_outcome.is_none());
    assert!(cancellation.invocation.function_result.is_none());
    assert_eq!(cancellation.invocation.opaque_live_handles, 0);
    assert_eq!(cancellation.invocation.opaque_current_bytes, 0);
    assert!(cancellation.invocation.capability_revoked);
    assert_eq!(
        cancellation_metrics
            .terminal_cancellations
            .load(Ordering::SeqCst),
        1
    );
    assert!(cancellation.reusable_instance.is_none());
    assert!(cancellation.memory_permit.is_none());
    assert_eq!(cancellation_metrics.teardowns.load(Ordering::SeqCst), 0);
    assert_eq!(cancellation_metrics.store_drops.load(Ordering::SeqCst), 1);
    assert!(cancellation_generation_weak.upgrade().is_none());
    assert_eq!(
        cancellation_metrics
            .fresh_initialization_attempts
            .load(Ordering::SeqCst),
        0
    );
    drop(cancellation.invocation.transaction);
    let cancelled = cancellation_controller.snapshot_for_test(&cancellation_identity);
    assert_eq!(cancelled.active_instances, 0);
    assert_eq!(cancelled.idle_instances, 0);

    let deadline_directory = generated_lifecycle_test_directory()?;
    let deadline_barrier = GeneratedLifecycleBarrier::load(
        deadline_directory.path().to_owned(),
        "generated_test:run".to_owned(),
    )?;
    let deadline_sha256 = "d".repeat(64);
    let deadline_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/lifecycle-deadline"),
        ValidatedDeploymentManifest::empty_for_test(&deadline_sha256),
    );
    let deadline_generation_weak = Arc::downgrade(&deadline_generation);
    let deadline_controller = generated_lifecycle_test_controller()?;
    let (deadline, deadline_identity, deadline_metrics) =
        spawn_generated_lifecycle_test_invocation(
            rt.clone(),
            &database,
            Arc::clone(&deadline_controller),
            deadline_generation,
            CancellationSignal::new_for_test(),
            Duration::from_millis(50),
            Some(deadline_barrier),
            false,
        )
        .await?;
    wait_for_generated_lifecycle_arrival(deadline_directory.path()).await?;
    let mut deadline = tokio::time::timeout(Duration::from_secs(5), deadline)
        .await
        .context("generated lifecycle system deadline did not finish")???;
    assert_eq!(
        deadline.invocation.outcome,
        InvocationOutcome::SystemTimeout
    );
    assert!(deadline.invocation.capability_revoked);
    assert!(deadline.system_error.take().is_some());
    assert!(deadline.reusable_instance.is_none());
    assert!(deadline.memory_permit.is_none());
    assert_eq!(deadline_metrics.teardowns.load(Ordering::SeqCst), 0);
    assert_eq!(deadline_metrics.store_drops.load(Ordering::SeqCst), 1);
    assert!(deadline_generation_weak.upgrade().is_none());
    assert_eq!(
        deadline_metrics
            .fresh_initialization_attempts
            .load(Ordering::SeqCst),
        0
    );
    drop(deadline.invocation.transaction);
    let timed_out = deadline_controller.snapshot_for_test(&deadline_identity);
    assert_eq!(timed_out.active_instances, 0);
    assert_eq!(timed_out.idle_instances, 0);

    let disabled_directory = generated_lifecycle_test_directory()?;
    let disabled_sha256 = "e".repeat(64);
    let disabled_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/lifecycle-disabled"),
        ValidatedDeploymentManifest::empty_for_test(&disabled_sha256),
    );
    let disabled_controller = generated_lifecycle_test_controller()?;
    let (disabled, disabled_identity, disabled_metrics) =
        spawn_generated_lifecycle_test_invocation(
            rt.clone(),
            &database,
            Arc::clone(&disabled_controller),
            disabled_generation,
            CancellationSignal::new_for_test(),
            Duration::from_secs(5),
            None,
            false,
        )
        .await?;
    let disabled = tokio::time::timeout(Duration::from_secs(5), disabled)
        .await
        .context("disabled generated lifecycle invocation did not finish")???;
    assert!(!disabled.cancelled);
    assert_eq!(disabled.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(disabled_metrics.teardowns.load(Ordering::SeqCst), 1);
    assert_eq!(
        disabled_metrics
            .fresh_initialization_attempts
            .load(Ordering::SeqCst),
        1
    );
    drop(disabled.invocation.transaction);
    assert!(fs::read_dir(disabled_directory.path())?.next().is_none());
    let disabled_snapshot = disabled_controller.snapshot_for_test(&disabled_identity);
    assert_eq!(disabled_snapshot.active_instances, 0);
    assert_eq!(disabled_snapshot.idle_instances, 0);

    // A retained runtime still owes destruction when its next invocation is
    // cancelled before entry. Fresh initialization must not run a second time.
    let warm_directory = generated_lifecycle_test_directory()?;
    let warm_barrier = GeneratedLifecycleBarrier::load(
        warm_directory.path().to_owned(),
        "generated_test:run".to_owned(),
    )?;
    let warm_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/lifecycle-warm-cancellation"),
        ValidatedDeploymentManifest::empty_for_test(&"f".repeat(64)),
    );
    let weak_generation = Arc::downgrade(&warm_generation);
    let warm_controller = generated_lifecycle_test_controller()?;
    let (warm, warm_identity, warm_metrics) = spawn_generated_lifecycle_test_invocation(
        rt.clone(),
        &database,
        Arc::clone(&warm_controller),
        warm_generation,
        CancellationSignal::new_for_test(),
        Duration::from_secs(5),
        None,
        true,
    )
    .await?;
    let mut warm = tokio::time::timeout(Duration::from_secs(5), warm).await???;
    assert_eq!(warm.invocation.outcome, InvocationOutcome::Success);
    assert!(warm.invocation.capability_revoked);
    let retained = retain_generated_test_runtime(&mut warm)?;
    let runtime_id = retained.instance.id;
    let routed = Arc::clone(&retained.instance.routed);
    drop(warm.invocation.transaction);
    assert_eq!(warm_metrics.teardowns.load(Ordering::SeqCst), 0);
    let signal = CancellationSignal::new_for_test();
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        generated_test_manifest_with_limits(
            ManifestUdfKind::Query,
            Vec::new(),
            4 * WASM_PAGE_BYTES,
        )?,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        signal.clone(),
        None,
        Arc::clone(&warm_metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&warm_controller),
            route_identity: routed.route_identity.clone(),
            existing_slot: Some(retained.memory_slot_id),
            values: Some(retained.values),
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    let cpu_limiter = state.active_wasm_cpu_limiter.clone();
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        Duration::from_secs(5),
    )?;
    let cancelled_warm = tokio::spawn(execute_generated_with_lifecycle_barrier(
        routed,
        state,
        Some(retained.instance),
        true,
        Some(warm_barrier),
    ));
    wait_for_generated_lifecycle_arrival(warm_directory.path()).await?;
    assert!(weak_generation.upgrade().is_some());
    signal.cancel_for_test();
    let cancelled_warm = tokio::time::timeout(Duration::from_secs(5), cancelled_warm).await???;
    assert_eq!(cancelled_warm.runtime_id, runtime_id);
    assert!(cancelled_warm.cancelled);
    assert!(cancelled_warm.system_error.is_none());
    assert!(cancelled_warm.invocation.function_outcome.is_none());
    assert!(cancelled_warm.invocation.function_result.is_none());
    assert_eq!(cancelled_warm.invocation.opaque_live_handles, 0);
    assert_eq!(cancelled_warm.invocation.opaque_current_bytes, 0);
    assert!(cancelled_warm.invocation.capability_revoked);
    assert!(cancelled_warm.reusable_instance.is_none());
    assert!(cancelled_warm.memory_permit.is_none());
    assert_eq!(
        warm_metrics
            .fresh_initialization_attempts
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(warm_metrics.teardowns.load(Ordering::SeqCst), 1);
    assert_eq!(cpu_limiter.active_permits(), 0);
    assert!(weak_generation.upgrade().is_none());
    drop(cancelled_warm.invocation.transaction);
    let warm_snapshot = warm_controller.snapshot_for_test(&warm_identity);
    assert_eq!(warm_snapshot.active_instances, 0);
    assert_eq!(warm_snapshot.idle_instances, 0);
    assert_eq!(warm_snapshot.evicting_instances, 0);
    assert_eq!(warm_snapshot.retained_baseline_bytes, 0);
    assert_eq!(warm_snapshot.unattributed_allowance_bytes, 0);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
pub(super) async fn new_test_database(rt: ProdRuntime) -> anyhow::Result<Database<ProdRuntime>> {
    let persistence: Arc<dyn Persistence> = Arc::new(SqlitePersistence::new(":memory:")?);
    let (deleted_tablet_sender, _deleted_tablet_receiver) = tokio::sync::mpsc::channel(16);
    let database = Database::load(
        persistence,
        rt.clone(),
        Arc::new(SearcherStub),
        ShutdownSignal::panic(),
        virtual_system_mapping().clone(),
        IndexCache::new(1 << 20).new_handle(),
        Arc::new(new_unlimited_rate_limiter(rt)),
        deleted_tablet_sender,
        "static_hermes_gate_tests".to_owned(),
    )
    .await?;
    initialize_application_system_tables(&database).await?;
    Ok(database)
}

#[cfg(test)]
async fn run_gate_provider_file_storage_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    initialize_application_system_tables(&database).await?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
    let origin = ConvexOrigin::from("http://127.0.0.1:3210".to_owned());
    let file_storage = TransactionalFileStorage::new(rt.clone(), storage, origin.clone());
    let unix_timestamp = rt.unix_timestamp();
    let mut state = new_state(
        rt,
        database.begin_system().await?,
        None,
        Arc::new(GateMetrics::default()),
        QueryJournal::new(),
    )
    .await?;
    state.provider.set_udf_type(UdfType::Mutation);
    state.provider.set_invocation(
        ResolvedComponentFunctionPath {
            component: ComponentId::Root,
            udf_path: "storage_provider_test:run".parse()?,
            component_path: ComponentPath::root(),
        },
        generated_test_execution_context(),
        DeploymentMetadata {
            name: "storage-provider-test".to_owned(),
            region: None,
            class: DeploymentClass::S16,
        },
        Version::new(1, 43, 0),
        unix_timestamp,
    );
    state.provider.set_test_file_storage_context(
        KeyBroker::dev().function_runner_keybroker(),
        file_storage,
        DeploymentMetadata {
            name: "storage-provider-test".to_owned(),
            region: None,
            class: DeploymentClass::S16,
        },
    );
    let timeout = state
        .timeout
        .as_mut()
        .context("storage provider test timeout missing")?;
    state.provider.prepare_execution(timeout).await?;

    let upload_url = state.provider.file_storage_generate_upload_url().await?;
    assert!(upload_url.starts_with(&format!("{origin}/api/storage/upload?token=")));

    let missing_storage_id: FileStorageId = "123e4567-e89b-12d3-a456-426614174000".parse()?;
    let mut urls = state
        .provider
        .file_storage_get_url_batch(BTreeMap::from([(7, missing_storage_id.clone())]))
        .await;
    assert_eq!(
        urls.remove(&7)
            .context("storage URL provider omitted its batch key")??,
        None
    );
    assert_eq!(
        state
            .provider
            .file_storage_get_entry(missing_storage_id.clone())
            .await?,
        None
    );
    assert!(state
        .provider
        .file_storage_delete(missing_storage_id)
        .await
        .is_err());

    drop(state);
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
pub(super) fn backend_gate_document(sequence: ConvexValue) -> anyhow::Result<ConvexObject> {
    obj!(
        "marker" => "backend-gate-document",
        "sequence" => sequence,
    )
}

#[cfg(test)]
pub(super) async fn insert_document_with_sequence(
    database: &Database<ProdRuntime>,
    table: &TableName,
    sequence: ConvexValue,
) -> anyhow::Result<ResolvedDocumentId> {
    let mut transaction = database.begin_system().await?;
    let id = SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
        .insert_metadata(table, backend_gate_document(sequence)?)
        .await?;
    database
        .commit_with_write_source(transaction, "static_hermes_backend_gate_setup")
        .await?;
    Ok(id)
}

#[cfg(test)]
pub(super) async fn insert_document(
    database: &Database<ProdRuntime>,
    table: &TableName,
) -> anyhow::Result<ResolvedDocumentId> {
    insert_document_with_sequence(database, table, ConvexValue::Int64(1)).await
}

#[cfg(test)]
async fn run_generated_environment_variable_tests(rt: ProdRuntime) -> anyhow::Result<()> {
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
        .commit_with_write_source(setup, "generated_wasm_environment_variable_setup")
        .await?;

    let metrics = Arc::new(GateMetrics::default());
    let mut initial_state = new_state(
        rt.clone(),
        database.begin_system().await?,
        None,
        Arc::clone(&metrics),
        QueryJournal::new(),
    )
    .await?;
    initial_state.provider.snoop_initialization_reads()?;
    let timeout = initial_state
        .timeout
        .as_mut()
        .context("environment variable test timeout missing")?;
    initial_state
        .provider
        .initialize_static_hermes(timeout)
        .await?;
    assert_eq!(
        initial_state.provider.get_environment_variable(&name)?,
        Some(initial_value.clone())
    );
    let initialization_reads = initial_state.provider.finish_initialization_reads()?;
    let context_read_set = ContextCache::capture_context_read_set(
        initialization_reads,
        initial_state.provider.tx_for_initialization()?,
    )
    .await?
    .context("environment initialization did not produce a reusable read set")?;
    drop(initial_state);

    let mut unchanged_state = new_state(
        rt.clone(),
        database.begin_system().await?,
        None,
        Arc::clone(&metrics),
        QueryJournal::new(),
    )
    .await?;
    assert!(
        ContextCache::validate_and_apply_context_read_set(
            unchanged_state.provider.tx_for_initialization()?,
            &context_read_set,
        )
        .await?
    );
    assert!(
        unchanged_state
            .provider
            .tx_for_initialization()?
            .execution_size()
            .num_intervals
            > 0
    );
    drop(unchanged_state);

    let mut update = database.begin_system().await?;
    let removed = EnvironmentVariablesModel::new(&mut update)
        .delete(&name)
        .await?
        .context("environment variable disappeared before update")?;
    assert_eq!(removed.value(), &initial_value);
    EnvironmentVariablesModel::new(&mut update)
        .create(
            EnvironmentVariable::new(name.clone(), updated_value.clone()),
            &Default::default(),
        )
        .await?;
    database
        .commit_with_write_source(update, "generated_wasm_environment_variable_update")
        .await?;

    let mut changed_state = new_state(
        rt.clone(),
        database.begin_system().await?,
        None,
        Arc::clone(&metrics),
        QueryJournal::new(),
    )
    .await?;
    assert!(
        !ContextCache::validate_and_apply_context_read_set(
            changed_state.provider.tx_for_initialization()?,
            &context_read_set,
        )
        .await?
    );
    drop(changed_state);

    let manifest = generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        1 << 20,
        1_000_000,
        3,
    )?;
    let mut state = generated_test_state(
        rt,
        database.begin_system().await?,
        manifest,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        metrics,
        None,
    )
    .await?;
    let timeout = state
        .timeout
        .as_mut()
        .context("environment variable capability test timeout missing")?;
    state.provider.prepare_execution(timeout).await?;
    generated_state_mut(&mut state)?.capability_bridge.issue()?;
    let capability = generated_state(&state)?.capability_bridge.handle()?;
    let invalid_name_error = "INVALID-NAME"
        .parse::<EnvVarName>()
        .expect_err("invalid environment variable name parsed")
        .short_msg()
        .to_owned();
    for (requested_name, expected) in [
        (
            name.as_ref(),
            json!({ "value": String::from(updated_value.clone()) }),
        ),
        ("MISSING_KEY", json!({ "value": null })),
        ("INVALID-NAME", json!({ "error": invalid_name_error })),
    ] {
        let envelope = GuestNativeValueCodec::encode(
            json!({
                "version": 4,
                "kind": "environmentVariableGet",
                "name": requested_name,
            }),
            MAX_REQUEST_BYTES,
        )?;
        let request =
            generated_state_mut(&mut state)?
                .values
                .insert(OpaqueValue::CapabilityRequest(
                    GuestCapabilityRequestCodec::decode(&envelope, MAX_REQUEST_BYTES)?,
                ))?;
        let result = opaque_handle(run_generated_capability_sync_operation(
            &mut state,
            capability,
            request.to_abi(),
        )?)?;
        assert_eq!(generated_state(&state)?.values.get_json(result)?, &expected);
        assert!(generated_state(&state)?
            .values
            .get(request, OpaqueValueKind::CapabilityRequest)
            .is_err());
        generated_state_mut(&mut state)?
            .values
            .release(result, OpaqueValueKind::ConvexJson)?;
    }
    assert_eq!(generated_state(&state)?.operation_count, 3);
    generated_state_mut(&mut state)?
        .capability_bridge
        .revoke()?;
    assert!(generated_state(&state)?.capability_bridge.handle().is_err());
    generated_state_mut(&mut state)?.values.cleanup();
    drop(state);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
fn missing_id(existing: DeveloperDocumentId) -> DeveloperDocumentId {
    DeveloperDocumentId::new(existing.table(), value::InternalId::MAX)
}

#[cfg(test)]
async fn run_generated_host_secret_verification_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let manifest = generated_test_manifest(
        ManifestUdfKind::Query,
        json!({
            "kind": "hostSecretVerify",
            "contractVersion": 1,
            "selector": "HOST_SECRET",
        }),
    )?;
    let secret_values = || {
        BTreeMap::from([(
            "HOST_SECRET".to_owned(),
            HostSecretValue::new(b"configured-secret".to_vec()),
        )])
    };

    for host_secret_values in [
        BTreeMap::new(),
        BTreeMap::from([("HOST_SECRET".to_owned(), HostSecretValue::new(Vec::new()))]),
    ] {
        let missing_before_memory_read = execute_generated_host_secret_verify_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            1,
            65_535,
            2,
            b"",
            host_secret_values,
            false,
        )
        .await?;
        assert_eq!(
            missing_before_memory_read.invocation.function_result,
            Some(json!("missing"))
        );
        drop(missing_before_memory_read.invocation.transaction);
    }

    for (candidate, expected) in [
        (b"".as_slice(), "mismatch"),
        (b"wrong".as_slice(), "mismatch"),
        ([0xff].as_slice(), "mismatch"),
        (b"configured-secret".as_slice(), "match"),
    ] {
        let output = execute_generated_host_secret_verify_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            1,
            64,
            i32::try_from(candidate.len())?,
            candidate,
            secret_values(),
            false,
        )
        .await?;
        assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
        assert_eq!(output.invocation.function_result, Some(json!(expected)));
        drop(output.invocation.transaction);
    }

    let non_string = execute_generated_host_secret_verify_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        1,
        0,
        0,
        b"",
        secret_values(),
        false,
    )
    .await?;
    assert_eq!(
        non_string.invocation.function_result,
        Some(json!("mismatch"))
    );
    drop(non_string.invocation.transaction);

    let configured_secret = "configured-secret";
    let invalid_bounds = execute_generated_host_secret_verify_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        1,
        65_535,
        2,
        b"",
        secret_values(),
        false,
    )
    .await?;
    assert_eq!(
        invalid_bounds.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert!(invalid_bounds
        .system_error
        .as_ref()
        .is_some_and(|error| !format!("{error:#}").contains(configured_secret)));
    drop(invalid_bounds.invocation.transaction);

    let mut reusable = execute_generated_host_secret_verify_test_module(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        1,
        64,
        i32::try_from(configured_secret.len())?,
        configured_secret.as_bytes(),
        secret_values(),
        true,
    )
    .await?;
    assert_eq!(reusable.invocation.function_result, Some(json!("match")));
    let instance = reusable
        .reusable_instance
        .take()
        .context("successful host-secret invocation was not reusable")?;
    assert!(generated_state(instance.store.data())?
        .host_secret_values
        .is_empty());
    discard_generated_instance(instance).await?;
    reusable
        .memory_permit
        .take()
        .context("reusable host-secret invocation lost its memory permit")?
        .finish(reusable.terminal_memory_outcome, false);
    drop(reusable.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
pub(super) async fn execute_generated_test_module(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    operation: GeneratedTestOperation,
    request: JsonValue,
    udf_type: UdfType,
    unix_timestamp: UnixTimestamp,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let routed = generated_test_routed_module(Arc::clone(&manifest), operation)?;
    let mut state = generated_test_state(
        rt.clone(),
        transaction,
        manifest,
        request,
        udf_type,
        unix_timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut output = execute_generated(Arc::clone(&routed), state, None, false).await?;
    if let Some(instance) = output.reusable_instance.take() {
        discard_generated_instance(instance).await?;
        output
            .memory_permit
            .take()
            .context("generated test reusable instance lost its memory permit")?
            .finish(output.terminal_memory_outcome, false);
    }
    Ok(output)
}

#[cfg(test)]
fn generated_async_batch_query_trace(
    output: &GeneratedExecutionOutput<ProdRuntime>,
) -> anyhow::Result<Vec<(LogicalHostOperation, LogicalHostOperationStatus)>> {
    let FunctionOutcome::Query(outcome) = output
        .invocation
        .function_outcome
        .as_ref()
        .context("generated async batch did not produce a query outcome")?
    else {
        anyhow::bail!("generated async batch produced a non-query outcome");
    };
    Ok(outcome
        .host_operation_trace
        .entries()
        .context("generated async batch host-operation trace is disabled")?
        .iter()
        .map(|entry| (entry.operation(), entry.status()))
        .collect())
}

#[cfg(test)]
fn generated_capability_query_trace(
    output: &GeneratedExecutionOutput<ProdRuntime>,
) -> anyhow::Result<Vec<(LogicalHostOperation, LogicalHostOperationStatus)>> {
    let FunctionOutcome::Query(outcome) = output
        .invocation
        .function_outcome
        .as_ref()
        .context("generated capability query did not produce a query outcome")?
    else {
        anyhow::bail!("generated capability query produced a non-query outcome");
    };
    Ok(outcome
        .host_operation_trace
        .entries()
        .context("generated capability query host-operation trace is disabled")?
        .iter()
        .map(|entry| (entry.operation(), entry.status()))
        .collect())
}

#[cfg(test)]
async fn execute_generated_capability_async_operation_test_module(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    request_envelope: Vec<u8>,
    expected_completion_status: i32,
    cancellation: CancellationSignal,
    read_control: Option<ReadControl>,
    metrics: Arc<GateMetrics>,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    GuestCapabilityRequestCodec::decode(&request_envelope, request_envelope.len())?;
    let mut routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        generated_capability_async_operation_test_module(
            &request_envelope,
            expected_completion_status,
        ),
    )?;
    Arc::get_mut(&mut routed)
        .context("capability test routed module was unexpectedly shared")?
        .entry_selector = Some(0);
    let mut state = generated_test_state(
        rt.clone(),
        transaction,
        manifest,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        cancellation,
        read_control,
        metrics,
        None,
    )
    .await?;
    state.provider.enable_host_operation_trace();
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    execute_generated(routed, state, None, false).await
}

#[cfg(test)]
async fn execute_generated_host_secret_verify_test_module(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    operation_id: i32,
    pointer: i32,
    length: i32,
    candidate: &[u8],
    host_secret_values: BTreeMap<String, HostSecretValue>,
    reuse_instance: bool,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let routed = generated_host_secret_verify_test_routed_module(
        Arc::clone(&manifest),
        operation_id,
        pointer,
        length,
        candidate,
    )?;
    let mut state = generated_test_state(
        rt.clone(),
        transaction,
        manifest,
        json!({
            "missing": "missing",
            "mismatch": "mismatch",
            "match": "match",
        }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    state
        .generated
        .as_mut()
        .context("generated host-secret test state is missing")?
        .host_secret_values = host_secret_values;
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    execute_generated(routed, state, None, reuse_instance).await
}

#[cfg(test)]
async fn execute_generated_async_batch_test_module(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    operation: GeneratedAsyncBatchTestOperation,
    request: JsonValue,
    udf_type: UdfType,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    execute_generated_async_batch_test_module_with_host_operation_trace(
        rt,
        transaction,
        manifest,
        operation,
        request,
        udf_type,
        false,
    )
    .await
}

#[cfg(test)]
async fn execute_generated_async_batch_test_module_with_host_operation_trace(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    operation: GeneratedAsyncBatchTestOperation,
    request: JsonValue,
    udf_type: UdfType,
    host_operation_trace: bool,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let routed = generated_async_batch_test_routed_module(Arc::clone(&manifest), operation)?;
    let mut state = generated_test_state(
        rt.clone(),
        transaction,
        manifest,
        request,
        udf_type,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    if host_operation_trace {
        state.provider.enable_host_operation_trace();
    }
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut output = execute_generated(Arc::clone(&routed), state, None, false).await?;
    if let Some(instance) = output.reusable_instance.take() {
        discard_generated_instance(instance).await?;
        output
            .memory_permit
            .take()
            .context("generated batch test reusable instance lost its memory permit")?
            .finish(output.terminal_memory_outcome, false);
    }
    Ok(output)
}

#[cfg(test)]
pub(super) async fn execute_generated_sequential_query_collect_test_module(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    request: JsonValue,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let routed = generated_sequential_query_collect_test_routed_module(Arc::clone(&manifest))?;
    let mut state = generated_test_state(
        rt.clone(),
        transaction,
        manifest,
        request,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut output = execute_generated(Arc::clone(&routed), state, None, false).await?;
    if let Some(instance) = output.reusable_instance.take() {
        discard_generated_instance(instance).await?;
        output
            .memory_permit
            .take()
            .context("generated query test reusable instance lost its memory permit")?
            .finish(output.terminal_memory_outcome, false);
    }
    Ok(output)
}

#[cfg(test)]
pub(super) async fn execute_reusable_generated_async_batch_test_module(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    routed: Arc<GeneratedRoutedModule>,
    request: JsonValue,
    controller: Arc<GeneratedMemoryController>,
    metrics: Arc<GateMetrics>,
    reusable: Option<RetainedGeneratedTestRuntime>,
    retain_on_success: bool,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let (reusable_instance, existing_slot, values) = match reusable {
        Some(reusable) => (
            Some(reusable.instance),
            Some(reusable.memory_slot_id),
            Some(reusable.values),
        ),
        None => (None, None, None),
    };
    let maximum_guest_memory_bytes = usize::try_from(manifest.limits().max_guest_memory_bytes())?;
    let route_identity = routed.route_identity.clone();
    let mut state = generated_test_state(
        rt.clone(),
        transaction,
        manifest,
        request,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        metrics,
        Some(GeneratedTestMemorySetup {
            controller,
            route_identity,
            existing_slot,
            values,
            maximum_guest_memory_bytes,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    execute_generated(routed, state, reusable_instance, retain_on_success).await
}

#[cfg(test)]
async fn execute_reusable_generated_normalize_id_test_module(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    routed: Arc<GeneratedRoutedModule>,
    request: JsonValue,
    udf_type: UdfType,
    controller: Arc<GeneratedMemoryController>,
    metrics: Arc<GateMetrics>,
    reusable: Option<RetainedGeneratedTestRuntime>,
    retain_on_success: bool,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let (reusable_instance, existing_slot, values) = match reusable {
        Some(reusable) => (
            Some(reusable.instance),
            Some(reusable.memory_slot_id),
            Some(reusable.values),
        ),
        None => (None, None, None),
    };
    let maximum_guest_memory_bytes = usize::try_from(manifest.limits().max_guest_memory_bytes())?;
    let route_identity = routed.route_identity.clone();
    let mut state = generated_test_state(
        rt.clone(),
        transaction,
        manifest,
        request,
        udf_type,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        metrics,
        Some(GeneratedTestMemorySetup {
            controller,
            route_identity,
            existing_slot,
            values,
            maximum_guest_memory_bytes,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    execute_generated(routed, state, reusable_instance, retain_on_success).await
}

#[cfg(test)]
pub(super) fn retain_generated_test_runtime(
    output: &mut GeneratedExecutionOutput<ProdRuntime>,
) -> anyhow::Result<RetainedGeneratedTestRuntime> {
    anyhow::ensure!(
        output.invocation.outcome == InvocationOutcome::Success,
        "only a successful generated invocation may retain its runtime"
    );
    let mut instance = output
        .reusable_instance
        .take()
        .context("successful generated invocation did not retain its runtime")?;
    let memory_permit = output
        .memory_permit
        .take()
        .context("successful generated invocation did not retain its memory permit")?;
    let memory_slot_id = memory_permit.slot_id();
    memory_permit.finish(output.terminal_memory_outcome, true);
    let mut completed = instance
        .store
        .data_mut()
        .generated
        .take()
        .context("retained generated Store lost its completed invocation state")?;
    anyhow::ensure!(
        completed.memory_permit.is_none(),
        "retained generated Store kept a completed memory permit"
    );
    anyhow::ensure!(
        completed.values.live_handle_count() == 0
            && completed.values.accounting().current_bytes == 0,
        "retained generated Store kept opaque values from its completed invocation"
    );
    // A pooled Store keeps completed invocation state available for runtime
    // teardown. Preserve that shape after moving its reusable value table into
    // the test fixture.
    let maximum_live_handles = usize::try_from(completed.manifest.limits().max_value_handles())?;
    let maximum_host_owned_bytes =
        usize::try_from(completed.manifest.limits().max_host_owned_bytes())?;
    let values = std::mem::replace(
        &mut completed.values,
        OpaqueValueTable::new(maximum_live_handles, maximum_host_owned_bytes),
    );
    instance.store.data_mut().generated = Some(completed);
    Ok(RetainedGeneratedTestRuntime {
        instance,
        memory_slot_id,
        values,
    })
}

#[cfg(test)]
async fn install_generated_test_function(database: &Database<ProdRuntime>) -> anyhow::Result<()> {
    initialize_application_system_tables(database).await?;
    let mut transaction = database.begin_system().await?;
    let source_package_id =
        SourcePackageModel::new(&mut transaction, TableNamespace::root_component())
            .put(SourcePackage {
                storage_key: ObjectKey::try_from("generated-wasm-scheduler-test")?,
                sha256: Sha256Digest::from([0; 32]),
                runtime_content_sha256: Some(Sha256Digest::from([9; 32])),
                runtime_generation: None,
                external_deps_package_id: None,
                package_size: PackageSize::default(),
                node_version: None,
                node_executor_pool_topology: Default::default(),
            })
            .await?;
    let analyzed_function = AnalyzedFunction::new(
        "run".parse()?,
        None,
        UdfType::Mutation,
        Some(Visibility::Public),
        ArgsValidator::Unvalidated,
        ReturnsValidator::Unvalidated,
    )?;
    let module_path: CanonicalizedModulePath = "scheduled.js".parse()?;
    let analyzed_module = AnalyzedModule {
        functions: vec![analyzed_function].into(),
        ..Default::default()
    };
    ModuleModel::new(&mut transaction)
        .put(
            None,
            CanonicalizedComponentModulePath {
                component: ComponentId::Root,
                module_path: module_path.clone(),
            },
            ModuleSource::from(""),
            source_package_id,
            None,
            Some(analyzed_module.clone()),
            ModuleEnvironment::Isolate,
            None,
        )
        .await?;
    FunctionHandlesModel::new(&mut transaction)
        .apply_config_diff(
            ComponentId::Root,
            Some(&BTreeMap::from([(module_path, analyzed_module)])),
        )
        .await?;
    database
        .commit_with_write_source(transaction, "generated_wasm_scheduler_test_setup")
        .await?;
    Ok(())
}

#[cfg(test)]
async fn run_deployed_source_identity_gate_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt).await?;
    install_generated_test_function(&database).await?;
    let module_path: CanonicalizedModulePath = "scheduled.js".parse()?;
    let mut invocation_transaction = database.begin_system().await?;
    let module = ModuleModel::new(&mut invocation_transaction)
        .get_metadata(CanonicalizedComponentModulePath {
            component: ComponentId::Root,
            module_path: module_path.clone(),
        })
        .await?
        .context("source identity test module missing")?;
    let source_package =
        SourcePackageModel::new(&mut invocation_transaction, TableNamespace::Global)
            .get_latest()
            .await?
            .context("source identity test package missing")?;
    let original =
        DeployedRuntimeIdentity::for_test(module.sha256.as_hex(), source_package.sha256.as_hex());
    let runtime_content_identity = DeployedRuntimeIdentity::for_runtime_content_test(
        module.sha256.as_hex(),
        Sha256Digest::from([9; 32]).as_hex(),
    );

    let redeployed_package_sha256 = Sha256Digest::from([1; 32]);
    let mut redeployment_transaction = database.begin_system().await?;
    SourcePackageModel::new(&mut redeployment_transaction, TableNamespace::Global)
        .put(SourcePackage {
            storage_key: ObjectKey::try_from("generated-wasm-scheduler-test-redeployment")?,
            sha256: redeployed_package_sha256.clone(),
            runtime_content_sha256: Some(Sha256Digest::from([7; 32])),
            runtime_generation: None,
            external_deps_package_id: None,
            package_size: PackageSize::default(),
            node_version: None,
            node_executor_pool_topology: Default::default(),
        })
        .await?;
    database
        .commit_with_write_source(
            redeployment_transaction,
            "generated_wasm_source_identity_redeployment",
        )
        .await?;
    let redeployed = DeployedRuntimeIdentity::for_test(
        module.sha256.as_hex(),
        redeployed_package_sha256.as_hex(),
    );
    let redeployed_runtime_content_identity = DeployedRuntimeIdentity::for_runtime_content_test(
        module.sha256.as_hex(),
        Sha256Digest::from([7; 32]).as_hex(),
    );

    verify_deployed_source_identity(&mut invocation_transaction, &module_path, &original).await?;
    assert!(verify_deployed_source_identity(
        &mut invocation_transaction,
        &module_path,
        &redeployed
    )
    .await
    .is_err());
    verify_deployed_source_identity(
        &mut invocation_transaction,
        &module_path,
        &runtime_content_identity,
    )
    .await?;
    assert!(verify_deployed_source_identity(
        &mut invocation_transaction,
        &module_path,
        &redeployed_runtime_content_identity,
    )
    .await
    .is_err());

    let mismatched_module = DeployedRuntimeIdentity::for_test(
        "f".repeat(64),
        original
            .source_package_archive_sha256()
            .context("legacy test identity must bind a source-package archive")?
            .to_owned(),
    );
    assert!(verify_deployed_source_identity(
        &mut invocation_transaction,
        &module_path,
        &mismatched_module,
    )
    .await
    .is_err());
    let mismatched_package =
        DeployedRuntimeIdentity::for_test(original.module_sha256().to_owned(), "e".repeat(64));
    assert!(verify_deployed_source_identity(
        &mut invocation_transaction,
        &module_path,
        &mismatched_package,
    )
    .await
    .is_err());
    drop(invocation_transaction);

    let mut fresh_transaction = database.begin_system().await?;
    verify_deployed_source_identity(&mut fresh_transaction, &module_path, &redeployed).await?;
    assert!(
        verify_deployed_source_identity(&mut fresh_transaction, &module_path, &original)
            .await
            .is_err()
    );
    verify_deployed_source_identity(
        &mut fresh_transaction,
        &module_path,
        &redeployed_runtime_content_identity,
    )
    .await?;
    assert!(verify_deployed_source_identity(
        &mut fresh_transaction,
        &module_path,
        &runtime_content_identity,
    )
    .await
    .is_err());
    let mismatched_runtime_content = DeployedRuntimeIdentity::for_runtime_content_test(
        redeployed.module_sha256().to_owned(),
        Sha256Digest::from([8; 32]).as_hex(),
    );
    assert!(verify_deployed_source_identity(
        &mut fresh_transaction,
        &module_path,
        &mismatched_runtime_content,
    )
    .await
    .is_err());
    drop(fresh_transaction);

    let mut legacy_redeployment_transaction = database.begin_system().await?;
    SourcePackageModel::new(&mut legacy_redeployment_transaction, TableNamespace::Global)
        .put(SourcePackage {
            storage_key: ObjectKey::try_from("generated-wasm-scheduler-test-legacy")?,
            sha256: Sha256Digest::from([2; 32]),
            runtime_content_sha256: None,
            runtime_generation: None,
            external_deps_package_id: None,
            package_size: PackageSize::default(),
            node_version: None,
            node_executor_pool_topology: Default::default(),
        })
        .await?;
    database
        .commit_with_write_source(
            legacy_redeployment_transaction,
            "generated_wasm_source_identity_legacy_redeployment",
        )
        .await?;
    let mut legacy_transaction = database.begin_system().await?;
    assert!(verify_deployed_source_identity(
        &mut legacy_transaction,
        &module_path,
        &redeployed_runtime_content_identity
    )
    .await
    .is_err());
    drop(legacy_transaction);
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn execute_generation_retirement_fixture(
    rt: ProdRuntime,
    database: &Database<ProdRuntime>,
    controller: &Arc<GeneratedMemoryController>,
    generation: Arc<DeploymentGeneration>,
    deployment: char,
    metrics: Arc<GateMetrics>,
) -> anyhow::Result<(
    GeneratedExecutionOutput<ProdRuntime>,
    FunctionMemoryIdentity,
)> {
    let manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: deployment.to_string().repeat(64),
        generation_sha256: generation.generation_sha256.clone(),
        package_key: format!("retirement-package-{deployment}"),
        entry: DeploymentEntryIdentity::LegacyExport {
            runtime_module_path: "functions/retirement.js".to_owned(),
            export_name: "run".to_owned(),
        },
    };
    let routed = generated_memory_test_routed_module_with_generation(
        Arc::clone(&manifest),
        GeneratedMemoryTestOperation {
            additional_pages: 0,
            destroy_infinite_loop: false,
            loop_iterations: Some(0),
            maximum_pages: 1,
        },
        route_identity.clone(),
        Some(generation),
    )?;
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        metrics,
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(controller),
            route_identity,
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 1 << 20,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    Ok((
        execute_generated(routed, state, None, true).await?,
        memory_identity,
    ))
}

#[cfg(test)]
async fn run_generation_retirement_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 4,
        soft_budget_bytes: 8 * 1024 * 1024,
        hard_budget_bytes: 16 * 1024 * 1024,
        safety_reserve_bytes: 1024,
        unattributed_bytes_per_slot: 1024,
        cold_peak_growth_bytes: 1024,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 1024,
        pressure_exit_headroom_bytes: 2048,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 32 * 1024 * 1024,
    });

    let active_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/active-retirement"),
        ValidatedDeploymentManifest::empty_for_test(&"d".repeat(64)),
    );
    let active_metrics = Arc::new(GateMetrics::default());
    let (mut active, active_identity) = execute_generation_retirement_fixture(
        rt.clone(),
        &database,
        &controller,
        Arc::clone(&active_generation),
        'd',
        Arc::clone(&active_metrics),
    )
    .await?;
    active_generation.retired.store(true, Ordering::Release);
    drop(active.invocation.transaction);
    return_generated_routed_instance(
        active
            .reusable_instance
            .take()
            .context("retired active fixture did not retain its runtime")?,
        active
            .memory_permit
            .take()
            .context("retired active fixture lost its memory permit")?,
        active.terminal_memory_outcome,
    )
    .await?;
    let active_snapshot = controller.snapshot_for_test(&active_identity);
    assert_eq!(active_snapshot.active_instances, 0);
    assert_eq!(active_snapshot.idle_instances, 0);
    assert_eq!(active_metrics.teardowns.load(Ordering::SeqCst), 1);

    let idle_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/idle-retirement"),
        ValidatedDeploymentManifest::empty_for_test(&"e".repeat(64)),
    );
    let idle_metrics = Arc::new(GateMetrics::default());
    let (mut idle, idle_identity) = execute_generation_retirement_fixture(
        rt,
        &database,
        &controller,
        Arc::clone(&idle_generation),
        'e',
        Arc::clone(&idle_metrics),
    )
    .await?;
    drop(idle.invocation.transaction);
    return_generated_routed_instance(
        idle.reusable_instance
            .take()
            .context("idle retirement fixture did not retain its runtime")?,
        idle.memory_permit
            .take()
            .context("idle retirement fixture lost its memory permit")?,
        idle.terminal_memory_outcome,
    )
    .await?;
    assert_eq!(
        controller.snapshot_for_test(&idle_identity).idle_instances,
        1
    );
    retire_deployment_generation::<ProdRuntime>(&controller, Arc::clone(&idle_generation)).await?;
    assert_eq!(
        controller.snapshot_for_test(&idle_identity).idle_instances,
        0
    );
    assert_eq!(idle_metrics.teardowns.load(Ordering::SeqCst), 1);
    assert!(!GENERATED_ROUTED_INSTANCE_POOL
        .lock()
        .iter()
        .any(|instance| {
            instance
                .downcast_ref::<GeneratedReusableInstance<ProdRuntime>>()
                .is_some_and(|instance| {
                    instance
                        .routed
                        .generation
                        .as_ref()
                        .is_some_and(|generation| Arc::ptr_eq(generation, &idle_generation))
                })
        }));

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_database_get_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_get_documents".parse()?;
    let id = insert_document(&database, &table).await?.developer_id;
    let manifest = generated_test_manifest(
        ManifestUdfKind::Query,
        json!({
            "kind": "databaseGet",
            "tableName": table,
        }),
    )?;

    let hit = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        GeneratedTestOperation::DatabaseGet,
        json!({ "id": id.encode() }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .await?;
    assert!(!hit.cancelled);
    assert_eq!(hit.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        hit.invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("marker")),
        Some(&json!("backend-gate-document"))
    );
    assert_eq!(hit.invocation.read_accounting.documents, 1);
    assert!(hit.invocation.read_accounting.bytes > 0);
    assert!(hit.invocation.read_accounting.intervals > 0);
    drop(hit.invocation.transaction);

    let missing = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        GeneratedTestOperation::DatabaseGet,
        json!({ "id": missing_id(id).encode() }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .await?;
    assert_eq!(missing.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(missing.invocation.function_result, Some(JsonValue::Null));
    assert_eq!(missing.invocation.read_accounting.documents, 0);
    assert!(missing.invocation.read_accounting.intervals > 0);
    drop(missing.invocation.transaction);

    let invalid = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        GeneratedTestOperation::DatabaseGet,
        json!({ "id": "not-a-document-id" }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .await?;
    assert_eq!(
        invalid.invocation.outcome,
        InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
    );
    assert_eq!(invalid.invocation.read_accounting.documents, 0);
    drop(invalid.invocation.transaction);

    let invalid_shape = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        GeneratedTestOperation::DatabaseGet,
        json!({ "id": { "not": "a document ID" } }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .await?;
    assert_eq!(
        invalid_shape.invocation.outcome,
        InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
    );
    assert_eq!(invalid_shape.invocation.read_accounting.documents, 0);
    drop(invalid_shape.invocation.transaction);

    let routed =
        generated_test_routed_module(Arc::clone(&manifest), GeneratedTestOperation::DatabaseGet)?;
    let cancellation = CancellationSignal::new_for_test();
    let (control, mut host) = read_control();
    let metrics = Arc::new(GateMetrics::default());
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        json!({ "id": id.encode() }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        cancellation.clone(),
        Some(control),
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
    let execution = tokio::spawn(execute_generated(routed, state, None, false));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("generated db.get did not become pending")??;
    cancellation.cancel_for_test();
    let cancelled = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("generated db.get cancellation did not complete")???;
    assert!(cancelled.cancelled);
    assert_eq!(metrics.read_cancelled.load(Ordering::SeqCst), 1);
    assert!(host
        .release
        .take()
        .context("generated db.get cancellation release missing")?
        .send(())
        .is_err());
    drop(cancelled.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_host_owned_limit_classification_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_host_owned_limit_documents".parse()?;
    let manifest = generated_test_manifest(
        ManifestUdfKind::Query,
        json!({
            "kind": "databaseGet",
            "tableName": table,
        }),
    )?;
    let mut host_owned_manifest = serde_json::to_value(manifest.as_ref())?;
    host_owned_manifest["limits"]["maxHostOwnedBytes"] = json!(64 * 1024);
    let host_owned_manifest = Arc::new(WasmUdfExecutionManifest::parse_for_runtime(
        &serde_json::to_vec(&host_owned_manifest)?,
        &generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?,
    )?);

    // The request fits the manifest's host-owned byte limit, but cloning its
    // field for the guest does not. Exercise the actual host-import trap path
    // so Wasmtime cannot erase the bounded resource-failure classification.
    let output = execute_generated_test_module(
        rt,
        database.begin_system().await?,
        host_owned_manifest,
        GeneratedTestOperation::DatabaseGet,
        json!({ "id": "x".repeat(40 * 1024) }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .await?;
    assert_eq!(output.invocation.outcome, InvocationOutcome::SystemError);
    assert_eq!(
        output
            .system_error
            .as_ref()
            .and_then(|error| error.downcast_ref::<StaticHermesWasmExecutionFailure>()),
        Some(&StaticHermesWasmExecutionFailure::HostOwnedBytesLimit)
    );
    drop(output.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_operation_limit_classification_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_operation_limit_documents".parse()?;
    let id = insert_document(&database, &table).await?.developer_id;
    let missing = missing_id(id);
    let manifest = generated_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        vec![json!({
            "id": 1,
            "debugName": "generatedOperationLimitGet",
            "operation": {
                "kind": "databaseGet",
                "tableName": table,
            },
        })],
        1 << 20,
        1_000_000,
        1,
    )?;
    let output = execute_generated_async_batch_test_module(
        rt,
        database.begin_system().await?,
        manifest,
        GeneratedAsyncBatchTestOperation {
            import_database_write: false,
            later_database_write_operation_id: None,
        },
        json!({
            "batch": [
                {
                    "operationId": 1.0,
                    "arguments": [id.encode()],
                },
                {
                    "operationId": 1.0,
                    "arguments": [missing.encode()],
                },
            ],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(output.invocation.outcome, InvocationOutcome::SystemError);
    assert_eq!(
        output
            .system_error
            .as_ref()
            .and_then(|error| error.downcast_ref::<StaticHermesWasmExecutionFailure>()),
        Some(&StaticHermesWasmExecutionFailure::OperationLimit)
    );
    drop(output.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_guest_promise_database_get_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_guest_promise_get_documents".parse()?;
    let id = insert_document(&database, &table).await?.developer_id;
    let manifest = generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        vec![json!({
            "id": 1,
            "debugName": "getDocument",
            "operation": {
                "kind": "databaseGet",
                "tableName": table,
            },
        })],
        1 << 20,
        1_000_000_000,
        128,
    )?;
    let routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        generated_guest_promise_database_get_test_module(),
    )?;

    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "id": id.encode() }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let hit = execute_generated(Arc::clone(&routed), state, None, false).await?;
    assert_eq!(hit.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        hit.invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("marker")),
        Some(&json!("backend-gate-document"))
    );
    assert_eq!(hit.invocation.read_accounting.documents, 1);
    assert_eq!(hit.invocation.opaque_live_handles, 0);
    assert_eq!(hit.invocation.opaque_current_bytes, 0);
    drop(hit.invocation.transaction);

    let cancellation = CancellationSignal::new_for_test();
    let (control, mut host) = read_control();
    let metrics = Arc::new(GateMetrics::default());
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        json!({ "id": id.encode() }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        cancellation.clone(),
        Some(control),
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
    let execution = tokio::spawn(execute_generated(routed, state, None, false));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("guest-promise db.get did not become pending")??;
    cancellation.cancel_for_test();
    let cancelled = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("guest-promise db.get cancellation did not complete")???;
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.invocation.opaque_live_handles, 0);
    assert_eq!(cancelled.invocation.opaque_current_bytes, 0);
    assert_eq!(metrics.read_cancelled.load(Ordering::SeqCst), 1);
    assert!(host
        .release
        .take()
        .context("guest-promise db.get release missing")?
        .send(())
        .is_err());
    drop(cancelled.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_guest_promise_host_operation_error_identity_tests(
    rt: ProdRuntime,
) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_guest_promise_host_operation_errors".parse()?;
    let existing = insert_document(&database, &table).await?.developer_id;
    let missing = missing_id(existing);
    for (operation, manifest_kind, host_operation) in [
        (
            GeneratedDatabaseWriteKind::Patch,
            "databasePatch",
            HostOperation::Patch,
        ),
        (
            GeneratedDatabaseWriteKind::Replace,
            "databaseReplace",
            HostOperation::Replace,
        ),
        (
            GeneratedDatabaseWriteKind::Delete,
            "databaseDelete",
            HostOperation::Delete,
        ),
    ] {
        let manifest = generated_guest_promise_test_manifest_with_runtime_limits(
            ManifestUdfKind::Mutation,
            vec![json!({
                "id": 1,
                "debugName": "hostOperationErrorDocument",
                "operation": {
                    "kind": manifest_kind,
                    "tableName": table,
                },
            })],
            WASM_PAGE_BYTES,
            1_000_000,
            16,
        )?;
        let request = || {
            json!({
                "id": missing.encode(),
                "value": { "marker": "not-written" },
            })
        };
        let expected_host_operation_error = HostOperationErrorV1::NonexistentDocument {
            operation: host_operation,
            document_id: missing,
        };

        for mode in [
            GeneratedGuestPromiseHostOperationErrorMode::Direct,
            GeneratedGuestPromiseHostOperationErrorMode::RethrowSame,
        ] {
            let routed = generated_test_routed_module_from_bytes(
                Arc::clone(&manifest),
                generated_guest_promise_host_operation_error_test_module(mode, operation),
            )?;
            let mut state = generated_test_state(
                rt.clone(),
                database.begin_system().await?,
                Arc::clone(&manifest),
                request(),
                UdfType::Mutation,
                UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                CancellationSignal::new_for_test(),
                None,
                Arc::new(GateMetrics::default()),
                None,
            )
            .await?;
            arm_generated_timeout(
                rt.clone(),
                &mut state,
                Duration::from_secs(5),
                *DATABASE_UDF_SYSTEM_TIMEOUT,
            )?;
            let output = execute_generated(routed, state, None, false).await?;
            assert_eq!(
                output.invocation.outcome,
                InvocationOutcome::DeveloperError("direct".to_owned())
            );
            assert_eq!(
                output.invocation.host_operation_error,
                Some(expected_host_operation_error)
            );
            drop(output.invocation.transaction);
        }

        let wrapped_routed = generated_test_routed_module_from_bytes(
            Arc::clone(&manifest),
            generated_guest_promise_host_operation_error_test_module(
                GeneratedGuestPromiseHostOperationErrorMode::CaughtAndWrapped,
                operation,
            ),
        )?;
        let mut wrapped_state = generated_test_state(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            request(),
            UdfType::Mutation,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            CancellationSignal::new_for_test(),
            None,
            Arc::new(GateMetrics::default()),
            None,
        )
        .await?;
        arm_generated_timeout(
            rt.clone(),
            &mut wrapped_state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let wrapped = execute_generated(wrapped_routed, wrapped_state, None, false).await?;
        assert_eq!(
            wrapped.invocation.outcome,
            InvocationOutcome::DeveloperError("wrapped".to_owned())
        );
        assert_eq!(wrapped.invocation.host_operation_error, None);
        drop(wrapped.invocation.transaction);

        let caught_routed = generated_test_routed_module_from_bytes(
            Arc::clone(&manifest),
            generated_guest_promise_host_operation_error_test_module(
                GeneratedGuestPromiseHostOperationErrorMode::CaughtSuccess,
                operation,
            ),
        )?;
        let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
            hard_instance_ceiling: 1,
            soft_budget_bytes: 8 * WASM_PAGE_BYTES,
            hard_budget_bytes: 12 * WASM_PAGE_BYTES,
            safety_reserve_bytes: WASM_PAGE_BYTES,
            unattributed_bytes_per_slot: WASM_PAGE_BYTES,
            cold_peak_growth_bytes: WASM_PAGE_BYTES,
            warm_idle_target: 0,
            maximum_idle_age: Duration::from_secs(1),
            pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
            pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
        })?;
        controller.set_pressure_for_test(BackendPressure::Healthy {
            headroom_bytes: 8 * WASM_PAGE_BYTES,
        });
        let metrics = Arc::new(GateMetrics::default());
        let route_identity = caught_routed.route_identity.clone();
        let mut caught_state = generated_test_state(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            request(),
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
                maximum_guest_memory_bytes: WASM_PAGE_BYTES,
            }),
        )
        .await?;
        arm_generated_timeout(
            rt.clone(),
            &mut caught_state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let mut caught =
            execute_generated(Arc::clone(&caught_routed), caught_state, None, true).await?;
        assert_eq!(caught.invocation.outcome, InvocationOutcome::Success);
        assert_eq!(caught.invocation.host_operation_error, None);
        assert_eq!(caught.invocation.function_result, Some(JsonValue::Null));
        assert_eq!(
            caught
                .reusable_instance
                .as_ref()
                .context("caught host-operation error did not retain its runtime")?
                .store
                .data()
                .generated
                .as_ref()
                .context("caught host-operation error lost generated state")?
                .async_operations
                .host_operation_error_count(),
            0
        );
        let first_runtime_id = caught
            .reusable_instance
            .as_ref()
            .context("caught host-operation error lost its retained runtime")?
            .id;
        let retained = retain_generated_test_runtime(&mut caught)?;
        drop(caught.invocation.transaction);

        let mut reused_state = generated_test_state(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            request(),
            UdfType::Mutation,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            CancellationSignal::new_for_test(),
            None,
            Arc::clone(&metrics),
            Some(GeneratedTestMemorySetup {
                controller,
                route_identity,
                existing_slot: Some(retained.memory_slot_id),
                values: Some(retained.values),
                maximum_guest_memory_bytes: WASM_PAGE_BYTES,
            }),
        )
        .await?;
        arm_generated_timeout(
            rt.clone(),
            &mut reused_state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let mut reused =
            execute_generated(caught_routed, reused_state, Some(retained.instance), true).await?;
        assert_eq!(reused.invocation.outcome, InvocationOutcome::Success);
        assert_eq!(reused.invocation.host_operation_error, None);
        assert_eq!(
            reused
                .reusable_instance
                .as_ref()
                .context("recovered host-operation error did not reuse its runtime")?
                .id,
            first_runtime_id
        );
        assert_eq!(
            reused
                .reusable_instance
                .as_ref()
                .context("recovered host-operation error lost generated state")?
                .store
                .data()
                .generated
                .as_ref()
                .context("recovered host-operation error lost invocation state")?
                .async_operations
                .host_operation_error_count(),
            0
        );
        let reusable_instance = reused
            .reusable_instance
            .take()
            .context("recovered host-operation error lost its reusable runtime")?;
        discard_generated_instance(reusable_instance).await?;
        reused
            .memory_permit
            .take()
            .context("recovered host-operation error lost its memory permit")?
            .finish(reused.terminal_memory_outcome, false);
        drop(reused.invocation.transaction);
    }

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_capability_database_get_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_capability_get_documents".parse()?;
    let id = insert_document(&database, &table).await?.developer_id;
    let request_envelope = serde_json::to_vec(&json!({
        "id": id.encode(),
        "kind": "dbGet",
        "version": 4,
    }))?;
    GuestCapabilityRequestCodec::decode(&request_envelope, request_envelope.len())?;
    let manifest = generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        1 << 20,
        1_000_000_000,
        1,
    )?;
    assert!(manifest.imported_operations().is_empty());
    let mut routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        generated_capability_async_operation_test_module(&request_envelope, 0),
    )?;
    Arc::get_mut(&mut routed)
        .context("capability test routed module was unexpectedly shared")?
        .entry_selector = Some(0);
    let route_identity = routed.route_identity.clone();
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 2,
        soft_budget_bytes: 8 * WASM_PAGE_BYTES,
        hard_budget_bytes: 12 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 1,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let metrics = Arc::new(GateMetrics::default());
    let request = json!({
        "capabilityRequest": {
            "version": 4,
            "kind": "dbGet",
            "id": id.encode(),
        },
    });
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
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut hit = execute_generated(Arc::clone(&routed), state, None, true).await?;
    assert_eq!(hit.invocation.outcome, InvocationOutcome::Success);
    let first_capability_identity = metrics.previous_capability_identity.load(Ordering::SeqCst);
    assert_ne!(first_capability_identity, 0);
    assert_eq!(
        hit.invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("marker")),
        Some(&json!("backend-gate-document"))
    );
    assert_eq!(hit.invocation.read_accounting.documents, 1);
    assert!(hit.invocation.read_accounting.bytes > 0);
    assert!(hit.invocation.read_accounting.intervals > 0);
    assert_eq!(hit.invocation.opaque_live_handles, 0);
    assert_eq!(hit.invocation.opaque_current_bytes, 0);
    let mut instance = hit
        .reusable_instance
        .take()
        .context("capability invocation did not retain its runtime")?;
    let completed = instance
        .store
        .data_mut()
        .generated
        .take()
        .context("capability invocation lost its completed state")?;
    assert_eq!(completed.operation_count, 1);
    assert!(completed.capability_bridge.is_revoked());
    let retained_values = completed.values;
    let first_instance_id = instance.id;
    let first_permit = hit
        .memory_permit
        .take()
        .context("capability invocation lost its memory permit")?;
    let memory_slot_id = first_permit.slot_id();
    drop(hit.invocation.transaction);
    first_permit.finish(hit.terminal_memory_outcome, true);

    let mut second_state = generated_test_state(
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
            existing_slot: Some(memory_slot_id),
            values: Some(retained_values),
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut second_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    assert_eq!(instance.id, first_instance_id);
    let second = execute_generated(Arc::clone(&routed), second_state, Some(instance), true).await?;
    assert_eq!(second.invocation.outcome, InvocationOutcome::Success);
    let second_capability_identity = metrics.previous_capability_identity.load(Ordering::SeqCst);
    assert_ne!(second_capability_identity, first_capability_identity);
    assert_eq!(second.invocation.read_accounting.documents, 1);
    assert!(second.invocation.read_accounting.bytes > 0);
    assert!(second.invocation.read_accounting.intervals > 0);
    assert_eq!(second.invocation.opaque_live_handles, 0);
    assert_eq!(second.invocation.opaque_current_bytes, 0);
    assert_eq!(second.invocation.operation_count, 1);
    assert!(second.invocation.capability_revoked);
    assert!(second.invocation.runtime_reuse_contaminated);
    assert!(second.reusable_instance.is_none());
    assert!(second.memory_permit.is_none());
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 1);
    let retired = controller.snapshot_for_test(&memory_identity);
    assert_eq!(retired.active_instances, 0);
    assert_eq!(retired.idle_instances, 0);
    assert_eq!(retired.evicting_instances, 0);
    assert_eq!(retired.retained_baseline_bytes, 0);
    assert_eq!(retired.unattributed_allowance_bytes, 0);
    drop(second.invocation.transaction);

    let mut third_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        request,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 4 * WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut third_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut third = execute_generated(Arc::clone(&routed), third_state, None, true).await?;
    assert_eq!(third.invocation.outcome, InvocationOutcome::Success);
    let third_capability_identity = metrics.previous_capability_identity.load(Ordering::SeqCst);
    assert_ne!(third_capability_identity, first_capability_identity);
    assert_ne!(third_capability_identity, second_capability_identity);
    assert_eq!(
        third
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("marker")),
        Some(&json!("backend-gate-document"))
    );
    assert_eq!(third.invocation.read_accounting.documents, 1);
    assert!(third.invocation.read_accounting.bytes > 0);
    assert!(third.invocation.read_accounting.intervals > 0);
    assert_eq!(third.invocation.operation_count, 1);
    assert!(third.invocation.capability_revoked);
    assert!(!third.invocation.runtime_reuse_contaminated);
    let third_instance = third
        .reusable_instance
        .take()
        .context("third capability invocation did not retain its runtime")?;
    assert_ne!(third_instance.id, first_instance_id);
    let third_permit = third
        .memory_permit
        .take()
        .context("third capability invocation lost its memory permit")?;
    assert_ne!(third_permit.slot_id(), memory_slot_id);
    drop(third.invocation.transaction);
    discard_generated_instance(third_instance).await?;
    third_permit.finish(third.terminal_memory_outcome, false);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 2);

    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    generated_state_mut(&mut state)?.capability_bridge.issue()?;
    let stale_capability_handle = generated_state(&state)?.capability_bridge.handle()?;
    generated_state_mut(&mut state)?
        .capability_bridge
        .revoke()?;
    generated_state_mut(&mut state)?.capability_bridge.reset()?;
    let current_capability_handle = generated_state(&state)?.capability_bridge.handle()?;
    assert_ne!(current_capability_handle, stale_capability_handle);

    let invalid_request_handle = generated_state_mut(&mut state)?.values.insert_json(json!({
        "version": 1,
        "kind": "dbGet",
        "table": table,
        "id": id.encode(),
        "operationId": 1,
    }))?;
    assert!(start_generated_capability_operation(
        &mut state,
        current_capability_handle,
        invalid_request_handle.to_abi(),
    )
    .is_err());
    assert!(generated_state(&state)?
        .values
        .get_json(invalid_request_handle)
        .is_err());
    assert!(!generated_state(&state)?.runtime_reuse_contaminated);

    let stale_request_handle = generated_state_mut(&mut state)?.values.insert_json(json!({
        "version": 1,
        "kind": "dbGet",
        "table": table,
        "id": id.encode(),
    }))?;
    assert_eq!(
        start_generated_capability_operation(&mut state, 0, stale_request_handle.to_abi(),)?,
        -1
    );
    assert!(generated_state(&state)?
        .values
        .get_json(stale_request_handle)
        .is_ok());
    assert!(!generated_state(&state)?.runtime_reuse_contaminated);

    assert_eq!(
        start_generated_capability_operation(
            &mut state,
            stale_capability_handle,
            stale_request_handle.to_abi(),
        )?,
        -1
    );
    let generated = generated_state(&state)?;
    assert!(generated.runtime_reuse_contaminated);
    assert_eq!(generated.operation_count, 0);
    assert!(generated.async_operations.is_empty());
    assert!(generated.values.get_json(invalid_request_handle).is_err());
    assert!(generated.values.get_json(stale_request_handle).is_ok());
    let execution_size = state.provider.tx_for_initialization()?.execution_size();
    assert_eq!(execution_size.read_size.total_document_count, 0);
    assert_eq!(execution_size.read_size.total_document_size, 0);
    assert_eq!(execution_size.num_intervals, 0);
    generated_state_mut(&mut state)?.values.cleanup();
    drop(state);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_capability_database_query_terminal_tests(
    rt: ProdRuntime,
) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_capability_query_documents".parse()?;
    let mut transaction = database.begin_system().await?;
    let index_name = IndexName::new(table.clone(), IndexDescriptor::new("by_tenant")?)?;
    IndexModel::new(&mut transaction)
        .add_application_index(
            TableNamespace::root_component(),
            IndexMetadata::new_enabled(
                index_name,
                IndexedFields::try_from(vec!["tenant".parse()?])?,
            ),
        )
        .await?;
    for (tenant, marker) in [
        ("first", "first-result"),
        ("unique", "unique-result"),
        ("duplicate", "duplicate-first"),
        ("duplicate", "duplicate-second"),
    ] {
        SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
            .insert_metadata(&table, obj!("tenant" => tenant, "marker" => marker)?)
            .await?;
    }
    database
        .commit_with_write_source(transaction, "generated_capability_query_setup")
        .await?;

    let manifest = generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        1 << 20,
        1_000_000,
        8,
    )?;
    let request = |tenant: &str, terminal: &str| {
        GuestNativeValueCodec::encode(
            json!({
                "version": 4,
                "kind": "dbQuery",
                "table": table.to_string(),
                "source": {
                    "type": "indexRange",
                    "index": "by_tenant",
                    "constraints": [{
                        "operator": "eq",
                        "field": "tenant",
                        "value": tenant,
                    }],
                },
                "operators": [],
                "order": "asc",
                "terminal": terminal,
            }),
            MAX_REQUEST_BYTES,
        )
    };

    let first_metrics = Arc::new(GateMetrics::default());
    let first = execute_generated_capability_async_operation_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        request("first", "first")?,
        0,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&first_metrics),
    )
    .await?;
    assert_eq!(first.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        first
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("marker")),
        Some(&json!("first-result"))
    );
    assert_eq!(first.invocation.operation_count, 3);
    assert_eq!(first_metrics.read_completed.load(Ordering::SeqCst), 2);
    assert_eq!(first.invocation.read_accounting.documents, 1);
    assert_eq!(first.invocation.opaque_live_handles, 0);
    assert_eq!(first.invocation.opaque_current_bytes, 0);
    assert_eq!(
        generated_capability_query_trace(&first)?,
        vec![
            (
                LogicalHostOperation::DatabaseQueryStream,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryStreamNext,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryStreamNext,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryCleanup,
                LogicalHostOperationStatus::Success,
            ),
        ]
    );
    drop(first.invocation.transaction);

    let unique_metrics = Arc::new(GateMetrics::default());
    let unique = execute_generated_capability_async_operation_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        request("unique", "unique")?,
        0,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&unique_metrics),
    )
    .await?;
    assert_eq!(unique.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        unique
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("marker")),
        Some(&json!("unique-result"))
    );
    assert_eq!(unique.invocation.operation_count, 3);
    assert_eq!(unique_metrics.read_completed.load(Ordering::SeqCst), 2);
    assert_eq!(unique.invocation.read_accounting.documents, 1);
    assert_eq!(unique.invocation.opaque_live_handles, 0);
    assert_eq!(unique.invocation.opaque_current_bytes, 0);
    assert_eq!(
        generated_capability_query_trace(&unique)?,
        vec![
            (
                LogicalHostOperation::DatabaseQueryStream,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryStreamNext,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryStreamNext,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryCleanup,
                LogicalHostOperationStatus::Success,
            ),
        ]
    );
    drop(unique.invocation.transaction);

    let duplicate_metrics = Arc::new(GateMetrics::default());
    let duplicate = execute_generated_capability_async_operation_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        request("duplicate", "unique")?,
        1,
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&duplicate_metrics),
    )
    .await?;
    assert_eq!(duplicate.invocation.outcome, InvocationOutcome::Success);
    assert!(duplicate
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_str)
        .is_some_and(|message| message.contains("unique() query returned more than one result")));
    assert_eq!(duplicate.invocation.operation_count, 4);
    assert_eq!(duplicate_metrics.read_completed.load(Ordering::SeqCst), 3);
    assert_eq!(duplicate.invocation.read_accounting.documents, 2);
    assert_eq!(duplicate.invocation.opaque_live_handles, 0);
    assert_eq!(duplicate.invocation.opaque_current_bytes, 0);
    assert_eq!(
        generated_capability_query_trace(&duplicate)?,
        vec![
            (
                LogicalHostOperation::DatabaseQueryStream,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryStreamNext,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryStreamNext,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryStreamNext,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryCleanup,
                LogicalHostOperationStatus::Success,
            ),
        ]
    );
    drop(duplicate.invocation.transaction);

    let cancellation = CancellationSignal::new_for_test();
    let (control, mut host) = read_control();
    let cancellation_metrics = Arc::new(GateMetrics::default());
    let execution = tokio::spawn(execute_generated_capability_async_operation_test_module(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        request("first", "first")?,
        0,
        cancellation.clone(),
        Some(control),
        Arc::clone(&cancellation_metrics),
    ));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("capability query did not become pending")??;
    cancellation.cancel_for_test();
    let cancelled = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("capability query cancellation did not complete")???;
    assert!(cancelled.cancelled);
    assert!(cancelled.invocation.function_result.is_none());
    assert_eq!(cancelled.invocation.operation_count, 2);
    assert_eq!(
        cancellation_metrics.read_cancelled.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        cancellation_metrics
            .database_query_starts
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(cancelled.invocation.opaque_live_handles, 0);
    assert_eq!(cancelled.invocation.opaque_current_bytes, 0);
    assert!(host
        .release
        .take()
        .context("capability query cancellation release missing")?
        .send(())
        .is_err());
    drop(cancelled.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_native_capability_jsi_tests(
    rt: ProdRuntime,
    package_directory: PathBuf,
    expectation: &NativeCapabilityTestExpectation,
) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;
    let engine = shared_generated_engine()?;
    let runtime = generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?;
    let load_package = |selector: &NativeCapabilityTestSelector| {
        load_capability_entry_package_for_compatibility_test(
            &package_directory,
            &expectation.package_key,
            &selector.entry_id,
            &selector.route_id,
            &format!("{:016x}", selector.entry_selector),
            &selector.entry_path,
            &selector.runtime_module_path,
            &selector.handler_export_name,
            selector.handler_udf_kind.manifest_udf_kind(),
            "public",
            &runtime,
        )
    };
    let primary_package = load_package(&expectation.primary)?;
    let alternate_package = load_package(&expectation.alternate)?;
    anyhow::ensure!(
        primary_package.package_key == expectation.package_key
            && alternate_package.package_key == expectation.package_key,
        "native capability package loader changed the authenticated package key"
    );
    let module_controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: expectation.package_key.clone(),
        generation_sha256: expectation.package_key.clone(),
        package_key: expectation.package_key.clone(),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    anyhow::ensure!(
        !GENERATED_ROUTED_MODULES
            .lock()
            .contains_key(&route_identity),
        "native capability package route was already cached"
    );
    let routed = cache_generated_routed_module(
        &module_controller,
        primary_package,
        route_identity.clone(),
        Arc::clone(&engine),
        None,
    )?;
    let sibling_routed = cache_generated_routed_module(
        &module_controller,
        alternate_package,
        route_identity.clone(),
        engine,
        None,
    )?;
    anyhow::ensure!(
        Arc::ptr_eq(&routed, &sibling_routed)
            && routed.entry_selector == Some(expectation.primary.entry_selector)
            && routed.package_identity.requires_entry_selector()
            && routed.manifest.as_ref() == &expectation.execution
            && routed.manifest.value_mode() == ValueMode::GuestNativeJson
            && routed.manifest.effect_execution_mode()
                == EffectExecutionMode::GuestPromiseEventLoop
            && routed.manifest.imported_operations().is_empty(),
        "native capability package loader changed the authenticated execution contract"
    );
    validate_generated_module_contract_with_imports(
        &routed.module,
        &routed.manifest,
        &routed.permitted_conditional_convex_imports,
        true,
    )?;
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_capability_get_documents".parse()?;
    let id = insert_document_with_sequence(&database, &table, ConvexValue::Float64(1.0))
        .await?
        .developer_id;
    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &route_identity,
    );
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 2,
        soft_budget_bytes: 960 * WASM_PAGE_BYTES,
        hard_budget_bytes: 1024 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: 512 * WASM_PAGE_BYTES,
        warm_idle_target: 1,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 1536 * WASM_PAGE_BYTES,
    });
    let metrics = Arc::new(GateMetrics::default());
    metrics
        .formatter_initialization_failure_arms
        .store(1, Ordering::SeqCst);
    let mut failed_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        json!({ "id": id.encode(), "mode": "forced-initialization-failure" }),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: None,
            memory_identity: memory_identity.clone(),
            values: None,
        },
        Arc::clone(&metrics),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut failed_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut failed = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.primary.entry_selector),
        failed_state,
        None,
        true,
    )
    .await?;
    assert_eq!(failed.invocation.outcome, InvocationOutcome::SystemError);
    assert_eq!(
        failed.partial_initialization_trace,
        Some(expectation.partial_initialization_failure.failure_trace)
    );
    let failed_runtime_id = failed.runtime_id;
    assert!(failed.reusable_instance.is_none());
    assert!(failed.memory_permit.is_none());
    assert_eq!(failed.invocation.opaque_live_handles, 0);
    assert_eq!(failed.invocation.opaque_current_bytes, 0);
    assert_eq!(failed.invocation.operation_count, 0);
    assert!(failed.invocation.capability_revoked);
    assert!(!failed.invocation.runtime_reuse_contaminated);
    let failure = failed
        .system_error
        .take()
        .context("partial-initialization failure lost its classified error")?;
    assert!(format!("{failure:#}").contains(&format!(
        "unexpected status {}",
        expectation.partial_initialization_failure.failure_status
    )));
    drop(failed.invocation.transaction);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.store_drops.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.generated_pool_hits.load(Ordering::SeqCst), 0);
    assert_eq!(metrics.generated_pool_misses.load(Ordering::SeqCst), 0);
    assert!(!GENERATED_ROUTED_INSTANCE_POOL
        .lock()
        .iter()
        .any(|instance| {
            instance
                .downcast_ref::<GeneratedReusableInstance<ProdRuntime>>()
                .is_some_and(|instance| instance.route_identity == route_identity)
        }));
    let released_after_failure = controller.snapshot_for_test(&memory_identity);
    assert_eq!(released_after_failure.active_instances, 0);
    assert_eq!(released_after_failure.idle_instances, 0);
    assert_eq!(released_after_failure.evicting_instances, 0);
    assert_eq!(released_after_failure.retained_baseline_bytes, 0);
    assert_eq!(released_after_failure.unattributed_allowance_bytes, 0);

    let mut first_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        json!({ "id": id.encode(), "mode": "retain" }),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: None,
            memory_identity: memory_identity.clone(),
            values: None,
        },
        Arc::clone(&metrics),
    )
    .await?;
    assert!(generated_state(&first_state)?
        .capability_bridge
        .is_unissued());
    arm_generated_timeout(
        rt.clone(),
        &mut first_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut first = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.primary.entry_selector),
        first_state,
        None,
        true,
    )
    .await?;
    assert_ne!(first.runtime_id, failed_runtime_id);
    assert_eq!(
        first.invocation.outcome,
        InvocationOutcome::Success,
        "native capability first invocation failed: {:?}",
        first.system_error
    );
    assert_eq!(
        first
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("selectedExport"))
            .and_then(JsonValue::as_str),
        Some(expectation.primary.handler_export_name.as_str()),
        "unexpected first native capability result: {:?}",
        first.invocation.function_result
    );
    assert_eq!(
        first
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("document"))
            .and_then(|value| value.get("marker")),
        Some(&json!("backend-gate-document")),
        "unexpected first native capability result: {:?}",
        first.invocation.function_result
    );
    assert_eq!(
        first
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("modeSeen")),
        Some(&json!("retain"))
    );
    assert_eq!(first.invocation.read_accounting.documents, 1);
    assert!(first.invocation.read_accounting.bytes > 0);
    assert!(first.invocation.read_accounting.intervals > 0);
    assert_eq!(first.invocation.opaque_live_handles, 0);
    assert_eq!(first.invocation.opaque_current_bytes, 0);
    assert_eq!(first.invocation.operation_count, 1);
    assert!(first.invocation.capability_revoked);
    assert!(!first.invocation.runtime_reuse_contaminated);
    let retained = retain_generated_test_runtime(&mut first)?;
    let first_instance_id = retained.instance.id;
    let mut instance = retained.instance;
    let memory_slot_id = retained.memory_slot_id;
    let retained_values = retained.values;
    drop(first.invocation.transaction);

    let mut second_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        json!({ "id": id.encode(), "mode": "alternate" }),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: Some(memory_slot_id),
            memory_identity: memory_identity.clone(),
            values: Some(retained_values),
        },
        Arc::clone(&metrics),
    )
    .await?;
    assert!(generated_state(&second_state)?
        .capability_bridge
        .is_unissued());
    arm_generated_timeout(
        rt.clone(),
        &mut second_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    assert_eq!(instance.id, first_instance_id);
    let mut second = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.alternate.entry_selector),
        second_state,
        Some(instance),
        true,
    )
    .await?;
    assert_eq!(
        second.partial_initialization_trace,
        Some(expectation.partial_initialization_failure.recovery_trace)
    );
    assert_eq!(
        second.invocation.outcome,
        InvocationOutcome::Success,
        "native capability second invocation failed: {:?}",
        second.system_error
    );
    assert_eq!(
        second
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("selectedExport"))
            .and_then(JsonValue::as_str),
        Some(expectation.alternate.handler_export_name.as_str())
    );
    assert_eq!(
        second
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("alternate"))
            .and_then(|value| value.get("marker")),
        Some(&json!("backend-gate-document"))
    );
    assert_eq!(
        second
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("modeSeen")),
        Some(&json!("alternate"))
    );
    assert_eq!(second.invocation.read_accounting.documents, 1);
    assert!(second.invocation.read_accounting.bytes > 0);
    assert!(second.invocation.read_accounting.intervals > 0);
    assert_eq!(second.invocation.opaque_live_handles, 0);
    assert_eq!(second.invocation.opaque_current_bytes, 0);
    assert_eq!(second.invocation.operation_count, 1);
    assert!(second.invocation.capability_revoked);
    assert!(!second.invocation.runtime_reuse_contaminated);
    let retained = retain_generated_test_runtime(&mut second)?;
    instance = retained.instance;
    assert_eq!(instance.id, first_instance_id);
    assert_eq!(retained.memory_slot_id, memory_slot_id);
    let retained_values = retained.values;
    drop(second.invocation.transaction);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.store_drops.load(Ordering::SeqCst), 2);

    let mut third_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        json!({ "id": id.encode(), "mode": "retain" }),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: Some(memory_slot_id),
            memory_identity: memory_identity.clone(),
            values: Some(retained_values),
        },
        Arc::clone(&metrics),
    )
    .await?;
    assert!(generated_state(&third_state)?
        .capability_bridge
        .is_unissued());
    arm_generated_timeout(
        rt.clone(),
        &mut third_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    assert_eq!(instance.id, first_instance_id);
    let mut third = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.primary.entry_selector),
        third_state,
        Some(instance),
        true,
    )
    .await?;
    assert_eq!(
        third.invocation.outcome,
        InvocationOutcome::Success,
        "native capability third invocation failed: {:?}",
        third.system_error
    );
    assert_eq!(
        third
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("selectedExport"))
            .and_then(JsonValue::as_str),
        Some(expectation.primary.handler_export_name.as_str())
    );
    assert_eq!(
        third
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("document"))
            .and_then(|value| value.get("marker")),
        Some(&json!("backend-gate-document"))
    );
    assert_eq!(third.invocation.read_accounting.documents, 1);
    assert!(third.invocation.read_accounting.bytes > 0);
    assert!(third.invocation.read_accounting.intervals > 0);
    assert_eq!(third.invocation.opaque_live_handles, 0);
    assert_eq!(third.invocation.opaque_current_bytes, 0);
    assert_eq!(third.invocation.operation_count, 1);
    assert!(third.invocation.capability_revoked);
    assert!(!third.invocation.runtime_reuse_contaminated);
    instance = third
        .reusable_instance
        .take()
        .context("third native capability invocation did not retain its runtime")?;
    assert_eq!(instance.id, first_instance_id);
    let completed = generated_state(instance.store.data())?;
    assert_eq!(completed.operation_count, 1);
    assert!(completed.capability_bridge.is_revoked());
    assert!(completed.async_operations.is_empty());
    assert_eq!(completed.values.live_handle_count(), 0);
    assert_eq!(completed.values.accounting().current_bytes, 0);
    assert!(completed.host_secret_values.is_empty());
    let third_permit = third
        .memory_permit
        .take()
        .context("third native capability invocation lost its memory permit")?;
    assert_eq!(third_permit.slot_id(), memory_slot_id);
    drop(third.invocation.transaction);
    assert_eq!(metrics.store_drops.load(Ordering::SeqCst), 3);
    discard_generated_instance(instance).await?;
    third_permit.finish(third.terminal_memory_outcome, false);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 2);
    assert_eq!(metrics.store_drops.load(Ordering::SeqCst), 4);
    assert!(!GENERATED_ROUTED_INSTANCE_POOL
        .lock()
        .iter()
        .any(|instance| {
            instance
                .downcast_ref::<GeneratedReusableInstance<ProdRuntime>>()
                .is_some_and(|instance| instance.route_identity == route_identity)
        }));
    let retired = controller.snapshot_for_test(&memory_identity);
    assert_eq!(retired.active_instances, 0);
    assert_eq!(retired.idle_instances, 0);
    assert_eq!(retired.evicting_instances, 0);
    assert_eq!(retired.retained_baseline_bytes, 0);
    assert_eq!(retired.unattributed_allowance_bytes, 0);
    database.shutdown().await?;

    let removed = GENERATED_ROUTED_MODULES
        .lock()
        .remove(
            &route_identity,
            "module_cache_evicted_native_capability_acceptance",
        )
        .context("native capability package was not cached")?;
    anyhow::ensure!(
        Arc::ptr_eq(&removed, &routed),
        "native capability cleanup removed an unexpected package"
    );
    drop(removed);
    drop(sibling_routed);
    drop(routed);
    assert_eq!(
        module_controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        0
    );
    Ok(())
}

#[cfg(test)]
fn assert_generated_official_output_chunk_native_validation_result(
    output: &GeneratedExecutionOutput<ProdRuntime>,
    expected_result: &str,
    behavior: &OfficialOutputChunkNativeValidationReport,
    stage: &str,
) {
    assert_eq!(
        output.invocation.outcome,
        InvocationOutcome::Success,
        "official-output chunk {stage} invocation failed: {:?}",
        output.system_error
    );
    assert_eq!(
        output.invocation.function_result.as_ref(),
        Some(&json!(expected_result)),
        "official-output chunk {stage} invocation returned an unexpected result"
    );
    assert!(
        output
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_str)
            .is_some_and(|result| result.contains(&behavior.unselected_top_level_trap_marker)),
        "official-output chunk {stage} invocation executed the unselected top-level trap"
    );
    assert_eq!(output.invocation.operation_count, 0);
    assert_eq!(output.invocation.opaque_live_handles, 0);
    assert_eq!(output.invocation.opaque_current_bytes, 0);
    assert!(output.invocation.capability_revoked);
    assert!(!output.invocation.runtime_reuse_contaminated);
}

#[cfg(test)]
fn take_generated_official_output_chunk_retained_values(
    instance: &mut GeneratedReusableInstance<ProdRuntime>,
    stage: &str,
) -> anyhow::Result<OpaqueValueTable> {
    let completed = instance
        .store
        .data_mut()
        .generated
        .take()
        .with_context(|| {
            format!("official-output chunk {stage} invocation lost its completed state")
        })?;
    assert_eq!(completed.operation_count, 0);
    assert!(completed.async_operations.is_empty());
    assert_eq!(completed.values.live_handle_count(), 0);
    assert_eq!(completed.values.accounting().current_bytes, 0);
    assert!(completed.host_secret_values.is_empty());
    Ok(completed.values)
}

#[cfg(test)]
async fn run_generated_official_output_chunk_descriptor_smoke(
    rt: ProdRuntime,
    package_directory: PathBuf,
    expectation: &OfficialOutputChunkSmokeExpectation,
) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let engine = shared_generated_engine()?;
    let runtime = generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?;
    let load_package = |selector: &OfficialOutputChunkSmokeSelector| {
        load_capability_entry_package_for_compatibility_test(
            &package_directory,
            &expectation.package_key,
            &selector.entry_id,
            &selector.route_id,
            &format!("{:016x}", selector.entry_selector),
            &selector.entry_path,
            &format!("{}.js", selector.module_path),
            &selector.export_name,
            ManifestUdfKind::Query,
            "public",
            &runtime,
        )
    };
    let primary_package = load_package(&expectation.primary)?;
    let secondary_package = load_package(&expectation.secondary)?;
    anyhow::ensure!(
        primary_package.package_key == expectation.package_key
            && secondary_package.package_key == expectation.package_key,
        "official-output chunk package loader changed the authenticated package key"
    );

    let module_controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: expectation.package_key.clone(),
        generation_sha256: expectation.package_key.clone(),
        package_key: expectation.package_key.clone(),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    anyhow::ensure!(
        !GENERATED_ROUTED_MODULES
            .lock()
            .contains_key(&route_identity),
        "official-output chunk package route was already cached"
    );
    let routed = cache_generated_routed_module(
        &module_controller,
        primary_package,
        route_identity.clone(),
        Arc::clone(&engine),
        None,
    )?;
    let sibling_routed = cache_generated_routed_module(
        &module_controller,
        secondary_package,
        route_identity.clone(),
        engine,
        None,
    )?;
    anyhow::ensure!(
        Arc::ptr_eq(&routed, &sibling_routed)
            && routed.entry_selector == Some(expectation.primary.entry_selector)
            && routed.package_identity.requires_entry_selector()
            && routed.manifest.value_mode() == ValueMode::GuestNativeJson
            && routed.manifest.effect_execution_mode()
                == EffectExecutionMode::GuestPromiseEventLoop
            && routed.manifest.imported_operations().is_empty(),
        "official-output chunk loader changed the authenticated package execution contract"
    );
    validate_generated_module_contract_with_imports(
        &routed.module,
        &routed.manifest,
        &routed.permitted_conditional_convex_imports,
        true,
    )?;

    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &route_identity,
    );
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 2,
        soft_budget_bytes: 960 * WASM_PAGE_BYTES,
        hard_budget_bytes: 1024 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: 512 * WASM_PAGE_BYTES,
        warm_idle_target: 1,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 1536 * WASM_PAGE_BYTES,
    });
    let metrics = Arc::new(GateMetrics::default());
    let database = new_test_database(rt.clone()).await?;

    let mut first_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        json!({}),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: None,
            memory_identity: memory_identity.clone(),
            values: None,
        },
        Arc::clone(&metrics),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut first_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut first = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.primary.entry_selector),
        first_state,
        None,
        true,
    )
    .await?;
    assert_generated_official_output_chunk_native_validation_result(
        &first,
        &expectation.primary.expected_result,
        &expectation.behavior,
        "initial primary",
    );
    assert!(
        expectation
            .primary
            .expected_result
            .contains(&expectation.behavior.common_js_cycle_marker),
        "initial primary invocation did not execute the CommonJS cycle"
    );
    let mut instance = first
        .reusable_instance
        .take()
        .context("official-output chunk initial primary invocation did not retain its runtime")?;
    let first_instance_id = instance.id;
    let retained_values =
        take_generated_official_output_chunk_retained_values(&mut instance, "initial primary")?;
    let first_permit = first
        .memory_permit
        .take()
        .context("official-output chunk initial primary invocation lost its memory permit")?;
    let memory_slot_id = first_permit.slot_id();
    drop(first.invocation.transaction);
    first_permit.finish(first.terminal_memory_outcome, true);

    let mut second_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        json!({}),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: Some(memory_slot_id),
            memory_identity: memory_identity.clone(),
            values: Some(retained_values),
        },
        Arc::clone(&metrics),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut second_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    assert_eq!(instance.id, first_instance_id);
    let mut second = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.secondary.entry_selector),
        second_state,
        Some(instance),
        true,
    )
    .await?;
    assert_generated_official_output_chunk_native_validation_result(
        &second,
        &expectation.secondary.expected_result,
        &expectation.behavior,
        "initial secondary",
    );
    instance = second
        .reusable_instance
        .take()
        .context("official-output chunk initial secondary invocation did not retain its runtime")?;
    assert_eq!(instance.id, first_instance_id);
    let retained_values =
        take_generated_official_output_chunk_retained_values(&mut instance, "initial secondary")?;
    let second_permit = second
        .memory_permit
        .take()
        .context("official-output chunk initial secondary invocation lost its memory permit")?;
    assert_eq!(second_permit.slot_id(), memory_slot_id);
    drop(second.invocation.transaction);
    second_permit.finish(second.terminal_memory_outcome, true);

    let mut third_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        json!({}),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: Some(memory_slot_id),
            memory_identity: memory_identity.clone(),
            values: Some(retained_values),
        },
        Arc::clone(&metrics),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut third_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut third = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.primary.entry_selector),
        third_state,
        Some(instance),
        true,
    )
    .await?;
    assert_generated_official_output_chunk_native_validation_result(
        &third,
        &expectation.behavior.reselected_primary_expected_result,
        &expectation.behavior,
        "reselected primary",
    );
    instance = third.reusable_instance.take().context(
        "official-output chunk reselected primary invocation did not retain its runtime",
    )?;
    assert_eq!(instance.id, first_instance_id);
    let retained_values =
        take_generated_official_output_chunk_retained_values(&mut instance, "reselected primary")?;
    let third_permit = third
        .memory_permit
        .take()
        .context("official-output chunk reselected primary invocation lost its memory permit")?;
    assert_eq!(third_permit.slot_id(), memory_slot_id);
    drop(third.invocation.transaction);
    third_permit.finish(third.terminal_memory_outcome, true);

    let mut deferred_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        expectation
            .behavior
            .deferred_dynamic_import
            .arguments
            .clone(),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: Some(memory_slot_id),
            memory_identity: memory_identity.clone(),
            values: Some(retained_values),
        },
        Arc::clone(&metrics),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut deferred_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut deferred = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.secondary.entry_selector),
        deferred_state,
        Some(instance),
        true,
    )
    .await?;
    assert_generated_official_output_chunk_native_validation_result(
        &deferred,
        &expectation.behavior.deferred_dynamic_import.expected_result,
        &expectation.behavior,
        "deferred dynamic import",
    );
    instance = deferred
        .reusable_instance
        .take()
        .context("official-output chunk deferred dynamic import did not retain its runtime")?;
    assert_eq!(instance.id, first_instance_id);
    let retained_values = take_generated_official_output_chunk_retained_values(
        &mut instance,
        "deferred dynamic import",
    )?;
    let deferred_permit = deferred
        .memory_permit
        .take()
        .context("official-output chunk deferred dynamic import lost its memory permit")?;
    assert_eq!(deferred_permit.slot_id(), memory_slot_id);
    drop(deferred.invocation.transaction);
    deferred_permit.finish(deferred.terminal_memory_outcome, true);

    let mut deferred_repeat_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        expectation
            .behavior
            .deferred_dynamic_import
            .arguments
            .clone(),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: Some(memory_slot_id),
            memory_identity: memory_identity.clone(),
            values: Some(retained_values),
        },
        Arc::clone(&metrics),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut deferred_repeat_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut deferred_repeat = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.secondary.entry_selector),
        deferred_repeat_state,
        Some(instance),
        true,
    )
    .await?;
    assert_generated_official_output_chunk_native_validation_result(
        &deferred_repeat,
        &expectation.behavior.deferred_dynamic_import.expected_result,
        &expectation.behavior,
        "repeated deferred dynamic import",
    );
    instance = deferred_repeat.reusable_instance.take().context(
        "official-output chunk repeated deferred dynamic import did not retain its runtime",
    )?;
    assert_eq!(instance.id, first_instance_id);
    let retained_values = take_generated_official_output_chunk_retained_values(
        &mut instance,
        "repeated deferred dynamic import",
    )?;
    let deferred_repeat_permit = deferred_repeat
        .memory_permit
        .take()
        .context("official-output chunk repeated deferred dynamic import lost its memory permit")?;
    assert_eq!(deferred_repeat_permit.slot_id(), memory_slot_id);
    drop(deferred_repeat.invocation.transaction);
    deferred_repeat_permit.finish(deferred_repeat.terminal_memory_outcome, true);

    let mut late_failure_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        expectation
            .behavior
            .late_initialization_failure
            .arguments
            .clone(),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: Some(memory_slot_id),
            memory_identity: memory_identity.clone(),
            values: Some(retained_values),
        },
        Arc::clone(&metrics),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut late_failure_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let late_failure = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.secondary.entry_selector),
        late_failure_state,
        Some(instance),
        true,
    )
    .await?;
    assert_generated_official_output_chunk_native_validation_result(
        &late_failure,
        &expectation
            .behavior
            .late_initialization_failure
            .expected_result,
        &expectation.behavior,
        "caught late initialization failure",
    );
    assert!(
        late_failure.reusable_instance.is_none() && late_failure.memory_permit.is_none(),
        "caught late initialization failure retained an unusable Store"
    );
    drop(late_failure.invocation.transaction);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 1);
    let retired_after_late_failure = controller.snapshot_for_test(&memory_identity);
    assert_eq!(retired_after_late_failure.active_instances, 0);
    assert_eq!(retired_after_late_failure.idle_instances, 0);
    assert_eq!(retired_after_late_failure.evicting_instances, 0);

    let mut recovered_state = generated_routed_test_state(
        rt.clone(),
        database.begin_system().await?,
        &routed,
        json!({}),
        GeneratedRoutedTestMemorySetup {
            controller: Arc::clone(&controller),
            existing_slot: None,
            memory_identity: memory_identity.clone(),
            values: None,
        },
        Arc::clone(&metrics),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut recovered_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut recovered = execute_generated_with_entry_selector(
        Arc::clone(&routed),
        Some(expectation.primary.entry_selector),
        recovered_state,
        None,
        true,
    )
    .await?;
    assert_generated_official_output_chunk_native_validation_result(
        &recovered,
        &expectation.primary.expected_result,
        &expectation.behavior,
        "recovered primary",
    );
    let recovered_instance = recovered
        .reusable_instance
        .take()
        .context("official-output chunk recovered primary invocation did not retain its runtime")?;
    assert_ne!(recovered_instance.id, first_instance_id);
    let recovered_permit = recovered
        .memory_permit
        .take()
        .context("official-output chunk recovered primary invocation lost its memory permit")?;
    drop(recovered.invocation.transaction);
    discard_generated_instance(recovered_instance).await?;
    recovered_permit.finish(recovered.terminal_memory_outcome, false);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 2);

    assert!(!GENERATED_ROUTED_INSTANCE_POOL
        .lock()
        .iter()
        .any(|instance| {
            instance
                .downcast_ref::<GeneratedReusableInstance<ProdRuntime>>()
                .is_some_and(|instance| instance.route_identity == route_identity)
        }));
    let retired = controller.snapshot_for_test(&memory_identity);
    assert_eq!(retired.active_instances, 0);
    assert_eq!(retired.idle_instances, 0);
    assert_eq!(retired.evicting_instances, 0);
    assert_eq!(retired.retained_baseline_bytes, 0);
    assert_eq!(retired.unattributed_allowance_bytes, 0);
    database.shutdown().await?;

    let removed = GENERATED_ROUTED_MODULES
        .lock()
        .remove(
            &route_identity,
            "module_cache_evicted_official_output_chunk_smoke",
        )
        .context("official-output chunk package was not cached")?;
    anyhow::ensure!(
        Arc::ptr_eq(&removed, &routed),
        "official-output chunk cleanup removed an unexpected package"
    );
    drop(removed);
    drop(sibling_routed);
    drop(routed);
    assert_eq!(
        module_controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        0
    );
    Ok(())
}

#[cfg(test)]
fn assert_generated_normalize_id_accounting(output: &GeneratedExecutionOutput<ProdRuntime>) {
    assert_eq!(output.invocation.read_accounting.documents, 0);
    assert_eq!(output.invocation.read_accounting.bytes, 0);
    // Provider initialization reads canonical URL overrides, even when the
    // handler only normalizes an ID using the in-memory table mapping.
    assert_eq!(output.invocation.read_accounting.intervals, 1);
    assert_eq!(
        output
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    assert_eq!(
        output
            .invocation
            .transaction
            .execution_size()
            .write_size
            .size,
        0
    );
    assert_eq!(
        output
            .invocation
            .transaction
            .execution_size()
            .scheduled_size
            .num_writes,
        0
    );
    assert_eq!(
        output
            .invocation
            .transaction
            .execution_size()
            .scheduled_size
            .size,
        0
    );
    assert!(output.invocation.syscall_trace.async_syscalls.is_empty());
    assert_eq!(output.invocation.opaque_live_handles, 0);
    assert_eq!(output.invocation.opaque_current_bytes, 0);
}

#[cfg(test)]
async fn assert_only_initialization_reads(
    transaction: Transaction<ProdRuntime>,
) -> anyhow::Result<()> {
    let mut initialization = transaction.clone_for_snapshot_query();
    udf::environment::PreloadedEnvVars::load(
        &mut initialization,
        ComponentId::Root,
        BTreeMap::new(),
    )
    .await?;
    // The one accounted range is the canonical URL scan. Table and index
    // existence add derived dependencies too; compare those, not only counts.
    assert_eq!(initialization.execution_size().num_intervals, 1);
    let (expected, _) = initialization.into_reads_and_writes();
    let (actual, _) = transaction.into_reads_and_writes();
    assert!(
        actual
            .read_set()
            .has_same_read_dependencies(expected.read_set()),
        "{:?}",
        actual.read_set().comparison_diagnostic(expected.read_set())
    );
    Ok(())
}

#[cfg(test)]
async fn run_generated_database_normalize_id_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_normalize_id_documents".parse()?;
    let other_table: TableName = "generated_normalize_id_other_documents".parse()?;
    let id = insert_document(&database, &table).await?.developer_id;
    let other_id = insert_document(&database, &other_table).await?.developer_id;
    let query_manifest = generated_test_manifest(
        ManifestUdfKind::Query,
        json!({
            "kind": "databaseNormalizeId",
            "tableName": table,
        }),
    )?;
    let query_routed = generated_database_normalize_id_test_routed_module(
        Arc::clone(&query_manifest),
        1,
        false,
        false,
    )?;
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 2,
        soft_budget_bytes: 12 * WASM_PAGE_BYTES,
        hard_budget_bytes: 16 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let metrics = Arc::new(GateMetrics::default());

    let capability_manifest = generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        1 << 20,
        1_000_000,
        6,
    )?;
    assert!(capability_manifest.imported_operations().is_empty());
    let mut capability_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        capability_manifest,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        None,
    )
    .await?;
    let timeout = capability_state
        .timeout
        .as_mut()
        .context("normalizeId capability test timeout missing")?;
    capability_state.provider.prepare_execution(timeout).await?;
    generated_state_mut(&mut capability_state)?
        .capability_bridge
        .issue()?;
    let normalize_request = |table: &str, value: String| -> anyhow::Result<OpaqueValue> {
        let envelope = GuestNativeValueCodec::encode(
            json!({ "version": 4, "kind": "dbNormalizeId", "table": table, "value": value }),
            MAX_REQUEST_BYTES,
        )?;
        Ok(OpaqueValue::CapabilityRequest(
            GuestCapabilityRequestCodec::decode(&envelope, MAX_REQUEST_BYTES)?,
        ))
    };
    let stale_capability = generated_state(&capability_state)?
        .capability_bridge
        .handle()?;
    for (input, expected) in [
        (id.encode(), json!(id.encode())),
        (id.internal_id().to_string(), json!(id.encode())),
        ("not-a-document-id".to_owned(), JsonValue::Null),
        (other_id.encode(), JsonValue::Null),
    ] {
        let request = generated_state_mut(&mut capability_state)?
            .values
            .insert(normalize_request(table.as_ref(), input)?)?;
        let result = run_generated_capability_sync_operation(
            &mut capability_state,
            stale_capability,
            request.to_abi(),
        )?;
        let result = opaque_handle(result)?;
        assert_eq!(
            generated_state(&capability_state)?
                .values
                .get_json(result)?,
            &expected
        );
        assert!(generated_state(&capability_state)?
            .values
            .get(request, OpaqueValueKind::CapabilityRequest)
            .is_err());
        generated_state_mut(&mut capability_state)?
            .values
            .release(result, OpaqueValueKind::ConvexJson)?;
    }
    assert_eq!(generated_state(&capability_state)?.operation_count, 4);

    let zero_identity_request = generated_state_mut(&mut capability_state)?
        .values
        .insert(normalize_request(table.as_ref(), id.encode())?)?;
    assert_eq!(
        run_generated_capability_sync_operation(
            &mut capability_state,
            0,
            zero_identity_request.to_abi(),
        )?,
        -2
    );
    assert!(generated_state(&capability_state)?
        .values
        .get(zero_identity_request, OpaqueValueKind::CapabilityRequest)
        .is_ok());
    assert!(!generated_state(&capability_state)?.runtime_reuse_contaminated);

    generated_state_mut(&mut capability_state)?
        .capability_bridge
        .revoke()?;
    generated_state_mut(&mut capability_state)?
        .capability_bridge
        .reset()?;
    let current_capability = generated_state(&capability_state)?
        .capability_bridge
        .handle()?;
    assert_ne!(current_capability, stale_capability);
    assert_eq!(
        run_generated_capability_sync_operation(
            &mut capability_state,
            stale_capability,
            zero_identity_request.to_abi(),
        )?,
        -2
    );
    assert!(generated_state(&capability_state)?
        .values
        .get(zero_identity_request, OpaqueValueKind::CapabilityRequest)
        .is_ok());
    assert!(generated_state(&capability_state)?.runtime_reuse_contaminated);

    let result = run_generated_capability_sync_operation(
        &mut capability_state,
        current_capability,
        zero_identity_request.to_abi(),
    )?;
    assert_eq!(generated_state(&capability_state)?.operation_count, 5);
    generated_state_mut(&mut capability_state)?
        .values
        .release(opaque_handle(result)?, OpaqueValueKind::ConvexJson)?;
    assert!(run_generated_capability_sync_operation(
        &mut capability_state,
        current_capability,
        zero_identity_request.to_abi(),
    )
    .is_err());

    let wrong_kind = generated_state_mut(&mut capability_state)?
        .values
        .insert(OpaqueValue::Bytes(vec![1]))?;
    assert!(run_generated_capability_sync_operation(
        &mut capability_state,
        current_capability,
        wrong_kind.to_abi(),
    )
    .is_err());
    assert!(generated_state(&capability_state)?
        .values
        .get(wrong_kind, OpaqueValueKind::Bytes)
        .is_err());

    let developer_error_request = generated_state_mut(&mut capability_state)?
        .values
        .insert(normalize_request("not a table name", id.encode())?)?;
    assert_eq!(
        run_generated_capability_sync_operation(
            &mut capability_state,
            current_capability,
            developer_error_request.to_abi(),
        )?,
        -1
    );
    assert_eq!(generated_state(&capability_state)?.operation_count, 6);
    assert!(capability_state.developer_error.is_some());

    let over_limit_request = generated_state_mut(&mut capability_state)?
        .values
        .insert(normalize_request(table.as_ref(), id.encode())?)?;
    assert!(run_generated_capability_sync_operation(
        &mut capability_state,
        current_capability,
        over_limit_request.to_abi(),
    )
    .is_err());
    assert_eq!(generated_state(&capability_state)?.operation_count, 6);
    assert!(generated_state(&capability_state)?
        .values
        .get(over_limit_request, OpaqueValueKind::CapabilityRequest)
        .is_err());

    let transaction = capability_state.provider.take_transaction()?;
    let mut generated = capability_state
        .generated
        .take()
        .context("capability normalizeId operand test state disappeared")?;
    generated.values.cleanup();
    let memory_permit = generated
        .memory_permit
        .take()
        .context("capability normalizeId operand test memory permit disappeared")?;
    drop(generated);
    drop(capability_state);
    memory_permit.finish(TerminalMemoryOutcome::SystemError, false);
    drop(transaction);

    for (input, expected) in [
        (json!(id.encode()), json!(id.encode())),
        (json!(id.internal_id().to_string()), json!(id.encode())),
        (json!("not-a-document-id"), JsonValue::Null),
        (json!(other_id.encode()), JsonValue::Null),
    ] {
        let output = execute_reusable_generated_normalize_id_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&query_manifest),
            Arc::clone(&query_routed),
            json!({ "id": input }),
            UdfType::Query,
            Arc::clone(&controller),
            Arc::clone(&metrics),
            None,
            false,
        )
        .await?;
        assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
        assert_eq!(output.invocation.function_result, Some(expected));
        assert_generated_normalize_id_accounting(&output);
        assert_only_initialization_reads(output.invocation.transaction).await?;
    }

    let mutation_manifest = generated_test_manifest(
        ManifestUdfKind::Mutation,
        json!({
            "kind": "databaseNormalizeId",
            "tableName": table,
        }),
    )?;
    let mutation_routed = generated_database_normalize_id_test_routed_module(
        Arc::clone(&mutation_manifest),
        1,
        false,
        false,
    )?;
    let mutation = execute_reusable_generated_normalize_id_test_module(
        rt.clone(),
        database.begin_system().await?,
        mutation_manifest,
        mutation_routed,
        json!({ "id": id.encode() }),
        UdfType::Mutation,
        Arc::clone(&controller),
        Arc::clone(&metrics),
        None,
        false,
    )
    .await?;
    assert_eq!(mutation.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        mutation.invocation.function_result,
        Some(json!(id.encode()))
    );
    assert_generated_normalize_id_accounting(&mutation);
    drop(mutation.invocation.transaction);

    let malformed = execute_reusable_generated_normalize_id_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&query_manifest),
        Arc::clone(&query_routed),
        json!({ "id": 7 }),
        UdfType::Query,
        Arc::clone(&controller),
        Arc::clone(&metrics),
        None,
        false,
    )
    .await?;
    assert_eq!(
        malformed.invocation.outcome,
        InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
    );
    assert_generated_normalize_id_accounting(&malformed);
    drop(malformed.invocation.transaction);

    let stale_routed = generated_database_normalize_id_test_routed_module(
        Arc::clone(&query_manifest),
        1,
        true,
        false,
    )?;
    let stale = execute_reusable_generated_normalize_id_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&query_manifest),
        stale_routed,
        json!({ "id": id.encode() }),
        UdfType::Query,
        Arc::clone(&controller),
        Arc::clone(&metrics),
        None,
        false,
    )
    .await?;
    assert_eq!(stale.invocation.outcome, InvocationOutcome::SystemError);
    assert_generated_normalize_id_accounting(&stale);
    drop(stale.invocation.transaction);

    let invalid_operation_routed = generated_database_normalize_id_test_routed_module(
        Arc::clone(&query_manifest),
        2,
        false,
        false,
    )?;
    let invalid_operation = execute_reusable_generated_normalize_id_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&query_manifest),
        invalid_operation_routed,
        json!({ "id": id.encode() }),
        UdfType::Query,
        Arc::clone(&controller),
        Arc::clone(&metrics),
        None,
        false,
    )
    .await?;
    assert_eq!(
        invalid_operation.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_generated_normalize_id_accounting(&invalid_operation);
    drop(invalid_operation.invocation.transaction);

    let operand_manifest = generated_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        vec![
            json!({
                "id": 1,
                "debugName": "normalizeId",
                "operation": {
                    "kind": "databaseNormalizeId",
                    "tableName": table,
                },
            }),
            json!({
                "id": 2,
                "debugName": "wrongDescriptor",
                "operation": {
                    "kind": "databaseGet",
                    "tableName": table,
                },
            }),
        ],
        1 << 20,
        1_000_000,
        16,
    )?;
    let mut operand_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        operand_manifest,
        JsonValue::Null,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        None,
    )
    .await?;
    for operation_id in [2, 3] {
        let handle = generated_state_mut(&mut operand_state)?
            .values
            .insert_string(id.encode())?
            .to_abi();
        assert!(
            run_generated_database_normalize_id(&mut operand_state, operation_id, handle,).is_err()
        );
        assert_eq!(
            generated_state(&operand_state)?.values.live_handle_count(),
            1
        );
    }
    let wrong_kind = generated_state_mut(&mut operand_state)?
        .values
        .insert(OpaqueValue::Bytes(vec![1]))?
        .to_abi();
    assert!(run_generated_database_normalize_id(&mut operand_state, 1, wrong_kind).is_err());
    assert_eq!(
        generated_state(&operand_state)?.values.live_handle_count(),
        1
    );
    let stale_handle = generated_state_mut(&mut operand_state)?
        .values
        .insert_string(id.encode())?;
    generated_state_mut(&mut operand_state)?
        .values
        .release(stale_handle, OpaqueValueKind::ConvexJson)?;
    assert!(
        run_generated_database_normalize_id(&mut operand_state, 1, stale_handle.to_abi(),).is_err()
    );
    assert_eq!(
        generated_state(&operand_state)?.values.live_handle_count(),
        1
    );
    let transaction = operand_state.provider.take_transaction()?;
    let mut generated = operand_state
        .generated
        .take()
        .context("normalizeId operand test state disappeared")?;
    generated.values.cleanup();
    let memory_permit = generated
        .memory_permit
        .take()
        .context("normalizeId operand test memory permit disappeared")?;
    drop(generated);
    drop(operand_state);
    memory_permit.finish(TerminalMemoryOutcome::SystemError, false);
    drop(transaction);

    let reuse_metrics = Arc::new(GateMetrics::default());
    let reuse_routed = generated_database_normalize_id_test_routed_module(
        Arc::clone(&query_manifest),
        1,
        false,
        true,
    )?;
    let mut first = execute_reusable_generated_normalize_id_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&query_manifest),
        Arc::clone(&reuse_routed),
        json!({ "id": id.encode() }),
        UdfType::Query,
        Arc::clone(&controller),
        Arc::clone(&reuse_metrics),
        None,
        true,
    )
    .await?;
    assert_eq!(first.invocation.outcome, InvocationOutcome::Success);
    let first_runtime_id = first
        .reusable_instance
        .as_ref()
        .context("first normalizeId invocation did not retain its runtime")?
        .id;
    let retained = retain_generated_test_runtime(&mut first)?;
    drop(first.invocation.transaction);
    let reused = execute_reusable_generated_normalize_id_test_module(
        rt,
        database.begin_system().await?,
        query_manifest,
        reuse_routed,
        json!({ "id": id.encode() }),
        UdfType::Query,
        controller,
        Arc::clone(&reuse_metrics),
        Some(retained),
        true,
    )
    .await?;
    assert_eq!(reused.invocation.outcome, InvocationOutcome::SystemError);
    assert!(reused.reusable_instance.is_none());
    assert!(reused.memory_permit.is_none());
    assert!(reused.system_error.is_some());
    assert_eq!(reuse_metrics.teardowns.load(Ordering::SeqCst), 1);
    assert_ne!(first_runtime_id, 0);
    assert_generated_normalize_id_accounting(&reused);
    drop(reused.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_suspended_system_timeout(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;
    const TEST_ACTIVE_TIMEOUT: Duration = Duration::from_millis(100);
    const TEST_SYSTEM_TIMEOUT: Duration = Duration::from_millis(250);

    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_system_timeout_documents".parse()?;
    let id = insert_document(&database, &table).await?.developer_id;
    let manifest = generated_test_manifest(
        ManifestUdfKind::Query,
        json!({
            "kind": "databaseGet",
            "tableName": table,
        }),
    )?;
    let routed =
        generated_test_routed_module(Arc::clone(&manifest), GeneratedTestOperation::DatabaseGet)?;
    let route_identity = routed.route_identity.clone();
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 2,
        soft_budget_bytes: 8 * WASM_PAGE_BYTES,
        hard_budget_bytes: 12 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: 2 * WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    let metrics = Arc::new(GateMetrics::default());
    let (control, mut host) = read_control();
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "id": id.encode() }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        Some(control),
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut state,
        TEST_ACTIVE_TIMEOUT,
        TEST_SYSTEM_TIMEOUT,
    )?;

    let execution = tokio::spawn(execute_generated(Arc::clone(&routed), state, None, true));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("generated database read did not become pending")??;
    let suspended = controller.snapshot_for_test(&memory_identity);
    assert_eq!(suspended.active_instances, 1);
    assert_eq!(suspended.idle_instances, 0);

    let mut timed_out = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("generated suspended-system timeout did not complete")???;
    assert!(!timed_out.cancelled);
    assert_eq!(
        timed_out.invocation.outcome,
        InvocationOutcome::SystemTimeout
    );
    let error = timed_out
        .system_error
        .take()
        .context("generated suspended-system timeout lost its classified error")?;
    assert!(error.is_bad_request());
    assert_eq!(error.short_msg(), "SystemTimeoutError");
    assert_eq!(error.msg(), SYSTEM_TIMEOUT_ERROR_MESSAGE);
    assert_eq!(error.user_facing_message(), SYSTEM_TIMEOUT_ERROR_MESSAGE);
    assert_eq!(metrics.read_cancelled.load(Ordering::SeqCst), 1);
    assert!(host
        .release
        .take()
        .context("generated suspended read release missing")?
        .send(())
        .is_err());
    assert_eq!(timed_out.invocation.opaque_live_handles, 0);
    assert_eq!(timed_out.invocation.opaque_current_bytes, 0);
    assert!(timed_out.reusable_instance.is_none());
    assert!(timed_out.memory_permit.is_none());
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.store_drops.load(Ordering::SeqCst), 1);
    let released = controller.snapshot_for_test(&memory_identity);
    assert_eq!(released.active_instances, 0);
    assert_eq!(released.idle_instances, 0);
    assert_eq!(released.function_samples, 1);
    drop(timed_out.invocation.transaction);

    let mut followup_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        json!({ "id": id.encode() }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&metrics),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity,
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: WASM_PAGE_BYTES,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut followup_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let followup = execute_generated(routed, followup_state, None, false).await?;
    assert_eq!(followup.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        followup
            .invocation
            .function_result
            .as_ref()
            .and_then(|value| value.get("marker")),
        Some(&json!("backend-gate-document"))
    );
    assert!(followup.reusable_instance.is_none());
    assert!(followup.memory_permit.is_none());
    let completed = controller.snapshot_for_test(&memory_identity);
    assert_eq!(completed.active_instances, 0);
    assert_eq!(completed.idle_instances, 0);
    assert_eq!(completed.function_samples, 2);
    drop(followup.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_database_resource_limit_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_resource_limit_documents".parse()?;
    let first_id = insert_document(&database, &table).await?.developer_id;
    let second_id = insert_document(&database, &table).await?.developer_id;
    let first_id_string = first_id.encode();
    let second_id_string = second_id.encode();
    let manifest = generated_test_manifest(
        ManifestUdfKind::Query,
        json!({
            "kind": "databaseGet",
            "tableName": table,
        }),
    )?;
    let routed = generated_async_batch_test_routed_module(
        Arc::clone(&manifest),
        GeneratedAsyncBatchTestOperation {
            import_database_write: false,
            later_database_write_operation_id: None,
        },
    )?;
    let route_identity = routed.route_identity.clone();
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 1,
        soft_budget_bytes: 12 * WASM_PAGE_BYTES,
        hard_budget_bytes: 16 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let metrics = Arc::new(GateMetrics::default());
    let request = |id: DeveloperDocumentId| {
        json!({
            "batch": [{
                "operationId": 1,
                "arguments": [id.encode()],
            }],
        })
    };

    let mut first = execute_reusable_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        Arc::clone(&routed),
        request(first_id),
        Arc::clone(&controller),
        Arc::clone(&metrics),
        None,
        true,
    )
    .await?;
    assert_eq!(first.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        first
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .and_then(|values| values.first())
            .and_then(|value| value.get("_id"))
            .and_then(JsonValue::as_str),
        Some(first_id_string.as_str())
    );
    assert_eq!(first.invocation.read_accounting.documents, 1);
    let document_bytes = first.invocation.read_accounting.bytes;
    assert!(document_bytes > 0);
    assert!(first.invocation.read_accounting.intervals > 0);
    assert_eq!(first.invocation.opaque_live_handles, 0);
    assert_eq!(first.invocation.opaque_current_bytes, 0);
    let first_runtime = retain_generated_test_runtime(&mut first)?;
    drop(first.invocation.transaction);
    let first_idle = controller.snapshot_for_test(&memory_identity);
    assert_eq!(first_idle.active_instances, 0);
    assert_eq!(first_idle.idle_instances, 1);
    assert_eq!(first_idle.function_samples, 1);

    let mut document_limited_transaction = database.begin_system().await?;
    document_limited_transaction.set_transaction_limits(TransactionLimits {
        documents_read: 0,
        ..TransactionLimits::default()
    });
    let document_limited = execute_reusable_generated_async_batch_test_module(
        rt.clone(),
        document_limited_transaction,
        Arc::clone(&manifest),
        Arc::clone(&routed),
        request(second_id),
        Arc::clone(&controller),
        Arc::clone(&metrics),
        Some(first_runtime),
        true,
    )
    .await?;
    assert_eq!(
        document_limited.invocation.outcome,
        InvocationOutcome::DeveloperError("TooManyDocumentsRead".to_owned())
    );
    assert_eq!(
        document_limited.terminal_memory_outcome,
        TerminalMemoryOutcome::DeveloperError
    );
    assert!(document_limited.system_error.is_none());
    assert!(document_limited.reusable_instance.is_none());
    assert!(document_limited.memory_permit.is_none());
    assert_eq!(document_limited.invocation.read_accounting.documents, 1);
    assert_eq!(
        document_limited.invocation.read_accounting.bytes,
        document_bytes
    );
    assert_eq!(document_limited.invocation.opaque_live_handles, 0);
    assert_eq!(document_limited.invocation.opaque_current_bytes, 0);
    assert_eq!(
        document_limited
            .invocation
            .transaction
            .execution_size()
            .read_size
            .total_document_count,
        1
    );
    assert_eq!(
        document_limited
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    drop(document_limited.invocation.transaction);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 1);
    let after_document_limit = controller.snapshot_for_test(&memory_identity);
    assert_eq!(after_document_limit.active_instances, 0);
    assert_eq!(after_document_limit.idle_instances, 0);
    assert_eq!(after_document_limit.function_samples, 2);
    assert_eq!(after_document_limit.developer_error_samples, 1);
    assert_eq!(after_document_limit.system_error_samples, 0);

    let mut second_success = execute_reusable_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        Arc::clone(&routed),
        request(second_id),
        Arc::clone(&controller),
        Arc::clone(&metrics),
        None,
        true,
    )
    .await?;
    assert_eq!(
        second_success
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .and_then(|values| values.first())
            .and_then(|value| value.get("_id"))
            .and_then(JsonValue::as_str),
        Some(second_id_string.as_str())
    );
    assert_eq!(second_success.invocation.opaque_live_handles, 0);
    assert_eq!(second_success.invocation.opaque_current_bytes, 0);
    let second_runtime = retain_generated_test_runtime(&mut second_success)?;
    drop(second_success.invocation.transaction);

    let mut byte_limited_transaction = database.begin_system().await?;
    byte_limited_transaction.set_transaction_limits(TransactionLimits {
        bytes_read: document_bytes - 1,
        ..TransactionLimits::default()
    });
    let byte_limited = execute_reusable_generated_async_batch_test_module(
        rt.clone(),
        byte_limited_transaction,
        Arc::clone(&manifest),
        Arc::clone(&routed),
        request(first_id),
        Arc::clone(&controller),
        Arc::clone(&metrics),
        Some(second_runtime),
        true,
    )
    .await?;
    assert_eq!(
        byte_limited.invocation.outcome,
        InvocationOutcome::DeveloperError("TooManyBytesRead".to_owned())
    );
    assert_eq!(
        byte_limited.terminal_memory_outcome,
        TerminalMemoryOutcome::DeveloperError
    );
    assert!(byte_limited.system_error.is_none());
    assert!(byte_limited.reusable_instance.is_none());
    assert!(byte_limited.memory_permit.is_none());
    assert_eq!(byte_limited.invocation.read_accounting.documents, 1);
    assert_eq!(
        byte_limited.invocation.read_accounting.bytes,
        document_bytes
    );
    assert_eq!(byte_limited.invocation.opaque_live_handles, 0);
    assert_eq!(byte_limited.invocation.opaque_current_bytes, 0);
    assert_eq!(
        byte_limited
            .invocation
            .transaction
            .execution_size()
            .read_size
            .total_document_size,
        document_bytes
    );
    assert_eq!(
        byte_limited
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    drop(byte_limited.invocation.transaction);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 2);
    let after_byte_limit = controller.snapshot_for_test(&memory_identity);
    assert_eq!(after_byte_limit.active_instances, 0);
    assert_eq!(after_byte_limit.idle_instances, 0);
    assert_eq!(after_byte_limit.function_samples, 4);
    assert_eq!(after_byte_limit.developer_error_samples, 2);
    assert_eq!(after_byte_limit.system_error_samples, 0);

    let final_success = execute_reusable_generated_async_batch_test_module(
        rt,
        database.begin_system().await?,
        manifest,
        routed,
        request(first_id),
        Arc::clone(&controller),
        Arc::clone(&metrics),
        None,
        false,
    )
    .await?;
    assert_eq!(final_success.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        final_success
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .and_then(|values| values.first())
            .and_then(|value| value.get("_id"))
            .and_then(JsonValue::as_str),
        Some(first_id_string.as_str())
    );
    assert_eq!(final_success.invocation.read_accounting.documents, 1);
    assert_eq!(
        final_success.invocation.read_accounting.bytes,
        document_bytes
    );
    assert_eq!(final_success.invocation.opaque_live_handles, 0);
    assert_eq!(final_success.invocation.opaque_current_bytes, 0);
    assert!(final_success.reusable_instance.is_none());
    assert!(final_success.memory_permit.is_none());
    drop(final_success.invocation.transaction);
    assert_eq!(metrics.teardowns.load(Ordering::SeqCst), 3);
    let completed = controller.snapshot_for_test(&memory_identity);
    assert_eq!(completed.active_instances, 0);
    assert_eq!(completed.idle_instances, 0);
    assert_eq!(completed.function_samples, 5);
    assert_eq!(completed.developer_error_samples, 2);
    assert_eq!(completed.system_error_samples, 0);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_authentication_get_user_identity_tests(
    rt: ProdRuntime,
) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let manifest = generated_test_manifest_with_limits(
        ManifestUdfKind::Query,
        vec![
            json!({
                "id": 1,
                "debugName": "currentUserIdentity",
                "operation": {
                    "kind": "authenticationGetUserIdentity",
                },
            }),
            json!({
                "id": 2,
                "debugName": "invalidDocumentRead",
                "operation": {
                    "kind": "databaseGet",
                    "tableName": "auth_test_documents",
                },
            }),
        ],
        1 << 20,
    )?;
    let operation = GeneratedAsyncBatchTestOperation {
        import_database_write: false,
        later_database_write_operation_id: None,
    };
    let auth_invocation = json!({
        "operationId": 1.0,
        "arguments": [],
    });

    let without_auth = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin(Identity::Unknown(None)).await?,
        Arc::clone(&manifest),
        operation,
        json!({ "batch": [] }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(without_auth.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(without_auth.invocation.function_result, Some(json!([])));
    assert!(!without_auth.invocation.observed_identity);
    assert!(without_auth
        .invocation
        .syscall_trace
        .async_syscalls
        .is_empty());
    drop(without_auth.invocation.transaction);

    let logged_out = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin(Identity::Unknown(None)).await?,
        Arc::clone(&manifest),
        operation,
        json!({
            "batch": [auth_invocation.clone(), auth_invocation.clone()],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(logged_out.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        logged_out.invocation.function_result,
        Some(json!([null, null]))
    );
    assert!(logged_out.invocation.observed_identity);
    assert!(logged_out
        .invocation
        .syscall_trace
        .async_syscalls
        .contains_key("1.0/getUserIdentity"));
    assert_eq!(logged_out.invocation.read_accounting.documents, 0);
    assert_eq!(logged_out.invocation.read_accounting.bytes, 0);
    drop(logged_out.invocation.transaction);

    let user_identity = UserIdentity::from_proto_unchecked(pb::convex_identity::UserIdentity {
        subject: Some("auth-test-subject".to_owned()),
        issuer: Some("https://issuer.invalid".to_owned()),
        expiration: Some((SystemTime::now() + Duration::from_secs(3_600)).into()),
        attributes: Some(pb::convex_identity::UserIdentityAttributes {
            token_identifier: Some("https://issuer.invalid|auth-test-subject".to_owned()),
            issuer: Some("https://issuer.invalid".to_owned()),
            subject: Some("auth-test-subject".to_owned()),
            name: Some("Auth Test User".to_owned()),
            ..Default::default()
        }),
        original_token: Some("auth-test-token".to_owned()),
    })?;
    let expected_identity: JsonValue = (*user_identity.attributes).clone().try_into()?;
    let logged_in = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin(Identity::user(user_identity)).await?,
        Arc::clone(&manifest),
        operation,
        json!({ "batch": [auth_invocation.clone()] }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(logged_in.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        logged_in.invocation.function_result,
        Some(json!([expected_identity]))
    );
    assert!(logged_in.invocation.observed_identity);
    assert!(logged_in
        .invocation
        .syscall_trace
        .async_syscalls
        .contains_key("1.0/getUserIdentity"));
    assert_eq!(logged_in.invocation.read_accounting.documents, 0);
    assert_eq!(logged_in.invocation.read_accounting.bytes, 0);
    drop(logged_in.invocation.transaction);

    let developer_error = execute_generated_async_batch_test_module(
        rt,
        database.begin(Identity::Unknown(None)).await?,
        manifest,
        operation,
        json!({
            "batch": [
                auth_invocation,
                {
                    "operationId": 2.0,
                    "arguments": ["not-a-document-id"],
                },
            ],
        }),
        UdfType::Query,
    )
    .await?;
    assert!(matches!(
        developer_error.invocation.outcome,
        InvocationOutcome::DeveloperError(_)
    ));
    assert!(developer_error.invocation.observed_identity);
    assert!(developer_error
        .invocation
        .syscall_trace
        .async_syscalls
        .contains_key("1.0/getUserIdentity"));
    drop(developer_error.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_compound_query_take_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_compound_query_documents".parse()?;
    insert_document(&database, &table).await?;
    let matching_ids = {
        let mut transaction = database.begin_system().await?;
        let index_name = IndexName::new(
            table.clone(),
            IndexDescriptor::new("by_tenant_status_sequence")?,
        )?;
        IndexModel::new(&mut transaction)
            .add_application_index(
                TableNamespace::root_component(),
                IndexMetadata::new_enabled(
                    index_name,
                    IndexedFields::try_from(vec![
                        "tenant".parse()?,
                        "status".parse()?,
                        "sequence".parse()?,
                    ])?,
                ),
            )
            .await?;
        let mut ids = Vec::new();
        // Static Hermes numeric operands are Float64 values, and index ranges
        // retain the distinction between Float64 and Int64 values.
        for sequence in [10.0_f64, 20.0, 30.0] {
            ids.push(
                SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
                    .insert_metadata(
                        &table,
                        obj!(
                            "tenant" => "tenant-a",
                            "status" => "open",
                            "sequence" => sequence,
                            "marker" => format!("matching-{sequence}"),
                        )?,
                    )
                    .await?
                    .developer_id,
            );
        }
        SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
            .insert_metadata(
                &table,
                obj!(
                    "tenant" => "tenant-a",
                    "status" => "closed",
                    "sequence" => 15.0_f64,
                    "marker" => "nonmatching",
                )?,
            )
            .await?;
        database
            .commit_with_write_source(transaction, "generated_compound_query_setup")
            .await?;
        ids
    };
    let imported_operation = json!({
        "id": 1,
        "debugName": "generatedCompoundTake",
        "operation": {
            "kind": "databaseIndexQuery",
            "tableName": table,
            "indexName": "by_tenant_status_sequence",
            "constraints": [
                { "fieldPath": "tenant", "operator": "eq" },
                { "fieldPath": "status", "operator": "eq" },
                { "fieldPath": "sequence", "operator": "gt" },
            ],
            "order": "ascending",
            "terminal": "collect",
            "limit": 2,
        },
    });
    let manifest = generated_schema_five_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        vec![imported_operation.clone()],
        1 << 20,
        1_000_000,
        4,
    )?;
    let query_values = json!(["tenant-a", "open", 0.0]);
    let query_arguments = query_values
        .as_array()
        .context("compound query values must be an array")?
        .clone();
    let npm_version = Version::new(1, 43, 0);
    parse_query_stream_request(generated_query_syscall_args(
        manifest.imported_operations()[0].operation(),
        query_arguments,
        &npm_version,
    )??)?;

    let sequential = execute_generated_sequential_query_collect_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "value": query_values }),
    )
    .await?;
    assert_eq!(
        sequential.invocation.outcome,
        InvocationOutcome::Success,
        "sequential compound query failed: {:#?}; reads=({}, {}, {})",
        sequential.system_error,
        sequential.invocation.read_accounting.documents,
        sequential.invocation.read_accounting.bytes,
        sequential.invocation.read_accounting.intervals,
    );
    let sequential_result = sequential
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .context("sequential compound query did not return an array")?;
    let sequential_ids = sequential_result
        .iter()
        .map(|document| {
            document
                .get("_id")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .context("sequential compound query document has no ID")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_eq!(
        sequential_ids,
        vec![matching_ids[0].encode(), matching_ids[1].encode()]
    );
    assert_eq!(sequential.invocation.read_accounting.documents, 2);
    assert!(sequential.invocation.read_accounting.bytes > 0);
    assert!(sequential.invocation.read_accounting.intervals > 0);
    assert_eq!(sequential.invocation.opaque_live_handles, 0);
    assert_eq!(sequential.invocation.opaque_current_bytes, 0);

    let batch_only = GeneratedAsyncBatchTestOperation {
        import_database_write: false,
        later_database_write_operation_id: None,
    };
    let direct = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        batch_only,
        json!({
            "batch": [{
                "operationId": 1.0,
                "arguments": ["tenant-a", "open", 0.0],
            }],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(direct.invocation.outcome, InvocationOutcome::Success);
    let direct_result = direct
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .and_then(|batch| batch.first())
        .and_then(JsonValue::as_array)
        .context("direct compound query did not return its result array")?;
    assert_eq!(direct_result, sequential_result);
    assert_eq!(direct.invocation.read_accounting.documents, 2);
    assert_eq!(
        direct.invocation.read_accounting.bytes,
        sequential.invocation.read_accounting.bytes
    );
    assert_eq!(
        direct.invocation.read_accounting.intervals,
        sequential.invocation.read_accounting.intervals
    );
    assert_eq!(direct.invocation.opaque_live_handles, 0);
    assert_eq!(direct.invocation.opaque_current_bytes, 0);

    let single_constraint_manifest = generated_schema_five_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        vec![json!({
            "id": 1,
            "debugName": "generatedSingleConstraintTake",
            "operation": {
                "kind": "databaseIndexQuery",
                "tableName": table.clone(),
                "indexName": "by_tenant_status_sequence",
                "constraints": [
                    { "fieldPath": "tenant", "operator": "eq" },
                ],
                "order": "ascending",
                "terminal": "collect",
                "limit": 2,
            },
        })],
        1 << 20,
        1_000_000,
        4,
    )?;
    let single_sequential = execute_generated_sequential_query_collect_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&single_constraint_manifest),
        json!({ "value": ["tenant-a"] }),
    )
    .await?;
    assert_eq!(
        single_sequential.invocation.outcome,
        InvocationOutcome::Success
    );
    let single_direct = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&single_constraint_manifest),
        batch_only,
        json!({
            "batch": [{
                "operationId": 1.0,
                "arguments": ["tenant-a"],
            }],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(single_direct.invocation.outcome, InvocationOutcome::Success);
    let single_sequential_result = single_sequential
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .context("sequential single-constraint query did not return an array")?;
    let single_direct_result = single_direct
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .and_then(|batch| batch.first())
        .and_then(JsonValue::as_array)
        .context("direct single-constraint query did not return its result array")?;
    assert_eq!(single_sequential_result.len(), 2);
    assert_eq!(single_direct_result, single_sequential_result);
    assert_eq!(single_sequential.invocation.read_accounting.documents, 2);
    assert_eq!(
        single_direct.invocation.read_accounting.documents,
        single_sequential.invocation.read_accounting.documents
    );
    assert_eq!(
        single_direct.invocation.read_accounting.bytes,
        single_sequential.invocation.read_accounting.bytes
    );
    assert_eq!(
        single_direct.invocation.read_accounting.intervals,
        single_sequential.invocation.read_accounting.intervals
    );
    assert_eq!(single_sequential.invocation.opaque_live_handles, 0);
    assert_eq!(single_sequential.invocation.opaque_current_bytes, 0);
    assert_eq!(single_direct.invocation.opaque_live_handles, 0);
    assert_eq!(single_direct.invocation.opaque_current_bytes, 0);

    let mut invalid_single_constraint = Vec::new();
    for value in [json!("tenant-a"), json!([]), json!(["tenant-a", "extra"])] {
        let invalid = execute_generated_sequential_query_collect_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&single_constraint_manifest),
            json!({ "value": value }),
        )
        .await?;
        assert_eq!(invalid.invocation.outcome, InvocationOutcome::SystemError);
        assert_eq!(invalid.invocation.read_accounting.documents, 0);
        assert_eq!(invalid.invocation.read_accounting.bytes, 0);
        // Initialization still reads system URL configuration before the
        // invalid user query is rejected; inspect the exact dependency below.
        assert_eq!(invalid.invocation.read_accounting.intervals, 1);
        assert_eq!(invalid.invocation.opaque_live_handles, 0);
        assert_eq!(invalid.invocation.opaque_current_bytes, 0);
        invalid_single_constraint.push(invalid);
    }

    let operation_limited_manifest = generated_schema_five_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        vec![imported_operation],
        1 << 20,
        1_000_000,
        3,
    )?;
    let sequential_operation_limited = execute_generated_sequential_query_collect_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&operation_limited_manifest),
        json!({ "value": ["tenant-a", "open", 0.0] }),
    )
    .await?;
    assert_eq!(
        sequential_operation_limited.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(
        sequential_operation_limited
            .invocation
            .read_accounting
            .documents,
        2
    );
    assert_eq!(
        sequential_operation_limited
            .invocation
            .read_accounting
            .bytes,
        sequential.invocation.read_accounting.bytes
    );
    assert_eq!(
        sequential_operation_limited.invocation.opaque_live_handles,
        0
    );
    assert_eq!(
        sequential_operation_limited.invocation.opaque_current_bytes,
        0
    );
    let direct_operation_limited = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        operation_limited_manifest,
        batch_only,
        json!({
            "batch": [{
                "operationId": 1.0,
                "arguments": ["tenant-a", "open", 0.0],
            }],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(
        direct_operation_limited.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(
        direct_operation_limited
            .invocation
            .read_accounting
            .documents,
        2
    );
    assert_eq!(
        direct_operation_limited.invocation.read_accounting.bytes,
        sequential.invocation.read_accounting.bytes
    );
    assert_eq!(direct_operation_limited.invocation.opaque_live_handles, 0);
    assert_eq!(direct_operation_limited.invocation.opaque_current_bytes, 0);

    let malformed_sequential = execute_generated_sequential_query_collect_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "value": "not-an-array" }),
    )
    .await?;
    assert_eq!(
        malformed_sequential.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(malformed_sequential.invocation.read_accounting.documents, 0);
    assert_eq!(malformed_sequential.invocation.read_accounting.bytes, 0);
    assert_eq!(malformed_sequential.invocation.read_accounting.intervals, 1);
    assert_eq!(malformed_sequential.invocation.opaque_live_handles, 0);
    assert_eq!(malformed_sequential.invocation.opaque_current_bytes, 0);
    let malformed_direct = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        batch_only,
        json!({
            "batch": [{
                "operationId": 1.0,
                "arguments": ["tenant-a", "open", 0.0, "extra"],
            }],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(
        malformed_direct.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(malformed_direct.invocation.read_accounting.documents, 0);
    assert_eq!(malformed_direct.invocation.read_accounting.bytes, 0);
    assert_eq!(malformed_direct.invocation.read_accounting.intervals, 1);
    assert_eq!(malformed_direct.invocation.opaque_live_handles, 0);
    assert_eq!(malformed_direct.invocation.opaque_current_bytes, 0);

    for direct_batch in [false, true] {
        let cancellation = CancellationSignal::new_for_test();
        let (control, mut host) = read_control();
        let metrics = Arc::new(GateMetrics::default());
        let routed = if direct_batch {
            generated_async_batch_test_routed_module(Arc::clone(&manifest), batch_only)?
        } else {
            generated_sequential_query_collect_test_routed_module(Arc::clone(&manifest))?
        };
        let request = if direct_batch {
            json!({
                "batch": [{
                    "operationId": 1.0,
                    "arguments": ["tenant-a", "open", 0.0],
                }],
            })
        } else {
            json!({ "value": ["tenant-a", "open", 0.0] })
        };
        let mut state = generated_test_state(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            request,
            UdfType::Query,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            cancellation.clone(),
            Some(control),
            Arc::clone(&metrics),
            None,
        )
        .await?;
        arm_generated_timeout(
            rt.clone(),
            &mut state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let execution = tokio::spawn(execute_generated(routed, state, None, false));
        tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
            .await
            .context("compound query read did not become pending")??;
        cancellation.cancel_for_test();
        let cancelled = tokio::time::timeout(Duration::from_secs(5), execution)
            .await
            .context("compound query cancellation did not complete")???;
        assert!(cancelled.cancelled);
        assert_eq!(metrics.read_cancelled.load(Ordering::SeqCst), 1);
        assert!(host
            .release
            .take()
            .context("compound query read release missing")?
            .send(())
            .is_err());
        assert_eq!(cancelled.invocation.opaque_live_handles, 0);
        assert_eq!(cancelled.invocation.opaque_current_bytes, 0);
        assert!(cancelled.reusable_instance.is_none());
        assert!(cancelled.memory_permit.is_none());
        drop(cancelled.invocation.transaction);
    }

    drop(sequential.invocation.transaction);
    drop(direct.invocation.transaction);
    drop(single_sequential.invocation.transaction);
    drop(single_direct.invocation.transaction);
    for invalid in invalid_single_constraint {
        assert_only_initialization_reads(invalid.invocation.transaction).await?;
    }
    drop(sequential_operation_limited.invocation.transaction);
    drop(direct_operation_limited.invocation.transaction);
    assert_only_initialization_reads(malformed_sequential.invocation.transaction).await?;
    assert_only_initialization_reads(malformed_direct.invocation.transaction).await?;
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_direct_async_batch_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_batch_documents".parse()?;
    let id = insert_document(&database, &table).await?.developer_id;
    let missing = missing_id(id);
    let imported_get = |operation_id: u32, table_name: &TableName| {
        json!({
            "id": operation_id,
            "debugName": format!("generatedBatchGet{operation_id}"),
            "operation": {
                "kind": "databaseGet",
                "tableName": table_name,
            },
        })
    };
    let invocation = |operation_id: u32, id: JsonValue| {
        json!({
            // Static Hermes represents every JavaScript number as an f64 on
            // this opaque-value wire, including compiler-assigned integer IDs.
            "operationId": f64::from(operation_id),
            "arguments": [id],
        })
    };
    let batch_only = GeneratedAsyncBatchTestOperation {
        import_database_write: false,
        later_database_write_operation_id: None,
    };
    let get_manifest = generated_test_manifest_with_limits(
        ManifestUdfKind::Query,
        vec![imported_get(1, &table), imported_get(2, &table)],
        1 << 20,
    )?;

    let empty = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&get_manifest),
        batch_only,
        json!({ "batch": [] }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(empty.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(empty.invocation.function_result, Some(json!([])));
    assert_eq!(empty.invocation.read_accounting.documents, 0);
    // Static Hermes initializes the database-UDF environment before the guest
    // runs, including the canonical URL override range read.
    assert_eq!(empty.invocation.read_accounting.intervals, 1);
    assert_eq!(empty.invocation.opaque_live_handles, 0);
    assert_eq!(empty.invocation.opaque_current_bytes, 0);
    drop(empty.invocation.transaction);

    let ordered = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&get_manifest),
        batch_only,
        json!({
            "batch": [
                invocation(1, json!(missing.encode())),
                invocation(2, json!(id.encode())),
            ],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(ordered.invocation.outcome, InvocationOutcome::Success);
    let ordered_results = ordered
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .context("generated batch did not return an array")?;
    assert_eq!(ordered_results.len(), 2);
    assert_eq!(ordered_results[0], JsonValue::Null);
    assert_eq!(
        ordered_results[1].get("marker"),
        Some(&json!("backend-gate-document"))
    );
    assert_eq!(ordered.invocation.read_accounting.documents, 1);
    assert!(ordered.invocation.read_accounting.bytes > 0);
    assert!(ordered.invocation.read_accounting.intervals > 0);
    assert_eq!(ordered.invocation.opaque_live_handles, 0);
    assert_eq!(ordered.invocation.opaque_current_bytes, 0);
    drop(ordered.invocation.transaction);

    Box::pin(async {
        let query_manifest = generated_test_manifest_with_limits(
            ManifestUdfKind::Query,
            vec![
                json!({
                    "id": 3,
                    "debugName": "generatedBatchCollectById",
                    "operation": {
                        "kind": "databaseIndexQuery",
                        "tableName": table,
                        "indexName": "by_id",
                        "equalityField": "_id",
                        "order": "ascending",
                        "terminal": "collect",
                        "limit": null,
                    },
                }),
                json!({
                    "id": 4,
                    "debugName": "generatedBatchFirstById",
                    "operation": {
                        "kind": "databaseIndexQuery",
                        "tableName": table,
                        "indexName": "by_id",
                        "equalityField": "_id",
                        "order": "ascending",
                        "terminal": "first",
                        "limit": null,
                    },
                }),
                json!({
                    "id": 5,
                    "debugName": "generatedBatchUniqueById",
                    "operation": {
                        "kind": "databaseIndexQuery",
                        "tableName": table,
                        "indexName": "by_id",
                        "equalityField": "_id",
                        "order": "ascending",
                        "terminal": "unique",
                        "limit": null,
                    },
                }),
            ],
            1 << 20,
        )?;
        let index_queries = execute_generated_async_batch_test_module_with_host_operation_trace(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&query_manifest),
            batch_only,
            json!({
                "batch": [
                    invocation(3, json!(missing.encode())),
                    invocation(3, json!(id.encode())),
                    invocation(4, json!(id.encode())),
                    invocation(5, json!(missing.encode())),
                    invocation(4, json!(missing.encode())),
                    invocation(5, json!(id.encode())),
                ],
            }),
            UdfType::Query,
            true,
        )
        .await?;
        assert_eq!(index_queries.invocation.outcome, InvocationOutcome::Success);
        let index_results = index_queries
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .context("generated index batch did not return an array")?;
        assert_eq!(index_results.len(), 6);
        assert_eq!(index_results[0], json!([]));
        let hit_collect = index_results[1]
            .as_array()
            .context("collect terminal did not return an array")?;
        assert_eq!(hit_collect.len(), 1);
        assert_eq!(
            hit_collect[0].get("marker"),
            Some(&json!("backend-gate-document"))
        );
        assert_eq!(
            index_results[2].get("marker"),
            Some(&json!("backend-gate-document"))
        );
        assert_eq!(index_results[3], JsonValue::Null);
        assert_eq!(index_results[4], JsonValue::Null);
        assert_eq!(
            index_results[5].get("marker"),
            Some(&json!("backend-gate-document"))
        );
        assert_eq!(
            generated_async_batch_query_trace(&index_queries)?,
            vec![
                (
                    LogicalHostOperation::DatabaseQueryStream,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStream,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStream,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStream,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStream,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStream,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryCleanup,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryCleanup,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryCleanup,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryCleanup,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryCleanup,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryCleanup,
                    LogicalHostOperationStatus::Success,
                ),
            ]
        );
        assert_eq!(index_queries.invocation.read_accounting.documents, 3);
        assert!(index_queries.invocation.read_accounting.bytes > 0);
        assert!(index_queries.invocation.read_accounting.intervals > 0);
        assert_eq!(index_queries.invocation.opaque_live_handles, 0);
        assert_eq!(index_queries.invocation.opaque_current_bytes, 0);
        drop(index_queries.invocation.transaction);

        let query_operation_limit_manifest = generated_test_manifest_with_runtime_limits(
            ManifestUdfKind::Query,
            vec![json!({
                "id": 3,
                "debugName": "generatedBatchCollectById",
                "operation": {
                    "kind": "databaseIndexQuery",
                    "tableName": table,
                    "indexName": "by_id",
                    "equalityField": "_id",
                    "order": "ascending",
                    "terminal": "collect",
                    "limit": null,
                },
            })],
            1 << 20,
            1_000_000,
            2,
        )?;
        let query_operation_limit = execute_generated_async_batch_test_module(
            rt.clone(),
            database.begin_system().await?,
            query_operation_limit_manifest,
            batch_only,
            json!({
                "batch": [invocation(3, json!(id.encode()))],
            }),
            UdfType::Query,
        )
        .await?;
        assert_eq!(
            query_operation_limit.invocation.outcome,
            InvocationOutcome::SystemError
        );
        assert_eq!(
            query_operation_limit.invocation.read_accounting.documents,
            1
        );
        assert_eq!(query_operation_limit.invocation.opaque_live_handles, 0);
        assert_eq!(query_operation_limit.invocation.opaque_current_bytes, 0);
        drop(query_operation_limit.invocation.transaction);

        let (first_duplicate_id, second_duplicate_id) = {
            let mut transaction = database.begin_system().await?;
            let index_name = IndexName::new(table.clone(), IndexDescriptor::new("by_marker")?)?;
            IndexModel::new(&mut transaction)
                .add_application_index(
                    TableNamespace::root_component(),
                    IndexMetadata::new_enabled(
                        index_name,
                        IndexedFields::try_from(vec!["marker".parse()?])?,
                    ),
                )
                .await?;
            let first =
                SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
                    .insert_metadata(
                        &table,
                        obj!(
                            "marker" => "duplicate-index-value",
                            "sequence" => 11_i64,
                        )?,
                    )
                    .await?
                    .developer_id;
            let second =
                SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
                    .insert_metadata(
                        &table,
                        obj!(
                            "marker" => "duplicate-index-value",
                            "sequence" => 12_i64,
                        )?,
                    )
                    .await?
                    .developer_id;
            database
                .commit_with_write_source(transaction, "generated_batch_duplicate_index_setup")
                .await?;
            (first, second)
        };
        let duplicate_query_manifest = generated_test_manifest_with_limits(
            ManifestUdfKind::Query,
            vec![
                json!({
                    "id": 11,
                    "debugName": "generatedBatchCollectByMarker",
                    "operation": {
                        "kind": "databaseIndexQuery",
                        "tableName": table,
                        "indexName": "by_marker",
                        "equalityField": "marker",
                        "order": "ascending",
                        "terminal": "collect",
                        "limit": null,
                    },
                }),
                json!({
                    "id": 12,
                    "debugName": "generatedBatchUniqueByMarker",
                    "operation": {
                        "kind": "databaseIndexQuery",
                        "tableName": table,
                        "indexName": "by_marker",
                        "equalityField": "marker",
                        "order": "ascending",
                        "terminal": "unique",
                        "limit": null,
                    },
                }),
            ],
            1 << 20,
        )?;
        let multi_page_collect = execute_generated_async_batch_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&duplicate_query_manifest),
            batch_only,
            json!({
                "batch": [invocation(11, json!("duplicate-index-value"))],
            }),
            UdfType::Query,
        )
        .await?;
        assert_eq!(
            multi_page_collect.invocation.outcome,
            InvocationOutcome::Success
        );
        let collected_duplicates = multi_page_collect
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .and_then(|batch| batch.first())
            .and_then(JsonValue::as_array)
            .context("multi-page collect did not return its result array")?;
        assert_eq!(collected_duplicates.len(), 2);
        let collected_ids = collected_duplicates
            .iter()
            .map(|document| {
                document
                    .get("_id")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned)
                    .context("collected duplicate document has no ID")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        assert_eq!(
            collected_ids.iter().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([first_duplicate_id.encode(), second_duplicate_id.encode(),])
        );
        assert_eq!(multi_page_collect.invocation.read_accounting.documents, 2);
        assert_eq!(multi_page_collect.invocation.opaque_live_handles, 0);
        assert_eq!(multi_page_collect.invocation.opaque_current_bytes, 0);
        drop(multi_page_collect.invocation.transaction);

        let non_unique = execute_generated_async_batch_test_module_with_host_operation_trace(
            rt.clone(),
            database.begin_system().await?,
            duplicate_query_manifest,
            batch_only,
            json!({
                "batch": [invocation(12, json!("duplicate-index-value"))],
            }),
            UdfType::Query,
            true,
        )
        .await?;
        assert_eq!(
            non_unique.invocation.outcome,
            InvocationOutcome::DeveloperError(format!(
                "unique() query returned more than one result from table {table}:\n [{}, {}, ...]",
                collected_ids[0], collected_ids[1],
            ))
        );
        assert_eq!(
            generated_async_batch_query_trace(&non_unique)?,
            vec![
                (
                    LogicalHostOperation::DatabaseQueryStream,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryCleanup,
                    LogicalHostOperationStatus::Success,
                ),
            ]
        );
        assert_eq!(non_unique.invocation.read_accounting.documents, 2);
        assert_eq!(non_unique.invocation.opaque_live_handles, 0);
        assert_eq!(non_unique.invocation.opaque_current_bytes, 0);
        drop(non_unique.invocation.transaction);
        anyhow::Ok(())
    })
    .await?;

    let chunk_size = *MAX_SYSCALL_BATCH_SIZE;
    anyhow::ensure!(chunk_size > 0, "async syscall batch size must be nonzero");
    let cross_chunk_arity = chunk_size
        .checked_add(1)
        .context("async syscall batch size overflow")?;
    let cross_chunk_manifest = generated_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        vec![imported_get(1, &table)],
        1 << 20,
        1_000_000,
        u64::try_from(cross_chunk_arity)?,
    )?;
    let cross_chunk_invocations = (0..cross_chunk_arity)
        .map(|index| {
            invocation(
                1,
                if index % 2 == 0 {
                    json!(id.encode())
                } else {
                    json!(missing.encode())
                },
            )
        })
        .collect::<Vec<_>>();
    let cross_chunk = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&cross_chunk_manifest),
        batch_only,
        json!({ "batch": cross_chunk_invocations }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(cross_chunk.invocation.outcome, InvocationOutcome::Success);
    let cross_chunk_results = cross_chunk
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .context("cross-chunk generated batch did not return an array")?;
    assert_eq!(cross_chunk_results.len(), cross_chunk_arity);
    for (index, result) in cross_chunk_results.iter().enumerate() {
        if index % 2 == 0 {
            assert_eq!(result.get("marker"), Some(&json!("backend-gate-document")));
        } else {
            assert_eq!(*result, JsonValue::Null);
        }
    }
    assert_eq!(
        cross_chunk.invocation.read_accounting.documents,
        cross_chunk_arity.div_ceil(2)
    );
    assert_eq!(cross_chunk.invocation.opaque_live_handles, 0);
    assert_eq!(cross_chunk.invocation.opaque_current_bytes, 0);
    drop(cross_chunk.invocation.transaction);

    let first_invalid_argument = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&get_manifest),
        batch_only,
        json!({
            "batch": [
                invocation(1, json!(id.encode())),
                invocation(1, json!("not-a-document-id")),
                invocation(2, json!({ "not": "a document ID" })),
            ],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(
        first_invalid_argument.invocation.outcome,
        InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
    );
    assert_eq!(
        first_invalid_argument
            .invocation
            .first_developer_error_index,
        Some(1)
    );
    assert_eq!(
        first_invalid_argument.invocation.read_accounting.documents,
        1
    );
    assert_eq!(first_invalid_argument.invocation.opaque_live_handles, 0);
    assert_eq!(first_invalid_argument.invocation.opaque_current_bytes, 0);
    drop(first_invalid_argument.invocation.transaction);

    let halt_after_rejection_invocations = (0..chunk_size)
        .map(|_| invocation(1, json!("not-a-document-id")))
        .chain(std::iter::once(invocation(1, json!(id.encode()))))
        .collect::<Vec<_>>();
    let halt_after_rejection = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&cross_chunk_manifest),
        batch_only,
        json!({ "batch": halt_after_rejection_invocations }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(
        halt_after_rejection.invocation.outcome,
        InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
    );
    assert_eq!(
        halt_after_rejection.invocation.first_developer_error_index,
        Some(0)
    );
    assert_eq!(halt_after_rejection.invocation.read_accounting.documents, 0);
    assert_eq!(halt_after_rejection.invocation.read_accounting.intervals, 1);
    assert_eq!(halt_after_rejection.invocation.opaque_live_handles, 0);
    assert_eq!(halt_after_rejection.invocation.opaque_current_bytes, 0);
    drop(halt_after_rejection.invocation.transaction);

    let mixed_manifest = generated_test_manifest_with_limits(
        ManifestUdfKind::Mutation,
        vec![
            imported_get(1, &table),
            json!({
                "id": 2,
                "debugName": "generatedBatchInsert",
                "operation": {
                    "kind": "databaseInsert",
                    "tableName": table,
                },
            }),
        ],
        1 << 20,
    )?;
    let mixed = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        mixed_manifest,
        GeneratedAsyncBatchTestOperation {
            import_database_write: true,
            later_database_write_operation_id: None,
        },
        json!({
            "batch": [
                invocation(1, json!(id.encode())),
                {
                    "operationId": 2,
                    "arguments": [],
                },
            ],
        }),
        UdfType::Mutation,
    )
    .await?;
    assert_eq!(mixed.invocation.outcome, InvocationOutcome::SystemError);
    assert_eq!(mixed.invocation.read_accounting.documents, 0);
    assert_eq!(mixed.invocation.read_accounting.intervals, 1);
    assert_eq!(
        mixed
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    assert_eq!(mixed.invocation.opaque_live_handles, 0);
    assert_eq!(mixed.invocation.opaque_current_bytes, 0);
    drop(mixed.invocation.transaction);

    let invalid_wire = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&get_manifest),
        batch_only,
        json!({
            "batch": [{
                "operationId": 1,
                "arguments": [],
            }],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(
        invalid_wire.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(invalid_wire.invocation.read_accounting.documents, 0);
    assert_eq!(invalid_wire.invocation.read_accounting.intervals, 1);
    assert_eq!(invalid_wire.invocation.opaque_live_handles, 0);
    assert_eq!(invalid_wire.invocation.opaque_current_bytes, 0);
    drop(invalid_wire.invocation.transaction);

    let unknown_operation = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&get_manifest),
        batch_only,
        json!({
            "batch": [invocation(3, json!(id.encode()))],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(
        unknown_operation.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(unknown_operation.invocation.read_accounting.documents, 0);
    assert_eq!(unknown_operation.invocation.read_accounting.intervals, 1);
    assert_eq!(unknown_operation.invocation.opaque_live_handles, 0);
    assert_eq!(unknown_operation.invocation.opaque_current_bytes, 0);
    drop(unknown_operation.invocation.transaction);

    let operation_limit_manifest = generated_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        vec![imported_get(1, &table)],
        1 << 20,
        1_000_000,
        1,
    )?;
    let operation_limit = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        operation_limit_manifest,
        batch_only,
        json!({
            "batch": [
                invocation(1, json!(id.encode())),
                invocation(1, json!(missing.encode())),
            ],
        }),
        UdfType::Query,
    )
    .await?;
    assert_eq!(
        operation_limit.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(operation_limit.invocation.read_accounting.documents, 0);
    assert_eq!(operation_limit.invocation.read_accounting.intervals, 1);
    assert_eq!(operation_limit.invocation.opaque_live_handles, 0);
    assert_eq!(operation_limit.invocation.opaque_current_bytes, 0);
    drop(operation_limit.invocation.transaction);

    let patch_manifest = generated_test_manifest_with_limits(
        ManifestUdfKind::Mutation,
        vec![
            imported_get(1, &table),
            json!({
                "id": 3,
                "debugName": "generatedBatchLaterPatch",
                "operation": {
                    "kind": "databasePatch",
                    "tableName": table,
                },
            }),
        ],
        1 << 20,
    )?;
    let mut patched = execute_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        patch_manifest,
        GeneratedAsyncBatchTestOperation {
            import_database_write: true,
            later_database_write_operation_id: Some(3),
        },
        json!({
            "batch": [
                invocation(1, json!(id.encode())),
                invocation(1, json!(missing.encode())),
            ],
            "id": id.encode(),
            "value": {
                "marker": "patched-after-batch",
                "sequence": 9,
            },
        }),
        UdfType::Mutation,
    )
    .await?;
    assert_eq!(patched.invocation.outcome, InvocationOutcome::Success);
    let batch_before_patch = patched
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .context("batch-then-patch fixture did not return its read results")?;
    assert_eq!(
        batch_before_patch[0].get("marker"),
        Some(&json!("backend-gate-document"))
    );
    assert_eq!(batch_before_patch[1], JsonValue::Null);
    // The batch reads the document once and the later patch reads its preimage.
    assert_eq!(patched.invocation.read_accounting.documents, 2);
    assert_eq!(
        patched
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        1
    );
    let patched_document = UserFacingModel::new(
        &mut patched.invocation.transaction,
        TableNamespace::root_component(),
    )
    .get_with_ts(id, Some(Version::new(1, 42, 3)))
    .await?
    .context("later batch mutation was not visible in its transaction")?
    .0
    .to_internal_json();
    assert_eq!(
        patched_document.get("marker"),
        Some(&json!("patched-after-batch"))
    );
    assert_eq!(patched_document.get("sequence"), Some(&json!(9.0)));
    assert_eq!(patched.invocation.opaque_live_handles, 0);
    assert_eq!(patched.invocation.opaque_current_bytes, 0);
    drop(patched.invocation.transaction);

    Box::pin(async {
        let generic_mutation_manifest = generated_test_manifest_with_limits(
            ManifestUdfKind::Mutation,
            vec![
                json!({
                    "id": 6,
                    "debugName": "generatedBatchPatch",
                    "operation": {
                        "kind": "databasePatch",
                        "tableName": table,
                    },
                }),
                imported_get(7, &table),
            ],
            1 << 20,
        )?;
        let mut ordered_writes = execute_generated_async_batch_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&generic_mutation_manifest),
            batch_only,
            json!({
                "batch": [
                    {
                        "operationId": 6,
                        "arguments": [
                            id.encode(),
                            {
                                "marker": "first-batch-patch",
                                "sequence": 2,
                            },
                        ],
                    },
                    {
                        "operationId": 6,
                        "arguments": [
                            id.encode(),
                            {
                                "marker": "second-batch-patch",
                                "sequence": 3,
                            },
                        ],
                    },
                    invocation(7, json!(id.encode())),
                ],
            }),
            UdfType::Mutation,
        )
        .await?;
        assert_eq!(
            ordered_writes.invocation.outcome,
            InvocationOutcome::Success
        );
        let write_results = ordered_writes
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .context("generic write batch did not return an array")?;
        assert_eq!(write_results[0], JsonValue::Null);
        assert_eq!(write_results[1], JsonValue::Null);
        assert_eq!(
            write_results[2].get("marker"),
            Some(&json!("second-batch-patch"))
        );
        assert_eq!(write_results[2].get("sequence"), Some(&json!(3.0)));
        assert_eq!(
            ordered_writes
                .invocation
                .transaction
                .execution_size()
                .write_size
                .num_writes,
            2
        );
        assert_eq!(ordered_writes.invocation.read_accounting.documents, 3);
        let visible_after_writes = UserFacingModel::new(
            &mut ordered_writes.invocation.transaction,
            TableNamespace::root_component(),
        )
        .get_with_ts(id, Some(Version::new(1, 42, 3)))
        .await?
        .context("ordered batch writes were not visible to their transaction")?
        .0
        .to_internal_json();
        assert_eq!(
            visible_after_writes.get("marker"),
            Some(&json!("second-batch-patch"))
        );
        assert_eq!(ordered_writes.invocation.opaque_live_handles, 0);
        assert_eq!(ordered_writes.invocation.opaque_current_bytes, 0);
        drop(ordered_writes.invocation.transaction);

        let rejected_after_write = execute_generated_async_batch_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&generic_mutation_manifest),
            batch_only,
            json!({
                "batch": [
                    {
                        "operationId": 6,
                        "arguments": [
                            id.encode(),
                            {
                                "marker": "must-roll-back",
                                "sequence": 4,
                            },
                        ],
                    },
                    invocation(7, json!("not-a-document-id")),
                ],
            }),
            UdfType::Mutation,
        )
        .await?;
        assert_eq!(
            rejected_after_write.invocation.outcome,
            InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
        );
        assert_eq!(
            rejected_after_write.invocation.first_developer_error_index,
            Some(1)
        );
        assert_eq!(
            rejected_after_write
                .invocation
                .transaction
                .execution_size()
                .write_size
                .num_writes,
            1
        );
        assert_eq!(rejected_after_write.invocation.opaque_live_handles, 0);
        assert_eq!(rejected_after_write.invocation.opaque_current_bytes, 0);
        drop(rejected_after_write.invocation.transaction);
        let mut persisted = database.begin_system().await?;
        let persisted_document =
            UserFacingModel::new(&mut persisted, TableNamespace::root_component())
                .get_with_ts(id, Some(Version::new(1, 42, 3)))
                .await?
                .context("rollback verification document disappeared")?
                .0
                .to_internal_json();
        assert_eq!(
            persisted_document.get("marker"),
            Some(&json!("backend-gate-document"))
        );
        assert_eq!(
            persisted_document.get("sequence"),
            Some(&json!({ "$integer": "AQAAAAAAAAA=" }))
        );
        drop(persisted);

        install_generated_test_function(&database).await?;
        let scheduler_reference = "_reference/function/scheduled:run";
        let effects_manifest = generated_test_manifest_with_limits(
            ManifestUdfKind::Mutation,
            vec![
                json!({
                    "id": 8,
                    "debugName": "generatedBatchInsert",
                    "operation": {
                        "kind": "databaseInsert",
                        "tableName": table,
                    },
                }),
                json!({
                    "id": 9,
                    "debugName": "generatedBatchRunAfter",
                    "operation": {
                        "kind": "schedulerRunAfter",
                        "functionReference": scheduler_reference,
                    },
                }),
                json!({
                    "id": 10,
                    "debugName": "generatedBatchRunAt",
                    "operation": {
                        "kind": "schedulerRunAt",
                        "functionReference": scheduler_reference,
                    },
                }),
            ],
            1 << 20,
        )?;
        let mut ordered_effects = execute_generated_async_batch_test_module(
            rt.clone(),
            database.begin_system().await?,
            effects_manifest,
            batch_only,
            json!({
                "batch": [
                    {
                        "operationId": 8,
                        "arguments": [{
                            "marker": "inserted-by-generic-batch",
                            "sequence": 8,
                        }],
                    },
                    {
                        "operationId": 9,
                        "arguments": [2_500.0, { "order": 1 }],
                    },
                    {
                        "operationId": 10,
                        "arguments": [1_700_000_010_000.0, { "order": 2 }],
                    },
                ],
            }),
            UdfType::Mutation,
        )
        .await?;
        assert_eq!(
            ordered_effects.invocation.outcome,
            InvocationOutcome::Success
        );
        let effect_results = ordered_effects
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .context("generic effect batch did not return an array")?;
        assert_eq!(effect_results.len(), 3);
        let inserted_id = DeveloperDocumentId::decode(
            effect_results[0]
                .as_str()
                .context("generic insert did not return its public ID")?,
        )?;
        let inserted = UserFacingModel::new(
            &mut ordered_effects.invocation.transaction,
            TableNamespace::root_component(),
        )
        .get_with_ts(inserted_id, Some(Version::new(1, 42, 3)))
        .await?
        .context("generic insert was not visible in its transaction")?
        .0
        .to_internal_json();
        assert_eq!(
            inserted.get("marker"),
            Some(&json!("inserted-by-generic-batch"))
        );
        for (index, (expected_order, expected_timestamp)) in
            [(1.0, 1_700_000_002_500.0), (2.0, 1_700_000_010_000.0)]
                .into_iter()
                .enumerate()
        {
            let virtual_id = DeveloperDocumentId::decode(
                effect_results[index + 1]
                    .as_str()
                    .context("generic scheduler operation did not return its public ID")?,
            )?;
            let scheduled = UserFacingModel::new(
                &mut ordered_effects.invocation.transaction,
                TableNamespace::root_component(),
            )
            .get_with_ts(virtual_id, Some(Version::new(1, 42, 3)))
            .await?
            .context("generic scheduler operation did not create its virtual document")?
            .0
            .to_internal_json();
            assert_eq!(
                scheduled.get("args"),
                Some(&json!([{ "order": expected_order }]))
            );
            assert_eq!(
                scheduled.get("scheduledTime").and_then(JsonValue::as_f64),
                Some(expected_timestamp)
            );
        }
        assert_eq!(
            ordered_effects
                .invocation
                .transaction
                .execution_size()
                .write_size
                .num_writes,
            1
        );
        assert_eq!(
            ordered_effects
                .invocation
                .transaction
                .execution_size()
                .scheduled_size
                .num_writes,
            2
        );
        assert_eq!(ordered_effects.invocation.opaque_live_handles, 0);
        assert_eq!(ordered_effects.invocation.opaque_current_bytes, 0);
        drop(ordered_effects.invocation.transaction);
        anyhow::Ok(())
    })
    .await?;

    let cancellation = CancellationSignal::new_for_test();
    let (control, mut host) = read_control();
    let cancellation_metrics = Arc::new(GateMetrics::default());
    let cancellation_manifest = get_manifest;
    let routed =
        generated_async_batch_test_routed_module(Arc::clone(&cancellation_manifest), batch_only)?;
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        cancellation_manifest,
        json!({
            "batch": [
                invocation(1, json!(id.encode())),
                invocation(2, json!(missing.encode())),
            ],
        }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        cancellation.clone(),
        Some(control),
        Arc::clone(&cancellation_metrics),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let execution = tokio::spawn(execute_generated(routed, state, None, false));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("generated async batch did not become pending")??;
    cancellation.cancel_for_test();
    let cancelled = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("generated async batch cancellation did not complete")???;
    assert!(cancelled.cancelled);
    assert_eq!(
        cancellation_metrics.read_cancelled.load(Ordering::SeqCst),
        1
    );
    assert!(host
        .release
        .take()
        .context("generated async batch cancellation release missing")?
        .send(())
        .is_err());
    assert_eq!(cancelled.invocation.opaque_live_handles, 0);
    assert_eq!(cancelled.invocation.opaque_current_bytes, 0);
    drop(cancelled.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_database_write_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_write_documents".parse()?;
    let timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    let operation = |kind, handle_fault| {
        GeneratedTestOperation::DatabaseWrite(GeneratedDatabaseWriteTestOperation {
            kind,
            operation_id: 1,
            handle_fault,
            import_database_get: false,
        })
    };

    let insert_manifest = generated_test_manifest(
        ManifestUdfKind::Mutation,
        json!({
            "kind": "databaseInsert",
            "tableName": table,
        }),
    )?;
    let mut insert = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&insert_manifest),
        operation(
            GeneratedDatabaseWriteKind::Insert,
            GeneratedDatabaseWriteHandleFault::None,
        ),
        json!({
            "value": {
                "marker": "inserted",
                "sequence": 2,
            },
        }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(insert.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        insert
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        1
    );
    let inserted_id = DeveloperDocumentId::decode(
        insert
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_str)
            .context("database insert did not return a document ID")?,
    )?;
    let inserted = UserFacingModel::new(
        &mut insert.invocation.transaction,
        TableNamespace::root_component(),
    )
    .get_with_ts(inserted_id, Some(Version::new(1, 42, 3)))
    .await?
    .context("database insert was not visible in its transaction")?
    .0
    .to_internal_json();
    assert_eq!(inserted.get("marker"), Some(&json!("inserted")));
    assert_eq!(inserted.get("sequence"), Some(&json!(2.0)));
    drop(insert.invocation.transaction);

    let patch_id = insert_document(&database, &table).await?.developer_id;
    let patch_manifest = generated_test_manifest(
        ManifestUdfKind::Mutation,
        json!({
            "kind": "databasePatch",
            "tableName": table,
        }),
    )?;
    let mut patch = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&patch_manifest),
        operation(
            GeneratedDatabaseWriteKind::Patch,
            GeneratedDatabaseWriteHandleFault::None,
        ),
        json!({
            "id": patch_id.encode(),
            "value": {
                "sequence": 3,
            },
            "result": null,
        }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(patch.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(patch.invocation.function_result, Some(JsonValue::Null));
    assert_eq!(
        patch
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        1
    );
    let patched = UserFacingModel::new(
        &mut patch.invocation.transaction,
        TableNamespace::root_component(),
    )
    .get_with_ts(patch_id, Some(Version::new(1, 42, 3)))
    .await?
    .context("database patch was not visible in its transaction")?
    .0
    .to_internal_json();
    assert_eq!(patched.get("marker"), Some(&json!("backend-gate-document")));
    assert_eq!(patched.get("sequence"), Some(&json!(3.0)));
    drop(patch.invocation.transaction);

    let replace_id = insert_document(&database, &table).await?.developer_id;
    let replace_manifest = generated_test_manifest(
        ManifestUdfKind::Mutation,
        json!({
            "kind": "databaseReplace",
            "tableName": table,
        }),
    )?;
    let mut replace = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        replace_manifest,
        operation(
            GeneratedDatabaseWriteKind::Replace,
            GeneratedDatabaseWriteHandleFault::None,
        ),
        json!({
            "id": replace_id.encode(),
            "value": {
                "marker": "replaced",
            },
            "result": null,
        }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(replace.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        replace
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        1
    );
    let replaced = UserFacingModel::new(
        &mut replace.invocation.transaction,
        TableNamespace::root_component(),
    )
    .get_with_ts(replace_id, Some(Version::new(1, 42, 3)))
    .await?
    .context("database replace was not visible in its transaction")?
    .0
    .to_internal_json();
    assert_eq!(replaced.get("marker"), Some(&json!("replaced")));
    assert!(replaced.get("sequence").is_none());
    drop(replace.invocation.transaction);

    let delete_id = insert_document(&database, &table).await?.developer_id;
    let delete_manifest = generated_test_manifest(
        ManifestUdfKind::Mutation,
        json!({
            "kind": "databaseDelete",
            "tableName": table,
        }),
    )?;
    let mut delete = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        delete_manifest,
        operation(
            GeneratedDatabaseWriteKind::Delete,
            GeneratedDatabaseWriteHandleFault::None,
        ),
        json!({
            "id": delete_id.encode(),
            "result": null,
        }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(delete.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        delete
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        1
    );
    assert!(UserFacingModel::new(
        &mut delete.invocation.transaction,
        TableNamespace::root_component(),
    )
    .get_with_ts(delete_id, Some(Version::new(1, 42, 3)))
    .await?
    .is_none());
    drop(delete.invocation.transaction);

    let invalid_id = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&patch_manifest),
        operation(
            GeneratedDatabaseWriteKind::Patch,
            GeneratedDatabaseWriteHandleFault::None,
        ),
        json!({
            "id": "not-a-document-id",
            "value": {
                "sequence": 4,
            },
        }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(
        invalid_id.invocation.outcome,
        InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
    );
    assert_eq!(
        invalid_id
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    drop(invalid_id.invocation.transaction);

    let invalid_value = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&insert_manifest),
        operation(
            GeneratedDatabaseWriteKind::Insert,
            GeneratedDatabaseWriteHandleFault::None,
        ),
        json!({ "value": "not-an-object" }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(
        invalid_value.invocation.outcome,
        InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
    );
    assert_eq!(
        invalid_value
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    drop(invalid_value.invocation.transaction);

    let other_table: TableName = "generated_other_write_documents".parse()?;
    let _other_id = insert_document(&database, &other_table).await?;
    let cross_table_manifest = generated_test_manifest(
        ManifestUdfKind::Mutation,
        json!({
            "kind": "databasePatch",
            "tableName": other_table,
        }),
    )?;
    let mut cross_table = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        cross_table_manifest,
        operation(
            GeneratedDatabaseWriteKind::Patch,
            GeneratedDatabaseWriteHandleFault::None,
        ),
        json!({
            "id": patch_id.encode(),
            "value": {
                "sequence": 6,
            },
        }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(
        cross_table.invocation.outcome,
        InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
    );
    assert_eq!(
        cross_table
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    let unchanged = UserFacingModel::new(
        &mut cross_table.invocation.transaction,
        TableNamespace::root_component(),
    )
    .get_with_ts(patch_id, Some(Version::new(1, 42, 3)))
    .await?
    .context("cross-table write removed the original document")?
    .0
    .to_internal_json();
    assert_eq!(
        unchanged.get("marker"),
        Some(&json!("backend-gate-document"))
    );
    let initial_sequence = ConvexValue::Int64(1).to_internal_json();
    assert_eq!(unchanged.get("sequence"), Some(&initial_sequence));
    drop(cross_table.invocation.transaction);

    let invalid_operation = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&insert_manifest),
        GeneratedTestOperation::DatabaseWrite(GeneratedDatabaseWriteTestOperation {
            kind: GeneratedDatabaseWriteKind::Insert,
            operation_id: 2,
            handle_fault: GeneratedDatabaseWriteHandleFault::None,
            import_database_get: false,
        }),
        json!({ "value": { "marker": "invalid-operation" } }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(
        invalid_operation.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(
        invalid_operation
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    drop(invalid_operation.invocation.transaction);

    let wrong_descriptor_manifest = generated_test_manifest_with_limits(
        ManifestUdfKind::Mutation,
        vec![
            json!({
                "id": 1,
                "debugName": "insertDocument",
                "operation": {
                    "kind": "databaseInsert",
                    "tableName": table,
                },
            }),
            json!({
                "id": 2,
                "debugName": "getDocument",
                "operation": {
                    "kind": "databaseGet",
                    "tableName": table,
                },
            }),
        ],
        1 << 20,
    )?;
    let wrong_descriptor = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        wrong_descriptor_manifest,
        GeneratedTestOperation::DatabaseWrite(GeneratedDatabaseWriteTestOperation {
            kind: GeneratedDatabaseWriteKind::Insert,
            operation_id: 2,
            handle_fault: GeneratedDatabaseWriteHandleFault::None,
            import_database_get: true,
        }),
        json!({ "value": { "marker": "wrong-descriptor" } }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(
        wrong_descriptor.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(
        wrong_descriptor
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    drop(wrong_descriptor.invocation.transaction);

    let wrong_id_shape = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        patch_manifest,
        operation(
            GeneratedDatabaseWriteKind::Patch,
            GeneratedDatabaseWriteHandleFault::WrongIdShape,
        ),
        json!({
            "value": {
                "sequence": 5,
            },
        }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(
        wrong_id_shape.invocation.outcome,
        InvocationOutcome::DeveloperError("InvalidArgument".to_owned())
    );
    assert_eq!(
        wrong_id_shape
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    drop(wrong_id_shape.invocation.transaction);

    let stale_handle = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        insert_manifest,
        operation(
            GeneratedDatabaseWriteKind::Insert,
            GeneratedDatabaseWriteHandleFault::StaleValue,
        ),
        json!({ "value": { "marker": "stale-handle" } }),
        UdfType::Mutation,
        timestamp,
    )
    .await?;
    assert_eq!(
        stale_handle.invocation.outcome,
        InvocationOutcome::SystemError
    );
    assert_eq!(
        stale_handle
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    drop(stale_handle.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_function_handle_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let database = new_test_database(rt.clone()).await?;
    install_generated_test_function(&database).await?;
    let reference = "_reference/function/scheduled:run";
    let manifest = generated_test_manifest(
        ManifestUdfKind::Query,
        json!({ "kind": "functionHandleCreate" }),
    )?;
    let operation =
        GeneratedTestOperation::FunctionHandleCreate(GeneratedFunctionHandleTestOperation {
            operation_id: 1,
            stale_reference: false,
        });

    let mut direct = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        operation,
        json!({ "functionAddress": { "reference": reference } }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .await?;
    assert_eq!(direct.invocation.outcome, InvocationOutcome::Success);
    let direct_handle = direct
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_str)
        .context("function-handle import did not return a string")?
        .to_owned();
    let parsed_handle = direct_handle.parse()?;
    let resolved = FunctionHandlesModel::new(&mut direct.invocation.transaction)
        .lookup(parsed_handle)
        .await?;
    assert_eq!(resolved.component, ComponentPath::root());
    assert_eq!(resolved.udf_path, "scheduled:run".parse()?);
    assert_eq!(direct.invocation.read_accounting.documents, 0);
    assert_eq!(direct.invocation.read_accounting.bytes, 0);
    // One canonical URL initialization range plus the function-handle lookup.
    assert_eq!(direct.invocation.read_accounting.intervals, 2);
    assert_eq!(
        direct
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes,
        0
    );
    assert_eq!(
        direct
            .invocation
            .transaction
            .execution_size()
            .scheduled_size
            .num_writes,
        0
    );
    assert!(!direct.invocation.observed_identity);
    assert_eq!(direct.invocation.opaque_live_handles, 0);
    assert_eq!(direct.invocation.opaque_current_bytes, 0);
    drop(direct.invocation.transaction);

    for (function_address, expected_read_intervals) in [
        (json!({ "name": "scheduled:run" }), 2),
        (json!({ "functionHandle": direct_handle.clone() }), 1),
    ] {
        let repeated = execute_generated_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            operation,
            json!({ "functionAddress": function_address }),
            UdfType::Query,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        )
        .await?;
        assert_eq!(repeated.invocation.outcome, InvocationOutcome::Success);
        assert_eq!(
            repeated
                .invocation
                .function_result
                .as_ref()
                .and_then(JsonValue::as_str),
            Some(direct_handle.as_str())
        );
        assert_eq!(repeated.invocation.read_accounting.documents, 0);
        assert_eq!(repeated.invocation.read_accounting.bytes, 0);
        assert_eq!(
            repeated.invocation.read_accounting.intervals,
            expected_read_intervals
        );
        assert_eq!(repeated.invocation.opaque_live_handles, 0);
        assert_eq!(repeated.invocation.opaque_current_bytes, 0);
        if expected_read_intervals == 1 {
            assert_only_initialization_reads(repeated.invocation.transaction).await?;
        } else {
            drop(repeated.invocation.transaction);
        }
    }

    for invalid_address in [
        json!(null),
        json!("scheduled:run"),
        json!({}),
        json!({ "name": "" }),
        json!({
            "name": "scheduled:run",
            "reference": reference,
        }),
        json!({ "reference": "_reference/function/missing:run" }),
    ] {
        let invalid = execute_generated_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&manifest),
            operation,
            json!({ "functionAddress": invalid_address }),
            UdfType::Query,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        )
        .await?;
        assert!(matches!(
            invalid.invocation.outcome,
            InvocationOutcome::DeveloperError(_)
        ));
        assert_eq!(
            invalid
                .invocation
                .transaction
                .execution_size()
                .write_size
                .num_writes,
            0
        );
        assert_eq!(invalid.invocation.opaque_live_handles, 0);
        assert_eq!(invalid.invocation.opaque_current_bytes, 0);
        drop(invalid.invocation.transaction);
    }

    let stale = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        GeneratedTestOperation::FunctionHandleCreate(GeneratedFunctionHandleTestOperation {
            operation_id: 1,
            stale_reference: true,
        }),
        json!({ "functionAddress": { "reference": reference } }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
    )
    .await?;
    assert_eq!(stale.invocation.outcome, InvocationOutcome::SystemError);
    assert_eq!(stale.invocation.read_accounting.documents, 0);
    assert_eq!(stale.invocation.opaque_live_handles, 0);
    assert_eq!(stale.invocation.opaque_current_bytes, 0);
    drop(stale.invocation.transaction);

    let batch_operation = GeneratedAsyncBatchTestOperation {
        import_database_write: false,
        later_database_write_operation_id: None,
    };
    let routed = generated_async_batch_test_routed_module(Arc::clone(&manifest), batch_operation)?;
    let route_identity = routed.route_identity.clone();
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 1,
        soft_budget_bytes: 12 * WASM_PAGE_BYTES,
        hard_budget_bytes: 16 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 8 * WASM_PAGE_BYTES,
    });
    let metrics = Arc::new(GateMetrics::default());
    let request = || {
        json!({
            "batch": [{
                "operationId": 1,
                "arguments": [{ "reference": reference }],
            }],
        })
    };
    let mut first = execute_reusable_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        Arc::clone(&routed),
        request(),
        Arc::clone(&controller),
        Arc::clone(&metrics),
        None,
        true,
    )
    .await?;
    assert_eq!(first.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        first
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .and_then(|values| values.first())
            .and_then(JsonValue::as_str),
        Some(direct_handle.as_str())
    );
    assert_eq!(first.invocation.read_accounting.documents, 0);
    assert_eq!(first.invocation.read_accounting.bytes, 0);
    assert_eq!(first.invocation.read_accounting.intervals, 2);
    assert_eq!(first.invocation.opaque_live_handles, 0);
    assert_eq!(first.invocation.opaque_current_bytes, 0);
    let first_runtime_id = first
        .reusable_instance
        .as_ref()
        .context("first function-handle invocation did not retain its runtime")?
        .id;
    let retained = retain_generated_test_runtime(&mut first)?;
    drop(first.invocation.transaction);

    let mut reused = execute_reusable_generated_async_batch_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        Arc::clone(&routed),
        request(),
        Arc::clone(&controller),
        Arc::clone(&metrics),
        Some(retained),
        true,
    )
    .await?;
    assert_eq!(reused.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        reused
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .and_then(|values| values.first())
            .and_then(JsonValue::as_str),
        Some(direct_handle.as_str())
    );
    assert_eq!(reused.invocation.read_accounting.documents, 0);
    assert_eq!(reused.invocation.read_accounting.bytes, 0);
    assert_eq!(reused.invocation.read_accounting.intervals, 2);
    assert_eq!(reused.invocation.opaque_live_handles, 0);
    assert_eq!(reused.invocation.opaque_current_bytes, 0);
    assert_eq!(
        reused
            .reusable_instance
            .as_ref()
            .context("reused function-handle invocation did not retain its runtime")?
            .id,
        first_runtime_id
    );
    let reused_instance = reused
        .reusable_instance
        .take()
        .context("reused function-handle runtime disappeared before cleanup")?;
    discard_generated_instance(reused_instance).await?;
    reused
        .memory_permit
        .take()
        .context("reused function-handle memory permit disappeared before cleanup")?
        .finish(reused.terminal_memory_outcome, false);
    drop(reused.invocation.transaction);

    let cancellation = CancellationSignal::new_for_test();
    let (control, mut host) = read_control();
    let cancellation_metrics = Arc::new(GateMetrics::default());
    let cancellation_routed = generated_test_routed_module(Arc::clone(&manifest), operation)?;
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        json!({ "functionAddress": { "reference": reference } }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        cancellation.clone(),
        Some(control),
        Arc::clone(&cancellation_metrics),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let execution = tokio::spawn(execute_generated(cancellation_routed, state, None, false));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("function-handle syscall did not become pending")??;
    cancellation.cancel_for_test();
    let cancelled = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("function-handle cancellation did not complete")???;
    assert!(cancelled.cancelled);
    assert_eq!(
        cancellation_metrics.read_cancelled.load(Ordering::SeqCst),
        1
    );
    assert!(host
        .release
        .take()
        .context("function-handle cancellation release missing")?
        .send(())
        .is_err());
    assert_eq!(cancelled.invocation.opaque_live_handles, 0);
    assert_eq!(cancelled.invocation.opaque_current_bytes, 0);
    drop(cancelled.invocation.transaction);

    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &route_identity,
    );
    let completed = controller.snapshot_for_test(&memory_identity);
    assert_eq!(completed.active_instances, 0);
    assert_eq!(completed.idle_instances, 0);
    database.shutdown().await?;
    Ok(())
}

#[cfg(test)]
async fn run_generated_scheduler_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    install_generated_test_function(&database).await?;
    let base_timestamp = UnixTimestamp::from_nanos(1_700_000_000_000_000_000);
    let reference = "_reference/function/scheduled:run";

    for (operation, descriptor, expected_timestamp_milliseconds) in [
        (
            GeneratedTestOperation::Scheduler {
                time_milliseconds: 2_500.0,
            },
            json!({
                "kind": "schedulerRunAfter",
                "functionReference": reference,
            }),
            1_700_000_002_500.0,
        ),
        (
            GeneratedTestOperation::Scheduler {
                time_milliseconds: 1_700_000_010_000.0,
            },
            json!({
                "kind": "schedulerRunAt",
                "functionReference": reference,
            }),
            1_700_000_010_000.0,
        ),
    ] {
        let manifest = generated_test_manifest(ManifestUdfKind::Mutation, descriptor)?;
        let mut output = execute_generated_test_module(
            rt.clone(),
            database.begin_system().await?,
            manifest,
            operation,
            json!({ "args": { "payload": 7 } }),
            UdfType::Mutation,
            base_timestamp,
        )
        .await?;
        assert!(!output.cancelled);
        assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
        assert_eq!(
            output
                .invocation
                .transaction
                .execution_size()
                .scheduled_size
                .num_writes,
            1
        );
        let virtual_id = DeveloperDocumentId::decode(
            output
                .invocation
                .function_result
                .as_ref()
                .and_then(JsonValue::as_str)
                .context("scheduler import did not return a document ID")?,
        )?;
        let scheduled = UserFacingModel::new(
            &mut output.invocation.transaction,
            TableNamespace::root_component(),
        )
        .get_with_ts(virtual_id, Some(Version::new(1, 42, 3)))
        .await?
        .context("scheduler import did not create its virtual document")?
        .0
        .to_internal_json();
        assert_eq!(scheduled.get("args"), Some(&json!([{ "payload": 7.0 }])));
        assert_eq!(
            scheduled.get("scheduledTime").and_then(JsonValue::as_f64),
            Some(expected_timestamp_milliseconds)
        );
        drop(output.invocation.transaction);
    }

    let invalid_reference_manifest = generated_test_manifest(
        ManifestUdfKind::Mutation,
        json!({
            "kind": "schedulerRunAfter",
            "functionReference": "_reference/function/missing:run",
        }),
    )?;
    let invalid_reference = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        invalid_reference_manifest,
        GeneratedTestOperation::Scheduler {
            time_milliseconds: 0.0,
        },
        json!({ "args": { "payload": 7 } }),
        UdfType::Mutation,
        base_timestamp,
    )
    .await?;
    assert!(matches!(
        invalid_reference.invocation.outcome,
        InvocationOutcome::DeveloperError(_)
    ));
    assert_eq!(
        invalid_reference
            .invocation
            .transaction
            .execution_size()
            .scheduled_size
            .num_writes,
        0
    );
    drop(invalid_reference.invocation.transaction);

    let invalid_args_manifest = generated_test_manifest(
        ManifestUdfKind::Mutation,
        json!({
            "kind": "schedulerRunAfter",
            "functionReference": reference,
        }),
    )?;
    let invalid_args = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        invalid_args_manifest,
        GeneratedTestOperation::Scheduler {
            time_milliseconds: 0.0,
        },
        json!({
            "args": {
                "$integer": "not-valid-base64",
            },
        }),
        UdfType::Mutation,
        base_timestamp,
    )
    .await?;
    assert!(matches!(
        invalid_args.invocation.outcome,
        InvocationOutcome::DeveloperError(_)
    ));
    assert_eq!(
        invalid_args
            .invocation
            .transaction
            .execution_size()
            .scheduled_size
            .num_writes,
        0
    );
    drop(invalid_args.invocation.transaction);

    let encoded_args = scheduler_syscall_args(
        reference.to_owned(),
        base_timestamp.as_secs_f64(),
        json!({ "payload": 7 }),
    );
    assert!(encoded_args["args"].is_object());
    assert_eq!(encoded_args["args"], json!({ "payload": 7 }));

    database.shutdown().await?;
    Ok(())
}

#[test]
#[ignore = "requires a producer-built Static Hermes Core Wasm fixture"]
fn generated_wasm_official_output_chunk_descriptor_smoke() -> anyhow::Result<()> {
    let module_path = std::env::var_os(OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_MODULE_ENV)
        .map(PathBuf::from)
        .context(format!(
            "{OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_MODULE_ENV} must identify a Core Wasm fixture"
        ))?;
    let expectation_path = std::env::var_os(OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_EXPECTATION_ENV)
        .map(PathBuf::from)
        .context(format!(
            "{OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_EXPECTATION_ENV} must identify an expectation \
             report"
        ))?;
    anyhow::ensure!(
        module_path.is_absolute(),
        "{OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_MODULE_ENV} must be an absolute path"
    );
    anyhow::ensure!(
        expectation_path.is_absolute(),
        "{OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_EXPECTATION_ENV} must be an absolute path"
    );
    let expectation =
        OfficialOutputChunkSmokeExpectation::parse(&fs::read(&expectation_path).with_context(
            || format!("failed to read official-output chunk expectation {expectation_path:?}"),
        )?)
        .with_context(|| {
            format!("failed to validate official-output chunk expectation {expectation_path:?}")
        })?;
    let module_bytes = fs::read(&module_path)
        .with_context(|| format!("failed to read official-output chunk module {module_path:?}"))?;
    expectation.validate_module_bytes(&module_bytes)?;
    let package_directory = module_path
        .parent()
        .context("official-output chunk module path has no package directory")?
        .to_owned();
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_official_output_chunk_descriptor_smoke",
        run_generated_official_output_chunk_descriptor_smoke(rt, package_directory, &expectation),
    )
}

#[test]
#[ignore = "requires a producer-built Static Hermes Core Wasm fixture"]
fn generated_wasm_native_capability_jsi_reuses_runtime() -> anyhow::Result<()> {
    // Set CONVEX_WASM_NATIVE_CAPABILITY_TEST_PACKAGE_DIRECTORY to the
    // producer-built authenticated package directory (including module.cwasm),
    // set CONVEX_WASM_NATIVE_CAPABILITY_TEST_EXPECTATION to the absolute path of
    // its expectation report, then run: scripts/run_cargo.sh test -p isolate
    // --features static-hermes-wasmtime-gate
    // generated_wasm_native_capability_jsi_reuses_runtime --lib -- --ignored
    // --nocapture
    //
    // This producer-built package crosses the Static Hermes object/result and
    // runtime-reuse boundary that the hand-encoded host ABI fixtures do not
    // exercise.
    let package_directory = std::env::var_os(NATIVE_CAPABILITY_TEST_PACKAGE_DIRECTORY_ENV)
        .map(PathBuf::from)
        .context(format!(
            "{NATIVE_CAPABILITY_TEST_PACKAGE_DIRECTORY_ENV} must identify a package directory"
        ))?;
    let expectation_path = std::env::var_os(NATIVE_CAPABILITY_TEST_EXPECTATION_ENV)
        .map(PathBuf::from)
        .context(format!(
            "{NATIVE_CAPABILITY_TEST_EXPECTATION_ENV} must identify an expectation report"
        ))?;
    anyhow::ensure!(
        package_directory.is_absolute(),
        "{NATIVE_CAPABILITY_TEST_PACKAGE_DIRECTORY_ENV} must be an absolute path"
    );
    anyhow::ensure!(
        expectation_path.is_absolute(),
        "{NATIVE_CAPABILITY_TEST_EXPECTATION_ENV} must be an absolute path"
    );
    let expectation =
        NativeCapabilityTestExpectation::parse(&fs::read(&expectation_path).with_context(
            || format!("failed to read native capability expectation {expectation_path:?}"),
        )?)
        .with_context(|| {
            format!("failed to validate native capability expectation {expectation_path:?}")
        })?;
    let module_path = package_directory.join("module.wasm");
    let module_bytes = fs::read(&module_path)
        .with_context(|| format!("failed to read native capability module {module_path:?}"))?;
    expectation.validate_module_bytes(&module_bytes)?;
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_native_capability_jsi",
        run_generated_native_capability_jsi_tests(rt, package_directory, &expectation),
    )
}

#[test]
fn generated_wasm_host_secret_verification_is_invocation_scoped() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_host_secret_verification",
        run_generated_host_secret_verification_tests(rt),
    )
}

// Broad acceptance futures can exceed Rust's default test-thread stack. This
// changes only test-harness headroom, not the production runtime stack policy.
fn run_generated_test_with_large_stack(
    name: &'static str,
    test: impl FnOnce() -> anyhow::Result<()> + Send + 'static,
) -> anyhow::Result<()> {
    let test_thread = std::thread::Builder::new()
        .name(name.to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(test)
        .with_context(|| format!("failed to spawn {name} test thread"))?;
    match test_thread.join() {
        Ok(result) => result,
        Err(_) => anyhow::bail!("{name} test thread panicked"),
    }
}

#[test]
fn generated_wasm_database_normalize_id_uses_transactional_sync_syscall() -> anyhow::Result<()> {
    run_generated_test_with_large_stack("generated-wasm-normalize-id", || {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on(
            "generated_wasm_database_normalize_id",
            run_generated_database_normalize_id_tests(rt),
        )
    })
}

#[test]
fn generated_wasm_environment_variables_are_transactional_and_invalidate_reuse(
) -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_environment_variables",
        run_generated_environment_variable_tests(rt),
    )
}

#[test]
fn generated_wasm_storage_provider_uses_retained_environment_data() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_storage_provider",
        run_gate_provider_file_storage_tests(rt),
    )
}

#[test]
fn generated_wasm_deployed_source_identity_uses_invocation_transaction() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_deployed_source_identity",
        run_deployed_source_identity_gate_tests(rt),
    )
}

#[test]
fn generated_wasm_generation_retirement_destroys_active_and_idle_runtimes() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_generation_retirement",
        run_generation_retirement_tests(rt),
    )
}

#[test]
fn generated_wasm_lifecycle_barrier_pins_active_generation_and_cleans_up() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_lifecycle_barrier",
        run_generated_lifecycle_barrier_tests(rt),
    )
}

#[test]
fn generated_wasm_lifecycle_barrier_configuration_fails_closed() -> anyhow::Result<()> {
    let directory = generated_lifecycle_test_directory()?;
    assert!(GeneratedLifecycleBarrier::load(directory.path().to_owned(), ":".to_owned(),).is_err());

    publish_generated_lifecycle_control_file(directory.path(), "stale", b"stale\n")?;
    assert!(GeneratedLifecycleBarrier::load(
        directory.path().to_owned(),
        "generated_test:run".to_owned(),
    )
    .is_err());
    fs::remove_file(directory.path().join("stale"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755))?;
        assert!(GeneratedLifecycleBarrier::load(
            directory.path().to_owned(),
            "generated_test:run".to_owned(),
        )
        .is_err());
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
    }

    assert!(GeneratedLifecycleBarrier::load(
        directory.path().to_owned(),
        "generated_test:run".to_owned(),
    )
    .is_ok());
    Ok(())
}

#[test]
fn generated_wasm_suspended_syscall_uses_system_timeout() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_suspended_system_timeout",
        run_generated_suspended_system_timeout(rt),
    )
}

#[test]
fn generated_wasm_database_resource_limits_discard_reused_runtime() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_database_resource_limits",
        run_generated_database_resource_limit_tests(rt),
    )
}

#[test]
fn generated_wasm_authentication_uses_transactional_async_syscall() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_authentication_get_user_identity",
        run_generated_authentication_get_user_identity_tests(rt),
    )
}

#[test]
fn generated_wasm_compound_query_take_preserves_generic_query_contract() -> anyhow::Result<()> {
    run_generated_test_with_large_stack("generated-wasm-compound-query", || {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on(
            "generated_wasm_compound_query_take",
            run_generated_compound_query_take_tests(rt),
        )
    })
}

#[test]
fn generated_wasm_direct_async_batch_preserves_promise_all_contract() -> anyhow::Result<()> {
    run_generated_test_with_large_stack("generated-wasm-direct-async-batch", || {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on(
            "generated_wasm_direct_async_batch",
            run_generated_direct_async_batch_tests(rt),
        )
    })
}

#[test]
fn generated_wasm_database_writes_use_transactional_async_syscalls() -> anyhow::Result<()> {
    run_generated_test_with_large_stack("generated-wasm-database-writes", || {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on(
            "generated_wasm_database_writes",
            run_generated_database_write_tests(rt),
        )
    })
}

#[test]
fn paired_insert_then_first_read_dependencies() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("paired_insert_then_first", async move {
        let database = new_test_database(rt).await?;
        let table: TableName = "pending_operations".parse()?;
        let inherited_table: TableName = "inherited_operations".parse()?;
        let namespace = TableNamespace::root_component();
        let mut setup = database.begin_system().await?;
        database::TableModel::new(&mut setup)
            .insert_table_metadata(namespace, &table)
            .await?;
        database::TableModel::new(&mut setup)
            .insert_table_metadata(namespace, &inherited_table)
            .await?;
        database
            .commit_with_write_source(setup, "test_setup")
            .await?;
        let timestamp = *database.now_ts_for_reads();
        let mut inherited = database
            .begin_with_ts(
                Identity::system(),
                timestamp,
                usage_tracking::FunctionUsageTracker::new(),
            )
            .await?;
        let inherited_id = SystemMetadataModel::new(&mut inherited, namespace)
            .insert_metadata(&inherited_table, obj!("status" => "ready")?)
            .await?;
        let (_, inherited_writes) = inherited.into_reads_and_writes();
        let inherited_writes = inherited_writes
            .as_flat()?
            .coalesced_writes()
            .cloned()
            .collect::<Vec<_>>();
        for (share_allocation_inputs, extra_shadow_read) in
            [(false, false), (true, false), (true, true)]
        {
            let mut lanes = Vec::new();
            let mut allocation_inputs = None;
            for is_shadow in [false, true] {
                let mut transaction = database
                    .begin_with_ts(
                        Identity::system(),
                        timestamp,
                        usage_tracking::FunctionUsageTracker::new(),
                    )
                    .await?;
                // Match function-runner setup: merge the same pending writes
                // before capturing or applying the initial allocation cursor.
                transaction.merge_writes(inherited_writes.clone())?;
                assert_eq!(
                    transaction
                        .get(inherited_id)
                        .await?
                        .context("inherited insert missing from paired transaction")?
                        .id(),
                    inherited_id,
                );
                if is_shadow && share_allocation_inputs {
                    transaction.apply_document_creation_state(
                        allocation_inputs
                            .as_ref()
                            .context("primary allocation inputs missing")?,
                    )?;
                } else if !is_shadow {
                    allocation_inputs = Some(transaction.document_creation_state());
                }
                let id = SystemMetadataModel::new(&mut transaction, namespace)
                    .insert_metadata(&table, obj!("status" => "pending")?)
                    .await?;
                let mut query = database::ResolvedQuery::new(
                    &mut transaction,
                    namespace,
                    common::query::Query::full_table_scan(table.clone(), common::query::Order::Asc),
                )?;
                let first = query
                    .next(&mut transaction, None)
                    .await?
                    .context("inserted operation missing from its own transaction")?;
                assert_eq!(first.id(), id);
                if is_shadow && extra_shadow_read {
                    assert!(query.next(&mut transaction, None).await?.is_none());
                }
                let (reads, _) = transaction.into_reads_and_writes();
                lanes.push((id, first.creation_time(), reads));
            }
            let [primary, shadow]: [_; 2] = lanes
                .try_into()
                .ok()
                .context("paired read observations missing")?;
            let (primary_id, primary_time, primary_reads) = primary;
            let (shadow_id, shadow_time, shadow_reads) = shadow;
            assert_eq!(primary_id == shadow_id, share_allocation_inputs);
            if share_allocation_inputs {
                assert_eq!(primary_time, shadow_time);
            }
            let mapping = BTreeMap::from([(primary_id, shadow_id)]);
            assert_eq!(
                primary_reads
                    .read_set()
                    .has_same_read_dependencies_with_lane_local_insert_ids(
                        shadow_reads.read_set(),
                        &mapping,
                    ),
                share_allocation_inputs && !extra_shadow_read,
                "{:?}",
                primary_reads
                    .read_set()
                    .comparison_diagnostic(shadow_reads.read_set())
            );
            assert_eq!(
                primary_reads
                    .read_set()
                    .has_same_read_dependencies(shadow_reads.read_set()),
                share_allocation_inputs && !extra_shadow_read,
            );
        }
        database.shutdown().await?;
        Ok(())
    })
}

#[test]
fn generated_wasm_function_handle_create_uses_transactional_async_syscall() -> anyhow::Result<()> {
    run_generated_test_with_large_stack("generated-wasm-function-handle", || {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on(
            "generated_wasm_function_handle_create",
            run_generated_function_handle_tests(rt),
        )
    })
}

#[test]
fn generated_wasm_scheduler_uses_virtual_scheduler_model() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_scheduler",
        run_generated_scheduler_tests(rt),
    )
}

#[test]
fn generated_wasm_fuel_exhaustion_is_a_resource_limit() -> anyhow::Result<()> {
    const CHILD_ENV: &str = "GENERATED_WASM_FUEL_TEST_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        // The fuel override is process-global. It must not exhaust unrelated
        // generated guests running concurrently in the same test suite.
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                concat!(
                    "environment::udf::static_hermes_wasmtime_gate::runtime_acceptance_tests::",
                    "generated_wasm_fuel_exhaustion_is_a_resource_limit",
                ),
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()?;
        anyhow::ensure!(
            output.status.success(),
            "fuel isolation test failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        return Ok(());
    }
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    block_rt.block_on(
        "generated_wasm_fuel_exhaustion",
        run_generated_fuel_acceptance(rt),
    )
}

#[test]
fn generated_wasm_fixed_entry_prepares_before_running() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_fixed_entry_prepare",
        run_generated_fixed_entry_prepare_test(rt),
    )
}

#[test]
fn generated_wasm_warm_entry_preparation_preserves_environment_dependencies() -> anyhow::Result<()>
{
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_warm_entry_prepare_environment",
        run_generated_warm_entry_prepare_environment_test(rt),
    )
}

#[test]
fn generated_wasm_preparation_timeout_precedes_handler_read_capture() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    rt.clone()
        .block_on("generated_preparation_timeout_capture", async move {
            const SELECTOR: u64 = 0xf123_4567_89ab_cdef;
            let database = new_test_database(rt.clone()).await?;
            for (udf_type, manifest_kind) in [
                (UdfType::Query, ManifestUdfKind::Query),
                (UdfType::Mutation, ManifestUdfKind::Mutation),
            ] {
                for (timed_out, shadow) in
                    [(false, false), (false, true), (true, false), (true, true)]
                {
                    let manifest = generated_schema_five_test_manifest_with_runtime_limits(
                        manifest_kind,
                        Vec::new(),
                        1 << 20,
                        1_000_000,
                        128,
                    )?;
                    let routed = generated_test_routed_module_from_bytes(
                        Arc::clone(&manifest),
                        generated_selected_entry_dispatch_fault_test_module(
                            if timed_out {
                                GeneratedSelectedEntryDispatchFault::PreparationTimeout
                            } else {
                                GeneratedSelectedEntryDispatchFault::Success
                            },
                            SELECTOR,
                        ),
                    )?;
                    let metrics = Arc::new(GateMetrics::default());
                    let mut state = generated_test_state(
                        rt.clone(),
                        database.begin_system().await?,
                        manifest,
                        JsonValue::Null,
                        udf_type,
                        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                        CancellationSignal::new_for_test(),
                        None,
                        Arc::clone(&metrics),
                        None,
                    )
                    .await?;
                    state.provider = DatabaseUdfWasmInvocation::<ProdRuntime>::new_for_test(
                        rt.clone(),
                        database.begin_system().await?,
                        QueryJournal::new(),
                        true,
                    )?;
                    state.provider.set_udf_type(udf_type);
                    assert!(state.provider.handler_read_capture_enabled()?);
                    arm_generated_timeout(
                        rt.clone(),
                        &mut state,
                        Duration::from_millis(100),
                        *DATABASE_UDF_SYSTEM_TIMEOUT,
                    )?;
                    let mut output = execute_generated_with_entry_selector(
                        routed,
                        Some(SELECTOR),
                        state,
                        None,
                        false,
                    )
                    .await?;
                    assert_eq!(
                        output.invocation.outcome,
                        if timed_out {
                            InvocationOutcome::InitializationTimeout
                        } else {
                            InvocationOutcome::Success
                        }
                    );
                    assert!(output.system_error.is_none());
                    let outcome = output
                        .invocation
                        .function_outcome
                        .as_ref()
                        .context("preparation outcome was not preserved")?;
                    let (FunctionOutcome::Query(outcome) | FunctionOutcome::Mutation(outcome)) =
                        outcome
                    else {
                        anyhow::bail!("unexpected function outcome");
                    };
                    if timed_out {
                        assert!(outcome
                            .result
                            .as_ref()
                            .unwrap_err()
                            .message
                            .starts_with("Function initialization timed out"));
                    } else {
                        assert!(outcome.result.is_ok());
                    }
                    assert_eq!(
                        output
                            .invocation
                            .transaction
                            .take_handler_read_set()
                            .is_some(),
                        !timed_out
                    );
                    let phases = metrics.generated_execution_phases.lock();
                    let [phase] = phases.as_slice() else {
                        anyhow::bail!("expected one initialization trace");
                    };
                    assert!(phase.prepare_started.is_some());
                    assert_eq!(phase.handler_started.is_some(), !timed_out);
                    assert!(output.reusable_instance.is_none());
                    assert!(output.memory_permit.is_none());
                    let result = output
                        .invocation
                        .into_routed_result(output.system_error, shadow);
                    if timed_out && shadow {
                        let error = result
                            .err()
                            .context("timed-out shadow was accepted for comparison")?;
                        assert_eq!(
                            error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
                            Some(&StaticHermesWasmExecutionFailure::InitializationTimeout)
                        );
                    } else {
                        let (transaction, outcome) = result?;
                        let (FunctionOutcome::Query(outcome) | FunctionOutcome::Mutation(outcome)) =
                            outcome
                        else {
                            anyhow::bail!("unexpected routed function outcome");
                        };
                        assert_eq!(outcome.result.is_err(), timed_out);
                        drop(transaction);
                    }
                }
            }
            database.shutdown().await?;
            Ok(())
        })
}

#[test]
fn routed_developer_error_preserves_timeout_like_message() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    rt.clone().block_on("routed_developer_error", async move {
        let database = new_test_database(rt.clone()).await?;
        let manifest =
            generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
        let routed = generated_test_routed_module_from_bytes(
            Arc::clone(&manifest),
            generated_fixed_entry_prepare_test_module(),
        )?;
        let mut state = generated_test_state(
            rt.clone(),
            database.begin_system().await?,
            manifest,
            JsonValue::Null,
            UdfType::Query,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            CancellationSignal::new_for_test(),
            None,
            Arc::new(GateMetrics::default()),
            None,
        )
        .await?;
        arm_generated_timeout(
            rt,
            &mut state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let mut output = execute_generated(routed, state, None, false).await?;
        assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
        // Supply a developer outcome at the routing boundary with exactly the
        // native timeout's public text. Text must not grant a resource-failure cause.
        let message = "Function initialization timed out (maximum duration: 5s)";
        output.invocation.outcome = InvocationOutcome::DeveloperError(message.to_owned());
        let Some(FunctionOutcome::Query(outcome)) = output.invocation.function_outcome.as_mut()
        else {
            anyhow::bail!("expected canonical query outcome");
        };
        outcome.result = Err(common::errors::JsError::from_message(message.to_owned()));
        let (transaction, outcome) = output
            .invocation
            .into_routed_result(output.system_error, true)?;
        let FunctionOutcome::Query(outcome) = outcome else {
            anyhow::bail!("expected routed query outcome");
        };
        assert_eq!(outcome.result.unwrap_err().message, message);
        drop(transaction);
        database.shutdown().await?;
        Ok(())
    })
}

#[test]
fn generated_wasm_selected_entry_dispatch_failures_are_phase_attributed() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_selected_entry_dispatch_failure",
        run_generated_selected_entry_dispatch_failure_test(rt),
    )
}

#[test]
fn generated_wasm_saturated_cpu_admission_preserves_warm_instance() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_saturated_cpu_admission_preserves_warm_instance",
        run_generated_saturated_cpu_admission_preserves_warm_instance(rt),
    )
}

#[test]
fn generated_wasm_routed_cpu_admission_validates_warm_state_before_rejection() -> anyhow::Result<()>
{
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    block_rt.block_on(
        "generated_wasm_routed_cpu_admission",
        run_generated_routed_cpu_admission_regression(rt),
    )
}

#[test]
fn generated_wasm_routed_request_limit_is_classified_before_guest_execution() -> anyhow::Result<()>
{
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    block_rt.block_on(
        "generated_wasm_routed_request_limit",
        run_generated_routed_request_limit_classification(rt),
    )
}

#[test]
fn generated_wasm_shared_epoch_isolates_timeout_and_cancellation() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_shared_epoch_isolation",
        run_generated_shared_epoch_isolation(rt),
    )
}

#[test]
fn generated_wasm_runtime_destruction_is_bounded() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_bounded_runtime_destruction",
        run_generated_bounded_runtime_destruction(rt),
    )
}

#[test]
fn generated_wasm_system_error_retains_cause_through_cleanup() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_system_error_preservation",
        run_generated_system_error_preservation(rt),
    )
}

#[test]
fn generated_wasm_memory_pool_acceptance() -> anyhow::Result<()> {
    run_generated_test_with_large_stack("generated-wasm-memory-pool", || {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on(
            "generated_wasm_memory_pool_acceptance",
            run_generated_memory_pool_acceptance(rt),
        )
    })
}

#[test]
fn generated_wasm_idle_cleanup_observes_primary_admission_control() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_idle_cleanup_admission_control",
        run_generated_idle_cleanup_observes_admission_control(rt),
    )
}

#[test]
fn generated_wasm_route_signals_function_started_after_memory_admission() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_function_started_after_memory_admission",
        run_generated_function_started_after_memory_admission_test(),
    )
}

#[test]
fn generated_wasm_database_get_uses_transactional_async_syscall() -> anyhow::Result<()> {
    run_generated_test_with_large_stack("generated-wasm-database-get", || {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on(
            "generated_wasm_database_get",
            run_generated_database_get_tests(rt),
        )
    })
}

#[test]
fn generated_wasm_host_owned_limit_survives_wasmtime_trap() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_host_owned_limit_classification",
        run_generated_host_owned_limit_classification_test(rt),
    )
}

#[test]
fn generated_wasm_operation_limit_survives_wasmtime_trap() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_operation_limit_classification",
        run_generated_operation_limit_classification_test(rt),
    )
}

#[test]
fn generated_wasm_guest_promise_database_get_uses_ready_poll_event_loop() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_guest_promise_database_get",
        run_generated_guest_promise_database_get_tests(rt),
    )
}

#[test]
fn generated_wasm_guest_promise_host_operation_errors_preserve_terminal_identity(
) -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_guest_promise_host_operation_error_identity",
        run_generated_guest_promise_host_operation_error_identity_tests(rt),
    )
}

#[test]
fn generated_wasm_capability_database_get_uses_host_queue() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_capability_database_get",
        run_generated_capability_database_get_tests(rt),
    )
}

#[test]
fn generated_wasm_capability_database_query_uses_guest_promise_cursor_lifecycle(
) -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_capability_database_query",
        run_generated_capability_database_query_terminal_tests(rt),
    )
}
