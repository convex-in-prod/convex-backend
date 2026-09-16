use wasm_encoder::MemArg as EncodedMemArg;

use super::*;

const WASM_PAGE_BYTES: usize = 64 * 1024;
const FIRST_RANDOM_POINTER: i32 = 64;
const UUID_POINTER: i32 = 96;
const LAST_RANDOM_POINTER: i32 = 160;
const RESULT_FIELD_POINTER: i32 = 256;

struct ExpectedRandomSequence {
    first: [u8; 7],
    uuid: [u8; RANDOM_UUID_BYTES],
    math: f64,
    last: [u8; 5],
}

struct RandomnessLease {
    existing_slot: Option<GeneratedSlotId>,
    values: Option<OpaqueValueTable>,
    rng_seed: [u8; 32],
}

fn expected_random_sequence(seed: [u8; 32]) -> ExpectedRandomSequence {
    let mut rng = ChaCha12Rng::from_seed(seed);
    let mut first = [0; 7];
    rng.fill(first.as_mut_slice());
    let uuid = uuid::Builder::from_random_bytes(rng.random())
        .into_uuid()
        .to_string()
        .into_bytes()
        .try_into()
        .expect("random UUID has 36 bytes");
    let math = rng.random::<f64>();
    let mut last = [0; 5];
    rng.fill(last.as_mut_slice());
    ExpectedRandomSequence {
        first,
        uuid,
        math,
        last,
    }
}

fn emit_byte_expectation(run: &mut EncodedFunction, pointer: i32, expected: &[u8]) {
    for (offset, byte) in expected.iter().copied().enumerate() {
        run.instruction(&EncodedInstruction::I32Const(pointer));
        run.instruction(&EncodedInstruction::I32Load8U(EncodedMemArg {
            offset: u64::try_from(offset).expect("randomness test offset overflow"),
            align: 0,
            memory_index: 0,
        }));
        run.instruction(&EncodedInstruction::I32Const(i32::from(byte)));
        run.instruction(&EncodedInstruction::I32Ne);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::I32Const(1));
        run.instruction(&EncodedInstruction::Return);
        run.instruction(&EncodedInstruction::End);
    }
}

fn randomness_test_module(
    expected: Option<&[ExpectedRandomSequence; 2]>,
    retain_capability_for_reuse: bool,
) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    types.ty().function(
        [
            EncodedValType::I64,
            EncodedValType::I32,
            EncodedValType::I32,
        ],
        std::iter::empty(),
    );
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::F64]);
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    types
        .ty()
        .function([EncodedValType::I64], std::iter::empty());
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::I32]);

    let mut imports = EncodedImportSection::new();
    for (name, ty) in [
        ("convex_capability_current", 0),
        ("convex_crypto_get_random_values", 1),
        ("convex_crypto_random_uuid", 1),
        ("convex_math_random", 2),
        ("convex_request_field", 3),
        ("convex_function_result", 4),
    ] {
        imports.import("convex", name, EncodedEntityType::Function(ty));
    }

    let imported_function_count = 6;
    let mut functions = EncodedFunctionSection::new();
    functions.function(5);
    functions.function(6);
    functions.function(5);
    functions.function(7);
    functions.function(6);
    let mut memory = EncodedMemorySection::new();
    memory.memory(EncodedMemoryType {
        minimum: 1,
        maximum: Some(1),
        memory64: false,
        shared: false,
        page_size_log2: None,
    });
    let mut globals = EncodedGlobalSection::new();
    globals.global(
        EncodedGlobalType {
            val_type: EncodedValType::I32,
            mutable: true,
            shared: false,
        },
        &EncodedConstExpr::i32_const(0),
    );
    globals.global(
        EncodedGlobalType {
            val_type: EncodedValType::I64,
            mutable: true,
            shared: false,
        },
        &EncodedConstExpr::i64_const(0),
    );
    let mut exports = EncodedExportSection::new();
    exports.export("memory", EncodedExportKind::Memory, 0);
    exports.export(
        "_initialize",
        EncodedExportKind::Func,
        imported_function_count,
    );
    exports.export(
        "convex_wasm_udf_run",
        EncodedExportKind::Func,
        imported_function_count + 1,
    );
    exports.export(
        "convex_wasm_udf_destroy_runtime",
        EncodedExportKind::Func,
        imported_function_count + 2,
    );
    exports.export(
        "convex_wasm_select_entry",
        EncodedExportKind::Func,
        imported_function_count + 3,
    );
    exports.export(
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        imported_function_count + 4,
    );

    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([(1, EncodedValType::F64)]);
    if retain_capability_for_reuse {
        run.instruction(&EncodedInstruction::GlobalGet(1));
        run.instruction(&EncodedInstruction::I64Eqz);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::Call(0));
        run.instruction(&EncodedInstruction::GlobalSet(1));
        run.instruction(&EncodedInstruction::End);
        run.instruction(&EncodedInstruction::GlobalGet(1));
        run.instruction(&EncodedInstruction::I32Const(FIRST_RANDOM_POINTER));
        run.instruction(&EncodedInstruction::I32Const(7));
        run.instruction(&EncodedInstruction::Call(1));
    } else {
        run.instruction(&EncodedInstruction::Call(0));
        run.instruction(&EncodedInstruction::I32Const(FIRST_RANDOM_POINTER));
        run.instruction(&EncodedInstruction::I32Const(7));
        run.instruction(&EncodedInstruction::Call(1));
        run.instruction(&EncodedInstruction::Call(0));
        run.instruction(&EncodedInstruction::I32Const(UUID_POINTER));
        run.instruction(&EncodedInstruction::I32Const(RANDOM_UUID_BYTES as i32));
        run.instruction(&EncodedInstruction::Call(2));
        run.instruction(&EncodedInstruction::Call(0));
        run.instruction(&EncodedInstruction::Call(3));
        run.instruction(&EncodedInstruction::LocalSet(0));
        run.instruction(&EncodedInstruction::Call(0));
        run.instruction(&EncodedInstruction::I32Const(LAST_RANDOM_POINTER));
        run.instruction(&EncodedInstruction::I32Const(5));
        run.instruction(&EncodedInstruction::Call(1));

        run.instruction(&EncodedInstruction::GlobalGet(0));
        run.instruction(&EncodedInstruction::I32Const(2));
        run.instruction(&EncodedInstruction::I32GeU);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::I32Const(1));
        run.instruction(&EncodedInstruction::Return);
        run.instruction(&EncodedInstruction::End);
        for (invocation_index, sequence) in expected
            .expect("mixed randomness module requires expectations")
            .iter()
            .enumerate()
        {
            run.instruction(&EncodedInstruction::GlobalGet(0));
            run.instruction(&EncodedInstruction::I32Const(
                i32::try_from(invocation_index).expect("invocation index overflow"),
            ));
            run.instruction(&EncodedInstruction::I32Eq);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            emit_byte_expectation(&mut run, FIRST_RANDOM_POINTER, &sequence.first);
            emit_byte_expectation(&mut run, UUID_POINTER, &sequence.uuid);
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::F64Const(sequence.math.into()));
            run.instruction(&EncodedInstruction::F64Ne);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(1));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            emit_byte_expectation(&mut run, LAST_RANDOM_POINTER, &sequence.last);
            run.instruction(&EncodedInstruction::End);
        }
        run.instruction(&EncodedInstruction::GlobalGet(0));
        run.instruction(&EncodedInstruction::I32Const(1));
        run.instruction(&EncodedInstruction::I32Add);
        run.instruction(&EncodedInstruction::GlobalSet(0));
    }
    run.instruction(&EncodedInstruction::I32Const(RESULT_FIELD_POINTER));
    run.instruction(&EncodedInstruction::I32Const(6));
    run.instruction(&EncodedInstruction::Call(4));
    run.instruction(&EncodedInstruction::Call(5));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::End);
    let mut destroy = EncodedFunction::new([]);
    destroy.instruction(&EncodedInstruction::End);
    let mut selector = EncodedFunction::new([]);
    selector.instruction(&EncodedInstruction::I32Const(0));
    selector.instruction(&EncodedInstruction::End);
    let mut prepare = EncodedFunction::new([]);
    prepare.instruction(&EncodedInstruction::I32Const(0));
    prepare.instruction(&EncodedInstruction::End);

    let mut code = EncodedCodeSection::new();
    code.function(&initialize);
    code.function(&run);
    code.function(&destroy);
    code.function(&selector);
    code.function(&prepare);
    let mut data = EncodedDataSection::new();
    data.active(
        0,
        &EncodedConstExpr::i32_const(RESULT_FIELD_POINTER),
        "result".bytes(),
    );
    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&memory)
        .section(&globals)
        .section(&exports)
        .section(&code)
        .section(&data);
    module.finish()
}

fn randomness_manifest(
    maximum_operation_count: u64,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        WASM_PAGE_BYTES,
        1_000_000,
        maximum_operation_count,
    )
}

fn randomness_controller() -> anyhow::Result<Arc<GeneratedMemoryController>> {
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
    Ok(controller)
}

async fn randomness_state(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    controller: Arc<GeneratedMemoryController>,
    route_identity: GeneratedRouteIdentity,
    lease: RandomnessLease,
) -> anyhow::Result<HostState<ProdRuntime>> {
    generated_test_state_with_rng_seed(
        rt,
        transaction,
        manifest,
        json!({ "result": "ok" }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        Some(GeneratedTestMemorySetup {
            controller,
            route_identity,
            existing_slot: lease.existing_slot,
            values: lease.values,
            maximum_guest_memory_bytes: WASM_PAGE_BYTES,
        }),
        lease.rng_seed,
    )
    .await
}

fn assert_successful_randomness_invocation(output: &GeneratedExecutionOutput<ProdRuntime>) {
    assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(output.invocation.function_result, Some(json!("ok")));
    assert_eq!(output.invocation.operation_count, 4);
    assert!(output.invocation.observed_rng);
    assert!(output.invocation.capability_revoked);
    assert!(!output.invocation.runtime_reuse_contaminated);
    assert_eq!(output.invocation.opaque_live_handles, 0);
    assert_eq!(output.invocation.opaque_current_bytes, 0);
}

fn assert_controller_released(
    controller: &GeneratedMemoryController,
    memory_identity: &FunctionMemoryIdentity,
) {
    let snapshot = controller.snapshot_for_test(memory_identity);
    assert_eq!(snapshot.active_instances, 0);
    assert_eq!(snapshot.idle_instances, 0);
    assert_eq!(snapshot.evicting_instances, 0);
    assert_eq!(snapshot.retained_baseline_bytes, 0);
    assert_eq!(snapshot.unattributed_allowance_bytes, 0);
}

async fn run_mixed_randomness_reuse_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let seeds = [[0x11; 32], [0xa7; 32]];
    let expected = [
        expected_random_sequence(seeds[0]),
        expected_random_sequence(seeds[1]),
    ];
    let database = new_test_database(rt.clone()).await?;
    let manifest = randomness_manifest(4)?;
    let mut routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        randomness_test_module(Some(&expected), false),
    )?;
    Arc::get_mut(&mut routed)
        .context("randomness test routed module was unexpectedly shared")?
        .entry_selector = Some(0);
    let route_identity = routed.route_identity.clone();
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let controller = randomness_controller()?;

    let mut first_state = randomness_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        Arc::clone(&controller),
        route_identity.clone(),
        RandomnessLease {
            existing_slot: None,
            values: None,
            rng_seed: seeds[0],
        },
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut first_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut first = execute_generated(Arc::clone(&routed), first_state, None, true).await?;
    assert_successful_randomness_invocation(&first);
    let retained = retain_generated_test_runtime(&mut first)?;
    let first_instance_id = retained.instance.id;
    let first_slot_id = retained.memory_slot_id;
    drop(first.invocation.transaction);

    let mut second_state = randomness_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        Arc::clone(&controller),
        route_identity,
        RandomnessLease {
            existing_slot: Some(first_slot_id),
            values: Some(retained.values),
            rng_seed: seeds[1],
        },
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut second_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut second = execute_generated(routed, second_state, Some(retained.instance), true).await?;
    assert_successful_randomness_invocation(&second);
    let second_instance = second
        .reusable_instance
        .take()
        .context("second randomness invocation did not retain its runtime")?;
    let second_permit = second
        .memory_permit
        .take()
        .context("second randomness invocation did not retain its memory permit")?;
    assert_eq!(second_instance.id, first_instance_id);
    assert_eq!(second_permit.slot_id(), first_slot_id);
    discard_generated_instance(second_instance).await?;
    second_permit.finish(second.terminal_memory_outcome, false);
    drop(second.invocation.transaction);
    assert_controller_released(&controller, &memory_identity);

    database.shutdown().await?;
    Ok(())
}

async fn run_stale_randomness_capability_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let manifest = randomness_manifest(1)?;
    let mut routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        randomness_test_module(None, true),
    )?;
    Arc::get_mut(&mut routed)
        .context("stale randomness test routed module was unexpectedly shared")?
        .entry_selector = Some(0);
    let route_identity = routed.route_identity.clone();
    let memory_identity = generated_memory_identity(&manifest, &route_identity);
    let controller = randomness_controller()?;

    let mut first_state = randomness_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        Arc::clone(&controller),
        route_identity.clone(),
        RandomnessLease {
            existing_slot: None,
            values: None,
            rng_seed: [0x29; 32],
        },
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut first_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut first = execute_generated(Arc::clone(&routed), first_state, None, true).await?;
    assert_eq!(first.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(first.invocation.operation_count, 1);
    assert!(first.invocation.observed_rng);
    assert!(first.invocation.capability_revoked);
    assert!(!first.invocation.runtime_reuse_contaminated);
    assert_eq!(first.invocation.opaque_live_handles, 0);
    assert_eq!(first.invocation.opaque_current_bytes, 0);
    let retained = retain_generated_test_runtime(&mut first)?;
    drop(first.invocation.transaction);

    let mut second_state = randomness_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        Arc::clone(&controller),
        route_identity,
        RandomnessLease {
            existing_slot: Some(retained.memory_slot_id),
            values: Some(retained.values),
            rng_seed: [0xd3; 32],
        },
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut second_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let second = execute_generated(routed, second_state, Some(retained.instance), true).await?;
    assert_eq!(second.invocation.outcome, InvocationOutcome::SystemError);
    assert_eq!(second.invocation.operation_count, 0);
    assert!(!second.invocation.observed_rng);
    assert!(second.invocation.capability_revoked);
    assert!(second.invocation.runtime_reuse_contaminated);
    assert_eq!(second.invocation.opaque_live_handles, 0);
    assert_eq!(second.invocation.opaque_current_bytes, 0);
    assert!(second.reusable_instance.is_none());
    assert!(second.memory_permit.is_none());
    drop(second.invocation.transaction);
    assert_controller_released(&controller, &memory_identity);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn randomness_uses_one_seeded_stream_and_resets_it_for_reuse() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_mixed_randomness_reuse",
        run_mixed_randomness_reuse_test(rt),
    )
}

#[test]
fn randomness_rejects_stale_capability_before_observing_rng() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_stale_randomness_capability",
        run_stale_randomness_capability_test(rt),
    )
}
