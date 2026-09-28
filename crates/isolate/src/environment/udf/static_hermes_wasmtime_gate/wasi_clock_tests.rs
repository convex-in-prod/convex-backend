use wasm_encoder::MemArg as EncodedMemArg;

use super::*;

const CLOCK_RESULT_POINTER: i32 = 32;
const SECOND_CLOCK_RESULT_POINTER: i32 = 40;
const PERSISTED_MONOTONIC_POINTER: i32 = 48;
const PERSISTED_MONOTONIC_INITIALIZED_POINTER: i32 = 56;
const UNSUPPORTED_CLOCK_SENTINEL: i64 = 0x5a5a;

enum WasiClockTestOperation {
    RealtimeAcrossSuspension { expected_timestamp_nanos: u64 },
    MonotonicAcrossSuspension { realtime_timestamp_nanos: u64 },
    MonotonicAcrossReusedInvocation,
    UnsupportedClockIds,
}

fn wasi_clock_test_module(operation: WasiClockTestOperation) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I64],
        [EncodedValType::I64],
    );
    types
        .ty()
        .function([EncodedValType::I64], std::iter::empty());
    types
        .ty()
        .function([EncodedValType::F64], [EncodedValType::I64]);
    types.ty().function(
        [
            EncodedValType::I32,
            EncodedValType::I64,
            EncodedValType::I32,
        ],
        [EncodedValType::I32],
    );
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);

    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_request_field",
        EncodedEntityType::Function(0),
    );
    imports.import("convex", "convex_db_get", EncodedEntityType::Function(1));
    imports.import(
        "convex",
        "convex_value_release",
        EncodedEntityType::Function(2),
    );
    imports.import(
        "convex",
        "convex_value_number_new",
        EncodedEntityType::Function(3),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(2),
    );
    imports.import(
        "wasi_snapshot_preview1",
        "clock_time_get",
        EncodedEntityType::Function(4),
    );

    let imported_function_count = 6;
    let mut functions = EncodedFunctionSection::new();
    functions.function(5);
    functions.function(6);
    functions.function(5);
    functions.function(6);

    let mut memory = EncodedMemorySection::new();
    memory.memory(EncodedMemoryType {
        minimum: 1,
        maximum: Some(1),
        memory64: false,
        shared: false,
        page_size_log2: None,
    });
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
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        imported_function_count + 3,
    );

    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([
        (1, EncodedValType::I64),
        (1, EncodedValType::I32),
        (2, EncodedValType::I64),
    ]);
    let clock_result = EncodedMemArg {
        offset: 0,
        align: 3,
        memory_index: 0,
    };
    let persisted_monotonic_initialized = EncodedMemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    };
    match operation {
        WasiClockTestOperation::RealtimeAcrossSuspension {
            expected_timestamp_nanos,
        } => {
            let expected_timestamp_nanos = i64::try_from(expected_timestamp_nanos)
                .expect("test invocation timestamp exceeds signed Wasm i64");
            run.instruction(&EncodedInstruction::I32Const(WASI_CLOCK_ID_REALTIME));
            run.instruction(&EncodedInstruction::I64Const(0));
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::Call(5));
            run.instruction(&EncodedInstruction::LocalSet(1));
            run.instruction(&EncodedInstruction::LocalGet(1));
            run.instruction(&EncodedInstruction::I32Eqz);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::Else);
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::I64Load(clock_result));
            run.instruction(&EncodedInstruction::LocalSet(2));
            run.instruction(&EncodedInstruction::LocalGet(2));
            run.instruction(&EncodedInstruction::I64Const(expected_timestamp_nanos));
            run.instruction(&EncodedInstruction::I64Ne);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);

            run.instruction(&EncodedInstruction::I32Const(1));
            run.instruction(&EncodedInstruction::I32Const(0));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Call(0));
            run.instruction(&EncodedInstruction::Call(1));
            run.instruction(&EncodedInstruction::LocalSet(0));
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::I64Const(-1));
            run.instruction(&EncodedInstruction::I64Eq);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::Call(2));

            run.instruction(&EncodedInstruction::I32Const(WASI_CLOCK_ID_REALTIME));
            run.instruction(&EncodedInstruction::I64Const(0));
            run.instruction(&EncodedInstruction::I32Const(SECOND_CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::Call(5));
            run.instruction(&EncodedInstruction::LocalSet(1));
            run.instruction(&EncodedInstruction::LocalGet(1));
            run.instruction(&EncodedInstruction::I32Eqz);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::Else);
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::I32Const(SECOND_CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::I64Load(clock_result));
            run.instruction(&EncodedInstruction::LocalSet(3));
            run.instruction(&EncodedInstruction::LocalGet(2));
            run.instruction(&EncodedInstruction::LocalGet(3));
            run.instruction(&EncodedInstruction::I64Ne);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
        },
        WasiClockTestOperation::MonotonicAcrossSuspension {
            realtime_timestamp_nanos,
        } => {
            let realtime_timestamp_nanos = i64::try_from(realtime_timestamp_nanos)
                .expect("test invocation timestamp exceeds signed Wasm i64");
            run.instruction(&EncodedInstruction::I32Const(WASI_CLOCK_ID_MONOTONIC));
            run.instruction(&EncodedInstruction::I64Const(0));
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::Call(5));
            run.instruction(&EncodedInstruction::LocalSet(1));
            run.instruction(&EncodedInstruction::LocalGet(1));
            run.instruction(&EncodedInstruction::I32Eqz);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::Else);
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::I64Load(clock_result));
            run.instruction(&EncodedInstruction::LocalSet(3));
            run.instruction(&EncodedInstruction::LocalGet(3));
            run.instruction(&EncodedInstruction::I64Const(realtime_timestamp_nanos));
            run.instruction(&EncodedInstruction::I64Eq);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);

            run.instruction(&EncodedInstruction::I32Const(1));
            run.instruction(&EncodedInstruction::I32Const(0));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Call(0));
            run.instruction(&EncodedInstruction::Call(1));
            run.instruction(&EncodedInstruction::LocalSet(0));
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::I64Const(-1));
            run.instruction(&EncodedInstruction::I64Eq);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::Call(2));

            run.instruction(&EncodedInstruction::I32Const(WASI_CLOCK_ID_MONOTONIC));
            run.instruction(&EncodedInstruction::I64Const(0));
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::Call(5));
            run.instruction(&EncodedInstruction::LocalSet(1));
            run.instruction(&EncodedInstruction::LocalGet(1));
            run.instruction(&EncodedInstruction::I32Eqz);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::Else);
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::I64Load(clock_result));
            run.instruction(&EncodedInstruction::LocalSet(2));
            run.instruction(&EncodedInstruction::LocalGet(2));
            run.instruction(&EncodedInstruction::LocalGet(3));
            run.instruction(&EncodedInstruction::I64LtU);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::LocalGet(2));
            run.instruction(&EncodedInstruction::LocalGet(3));
            run.instruction(&EncodedInstruction::I64Eq);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(3));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
        },
        WasiClockTestOperation::MonotonicAcrossReusedInvocation => {
            run.instruction(&EncodedInstruction::I32Const(
                PERSISTED_MONOTONIC_INITIALIZED_POINTER,
            ));
            run.instruction(&EncodedInstruction::I32Load(
                persisted_monotonic_initialized,
            ));
            run.instruction(&EncodedInstruction::I32Eqz);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(1));
            run.instruction(&EncodedInstruction::I32Const(0));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Call(0));
            run.instruction(&EncodedInstruction::Call(1));
            run.instruction(&EncodedInstruction::LocalSet(0));
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::I64Const(-1));
            run.instruction(&EncodedInstruction::I64Eq);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::Call(2));

            run.instruction(&EncodedInstruction::I32Const(WASI_CLOCK_ID_MONOTONIC));
            run.instruction(&EncodedInstruction::I64Const(0));
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::Call(5));
            run.instruction(&EncodedInstruction::LocalSet(1));
            run.instruction(&EncodedInstruction::LocalGet(1));
            run.instruction(&EncodedInstruction::I32Eqz);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::Else);
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::I32Const(PERSISTED_MONOTONIC_POINTER));
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::I64Load(clock_result));
            run.instruction(&EncodedInstruction::I64Store(clock_result));
            run.instruction(&EncodedInstruction::I32Const(
                PERSISTED_MONOTONIC_INITIALIZED_POINTER,
            ));
            run.instruction(&EncodedInstruction::I32Const(1));
            run.instruction(&EncodedInstruction::I32Store(
                persisted_monotonic_initialized,
            ));
            run.instruction(&EncodedInstruction::Else);
            run.instruction(&EncodedInstruction::I32Const(WASI_CLOCK_ID_MONOTONIC));
            run.instruction(&EncodedInstruction::I64Const(0));
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::Call(5));
            run.instruction(&EncodedInstruction::LocalSet(1));
            run.instruction(&EncodedInstruction::LocalGet(1));
            run.instruction(&EncodedInstruction::I32Eqz);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::Else);
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
            run.instruction(&EncodedInstruction::I64Load(clock_result));
            run.instruction(&EncodedInstruction::LocalSet(2));
            run.instruction(&EncodedInstruction::I32Const(PERSISTED_MONOTONIC_POINTER));
            run.instruction(&EncodedInstruction::I64Load(clock_result));
            run.instruction(&EncodedInstruction::LocalSet(3));
            run.instruction(&EncodedInstruction::LocalGet(2));
            run.instruction(&EncodedInstruction::LocalGet(3));
            run.instruction(&EncodedInstruction::I64LtU);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(2));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::LocalGet(2));
            run.instruction(&EncodedInstruction::LocalGet(3));
            run.instruction(&EncodedInstruction::I64Eq);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::I32Const(3));
            run.instruction(&EncodedInstruction::Return);
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::End);
        },
        WasiClockTestOperation::UnsupportedClockIds => {
            for clock_id in [2, 3, 4] {
                run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
                run.instruction(&EncodedInstruction::I64Const(UNSUPPORTED_CLOCK_SENTINEL));
                run.instruction(&EncodedInstruction::I64Store(clock_result));
                run.instruction(&EncodedInstruction::I32Const(clock_id));
                run.instruction(&EncodedInstruction::I64Const(0));
                run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
                run.instruction(&EncodedInstruction::Call(5));
                run.instruction(&EncodedInstruction::LocalSet(1));
                run.instruction(&EncodedInstruction::LocalGet(1));
                run.instruction(&EncodedInstruction::I32Const(WASI_ERRNO_NOTSUP));
                run.instruction(&EncodedInstruction::I32Ne);
                run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
                run.instruction(&EncodedInstruction::I32Const(2));
                run.instruction(&EncodedInstruction::Return);
                run.instruction(&EncodedInstruction::End);
                run.instruction(&EncodedInstruction::I32Const(CLOCK_RESULT_POINTER));
                run.instruction(&EncodedInstruction::I64Load(clock_result));
                run.instruction(&EncodedInstruction::I64Const(UNSUPPORTED_CLOCK_SENTINEL));
                run.instruction(&EncodedInstruction::I64Ne);
                run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
                run.instruction(&EncodedInstruction::I32Const(2));
                run.instruction(&EncodedInstruction::Return);
                run.instruction(&EncodedInstruction::End);
            }
        },
    }
    run.instruction(&EncodedInstruction::F64Const(1.0.into()));
    run.instruction(&EncodedInstruction::Call(3));
    run.instruction(&EncodedInstruction::Call(4));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::End);
    let mut destroy = EncodedFunction::new([]);
    destroy.instruction(&EncodedInstruction::End);
    let mut prepare = EncodedFunction::new([]);
    prepare.instruction(&EncodedInstruction::I32Const(0));
    prepare.instruction(&EncodedInstruction::End);
    let mut code = EncodedCodeSection::new();
    code.function(&initialize);
    code.function(&run);
    code.function(&destroy);
    code.function(&prepare);
    let mut data = EncodedDataSection::new();
    data.active(0, &EncodedConstExpr::i32_const(0), "id".bytes());
    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&memory)
        .section(&exports)
        .section(&code)
        .section(&data);
    module.finish()
}

async fn run_wasi_clock_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_wasi_clock_documents".parse()?;
    let id = insert_document(&database, &table).await?.developer_id;
    let manifest = generated_test_manifest(
        ManifestUdfKind::Query,
        json!({
            "kind": "databaseGet",
            "tableName": table,
        }),
    )?;
    let invocation_timestamp = UnixTimestamp::from_nanos(1_700_000_000_123_456_789);
    let expected_timestamp_nanos = invocation_timestamp
        .as_ms_since_epoch()?
        .checked_mul(NANOS_PER_MILLISECOND)
        .context("test timestamp nanosecond conversion overflowed")?;
    let realtime_routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        wasi_clock_test_module(WasiClockTestOperation::RealtimeAcrossSuspension {
            expected_timestamp_nanos,
        }),
    )?;

    let (control, mut host) = read_control();
    let mut realtime_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "id": id.encode() }),
        UdfType::Query,
        invocation_timestamp,
        CancellationSignal::new_for_test(),
        Some(control),
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut realtime_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let execution = tokio::spawn(execute_generated(
        Arc::clone(&realtime_routed),
        realtime_state,
        None,
        false,
    ));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("WASI realtime test database read did not suspend")??;
    host.release
        .take()
        .context("WASI realtime test database read release missing")?
        .send(())
        .map_err(|_| anyhow::anyhow!("WASI realtime test database read already completed"))?;
    let realtime = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("WASI realtime test did not resume")???;
    assert_eq!(realtime.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(realtime.invocation.function_result, Some(json!(1.0)));
    assert!(realtime.invocation.observed_time);
    drop(realtime.invocation.transaction);

    let monotonic_routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        wasi_clock_test_module(WasiClockTestOperation::MonotonicAcrossSuspension {
            realtime_timestamp_nanos: expected_timestamp_nanos,
        }),
    )?;
    let (control, mut host) = read_control();
    let mut monotonic_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "id": id.encode() }),
        UdfType::Query,
        invocation_timestamp,
        CancellationSignal::new_for_test(),
        Some(control),
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut monotonic_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let execution = tokio::spawn(execute_generated(
        Arc::clone(&monotonic_routed),
        monotonic_state,
        None,
        false,
    ));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("WASI monotonic test database read did not suspend")??;
    tokio::time::sleep(Duration::from_millis(10)).await;
    host.release
        .take()
        .context("WASI monotonic test database read release missing")?
        .send(())
        .map_err(|_| anyhow::anyhow!("WASI monotonic test database read already completed"))?;
    let monotonic = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("WASI monotonic test did not resume")???;
    assert_eq!(monotonic.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(monotonic.invocation.function_result, Some(json!(1.0)));
    assert!(!monotonic.invocation.observed_time);
    drop(monotonic.invocation.transaction);

    let monotonic_reused_routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        wasi_clock_test_module(WasiClockTestOperation::MonotonicAcrossReusedInvocation),
    )?;
    let controller = Arc::clone(&route_configuration()?.generated_memory_controller);
    let route_identity = GeneratedRouteIdentity::Singleton {
        package_key: "generated-wasi-monotonic-reuse-test".to_owned(),
    };
    let (control, mut host) = read_control();
    let mut first_monotonic_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "id": id.encode() }),
        UdfType::Query,
        invocation_timestamp,
        CancellationSignal::new_for_test(),
        Some(control),
        Arc::new(GateMetrics::default()),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&controller),
            route_identity: route_identity.clone(),
            existing_slot: None,
            values: None,
            maximum_guest_memory_bytes: 1 << 20,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut first_monotonic_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let execution = tokio::spawn(execute_generated(
        Arc::clone(&monotonic_reused_routed),
        first_monotonic_state,
        None,
        true,
    ));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("WASI reused monotonic test database read did not suspend")??;
    tokio::time::sleep(Duration::from_millis(10)).await;
    host.release
        .take()
        .context("WASI reused monotonic test database read release missing")?
        .send(())
        .map_err(|_| {
            anyhow::anyhow!("WASI reused monotonic test database read already completed")
        })?;
    let mut first_monotonic = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("WASI reused monotonic first invocation did not resume")???;
    assert_eq!(
        first_monotonic.invocation.outcome,
        InvocationOutcome::Success
    );
    assert_eq!(first_monotonic.invocation.function_result, Some(json!(1.0)));
    assert!(!first_monotonic.invocation.observed_time);
    let reusable_instance = first_monotonic
        .reusable_instance
        .take()
        .context("WASI monotonic first invocation was not retained")?;
    let memory_permit = first_monotonic
        .memory_permit
        .take()
        .context("WASI monotonic first invocation lost its memory permit")?;
    let memory_slot_id = memory_permit.slot_id();
    drop(first_monotonic.invocation.transaction);
    memory_permit.finish(TerminalMemoryOutcome::Success, true);

    let mut second_monotonic_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "id": id.encode() }),
        UdfType::Query,
        invocation_timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        Some(GeneratedTestMemorySetup {
            controller,
            route_identity,
            existing_slot: Some(memory_slot_id),
            values: None,
            maximum_guest_memory_bytes: 1 << 20,
        }),
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut second_monotonic_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut second_monotonic = execute_generated(
        monotonic_reused_routed,
        second_monotonic_state,
        Some(reusable_instance),
        true,
    )
    .await?;
    assert_eq!(
        second_monotonic.invocation.outcome,
        InvocationOutcome::Success
    );
    assert_eq!(
        second_monotonic.invocation.function_result,
        Some(json!(1.0))
    );
    assert!(!second_monotonic.invocation.observed_time);
    drop(second_monotonic.invocation.transaction);
    discard_generated_instance(
        second_monotonic
            .reusable_instance
            .take()
            .context("WASI monotonic second invocation was not retained")?,
    )
    .await?;
    second_monotonic
        .memory_permit
        .take()
        .context("WASI monotonic second invocation lost its memory permit")?
        .finish(TerminalMemoryOutcome::Success, false);

    let unsupported_routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        wasi_clock_test_module(WasiClockTestOperation::UnsupportedClockIds),
    )?;
    let mut unsupported_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "id": id.encode() }),
        UdfType::Query,
        invocation_timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut unsupported_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let unsupported = execute_generated(unsupported_routed, unsupported_state, None, false).await?;
    assert_eq!(unsupported.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(unsupported.invocation.function_result, Some(json!(1.0)));
    assert!(!unsupported.invocation.observed_time);
    drop(unsupported.invocation.transaction);

    let mut missing_invocation_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        json!({ "id": id.encode() }),
        UdfType::Query,
        invocation_timestamp,
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    missing_invocation_state.provider.clear_invocation();
    arm_generated_timeout(
        rt.clone(),
        &mut missing_invocation_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let missing = execute_generated(
        Arc::clone(&realtime_routed),
        missing_invocation_state,
        None,
        false,
    )
    .await?;
    assert_eq!(missing.invocation.outcome, InvocationOutcome::SystemError);
    assert!(missing.system_error.as_ref().is_some_and(
        |error| format!("{error:#}").contains("Static Hermes host ABI invariant failed")
    ));
    assert!(!missing.invocation.observed_time);
    drop(missing.invocation.transaction);

    let mut overflow_state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        json!({ "id": id.encode() }),
        UdfType::Query,
        UnixTimestamp::from_millis(u64::MAX / NANOS_PER_MILLISECOND + 1),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt.clone(),
        &mut overflow_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let overflow = execute_generated(realtime_routed, overflow_state, None, false).await?;
    assert_eq!(overflow.invocation.outcome, InvocationOutcome::SystemError);
    assert!(overflow.system_error.as_ref().is_some_and(
        |error| format!("{error:#}").contains("Static Hermes host ABI invariant failed")
    ));
    assert!(!overflow.invocation.observed_time);
    drop(overflow.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn wasi_clocks_keep_realtime_fixed_and_monotonic_available() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("generated_wasm_wasi_clock", run_wasi_clock_tests(rt))
}
