use super::*;

fn invocation_timestamp_test_module() -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::F64]);
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
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);

    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        INVOCATION_UNIX_TIMESTAMP_MS_IMPORT,
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_request_field",
        EncodedEntityType::Function(1),
    );
    imports.import("convex", "convex_db_get", EncodedEntityType::Function(2));
    imports.import(
        "convex",
        "convex_value_release",
        EncodedEntityType::Function(3),
    );
    imports.import(
        "convex",
        "convex_value_number_new",
        EncodedEntityType::Function(4),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(3),
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
        (1, EncodedValType::F64),
        (1, EncodedValType::I64),
        (1, EncodedValType::F64),
    ]);
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::LocalSet(0));
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::I32Const(2));
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::Call(2));
    run.instruction(&EncodedInstruction::LocalSet(1));
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::I64Const(-1));
    run.instruction(&EncodedInstruction::I64Eq);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(2));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::Call(3));
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::LocalSet(2));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::LocalGet(2));
    run.instruction(&EncodedInstruction::F64Ne);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(2));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(2));
    run.instruction(&EncodedInstruction::Call(4));
    run.instruction(&EncodedInstruction::Call(5));
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

async fn run_invocation_timestamp_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_timestamp_documents".parse()?;
    let id = insert_document(&database, &table).await?.developer_id;
    let manifest = generated_test_manifest(
        ManifestUdfKind::Query,
        json!({
            "kind": "databaseGet",
            "tableName": table,
        }),
    )?;
    let routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        invocation_timestamp_test_module(),
    )?;
    let invocation_timestamp = UnixTimestamp::from_nanos(1_700_000_000_123_456_789);
    let expected_timestamp_ms = invocation_timestamp.as_ms_since_epoch()?;
    let (control, mut host) = read_control();
    let mut state = generated_test_state(
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
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let execution = tokio::spawn(execute_generated(Arc::clone(&routed), state, None, false));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("timestamp test database read did not suspend")??;
    host.release
        .take()
        .context("timestamp test database read release missing")?
        .send(())
        .map_err(|_| anyhow::anyhow!("timestamp test database read already completed"))?;
    let output = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("timestamp test did not resume")???;
    assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        output
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_f64),
        Some(expected_timestamp_ms as f64)
    );
    assert!(output.invocation.observed_time);
    drop(output.invocation.transaction);

    let negative_control = execute_generated_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        GeneratedTestOperation::DatabaseGet,
        json!({ "id": id.encode() }),
        UdfType::Query,
        invocation_timestamp,
    )
    .await?;
    assert_eq!(
        negative_control.invocation.outcome,
        InvocationOutcome::Success
    );
    assert!(!negative_control.invocation.observed_time);
    drop(negative_control.invocation.transaction);

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
    let missing =
        execute_generated(Arc::clone(&routed), missing_invocation_state, None, false).await?;
    assert_eq!(missing.invocation.outcome, InvocationOutcome::SystemError);
    assert!(missing.system_error.as_ref().is_some_and(
        |error| format!("{error:#}").contains("Static Hermes host ABI invariant failed")
    ));
    assert!(!missing.invocation.observed_time);
    drop(missing.invocation.transaction);

    let mut oversized_timestamp = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        json!({ "id": id.encode() }),
        UdfType::Query,
        UnixTimestamp::from_millis(MAX_EXACT_JAVASCRIPT_INTEGER + 1),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut oversized_timestamp,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let oversized = execute_generated(routed, oversized_timestamp, None, false).await?;
    assert_eq!(oversized.invocation.outcome, InvocationOutcome::SystemError);
    assert!(!oversized.invocation.observed_time);
    drop(oversized.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn invocation_timestamp_is_attempt_scoped_and_fails_closed() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_invocation_timestamp",
        run_invocation_timestamp_tests(rt),
    )
}
