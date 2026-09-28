use super::{
    test_support::{
        generated_guest_query_terminal_pair_module,
        query_terminal_pair_manifest,
    },
    *,
};

fn dynamic_take_operation(table: &TableName) -> JsonValue {
    json!({
        "id": 1,
        "debugName": "dynamicTakeByTenant",
        "operation": {
            "kind": "databaseIndexQuery",
            "tableName": table,
            "indexName": "by_tenant",
            "constraints": [
                { "fieldPath": "tenant", "operator": "eq" },
            ],
            "order": "ascending",
            "terminal": "collect",
            "limit": null,
            "limitArgumentIndex": 1,
        },
    })
}

fn query_stream_operation(table: &TableName) -> JsonValue {
    json!({
        "id": 1,
        "debugName": "streamByTenant",
        "operation": {
            "kind": "databaseIndexQuery",
            "tableName": table,
            "indexName": "by_tenant",
            "constraints": [
                { "fieldPath": "tenant", "operator": "eq" },
            ],
            "order": "ascending",
            "terminal": "stream",
            "limit": null,
        },
    })
}

fn dynamic_take_manifest(
    table: &TableName,
    mode: EffectExecutionMode,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_test_manifest_with_runtime_limits_and_schema(
        ManifestUdfKind::Query,
        vec![dynamic_take_operation(table)],
        1 << 20,
        1_000_000_000,
        128,
        EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION,
        Some(mode),
    )
}

fn query_stream_manifest(table: &TableName) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_test_manifest_with_runtime_limits_and_schema(
        ManifestUdfKind::Query,
        vec![query_stream_operation(table)],
        1 << 20,
        1_000_000_000,
        128,
        EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION,
        Some(EffectExecutionMode::GuestPromiseEventLoop),
    )
}

fn generated_guest_single_operation_module(fields: &[&str]) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    types.ty().function(
        [EncodedValType::I64, EncodedValType::I64],
        std::iter::empty(),
    );
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I64],
        [EncodedValType::I32],
    );
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);
    types
        .ty()
        .function([EncodedValType::I32], [EncodedValType::I32]);
    types
        .ty()
        .function([EncodedValType::I32], [EncodedValType::I64]);
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
        ("convex_request_field", 0),
        ("convex_value_array_new", 1),
        ("convex_value_array_push", 2),
        ("convex_async_operation_start_take", 3),
        ("convex_async_operation_wait_any", 4),
        ("convex_async_operation_completion_status", 5),
        ("convex_async_operation_completion_take", 6),
        ("convex_function_result", 7),
        ("convex_async_operation_poll_ready", 4),
        ("convex_async_operation_cancel_all", 4),
    ] {
        imports.import("convex", name, EncodedEntityType::Function(ty));
    }

    let imported_function_count = 10;
    let mut functions = EncodedFunctionSection::new();
    functions.function(8);
    functions.function(9);
    functions.function(8);
    functions.function(10);
    functions.function(9);
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
    let mut run = EncodedFunction::new([
        (1, EncodedValType::I64),
        (2, EncodedValType::I32),
        (1, EncodedValType::I64),
    ]);
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::LocalSet(0));
    let mut data_bytes = Vec::new();
    for field in fields {
        let offset = i32::try_from(data_bytes.len()).expect("test field offsets exceed i32");
        let length = i32::try_from(field.len()).expect("test field length exceeds i32");
        data_bytes.extend_from_slice(field.as_bytes());
        run.instruction(&EncodedInstruction::LocalGet(0));
        run.instruction(&EncodedInstruction::I32Const(offset));
        run.instruction(&EncodedInstruction::I32Const(length));
        run.instruction(&EncodedInstruction::Call(0));
        run.instruction(&EncodedInstruction::Call(2));
    }
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(3));
    run.instruction(&EncodedInstruction::LocalSet(1));
    run.instruction(&EncodedInstruction::Call(4));
    run.instruction(&EncodedInstruction::LocalSet(2));
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::LocalGet(2));
    run.instruction(&EncodedInstruction::I32Ne);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(2));
    run.instruction(&EncodedInstruction::Call(5));
    run.instruction(&EncodedInstruction::I32Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::Else);
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(2));
    run.instruction(&EncodedInstruction::Call(6));
    run.instruction(&EncodedInstruction::LocalSet(3));
    run.instruction(&EncodedInstruction::LocalGet(3));
    run.instruction(&EncodedInstruction::I64Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(3));
    run.instruction(&EncodedInstruction::Call(7));
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
    data.active(0, &EncodedConstExpr::i32_const(0), data_bytes);
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

#[derive(Clone, Copy)]
enum QueryStreamFinish {
    ExplicitClose,
    CancelRemaining,
    LeaveOpen,
}

fn generated_guest_query_stream_module(next_count: usize, finish: QueryStreamFinish) -> Vec<u8> {
    assert!(next_count > 0);
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    types.ty().function(
        [EncodedValType::I64, EncodedValType::I64],
        std::iter::empty(),
    );
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I64],
        [EncodedValType::I32],
    );
    types
        .ty()
        .function([EncodedValType::I32], [EncodedValType::I32]);
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);
    types
        .ty()
        .function([EncodedValType::I32], [EncodedValType::I64]);
    types
        .ty()
        .function([EncodedValType::I32], std::iter::empty());
    types
        .ty()
        .function([EncodedValType::I64], std::iter::empty());
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::I32]);

    let mut imports = EncodedImportSection::new();
    for (name, ty) in [
        ("convex_request_field", 0),
        ("convex_value_array_new", 1),
        ("convex_value_array_push", 2),
        ("convex_async_query_stream_open_take", 3),
        ("convex_async_query_stream_next", 4),
        ("convex_async_operation_wait_any", 5),
        ("convex_async_operation_completion_status", 4),
        ("convex_async_operation_completion_take", 6),
        ("convex_async_query_stream_close", 7),
        ("convex_value_release", 8),
        ("convex_function_result", 8),
        ("convex_async_operation_poll_ready", 5),
        ("convex_async_operation_cancel_all", 5),
    ] {
        imports.import("convex", name, EncodedEntityType::Function(ty));
    }

    let imported_function_count = 13;
    let mut functions = EncodedFunctionSection::new();
    functions.function(9);
    functions.function(5);
    functions.function(9);
    functions.function(10);
    functions.function(5);
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
    let mut run = EncodedFunction::new([
        (1, EncodedValType::I64),
        (3, EncodedValType::I32),
        (1, EncodedValType::I64),
    ]);
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::LocalSet(0));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::I32Const(6));
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::Call(2));
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(3));
    run.instruction(&EncodedInstruction::LocalSet(1));
    for index in 0..next_count {
        run.instruction(&EncodedInstruction::LocalGet(1));
        run.instruction(&EncodedInstruction::Call(4));
        run.instruction(&EncodedInstruction::LocalSet(2));
        run.instruction(&EncodedInstruction::Call(5));
        run.instruction(&EncodedInstruction::LocalSet(3));
        run.instruction(&EncodedInstruction::LocalGet(2));
        run.instruction(&EncodedInstruction::LocalGet(3));
        run.instruction(&EncodedInstruction::I32Ne);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::I32Const(1));
        run.instruction(&EncodedInstruction::Return);
        run.instruction(&EncodedInstruction::End);
        run.instruction(&EncodedInstruction::LocalGet(3));
        run.instruction(&EncodedInstruction::Call(6));
        run.instruction(&EncodedInstruction::I32Eqz);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::Else);
        run.instruction(&EncodedInstruction::I32Const(1));
        run.instruction(&EncodedInstruction::Return);
        run.instruction(&EncodedInstruction::End);
        run.instruction(&EncodedInstruction::LocalGet(3));
        run.instruction(&EncodedInstruction::Call(7));
        run.instruction(&EncodedInstruction::LocalSet(4));
        run.instruction(&EncodedInstruction::LocalGet(4));
        run.instruction(&EncodedInstruction::I64Eqz);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::I32Const(1));
        run.instruction(&EncodedInstruction::Return);
        run.instruction(&EncodedInstruction::End);
        if index + 1 < next_count {
            run.instruction(&EncodedInstruction::LocalGet(4));
            run.instruction(&EncodedInstruction::Call(9));
        }
    }
    if matches!(finish, QueryStreamFinish::ExplicitClose) {
        run.instruction(&EncodedInstruction::LocalGet(1));
        run.instruction(&EncodedInstruction::Call(8));
    }
    run.instruction(&EncodedInstruction::LocalGet(4));
    run.instruction(&EncodedInstruction::Call(10));
    if matches!(finish, QueryStreamFinish::CancelRemaining) {
        // Invocation teardown releases cursors without issuing an application
        // queryCleanup. No operation is pending after the last consumed value.
        run.instruction(&EncodedInstruction::Call(12));
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::Unreachable);
        run.instruction(&EncodedInstruction::End);
    }
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
        &EncodedConstExpr::i32_const(0),
        b"tenant".iter().copied(),
    );
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

async fn execute_guest_dynamic_take(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    request: JsonValue,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        generated_guest_single_operation_module(&["tenant", "limit"]),
    )?;
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
    execute_generated(routed, state, None, false).await
}

async fn execute_guest_query_stream(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    next_count: usize,
    finish: QueryStreamFinish,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        generated_guest_query_stream_module(next_count, finish),
    )?;
    let mut state = generated_test_state(
        rt.clone(),
        transaction,
        manifest,
        json!({ "tenant": "tenant-a" }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
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

async fn execute_guest_query_terminal_pair(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    manifest: Arc<WasmUdfExecutionManifest>,
    routed: Arc<GeneratedRoutedModule>,
    request: JsonValue,
    cancellation: CancellationSignal,
    read_control: Option<ReadControl>,
    metrics: Arc<GateMetrics>,
    reusable: Option<RetainedGeneratedTestRuntime>,
    controller: Option<Arc<GeneratedMemoryController>>,
    retain_on_success: bool,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let maximum_guest_memory_bytes = usize::try_from(manifest.limits().max_guest_memory_bytes())?;
    let (reusable_instance, existing_slot, values) = match reusable {
        Some(reusable) => (
            Some(reusable.instance),
            Some(reusable.memory_slot_id),
            Some(reusable.values),
        ),
        None => (None, None, None),
    };
    let memory_setup = controller.map(|controller| GeneratedTestMemorySetup {
        controller,
        route_identity: routed.route_identity.clone(),
        existing_slot,
        values,
        maximum_guest_memory_bytes,
    });
    let mut state = generated_test_state(
        rt.clone(),
        transaction,
        manifest,
        request,
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        cancellation,
        read_control,
        metrics,
        memory_setup,
    )
    .await?;
    state.provider.enable_host_operation_trace();
    arm_generated_timeout(
        rt,
        &mut state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    execute_generated(routed, state, reusable_instance, retain_on_success).await
}

fn generated_query_terminal_trace(
    output: &GeneratedExecutionOutput<ProdRuntime>,
) -> anyhow::Result<Vec<(LogicalHostOperation, LogicalHostOperationStatus)>> {
    let FunctionOutcome::Query(outcome) = output
        .invocation
        .function_outcome
        .as_ref()
        .context("query terminal did not produce a query outcome")?
    else {
        anyhow::bail!("query terminal produced a non-query outcome");
    };
    Ok(outcome
        .host_operation_trace
        .entries()
        .context("query terminal host-operation trace is disabled")?
        .iter()
        .map(|entry| (entry.operation(), entry.status()))
        .collect())
}

async fn run_dynamic_take_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_dynamic_take_documents".parse()?;
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
        ("tenant-a", "first"),
        ("tenant-a", "second"),
        ("tenant-b", "other"),
    ] {
        SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
            .insert_metadata(&table, obj!("tenant" => tenant, "marker" => marker)?)
            .await?;
    }
    database
        .commit_with_write_source(transaction, "generated_dynamic_take_setup")
        .await?;

    let blocking_manifest = dynamic_take_manifest(&table, EffectExecutionMode::BlockingFiber)?;
    let blocking = execute_generated_sequential_query_collect_test_module(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&blocking_manifest),
        json!({ "value": ["tenant-a", 2.0] }),
    )
    .await?;
    assert_eq!(
        blocking.invocation.outcome,
        InvocationOutcome::Success,
        "blocking dynamic take failed: {:#?}",
        blocking.system_error
    );
    assert_eq!(
        blocking
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .context("blocking dynamic take did not return an array")?
            .len(),
        2
    );
    assert_eq!(blocking.invocation.read_accounting.documents, 2);
    drop(blocking.invocation.transaction);

    let invalid_blocking = execute_generated_sequential_query_collect_test_module(
        rt.clone(),
        database.begin_system().await?,
        blocking_manifest,
        json!({ "value": ["tenant-a", 0.0] }),
    )
    .await?;
    assert!(matches!(
        invalid_blocking.invocation.outcome,
        InvocationOutcome::DeveloperError(_)
    ));
    assert_eq!(invalid_blocking.invocation.read_accounting.documents, 0);
    drop(invalid_blocking.invocation.transaction);

    let guest_manifest =
        dynamic_take_manifest(&table, EffectExecutionMode::GuestPromiseEventLoop)?;
    let guest = execute_guest_dynamic_take(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&guest_manifest),
        json!({ "tenant": "tenant-a", "limit": 1.0 }),
    )
    .await?;
    assert_eq!(guest.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        guest
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .context("guest dynamic take did not return an array")?
            .len(),
        1
    );
    assert_eq!(guest.invocation.read_accounting.documents, 1);
    drop(guest.invocation.transaction);

    let invalid_guest = execute_guest_dynamic_take(
        rt,
        database.begin_system().await?,
        guest_manifest,
        json!({ "tenant": "tenant-a", "limit": 100_001.0 }),
    )
    .await?;
    assert!(matches!(
        invalid_guest.invocation.outcome,
        InvocationOutcome::DeveloperError(_)
    ));
    assert_eq!(invalid_guest.invocation.read_accounting.documents, 0);
    drop(invalid_guest.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn dynamic_take_executes_in_blocking_and_guest_modes() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("generated_wasm_dynamic_take", run_dynamic_take_tests(rt))
}

async fn run_guest_query_stream_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_guest_query_stream_documents".parse()?;
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
    for marker in ["first", "second"] {
        SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
            .insert_metadata(&table, obj!("tenant" => "tenant-a", "marker" => marker)?)
            .await?;
    }
    database
        .commit_with_write_source(transaction, "generated_guest_query_stream_setup")
        .await?;
    let manifest = query_stream_manifest(&table)?;

    let early = execute_guest_query_stream(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        1,
        QueryStreamFinish::ExplicitClose,
    )
    .await?;
    assert_eq!(early.invocation.outcome, InvocationOutcome::Success);
    let early_result = early
        .invocation
        .function_result
        .as_ref()
        .context("query stream value result is missing")?;
    assert_eq!(
        early_result.get("done").and_then(JsonValue::as_bool),
        Some(false)
    );
    assert_eq!(
        early_result
            .pointer("/value/tenant")
            .and_then(JsonValue::as_str),
        Some("tenant-a")
    );
    assert_eq!(early.invocation.read_accounting.documents, 1);
    let FunctionOutcome::Query(early_outcome) = early
        .invocation
        .function_outcome
        .as_ref()
        .context("query stream did not produce a query outcome")?
    else {
        anyhow::bail!("query stream produced a non-query outcome");
    };
    assert_eq!(
        early_outcome
            .host_operation_trace
            .entries()
            .context("query stream host-operation trace is disabled")?
            .iter()
            .map(|entry| (entry.operation(), entry.status()))
            .collect::<Vec<_>>(),
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
                LogicalHostOperation::DatabaseQueryCleanup,
                LogicalHostOperationStatus::Success,
            ),
        ]
    );
    drop(early.invocation.transaction);

    let exhausted = execute_guest_query_stream(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        3,
        QueryStreamFinish::LeaveOpen,
    )
    .await?;
    assert_eq!(exhausted.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        exhausted.invocation.function_result,
        Some(json!({ "done": true, "value": null }))
    );
    assert_eq!(exhausted.invocation.read_accounting.documents, 2);
    let FunctionOutcome::Query(exhausted_outcome) = exhausted
        .invocation
        .function_outcome
        .as_ref()
        .context("exhausted query did not produce a query outcome")?
    else {
        anyhow::bail!("exhausted query produced a non-query outcome");
    };
    assert_eq!(
        exhausted_outcome
            .host_operation_trace
            .entries()
            .context("exhausted query host-operation trace is disabled")?
            .iter()
            .map(|entry| (entry.operation(), entry.status()))
            .collect::<Vec<_>>(),
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
    drop(exhausted.invocation.transaction);

    let abandoned = execute_guest_query_stream(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&manifest),
        1,
        QueryStreamFinish::CancelRemaining,
    )
    .await?;
    assert_eq!(abandoned.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(abandoned.invocation.read_accounting.documents, 1);
    assert_eq!(abandoned.invocation.opaque_live_handles, 0);
    assert_eq!(abandoned.invocation.opaque_current_bytes, 0);
    assert_eq!(
        generated_query_terminal_trace(&abandoned)?,
        vec![
            (
                LogicalHostOperation::DatabaseQueryStream,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryStreamNext,
                LogicalHostOperationStatus::Success,
            ),
        ]
    );
    drop(abandoned.invocation.transaction);

    let leaked = execute_guest_query_stream(
        rt,
        database.begin_system().await?,
        manifest,
        1,
        QueryStreamFinish::LeaveOpen,
    )
    .await?;
    assert_eq!(leaked.invocation.outcome, InvocationOutcome::SystemError);
    assert!(leaked.invocation.function_result.is_none());
    assert_eq!(
        leaked
            .system_error
            .as_ref()
            .and_then(|error| error.downcast_ref::<StaticHermesWasmExecutionFailure>()),
        Some(&StaticHermesWasmExecutionFailure::ResultFinalization)
    );
    drop(leaked.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn guest_query_stream_advances_closes_and_exhausts_exactly() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_query_stream",
        run_guest_query_stream_tests(rt),
    )
}

async fn run_guest_query_terminal_wave_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_guest_query_terminal_wave_documents".parse()?;
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
        ("long", "long-a"),
        ("long", "long-b"),
        ("long", "long-c"),
        ("short", "short"),
        ("fifo-a", "fifo-a"),
        ("fifo-b", "fifo-b"),
    ] {
        SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
            .insert_metadata(&table, obj!("tenant" => tenant, "marker" => marker)?)
            .await?;
    }
    database
        .commit_with_write_source(transaction, "generated_guest_query_terminal_wave_setup")
        .await?;

    let collect_first_manifest = query_terminal_pair_manifest(&table, "collect", "first")?;
    let collect_first_routed = generated_test_routed_module_from_bytes(
        Arc::clone(&collect_first_manifest),
        generated_guest_query_terminal_pair_module(&[1, 0], &[0, 0], 0),
    )?;
    let collect_first_metrics = Arc::new(GateMetrics::default());
    let collect_first = execute_guest_query_terminal_pair(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&collect_first_manifest),
        collect_first_routed,
        json!({ "firstTenant": "long", "secondTenant": "short" }),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&collect_first_metrics),
        None,
        None,
        false,
    )
    .await?;
    assert_eq!(collect_first.invocation.outcome, InvocationOutcome::Success);
    let collect_first_result = collect_first
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .context("collect/first completion result is not an array")?;
    assert_eq!(
        collect_first_result[0]
            .get("marker")
            .and_then(JsonValue::as_str),
        Some("short")
    );
    assert_eq!(
        collect_first_result[1]
            .as_array()
            .context("collect completion is not an array")?
            .len(),
        3
    );
    assert_eq!(
        collect_first_metrics.read_completed.load(Ordering::SeqCst),
        4
    );
    assert_eq!(
        collect_first_metrics
            .database_query_starts
            .load(Ordering::SeqCst),
        2
    );
    assert_eq!(collect_first.invocation.read_accounting.documents, 4);
    assert_eq!(collect_first.invocation.opaque_live_handles, 0);
    assert_eq!(collect_first.invocation.opaque_current_bytes, 0);
    drop(collect_first.invocation.transaction);

    let first_pair_manifest = query_terminal_pair_manifest(&table, "first", "first")?;
    let first_pair_routed = generated_test_routed_module_from_bytes(
        Arc::clone(&first_pair_manifest),
        generated_guest_query_terminal_pair_module(&[0, 1], &[0, 0], 0),
    )?;
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 1,
        soft_budget_bytes: 32 * WASM_PAGE_BYTES,
        hard_budget_bytes: 48 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 64 * WASM_PAGE_BYTES,
    });
    let reuse_metrics = Arc::new(GateMetrics::default());
    let request = || json!({ "firstTenant": "fifo-a", "secondTenant": "fifo-b" });
    let mut first_pair = execute_guest_query_terminal_pair(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&first_pair_manifest),
        Arc::clone(&first_pair_routed),
        request(),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&reuse_metrics),
        None,
        Some(Arc::clone(&controller)),
        true,
    )
    .await?;
    assert_eq!(first_pair.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        first_pair
            .invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .and_then(|values| values.first())
            .and_then(|value| value.get("marker"))
            .and_then(JsonValue::as_str),
        Some("fifo-a")
    );
    assert_eq!(reuse_metrics.read_completed.load(Ordering::SeqCst), 2);
    assert_eq!(
        generated_query_terminal_trace(&first_pair)?,
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
        ]
    );
    let first_runtime_id = first_pair
        .reusable_instance
        .as_ref()
        .context("query-terminal runtime was not retained")?
        .id;
    let retained = retain_generated_test_runtime(&mut first_pair)?;
    drop(first_pair.invocation.transaction);

    let mut reused_pair = execute_guest_query_terminal_pair(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&first_pair_manifest),
        Arc::clone(&first_pair_routed),
        request(),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&reuse_metrics),
        Some(retained),
        Some(Arc::clone(&controller)),
        true,
    )
    .await?;
    assert_eq!(reused_pair.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        reused_pair
            .reusable_instance
            .as_ref()
            .context("query-terminal reused runtime was not retained")?
            .id,
        first_runtime_id
    );
    assert_eq!(reuse_metrics.read_completed.load(Ordering::SeqCst), 4);
    let reused_instance = reused_pair
        .reusable_instance
        .take()
        .context("query-terminal reused runtime disappeared")?;
    discard_generated_instance(reused_instance).await?;
    reused_pair
        .memory_permit
        .take()
        .context("query-terminal reused runtime lost its memory permit")?
        .finish(reused_pair.terminal_memory_outcome, false);
    drop(reused_pair.invocation.transaction);

    let unique_first_manifest = query_terminal_pair_manifest(&table, "unique", "first")?;
    let unique_first_routed = generated_test_routed_module_from_bytes(
        Arc::clone(&unique_first_manifest),
        generated_guest_query_terminal_pair_module(&[1, 0], &[0, 1], 0),
    )?;
    let unique_first_metrics = Arc::new(GateMetrics::default());
    let unique_first = execute_guest_query_terminal_pair(
        rt.clone(),
        database.begin_system().await?,
        unique_first_manifest,
        unique_first_routed,
        json!({ "firstTenant": "long", "secondTenant": "short" }),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&unique_first_metrics),
        None,
        None,
        false,
    )
    .await?;
    assert_eq!(unique_first.invocation.outcome, InvocationOutcome::Success);
    let unique_first_result = unique_first
        .invocation
        .function_result
        .as_ref()
        .and_then(JsonValue::as_array)
        .context("unique/first completion result is not an array")?;
    assert_eq!(
        unique_first_result[0]
            .get("marker")
            .and_then(JsonValue::as_str),
        Some("short")
    );
    assert!(unique_first_result[1]
        .as_str()
        .is_some_and(|message| message.contains("unique() query returned more than one result")));
    assert_eq!(
        unique_first_metrics.read_completed.load(Ordering::SeqCst),
        3
    );
    assert_eq!(
        generated_query_terminal_trace(&unique_first)?,
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
                LogicalHostOperation::DatabaseQueryStreamNext,
                LogicalHostOperationStatus::Success,
            ),
            (
                LogicalHostOperation::DatabaseQueryCleanup,
                LogicalHostOperationStatus::Success,
            ),
        ]
    );
    assert_eq!(unique_first.invocation.first_developer_error_index, None);
    assert_eq!(unique_first.invocation.opaque_live_handles, 0);
    assert_eq!(unique_first.invocation.opaque_current_bytes, 0);
    drop(unique_first.invocation.transaction);

    let race_routed = generated_test_routed_module_from_bytes(
        Arc::clone(&collect_first_manifest),
        generated_guest_query_terminal_pair_module(&[1], &[0], 1),
    )?;
    let race_metrics = Arc::new(GateMetrics::default());
    let race_request = || json!({ "firstTenant": "long", "secondTenant": "short" });
    let mut race = execute_guest_query_terminal_pair(
        rt.clone(),
        database.begin_system().await?,
        Arc::clone(&collect_first_manifest),
        Arc::clone(&race_routed),
        race_request(),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&race_metrics),
        None,
        Some(Arc::clone(&controller)),
        true,
    )
    .await?;
    assert_eq!(race.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        race.invocation
            .function_result
            .as_ref()
            .and_then(JsonValue::as_array)
            .and_then(|values| values.first())
            .and_then(|value| value.get("marker"))
            .and_then(JsonValue::as_str),
        Some("short")
    );
    assert_eq!(race_metrics.read_completed.load(Ordering::SeqCst), 2);
    assert_eq!(race.invocation.opaque_live_handles, 0);
    assert_eq!(race.invocation.opaque_current_bytes, 0);
    let race_runtime_id = race
        .reusable_instance
        .as_ref()
        .context("race-winner runtime was not retained")?
        .id;
    let retained_race = retain_generated_test_runtime(&mut race)?;
    drop(race.invocation.transaction);

    let mut reused_race = execute_guest_query_terminal_pair(
        rt.clone(),
        database.begin_system().await?,
        collect_first_manifest,
        race_routed,
        race_request(),
        CancellationSignal::new_for_test(),
        None,
        Arc::clone(&race_metrics),
        Some(retained_race),
        Some(Arc::clone(&controller)),
        true,
    )
    .await?;
    assert_eq!(reused_race.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(
        reused_race
            .reusable_instance
            .as_ref()
            .context("race-winner reused runtime was not retained")?
            .id,
        race_runtime_id
    );
    assert_eq!(race_metrics.read_completed.load(Ordering::SeqCst), 4);
    let reused_race_instance = reused_race
        .reusable_instance
        .take()
        .context("race-winner reused runtime disappeared")?;
    discard_generated_instance(reused_race_instance).await?;
    reused_race
        .memory_permit
        .take()
        .context("race-winner reused runtime lost its memory permit")?
        .finish(reused_race.terminal_memory_outcome, false);
    drop(reused_race.invocation.transaction);

    let cancellation = CancellationSignal::new_for_test();
    let (control, mut host) = read_control();
    let cancellation_metrics = Arc::new(GateMetrics::default());
    let cancellation_routed = generated_test_routed_module_from_bytes(
        Arc::clone(&first_pair_manifest),
        generated_guest_query_terminal_pair_module(&[0, 1], &[0, 0], 0),
    )?;
    let execution = tokio::spawn(execute_guest_query_terminal_pair(
        rt.clone(),
        database.begin_system().await?,
        first_pair_manifest,
        cancellation_routed,
        request(),
        cancellation.clone(),
        Some(control),
        Arc::clone(&cancellation_metrics),
        None,
        None,
        false,
    ));
    tokio::time::timeout(Duration::from_secs(5), &mut host.entered)
        .await
        .context("query-terminal wave did not become pending")??;
    cancellation.cancel_for_test();
    let cancelled = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .context("query-terminal cancellation did not complete")???;
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.invocation.opaque_live_handles, 0);
    assert_eq!(cancelled.invocation.opaque_current_bytes, 0);
    assert_eq!(
        cancellation_metrics.read_cancelled.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        cancellation_metrics
            .database_query_starts
            .load(Ordering::SeqCst),
        2
    );
    assert!(host
        .release
        .take()
        .context("query-terminal cancellation release missing")?
        .send(())
        .is_err());
    drop(cancelled.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn guest_query_terminals_use_deterministic_waves_and_cleanup() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_query_terminal_waves",
        run_guest_query_terminal_wave_tests(rt),
    )
}
