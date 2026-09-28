use super::*;

#[cfg(any(test, feature = "testing"))]
mod fixtures;
#[cfg(any(test, feature = "testing"))]
mod harness;

#[cfg(test)]
pub(super) use fixtures::{
    generated_capability_async_operation_test_module,
    generated_database_normalize_id_test_module,
    generated_entry_prepare_environment_test_module,
    generated_fixed_entry_prepare_test_module,
    generated_guest_promise_database_get_test_module,
    generated_guest_promise_host_operation_error_test_module,
    generated_guest_promise_test_manifest_with_runtime_limits,
    generated_host_secret_verify_test_module,
    generated_memory_test_module,
    generated_schema_five_test_manifest_with_runtime_limits,
    generated_selected_entry_dispatch_fault_test_module,
    generated_test_manifest,
    generated_test_manifest_with_limits,
    generated_test_manifest_with_limits_and_fuel,
    generated_test_manifest_with_runtime_limits,
    generated_test_manifest_with_timeout,
    generated_test_module,
    GeneratedAsyncBatchTestOperation,
    GeneratedDatabaseWriteHandleFault,
    GeneratedDatabaseWriteKind,
    GeneratedDatabaseWriteTestOperation,
    GeneratedFunctionHandleTestOperation,
    GeneratedGuestPromiseHostOperationErrorMode,
    GeneratedMemoryTestOperation,
    GeneratedSelectedEntryDispatchFault,
    GeneratedTestOperation,
};
#[cfg(any(test, feature = "testing"))]
pub(super) use fixtures::{
    generated_occ_test_routed_module,
    generated_test_manifest_with_runtime_limits_and_schema,
};
#[cfg(any(test, feature = "testing"))]
pub(super) use harness::generated_test_routed_module_from_bytes;
#[cfg(test)]
pub(super) use harness::{
    generated_async_batch_test_routed_module,
    generated_database_normalize_id_test_routed_module,
    generated_host_secret_verify_test_routed_module,
    generated_memory_test_routed_module,
    generated_memory_test_routed_module_with_generation,
    generated_routed_test_state,
    generated_sequential_query_collect_test_routed_module,
    generated_test_execution_context,
    generated_test_routed_module,
    generated_test_state,
    generated_test_state_with_rng_seed,
    GeneratedRoutedTestMemorySetup,
    GeneratedTestMemorySetup,
    RetainedGeneratedTestRuntime,
};

pub(super) fn query_terminal_pair_manifest(
    table: &TableName,
    first_terminal: &str,
    second_terminal: &str,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    let operation = |id: u32, debug_name: &str, terminal: &str| {
        json!({
            "id": id,
            "debugName": debug_name,
            "operation": {
                "kind": "databaseIndexQuery",
                "tableName": table,
                "indexName": "by_tenant",
                "constraints": [
                    { "fieldPath": "tenant", "operator": "eq" },
                ],
                "order": "ascending",
                "terminal": terminal,
                "limit": null,
            },
        })
    };
    generated_test_manifest_with_runtime_limits_and_schema(
        ManifestUdfKind::Query,
        vec![
            operation(1, "firstTerminal", first_terminal),
            operation(2, "secondTerminal", second_terminal),
        ],
        1 << 20,
        1_000_000_000,
        128,
        EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION,
        Some(EffectExecutionMode::GuestPromiseEventLoop),
    )
}

pub(super) fn generated_guest_query_terminal_pair_module(
    completion_order: &[usize],
    completion_statuses: &[i32],
    expected_abandoned_operations: i32,
) -> Vec<u8> {
    assert_eq!(completion_order.len(), completion_statuses.len());
    assert!(completion_order.len() <= 2);
    assert!(completion_order.iter().all(|index| *index < 2));
    assert_eq!(
        completion_order
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len(),
        completion_order.len()
    );
    assert!(completion_statuses
        .iter()
        .all(|status| matches!(*status, 0 | 1)));
    assert_eq!(
        usize::try_from(expected_abandoned_operations)
            .expect("test abandoned operation count must be nonnegative")
            + completion_order.len(),
        2
    );

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
        ("convex_async_operation_cancel_all", 4),
        ("convex_function_result", 7),
        ("convex_async_operation_poll_ready", 4),
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
        (2, EncodedValType::I64),
        (3, EncodedValType::I32),
        (1, EncodedValType::I64),
    ]);
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::LocalSet(0));
    let fields = [
        ("firstTenant", 1_i32, 2_u32),
        ("secondTenant", 2_i32, 3_u32),
    ];
    let mut field_offset = 0_i32;
    let mut data_bytes = Vec::new();
    for (field, operation_id, operation_handle_local) in fields {
        let field_length = i32::try_from(field.len()).expect("test field length exceeds i32");
        data_bytes.extend_from_slice(field.as_bytes());
        run.instruction(&EncodedInstruction::Call(1));
        run.instruction(&EncodedInstruction::LocalSet(1));
        run.instruction(&EncodedInstruction::LocalGet(1));
        run.instruction(&EncodedInstruction::I32Const(field_offset));
        run.instruction(&EncodedInstruction::I32Const(field_length));
        run.instruction(&EncodedInstruction::Call(0));
        run.instruction(&EncodedInstruction::Call(2));
        run.instruction(&EncodedInstruction::I32Const(operation_id));
        run.instruction(&EncodedInstruction::LocalGet(1));
        run.instruction(&EncodedInstruction::Call(3));
        run.instruction(&EncodedInstruction::LocalSet(operation_handle_local));
        field_offset += field_length;
    }

    for (completion_index, operation_index) in completion_order.iter().copied().enumerate() {
        let operation_handle_local =
            2_u32 + u32::try_from(operation_index).expect("test operation index exceeds u32");
        run.instruction(&EncodedInstruction::Call(4));
        run.instruction(&EncodedInstruction::LocalSet(4));
        run.instruction(&EncodedInstruction::LocalGet(4));
        run.instruction(&EncodedInstruction::LocalGet(operation_handle_local));
        run.instruction(&EncodedInstruction::I32Ne);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::I32Const(1));
        run.instruction(&EncodedInstruction::Return);
        run.instruction(&EncodedInstruction::End);
        run.instruction(&EncodedInstruction::LocalGet(4));
        run.instruction(&EncodedInstruction::Call(5));
        run.instruction(&EncodedInstruction::I32Const(
            completion_statuses[completion_index],
        ));
        run.instruction(&EncodedInstruction::I32Ne);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::I32Const(1));
        run.instruction(&EncodedInstruction::Return);
        run.instruction(&EncodedInstruction::End);
        run.instruction(&EncodedInstruction::LocalGet(4));
        run.instruction(&EncodedInstruction::Call(6));
        run.instruction(&EncodedInstruction::LocalSet(5));
        run.instruction(&EncodedInstruction::LocalGet(0));
        run.instruction(&EncodedInstruction::LocalGet(5));
        run.instruction(&EncodedInstruction::Call(2));
    }
    run.instruction(&EncodedInstruction::Call(7));
    run.instruction(&EncodedInstruction::I32Const(expected_abandoned_operations));
    run.instruction(&EncodedInstruction::I32Ne);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(8));
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

pub(super) fn generated_query_terminal_pair_routed_module(
    table_name: &str,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    let table: TableName = table_name.parse()?;
    let manifest = query_terminal_pair_manifest(&table, "first", "first")?;
    generated_test_routed_module_from_bytes(
        manifest,
        generated_guest_query_terminal_pair_module(&[0, 1], &[0, 0], 0),
    )
}

/// The generated side of the authenticated query-shadow fixture.
///
/// It uses the blocking-fiber query cursor ABI so the query start and ordered
/// advances follow the same logical host-operation sequence as the V8 query.
/// The caller supplies `batch` as one `authenticationGetUserIdentity`
/// invocation, which makes the generated lane observe its invocation identity
/// without embedding identity data in this fixture.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn query_shadow_observation_imported_operations(table: &TableName) -> Vec<JsonValue> {
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
            "debugName": "documentsByTenant",
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
            },
        }),
    ]
}

/// Build a small generated query that observes the paired invocation inputs.
///
/// The registry fixture supplies its authenticated manifest separately. Keeping
/// the module bytes here makes the test package use the ordinary generated
/// module contract instead of a routing test hook.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn generated_query_shadow_observation_module() -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::F64]);
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::F64]);
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::I64]);
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I64],
        [EncodedValType::I64],
    );
    types
        .ty()
        .function([EncodedValType::I64], std::iter::empty());
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);

    let mut imports = EncodedImportSection::new();
    for (name, ty) in [
        (INVOCATION_UNIX_TIMESTAMP_MS_IMPORT, 0),
        ("convex_capability_current", 1),
        ("convex_math_random", 2),
        ("convex_request_field", 3),
        ("convex_async_batch_take", 4),
        ("convex_query_start_value", 5),
        ("convex_query_next", 4),
        ("convex_value_release", 6),
        ("convex_value_string_new", 3),
        ("convex_function_result", 6),
    ] {
        imports.import("convex", name, EncodedEntityType::Function(ty));
    }

    let imported_function_count = 10;
    let mut functions = EncodedFunctionSection::new();
    functions.function(7);
    functions.function(8);
    functions.function(7);
    functions.function(8);
    functions.function(8);

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
    let mut run = EncodedFunction::new([(5, EncodedValType::I64)]);

    // These imports mark the exact invocation clock and RNG as observed.
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::Drop);
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::LocalSet(0));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(2));
    run.instruction(&EncodedInstruction::Drop);

    // `batch` contains the one authenticated identity operation specified by
    // the caller. The returned value is released immediately after the host
    // operation has recorded the identity observation.
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::I32Const(5));
    run.instruction(&EncodedInstruction::Call(3));
    run.instruction(&EncodedInstruction::Call(4));
    run.instruction(&EncodedInstruction::LocalSet(1));
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::I64Const(-1));
    run.instruction(&EncodedInstruction::I64Eq);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::Call(7));

    // A single matching document makes these two advances observe the value
    // and then query exhaustion, matching a V8 `.collect()` query.
    run.instruction(&EncodedInstruction::I32Const(8));
    run.instruction(&EncodedInstruction::I32Const(6));
    run.instruction(&EncodedInstruction::Call(3));
    run.instruction(&EncodedInstruction::LocalSet(3));
    run.instruction(&EncodedInstruction::I32Const(2));
    run.instruction(&EncodedInstruction::LocalGet(3));
    run.instruction(&EncodedInstruction::Call(5));
    run.instruction(&EncodedInstruction::LocalSet(2));
    run.instruction(&EncodedInstruction::LocalGet(2));
    run.instruction(&EncodedInstruction::I64Const(-1));
    run.instruction(&EncodedInstruction::I64Eq);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(2));
    run.instruction(&EncodedInstruction::Call(6));
    run.instruction(&EncodedInstruction::LocalSet(3));
    run.instruction(&EncodedInstruction::LocalGet(3));
    run.instruction(&EncodedInstruction::I64Const(-1));
    run.instruction(&EncodedInstruction::I64Eq);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(3));
    run.instruction(&EncodedInstruction::Call(7));
    run.instruction(&EncodedInstruction::LocalGet(2));
    run.instruction(&EncodedInstruction::Call(6));
    run.instruction(&EncodedInstruction::I64Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::Else);
    run.instruction(&EncodedInstruction::Unreachable);
    run.instruction(&EncodedInstruction::End);

    run.instruction(&EncodedInstruction::I32Const(16));
    run.instruction(&EncodedInstruction::I32Const(2));
    run.instruction(&EncodedInstruction::Call(8));
    run.instruction(&EncodedInstruction::Call(9));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::End);

    let mut destroy = EncodedFunction::new([]);
    destroy.instruction(&EncodedInstruction::End);
    let mut select_entry = EncodedFunction::new([]);
    select_entry.instruction(&EncodedInstruction::I32Const(0));
    select_entry.instruction(&EncodedInstruction::End);
    let mut prepare = EncodedFunction::new([]);
    prepare.instruction(&EncodedInstruction::I32Const(0));
    prepare.instruction(&EncodedInstruction::End);

    let mut code = EncodedCodeSection::new();
    code.function(&initialize);
    code.function(&run);
    code.function(&destroy);
    code.function(&select_entry);
    code.function(&prepare);

    let mut data = EncodedDataSection::new();
    data.active(
        0,
        &EncodedConstExpr::i32_const(0),
        "batch\0\0\0tenant\0\0ok".bytes(),
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
