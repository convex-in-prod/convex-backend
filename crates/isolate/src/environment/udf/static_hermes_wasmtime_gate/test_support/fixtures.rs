use super::{
    super::*,
    harness::generated_test_routed_module_from_bytes,
};

#[cfg(any(test, feature = "testing"))]
pub(in super::super) fn generated_occ_test_routed_module(
    read_table_name: &str,
    insert_table_name: &str,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    let manifest = generated_test_manifest_with_runtime_limits(
        ManifestUdfKind::Mutation,
        vec![
            json!({
                "id": 1,
                "debugName": "readConflictDocument",
                "operation": {
                    "kind": "databaseGet",
                    "tableName": read_table_name,
                },
            }),
            json!({
                "id": 2,
                "debugName": "insertEffectDocument",
                "operation": {
                    "kind": "databaseInsert",
                    "tableName": insert_table_name,
                },
            }),
        ],
        1 << 20,
        1_000_000,
        6,
    )?;

    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I64],
        [EncodedValType::I64],
    );
    types.ty().function(
        [
            EncodedValType::I32,
            EncodedValType::I64,
            EncodedValType::I64,
        ],
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
    imports.import(
        "convex",
        "convex_request_field",
        EncodedEntityType::Function(0),
    );
    imports.import("convex", "convex_db_get", EncodedEntityType::Function(1));
    imports.import("convex", "convex_db_write", EncodedEntityType::Function(2));
    imports.import(
        "convex",
        "convex_value_release",
        EncodedEntityType::Function(3),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(3),
    );

    let imported_function_count = 5;
    let mut functions = EncodedFunctionSection::new();
    functions.function(4);
    functions.function(5);
    functions.function(4);
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
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        imported_function_count + 3,
    );

    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([(2, EncodedValType::I64)]);
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
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(3));
    run.instruction(&EncodedInstruction::I32Const(2));
    run.instruction(&EncodedInstruction::I64Const(0));
    run.instruction(&EncodedInstruction::I32Const(2));
    run.instruction(&EncodedInstruction::I32Const(5));
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::Call(2));
    run.instruction(&EncodedInstruction::LocalSet(1));
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::I64Const(-1));
    run.instruction(&EncodedInstruction::I64Eq);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(1));
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
    data.active(0, &EncodedConstExpr::i32_const(0), "idvalue".bytes());
    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&memory)
        .section(&exports)
        .section(&code)
        .section(&data);
    generated_test_routed_module_from_bytes(manifest, module.finish())
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(in super::super) enum GeneratedTestOperation {
    DatabaseGet,
    DatabaseWrite(GeneratedDatabaseWriteTestOperation),
    FunctionHandleCreate(GeneratedFunctionHandleTestOperation),
    Scheduler { time_milliseconds: f64 },
}

#[cfg(test)]
pub(in super::super) fn generated_guest_promise_database_get_test_module() -> Vec<u8> {
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
    imports.import(
        "convex",
        "convex_request_field",
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_value_array_new",
        EncodedEntityType::Function(1),
    );
    imports.import(
        "convex",
        "convex_value_array_push",
        EncodedEntityType::Function(2),
    );
    imports.import(
        "convex",
        "convex_async_operation_start_take",
        EncodedEntityType::Function(3),
    );
    imports.import(
        "convex",
        "convex_async_operation_wait_any",
        EncodedEntityType::Function(4),
    );
    imports.import(
        "convex",
        "convex_async_operation_completion_status",
        EncodedEntityType::Function(5),
    );
    imports.import(
        "convex",
        "convex_async_operation_completion_take",
        EncodedEntityType::Function(6),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(7),
    );
    imports.import(
        "convex",
        "convex_async_operation_poll_ready",
        EncodedEntityType::Function(4),
    );
    imports.import(
        "convex",
        "convex_async_operation_cancel_all",
        EncodedEntityType::Function(4),
    );

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
        (2, EncodedValType::I64),
    ]);
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::LocalSet(0));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::I32Const(2));
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::Call(2));
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

#[cfg(test)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in super::super) enum GeneratedGuestPromiseHostOperationErrorMode {
    Direct,
    RethrowSame,
    CaughtAndWrapped,
    CaughtSuccess,
}

#[cfg(test)]
pub(in super::super) fn generated_guest_promise_host_operation_error_test_module(
    mode: GeneratedGuestPromiseHostOperationErrorMode,
    operation: GeneratedDatabaseWriteKind,
) -> Vec<u8> {
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
    types.ty().function(
        [
            EncodedValType::I32,
            EncodedValType::I32,
            EncodedValType::I32,
        ],
        std::iter::empty(),
    );
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
        ("convex_value_release", 7),
        ("convex_developer_error", 8),
        ("convex_value_null_new", 1),
        ("convex_function_result", 7),
        ("convex_async_operation_poll_ready", 4),
        ("convex_async_operation_cancel_all", 4),
    ] {
        imports.import("convex", name, EncodedEntityType::Function(ty));
    }

    let imported_function_count = 13;
    let mut functions = EncodedFunctionSection::new();
    functions.function(9);
    functions.function(10);
    functions.function(9);
    functions.function(11);
    functions.function(10);

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
    let request_fields: &[(i32, i32)] = match operation {
        GeneratedDatabaseWriteKind::Patch | GeneratedDatabaseWriteKind::Replace => {
            &[(0, 2), (2, 5)]
        },
        GeneratedDatabaseWriteKind::Delete => &[(0, 2)],
        GeneratedDatabaseWriteKind::Insert => {
            panic!("host-operation error fixture does not support database insert")
        },
    };
    for &(pointer, length) in request_fields {
        run.instruction(&EncodedInstruction::LocalGet(0));
        run.instruction(&EncodedInstruction::I32Const(pointer));
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
    match mode {
        GeneratedGuestPromiseHostOperationErrorMode::Direct
        | GeneratedGuestPromiseHostOperationErrorMode::RethrowSame => {
            run.instruction(&EncodedInstruction::I32Const(7));
            run.instruction(&EncodedInstruction::I32Const(6));
            run.instruction(&EncodedInstruction::LocalGet(1));
            run.instruction(&EncodedInstruction::Call(8));
        },
        GeneratedGuestPromiseHostOperationErrorMode::CaughtAndWrapped => {
            run.instruction(&EncodedInstruction::I32Const(13));
            run.instruction(&EncodedInstruction::I32Const(7));
            run.instruction(&EncodedInstruction::I32Const(0));
            run.instruction(&EncodedInstruction::Call(8));
        },
        GeneratedGuestPromiseHostOperationErrorMode::CaughtSuccess => {
            run.instruction(&EncodedInstruction::Call(9));
            run.instruction(&EncodedInstruction::Call(10));
        },
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
        "idvaluedirectwrapped".bytes(),
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

#[cfg(test)]
pub(in super::super) fn generated_capability_async_operation_test_module(
    request_envelope: &[u8],
    expected_completion_status: i32,
) -> Vec<u8> {
    assert!(matches!(expected_completion_status, 0 | 1));
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    types.ty().function(
        [EncodedValType::I64, EncodedValType::I64],
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
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);

    let mut imports = EncodedImportSection::new();
    for (name, ty) in [
        ("convex_capability_request_decode", 0),
        ("convex_capability_request_release", 5),
        ("convex_capability_current", 9),
        ("convex_capability_start_take", 1),
        ("convex_async_operation_wait_any", 2),
        ("convex_async_operation_completion_status", 3),
        ("convex_async_operation_completion_take", 4),
        ("convex_function_result", 5),
        ("convex_async_operation_poll_ready", 2),
        ("convex_async_operation_cancel_all", 2),
    ] {
        imports.import("convex", name, EncodedEntityType::Function(ty));
    }

    let imported_function_count = 10;
    let mut functions = EncodedFunctionSection::new();
    functions.function(6);
    functions.function(7);
    functions.function(6);
    functions.function(8);
    functions.function(7);
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
    let mut run = EncodedFunction::new([
        (1, EncodedValType::I64),
        (2, EncodedValType::I32),
        (2, EncodedValType::I64),
    ]);
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::I32Const(
        i32::try_from(request_envelope.len()).expect("test request envelope exceeds the Wasm ABI"),
    ));
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::LocalSet(0));
    run.instruction(&EncodedInstruction::Call(2));
    run.instruction(&EncodedInstruction::LocalSet(4));
    run.instruction(&EncodedInstruction::GlobalGet(0));
    run.instruction(&EncodedInstruction::I64Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::Else);
    run.instruction(&EncodedInstruction::GlobalGet(0));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(3));
    run.instruction(&EncodedInstruction::I32Const(-1));
    run.instruction(&EncodedInstruction::I32Ne);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(4));
    run.instruction(&EncodedInstruction::GlobalSet(0));
    run.instruction(&EncodedInstruction::LocalGet(4));
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
    run.instruction(&EncodedInstruction::I32Const(expected_completion_status));
    run.instruction(&EncodedInstruction::I32Ne);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
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
    data.active(
        0,
        &EncodedConstExpr::i32_const(0),
        request_envelope.iter().copied(),
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

#[cfg(test)]
#[derive(Clone, Copy)]
pub(in super::super) struct GeneratedFunctionHandleTestOperation {
    pub(in super::super) operation_id: i32,
    pub(in super::super) stale_reference: bool,
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(in super::super) struct GeneratedDatabaseWriteTestOperation {
    pub(in super::super) kind: GeneratedDatabaseWriteKind,
    pub(in super::super) operation_id: i32,
    pub(in super::super) handle_fault: GeneratedDatabaseWriteHandleFault,
    pub(in super::super) import_database_get: bool,
}

#[cfg(test)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in super::super) enum GeneratedDatabaseWriteKind {
    Insert,
    Patch,
    Replace,
    Delete,
}

#[cfg(test)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in super::super) enum GeneratedDatabaseWriteHandleFault {
    None,
    WrongIdShape,
    StaleValue,
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(in super::super) struct GeneratedMemoryTestOperation {
    pub(in super::super) additional_pages: u32,
    pub(in super::super) destroy_infinite_loop: bool,
    pub(in super::super) loop_iterations: Option<u64>,
    pub(in super::super) maximum_pages: u64,
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(in super::super) struct GeneratedAsyncBatchTestOperation {
    pub(in super::super) import_database_write: bool,
    pub(in super::super) later_database_write_operation_id: Option<i32>,
}

#[cfg(test)]
pub(in super::super) fn generated_test_module(operation: GeneratedTestOperation) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    match operation {
        GeneratedTestOperation::DatabaseGet | GeneratedTestOperation::FunctionHandleCreate(_) => {
            types.ty().function(
                [EncodedValType::I32, EncodedValType::I64],
                [EncodedValType::I64],
            )
        },
        GeneratedTestOperation::DatabaseWrite(_) => types.ty().function(
            [
                EncodedValType::I32,
                EncodedValType::I64,
                EncodedValType::I64,
            ],
            [EncodedValType::I64],
        ),
        GeneratedTestOperation::Scheduler { .. } => types.ty().function(
            [
                EncodedValType::I32,
                EncodedValType::F64,
                EncodedValType::I64,
            ],
            [EncodedValType::I64],
        ),
    }
    types
        .ty()
        .function([EncodedValType::I64], std::iter::empty());
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);
    if matches!(
        operation,
        GeneratedTestOperation::DatabaseWrite(GeneratedDatabaseWriteTestOperation {
            import_database_get: true,
            ..
        })
    ) {
        types.ty().function(
            [EncodedValType::I32, EncodedValType::I64],
            [EncodedValType::I64],
        );
    }

    let operation_import = match operation {
        GeneratedTestOperation::DatabaseGet => "convex_db_get",
        GeneratedTestOperation::DatabaseWrite(_) => "convex_db_write",
        GeneratedTestOperation::FunctionHandleCreate(_) => "convex_function_handle_create",
        GeneratedTestOperation::Scheduler { .. } => "convex_scheduler_schedule",
    };
    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_request_field",
        EncodedEntityType::Function(0),
    );
    imports.import("convex", operation_import, EncodedEntityType::Function(1));
    let mut imported_function_count = 2;
    if matches!(
        operation,
        GeneratedTestOperation::DatabaseWrite(GeneratedDatabaseWriteTestOperation {
            import_database_get: true,
            ..
        })
    ) {
        imports.import("convex", "convex_db_get", EncodedEntityType::Function(5));
        imported_function_count += 1;
    }
    let value_release_import = if matches!(
        operation,
        GeneratedTestOperation::DatabaseWrite(GeneratedDatabaseWriteTestOperation {
            handle_fault: GeneratedDatabaseWriteHandleFault::StaleValue,
            ..
        }) | GeneratedTestOperation::FunctionHandleCreate(GeneratedFunctionHandleTestOperation {
            stale_reference: true,
            ..
        })
    ) {
        let index = imported_function_count;
        imports.import(
            "convex",
            "convex_value_release",
            EncodedEntityType::Function(2),
        );
        imported_function_count += 1;
        Some(index)
    } else {
        None
    };
    let function_result_import = imported_function_count;
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(2),
    );
    imported_function_count += 1;

    let mut functions = EncodedFunctionSection::new();
    functions.function(3);
    functions.function(4);
    functions.function(3);
    functions.function(4);

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

    let field_name = match operation {
        GeneratedTestOperation::DatabaseGet => "id",
        GeneratedTestOperation::DatabaseWrite(_) => "idvalueresult",
        GeneratedTestOperation::FunctionHandleCreate(_) => "functionAddress",
        GeneratedTestOperation::Scheduler { .. } => "args",
    };
    let request_field = |run: &mut EncodedFunction, pointer: i32, length: i32| {
        run.instruction(&EncodedInstruction::I32Const(pointer));
        run.instruction(&EncodedInstruction::I32Const(length));
        run.instruction(&EncodedInstruction::Call(0));
    };
    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([(2, EncodedValType::I64)]);
    if let GeneratedTestOperation::FunctionHandleCreate(create) = operation {
        request_field(
            &mut run,
            0,
            i32::try_from(field_name.len()).expect("test field length"),
        );
        run.instruction(&EncodedInstruction::LocalSet(0));
        if create.stale_reference {
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::Call(
                value_release_import.expect("stale-reference test import missing"),
            ));
        }
        run.instruction(&EncodedInstruction::I32Const(create.operation_id));
        run.instruction(&EncodedInstruction::LocalGet(0));
    } else if let GeneratedTestOperation::DatabaseWrite(write) = operation {
        if write.handle_fault == GeneratedDatabaseWriteHandleFault::StaleValue {
            request_field(&mut run, 2, 5);
            run.instruction(&EncodedInstruction::LocalSet(0));
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::Call(
                value_release_import.expect("stale-value test import missing"),
            ));
        }
        run.instruction(&EncodedInstruction::I32Const(write.operation_id));
        if write.kind == GeneratedDatabaseWriteKind::Insert {
            run.instruction(&EncodedInstruction::I64Const(0));
        } else if write.handle_fault == GeneratedDatabaseWriteHandleFault::WrongIdShape {
            request_field(&mut run, 2, 5);
        } else {
            request_field(&mut run, 0, 2);
        }
        if write.kind == GeneratedDatabaseWriteKind::Delete {
            run.instruction(&EncodedInstruction::I64Const(0));
        } else if write.handle_fault == GeneratedDatabaseWriteHandleFault::StaleValue {
            run.instruction(&EncodedInstruction::LocalGet(0));
        } else {
            request_field(&mut run, 2, 5);
        }
    } else {
        run.instruction(&EncodedInstruction::I32Const(1));
        if let GeneratedTestOperation::Scheduler { time_milliseconds } = operation {
            run.instruction(&EncodedInstruction::F64Const(time_milliseconds.into()));
        }
        run.instruction(&EncodedInstruction::I32Const(0));
        run.instruction(&EncodedInstruction::I32Const(
            i32::try_from(field_name.len()).expect("test field name length overflow"),
        ));
        run.instruction(&EncodedInstruction::Call(0));
    }
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::LocalSet(1));
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::I64Const(-1));
    run.instruction(&EncodedInstruction::I64Eq);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    if matches!(
        operation,
        GeneratedTestOperation::DatabaseWrite(GeneratedDatabaseWriteTestOperation {
            kind: GeneratedDatabaseWriteKind::Patch
                | GeneratedDatabaseWriteKind::Replace
                | GeneratedDatabaseWriteKind::Delete,
            ..
        })
    ) {
        request_field(&mut run, 7, 6);
    } else {
        run.instruction(&EncodedInstruction::LocalGet(1));
    }
    run.instruction(&EncodedInstruction::Call(function_result_import));
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
    data.active(0, &EncodedConstExpr::i32_const(0), field_name.bytes());

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

#[cfg(test)]
pub(in super::super) fn generated_host_secret_verify_test_module(
    operation_id: i32,
    pointer: i32,
    length: i32,
    candidate: &[u8],
) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [
            EncodedValType::I32,
            EncodedValType::I32,
            EncodedValType::I32,
        ],
        [EncodedValType::I32],
    );
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

    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_host_secret_verify",
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_request_field",
        EncodedEntityType::Function(1),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(2),
    );

    let mut functions = EncodedFunctionSection::new();
    functions.function(3);
    functions.function(4);
    functions.function(3);
    functions.function(4);

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
    exports.export("_initialize", EncodedExportKind::Func, 3);
    exports.export("convex_wasm_udf_run", EncodedExportKind::Func, 4);
    exports.export(
        "convex_wasm_udf_destroy_runtime",
        EncodedExportKind::Func,
        5,
    );
    exports.export(
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        6,
    );

    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([(1, EncodedValType::I32)]);
    run.instruction(&EncodedInstruction::I32Const(operation_id));
    run.instruction(&EncodedInstruction::I32Const(pointer));
    run.instruction(&EncodedInstruction::I32Const(length));
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::LocalSet(0));
    for (status, field_pointer, field_length) in [(-1, 0, 7), (0, 8, 8)] {
        run.instruction(&EncodedInstruction::LocalGet(0));
        run.instruction(&EncodedInstruction::I32Const(status));
        run.instruction(&EncodedInstruction::I32Eq);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::I32Const(field_pointer));
        run.instruction(&EncodedInstruction::I32Const(field_length));
        run.instruction(&EncodedInstruction::Call(1));
        run.instruction(&EncodedInstruction::Call(2));
        run.instruction(&EncodedInstruction::I32Const(0));
        run.instruction(&EncodedInstruction::Return);
        run.instruction(&EncodedInstruction::End);
    }
    run.instruction(&EncodedInstruction::I32Const(16));
    run.instruction(&EncodedInstruction::I32Const(5));
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::Call(2));
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
    data.active(
        0,
        &EncodedConstExpr::i32_const(0),
        "missing\0mismatchmatch".bytes(),
    );
    data.active(
        0,
        &EncodedConstExpr::i32_const(64),
        candidate.iter().copied(),
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

#[cfg(test)]
pub(in super::super) fn generated_database_normalize_id_test_module(
    operation_id: i32,
    stale_operand: bool,
    retain_operand_for_reuse: bool,
) -> Vec<u8> {
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
    imports.import(
        "convex",
        "convex_db_normalize_id",
        EncodedEntityType::Function(1),
    );
    let mut imported_function_count = 2;
    let release_import = stale_operand.then(|| {
        let index = imported_function_count;
        imports.import(
            "convex",
            "convex_value_release",
            EncodedEntityType::Function(2),
        );
        imported_function_count += 1;
        index
    });
    let function_result_import = imported_function_count;
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(2),
    );
    imported_function_count += 1;

    let mut functions = EncodedFunctionSection::new();
    functions.function(3);
    functions.function(4);
    functions.function(3);
    functions.function(4);
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
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        imported_function_count + 3,
    );

    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([(2, EncodedValType::I64)]);
    if retain_operand_for_reuse {
        run.instruction(&EncodedInstruction::GlobalGet(0));
        run.instruction(&EncodedInstruction::I64Eqz);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::I32Const(0));
        run.instruction(&EncodedInstruction::I32Const(2));
        run.instruction(&EncodedInstruction::Call(0));
        run.instruction(&EncodedInstruction::LocalSet(0));
        run.instruction(&EncodedInstruction::LocalGet(0));
        run.instruction(&EncodedInstruction::GlobalSet(0));
        run.instruction(&EncodedInstruction::Else);
        run.instruction(&EncodedInstruction::GlobalGet(0));
        run.instruction(&EncodedInstruction::LocalSet(0));
        run.instruction(&EncodedInstruction::End);
    } else {
        run.instruction(&EncodedInstruction::I32Const(0));
        run.instruction(&EncodedInstruction::I32Const(2));
        run.instruction(&EncodedInstruction::Call(0));
        run.instruction(&EncodedInstruction::LocalSet(0));
    }
    if stale_operand {
        run.instruction(&EncodedInstruction::LocalGet(0));
        run.instruction(&EncodedInstruction::Call(
            release_import.expect("stale normalizeId operand release import missing"),
        ));
    }
    run.instruction(&EncodedInstruction::I32Const(operation_id));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::LocalSet(1));
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::I64Const(-1));
    run.instruction(&EncodedInstruction::I64Eq);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::Call(function_result_import));
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
        .section(&globals)
        .section(&exports)
        .section(&code)
        .section(&data);
    module.finish()
}

#[cfg(test)]
pub(in super::super) fn generated_sequential_query_collect_test_module() -> Vec<u8> {
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
        .function([EncodedValType::I64], [EncodedValType::I64]);
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    types.ty().function(
        [EncodedValType::I64, EncodedValType::I64],
        std::iter::empty(),
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
    imports.import(
        "convex",
        "convex_request_field",
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_query_start_value",
        EncodedEntityType::Function(1),
    );
    imports.import(
        "convex",
        "convex_query_next",
        EncodedEntityType::Function(2),
    );
    imports.import(
        "convex",
        "convex_value_array_new",
        EncodedEntityType::Function(3),
    );
    imports.import(
        "convex",
        "convex_value_array_push",
        EncodedEntityType::Function(4),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(5),
    );
    let imported_function_count = 6;

    let mut functions = EncodedFunctionSection::new();
    functions.function(6);
    functions.function(7);
    functions.function(6);
    functions.function(8);
    functions.function(7);

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
    let mut run = EncodedFunction::new([(3, EncodedValType::I64)]);
    run.instruction(&EncodedInstruction::Call(3));
    run.instruction(&EncodedInstruction::LocalSet(1));
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::I32Const(5));
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::LocalSet(0));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::I64Const(-1));
    run.instruction(&EncodedInstruction::I64Eq);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    for _ in 0..2 {
        run.instruction(&EncodedInstruction::LocalGet(1));
        run.instruction(&EncodedInstruction::LocalGet(0));
        run.instruction(&EncodedInstruction::Call(2));
        run.instruction(&EncodedInstruction::LocalSet(2));
        run.instruction(&EncodedInstruction::LocalGet(2));
        run.instruction(&EncodedInstruction::I64Const(-1));
        run.instruction(&EncodedInstruction::I64Eq);
        run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
        run.instruction(&EncodedInstruction::I32Const(0));
        run.instruction(&EncodedInstruction::Return);
        run.instruction(&EncodedInstruction::End);
        run.instruction(&EncodedInstruction::LocalGet(2));
        run.instruction(&EncodedInstruction::Call(4));
    }
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(2));
    run.instruction(&EncodedInstruction::I64Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::Else);
    run.instruction(&EncodedInstruction::Unreachable);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::LocalGet(1));
    run.instruction(&EncodedInstruction::Call(5));
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
    data.active(0, &EncodedConstExpr::i32_const(0), "value".bytes());

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

#[cfg(test)]
pub(in super::super) fn generated_async_batch_test_module(
    operation: GeneratedAsyncBatchTestOperation,
) -> Vec<u8> {
    assert!(
        operation.import_database_write || operation.later_database_write_operation_id.is_none()
    );
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::I64]);
    types.ty().function(
        [
            EncodedValType::I32,
            EncodedValType::I64,
            EncodedValType::I64,
        ],
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
    imports.import(
        "convex",
        "convex_request_field",
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_async_batch_take",
        EncodedEntityType::Function(1),
    );
    let mut imported_function_count = 2;
    let database_write_import = operation.import_database_write.then(|| {
        let index = imported_function_count;
        imports.import("convex", "convex_db_write", EncodedEntityType::Function(2));
        imported_function_count += 1;
        index
    });
    let function_result_import = imported_function_count;
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(3),
    );
    imported_function_count += 1;

    let mut functions = EncodedFunctionSection::new();
    functions.function(4);
    functions.function(5);
    functions.function(4);
    functions.function(6);
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

    let request_field = |run: &mut EncodedFunction, pointer: i32, length: i32| {
        run.instruction(&EncodedInstruction::I32Const(pointer));
        run.instruction(&EncodedInstruction::I32Const(length));
        run.instruction(&EncodedInstruction::Call(0));
    };
    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([(1, EncodedValType::I64)]);
    request_field(&mut run, 0, 5);
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::LocalSet(0));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::I64Const(-1));
    run.instruction(&EncodedInstruction::I64Eq);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    if let Some(operation_id) = operation.later_database_write_operation_id {
        run.instruction(&EncodedInstruction::I32Const(operation_id));
        request_field(&mut run, 5, 2);
        request_field(&mut run, 7, 5);
        run.instruction(&EncodedInstruction::Call(
            database_write_import.expect("database-write test import missing"),
        ));
        run.instruction(&EncodedInstruction::Drop);
    }
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(function_result_import));
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
    data.active(0, &EncodedConstExpr::i32_const(0), "batchidvalue".bytes());

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

#[cfg(test)]
pub(in super::super) fn generated_memory_test_module(
    operation: GeneratedMemoryTestOperation,
) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    types
        .ty()
        .function([EncodedValType::I64], std::iter::empty());
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);
    types
        .ty()
        .function([EncodedValType::I32], std::iter::empty());

    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_value_null_new",
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(1),
    );
    imports.import(
        "convex",
        "convex_profile_mark",
        EncodedEntityType::Function(4),
    );

    let mut functions = EncodedFunctionSection::new();
    functions.function(2);
    functions.function(3);
    functions.function(2);
    functions.function(3);

    let mut memory = EncodedMemorySection::new();
    memory.memory(EncodedMemoryType {
        minimum: 1,
        maximum: Some(operation.maximum_pages),
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

    let mut exports = EncodedExportSection::new();
    exports.export("memory", EncodedExportKind::Memory, 0);
    exports.export("_initialize", EncodedExportKind::Func, 3);
    exports.export("convex_wasm_udf_run", EncodedExportKind::Func, 4);
    exports.export(
        "convex_wasm_udf_destroy_runtime",
        EncodedExportKind::Func,
        5,
    );
    exports.export(
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        6,
    );

    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::I32Const(1));
    initialize.instruction(&EncodedInstruction::GlobalSet(0));
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([(1, EncodedValType::I64)]);
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Call(2));
    match operation.loop_iterations {
        Some(0) => {},
        Some(iterations) => {
            run.instruction(&EncodedInstruction::I64Const(
                i64::try_from(iterations).expect("test loop iteration count overflow"),
            ));
            run.instruction(&EncodedInstruction::LocalSet(0));
            run.instruction(&EncodedInstruction::Block(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::Loop(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::I64Eqz);
            run.instruction(&EncodedInstruction::BrIf(1));
            run.instruction(&EncodedInstruction::LocalGet(0));
            run.instruction(&EncodedInstruction::I64Const(1));
            run.instruction(&EncodedInstruction::I64Sub);
            run.instruction(&EncodedInstruction::LocalSet(0));
            run.instruction(&EncodedInstruction::Br(0));
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::End);
        },
        None => {
            run.instruction(&EncodedInstruction::Loop(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::Br(0));
            run.instruction(&EncodedInstruction::End);
        },
    }
    run.instruction(&EncodedInstruction::I32Const(
        i32::try_from(operation.additional_pages).expect("test memory growth overflow"),
    ));
    run.instruction(&EncodedInstruction::MemoryGrow(0));
    run.instruction(&EncodedInstruction::Drop);
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::End);
    let mut destroy = EncodedFunction::new([]);
    // Real guest teardown can depend on initialization having entered.
    // Reject pre-initialization destruction instead of hiding that ordering bug.
    destroy.instruction(&EncodedInstruction::GlobalGet(0));
    destroy.instruction(&EncodedInstruction::I32Eqz);
    destroy.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    destroy.instruction(&EncodedInstruction::Unreachable);
    destroy.instruction(&EncodedInstruction::End);
    if operation.destroy_infinite_loop {
        destroy.instruction(&EncodedInstruction::Loop(EncodedBlockType::Empty));
        destroy.instruction(&EncodedInstruction::Br(0));
        destroy.instruction(&EncodedInstruction::End);
    }
    destroy.instruction(&EncodedInstruction::End);
    let mut prepare = EncodedFunction::new([]);
    prepare.instruction(&EncodedInstruction::I32Const(0));
    prepare.instruction(&EncodedInstruction::End);

    let mut code = EncodedCodeSection::new();
    code.function(&initialize);
    code.function(&run);
    code.function(&destroy);
    code.function(&prepare);

    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&memory)
        .section(&globals)
        .section(&exports)
        .section(&code);
    module.finish()
}

#[cfg(test)]
pub(in super::super) fn generated_fixed_entry_prepare_test_module() -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    types
        .ty()
        .function([EncodedValType::I64], std::iter::empty());
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);

    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_value_null_new",
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(1),
    );

    let mut functions = EncodedFunctionSection::new();
    functions.function(2);
    functions.function(3);
    functions.function(2);
    functions.function(3);

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

    let mut exports = EncodedExportSection::new();
    exports.export("memory", EncodedExportKind::Memory, 0);
    exports.export("_initialize", EncodedExportKind::Func, 2);
    exports.export("convex_wasm_udf_run", EncodedExportKind::Func, 3);
    exports.export(
        "convex_wasm_udf_destroy_runtime",
        EncodedExportKind::Func,
        4,
    );
    exports.export(
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        5,
    );

    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([]);
    run.instruction(&EncodedInstruction::GlobalGet(0));
    run.instruction(&EncodedInstruction::I32Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::End);
    let mut destroy = EncodedFunction::new([]);
    destroy.instruction(&EncodedInstruction::End);
    let mut prepare = EncodedFunction::new([]);
    prepare.instruction(&EncodedInstruction::I32Const(1));
    prepare.instruction(&EncodedInstruction::GlobalSet(0));
    prepare.instruction(&EncodedInstruction::I32Const(0));
    prepare.instruction(&EncodedInstruction::End);

    let mut code = EncodedCodeSection::new();
    code.function(&initialize);
    code.function(&run);
    code.function(&destroy);
    code.function(&prepare);

    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&memory)
        .section(&globals)
        .section(&exports)
        .section(&code);
    module.finish()
}

#[cfg(test)]
pub(in super::super) fn generated_entry_prepare_environment_test_module(
    environment_requests: [&[u8]; 2],
    unauthorized_request: &[u8],
) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    types
        .ty()
        .function([EncodedValType::I64], std::iter::empty());
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);

    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        [EncodedValType::I64],
    );
    types.ty().function(
        [EncodedValType::I64, EncodedValType::I64],
        [EncodedValType::I64],
    );
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::I32]);

    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_value_null_new",
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(1),
    );
    imports.import(
        "convex",
        "convex_capability_request_decode",
        EncodedEntityType::Function(4),
    );
    imports.import(
        "convex",
        "convex_capability_sync_take",
        EncodedEntityType::Function(5),
    );
    imports.import(
        "convex",
        "convex_value_release",
        EncodedEntityType::Function(1),
    );
    imports.import(
        "convex",
        "convex_capability_request_release",
        EncodedEntityType::Function(1),
    );
    imports.import(
        "convex",
        "convex_capability_current",
        EncodedEntityType::Function(0),
    );

    let mut functions = EncodedFunctionSection::new();
    functions.function(2);
    functions.function(3);
    functions.function(2);
    functions.function(3);
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
            val_type: EncodedValType::I64,
            mutable: true,
            shared: false,
        },
        &EncodedConstExpr::i64_const(-1),
    );
    for _ in 0..2 {
        globals.global(
            EncodedGlobalType {
                val_type: EncodedValType::I32,
                mutable: true,
                shared: false,
            },
            &EncodedConstExpr::i32_const(0),
        );
    }

    let mut exports = EncodedExportSection::new();
    exports.export("memory", EncodedExportKind::Memory, 0);
    exports.export("_initialize", EncodedExportKind::Func, 7);
    exports.export("convex_wasm_udf_run", EncodedExportKind::Func, 8);
    exports.export(
        "convex_wasm_udf_destroy_runtime",
        EncodedExportKind::Func,
        9,
    );
    exports.export(
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        10,
    );
    exports.export("convex_wasm_select_entry", EncodedExportKind::Func, 11);

    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([]);
    run.instruction(&EncodedInstruction::GlobalGet(0));
    run.instruction(&EncodedInstruction::I64Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Result(
        EncodedValType::I32,
    )));
    run.instruction(&EncodedInstruction::GlobalGet(1));
    run.instruction(&EncodedInstruction::Else);
    run.instruction(&EncodedInstruction::GlobalGet(2));
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::I32Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(1));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::Call(6));
    run.instruction(&EncodedInstruction::I64Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::I32Const(2));
    run.instruction(&EncodedInstruction::Return);
    run.instruction(&EncodedInstruction::End);
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::End);
    let mut destroy = EncodedFunction::new([]);
    destroy.instruction(&EncodedInstruction::End);
    let append_environment_read =
        |prepare: &mut EncodedFunction, request_offset: i32, request_len: i32| {
            prepare.instruction(&EncodedInstruction::I64Const(0));
            prepare.instruction(&EncodedInstruction::I32Const(request_offset));
            prepare.instruction(&EncodedInstruction::I32Const(request_len));
            prepare.instruction(&EncodedInstruction::Call(2));
            prepare.instruction(&EncodedInstruction::Call(3));
            prepare.instruction(&EncodedInstruction::LocalTee(0));
            prepare.instruction(&EncodedInstruction::I64Const(0));
            prepare.instruction(&EncodedInstruction::I64LtS);
            prepare.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            prepare.instruction(&EncodedInstruction::I32Const(65));
            prepare.instruction(&EncodedInstruction::Return);
            prepare.instruction(&EncodedInstruction::End);
            prepare.instruction(&EncodedInstruction::LocalGet(0));
            prepare.instruction(&EncodedInstruction::Call(4));
        };
    let append_unauthorized_initialization_request =
        |prepare: &mut EncodedFunction, request_offset: i32, request_len: i32| {
            prepare.instruction(&EncodedInstruction::Call(6));
            prepare.instruction(&EncodedInstruction::I32Const(request_offset));
            prepare.instruction(&EncodedInstruction::I32Const(request_len));
            prepare.instruction(&EncodedInstruction::Call(2));
            prepare.instruction(&EncodedInstruction::LocalTee(0));
            prepare.instruction(&EncodedInstruction::Call(3));
            prepare.instruction(&EncodedInstruction::I64Const(-2));
            prepare.instruction(&EncodedInstruction::I64Ne);
            prepare.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            prepare.instruction(&EncodedInstruction::I32Const(66));
            prepare.instruction(&EncodedInstruction::Return);
            prepare.instruction(&EncodedInstruction::End);
            prepare.instruction(&EncodedInstruction::LocalGet(0));
            prepare.instruction(&EncodedInstruction::Call(5));
        };
    let mut prepare = EncodedFunction::new([(1, EncodedValType::I64)]);
    prepare.instruction(&EncodedInstruction::GlobalGet(0));
    prepare.instruction(&EncodedInstruction::I64Eqz);
    prepare.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    prepare.instruction(&EncodedInstruction::GlobalGet(1));
    prepare.instruction(&EncodedInstruction::I32Eqz);
    prepare.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    append_unauthorized_initialization_request(
        &mut prepare,
        (environment_requests[0].len() + environment_requests[1].len()) as i32,
        unauthorized_request.len() as i32,
    );
    append_environment_read(&mut prepare, 0, environment_requests[0].len() as i32);
    prepare.instruction(&EncodedInstruction::I32Const(1));
    prepare.instruction(&EncodedInstruction::GlobalSet(1));
    prepare.instruction(&EncodedInstruction::End);
    prepare.instruction(&EncodedInstruction::Else);
    prepare.instruction(&EncodedInstruction::GlobalGet(2));
    prepare.instruction(&EncodedInstruction::I32Eqz);
    prepare.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    append_unauthorized_initialization_request(
        &mut prepare,
        (environment_requests[0].len() + environment_requests[1].len()) as i32,
        unauthorized_request.len() as i32,
    );
    // The second selected entry overlaps the first entry's configuration read
    // before adding a new dependency in the already initialized Store.
    append_environment_read(&mut prepare, 0, environment_requests[0].len() as i32);
    append_environment_read(
        &mut prepare,
        environment_requests[0].len() as i32,
        environment_requests[1].len() as i32,
    );
    prepare.instruction(&EncodedInstruction::I32Const(1));
    prepare.instruction(&EncodedInstruction::GlobalSet(2));
    prepare.instruction(&EncodedInstruction::End);
    prepare.instruction(&EncodedInstruction::End);
    prepare.instruction(&EncodedInstruction::I32Const(0));
    prepare.instruction(&EncodedInstruction::End);
    let mut selector = EncodedFunction::new([]);
    selector.instruction(&EncodedInstruction::LocalGet(0));
    selector.instruction(&EncodedInstruction::I64Const(0));
    selector.instruction(&EncodedInstruction::I64Eq);
    selector.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    selector.instruction(&EncodedInstruction::I64Const(0));
    selector.instruction(&EncodedInstruction::GlobalSet(0));
    selector.instruction(&EncodedInstruction::I32Const(0));
    selector.instruction(&EncodedInstruction::Return);
    selector.instruction(&EncodedInstruction::End);
    selector.instruction(&EncodedInstruction::LocalGet(0));
    selector.instruction(&EncodedInstruction::I64Const(1));
    selector.instruction(&EncodedInstruction::I64Eq);
    selector.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    selector.instruction(&EncodedInstruction::I64Const(1));
    selector.instruction(&EncodedInstruction::GlobalSet(0));
    selector.instruction(&EncodedInstruction::I32Const(0));
    selector.instruction(&EncodedInstruction::Return);
    selector.instruction(&EncodedInstruction::End);
    selector.instruction(&EncodedInstruction::I32Const(7));
    selector.instruction(&EncodedInstruction::End);

    let mut code = EncodedCodeSection::new();
    code.function(&initialize);
    code.function(&run);
    code.function(&destroy);
    code.function(&prepare);
    code.function(&selector);

    let mut data = EncodedDataSection::new();
    data.active(
        0,
        &EncodedConstExpr::i32_const(0),
        environment_requests[0].iter().copied(),
    );
    data.active(
        0,
        &EncodedConstExpr::i32_const(environment_requests[0].len() as i32),
        environment_requests[1].iter().copied(),
    );
    data.active(
        0,
        &EncodedConstExpr::i32_const(
            (environment_requests[0].len() + environment_requests[1].len()) as i32,
        ),
        unauthorized_request.iter().copied(),
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

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(in super::super) enum GeneratedSelectedEntryDispatchFault {
    Success,
    HandlerTrap,
    FirstSelectorReject,
    FirstSelectorTrap,
    PreparationStatus(i32),
    PreparationTrap,
    PreparationTimeout,
    SecondSelectorReject,
    SecondSelectorTrap,
}

#[cfg(test)]
pub(in super::super) fn generated_selected_entry_dispatch_fault_test_module(
    fault: GeneratedSelectedEntryDispatchFault,
    entry_selector: u64,
) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
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
    imports.import(
        "convex",
        "convex_value_null_new",
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(1),
    );

    let mut functions = EncodedFunctionSection::new();
    functions.function(2);
    functions.function(3);
    functions.function(2);
    functions.function(3);
    functions.function(4);

    let mut memory = EncodedMemorySection::new();
    memory.memory(EncodedMemoryType {
        minimum: 1,
        maximum: Some(1),
        memory64: false,
        shared: false,
        page_size_log2: None,
    });
    let mut globals = EncodedGlobalSection::new();
    for _ in 0..2 {
        globals.global(
            EncodedGlobalType {
                val_type: EncodedValType::I32,
                mutable: true,
                shared: false,
            },
            &EncodedConstExpr::i32_const(0),
        );
    }

    let mut exports = EncodedExportSection::new();
    exports.export("memory", EncodedExportKind::Memory, 0);
    exports.export("_initialize", EncodedExportKind::Func, 2);
    exports.export("convex_wasm_udf_run", EncodedExportKind::Func, 3);
    exports.export(
        "convex_wasm_udf_destroy_runtime",
        EncodedExportKind::Func,
        4,
    );
    exports.export(
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        5,
    );
    exports.export("convex_wasm_select_entry", EncodedExportKind::Func, 6);

    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);

    let mut run = EncodedFunction::new([]);
    run.instruction(&EncodedInstruction::GlobalGet(0));
    run.instruction(&EncodedInstruction::I32Eqz);
    run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    run.instruction(&EncodedInstruction::Unreachable);
    run.instruction(&EncodedInstruction::End);
    if matches!(fault, GeneratedSelectedEntryDispatchFault::HandlerTrap) {
        run.instruction(&EncodedInstruction::Unreachable);
    }
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::Call(1));
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::End);

    let mut destroy = EncodedFunction::new([]);
    destroy.instruction(&EncodedInstruction::End);

    let mut prepare = EncodedFunction::new([]);
    match fault {
        GeneratedSelectedEntryDispatchFault::PreparationStatus(status) => {
            prepare.instruction(&EncodedInstruction::I32Const(status));
        },
        GeneratedSelectedEntryDispatchFault::PreparationTrap => {
            prepare.instruction(&EncodedInstruction::Unreachable);
        },
        GeneratedSelectedEntryDispatchFault::PreparationTimeout => {
            prepare.instruction(&EncodedInstruction::Loop(EncodedBlockType::Empty));
            prepare.instruction(&EncodedInstruction::Br(0));
            prepare.instruction(&EncodedInstruction::End);
            prepare.instruction(&EncodedInstruction::Unreachable);
        },
        GeneratedSelectedEntryDispatchFault::Success
        | GeneratedSelectedEntryDispatchFault::HandlerTrap
        | GeneratedSelectedEntryDispatchFault::FirstSelectorReject
        | GeneratedSelectedEntryDispatchFault::FirstSelectorTrap
        | GeneratedSelectedEntryDispatchFault::SecondSelectorReject
        | GeneratedSelectedEntryDispatchFault::SecondSelectorTrap => {
            prepare.instruction(&EncodedInstruction::I32Const(1));
            prepare.instruction(&EncodedInstruction::GlobalSet(0));
            prepare.instruction(&EncodedInstruction::I32Const(0));
        },
    }
    prepare.instruction(&EncodedInstruction::End);

    let mut selector = EncodedFunction::new([]);
    selector.instruction(&EncodedInstruction::LocalGet(0));
    selector.instruction(&EncodedInstruction::I64Const(entry_selector as i64));
    selector.instruction(&EncodedInstruction::I64Ne);
    selector.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    selector.instruction(&EncodedInstruction::Unreachable);
    selector.instruction(&EncodedInstruction::End);
    selector.instruction(&EncodedInstruction::GlobalGet(1));
    selector.instruction(&EncodedInstruction::I32Eqz);
    selector.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
    selector.instruction(&EncodedInstruction::I32Const(1));
    selector.instruction(&EncodedInstruction::GlobalSet(1));
    match fault {
        GeneratedSelectedEntryDispatchFault::FirstSelectorReject => {
            selector.instruction(&EncodedInstruction::I32Const(7));
            selector.instruction(&EncodedInstruction::Return);
        },
        GeneratedSelectedEntryDispatchFault::FirstSelectorTrap => {
            selector.instruction(&EncodedInstruction::Unreachable);
        },
        GeneratedSelectedEntryDispatchFault::Success
        | GeneratedSelectedEntryDispatchFault::HandlerTrap
        | GeneratedSelectedEntryDispatchFault::PreparationStatus(_)
        | GeneratedSelectedEntryDispatchFault::PreparationTrap
        | GeneratedSelectedEntryDispatchFault::PreparationTimeout
        | GeneratedSelectedEntryDispatchFault::SecondSelectorReject
        | GeneratedSelectedEntryDispatchFault::SecondSelectorTrap => {
            selector.instruction(&EncodedInstruction::I32Const(0));
            selector.instruction(&EncodedInstruction::Return);
        },
    }
    selector.instruction(&EncodedInstruction::End);
    match fault {
        GeneratedSelectedEntryDispatchFault::SecondSelectorReject => {
            selector.instruction(&EncodedInstruction::I32Const(9));
        },
        GeneratedSelectedEntryDispatchFault::SecondSelectorTrap => {
            selector.instruction(&EncodedInstruction::Unreachable);
        },
        GeneratedSelectedEntryDispatchFault::Success
        | GeneratedSelectedEntryDispatchFault::HandlerTrap
        | GeneratedSelectedEntryDispatchFault::FirstSelectorReject
        | GeneratedSelectedEntryDispatchFault::FirstSelectorTrap
        | GeneratedSelectedEntryDispatchFault::PreparationStatus(_)
        | GeneratedSelectedEntryDispatchFault::PreparationTrap
        | GeneratedSelectedEntryDispatchFault::PreparationTimeout => {
            selector.instruction(&EncodedInstruction::I32Const(0));
        },
    }
    selector.instruction(&EncodedInstruction::End);

    let mut code = EncodedCodeSection::new();
    code.function(&initialize);
    code.function(&run);
    code.function(&destroy);
    code.function(&prepare);
    code.function(&selector);

    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&memory)
        .section(&globals)
        .section(&exports)
        .section(&code);
    module.finish()
}

#[cfg(test)]
pub(in super::super) fn generated_test_manifest(
    udf_kind: ManifestUdfKind,
    operation: JsonValue,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_test_manifest_with_limits(
        udf_kind,
        vec![json!({
            "id": 1,
            "debugName": "generatedTestOperation",
            "operation": operation,
        })],
        1 << 20,
    )
}

#[cfg(test)]
pub(in super::super) fn generated_test_manifest_with_limits(
    udf_kind: ManifestUdfKind,
    imported_operations: Vec<JsonValue>,
    maximum_guest_memory_bytes: usize,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_test_manifest_with_limits_and_fuel(
        udf_kind,
        imported_operations,
        maximum_guest_memory_bytes,
        1_000_000,
    )
}

#[cfg(test)]
pub(in super::super) fn generated_test_manifest_with_limits_and_fuel(
    udf_kind: ManifestUdfKind,
    imported_operations: Vec<JsonValue>,
    maximum_guest_memory_bytes: usize,
    execution_fuel: u64,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_test_manifest_with_runtime_limits(
        udf_kind,
        imported_operations,
        maximum_guest_memory_bytes,
        execution_fuel,
        128,
    )
}

#[cfg(test)]
pub(in super::super) fn generated_test_manifest_with_timeout(
    manifest: &WasmUdfExecutionManifest,
    timeout_milliseconds: u64,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    let mut manifest = serde_json::to_value(manifest)?;
    manifest["limits"]["timeoutMilliseconds"] = json!(timeout_milliseconds);
    Ok(Arc::new(WasmUdfExecutionManifest::parse_for_runtime(
        &serde_json::to_vec(&manifest)?,
        &generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?,
    )?))
}

#[cfg(any(test, feature = "testing"))]
pub(in super::super) fn generated_test_manifest_with_runtime_limits(
    udf_kind: ManifestUdfKind,
    imported_operations: Vec<JsonValue>,
    maximum_guest_memory_bytes: usize,
    execution_fuel: u64,
    maximum_operation_count: u64,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_test_manifest_with_runtime_limits_and_schema(
        udf_kind,
        imported_operations,
        maximum_guest_memory_bytes,
        execution_fuel,
        maximum_operation_count,
        MANIFEST_SCHEMA_VERSION,
        None,
    )
}

#[cfg(test)]
pub(in super::super) fn generated_schema_five_test_manifest_with_runtime_limits(
    udf_kind: ManifestUdfKind,
    imported_operations: Vec<JsonValue>,
    maximum_guest_memory_bytes: usize,
    execution_fuel: u64,
    maximum_operation_count: u64,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_test_manifest_with_runtime_limits_and_schema(
        udf_kind,
        imported_operations,
        maximum_guest_memory_bytes,
        execution_fuel,
        maximum_operation_count,
        COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION,
        None,
    )
}

#[cfg(test)]
pub(in super::super) fn generated_guest_promise_test_manifest_with_runtime_limits(
    udf_kind: ManifestUdfKind,
    imported_operations: Vec<JsonValue>,
    maximum_guest_memory_bytes: usize,
    execution_fuel: u64,
    maximum_operation_count: u64,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_test_manifest_with_runtime_limits_and_schema(
        udf_kind,
        imported_operations,
        maximum_guest_memory_bytes,
        execution_fuel,
        maximum_operation_count,
        EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION,
        Some(EffectExecutionMode::GuestPromiseEventLoop),
    )
}

#[cfg(test)]
pub(in super::super) fn generated_native_capability_test_manifest(
    udf_kind: ManifestUdfKind,
    abi: &NativeCapabilityTestAbiReport,
    execution: &WasmUdfExecutionPolicy,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    let runtime = generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?;
    execution.validate_capability_entry(abi.opaque_value_abi_version, &runtime)?;

    let base = generated_guest_promise_test_manifest_with_runtime_limits(
        udf_kind,
        execution
            .imported_operations()
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?,
        usize::try_from(execution.limits().max_guest_memory_bytes())?,
        execution.limits().execution_fuel(),
        execution.limits().max_operation_count(),
    )?;
    let mut manifest = serde_json::to_value(base.as_ref())?;
    // The generated test manifest supplies source and artifact identities. The
    // execution fields must remain the authenticated producer policy because
    // they determine which imports the real module is required to expose.
    manifest["opaqueValueAbiVersion"] = json!(abi.opaque_value_abi_version);
    manifest["valueMode"] = serde_json::to_value(execution.value_mode())?;
    manifest["effectExecutionMode"] = serde_json::to_value(execution.effect_execution_mode())?;
    manifest["limits"] = serde_json::to_value(execution.limits())?;
    manifest["platformLimits"] = serde_json::to_value(execution.platform_limits())?;
    manifest["importedOperations"] = serde_json::to_value(execution.imported_operations())?;
    Ok(Arc::new(WasmUdfExecutionManifest::parse_for_runtime(
        &serde_json::to_vec(&manifest)?,
        &runtime,
    )?))
}

#[cfg(any(test, feature = "testing"))]
pub(in super::super) fn generated_test_manifest_with_runtime_limits_and_schema(
    udf_kind: ManifestUdfKind,
    imported_operations: Vec<JsonValue>,
    maximum_guest_memory_bytes: usize,
    execution_fuel: u64,
    maximum_operation_count: u64,
    manifest_schema_version: u32,
    effect_execution_mode: Option<EffectExecutionMode>,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    let udf_kind = match udf_kind {
        ManifestUdfKind::Query => "query",
        ManifestUdfKind::Mutation => "mutation",
    };
    let platform_limits = PlatformLimits::authoritative()?;
    let mut manifest = json!({
        "manifestSchemaVersion": manifest_schema_version,
        "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
        "source": {
            "resolvedGraphSha256":
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "exportSha256":
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "modulePath": "functions/generated_test.ts",
            "runtimeModulePath": "functions/generated_test.js",
            "exportName": "run",
            "udfKind": udf_kind,
        },
        "compiler": {
            "artifactPipelineSha256":
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "compilerRevision": "test-compiler",
            "loweringPipelineSha256":
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "sourcePipelineSha256":
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "staticHermesRevision": "test-static-hermes",
            "admittedLanguageVersion": 1,
        },
        "artifact": {
            "coreWasmSha256":
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "provenanceSha256":
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "serializedModuleSha256":
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "wasmtimeRevision": GENERATED_WASMTIME_REVISION,
            "targetTriple": GENERATED_TARGET_TRIPLE,
            "targetCpu": GENERATED_TARGET_CPU,
            "engineConfigurationSha256": GENERATED_ENGINE_CONFIGURATION_SHA256,
            "engineCompatibilitySha256": GENERATED_ENGINE_COMPATIBILITY_SHA256,
            "coreWasmBytes": 1,
            "provenanceBytes": 1,
            "serializedModuleBytes": 1,
        },
        "limits": {
            "maxGuestMemoryBytes": maximum_guest_memory_bytes,
            "maxHostOwnedBytes": 1 << 20,
            "maxValueHandles": 128,
            "maxOperationCount": maximum_operation_count,
            "maxResultBytes": 1 << 20,
            "executionFuel": execution_fuel,
            "timeoutMilliseconds": platform_limits.execution_time_ms,
        },
        "platformLimits": platform_limits,
        "importedOperations": imported_operations,
        "routing": {
            "decision": "wasm",
        },
    });
    if let Some(effect_execution_mode) = effect_execution_mode {
        manifest["effectExecutionMode"] = serde_json::to_value(effect_execution_mode)?;
    }
    if matches!(
        manifest_schema_version,
        COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION
    ) {
        manifest["route"] = json!({
            "exportName": "run",
            "kind": "convex-wasm-runtime-route-v1",
            "runtimeModulePath": "functions/generated_test.js",
            "udfKind": udf_kind,
        });
        let artifact = manifest["artifact"]
            .as_object_mut()
            .context("generated cohort test artifact must be an object")?;
        artifact.remove("provenanceBytes");
        artifact.remove("provenanceSha256");
        artifact.insert(
            "cohort".to_owned(),
            json!({
                "bucket": 0,
                "manifestSha256":
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "packageId":
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "partitionPolicy": {
                    "bucketCount": 16,
                    "hash": "sha256-route-id-domain-first-u32-be-modulo",
                    "kind": "convex-wasm-cohort-partition-v1",
                },
            }),
        );
        artifact.insert(
            "entryId".to_owned(),
            json!("1111111111111111111111111111111111111111111111111111111111111111"),
        );
        artifact.insert("entrySelectionIndex".to_owned(), json!(0));
        artifact.insert("entrySelectorId".to_owned(), json!("1111111111111111"));
        artifact.insert("entrySymbol".to_owned(), json!("sh_export_generated_test"));
    }
    Ok(Arc::new(WasmUdfExecutionManifest::parse_for_runtime(
        &serde_json::to_vec(&manifest)?,
        &generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?,
    )?))
}
