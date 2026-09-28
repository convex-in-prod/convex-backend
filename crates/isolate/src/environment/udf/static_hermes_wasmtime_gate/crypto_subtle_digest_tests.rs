use wasm_encoder::MemArg as EncodedMemArg;

use super::*;

const WASM_PAGE_BYTES: usize = 64 * 1024;

struct DigestFixture<'a> {
    input_pointer: i32,
    input_length: i32,
    output_pointer: i32,
    output_length: i32,
    initialized_input: &'a [u8],
    expected_digest: Option<[u8; SHA256_DIGEST_BYTES]>,
    memory_pages: u64,
    retain_capability_for_reuse: bool,
    call_count: u32,
}

fn digest_test_module(fixture: &DigestFixture<'_>) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    types.ty().function(
        [
            EncodedValType::I64,
            EncodedValType::I32,
            EncodedValType::I32,
            EncodedValType::I32,
            EncodedValType::I32,
        ],
        std::iter::empty(),
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
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::I32]);

    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_capability_current",
        EncodedEntityType::Function(0),
    );
    imports.import(
        "convex",
        "convex_crypto_subtle_digest_sha256",
        EncodedEntityType::Function(1),
    );
    imports.import(
        "convex",
        "convex_request_field",
        EncodedEntityType::Function(2),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(3),
    );

    let imported_function_count = 4;
    let mut functions = EncodedFunctionSection::new();
    functions.function(4);
    functions.function(5);
    functions.function(4);
    functions.function(6);
    functions.function(5);

    let mut memory = EncodedMemorySection::new();
    memory.memory(EncodedMemoryType {
        minimum: fixture.memory_pages,
        maximum: Some(fixture.memory_pages),
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
    let mut run = EncodedFunction::new([(1, EncodedValType::I64)]);
    for _ in 0..fixture.call_count {
        if fixture.retain_capability_for_reuse {
            run.instruction(&EncodedInstruction::GlobalGet(0));
            run.instruction(&EncodedInstruction::I64Eqz);
            run.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            run.instruction(&EncodedInstruction::Call(0));
            run.instruction(&EncodedInstruction::GlobalSet(0));
            run.instruction(&EncodedInstruction::End);
            run.instruction(&EncodedInstruction::GlobalGet(0));
        } else {
            run.instruction(&EncodedInstruction::Call(0));
        }
        run.instruction(&EncodedInstruction::I32Const(fixture.input_pointer));
        run.instruction(&EncodedInstruction::I32Const(fixture.input_length));
        run.instruction(&EncodedInstruction::I32Const(fixture.output_pointer));
        run.instruction(&EncodedInstruction::I32Const(fixture.output_length));
        run.instruction(&EncodedInstruction::Call(1));
    }
    if let Some(expected_digest) = fixture.expected_digest {
        for (offset, byte) in expected_digest.into_iter().enumerate() {
            run.instruction(&EncodedInstruction::I32Const(fixture.output_pointer));
            run.instruction(&EncodedInstruction::I32Load8U(EncodedMemArg {
                offset: u64::try_from(offset).expect("digest test offset overflow"),
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
    let result_field_pointer = i32::try_from(
        usize::try_from(fixture.memory_pages)
            .expect("digest test memory page count overflow")
            .checked_mul(WASM_PAGE_BYTES)
            .and_then(|bytes| bytes.checked_sub("result".len()))
            .expect("digest test memory is too small"),
    )
    .expect("digest test result field pointer exceeds Wasm i32");
    run.instruction(&EncodedInstruction::I32Const(result_field_pointer));
    run.instruction(&EncodedInstruction::I32Const(6));
    run.instruction(&EncodedInstruction::Call(2));
    run.instruction(&EncodedInstruction::LocalSet(0));
    run.instruction(&EncodedInstruction::LocalGet(0));
    run.instruction(&EncodedInstruction::Call(3));
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
    if !fixture.initialized_input.is_empty() {
        data.active(
            0,
            &EncodedConstExpr::i32_const(fixture.input_pointer),
            fixture.initialized_input.iter().copied(),
        );
    }
    data.active(
        0,
        &EncodedConstExpr::i32_const(result_field_pointer),
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

fn digest_contract_test_module(
    include_capability_current: bool,
    compatible_signature: bool,
) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I64]);
    if compatible_signature {
        types.ty().function(
            [
                EncodedValType::I64,
                EncodedValType::I32,
                EncodedValType::I32,
                EncodedValType::I32,
                EncodedValType::I32,
            ],
            std::iter::empty(),
        );
    } else {
        types.ty().function(
            [EncodedValType::I32, EncodedValType::I32],
            std::iter::empty(),
        );
    }
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
    if include_capability_current {
        imports.import(
            "convex",
            "convex_capability_current",
            EncodedEntityType::Function(0),
        );
    }
    imports.import(
        "convex",
        "convex_crypto_subtle_digest_sha256",
        EncodedEntityType::Function(1),
    );
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(2),
    );
    let imported_function_count = if include_capability_current { 3 } else { 2 };

    let mut functions = EncodedFunctionSection::new();
    functions.function(3);
    functions.function(4);
    functions.function(3);
    functions.function(5);
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
    let mut run = EncodedFunction::new([]);
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
    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&memory)
        .section(&exports)
        .section(&code);
    module.finish()
}

fn digest_manifest(
    maximum_guest_memory_bytes: usize,
    execution_fuel: u64,
    maximum_operation_count: u64,
) -> anyhow::Result<Arc<WasmUdfExecutionManifest>> {
    generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Query,
        Vec::new(),
        maximum_guest_memory_bytes,
        execution_fuel,
        maximum_operation_count,
    )
}

async fn execute_digest_fixture(
    rt: ProdRuntime,
    database: &Database<ProdRuntime>,
    fixture: &DigestFixture<'_>,
    execution_fuel: u64,
    maximum_operation_count: u64,
) -> anyhow::Result<GeneratedExecutionOutput<ProdRuntime>> {
    let maximum_guest_memory_bytes = usize::try_from(fixture.memory_pages)?
        .checked_mul(WASM_PAGE_BYTES)
        .context("digest test memory byte count overflow")?;
    let manifest = digest_manifest(
        maximum_guest_memory_bytes,
        execution_fuel,
        maximum_operation_count,
    )?;
    let mut routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        digest_test_module(fixture),
    )?;
    Arc::get_mut(&mut routed)
        .context("digest test routed module was unexpectedly shared")?
        .entry_selector = Some(0);
    let route_identity = routed.route_identity.clone();
    let mut state = generated_test_state(
        rt.clone(),
        database.begin_system().await?,
        manifest,
        json!({ "result": "ok" }),
        UdfType::Query,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        Some(GeneratedTestMemorySetup {
            controller: Arc::clone(&route_configuration()?.generated_memory_controller),
            route_identity,
            existing_slot: None,
            values: None,
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
    execute_generated(routed, state, None, false).await
}

fn assert_clean(output: &GeneratedExecutionOutput<ProdRuntime>) {
    assert_eq!(output.invocation.opaque_live_handles, 0);
    assert_eq!(output.invocation.opaque_current_bytes, 0);
    assert!(!output.invocation.observed_rng);
}

fn sha256_zero_bytes(length: usize) -> [u8; SHA256_DIGEST_BYTES] {
    const ZERO_BLOCK: [u8; 4 * 1024] = [0; 4 * 1024];
    let mut hasher = Sha256::new();
    for _ in 0..length / ZERO_BLOCK.len() {
        hasher.update(&ZERO_BLOCK);
    }
    hasher.update(&ZERO_BLOCK[..length % ZERO_BLOCK.len()]);
    hasher.finalize().into()
}

async fn run_digest_value_and_limit_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    for (input, expected) in [
        (
            b"".as_slice(),
            [
                0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
                0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
                0x78, 0x52, 0xb8, 0x55,
            ],
        ),
        (
            b"abc".as_slice(),
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ],
        ),
    ] {
        let fixture = DigestFixture {
            input_pointer: 64,
            input_length: i32::try_from(input.len())?,
            output_pointer: 128,
            output_length: i32::try_from(SHA256_DIGEST_BYTES)?,
            initialized_input: input,
            expected_digest: Some(expected),
            memory_pages: 1,
            retain_capability_for_reuse: false,
            call_count: 1,
        };
        let output = execute_digest_fixture(rt.clone(), &database, &fixture, 1_000_000, 1).await?;
        assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
        assert_eq!(output.invocation.function_result, Some(json!("ok")));
        assert_eq!(output.invocation.operation_count, 1);
        assert_clean(&output);
        drop(output.invocation.transaction);
    }

    let maximum_digest = sha256_zero_bytes(MAX_CRYPTO_SUBTLE_DIGEST_INPUT_BYTES);
    let maximum = DigestFixture {
        input_pointer: 0,
        input_length: i32::try_from(MAX_CRYPTO_SUBTLE_DIGEST_INPUT_BYTES)?,
        output_pointer: i32::try_from(MAX_CRYPTO_SUBTLE_DIGEST_INPUT_BYTES)?,
        output_length: i32::try_from(SHA256_DIGEST_BYTES)?,
        initialized_input: &[],
        expected_digest: Some(maximum_digest),
        memory_pages: 257,
        retain_capability_for_reuse: false,
        call_count: 1,
    };
    let output = execute_digest_fixture(rt.clone(), &database, &maximum, 1_000_000_000, 1).await?;
    assert_eq!(output.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(output.invocation.operation_count, 1);
    assert_clean(&output);
    drop(output.invocation.transaction);

    let over_limit = DigestFixture {
        input_pointer: 0,
        input_length: i32::try_from(MAX_CRYPTO_SUBTLE_DIGEST_INPUT_BYTES + 1)?,
        output_pointer: i32::try_from(MAX_CRYPTO_SUBTLE_DIGEST_INPUT_BYTES + 1)?,
        output_length: i32::try_from(SHA256_DIGEST_BYTES)?,
        initialized_input: &[],
        expected_digest: None,
        memory_pages: 257,
        retain_capability_for_reuse: false,
        call_count: 1,
    };
    let output =
        execute_digest_fixture(rt.clone(), &database, &over_limit, 1_000_000_000, 1).await?;
    assert_eq!(output.invocation.outcome, InvocationOutcome::SystemError);
    assert_eq!(output.invocation.operation_count, 0);
    assert_clean(&output);
    drop(output.invocation.transaction);

    for (fixture, expected_operation_count) in [
        (
            DigestFixture {
                input_pointer: 0,
                input_length: 0,
                output_pointer: 128,
                output_length: 31,
                initialized_input: &[],
                expected_digest: None,
                memory_pages: 1,
                retain_capability_for_reuse: false,
                call_count: 1,
            },
            0,
        ),
        (
            DigestFixture {
                input_pointer: 0,
                input_length: 0,
                output_pointer: 128,
                output_length: 33,
                initialized_input: &[],
                expected_digest: None,
                memory_pages: 1,
                retain_capability_for_reuse: false,
                call_count: 1,
            },
            0,
        ),
        (
            DigestFixture {
                input_pointer: 65_535,
                input_length: 2,
                output_pointer: 128,
                output_length: 32,
                initialized_input: &[],
                expected_digest: None,
                memory_pages: 1,
                retain_capability_for_reuse: false,
                call_count: 1,
            },
            1,
        ),
        (
            DigestFixture {
                input_pointer: 0,
                input_length: 0,
                output_pointer: 65_535,
                output_length: 32,
                initialized_input: &[],
                expected_digest: None,
                memory_pages: 1,
                retain_capability_for_reuse: false,
                call_count: 1,
            },
            1,
        ),
    ] {
        let output = execute_digest_fixture(rt.clone(), &database, &fixture, 1_000_000, 1).await?;
        assert_eq!(output.invocation.outcome, InvocationOutcome::SystemError);
        assert_eq!(output.invocation.operation_count, expected_operation_count);
        assert_clean(&output);
        drop(output.invocation.transaction);
    }

    let fuel_limited = DigestFixture {
        input_pointer: 0,
        input_length: 4_096,
        output_pointer: 8_192,
        output_length: 32,
        initialized_input: &[],
        expected_digest: None,
        memory_pages: 1,
        retain_capability_for_reuse: false,
        call_count: 1,
    };
    let output = execute_digest_fixture(rt.clone(), &database, &fuel_limited, 1_024, 1).await?;
    assert_eq!(output.invocation.outcome, InvocationOutcome::FuelExhausted);
    assert_eq!(output.invocation.operation_count, 1);
    assert_clean(&output);
    drop(output.invocation.transaction);

    let operation_limited = DigestFixture {
        input_pointer: 0,
        input_length: 0,
        output_pointer: 128,
        output_length: 32,
        initialized_input: &[],
        expected_digest: None,
        memory_pages: 1,
        retain_capability_for_reuse: false,
        call_count: 2,
    };
    let output = execute_digest_fixture(rt, &database, &operation_limited, 1_000_000, 1).await?;
    assert_eq!(output.invocation.outcome, InvocationOutcome::SystemError);
    assert_eq!(output.invocation.operation_count, 1);
    assert_clean(&output);
    drop(output.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

async fn run_digest_stale_capability_test(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let fixture = DigestFixture {
        input_pointer: 64,
        input_length: 3,
        output_pointer: 128,
        output_length: 32,
        initialized_input: b"abc",
        expected_digest: Some([
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ]),
        memory_pages: 1,
        retain_capability_for_reuse: true,
        call_count: 1,
    };
    let maximum_guest_memory_bytes = WASM_PAGE_BYTES;
    let manifest = digest_manifest(maximum_guest_memory_bytes, 1_000_000, 1)?;
    let mut routed = generated_test_routed_module_from_bytes(
        Arc::clone(&manifest),
        digest_test_module(&fixture),
    )?;
    Arc::get_mut(&mut routed)
        .context("digest reuse test routed module was unexpectedly shared")?
        .entry_selector = Some(0);
    let controller = Arc::clone(&route_configuration()?.generated_memory_controller);
    let route_identity = routed.route_identity.clone();
    let state_for = |transaction, existing_slot, values| {
        generated_test_state(
            rt.clone(),
            transaction,
            Arc::clone(&manifest),
            json!({ "result": "ok" }),
            UdfType::Query,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            CancellationSignal::new_for_test(),
            None,
            Arc::new(GateMetrics::default()),
            Some(GeneratedTestMemorySetup {
                controller: Arc::clone(&controller),
                route_identity: route_identity.clone(),
                existing_slot,
                values,
                maximum_guest_memory_bytes,
            }),
        )
    };

    let mut first_state = state_for(database.begin_system().await?, None, None).await?;
    arm_generated_timeout(
        rt.clone(),
        &mut first_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let mut first = execute_generated(Arc::clone(&routed), first_state, None, true).await?;
    assert_eq!(first.invocation.outcome, InvocationOutcome::Success);
    assert_eq!(first.invocation.operation_count, 1);
    assert!(first.invocation.capability_revoked);
    assert_clean(&first);
    let mut instance = first
        .reusable_instance
        .take()
        .context("digest test did not retain its first runtime")?;
    let completed = instance
        .store
        .data_mut()
        .generated
        .take()
        .context("digest test first invocation lost its completed state")?;
    assert_eq!(completed.operation_count, 1);
    assert!(completed.capability_bridge.is_revoked());
    let retained_values = completed.values;
    let slot_id = instance.memory_slot_id;
    first
        .memory_permit
        .take()
        .context("digest test first invocation lost its memory permit")?
        .finish(first.terminal_memory_outcome, true);
    drop(first.invocation.transaction);

    let mut second_state = state_for(
        database.begin_system().await?,
        Some(slot_id),
        Some(retained_values),
    )
    .await?;
    arm_generated_timeout(
        rt,
        &mut second_state,
        Duration::from_secs(5),
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    )?;
    let second = execute_generated(routed, second_state, Some(instance), true).await?;
    assert_eq!(second.invocation.outcome, InvocationOutcome::SystemError);
    assert_eq!(second.invocation.operation_count, 0);
    assert!(second.invocation.runtime_reuse_contaminated);
    assert_clean(&second);
    assert!(second.reusable_instance.is_none());
    assert!(second.memory_permit.is_none());
    drop(second.invocation.transaction);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn crypto_subtle_digest_import_contract_is_mode_and_capability_scoped() -> anyhow::Result<()> {
    let engine = shared_generated_engine()?;
    let guest_promise_manifest = digest_manifest(WASM_PAGE_BYTES, 1_000_000, 1)?;
    let valid = Module::new(&engine, digest_contract_test_module(true, true))?;
    validate_generated_module_contract(&valid, &guest_promise_manifest)?;

    let missing_capability = Module::new(&engine, digest_contract_test_module(false, true))?;
    let error = validate_generated_module_contract(&missing_capability, &guest_promise_manifest)
        .expect_err("digest import passed without the invocation capability import");
    assert!(error
        .to_string()
        .contains("require the current invocation-capability ABI"));

    let incompatible = Module::new(&engine, digest_contract_test_module(true, false))?;
    let error = validate_generated_module_contract(&incompatible, &guest_promise_manifest)
        .expect_err("digest import passed with an incompatible signature");
    assert!(error.to_string().contains("incompatible function type"));

    let blocking_manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), WASM_PAGE_BYTES)?;
    let error = validate_generated_module_contract(&valid, &blocking_manifest)
        .expect_err("digest import passed a legacy blocking manifest");
    assert!(error
        .to_string()
        .contains("not declared by its authenticated package members"));
    Ok(())
}

#[test]
fn crypto_subtle_digest_hashes_raw_bytes_and_enforces_limits() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_crypto_subtle_digest_values",
        run_digest_value_and_limit_tests(rt),
    )
}

#[test]
fn crypto_subtle_digest_rejects_a_stale_invocation_capability() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_crypto_subtle_digest_stale_capability",
        run_digest_stale_capability_test(rt),
    )
}
