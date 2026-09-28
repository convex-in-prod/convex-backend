use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GuestExit {
    InitializationTrap,
    SelectorTrap,
    SecondSelectorTrap,
    PreparationTrap,
    HandlerTrap,
    PreparationRejected,
    Success,
}

fn module(exit: GuestExit) -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types.ty().function([], [EncodedValType::I64]);
    types.ty().function([EncodedValType::I64], []);
    types.ty().function([EncodedValType::I32], []);
    types.ty().function([], []);
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::I32]);
    types.ty().function([], [EncodedValType::I32]);
    let mut imports = EncodedImportSection::new();
    for (name, signature) in [
        ("convex_value_null_new", 0),
        ("convex_function_result", 1),
        ("convex_profile_mark", 2),
    ] {
        imports.import("convex", name, EncodedEntityType::Function(signature));
    }
    let mut functions = EncodedFunctionSection::new();
    let mut exports = EncodedExportSection::new();
    for (index, (name, signature)) in [
        ("_initialize", 3),
        ("convex_wasm_select_entry", 4),
        ("convex_wasm_udf_prepare_selected_entry", 5),
        ("convex_wasm_udf_run", 5),
        ("convex_wasm_udf_destroy_runtime", 3),
        (NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_TRACE_EXPORT, 0),
    ]
    .into_iter()
    .enumerate()
    {
        functions.function(signature);
        exports.export(
            name,
            EncodedExportKind::Func,
            3 + u32::try_from(index).unwrap(),
        );
    }
    let mut memory = EncodedMemorySection::new();
    memory.memory(EncodedMemoryType {
        minimum: 1,
        maximum: Some(1),
        memory64: false,
        shared: false,
        page_size_log2: None,
    });
    exports.export("memory", EncodedExportKind::Memory, 0);
    let mut globals = EncodedGlobalSection::new();
    globals.global(
        EncodedGlobalType {
            val_type: EncodedValType::I32,
            mutable: true,
            shared: false,
        },
        &EncodedConstExpr::i32_const(0),
    );
    let mut code = EncodedCodeSection::new();
    for phase in [
        GuestExit::InitializationTrap,
        GuestExit::SelectorTrap,
        GuestExit::PreparationTrap,
        GuestExit::HandlerTrap,
    ] {
        let mut body = EncodedFunction::new([]);
        if exit == phase {
            if phase == GuestExit::HandlerTrap {
                // Abandon a real host-owned value for finalization to release.
                body.instruction(&EncodedInstruction::Call(0));
                body.instruction(&EncodedInstruction::Drop);
            }
            body.instruction(&EncodedInstruction::Unreachable);
        } else if phase == GuestExit::SelectorTrap && exit == GuestExit::SecondSelectorTrap {
            body.instruction(&EncodedInstruction::GlobalGet(0));
            body.instruction(&EncodedInstruction::If(EncodedBlockType::Empty));
            body.instruction(&EncodedInstruction::Unreachable);
            body.instruction(&EncodedInstruction::End);
            body.instruction(&EncodedInstruction::I32Const(1));
            body.instruction(&EncodedInstruction::GlobalSet(0));
        } else if phase == GuestExit::HandlerTrap {
            body.instruction(&EncodedInstruction::Call(0));
            body.instruction(&EncodedInstruction::Call(1));
        }
        if phase != GuestExit::InitializationTrap {
            let status =
                if phase == GuestExit::PreparationTrap && exit == GuestExit::PreparationRejected {
                    62
                } else {
                    0
                };
            body.instruction(&EncodedInstruction::I32Const(status));
        }
        body.instruction(&EncodedInstruction::End);
        code.function(&body);
    }
    // Count entry into both cleanup and diagnostic exports, even if either
    // were to fail later. A teardown-completion counter alone misses reentry.
    for is_trace in [false, true] {
        let mut body = EncodedFunction::new([]);
        body.instruction(&EncodedInstruction::I32Const(0));
        body.instruction(&EncodedInstruction::Call(2));
        if is_trace {
            body.instruction(&EncodedInstruction::I64Const(1));
        }
        body.instruction(&EncodedInstruction::End);
        code.function(&body);
    }
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

#[test]
fn generated_wasm_traps_discard_without_guest_reentry() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    rt.clone()
        .block_on("generated_wasm_trap_disposal", async move {
            let database = new_test_database(rt.clone()).await?;
            let manifest = generated_schema_five_test_manifest_with_runtime_limits(
                ManifestUdfKind::Query,
                Vec::new(),
                1 << 20,
                1_000_000,
                128,
            )?;
            for exit in [
                GuestExit::InitializationTrap,
                GuestExit::SelectorTrap,
                GuestExit::SecondSelectorTrap,
                GuestExit::PreparationTrap,
                GuestExit::HandlerTrap,
                GuestExit::PreparationRejected,
                GuestExit::Success,
            ] {
                let metrics = Arc::new(GateMetrics::default());
                let routed =
                    generated_test_routed_module_from_bytes(Arc::clone(&manifest), module(exit))?;
                let mut state = generated_test_state(
                    rt.clone(),
                    database.begin_system().await?,
                    Arc::clone(&manifest),
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
                    rt.clone(),
                    &mut state,
                    Duration::from_secs(5),
                    *DATABASE_UDF_SYSTEM_TIMEOUT,
                )?;
                let output =
                    execute_generated(routed, state, None, exit != GuestExit::Success).await?;
                let healthy = matches!(exit, GuestExit::Success | GuestExit::PreparationRejected);
                assert_eq!(
                    metrics.profile_marks.load(Ordering::SeqCst),
                    if healthy { 2 } else { 0 },
                    "{exit:?}"
                );
                assert_eq!(
                    metrics.teardowns.load(Ordering::SeqCst),
                    usize::from(healthy),
                    "{exit:?}"
                );
                assert_eq!(metrics.store_drops.load(Ordering::SeqCst), 1, "{exit:?}");
                assert!(output.reusable_instance.is_none(), "{exit:?}");
                assert!(output.memory_permit.is_none(), "{exit:?}");
                assert_eq!(
                    output.partial_initialization_trace,
                    healthy.then_some(1),
                    "{exit:?}"
                );
                assert_eq!(
                    output.invocation.outcome,
                    if exit == GuestExit::Success {
                        InvocationOutcome::Success
                    } else {
                        InvocationOutcome::SystemError
                    },
                    "{exit:?}"
                );
                assert_eq!(output.invocation.opaque_live_handles, 0, "{exit:?}");
                assert!(output.invocation.capability_revoked, "{exit:?}");
                drop(output.invocation.transaction);
            }
            database.shutdown().await
        })
}
