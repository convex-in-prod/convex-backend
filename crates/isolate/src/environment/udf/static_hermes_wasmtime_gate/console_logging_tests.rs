use super::*;

fn console_only_import_module() -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [
            EncodedValType::I64,
            EncodedValType::I32,
            EncodedValType::I32,
            EncodedValType::I32,
        ],
        [EncodedValType::I32],
    );
    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_console_message",
        EncodedEntityType::Function(0),
    );
    let mut module = EncodedModule::new();
    module.section(&types).section(&imports);
    module.finish()
}

fn console_host_call_module() -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [
            EncodedValType::I64,
            EncodedValType::I32,
            EncodedValType::I32,
            EncodedValType::I32,
        ],
        [EncodedValType::I32],
    );
    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_console_message",
        EncodedEntityType::Function(0),
    );
    let mut functions = EncodedFunctionSection::new();
    functions.function(0);
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
    exports.export("call_console", EncodedExportKind::Func, 1);
    let mut call_console = EncodedFunction::new([]);
    for local in 0..4 {
        call_console.instruction(&EncodedInstruction::LocalGet(local));
    }
    call_console.instruction(&EncodedInstruction::Call(0));
    call_console.instruction(&EncodedInstruction::End);
    let mut code = EncodedCodeSection::new();
    code.function(&call_console);
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

async fn console_host_harness(
    state: HostState<ProdRuntime>,
) -> anyhow::Result<(
    Store<HostState<ProdRuntime>>,
    Memory,
    TypedFunc<(i64, i32, i32, i32), i32>,
)> {
    let engine = shared_generated_engine()?;
    let module = Module::new(engine.as_ref(), console_host_call_module())?;
    let mut linker = Linker::new(engine.as_ref());
    add_generated_convex_imports(&mut linker).map_err(wasmtime_anyhow)?;
    let mut store = Store::new(engine.as_ref(), state);
    store.set_fuel(1_000_000).map_err(wasmtime_anyhow)?;
    store.epoch_deadline_trap();
    store.set_epoch_deadline(u64::MAX / 2);
    store.limiter(|state| {
        &mut state
            .generated
            .as_mut()
            .expect("console test Store lost its invocation state")
            .memory_limiter
    });
    let instance = linker
        .instantiate_async(&mut store, &module)
        .await
        .map_err(wasmtime_anyhow)?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .context("console test memory missing")?;
    let call_console = instance
        .get_typed_func::<(i64, i32, i32, i32), i32>(&mut store, "call_console")
        .map_err(wasmtime_anyhow)?;
    Ok((store, memory, call_console))
}

async fn call_console(
    store: &mut Store<HostState<ProdRuntime>>,
    memory: &Memory,
    call: &TypedFunc<(i64, i32, i32, i32), i32>,
    capability: i64,
    level: i32,
    messages: &[String],
) -> anyhow::Result<i32> {
    let payload = serde_json::to_vec(messages)?;
    memory.write(&mut *store, 0, &payload)?;
    call.call_async(store, (capability, level, 0, i32::try_from(payload.len())?))
        .await
        .map_err(wasmtime_anyhow)
}

async fn console_test_state(
    rt: ProdRuntime,
    database: &Database<ProdRuntime>,
) -> anyhow::Result<HostState<ProdRuntime>> {
    generated_test_state(
        rt,
        database.begin_system().await?,
        generated_guest_promise_test_manifest_with_runtime_limits(
            ManifestUdfKind::Mutation,
            Vec::new(),
            1 << 20,
            1_000_000,
            16,
        )?,
        JsonValue::Null,
        UdfType::Mutation,
        UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        CancellationSignal::new_for_test(),
        None,
        Arc::new(GateMetrics::default()),
        None,
    )
    .await
}

fn structured_line(line: &LogLine) -> &common::log_lines::LogLineStructured {
    let LogLine::Structured(line) = line else {
        panic!("console emitted a sub-function log line")
    };
    line
}

async fn run_console_logging_tests(rt: ProdRuntime) -> anyhow::Result<()> {
    let database = new_test_database(rt.clone()).await?;
    let state = console_test_state(rt.clone(), &database).await?;
    let capability = generated_state(&state)?.capability_bridge.handle()?;
    let (mut store, memory, call) = console_host_harness(state).await?;

    for (level, expected_level, message) in [
        (0, LogLevel::Debug, "debug"),
        (1, LogLevel::Error, "error"),
        (2, LogLevel::Info, "info"),
        (3, LogLevel::Log, "log"),
        (4, LogLevel::Warn, "warn"),
    ] {
        assert_eq!(
            call_console(
                &mut store,
                &memory,
                &call,
                capability,
                level,
                &[message.to_owned()],
            )
            .await?,
            0
        );
        let line = store.data_mut().provider.take_log_lines();
        let line = structured_line(&line[0]);
        assert_eq!(line.level, expected_level);
        assert_eq!(&*line.messages, &[message.to_owned()]);
        assert_ne!(
            line.timestamp,
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000)
        );
        assert!(!line.is_truncated);
    }

    assert_eq!(
        call_console(
            &mut store,
            &memory,
            &call,
            capability,
            3,
            &["x".repeat(common::log_lines::MAX_LOG_LINE_LENGTH + 1)],
        )
        .await?,
        0
    );
    let truncated = store.data_mut().provider.take_log_lines();
    let truncated = structured_line(&truncated[0]);
    assert!(truncated.is_truncated);
    assert_eq!(
        truncated.messages[0].len(),
        common::log_lines::MAX_LOG_LINE_LENGTH
    );

    for index in 0..(MAX_LOG_LINES + 4) {
        assert_eq!(
            call_console(
                &mut store,
                &memory,
                &call,
                capability,
                3,
                &[index.to_string()],
            )
            .await?,
            0
        );
    }
    let overflow = store.data_mut().provider.take_log_lines();
    assert_eq!(overflow.len(), MAX_LOG_LINES);
    let overflow_line = structured_line(&overflow[MAX_LOG_LINES - 1]);
    assert_eq!(overflow_line.level, LogLevel::Error);
    assert_eq!(
        &*overflow_line.messages,
        &[format!(
            "Log overflow (maximum {MAX_LOG_LINES}). Remaining log lines omitted."
        )]
    );

    assert_eq!(
        call.call_async(&mut store, (0, 3, i32::MAX, 1))
            .await
            .map_err(wasmtime_anyhow)?,
        -1
    );
    assert!(!generated_state(store.data())?.runtime_reuse_contaminated);
    assert!(store.data_mut().provider.take_log_lines().is_empty());

    let forged = capability
        .checked_add(1)
        .context("test capability overflow")?;
    assert_eq!(
        call.call_async(&mut store, (forged, 3, i32::MAX, 1))
            .await
            .map_err(wasmtime_anyhow)?,
        -1
    );
    assert!(generated_state(store.data())?.runtime_reuse_contaminated);
    assert!(store.data_mut().provider.take_log_lines().is_empty());
    drop(store);

    let mut revoked = console_test_state(rt.clone(), &database).await?;
    let revoked_capability = generated_state(&revoked)?.capability_bridge.handle()?;
    generated_state_mut(&mut revoked)?
        .capability_bridge
        .revoke()?;
    let (mut revoked_store, _, revoked_call) = console_host_harness(revoked).await?;
    assert_eq!(
        revoked_call
            .call_async(&mut revoked_store, (revoked_capability, 3, i32::MAX, 1))
            .await
            .map_err(wasmtime_anyhow)?,
        -1
    );
    assert!(generated_state(revoked_store.data())?.runtime_reuse_contaminated);
    assert!(revoked_store
        .data_mut()
        .provider
        .take_log_lines()
        .is_empty());
    drop(revoked_store);

    let prior_capability = capability;
    let next = console_test_state(rt, &database).await?;
    let (mut next_store, _, next_call) = console_host_harness(next).await?;
    assert_eq!(
        next_call
            .call_async(&mut next_store, (prior_capability, 3, i32::MAX, 1))
            .await
            .map_err(wasmtime_anyhow)?,
        -1
    );
    assert!(generated_state(next_store.data())?.runtime_reuse_contaminated);
    assert!(next_store.data_mut().provider.take_log_lines().is_empty());
    drop(next_store);

    database.shutdown().await?;
    Ok(())
}

#[test]
fn console_logging_preserves_log_lines_and_invocation_authority() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_console_logging",
        run_console_logging_tests(rt),
    )
}

#[test]
fn console_logging_import_requires_guest_promise_capability_authority() -> anyhow::Result<()> {
    let (parameters, results) = generated_convex_import_signature("convex_console_message")
        .context("console import signature missing")?;
    assert_eq!(parameters.len(), 4);
    assert!(parameters
        .iter()
        .zip([ValType::I64, ValType::I32, ValType::I32, ValType::I32])
        .all(|(actual, expected)| ValType::eq(actual, &expected)));
    assert_eq!(results.len(), 1);
    assert!(ValType::eq(&results[0], &ValType::I32));
    assert!(permitted_conditional_convex_imports(
        ValueMode::GuestNativeJson,
        EffectExecutionMode::GuestPromiseEventLoop,
        &[],
    )
    .contains("convex_console_message"));
    assert!(!permitted_conditional_convex_imports(
        ValueMode::GuestNativeJson,
        EffectExecutionMode::BlockingFiber,
        &[],
    )
    .contains("convex_console_message"));

    let manifest = generated_guest_promise_test_manifest_with_runtime_limits(
        ManifestUdfKind::Mutation,
        Vec::new(),
        1 << 20,
        1_000_000,
        16,
    )?;
    let module = Module::new(
        shared_generated_engine()?.as_ref(),
        console_only_import_module(),
    )?;
    let error = validate_generated_module_contract(&module, &manifest)
        .expect_err("console import passed without current capability authority");
    assert!(error.to_string().contains(
        "generated invocation-scoped imports require the current invocation-capability ABI"
    ));
    Ok(())
}
