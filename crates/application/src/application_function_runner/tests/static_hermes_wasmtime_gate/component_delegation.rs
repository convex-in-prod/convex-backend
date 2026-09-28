use common::{
    bootstrap_model::components::{
        definition::{
            ComponentDefinitionMetadata,
            ComponentDefinitionType,
            ComponentExport,
        },
        ComponentMetadata,
        ComponentState,
        ComponentType,
    },
    components::Reference,
};
use database::{
    COMPONENTS_TABLE,
    SCHEMAS_TABLE,
};
use model::components::config::{
    ComponentConfigModel,
    ComponentDefinitionConfigModel,
};

use super::*;

async fn install_component_fixture(
    database: &Database<ProdRuntime>,
    storage: Arc<dyn Storage>,
    directory: &Path,
) -> anyhow::Result<DeveloperDocumentId> {
    let mut tx = database.begin_system().await?;
    let root = ComponentConfigModel::new(&mut tx)
        .initialize_component_namespace(true)
        .await?;
    let child = ComponentConfigModel::new(&mut tx)
        .initialize_component_namespace(false)
        .await?;
    database.commit(tx, "component_fixture_namespaces").await?;
    database
        .load_indexes_into_memory(BTreeSet::from([SCHEMAS_TABLE.clone()]))
        .await?;
    let mut tx = database.begin_system().await?;
    for (id, is_root, source_name) in [(root, true, "root"), (child, false, "child")] {
        let component = ComponentId::new(is_root, id);
        let mut definition = ComponentDefinitionMetadata::default_root();
        if !is_root {
            definition.path = "child".parse()?;
            definition.definition_type = ComponentDefinitionType::ChildComponent {
                name: "child".parse()?,
            };
            definition.exports.insert(
                "functions".parse()?,
                ComponentExport::Branch(
                    ["read", "write"]
                        .into_iter()
                        .map(|name| {
                            Ok((
                                name.parse()?,
                                ComponentExport::Leaf(Reference::Function(
                                    format!("functions:{name}").parse()?,
                                )),
                            ))
                        })
                        .collect::<anyhow::Result<_>>()?,
                ),
            );
        }
        let (definition_id, _) = ComponentDefinitionConfigModel::new(&mut tx)
            .create_component_definition(definition)
            .await?;
        SystemMetadataModel::new_global(&mut tx)
            .insert_with_internal_id(
                &COMPONENTS_TABLE,
                id.internal_id(),
                ComponentMetadata {
                    definition_id,
                    component_type: if is_root {
                        ComponentType::App
                    } else {
                        ComponentType::ChildComponent {
                            parent: root,
                            name: "child".parse()?,
                            env: BTreeMap::new(),
                        }
                    },
                    state: ComponentState::Active,
                    http_prefix: None,
                }
                .try_into()?,
            )
            .await?;
        UdfConfigModel::new(&mut tx, component.into())
            .set(UdfConfig {
                server_version: semver::Version::new(1, 99, 0),
                import_phase_rng_seed: [0; 32],
                import_phase_unix_timestamp: UnixTimestamp::from_nanos(0),
            })
            .await?;
        let module = ModuleConfig {
            path: "functions.js".parse()?,
            source: ModuleSource::new(&std::fs::read_to_string(
                directory.join(format!("{source_name}.js")),
            )?),
            source_map: None,
            environment: ModuleEnvironment::Isolate,
            node_pool: None,
        };
        let (storage_key, sha256, package_size) =
            model::source_packages::upload_download::upload_package(
                BTreeMap::from([("functions.js".parse()?, &module)]),
                storage.clone(),
                None,
            )
            .await?;
        let package = SourcePackageModel::new(&mut tx, component.into())
            .put(SourcePackage {
                storage_key,
                sha256,
                package_size,
                runtime_content_sha256: None,
                native_resident: None,
                runtime_generation: None,
                external_deps_package_id: None,
                node_version: None,
                node_executor_pool_topology: Default::default(),
            })
            .await?;
        let functions = [("read", UdfType::Query), ("write", UdfType::Mutation)]
            .into_iter()
            .map(|(name, kind)| {
                AnalyzedFunction::new(
                    name.parse()?,
                    None,
                    kind,
                    Some(Visibility::Public),
                    ArgsValidator::Unvalidated,
                    ReturnsValidator::Unvalidated,
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        ModuleModel::new(&mut tx)
            .put(
                None,
                CanonicalizedComponentModulePath {
                    component,
                    module_path: "functions.js".parse()?,
                },
                module.source,
                package,
                None,
                Some(AnalyzedModule {
                    functions: functions.into(),
                    ..Default::default()
                }),
                ModuleEnvironment::Isolate,
                None,
            )
            .await?;
    }
    let mut model = UserFacingModel::new(&mut tx, ComponentId::Child(child).into());
    let missing_id = model.insert("documents".parse()?, obj!()?).await?;
    model.delete(missing_id).await?;
    database.commit(tx, "component_fixture_modules").await?;
    Ok(missing_id)
}

async fn invoke_component_caller(
    runner: &ApplicationFunctionRunner<ProdRuntime>,
    database: &Database<ProdRuntime>,
    udf_type: UdfType,
    arguments: JsonValue,
    mode: FunctionExecutionMode,
) -> anyhow::Result<udf::UdfOutcome> {
    let (name, kind) = match udf_type {
        UdfType::Query => (
            "read",
            DatabaseFunctionKind::Query(ActiveJavascriptClass::Protected),
        ),
        UdfType::Mutation => ("write", DatabaseFunctionKind::Mutation),
        UdfType::Init | UdfType::Action | UdfType::HttpAction => {
            anyhow::bail!("invalid fixture kind")
        },
    };
    let caller = FunctionCaller::Cron;
    let context = ExecutionContext::new(
        RequestContext::new_for_system_request(RequestId::new()),
        &caller,
    );
    let permit = runner
        .isolate_functions
        .acquire_permit(udf_type, SchedulerDependencyClass::Independent)
        .await?;
    let mut tx = database.begin_system().await?;
    let path_and_args = udf::validation::ValidatedPathAndArgs::new(
        caller.allowed_visibility(),
        &mut tx,
        PublicFunctionPath::Component(CanonicalizedComponentFunctionPath {
            component: ComponentPath::root(),
            udf_path: format!("functions:{name}").parse()?,
        }),
        SerializedArgs::from_args(vec![arguments])?,
        udf_type,
    )
    .await??;
    let (_, outcome) = runner
        .isolate_functions
        .execute_query_or_mutation_with_mode(
            tx,
            permit,
            path_and_args,
            kind,
            QueryJournal::new(),
            context,
            SchedulerDependencyClass::Independent,
            mode,
        )
        .await?;
    match outcome {
        udf::FunctionOutcome::Query(outcome) | udf::FunctionOutcome::Mutation(outcome) => {
            Ok(outcome)
        },
        udf::FunctionOutcome::Action(_) | udf::FunctionOutcome::HttpAction(_) => {
            anyhow::bail!("component fixture returned a non-database outcome")
        },
    }
}

#[test]
#[ignore = "requires CONVEX_WASM_COMPONENT_FIXTURE from the compiler component-delegation fixture"]
fn compiled_wasm_component_delegation() -> anyhow::Result<()> {
    cmd_util::env::config_test();
    let _lock = GATE_TEST_LOCK.lock().expect("gate test lock was poisoned");
    let directory = PathBuf::from(std::env::var("CONVEX_WASM_COMPONENT_FIXTURE")?);
    let thread =
        std::thread::Builder::new()
            .stack_size(8 << 20)
            .spawn(move || -> anyhow::Result<()> {
                let tokio = ProdRuntime::init_tokio()?;
                let rt = ProdRuntime::new(&tokio);
                rt.clone()
                    .block_on("compiled_component_delegation", async move {
                        run_compiled_component_delegation(rt, directory).await
                    })
            })?;
    match thread.join() {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn run_compiled_component_delegation(
    rt: ProdRuntime,
    directory: PathBuf,
) -> anyhow::Result<()> {
    let persistence = Arc::new(SqlitePersistence::new(":memory:")?);
    let database = new_database(rt.clone(), persistence.clone()).await?;
    initialize_application_system_tables(&database).await?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
    let missing_id = install_component_fixture(&database, storage.clone(), &directory).await?;
    let runner = new_runner(
        rt,
        database.clone(),
        persistence,
        storage,
        Arc::new(UnusedModuleLoader),
        QueryCache::new(1 << 20),
        KeyBroker::dev(),
    )
    .await?;
    let payload = json!({"count": {"$integer": "BwAAAAAAAAA="}, "bytes": {"$bytes": "AP8="}});
    for (selector, udf_type) in [
        (0, UdfType::Query),
        (1, UdfType::Mutation),
        (2, UdfType::Query),
        (3, UdfType::Mutation),
    ] {
        // The directory is an explicitly supplied local compiler fixture; its
        // precompiled module is trusted native code, not a deployment upload.
        let hooks = unsafe {
            StaticHermesGateTestHooks::new_compiled_component_caller(
                std::fs::read(directory.join("component.cwasm"))?,
                selector,
                udf_type,
            )?
        };
        let _hooks = install_static_hermes_gate_test_hooks(&hooks)?;
        let mut cases = vec![
            ("success", "catch", payload.clone()),
            ("structured", "catch", json!(null)),
            ("structured", "catch", json!(false)),
            ("structured", "catch", payload.clone()),
            ("structured", "rethrow", json!(null)),
            ("structured", "rethrow", json!(false)),
            ("structured", "rethrow", payload.clone()),
            ("structured", "edit", payload.clone()),
            ("ordinary", "catch", json!(null)),
            ("ordinary", "rethrow", json!(null)),
        ];
        if udf_type == UdfType::Mutation {
            cases.push(("attributed", "edit", payload.clone()));
            cases.push(("unhandled", "rethrow", payload.clone()));
            for mode in ["missing", "attributed"] {
                for handle in ["rethrow", "catch", "replace"] {
                    cases.push((mode, handle, payload.clone()));
                }
            }
        }
        let expected_invocations = cases.len();
        for (mode, handle, data) in cases {
            let case = format!("selector={selector}, mode={mode}, handle={handle}");
            eprintln!("component delegation: {case}");
            let args = json!({"query": udf_type == UdfType::Query, "mode": mode,
                        "handle": handle, "payload": data, "id": missing_id.to_string()});
            let v8 = invoke_component_caller(
                &runner,
                &database,
                udf_type,
                args.clone(),
                FunctionExecutionMode::V8WithHostOperationTrace,
            )
            .await?;
            let wasm = invoke_component_caller(
                &runner,
                &database,
                udf_type,
                args,
                FunctionExecutionMode::StaticHermesWasmWithHostOperationTrace,
            )
            .await?;
            assert_eq!(wasm.host_operation_trace, v8.host_operation_trace, "{case}");
            let expected_attribution = (matches!(mode, "missing" | "attributed")
                && matches!(handle, "rethrow" | "edit"))
            .then_some(udf::HostOperationErrorV1::NonexistentDocument {
                operation: udf::HostOperation::Patch,
                document_id: missing_id,
            });
            // These are the canonical attribution fields consumed by shadow
            // comparison; text-only equality would miss their loss.
            assert_eq!(
                v8.host_operation_error, expected_attribution,
                "V8: {case}: {:?}",
                v8.result
            );
            assert_eq!(
                wasm.host_operation_error, expected_attribution,
                "Wasm: {case}: {:?}",
                wasm.result
            );
            let should_succeed = mode == "success" || handle == "catch";
            assert_eq!(
                v8.result.is_ok(),
                should_succeed,
                "V8: {case}: {:?}",
                v8.result
            );
            assert_eq!(
                wasm.result.is_ok(),
                should_succeed,
                "Wasm: {case}: {:?}",
                wasm.result
            );
            match (v8.result, wasm.result) {
                (Ok(v8), Ok(wasm)) => {
                    let expected = if mode == "success" {
                        data
                    } else {
                        let structured = matches!(mode, "structured" | "attributed");
                        json!({"structured": structured, "data": if structured { data } else { json!(null) }})
                    };
                    assert_eq!(v8.json_value(), expected, "V8: {case}");
                    assert_eq!(wasm.json_value(), expected, "Wasm: {case}");
                },
                (Err(v8), Err(wasm)) => {
                    let expected =
                        if matches!(mode, "structured" | "attributed") && handle != "replace" {
                            let mut expected = data;
                            if handle == "edit" {
                                expected["count"] = json!({"$integer": "CAAAAAAAAAA="});
                            }
                            Some(value::ConvexValue::try_from(expected)?)
                        } else {
                            None
                        };
                    assert_eq!(v8.custom_data, expected, "V8: {case}");
                    assert_eq!(wasm.custom_data, expected, "Wasm: {case}");
                    assert_eq!(wasm.message, v8.message, "{case}");
                    if mode == "unhandled" {
                        assert_eq!(
                            v8.message.lines().next(),
                            Some("Uncaught Error: detached component rejection"),
                            "{case}"
                        );
                    }
                },
                _ => unreachable!("success assertions checked both outcomes"),
            }
        }
        let invocations = hooks.generated_invocations();
        assert_eq!(invocations.len(), expected_invocations);
        assert!(invocations.iter().all(|invocation| {
            invocation.capability_revoked
                && invocation.opaque_live_handles == 0
                && invocation.opaque_current_bytes == 0
                && !invocation.runtime_reuse_contaminated
        }));
        assert!(
            hooks.execution_errors().is_empty(),
            "{:?}",
            hooks.execution_errors()
        );
        cleanup_static_hermes_test_instances::<ProdRuntime>().await?;
    }
    database.shutdown().await?;
    Ok(())
}
