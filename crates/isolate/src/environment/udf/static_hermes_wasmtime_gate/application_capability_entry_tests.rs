use storage::Upload;

use super::*;

const PACKAGE_DIRECTORY_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_APPLICATION_CAPABILITY_PACKAGE_TEST_DIRECTORY";
const EXPECTATION_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_APPLICATION_CAPABILITY_PACKAGE_TEST_EXPECTATION";
const SINGLETON_QUERY_PACKAGE_DIRECTORY_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_SINGLETON_QUERY_PACKAGE_DIRECTORY";
const SINGLETON_QUERY_EXPECTATION_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_SINGLETON_QUERY_EXPECTATION";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApplicationCapabilityExpectation {
    package: RealCapabilityEntryPackageTestExpectation,
    invocation_npm_version: String,
    setup: ApplicationCapabilitySetup,
    sequence: Vec<ApplicationCapabilityInvocation>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SingletonQueryExpectation {
    package: RealCapabilityEntryPackageTestExpectation,
    invocation_npm_version: String,
    content_base64: String,
    content_type: String,
    storage_uuid: String,
    expected_result: JsonValue,
    expected_operation_count: u64,
    expected_observed_identity: bool,
    expected_observed_time: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApplicationCapabilitySetup {
    application_table: String,
    application_index: String,
    application_index_fields: Vec<String>,
    user_table: String,
    scheduled_function: ApplicationScheduledFunction,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApplicationScheduledFunction {
    module_path: String,
    module_environment: String,
    export_name: String,
    udf_kind: String,
    visibility: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApplicationCapabilityInvocation {
    export_name: String,
    udf_kind: ManifestUdfKind,
    unix_timestamp_milliseconds: i64,
    request: JsonValue,
    expected_result_fields: BTreeMap<String, JsonValue>,
    expected_dynamic_result_fields: Vec<String>,
    forbidden_result_fields: Vec<String>,
    expected_operation_count: u64,
    expected_write_count: usize,
    expected_scheduled_write_count: usize,
    expected_observed_identity: bool,
    expected_observed_time: bool,
    commit: bool,
}

struct ApplicationCapabilityRuntime {
    routed: Arc<GeneratedRoutedModule>,
    controller: Arc<GeneratedMemoryController>,
    memory_identity: FunctionMemoryIdentity,
    selectors: BTreeMap<String, u64>,
}

fn application_user_identity(subject: String) -> anyhow::Result<UserIdentity> {
    UserIdentity::from_proto_unchecked(pb::convex_identity::UserIdentity {
        subject: Some(subject.clone()),
        issuer: Some("https://application-capability.invalid".to_owned()),
        expiration: Some((SystemTime::now() + Duration::from_secs(3_600)).into()),
        attributes: Some(pb::convex_identity::UserIdentityAttributes {
            token_identifier: Some(format!("https://application-capability.invalid|{subject}")),
            issuer: Some("https://application-capability.invalid".to_owned()),
            subject: Some(subject),
            ..Default::default()
        }),
        original_token: Some("application-capability-test-token".to_owned()),
    })
}

async fn install_application_setup(
    database: &Database<ProdRuntime>,
    setup: &ApplicationCapabilitySetup,
) -> anyhow::Result<DeveloperDocumentId> {
    initialize_application_system_tables(database).await?;
    let table: TableName = setup.application_table.parse()?;
    let index_name = IndexName::new(
        table,
        IndexDescriptor::new(setup.application_index.clone())?,
    )?;
    let index_fields = setup
        .application_index_fields
        .iter()
        .map(|field| field.parse())
        .collect::<anyhow::Result<Vec<_>>>()?;
    let user_table: TableName = setup.user_table.parse()?;
    let module_path: CanonicalizedModulePath = setup.scheduled_function.module_path.parse()?;
    let udf_type = match setup.scheduled_function.udf_kind.as_str() {
        "query" => UdfType::Query,
        "mutation" => UdfType::Mutation,
        "action" => UdfType::Action,
        kind => anyhow::bail!("application capability setup has unsupported UDF kind {kind}"),
    };
    let visibility = match setup.scheduled_function.visibility.as_str() {
        "public" => Visibility::Public,
        "internal" => Visibility::Internal,
        visibility => {
            anyhow::bail!("application capability setup has unsupported visibility {visibility}")
        },
    };
    let module_environment: ModuleEnvironment =
        setup.scheduled_function.module_environment.parse()?;

    let mut transaction = database.begin_system().await?;
    IndexModel::new(&mut transaction)
        .add_application_index(
            TableNamespace::root_component(),
            IndexMetadata::new_enabled(index_name, IndexedFields::try_from(index_fields)?),
        )
        .await?;
    let user = SystemMetadataModel::new(&mut transaction, TableNamespace::root_component())
        .insert_metadata(
            &user_table,
            obj!("userId" => "application-capability-user", "role" => "user")?,
        )
        .await?;

    let source_package_id =
        SourcePackageModel::new(&mut transaction, TableNamespace::root_component())
            .put(SourcePackage {
                storage_key: ObjectKey::try_from("application-capability-test")?,
                sha256: Sha256Digest::from([0; 32]),
                runtime_content_sha256: None,
                runtime_generation: None,
                external_deps_package_id: None,
                package_size: PackageSize::default(),
                node_version: None,
                node_executor_pool_topology: Default::default(),
            })
            .await?;
    let analyzed_function = AnalyzedFunction::new(
        setup.scheduled_function.export_name.parse()?,
        None,
        udf_type,
        Some(visibility),
        ArgsValidator::Unvalidated,
        ReturnsValidator::Unvalidated,
    )?;
    let analyzed_module = AnalyzedModule {
        functions: vec![analyzed_function].into(),
        ..Default::default()
    };
    ModuleModel::new(&mut transaction)
        .put(
            None,
            CanonicalizedComponentModulePath {
                component: ComponentId::Root,
                module_path: module_path.clone(),
            },
            ModuleSource::from(""),
            source_package_id,
            None,
            Some(analyzed_module.clone()),
            module_environment,
            None,
        )
        .await?;
    FunctionHandlesModel::new(&mut transaction)
        .apply_config_diff(
            ComponentId::Root,
            Some(&BTreeMap::from([(module_path, analyzed_module)])),
        )
        .await?;
    database
        .commit_with_write_source(transaction, "application_capability_test_setup")
        .await?;
    Ok(user.developer_id)
}

fn load_application_runtime(
    package_directory: &Path,
    package: &RealCapabilityEntryPackageTestExpectation,
) -> anyhow::Result<ApplicationCapabilityRuntime> {
    let engine = shared_generated_engine()?;
    let engine_compatibility_sha256 = calculated_precompile_compatibility_sha256(&engine);
    let compatibility = generated_runtime_compatibility(&engine_compatibility_sha256)?;
    let controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: package.package_id.clone(),
        generation_sha256: package.package_id.clone(),
        package_key: package.package_id.clone(),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    anyhow::ensure!(
        !GENERATED_ROUTED_MODULES
            .lock()
            .contains_key(&route_identity),
        "application capability package was already cached"
    );

    let mut routed = None;
    let mut selectors = BTreeMap::new();
    for expected in &package.routes {
        let loaded_package = load_capability_entry_package_for_compatibility_test(
            package_directory,
            &package.package_id,
            &package.entry_id,
            &expected.route_id,
            &expected.entry_selector_id,
            &package.entry_path,
            &package.runtime_module_path,
            &expected.export_name,
            expected.udf_kind,
            &expected.visibility,
            &compatibility,
        )?;
        let selector = u64::from_str_radix(&expected.entry_selector_id, 16)
            .context("application capability selector is invalid")?;
        anyhow::ensure!(
            selectors
                .insert(expected.export_name.clone(), selector)
                .is_none(),
            "application capability expectation has duplicate exports"
        );
        let loaded = cache_generated_routed_module(
            &controller,
            loaded_package,
            route_identity.clone(),
            Arc::clone(&engine),
            None,
        )?;
        if let Some(first) = &routed {
            anyhow::ensure!(
                Arc::ptr_eq(first, &loaded),
                "application capability sibling routes did not share one cached module"
            );
        } else {
            routed = Some(loaded);
        }
    }
    let routed = routed.context("application capability expectation contains no routes")?;
    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &route_identity,
    );
    Ok(ApplicationCapabilityRuntime {
        routed,
        controller,
        memory_identity,
        selectors,
    })
}

#[allow(clippy::too_many_arguments)]
async fn application_invocation_state(
    rt: ProdRuntime,
    transaction: Transaction<ProdRuntime>,
    routed: &GeneratedRoutedModule,
    controller: Arc<GeneratedMemoryController>,
    request: JsonValue,
    udf_type: UdfType,
    udf_path: String,
    npm_version: Version,
    unix_timestamp: UnixTimestamp,
    metrics: Arc<GateMetrics>,
    existing_slot: Option<GeneratedSlotId>,
    values: Option<OpaqueValueTable>,
    initialize_runtime: bool,
) -> anyhow::Result<HostState<ProdRuntime>> {
    let mut state = new_state(rt, transaction, None, metrics, QueryJournal::new()).await?;
    state.provider.set_udf_type(udf_type);
    state.provider.set_invocation(
        ResolvedComponentFunctionPath {
            component: ComponentId::Root,
            udf_path: udf_path.parse()?,
            component_path: ComponentPath::root(),
        },
        generated_test_execution_context(),
        DeploymentMetadata {
            name: "application-capability-test".to_owned(),
            region: None,
            class: DeploymentClass::S16,
        },
        npm_version,
        unix_timestamp,
    );
    if initialize_runtime {
        state.provider.snoop_initialization_reads()?;
    }
    let timeout = state
        .timeout
        .as_mut()
        .context("application capability test timeout missing")?;
    state.provider.initialize_static_hermes(timeout).await?;
    let memory_permit = controller
        .admit(
            generated_memory_identity_for_package(
                &routed.manifest,
                &routed.package_identity,
                &routed.route_identity,
            ),
            existing_slot,
        )
        .await
        .map_err(|_| anyhow::anyhow!("application capability memory admission was rejected"))?;
    let observer = memory_permit.observer();
    let mut values = match values {
        Some(mut values) => {
            values.begin_invocation(observer)?;
            values
        },
        None => OpaqueValueTable::new_with_observer(
            usize::try_from(routed.manifest.limits().max_value_handles())?,
            usize::try_from(routed.manifest.limits().max_host_owned_bytes())?,
            Some(observer),
        ),
    };
    let maximum =
        usize::try_from(routed.manifest.platform_limits().argument_bytes)?.min(MAX_REQUEST_BYTES);
    let request_handle = values.insert(OpaqueValue::Bytes(GuestNativeValueCodec::encode(
        request, maximum,
    )?))?;
    let capability_bridge = InvocationCapabilityBridge::unissued();
    let performance_monotonic_start = state.provider.rt().monotonic_now();
    state.generated = Some(GeneratedInvocationState {
        manifest: Arc::clone(&routed.manifest),
        context_read_set_required: true,
        values,
        async_operations: AsyncOperationState::default(),
        capability_bridge,
        performance_monotonic_start,
        performance_runtime_available: false,
        runtime_reuse_contaminated: false,
        allows_caught_official_output_chunk_initialization_failure: false,
        discard_after_caught_initialization_failure: false,
        request_handle,
        operation_count: 0,
        terminal_failure: None,
        cancellation: CancellationSignal::new_for_test(),
        interrupt: Arc::new(GeneratedInterruptState::default()),
        memory_limiter: memory_permit.limiter(
            StoreLimitsBuilder::new()
                .memory_size(usize::try_from(
                    routed.manifest.limits().max_guest_memory_bytes(),
                )?)
                .instances(1)
                .memories(1)
                .trap_on_grow_failure(true)
                .build(),
        ),
        memory_permit: Some(memory_permit),
        host_secret_values: BTreeMap::new(),
    });
    Ok(state)
}

pub(super) fn application_result_values_equal(expected: &JsonValue, actual: &JsonValue) -> bool {
    match (expected, actual) {
        (JsonValue::Null, JsonValue::Null) => true,
        (JsonValue::Bool(expected), JsonValue::Bool(actual)) => expected == actual,
        (JsonValue::Number(expected), JsonValue::Number(actual)) => {
            match (expected.as_f64(), actual.as_f64()) {
                (Some(expected), Some(actual)) => {
                    expected.is_finite() && actual.is_finite() && expected == actual
                },
                _ => false,
            }
        },
        (JsonValue::String(expected), JsonValue::String(actual)) => expected == actual,
        (JsonValue::Array(expected), JsonValue::Array(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| application_result_values_equal(expected, actual))
        },
        (JsonValue::Object(expected), JsonValue::Object(actual)) => {
            expected.len() == actual.len()
                && expected.iter().all(|(field, expected)| {
                    actual
                        .get(field)
                        .is_some_and(|actual| application_result_values_equal(expected, actual))
                })
        },
        _ => false,
    }
}

#[test]
fn application_result_comparison_uses_javascript_number_semantics() {
    assert!(application_result_values_equal(
        &json!({"nested": [1, {"timestamp": 1_700_000_000_000_u64}]}),
        &json!({"nested": [1.0, {"timestamp": 1_700_000_000_000.0_f64}]})
    ));
    assert!(application_result_values_equal(&json!(-0.0), &json!(0)));
    assert!(!application_result_values_equal(&json!(1), &json!(1.5)));
    assert!(!application_result_values_equal(
        &json!([1, 2]),
        &json!([2, 1])
    ));
    assert!(!application_result_values_equal(
        &json!({"value": true}),
        &json!({"value": true, "extra": null})
    ));

    let nan = ConvexValue::from(f64::NAN).to_internal_json();
    let infinity = ConvexValue::from(f64::INFINITY).to_internal_json();
    assert!(application_result_values_equal(&nan, &nan));
    assert!(!application_result_values_equal(&nan, &infinity));
}

fn assert_application_result(
    execution_context: &str,
    sequence_index: usize,
    invocation: &ApplicationCapabilityInvocation,
    output: &GeneratedExecutionOutput<ProdRuntime>,
) -> anyhow::Result<()> {
    let developer_error = match &output.invocation.outcome {
        InvocationOutcome::DeveloperError(message) => Some(message.as_str()),
        _ => None,
    };
    anyhow::ensure!(
        output.invocation.outcome == InvocationOutcome::Success,
        "application capability {execution_context} sequence index {sequence_index} export {} \
         failed: outcome {:?}, function result {:?}, developer error {:?}, first developer error \
         operation {:?}, operation count {}, syscall trace {:?}, system error {:?}",
        invocation.export_name,
        output.invocation.outcome,
        output.invocation.function_result,
        developer_error,
        output.invocation.first_developer_error_index,
        output.invocation.operation_count,
        output.invocation.syscall_trace,
        output.system_error
    );
    anyhow::ensure!(
        output.invocation.operation_count == invocation.expected_operation_count,
        "application capability invocation operation count changed"
    );
    anyhow::ensure!(
        output.invocation.observed_identity == invocation.expected_observed_identity,
        "application capability invocation identity observation changed"
    );
    anyhow::ensure!(
        output.invocation.observed_time == invocation.expected_observed_time,
        "application capability invocation time observation changed"
    );
    anyhow::ensure!(
        output.invocation.opaque_live_handles == 0
            && output.invocation.opaque_current_bytes == 0
            && output.invocation.capability_revoked
            && !output.invocation.runtime_reuse_contaminated,
        "application capability invocation did not reach the reusable lifecycle boundary"
    );
    anyhow::ensure!(
        output
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes
            == invocation.expected_write_count,
        "application capability invocation database write count changed"
    );
    anyhow::ensure!(
        output
            .invocation
            .transaction
            .execution_size()
            .scheduled_size
            .num_writes
            == invocation.expected_scheduled_write_count,
        "application capability invocation scheduler write count changed"
    );
    let result = output
        .invocation
        .function_result
        .as_ref()
        .context("application capability invocation did not publish a result")?;
    if invocation.expected_result_fields.is_empty() {
        anyhow::ensure!(
            result == &JsonValue::Null,
            "application capability invocation did not return the expected null result"
        );
    } else {
        let result_object = result
            .as_object()
            .context("application capability invocation result is not an object")?;
        anyhow::ensure!(
            result_object.len()
                == invocation.expected_result_fields.len()
                    + invocation.expected_dynamic_result_fields.len(),
            "application capability invocation result field set changed"
        );
        for (field, expected) in &invocation.expected_result_fields {
            let actual = result_object.get(field);
            anyhow::ensure!(
                actual.is_some_and(|actual| application_result_values_equal(expected, actual)),
                "application capability result field {field} changed: expected {expected:?}, \
                 actual {actual:?}"
            );
        }
        for field in &invocation.expected_dynamic_result_fields {
            anyhow::ensure!(
                result_object.get(field).is_some_and(JsonValue::is_number),
                "application capability dynamic result field {field} is missing or not numeric"
            );
        }
        for field in &invocation.forbidden_result_fields {
            anyhow::ensure!(
                !result_object.contains_key(field),
                "application capability result unexpectedly contains {field}"
            );
        }
    }
    Ok(())
}

async fn run_application_capability_sequence(
    rt: ProdRuntime,
    package_directory: PathBuf,
    expectation: ApplicationCapabilityExpectation,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        expectation.sequence.len() == 3
            && expectation.sequence[0].udf_kind == ManifestUdfKind::Query
            && expectation.sequence[1].udf_kind == ManifestUdfKind::Mutation
            && expectation.sequence[2].udf_kind == ManifestUdfKind::Query
            && !expectation.sequence[0].commit
            && expectation.sequence[1].commit
            && !expectation.sequence[2].commit,
        "application capability expectation must describe query, mutation, query"
    );
    let npm_version = Version::parse(&expectation.invocation_npm_version)
        .context("application capability npm version is invalid")?;
    let database = new_test_database(rt.clone()).await?;
    let user_id = install_application_setup(&database, &expectation.setup).await?;
    let runtime = load_application_runtime(&package_directory, &expectation.package)?;
    let metrics = Arc::new(GateMetrics::default());
    let mut reusable = None;
    let mut runtime_id = None;
    let mut memory_slot_id = None;

    for (sequence_index, invocation) in expectation.sequence.iter().enumerate() {
        let route = expectation
            .package
            .routes
            .iter()
            .find(|route| route.export_name == invocation.export_name)
            .context("application capability sequence names an unknown export")?;
        anyhow::ensure!(route.udf_kind == invocation.udf_kind);
        let selector = *runtime
            .selectors
            .get(&invocation.export_name)
            .context("application capability route has no selector")?;
        let timestamp_nanos = invocation
            .unix_timestamp_milliseconds
            .checked_mul(1_000_000)
            .context("application capability timestamp overflowed")?;
        let timestamp_nanos = u64::try_from(timestamp_nanos)
            .context("application capability timestamp must not precede the Unix epoch")?;
        let udf_type = match invocation.udf_kind {
            ManifestUdfKind::Query => UdfType::Query,
            ManifestUdfKind::Mutation => UdfType::Mutation,
        };
        let udf_path = format!(
            "{}:{}",
            expectation
                .package
                .runtime_module_path
                .strip_suffix(".js")
                .context("application runtime module path must end in .js")?,
            invocation.export_name
        );
        if sequence_index == 1 {
            let transaction = database
                .begin(Identity::user(application_user_identity(user_id.encode())?))
                .await?;
            let mut control_state = application_invocation_state(
                rt.clone(),
                transaction,
                &runtime.routed,
                Arc::clone(&runtime.controller),
                invocation.request.clone(),
                udf_type,
                udf_path.clone(),
                npm_version.clone(),
                UnixTimestamp::from_nanos(timestamp_nanos),
                Arc::clone(&metrics),
                None,
                None,
                true,
            )
            .await?;
            arm_generated_timeout(
                rt.clone(),
                &mut control_state,
                Duration::from_secs(10),
                *DATABASE_UDF_SYSTEM_TIMEOUT,
            )?;
            let control = execute_generated_with_entry_selector(
                Arc::clone(&runtime.routed),
                Some(selector),
                control_state,
                None,
                false,
            )
            .await?;
            assert_application_result(
                "fresh-instance control",
                sequence_index,
                invocation,
                &control,
            )?;
            drop(control.invocation.transaction);
        }
        let (instance, existing_slot, values) = match reusable.take() {
            Some(RetainedGeneratedTestRuntime {
                instance,
                memory_slot_id,
                values,
            }) => (Some(instance), Some(memory_slot_id), Some(values)),
            None => (None, None, None),
        };
        let mut transaction = database
            .begin(Identity::user(application_user_identity(user_id.encode())?))
            .await?;
        let initialize_runtime = instance.is_none();
        if let Some(instance) = &instance {
            let read_set = instance
                .context_read_set
                .as_ref()
                .context("reused application runtime has no initialization read set")?;
            anyhow::ensure!(
                ContextCache::validate_and_apply_context_read_set(&mut transaction, read_set)
                    .await?,
                "application initialization read set changed during the unchanged sequence"
            );
        }
        let mut state = application_invocation_state(
            rt.clone(),
            transaction,
            &runtime.routed,
            Arc::clone(&runtime.controller),
            invocation.request.clone(),
            udf_type,
            udf_path,
            npm_version.clone(),
            UnixTimestamp::from_nanos(timestamp_nanos),
            Arc::clone(&metrics),
            existing_slot,
            values,
            initialize_runtime,
        )
        .await?;
        arm_generated_timeout(
            rt.clone(),
            &mut state,
            Duration::from_secs(10),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let mut output = execute_generated_with_entry_selector(
            Arc::clone(&runtime.routed),
            Some(selector),
            state,
            instance,
            true,
        )
        .await?;
        assert_application_result("reused sequence", sequence_index, invocation, &output)?;
        reusable = Some(retain_generated_test_runtime(&mut output)?);
        let retained = reusable
            .as_ref()
            .context("application runtime was not retained")?;
        match (runtime_id, memory_slot_id) {
            (Some(expected_runtime), Some(expected_slot)) => {
                anyhow::ensure!(
                    retained.instance.id == expected_runtime
                        && retained.memory_slot_id == expected_slot,
                    "application capability sequence did not reuse one runtime slot"
                );
            },
            (None, None) => {
                runtime_id = Some(retained.instance.id);
                memory_slot_id = Some(retained.memory_slot_id);
            },
            _ => unreachable!(),
        }
        if invocation.commit {
            database
                .commit_with_write_source(
                    output.invocation.transaction,
                    "application_capability_test_invocation",
                )
                .await?;
        } else {
            drop(output.invocation.transaction);
        }
    }

    let RetainedGeneratedTestRuntime {
        instance,
        memory_slot_id,
        values,
    } = reusable.context("application capability sequence did not retain its final runtime")?;
    anyhow::ensure!(values.live_handle_count() == 0 && values.accounting().current_bytes == 0);
    let [(eviction_slot_id, eviction_reason)] =
        <[_; 1]>::try_from(runtime.controller.idle_eviction_candidates(
            &[(memory_slot_id, instance.idle_since.elapsed())],
            IdleEvictionTrigger::GenerationRetirement,
        ))
        .map_err(|_| anyhow::anyhow!("application capability cleanup did not select its slot"))?;
    anyhow::ensure!(eviction_slot_id == memory_slot_id);
    discard_generated_instance(instance).await?;
    runtime
        .controller
        .finish_idle_eviction(memory_slot_id, eviction_reason);

    let removed = GENERATED_ROUTED_MODULES
        .lock()
        .remove(
            &runtime.routed.route_identity,
            "module_cache_evicted_application_capability_test",
        )
        .context("application capability package was not cached")?;
    anyhow::ensure!(Arc::ptr_eq(&removed, &runtime.routed));
    drop(removed);
    drop(runtime.routed);
    let completed = runtime
        .controller
        .snapshot_for_test(&runtime.memory_identity);
    anyhow::ensure!(
        completed.active_instances == 0
            && completed.idle_instances == 0
            && completed.evicting_instances == 0
            && completed.fixed_module_bytes == 0,
        "application capability test retained guest or host ownership"
    );
    database.shutdown().await?;
    Ok(())
}

fn assert_singleton_query_result(
    output: &GeneratedExecutionOutput<ProdRuntime>,
    expectation: &SingletonQueryExpectation,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        output.invocation.outcome == InvocationOutcome::Success,
        "singleton query invocation failed: outcome {:?}, result {:?}, syscall trace {:?}, system \
         error {:?}",
        output.invocation.outcome,
        output.invocation.function_result,
        output.invocation.syscall_trace,
        output.system_error,
    );
    let result = output
        .invocation
        .function_result
        .as_ref()
        .context("singleton query invocation did not publish a result")?;
    anyhow::ensure!(
        application_result_values_equal(&expectation.expected_result, result),
        "singleton query result changed: expected {:?}, actual {:?}",
        expectation.expected_result,
        result,
    );
    anyhow::ensure!(
        output.invocation.operation_count == expectation.expected_operation_count
            && output.invocation.observed_identity == expectation.expected_observed_identity
            && output.invocation.observed_time == expectation.expected_observed_time,
        "singleton query invocation observations changed",
    );
    anyhow::ensure!(
        output
            .invocation
            .transaction
            .execution_size()
            .write_size
            .num_writes
            == 0
            && output
                .invocation
                .transaction
                .execution_size()
                .scheduled_size
                .num_writes
                == 0
            && output.invocation.opaque_live_handles == 0
            && output.invocation.opaque_current_bytes == 0
            && output.invocation.capability_revoked
            && !output.invocation.runtime_reuse_contaminated,
        "singleton query invocation did not reach the reusable read-only boundary",
    );
    Ok(())
}

async fn run_singleton_query_sequence(
    rt: ProdRuntime,
    package_directory: PathBuf,
    expectation: SingletonQueryExpectation,
) -> anyhow::Result<()> {
    let [route] = expectation.package.routes.as_slice() else {
        anyhow::bail!("singleton query expectation must contain exactly one route")
    };
    anyhow::ensure!(
        route.udf_kind == ManifestUdfKind::Query,
        "singleton query route must be a query",
    );
    let npm_version = Version::parse(&expectation.invocation_npm_version)
        .context("singleton query npm version is invalid")?;
    let database = new_test_database(rt.clone()).await?;
    initialize_application_system_tables(&database).await?;
    let runtime = load_application_runtime(&package_directory, &expectation.package)?;
    let selector = *runtime
        .selectors
        .get(&route.export_name)
        .context("singleton query route has no selector")?;
    let storage: Arc<dyn Storage> = Arc::new(LocalDirStorage::new(rt.clone())?);
    let file_storage = TransactionalFileStorage::new(
        rt.clone(),
        Arc::clone(&storage),
        ConvexOrigin::from("http://127.0.0.1:3210".to_owned()),
    );
    let content = base64::decode(&expectation.content_base64)
        .context("singleton query file-storage content is not valid base64")?;
    anyhow::ensure!(
        !content.is_empty() && base64::encode(&content) == expectation.content_base64,
        "singleton query file-storage content must be nonempty canonical base64",
    );
    let storage_uuid = match expectation.storage_uuid.parse::<FileStorageId>()? {
        FileStorageId::LegacyStorageId(storage_uuid) => storage_uuid,
        FileStorageId::DocumentId(_) => {
            anyhow::bail!("singleton query file-storage UUID parsed as a document ID")
        },
    };
    anyhow::ensure!(
        expectation.expected_result
            == json!(format!("http://127.0.0.1:3210/api/storage/{storage_uuid}")),
        "singleton query expected result is not its exact file-storage URL",
    );
    let mut upload = storage.start_upload().await?;
    upload.write(content.clone().into()).await?;
    let storage_key = upload.complete().await?;
    let digest: [u8; 32] = Sha256::digest(&content).into();
    let mut setup = database.begin_system().await?;
    let developer_id =
        model::file_storage::FileStorageModel::new(&mut setup, TableNamespace::root_component())
            .store_file(FileStorageEntry {
                storage_id: storage_uuid,
                storage_key,
                sha256: Sha256Digest::from(digest),
                size: content.len().try_into()?,
                content_type: Some(expectation.content_type.clone()),
            })
            .await?;
    let developer_id = setup
        .virtual_system_mapping()
        .system_resolved_id_to_virtual_developer_id(developer_id)?;
    database
        .commit_with_write_source(setup, "singleton_query_file_storage_setup")
        .await?;
    let request = json!({ "imageId": developer_id.encode() });
    let environment_data = EnvironmentData {
        key_broker: KeyBroker::dev().function_runner_keybroker(),
        default_system_env_vars: BTreeMap::new(),
        file_storage,
        module_loader: Arc::new(UnusedGateModuleCache),
        deployment: DeploymentMetadata {
            name: "singleton-query-test".to_owned(),
            region: None,
            class: DeploymentClass::S16,
        },
        host_secret_values: Some(BTreeMap::new()),
    };
    let metrics = Arc::new(GateMetrics::default());
    let mut retained: Option<RetainedGeneratedTestRuntime> = None;
    let mut retained_identity = None;

    for _ in 0..2 {
        let (instance, existing_slot, values) = match retained.take() {
            Some(retained) => (
                Some(retained.instance),
                Some(retained.memory_slot_id),
                Some(retained.values),
            ),
            None => (None, None, None),
        };
        let mut transaction = database.begin_system().await?;
        let initialize_runtime = instance.is_none();
        if let Some(instance) = &instance {
            let read_set = instance
                .context_read_set
                .as_ref()
                .context("reused singleton query runtime has no initialization read set")?;
            anyhow::ensure!(
                ContextCache::validate_and_apply_context_read_set(&mut transaction, read_set)
                    .await?,
                "singleton query initialization read set changed",
            );
        }
        let mut state = application_invocation_state(
            rt.clone(),
            transaction,
            &runtime.routed,
            Arc::clone(&runtime.controller),
            request.clone(),
            UdfType::Query,
            format!(
                "{}:{}",
                expectation
                    .package
                    .runtime_module_path
                    .strip_suffix(".js")
                    .context("singleton query runtime module path must end in .js")?,
                route.export_name,
            ),
            npm_version.clone(),
            UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
            Arc::clone(&metrics),
            existing_slot,
            values,
            initialize_runtime,
        )
        .await?;
        state.provider.set_test_file_storage_context(
            environment_data.key_broker.clone(),
            environment_data.file_storage.clone(),
            environment_data.deployment.clone(),
        );
        arm_generated_timeout(
            rt.clone(),
            &mut state,
            Duration::from_secs(10),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let mut output = execute_generated_with_entry_selector(
            Arc::clone(&runtime.routed),
            Some(selector),
            state,
            instance,
            true,
        )
        .await?;
        assert_singleton_query_result(&output, &expectation)?;
        let next_retained = retain_generated_test_runtime(&mut output)?;
        let identity = (next_retained.instance.id, next_retained.memory_slot_id);
        match retained_identity {
            Some(expected) => anyhow::ensure!(
                identity == expected,
                "singleton query invocation did not reuse its runtime instance and memory slot",
            ),
            None => retained_identity = Some(identity),
        }
        drop(output.invocation.transaction);
        retained = Some(next_retained);
    }

    let retained = retained.context("singleton query sequence did not retain its final runtime")?;
    anyhow::ensure!(
        retained.values.live_handle_count() == 0 && retained.values.accounting().current_bytes == 0,
        "singleton query cleanup retained opaque values",
    );
    let [(eviction_slot_id, eviction_reason)] =
        <[_; 1]>::try_from(runtime.controller.idle_eviction_candidates(
            &[(
                retained.memory_slot_id,
                retained.instance.idle_since.elapsed(),
            )],
            IdleEvictionTrigger::GenerationRetirement,
        ))
        .map_err(|_| anyhow::anyhow!("singleton query cleanup did not select its slot"))?;
    anyhow::ensure!(eviction_slot_id == retained.memory_slot_id);
    discard_generated_instance(retained.instance).await?;
    runtime
        .controller
        .finish_idle_eviction(eviction_slot_id, eviction_reason);
    anyhow::ensure!(
        metrics.teardowns.load(Ordering::SeqCst) == 1,
        "singleton query cleanup retained invocation state",
    );
    let removed = GENERATED_ROUTED_MODULES
        .lock()
        .remove(
            &runtime.routed.route_identity,
            "module_cache_evicted_singleton_query_test",
        )
        .context("singleton query package was not cached")?;
    anyhow::ensure!(Arc::ptr_eq(&removed, &runtime.routed));
    drop(removed);
    drop(runtime.routed);
    let completed = runtime
        .controller
        .snapshot_for_test(&runtime.memory_identity);
    anyhow::ensure!(
        completed.active_instances == 0
            && completed.idle_instances == 0
            && completed.evicting_instances == 0
            && completed.fixed_module_bytes == 0,
        "singleton query test retained guest or host ownership",
    );
    database.shutdown().await?;
    Ok(())
}

#[test]
#[ignore = "requires an operator-provided application capability-entry package"]
fn generated_wasm_application_capability_entry_reuses_query_mutation_query() -> anyhow::Result<()> {
    let package_directory = std::env::var_os(PACKAGE_DIRECTORY_ENV)
        .map(PathBuf::from)
        .context(format!("{PACKAGE_DIRECTORY_ENV} must identify the package"))?;
    let expectation: ApplicationCapabilityExpectation = serde_json::from_str(
        &std::env::var(EXPECTATION_ENV)
            .with_context(|| format!("{EXPECTATION_ENV} must contain the expectation"))?,
    )
    .context("failed to parse the application capability expectation")?;
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_application_capability_entry_query_mutation_query",
        run_application_capability_sequence(rt, package_directory, expectation),
    )
}

#[test]
#[ignore = "requires an operator-provided singleton query capability-entry package"]
fn generated_wasm_singleton_query_reuses_runtime() -> anyhow::Result<()> {
    let package_directory = std::env::var_os(SINGLETON_QUERY_PACKAGE_DIRECTORY_ENV)
        .map(PathBuf::from)
        .context(format!(
            "{SINGLETON_QUERY_PACKAGE_DIRECTORY_ENV} must identify the package"
        ))?;
    let expectation: SingletonQueryExpectation = serde_json::from_str(
        &std::env::var(SINGLETON_QUERY_EXPECTATION_ENV).with_context(|| {
            format!("{SINGLETON_QUERY_EXPECTATION_ENV} must contain the expectation")
        })?,
    )
    .context("failed to parse the singleton query expectation")?;
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on(
        "generated_wasm_singleton_query_reuse",
        run_singleton_query_sequence(rt, package_directory, expectation),
    )
}
