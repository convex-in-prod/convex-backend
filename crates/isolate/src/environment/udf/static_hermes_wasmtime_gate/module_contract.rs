use super::{
    routed_module_cache::GeneratedRouteIdentity,
    *,
};

const GENERATED_GUEST_PROMISE_ASYNC_COMPLETION_IMPORTS: [&str; 5] = [
    "convex_async_operation_wait_any",
    "convex_async_operation_poll_ready",
    "convex_async_operation_completion_status",
    "convex_async_operation_completion_take",
    "convex_async_operation_cancel_all",
];

const GENERATED_GUEST_PROMISE_ASYNC_OPERATION_IMPORTS: [&str; 6] = [
    "convex_async_operation_start_take",
    "convex_async_operation_wait_any",
    "convex_async_operation_poll_ready",
    "convex_async_operation_completion_status",
    "convex_async_operation_completion_take",
    "convex_async_operation_cancel_all",
];

fn validate_generated_async_completion_imports(
    convex_imports: &BTreeSet<String>,
) -> anyhow::Result<usize> {
    let import_count = GENERATED_GUEST_PROMISE_ASYNC_COMPLETION_IMPORTS
        .into_iter()
        .filter(|name| convex_imports.contains(*name))
        .count();
    anyhow::ensure!(
        matches!(import_count, 0 | 5),
        "generated module must import the complete async completion ABI"
    );
    Ok(import_count)
}

pub(super) fn validate_function_type(
    ty: &ExternType,
    parameters: &[ValType],
    results: &[ValType],
    description: &str,
) -> anyhow::Result<()> {
    let ExternType::Func(ty) = ty else {
        anyhow::bail!("{description} is not a function");
    };
    let actual_parameters = ty.params().collect::<Vec<_>>();
    let actual_results = ty.results().collect::<Vec<_>>();
    anyhow::ensure!(
        actual_parameters.len() == parameters.len()
            && actual_parameters
                .iter()
                .zip(parameters)
                .all(|(actual, expected)| ValType::eq(actual, expected))
            && actual_results.len() == results.len()
            && actual_results
                .iter()
                .zip(results)
                .all(|(actual, expected)| ValType::eq(actual, expected)),
        "{description} has an incompatible function type"
    );
    Ok(())
}

pub(super) fn generated_convex_import_signature(
    name: &str,
) -> Option<(&'static [ValType], &'static [ValType])> {
    Some(match name {
        "convex_guest_value_request_len" => (&[], &[ValType::I32]),
        "convex_guest_value_request_copy" => (&[ValType::I32, ValType::I32], &[ValType::I32]),
        "convex_guest_value_result" => (&[ValType::I32, ValType::I32], &[]),
        "convex_guest_value_decode" => (&[ValType::I32, ValType::I32], &[ValType::I64]),
        "convex_capability_request_decode" => (&[ValType::I32, ValType::I32], &[ValType::I64]),
        "convex_capability_request_release" => (&[ValType::I64], &[]),
        "convex_guest_value_encode" => (&[ValType::I64], &[ValType::I64]),
        "convex_guest_value_payload_len" => (&[ValType::I64], &[ValType::I32]),
        "convex_guest_value_payload_copy" => {
            (&[ValType::I64, ValType::I32, ValType::I32], &[ValType::I32])
        },
        "convex_guest_value_payload_release" => (&[ValType::I64], &[]),
        "convex_request_field" => (&[ValType::I32, ValType::I32], &[ValType::I64]),
        "convex_host_secret_verify" => {
            (&[ValType::I32, ValType::I32, ValType::I32], &[ValType::I32])
        },
        "convex_sha256_value" => (&[ValType::I32, ValType::I32, ValType::I32], &[ValType::I64]),
        "convex_crypto_subtle_digest_sha256" => (
            &[
                ValType::I64,
                ValType::I32,
                ValType::I32,
                ValType::I32,
                ValType::I32,
            ],
            &[],
        ),
        "convex_crypto_get_random_values" | "convex_crypto_random_uuid" => {
            (&[ValType::I64, ValType::I32, ValType::I32], &[])
        },
        "convex_math_random" => (&[ValType::I64], &[ValType::F64]),
        "convex_query_start_value" => (&[ValType::I32, ValType::I64], &[ValType::I64]),
        "convex_query_start_utf8" => (&[ValType::I32, ValType::I32, ValType::I32], &[ValType::I64]),
        "convex_async_batch_take" => (&[ValType::I64], &[ValType::I64]),
        "convex_async_query_stream_open_take" => (&[ValType::I32, ValType::I64], &[ValType::I32]),
        "convex_async_query_stream_next" => (&[ValType::I32], &[ValType::I32]),
        "convex_async_query_stream_close" => (&[ValType::I32], &[]),
        "convex_async_operation_start_take" => (&[ValType::I32, ValType::I64], &[ValType::I32]),
        "convex_capability_current" => (&[], &[ValType::I64]),
        "convex_console_message" => (
            &[ValType::I64, ValType::I32, ValType::I32, ValType::I32],
            &[ValType::I32],
        ),
        "convex_capability_query_stream_open_take" => {
            (&[ValType::I64, ValType::I64], &[ValType::I32])
        },
        "convex_capability_start_take" => (&[ValType::I64, ValType::I64], &[ValType::I32]),
        "convex_capability_sync_take" => (&[ValType::I64, ValType::I64], &[ValType::I64]),
        "convex_async_operation_wait_any" => (&[], &[ValType::I32]),
        "convex_async_operation_poll_ready" => (&[], &[ValType::I32]),
        "convex_async_operation_completion_status" => (&[ValType::I32], &[ValType::I32]),
        "convex_async_operation_completion_take" => (&[ValType::I32], &[ValType::I64]),
        "convex_async_operation_cancel_all" => (&[], &[ValType::I32]),
        "convex_function_handle_create" => (&[ValType::I32, ValType::I64], &[ValType::I64]),
        "convex_db_normalize_id" => (&[ValType::I32, ValType::I64], &[ValType::I64]),
        "convex_db_get" => (&[ValType::I32, ValType::I64], &[ValType::I64]),
        "convex_db_write" => (&[ValType::I32, ValType::I64, ValType::I64], &[ValType::I64]),
        "convex_scheduler_schedule" => {
            (&[ValType::I32, ValType::F64, ValType::I64], &[ValType::I64])
        },
        "convex_query_next" => (&[ValType::I64], &[ValType::I64]),
        "convex_value_type"
        | "convex_value_bool"
        | "convex_value_string_len"
        | "convex_value_array_len" => (&[ValType::I64], &[ValType::I32]),
        "convex_value_array_get" => (&[ValType::I64, ValType::I32], &[ValType::I64]),
        "convex_has_developer_error" => (&[], &[ValType::I32]),
        "convex_value_number" => (&[ValType::I64], &[ValType::F64]),
        "convex_value_string_copy" => {
            (&[ValType::I64, ValType::I32, ValType::I32], &[ValType::I32])
        },
        "convex_value_field" => (&[ValType::I64, ValType::I32, ValType::I32], &[ValType::I64]),
        "convex_value_release" | "convex_function_result" => (&[ValType::I64], &[]),
        "convex_value_null_new" | "convex_value_array_new" | "convex_value_object_new" => {
            (&[], &[ValType::I64])
        },
        "convex_value_bool_new" => (&[ValType::I32], &[ValType::I64]),
        "convex_value_number_new" => (&[ValType::F64], &[ValType::I64]),
        "convex_value_string_new" => (&[ValType::I32, ValType::I32], &[ValType::I64]),
        "convex_value_array_push" => (&[ValType::I64, ValType::I64], &[]),
        "convex_value_object_insert" => (
            &[ValType::I64, ValType::I32, ValType::I32, ValType::I64],
            &[],
        ),
        "convex_developer_error" => (&[ValType::I32, ValType::I32, ValType::I32], &[]),
        "convex_profile_mark" => (&[ValType::I32], &[]),
        INVOCATION_UNIX_TIMESTAMP_MS_IMPORT => (&[], &[ValType::F64]),
        _ => return None,
    })
}

pub(super) fn generated_host_import_signature(
    module: &str,
    name: &str,
) -> Option<(&'static [ValType], &'static [ValType])> {
    match module {
        "convex" => generated_convex_import_signature(name),
        "env" => Some(match name {
            "emscripten_notify_memory_growth" => (&[ValType::I32], &[]),
            "__syscall_faccessat" | "__syscall_readlinkat" => (
                &[ValType::I32, ValType::I32, ValType::I32, ValType::I32],
                &[ValType::I32],
            ),
            "__syscall_getcwd" => (&[ValType::I32, ValType::I32], &[ValType::I32]),
            "__syscall_unlinkat" => (&[ValType::I32, ValType::I32, ValType::I32], &[ValType::I32]),
            _ => return None,
        }),
        "wasi_snapshot_preview1" => Some(match name {
            "args_get" | "args_sizes_get" | "environ_get" | "environ_sizes_get"
            | "fd_fdstat_get" | "random_get" => (&[ValType::I32, ValType::I32], &[ValType::I32]),
            "fd_close" => (&[ValType::I32], &[ValType::I32]),
            "fd_seek" => (
                &[ValType::I32, ValType::I64, ValType::I32, ValType::I32],
                &[ValType::I32],
            ),
            "fd_read" | "fd_write" => (
                &[ValType::I32, ValType::I32, ValType::I32, ValType::I32],
                &[ValType::I32],
            ),
            "fd_pread" => (
                &[
                    ValType::I32,
                    ValType::I32,
                    ValType::I32,
                    ValType::I64,
                    ValType::I32,
                ],
                &[ValType::I32],
            ),
            "clock_time_get" => (&[ValType::I32, ValType::I64, ValType::I32], &[ValType::I32]),
            "proc_exit" => (&[ValType::I32], &[]),
            _ => return None,
        }),
        _ => None,
    }
}

pub(super) fn validate_generated_module_contract(
    module: &Module,
    manifest: &WasmUdfExecutionManifest,
) -> anyhow::Result<()> {
    let execution = WasmUdfExecutionPolicy::from_legacy(manifest);
    let permitted = permitted_conditional_convex_imports(
        execution.value_mode(),
        execution.effect_execution_mode(),
        execution.imported_operations(),
    );
    let requires_entry_selector = matches!(
        manifest.manifest_schema_version(),
        COHORT_MANIFEST_SCHEMA_VERSION
            | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
            | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION
    );
    validate_generated_module_contract_with_imports(
        module,
        &execution,
        &permitted,
        requires_entry_selector,
    )
}

pub(super) fn validate_generated_module_contract_with_imports(
    module: &Module,
    manifest: &WasmUdfExecutionPolicy,
    permitted_conditional_imports: &BTreeSet<&'static str>,
    requires_entry_selector: bool,
) -> anyhow::Result<()> {
    let mut host_imports = BTreeSet::new();
    let mut convex_imports = BTreeSet::new();
    for import in module.imports() {
        let import_module = import.module();
        let name = import.name();
        let (parameters, results) = generated_host_import_signature(import_module, name)
            .context("generated module has an unsupported host import")?;
        anyhow::ensure!(
            host_imports.insert((import_module.to_owned(), name.to_owned())),
            "generated module contains a duplicate host import"
        );
        if import_module == "convex" {
            convex_imports.insert(name.to_owned());
        }
        validate_function_type(&import.ty(), parameters, results, "generated host import")?;
    }
    validate_generated_execution_contract(
        module,
        manifest,
        permitted_conditional_imports,
        requires_entry_selector,
        &convex_imports,
        "convex_wasm_select_entry",
    )
}

pub(super) fn validate_generated_execution_contract(
    leaf_module: &Module,
    manifest: &WasmUdfExecutionPolicy,
    permitted_conditional_imports: &BTreeSet<&'static str>,
    requires_entry_selector: bool,
    convex_imports: &BTreeSet<String>,
    selector_export: &str,
) -> anyhow::Result<()> {
    let uses_async_batch = convex_imports.contains("convex_async_batch_take");
    let async_completion_import_count =
        validate_generated_async_completion_imports(convex_imports)?;
    let has_legacy_query_stream_open =
        convex_imports.contains("convex_async_query_stream_open_take");
    let has_capability_query_stream_open =
        convex_imports.contains("convex_capability_query_stream_open_take");
    let has_query_stream_next = convex_imports.contains("convex_async_query_stream_next");
    let has_query_stream_close = convex_imports.contains("convex_async_query_stream_close");
    let has_query_stream = has_legacy_query_stream_open || has_capability_query_stream_open;
    anyhow::ensure!(
        !(has_legacy_query_stream_open && has_capability_query_stream_open)
            && has_query_stream_next == has_query_stream
            && has_query_stream_close == has_query_stream,
        "generated module must import the complete async query-stream ABI"
    );
    let has_capability_current = convex_imports.contains("convex_capability_current");
    let has_console_message = convex_imports.contains("convex_console_message");
    let has_capability_request_decode = convex_imports.contains("convex_capability_request_decode");
    let has_capability_request_release =
        convex_imports.contains("convex_capability_request_release");
    let has_capability_start = convex_imports.contains("convex_capability_start_take");
    let has_capability_query_stream =
        convex_imports.contains("convex_capability_query_stream_open_take");
    let has_capability_sync = convex_imports.contains("convex_capability_sync_take");
    let has_crypto_subtle_digest_sha256 =
        convex_imports.contains("convex_crypto_subtle_digest_sha256");
    let has_invocation_randomness = [
        "convex_crypto_get_random_values",
        "convex_crypto_random_uuid",
        "convex_math_random",
    ]
    .into_iter()
    .any(|name| convex_imports.contains(name));
    anyhow::ensure!(
        (!has_crypto_subtle_digest_sha256 && !has_invocation_randomness && !has_console_message)
            || has_capability_current,
        "generated invocation-scoped imports require the current invocation-capability ABI"
    );
    anyhow::ensure!(
        (!has_capability_start && !has_capability_query_stream) || has_capability_current,
        "generated module must import the complete async invocation-capability ABI"
    );
    anyhow::ensure!(
        has_capability_request_decode == has_capability_request_release,
        "generated module must import the complete capability request-envelope handle ABI"
    );
    anyhow::ensure!(
        (!has_capability_start && !has_capability_sync && !has_capability_query_stream)
            || (has_capability_request_decode && has_capability_request_release),
        "generated capability dispatch requires the typed request-envelope ABI"
    );
    anyhow::ensure!(
        !has_capability_request_decode
            || has_capability_start
            || has_capability_sync
            || has_capability_query_stream,
        "generated request-envelope imports have no capability dispatch consumer"
    );
    anyhow::ensure!(
        !has_capability_current
            || has_capability_start
            || has_capability_sync
            || has_capability_query_stream
            || has_crypto_subtle_digest_sha256
            || has_invocation_randomness
            || has_console_message,
        "generated current invocation-capability import has no authorized consumer"
    );
    anyhow::ensure!(
        !has_capability_sync || has_capability_current,
        "generated synchronous capability imports require the invocation-capability ABI"
    );
    anyhow::ensure!(
        (!convex_imports.contains("convex_async_operation_start_take")
            && !convex_imports.contains("convex_capability_start_take")
            && !has_query_stream)
            || async_completion_import_count
                == GENERATED_GUEST_PROMISE_ASYNC_COMPLETION_IMPORTS.len(),
        "generated async starts require the complete async completion ABI"
    );
    let mut required_imports = match manifest.value_mode() {
        ValueMode::Opaque => BTreeSet::from(["convex_function_result"]),
        ValueMode::GuestNativeJson => BTreeSet::from([
            "convex_guest_value_request_len",
            "convex_guest_value_request_copy",
            "convex_guest_value_encode",
            "convex_guest_value_result",
        ]),
    };
    if matches!(manifest.value_mode(), ValueMode::GuestNativeJson) {
        validate_guest_value_decode_imports(&convex_imports)?;
    }
    let mut has_batchable_descriptor = false;
    for operation in manifest.imported_operations() {
        match (manifest.effect_execution_mode(), operation.operation()) {
            (_, ImportedOperationDescriptor::Sha256) => {
                required_imports.insert("convex_sha256_value");
            },
            (_, ImportedOperationDescriptor::HostSecretVerify { .. }) => {
                required_imports.insert("convex_host_secret_verify");
            },
            (_, ImportedOperationDescriptor::DatabaseNormalizeId { .. }) => {
                required_imports.insert("convex_db_normalize_id");
            },
            (
                EffectExecutionMode::GuestPromiseEventLoop,
                ImportedOperationDescriptor::AuthenticationGetUserIdentity {}
                | ImportedOperationDescriptor::FunctionHandleCreate {}
                | ImportedOperationDescriptor::DatabaseGet { .. }
                | ImportedOperationDescriptor::DatabaseInsert { .. }
                | ImportedOperationDescriptor::DatabasePatch { .. }
                | ImportedOperationDescriptor::DatabaseReplace { .. }
                | ImportedOperationDescriptor::DatabaseDelete { .. }
                | ImportedOperationDescriptor::SchedulerRunAfter { .. }
                | ImportedOperationDescriptor::SchedulerRunAt { .. },
            ) => {
                required_imports.extend(GENERATED_GUEST_PROMISE_ASYNC_OPERATION_IMPORTS);
                has_batchable_descriptor = true;
            },
            (
                EffectExecutionMode::GuestPromiseEventLoop,
                ImportedOperationDescriptor::DatabaseIndexQuery {
                    terminal: QueryTerminal::Stream,
                    ..
                },
            ) => {
                required_imports.extend([
                    "convex_async_query_stream_open_take",
                    "convex_async_query_stream_next",
                    "convex_async_query_stream_close",
                ]);
                required_imports.extend(GENERATED_GUEST_PROMISE_ASYNC_COMPLETION_IMPORTS);
                has_batchable_descriptor = true;
            },
            (
                EffectExecutionMode::GuestPromiseEventLoop,
                ImportedOperationDescriptor::DatabaseIndexQuery { .. },
            ) => {
                required_imports.extend(GENERATED_GUEST_PROMISE_ASYNC_OPERATION_IMPORTS);
                has_batchable_descriptor = true;
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::AuthenticationGetUserIdentity {},
            ) => {
                required_imports.insert("convex_async_batch_take");
                has_batchable_descriptor = true;
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::FunctionHandleCreate {},
            ) => {
                if !uses_async_batch {
                    required_imports.insert("convex_function_handle_create");
                }
                has_batchable_descriptor = true;
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::DatabaseIndexQuery { .. },
            ) => {
                if !uses_async_batch {
                    required_imports.insert("convex_query_next");
                    anyhow::ensure!(
                        convex_imports.contains("convex_query_start_value")
                            || convex_imports.contains("convex_query_start_utf8"),
                        "generated module omits a query-start import required by its execution \
                         manifest"
                    );
                }
                has_batchable_descriptor = true;
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::DatabaseGet { .. },
            ) => {
                if !uses_async_batch {
                    required_imports.insert("convex_db_get");
                }
                has_batchable_descriptor = true;
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::DatabaseInsert { .. }
                | ImportedOperationDescriptor::DatabasePatch { .. }
                | ImportedOperationDescriptor::DatabaseReplace { .. }
                | ImportedOperationDescriptor::DatabaseDelete { .. },
            ) => {
                if !uses_async_batch {
                    required_imports.insert("convex_db_write");
                }
                has_batchable_descriptor = true;
            },
            (
                EffectExecutionMode::BlockingFiber,
                ImportedOperationDescriptor::SchedulerRunAfter { .. }
                | ImportedOperationDescriptor::SchedulerRunAt { .. },
            ) => {
                if !uses_async_batch {
                    required_imports.insert("convex_scheduler_schedule");
                }
                has_batchable_descriptor = true;
            },
        }
    }
    anyhow::ensure!(
        required_imports
            .iter()
            .all(|required| convex_imports.contains(*required)),
        "generated module omits an import required by its execution manifest"
    );
    anyhow::ensure!(
        CONDITIONAL_CONVEX_IMPORTS.iter().all(|name| {
            !convex_imports.contains(*name) || permitted_conditional_imports.contains(*name)
        }),
        "generated module imports a conditional host operation not declared by its authenticated \
         package members"
    );
    anyhow::ensure!(
        requires_entry_selector || !uses_async_batch || has_batchable_descriptor,
        "generated module imports the async batch operation without a batchable descriptor"
    );

    for (name, parameters, results) in [
        ("_initialize", &[] as &[ValType], &[] as &[ValType]),
        (
            "convex_wasm_udf_run",
            &[] as &[ValType],
            &[ValType::I32] as &[ValType],
        ),
        (
            "convex_wasm_udf_destroy_runtime",
            &[] as &[ValType],
            &[] as &[ValType],
        ),
    ] {
        let ty = leaf_module
            .get_export(name)
            .with_context(|| format!("generated module export {name} is missing"))?;
        validate_function_type(
            &ty,
            parameters,
            results,
            &format!("generated export {name}"),
        )?;
    }
    let prepare = leaf_module
        .get_export("convex_wasm_udf_prepare_selected_entry")
        .context("generated module export convex_wasm_udf_prepare_selected_entry is missing")?;
    validate_function_type(
        &prepare,
        &[],
        &[ValType::I32],
        "generated selected-entry preparation export",
    )?;
    if requires_entry_selector {
        let selector = leaf_module
            .get_export(selector_export)
            .context("generated cohort module export convex_wasm_select_entry is missing")?;
        validate_function_type(
            &selector,
            &[ValType::I64],
            &[ValType::I32],
            "generated cohort selector export",
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_guest_promise_async_completion_abi_requires_cancellation() -> anyhow::Result<()> {
        let complete_imports = GENERATED_GUEST_PROMISE_ASYNC_OPERATION_IMPORTS
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert_eq!(
            validate_generated_async_completion_imports(&complete_imports)?,
            5
        );

        let mut missing_cancellation = complete_imports;
        assert!(missing_cancellation.remove("convex_async_operation_cancel_all"));
        let error = validate_generated_async_completion_imports(&missing_cancellation)
            .expect_err("guest-Promise completion imports passed without cancellation");
        assert!(error.to_string().contains("complete async completion ABI"));
        Ok(())
    }
}

pub(super) fn validate_generated_graph_module_contract(
    module: &Module,
    contract: &CapabilityGraphModuleContract,
) -> anyhow::Result<BTreeSet<String>> {
    let expected_imports = contract
        .imports
        .iter()
        .map(|import| ((import.module.as_str(), import.name.as_str()), import))
        .collect::<BTreeMap<_, _>>();
    anyhow::ensure!(
        expected_imports.len() == contract.imports.len(),
        "generated graph module contract contains duplicate imports"
    );
    let mut actual_imports = BTreeSet::new();
    let mut convex_host_imports = BTreeSet::new();
    for import in module.imports() {
        let key = (import.module().to_owned(), import.name().to_owned());
        anyhow::ensure!(
            actual_imports.insert(key.clone()),
            "generated graph module contains a duplicate import"
        );
        let expected = expected_imports
            .get(&(key.0.as_str(), key.1.as_str()))
            .context("generated graph module contains an undeclared import")?;
        validate_generated_graph_extern_type(&import.ty(), &expected.ty)?;
        if matches!(expected.provider, CapabilityGraphImportProvider::Host) {
            let (parameters, results) = generated_host_import_signature(&key.0, &key.1)
                .context("generated graph module has an unsupported host import")?;
            validate_function_type(
                &import.ty(),
                parameters,
                results,
                "generated graph host import",
            )?;
            if key.0 == "convex" {
                convex_host_imports.insert(key.1);
            }
        }
    }
    anyhow::ensure!(
        actual_imports.len() == expected_imports.len(),
        "generated graph module omits an import declared by its contract"
    );

    let expected_exports = contract
        .exports
        .iter()
        .map(|export| (export.name.as_str(), &export.ty))
        .collect::<BTreeMap<_, _>>();
    anyhow::ensure!(
        expected_exports.len() == contract.exports.len(),
        "generated graph module contract contains duplicate exports"
    );
    let mut actual_exports = BTreeSet::new();
    for export in module.exports() {
        anyhow::ensure!(
            actual_exports.insert(export.name().to_owned()),
            "generated graph module contains a duplicate export"
        );
        let expected = expected_exports
            .get(export.name())
            .context("generated graph module contains an undeclared export")?;
        validate_generated_graph_extern_type(&export.ty(), expected)?;
    }
    anyhow::ensure!(
        actual_exports.len() == expected_exports.len(),
        "generated graph module omits an export declared by its contract"
    );
    Ok(convex_host_imports)
}

fn validate_generated_graph_extern_type(
    actual: &ExternType,
    expected: &CapabilityGraphExternType,
) -> anyhow::Result<()> {
    let matches = match (actual, expected) {
        (
            ExternType::Func(actual),
            CapabilityGraphExternType::Function {
                parameters,
                results,
            },
        ) => generated_graph_function_type_matches(actual, parameters, results),
        (ExternType::Global(actual), CapabilityGraphExternType::Global { mutable, value }) => {
            actual.mutability()
                == if *mutable {
                    Mutability::Var
                } else {
                    Mutability::Const
                }
                && generated_graph_value_type_matches(actual.content(), *value)
        },
        (
            ExternType::Memory(actual),
            CapabilityGraphExternType::Memory {
                maximum_pages,
                memory64,
                minimum_pages,
                page_size_bytes,
                shared,
            },
        ) => {
            actual.minimum() == *minimum_pages
                && actual.maximum() == *maximum_pages
                && actual.is_64() == *memory64
                && actual.is_shared() == *shared
                && actual.page_size() == *page_size_bytes
        },
        (
            ExternType::Table(actual),
            CapabilityGraphExternType::Table {
                element,
                maximum_elements,
                minimum_elements,
                table64,
            },
        ) => {
            actual.minimum() == *minimum_elements
                && actual.maximum() == *maximum_elements
                && actual.is_64() == *table64
                && generated_graph_value_type_matches(
                    &ValType::Ref(actual.element().clone()),
                    *element,
                )
        },
        (
            ExternType::Tag(actual),
            CapabilityGraphExternType::Tag {
                parameters,
                results,
            },
        ) => generated_graph_function_type_matches(actual.ty(), parameters, results),
        (
            ExternType::Func(_)
            | ExternType::Global(_)
            | ExternType::Table(_)
            | ExternType::Memory(_)
            | ExternType::Tag(_),
            CapabilityGraphExternType::Function { .. }
            | CapabilityGraphExternType::Global { .. }
            | CapabilityGraphExternType::Memory { .. }
            | CapabilityGraphExternType::Table { .. }
            | CapabilityGraphExternType::Tag { .. },
        ) => false,
    };
    anyhow::ensure!(
        matches,
        "generated graph extern type differs from its contract"
    );
    Ok(())
}

fn generated_graph_function_type_matches(
    actual: &wasmtime::FuncType,
    parameters: &[CapabilityGraphValueType],
    results: &[CapabilityGraphValueType],
) -> bool {
    let actual_parameters = actual.params().collect::<Vec<_>>();
    let actual_results = actual.results().collect::<Vec<_>>();
    actual_parameters.len() == parameters.len()
        && actual_parameters
            .iter()
            .zip(parameters)
            .all(|(actual, expected)| generated_graph_value_type_matches(actual, *expected))
        && actual_results.len() == results.len()
        && actual_results
            .iter()
            .zip(results)
            .all(|(actual, expected)| generated_graph_value_type_matches(actual, *expected))
}

fn generated_graph_value_type_matches(
    actual: &ValType,
    expected: CapabilityGraphValueType,
) -> bool {
    let expected = match expected {
        CapabilityGraphValueType::I32 => ValType::I32,
        CapabilityGraphValueType::I64 => ValType::I64,
        CapabilityGraphValueType::F32 => ValType::F32,
        CapabilityGraphValueType::F64 => ValType::F64,
        CapabilityGraphValueType::V128 => ValType::V128,
        CapabilityGraphValueType::FuncRef => ValType::FUNCREF,
        CapabilityGraphValueType::ExternRef => ValType::EXTERNREF,
        CapabilityGraphValueType::ExnRef => ValType::EXNREF,
    };
    ValType::eq(actual, &expected)
}

pub(super) fn validate_guest_value_decode_imports(
    convex_imports: &BTreeSet<String>,
) -> anyhow::Result<()> {
    let imports_opaque_decode = convex_imports.contains("convex_guest_value_decode");
    let imports_owned_payload = [
        "convex_guest_value_payload_len",
        "convex_guest_value_payload_copy",
        "convex_guest_value_payload_release",
    ]
    .into_iter()
    .all(|name| convex_imports.contains(name));
    anyhow::ensure!(
        imports_opaque_decode || imports_owned_payload,
        "generated module omits the complete guest-value decode imports required by its execution \
         manifest"
    );
    Ok(())
}

pub(super) fn generated_runtime_compatibility<'a>(
    engine_compatibility_sha256: &'a str,
) -> anyhow::Result<RuntimeCompatibility<'a>> {
    let mut platform_limits = PlatformLimits::authoritative()?;
    #[cfg(any(test, feature = "testing"))]
    if let Some(execution_time_ms) = super::generated_runtime_execution_time_limit_override() {
        platform_limits.execution_time_ms = execution_time_ms;
    }
    Ok(RuntimeCompatibility {
        opaque_value_abi_version: OPAQUE_VALUE_ABI_VERSION,
        platform_limits,
        wasmtime_revision: GENERATED_WASMTIME_REVISION,
        target_triple: GENERATED_TARGET_TRIPLE,
        target_cpu: GENERATED_TARGET_CPU,
        engine_configuration_sha256: GENERATED_ENGINE_CONFIGURATION_SHA256,
        engine_compatibility_sha256,
    })
}

pub(super) fn generated_memory_identity_for_package(
    manifest: &WasmUdfExecutionPolicy,
    package_identity: &ValidatedWasmUdfPackageIdentity,
    route_identity: &GeneratedRouteIdentity,
) -> FunctionMemoryIdentity {
    let (deployment, package_key) = match route_identity {
        GeneratedRouteIdentity::Singleton { package_key } => {
            (format!("singleton:{package_key}"), package_key.clone())
        },
        GeneratedRouteIdentity::DeploymentExport {
            deployment_sha256,
            package_key,
            ..
        } => (deployment_sha256.clone(), package_key.clone()),
    };
    let (
        lineage,
        compiler_pipeline_sha256,
        compiler_revision,
        static_hermes_revision,
        admitted_language_version,
        manifest_schema_version,
        opaque_value_abi_version,
    ) = match package_identity {
        ValidatedWasmUdfPackageIdentity::Legacy(legacy) => {
            let source = legacy.source();
            let compiler = legacy.compiler();
            (
                FunctionMemoryLineage::LegacyExport {
                    module_path: source.module_path().to_owned(),
                    export_name: source.export_name().to_owned(),
                    udf_kind: source.udf_kind(),
                    resolved_graph_sha256: source.resolved_graph_sha256().as_str().to_owned(),
                    export_sha256: source.export_sha256().as_str().to_owned(),
                },
                compiler.artifact_pipeline_sha256().as_str().to_owned(),
                compiler.compiler_revision().to_owned(),
                compiler.static_hermes_revision().to_owned(),
                compiler.admitted_language_version(),
                legacy.manifest_schema_version(),
                legacy.opaque_value_abi_version(),
            )
        },
        ValidatedWasmUdfPackageIdentity::CapabilityEntry(entry) => (
            FunctionMemoryLineage::CapabilityPackage {
                package_key: package_key.clone(),
            },
            entry.artifact_pipeline_sha256().to_owned(),
            entry.compiler_revision().to_owned(),
            entry.static_hermes_revision().to_owned(),
            entry.admitted_language_version(),
            entry.manifest_schema_version(),
            entry.opaque_value_abi_version(),
        ),
        ValidatedWasmUdfPackageIdentity::ModuleGraphCohort(cohort) => (
            FunctionMemoryLineage::CapabilityPackage {
                package_key: package_key.clone(),
            },
            cohort.artifact_pipeline_sha256().to_owned(),
            cohort.compiler_revision().to_owned(),
            cohort.static_hermes_revision().to_owned(),
            cohort.admitted_language_version(),
            1,
            OPAQUE_VALUE_ABI_VERSION,
        ),
    };
    FunctionMemoryIdentity {
        deployment,
        package_key,
        lineage,
        compiler_pipeline_sha256,
        compiler_revision,
        static_hermes_revision,
        admitted_language_version,
        manifest_schema_version,
        effect_execution_mode: manifest.effect_execution_mode(),
        opaque_value_abi_version,
        wasmtime_revision: GENERATED_WASMTIME_REVISION.to_owned(),
        target_triple: GENERATED_TARGET_TRIPLE.to_owned(),
        target_cpu: GENERATED_TARGET_CPU.to_owned(),
        engine_configuration_sha256: GENERATED_ENGINE_CONFIGURATION_SHA256.to_owned(),
        admission_policy_version: GENERATED_MEMORY_ADMISSION_POLICY_VERSION,
    }
}

#[cfg(any(test, feature = "testing"))]
pub(super) fn generated_memory_identity(
    manifest: &WasmUdfExecutionManifest,
    route_identity: &GeneratedRouteIdentity,
) -> FunctionMemoryIdentity {
    generated_memory_identity_for_package(
        &WasmUdfExecutionPolicy::from_legacy(manifest),
        &ValidatedWasmUdfPackageIdentity::Legacy(manifest.clone()),
        route_identity,
    )
}

#[cfg(test)]
#[test]
fn async_completion_poll_ready_import_has_the_opaque_completion_signature() {
    let (parameters, results) =
        generated_convex_import_signature("convex_async_operation_poll_ready")
            .expect("poll-ready import must be part of the generated host ABI");
    assert!(parameters.is_empty());
    assert_eq!(results.len(), 1);
    assert!(ValType::eq(&results[0], &ValType::I32));
}
