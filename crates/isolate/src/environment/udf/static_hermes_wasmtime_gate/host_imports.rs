use super::*;

fn ensure_wasi_output_within_limit(
    stdout_bytes: usize,
    stderr_bytes: usize,
    pending_bytes: usize,
) -> Result<(), WasmtimeError> {
    if stdout_bytes
        .checked_add(stderr_bytes)
        .and_then(|retained| retained.checked_add(pending_bytes))
        .is_none_or(|total| total > MAX_RESULT_BYTES)
    {
        return Err(WasmtimeError::new(HostInvariant));
    }
    Ok(())
}

pub(super) fn add_generated_convex_imports<RT: Runtime>(
    linker: &mut Linker<HostState<RT>>,
) -> Result<(), WasmtimeError> {
    linker.func_wrap(
        "convex",
        INVOCATION_UNIX_TIMESTAMP_MS_IMPORT,
        |mut caller: Caller<'_, HostState<RT>>| -> Result<f64, WasmtimeError> {
            require_active_generated_execution(caller.data())?;
            let unix_timestamp = caller
                .data_mut()
                .provider
                .unix_timestamp()
                .map_err(|_| WasmtimeError::new(HostInvariant))?;
            let unix_timestamp_ms = unix_timestamp
                .as_ms_since_epoch()
                .map_err(|_| WasmtimeError::new(HostInvariant))?;
            if unix_timestamp_ms > MAX_EXACT_JAVASCRIPT_INTEGER {
                return Err(WasmtimeError::new(HostInvariant));
            }
            Ok(unix_timestamp_ms as f64)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_console_message",
        |mut caller: Caller<'_, HostState<RT>>,
         capability_handle: i64,
         level: i32,
         pointer: i32,
         length: i32| {
            if !authorize_generated_capability(caller.data_mut(), capability_handle)? {
                return Ok(-1);
            }
            let level = match level {
                0 => LogLevel::Debug,
                1 => LogLevel::Error,
                2 => LogLevel::Info,
                3 => LogLevel::Log,
                4 => LogLevel::Warn,
                _ => return Err(WasmtimeError::new(HostInvariant)),
            };
            let messages: Vec<String> =
                with_guest_bytes(&mut caller, pointer, length, MAX_REQUEST_BYTES, |bytes| {
                    serde_json::from_slice(bytes).map_err(|_| WasmtimeError::new(HostInvariant))
                })?;
            caller
                .data_mut()
                .provider
                .emit_log_line(level, messages)
                .map_err(|_| WasmtimeError::new(HostInvariant))?;
            Ok(0)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_guest_value_request_len",
        |caller: Caller<'_, HostState<RT>>| {
            let generated = generated_state(caller.data())?;
            if generated.manifest.value_mode() != ValueMode::GuestNativeJson {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let length = generated
                .values
                .bytes_value(generated.request_handle)
                .map_err(|_| WasmtimeError::new(HostInvariant))?
                .len();
            i32::try_from(length).map_err(|_| WasmtimeError::new(HostInvariant))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_guest_value_request_copy",
        |mut caller: Caller<'_, HostState<RT>>, pointer: i32, capacity: i32| {
            let capacity = checked_len(capacity, MAX_REQUEST_BYTES)?;
            let length = write_guest_bytes_from_state(&mut caller, pointer, capacity, |state| {
                let generated = generated_state(state)?;
                if generated.manifest.value_mode() != ValueMode::GuestNativeJson {
                    return Err(WasmtimeError::new(HostInvariant));
                }
                generated
                    .values
                    .bytes_value(generated.request_handle)
                    .map_err(|_| WasmtimeError::new(HostInvariant))
            })?;
            i32::try_from(length).map_err(|_| WasmtimeError::new(HostInvariant))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_guest_value_decode",
        |mut caller: Caller<'_, HostState<RT>>, pointer: i32, length: i32| {
            let maximum = usize::try_from(
                generated_state(caller.data())?
                    .manifest
                    .limits()
                    .max_host_owned_bytes(),
            )
            .map_err(|_| WasmtimeError::new(HostInvariant))?
            .min(MAX_REQUEST_BYTES);
            let allows_pending_values = caller.data().provider.allows_pending_values();
            let value = with_guest_bytes(&mut caller, pointer, length, maximum, |bytes| {
                let value = if allows_pending_values {
                    GuestNativeValueCodec::decode_pending(bytes, maximum)
                } else {
                    GuestNativeValueCodec::decode(bytes, maximum)
                };
                value.map_err(|_| WasmtimeError::new(HostInvariant))
            })?;
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.insert_json(value))
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_capability_request_decode",
        |mut caller: Caller<'_, HostState<RT>>, pointer: i32, length: i32| {
            let generated = generated_state(caller.data())?;
            let maximum = usize::try_from(generated.manifest.limits().max_host_owned_bytes())
                .map_err(|_| WasmtimeError::new(HostInvariant))?
                .min(MAX_REQUEST_BYTES);
            let request = with_guest_bytes(&mut caller, pointer, length, maximum, |bytes| {
                GuestCapabilityRequestCodec::decode(bytes, maximum)
                    .map_err(|_| WasmtimeError::new(HostInvariant))
            })?;
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| {
                    values.insert(OpaqueValue::CapabilityRequest(request))
                })
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_capability_request_release",
        |mut caller: Caller<'_, HostState<RT>>, handle: i64| {
            let handle = opaque_handle(handle)?;
            generated_state_mut(caller.data_mut())?.opaque_value_operation(|values| {
                values.release(handle, OpaqueValueKind::CapabilityRequest)
            })
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_guest_value_encode",
        |mut caller: Caller<'_, HostState<RT>>, handle: i64| {
            record_guest_native_completion_stage(&caller.data().metrics, "encode:start");
            let maximum = usize::try_from(
                generated_state(caller.data())?
                    .manifest
                    .limits()
                    .max_host_owned_bytes(),
            )
            .map_err(|_| WasmtimeError::new(HostInvariant))?
            .min(MAX_RESULT_BYTES);
            let allows_pending_values = caller.data().provider.allows_pending_values();
            let handle = opaque_handle(handle)?;
            let value = match generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.take(handle, OpaqueValueKind::ConvexJson))?
            {
                OpaqueValue::ConvexJson(value) => value,
                OpaqueValue::Bytes(_)
                | OpaqueValue::QueryCursor(_)
                | OpaqueValue::CapabilityRequest(_) => {
                    return Err(WasmtimeError::new(HostInvariant));
                },
            };
            let bytes = if allows_pending_values {
                GuestNativeValueCodec::encode_pending(value, maximum)
            } else {
                GuestNativeValueCodec::encode(value, maximum)
            }
            .map_err(|_| WasmtimeError::new(HostInvariant))?;
            record_guest_native_completion_stage(&caller.data().metrics, "encode:codec-ok");
            let handle = generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.insert_bytes(bytes))
                .map(OpaqueHandle::to_abi)?;
            record_guest_native_completion_stage(&caller.data().metrics, "encode:stored");
            Ok(handle)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_guest_value_payload_len",
        |caller: Caller<'_, HostState<RT>>, handle: i64| {
            record_guest_native_completion_stage(&caller.data().metrics, "payload-len:start");
            let length = generated_state(caller.data())?
                .values
                .bytes_value(opaque_handle(handle)?)
                .map_err(|_| WasmtimeError::new(HostInvariant))?
                .len();
            let length = i32::try_from(length).map_err(|_| WasmtimeError::new(HostInvariant))?;
            record_guest_native_completion_stage(&caller.data().metrics, "payload-len:ok");
            Ok(length)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_guest_value_payload_copy",
        |mut caller: Caller<'_, HostState<RT>>, handle: i64, pointer: i32, capacity: i32| {
            record_guest_native_completion_stage(&caller.data().metrics, "payload-copy:start");
            let capacity = checked_len(capacity, MAX_RESULT_BYTES)?;
            let handle = opaque_handle(handle)?;
            let length = write_guest_bytes_from_state(&mut caller, pointer, capacity, |state| {
                generated_state(state)?
                    .values
                    .bytes_value(handle)
                    .map_err(|_| WasmtimeError::new(HostInvariant))
            })?;
            let length = i32::try_from(length).map_err(|_| WasmtimeError::new(HostInvariant))?;
            record_guest_native_completion_stage(&caller.data().metrics, "payload-copy:ok");
            Ok(length)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_guest_value_payload_release",
        |mut caller: Caller<'_, HostState<RT>>, handle: i64| {
            record_guest_native_completion_stage(&caller.data().metrics, "payload-release:start");
            let handle = opaque_handle(handle)?;
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.release(handle, OpaqueValueKind::Bytes))?;
            record_guest_native_completion_stage(&caller.data().metrics, "payload-release:ok");
            Ok(())
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_guest_value_result",
        |mut caller: Caller<'_, HostState<RT>>, pointer: i32, length: i32| {
            record_guest_native_completion_stage(&caller.data().metrics, "result:start");
            if (caller.data().developer_error.is_some()
                && !generated_state(caller.data())?.discard_after_caught_initialization_failure)
                || generated_state(caller.data())?.manifest.value_mode()
                    != ValueMode::GuestNativeJson
            {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let maximum = usize::try_from(
                generated_state(caller.data())?
                    .manifest
                    .limits()
                    .max_result_bytes(),
            )
            .map_err(|_| WasmtimeError::new(HostInvariant))?
            .min(MAX_RESULT_BYTES);
            let allows_pending_values = caller.data().provider.allows_pending_values();
            let result = with_guest_bytes(&mut caller, pointer, length, maximum, |bytes| {
                let result = if allows_pending_values {
                    GuestNativeValueCodec::decode_pending(bytes, maximum)
                } else {
                    GuestNativeValueCodec::decode(bytes, maximum)
                };
                result.map_err(|_| WasmtimeError::new(HostInvariant))
            })?;
            record_guest_native_completion_stage(&caller.data().metrics, "result:decoded");
            let generated = generated_state_mut(caller.data_mut())?;
            let handle = generated.opaque_value_operation(|values| values.insert_json(result))?;
            generated.opaque_value_operation(|values| values.set_final_result(handle))?;
            record_guest_native_completion_stage(&caller.data().metrics, "result:stored");
            Ok(())
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_request_field",
        |mut caller: Caller<'_, HostState<RT>>, pointer: i32, length: i32| {
            let name =
                read_generated_utf8(&mut caller, pointer, length, MAX_GENERATED_FIELD_NAME_BYTES)?;
            let generated = generated_state_mut(caller.data_mut())?;
            let request_handle = generated.request_handle;
            generated
                .opaque_value_operation(|values| values.clone_object_field(request_handle, &name))
                .map(|handle| handle.map_or(0, OpaqueHandle::to_abi))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_host_secret_verify",
        |mut caller: Caller<'_, HostState<RT>>, operation_id: i32, pointer: i32, length: i32| {
            let descriptor = generated_state_mut(caller.data_mut())?.operation(operation_id)?;
            let ImportedOperationDescriptor::HostSecretVerify {
                contract_version: 1,
                selector,
            } = descriptor
            else {
                return Err(WasmtimeError::new(HostInvariant));
            };
            let configured = generated_state(caller.data())?
                .host_secret_values
                .get(&selector)
                .filter(|value| !value.as_bytes().is_empty())
                .cloned();
            let Some(configured) = configured else {
                return Ok(-1);
            };
            if pointer == 0 && length == 0 {
                return Ok(0);
            }
            with_guest_bytes(
                &mut caller,
                pointer,
                length,
                MAX_HOST_SECRET_INPUT_BYTES,
                |provided| {
                    Ok(i32::from(
                        aws_lc_rs::constant_time::verify_slices_are_equal(
                            configured.as_bytes(),
                            provided,
                        )
                        .is_ok(),
                    ))
                },
            )
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_sha256_value",
        |mut caller: Caller<'_, HostState<RT>>, operation_id: i32, pointer: i32, length: i32| {
            let digest =
                with_guest_bytes(&mut caller, pointer, length, MAX_REQUEST_BYTES, |input| {
                    Ok(format!("{:x}", Sha256::digest(input)))
                })?;
            let descriptor = generated_state_mut(caller.data_mut())?.operation(operation_id)?;
            if !matches!(descriptor, ImportedOperationDescriptor::Sha256) {
                return Err(WasmtimeError::new(HostInvariant));
            }
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.insert_string(digest))
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_crypto_subtle_digest_sha256",
        |mut caller: Caller<'_, HostState<RT>>,
         capability: i64,
         input_pointer: i32,
         input_length: i32,
         output_pointer: i32,
         output_length: i32| {
            if !authorize_generated_capability(caller.data_mut(), capability)? {
                return Err(WasmtimeError::new(HostInvariant));
            }
            if output_length != SHA256_DIGEST_BYTES as i32 {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let input_length = checked_len(input_length, MAX_CRYPTO_SUBTLE_DIGEST_INPUT_BYTES)?;
            generated_state_mut(caller.data_mut())?.count_operation()?;
            let input_bytes =
                u64::try_from(input_length).map_err(|_| WasmtimeError::new(HostInvariant))?;
            let digest_fuel = input_bytes
                .checked_mul(SHA256_DIGEST_FUEL_PER_INPUT_BYTE)
                .and_then(|fuel| fuel.checked_add(SHA256_DIGEST_BASE_FUEL))
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
            consume_generated_fuel(&mut caller, digest_fuel)?;

            let memory = guest_memory(&mut caller)?;
            let input_start = checked_offset(input_pointer)?;
            let input_end = input_start
                .checked_add(input_length)
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
            let digest = {
                let input = memory
                    .data(&caller)
                    .get(input_start..input_end)
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
                aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, input)
            };
            write_guest_bytes(&mut caller, output_pointer, digest.as_ref())
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_crypto_get_random_values",
        |mut caller: Caller<'_, HostState<RT>>,
         capability: i64,
         output_pointer: i32,
         output_length: i32| {
            let output_length = checked_len(output_length, MAX_CRYPTO_GET_RANDOM_VALUES_BYTES)?;
            // Capability rejection must happen before inspecting guest memory:
            // a stale/rejected identity may mark the invocation contaminated,
            // and pointer validity is not an authority oracle for that caller.
            if !authorize_generated_capability(caller.data_mut(), capability)? {
                return Err(WasmtimeError::new(HostInvariant));
            }
            generated_state_mut(caller.data_mut())?.count_operation()?;
            consume_generated_random_fuel(
                &mut caller,
                u64::try_from(output_length).map_err(|_| WasmtimeError::new(HostInvariant))?,
            )?;
            let output_start = checked_offset(output_pointer)?;
            let output_end = output_start
                .checked_add(output_length)
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
            let memory = guest_memory(&mut caller)?;
            if memory.data(&caller).get(output_start..output_end).is_none() {
                return Err(WasmtimeError::new(HostInvariant));
            }
            // The output range was validated above. Fill it in place while
            // the host state and guest memory are both borrowed, avoiding a
            // temporary buffer and a second copy for every crypto call.
            let (guest_bytes, state) = memory.data_and_store_mut(&mut caller);
            let output = guest_bytes
                .get_mut(output_start..output_end)
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
            state
                .provider
                .rng()
                .map_err(|_| WasmtimeError::new(HostInvariant))?
                .fill(output);
            Ok(())
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_crypto_random_uuid",
        |mut caller: Caller<'_, HostState<RT>>,
         capability: i64,
         output_pointer: i32,
         output_length: i32| {
            if output_length != RANDOM_UUID_BYTES as i32 {
                return Err(WasmtimeError::new(HostInvariant));
            }
            // Keep the capability boundary ahead of all guest-memory
            // validation, matching the random-values import above.
            if !authorize_generated_capability(caller.data_mut(), capability)? {
                return Err(WasmtimeError::new(HostInvariant));
            }
            generated_state_mut(caller.data_mut())?.count_operation()?;
            consume_generated_random_fuel(&mut caller, RANDOM_UUID_ENTROPY_BYTES)?;
            let output_start = checked_offset(output_pointer)?;
            let output_end = output_start
                .checked_add(RANDOM_UUID_BYTES)
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
            let memory = guest_memory(&mut caller)?;
            if memory.data(&caller).get(output_start..output_end).is_none() {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let random_bytes = caller
                .data_mut()
                .provider
                .rng()
                .map_err(|_| WasmtimeError::new(HostInvariant))?
                .random();
            // UUID formatting has a fixed 36-byte representation. Encode it
            // into stack storage instead of allocating a String before the
            // already-validated guest copy.
            let uuid = uuid::Builder::from_random_bytes(random_bytes).into_uuid();
            let mut uuid_bytes = [0; RANDOM_UUID_BYTES];
            let uuid_text = uuid.hyphenated().encode_lower(&mut uuid_bytes);
            debug_assert_eq!(uuid_text.len(), RANDOM_UUID_BYTES);
            memory
                .data_mut(&mut caller)
                .get_mut(output_start..output_end)
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?
                .copy_from_slice(&uuid_bytes);
            Ok(())
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_math_random",
        |mut caller: Caller<'_, HostState<RT>>, capability: i64| {
            if !authorize_generated_capability(caller.data_mut(), capability)? {
                return Err(WasmtimeError::new(HostInvariant));
            }
            generated_state_mut(caller.data_mut())?.count_operation()?;
            consume_generated_random_fuel(&mut caller, MATH_RANDOM_ENTROPY_BYTES)?;
            Ok(caller
                .data_mut()
                .provider
                .rng()
                .map_err(|_| WasmtimeError::new(HostInvariant))?
                .random::<f64>())
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_query_start_value",
        |mut caller: Caller<'_, HostState<RT>>, operation_id: i32, handle: i64| {
            let value = take_generated_json_value(caller.data_mut(), handle)?;
            start_generated_query(caller.data_mut(), operation_id, value)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_query_start_utf8",
        |mut caller: Caller<'_, HostState<RT>>, operation_id: i32, pointer: i32, length: i32| {
            let value = read_generated_utf8(&mut caller, pointer, length, MAX_REQUEST_BYTES)?;
            start_generated_query(caller.data_mut(), operation_id, JsonValue::String(value))
        },
    )?;

    linker.func_wrap_async(
        "convex",
        "convex_async_batch_take",
        |mut caller: Caller<'_, HostState<RT>>, (invocations_handle,): (i64,)| {
            Box::new(async move {
                let result =
                    run_generated_direct_async_batch(caller.data_mut(), invocations_handle).await?;
                let Some(result) = result else {
                    return Ok(-1);
                };
                generated_state_mut(caller.data_mut())?
                    .opaque_value_operation(|values| values.insert_json(result))
                    .map(OpaqueHandle::to_abi)
            })
        },
    )?;

    linker.func_wrap_async(
        "convex",
        "convex_function_handle_create",
        |mut caller: Caller<'_, HostState<RT>>,
         (operation_id, function_reference_handle): (i32, i64)| {
            Box::new(async move {
                let descriptor = generated_state_mut(caller.data_mut())?.operation(operation_id)?;
                let ImportedOperationDescriptor::FunctionHandleCreate {} = descriptor else {
                    return Err(WasmtimeError::new(HostInvariant));
                };
                let function_reference =
                    take_generated_json_value(caller.data_mut(), function_reference_handle)?;
                let function_address = match decode_function_address(function_reference) {
                    Ok(function_address) => function_address,
                    Err(_) => {
                        record_generated_developer_error(
                            caller.data_mut(),
                            0,
                            "Function reference must resolve to one exact address".to_owned(),
                        )?;
                        return Ok(-1);
                    },
                };
                let npm_version = caller
                    .data()
                    .provider
                    .npm_version()
                    .map_err(|_| WasmtimeError::new(HostInvariant))?
                    .clone();
                let result = run_generated_async_syscall(
                    caller.data_mut(),
                    "1.0/createFunctionHandle".to_owned(),
                    capability_function_address_syscall_args(function_address, &npm_version),
                )
                .await?;
                let Some(result) = result else {
                    return Ok(-1);
                };
                if !result.is_string() {
                    return Err(WasmtimeError::new(HostInvariant));
                }
                generated_state_mut(caller.data_mut())?
                    .opaque_value_operation(|values| values.insert_json(result))
                    .map(OpaqueHandle::to_abi)
            })
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_db_normalize_id",
        |mut caller: Caller<'_, HostState<RT>>, operation_id: i32, id_string_handle: i64| {
            run_generated_database_normalize_id(caller.data_mut(), operation_id, id_string_handle)
        },
    )?;

    linker.func_wrap_async(
        "convex",
        "convex_db_get",
        |mut caller: Caller<'_, HostState<RT>>, (operation_id, id_handle): (i32, i64)| {
            Box::new(async move {
                let descriptor = generated_state_mut(caller.data_mut())?.operation(operation_id)?;
                let ImportedOperationDescriptor::DatabaseGet { table_name } = descriptor else {
                    return Err(WasmtimeError::new(HostInvariant));
                };
                let id = take_generated_json_value(caller.data_mut(), id_handle)?;
                let result = run_generated_async_syscall(
                    caller.data_mut(),
                    "1.0/get".to_owned(),
                    json!({
                        "id": id,
                        "table": table_name,
                    }),
                )
                .await?;
                let Some(result) = result else {
                    return Ok(-1);
                };
                generated_state_mut(caller.data_mut())?
                    .opaque_value_operation(|values| values.insert_json(result))
                    .map(OpaqueHandle::to_abi)
            })
        },
    )?;

    linker.func_wrap_async(
        "convex",
        "convex_db_write",
        |mut caller: Caller<'_, HostState<RT>>,
         (operation_id, id_handle, value_handle): (i32, i64, i64)| {
            Box::new(async move {
                let descriptor = generated_state_mut(caller.data_mut())?.operation(operation_id)?;
                let (name, args, returns_id) = match descriptor {
                    ImportedOperationDescriptor::DatabaseInsert { table_name } => {
                        if id_handle != 0 {
                            return Err(WasmtimeError::new(HostInvariant));
                        }
                        let value = take_generated_json_value(caller.data_mut(), value_handle)?;
                        (
                            "1.0/insert",
                            json!({
                                "table": table_name,
                                "value": value,
                            }),
                            true,
                        )
                    },
                    ImportedOperationDescriptor::DatabasePatch { table_name } => (
                        "1.0/shallowMerge",
                        generated_database_update_args(
                            caller.data_mut(),
                            table_name,
                            id_handle,
                            value_handle,
                        )?,
                        false,
                    ),
                    ImportedOperationDescriptor::DatabaseReplace { table_name } => (
                        "1.0/replace",
                        generated_database_update_args(
                            caller.data_mut(),
                            table_name,
                            id_handle,
                            value_handle,
                        )?,
                        false,
                    ),
                    ImportedOperationDescriptor::DatabaseDelete { table_name } => {
                        if value_handle != 0 {
                            return Err(WasmtimeError::new(HostInvariant));
                        }
                        let id = take_generated_json_value(caller.data_mut(), id_handle)?;
                        (
                            "1.0/remove",
                            json!({
                                "table": table_name,
                                "id": id,
                            }),
                            false,
                        )
                    },
                    ImportedOperationDescriptor::Sha256
                    | ImportedOperationDescriptor::AuthenticationGetUserIdentity {}
                    | ImportedOperationDescriptor::HostSecretVerify { .. }
                    | ImportedOperationDescriptor::FunctionHandleCreate {}
                    | ImportedOperationDescriptor::DatabaseNormalizeId { .. }
                    | ImportedOperationDescriptor::DatabaseGet { .. }
                    | ImportedOperationDescriptor::DatabaseIndexQuery { .. }
                    | ImportedOperationDescriptor::SchedulerRunAfter { .. }
                    | ImportedOperationDescriptor::SchedulerRunAt { .. } => {
                        return Err(WasmtimeError::new(HostInvariant));
                    },
                };
                let result =
                    run_generated_async_syscall(caller.data_mut(), name.to_owned(), args).await?;
                let Some(result) = result else {
                    return Ok(-1);
                };
                if !returns_id {
                    return Ok(0);
                }
                let id = result
                    .as_object()
                    .and_then(|result| result.get("_id"))
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))?
                    .to_owned();
                generated_state_mut(caller.data_mut())?
                    .opaque_value_operation(|values| values.insert_string(id))
                    .map(OpaqueHandle::to_abi)
            })
        },
    )?;

    linker.func_wrap_async(
        "convex",
        "convex_scheduler_schedule",
        |mut caller: Caller<'_, HostState<RT>>,
         (operation_id, time_milliseconds, args_handle): (i32, f64, i64)| {
            Box::new(async move {
                let descriptor = generated_state_mut(caller.data_mut())?.operation(operation_id)?;
                let function_reference = match &descriptor {
                    ImportedOperationDescriptor::SchedulerRunAfter { function_reference }
                    | ImportedOperationDescriptor::SchedulerRunAt { function_reference } => {
                        function_reference.clone()
                    },
                    ImportedOperationDescriptor::Sha256
                    | ImportedOperationDescriptor::AuthenticationGetUserIdentity {}
                    | ImportedOperationDescriptor::HostSecretVerify { .. }
                    | ImportedOperationDescriptor::FunctionHandleCreate {}
                    | ImportedOperationDescriptor::DatabaseNormalizeId { .. }
                    | ImportedOperationDescriptor::DatabaseGet { .. }
                    | ImportedOperationDescriptor::DatabaseIndexQuery { .. }
                    | ImportedOperationDescriptor::DatabaseInsert { .. }
                    | ImportedOperationDescriptor::DatabasePatch { .. }
                    | ImportedOperationDescriptor::DatabaseReplace { .. }
                    | ImportedOperationDescriptor::DatabaseDelete { .. } => {
                        return Err(WasmtimeError::new(HostInvariant));
                    },
                };
                let args = take_generated_json_value(caller.data_mut(), args_handle)?;
                let unix_timestamp = caller
                    .data_mut()
                    .provider
                    .unix_timestamp()
                    .map_err(|_| WasmtimeError::new(HostInvariant))?;
                let timestamp = match classify_host_result(
                    caller.data_mut(),
                    scheduler_timestamp(&descriptor, time_milliseconds, unix_timestamp),
                )? {
                    Some(timestamp) => timestamp,
                    None => return Ok(-1),
                };
                let result = run_generated_async_syscall(
                    caller.data_mut(),
                    "1.0/schedule".to_owned(),
                    scheduler_syscall_args(function_reference, timestamp, args),
                )
                .await?;
                let Some(result) = result else {
                    return Ok(-1);
                };
                generated_state_mut(caller.data_mut())?
                    .opaque_value_operation(|values| values.insert_json(result))
                    .map(OpaqueHandle::to_abi)
            })
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_type",
        |caller: Caller<'_, HostState<RT>>, handle: i64| {
            let handle = opaque_handle(handle)?;
            let kind = generated_state(caller.data())?
                .values
                .json_kind(handle)
                .map_err(|_| WasmtimeError::new(HostInvariant))?;
            Ok::<i32, WasmtimeError>(match kind {
                OpaqueJsonKind::Null => 0,
                OpaqueJsonKind::Boolean => {
                    if generated_state(caller.data())?
                        .values
                        .bool_value(handle)
                        .map_err(|_| WasmtimeError::new(HostInvariant))?
                    {
                        2
                    } else {
                        1
                    }
                },
                OpaqueJsonKind::Number => 3,
                OpaqueJsonKind::String => 4,
                OpaqueJsonKind::Array => 5,
                OpaqueJsonKind::Object => 6,
            })
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_array_len",
        |caller: Caller<'_, HostState<RT>>, handle: i64| {
            let length = generated_state(caller.data())?
                .values
                .array_len(opaque_handle(handle)?)
                .map_err(|_| WasmtimeError::new(HostInvariant))?;
            i32::try_from(length).map_err(|_| WasmtimeError::new(HostInvariant))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_array_get",
        |mut caller: Caller<'_, HostState<RT>>, handle: i64, index: i32| {
            let index = usize::try_from(index).map_err(|_| WasmtimeError::new(HostInvariant))?;
            let handle = opaque_handle(handle)?;
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.clone_array_element(handle, index))
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_bool",
        |caller: Caller<'_, HostState<RT>>, handle: i64| {
            generated_state(caller.data())?
                .values
                .bool_value(opaque_handle(handle)?)
                .map(i32::from)
                .map_err(|_| WasmtimeError::new(HostInvariant))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_number",
        |caller: Caller<'_, HostState<RT>>, handle: i64| {
            generated_state(caller.data())?
                .values
                .number_value(opaque_handle(handle)?)
                .map_err(|_| WasmtimeError::new(HostInvariant))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_string_len",
        |caller: Caller<'_, HostState<RT>>, handle: i64| {
            let length = generated_state(caller.data())?
                .values
                .string_value(opaque_handle(handle)?)
                .map_err(|_| WasmtimeError::new(HostInvariant))?
                .len();
            i32::try_from(length).map_err(|_| WasmtimeError::new(HostInvariant))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_string_copy",
        |mut caller: Caller<'_, HostState<RT>>, handle: i64, pointer: i32, capacity: i32| {
            let capacity = checked_len(capacity, MAX_RESULT_BYTES)?;
            let handle = opaque_handle(handle)?;
            let length = write_guest_bytes_from_state(&mut caller, pointer, capacity, |state| {
                generated_state(state)?
                    .values
                    .string_value(handle)
                    .map(|value| value.as_bytes())
                    .map_err(|_| WasmtimeError::new(HostInvariant))
            })?;
            i32::try_from(length).map_err(|_| WasmtimeError::new(HostInvariant))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_field",
        |mut caller: Caller<'_, HostState<RT>>, handle: i64, pointer: i32, length: i32| {
            let name =
                read_generated_utf8(&mut caller, pointer, length, MAX_GENERATED_FIELD_NAME_BYTES)?;
            let handle = opaque_handle(handle)?;
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.clone_object_field(handle, &name))
                .map(|handle| handle.map_or(0, OpaqueHandle::to_abi))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_release",
        |mut caller: Caller<'_, HostState<RT>>, handle: i64| {
            let handle = opaque_handle(handle)?;
            generated_state_mut(caller.data_mut())?.opaque_value_operation(|values| {
                values.release(handle, OpaqueValueKind::ConvexJson)
            })
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_null_new",
        |mut caller: Caller<'_, HostState<RT>>| {
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(OpaqueValueTable::insert_null)
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_bool_new",
        |mut caller: Caller<'_, HostState<RT>>, value: i32| {
            let value = match value {
                0 => false,
                1 => true,
                _ => return Err(WasmtimeError::new(HostInvariant)),
            };
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.insert_bool(value))
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_number_new",
        |mut caller: Caller<'_, HostState<RT>>, value: f64| {
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.insert_f64(value))
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_string_new",
        |mut caller: Caller<'_, HostState<RT>>, pointer: i32, length: i32| {
            let value = read_generated_utf8(&mut caller, pointer, length, MAX_RESULT_BYTES)?;
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.insert_string(value))
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_array_new",
        |mut caller: Caller<'_, HostState<RT>>| {
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(OpaqueValueTable::insert_array)
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_array_push",
        |mut caller: Caller<'_, HostState<RT>>, array_handle: i64, value_handle: i64| {
            let array_handle = opaque_handle(array_handle)?;
            let value_handle = opaque_handle(value_handle)?;
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.array_push(array_handle, value_handle))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_object_new",
        |mut caller: Caller<'_, HostState<RT>>| {
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(OpaqueValueTable::insert_object)
                .map(OpaqueHandle::to_abi)
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_value_object_insert",
        |mut caller: Caller<'_, HostState<RT>>,
         object_handle: i64,
         pointer: i32,
         length: i32,
         value_handle: i64| {
            let name =
                read_generated_utf8(&mut caller, pointer, length, MAX_GENERATED_FIELD_NAME_BYTES)?;
            let object_handle = opaque_handle(object_handle)?;
            let value_handle = opaque_handle(value_handle)?;
            generated_state_mut(caller.data_mut())?.opaque_value_operation(|values| {
                values.object_insert(object_handle, name, value_handle)
            })
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_function_result",
        |mut caller: Caller<'_, HostState<RT>>, handle: i64| {
            if caller.data().developer_error.is_some()
                && !generated_state(caller.data())?.discard_after_caught_initialization_failure
            {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let handle = opaque_handle(handle)?;
            generated_state_mut(caller.data_mut())?
                .opaque_value_operation(|values| values.set_final_result(handle))
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_developer_error",
        |mut caller: Caller<'_, HostState<RT>>,
         pointer: i32,
         length: i32,
         host_operation_error_handle: i32| {
            let maximum = usize::try_from(
                generated_state(caller.data())?
                    .manifest
                    .limits()
                    .max_result_bytes(),
            )
            .map_err(|_| WasmtimeError::new(HostInvariant))?;
            let message = read_generated_utf8(&mut caller, pointer, length, maximum)?;
            if caller.data().developer_error.is_some() {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let caught_initialization_failure = generated_state(caller.data())?
                .allows_caught_official_output_chunk_initialization_failure
                && is_official_output_chunk_initialization_failure(&message);
            let state = caller.data_mut();
            let host_operation_error = if host_operation_error_handle == 0 {
                None
            } else {
                let handle = AsyncOperationHandle::from_abi(host_operation_error_handle)?;
                generated_state_mut(state)?
                    .async_operations
                    .take_host_operation_error(handle)
            };
            state.developer_error = Some(GeneratedDeveloperError {
                message,
                host_operation_error,
            });
            state.guest_developer_error = true;
            if caught_initialization_failure {
                generated_state_mut(state)?.discard_after_caught_initialization_failure = true;
            }
            Ok::<(), WasmtimeError>(())
        },
    )?;

    linker.func_wrap(
        "convex",
        "convex_has_developer_error",
        |caller: Caller<'_, HostState<RT>>| i32::from(caller.data().developer_error.is_some()),
    )?;

    linker.func_wrap(
        "convex",
        "convex_profile_mark",
        |mut caller: Caller<'_, HostState<RT>>, phase: i32| {
            let now = Instant::now();
            let fuel = caller.get_fuel().ok();
            #[cfg(any(test, feature = "testing"))]
            caller
                .data()
                .metrics
                .generated_profile_marks
                .lock()
                .push((phase, now));
            if phase == 0 {
                #[cfg(test)]
                caller
                    .data()
                    .metrics
                    .profile_marks
                    .fetch_add(1, Ordering::SeqCst);
                caller.data_mut().profile_last_mark = Some((now, fuel));
                return Ok(());
            }
            let phase = guest_phase(phase)?;
            let (previous_time, previous_fuel) = caller
                .data_mut()
                .profile_last_mark
                .replace((now, fuel))
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
            log_gate_phase(phase, now.duration_since(previous_time));
            if let (Some(previous_fuel), Some(fuel)) = (previous_fuel, fuel) {
                let consumed_fuel = previous_fuel
                    .checked_sub(fuel)
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
                // Profile marks are emitted from guest execution. Avoid the
                // generic label-map helper here because this fixed one-label
                // lookup runs for every marked guest phase.
                STATIC_HERMES_WASMTIME_GATE_GUEST_FUEL_OPERATIONS
                    .with_label_values(&[phase])
                    .observe(consumed_fuel as f64);
            }
            Ok::<(), WasmtimeError>(())
        },
    )?;

    add_generated_async_operation_imports(linker)?;
    add_generated_query_next(linker)
}

fn add_generated_query_next<RT: Runtime>(
    linker: &mut Linker<HostState<RT>>,
) -> Result<(), WasmtimeError> {
    linker.func_wrap_async(
        "convex",
        "convex_query_next",
        |mut caller: Caller<'_, HostState<RT>>, (cursor_handle,): (i64,)| {
            Box::new(async move {
                let cursor_handle = opaque_handle(cursor_handle)?;
                let query_id = match generated_state(caller.data())?
                    .values
                    .get(cursor_handle, OpaqueValueKind::QueryCursor)
                    .map_err(|_| WasmtimeError::new(HostInvariant))?
                {
                    OpaqueValue::QueryCursor(query_id) => *query_id,
                    OpaqueValue::ConvexJson(_)
                    | OpaqueValue::Bytes(_)
                    | OpaqueValue::CapabilityRequest(_) => {
                        unreachable!("validated generated cursor changed kind")
                    },
                };
                generated_state_mut(caller.data_mut())?.count_operation()?;
                let state = caller.data_mut();
                let cancellation = generated_state(state)?.cancellation.clone();
                let Some(result) = run_query_next(state, query_id, Some(cancellation)).await?
                else {
                    generated_state_mut(state)?.opaque_value_operation(|values| {
                        values.release(cursor_handle, OpaqueValueKind::QueryCursor)
                    })?;
                    return Ok(-1);
                };
                match result {
                    QueryNext::Done => {
                        generated_state_mut(state)?.opaque_value_operation(|values| {
                            values.release(cursor_handle, OpaqueValueKind::QueryCursor)
                        })?;
                        Ok(0)
                    },
                    QueryNext::Value(value) => generated_state_mut(state)?
                        .opaque_value_operation(|values| values.insert_json(value))
                        .map(OpaqueHandle::to_abi),
                }
            })
        },
    )?;
    Ok(())
}

pub(super) fn add_wasi_imports<RT: Runtime>(
    linker: &mut Linker<HostState<RT>>,
) -> Result<(), WasmtimeError> {
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "args_sizes_get",
        |mut caller: Caller<'_, HostState<RT>>, argc_pointer: i32, size_pointer: i32| {
            let argc = u32::try_from(STATIC_HERMES_WASI_ARGS.len())
                .map_err(|_| WasmtimeError::new(HostInvariant))?;
            let byte_count =
                STATIC_HERMES_WASI_ARGS
                    .iter()
                    .try_fold(0_u32, |total, argument| {
                        let length = u32::try_from(argument.len() + 1)
                            .map_err(|_| WasmtimeError::new(HostInvariant))?;
                        total
                            .checked_add(length)
                            .ok_or_else(|| WasmtimeError::new(HostInvariant))
                    })?;
            write_u32(&mut caller, argc_pointer, argc)?;
            write_u32(&mut caller, size_pointer, byte_count)?;
            Ok::<i32, WasmtimeError>(0)
        },
    )?;

    linker.func_wrap(
        "wasi_snapshot_preview1",
        "args_get",
        |mut caller: Caller<'_, HostState<RT>>, argv_pointer: i32, buffer_pointer: i32| {
            let argv_base = checked_offset(argv_pointer)?;
            let mut buffer_offset = checked_offset(buffer_pointer)?;
            for (index, argument) in STATIC_HERMES_WASI_ARGS.iter().enumerate() {
                let guest_pointer =
                    u32::try_from(buffer_offset).map_err(|_| WasmtimeError::new(HostInvariant))?;
                guest_memory(&mut caller)?
                    .write(
                        &mut caller,
                        argv_base + index * 4,
                        &guest_pointer.to_le_bytes(),
                    )
                    .map_err(WasmtimeError::from)?;
                guest_memory(&mut caller)?
                    .write(&mut caller, buffer_offset, argument)
                    .map_err(WasmtimeError::from)?;
                buffer_offset += argument.len();
                guest_memory(&mut caller)?
                    .write(&mut caller, buffer_offset, &[0])
                    .map_err(WasmtimeError::from)?;
                buffer_offset += 1;
            }
            Ok::<i32, WasmtimeError>(0)
        },
    )?;

    linker.func_wrap(
        "wasi_snapshot_preview1",
        "environ_sizes_get",
        |mut caller: Caller<'_, HostState<RT>>, count_pointer: i32, size_pointer: i32| {
            write_u32(&mut caller, count_pointer, 0)?;
            write_u32(&mut caller, size_pointer, 0)?;
            Ok::<i32, WasmtimeError>(0)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "environ_get",
        |_caller: Caller<'_, HostState<RT>>, _environ_pointer: i32, _buffer_pointer: i32| 0_i32,
    )?;
    linker.func_wrap(
        "env",
        "emscripten_notify_memory_growth",
        |_caller: Caller<'_, HostState<RT>>, _memory_index: i32| {},
    )?;
    linker.func_wrap(
        "env",
        "__syscall_unlinkat",
        |_caller: Caller<'_, HostState<RT>>, _directory_fd: i32, _path: i32, _flags: i32| -2_i32,
    )?;
    // Emscripten probes these filesystem operations while preparing the
    // runtime. The guest has no filesystem, so report ENOENT like unlinkat.
    linker.func_wrap(
        "env",
        "__syscall_faccessat",
        |_caller: Caller<'_, HostState<RT>>,
         _directory_fd: i32,
         _path: i32,
         _mode: i32,
         _flags: i32| { -2_i32 },
    )?;
    linker.func_wrap(
        "env",
        "__syscall_getcwd",
        |_caller: Caller<'_, HostState<RT>>, _buffer: i32, _size: i32| -2_i32,
    )?;
    linker.func_wrap(
        "env",
        "__syscall_readlinkat",
        |_caller: Caller<'_, HostState<RT>>,
         _directory_fd: i32,
         _path: i32,
         _buffer: i32,
         _size: i32| { -2_i32 },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_close",
        |_caller: Caller<'_, HostState<RT>>, fd: i32| {
            if (0..=2).contains(&fd) {
                0
            } else {
                8
            }
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_fdstat_get",
        |mut caller: Caller<'_, HostState<RT>>, fd: i32, stat_pointer: i32| {
            if !(0..=2).contains(&fd) {
                return Ok::<i32, WasmtimeError>(8);
            }
            let mut stat = [0; 24];
            stat[0] = 2;
            write_guest_bytes(&mut caller, stat_pointer, &stat)?;
            Ok::<i32, WasmtimeError>(0)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_seek",
        |mut caller: Caller<'_, HostState<RT>>,
         _fd: i32,
         _offset: i64,
         _whence: i32,
         result_pointer: i32| {
            write_u64(&mut caller, result_pointer, 0)?;
            Ok::<i32, WasmtimeError>(8)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_read",
        |mut caller: Caller<'_, HostState<RT>>,
         _fd: i32,
         _iovec_pointer: i32,
         _iovec_count: i32,
         read_pointer: i32| {
            write_u32(&mut caller, read_pointer, 0)?;
            Ok::<i32, WasmtimeError>(0)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_pread",
        |mut caller: Caller<'_, HostState<RT>>,
         _fd: i32,
         _iovec_pointer: i32,
         _iovec_count: i32,
         _offset: i64,
         read_pointer: i32| {
            write_u32(&mut caller, read_pointer, 0)?;
            Ok::<i32, WasmtimeError>(8)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "fd_write",
        |mut caller: Caller<'_, HostState<RT>>,
         fd: i32,
         iovec_pointer: i32,
         iovec_count: i32,
         written_pointer: i32| {
            if !matches!(fd, 1 | 2) {
                return Ok::<i32, WasmtimeError>(8);
            }
            let count = checked_len(iovec_count, 128)?;
            let iovec_base = checked_offset(iovec_pointer)?;
            let mut bytes = Vec::new();
            for index in 0..count {
                let entry = iovec_base + index * 8;
                let data_pointer = usize::try_from(read_u32(&mut caller, entry)?)
                    .map_err(|_| WasmtimeError::new(HostInvariant))?;
                let data_length = usize::try_from(read_u32(&mut caller, entry + 4)?)
                    .map_err(|_| WasmtimeError::new(HostInvariant))?;
                let pending_bytes = bytes
                    .len()
                    .checked_add(data_length)
                    .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
                // Bound the retained streams together and check before growing
                // the temporary buffer, so repeated writes cannot bypass the
                // per-invocation retained-output byte ceiling.
                ensure_wasi_output_within_limit(
                    caller.data().stdout.len(),
                    caller.data().stderr.len(),
                    pending_bytes,
                )?;
                let start = bytes.len();
                bytes.resize(start + data_length, 0);
                guest_memory(&mut caller)?
                    .read(&caller, data_pointer, &mut bytes[start..])
                    .map_err(WasmtimeError::from)?;
            }
            match fd {
                1 => caller.data_mut().stdout.extend_from_slice(&bytes),
                2 => caller.data_mut().stderr.extend_from_slice(&bytes),
                _ => unreachable!("WASI output descriptor was validated before reading"),
            }
            write_u32(
                &mut caller,
                written_pointer,
                u32::try_from(bytes.len()).map_err(|_| WasmtimeError::new(HostInvariant))?,
            )?;
            Ok::<i32, WasmtimeError>(0)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "clock_time_get",
        |mut caller: Caller<'_, HostState<RT>>,
         clock_id: i32,
         _precision: i64,
         result_pointer: i32| {
            // Runtime construction reads WASI clocks before invocation
            // authority is issued. Only active realtime reads count as an
            // invocation time observation.
            let execution_is_active = generated_state(caller.data())?.interrupt.is_active();
            let nanos = match clock_id {
                WASI_CLOCK_ID_REALTIME => {
                    // WASI realtime shares Date.now's fixed, millisecond-precision
                    // invocation clock.
                    let milliseconds = caller
                        .data()
                        .provider
                        .invocation_unix_timestamp()
                        .map_err(|_| WasmtimeError::new(HostInvariant))?
                        .as_ms_since_epoch()
                        .map_err(|_| WasmtimeError::new(HostInvariant))?;
                    if milliseconds > MAX_EXACT_JAVASCRIPT_INTEGER {
                        return Err(WasmtimeError::new(HostInvariant));
                    }
                    milliseconds
                        .checked_mul(NANOS_PER_MILLISECOND)
                        .ok_or_else(|| WasmtimeError::new(HostInvariant))?
                },
                WASI_CLOCK_ID_MONOTONIC => {
                    let duration = caller
                        .data()
                        .provider
                        .rt()
                        .monotonic_now()
                        .checked_duration_since(caller.data().wasi_monotonic_epoch)
                        .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
                    u64::try_from(duration.as_nanos())
                        .map_err(|_| WasmtimeError::new(HostInvariant))?
                },
                _ => return Ok::<i32, WasmtimeError>(WASI_ERRNO_NOTSUP),
            };
            write_u64(&mut caller, result_pointer, nanos)?;
            if clock_id == WASI_CLOCK_ID_REALTIME && execution_is_active {
                caller
                    .data_mut()
                    .provider
                    .observe_time()
                    .map_err(|_| WasmtimeError::new(HostInvariant))?;
            }
            Ok::<i32, WasmtimeError>(0)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "random_get",
        |mut caller: Caller<'_, HostState<RT>>, pointer: i32, length: i32| {
            // Runtime construction uses this deterministic compatibility
            // source before capability-scoped invocation randomness exists.
            let length = checked_len(length, MAX_RESULT_BYTES)?;
            let start = checked_offset(pointer)?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?;
            let memory = guest_memory(&mut caller)?;
            memory
                .data_mut(&mut caller)
                .get_mut(start..end)
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?
                .fill(0x5a);
            Ok::<i32, WasmtimeError>(0)
        },
    )?;
    linker.func_wrap(
        "wasi_snapshot_preview1",
        "proc_exit",
        |_caller: Caller<'_, HostState<RT>>, status: i32| -> Result<(), WasmtimeError> {
            Err(WasmtimeError::new(WasiExit(status)))
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ensure_wasi_output_within_limit;
    use crate::environment::udf::static_hermes_wasmtime_gate::MAX_RESULT_BYTES;

    #[test]
    fn wasi_output_limit_is_cumulative_across_streams_and_writes() {
        assert!(ensure_wasi_output_within_limit(0, 0, MAX_RESULT_BYTES).is_ok());
        assert!(ensure_wasi_output_within_limit(MAX_RESULT_BYTES - 2, 1, 1).is_ok());
        assert!(ensure_wasi_output_within_limit(MAX_RESULT_BYTES - 1, 1, 1).is_err());
        assert!(ensure_wasi_output_within_limit(usize::MAX, 1, 0).is_err());
    }
}
