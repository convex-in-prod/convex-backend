use super::*;

#[test]
fn host_copies_and_randomness_preserve_bounded_cow_exposure() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("bounded_host_writes", async {
        let database = new_test_database(rt.clone()).await?;
        let engine = shared_generated_engine(WasmMeteringMode::EpochChecks)?;
        let module = Module::new(
            engine.as_ref(),
            r#"(module
            (import "convex" "convex_guest_value_request_copy"
                (func $request (param i32 i32) (result i32)))
            (import "convex" "convex_guest_value_payload_copy"
                (func $payload (param i64 i32 i32) (result i32)))
            (import "convex" "convex_value_string_copy"
                (func $string (param i64 i32 i32) (result i32)))
            (import "convex" "convex_crypto_get_random_values"
                (func $random (param i64 i32 i32)))
            (import "convex" "convex_crypto_random_uuid"
                (func $uuid (param i64 i32 i32)))
            (import "wasi_snapshot_preview1" "random_get"
                (func $wasi_random (param i32 i32) (result i32)))
            (memory (export "memory") 1 1)
            (func (export "request") (param i32 i32) (result i32)
                local.get 0 local.get 1 call $request)
            (func (export "payload") (param i64 i32 i32) (result i32)
                local.get 0 local.get 1 local.get 2 call $payload)
            (func (export "string") (param i64 i32 i32) (result i32)
                local.get 0 local.get 1 local.get 2 call $string)
            (func (export "random") (param i64 i32 i32)
                local.get 0 local.get 1 local.get 2 call $random)
            (func (export "uuid") (param i64 i32 i32)
                local.get 0 local.get 1 local.get 2 call $uuid)
            (func (export "wasi_random") (param i32 i32) (result i32)
                local.get 0 local.get 1 call $wasi_random))"#,
        )?;
        let mut manifest = serde_json::to_value(
            generated_guest_promise_test_manifest_with_runtime_limits(
                ManifestUdfKind::Query,
                Vec::new(),
                65536,
                1_000_000,
                16,
            )?
            .as_ref(),
        )?;
        manifest["valueMode"] = serde_json::to_value(ValueMode::GuestNativeJson)?;
        let manifest = Arc::new(WasmUdfExecutionManifest::parse_for_runtime(
            &serde_json::to_vec(&manifest)?,
            &generated_runtime_compatibility(
                GENERATED_ENGINE_COMPATIBILITY_SHA256,
                WasmMeteringMode::EpochChecks,
            )?,
        )?);

        // Denial poisons the image view, so exercise each write boundary in
        // a separate Store. All must return an error without writing or panicking.
        for (denied_import, private_allowance) in [
            ("copy", 3 * 4096),
            ("copy", 4 * 4096),
            ("random", 3 * 4096),
            ("uuid", 3 * 4096),
            ("wasi_random", 3 * 4096),
        ] {
            let mut state = generated_test_state(
                rt.clone(),
                database.begin_system().await?,
                Arc::clone(&manifest),
                json!({"value": 7}),
                UdfType::Query,
                UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                CancellationSignal::new_for_test(),
                None,
                Arc::new(GateMetrics::default()),
                None,
            )
            .await?;
            let timeout = state.timeout.as_mut().context("missing test timeout")?;
            state.provider.prepare_execution(timeout).await?;
            let generated = generated_state_mut(&mut state)?;
            generated.capability_bridge.issue()?;
            let capability = generated.capability_bridge.handle()?;
            let request_bytes = generated
                .values
                .bytes_value(generated.request_handle.context("missing request")?)?
                .to_vec();
            let payload = generated
                .values
                .insert(OpaqueValue::Bytes(vec![11, 22, 33]))?
                .to_abi();
            let denied_payload = generated
                .values
                .insert(OpaqueValue::Bytes(vec![0xa1; 4099]))?
                .to_abi();
            let empty = generated
                .values
                .insert(OpaqueValue::Bytes(Vec::new()))?
                .to_abi();
            let string = generated
                .values
                .insert(OpaqueValue::ConvexJson(json!("hello")))?
                .to_abi();
            let mut linker = Linker::new(engine.as_ref());
            add_generated_convex_imports(&mut linker).map_err(wasmtime_anyhow)?;
            add_wasi_imports(&mut linker).map_err(wasmtime_anyhow)?;
            // Declare the image first so it also outlives Store teardown on an
            // early error or assertion failure.
            let image = initialized_memory::InitializedMemoryImage::capture(&vec![0x55; 65536])?;
            let mut store = Store::new(engine.as_ref(), state);
            store.set_fuel(1_000_000).map_err(wasmtime_anyhow)?;
            store.epoch_deadline_trap();
            store.set_epoch_deadline(u64::MAX / 2);
            let instance = linker.instantiate_async(&mut store, &module).await?;
            let memory = instance
                .get_memory(&mut store, "memory")
                .context("missing memory")?;
            let request = instance.get_typed_func::<(i32, i32), i32>(&mut store, "request")?;
            let copy = instance.get_typed_func::<(i64, i32, i32), i32>(&mut store, "payload")?;
            let text = instance.get_typed_func::<(i64, i32, i32), i32>(&mut store, "string")?;
            let random = instance.get_typed_func::<(i64, i32, i32), ()>(&mut store, "random")?;
            let uuid = instance.get_typed_func::<(i64, i32, i32), ()>(&mut store, "uuid")?;
            let wasi_random =
                instance.get_typed_func::<(i32, i32), i32>(&mut store, "wasi_random")?;
            // Real mapping hooks enforce the allowance against sixteen host
            // pages. The extra copy case leaves one page of unused allowance.
            // SAFETY: this idle Store owns the only memory, with no outstanding
            // memory borrow. It is dropped before the image on every exit path.
            let pages = unsafe {
                image.attach(
                    &mut store,
                    memory,
                    private_allowance,
                    private_allowance,
                    None,
                )?
            };

            assert!(request
                .call_async(&mut store, (64, 0))
                .await
                .unwrap_err()
                .is::<HostInvariant>());
            assert!(copy
                .call_async(&mut store, (payload, 65535, 3))
                .await
                .unwrap_err()
                .is::<HostInvariant>());
            assert!(copy
                .call_async(&mut store, (0, 64, 3))
                .await
                .unwrap_err()
                .is::<HostInvariant>());
            assert!(random
                .call_async(&mut store, (capability, 65535, 2))
                .await
                .unwrap_err()
                .is::<HostInvariant>());
            assert!(uuid
                .call_async(&mut store, (capability, 65535, 36))
                .await
                .unwrap_err()
                .is::<HostInvariant>());
            assert!(wasi_random
                .call_async(&mut store, (65535, 2))
                .await
                .unwrap_err()
                .is::<HostInvariant>());
            assert_eq!(copy.call_async(&mut store, (empty, 65536, 0)).await?, 0);
            random
                .call_async(&mut store, (capability, 65536, 0))
                .await?;
            assert_eq!(wasi_random.call_async(&mut store, (65536, 0)).await?, 0);
            assert_eq!(pages.observation().exposed_bytes, 0);

            assert_eq!(
                request.call_async(&mut store, (64, 4096)).await?,
                request_bytes.len() as i32
            );
            assert_eq!(
                &memory.data(&store)[64..64 + request_bytes.len()],
                request_bytes.as_slice()
            );
            assert_eq!(pages.observation().exposed_bytes, 4096);
            assert_eq!(copy.call_async(&mut store, (payload, 4095, 3)).await?, 3);
            assert_eq!(&memory.data(&store)[4095..4098], &[11, 22, 33]);
            assert_eq!(text.call_async(&mut store, (string, 4200, 5)).await?, 5);
            assert_eq!(&memory.data(&store)[4200..4205], b"hello");
            assert_eq!(pages.observation().exposed_bytes, 2 * 4096);
            random.call_async(&mut store, (capability, 8192, 7)).await?;
            let mut expected = [0u8; 7];
            let mut expected_rng = ChaCha12Rng::from_seed([0; 32]);
            expected_rng.fill(expected.as_mut_slice());
            assert_eq!(&memory.data(&store)[8192..8199], &expected);
            uuid.call_async(&mut store, (capability, 8300, 36)).await?;
            let expected_uuid = uuid::Builder::from_random_bytes(expected_rng.random())
                .into_uuid()
                .to_string();
            assert_eq!(&memory.data(&store)[8300..8336], expected_uuid.as_bytes());
            assert_eq!(wasi_random.call_async(&mut store, (8400, 7)).await?, 0);
            assert_eq!(&memory.data(&store)[8400..8407], &[0x5a; 7]);
            assert_eq!(pages.observation().exposed_bytes, 3 * 4096);

            // The copy starts in an exposed page and needs two new pages.
            // Admission must reject the whole range with zero or one page left,
            // before copying even its already-writable prefix.
            let failure = match denied_import {
                "copy" => copy
                    .call_async(&mut store, (denied_payload, 12287, 4099))
                    .await
                    .unwrap_err(),
                "random" => random
                    .call_async(&mut store, (capability, 12288, 1))
                    .await
                    .unwrap_err(),
                "uuid" => uuid
                    .call_async(&mut store, (capability, 12288, 36))
                    .await
                    .unwrap_err(),
                "wasi_random" => wasi_random
                    .call_async(&mut store, (12288, 1))
                    .await
                    .unwrap_err(),
                _ => unreachable!(),
            };
            assert!(failure
                .downcast_ref::<wasmtime::MemoryAccessError>()
                .is_some_and(wasmtime::MemoryAccessError::is_write_denied));
            assert_eq!(pages.observation().exposed_bytes, 3 * 4096);
            assert_eq!(pages.observation().denied_writes, 1);
            assert!(pages.observation().poisoned);
            assert!(memory.data(&store)[12287..]
                .iter()
                .all(|&byte| byte == 0x55));
            drop(store);
        }
        database.shutdown().await
    })
}
