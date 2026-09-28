#![cfg(test)]

use sha2::{
    Digest,
    Sha256,
};
use wasmtime::{
    Config,
    Engine,
    Instance,
    Module,
    Precompiled,
    ProfilingStrategy,
    Store,
};

const PRECOMPILED_MODULES: [(u64, &[u8], &str); 2] = [
    (
        0,
        include_bytes!(concat!(env!("OUT_DIR"), "/ordinary.cwasm")),
        env!("CWASM_ORDINARY_SHA256"),
    ),
    (
        100_000,
        include_bytes!(concat!(env!("OUT_DIR"), "/polling.cwasm")),
        env!("CWASM_POLLING_SHA256"),
    ),
];
const TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";

#[test]
fn authenticated_precompiled_module_executes_without_a_target_compiler(
) -> Result<(), Box<dyn std::error::Error>> {
    for (interval, bytes, sha256) in &PRECOMPILED_MODULES {
        // The detector reads the ELF header through an aligned view. Keep the
        // artifact bytes exact while giving it the same alignment as runtime-owned
        // package buffers.
        let precompiled_module = bytes.to_vec();
        let actual_sha256 = format!("{:x}", Sha256::digest(&precompiled_module));
        assert_eq!(actual_sha256, *sha256);
        assert_eq!(
            Engine::detect_precompiled(&precompiled_module),
            Some(Precompiled::Module),
            "authenticated fixture is not a precompiled core module",
        );

        let mut config = Config::new();
        config.target(TARGET_TRIPLE)?;
        config
            .consume_fuel(true)
            .epoch_interruption(true)
            .fuel_epoch_poll_interval(*interval)
            // build.rs precompiles this artifact with PerfMap enabled. Loading it with no runtime
            // profiler proves that profiling is not part of Wasmtime's AOT compatibility boundary.
            .profiler(ProfilingStrategy::None)
            .wasm_exceptions(true);
        let engine = Engine::new(&config)?;
        for (other_interval, other_bytes, other_sha256) in &PRECOMPILED_MODULES {
            if other_interval == interval {
                continue;
            }
            let other = other_bytes.to_vec();
            assert_eq!(format!("{:x}", Sha256::digest(&other)), *other_sha256);
            // Both fixtures are locally produced and authenticated, even when their
            // metering configurations are deliberately incompatible.
            assert!(unsafe { Module::deserialize(&engine, &other) }.is_err());
        }
        let module = unsafe {
            // The SHA-256 check above authenticates the exact bytes before Wasmtime maps
            // them.
            Module::deserialize(&engine, &precompiled_module)
        }?;
        let mut store = Store::new(&engine, ());
        store.set_fuel(10_000)?;
        store.set_epoch_deadline(1);
        let instance = Instance::new(&mut store, &module, &[])?;
        let answer = instance.get_typed_func::<(), i32>(&mut store, "answer")?;
        assert_eq!(answer.call(&mut store, ())?, 42);
        assert_eq!(store.get_fuel()?, 9_998);
    }
    Ok(())
}
