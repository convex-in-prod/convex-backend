use std::{
    env,
    fs,
    path::PathBuf,
};

use sha2::{
    Digest,
    Sha256,
};
use wasmtime::{
    Config,
    Engine,
    ProfilingStrategy,
};

const MODULE: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f, 0x03,
    0x02, 0x01, 0x00, 0x07, 0x0a, 0x01, 0x06, 0x61, 0x6e, 0x73, 0x77, 0x65, 0x72, 0x00, 0x00, 0x0a,
    0x06, 0x01, 0x04, 0x00, 0x41, 0x2a, 0x0b,
];
const TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";

fn main() {
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo must set OUT_DIR"));
    for (name, interval) in [("ordinary", 0), ("polling", 100_000)] {
        let mut config = Config::new();
        config
            .target(TARGET_TRIPLE)
            .expect("compiler target configuration must be valid");
        config
            .consume_fuel(true)
            .epoch_interruption(true)
            .fuel_epoch_poll_interval(interval)
            .profiler(ProfilingStrategy::PerfMap)
            .wasm_exceptions(true);
        let engine = Engine::new(&config).expect("compiler engine configuration must be valid");
        let cwasm = engine
            .precompile_module(MODULE)
            .expect("fixture module must precompile");
        fs::write(output.join(format!("{name}.cwasm")), &cwasm)
            .expect("precompiled fixture must be writable");
        println!(
            "cargo:rustc-env=CWASM_{}_SHA256={:x}",
            name.to_uppercase(),
            Sha256::digest(&cwasm)
        );
    }
}
