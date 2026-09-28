use std::{
    env,
    fs,
    path::Path,
    process::Command,
    sync::Arc,
};

use serde_json::{
    json,
    Value as JsonValue,
};
use tempfile::TempDir;
use wasm_encoder::MemArg as EncodedMemArg;

use super::*;
use crate::environment::udf::{
    module_graph_registry::materialize_registry_v5_test_fixture,
    wasm_udf_package::{
        load_runtime_registry_current,
        load_runtime_registry_generation,
    },
};

const PRODUCER_FIXTURE_BUILDER_ENV: &str = "CONVEX_WASM_MODULE_GRAPH_PRODUCER_FIXTURE_BUILDER";
const EXPECTED_RUN_RESULT: i32 = 4351;

fn memory_type() -> EncodedMemoryType {
    EncodedMemoryType {
        minimum: 1,
        maximum: Some(2),
        memory64: false,
        shared: false,
        page_size_log2: None,
    }
}

fn table_type() -> EncodedTableType {
    EncodedTableType {
        element_type: EncodedRefType::FUNCREF,
        table64: false,
        minimum: 10,
        maximum: Some(100),
        shared: false,
    }
}

fn global_type(mutable: bool) -> EncodedGlobalType {
    EncodedGlobalType {
        val_type: EncodedValType::I32,
        mutable,
        shared: false,
    }
}

fn memory_word() -> EncodedMemArg {
    EncodedMemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }
}

fn address_with_offset(function: &mut EncodedFunction, base_global: u32, offset: i32) {
    function.instruction(&EncodedInstruction::GlobalGet(base_global));
    if offset != 0 {
        function.instruction(&EncodedInstruction::I32Const(offset));
        function.instruction(&EncodedInstruction::I32Add);
    }
}

fn store_constant_at_absolute(function: &mut EncodedFunction, address: i32, value: i32) {
    function.instruction(&EncodedInstruction::I32Const(address));
    function.instruction(&EncodedInstruction::I32Const(value));
    function.instruction(&EncodedInstruction::I32Store(memory_word()));
}

fn store_constant_at_offset(
    function: &mut EncodedFunction,
    base_global: u32,
    offset: i32,
    value: i32,
) {
    address_with_offset(function, base_global, offset);
    function.instruction(&EncodedInstruction::I32Const(value));
    function.instruction(&EncodedInstruction::I32Store(memory_word()));
}

fn store_direct_call_from_memory_at_offset(
    function: &mut EncodedFunction,
    base_global: u32,
    offset: i32,
    argument_address: impl Fn(&mut EncodedFunction),
    function_index: u32,
) {
    address_with_offset(function, base_global, offset);
    argument_address(function);
    function.instruction(&EncodedInstruction::I32Load(memory_word()));
    function.instruction(&EncodedInstruction::Call(function_index));
    function.instruction(&EncodedInstruction::I32Store(memory_word()));
}

fn store_indirect_call_at_offset(
    function: &mut EncodedFunction,
    base_global: u32,
    offset: i32,
    argument: i32,
    table_index_global: u32,
) {
    address_with_offset(function, base_global, offset);
    function.instruction(&EncodedInstruction::I32Const(argument));
    function.instruction(&EncodedInstruction::GlobalGet(table_index_global));
    function.instruction(&EncodedInstruction::CallIndirect {
        type_index: 0,
        table_index: 0,
    });
    function.instruction(&EncodedInstruction::I32Store(memory_word()));
}

fn store_global_at_offset(
    function: &mut EncodedFunction,
    base_global: u32,
    offset: i32,
    value_global: u32,
) {
    address_with_offset(function, base_global, offset);
    function.instruction(&EncodedInstruction::GlobalGet(value_global));
    function.instruction(&EncodedInstruction::I32Store(memory_word()));
}

fn base_module() -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function([EncodedValType::I32], [EncodedValType::I32]);
    types.ty().function([], []);
    types
        .ty()
        .function([EncodedValType::I64], [EncodedValType::I32]);
    types.ty().function([], [EncodedValType::I32]);

    let mut functions = EncodedFunctionSection::new();
    for type_index in [0, 0, 1, 1, 2, 3, 3, 1] {
        functions.function(type_index);
    }
    let mut tables = EncodedTableSection::new();
    tables.table(table_type());
    let mut memories = EncodedMemorySection::new();
    memories.memory(memory_type());
    let mut globals = EncodedGlobalSection::new();
    globals.global(global_type(true), &EncodedConstExpr::i32_const(512));
    globals.global(global_type(false), &EncodedConstExpr::i32_const(1024));
    globals.global(global_type(false), &EncodedConstExpr::i32_const(17));
    globals.global(global_type(true), &EncodedConstExpr::i32_const(1024));
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
    exports.export("__indirect_function_table", EncodedExportKind::Table, 0);
    exports.export("__stack_pointer", EncodedExportKind::Global, 0);
    exports.export("__heap_base", EncodedExportKind::Global, 1);
    exports.export("base_data", EncodedExportKind::Global, 2);
    exports.export("sbrk", EncodedExportKind::Func, 0);
    exports.export("base_value", EncodedExportKind::Func, 1);
    exports.export("__wasm_apply_data_relocs", EncodedExportKind::Func, 2);
    exports.export("_initialize", EncodedExportKind::Func, 3);
    exports.export("convex_wasm_graph_select_entry", EncodedExportKind::Func, 4);
    exports.export(
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        5,
    );
    exports.export("convex_wasm_udf_run", EncodedExportKind::Func, 6);
    exports.export(
        "convex_wasm_udf_destroy_runtime",
        EncodedExportKind::Func,
        7,
    );

    let mut code = EncodedCodeSection::new();
    let mut sbrk = EncodedFunction::new([(1, EncodedValType::I32)]);
    sbrk.instruction(&EncodedInstruction::GlobalGet(3));
    sbrk.instruction(&EncodedInstruction::LocalSet(1));
    sbrk.instruction(&EncodedInstruction::GlobalGet(3));
    sbrk.instruction(&EncodedInstruction::LocalGet(0));
    sbrk.instruction(&EncodedInstruction::I32Add);
    sbrk.instruction(&EncodedInstruction::GlobalSet(3));
    sbrk.instruction(&EncodedInstruction::LocalGet(1));
    sbrk.instruction(&EncodedInstruction::End);
    code.function(&sbrk);

    let mut base_value = EncodedFunction::new([]);
    base_value.instruction(&EncodedInstruction::LocalGet(0));
    base_value.instruction(&EncodedInstruction::I32Const(10));
    base_value.instruction(&EncodedInstruction::I32Add);
    base_value.instruction(&EncodedInstruction::End);
    code.function(&base_value);

    let mut relocations = EncodedFunction::new([]);
    store_constant_at_absolute(&mut relocations, 100, 1);
    relocations.instruction(&EncodedInstruction::End);
    code.function(&relocations);

    let mut initialize = EncodedFunction::new([]);
    store_constant_at_absolute(&mut initialize, 104, 2);
    initialize.instruction(&EncodedInstruction::End);
    code.function(&initialize);

    let mut select = EncodedFunction::new([]);
    select.instruction(&EncodedInstruction::LocalGet(0));
    select.instruction(&EncodedInstruction::GlobalSet(4));
    select.instruction(&EncodedInstruction::I32Const(0));
    select.instruction(&EncodedInstruction::End);
    code.function(&select);

    let mut prepare = EncodedFunction::new([]);
    prepare.instruction(&EncodedInstruction::I32Const(0));
    prepare.instruction(&EncodedInstruction::End);
    code.function(&prepare);

    let mut run = EncodedFunction::new([]);
    for address in [
        100, 104, 1024, 1028, 1032, 1036, 1040, 1044, 1048, 1052, 1056, 1060, 1064, 1068, 1072,
        1076, 1080,
    ] {
        run.instruction(&EncodedInstruction::I32Const(address));
        run.instruction(&EncodedInstruction::I32Load(memory_word()));
    }
    for _ in 1..17 {
        run.instruction(&EncodedInstruction::I32Add);
    }
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::I32Add);
    run.instruction(&EncodedInstruction::TableSize(0));
    run.instruction(&EncodedInstruction::I32Add);
    run.instruction(&EncodedInstruction::GlobalGet(4));
    run.instruction(&EncodedInstruction::I64Eqz);
    run.instruction(&EncodedInstruction::I32Eqz);
    run.instruction(&EncodedInstruction::I32Add);
    run.instruction(&EncodedInstruction::End);
    code.function(&run);

    let mut destroy = EncodedFunction::new([]);
    destroy.instruction(&EncodedInstruction::End);
    code.function(&destroy);

    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&functions)
        .section(&tables)
        .section(&memories)
        .section(&globals)
        .section(&exports)
        .section(&code);
    module.finish()
}

fn side_imports(imports: &mut EncodedImportSection, common: bool) {
    imports.import("env", "memory", EncodedEntityType::Memory(memory_type()));
    imports.import(
        "env",
        "__indirect_function_table",
        EncodedEntityType::Table(table_type()),
    );
    imports.import(
        "env",
        "__stack_pointer",
        EncodedEntityType::Global(global_type(true)),
    );
    imports.import(
        "env",
        "__memory_base",
        EncodedEntityType::Global(global_type(false)),
    );
    imports.import(
        "env",
        "__table_base",
        EncodedEntityType::Global(global_type(false)),
    );
    imports.import(
        "env",
        if common { "base_value" } else { "common_value" },
        EncodedEntityType::Function(0),
    );
    if common {
        imports.import(
            "GOT.func",
            "base_value",
            EncodedEntityType::Global(global_type(true)),
        );
        imports.import(
            "GOT.func",
            "common_value",
            EncodedEntityType::Global(global_type(true)),
        );
        imports.import(
            "GOT.mem",
            "common_data",
            EncodedEntityType::Global(global_type(true)),
        );
        imports.import(
            "GOT.func",
            "missing_weak",
            EncodedEntityType::Global(global_type(true)),
        );
    } else {
        imports.import(
            "GOT.func",
            "common_value",
            EncodedEntityType::Global(global_type(true)),
        );
        imports.import(
            "GOT.mem",
            "common_data",
            EncodedEntityType::Global(global_type(true)),
        );
        imports.import(
            "GOT.mem",
            "missing_weak",
            EncodedEntityType::Global(global_type(true)),
        );
    }
}

fn common_module() -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function([EncodedValType::I32], [EncodedValType::I32]);
    types.ty().function([], []);
    let mut imports = EncodedImportSection::new();
    side_imports(&mut imports, true);
    let mut functions = EncodedFunctionSection::new();
    functions.function(0);
    functions.function(1);
    functions.function(1);
    let mut globals = EncodedGlobalSection::new();
    globals.global(global_type(false), &EncodedConstExpr::global_get(1));
    let mut exports = EncodedExportSection::new();
    exports.export("common_data", EncodedExportKind::Global, 7);
    exports.export("common_value", EncodedExportKind::Func, 1);
    exports.export("__wasm_apply_data_relocs", EncodedExportKind::Func, 2);
    exports.export("__wasm_call_ctors", EncodedExportKind::Func, 3);

    let mut code = EncodedCodeSection::new();
    let mut common_value = EncodedFunction::new([]);
    common_value.instruction(&EncodedInstruction::LocalGet(0));
    common_value.instruction(&EncodedInstruction::Call(0));
    common_value.instruction(&EncodedInstruction::I32Const(20));
    common_value.instruction(&EncodedInstruction::I32Add);
    common_value.instruction(&EncodedInstruction::End);
    code.function(&common_value);
    let mut relocations = EncodedFunction::new([]);
    store_constant_at_offset(&mut relocations, 1, 0, 3);
    relocations.instruction(&EncodedInstruction::End);
    code.function(&relocations);
    let mut constructors = EncodedFunction::new([]);
    store_direct_call_from_memory_at_offset(
        &mut constructors,
        1,
        4,
        |function| address_with_offset(function, 1, 0),
        0,
    );
    store_indirect_call_at_offset(&mut constructors, 1, 8, 7, 3);
    store_indirect_call_at_offset(&mut constructors, 1, 12, 5, 4);
    for (offset, global_index) in [(16, 5), (20, 6), (24, 0), (28, 2)] {
        store_global_at_offset(&mut constructors, 1, offset, global_index);
    }
    constructors.instruction(&EncodedInstruction::End);
    code.function(&constructors);

    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&globals)
        .section(&exports)
        .section(&code);
    module.finish()
}

fn leaf_module() -> Vec<u8> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function([EncodedValType::I32], [EncodedValType::I32]);
    types.ty().function([], []);
    let mut imports = EncodedImportSection::new();
    side_imports(&mut imports, false);
    let mut functions = EncodedFunctionSection::new();
    functions.function(0);
    functions.function(1);
    functions.function(1);
    let mut exports = EncodedExportSection::new();
    exports.export("leaf_value", EncodedExportKind::Func, 1);
    exports.export("__wasm_apply_data_relocs", EncodedExportKind::Func, 2);
    exports.export("__wasm_call_ctors", EncodedExportKind::Func, 3);

    let mut code = EncodedCodeSection::new();
    let mut leaf_value = EncodedFunction::new([]);
    leaf_value.instruction(&EncodedInstruction::LocalGet(0));
    leaf_value.instruction(&EncodedInstruction::Call(0));
    leaf_value.instruction(&EncodedInstruction::I32Const(30));
    leaf_value.instruction(&EncodedInstruction::I32Add);
    leaf_value.instruction(&EncodedInstruction::End);
    code.function(&leaf_value);
    let mut relocations = EncodedFunction::new([]);
    store_constant_at_offset(&mut relocations, 1, 0, 4);
    relocations.instruction(&EncodedInstruction::End);
    code.function(&relocations);
    let mut constructors = EncodedFunction::new([]);
    store_direct_call_from_memory_at_offset(
        &mut constructors,
        1,
        4,
        |function| {
            function.instruction(&EncodedInstruction::I32Const(1028));
        },
        0,
    );
    store_indirect_call_at_offset(&mut constructors, 1, 8, 5, 3);
    for (offset, global_index) in [(12, 4), (16, 5), (20, 0), (24, 2)] {
        store_global_at_offset(&mut constructors, 1, offset, global_index);
    }
    constructors.instruction(&EncodedInstruction::End);
    code.function(&constructors);

    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&exports)
        .section(&code);
    module.finish()
}

fn write_execution_material(root: &Path, engine: &Engine) -> anyhow::Result<()> {
    for (role, wasm) in [
        ("base", base_module()),
        ("common", common_module()),
        ("leaf", leaf_module()),
    ] {
        Module::new(engine, &wasm)
            .map_err(wasmtime_anyhow)
            .with_context(|| format!("validate {role} Core Wasm"))?;
        fs::write(root.join(format!("{role}.wasm")), &wasm)?;
        fs::write(
            root.join(format!("{role}.cwasm")),
            engine
                .precompile_module(&wasm)
                .map_err(wasmtime_anyhow)
                .with_context(|| format!("precompile {role} module"))?,
        )?;
    }
    fs::write(
        root.join("engine-identity.json"),
        serde_json::to_vec(&json!({
            "engineCompatibilitySha256": calculated_precompile_compatibility_sha256(engine),
            "engineConfig": {
                "consumeFuel": true,
                "epochInterruption": true,
                "profilingStrategy": "perf-map",
                "wasmExceptions": true,
            },
            "kind": "convex-wasm-wasmtime-engine-identity",
            "target": {"cpu": "baseline", "triple": GENERATED_TARGET_TRIPLE},
        }))?,
    )?;
    Ok(())
}

fn deserialize_graph_module(
    engine: &Engine,
    record: &AuthenticatedModuleGraphExecutionModule<'_>,
) -> anyhow::Result<Module> {
    deserialize_authenticated_graph_module(
        engine,
        &fs::read(record.aot_path())?,
        record.contract(),
        record.providers(),
    )
}

fn shared_module(
    engine_compatibility_sha256: &str,
    record: &AuthenticatedModuleGraphExecutionModule<'_>,
    module: Module,
) -> Arc<GeneratedSharedAotModule> {
    Arc::new(GeneratedSharedAotModule {
        identity: GeneratedAotModuleIdentity {
            engine_compatibility_sha256: engine_compatibility_sha256.to_owned(),
            serialized_module_sha256: record.aot_sha256().to_owned(),
        },
        module: Arc::new(module),
        module_charge: parking_lot::Mutex::new(None),
    })
}

fn graph_manifest(fixture: &JsonValue) -> anyhow::Result<JsonValue> {
    let encoded = fixture["material"]["moduleGraph"]["packageFiles"]
        .as_array()
        .context("producer graph package files")?
        .iter()
        .find(|file| file["name"] == "graph-manifest.json")
        .context("producer graph manifest file")?["base64"]
        .as_str()
        .context("producer graph manifest bytes")?;
    Ok(serde_json::from_slice(&base64::decode(encoded)?)?)
}

#[test]
#[ignore = "requires the cross-boundary producer fixture builder"]
fn canonical_producer_v2_deployment_v6_registry_v5_executes_emscripten_graph_without_compiler(
) -> anyhow::Result<()> {
    let builder = env::var_os(PRODUCER_FIXTURE_BUILDER_ENV)
        .context("cross-boundary producer fixture builder is not set")?;
    let material = TempDir::new()?;
    let fixture_root = TempDir::new()?;
    let registry_root = TempDir::new()?;
    let fixture_path = fixture_root.path().join("fixture.json");
    let engine = new_generated_engine()?;
    write_execution_material(material.path(), &engine)?;
    let output = Command::new("node")
        .arg(&builder)
        .arg(&fixture_path)
        .arg("--execution-material")
        .arg(material.path())
        .output()
        .context("run producer graph-v2 fixture builder")?;
    anyhow::ensure!(
        output.status.success(),
        "producer graph-v2 fixture builder failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let fixture: JsonValue = serde_json::from_slice(&fs::read(&fixture_path)?)?;
    anyhow::ensure!(
        fixture["expected"]["registryGenerationV5"]["kind"]
            == "convex-wasm-runtime-registry-generation-v5",
        "producer fixture did not publish registry generation-v5"
    );
    anyhow::ensure!(
        fixture["expected"]["registryGenerationV5"]
            .get("capabilityEntryPackages")
            .is_none()
            && fixture["expected"]["registryGenerationV5"]["moduleGraphCohorts"]
                .as_array()
                .is_some_and(|cohorts| !cohorts.is_empty())
            && fixture["material"].get("capabilityEntryPackage").is_none(),
        "producer fixture did not publish graph-only registry-v5 material"
    );
    let deployment: JsonValue = serde_json::from_slice(&base64::decode(
        fixture["material"]["deployment"]["base64"]
            .as_str()
            .context("producer fixture deployment bytes")?,
    )?)?;
    anyhow::ensure!(
        deployment["kind"] == "convex-wasm-deployment-v6",
        "producer fixture did not publish deployment-v6"
    );
    materialize_registry_v5_test_fixture(&fixture, registry_root.path())?;
    anyhow::ensure!(
        !registry_root
            .path()
            .join("capability-entry-packages")
            .exists(),
        "graph-only registry fixture materialized a legacy capability package root"
    );
    let current = load_runtime_registry_current(registry_root.path())?;
    let generation = load_runtime_registry_generation(registry_root.path(), &current)?;
    let catalog = generation
        .into_parts()
        .5
        .context("registry generation-v5 omitted its authenticated graph catalog")?;
    let selected_routes = fixture["expected"]["registryGenerationV5"]["graphRouting"]
        ["selectedRouteIds"]
        .as_array()
        .context("producer fixture selected routes")?;
    let [route_id] = selected_routes.as_slice() else {
        anyhow::bail!("producer fixture must contain exactly one selected route")
    };
    let graph = catalog
        .execution_graph(route_id.as_str().context("producer fixture route ID")?)
        .context("registry generation-v5 did not route to the authenticated graph")?;
    anyhow::ensure!(
        graph.engine_compatibility_sha256() == calculated_precompile_compatibility_sha256(&engine),
        "producer graph engine identity differs from the execution engine"
    );
    anyhow::ensure!(
        graph.initialization().module_order.as_slice() == ["base", "common", "leaf"]
            && graph.initialization().relocation_order.as_slice() == ["common", "leaf"]
            && graph.initialization().constructor_order.as_slice() == ["common", "leaf"],
        "authenticated graph initialization order changed"
    );
    let [base_record, common_record, leaf_record] = graph.modules() else {
        anyhow::bail!("producer fixture did not retain the compatible three-module graph")
    };
    let base = deserialize_graph_module(&engine, base_record)?;
    let common = deserialize_graph_module(&engine, common_record)?;
    let leaf = deserialize_graph_module(&engine, leaf_record)?;
    let module_contract = |record: &AuthenticatedModuleGraphExecutionModule<'_>| {
        GeneratedEmscriptenGraphModuleContract {
            contract: record.contract().clone(),
            layout: record.layout().clone(),
            providers: record.providers().to_vec(),
        }
    };
    let modules = GeneratedEmscriptenGraphModules {
        base: shared_module(graph.engine_compatibility_sha256(), base_record, base),
        shared: vec![shared_module(
            graph.engine_compatibility_sha256(),
            common_record,
            common,
        )],
        leaf: shared_module(graph.engine_compatibility_sha256(), leaf_record, leaf),
        initialization: graph.initialization().clone(),
        modules: vec![
            module_contract(base_record),
            module_contract(common_record),
            module_contract(leaf_record),
        ],
    };
    let selector = u64::from_str_radix(
        graph_manifest(&fixture)?["routing"]["routes"][0]["entrySelectorId"]
            .as_str()
            .context("producer graph selector ID")?,
        16,
    )?;
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    rt.block_on("canonical_module_graph_execution", async move {
        let mut store = Store::new(&engine, ());
        store.set_fuel(1_000_000)?;
        store.set_epoch_deadline(u64::MAX / 2);
        let linker = Linker::new(&engine);
        let instances =
            instantiate_generated_emscripten_graph(&linker, &mut store, &modules).await?;
        let initialization = GeneratedFreshInitialization::EmscriptenGraph {
            instances: instances.clone(),
            base_initialize: instances.modules[0].get_typed_func(&mut store, "_initialize")?,
            initialization: modules.initialization.clone(),
        };
        run_generated_fresh_initialization(&mut store, initialization).await?;
        let select = instances.modules[0]
            .get_typed_func::<i64, i32>(&mut store, "convex_wasm_graph_select_entry")?;
        anyhow::ensure!(
            select.call_async(&mut store, selector as i64).await? == 0,
            "authenticated graph selector rejected its exact route"
        );
        let prepare = instances.modules[0]
            .get_typed_func::<(), i32>(&mut store, "convex_wasm_udf_prepare_selected_entry")?;
        anyhow::ensure!(
            prepare.call_async(&mut store, ()).await? == 0,
            "authenticated graph selected-entry preparation failed"
        );
        let run =
            instances.modules[0].get_typed_func::<(), i32>(&mut store, "convex_wasm_udf_run")?;
        anyhow::ensure!(
            run.call_async(&mut store, ()).await? == EXPECTED_RUN_RESULT,
            "authenticated graph returned the wrong provider, layout, relocation, GOT, or \
             initialization result"
        );
        Ok(())
    })
}
