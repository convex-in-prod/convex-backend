use std::collections::{
    BTreeMap,
    BTreeSet,
};

use anyhow::Context as _;
use wasmtime::{
    Error as WasmtimeError,
    Extern,
    ExternType,
    Global,
    GlobalType,
    Instance,
    Linker,
    Memory,
    Ref,
    Store,
    Val,
};

use super::{
    super::{
        module_graph_registry::{
            GraphInitialization,
            GraphModuleProvider,
        },
        wasm_udf_package::{
            CapabilityGraphExportReference,
            CapabilityGraphLayout,
        },
    },
    routed_module_cache::{
        GeneratedEmscriptenGraphModules,
        GeneratedGraphModules,
    },
    wasmtime_anyhow,
};

pub(super) async fn instantiate_generated_graph_dependencies<T: Send + 'static>(
    linker: &mut Linker<T>,
    store: &mut Store<T>,
    graph: &GeneratedGraphModules,
) -> anyhow::Result<BTreeMap<String, Instance>> {
    let mut instances = BTreeMap::new();
    for dependency in &graph.dependencies {
        let instance = linker
            .instantiate_async(&mut *store, &dependency.module.module)
            .await
            .map_err(wasmtime_anyhow)
            .with_context(|| {
                format!(
                    "failed to instantiate generated graph module {}",
                    dependency.module_id
                )
            })?;
        linker
            .instance(&mut *store, &dependency.provider_namespace, instance)
            .map_err(wasmtime_anyhow)
            .with_context(|| {
                format!(
                    "failed to publish generated graph module {}",
                    dependency.module_id
                )
            })?;
        anyhow::ensure!(
            instances
                .insert(dependency.module_id.clone(), instance)
                .is_none(),
            "generated graph instantiated a duplicate module identity"
        );
    }
    validate_generated_graph_instance_layout(store, &instances, &graph.layout)?;
    Ok(instances)
}

#[derive(Clone)]
struct GeneratedGraphGotBinding {
    global: Global,
    provider: String,
    provider_export: Option<String>,
    weak: bool,
}

type GeneratedGraphGotBindings = BTreeMap<(String, String), GeneratedGraphGotBinding>;

#[derive(Clone)]
pub(super) struct GeneratedEmscriptenGraphInstances {
    pub(super) modules: Vec<Instance>,
}

pub(super) enum GeneratedFreshInitialization {
    Monolithic(wasmtime::TypedFunc<(), ()>),
    EmscriptenGraph {
        instances: GeneratedEmscriptenGraphInstances,
        base_initialize: wasmtime::TypedFunc<(), ()>,
        initialization: GraphInitialization,
    },
}

pub(super) async fn instantiate_generated_emscripten_graph<T: Send + 'static>(
    linker: &Linker<T>,
    store: &mut Store<T>,
    graph: &GeneratedEmscriptenGraphModules,
) -> anyhow::Result<GeneratedEmscriptenGraphInstances> {
    let modules = std::iter::once(graph.base.module.as_ref())
        .chain(graph.shared.iter().map(|shared| shared.module.as_ref()))
        .chain(std::iter::once(graph.leaf.module.as_ref()))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        modules.len() == graph.modules.len(),
        "authenticated module graph modules lost their manifest order"
    );
    let base_imports = modules[0]
        .imports()
        .enumerate()
        .map(|(index, imported)| {
            let provider = &graph.modules[0].providers[index];
            anyhow::ensure!(
                provider.provider == "host"
                    && provider.import_index == index
                    && provider.imported_module == imported.module()
                    && provider.imported_name == imported.name(),
                "base module import differs from its authenticated host provider"
            );
            linker
                .get(&mut *store, imported.module(), imported.name())
                .map_err(wasmtime_anyhow)
                .context("base module host provider is unavailable")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let base = Instance::new_async(&mut *store, modules[0], &base_imports)
        .await
        .map_err(wasmtime_anyhow)
        .context("instantiate authenticated base module")?;
    let memory = base
        .get_memory(&mut *store, "memory")
        .context("base module does not export memory")?;
    let table = base
        .get_table(&mut *store, "__indirect_function_table")
        .context("base module does not export the function table")?;
    let stack_pointer = base
        .get_global(&mut *store, "__stack_pointer")
        .context("base module does not export __stack_pointer")?;
    let heap_base = base
        .get_global(&mut *store, "__heap_base")
        .context("base module does not export __heap_base")?
        .get(&mut *store)
        .i32()
        .context("base __heap_base is not i32")?;
    anyhow::ensure!(
        u32::try_from(heap_base)? == graph.initialization.base_heap_base
            && table.size(&*store) == graph.initialization.base_table_size,
        "base module layout differs from authenticated graph initialization"
    );
    prepare_generated_emscripten_memory_and_table(
        store,
        base,
        memory,
        table,
        &graph.initialization,
    )
    .await?;

    let mut got = GeneratedGraphGotBindings::new();
    let mut instances = vec![base];
    for (module_index, &module) in modules.iter().enumerate().skip(1) {
        let imports = resolve_generated_emscripten_side_imports(
            store,
            graph,
            module_index,
            module,
            &instances,
            memory,
            table,
            stack_pointer,
            &mut got,
        )?;
        let instance = Instance::new_async(&mut *store, module, &imports)
            .await
            .map_err(wasmtime_anyhow)
            .with_context(|| {
                format!(
                    "instantiate authenticated {} module",
                    graph.initialization.module_order[module_index]
                )
            })?;
        instances.push(instance);
        update_generated_emscripten_self_got(
            store,
            graph,
            module_index,
            table,
            &mut got,
            &instances,
        )?;
    }
    reject_unresolved_generated_emscripten_got(store, graph, &got)?;
    Ok(GeneratedEmscriptenGraphInstances { modules: instances })
}

pub(super) async fn run_generated_fresh_initialization<T: Send + 'static>(
    store: &mut Store<T>,
    initialization: GeneratedFreshInitialization,
) -> Result<(), WasmtimeError> {
    let (instances, base_initialize, initialization) = match initialization {
        GeneratedFreshInitialization::Monolithic(initialize) => {
            return initialize.call_async(store, ()).await;
        },
        GeneratedFreshInitialization::EmscriptenGraph {
            instances,
            base_initialize,
            initialization,
        } => (instances, base_initialize, initialization),
    };
    let base = instances.modules[0];
    wasmtime::error::Context::with_context(
        call_generated_graph_optional_0(store, base, "__wasm_apply_data_relocs").await,
        || "run data relocations for authenticated module graph role base (module index 0)",
    )?;
    wasmtime::error::Context::with_context(
        base_initialize.call_async(&mut *store, ()).await,
        || "run initializer for authenticated module graph role base (module index 0)",
    )?;
    for role in &initialization.relocation_order {
        let module_index = generated_graph_module_index(&initialization.module_order, role)
            .map_err(|error| WasmtimeError::msg(format!("{error:#}")))?;
        let instance = instances.modules[module_index];
        wasmtime::error::Context::with_context(
            call_generated_graph_optional_0(store, instance, "__wasm_apply_data_relocs").await,
            || {
                format!(
                    "run data relocations for authenticated module graph role {role} (module \
                     index {module_index})"
                )
            },
        )?;
    }
    for role in &initialization.constructor_order {
        let module_index = generated_graph_module_index(&initialization.module_order, role)
            .map_err(|error| WasmtimeError::msg(format!("{error:#}")))?;
        let instance = instances.modules[module_index];
        wasmtime::error::Context::with_context(
            call_generated_graph_required_0(store, instance, "__wasm_call_ctors").await,
            || {
                format!(
                    "run constructors for authenticated module graph role {role} (module index \
                     {module_index})"
                )
            },
        )?;
    }
    Ok(())
}

async fn prepare_generated_emscripten_memory_and_table<T: Send + 'static>(
    store: &mut Store<T>,
    base: Instance,
    memory: Memory,
    table: wasmtime::Table,
    initialization: &GraphInitialization,
) -> anyhow::Result<()> {
    const WASM_PAGE_BYTES: u64 = 65_536;
    let required_pages = u64::from(initialization.final_memory_cursor).div_ceil(WASM_PAGE_BYTES);
    let current_pages = memory.size(&*store);
    if required_pages > current_pages {
        let previous = memory
            .grow(&mut *store, required_pages - current_pages)
            .map_err(wasmtime_anyhow)?;
        anyhow::ensure!(
            previous == current_pages,
            "shared module graph memory grew from an unexpected page count"
        );
    }
    let sbrk = base
        .get_typed_func::<i32, i32>(&mut *store, "sbrk")
        .map_err(wasmtime_anyhow)
        .context("base module does not export the required sbrk boundary")?;
    let current_break = sbrk.call_async(&mut *store, 0).await?;
    let base_heap_base = i32::try_from(initialization.base_heap_base)?;
    anyhow::ensure!(
        current_break == base_heap_base,
        "base sbrk cursor differs from authenticated __heap_base"
    );
    let allocation_bytes = initialization
        .final_memory_cursor
        .checked_sub(initialization.base_heap_base)
        .context("module graph final memory cursor precedes __heap_base")?;
    if allocation_bytes != 0 {
        let previous = sbrk
            .call_async(&mut *store, i32::try_from(allocation_bytes)?)
            .await?;
        anyhow::ensure!(
            previous == base_heap_base,
            "base sbrk did not reserve the authenticated side-module range"
        );
    }
    let current_table_size = table.size(&*store);
    anyhow::ensure!(
        initialization.final_table_cursor >= current_table_size,
        "module graph final table cursor precedes the base table"
    );
    let additional = initialization.final_table_cursor - current_table_size;
    if additional != 0 {
        let table_type = table.ty(&*store);
        let previous = table
            .grow(
                &mut *store,
                additional,
                Ref::null(table_type.element().heap_type()),
            )
            .map_err(wasmtime_anyhow)?;
        anyhow::ensure!(
            previous == current_table_size,
            "shared module graph table grew from an unexpected size"
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn resolve_generated_emscripten_side_imports<T>(
    store: &mut Store<T>,
    graph: &GeneratedEmscriptenGraphModules,
    module_index: usize,
    module: &wasmtime::Module,
    instances: &[Instance],
    memory: Memory,
    table: wasmtime::Table,
    stack_pointer: Global,
    got: &mut GeneratedGraphGotBindings,
) -> anyhow::Result<Vec<Extern>> {
    module
        .imports()
        .enumerate()
        .map(|(import_index, imported)| {
            let provider = &graph.modules[module_index].providers[import_index];
            anyhow::ensure!(
                provider.import_index == import_index
                    && provider.imported_module == imported.module()
                    && provider.imported_name == imported.name(),
                "side-module import differs from its authenticated provider"
            );
            match provider.provider.as_str() {
                "loader" => {
                    let ExternType::Global(global_type) = imported.ty() else {
                        anyhow::bail!("loader relocation base import is not a global")
                    };
                    let placement = &graph.modules[module_index].layout;
                    let value = match provider.imported_name.as_str() {
                        "__memory_base" => u64::from(placement.memory_base),
                        "__table_base" => placement.table_base,
                        _ => anyhow::bail!("loader provider names an unsupported relocation base"),
                    };
                    Ok(
                        Global::new(&mut *store, global_type, Val::I32(i32::try_from(value)?))?
                            .into(),
                    )
                },
                "weak-zero" => {
                    let ExternType::Global(global_type) = imported.ty() else {
                        anyhow::bail!("weak GOT import is not a global")
                    };
                    generated_emscripten_got_import(
                        store,
                        graph,
                        provider,
                        global_type,
                        instances,
                        table,
                        got,
                        false,
                    )
                },
                role if matches!(provider.imported_module.as_str(), "GOT.mem" | "GOT.func") => {
                    let provider_index =
                        generated_graph_module_index(&graph.initialization.module_order, role)?;
                    let ExternType::Global(global_type) = imported.ty() else {
                        anyhow::bail!("GOT import is not a global")
                    };
                    generated_emscripten_got_import(
                        store,
                        graph,
                        provider,
                        global_type,
                        instances,
                        table,
                        got,
                        provider_index < module_index,
                    )
                },
                role => {
                    let provider_index =
                        generated_graph_module_index(&graph.initialization.module_order, role)?;
                    let provider_instance = instances
                        .get(provider_index)
                        .copied()
                        .context("side-module provider has not been instantiated")?;
                    let export_name = provider
                        .provider_export
                        .as_deref()
                        .context("side-module provider export is missing")?;
                    let external = provider_instance
                        .get_export(&mut *store, export_name)
                        .context("side-module provider export is unavailable")?;
                    match external {
                        Extern::Memory(_) => {
                            anyhow::ensure!(provider_index == 0, "shared memory is not base-owned");
                            Ok(memory.into())
                        },
                        Extern::Table(_) => {
                            anyhow::ensure!(provider_index == 0, "shared table is not base-owned");
                            Ok(table.into())
                        },
                        Extern::Global(_) if export_name == "__stack_pointer" => {
                            anyhow::ensure!(
                                provider_index == 0,
                                "shared stack pointer is not base-owned"
                            );
                            Ok(stack_pointer.into())
                        },
                        external => Ok(external),
                    }
                },
            }
        })
        .collect()
}

fn generated_graph_module_index(module_order: &[String], role: &str) -> anyhow::Result<usize> {
    module_order
        .iter()
        .position(|candidate| candidate.as_str() == role)
        .with_context(|| format!("module graph provider {role} is not an authenticated module"))
}

#[allow(clippy::too_many_arguments)]
fn generated_emscripten_got_import<T>(
    store: &mut Store<T>,
    graph: &GeneratedEmscriptenGraphModules,
    provider: &GraphModuleProvider,
    global_type: GlobalType,
    instances: &[Instance],
    table: wasmtime::Table,
    got: &mut GeneratedGraphGotBindings,
    populate: bool,
) -> anyhow::Result<Extern> {
    let key = (
        provider.imported_module.clone(),
        provider.imported_name.clone(),
    );
    if let Some(binding) = got.get(&key) {
        anyhow::ensure!(
            binding.provider == provider.provider
                && binding.provider_export == provider.provider_export
                && binding.weak == provider.weak,
            "shared GOT symbol has conflicting authenticated providers"
        );
        return Ok(binding.global.into());
    }
    let consumer_index =
        generated_graph_module_index(&graph.initialization.module_order, &provider.consumer)?;
    anyhow::ensure!(
        graph.modules[consumer_index].contract.imports[provider.import_index]
            .r#type
            .canonical
            == "global(i32,var)",
        "GOT import is not a mutable i32 global"
    );
    let global = Global::new(&mut *store, global_type, Val::I32(0))?;
    got.insert(
        key,
        GeneratedGraphGotBinding {
            global,
            provider: provider.provider.clone(),
            provider_export: provider.provider_export.clone(),
            weak: provider.weak,
        },
    );
    if populate {
        populate_generated_emscripten_got(
            store,
            &graph.initialization.module_order,
            provider,
            instances,
            table,
            global,
        )?;
    }
    Ok(global.into())
}

fn populate_generated_emscripten_got<T>(
    store: &mut Store<T>,
    module_order: &[String],
    provider: &GraphModuleProvider,
    instances: &[Instance],
    table: wasmtime::Table,
    global: Global,
) -> anyhow::Result<()> {
    if provider.weak {
        return Ok(());
    }
    let provider_index = generated_graph_module_index(module_order, &provider.provider)?;
    let instance = instances
        .get(provider_index)
        .copied()
        .context("GOT provider is unavailable")?;
    let export_name = provider
        .provider_export
        .as_deref()
        .context("strong GOT provider export is missing")?;
    let external = instance
        .get_export(&mut *store, export_name)
        .context("strong GOT provider export is unavailable")?;
    let value = match provider.imported_module.as_str() {
        "GOT.mem" => external
            .into_global()
            .context("GOT.mem provider export is not a global")?
            .get(&mut *store)
            .i32()
            .context("GOT.mem provider export is not i32")?,
        "GOT.func" => {
            let function = external
                .into_func()
                .context("GOT.func provider export is not a function")?;
            let index = table.size(&*store);
            let table_type = table.ty(&*store);
            let previous =
                table.grow(&mut *store, 1, Ref::null(table_type.element().heap_type()))?;
            anyhow::ensure!(previous == index, "GOT function table allocation changed");
            table.set(&mut *store, index, function.into())?;
            i32::try_from(index)?
        },
        _ => anyhow::bail!("GOT provider uses an unsupported namespace"),
    };
    global.set(&mut *store, Val::I32(value))?;
    Ok(())
}

fn update_generated_emscripten_self_got<T>(
    store: &mut Store<T>,
    graph: &GeneratedEmscriptenGraphModules,
    module_index: usize,
    table: wasmtime::Table,
    got: &mut GeneratedGraphGotBindings,
    instances: &[Instance],
) -> anyhow::Result<()> {
    for provider in graph.modules[module_index]
        .providers
        .iter()
        .filter(|provider| {
            provider.provider == provider.consumer
                && matches!(provider.imported_module.as_str(), "GOT.mem" | "GOT.func")
        })
    {
        let key = (
            provider.imported_module.clone(),
            provider.imported_name.clone(),
        );
        let global = got
            .get(&key)
            .context("self GOT provider has no loader-created global")?
            .global;
        populate_generated_emscripten_got(
            store,
            &graph.initialization.module_order,
            provider,
            instances,
            table,
            global,
        )?;
    }
    Ok(())
}

fn reject_unresolved_generated_emscripten_got<T>(
    store: &mut Store<T>,
    graph: &GeneratedEmscriptenGraphModules,
    got: &GeneratedGraphGotBindings,
) -> anyhow::Result<()> {
    let expected = graph.modules[1..]
        .iter()
        .flat_map(|module| &module.providers)
        .filter(|provider| matches!(provider.imported_module.as_str(), "GOT.mem" | "GOT.func"))
        .map(|provider| {
            (
                provider.imported_module.clone(),
                provider.imported_name.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        expected.len() == got.len() && expected.iter().all(|key| got.contains_key(key)),
        "loader GOT set differs from authenticated providers"
    );
    for ((namespace, name), binding) in got {
        let value = binding
            .global
            .get(&mut *store)
            .i32()
            .context("loader GOT global is not i32")?;
        anyhow::ensure!(
            (binding.weak && value == 0) || (!binding.weak && value != 0),
            "loader GOT symbol {namespace}.{name} has an invalid resolved value"
        );
    }
    Ok(())
}

async fn call_generated_graph_optional_0<T: Send + 'static>(
    store: &mut Store<T>,
    instance: Instance,
    name: &str,
) -> Result<(), WasmtimeError> {
    if let Some(function) = instance.get_func(&mut *store, name) {
        function
            .typed::<(), ()>(&*store)?
            .call_async(&mut *store, ())
            .await?;
    }
    Ok(())
}

async fn call_generated_graph_required_0<T: Send + 'static>(
    store: &mut Store<T>,
    instance: Instance,
    name: &str,
) -> Result<(), WasmtimeError> {
    instance
        .get_typed_func::<(), ()>(&mut *store, name)?
        .call_async(&mut *store, ())
        .await?;
    Ok(())
}

pub(super) fn validate_generated_graph_instance_layout<T>(
    store: &mut Store<T>,
    instances: &BTreeMap<String, Instance>,
    layout: &CapabilityGraphLayout,
) -> anyhow::Result<()> {
    let memory = generated_graph_instance_export(store, instances, &layout.memory)?;
    anyhow::ensure!(
        matches!(memory, Extern::Memory(_)),
        "generated graph shared memory layout resolved to a different extern kind"
    );
    let table = generated_graph_instance_export(store, instances, &layout.table)?;
    anyhow::ensure!(
        matches!(table, Extern::Table(_)),
        "generated graph shared table layout resolved to a different extern kind"
    );
    let pointer = generated_graph_instance_export(store, instances, &layout.stack.pointer)?;
    let Extern::Global(pointer) = pointer else {
        anyhow::bail!("generated graph stack pointer resolved to a different extern kind");
    };
    let actual_pointer = match pointer.get(&mut *store) {
        Val::I32(value) => u64::from(value as u32),
        Val::I64(value) => value as u64,
        Val::F32(_)
        | Val::F64(_)
        | Val::V128(_)
        | Val::FuncRef(_)
        | Val::ExternRef(_)
        | Val::AnyRef(_)
        | Val::ExnRef(_)
        | Val::ContRef(_) => {
            anyhow::bail!("generated graph stack pointer has a non-integer value")
        },
    };
    anyhow::ensure!(
        actual_pointer == layout.stack.initial_pointer,
        "generated graph stack pointer differs from its authenticated initial layout"
    );
    for tag in &layout.tags {
        anyhow::ensure!(
            matches!(
                generated_graph_instance_export(store, instances, tag)?,
                Extern::Tag(_)
            ),
            "generated graph tag layout resolved to a different extern kind"
        );
    }
    Ok(())
}

fn generated_graph_instance_export<T>(
    store: &mut Store<T>,
    instances: &BTreeMap<String, Instance>,
    reference: &CapabilityGraphExportReference,
) -> anyhow::Result<Extern> {
    instances
        .get(&reference.module_id)
        .with_context(|| {
            format!(
                "generated graph layout provider {} was not instantiated",
                reference.module_id
            )
        })?
        .get_export(store, &reference.export_name)
        .with_context(|| {
            format!(
                "generated graph layout export {} is missing",
                reference.export_name
            )
        })
}
