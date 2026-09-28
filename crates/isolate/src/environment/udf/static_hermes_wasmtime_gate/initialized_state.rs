//! State outside linear memory that an initialized Emscripten graph must
//! reconstruct. Published static metadata supplies restoration rules; older
//! artifacts are inspected at load time using the same versioned rules.

use std::sync::Arc;

use anyhow::Context as _;
use wasmtime::{
    Global,
    Instance,
    Store,
    TypedFunc,
    Val,
};

use super::wasmtime_anyhow;
use crate::environment::udf::wasm_snapshot_metadata::SnapshotState;

#[derive(Debug)]
pub(super) enum ModuleSnapshotSupport {
    Supported(ModuleSnapshotLayout),
    Unsupported(String),
}

#[derive(Debug)]
pub(super) struct ModuleSnapshotLayout {
    core_sha256: Arc<str>,
    globals: Vec<String>,
    stack_limits: bool,
}

/// Only scalar values cross Stores. Function references and GC objects belong
/// to their original Store and cannot be copied with a linear-memory image.
pub(super) struct CapturedModuleState {
    core_sha256: Arc<str>,
    globals: Vec<Val>,
    stack_limits: Option<(i32, i32)>,
}

#[derive(Clone)]
pub(super) struct BoundModuleState {
    core_sha256: Arc<str>,
    globals: Vec<Global>,
    stack_limits: Option<StackLimits>,
}

#[derive(Clone)]
struct StackLimits {
    base: TypedFunc<(), i32>,
    end: TypedFunc<(), i32>,
    set: TypedFunc<(i32, i32), ()>,
}

impl ModuleSnapshotLayout {
    /// Inspect snapshot eligibility, not Wasm validity: publication supplies
    /// compiler-produced Core and AOT. Retain the declared Core identity so a
    /// captured state cannot be restored into a different module.
    pub(super) fn inspect(core: &[u8], core_sha256: &str) -> anyhow::Result<ModuleSnapshotSupport> {
        Ok(Self::from_state(SnapshotState::inspect(core)?, core_sha256))
    }

    pub(super) fn from_state(state: SnapshotState, core_sha256: &str) -> ModuleSnapshotSupport {
        match state {
            SnapshotState::Supported {
                globals,
                stack_limits,
            } => ModuleSnapshotSupport::Supported(Self {
                core_sha256: Arc::from(core_sha256),
                globals,
                stack_limits,
            }),
            SnapshotState::Unsupported { reason } => ModuleSnapshotSupport::Unsupported(reason),
        }
    }

    /// Bind only to the instance of the exact retained Module inspected above.
    /// Inspect names before fetching exports: enumerating every live export can
    /// materialize thousands of unnecessary Store-owned function records.
    pub(super) fn bind<T: 'static>(
        &self,
        store: &mut Store<T>,
        instance: Instance,
        module: &wasmtime::Module,
    ) -> anyhow::Result<BoundModuleState> {
        anyhow::ensure!(
            wasmtime::Module::same(instance.module(&*store), module),
            "initialized state binding belongs to a different compiled module"
        );
        let globals = self
            .globals
            .iter()
            .map(|name| {
                instance
                    .get_global(&mut *store, name)
                    .context("initialized graph lost a captured global")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let stack_limits = if self.stack_limits {
            Some(StackLimits {
                base: instance
                    .get_typed_func(&mut *store, "emscripten_stack_get_base")
                    .map_err(wasmtime_anyhow)?,
                end: instance
                    .get_typed_func(&mut *store, "emscripten_stack_get_end")
                    .map_err(wasmtime_anyhow)?,
                set: instance
                    .get_typed_func(&mut *store, "emscripten_stack_set_limits")
                    .map_err(wasmtime_anyhow)?,
            })
        } else {
            None
        };
        Ok(BoundModuleState {
            core_sha256: Arc::clone(&self.core_sha256),
            globals,
            stack_limits,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        task::{
            Context,
            Poll,
            Waker,
        },
    };

    use wasm_encoder::{
        CodeSection,
        ConstExpr,
        DataCountSection,
        DataSection,
        ExportKind,
        ExportSection,
        Function,
        FunctionSection,
        GlobalSection,
        GlobalType,
        ImportSection,
        Instruction,
        MemoryType,
        Module,
        TypeSection,
        ValType,
    };

    use super::*;

    fn inspect(core: &[u8]) -> anyhow::Result<ModuleSnapshotSupport> {
        use sha2::{
            Digest,
            Sha256,
        };
        ModuleSnapshotLayout::inspect(core, &format!("{:x}", Sha256::digest(core)))
    }

    fn fixture(
        hidden_extra: bool,
        wrong_setter: bool,
        drop_segment: bool,
        extra_getter_parameter: bool,
    ) -> Vec<u8> {
        let mut types = TypeSection::new();
        types.ty().function(
            extra_getter_parameter.then_some(ValType::I32),
            [ValType::I32],
        );
        types.ty().function([ValType::I32, ValType::I32], []);
        let mut globals = GlobalSection::new();
        for value in [4096, 8192, 1024] {
            globals.global(
                GlobalType {
                    val_type: ValType::I32,
                    mutable: true,
                    shared: false,
                },
                &ConstExpr::i32_const(value),
            );
        }
        if hidden_extra {
            globals.global(
                GlobalType {
                    val_type: ValType::I32,
                    mutable: true,
                    shared: false,
                },
                &ConstExpr::i32_const(7),
            );
        }
        let mut functions = FunctionSection::new();
        functions.function(0).function(0).function(1);
        let mut exports = ExportSection::new();
        exports.export("__stack_pointer", ExportKind::Global, 0);
        exports.export("emscripten_stack_get_base", ExportKind::Func, 0);
        exports.export("emscripten_stack_get_end", ExportKind::Func, 1);
        exports.export("emscripten_stack_set_limits", ExportKind::Func, 2);
        let mut code = CodeSection::new();
        for index in [1, 2] {
            let mut function = Function::new([]);
            function
                .instruction(&Instruction::GlobalGet(index))
                .instruction(&Instruction::End);
            code.function(&function);
        }
        let mut setter = Function::new([]);
        setter
            .instruction(&Instruction::LocalGet(0))
            .instruction(&Instruction::GlobalSet(if wrong_setter { 0 } else { 1 }));
        setter
            .instruction(&Instruction::LocalGet(1))
            .instruction(&Instruction::GlobalSet(2));
        if drop_segment {
            setter.instruction(&Instruction::DataDrop(0));
        }
        setter.instruction(&Instruction::End);
        code.function(&setter);
        let mut module = Module::new();
        module
            .section(&types)
            .section(&functions)
            .section(&globals)
            .section(&exports);
        if drop_segment {
            module.section(&DataCountSection { count: 1 });
        }
        module.section(&code);
        if drop_segment {
            let mut data = DataSection::new();
            data.passive([1, 2, 3]);
            module.section(&data);
        }
        module.finish()
    }

    #[test]
    fn hidden_state_or_changed_stack_accessor_cannot_grant_image_authority() -> anyhow::Result<()> {
        for (core, reason) in [
            (
                fixture(true, false, false, false),
                "hidden mutable global has no restoration operation",
            ),
            (
                fixture(false, true, false, false),
                "incomplete stack-limit accessors",
            ),
            (
                fixture(false, false, true, false),
                "guest mutates table or segment state",
            ),
        ] {
            let ModuleSnapshotSupport::Unsupported(actual) = inspect(&core)? else {
                anyhow::bail!("unsupported state received snapshot authority");
            };
            assert_eq!(actual, reason);
        }
        Ok(())
    }

    #[test]
    fn valid_accessor_with_extra_parameter_keeps_ordinary_preparation() -> anyhow::Result<()> {
        let core = fixture(false, false, false, true);
        wasmtime::Module::new(&wasmtime::Engine::default(), &core).map_err(wasmtime_anyhow)?;
        assert!(matches!(
            inspect(&core)?,
            ModuleSnapshotSupport::Unsupported(reason) if reason == "stack accessor has an unsupported signature"
        ));
        Ok(())
    }

    #[test]
    fn start_function_keeps_ordinary_preparation() -> anyhow::Result<()> {
        let mut types = TypeSection::new();
        types.ty().function([], []);
        let mut functions = FunctionSection::new();
        functions.function(0);
        let mut body = Function::new([]);
        body.instruction(&Instruction::End);
        let mut code = CodeSection::new();
        code.function(&body);
        let mut module = Module::new();
        module
            .section(&types)
            .section(&functions)
            .section(&wasm_encoder::StartSection { function_index: 0 })
            .section(&code);
        let core = module.finish();
        wasmtime::Module::new(&wasmtime::Engine::default(), &core).map_err(wasmtime_anyhow)?;
        assert!(matches!(
            inspect(&core)?,
            ModuleSnapshotSupport::Unsupported(reason) if reason == "start function is not a proven memory initializer"
        ));
        Ok(())
    }

    const ZERO_FILL_START: [Instruction<'static>; 7] = [
        Instruction::I32Const(64),
        Instruction::GlobalGet(1),
        Instruction::I32Add,
        Instruction::I32Const(0),
        Instruction::I32Const(9),
        Instruction::MemoryFill(0),
        Instruction::End,
    ];

    fn memory_start_fixture(imports: &ImportSection, body: &Function, start_index: u32) -> Vec<u8> {
        let mut types = TypeSection::new();
        types.ty().function([], []);
        let mut functions = FunctionSection::new();
        functions.function(0);
        let mut code = CodeSection::new();
        code.function(body);
        let mut module = Module::new();
        module
            .section(&types)
            .section(imports)
            .section(&functions)
            .section(&wasm_encoder::StartSection {
                function_index: start_index,
            })
            .section(&code);
        module.finish()
    }

    fn memory_start_imports() -> ImportSection {
        let mut imports = ImportSection::new();
        imports.import("env", "host_effect", wasm_encoder::EntityType::Function(0));
        imports.import(
            "env",
            "memory",
            MemoryType {
                minimum: 1,
                maximum: None,
                memory64: false,
                shared: false,
                page_size_log2: None,
            },
        );
        for name in ["__table_base", "__memory_base"] {
            imports.import(
                "env",
                name,
                GlobalType {
                    val_type: ValType::I32,
                    mutable: false,
                    shared: false,
                },
            );
        }
        imports
    }

    #[test]
    fn relocated_zero_fill_start_needs_no_function_name() -> anyhow::Result<()> {
        let mut body = Function::new([]);
        for op in &ZERO_FILL_START {
            body.instruction(op);
        }
        let core = memory_start_fixture(&memory_start_imports(), &body, 1);
        assert!(matches!(
            inspect(&core)?,
            ModuleSnapshotSupport::Supported(_)
        ));
        Ok(())
    }

    #[test]
    fn starts_cannot_extend_zero_fill_authority() -> anyhow::Result<()> {
        let imports = memory_start_imports();
        let mut variants = Vec::new();
        for call_index in [0, 1] {
            // A host call and a recursive defined call must both be rejected.
            for position in [0, ZERO_FILL_START.len() - 1] {
                let mut ops = ZERO_FILL_START.to_vec();
                ops.insert(position, Instruction::Call(call_index));
                variants.push(ops);
            }
        }
        for (position, replacement) in [
            (1, Instruction::GlobalGet(0)),
            (3, Instruction::I32Const(1)),
            (4, Instruction::I32Const(-1)),
        ] {
            let mut ops = ZERO_FILL_START.to_vec();
            ops[position] = replacement;
            variants.push(ops);
        }
        // Stack-valid memory-only work is still outside the narrow proof.
        let mut extra_effect = ZERO_FILL_START.to_vec();
        extra_effect.splice(
            6..6,
            [
                Instruction::I32Const(1),
                Instruction::MemoryGrow(0),
                Instruction::Drop,
            ],
        );
        variants.push(extra_effect);
        for ops in variants {
            let mut body = Function::new([]);
            for op in &ops {
                body.instruction(op);
            }
            let core = memory_start_fixture(&imports, &body, 1);
            wasmparser::Validator::new_with_features(wasmparser::WasmFeatures::all())
                .validate_all(&core)?;
            assert!(matches!(
                inspect(&core)?,
                ModuleSnapshotSupport::Unsupported(_)
            ));
        }
        for (locals, start_index) in [(vec![], 0), (vec![(1, ValType::I32)], 1)] {
            let mut body = Function::new(locals);
            for op in &ZERO_FILL_START {
                body.instruction(op);
            }
            let core = memory_start_fixture(&imports, &body, start_index);
            assert!(matches!(
                inspect(&core)?,
                ModuleSnapshotSupport::Unsupported(_)
            ));
        }
        Ok(())
    }

    #[test]
    fn zero_fill_requires_the_graph_memory_and_immutable_i32_base() -> anyhow::Result<()> {
        let mut body = Function::new([]);
        for op in &ZERO_FILL_START {
            body.instruction(op);
        }
        // Same instruction bytes must not grant authority to a different
        // memory or relocation binding, including an additional memory.
        for (mem_module, mem_name, count, base_module, base_name, mutable, val_type) in [
            (
                "other",
                "memory",
                1,
                "env",
                "__memory_base",
                false,
                ValType::I32,
            ),
            (
                "env",
                "other",
                1,
                "env",
                "__memory_base",
                false,
                ValType::I32,
            ),
            (
                "env",
                "memory",
                2,
                "env",
                "__memory_base",
                false,
                ValType::I32,
            ),
            (
                "env",
                "memory",
                1,
                "other",
                "__memory_base",
                false,
                ValType::I32,
            ),
            ("env", "memory", 1, "env", "other", false, ValType::I32),
            (
                "env",
                "memory",
                1,
                "env",
                "__memory_base",
                true,
                ValType::I32,
            ),
            (
                "env",
                "memory",
                1,
                "env",
                "__memory_base",
                false,
                ValType::I64,
            ),
        ] {
            let mut imports = ImportSection::new();
            imports.import("env", "host_effect", wasm_encoder::EntityType::Function(0));
            for _ in 0..count {
                imports.import(
                    mem_module,
                    mem_name,
                    MemoryType {
                        minimum: 1,
                        maximum: None,
                        memory64: false,
                        shared: false,
                        page_size_log2: None,
                    },
                );
            }
            for _ in 0..2 {
                imports.import(
                    base_module,
                    base_name,
                    GlobalType {
                        val_type,
                        mutable,
                        shared: false,
                    },
                );
            }
            let core = memory_start_fixture(&imports, &body, 1);
            assert!(matches!(
                inspect(&core)?,
                ModuleSnapshotSupport::Unsupported(_)
            ));
        }
        Ok(())
    }

    #[test]
    fn reference_interfaces_cannot_retain_roots_outside_the_image() -> anyhow::Result<()> {
        for imported in [true, false] {
            let mut types = TypeSection::new();
            types.ty().function([ValType::EXTERNREF], []);
            let mut module = Module::new();
            module.section(&types);
            if imported {
                let mut imports = ImportSection::new();
                imports.import("host", "retain", wasm_encoder::EntityType::Function(0));
                module.section(&imports);
            } else {
                let mut functions = FunctionSection::new();
                functions.function(0);
                let mut exports = ExportSection::new();
                exports.export("retain", ExportKind::Func, 0);
                let mut code = CodeSection::new();
                let mut body = Function::new([]);
                body.instruction(&Instruction::End);
                code.function(&body);
                module.section(&functions).section(&exports).section(&code);
            }
            assert!(matches!(
                inspect(&module.finish())?,
                ModuleSnapshotSupport::Unsupported(reason) if reason == "reference-valued function interface"
            ));
        }
        let mut elements = wasm_encoder::ElementSection::new();
        elements.passive(wasm_encoder::Elements::Expressions(
            wasm_encoder::RefType::EXTERNREF,
            std::borrow::Cow::Borrowed(&[ConstExpr::ref_null(wasm_encoder::HeapType::EXTERN)]),
        ));
        let mut module = Module::new();
        module.section(&elements);
        assert!(matches!(
            inspect(&module.finish())?,
            ModuleSnapshotSupport::Unsupported(reason) if reason == "element segment retains non-function references"
        ));
        Ok(())
    }

    #[test]
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    fn caught_exception_capacity_does_not_cross_the_initialized_image() -> anyhow::Result<()> {
        use super::super::initialized_memory::InitializedMemoryImage;

        let mut types = TypeSection::new();
        types.ty().function([], []);
        types.ty().function([ValType::I32], []);
        types.ty().function([], [ValType::I32]);
        let mut functions = FunctionSection::new();
        functions.function(0).function(0).function(2);
        let mut memories = wasm_encoder::MemorySection::new();
        memories.memory(MemoryType {
            minimum: 1,
            maximum: Some(1),
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        let mut tags = wasm_encoder::TagSection::new();
        tags.tag(wasm_encoder::TagType {
            kind: wasm_encoder::TagKind::Exception,
            func_type_idx: 1,
        });
        let mut globals = GlobalSection::new();
        globals.global(
            GlobalType {
                val_type: ValType::I32,
                mutable: true,
                shared: false,
            },
            &ConstExpr::i32_const(0),
        );
        let mut exports = ExportSection::new();
        exports.export("memory", ExportKind::Memory, 0);
        exports.export("prepared_scalar", ExportKind::Global, 0);
        exports.export("prepare", ExportKind::Func, 0);
        exports.export("throw", ExportKind::Func, 1);
        exports.export("resume", ExportKind::Func, 2);
        let mut prepare = Function::new([]);
        prepare
            .instruction(&Instruction::I32Const(0))
            .instruction(&Instruction::I32Const(42))
            .instruction(&Instruction::I32Store(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }))
            .instruction(&Instruction::I32Const(7))
            .instruction(&Instruction::GlobalSet(0))
            .instruction(&Instruction::Block(wasm_encoder::BlockType::Empty))
            .instruction(&Instruction::TryTable(
                wasm_encoder::BlockType::Empty,
                std::borrow::Cow::Borrowed(&[wasm_encoder::Catch::All { label: 0 }]),
            ))
            .instruction(&Instruction::I32Const(0))
            .instruction(&Instruction::Throw(0))
            .instruction(&Instruction::End)
            .instruction(&Instruction::End)
            .instruction(&Instruction::End);
        let mut throw = Function::new([]);
        throw
            .instruction(&Instruction::I32Const(0))
            .instruction(&Instruction::Throw(0))
            .instruction(&Instruction::End);
        let mut resume = Function::new([]);
        resume
            .instruction(&Instruction::Block(wasm_encoder::BlockType::Result(
                ValType::I32,
            )))
            .instruction(&Instruction::TryTable(
                wasm_encoder::BlockType::Empty,
                std::borrow::Cow::Borrowed(&[wasm_encoder::Catch::One { tag: 0, label: 0 }]),
            ))
            .instruction(&Instruction::GlobalGet(0))
            .instruction(&Instruction::I32Const(0))
            .instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }))
            .instruction(&Instruction::I32Add)
            .instruction(&Instruction::Throw(0))
            .instruction(&Instruction::End)
            .instruction(&Instruction::Unreachable)
            .instruction(&Instruction::End)
            .instruction(&Instruction::End);
        let mut code = CodeSection::new();
        code.function(&prepare).function(&throw).function(&resume);
        let mut module = Module::new();
        module
            .section(&types)
            .section(&functions)
            .section(&memories)
            .section(&tags)
            .section(&globals)
            .section(&exports)
            .section(&code);
        let core = module.finish();
        let ModuleSnapshotSupport::Supported(layout) = inspect(&core)? else {
            anyhow::bail!("caught exception fixture must support initialized state");
        };
        let mut config = wasmtime::Config::new();
        config.wasm_exceptions(true).memory_reservation(1u64 << 32);
        let engine = wasmtime::Engine::new(&config).map_err(wasmtime_anyhow)?;
        let module = wasmtime::Module::new(&engine, &core).map_err(wasmtime_anyhow)?;
        let operation = async {
            let mut source_store = Store::new(&engine, ());
            let source = Instance::new_async(&mut source_store, &module, &[])
                .await
                .map_err(wasmtime_anyhow)?;
            source
                .get_typed_func::<(), ()>(&mut source_store, "prepare")
                .map_err(wasmtime_anyhow)?
                .call_async(&mut source_store, ())
                .await
                .map_err(wasmtime_anyhow)?;
            assert!(source_store.gc_heap_capacity() > 0);
            assert!(!source_store.has_pending_exception());
            let source_memory = source.get_memory(&mut source_store, "memory").unwrap();
            let state = layout
                .bind(&mut source_store, source, &module)?
                .capture(&mut source_store)
                .await?;
            let image = InitializedMemoryImage::capture(source_memory.data(&source_store))?;
            let source_gc_capacity = source_store.gc_heap_capacity();
            let source_pages =
                unsafe { image.attach(&mut source_store, source_memory, 65536, 65536, None)? };
            assert_eq!(source_store.gc_heap_capacity(), source_gc_capacity);
            let mut target_store = Store::new(&engine, ());
            let target = Instance::new_async(&mut target_store, &module, &[])
                .await
                .map_err(wasmtime_anyhow)?;
            let memory = target.get_memory(&mut target_store, "memory").unwrap();
            layout
                .bind(&mut target_store, target, &module)?
                .restore(&mut target_store, &state)
                .await?;
            let _pages = unsafe { image.attach(&mut target_store, memory, 65536, 65536, None)? };
            assert_eq!(&memory.data(&target_store)[..4], &42i32.to_le_bytes());
            assert_eq!(target_store.gc_heap_capacity(), 0);
            memory.write(&mut target_store, 0, &99i32.to_le_bytes())?;
            assert_eq!(
                &source_memory.data(&source_store)[..4],
                &42i32.to_le_bytes()
            );
            drop(source_store);
            drop(source_pages);
            // The restored scalar and bytes remain usable after source disposal.
            // A fresh exception uses the target's own tag and GC heap.
            assert_eq!(
                target
                    .get_typed_func::<(), i32>(&mut target_store, "resume")
                    .map_err(wasmtime_anyhow)?
                    .call_async(&mut target_store, ())
                    .await
                    .map_err(wasmtime_anyhow)?,
                106
            );
            assert!(target_store.gc_heap_capacity() > 0);
            assert!(!target_store.has_pending_exception());
            // A subsequently uncaught exception is real Store-owned state,
            // unlike the capacity left by the caught initialization exception.
            assert!(target
                .get_typed_func::<(), ()>(&mut target_store, "throw")
                .map_err(wasmtime_anyhow)?
                .call_async(&mut target_store, ())
                .await
                .is_err());
            assert!(target_store.has_pending_exception());
            Ok(())
        };
        match std::pin::pin!(operation).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => result,
            Poll::Pending => anyhow::bail!("exception fixture unexpectedly suspended"),
        }
    }

    #[test]
    fn prepared_scalars_restore_into_independent_store_owned_globals() -> anyhow::Result<()> {
        use crate::environment::udf::wasm_snapshot_metadata::{
            SnapshotMetadata,
            SNAPSHOT_METADATA_KIND,
        };
        let core = fixture(false, false, false, false);
        let ModuleSnapshotSupport::Supported(layout) = inspect(&core)? else {
            anyhow::bail!("scalar fixture must support initialized state");
        };
        let metadata: SnapshotMetadata =
            serde_json::from_slice(&serde_json::to_vec(&SnapshotMetadata {
                kind: SNAPSHOT_METADATA_KIND.to_owned(),
                core_sha256: layout.core_sha256.to_string(),
                core_size: core.len() as u64,
                state: SnapshotState::inspect(&core)?,
            })?)?;
        metadata.validate_for(&layout.core_sha256, core.len() as u64)?;
        let ModuleSnapshotSupport::Supported(published_layout) =
            ModuleSnapshotLayout::from_state(metadata.state, &metadata.core_sha256)
        else {
            anyhow::bail!("published scalar layout must support initialized state");
        };
        let engine = wasmtime::Engine::default();
        let module = wasmtime::Module::new(&engine, &core).map_err(wasmtime_anyhow)?;
        // No imported async functions, fuel yields or host waits occur in this
        // fixture. One poll executes actual Wasm without an unrelated executor.
        let operation = async {
            let mut source_store = Store::new(&engine, ());
            let source = Instance::new_async(&mut source_store, &module, &[])
                .await
                .map_err(wasmtime_anyhow)?;
            let source_state = layout.bind(&mut source_store, source, &module)?;
            let prepared = source_state.capture(&mut source_store).await?;
            let mut target_store = Store::new(&engine, ());
            let target = Instance::new_async(&mut target_store, &module, &[])
                .await
                .map_err(wasmtime_anyhow)?;
            // Published metadata must restore the same globals and stack limits
            // as the legacy runtime-inspection path captured in the source Store.
            let target_state = published_layout.bind(&mut target_store, target, &module)?;
            target_state.globals[0]
                .set(&mut target_store, Val::I32(99))
                .map_err(wasmtime_anyhow)?;
            target_state
                .stack_limits
                .as_ref()
                .unwrap()
                .set
                .call_async(&mut target_store, (100, 200))
                .await
                .map_err(wasmtime_anyhow)?;
            target_state.restore(&mut target_store, &prepared).await?;
            let observed = target_state.capture(&mut target_store).await?;
            assert_eq!(observed.globals[0].i32(), Some(4096));
            assert_eq!(observed.stack_limits, Some((8192, 1024)));
            assert_eq!(
                source_state.globals[0].get(&mut source_store).i32(),
                Some(4096)
            );
            let mut wrong = prepared;
            wrong.core_sha256 = Arc::from("different module");
            assert!(target_state
                .restore(&mut target_store, &wrong)
                .await
                .is_err());
            Ok(())
        };
        match std::pin::pin!(operation).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => result,
            Poll::Pending => anyhow::bail!("scalar fixture unexpectedly suspended"),
        }
    }
}

impl BoundModuleState {
    pub(super) fn captured_bytes(&self) -> usize {
        std::mem::size_of::<CapturedModuleState>().saturating_add(
            self.globals
                .len()
                .saturating_mul(std::mem::size_of::<Val>()),
        )
    }

    pub(super) async fn capture<T: Send>(
        &self,
        store: &mut Store<T>,
    ) -> anyhow::Result<CapturedModuleState> {
        let globals = self
            .globals
            .iter()
            .map(|global| global.get(&mut *store))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            globals.iter().all(|value| matches!(
                value,
                Val::I32(_) | Val::I64(_) | Val::F32(_) | Val::F64(_) | Val::V128(_)
            )),
            "snapshot global is not a scalar"
        );
        let stack_limits = match &self.stack_limits {
            Some(stack) => Some((
                stack
                    .base
                    .call_async(&mut *store, ())
                    .await
                    .map_err(wasmtime_anyhow)?,
                stack
                    .end
                    .call_async(&mut *store, ())
                    .await
                    .map_err(wasmtime_anyhow)?,
            )),
            None => None,
        };
        Ok(CapturedModuleState {
            core_sha256: Arc::clone(&self.core_sha256),
            globals,
            stack_limits,
        })
    }

    /// Requires exclusive idle Store ownership under the existing CPU/deadline
    /// budget. Dispose on cancellation/error; partial restoration is not reuse.
    pub(super) async fn restore<T: Send>(
        &self,
        store: &mut Store<T>,
        captured: &CapturedModuleState,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.core_sha256 == captured.core_sha256
                && self.globals.len() == captured.globals.len()
                && self.stack_limits.is_some() == captured.stack_limits.is_some(),
            "initialized global state belongs to a different module"
        );
        for (global, value) in self.globals.iter().zip(&captured.globals) {
            global
                .set(&mut *store, value.clone())
                .map_err(wasmtime_anyhow)?;
        }
        if let (Some(stack), Some(limits)) = (&self.stack_limits, captured.stack_limits) {
            stack
                .set
                .call_async(&mut *store, limits)
                .await
                .map_err(wasmtime_anyhow)?;
        }
        Ok(())
    }
}
