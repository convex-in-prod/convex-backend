//! Static restoration metadata for initialized Core Wasm modules.
//!
//! Keep this module identical in the precompiler and backend. Changing
//! inspection or restoration semantics requires a new metadata kind; existing
//! publications without metadata are inspected by the backend.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use anyhow::Context as _;
use serde::{
    Deserialize,
    Serialize,
};
use wasmparser::{
    CompositeInnerType,
    ExternalKind,
    Operator,
    Parser,
    Payload,
    RefType,
    TypeRef,
    ValType,
};

pub const SNAPSHOT_METADATA_KIND: &str = "convex-wasm-module-snapshot-v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotMetadata {
    pub kind: String,
    pub core_sha256: String,
    pub core_size: u64,
    pub state: SnapshotState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum SnapshotState {
    Supported {
        globals: Vec<String>,
        #[serde(rename = "stackLimits")]
        stack_limits: bool,
    },
    Unsupported {
        reason: String,
    },
}

impl SnapshotMetadata {
    pub fn validate_for(&self, core_sha256: &str, core_size: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.kind == SNAPSHOT_METADATA_KIND,
            "unsupported module snapshot metadata version"
        );
        anyhow::ensure!(
            self.core_sha256 == core_sha256 && self.core_size == core_size,
            "module snapshot metadata differs from its Core artifact"
        );
        match &self.state {
            SnapshotState::Supported { globals, .. } => {
                let mut names = BTreeSet::new();
                anyhow::ensure!(
                    globals
                        .iter()
                        .all(|name| !name.is_empty() && names.insert(name)),
                    "module snapshot globals must have unique nonempty export names"
                );
            },
            SnapshotState::Unsupported { reason } => anyhow::ensure!(
                !reason.is_empty(),
                "module snapshot rejection has no reason"
            ),
        }
        Ok(())
    }
}

impl SnapshotState {
    pub fn inspect(core: &[u8]) -> anyhow::Result<SnapshotState> {
        let unsupported = |reason: &str| {
            Ok(SnapshotState::Unsupported {
                reason: reason.to_owned(),
            })
        };
        let mut globals = Vec::new();
        let mut function_types = Vec::new();
        let mut functions = Vec::new();
        let mut interface_functions = Vec::new();
        let mut imported_globals = Vec::new();
        let mut imported_functions = 0u32;
        let mut function_index = 0u32;
        let mut memory_count = 0;
        let mut imports_graph_memory = false;
        let mut start_function = None;
        let mut proven_memory_start = false;
        let mut global_exports = BTreeMap::<u32, String>::new();
        let mut accessors = BTreeMap::<u32, &str>::new();
        let mut stack_base = None;
        let mut stack_end = None;
        let mut stack_set = None;
        for payload in Parser::new(0).parse_all(core) {
            match payload? {
                Payload::Version { encoding, .. } => {
                    if encoding != wasmparser::Encoding::Module {
                        return unsupported("component state is not an initialized Core image");
                    }
                },
                Payload::TypeSection(types) => {
                    // GC aggregate objects have state outside linear memory.
                    // An ordinary function-only type section needs no such image.
                    for group in types {
                        for ty in group?.into_types() {
                            if !ty.is_final
                                || ty.supertype_idx.is_some()
                                || ty.composite_type.shared
                                || ty.composite_type.descriptor_idx.is_some()
                                || ty.composite_type.describes_idx.is_some()
                            {
                                return unsupported("unsupported snapshot type state");
                            }
                            let CompositeInnerType::Func(function) = ty.composite_type.inner else {
                                return unsupported(
                                    "GC aggregate types require additional snapshot state",
                                );
                            };
                            function_types.push(function);
                        }
                    }
                },
                Payload::ImportSection(imports) => {
                    for import in imports.into_imports() {
                        let import = import?;
                        match import.ty {
                            TypeRef::Global(ty) => {
                                if matches!(ty.content_type, ValType::Ref(_)) || ty.shared {
                                    return unsupported("reference or shared global state");
                                }
                                if ty.mutable
                                    && !((import.module == "env"
                                        && import.name == "__stack_pointer")
                                        || matches!(import.module, "GOT.mem" | "GOT.func"))
                                {
                                    return unsupported(
                                        "mutable global import outside graph relocation",
                                    );
                                }
                                imported_globals.push((import.module, import.name, ty.mutable));
                                globals.push(ty);
                            },
                            TypeRef::Func(index) | TypeRef::FuncExact(index) => {
                                interface_functions.push(functions.len() as u32);
                                imported_functions += 1;
                                functions.push(index);
                            },
                            TypeRef::Table(ty) => {
                                if ty.element_type != RefType::FUNCREF || ty.shared {
                                    return unsupported(
                                        "table retains state other than graph functions",
                                    );
                                }
                            },
                            TypeRef::Memory(ty) => {
                                if ty.shared || ty.memory64 {
                                    return unsupported("shared or memory64 image");
                                }
                                memory_count += 1;
                                imports_graph_memory =
                                    import.module == "env" && import.name == "memory";
                            },
                            _ => {},
                        }
                    }
                    function_index = imported_functions;
                },
                Payload::FunctionSection(entries) => {
                    for index in entries {
                        functions.push(index?);
                    }
                },
                Payload::StartSection { func, .. } => {
                    start_function = Some(func);
                },
                Payload::GlobalSection(entries) => {
                    for entry in entries {
                        let entry = entry?;
                        if matches!(entry.ty.content_type, ValType::Ref(_)) || entry.ty.shared {
                            return unsupported("reference or shared global state");
                        }
                        globals.push(entry.ty);
                    }
                },
                Payload::MemorySection(memories) => {
                    for memory in memories {
                        let memory = memory?;
                        if memory.shared || memory.memory64 {
                            return unsupported("shared or memory64 image");
                        }
                        memory_count += 1;
                    }
                },
                Payload::TableSection(tables) => {
                    for table in tables {
                        let table = table?;
                        if table.ty.element_type != RefType::FUNCREF || table.ty.shared {
                            return unsupported("table retains state other than graph functions");
                        }
                    }
                },
                Payload::ExportSection(exports) => {
                    for export in exports {
                        let export = export?;
                        match export.kind {
                            ExternalKind::Global => {
                                global_exports
                                    .entry(export.index)
                                    .or_insert_with(|| export.name.to_owned());
                            },
                            ExternalKind::Func => {
                                interface_functions.push(export.index);
                                if matches!(
                                    export.name,
                                    "emscripten_stack_get_base"
                                        | "emscripten_stack_get_end"
                                        | "emscripten_stack_set_limits"
                                ) {
                                    if accessors.insert(export.index, export.name).is_some() {
                                        return unsupported("stack accessors alias one function");
                                    }
                                }
                            },
                            _ => {},
                        }
                    }
                },
                Payload::ElementSection(elements) => {
                    for element in elements {
                        if let wasmparser::ElementItems::Expressions(ty, _) = element?.items
                            && ty != RefType::FUNCREF
                        {
                            return unsupported("element segment retains non-function references");
                        }
                    }
                },
                Payload::CodeSectionEntry(body) => {
                    if start_function == Some(function_index) {
                        // Instantiation runs starts before image restoration.
                        // Permit only the linker's single relocated zero-fill:
                        // it writes the ordinary graph memory that restoration
                        // replaces, with no calls or effects outside that memory.
                        // Function names confer no authority, and extra memories
                        // could retain writes outside the restored image.
                        if memory_count != 1
                            || !imports_graph_memory
                            || body.get_locals_reader()?.get_count() != 0
                        {
                            return unsupported(
                                "start function is not a proven memory initializer",
                            );
                        }
                        let ops = body
                            .get_operators_reader()?
                            .into_iter()
                            .take(8)
                            .collect::<wasmparser::Result<Vec<_>>>()?;
                        let (offset, global_index, length) = match ops.as_slice() {
                            [
                                Operator::I32Const { value: offset },
                                Operator::GlobalGet { global_index },
                                Operator::I32Add,
                                // memory.fill consumes destination, byte, length.
                                Operator::I32Const { value: 0 },
                                Operator::I32Const { value: length },
                                Operator::MemoryFill { mem: 0 },
                                Operator::End,
                            ] => (*offset, *global_index, *length),
                            _ => {
                                return unsupported(
                                    "start function is not a proven memory initializer",
                                )
                            },
                        };
                        if offset < 0
                            || length <= 0
                            || !matches!(
                                imported_globals.get(global_index as usize),
                                Some(("env", "__memory_base", false))
                            )
                            || !globals
                                .get(global_index as usize)
                                .is_some_and(|ty| ty.content_type == ValType::I32)
                        {
                            return unsupported(
                                "start function is not a proven memory initializer",
                            );
                        }
                        proven_memory_start = true;
                    }
                    let accessor = accessors.get(&function_index).copied();
                    if let Some(name) = accessor {
                        let type_index = functions
                            .get(function_index as usize)
                            .context("stack accessor references missing function")?;
                        let ty = function_types
                            .get(*type_index as usize)
                            .context("stack accessor references missing function type")?;
                        let valid = if name == "emscripten_stack_set_limits" {
                            ty.params() == [ValType::I32, ValType::I32] && ty.results().is_empty()
                        } else {
                            ty.params().is_empty() && ty.results() == [ValType::I32]
                        };
                        if !valid {
                            return unsupported("stack accessor has an unsupported signature");
                        }
                    }
                    let mut accessor_ops = Vec::new();
                    let mut ops = body.get_operators_reader()?;
                    while !ops.eof() {
                        let op = ops.read()?;
                        match &op {
                            Operator::TableSet { .. }
                            | Operator::TableInit { .. }
                            | Operator::TableCopy { .. }
                            | Operator::TableFill { .. }
                            | Operator::TableGrow { .. }
                            | Operator::ElemDrop { .. }
                            | Operator::DataDrop { .. } => {
                                return unsupported("guest mutates table or segment state");
                            },
                            Operator::GlobalSet { global_index } => {
                                if let Some((module, name, _)) =
                                    imported_globals.get(*global_index as usize)
                                {
                                    if *module != "env" || *name != "__stack_pointer" {
                                        return unsupported("guest writes a relocation global");
                                    }
                                }
                            },
                            _ => {},
                        }
                        if accessor.is_some() {
                            if accessor_ops.len() == 5 {
                                return unsupported(
                                    "stack accessor is not a scalar get/set operation",
                                );
                            }
                            accessor_ops.push(op);
                        }
                    }
                    if let Some(name) = accessor {
                        if body.get_locals_reader()?.get_count() != 0 {
                            return unsupported("stack accessor declares local state");
                        }
                        match (name, accessor_ops.as_slice()) {
                            (
                                "emscripten_stack_get_base",
                                [Operator::GlobalGet { global_index }, Operator::End],
                            ) => stack_base = Some(*global_index),
                            (
                                "emscripten_stack_get_end",
                                [Operator::GlobalGet { global_index }, Operator::End],
                            ) => stack_end = Some(*global_index),
                            (
                                "emscripten_stack_set_limits",
                                [first, second, third, fourth, Operator::End],
                            ) if matches!(first, Operator::LocalGet { local_index: 0 })
                                && matches!(third, Operator::LocalGet { local_index: 1 }) =>
                            {
                                let (
                                    Operator::GlobalSet { global_index: base },
                                    Operator::GlobalSet { global_index: end },
                                ) = (second, fourth)
                                else {
                                    return unsupported(
                                        "stack setter does not write its two globals",
                                    );
                                };
                                stack_set = Some((*base, *end));
                            },
                            _ => {
                                return unsupported(
                                    "stack accessor is not a scalar get/set operation",
                                )
                            },
                        }
                    }
                    function_index += 1;
                },
                _ => {},
            }
        }
        // References may exist in locals and exception payloads during a call,
        // but must not cross function interfaces into host roots. Scalar
        // signatures alone do not exclude host-owned resources behind numeric
        // handles: the lifecycle also requires the closed host import boundary,
        // completed guest calls, empty handle/operation tables and no pending
        // exception before granting image authority.
        for index in interface_functions {
            let ty = function_types
                .get(
                    *functions
                        .get(index as usize)
                        .context("missing interface function")? as usize,
                )
                .context("missing interface function type")?;
            if ty
                .params()
                .iter()
                .chain(ty.results())
                .any(|ty| matches!(ty, ValType::Ref(_)))
            {
                return unsupported("reference-valued function interface");
            }
        }
        if start_function.is_some() && !proven_memory_start {
            // Imported starts have no inspectable body. A missing definition
            // must not acquire image authority either.
            return unsupported("start function is not a proven memory initializer");
        }
        let hidden_stack = match (stack_base, stack_end, stack_set) {
            (None, None, None) if accessors.is_empty() => None,
            (Some(base), Some(end), Some(pair)) if base != end && pair == (base, end) => {
                for index in [base, end] {
                    let ty = globals
                        .get(index as usize)
                        .context("stack accessor references missing global")?;
                    if (index as usize) < imported_globals.len()
                        || !ty.mutable
                        || ty.content_type != ValType::I32
                    {
                        return unsupported("stack limits are not defined mutable i32 globals");
                    }
                }
                Some((base, end))
            },
            _ => return unsupported("incomplete stack-limit accessors"),
        };
        let mut names = Vec::new();
        for (index, ty) in globals.iter().enumerate().skip(imported_globals.len()) {
            if !ty.mutable
                || hidden_stack
                    .is_some_and(|(base, end)| index == base as usize || index == end as usize)
            {
                continue;
            }
            let Some(name) = global_exports.get(&(index as u32)) else {
                return unsupported("hidden mutable global has no restoration operation");
            };
            names.push(name.clone());
        }
        Ok(SnapshotState::Supported {
            globals: names,
            stack_limits: hidden_stack.is_some(),
        })
    }
}

#[cfg(test)]
mod tests {
    use wasm_encoder::{
        ConstExpr,
        ExportKind,
        ExportSection,
        GlobalSection,
        GlobalType,
        Module,
        ValType,
    };

    use super::*;

    fn scalar_module(exported: bool) -> Vec<u8> {
        let mut globals = GlobalSection::new();
        globals.global(
            GlobalType {
                val_type: ValType::I32,
                mutable: true,
                shared: false,
            },
            &ConstExpr::i32_const(7),
        );
        let mut module = Module::new();
        module.section(&globals);
        if exported {
            let mut exports = ExportSection::new();
            exports.export("counter", ExportKind::Global, 0);
            module.section(&exports);
        }
        module.finish()
    }

    #[test]
    fn snapshot_metadata_retains_scalar_layout_and_rejects_hidden_state() -> anyhow::Result<()> {
        let core = scalar_module(true);
        let metadata = SnapshotMetadata {
            kind: SNAPSHOT_METADATA_KIND.to_owned(),
            core_sha256: "a".repeat(64),
            core_size: core.len() as u64,
            state: SnapshotState::inspect(&core)?,
        };
        assert_eq!(
            metadata.state,
            SnapshotState::Supported {
                globals: vec!["counter".to_owned()],
                stack_limits: false
            }
        );
        let decoded: SnapshotMetadata = serde_json::from_slice(&serde_json::to_vec(&metadata)?)?;
        assert_eq!(metadata, decoded);
        decoded.validate_for(&metadata.core_sha256, metadata.core_size)?;
        assert!(decoded
            .validate_for(&"b".repeat(64), metadata.core_size)
            .is_err());
        assert!(decoded
            .validate_for(&metadata.core_sha256, metadata.core_size + 1)
            .is_err());
        assert!(matches!(
            SnapshotState::inspect(&scalar_module(false))?,
            SnapshotState::Unsupported { .. }
        ));
        Ok(())
    }

    #[test]
    fn snapshot_metadata_rejects_unknown_rules_and_malformed_layouts() -> anyhow::Result<()> {
        let mut metadata = SnapshotMetadata {
            kind: SNAPSHOT_METADATA_KIND.to_owned(),
            core_sha256: "a".repeat(64),
            core_size: 8,
            state: SnapshotState::Supported {
                globals: vec![],
                stack_limits: false,
            },
        };
        metadata.kind.push_str("-unknown");
        assert!(metadata.validate_for(&metadata.core_sha256, 8).is_err());
        metadata.kind = SNAPSHOT_METADATA_KIND.to_owned();
        for names in [vec!["".to_owned()], vec!["counter".to_owned(); 2]] {
            metadata.state = SnapshotState::Supported {
                globals: names,
                stack_limits: false,
            };
            assert!(metadata.validate_for(&metadata.core_sha256, 8).is_err());
        }
        let mut json = serde_json::to_value(&metadata)?;
        json["state"]["stackLimits"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<SnapshotMetadata>(json).is_err());
        assert!(SnapshotState::inspect(b"not wasm").is_err());
        Ok(())
    }
}
