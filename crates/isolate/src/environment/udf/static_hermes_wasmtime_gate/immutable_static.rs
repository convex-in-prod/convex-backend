use std::ops::Range;

use anyhow::Context as _;
use wasmtime::{
    ExternType,
    Memory,
    Module,
    Mutability,
    Store,
    ValType,
};

const PREFIX: &str = "convex_wasm_immutable_static_v1_";

/// Evidence from an actual COW mapping of this exact retained Module. Its
/// initialization image and engine cannot change while the Module is alive.
/// This is not authority derived from certificate byte counts alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct VerifiedStaticMemory {
    pub(super) initial_bytes: usize,
    pub(super) shared_bytes: usize,
}

struct Declaration {
    ranges: Vec<Range<usize>>,
    payload_bytes: usize,
    initial_bytes: usize,
}

pub(super) struct ModuleStaticAccounting {
    pub(super) payload_bytes: usize,
    pub(super) separate_image_bytes: usize,
}

fn declaration(module: &Module) -> anyhow::Result<Declaration> {
    let mut ranges = Vec::new();
    let mut payload_bytes = 0usize;
    for export in module
        .exports()
        .filter(|export| export.name().starts_with(PREFIX))
    {
        let ExternType::Global(global) = export.ty() else {
            anyhow::bail!("immutable static declaration is not a global");
        };
        anyhow::ensure!(
            global.mutability() == Mutability::Const && matches!(global.content(), ValType::I32),
            "immutable static declaration must be an immutable i32 global"
        );
        let values = export.name()[PREFIX.len()..]
            .split('_')
            .map(|value| {
                anyhow::ensure!(
                    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
                    "invalid immutable static declaration number"
                );
                value
                    .parse::<u64>()
                    .context("immutable static bound exceeds wasm32")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let [start, end, payload] = values.as_slice() else {
            anyhow::bail!("invalid immutable static declaration fields");
        };
        anyhow::ensure!(
            start < end && *end <= (1u64 << 32) && *payload <= end - start,
            "invalid immutable static declaration range"
        );
        // The exclusive end of a wasm32 interval may be exactly 4 GiB.
        ranges.push(usize::try_from(*start)?..usize::try_from(*end)?);
        payload_bytes = payload_bytes
            .checked_add(usize::try_from(*payload)?)
            .context("immutable static payload size overflow")?;
        anyhow::ensure!(ranges.len() <= 64, "too many immutable static declarations");
    }
    if ranges.is_empty() {
        return Ok(Declaration {
            ranges,
            payload_bytes: 0,
            initial_bytes: 0,
        });
    }
    let Some(ExternType::Memory(memory)) = module.get_export("memory") else {
        anyhow::bail!("immutable static data requires an exported memory");
    };
    anyhow::ensure!(
        module.resources_required().num_memories == 1
            && !memory.is_64()
            && !memory.is_shared()
            && memory.page_size() == 65_536
            && !module
                .imports()
                .any(|import| matches!(import.ty(), ExternType::Memory(_))),
        "immutable static data requires one defined unshared wasm32 memory with 64 KiB pages"
    );
    let initial_bytes = usize::try_from(
        memory
            .minimum()
            .checked_mul(memory.page_size())
            .context("immutable static memory size overflow")?,
    )?;
    ranges.sort_unstable_by_key(|range| range.start);
    let mut previous_end = 0;
    for range in &ranges {
        anyhow::ensure!(
            range.start >= previous_end && range.end <= initial_bytes,
            "immutable static declarations overlap or exceed initial memory"
        );
        previous_end = range.end;
    }
    Ok(Declaration {
        ranges,
        payload_bytes,
        initial_bytes,
    })
}

/// Static payload is retained in the compilation image, not parsed into module
/// metadata. A separate COW image needs a once-per-Module allowance only when
/// it cannot use that same file backing. Reserve its maximum size before any
/// Store can trigger lazy image construction, and retain it with the Module's
/// charge.
pub(super) fn module_accounting(module: &Module) -> anyhow::Result<ModuleStaticAccounting> {
    let declared = declaration(module)?;
    if declared.ranges.is_empty() {
        return Ok(ModuleStaticAccounting {
            payload_bytes: 0,
            separate_image_bytes: 0,
        });
    }
    // Disabling COW creates no lazy initialization image. Payload still lives
    // in the compilation image, so its metadata deduction is independent.
    if !module.engine().get_memory_init_cow() {
        return Ok(ModuleStaticAccounting {
            payload_bytes: declared.payload_bytes,
            separate_image_bytes: 0,
        });
    }
    #[cfg(target_os = "linux")]
    let extra_image = {
        let mappings = mappings()?;
        // Maintained loaders use deserialize_open_file and the generated engine
        // sets force_memory_init_memfd(false). In the pinned Unix implementation,
        // MemoryImageSource::from_file always retains that original file. File
        // coverage alone would not justify this deduction with forced memfd.
        if mappings
            .as_ref()
            .is_some_and(|mappings| image_file_backed(module, mappings))
        {
            0
        } else {
            declared.initial_bytes
        }
    };
    #[cfg(not(target_os = "linux"))]
    let extra_image = declared.initial_bytes;
    Ok(ModuleStaticAccounting {
        payload_bytes: declared.payload_bytes,
        separate_image_bytes: extra_image,
    })
}

pub(super) fn shared_bytes<T>(
    store: &Store<T>,
    module: &Module,
    memory: Memory,
) -> anyhow::Result<usize> {
    let declared = declaration(module)?;
    if declared.ranges.is_empty() {
        return Ok(0);
    }
    if !module.engine().get_memory_init_cow()
        || module.engine().get_memory_reservation() < (1u64 << 32)
    {
        return Ok(0);
    }
    anyhow::ensure!(
        declared.initial_bytes <= memory.data_size(store),
        "immutable static data exceeds current guest memory"
    );
    #[cfg(target_os = "linux")]
    {
        let Some(mappings) = mappings()? else {
            return Ok(0);
        };
        let separate_image_reserved = !image_file_backed(module, &mappings);
        let image = module.image_range();
        let image = image.start as usize..image.end as usize;
        let base = memory.data_ptr(store) as usize;
        // The generated wasm32 engine reserves the full address space. Growth
        // cannot relocate COW pages into a private memcpy allocation. The native
        // compiler certifies these pages contain only non-relocated const data;
        // mutable data and mixed boundary pages retain their full Store charge.
        // SAFETY: sysconf takes no pointers and cannot mutate guest memory.
        let page = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })?;
        anyhow::ensure!(page.is_power_of_two(), "invalid host page size");
        let mut total = 0usize;
        for range in declared.ranges {
            let start = base
                .checked_add(range.start)
                .and_then(|start| start.checked_add(page - 1))
                .context("immutable static page rounding overflow")?
                & !(page - 1);
            let end = base
                .checked_add(range.end)
                .context("immutable static address overflow")?
                & !(page - 1);
            for mapping in mappings.iter().filter(|mapping| mapping.file_backed()) {
                let overlap = start.max(mapping.range.start)..end.min(mapping.range.end);
                if overlap.start >= overlap.end {
                    continue;
                }
                if separate_image_reserved {
                    // Wasmtime owns this defined memory's initialization mapping.
                    // Its separate image was reserved with the compiled Module.
                    total = total
                        .checked_add(overlap.len())
                        .context("static credit overflow")?;
                } else {
                    // Confirm identical file pages, not merely identical content
                    // or a filename. File-loaded AOT already owns this backing.
                    let offset = mapping.offset + (overlap.start - mapping.range.start);
                    let file_range = offset..offset + overlap.len();
                    for source in mappings.iter().filter(|source| {
                        source.file_backed()
                            && source.device == mapping.device
                            && source.inode == mapping.inode
                    }) {
                        let source_range =
                            image.start.max(source.range.start)..image.end.min(source.range.end);
                        if source_range.start >= source_range.end {
                            continue;
                        }
                        let source_offset =
                            source.offset + (source_range.start - source.range.start);
                        let source_end = source_offset + source_range.len();
                        total = total
                            .checked_add(
                                file_range
                                    .end
                                    .min(source_end)
                                    .saturating_sub(file_range.start.max(source_offset)),
                            )
                            .context("static credit overflow")?;
                    }
                }
            }
        }
        Ok(total)
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Until this platform has mapping verification, retain the full charge.
        Ok(0)
    }
}

#[cfg(target_os = "linux")]
struct Mapping {
    range: Range<usize>,
    offset: usize,
    device: String,
    inode: u64,
    private: bool,
}

#[cfg(target_os = "linux")]
impl Mapping {
    fn file_backed(&self) -> bool {
        self.inode != 0 && self.private
    }
}

#[cfg(target_os = "linux")]
fn image_file_backed(module: &Module, mappings: &[Mapping]) -> bool {
    let image = module.image_range();
    let image = image.start as usize..image.end as usize;
    let covered: usize = mappings
        .iter()
        .filter(|mapping| mapping.file_backed())
        .map(|mapping| {
            image
                .end
                .min(mapping.range.end)
                .saturating_sub(image.start.max(mapping.range.start))
        })
        .sum();
    covered == image.len()
}

#[cfg(target_os = "linux")]
fn mappings() -> anyhow::Result<Option<Vec<Mapping>>> {
    let contents = match std::fs::read_to_string("/proc/self/maps") {
        Ok(contents) => contents,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            // Restricted hosts may not expose procfs. This optional accounting
            // evidence grants no credit when unavailable; reserve a possible
            // separate image instead of making an otherwise valid module unusable.
            return Ok(None);
        },
        Err(error) => return Err(error.into()),
    };
    contents
        .lines()
        .map(|line| {
            let mut fields = line.split_whitespace();
            let (start, end) = fields
                .next()
                .context("memory mapping has no range")?
                .split_once('-')
                .context("memory mapping range is malformed")?;
            let start = usize::from_str_radix(start, 16)?;
            let end = usize::from_str_radix(end, 16)?;
            anyhow::ensure!(start < end, "invalid memory mapping range");
            let permissions = fields.next().context("memory mapping has no permissions")?;
            let offset =
                usize::from_str_radix(fields.next().context("memory mapping has no offset")?, 16)?;
            let device = fields
                .next()
                .context("memory mapping has no device")?
                .to_owned();
            let inode = fields
                .next()
                .context("memory mapping has no inode")?
                .parse::<u64>()?;
            Ok(Mapping {
                range: start..end,
                offset,
                device,
                inode,
                private: permissions.ends_with('p'),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(Some)
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use wasm_encoder::{
        ConstExpr,
        DataSection,
        ExportKind,
        ExportSection,
        GlobalSection,
        GlobalType,
        MemorySection,
        MemoryType,
        Module as EncodedModule,
    };
    use wasmtime::{
        Config,
        Engine,
        Instance,
    };

    use super::*;

    fn fixture(names: &[&str]) -> Vec<u8> {
        let mut memory = MemorySection::new();
        memory.memory(MemoryType {
            minimum: 2,
            maximum: Some(4),
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        let mut globals = GlobalSection::new();
        let mut exports = ExportSection::new();
        exports.export("memory", ExportKind::Memory, 0);
        for (index, name) in names.iter().enumerate() {
            globals.global(
                GlobalType {
                    val_type: wasm_encoder::ValType::I32,
                    mutable: false,
                    shared: false,
                },
                &ConstExpr::i32_const(0),
            );
            exports.export(name, ExportKind::Global, index as u32);
        }
        let mut data = DataSection::new();
        data.active(0, &ConstExpr::i32_const(0), vec![13; 65536]);
        let mut module = EncodedModule::new();
        module
            .section(&memory)
            .section(&globals)
            .section(&exports)
            .section(&data);
        module.finish()
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn generated_wasm_memory_static_backing_is_charged_once_and_survives_growth(
    ) -> anyhow::Result<()> {
        let names = ["convex_wasm_immutable_static_v1_1_32769_32768"];
        let wasm = fixture(&names);
        for cow in [true, false] {
            let mut config = Config::new();
            config.memory_init_cow(cow).memory_reservation(1u64 << 32);
            let engine = Engine::new(&config)?;
            let mut file = tempfile::tempfile()?;
            let aot = engine.precompile_module(&wasm)?;
            file.write_all(&aot)?;
            // SAFETY: this private file contains only this Engine's fresh output.
            let module = unsafe { Module::deserialize_open_file(&engine, file) }?;
            let accounting = module_accounting(&module)?;
            assert_eq!(accounting.payload_bytes, 32768);
            assert_eq!(accounting.separate_image_bytes, 0);
            let image = module.image_range();
            assert_eq!(
                super::super::routed_module_cache::generated_module_fixed_bytes(
                    wasm.len() as u64,
                    aot.len() as u64,
                    &module
                )?,
                (image.end as usize - image.start as usize).max(aot.len()) + wasm.len() - 32768
            );
            let mut instances = Vec::new();
            for _ in 0..2 {
                let mut store = Store::new(&engine, ());
                let instance = Instance::new(&mut store, &module, &[])?;
                let memory = instance.get_memory(&mut store, "memory").unwrap();
                let credit = shared_bytes(&store, &module, memory)?;
                assert_eq!(credit, if cow { 7 * 4096 } else { 0 });
                let base = memory.data_ptr(&store);
                memory.grow(&mut store, 1)?;
                // Writes outside certified pages remain instance-owned and do
                // not invalidate the credit. The partial first page is excluded.
                memory.write(&mut store, 0, &[99])?;
                memory.write(&mut store, 32769, &[99])?;
                assert_eq!(base, memory.data_ptr(&store));
                assert_eq!(shared_bytes(&store, &module, memory)?, credit);
                instances.push((store, memory));
            }
            assert_eq!(instances.len(), 2);
        }
        let engine = Engine::default();
        let module = Module::new(&engine, &wasm)?;
        // In-memory compilation owns a separate initialization memfd. Reserve
        // its full possible extent once, even before lazy image creation.
        let accounting = module_accounting(&module)?;
        assert_eq!(accounting.payload_bytes, 32768);
        assert_eq!(accounting.separate_image_bytes, 2 * 65536);
        let mut store = Store::new(&engine, ());
        let instance = Instance::new(&mut store, &module, &[])?;
        let memory = instance.get_memory(&mut store, "memory").unwrap();
        assert_eq!(shared_bytes(&store, &module, memory)?, 7 * 4096);
        Ok(())
    }

    #[test]
    fn generated_wasm_memory_static_declarations_reject_invalid_authority() -> anyhow::Result<()> {
        let engine = Engine::default();
        for names in [
            vec!["convex_wasm_immutable_static_v1_1_262144_5"],
            vec!["convex_wasm_immutable_static_v1_1_2_3"],
            vec!["convex_wasm_immutable_static_v1_1_2"],
            vec![
                "convex_wasm_immutable_static_v1_1_20_0",
                "convex_wasm_immutable_static_v1_19_30_0",
            ],
        ] {
            let module = Module::new(&engine, fixture(&names))?;
            assert!(module_accounting(&module).is_err());
        }
        let module = Module::new(&engine, fixture(&[]))?;
        let accounting = module_accounting(&module)?;
        assert_eq!(accounting.payload_bytes, 0);
        assert_eq!(accounting.separate_image_bytes, 0);
        Ok(())
    }
}
