use std::{
    borrow::Cow,
    collections::BTreeMap,
    ffi::c_char,
    num::Saturating,
    sync::Arc,
};

use async_lru::async_lru::SizedValue;
use async_trait::async_trait;
use common::{
    document::ParsedDocument,
    knobs::{
        ANALYZE_CODE_CACHE_MAX_BYTES,
        ANALYZE_SOURCE_MAP_CACHE_MAX_BYTES,
    },
    runtime::Runtime,
};
use deno_core::v8;
use model::{
    modules::{
        module_versions::{
            FullModuleSource,
            SourceMap,
        },
        types::ModuleMetadata,
    },
    source_packages::types::SourcePackage,
};
use parking_lot::Mutex;
use sync_types::CanonicalizedModulePath;
use value::heap_size::HeapSize;

use crate::{
    environment::ModuleCodeCacheResult,
    helpers::source_map_from_slice,
    metrics::{
        log_module_code_cache_outcome,
        ModuleCodeCacheOutcome,
    },
};

/// Owns exact source inputs for one analysis. Only immutable acceleration data
/// is shared across roots: each root still creates and evaluates fresh modules.
pub struct AnalysisModuleSnapshot {
    modules: BTreeMap<CanonicalizedModulePath, Arc<V8ModuleSource>>,
    code_limit: usize,
    source_map_limit: usize,
    cache: Mutex<AnalysisCache>,
}

#[derive(Default)]
struct AnalysisCache {
    code: BTreeMap<CanonicalizedModulePath, Arc<[u8]>>,
    code_bytes: usize,
    code_budget_refused: bool,
    source_maps: BTreeMap<CanonicalizedModulePath, Option<Arc<sourcemap::SourceMap>>>,
    source_map_bytes: usize,
}

impl From<BTreeMap<CanonicalizedModulePath, Arc<V8ModuleSource>>> for AnalysisModuleSnapshot {
    fn from(modules: BTreeMap<CanonicalizedModulePath, Arc<V8ModuleSource>>) -> Self {
        Self {
            modules,
            code_limit: *ANALYZE_CODE_CACHE_MAX_BYTES,
            source_map_limit: *ANALYZE_SOURCE_MAP_CACHE_MAX_BYTES,
            cache: Mutex::default(),
        }
    }
}

impl AnalysisModuleSnapshot {
    pub fn lookup(
        self: &Arc<Self>,
        path: &CanonicalizedModulePath,
    ) -> Option<(Arc<V8ModuleSource>, ModuleCodeCacheResult)> {
        let source = self.modules.get(path)?.clone();
        let cache = self.cache.lock();
        let cached = cache.code.get(path).cloned();
        if self.code_limit == 0
            || (cached.is_none()
                && (cache.code_budget_refused || cache.code_bytes >= self.code_limit))
        {
            return Some((source, ModuleCodeCacheResult::Disabled));
        }
        drop(cache);
        let snapshot = self.clone();
        let path = path.clone();
        let previous = cached.clone();
        let populate = Box::new(move |data| snapshot.put_code(path, previous, data));
        let outcome = match cached {
            Some(data) => ModuleCodeCacheResult::Cached(data, populate),
            None => ModuleCodeCacheResult::Uncached(populate),
        };
        Some((source, outcome))
    }

    fn put_code(
        &self,
        path: CanonicalizedModulePath,
        rejected: Option<Arc<[u8]>>,
        data: Arc<[u8]>,
    ) {
        // Compilation happens without this lock. Concurrent first misses are
        // allowed, but replacing a winner cannot exceed the retention budget.
        let mut cache = self.cache.lock();
        if let Some(current) = cache.code.get(&path) {
            if !rejected
                .as_ref()
                .is_some_and(|old| Arc::ptr_eq(old, current))
            {
                return;
            }
        }
        let previous = cache.code.get(&path).map_or(0, |old| {
            old.len()
                .checked_add(path.as_str().len())
                .and_then(|bytes| bytes.checked_add(128))
                .expect("retained code entry charge overflowed")
        });
        let without_previous = cache
            .code_bytes
            .checked_sub(previous)
            .expect("retained code bytes do not include replaced entry");
        let next = without_previous
            .checked_add(data.len())
            .and_then(|bytes| bytes.checked_add(path.as_str().len()))
            .and_then(|bytes| bytes.checked_add(128))
            .filter(|bytes| *bytes <= self.code_limit);
        let Some(next) = next else {
            // Stop generating new entries for this snapshot once its measured
            // budget refuses one. Existing hits remain usable; later roots
            // cannot repeatedly serialize the same oversized dependency.
            cache.code_budget_refused = true;
            // A rejected entry is no longer useful. Removing it also prevents
            // every later root from repeatedly offering the same rejected bytes.
            if cache.code.remove(&path).is_some() {
                cache.code_bytes = without_previous;
            }
            log_module_code_cache_outcome(ModuleCodeCacheOutcome::BudgetRefused);
            return;
        };
        cache.code.insert(path, data);
        cache.code_bytes = next;
    }

    pub fn source_map(
        &self,
        path: &CanonicalizedModulePath,
    ) -> anyhow::Result<Option<Arc<sourcemap::SourceMap>>> {
        use anyhow::Context;

        if let Some(source_map) = self.cache.lock().source_maps.get(path) {
            return Ok(source_map.clone());
        }
        let module = self
            .modules
            .get(path)
            .context("analysis source map has no module")?;
        let source_map = module
            .source_map()
            .and_then(|source| source_map_from_slice(source.as_bytes()))
            .map(Arc::new);
        if self.source_map_limit == 0 {
            return Ok(source_map);
        }
        // Charge owned text, tokens, collection capacity and entry metadata.
        // This is a logical cache budget, not a claim about allocator RSS.
        let map_bytes = if let Some(map) = &source_map {
            #[derive(serde::Deserialize)]
            struct MapStorage<'a> {
                #[serde(rename = "sourcesContent")]
                contents: Option<Vec<serde::de::IgnoredAny>>,
                #[serde(borrow, default)]
                mappings: Cow<'a, str>,
            }
            // The parser retains excess sourcesContent slots, including nulls,
            // but source_contents() stops at sources.len(). This second JSON
            // scan counts slots without retaining content: IgnoredAny is
            // zero-sized. Unescaped mappings borrow the existing JSON text.
            // If all retained slots cannot be inspected, use the parsed map
            // without caching it. This also preserves tolerated header formats.
            let Ok(storage) = serde_json::from_str::<MapStorage<'_>>(
                module.source_map().expect("parsed map lost source"),
            ) else {
                return Ok(source_map);
            };
            if storage.contents.map_or(0, |contents| contents.len())
                > map.get_source_count() as usize
            {
                return Ok(source_map);
            }
            // sourcemap::decode_regular reserves one token per separator plus
            // ten, including empty segments which produce no decoded tokens.
            // Counting only get_token_count() undercharges sparse maps.
            let token_capacity = Saturating(
                storage
                    .mappings
                    .bytes()
                    .filter(|&b| matches!(b, b',' | b';'))
                    .count(),
            ) + Saturating(10);
            let token_bytes =
                token_capacity * Saturating(std::mem::size_of::<sourcemap::RawToken>());
            let text_bytes = map
                .get_file()
                .into_iter()
                .chain(map.get_source_root())
                .chain(map.sources())
                .chain(map.sources())
                .chain(map.names())
                .chain(map.source_contents().flatten())
                .fold(Saturating(0), |bytes, text| bytes + Saturating(text.len()));
            let entries = Saturating(map.get_source_count() as usize)
                + Saturating(map.get_name_count() as usize)
                + Saturating(map.ignore_list().count());
            // SourceView lazily retains a slice for every line it traverses.
            // Reserve for that eventual growth, not just today's parsed text.
            let line_bytes =
                map.source_contents()
                    .flatten()
                    .fold(Saturating(0), |bytes, source| {
                        // SourceView also splits bare CR; str::lines does not.
                        let separators = source
                            .bytes()
                            .filter(|&b| matches!(b, b'\r' | b'\n'))
                            .count();
                        bytes
                            + (Saturating(separators) + Saturating(1))
                                * Saturating(std::mem::size_of::<&str>())
                    });
            let payload_bytes = text_bytes + line_bytes + entries * Saturating(128);
            (Saturating(std::mem::size_of::<sourcemap::SourceMap>())
                + token_bytes
                + Saturating(2) * payload_bytes)
                .0
        } else {
            0
        };
        let charge = path
            .as_str()
            .len()
            .saturating_add(128)
            .saturating_add(map_bytes);
        let mut cache = self.cache.lock();
        if let Some(existing) = cache.source_maps.get(path) {
            return Ok(existing.clone());
        }
        // A saturated estimate always refuses retention, including at a
        // configured usize::MAX limit. Parsing remains an optional aid.
        if charge < usize::MAX
            && charge <= self.source_map_limit.saturating_sub(cache.source_map_bytes)
        {
            cache.source_maps.insert(path.clone(), source_map.clone());
            cache.source_map_bytes += charge;
        }
        Ok(source_map)
    }
}

#[cfg(test)]
mod analysis_tests {
    use model::modules::module_versions::ModuleSource;

    use super::*;

    fn snapshot(
        code_limit: usize,
        source_map_limit: usize,
        source: &str,
    ) -> Arc<AnalysisModuleSnapshot> {
        let source_map = r#"{"version":3,"sources":["root.ts"],"names":[],"mappings":"AAAA"}"#;
        let mut snapshot = AnalysisModuleSnapshot::from(BTreeMap::from([(
            "root.js".parse().unwrap(),
            Arc::new(V8ModuleSource::new(FullModuleSource {
                source: ModuleSource::new(source),
                source_map: Some(source_map.into()),
            })),
        )]));
        snapshot.code_limit = code_limit;
        snapshot.source_map_limit = source_map_limit;
        Arc::new(snapshot)
    }

    #[test]
    fn disabled_analysis_cache_has_no_population_callback() {
        let snapshot = snapshot(0, 0, "export const value = 1;");
        assert!(matches!(
            snapshot.lookup(&"root.js".parse().unwrap()).unwrap().1,
            ModuleCodeCacheResult::Disabled
        ));
    }

    #[test]
    fn compiled_data_is_shared_only_within_exact_snapshot() {
        let path = "root.js".parse().unwrap();
        let first = snapshot(1024, 1024, "export const value = 1;");
        let ModuleCodeCacheResult::Uncached(populate) = first.lookup(&path).unwrap().1 else {
            panic!("first lookup must miss");
        };
        populate(Arc::from([1_u8, 2, 3]));
        let ModuleCodeCacheResult::Cached(data, _) = first.lookup(&path).unwrap().1 else {
            panic!("later root must see compiled bytes");
        };
        assert_eq!(&*data, &[1, 2, 3]);
        let second = snapshot(1024, 1024, "export const value = 2;");
        assert!(matches!(
            second.lookup(&path).unwrap().1,
            ModuleCodeCacheResult::Uncached(_)
        ));
    }

    #[test]
    fn rejected_data_replaced_without_overwriting_concurrent_winner() {
        let snapshot = snapshot(1024, 1024, "export const value = 1;");
        let path = "root.js".parse().unwrap();
        let ModuleCodeCacheResult::Uncached(populate) = snapshot.lookup(&path).unwrap().1 else {
            panic!("first lookup must miss");
        };
        populate(Arc::from([1_u8]));
        let ModuleCodeCacheResult::Cached(_, first) = snapshot.lookup(&path).unwrap().1 else {
            panic!("cache entry missing");
        };
        let ModuleCodeCacheResult::Cached(_, late) = snapshot.lookup(&path).unwrap().1 else {
            panic!("cache entry missing");
        };
        first(Arc::from([2_u8]));
        late(Arc::from([3_u8]));
        let ModuleCodeCacheResult::Cached(data, _) = snapshot.lookup(&path).unwrap().1 else {
            panic!("replacement missing");
        };
        assert_eq!(&*data, &[2]);
        assert!(snapshot.cache.lock().code_bytes <= snapshot.code_limit);
    }

    #[test]
    fn budgets_include_keys_and_oversized_replacement_removes_rejected_data() {
        let snapshot = snapshot(140, 0, "export const value = 1;");
        let path = "root.js".parse().unwrap();
        let ModuleCodeCacheResult::Uncached(populate) = snapshot.lookup(&path).unwrap().1 else {
            panic!("first lookup must miss");
        };
        populate(Arc::from([1_u8]));
        let ModuleCodeCacheResult::Cached(_, replace) = snapshot.lookup(&path).unwrap().1 else {
            panic!("small entry should fit");
        };
        replace(Arc::from([0_u8; 140]));
        assert!(snapshot.cache.lock().code.is_empty());
        assert_eq!(snapshot.cache.lock().code_bytes, 0);
        assert!(matches!(
            snapshot.lookup(&path).unwrap().1,
            ModuleCodeCacheResult::Disabled
        ));
        assert!(snapshot.source_map(&path).unwrap().is_some());
        assert!(snapshot.cache.lock().source_maps.is_empty());
    }

    #[test]
    fn late_population_after_budget_refusal_preserves_useful_winners() {
        let snapshot = snapshot(140, 0, "export const value = 1;");
        let path = "root.js".parse().unwrap();
        let ModuleCodeCacheResult::Uncached(oversized) = snapshot.lookup(&path).unwrap().1 else {
            panic!("first lookup must miss");
        };
        let ModuleCodeCacheResult::Uncached(winner) = snapshot.lookup(&path).unwrap().1 else {
            panic!("concurrent lookup must miss");
        };
        let ModuleCodeCacheResult::Uncached(late) = snapshot.lookup(&path).unwrap().1 else {
            panic!("concurrent lookup must miss");
        };
        oversized(Arc::from([0_u8; 140]));
        assert!(snapshot.cache.lock().code_budget_refused);
        // Bytes already produced by an in-flight compilation may still fit.
        winner(Arc::from([1_u8]));
        late(Arc::from([0_u8; 140]));
        let ModuleCodeCacheResult::Cached(data, oversized) = snapshot.lookup(&path).unwrap().1
        else {
            panic!("late callback must not remove the useful winner");
        };
        assert_eq!(&*data, &[1]);
        let ModuleCodeCacheResult::Cached(_, late) = snapshot.lookup(&path).unwrap().1 else {
            panic!("budget refusal must preserve existing hits");
        };
        oversized(Arc::from([0_u8; 140]));
        assert_eq!(snapshot.cache.lock().code_bytes, 0);
        late(Arc::from([2_u8]));
        let ModuleCodeCacheResult::Cached(data, _) = snapshot.lookup(&path).unwrap().1 else {
            panic!("in-flight replacement should fit after rejected data removal");
        };
        assert_eq!(&*data, &[2]);
        assert_eq!(snapshot.cache.lock().code_bytes, 136);
    }

    #[test]
    fn source_map_budget_charges_contents_not_exposed_by_iterator() {
        let mut snapshot = AnalysisModuleSnapshot::from(BTreeMap::from([(
            "root.js".parse().unwrap(),
            Arc::new(V8ModuleSource::new(FullModuleSource {
                source: ModuleSource::new("export const value = 1;"),
                // The parser accepts these contents, although sources() and
                // source_contents() expose none of their retained storage.
                source_map: Some(
                    serde_json::json!({
                        "version": 3,
                        "sources": [],
                        "sourcesContent": [null, "x".repeat(4096)],
                        "names": [],
                        "mappings": "",
                    })
                    .to_string()
                    .into(),
                ),
            })),
        )]));
        snapshot.source_map_limit = 1024;
        let map = snapshot
            .source_map(&"root.js".parse().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(map.source_contents().count(), 0);
        assert_eq!(map.get_source_contents(1).unwrap().len(), 4096);
        assert!(snapshot.cache.lock().source_maps.is_empty());
        assert_eq!(snapshot.cache.lock().source_map_bytes, 0);
    }

    #[test]
    fn ordinary_source_map_retains_default_budget_capacity() {
        let source_map = serde_json::json!({
            "version": 3,
            "sources": ["root.ts"],
            "sourcesContent": ["const value = 1;\r".repeat(1 << 16)],
            "names": [],
            "mappings": "AAAA",
            "ignoreList": [0],
        })
        .to_string();
        assert!(source_map.len() > 1 << 20);
        let mut snapshot = AnalysisModuleSnapshot::from(BTreeMap::from([(
            "root.js".parse().unwrap(),
            Arc::new(V8ModuleSource::new(FullModuleSource {
                source: ModuleSource::new("export const value = 1;"),
                source_map: Some(source_map.into()),
            })),
        )]));
        snapshot.source_map_limit = 32 << 20;
        let path = "root.js".parse().unwrap();
        let first = snapshot.source_map(&path).unwrap().unwrap();
        let second = snapshot.source_map(&path).unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(snapshot.cache.lock().source_map_bytes <= snapshot.source_map_limit);
    }

    #[test]
    fn source_map_budget_charges_empty_mapping_segments() {
        for mappings in [";".repeat(4096), ",".repeat(4096), "\\u003b".repeat(4096)] {
            let mut snapshot = AnalysisModuleSnapshot::from(BTreeMap::from([(
                "root.js".parse().unwrap(),
                Arc::new(V8ModuleSource::new(FullModuleSource {
                    source: ModuleSource::new("export const value = 1;"),
                    source_map: Some(
                        format!(
                            r#"{{"version":3,"sources":[],"names":[],"mappings":"{mappings}"}}"#,
                        )
                        .into(),
                    ),
                })),
            )]));
            snapshot.source_map_limit = 4096;
            let map = snapshot
                .source_map(&"root.js".parse().unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(map.get_token_count(), 0);
            assert!(snapshot.cache.lock().source_maps.is_empty());
            assert_eq!(snapshot.cache.lock().source_map_bytes, 0);
        }
    }

    #[test]
    fn parsed_source_maps_are_shared_without_v8_state() {
        let snapshot = snapshot(0, 4096, "export const value = 1;");
        let path = "root.js".parse().unwrap();
        let first = snapshot.source_map(&path).unwrap().unwrap();
        let second = snapshot.source_map(&path).unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(snapshot.cache.lock().source_map_bytes <= snapshot.source_map_limit);
    }
}

/// A `ModuleLoader` that also has the ability to store V8 code caches.
#[async_trait]
pub trait ModuleCache<RT: Runtime>: Sync + Send + 'static {
    /// Approximately the same thing as
    /// [`ModuleLoader::get_module_with_metadata`][model::config::module_loader::ModuleLoader::get_module_with_metadata],
    /// but returns a different type. As with that method, note that
    /// `source_package` may differ from `module_metadata.source_package_id`.
    async fn get_module_with_metadata(
        &self,
        module_metadata: &ParsedDocument<ModuleMetadata>,
        source_package: &ParsedDocument<SourcePackage>,
    ) -> anyhow::Result<Arc<V8ModuleSource>>;
    fn put_cached_code(&self, module_metadata: &ModuleMetadata, cached_data: Arc<[u8]>);
    fn get_cached_code(&self, module_metadata: &ModuleMetadata) -> Option<Arc<[u8]>>;
}

impl<RT: Runtime> dyn ModuleCache<RT> {
    pub fn code_cache_result(
        self: Arc<Self>,
        module_metadata: &Arc<ParsedDocument<ModuleMetadata>>,
    ) -> ModuleCodeCacheResult {
        let cached_data = self.get_cached_code(module_metadata);
        let module_metadata = module_metadata.clone();
        let populate = Box::new(move |cached_data| {
            self.put_cached_code(&module_metadata, cached_data);
        });
        if let Some(cached_data) = cached_data {
            ModuleCodeCacheResult::Cached(cached_data, populate)
        } else {
            ModuleCodeCacheResult::Uncached(populate)
        }
    }
}

pub enum V8ExternalString {
    OneByte(Arc<[u8]>),
    TwoByte(Arc<[u16]>),
}

impl V8ExternalString {
    pub fn to_source_string(&self) -> String {
        match self {
            Self::OneByte(s) => s.iter().map(|&b| char::from(b)).collect(),
            Self::TwoByte(s) => String::from_utf16_lossy(s),
        }
    }

    fn new(s: &str) -> Self {
        if s.chars().all(|c| (c as u32) < 256) {
            // latin-1 (one-byte) case
            Self::OneByte(s.chars().map(|c| c as u32 as u8).collect::<Vec<_>>().into())
        } else {
            Self::TwoByte(s.encode_utf16().collect::<Vec<_>>().into())
        }
    }

    /// The inverse of [`Self::new`], for consumers that need the source as
    /// plain text rather than as a V8 string — the wasm runtime bundles module
    /// text with esbuild and never builds a V8 string out of it.
    pub fn to_utf8(&self) -> String {
        match self {
            Self::OneByte(s) => s.iter().map(|&byte| byte as char).collect(),
            Self::TwoByte(s) => String::from_utf16_lossy(s),
        }
    }

    pub fn create_v8_string<'s>(
        &self,
        scope: &v8::PinScope<'s, '_, ()>,
    ) -> Option<v8::Local<'s, v8::String>> {
        match self {
            V8ExternalString::OneByte(s) => {
                let p = Arc::into_raw(s.clone());
                unsafe extern "C" fn destructor(p: *mut c_char, len: usize) {
                    unsafe {
                        drop(<Arc<[u8]>>::from_raw(std::ptr::slice_from_raw_parts(
                            p.cast::<u8>(),
                            len,
                        )));
                    }
                }
                unsafe {
                    let v8_string = v8::String::new_external_onebyte_raw(
                        scope,
                        p as *const u8 as *mut c_char,
                        p.len(),
                        destructor,
                    );
                    if v8_string.is_none() {
                        // N.B.: V8 doesn't take ownership in this case
                        drop(Arc::from_raw(p));
                    }
                    v8_string
                }
            },
            V8ExternalString::TwoByte(s) => {
                let p = Arc::into_raw(s.clone());
                unsafe extern "C" fn destructor(p: *mut u16, len: usize) {
                    unsafe {
                        drop(<Arc<[u16]>>::from_raw(std::ptr::slice_from_raw_parts(
                            p, len,
                        )));
                    }
                }
                unsafe {
                    let v8_string = v8::String::new_external_twobyte_raw(
                        scope,
                        p as *const u16 as *mut u16,
                        p.len(),
                        destructor,
                    );
                    if v8_string.is_none() {
                        drop(Arc::from_raw(p));
                    }
                    v8_string
                }
            },
        }
    }
}

pub struct V8ModuleSource {
    source: V8ExternalString,
    source_map: Option<SourceMap>,
}

impl V8ModuleSource {
    pub fn new(source: FullModuleSource) -> Self {
        Self {
            source: V8ExternalString::new(&source.source),
            source_map: source.source_map,
        }
    }

    pub fn source(&self) -> &V8ExternalString {
        &self.source
    }

    pub fn source_map(&self) -> Option<&SourceMap> {
        self.source_map.as_ref()
    }
}

impl HeapSize for V8ExternalString {
    fn heap_size(&self) -> usize {
        match self {
            V8ExternalString::OneByte(s) => s.len(),
            V8ExternalString::TwoByte(s) => s.len() * 2,
        }
    }
}

impl SizedValue for V8ModuleSource {
    fn size(&self) -> u64 {
        (self.source.heap_size() + self.source_map.heap_size()) as u64
    }
}
