use std::sync::Arc;

use ::metrics::IntoLabel;
use common::{
    components::{
        CanonicalizedComponentModulePath,
        ResolvedComponentFunctionPath,
    },
    interval::IntervalSet,
    runtime::Runtime,
    types::TabletIndexName,
};
use database::{
    Transaction,
    TransactionReadSet,
};
use deno_core::v8::{
    self,
    scope,
};
use fastrace::{
    local::LocalSpan,
    Event,
};
use parking_lot::Mutex;
use value::{
    sha256::Sha256Digest,
    TableName,
    TableNamespace,
};

use crate::{
    client::Request,
    metrics::{
        create_context_timer,
        log_context_cache_cleared,
        log_context_cache_entry_added,
        log_context_cache_entry_removed,
        log_context_cache_operation,
        ContextCacheOperation,
    },
    module_map::ModuleMap,
};

enum SavedContext {
    Fresh(v8::Global<v8::Context>),
    DatabaseUdf {
        module_path: CanonicalizedComponentModulePath,
        context: v8::Global<v8::Context>,
        module_map: ModuleMap,
        read_set: ContextReadSet,
    },
    HttpAction {
        module_path: CanonicalizedComponentModulePath,
        context: v8::Global<v8::Context>,
        module_map: ModuleMap,
        read_set: ContextReadSet,
    },
}

impl SavedContext {
    fn reusable_key(&self) -> Option<(ReusableContextKind, &CanonicalizedComponentModulePath)> {
        match self {
            Self::Fresh(_) => None,
            Self::DatabaseUdf { module_path, .. } => {
                Some((ReusableContextKind::DatabaseUdf, module_path))
            },
            Self::HttpAction { module_path, .. } => {
                Some((ReusableContextKind::HttpAction, module_path))
            },
        }
    }
}

pub struct ContextCache {
    saved_context: Option<SavedContext>,
    cached_contexts: Arc<CachedContexts>,
}

/// A mirror of the cache keys present in a `ContextCache`.
/// This struct is `Send + Sync` so that it can be used by the isolate
/// scheduler.
pub struct CachedContexts {
    inner: Mutex<CachedContextsInner>,
}

struct CachedContextsInner {
    saved_context: Option<CachedContext>,
}

enum CachedContext {
    DatabaseUdf(CanonicalizedComponentModulePath),
    HttpAction(CanonicalizedComponentModulePath),
}

impl CachedContext {
    fn reusable_key(&self) -> (ReusableContextKind, &CanonicalizedComponentModulePath) {
        match self {
            Self::DatabaseUdf(module_path) => (ReusableContextKind::DatabaseUdf, module_path),
            Self::HttpAction(module_path) => (ReusableContextKind::HttpAction, module_path),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReusableContextKind {
    DatabaseUdf,
    HttpAction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextCacheClearReason {
    FreshContextClobber,
    ReusableContextReplacement,
    AppDefinitionEvaluation,
    CacheDrop,
}

pub struct ContextReadSet {
    pub read_set: TransactionReadSet,
    pub range_hashes: Vec<(
        TableNamespace,
        TabletIndexName,
        TableName,
        IntervalSet,
        Sha256Digest,
    )>,
}

pub(crate) fn context_cache_key(
    function_path: &ResolvedComponentFunctionPath,
) -> CanonicalizedComponentModulePath {
    CanonicalizedComponentModulePath {
        component: function_path.component,
        module_path: function_path.udf_path.module().clone(),
    }
}

impl ContextCache {
    pub fn new() -> Self {
        Self {
            saved_context: None,
            cached_contexts: Arc::new(CachedContexts {
                inner: Mutex::new(CachedContextsInner {
                    saved_context: None,
                }),
            }),
        }
    }

    pub(crate) fn prepare(&mut self, isolate: &mut v8::Isolate) {
        if self.saved_context.is_none() {
            scope!(let scope, isolate);
            let context = make_context(scope);
            self.saved_context = Some(SavedContext::Fresh(v8::Global::new(scope, context)));
        }
    }

    pub(crate) fn has_saved_reusable_context(&self) -> bool {
        matches!(
            self.saved_context,
            Some(SavedContext::DatabaseUdf { .. } | SavedContext::HttpAction { .. })
        )
    }

    /// Remove the scheduler mirror before moving a V8 context out of the cache.
    /// The scheduler can retain this mirror while an idle worker recreates its
    /// isolate, so the order prevents it from advertising a destroyed context.
    fn take_saved_context(&mut self) -> Option<SavedContext> {
        let local_key = self
            .saved_context
            .as_ref()
            .and_then(SavedContext::reusable_key);
        let mut cached_contexts = self.cached_contexts.inner.lock();
        let advertised_key = cached_contexts
            .saved_context
            .as_ref()
            .map(CachedContext::reusable_key);
        assert_eq!(local_key, advertised_key, "context cache mirror drifted");
        cached_contexts.saved_context = None;
        drop(cached_contexts);
        self.saved_context.take()
    }

    pub(crate) fn clear(&mut self, reason: ContextCacheClearReason) {
        let saved_context = self.take_saved_context();
        if let Some((context_kind, _)) = saved_context.as_ref().and_then(SavedContext::reusable_key)
        {
            log_context_cache_cleared(context_kind, reason);
            log_context_cache_entry_removed(context_kind);
        }
    }

    pub(crate) fn get_or_create_fresh_context<'s>(
        &mut self,
        scope: &v8::PinScope<'s, '_, ()>,
    ) -> v8::Local<'s, v8::Context> {
        let saved_context = self.take_saved_context();
        if let Some((context_kind, _)) = saved_context.as_ref().and_then(SavedContext::reusable_key)
        {
            LocalSpan::add_event(Event::new("clobbered_saved_context"));
            log_context_cache_cleared(context_kind, ContextCacheClearReason::FreshContextClobber);
            log_context_cache_entry_removed(context_kind);
        }
        if let Some(SavedContext::Fresh(context)) = saved_context {
            v8::Local::new(scope, context)
        } else {
            make_context(scope)
        }
    }

    pub(crate) fn save_context(
        &mut self,
        module_path: CanonicalizedComponentModulePath,
        context: v8::Global<v8::Context>,
        module_map: ModuleMap,
        read_set: ContextReadSet,
    ) {
        self.save_reusable_context(
            SavedContext::DatabaseUdf {
                module_path: module_path.clone(),
                context,
                module_map,
                read_set,
            },
            CachedContext::DatabaseUdf(module_path),
        );
    }

    pub(crate) fn save_http_action_context(
        &mut self,
        module_path: CanonicalizedComponentModulePath,
        context: v8::Global<v8::Context>,
        module_map: ModuleMap,
        read_set: ContextReadSet,
    ) {
        self.save_reusable_context(
            SavedContext::HttpAction {
                module_path: module_path.clone(),
                context,
                module_map,
                read_set,
            },
            CachedContext::HttpAction(module_path),
        );
    }

    fn save_reusable_context(
        &mut self,
        saved_context: SavedContext,
        cached_context: CachedContext,
    ) {
        let context_kind = saved_context
            .reusable_key()
            .map(|(context_kind, _)| context_kind)
            .expect("saved reusable context must have a reusable kind");
        assert_eq!(
            saved_context.reusable_key(),
            Some(cached_context.reusable_key()),
            "context cache mirror key drifted"
        );
        // A same-isolate nested reusable UDF can save while its reusable parent
        // context is in flight. The parent's later save wins the one cache slot.
        if self.saved_context.is_some() {
            self.clear(ContextCacheClearReason::ReusableContextReplacement);
        }
        let mut cached_contexts = self.cached_contexts.inner.lock();
        assert!(
            cached_contexts.saved_context.is_none(),
            "saving a reusable context requires an empty cache mirror"
        );
        // Publish the mirror only after the owning V8 context is installed.
        // `take_saved_context` removes the mirror first, so the scheduler never
        // observes a key whose context has already been moved or destroyed.
        self.saved_context = Some(saved_context);
        cached_contexts.saved_context = Some(cached_context);
        drop(cached_contexts);
        log_context_cache_entry_added(context_kind);
        log_context_cache_operation(context_kind, ContextCacheOperation::Save);
    }

    pub(crate) fn take_reused_context(
        &mut self,
        module_path: &CanonicalizedComponentModulePath,
    ) -> Option<(v8::Global<v8::Context>, ModuleMap, ContextReadSet)> {
        if let Some(SavedContext::DatabaseUdf {
            module_path: saved_path,
            ..
        }) = &self.saved_context
            && saved_path == module_path
        {
            let Some(SavedContext::DatabaseUdf {
                module_path: _,
                context,
                module_map,
                read_set,
            }) = self.take_saved_context()
            else {
                unreachable!()
            };
            log_context_cache_entry_removed(ReusableContextKind::DatabaseUdf);
            log_context_cache_operation(
                ReusableContextKind::DatabaseUdf,
                ContextCacheOperation::Take,
            );
            Some((context, module_map, read_set))
        } else {
            None
        }
    }

    pub(crate) fn take_http_action_context(
        &mut self,
        module_path: &CanonicalizedComponentModulePath,
    ) -> Option<(v8::Global<v8::Context>, ModuleMap, ContextReadSet)> {
        if let Some(SavedContext::HttpAction {
            module_path: saved_path,
            ..
        }) = &self.saved_context
            && saved_path == module_path
        {
            let Some(SavedContext::HttpAction {
                module_path: _,
                context,
                module_map,
                read_set,
            }) = self.take_saved_context()
            else {
                unreachable!()
            };
            log_context_cache_entry_removed(ReusableContextKind::HttpAction);
            log_context_cache_operation(
                ReusableContextKind::HttpAction,
                ContextCacheOperation::Take,
            );
            Some((context, module_map, read_set))
        } else {
            None
        }
    }

    #[fastrace::trace]
    pub(crate) async fn validate_and_apply_context_read_set<RT: Runtime>(
        tx: &mut Transaction<RT>,
        read_set: &ContextReadSet,
    ) -> anyhow::Result<bool> {
        let mut reusable = scopeguard::guard(false, |reusable| {
            LocalSpan::add_property(|| ("reuse_success", reusable.as_label()));
        });
        for (namespace, tablet_index_name, table_name, intervals, hash) in &read_set.range_hashes {
            let tablet = *tablet_index_name.table();
            if !tx.table_mapping().tablet_id_exists(tablet) {
                return Ok(false);
            }
            let (new_namespace, _, new_table_name) =
                tx.table_mapping().get_table_metadata(tablet)?;
            anyhow::ensure!(namespace == new_namespace, "{tablet} changed namespace?");
            anyhow::ensure!(table_name == new_table_name, "{tablet} changed name?");
            let Some(new_hash) = tx
                .hash_index_interval_no_deps(tablet_index_name, table_name, intervals)
                .await?
            else {
                return Ok(false);
            };
            if new_hash != *hash {
                return Ok(false);
            }
        }
        *reusable = true;
        // All hashes match, so merge the saved reads into the request transaction
        // before running code compiled from the cached context.
        tx.apply_reads(read_set.read_set.clone());
        Ok(true)
    }

    #[fastrace::trace]
    pub(crate) async fn capture_context_read_set<RT: Runtime>(
        read_set: TransactionReadSet,
        tx: &mut Transaction<RT>,
    ) -> anyhow::Result<Option<ContextReadSet>> {
        anyhow::ensure!(
            read_set.read_set().iter_search().count() == 0,
            "searches can't be done during init"
        );
        let mut range_hashes = vec![];
        for (tablet_index_name, reads) in read_set.read_set().iter_indexed() {
            let &(namespace, _table_number, ref table_name) = tx
                .table_mapping()
                .get_table_metadata(*tablet_index_name.table())?;
            anyhow::ensure!(
                table_name.is_system(),
                "context init read non-system table {table_name}?"
            );
            let table_name = table_name.clone();
            let Some(hash) = tx
                .hash_index_interval_no_deps(tablet_index_name, &table_name, &reads.intervals)
                .await?
            else {
                return Ok(None);
            };
            range_hashes.push((
                namespace,
                tablet_index_name.clone(),
                table_name,
                reads.intervals.clone(),
                hash,
            ));
        }
        Ok(Some(ContextReadSet {
            read_set,
            range_hashes,
        }))
    }

    pub fn cached_contexts(&self) -> &Arc<CachedContexts> {
        &self.cached_contexts
    }
}

impl CachedContexts {
    pub fn can_serve_request<RT: Runtime>(&self, request: &Request<RT>) -> bool {
        let this = self.inner.lock();
        match request.reusable_context_kind() {
            Some(ReusableContextKind::DatabaseUdf) => request.module().is_some_and(|module| {
                matches!(
                    &this.saved_context,
                    Some(CachedContext::DatabaseUdf(saved_module)) if saved_module == &module
                )
            }),
            Some(ReusableContextKind::HttpAction) => request.module().is_some_and(|module| {
                matches!(
                    &this.saved_context,
                    Some(CachedContext::HttpAction(saved_module)) if saved_module == &module
                )
            }),
            // Prefer routing other requests to isolates that don't have warmed contexts
            None => this.saved_context.is_none(),
        }
    }

    pub(crate) fn has_no_reusable_context(&self) -> bool {
        self.inner.lock().saved_context.is_none()
    }
}

impl Drop for ContextCache {
    fn drop(&mut self) {
        // The scheduler holds a clone of `CachedContexts` while a worker is idle, so
        // the mirror can outlive this cache during isolate recreation. Clear it here
        // to avoid advertising contexts that were destroyed with the old isolate.
        self.clear(ContextCacheClearReason::CacheDrop);
    }
}

fn make_context<'s>(scope: &v8::PinScope<'s, '_, ()>) -> v8::Local<'s, v8::Context> {
    let _create_context_timer = create_context_timer();
    v8::Context::new(scope, v8::ContextOptions::default())
}
