use std::sync::Arc;

use ::metrics::IntoLabel;
use common::{
    components::{
        CanonicalizedComponentModulePath,
        ResolvedComponentFunctionPath,
    },
    interval::IntervalSet,
    knobs::REUSE_HTTP_ACTION_CONTEXTS,
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
    client::{
        Request,
        RequestType,
    },
    metrics::create_context_timer,
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

    pub(crate) fn has_saved_context(&mut self) -> bool {
        matches!(
            self.saved_context,
            Some(SavedContext::DatabaseUdf { .. } | SavedContext::HttpAction { .. })
        )
    }

    pub(crate) fn clear(&mut self) {
        self.saved_context = None;
        self.cached_contexts.inner.lock().saved_context = None;
    }

    pub(crate) fn get_or_create_fresh_context<'s>(
        &mut self,
        scope: &v8::PinScope<'s, '_, ()>,
    ) -> v8::Local<'s, v8::Context> {
        let saved_context = self.saved_context.take();
        self.cached_contexts.inner.lock().saved_context = None;
        if matches!(
            &saved_context,
            Some(SavedContext::DatabaseUdf { .. } | SavedContext::HttpAction { .. })
        ) {
            LocalSpan::add_event(Event::new("clobbered_saved_context"));
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
        self.saved_context = Some(SavedContext::DatabaseUdf {
            module_path: module_path.clone(),
            context,
            module_map,
            read_set,
        });
        self.cached_contexts.inner.lock().saved_context =
            Some(CachedContext::DatabaseUdf(module_path));
    }

    pub(crate) fn save_http_action_context(
        &mut self,
        module_path: CanonicalizedComponentModulePath,
        context: v8::Global<v8::Context>,
        module_map: ModuleMap,
        read_set: ContextReadSet,
    ) {
        self.saved_context = Some(SavedContext::HttpAction {
            module_path: module_path.clone(),
            context,
            module_map,
            read_set,
        });
        self.cached_contexts.inner.lock().saved_context =
            Some(CachedContext::HttpAction(module_path));
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
            }) = self.saved_context.take()
            else {
                unreachable!()
            };
            self.cached_contexts.inner.lock().saved_context = None;
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
            }) = self.saved_context.take()
            else {
                unreachable!()
            };
            self.cached_contexts.inner.lock().saved_context = None;
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
        match &request.inner {
            RequestType::Udf { request: inner, .. }
                if inner.path_and_args.reuse_context(inner.udf_type) =>
            {
                request.module().is_some_and(|module| {
                    matches!(
                        &this.saved_context,
                        Some(CachedContext::DatabaseUdf(saved_module)) if saved_module == &module
                    )
                })
            },
            RequestType::HttpAction { .. } if *REUSE_HTTP_ACTION_CONTEXTS => {
                request.module().is_some_and(|module| {
                    matches!(
                        &this.saved_context,
                        Some(CachedContext::HttpAction(saved_module)) if saved_module == &module
                    )
                })
            },
            // Prefer routing other requests to isolates that don't have warmed contexts
            _ => this.saved_context.is_none(),
        }
    }
}

fn make_context<'s>(scope: &v8::PinScope<'s, '_, ()>) -> v8::Local<'s, v8::Context> {
    let _create_context_timer = create_context_timer();
    v8::Context::new(scope, v8::ContextOptions::default())
}
