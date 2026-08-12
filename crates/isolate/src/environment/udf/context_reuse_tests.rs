use std::{
    collections::BTreeMap,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use common::{
    components::{
        CanonicalizedComponentModulePath,
        ComponentId,
        ComponentPath,
    },
    document::ParsedDocument,
    execution_context::ExecutionContext,
    persistence::Persistence,
    query_journal::QueryJournal,
    runtime::{
        new_unlimited_rate_limiter,
        Runtime,
    },
    shutdown::ShutdownSignal,
    types::{
        ConvexOrigin,
        DeploymentClass,
        DeploymentMetadata,
        FunctionCaller,
        ModuleEnvironment,
        ObjectKey,
        UdfType,
    },
    virtual_system_mapping::VirtualSystemMapping,
    RequestContext,
    RequestId,
};
use database::Database;
use file_storage::TransactionalFileStorage;
use indexing::index_cache::IndexCache;
use keybroker::{
    Identity,
    KeyBroker,
};
use model::{
    initialize_application_system_tables,
    modules::{
        module_versions::{
            AnalyzedModule,
            FullModuleSource,
            ModuleSource,
        },
        types::ModuleMetadata,
        ModuleModel,
    },
    source_packages::{
        types::{
            PackageSize,
            SourcePackage,
            SourcePackageId,
        },
        SourcePackageModel,
    },
    udf_config::{
        types::UdfConfig,
        UdfConfigModel,
    },
};
use parking_lot::RwLock;
use runtime::prod::ProdRuntime;
use search::searcher::SearcherStub;
use serde_json::{
    json,
    Value as JsonValue,
};
use sqlite::SqlitePersistence;
use storage::LocalDirStorage;
use sync_types::types::SerializedArgs;
use udf::{
    validation::ValidatedPathAndArgs,
    FunctionOutcome,
};
use value::{
    sha256::Sha256Digest,
    TableNamespace,
};

use super::DatabaseUdfEnvironment;
use crate::{
    client::{
        CancellationSignal,
        EnvironmentData,
        UdfRequest,
    },
    context_cache::ContextCache,
    isolate::Isolate,
    module_cache::{
        ModuleCache,
        V8ModuleSource,
    },
    ConcurrencyLimiter,
};

const INITIALIZER: &str = "_deps/context_init_000000000000000000000000000000000000000000000000.js";

#[derive(Default)]
struct TestModules(RwLock<BTreeMap<String, Arc<V8ModuleSource>>>);

#[async_trait]
impl ModuleCache<ProdRuntime> for TestModules {
    async fn get_module_with_metadata(
        &self,
        metadata: &ParsedDocument<ModuleMetadata>,
        _source_package: &ParsedDocument<SourcePackage>,
    ) -> anyhow::Result<Arc<V8ModuleSource>> {
        self.0
            .read()
            .get(metadata.path.as_str())
            .cloned()
            .context("Missing test source")
    }

    fn put_cached_code(&self, _metadata: &ModuleMetadata, _data: Arc<[u8]>) {}

    fn get_cached_code(&self, _metadata: &ModuleMetadata) -> Option<Arc<[u8]>> {
        None
    }
}

struct Fixture {
    // Drop V8 roots before the isolate they belong to.
    contexts: ContextCache,
    isolate: Isolate<ProdRuntime>,
    rt: ProdRuntime,
    database: Database<ProdRuntime>,
    modules: Arc<TestModules>,
    package: SourcePackageId,
    file_storage: TransactionalFileStorage<ProdRuntime>,
}

impl Fixture {
    async fn new(rt: ProdRuntime) -> anyhow::Result<Self> {
        let persistence: Arc<dyn Persistence> = Arc::new(SqlitePersistence::new(":memory:")?);
        let (deleted_tablet_sender, _receiver) = tokio::sync::mpsc::channel(16);
        let database = Database::load(
            persistence,
            rt.clone(),
            Arc::new(SearcherStub),
            ShutdownSignal::panic(),
            VirtualSystemMapping::default(),
            IndexCache::new(1 << 20).new_handle(),
            Arc::new(new_unlimited_rate_limiter(rt.clone())),
            deleted_tablet_sender,
            "context_reuse_test".to_owned(),
        )
        .await?;
        initialize_application_system_tables(&database).await?;
        let mut tx = database.begin_system().await?;
        UdfConfigModel::new(&mut tx, TableNamespace::root_component())
            .set(UdfConfig {
                server_version: semver::Version::new(1, 36, 0),
                import_phase_rng_seed: [7; 32],
                import_phase_unix_timestamp: rt.unix_timestamp(),
            })
            .await?;
        let package = SourcePackageModel::new(&mut tx, TableNamespace::root_component())
            .put(SourcePackage {
                storage_key: ObjectKey::try_from("context-reuse-test")?,
                sha256: Sha256Digest::from([0; 32]),
                external_deps_package_id: None,
                package_size: PackageSize::default(),
                node_version: None,
                node_executor_pool_topology: Default::default(),
            })
            .await?;
        database
            .commit_with_write_source(tx, "context_reuse_test")
            .await?;
        Ok(Self {
            contexts: ContextCache::new(),
            isolate: Isolate::new(rt.clone(), Some(Duration::from_secs(10)), 1 << 26),
            database,
            modules: Arc::new(TestModules::default()),
            package,
            file_storage: TransactionalFileStorage::new(
                rt.clone(),
                Arc::new(LocalDirStorage::new(rt.clone())?),
                ConvexOrigin::from("http://localhost".to_owned()),
            ),
            rt,
        })
    }

    async fn put(&self, path: &str, source: &str) -> anyhow::Result<()> {
        let mut tx = self.database.begin_system().await?;
        let path = CanonicalizedComponentModulePath {
            component: ComponentId::Root,
            module_path: path.parse()?,
        };
        let existing = ModuleModel::new(&mut tx).get_metadata(path.clone()).await?;
        ModuleModel::new(&mut tx)
            .put(
                existing.map(|m| m.id()),
                path.clone(),
                ModuleSource::from(source),
                self.package,
                None,
                Some(AnalyzedModule::default()),
                ModuleEnvironment::Isolate,
                None,
            )
            .await?;
        self.database
            .commit_with_write_source(tx, "context_reuse_test")
            .await?;
        self.modules.0.write().insert(
            path.module_path.as_str().to_owned(),
            Arc::new(V8ModuleSource::new(FullModuleSource {
                source: ModuleSource::from(source),
                source_map: None,
            })),
        );
        Ok(())
    }

    async fn run(&mut self, entry: &str, arg: i64, reuse: bool) -> anyhow::Result<JsonValue> {
        let path_and_args = ValidatedPathAndArgs::from_proto(pb::common::ValidatedPathAndArgs {
            path: Some(format!("{entry}:run")),
            args: Some(SerializedArgs::from_args(vec![json!(arg)])?.into_bytes()),
            component_path: Some(ComponentPath::root().into()),
            reuse_context: Some(reuse),
            context_initialization_module: Some(INITIALIZER.to_owned()),
            ..Default::default()
        })?;
        let (environment, args) = DatabaseUdfEnvironment::new(
            self.rt.clone(),
            UdfRequest {
                path_and_args,
                udf_type: UdfType::Query,
                transaction: self.database.begin(Identity::system()).await?,
                unix_timestamp: self.rt.unix_timestamp(),
                journal: QueryJournal::new(),
                context: ExecutionContext::new(
                    RequestContext::new_for_system_request(RequestId::new()),
                    &FunctionCaller::Cron,
                ),
                environment_data: EnvironmentData {
                    key_broker: KeyBroker::dev().function_runner_keybroker(),
                    default_system_env_vars: BTreeMap::new(),
                    file_storage: self.file_storage.clone(),
                    module_loader: self.modules.clone(),
                    deployment: DeploymentMetadata {
                        name: "context_reuse_test".to_owned(),
                        region: None,
                        class: DeploymentClass::S16,
                    },
                },
            },
            0,
            "context_reuse_test".to_owned(),
            [7; 32],
        );
        let permit = ConcurrencyLimiter::unlimited()
            .acquire(Arc::new("context_reuse_test".to_owned()), false)
            .await;
        let mut clean = false;
        let (_tx, outcome) = Box::pin(environment.run(
            &mut self.isolate,
            &mut self.contexts,
            permit,
            &mut clean,
            CancellationSignal::new_for_test(),
            args,
            None,
            None,
        ))
        .await?;
        assert!(clean);
        let FunctionOutcome::Query(outcome) = outcome else {
            anyhow::bail!("Expected query outcome");
        };
        Ok(outcome.result?.json_value())
    }
}

#[test]
fn grouped_context_reuses_entries_and_invalidates_uncalled_members() -> anyhow::Result<()> {
    // Unoptimized database futures exceed the default test-thread stack.
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            crate::client::initialize_v8();
            let tokio = ProdRuntime::init_tokio()?;
            let rt = ProdRuntime::new(&tokio);
            tokio.block_on(Box::pin(async {
                let mut fixture = Fixture::new(rt).await?;
                fixture
                    .put(
                        "_deps/shared.js",
                        r#"
            let calls = 0;
            const query = (name) => ({ isQuery: true, invokeQuery: async (args) =>
                JSON.stringify([name, JSON.parse(args)[0], ++calls]) });
            export const a = query("a");
            export const b = query("b");
        "#,
                    )
                    .await?;
                for (entry, name) in [
                    ("a.js", "a"),
                    ("b.js", "b"),
                    ("c.js", "a"),
                    ("outside.js", "a"),
                ] {
                    fixture
                        .put(
                            entry,
                            &format!("export {{ {name} as run }} from './_deps/shared.js';"),
                        )
                        .await?;
                }
                fixture
                    .put(
                        INITIALIZER,
                        "import '../a.js'; import '../b.js'; import '../c.js';",
                    )
                    .await?;

                assert_eq!(
                    fixture.run("a.js", 10, true).await?,
                    json!(["a", 10.0, 1.0])
                );
                assert_eq!(
                    fixture.run("b.js", 20, true).await?,
                    json!(["b", 20.0, 2.0])
                );

                // Neither the requested entry nor its shared chunk changed. The complete
                // cold read set must still notice a change to the uncalled third member.
                fixture
                    .put(
                        "c.js",
                        "export { a as run } from './_deps/shared.js'; export const added = 1;",
                    )
                    .await?;
                assert_eq!(
                    fixture.run("a.js", 30, true).await?,
                    json!(["a", 30.0, 1.0])
                );
                assert_eq!(
                    fixture.run("b.js", 40, true).await?,
                    json!(["b", 40.0, 2.0])
                );

                let error = fixture.run("outside.js", 50, true).await.unwrap_err();
                assert!(format!("{error:#}").contains("did not load the requested entry"));
                // A malformed group cannot be expanded by a warm request, nor retained
                // after that request fails.
                assert_eq!(
                    fixture.run("a.js", 60, true).await?,
                    json!(["a", 60.0, 1.0])
                );
                assert_eq!(
                    fixture.run("a.js", 70, false).await?,
                    json!(["a", 70.0, 1.0])
                );
                assert_eq!(
                    fixture.run("a.js", 80, true).await?,
                    json!(["a", 80.0, 2.0])
                );
                Ok(())
            }))
        })?
        .join()
        .expect("context reuse test panicked")
}
