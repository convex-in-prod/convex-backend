use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{
        Duration,
        SystemTime,
    },
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
    schemas::validator::Validator,
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
        function_validators::ReturnsValidator,
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
    validation::{
        ActionReturnValidation,
        ValidatedPathAndArgs,
    },
    FunctionOutcome,
};
use value::{
    sha256::Sha256Digest,
    DeveloperDocumentId,
    InternalId,
    TableMapping,
    TableNamespace,
    TableNumber,
    TabletId,
};

use super::DatabaseUdfEnvironment;
use crate::{
    client::{
        ActionRequest,
        ActionRequestParams,
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
    fetch_client: Arc<common::http::fetch::StaticFetchClient>,
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
            fetch_client: Arc::new(common::http::fetch::StaticFetchClient::new()),
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

    fn environment_data(&self) -> EnvironmentData<ProdRuntime> {
        EnvironmentData {
            key_broker: KeyBroker::dev().function_runner_keybroker(),
            default_system_env_vars: BTreeMap::new(),
            file_storage: self.file_storage.clone(),
            module_loader: self.modules.clone(),
            deployment: DeploymentMetadata {
                name: "context_reuse_test".to_owned(),
                region: None,
                class: DeploymentClass::S16,
            },
        }
    }

    async fn action_request(
        &self,
        path: &str,
        argument: JsonValue,
        options: &ActionOptions,
    ) -> anyhow::Result<ActionRequest<ProdRuntime>> {
        let path_and_args = ValidatedPathAndArgs::from_proto(pb::common::ValidatedPathAndArgs {
            path: Some(path.to_owned()),
            args: Some(SerializedArgs::from_args(vec![argument])?.into_bytes()),
            component_path: Some(ComponentPath::root().into()),
            context_reuse: Some(pb::common::ContextReusePolicy {
                actions: Some(options.reuse),
                ..Default::default()
            }),
            context_initialization_module: options.grouped.then(|| INITIALIZER.to_owned()),
            ..Default::default()
        })?;
        let mut transaction = self.database.begin(options.identity.clone()).await?;
        let table_mapping = transaction
            .table_mapping()
            .namespace(TableNamespace::root_component());
        Ok(ActionRequest {
            params: ActionRequestParams { path_and_args },
            return_validation: ActionReturnValidation {
                returns_validator: ReturnsValidator::Unvalidated,
                table_mapping,
            },
            identity: options.identity.clone(),
            transaction,
            environment_data: self.environment_data(),
            context: ExecutionContext::new(
                RequestContext::new_for_system_request(RequestId::new()),
                &FunctionCaller::Cron,
            ),
        })
    }

    async fn run_action(
        &mut self,
        path: &str,
        argument: JsonValue,
        options: ActionOptions,
    ) -> anyhow::Result<(Result<JsonValue, common::errors::JsError>, bool)> {
        let request = self.action_request(path, argument, &options).await?;
        let (log_sender, _log_receiver) = tokio::sync::mpsc::unbounded_channel();
        let environment = crate::environment::action::ActionEnvironment::new(
            self.rt.clone(),
            ComponentId::Root,
            request.params.path_and_args.path().udf_path.clone(),
            ComponentPath::root(),
            request.environment_data,
            request.identity,
            request.transaction,
            Arc::new(NoActionCallbacks),
            self.fetch_client.clone(),
            log_sender,
            None,
            request.context,
        );
        let permit = ConcurrencyLimiter::unlimited()
            .acquire(Arc::new("context_reuse_test".to_owned()), false)
            .await;
        let cancellation = CancellationSignal::new_for_test();
        if options.cancelled {
            cancellation.cancel_for_test();
        }
        let mut clean = false;
        let (outcome, candidate) = Box::pin(environment.run_action(
            &mut self.isolate,
            &mut self.contexts,
            permit,
            &mut clean,
            request.params,
            cancellation,
            None,
        ))
        .await?;
        assert!(clean);
        let retained = candidate.is_some();
        if options.deliver {
            if let Some(candidate) = candidate {
                candidate.publish(&mut self.contexts);
            }
        }
        Ok((outcome.result.map(|value| value.json_value()), retained))
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
                file_downloads: file_storage::FileDownloads::default(),
                path_and_args,
                udf_type: UdfType::Query,
                transaction: self.database.begin(Identity::system()).await?,
                unix_timestamp: self.rt.unix_timestamp(),
                journal: QueryJournal::new(),
                context: ExecutionContext::new(
                    RequestContext::new_for_system_request(RequestId::new()),
                    &FunctionCaller::Cron,
                ),
                environment_data: self.environment_data(),
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

mod action_callbacks {
    use common::{
        bootstrap_model::components::handles::FunctionHandle,
        components::{
            CanonicalizedComponentFunctionPath,
            ComponentId,
            ComponentPath,
        },
        execution_context::{
            ExecutionContext,
            RequestId,
        },
        runtime::UnixTimestamp,
        types::AttributedCaller,
    };
    use keybroker::Identity;
    use model::file_storage::{
        types::FileStorageEntry,
        FileStorageId,
    };
    use serde_json::Value as JsonValue;
    use sync_types::types::SerializedArgs;
    use udf::FunctionResult;
    use usage_tracking::FunctionUsageStats;
    use value::DeveloperDocumentId;
    use vector::PublicVectorSearchQueryResult;

    pub(super) struct NoActionCallbacks;

    #[async_trait::async_trait]
    #[allow(unused_variables)]
    impl udf::ActionCallbacks for NoActionCallbacks {
        async fn create_ai_gateway_token(
            &self,
            identity: Identity,
            caller: AttributedCaller,
            request_id: RequestId,
        ) -> anyhow::Result<String> {
            anyhow::bail!("Unexpected action callback: create_ai_gateway_token")
        }

        async fn execute_query(
            &self,
            identity: Identity,
            path: CanonicalizedComponentFunctionPath,
            args: SerializedArgs,
            context: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            anyhow::bail!("Unexpected action callback: execute_query")
        }

        async fn execute_mutation(
            &self,
            identity: Identity,
            path: CanonicalizedComponentFunctionPath,
            args: SerializedArgs,
            context: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            anyhow::bail!("Unexpected action callback: execute_mutation")
        }

        async fn execute_action(
            &self,
            identity: Identity,
            path: CanonicalizedComponentFunctionPath,
            args: SerializedArgs,
            context: ExecutionContext,
        ) -> anyhow::Result<FunctionResult> {
            anyhow::bail!("Unexpected action callback: execute_action")
        }

        async fn storage_get_url(
            &self,
            identity: Identity,
            component: ComponentId,
            storage_id: FileStorageId,
        ) -> anyhow::Result<Option<String>> {
            anyhow::bail!("Unexpected action callback: storage_get_url")
        }

        async fn storage_delete(
            &self,
            identity: Identity,
            component: ComponentId,
            storage_id: FileStorageId,
        ) -> anyhow::Result<()> {
            anyhow::bail!("Unexpected action callback: storage_delete")
        }

        async fn storage_get_file_entry(
            &self,
            identity: Identity,
            component: ComponentId,
            storage_id: FileStorageId,
        ) -> anyhow::Result<Option<(ComponentPath, FileStorageEntry)>> {
            anyhow::bail!("Unexpected action callback: storage_get_file_entry")
        }

        async fn storage_store_file_entry(
            &self,
            identity: Identity,
            component: ComponentId,
            entry: FileStorageEntry,
        ) -> anyhow::Result<(ComponentPath, DeveloperDocumentId)> {
            anyhow::bail!("Unexpected action callback: storage_store_file_entry")
        }

        async fn schedule_job(
            &self,
            identity: Identity,
            scheduling_component: ComponentId,
            scheduled_path: CanonicalizedComponentFunctionPath,
            udf_args: SerializedArgs,
            scheduled_ts: UnixTimestamp,
            context: ExecutionContext,
        ) -> anyhow::Result<DeveloperDocumentId> {
            anyhow::bail!("Unexpected action callback: schedule_job")
        }

        async fn cancel_job(
            &self,
            identity: Identity,
            virtual_id: DeveloperDocumentId,
        ) -> anyhow::Result<()> {
            anyhow::bail!("Unexpected action callback: cancel_job")
        }

        async fn vector_search(
            &self,
            identity: Identity,
            query: JsonValue,
        ) -> anyhow::Result<(Vec<PublicVectorSearchQueryResult>, FunctionUsageStats)> {
            anyhow::bail!("Unexpected action callback: vector_search")
        }

        async fn lookup_function_handle(
            &self,
            identity: Identity,
            handle: FunctionHandle,
        ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
            anyhow::bail!("Unexpected action callback: lookup_function_handle")
        }

        async fn create_function_handle(
            &self,
            identity: Identity,
            path: CanonicalizedComponentFunctionPath,
        ) -> anyhow::Result<FunctionHandle> {
            anyhow::bail!("Unexpected action callback: create_function_handle")
        }
    }
}
use action_callbacks::NoActionCallbacks;

struct ActionOptions {
    reuse: bool,
    grouped: bool,
    deliver: bool,
    cancelled: bool,
    identity: Identity,
}

impl Default for ActionOptions {
    fn default() -> Self {
        Self {
            reuse: true,
            grouped: true,
            deliver: true,
            cancelled: false,
            identity: Identity::Unknown(None),
        }
    }
}

fn action_identity(subject: &str) -> anyhow::Result<Identity> {
    let attributes = keybroker::UserIdentityAttributes {
        token_identifier: sync_types::UserIdentifier::construct("https://issuer.invalid", subject),
        subject: Some(subject.to_owned()),
        issuer: Some("https://issuer.invalid".to_owned()),
        ..Default::default()
    };
    Ok(Identity::user(
        keybroker::UserIdentity::from_proto_unchecked(pb::convex_identity::UserIdentity {
            subject: Some(subject.to_owned()),
            issuer: Some("https://issuer.invalid".to_owned()),
            expiration: Some((SystemTime::now() + Duration::from_secs(3600)).into()),
            attributes: Some(attributes.into()),
            original_token: Some("fixture-token".to_owned()),
        })?,
    ))
}

#[test]
fn grouped_action_context_reuses_and_invalidates_with_fresh_request_state() -> anyhow::Result<()> {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            crate::client::initialize_v8();
            let tokio = ProdRuntime::init_tokio()?;
            let rt = ProdRuntime::new(&tokio);
            tokio.block_on(Box::pin(async {
                let mut fixture = Fixture::new(rt).await?;
                // The counter deliberately exposes retained state so a test cannot
                // pass by silently running every request in a fresh context.
                fixture.put("_deps/shared.js", r#"
                    let calls = 0;
                    const action = name => ({ isAction: true, invokeAction: async (requestId, args) => {
                        const identity = JSON.parse(await Convex.asyncSyscall("1.0/getUserIdentity", JSON.stringify({requestId})));
                        const value = JSON.parse(args)[0];
                        ++calls;
                        if (value === "fail") throw new Error("fixture failure");
                        if (value === "pending") setTimeout(() => {}, 60000);
                        if (value === "wait") await new Promise(resolve => setTimeout(resolve, 60000));
                        return JSON.stringify([name, value, calls, identity?.subject ?? null]);
                    }});
                    export const first = action("first");
                    export const second = action("second");
                "#).await?;
                fixture.put("a.js", "export { first as run, second as other } from './_deps/shared.js';").await?;
                fixture.put("b.js", "export { second as run } from './_deps/shared.js';").await?;
                fixture.put("unused.js", "export const version = 1;").await?;
                fixture.put(INITIALIZER, "import '../a.js'; import '../b.js'; import '../unused.js';").await?;
                let (value, retained) = fixture.run_action("a.js:run", json!(10), ActionOptions {
                    identity: action_identity("first-user")?, ..Default::default()
                }).await?;
                assert!(retained);
                assert_eq!(value?, json!(["first", 10.0, 1.0, "first-user"]));
                let (value, retained) = fixture.run_action("b.js:run", json!(20), ActionOptions {
                    identity: action_identity("second-user")?, ..Default::default()
                }).await?;
                assert!(retained);
                assert_eq!(value?, json!(["second", 20.0, 2.0, "second-user"]));
                let (value, _) = fixture.run_action("a.js:other", json!(30), ActionOptions::default()).await?;
                assert_eq!(value?, json!(["second", 30.0, 3.0, null]));
                fixture.put("unused.js", "export const version = 2;").await?;
                let (value, _) = fixture.run_action("b.js:run", json!(40), ActionOptions::default()).await?;
                assert_eq!(value?, json!(["second", 40.0, 1.0, null]));

                let mut tx = fixture.database.begin_system().await?;
                model::environment_variables::EnvironmentVariablesModel::new(&mut tx)
                    .create(common::types::EnvironmentVariable {
                        name: "CONTEXT_REUSE_TEST".parse()?, value: "changed".parse()?,
                    }, &Default::default()).await?;
                fixture.database.commit_with_write_source(tx, "context_reuse_test").await?;
                let (value, _) = fixture.run_action("a.js:run", json!(45), ActionOptions::default()).await?;
                assert_eq!(value?, json!(["first", 45.0, 2.0, null]));

                let (value, retained) = fixture.run_action("a.js:run", json!("fail"), ActionOptions::default()).await?;
                assert!(value.is_err());
                assert!(!retained);
                let (value, _) = fixture.run_action("a.js:run", json!(50), ActionOptions::default()).await?;
                assert_eq!(value?, json!(["first", 50.0, 1.0, null]));
                let (value, retained) = fixture.run_action("b.js:run", json!("pending"), ActionOptions::default()).await?;
                assert!(value.is_ok());
                assert!(!retained);
                let (value, _) = fixture.run_action("b.js:run", json!(60), ActionOptions::default()).await?;
                assert_eq!(value?, json!(["second", 60.0, 1.0, null]));

                let cancelled = fixture.run_action("a.js:run", json!("wait"), ActionOptions {
                    cancelled: true, ..Default::default()
                }).await.unwrap_err();
                assert!(format!("{cancelled:#}").contains("Cancelled"));
                let (value, _) = fixture.run_action("a.js:run", json!(80), ActionOptions {
                    deliver: false, ..Default::default()
                }).await?;
                assert_eq!(value?, json!(["first", 80.0, 1.0, null]));
                let (value, _) = fixture.run_action("a.js:run", json!(90), ActionOptions::default()).await?;
                assert_eq!(value?, json!(["first", 90.0, 1.0, null]));

                // Disabling reuse ignores the initializer and cannot borrow the group.
                let (value, retained) = fixture.run_action("b.js:run", json!(100), ActionOptions {
                    reuse: false, ..Default::default()
                }).await?;
                assert!(!retained);
                assert_eq!(value?, json!(["second", 100.0, 1.0, null]));
                fixture.put("outside.js", "export { first as run } from './_deps/shared.js';").await?;
                let error = fixture.run_action("outside.js:run", json!(0), ActionOptions::default()).await;
                assert!(error.is_err());
                // An ungrouped entry retains its own context and never expands a group.
                let (value, _) = fixture.run_action("outside.js:run", json!(1), ActionOptions {
                    grouped: false, ..Default::default()
                }).await?;
                assert_eq!(value?, json!(["first", 1.0, 1.0, null]));
                let (value, _) = fixture.run_action("outside.js:run", json!(2), ActionOptions {
                    grouped: false, ..Default::default()
                }).await?;
                assert_eq!(value?, json!(["first", 2.0, 2.0, null]));
                Ok(())
            }))
        })?
        .join()
        .expect("context-reuse test thread panicked")
}

#[test]
fn cleared_action_timers_release_callbacks_before_context_reuse() -> anyhow::Result<()> {
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
                        "timers.js",
                        r#"
                    let calls = 0;
                    let cancelledCalls = 0;
                    export const run = { isAction: true, invokeAction: async (_, args) => {
                        const mode = JSON.parse(args)[0];
                        ++calls;
                        const callback = () => { ++cancelledCalls; };
                        if (mode === "pending") setTimeout(callback, 60000);
                        if (mode === "timeout") {
                            const timer = setTimeout(callback, 60000);
                            clearTimeout(timer);
                            clearTimeout(timer);
                        }
                        if (mode === "interval") {
                            const timer = setInterval(callback, 60000);
                            clearTimeout(timer);
                        }
                        if (mode === "late-completion") {
                            const timer = setTimeout(callback, 0);
                            clearInterval(timer);
                            // Process the cancelled sleep's response while another task is pending.
                            await new Promise(resolve => setTimeout(resolve, 5));
                        }
                        if (mode === "fired-interval") {
                            await new Promise(resolve => {
                                const timer = setInterval(() => {
                                    clearInterval(timer);
                                    resolve();
                                }, 0);
                            });
                        }
                        if (mode === "abort-deadline") {
                            const controller = new AbortController();
                            const timer = setTimeout(() => controller.abort(), 60000);
                            try { await new Promise(resolve => setTimeout(resolve, 0)); }
                            finally { clearTimeout(timer); }
                            if (controller.signal.aborted) throw new Error("deadline fired");
                        }
                        return JSON.stringify([calls, cancelledCalls]);
                    }};
                "#,
                    )
                    .await?;
                for (mode, calls, retained) in [
                    ("timeout", 1, true),
                    ("interval", 2, true),
                    ("late-completion", 3, true),
                    ("fired-interval", 4, true),
                    ("abort-deadline", 5, true),
                    ("pending", 6, false),
                    ("timeout", 1, true),
                ] {
                    let (value, saved) = fixture
                        .run_action(
                            "timers.js:run",
                            json!(mode),
                            ActionOptions {
                                grouped: false,
                                ..Default::default()
                            },
                        )
                        .await?;
                    assert_eq!(value?, json!([calls as f64, 0.0]), "{mode}");
                    assert_eq!(saved, retained, "{mode}");
                }
                Ok(())
            }))
        })?
        .join()
        .expect("timer context-reuse test thread panicked")
}

#[test]
fn action_deadlines_end_with_the_invocation() -> anyhow::Result<()> {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            crate::client::initialize_v8();
            let tokio = ProdRuntime::init_tokio()?;
            let rt = ProdRuntime::new(&tokio);
            tokio.block_on(Box::pin(async {
                let mut fixture = Fixture::new(rt).await?;
                fixture.put("deadlines.js", r#"
                    let calls = 0;
                    let oldAborts = 0;
                    export const run = { isAction: true, invokeAction: async (_, args) => {
                        const mode = JSON.parse(args)[0];
                        ++calls;
                        if (mode === "unused") {
                            AbortSignal.timeout(10).addEventListener("abort", () => ++oldAborts);
                        } else if (mode === "awaited") {
                            const signal = AbortSignal.timeout(0);
                            if (signal.aborted) throw new Error("deadline fired synchronously");
                            await new Promise(resolve => signal.addEventListener("abort", resolve));
                            if (!signal.aborted || signal.reason.name !== "TimeoutError") {
                                throw new Error("deadline did not abort with TimeoutError");
                            }
                        } else if (mode === "normalized") {
                            // Preserve the existing polyfill's timer normalization.
                            for (const time of [-1, 0.5, NaN, Infinity, "0", undefined]) {
                                const signal = AbortSignal.timeout(time);
                                await new Promise(resolve => signal.addEventListener("abort", resolve));
                                if (signal.reason.name !== "TimeoutError") throw new Error("wrong reason");
                            }
                        } else if (mode === "pending") {
                            AbortSignal.timeout(60000);
                            setTimeout(() => ++oldAborts, 60000);
                        }
                        return JSON.stringify([calls, oldAborts]);
                    }};
                "#).await?;
                for (mode, calls, retained) in [
                    ("unused", 1, true),
                    ("awaited", 2, true),
                    ("normalized", 3, true),
                    ("pending", 4, false),
                    ("awaited", 1, true),
                ] {
                    let (value, saved) = fixture.run_action("deadlines.js:run", json!(mode), ActionOptions {
                        grouped: false,
                        ..Default::default()
                    }).await?;
                    assert_eq!(value?, json!([calls as f64, 0.0]), "{mode}");
                    assert_eq!(saved, retained, "{mode}");
                    if mode == "unused" {
                        // Cross the discarded deadline before reentering the same context.
                        tokio::time::sleep(Duration::from_millis(30)).await;
                    }
                }
                Ok(())
            }))
        })?
        .join()
        .expect("deadline context-reuse test thread panicked")
}

#[test]
fn completed_action_fetches_allow_reuse_but_pending_work_does_not() -> anyhow::Result<()> {
    use common::http::HttpResponseStream;
    use futures::{
        FutureExt,
        StreamExt,
    };

    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            crate::client::initialize_v8();
            let tokio = ProdRuntime::init_tokio()?;
            let rt = ProdRuntime::new(&tokio);
            tokio.block_on(Box::pin(async {
                let mut fixture = Fixture::new(rt).await?;
                let mut fetch_client = common::http::fetch::StaticFetchClient::new();
                for path in ["body", "pending-body"] {
                    fetch_client.register_http_route(
                        format!("https://example.invalid/{path}").parse()?,
                        http::Method::GET,
                        move |_| {
                            async move {
                                let body = if path == "body" {
                                    futures::stream::iter([Ok(bytes::Bytes::from_static(
                                        b"response",
                                    ))])
                                    .boxed()
                                } else {
                                    futures::stream::pending().boxed()
                                };
                                Ok(HttpResponseStream {
                                    body: Some(body),
                                    status: http::StatusCode::OK,
                                    headers: Default::default(),
                                    url: None,
                                    request_size: Default::default(),
                                })
                            }
                            .boxed()
                        },
                    );
                }
                fetch_client.register_http_route(
                    "https://example.invalid/pending".parse()?,
                    http::Method::GET,
                    |_| futures::future::pending().boxed(),
                );
                for path in ["upload", "pending-upload"] {
                    fetch_client.register_http_route(
                        format!("https://example.invalid/{path}").parse()?,
                        http::Method::POST,
                        move |request| {
                            async move {
                                if path == "upload" {
                                    let mut body = request.body.context("Missing request body")?;
                                    let mut uploaded = Vec::new();
                                    while let Some(chunk) = body.next().await {
                                        uploaded.extend(chunk?);
                                    }
                                    anyhow::ensure!(uploaded == [1, 2, 3], "Wrong upload contents");
                                }
                                // A server may send a response without consuming an unfinished
                                // upload.
                                Ok(HttpResponseStream {
                                    body: Some(
                                        futures::stream::iter([Ok(bytes::Bytes::from_static(
                                            b"response",
                                        ))])
                                        .boxed(),
                                    ),
                                    status: http::StatusCode::OK,
                                    headers: Default::default(),
                                    url: None,
                                    request_size: Default::default(),
                                })
                            }
                            .boxed()
                        },
                    );
                }
                fixture.fetch_client = Arc::new(fetch_client);
                fixture
                    .put(
                        "fetches.js",
                        r#"
                    let calls = 0;
                    let lateAborts = 0;
                    export const run = { isAction: true, invokeAction: async (_, args) => {
                        const mode = JSON.parse(args)[0];
                        ++calls;
                        const signal = AbortSignal.timeout(60000);
                        signal.addEventListener("abort", () => ++lateAborts);
                        if (mode === "pending-fetch") {
                            void fetch("https://example.invalid/pending", {signal});
                        }
                        const uploading = mode === "upload" || mode === "pending-upload";
                        const path = uploading ? mode : mode === "pending-body" ? mode : "body";
                        const options = uploading ? {
                            signal, method: "POST", body: new ReadableStream({start(controller) {
                                controller.enqueue(new Uint8Array([1, 2, 3]));
                                if (mode === "upload") controller.close();
                            }}),
                        } : {signal};
                        const response = await fetch(`https://example.invalid/${path}`, options);
                        if (mode !== "unread" && mode !== "pending-body") {
                            if (await response.text() !== "response") throw new Error("wrong body");
                        }
                        return JSON.stringify([calls, lateAborts]);
                    }};
                "#,
                    )
                    .await?;
                for (mode, calls, retained) in [
                    ("complete", 1, true),
                    ("complete", 2, true),
                    ("unread", 3, false),
                    ("complete", 1, true),
                    ("pending-body", 2, false),
                    ("complete", 1, true),
                    ("pending-fetch", 2, false),
                    ("complete", 1, true),
                    ("upload", 2, true),
                    ("pending-upload", 3, false),
                    ("upload", 1, true),
                ] {
                    let (value, saved) = fixture
                        .run_action(
                            "fetches.js:run",
                            json!(mode),
                            ActionOptions {
                                grouped: false,
                                ..Default::default()
                            },
                        )
                        .await?;
                    assert_eq!(value?, json!([calls as f64, 0.0]), "{mode}");
                    assert_eq!(saved, retained, "{mode}");
                }
                assert_eq!(fixture.fetch_client.num_calls(), 12);
                Ok(())
            }))
        })?
        .join()
        .expect("fetch context-reuse test thread panicked")
}

#[test]
fn grouped_action_worker_publishes_only_after_uncancelled_delivery() -> anyhow::Result<()> {
    use std::{
        future::Future,
        pin::Pin,
        task::{
            Context as TaskContext,
            Wake,
            Waker,
        },
    };

    use common::fastrace_helpers::EncodedSpan;

    use crate::{
        client::{
            IsolateConfig,
            IsolateWorker,
            Request,
            RequestType,
        },
        context_cache::{
            CachedContexts,
            ReusableContextKind,
        },
        isolate_worker::FunctionRunnerIsolateWorker,
    };

    struct CancelOnWake(CancellationSignal);
    impl Wake for CancelOnWake {
        fn wake(self: Arc<Self>) {
            self.0.cancel_for_test();
        }
    }

    enum Delivery {
        Delivered,
        ReceiverDropped,
        CancelOnSend,
        CancelWhileWaiting,
    }

    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            crate::client::initialize_v8();
            let tokio = ProdRuntime::init_tokio()?;
            let rt = ProdRuntime::new(&tokio);
            tokio.block_on(Box::pin(async {
                let mut fixture = Fixture::new(rt.clone()).await?;
                fixture
                    .put(
                        "_deps/shared.js",
                        r#"
                    let calls = 0;
                    export const run = { isAction: true, invokeAction: async (_, args) => {
                        const value = JSON.parse(args)[0];
                        ++calls;
                        if (value === "malformed") return "{";
                        if (value === "invalid-return") return JSON.stringify("not an array");
                        if (value === "waiting") {
                            console.log("waiting");
                            await new Promise(resolve => setTimeout(resolve, 60000));
                        }
                        return JSON.stringify([value, calls]);
                    }};
                "#,
                    )
                    .await?;
                for entry in ["a.js", "b.js"] {
                    fixture
                        .put(entry, "export { run } from './_deps/shared.js';")
                        .await?;
                }
                fixture
                    .put(INITIALIZER, "import '../a.js'; import '../b.js';")
                    .await?;
                let limiter = ConcurrencyLimiter::unlimited();
                let worker = FunctionRunnerIsolateWorker::new(
                    rt,
                    IsolateConfig::new("context_reuse_test", limiter.clone()),
                );
                let returned_id = DeveloperDocumentId::new(TableNumber::MAX, InternalId::MIN);
                for (entry, value, delivery, expected_calls, retained) in [
                    ("a.js", "first", Delivery::Delivered, 1, true),
                    ("b.js", "second", Delivery::Delivered, 2, true),
                    ("a.js", "cancelled", Delivery::CancelOnSend, 3, false),
                    ("b.js", "after-cancellation", Delivery::Delivered, 1, true),
                    ("a.js", "dropped", Delivery::ReceiverDropped, 2, false),
                    ("b.js", "after-drop", Delivery::Delivered, 1, true),
                    ("a.js", "malformed", Delivery::Delivered, 2, false),
                    ("b.js", "after-malformed", Delivery::Delivered, 1, true),
                    ("a.js", "invalid-return", Delivery::Delivered, 2, false),
                    (
                        "b.js",
                        "after-return-validation",
                        Delivery::Delivered,
                        1,
                        true,
                    ),
                    ("a.js", "waiting", Delivery::CancelWhileWaiting, 2, false),
                    ("b.js", "after-wait", Delivery::Delivered, 1, true),
                    ("a.js", "after-eviction", Delivery::Delivered, 1, true),
                    ("b.js", "id-valid", Delivery::Delivered, 2, true),
                    ("a.js", "id-invalid", Delivery::Delivered, 3, false),
                    ("b.js", "after-id-validation", Delivery::Delivered, 1, true),
                ] {
                    let argument = if matches!(value, "id-valid" | "id-invalid") {
                        json!(returned_id.to_string())
                    } else {
                        json!(value)
                    };
                    let mut action = fixture
                        .action_request(
                            &format!("{entry}:run"),
                            argument.clone(),
                            &ActionOptions::default(),
                        )
                        .await?;
                    action.return_validation.returns_validator =
                        ReturnsValidator::Validated(Validator::Array(Box::new(Validator::Any)));
                    if matches!(value, "id-valid" | "id-invalid") {
                        // The caller's snapshot can differ from execution metadata.
                        // Only that snapshot may decide whether this ID is valid,
                        // and a warm context must not retain the previous validator.
                        assert!(action
                            .return_validation
                            .table_mapping
                            .name_by_number_if_exists(returned_id.table())
                            .is_none());
                        let mut mapping = TableMapping::new();
                        mapping.insert(
                            TabletId::MIN,
                            TableNamespace::root_component(),
                            returned_id.table(),
                            "callerTable".parse()?,
                        );
                        action.return_validation.table_mapping =
                            mapping.namespace(TableNamespace::root_component());
                        let expected_table = if value == "id-valid" {
                            "callerTable"
                        } else {
                            "otherTable"
                        };
                        action.return_validation.returns_validator = ReturnsValidator::Validated(
                            Validator::Array(Box::new(Validator::Union(vec![
                                Validator::Id(expected_table.parse()?),
                                Validator::Float64,
                            ]))),
                        );
                    }
                    let cancellation = CancellationSignal::new_for_test();
                    let (response, mut receiver) = tokio::sync::oneshot::channel();
                    if matches!(delivery, Delivery::CancelOnSend) {
                        // Reproduce caller drop between its cancellation signal and
                        // receiver destruction. Sending succeeds while waking a caller
                        // that has already abandoned the invocation.
                        let waker = Waker::from(Arc::new(CancelOnWake(cancellation.clone())));
                        assert!(Pin::new(&mut receiver)
                            .poll(&mut TaskContext::from_waker(&waker))
                            .is_pending());
                    }
                    let receiver = match delivery {
                        Delivery::ReceiverDropped => {
                            drop(receiver);
                            None
                        },
                        Delivery::Delivered
                        | Delivery::CancelOnSend
                        | Delivery::CancelWhileWaiting => Some(receiver),
                    };
                    let (log_line_sender, mut logs) = tokio::sync::mpsc::unbounded_channel();
                    let request = Request::new(
                        "context_reuse_test".to_owned(),
                        RequestType::Action {
                            request: action,
                            execution_observation: None,
                            cancellation: cancellation.clone(),
                            response,
                            queue_timer: crate::metrics::queue_timer(),
                            action_callbacks: Arc::new(NoActionCallbacks),
                            fetch_client: Arc::new(common::http::fetch::StaticFetchClient::new()),
                            log_line_sender,
                            function_started_sender: None,
                        },
                        EncodedSpan::empty(),
                    );
                    let key = request.module().context("Missing action affinity key")?;
                    assert_eq!(key.module_path.as_str(), INITIALIZER);
                    assert!(CachedContexts::new_for_test([(
                        ReusableContextKind::Action,
                        key.clone(),
                    )])
                    .can_serve_request(&request));
                    for kind in [
                        ReusableContextKind::DatabaseUdf,
                        ReusableContextKind::HttpAction,
                    ] {
                        assert!(!CachedContexts::new_for_test([(kind, key.clone())])
                            .can_serve_request(&request));
                    }
                    assert!(!CachedContexts::new_for_test([(
                        ReusableContextKind::Action,
                        CanonicalizedComponentModulePath {
                            component: ComponentId::Child(fixture.package.into()),
                            module_path: key.module_path,
                        },
                    )])
                    .can_serve_request(&request));
                    let cancel_waiter = if matches!(delivery, Delivery::CancelWhileWaiting) {
                        let cancellation = cancellation.clone();
                        Some(tokio::spawn(async move {
                            tokio::time::timeout(Duration::from_secs(5), logs.recv())
                                .await
                                .expect("action did not reach its handler")
                                .expect("action log channel closed before handler entry");
                            cancellation.cancel_for_test();
                        }))
                    } else {
                        None
                    };
                    let permit = limiter
                        .acquire(Arc::new("context_reuse_test".to_owned()), false)
                        .await;
                    let (_, clean) = worker
                        .handle_request_inner(
                            &mut fixture.isolate,
                            &mut fixture.contexts,
                            request,
                            permit,
                        )
                        .await;
                    assert!(clean);
                    if let Some(cancel_waiter) = cancel_waiter {
                        cancel_waiter.await?;
                    }
                    if let Some(receiver) = receiver {
                        let result = receiver.await?;
                        if value == "malformed" || matches!(delivery, Delivery::CancelWhileWaiting)
                        {
                            assert!(result.is_err());
                        } else if matches!(value, "invalid-return" | "id-invalid") {
                            let error = result?
                                .result
                                .expect_err("return validator accepted an invalid result");
                            assert!(error.message.starts_with("ReturnsValidationError:"));
                        } else {
                            assert_eq!(
                                result?.result?.json_value(),
                                json!([argument, expected_calls as f64])
                            );
                        }
                    }
                    assert_eq!(
                        cancellation.is_cancelled(),
                        matches!(
                            delivery,
                            Delivery::CancelOnSend | Delivery::CancelWhileWaiting
                        )
                    );
                    assert_eq!(
                        fixture.contexts.cached_contexts().has_saved_contexts(),
                        retained
                    );
                    if value == "after-wait" {
                        assert!(fixture.contexts.evict_for_memory_pressure());
                        assert!(!fixture.contexts.cached_contexts().has_saved_contexts());
                    }
                }
                Ok(())
            }))
        })?
        .join()
        .expect("context-reuse worker test panicked")
}
