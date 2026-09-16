use std::{
    collections::BTreeMap,
    sync::Arc,
};

use anyhow::Context as _;
use async_trait::async_trait;
use common::{
    components::ComponentPath,
    document::ParsedDocument,
    execution_context::ExecutionContext,
    persistence::Persistence,
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
        UdfType,
    },
    virtual_system_mapping::VirtualSystemMapping,
    RequestId,
};
use database::{
    Database,
    Transaction,
};
use file_storage::TransactionalFileStorage;
use indexing::index_cache::IndexCache;
use keybroker::{
    Identity,
    KeyBroker,
};
use model::{
    initialize_application_system_tables,
    modules::types::ModuleMetadata,
    source_packages::types::SourcePackage,
};
use runtime::prod::ProdRuntime;
use search::searcher::SearcherStub;
use serde_json::Value as JsonValue;
use sqlite::SqlitePersistence;
use storage::LocalDirStorage;
use sync_types::types::SerializedArgs;
use udf::{
    validation::ValidatedPathAndArgs,
    FunctionOutcome,
};

use super::run_in_wasm;
use crate::{
    client::{
        EnvironmentData,
        UdfRequest,
    },
    environment::udf::DatabaseUdfEnvironment,
    module_cache::{
        ModuleCache,
        V8ModuleSource,
    },
    ConcurrencyLimiter,
};

struct UncalledModuleCache;

#[async_trait]
impl ModuleCache<ProdRuntime> for UncalledModuleCache {
    async fn get_module_with_metadata(
        &self,
        _module_metadata: &ParsedDocument<ModuleMetadata>,
        _source_package: &ParsedDocument<SourcePackage>,
    ) -> anyhow::Result<Arc<V8ModuleSource>> {
        anyhow::bail!("direct wasm execution does not load V8 modules")
    }

    fn put_cached_code(&self, _module_metadata: &ModuleMetadata, _cached_data: Arc<[u8]>) {}

    fn get_cached_code(&self, _module_metadata: &ModuleMetadata) -> Option<Arc<[u8]>> {
        None
    }
}

async fn new_test_database(rt: ProdRuntime) -> anyhow::Result<Database<ProdRuntime>> {
    let persistence: Arc<dyn Persistence> = Arc::new(SqlitePersistence::new(":memory:")?);
    let (deleted_tablet_sender, _deleted_tablet_receiver) = tokio::sync::mpsc::channel(16);
    let database = Database::load(
        persistence,
        rt.clone(),
        Arc::new(SearcherStub),
        ShutdownSignal::panic(),
        VirtualSystemMapping::default(),
        IndexCache::new(1 << 20).new_handle(),
        Arc::new(new_unlimited_rate_limiter(rt)),
        deleted_tablet_sender,
        "wasm_udf_tests".to_owned(),
    )
    .await?;
    initialize_application_system_tables(&database).await?;
    Ok(database)
}

fn path_and_args(
    function_name: &str,
    args: Vec<JsonValue>,
) -> anyhow::Result<ValidatedPathAndArgs> {
    let args = SerializedArgs::from_args(args)?;
    ValidatedPathAndArgs::from_proto(pb::common::ValidatedPathAndArgs {
        path: Some(format!("messages.js:{function_name}")),
        args: Some(args.into_bytes()),
        npm_version: None,
        component_path: Some(ComponentPath::root().into()),
        component_id: None,
        reuse_context: Some(false),
        context_reuse: Some(Default::default()),
    })
}

async fn run_function(
    database: &Database<ProdRuntime>,
    rt: ProdRuntime,
    file_storage: &TransactionalFileStorage<ProdRuntime>,
    module_loader: &Arc<dyn ModuleCache<ProdRuntime>>,
    udf_type: UdfType,
    function_name: &str,
    args: Vec<JsonValue>,
    artifact: &[u8],
) -> anyhow::Result<(Transaction<ProdRuntime>, FunctionOutcome)> {
    let transaction = database.begin(Identity::system()).await?;
    let (environment, udf_args) = DatabaseUdfEnvironment::new(
        rt.clone(),
        UdfRequest {
            path_and_args: path_and_args(function_name, args)?,
            udf_type,
            transaction,
            unix_timestamp: rt.unix_timestamp(),
            journal: common::query_journal::QueryJournal::new(),
            context: ExecutionContext::new(
                common::RequestContext::new_for_system_request(RequestId::new()),
                &FunctionCaller::Cron,
            ),
            environment_data: EnvironmentData {
                key_broker: KeyBroker::dev().function_runner_keybroker(),
                default_system_env_vars: BTreeMap::new(),
                file_storage: file_storage.clone(),
                module_loader: module_loader.clone(),
                deployment: DeploymentMetadata {
                    name: "wasm_udf_tests".to_owned(),
                    region: None,
                    class: DeploymentClass::S16,
                },
                #[cfg(feature = "static-hermes-wasmtime-gate")]
                host_secret_values: Some(BTreeMap::new()),
            },
            trace_host_operations: false,
            capture_handler_reads: false,
            #[cfg(feature = "static-hermes-wasmtime-gate")]
            shadow_work_guard: None,
        },
        0,
        "wasm_udf_tests".to_owned(),
        [7; 32],
    );
    let permit = ConcurrencyLimiter::unlimited()
        .acquire(Arc::new("wasm_udf_tests".to_owned()), false)
        .await;
    run_in_wasm(environment, permit, udf_args, artifact).await
}

fn outcome_result(outcome: FunctionOutcome) -> anyhow::Result<JsonValue> {
    let result = match outcome {
        FunctionOutcome::Query(outcome) | FunctionOutcome::Mutation(outcome) => outcome.result,
        _ => anyhow::bail!("wasm test returned a non-database outcome"),
    }?;
    Ok(result.json_value())
}

#[test]
fn wasm_udf_query_and_mutation_use_database_provider() -> anyhow::Result<()> {
    let sources = BTreeMap::from([(
        "messages.js".to_owned(),
        r#"
const invoke = (handler) => async (argsJson) =>
  JSON.stringify(await handler(...JSON.parse(argsJson)));
const query = (handler) => ({ isQuery: true, invokeQuery: invoke(handler) });
const mutation = (handler) => ({ isMutation: true, invokeMutation: invoke(handler) });
const call = async (name, args) =>
  JSON.parse(await Convex.asyncSyscall(name, JSON.stringify(args)));

export const insertMessage = mutation(async () => {
  const result = await call("1.0/insert", {
    table: "messages",
    value: { body: "hello" },
  });
  return result._id;
});
export const getMessage = query(async (id) =>
  call("1.0/get", { table: "messages", id })
);
"#
        .to_owned(),
    )]);
    let artifact = wasm_runtime::compile::compile_modules(&sources, "messages.js")?.wasm;
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    rt.clone().block_on(
        "wasm_udf_query_and_mutation_use_database_provider",
        async move {
            let database = new_test_database(rt.clone()).await?;
            let storage = Arc::new(LocalDirStorage::new(rt.clone())?);
            let file_storage = TransactionalFileStorage::new(
                rt.clone(),
                storage,
                ConvexOrigin::from("http://localhost".to_owned()),
            );
            let module_loader: Arc<dyn ModuleCache<ProdRuntime>> = Arc::new(UncalledModuleCache);

            let (mutation_tx, mutation_outcome) = run_function(
                &database,
                rt.clone(),
                &file_storage,
                &module_loader,
                UdfType::Mutation,
                "insertMessage",
                vec![],
                &artifact,
            )
            .await?;
            let id = outcome_result(mutation_outcome)?
                .as_str()
                .context("mutation should return an inserted document id")?
                .to_owned();
            let commit_ts = database
                .commit_with_write_source(mutation_tx, "wasm_udf_test_mutation")
                .await?;
            database.wait_for_write_ts(commit_ts).await;

            let (_query_tx, query_outcome) = run_function(
                &database,
                rt,
                &file_storage,
                &module_loader,
                UdfType::Query,
                "getMessage",
                vec![JsonValue::String(id.clone())],
                &artifact,
            )
            .await?;
            let query_result = outcome_result(query_outcome)?;
            assert_eq!(query_result["_id"], id);
            assert_eq!(query_result["body"], "hello");
            Ok(())
        },
    )
}
