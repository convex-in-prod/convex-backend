use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

metrics::register_convex_counter!(
    DEPLOYMENT_FINISH_EVENTS_TOTAL,
    "Non-dry-run finish commit outcomes observed after the transaction; replay is historical, and \
     activation commit does not establish Node cutover completion",
    &["outcome"],
    Duration::MAX,
);

use anyhow::Context;
use async_trait::async_trait;
use common::{
    auth::AuthInfo,
    bootstrap_model::{
        components::{
            definition::ComponentDefinitionMetadata,
            ComponentState,
        },
        schema::{
            SchemaMetadata,
            SchemaState,
        },
    },
    components::{
        ComponentDefinitionPath,
        ComponentId,
        ComponentName,
        ComponentPath,
        Resource,
    },
    errors::JsError,
    execution_context::RequestMetadata,
    knobs::FINISH_PUSH_MAX_OCC_FAILURES,
    runtime::{
        try_join,
        Runtime,
    },
    schemas::{
        DatabaseSchema,
        TableValidationOutcome,
    },
    types::{
        EnvVarName,
        EnvVarValue,
        IndexName,
        ModuleEnvironment,
        NodeDependency,
        RepeatableTimestamp,
        Timestamp,
    },
    version::Version,
};
use database::{
    table_summary::table_summary_bootstrapping_error,
    BootstrapComponentsModel,
    IndexModel,
    OccRetryStats,
    SchemaModel,
    Snapshot,
    TableShapes,
    Token,
    Transaction,
    WriteSource,
    MAX_OCC_FAILURES,
    SCHEMAS_TABLE,
};
use errors::{
    ErrorMetadata,
    ErrorMetadataAnyhowExt,
};
use fastrace::{
    future::FutureExt as _,
    Span,
};
use futures::FutureExt;
use keybroker::Identity;
use storage::ObjectKey;
use maplit::btreeset;
use model::{
    auth::{
        types::AuthDiff,
        AuthInfoModel,
    },
    components::{
        config::{
            ComponentConfigModel,
            ComponentDefinitionConfigModel,
            ComponentDefinitionDiff,
            ComponentDiff,
            SchemaChange,
            SerializedComponentDefinitionDiff,
            SerializedComponentDiff,
        },
        file_based_routing::file_based_exports,
        type_checking::{
            CheckedComponent,
            InitializerEvaluator,
            TypecheckContext,
        },
        types::{
            AppDefinitionConfig,
            ComponentDefinitionConfig,
            EvaluatedComponentDefinition,
            ProjectConfig,
        },
    },
    config::types::{
        deprecated_extract_environment_from_path,
        node_executor_pool_topology,
        parse_module_environment_and_pool as parse_required_module_environment_and_pool,
        ConfigFile,
        ConfigMetadata,
        ModuleConfig,
        ModuleHashConfig,
        NodeExecutorPoolName,
    },
    deployment_audit_log::{
        developer_index_config::DeveloperIndexConfig,
        types::{
            DeploymentAuditLogEvent,
            PushComponentDiffs,
            PushMessage,
        },
    },
    environment_variables::EnvironmentVariablesModel,
    external_packages::types::ExternalDepsPackageId,
    modules::module_versions::{
        AnalyzedModule,
        ModuleSource,
        SourceMap,
    },
    source_packages::{
        types::{
            NodeExecutorPoolTopology,
            NodeVersion,
            NodeVersionDiff,
            SourcePackage,
        },
        upload_download::download_package,
        SourcePackageModel,
    },
    udf_config::types::UdfConfig,
};
use serde::{
    Deserialize,
    Serialize,
};
use sync_types::{
    CanonicalizedModulePath,
    ModulePath,
};
use tokio::sync::oneshot;
use udf::{
    environment::system_env_var_overrides,
    EvaluateAppDefinitionsResult,
};
use usage_tracking::FunctionUsageTracker;
use value::{
    identifier::Identifier,
    sha256::Sha256Digest,
    DeveloperDocumentId,
    ResolvedDocumentId,
    TableName,
    TableNamespace,
};

use crate::{
    schema_worker::table_shape_provider,
    validate_env_var_values,
    Application,
    ApplyConfigArgs,
    ConfigMetadataAndSchema,
};

pub struct PushAnalytics {
    pub config: ConfigMetadata,
    pub modules: Vec<ModuleConfig>,
    pub udf_server_version: Version,
    pub analyze_results: BTreeMap<CanonicalizedModulePath, AnalyzedModule>,
    pub schema: Option<DatabaseSchema>,
}

pub struct PushMetrics {
    pub build_external_deps_time: Duration,
    pub upload_source_package_time: Duration,
    pub analyze_time: Duration,
    pub occ_stats: OccRetryStats,
}

pub(crate) async fn validate_native_resident_activation_in_tx<RT: Runtime>(
    tx: &mut database::Transaction<RT>,
    activation: Option<&model::source_packages::native::NativeResidentActivation>,
) -> anyhow::Result<()> {
    let latest = SourcePackageModel::new(tx, TableNamespace::Global)
        .get_latest_record()
        .await?;
    let prior = latest
        .as_ref()
        .and_then(|package| package.native_resident.as_ref());
    match activation {
        Some(activation) => activation.validate_prior(prior),
        None => {
            anyhow::ensure!(
                prior.is_none(),
                ErrorMetadata::bad_request(
                    "NativeResidentActivationRequired",
                    "This deployment must explicitly retain or retire its selected native resident",
                )
            );
            Ok(())
        },
    }
}

fn project_source_bytes(config: &ProjectConfig) -> anyhow::Result<usize> {
    let mut source_bytes = 0usize;
    for module in config
        .app_definition
        .all_modules(&config.app_definition.changed_runtime_modules)
        .chain(
            config
                .component_definitions
                .iter()
                .flat_map(|component| component.modules()),
        )
    {
        source_bytes = source_bytes
            .checked_add(module.source.as_bytes().len())
            .and_then(|size| size.checked_add(module.source_map.as_deref().map_or(0, str::len)))
            .context("analysis source size overflow")?;
    }
    Ok(source_bytes)
}

// Charge source copies, V8 strings and the two snapshot acceleration caches.
// Runtime heap and root parallelism remain independently bounded by isolate
// admission. Unchanged sources are bounded by the existing archive size limit.
fn analysis_retention_estimate(config: &ProjectConfig) -> anyhow::Result<usize> {
    let mut source_bytes = project_source_bytes(config)?;
    if !config
        .app_definition
        .unchanged_runtime_module_hashes
        .is_empty()
    {
        source_bytes = source_bytes
            .checked_add(model::source_packages::types::MAX_UNZIPPED_PACKAGES_SIZE)
            .context("analysis source size overflow")?;
    }
    source_bytes
        .checked_mul(2)
        // Components are analyzed sequentially; only one snapshot's caches
        // are retained at a time.
        .and_then(|size| size.checked_add(*common::knobs::ANALYZE_CODE_CACHE_MAX_BYTES))
        .and_then(|size| size.checked_add(*common::knobs::ANALYZE_SOURCE_MAP_CACHE_MAX_BYTES))
        .context("analysis retention size overflow")
}

struct EvaluatedPushContents {
    app: CheckedComponent,
    auth_info: Vec<AuthInfo>,
    component_definition_packages: BTreeMap<ComponentDefinitionPath, Option<SourcePackage>>,
    evaluated_components: BTreeMap<ComponentDefinitionPath, EvaluatedComponentDefinition>,
    external_deps_id: Option<ExternalDepsPackageId>,
    user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
    system_env_var_overrides: BTreeMap<EnvVarName, EnvVarValue>,
    app_functions: Vec<ModuleConfig>,
}

impl<RT: Runtime> Application<RT> {
    async fn complete_node_executor_pool_cutover_after_commit(
        &self,
        topology: &NodeExecutorPoolTopology,
        version: Timestamp,
        mut reservation: Option<node_executor::NodeExecutorCutoverReservation>,
    ) -> anyhow::Result<()> {
        let runner = self.runner();
        let topology = topology.clone();
        if runner
            .begin_node_executor_pool_cutover(&topology, version, &mut reservation)
            .is_err()
        {
            runner.record_node_executor_pool_cutover_post_commit_failure();
            tracing::error!(
                commit_timestamp = %version,
                lifecycle_context = "deployment_cutover",
                outcome = "start_failed",
                "Failed to claim committed Node executor cutover"
            );
            anyhow::bail!(ErrorMetadata::overloaded(
                "NodeExecutorCutoverFailedAfterCommit",
                format!(
                    "Deployment committed at {version}, but Node executor cutover did not \
                     complete."
                ),
            ));
        }
        let (result_sender, result_receiver) = oneshot::channel();
        let cutover_runner = runner.clone();
        self.runtime
            .spawn("node_executor_cutover_after_commit", async move {
                // Move the reservation into a detached runtime owner before
                // the first post-commit await. Caller cancellation must not
                // return capacity while this committed version is unresolved.
                let result = async {
                    let target = cutover_runner
                        .node_executor_cutover_target(&topology, version)
                        .await
                        .map_err(|_| "target_failed")?;
                    cutover_runner
                        .complete_node_executor_pool_cutover(target, version, reservation)
                        .await
                        .map_err(|_| "runtime_failed")
                }
                .await;
                if let Err(outcome) = result {
                    if outcome != "runtime_failed" {
                        cutover_runner.record_node_executor_pool_cutover_post_commit_failure();
                    }
                    tracing::error!(
                        commit_timestamp = %version,
                        lifecycle_context = "deployment_cutover",
                        outcome,
                        "Committed Node executor cutover failed"
                    );
                }
                let _ = result_sender.send(result);
            })
            .detach();

        let result = match result_receiver.await {
            Ok(result) => result,
            Err(_) => {
                runner.record_node_executor_pool_cutover_post_commit_failure();
                Err("task_failed")
            },
        };
        if result.is_err() {
            tracing::error!(
                commit_timestamp = %version,
                lifecycle_context = "deployment_cutover",
                outcome = "post_commit_failed",
                "Committed Node executor cutover did not complete"
            );
            anyhow::bail!(ErrorMetadata::overloaded(
                "NodeExecutorCutoverFailedAfterCommit",
                format!(
                    "Deployment committed at {version}, but Node executor cutover did not \
                     complete."
                ),
            ));
        }
        Ok(())
    }

    #[fastrace::trace]
    pub async fn start_push(&self, config: &ProjectConfig) -> anyhow::Result<StartPushResult> {
        self.start_push_with_retention(config, false).await
    }

    pub async fn start_push_with_prepared_sources(
        &self,
        config: &ProjectConfig,
    ) -> anyhow::Result<StartPushResult> {
        self.start_push_with_retention(config, true).await
    }

    async fn start_push_with_retention(
        &self,
        config: &ProjectConfig,
        retain_sources: bool,
    ) -> anyhow::Result<StartPushResult> {
        use common::query_analysis_admission::{
            DeploymentAnalysisKind,
            DEPLOYMENT_ANALYSIS_JOB,
        };
        let permit = self
            .deployment_analysis
            .acquire(
                if config.for_codegen || config.dry_run {
                    DeploymentAnalysisKind::Preflight
                } else {
                    DeploymentAnalysisKind::Deploy
                },
                analysis_retention_estimate(config)?,
            )
            .await?;
        DEPLOYMENT_ANALYSIS_JOB
            .scope(permit, self.start_push_admitted(config, retain_sources))
            .await
    }

    async fn start_push_admitted(
        &self,
        config: &ProjectConfig,
        retain_sources: bool,
    ) -> anyhow::Result<StartPushResult> {
        let expected_prior = if retain_sources {
            let mut tx = self.begin(Identity::system()).await?;
            SourcePackageModel::new(&mut tx, TableNamespace::Global)
                .get_latest_record()
                .await?
                .map(|package| package.developer_id())
        } else {
            None
        };
        let EvaluatedPushContents {
            app,
            auth_info,
            component_definition_packages,
            mut evaluated_components,
            external_deps_id,
            user_environment_variables,
            system_env_var_overrides,
            app_functions,
        } = self.evaluate_push_contents(config, true).await?;

        let skip_index_diff = config.dry_run || config.for_codegen;
        let mut schema_change = self
            .handle_schema_change_in_start_push(&app, &evaluated_components, skip_index_diff)
            .await?;
        if skip_index_diff {
            // Compute index diffs in a throwaway transaction so they're returned in
            // the response but not committed as pending indexes.
            let dry_run_schema_change = self
                .handle_schema_change_read_only(&app, &evaluated_components)
                .await?;
            schema_change.index_diffs = dry_run_schema_change.index_diffs;
        }
        self.database
            .load_indexes_into_memory(btreeset! { SCHEMAS_TABLE.clone() })
            .await?;

        add_file_based_exports_to_analysis(&mut evaluated_components)?;

        let resp = StartPushResponse {
            environment_variables: user_environment_variables,
            external_deps_id,
            component_definition_packages: component_definition_packages
                .into_iter()
                .map(|(path, package)| {
                    Ok((
                        path,
                        package.context("start_push requires an uploaded source package")?,
                    ))
                })
                .collect::<anyhow::Result<_>>()?,
            app_auth: auth_info,
            analysis: evaluated_components,
            app,
            schema_change,
        };
        let prepared = if retain_sources {
            let mut packages = BTreeMap::new();
            packages.insert(
                ComponentDefinitionPath::root(),
                config
                    .app_definition
                    .all_modules(&app_functions)
                    .map(|module| (module.path.clone().canonicalize(), module.clone()))
                    .collect(),
            );
            for component in &config.component_definitions {
                packages.insert(
                    component.definition_path.clone(),
                    component
                        .modules()
                        .map(|module| (module.path.clone().canonicalize(), module.clone()))
                        .collect(),
                );
            }
            let external_deps_storage_key = if let Some(id) = &resp.external_deps_id {
                let mut tx = self.begin(Identity::system()).await?;
                Some(
                    ExternalPackagesModel::new(&mut tx)
                        .get(id.clone())
                        .await?
                        .storage_key
                        .clone(),
                )
            } else {
                None
            };
            Some(Arc::new(PreparedPush {
                packages,
                external_deps_storage_key,
                expected_prior,
                system_env_var_overrides,
            }))
        } else {
            None
        };
        Ok(StartPushResult {
            response: resp,
            app_functions,
            prepared,
        })
    }

    #[fastrace::trace]
    async fn evaluate_push_contents(
        &self,
        config: &ProjectConfig,
        upload: bool,
    ) -> anyhow::Result<EvaluatedPushContents> {
        let (external_deps_id, component_definition_packages, app_functions) =
            self.prepare_packages(config, upload).await?;

        let app_udf_config = self
            .generate_udf_config(
                config.app_definition.udf_server_version.clone(),
                TableNamespace::root_component(),
                &Identity::system(),
            )
            .await?;
        let app_pkg = component_definition_packages
            .get(&ComponentDefinitionPath::root())
            .context("No package for app?")?;

        let (user_environment_variables, system_env_var_overrides) = {
            let mut tx = self.begin(Identity::system()).await?;
            let vars = EnvironmentVariablesModel::new(&mut tx).get_all().await?;
            let system_env_var_overrides = system_env_var_overrides(&mut tx).await?;
            tx.into_token()?;
            (vars, system_env_var_overrides)
        };
        let (auth_module, app_analysis) = self
            .analyze_modules_with_auth_config(
                app_udf_config.clone(),
                app_functions.clone(),
                app_pkg.clone(),
                user_environment_variables.clone(),
                system_env_var_overrides.clone(),
            )
            .await?;

        let auth_info = Application::get_evaluated_auth_config(
            self.runner(),
            user_environment_variables.clone(),
            system_env_var_overrides.clone(),
            auth_module,
            &ConfigFile {
                functions: config.config.functions.clone(),
                auth_info: if config.config.auth_info.is_empty() {
                    None
                } else {
                    let auth_info = config
                        .config
                        .auth_info
                        .clone()
                        .into_iter()
                        .map(|v| v.try_into())
                        .collect::<Result<Vec<_>, _>>()?;
                    Some(auth_info)
                },
            },
        )
        .await?;

        let evaluated_components = self
            .evaluate_components(
                config,
                &component_definition_packages,
                app_analysis,
                app_udf_config,
                user_environment_variables.clone(),
                system_env_var_overrides.clone(),
            )
            .await?;
        validate_env_var_declarations(&evaluated_components)?;
        // Build and typecheck the component tree. We don't strictly need to do this
        // before `/finish_push`, but it's better to fail fast here on errors before
        // waiting for schema backfills to complete.
        let initializer_evaluator = ApplicationInitializerEvaluator::new(
            self,
            config,
            evaluated_components
                .iter()
                .map(|(k, v)| (k.clone(), v.definition.clone()))
                .collect(),
        )?;
        let ctx = if config.for_codegen {
            TypecheckContext::new_for_codegen(&evaluated_components, &initializer_evaluator)?
        } else {
            TypecheckContext::new(&evaluated_components, &initializer_evaluator)
        };
        let app = ctx.instantiate_root().await?;

        Ok(EvaluatedPushContents {
            app,
            auth_info,
            component_definition_packages,
            evaluated_components,
            external_deps_id,
            user_environment_variables,
            system_env_var_overrides,
            app_functions,
        })
    }

    #[fastrace::trace]
    async fn handle_schema_change_in_start_push(
        &self,
        app: &CheckedComponent,
        evaluated_components: &BTreeMap<ComponentDefinitionPath, EvaluatedComponentDefinition>,
        skip_index_diff: bool,
    ) -> anyhow::Result<SchemaChange> {
        let (_ts, schema_change) = self
            .execute_with_occ_retries(
                Identity::system(),
                FunctionUsageTracker::new(),
                MAX_OCC_FAILURES,
                WriteSource::system("start_push"),
                |tx| {
                    async move {
                        let schema_change = ComponentConfigModel::new(tx)
                            .start_component_schema_changes(
                                app,
                                evaluated_components,
                                skip_index_diff,
                            )
                            .await?;
                        Ok(schema_change)
                    }
                    .into()
                },
            )
            .await?;
        Ok(schema_change)
    }

    #[fastrace::trace]
    async fn handle_schema_change_read_only(
        &self,
        app: &CheckedComponent,
        evaluated_components: &BTreeMap<ComponentDefinitionPath, EvaluatedComponentDefinition>,
    ) -> anyhow::Result<SchemaChange> {
        let mut tx = self.begin(Identity::system()).await?;
        // Reuse the canonical preparation logic, but keep every schema, index,
        // and component-namespace write inside this uncommitted transaction.
        // Schema and backfill workers can only observe the committed metadata.
        let schema_change = ComponentConfigModel::new(&mut tx)
            .start_component_schema_changes(app, evaluated_components, false)
            .await?;
        drop(tx);
        Ok(schema_change)
    }

    #[fastrace::trace]
    async fn evaluate_components(
        &self,
        config: &ProjectConfig,
        component_definition_packages: &BTreeMap<ComponentDefinitionPath, Option<SourcePackage>>,
        app_analysis: BTreeMap<CanonicalizedModulePath, AnalyzedModule>,
        app_udf_config: UdfConfig,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_var_overrides: BTreeMap<EnvVarName, EnvVarValue>,
    ) -> anyhow::Result<BTreeMap<ComponentDefinitionPath, EvaluatedComponentDefinition>> {
        let mut app_schema = None;
        if let Some(schema_module) = &config.app_definition.schema {
            app_schema = Some(self.evaluate_schema(schema_module.clone()).await?);
        }

        let mut component_analysis_by_def_path = BTreeMap::new();
        let mut component_schema_by_def_path = BTreeMap::new();
        let mut component_udf_config_by_def_path = BTreeMap::new();

        for component_def in &config.component_definitions {
            // The rng seed and unix timestamp are tied to the root because all component
            // definitions may not correspond to an existing `UdfConfig` yet. Instead, we
            // use the root's values because always know it will have a defined config.
            let udf_config = UdfConfig {
                server_version: component_def.udf_server_version.clone(),
                import_phase_rng_seed: app_udf_config.import_phase_rng_seed,
                import_phase_unix_timestamp: app_udf_config.import_phase_unix_timestamp,
            };
            component_udf_config_by_def_path
                .insert(component_def.definition_path.clone(), udf_config.clone());

            let component_pkg = component_definition_packages
                .get(&component_def.definition_path)
                .context("No package for component?")?;
            let component_analysis = self
                .analyze_modules(
                    udf_config,
                    component_def.functions.clone(),
                    component_pkg.clone(),
                    // User env vars are root-only; analyze() itself supplies
                    // the default system env vars.
                    BTreeMap::new(),
                    BTreeMap::new(),
                )
                .await?;
            anyhow::ensure!(component_analysis_by_def_path
                .insert(component_def.definition_path.clone(), component_analysis)
                .is_none());

            if let Some(schema_module) = &component_def.schema {
                let schema = match self.evaluate_schema(schema_module.clone()).await {
                    Ok(schema) => schema,
                    Err(e) => {
                        // Try to downcast to a JsError and turn that into a user-visible error if
                        // so.
                        let e = e.downcast::<JsError>()?;
                        anyhow::bail!(ErrorMetadata::bad_request("InvalidSchema", e.to_string()));
                    },
                };
                anyhow::ensure!(component_schema_by_def_path
                    .insert(component_def.definition_path.clone(), schema)
                    .is_none());
            }
        }

        let mut evaluated_definitions = BTreeMap::new();

        if let Some(ref app_definition) = config.app_definition.definition {
            let mut dependency_graph = BTreeSet::new();
            let mut component_definitions = BTreeMap::new();

            for dep in &config.app_definition.dependencies {
                dependency_graph.insert((ComponentDefinitionPath::root(), dep.clone()));
            }

            for component_def in &config.component_definitions {
                anyhow::ensure!(!component_def.definition_path.is_root());
                component_definitions.insert(
                    component_def.definition_path.clone(),
                    component_def.definition.clone(),
                );
                for dep in &component_def.dependencies {
                    dependency_graph.insert((component_def.definition_path.clone(), dep.clone()));
                }
            }

            let definition_result = self
                .evaluate_app_definitions(
                    app_definition.clone(),
                    component_definitions,
                    dependency_graph,
                    user_environment_variables,
                    system_env_var_overrides,
                )
                .await;
            evaluated_definitions = match definition_result {
                Ok(r) => r,
                Err(e) => {
                    let e = e.downcast::<JsError>()?;
                    anyhow::bail!(ErrorMetadata::bad_request(
                        "InvalidConvexConfig",
                        e.to_string()
                    ));
                },
            };
        } else {
            evaluated_definitions.insert(
                ComponentDefinitionPath::root(),
                ComponentDefinitionMetadata::default_root(),
            );
        }

        let mut evaluated_components = BTreeMap::new();
        evaluated_components.insert(
            ComponentDefinitionPath::root(),
            EvaluatedComponentDefinition {
                definition: evaluated_definitions[&ComponentDefinitionPath::root()].clone(),
                schema: app_schema.clone(),
                functions: app_analysis.clone(),
                udf_config: app_udf_config.clone(),
            },
        );
        for (path, definition) in &evaluated_definitions {
            if path.is_root() {
                continue;
            }
            evaluated_components.insert(
                path.clone(),
                EvaluatedComponentDefinition {
                    definition: definition.clone(),
                    schema: component_schema_by_def_path.get(path).cloned(),
                    functions: component_analysis_by_def_path
                        .get(path)
                        .context("Missing analysis for component?")?
                        .clone(),
                    udf_config: component_udf_config_by_def_path
                        .get(path)
                        .context("Missing UDF config for component?")?
                        .clone(),
                },
            );
        }
        Ok(evaluated_components)
    }

    #[fastrace::trace]
    async fn evaluate_app_definitions(
        &self,
        app_definition: ModuleConfig,
        component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_var_overrides: BTreeMap<EnvVarName, EnvVarValue>,
    ) -> anyhow::Result<EvaluateAppDefinitionsResult> {
        self.runner
            .evaluate_app_definitions(
                app_definition,
                component_definitions,
                dependency_graph,
                user_environment_variables,
                system_env_var_overrides,
            )
            .await
    }

    #[fastrace::trace]
    pub async fn evaluate_push(
        &self,
        config: &ProjectConfig,
    ) -> anyhow::Result<EvaluatePushResponse> {
        use common::query_analysis_admission::{
            DeploymentAnalysisKind,
            DEPLOYMENT_ANALYSIS_JOB,
        };
        let permit = self
            .deployment_analysis
            .acquire(
                DeploymentAnalysisKind::Preflight,
                analysis_retention_estimate(config)?,
            )
            .await?;
        DEPLOYMENT_ANALYSIS_JOB
            .scope(permit, self.evaluate_push_admitted(config))
            .await
    }

    async fn evaluate_push_admitted(
        &self,
        config: &ProjectConfig,
    ) -> anyhow::Result<EvaluatePushResponse> {
        let EvaluatedPushContents {
            app,
            mut evaluated_components,
            ..
        } = self.evaluate_push_contents(config, false).await?;

        let schema_change = self
            .handle_schema_change_read_only(&app, &evaluated_components)
            .await?;
        let analysis = if config.include_analysis {
            add_file_based_exports_to_analysis(&mut evaluated_components)?;
            Some(evaluated_components)
        } else {
            None
        };

        Ok(EvaluatePushResponse {
            analysis,
            schema_change,
        })
    }

    /// Predict, without side effects, the schema validation and index
    /// backfill work a push of `config` would trigger. Only the schema
    /// bundles are evaluated — modules are neither analyzed nor uploaded —
    /// and the transaction is dropped uncommitted.
    #[fastrace::trace]
    pub async fn evaluate_schema_prediction(
        &self,
        config: &ProjectConfig,
    ) -> anyhow::Result<EvaluateSchemaPredictionResponse> {
        use common::query_analysis_admission::{
            DeploymentAnalysisKind,
            DEPLOYMENT_ANALYSIS_JOB,
        };
        // The endpoint accepts the complete push configuration and retains it
        // while waiting, even though only schema bundles are evaluated.
        let bytes = project_source_bytes(config)?;
        let permit = self
            .deployment_analysis
            .acquire(
                DeploymentAnalysisKind::Preflight,
                bytes.checked_mul(2).context("schema input size overflow")?,
            )
            .await?;
        DEPLOYMENT_ANALYSIS_JOB
            .scope(permit, self.evaluate_schema_prediction_admitted(config))
            .await
    }

    async fn evaluate_schema_prediction_admitted(
        &self,
        config: &ProjectConfig,
    ) -> anyhow::Result<EvaluateSchemaPredictionResponse> {
        // `None` = the definition is in the push but has no schema.ts.
        let mut pushed_schemas: BTreeMap<ComponentDefinitionPath, Option<DatabaseSchema>> =
            BTreeMap::new();
        let app_schema = match &config.app_definition.schema {
            Some(module) => Some(self.evaluate_schema_or_user_error(module.clone()).await?),
            None => None,
        };
        pushed_schemas.insert(ComponentDefinitionPath::root(), app_schema);
        for component_def in &config.component_definitions {
            let schema = match &component_def.schema {
                Some(module) => Some(self.evaluate_schema_or_user_error(module.clone()).await?),
                None => None,
            };
            pushed_schemas.insert(component_def.definition_path.clone(), schema);
        }

        let mut tx = self.begin(Identity::system()).await?;
        let ts = tx.begin_timestamp();
        let snapshot = self.snapshot(ts)?;
        if snapshot.table_counts.is_none() {
            // Document counts and sizes aren't available until table
            // summaries finish bootstrapping. Rather than predicting with
            // partial data, fail with a retriable error, matching how other
            // callers of table counts (e.g. `Transaction::must_table_counts`)
            // treat this as a transient condition.
            return Err(table_summary_bootstrapping_error(None));
        }
        let table_shapes = self.table_shapes_at(ts).await?;

        // Shape and validator subset checks can pin the CPU on large shapes,
        // so run the prediction on its own task. Cancellation only requests
        // task abortion; that task must retain admission until it actually stops.
        let deployment_job =
            common::query_analysis_admission::DEPLOYMENT_ANALYSIS_JOB.with(Clone::clone);
        let prediction = async move {
            let definitions = BootstrapComponentsModel::new(&mut tx)
                .load_all_definitions()
                .await?;
            let mut definition_paths_by_id = BTreeMap::new();
            for (path, definition) in &definitions {
                definition_paths_by_id.insert(definition.developer_id(), path.clone());
            }

            // The root component's namespace exists even when the
            // `_components` table has no root document.
            let mut instances = vec![(
                ComponentId::Root,
                ComponentPath::root(),
                ComponentDefinitionPath::root(),
            )];
            for component in BootstrapComponentsModel::new(&mut tx)
                .load_all_components()
                .await?
            {
                if component.component_type.is_root() {
                    continue;
                }
                let component_id = ComponentId::Child(component.developer_id());
                let Some(path) =
                    BootstrapComponentsModel::new(&mut tx).get_component_path(component_id)
                else {
                    continue;
                };
                let unmounted = matches!(component.state, ComponentState::Unmounted);
                let Some(definition_path) = definition_paths_by_id.get(&component.definition_id)
                else {
                    // An unmounted component's definition can be gone from
                    // `_component_definitions` while its instance and tables
                    // are still left in place (see
                    // `model::components::config`'s "leaving existing schema
                    // and tables in place for deleted component"); there's
                    // nothing to predict for it.
                    if unmounted {
                        continue;
                    }
                    anyhow::bail!("component {path:?} references an unknown definition");
                };
                instances.push((component_id, path, definition_path.clone()));
            }

            let mut component_schema_evaluations = BTreeMap::new();
            let mut instantiated_definitions = BTreeSet::new();
            for (component_id, component_path, definition_path) in instances {
                instantiated_definitions.insert(definition_path.clone());
                let prediction = predict_component_schema(
                    &mut tx,
                    &snapshot,
                    &table_shapes,
                    ts,
                    component_id,
                    definition_path.clone(),
                    pushed_schemas.get(&definition_path),
                )
                .await?;
                component_schema_evaluations.insert(component_path, prediction);
            }
            let new_component_definitions = pushed_schemas
                .keys()
                .filter(|path| !instantiated_definitions.contains(*path))
                .cloned()
                .collect();
            drop(tx);
            Ok(EvaluateSchemaPredictionResponse {
                component_schema_evaluations,
                new_component_definitions,
            })
        };
        try_join(
            "evaluate_schema_prediction",
            common::query_analysis_admission::DEPLOYMENT_ANALYSIS_JOB
                .scope(deployment_job, prediction),
        )
        .await
    }

    async fn evaluate_schema_or_user_error(
        &self,
        module: ModuleConfig,
    ) -> anyhow::Result<DatabaseSchema> {
        match self.evaluate_schema(module).await {
            Ok(schema) => Ok(schema),
            Err(e) => {
                let e = e.downcast::<JsError>()?;
                anyhow::bail!(ErrorMetadata::bad_request("InvalidSchema", e.to_string()))
            },
        }
    }

    #[fastrace::trace]
    pub async fn wait_for_schema(
        &self,
        identity: Identity,
        schema_change: SchemaChange,
        timeout: Duration,
    ) -> anyhow::Result<SchemaStatus> {
        let deadline = self.runtime().monotonic_now() + timeout;
        loop {
            let (status, token) = self
                .load_component_schema_status(&identity, &schema_change)
                .await?;
            let now = self.runtime().monotonic_now();
            let in_progress = matches!(status, SchemaStatus::InProgress { .. });
            if !in_progress || now > deadline {
                return Ok(status);
            }
            let subscription_fut = self.subscribe_and_wait_for_invalidation(token);
            tokio::select! {
                _ = subscription_fut.fuse() => {},
                _ = self.runtime.wait(deadline - now)
                    .in_span(fastrace::Span::enter_with_local_parent("wait_for_deadline"))
                 => {},
            }
        }
    }

    #[fastrace::trace]
    pub(crate) async fn load_component_schema_status(
        &self,
        identity: &Identity,
        schema_change: &SchemaChange,
    ) -> anyhow::Result<(SchemaStatus, Token)> {
        let mut tx = self.begin(identity.clone()).await?;
        let mut components_status = BTreeMap::new();
        for (component_path, schema_id) in &schema_change.schema_ids {
            let Some(schema_id) = schema_id else {
                continue;
            };
            let schema_table_number = tx.table_mapping().tablet_number(schema_id.table())?;
            let schema_id = ResolvedDocumentId::new(
                schema_id.table(),
                DeveloperDocumentId::new(schema_table_number, schema_id.internal_id()),
            );
            let document = tx
                .get(schema_id)
                .await?
                .context("Missing schema document")?;
            let SchemaMetadata { state, .. } = document.into_value().0.try_into()?;
            let schema_validation_complete = match state {
                SchemaState::Pending => false,
                SchemaState::Active | SchemaState::Validated => true,
                SchemaState::Failed { error, table_name } => {
                    let status = SchemaStatus::Failed {
                        error,
                        component_path: component_path.clone(),
                        table_name,
                    };
                    return Ok((status, tx.into_token()?));
                },
                SchemaState::Overwritten => {
                    return Ok((SchemaStatus::RaceDetected, tx.into_token()?))
                },
            };

            let component_id = if component_path.is_root() {
                ComponentId::Root
            } else {
                let existing =
                    BootstrapComponentsModel::new(&mut tx).resolve_path(component_path)?;
                let allocated = schema_change.allocated_component_ids.get(component_path);
                let internal_id = match (existing, allocated) {
                    (None, Some(id)) => *id,
                    (Some(doc), None) => doc.id().into(),
                    r => anyhow::bail!("Invalid existing component state: {r:?}"),
                };
                ComponentId::Child(internal_id)
            };
            let namespace = TableNamespace::from(component_id);
            let mut indexes_complete = 0;
            let mut indexes_total = 0;
            for index in IndexModel::new(&mut tx)
                .get_application_indexes(namespace)
                .await?
            {
                // Skip counting indexes that are staged
                if index.config.is_staged() {
                    continue;
                }
                if !index.config.is_backfilling() {
                    indexes_complete += 1;
                }
                indexes_total += 1;
            }
            components_status.insert(
                component_path.clone(),
                ComponentSchemaStatus {
                    schema_validation_complete,
                    indexes_complete,
                    indexes_total,
                },
            );
        }
        let status = if components_status.values().all(|c| c.is_complete()) {
            SchemaStatus::Complete
        } else {
            SchemaStatus::InProgress {
                components: components_status,
            }
        };
        let token = tx.into_token()?;
        Ok((status, token))
    }

    #[fastrace::trace]
    pub async fn finish_push(
        &self,
        identity: Identity,
        request_metadata: RequestMetadata,
        mut start_push: StartPushResponse,
        message: Option<PushMessage>,
        native_resident_activation: Option<
            model::source_packages::native::NativeResidentActivation,
        >,
        native_resident: &node_executor::native::NativeResidentSupervisor,
        force_node_cutover: bool,
        operation: Option<FinishPushOperation>,
    ) -> anyhow::Result<(SerializedFinishPushDiff, Timestamp)> {
        if let Some(operation) = &operation {
            let mut tx = self.begin(identity.clone()).await?;
            if let Some(replay) = operation.validate(&mut tx).await? {
                DEPLOYMENT_FINISH_EVENTS_TOTAL
                    .with_label_values(&["historical_replay"])
                    .inc();
                return Ok(replay);
            }
        }
        let prepared = operation
            .as_ref()
            .and_then(|operation| operation.prepared.as_ref());
        // Selection is installed on the root definition below. The client
        // round trip cannot redirect that package into a different component.
        anyhow::ensure!(
            start_push.app.definition_path.is_root()
                && start_push
                    .analysis
                    .iter()
                    .all(|(path, definition)| { path == &definition.definition.path }),
            ErrorMetadata::bad_request(
                "InvalidDeploymentComponentDefinition",
                "Deployment component definitions do not match their source packages",
            )
        );
        // Resolve source bytes from this server's retained preparation or the
        // uploaded archive. Both paths still validate activation inputs below.
        let mut downloaded_source_packages = BTreeMap::new();
        for (definition_path, source_package) in &mut start_push.component_definition_packages {
            // The client round trip cannot supply durable native selection.
            source_package.native_resident = None;
            let package = if let Some(prepared) = prepared {
                prepared
                    .packages
                    .get(definition_path)
                    .context("prepared component package missing")?
            } else {
                let package =
                    download_package(self.modules_storage().clone(), source_package).await?;
                anyhow::ensure!(downloaded_source_packages
                    .insert(definition_path.clone(), package)
                    .is_none());
                downloaded_source_packages
                    .get(definition_path)
                    .context("downloaded component package missing")?
            };
            if !definition_path.is_root() {
                anyhow::ensure!(
                    package.values().all(|module| {
                        module.environment == ModuleEnvironment::Isolate
                            && module.node_pool.is_none()
                    }),
                    ErrorMetadata::bad_request(
                        "InvalidComponentModuleEnvironment",
                        "Components do not support Node modules",
                    )
                );
            }
            // `StartPushResponse` crosses a client round trip before this point.
            // Rebuild complete topology metadata from verified sources so a client
            // that omits a newly added optional field cannot weaken the commit.
            source_package.node_executor_pool_topology =
                node_executor_pool_topology(package.values())?;
        }
        // In particular, source maps are owned strings. Borrow the retained
        // packages while waiting for cutover instead of copying them once per
        // finish outside the registry's retained-source reservation.
        let downloaded_source_packages = match prepared {
            Some(prepared) => &prepared.packages,
            None => &downloaded_source_packages,
        };
        let committed_pool_topology = start_push
            .component_definition_packages
            .get(&ComponentDefinitionPath::root())
            .context("No source package for the root component")?
            .node_executor_pool_topology
            .clone();
        // The response crossed a client round trip after start-push
        // validation. Validate the archive-normalized topology again so the
        // durable commit cannot exceed this runtime's pool capability or
        // configured process budget.
        self.runner()
            .validate_node_executor_pool_topology(&committed_pool_topology)?;

        let root_definition_path = ComponentDefinitionPath::root();
        if let Some(activation) = &native_resident_activation {
            activation.validate()?;
            start_push
                .component_definition_packages
                .get_mut(&root_definition_path)
                .context("Missing root source package")?
                .native_resident = activation.target.clone();
        }

        let cutover_reservation = self
            .runner()
            .reserve_node_executor_pool_cutover(&committed_pool_topology, force_node_cutover)
            .await?;

        // TODO(ENG-7533): Strip out exports from the `StartPushResponse` since we don't
        // want to actually store it in the database. Remove this path once
        // we've stopped sending exports down to the client.
        for definition in start_push.analysis.values_mut() {
            definition.definition.exports = BTreeMap::new();
        }

        let finish_push_write_source = "finish_push";
        // Capacity waits and post-commit Node drain must not hold this lock:
        // another operator may need forced cutover to reclaim their surge owner.
        let native_publication = self
            .lock_native_resident_publication(
                native_resident,
                native_resident_activation
                    .as_ref()
                    .and_then(|activation| activation.application_contract.as_deref()),
            )
            .await?;

        let (diff, ts) = self
            .execute_with_audit_log_events_and_occ_retries_with_timestamp(
                identity.clone(),
                request_metadata,
                finish_push_write_source,
                *FINISH_PUSH_MAX_OCC_FAILURES,
                |tx| {
                    let operation = &operation;
                    let start_push = &start_push;
                    let message = &message;
                    let native_resident_activation = &native_resident_activation;
                    async move {
                        // Reading the bounded receipt table participates in OCC.
                        // Concurrent retries cannot both activate this operation.
                        if let Some(operation) = operation {
                            if let Some((diff, ts)) = operation.validate(tx).await? {
                                return Ok((FinishPushCommit::Replayed(diff, ts), vec![]));
                            }
                        }
                        validate_native_resident_activation_in_tx(
                            tx,
                            native_resident_activation.as_ref(),
                        )
                        .await?;
                        // Validate that environment variables haven't changed since `start_push`.
                        let environment_variables =
                            EnvironmentVariablesModel::new(tx).get_all().await?;
                        if environment_variables != start_push.environment_variables {
                            anyhow::bail!(ErrorMetadata::bad_request(
                                "RaceDetected",
                                "Environment variables have changed during push"
                            ));
                        }

                        // Validate that all required env vars declared in the
                        // app definition are present.
                        if let Some(app_def) =
                            start_push.analysis.get(&ComponentDefinitionPath::root())
                        {
                            let missing: Vec<_> = app_def
                                .definition
                                .required_env_var_names()
                                .into_iter()
                                .filter(|name| {
                                    !environment_variables
                                        .iter()
                                        .any(|(k, _)| k.to_string() == *name)
                                })
                                .collect();
                            if !missing.is_empty() {
                                anyhow::bail!(ErrorMetadata::bad_request(
                                    "MissingEnvironmentVariables",
                                    format!(
                                        "Required environment variables are not set: {}. Set them \
                                         in the Convex dashboard or CLI before pushing.",
                                        missing.join(", ")
                                    )
                                ));
                            }

                            // Validate existing values match the new validators.
                            validate_env_var_values(
                                &environment_variables,
                                &app_def.definition.env_vars,
                            )?;
                        }

                        // Update app state: auth info and UDF server version.
                        let auth_diff = AuthInfoModel::new(tx)
                            .put(start_push.app_auth.clone())
                            .await?;

                        let prev_node_version = SourcePackageModel::new(tx, TableNamespace::Global)
                            .get_latest()
                            .await?
                            .and_then(|p| p.node_version);

                        // Diff the component definitions.
                        let (definition_diffs, modules_by_definition, udf_config_by_definition) =
                            ComponentDefinitionConfigModel::new(tx)
                                .apply_component_definitions_diff(
                                    &start_push.analysis,
                                    &start_push.component_definition_packages,
                                    downloaded_source_packages,
                                )
                                .await?;

                        // Diff component tree.
                        let component_diffs = ComponentConfigModel::new(tx)
                            .apply_component_tree_diff(
                                &start_push.app,
                                udf_config_by_definition,
                                &start_push.schema_change,
                                modules_by_definition,
                            )
                            .await?;

                        let next_node_version = SourcePackageModel::new(tx, TableNamespace::Global)
                            .get_latest()
                            .await?
                            .and_then(|p| p.node_version);

                        let node_version_diff =
                            (prev_node_version != next_node_version).then_some(NodeVersionDiff {
                                previous_version: prev_node_version,
                                next_version: next_node_version,
                            });

                        let diffs = PushComponentDiffs {
                            auth_diff: auth_diff.clone(),
                            component_diffs: component_diffs.clone(),
                            message: message.clone(),
                            node_version_diff,
                        };
                        let audit_log_events =
                            vec![DeploymentAuditLogEvent::PushConfigWithComponents { diffs }];
                        let diff = FinishPushDiff {
                            auth_diff,
                            definition_diffs,
                            component_diffs,
                        };
                        let diff = SerializedFinishPushDiff::try_from(diff)?;
                        if let Some(operation) = operation {
                            model::deployment_receipts::DeploymentReceiptModel::new(tx)
                                .record(model::deployment_receipts::DeploymentReceipt {
                                    operation_id: operation.operation_id.clone(),
                                    input_sha256: operation.input_sha256.clone(),
                                    expires_unix_seconds: operation.expires_unix_seconds,
                                    result_json: serde_json::to_string(&diff)?,
                                })
                                .await?;
                        }
                        Ok((FinishPushCommit::Applied(diff), audit_log_events))
                    }
                    .in_span(Span::enter_with_local_parent("finish_push_tx"))
                    .into()
                },
            )
            .await
            .map_err(|e| {
                if let Some(occ_error_info) = e.occ_info()
                    && let Some(write_source) = occ_error_info.write_source
                    && write_source == finish_push_write_source
                {
                    e.context(ErrorMetadata::bad_request(
                        "ConcurrentPush",
                        "Are you running multiple `npx convex dev` processes in the same \
                         directory?"
                            .to_string(),
                    ))
                } else {
                    e
                }
            })?;

        let diff = match diff {
            FinishPushCommit::Applied(diff) => {
                DEPLOYMENT_FINISH_EVENTS_TOTAL
                    .with_label_values(&["activation_committed"])
                    .inc();
                diff
            },
            // A receipt proves commit only. Do not repeat activation or launch
            // a second cutover when the original owner already committed it.
            FinishPushCommit::Replayed(diff, ts) => {
                DEPLOYMENT_FINISH_EVENTS_TOTAL
                    .with_label_values(&["historical_replay"])
                    .inc();
                return Ok((diff, ts));
            },
        };

        drop(native_publication);
        self.complete_node_executor_pool_cutover_after_commit(
            &committed_pool_topology,
            ts,
            cutover_reservation,
        )
        .await?;

        Ok((diff, ts))
    }

    /// N.B.: does not check auth
    pub async fn push_config_no_components(
        &self,
        identity: Identity,
        request_metadata: RequestMetadata,
        config_file: ConfigFile,
        modules: Vec<ModuleConfig>,
        udf_server_version: Version,
        schema_id: Option<String>,
        node_dependencies: Option<Vec<NodeDependencyJson>>,
        node_version: Option<NodeVersion>,
        native_resident: &node_executor::native::NativeResidentSupervisor,
        force_node_cutover: bool,
    ) -> anyhow::Result<(PushAnalytics, PushMetrics)> {
        use common::query_analysis_admission::{
            DeploymentAnalysisKind,
            DEPLOYMENT_ANALYSIS_JOB,
        };
        let bytes = modules.iter().try_fold(0usize, |bytes, module| {
            bytes
                .checked_add(module.source.as_bytes().len())
                .and_then(|bytes| {
                    bytes.checked_add(module.source_map.as_deref().map_or(0, str::len))
                })
                .context("deployment input size overflow")
        })?;
        let bytes = bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(*common::knobs::ANALYZE_CODE_CACHE_MAX_BYTES))
            .and_then(|bytes| bytes.checked_add(*common::knobs::ANALYZE_SOURCE_MAP_CACHE_MAX_BYTES))
            .context("deployment input size overflow")?;
        let permit = self
            .deployment_analysis
            .acquire(DeploymentAnalysisKind::Deploy, bytes)
            .await?;
        DEPLOYMENT_ANALYSIS_JOB
            .scope(permit, async move {
                let begin_build_external_deps = Instant::now();
                // Upload external node dependencies separately
                let external_deps_id_and_pkg = if let Some(deps) = node_dependencies
                    && !deps.is_empty()
                {
                    let deps: Vec<_> = deps.into_iter().map(NodeDependency::from).collect();
                    Some(self.build_external_node_deps(deps).await?)
                } else {
                    None
                };
                let end_build_external_deps = Instant::now();
                let external_deps_pkg_size = external_deps_id_and_pkg
                    .as_ref()
                    .map(|(_, pkg)| pkg.package_size)
                    .unwrap_or_default();

                let source_package = self
                    .upload_package(&modules, external_deps_id_and_pkg, node_version)
                    .await?;
                let committed_pool_topology = source_package.node_executor_pool_topology.clone();
                let end_upload_source_package = Instant::now();
                // Verify that we have not exceeded the max zipped or unzipped file size
                let combined_pkg_size = source_package.package_size + external_deps_pkg_size;
                combined_pkg_size.verify_size()?;

                let udf_config = self
                    .generate_udf_config(
                        udf_server_version,
                        TableNamespace::root_component(),
                        &Identity::system(),
                    )
                    .await?;
                let begin_analyze = Instant::now();
                // Note: This is not transactional with the rest of the deploy to avoid keeping
                // a transaction open for a long time.
                let mut tx = self.begin(Identity::system()).await?;
                validate_native_resident_activation_in_tx(&mut tx, None).await?;
                let user_environment_variables =
                    EnvironmentVariablesModel::new(&mut tx).get_all().await?;
                let system_env_var_overrides = system_env_var_overrides(&mut tx).await?;
                drop(tx);
                // Run analyze to make sure the new modules are valid.
                let (auth_module, analyze_results) = self
                    .analyze_modules_with_auth_config(
                        udf_config.clone(),
                        modules.clone(),
                        source_package.clone(),
                        user_environment_variables,
                        system_env_var_overrides,
                    )
                    .await?;
                let end_analyze = Instant::now();
                let cutover_reservation = self
                    .runner()
                    .reserve_node_executor_pool_cutover(
                        &committed_pool_topology,
                        force_node_cutover,
                    )
                    .await?;
                let native_publication = self
                    .lock_native_resident_publication(native_resident, None)
                    .await?;
                let (
                    ConfigMetadataAndSchema {
                        config_metadata,
                        schema,
                    },
                    occ_stats,
                    commit_ts,
                ) = self
                    .apply_config_with_retries(
                        identity.clone(),
                        request_metadata,
                        ApplyConfigArgs {
                            auth_module,
                            config_file,
                            schema_id,
                            modules: modules.clone(),
                            udf_config: udf_config.clone(),
                            source_package,
                            analyze_results: analyze_results.clone(),
                        },
                    )
                    .await?;

                drop(native_publication);
                self.complete_node_executor_pool_cutover_after_commit(
                    &committed_pool_topology,
                    commit_ts,
                    cutover_reservation,
                )
                .await?;

                Ok((
                    PushAnalytics {
                        config: config_metadata,
                        modules,
                        udf_server_version: udf_config.server_version,
                        analyze_results,
                        schema,
                    },
                    PushMetrics {
                        build_external_deps_time: end_build_external_deps
                            - begin_build_external_deps,
                        upload_source_package_time: end_upload_source_package
                            - end_build_external_deps,
                        analyze_time: end_analyze - begin_analyze,
                        occ_stats,
                    },
                ))
            })
            .await
    }

    async fn lock_native_resident_publication<'a>(
        &self,
        supervisor: &'a node_executor::native::NativeResidentSupervisor,
        contract: Option<&str>,
    ) -> anyhow::Result<tokio::sync::MutexGuard<'a, ()>> {
        let guard = supervisor.publication_guard().await;
        // A canceled HTTP caller can commit before notifying supervision. Read
        // the durable head under the guard before trusting live compatibility.
        let mut tx = self.begin(Identity::system()).await?;
        let selected = SourcePackageModel::new(&mut tx, TableNamespace::Global)
            .get_latest_record()
            .await?
            .and_then(|package| package.native_resident.clone());
        supervisor.reconcile(selected, tx.into_token()?.ts())?;
        supervisor.validate_publication_contract(contract)?;
        Ok(guard)
    }
}

struct ApplicationInitializerEvaluator<'a, RT: Runtime> {
    application: &'a Application<RT>,
    component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
    evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
}

impl<'a, RT: Runtime> ApplicationInitializerEvaluator<'a, RT> {
    fn new(
        application: &'a Application<RT>,
        config: &'a ProjectConfig,
        evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
    ) -> anyhow::Result<Self> {
        let mut component_definitions = BTreeMap::new();
        for component_definition in &config.component_definitions {
            anyhow::ensure!(component_definitions
                .insert(
                    component_definition.definition_path.clone(),
                    component_definition.definition.clone(),
                )
                .is_none());
        }
        Ok(Self {
            application,
            component_definitions,
            evaluated_definitions,
        })
    }
}

#[async_trait]
impl<RT: Runtime> InitializerEvaluator for ApplicationInitializerEvaluator<'_, RT> {
    async fn evaluate(
        &self,
        path: ComponentDefinitionPath,
        args: BTreeMap<Identifier, Resource>,
        name: ComponentName,
    ) -> anyhow::Result<BTreeMap<Identifier, Resource>> {
        let component_definition = self
            .component_definitions
            .get(&path)
            .context(format!("Missing component definition for {path:?}"))?
            .clone();
        self.application
            .runner
            .evaluate_component_initializer(
                self.evaluated_definitions.clone(),
                path,
                component_definition,
                args,
                name,
            )
            .await
    }
}

fn validate_env_var_declarations(
    evaluated_components: &BTreeMap<ComponentDefinitionPath, EvaluatedComponentDefinition>,
) -> anyhow::Result<()> {
    for (path, evaluated) in evaluated_components {
        for (name, env_var_validator) in &evaluated.definition.env_vars {
            if !env_var_validator.validator.is_string_like_validator() {
                let component_label = if path.is_root() {
                    "the app".to_string()
                } else {
                    format!("component {path}", path = String::from(path.clone()))
                };
                anyhow::bail!(ErrorMetadata::bad_request(
                    "InvalidEnvVarDeclaration",
                    format!(
                        "Env var `{name}` on {component_label} has a non-string validator. \
                         Component env vars must be declared with `v.string()`, \
                         `v.literal(\"...\")`, or a union of those."
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Convex code push is a multiphase process.
///
/// Deploying clients send this message to `start_push`, then use the resulting
/// [StartPushResponse] for code generation and to complete the push. Clients
/// that only need schema diffs or code generation analysis send the same
/// message to `evaluate_push`, which does not start the multiphase push.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StartPushRequest {
    pub admin_key: String,

    pub functions: String,

    pub app_definition: AppDefinitionConfigJson,
    pub component_definitions: Vec<ComponentDefinitionConfigJson>,

    pub node_dependencies: Vec<NodeDependencyJson>,

    pub node_version: Option<String>,

    #[serde(default)]
    pub dry_run: bool,

    /// Indicates standalone component codegen, where the CLI uses a synthetic
    /// root that cannot provide the component's required environment bindings.
    /// Older clients send this request to `start_push`, so that path also
    /// avoids committing index changes when this is set.
    #[serde(default)]
    pub for_codegen: bool,

    /// Requests evaluated module and component analysis from `evaluate_push`.
    /// `start_push` already returns this analysis regardless of this field.
    #[serde(default)]
    pub include_analysis: bool,
}

impl StartPushRequest {
    pub fn into_project_config(self) -> anyhow::Result<ProjectConfig> {
        let proposed_node_version: Option<NodeVersion> =
            self.node_version.map(|v| v.parse()).transpose()?;
        let node_version = match proposed_node_version {
            Some(NodeVersion::V18x) => {
                anyhow::bail!(ErrorMetadata::bad_request(
                    "NodeVersionNotSupported",
                    "Node 18 is no longer supported. Upgrade to a newer Node version (https://docs.convex.dev/config/convex.json#configuring-the-nodejs-version)."
                ))
            },
            version => version,
        };

        Ok(ProjectConfig {
            config: ConfigMetadata {
                functions: self.functions,
                auth_info: vec![],
            },
            app_definition: self.app_definition.try_into()?,
            component_definitions: self
                .component_definitions
                .into_iter()
                .map(TryInto::try_into)
                .collect::<anyhow::Result<_>>()?,
            node_dependencies: self
                .node_dependencies
                .into_iter()
                .map(NodeDependency::from)
                .collect(),
            node_version,
            dry_run: self.dry_run,
            for_codegen: self.for_codegen,
            include_analysis: self.include_analysis,
        })
    }
}

#[derive(Debug)]
pub struct StartPushResponse {
    // We read the current environment variables when evaluating the definitions, so we need to
    // cancel the push if they change before the commit point.
    pub environment_variables: BTreeMap<EnvVarName, EnvVarValue>,

    pub external_deps_id: Option<ExternalDepsPackageId>,
    pub component_definition_packages: BTreeMap<ComponentDefinitionPath, SourcePackage>,

    pub app_auth: Vec<AuthInfo>,
    pub analysis: BTreeMap<ComponentDefinitionPath, EvaluatedComponentDefinition>,

    pub app: CheckedComponent,

    pub schema_change: SchemaChange,
}

#[derive(Debug)]
pub struct StartPushResult {
    pub response: StartPushResponse,
    /// All runtime function modules in the app component
    pub app_functions: Vec<ModuleConfig>,
    pub prepared: Option<Arc<PreparedPush>>,
}

/// Sources validated and uploaded by this server. Fields are private so an
/// HTTP echo cannot manufacture authority to skip archive verification.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "testing"), derive(Default))]
pub struct PreparedPush {
    packages: BTreeMap<ComponentDefinitionPath, BTreeMap<CanonicalizedModulePath, ModuleConfig>>,
    external_deps_storage_key: Option<ObjectKey>,
    expected_prior: Option<DeveloperDocumentId>,
    system_env_var_overrides: BTreeMap<EnvVarName, EnvVarValue>,
}

impl PreparedPush {
    async fn validate_activation<RT: Runtime>(
        &self,
        tx: &mut Transaction<RT>,
    ) -> anyhow::Result<()> {
        // The newest record also covers deployments with zero modules. This
        // index read conflicts with concurrent activation, including legacy.
        let actual = SourcePackageModel::new(tx, TableNamespace::Global)
            .get_latest_record()
            .await?
            .map(|package| package.developer_id());
        anyhow::ensure!(
            actual == self.expected_prior,
            ErrorMetadata::conflict(
                "DeploymentOperationSuperseded",
                "Another deployment activated after this operation began. Start a new operation \
                 with current inputs."
            )
        );
        // Canonical URLs also affect import-time analysis and auth. Their read
        // must join the activation transaction, including every OCC retry.
        anyhow::ensure!(
            system_env_var_overrides(tx).await? == self.system_env_var_overrides,
            ErrorMetadata::bad_request(
                "RaceDetected",
                "System environment variables have changed during push"
            )
        );
        Ok(())
    }

    pub fn retained_bytes(&self) -> usize {
        self.packages
            .iter()
            .map(|(path, modules)| {
                path.to_string().len()
                    + 128
                    + modules
                        .values()
                        .map(|module| {
                            module.source.as_bytes().len()
                                + module.source_map.as_deref().map_or(0, str::len)
                                + module.path.as_str().len()
                                + 256
                        })
                        .sum::<usize>()
            })
            .sum::<usize>()
            + self
                .system_env_var_overrides
                .iter()
                .map(|(name, value)| name.as_ref().len() + value.as_ref().len() + 128)
                .sum::<usize>()
    }
}

#[derive(Debug)]
pub struct EvaluatePushResponse {
    pub analysis: Option<BTreeMap<ComponentDefinitionPath, EvaluatedComponentDefinition>>,
    pub schema_change: SchemaChange,
}

/// Side-effect-free prediction of the schema validation and index backfill
/// work a push would trigger.
#[derive(Debug)]
pub struct EvaluateSchemaPredictionResponse {
    /// One entry per existing component instance; multiple instances of one
    /// definition each get their own entry.
    pub component_schema_evaluations: BTreeMap<ComponentPath, ComponentSchemaPrediction>,
    /// Pushed definitions with no existing instance: they get a fresh
    /// namespace, so nothing is walked and their indexes backfill trivially.
    pub new_component_definitions: Vec<ComponentDefinitionPath>,
}

#[derive(Debug)]
pub struct ComponentSchemaPrediction {
    pub definition_path: ComponentDefinitionPath,
    pub schema_validation: bool,
    pub tables: Vec<TablePrediction>,
    pub indexes: Vec<IndexPrediction>,
}

#[derive(Debug)]
pub struct TablePrediction {
    pub name: TableName,
    pub outcome: TableValidationOutcome,
    pub num_docs: u64,
    pub size_bytes: u64,
}

#[derive(Debug)]
pub struct IndexPrediction {
    pub name: IndexName,
    pub config: DeveloperIndexConfig,
    pub change: IndexChangePrediction,
    /// A backfill walk will run (or is already running) for this index. It
    /// blocks the push only when the index is not staged.
    pub needs_backfill: bool,
    /// Document count of the indexed table.
    pub num_docs: u64,
}

/// How a pushed schema changes an index, mirroring `IndexDiff`'s categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum IndexChangePrediction {
    Added,
    Identical,
    Enabled,
    Disabled,
    Dropped,
}

async fn predict_component_schema<RT: Runtime>(
    tx: &mut Transaction<RT>,
    snapshot: &Snapshot,
    table_shapes: &Option<Arc<TableShapes>>,
    ts: RepeatableTimestamp,
    component_id: ComponentId,
    definition_path: ComponentDefinitionPath,
    pushed_schema: Option<&Option<DatabaseSchema>>,
) -> anyhow::Result<ComponentSchemaPrediction> {
    let namespace = TableNamespace::from(component_id);
    // An existing component whose definition is absent from the push keeps
    // its schema, tables, and indexes untouched.
    let Some(new_schema) = pushed_schema else {
        return Ok(ComponentSchemaPrediction {
            definition_path,
            schema_validation: false,
            tables: Vec::new(),
            indexes: Vec::new(),
        });
    };

    // A pushed definition without a schema drops every index, matching
    // `start_component_schema_changes`.
    let empty_tables = BTreeMap::new();
    let index_diff = IndexModel::new(tx)
        .get_index_diff(
            namespace,
            new_schema
                .as_ref()
                .map(|schema| &schema.tables)
                .unwrap_or(&empty_tables),
        )
        .await?;
    let table_num_docs = |name: &IndexName| -> anyhow::Result<u64> {
        Ok(snapshot
            .must_table_count(namespace, name.table())?
            .num_values())
    };
    let mut indexes = Vec::new();
    for metadata in &index_diff.added {
        indexes.push(IndexPrediction {
            name: metadata.name.clone(),
            config: metadata.config.clone().into(),
            change: IndexChangePrediction::Added,
            needs_backfill: true,
            num_docs: table_num_docs(&metadata.name)?,
        });
    }
    for document in &index_diff.identical {
        indexes.push(IndexPrediction {
            name: document.name.clone(),
            config: document.config.clone().into(),
            change: IndexChangePrediction::Identical,
            needs_backfill: document.config.is_backfilling(),
            num_docs: table_num_docs(&document.name)?,
        });
    }
    for document in &index_diff.enabled {
        indexes.push(IndexPrediction {
            name: document.name.clone(),
            config: document.config.clone().into(),
            change: IndexChangePrediction::Enabled,
            needs_backfill: document.config.is_backfilling(),
            num_docs: table_num_docs(&document.name)?,
        });
    }
    for document in &index_diff.disabled {
        indexes.push(IndexPrediction {
            name: document.name.clone(),
            config: document.config.clone().into(),
            change: IndexChangePrediction::Disabled,
            needs_backfill: false,
            num_docs: table_num_docs(&document.name)?,
        });
    }
    for document in &index_diff.dropped {
        indexes.push(IndexPrediction {
            name: document.name.clone(),
            config: document.config.clone().into(),
            change: IndexChangePrediction::Dropped,
            needs_backfill: false,
            num_docs: table_num_docs(&document.name)?,
        });
    }

    let (schema_validation, tables) = match new_schema {
        Some(schema) => {
            let active_schema = SchemaModel::new(tx, namespace)
                .get_by_state(SchemaState::Active)
                .await?
                .map(|(_id, schema)| schema);
            let table_mapping = tx.table_mapping().namespace(namespace);
            let virtual_system_mapping = tx.virtual_system_mapping().clone();
            let outcomes = DatabaseSchema::table_validation_outcomes(
                schema,
                active_schema.as_deref(),
                &table_mapping,
                &virtual_system_mapping,
                &table_shape_provider(table_shapes, &table_mapping, ts),
            )?;
            let tables = outcomes
                .into_iter()
                .map(|(name, outcome)| {
                    let count = snapshot.must_table_count(namespace, name)?;
                    Ok(TablePrediction {
                        name: name.clone(),
                        outcome,
                        num_docs: count.num_values(),
                        size_bytes: count.total_size(),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            (schema.schema_validation, tables)
        },
        None => (false, Vec::new()),
    };
    Ok(ComponentSchemaPrediction {
        definition_path,
        schema_validation,
        tables,
        indexes,
    })
}

fn add_file_based_exports_to_analysis(
    analysis: &mut BTreeMap<ComponentDefinitionPath, EvaluatedComponentDefinition>,
) -> anyhow::Result<()> {
    // TODO(ENG-7533): Stop adding exports to analysis after clients use
    // `functions` directly for code generation.
    for (path, definition) in analysis {
        // The app's `api` object does not use these generated exports.
        if path.is_root() {
            continue;
        }
        anyhow::ensure!(definition.definition.exports.is_empty());
        definition.definition.exports = file_based_exports(&definition.functions)?;
    }
    Ok(())
}

impl From<NodeDependencyJson> for NodeDependency {
    fn from(value: NodeDependencyJson) -> Self {
        Self {
            package: value.name,
            version: value.version,
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AppDefinitionConfigJson {
    pub definition: Option<ModuleJson>,
    pub dependencies: Vec<String>,
    pub schema: Option<ModuleJson>,
    // CLI versions <= 1.31.5 used functions and did not upload unchanged_module_hashes
    #[serde(alias = "functions")]
    pub changed_modules: Vec<ModuleJson>,
    #[serde(default)]
    pub unchanged_module_hashes: Vec<ModuleHashJson>,
    pub udf_server_version: String,
}

impl TryFrom<AppDefinitionConfigJson> for AppDefinitionConfig {
    type Error = anyhow::Error;

    fn try_from(value: AppDefinitionConfigJson) -> Result<Self, Self::Error> {
        let definition: Option<ModuleConfig> =
            value.definition.map(TryInto::try_into).transpose()?;
        let schema: Option<ModuleConfig> = value.schema.map(TryInto::try_into).transpose()?;
        for module in definition.iter().chain(schema.iter()) {
            anyhow::ensure!(
                module.environment == ModuleEnvironment::Isolate && module.node_pool.is_none(),
                ErrorMetadata::bad_request(
                    "InvalidStaticModuleEnvironment",
                    "Application definition and schema modules must use the isolate environment",
                )
            );
        }
        Ok(Self {
            definition,
            dependencies: value
                .dependencies
                .into_iter()
                .map(|s| s.parse())
                .collect::<anyhow::Result<_>>()?,
            schema,
            changed_runtime_modules: value
                .changed_modules
                .into_iter()
                .map(TryInto::try_into)
                .collect::<anyhow::Result<_>>()?,
            udf_server_version: value.udf_server_version.parse()?,
            unchanged_runtime_module_hashes: value
                .unchanged_module_hashes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<anyhow::Result<_>>()?,
        })
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ComponentDefinitionConfigJson {
    pub definition_path: String,
    pub definition: ModuleJson,
    pub dependencies: Vec<String>,
    pub schema: Option<ModuleJson>,
    pub functions: Vec<ModuleJson>,
    pub udf_server_version: String,
}

impl TryFrom<ComponentDefinitionConfigJson> for ComponentDefinitionConfig {
    type Error = anyhow::Error;

    fn try_from(value: ComponentDefinitionConfigJson) -> Result<Self, Self::Error> {
        let definition: ModuleConfig = value.definition.try_into()?;
        let schema: Option<ModuleConfig> = value.schema.map(TryInto::try_into).transpose()?;
        let functions: Vec<ModuleConfig> = value
            .functions
            .into_iter()
            .map(TryInto::try_into)
            .collect::<anyhow::Result<_>>()?;
        for module in std::iter::once(&definition)
            .chain(schema.iter())
            .chain(&functions)
        {
            match module.environment {
                ModuleEnvironment::Node => {
                    anyhow::bail!(ErrorMetadata::bad_request(
                        "NodeActionsNotSupported",
                        format!(
                            "Node actions are not supported in components. Remove `\"use node;\" \
                             from {}",
                            module.path.as_str()
                        )
                    ));
                },
                ModuleEnvironment::Invalid | ModuleEnvironment::Isolate => {},
            }
        }
        Ok(Self {
            definition_path: value.definition_path.parse()?,
            definition,
            dependencies: value
                .dependencies
                .into_iter()
                .map(|s| s.parse())
                .collect::<anyhow::Result<_>>()?,
            schema,
            functions,
            udf_server_version: value.udf_server_version.parse()?,
        })
    }
}

/// API level structure for representing modules as Json
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ModuleJson {
    pub path: String,
    pub source: String,
    pub source_map: Option<SourceMap>,
    pub environment: Option<String>,
    pub node_pool: Option<String>,
}

/// API level structure for representing module hashes as Json (for unchanged
/// modules)
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ModuleHashJson {
    pub path: String,
    pub environment: Option<String>,
    pub node_pool: Option<String>,
    pub sha256: String,
}

impl From<ModuleConfig> for ModuleJson {
    fn from(
        ModuleConfig {
            path,
            source,
            source_map,
            environment,
            node_pool,
        }: ModuleConfig,
    ) -> ModuleJson {
        ModuleJson {
            path: path.into(),
            source: source.to_string(),
            source_map,
            environment: Some(format_module_environment(environment, node_pool.as_ref())),
            node_pool: node_pool.map(|pool| pool.to_string()),
        }
    }
}

impl TryFrom<ModuleJson> for ModuleConfig {
    type Error = anyhow::Error;

    fn try_from(
        ModuleJson {
            path,
            source,
            source_map,
            environment,
            node_pool,
        }: ModuleJson,
    ) -> anyhow::Result<ModuleConfig> {
        let (environment, node_pool) =
            parse_module_environment_and_pool(&environment, node_pool, &path)?;
        Ok(ModuleConfig {
            path: parse_module_path(&path)?,
            source: ModuleSource::new(&source),
            source_map,
            environment,
            node_pool,
        })
    }
}

impl TryFrom<ModuleHashJson> for ModuleHashConfig {
    type Error = anyhow::Error;

    fn try_from(
        ModuleHashJson {
            path,
            environment,
            node_pool,
            sha256,
        }: ModuleHashJson,
    ) -> anyhow::Result<ModuleHashConfig> {
        let sha256_bytes = const_hex::decode(&sha256).context("Invalid hex in sha256")?;
        let sha256_array: [u8; 32] = sha256_bytes
            .try_into()
            .ok()
            .context("sha256 not 32 bytes")?;
        let (environment, node_pool) =
            parse_module_environment_and_pool(&environment, node_pool, &path)?;
        Ok(ModuleHashConfig {
            path: parse_module_path(&path)?,
            environment,
            node_pool,
            sha256: Sha256Digest::from(sha256_array),
        })
    }
}

pub use model::config::types::format_module_environment;

fn parse_module_environment_and_pool(
    environment: &Option<String>,
    node_pool: Option<String>,
    path: &String,
) -> anyhow::Result<(ModuleEnvironment, Option<NodeExecutorPoolName>)> {
    match environment {
        Some(value) => parse_required_module_environment_and_pool(value, node_pool),
        None => {
            anyhow::ensure!(
                node_pool.is_none(),
                "Node pool metadata requires an explicit module environment"
            );
            Ok((
                deprecated_extract_environment_from_path(path.clone())?,
                None,
            ))
        },
    }
}

pub fn parse_module_environment(
    environment: &Option<String>,
    path: &String,
) -> anyhow::Result<ModuleEnvironment> {
    Ok(parse_module_environment_and_pool(environment, None, path)?.0)
}

pub fn parse_module_path(path: &str) -> anyhow::Result<ModulePath> {
    path.parse().map_err(|e: anyhow::Error| {
        let msg = format!("{path} is not a valid path to a Convex module. {e}");
        e.context(ErrorMetadata::bad_request("BadConvexModuleIdentifier", msg))
    })
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct NodeDependencyJson {
    name: String,
    version: String,
}

#[derive(Clone)]
pub struct FinishPushOperation {
    pub operation_id: String,
    pub input_sha256: String,
    pub expires_unix_seconds: i64,
    pub prepared: Option<Arc<PreparedPush>>,
}

impl FinishPushOperation {
    pub async fn validate<RT: Runtime>(
        &self,
        tx: &mut Transaction<RT>,
    ) -> anyhow::Result<Option<(SerializedFinishPushDiff, Timestamp)>> {
        if let Some((result, ts)) = model::deployment_receipts::DeploymentReceiptModel::new(tx)
            .lookup(&self.operation_id, &self.input_sha256)
            .await?
        {
            let mut diff: SerializedFinishPushDiff = serde_json::from_str(&result)?;
            diff.activation_replay = Some(ActivationReplay {
                commit_timestamp: ts.to_string(),
                node_cutover_completion: "unverified".to_owned(),
            });
            return Ok(Some((diff, ts)));
        }
        let prepared = self.prepared.as_ref().context(ErrorMetadata::bad_request(
            "DeploymentOperationUnknown",
            "No prepared operation exists in this process and no activation receipt was found. \
             Inspect deployment state before starting another operation.",
        ))?;
        // Reject known stale preparation before cutover admission can force
        // an old generation to terminate. The same validator runs again in
        // the activation transaction to fence changes after this early read.
        prepared.validate_activation(tx).await?;
        Ok(None)
    }
}

enum FinishPushCommit {
    Applied(SerializedFinishPushDiff),
    Replayed(SerializedFinishPushDiff, Timestamp),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedFinishPushDiff {
    /// A replay proves historical activation, including after a process
    /// restart. It does not establish completion of the original Node
    /// cutover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    activation_replay: Option<ActivationReplay>,
    auth_diff: AuthDiff,
    definition_diffs: BTreeMap<String, SerializedComponentDefinitionDiff>,
    component_diffs: BTreeMap<String, SerializedComponentDiff>,
}

impl TryFrom<FinishPushDiff> for SerializedFinishPushDiff {
    type Error = anyhow::Error;

    fn try_from(value: FinishPushDiff) -> Result<Self, Self::Error> {
        Ok(Self {
            activation_replay: None,
            auth_diff: value.auth_diff,
            definition_diffs: value
                .definition_diffs
                .into_iter()
                .map(|(k, v)| Ok((String::from(k), v.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
            component_diffs: value
                .component_diffs
                .into_iter()
                .map(|(k, v)| Ok((String::from(k), v.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActivationReplay {
    commit_timestamp: String,
    node_cutover_completion: String,
}

#[derive(Debug, Default)]
pub struct FinishPushDiff {
    pub auth_diff: AuthDiff,
    pub definition_diffs: BTreeMap<ComponentDefinitionPath, ComponentDefinitionDiff>,
    pub component_diffs: BTreeMap<ComponentPath, ComponentDiff>,
}

#[derive(Debug)]
pub enum SchemaStatus {
    InProgress {
        components: BTreeMap<ComponentPath, ComponentSchemaStatus>,
    },
    Failed {
        error: String,
        component_path: ComponentPath,
        table_name: Option<String>,
    },
    RaceDetected,
    Complete,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum SchemaStatusJson {
    #[serde(rename_all = "camelCase")]
    InProgress {
        components: BTreeMap<String, ComponentSchemaStatusJson>,
    },
    #[serde(rename_all = "camelCase")]
    Failed {
        error: String,
        component_path: String,
        table_name: Option<String>,
    },
    RaceDetected,
    Complete,
}

impl From<SchemaStatus> for SchemaStatusJson {
    fn from(value: SchemaStatus) -> Self {
        match value {
            SchemaStatus::InProgress { components } => SchemaStatusJson::InProgress {
                components: components
                    .into_iter()
                    .map(|(k, v)| (String::from(k), v.into()))
                    .collect(),
            },
            SchemaStatus::Failed {
                error,
                component_path,
                table_name,
            } => SchemaStatusJson::Failed {
                error,
                component_path: String::from(component_path),
                table_name,
            },
            SchemaStatus::RaceDetected => SchemaStatusJson::RaceDetected,
            SchemaStatus::Complete => SchemaStatusJson::Complete,
        }
    }
}

#[cfg(test)]
mod node_pool_tests {
    use super::*;

    fn module(environment: Option<&str>, node_pool: Option<&str>) -> ModuleJson {
        ModuleJson {
            path: "consumer.js".to_owned(),
            source: "export const run = 1;".to_owned(),
            source_map: None,
            environment: environment.map(str::to_owned),
            node_pool: node_pool.map(str::to_owned),
        }
    }

    #[test]
    fn pooled_node_environment_is_required_and_round_trips() {
        let config: ModuleConfig = module(Some("node:pool:consumer"), Some("consumer"))
            .try_into()
            .unwrap();
        assert_eq!(config.environment, ModuleEnvironment::Node);
        assert_eq!(config.node_pool.as_ref().unwrap().as_ref(), "consumer");

        let json: ModuleJson = config.into();
        assert_eq!(json.environment.as_deref(), Some("node:pool:consumer"));
        assert_eq!(json.node_pool.as_deref(), Some("consumer"));
    }

    #[test]
    fn rejects_optional_pool_without_required_environment_marker() {
        assert!(ModuleConfig::try_from(module(Some("node"), Some("consumer"))).is_err());
        assert!(
            ModuleConfig::try_from(module(Some("node:pool:consumer"), Some("different"))).is_err()
        );
        assert!(
            ModuleConfig::try_from(module(Some("node:pool:default"), Some("default"))).is_err()
        );
    }

    fn app_definition(module: ModuleJson) -> AppDefinitionConfigJson {
        AppDefinitionConfigJson {
            definition: Some(module),
            dependencies: vec![],
            schema: None,
            changed_modules: vec![],
            unchanged_module_hashes: vec![],
            udf_server_version: "1.0.0".to_owned(),
        }
    }

    #[test]
    fn rejects_node_pool_on_application_definition() {
        let definition = module(Some("node:pool:consumer"), Some("consumer"));
        assert!(AppDefinitionConfig::try_from(app_definition(definition)).is_err());
    }

    #[test]
    fn rejects_node_pool_on_component_definition() {
        let definition = module(Some("node:pool:consumer"), Some("consumer"));
        let component = ComponentDefinitionConfigJson {
            definition_path: "component".to_owned(),
            definition,
            dependencies: vec![],
            schema: None,
            functions: vec![],
            udf_server_version: "1.0.0".to_owned(),
        };
        assert!(ComponentDefinitionConfig::try_from(component).is_err());
    }
}

#[cfg(test)]
async fn deployment_test_database(
    runtime: runtime::prod::ProdRuntime,
    persistence: Arc<dyn common::persistence::Persistence>,
) -> anyhow::Result<database::Database<runtime::prod::ProdRuntime>> {
    use common::{
        runtime::new_unlimited_rate_limiter,
        shutdown::ShutdownSignal,
    };
    use database::Database;
    use indexing::index_cache::IndexCache;
    use model::virtual_system_mapping;
    use search::searcher::SearcherStub;

    let (deleted_tablet_sender, _deleted_tablet_receiver) = tokio::sync::mpsc::channel(16);
    Database::load(
        persistence,
        runtime.clone(),
        Arc::new(SearcherStub),
        ShutdownSignal::panic(),
        virtual_system_mapping().clone(),
        IndexCache::new(1 << 20).new_handle(),
        Arc::new(new_unlimited_rate_limiter(runtime)),
        deleted_tablet_sender,
        "deployment_tests".to_owned(),
    )
    .await
}

#[cfg(test)]
mod deployment_receipt_tests {
    use model::deployment_receipts::{
        DeploymentReceipt,
        DeploymentReceiptModel,
    };
    use runtime::prod::ProdRuntime;

    use super::*;

    fn receipt(id: &str, expires: i64) -> DeploymentReceipt {
        DeploymentReceipt {
            operation_id: id.to_owned(),
            input_sha256: "input".to_owned(),
            expires_unix_seconds: expires,
            result_json: "{}".to_owned(),
        }
    }

    fn source() -> SourcePackage {
        SourcePackage {
            storage_key: "deployment-receipt-test".try_into().unwrap(),
            sha256: Sha256Digest::from([1; 32]),
            native_resident: None,
            external_deps_package_id: None,
            package_size: Default::default(),
            node_version: None,
            node_executor_pool_topology: Default::default(),
        }
    }

    #[test]
    fn native_selection_participates_in_publication_occ_and_rejects_omission() -> anyhow::Result<()>
    {
        use model::source_packages::native::{
            NativeResidentActivation,
            NativeResidentDescriptor,
        };
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let db =
                deployment_test_database(rt, Arc::new(sqlite::SqlitePersistence::new(":memory:")?))
                    .await?;
            model::initialize_application_system_tables(&db).await?;
            let descriptor = NativeResidentDescriptor {
                artifact_sha256: "a".repeat(64),
                configuration_sha256: "b".repeat(64),
                lifecycle_protocol: 1,
                application_contract: "example-v1".into(),
            };
            let activation = NativeResidentActivation {
                expected_prior: None,
                target: Some(descriptor.clone()),
                application_contract: Some("example-v1".into()),
            };
            let mut stale = db.begin_system().await?;
            validate_native_resident_activation_in_tx(&mut stale, None).await?;
            let mut native = db.begin_system().await?;
            validate_native_resident_activation_in_tx(&mut native, Some(&activation)).await?;
            let mut package = source();
            package.native_resident = Some(descriptor.clone());
            SourcePackageModel::new(&mut native, TableNamespace::Global)
                .put(package)
                .await?;
            db.commit_with_write_source(native, "native_selection_test")
                .await?;
            SourcePackageModel::new(&mut stale, TableNamespace::Global)
                .put(source())
                .await?;
            assert!(db
                .commit_with_write_source(stale, "stale_native_selection_test")
                .await
                .unwrap_err()
                .is_occ());
            let mut tx = db.begin_system().await?;
            assert!(validate_native_resident_activation_in_tx(&mut tx, None)
                .await
                .is_err());
            assert!(
                validate_native_resident_activation_in_tx(&mut tx, Some(&activation))
                    .await
                    .is_err()
            );
            let retain = NativeResidentActivation {
                expected_prior: Some(descriptor.clone()),
                target: Some(descriptor),
                application_contract: Some("example-v1".into()),
            };
            validate_native_resident_activation_in_tx(&mut tx, Some(&retain)).await?;
            drop(tx);
            db.shutdown().await?;
            anyhow::Ok(())
        })
    }

    #[test]
    fn empty_source_activations_serialize_and_advance_the_generation() -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let db =
                deployment_test_database(rt, Arc::new(sqlite::SqlitePersistence::new(":memory:")?))
                    .await?;
            model::initialize_application_system_tables(&db).await?;
            let mut older = db.begin_system().await?;
            let mut newer = db.begin_system().await?;
            // Empty legacy activations may have no changed module or config
            // rows. Source insertion itself must order their generations.
            SourcePackageModel::new(&mut older, TableNamespace::Global)
                .put(source())
                .await?;
            // Future or tied transaction creation times must not make a later
            // activation sort before the record used as current authority.
            newer.advance_creation_time(common::document::CreationTime::try_from(
                f64::from(newer.next_creation_time()) + 1000.0,
            )?)?;
            let first_id = SourcePackageModel::new(&mut newer, TableNamespace::Global)
                .put(source())
                .await?;
            db.commit_with_write_source(newer, "test_newer_source")
                .await?;
            assert!(db
                .commit_with_write_source(older, "test_older_source")
                .await
                .unwrap_err()
                .is_occ());

            let prepared = PreparedPush {
                packages: BTreeMap::new(),
                external_deps_storage_key: None,
                expected_prior: Some(first_id.into()),
                system_env_var_overrides: BTreeMap::new(),
            };
            let mut retry = db.begin_system().await?;
            prepared.validate_activation(&mut retry).await?;
            let second_id = SourcePackageModel::new(&mut retry, TableNamespace::Global)
                .put(source())
                .await?;
            assert_ne!(first_id, second_id);
            db.commit_with_write_source(retry, "test_source_retry")
                .await?;
            let mut tx = db.begin_system().await?;
            let latest = SourcePackageModel::new(&mut tx, TableNamespace::Global)
                .get_latest_record()
                .await?
                .context("missing source activation")?;
            assert_eq!(latest.developer_id(), DeveloperDocumentId::from(second_id));
            let operation = FinishPushOperation {
                operation_id: "superseded-operation".to_owned(),
                input_sha256: "input".to_owned(),
                expires_unix_seconds: i64::try_from(tx.runtime().unix_timestamp().as_secs())? + 600,
                prepared: Some(Arc::new(prepared)),
            };
            // The early finish check must reject before source resolution and
            // force-capable cutover admission, not only inside the commit.
            assert_eq!(
                operation.validate(&mut tx).await.err().unwrap().short_msg(),
                "DeploymentOperationSuperseded"
            );
            Ok(())
        })
    }

    #[test]
    fn finish_validation_requires_preparation_or_an_exact_receipt() -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let db = deployment_test_database(
                rt.clone(),
                Arc::new(sqlite::SqlitePersistence::new(":memory:")?),
            )
            .await?;
            model::initialize_application_system_tables(&db).await?;
            let mut operation = FinishPushOperation {
                operation_id: "operation".to_owned(),
                input_sha256: "input".to_owned(),
                expires_unix_seconds: i64::try_from(rt.unix_timestamp().as_secs())? + 600,
                prepared: None,
            };
            let mut tx = db.begin_system().await?;
            assert_eq!(
                operation.validate(&mut tx).await.err().unwrap().short_msg(),
                "DeploymentOperationUnknown"
            );
            operation.prepared = Some(Arc::new(PreparedPush {
                packages: BTreeMap::new(),
                external_deps_storage_key: None,
                expected_prior: None,
                system_env_var_overrides: BTreeMap::new(),
            }));
            assert!(operation.validate(&mut tx).await?.is_none());
            let mut committed_receipt = receipt("operation", operation.expires_unix_seconds);
            committed_receipt.result_json = serde_json::to_string(
                &SerializedFinishPushDiff::try_from(FinishPushDiff::default())?,
            )?;
            DeploymentReceiptModel::new(&mut tx)
                .record(committed_receipt)
                .await?;
            let committed = db
                .commit_with_write_source(tx, "test_finish_receipt")
                .await?;
            let mut newer = db.begin_system().await?;
            SourcePackageModel::new(&mut newer, TableNamespace::Global)
                .put(source())
                .await?;
            db.commit_with_write_source(newer, "test_activation_after_receipt")
                .await?;
            let mut tx = db.begin_system().await?;
            // Historical replay wins over the now-stale preparation. It must
            // also remain available when restart removes that preparation.
            let (diff, ts) = operation.validate(&mut tx).await?.unwrap();
            assert_eq!(ts, committed);
            assert_eq!(
                diff.activation_replay.unwrap().node_cutover_completion,
                "unverified"
            );
            operation.prepared = None;
            assert_eq!(operation.validate(&mut tx).await?.unwrap().1, committed);
            operation.input_sha256 = "changed-finish-intent".to_owned();
            assert_eq!(
                operation.validate(&mut tx).await.err().unwrap().short_msg(),
                "DeploymentOperationInputMismatch"
            );
            Ok(())
        })
    }

    #[test]
    fn prepared_activation_rechecks_system_environment_transactionally() -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let db =
                deployment_test_database(rt, Arc::new(sqlite::SqlitePersistence::new(":memory:")?))
                    .await?;
            model::initialize_application_system_tables(&db).await?;
            let prepared = PreparedPush {
                packages: BTreeMap::new(),
                external_deps_storage_key: None,
                expected_prior: None,
                system_env_var_overrides: BTreeMap::new(),
            };
            let mut activation = db.begin_system().await?;
            prepared.validate_activation(&mut activation).await?;
            SourcePackageModel::new(&mut activation, TableNamespace::Global)
                .put(source())
                .await?;
            let mut update = db.begin_system().await?;
            model::canonical_urls::CanonicalUrlsModel::new(&mut update)
                .set_canonical_url(
                    common::http::RequestDestination::ConvexCloud,
                    "https://changed.example.invalid".to_owned(),
                )
                .await?;
            db.commit_with_write_source(update, "test_canonical_url_change")
                .await?;
            assert!(db
                .commit_with_write_source(activation, "test_stale_environment_activation")
                .await
                .unwrap_err()
                .is_occ());
            let mut retry = db.begin_system().await?;
            assert_eq!(
                prepared
                    .validate_activation(&mut retry)
                    .await
                    .unwrap_err()
                    .short_msg(),
                "RaceDetected"
            );
            Ok(())
        })
    }

    #[test]
    fn activation_and_receipt_are_atomic_conflict_and_survive_restart() -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("deployment.sqlite");
            let path = path.to_str().context("test database path must be UTF-8")?;
            let persistence: Arc<dyn common::persistence::Persistence> =
                Arc::new(sqlite::SqlitePersistence::new(path)?);
            let db = deployment_test_database(rt.clone(), persistence.clone()).await?;
            model::initialize_application_system_tables(&db).await?;
            let expiry = i64::try_from(rt.unix_timestamp().as_secs())? + 600;
            let prepared = PreparedPush {
                packages: BTreeMap::new(),
                external_deps_storage_key: None,
                expected_prior: None,
                system_env_var_overrides: BTreeMap::new(),
            };
            let mut first = db.begin_system().await?;
            let mut competing = db.begin_system().await?;
            for tx in [&mut first, &mut competing] {
                assert!(DeploymentReceiptModel::new(tx)
                    .lookup("operation", "input")
                    .await?
                    .is_none());
                prepared.validate_activation(tx).await?;
                SourcePackageModel::new(tx, TableNamespace::Global)
                    .put(source())
                    .await?;
                DeploymentReceiptModel::new(tx)
                    .record(receipt("operation", expiry))
                    .await?;
            }
            let committed = db
                .commit_with_write_source(first, "test_deployment_receipt")
                .await?;
            let conflict = db
                .commit_with_write_source(competing, "test_competing_deployment")
                .await
                .unwrap_err();
            assert!(conflict.is_occ());
            let mut tx = db.begin_system().await?;
            assert!(prepared.validate_activation(&mut tx).await.is_err());
            assert!(DeploymentReceiptModel::new(&mut tx)
                .lookup("operation", "different")
                .await
                .is_err());
            // A failed transaction cannot leave a replay receipt or activation.
            DeploymentReceiptModel::new(&mut tx)
                .record(receipt("aborted", expiry))
                .await?;
            drop(tx);
            db.shutdown().await?;
            drop(db);
            drop(persistence);
            // Reopen persistence so startup sees an existing database. A reused
            // handle retains its initial is_fresh flag and bootstraps again.
            let persistence = Arc::new(sqlite::SqlitePersistence::new(path)?);
            let db = deployment_test_database(rt, persistence).await?;
            let mut tx = db.begin_system().await?;
            assert_eq!(
                DeploymentReceiptModel::new(&mut tx)
                    .lookup("operation", "input")
                    .await?,
                Some(("{}".to_owned(), committed))
            );
            assert!(DeploymentReceiptModel::new(&mut tx)
                .lookup("aborted", "input")
                .await?
                .is_none());
            assert!(prepared.validate_activation(&mut tx).await.is_err());
            Ok(())
        })
    }

    #[test]
    fn receipts_have_finite_capacity_and_reject_expired_activation() -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let db = deployment_test_database(
                rt.clone(),
                Arc::new(sqlite::SqlitePersistence::new(":memory:")?),
            )
            .await?;
            model::initialize_application_system_tables(&db).await?;
            let now = i64::try_from(rt.unix_timestamp().as_secs())?;
            let mut tx = db.begin_system().await?;
            assert!(DeploymentReceiptModel::new(&mut tx)
                .record(receipt("expired", now))
                .await
                .is_err());
            drop(tx);
            for index in 0..16 {
                let mut tx = db.begin_system().await?;
                DeploymentReceiptModel::new(&mut tx)
                    .record(receipt(&format!("operation-{index}"), now + 600))
                    .await?;
                db.commit_with_write_source(tx, "test_receipt_capacity")
                    .await?;
            }
            let mut tx = db.begin_system().await?;
            let error = DeploymentReceiptModel::new(&mut tx)
                .record(receipt("excess", now + 600))
                .await
                .unwrap_err();
            assert_eq!(error.short_msg(), "DeploymentReceiptsFull");
            Ok(())
        })
    }
}

#[derive(Debug)]
pub struct ComponentSchemaStatus {
    pub schema_validation_complete: bool,
    pub indexes_complete: usize,
    pub indexes_total: usize,
}

impl ComponentSchemaStatus {
    pub fn is_complete(&self) -> bool {
        self.schema_validation_complete && self.indexes_complete == self.indexes_total
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ComponentSchemaStatusJson {
    pub schema_validation_complete: bool,
    pub indexes_complete: usize,
    pub indexes_total: usize,
}

impl From<ComponentSchemaStatus> for ComponentSchemaStatusJson {
    fn from(value: ComponentSchemaStatus) -> Self {
        Self {
            schema_validation_complete: value.schema_validation_complete,
            indexes_complete: value.indexes_complete,
            indexes_total: value.indexes_total,
        }
    }
}
