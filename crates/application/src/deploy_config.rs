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
        ObjectKey,
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
#[cfg(feature = "static-hermes-wasmtime-gate")]
use isolate::{
    source_keyed_paired_deployment_guard_enabled,
    source_keyed_runtime_readiness,
    SourceKeyedRuntimeGenerationIdentity,
    SourceKeyedRuntimeReadiness,
};
use keybroker::Identity;
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
    external_packages::{
        types::{
            ExternalDepsPackage,
            ExternalDepsPackageId,
        },
        ExternalPackagesModel,
    },
    modules::module_versions::{
        AnalyzedModule,
        ModuleSource,
        SourceMap,
    },
    source_packages::{
        runtime_content::runtime_content_sha256,
        types::{
            NodeExecutorPoolTopology,
            NodeVersion,
            NodeVersionDiff,
            SourcePackage,
            SourcePackageRuntimeGeneration,
        },
        upload_download::download_package_with_metadata,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceKeyedRuntimeExpectedPrior {
    pub source_package_id: DeveloperDocumentId,
    pub source_package_sha256: Sha256Digest,
    pub source_package_runtime_content_sha256: Option<Sha256Digest>,
    pub runtime_generation: Option<SourcePackageRuntimeGeneration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceKeyedRuntimeActivation {
    pub expected_prior: SourceKeyedRuntimeExpectedPrior,
    pub target_generation: SourcePackageRuntimeGeneration,
}

struct EvaluatedPushContents {
    app: CheckedComponent,
    auth_info: Vec<AuthInfo>,
    component_definition_packages: BTreeMap<ComponentDefinitionPath, SourcePackage>,
    evaluated_components: BTreeMap<ComponentDefinitionPath, EvaluatedComponentDefinition>,
    external_deps_id: Option<ExternalDepsPackageId>,
    user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
    app_functions: Vec<ModuleConfig>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn validate_source_keyed_runtime_readiness(
    paired_primary_enabled: bool,
    runtime_content_sha256: Option<&Sha256Digest>,
    runtime_generation: Option<&SourcePackageRuntimeGeneration>,
    readiness: Option<SourceKeyedRuntimeReadiness>,
) -> anyhow::Result<()> {
    if !paired_primary_enabled {
        return Ok(());
    }
    anyhow::ensure!(
        runtime_content_sha256.is_some(),
        ErrorMetadata::bad_request(
            "StaticHermesWasmGenerationNotReady",
            "The source package has no load-verified Wasm generation staged for paired deployment",
        )
    );
    anyhow::ensure!(
        runtime_generation.is_some(),
        ErrorMetadata::bad_request(
            "StaticHermesWasmGenerationNotReady",
            "The paired deployment request has no exact Wasm generation selector",
        )
    );
    anyhow::ensure!(
        readiness == Some(SourceKeyedRuntimeReadiness::Ready),
        ErrorMetadata::bad_request(
            "StaticHermesWasmGenerationNotReady",
            "The source package has no load-verified Wasm generation staged for paired deployment",
        )
    );
    Ok(())
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
async fn prepare_source_keyed_runtime_activation(
    source_package: &mut SourcePackage,
    activation: Option<SourceKeyedRuntimeActivation>,
) -> anyhow::Result<Option<SourceKeyedRuntimeExpectedPrior>> {
    // Paired activation is opt-in per publication, not mandatory for every client
    // of a Wasm-capable backend. Ordinary clients publish V8 source and clear the
    // selected generation; retained artifacts cannot authorize that new source.
    let Some(activation) = activation else {
        source_package.runtime_generation = None;
        return Ok(None);
    };
    let paired_deployment_enabled = source_keyed_paired_deployment_guard_enabled()?;
    anyhow::ensure!(
        paired_deployment_enabled,
        ErrorMetadata::bad_request(
            "SourceKeyedRuntimeActivationUnsupported",
            "This backend is not configured for paired source-keyed activation",
        )
    );
    let runtime_content_sha256 = source_package.runtime_content_sha256.as_ref();
    let generation_identity = SourceKeyedRuntimeGenerationIdentity {
        deployment_sha256: activation.target_generation.deployment_sha256.as_hex(),
        generation_manifest_sha256: activation
            .target_generation
            .generation_manifest_sha256
            .as_hex(),
        generation_sha256: activation.target_generation.generation_sha256.as_hex(),
    };
    let readiness = match runtime_content_sha256.map(Sha256Digest::as_hex) {
        Some(sha256) => Some(source_keyed_runtime_readiness(&sha256, &generation_identity).await?),
        None => None,
    };
    validate_source_keyed_runtime_readiness(
        paired_deployment_enabled,
        runtime_content_sha256,
        Some(&activation.target_generation),
        readiness,
    )?;
    source_package.runtime_generation = Some(activation.target_generation);
    Ok(Some(activation.expected_prior))
}

#[cfg(not(feature = "static-hermes-wasmtime-gate"))]
async fn prepare_source_keyed_runtime_activation(
    _source_package: &mut SourcePackage,
    activation: Option<SourceKeyedRuntimeActivation>,
) -> anyhow::Result<Option<SourceKeyedRuntimeExpectedPrior>> {
    anyhow::ensure!(
        activation.is_none(),
        ErrorMetadata::bad_request(
            "SourceKeyedRuntimeActivationUnsupported",
            "This backend does not support paired source-keyed activation",
        )
    );
    Ok(None)
}

fn validate_source_keyed_runtime_expected_prior(
    expected: &SourceKeyedRuntimeExpectedPrior,
    actual_id: DeveloperDocumentId,
    actual: &SourcePackage,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        actual_id == expected.source_package_id
            && actual.sha256 == expected.source_package_sha256
            && actual.runtime_content_sha256 == expected.source_package_runtime_content_sha256
            && actual.runtime_generation == expected.runtime_generation,
        ErrorMetadata::bad_request(
            "SourceKeyedRuntimeExpectedPriorMismatch",
            "The committed source/runtime state changed before paired activation",
        )
    );
    Ok(())
}

async fn validate_source_keyed_runtime_expected_prior_in_tx<RT: Runtime>(
    tx: &mut database::Transaction<RT>,
    expected: &SourceKeyedRuntimeExpectedPrior,
) -> anyhow::Result<()> {
    let actual = SourcePackageModel::new(tx, TableNamespace::Global)
        .get_latest_record()
        .await?
        .context(ErrorMetadata::bad_request(
            "SourceKeyedRuntimeExpectedPriorMismatch",
            "The committed source package disappeared before paired activation",
        ))?;
    validate_source_keyed_runtime_expected_prior(expected, actual.developer_id(), &actual)
}

fn restore_source_package_runtime_content_sha256(
    source_package: &mut SourcePackage,
    verified_runtime_content_sha256: Sha256Digest,
) -> anyhow::Result<()> {
    if let Some(returned_runtime_content_sha256) = &source_package.runtime_content_sha256 {
        anyhow::ensure!(
            returned_runtime_content_sha256 == &verified_runtime_content_sha256,
            ErrorMetadata::bad_request(
                "SourcePackageRuntimeContentMismatch",
                "The source package runtime-content identity does not match its downloaded \
                 contents",
            )
        );
    }
    source_package.runtime_content_sha256 = Some(verified_runtime_content_sha256);
    Ok(())
}

fn validate_source_package_external_deps_identity(
    archive_external_deps_storage_key: Option<&ObjectKey>,
    external_deps_package: Option<&ExternalDepsPackage>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        archive_external_deps_storage_key
            == external_deps_package.map(|package| &package.storage_key),
        ErrorMetadata::bad_request(
            "SourcePackageExternalDepsMismatch",
            "The source package dependencies do not match its downloaded contents",
        )
    );
    Ok(())
}

impl<RT: Runtime> Application<RT> {
    async fn restore_downloaded_source_package_runtime_content_identity(
        &self,
        source_package: &mut SourcePackage,
        downloaded_package: &BTreeMap<CanonicalizedModulePath, ModuleConfig>,
        archive_external_deps_storage_key: Option<&ObjectKey>,
    ) -> anyhow::Result<()> {
        let external_deps_package = match &source_package.external_deps_package_id {
            Some(external_deps_package_id) => {
                let mut tx = self.begin(Identity::system()).await?;
                let external_deps_package = ExternalPackagesModel::new(&mut tx)
                    .get(external_deps_package_id.clone())
                    .await?
                    .into_value();
                tx.into_token()?;
                Some(external_deps_package)
            },
            None => None,
        };
        validate_source_package_external_deps_identity(
            archive_external_deps_storage_key,
            external_deps_package.as_ref(),
        )?;
        let modules = downloaded_package.values().cloned().collect::<Vec<_>>();
        let verified_runtime_content_sha256 = runtime_content_sha256(
            &modules,
            external_deps_package.as_ref(),
            source_package.node_version,
        )?;
        restore_source_package_runtime_content_sha256(
            source_package,
            verified_runtime_content_sha256,
        )
    }

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
        let EvaluatedPushContents {
            app,
            auth_info,
            component_definition_packages,
            mut evaluated_components,
            external_deps_id,
            user_environment_variables,
            app_functions,
        } = self.evaluate_push_contents(config).await?;

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
            component_definition_packages,
            app_auth: auth_info,
            analysis: evaluated_components,
            app,
            schema_change,
        };
        Ok(StartPushResult {
            response: resp,
            app_functions,
        })
    }

    #[fastrace::trace]
    async fn evaluate_push_contents(
        &self,
        config: &ProjectConfig,
    ) -> anyhow::Result<EvaluatedPushContents> {
        let (external_deps_id, component_definition_packages, app_functions) =
            self.upload_packages(config).await?;

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
                system_env_var_overrides,
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
        component_definition_packages: &BTreeMap<ComponentDefinitionPath, SourcePackage>,
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
        let EvaluatedPushContents {
            app,
            mut evaluated_components,
            ..
        } = self.evaluate_push_contents(config).await?;

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
        // so run the prediction on its own task.
        try_join("evaluate_schema_prediction", async move {
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
        })
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
        source_keyed_runtime_activation: Option<SourceKeyedRuntimeActivation>,
        force_node_cutover: bool,
    ) -> anyhow::Result<(FinishPushDiff, Timestamp)> {
        // Download all source packages. We can remove this once we don't store source
        // in the database.
        let mut downloaded_source_packages = BTreeMap::new();
        let mut root_archive_external_deps_storage_key = None;
        for (definition_path, source_package) in &mut start_push.component_definition_packages {
            // `StartPushResponse` crosses a client round trip. A client cannot
            // provide durable runtime-generation authority for any component.
            source_package.runtime_generation = None;
            let downloaded_package =
                download_package_with_metadata(self.modules_storage().clone(), source_package)
                    .await?;
            if definition_path.is_root() {
                root_archive_external_deps_storage_key =
                    downloaded_package.external_deps_storage_key.clone();
            }
            let package = downloaded_package.modules;
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
            // Rebuild complete topology metadata from the archive so a client
            // that omits a newly added optional field cannot weaken the commit.
            source_package.node_executor_pool_topology =
                node_executor_pool_topology(package.values())?;
            downloaded_source_packages.insert(definition_path.clone(), package);
        }
        let root_definition_path = ComponentDefinitionPath::root();
        let downloaded_root_package = downloaded_source_packages
            .get(&root_definition_path)
            .context("No downloaded source package for the root component")?;
        let root_source_package = start_push
            .component_definition_packages
            .get_mut(&root_definition_path)
            .context("No source package for the root component")?;
        // The source-package response crosses a client round trip. Restore the
        // runtime identity from the downloaded contents before it becomes
        // deployment authority.
        self.restore_downloaded_source_package_runtime_content_identity(
            root_source_package,
            downloaded_root_package,
            root_archive_external_deps_storage_key.as_ref(),
        )
        .await?;
        let committed_pool_topology = start_push
            .component_definition_packages
            .get(&root_definition_path)
            .context("No source package for the root component")?
            .node_executor_pool_topology
            .clone();
        // The response crossed a client round trip after start-push
        // validation. Validate the archive-normalized topology again so the
        // durable commit cannot exceed this runtime's pool capability or
        // configured process budget.
        self.runner()
            .validate_node_executor_pool_topology(&committed_pool_topology)?;

        let source_keyed_runtime_expected_prior = {
            let source_package = start_push
                .component_definition_packages
                .get_mut(&root_definition_path)
                .context("No source package for the root component")?;
            prepare_source_keyed_runtime_activation(source_package, source_keyed_runtime_activation)
                .await?
        };

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

        let (diff, ts) = self
            .execute_with_audit_log_events_and_occ_retries_with_timestamp(
                identity.clone(),
                request_metadata,
                finish_push_write_source,
                *FINISH_PUSH_MAX_OCC_FAILURES,
                |tx| {
                    let start_push = &start_push;
                    let downloaded_source_packages = &downloaded_source_packages;
                    let message = &message;
                    let source_keyed_runtime_expected_prior = &source_keyed_runtime_expected_prior;
                    async move {
                        if let Some(expected_prior) = source_keyed_runtime_expected_prior {
                            validate_source_keyed_runtime_expected_prior_in_tx(tx, expected_prior)
                                .await?;
                        }
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
                            .get_latest_record()
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
                            .get_latest_record()
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
                        Ok((diff, audit_log_events))
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
        force_node_cutover: bool,
    ) -> anyhow::Result<(PushAnalytics, PushMetrics)> {
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

        let mut source_package = self
            .upload_package(&modules, external_deps_id_and_pkg, node_version)
            .await?;
        prepare_source_keyed_runtime_activation(&mut source_package, None).await?;
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
        let user_environment_variables = EnvironmentVariablesModel::new(&mut tx).get_all().await?;
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
            .reserve_node_executor_pool_cutover(&committed_pool_topology, force_node_cutover)
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
                build_external_deps_time: end_build_external_deps - begin_build_external_deps,
                upload_source_package_time: end_upload_source_package - end_build_external_deps,
                analyze_time: end_analyze - begin_analyze,
                occ_stats,
            },
        ))
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

    /// Omitted by ordinary clients; provided when staging selected exact
    /// dependency material.
    #[serde(
        default,
        deserialize_with = "SerializedExternalDepsPackageSelection::deserialize_present"
    )]
    pub external_deps_package: Option<SerializedExternalDepsPackageSelection>,

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
            external_deps_package: self
                .external_deps_package
                .map(TryInto::try_into)
                .transpose()?,
            node_version,
            dry_run: self.dry_run,
            for_codegen: self.for_codegen,
            include_analysis: self.include_analysis,
        })
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SerializedExternalDepsPackageSelection {
    pub id: String,
    pub sha256: String,
}

impl SerializedExternalDepsPackageSelection {
    fn deserialize_present<'de, D>(deserializer: D) -> Result<Option<Self>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // A present null selector must not silently select the ordinary build path.
        Self::deserialize(deserializer).map(Some)
    }
}

impl TryFrom<SerializedExternalDepsPackageSelection>
    for model::external_packages::types::ExternalDepsPackageSelection
{
    type Error = anyhow::Error;

    fn try_from(value: SerializedExternalDepsPackageSelection) -> anyhow::Result<Self> {
        anyhow::ensure!(
            value.sha256.len() == 64
                && value
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "External dependency package SHA-256 must be lowercase hex"
        );
        Ok(Self {
            id: value.id.try_into()?,
            sha256: const_hex::decode(value.sha256)?.try_into()?,
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

#[cfg(all(test, feature = "static-hermes-wasmtime-gate"))]
mod source_keyed_deployment_readiness_tests {
    use std::sync::Arc;

    use anyhow::Context;
    use common::{
        persistence::Persistence,
        runtime::new_unlimited_rate_limiter,
        shutdown::ShutdownSignal,
    };
    use database::Database;
    use indexing::index_cache::IndexCache;
    use model::{
        initialize_application_system_tables,
        virtual_system_mapping,
    };
    use runtime::prod::ProdRuntime;
    use search::searcher::SearcherStub;
    use sqlite::SqlitePersistence;

    use super::*;

    #[test]
    fn selected_external_deps_survive_cache_replacement_and_eviction() -> anyhow::Result<()> {
        use model::external_packages::types::ExternalDepsPackageSelection;

        let tokio = ProdRuntime::init_tokio()?;
        let runtime = ProdRuntime::new(&tokio);
        let block_runtime = runtime.clone();
        block_runtime.block_on("selected_external_deps", async move {
            let database = new_test_database(runtime).await?;
            initialize_application_system_tables(&database).await?;
            let dependencies = vec![NodeDependency {
                package: "example-package".to_owned(),
                version: "1.0.0".to_owned(),
            }];
            let mut original = external_deps_package("original-dependencies");
            original.deps = dependencies.clone();
            let mut tx = database.begin_system().await?;
            let id = ExternalPackagesModel::new(&mut tx)
                .put(original.clone())
                .await?;
            database
                .commit_with_write_source(tx, "test_original_dependencies")
                .await?;
            let selection = ExternalDepsPackageSelection {
                id,
                sha256: original.sha256.clone(),
            };

            let mut replacement = original.clone();
            replacement.sha256 = Sha256Digest::from([8; 32]);
            replacement.storage_key = "replacement-dependencies".try_into()?;
            let mut tx = database.begin_system().await?;
            let replacement_id = ExternalPackagesModel::new(&mut tx)
                .put(replacement.clone())
                .await?;
            database
                .commit_with_write_source(tx, "test_replacement_dependencies")
                .await?;
            let mut tx = database.begin_system().await?;
            let mut model = ExternalPackagesModel::new(&mut tx);
            assert_eq!(
                model
                    .get_cached_package_match(dependencies.clone())
                    .await?
                    .unwrap()
                    .0,
                replacement_id
            );
            assert_eq!(model.get_selected(&selection).await?, original);

            for index in 0..11 {
                let mut other = replacement.clone();
                other.deps[0].package = format!("other-package-{index}");
                let mut tx = database.begin_system().await?;
                ExternalPackagesModel::new(&mut tx).put(other).await?;
                database
                    .commit_with_write_source(tx, "test_dependency_cache_churn")
                    .await?;
            }
            let mut tx = database.begin_system().await?;
            let mut model = ExternalPackagesModel::new(&mut tx);
            assert!(model
                .get_cached_package_match(dependencies.clone())
                .await?
                .is_none());
            let selected = model.get_selected(&selection).await?;
            selected.validate_dependencies(&dependencies)?;
            assert_eq!(selected, original);
            assert_eq!(
                runtime_content_sha256(&[], Some(&selected), None)?,
                runtime_content_sha256(&[], Some(&original), None)?
            );
            assert_ne!(
                runtime_content_sha256(&[], Some(&selected), None)?,
                runtime_content_sha256(&[], Some(&replacement), None)?
            );
            assert!(model
                .get_selected(&ExternalDepsPackageSelection {
                    sha256: replacement.sha256,
                    ..selection.clone()
                })
                .await
                .is_err());
            assert!(model
                .get_selected(&ExternalDepsPackageSelection {
                    id: DeveloperDocumentId::MAX.into(),
                    ..selection
                })
                .await
                .is_err());
            Ok(())
        })
    }

    fn source_package(runtime_content_sha256: Option<Sha256Digest>) -> SourcePackage {
        SourcePackage {
            storage_key: "runtime-content-identity-test".try_into().unwrap(),
            sha256: Sha256Digest::from([0; 32]),
            runtime_content_sha256,
            runtime_generation: None,
            external_deps_package_id: None,
            package_size: Default::default(),
            node_version: None,
            node_executor_pool_topology: Default::default(),
        }
    }

    fn runtime_generation() -> SourcePackageRuntimeGeneration {
        SourcePackageRuntimeGeneration {
            deployment_sha256: Sha256Digest::from([2; 32]),
            generation_manifest_sha256: Sha256Digest::from([3; 32]),
            generation_sha256: Sha256Digest::from([4; 32]),
        }
    }

    fn external_deps_package(storage_key: &str) -> ExternalDepsPackage {
        ExternalDepsPackage {
            storage_key: storage_key.try_into().unwrap(),
            sha256: Sha256Digest::from([9; 32]),
            deps: vec![],
            package_size: Default::default(),
        }
    }

    async fn new_test_database(runtime: ProdRuntime) -> anyhow::Result<Database<ProdRuntime>> {
        let persistence: Arc<dyn Persistence> = Arc::new(SqlitePersistence::new(":memory:")?);
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
            "source_keyed_finish_push_tests".to_owned(),
        )
        .await
    }

    fn source_package_with_generation(
        sha256_byte: u8,
        runtime_generation: Option<SourcePackageRuntimeGeneration>,
    ) -> SourcePackage {
        SourcePackage {
            storage_key: format!("source-keyed-finish-push-{sha256_byte}")
                .try_into()
                .unwrap(),
            sha256: Sha256Digest::from([sha256_byte; 32]),
            runtime_content_sha256: Some(Sha256Digest::from([sha256_byte.wrapping_add(1); 32])),
            runtime_generation,
            external_deps_package_id: None,
            package_size: Default::default(),
            node_version: None,
            node_executor_pool_topology: Default::default(),
        }
    }

    async fn commit_source_package(
        database: &Database<ProdRuntime>,
        source_package: SourcePackage,
        write_source: &'static str,
    ) -> anyhow::Result<()> {
        let mut tx = database.begin_system().await?;
        SourcePackageModel::new(&mut tx, TableNamespace::Global)
            .put(source_package)
            .await?;
        database.commit_with_write_source(tx, write_source).await?;
        Ok(())
    }

    async fn latest_source_package(
        database: &Database<ProdRuntime>,
    ) -> anyhow::Result<(DeveloperDocumentId, SourcePackage)> {
        let mut tx = database.begin_system().await?;
        let source_package = SourcePackageModel::new(&mut tx, TableNamespace::Global)
            .get_latest_record()
            .await?
            .context("source-keyed finish-push test has no source package")?;
        let source_package_id = source_package.developer_id();
        let source_package = source_package.as_ref().clone().into_value();
        tx.into_token()?;
        Ok((source_package_id, source_package))
    }

    fn expected_prior(
        source_package_id: DeveloperDocumentId,
        source_package: &SourcePackage,
    ) -> SourceKeyedRuntimeExpectedPrior {
        SourceKeyedRuntimeExpectedPrior {
            source_package_id,
            source_package_sha256: source_package.sha256.clone(),
            source_package_runtime_content_sha256: source_package.runtime_content_sha256.clone(),
            runtime_generation: source_package.runtime_generation.clone(),
        }
    }

    async fn commit_paired_source_package(
        database: &Database<ProdRuntime>,
        expected_prior: &SourceKeyedRuntimeExpectedPrior,
        source_package: SourcePackage,
    ) -> anyhow::Result<()> {
        let mut tx = database.begin_system().await?;
        validate_source_keyed_runtime_expected_prior_in_tx(&mut tx, expected_prior).await?;
        SourcePackageModel::new(&mut tx, TableNamespace::Global)
            .put(source_package)
            .await?;
        database
            .commit_with_write_source(tx, "source_keyed_paired_finish_push")
            .await?;
        Ok(())
    }

    #[test]
    fn downloaded_runtime_content_identity_is_restored_but_cannot_be_replaced() {
        let verified = Sha256Digest::from([1; 32]);
        let mut omitted = source_package(None);
        restore_source_package_runtime_content_sha256(&mut omitted, verified.clone()).unwrap();
        assert_eq!(omitted.runtime_content_sha256, Some(verified.clone()));

        let mut matching = source_package(Some(verified.clone()));
        restore_source_package_runtime_content_sha256(&mut matching, verified.clone()).unwrap();
        assert_eq!(matching.runtime_content_sha256, Some(verified));

        let mut replaced = source_package(Some(Sha256Digest::from([2; 32])));
        assert!(restore_source_package_runtime_content_sha256(
            &mut replaced,
            Sha256Digest::from([1; 32]),
        )
        .is_err());
    }

    #[test]
    fn downloaded_external_deps_identity_must_match_the_source_package_record() {
        let external_deps = external_deps_package("source-keyed-external-deps");
        assert!(validate_source_package_external_deps_identity(
            Some(&external_deps.storage_key),
            Some(&external_deps),
        )
        .is_ok());
        assert!(validate_source_package_external_deps_identity(None, None).is_ok());

        let different_external_deps = external_deps_package("source-keyed-other-external-deps");
        assert!(validate_source_package_external_deps_identity(
            Some(&different_external_deps.storage_key),
            Some(&external_deps),
        )
        .is_err());
        assert!(
            validate_source_package_external_deps_identity(None, Some(&external_deps)).is_err()
        );
        assert!(validate_source_package_external_deps_identity(
            Some(&external_deps.storage_key),
            None,
        )
        .is_err());
    }

    #[test]
    fn ordinary_publication_clears_wasm_selection_without_requiring_runtime_configuration(
    ) -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let runtime = ProdRuntime::new(&tokio);
        runtime.block_on("ordinary_v8_publication", async {
            for digest in [None, Some(Sha256Digest::from([1; 32]))] {
                let mut package = source_package(digest.clone());
                package.runtime_generation = Some(runtime_generation());
                assert!(prepare_source_keyed_runtime_activation(&mut package, None)
                    .await?
                    .is_none());
                assert!(package.runtime_generation.is_none());
                assert_eq!(package.runtime_content_sha256, digest);
            }
            Ok(())
        })
    }

    #[test]
    fn paired_source_package_requires_the_exact_ready_runtime_content() {
        let runtime_content_sha256 = Sha256Digest::from([1; 32]);
        let runtime_generation = runtime_generation();
        assert!(validate_source_keyed_runtime_readiness(
            true,
            Some(&runtime_content_sha256),
            Some(&runtime_generation),
            Some(SourceKeyedRuntimeReadiness::Ready),
        )
        .is_ok());
        assert!(validate_source_keyed_runtime_readiness(
            true,
            Some(&runtime_content_sha256),
            Some(&runtime_generation),
            Some(SourceKeyedRuntimeReadiness::NotStaged),
        )
        .is_err());
        assert!(validate_source_keyed_runtime_readiness(
            true,
            Some(&runtime_content_sha256),
            Some(&runtime_generation),
            None,
        )
        .is_err());
        assert!(validate_source_keyed_runtime_readiness(
            true,
            None,
            Some(&runtime_generation),
            Some(SourceKeyedRuntimeReadiness::Ready),
        )
        .is_err());
        assert!(validate_source_keyed_runtime_readiness(
            true,
            Some(&runtime_content_sha256),
            None,
            Some(SourceKeyedRuntimeReadiness::Ready),
        )
        .is_err());
    }

    #[test]
    fn disabled_paired_deployment_ignores_catalog_readiness() {
        assert!(validate_source_keyed_runtime_readiness(false, None, None, None).is_ok());
        assert!(validate_source_keyed_runtime_readiness(
            false,
            None,
            None,
            Some(SourceKeyedRuntimeReadiness::NotStaged),
        )
        .is_ok());
    }

    #[test]
    fn expected_prior_requires_the_exact_transaction_visible_source_and_generation() {
        let mut actual = source_package(Some(Sha256Digest::from([1; 32])));
        actual.sha256 = Sha256Digest::from([5; 32]);
        actual.runtime_generation = Some(runtime_generation());
        let expected = SourceKeyedRuntimeExpectedPrior {
            source_package_id: DeveloperDocumentId::MIN,
            source_package_sha256: actual.sha256.clone(),
            source_package_runtime_content_sha256: actual.runtime_content_sha256.clone(),
            runtime_generation: actual.runtime_generation.clone(),
        };
        validate_source_keyed_runtime_expected_prior(&expected, DeveloperDocumentId::MIN, &actual)
            .unwrap();

        assert!(validate_source_keyed_runtime_expected_prior(
            &expected,
            DeveloperDocumentId::MAX,
            &actual,
        )
        .is_err());
        actual.runtime_generation = None;
        assert!(validate_source_keyed_runtime_expected_prior(
            &expected,
            DeveloperDocumentId::MIN,
            &actual,
        )
        .is_err());
    }

    #[test]
    fn source_keyed_finish_push_persists_exact_generation_and_fences_both_commit_orders(
    ) -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let runtime = ProdRuntime::new(&tokio);
        let block_runtime = runtime.clone();
        block_runtime.block_on("source_keyed_finish_push_fencing", async move {
            let foreign_first_database = new_test_database(runtime.clone()).await?;
            initialize_application_system_tables(&foreign_first_database).await?;
            let initial = source_package_with_generation(10, None);
            commit_source_package(
                &foreign_first_database,
                initial,
                "source_keyed_finish_push_initial",
            )
            .await?;
            let (initial_id, initial) = latest_source_package(&foreign_first_database).await?;
            let initial_expected_prior = expected_prior(initial_id, &initial);
            let mut stale_paired_tx = foreign_first_database.begin_system().await?;
            validate_source_keyed_runtime_expected_prior_in_tx(
                &mut stale_paired_tx,
                &initial_expected_prior,
            )
            .await?;
            SourcePackageModel::new(&mut stale_paired_tx, TableNamespace::Global)
                .put(source_package_with_generation(
                    30,
                    Some(runtime_generation()),
                ))
                .await?;
            let foreign = source_package_with_generation(20, None);
            commit_source_package(
                &foreign_first_database,
                foreign.clone(),
                "source_keyed_finish_push_competing",
            )
            .await?;
            let stale_commit_error = foreign_first_database
                .commit_with_write_source(stale_paired_tx, "source_keyed_paired_finish_push")
                .await
                .err()
                .context("the transaction-visible expected-prior read did not fence its commit")?;
            assert!(stale_commit_error.occ_info().is_some());
            // An OCC retry starts a fresh transaction and observes the competing
            // source package, so the exact expected-prior check must then reject it.
            assert!(commit_paired_source_package(
                &foreign_first_database,
                &initial_expected_prior,
                source_package_with_generation(30, Some(runtime_generation())),
            )
            .await
            .is_err());
            let (_, active_after_foreign_first) =
                latest_source_package(&foreign_first_database).await?;
            assert_eq!(active_after_foreign_first, foreign);

            let paired_first_database = new_test_database(runtime.clone()).await?;
            initialize_application_system_tables(&paired_first_database).await?;
            let initial = source_package_with_generation(40, None);
            commit_source_package(
                &paired_first_database,
                initial,
                "source_keyed_finish_push_initial",
            )
            .await?;
            let (initial_id, initial) = latest_source_package(&paired_first_database).await?;
            let initial_expected_prior = expected_prior(initial_id, &initial);
            let paired = source_package_with_generation(50, Some(runtime_generation()));
            commit_paired_source_package(
                &paired_first_database,
                &initial_expected_prior,
                paired.clone(),
            )
            .await?;
            let (_, active_after_paired_first) =
                latest_source_package(&paired_first_database).await?;
            assert_eq!(active_after_paired_first, paired);

            let stale_foreign = source_package_with_generation(60, Some(runtime_generation()));
            assert!(commit_paired_source_package(
                &paired_first_database,
                &initial_expected_prior,
                stale_foreign,
            )
            .await
            .is_err());
            let (_, active_after_foreign_second) =
                latest_source_package(&paired_first_database).await?;
            assert_eq!(active_after_foreign_second, paired);
            foreign_first_database.shutdown().await?;
            paired_first_database.shutdown().await?;
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
