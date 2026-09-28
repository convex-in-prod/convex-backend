use super::*;

#[test]
fn generated_publication_separates_activation_from_behavior_dependencies() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("generated_selective_invalidation", async move {
        let database = new_test_database(rt).await?;
        install_generated_test_function(&database).await?;
        let path = CanonicalizedComponentModulePath {
            component: ComponentId::Root,
            module_path: "scheduled.js".parse()?,
        };
        let mut setup = database.begin_system().await?;
        let module = ModuleModel::new(&mut setup)
            .get_metadata(path.clone())
            .await?
            .context("missing module")?;
        let package = SourcePackageModel::new(&mut setup, TableNamespace::Global)
            .get_latest_record()
            .await?
            .context("missing package")?;
        let expected = DeployedRuntimeIdentity::for_runtime_content_test(
            module.sha256.as_hex(),
            package
                .runtime_content_sha256
                .as_ref()
                .context("missing source identity")?
                .as_hex(),
        );
        drop(setup);

        let mut old = database.begin_system().await?;
        verify_deployed_source_identity(&mut old, &path.module_path, &expected).await?;
        let mut behavioral_read = database.begin_system().await?;
        verify_deployed_source_identity(&mut behavioral_read, &path.module_path, &expected).await?;
        let token = behavioral_read.into_token()?;
        let subscription = database.subscribe(token.clone()).await?;
        let mut explicit_metadata_read = database.begin_system().await?;
        SourcePackageModel::new(&mut explicit_metadata_read, TableNamespace::Global)
            .get_latest_record()
            .await?;
        let metadata_token = explicit_metadata_read.into_token()?;

        let mut publish = database.begin_system().await?;
        let mut next = (**package).clone();
        next.runtime_generation = Some(
            model::source_packages::types::SourcePackageRuntimeGeneration {
                deployment_sha256: Sha256Digest::from([3; 32]),
                generation_manifest_sha256: Sha256Digest::from([4; 32]),
                generation_sha256: Sha256Digest::from([5; 32]),
            },
        );
        let next_id = SourcePackageModel::new(&mut publish, TableNamespace::Global)
            .put(next.clone())
            .await?;
        // A pending activation-only publication has the same authenticated graph.
        verify_deployed_source_identity(&mut publish, &path.module_path, &expected).await?;
        // Pending publication must select its own package, not the snapshot clone.
        assert_eq!(
            SourcePackageModel::new(&mut publish, TableNamespace::Global)
                .get_latest_for_runtime_selection()
                .await?
                .context("pending package missing")?
                .developer_id(),
            DeveloperDocumentId::from(next_id)
        );
        ModuleModel::new(&mut publish)
            .put(
                Some(module.id()),
                path.clone(),
                ModuleSource::from(""),
                next_id,
                None,
                module.analyze_result.clone(),
                ModuleEnvironment::Isolate,
                None,
            )
            .await?;
        let publication_ts = database
            .commit_with_write_source(publish, "selective_invalidation_test")
            .await?;
        assert!(database
            .refresh_token(token.clone(), publication_ts)
            .await?
            .is_ok());
        // An application explicitly observing deployment/native authority must retry.
        assert!(database
            .refresh_token(metadata_token, publication_ts)
            .await?
            .is_err());

        assert_eq!(
            SourcePackageModel::new(&mut old, TableNamespace::Global)
                .get_latest_for_runtime_selection()
                .await?
                .context("old package missing")?
                .id(),
            package.id()
        );
        verify_deployed_source_identity(&mut old, &path.module_path, &expected).await?;
        drop(old);
        let mut current = database.begin_system().await?;
        let selected = SourcePackageModel::new(&mut current, TableNamespace::Global)
            .get_latest_for_runtime_selection()
            .await?
            .context("current package missing")?;
        assert_eq!(selected.runtime_generation, next.runtime_generation);
        assert_eq!(selected.developer_id(), DeveloperDocumentId::from(next_id));
        assert_eq!(
            ModuleModel::new(&mut current)
                .get_metadata(path.clone())
                .await?
                .context("module missing")?
                .source_package_id,
            module.source_package_id
        );
        verify_deployed_source_identity(&mut current, &path.module_path, &expected).await?;
        drop(current);

        // A standalone/pending module must not authenticate against the committed
        // snapshot's entry bytes, even though it has no new activation record.
        let mut pending = database.begin_system().await?;
        ModuleModel::new(&mut pending)
            .put(
                Some(module.id()),
                path.clone(),
                ModuleSource::from("export const changed = true;"),
                next_id,
                None,
                module.analyze_result.clone(),
                ModuleEnvironment::Isolate,
                None,
            )
            .await?;
        assert!(
            verify_deployed_source_identity(&mut pending, &path.module_path, &expected)
                .await
                .is_err()
        );
        drop(pending);

        // An unchanged entry does not authenticate a pending imported module.
        // A fresh activation record cannot prove that edited graph either.
        let mut pending_import = database.begin_system().await?;
        let imported_path = CanonicalizedComponentModulePath {
            component: ComponentId::Root,
            module_path: "_deps/imported.js".parse()?,
        };
        ModuleModel::new(&mut pending_import)
            .put(
                None,
                imported_path,
                ModuleSource::from("export const changed = true;"),
                next_id,
                None,
                None,
                ModuleEnvironment::Isolate,
                None,
            )
            .await?;
        assert!(
            verify_deployed_source_identity(&mut pending_import, &path.module_path, &expected)
                .await
                .is_err()
        );
        SourcePackageModel::new(&mut pending_import, TableNamespace::Global)
            .put(next.clone())
            .await?;
        assert!(
            verify_deployed_source_identity(&mut pending_import, &path.module_path, &expected)
                .await
                .is_err()
        );
        drop(pending_import);

        // Current artifact contracts conservatively cover the complete module
        // namespace, including previously missing imports and future additions.
        let mut change = database.begin_system().await?;
        ModuleModel::new(&mut change)
            .put(
                None,
                CanonicalizedComponentModulePath {
                    component: ComponentId::Root,
                    module_path: "_deps/imported.js".parse()?,
                },
                ModuleSource::from("export const changed = true;"),
                next_id,
                None,
                None,
                ModuleEnvironment::Isolate,
                None,
            )
            .await?;
        let changed_ts = database
            .commit_with_write_source(change, "selective_invalidation_test")
            .await?;
        assert!(database.refresh_token(token, changed_ts).await?.is_err());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), subscription.wait_for_invalidation())
                .await?,
            Some(changed_ts)
        );
        database.shutdown().await?;
        Ok(())
    })
}

#[test]
fn generated_initialization_ignores_unrelated_table_metadata() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("generated_initialization_table_dependency", async move {
        let database = new_test_database(rt.clone()).await?;
        initialize_application_system_tables(&database).await?;
        let name: EnvVarName = "INITIALIZATION_TABLE_DEPENDENCY".parse()?;
        let value: EnvVarValue = "unchanged".parse()?;
        let mut setup = database.begin_system().await?;
        EnvironmentVariablesModel::new(&mut setup)
            .create(
                EnvironmentVariable::new(name.clone(), value.clone()),
                &Default::default(),
            )
            .await?;
        database
            .commit_with_write_source(setup, "initialization_table_dependency_setup")
            .await?;

        let read_set =
            capture_configuration_reads(rt, &database, &[(name.clone(), Some(value.clone()))])
                .await?;

        let mut old_snapshot = database.begin_system().await?;
        assert!(
            ContextCache::validate_and_apply_context_read_set(&mut old_snapshot, &read_set).await?
        );
        let mut update = database.begin_system().await?;
        database::TableModel::new(&mut update)
            .insert_table_metadata(TableNamespace::Global, &"unrelated_fixture_table".parse()?)
            .await?;
        let updated_ts = database
            .commit_with_write_source(update, "initialization_unrelated_table")
            .await?;

        // The provider configuration and the old transaction remain unchanged.
        assert!(
            ContextCache::validate_and_apply_context_read_set(&mut old_snapshot, &read_set).await?
        );
        // Validation must not reintroduce a catalog dependency into the
        // request, even when all saved initialization hashes still match.
        assert!(database
            .refresh_token(old_snapshot.into_token()?, updated_ts)
            .await?
            .is_ok());
        let mut current = database.begin_system().await?;
        assert_eq!(
            EnvironmentVariablesModel::new(&mut current)
                .get(&name)
                .await?
                .context("fixture environment variable disappeared")?
                .value(),
            &value,
        );
        assert!(ContextCache::validate_and_apply_context_read_set(&mut current, &read_set).await?);
        drop(current);
        database.shutdown().await?;
        Ok(())
    })
}

async fn capture_configuration_reads(
    rt: ProdRuntime,
    database: &Database<ProdRuntime>,
    observations: &[(EnvVarName, Option<EnvVarValue>)],
) -> anyhow::Result<ContextReadSet> {
    let mut state = new_state(
        rt,
        database.begin_system().await?,
        None,
        Arc::new(GateMetrics::default()),
        QueryJournal::new(),
    )
    .await?;
    state.provider.snoop_initialization_reads()?;
    let timeout = state
        .timeout
        .as_mut()
        .context("missing initialization timeout")?;
    state.provider.initialize_static_hermes(timeout).await?;
    for (name, expected) in observations {
        assert_eq!(&state.provider.get_environment_variable(name)?, expected);
    }
    let reads = state.provider.finish_initialization_reads()?;
    ContextCache::capture_context_read_set(reads, state.provider.tx_for_initialization()?)
        .await?
        .context("missing captured initialization reads")
}

#[test]
fn generated_initialization_preserves_configuration_invalidation() -> anyhow::Result<()> {
    use common::http::RequestDestination;
    use model::canonical_urls::CanonicalUrlsModel;

    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("generated_configuration_invalidation", async move {
        for change in ["observed_value", "missing_variable", "canonical_url"] {
            let database = new_test_database(rt.clone()).await?;
            initialize_application_system_tables(&database).await?;
            let name: EnvVarName = "OBSERVED_SETTING".parse()?;
            let missing: EnvVarName = "MISSING_SETTING".parse()?;
            let value: EnvVarValue = "before".parse()?;
            let mut setup = database.begin_system().await?;
            EnvironmentVariablesModel::new(&mut setup)
                .create(
                    EnvironmentVariable::new(name.clone(), value.clone()),
                    &Default::default(),
                )
                .await?;
            database
                .commit_with_write_source(setup, "configuration_setup")
                .await?;
            let read_set = capture_configuration_reads(
                rt.clone(),
                &database,
                &[(name.clone(), Some(value)), (missing.clone(), None)],
            )
            .await?;
            let mut old = database.begin_system().await?;
            let mut update = database.begin_system().await?;
            match change {
                "observed_value" => {
                    EnvironmentVariablesModel::new(&mut update)
                        .delete(&name)
                        .await?
                        .context("fixture variable disappeared")?;
                    EnvironmentVariablesModel::new(&mut update)
                        .create(
                            EnvironmentVariable::new(name, "after".parse()?),
                            &Default::default(),
                        )
                        .await?;
                },
                "missing_variable" => {
                    EnvironmentVariablesModel::new(&mut update)
                        .create(
                            EnvironmentVariable::new(missing, "present".parse()?),
                            &Default::default(),
                        )
                        .await?;
                },
                "canonical_url" => {
                    CanonicalUrlsModel::new(&mut update)
                        .set_canonical_url(
                            RequestDestination::ConvexSite,
                            "https://example.com".to_owned(),
                        )
                        .await?;
                },
                _ => unreachable!(),
            }
            let updated_ts = database
                .commit_with_write_source(update, "configuration_change")
                .await?;
            assert!(ContextCache::validate_and_apply_context_read_set(&mut old, &read_set).await?);
            // A snapshot-valid hit must still conflict with a later change
            // when its saved configuration reads reach OCC/token refresh.
            assert!(database
                .refresh_token(old.into_token()?, updated_ts)
                .await?
                .is_err());
            let mut current = database.begin_system().await?;
            assert!(
                !ContextCache::validate_and_apply_context_read_set(&mut current, &read_set).await?,
                "{change}"
            );
            drop(current);
            database.shutdown().await?;
        }
        Ok(())
    })
}

#[test]
fn table_resolution_preserves_missing_and_replaced_mapping_dependencies() -> anyhow::Result<()> {
    use database::TableModel;

    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("table_resolution_dependencies", async move {
        let database = new_test_database(rt.clone()).await?;
        initialize_application_system_tables(&database).await?;
        let name = "mapping_fixture".parse()?;
        let namespace = TableNamespace::Global;
        let mut missing = database.begin_system().await?;
        assert!(missing.resolve_table_name(namespace, &name)?.is_none());
        let missing_token = missing.into_token()?;
        let mut insert = database.begin_system().await?;
        TableModel::new(&mut insert)
            .insert_table_metadata(namespace, &name)
            .await?;
        let original = insert
            .resolve_table_name(namespace, &name)?
            .context("pending table missing")?;
        let inserted_ts = database
            .commit_with_write_source(insert, "mapping_insert")
            .await?;
        assert!(database
            .refresh_token(missing_token, inserted_ts)
            .await?
            .is_err());

        let mut old = database.begin_system().await?;
        assert_eq!(old.resolve_table_name(namespace, &name)?, Some(original));
        let mut token = old.clone_for_snapshot_query();
        assert_eq!(token.resolve_table_name(namespace, &name)?, Some(original));
        let token = token.into_token()?;
        let mut replace = database.begin_system().await?;
        TableModel::new(&mut replace)
            .delete_table_by_id_bypassing_schema_enforcement(original.tablet_id)
            .await?;
        assert!(replace.resolve_table_name(namespace, &name)?.is_none());
        TableModel::new(&mut replace)
            .insert_table_metadata(namespace, &name)
            .await?;
        let replacement = replace
            .resolve_table_name(namespace, &name)?
            .context("replacement missing")?;
        assert_ne!(replacement.tablet_id, original.tablet_id);
        let replaced_ts = database
            .commit_with_write_source(replace, "mapping_replace")
            .await?;
        assert!(database.refresh_token(token, replaced_ts).await?.is_err());
        assert_eq!(old.resolve_table_name(namespace, &name)?, Some(original));
        let mut current = database.begin_system().await?;
        assert_eq!(
            current.resolve_table_name(namespace, &name)?,
            Some(replacement)
        );
        drop(old);
        drop(current);
        database.shutdown().await?;
        Ok(())
    })
}
