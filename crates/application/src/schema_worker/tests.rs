use std::{
    future::Future,
    sync::Arc,
};

use anyhow::Context as _;
use common::{
    bootstrap_model::schema::{
        SchemaMetadata,
        SchemaState,
    },
    db_schema,
    object_validator,
    persistence::Persistence,
    runtime::new_unlimited_rate_limiter,
    schemas::{
        validator::{
            FieldValidator,
            Validator,
        },
        DatabaseSchema,
        DocumentSchema,
    },
    shutdown::ShutdownSignal,
};
use database::{
    Database,
    SchemaModel,
    SchemaValidationModel,
    UserFacingModel,
    ValidationAttemptUpdate,
};
use errors::ErrorMetadataAnyhowExt;
use indexing::index_cache::IndexCache;
use keybroker::Identity;
use runtime::prod::ProdRuntime;
use search::searcher::SearcherStub;
use sqlite::SqlitePersistence;
use value::{
    ConvexObject,
    ResolvedDocumentId,
    TableName,
    TableNamespace,
};

use super::SchemaWorker;

const TEST_NAMESPACE: TableNamespace = TableNamespace::root_component();

fn run_schema_test<F, Fut>(name: &'static str, test: F) -> anyhow::Result<()>
where
    F: FnOnce(ProdRuntime) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let tokio = ProdRuntime::init_tokio()?;
    let runtime = ProdRuntime::new(&tokio);
    runtime.block_on(name, test(runtime.clone()))
}

async fn new_test_database(runtime: ProdRuntime) -> anyhow::Result<Database<ProdRuntime>> {
    let persistence: Arc<dyn Persistence> = Arc::new(SqlitePersistence::new(":memory:")?);
    let (deleted_tablet_sender, _deleted_tablet_receiver) = tokio::sync::mpsc::channel(16);
    let database = Database::load(
        persistence,
        runtime.clone(),
        Arc::new(SearcherStub),
        ShutdownSignal::panic(),
        model::virtual_system_mapping().clone(),
        IndexCache::new(1 << 20).new_handle(),
        Arc::new(new_unlimited_rate_limiter(runtime)),
        deleted_tablet_sender,
        "schema_worker_tests".to_owned(),
    )
    .await?;
    model::initialize_application_system_tables(&database).await?;
    Ok(database)
}

async fn submit_active_and_pending_schema(
    database: &Database<ProdRuntime>,
    table_name: &TableName,
) -> anyhow::Result<ResolvedDocumentId> {
    let mut tx = database.begin_system().await?;
    let (active_id, active_state) = SchemaModel::new(&mut tx, TEST_NAMESPACE)
        .submit_pending(db_schema!(table_name => DocumentSchema::Any))
        .await?;
    assert_eq!(active_state, SchemaState::Pending);
    SchemaModel::new(&mut tx, TEST_NAMESPACE)
        .mark_validated(active_id)
        .await?;
    SchemaModel::new(&mut tx, TEST_NAMESPACE)
        .mark_active(active_id)
        .await?;
    let pending_schema: DatabaseSchema = db_schema!(
        table_name => DocumentSchema::Union(vec![object_validator!(
            "required" => FieldValidator::required_field_type(Validator::Int64)
        )]),
    );
    let (pending_id, pending_state) = SchemaModel::new(&mut tx, TEST_NAMESPACE)
        .submit_pending(pending_schema)
        .await?;
    assert_eq!(pending_state, SchemaState::Pending);
    database
        .commit_with_write_source(tx, "test_schema_setup")
        .await?;
    Ok(pending_id)
}

async fn pending_schema_with_attempt(
    database: &Database<ProdRuntime>,
    table_name: &TableName,
) -> anyhow::Result<(ResolvedDocumentId, ResolvedDocumentId)> {
    let schema_id = submit_active_and_pending_schema(database, table_name).await?;
    let mut tx = database.begin_system().await?;
    let validation_id = SchemaValidationModel::new(&mut tx, TEST_NAMESPACE)
        .start_table_validation(schema_id, table_name.clone(), None, Some(10))
        .await?;
    database
        .commit_with_write_source(tx, "test_validation_setup")
        .await?;
    Ok((schema_id, validation_id))
}

#[test]
fn schema_failure_write_commits_after_progress_checkpoint() -> anyhow::Result<()> {
    run_schema_test(
        "schema_failure_write_commits_after_progress_checkpoint",
        |runtime| async move {
            let database = new_test_database(runtime.clone()).await?;
            let table_name: TableName = "documents".parse()?;
            let (schema_id, validation_id) =
                pending_schema_with_attempt(&database, &table_name).await?;

            // The application transaction stages its schema failure first.
            let mut application_tx = database.begin(Identity::Unknown(None)).await?;
            let document_id = UserFacingModel::new(&mut application_tx, TEST_NAMESPACE)
                .insert(table_name.clone(), ConvexObject::empty())
                .await?;
            let local_schema = application_tx
                .get(schema_id)
                .await?
                .context("pending schema disappeared in application transaction")?;
            let local_schema = SchemaMetadata::try_from(local_schema.into_value().into_value())?;
            assert!(matches!(local_schema.state, SchemaState::Failed { .. }));

            // A worker checkpoint lands before that application transaction commits.
            let worker = SchemaWorker {
                runtime: runtime.clone(),
                database: database.clone(),
            };
            assert!(
                worker
                    .commit_progress_write(
                        TEST_NAMESPACE,
                        validation_id,
                        ValidationAttemptUpdate::RecordProgress {
                            additional_docs_validated: 1,
                            total_docs: Some(10)
                        },
                        "test_progress_checkpoint",
                    )
                    .await?
            );
            database
                .commit_with_write_source(application_tx, "test_schema_failing_write")
                .await?;

            let mut verify_tx = database.begin_system().await?;
            let schema = verify_tx
                .get(schema_id)
                .await?
                .context("failed schema disappeared before verification")?;
            let schema = SchemaMetadata::try_from(schema.into_value().into_value())?;
            assert!(matches!(schema.state, SchemaState::Failed { .. }));
            let resolved_document_id =
                verify_tx.resolve_developer_id(&document_id, TEST_NAMESPACE)?;
            assert!(verify_tx.get(resolved_document_id).await?.is_some());
            assert_eq!(
                SchemaValidationModel::new(&mut verify_tx, TEST_NAMESPACE)
                    .progress(validation_id)
                    .await?
                    .num_docs_validated,
                1
            );
            drop(verify_tx);

            // Failure leaves cleanup to the worker, after the application write.
            let worker = SchemaWorker {
                runtime,
                database: database.clone(),
            };
            worker.delete_inactive_schema_validations().await?;
            let mut verify_tx = database.begin_system().await?;
            assert!(SchemaValidationModel::new(&mut verify_tx, TEST_NAMESPACE)
                .validations_for_schema(schema_id)
                .await?
                .is_empty());
            Ok(())
        },
    )
}

#[test]
fn stale_progress_checkpoint_loses_to_schema_failure() -> anyhow::Result<()> {
    run_schema_test(
        "stale_progress_checkpoint_loses_to_schema_failure",
        |runtime| async move {
            let database = new_test_database(runtime).await?;
            let table_name: TableName = "documents".parse()?;
            let (schema_id, validation_id) =
                pending_schema_with_attempt(&database, &table_name).await?;

            let mut stale_checkpoint = database.begin_system().await?;
            assert!(
                SchemaValidationModel::new(&mut stale_checkpoint, TEST_NAMESPACE)
                    .update_attempt(
                        validation_id,
                        ValidationAttemptUpdate::RecordProgress {
                            additional_docs_validated: 1,
                            total_docs: Some(10),
                        },
                    )
                    .await?
            );

            let mut application_tx = database.begin(Identity::Unknown(None)).await?;
            let document_id = UserFacingModel::new(&mut application_tx, TEST_NAMESPACE)
                .insert(table_name, ConvexObject::empty())
                .await?;
            database
                .commit_with_write_source(application_tx, "test_schema_failing_write")
                .await?;

            let error = database
                .commit_with_write_source(stale_checkpoint, "test_stale_schema_checkpoint")
                .await
                .expect_err("a checkpoint based on Pending must lose to Failed");
            assert!(error.is_occ(), "expected OCC, got {error:#}");

            let mut verify_tx = database.begin_system().await?;
            let schema = verify_tx
                .get(schema_id)
                .await?
                .context("failed schema disappeared before verification")?;
            let schema = SchemaMetadata::try_from(schema.into_value().into_value())?;
            assert!(matches!(schema.state, SchemaState::Failed { .. }));
            let resolved_document_id =
                verify_tx.resolve_developer_id(&document_id, TEST_NAMESPACE)?;
            assert!(verify_tx.get(resolved_document_id).await?.is_some());
            assert_eq!(
                SchemaValidationModel::new(&mut verify_tx, TEST_NAMESPACE)
                    .progress(validation_id)
                    .await?
                    .num_docs_validated,
                0
            );
            Ok(())
        },
    )
}

#[test]
fn staged_checkpoint_tracks_schema_ownership() -> anyhow::Result<()> {
    run_schema_test(
        "staged_checkpoint_tracks_schema_ownership",
        |runtime| async move {
            for activate in [false, true] {
                let database = new_test_database(runtime.clone()).await?;
                let table_name: TableName = "documents".parse()?;
                let mut schema = db_schema!(&table_name => DocumentSchema::Any);
                schema
                    .tables
                    .get_mut(&table_name)
                    .unwrap()
                    .staged_document_type = Some(DocumentSchema::Union(vec![object_validator!(
                    "required" => FieldValidator::required_field_type(Validator::Int64)
                )]));
                let mut tx = database.begin_system().await?;
                let (schema_id, _) = SchemaModel::new(&mut tx, TEST_NAMESPACE)
                    .submit_pending(schema)
                    .await?;
                if activate {
                    SchemaModel::new(&mut tx, TEST_NAMESPACE)
                        .mark_validated(schema_id)
                        .await?;
                    SchemaModel::new(&mut tx, TEST_NAMESPACE)
                        .mark_active(schema_id)
                        .await?;
                }
                let attempt_id = SchemaValidationModel::new(&mut tx, TEST_NAMESPACE)
                    .validation_metadata_for_table(schema_id, &table_name)
                    .await?
                    .unwrap()
                    .id();
                database
                    .commit_with_write_source(tx, "test_staged_setup")
                    .await?;

                let worker = SchemaWorker {
                    runtime: runtime.clone(),
                    database: database.clone(),
                };
                let checkpoint = ValidationAttemptUpdate::RecordProgress {
                    additional_docs_validated: 1,
                    total_docs: Some(10),
                };
                // An active schema still owns its staged walk; enforced-only guards
                // would cancel this work and leave staged validation stuck.
                assert!(
                    worker
                        .commit_progress_write(
                            TEST_NAMESPACE,
                            attempt_id,
                            checkpoint.clone(),
                            "test_active_staged_checkpoint"
                        )
                        .await?
                );
                let mut stale_checkpoint = database.begin_system().await?;
                assert!(
                    SchemaValidationModel::new(&mut stale_checkpoint, TEST_NAMESPACE)
                        .update_attempt(attempt_id, checkpoint.clone())
                        .await?
                );

                let mut replacement = database.begin_system().await?;
                let (next_id, _) = SchemaModel::new(&mut replacement, TEST_NAMESPACE)
                    .submit_pending(db_schema!(&table_name => DocumentSchema::Any))
                    .await?;
                SchemaModel::new(&mut replacement, TEST_NAMESPACE)
                    .mark_validated(next_id)
                    .await?;
                SchemaModel::new(&mut replacement, TEST_NAMESPACE)
                    .mark_active(next_id)
                    .await?;
                database
                    .commit_with_write_source(replacement, "test_replace_staged_schema")
                    .await?;
                let error = database
                    .commit_with_write_source(stale_checkpoint, "test_stale_staged_checkpoint")
                    .await
                    .expect_err("overwriting the owner must fence a staged checkpoint");
                assert!(error.is_occ(), "expected OCC, got {error:#}");

                // Replacing a pending schema defers cleanup; active schema
                // replacement deletes attempts immediately. Both fence proofs.
                let mut tx = database.begin_system().await?;
                assert_eq!(tx.get(attempt_id).await?.is_some(), !activate);
                assert!(
                    !SchemaValidationModel::new(&mut tx, TEST_NAMESPACE)
                        .update_attempt(attempt_id, ValidationAttemptUpdate::MarkValid)
                        .await?
                );
                if !activate {
                    assert_eq!(
                        SchemaValidationModel::new(&mut tx, TEST_NAMESPACE)
                            .progress(attempt_id)
                            .await?
                            .num_docs_validated,
                        1
                    );
                }
                drop(tx);
                worker.delete_inactive_schema_validations().await?;
                let mut tx = database.begin_system().await?;
                assert!(tx.get(attempt_id).await?.is_none());
            }
            Ok(())
        },
    )
}
