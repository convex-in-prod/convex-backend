use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    num::NonZeroU64,
    sync::Arc,
    time::Duration,
};

use ::metrics::StatusTimer;
use anyhow::Context;
use common::{
    backoff::Backoff,
    bootstrap_model::schema::SchemaState,
    errors::report_error,
    persistence::LatestDocument,
    runtime::Runtime,
    schemas::{
        DatabaseSchema,
        TableValidationOutcome,
    },
    types::{
        IndexRef,
        RepeatableTimestamp,
    },
    virtual_system_mapping::VirtualSystemMapping,
};
use database::{
    Database,
    IndexModel,
    SchemaModel,
    SchemaValidationModel,
    SchemasTable,
    Snapshot,
    TableShape,
    TableShapes,
    Token,
    Transaction,
    ValidationAttemptUpdate,
    SCHEMAS_TABLE,
    SCHEMA_VALIDATIONS_TABLE,
};
use errors::ErrorMetadataAnyhowExt;
use futures::{
    future::{
        select,
        Either,
    },
    pin_mut,
    Future,
    FutureExt,
    TryStreamExt,
};
use keybroker::Identity;
use metrics::{
    log_document_bytes,
    log_document_validated,
    log_walk_ts_lag,
    schema_validation_timer,
};
use shape_inference::{
    CountedShape,
    ProdConfig,
};
use usage_tracking::FunctionUsageTracker;
use value::{
    NamespacedTableMapping,
    ResolvedDocumentId,
    TableName,
    TableNamespace,
    TabletId,
};

use crate::metrics::log_worker_starting;

mod metrics;
#[cfg(test)]
mod tests;

const INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const MAX_OCC_FAILURES: u32 = 3;
const MAX_INACTIVE_VALIDATIONS_PER_TRANSACTION: usize = 32;

async fn exact_schema_is_pending<RT: Runtime>(
    tx: &mut Transaction<RT>,
    namespace: TableNamespace,
    schema_id: ResolvedDocumentId,
) -> anyhow::Result<bool> {
    if !tx
        .table_mapping()
        .namespace(namespace)
        .name_exists(&SCHEMAS_TABLE)
    {
        return Ok(false);
    }
    Ok(tx
        .get_system::<SchemasTable>(namespace, schema_id.developer_id)
        .await?
        .is_some_and(|schema| schema.id() == schema_id && schema.state == SchemaState::Pending))
}

pub struct SchemaWorker<RT: Runtime> {
    runtime: RT,
    database: Database<RT>,
}

pub struct PendingSchemaValidation {
    namespace: TableNamespace,
    id: ResolvedDocumentId,
    timer: StatusTimer,
    table_mapping: NamespacedTableMapping,
    virtual_system_mapping: VirtualSystemMapping,
    db_schema: Arc<DatabaseSchema>,
    ts: RepeatableTimestamp,
    active_schema: Option<Arc<DatabaseSchema>>,
    by_id_indexes: BTreeMap<TabletId, IndexRef>,
}

pub struct SchemaValidationResult {
    pub token: Token,
    /// When no schema is pending, also wake when an attempt needs cleanup.
    pub cleanup_token: Option<Token>,
    /// For each pending schema that was validated, the tables whose documents
    /// were walked. Tables whose shape are a subset of the schema should not be
    /// walked.
    pub walked_tables: BTreeMap<TableNamespace, BTreeSet<TableName>>,
}

impl<RT: Runtime> SchemaWorker<RT> {
    pub fn start(runtime: RT, database: Database<RT>) -> impl Future<Output = ()> + Send {
        let worker = Self { runtime, database };
        async move {
            tracing::info!("Starting SchemaWorker");
            let mut backoff = Backoff::new(INITIAL_BACKOFF, MAX_BACKOFF);
            loop {
                let result: anyhow::Result<()> = async {
                    let SchemaValidationResult {
                        token,
                        cleanup_token,
                        walked_tables,
                    } = Box::pin(worker.run()).await?;
                    let num_walked: usize = walked_tables.values().map(|tables| tables.len()).sum();
                    if !walked_tables.is_empty() {
                        tracing::info!(
                            "SchemaWorker validated {} pending schema(s), walking {num_walked} \
                             table(s)",
                            walked_tables.len()
                        );
                    }
                    if let Some(cleanup_token) = cleanup_token {
                        let schema_invalidation =
                            worker.database.subscribe_and_wait_for_invalidation(token);
                        let attempt_invalidation = worker
                            .database
                            .subscribe_and_wait_for_invalidation(cleanup_token);
                        pin_mut!(schema_invalidation, attempt_invalidation);
                        match select(schema_invalidation, attempt_invalidation).await {
                            Either::Left((result, _)) | Either::Right((result, _)) => result?,
                        };
                    } else {
                        worker
                            .database
                            .subscribe_and_wait_for_invalidation(token)
                            .await?;
                    }
                    Ok(())
                }
                .await;
                if let Err(e) = result {
                    let delay = backoff.fail(&mut worker.runtime.rng());
                    report_error(&mut e.context("SchemaWorker died")).await;
                    tracing::error!("Schema worker failed, sleeping {delay:?}");
                    worker.runtime.wait(delay).await;
                } else {
                    backoff.reset();
                }
            }
        }
    }

    pub(crate) async fn pending_schema_validations(
        tx: &mut Transaction<RT>,
    ) -> anyhow::Result<Vec<PendingSchemaValidation>> {
        let mut pending_schema_work = Vec::new();
        let namespaces: Vec<_> = tx.table_mapping().namespaces_for_name(&SCHEMAS_TABLE);
        for namespace in namespaces {
            if let Some((id, db_schema)) = SchemaModel::new(tx, namespace)
                .get_by_state(SchemaState::Pending)
                .await?
            {
                tracing::debug!("SchemaWorker found a pending schema and is validating it...");
                let timer = schema_validation_timer();
                let table_mapping = tx.table_mapping().namespace(namespace);
                let virtual_system_mapping = tx.virtual_system_mapping().clone();

                let active_schema = SchemaModel::new(tx, namespace)
                    .get_by_state(SchemaState::Active)
                    .await?
                    .map(|(_id, active_schema)| active_schema);
                let ts = tx.begin_timestamp();
                let by_id_indexes = IndexModel::new(tx).by_id_indexes().await?;
                pending_schema_work.push(PendingSchemaValidation {
                    namespace,
                    id,
                    timer,
                    table_mapping,
                    virtual_system_mapping,
                    db_schema,
                    ts,
                    active_schema,
                    by_id_indexes,
                });
            }
        }
        Ok(pending_schema_work)
    }

    pub async fn run(&self) -> anyhow::Result<SchemaValidationResult> {
        let status = log_worker_starting("SchemaWorker");
        let mut tx: Transaction<RT> = self.database.begin(Identity::system()).await?;
        let ts = tx.begin_timestamp();
        let pending_validations = SchemaWorker::pending_schema_validations(&mut tx).await?;
        let token = tx.into_token()?;
        let cleanup_token = self.delete_inactive_schema_validations().await?;

        let mut walked_tables = BTreeMap::new();
        if pending_validations.is_empty() {
            drop(status);
            tracing::debug!("SchemaWorker waiting...");
            return Ok(SchemaValidationResult {
                token,
                cleanup_token: Some(cleanup_token),
                walked_tables,
            });
        }
        let snapshot = self.database.snapshot(ts)?;
        let table_shapes = self.database.table_shapes_at(ts).await?;

        for pending_validation in pending_validations {
            let outcomes = DatabaseSchema::table_validation_outcomes(
                &pending_validation.db_schema,
                pending_validation.active_schema.as_deref(),
                &pending_validation.table_mapping,
                &pending_validation.virtual_system_mapping,
                &table_shape_provider(
                    &table_shapes,
                    &pending_validation.table_mapping,
                    pending_validation.ts,
                ),
            )?;
            tracing::info!(
                "SchemaWorker: table validation outcomes for {:?}: {:?}",
                pending_validation.namespace,
                outcomes,
            );
            let per_table_totals = outcomes
                .iter()
                .filter(|(_, outcome)| matches!(outcome, TableValidationOutcome::MustWalk))
                .map(|(table_name, _)| {
                    let total =
                        count_total_docs(&snapshot, table_name, pending_validation.namespace)?;
                    Ok(((*table_name).clone(), total))
                })
                .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
            walked_tables.insert(
                pending_validation.namespace,
                per_table_totals.keys().cloned().collect(),
            );
            self.validate_tables(pending_validation, per_table_totals)
                .await?;
        }

        drop(status);
        tracing::debug!("SchemaWorker waiting...");
        Ok(SchemaValidationResult {
            token,
            cleanup_token: None,
            walked_tables,
        })
    }

    async fn delete_inactive_schema_validations(&self) -> anyhow::Result<Token> {
        // Discover under a read-only token, then recheck each ID in a separate
        // bounded write transaction so cleanup does not carry a table-range read.
        let (inactive, token) = {
            let mut tx = self.database.begin_system().await?;
            let mut inactive = BTreeMap::new();
            let mut remaining = MAX_INACTIVE_VALIDATIONS_PER_TRANSACTION;
            let namespaces: Vec<_> = tx
                .table_mapping()
                .namespaces_for_name(&SCHEMA_VALIDATIONS_TABLE);
            for namespace in namespaces {
                if remaining == 0 {
                    break;
                }
                let mapping = tx.table_mapping().namespace(namespace);
                if !mapping.name_exists(&SCHEMA_VALIDATIONS_TABLE)
                    || !mapping.name_exists(&SCHEMAS_TABLE)
                {
                    continue;
                }
                let attempt_ids = SchemaValidationModel::new(&mut tx, namespace)
                    .inactive_attempt_ids(remaining)
                    .await?;
                remaining -= attempt_ids.len();
                if !attempt_ids.is_empty() {
                    inactive.insert(namespace, attempt_ids);
                }
            }
            let token = tx.into_token()?;
            (inactive, token)
        };
        for (namespace, attempt_ids) in inactive {
            let mut tx = self.database.begin_system().await?;
            let deleted = SchemaValidationModel::new(&mut tx, namespace)
                .delete_inactive_attempts(&attempt_ids)
                .await?;
            if deleted > 0 {
                self.database
                    .commit_with_write_source(tx, "schema_validation_attempt_cleanup")
                    .await?;
            }
        }
        Ok(token)
    }

    /// Validate tables by walking them at fresh timestamps rather than
    /// reconstructing the snapshot at the schema's pending timestamp.
    ///
    /// Soundness: every document write while a schema is Pending (or
    /// Validated) is checked against it in the writing transaction
    /// (`SchemaModel::enforce_with_table_mapping`), and a violation marks the
    /// schema Failed. So a document modified after the schema became pending
    /// is already covered, and a document unchanged since then looks the same
    /// at any timestamp in between — including each page's fresh timestamp.
    /// Deletes need no validation. The OCC read on `_schemas.by_state` taken
    /// by every write serializes those mark-Failed transitions with
    /// `mark_validated` below.
    ///
    /// This avoids reconstructing the pending-timestamp snapshot in
    /// `stream_documents_in_table`, which re-walks the instance's document
    /// log once per page and grows with concurrent write traffic.
    async fn validate_tables(
        &self,
        pending_validation: PendingSchemaValidation,
        // The tables to walk, with each table's approximate document count.
        per_table_totals: BTreeMap<TableName, Option<u64>>,
    ) -> anyhow::Result<()> {
        let PendingSchemaValidation {
            namespace,
            id,
            timer,
            table_mapping,
            virtual_system_mapping,
            db_schema,
            ts,
            active_schema: _,
            by_id_indexes,
        } = pending_validation;

        let tablet_ids = per_table_totals
            .keys()
            .map(|table_name| table_mapping.name_to_tablet()(table_name.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let Some(mut schema_validation_progress_tracker) = SchemaValidationProgressTracker::new(
            self.database.clone(),
            namespace,
            id,
            per_table_totals,
        )
        .await?
        else {
            timer.finish_with("canceled");
            return Ok(());
        };
        let mut last_page_ts = ts;
        'tables: for tablet_id in tablet_ids {
            let by_id = *by_id_indexes.get(&tablet_id).ok_or_else(|| {
                anyhow::anyhow!("Failed to find id index for table id {tablet_id}")
            })?;
            let stream = self
                .database
                .table_iterator(ts, 1000)
                .stream_latest_documents_in_table(tablet_id, by_id);
            pin_mut!(stream);
            // The walk observes *current* documents, so it must validate with
            // current table mappings (a snapshot import can replace a table
            // mid-walk). Refresh once per page.
            let mut current_page_ts = None;
            let mut fresh_mapping = table_mapping.clone();
            let mut table_name = fresh_mapping.tablet_name(tablet_id)?;
            // Progress rows are keyed by the table's name when the walk
            // started; a mid-walk rename must keep writing to the same row.
            let row_table_name = table_name.clone();
            while let Some((LatestDocument { value: doc, .. }, page_ts)) = stream.try_next().await?
            {
                if current_page_ts != Some(page_ts) {
                    current_page_ts = Some(page_ts);
                    last_page_ts = page_ts;
                    let snapshot = self.database.latest_snapshot()?;
                    fresh_mapping = snapshot.table_mapping().namespace(namespace);
                    match fresh_mapping.tablet_name(tablet_id) {
                        Ok(name) => table_name = name,
                        Err(_) => {
                            // The table was deleted or replaced mid-walk. Its
                            // documents no longer exist at current timestamps,
                            // and a replacement table's documents were
                            // validated at insert time.
                            let progress_exists = schema_validation_progress_tracker
                                .record_table_finished(&row_table_name)
                                .await?;
                            if !progress_exists {
                                // Validation was canceled by a newer push.
                                timer.finish_with("canceled");
                                return Ok(());
                            }
                            continue 'tables;
                        },
                    }
                }
                log_document_validated();
                log_document_bytes(doc.size());
                if let Err(schema_error) = db_schema.check_existing_document(
                    &doc,
                    table_name.clone(),
                    &fresh_mapping,
                    &virtual_system_mapping,
                ) {
                    let (_, schema_is_failed, _) = self
                        .database
                        .execute_with_occ_retries(
                            Identity::system(),
                            FunctionUsageTracker::new(),
                            MAX_OCC_FAILURES,
                            "schema_worker_mark_failed",
                            |tx| {
                                let schema_error = schema_error.clone();
                                async move {
                                    SchemaModel::new(tx, namespace)
                                        .mark_failed(id, schema_error)
                                        .await
                                }
                                .boxed()
                                .into()
                            },
                        )
                        .await?;

                    if !schema_is_failed {
                        timer.finish_with("canceled");
                        return Ok(());
                    }

                    tracing::info!("Schema is invalid");
                    timer.finish_developer_error();
                    return Ok(());
                }
                // Return early if progress does not exist - this means the
                // schema validation has been canceled either by a document
                // update that does not match the pending schema or by the
                // submission of a new pending schema.
                let progress_exists = schema_validation_progress_tracker
                    .record_document_validated(&row_table_name)
                    .await?;
                if !progress_exists {
                    timer.finish_with("canceled");
                    return Ok(());
                }
            }
            let progress_exists = schema_validation_progress_tracker
                .record_table_finished(&row_table_name)
                .await?;
            if !progress_exists {
                timer.finish_with("canceled");
                return Ok(());
            }
        }
        log_walk_ts_lag(Duration::from_nanos(
            (i64::from(*last_page_ts) - i64::from(*ts)).max(0) as u64,
        ));
        let mut tx = self.database.begin(Identity::system()).await?;
        if !exact_schema_is_pending(&mut tx, namespace, id).await? {
            timer.finish_with("canceled");
            return Ok(());
        }
        if let Err(error) = SchemaModel::new(&mut tx, namespace)
            .mark_validated(id)
            .await
        {
            if error.is_bad_request() {
                timer.finish_developer_error();
            }
            tracing::info!("Schema not marked valid");
            return Err(error);
        }
        if let Err(error) = self
            .database
            .commit_with_write_source(tx, "schema_worker_mark_valid")
            .await
        {
            if error.is_occ() {
                timer.finish_with("canceled");
                return Ok(());
            }
            return Err(error);
        }
        tracing::info!("Schema is valid");
        timer.finish();
        Ok(())
    }
}

/// Tracks per-table progress of schema validation for the tables that need to
/// be validated, periodically writing progress to that table's row in the
/// `_schema_validation_progress` table for the given namespace and schema.
struct SchemaValidationProgressTracker<RT: Runtime> {
    database: Database<RT>,
    namespace: TableNamespace,
    /// Checkpoints must stop after this exact pending schema changes state.
    schema_id: ResolvedDocumentId,
    tables: BTreeMap<TableName, TableProgress>,
}

struct TableProgress {
    validation_id: ResolvedDocumentId,
    /// The threshold at which to write validation progress to the database.
    update_threshold: NonZeroU64,
    /// The number of documents that have been validated since writing progress
    /// to the database.
    docs_validated: u64,
}

impl<RT: Runtime> SchemaValidationProgressTracker<RT> {
    pub async fn new(
        database: Database<RT>,
        namespace: TableNamespace,
        schema_id: ResolvedDocumentId,
        per_table_totals: BTreeMap<TableName, Option<u64>>,
    ) -> anyhow::Result<Option<Self>> {
        let mut tx = database.begin(Identity::system()).await?;
        if !exact_schema_is_pending(&mut tx, namespace, schema_id).await? {
            return Ok(None);
        }
        let mut model = SchemaValidationModel::new(&mut tx, namespace);
        let mut tables = BTreeMap::new();
        for (table_name, total_docs) in per_table_totals {
            let validation_id = model
                .start_table_validation(schema_id, table_name.clone(), None, total_docs)
                .await?;
            tables.insert(
                table_name,
                TableProgress {
                    validation_id,
                    update_threshold: progress_update_threshold(total_docs),
                    docs_validated: 0,
                },
            );
        }
        database
            .commit_with_write_source(tx, "schema_validation_tracker_initialized")
            .await?;
        Ok(Some(Self {
            database,
            namespace,
            schema_id,
            tables,
        }))
    }

    fn total_docs_at_ts(
        &self,
        table_name: &TableName,
        ts: RepeatableTimestamp,
    ) -> anyhow::Result<Option<u64>> {
        let snapshot = self.database.snapshot(ts)?;
        count_total_docs(&snapshot, table_name, self.namespace)
    }

    fn table(&mut self, table_name: &TableName) -> anyhow::Result<&mut TableProgress> {
        self.tables
            .get_mut(table_name)
            .with_context(|| format!("progress tracker missing table {table_name}"))
    }

    /// Flush the table's in-memory count into its progress document. Returns
    /// false if the document is gone, meaning validation was canceled.
    async fn update_validation_progress(&mut self, table_name: &TableName) -> anyhow::Result<bool> {
        let validation_id = self.table(table_name)?.validation_id;
        let docs_validated = self.table(table_name)?.docs_validated;
        let mut tx = self.database.begin_system().await?;
        if !exact_schema_is_pending(&mut tx, self.namespace, self.schema_id).await? {
            return Ok(false);
        }
        let total_docs = self.total_docs_at_ts(table_name, tx.begin_timestamp())?;
        let mut model = SchemaValidationModel::new(&mut tx, self.namespace);
        let progress_exists = model
            .update_attempt(
                validation_id,
                ValidationAttemptUpdate::RecordProgress {
                    additional_docs_validated: docs_validated,
                    total_docs,
                },
            )
            .await?;
        self.database
            .commit_with_write_source(tx, "schema_validation_progress_updated")
            .await?;
        self.table(table_name)?.docs_validated = 0;
        Ok(progress_exists)
    }

    /// Records that one of the table's documents has been validated, writing
    /// to the db iff we have hit the update threshold, otherwise tracking
    /// progress in memory.
    async fn record_document_validated(&mut self, table_name: &TableName) -> anyhow::Result<bool> {
        let table = self.table(table_name)?;
        table.docs_validated += 1;
        if table.docs_validated % table.update_threshold != 0 {
            return Ok(true);
        }
        tracing::debug!(
            "Updating schema validation progress for {table_name} with docs_validated: {}",
            table.docs_validated,
        );
        self.update_validation_progress(table_name).await
    }

    /// Flushes the table's remaining progress and marks its row `Valid` once
    /// its walk completes. Returns false if validation was canceled.
    async fn record_table_finished(&mut self, table_name: &TableName) -> anyhow::Result<bool> {
        if !self.update_validation_progress(table_name).await? {
            return Ok(false);
        }
        let mut tx = self.database.begin_system().await?;
        if !exact_schema_is_pending(&mut tx, self.namespace, self.schema_id).await? {
            return Ok(false);
        }
        let mut model = SchemaValidationModel::new(&mut tx, self.namespace);
        let marked = model
            .update_attempt(
                self.table(table_name)?.validation_id,
                ValidationAttemptUpdate::MarkValid,
            )
            .await?;
        self.database
            .commit_with_write_source(tx, "schema_validation_progress_finished")
            .await?;
        Ok(marked)
    }
}

/// Flush progress to the table's row every 5% of the table or 500 documents,
/// whichever is smaller, so progress stays fresh without slowing validation
/// down with writes.
fn progress_update_threshold(total_docs: Option<u64>) -> NonZeroU64 {
    NonZeroU64::new(
        total_docs
            .map(|total| std::cmp::min(500, (total as f64 * 0.05).ceil() as u64))
            .unwrap_or(500),
    )
    .unwrap_or(NonZeroU64::MIN)
}

/// Shape provider for [`DatabaseSchema::tables_to_validate`] and
/// [`DatabaseSchema::table_validation_outcomes`]: a table whose shape at the
/// given timestamp is already a subset of the schema being validated can skip
/// the document walk. Returning `None` means "shape unavailable" and the table
/// gets walked. `table_shapes` must be caught up to exactly `ts`, the
/// timestamp `table_mapping` is from.
pub(crate) fn table_shape_provider<'a>(
    table_shapes: &'a Option<Arc<TableShapes>>,
    table_mapping: &'a NamespacedTableMapping,
    ts: RepeatableTimestamp,
) -> impl Fn(&TableName) -> anyhow::Result<Option<CountedShape<ProdConfig>>> + 'a {
    move |table_name| {
        let Some(table_shapes) = table_shapes.as_ref() else {
            return Ok(None);
        };
        let Ok(table_id) = table_mapping.id(table_name) else {
            // Nonexistent tables have no documents to validate, so an
            // empty shape lets them skip validation.
            return Ok(Some(TableShape::empty().inferred_type().clone()));
        };
        // Every tablet in the table mapping must have a shape because the
        // shapes are caught up to exactly the mapping's timestamp.
        let shape = table_shapes
            .tablet_shape(&table_id.tablet_id)
            .with_context(|| {
                format!(
                    "table {table_name} (tablet {}) is in the table mapping at ts {} but has no \
                     shape in the table shapes at ts {}",
                    table_id.tablet_id, *ts, table_shapes.ts,
                )
            })?;
        Ok(Some(shape.inferred_type().clone()))
    }
}

/// Number of documents in the table at the snapshot, or `None` if table counts
/// haven't been bootstrapped yet.
fn count_total_docs(
    snapshot: &Snapshot,
    table_name: &TableName,
    namespace: TableNamespace,
) -> anyhow::Result<Option<u64>> {
    if snapshot.table_counts.is_none() {
        return Ok(None);
    }
    let total_docs = snapshot
        .table_count(namespace, table_name)
        .context("Failed to retrieve table count when table counts were present")?
        .num_values();
    Ok(Some(total_docs))
}
