//! Read set tracking for an active transaction
use std::{
    any::Any,
    collections::BTreeMap,
    mem,
    ops::Bound,
    sync::LazyLock,
};

use cmd_util::env::env_config;
use common::{
    bootstrap_model::index::database_index::IndexedFields,
    components::ComponentPath,
    document::PackedDocument,
    document_index_keys::{
        DatabaseIndexWrite,
        TextIndexWrite,
    },
    index::IndexKey,
    interval::{
        Interval,
        IntervalSet,
    },
    static_span,
    types::{
        TabletIndexName,
        Timestamp,
    },
    value::ResolvedDocumentId,
    virtual_system_mapping::VirtualSystemMapping,
};
use errors::ErrorMetadata;
use imbl::{
    OrdMap,
    OrdSet,
};
use search::QueryReads as SearchQueryReads;
use usage_tracking::FunctionUsageTracker;
use value::{
    heap_size::{
        HeapSize,
        WithHeapSize,
    },
    TableName,
    TabletId,
};

#[cfg(doc)]
use crate::Transaction;
use crate::{
    database::{
        ConflictingRead,
        ConflictingReadWithWriteSource,
    },
    execution_size::TransactionLimits,
    stack_traces::StackTrace,
    write_log::ArcWriteInIndex,
};

pub const OVER_LIMIT_HELP: &str = "Consider using smaller limits in your queries, paginating your \
                                   queries, or using indexed queries with a selective index range \
                                   expressions.";

/// If set to 'true', then collect backtraces of every database read in order
/// to help debug OCC errors. Collecting stack traces is expensive and should
/// only be used in development.
static READ_SET_CAPTURE_BACKTRACES: LazyLock<bool> =
    LazyLock::new(|| env_config("READ_SET_CAPTURE_BACKTRACES", false));

#[derive(Debug, Clone)]
pub struct IndexReads {
    pub fields: IndexedFields,
    pub intervals: IntervalSet,
    pub stack_traces: Option<Vec<(Interval, StackTrace)>>,
}

impl IndexReads {
    /// The stack traces of the reads that cover `index_key`, or `None` if
    /// backtraces aren't being collected.
    pub(crate) fn stack_traces_covering(&self, index_key: &[u8]) -> Option<Vec<StackTrace>> {
        let stack_traces = self.stack_traces.as_ref()?;
        Some(
            stack_traces
                .iter()
                .filter(|(interval, _)| interval.contains(index_key))
                .map(|(_, trace)| trace.clone())
                .collect(),
        )
    }
}

impl HeapSize for IndexReads {
    fn heap_size(&self) -> usize {
        self.fields.heap_size() + self.intervals.heap_size()
    }
}

#[derive(Debug, Clone)]
pub struct ReadSet {
    indexed: WithHeapSize<BTreeMap<TabletIndexName, IndexReads>>,
    search: WithHeapSize<BTreeMap<TabletIndexName, SearchQueryReads>>,
}

/// Data-free structural differences between two read sets for local tests.
#[cfg(feature = "testing")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadSetComparisonDiagnostic {
    pub indexed: Vec<IndexedReadComparisonDiagnostic>,
    pub primary_search_count: usize,
    pub shadow_search_count: usize,
    pub search_matches: bool,
}

#[cfg(feature = "testing")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedReadComparisonDiagnostic {
    pub index_descriptor: String,
    pub primary_present: bool,
    pub shadow_present: bool,
    pub fields_match: bool,
    pub intervals_match: bool,
    pub primary_interval_count: usize,
    pub shadow_interval_count: usize,
}

fn remove_lane_local_insert_id_intervals(
    index_name: &TabletIndexName,
    intervals: &IntervalSet,
    document_ids: impl Iterator<Item = ResolvedDocumentId>,
) -> IntervalSet {
    let lane_local_intervals = document_ids
        .filter(|document_id| index_name == &TabletIndexName::by_id(document_id.tablet_id))
        .map(|document_id| {
            Interval::prefix(IndexKey::new(vec![], document_id.into()).to_bytes().into())
        })
        .collect::<std::collections::BTreeSet<_>>();
    let mut normalized = IntervalSet::new();
    for interval in intervals.iter() {
        if !lane_local_intervals.contains(&interval) {
            normalized.add(interval);
        }
    }
    normalized
}

impl HeapSize for ReadSet {
    fn heap_size(&self) -> usize {
        self.indexed.heap_size() + self.search.heap_size()
    }
}

impl ReadSet {
    fn num_intervals(&self) -> usize {
        self.indexed
            .values()
            .map(|reads| reads.intervals.len())
            .sum()
    }

    /// Compare transaction dependencies while ignoring optional diagnostic
    /// stack traces attached to indexed reads.
    pub fn has_same_read_dependencies(&self, other: &Self) -> bool {
        self.indexed.len() == other.indexed.len()
            && self.indexed.iter().all(|(index_name, reads)| {
                other.indexed.get(index_name).is_some_and(|other_reads| {
                    reads.fields == other_reads.fields && reads.intervals == other_reads.intervals
                })
            })
            && self.search == other.search
    }

    /// Compare transaction dependencies after removing only the by-id
    /// singleton reads used to establish independently allocated insert IDs.
    pub fn has_same_read_dependencies_with_lane_local_insert_ids(
        &self,
        other: &Self,
        inserted_id_mapping: &BTreeMap<ResolvedDocumentId, ResolvedDocumentId>,
    ) -> bool {
        self.indexed.len() == other.indexed.len()
            && self.indexed.iter().all(|(index_name, reads)| {
                other.indexed.get(index_name).is_some_and(|other_reads| {
                    reads.fields == other_reads.fields
                        && remove_lane_local_insert_id_intervals(
                            index_name,
                            &reads.intervals,
                            inserted_id_mapping.keys().copied(),
                        ) == remove_lane_local_insert_id_intervals(
                            index_name,
                            &other_reads.intervals,
                            inserted_id_mapping.values().copied(),
                        )
                })
            })
            && self.search == other.search
    }

    #[cfg(feature = "testing")]
    pub fn comparison_diagnostic(&self, other: &Self) -> ReadSetComparisonDiagnostic {
        let index_names = self
            .indexed
            .keys()
            .chain(other.indexed.keys())
            .collect::<std::collections::BTreeSet<_>>();
        let indexed = index_names
            .into_iter()
            .map(|index_name| {
                let primary = self.indexed.get(index_name);
                let shadow = other.indexed.get(index_name);
                let (fields_match, intervals_match) = match (primary, shadow) {
                    (Some(primary), Some(shadow)) => (
                        primary.fields == shadow.fields,
                        primary.intervals == shadow.intervals,
                    ),
                    _ => (false, false),
                };
                IndexedReadComparisonDiagnostic {
                    index_descriptor: index_name.descriptor().to_string(),
                    primary_present: primary.is_some(),
                    shadow_present: shadow.is_some(),
                    fields_match,
                    intervals_match,
                    primary_interval_count: primary.map_or(0, |reads| reads.intervals.len()),
                    shadow_interval_count: shadow.map_or(0, |reads| reads.intervals.len()),
                }
            })
            .collect();
        ReadSetComparisonDiagnostic {
            indexed,
            primary_search_count: self.search.len(),
            shadow_search_count: other.search.len(),
            search_matches: self.search == other.search,
        }
    }

    pub fn empty() -> Self {
        Self {
            indexed: WithHeapSize::default(),
            search: WithHeapSize::default(),
        }
    }

    pub fn new(
        indexed: BTreeMap<TabletIndexName, IndexReads>,
        search: BTreeMap<TabletIndexName, SearchQueryReads>,
    ) -> Self {
        Self {
            indexed: indexed.into(),
            search: search.into(),
        }
    }

    /// Iterate over all range reads for the given index.
    pub fn iter_indexed(&self) -> impl Iterator<Item = (&TabletIndexName, &IndexReads)> {
        self.indexed.iter()
    }

    pub fn iter_search(&self) -> impl Iterator<Item = (&TabletIndexName, &SearchQueryReads)> {
        self.search.iter()
    }

    pub fn has_search_reads(&self) -> bool {
        !self.search.is_empty()
    }

    pub fn consume(
        self,
    ) -> (
        impl Iterator<Item = (TabletIndexName, IndexReads)>,
        impl Iterator<Item = (TabletIndexName, SearchQueryReads)>,
    ) {
        (self.indexed.into_iter(), self.search.into_iter())
    }

    /// Determine whether a mutation to a document overlaps with the text index
    /// reads in the read set.
    pub fn search_overlaps_document(&self, document: &PackedDocument) -> Option<ConflictingRead> {
        for (index, search_reads) in iter_indexes_for_table(&self.search, document.id().tablet_id) {
            if search_reads.overlaps_document(document) {
                return Some(ConflictingRead {
                    index: index.clone(),
                    id: document.id(),
                    stack_traces: None,
                });
            }
        }
        None
    }

    /// Check whether any writes in the given index maps in the timestamp
    /// range `(from, to]` conflict with this read set. Only looks up indexes
    /// that were read.
    #[fastrace::trace]
    pub(crate) fn writes_overlap_by_index(
        &self,
        by_database_index: &OrdMap<TabletIndexName, OrdSet<ArcWriteInIndex<DatabaseIndexWrite>>>,
        by_search_index: &OrdMap<TabletIndexName, OrdSet<ArcWriteInIndex<TextIndexWrite>>>,
        from: Timestamp,
        to: Timestamp,
    ) -> Option<ConflictingReadWithWriteSource> {
        // Check database index reads
        for (index, index_reads) in self.indexed.iter() {
            let Some(updates) = by_database_index.get(index) else {
                continue;
            };
            for write in updates.range((Bound::Excluded(from), Bound::Included(to))) {
                for update in &write.index_updates {
                    for index_key in update.update.iter() {
                        if index_reads.intervals.contains(index_key) {
                            return Some(ConflictingReadWithWriteSource {
                                read: ConflictingRead {
                                    index: index.clone(),
                                    id: update.document_id,
                                    stack_traces: index_reads.stack_traces_covering(index_key),
                                },
                                write_source: write.write_source.clone(),
                                write_ts: write.ts,
                            });
                        }
                    }
                }
            }
        }
        // Check search index reads
        for (index, search_reads) in self.search.iter() {
            let Some(updates) = by_search_index.get(index) else {
                continue;
            };
            for write in updates.range((Bound::Excluded(from), Bound::Included(to))) {
                for update in &write.index_updates {
                    for value in update.update.iter() {
                        if search_reads.overlaps_search_index_key_value(value) {
                            return Some(ConflictingReadWithWriteSource {
                                read: ConflictingRead {
                                    index: index.clone(),
                                    id: update.document_id,
                                    stack_traces: None,
                                },
                                write_source: write.write_source.clone(),
                                write_ts: write.ts,
                            });
                        }
                    }
                }
            }
        }
        None
    }
}

/// Iterates just those pairs in `map` whose table matches `tablet_id`
fn iter_indexes_for_table<T>(
    map: &BTreeMap<TabletIndexName, T>,
    tablet_id: TabletId,
) -> impl Iterator<Item = (&TabletIndexName, &T)> {
    // uses the fact that TabletIndexName is ordered by TabletId first,
    // then descriptor
    map.range(TabletIndexName::min_for_table(tablet_id)..)
        .take_while(move |(index, _)| *index.table() == tablet_id)
}

/// Tracks the read set for the current transaction. Records successful reads as
/// well as missing documents so we can ensure future reads in this transaction
/// are consistent against the current snapshot.
///
/// [`Transaction`] keeps this read set up to date when accessing documents
/// or the index. We want to minimize the amount of code that updates this state
/// so we avoid missing an update.
#[derive(Debug, Clone)]
pub struct TransactionReadSet {
    read_set: ReadSet,

    // During handler snooping, this tracks the complete read set for limit
    // accounting while `read_set` contains only the handler segment.
    limit_read_set: Option<Box<ReadSet>>,

    // Pre-computed sum of all of the `IntervalSet`'s sizes.
    num_intervals: usize,

    user_tx_size: TransactionReadSize,
    system_tx_size: TransactionReadSize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, derive_more::Add, derive_more::AddAssign)]
pub struct TransactionReadSize {
    // Sum of doc.size() for all documents read.
    pub total_document_size: usize,
    // Count of all documents read.
    pub total_document_count: usize,
}

impl TransactionReadSize {
    fn checked_sub(&self, baseline: &Self) -> Self {
        Self {
            total_document_size: self
                .total_document_size
                .checked_sub(baseline.total_document_size)
                .expect("handler read accounting must include its baseline"),
            total_document_count: self
                .total_document_count
                .checked_sub(baseline.total_document_count)
                .expect("handler read accounting must include its baseline"),
        }
    }
}

impl TransactionReadSet {
    /// Create a read-set at the given timestamp.
    pub fn new() -> Self {
        Self {
            read_set: ReadSet::empty(),
            limit_read_set: None,
            num_intervals: 0,
            user_tx_size: TransactionReadSize::default(),
            system_tx_size: TransactionReadSize::default(),
        }
    }

    pub(crate) fn empty_with_accounting(&self) -> Self {
        Self {
            read_set: ReadSet::empty(),
            limit_read_set: Some(Box::new(self.read_set.clone())),
            num_intervals: self.num_intervals,
            user_tx_size: self.user_tx_size.clone(),
            system_tx_size: self.system_tx_size.clone(),
        }
    }

    pub(crate) fn split_handler_capture(self, baseline: &Self) -> (Self, Self) {
        let Self {
            read_set,
            limit_read_set,
            num_intervals,
            user_tx_size,
            system_tx_size,
        } = self;
        let handler_num_intervals = read_set.num_intervals();
        let handler_reads = Self {
            read_set,
            limit_read_set: None,
            num_intervals: handler_num_intervals,
            user_tx_size: user_tx_size.checked_sub(&baseline.user_tx_size),
            system_tx_size: system_tx_size.checked_sub(&baseline.system_tx_size),
        };
        let full_reads = Self {
            read_set: *limit_read_set.expect("handler capture missing full read set"),
            limit_read_set: None,
            num_intervals,
            user_tx_size,
            system_tx_size,
        };
        (handler_reads, full_reads)
    }

    pub fn into_read_set(self) -> ReadSet {
        self.read_set
    }

    pub fn read_set(&self) -> &ReadSet {
        &self.read_set
    }

    fn _record_indexed(
        &mut self,
        index_name: TabletIndexName,
        fields: IndexedFields,
        intervals: impl IntoIterator<Item = Interval> + 'static,
    ) -> (usize, usize) {
        if self.limit_read_set.is_none() {
            return Self::_record_indexed_into(&mut self.read_set, index_name, fields, intervals);
        }
        let intervals: Vec<_> = intervals.into_iter().collect();
        let result = Self::_record_indexed_into(
            &mut self.read_set,
            index_name.clone(),
            fields.clone(),
            intervals.clone(),
        );
        if let Some(limit_read_set) = self.limit_read_set.as_mut() {
            return Self::_record_indexed_into(limit_read_set, index_name, fields, intervals);
        }
        result
    }

    fn _record_indexed_into(
        read_set: &mut ReadSet,
        index_name: TabletIndexName,
        fields: IndexedFields,
        mut intervals: impl IntoIterator<Item = Interval> + 'static,
    ) -> (usize, usize) {
        read_set.indexed.mutate_entry_or_insert_with(
            index_name.clone(),
            || IndexReads {
                fields: fields.clone(),
                intervals: IntervalSet::new(),
                stack_traces: (*READ_SET_CAPTURE_BACKTRACES).then_some(vec![]),
            },
            |reads| {
                let IndexReads {
                    intervals: range_set,
                    stack_traces,
                    fields: existing_fields,
                } = reads;

                assert_eq!(
                    *existing_fields, fields,
                    "trying to change index fields for index {index_name:?}!"
                );

                let range_num_intervals_before = range_set.len();
                if range_set.is_empty()
                    && let Some(intervals) =
                        (&mut intervals as &mut dyn Any).downcast_mut::<IntervalSet>()
                {
                    // optimization: reuse the existing IntervalSet
                    *range_set = mem::take(intervals);
                    if let Some(stack_traces) = stack_traces.as_mut() {
                        for interval in range_set.iter() {
                            stack_traces.push((interval, StackTrace::new()));
                        }
                    }
                } else {
                    for interval in intervals {
                        if let Some(stack_traces) = stack_traces.as_mut() {
                            stack_traces.push((interval.clone(), StackTrace::new()));
                        }
                        range_set.add(interval);
                    }
                }
                let range_num_intervals_after = range_set.len();

                (range_num_intervals_before, range_num_intervals_after)
            },
        )
    }

    /// Call record_indexed_derived to take a read dependency when the user
    /// didn't directly initiate the read and the read didn't go to persistence,
    /// but we are taking a read dependency anyway.
    /// For example, when writing to a table, take a derived read on the table
    /// to make sure it still exists.
    pub fn record_indexed_derived(
        &mut self,
        index_name: TabletIndexName,
        fields: IndexedFields,
        interval: Interval,
    ) {
        self._record_indexed(index_name, fields, [interval]);
    }

    pub fn merge(
        &mut self,
        reads: ReadSet,
        num_intervals: usize,
        user_tx_size: TransactionReadSize,
        system_tx_size: TransactionReadSize,
    ) {
        let (index_reads, search_reads) = reads.consume();
        for (index_name, index_reads) in index_reads {
            self._record_indexed(index_name, index_reads.fields, index_reads.intervals);
        }
        for (index_name, search_reads) in search_reads {
            self.record_search(index_name, search_reads);
        }
        self.num_intervals += num_intervals;
        self.user_tx_size += user_tx_size;
        self.system_tx_size += system_tx_size;
    }

    pub(crate) fn merge_handler_segment(&mut self, reads: TransactionReadSet) {
        let num_intervals = reads.num_intervals();
        let user_tx_size = reads.user_tx_size().clone();
        let system_tx_size = reads.system_tx_size().clone();
        self.merge(
            reads.into_read_set(),
            num_intervals,
            user_tx_size,
            system_tx_size,
        );
        self.num_intervals = self.read_set.num_intervals();
    }

    pub fn record_read_document(
        &mut self,
        component_path: ComponentPath,
        table_name: TableName,
        document_size: usize,
        usage_tracker: &FunctionUsageTracker,
        virtual_system_mapping: &VirtualSystemMapping,
        limits: &TransactionLimits,
    ) -> anyhow::Result<()> {
        // Database bandwidth for document reads
        // TODO: Remove when we switch over to using egress_v2
        let skip_logging_usage = table_name.is_system();
        usage_tracker.track_database_egress(
            component_path.clone(),
            &table_name,
            document_size as u64,
            skip_logging_usage,
        );
        usage_tracker.track_database_egress_v2(
            component_path.clone(),
            &table_name,
            document_size as u64,
            table_name.is_system(),
        );
        if let Some(virtual_table_name) =
            virtual_system_mapping.associated_virtual_table_name(&table_name)
        {
            usage_tracker.track_virtual_table_egress(
                component_path.clone(),
                virtual_table_name,
                document_size as u64,
            );
        }
        usage_tracker.track_database_egress_rows(
            component_path,
            &table_name,
            1,
            skip_logging_usage,
        );

        let max_rows = limits.documents_read;
        let max_bytes = limits.bytes_read;

        let tx_size = if skip_logging_usage {
            &mut self.system_tx_size
        } else {
            &mut self.user_tx_size
        };

        // We always increment the size first, even if we throw,
        // we want the size to reflect the read, so that
        // we can tell that we threw and not issue a warning.
        tx_size.total_document_count += 1;
        tx_size.total_document_size += document_size;

        if !skip_logging_usage {
            anyhow::ensure!(
                tx_size.total_document_count <= max_rows,
                ErrorMetadata::pagination_limit(
                    "TooManyDocumentsRead",
                    format!(
                        "Too many documents read in a single function execution (limit: {}). \
                         {OVER_LIMIT_HELP}",
                        max_rows,
                    )
                ),
            );
            anyhow::ensure!(
                tx_size.total_document_size <= max_bytes,
                ErrorMetadata::pagination_limit(
                    "TooManyBytesRead",
                    format!(
                        "Too many bytes read in a single function execution (limit: {} bytes). \
                         {OVER_LIMIT_HELP}",
                        max_bytes,
                    )
                ),
            );
        }
        Ok(())
    }

    pub fn record_indexed_directly(
        &mut self,
        index_name: TabletIndexName,
        fields: IndexedFields,
        interval: Interval,
        limits: &TransactionLimits,
    ) -> anyhow::Result<()> {
        let _s = static_span!();

        let (num_intervals_before, num_intervals_after) =
            self._record_indexed(index_name, fields, [interval]);

        self.num_intervals = self.num_intervals.saturating_sub(num_intervals_before);
        self.num_intervals += num_intervals_after;
        let max_intervals = limits.database_queries;
        if self.num_intervals > max_intervals {
            anyhow::bail!(
                anyhow::anyhow!("top three: {}", self.top_three_intervals()).context(
                    ErrorMetadata::pagination_limit(
                        "TooManyReads",
                        format!(
                            "Too many reads in a single function execution (limit: {}). \
                             {OVER_LIMIT_HELP}",
                            max_intervals,
                        ),
                    )
                )
            );
        }
        Ok(())
    }

    pub fn top_three_intervals(&self) -> String {
        let read_set = self.limit_read_set.as_deref().unwrap_or(&self.read_set);
        let mut intervals: Vec<_> = read_set
            .indexed
            .iter()
            .map(|(index, reads)| (reads.intervals.len(), index))
            .collect();
        intervals.sort_by_key(|(len, _)| *len);
        let top_three = intervals
            .iter()
            .rev()
            .take(3)
            .map(|(amt, index)| format!("{index}: {amt}"))
            .collect::<Vec<_>>();
        top_three.join(",")
    }

    pub fn record_search(&mut self, index_name: TabletIndexName, search_reads: SearchQueryReads) {
        if let Some(limit_read_set) = self.limit_read_set.as_mut() {
            Self::record_search_into(&mut self.read_set, index_name.clone(), search_reads.clone());
            Self::record_search_into(limit_read_set, index_name, search_reads);
        } else {
            Self::record_search_into(&mut self.read_set, index_name, search_reads);
        }
    }

    fn record_search_into(
        read_set: &mut ReadSet,
        index_name: TabletIndexName,
        search_reads: SearchQueryReads,
    ) {
        read_set.search.mutate_entry_or_insert_with(
            index_name,
            SearchQueryReads::empty,
            |existing_reads| existing_reads.merge(search_reads),
        );
    }

    pub fn num_intervals(&self) -> usize {
        self.num_intervals
    }

    pub fn user_tx_size(&self) -> &TransactionReadSize {
        &self.user_tx_size
    }

    pub fn system_tx_size(&self) -> &TransactionReadSize {
        &self.system_tx_size
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "testing")]
    use value::{
        DeveloperDocumentId,
        InternalId,
        ResolvedDocumentId,
        TableNumber,
    };

    use super::*;

    #[cfg(feature = "testing")]
    #[test]
    fn lane_local_insert_id_normalization_removes_prefix_dependency_only() {
        let primary_insert = ResolvedDocumentId::new(
            TabletId::MIN,
            DeveloperDocumentId::new(TableNumber::MIN, InternalId([1; 16])),
        );
        let shadow_insert = ResolvedDocumentId::new(
            TabletId::MIN,
            DeveloperDocumentId::new(TableNumber::MIN, InternalId([2; 16])),
        );
        let primary_existing = ResolvedDocumentId::new(
            TabletId::MIN,
            DeveloperDocumentId::new(TableNumber::MIN, InternalId([3; 16])),
        );
        let shadow_existing = ResolvedDocumentId::new(
            TabletId::MIN,
            DeveloperDocumentId::new(TableNumber::MIN, InternalId([4; 16])),
        );
        let read_set = |insert: ResolvedDocumentId, existing: Option<ResolvedDocumentId>| {
            let mut intervals = IntervalSet::new();
            // Keep this byte-for-byte aligned with Writes::register_new_id:
            // generated-ID admission uses a prefix dependency, not a singleton.
            intervals.add(Interval::prefix(
                IndexKey::new(vec![], insert.into()).to_bytes().into(),
            ));
            if let Some(existing) = existing {
                intervals.add(Interval::singleton(
                    IndexKey::new(vec![], existing.into()).to_bytes().into(),
                ));
            }
            ReadSet::new(
                BTreeMap::from([(
                    TabletIndexName::by_id(insert.tablet_id),
                    IndexReads {
                        fields: IndexedFields::by_id(),
                        intervals,
                        stack_traces: None,
                    },
                )]),
                BTreeMap::new(),
            )
        };
        let insert_mapping = BTreeMap::from([(primary_insert, shadow_insert)]);

        assert!(read_set(primary_insert, None)
            .has_same_read_dependencies_with_lane_local_insert_ids(
                &read_set(shadow_insert, None),
                &insert_mapping,
            ));
        assert!(!read_set(primary_insert, Some(primary_existing))
            .has_same_read_dependencies_with_lane_local_insert_ids(
                &read_set(shadow_insert, Some(shadow_existing)),
                &insert_mapping,
            ));
    }

    #[test]
    fn handler_capture_split_keeps_coalesced_full_accounting() -> anyhow::Result<()> {
        let limits = TransactionLimits::default();
        let index_name = TabletIndexName::by_id(TabletId::MIN);
        let fields = IndexedFields::by_id();
        let mut baseline = TransactionReadSet::new();
        baseline.record_indexed_directly(
            index_name.clone(),
            fields.clone(),
            Interval::prefix(vec![0].into()),
            &limits,
        )?;
        baseline.record_indexed_directly(
            index_name.clone(),
            fields.clone(),
            Interval::prefix(vec![1].into()),
            &limits,
        )?;
        baseline.user_tx_size = TransactionReadSize {
            total_document_size: 10,
            total_document_count: 1,
        };
        baseline.system_tx_size = TransactionReadSize {
            total_document_size: 20,
            total_document_count: 2,
        };

        let mut capture = baseline.empty_with_accounting();
        capture.record_indexed_directly(index_name.clone(), fields, Interval::all(), &limits)?;
        capture.user_tx_size.total_document_size += 30;
        capture.user_tx_size.total_document_count += 3;
        capture.system_tx_size.total_document_size += 40;
        capture.system_tx_size.total_document_count += 4;

        let (handler_reads, full_reads) = capture.split_handler_capture(&baseline);

        assert_eq!(handler_reads.num_intervals(), 1);
        assert_eq!(full_reads.num_intervals(), 1);
        assert_eq!(
            handler_reads
                .read_set
                .indexed
                .get(&index_name)
                .expect("handler read set did not retain its range")
                .intervals
                .len(),
            1
        );
        assert_eq!(
            full_reads
                .read_set
                .indexed
                .get(&index_name)
                .expect("full read set did not retain its range")
                .intervals
                .len(),
            1
        );
        assert_eq!(
            handler_reads.user_tx_size,
            TransactionReadSize {
                total_document_size: 30,
                total_document_count: 3,
            }
        );
        assert_eq!(
            handler_reads.system_tx_size,
            TransactionReadSize {
                total_document_size: 40,
                total_document_count: 4,
            }
        );
        assert_eq!(
            full_reads.user_tx_size,
            TransactionReadSize {
                total_document_size: 40,
                total_document_count: 4,
            }
        );
        assert_eq!(
            full_reads.system_tx_size,
            TransactionReadSize {
                total_document_size: 60,
                total_document_count: 6,
            }
        );
        Ok(())
    }

    #[test]
    fn merge_unions_derived_dependencies_and_adds_accounting() -> anyhow::Result<()> {
        let limits = TransactionLimits::default();
        let derived_index = TabletIndexName::by_id(TabletId::MIN);
        let direct_index = TabletIndexName::by_creation_time(TabletId::MIN);
        let mut previous = TransactionReadSet::new();
        previous.record_indexed_derived(
            derived_index.clone(),
            IndexedFields::by_id(),
            Interval::prefix(vec![0].into()),
        );
        previous.record_indexed_directly(
            direct_index.clone(),
            IndexedFields::creation_time(),
            Interval::prefix(vec![0].into()),
            &limits,
        )?;
        previous.user_tx_size = TransactionReadSize {
            total_document_size: 10,
            total_document_count: 1,
        };
        previous.system_tx_size = TransactionReadSize {
            total_document_size: 20,
            total_document_count: 2,
        };

        let mut additional = TransactionReadSet::new();
        for key in [0, 2] {
            additional.record_indexed_derived(
                derived_index.clone(),
                IndexedFields::by_id(),
                Interval::prefix(vec![key].into()),
            );
        }
        additional.record_indexed_directly(
            direct_index,
            IndexedFields::creation_time(),
            Interval::prefix(vec![0].into()),
            &limits,
        )?;
        additional.user_tx_size = TransactionReadSize {
            total_document_size: 30,
            total_document_count: 3,
        };
        additional.system_tx_size = TransactionReadSize {
            total_document_size: 40,
            total_document_count: 4,
        };

        let additional_num_intervals = additional.num_intervals();
        let additional_user_tx_size = additional.user_tx_size().clone();
        let additional_system_tx_size = additional.system_tx_size().clone();
        previous.merge(
            additional.into_read_set(),
            additional_num_intervals,
            additional_user_tx_size,
            additional_system_tx_size,
        );

        // Derived ranges remain OCC dependencies without becoming query
        // accounting, while the direct range is charged once per segment.
        assert_eq!(previous.num_intervals(), 2);
        assert_eq!(previous.read_set.num_intervals(), 3);
        assert_eq!(
            previous.user_tx_size,
            TransactionReadSize {
                total_document_size: 40,
                total_document_count: 4,
            }
        );
        assert_eq!(
            previous.system_tx_size,
            TransactionReadSize {
                total_document_size: 60,
                total_document_count: 6,
            }
        );
        Ok(())
    }
}
