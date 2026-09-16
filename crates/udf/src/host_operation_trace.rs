use std::mem;

use value::heap_size::HeapSize;

/// A fixed, data-free classification of a logical UDF host operation.
///
/// The ordered trace intentionally stores no operation arguments, concrete
/// results, document IDs, table names, function paths, or caller-provided
/// operation names. It is process-local comparison evidence for the
/// query-shadow lanes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum LogicalHostOperation {
    AuditLog,
    CancelJob,
    ComponentArgument,
    CreateFunctionHandle,
    DatabaseCount,
    DatabaseDelete,
    DatabaseGet,
    DatabaseInsert,
    DatabaseNormalizeId,
    DatabasePatch,
    DatabaseQueryPage,
    DatabaseQueryCleanup,
    DatabaseQueryStream,
    DatabaseQueryStreamNext,
    DatabaseReplace,
    DeploymentMetadata,
    FunctionMetadata,
    RequestMetadata,
    RequireOperation,
    RunUdf,
    Schedule,
    SnapshotTimestamp,
    StorageDelete,
    StorageGenerateUploadUrl,
    StorageGetMetadata,
    StorageGetUrl,
    ThrowOcc,
    ThrowOverloaded,
    TransactionMetrics,
    UnknownAsyncSyscall,
    UnknownSyncSyscall,
    UserIdentity,
    WriteDeploymentAuditLog,
}

impl LogicalHostOperation {
    /// Stable, data-free name used in query-shadow diagnostics.
    pub const fn diagnostic_key(self) -> &'static str {
        match self {
            Self::AuditLog => "audit_log",
            Self::CancelJob => "cancel_job",
            Self::ComponentArgument => "component_argument",
            Self::CreateFunctionHandle => "create_function_handle",
            Self::DatabaseCount => "database_count",
            Self::DatabaseDelete => "database_delete",
            Self::DatabaseGet => "database_get",
            Self::DatabaseInsert => "database_insert",
            Self::DatabaseNormalizeId => "database_normalize_id",
            Self::DatabasePatch => "database_patch",
            Self::DatabaseQueryPage => "database_query_page",
            Self::DatabaseQueryCleanup => "database_query_cleanup",
            Self::DatabaseQueryStream => "database_query_stream",
            Self::DatabaseQueryStreamNext => "database_query_stream_next",
            Self::DatabaseReplace => "database_replace",
            Self::DeploymentMetadata => "deployment_metadata",
            Self::FunctionMetadata => "function_metadata",
            Self::RequestMetadata => "request_metadata",
            Self::RequireOperation => "require_operation",
            Self::RunUdf => "run_udf",
            Self::Schedule => "schedule",
            Self::SnapshotTimestamp => "snapshot_timestamp",
            Self::StorageDelete => "storage_delete",
            Self::StorageGenerateUploadUrl => "storage_generate_upload_url",
            Self::StorageGetMetadata => "storage_get_metadata",
            Self::StorageGetUrl => "storage_get_url",
            Self::ThrowOcc => "throw_occ",
            Self::ThrowOverloaded => "throw_overloaded",
            Self::TransactionMetrics => "transaction_metrics",
            Self::UnknownAsyncSyscall => "unknown_async_syscall",
            Self::UnknownSyncSyscall => "unknown_sync_syscall",
            Self::UserIdentity => "user_identity",
            Self::WriteDeploymentAuditLog => "write_deployment_audit_log",
        }
    }
}

impl HeapSize for LogicalHostOperation {
    fn heap_size(&self) -> usize {
        0
    }
}

/// The completed status of one logical host operation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum LogicalHostOperationStatus {
    Pending,
    Success,
    Failure,
}

impl LogicalHostOperationStatus {
    /// Stable, data-free name used in query-shadow diagnostics.
    pub const fn diagnostic_key(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

impl HeapSize for LogicalHostOperationStatus {
    fn heap_size(&self) -> usize {
        0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct HostOperationTraceEntry {
    operation: LogicalHostOperation,
    status: LogicalHostOperationStatus,
}

impl HeapSize for HostOperationTraceEntry {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HostOperationTraceEntry {
    pub fn operation(self) -> LogicalHostOperation {
        self.operation
    }

    pub fn status(self) -> LogicalHostOperationStatus {
        self.status
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HostOperationTraceEntryHandle(usize);

/// Ordered, process-local host-operation evidence for a UDF outcome.
///
/// This is deliberately separate from `SyscallTrace`: the latter is durable
/// aggregate observability, while this sequence exists only to compare the
/// in-process V8-primary and Static Hermes shadow executions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostOperationTrace {
    // A normal UDF invocation never needs this shadow-only evidence. Keeping
    // the disabled state distinct prevents an absent trace from being treated
    // as an empty, comparable trace.
    entries: Option<Vec<HostOperationTraceEntry>>,
}

impl Default for HostOperationTrace {
    fn default() -> Self {
        Self { entries: None }
    }
}

impl HeapSize for HostOperationTrace {
    fn heap_size(&self) -> usize {
        self.entries.as_ref().map_or(0, |entries| {
            entries
                .capacity()
                .saturating_mul(mem::size_of::<HostOperationTraceEntry>())
        })
    }
}

impl From<Vec<LogicalHostOperation>> for HostOperationTrace {
    fn from(operations: Vec<LogicalHostOperation>) -> Self {
        let mut trace = Self::for_query_shadow();
        for operation in operations {
            trace.record(operation, LogicalHostOperationStatus::Success);
        }
        trace
    }
}

impl HostOperationTrace {
    /// Enables collection for one side of a paired query-shadow invocation.
    pub fn for_query_shadow() -> Self {
        Self {
            entries: Some(Vec::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.entries.is_some()
    }

    /// Returns false for disabled traces, incomplete traces, and operations
    /// whose classification is intentionally unknown. Any of those states
    /// makes an equality comparison unsafe.
    pub fn is_comparison_eligible(&self) -> bool {
        self.entries.as_ref().is_some_and(|entries| {
            entries.iter().all(|entry| {
                entry.status != LogicalHostOperationStatus::Pending
                    && !matches!(
                        entry.operation,
                        LogicalHostOperation::UnknownAsyncSyscall
                            | LogicalHostOperation::UnknownSyncSyscall
                    )
            })
        })
    }

    pub fn start(
        &mut self,
        operation: LogicalHostOperation,
    ) -> Option<HostOperationTraceEntryHandle> {
        let entries = self.entries.as_mut()?;
        let handle = HostOperationTraceEntryHandle(entries.len());
        entries.push(HostOperationTraceEntry {
            operation,
            status: LogicalHostOperationStatus::Pending,
        });
        Some(handle)
    }

    pub fn complete(
        &mut self,
        entry: Option<HostOperationTraceEntryHandle>,
        status: LogicalHostOperationStatus,
    ) {
        let Some(HostOperationTraceEntryHandle(index)) = entry else {
            return;
        };
        let entry = self
            .entries
            .as_mut()
            .expect("host-operation trace completion occurred while tracing is disabled")
            .get_mut(index)
            .expect("host-operation trace completion index is invalid");
        assert_eq!(
            entry.status,
            LogicalHostOperationStatus::Pending,
            "host-operation trace entry completed more than once"
        );
        entry.status = status;
    }

    pub fn record(&mut self, operation: LogicalHostOperation, status: LogicalHostOperationStatus) {
        let handle = self.start(operation);
        self.complete(handle, status);
    }

    pub fn extend(&mut self, other: Self) {
        match (&mut self.entries, other.entries) {
            (Some(entries), Some(other_entries)) => entries.extend(other_entries),
            (None, None) => {},
            (Some(_), None) | (None, Some(_)) => {
                panic!("nested UDF host-operation trace enablement diverged")
            },
        }
    }

    pub fn entries(&self) -> Option<&[HostOperationTraceEntry]> {
        self.entries.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HostOperationTrace,
        LogicalHostOperation,
        LogicalHostOperationStatus,
    };

    #[test]
    fn trace_preserves_operation_order_across_nested_outcomes() {
        let mut trace = HostOperationTrace::from(vec![LogicalHostOperation::RunUdf]);
        trace.extend(HostOperationTrace::from(vec![
            LogicalHostOperation::DatabaseGet,
            LogicalHostOperation::DatabaseQueryStreamNext,
        ]));

        assert_eq!(
            trace
                .entries()
                .unwrap()
                .iter()
                .map(|entry| (entry.operation(), entry.status()))
                .collect::<Vec<_>>(),
            vec![
                (
                    LogicalHostOperation::RunUdf,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseGet,
                    LogicalHostOperationStatus::Success,
                ),
                (
                    LogicalHostOperation::DatabaseQueryStreamNext,
                    LogicalHostOperationStatus::Success,
                ),
            ]
        );
    }

    #[test]
    fn trace_preserves_per_operation_failure_status() {
        let mut trace = HostOperationTrace::for_query_shadow();
        trace.record(
            LogicalHostOperation::DatabaseGet,
            LogicalHostOperationStatus::Failure,
        );

        assert_eq!(
            trace.entries().unwrap()[0].status(),
            LogicalHostOperationStatus::Failure
        );
    }

    #[test]
    fn disabled_trace_does_not_collect_operations() {
        let mut trace = HostOperationTrace::default();
        trace.record(
            LogicalHostOperation::DatabaseGet,
            LogicalHostOperationStatus::Success,
        );

        assert!(!trace.is_enabled());
        assert!(!trace.is_comparison_eligible());
        assert_eq!(trace.entries(), None);
    }

    #[test]
    fn incomplete_or_unknown_trace_is_not_comparison_eligible() {
        let mut pending = HostOperationTrace::for_query_shadow();
        let _ = pending.start(LogicalHostOperation::DatabaseGet);
        assert!(!pending.is_comparison_eligible());

        let mut unknown = HostOperationTrace::for_query_shadow();
        unknown.record(
            LogicalHostOperation::UnknownAsyncSyscall,
            LogicalHostOperationStatus::Failure,
        );
        assert!(!unknown.is_comparison_eligible());
    }
}
