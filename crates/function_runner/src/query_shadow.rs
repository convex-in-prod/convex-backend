#[cfg(feature = "static-hermes-wasmtime-gate")]
use std::collections::{
    BTreeMap,
    BTreeSet,
};
#[cfg(feature = "testing")]
use std::sync::LazyLock;
use std::{
    fmt,
    time::Duration,
};

#[cfg(feature = "static-hermes-wasmtime-gate")]
use anyhow::Context;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use common::document::PendingDocumentUpdate;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use common::errors::JsError;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use common::log_lines::{
    LogLine,
    LogLines,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use common::sha256::Sha256;
use common::{
    sha256::Sha256Digest,
    types::UdfType,
};
#[cfg(feature = "testing")]
use database::reads::ReadSetComparisonDiagnostic;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use database::ReadSet;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use isolate::StaticHermesWasmTrapDiagnostic;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use metrics::{
    log_counter_with_labels,
    register_convex_counter,
    StaticMetricLabel,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use model::scheduled_jobs::{
    args::SCHEDULED_JOBS_ARGS_TABLE,
    SCHEDULED_JOBS_TABLE,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use serde_json::Value as JsonValue;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use sync_types::Timestamp;
#[cfg(feature = "static-hermes-wasmtime-gate")]
use udf::{
    FunctionOutcome,
    HostOperationTrace,
    UdfOutcome,
};
use udf::{
    LogicalHostOperation,
    LogicalHostOperationStatus,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use value::{
    json_deserialize_bytes,
    ConvexValue,
    ResolvedDocumentId,
    TableMapping,
};

#[cfg(feature = "static-hermes-wasmtime-gate")]
use crate::FunctionFinalTransaction;

const QUERY_SHADOW_SHA256_LENGTH: usize = 64;
#[cfg(feature = "static-hermes-wasmtime-gate")]
const QUERY_SHADOW_HOST_OPERATION_DIAGNOSTIC_LIMIT: usize = 16;

/// An opaque authenticated registry-generation and route key for shadow
/// metrics.
///
/// Both parts are SHA-256 digests authenticated by the runtime registry. The
/// generation digest prevents observations for a stable module-graph route ID
/// from being combined across source and artifact generations.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct QueryShadowRouteKey(Box<str>);

impl QueryShadowRouteKey {
    pub fn new(generation_sha256: &str, route_id: &str) -> Result<Self, QueryShadowRouteKeyError> {
        for digest in [generation_sha256, route_id] {
            if digest.len() != QUERY_SHADOW_SHA256_LENGTH
                || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(QueryShadowRouteKeyError);
            }
        }
        Ok(Self(
            format!(
                "{}:{}",
                generation_sha256.to_ascii_lowercase(),
                route_id.to_ascii_lowercase()
            )
            .into_boxed_str(),
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Return the authenticated registry-generation SHA-256 digest.
    pub fn generation_sha256(&self) -> &str {
        &self.0[..QUERY_SHADOW_SHA256_LENGTH]
    }

    /// Return the authenticated registry route SHA-256 digest.
    pub fn route_sha256(&self) -> &str {
        &self.0[QUERY_SHADOW_SHA256_LENGTH + 1..]
    }

    /// Parse the stable cursor representation emitted by an operator surface.
    pub fn parse(value: &str) -> Result<Self, QueryShadowRouteKeyError> {
        let Some((generation_sha256, route_sha256)) = value.split_once(':') else {
            return Err(QueryShadowRouteKeyError);
        };
        if route_sha256.contains(':') {
            return Err(QueryShadowRouteKeyError);
        }
        Self::new(generation_sha256, route_sha256)
    }
}

impl fmt::Debug for QueryShadowRouteKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QueryShadowRouteKey(..)")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid query-shadow registry-generation or route ID")]
pub struct QueryShadowRouteKeyError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryShadowComparison {
    pub snapshot_matches: bool,
    pub result_matches: bool,
    pub journal_matches: bool,
    pub read_dependencies_match: bool,
    pub invocation_inputs_match: bool,
    pub host_operation_trace_matches: bool,
    pub host_operation_error_matches: bool,
    pub observed_identity_matches: bool,
    pub observed_time_matches: bool,
    pub observed_rng_matches: bool,
    pub log_lines_match: bool,
    pub audit_log_lines_match: bool,
    pub write_set_matches: bool,
    pub inserted_document_identity: QueryShadowInsertedDocumentIdentity,
    pub host_operation_trace_diagnostic: Option<QueryShadowHostOperationTraceDiagnostic>,
    pub result_diagnostic: Option<QueryShadowResultDiagnostic>,
}

/// First structural difference in query results, without retaining request
/// data. Path elements are array indexes or canonical object-field ordinals,
/// never field names. Types and reasons are fixed vocabulary, not error
/// messages. Error fingerprints permit comparison with known error templates,
/// never comparison authority. Exhausting inspection does not alter comparison.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryShadowResultDiagnostic {
    pub reason: QueryShadowResultDifference,
    pub path: Vec<usize>,
    pub primary_type: &'static str,
    pub shadow_type: &'static str,
    pub primary_error: Option<QueryShadowErrorDiagnostic>,
    pub shadow_error: Option<QueryShadowErrorDiagnostic>,
}

/// Bounded error evidence for authenticated operator inspection, never metric
/// labels. No message, stack, or custom data is retained. Fingerprints
/// correlate exact messages; they do not make potentially sensitive messages
/// anonymous.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryShadowErrorDiagnostic {
    pub kind: QueryShadowErrorKind,
    /// Absent when the UTF-8 message exceeds the 64 KiB inspection bound.
    pub message_sha256: Option<Sha256Digest>,
    pub has_custom_data: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryShadowErrorKind {
    InitializationTimeout,
    ExecutionTimeout,
    InstructionBudget,
    TypeError,
    RangeError,
    ReferenceError,
    SyntaxError,
    EvalError,
    UriError,
    AggregateError,
    Error,
    ConvexError,
    Other,
}

impl QueryShadowErrorKind {
    pub const fn diagnostic_key(self) -> &'static str {
        match self {
            Self::InitializationTimeout => "initialization_timeout",
            Self::ExecutionTimeout => "execution_timeout",
            Self::InstructionBudget => "instruction_budget",
            Self::TypeError => "type_error",
            Self::RangeError => "range_error",
            Self::ReferenceError => "reference_error",
            Self::SyntaxError => "syntax_error",
            Self::EvalError => "eval_error",
            Self::UriError => "uri_error",
            Self::AggregateError => "aggregate_error",
            Self::Error => "error",
            Self::ConvexError => "convex_error",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryShadowResultDifference {
    Value,
    Type,
    ArrayLength,
    ObjectFields,
    OutcomeType,
    ErrorMessage,
    ErrorData,
    InvalidValue,
    InspectionLimit,
}

impl QueryShadowResultDifference {
    pub const fn diagnostic_key(self) -> &'static str {
        match self {
            Self::Value => "value",
            Self::Type => "type",
            Self::ArrayLength => "array_length",
            Self::ObjectFields => "object_fields",
            Self::OutcomeType => "outcome_type",
            Self::ErrorMessage => "error_message",
            Self::ErrorData => "error_data",
            Self::InvalidValue => "invalid_value",
            Self::InspectionLimit => "inspection_limit",
        }
    }
}

/// One data-free host-operation count that differs between the paired lanes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryShadowHostOperationCountDifference {
    pub operation: LogicalHostOperation,
    pub status: LogicalHostOperationStatus,
    pub primary_count: u64,
    pub shadow_count: u64,
}

/// Bounded details for a host-operation trace mismatch.
///
/// This diagnostic contains only fixed operation and status enums plus counts.
/// It never retains syscall arguments, function paths, IDs, values, or errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryShadowHostOperationTraceDiagnostic {
    pub primary_comparison_eligible: bool,
    pub shadow_comparison_eligible: bool,
    pub differing_counts: Vec<QueryShadowHostOperationCountDifference>,
    pub omitted_differing_count: u64,
}

/// Whether lane-local document IDs affected mutation comparison.
///
/// This classification contains no document IDs, table names, values, or
/// request material. It distinguishes an observable semantic mismatch from a
/// failure in the comparison's inserted-document normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryShadowInsertedDocumentIdentity {
    NotApplicable,
    NotRequired,
    Matched,
    Inconsistent,
    Unresolved,
}

impl QueryShadowInsertedDocumentIdentity {
    pub const fn metric_key(self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::NotRequired => "not_required",
            Self::Matched => "matched",
            Self::Inconsistent => "inconsistent",
            Self::Unresolved => "unresolved",
        }
    }
}

/// Fixed, data-free trace evidence for an in-process query-shadow test.
#[cfg(feature = "testing")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryShadowTestTraceDiagnostic {
    pub primary: Vec<(LogicalHostOperation, LogicalHostOperationStatus)>,
    pub shadow: Vec<(LogicalHostOperation, LogicalHostOperationStatus)>,
    pub read_dependencies: ReadSetComparisonDiagnostic,
}

#[cfg(feature = "testing")]
static QUERY_SHADOW_TEST_TRACE_DIAGNOSTICS: LazyLock<
    parking_lot::Mutex<Vec<QueryShadowTestTraceDiagnostic>>,
> = LazyLock::new(Default::default);

#[cfg(feature = "testing")]
pub fn take_query_shadow_test_trace_diagnostics() -> Vec<QueryShadowTestTraceDiagnostic> {
    std::mem::take(&mut *QUERY_SHADOW_TEST_TRACE_DIAGNOSTICS.lock())
}

impl QueryShadowComparison {
    /// Whether every privacy-safe comparison dimension matched.
    pub const fn matches(&self) -> bool {
        self.snapshot_matches
            && self.result_matches
            && self.journal_matches
            && self.read_dependencies_match
            && self.invocation_inputs_match
            && self.host_operation_trace_matches
            && self.host_operation_error_matches
            && self.observed_identity_matches
            && self.observed_time_matches
            && self.observed_rng_matches
            && self.log_lines_match
            && self.audit_log_lines_match
            && self.write_set_matches
            && match self.inserted_document_identity {
                QueryShadowInsertedDocumentIdentity::NotApplicable
                | QueryShadowInsertedDocumentIdentity::NotRequired
                | QueryShadowInsertedDocumentIdentity::Matched => true,
                QueryShadowInsertedDocumentIdentity::Inconsistent
                | QueryShadowInsertedDocumentIdentity::Unresolved => false,
            }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryShadowTerminal {
    CapacityDrop,
    ShadowFailure {
        stage: QueryShadowFailureStage,
        reason: QueryShadowFailureReason,
        generated_export_diagnostic: Option<QueryShadowGeneratedExportDiagnostic>,
        wasmtime_trap_diagnostic: Option<StaticHermesWasmTrapDiagnostic>,
    },
    Timeout,
    InvalidShadow {
        reason: QueryShadowInvalidReason,
    },
    InvalidPrimary,
    PrimaryFailure,
    PrimarySnapshotRejected,
    UnexpectedTaskExit,
}

/// Fixed rejection causes, without transaction, request, or error payloads.
/// Invalid observations cannot establish either agreement or divergence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum QueryShadowInvalidReason {
    #[error("shadow snapshot validation failed")]
    SnapshotValidation,
    #[error("shadow lanes returned different UDF types")]
    LaneTypeMismatch,
    #[error("query shadow produced database writes")]
    QueryWrites,
    #[error("shadow received an unexpected UDF outcome type")]
    OutcomeType,
    #[error("shadow did not capture handler reads")]
    MissingHandlerReads,
    #[error("shadow host-operation trace is incomplete")]
    IncompleteHostTrace,
    #[error("shadow could not normalize mutation writes")]
    WriteNormalization,
}

impl QueryShadowInvalidReason {
    pub const fn diagnostic_key(self) -> &'static str {
        match self {
            Self::SnapshotValidation => "snapshot_validation",
            Self::LaneTypeMismatch => "lane_type_mismatch",
            Self::QueryWrites => "query_writes",
            Self::OutcomeType => "outcome_type",
            Self::MissingHandlerReads => "missing_handler_reads",
            Self::IncompleteHostTrace => "incomplete_host_trace",
            Self::WriteNormalization => "write_normalization",
        }
    }
}

/// Fixed, data-free detail for a generated-export dispatch failure.
///
/// The optional status is a bounded guest return code. No route identity,
/// request data, result, or underlying runtime error is retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryShadowGeneratedExportDiagnostic {
    FirstSelectorTrap,
    FirstSelectorRejected { status: i32 },
    SelectedEntryPreparationTrap,
    SelectedEntryPreparationRejected { status: i32 },
    SecondSelectorTrap,
    SecondSelectorRejected { status: i32 },
}

impl QueryShadowGeneratedExportDiagnostic {
    pub const fn phase_key(self) -> &'static str {
        match self {
            Self::FirstSelectorTrap | Self::FirstSelectorRejected { .. } => "first_selector",
            Self::SelectedEntryPreparationTrap | Self::SelectedEntryPreparationRejected { .. } => {
                "selected_entry_preparation"
            },
            Self::SecondSelectorTrap | Self::SecondSelectorRejected { .. } => "second_selector",
        }
    }

    pub const fn outcome_key(self) -> &'static str {
        match self {
            Self::FirstSelectorTrap
            | Self::SelectedEntryPreparationTrap
            | Self::SecondSelectorTrap => "trap",
            Self::FirstSelectorRejected { .. }
            | Self::SelectedEntryPreparationRejected { .. }
            | Self::SecondSelectorRejected { .. } => "rejected",
        }
    }

    pub const fn status(self) -> Option<i32> {
        match self {
            Self::FirstSelectorRejected { status }
            | Self::SelectedEntryPreparationRejected { status }
            | Self::SecondSelectorRejected { status } => Some(status),
            Self::FirstSelectorTrap
            | Self::SelectedEntryPreparationTrap
            | Self::SecondSelectorTrap => None,
        }
    }
}

/// A fixed, privacy-safe stage for a failed verifier lane.
///
/// This classification intentionally excludes the underlying error, function
/// path, arguments, identity, values, and host-operation data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryShadowFailureStage {
    TransactionSetup,
    EnvironmentSetup,
    RouteAuthentication,
    ModuleLoading,
    WasmExecution,
    VerifierExecution,
    TransactionFinalization,
    Unclassified,
}

/// Fixed, data-free reason for a failed verifier lane.
///
/// The stage identifies the backend boundary. This reason preserves the
/// execution subphase needed to distinguish guest failure from generated
/// export dispatch and runtime lifecycle failures without retaining the
/// underlying error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryShadowFailureReason {
    Infrastructure,
    RouteAuthentication,
    ModuleLoading,
    RuntimeInitialization,
    InitializationTimeout,
    ExecutionTimeout,
    InstructionBudget,
    GeneratedExportDispatch,
    GuestMemoryLimit,
    HostOwnedBytesLimit,
    OpaqueHandleLimit,
    OpaqueHandleSpaceExhausted,
    AggregateMemoryLimit,
    OperationLimit,
    HostAbiInvariant,
    HermesHeapOutOfMemory,
    WasmtimeTrap,
    GuestExecution,
    ResultFinalization,
    RuntimeCleanup,
    VerifierExecution,
    TransactionFinalization,
    Unclassified,
}

impl QueryShadowFailureReason {
    pub const fn metric_key(self) -> &'static str {
        match self {
            Self::Infrastructure => "infrastructure",
            Self::RouteAuthentication => "route_authentication",
            Self::ModuleLoading => "module_loading",
            Self::RuntimeInitialization => "runtime_initialization",
            Self::InitializationTimeout => "initialization_timeout",
            Self::ExecutionTimeout => "execution_timeout",
            Self::InstructionBudget => "instruction_budget",
            Self::GeneratedExportDispatch => "generated_export_dispatch",
            Self::GuestMemoryLimit => "guest_memory_limit",
            Self::HostOwnedBytesLimit => "host_owned_bytes_limit",
            Self::OpaqueHandleLimit => "opaque_handle_limit",
            Self::OpaqueHandleSpaceExhausted => "opaque_handle_space_exhausted",
            Self::AggregateMemoryLimit => "aggregate_memory_limit",
            Self::OperationLimit => "operation_limit",
            Self::HostAbiInvariant => "host_abi_invariant",
            Self::HermesHeapOutOfMemory => "hermes_heap_out_of_memory",
            Self::WasmtimeTrap => "wasmtime_trap",
            Self::GuestExecution => "guest_execution",
            Self::ResultFinalization => "result_finalization",
            Self::RuntimeCleanup => "runtime_cleanup",
            Self::VerifierExecution => "verifier_execution",
            Self::TransactionFinalization => "transaction_finalization",
            Self::Unclassified => "unclassified",
        }
    }
}

impl QueryShadowFailureStage {
    pub const fn metric_key(self) -> &'static str {
        match self {
            Self::TransactionSetup => "transaction_setup",
            Self::EnvironmentSetup => "environment_setup",
            Self::RouteAuthentication => "route_authentication",
            Self::ModuleLoading => "module_loading",
            Self::WasmExecution => "wasm_execution",
            Self::VerifierExecution => "verifier_execution",
            Self::TransactionFinalization => "transaction_finalization",
            Self::Unclassified => "unclassified",
        }
    }
}

/// A privacy-safe query-shadow admission event for an authenticated route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryShadowAdmissionEvent {
    Attempt,
    Admitted,
    PendingDrop,
    CapacityDrop,
}

/// Runtime direction represented by a completed comparison.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueryShadowDirection {
    V8PrimaryWasmShadow,
    WasmPrimaryV8Shadow,
}

impl QueryShadowDirection {
    pub const fn metric_key(self) -> &'static str {
        match self {
            Self::V8PrimaryWasmShadow => "v8_primary_wasm_shadow",
            Self::WasmPrimaryV8Shadow => "wasm_primary_v8_shadow",
        }
    }
}

impl QueryShadowAdmissionEvent {
    pub const fn metric_key(self) -> &'static str {
        match self {
            Self::Attempt => "attempt",
            Self::Admitted => "admitted",
            Self::PendingDrop => "pending_drop",
            Self::CapacityDrop => "capacity_drop",
        }
    }
}

impl QueryShadowTerminal {
    pub const fn metric_key(&self) -> &'static str {
        match self {
            Self::CapacityDrop => "capacity_drop",
            Self::ShadowFailure { .. } => "shadow_failure",
            Self::Timeout => "timeout",
            Self::InvalidShadow { .. } => "invalid_shadow",
            Self::InvalidPrimary => "invalid_primary",
            Self::PrimaryFailure => "primary_failure",
            Self::PrimarySnapshotRejected => "primary_snapshot_rejected",
            Self::UnexpectedTaskExit => "unexpected_task_exit",
        }
    }

    pub const fn shadow_failure_stage(&self) -> Option<QueryShadowFailureStage> {
        match self {
            Self::ShadowFailure { stage, .. } => Some(*stage),
            Self::CapacityDrop
            | Self::Timeout
            | Self::InvalidShadow { .. }
            | Self::InvalidPrimary
            | Self::PrimaryFailure
            | Self::PrimarySnapshotRejected
            | Self::UnexpectedTaskExit => None,
        }
    }

    pub const fn shadow_failure_reason(&self) -> Option<QueryShadowFailureReason> {
        match self {
            Self::ShadowFailure { reason, .. } => Some(*reason),
            Self::CapacityDrop
            | Self::Timeout
            | Self::InvalidShadow { .. }
            | Self::InvalidPrimary
            | Self::PrimaryFailure
            | Self::PrimarySnapshotRejected
            | Self::UnexpectedTaskExit => None,
        }
    }

    pub const fn generated_export_diagnostic(
        &self,
    ) -> Option<QueryShadowGeneratedExportDiagnostic> {
        match self {
            Self::ShadowFailure {
                generated_export_diagnostic,
                ..
            } => *generated_export_diagnostic,
            Self::CapacityDrop
            | Self::Timeout
            | Self::InvalidShadow { .. }
            | Self::InvalidPrimary
            | Self::PrimaryFailure
            | Self::PrimarySnapshotRejected
            | Self::UnexpectedTaskExit => None,
        }
    }

    pub fn wasmtime_trap_diagnostic(&self) -> Option<StaticHermesWasmTrapDiagnostic> {
        match self {
            Self::ShadowFailure {
                wasmtime_trap_diagnostic,
                ..
            } => wasmtime_trap_diagnostic.clone(),
            Self::CapacityDrop
            | Self::Timeout
            | Self::InvalidShadow { .. }
            | Self::InvalidPrimary
            | Self::PrimaryFailure
            | Self::PrimarySnapshotRejected
            | Self::UnexpectedTaskExit => None,
        }
    }
}

/// Privacy-safe report from one authenticated registry-generation route.
///
/// The report deliberately cannot carry a function path, arguments, identity,
/// result, journal contents, or error text. Memory completion is independent
/// of semantic comparison completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryShadowReport {
    Memory {
        udf_type: UdfType,
        route_key: QueryShadowRouteKey,
        direction: QueryShadowDirection,
        observation: udf::wasm_memory::WasmMemoryObservation,
    },
    Admission {
        udf_type: UdfType,
        route_key: QueryShadowRouteKey,
        direction: QueryShadowDirection,
        event: QueryShadowAdmissionEvent,
    },
    Compared {
        udf_type: UdfType,
        route_key: QueryShadowRouteKey,
        direction: QueryShadowDirection,
        primary_duration: Duration,
        wasm_duration: Duration,
        verifier_duration: Option<Duration>,
        comparison: QueryShadowComparison,
    },
    Terminal {
        udf_type: UdfType,
        route_key: QueryShadowRouteKey,
        direction: QueryShadowDirection,
        terminal: QueryShadowTerminal,
    },
}

impl QueryShadowReport {
    pub fn new(
        route_key: QueryShadowRouteKey,
        primary_duration: Duration,
        wasm_duration: Duration,
        comparison: QueryShadowComparison,
    ) -> Self {
        Self::new_for_type(
            UdfType::Query,
            route_key,
            primary_duration,
            wasm_duration,
            comparison,
        )
    }

    pub fn new_for_type(
        udf_type: UdfType,
        route_key: QueryShadowRouteKey,
        primary_duration: Duration,
        wasm_duration: Duration,
        comparison: QueryShadowComparison,
    ) -> Self {
        Self::new_for_type_and_direction(
            udf_type,
            route_key,
            QueryShadowDirection::V8PrimaryWasmShadow,
            primary_duration,
            wasm_duration,
            None,
            comparison,
        )
    }

    pub fn new_for_type_and_direction(
        udf_type: UdfType,
        route_key: QueryShadowRouteKey,
        direction: QueryShadowDirection,
        primary_duration: Duration,
        wasm_duration: Duration,
        verifier_duration: Option<Duration>,
        comparison: QueryShadowComparison,
    ) -> Self {
        Self::Compared {
            udf_type,
            route_key,
            direction,
            primary_duration,
            wasm_duration,
            verifier_duration,
            comparison,
        }
    }

    pub fn route_key(&self) -> &QueryShadowRouteKey {
        match self {
            Self::Memory { route_key, .. }
            | Self::Admission { route_key, .. }
            | Self::Compared { route_key, .. }
            | Self::Terminal { route_key, .. } => route_key,
        }
    }

    pub fn udf_type(&self) -> UdfType {
        match self {
            Self::Memory { udf_type, .. }
            | Self::Admission { udf_type, .. }
            | Self::Compared { udf_type, .. }
            | Self::Terminal { udf_type, .. } => *udf_type,
        }
    }

    /// Return the runtime direction represented by this report.
    pub fn direction(&self) -> QueryShadowDirection {
        match self {
            Self::Memory { direction, .. }
            | Self::Admission { direction, .. }
            | Self::Compared { direction, .. }
            | Self::Terminal { direction, .. } => *direction,
        }
    }
}

pub trait QueryShadowReportSink: Send + Sync + 'static {
    /// Return true only when the report was accepted into route-level evidence.
    fn record_query_shadow_report(&self, report: QueryShadowReport) -> bool;
}

impl<F> QueryShadowReportSink for F
where
    F: Fn(QueryShadowReport) -> bool + Send + Sync + 'static,
{
    fn record_query_shadow_report(&self, report: QueryShadowReport) -> bool {
        self(report)
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
register_convex_counter!(
    STATIC_HERMES_QUERY_SHADOW_TOTAL,
    "Static Hermes shadow outcomes",
    &["udf_type", "dimension", "status"]
);

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) struct QueryShadowObservation {
    begin_timestamp: Timestamp,
    outcome: UdfOutcome,
    reads: ReadSet,
}

/// Observation of document transitions from paired mutation lanes at
/// one database snapshot.
#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) struct MutationShadowObservation {
    begin_timestamp: Timestamp,
    outcome: UdfOutcome,
    reads: ReadSet,
    writes: NormalizedMutationWriteSet,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Debug, Eq, PartialEq)]
struct NormalizedMutationWriteSet {
    existing: BTreeMap<ResolvedDocumentId, NormalizedExistingDocumentWrite>,
    preexisting_inserts: BTreeMap<ResolvedDocumentId, JsonValue>,
    inserts: BTreeMap<ResolvedDocumentId, JsonValue>,
    scheduled_job_inserts: BTreeSet<ResolvedDocumentId>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Debug, Eq, PartialEq)]
struct NormalizedExistingDocumentWrite {
    old_document: JsonValue,
    old_timestamp: Timestamp,
    new_document: Option<JsonValue>,
}

/// A proven relation between IDs allocated by the authoritative primary lane
/// and IDs allocated by the discarded verifier lane.
///
/// Convex document IDs are values, so the relation applies anywhere one of
/// those newly allocated IDs appears in a returned value or staged write.
#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Clone, Debug, Default)]
struct MutationInsertIdMapping {
    document_ids: BTreeMap<ResolvedDocumentId, ResolvedDocumentId>,
    encoded_ids: BTreeMap<String, String>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl MutationInsertIdMapping {
    fn new(document_ids: BTreeMap<ResolvedDocumentId, ResolvedDocumentId>) -> anyhow::Result<Self> {
        let mut encoded_ids = BTreeMap::new();
        let mut shadow_ids = BTreeSet::new();
        for (primary_id, shadow_id) in &document_ids {
            let primary_id = primary_id.developer_id.encode();
            let shadow_id = shadow_id.developer_id.encode();
            anyhow::ensure!(
                !encoded_ids.contains_key(&primary_id),
                "mutation shadow allocated duplicate externally visible document IDs"
            );
            anyhow::ensure!(
                shadow_ids.insert(shadow_id.clone()),
                "mutation shadow allocated duplicate externally visible document IDs"
            );
            encoded_ids.insert(primary_id, shadow_id);
        }
        Ok(Self {
            document_ids,
            encoded_ids,
        })
    }

    fn replace_primary_ids(&self, value: &JsonValue) -> JsonValue {
        match value {
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => value.clone(),
            JsonValue::String(value) => self
                .encoded_ids
                .get(value)
                .cloned()
                .map(JsonValue::String)
                .unwrap_or_else(|| value.clone().into()),
            JsonValue::Array(values) => JsonValue::Array(
                values
                    .iter()
                    .map(|value| self.replace_primary_ids(value))
                    .collect(),
            ),
            // Base64 is a scalar encoding, not a document-ID value, even
            // when its text happens to equal an allocated ID.
            JsonValue::Object(fields) if fields.contains_key("$bytes") => value.clone(),
            JsonValue::Object(fields) => JsonValue::Object(
                fields
                    .iter()
                    .map(|(field, value)| (field.clone(), self.replace_primary_ids(value)))
                    .collect(),
            ),
        }
    }
}

/// The search is bounded so an intentionally symmetric
/// write set cannot turn parity verification into unbounded factorial work.
#[cfg(feature = "static-hermes-wasmtime-gate")]
const MAX_MUTATION_SHADOW_INSERT_MAPPING_CANDIDATES: usize = 4_096;

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl MutationShadowObservation {
    pub(crate) fn new(
        transaction: &FunctionFinalTransaction,
        outcome: &FunctionOutcome,
        preexisting_write_ids: &BTreeSet<ResolvedDocumentId>,
    ) -> Result<Self, QueryShadowInvalidReason> {
        let FunctionOutcome::Mutation(outcome) = outcome else {
            return Err(QueryShadowInvalidReason::OutcomeType);
        };
        let Some(handler_reads) = transaction.handler_reads.as_ref() else {
            return Err(QueryShadowInvalidReason::MissingHandlerReads);
        };
        if !outcome.host_operation_trace.is_comparison_eligible() {
            return Err(QueryShadowInvalidReason::IncompleteHostTrace);
        }
        Ok(Self {
            begin_timestamp: transaction.begin_timestamp,
            outcome: outcome.clone(),
            reads: handler_reads.reads.clone(),
            writes: normalize_mutation_writes(
                &transaction.writes.updates,
                &transaction.table_mapping,
                preexisting_write_ids,
            )
            .map_err(|_| QueryShadowInvalidReason::WriteNormalization)?,
        })
    }

    /// Build an observation for a completed shadow lane whose transaction and
    /// outcome are no longer needed by the caller.
    ///
    /// The primary lane must retain its execution values for publication, but
    /// the shadow lane can transfer them directly into comparison evidence.
    pub(crate) fn from_owned(
        mut transaction: FunctionFinalTransaction,
        outcome: FunctionOutcome,
        preexisting_write_ids: &BTreeSet<ResolvedDocumentId>,
    ) -> Result<Self, QueryShadowInvalidReason> {
        let FunctionOutcome::Mutation(outcome) = outcome else {
            return Err(QueryShadowInvalidReason::OutcomeType);
        };
        let handler_reads = transaction
            .handler_reads
            .take()
            .ok_or(QueryShadowInvalidReason::MissingHandlerReads)?;
        if !outcome.host_operation_trace.is_comparison_eligible() {
            return Err(QueryShadowInvalidReason::IncompleteHostTrace);
        }
        Ok(Self {
            begin_timestamp: transaction.begin_timestamp,
            outcome,
            reads: handler_reads.reads,
            writes: normalize_mutation_writes(
                &transaction.writes.updates,
                &transaction.table_mapping,
                preexisting_write_ids,
            )
            .map_err(|_| QueryShadowInvalidReason::WriteNormalization)?,
        })
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn normalize_mutation_writes(
    updates: &[PendingDocumentUpdate],
    table_mapping: &TableMapping,
    preexisting_write_ids: &BTreeSet<ResolvedDocumentId>,
) -> anyhow::Result<NormalizedMutationWriteSet> {
    let mut existing = BTreeMap::new();
    let mut preexisting_inserts = BTreeMap::new();
    let mut inserts = BTreeMap::new();
    let mut scheduled_job_inserts = BTreeSet::new();
    for update in updates {
        let new_document = update.new_document.as_ref();
        anyhow::ensure!(
            new_document.is_none_or(|document| document.id() == update.id()),
            "mutation shadow received a document write with inconsistent identity"
        );
        match (update.old_document(), new_document) {
            (Some((old_document, old_timestamp)), _) => {
                anyhow::ensure!(
                    old_document.id() == update.id(),
                    "mutation shadow received an existing-document write with inconsistent \
                     identity"
                );
                let write = NormalizedExistingDocumentWrite {
                    old_document: old_document.to_internal_json(),
                    old_timestamp: *old_timestamp,
                    new_document: update
                        .new_document_internal_json()
                        .map(serde_json::to_value)
                        .transpose()
                        .context(
                            "mutation shadow could not normalize an existing-document write",
                        )?,
                };
                anyhow::ensure!(
                    existing.insert(update.id(), write).is_none(),
                    "mutation shadow received duplicate existing-document writes"
                );
            },
            (None, Some(_)) => {
                let mut document = serde_json::to_value(
                    update
                        .new_document_internal_json()
                        .context("mutation shadow insert has no new document")?,
                )
                .context("mutation shadow could not normalize an inserted document")?;
                let table_name = table_mapping.tablet_name(update.id().tablet_id)?;
                if table_name == SCHEDULED_JOBS_ARGS_TABLE {
                    document = normalize_inserted_scheduled_args(document)?;
                }
                // An inherited uncommitted insert is an invocation input, not
                // a lane-local allocation. Its ID and creation time are fixed.
                if preexisting_write_ids.contains(&update.id()) {
                    anyhow::ensure!(
                        preexisting_inserts.insert(update.id(), document).is_none(),
                        "mutation shadow received duplicate inherited-document writes"
                    );
                    continue;
                }
                anyhow::ensure!(
                    inserts.insert(update.id(), document).is_none(),
                    "mutation shadow received duplicate inserted-document writes"
                );
                if table_name == SCHEDULED_JOBS_TABLE {
                    scheduled_job_inserts.insert(update.id());
                }
            },
            (None, None) => anyhow::bail!(
                "mutation shadow received a document write without an old or new document"
            ),
        }
    }
    Ok(NormalizedMutationWriteSet {
        existing,
        preexisting_inserts,
        inserts,
        scheduled_job_inserts,
    })
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn normalize_inserted_scheduled_args(value: JsonValue) -> anyhow::Result<JsonValue> {
    let JsonValue::Object(mut fields) = value else {
        anyhow::bail!("mutation shadow received malformed scheduled-function arguments")
    };
    let serialized_args = fields
        .get("args")
        .context("mutation shadow scheduled-function arguments have no payload")?;
    let ConvexValue::Bytes(serialized_args) = ConvexValue::try_from(serialized_args.clone())?
    else {
        anyhow::bail!("mutation shadow scheduled-function argument payload is not bytes")
    };
    let ConvexValue::Array(args) = json_deserialize_bytes(serialized_args.as_ref())? else {
        anyhow::bail!("mutation shadow scheduled-function argument payload is not an array")
    };

    // Scheduled arguments are canonical Convex JSON stored inside a private
    // byte field. Expose that structure only in comparator-owned data so the
    // inserted-ID relation applies to IDs passed to the scheduled function.
    fields.insert(
        "args".to_owned(),
        serde_json::json!({ "$mutationShadowScheduledArgs": args.to_internal_json() }),
    );
    Ok(JsonValue::Object(fields))
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn encoded_insert_ids(
    inserts: &BTreeMap<ResolvedDocumentId, JsonValue>,
) -> anyhow::Result<BTreeSet<String>> {
    let mut encoded_ids = BTreeSet::new();
    for id in inserts.keys() {
        anyhow::ensure!(
            encoded_ids.insert(id.developer_id.encode()),
            "mutation shadow allocated duplicate externally visible document IDs"
        );
    }
    Ok(encoded_ids)
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn erase_insert_identities(value: &JsonValue, insert_ids: &BTreeSet<String>) -> JsonValue {
    erase_insert_identities_in_value(value, insert_ids, true)
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn normalize_inserted_scheduler_readiness(
    value: &JsonValue,
    scheduled_job_insert: bool,
) -> JsonValue {
    if !scheduled_job_insert {
        return value.clone();
    }
    let mut value = value.clone();
    let JsonValue::Object(fields) = &mut value else {
        return value;
    };
    if !fields.contains_key("nextTs") || !fields.contains_key("originalScheduledTs") {
        return value;
    }
    if !fields.get("argsId").is_some_and(JsonValue::is_string)
        || !fields.get("udfPath").is_some_and(JsonValue::is_string)
        || fields.get("state") != Some(&serde_json::json!({ "type": "pending" }))
        || fields
            .get("completedTs")
            .is_some_and(|value| !value.is_null())
    {
        return value;
    }

    // Scheduling preserves the requested time in originalScheduledTs. The
    // private nextTs field is separately clamped to each transaction lane's
    // wall-clock instant so a due job is not reported as scheduler lag. That
    // readiness instant is not a JavaScript result or developer-visible
    // scheduled time and therefore cannot be equal across paired lanes.
    fields.insert(
        "nextTs".to_owned(),
        serde_json::json!({ "$mutationShadowSchedulerReadyTime": null }),
    );
    value
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn erase_insert_identities_in_value(
    value: &JsonValue,
    insert_ids: &BTreeSet<String>,
    document_root: bool,
) -> JsonValue {
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => value.clone(),
        JsonValue::String(value) if insert_ids.contains(value) => {
            // This object cannot be a user Convex value: ordinary field names
            // cannot begin with '$', and PendingValue reserves only $commitTs.
            serde_json::json!({ "$mutationShadowInsertedId": null })
        },
        JsonValue::String(value) => value.clone().into(),
        JsonValue::Array(values) => JsonValue::Array(
            values
                .iter()
                .map(|value| erase_insert_identities_in_value(value, insert_ids, false))
                .collect(),
        ),
        // Candidate pruning must preserve the same opaque byte values as
        // the complete comparison, including inside decoded scheduler args.
        JsonValue::Object(fields) if fields.contains_key("$bytes") => value.clone(),
        JsonValue::Object(fields) => JsonValue::Object(
            fields
                .iter()
                .map(|(field, value)| {
                    let value = if document_root && field == "_creationTime" {
                        // Insert creation times are assigned independently by
                        // the V8 and Wasm transaction lanes. Only the root
                        // document owns this system field; nested user values
                        // named _creationTime remain part of the comparison.
                        JsonValue::Object(
                            [("$mutationShadowCreationTime".to_owned(), JsonValue::Null)]
                                .into_iter()
                                .collect(),
                        )
                    } else {
                        erase_insert_identities_in_value(value, insert_ids, false)
                    };
                    (field.clone(), value)
                })
                .collect(),
        ),
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn mutation_results_equal(
    primary: &UdfOutcome,
    shadow: &UdfOutcome,
    insert_id_mapping: &MutationInsertIdMapping,
) -> bool {
    match (&primary.result, &shadow.result) {
        (Ok(primary), Ok(shadow)) => {
            insert_id_mapping.replace_primary_ids(&primary.json_value()) == shadow.json_value()
        },
        (Err(primary), Err(shadow)) => {
            primary.message == shadow.message
                && match (&primary.custom_data, &shadow.custom_data) {
                    (Some(primary), Some(shadow)) => {
                        insert_id_mapping.replace_primary_ids(&primary.to_internal_json())
                            == shadow.to_internal_json()
                    },
                    (None, None) => true,
                    (Some(_), None) | (None, Some(_)) => false,
                }
        },
        (Ok(_), Err(_)) | (Err(_), Ok(_)) => false,
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn mutation_write_sets_equal(
    primary: &NormalizedMutationWriteSet,
    shadow: &NormalizedMutationWriteSet,
    insert_id_mapping: &MutationInsertIdMapping,
) -> bool {
    primary.existing.len() == shadow.existing.len()
        && primary.existing.iter().all(|(id, primary)| {
            shadow.existing.get(id).is_some_and(|shadow| {
                primary.old_document == shadow.old_document
                    && primary.old_timestamp == shadow.old_timestamp
                    && match (&primary.new_document, &shadow.new_document) {
                        (Some(primary), Some(shadow)) => {
                            insert_id_mapping.replace_primary_ids(primary) == *shadow
                        },
                        (None, None) => true,
                        (Some(_), None) | (None, Some(_)) => false,
                    }
            })
        })
        && primary.inserts.len() == shadow.inserts.len()
        && primary.preexisting_inserts.len() == shadow.preexisting_inserts.len()
        && primary.preexisting_inserts.iter().all(|(id, primary)| {
            shadow
                .preexisting_inserts
                .get(id)
                .is_some_and(|shadow| insert_id_mapping.replace_primary_ids(primary) == *shadow)
        })
        && primary
            .inserts
            .iter()
            .all(|(primary_id, primary_document)| {
                let Some(shadow_id) = insert_id_mapping.document_ids.get(primary_id) else {
                    return false;
                };
                let primary_is_scheduled_job = primary.scheduled_job_inserts.contains(primary_id);
                let shadow_is_scheduled_job = shadow.scheduled_job_inserts.contains(shadow_id);
                shadow
                    .inserts
                    .get(shadow_id)
                    .is_some_and(|shadow_document| {
                        let primary_document = normalize_inserted_scheduler_readiness(
                            &insert_id_mapping.replace_primary_ids(primary_document),
                            primary_is_scheduled_job,
                        );
                        let shadow_document = normalize_inserted_scheduler_readiness(
                            shadow_document,
                            shadow_is_scheduled_job,
                        );
                        erase_insert_identities(&primary_document, &BTreeSet::new())
                            == erase_insert_identities(&shadow_document, &BTreeSet::new())
                    })
            })
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
struct MutationInsertIdMappingSearch {
    examined_candidates: usize,
    primary_insert_ids: BTreeSet<String>,
    shadow_insert_ids: BTreeSet<String>,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl MutationInsertIdMappingSearch {
    fn partial_mapping_matches(
        &self,
        primary_writes: &NormalizedMutationWriteSet,
        shadow_writes: &NormalizedMutationWriteSet,
        document_ids: &BTreeMap<ResolvedDocumentId, ResolvedDocumentId>,
    ) -> anyhow::Result<bool> {
        let mapping = MutationInsertIdMapping::new(document_ids.clone())?;
        let mut unmapped_primary = self.primary_insert_ids.clone();
        let mut unmapped_shadow = self.shadow_insert_ids.clone();
        for (primary_id, shadow_id) in &mapping.encoded_ids {
            unmapped_primary.remove(primary_id);
            unmapped_shadow.remove(shadow_id);
        }
        for (primary_id, shadow_id) in document_ids {
            let primary = erase_insert_identities(
                &normalize_inserted_scheduler_readiness(
                    &primary_writes.inserts[primary_id],
                    primary_writes.scheduled_job_inserts.contains(primary_id),
                ),
                &unmapped_primary,
            );
            let shadow = erase_insert_identities(
                &normalize_inserted_scheduler_readiness(
                    &shadow_writes.inserts[shadow_id],
                    shadow_writes.scheduled_job_inserts.contains(shadow_id),
                ),
                &unmapped_shadow,
            );
            if mapping.replace_primary_ids(&primary) != shadow {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // One complete mapping that satisfies the caller's exact predicate proves
    // the cross-lane relation, so it can stop the factorial search early.
    fn visit<F>(
        &mut self,
        candidate_sets: &[(ResolvedDocumentId, Vec<ResolvedDocumentId>)],
        primary_writes: &NormalizedMutationWriteSet,
        shadow_writes: &NormalizedMutationWriteSet,
        next_primary: usize,
        document_ids: &mut BTreeMap<ResolvedDocumentId, ResolvedDocumentId>,
        used_shadow_ids: &mut BTreeSet<ResolvedDocumentId>,
        on_mapping: &mut F,
    ) -> anyhow::Result<std::ops::ControlFlow<()>>
    where
        F: FnMut(MutationInsertIdMapping) -> std::ops::ControlFlow<()>,
    {
        if next_primary == candidate_sets.len() {
            let mapping = MutationInsertIdMapping::new(document_ids.clone())?;
            if mutation_write_sets_equal(primary_writes, shadow_writes, &mapping) {
                return Ok(on_mapping(mapping));
            }
            return Ok(std::ops::ControlFlow::Continue(()));
        }

        let (primary_id, shadow_candidates) = &candidate_sets[next_primary];
        for shadow_id in shadow_candidates {
            if !used_shadow_ids.insert(*shadow_id) {
                continue;
            }
            // Count partial expansions too. A candidate graph without a perfect
            // matching may never reach a complete assignment, so leaf-only
            // accounting would leave factorial search work unbounded.
            self.examined_candidates += 1;
            anyhow::ensure!(
                self.examined_candidates <= MAX_MUTATION_SHADOW_INSERT_MAPPING_CANDIDATES,
                "mutation shadow inserted-document identity mapping exceeded its bounded search"
            );
            let previous = document_ids.insert(*primary_id, *shadow_id);
            debug_assert!(previous.is_none());
            // Use already assigned references before exploring permutations of
            // otherwise identical parents, such as jobs with distinct args.
            // Unknown references remain placeholders; acceptance still requires
            // the complete write, read, and result comparison at a leaf.
            let result =
                match self.partial_mapping_matches(primary_writes, shadow_writes, document_ids) {
                    Ok(true) => self.visit(
                        candidate_sets,
                        primary_writes,
                        shadow_writes,
                        next_primary + 1,
                        document_ids,
                        used_shadow_ids,
                        on_mapping,
                    ),
                    Ok(false) => Ok(std::ops::ControlFlow::Continue(())),
                    Err(error) => Err(error),
                };
            let removed = document_ids.remove(primary_id);
            debug_assert_eq!(removed, Some(*shadow_id));
            used_shadow_ids.remove(shadow_id);
            if let std::ops::ControlFlow::Break(()) = result? {
                return Ok(std::ops::ControlFlow::Break(()));
            }
        }
        Ok(std::ops::ControlFlow::Continue(()))
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn visit_mutation_insert_id_mappings<F>(
    primary_writes: &NormalizedMutationWriteSet,
    shadow_writes: &NormalizedMutationWriteSet,
    mut on_mapping: F,
) -> anyhow::Result<()>
where
    F: FnMut(MutationInsertIdMapping) -> std::ops::ControlFlow<()>,
{
    anyhow::ensure!(
        primary_writes.inserts.len() == shadow_writes.inserts.len(),
        "mutation shadow inserted-document counts differ"
    );
    if primary_writes.inserts.is_empty() {
        let _ = on_mapping(MutationInsertIdMapping::new(BTreeMap::new())?);
        return Ok(());
    }

    let mut found_solution = false;
    if primary_writes
        .inserts
        .keys()
        .eq(shadow_writes.inserts.keys())
    {
        let identity_mapping = MutationInsertIdMapping::new(
            primary_writes.inserts.keys().map(|id| (*id, *id)).collect(),
        )?;
        if mutation_write_sets_equal(primary_writes, shadow_writes, &identity_mapping) {
            found_solution = true;
            // Shared allocation normally supplies this relation directly.
            // Prove every dimension before building a quadratic candidate
            // graph or spending the bounded search on an identical write set.
            if let std::ops::ControlFlow::Break(()) = on_mapping(identity_mapping) {
                return Ok(());
            }
        }
    }
    anyhow::ensure!(
        primary_writes.inserts.len() <= MAX_MUTATION_SHADOW_INSERT_MAPPING_CANDIDATES,
        "mutation shadow inserted-document identity mapping exceeded its bounded search"
    );

    let primary_insert_ids = encoded_insert_ids(&primary_writes.inserts)?;
    let shadow_insert_ids = encoded_insert_ids(&shadow_writes.inserts)?;
    let mut candidate_sets = Vec::with_capacity(primary_writes.inserts.len());
    for (primary_id, primary_document) in &primary_writes.inserts {
        let primary_shape = erase_insert_identities(
            &normalize_inserted_scheduler_readiness(
                primary_document,
                primary_writes.scheduled_job_inserts.contains(primary_id),
            ),
            &primary_insert_ids,
        );
        let shadow_candidates = shadow_writes
            .inserts
            .iter()
            .filter_map(|(shadow_id, shadow_document)| {
                (primary_id.tablet_id == shadow_id.tablet_id
                    && primary_id.developer_id.table() == shadow_id.developer_id.table()
                    && primary_shape
                        == erase_insert_identities(
                            &normalize_inserted_scheduler_readiness(
                                shadow_document,
                                shadow_writes.scheduled_job_inserts.contains(shadow_id),
                            ),
                            &shadow_insert_ids,
                        ))
                .then_some(*shadow_id)
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !shadow_candidates.is_empty(),
            "mutation shadow inserted-document write set differs"
        );
        candidate_sets.push((*primary_id, shadow_candidates));
    }
    // Candidate order only bounds search work; the final relation is proved
    // from complete writes rather than physical ID ordering.
    candidate_sets
        .sort_by_key(|(primary_id, shadow_candidates)| (shadow_candidates.len(), *primary_id));

    let mut search = MutationInsertIdMappingSearch {
        examined_candidates: 0,
        primary_insert_ids,
        shadow_insert_ids,
    };
    let _ = search.visit(
        &candidate_sets,
        primary_writes,
        shadow_writes,
        0,
        &mut BTreeMap::new(),
        &mut BTreeSet::new(),
        &mut |mapping| {
            found_solution = true;
            on_mapping(mapping)
        },
    )?;
    anyhow::ensure!(
        found_solution,
        "mutation shadow could not construct an inserted-document identity mapping"
    );
    Ok(())
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl QueryShadowObservation {
    pub(crate) fn new(
        transaction: &FunctionFinalTransaction,
        outcome: &FunctionOutcome,
    ) -> Result<Self, QueryShadowInvalidReason> {
        if !transaction.writes.updates.is_empty() {
            return Err(QueryShadowInvalidReason::QueryWrites);
        }
        let FunctionOutcome::Query(outcome) = outcome else {
            return Err(QueryShadowInvalidReason::OutcomeType);
        };
        let Some(handler_reads) = transaction.handler_reads.as_ref() else {
            return Err(QueryShadowInvalidReason::MissingHandlerReads);
        };
        Ok(Self {
            begin_timestamp: transaction.begin_timestamp,
            outcome: outcome.clone(),
            reads: handler_reads.reads.clone(),
        })
    }

    /// Build an observation for a completed shadow lane whose transaction and
    /// outcome are no longer needed by the caller.
    pub(crate) fn from_owned(
        mut transaction: FunctionFinalTransaction,
        outcome: FunctionOutcome,
    ) -> Result<Self, QueryShadowInvalidReason> {
        if !transaction.writes.updates.is_empty() {
            return Err(QueryShadowInvalidReason::QueryWrites);
        }
        let FunctionOutcome::Query(outcome) = outcome else {
            return Err(QueryShadowInvalidReason::OutcomeType);
        };
        let handler_reads = transaction
            .handler_reads
            .take()
            .ok_or(QueryShadowInvalidReason::MissingHandlerReads)?;
        Ok(Self {
            begin_timestamp: transaction.begin_timestamp,
            outcome,
            reads: handler_reads.reads,
        })
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn log_dimension(udf_type: UdfType, dimension: &'static str, matches: bool) {
    log_counter_with_labels(
        &STATIC_HERMES_QUERY_SHADOW_TOTAL,
        1,
        vec![
            udf_type.metric_label(),
            StaticMetricLabel::new("dimension", dimension),
            StaticMetricLabel::new("status", if matches { "match" } else { "mismatch" }),
        ],
    );
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) fn log_terminal(udf_type: UdfType, status: &'static str) {
    log_counter_with_labels(
        &STATIC_HERMES_QUERY_SHADOW_TOTAL,
        1,
        vec![
            udf_type.metric_label(),
            StaticMetricLabel::new("dimension", "execution"),
            StaticMetricLabel::new("status", status),
        ],
    );
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn js_errors_equal(left: &JsError, right: &JsError) -> bool {
    left.message == right.message && left.custom_data == right.custom_data
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn results_equal(left: &UdfOutcome, right: &UdfOutcome) -> bool {
    match (&left.result, &right.result) {
        (Ok(left), Ok(right)) => match (left.unpack(), right.unpack()) {
            (Ok(left), Ok(right)) => left == right,
            (Ok(_), Err(_)) | (Err(_), Ok(_)) | (Err(_), Err(_)) => false,
        },
        (Err(left), Err(right)) => js_errors_equal(left, right),
        (Ok(_), Err(_)) | (Err(_), Ok(_)) => false,
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl From<&JsError> for QueryShadowErrorDiagnostic {
    fn from(error: &JsError) -> Self {
        use QueryShadowErrorKind as Kind;
        // Only fixed prefixes classify the message. User-defined exception names
        // and embedded values must not become diagnostic strings or metric labels.
        let kind = [
            (
                "Function initialization timed out (maximum duration: ",
                Kind::InitializationTimeout,
            ),
            (
                "Function execution timed out (maximum duration: ",
                Kind::ExecutionTimeout,
            ),
        ]
        .into_iter()
        .find_map(|(prefix, kind)| error.message.starts_with(prefix).then_some(kind))
        .or_else(|| {
            (error.message == "Function execution exhausted its Wasm instruction budget")
                .then_some(Kind::InstructionBudget)
        })
        .or_else(|| {
            let message = error
                .message
                .strip_prefix("Uncaught ")
                .unwrap_or(&error.message);
            [
                ("TypeError", Kind::TypeError),
                ("RangeError", Kind::RangeError),
                ("ReferenceError", Kind::ReferenceError),
                ("SyntaxError", Kind::SyntaxError),
                ("EvalError", Kind::EvalError),
                ("URIError", Kind::UriError),
                ("AggregateError", Kind::AggregateError),
                ("Error", Kind::Error),
                ("ConvexError", Kind::ConvexError),
            ]
            .into_iter()
            .find_map(|(name, kind)| {
                (message == name
                    || message
                        .strip_prefix(name)
                        .is_some_and(|rest| rest.starts_with(": ")))
                .then_some(kind)
            })
        })
        .unwrap_or(Kind::Other);
        Self {
            kind,
            message_sha256: (error.message.len() <= 64 * 1024)
                .then(|| Sha256::hash(error.message.as_bytes())),
            has_custom_data: error.custom_data.is_some(),
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn result_diagnostic(left: &UdfOutcome, right: &UdfOutcome) -> Option<QueryShadowResultDiagnostic> {
    let (reason, primary_type, shadow_type) = match (&left.result, &right.result) {
        // The shared outcome encoding admits pending mutation timestamps, but
        // query results must be concrete before structural inspection.
        (Ok(left), Ok(right)) => match (
            left.unpack().and_then(|value| value.try_into_concrete()),
            right.unpack().and_then(|value| value.try_into_concrete()),
        ) {
            (Ok(left), Ok(right)) => {
                return value_result_diagnostic(&left, &right, &mut Vec::new(), &mut 4096);
            },
            (Ok(_), Err(_)) => (
                QueryShadowResultDifference::InvalidValue,
                "value",
                "invalid",
            ),
            (Err(_), Ok(_)) => (
                QueryShadowResultDifference::InvalidValue,
                "invalid",
                "value",
            ),
            (Err(_), Err(_)) => (
                QueryShadowResultDifference::InvalidValue,
                "invalid",
                "invalid",
            ),
        },
        (Err(left), Err(right)) => {
            if left.message != right.message {
                (QueryShadowResultDifference::ErrorMessage, "error", "error")
            } else if left.custom_data != right.custom_data {
                (QueryShadowResultDifference::ErrorData, "error", "error")
            } else {
                return None;
            }
        },
        (Ok(_), Err(_)) => (QueryShadowResultDifference::OutcomeType, "value", "error"),
        (Err(_), Ok(_)) => (QueryShadowResultDifference::OutcomeType, "error", "value"),
    };
    Some(QueryShadowResultDiagnostic {
        reason,
        path: Vec::new(),
        primary_type,
        shadow_type,
        primary_error: left.result.as_ref().err().map(Into::into),
        shadow_error: right.result.as_ref().err().map(Into::into),
    })
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn value_result_diagnostic(
    left: &ConvexValue,
    right: &ConvexValue,
    path: &mut Vec<usize>,
    remaining: &mut usize,
) -> Option<QueryShadowResultDiagnostic> {
    let reason = if *remaining == 0 || path.len() >= 32 {
        QueryShadowResultDifference::InspectionLimit
    } else {
        *remaining -= 1;
        match (left, right) {
            (ConvexValue::Array(left), ConvexValue::Array(right)) => {
                if left.len() == right.len() {
                    for (index, (left, right)) in left.iter().zip(right.iter()).enumerate() {
                        path.push(index);
                        let difference = value_result_diagnostic(left, right, path, remaining);
                        path.pop();
                        if difference.is_some() {
                            return difference;
                        }
                    }
                    return None;
                }
                QueryShadowResultDifference::ArrayLength
            },
            (ConvexValue::Object(left), ConvexValue::Object(right)) => {
                if left.len() == right.len() {
                    for (index, ((left_key, left), (right_key, right))) in
                        left.iter().zip(right.iter()).enumerate()
                    {
                        // BTreeMap order provides a stable ordinal without retaining
                        // arbitrary field names, which can themselves contain user data.
                        if left_key != right_key {
                            return Some(QueryShadowResultDiagnostic {
                                reason: QueryShadowResultDifference::ObjectFields,
                                path: path.clone(),
                                primary_type: "Object",
                                shadow_type: "Object",
                                primary_error: None,
                                shadow_error: None,
                            });
                        }
                        path.push(index);
                        let difference = value_result_diagnostic(left, right, path, remaining);
                        path.pop();
                        if difference.is_some() {
                            return difference;
                        }
                    }
                    return None;
                }
                QueryShadowResultDifference::ObjectFields
            },
            _ if std::mem::discriminant(left) != std::mem::discriminant(right) => {
                QueryShadowResultDifference::Type
            },
            _ if left == right => return None,
            _ => QueryShadowResultDifference::Value,
        }
    };
    Some(QueryShadowResultDiagnostic {
        reason,
        path: path.clone(),
        primary_type: left.type_name(),
        shadow_type: right.type_name(),
        primary_error: None,
        shadow_error: None,
    })
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn host_operation_trace_counts(
    trace: &HostOperationTrace,
) -> BTreeMap<(LogicalHostOperation, LogicalHostOperationStatus), u64> {
    let mut counts = BTreeMap::new();
    for entry in trace.entries().unwrap_or_default() {
        let count = counts
            .entry((entry.operation(), entry.status()))
            .or_insert(0u64);
        *count = count
            .checked_add(1)
            .expect("host-operation trace count overflow");
    }
    counts
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn compare_host_operation_traces(
    primary: &HostOperationTrace,
    shadow: &HostOperationTrace,
) -> (bool, Option<QueryShadowHostOperationTraceDiagnostic>) {
    let primary_comparison_eligible = primary.is_comparison_eligible();
    let shadow_comparison_eligible = shadow.is_comparison_eligible();

    // Promise scheduling can interleave independent host calls differently in
    // two JavaScript engines. Their raw completion order is not an observable
    // UDF result; results, reads, query journals, writes, logs, and errors are
    // compared separately. Preserve operation and status multiplicity while
    // allowing only this semantically irrelevant reordering.
    let mut primary_entries = primary.entries().unwrap_or_default().to_vec();
    let mut shadow_entries = shadow.entries().unwrap_or_default().to_vec();
    primary_entries.sort_unstable();
    shadow_entries.sort_unstable();
    let matches = primary_comparison_eligible
        && shadow_comparison_eligible
        && primary_entries == shadow_entries;
    if matches {
        return (true, None);
    }

    // Count only mismatches. Matching shadow comparisons stay on the existing
    // sorted-vector path and do not allocate diagnostic maps.
    let primary_counts = host_operation_trace_counts(primary);
    let shadow_counts = host_operation_trace_counts(shadow);
    let keys = primary_counts
        .keys()
        .chain(shadow_counts.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let differing_count = keys
        .iter()
        .filter(|key| primary_counts.get(*key) != shadow_counts.get(*key))
        .count();
    let differing_counts = keys
        .into_iter()
        .filter_map(|(operation, status)| {
            let primary_count = primary_counts
                .get(&(operation, status))
                .copied()
                .unwrap_or(0);
            let shadow_count = shadow_counts
                .get(&(operation, status))
                .copied()
                .unwrap_or(0);
            (primary_count != shadow_count).then_some(QueryShadowHostOperationCountDifference {
                operation,
                status,
                primary_count,
                shadow_count,
            })
        })
        .take(QUERY_SHADOW_HOST_OPERATION_DIAGNOSTIC_LIMIT)
        .collect::<Vec<_>>();
    let omitted_differing_count = differing_count
        .saturating_sub(differing_counts.len())
        .try_into()
        .expect("host-operation diagnostic difference count exceeds u64");
    (
        false,
        Some(QueryShadowHostOperationTraceDiagnostic {
            primary_comparison_eligible,
            shadow_comparison_eligible,
            differing_counts,
            omitted_differing_count,
        }),
    )
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn host_operation_traces_equal(left: &HostOperationTrace, right: &HostOperationTrace) -> bool {
    compare_host_operation_traces(left, right).0
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn log_lines_equal(left: &LogLines, right: &LogLines) -> bool {
    let mut left = left.iter();
    let mut right = right.iter();
    loop {
        match (left.next(), right.next()) {
            (Some(left), Some(right)) if log_line_equal(left, right) => {},
            (None, None) => return true,
            (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) => return false,
        }
    }
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn log_line_equal(left: &LogLine, right: &LogLine) -> bool {
    match (left, right) {
        (LogLine::Structured(left), LogLine::Structured(right)) => {
            left.messages == right.messages
                && left.level == right.level
                && left.is_truncated == right.is_truncated
                && left.system_metadata == right.system_metadata
        },
        (
            LogLine::SubFunction {
                path: left_path,
                log_lines: left_log_lines,
            },
            LogLine::SubFunction {
                path: right_path,
                log_lines: right_log_lines,
            },
        ) => left_path == right_path && log_lines_equal(left_log_lines, right_log_lines),
        (LogLine::Structured(_), LogLine::SubFunction { .. })
        | (LogLine::SubFunction { .. }, LogLine::Structured(_)) => false,
    }
}

#[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
pub(crate) type MutationShadowTestObservation = MutationShadowObservation;

/// Compare mutation-shadow observations in the test-only paired-lane mode.
#[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
pub(crate) fn compare_mutation_for_test(
    primary: MutationShadowTestObservation,
    shadow: MutationShadowTestObservation,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        primary.begin_timestamp == shadow.begin_timestamp,
        "test-only mutation shadow did not execute at the same database snapshot"
    );
    let mut first_mapping = None;
    let mut matching_mapping = None;
    visit_mutation_insert_id_mappings(&primary.writes, &shadow.writes, |mapping| {
        let result_matches = mutation_results_equal(&primary.outcome, &shadow.outcome, &mapping);
        let read_dependencies_match = primary
            .reads
            .has_same_read_dependencies_with_lane_local_insert_ids(
                &shadow.reads,
                &mapping.document_ids,
            );
        let write_set_matches =
            mutation_write_sets_equal(&primary.writes, &shadow.writes, &mapping);
        if first_mapping.is_none() {
            first_mapping = Some(mapping.clone());
        }
        if result_matches && read_dependencies_match && write_set_matches {
            matching_mapping = Some(mapping);
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    })?;
    let insert_id_mapping = matching_mapping
        .or(first_mapping)
        .expect("mutation shadow identity mapping search returned no solutions");
    anyhow::ensure!(
        mutation_results_equal(&primary.outcome, &shadow.outcome, &insert_id_mapping),
        "test-only mutation shadow result differs"
    );
    anyhow::ensure!(
        primary.outcome.journal == shadow.outcome.journal,
        "test-only mutation shadow journal differs"
    );
    let read_dependencies_match = primary
        .reads
        .has_same_read_dependencies_with_lane_local_insert_ids(
            &shadow.reads,
            &insert_id_mapping.document_ids,
        );
    anyhow::ensure!(
        read_dependencies_match,
        "test-only mutation shadow handler reads differ: {:?}",
        primary.reads.comparison_diagnostic(&shadow.reads)
    );
    anyhow::ensure!(
        primary.outcome.path == shadow.outcome.path
            && primary.outcome.arguments == shadow.outcome.arguments
            && primary.outcome.identity == shadow.outcome.identity
            && primary.outcome.rng_seed == shadow.outcome.rng_seed
            && primary.outcome.unix_timestamp == shadow.outcome.unix_timestamp
            && primary.outcome.udf_server_version == shadow.outcome.udf_server_version,
        "test-only mutation shadow invocation inputs differ"
    );
    anyhow::ensure!(
        host_operation_traces_equal(
            &primary.outcome.host_operation_trace,
            &shadow.outcome.host_operation_trace,
        ),
        "test-only mutation shadow host-operation trace differs"
    );
    anyhow::ensure!(
        primary.outcome.host_operation_error == shadow.outcome.host_operation_error
            && primary.outcome.observed_identity == shadow.outcome.observed_identity
            && primary.outcome.observed_time == shadow.outcome.observed_time
            && primary.outcome.observed_rng == shadow.outcome.observed_rng
            && primary.outcome.log_lines == shadow.outcome.log_lines
            && primary.outcome.audit_log_lines == shadow.outcome.audit_log_lines,
        "test-only mutation shadow side-effect observation differs"
    );
    anyhow::ensure!(
        mutation_write_sets_equal(&primary.writes, &shadow.writes, &insert_id_mapping),
        "test-only mutation shadow normalized write set differs"
    );
    Ok(())
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) fn compare_query(
    primary: QueryShadowObservation,
    shadow: QueryShadowObservation,
) -> QueryShadowComparison {
    #[cfg(feature = "testing")]
    QUERY_SHADOW_TEST_TRACE_DIAGNOSTICS
        .lock()
        .push(QueryShadowTestTraceDiagnostic {
            primary: primary
                .outcome
                .host_operation_trace
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| (entry.operation(), entry.status()))
                .collect(),
            shadow: shadow
                .outcome
                .host_operation_trace
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| (entry.operation(), entry.status()))
                .collect(),
            read_dependencies: primary.reads.comparison_diagnostic(&shadow.reads),
        });
    let (host_operation_trace_matches, host_operation_trace_diagnostic) =
        compare_host_operation_traces(
            &primary.outcome.host_operation_trace,
            &shadow.outcome.host_operation_trace,
        );
    let result_matches = results_equal(&primary.outcome, &shadow.outcome);
    let result_diagnostic = if result_matches {
        None
    } else {
        result_diagnostic(&primary.outcome, &shadow.outcome)
    };
    let comparison = QueryShadowComparison {
        snapshot_matches: primary.begin_timestamp == shadow.begin_timestamp,
        result_matches,
        result_diagnostic,
        journal_matches: primary.outcome.journal == shadow.outcome.journal,
        read_dependencies_match: primary.reads.has_same_read_dependencies(&shadow.reads),
        invocation_inputs_match: primary.outcome.path == shadow.outcome.path
            && primary.outcome.arguments == shadow.outcome.arguments
            && primary.outcome.identity == shadow.outcome.identity
            && primary.outcome.rng_seed == shadow.outcome.rng_seed
            && primary.outcome.unix_timestamp == shadow.outcome.unix_timestamp
            && primary.outcome.udf_server_version == shadow.outcome.udf_server_version,
        host_operation_trace_matches,
        host_operation_error_matches: primary.outcome.host_operation_error
            == shadow.outcome.host_operation_error,
        observed_identity_matches: primary.outcome.observed_identity
            == shadow.outcome.observed_identity,
        observed_time_matches: primary.outcome.observed_time == shadow.outcome.observed_time,
        observed_rng_matches: primary.outcome.observed_rng == shadow.outcome.observed_rng,
        log_lines_match: log_lines_equal(&primary.outcome.log_lines, &shadow.outcome.log_lines),
        audit_log_lines_match: primary.outcome.audit_log_lines == shadow.outcome.audit_log_lines,
        write_set_matches: true,
        inserted_document_identity: QueryShadowInsertedDocumentIdentity::NotApplicable,
        host_operation_trace_diagnostic,
    };
    log_comparison_dimensions(UdfType::Query, &comparison);
    comparison
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) fn compare_mutation(
    primary: MutationShadowObservation,
    shadow: MutationShadowObservation,
) -> QueryShadowComparison {
    let insert_identity_required =
        !primary.writes.inserts.is_empty() || !shadow.writes.inserts.is_empty();
    let mut any_result_matches = false;
    let mut any_read_dependencies_match = false;
    let mut any_write_set_matches = false;
    let mut one_mapping_matches_all = false;
    let (result_matches, read_dependencies_match, write_set_matches, inserted_document_identity) =
        match visit_mutation_insert_id_mappings(&primary.writes, &shadow.writes, |mapping| {
            let result_matches =
                mutation_results_equal(&primary.outcome, &shadow.outcome, &mapping);
            let read_dependencies_match = primary
                .reads
                .has_same_read_dependencies_with_lane_local_insert_ids(
                    &shadow.reads,
                    &mapping.document_ids,
                );
            let write_set_matches =
                mutation_write_sets_equal(&primary.writes, &shadow.writes, &mapping);
            any_result_matches |= result_matches;
            any_read_dependencies_match |= read_dependencies_match;
            any_write_set_matches |= write_set_matches;
            one_mapping_matches_all |=
                result_matches && read_dependencies_match && write_set_matches;
            if one_mapping_matches_all {
                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        }) {
            Ok(()) => {
                // A complete match requires one identity relation to explain
                // the result, reads, and writes together. If separate
                // relations explain each dimension, attribute the
                // inconsistency to the write/identity relation rather than
                // reporting a false complete match.
                let inserted_document_identity = if !insert_identity_required {
                    QueryShadowInsertedDocumentIdentity::NotRequired
                } else if one_mapping_matches_all {
                    QueryShadowInsertedDocumentIdentity::Matched
                } else {
                    QueryShadowInsertedDocumentIdentity::Inconsistent
                };
                (
                    any_result_matches,
                    any_read_dependencies_match,
                    any_write_set_matches
                        && (one_mapping_matches_all
                            || !(any_result_matches && any_read_dependencies_match)),
                    inserted_document_identity,
                )
            },
            Err(_) => {
                // Failure to relate inserted IDs is a write-set difference.
                // Results and reads may still agree directly, so retain their
                // independent attribution instead of collapsing all three.
                let empty_mapping = MutationInsertIdMapping::default();
                (
                    mutation_results_equal(&primary.outcome, &shadow.outcome, &empty_mapping),
                    primary
                        .reads
                        .has_same_read_dependencies_with_lane_local_insert_ids(
                            &shadow.reads,
                            &empty_mapping.document_ids,
                        ),
                    false,
                    QueryShadowInsertedDocumentIdentity::Unresolved,
                )
            },
        };
    let (host_operation_trace_matches, host_operation_trace_diagnostic) =
        compare_host_operation_traces(
            &primary.outcome.host_operation_trace,
            &shadow.outcome.host_operation_trace,
        );
    let comparison = QueryShadowComparison {
        snapshot_matches: primary.begin_timestamp == shadow.begin_timestamp,
        result_matches,
        // Only error metadata is meaningful before inserted-ID normalization.
        // Successful mutation values still require the proven mapping for inspection.
        result_diagnostic: if !result_matches
            && (primary.outcome.result.is_err() || shadow.outcome.result.is_err())
        {
            result_diagnostic(&primary.outcome, &shadow.outcome)
        } else {
            None
        },
        journal_matches: primary.outcome.journal == shadow.outcome.journal,
        read_dependencies_match,
        invocation_inputs_match: primary.outcome.path == shadow.outcome.path
            && primary.outcome.arguments == shadow.outcome.arguments
            && primary.outcome.identity == shadow.outcome.identity
            && primary.outcome.rng_seed == shadow.outcome.rng_seed
            && primary.outcome.unix_timestamp == shadow.outcome.unix_timestamp
            && primary.outcome.udf_server_version == shadow.outcome.udf_server_version,
        host_operation_trace_matches,
        host_operation_error_matches: primary.outcome.host_operation_error
            == shadow.outcome.host_operation_error,
        observed_identity_matches: primary.outcome.observed_identity
            == shadow.outcome.observed_identity,
        observed_time_matches: primary.outcome.observed_time == shadow.outcome.observed_time,
        observed_rng_matches: primary.outcome.observed_rng == shadow.outcome.observed_rng,
        log_lines_match: log_lines_equal(&primary.outcome.log_lines, &shadow.outcome.log_lines),
        audit_log_lines_match: primary.outcome.audit_log_lines == shadow.outcome.audit_log_lines,
        write_set_matches,
        inserted_document_identity,
        host_operation_trace_diagnostic,
    };
    log_comparison_dimensions(UdfType::Mutation, &comparison);
    comparison
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
fn log_comparison_dimensions(udf_type: UdfType, comparison: &QueryShadowComparison) {
    for (dimension, matches) in [
        ("snapshot", comparison.snapshot_matches),
        ("result", comparison.result_matches),
        ("query_journal", comparison.journal_matches),
        ("read_dependencies", comparison.read_dependencies_match),
        ("invocation_inputs", comparison.invocation_inputs_match),
        (
            "host_operation_trace",
            comparison.host_operation_trace_matches,
        ),
        (
            "host_operation_error",
            comparison.host_operation_error_matches,
        ),
        ("observed_identity", comparison.observed_identity_matches),
        ("observed_time", comparison.observed_time_matches),
        ("observed_rng", comparison.observed_rng_matches),
        ("log_lines", comparison.log_lines_match),
        ("audit_log_lines", comparison.audit_log_lines_match),
        ("write_set", comparison.write_set_matches),
    ] {
        log_dimension(udf_type, dimension, matches);
    }
    log_counter_with_labels(
        &STATIC_HERMES_QUERY_SHADOW_TOTAL,
        1,
        vec![
            udf_type.metric_label(),
            StaticMetricLabel::new("dimension", "inserted_document_identity"),
            StaticMetricLabel::new("status", comparison.inserted_document_identity.metric_key()),
        ],
    );
}

#[cfg(test)]
mod tests {
    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    use std::collections::{
        BTreeMap,
        BTreeSet,
    };
    use std::time::Duration;

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    use common::{
        audit_log_lines::AuditLogLine,
        bootstrap_model::index::database_index::IndexedFields,
        components::{
            ExportPath,
            PublicFunctionPath,
        },
        document::{
            CreationTime,
            PendingDocumentUpdate,
            ResolvedDocument,
        },
        identity::InertIdentity,
        index::{
            IndexKey,
            IndexKeyBytes,
        },
        interval::{
            Interval,
            IntervalSet,
        },
        query::{
            Cursor,
            CursorPosition,
        },
        query_journal::QueryJournal,
        runtime::UnixTimestamp,
        types::TabletIndexName,
    };
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    use common::{
        components::{
            ComponentFunctionPath,
            ComponentPath,
        },
        errors::{
            JsError,
            JsFrames,
        },
        log_lines::{
            LogLevel,
            LogLine,
            LogLineStructured,
            LogLines,
            SystemLogMetadata,
        },
    };
    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    use database::{
        reads::IndexReads,
        ReadSet,
        TransactionReadSize,
    };
    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    use sync_types::{
        types::SerializedArgs,
        Timestamp,
    };
    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    use udf::{
        FunctionOutcome,
        SyscallTrace,
        UdfOutcome,
    };
    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    use udf::{
        HostOperation,
        HostOperationErrorV1,
    };
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    use udf::{
        HostOperationTrace,
        LogicalHostOperation,
        LogicalHostOperationStatus,
    };
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    use value::ConvexValue;
    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    use value::{
        ConvexArray,
        ConvexObject,
        DeveloperDocumentId,
        InternalId,
        JsonPackedValue,
        PendingValue,
        ResolvedDocumentId,
        TableMapping,
        TableName,
        TableNamespace,
        TableNumber,
        TabletId,
    };

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    use super::log_lines_equal;
    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    use super::QueryShadowHostOperationCountDifference;
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    use super::{
        compare_host_operation_traces,
        host_operation_traces_equal,
        js_errors_equal,
        QUERY_SHADOW_HOST_OPERATION_DIAGNOSTIC_LIMIT,
    };
    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    use super::{
        compare_mutation,
        compare_mutation_for_test,
        compare_query,
        MutationShadowTestObservation,
        QueryShadowObservation,
    };
    use super::{
        QueryShadowComparison,
        QueryShadowFailureReason,
        QueryShadowFailureStage,
        QueryShadowInsertedDocumentIdentity,
        QueryShadowReport,
        QueryShadowRouteKey,
        QueryShadowTerminal,
    };

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    fn nested_log_lines(
        path: &str,
        first_message: &str,
        level: LogLevel,
        is_truncated: bool,
        system_code: &str,
        first_timestamp_nanos: u64,
        second_timestamp_nanos: u64,
        reverse_order: bool,
    ) -> LogLines {
        let path = ComponentFunctionPath {
            component: ComponentPath::root(),
            udf_path: path.parse().unwrap(),
        }
        .canonicalize();
        let line = |message: &str, timestamp_nanos: u64| {
            LogLine::Structured(LogLineStructured {
                messages: vec![message.to_owned()].into(),
                level: level.clone(),
                is_truncated,
                timestamp: common::runtime::UnixTimestamp::from_nanos(timestamp_nanos),
                system_metadata: Some(SystemLogMetadata {
                    code: system_code.to_owned(),
                }),
            })
        };
        let first_line = line(first_message, first_timestamp_nanos);
        let second_line = line("second", second_timestamp_nanos);
        let log_lines = if reverse_order {
            vec![second_line, first_line]
        } else {
            vec![first_line, second_line]
        };
        vec![LogLine::SubFunction {
            path,
            log_lines: log_lines.into(),
        }]
        .into()
    }

    #[test]
    fn route_id_accepts_only_sha256_digest_form() {
        let route_key = QueryShadowRouteKey::new(&"A".repeat(64), &"B".repeat(64)).unwrap();
        assert_eq!(
            route_key.as_str(),
            format!("{}:{}", "a".repeat(64), "b".repeat(64))
        );
        assert_eq!(route_key.generation_sha256(), "a".repeat(64));
        assert_eq!(route_key.route_sha256(), "b".repeat(64));
        assert_eq!(
            QueryShadowRouteKey::parse(&route_key.as_str().to_ascii_uppercase())
                .unwrap()
                .as_str(),
            route_key.as_str()
        );
        assert!(QueryShadowRouteKey::new("generation", &"b".repeat(64)).is_err());
        assert!(QueryShadowRouteKey::new(&"a".repeat(64), "function/path").is_err());
        assert!(QueryShadowRouteKey::new(&"a".repeat(63), &"b".repeat(64)).is_err());
        assert!(QueryShadowRouteKey::new(&"a".repeat(64), &"g".repeat(64)).is_err());
        assert!(QueryShadowRouteKey::parse("not-a-query-shadow-cursor").is_err());
        assert_eq!(format!("{route_key:?}"), "QueryShadowRouteKey(..)");
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn semantic_error_comparison_ignores_runtime_specific_frames() {
        let left = JsError::from_message("same".to_owned());
        let mut right = JsError::from_message("same".to_owned());
        right.frames = Some(JsFrames(Default::default()));
        assert!(js_errors_equal(&left, &right));

        let different = JsError::from_message("different".to_owned());
        assert!(!js_errors_equal(&left, &different));
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn shadow_error_diagnostic_is_bounded_and_contains_no_message_or_custom_data() {
        use super::{
            QueryShadowErrorDiagnostic,
            QueryShadowErrorKind,
        };
        for (message, kind) in [
            ("TypeError: private-value", QueryShadowErrorKind::TypeError),
            (
                "Uncaught RangeError: private-value",
                QueryShadowErrorKind::RangeError,
            ),
            ("ReferenceError", QueryShadowErrorKind::ReferenceError),
            (
                "TypeErrorSuffix: private-value",
                QueryShadowErrorKind::Other,
            ),
            ("private-value", QueryShadowErrorKind::Other),
            (
                "Function execution timed out (maximum duration: 5s)",
                QueryShadowErrorKind::ExecutionTimeout,
            ),
            (
                "Function initialization timed out (maximum duration: 5s)",
                QueryShadowErrorKind::InitializationTimeout,
            ),
            (
                "Function execution exhausted its Wasm instruction budget",
                QueryShadowErrorKind::InstructionBudget,
            ),
        ] {
            let error = JsError::convex_error(
                message.to_owned(),
                ConvexValue::String("private-data".try_into().unwrap()),
            );
            let diagnostic = QueryShadowErrorDiagnostic::from(&error);
            assert_eq!(diagnostic.kind, kind);
            assert_eq!(
                diagnostic.message_sha256,
                Some(common::sha256::Sha256::hash(message.as_bytes()))
            );
            assert!(diagnostic.has_custom_data);
            assert!(!format!("{diagnostic:?}").contains("private-"));
        }
        let mut error = JsError::from_message("я".repeat(32 * 1024));
        assert!(QueryShadowErrorDiagnostic::from(&error)
            .message_sha256
            .is_some());
        error.message.push('я');
        let diagnostic = QueryShadowErrorDiagnostic::from(&error);
        assert!(diagnostic.message_sha256.is_none());
        assert!(!diagnostic.has_custom_data);
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn shadow_error_diagnostic_preserves_both_runtime_directions_and_mutation_errors(
    ) -> anyhow::Result<()> {
        for error_in_primary in [false, true] {
            let mut primary = comparator_query_observation()?;
            let mut shadow = comparator_query_observation()?;
            let lane = if error_in_primary {
                &mut primary
            } else {
                &mut shadow
            };
            lane.outcome.result = Err(JsError::from_message("TypeError: fixture".to_owned()));
            let comparison = compare_query(primary, shadow);
            assert!(!comparison.result_matches);
            let diagnostic = comparison.result_diagnostic.unwrap();
            assert_eq!(diagnostic.primary_error.is_some(), error_in_primary);
            assert_eq!(diagnostic.shadow_error.is_some(), !error_in_primary);
        }
        let primary = mutation_observation(vec![])?;
        let shadow = mutation_observation_with(
            vec![],
            mutation_outcome_with_error_data(serde_json::json!({"private": "value"}))?,
            Timestamp::MIN,
        )?;
        let comparison = compare_mutation(primary, shadow);
        assert!(!comparison.result_matches);
        let diagnostic = comparison.result_diagnostic.unwrap();
        assert_eq!(
            diagnostic.reason,
            super::QueryShadowResultDifference::OutcomeType
        );
        assert!(diagnostic.primary_error.is_none());
        assert!(diagnostic.shadow_error.unwrap().has_custom_data);
        Ok(())
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn log_comparison_ignores_lane_local_timestamps() {
        let primary = nested_log_lines(
            "nested:default",
            "first",
            LogLevel::Info,
            true,
            "code",
            1,
            2,
            false,
        );
        let shadow = nested_log_lines(
            "nested:default",
            "first",
            LogLevel::Info,
            true,
            "code",
            10,
            20,
            false,
        );

        assert!(log_lines_equal(&primary, &shadow));
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn log_comparison_preserves_structured_content_and_nested_paths() {
        let primary = nested_log_lines(
            "nested:default",
            "first",
            LogLevel::Info,
            true,
            "code",
            1,
            2,
            false,
        );
        let different_message = nested_log_lines(
            "nested:default",
            "changed",
            LogLevel::Info,
            true,
            "code",
            1,
            2,
            false,
        );
        let different_level = nested_log_lines(
            "nested:default",
            "first",
            LogLevel::Warn,
            true,
            "code",
            1,
            2,
            false,
        );
        let different_truncation = nested_log_lines(
            "nested:default",
            "first",
            LogLevel::Info,
            false,
            "code",
            1,
            2,
            false,
        );
        let different_metadata = nested_log_lines(
            "nested:default",
            "first",
            LogLevel::Info,
            true,
            "different_code",
            1,
            2,
            false,
        );
        let different_path = nested_log_lines(
            "other:default",
            "first",
            LogLevel::Info,
            true,
            "code",
            1,
            2,
            false,
        );
        let reordered = nested_log_lines(
            "nested:default",
            "first",
            LogLevel::Info,
            true,
            "code",
            1,
            2,
            true,
        );

        assert!(!log_lines_equal(&primary, &different_message));
        assert!(!log_lines_equal(&primary, &different_level));
        assert!(!log_lines_equal(&primary, &different_truncation));
        assert!(!log_lines_equal(&primary, &different_metadata));
        assert!(!log_lines_equal(&primary, &different_path));
        assert!(!log_lines_equal(&primary, &reordered));
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn host_operation_trace_comparison_requires_complete_known_operation_multiset() {
        let left = HostOperationTrace::from(vec![
            LogicalHostOperation::DatabaseGet,
            LogicalHostOperation::DatabaseQueryStreamNext,
        ]);
        let same = left.clone();
        let reordered = HostOperationTrace::from(vec![
            LogicalHostOperation::DatabaseQueryStreamNext,
            LogicalHostOperation::DatabaseGet,
        ]);
        let duplicated = HostOperationTrace::from(vec![
            LogicalHostOperation::DatabaseGet,
            LogicalHostOperation::DatabaseQueryStreamNext,
            LogicalHostOperation::DatabaseGet,
        ]);
        let mut status_mismatch = HostOperationTrace::for_query_shadow();
        status_mismatch.record(
            LogicalHostOperation::DatabaseGet,
            LogicalHostOperationStatus::Failure,
        );
        status_mismatch.record(
            LogicalHostOperation::DatabaseQueryStreamNext,
            LogicalHostOperationStatus::Success,
        );
        let disabled = HostOperationTrace::default();
        let mut pending = HostOperationTrace::for_query_shadow();
        let _ = pending.start(LogicalHostOperation::DatabaseGet);
        let mut unknown = HostOperationTrace::for_query_shadow();
        unknown.record(
            LogicalHostOperation::UnknownSyncSyscall,
            LogicalHostOperationStatus::Failure,
        );

        assert!(host_operation_traces_equal(&left, &same));
        assert!(host_operation_traces_equal(&left, &reordered));
        assert!(!host_operation_traces_equal(&left, &duplicated));
        assert!(!host_operation_traces_equal(&left, &status_mismatch));
        assert!(!host_operation_traces_equal(&disabled, &disabled));
        assert!(!host_operation_traces_equal(&left, &pending));
        assert!(!host_operation_traces_equal(&left, &unknown));

        let (_, diagnostic) = compare_host_operation_traces(&disabled, &disabled);
        let diagnostic = diagnostic.expect("ineligible traces require a diagnostic");
        assert!(!diagnostic.primary_comparison_eligible);
        assert!(!diagnostic.shadow_comparison_eligible);
        assert!(diagnostic.differing_counts.is_empty());
        assert_eq!(diagnostic.omitted_differing_count, 0);

        let primary = HostOperationTrace::from(vec![
            LogicalHostOperation::AuditLog,
            LogicalHostOperation::CancelJob,
            LogicalHostOperation::ComponentArgument,
            LogicalHostOperation::CreateFunctionHandle,
            LogicalHostOperation::DatabaseCount,
            LogicalHostOperation::DatabaseDelete,
            LogicalHostOperation::DatabaseGet,
            LogicalHostOperation::DatabaseInsert,
            LogicalHostOperation::DatabaseNormalizeId,
            LogicalHostOperation::DatabasePatch,
            LogicalHostOperation::DatabaseQueryCleanup,
            LogicalHostOperation::DatabaseQueryPage,
            LogicalHostOperation::DatabaseQueryStream,
            LogicalHostOperation::DatabaseQueryStreamNext,
            LogicalHostOperation::DatabaseReplace,
            LogicalHostOperation::DeploymentMetadata,
            LogicalHostOperation::FunctionMetadata,
        ]);
        let shadow = HostOperationTrace::for_query_shadow();
        let (_, diagnostic) = compare_host_operation_traces(&primary, &shadow);
        let diagnostic = diagnostic.expect("different traces require a diagnostic");
        assert_eq!(
            diagnostic.differing_counts.len(),
            QUERY_SHADOW_HOST_OPERATION_DIAGNOSTIC_LIMIT
        );
        assert_eq!(diagnostic.omitted_differing_count, 1);
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn test_document_id(number: u8) -> ResolvedDocumentId {
        ResolvedDocumentId::new(
            TabletId::MIN,
            DeveloperDocumentId::new(TableNumber::MIN, InternalId([number; 16])),
        )
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn test_document_id_in_table(
        tablet_number: u8,
        table_number: u32,
        document_number: u8,
    ) -> ResolvedDocumentId {
        ResolvedDocumentId::new(
            TabletId(InternalId([tablet_number; 16])),
            DeveloperDocumentId::new(
                TableNumber::try_from(table_number).expect("test table number must be valid"),
                InternalId([document_number; 16]),
            ),
        )
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn test_document(
        id: ResolvedDocumentId,
        body: serde_json::Value,
    ) -> anyhow::Result<ResolvedDocument> {
        test_document_with_creation_time(id, body, 1.0)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn test_document_with_creation_time(
        id: ResolvedDocumentId,
        body: serde_json::Value,
        creation_time: f64,
    ) -> anyhow::Result<ResolvedDocument> {
        ResolvedDocument::new(
            id,
            CreationTime::try_from(creation_time)?,
            ConvexObject::try_from(body)?,
        )
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn existing_update(
        id: ResolvedDocumentId,
        old_body: serde_json::Value,
        new_body: serde_json::Value,
    ) -> anyhow::Result<PendingDocumentUpdate> {
        Ok(PendingDocumentUpdate::new(
            id,
            Some((test_document(id, old_body)?, Timestamp::MIN)),
            Some(test_document(id, new_body)?.into()),
        ))
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn existing_delete(
        id: ResolvedDocumentId,
        old_body: serde_json::Value,
    ) -> anyhow::Result<PendingDocumentUpdate> {
        Ok(PendingDocumentUpdate::new(
            id,
            Some((test_document(id, old_body)?, Timestamp::MIN)),
            None,
        ))
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn inserted_document(
        id: ResolvedDocumentId,
        body: serde_json::Value,
    ) -> anyhow::Result<PendingDocumentUpdate> {
        inserted_document_with_creation_time(id, body, 1.0)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn inserted_document_with_creation_time(
        id: ResolvedDocumentId,
        body: serde_json::Value,
        creation_time: f64,
    ) -> anyhow::Result<PendingDocumentUpdate> {
        Ok(PendingDocumentUpdate::new(
            id,
            None,
            Some(test_document_with_creation_time(id, body, creation_time)?.into()),
        ))
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn scheduled_job_insert(
        id: ResolvedDocumentId,
        args_id: ResolvedDocumentId,
        next_ts: i64,
        original_scheduled_ts: i64,
    ) -> anyhow::Result<PendingDocumentUpdate> {
        scheduled_job_insert_with_creation_time(id, args_id, next_ts, original_scheduled_ts, 1.0)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn scheduled_job_insert_with_creation_time(
        id: ResolvedDocumentId,
        args_id: ResolvedDocumentId,
        next_ts: i64,
        original_scheduled_ts: i64,
        creation_time: f64,
    ) -> anyhow::Result<PendingDocumentUpdate> {
        inserted_document_with_creation_time(
            id,
            serde_json::json!({
                "argsId": args_id.developer_id.encode(),
                "attempts": {
                    "occErrors": 0,
                    "systemErrors": 0,
                },
                "component": "",
                "nextTs": next_ts,
                "originalScheduledTs": original_scheduled_ts,
                "state": { "type": "pending" },
                "udfPath": "jobs:run",
            }),
            creation_time,
        )
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn scheduled_args_insert(
        id: ResolvedDocumentId,
        operation_id: ResolvedDocumentId,
        stable_argument: &str,
        creation_time: f64,
    ) -> anyhow::Result<PendingDocumentUpdate> {
        let args = ConvexArray::try_from(serde_json::json!([{
            "operationId": operation_id.developer_id.encode(),
            "stableArgument": stable_argument,
        }]))?;
        let bytes = ConvexValue::Bytes(args.json_serialize()?.into_bytes().try_into()?);
        inserted_document_with_creation_time(
            id,
            serde_json::json!({ "args": bytes.to_internal_json() }),
            creation_time,
        )
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn insert_id_read_set(ids: &[ResolvedDocumentId]) -> ReadSet {
        let mut indexed = BTreeMap::new();
        for id in ids {
            indexed
                .entry(TabletIndexName::by_id(id.tablet_id))
                .or_insert_with(|| IndexReads {
                    fields: IndexedFields::by_id(),
                    intervals: IntervalSet::new(),
                    stack_traces: None,
                })
                .intervals
                .add(Interval::prefix(
                    IndexKey::new(vec![], (*id).into()).to_bytes().into(),
                ));
        }
        ReadSet::new(indexed, BTreeMap::new())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn scheduled_jobs_table_mapping() -> TableMapping {
        let mut table_mapping = TableMapping::new();
        table_mapping.insert(
            TabletId::MIN,
            TableNamespace::Global,
            TableNumber::MIN,
            model::scheduled_jobs::SCHEDULED_JOBS_TABLE.clone(),
        );
        table_mapping
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn user_table_mapping() -> TableMapping {
        let mut table_mapping = TableMapping::new();
        table_mapping.insert(
            TabletId::MIN,
            TableNamespace::Global,
            TableNumber::MIN,
            "shadowTestDocuments"
                .parse::<TableName>()
                .expect("test table name must be valid"),
        );
        table_mapping
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn mutation_outcome() -> anyhow::Result<FunctionOutcome> {
        Ok(FunctionOutcome::Mutation(UdfOutcome {
            path: PublicFunctionPath::RootExport("test:default".parse::<ExportPath>()?)
                .debug_into_component_path(),
            arguments: SerializedArgs::from_args(vec![])?,
            identity: InertIdentity::Unknown,
            observed_identity: false,
            rng_seed: [0; 32],
            observed_rng: false,
            unix_timestamp: UnixTimestamp::from_nanos(0),
            observed_time: false,
            log_lines: vec![].into(),
            journal: QueryJournal::new(),
            audit_log_lines: vec![].into(),
            result: Ok(JsonPackedValue::pack(PendingValue::Concrete(
                ConvexValue::Null,
            ))),
            host_operation_error: None,
            host_operation_trace: HostOperationTrace::from(vec![
                LogicalHostOperation::DatabasePatch,
                LogicalHostOperation::DatabaseReplace,
                LogicalHostOperation::DatabaseDelete,
            ]),
            syscall_trace: SyscallTrace::new(),
            udf_server_version: None,
            memory_in_mb: 0,
            user_execution_time: Some(Duration::ZERO),
        }))
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn mutation_outcome_with_result(result: serde_json::Value) -> anyhow::Result<FunctionOutcome> {
        let FunctionOutcome::Mutation(mut outcome) = mutation_outcome()? else {
            unreachable!("mutation_outcome returned a non-mutation outcome")
        };
        outcome.result = Ok(JsonPackedValue::pack(PendingValue::from_uncommitted_json(
            result,
        )?));
        Ok(FunctionOutcome::Mutation(outcome))
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn mutation_outcome_with_error_data(
        custom_data: serde_json::Value,
    ) -> anyhow::Result<FunctionOutcome> {
        let FunctionOutcome::Mutation(mut outcome) = mutation_outcome()? else {
            unreachable!("mutation_outcome returned a non-mutation outcome")
        };
        let mut error = JsError::from_message("same".to_owned());
        error.custom_data = Some(ConvexValue::try_from(custom_data)?);
        outcome.result = Err(error);
        Ok(FunctionOutcome::Mutation(outcome))
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn mutation_observation(
        updates: Vec<PendingDocumentUpdate>,
    ) -> anyhow::Result<MutationShadowTestObservation> {
        mutation_observation_with(updates, mutation_outcome()?, Timestamp::MIN)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn symmetric_insert_observation(
        first_document_number: u8,
        last_document_number: u8,
        creation_time: f64,
    ) -> anyhow::Result<MutationShadowTestObservation> {
        mutation_observation(
            (first_document_number..=last_document_number)
                .map(|number| {
                    inserted_document_with_creation_time(
                        test_document_id(number),
                        serde_json::json!({ "value": "same" }),
                        creation_time,
                    )
                })
                .collect::<anyhow::Result<_>>()?,
        )
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn mutation_observation_with(
        updates: Vec<PendingDocumentUpdate>,
        outcome: FunctionOutcome,
        begin_timestamp: Timestamp,
    ) -> anyhow::Result<MutationShadowTestObservation> {
        mutation_observation_with_table_mapping(
            updates,
            outcome,
            begin_timestamp,
            user_table_mapping(),
        )
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn mutation_observation_with_table_mapping(
        updates: Vec<PendingDocumentUpdate>,
        outcome: FunctionOutcome,
        begin_timestamp: Timestamp,
        table_mapping: TableMapping,
    ) -> anyhow::Result<MutationShadowTestObservation> {
        mutation_observation_with_table_mapping_and_reads(
            updates,
            outcome,
            begin_timestamp,
            table_mapping,
            ReadSet::empty(),
            &BTreeSet::new(),
        )
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn mutation_observation_with_table_mapping_and_reads(
        updates: Vec<PendingDocumentUpdate>,
        outcome: FunctionOutcome,
        begin_timestamp: Timestamp,
        table_mapping: TableMapping,
        handler_read_set: ReadSet,
        preexisting_write_ids: &BTreeSet<ResolvedDocumentId>,
    ) -> anyhow::Result<MutationShadowTestObservation> {
        let reads = || crate::FunctionReads {
            reads: ReadSet::empty(),
            num_intervals: 0,
            user_tx_size: TransactionReadSize::default(),
            system_tx_size: TransactionReadSize::default(),
        };
        let transaction = crate::FunctionFinalTransaction {
            begin_timestamp,
            reads: reads(),
            handler_reads: Some(crate::FunctionReads {
                reads: handler_read_set,
                num_intervals: 0,
                user_tx_size: TransactionReadSize::default(),
                system_tx_size: TransactionReadSize::default(),
            }),
            writes: crate::FunctionWrites { updates },
            rows_read_by_tablet: Default::default(),
            table_mapping,
        };
        Ok(MutationShadowTestObservation::new(
            &transaction,
            &outcome,
            preexisting_write_ids,
        )?)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn comparator_read_set() -> ReadSet {
        let mut intervals = IntervalSet::new();
        intervals.add(Interval::prefix(vec![1, 2, 3].into()));
        ReadSet::new(
            BTreeMap::from([(
                TabletIndexName::by_id(TabletId::MIN),
                IndexReads {
                    fields: IndexedFields::by_id(),
                    intervals,
                    stack_traces: None,
                },
            )]),
            BTreeMap::new(),
        )
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn comparator_query_journal() -> QueryJournal {
        QueryJournal {
            end_cursor: Some(Cursor {
                position: CursorPosition::After(IndexKeyBytes(vec![1, 2, 3])),
                query_fingerprint: vec![4, 5, 6],
            }),
        }
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn comparator_query_observation() -> anyhow::Result<QueryShadowObservation> {
        let FunctionOutcome::Mutation(mut outcome) = mutation_outcome()? else {
            unreachable!("mutation_outcome returned a non-mutation outcome")
        };
        outcome.observed_identity = true;
        outcome.observed_rng = true;
        outcome.observed_time = true;
        outcome.journal = comparator_query_journal();
        outcome.log_lines = nested_log_lines(
            "nested:default",
            "first",
            LogLevel::Info,
            false,
            "code",
            1,
            2,
            false,
        );
        outcome.audit_log_lines = vec![
            AuditLogLine {
                body: serde_json::json!({ "sequence": 1 }),
            },
            AuditLogLine {
                body: serde_json::json!({ "sequence": 2 }),
            },
        ]
        .into();
        outcome.host_operation_trace = HostOperationTrace::from(vec![
            LogicalHostOperation::DatabaseGet,
            LogicalHostOperation::DatabaseQueryPage,
        ]);
        Ok(QueryShadowObservation {
            begin_timestamp: Timestamp::MIN,
            outcome,
            reads: comparator_read_set(),
        })
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn mismatch_dimensions(comparison: &QueryShadowComparison) -> Vec<&'static str> {
        [
            ("snapshot", comparison.snapshot_matches),
            ("result", comparison.result_matches),
            ("query_journal", comparison.journal_matches),
            ("read_dependencies", comparison.read_dependencies_match),
            ("invocation_inputs", comparison.invocation_inputs_match),
            (
                "host_operation_trace",
                comparison.host_operation_trace_matches,
            ),
            (
                "host_operation_error",
                comparison.host_operation_error_matches,
            ),
            ("observed_identity", comparison.observed_identity_matches),
            ("observed_time", comparison.observed_time_matches),
            ("observed_rng", comparison.observed_rng_matches),
            ("log_lines", comparison.log_lines_match),
            ("audit_log_lines", comparison.audit_log_lines_match),
            ("write_set", comparison.write_set_matches),
        ]
        .into_iter()
        .filter_map(|(dimension, matches)| (!matches).then_some(dimension))
        .collect()
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    fn assert_only_mismatch(comparison: QueryShadowComparison, expected: &'static str, case: &str) {
        assert_eq!(mismatch_dimensions(&comparison), vec![expected], "{case}");
        assert!(!comparison.matches(), "{case}");
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_accepts_matching_full_observations() -> anyhow::Result<()>
    {
        assert!(compare_query(
            comparator_query_observation()?,
            comparator_query_observation()?,
        )
        .matches());

        let mut primary = comparator_query_observation()?;
        primary.outcome.result = Ok(JsonPackedValue::from_network(
            r#"{"first":true,"second":false}"#.to_owned(),
        )?);
        let mut shadow = comparator_query_observation()?;
        shadow.outcome.result = Ok(JsonPackedValue::from_network(
            r#"{"second":false,"first":true}"#.to_owned(),
        )?);
        assert!(compare_query(primary, shadow).matches());
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_result_diagnostic_reports_only_structural_positions() -> anyhow::Result<()> {
        use super::QueryShadowResultDifference;
        for (left, right, reason, path) in [
            (
                serde_json::json!([{ "field": 1 }]),
                serde_json::json!([{ "field": 2 }]),
                QueryShadowResultDifference::Value,
                vec![0, 0],
            ),
            (
                serde_json::json!([{ "field": "private-value" }]),
                serde_json::json!([{ "field": null }]),
                QueryShadowResultDifference::Type,
                vec![0, 0],
            ),
            (
                serde_json::json!({ "field": [1] }),
                serde_json::json!({ "field": [] }),
                QueryShadowResultDifference::ArrayLength,
                vec![0],
            ),
            (
                serde_json::json!({ "left-field": 1 }),
                serde_json::json!({ "right-field": 1 }),
                QueryShadowResultDifference::ObjectFields,
                vec![],
            ),
        ] {
            let left = value::json_deserialize_bytes(&serde_json::to_vec(&left)?)?;
            let right = value::json_deserialize_bytes(&serde_json::to_vec(&right)?)?;
            let diagnostic =
                super::value_result_diagnostic(&left, &right, &mut Vec::new(), &mut 4096).unwrap();
            assert_eq!(diagnostic.reason, reason);
            assert_eq!(diagnostic.path, path);
            assert!(!format!("{diagnostic:?}").contains("private-value"));
            assert!(!format!("{diagnostic:?}").contains("field"));
        }
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_result_diagnostic_limit_does_not_hide_a_mismatch() -> anyhow::Result<()> {
        let mut primary = comparator_query_observation()?;
        let mut shadow = comparator_query_observation()?;
        let left = vec![false; 4096];
        let mut right = left.clone();
        right[4095] = true;
        primary.outcome.result = Ok(JsonPackedValue::from_network(serde_json::to_string(
            &left,
        )?)?);
        shadow.outcome.result = Ok(JsonPackedValue::from_network(serde_json::to_string(
            &right,
        )?)?);
        let comparison = compare_query(primary, shadow);
        assert!(!comparison.matches());
        assert!(!comparison.result_matches);
        let diagnostic = comparison.result_diagnostic.unwrap();
        assert_eq!(
            diagnostic.reason,
            super::QueryShadowResultDifference::InspectionLimit
        );
        assert_eq!(diagnostic.path, vec![4095]);
        let value = ConvexValue::Boolean(false);
        let depth_limited =
            super::value_result_diagnostic(&value, &value, &mut vec![0; 32], &mut 4096).unwrap();
        assert_eq!(
            depth_limited.reason,
            super::QueryShadowResultDifference::InspectionLimit
        );
        assert_eq!(depth_limited.path.len(), 32);
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_result_diagnostic_rejects_pending_query_values() -> anyhow::Result<()> {
        let primary = comparator_query_observation()?;
        let mut shadow = comparator_query_observation()?;
        shadow.outcome.result = Ok(JsonPackedValue::pack(PendingValue::CommitTs));
        let comparison = compare_query(primary, shadow);
        assert!(!comparison.result_matches);
        let diagnostic = comparison.result_diagnostic.unwrap();
        assert_eq!(
            diagnostic.reason,
            super::QueryShadowResultDifference::InvalidValue
        );
        assert!(diagnostic.path.is_empty());
        assert_eq!(diagnostic.primary_type, "value");
        assert_eq!(diagnostic.shadow_type, "invalid");
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_attributes_result_and_normalized_error_mismatches(
    ) -> anyhow::Result<()> {
        let primary = comparator_query_observation()?;
        let mut shadow = comparator_query_observation()?;
        shadow.outcome.result = Ok(JsonPackedValue::pack(PendingValue::Concrete(
            ConvexValue::Boolean(true),
        )));
        let comparison = compare_query(primary, shadow);
        assert!(comparison.result_diagnostic.is_some());
        assert_only_mismatch(comparison, "result", "result value");

        for (case, primary_error, shadow_error) in [
            (
                "error message",
                JsError::from_message("primary".to_owned()),
                JsError::from_message("shadow".to_owned()),
            ),
            (
                "error custom data",
                JsError::convex_error("same".to_owned(), ConvexValue::Boolean(false)),
                JsError::convex_error("same".to_owned(), ConvexValue::Boolean(true)),
            ),
        ] {
            let mut primary = comparator_query_observation()?;
            let mut shadow = comparator_query_observation()?;
            primary.outcome.result = Err(primary_error);
            shadow.outcome.result = Err(shadow_error);
            let comparison = compare_query(primary, shadow);
            assert_eq!(
                comparison.result_diagnostic.as_ref().unwrap().reason,
                if case == "error message" {
                    super::QueryShadowResultDifference::ErrorMessage
                } else {
                    super::QueryShadowResultDifference::ErrorData
                }
            );
            assert_only_mismatch(comparison, "result", case);
        }
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_attributes_full_journal_drops_and_cursor_changes(
    ) -> anyhow::Result<()> {
        for case in ["drop", "fingerprint", "cursor byte order"] {
            let primary = comparator_query_observation()?;
            let mut shadow = comparator_query_observation()?;
            match case {
                "drop" => shadow.outcome.journal = QueryJournal::new(),
                "fingerprint" => {
                    shadow
                        .outcome
                        .journal
                        .end_cursor
                        .as_mut()
                        .expect("comparator fixture omitted its cursor")
                        .query_fingerprint = vec![6, 5, 4];
                },
                "cursor byte order" => {
                    shadow
                        .outcome
                        .journal
                        .end_cursor
                        .as_mut()
                        .expect("comparator fixture omitted its cursor")
                        .position = CursorPosition::After(IndexKeyBytes(vec![3, 2, 1]));
                },
                _ => unreachable!("unknown journal negative control"),
            }
            assert_only_mismatch(compare_query(primary, shadow), "query_journal", case);
        }
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_attributes_read_dependency_drop() -> anyhow::Result<()> {
        let primary = comparator_query_observation()?;
        let mut shadow = comparator_query_observation()?;
        shadow.reads = ReadSet::empty();

        assert_only_mismatch(
            compare_query(primary, shadow),
            "read_dependencies",
            "indexed read dependency drop",
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_attributes_each_invocation_input() -> anyhow::Result<()> {
        type InputMutation = fn(&mut UdfOutcome) -> anyhow::Result<()>;
        let cases: [(&str, InputMutation); 6] = [
            ("path", |outcome| {
                outcome.path =
                    PublicFunctionPath::RootExport("other:default".parse::<ExportPath>()?)
                        .debug_into_component_path();
                Ok(())
            }),
            ("arguments", |outcome| {
                outcome.arguments =
                    SerializedArgs::from_args(vec![serde_json::json!({ "different": true })])?;
                Ok(())
            }),
            ("identity", |outcome| {
                outcome.identity = InertIdentity::System;
                Ok(())
            }),
            ("rng seed", |outcome| {
                outcome.rng_seed = [1; 32];
                Ok(())
            }),
            ("Unix timestamp", |outcome| {
                outcome.unix_timestamp = UnixTimestamp::from_nanos(1);
                Ok(())
            }),
            ("UDF server version", |outcome| {
                outcome.udf_server_version = Some("1.0.0".parse()?);
                Ok(())
            }),
        ];

        for (case, mutate) in cases {
            let primary = comparator_query_observation()?;
            let mut shadow = comparator_query_observation()?;
            mutate(&mut shadow.outcome)?;
            assert_only_mismatch(compare_query(primary, shadow), "invocation_inputs", case);
        }
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_attributes_host_trace_drop_and_status(
    ) -> anyhow::Result<()> {
        for case in ["drop", "status"] {
            let primary = comparator_query_observation()?;
            let mut shadow = comparator_query_observation()?;
            shadow.outcome.host_operation_trace = match case {
                "drop" => HostOperationTrace::from(vec![LogicalHostOperation::DatabaseGet]),
                "status" => {
                    let mut trace = HostOperationTrace::for_query_shadow();
                    trace.record(
                        LogicalHostOperation::DatabaseGet,
                        LogicalHostOperationStatus::Failure,
                    );
                    trace.record(
                        LogicalHostOperation::DatabaseQueryPage,
                        LogicalHostOperationStatus::Success,
                    );
                    trace
                },
                _ => unreachable!("unknown host-trace negative control"),
            };
            let comparison = compare_query(primary, shadow);
            let diagnostic = comparison
                .host_operation_trace_diagnostic
                .as_ref()
                .expect("trace mismatch requires bounded details");
            assert!(diagnostic.primary_comparison_eligible);
            assert!(diagnostic.shadow_comparison_eligible);
            assert_eq!(diagnostic.omitted_differing_count, 0);
            match case {
                "drop" => assert_eq!(
                    diagnostic.differing_counts,
                    vec![QueryShadowHostOperationCountDifference {
                        operation: LogicalHostOperation::DatabaseQueryPage,
                        status: LogicalHostOperationStatus::Success,
                        primary_count: 1,
                        shadow_count: 0,
                    }]
                ),
                "status" => assert_eq!(
                    diagnostic.differing_counts,
                    vec![
                        QueryShadowHostOperationCountDifference {
                            operation: LogicalHostOperation::DatabaseGet,
                            status: LogicalHostOperationStatus::Success,
                            primary_count: 1,
                            shadow_count: 0,
                        },
                        QueryShadowHostOperationCountDifference {
                            operation: LogicalHostOperation::DatabaseGet,
                            status: LogicalHostOperationStatus::Failure,
                            primary_count: 0,
                            shadow_count: 1,
                        },
                    ]
                ),
                _ => unreachable!("unknown host-trace negative control"),
            }
            assert_only_mismatch(comparison, "host_operation_trace", case);
        }

        let primary = comparator_query_observation()?;
        let mut reordered = comparator_query_observation()?;
        reordered.outcome.host_operation_trace = HostOperationTrace::from(vec![
            LogicalHostOperation::DatabaseQueryPage,
            LogicalHostOperation::DatabaseGet,
        ]);
        let comparison = compare_query(primary, reordered);
        assert!(comparison.matches());
        assert!(comparison.host_operation_trace_diagnostic.is_none());
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_attributes_host_operation_error_drop(
    ) -> anyhow::Result<()> {
        let error = HostOperationErrorV1::NonexistentDocument {
            operation: HostOperation::Patch,
            document_id: DeveloperDocumentId::MAX,
        };
        let mut primary = comparator_query_observation()?;
        let mut shadow = comparator_query_observation()?;
        primary.outcome.result = Err(JsError::from_message("same".to_owned()));
        shadow.outcome.result = Err(JsError::from_message("same".to_owned()));
        primary.outcome.host_operation_error = Some(error);
        shadow.outcome.host_operation_error = None;

        assert_only_mismatch(
            compare_query(primary, shadow),
            "host_operation_error",
            "typed host-operation error drop",
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_attributes_observed_signal_drops() -> anyhow::Result<()> {
        for case in ["identity", "time", "RNG"] {
            let primary = comparator_query_observation()?;
            let mut shadow = comparator_query_observation()?;
            let expected = match case {
                "identity" => {
                    shadow.outcome.observed_identity = false;
                    "observed_identity"
                },
                "time" => {
                    shadow.outcome.observed_time = false;
                    "observed_time"
                },
                "RNG" => {
                    shadow.outcome.observed_rng = false;
                    "observed_rng"
                },
                _ => unreachable!("unknown observation-signal negative control"),
            };
            assert_only_mismatch(compare_query(primary, shadow), expected, case);
        }
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_attributes_log_drop_and_reorder() -> anyhow::Result<()> {
        for case in ["drop", "reorder"] {
            let primary = comparator_query_observation()?;
            let mut shadow = comparator_query_observation()?;
            match case {
                "drop" => {
                    shadow.outcome.log_lines.pop();
                },
                "reorder" => {
                    shadow.outcome.log_lines = nested_log_lines(
                        "nested:default",
                        "first",
                        LogLevel::Info,
                        false,
                        "code",
                        1,
                        2,
                        true,
                    );
                },
                _ => unreachable!("unknown log negative control"),
            }
            assert_only_mismatch(compare_query(primary, shadow), "log_lines", case);
        }
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn query_shadow_production_comparator_attributes_audit_log_drop_and_reorder(
    ) -> anyhow::Result<()> {
        for case in ["drop", "reorder"] {
            let primary = comparator_query_observation()?;
            let mut shadow = comparator_query_observation()?;
            match case {
                "drop" => {
                    shadow.outcome.audit_log_lines.pop();
                },
                "reorder" => {
                    shadow.outcome.audit_log_lines = shadow
                        .outcome
                        .audit_log_lines
                        .clone()
                        .into_iter()
                        .rev()
                        .collect();
                },
                _ => unreachable!("unknown audit-log negative control"),
            }
            assert_only_mismatch(compare_query(primary, shadow), "audit_log_lines", case);
        }
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_production_comparator_attributes_write_drop() -> anyhow::Result<()> {
        let document_id = test_document_id(1);
        let primary = mutation_observation(vec![existing_update(
            document_id,
            serde_json::json!({ "value": "before" }),
            serde_json::json!({ "value": "after" }),
        )?])?;
        let shadow = mutation_observation(vec![])?;

        assert_only_mismatch(
            compare_mutation(primary, shadow),
            "write_set",
            "existing-document write drop",
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_production_comparator_keeps_result_and_read_attribution_when_insert_differs(
    ) -> anyhow::Result<()> {
        let primary = mutation_observation(vec![inserted_document(
            test_document_id(1),
            serde_json::json!({ "value": "primary" }),
        )?])?;
        let shadow = mutation_observation(vec![inserted_document(
            test_document_id(2),
            serde_json::json!({ "value": "shadow" }),
        )?])?;

        assert_only_mismatch(
            compare_mutation(primary, shadow),
            "write_set",
            "inserted-document write difference",
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_production_comparator_attributes_result_mismatch() -> anyhow::Result<()> {
        let primary = mutation_observation_with(
            vec![],
            mutation_outcome_with_result(serde_json::json!({ "lane": "primary" }))?,
            Timestamp::MIN,
        )?;
        let shadow = mutation_observation_with(
            vec![],
            mutation_outcome_with_result(serde_json::json!({ "lane": "shadow" }))?,
            Timestamp::MIN,
        )?;

        assert_only_mismatch(
            compare_mutation(primary, shadow),
            "result",
            "mutation result",
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_production_comparator_accepts_effect_reordering() -> anyhow::Result<()> {
        let patch_id = test_document_id(1);
        let replace_id = test_document_id(2);
        let delete_id = test_document_id(3);
        let updates = || -> anyhow::Result<Vec<PendingDocumentUpdate>> {
            Ok(vec![
                existing_update(
                    patch_id,
                    serde_json::json!({ "patched": "before" }),
                    serde_json::json!({ "patched": "after" }),
                )?,
                existing_update(
                    replace_id,
                    serde_json::json!({ "replaced": "before" }),
                    serde_json::json!({ "replacement": "after" }),
                )?,
                existing_delete(delete_id, serde_json::json!({ "deleted": true }))?,
            ])
        };
        let primary = mutation_observation(updates()?)?;
        let mut shadow = mutation_observation(updates()?)?;
        shadow.outcome.host_operation_trace = HostOperationTrace::from(vec![
            LogicalHostOperation::DatabaseReplace,
            LogicalHostOperation::DatabasePatch,
            LogicalHostOperation::DatabaseDelete,
        ]);

        assert!(compare_mutation(primary, shadow).matches());
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_normalizes_multi_write_patch_replace_and_delete_transitions(
    ) -> anyhow::Result<()> {
        let patch_id = test_document_id(1);
        let replace_id = test_document_id(2);
        let delete_id = test_document_id(3);
        let primary = mutation_observation(vec![
            existing_update(
                patch_id,
                serde_json::json!({ "retained": "same", "patched": "before" }),
                serde_json::json!({ "retained": "same", "patched": "after" }),
            )?,
            existing_update(
                replace_id,
                serde_json::json!({ "replaced": "before", "removed": true }),
                serde_json::json!({ "replacement": "after" }),
            )?,
            existing_delete(delete_id, serde_json::json!({ "deleted": true }))?,
        ])?;
        let shadow = mutation_observation(vec![
            existing_delete(delete_id, serde_json::json!({ "deleted": true }))?,
            existing_update(
                replace_id,
                serde_json::json!({ "replaced": "before", "removed": true }),
                serde_json::json!({ "replacement": "after" }),
            )?,
            existing_update(
                patch_id,
                serde_json::json!({ "retained": "same", "patched": "before" }),
                serde_json::json!({ "retained": "same", "patched": "after" }),
            )?,
        ])?;

        compare_mutation_for_test(primary, shadow)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_accepts_zero_existing_document_writes() -> anyhow::Result<()> {
        compare_mutation_for_test(mutation_observation(vec![])?, mutation_observation(vec![])?)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_rejects_mismatched_existing_document_transitions() -> anyhow::Result<()> {
        let document_id = test_document_id(1);
        let primary = mutation_observation(vec![existing_update(
            document_id,
            serde_json::json!({ "value": "before" }),
            serde_json::json!({ "value": "after" }),
        )?])?;
        let shadow = mutation_observation(vec![existing_update(
            document_id,
            serde_json::json!({ "value": "before" }),
            serde_json::json!({ "value": "different" }),
        )?])?;

        let error = compare_mutation_for_test(primary, shadow).unwrap_err();
        assert_eq!(
            error.to_string(),
            "test-only mutation shadow normalized write set differs"
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_normalizes_insert_ids_in_results_and_writes() -> anyhow::Result<()> {
        let primary_parent = test_document_id(4);
        let primary_child = test_document_id(5);
        let shadow_parent = test_document_id(6);
        let shadow_child = test_document_id(7);
        let existing = test_document_id(8);
        let primary_parent_id = primary_parent.developer_id.encode();
        let primary_child_id = primary_child.developer_id.encode();
        let shadow_parent_id = shadow_parent.developer_id.encode();
        let shadow_child_id = shadow_child.developer_id.encode();

        let primary = mutation_observation_with(
            vec![
                existing_update(
                    existing,
                    serde_json::json!({ "owner": null }),
                    serde_json::json!({ "owner": primary_parent_id.clone() }),
                )?,
                inserted_document(
                    primary_parent,
                    serde_json::json!({
                        "kind": "parent",
                        "child": primary_child_id.clone(),
                    }),
                )?,
                inserted_document(primary_child, serde_json::json!({ "kind": "child" }))?,
            ],
            mutation_outcome_with_result(serde_json::json!({
                "parent": primary_parent_id,
                "child": { "id": primary_child_id },
            }))?,
            Timestamp::MIN,
        )?;
        let shadow = mutation_observation_with(
            vec![
                inserted_document(shadow_child, serde_json::json!({ "kind": "child" }))?,
                existing_update(
                    existing,
                    serde_json::json!({ "owner": null }),
                    serde_json::json!({ "owner": shadow_parent_id.clone() }),
                )?,
                inserted_document(
                    shadow_parent,
                    serde_json::json!({
                        "kind": "parent",
                        "child": shadow_child_id.clone(),
                    }),
                )?,
            ],
            mutation_outcome_with_result(serde_json::json!({
                "parent": shadow_parent_id,
                "child": { "id": shadow_child_id },
            }))?,
            Timestamp::MIN,
        )?;

        compare_mutation_for_test(primary, shadow)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_preserves_inherited_insert_identity_and_dependencies() -> anyhow::Result<()>
    {
        let inherited_first = test_document_id(1);
        let inherited_second = test_document_id(2);
        let inherited_ids = BTreeSet::from([inherited_first, inherited_second]);
        let primary_new = test_document_id(3);
        let shadow_new = test_document_id(4);
        let observation = |new_id: ResolvedDocumentId,
                           result_id: ResolvedDocumentId,
                           read_id,
                           inherited_creation_time| {
            mutation_observation_with_table_mapping_and_reads(
                vec![
                    inserted_document_with_creation_time(
                        inherited_first,
                        serde_json::json!({ "value": "inherited" }),
                        inherited_creation_time,
                    )?,
                    inserted_document(
                        inherited_second,
                        serde_json::json!({ "value": "inherited" }),
                    )?,
                    inserted_document(new_id, serde_json::json!({ "value": "new" }))?,
                ],
                mutation_outcome_with_result(serde_json::json!({
                    "inherited": result_id.developer_id.encode(),
                    "new": new_id.developer_id.encode(),
                }))?,
                Timestamp::MIN,
                user_table_mapping(),
                insert_id_read_set(&[read_id, new_id]),
                &inherited_ids,
            )
        };
        let primary = || observation(primary_new, inherited_first, inherited_first, 1.0);
        assert!(compare_mutation(
            primary()?,
            observation(shadow_new, inherited_first, inherited_first, 1.0)?,
        )
        .matches());

        let changed_result = compare_mutation(
            primary()?,
            observation(shadow_new, inherited_second, inherited_first, 1.0)?,
        );
        assert!(!changed_result.result_matches, "{changed_result:?}");
        let changed_read = compare_mutation(
            primary()?,
            observation(shadow_new, inherited_first, inherited_second, 1.0)?,
        );
        assert!(!changed_read.read_dependencies_match, "{changed_read:?}");
        let changed_creation_time = compare_mutation(
            primary()?,
            observation(shadow_new, inherited_first, inherited_first, 2.0)?,
        );
        assert!(
            !changed_creation_time.write_set_matches,
            "{changed_creation_time:?}"
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_normalizes_insert_ids_in_error_custom_data() -> anyhow::Result<()> {
        let primary_id = test_document_id(4);
        let shadow_id = test_document_id(5);
        let primary = mutation_observation_with(
            vec![inserted_document(
                primary_id,
                serde_json::json!({ "value": "same" }),
            )?],
            mutation_outcome_with_error_data(serde_json::json!({
                "insertedId": primary_id.developer_id.encode(),
            }))?,
            Timestamp::MIN,
        )?;
        let shadow = mutation_observation_with(
            vec![inserted_document(
                shadow_id,
                serde_json::json!({ "value": "same" }),
            )?],
            mutation_outcome_with_error_data(serde_json::json!({
                "insertedId": shadow_id.developer_id.encode(),
            }))?,
            Timestamp::MIN,
        )?;

        let comparison = compare_mutation(primary, shadow);
        assert!(comparison.matches(), "{comparison:?}");
        assert_eq!(
            comparison.inserted_document_identity,
            QueryShadowInsertedDocumentIdentity::Matched
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_normalizes_lane_local_insert_creation_time() -> anyhow::Result<()> {
        let primary = mutation_observation(vec![inserted_document_with_creation_time(
            test_document_id(4),
            serde_json::json!({ "value": "same" }),
            1.0,
        )?])?;
        let shadow = mutation_observation(vec![inserted_document_with_creation_time(
            test_document_id(5),
            serde_json::json!({ "value": "same" }),
            2.0,
        )?])?;

        compare_mutation_for_test(primary, shadow)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_normalizes_only_private_scheduler_readiness_time() -> anyhow::Result<()> {
        let primary_args = test_document_id(1);
        let primary_job = test_document_id(2);
        let shadow_args = test_document_id(3);
        let shadow_job = test_document_id(4);
        let scheduled_job_updates =
            |args_id, job_id, next_ts, original_scheduled_ts| -> anyhow::Result<_> {
                Ok(vec![
                    inserted_document(args_id, serde_json::json!({ "args": ["same"] }))?,
                    scheduled_job_insert(job_id, args_id, next_ts, original_scheduled_ts)?,
                ])
            };
        let observation = |updates, table_mapping| {
            mutation_observation_with_table_mapping(
                updates,
                mutation_outcome()?,
                Timestamp::MIN,
                table_mapping,
            )
        };

        compare_mutation_for_test(
            observation(
                scheduled_job_updates(primary_args, primary_job, 1_001, 1_000)?,
                scheduled_jobs_table_mapping(),
            )?,
            observation(
                scheduled_job_updates(shadow_args, shadow_job, 1_002, 1_000)?,
                scheduled_jobs_table_mapping(),
            )?,
        )?;

        let different_requested_time = compare_mutation_for_test(
            observation(
                scheduled_job_updates(primary_args, primary_job, 1_001, 1_000)?,
                scheduled_jobs_table_mapping(),
            )?,
            observation(
                scheduled_job_updates(shadow_args, shadow_job, 1_002, 1_001)?,
                scheduled_jobs_table_mapping(),
            )?,
        )
        .unwrap_err();
        assert_eq!(
            different_requested_time.to_string(),
            "mutation shadow inserted-document write set differs"
        );

        let user_document_shape = compare_mutation_for_test(
            mutation_observation(scheduled_job_updates(
                primary_args,
                primary_job,
                1_001,
                1_000,
            )?)?,
            mutation_observation(scheduled_job_updates(
                shadow_args,
                shadow_job,
                1_002,
                1_000,
            )?)?,
        )
        .unwrap_err();
        assert_eq!(
            user_document_shape.to_string(),
            "mutation shadow inserted-document write set differs"
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_normalizes_scheduled_operation_insert_graph() -> anyhow::Result<()> {
        let operation_tablet = 11;
        let args_tablet = 12;
        let job_tablet = 13;
        let operation_table_number = 101;
        let args_table_number = 102;
        let job_table_number = 103;
        let primary_operation =
            test_document_id_in_table(operation_tablet, operation_table_number, 1);
        let primary_args = test_document_id_in_table(args_tablet, args_table_number, 2);
        let primary_job = test_document_id_in_table(job_tablet, job_table_number, 3);
        let shadow_operation =
            test_document_id_in_table(operation_tablet, operation_table_number, 4);
        let shadow_args = test_document_id_in_table(args_tablet, args_table_number, 5);
        let shadow_job = test_document_id_in_table(job_tablet, job_table_number, 6);

        let mut table_mapping = TableMapping::new();
        table_mapping.insert(
            primary_operation.tablet_id,
            TableNamespace::Global,
            primary_operation.developer_id.table(),
            "operations".parse::<TableName>()?,
        );
        table_mapping.insert(
            primary_args.tablet_id,
            TableNamespace::Global,
            primary_args.developer_id.table(),
            model::scheduled_jobs::args::SCHEDULED_JOBS_ARGS_TABLE.clone(),
        );
        table_mapping.insert(
            primary_job.tablet_id,
            TableNamespace::Global,
            primary_job.developer_id.table(),
            model::scheduled_jobs::SCHEDULED_JOBS_TABLE.clone(),
        );

        let observation = |operation_id: ResolvedDocumentId,
                           args_id: ResolvedDocumentId,
                           job_id: ResolvedDocumentId,
                           creation_time: f64,
                           next_ts: i64,
                           stable_argument: &str| {
            mutation_observation_with_table_mapping_and_reads(
                vec![
                    inserted_document_with_creation_time(
                        operation_id,
                        serde_json::json!({
                            "running": true,
                            "scheduledFnId": job_id.developer_id.encode(),
                            "status": "running",
                        }),
                        creation_time,
                    )?,
                    scheduled_args_insert(args_id, operation_id, stable_argument, creation_time)?,
                    scheduled_job_insert_with_creation_time(
                        job_id,
                        args_id,
                        next_ts,
                        1_000,
                        creation_time,
                    )?,
                ],
                mutation_outcome()?,
                Timestamp::MIN,
                table_mapping.clone(),
                insert_id_read_set(&[operation_id, args_id, job_id]),
                &BTreeSet::new(),
            )
        };

        let comparison = compare_mutation(
            observation(
                primary_operation,
                primary_args,
                primary_job,
                1.0,
                1_001,
                "same",
            )?,
            observation(
                shadow_operation,
                shadow_args,
                shadow_job,
                2.0,
                1_002,
                "same",
            )?,
        );
        assert!(comparison.matches(), "{comparison:?}");

        let changed_arguments = compare_mutation(
            observation(
                primary_operation,
                primary_args,
                primary_job,
                1.0,
                1_001,
                "same",
            )?,
            observation(
                shadow_operation,
                shadow_args,
                shadow_job,
                2.0,
                1_002,
                "different",
            )?,
        );
        assert!(!changed_arguments.write_set_matches);
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_matches_scheduled_batch_with_reversed_allocated_ids() -> anyhow::Result<()> {
        let operation = test_document_id_in_table(11, 101, 1);
        let args_sample = test_document_id_in_table(12, 102, 1);
        let job_sample = test_document_id_in_table(13, 103, 1);
        let mut table_mapping = TableMapping::new();
        for (id, table_name) in [
            (
                args_sample,
                model::scheduled_jobs::args::SCHEDULED_JOBS_ARGS_TABLE.clone(),
            ),
            (
                job_sample,
                model::scheduled_jobs::SCHEDULED_JOBS_TABLE.clone(),
            ),
        ] {
            table_mapping.insert(
                id.tablet_id,
                TableNamespace::Global,
                id.developer_id.table(),
                table_name,
            );
        }
        let observation = |shadow: bool, changed_argument: bool| -> anyhow::Result<_> {
            let mut writes = Vec::new();
            let mut ids = Vec::new();
            for index in 0..8u8 {
                let args_id =
                    test_document_id_in_table(12, 102, if shadow { 20 + index } else { index + 1 });
                let job_id =
                    test_document_id_in_table(13, 103, if shadow { 40 - index } else { index + 1 });
                let creation_time = if shadow { 2.0 } else { 1.0 };
                let argument = if changed_argument && index == 0 {
                    "changed".to_owned()
                } else {
                    format!("argument-{index}")
                };
                writes.push(scheduled_args_insert(
                    args_id,
                    operation,
                    &argument,
                    creation_time,
                )?);
                writes.push(scheduled_job_insert_with_creation_time(
                    job_id,
                    args_id,
                    if shadow { 1_002 } else { 1_001 },
                    1_000,
                    creation_time,
                )?);
                ids.extend([args_id, job_id]);
            }
            mutation_observation_with_table_mapping_and_reads(
                writes,
                mutation_outcome()?,
                Timestamp::MIN,
                table_mapping.clone(),
                insert_id_read_set(&ids),
                &BTreeSet::new(),
            )
        };

        // Job metadata has the same shape, but distinct argument documents
        // determine the relation. Allocation order is not execution identity.
        let comparison = compare_mutation(observation(false, false)?, observation(true, false)?);
        assert!(comparison.matches(), "{comparison:?}");
        let changed = compare_mutation(observation(false, false)?, observation(true, true)?);
        assert!(!changed.write_set_matches);
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_preserves_user_bytes_that_encode_insert_ids() -> anyhow::Result<()> {
        let primary_id = test_document_id(1);
        let shadow_id = test_document_id(2);
        let encoded_bytes = |id: ResolvedDocumentId| -> anyhow::Result<_> {
            let value = ConvexArray::try_from(
                serde_json::json!([{ "documentId": id.developer_id.encode() }]),
            )?;
            Ok(ConvexValue::Bytes(
                value.json_serialize()?.into_bytes().try_into()?,
            ))
        };
        let observation = |id| {
            mutation_observation(vec![inserted_document(
                id,
                serde_json::json!({ "payload": encoded_bytes(id)?.to_internal_json() }),
            )?])
        };

        let comparison = compare_mutation(observation(primary_id)?, observation(shadow_id)?);
        assert!(!comparison.write_set_matches);
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_preserves_bytes_whose_base64_matches_inserted_ids() -> anyhow::Result<()> {
        // Two-byte table numbers produce 32-character IDs that are also
        // canonical Base64 for unrelated binary values.
        let primary_id = test_document_id_in_table(11, 128, 1);
        let shadow_id = test_document_id_in_table(11, 128, 2);
        let primary_bytes = serde_json::json!({ "$bytes": primary_id.developer_id.encode() });
        let shadow_bytes = serde_json::json!({ "$bytes": shadow_id.developer_id.encode() });
        for bytes in [&primary_bytes, &shadow_bytes] {
            assert_eq!(
                ConvexValue::try_from(bytes.clone())?.to_internal_json(),
                *bytes
            );
        }
        let mut table_mapping = TableMapping::new();
        table_mapping.insert(
            primary_id.tablet_id,
            TableNamespace::Global,
            primary_id.developer_id.table(),
            "shadowTestDocuments".parse()?,
        );
        let observation = |id, payload, outcome| {
            mutation_observation_with_table_mapping(
                vec![inserted_document(
                    id,
                    serde_json::json!({ "payload": payload }),
                )?],
                outcome,
                Timestamp::MIN,
                table_mapping.clone(),
            )
        };

        // Equal bytes must survive shape pruning even if only one lane
        // allocated the ID matching their encoding; changed bytes must fail.
        for (payload, expected_match) in [(&primary_bytes, true), (&shadow_bytes, false)] {
            let comparison = compare_mutation(
                observation(primary_id, &primary_bytes, mutation_outcome()?)?,
                observation(shadow_id, payload, mutation_outcome()?)?,
            );
            assert_eq!(comparison.matches(), expected_match, "{comparison:?}");
        }
        let comparison = compare_mutation(
            observation(
                primary_id,
                &primary_bytes,
                mutation_outcome_with_result(primary_bytes.clone())?,
            )?,
            observation(
                shadow_id,
                &primary_bytes,
                mutation_outcome_with_result(shadow_bytes.clone())?,
            )?,
        );
        assert!(!comparison.result_matches, "{comparison:?}");
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_uses_result_ids_to_disambiguate_identical_inserts() -> anyhow::Result<()> {
        let primary_first = test_document_id(9);
        let primary_second = test_document_id(10);
        let shadow_first = test_document_id(11);
        let shadow_second = test_document_id(12);
        let primary = mutation_observation_with(
            vec![
                inserted_document(primary_first, serde_json::json!({ "value": "same" }))?,
                inserted_document(primary_second, serde_json::json!({ "value": "same" }))?,
            ],
            mutation_outcome_with_result(serde_json::json!([
                primary_first.developer_id.encode(),
                primary_second.developer_id.encode(),
            ]))?,
            Timestamp::MIN,
        )?;
        let shadow = mutation_observation_with(
            vec![
                inserted_document(shadow_first, serde_json::json!({ "value": "same" }))?,
                inserted_document(shadow_second, serde_json::json!({ "value": "same" }))?,
            ],
            mutation_outcome_with_result(serde_json::json!([
                shadow_second.developer_id.encode(),
                shadow_first.developer_id.encode(),
            ]))?,
            Timestamp::MIN,
        )?;

        compare_mutation_for_test(primary, shadow)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_rejects_mismatched_inserted_document_transitions() -> anyhow::Result<()> {
        let primary_id = test_document_id(4);
        let shadow_id = test_document_id(5);
        let primary = mutation_observation(vec![inserted_document(
            primary_id,
            serde_json::json!({ "value": "primary" }),
        )?])?;
        let shadow = mutation_observation(vec![inserted_document(
            shadow_id,
            serde_json::json!({ "value": "shadow" }),
        )?])?;

        assert_eq!(
            compare_mutation_for_test(primary, shadow)
                .unwrap_err()
                .to_string(),
            "mutation shadow inserted-document write set differs"
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_accepts_equivalent_symmetric_insert_identity_mappings() -> anyhow::Result<()>
    {
        let primary = mutation_observation(vec![
            inserted_document(test_document_id(1), serde_json::json!({ "value": "same" }))?,
            inserted_document(test_document_id(2), serde_json::json!({ "value": "same" }))?,
        ])?;
        let shadow = mutation_observation(vec![
            inserted_document(test_document_id(3), serde_json::json!({ "value": "same" }))?,
            inserted_document(test_document_id(4), serde_json::json!({ "value": "same" }))?,
        ])?;

        compare_mutation_for_test(primary, shadow)
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_accepts_equivalent_symmetric_inserts_below_mapping_bound(
    ) -> anyhow::Result<()> {
        let primary = symmetric_insert_observation(1, 6, 1.0)?;
        let shadow = symmetric_insert_observation(7, 12, 2.0)?;

        let comparison = compare_mutation(primary, shadow);
        assert!(comparison.matches(), "{comparison:?}");
        assert_eq!(
            comparison.inserted_document_identity,
            QueryShadowInsertedDocumentIdentity::Matched
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_accepts_equivalent_symmetric_inserts_over_mapping_bound(
    ) -> anyhow::Result<()> {
        let primary = symmetric_insert_observation(1, 7, 1.0)?;
        let shadow = symmetric_insert_observation(8, 14, 2.0)?;

        let comparison = compare_mutation(primary, shadow);
        assert!(comparison.matches(), "{comparison:?}");
        assert_eq!(
            comparison.inserted_document_identity,
            QueryShadowInsertedDocumentIdentity::Matched
        );
        compare_mutation_for_test(
            symmetric_insert_observation(1, 7, 1.0)?,
            symmetric_insert_observation(8, 14, 2.0)?,
        )?;
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_shared_allocation_proves_large_identical_write_sets() -> anyhow::Result<()> {
        let observation = || {
            mutation_observation(
                (0..=super::MAX_MUTATION_SHADOW_INSERT_MAPPING_CANDIDATES)
                    .map(|number| {
                        let mut bytes = [0; 16];
                        bytes[..8].copy_from_slice(&u64::try_from(number)?.to_le_bytes());
                        let id = ResolvedDocumentId::new(
                            TabletId::MIN,
                            DeveloperDocumentId::new(TableNumber::MIN, InternalId(bytes)),
                        );
                        inserted_document(id, serde_json::json!({ "value": "same" }))
                    })
                    .collect::<anyhow::Result<_>>()?,
            )
        };
        let comparison = compare_mutation(observation()?, observation()?);
        assert!(comparison.matches(), "{comparison:?}");
        let mut changed = observation()?;
        changed.outcome.result = Ok(JsonPackedValue::pack(PendingValue::Concrete(
            ConvexValue::Boolean(true),
        )));
        changed.reads = comparator_read_set();
        let comparison = compare_mutation(observation()?, changed);
        assert!(!comparison.result_matches, "{comparison:?}");
        assert!(!comparison.read_dependencies_match, "{comparison:?}");
        assert!(!comparison.matches(), "{comparison:?}");
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_matches_cycles_with_overlapping_insert_id_domains() -> anyhow::Result<()> {
        let first = test_document_id(1);
        let second = test_document_id(2);
        let third = test_document_id(3);
        let observation = |ids: [ResolvedDocumentId; 3], changed_result| {
            mutation_observation_with(
                (0..3)
                    .map(|index| {
                        inserted_document(
                            ids[index],
                            serde_json::json!({
                                "next": ids[(index + 1) % 3].developer_id.encode(),
                            }),
                        )
                    })
                    .collect::<anyhow::Result<_>>()?,
                mutation_outcome_with_result(serde_json::json!([
                    ids[0].developer_id.encode(),
                    ids[1].developer_id.encode(),
                    ids[if changed_result { 0 } else { 2 }]
                        .developer_id
                        .encode(),
                ]))?,
                Timestamp::MIN,
            )
        };
        for (primary, shadow) in [
            ([first, second, third], [second, first, third]),
            ([second, first, third], [first, second, third]),
        ] {
            let comparison =
                compare_mutation(observation(primary, false)?, observation(shadow, false)?);
            assert!(comparison.matches(), "{comparison:?}");
            let changed =
                compare_mutation(observation(primary, false)?, observation(shadow, true)?);
            assert!(!changed.result_matches, "{changed:?}");
        }
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_bounds_partial_insert_identity_mapping_search() -> anyhow::Result<()> {
        let primary = mutation_observation(
            (1..=8)
                .map(|number| {
                    inserted_document(
                        test_document_id(number),
                        serde_json::json!({ "value": "same" }),
                    )
                })
                .collect::<anyhow::Result<_>>()?,
        )?;
        let mut shadow_updates = (9..=15)
            .map(|number| {
                inserted_document(
                    test_document_id(number),
                    serde_json::json!({ "value": "same" }),
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        shadow_updates.push(inserted_document(
            test_document_id(16),
            serde_json::json!({ "value": "different" }),
        )?);
        let shadow = mutation_observation(shadow_updates)?;

        assert_eq!(
            compare_mutation_for_test(primary, shadow)
                .unwrap_err()
                .to_string(),
            "mutation shadow inserted-document identity mapping exceeded its bounded search"
        );
        Ok(())
    }

    #[cfg(all(feature = "static-hermes-wasmtime-gate", feature = "testing"))]
    #[test]
    fn mutation_shadow_rejects_different_snapshots() -> anyhow::Result<()> {
        let primary = mutation_observation_with(vec![], mutation_outcome()?, Timestamp::MIN)?;
        let shadow =
            mutation_observation_with(vec![], mutation_outcome()?, Timestamp::MIN.succ()?)?;

        assert_eq!(
            compare_mutation_for_test(primary, shadow)
                .unwrap_err()
                .to_string(),
            "test-only mutation shadow did not execute at the same database snapshot"
        );
        Ok(())
    }

    #[test]
    fn report_retains_zero_primary_duration() {
        let report = QueryShadowReport::new(
            QueryShadowRouteKey::new(&"1".repeat(64), &"a".repeat(64)).unwrap(),
            Duration::ZERO,
            Duration::from_millis(1),
            QueryShadowComparison {
                snapshot_matches: true,
                result_matches: true,
                journal_matches: true,
                read_dependencies_match: true,
                invocation_inputs_match: true,
                host_operation_trace_matches: true,
                host_operation_error_matches: true,
                observed_identity_matches: true,
                observed_time_matches: true,
                observed_rng_matches: true,
                log_lines_match: true,
                audit_log_lines_match: true,
                write_set_matches: true,
                inserted_document_identity: QueryShadowInsertedDocumentIdentity::NotApplicable,
                host_operation_trace_diagnostic: None,
                result_diagnostic: None,
            },
        );

        assert!(matches!(
            report,
            QueryShadowReport::Compared {
                primary_duration: Duration::ZERO,
                ..
            }
        ));
    }

    #[test]
    fn comparison_requires_matching_invocation_inputs() {
        let comparison = QueryShadowComparison {
            snapshot_matches: true,
            result_matches: true,
            journal_matches: true,
            read_dependencies_match: true,
            invocation_inputs_match: false,
            host_operation_trace_matches: true,
            host_operation_error_matches: true,
            observed_identity_matches: true,
            observed_time_matches: true,
            observed_rng_matches: true,
            log_lines_match: true,
            audit_log_lines_match: true,
            write_set_matches: true,
            inserted_document_identity: QueryShadowInsertedDocumentIdentity::NotApplicable,
            host_operation_trace_diagnostic: None,
            result_diagnostic: None,
        };

        assert!(!comparison.matches());
    }

    #[test]
    fn comparison_requires_a_consistent_inserted_document_identity() {
        let comparison = QueryShadowComparison {
            snapshot_matches: true,
            result_matches: true,
            journal_matches: true,
            read_dependencies_match: true,
            invocation_inputs_match: true,
            host_operation_trace_matches: true,
            host_operation_error_matches: true,
            observed_identity_matches: true,
            observed_time_matches: true,
            observed_rng_matches: true,
            log_lines_match: true,
            audit_log_lines_match: true,
            write_set_matches: true,
            inserted_document_identity: QueryShadowInsertedDocumentIdentity::Inconsistent,
            host_operation_trace_diagnostic: None,
            result_diagnostic: None,
        };

        assert!(!comparison.matches());
    }

    #[test]
    fn invalid_primary_terminal_has_a_stable_metric_key() {
        assert_eq!(
            QueryShadowTerminal::InvalidPrimary.metric_key(),
            "invalid_primary"
        );
    }

    #[test]
    fn shadow_failure_stages_have_fixed_metric_keys() {
        assert_eq!(
            QueryShadowFailureStage::TransactionSetup.metric_key(),
            "transaction_setup"
        );
        assert_eq!(
            QueryShadowFailureStage::EnvironmentSetup.metric_key(),
            "environment_setup"
        );
        assert_eq!(
            QueryShadowFailureStage::RouteAuthentication.metric_key(),
            "route_authentication"
        );
        assert_eq!(
            QueryShadowFailureStage::ModuleLoading.metric_key(),
            "module_loading"
        );
        assert_eq!(
            QueryShadowFailureStage::WasmExecution.metric_key(),
            "wasm_execution"
        );
        assert_eq!(
            QueryShadowFailureStage::VerifierExecution.metric_key(),
            "verifier_execution"
        );
        assert_eq!(
            QueryShadowFailureStage::TransactionFinalization.metric_key(),
            "transaction_finalization"
        );
        assert_eq!(
            QueryShadowFailureStage::Unclassified.metric_key(),
            "unclassified"
        );
        assert_eq!(
            QueryShadowFailureReason::VerifierExecution.metric_key(),
            "verifier_execution"
        );
    }
}
