#![allow(non_snake_case)]
use std::{
    collections::BTreeMap,
    fmt,
    str::FromStr,
};

use anyhow::Context;
use common::{
    audit_log_lines::AuditLogLine,
    bootstrap_model::components::handles::FunctionHandle,
    components::{
        CanonicalizedComponentFunctionPath,
        PublicFunctionPath,
        Reference,
        ResolvedComponentFunctionPath,
        Resource,
    },
    document::{
        DeveloperDocument,
        PackedDocument,
        PendingDocument,
    },
    knobs::{
        COMPONENT_GET_USER_IDENTITY_LOG_SAMPLE_RATIO,
        MAX_REACTOR_CALL_DEPTH,
        MAX_SYSCALL_BATCH_SIZE,
        TRANSACTION_MAX_READ_SIZE_ROWS,
    },
    query::{
        Cursor,
        CursorPosition,
        Query,
    },
    query_journal::QueryJournal,
    runtime::{
        Runtime,
        UnixTimestamp,
    },
    try_anyhow,
    types::{
        AllowedVisibility,
        UdfType,
        WriteTimestamp,
    },
    value::ConvexValue,
    version::Version,
};
use database::{
    query::{
        query_batch_next_document,
        PaginationOptions,
        QueryDocument,
        TableFilter,
    },
    soft_data_limit,
    table_summary::table_summary_bootstrapping_error,
    BootstrapComponentsModel,
    DeveloperQuery,
    PatchValue,
    Transaction,
    TransactionLimits,
    UserFacingModel,
};
use deno_core::v8;
use errors::{
    ErrorCode,
    ErrorMetadata,
    ErrorMetadataAnyhowExt,
};
use headers::ContentType;
use itertools::Itertools;
use model::{
    components::{
        handles::FunctionHandlesModel,
        ComponentsModel,
    },
    deployment_audit_log::{
        types::DeploymentAuditLogEvent,
        DeploymentAuditLogModel,
    },
    file_storage::{
        types::FileStorageEntry,
        BatchKey,
        FileStorageId,
    },
    scheduled_jobs::{
        VirtualSchedulerModel,
        MIN_NPM_VERSION_MUTATION_SELF_CANCEL,
    },
    virtual_system_mapping,
};
use rand::Rng;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    json,
    value::RawValue,
    Value as JsonValue,
};
use sync_types::{
    types::SerializedArgs,
    AuthenticationToken,
};
use udf::{
    helpers::UdfArgsJson,
    validation::{
        validate_schedule_args_with_input,
        PendingArgsPolicy,
        ScheduleArgumentInput,
        ValidatedPathAndArgs,
    },
    HostOperation,
    HostOperationErrorV1,
    LogicalHostOperation,
    LogicalHostOperationStatus,
    NestedUdfOutcome,
};
use value::{
    heap_size::HeapSize,
    id_v6::DeveloperDocumentId,
    obj,
    sha256::Sha256Digest,
    wasm_abi::{
        self,
        DocumentResult,
    },
    ConvexArray,
    ConvexObject,
    PendingValue,
    Size,
    TableName,
    TableNamespace,
};

use super::DatabaseUdfSyscallProvider;
use crate::{
    client::{
        EnvironmentData,
        UdfCallback,
        UdfRequest,
    },
    environment::{
        action::parse_name_or_reference,
        helpers::{
            check_table_name,
            parse_version,
            remove_rejected_before_execution,
            with_argument_error,
            ArgName,
        },
    },
    metrics::{
        async_syscall_timer,
        log_component_get_user_identity,
        log_run_udf,
    },
};

/// A type for UDFs that can be run as subtransactions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NestedUdfType {
    Query,
    Mutation,
    SnapshotQuery,
}

impl NestedUdfType {
    /// Returns the underlying UdfType used for execution.
    pub fn execution_type(&self) -> UdfType {
        match self {
            Self::Query | Self::SnapshotQuery => UdfType::Query,
            Self::Mutation => UdfType::Mutation,
        }
    }
}

impl fmt::Display for NestedUdfType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query => write!(f, "Query"),
            Self::Mutation => write!(f, "Mutation"),
            Self::SnapshotQuery => write!(f, "SnapshotQuery"),
        }
    }
}

impl FromStr for NestedUdfType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "snapshotQuery" => Ok(Self::SnapshotQuery),
            _ => {
                let udf_type: UdfType = s.parse()?;
                match udf_type {
                    UdfType::Query => Ok(Self::Query),
                    UdfType::Mutation => Ok(Self::Mutation),
                    _ => anyhow::bail!(
                        "Only queries and mutations can be called as nested UDFs, got {udf_type}"
                    ),
                }
            },
        }
    }
}

pub struct PendingSyscall {
    pub name: String,
    pub args: JsonValue,
    pub resolver: v8::Global<v8::PromiseResolver>,
}

impl HeapSize for PendingSyscall {
    fn heap_size(&self) -> usize {
        self.name.heap_size() + self.args.heap_size()
    }
}

// Checks if the underlying table and the request's expectation for the table
// line up.
pub fn system_table_guard(name: &TableName, expect_system_table: bool) -> anyhow::Result<()> {
    if expect_system_table && !name.is_system() {
        return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
            "SystemTableError",
            "User tables cannot be accessed with db.system."
        )));
    } else if !expect_system_table && name.is_system() {
        return Err(anyhow::anyhow!(ErrorMetadata::bad_request(
            "SystemTableError",
            "System tables can only be accessed with db.system."
        )));
    }
    Ok(())
}

/// A batch of async syscalls that can run "in parallel", where they actually
/// execute in a batch for determinism, but as far as the js promises are
/// concerned, they're running in parallel.
/// This could conceivably run reads
/// (get/queryStreamNext/queryPage/storageGetUrl) all in a single batch.
/// We could also run inserts, deletes, patches, and replaces in a batch
/// together, disjoint from reads, as long as the affected IDs are disjoint.
/// For now, we only allow batches of `db.get`s and `db.query`s.
/// TODO(lee) implement other kinds of batches.
#[derive(Debug, Clone, PartialEq)]
pub enum AsyncSyscallBatch {
    Reads(Vec<AsyncRead>),
    StorageGetUrls(Vec<JsonValue>),
    TypedQueryPage(TypedQueryPageArgs),
    TypedWrite(TypedDatabaseWrite),
    TypedRunUdf(TypedRunUdfArgs),
    TypedSchedule(TypedScheduleArgs),
    Unbatched { name: String, args: JsonValue },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TypedDatabaseWrite {
    Insert {
        table: String,
        value: PendingValue,
    },
    Patch {
        table: Option<String>,
        id: String,
        value: PatchValue,
    },
    Replace {
        table: Option<String>,
        id: String,
        value: PendingValue,
    },
    Delete {
        table: Option<String>,
        id: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedRunUdfArgs {
    pub udf_type: NestedUdfType,
    pub name: Option<String>,
    pub reference: Option<String>,
    pub function_handle: Option<String>,
    pub args: PendingValue,
    pub transaction_limits: Option<TransactionLimits>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedScheduleArgs {
    pub name: Option<String>,
    pub reference: Option<String>,
    pub function_handle: Option<String>,
    pub timestamp_seconds: f64,
    pub args: ConvexArray,
}

struct ScheduleCallArgs {
    name: Option<String>,
    reference: Option<String>,
    function_handle: Option<String>,
    timestamp_seconds: f64,
    args: ScheduleArgumentInput,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AsyncRead {
    Get(DatabaseGetArguments),
    QueryStreamNext(JsonValue),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseGetArguments {
    LegacyJson(JsonValue),
    Typed(DatabaseGetArgs),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseGetArgs {
    #[serde(default)]
    pub table: Option<String>,
    pub id: String,
    #[serde(default)]
    pub is_system: bool,
    #[serde(default)]
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedQueryPageArgs {
    pub query: Query,
    pub cursor: Option<String>,
    pub end_cursor: Option<String>,
    pub page_size: usize,
    pub maximum_rows_read: Option<usize>,
    pub maximum_bytes_read: Option<usize>,
    pub version: Option<Version>,
}

impl AsyncRead {
    fn logical_host_operation(&self) -> LogicalHostOperation {
        match self {
            Self::Get(_) => LogicalHostOperation::DatabaseGet,
            Self::QueryStreamNext(_) => LogicalHostOperation::DatabaseQueryStreamNext,
        }
    }
}

pub struct AsyncSyscallResult {
    pub(super) result: anyhow::Result<AsyncSyscallValue>,
    pub(super) host_operation_error: Option<HostOperationErrorV1>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum AsyncSyscallValue {
    Json(String),
    OwnedJson(JsonValue),
    Pending(PendingValue),
    Document(QueryDocumentResult),
    DocumentCollection(Vec<QueryDocumentResult>),
    QueryStreamNext(Option<QueryDocumentResult>),
    Page(QueryPageResult),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum QueryDocumentResult {
    Packed(PackedDocument),
    Typed(PendingValue),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct QueryPageResult {
    page: Vec<QueryDocumentResult>,
    is_done: bool,
    continue_cursor: String,
    split_cursor: Option<String>,
    page_status: Option<&'static str>,
}

impl QueryDocumentResult {
    fn as_value_abi_document(&self) -> DocumentResult<'_> {
        match self {
            Self::Packed(document) => DocumentResult::Packed(document.value().as_bytes()),
            Self::Typed(value) => DocumentResult::Pending(value),
        }
    }

    fn host_owned_bytes(&self) -> usize {
        match self {
            Self::Packed(document) => document.heap_size(),
            Self::Typed(value) => pending_value_heap_size(value),
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Packed(document) => document.size(),
            Self::Typed(value) => value.size(),
        }
    }

    fn to_raw_json(&self) -> anyhow::Result<Box<RawValue>> {
        match self {
            Self::Packed(document) => Ok(value::json_value::to_raw_value(
                document.value().as_ref().open()?,
            )?),
            Self::Typed(value) => Ok(serde_json::value::to_raw_value(
                &value.to_uncommitted_json_serializable(),
            )?),
        }
    }

    fn into_json_value(self) -> anyhow::Result<JsonValue> {
        match self {
            Self::Packed(document) => Ok(value::json_value::serialize(
                document.value().as_ref().open()?,
                serde_json::value::Serializer,
            )?),
            Self::Typed(value) => Ok(value.to_uncommitted_json()),
        }
    }
}

fn convex_value_heap_size(value: &ConvexValue) -> usize {
    match value {
        ConvexValue::Null
        | ConvexValue::Boolean(_)
        | ConvexValue::Float64(_)
        | ConvexValue::Int64(_) => 0,
        ConvexValue::String(value) => value.heap_size(),
        ConvexValue::Bytes(value) => value.heap_size(),
        ConvexValue::Array(values) => {
            values.len() * std::mem::size_of::<ConvexValue>()
                + values.iter().map(convex_value_heap_size).sum::<usize>()
        },
        ConvexValue::Object(fields) => {
            let entry_bytes = std::mem::size_of::<(value::FieldName, ConvexValue)>()
                + 4 * std::mem::size_of::<usize>();
            fields.len() * entry_bytes
                + fields
                    .iter()
                    .map(|(name, value)| name.heap_size() + convex_value_heap_size(value))
                    .sum::<usize>()
        },
    }
}

fn pending_value_heap_size(value: &PendingValue) -> usize {
    match value {
        PendingValue::Concrete(value) => convex_value_heap_size(value),
        PendingValue::CommitTs => 0,
        PendingValue::Array { values, .. } => {
            values.capacity() * std::mem::size_of::<PendingValue>()
                + values.iter().map(pending_value_heap_size).sum::<usize>()
        },
        PendingValue::Object { fields, .. } => {
            let entry_bytes = std::mem::size_of::<(value::FieldName, PendingValue)>()
                + 4 * std::mem::size_of::<usize>();
            fields.len() * entry_bytes
                + fields
                    .iter()
                    .map(|(name, value)| name.heap_size() + pending_value_heap_size(value))
                    .sum::<usize>()
        },
    }
}

impl AsyncSyscallValue {
    pub(super) fn host_owned_bytes(&self) -> usize {
        match self {
            Self::Json(value) => value.heap_size(),
            Self::OwnedJson(value) => value.heap_size(),
            Self::Pending(value) => pending_value_heap_size(value),
            Self::Document(document) | Self::QueryStreamNext(Some(document)) => {
                document.host_owned_bytes()
            },
            Self::DocumentCollection(documents) => {
                documents.capacity() * std::mem::size_of::<QueryDocumentResult>()
                    + documents
                        .iter()
                        .map(QueryDocumentResult::host_owned_bytes)
                        .sum::<usize>()
            },
            Self::QueryStreamNext(None) => 0,
            Self::Page(page) => {
                page.page.capacity() * std::mem::size_of::<QueryDocumentResult>()
                    + page
                        .page
                        .iter()
                        .map(QueryDocumentResult::host_owned_bytes)
                        .sum::<usize>()
                    + page.continue_cursor.heap_size()
                    + page.split_cursor.heap_size()
            },
        }
    }

    pub(super) fn into_value_abi(
        self,
        maximum_bytes: usize,
        allows_pending: bool,
    ) -> anyhow::Result<Vec<u8>> {
        Ok(match self {
            Self::Json(value) => {
                let value: JsonValue = serde_json::from_str(&value)?;
                if allows_pending {
                    let value = PendingValue::from_uncommitted_json(value)?;
                    wasm_abi::encode_pending(&value, maximum_bytes)?
                } else {
                    let value = ConvexValue::try_from(value)?;
                    wasm_abi::encode_committed(&value, maximum_bytes)?
                }
            },
            Self::OwnedJson(value) => {
                if allows_pending {
                    let value = PendingValue::from_uncommitted_json(value)?;
                    wasm_abi::encode_pending(&value, maximum_bytes)?
                } else {
                    let value = ConvexValue::try_from(value)?;
                    wasm_abi::encode_committed(&value, maximum_bytes)?
                }
            },
            Self::Pending(value) => {
                if allows_pending {
                    wasm_abi::encode_pending(&value, maximum_bytes)?
                } else {
                    let value = value.try_into_concrete()?;
                    wasm_abi::encode_committed(&value, maximum_bytes)?
                }
            },
            Self::Document(document) => match document.as_value_abi_document() {
                DocumentResult::Packed(bytes) => {
                    wasm_abi::encode_packed_document(bytes, maximum_bytes)?
                },
                DocumentResult::Pending(value) => wasm_abi::encode_pending(value, maximum_bytes)?,
            },
            Self::DocumentCollection(documents) => {
                let documents = documents
                    .iter()
                    .map(QueryDocumentResult::as_value_abi_document)
                    .collect::<Vec<_>>();
                wasm_abi::encode_document_collection(&documents, maximum_bytes)?
            },
            Self::QueryStreamNext(document) => wasm_abi::encode_query_stream_next(
                document
                    .as_ref()
                    .map(QueryDocumentResult::as_value_abi_document),
                maximum_bytes,
            )?,
            Self::Page(page) => {
                let documents = page
                    .page
                    .iter()
                    .map(QueryDocumentResult::as_value_abi_document)
                    .collect::<Vec<_>>();
                wasm_abi::encode_query_page(
                    &documents,
                    page.is_done,
                    &page.continue_cursor,
                    page.split_cursor.as_deref(),
                    page.page_status,
                    maximum_bytes,
                )?
            },
        })
    }

    pub(super) fn into_json_value(self) -> anyhow::Result<JsonValue> {
        match self {
            Self::Json(value) => Ok(serde_json::from_str(&value)?),
            Self::OwnedJson(value) => Ok(value),
            Self::Pending(value) => Ok(value.to_uncommitted_json()),
            Self::Document(document) => document.into_json_value(),
            Self::DocumentCollection(documents) => Ok(JsonValue::Array(
                documents
                    .into_iter()
                    .map(QueryDocumentResult::into_json_value)
                    .collect::<anyhow::Result<Vec<_>>>()?,
            )),
            Self::QueryStreamNext(value) => {
                let done = value.is_none();
                let value = match value {
                    Some(document) => document.into_json_value()?,
                    None => JsonValue::Null,
                };
                Ok(json!({ "value": value, "done": done }))
            },
            Self::Page(page) => {
                let documents = page
                    .page
                    .into_iter()
                    .map(QueryDocumentResult::into_json_value)
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Ok(json!({
                    "page": documents,
                    "isDone": page.is_done,
                    "continueCursor": page.continue_cursor,
                    "splitCursor": page.split_cursor,
                    "pageStatus": page.page_status,
                }))
            },
        }
    }

    pub(super) fn into_json_string(self) -> anyhow::Result<String> {
        match self {
            Self::Json(value) => Ok(value),
            Self::OwnedJson(value) => Ok(serde_json::to_string(&value)?),
            Self::Pending(value) => Ok(serde_json::value::to_raw_value(
                &value.to_uncommitted_json_serializable(),
            )?
            .get()
            .to_owned()),
            Self::Document(document) => Ok(document.to_raw_json()?.get().to_owned()),
            Self::DocumentCollection(documents) => {
                let documents = documents
                    .iter()
                    .map(QueryDocumentResult::to_raw_json)
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Ok(serde_json::value::to_raw_value(&documents)?
                    .get()
                    .to_owned())
            },
            Self::QueryStreamNext(value) => {
                #[derive(Serialize)]
                struct QueryStreamNextResult {
                    value: Box<RawValue>,
                    done: bool,
                }
                let done = value.is_none();
                let value = match value {
                    Some(document) => document.to_raw_json()?,
                    None => RawValue::NULL.to_owned(),
                };
                Ok(
                    serde_json::value::to_raw_value(&QueryStreamNextResult { value, done })?
                        .get()
                        .to_owned(),
                )
            },
            Self::Page(page) => {
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                struct JsonPageResult {
                    page: Vec<Box<RawValue>>,
                    is_done: bool,
                    continue_cursor: String,
                    split_cursor: Option<String>,
                    page_status: Option<&'static str>,
                }
                let documents = page
                    .page
                    .iter()
                    .map(QueryDocumentResult::to_raw_json)
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Ok(serde_json::value::to_raw_value(&JsonPageResult {
                    page: documents,
                    is_done: page.is_done,
                    continue_cursor: page.continue_cursor,
                    split_cursor: page.split_cursor,
                    page_status: page.page_status,
                })?
                .get()
                .to_owned())
            },
        }
    }

    fn observed_bytes(&self) -> usize {
        match self {
            Self::Json(value) => value.len(),
            Self::OwnedJson(value) => value.heap_size(),
            Self::Pending(value) => value.size(),
            Self::Document(document) | Self::QueryStreamNext(Some(document)) => document.size(),
            Self::DocumentCollection(documents) => {
                documents.iter().map(QueryDocumentResult::size).sum()
            },
            Self::QueryStreamNext(None) => 0,
            Self::Page(page) => page.page.iter().map(QueryDocumentResult::size).sum(),
        }
    }
}

struct NestedUdfExecutionResult {
    result: anyhow::Result<PendingValue>,
    host_operation_error: Option<HostOperationErrorV1>,
}

fn nested_udf_async_syscall_result(
    nested_udf_result: anyhow::Result<NestedUdfExecutionResult>,
) -> AsyncSyscallResult {
    let NestedUdfExecutionResult {
        result,
        host_operation_error,
    } = match nested_udf_result {
        Ok(result) => result,
        Err(error) => return AsyncSyscallResult::with_host_operation_error(Err(error), None),
    };
    match result {
        Ok(value) => {
            assert!(
                host_operation_error.is_none(),
                "successful nested UDF carried a terminal host operation error"
            );
            AsyncSyscallResult {
                result: Ok(AsyncSyscallValue::Pending(value)),
                host_operation_error: None,
            }
        },
        Err(error) => {
            AsyncSyscallResult::with_host_operation_error(Err(error), host_operation_error)
        },
    }
}

impl AsyncSyscallResult {
    fn from_json(result: anyhow::Result<Box<RawValue>>) -> Self {
        Self {
            result: result.map(|value| AsyncSyscallValue::Json(value.get().to_owned())),
            host_operation_error: None,
        }
    }

    fn with_host_operation_error(
        result: anyhow::Result<Box<RawValue>>,
        host_operation_error: Option<HostOperationErrorV1>,
    ) -> Self {
        Self {
            result: result.map(|value| AsyncSyscallValue::Json(value.get().to_owned())),
            host_operation_error,
        }
    }

    pub(super) fn is_ok(&self) -> bool {
        self.result.is_ok()
    }
}

fn missing_document_host_operation_error<T>(
    document_id: Option<&str>,
    operation: HostOperation,
    result: &anyhow::Result<T>,
) -> Option<HostOperationErrorV1> {
    let error = result.as_ref().err()?;
    let ErrorMetadata {
        code: ErrorCode::BadRequest,
        short_msg,
        ..
    } = error.downcast_ref::<ErrorMetadata>()?
    else {
        return None;
    };
    if short_msg != "NonexistentDocument" {
        return None;
    }
    let document_id = DeveloperDocumentId::decode(document_id?).ok()?;
    Some(HostOperationErrorV1::NonexistentDocument {
        operation,
        document_id,
    })
}

impl AsyncSyscallBatch {
    pub fn new(name: String, args: JsonValue) -> Self {
        match &*name {
            "1.0/get" => Self::Reads(vec![AsyncRead::Get(DatabaseGetArguments::LegacyJson(args))]),
            "1.0/queryStreamNext" => Self::Reads(vec![AsyncRead::QueryStreamNext(args)]),
            "1.0/storageGetUrl" => Self::StorageGetUrls(vec![args]),
            _ => Self::Unbatched { name, args },
        }
    }

    pub fn typed_get(args: DatabaseGetArgs) -> Self {
        Self::Reads(vec![AsyncRead::Get(DatabaseGetArguments::Typed(args))])
    }

    pub fn typed_query_page(args: TypedQueryPageArgs) -> Self {
        Self::TypedQueryPage(args)
    }

    pub fn typed_write(args: TypedDatabaseWrite) -> Self {
        Self::TypedWrite(args)
    }

    pub fn typed_run_udf(args: TypedRunUdfArgs) -> Self {
        Self::TypedRunUdf(args)
    }

    pub fn typed_schedule(args: TypedScheduleArgs) -> Self {
        Self::TypedSchedule(args)
    }

    pub fn can_push(&self, name: &str, _args: &JsonValue) -> bool {
        if self.len() >= *MAX_SYSCALL_BATCH_SIZE {
            return false;
        }
        match (self, name) {
            (Self::Reads(_), "1.0/get") => true,
            (Self::Reads(_), "1.0/queryStreamNext") => true,
            (Self::Reads(_), _) => false,
            (Self::StorageGetUrls(_), "1.0/storageGetUrl") => true,
            (Self::StorageGetUrls(_), _) => false,
            (Self::TypedQueryPage(_), _) => false,
            (Self::TypedWrite(_), _) => false,
            (Self::TypedRunUdf(_), _) => false,
            (Self::TypedSchedule(_), _) => false,
            (Self::Unbatched { .. }, _) => false,
        }
    }

    pub fn push(&mut self, name: String, args: JsonValue) -> anyhow::Result<()> {
        match (&mut *self, &*name) {
            (Self::Reads(batch_args), "1.0/get") => {
                batch_args.push(AsyncRead::Get(DatabaseGetArguments::LegacyJson(args)))
            },
            (Self::Reads(batch_args), "1.0/queryStreamNext") => {
                batch_args.push(AsyncRead::QueryStreamNext(args))
            },
            (Self::StorageGetUrls(batch_args), "1.0/storageGetUrl") => {
                batch_args.push(args);
            },
            _ => anyhow::bail!("cannot push {name} onto {self:?}"),
        }
        Ok(())
    }

    pub fn push_typed_get(&mut self, args: DatabaseGetArgs) -> anyhow::Result<()> {
        match self {
            Self::Reads(reads) if reads.len() < *MAX_SYSCALL_BATCH_SIZE => {
                reads.push(AsyncRead::Get(DatabaseGetArguments::Typed(args)));
                Ok(())
            },
            _ => anyhow::bail!("cannot push typed db.get onto {self:?}"),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            // 1.0/get is grouped in with 1.0/queryStreamNext.
            Self::Reads(_) => "1.0/queryStreamNext",
            Self::StorageGetUrls(_) => "1.0/storageGetUrl",
            Self::TypedQueryPage(_) => "1.0/queryPage",
            Self::TypedWrite(args) => match args {
                TypedDatabaseWrite::Insert { .. } => "1.0/insert",
                TypedDatabaseWrite::Patch { .. } => "1.0/shallowMerge",
                TypedDatabaseWrite::Replace { .. } => "1.0/replace",
                TypedDatabaseWrite::Delete { .. } => "1.0/remove",
            },
            Self::TypedRunUdf(_) => "1.0/runUdf",
            Self::TypedSchedule(_) => "1.0/schedule",
            Self::Unbatched { name, .. } => name,
        }
    }

    pub(crate) fn logical_host_operations(&self) -> Vec<LogicalHostOperation> {
        match self {
            Self::Reads(reads) => reads
                .iter()
                .map(AsyncRead::logical_host_operation)
                .collect(),
            Self::StorageGetUrls(args) => {
                vec![LogicalHostOperation::StorageGetUrl; args.len()]
            },
            Self::TypedQueryPage(_) => vec![LogicalHostOperation::DatabaseQueryPage],
            Self::TypedWrite(args) => vec![match args {
                TypedDatabaseWrite::Insert { .. } => LogicalHostOperation::DatabaseInsert,
                TypedDatabaseWrite::Patch { .. } => LogicalHostOperation::DatabasePatch,
                TypedDatabaseWrite::Replace { .. } => LogicalHostOperation::DatabaseReplace,
                TypedDatabaseWrite::Delete { .. } => LogicalHostOperation::DatabaseDelete,
            }],
            Self::TypedRunUdf(_) => vec![LogicalHostOperation::RunUdf],
            Self::TypedSchedule(_) => vec![LogicalHostOperation::Schedule],
            Self::Unbatched { name, .. } => vec![logical_async_syscall_operation(name)],
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Reads(args) => args.len(),
            Self::StorageGetUrls(args) => args.len(),
            Self::TypedQueryPage(_) => 1,
            Self::TypedWrite(_) => 1,
            Self::TypedRunUdf(_) => 1,
            Self::TypedSchedule(_) => 1,
            Self::Unbatched { .. } => 1,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn logical_async_syscall_operation(name: &str) -> LogicalHostOperation {
    match name {
        "1.0/count" => LogicalHostOperation::DatabaseCount,
        "1.0/insert" => LogicalHostOperation::DatabaseInsert,
        "1.0/shallowMerge" => LogicalHostOperation::DatabasePatch,
        "1.0/replace" => LogicalHostOperation::DatabaseReplace,
        "1.0/remove" => LogicalHostOperation::DatabaseDelete,
        "1.0/queryPage" => LogicalHostOperation::DatabaseQueryPage,
        "1.0/getTransactionMetrics" => LogicalHostOperation::TransactionMetrics,
        "1.0/getFunctionMetadata" => LogicalHostOperation::FunctionMetadata,
        "1.0/getDeploymentMetadata" => LogicalHostOperation::DeploymentMetadata,
        "1.0/getRequestMetadata" => LogicalHostOperation::RequestMetadata,
        "1.0/getUserIdentity" => LogicalHostOperation::UserIdentity,
        "1.0/storageDelete" => LogicalHostOperation::StorageDelete,
        "1.0/storageGetMetadata" => LogicalHostOperation::StorageGetMetadata,
        "1.0/storageGenerateUploadUrl" => LogicalHostOperation::StorageGenerateUploadUrl,
        "1.0/schedule" => LogicalHostOperation::Schedule,
        "1.0/cancel_job" => LogicalHostOperation::CancelJob,
        "1.0/auditLog" => LogicalHostOperation::AuditLog,
        "1.0/writeDeploymentAuditLog" => LogicalHostOperation::WriteDeploymentAuditLog,
        "1.0/runUdf" => LogicalHostOperation::RunUdf,
        "1.0/createFunctionHandle" => LogicalHostOperation::CreateFunctionHandle,
        _ => LogicalHostOperation::UnknownAsyncSyscall,
    }
}

pub struct QueryManager<RT: Runtime> {
    next_id: u32,
    developer_queries: BTreeMap<u32, DeveloperQuery<RT>>,
}

impl<RT: Runtime> QueryManager<RT> {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            developer_queries: BTreeMap::new(),
        }
    }

    pub fn put_developer(&mut self, query: DeveloperQuery<RT>) -> u32 {
        let id = self.next_id;
        self.developer_queries.insert(id, query);
        self.next_id += 1;
        id
    }

    pub fn take_developer(&mut self, id: u32) -> Option<DeveloperQuery<RT>> {
        self.developer_queries.remove(&id)
    }

    pub fn insert_developer(&mut self, id: u32, query: DeveloperQuery<RT>) {
        self.developer_queries.insert(id, query);
    }

    pub fn cleanup_developer(&mut self, id: u32) -> bool {
        self.developer_queries.remove(&id).is_some()
    }

    pub fn clear_developer_queries(&mut self) {
        self.developer_queries.clear();
    }

    pub fn has_developer_queries(&self) -> bool {
        !self.developer_queries.is_empty()
    }
}

pub enum ManagedQuery<RT: Runtime> {
    Pending {
        query: Query,
        version: Option<Version>,
    },
    Active(DeveloperQuery<RT>),
}

pub type QueryId = u32;

impl<RT: Runtime> DatabaseUdfSyscallProvider<RT> {
    fn is_system(&self) -> bool {
        self.path.udf_path.is_system()
    }

    fn table_filter(&self) -> TableFilter {
        if self.path.udf_path.is_system() {
            TableFilter::IncludePrivateSystemTables
        } else {
            TableFilter::ExcludePrivateSystemTables
        }
    }

    async fn validate_schedule_args(
        &mut self,
        path: CanonicalizedComponentFunctionPath,
        args: ScheduleArgumentInput,
        scheduled_ts: UnixTimestamp,
    ) -> anyhow::Result<(CanonicalizedComponentFunctionPath, ConvexArray)> {
        validate_schedule_args_with_input(
            path,
            args,
            scheduled_ts,
            self.phase.unix_timestamp()?,
            self.phase.tx()?,
        )
        .await
    }

    pub(super) async fn file_storage_generate_upload_url(&mut self) -> anyhow::Result<String> {
        let issued_ts = self.phase.unix_timestamp()?;
        let component = self.phase.component()?;
        let post_url = self
            .file_storage
            .generate_upload_url(self.phase.tx()?, &self.key_broker, issued_ts, component)
            .await?;
        Ok(post_url)
    }

    pub(super) async fn file_storage_get_url_batch(
        &mut self,
        storage_ids: BTreeMap<BatchKey, FileStorageId>,
    ) -> BTreeMap<BatchKey, anyhow::Result<Option<String>>> {
        let component = match self.phase.component() {
            Ok(c) => c,
            Err(e) => {
                return storage_ids
                    .into_keys()
                    .map(|batch_key| (batch_key, Err(e.clone_error())))
                    .collect();
            },
        };
        let tx = match self.phase.tx() {
            Ok(tx) => tx,
            Err(e) => {
                return storage_ids
                    .into_keys()
                    .map(|batch_key| (batch_key, Err(e.clone_error())))
                    .collect();
            },
        };
        self.file_storage
            .get_url_batch(tx, component, storage_ids)
            .await
    }

    pub(super) async fn file_storage_delete(
        &mut self,
        storage_id: FileStorageId,
    ) -> anyhow::Result<()> {
        let component = self.phase.component()?;
        self.file_storage
            .delete(self.phase.tx()?, component.into(), storage_id)
            .await
    }

    pub(super) async fn file_storage_get_entry(
        &mut self,
        storage_id: FileStorageId,
    ) -> anyhow::Result<Option<FileStorageEntry>> {
        let component = self.phase.component()?;
        self.file_storage
            .get_file_entry(self.phase.tx()?, component.into(), storage_id)
            .await
    }

    #[fastrace::trace]
    async fn run_udf(
        &mut self,
        nested_udf_type: NestedUdfType,
        path: ResolvedComponentFunctionPath,
        args: PendingValue,
        transaction_limits: Option<TransactionLimits>,
        udf_callback: impl UdfCallback<RT>,
    ) -> anyhow::Result<NestedUdfExecutionResult> {
        match (self.udf_type, nested_udf_type) {
            // Queries can call other queries, but not snapshot queries.
            (UdfType::Query, NestedUdfType::Query) => (),
            // Mutations can call queries (including snapshot queries) or mutations.
            (
                UdfType::Mutation,
                NestedUdfType::Query | NestedUdfType::SnapshotQuery | NestedUdfType::Mutation,
            ) => (),
            _ => {
                anyhow::bail!(ErrorMetadata::bad_request(
                    "InvalidFunctionCall",
                    format!(
                        "Cannot call a {} function from a {} function",
                        nested_udf_type, self.udf_type
                    )
                ));
            },
        }
        let tx = self.phase.tx()?;
        let called_component_id = path.component;

        let execution_type = nested_udf_type.execution_type();
        let pending_args_policy = match self.udf_type {
            UdfType::Mutation => PendingArgsPolicy::Allow,
            _ => PendingArgsPolicy::Reject,
        };
        let path_and_args_result = ValidatedPathAndArgs::new_with_returns_validator(
            AllowedVisibility::All,
            tx,
            PublicFunctionPath::ResolvedComponent(path.clone()),
            SerializedArgs::from_raw(serde_json::value::to_raw_value(&[
                args.to_uncommitted_json_serializable()
            ])?),
            execution_type,
            pending_args_policy,
        )
        .await?;
        // We don't need to store visibility_info for non-queries
        let (path_and_args, returns_validator, _visibility_info) = match path_and_args_result {
            Ok(r) => r,
            Err(e) => {
                // TODO: Propagate this JsError to user space correctly.
                anyhow::bail!(ErrorMetadata::bad_request("InvalidArgs", e.message));
            },
        };

        // NB: Since this is a user error, we need to do this check before we take the
        // transaction below.
        if self.reactor_depth >= *MAX_REACTOR_CALL_DEPTH {
            anyhow::bail!(ErrorMetadata::bad_request(
                "MaximumCallDepthExceeded",
                "Cross component call depth limit exceeded. Do you have an infinite loop in your \
                 app?"
            ));
        }
        let new_reactor_depth = self.reactor_depth + 1;

        let (initial_tx, rng_seed, unix_timestamp) = self.phase.start_nested_udf()?;
        let (mut nested_tx, saved_tx) = match nested_udf_type {
            NestedUdfType::SnapshotQuery => {
                (initial_tx.clone_for_snapshot_query(), Some(initial_tx))
            },
            _ => (initial_tx, None),
        };

        let tokens = nested_tx.begin_subtransaction();

        if let Some(limits) = transaction_limits {
            nested_tx.set_transaction_limits(limits);
        }

        let query_journal = if self.is_system() && nested_udf_type == NestedUdfType::Query {
            self.prev_journal.clone()
        } else {
            QueryJournal::new()
        };
        let (mut result_tx, outcome) = udf_callback
            .execute_nested_udf(
                self.client_id.clone(),
                UdfRequest {
                    udf_type: execution_type,
                    path_and_args,
                    transaction: nested_tx,
                    unix_timestamp,
                    journal: query_journal,
                    context: self.context.clone(),
                    environment_data: EnvironmentData {
                        key_broker: self.key_broker.clone(),
                        default_system_env_vars: self.phase.default_system_env_vars().clone(),
                        file_storage: self.file_storage.clone(),
                        module_loader: self.phase.module_loader().clone(),
                        deployment: self.deployment.clone(),
                        #[cfg(feature = "static-hermes-wasmtime-gate")]
                        // Nested UDFs execute through the existing V8 recursion path. Keep this
                        // boundary explicit so a future nested Wasm route fails before guest startup.
                        host_secret_values: None,
                    },
                    trace_host_operations: self.host_operation_trace.is_enabled(),
                    capture_handler_reads: self.phase.handler_read_capture_enabled(),
                    #[cfg(feature = "static-hermes-wasmtime-gate")]
                    shadow_work_guard: self.shadow_work_guard.clone(),
                },
                rng_seed,
                new_reactor_depth,
            )
            .await
            .map_err(|mut e| {
                e = remove_rejected_before_execution(e);
                if let Some(em) = e.downcast_mut::<ErrorMetadata>()
                    && em.is_deterministic_user_error()
                {
                    // This is a bit gross, but at this layer, "deterministic
                    // user errors" get converted into catchable JS exceptions.
                    // However, if there is an error at this point, we cannot
                    // return to JS because the transaction has been lost.
                    //
                    // So upgrade such an error to a non-catchable system error.
                    // However this should only be happening for nested system
                    // timeouts and so it's likely that this error will again be
                    // replaced with a higher error.
                    em.code = ErrorCode::OperationalInternalServerError;
                    tracing::warn!("Upgrading error from nested UDF: {e:#}");
                }
                e
            })?;
        match nested_udf_type {
            NestedUdfType::Mutation if outcome.result.is_err() => {
                result_tx.rollback_subtransaction(tokens)?
            },
            _ => result_tx.commit_subtransaction(tokens)?,
        }
        if let Some(tx) = saved_tx {
            self.phase.put_tx(tx)?;
        } else {
            self.phase.put_tx(result_tx)?;
        }

        let NestedUdfOutcome {
            result,
            observed_identity,
            observed_rng,
            observed_time,
            host_operation_error,
            host_operation_trace,
            syscall_trace,
            audit_log_lines,
            log_lines,
            journal,
        } = outcome;

        log_run_udf(
            self.udf_type,
            execution_type,
            self.phase.observed_identity(),
            observed_identity,
        );

        if observed_identity {
            self.phase.observe_identity()?;
        }

        if observed_rng {
            self.phase.observe_rng();
        }

        if observed_time {
            self.phase.unix_timestamp()?;
        }

        self.syscall_trace.merge(&syscall_trace);
        self.host_operation_trace.extend(host_operation_trace);

        if self.is_system() && nested_udf_type == NestedUdfType::Query && result.is_ok() {
            self.next_journal = journal;
        }

        // TODO(ENG-7663): restrict log lines within subfunctions instead of
        // limiting them only when they are returned to the parent.
        self.emit_sub_function_log_lines(path.for_logging(), log_lines);

        for audit_log_line in audit_log_lines {
            self.emit_audit_log_line(audit_log_line)?;
        }

        // TODO: How do we want to propagate stack traces between component calls?
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                return Ok(NestedUdfExecutionResult {
                    result: Err(error.into()),
                    host_operation_error,
                });
            },
        };
        let tx = self.phase.tx()?;
        let table_mapping = tx.table_mapping().namespace(called_component_id.into());
        if let Some(e) = returns_validator.check_pending_output(
            &result,
            &table_mapping,
            virtual_system_mapping(),
        )? {
            anyhow::bail!(ErrorMetadata::bad_request("InvalidReturnValue", e.message));
        }
        Ok(NestedUdfExecutionResult {
            result: Ok(result),
            host_operation_error: None,
        })
    }

    async fn create_function_handle(
        &mut self,
        path: CanonicalizedComponentFunctionPath,
    ) -> anyhow::Result<FunctionHandle> {
        let tx = self.phase.tx()?;
        FunctionHandlesModel::new(tx)
            .get_with_component_path(path)
            .await
    }

    async fn resolve(&mut self, reference: Reference) -> anyhow::Result<Resource> {
        let current_component_id = self.phase.component()?;
        let current_udf_path = self.path.udf_path.clone().into();

        let tx = self.phase.tx()?;

        ComponentsModel::new(tx)
            .resolve(current_component_id, Some(current_udf_path), &reference)
            .await
    }

    async fn lookup_function_handle(
        &mut self,
        handle: FunctionHandle,
    ) -> anyhow::Result<CanonicalizedComponentFunctionPath> {
        FunctionHandlesModel::new(self.phase.tx()?)
            .lookup(handle)
            .await
    }
}

#[fastrace::trace]
pub(super) async fn run_async_syscall_batch<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    batch: AsyncSyscallBatch,
    udf_callback: impl UdfCallback<RT>,
) -> Vec<AsyncSyscallResult> {
    crate::execution_observation::observe_poll(
        run_async_syscall_batch_inner(provider, batch, udf_callback),
        Some(crate::execution_observation::Owner::Provider),
        crate::execution_observation::Suspension::Provider,
    )
    .await
}

async fn run_async_syscall_batch_inner<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    batch: AsyncSyscallBatch,
    udf_callback: impl UdfCallback<RT>,
) -> Vec<AsyncSyscallResult> {
    let trace_entries = provider.host_operation_trace.is_enabled().then(|| {
        batch
            .logical_host_operations()
            .into_iter()
            .map(|operation| {
                provider
                    .host_operation_trace
                    .start(operation)
                    .expect("enabled host-operation trace did not start an entry")
            })
            .collect::<Vec<_>>()
    });
    let start = provider.phase.rt.monotonic_now();
    let batch_name = batch.name().to_string();
    let timer = async_syscall_timer(&batch_name);
    // Outer error is a system error that encompases the whole batch, while
    // inner errors are for individual batch items that may be system or developer
    // errors.
    let results = match batch {
        AsyncSyscallBatch::Reads(batch_args) => query_batch(provider, batch_args)
            .await
            .into_iter()
            .map(|result| AsyncSyscallResult {
                result,
                host_operation_error: None,
            })
            .collect(),
        AsyncSyscallBatch::StorageGetUrls(batch_args) => {
            storage_get_url_batch(provider, batch_args)
                .await
                .into_iter()
                .map(AsyncSyscallResult::from_json)
                .collect()
        },
        AsyncSyscallBatch::TypedQueryPage(args) => vec![AsyncSyscallResult {
            result: Box::pin(query_page_typed(provider, args))
                .await
                .map(AsyncSyscallValue::Page),
            host_operation_error: None,
        }],
        AsyncSyscallBatch::TypedWrite(write) => vec![match write {
            TypedDatabaseWrite::Insert { table, value } => AsyncSyscallResult {
                result: Box::pin(insert_typed(provider, table, value))
                    .await
                    .map(AsyncSyscallValue::Pending),
                host_operation_error: None,
            },
            TypedDatabaseWrite::Patch { table, id, value } => {
                let result = Box::pin(shallow_merge_typed(provider, table, &id, value)).await;
                let host_operation_error =
                    missing_document_host_operation_error(Some(&id), HostOperation::Patch, &result);
                AsyncSyscallResult {
                    result: result
                        .map(|document| AsyncSyscallValue::Pending(document.into_pending_value())),
                    host_operation_error,
                }
            },
            TypedDatabaseWrite::Replace { table, id, value } => {
                let result = Box::pin(replace_typed(provider, table, &id, value)).await;
                let host_operation_error = missing_document_host_operation_error(
                    Some(&id),
                    HostOperation::Replace,
                    &result,
                );
                AsyncSyscallResult {
                    result: result
                        .map(|document| AsyncSyscallValue::Pending(document.into_pending_value())),
                    host_operation_error,
                }
            },
            TypedDatabaseWrite::Delete { table, id } => {
                let result = Box::pin(remove_typed(provider, table, &id)).await;
                let host_operation_error = missing_document_host_operation_error(
                    Some(&id),
                    HostOperation::Delete,
                    &result,
                );
                AsyncSyscallResult {
                    result: result.map(|_| AsyncSyscallValue::OwnedJson(JsonValue::Null)),
                    host_operation_error,
                }
            },
        }],
        AsyncSyscallBatch::TypedRunUdf(args) => {
            vec![Box::pin(run_udf_typed(provider, args, udf_callback)).await]
        },
        AsyncSyscallBatch::TypedSchedule(args) => vec![AsyncSyscallResult {
            result: Box::pin(schedule_typed(provider, args))
                .await
                .map(AsyncSyscallValue::Pending),
            host_operation_error: None,
        }],
        AsyncSyscallBatch::Unbatched { name, args } => {
            let result = match &name[..] {
                // Database
                "1.0/count" => AsyncSyscallResult::from_json(Box::pin(count(provider, args)).await),
                "1.0/insert" => AsyncSyscallResult {
                    result: Box::pin(insert(provider, args))
                        .await
                        .map(AsyncSyscallValue::Pending),
                    host_operation_error: None,
                },
                "1.0/shallowMerge" => {
                    let host_operation_error_id = args
                        .get("id")
                        .and_then(JsonValue::as_str)
                        .map(str::to_owned);
                    let result = Box::pin(shallow_merge(provider, args)).await;
                    let host_operation_error = missing_document_host_operation_error(
                        host_operation_error_id.as_deref(),
                        HostOperation::Patch,
                        &result,
                    );
                    AsyncSyscallResult {
                        result: result.map(|document| {
                            AsyncSyscallValue::Pending(document.into_pending_value())
                        }),
                        host_operation_error,
                    }
                },
                "1.0/replace" => {
                    let host_operation_error_id = args
                        .get("id")
                        .and_then(JsonValue::as_str)
                        .map(str::to_owned);
                    let result = Box::pin(replace(provider, args)).await;
                    let host_operation_error = missing_document_host_operation_error(
                        host_operation_error_id.as_deref(),
                        HostOperation::Replace,
                        &result,
                    );
                    AsyncSyscallResult {
                        result: result.map(|document| {
                            AsyncSyscallValue::Pending(document.into_pending_value())
                        }),
                        host_operation_error,
                    }
                },
                "1.0/remove" => {
                    let host_operation_error_id = args
                        .get("id")
                        .and_then(JsonValue::as_str)
                        .map(str::to_owned);
                    let result = Box::pin(remove(provider, args)).await;
                    let host_operation_error = missing_document_host_operation_error(
                        host_operation_error_id.as_deref(),
                        HostOperation::Delete,
                        &result,
                    );
                    AsyncSyscallResult::with_host_operation_error(result, host_operation_error)
                },
                "1.0/queryPage" => AsyncSyscallResult {
                    result: Box::pin(query_page(provider, args))
                        .await
                        .map(AsyncSyscallValue::Page),
                    host_operation_error: None,
                },
                "1.0/getTransactionMetrics" => AsyncSyscallResult::from_json(tx_metrics(provider)),
                "1.0/getFunctionMetadata" => {
                    AsyncSyscallResult::from_json(function_metadata(provider))
                },
                "1.0/getDeploymentMetadata" => {
                    AsyncSyscallResult::from_json(Box::pin(deployment_metadata(provider)).await)
                },
                "1.0/getRequestMetadata" => {
                    AsyncSyscallResult::from_json(request_metadata(provider))
                },
                // Auth
                "1.0/getUserIdentity" => {
                    AsyncSyscallResult::from_json(Box::pin(get_user_identity(provider, args)).await)
                },
                // Storage
                "1.0/storageDelete" => {
                    AsyncSyscallResult::from_json(Box::pin(storage_delete(provider, args)).await)
                },
                "1.0/storageGetMetadata" => AsyncSyscallResult::from_json(
                    Box::pin(storage_get_metadata(provider, args)).await,
                ),
                "1.0/storageGenerateUploadUrl" => AsyncSyscallResult::from_json(
                    Box::pin(storage_generate_upload_url(provider, args)).await,
                ),
                "1.0/storageStore" => {
                    AsyncSyscallResult::from_json(Box::pin(storage_store(provider, args)).await)
                },
                // Scheduling
                "1.0/schedule" => {
                    AsyncSyscallResult::from_json(Box::pin(schedule(provider, args)).await)
                },
                "1.0/cancel_job" => {
                    AsyncSyscallResult::from_json(Box::pin(cancel_job(provider, args)).await)
                },

                // Audit logging
                "1.0/auditLog" => {
                    AsyncSyscallResult::from_json(Box::pin(audit_log(provider, args)).await)
                },
                // Audit logging (system UDFs only)
                "1.0/writeDeploymentAuditLog" => AsyncSyscallResult::from_json(
                    Box::pin(write_deployment_audit_log(provider, args)).await,
                ),

                // Components
                "1.0/runUdf" => Box::pin(run_udf(provider, args, udf_callback)).await,
                "1.0/createFunctionHandle" => AsyncSyscallResult::from_json(
                    Box::pin(create_function_handle(provider, args)).await,
                ),

                _ => AsyncSyscallResult::from_json(Err(ErrorMetadata::bad_request(
                    "UnknownAsyncOperation",
                    format!("Unknown async operation {name}"),
                )
                .into())),
            };
            vec![result]
        },
    };
    if let Some(trace_entries) = trace_entries {
        assert_eq!(
            trace_entries.len(),
            results.len(),
            "logical host-operation trace and async syscall result count diverged"
        );
        for (trace_entry, result) in trace_entries.into_iter().zip(&results) {
            provider.host_operation_trace.complete(
                Some(trace_entry),
                if result.is_ok() {
                    LogicalHostOperationStatus::Success
                } else {
                    LogicalHostOperationStatus::Failure
                },
            );
        }
    }
    provider.syscall_trace.log_async_syscall(
        batch_name,
        start.elapsed(),
        results.iter().all(AsyncSyscallResult::is_ok),
    );
    if crate::execution_observation::current().is_some() {
        crate::execution_observation::record_provider(
            results.len(),
            results
                .iter()
                .filter_map(|result| result.result.as_ref().ok())
                .map(AsyncSyscallValue::observed_bytes)
                .sum(),
        );
    }
    timer.finish();
    results
}

/// Returns the remaining headroom for this transaction before hitting
/// limits.
fn tx_metrics<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
) -> anyhow::Result<Box<RawValue>> {
    let tx = provider.phase.tx()?;
    let s = tx.execution_size();
    let limits = tx.transaction_limits();
    #[derive(Serialize)]
    struct LimitedValue {
        used: usize,
        remaining: isize,
    }
    let limit_value = |limit: usize, used: usize| {
        let remaining = limit as isize - used as isize;
        LimitedValue { used, remaining }
    };
    #[allow(non_snake_case)]
    #[derive(Serialize)]
    struct TxMetricsJson {
        bytesRead: LimitedValue,
        bytesWritten: LimitedValue,
        databaseQueries: LimitedValue,
        documentsRead: LimitedValue,
        documentsWritten: LimitedValue,
        functionsScheduled: LimitedValue,
        scheduledFunctionArgsBytes: LimitedValue,
        filesWritten: LimitedValue,
        fileWriteBytes: LimitedValue,
        filesRead: LimitedValue,
        fileReadBytes: LimitedValue,
    }
    Ok(serde_json::value::to_raw_value(&TxMetricsJson {
        bytesRead: limit_value(limits.bytes_read, s.read_size.total_document_size),
        bytesWritten: limit_value(limits.bytes_written, s.write_size.size),
        databaseQueries: limit_value(limits.database_queries, s.num_intervals),
        documentsRead: limit_value(limits.documents_read, s.read_size.total_document_count),
        documentsWritten: limit_value(limits.documents_written, s.write_size.num_writes),
        functionsScheduled: limit_value(limits.functions_scheduled, s.scheduled_size.num_writes),
        scheduledFunctionArgsBytes: limit_value(
            limits.scheduled_function_args_bytes,
            s.scheduled_size.size,
        ),
        filesWritten: limit_value(limits.files_written, s.file_storage_size.num_writes),
        fileWriteBytes: limit_value(limits.file_write_bytes, s.file_storage_size.write_size),
        filesRead: limit_value(limits.files_read, s.file_storage_size.num_reads),
        fileReadBytes: limit_value(limits.file_read_bytes, s.file_storage_size.read_size),
    })?)
}

/// Returns metadata about the currently executing function.
fn function_metadata<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
) -> anyhow::Result<Box<RawValue>> {
    Ok(serde_json::value::to_raw_value(&json!({
        "name": provider.path.udf_path.clone().strip().to_string(),
        "componentPath": provider.path.component_path.to_string(),
    }))?)
}

/// Returns metadata about the deployment this function is running on.
async fn deployment_metadata<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
) -> anyhow::Result<Box<RawValue>> {
    // This read participates in the caller's snapshot and OCC read set. Native
    // selection cannot race a lease acquisition that checks deployment metadata.
    let native_resident = model::source_packages::SourcePackageModel::new(
        provider.phase.tx()?,
        TableNamespace::Global,
    )
    .get_latest_record()
    .await?
    .and_then(|package| package.native_resident.clone());
    Ok(serde_json::value::to_raw_value(&json!({
        "name": provider.deployment.name,
        "region": provider.deployment.region,
        "class": provider.deployment.class,
        "nativeResident": native_resident,
    }))?)
}

/// Returns metadata about the originating HTTP request.
fn request_metadata<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
) -> anyhow::Result<Box<RawValue>> {
    anyhow::ensure!(
        provider.udf_type == UdfType::Mutation,
        ErrorMetadata::bad_request(
            "RequestMetadataNotAllowed",
            format!("Cannot get request metadata in a {}", provider.udf_type)
        )
    );
    // Expose the raw auth JWT the request was authenticated with, if any. Only
    // `User` identities carry a JWT (an OIDC or custom JWT); admin keys and
    // logged-out requests have no token.
    provider.phase.observe_identity()?;
    let auth_token = match provider.phase.tx()?.authentication_token() {
        AuthenticationToken::User(token) => Some(token),
        AuthenticationToken::Admin(..) | AuthenticationToken::None => None,
    };
    let context = &provider.context;
    let metadata = &context.request_metadata;
    // The top-level scheduled function and all of its descendants (e.g. a
    // mutation called by a scheduled action) report the scheduled function's
    // id, since `parent_scheduled_job` is propagated down the call tree. It
    // is `None` when the function was not scheduled.
    let scheduled_function_id = context
        .parent_scheduled_job
        .map(|(_, job_id)| job_id.encode());
    #[allow(non_snake_case)]
    #[derive(Serialize)]
    struct RequestMetadataJson<'a> {
        ip: Option<&'a str>,
        userAgent: Option<&'a str>,
        requestId: &'a str,
        scheduledFunctionId: Option<String>,
        authToken: Option<String>,
    }
    Ok(serde_json::value::to_raw_value(&RequestMetadataJson {
        ip: metadata.ip.as_ref().map(|ip| ip.as_str()),
        userAgent: metadata.user_agent.as_ref().map(|ua| ua.as_str()),
        requestId: context.request_id.as_str(),
        scheduledFunctionId: scheduled_function_id,
        authToken: auth_token,
    })?)
}

#[convex_macro::instrument_future]
async fn count<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CountArgs {
        table: String,
    }
    let table = with_argument_error("db.count", || {
        let args: CountArgs = serde_json::from_value(args)?;
        args.table.parse().context(ArgName("table"))
    })?;
    let component = provider.phase.component()?;
    let tx = provider.phase.tx()?;
    let result = tx.count(component.into(), &table).await?;
    let Some(result) = result else {
        return Err(table_summary_bootstrapping_error(Some(
            "Table count unavailable while bootstrapping",
        )));
    };

    // Trim to u32 and check for overflow.
    let result = u32::try_from(result)?;
    // Return as f64, which converts to number type in JavaScript.
    let result = f64::from(result);
    Ok(serde_json::value::to_raw_value(
        &ConvexValue::from(result).to_internal_json_serializable(),
    )?)
}

#[convex_macro::instrument_future]
async fn get_user_identity<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    _args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    provider.phase.observe_identity()?;
    // TODO: Somehow make the Transaction aware of the dependency on the user.
    let component = provider.phase.component()?;
    let tx = provider.phase.tx()?;
    let user_identity = tx.user_identity();
    if !component.is_root() {
        log_component_get_user_identity(user_identity.is_some());
        if provider
            .rt
            .rng()
            .random_bool(*COMPONENT_GET_USER_IDENTITY_LOG_SAMPLE_RATIO)
        {
            let component_path = tx.get_component_path_untracked(component);
            tracing::info!(
                component_path = ?component_path,
                has_user_identity = user_identity.is_some(),
                "component called getUserIdentity()"
            );
        }
    }
    if let Some(user_identity) = user_identity {
        return Ok(serde_json::value::to_raw_value(&JsonValue::try_from(
            user_identity,
        )?)?);
    }

    Ok(RawValue::NULL.to_owned())
}

#[convex_macro::instrument_future]
async fn storage_generate_upload_url<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    _args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    let post_url = provider.file_storage_generate_upload_url().await?;
    Ok(serde_json::value::to_raw_value(&post_url)?)
}

#[convex_macro::instrument_future]
async fn storage_get_url_batch<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    batch_args: Vec<JsonValue>,
) -> Vec<anyhow::Result<Box<RawValue>>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct GetUrlArgs {
        storage_id: String,
    }
    let batch_size = batch_args.len();
    let mut results = BTreeMap::new();
    let mut storage_ids = BTreeMap::new();
    for (idx, args) in batch_args.into_iter().enumerate() {
        let storage_id_result = with_argument_error("storage.getUrl", || {
            let GetUrlArgs { storage_id } = serde_json::from_value(args)?;
            storage_id.parse().context(ArgName("storageId"))
        });
        match storage_id_result {
            Ok(storage_id) => {
                storage_ids.insert(idx, storage_id);
            },
            Err(e) => {
                assert!(results.insert(idx, Err(e)).is_none());
            },
        }
    }
    let urls = provider.file_storage_get_url_batch(storage_ids).await;
    for (batch_key, url) in urls {
        assert!(results
            .insert(
                batch_key,
                url.and_then(|url| Ok(serde_json::value::to_raw_value(&url)?))
            )
            .is_none());
    }
    assert_eq!(results.len(), batch_size);
    results.into_values().collect()
}

#[convex_macro::instrument_future]
async fn storage_delete<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct StorageDeleteArgs {
        storage_id: String,
    }
    let storage_id: FileStorageId = with_argument_error("storage.delete", || {
        let StorageDeleteArgs { storage_id } = serde_json::from_value(args)?;
        storage_id.parse().context(ArgName("storageId"))
    })?;

    // Synchronously delete the file from storage
    provider.file_storage_delete(storage_id).await?;

    Ok(RawValue::NULL.to_owned())
}

#[convex_macro::instrument_future]
async fn storage_store<RT: Runtime>(
    _provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct StorageStoreArgs {
        /// Base64-encoded file contents.
        blob: String,
        content_type: Option<String>,
        /// Base64-encoded sha256 of the contents, to check them against.
        sha256: Option<String>,
    }
    let (_blob, _content_type, _expected_sha256) = with_argument_error("storage.store", || {
        let StorageStoreArgs {
            blob,
            content_type,
            sha256,
        } = serde_json::from_value(args)?;
        let blob = base64::decode(&blob).context(ArgName("blob"))?;
        let content_type = content_type
            .filter(|ct| !ct.is_empty())
            .map(|ct| mime::Mime::from_str(&ct).map(ContentType::from))
            .transpose()
            .context(ArgName("contentType"))?;
        let expected_sha256 = sha256
            .as_deref()
            .map(Sha256Digest::from_base64)
            .transpose()
            .context(ArgName("sha256"))?;
        Ok((blob, content_type, expected_sha256))
    })?;

    anyhow::bail!(ErrorMetadata::bad_request(
        "StorageStoreNotImplemented",
        "ctx.storage.store() is not supported in queries and mutations yet. Please use an action, \
         or ctx.storage.generateUploadUrl() to upload from a client.",
    ))
}

#[convex_macro::instrument_future]
async fn storage_get_metadata<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct StorageGetMetadataArgs {
        storage_id: String,
    }
    let storage_id: FileStorageId = with_argument_error("storage.getMetadata", || {
        let StorageGetMetadataArgs { storage_id } = serde_json::from_value(args)?;
        storage_id.parse().context(ArgName("storageId"))
    })?;

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct FileMetadataJson {
        storage_id: String,
        sha256: String,
        size: i64,
        content_type: Option<String>,
    }
    let file_metadata = provider.file_storage_get_entry(storage_id).await?.map(
        |FileStorageEntry {
             storage_id,
             storage_key: _, // internal field that we shouldn't return in syscalls
             sha256,
             size,
             content_type,
         }| {
            FileMetadataJson {
                storage_id: storage_id.to_string(),
                // TODO(CX-5533) use base64 for consistency.
                sha256: sha256.as_hex(),
                size,
                content_type,
            }
        },
    );
    Ok(serde_json::value::to_raw_value(&file_metadata)?)
}

#[convex_macro::instrument_future]
async fn schedule<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ScheduleArgs {
        name: Option<String>,
        reference: Option<String>,
        function_handle: Option<String>,
        ts: f64,
        args: UdfArgsJson,
    }

    let ScheduleArgs {
        name,
        reference,
        function_handle,
        ts,
        args,
    }: ScheduleArgs = with_argument_error("scheduler", || Ok(serde_json::from_value(args)?))?;

    let scheduled_id = schedule_resolved(
        provider,
        ScheduleCallArgs {
            name,
            reference,
            function_handle,
            timestamp_seconds: ts,
            args: ScheduleArgumentInput::Legacy(args),
        },
    )
    .await?;
    Ok(serde_json::value::to_raw_value(&scheduled_id)?)
}

async fn schedule_typed<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: TypedScheduleArgs,
) -> anyhow::Result<PendingValue> {
    let scheduled_id = schedule_resolved(
        provider,
        ScheduleCallArgs {
            name: args.name,
            reference: args.reference,
            function_handle: args.function_handle,
            timestamp_seconds: args.timestamp_seconds,
            args: ScheduleArgumentInput::Typed(args.args),
        },
    )
    .await?;
    Ok(PendingValue::from(ConvexValue::try_from(scheduled_id)?))
}

async fn schedule_resolved<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    ScheduleCallArgs {
        name,
        reference,
        function_handle,
        timestamp_seconds,
        args,
    }: ScheduleCallArgs,
) -> anyhow::Result<String> {
    let path = match function_handle {
        Some(h) => {
            let handle: FunctionHandle = with_argument_error("scheduler", || h.parse())?;
            provider.lookup_function_handle(handle).await?
        },
        None => {
            let reference = parse_name_or_reference("scheduler", name, reference)?;
            match provider.resolve(reference).await? {
                Resource::Value(v) => {
                    anyhow::bail!(ErrorMetadata::bad_request(
                        "InvalidResource",
                        format!(
                            "Only functions can be scheduled. {} is not a function",
                            v.to_internal_json()
                        ),
                    ));
                },
                Resource::Function(p) => p,
                Resource::ResolvedSystemUdf { .. } => {
                    anyhow::bail!("Cannot schedule function by component id");
                },
            }
        },
    };

    let scheduling_component = provider.phase.component()?;

    let scheduled_ts =
        with_argument_error("ts", || UnixTimestamp::from_secs_f64(timestamp_seconds))?;
    let (path, udf_args) = provider
        .validate_schedule_args(path, args, scheduled_ts)
        .await?;

    let context = provider.context.clone();
    let tx = provider.phase.tx()?;
    let virtual_id = VirtualSchedulerModel::new(tx, scheduling_component.into())
        .schedule(path, udf_args, scheduled_ts, context)
        .await?;

    Ok(String::from(virtual_id))
}

#[convex_macro::instrument_future]
async fn cancel_job<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CancelJobArgs {
        id: String,
    }
    let component = provider.phase.component()?;

    let virtual_id_v6 = with_argument_error("db.cancel_job", || {
        let args: CancelJobArgs = serde_json::from_value(args)?;
        let id = DeveloperDocumentId::decode(&args.id).context(ArgName("id"))?;
        Ok(id)
    })?;

    // A scheduled mutation observes its own record as `inProgress` via a pending
    // write, so canceling itself would conflict with that write. Clients on
    // MIN_NPM_VERSION_MUTATION_SELF_CANCEL or newer get an error; older clients
    // keep the historical no-op behavior but are warned that it will change.
    if let Some((_, self_job_id)) = provider.context.parent_scheduled_job
        && self_job_id == virtual_id_v6
    {
        if provider
            .udf_server_version
            .as_ref()
            .is_some_and(|version| *version >= *MIN_NPM_VERSION_MUTATION_SELF_CANCEL)
        {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ScheduledFunctionCancelingItself",
                "A mutation cannot cancel itself",
            ));
        }
        provider.emit_warning(
            "A scheduled mutation canceling itself is deprecated and will throw an error in a \
             future version of Convex."
                .to_string(),
        )?;
        return Ok(RawValue::NULL.to_owned());
    }

    let tx = provider.phase.tx()?;
    VirtualSchedulerModel::new(tx, component.into())
        .cancel(virtual_id_v6)
        .await?;

    Ok(RawValue::NULL.to_owned())
}

async fn audit_log<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AuditLogArgs {
        body: JsonValue,
    }
    let args: AuditLogArgs = with_argument_error("auditLog", || Ok(serde_json::from_value(args)?))?;
    provider.emit_audit_log_line(AuditLogLine { body: args.body })?;
    Ok(RawValue::NULL.to_owned())
}

async fn write_deployment_audit_log<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    if !provider.is_system() {
        anyhow::bail!(ErrorMetadata::bad_request(
            "Unauthorized",
            "writeDeploymentAuditLog is only available in system UDFs"
        ));
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AuditLogArgs {
        action: String,
        metadata: serde_json::Map<String, JsonValue>,
    }
    let args: AuditLogArgs = serde_json::from_value(args)?;

    let component_id = provider.phase.component()?;
    let request_metadata = provider.context.request_metadata.clone();
    let tx = provider.phase.tx()?;
    let component_path = tx.must_component_path(component_id)?;

    // Inject component_id and component into metadata
    let mut metadata = args.metadata;
    metadata.insert(
        "component_id".to_string(),
        component_id
            .serialize_to_string()
            .map_or(JsonValue::Null, JsonValue::String),
    );
    metadata.insert(
        "component".to_string(),
        component_path
            .serialize()
            .map_or(JsonValue::Null, JsonValue::String),
    );

    let metadata_value: JsonValue = JsonValue::Object(metadata);
    let metadata: ConvexObject = metadata_value.try_into()?;

    let event_obj = obj!("action" => args.action, "metadata" => metadata)?;
    DeploymentAuditLogModel::new(tx)
        .insert(
            vec![DeploymentAuditLogEvent::try_from(event_obj)?],
            &request_metadata,
        )
        .await?;

    Ok(RawValue::NULL.to_owned())
}

#[fastrace::trace]
#[convex_macro::instrument_future]
async fn insert<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<PendingValue> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct InsertArgs {
        table: String,
        value: JsonValue,
    }
    let (table, value) = with_argument_error("db.insert", || {
        let args: InsertArgs = serde_json::from_value(args)?;
        let value = PendingValue::from_uncommitted_json(args.value).context(ArgName("value"))?;
        Ok((args.table, value))
    })?;
    insert_typed(provider, table, value).await
}

async fn insert_typed<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    table: String,
    value: PendingValue,
) -> anyhow::Result<PendingValue> {
    let table = with_argument_error("db.insert", || {
        if !value.is_object() {
            return Err(anyhow::anyhow!("Value must be an Object").context(ArgName("value")));
        }
        table.parse::<TableName>().context(ArgName("table"))
    })?;
    system_table_guard(&table, false)?;
    let component = provider.phase.component()?;
    let tx = provider.phase.tx()?;
    let document_id = UserFacingModel::new(tx, component.into())
        .insert(table, value)
        .await?;
    Ok(obj!("_id" => document_id.encode())?.into())
}

#[fastrace::trace]
#[convex_macro::instrument_future]
async fn shallow_merge<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<PendingDocument> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct UpdateArgs {
        #[serde(default)]
        table: Option<String>,
        id: String,
        value: JsonValue,
    }
    let args: UpdateArgs = with_argument_error("db.patch", || Ok(serde_json::from_value(args)?))?;
    let value = with_argument_error("db.patch", || {
        PatchValue::from_uncommitted_json(args.value).context(ArgName("value"))
    })?;
    shallow_merge_typed(provider, args.table, &args.id, value).await
}

async fn shallow_merge_typed<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    table: Option<String>,
    id: &str,
    value: PatchValue,
) -> anyhow::Result<PendingDocument> {
    let table_filter = provider.table_filter();
    let component = provider.phase.component()?;
    let tx = provider.phase.tx()?;
    let (id, table_name) = with_argument_error("db.patch", || {
        let id = DeveloperDocumentId::decode(id).context(ArgName("id"))?;
        let actual_table_name = tx
            .resolve_idv6(id, component.into(), table_filter)
            .context(ArgName("id"))?;
        check_table_name(&table, &actual_table_name)?;
        Ok((id, actual_table_name))
    })?;

    system_table_guard(&table_name, false)?;

    let document = UserFacingModel::new(tx, component.into())
        .patch(id, value)
        .await?;
    Ok(document)
}

#[fastrace::trace]
#[convex_macro::instrument_future]
async fn replace<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<PendingDocument> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ReplaceArgs {
        #[serde(default)]
        table: Option<String>,
        id: String,
        value: JsonValue,
    }
    let args: ReplaceArgs =
        with_argument_error("db.replace", || Ok(serde_json::from_value(args)?))?;
    let value = with_argument_error("db.replace", || {
        PendingValue::from_uncommitted_json(args.value).context(ArgName("value"))
    })?;
    replace_typed(provider, args.table, &args.id, value).await
}

async fn replace_typed<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    table: Option<String>,
    id: &str,
    value: PendingValue,
) -> anyhow::Result<PendingDocument> {
    let table_filter = provider.table_filter();
    let component = provider.phase.component()?;
    let tx = provider.phase.tx()?;
    let (id, table_name) = with_argument_error("db.replace", || {
        let id = DeveloperDocumentId::decode(id).context(ArgName("id"))?;
        let actual_table_name = tx
            .resolve_idv6(id, component.into(), table_filter)
            .context(ArgName("id"))?;
        check_table_name(&table, &actual_table_name)?;

        if !value.is_object() {
            return Err(anyhow::anyhow!("Value must be an Object").context(ArgName("value")));
        }
        Ok((id, actual_table_name))
    })?;

    system_table_guard(&table_name, false)?;

    let document = UserFacingModel::new(tx, component.into())
        .replace(id, value)
        .await?;
    Ok(document)
}

#[fastrace::trace]
#[convex_macro::instrument_future]
async fn query_batch<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    batch_args: Vec<AsyncRead>,
) -> Vec<anyhow::Result<AsyncSyscallValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct QueryStreamNextArgs {
        query_id: u32,
    }

    let table_filter = provider.table_filter();
    let mut queries_to_fetch = BTreeMap::new();
    let mut results = BTreeMap::new();
    let batch_size = batch_args.len();
    for (idx, args) in batch_args.into_iter().enumerate() {
        let result: anyhow::Result<_> = try_anyhow!({
            match args {
                AsyncRead::QueryStreamNext(args) => {
                    let query_id = with_argument_error("queryStreamNext", || {
                        let args: QueryStreamNextArgs = serde_json::from_value(args)?;
                        Ok(args.query_id)
                    })?;
                    let managed_query = provider
                        .query_manager
                        .take_developer(query_id)
                        .map(ManagedQuery::Active)
                        .context(ErrorMetadata::bad_request(
                            "QueryNotFound",
                            "in-progress query not found",
                        ))?;
                    let local_query = match managed_query {
                        ManagedQuery::Pending { query, version } => {
                            let component = provider.phase.component()?;
                            DeveloperQuery::new_with_version(
                                provider.phase.tx()?,
                                component.into(),
                                query,
                                version,
                                table_filter,
                            )?
                        },
                        ManagedQuery::Active(local_query) => local_query,
                    };
                    Some((Some(query_id), local_query))
                },
                AsyncRead::Get(arguments) => {
                    let component = provider.phase.component()?;
                    let tx = provider.phase.tx()?;

                    let args = match arguments {
                        DatabaseGetArguments::LegacyJson(value) => {
                            with_argument_error("db.get", || {
                                Ok(serde_json::from_value::<DatabaseGetArgs>(value)?)
                            })?
                        },
                        DatabaseGetArguments::Typed(args) => args,
                    };
                    let method_name = if args.is_system {
                        "db.system.get"
                    } else {
                        "db.get"
                    };

                    let (id, is_system, version) = with_argument_error(method_name, || {
                        let id = DeveloperDocumentId::decode(&args.id).context(ArgName("id"))?;
                        let version = parse_version(args.version)?;
                        Ok((id, args.is_system, version))
                    })?;
                    let name: Result<TableName, anyhow::Error> =
                        tx.all_tables_number_to_name(component.into(), table_filter)(id.table());
                    if name.is_ok() {
                        system_table_guard(&name?, is_system)?;
                    }
                    match tx.resolve_idv6(id, component.into(), table_filter) {
                        Ok(table_name) => {
                            with_argument_error(method_name, || {
                                check_table_name(&args.table, &table_name)
                            })?;

                            let query = Query::get(table_name, id);
                            Some((
                                None,
                                DeveloperQuery::new_with_version(
                                    tx,
                                    component.into(),
                                    query,
                                    version,
                                    table_filter,
                                )?,
                            ))
                        },
                        Err(_) => {
                            // Get on a non-existent table should return
                            // null.
                            None
                        },
                    }
                },
            }
        });
        match result {
            Err(e) => {
                assert!(results.insert(idx, Err(e)).is_none());
            },
            Ok(Some((query_id, query_to_fetch))) => {
                assert!(queries_to_fetch
                    .insert(idx, (query_id, query_to_fetch))
                    .is_none());
            },
            Ok(None) => {
                assert!(results
                    .insert(idx, Ok(AsyncSyscallValue::Json("null".to_owned())))
                    .is_none());
            },
        }
    }

    let tx = match provider.phase.tx() {
        Ok(tx) => tx,
        Err(e) => {
            return (0..batch_size).map(|_| Err(e.clone_error())).collect_vec();
        },
    };

    let mut fetch_results = query_batch_next_document(
        queries_to_fetch
            .iter_mut()
            .map(|(idx, (_, local_query))| (*idx, (local_query, None)))
            .collect(),
        tx,
    )
    .await;

    for (batch_key, (query_id, local_query)) in queries_to_fetch {
        let result: anyhow::Result<_> = try_anyhow!({
            if let Some(query_id) = query_id {
                provider
                    .query_manager
                    .insert_developer(query_id, local_query);
            }
            let maybe_next = fetch_results
                .remove(&batch_key)
                .context("batch_key missing")??;

            let done = maybe_next.is_none();
            let component = provider.phase.component()?;
            let tx = provider.phase.tx()?;
            let value = maybe_next
                .map(|(doc, ts)| query_document_to_result(tx, component.into(), doc, ts))
                .transpose()?;

            if let Some(query_id) = query_id {
                if done {
                    provider.query_manager.cleanup_developer(query_id);
                }
                AsyncSyscallValue::QueryStreamNext(value)
            } else {
                match value {
                    Some(value) => AsyncSyscallValue::Document(value),
                    None => AsyncSyscallValue::Json("null".to_owned()),
                }
            }
        });
        results.insert(batch_key, result);
    }
    assert_eq!(results.len(), batch_size);
    results.into_values().collect()
}

#[fastrace::trace]
#[convex_macro::instrument_future]
async fn remove<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RemoveArgs {
        #[serde(default)]
        table: Option<String>,
        id: String,
    }

    let args: RemoveArgs = with_argument_error("db.delete", || Ok(serde_json::from_value(args)?))?;
    let document = remove_typed(provider, args.table, &args.id).await?;
    Ok(serde_json::value::to_raw_value(
        &document.to_internal_json_serializable(),
    )?)
}

async fn remove_typed<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    table: Option<String>,
    id: &str,
) -> anyhow::Result<DeveloperDocument> {
    let table_filter = provider.table_filter();
    let component = provider.phase.component()?;
    let tx = provider.phase.tx()?;
    let (id, table_name) = with_argument_error("db.delete", || {
        let id = DeveloperDocumentId::decode(id).context(ArgName("id"))?;
        let actual_table_name = tx
            .resolve_idv6(id, component.into(), table_filter)
            .context(ArgName("id"))?;
        check_table_name(&table, &actual_table_name)?;
        Ok((id, actual_table_name))
    })?;

    system_table_guard(&table_name, false)?;

    UserFacingModel::new(tx, component.into()).delete(id).await
}

#[convex_macro::instrument_future]
async fn run_udf<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
    udf_callback: impl UdfCallback<RT>,
) -> AsyncSyscallResult {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RunUdfArgs {
        udf_type: String,
        name: Option<String>,
        reference: Option<String>,
        function_handle: Option<String>,
        args: JsonValue,
        transaction_limits: Option<TransactionLimits>,
    }
    let caller_udf_type = provider.udf_type;
    let typed_args = with_argument_error("runUdf", || {
        let RunUdfArgs {
            udf_type,
            name,
            reference,
            function_handle,
            args,
            transaction_limits,
        } = serde_json::from_value(args)?;
        let udf_type = udf_type.parse().context(ArgName("udfType"))?;
        // The legacy query path must reject the unresolved commit token before
        // it can become a pending value; mutations may pass it to nested UDFs.
        let args = match caller_udf_type {
            UdfType::Mutation => {
                PendingValue::from_uncommitted_json(args).context(ArgName("args"))?
            },
            UdfType::Query | UdfType::Action | UdfType::HttpAction => {
                PendingValue::from(ConvexValue::try_from(args).context(ArgName("args"))?)
            },
        };
        Ok(TypedRunUdfArgs {
            udf_type,
            name,
            reference,
            function_handle,
            args,
            transaction_limits,
        })
    });
    match typed_args {
        Ok(args) => run_udf_typed(provider, args, udf_callback).await,
        Err(error) => nested_udf_async_syscall_result(Err(error)),
    }
}

async fn run_udf_typed<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: TypedRunUdfArgs,
    udf_callback: impl UdfCallback<RT>,
) -> AsyncSyscallResult {
    let result = async {
        let TypedRunUdfArgs {
            udf_type,
            name,
            reference,
            function_handle,
            args,
            transaction_limits,
        } = args;
        let args = with_argument_error("runUdf", || match provider.udf_type {
            UdfType::Mutation if args.is_object() => Ok(args),
            UdfType::Mutation => {
                Err(anyhow::anyhow!("Value must be an Object").context(ArgName("args")))
            },
            UdfType::Query | UdfType::Action | UdfType::HttpAction => {
                let args: ConvexObject = args
                    .try_into_concrete()
                    .context(ArgName("args"))?
                    .try_into()
                    .context(ArgName("args"))?;
                Ok(args.into())
            },
        })?;
        let path = match function_handle {
            Some(function_handle) => {
                let handle: FunctionHandle =
                    with_argument_error("runUdf", || function_handle.parse())?;
                let path = provider.lookup_function_handle(handle).await?;
                let tx = provider.phase.tx()?;
                let (_, component) = BootstrapComponentsModel::new(tx)
                    .must_component_path_to_ids(&path.component)?;
                ResolvedComponentFunctionPath {
                    component,
                    udf_path: path.udf_path,
                    component_path: path.component,
                }
            },
            None => {
                let reference = parse_name_or_reference("runUdf", name, reference)?;
                let resource = provider.resolve(reference).await?;
                match resource {
                    Resource::ResolvedSystemUdf(path) => path,
                    Resource::Value(_) => {
                        anyhow::bail!(ErrorMetadata::bad_request(
                            "InvalidResource",
                            "Cannot execute a value resource"
                        ));
                    },
                    Resource::Function(path) => {
                        let tx = provider.phase.tx()?;
                        let (_, component) = BootstrapComponentsModel::new(tx)
                            .must_component_path_to_ids(&path.component)?;
                        ResolvedComponentFunctionPath {
                            component,
                            udf_path: path.udf_path,
                            component_path: path.component,
                        }
                    },
                }
            },
        };
        provider
            .run_udf(udf_type, path, args, transaction_limits, udf_callback)
            .await
    }
    .await;
    nested_udf_async_syscall_result(result)
}

async fn create_function_handle<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<Box<RawValue>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CreateFunctionHandleArgs {
        name: Option<String>,
        function_handle: Option<String>,
        reference: Option<String>,
    }
    let CreateFunctionHandleArgs {
        name,
        function_handle,
        reference,
    } = with_argument_error("createFunctionHandle", || Ok(serde_json::from_value(args)?))?;
    let function_path = match function_handle {
        Some(function_handle) => {
            return Ok(serde_json::value::to_raw_value(&function_handle)?);
        },
        None => {
            let reference = parse_name_or_reference("createFunctionHandle", name, reference)?;
            match provider.resolve(reference).await? {
                Resource::Function(path) => path,
                Resource::ResolvedSystemUdf { .. } => {
                    anyhow::bail!("Cannot create function handle for system UDF");
                },
                Resource::Value(_) => {
                    anyhow::bail!(ErrorMetadata::bad_request(
                        "InvalidResource",
                        "Cannot create a function handle for a value resource"
                    ));
                },
            }
        },
    };
    let handle = provider.create_function_handle(function_path).await?;
    Ok(serde_json::value::to_raw_value(&String::from(handle))?)
}

/// As pages of results are commonly returned directly from UDFs, a page should
/// be convertible to Value::Array, which has a size limit of MAX_ARRAY_LEN.
/// If there are more results than 75% of that, we recommend splitting the page.
const SOFT_MAX_PAGE_LEN: usize = soft_data_limit(8192);

#[derive(Debug, Copy, Clone)]
enum QueryPageStatus {
    SplitRequired,
    SplitRecommended,
}

impl QueryPageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SplitRecommended => "SplitRecommended",
            Self::SplitRequired => "SplitRequired",
        }
    }
}

struct QueryPageMetadata {
    cursor: Cursor,
    split_cursor: Option<Cursor>,
    page_status: Option<QueryPageStatus>,
}

/// Return the original pending body when a query sees this transaction's own
/// unresolved write. The concrete query view contains substituted timestamps.
fn pending_document_value<'a, RT: Runtime>(
    tx: &'a mut Transaction<RT>,
    namespace: TableNamespace,
    id: DeveloperDocumentId,
    ts: WriteTimestamp,
) -> anyhow::Result<Option<&'a PendingValue>> {
    if ts != WriteTimestamp::Pending {
        return Ok(None);
    }
    // Virtual-table reads can also be pending, but their physical table is not
    // in this namespace and its placeholder value is not a document to forward.
    if !tx
        .table_mapping()
        .namespace(namespace)
        .table_number_exists()(id.table())
    {
        return Ok(None);
    }
    let id = tx.resolve_developer_id(&id, namespace)?;
    Ok(
        match tx
            .pending_write(&id)
            .and_then(|update| update.new_document.as_ref())
        {
            Some(PendingDocument::Pending { body, .. }) => Some(body),
            Some(PendingDocument::Concrete(_)) | None => None,
        },
    )
}

fn query_document_to_result<RT: Runtime>(
    tx: &mut Transaction<RT>,
    namespace: TableNamespace,
    document: QueryDocument,
    ts: WriteTimestamp,
) -> anyhow::Result<QueryDocumentResult> {
    let id = match &document {
        QueryDocument::Packed(document) => document.developer_id(),
        QueryDocument::Materialized(document) => document.id(),
    };
    if let Some(pending) = pending_document_value(tx, namespace, id, ts)? {
        return Ok(QueryDocumentResult::Typed(pending.clone()));
    }
    Ok(match document {
        QueryDocument::Packed(document) => QueryDocumentResult::Packed(document),
        QueryDocument::Materialized(document) => {
            QueryDocumentResult::Typed(ConvexValue::Object(document.into_value().0).into())
        },
    })
}

async fn read_page_from_query<RT: Runtime>(
    mut query: DeveloperQuery<RT>,
    tx: &mut Transaction<RT>,
    page_size: usize,
) -> anyhow::Result<(Vec<(QueryDocument, WriteTimestamp)>, QueryPageMetadata)> {
    let end_cursor = query.end_cursor();
    let has_end_cursor = end_cursor.is_some();
    let mut page = Vec::with_capacity(page_size);
    let mut page_status = None;
    // If we don't have an end cursor, collect results until we hit our page size.
    // If we do have an end cursor, ignore the page size and collect everything
    while has_end_cursor || (page.len() < page_size) {
        // If we don't have an end cursor, we really have no idea
        // how many results we need to prefetch, but we can
        // use the original page size as a hint.
        let prefetch_hint = if has_end_cursor {
            Some(page_size)
        } else {
            Some(page_size - page.len())
        };

        let next_value = match query.next_document_with_ts(tx, prefetch_hint).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                break;
            },
            Err(e) => {
                if e.is_pagination_limit() {
                    // An initial page can advance through its continuation cursor,
                    // even when large rows leave no usable split cursor. A bounded
                    // page must split to preserve every row in its existing range.
                    page_status = Some(if has_end_cursor {
                        QueryPageStatus::SplitRequired
                    } else {
                        QueryPageStatus::SplitRecommended
                    });
                    if query.cursor().is_none() {
                        // Intentionally drop ErrorMetadata because this should
                        // be impossible, so we want to throw a system error instead.
                        anyhow::bail!(
                            "This should be impossible. Hit pagination limit before setting query \
                             cursor: {e:?}"
                        );
                    }
                    break;
                }
                anyhow::bail!(e);
            },
        };
        page.push(next_value)
    }
    if page_status.is_none()
        && (query.is_approaching_data_limit() || page.len() > SOFT_MAX_PAGE_LEN)
    {
        page_status = Some(QueryPageStatus::SplitRecommended);
    }
    let cursor = end_cursor.or_else(|| query.cursor()).context(
        "Cursor was None. This should be impossible if `.next` was called on the query.",
    )?;
    Ok((
        page,
        QueryPageMetadata {
            cursor,
            split_cursor: query.split_cursor(),
            page_status,
        },
    ))
}

#[fastrace::trace]
#[convex_macro::instrument_future]
async fn query_page<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: JsonValue,
) -> anyhow::Result<QueryPageResult> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct QueryPageArgs {
        query: JsonValue,
        cursor: Option<String>,
        end_cursor: Option<String>,
        page_size: usize,
        maximum_rows_read: Option<usize>,
        maximum_bytes_read: Option<usize>,
        #[serde(default)]
        version: Option<String>,
    }
    let args: QueryPageArgs =
        with_argument_error("queryPage", || Ok(serde_json::from_value(args)?))?;
    let query = with_argument_error("queryPage", || {
        Query::try_from(args.query).context(ArgName("query"))
    })?;
    let version = parse_version(args.version)?;
    query_page_typed(
        provider,
        TypedQueryPageArgs {
            query,
            cursor: args.cursor,
            end_cursor: args.end_cursor,
            page_size: args.page_size,
            maximum_rows_read: args.maximum_rows_read,
            maximum_bytes_read: args.maximum_bytes_read,
            version,
        },
    )
    .await
}

#[fastrace::trace]
#[convex_macro::instrument_future]
async fn query_page_typed<RT: Runtime>(
    provider: &mut DatabaseUdfSyscallProvider<RT>,
    args: TypedQueryPageArgs,
) -> anyhow::Result<QueryPageResult> {
    let table_filter = provider.table_filter();

    let page_size = args.page_size;
    if page_size == 0 {
        anyhow::bail!(ErrorMetadata::bad_request(
            "NoDocumentsForPagination",
            "Must request at least 1 document while paginating"
        ));
    }
    if page_size > *TRANSACTION_MAX_READ_SIZE_ROWS {
        anyhow::bail!(ErrorMetadata::bad_request(
            "PageSizeTooLarge",
            format!("Requested too many items: {page_size}")
        ));
    }
    if args.maximum_rows_read == Some(0) || args.maximum_bytes_read == Some(0) {
        anyhow::bail!(ErrorMetadata::bad_request(
            "InvalidPaginationLimit",
            "maximumRowsRead and maximumBytesRead must be greater than 0"
        ));
    }

    let start_cursor = args
        .cursor
        .map(|c| provider.key_broker.decrypt_cursor(c))
        .transpose()?;

    let end_cursor = match args.end_cursor {
        Some(end_cursor) => Some(provider.key_broker.decrypt_cursor(end_cursor)?),
        None => provider.prev_journal.end_cursor.clone(),
    };

    let component = provider.phase.component()?;
    if !component.is_root() && !provider.is_system() {
        anyhow::bail!(ErrorMetadata::bad_request(
                "PaginationUnsupportedInComponents",
                "paginate() is only supported in the app. Learn more at https://docs.convex.dev/components/authoring#pagination",
            ));
    }

    let tx = provider.phase.tx()?;

    let (
        page,
        QueryPageMetadata {
            cursor,
            split_cursor,
            page_status,
        },
    ) = {
        let query = DeveloperQuery::new_bounded(
            tx,
            component.into(),
            args.query,
            PaginationOptions::ReactivePagination {
                start_cursor,
                end_cursor,
                maximum_rows_read: args.maximum_rows_read,
                maximum_bytes_read: args.maximum_bytes_read,
            },
            args.version,
            table_filter,
        )?;
        let (page, metadata) = read_page_from_query(query, tx, page_size).await?;
        let page = page
            .into_iter()
            .map(|(doc, ts)| query_document_to_result(tx, component.into(), doc, ts))
            .collect::<anyhow::Result<_>>()?;
        (page, metadata)
    };

    let page_status = page_status.map(|s| s.as_str());

    // Place split_cursor in the middle.
    let split_cursor = split_cursor.map(|split| provider.key_broker.encrypt_cursor(&split));

    let continue_cursor = provider.key_broker.encrypt_cursor(&cursor);

    let is_done = matches!(
        cursor,
        Cursor {
            position: CursorPosition::End,
            ..
        }
    );

    anyhow::ensure!(
        provider.next_journal.end_cursor.is_none(),
        ErrorMetadata::bad_request(
            "MultiplePaginatedDatabaseQueries",
            "This query or mutation function ran multiple paginated queries. Convex only supports \
             a single paginated query in each function.",
        )
    );
    provider.next_journal.end_cursor = Some(cursor);

    Ok(QueryPageResult {
        page,
        is_done,
        continue_cursor,
        split_cursor,
        page_status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonexistent_document_error(message: &str) -> anyhow::Result<JsonValue> {
        Err(ErrorMetadata::bad_request("NonexistentDocument", message.to_owned()).into())
    }

    #[test]
    fn missing_document_host_operation_error_uses_typed_metadata() {
        let document_id = DeveloperDocumentId::MIN;
        let encoded_id = document_id.encode();

        for (operation, expected) in [
            (HostOperation::Patch, HostOperation::Patch),
            (HostOperation::Replace, HostOperation::Replace),
            (HostOperation::Delete, HostOperation::Delete),
        ] {
            assert_eq!(
                missing_document_host_operation_error(
                    Some(&encoded_id),
                    operation,
                    &nonexistent_document_error("arbitrary human copy"),
                ),
                Some(HostOperationErrorV1::NonexistentDocument {
                    operation: expected,
                    document_id,
                }),
            );
        }

        let unrelated_error: anyhow::Result<JsonValue> = Err(ErrorMetadata::bad_request(
            "OtherBadRequest",
            "nonexistent document in free-form copy",
        )
        .into());
        assert_eq!(
            missing_document_host_operation_error(
                Some(&encoded_id),
                HostOperation::Patch,
                &unrelated_error,
            ),
            None,
        );
    }

    #[test]
    fn nested_udf_terminal_error_reaches_only_the_parent_run_udf_rejection() {
        let host_operation_error = HostOperationErrorV1::NonexistentDocument {
            operation: HostOperation::Patch,
            document_id: DeveloperDocumentId::MIN,
        };

        let child_terminal_error = nested_udf_async_syscall_result(Ok(NestedUdfExecutionResult {
            result: Err(anyhow::anyhow!("child terminal developer error")),
            host_operation_error: Some(host_operation_error),
        }));
        assert!(child_terminal_error.result.is_err());
        assert_eq!(
            child_terminal_error.host_operation_error,
            Some(host_operation_error)
        );

        let successful_child = nested_udf_async_syscall_result(Ok(NestedUdfExecutionResult {
            result: Ok(PendingValue::from_uncommitted_json(json!(true)).unwrap()),
            host_operation_error: None,
        }));
        assert!(successful_child.result.is_ok());
        assert_eq!(successful_child.host_operation_error, None);

        let outer_provider_error =
            nested_udf_async_syscall_result(Err(anyhow::anyhow!("outer provider error")));
        assert!(outer_provider_error.result.is_err());
        assert_eq!(outer_provider_error.host_operation_error, None);
    }

    #[test]
    fn logical_host_operations_preserve_each_batched_read_in_order() -> anyhow::Result<()> {
        let mut batch = AsyncSyscallBatch::new("1.0/get".to_owned(), json!({}));
        batch.push("1.0/queryStreamNext".to_owned(), json!({}))?;

        assert_eq!(
            batch.logical_host_operations(),
            vec![
                LogicalHostOperation::DatabaseGet,
                LogicalHostOperation::DatabaseQueryStreamNext,
            ]
        );
        Ok(())
    }
}
