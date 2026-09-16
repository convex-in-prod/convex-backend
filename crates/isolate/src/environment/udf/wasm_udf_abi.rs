use std::{
    fmt,
    mem::size_of,
    sync::Arc,
};

use serde_json::{
    Map as JsonMap,
    Number as JsonNumber,
    Value as JsonValue,
};
use thiserror::Error;
use value::{
    heap_size::HeapSize,
    ConvexValue,
    FieldName,
    PendingValue,
};

// Static Hermes converts native `long long` results to JavaScript numbers.
// Keep every valid handle below 2^53 so that round trips through guest object
// properties remain exact while retaining a full u32 slot generation.
const HANDLE_INDEX_BITS: u32 = 17;
const HANDLE_GENERATION_BITS: u32 = 32;
const HANDLE_INDEX_MASK: u64 = (1_u64 << HANDLE_INDEX_BITS) - 1;
const HANDLE_GENERATION_MASK: u64 = (1_u64 << HANDLE_GENERATION_BITS) - 1;
const HANDLE_GENERATION_SHIFT: u32 = HANDLE_INDEX_BITS;
const HANDLE_KIND_SHIFT: u32 = HANDLE_INDEX_BITS + HANDLE_GENERATION_BITS;
const MAX_ENCODED_SLOTS: usize = HANDLE_INDEX_MASK as usize;
pub(crate) const MAX_EXACT_JAVASCRIPT_INTEGER: u64 = (1_u64 << 53) - 1;
const MAX_FIELD_NAME_BYTES: usize = 4 * 1024;

pub(crate) const INVOCATION_UNIX_TIMESTAMP_MS_IMPORT: &str = "convex_invocation_unix_timestamp_ms";

#[derive(Debug, Error)]
pub(crate) enum GuestNativeValueError {
    #[error("guest-native Convex JSON payload exceeds the {maximum_bytes}-byte limit")]
    TooLarge { maximum_bytes: usize },
    #[error("guest-native Convex JSON payload is malformed")]
    Malformed,
    #[error("guest-native Convex JSON payload is not a valid Convex value")]
    InvalidConvexValue,
    #[error("guest-native Convex JSON payload is not canonical")]
    NonCanonical,
}

pub(crate) struct GuestNativeValueCodec;

impl GuestNativeValueCodec {
    pub(crate) fn encode(
        value: JsonValue,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, GuestNativeValueError> {
        Self::encode_with_format(value, maximum_bytes, GuestNativeValueFormat::Committed)
    }

    pub(crate) fn encode_pending(
        value: JsonValue,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, GuestNativeValueError> {
        Self::encode_with_format(value, maximum_bytes, GuestNativeValueFormat::Pending)
    }

    fn encode_with_format(
        value: JsonValue,
        maximum_bytes: usize,
        format: GuestNativeValueFormat,
    ) -> Result<Vec<u8>, GuestNativeValueError> {
        let value = canonical_guest_native_value(value, format)?;
        let mut bytes = Vec::new();
        write_guest_native_json(&value, &mut bytes)?;
        if bytes.len() > maximum_bytes {
            return Err(GuestNativeValueError::TooLarge { maximum_bytes });
        }
        Ok(bytes)
    }

    pub(crate) fn decode(
        bytes: &[u8],
        maximum_bytes: usize,
    ) -> Result<JsonValue, GuestNativeValueError> {
        Self::decode_with_format(bytes, maximum_bytes, GuestNativeValueFormat::Committed)
    }

    pub(crate) fn decode_pending(
        bytes: &[u8],
        maximum_bytes: usize,
    ) -> Result<JsonValue, GuestNativeValueError> {
        Self::decode_with_format(bytes, maximum_bytes, GuestNativeValueFormat::Pending)
    }

    fn decode_with_format(
        bytes: &[u8],
        maximum_bytes: usize,
        format: GuestNativeValueFormat,
    ) -> Result<JsonValue, GuestNativeValueError> {
        if bytes.len() > maximum_bytes {
            return Err(GuestNativeValueError::TooLarge { maximum_bytes });
        }
        let parsed: JsonValue =
            serde_json::from_slice(bytes).map_err(|_| GuestNativeValueError::Malformed)?;
        let canonical = canonical_guest_native_value(parsed, format)?;
        let mut canonical_bytes = Vec::with_capacity(bytes.len());
        write_guest_native_json(&canonical, &mut canonical_bytes)?;
        if canonical_bytes != bytes {
            return Err(GuestNativeValueError::NonCanonical);
        }
        Ok(canonical)
    }
}

#[derive(Clone, Copy)]
enum GuestNativeValueFormat {
    Committed,
    Pending,
}

fn canonical_guest_native_value(
    value: JsonValue,
    format: GuestNativeValueFormat,
) -> Result<JsonValue, GuestNativeValueError> {
    match format {
        GuestNativeValueFormat::Committed => ConvexValue::try_from(value)
            .map(|value| value.to_internal_json())
            .map_err(|_| GuestNativeValueError::InvalidConvexValue),
        GuestNativeValueFormat::Pending => PendingValue::from_uncommitted_json(value)
            .map(|value| value.to_uncommitted_json())
            .map_err(|_| GuestNativeValueError::InvalidConvexValue),
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum GuestCapabilityRequestError {
    #[error("guest capability request exceeds the {maximum_bytes}-byte limit")]
    TooLarge { maximum_bytes: usize },
    #[error("guest capability request is malformed")]
    Malformed,
    #[error("guest capability request is invalid")]
    InvalidRequest,
    #[error("guest capability request is not canonical")]
    NonCanonical,
    #[error("guest capability request version is unsupported")]
    UnsupportedVersion,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GuestCapabilityRequestEnvelope {
    request: GuestCapabilityRequest,
    heap_size: usize,
}

impl GuestCapabilityRequestEnvelope {
    pub(crate) fn into_request(self) -> GuestCapabilityRequest {
        self.request
    }

    fn heap_size(&self) -> usize {
        self.heap_size
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GuestCapabilityRequest {
    AuthGetUserIdentity,
    AuditLog {
        body: JsonValue,
    },
    GetFunctionMetadata,
    GetDeploymentMetadata,
    GetTransactionMetrics,
    GetRequestMetadata,
    FunctionHandleCreate {
        function_address: GuestCapabilityFunctionAddress,
    },
    DbGet {
        table: Option<String>,
        id: JsonValue,
    },
    DbSystemGet {
        table: Option<String>,
        id: JsonValue,
    },
    DbNormalizeId {
        table: String,
        value: String,
    },
    DbInsert {
        table: String,
        value: JsonValue,
    },
    DbPatch {
        table: String,
        id: JsonValue,
        patch: JsonValue,
    },
    DbReplace {
        table: String,
        id: JsonValue,
        value: JsonValue,
    },
    DbDelete {
        table: String,
        id: JsonValue,
    },
    DbQuery {
        table: String,
        source: GuestCapabilityQuerySource,
        operators: Vec<GuestCapabilityQueryOperator>,
        order: GuestCapabilityQueryOrder,
        terminal: GuestCapabilityQueryTerminal,
    },
    StorageGetUrl {
        storage_id: String,
    },
    StorageGetMetadata {
        storage_id: String,
    },
    StorageGenerateUploadUrl,
    StorageDelete {
        storage_id: String,
    },
    RunUdf {
        udf_type: GuestCapabilityNestedUdfType,
        function_address: GuestCapabilityFunctionAddress,
        args: JsonValue,
        transaction_limits: Option<JsonValue>,
    },
    SchedulerRunAfter {
        delay_milliseconds: JsonValue,
        function_address: GuestCapabilityFunctionAddress,
        args: JsonValue,
    },
    SchedulerRunAt {
        timestamp_milliseconds: JsonValue,
        function_address: GuestCapabilityFunctionAddress,
        args: JsonValue,
    },
    SchedulerCancel {
        id: JsonValue,
    },
    EnvironmentVariableGet {
        name: String,
    },
    PerformanceNow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GuestCapabilityFunctionAddress {
    Name(String),
    Reference(String),
    FunctionHandle(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GuestCapabilityNestedUdfType {
    Query,
    Mutation,
    SnapshotQuery,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GuestCapabilityQueryConstraint {
    Eq { field: String, value: JsonValue },
    Gt { field: String, value: JsonValue },
    Gte { field: String, value: JsonValue },
    Lt { field: String, value: JsonValue },
    Lte { field: String, value: JsonValue },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GuestCapabilitySearchFilter {
    Search { field: String, value: String },
    Eq { field: String, value: JsonValue },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GuestCapabilityQuerySource {
    FullTableScan,
    IndexRange {
        index: String,
        constraints: Vec<GuestCapabilityQueryConstraint>,
    },
    Search {
        index: String,
        filters: Vec<GuestCapabilitySearchFilter>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GuestCapabilityQueryOperator {
    Filter { expression: JsonValue },
    Limit { limit: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GuestCapabilityQueryOrder {
    Default,
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GuestCapabilityQueryTerminal {
    Collect,
    First,
    Paginate(GuestCapabilityQueryPagination),
    Stream,
    Unique,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GuestCapabilityQueryPagination {
    pub(crate) cursor: Option<String>,
    pub(crate) end_cursor: Option<String>,
    pub(crate) maximum_bytes_read: Option<usize>,
    pub(crate) maximum_rows_read: Option<usize>,
    pub(crate) page_size: usize,
}

pub(crate) struct GuestCapabilityRequestCodec;

#[derive(Clone, Copy)]
enum GuestCapabilityRequestVersion {
    V1,
    V2,
    V3,
    V4,
}

impl GuestCapabilityRequestCodec {
    pub(crate) fn decode(
        bytes: &[u8],
        maximum_bytes: usize,
    ) -> Result<GuestCapabilityRequestEnvelope, GuestCapabilityRequestError> {
        if bytes.len() > maximum_bytes {
            return Err(GuestCapabilityRequestError::TooLarge { maximum_bytes });
        }
        let parsed: JsonValue =
            serde_json::from_slice(bytes).map_err(|_| GuestCapabilityRequestError::Malformed)?;
        let mut canonical_bytes = Vec::with_capacity(bytes.len());
        write_guest_native_json(&parsed, &mut canonical_bytes).map_err(|error| match error {
            GuestNativeValueError::Malformed => GuestCapabilityRequestError::Malformed,
            GuestNativeValueError::InvalidConvexValue => {
                GuestCapabilityRequestError::InvalidRequest
            },
            GuestNativeValueError::TooLarge { .. } | GuestNativeValueError::NonCanonical => {
                unreachable!("request canonical writer returned a decode-only error")
            },
        })?;
        if canonical_bytes != bytes {
            return Err(GuestCapabilityRequestError::NonCanonical);
        }
        let heap_size = parsed.heap_size();
        let request = decode_guest_capability_request(parsed)?;
        Ok(GuestCapabilityRequestEnvelope { request, heap_size })
    }

    #[cfg(test)]
    pub(crate) fn decode_value(
        value: JsonValue,
    ) -> Result<GuestCapabilityRequest, GuestCapabilityRequestError> {
        decode_guest_capability_request(value)
    }
}

fn decode_guest_capability_request(
    value: JsonValue,
) -> Result<GuestCapabilityRequest, GuestCapabilityRequestError> {
    let mut request = require_object(value)?;
    let version = match take_required(&mut request, "version")? {
        JsonValue::Number(version) if version.as_u64() == Some(1) => {
            GuestCapabilityRequestVersion::V1
        },
        JsonValue::Number(version) if version.as_u64() == Some(2) => {
            GuestCapabilityRequestVersion::V2
        },
        JsonValue::Number(version) if version.as_u64() == Some(3) => {
            GuestCapabilityRequestVersion::V3
        },
        JsonValue::Number(version) if version.as_u64() == Some(4) => {
            GuestCapabilityRequestVersion::V4
        },
        JsonValue::Number(_) => return Err(GuestCapabilityRequestError::UnsupportedVersion),
        _ => return Err(GuestCapabilityRequestError::InvalidRequest),
    };
    let kind = require_string(take_required(&mut request, "kind")?)?;
    let decoded = match kind.as_str() {
        "authGetUserIdentity" => GuestCapabilityRequest::AuthGetUserIdentity,
        "auditLog" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::AuditLog {
                body: JsonValue::Object(require_object(take_required(&mut request, "body")?)?),
            }
        },
        "getFunctionMetadata" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::GetFunctionMetadata
        },
        "getDeploymentMetadata" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::GetDeploymentMetadata
        },
        "getTransactionMetrics" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::GetTransactionMetrics
        },
        "getRequestMetadata" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::GetRequestMetadata
        },
        "functionHandleCreate" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::FunctionHandleCreate {
                function_address: decode_function_address(take_required(
                    &mut request,
                    "functionAddress",
                )?)?,
            }
        },
        "dbGet" => GuestCapabilityRequest::DbGet {
            table: match version {
                GuestCapabilityRequestVersion::V4 => request
                    .shift_remove("table")
                    .map(require_string)
                    .transpose()?,
                GuestCapabilityRequestVersion::V1
                | GuestCapabilityRequestVersion::V2
                | GuestCapabilityRequestVersion::V3 => Some(take_string(&mut request, "table")?),
            },
            id: decode_committed_value(take_required(&mut request, "id")?)?,
        },
        "dbSystemGet" => GuestCapabilityRequest::DbSystemGet {
            table: match version {
                GuestCapabilityRequestVersion::V4 => request
                    .shift_remove("table")
                    .map(require_string)
                    .transpose()?,
                GuestCapabilityRequestVersion::V1
                | GuestCapabilityRequestVersion::V2
                | GuestCapabilityRequestVersion::V3 => Some(take_string(&mut request, "table")?),
            },
            id: decode_committed_value(take_required(&mut request, "id")?)?,
        },
        "dbNormalizeId" => GuestCapabilityRequest::DbNormalizeId {
            table: take_string(&mut request, "table")?,
            value: take_string(&mut request, "value")?,
        },
        "dbInsert" => GuestCapabilityRequest::DbInsert {
            table: take_string(&mut request, "table")?,
            value: decode_write_value(take_required(&mut request, "value")?, version)?,
        },
        "dbPatch" => GuestCapabilityRequest::DbPatch {
            table: take_string(&mut request, "table")?,
            id: decode_committed_value(take_required(&mut request, "id")?)?,
            patch: decode_patch(take_required(&mut request, "patch")?, version)?,
        },
        "dbReplace" => GuestCapabilityRequest::DbReplace {
            table: take_string(&mut request, "table")?,
            id: decode_committed_value(take_required(&mut request, "id")?)?,
            value: decode_write_value(take_required(&mut request, "value")?, version)?,
        },
        "dbDelete" => GuestCapabilityRequest::DbDelete {
            table: take_string(&mut request, "table")?,
            id: decode_committed_value(take_required(&mut request, "id")?)?,
        },
        "dbQuery" => GuestCapabilityRequest::DbQuery {
            table: take_string(&mut request, "table")?,
            source: decode_query_source(take_required(&mut request, "source")?, version)?,
            operators: decode_query_operators(take_required(&mut request, "operators")?)?,
            order: decode_query_order(take_required(&mut request, "order")?)?,
            terminal: decode_query_terminal(
                take_required(&mut request, "terminal")?,
                &mut request,
                version,
            )?,
        },
        "storageGetUrl" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::StorageGetUrl {
                storage_id: decode_committed_string(take_required(&mut request, "storageId")?)?,
            }
        },
        "storageGetMetadata" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::StorageGetMetadata {
                storage_id: decode_committed_string(take_required(&mut request, "storageId")?)?,
            }
        },
        "storageGenerateUploadUrl" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::StorageGenerateUploadUrl
        },
        "storageDelete" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            GuestCapabilityRequest::StorageDelete {
                storage_id: decode_committed_string(take_required(&mut request, "storageId")?)?,
            }
        },
        "runUdf"
            if matches!(
                version,
                GuestCapabilityRequestVersion::V2
                    | GuestCapabilityRequestVersion::V3
                    | GuestCapabilityRequestVersion::V4
            ) =>
        {
            let udf_type = match require_string(take_required(&mut request, "udfType")?)?.as_str() {
                "query" => GuestCapabilityNestedUdfType::Query,
                "mutation" => GuestCapabilityNestedUdfType::Mutation,
                "snapshotQuery" => GuestCapabilityNestedUdfType::SnapshotQuery,
                _ => return Err(GuestCapabilityRequestError::InvalidRequest),
            };
            GuestCapabilityRequest::RunUdf {
                udf_type,
                function_address: decode_function_address(take_required(
                    &mut request,
                    "functionAddress",
                )?)?,
                args: decode_nested_udf_args(take_required(&mut request, "args")?, version)?,
                transaction_limits: decode_transaction_limits(take_required(
                    &mut request,
                    "transactionLimits",
                )?)?,
            }
        },
        "schedulerRunAfter" => GuestCapabilityRequest::SchedulerRunAfter {
            delay_milliseconds: require_number(take_required(&mut request, "delayMilliseconds")?)?,
            function_address: decode_function_address(take_required(
                &mut request,
                "functionAddress",
            )?)?,
            args: decode_committed_value(take_required(&mut request, "args")?)?,
        },
        "schedulerRunAt" => GuestCapabilityRequest::SchedulerRunAt {
            timestamp_milliseconds: require_number(take_required(
                &mut request,
                "timestampMilliseconds",
            )?)?,
            function_address: decode_function_address(take_required(
                &mut request,
                "functionAddress",
            )?)?,
            args: decode_committed_value(take_required(&mut request, "args")?)?,
        },
        "schedulerCancel" => GuestCapabilityRequest::SchedulerCancel {
            id: decode_committed_value(take_required(&mut request, "id")?)?,
        },
        "environmentVariableGet" => GuestCapabilityRequest::EnvironmentVariableGet {
            name: take_string(&mut request, "name")?,
        },
        "performanceNow" => GuestCapabilityRequest::PerformanceNow,
        _ => return Err(GuestCapabilityRequestError::InvalidRequest),
    };
    if matches!(
        &decoded,
        GuestCapabilityRequest::DbQuery {
            source: GuestCapabilityQuerySource::Search { .. },
            order: GuestCapabilityQueryOrder::Asc | GuestCapabilityQueryOrder::Desc,
            ..
        }
    ) {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    if !request.is_empty() {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    Ok(decoded)
}

fn require_object(
    value: JsonValue,
) -> Result<JsonMap<String, JsonValue>, GuestCapabilityRequestError> {
    value
        .as_object()
        .cloned()
        .ok_or(GuestCapabilityRequestError::InvalidRequest)
}

fn take_required(
    object: &mut JsonMap<String, JsonValue>,
    field: &str,
) -> Result<JsonValue, GuestCapabilityRequestError> {
    object
        .shift_remove(field)
        .ok_or(GuestCapabilityRequestError::InvalidRequest)
}

fn require_string(value: JsonValue) -> Result<String, GuestCapabilityRequestError> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or(GuestCapabilityRequestError::InvalidRequest)
}

fn take_string(
    object: &mut JsonMap<String, JsonValue>,
    field: &str,
) -> Result<String, GuestCapabilityRequestError> {
    require_string(take_required(object, field)?)
}

fn require_number(value: JsonValue) -> Result<JsonValue, GuestCapabilityRequestError> {
    if value.is_number() {
        Ok(value)
    } else {
        Err(GuestCapabilityRequestError::InvalidRequest)
    }
}

fn decode_committed_value(value: JsonValue) -> Result<JsonValue, GuestCapabilityRequestError> {
    let committed = ConvexValue::try_from(value.clone())
        .map_err(|_| GuestCapabilityRequestError::InvalidRequest)?
        .to_internal_json();
    // The complete request envelope has already passed the byte-level
    // canonicality check. Compare the normalized subtree structurally here so
    // nested values do not each require two more serialization passes.
    if !guest_native_values_equivalent(&committed, &value) {
        return Err(GuestCapabilityRequestError::NonCanonical);
    }
    Ok(committed)
}

fn decode_committed_string(value: JsonValue) -> Result<String, GuestCapabilityRequestError> {
    require_string(decode_committed_value(value)?)
}

fn decode_write_value(
    value: JsonValue,
    version: GuestCapabilityRequestVersion,
) -> Result<JsonValue, GuestCapabilityRequestError> {
    match version {
        GuestCapabilityRequestVersion::V1
        | GuestCapabilityRequestVersion::V2
        | GuestCapabilityRequestVersion::V3 => decode_committed_value(value),
        GuestCapabilityRequestVersion::V4 => decode_pending_value(value),
    }
}

fn decode_pending_value(value: JsonValue) -> Result<JsonValue, GuestCapabilityRequestError> {
    let pending = PendingValue::from_uncommitted_json(value.clone())
        .map_err(|_| GuestCapabilityRequestError::InvalidRequest)?
        .to_uncommitted_json();
    if !guest_native_values_equivalent(&pending, &value) {
        return Err(GuestCapabilityRequestError::NonCanonical);
    }
    Ok(pending)
}

fn guest_native_values_equivalent(left: &JsonValue, right: &JsonValue) -> bool {
    match (left, right) {
        (JsonValue::Null, JsonValue::Null) => true,
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left == right,
        (JsonValue::Number(left), JsonValue::Number(right)) => left.as_f64() == right.as_f64(),
        (JsonValue::String(left), JsonValue::String(right)) => left == right,
        (JsonValue::Array(left), JsonValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| guest_native_values_equivalent(left, right))
        },
        (JsonValue::Object(left), JsonValue::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| guest_native_values_equivalent(left, right))
                })
        },
        _ => false,
    }
}

fn decode_nested_udf_args(
    value: JsonValue,
    version: GuestCapabilityRequestVersion,
) -> Result<JsonValue, GuestCapabilityRequestError> {
    let args = decode_write_value(value, version)?;
    let pending = PendingValue::from_uncommitted_json(args.clone())
        .map_err(|_| GuestCapabilityRequestError::InvalidRequest)?;
    if !pending.is_object() {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    Ok(args)
}

fn decode_patch(
    value: JsonValue,
    version: GuestCapabilityRequestVersion,
) -> Result<JsonValue, GuestCapabilityRequestError> {
    let patch = require_object(value)?;
    let mut decoded = JsonMap::with_capacity(patch.len());
    for (field, value) in patch {
        field
            .parse::<FieldName>()
            .map_err(|_| GuestCapabilityRequestError::InvalidRequest)?;
        let value = if is_undefined_marker(&value) {
            value
        } else {
            decode_write_value(value, version)?
        };
        decoded.insert(field, value);
    }
    Ok(JsonValue::Object(decoded))
}

pub(super) fn is_undefined_marker(value: &JsonValue) -> bool {
    let Some(marker) = value.as_object() else {
        return false;
    };
    marker.len() == 1 && marker.get("$undefined") == Some(&JsonValue::Null)
}

fn decode_query_source(
    value: JsonValue,
    version: GuestCapabilityRequestVersion,
) -> Result<GuestCapabilityQuerySource, GuestCapabilityRequestError> {
    let mut source = require_object(value)?;
    let source_type = take_string(&mut source, "type")?;
    let decoded = match source_type.as_str() {
        "fullTableScan" => GuestCapabilityQuerySource::FullTableScan,
        "indexRange" => {
            let index = take_string(&mut source, "index")?;
            let constraints = require_array(take_required(&mut source, "constraints")?)?
                .into_iter()
                .map(|constraint| decode_query_constraint(constraint, version))
                .collect::<Result<Vec<_>, _>>()?;
            GuestCapabilityQuerySource::IndexRange { index, constraints }
        },
        "search" if matches!(version, GuestCapabilityRequestVersion::V4) => {
            let index = take_string(&mut source, "index")?;
            let filters = decode_search_filters(take_required(&mut source, "filters")?)?;
            GuestCapabilityQuerySource::Search { index, filters }
        },
        _ => return Err(GuestCapabilityRequestError::InvalidRequest),
    };
    if !source.is_empty() {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    Ok(decoded)
}

fn decode_search_filters(
    value: JsonValue,
) -> Result<Vec<GuestCapabilitySearchFilter>, GuestCapabilityRequestError> {
    let filters = require_array(value)?;
    if filters.is_empty() {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    filters
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            let mut filter = require_object(value)?;
            let filter_type = take_string(&mut filter, "type")?;
            let field = take_string(&mut filter, "field")?;
            let decoded = match (index, filter_type.as_str()) {
                (0, "search") => GuestCapabilitySearchFilter::Search {
                    field,
                    value: take_string(&mut filter, "value")?,
                },
                (_, "eq") if index > 0 => {
                    let value = take_required(&mut filter, "value")?;
                    let value = if is_undefined_marker(&value) {
                        value
                    } else {
                        decode_committed_value(value)?
                    };
                    GuestCapabilitySearchFilter::Eq { field, value }
                },
                _ => return Err(GuestCapabilityRequestError::InvalidRequest),
            };
            if !filter.is_empty() {
                return Err(GuestCapabilityRequestError::InvalidRequest);
            }
            Ok(decoded)
        })
        .collect()
}

fn decode_query_constraint(
    value: JsonValue,
    version: GuestCapabilityRequestVersion,
) -> Result<GuestCapabilityQueryConstraint, GuestCapabilityRequestError> {
    let mut constraint = require_object(value)?;
    let operator = take_string(&mut constraint, "operator")?;
    let field = take_string(&mut constraint, "field")?;
    let value = match version {
        GuestCapabilityRequestVersion::V1
        | GuestCapabilityRequestVersion::V2
        | GuestCapabilityRequestVersion::V3 => {
            decode_committed_value(take_required(&mut constraint, "value")?)?
        },
        GuestCapabilityRequestVersion::V4 => {
            let value = take_required(&mut constraint, "value")?;
            if is_undefined_marker(&value) {
                value
            } else {
                decode_pending_value(value)?
            }
        },
    };
    if !constraint.is_empty() {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    Ok(match operator.as_str() {
        "eq" => GuestCapabilityQueryConstraint::Eq { field, value },
        "gt" => GuestCapabilityQueryConstraint::Gt { field, value },
        "gte" => GuestCapabilityQueryConstraint::Gte { field, value },
        "lt" => GuestCapabilityQueryConstraint::Lt { field, value },
        "lte" => GuestCapabilityQueryConstraint::Lte { field, value },
        _ => return Err(GuestCapabilityRequestError::InvalidRequest),
    })
}

fn decode_query_operators(
    value: JsonValue,
) -> Result<Vec<GuestCapabilityQueryOperator>, GuestCapabilityRequestError> {
    require_array(value)?
        .into_iter()
        .map(|value| {
            let mut operator = require_object(value)?;
            let operator_type = take_string(&mut operator, "type")?;
            let decoded = match operator_type.as_str() {
                "filter" => GuestCapabilityQueryOperator::Filter {
                    expression: decode_query_expression(take_required(
                        &mut operator,
                        "expression",
                    )?)?,
                },
                "limit" => GuestCapabilityQueryOperator::Limit {
                    limit: decode_nonnegative_integer(take_required(&mut operator, "limit")?)?,
                },
                _ => return Err(GuestCapabilityRequestError::InvalidRequest),
            };
            if !operator.is_empty() {
                return Err(GuestCapabilityRequestError::InvalidRequest);
            }
            Ok(decoded)
        })
        .collect()
}

fn decode_query_expression(value: JsonValue) -> Result<JsonValue, GuestCapabilityRequestError> {
    let expression = require_object(value)?;
    if expression.len() != 1 {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    let (operator, operand) = expression
        .into_iter()
        .next()
        .expect("validated singleton query expression disappeared");
    let operand = match operator.as_str() {
        "$literal" => decode_committed_value(operand)?,
        "$field" => JsonValue::String(require_string(operand)?),
        "$eq" | "$neq" | "$lt" | "$lte" | "$gt" | "$gte" | "$add" | "$sub" | "$mul" | "$div"
        | "$mod" => {
            let operands = require_array(operand)?;
            let [left, right]: [JsonValue; 2] = operands
                .try_into()
                .map_err(|_| GuestCapabilityRequestError::InvalidRequest)?;
            JsonValue::Array(vec![
                decode_query_expression(left)?,
                decode_query_expression(right)?,
            ])
        },
        "$neg" | "$not" => decode_query_expression(operand)?,
        "$and" | "$or" => JsonValue::Array(
            require_array(operand)?
                .into_iter()
                .map(decode_query_expression)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => return Err(GuestCapabilityRequestError::InvalidRequest),
    };
    Ok(JsonValue::Object(JsonMap::from_iter([(operator, operand)])))
}

fn require_array(value: JsonValue) -> Result<Vec<JsonValue>, GuestCapabilityRequestError> {
    value
        .as_array()
        .cloned()
        .ok_or(GuestCapabilityRequestError::InvalidRequest)
}

fn decode_nonnegative_integer(value: JsonValue) -> Result<u64, GuestCapabilityRequestError> {
    let JsonValue::Number(value) = value else {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    };
    if let Some(value) = value.as_u64() {
        return Ok(value);
    }
    value
        .as_f64()
        .filter(|value| {
            value.is_finite() && *value >= 0.0 && value.fract() == 0.0 && *value < u64::MAX as f64
        })
        .map(|value| value as u64)
        .ok_or(GuestCapabilityRequestError::InvalidRequest)
}

fn decode_query_order(
    value: JsonValue,
) -> Result<GuestCapabilityQueryOrder, GuestCapabilityRequestError> {
    match value {
        JsonValue::Null => Ok(GuestCapabilityQueryOrder::Default),
        JsonValue::String(order) if order == "asc" => Ok(GuestCapabilityQueryOrder::Asc),
        JsonValue::String(order) if order == "desc" => Ok(GuestCapabilityQueryOrder::Desc),
        _ => Err(GuestCapabilityRequestError::InvalidRequest),
    }
}

fn decode_query_terminal(
    value: JsonValue,
    request: &mut JsonMap<String, JsonValue>,
    version: GuestCapabilityRequestVersion,
) -> Result<GuestCapabilityQueryTerminal, GuestCapabilityRequestError> {
    match value.as_str() {
        Some("collect") => Ok(GuestCapabilityQueryTerminal::Collect),
        Some("first") => Ok(GuestCapabilityQueryTerminal::First),
        Some("paginate")
            if matches!(
                version,
                GuestCapabilityRequestVersion::V3 | GuestCapabilityRequestVersion::V4
            ) =>
        {
            Ok(GuestCapabilityQueryTerminal::Paginate(
                decode_query_pagination(take_required(request, "pagination")?)?,
            ))
        },
        Some("stream") if matches!(version, GuestCapabilityRequestVersion::V4) => {
            Ok(GuestCapabilityQueryTerminal::Stream)
        },
        Some("unique") => Ok(GuestCapabilityQueryTerminal::Unique),
        _ => Err(GuestCapabilityRequestError::InvalidRequest),
    }
}

fn decode_query_pagination(
    value: JsonValue,
) -> Result<GuestCapabilityQueryPagination, GuestCapabilityRequestError> {
    let mut pagination = require_object(value)?;
    let decoded = GuestCapabilityQueryPagination {
        cursor: decode_nullable_string(take_required(&mut pagination, "cursor")?)?,
        end_cursor: decode_nullable_string(take_required(&mut pagination, "endCursor")?)?,
        maximum_bytes_read: decode_nullable_nonnegative_integer(take_required(
            &mut pagination,
            "maximumBytesRead",
        )?)?,
        maximum_rows_read: decode_nullable_nonnegative_integer(take_required(
            &mut pagination,
            "maximumRowsRead",
        )?)?,
        page_size: decode_nonnegative_usize(take_required(&mut pagination, "pageSize")?)?,
    };
    if !pagination.is_empty() {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    Ok(decoded)
}

fn decode_nullable_string(value: JsonValue) -> Result<Option<String>, GuestCapabilityRequestError> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::String(value) => Ok(Some(value)),
        _ => Err(GuestCapabilityRequestError::InvalidRequest),
    }
}

fn decode_nullable_nonnegative_integer(
    value: JsonValue,
) -> Result<Option<usize>, GuestCapabilityRequestError> {
    match value {
        JsonValue::Null => Ok(None),
        value => decode_nonnegative_usize(value).map(Some),
    }
}

fn decode_nonnegative_usize(value: JsonValue) -> Result<usize, GuestCapabilityRequestError> {
    usize::try_from(decode_nonnegative_integer(value)?)
        .map_err(|_| GuestCapabilityRequestError::InvalidRequest)
}

fn decode_function_address(
    value: JsonValue,
) -> Result<GuestCapabilityFunctionAddress, GuestCapabilityRequestError> {
    let address = require_object(value)?;
    if address.len() != 1 {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    let (kind, value) = address
        .into_iter()
        .next()
        .expect("validated singleton function address disappeared");
    let value = require_string(value)?;
    if value.is_empty() {
        return Err(GuestCapabilityRequestError::InvalidRequest);
    }
    match kind.as_str() {
        "name" => Ok(GuestCapabilityFunctionAddress::Name(value)),
        "reference" => Ok(GuestCapabilityFunctionAddress::Reference(value)),
        "functionHandle" => Ok(GuestCapabilityFunctionAddress::FunctionHandle(value)),
        _ => Err(GuestCapabilityRequestError::InvalidRequest),
    }
}

fn decode_transaction_limits(
    value: JsonValue,
) -> Result<Option<JsonValue>, GuestCapabilityRequestError> {
    let JsonValue::Object(limits) = value else {
        return if value.is_null() {
            Ok(None)
        } else {
            Err(GuestCapabilityRequestError::InvalidRequest)
        };
    };
    let mut decoded = JsonMap::with_capacity(limits.len());
    for (field, value) in limits {
        if !matches!(
            field.as_str(),
            "bytesRead"
                | "bytesWritten"
                | "databaseQueries"
                | "documentsRead"
                | "documentsWritten"
                | "functionsScheduled"
                | "scheduledFunctionArgsBytes"
        ) {
            return Err(GuestCapabilityRequestError::InvalidRequest);
        }
        let value = decode_nonnegative_integer(value)?;
        usize::try_from(value).map_err(|_| GuestCapabilityRequestError::InvalidRequest)?;
        decoded.insert(field, JsonValue::Number(value.into()));
    }
    Ok(Some(JsonValue::Object(decoded)))
}

fn write_guest_native_json(
    value: &JsonValue,
    output: &mut Vec<u8>,
) -> Result<(), GuestNativeValueError> {
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::String(_) => {
            serde_json::to_writer(output, value).map_err(|_| GuestNativeValueError::Malformed)?;
        },
        JsonValue::Number(number) => {
            let number = number
                .as_f64()
                .filter(|number| number.is_finite())
                .ok_or(GuestNativeValueError::InvalidConvexValue)?;
            // The guest writes numbers with JSON.stringify, whose integral and
            // exponent spellings differ from serde_json's f64 serialization.
            output.extend_from_slice(ryu_js::Buffer::new().format_finite(number).as_bytes());
        },
        JsonValue::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_guest_native_json(value, output)?;
            }
            output.push(b']');
        },
        JsonValue::Object(values) => {
            output.push(b'{');
            let mut keys = values.keys().collect::<Vec<_>>();
            // JSON.stringify emits array-index property names numerically
            // before other names, even when the object was built in sorted order.
            keys.sort_unstable_by(|left, right| {
                let array_index = |key: &str| {
                    if key.len() > 10 || !key.as_bytes().first().is_some_and(u8::is_ascii_digit) {
                        return None;
                    }
                    key.parse::<u32>()
                        .ok()
                        .filter(|index| *index != u32::MAX && index.to_string() == key)
                };
                match (array_index(left), array_index(right)) {
                    (Some(left), Some(right)) => left.cmp(&right),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => left.cmp(right),
                }
            });
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)
                    .map_err(|_| GuestNativeValueError::Malformed)?;
                output.push(b':');
                write_guest_native_json(
                    values
                        .get(key)
                        .expect("guest-native JSON object key disappeared"),
                    output,
                )?;
            }
            output.push(b'}');
        },
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) struct OpaqueHandle(u64);

impl OpaqueHandle {
    pub(crate) fn from_abi(value: i64) -> Result<Self, OpaqueValueError> {
        let value = u64::try_from(value).map_err(|_| OpaqueValueError::InvalidHandle)?;
        if value == 0 || value > MAX_EXACT_JAVASCRIPT_INTEGER {
            return Err(OpaqueValueError::InvalidHandle);
        }
        Ok(Self(value))
    }

    pub(crate) fn to_abi(self) -> i64 {
        // All current kind tags keep the sign bit clear.
        i64::try_from(self.0).expect("opaque handle kind tag exceeded the signed ABI")
    }

    fn new(
        slot_index: usize,
        generation: u32,
        kind: OpaqueValueKind,
    ) -> Result<Self, OpaqueValueError> {
        let one_based_index = slot_index
            .checked_add(1)
            .ok_or(OpaqueValueError::HandleSpaceExhausted)?;
        if one_based_index > MAX_ENCODED_SLOTS || generation == 0 {
            return Err(OpaqueValueError::HandleSpaceExhausted);
        }
        let raw = (u64::from(kind.tag()) << HANDLE_KIND_SHIFT)
            | (u64::from(generation) << HANDLE_GENERATION_SHIFT)
            | u64::try_from(one_based_index).map_err(|_| OpaqueValueError::HandleSpaceExhausted)?;
        Ok(Self(raw))
    }

    fn parts(self) -> Result<HandleParts, OpaqueValueError> {
        let one_based_index = self.0 & HANDLE_INDEX_MASK;
        let generation = (self.0 >> HANDLE_GENERATION_SHIFT) & HANDLE_GENERATION_MASK;
        let kind_tag = self.0 >> HANDLE_KIND_SHIFT;
        if one_based_index == 0 || generation == 0 {
            return Err(OpaqueValueError::InvalidHandle);
        }
        Ok(HandleParts {
            slot_index: usize::try_from(one_based_index - 1)
                .map_err(|_| OpaqueValueError::InvalidHandle)?,
            generation: u32::try_from(generation).map_err(|_| OpaqueValueError::InvalidHandle)?,
            encoded_kind: OpaqueValueKind::from_tag(
                u8::try_from(kind_tag).map_err(|_| OpaqueValueError::InvalidHandle)?,
            )?,
        })
    }
}

impl fmt::Debug for OpaqueHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("OpaqueHandle")
            .field(&format_args!("{:#x}", self.0))
            .finish()
    }
}

struct HandleParts {
    slot_index: usize,
    generation: u32,
    encoded_kind: OpaqueValueKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum OpaqueValueKind {
    ConvexJson = 1,
    Bytes = 2,
    QueryCursor = 3,
    CapabilityRequest = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpaqueJsonKind {
    Null,
    Boolean,
    Number,
    String,
    Array,
    Object,
}

impl OpaqueValueKind {
    fn tag(self) -> u8 {
        self as u8
    }

    fn from_tag(tag: u8) -> Result<Self, OpaqueValueError> {
        match tag {
            1 => Ok(Self::ConvexJson),
            2 => Ok(Self::Bytes),
            3 => Ok(Self::QueryCursor),
            4 => Ok(Self::CapabilityRequest),
            _ => Err(OpaqueValueError::InvalidHandle),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OpaqueValue {
    ConvexJson(JsonValue),
    Bytes(Vec<u8>),
    QueryCursor(u32),
    CapabilityRequest(GuestCapabilityRequestEnvelope),
}

impl OpaqueValue {
    pub(crate) fn kind(&self) -> OpaqueValueKind {
        match self {
            Self::ConvexJson(_) => OpaqueValueKind::ConvexJson,
            Self::Bytes(_) => OpaqueValueKind::Bytes,
            Self::QueryCursor(_) => OpaqueValueKind::QueryCursor,
            Self::CapabilityRequest(_) => OpaqueValueKind::CapabilityRequest,
        }
    }

    fn host_owned_bytes(&self) -> usize {
        size_of::<Self>()
            + match self {
                Self::ConvexJson(value) => value.heap_size(),
                Self::Bytes(bytes) => bytes.heap_size(),
                Self::QueryCursor(_) => 0,
                Self::CapabilityRequest(request) => request.heap_size(),
            }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct HostOwnedByteAccounting {
    pub(crate) current_bytes: usize,
    pub(crate) peak_bytes: usize,
    pub(crate) retained_bytes: usize,
}

pub(crate) trait HostOwnedByteObserver: Send + Sync {
    fn host_owned_bytes_changing(&self, _accounting: HostOwnedByteAccounting) -> bool {
        true
    }

    fn host_owned_bytes_changed(&self, accounting: HostOwnedByteAccounting) -> bool;
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum OpaqueValueError {
    #[error("opaque value handle is invalid or no longer owned by this invocation")]
    InvalidHandle,
    #[error("opaque value handle space is exhausted")]
    HandleSpaceExhausted,
    #[error("opaque value handle limit of {maximum} was reached")]
    HandleLimitExceeded { maximum: usize },
    #[error(
        "opaque host-owned value bytes exceeded the invocation limit of {maximum_bytes} bytes"
    )]
    HostOwnedBytesLimitExceeded { maximum_bytes: usize },
    #[error("opaque host-owned value allocation exceeded the aggregate runtime memory limit")]
    AggregateMemoryLimitExceeded,
    #[error("opaque value kind mismatch: expected {expected:?}, found {actual:?}")]
    KindMismatch {
        expected: OpaqueValueKind,
        actual: OpaqueValueKind,
    },
    #[error("opaque JSON value has the wrong shape: expected {expected}")]
    JsonShapeMismatch { expected: &'static str },
    #[error("non-finite numbers cannot cross the opaque Convex value ABI")]
    NonFiniteNumber,
    #[error("opaque JSON object field name is invalid")]
    InvalidFieldName,
    #[error("opaque JSON object field {field:?} is already present")]
    DuplicateObjectField { field: String },
    #[error("opaque array index is out of bounds")]
    ArrayIndexOutOfBounds,
    #[error("final function result was already set")]
    FinalResultAlreadySet,
    #[error("final function result is not available")]
    FinalResultMissing,
    #[error(
        "opaque invocation completed with {value_count} unreleased values and {host_owned_bytes} \
         bytes of outstanding invocation charge"
    )]
    LeakedValues {
        value_count: usize,
        host_owned_bytes: usize,
    },
}

struct AccountedValue {
    value: OpaqueValue,
    host_owned_bytes: usize,
}

impl AccountedValue {
    fn new(value: OpaqueValue) -> Self {
        let host_owned_bytes = value.host_owned_bytes();
        Self {
            value,
            host_owned_bytes,
        }
    }
}

#[derive(Default)]
struct HandleSlot {
    generation: u32,
    value: Option<AccountedValue>,
    retired: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CleanupReport {
    pub(crate) released_value_count: usize,
    pub(crate) released_host_owned_bytes: usize,
}

pub(crate) struct OpaqueValueTable {
    slots: Vec<HandleSlot>,
    reusable_slots: Vec<usize>,
    live_handle_count: usize,
    maximum_live_handles: usize,
    maximum_host_owned_bytes: usize,
    accounting: HostOwnedByteAccounting,
    final_result: Option<AccountedValue>,
    observer: Option<Arc<dyn HostOwnedByteObserver>>,
}

impl OpaqueValueTable {
    pub(crate) fn new(maximum_live_handles: usize, maximum_host_owned_bytes: usize) -> Self {
        Self::new_with_observer(maximum_live_handles, maximum_host_owned_bytes, None)
    }

    pub(crate) fn new_with_observer(
        maximum_live_handles: usize,
        maximum_host_owned_bytes: usize,
        observer: Option<Arc<dyn HostOwnedByteObserver>>,
    ) -> Self {
        let table = Self {
            slots: Vec::new(),
            reusable_slots: Vec::new(),
            live_handle_count: 0,
            maximum_live_handles,
            maximum_host_owned_bytes,
            accounting: HostOwnedByteAccounting::default(),
            final_result: None,
            observer,
        };
        let _ = table.notify_observer();
        table
    }

    pub(crate) fn accounting(&self) -> HostOwnedByteAccounting {
        self.observed_accounting()
    }

    pub(crate) fn begin_invocation(
        &mut self,
        observer: Arc<dyn HostOwnedByteObserver>,
    ) -> Result<(), OpaqueValueError> {
        self.finish_invocation()?;
        self.observer = Some(observer);
        if !self.notify_observer() {
            return Err(OpaqueValueError::AggregateMemoryLimitExceeded);
        }
        Ok(())
    }

    pub(crate) fn live_handle_count(&self) -> usize {
        self.live_handle_count
    }

    pub(crate) fn insert(&mut self, value: OpaqueValue) -> Result<OpaqueHandle, OpaqueValueError> {
        let accounted = AccountedValue::new(value);
        if self.live_handle_count >= self.maximum_live_handles {
            return Err(OpaqueValueError::HandleLimitExceeded {
                maximum: self.maximum_live_handles,
            });
        }
        self.reserve_host_bytes(accounted.host_owned_bytes)?;

        let slot_index = self.allocate_slot()?;
        let slot = &mut self.slots[slot_index];
        let generation = slot
            .generation
            .checked_add(1)
            .ok_or(OpaqueValueError::HandleSpaceExhausted)?;
        slot.generation = generation;
        let kind = accounted.value.kind();
        slot.value = Some(accounted);
        self.live_handle_count += 1;
        self.add_host_bytes(
            self.slots[slot_index]
                .value
                .as_ref()
                .expect("inserted opaque value disappeared")
                .host_owned_bytes,
        )?;
        OpaqueHandle::new(slot_index, generation, kind)
    }

    pub(crate) fn insert_json(
        &mut self,
        value: JsonValue,
    ) -> Result<OpaqueHandle, OpaqueValueError> {
        self.insert(OpaqueValue::ConvexJson(value))
    }

    pub(crate) fn insert_bytes(
        &mut self,
        value: Vec<u8>,
    ) -> Result<OpaqueHandle, OpaqueValueError> {
        self.insert(OpaqueValue::Bytes(value))
    }

    pub(crate) fn insert_null(&mut self) -> Result<OpaqueHandle, OpaqueValueError> {
        self.insert_json(JsonValue::Null)
    }

    pub(crate) fn insert_bool(&mut self, value: bool) -> Result<OpaqueHandle, OpaqueValueError> {
        self.insert_json(JsonValue::Bool(value))
    }

    pub(crate) fn insert_i64(&mut self, value: i64) -> Result<OpaqueHandle, OpaqueValueError> {
        self.insert_json(JsonValue::Number(JsonNumber::from(value)))
    }

    pub(crate) fn insert_f64(&mut self, value: f64) -> Result<OpaqueHandle, OpaqueValueError> {
        let number = JsonNumber::from_f64(value).ok_or(OpaqueValueError::NonFiniteNumber)?;
        self.insert_json(JsonValue::Number(number))
    }

    pub(crate) fn insert_string(
        &mut self,
        value: String,
    ) -> Result<OpaqueHandle, OpaqueValueError> {
        self.insert_json(JsonValue::String(value))
    }

    pub(crate) fn insert_array(&mut self) -> Result<OpaqueHandle, OpaqueValueError> {
        self.insert_json(JsonValue::Array(Vec::new()))
    }

    pub(crate) fn insert_object(&mut self) -> Result<OpaqueHandle, OpaqueValueError> {
        self.insert_json(JsonValue::Object(JsonMap::new()))
    }

    pub(crate) fn get(
        &self,
        handle: OpaqueHandle,
        expected_kind: OpaqueValueKind,
    ) -> Result<&OpaqueValue, OpaqueValueError> {
        Ok(&self.entry(handle, expected_kind)?.value)
    }

    pub(crate) fn get_json(&self, handle: OpaqueHandle) -> Result<&JsonValue, OpaqueValueError> {
        match self.get(handle, OpaqueValueKind::ConvexJson)? {
            OpaqueValue::ConvexJson(value) => Ok(value),
            OpaqueValue::Bytes(_)
            | OpaqueValue::QueryCursor(_)
            | OpaqueValue::CapabilityRequest(_) => {
                unreachable!("opaque value kind validation returned a different variant")
            },
        }
    }

    pub(crate) fn bytes_value(&self, handle: OpaqueHandle) -> Result<&[u8], OpaqueValueError> {
        match self.get(handle, OpaqueValueKind::Bytes)? {
            OpaqueValue::Bytes(value) => Ok(value),
            OpaqueValue::ConvexJson(_)
            | OpaqueValue::QueryCursor(_)
            | OpaqueValue::CapabilityRequest(_) => {
                unreachable!("opaque value kind validation returned a different variant")
            },
        }
    }

    pub(crate) fn json_kind(
        &self,
        handle: OpaqueHandle,
    ) -> Result<OpaqueJsonKind, OpaqueValueError> {
        Ok(match self.get_json(handle)? {
            JsonValue::Null => OpaqueJsonKind::Null,
            JsonValue::Bool(_) => OpaqueJsonKind::Boolean,
            JsonValue::Number(_) => OpaqueJsonKind::Number,
            JsonValue::String(_) => OpaqueJsonKind::String,
            JsonValue::Array(_) => OpaqueJsonKind::Array,
            JsonValue::Object(_) => OpaqueJsonKind::Object,
        })
    }

    pub(crate) fn bool_value(&self, handle: OpaqueHandle) -> Result<bool, OpaqueValueError> {
        self.get_json(handle)?
            .as_bool()
            .ok_or(OpaqueValueError::JsonShapeMismatch {
                expected: "boolean",
            })
    }

    pub(crate) fn number_value(&self, handle: OpaqueHandle) -> Result<f64, OpaqueValueError> {
        self.get_json(handle)?
            .as_f64()
            .ok_or(OpaqueValueError::JsonShapeMismatch { expected: "number" })
    }

    pub(crate) fn string_value(&self, handle: OpaqueHandle) -> Result<&str, OpaqueValueError> {
        self.get_json(handle)?
            .as_str()
            .ok_or(OpaqueValueError::JsonShapeMismatch { expected: "string" })
    }

    pub(crate) fn take(
        &mut self,
        handle: OpaqueHandle,
        expected_kind: OpaqueValueKind,
    ) -> Result<OpaqueValue, OpaqueValueError> {
        let accounted = self.take_accounted(handle, expected_kind)?;
        self.remove_host_bytes(accounted.host_owned_bytes)?;
        Ok(accounted.value)
    }

    /// Transfers a syscall operand out of the handle table while conservatively
    /// retaining its byte charge for the rest of the invocation. The handle is
    /// invalidated immediately, but later guest allocations cannot reuse the
    /// transferred operand's budget even after the host-side request drops it.
    pub(crate) fn take_transferred(
        &mut self,
        handle: OpaqueHandle,
        expected_kind: OpaqueValueKind,
    ) -> Result<OpaqueValue, OpaqueValueError> {
        let accounted = self.take_accounted(handle, expected_kind)?;
        // take_accounted may grow reusable_slots without changing current_bytes.
        if !self.notify_observer() {
            return Err(OpaqueValueError::AggregateMemoryLimitExceeded);
        }
        Ok(accounted.value)
    }

    /// Transfers an operation operand before validating its value kind. This
    /// lets a terminal host-import error invalidate a valid but wrong-kind
    /// operand instead of leaving it owned by the guest.
    pub(crate) fn take_transferred_operand(
        &mut self,
        handle: OpaqueHandle,
    ) -> Result<OpaqueValue, OpaqueValueError> {
        let parts = handle.parts()?;
        let encoded_kind = parts.encoded_kind;
        let accounted = self.take_accounted(handle, encoded_kind)?;
        // take_accounted may grow reusable_slots without changing current_bytes.
        if !self.notify_observer() {
            return Err(OpaqueValueError::AggregateMemoryLimitExceeded);
        }
        Ok(accounted.value)
    }

    pub(crate) fn release(
        &mut self,
        handle: OpaqueHandle,
        expected_kind: OpaqueValueKind,
    ) -> Result<(), OpaqueValueError> {
        drop(self.take(handle, expected_kind)?);
        Ok(())
    }

    /// Looks up either a request argument or a document property by its UTF-8
    /// field name. Missing properties are represented without allocating a
    /// sentinel handle.
    pub(crate) fn clone_object_field(
        &mut self,
        object_handle: OpaqueHandle,
        field_name: &str,
    ) -> Result<Option<OpaqueHandle>, OpaqueValueError> {
        validate_field_name(field_name)?;
        let value = self
            .get_json(object_handle)?
            .as_object()
            .ok_or(OpaqueValueError::JsonShapeMismatch { expected: "object" })?
            .get(field_name)
            .cloned();
        value.map(|value| self.insert_json(value)).transpose()
    }

    pub(crate) fn array_len(&self, array_handle: OpaqueHandle) -> Result<usize, OpaqueValueError> {
        self.get_json(array_handle)?
            .as_array()
            .map(Vec::len)
            .ok_or(OpaqueValueError::JsonShapeMismatch { expected: "array" })
    }

    pub(crate) fn clone_array_element(
        &mut self,
        array_handle: OpaqueHandle,
        index: usize,
    ) -> Result<OpaqueHandle, OpaqueValueError> {
        let value = self
            .get_json(array_handle)?
            .as_array()
            .ok_or(OpaqueValueError::JsonShapeMismatch { expected: "array" })?
            .get(index)
            .cloned()
            .ok_or(OpaqueValueError::ArrayIndexOutOfBounds)?;
        self.insert_json(value)
    }

    pub(crate) fn array_push(
        &mut self,
        array_handle: OpaqueHandle,
        value_handle: OpaqueHandle,
    ) -> Result<(), OpaqueValueError> {
        if array_handle == value_handle {
            return Err(OpaqueValueError::InvalidHandle);
        }
        if !self.get_json(array_handle)?.is_array() {
            return Err(OpaqueValueError::JsonShapeMismatch { expected: "array" });
        }
        self.get_json(value_handle)?;

        let child = self.take_accounted(value_handle, OpaqueValueKind::ConvexJson)?;
        let child_bytes = child.host_owned_bytes;
        let OpaqueValue::ConvexJson(child_value) = child.value else {
            unreachable!("validated array child changed kind")
        };
        let container_parts = array_handle.parts()?;
        let container = self.slots[container_parts.slot_index]
            .value
            .as_mut()
            .expect("validated opaque array disappeared");
        let old_container_bytes = container.host_owned_bytes;
        let OpaqueValue::ConvexJson(container_value) = &mut container.value else {
            unreachable!("validated opaque array changed kind")
        };
        container_value
            .as_array_mut()
            .expect("validated opaque array changed shape")
            .push(child_value);
        container.host_owned_bytes = container.value.host_owned_bytes();
        let new_container_bytes = container.host_owned_bytes;
        self.finish_child_move(old_container_bytes, child_bytes, new_container_bytes)
    }

    pub(crate) fn object_insert(
        &mut self,
        object_handle: OpaqueHandle,
        field_name: String,
        value_handle: OpaqueHandle,
    ) -> Result<(), OpaqueValueError> {
        validate_field_name(&field_name)?;
        if object_handle == value_handle {
            return Err(OpaqueValueError::InvalidHandle);
        }
        let object = self
            .get_json(object_handle)?
            .as_object()
            .ok_or(OpaqueValueError::JsonShapeMismatch { expected: "object" })?;
        if object.contains_key(&field_name) {
            return Err(OpaqueValueError::DuplicateObjectField { field: field_name });
        }
        self.get_json(value_handle)?;

        let child = self.take_accounted(value_handle, OpaqueValueKind::ConvexJson)?;
        let child_bytes = child.host_owned_bytes;
        let OpaqueValue::ConvexJson(child_value) = child.value else {
            unreachable!("validated object child changed kind")
        };
        let container_parts = object_handle.parts()?;
        let container = self.slots[container_parts.slot_index]
            .value
            .as_mut()
            .expect("validated opaque object disappeared");
        let old_container_bytes = container.host_owned_bytes;
        let OpaqueValue::ConvexJson(container_value) = &mut container.value else {
            unreachable!("validated opaque object changed kind")
        };
        assert!(
            container_value
                .as_object_mut()
                .expect("validated opaque object changed shape")
                .insert(field_name, child_value)
                .is_none(),
            "validated absent opaque object field became present"
        );
        container.host_owned_bytes = container.value.host_owned_bytes();
        let new_container_bytes = container.host_owned_bytes;
        self.finish_child_move(old_container_bytes, child_bytes, new_container_bytes)
    }

    /// Transfers the handle's value into the invocation result slot. This
    /// invalidates the handle but retains its byte charge until the host takes
    /// or cleans up the result.
    pub(crate) fn set_final_result(
        &mut self,
        result_handle: OpaqueHandle,
    ) -> Result<(), OpaqueValueError> {
        if self.final_result.is_some() {
            return Err(OpaqueValueError::FinalResultAlreadySet);
        }
        let result = self.take_accounted(result_handle, OpaqueValueKind::ConvexJson)?;
        self.final_result = Some(result);
        if !self.notify_observer() {
            return Err(OpaqueValueError::AggregateMemoryLimitExceeded);
        }
        Ok(())
    }

    pub(crate) fn take_final_result(&mut self) -> Result<JsonValue, OpaqueValueError> {
        let result = self
            .final_result
            .take()
            .ok_or(OpaqueValueError::FinalResultMissing)?;
        self.remove_host_bytes(result.host_owned_bytes)?;
        match result.value {
            OpaqueValue::ConvexJson(value) => Ok(value),
            OpaqueValue::Bytes(_)
            | OpaqueValue::QueryCursor(_)
            | OpaqueValue::CapabilityRequest(_) => {
                unreachable!("final result accepted a non-JSON value")
            },
        }
    }

    pub(crate) fn finish_invocation(
        &mut self,
    ) -> Result<HostOwnedByteAccounting, OpaqueValueError> {
        let value_count = self.live_handle_count + usize::from(self.final_result.is_some());
        let host_owned_bytes = self.accounting.current_bytes;
        if value_count > 0 {
            self.cleanup();
            return Err(OpaqueValueError::LeakedValues {
                value_count,
                host_owned_bytes,
            });
        }
        let accounting = self.observed_accounting();
        self.accounting = HostOwnedByteAccounting::default();
        let _ = self.notify_observer();
        Ok(accounting)
    }

    pub(crate) fn finish(mut self) -> Result<HostOwnedByteAccounting, OpaqueValueError> {
        self.finish_invocation()
    }

    pub(crate) fn cleanup(&mut self) -> CleanupReport {
        let released_value_count =
            self.live_handle_count + usize::from(self.final_result.is_some());
        let released_host_owned_bytes = self.accounting.current_bytes;
        for slot in &mut self.slots {
            slot.value = None;
        }
        self.reusable_slots.clear();
        self.reusable_slots.extend(
            self.slots
                .iter()
                .enumerate()
                .filter(|(_, slot)| !slot.retired && slot.generation < u32::MAX)
                .map(|(slot_index, _)| slot_index),
        );
        self.final_result = None;
        self.live_handle_count = 0;
        self.accounting.current_bytes = 0;
        let _ = self.notify_observer();
        CleanupReport {
            released_value_count,
            released_host_owned_bytes,
        }
    }

    fn allocate_slot(&mut self) -> Result<usize, OpaqueValueError> {
        while let Some(slot_index) = self.reusable_slots.pop() {
            let slot = &mut self.slots[slot_index];
            if slot.generation == u32::MAX {
                slot.retired = true;
                continue;
            }
            return Ok(slot_index);
        }
        if self.slots.len() >= MAX_ENCODED_SLOTS {
            return Err(OpaqueValueError::HandleSpaceExhausted);
        }
        let slot_index = self.slots.len();
        self.slots.push(HandleSlot::default());
        Ok(slot_index)
    }

    fn entry(
        &self,
        handle: OpaqueHandle,
        expected_kind: OpaqueValueKind,
    ) -> Result<&AccountedValue, OpaqueValueError> {
        let parts = handle.parts()?;
        if parts.encoded_kind != expected_kind {
            return Err(OpaqueValueError::KindMismatch {
                expected: expected_kind,
                actual: parts.encoded_kind,
            });
        }
        let slot = self
            .slots
            .get(parts.slot_index)
            .ok_or(OpaqueValueError::InvalidHandle)?;
        if slot.retired || slot.generation != parts.generation {
            return Err(OpaqueValueError::InvalidHandle);
        }
        let value = slot.value.as_ref().ok_or(OpaqueValueError::InvalidHandle)?;
        let actual = value.value.kind();
        if actual != expected_kind {
            return Err(OpaqueValueError::KindMismatch {
                expected: expected_kind,
                actual,
            });
        }
        Ok(value)
    }

    fn take_accounted(
        &mut self,
        handle: OpaqueHandle,
        expected_kind: OpaqueValueKind,
    ) -> Result<AccountedValue, OpaqueValueError> {
        let parts = handle.parts()?;
        self.entry(handle, expected_kind)?;
        let slot = &mut self.slots[parts.slot_index];
        let value = slot.value.take().ok_or(OpaqueValueError::InvalidHandle)?;
        self.live_handle_count -= 1;
        if slot.generation < u32::MAX {
            self.reusable_slots.push(parts.slot_index);
        } else {
            slot.retired = true;
        }
        Ok(value)
    }

    /// Builder limit errors are terminal invocation errors. Mutation happens
    /// before the exact post-allocation charge is known, and the invocation
    /// table is then dropped in full rather than exposing a partially moved
    /// child handle back to the guest.
    fn finish_child_move(
        &mut self,
        old_container_bytes: usize,
        child_bytes: usize,
        new_container_bytes: usize,
    ) -> Result<(), OpaqueValueError> {
        let resulting_bytes = self
            .accounting
            .current_bytes
            .checked_sub(old_container_bytes)
            .and_then(|bytes| bytes.checked_sub(child_bytes))
            .and_then(|bytes| bytes.checked_add(new_container_bytes))
            .ok_or(OpaqueValueError::HostOwnedBytesLimitExceeded {
                maximum_bytes: self.maximum_host_owned_bytes,
            })?;
        if !self.observer_accepts_current_bytes(resulting_bytes) {
            return Err(OpaqueValueError::AggregateMemoryLimitExceeded);
        }
        self.set_current_host_bytes(resulting_bytes)?;
        if resulting_bytes <= self.maximum_host_owned_bytes {
            Ok(())
        } else {
            Err(OpaqueValueError::HostOwnedBytesLimitExceeded {
                maximum_bytes: self.maximum_host_owned_bytes,
            })
        }
    }

    fn reserve_host_bytes(&self, additional_bytes: usize) -> Result<(), OpaqueValueError> {
        let Some(resulting_bytes) = self.accounting.current_bytes.checked_add(additional_bytes)
        else {
            return Err(OpaqueValueError::HostOwnedBytesLimitExceeded {
                maximum_bytes: self.maximum_host_owned_bytes,
            });
        };
        if resulting_bytes > self.maximum_host_owned_bytes {
            return Err(OpaqueValueError::HostOwnedBytesLimitExceeded {
                maximum_bytes: self.maximum_host_owned_bytes,
            });
        }
        if !self.observer_accepts_current_bytes(resulting_bytes) {
            return Err(OpaqueValueError::AggregateMemoryLimitExceeded);
        }
        Ok(())
    }

    fn add_host_bytes(&mut self, bytes: usize) -> Result<(), OpaqueValueError> {
        let current = self
            .accounting
            .current_bytes
            .checked_add(bytes)
            .expect("reserved opaque host bytes overflowed");
        self.set_current_host_bytes(current)
    }

    fn remove_host_bytes(&mut self, bytes: usize) -> Result<(), OpaqueValueError> {
        let current = self
            .accounting
            .current_bytes
            .checked_sub(bytes)
            .expect("opaque host byte accounting underflow");
        self.set_current_host_bytes(current)
    }

    fn set_current_host_bytes(&mut self, current_bytes: usize) -> Result<(), OpaqueValueError> {
        self.accounting.current_bytes = current_bytes;
        self.accounting.peak_bytes = self.accounting.peak_bytes.max(current_bytes);
        if !self.notify_observer() {
            return Err(OpaqueValueError::AggregateMemoryLimitExceeded);
        }
        Ok(())
    }

    fn notify_observer(&self) -> bool {
        self.observer
            .as_ref()
            .is_none_or(|observer| observer.host_owned_bytes_changed(self.observed_accounting()))
    }

    fn observed_accounting(&self) -> HostOwnedByteAccounting {
        HostOwnedByteAccounting {
            retained_bytes: size_of::<Self>()
                .saturating_add(
                    self.slots
                        .capacity()
                        .saturating_mul(size_of::<HandleSlot>()),
                )
                .saturating_add(
                    self.reusable_slots
                        .capacity()
                        .saturating_mul(size_of::<usize>()),
                ),
            ..self.accounting
        }
    }

    fn observer_accepts_current_bytes(&self, current_bytes: usize) -> bool {
        self.observer.as_ref().is_none_or(|observer| {
            observer.host_owned_bytes_changing(HostOwnedByteAccounting {
                current_bytes,
                peak_bytes: self.accounting.peak_bytes.max(current_bytes),
                retained_bytes: self.observed_accounting().retained_bytes,
            })
        })
    }
}

impl Drop for OpaqueValueTable {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn validate_field_name(field_name: &str) -> Result<(), OpaqueValueError> {
    if field_name.is_empty()
        || field_name.len() > MAX_FIELD_NAME_BYTES
        || field_name.chars().any(char::is_control)
    {
        return Err(OpaqueValueError::InvalidFieldName);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Mutex,
    };

    use serde_json::json;

    use super::*;

    #[derive(Default)]
    struct RecordingObserver(Mutex<Vec<HostOwnedByteAccounting>>);

    impl HostOwnedByteObserver for RecordingObserver {
        fn host_owned_bytes_changed(&self, accounting: HostOwnedByteAccounting) -> bool {
            self.0
                .lock()
                .expect("observer mutex poisoned")
                .push(accounting);
            true
        }
    }

    struct RejectingObserver;

    impl HostOwnedByteObserver for RejectingObserver {
        fn host_owned_bytes_changing(&self, _accounting: HostOwnedByteAccounting) -> bool {
            false
        }

        fn host_owned_bytes_changed(&self, _accounting: HostOwnedByteAccounting) -> bool {
            false
        }
    }

    struct RejectingPostChangeObserver;

    impl HostOwnedByteObserver for RejectingPostChangeObserver {
        fn host_owned_bytes_changing(&self, _accounting: HostOwnedByteAccounting) -> bool {
            true
        }

        fn host_owned_bytes_changed(&self, _accounting: HostOwnedByteAccounting) -> bool {
            false
        }
    }

    #[derive(Default)]
    struct SwitchableObserver(AtomicBool);

    impl HostOwnedByteObserver for SwitchableObserver {
        fn host_owned_bytes_changing(&self, _accounting: HostOwnedByteAccounting) -> bool {
            !self.0.load(Ordering::Acquire)
        }

        fn host_owned_bytes_changed(&self, _accounting: HostOwnedByteAccounting) -> bool {
            !self.0.load(Ordering::Acquire)
        }
    }

    #[test]
    fn observer_tracks_retained_handle_capacity() -> anyhow::Result<()> {
        let observer = Arc::new(RecordingObserver::default());
        let observer_trait: Arc<dyn HostOwnedByteObserver> = observer.clone();
        let mut table = OpaqueValueTable::new_with_observer(4, 1 << 20, Some(observer_trait));
        let initial_retained_bytes = table.accounting().retained_bytes;
        let handle = table.insert_null()?;
        assert!(table.accounting().retained_bytes > initial_retained_bytes);
        table.release(handle, OpaqueValueKind::ConvexJson)?;
        table.finish()?;
        assert!(observer
            .0
            .lock()
            .expect("observer mutex poisoned")
            .iter()
            .any(|accounting| accounting.retained_bytes > initial_retained_bytes));
        Ok(())
    }

    #[test]
    fn observer_can_deny_host_owned_allocation_before_state_change() {
        let observer: Arc<dyn HostOwnedByteObserver> = Arc::new(RejectingObserver);
        let mut table = OpaqueValueTable::new_with_observer(4, 1 << 20, Some(observer));
        assert_eq!(
            table.insert_null(),
            Err(OpaqueValueError::AggregateMemoryLimitExceeded)
        );
        assert_eq!(table.live_handle_count(), 0);
        assert_eq!(table.accounting().current_bytes, 0);
    }

    #[test]
    fn post_allocation_capacity_rejection_is_terminal_and_cleanable() {
        let observer: Arc<dyn HostOwnedByteObserver> = Arc::new(RejectingPostChangeObserver);
        let mut table = OpaqueValueTable::new_with_observer(4, 1 << 20, Some(observer));
        assert_eq!(
            table.insert_null(),
            Err(OpaqueValueError::AggregateMemoryLimitExceeded)
        );
        assert_eq!(table.live_handle_count(), 1);
        assert!(table.accounting().current_bytes > 0);
        assert_eq!(table.cleanup().released_value_count, 1);
        assert_eq!(table.live_handle_count(), 0);
        assert_eq!(table.accounting().current_bytes, 0);
    }

    #[test]
    fn transferred_operand_reports_late_aggregate_memory_rejection() -> anyhow::Result<()> {
        let observer = Arc::new(SwitchableObserver::default());
        let observer_trait: Arc<dyn HostOwnedByteObserver> = observer.clone();
        let mut table = OpaqueValueTable::new_with_observer(4, 1 << 20, Some(observer_trait));
        let handle = table.insert_json(json!({"request": true}))?;
        observer.0.store(true, Ordering::Release);

        assert_eq!(
            table.take_transferred_operand(handle),
            Err(OpaqueValueError::AggregateMemoryLimitExceeded)
        );
        assert_eq!(table.live_handle_count(), 0);
        assert!(table.accounting().current_bytes > 0);
        Ok(())
    }

    #[test]
    fn released_handle_cannot_alias_reused_slot() -> anyhow::Result<()> {
        let mut table = OpaqueValueTable::new(2, 1 << 20);
        let first = table.insert_string("first".to_owned())?;
        table.release(first, OpaqueValueKind::ConvexJson)?;
        let second = table.insert_string("second".to_owned())?;

        assert_ne!(first, second);
        assert_eq!(table.get_json(first), Err(OpaqueValueError::InvalidHandle));
        assert_eq!(table.get_json(second), Ok(&json!("second")));
        table.release(second, OpaqueValueKind::ConvexJson)?;
        table.finish()?;
        Ok(())
    }

    #[test]
    fn transferred_value_retains_invocation_charge_until_reset() -> anyhow::Result<()> {
        let value_bytes =
            OpaqueValue::ConvexJson(JsonValue::String(String::new())).host_owned_bytes();
        let mut table = OpaqueValueTable::new(2, value_bytes);
        let handle = table.insert_string(String::new())?;
        let charged = table.accounting().current_bytes;
        assert_eq!(charged, value_bytes);

        assert_eq!(
            table.take_transferred(handle, OpaqueValueKind::ConvexJson)?,
            OpaqueValue::ConvexJson(JsonValue::String(String::new()))
        );
        assert_eq!(table.live_handle_count(), 0);
        assert_eq!(table.get_json(handle), Err(OpaqueValueError::InvalidHandle));
        assert_eq!(table.accounting().current_bytes, charged);
        assert_eq!(
            table.insert_null(),
            Err(OpaqueValueError::HostOwnedBytesLimitExceeded {
                maximum_bytes: value_bytes,
            })
        );

        let finished = table.finish_invocation()?;
        assert_eq!(finished.current_bytes, charged);
        assert_eq!(table.accounting().current_bytes, 0);

        let cleanup_handle = table.insert_string(String::new())?;
        table.take_transferred(cleanup_handle, OpaqueValueKind::ConvexJson)?;
        assert_eq!(
            table.cleanup(),
            CleanupReport {
                released_value_count: 0,
                released_host_owned_bytes: charged,
            }
        );
        assert_eq!(table.accounting().current_bytes, 0);
        let after_cleanup = table.insert_string(String::new())?;
        table.release(after_cleanup, OpaqueValueKind::ConvexJson)?;
        table.finish()?;
        Ok(())
    }

    #[test]
    fn transferred_database_request_charges_fit_a_high_fanout_batch() -> anyhow::Result<()> {
        fn transfer_request(
            table: &mut OpaqueValueTable,
            request: JsonValue,
        ) -> anyhow::Result<()> {
            let heap_size = request.heap_size();
            let request = decode_guest_capability_request(request)?;
            let handle = table.insert(OpaqueValue::CapabilityRequest(
                GuestCapabilityRequestEnvelope { request, heap_size },
            ))?;
            table.take_transferred(handle, OpaqueValueKind::CapabilityRequest)?;
            Ok(())
        }

        let maximum_host_owned_bytes = 16 * 1024 * 1024;
        let mut table = OpaqueValueTable::new(1_024, maximum_host_owned_bytes);
        for index in 0..100 {
            let id = format!("document-{index:023}");
            for request in [
                json!({
                    "version": 4,
                    "kind": "dbGet",
                    "table": "applicationRecords",
                    "id": id.clone(),
                }),
                json!({
                    "version": 4,
                    "kind": "dbPatch",
                    "table": "applicationRecords",
                    "id": id.clone(),
                    "patch": { "retainedReference": null },
                }),
                json!({
                    "version": 4,
                    "kind": "dbDelete",
                    "table": "applicationPayloads",
                    "id": id,
                }),
            ] {
                transfer_request(&mut table, request)?;
            }
        }

        let accounting = table.accounting();
        assert!(accounting.current_bytes < 1024 * 1024, "{accounting:?}");
        assert_eq!(accounting.current_bytes, accounting.peak_bytes);
        table.finish_invocation()?;
        Ok(())
    }

    #[test]
    fn stale_handle_cannot_alias_a_value_in_the_next_invocation() -> anyhow::Result<()> {
        let mut table = OpaqueValueTable::new(2, 1 << 20);
        let first_invocation = table.insert_string("first".to_owned())?;
        table.release(first_invocation, OpaqueValueKind::ConvexJson)?;
        table.finish_invocation()?;

        let second_invocation = table.insert_string("second".to_owned())?;
        assert_ne!(first_invocation, second_invocation);
        assert_eq!(
            table.get_json(first_invocation),
            Err(OpaqueValueError::InvalidHandle)
        );
        assert_eq!(table.get_json(second_invocation), Ok(&json!("second")));
        table.release(second_invocation, OpaqueValueKind::ConvexJson)?;
        table.finish_invocation()?;
        Ok(())
    }

    #[test]
    fn validates_kind_before_access_or_consumption() -> anyhow::Result<()> {
        let mut table = OpaqueValueTable::new(2, 1 << 20);
        let bytes = table.insert(OpaqueValue::Bytes(vec![1, 2, 3]))?;
        assert!(u64::try_from(bytes.to_abi())? <= MAX_EXACT_JAVASCRIPT_INTEGER);
        assert_eq!(OpaqueHandle::from_abi(bytes.to_abi())?, bytes);
        assert!(matches!(
            table.get_json(bytes),
            Err(OpaqueValueError::KindMismatch {
                expected: OpaqueValueKind::ConvexJson,
                actual: OpaqueValueKind::Bytes,
            })
        ));
        table.release(bytes, OpaqueValueKind::Bytes)?;
        Ok(())
    }

    #[test]
    fn maximum_handle_round_trips_through_an_exact_javascript_number() -> anyhow::Result<()> {
        let handle = OpaqueHandle::new(
            MAX_ENCODED_SLOTS - 1,
            u32::MAX,
            OpaqueValueKind::QueryCursor,
        )?;
        assert!(u64::try_from(handle.to_abi())? <= MAX_EXACT_JAVASCRIPT_INTEGER);
        assert_eq!(OpaqueHandle::from_abi(handle.to_abi())?, handle);
        Ok(())
    }

    #[test]
    fn builds_values_and_accesses_request_and_document_fields_by_name() -> anyhow::Result<()> {
        let mut table = OpaqueValueTable::new(16, 1 << 20);
        let request = table.insert_json(json!({
            "lookupKey": "lookup-value",
            "document": {"isActive": true, "_id": "document-id"},
            "ключ": "utf8-field",
        }))?;
        let lookup_key = table
            .clone_object_field(request, "lookupKey")?
            .expect("lookupKey field missing");
        let document = table
            .clone_object_field(request, "document")?
            .expect("document field missing");
        let active = table
            .clone_object_field(document, "isActive")?
            .expect("isActive field missing");
        assert_eq!(table.get_json(lookup_key), Ok(&json!("lookup-value")));
        assert_eq!(table.get_json(active), Ok(&json!(true)));
        let utf8 = table
            .clone_object_field(request, "ключ")?
            .expect("UTF-8 field missing");
        assert_eq!(table.string_value(utf8)?, "utf8-field");
        assert!(table.clone_object_field(document, "absent")?.is_none());

        let output = table.insert_object()?;
        table.object_insert(output, "lookupKey".to_owned(), lookup_key)?;
        let items = table.insert_array()?;
        let item = table.insert_object()?;
        let id = table
            .clone_object_field(document, "_id")?
            .expect("_id field missing");
        table.object_insert(item, "_id".to_owned(), id)?;
        table.array_push(items, item)?;
        table.object_insert(output, "items".to_owned(), items)?;
        table.set_final_result(output)?;
        assert_eq!(
            table.take_final_result()?,
            json!({
                "lookupKey": "lookup-value",
                "items": [{"_id": "document-id"}],
            })
        );

        for handle in [request, document, active, utf8] {
            table.release(handle, OpaqueValueKind::ConvexJson)?;
        }
        table.finish()?;
        Ok(())
    }

    #[test]
    fn final_result_is_single_consume_and_cleanup_is_total() -> anyhow::Result<()> {
        let observer = Arc::new(RecordingObserver::default());
        let observer_trait: Arc<dyn HostOwnedByteObserver> = observer.clone();
        let mut table = OpaqueValueTable::new_with_observer(4, 1 << 20, Some(observer_trait));
        let first = table.insert_json(json!({"ok": true}))?;
        let leaked = table.insert_string("leaked".to_owned())?;
        table.set_final_result(first)?;
        assert_eq!(
            table.set_final_result(leaked),
            Err(OpaqueValueError::FinalResultAlreadySet)
        );
        assert_eq!(table.take_final_result()?, json!({"ok": true}));
        let before_cleanup = table.accounting().current_bytes;
        assert!(before_cleanup > 0);
        assert_eq!(
            table.cleanup(),
            CleanupReport {
                released_value_count: 1,
                released_host_owned_bytes: before_cleanup,
            }
        );
        assert_eq!(table.accounting().current_bytes, 0);
        assert!(observer
            .0
            .lock()
            .expect("observer mutex poisoned")
            .iter()
            .any(|accounting| accounting.current_bytes == 0));
        Ok(())
    }

    #[test]
    fn enforces_handle_and_host_owned_byte_limits_without_state_change() -> anyhow::Result<()> {
        let empty_string_bytes =
            OpaqueValue::ConvexJson(JsonValue::String(String::new())).host_owned_bytes();
        let mut handles = OpaqueValueTable::new(1, 1 << 20);
        let first = handles.insert_string(String::new())?;
        assert_eq!(
            handles.insert_null(),
            Err(OpaqueValueError::HandleLimitExceeded { maximum: 1 })
        );
        assert_eq!(handles.live_handle_count(), 1);
        handles.release(first, OpaqueValueKind::ConvexJson)?;

        let mut bytes = OpaqueValueTable::new(2, empty_string_bytes);
        let within_limit = bytes.insert_string(String::new())?;
        let accounting = bytes.accounting();
        assert_eq!(
            bytes.insert_string("x".to_owned()),
            Err(OpaqueValueError::HostOwnedBytesLimitExceeded {
                maximum_bytes: empty_string_bytes,
            })
        );
        assert_eq!(bytes.accounting(), accounting);
        bytes.release(within_limit, OpaqueValueKind::ConvexJson)?;
        Ok(())
    }

    #[test]
    fn exposes_generic_json_primitives_and_rejects_non_finite_numbers() -> anyhow::Result<()> {
        let mut table = OpaqueValueTable::new(4, 1 << 20);
        let boolean = table.insert_bool(true)?;
        let integer = table.insert_i64(42)?;
        let number = table.insert_f64(1.5)?;
        assert_eq!(table.json_kind(boolean)?, OpaqueJsonKind::Boolean);
        assert!(table.bool_value(boolean)?);
        assert_eq!(table.number_value(integer)?, 42.0);
        assert_eq!(table.number_value(number)?, 1.5);
        assert_eq!(
            table.insert_f64(f64::NAN),
            Err(OpaqueValueError::NonFiniteNumber)
        );
        for handle in [boolean, integer, number] {
            table.release(handle, OpaqueValueKind::ConvexJson)?;
        }
        Ok(())
    }

    #[test]
    fn guest_native_codec_round_trips_full_convex_json_and_rejects_bad_wire_values(
    ) -> anyhow::Result<()> {
        let value = json!({
            "array": [null, true, "text", {"nested": 1.5}],
            "bytes": {"$bytes": "AQID"},
            "integer": {"$integer": "KgAAAAAAAAA="},
            "nan": ConvexValue::from(f64::NAN).to_internal_json(),
            "negativeZero": ConvexValue::from(-0.0).to_internal_json(),
        });
        let encoded = GuestNativeValueCodec::encode(value.clone(), 1 << 20)?;
        assert_eq!(GuestNativeValueCodec::decode(&encoded, 1 << 20)?, value);

        let with_whitespace = [b" ".as_slice(), encoded.as_slice()].concat();
        assert!(matches!(
            GuestNativeValueCodec::decode(&with_whitespace, 1 << 20),
            Err(GuestNativeValueError::NonCanonical)
        ));
        assert!(matches!(
            GuestNativeValueCodec::decode(br#"{"$float":"AAAAAAAA8D8="}"#, 1 << 20),
            Err(GuestNativeValueError::InvalidConvexValue)
        ));
        assert!(matches!(
            GuestNativeValueCodec::decode(&encoded, encoded.len() - 1),
            Err(GuestNativeValueError::TooLarge { .. })
        ));
        assert!(matches!(
            GuestNativeValueCodec::decode(b"{", 1 << 20),
            Err(GuestNativeValueError::Malformed)
        ));
        assert!(matches!(
            GuestNativeValueCodec::decode(&[0xff], 1 << 20),
            Err(GuestNativeValueError::Malformed)
        ));
        Ok(())
    }

    #[test]
    fn guest_native_codec_accepts_json_stringify_integral_numbers() -> anyhow::Result<()> {
        let integral_result = json!({
            "result": {"timestamp": 1_720_000_000_000_u64},
            "version": 1,
        });
        assert_eq!(
            GuestNativeValueCodec::encode(integral_result, 1 << 20)?,
            br#"{"result":{"timestamp":1720000000000},"version":1}"#
        );
        Ok(())
    }

    #[test]
    fn guest_native_pending_codec_is_explicit_and_canonical() -> anyhow::Result<()> {
        let pending = json!({
            "array": [{ "$commitTs": null }],
            "nested": { "commitTs": { "$commitTs": null } },
        });
        let encoded = GuestNativeValueCodec::encode_pending(pending.clone(), 1 << 20)?;
        assert_eq!(
            encoded,
            br#"{"array":[{"$commitTs":null}],"nested":{"commitTs":{"$commitTs":null}}}"#
        );
        assert_eq!(
            GuestNativeValueCodec::decode_pending(&encoded, 1 << 20)?,
            pending
        );
        assert!(matches!(
            GuestNativeValueCodec::encode(pending.clone(), 1 << 20),
            Err(GuestNativeValueError::InvalidConvexValue)
        ));
        assert!(matches!(
            GuestNativeValueCodec::decode(&encoded, 1 << 20),
            Err(GuestNativeValueError::InvalidConvexValue)
        ));
        for invalid in [
            json!({ "$commitTs": 1 }),
            json!({ "$commitTs": null, "extra": true }),
            json!({ "$undefined": null }),
        ] {
            assert!(matches!(
                GuestNativeValueCodec::encode_pending(invalid, 1 << 20),
                Err(GuestNativeValueError::InvalidConvexValue)
            ));
        }
        Ok(())
    }

    #[test]
    fn capability_request_v4_allows_pending_values_only_at_canonical_transaction_positions() {
        let commit_ts = json!({ "$commitTs": null });
        for request in [
            json!({
                "version": 4,
                "kind": "dbInsert",
                "table": "documents",
                "value": { "commitTs": commit_ts.clone() },
            }),
            json!({
                "version": 4,
                "kind": "dbPatch",
                "table": "documents",
                "id": "document-id",
                "patch": {
                    "commitTs": commit_ts.clone(),
                    "nested": { "commitTs": commit_ts.clone() },
                    "removed": { "$undefined": null },
                },
            }),
            json!({
                "version": 4,
                "kind": "dbReplace",
                "table": "documents",
                "id": "document-id",
                "value": { "commitTs": [commit_ts.clone()] },
            }),
            json!({
                "version": 4,
                "kind": "dbQuery",
                "table": "documents",
                "source": {
                    "type": "indexRange",
                    "index": "by_commit_ts",
                    "constraints": [
                        { "operator": "eq", "field": "commitTs", "value": commit_ts.clone() },
                    ],
                },
                "operators": [],
                "order": null,
                "terminal": "collect",
            }),
            json!({
                "version": 4,
                "kind": "runUdf",
                "udfType": "query",
                "functionAddress": { "name": "tasks:readWithTimestamp" },
                "args": { "commitTs": commit_ts.clone() },
                "transactionLimits": { "documentsRead": 4 },
            }),
            json!({
                "version": 4,
                "kind": "runUdf",
                "udfType": "mutation",
                "functionAddress": { "reference": "tasks:enqueueWithTimestamp" },
                "args": { "commitTs": commit_ts.clone() },
                "transactionLimits": { "documentsWritten": 2 },
            }),
        ] {
            assert!(GuestCapabilityRequestCodec::decode_value(request).is_ok());
        }

        for request in [
            json!({
                "version": 4,
                "kind": "dbGet",
                "table": "documents",
                "id": commit_ts.clone(),
            }),
            json!({
                "version": 4,
                "kind": "dbQuery",
                "table": "documents",
                "source": { "type": "fullTableScan" },
                "operators": [{
                    "type": "filter",
                    "expression": { "$eq": [
                        { "$field": "commitTs" },
                        { "$literal": commit_ts.clone() },
                    ] },
                }],
                "order": null,
                "terminal": "collect",
            }),
            json!({
                "version": 4,
                "kind": "schedulerRunAfter",
                "delayMilliseconds": 0,
                "functionAddress": { "name": "tasks:run" },
                "args": { "commitTs": commit_ts.clone() },
            }),
            json!({
                "version": 4,
                "kind": "schedulerCancel",
                "id": commit_ts.clone(),
            }),
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(request),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }

        for version in [1, 2, 3] {
            for request in [
                json!({
                    "version": version,
                    "kind": "dbInsert",
                    "table": "documents",
                    "value": { "commitTs": commit_ts.clone() },
                }),
                json!({
                    "version": version,
                    "kind": "dbQuery",
                    "table": "documents",
                    "source": {
                        "type": "indexRange",
                        "index": "by_commit_ts",
                        "constraints": [
                            { "operator": "eq", "field": "commitTs", "value": commit_ts.clone() },
                        ],
                    },
                    "operators": [],
                    "order": null,
                    "terminal": "collect",
                }),
            ] {
                assert_eq!(
                    GuestCapabilityRequestCodec::decode_value(request),
                    Err(GuestCapabilityRequestError::InvalidRequest)
                );
            }
        }

        for version in [2, 3] {
            let request = json!({
                "version": version,
                "kind": "runUdf",
                "udfType": "mutation",
                "functionAddress": { "name": "tasks:enqueue" },
                "args": { "commitTs": commit_ts.clone() },
                "transactionLimits": null,
            });
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(request),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
    }

    #[test]
    fn capability_request_v4_decodes_closed_search_query_sources() {
        let request = json!({
            "version": 4,
            "kind": "dbQuery",
            "table": "documents",
            "source": {
                "type": "search",
                "index": "by_content",
                "filters": [
                    { "type": "search", "field": "body", "value": "needle phrase" },
                    { "type": "eq", "field": "tenant", "value": "tenant-a" },
                    { "type": "eq", "field": "category", "value": { "$undefined": null } },
                ],
            },
            "operators": [{ "type": "limit", "limit": 4 }],
            "order": null,
            "terminal": "collect",
        });
        assert_eq!(
            GuestCapabilityRequestCodec::decode_value(request.clone()),
            Ok(GuestCapabilityRequest::DbQuery {
                table: "documents".to_owned(),
                source: GuestCapabilityQuerySource::Search {
                    index: "by_content".to_owned(),
                    filters: vec![
                        GuestCapabilitySearchFilter::Search {
                            field: "body".to_owned(),
                            value: "needle phrase".to_owned(),
                        },
                        GuestCapabilitySearchFilter::Eq {
                            field: "tenant".to_owned(),
                            value: json!("tenant-a"),
                        },
                        GuestCapabilitySearchFilter::Eq {
                            field: "category".to_owned(),
                            value: json!({ "$undefined": null }),
                        },
                    ],
                },
                operators: vec![GuestCapabilityQueryOperator::Limit { limit: 4 }],
                order: GuestCapabilityQueryOrder::Default,
                terminal: GuestCapabilityQueryTerminal::Collect,
            })
        );

        let invalid_filters = [
            json!([]),
            json!([{ "type": "eq", "field": "tenant", "value": "tenant-a" }]),
            json!([
                { "type": "search", "field": "body", "value": "first" },
                { "type": "search", "field": "body", "value": "second" },
            ]),
            json!([{ "type": "search", "field": "body", "value": 1 }]),
            json!([
                { "type": "search", "field": "body", "value": "needle" },
                {
                    "type": "eq",
                    "field": "commitTs",
                    "value": { "$commitTs": null },
                },
            ]),
            json!([{ "type": "search", "field": "body", "value": "needle", "extra": true }]),
        ];
        for filters in invalid_filters {
            let mut invalid = request.clone();
            invalid["source"]["filters"] = filters;
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(invalid),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }

        let mut ordered = request.clone();
        ordered["order"] = json!("asc");
        assert_eq!(
            GuestCapabilityRequestCodec::decode_value(ordered),
            Err(GuestCapabilityRequestError::InvalidRequest)
        );
        let mut legacy = request;
        legacy["version"] = json!(3);
        assert_eq!(
            GuestCapabilityRequestCodec::decode_value(legacy),
            Err(GuestCapabilityRequestError::InvalidRequest)
        );
    }

    #[test]
    fn capability_request_v2_through_v4_reject_invalid_nested_udf_contracts() {
        for version in [2, 3, 4] {
            for request in [
                json!({
                    "version": version,
                    "kind": "runUdf",
                    "udfType": "action",
                    "functionAddress": { "name": "tasks:run" },
                    "args": {},
                    "transactionLimits": null,
                }),
                json!({
                    "version": version,
                    "kind": "runUdf",
                    "udfType": "query",
                    "functionAddress": { "name": "tasks:run" },
                    "args": 1,
                    "transactionLimits": null,
                }),
                json!({
                    "version": version,
                    "kind": "runUdf",
                    "udfType": "mutation",
                    "functionAddress": { "name": "tasks:run" },
                    "args": {},
                    "transactionLimits": { "unknown": 1 },
                }),
                json!({
                    "version": version,
                    "kind": "runUdf",
                    "udfType": "query",
                    "functionAddress": { "name": "tasks:run" },
                    "args": {},
                    "transactionLimits": { "documentsRead": -1 },
                }),
                json!({
                    "version": version,
                    "kind": "runUdf",
                    "udfType": "query",
                    "functionAddress": { "name": "tasks:run" },
                    "args": {},
                    "transactionLimits": { "documentsRead": 1.5 },
                }),
            ] {
                assert_eq!(
                    GuestCapabilityRequestCodec::decode_value(request),
                    Err(GuestCapabilityRequestError::InvalidRequest)
                );
            }
        }
    }

    #[test]
    fn capability_request_codec_v4_accepts_only_gets_without_static_tables() {
        for (kind, expected) in [
            (
                "dbGet",
                GuestCapabilityRequest::DbGet {
                    table: None,
                    id: json!("document-id"),
                },
            ),
            (
                "dbSystemGet",
                GuestCapabilityRequest::DbSystemGet {
                    table: None,
                    id: json!("document-id"),
                },
            ),
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(json!({
                    "version": 4,
                    "kind": kind,
                    "id": "document-id",
                })),
                Ok(expected)
            );
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(json!({
                    "version": 4,
                    "kind": kind,
                    "table": null,
                    "id": "document-id",
                })),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
            for version in [1, 2, 3] {
                assert_eq!(
                    GuestCapabilityRequestCodec::decode_value(json!({
                        "version": version,
                        "kind": kind,
                        "id": "document-id",
                    })),
                    Err(GuestCapabilityRequestError::InvalidRequest)
                );
            }
        }

        for kind in ["dbPatch", "dbReplace", "dbDelete"] {
            let mut request = json!({
                "version": 4,
                "kind": kind,
                "id": "document-id",
            });
            if kind != "dbDelete" {
                request[if kind == "dbPatch" { "patch" } else { "value" }] = json!({});
            }
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(request),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
    }

    #[test]
    fn capability_request_codec_v4_accepts_patch_deletion_only_at_the_patch_top_level(
    ) -> anyhow::Result<()> {
        let request = br#"{"id":"document-id","kind":"dbPatch","patch":{"delete":{"$undefined":null},"keep":{"nested":1}},"table":"documents","version":4}"#;
        let decoded = GuestCapabilityRequestCodec::decode(request, 1 << 20)?.into_request();
        assert_eq!(
            decoded,
            GuestCapabilityRequest::DbPatch {
                table: "documents".to_owned(),
                id: json!("document-id"),
                patch: json!({
                    "delete": { "$undefined": null },
                    "keep": { "nested": 1.0 },
                }),
            }
        );

        for invalid in [
            br#"{"id":"document-id","kind":"dbPatch","patch":{"nested":{"child":{"$undefined":null}}},"table":"documents","version":4}"#.as_slice(),
            br#"{"kind":"dbInsert","table":"documents","value":{"$undefined":null},"version":4}"#.as_slice(),
            br#"{"kind":"dbInsert","table":"documents","value":{"$literal":"value"},"version":4}"#.as_slice(),
            br#"{"kind":"dbInsert","table":"documents","value":{"$eq":[1,2]},"version":4}"#.as_slice(),
            br#"{"kind":"dbReplace","id":"document-id","table":"documents","value":{"nested":{"$undefined":null}},"version":4}"#.as_slice(),
        ] {
            assert!(matches!(
                GuestCapabilityRequestCodec::decode(invalid, 1 << 20),
                Err(GuestCapabilityRequestError::InvalidRequest
                    | GuestCapabilityRequestError::NonCanonical)
            ));
        }
        Ok(())
    }

    #[test]
    fn capability_request_codec_v4_preserves_undefined_index_range_constraints(
    ) -> anyhow::Result<()> {
        let request = br#"{"kind":"dbQuery","operators":[],"order":null,"source":{"constraints":[{"field":"nextAttemptAt","operator":"gt","value":{"$undefined":null}},{"field":"nextAttemptAt","operator":"lte","value":123}],"index":"by_next_attempt","type":"indexRange"},"table":"documents","terminal":"collect","version":4}"#;
        let expected = GuestCapabilityRequest::DbQuery {
            table: "documents".to_owned(),
            source: GuestCapabilityQuerySource::IndexRange {
                index: "by_next_attempt".to_owned(),
                constraints: vec![
                    GuestCapabilityQueryConstraint::Gt {
                        field: "nextAttemptAt".to_owned(),
                        value: json!({ "$undefined": null }),
                    },
                    GuestCapabilityQueryConstraint::Lte {
                        field: "nextAttemptAt".to_owned(),
                        value: json!(123.0),
                    },
                ],
            },
            operators: vec![],
            order: GuestCapabilityQueryOrder::Default,
            terminal: GuestCapabilityQueryTerminal::Collect,
        };

        assert_eq!(
            GuestCapabilityRequestCodec::decode(request, 1 << 20)?.into_request(),
            expected
        );
        assert_eq!(
            GuestCapabilityRequestCodec::decode_value(serde_json::from_slice(request)?),
            Ok(expected)
        );
        Ok(())
    }

    #[test]
    fn capability_request_codec_decodes_every_canonical_guest_matrix_variant() -> anyhow::Result<()>
    {
        for (expected_kind, bytes) in [
            (
                "authGetUserIdentity",
                br#"{"kind":"authGetUserIdentity","version":1}"#.as_slice(),
            ),
            (
                "auditLog",
                br#"{"body":{"action":"document.viewed","source":{"ip":{"$var":"ip"}}},"kind":"auditLog","version":4}"#.as_slice(),
            ),
            (
                "getFunctionMetadata",
                br#"{"kind":"getFunctionMetadata","version":4}"#.as_slice(),
            ),
            (
                "getDeploymentMetadata",
                br#"{"kind":"getDeploymentMetadata","version":4}"#.as_slice(),
            ),
            (
                "getTransactionMetrics",
                br#"{"kind":"getTransactionMetrics","version":4}"#.as_slice(),
            ),
            (
                "getRequestMetadata",
                br#"{"kind":"getRequestMetadata","version":4}"#.as_slice(),
            ),
            (
                "functionHandleCreate",
                br#"{"functionAddress":{"reference":"_reference/function/tasks:run"},"kind":"functionHandleCreate","version":4}"#.as_slice(),
            ),
            (
                "performanceNow",
                br#"{"kind":"performanceNow","version":1}"#.as_slice(),
            ),
            (
                "environmentVariableGet",
                br#"{"kind":"environmentVariableGet","name":"REQUEST_ENVELOPE_TEST","version":1}"#.as_slice(),
            ),
            (
                "dbNormalizeId",
                br#"{"kind":"dbNormalizeId","table":"documents","value":"j97b7xnjnh7ty0hnr4zfmf0bf17kry8y","version":1}"#.as_slice(),
            ),
            (
                "dbGet",
                br#"{"id":"j97b7xnjnh7ty0hnr4zfmf0bf17kry8y","kind":"dbGet","table":"documents","version":1}"#.as_slice(),
            ),
            (
                "dbSystemGet",
                br#"{"id":"j97b7xnjnh7ty0hnr4zfmf0bf17kry8y","kind":"dbSystemGet","table":"_storage","version":1}"#.as_slice(),
            ),
            (
                "dbDelete",
                br#"{"id":"j97b7xnjnh7ty0hnr4zfmf0bf17kry8y","kind":"dbDelete","table":"documents","version":1}"#.as_slice(),
            ),
            (
                "dbInsert",
                br#"{"kind":"dbInsert","table":"documents","value":{"counter":{"$integer":"BAAAAAAAAAA="},"status":"ready"},"version":1}"#.as_slice(),
            ),
            (
                "dbPatch",
                br#"{"id":"j97b7xnjnh7ty0hnr4zfmf0bf17kry8y","kind":"dbPatch","patch":{"counter":{"$integer":"//////////8="},"nested":{"kept":3},"removed":{"$undefined":null},"status":"ready"},"table":"documents","version":1}"#.as_slice(),
            ),
            (
                "dbReplace",
                br#"{"id":"j97b7xnjnh7ty0hnr4zfmf0bf17kry8y","kind":"dbReplace","table":"documents","value":{"counter":{"$integer":"BAAAAAAAAAA="},"status":"ready"},"version":1}"#.as_slice(),
            ),
            (
                "dbQuery",
                br#"{"kind":"dbQuery","operators":[{"expression":{"$and":[{"$eq":[{"$field":"status"},{"$literal":"ready"}]},{"$eq":[{"$field":"sequence"},{"$literal":{"$integer":"BAAAAAAAAAA="}}]}]},"type":"filter"},{"limit":7,"type":"limit"}],"order":"asc","source":{"constraints":[{"field":"status","operator":"eq","value":"ready"},{"field":"sequence","operator":"gte","value":{"$integer":"BAAAAAAAAAA="}}],"index":"by_status_sequence","type":"indexRange"},"table":"documents","terminal":"unique","version":1}"#.as_slice(),
            ),
            (
                "dbQuery",
                br#"{"kind":"dbQuery","operators":[],"order":null,"pagination":{"cursor":null,"endCursor":"end-cursor","maximumBytesRead":4096,"maximumRowsRead":7,"pageSize":2},"source":{"type":"fullTableScan"},"table":"documents","terminal":"paginate","version":3}"#.as_slice(),
            ),
            (
                "dbQuery",
                br#"{"kind":"dbQuery","operators":[{"limit":4,"type":"limit"}],"order":null,"source":{"filters":[{"field":"body","type":"search","value":"needle phrase"},{"field":"tenant","type":"eq","value":"tenant-a"},{"field":"category","type":"eq","value":{"$undefined":null}}],"index":"by_content","type":"search"},"table":"documents","terminal":"collect","version":4}"#.as_slice(),
            ),
            (
                "storageGetUrl",
                br#"{"kind":"storageGetUrl","storageId":"kg2f3x2m7y1v6b4n8c9d0e5h3j7k1p4q","version":4}"#.as_slice(),
            ),
            (
                "storageGetMetadata",
                br#"{"kind":"storageGetMetadata","storageId":"kg2f3x2m7y1v6b4n8c9d0e5h3j7k1p4q","version":4}"#.as_slice(),
            ),
            (
                "storageGenerateUploadUrl",
                br#"{"kind":"storageGenerateUploadUrl","version":4}"#.as_slice(),
            ),
            (
                "storageDelete",
                br#"{"kind":"storageDelete","storageId":"kg2f3x2m7y1v6b4n8c9d0e5h3j7k1p4q","version":4}"#.as_slice(),
            ),
            (
                "runUdf",
                br#"{"args":{"payload":"queued","updatedAt":{"$commitTs":null}},"functionAddress":{"reference":"tasks:enqueueWithTimestamp"},"kind":"runUdf","transactionLimits":null,"udfType":"mutation","version":4}"#.as_slice(),
            ),
            (
                "runUdf",
                br#"{"args":{"tenant":"tenant-a"},"functionAddress":{"name":"tasks:read"},"kind":"runUdf","transactionLimits":{"documentsRead":8},"udfType":"query","version":2}"#.as_slice(),
            ),
            (
                "runUdf",
                br#"{"args":{},"functionAddress":{"functionHandle":"function://read-stale"},"kind":"runUdf","transactionLimits":null,"udfType":"snapshotQuery","version":2}"#.as_slice(),
            ),
            (
                "schedulerRunAfter",
                br#"{"args":{"sequence":{"$integer":"BAAAAAAAAAA="}},"delayMilliseconds":125,"functionAddress":{"name":"tasks:run"},"kind":"schedulerRunAfter","version":1}"#.as_slice(),
            ),
            (
                "schedulerRunAt",
                br#"{"args":{"sequence":{"$integer":"BAAAAAAAAAA="}},"functionAddress":{"reference":"tasks:run"},"kind":"schedulerRunAt","timestampMilliseconds":1710000000000,"version":1}"#.as_slice(),
            ),
            (
                "schedulerCancel",
                br#"{"id":"j97b7xnjnh7ty0hnr4zfmf0bf17kry8y","kind":"schedulerCancel","version":1}"#.as_slice(),
            ),
        ] {
            let request = GuestCapabilityRequestCodec::decode(bytes, 1 << 20)?.into_request();
            let actual_kind = match request {
                GuestCapabilityRequest::AuthGetUserIdentity => "authGetUserIdentity",
                GuestCapabilityRequest::AuditLog { .. } => "auditLog",
                GuestCapabilityRequest::GetFunctionMetadata => "getFunctionMetadata",
                GuestCapabilityRequest::GetDeploymentMetadata => "getDeploymentMetadata",
                GuestCapabilityRequest::GetTransactionMetrics => "getTransactionMetrics",
                GuestCapabilityRequest::GetRequestMetadata => "getRequestMetadata",
                GuestCapabilityRequest::FunctionHandleCreate { .. } => "functionHandleCreate",
                GuestCapabilityRequest::DbGet { .. } => "dbGet",
                GuestCapabilityRequest::DbSystemGet { .. } => "dbSystemGet",
                GuestCapabilityRequest::DbNormalizeId { .. } => "dbNormalizeId",
                GuestCapabilityRequest::DbInsert { .. } => "dbInsert",
                GuestCapabilityRequest::DbPatch { .. } => "dbPatch",
                GuestCapabilityRequest::DbReplace { .. } => "dbReplace",
                GuestCapabilityRequest::DbDelete { .. } => "dbDelete",
                GuestCapabilityRequest::DbQuery { .. } => "dbQuery",
                GuestCapabilityRequest::StorageGetUrl { .. } => "storageGetUrl",
                GuestCapabilityRequest::StorageGetMetadata { .. } => "storageGetMetadata",
                GuestCapabilityRequest::StorageGenerateUploadUrl => "storageGenerateUploadUrl",
                GuestCapabilityRequest::StorageDelete { .. } => "storageDelete",
                GuestCapabilityRequest::RunUdf { .. } => "runUdf",
                GuestCapabilityRequest::SchedulerRunAfter { .. } => "schedulerRunAfter",
                GuestCapabilityRequest::SchedulerRunAt { .. } => "schedulerRunAt",
                GuestCapabilityRequest::SchedulerCancel { .. } => "schedulerCancel",
                GuestCapabilityRequest::EnvironmentVariableGet { .. } => {
                    "environmentVariableGet"
                },
                GuestCapabilityRequest::PerformanceNow => "performanceNow",
            };
            assert_eq!(actual_kind, expected_kind);
        }
        Ok(())
    }

    #[test]
    fn capability_request_codec_decodes_closed_v4_audit_log_contract() {
        let body = json!({
            "action": "document.viewed",
            "actor": { "id": "user-id" },
            "source": { "ip": { "$var": "ip" } },
        });
        assert_eq!(
            GuestCapabilityRequestCodec::decode_value(json!({
                "version": 4,
                "kind": "auditLog",
                "body": body.clone(),
            })),
            Ok(GuestCapabilityRequest::AuditLog { body: body.clone() })
        );

        for version in [1, 2, 3] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(json!({
                    "version": version,
                    "kind": "auditLog",
                    "body": body.clone(),
                })),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
        for invalid in [
            json!({ "version": 4, "kind": "auditLog" }),
            json!({ "version": 4, "kind": "auditLog", "body": null }),
            json!({ "version": 4, "kind": "auditLog", "body": body, "extra": null }),
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(invalid),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
    }

    #[test]
    fn capability_request_codec_decodes_closed_v4_metadata_contracts() {
        for (kind, expected) in [
            (
                "getFunctionMetadata",
                GuestCapabilityRequest::GetFunctionMetadata,
            ),
            (
                "getDeploymentMetadata",
                GuestCapabilityRequest::GetDeploymentMetadata,
            ),
            (
                "getTransactionMetrics",
                GuestCapabilityRequest::GetTransactionMetrics,
            ),
            (
                "getRequestMetadata",
                GuestCapabilityRequest::GetRequestMetadata,
            ),
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(json!({
                    "version": 4,
                    "kind": kind,
                })),
                Ok(expected)
            );

            for version in [1, 2, 3] {
                assert_eq!(
                    GuestCapabilityRequestCodec::decode_value(json!({
                        "version": version,
                        "kind": kind,
                    })),
                    Err(GuestCapabilityRequestError::InvalidRequest)
                );
            }

            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(json!({
                    "version": 4,
                    "kind": kind,
                    "extra": null,
                })),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }

        for incomplete in [
            json!({ "version": 4 }),
            json!({ "kind": "getFunctionMetadata" }),
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(incomplete),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
    }

    #[test]
    fn capability_request_codec_decodes_closed_v4_function_handle_create_contract() {
        assert_eq!(
            GuestCapabilityRequestCodec::decode_value(json!({
                "version": 4,
                "kind": "functionHandleCreate",
                "functionAddress": {
                    "reference": "_reference/function/tasks:run",
                },
            })),
            Ok(GuestCapabilityRequest::FunctionHandleCreate {
                function_address: GuestCapabilityFunctionAddress::Reference(
                    "_reference/function/tasks:run".to_owned(),
                ),
            })
        );

        for version in [1, 2, 3] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(json!({
                    "version": version,
                    "kind": "functionHandleCreate",
                    "functionAddress": { "name": "tasks:run" },
                })),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
        for invalid in [
            json!({ "version": 4, "kind": "functionHandleCreate" }),
            json!({
                "version": 4,
                "kind": "functionHandleCreate",
                "functionAddress": { "name": "" },
            }),
            json!({
                "version": 4,
                "kind": "functionHandleCreate",
                "functionAddress": {
                    "name": "tasks:run",
                    "reference": "_reference/function/tasks:run",
                },
            }),
            json!({
                "version": 4,
                "kind": "functionHandleCreate",
                "functionAddress": { "reference": "_reference/function/tasks:run" },
                "extra": null,
            }),
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(invalid),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
    }

    #[test]
    fn capability_request_codec_decodes_closed_storage_contracts_canonically() {
        let storage_id = "kg2f3x2m7y1v6b4n8c9d0e5h3j7k1p4q";
        for (request, expected) in [
            (
                json!({
                    "version": 4,
                    "kind": "storageGetUrl",
                    "storageId": storage_id,
                }),
                GuestCapabilityRequest::StorageGetUrl {
                    storage_id: storage_id.to_owned(),
                },
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageGetMetadata",
                    "storageId": storage_id,
                }),
                GuestCapabilityRequest::StorageGetMetadata {
                    storage_id: storage_id.to_owned(),
                },
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageGenerateUploadUrl",
                }),
                GuestCapabilityRequest::StorageGenerateUploadUrl,
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageDelete",
                    "storageId": storage_id,
                }),
                GuestCapabilityRequest::StorageDelete {
                    storage_id: storage_id.to_owned(),
                },
            ),
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(request),
                Ok(expected)
            );
        }

        for version in [1, 2, 3] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(json!({
                    "version": version,
                    "kind": "storageGetUrl",
                    "storageId": storage_id,
                })),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
        for invalid in [
            json!({ "version": 4, "kind": "storageGetUrl", "storageId": 1 }),
            json!({
                "version": 4,
                "kind": "storageGetMetadata",
                "storageId": storage_id,
                "extra": null,
            }),
            json!({ "version": 4, "kind": "storageDelete", "id": storage_id }),
            json!({
                "version": 4,
                "kind": "storageGenerateUploadUrl",
                "storageId": storage_id,
            }),
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(invalid),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
        assert_eq!(
            GuestCapabilityRequestCodec::decode(
                br#"{"version":4,"kind":"storageGetUrl","storageId":"kg2f3x2m7y1v6b4n8c9d0e5h3j7k1p4q"}"#,
                1 << 20,
            ),
            Err(GuestCapabilityRequestError::NonCanonical)
        );
    }

    #[test]
    fn capability_request_codec_accepts_only_the_closed_query_expression_grammar(
    ) -> anyhow::Result<()> {
        let request = br#"{"kind":"dbQuery","operators":[{"expression":{"$and":[{"$eq":[{"$field":"tenant"},{"$literal":"tenant-a"}]},{"$not":{"$literal":false}}]},"type":"filter"},{"limit":7,"type":"limit"}],"order":"desc","source":{"constraints":[{"field":"sequence","operator":"gte","value":3}],"index":"by_sequence","type":"indexRange"},"table":"documents","terminal":"collect","version":1}"#;
        let decoded = GuestCapabilityRequestCodec::decode(request, 1 << 20)?.into_request();
        assert!(matches!(
            decoded,
            GuestCapabilityRequest::DbQuery {
                source: GuestCapabilityQuerySource::IndexRange { .. },
                operators,
                order: GuestCapabilityQueryOrder::Desc,
                terminal: GuestCapabilityQueryTerminal::Collect,
                ..
            } if operators.len() == 2
        ));

        for expression in [
            r#"{"$unknown":[]}"#,
            r#"{"$eq":[{"$literal":1}]}"#,
            r#"{"$field":"field","$literal":1}"#,
            r#"{"$literal":{"$undefined":null}}"#,
            r#"{"$literal":{"$eq":[]}}"#,
        ] {
            let request = format!(
                "{{\"kind\":\"dbQuery\",\"operators\":[{{\"expression\":{expression},\"type\":\"\
                 filter\"}}],\"order\":null,\"source\":{{\"type\":\"fullTableScan\"}},\"table\":\"\
                 documents\",\"terminal\":\"collect\",\"version\":1}}"
            );
            assert_eq!(
                GuestCapabilityRequestCodec::decode(request.as_bytes(), 1 << 20),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }

        let expression_as_value =
            br#"{"kind":"dbInsert","table":"documents","value":{"$eq":[]},"version":1}"#;
        assert_eq!(
            GuestCapabilityRequestCodec::decode(expression_as_value, 1 << 20),
            Err(GuestCapabilityRequestError::InvalidRequest)
        );
        Ok(())
    }

    #[test]
    fn capability_request_codec_accepts_only_closed_typed_query_pagination() {
        let request = json!({
            "version": 3,
            "kind": "dbQuery",
            "table": "documents",
            "source": { "type": "fullTableScan" },
            "operators": [],
            "order": null,
            "pagination": {
                "cursor": null,
                "endCursor": "end-cursor",
                "maximumBytesRead": 4096,
                "maximumRowsRead": 0,
                "pageSize": 0,
            },
            "terminal": "paginate",
        });
        assert!(matches!(
            GuestCapabilityRequestCodec::decode_value(request.clone()),
            Ok(GuestCapabilityRequest::DbQuery {
                terminal: GuestCapabilityQueryTerminal::Paginate(
                    GuestCapabilityQueryPagination {
                        cursor: None,
                        end_cursor: Some(end_cursor),
                        maximum_bytes_read: Some(4096),
                        maximum_rows_read: Some(0),
                        page_size: 0,
                    }
                ),
                ..
            }) if end_cursor == "end-cursor"
        ));
        let mut current = request.clone();
        current["version"] = json!(4);
        assert!(GuestCapabilityRequestCodec::decode_value(current).is_ok());

        for legacy_version in [1, 2] {
            let mut legacy = request.clone();
            legacy["version"] = json!(legacy_version);
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(legacy),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }

        let mut missing = request.clone();
        missing
            .as_object_mut()
            .expect("pagination request must be an object")
            .remove("pagination");
        let mut unrelated = request.clone();
        unrelated["terminal"] = json!("collect");
        let mut extra = request.clone();
        extra["pagination"]["extra"] = json!(true);
        let mut cursor = request.clone();
        cursor["pagination"]["cursor"] = json!(17);
        let mut fractional_page = request.clone();
        fractional_page["pagination"]["pageSize"] = json!(1.5);
        let mut negative_limit = request;
        negative_limit["pagination"]["maximumRowsRead"] = json!(-1);
        for invalid in [
            missing,
            unrelated,
            extra,
            cursor,
            fractional_page,
            negative_limit,
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(invalid),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
    }

    #[test]
    fn capability_request_v4_adds_only_the_typed_query_stream_terminal() {
        let request = json!({
            "version": 4,
            "kind": "dbQuery",
            "table": "documents",
            "source": {
                "type": "indexRange",
                "index": "by_tenant",
                "constraints": [
                    { "operator": "eq", "field": "tenant", "value": "tenant-a" },
                ],
            },
            "operators": [],
            "order": "desc",
            "terminal": "stream",
        });
        assert!(matches!(
            GuestCapabilityRequestCodec::decode_value(request.clone()),
            Ok(GuestCapabilityRequest::DbQuery {
                terminal: GuestCapabilityQueryTerminal::Stream,
                ..
            })
        ));

        for legacy_version in [1, 2, 3] {
            let mut legacy = request.clone();
            legacy["version"] = json!(legacy_version);
            assert_eq!(
                GuestCapabilityRequestCodec::decode_value(legacy),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }

        let mut forged_pagination = request;
        forged_pagination["pagination"] = json!({
            "cursor": null,
            "endCursor": null,
            "maximumBytesRead": null,
            "maximumRowsRead": null,
            "pageSize": 1,
        });
        assert_eq!(
            GuestCapabilityRequestCodec::decode_value(forged_pagination),
            Err(GuestCapabilityRequestError::InvalidRequest)
        );
    }

    #[test]
    fn capability_request_codec_rejects_unknown_noncanonical_and_oversized_envelopes(
    ) -> anyhow::Result<()> {
        for invalid in [
            br#"{"kind":"unknown","version":1}"#.as_slice(),
            br#"{"forged":true,"kind":"authGetUserIdentity","version":1}"#.as_slice(),
            br#"{"forged":{"$field":"status"},"kind":"authGetUserIdentity","version":1}"#
                .as_slice(),
            br#"{"kind":"dbQueryUnique","version":1}"#.as_slice(),
        ] {
            assert_eq!(
                GuestCapabilityRequestCodec::decode(invalid, 1 << 20),
                Err(GuestCapabilityRequestError::InvalidRequest)
            );
        }
        assert_eq!(
            GuestCapabilityRequestCodec::decode(
                br#"{"kind":"authGetUserIdentity","version":5}"#,
                1 << 20,
            ),
            Err(GuestCapabilityRequestError::UnsupportedVersion)
        );
        let canonical = br#"{"kind":"authGetUserIdentity","version":1}"#;
        assert!(matches!(
            GuestCapabilityRequestCodec::decode(canonical, 1 << 20)?.into_request(),
            GuestCapabilityRequest::AuthGetUserIdentity
        ));
        assert_eq!(
            GuestCapabilityRequestCodec::decode(
                br#"{"version":1,"kind":"authGetUserIdentity"}"#,
                1 << 20,
            ),
            Err(GuestCapabilityRequestError::NonCanonical)
        );
        assert!(matches!(
            GuestCapabilityRequestCodec::decode(canonical, canonical.len() - 1),
            Err(GuestCapabilityRequestError::TooLarge { .. })
        ));
        assert_eq!(
            GuestCapabilityRequestCodec::decode(b"{", 1 << 20),
            Err(GuestCapabilityRequestError::Malformed)
        );
        Ok(())
    }

    #[test]
    fn guest_native_codec_uses_json_stringify_number_canonicalization() -> anyhow::Result<()> {
        for canonical in [
            "0",
            "1",
            "-1",
            "0.000001",
            "1e-7",
            "100000000000000000000",
            "1e+21",
            "5e-324",
            "1.7976931348623157e+308",
            "9007199254740992",
        ] {
            let decoded = GuestNativeValueCodec::decode(canonical.as_bytes(), 1 << 20)?;
            assert_eq!(
                GuestNativeValueCodec::encode(decoded, 1 << 20)?,
                canonical.as_bytes(),
                "canonical number {canonical} did not round trip"
            );
        }

        for noncanonical in [
            "-0",
            "0.0",
            "1.0",
            "1e0",
            "1e-6",
            "1e21",
            "1E+21",
            "1e+020",
            "9007199254740993",
        ] {
            assert!(
                matches!(
                    GuestNativeValueCodec::decode(noncanonical.as_bytes(), 1 << 20),
                    Err(GuestNativeValueError::NonCanonical)
                ),
                "noncanonical number {noncanonical} was accepted"
            );
        }

        for malformed in ["NaN", "Infinity", "-Infinity", "1e999"] {
            assert!(
                matches!(
                    GuestNativeValueCodec::decode(malformed.as_bytes(), 1 << 20),
                    Err(GuestNativeValueError::Malformed)
                ),
                "malformed number {malformed} was accepted"
            );
        }
        Ok(())
    }

    #[test]
    fn guest_native_codec_preserves_structural_canonicality() -> anyhow::Result<()> {
        let canonical = br#"{"a":1,"b":[2,3]}"#;
        let decoded = GuestNativeValueCodec::decode(canonical, 1 << 20)?;
        assert_eq!(GuestNativeValueCodec::encode(decoded, 1 << 20)?, canonical);

        let numeric_fields = br#"{"2":"two","10":"ten","4294967294":"last index","01":"leading zero","4294967295":"not an index","a":"other"}"#;
        let decoded = GuestNativeValueCodec::decode(numeric_fields, 1 << 20)?;
        assert_eq!(
            GuestNativeValueCodec::encode(decoded, 1 << 20)?,
            numeric_fields
        );
        let request = br#"{"kind":"dbInsert","table":"documents","value":{"2":"two","10":"ten","4294967294":"last index","01":"leading zero","4294967295":"not an index","a":"other"},"version":4}"#;
        assert!(GuestCapabilityRequestCodec::decode(request, 1 << 20).is_ok());

        for noncanonical in [
            br#"{"b":[2,3],"a":1}"#.as_slice(),
            br#"{"a":1, "b":[2,3]}"#.as_slice(),
            br#"{"a":1,"a":1,"b":[2,3]}"#.as_slice(),
            br#"{"a":1,"b":[2.0,3]}"#.as_slice(),
            br#"{"10":"ten","2":"two"}"#.as_slice(),
        ] {
            assert!(matches!(
                GuestNativeValueCodec::decode(noncanonical, 1 << 20),
                Err(GuestNativeValueError::NonCanonical)
            ));
        }
        Ok(())
    }

    #[test]
    fn terminal_builder_limit_error_remains_fully_cleanable() -> anyhow::Result<()> {
        let mut table = OpaqueValueTable::new(4, 1 << 20);
        let object = table.insert_object()?;
        let child = table.insert_null()?;
        table.maximum_host_owned_bytes = table.accounting().current_bytes;
        let field = "f".repeat(MAX_FIELD_NAME_BYTES);
        assert!(matches!(
            table.object_insert(object, field, child),
            Err(OpaqueValueError::HostOwnedBytesLimitExceeded { .. })
        ));
        assert_eq!(table.live_handle_count(), 1);
        let cleanup = table.cleanup();
        assert_eq!(cleanup.released_value_count, 1);
        assert!(cleanup.released_host_owned_bytes > 0);
        assert_eq!(table.accounting().current_bytes, 0);
        Ok(())
    }

    #[test]
    fn finish_reports_leaks_after_releasing_all_owned_values() -> anyhow::Result<()> {
        let observer = Arc::new(RecordingObserver::default());
        let observer_trait: Arc<dyn HostOwnedByteObserver> = observer.clone();
        let mut table = OpaqueValueTable::new_with_observer(4, 1 << 20, Some(observer_trait));
        table.insert_string("unreleased".to_owned())?;
        let bytes = table.accounting().current_bytes;
        assert!(matches!(
            table.finish(),
            Err(OpaqueValueError::LeakedValues {
                value_count: 1,
                host_owned_bytes,
            }) if host_owned_bytes == bytes
        ));
        assert_eq!(
            observer
                .0
                .lock()
                .expect("observer mutex poisoned")
                .last()
                .expect("observer received no events")
                .current_bytes,
            0
        );
        Ok(())
    }
}
