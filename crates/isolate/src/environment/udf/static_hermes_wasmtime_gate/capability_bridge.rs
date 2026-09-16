use std::{
    num::NonZeroU64,
    sync::atomic::{
        AtomicU64,
        Ordering,
    },
};

use serde::Deserialize;
use serde_json::Value as JsonValue;

use super::super::wasm_udf_abi::{
    GuestCapabilityFunctionAddress,
    GuestCapabilityNestedUdfType,
    GuestCapabilityQueryConstraint,
    GuestCapabilityQueryOperator,
    GuestCapabilityQueryOrder,
    GuestCapabilityQueryPagination,
    GuestCapabilityQuerySource,
    GuestCapabilityQueryTerminal,
    GuestCapabilityRequest,
    GuestCapabilitySearchFilter,
};
#[cfg(test)]
use super::super::wasm_udf_abi::{
    GuestCapabilityRequestCodec,
    GuestCapabilityRequestError,
};

static NEXT_INVOCATION_CAPABILITY_IDENTITY: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InvocationCapabilityIdentity(NonZeroU64);

impl InvocationCapabilityIdentity {
    pub(super) fn from_abi(value: i64) -> Result<Self, CapabilityBridgeError> {
        NonZeroU64::new(value as u64)
            .map(Self)
            .ok_or(CapabilityBridgeError::ForgedIdentity)
    }

    pub(super) fn to_abi(self) -> i64 {
        self.0.get() as i64
    }
}

pub(super) struct InvocationCapabilityBridge {
    state: InvocationCapabilityState,
}

enum InvocationCapabilityState {
    Unissued,
    Initialization,
    Active(InvocationCapabilityIdentity),
    Revoked,
}

impl InvocationCapabilityBridge {
    pub(super) fn new() -> Result<Self, CapabilityBridgeError> {
        let mut bridge = Self::unissued();
        bridge.issue()?;
        Ok(bridge)
    }

    pub(super) fn unissued() -> Self {
        Self {
            state: InvocationCapabilityState::Unissued,
        }
    }

    pub(super) fn issue(&mut self) -> Result<(), CapabilityBridgeError> {
        match self.state {
            InvocationCapabilityState::Unissued => {
                self.state =
                    InvocationCapabilityState::Active(next_invocation_capability_identity()?);
                Ok(())
            },
            InvocationCapabilityState::Initialization | InvocationCapabilityState::Active(_) => {
                Err(CapabilityBridgeError::AlreadyActive)
            },
            InvocationCapabilityState::Revoked => Err(CapabilityBridgeError::Revoked),
        }
    }

    pub(super) fn begin_initialization(&mut self) -> Result<(), CapabilityBridgeError> {
        match self.state {
            InvocationCapabilityState::Unissued => {
                self.state = InvocationCapabilityState::Initialization;
                Ok(())
            },
            InvocationCapabilityState::Initialization | InvocationCapabilityState::Active(_) => {
                Err(CapabilityBridgeError::AlreadyActive)
            },
            InvocationCapabilityState::Revoked => Err(CapabilityBridgeError::Revoked),
        }
    }

    pub(super) fn finish_initialization(&mut self) -> Result<(), CapabilityBridgeError> {
        match self.state {
            InvocationCapabilityState::Initialization => {
                self.state = InvocationCapabilityState::Unissued;
                Ok(())
            },
            InvocationCapabilityState::Unissued | InvocationCapabilityState::Active(_) => {
                Err(CapabilityBridgeError::AlreadyActive)
            },
            InvocationCapabilityState::Revoked => Err(CapabilityBridgeError::Revoked),
        }
    }

    #[cfg(test)]
    pub(super) fn reset(&mut self) -> Result<(), CapabilityBridgeError> {
        if self.is_active() {
            return Err(CapabilityBridgeError::AlreadyActive);
        }
        if matches!(self.state, InvocationCapabilityState::Initialization) {
            return Err(CapabilityBridgeError::AlreadyActive);
        }
        self.state = InvocationCapabilityState::Unissued;
        self.issue()
    }

    pub(super) fn revoke(&mut self) -> Result<(), CapabilityBridgeError> {
        match self.state {
            InvocationCapabilityState::Unissued
            | InvocationCapabilityState::Initialization
            | InvocationCapabilityState::Active(_) => {
                self.state = InvocationCapabilityState::Revoked;
                Ok(())
            },
            InvocationCapabilityState::Revoked => Err(CapabilityBridgeError::Revoked),
        }
    }

    #[cfg(test)]
    pub(super) fn authorize_and_decode_async(
        &self,
        presented_identity: InvocationCapabilityIdentity,
        request: JsonValue,
    ) -> Result<AsyncCapabilityOperation, CapabilityBridgeError> {
        self.authorize(presented_identity)?;
        decode_async(decode_test_request(request)?)
    }

    #[cfg(test)]
    pub(super) fn authorize_and_decode_sync(
        &self,
        presented_identity: InvocationCapabilityIdentity,
        request: JsonValue,
    ) -> Result<SyncCapabilityOperation, CapabilityBridgeError> {
        self.authorize(presented_identity)?;
        decode_sync(decode_test_request(request)?)
    }

    #[cfg(test)]
    pub(super) fn authorize_and_decode_query_stream(
        &self,
        presented_identity: InvocationCapabilityIdentity,
        request: JsonValue,
    ) -> Result<AsyncCapabilityOperation, CapabilityBridgeError> {
        self.authorize(presented_identity)?;
        decode_query_stream(decode_test_request(request)?)
    }

    pub(super) fn authorize(
        &self,
        presented_identity: InvocationCapabilityIdentity,
    ) -> Result<(), CapabilityBridgeError> {
        match self.state {
            InvocationCapabilityState::Unissued
            | InvocationCapabilityState::Initialization
            | InvocationCapabilityState::Revoked => {
                return Err(CapabilityBridgeError::Revoked);
            },
            InvocationCapabilityState::Active(identity) if identity != presented_identity => {
                return Err(CapabilityBridgeError::ForgedIdentity);
            },
            InvocationCapabilityState::Active(_) => {},
        }
        Ok(())
    }

    pub(super) fn decode_async(
        &self,
        request: GuestCapabilityRequest,
    ) -> Result<AsyncCapabilityOperation, CapabilityBridgeError> {
        decode_async(request)
    }

    pub(super) fn decode_sync(
        &self,
        request: GuestCapabilityRequest,
    ) -> Result<SyncCapabilityOperation, CapabilityBridgeError> {
        decode_sync(request)
    }

    pub(super) fn decode_query_stream(
        &self,
        request: GuestCapabilityRequest,
    ) -> Result<AsyncCapabilityOperation, CapabilityBridgeError> {
        decode_query_stream(request)
    }

    pub(super) fn is_revoked(&self) -> bool {
        matches!(self.state, InvocationCapabilityState::Revoked)
    }

    pub(super) fn is_active(&self) -> bool {
        matches!(self.state, InvocationCapabilityState::Active(_))
    }

    pub(super) fn is_unissued(&self) -> bool {
        matches!(self.state, InvocationCapabilityState::Unissued)
    }

    pub(super) fn allows_initialization_environment(&self) -> bool {
        matches!(self.state, InvocationCapabilityState::Initialization)
    }

    pub(super) fn handle(&self) -> Result<i64, CapabilityBridgeError> {
        match self.state {
            InvocationCapabilityState::Unissued | InvocationCapabilityState::Initialization => {
                Ok(0)
            },
            InvocationCapabilityState::Active(identity) => Ok(identity.to_abi()),
            InvocationCapabilityState::Revoked => Err(CapabilityBridgeError::Revoked),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(super) enum AsyncCapabilityOperation {
    AuthenticationGetUserIdentity,
    AuditLog {
        body: JsonValue,
    },
    GetFunctionMetadata,
    GetDeploymentMetadata,
    GetTransactionMetrics,
    GetRequestMetadata,
    FunctionHandleCreate {
        function_address: CapabilityFunctionAddress,
    },
    DatabaseGet {
        table: Option<String>,
        id: JsonValue,
        is_system: bool,
    },
    DatabaseQuery {
        table: String,
        source: CapabilityQuerySource,
        operators: Vec<CapabilityQueryOperator>,
        order: CapabilityQueryOrder,
        terminal: CapabilityQueryTerminal,
    },
    DatabaseInsert {
        table: String,
        value: JsonValue,
    },
    DatabasePatch {
        table: String,
        id: JsonValue,
        patch: JsonValue,
    },
    DatabaseReplace {
        table: String,
        id: JsonValue,
        value: JsonValue,
    },
    DatabaseDelete {
        table: String,
        id: JsonValue,
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
    SchedulerRunAfter {
        delay_milliseconds: JsonValue,
        function_address: CapabilityFunctionAddress,
        args: JsonValue,
    },
    SchedulerRunAt {
        timestamp_milliseconds: JsonValue,
        function_address: CapabilityFunctionAddress,
        args: JsonValue,
    },
    SchedulerCancel {
        id: JsonValue,
    },
    RunUdf {
        udf_type: CapabilityNestedUdfType,
        function_address: CapabilityFunctionAddress,
        args: JsonValue,
        transaction_limits: Option<JsonValue>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CapabilityNestedUdfType {
    Query,
    Mutation,
    SnapshotQuery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CapabilityFunctionAddress {
    Name(String),
    Reference(String),
    FunctionHandle(String),
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum SyncCapabilityOperation {
    DatabaseNormalizeId { table: String, value: String },
    EnvironmentVariableGet { name: String },
    PerformanceNow,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum CapabilityQueryConstraint {
    Eq { field: String, value: JsonValue },
    Gt { field: String, value: JsonValue },
    Gte { field: String, value: JsonValue },
    Lt { field: String, value: JsonValue },
    Lte { field: String, value: JsonValue },
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum CapabilityQuerySource {
    FullTableScan,
    IndexRange {
        index: String,
        constraints: Vec<CapabilityQueryConstraint>,
    },
    Search {
        index: String,
        filters: Vec<CapabilitySearchFilter>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum CapabilitySearchFilter {
    Search { field: String, value: String },
    Eq { field: String, value: JsonValue },
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum CapabilityQueryOperator {
    Filter { expression: JsonValue },
    Limit { limit: u64 },
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum CapabilityQueryTerminal {
    Collect,
    First,
    Paginate(CapabilityQueryPagination),
    Stream,
    Unique,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct CapabilityQueryPagination {
    pub(super) cursor: Option<String>,
    pub(super) end_cursor: Option<String>,
    pub(super) maximum_bytes_read: Option<usize>,
    pub(super) maximum_rows_read: Option<usize>,
    pub(super) page_size: usize,
}

impl CapabilityQueryConstraint {
    pub(super) fn field(&self) -> &str {
        match self {
            Self::Eq { field, .. }
            | Self::Gt { field, .. }
            | Self::Gte { field, .. }
            | Self::Lt { field, .. }
            | Self::Lte { field, .. } => field,
        }
    }

    pub(super) fn value(self) -> JsonValue {
        match self {
            Self::Eq { value, .. }
            | Self::Gt { value, .. }
            | Self::Gte { value, .. }
            | Self::Lt { value, .. }
            | Self::Lte { value, .. } => value,
        }
    }

    pub(super) fn query_range_kind(&self) -> &'static str {
        match self {
            Self::Eq { .. } => "Eq",
            Self::Gt { .. } => "Gt",
            Self::Gte { .. } => "Gte",
            Self::Lt { .. } => "Lt",
            Self::Lte { .. } => "Lte",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CapabilityQueryOrder {
    Default,
    Asc,
    Desc,
}

impl CapabilityQueryOrder {
    pub(super) fn into_json(self) -> JsonValue {
        match self {
            Self::Default => JsonValue::Null,
            Self::Asc => JsonValue::String("asc".to_owned()),
            Self::Desc => JsonValue::String("desc".to_owned()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum CapabilityBridgeError {
    #[error("invocation capability is already active")]
    AlreadyActive,
    #[error("invocation capability identity is invalid")]
    ForgedIdentity,
    #[error("invocation capability identity space is exhausted")]
    IdentitySpaceExhausted,
    #[error("runtime capability request is invalid")]
    InvalidRequest,
    #[error("runtime capability request used the wrong execution mode")]
    InvalidRequestMode,
    #[error("invocation capability is revoked")]
    Revoked,
    #[error("runtime capability request version is unsupported")]
    UnsupportedVersion,
}

#[cfg(test)]
fn decode_test_request(
    request: JsonValue,
) -> Result<GuestCapabilityRequest, CapabilityBridgeError> {
    GuestCapabilityRequestCodec::decode_value(request).map_err(|error| match error {
        GuestCapabilityRequestError::UnsupportedVersion => {
            CapabilityBridgeError::UnsupportedVersion
        },
        GuestCapabilityRequestError::TooLarge { .. }
        | GuestCapabilityRequestError::Malformed
        | GuestCapabilityRequestError::InvalidRequest
        | GuestCapabilityRequestError::NonCanonical => CapabilityBridgeError::InvalidRequest,
    })
}

impl From<GuestCapabilityQuerySource> for CapabilityQuerySource {
    fn from(source: GuestCapabilityQuerySource) -> Self {
        match source {
            GuestCapabilityQuerySource::FullTableScan => Self::FullTableScan,
            GuestCapabilityQuerySource::IndexRange { index, constraints } => Self::IndexRange {
                index,
                constraints: constraints.into_iter().map(Into::into).collect(),
            },
            GuestCapabilityQuerySource::Search { index, filters } => Self::Search {
                index,
                filters: filters.into_iter().map(Into::into).collect(),
            },
        }
    }
}

impl From<GuestCapabilitySearchFilter> for CapabilitySearchFilter {
    fn from(filter: GuestCapabilitySearchFilter) -> Self {
        match filter {
            GuestCapabilitySearchFilter::Search { field, value } => Self::Search { field, value },
            GuestCapabilitySearchFilter::Eq { field, value } => Self::Eq { field, value },
        }
    }
}

impl From<GuestCapabilityQueryOperator> for CapabilityQueryOperator {
    fn from(operator: GuestCapabilityQueryOperator) -> Self {
        match operator {
            GuestCapabilityQueryOperator::Filter { expression } => Self::Filter { expression },
            GuestCapabilityQueryOperator::Limit { limit } => Self::Limit { limit },
        }
    }
}

impl From<GuestCapabilityQueryTerminal> for CapabilityQueryTerminal {
    fn from(terminal: GuestCapabilityQueryTerminal) -> Self {
        match terminal {
            GuestCapabilityQueryTerminal::Collect => Self::Collect,
            GuestCapabilityQueryTerminal::First => Self::First,
            GuestCapabilityQueryTerminal::Paginate(pagination) => Self::Paginate(pagination.into()),
            GuestCapabilityQueryTerminal::Stream => Self::Stream,
            GuestCapabilityQueryTerminal::Unique => Self::Unique,
        }
    }
}

impl From<GuestCapabilityQueryPagination> for CapabilityQueryPagination {
    fn from(pagination: GuestCapabilityQueryPagination) -> Self {
        Self {
            cursor: pagination.cursor,
            end_cursor: pagination.end_cursor,
            maximum_bytes_read: pagination.maximum_bytes_read,
            maximum_rows_read: pagination.maximum_rows_read,
            page_size: pagination.page_size,
        }
    }
}

impl From<GuestCapabilityQueryConstraint> for CapabilityQueryConstraint {
    fn from(constraint: GuestCapabilityQueryConstraint) -> Self {
        match constraint {
            GuestCapabilityQueryConstraint::Eq { field, value } => Self::Eq { field, value },
            GuestCapabilityQueryConstraint::Gt { field, value } => Self::Gt { field, value },
            GuestCapabilityQueryConstraint::Gte { field, value } => Self::Gte { field, value },
            GuestCapabilityQueryConstraint::Lt { field, value } => Self::Lt { field, value },
            GuestCapabilityQueryConstraint::Lte { field, value } => Self::Lte { field, value },
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RuntimeCapabilityFunctionAddress {
    Name(RuntimeCapabilityFunctionName),
    Reference(RuntimeCapabilityFunctionReference),
    FunctionHandle(RuntimeCapabilityFunctionHandle),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeCapabilityFunctionName {
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeCapabilityFunctionReference {
    reference: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeCapabilityFunctionHandle {
    function_handle: String,
}

impl TryFrom<RuntimeCapabilityFunctionAddress> for CapabilityFunctionAddress {
    type Error = CapabilityBridgeError;

    fn try_from(address: RuntimeCapabilityFunctionAddress) -> Result<Self, Self::Error> {
        let address = match address {
            RuntimeCapabilityFunctionAddress::Name(address) => Self::Name(address.name),
            RuntimeCapabilityFunctionAddress::Reference(address) => {
                Self::Reference(address.reference)
            },
            RuntimeCapabilityFunctionAddress::FunctionHandle(address) => {
                Self::FunctionHandle(address.function_handle)
            },
        };
        let value = match &address {
            Self::Name(value) | Self::Reference(value) | Self::FunctionHandle(value) => value,
        };
        if value.is_empty() {
            return Err(CapabilityBridgeError::InvalidRequest);
        }
        Ok(address)
    }
}

impl From<GuestCapabilityFunctionAddress> for CapabilityFunctionAddress {
    fn from(address: GuestCapabilityFunctionAddress) -> Self {
        match address {
            GuestCapabilityFunctionAddress::Name(value) => Self::Name(value),
            GuestCapabilityFunctionAddress::Reference(value) => Self::Reference(value),
            GuestCapabilityFunctionAddress::FunctionHandle(value) => Self::FunctionHandle(value),
        }
    }
}

impl From<GuestCapabilityNestedUdfType> for CapabilityNestedUdfType {
    fn from(udf_type: GuestCapabilityNestedUdfType) -> Self {
        match udf_type {
            GuestCapabilityNestedUdfType::Query => Self::Query,
            GuestCapabilityNestedUdfType::Mutation => Self::Mutation,
            GuestCapabilityNestedUdfType::SnapshotQuery => Self::SnapshotQuery,
        }
    }
}

pub(super) fn decode_function_address(
    value: JsonValue,
) -> Result<CapabilityFunctionAddress, CapabilityBridgeError> {
    let address: RuntimeCapabilityFunctionAddress =
        serde_json::from_value(value).map_err(|_| CapabilityBridgeError::InvalidRequest)?;
    address.try_into()
}

impl From<GuestCapabilityQueryOrder> for CapabilityQueryOrder {
    fn from(order: GuestCapabilityQueryOrder) -> Self {
        match order {
            GuestCapabilityQueryOrder::Default => Self::Default,
            GuestCapabilityQueryOrder::Asc => Self::Asc,
            GuestCapabilityQueryOrder::Desc => Self::Desc,
        }
    }
}

fn decode_async(
    request: GuestCapabilityRequest,
) -> Result<AsyncCapabilityOperation, CapabilityBridgeError> {
    Ok(match request {
        GuestCapabilityRequest::AuthGetUserIdentity => {
            AsyncCapabilityOperation::AuthenticationGetUserIdentity
        },
        GuestCapabilityRequest::AuditLog { body } => AsyncCapabilityOperation::AuditLog { body },
        GuestCapabilityRequest::GetFunctionMetadata => {
            AsyncCapabilityOperation::GetFunctionMetadata
        },
        GuestCapabilityRequest::GetDeploymentMetadata => {
            AsyncCapabilityOperation::GetDeploymentMetadata
        },
        GuestCapabilityRequest::GetTransactionMetrics => {
            AsyncCapabilityOperation::GetTransactionMetrics
        },
        GuestCapabilityRequest::GetRequestMetadata => AsyncCapabilityOperation::GetRequestMetadata,
        GuestCapabilityRequest::FunctionHandleCreate { function_address } => {
            AsyncCapabilityOperation::FunctionHandleCreate {
                function_address: function_address.into(),
            }
        },
        GuestCapabilityRequest::DbGet { table, id } => AsyncCapabilityOperation::DatabaseGet {
            table,
            id,
            is_system: false,
        },
        GuestCapabilityRequest::DbSystemGet { table, id } => {
            AsyncCapabilityOperation::DatabaseGet {
                table,
                id,
                is_system: true,
            }
        },
        GuestCapabilityRequest::DbQuery {
            table,
            source,
            operators,
            order,
            terminal,
        } => {
            if matches!(terminal, GuestCapabilityQueryTerminal::Stream) {
                return Err(CapabilityBridgeError::InvalidRequestMode);
            }
            AsyncCapabilityOperation::DatabaseQuery {
                table,
                source: source.into(),
                operators: operators.into_iter().map(Into::into).collect(),
                order: order.into(),
                terminal: terminal.into(),
            }
        },
        GuestCapabilityRequest::DbInsert { table, value } => {
            AsyncCapabilityOperation::DatabaseInsert { table, value }
        },
        GuestCapabilityRequest::DbPatch { table, id, patch } => {
            AsyncCapabilityOperation::DatabasePatch { table, id, patch }
        },
        GuestCapabilityRequest::DbReplace { table, id, value } => {
            AsyncCapabilityOperation::DatabaseReplace { table, id, value }
        },
        GuestCapabilityRequest::DbDelete { table, id } => {
            AsyncCapabilityOperation::DatabaseDelete { table, id }
        },
        GuestCapabilityRequest::StorageGetUrl { storage_id } => {
            AsyncCapabilityOperation::StorageGetUrl { storage_id }
        },
        GuestCapabilityRequest::StorageGetMetadata { storage_id } => {
            AsyncCapabilityOperation::StorageGetMetadata { storage_id }
        },
        GuestCapabilityRequest::StorageGenerateUploadUrl => {
            AsyncCapabilityOperation::StorageGenerateUploadUrl
        },
        GuestCapabilityRequest::StorageDelete { storage_id } => {
            AsyncCapabilityOperation::StorageDelete { storage_id }
        },
        GuestCapabilityRequest::SchedulerRunAfter {
            delay_milliseconds,
            function_address,
            args,
        } => AsyncCapabilityOperation::SchedulerRunAfter {
            delay_milliseconds,
            function_address: function_address.into(),
            args,
        },
        GuestCapabilityRequest::SchedulerRunAt {
            timestamp_milliseconds,
            function_address,
            args,
        } => AsyncCapabilityOperation::SchedulerRunAt {
            timestamp_milliseconds,
            function_address: function_address.into(),
            args,
        },
        GuestCapabilityRequest::SchedulerCancel { id } => {
            AsyncCapabilityOperation::SchedulerCancel { id }
        },
        GuestCapabilityRequest::RunUdf {
            udf_type,
            function_address,
            args,
            transaction_limits,
        } => AsyncCapabilityOperation::RunUdf {
            udf_type: udf_type.into(),
            function_address: function_address.into(),
            args,
            transaction_limits,
        },
        GuestCapabilityRequest::DbNormalizeId { .. }
        | GuestCapabilityRequest::EnvironmentVariableGet { .. }
        | GuestCapabilityRequest::PerformanceNow => {
            return Err(CapabilityBridgeError::InvalidRequestMode);
        },
    })
}

fn decode_query_stream(
    request: GuestCapabilityRequest,
) -> Result<AsyncCapabilityOperation, CapabilityBridgeError> {
    let GuestCapabilityRequest::DbQuery {
        table,
        source,
        operators,
        order,
        terminal: GuestCapabilityQueryTerminal::Stream,
    } = request
    else {
        return Err(CapabilityBridgeError::InvalidRequestMode);
    };
    Ok(AsyncCapabilityOperation::DatabaseQuery {
        table,
        source: source.into(),
        operators: operators.into_iter().map(Into::into).collect(),
        order: order.into(),
        terminal: CapabilityQueryTerminal::Stream,
    })
}

fn decode_sync(
    request: GuestCapabilityRequest,
) -> Result<SyncCapabilityOperation, CapabilityBridgeError> {
    match request {
        GuestCapabilityRequest::DbNormalizeId { table, value } => {
            Ok(SyncCapabilityOperation::DatabaseNormalizeId { table, value })
        },
        GuestCapabilityRequest::EnvironmentVariableGet { name } => {
            Ok(SyncCapabilityOperation::EnvironmentVariableGet { name })
        },
        GuestCapabilityRequest::PerformanceNow => Ok(SyncCapabilityOperation::PerformanceNow),
        GuestCapabilityRequest::AuthGetUserIdentity
        | GuestCapabilityRequest::AuditLog { .. }
        | GuestCapabilityRequest::GetFunctionMetadata
        | GuestCapabilityRequest::GetDeploymentMetadata
        | GuestCapabilityRequest::GetTransactionMetrics
        | GuestCapabilityRequest::GetRequestMetadata
        | GuestCapabilityRequest::FunctionHandleCreate { .. }
        | GuestCapabilityRequest::DbGet { .. }
        | GuestCapabilityRequest::DbSystemGet { .. }
        | GuestCapabilityRequest::DbQuery { .. }
        | GuestCapabilityRequest::DbInsert { .. }
        | GuestCapabilityRequest::DbPatch { .. }
        | GuestCapabilityRequest::DbReplace { .. }
        | GuestCapabilityRequest::DbDelete { .. }
        | GuestCapabilityRequest::StorageGetUrl { .. }
        | GuestCapabilityRequest::StorageGetMetadata { .. }
        | GuestCapabilityRequest::StorageGenerateUploadUrl
        | GuestCapabilityRequest::StorageDelete { .. }
        | GuestCapabilityRequest::SchedulerRunAfter { .. }
        | GuestCapabilityRequest::SchedulerRunAt { .. }
        | GuestCapabilityRequest::SchedulerCancel { .. }
        | GuestCapabilityRequest::RunUdf { .. } => Err(CapabilityBridgeError::InvalidRequestMode),
    }
}

fn next_invocation_capability_identity(
) -> Result<InvocationCapabilityIdentity, CapabilityBridgeError> {
    next_invocation_capability_identity_from(&NEXT_INVOCATION_CAPABILITY_IDENTITY)
}

fn next_invocation_capability_identity_from(
    next_identity: &AtomicU64,
) -> Result<InvocationCapabilityIdentity, CapabilityBridgeError> {
    let identity = next_identity
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |identity| {
            identity.checked_add(1)
        })
        .map_err(|_| CapabilityBridgeError::IdentitySpaceExhausted)?;
    NonZeroU64::new(identity)
        .map(InvocationCapabilityIdentity)
        .ok_or(CapabilityBridgeError::IdentitySpaceExhausted)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn db_get_request() -> JsonValue {
        json!({
            "version": 1,
            "kind": "dbGet",
            "table": "documents",
            "id": "document-id",
        })
    }

    fn performance_now_request() -> JsonValue {
        json!({
            "version": 1,
            "kind": "performanceNow",
        })
    }

    fn request_fixtures() -> Vec<(JsonValue, &'static str)> {
        vec![
            (
                json!({ "version": 1, "kind": "authGetUserIdentity" }),
                "authGetUserIdentity",
            ),
            (
                json!({
                    "version": 4,
                    "kind": "auditLog",
                    "body": {
                        "action": "document.viewed",
                        "source": { "ip": { "$var": "ip" } },
                    },
                }),
                "auditLog",
            ),
            (
                json!({ "version": 4, "kind": "getFunctionMetadata" }),
                "getFunctionMetadata",
            ),
            (
                json!({ "version": 4, "kind": "getDeploymentMetadata" }),
                "getDeploymentMetadata",
            ),
            (
                json!({ "version": 4, "kind": "getTransactionMetrics" }),
                "getTransactionMetrics",
            ),
            (
                json!({ "version": 4, "kind": "getRequestMetadata" }),
                "getRequestMetadata",
            ),
            (
                json!({
                    "version": 4,
                    "kind": "functionHandleCreate",
                    "functionAddress": {
                        "reference": "_reference/function/tasks:run",
                    },
                }),
                "functionHandleCreate",
            ),
            (db_get_request(), "dbGet"),
            (
                json!({
                    "version": 1,
                    "kind": "dbSystemGet",
                    "table": "_scheduled_functions",
                    "id": "scheduled-function-id",
                }),
                "dbSystemGet",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "dbQuery",
                    "table": "documents",
                    "source": {
                        "type": "indexRange",
                        "index": "by_tenant_sequence",
                        "constraints": [
                            { "operator": "eq", "field": "tenant", "value": "tenant-a" },
                            { "operator": "gt", "field": "sequence", "value": 3 },
                        ],
                    },
                    "operators": [],
                    "order": "asc",
                    "terminal": "unique",
                }),
                "dbQuery unique index",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "dbQuery",
                    "table": "documents",
                    "source": { "type": "fullTableScan" },
                    "operators": [
                        {
                            "type": "filter",
                            "expression": {
                                "$eq": [
                                    { "$field": "tenant" },
                                    { "$literal": "tenant-a" },
                                ],
                            },
                        },
                    ],
                    "order": null,
                    "terminal": "collect",
                }),
                "dbQuery collect",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "dbQuery",
                    "table": "documents",
                    "source": {
                        "type": "indexRange",
                        "index": "by_tenant_sequence",
                        "constraints": [
                            { "operator": "eq", "field": "tenant", "value": "tenant-a" },
                        ],
                    },
                    "operators": [{ "type": "limit", "limit": 4 }],
                    "order": "desc",
                    "terminal": "first",
                }),
                "dbQuery first",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "dbQuery",
                    "table": "documents",
                    "source": { "type": "fullTableScan" },
                    "operators": [],
                    "order": "asc",
                    "terminal": "unique",
                }),
                "dbQuery unique",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "dbInsert",
                    "table": "documents",
                    "value": { "tenant": "tenant-a" },
                }),
                "dbInsert",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "dbPatch",
                    "table": "documents",
                    "id": "document-id",
                    "patch": { "sequence": 4 },
                }),
                "dbPatch",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "dbReplace",
                    "table": "documents",
                    "id": "document-id",
                    "value": { "sequence": 5 },
                }),
                "dbReplace",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "dbDelete",
                    "table": "documents",
                    "id": "document-id",
                }),
                "dbDelete",
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageGetUrl",
                    "storageId": "storage-id",
                }),
                "storageGetUrl",
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageGetMetadata",
                    "storageId": "storage-id",
                }),
                "storageGetMetadata",
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageGenerateUploadUrl",
                }),
                "storageGenerateUploadUrl",
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageDelete",
                    "storageId": "storage-id",
                }),
                "storageDelete",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "schedulerRunAfter",
                    "delayMilliseconds": 0,
                    "functionAddress": { "name": "module:run" },
                    "args": { "sequence": 4 },
                }),
                "schedulerRunAfter name",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "schedulerRunAfter",
                    "delayMilliseconds": 0,
                    "functionAddress": {
                        "reference": "_reference/function/module:run",
                    },
                    "args": { "sequence": 4 },
                }),
                "schedulerRunAfter reference",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "schedulerRunAfter",
                    "delayMilliseconds": 0,
                    "functionAddress": { "functionHandle": "function://handle" },
                    "args": { "sequence": 4 },
                }),
                "schedulerRunAfter function handle",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "schedulerRunAt",
                    "timestampMilliseconds": 1_700_000_000_250_u64,
                    "functionAddress": { "name": "module:run" },
                    "args": { "sequence": 5 },
                }),
                "schedulerRunAt",
            ),
            (
                json!({
                    "version": 1,
                    "kind": "schedulerCancel",
                    "id": "scheduled-function-id",
                }),
                "schedulerCancel",
            ),
            (
                json!({
                    "version": 4,
                    "kind": "runUdf",
                    "udfType": "mutation",
                    "functionAddress": { "reference": "_reference/function/tasks:run" },
                    "args": { "commitTs": { "$commitTs": null }, "sequence": 6 },
                    "transactionLimits": { "documentsRead": 8, "documentsWritten": 3 },
                }),
                "runUdf mutation",
            ),
            (
                json!({
                    "version": 2,
                    "kind": "runUdf",
                    "udfType": "snapshotQuery",
                    "functionAddress": { "name": "tasks:readStale" },
                    "args": {},
                    "transactionLimits": null,
                }),
                "runUdf snapshot query",
            ),
        ]
    }

    #[test]
    fn runtime_request_decodes_every_closed_variant() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        for (request, kind) in request_fixtures() {
            bridge
                .authorize_and_decode_async(identity, request.clone())
                .unwrap_or_else(|error| panic!("{kind} failed to decode: {error}"));
            assert_eq!(
                bridge.authorize_and_decode_sync(identity, request),
                Err(CapabilityBridgeError::InvalidRequestMode),
                "async request {kind} was accepted in synchronous mode"
            );
        }
        assert_eq!(
            bridge.authorize_and_decode_sync(
                identity,
                json!({
                    "version": 1,
                    "kind": "dbNormalizeId",
                    "table": "documents",
                    "value": "document-id",
                }),
            )?,
            SyncCapabilityOperation::DatabaseNormalizeId {
                table: "documents".to_owned(),
                value: "document-id".to_owned(),
            }
        );
        assert_eq!(
            bridge.authorize_and_decode_sync(
                identity,
                json!({
                    "version": 1,
                    "kind": "environmentVariableGet",
                    "name": "APPLICATION_KEY",
                }),
            )?,
            SyncCapabilityOperation::EnvironmentVariableGet {
                name: "APPLICATION_KEY".to_owned(),
            }
        );
        assert_eq!(
            bridge.authorize_and_decode_sync(identity, performance_now_request())?,
            SyncCapabilityOperation::PerformanceNow
        );
        assert_eq!(
            bridge.authorize_and_decode_async(identity, performance_now_request()),
            Err(CapabilityBridgeError::InvalidRequestMode)
        );
        Ok(())
    }

    #[test]
    fn tableless_gets_preserve_user_and_system_authority() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        for (kind, is_system) in [("dbGet", false), ("dbSystemGet", true)] {
            assert_eq!(
                bridge.authorize_and_decode_async(
                    identity,
                    json!({
                        "version": 4,
                        "kind": kind,
                        "id": "document-id",
                    }),
                )?,
                AsyncCapabilityOperation::DatabaseGet {
                    table: None,
                    id: json!("document-id"),
                    is_system,
                }
            );
        }
        Ok(())
    }

    #[test]
    fn runtime_metadata_requests_decode_only_as_async_operations() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        for (kind, expected) in [
            (
                "getFunctionMetadata",
                AsyncCapabilityOperation::GetFunctionMetadata,
            ),
            (
                "getDeploymentMetadata",
                AsyncCapabilityOperation::GetDeploymentMetadata,
            ),
            (
                "getTransactionMetrics",
                AsyncCapabilityOperation::GetTransactionMetrics,
            ),
            (
                "getRequestMetadata",
                AsyncCapabilityOperation::GetRequestMetadata,
            ),
        ] {
            let request = json!({ "version": 4, "kind": kind });
            assert_eq!(
                bridge.authorize_and_decode_async(identity, request.clone())?,
                expected
            );
            assert_eq!(
                bridge.authorize_and_decode_sync(identity, request),
                Err(CapabilityBridgeError::InvalidRequestMode)
            );
        }
        Ok(())
    }

    #[test]
    fn runtime_audit_log_request_decodes_only_as_an_async_operation() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        let body = json!({
            "action": "document.viewed",
            "source": { "ip": { "$var": "ip" } },
        });
        let request = json!({
            "version": 4,
            "kind": "auditLog",
            "body": body.clone(),
        });
        assert_eq!(
            bridge.authorize_and_decode_async(identity, request.clone())?,
            AsyncCapabilityOperation::AuditLog { body }
        );
        assert_eq!(
            bridge.authorize_and_decode_sync(identity, request),
            Err(CapabilityBridgeError::InvalidRequestMode)
        );
        Ok(())
    }

    #[test]
    fn runtime_function_handle_create_decodes_only_as_an_async_operation() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        let request = json!({
            "version": 4,
            "kind": "functionHandleCreate",
            "functionAddress": {
                "reference": "_reference/function/tasks:run",
            },
        });
        assert_eq!(
            bridge.authorize_and_decode_async(identity, request.clone())?,
            AsyncCapabilityOperation::FunctionHandleCreate {
                function_address: CapabilityFunctionAddress::Reference(
                    "_reference/function/tasks:run".to_owned(),
                ),
            }
        );
        assert_eq!(
            bridge.authorize_and_decode_sync(identity, request),
            Err(CapabilityBridgeError::InvalidRequestMode)
        );
        Ok(())
    }

    #[test]
    fn runtime_request_rejects_authority_fields() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        for (base_request, kind) in request_fixtures() {
            for field in [
                "allowlist",
                "capability",
                "compilerCallsite",
                "importedOperation",
                "operationId",
                "perRouteTables",
            ] {
                let mut request = base_request.clone();
                request
                    .as_object_mut()
                    .expect("request fixture must be an object")
                    .insert(field.to_owned(), json!("forged"));
                assert_eq!(
                    bridge.authorize_and_decode_async(identity, request),
                    Err(CapabilityBridgeError::InvalidRequest),
                    "authority field {field} was accepted by {kind}"
                );
            }
        }
        let normalize = json!({
            "version": 1,
            "kind": "dbNormalizeId",
            "table": "documents",
            "value": "document-id",
            "operationId": 1,
        });
        assert_eq!(
            bridge.authorize_and_decode_sync(identity, normalize),
            Err(CapabilityBridgeError::InvalidRequest)
        );
        let mut performance_now = performance_now_request();
        performance_now
            .as_object_mut()
            .expect("performance.now request fixture must be an object")
            .insert("operationId".to_owned(), json!(1));
        assert_eq!(
            bridge.authorize_and_decode_sync(identity, performance_now),
            Err(CapabilityBridgeError::InvalidRequest)
        );
        Ok(())
    }

    #[test]
    fn mutation_runtime_operands_decode_exactly() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        for (request, expected) in [
            (
                json!({
                    "version": 1,
                    "kind": "dbReplace",
                    "table": "documents",
                    "id": "document-id",
                    "value": { "sequence": 5 },
                }),
                AsyncCapabilityOperation::DatabaseReplace {
                    table: "documents".to_owned(),
                    id: json!("document-id"),
                    value: json!({ "sequence": 5.0 }),
                },
            ),
            (
                json!({
                    "version": 1,
                    "kind": "dbDelete",
                    "table": "documents",
                    "id": "document-id",
                }),
                AsyncCapabilityOperation::DatabaseDelete {
                    table: "documents".to_owned(),
                    id: json!("document-id"),
                },
            ),
            (
                json!({
                    "version": 1,
                    "kind": "schedulerRunAt",
                    "timestampMilliseconds": 1_700_000_000_250_u64,
                    "functionAddress": { "reference": "_reference/function/tasks:run" },
                    "args": { "sequence": 5 },
                }),
                AsyncCapabilityOperation::SchedulerRunAt {
                    timestamp_milliseconds: json!(1_700_000_000_250_u64),
                    function_address: CapabilityFunctionAddress::Reference(
                        "_reference/function/tasks:run".to_owned(),
                    ),
                    args: json!({ "sequence": 5.0 }),
                },
            ),
            (
                json!({
                    "version": 1,
                    "kind": "schedulerCancel",
                    "id": "scheduled-function-id",
                }),
                AsyncCapabilityOperation::SchedulerCancel {
                    id: json!("scheduled-function-id"),
                },
            ),
        ] {
            assert_eq!(
                bridge.authorize_and_decode_async(identity, request)?,
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn storage_runtime_operands_decode_exactly() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        for (request, expected) in [
            (
                json!({
                    "version": 4,
                    "kind": "storageGetUrl",
                    "storageId": "storage-id",
                }),
                AsyncCapabilityOperation::StorageGetUrl {
                    storage_id: "storage-id".to_owned(),
                },
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageGetMetadata",
                    "storageId": "storage-id",
                }),
                AsyncCapabilityOperation::StorageGetMetadata {
                    storage_id: "storage-id".to_owned(),
                },
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageGenerateUploadUrl",
                }),
                AsyncCapabilityOperation::StorageGenerateUploadUrl,
            ),
            (
                json!({
                    "version": 4,
                    "kind": "storageDelete",
                    "storageId": "storage-id",
                }),
                AsyncCapabilityOperation::StorageDelete {
                    storage_id: "storage-id".to_owned(),
                },
            ),
        ] {
            assert_eq!(
                bridge.authorize_and_decode_async(identity, request.clone())?,
                expected
            );
            assert_eq!(
                bridge.authorize_and_decode_sync(identity, request),
                Err(CapabilityBridgeError::InvalidRequestMode)
            );
        }
        Ok(())
    }

    #[test]
    fn nested_mutation_runtime_operand_decodes_exactly() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        let request = json!({
            "version": 4,
            "kind": "runUdf",
            "udfType": "mutation",
            "functionAddress": { "reference": "_reference/function/tasks:run" },
            "args": { "commitTs": { "$commitTs": null }, "sequence": 6 },
            "transactionLimits": { "documentsRead": 8, "documentsWritten": 3 },
        });
        assert_eq!(
            bridge.authorize_and_decode_async(identity, request.clone())?,
            AsyncCapabilityOperation::RunUdf {
                udf_type: CapabilityNestedUdfType::Mutation,
                function_address: CapabilityFunctionAddress::Reference(
                    "_reference/function/tasks:run".to_owned(),
                ),
                args: json!({ "commitTs": { "$commitTs": null }, "sequence": 6.0 }),
                transaction_limits: Some(json!({
                    "documentsRead": 8,
                    "documentsWritten": 3,
                })),
            }
        );
        assert_eq!(
            bridge.authorize_and_decode_sync(identity, request),
            Err(CapabilityBridgeError::InvalidRequestMode)
        );
        assert_eq!(
            bridge.authorize_and_decode_async(
                identity,
                json!({
                    "version": 2,
                    "kind": "runUdf",
                    "udfType": "snapshotQuery",
                    "functionAddress": { "functionHandle": "function://read-stale" },
                    "args": {},
                    "transactionLimits": null,
                }),
            )?,
            AsyncCapabilityOperation::RunUdf {
                udf_type: CapabilityNestedUdfType::SnapshotQuery,
                function_address: CapabilityFunctionAddress::FunctionHandle(
                    "function://read-stale".to_owned(),
                ),
                args: json!({}),
                transaction_limits: None,
            }
        );
        Ok(())
    }

    #[test]
    fn nested_query_runtime_operand_decodes_exactly() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        let request = json!({
            "version": 4,
            "kind": "runUdf",
            "udfType": "query",
            "functionAddress": { "name": "tasks:readWithTimestamp" },
            "args": { "commitTs": { "$commitTs": null } },
            "transactionLimits": { "documentsRead": 8 },
        });
        assert_eq!(
            bridge.authorize_and_decode_async(identity, request.clone())?,
            AsyncCapabilityOperation::RunUdf {
                udf_type: CapabilityNestedUdfType::Query,
                function_address: CapabilityFunctionAddress::Name(
                    "tasks:readWithTimestamp".to_owned(),
                ),
                args: json!({ "commitTs": { "$commitTs": null } }),
                transaction_limits: Some(json!({ "documentsRead": 8 })),
            }
        );
        assert_eq!(
            bridge.authorize_and_decode_sync(identity, request),
            Err(CapabilityBridgeError::InvalidRequestMode)
        );
        Ok(())
    }

    #[test]
    fn pending_commit_timestamp_index_range_operand_decodes_exactly() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        let request = json!({
            "version": 4,
            "kind": "dbQuery",
            "table": "documents",
            "source": {
                "type": "indexRange",
                "index": "by_commit_ts",
                "constraints": [{
                    "operator": "eq",
                    "field": "commitTs",
                    "value": { "$commitTs": null },
                }],
            },
            "operators": [],
            "order": null,
            "terminal": "collect",
        });
        assert_eq!(
            bridge.authorize_and_decode_async(identity, request)?,
            AsyncCapabilityOperation::DatabaseQuery {
                table: "documents".to_owned(),
                source: CapabilityQuerySource::IndexRange {
                    index: "by_commit_ts".to_owned(),
                    constraints: vec![CapabilityQueryConstraint::Eq {
                        field: "commitTs".to_owned(),
                        value: json!({ "$commitTs": null }),
                    }],
                },
                operators: vec![],
                order: CapabilityQueryOrder::Default,
                terminal: CapabilityQueryTerminal::Collect,
            }
        );
        Ok(())
    }

    #[test]
    fn runtime_request_rejects_versions_shapes_and_execution_mode_mismatch() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        assert_eq!(
            bridge.authorize_and_decode_async(
                identity,
                json!({
                    "version": 5,
                    "kind": "dbGet",
                    "table": "documents",
                    "id": "document-id",
                }),
            ),
            Err(CapabilityBridgeError::UnsupportedVersion)
        );
        let mut host_numeric_request = db_get_request();
        host_numeric_request
            .as_object_mut()
            .expect("request fixture must be an object")
            .insert("version".to_owned(), json!(1.0));
        assert!(matches!(
            bridge.authorize_and_decode_async(identity, host_numeric_request),
            Err(CapabilityBridgeError::UnsupportedVersion)
        ));
        for version in [json!(1.1), json!("1"), JsonValue::Null] {
            let mut request = db_get_request();
            request
                .as_object_mut()
                .expect("request fixture must be an object")
                .insert("version".to_owned(), version);
            assert!(matches!(
                bridge.authorize_and_decode_async(identity, request),
                Err(CapabilityBridgeError::InvalidRequest
                    | CapabilityBridgeError::UnsupportedVersion)
            ));
        }
        assert_eq!(
            bridge.authorize_and_decode_sync(identity, db_get_request()),
            Err(CapabilityBridgeError::InvalidRequestMode)
        );
        assert_eq!(
            bridge.authorize_and_decode_async(
                identity,
                json!({
                    "version": 1,
                    "kind": "dbNormalizeId",
                    "table": "documents",
                    "value": "document-id",
                }),
            ),
            Err(CapabilityBridgeError::InvalidRequestMode)
        );
        Ok(())
    }

    #[test]
    fn query_order_preserves_sdk_default_and_scheduler_address_is_exact() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        let query = bridge.authorize_and_decode_async(
            identity,
            json!({
                "version": 1,
                "kind": "dbQuery",
                "table": "documents",
                "source": {
                    "type": "indexRange",
                    "index": "by_tenant",
                    "constraints": [],
                },
                "operators": [],
                "order": null,
                "terminal": "unique",
            }),
        )?;
        assert!(matches!(
            query,
            AsyncCapabilityOperation::DatabaseQuery {
                order: CapabilityQueryOrder::Default,
                terminal: CapabilityQueryTerminal::Unique,
                ..
            }
        ));

        for function_address in [
            json!({}),
            json!({ "name": "" }),
            json!({ "reference": "" }),
            json!({ "functionHandle": "" }),
            json!({ "name": "tasks:run", "reference": "_reference/function/tasks:run" }),
            json!({ "name": "tasks:run", "functionHandle": "function://handle" }),
            json!({
                "reference": "_reference/function/tasks:run",
                "functionHandle": "function://handle",
            }),
        ] {
            assert_eq!(
                bridge.authorize_and_decode_async(
                    identity,
                    json!({
                        "version": 1,
                        "kind": "schedulerRunAfter",
                        "delayMilliseconds": 0,
                        "functionAddress": function_address,
                        "args": {},
                    }),
                ),
                Err(CapabilityBridgeError::InvalidRequest)
            );
        }
        Ok(())
    }

    #[test]
    fn generic_query_runtime_operands_decode_exactly_and_fail_closed() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        let request = json!({
            "version": 1,
            "kind": "dbQuery",
            "table": "documents",
            "source": {
                "type": "indexRange",
                "index": "by_tenant_sequence",
                "constraints": [
                    { "operator": "eq", "field": "tenant", "value": "tenant-a" },
                    { "operator": "gte", "field": "sequence", "value": 3 },
                ],
            },
            "operators": [
                {
                    "type": "filter",
                    "expression": {
                        "$neq": [
                            { "$field": "disabled" },
                            { "$literal": true },
                        ],
                    },
                },
                { "type": "limit", "limit": 7.0 },
            ],
            "order": "desc",
            "terminal": "collect",
        });
        assert_eq!(
            bridge.authorize_and_decode_async(identity, request.clone())?,
            AsyncCapabilityOperation::DatabaseQuery {
                table: "documents".to_owned(),
                source: CapabilityQuerySource::IndexRange {
                    index: "by_tenant_sequence".to_owned(),
                    constraints: vec![
                        CapabilityQueryConstraint::Eq {
                            field: "tenant".to_owned(),
                            value: json!("tenant-a"),
                        },
                        CapabilityQueryConstraint::Gte {
                            field: "sequence".to_owned(),
                            value: json!(3.0),
                        },
                    ],
                },
                operators: vec![
                    CapabilityQueryOperator::Filter {
                        expression: json!({
                            "$neq": [
                                { "$field": "disabled" },
                                { "$literal": true },
                            ],
                        }),
                    },
                    CapabilityQueryOperator::Limit { limit: 7 },
                ],
                order: CapabilityQueryOrder::Desc,
                terminal: CapabilityQueryTerminal::Collect,
            }
        );

        assert_eq!(
            bridge.authorize_and_decode_async(
                identity,
                json!({
                    "version": 4,
                    "kind": "dbQuery",
                    "table": "documents",
                    "source": {
                        "type": "search",
                        "index": "by_content",
                        "filters": [
                            { "type": "search", "field": "body", "value": "needle phrase" },
                            { "type": "eq", "field": "tenant", "value": "tenant-a" },
                        ],
                    },
                    "operators": [{ "type": "limit", "limit": 4 }],
                    "order": null,
                    "terminal": "collect",
                }),
            )?,
            AsyncCapabilityOperation::DatabaseQuery {
                table: "documents".to_owned(),
                source: CapabilityQuerySource::Search {
                    index: "by_content".to_owned(),
                    filters: vec![
                        CapabilitySearchFilter::Search {
                            field: "body".to_owned(),
                            value: "needle phrase".to_owned(),
                        },
                        CapabilitySearchFilter::Eq {
                            field: "tenant".to_owned(),
                            value: json!("tenant-a"),
                        },
                    ],
                },
                operators: vec![CapabilityQueryOperator::Limit { limit: 4 }],
                order: CapabilityQueryOrder::Default,
                terminal: CapabilityQueryTerminal::Collect,
            }
        );

        let maximum_guest_integer = f64::from_bits((u64::MAX as f64).to_bits() - 1);
        let mut maximum_request = request.clone();
        maximum_request["operators"] = json!([{
            "type": "limit",
            "limit": maximum_guest_integer,
        }]);
        assert!(matches!(
            bridge.authorize_and_decode_async(identity, maximum_request)?,
            AsyncCapabilityOperation::DatabaseQuery {
                operators,
                ..
            } if operators == vec![CapabilityQueryOperator::Limit {
                limit: maximum_guest_integer as u64,
            }]
        ));

        let mut exact_u64_request = request.clone();
        exact_u64_request["operators"] = json!([{
            "type": "limit",
            "limit": u64::MAX,
        }]);
        assert!(matches!(
            bridge.authorize_and_decode_async(identity, exact_u64_request)?,
            AsyncCapabilityOperation::DatabaseQuery {
                operators,
                ..
            } if operators == vec![CapabilityQueryOperator::Limit { limit: u64::MAX }]
        ));

        for invalid_limit in [
            json!(-1.0),
            json!(1.5),
            json!(u64::MAX as f64),
            json!({ "$float": "AAAAAAAA+H8=" }),
            json!({ "$float": "AAAAAAAA8H8=" }),
            json!({ "$float": "AAAAAAAA8P8=" }),
        ] {
            let mut invalid = request.clone();
            invalid["operators"] = json!([{
                "type": "limit",
                "limit": invalid_limit,
            }]);
            assert_eq!(
                bridge.authorize_and_decode_async(identity, invalid),
                Err(CapabilityBridgeError::InvalidRequest)
            );
        }

        for invalid in [
            json!({
                "version": 1,
                "kind": "dbQuery",
                "table": "documents",
                "source": { "type": "fullTableScan", "index": "forged" },
                "operators": [],
                "order": null,
                "terminal": "collect",
            }),
            json!({
                "version": 1,
                "kind": "dbQuery",
                "table": "documents",
                "source": { "type": "fullTableScan" },
                "operators": [],
                "order": null,
                "terminal": "stream",
            }),
        ] {
            assert_eq!(
                bridge.authorize_and_decode_async(identity, invalid),
                Err(CapabilityBridgeError::InvalidRequest)
            );
        }
        Ok(())
    }

    #[test]
    fn generic_query_stream_has_a_distinct_authenticated_execution_mode() -> anyhow::Result<()> {
        let bridge = InvocationCapabilityBridge::new()?;
        let identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
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
            "operators": [{ "type": "limit", "limit": 7 }],
            "order": "desc",
            "terminal": "stream",
        });
        assert_eq!(
            bridge.authorize_and_decode_async(identity, request.clone()),
            Err(CapabilityBridgeError::InvalidRequestMode)
        );
        assert_eq!(
            bridge.authorize_and_decode_query_stream(identity, request)?,
            AsyncCapabilityOperation::DatabaseQuery {
                table: "documents".to_owned(),
                source: CapabilityQuerySource::IndexRange {
                    index: "by_tenant".to_owned(),
                    constraints: vec![CapabilityQueryConstraint::Eq {
                        field: "tenant".to_owned(),
                        value: json!("tenant-a"),
                    }],
                },
                operators: vec![CapabilityQueryOperator::Limit { limit: 7 }],
                order: CapabilityQueryOrder::Desc,
                terminal: CapabilityQueryTerminal::Stream,
            }
        );
        assert_eq!(
            bridge.authorize_and_decode_query_stream(
                identity,
                json!({
                    "version": 4,
                    "kind": "dbQuery",
                    "table": "documents",
                    "source": { "type": "fullTableScan" },
                    "operators": [],
                    "order": null,
                    "terminal": "collect",
                }),
            ),
            Err(CapabilityBridgeError::InvalidRequestMode)
        );
        Ok(())
    }

    #[test]
    fn invocation_identity_rejects_forged_stale_and_revoked_use() -> anyhow::Result<()> {
        let mut bridge = InvocationCapabilityBridge::new()?;
        assert_eq!(
            InvocationCapabilityIdentity::from_abi(0),
            Err(CapabilityBridgeError::ForgedIdentity)
        );
        let stale_identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        let forged_identity =
            InvocationCapabilityIdentity::from_abi(InvocationCapabilityBridge::new()?.handle()?)?;
        assert_eq!(
            bridge.authorize_and_decode_async(forged_identity, db_get_request()),
            Err(CapabilityBridgeError::ForgedIdentity)
        );
        assert_eq!(
            bridge.authorize_and_decode_sync(forged_identity, performance_now_request()),
            Err(CapabilityBridgeError::ForgedIdentity)
        );

        bridge.revoke()?;
        assert!(bridge.is_revoked());
        assert_eq!(
            bridge.authorize_and_decode_async(stale_identity, db_get_request()),
            Err(CapabilityBridgeError::Revoked)
        );
        assert_eq!(
            bridge.authorize_and_decode_sync(stale_identity, performance_now_request()),
            Err(CapabilityBridgeError::Revoked)
        );
        assert_eq!(bridge.handle(), Err(CapabilityBridgeError::Revoked),);

        bridge.reset()?;
        let current_identity = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        assert_ne!(current_identity, stale_identity);
        assert_eq!(
            bridge.authorize_and_decode_async(stale_identity, db_get_request()),
            Err(CapabilityBridgeError::ForgedIdentity)
        );
        assert_eq!(
            bridge.authorize_and_decode_sync(stale_identity, performance_now_request()),
            Err(CapabilityBridgeError::ForgedIdentity)
        );
        assert!(matches!(
            bridge.authorize_and_decode_async(current_identity, db_get_request()),
            Ok(AsyncCapabilityOperation::DatabaseGet { .. })
        ));
        Ok(())
    }

    #[test]
    fn unissued_capability_is_zero_and_cannot_authorize() -> anyhow::Result<()> {
        let mut bridge = InvocationCapabilityBridge::unissued();
        assert!(bridge.is_unissued());
        assert_eq!(bridge.handle()?, 0);
        let identity = InvocationCapabilityIdentity::from_abi(1)?;
        assert_eq!(
            bridge.authorize(identity),
            Err(CapabilityBridgeError::Revoked)
        );

        bridge.revoke()?;
        assert!(bridge.is_revoked());
        bridge = InvocationCapabilityBridge::unissued();
        bridge.issue()?;
        let issued = InvocationCapabilityIdentity::from_abi(bridge.handle()?)?;
        assert_eq!(bridge.authorize(issued), Ok(()));
        bridge.revoke()?;
        assert!(bridge.is_revoked());
        Ok(())
    }

    #[test]
    fn initialization_environment_authority_keeps_identity_zero() -> anyhow::Result<()> {
        let mut bridge = InvocationCapabilityBridge::unissued();
        assert!(!bridge.allows_initialization_environment());
        bridge.begin_initialization()?;
        assert!(!bridge.is_unissued());
        assert!(bridge.allows_initialization_environment());
        assert_eq!(bridge.handle()?, 0);
        assert_eq!(
            bridge.authorize(InvocationCapabilityIdentity::from_abi(1)?),
            Err(CapabilityBridgeError::Revoked)
        );

        bridge.finish_initialization()?;
        assert!(bridge.is_unissued());
        assert!(!bridge.allows_initialization_environment());
        assert_eq!(bridge.handle()?, 0);
        bridge.issue()?;
        assert!(!bridge.allows_initialization_environment());
        assert_eq!(
            bridge.begin_initialization(),
            Err(CapabilityBridgeError::AlreadyActive)
        );
        Ok(())
    }

    #[test]
    fn invocation_identity_exhaustion_fails_closed() -> anyhow::Result<()> {
        let next_identity = AtomicU64::new(u64::MAX - 1);
        assert_eq!(
            next_invocation_capability_identity_from(&next_identity)?,
            InvocationCapabilityIdentity(NonZeroU64::new(u64::MAX - 1).unwrap())
        );
        assert_eq!(next_identity.load(Ordering::Acquire), u64::MAX);
        assert_eq!(
            next_invocation_capability_identity_from(&next_identity),
            Err(CapabilityBridgeError::IdentitySpaceExhausted)
        );
        assert_eq!(next_identity.load(Ordering::Acquire), u64::MAX);
        Ok(())
    }
}
