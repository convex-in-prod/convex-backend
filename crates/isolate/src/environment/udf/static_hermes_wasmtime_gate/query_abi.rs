use common::{
    paths::FieldPath,
    query::{
        Expression,
        FullTableScan,
        IndexRange,
        IndexRangeExpression,
        Order,
        Query,
        QueryOperator as DatabaseQueryOperator,
        QuerySource as DatabaseQuerySource,
        Search,
        SearchFilterExpression,
        MAX_QUERY_OPERATORS,
    },
    types::{
        IndexName,
        MaybeValue,
        TableName,
    },
    value::MAX_COMMIT_TS,
};
use value::{
    wasm_abi,
    PendingValue,
};

use super::{
    capability_bridge::{
        CapabilityQueryOrder,
        CapabilityQueryPagination,
        CapabilityQueryTerminal,
    },
    UdfType,
};

const MAGIC: &[u8; 4] = b"CQR1";
const MAX_OPERATORS: usize = 256;
const MAX_EXPRESSION_DEPTH: usize = 64;
const MAX_EXPRESSION_NODES: usize = 4096;

#[derive(Debug, PartialEq)]
pub(super) struct QueryRequest {
    pub table: String,
    pub source: QuerySource,
    pub order: CapabilityQueryOrder,
    pub operators: Vec<QueryOperator>,
    pub terminal: CapabilityQueryTerminal,
}

#[derive(Debug, PartialEq)]
pub(super) enum QuerySource {
    FullTableScan,
    IndexRange {
        index: String,
        constraints: Vec<QueryConstraint>,
    },
    Search {
        index: String,
        filters: Vec<SearchFilter>,
    },
}

#[derive(Debug, PartialEq)]
pub(super) struct QueryConstraint {
    pub kind: ConstraintKind,
    pub field: FieldPath,
    pub value: Option<PendingValue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConstraintKind {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, PartialEq)]
pub(super) enum SearchFilter {
    Search { field: FieldPath, value: String },
    Eq { field: FieldPath, value: MaybeValue },
}

#[derive(Debug, PartialEq)]
pub(super) enum QueryOperator {
    Filter(Expression),
    Limit(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum QueryRecordError {
    #[error("typed query record is malformed")]
    Malformed,
    #[error("typed query record exceeds its byte limit")]
    TooLarge,
}

struct Reader<'a> {
    remaining: &'a [u8],
    maximum: usize,
    expression_nodes: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], QueryRecordError> {
        let bytes = self
            .remaining
            .get(..length)
            .ok_or(QueryRecordError::Malformed)?;
        self.remaining = &self.remaining[length..];
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, QueryRecordError> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Result<usize, QueryRecordError> {
        let bytes: [u8; 4] = self.bytes(4)?.try_into().expect("fixed-width slice");
        Ok(u32::from_le_bytes(bytes) as usize)
    }

    fn u64(&mut self) -> Result<u64, QueryRecordError> {
        let bytes: [u8; 8] = self.bytes(8)?.try_into().expect("fixed-width slice");
        Ok(u64::from_le_bytes(bytes))
    }

    fn string(&mut self) -> Result<String, QueryRecordError> {
        let length = self.u32()?;
        std::str::from_utf8(self.bytes(length)?)
            .map(str::to_owned)
            .map_err(|_| QueryRecordError::Malformed)
    }

    fn optional_string(&mut self) -> Result<Option<String>, QueryRecordError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.string().map(Some),
            _ => Err(QueryRecordError::Malformed),
        }
    }

    fn optional_usize(&mut self) -> Result<Option<usize>, QueryRecordError> {
        match self.u8()? {
            0 => Ok(None),
            1 => usize::try_from(self.u64()?)
                .map(Some)
                .map_err(|_| QueryRecordError::Malformed),
            _ => Err(QueryRecordError::Malformed),
        }
    }

    fn field(&mut self) -> Result<FieldPath, QueryRecordError> {
        self.string()?
            .parse()
            .map_err(|_| QueryRecordError::Malformed)
    }

    fn value_frame(&mut self, pending: bool) -> Result<PendingValue, QueryRecordError> {
        let length = self.u32()?;
        let bytes = self.bytes(length)?;
        if pending {
            wasm_abi::decode_pending(bytes, self.maximum).map_err(|_| QueryRecordError::Malformed)
        } else {
            wasm_abi::decode_committed(bytes, self.maximum)
                .map(PendingValue::from)
                .map_err(|_| QueryRecordError::Malformed)
        }
    }

    fn optional_range_value(&mut self) -> Result<Option<PendingValue>, QueryRecordError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.value_frame(true).map(Some),
            _ => Err(QueryRecordError::Malformed),
        }
    }

    fn maybe_value(&mut self) -> Result<MaybeValue, QueryRecordError> {
        match self.u8()? {
            0 => Ok(MaybeValue(None)),
            1 => {
                let length = self.u32()?;
                wasm_abi::decode_committed(self.bytes(length)?, self.maximum)
                    .map(|value| MaybeValue(Some(value)))
                    .map_err(|_| QueryRecordError::Malformed)
            },
            _ => Err(QueryRecordError::Malformed),
        }
    }

    fn expression(&mut self, depth: usize) -> Result<Expression, QueryRecordError> {
        if depth >= MAX_EXPRESSION_DEPTH || self.expression_nodes >= MAX_EXPRESSION_NODES {
            return Err(QueryRecordError::Malformed);
        }
        self.expression_nodes += 1;
        let next = depth + 1;
        let tag = self.u8()?;
        Ok(match tag {
            1 => Expression::Literal(self.maybe_value()?),
            2 => Expression::Field(self.field()?),
            3 => Expression::Eq(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            4 => Expression::Neq(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            5 => Expression::Lt(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            6 => Expression::Lte(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            7 => Expression::Gt(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            8 => Expression::Gte(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            9 => Expression::Add(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            10 => Expression::Sub(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            11 => Expression::Mul(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            12 => Expression::Div(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            13 => Expression::Mod(
                Box::new(self.expression(next)?),
                Box::new(self.expression(next)?),
            ),
            14 => Expression::Neg(Box::new(self.expression(next)?)),
            15 => Expression::Not(Box::new(self.expression(next)?)),
            16 | 17 => {
                let count = self.u32()?;
                if count > MAX_OPERATORS {
                    return Err(QueryRecordError::Malformed);
                }
                let expressions = (0..count)
                    .map(|_| self.expression(next))
                    .collect::<Result<Vec<_>, _>>()?;
                if tag == 16 {
                    Expression::And(expressions)
                } else {
                    Expression::Or(expressions)
                }
            },
            _ => return Err(QueryRecordError::Malformed),
        })
    }
}

pub(super) fn decode(bytes: &[u8], maximum: usize) -> Result<QueryRequest, QueryRecordError> {
    if bytes.len() > maximum {
        return Err(QueryRecordError::TooLarge);
    }
    let mut reader = Reader {
        remaining: bytes,
        maximum,
        expression_nodes: 0,
    };
    if reader.bytes(MAGIC.len())? != MAGIC {
        return Err(QueryRecordError::Malformed);
    }
    let terminal_kind = reader.u8()?;
    let source_kind = reader.u8()?;
    let order = match reader.u8()? {
        0 => CapabilityQueryOrder::Default,
        1 => CapabilityQueryOrder::Asc,
        2 => CapabilityQueryOrder::Desc,
        _ => return Err(QueryRecordError::Malformed),
    };
    let table = reader.string()?;
    let source = match source_kind {
        1 => QuerySource::FullTableScan,
        2 => {
            let index = reader.string()?;
            let count = reader.u32()?;
            if count > MAX_OPERATORS {
                return Err(QueryRecordError::Malformed);
            }
            let constraints = (0..count)
                .map(|_| {
                    let kind = match reader.u8()? {
                        1 => ConstraintKind::Eq,
                        2 => ConstraintKind::Gt,
                        3 => ConstraintKind::Gte,
                        4 => ConstraintKind::Lt,
                        5 => ConstraintKind::Lte,
                        _ => return Err(QueryRecordError::Malformed),
                    };
                    Ok(QueryConstraint {
                        kind,
                        field: reader.field()?,
                        value: reader.optional_range_value()?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            QuerySource::IndexRange { index, constraints }
        },
        3 => {
            if order != CapabilityQueryOrder::Default {
                return Err(QueryRecordError::Malformed);
            }
            let index = reader.string()?;
            let count = reader.u32()?;
            if count == 0 || count > MAX_OPERATORS {
                return Err(QueryRecordError::Malformed);
            }
            let filters = (0..count)
                .map(|position| {
                    let kind = reader.u8()?;
                    let field = reader.field()?;
                    match (position, kind) {
                        (0, 1) => Ok(SearchFilter::Search {
                            field,
                            value: reader.string()?,
                        }),
                        (_, 2) if position > 0 => Ok(SearchFilter::Eq {
                            field,
                            value: reader.maybe_value()?,
                        }),
                        _ => Err(QueryRecordError::Malformed),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            QuerySource::Search { index, filters }
        },
        _ => return Err(QueryRecordError::Malformed),
    };
    let count = reader.u32()?;
    if count > MAX_OPERATORS {
        return Err(QueryRecordError::Malformed);
    }
    let operators = (0..count)
        .map(|_| match reader.u8()? {
            1 => reader.expression(0).map(QueryOperator::Filter),
            2 => reader.u64().map(QueryOperator::Limit),
            _ => Err(QueryRecordError::Malformed),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let terminal = match terminal_kind {
        1 => CapabilityQueryTerminal::Collect,
        2 => CapabilityQueryTerminal::First,
        3 => CapabilityQueryTerminal::Unique,
        4 => CapabilityQueryTerminal::Stream,
        5 => CapabilityQueryTerminal::Paginate(CapabilityQueryPagination {
            cursor: reader.optional_string()?,
            end_cursor: reader.optional_string()?,
            maximum_bytes_read: reader.optional_usize()?,
            maximum_rows_read: reader.optional_usize()?,
            page_size: usize::try_from(reader.u64()?).map_err(|_| QueryRecordError::Malformed)?,
        }),
        _ => return Err(QueryRecordError::Malformed),
    };
    if !reader.remaining.is_empty() {
        return Err(QueryRecordError::Malformed);
    }
    Ok(QueryRequest {
        table,
        source,
        order,
        operators,
        terminal,
    })
}

pub(super) fn into_query(
    request: QueryRequest,
    terminal_limit: Option<u32>,
    udf_type: UdfType,
    invalid_query_source: &mut bool,
) -> anyhow::Result<Query> {
    let operator_count = request.operators.len() + usize::from(terminal_limit.is_some());
    anyhow::ensure!(
        operator_count <= MAX_QUERY_OPERATORS,
        "Query has too many operators: {operator_count}"
    );
    let table_name: TableName = request.table.parse()?;
    let order = match request.order {
        CapabilityQueryOrder::Default | CapabilityQueryOrder::Asc => Order::Asc,
        CapabilityQueryOrder::Desc => Order::Desc,
    };
    let source = match request.source {
        QuerySource::FullTableScan => {
            DatabaseQuerySource::FullTableScan(FullTableScan { table_name, order })
        },
        QuerySource::IndexRange { index, constraints } => {
            let mut range = Vec::with_capacity(constraints.len());
            for constraint in constraints {
                let value = match constraint.value {
                    None => MaybeValue(None),
                    Some(pending) => {
                        if udf_type == UdfType::Query && pending.is_pending() {
                            *invalid_query_source = true;
                        }
                        MaybeValue(Some(pending.into_resolved(MAX_COMMIT_TS)?))
                    },
                };
                range.push(match constraint.kind {
                    ConstraintKind::Eq => IndexRangeExpression::Eq(constraint.field, value),
                    ConstraintKind::Gt => IndexRangeExpression::Gt(constraint.field, value),
                    ConstraintKind::Gte => IndexRangeExpression::Gte(constraint.field, value),
                    ConstraintKind::Lt => IndexRangeExpression::Lt(constraint.field, value),
                    ConstraintKind::Lte => IndexRangeExpression::Lte(constraint.field, value),
                });
            }
            DatabaseQuerySource::IndexRange(IndexRange {
                index_name: format!("{}.{index}", request.table).parse::<IndexName>()?,
                range,
                order,
            })
        },
        QuerySource::Search { index, filters } => {
            let filters = filters
                .into_iter()
                .map(|filter| match filter {
                    SearchFilter::Search { field, value } => {
                        SearchFilterExpression::Search(field, value)
                    },
                    SearchFilter::Eq { field, value } => SearchFilterExpression::Eq(field, value.0),
                })
                .collect();
            DatabaseQuerySource::Search(Search {
                index_name: format!("{}.{index}", request.table).parse::<IndexName>()?,
                table: table_name,
                filters,
            })
        },
    };
    let mut operators = request
        .operators
        .into_iter()
        .map(|operator| match operator {
            QueryOperator::Filter(expression) => Ok(DatabaseQueryOperator::Filter(expression)),
            QueryOperator::Limit(limit) => {
                Ok(DatabaseQueryOperator::Limit(usize::try_from(limit)?))
            },
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if let Some(limit) = terminal_limit {
        operators.push(DatabaseQueryOperator::Limit(usize::try_from(limit)?));
    }
    Ok(Query { source, operators })
}
