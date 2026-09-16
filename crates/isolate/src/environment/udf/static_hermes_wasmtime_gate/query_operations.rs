use errors::ErrorMetadata;

use super::*;
use crate::environment::udf::wasm_udf_manifest::MAX_QUERY_TAKE_LIMIT;

pub(super) fn generated_query_syscall_args(
    descriptor: &ImportedOperationDescriptor,
    mut values: Vec<JsonValue>,
    npm_version: &Version,
) -> Result<anyhow::Result<JsonValue>, WasmtimeError> {
    let ImportedOperationDescriptor::DatabaseIndexQuery {
        table_name,
        index_name,
        equality_field,
        constraints,
        order,
        terminal,
        limit,
        limit_argument_index,
        ..
    } = descriptor
    else {
        return Err(WasmtimeError::new(HostInvariant));
    };
    let dynamic_limit = match limit_argument_index {
        Some(limit_argument_index) => {
            let expected_index = constraints
                .as_ref()
                .ok_or_else(|| WasmtimeError::new(HostInvariant))?
                .len();
            if usize::try_from(*limit_argument_index).ok() != Some(expected_index)
                || values.len() != expected_index + 1
            {
                return Err(WasmtimeError::new(HostInvariant));
            }
            let value = values
                .pop()
                .expect("validated dynamic limit argument missing");
            match dynamic_take_limit(value) {
                Ok(limit) => Some(limit),
                Err(error) => return Ok(Err(error)),
            }
        },
        None => None,
    };
    let mut operators = match (*limit, dynamic_limit) {
        (Some(limit), None) => vec![json!({ "limit": limit })],
        (None, Some(limit)) => vec![json!({ "limit": limit })],
        (None, None) => Vec::new(),
        (Some(_), Some(_)) => return Err(WasmtimeError::new(HostInvariant)),
    };
    if let Some(limit) = canonical_query_terminal_limit(*terminal) {
        operators.push(json!({ "limit": limit }));
    }
    let order = match order {
        QueryOrder::Ascending => json!("asc"),
        QueryOrder::Descending => json!("desc"),
    };
    let range = match (equality_field, constraints) {
        (Some(equality_field), None) => {
            let [value] =
                <[_; 1]>::try_from(values).map_err(|_| WasmtimeError::new(HostInvariant))?;
            vec![query_range_expression(
                equality_field,
                QueryConstraintOperator::Eq,
                value,
            )]
        },
        (None, Some(constraints)) if constraints.len() == values.len() => constraints
            .iter()
            .zip(values)
            .map(|(constraint, value)| {
                query_range_expression(constraint.field_path(), constraint.operator(), value)
            })
            .collect(),
        _ => return Err(WasmtimeError::new(HostInvariant)),
    };
    Ok(Ok(index_query_syscall_args(
        table_name,
        index_name,
        order,
        operators,
        range,
        npm_version,
    )))
}

pub(super) fn canonical_query_terminal_limit(terminal: QueryTerminal) -> Option<u32> {
    match terminal {
        QueryTerminal::Collect | QueryTerminal::Stream => None,
        // Convex implements these terminals as bounded collections, so the
        // cursor must be advanced once more to observe `done` before cleanup.
        QueryTerminal::First => Some(1),
        QueryTerminal::Unique => Some(2),
    }
}

fn dynamic_take_limit(value: JsonValue) -> anyhow::Result<u32> {
    let valid = value.as_f64().filter(|value| {
        value.is_finite()
            && value.fract() == 0.0
            && *value >= 1.0
            && *value <= f64::from(MAX_QUERY_TAKE_LIMIT)
    });
    valid.map(|value| value as u32).ok_or_else(|| {
        ErrorMetadata::bad_request(
            "InvalidArgument",
            "The query take limit must be a finite integer between 1 and 100000",
        )
        .into()
    })
}

fn query_range_expression(
    field_path: &str,
    operator: QueryConstraintOperator,
    value: JsonValue,
) -> JsonValue {
    let kind = match operator {
        QueryConstraintOperator::Eq => "Eq",
        QueryConstraintOperator::Gt => "Gt",
        QueryConstraintOperator::Gte => "Gte",
        QueryConstraintOperator::Lt => "Lt",
        QueryConstraintOperator::Lte => "Lte",
    };
    json!({
        "type": kind,
        "fieldPath": field_path,
        "value": value,
    })
}

fn index_query_syscall_args(
    table_name: &str,
    index_name: &str,
    order: JsonValue,
    operators: Vec<JsonValue>,
    range: Vec<JsonValue>,
    npm_version: &Version,
) -> JsonValue {
    json!({
        "query": {
            "source": {
                "type": "IndexRange",
                "indexName": format!("{table_name}.{index_name}"),
                "range": range,
                "order": order,
            },
            "operators": operators,
        },
        "version": npm_version.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(limit_argument_index: Option<u32>) -> ImportedOperationDescriptor {
        serde_json::from_value(json!({
            "kind": "databaseIndexQuery",
            "tableName": "documents",
            "indexName": "by_tenant_sequence",
            "constraints": [
                { "fieldPath": "tenant", "operator": "eq" },
                { "fieldPath": "sequence", "operator": "gt" },
            ],
            "order": "ascending",
            "terminal": "collect",
            "limit": null,
            "limitArgumentIndex": limit_argument_index,
        }))
        .expect("query descriptor fixture is malformed")
    }

    #[test]
    fn dynamic_take_limit_follows_constraint_arguments() -> anyhow::Result<()> {
        let npm_version = Version::new(1, 43, 0);
        let args = generated_query_syscall_args(
            &descriptor(Some(2)),
            vec![json!("tenant-a"), json!(3.0), json!(7.0)],
            &npm_version,
        )??;
        assert_eq!(args["query"]["operators"], json!([{ "limit": 7 }]));
        assert_eq!(
            args["query"]["source"]["range"]
                .as_array()
                .expect("query range is not an array")
                .len(),
            2
        );
        parse_query_stream_request(args)?;
        Ok(())
    }

    #[test]
    fn dynamic_take_limit_rejects_invalid_values() -> anyhow::Result<()> {
        let npm_version = Version::new(1, 43, 0);
        for invalid in [
            json!(null),
            json!("7"),
            json!(0),
            json!(-1),
            json!(1.5),
            json!(100_001),
        ] {
            let error = generated_query_syscall_args(
                &descriptor(Some(2)),
                vec![json!("tenant-a"), json!(3.0), invalid],
                &npm_version,
            )?
            .expect_err("invalid dynamic take limit passed validation");
            assert!(error.is_bad_request());
        }
        Ok(())
    }

    #[test]
    fn dynamic_take_limit_rejects_argument_count_corruption() {
        let npm_version = Version::new(1, 43, 0);
        for values in [
            vec![json!("tenant-a"), json!(3.0)],
            vec![json!("tenant-a"), json!(3.0), json!(7.0), json!("extra")],
        ] {
            assert!(
                generated_query_syscall_args(&descriptor(Some(2)), values, &npm_version).is_err()
            );
        }
    }

    #[test]
    fn query_syscall_args_use_invocation_npm_version() -> anyhow::Result<()> {
        for npm_version in [Version::new(1, 42, 3), Version::new(1, 43, 0)] {
            let args = generated_query_syscall_args(
                &descriptor(Some(2)),
                vec![json!("tenant-a"), json!(3.0), json!(7.0)],
                &npm_version,
            )??;
            assert_eq!(args["version"], json!(npm_version.to_string()));
            assert_eq!(parse_query_stream_request(args)?.version, Some(npm_version),);
        }
        Ok(())
    }

    #[test]
    fn terminal_queries_append_canonical_collect_limits() -> anyhow::Result<()> {
        let npm_version = Version::new(1, 43, 0);
        for (terminal, limit, expected_limits) in [
            ("collect", Some(3), vec![3]),
            ("first", None, vec![1]),
            ("first", Some(3), vec![3, 1]),
            ("unique", None, vec![2]),
            ("unique", Some(3), vec![3, 2]),
        ] {
            let descriptor = serde_json::from_value(json!({
                "kind": "databaseIndexQuery",
                "tableName": "documents",
                "indexName": "by_tenant",
                "constraints": [
                    { "fieldPath": "tenant", "operator": "eq" },
                ],
                "order": "ascending",
                "terminal": terminal,
                "limit": limit,
            }))
            .expect("query descriptor fixture is malformed");
            let args =
                generated_query_syscall_args(&descriptor, vec![json!("tenant-a")], &npm_version)??;
            assert_eq!(
                args["query"]["operators"],
                JsonValue::Array(
                    expected_limits
                        .into_iter()
                        .map(|limit| json!({ "limit": limit }))
                        .collect(),
                ),
            );
            parse_query_stream_request(args)?;
        }
        Ok(())
    }
}
