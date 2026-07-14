use common::types::UdfType;
use metrics::{
    log_counter,
    log_counter_with_labels,
    register_convex_counter,
    StaticMetricLabel,
};
use sync_types::CanonicalizedUdfPath;

use crate::{
    ActionOutcome,
    FunctionOutcome,
    HttpActionOutcome,
    HttpActionResult,
    UdfOutcome,
};

register_convex_counter!(
    FUNCTION_LIMIT_WARNING_TOTAL,
    "Count of functions that exceeded some limit warning level",
    &["limit", "system_udf_path"]
);
pub(crate) fn log_function_limit_warning(
    limit_name: &'static str,
    system_udf_path: Option<&CanonicalizedUdfPath>,
) {
    let labels = match system_udf_path {
        Some(udf_path) => vec![
            StaticMetricLabel::new("limit", limit_name),
            StaticMetricLabel::new("system_udf_path", udf_path.to_string()),
        ],
        None => vec![
            StaticMetricLabel::new("limit", limit_name),
            StaticMetricLabel::new("system_udf_path", "none"),
        ],
    };
    log_counter_with_labels(&FUNCTION_LIMIT_WARNING_TOTAL, 1, labels);
}

register_convex_counter!(
    DATABASE_UDF_CONTEXT_REUSE_DECISION_TOTAL,
    "Number of validated marked database UDF attempts by effective context reuse decision",
    &["udf_type", "decision"],
    std::time::Duration::MAX,
);

pub(crate) fn log_database_udf_context_reuse_decision(
    udf_type: UdfType,
    reuse_context_enabled: bool,
) {
    let (udf_type, decision) = match (udf_type, reuse_context_enabled) {
        (UdfType::Query, true) => ("query", "allowed"),
        (UdfType::Mutation, true) => ("mutation", "allowed"),
        (UdfType::Action | UdfType::HttpAction, false) => return,
        (UdfType::Query | UdfType::Mutation, false)
        | (UdfType::Action | UdfType::HttpAction, true) => {
            unreachable!("invalid effective database context reuse decision")
        },
    };
    log_counter_with_labels(
        &DATABASE_UDF_CONTEXT_REUSE_DECISION_TOTAL,
        1,
        vec![
            StaticMetricLabel::new("udf_type", udf_type),
            StaticMetricLabel::new("decision", decision),
        ],
    );
}

pub fn is_developer_ok(outcome: &FunctionOutcome) -> bool {
    match &outcome {
        FunctionOutcome::Query(UdfOutcome { result, .. }) => result.is_ok(),
        FunctionOutcome::Mutation(UdfOutcome { result, .. }) => result.is_ok(),
        FunctionOutcome::Action(ActionOutcome { result, .. }) => result.is_ok(),
        FunctionOutcome::HttpAction(HttpActionOutcome { result, .. }) => match result {
            // The developer might hit errors after beginning to stream the response that wouldn't
            // be captured here
            HttpActionResult::Streamed => true,
            HttpActionResult::Error(_) => false,
        },
    }
}

register_convex_counter!(
    LEGACY_POSITIONAL_ARGS_TOTAL,
    "Number of times that legacy positional arguments are used",
);

pub fn log_legacy_positional_args() {
    log_counter(&LEGACY_POSITIONAL_ARGS_TOTAL, 1);
}
