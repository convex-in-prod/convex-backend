//! A small, operator-owned comparison contract. It never changes execution or
//! persisted documents. Only explicitly declared derivatives and opaque fresh
//! identities may be represented differently in comparator-owned copies.
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::LazyLock,
};

use common::components::CanonicalizedComponentFunctionPath;
use serde::{
    de::{
        MapAccess,
        Visitor,
    },
    Deserialize,
    Deserializer,
};
use serde_json::{
    json,
    Value,
};
use value::serialized_args_ext::SerializedArgsExt;

use super::{
    MutationIdentityFailure as Failure,
    MutationShadowObservation,
    NormalizedMutationWriteSet,
};

// The fallback is rare and detached from primary completion, but still shares
// server resources. Bound bytes, records and nesting before cloning or parsing.
const MAX_BYTES: usize = 512 * 1024;
const MAX_RECORDS: usize = 256;
const MAX_DEPTH: usize = 64;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct IdentityContract {
    serialized_fields: Vec<SerializedField>,
    opaque_scheduled_tokens: Vec<ScheduledToken>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SerializedField {
    table: String,
    field: String,
    // Missing optional fields are omitted, just as JSON.stringify omits
    // undefined object fields. Undeclared document fields still compare normally.
    payload_fields: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScheduledToken {
    function: String,
    field: String,
}

static CONTRACTS: LazyLock<Result<BTreeMap<String, IdentityContract>, Failure>> = LazyLock::new(
    || match std::env::var("FUNCTION_RUNNER_MUTATION_SHADOW_IDENTITY_CONTRACTS") {
        Ok(text) => parse_contracts(&text),
        Err(std::env::VarError::NotPresent) => Ok(BTreeMap::new()),
        Err(std::env::VarError::NotUnicode(_)) => Err(Failure::InvalidContract),
    },
);

fn parse_contracts(text: &str) -> Result<BTreeMap<String, IdentityContract>, Failure> {
    if text.len() > 64 * 1024 {
        return Err(Failure::InvalidContract);
    }
    // Deserializing directly into a map would silently keep the last of two
    // identical selectors before the canonical-alias check can inspect them.
    struct ContractsVisitor;
    impl<'de> Visitor<'de> for ContractsVisitor {
        type Value = BTreeMap<String, IdentityContract>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("distinct mutation comparison selectors")
        }

        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut contracts = BTreeMap::new();
            while let Some((path, contract)) = map.next_entry::<String, IdentityContract>()? {
                if contracts.insert(path, contract).is_some() {
                    return Err(serde::de::Error::custom("duplicate comparison selector"));
                }
            }
            Ok(contracts)
        }
    }
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let supplied = deserializer
        .deserialize_map(ContractsVisitor)
        .map_err(|_| Failure::InvalidContract)?;
    deserializer.end().map_err(|_| Failure::InvalidContract)?;
    if supplied.len() > 32 {
        return Err(Failure::InvalidContract);
    }
    let mut contracts = BTreeMap::new();
    for (path, contract) in supplied {
        let path = path
            .parse::<sync_types::UdfPath>()
            .map_err(|_| Failure::InvalidContract)?
            .canonicalize()
            .strip()
            .to_string();
        if contract.serialized_fields.len() + contract.opaque_scheduled_tokens.len() > 32 {
            return Err(Failure::InvalidContract);
        }
        let mut fields = BTreeSet::new();
        for rule in &contract.serialized_fields {
            if rule.table.is_empty()
                || rule.table.starts_with('_')
                || rule.field.starts_with('_')
                || rule.field.is_empty()
                || rule.payload_fields.is_empty()
                || rule.payload_fields.len() > 64
                || rule
                    .payload_fields
                    .iter()
                    .any(|field| field == &rule.field || field.starts_with('_') || field.is_empty())
                || rule.payload_fields.iter().collect::<BTreeSet<_>>().len()
                    != rule.payload_fields.len()
                || !fields.insert((&rule.table, &rule.field))
            {
                return Err(Failure::InvalidContract);
            }
        }
        // Derivatives are validated against original document fields, never
        // another rule's comparator-owned representation.
        if contract.serialized_fields.iter().any(|rule| {
            rule.payload_fields
                .iter()
                .any(|field| fields.contains(&(&rule.table, field)))
        }) {
            return Err(Failure::InvalidContract);
        }
        let mut tokens = BTreeSet::new();
        for rule in &contract.opaque_scheduled_tokens {
            let function = rule
                .function
                .parse::<sync_types::CanonicalizedUdfPath>()
                .map_err(|_| Failure::InvalidContract)?;
            if rule.field.is_empty() || !tokens.insert(function) {
                return Err(Failure::InvalidContract);
            }
        }
        if contracts.insert(path, contract).is_some() {
            return Err(Failure::InvalidContract);
        }
    }
    Ok(contracts)
}

pub(super) fn configured_contract(
    path: &CanonicalizedComponentFunctionPath,
) -> Result<Option<&'static IdentityContract>, Failure> {
    // A root application's declaration grants no authority over components.
    if !path.component.is_root() {
        return Ok(None);
    }
    let contracts = CONTRACTS.as_ref().map_err(|error| *error)?;
    Ok(contracts.get(&path.udf_path.clone().strip().to_string()))
}

pub(super) struct Normalized {
    pub(super) writes: NormalizedMutationWriteSet,
    pub(super) tokens: BTreeMap<String, String>,
}

pub(super) fn normalize_pair(
    primary: &MutationShadowObservation,
    shadow: &MutationShadowObservation,
    contract: Result<Option<&IdentityContract>, Failure>,
) -> Result<Option<(Normalized, Normalized)>, Failure> {
    let Some(contract) = contract? else {
        return Ok(None);
    };
    if primary.outcome.path != shadow.outcome.path
        || primary.outcome.arguments != shadow.outcome.arguments
    {
        return Err(Failure::Different);
    }
    // Never infer a contract from a string's spelling or from the mismatch itself.
    let primary_normalized = normalize(primary, contract)?;
    let shadow_normalized = normalize(shadow, contract)?;
    if !primary_normalized.tokens.is_empty() || !shadow_normalized.tokens.is_empty() {
        let mut primary_draws = primary
            .outcome
            .host_operation_trace
            .generated_uuids()
            .ok_or(Failure::MissingUuidProvenance)?
            .to_vec();
        let mut shadow_draws = shadow
            .outcome
            .host_operation_trace
            .generated_uuids()
            .ok_or(Failure::MissingUuidProvenance)?
            .to_vec();
        primary_draws.sort_unstable();
        shadow_draws.sort_unstable();
        // Allow different assignment order, not lost, extra or changed draws.
        if primary_draws != shadow_draws {
            return Err(Failure::UuidProvenanceMismatch);
        }
    }
    Ok(Some((primary_normalized, shadow_normalized)))
}

pub(super) fn charge(value: &Value, remaining: &mut usize, depth: usize) -> Result<(), Failure> {
    if depth > MAX_DEPTH {
        return Err(Failure::WorkLimit);
    }
    let bytes = match value {
        Value::String(text) => text.len() + 16,
        _ => 16,
    };
    *remaining = remaining.checked_sub(bytes).ok_or(Failure::WorkLimit)?;
    match value {
        Value::Array(values) => {
            for value in values {
                charge(value, remaining, depth + 1)?;
            }
        },
        Value::Object(fields) => {
            for (name, value) in fields {
                *remaining = remaining
                    .checked_sub(name.len())
                    .ok_or(Failure::WorkLimit)?;
                charge(value, remaining, depth + 1)?;
            }
        },
        _ => {},
    }
    Ok(())
}

fn normalize(
    observation: &MutationShadowObservation,
    contract: &IdentityContract,
) -> Result<Normalized, Failure> {
    let source = &observation.writes;
    if source.inserts.len() + source.existing.len() + source.preexisting_inserts.len() > MAX_RECORDS
    {
        return Err(Failure::WorkLimit);
    }
    let mut remaining = MAX_BYTES;
    for value in source
        .inserts
        .values()
        .chain(source.preexisting_inserts.values())
    {
        charge(value, &mut remaining, 0)?;
    }
    for write in source.existing.values() {
        charge(&write.old_document, &mut remaining, 0)?;
        if let Some(value) = &write.new_document {
            charge(value, &mut remaining, 0)?;
        }
    }
    if observation.outcome.arguments.get().len() > remaining {
        return Err(Failure::WorkLimit);
    }
    remaining -= observation.outcome.arguments.get().len();
    match &observation.outcome.result {
        Ok(value) => {
            // JsonPackedValue materializes JSON on demand. Admit its source
            // bytes before allocating the parsed representation.
            if value.as_str().len() > remaining {
                return Err(Failure::WorkLimit);
            }
            charge(&value.json_value(), &mut remaining, 0)?;
        },
        Err(error) => {
            if let Some(value) = &error.custom_data {
                use value::heap_size::HeapSize;
                if value.heap_size() > remaining {
                    return Err(Failure::WorkLimit);
                }
                charge(&value.to_internal_json(), &mut remaining, 0)?;
            }
        },
    }
    let tokens = scheduled_tokens(observation, contract)?;
    let mut writes = source.clone();
    let mut fresh_identities = super::encoded_insert_ids(&writes.inserts)?;
    fresh_identities.extend(tokens.keys().cloned());
    for (id, value) in writes
        .inserts
        .iter_mut()
        .chain(writes.preexisting_inserts.iter_mut())
    {
        if has_identity_key(value, &|key| fresh_identities.contains(key)) {
            return Err(Failure::InvalidIdentity);
        }
        if let Some(table) = writes.root_table_names.get(&id.tablet_id) {
            normalize_document(value, table, contract, &fresh_identities, &mut remaining)?;
        }
        *value = replace_tokens(value, &tokens);
    }
    for (id, write) in &mut writes.existing {
        if let Some(value) = &mut write.new_document {
            if has_identity_key(value, &|key| fresh_identities.contains(key)) {
                return Err(Failure::InvalidIdentity);
            }
            if let Some(table) = writes.root_table_names.get(&id.tablet_id) {
                normalize_document(value, table, contract, &fresh_identities, &mut remaining)?;
            }
            *value = replace_tokens(value, &tokens);
        }
    }
    Ok(Normalized { writes, tokens })
}

fn scheduled_tokens(
    observation: &MutationShadowObservation,
    contract: &IdentityContract,
) -> Result<BTreeMap<String, String>, Failure> {
    let writes = &observation.writes;
    let mut tokens = BTreeMap::new();
    let mut generated_counts = None;
    let ids: BTreeMap<_, _> = writes
        .inserts
        .keys()
        .map(|id| (id.developer_id.encode(), id))
        .collect();
    for job_id in &writes.scheduled_job_inserts {
        // The stored target component is independent of the component which
        // scheduled the job. Root policy cannot authorize a child-owned job.
        if writes
            .root_table_names
            .get(&job_id.tablet_id)
            .map(String::as_str)
            != Some("_scheduled_jobs")
        {
            continue;
        }
        let job = &writes.inserts[job_id];
        for rule in &contract.opaque_scheduled_tokens {
            let expected_function = rule
                .function
                .parse::<sync_types::CanonicalizedUdfPath>()
                .map_err(|_| Failure::InvalidContract)?;
            let actual_function = job
                .get("udfPath")
                .and_then(Value::as_str)
                .ok_or(Failure::InvalidOpaqueToken)?
                .parse::<sync_types::CanonicalizedUdfPath>()
                .map_err(|_| Failure::InvalidOpaqueToken)?;
            if job.get("component").and_then(Value::as_str) != Some("")
                || actual_function != expected_function
            {
                continue;
            }
            let args_id = job
                .get("argsId")
                .and_then(Value::as_str)
                .and_then(|id| ids.get(id))
                .ok_or(Failure::InvalidOpaqueToken)?;
            if writes
                .root_table_names
                .get(&args_id.tablet_id)
                .map(String::as_str)
                != Some("_scheduled_job_args")
            {
                return Err(Failure::InvalidOpaqueToken);
            }
            let args = writes.inserts[*args_id]
                .pointer("/args/$mutationShadowScheduledArgs")
                .and_then(Value::as_array)
                .filter(|args| args.len() == 1)
                .ok_or(Failure::InvalidOpaqueToken)?;
            let token = args[0]
                .get(&rule.field)
                .and_then(Value::as_str)
                .ok_or(Failure::InvalidOpaqueToken)?;
            if generated_counts.is_none() {
                let draws = observation
                    .outcome
                    .host_operation_trace
                    .generated_uuids()
                    .ok_or(Failure::MissingUuidProvenance)?;
                let mut counts = BTreeMap::<String, usize>::new();
                for bytes in draws {
                    *counts.entry(uuid_text(bytes)).or_default() += 1;
                }
                generated_counts = Some(counts);
            }
            // Provenance is established by the host's actual generated value,
            // not UUID syntax or the fact that both lanes observed randomness.
            let count = generated_counts
                .as_ref()
                .expect("UUID evidence initialized")
                .get(token)
                .copied();
            if count != Some(1)
                || tokens
                    .insert(token.to_owned(), job_id.developer_id.encode())
                    .is_some()
            {
                return Err(Failure::InvalidOpaqueToken);
            }
        }
    }
    if !tokens.is_empty() {
        let arguments = observation
            .outcome
            .arguments
            .as_args()
            .map_err(|_| Failure::InvalidOpaqueToken)?;
        let contains = |value: &Value| contains_token(value, &tokens);
        if arguments.iter().any(contains)
            || writes.preexisting_inserts.values().any(contains)
            || writes
                .existing
                .values()
                .any(|write| contains(&write.old_document))
        {
            return Err(Failure::InvalidOpaqueToken);
        }
    }
    Ok(tokens)
}

fn uuid_text(bytes: &[u8; 16]) -> String {
    // No UUID dependency is needed just to inspect the host's fixed representation.
    let hex = b"0123456789abcdef";
    let mut text = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if [4, 6, 8, 10].contains(&index) {
            text.push('-');
        }
        text.push(hex[(byte >> 4) as usize] as char);
        text.push(hex[(byte & 15) as usize] as char);
    }
    text
}

fn contains_token(value: &Value, tokens: &BTreeMap<String, String>) -> bool {
    match value {
        Value::String(text) => tokens.contains_key(text),
        Value::Array(values) => values.iter().any(|value| contains_token(value, tokens)),
        Value::Object(fields) => fields
            .iter()
            .any(|(key, value)| tokens.contains_key(key) || contains_token(value, tokens)),
        _ => false,
    }
}

// Object keys cannot hold typed identity references. Leaving a known identity
// key literal could accept the same spelling attached to different logical
// owners in the two lanes. Reject that unsupported representation instead.
pub(super) fn has_identity_key(value: &Value, is_identity: &impl Fn(&str) -> bool) -> bool {
    match value {
        Value::Array(values) => values
            .iter()
            .any(|value| has_identity_key(value, is_identity)),
        Value::Object(fields) => fields
            .iter()
            .any(|(key, value)| is_identity(key) || has_identity_key(value, is_identity)),
        _ => false,
    }
}

pub(super) fn replace_tokens(value: &Value, tokens: &BTreeMap<String, String>) -> Value {
    if tokens.is_empty() {
        return value.clone();
    }
    match value {
        Value::String(text) if tokens.contains_key(text) => {
            json!({"$shadowFreshUuid": tokens[text]})
        },
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| replace_tokens(value, tokens))
                .collect(),
        ),
        Value::Object(fields) if !fields.contains_key("$bytes") => Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), replace_tokens(value, tokens)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn normalize_document(
    value: &mut Value,
    table: &str,
    contract: &IdentityContract,
    ids: &BTreeSet<String>,
    remaining: &mut usize,
) -> Result<(), Failure> {
    for rule in &contract.serialized_fields {
        if rule.table != table {
            continue;
        }
        let fields = value
            .as_object_mut()
            .ok_or(Failure::InvalidSerializedField)?;
        let encoded = fields
            .get(&rule.field)
            .and_then(Value::as_str)
            .ok_or(Failure::InvalidSerializedField)?;
        *remaining = remaining
            .checked_sub(encoded.len())
            .ok_or(Failure::WorkLimit)?;
        let parsed: Value =
            serde_json::from_str(encoded).map_err(|_| Failure::InvalidSerializedField)?;
        charge(&parsed, remaining, 0)?;
        let payload = Value::Object(
            rule.payload_fields
                .iter()
                .filter_map(|field| {
                    fields
                        .get(field)
                        .map(|value| (field.clone(), value.clone()))
                })
                .collect(),
        );
        if !json_payload_equal(&parsed, &payload) {
            return Err(Failure::InvalidSerializedField);
        }
        // Preserve encoding bytes and relate every complete fresh-identity
        // token, including occurrences hidden by JSON's last-key-wins parsing.
        // Merely masking tokens would lose references in duplicate fields.
        let format = identity_format(encoded, ids)?;
        fields.insert(
            rule.field.clone(),
            json!({"$shadowSerializedJson": parsed, "$shadowJsonFormat": format}),
        );
    }
    Ok(())
}

fn json_payload_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| json_payload_equal(a, b))
        },
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, a)| b.get(key).is_some_and(|b| json_payload_equal(a, b)))
        },
        _ => left == right,
    }
}

fn identity_format(encoded: &str, ids: &BTreeSet<String>) -> Result<Value, Failure> {
    let bytes = encoded.as_bytes();
    let mut result = Vec::new();
    let mut copied = 0;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'"' {
            index += 1;
            continue;
        }
        let start = index;
        index += 1;
        let mut escaped = false;
        while index < bytes.len() && bytes[index] != b'"' {
            if bytes[index] == b'\\' {
                escaped = true;
                index += 1;
            }
            index += 1;
        }
        // Parsing already established the closing quote and UTF-8 boundaries.
        let decoded;
        let text = if escaped {
            decoded = serde_json::from_str::<String>(&encoded[start..=index])
                .map_err(|_| Failure::InvalidSerializedField)?;
            decoded.as_str()
        } else {
            &encoded[start + 1..index]
        };
        let is_identity = ids.contains(text);
        if encoded[index + 1..].trim_start().starts_with(':') {
            // JSON text can contain reserved Convex encodings and comparator
            // markers which ordinary document fields cannot contain. Reject
            // these even in duplicate fields hidden by last-key-wins parsing:
            // an ID-shaped $bytes scalar is still bytes, not an ID reference.
            if text.starts_with('$') {
                return Err(Failure::InvalidSerializedField);
            }
            if is_identity {
                return Err(Failure::InvalidIdentity);
            }
        }
        // An escaped identity hidden by a duplicate key has no parsed value
        // left to constrain its owner. Literal encoding equality alone would
        // accept the same spelling after ownership changes between lanes.
        if escaped && is_identity {
            return Err(Failure::InvalidSerializedField);
        }
        if is_identity {
            result.push(Value::String(encoded[copied..start].to_owned()));
            result.push(json!({"$shadowEncodedIdentity": &encoded[start + 1..index]}));
            copied = index + 1;
        }
        index += 1;
    }
    result.push(Value::String(encoded[copied..].to_owned()));
    Ok(Value::Array(result))
}

#[cfg(test)]
mod tests {
    use super::{
        identity_format,
        parse_contracts,
    };

    #[test]
    fn serialized_identity_format_preserves_references_hidden_by_duplicate_keys() {
        let ids = ["freshA".to_owned(), "freshB".to_owned()]
            .into_iter()
            .collect();
        let left = r#"{"id":"freshA","id":"freshB"}"#;
        let right = r#"{"id":"freshB","id":"freshB"}"#;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(left).unwrap(),
            serde_json::from_str::<serde_json::Value>(right).unwrap()
        );
        assert_ne!(
            identity_format(left, &ids).unwrap(),
            identity_format(right, &ids).unwrap()
        );
    }

    #[test]
    fn contract_rejects_overlapping_selectors_and_system_field_authority() {
        let empty = serde_json::json!({"serializedFields": [], "opaqueScheduledTokens": []});
        let aliases = serde_json::json!({"members:run": empty, "members.js:run": empty});
        assert!(parse_contracts(&aliases.to_string()).is_err());
        assert!(parse_contracts(&format!(
            r#"{{"members:run":{empty},"members:run":{empty}}}"#,
        ))
        .is_err());
        for rules in [
            serde_json::json!({"serializedFields": [{"table": "_scheduled_jobs", "field": "digest", "payloadFields": ["state"]}], "opaqueScheduledTokens": []}),
            serde_json::json!({"serializedFields": [{"table": "rows", "field": "digest", "payloadFields": ["digest"]}], "opaqueScheduledTokens": []}),
            serde_json::json!({"serializedFields": [{"table": "rows", "field": "first", "payloadFields": ["owner"]}, {"table": "rows", "field": "second", "payloadFields": ["first"]}], "opaqueScheduledTokens": []}),
            serde_json::json!({"serializedFields": [], "opaqueScheduledTokens": [{"function": "jobs:run", "field": "first"}, {"function": "jobs.js:run", "field": "second"}]}),
        ] {
            assert!(
                parse_contracts(&serde_json::json!({"members:run": rules}).to_string()).is_err()
            );
        }
        let valid =
            parse_contracts(&serde_json::json!({"members.js:run": empty}).to_string()).unwrap();
        assert!(valid.contains_key("members:run"));
    }
}
