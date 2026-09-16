use std::os::unix::fs::OpenOptionsExt;

use common::query_journal::QueryJournalLogicalIdentity;

use super::*;

const REQUEST_ENV: &str = "CONVEX_WASM_DATABASE_GET_CONFORMANCE_REQUEST";
const OUTPUT_ENV: &str = "CONVEX_WASM_DATABASE_GET_CONFORMANCE_OUTPUT";
const REQUEST_KIND: &str = "convex-wasm-database-get-native-boundary-request-v1";
const REPORT_KIND: &str = "convex-wasm-database-get-native-boundary-report-v1";
const OPERATION_KIND: &str = "databaseGet";
const EFFECT_EXECUTION_MODE: &str = "guest-promise-event-loop";
const PROVIDER_KIND: &str = "canonical-provider";
const MAX_REQUEST_BYTES: u64 = 128 * 1024;
const MAX_SOURCE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_METADATA_BYTES: u64 = 16 * 1024 * 1024;
const MAX_REPORT_BYTES: usize = 1024 * 1024;
const TARGET_NAMES: [&str; 2] = [
    "static-hermes-wasmtime-optimized",
    "static-hermes-wasmtime-unoptimized",
];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BoundaryRequest {
    kind: String,
    schema_version: u16,
    source_paths: BoundarySourcePaths,
    targets: BTreeMap<String, BoundaryTargetInput>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BoundarySourcePaths {
    conformance: PathBuf,
    runtime_main: PathBuf,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BoundaryTargetInput {
    expectation_path: PathBuf,
    package_directory: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FileIdentity {
    sha256: String,
    size: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderIdentity {
    kind: &'static str,
    source_sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceIdentity {
    canonical_provider: FileIdentity,
    conformance: FileIdentity,
    runtime_main: FileIdentity,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct TargetIdentity {
    artifact: FileIdentity,
    generated_object: FileIdentity,
    provider: ProviderIdentity,
    runtime_object: FileIdentity,
    target_name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactPrecompilerIdentity {
    wasmtime_revision: String,
}

#[derive(Serialize)]
struct CompilerIdentity {
    compiler: JsonValue,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeIdentity {
    artifact_precompiler: ArtifactPrecompilerIdentity,
    compiler: Vec<CompilerIdentity>,
    effect_execution_mode: &'static str,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct Accounting {
    bytes: usize,
    documents: usize,
    intervals: usize,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct CleanupState {
    active_operations: usize,
    active_queries: usize,
    live_opaque_values: usize,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DeveloperErrorRecord {
    category: String,
    kind: &'static str,
    source: &'static str,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryJournalRecord {
    kind: &'static str,
}

#[derive(Clone, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
enum JournalEntry {
    InvocationStart,
    EffectStart {
        operation: &'static str,
    },
    EffectFinish {
        operation: &'static str,
        status: &'static str,
    },
    InvocationFinish {
        outcome: &'static str,
    },
    CleanupFinish {
        active_operations: usize,
        active_queries: usize,
        live_opaque_values: usize,
    },
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct JournalRecord {
    entries: Vec<JournalEntry>,
    query_journal: QueryJournalRecord,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionRecord {
    accounting: Accounting,
    cleanup: CleanupState,
    error: Option<DeveloperErrorRecord>,
    function_result: Option<JsonValue>,
    journal: JournalRecord,
    operation_kind: &'static str,
    outcome: &'static str,
    provider: ProviderIdentity,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderTraceEntry {
    operation: &'static str,
    status: &'static str,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CaseRuntimeIdentity {
    deployment_sha256: String,
    entry_id: String,
    entry_selector_id: String,
    generation_sha256: String,
    package_key: String,
    route_id: String,
    runtime_id: u64,
    runtime_module_path: String,
    runtime_retained: bool,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CaseRecord {
    case_id: &'static str,
    execution: ExecutionRecord,
    provider_trace: Vec<ProviderTraceEntry>,
    runtime: CaseRuntimeIdentity,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ControlRecord {
    control_id: &'static str,
    observed_failure_kind: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TargetRecord {
    cases: Vec<CaseRecord>,
    controls: Vec<ControlRecord>,
    identity: TargetIdentity,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BoundaryReport {
    effect_execution_mode: &'static str,
    kind: &'static str,
    operation_kind: &'static str,
    runtime_identity: RuntimeIdentity,
    schema_version: u16,
    source_identity: SourceIdentity,
    targets: BTreeMap<String, TargetRecord>,
}

struct AuthenticatedTargetMaterial {
    compiler: JsonValue,
    generated_object: FileIdentity,
    runtime_object: FileIdentity,
    wasmtime_revision: String,
}

fn read_bounded(path: &Path, maximum_bytes: u64, description: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(path.is_absolute(), "{description} path must be absolute");
    anyhow::ensure!(
        fs::canonicalize(path)? == path,
        "{description} path must be canonical"
    );
    let metadata = fs::metadata(path)?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() > 0 && metadata.len() <= maximum_bytes,
        "{description} size is outside its boundary"
    );
    fs::read(path).with_context(|| format!("failed to read {description}"))
}

fn canonical_json_bytes(value: &JsonValue) -> anyhow::Result<Vec<u8>> {
    fn write(value: &JsonValue, output: &mut Vec<u8>) -> std::io::Result<()> {
        match value {
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                serde_json::to_writer(output, value)?;
            },
            JsonValue::Array(values) => {
                output.push(b'[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(b',');
                    }
                    write(value, output)?;
                }
                output.push(b']');
            },
            JsonValue::Object(values) => {
                output.push(b'{');
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        output.push(b',');
                    }
                    serde_json::to_writer(&mut *output, key)?;
                    output.push(b':');
                    write(&values[key], output)?;
                }
                output.push(b'}');
            },
        }
        Ok(())
    }

    let mut bytes = Vec::new();
    write(value, &mut bytes)?;
    Ok(bytes)
}

fn read_canonical_request(path: &Path) -> anyhow::Result<BoundaryRequest> {
    let bytes = read_bounded(path, MAX_REQUEST_BYTES, "database-get boundary request")?;
    let value: JsonValue = serde_json::from_slice(&bytes)
        .context("database-get boundary request is not valid JSON")?;
    let mut canonical = canonical_json_bytes(&value)?;
    canonical.push(b'\n');
    anyhow::ensure!(
        bytes == canonical,
        "database-get boundary request must be canonical JSON"
    );
    serde_json::from_value(value).context("database-get boundary request shape is invalid")
}

fn file_identity(bytes: &[u8]) -> anyhow::Result<FileIdentity> {
    Ok(FileIdentity {
        sha256: format!("{:x}", Sha256::digest(bytes)),
        size: u64::try_from(bytes.len()).context("file size does not fit in the report schema")?,
    })
}

fn validate_file_identity(identity: &FileIdentity, description: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        identity.size > 0
            && identity.sha256.len() == 64
            && identity
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{description} identity is invalid"
    );
    Ok(())
}

fn parse_file_identity(value: &JsonValue, description: &str) -> anyhow::Result<FileIdentity> {
    let identity: FileIdentity = serde_json::from_value(value.clone())
        .with_context(|| format!("{description} identity is malformed"))?;
    validate_file_identity(&identity, description)?;
    Ok(identity)
}

fn json_field<'a>(
    value: &'a JsonValue,
    field: &str,
    description: &str,
) -> anyhow::Result<&'a JsonValue> {
    value
        .as_object()
        .and_then(|object| object.get(field))
        .with_context(|| format!("{description} omitted {field}"))
}

fn json_string(value: &JsonValue, description: &str) -> anyhow::Result<String> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("{description} must be a nonempty string"))
}

fn validate_target_optimization(
    target_name: &str,
    core_wasm_identity: &JsonValue,
) -> anyhow::Result<()> {
    let flags = json_field(
        json_field(core_wasm_identity, "staticHermes", "Core Wasm identity")?,
        "applicationFlags",
        "Core Wasm Static Hermes identity",
    )?
    .as_array()
    .context("Core Wasm application flags must be an array")?
    .iter()
    .map(|value| json_string(value, "Core Wasm application flag"))
    .collect::<anyhow::Result<Vec<_>>>()?;
    let optimization_flags = flags
        .iter()
        .filter(|flag| matches!(flag.as_str(), "-O" | "-O0"))
        .map(String::as_str)
        .collect::<Vec<_>>();
    let expected = match target_name {
        "static-hermes-wasmtime-optimized" => "-O",
        "static-hermes-wasmtime-unoptimized" => "-O0",
        _ => anyhow::bail!("database-get boundary target name is unsupported"),
    };
    anyhow::ensure!(
        optimization_flags == [expected],
        "database-get target does not authenticate its optimization mode"
    );
    Ok(())
}

fn authenticate_target_material(
    target_name: &str,
    package_directory: &Path,
    expectation: &NativeCapabilityTestExpectation,
    module_bytes: &[u8],
) -> anyhow::Result<AuthenticatedTargetMaterial> {
    let manifest_bytes = read_bounded(
        &package_directory.join("entry-manifest.json"),
        MAX_METADATA_BYTES,
        "capability entry manifest",
    )?;
    let provenance_bytes = read_bounded(
        &package_directory.join("build-provenance.json"),
        MAX_METADATA_BYTES,
        "capability entry provenance",
    )?;
    let manifest: JsonValue = serde_json::from_slice(&manifest_bytes)
        .context("capability entry manifest is not valid JSON")?;
    let provenance: JsonValue = serde_json::from_slice(&provenance_bytes)
        .context("capability entry provenance is not valid JSON")?;
    let core_wasm_identity = json_field(
        json_field(&provenance, "identities", "capability entry provenance")?,
        "coreWasm",
        "capability entry provenance identities",
    )?;
    validate_target_optimization(target_name, core_wasm_identity)?;
    let members = json_field(core_wasm_identity, "members", "Core Wasm identity")?
        .as_array()
        .context("Core Wasm identity members must be an array")?;
    let primary_member = members
        .iter()
        .find(|member| {
            member
                .get("entryId")
                .and_then(JsonValue::as_str)
                .is_some_and(|entry_id| entry_id == expectation.primary.entry_id)
        })
        .context("Core Wasm identity omitted the selected application member")?;
    let generated_object = parse_file_identity(
        json_field(primary_member, "object", "selected application member")?,
        "generated application object",
    )?;
    let runtime_object = parse_file_identity(
        json_field(
            json_field(&manifest, "runtime", "capability entry manifest")?,
            "mainObject",
            "capability entry runtime",
        )?,
        "runtime object",
    )?;
    let core_runtime_object = parse_file_identity(
        json_field(
            json_field(core_wasm_identity, "runtime", "Core Wasm identity")?,
            "mainObject",
            "Core Wasm runtime identity",
        )?,
        "Core Wasm runtime object",
    )?;
    anyhow::ensure!(
        runtime_object == core_runtime_object,
        "manifest and provenance runtime object identities differ"
    );
    let artifact = file_identity(module_bytes)?;
    anyhow::ensure!(
        artifact.sha256 == expectation.module_sha256 && artifact.size == expectation.module_size,
        "module identity differs from its expectation"
    );
    let execution = json_field(&manifest, "execution", "capability entry manifest")?;
    anyhow::ensure!(
        json_string(
            json_field(
                execution,
                "effectExecutionMode",
                "capability entry execution"
            )?,
            "effect execution mode",
        )? == EFFECT_EXECUTION_MODE,
        "capability entry effect execution mode is unsupported"
    );
    let compiler = json_field(&manifest, "compiler", "capability entry manifest")?.clone();
    let wasmtime_revision = json_string(
        json_field(
            json_field(&manifest, "engine", "capability entry manifest")?,
            "revision",
            "capability entry engine",
        )?,
        "Wasmtime revision",
    )?;
    Ok(AuthenticatedTargetMaterial {
        compiler,
        generated_object,
        runtime_object,
        wasmtime_revision,
    })
}

fn provider_trace(
    output: &GeneratedExecutionOutput<ProdRuntime>,
) -> anyhow::Result<Vec<ProviderTraceEntry>> {
    let FunctionOutcome::Query(outcome) = output
        .invocation
        .function_outcome
        .as_ref()
        .context("database-get invocation did not produce a query outcome")?
    else {
        anyhow::bail!("database-get invocation produced a non-query outcome");
    };
    let entries = outcome
        .host_operation_trace
        .entries()
        .context("database-get host-operation trace is disabled")?;
    entries
        .iter()
        .map(|entry| {
            anyhow::ensure!(
                entry.operation() == LogicalHostOperation::DatabaseGet,
                "database-get invocation reached a different host operation"
            );
            let status = match entry.status() {
                LogicalHostOperationStatus::Success => "success",
                LogicalHostOperationStatus::Failure => "failure",
                LogicalHostOperationStatus::Pending => {
                    anyhow::bail!("database-get provider trace retained a pending operation")
                },
            };
            Ok(ProviderTraceEntry {
                operation: OPERATION_KIND,
                status,
            })
        })
        .collect()
}

fn journal(outcome: &'static str, cleanup: CleanupState) -> JournalRecord {
    let mut entries = vec![JournalEntry::InvocationStart];
    if outcome == "success" {
        entries.extend([
            JournalEntry::EffectStart {
                operation: OPERATION_KIND,
            },
            JournalEntry::EffectFinish {
                operation: OPERATION_KIND,
                status: "success",
            },
        ]);
    }
    entries.extend([
        JournalEntry::InvocationFinish { outcome },
        JournalEntry::CleanupFinish {
            active_operations: cleanup.active_operations,
            active_queries: cleanup.active_queries,
            live_opaque_values: cleanup.live_opaque_values,
        },
    ]);
    JournalRecord {
        entries,
        query_journal: QueryJournalRecord { kind: "empty" },
    }
}

fn case_record(
    case_id: &'static str,
    output: &GeneratedExecutionOutput<ProdRuntime>,
    expectation: &NativeCapabilityTestExpectation,
    provider: ProviderIdentity,
) -> anyhow::Result<CaseRecord> {
    anyhow::ensure!(
        !output.cancelled
            && output.system_error.is_none()
            && output.reusable_instance.is_none()
            && output.memory_permit.is_none()
            && output.invocation.opaque_live_handles == 0
            && output.invocation.opaque_current_bytes == 0
            && output.invocation.operation_count == 1
            && output.invocation.capability_revoked
            && !output.invocation.runtime_reuse_contaminated,
        "database-get invocation cleanup state is incomplete"
    );
    anyhow::ensure!(
        matches!(
            output.invocation.journal.logical_identity(),
            QueryJournalLogicalIdentity::None
        ),
        "database-get invocation unexpectedly produced a query journal"
    );
    let provider_trace = provider_trace(output)?;
    anyhow::ensure!(
        provider_trace.len() == 1,
        "database-get provider trace must contain exactly one operation"
    );
    let (outcome, error) = match &output.invocation.outcome {
        InvocationOutcome::Success => ("success", None),
        InvocationOutcome::DeveloperError(category) => (
            "developer-error",
            Some(DeveloperErrorRecord {
                category: category.clone(),
                kind: "developer-error",
                source: PROVIDER_KIND,
            }),
        ),
        InvocationOutcome::InitializationTimeout
        | InvocationOutcome::ActiveTimeout
        | InvocationOutcome::FuelExhausted
        | InvocationOutcome::SystemTimeout
        | InvocationOutcome::SystemError => {
            anyhow::bail!("database-get invocation did not reach a conformance outcome")
        },
    };
    let expected_trace_status = if outcome == "success" {
        "success"
    } else {
        "failure"
    };
    anyhow::ensure!(
        provider_trace[0].status == expected_trace_status,
        "database-get outcome and provider trace differ"
    );
    match case_id {
        "present-document" => anyhow::ensure!(
            outcome == "success"
                && output.invocation.function_result
                    == Some(json!({ "marker": "backend-gate-document" }))
                && output.invocation.read_accounting.documents == 1,
            "present-document execution differs from the canonical provider result"
        ),
        "missing-document" => anyhow::ensure!(
            outcome == "success"
                && output.invocation.function_result == Some(JsonValue::Null)
                && output.invocation.read_accounting.documents == 0,
            "missing-document execution differs from the canonical provider result"
        ),
        "invalid-document-id" => anyhow::ensure!(
            outcome == "developer-error"
                && error.as_ref().map(|error| error.category.as_str()) == Some("InvalidArgument")
                && output.invocation.function_result.is_none()
                && output.invocation.read_accounting.documents == 0,
            "invalid-document-id execution differs from the canonical provider result"
        ),
        _ => anyhow::bail!("database-get case identity is unsupported"),
    }
    let cleanup = CleanupState {
        active_operations: 0,
        active_queries: 0,
        live_opaque_values: output.invocation.opaque_live_handles,
    };
    Ok(CaseRecord {
        case_id,
        execution: ExecutionRecord {
            accounting: Accounting {
                bytes: output.invocation.read_accounting.bytes,
                documents: output.invocation.read_accounting.documents,
                intervals: output.invocation.read_accounting.intervals,
            },
            cleanup,
            error,
            function_result: output.invocation.function_result.clone(),
            journal: journal(outcome, cleanup),
            operation_kind: OPERATION_KIND,
            outcome,
            provider,
        },
        provider_trace,
        runtime: CaseRuntimeIdentity {
            deployment_sha256: expectation.package_key.clone(),
            entry_id: expectation.primary.entry_id.clone(),
            entry_selector_id: format!("{:016x}", expectation.primary.entry_selector),
            generation_sha256: expectation.package_key.clone(),
            package_key: expectation.package_key.clone(),
            route_id: expectation.primary.route_id.clone(),
            runtime_id: output.runtime_id,
            runtime_module_path: expectation.primary.runtime_module_path.clone(),
            runtime_retained: false,
        },
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectionFailure {
    JournalIncomplete,
    OperationKindMismatch,
}

fn validate_projection(operation_kind: &str, record: &CaseRecord) -> Result<(), ProjectionFailure> {
    if operation_kind != OPERATION_KIND || record.execution.operation_kind != OPERATION_KIND {
        return Err(ProjectionFailure::OperationKindMismatch);
    }
    if !matches!(
        record.execution.journal.entries.last(),
        Some(JournalEntry::CleanupFinish { .. })
    ) {
        return Err(ProjectionFailure::JournalIncomplete);
    }
    Ok(())
}

fn controls(present: &CaseRecord) -> anyhow::Result<Vec<ControlRecord>> {
    validate_projection(OPERATION_KIND, present)
        .map_err(|failure| anyhow::anyhow!("valid database-get projection failed: {failure:?}"))?;
    let mut incomplete = present.clone();
    incomplete.execution.journal.entries.pop();
    anyhow::ensure!(
        validate_projection(OPERATION_KIND, &incomplete)
            == Err(ProjectionFailure::JournalIncomplete),
        "incomplete-journal control did not reach the validator boundary"
    );
    anyhow::ensure!(
        validate_projection("databaseInsert", present)
            == Err(ProjectionFailure::OperationKindMismatch),
        "wrong-operation-kind control did not reach the validator boundary"
    );
    Ok(vec![
        ControlRecord {
            control_id: "incomplete-journal",
            observed_failure_kind: "journal-incomplete",
        },
        ControlRecord {
            control_id: "wrong-operation-kind",
            observed_failure_kind: "operation-kind-mismatch",
        },
    ])
}

async fn run_target(
    rt: ProdRuntime,
    target_name: &str,
    input: BoundaryTargetInput,
    canonical_provider: &FileIdentity,
) -> anyhow::Result<(TargetRecord, AuthenticatedTargetMaterial)> {
    const WASM_PAGE_BYTES: usize = 64 * 1024;

    anyhow::ensure!(
        input.package_directory.is_absolute() && input.expectation_path.is_absolute(),
        "database-get target paths must be absolute"
    );
    anyhow::ensure!(
        fs::canonicalize(&input.package_directory)? == input.package_directory,
        "database-get package directory path must be canonical"
    );
    let expectation_bytes = read_bounded(
        &input.expectation_path,
        MAX_METADATA_BYTES,
        "database-get target expectation",
    )?;
    let expectation = NativeCapabilityTestExpectation::parse(&expectation_bytes)?;
    anyhow::ensure!(
        expectation.primary.handler_udf_kind.manifest_udf_kind() == ManifestUdfKind::Query,
        "database-get selected application entry must be a query"
    );
    let module_bytes = read_bounded(
        &input.package_directory.join("module.wasm"),
        MAX_METADATA_BYTES,
        "database-get Core Wasm module",
    )?;
    expectation.validate_module_bytes(&module_bytes)?;
    let material = authenticate_target_material(
        target_name,
        &input.package_directory,
        &expectation,
        &module_bytes,
    )?;
    let engine = shared_generated_engine()?;
    let runtime = generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?;
    let package = load_capability_entry_package_for_compatibility_test(
        &input.package_directory,
        &expectation.package_key,
        &expectation.primary.entry_id,
        &expectation.primary.route_id,
        &format!("{:016x}", expectation.primary.entry_selector),
        &expectation.primary.entry_path,
        &expectation.primary.runtime_module_path,
        &expectation.primary.handler_export_name,
        ManifestUdfKind::Query,
        "public",
        &runtime,
    )?;
    let module_controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: expectation.package_key.clone(),
        generation_sha256: expectation.package_key.clone(),
        package_key: expectation.package_key.clone(),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    anyhow::ensure!(
        !GENERATED_ROUTED_MODULES
            .lock()
            .contains_key(&route_identity),
        "database-get target route was already cached"
    );
    let routed = cache_generated_routed_module(
        &module_controller,
        package,
        route_identity.clone(),
        engine,
        None,
    )?;
    anyhow::ensure!(
        routed.entry_selector == Some(expectation.primary.entry_selector)
            && routed.package_identity.requires_entry_selector()
            && routed.manifest.value_mode() == ValueMode::GuestNativeJson
            && routed.manifest.effect_execution_mode()
                == EffectExecutionMode::GuestPromiseEventLoop
            && routed.manifest.imported_operations().is_empty(),
        "database-get package loader changed the authenticated execution contract"
    );
    validate_generated_module_contract_with_imports(
        &routed.module,
        &routed.manifest,
        &routed.permitted_conditional_convex_imports,
        true,
    )?;
    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &route_identity,
    );
    let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 2,
        soft_budget_bytes: 960 * WASM_PAGE_BYTES,
        hard_budget_bytes: 1024 * WASM_PAGE_BYTES,
        safety_reserve_bytes: WASM_PAGE_BYTES,
        unattributed_bytes_per_slot: WASM_PAGE_BYTES,
        cold_peak_growth_bytes: 512 * WASM_PAGE_BYTES,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(60),
        pressure_enter_headroom_bytes: 2 * WASM_PAGE_BYTES,
        pressure_exit_headroom_bytes: 4 * WASM_PAGE_BYTES,
    })?;
    controller.set_pressure_for_test(BackendPressure::Healthy {
        headroom_bytes: 1536 * WASM_PAGE_BYTES,
    });
    let database = new_test_database(rt.clone()).await?;
    let table: TableName = "generated_capability_get_documents".parse()?;
    let present_id = insert_document(&database, &table).await?.developer_id;
    let missing_document_id = missing_id(present_id);
    let metrics = Arc::new(GateMetrics::default());
    let provider = ProviderIdentity {
        kind: PROVIDER_KIND,
        source_sha256: canonical_provider.sha256.clone(),
    };
    let mut cases = Vec::new();
    for (case_id, id) in [
        ("present-document", present_id.encode()),
        ("missing-document", missing_document_id.encode()),
        ("invalid-document-id", "not-a-document-id".to_owned()),
    ] {
        let mut state = generated_routed_test_state(
            rt.clone(),
            database.begin_system().await?,
            &routed,
            json!({ "id": id }),
            GeneratedRoutedTestMemorySetup {
                controller: Arc::clone(&controller),
                existing_slot: None,
                memory_identity: memory_identity.clone(),
                values: None,
            },
            Arc::clone(&metrics),
        )
        .await?;
        state.provider.enable_host_operation_trace();
        arm_generated_timeout(
            rt.clone(),
            &mut state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let output = execute_generated_with_entry_selector(
            Arc::clone(&routed),
            Some(expectation.primary.entry_selector),
            state,
            None,
            false,
        )
        .await?;
        let record = case_record(case_id, &output, &expectation, provider.clone())?;
        drop(output.invocation.transaction);
        cases.push(record);
    }
    let memory = controller.snapshot_for_test(&memory_identity);
    anyhow::ensure!(
        memory.active_instances == 0
            && memory.idle_instances == 0
            && memory.evicting_instances == 0
            && memory.retained_baseline_bytes == 0
            && memory.unattributed_allowance_bytes == 0,
        "database-get target retained runtime memory after cleanup"
    );
    database.shutdown().await?;
    let removed = GENERATED_ROUTED_MODULES
        .lock()
        .remove(
            &route_identity,
            "module_cache_evicted_database_get_conformance",
        )
        .context("database-get target module was not cached")?;
    anyhow::ensure!(
        Arc::ptr_eq(&removed, &routed),
        "database-get cleanup removed a different module"
    );
    drop(removed);
    drop(routed);
    anyhow::ensure!(
        module_controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes
            == 0,
        "database-get cleanup retained authenticated module bytes"
    );
    let target_identity = TargetIdentity {
        artifact: file_identity(&module_bytes)?,
        generated_object: material.generated_object.clone(),
        provider,
        runtime_object: material.runtime_object.clone(),
        target_name: target_name.to_owned(),
    };
    let target_controls = controls(
        cases
            .first()
            .context("database-get target produced no cases")?,
    )?;
    Ok((
        TargetRecord {
            cases,
            controls: target_controls,
            identity: target_identity,
        },
        material,
    ))
}

async fn run_boundary(
    rt: ProdRuntime,
    mut request: BoundaryRequest,
) -> anyhow::Result<BoundaryReport> {
    anyhow::ensure!(
        request.kind == REQUEST_KIND && request.schema_version == 1,
        "database-get boundary request kind is unsupported"
    );
    anyhow::ensure!(
        request.targets.len() == TARGET_NAMES.len()
            && TARGET_NAMES
                .iter()
                .all(|target_name| request.targets.contains_key(*target_name)),
        "database-get boundary request must contain both target artifacts"
    );
    let canonical_provider = file_identity(include_bytes!("../async_operations.rs"))?;
    let conformance = file_identity(&read_bounded(
        &request.source_paths.conformance,
        MAX_SOURCE_BYTES,
        "database-get conformance source",
    )?)?;
    let runtime_main = file_identity(&read_bounded(
        &request.source_paths.runtime_main,
        MAX_SOURCE_BYTES,
        "database-get runtime source",
    )?)?;
    let mut targets = BTreeMap::new();
    let mut materials = Vec::new();
    for target_name in TARGET_NAMES {
        let input = request
            .targets
            .remove(target_name)
            .context("database-get target disappeared after validation")?;
        let (target, material) =
            run_target(rt.clone(), target_name, input, &canonical_provider).await?;
        targets.insert(target_name.to_owned(), target);
        materials.push(material);
    }
    let wasmtime_revision = materials
        .first()
        .context("database-get boundary produced no runtime material")?
        .wasmtime_revision
        .clone();
    anyhow::ensure!(
        materials
            .iter()
            .all(|material| material.wasmtime_revision == wasmtime_revision),
        "database-get targets use different Wasmtime revisions"
    );
    let mut compiler_values = BTreeMap::new();
    for material in materials {
        compiler_values.insert(canonical_json_bytes(&material.compiler)?, material.compiler);
    }
    Ok(BoundaryReport {
        effect_execution_mode: EFFECT_EXECUTION_MODE,
        kind: REPORT_KIND,
        operation_kind: OPERATION_KIND,
        runtime_identity: RuntimeIdentity {
            artifact_precompiler: ArtifactPrecompilerIdentity { wasmtime_revision },
            compiler: compiler_values
                .into_values()
                .map(|compiler| CompilerIdentity { compiler })
                .collect(),
            effect_execution_mode: EFFECT_EXECUTION_MODE,
        },
        schema_version: 1,
        source_identity: SourceIdentity {
            canonical_provider,
            conformance,
            runtime_main,
        },
        targets,
    })
}

fn write_report(path: &Path, report: BoundaryReport) -> anyhow::Result<()> {
    anyhow::ensure!(
        path.is_absolute(),
        "database-get report path must be absolute"
    );
    let parent = path
        .parent()
        .context("database-get report path has no parent")?;
    anyhow::ensure!(
        fs::canonicalize(parent)? == parent,
        "database-get report parent path must be canonical"
    );
    let value = serde_json::to_value(report)?;
    let mut bytes = canonical_json_bytes(&value)?;
    bytes.push(b'\n');
    anyhow::ensure!(
        bytes.len() <= MAX_REPORT_BYTES,
        "database-get report exceeds its output boundary"
    );
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .context("failed to create database-get report")?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

#[test]
#[ignore = "requires downstream-built optimized and unoptimized Static Hermes packages"]
fn generated_wasm_database_get_conformance_boundary_exports_canonical_record() -> anyhow::Result<()>
{
    let request_path = std::env::var_os(REQUEST_ENV)
        .map(PathBuf::from)
        .context(format!("{REQUEST_ENV} must identify the boundary request"))?;
    let output_path = std::env::var_os(OUTPUT_ENV)
        .map(PathBuf::from)
        .context(format!("{OUTPUT_ENV} must identify the boundary report"))?;
    let request = read_canonical_request(&request_path)?;
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    let report = block_rt.block_on(
        "generated_wasm_database_get_conformance_boundary",
        run_boundary(rt, request),
    )?;
    write_report(&output_path, report)
}
