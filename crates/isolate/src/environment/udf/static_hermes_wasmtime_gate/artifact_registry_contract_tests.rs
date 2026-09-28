use super::*;

#[cfg(test)]
const REAL_PACKAGE_TEST_ARTIFACT_CACHE_ROOT_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_PACKAGE_TEST_ARTIFACT_CACHE_ROOT";
#[cfg(test)]
const REAL_PACKAGE_TEST_DEPLOYMENT_MANIFEST_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_PACKAGE_TEST_DEPLOYMENT_MANIFEST";
#[cfg(test)]
const REAL_PACKAGE_TEST_EXPECTATION_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_PACKAGE_TEST_EXPECTATION";
#[cfg(test)]
const REAL_MULTI_MEMBER_PACKAGE_TEST_EXPECTATION_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_MULTI_MEMBER_PACKAGE_TEST_EXPECTATION";
#[cfg(test)]
const REAL_MULTI_MEMBER_PACKAGE_TEST_RUNTIME_REGISTRY_ROOT_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_MULTI_MEMBER_PACKAGE_TEST_RUNTIME_REGISTRY_ROOT";
#[cfg(test)]
const REAL_MODULE_GRAPH_TEST_RUNTIME_REGISTRY_ROOT_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_MODULE_GRAPH_TEST_RUNTIME_REGISTRY_ROOT";
#[cfg(test)]
const REAL_MODULE_GRAPH_TEST_EXPECTATION_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_MODULE_GRAPH_TEST_EXPECTATION";
#[cfg(test)]
const REAL_CAPABILITY_ENTRY_PACKAGE_TEST_DIRECTORY_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_CAPABILITY_ENTRY_PACKAGE_TEST_DIRECTORY";
#[cfg(test)]
const REAL_CAPABILITY_ENTRY_PACKAGE_TEST_EXPECTATION_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_REAL_CAPABILITY_ENTRY_PACKAGE_TEST_EXPECTATION";
#[cfg(test)]
pub(super) const NATIVE_CAPABILITY_TEST_PACKAGE_DIRECTORY_ENV: &str =
    "CONVEX_WASM_NATIVE_CAPABILITY_TEST_PACKAGE_DIRECTORY";
#[cfg(test)]
pub(super) const NATIVE_CAPABILITY_TEST_EXPECTATION_ENV: &str =
    "CONVEX_WASM_NATIVE_CAPABILITY_TEST_EXPECTATION";
#[cfg(test)]
const NATIVE_CAPABILITY_TEST_EXPECTATION_KIND: &str =
    "convex-wasm-native-capability-test-expectation-v3";
#[cfg(test)]
const NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_FIXTURE_KIND: &str =
    "convex-wasm-formatter-partial-initialization-fixture-v1";
#[cfg(test)]
pub(super) const NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_ARM_EXPORT: &str =
    "convex_wasm_fixture_arm_formatter_initialization_failure";
#[cfg(test)]
pub(super) const NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_TRACE_EXPORT: &str =
    "convex_wasm_fixture_initialization_trace";
#[cfg(test)]
pub(super) const OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_MODULE_ENV: &str =
    "CONVEX_WASM_OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_MODULE";
#[cfg(test)]
pub(super) const OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_EXPECTATION_ENV: &str =
    "CONVEX_WASM_OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_EXPECTATION";
#[cfg(test)]
const OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_EXPECTATION_KIND: &str =
    "convex-wasm-official-output-chunk-native-validation-expectation-v3";
#[cfg(test)]
const OFFICIAL_OUTPUT_CHUNK_SMOKE_PACKAGE_BINDING_KIND: &str =
    "convex-wasm-official-output-chunk-native-expectation-package-binding-v1";
#[cfg(test)]
const OFFICIAL_OUTPUT_CHUNK_DESCRIPTOR_KIND: &str =
    "convex-wasm-official-output-chunk-native-application-descriptor-v2";
#[cfg(test)]
const OFFICIAL_OUTPUT_CHUNK_DESCRIPTOR_ABI_KIND: &str =
    "convex-wasm-official-output-chunk-native-descriptor-abi-v2";
#[cfg(test)]
const OFFICIAL_OUTPUT_CHUNK_UNIT_KIND: &str = "convex-wasm-official-output-chunk-unit-v1";
#[cfg(test)]
const OFFICIAL_OUTPUT_CHUNK_ENTRY_PUBLICATION_UNIT_KIND: &str =
    "convex-wasm-official-output-chunk-entry-publication-unit-v2";
#[cfg(test)]
const OFFICIAL_OUTPUT_CHUNK_INITIALIZATION_KIND: &str =
    "closed-numbered-chunk-slots-with-per-entry-publication-v1";
#[cfg(test)]
const OFFICIAL_OUTPUT_CHUNK_MAXIMUM_UNITS: usize = 1024;
#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealPackageTestRoute {
    runtime_module_path: String,
    export_name: String,
    udf_kind: ManifestUdfKind,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RealCapabilityEntryPackageTestExpectation {
    pub(super) package_id: String,
    pub(super) entry_id: String,
    pub(super) entry_path: String,
    pub(super) runtime_module_path: String,
    pub(super) routes: Vec<RealCapabilityEntryPackageTestRoute>,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RealCapabilityEntryPackageTestRoute {
    pub(super) route_id: String,
    pub(super) entry_selector_id: String,
    pub(super) export_name: String,
    pub(super) udf_kind: ManifestUdfKind,
    pub(super) visibility: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NativeCapabilityTestExpectationReport {
    abi: NativeCapabilityTestAbiReport,
    execution: WasmUdfExecutionPolicy,
    kind: String,
    module: NativeCapabilityTestModuleReport,
    package_key: String,
    partial_initialization_failure: NativeCapabilityPartialInitializationFailureReport,
    selectors: Vec<NativeCapabilityTestSelectorReport>,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct NativeCapabilityPartialInitializationFailureReport {
    pub(super) application_entry_order: [String; 2],
    pub(super) arm_export_name: String,
    pub(super) failure_status: i32,
    pub(super) failure_trace: i64,
    pub(super) initialization_order: [String; 3],
    pub(super) kind: String,
    pub(super) recovery_trace: i64,
    pub(super) trace_export_name: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct NativeCapabilityTestAbiReport {
    pub(super) capability_request_abi_version: u32,
    pub(super) entry_selector_abi_version: u32,
    pub(super) opaque_value_abi_version: u32,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NativeCapabilityTestModuleReport {
    sha256: String,
    size: u64,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NativeCapabilityTestSelectorReport {
    entry_id: String,
    entry_path: String,
    entry_selector_id: String,
    handler_export_name: String,
    handler_udf_kind: NativeCapabilityTestUdfKind,
    invocation_abi: NativeCapabilityTestInvocationAbi,
    role: NativeCapabilityTestSelectorRole,
    route_id: String,
    runtime_module_path: String,
    visibility: String,
}

#[cfg(test)]
#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
enum NativeCapabilityTestSelectorRole {
    Primary,
    Alternate,
}

#[cfg(test)]
#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) enum NativeCapabilityTestUdfKind {
    Query,
    Mutation,
}

#[cfg(test)]
#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
enum NativeCapabilityTestInvocationAbi {
    #[serde(rename = "convex-wasm-legacy-custom-context-handler-v1")]
    LegacyHandler,
}

#[cfg(test)]
impl NativeCapabilityTestUdfKind {
    pub(super) fn manifest_udf_kind(self) -> ManifestUdfKind {
        match self {
            Self::Query => ManifestUdfKind::Query,
            Self::Mutation => ManifestUdfKind::Mutation,
        }
    }
}

#[cfg(test)]
pub(super) struct NativeCapabilityTestExpectation {
    pub(super) execution: WasmUdfExecutionPolicy,
    pub(super) module_sha256: String,
    pub(super) module_size: u64,
    pub(super) package_key: String,
    pub(super) partial_initialization_failure: NativeCapabilityPartialInitializationFailureReport,
    pub(super) primary: NativeCapabilityTestSelector,
    pub(super) alternate: NativeCapabilityTestSelector,
}

#[cfg(test)]
pub(super) struct NativeCapabilityTestSelector {
    pub(super) entry_id: String,
    pub(super) entry_path: String,
    pub(super) entry_selector: u64,
    pub(super) handler_export_name: String,
    pub(super) handler_udf_kind: NativeCapabilityTestUdfKind,
    pub(super) route_id: String,
    pub(super) runtime_module_path: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeExpectationReport {
    abi: OfficialOutputChunkSmokeAbiReport,
    behavior: OfficialOutputChunkNativeValidationReport,
    descriptor: OfficialOutputChunkSmokeDescriptorReport,
    execution: WasmUdfExecutionPolicy,
    kind: String,
    module: OfficialOutputChunkSmokeModuleReport,
    package_binding: OfficialOutputChunkSmokePackageBindingReport,
    package_key: String,
    selectors: Vec<OfficialOutputChunkSmokeSelectorReport>,
}

#[cfg(test)]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokePackageBindingReport {
    descriptor_identity_sha256: String,
    entries: Vec<OfficialOutputChunkSmokePackageBindingEntryReport>,
    kind: String,
    package_key: String,
    sha256: String,
}

#[cfg(test)]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokePackageBindingEntryReport {
    entry_id: String,
    entry_path: String,
    local_profile_sha256: String,
    module_path: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeAbiReport {
    capability_request_abi_version: u32,
    entry_selector_abi_version: u32,
    opaque_value_abi_version: u32,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeModuleReport {
    sha256: String,
    size: u64,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct OfficialOutputChunkNativeValidationReport {
    pub(super) common_js_cycle_marker: String,
    pub(super) deferred_dynamic_import: OfficialOutputChunkDeferredDynamicImportReport,
    pub(super) late_initialization_failure: OfficialOutputChunkLateInitializationFailureReport,
    pub(super) reselected_primary_expected_result: String,
    pub(super) unselected_top_level_trap_marker: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct OfficialOutputChunkDeferredDynamicImportReport {
    pub(super) arguments: JsonValue,
    pub(super) expected_result: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct OfficialOutputChunkLateInitializationFailureReport {
    pub(super) arguments: JsonValue,
    pub(super) expected_result: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeDescriptorReport {
    entries: Vec<OfficialOutputChunkSmokeDescriptorEntryReport>,
    identity_sha256: String,
    initialization: OfficialOutputChunkSmokeInitializationReport,
    kind: String,
    native_descriptor: OfficialOutputChunkSmokeNativeDescriptorReport,
    units: Vec<OfficialOutputChunkSmokeUnitReport>,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeDescriptorEntryReport {
    dependency_graph_sha256: String,
    entry_module_path: String,
    entry_path: String,
    entry_publication_unit_slot: usize,
    entry_slot: usize,
    handoff_slot: usize,
    module_path: String,
    routes: Vec<OfficialOutputChunkSmokeDescriptorRouteReport>,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeDescriptorRouteReport {
    export_name: String,
    udf_kind: ManifestUdfKind,
    visibility: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeInitializationReport {
    chunk_slot_count: usize,
    entry_publication_unit_slots: Vec<usize>,
    kind: String,
    namespace_slot_count: usize,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeNativeDescriptorReport {
    destruction: String,
    initialization: String,
    kind: String,
    publication: String,
    slots: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeUnitReport {
    application_unit_slot: usize,
    chunk_slot: i32,
    dependencies: Vec<OfficialOutputChunkSmokeDependencyReport>,
    entry_publication: bool,
    entry_symbol: String,
    exported_unit_name: String,
    identity_sha256: String,
    javascript: OfficialOutputChunkSmokeJavascriptReport,
    kind: String,
    publication_handoff_slot: i32,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeDependencyReport {
    kind: String,
    path: String,
    slot: usize,
    specifier: String,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeJavascriptReport {
    sha256: String,
    size: u64,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum OfficialOutputChunkSmokeSelectorRole {
    Primary,
    Secondary,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OfficialOutputChunkSmokeSelectorReport {
    entry_id: String,
    entry_path: String,
    entry_selector_id: String,
    entry_symbol: String,
    expected_result: String,
    export_name: String,
    module_path: String,
    role: OfficialOutputChunkSmokeSelectorRole,
    route_id: String,
    udf_kind: ManifestUdfKind,
}

#[cfg(test)]
pub(super) struct OfficialOutputChunkSmokeExpectation {
    pub(super) behavior: OfficialOutputChunkNativeValidationReport,
    pub(super) module_sha256: String,
    pub(super) module_size: u64,
    pub(super) package_key: String,
    pub(super) primary: OfficialOutputChunkSmokeSelector,
    pub(super) secondary: OfficialOutputChunkSmokeSelector,
}

#[cfg(test)]
pub(super) struct OfficialOutputChunkSmokeSelector {
    pub(super) entry_id: String,
    pub(super) entry_path: String,
    pub(super) entry_selector: u64,
    pub(super) expected_result: String,
    pub(super) export_name: String,
    pub(super) module_path: String,
    pub(super) route_id: String,
}

#[cfg(test)]
fn official_output_chunk_smoke_package_binding_projection(
    binding: &OfficialOutputChunkSmokePackageBindingReport,
) -> JsonValue {
    json!({
        "descriptorIdentitySha256": binding.descriptor_identity_sha256,
        "entries": binding
            .entries
            .iter()
            .map(|entry| json!({
                "entryId": entry.entry_id,
                "entryPath": entry.entry_path,
                "localProfileSha256": entry.local_profile_sha256,
                "modulePath": entry.module_path,
            }))
            .collect::<Vec<_>>(),
        "kind": binding.kind,
        "packageKey": binding.package_key,
    })
}

#[cfg(test)]
fn validate_official_output_chunk_smoke_package_binding_digest(
    binding: &OfficialOutputChunkSmokePackageBindingReport,
) -> anyhow::Result<()> {
    let actual = canonical_json_sha256(&official_output_chunk_smoke_package_binding_projection(
        binding,
    ))
    .map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        actual == binding.sha256,
        "official-output chunk smoke package binding digest does not authenticate its payload"
    );
    Ok(())
}

#[cfg(test)]
fn official_output_chunk_smoke_package_binding_entry_id(
    binding: &OfficialOutputChunkSmokePackageBindingEntryReport,
) -> anyhow::Result<String> {
    canonical_json_sha256(&json!({
        "domain": "convex-wasm-capability-entry-v1",
        "invocationAbi": "convex-sdk-registration-wrapper-tagged-json-v1",
        "localProfileSha256": binding.local_profile_sha256,
        "selectedEntry": {
            "entryPath": binding.entry_path,
            "modulePath": binding.module_path,
        },
    }))
    .map_err(anyhow::Error::msg)
}

#[cfg(test)]
fn official_output_chunk_smoke_entry_symbol(entry_id: &str) -> String {
    format!("sh_export_convex_wasm_entry_{entry_id}")
}

#[cfg(test)]
fn validate_official_output_chunk_smoke_package_binding_semantics(
    binding: &OfficialOutputChunkSmokePackageBindingReport,
    descriptor_entries: &[OfficialOutputChunkSmokeDescriptorEntryReport],
    selectors: &[OfficialOutputChunkSmokeSelectorReport],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        binding.entries.len() == descriptor_entries.len()
            && binding.entries.len() == selectors.len(),
        "official-output chunk smoke package binding, descriptor, and selector membership differ"
    );
    let mut descriptors_by_entry_path = BTreeMap::new();
    for entry in descriptor_entries {
        anyhow::ensure!(
            descriptors_by_entry_path
                .insert(entry.entry_path.as_str(), entry)
                .is_none(),
            "official-output chunk smoke descriptor repeats an entry path"
        );
    }
    let mut selectors_by_entry_path = BTreeMap::new();
    for selector in selectors {
        anyhow::ensure!(
            selectors_by_entry_path
                .insert(selector.entry_path.as_str(), selector)
                .is_none(),
            "official-output chunk smoke selectors repeat an entry path"
        );
    }

    let mut bound_entry_ids = BTreeSet::new();
    let mut bound_entry_paths = BTreeSet::new();
    for entry in &binding.entries {
        validate_official_output_chunk_smoke_lowercase_hex(
            "packageBinding.entries.entryId",
            &entry.entry_id,
            64,
        )?;
        validate_official_output_chunk_smoke_lowercase_hex(
            "packageBinding.entries.localProfileSha256",
            &entry.local_profile_sha256,
            64,
        )?;
        let descriptor = descriptors_by_entry_path
            .get(entry.entry_path.as_str())
            .context(
                "official-output chunk smoke package binding entry is absent from the descriptor",
            )?;
        let selector = selectors_by_entry_path
            .get(entry.entry_path.as_str())
            .context(
                "official-output chunk smoke package binding entry has no matching selector",
            )?;
        anyhow::ensure!(
            bound_entry_ids.insert(entry.entry_id.as_str())
                && bound_entry_paths.insert(entry.entry_path.as_str())
                && descriptor.module_path == entry.module_path
                && descriptor.entry_module_path == format!("{}.js", entry.module_path)
                && selector.entry_id == entry.entry_id
                && selector.entry_path == entry.entry_path
                && selector.module_path == entry.module_path
                && selector.entry_symbol
                    == official_output_chunk_smoke_entry_symbol(&entry.entry_id),
            "official-output chunk smoke package binding entry disagrees with its descriptor or \
             selector"
        );
        anyhow::ensure!(
            entry.entry_id == official_output_chunk_smoke_package_binding_entry_id(entry)?,
            "official-output chunk smoke package binding entry ID does not authenticate its local \
             profile and selected descriptor entry"
        );
    }
    Ok(())
}

#[cfg(test)]
impl NativeCapabilityTestExpectation {
    pub(super) fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        let report: NativeCapabilityTestExpectationReport = serde_json::from_slice(bytes)
            .context("native capability test expectation report is malformed")?;
        anyhow::ensure!(
            report.kind == NATIVE_CAPABILITY_TEST_EXPECTATION_KIND,
            "native capability test expectation report kind is unsupported"
        );
        anyhow::ensure!(
            matches!(report.abi.capability_request_abi_version, 1 | 2 | 3 | 4)
                && report.abi.entry_selector_abi_version == 2
                && report.abi.opaque_value_abi_version == OPAQUE_VALUE_ABI_VERSION,
            "native capability test expectation ABI is unsupported"
        );
        anyhow::ensure!(
            report.execution.value_mode() == ValueMode::GuestNativeJson
                && report.execution.effect_execution_mode()
                    == EffectExecutionMode::GuestPromiseEventLoop
                && report.execution.imported_operations().is_empty()
                && report.execution.value_codec().is_some()
                && report.execution.request_envelope().is_some(),
            "native capability test expectation execution policy is not a capability-entry policy"
        );
        anyhow::ensure!(
            report.module.size > 0,
            "native capability test expectation module size must be greater than zero"
        );
        anyhow::ensure!(
            report.partial_initialization_failure.kind
                == NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_FIXTURE_KIND
                && report.partial_initialization_failure.arm_export_name
                    == NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_ARM_EXPORT
                && report.partial_initialization_failure.trace_export_name
                    == NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_TRACE_EXPORT
                && report.partial_initialization_failure.failure_status == 62
                && report.partial_initialization_failure.failure_trace == 0x0105
                && report.partial_initialization_failure.recovery_trace == 0x0102_0304
                && report
                    .partial_initialization_failure
                    .application_entry_order
                    == [
                        "convex/nativeCapabilityApplicationA.ts",
                        "convex/nativeCapabilityApplicationB.ts",
                    ]
                && report.partial_initialization_failure.initialization_order
                    == [
                        "shared-typed-capability-bridge",
                        "shared-untyped-runtime-support",
                        "untyped-applications-by-entry-slot",
                    ],
            "native capability partial-initialization fixture contract is invalid"
        );
        validate_native_capability_test_lowercase_hex("module.sha256", &report.module.sha256, 64)?;
        validate_native_capability_test_lowercase_hex("packageKey", &report.package_key, 64)?;
        anyhow::ensure!(
            report.selectors.len() == 2,
            "native capability test expectation must contain exactly two selectors"
        );

        let mut entry_selector_ids = BTreeSet::new();
        let mut primary = None;
        let mut alternate = None;
        for selector in report.selectors {
            let selector_role = selector.role;
            validate_native_capability_test_lowercase_hex(
                "selectors.entryId",
                &selector.entry_id,
                64,
            )?;
            validate_native_capability_test_lowercase_hex(
                "selectors.entrySelectorId",
                &selector.entry_selector_id,
                16,
            )?;
            validate_native_capability_test_lowercase_hex(
                "selectors.routeId",
                &selector.route_id,
                64,
            )?;
            anyhow::ensure!(
                entry_selector_ids.insert(selector.entry_selector_id.clone()),
                "native capability test expectation contains duplicate entry selector IDs"
            );
            validate_native_capability_test_handler_export_name(&selector.handler_export_name)?;
            validate_native_capability_test_nonempty("selectors.entryPath", &selector.entry_path)?;
            validate_native_capability_test_nonempty(
                "selectors.runtimeModulePath",
                &selector.runtime_module_path,
            )?;
            anyhow::ensure!(
                selector.handler_udf_kind == NativeCapabilityTestUdfKind::Query,
                "native capability test currently supports only query handlers"
            );
            anyhow::ensure!(
                selector.invocation_abi == NativeCapabilityTestInvocationAbi::LegacyHandler,
                "native capability test requires the legacy custom-context invocation ABI"
            );
            anyhow::ensure!(
                selector.visibility == "public",
                "native capability test currently supports only public handlers"
            );
            let selector = NativeCapabilityTestSelector {
                entry_id: selector.entry_id,
                entry_path: selector.entry_path,
                entry_selector: u64::from_str_radix(&selector.entry_selector_id, 16)
                    .context("native capability test entry selector ID is invalid")?,
                handler_export_name: selector.handler_export_name,
                handler_udf_kind: selector.handler_udf_kind,
                route_id: selector.route_id,
                runtime_module_path: selector.runtime_module_path,
            };
            let slot = match selector_role {
                NativeCapabilityTestSelectorRole::Primary => &mut primary,
                NativeCapabilityTestSelectorRole::Alternate => &mut alternate,
            };
            anyhow::ensure!(
                slot.replace(selector).is_none(),
                "native capability test expectation contains a duplicate selector role"
            );
        }
        let primary = primary.context(
            "native capability test expectation must contain exactly one primary selector",
        )?;
        let alternate = alternate.context(
            "native capability test expectation must contain exactly one alternate selector",
        )?;
        anyhow::ensure!(
            primary.entry_id != alternate.entry_id
                && primary.entry_path != alternate.entry_path
                && primary.entry_selector != alternate.entry_selector
                && primary.handler_export_name != alternate.handler_export_name
                && primary.route_id != alternate.route_id
                && primary.runtime_module_path != alternate.runtime_module_path,
            "native capability test selectors must identify distinct entries and routes"
        );
        Ok(Self {
            execution: report.execution,
            module_sha256: report.module.sha256,
            module_size: report.module.size,
            package_key: report.package_key,
            partial_initialization_failure: report.partial_initialization_failure,
            primary,
            alternate,
        })
    }

    pub(super) fn validate_module_bytes(&self, module_bytes: &[u8]) -> anyhow::Result<()> {
        let actual_size = u64::try_from(module_bytes.len())
            .context("native capability module size does not fit in the report schema")?;
        anyhow::ensure!(
            actual_size == self.module_size,
            "native capability module size does not match the expectation report"
        );
        let actual_sha256 = format!("{:x}", Sha256::digest(module_bytes));
        anyhow::ensure!(
            actual_sha256 == self.module_sha256,
            "native capability module SHA-256 does not match the expectation report"
        );
        Ok(())
    }
}

#[cfg(test)]
impl OfficialOutputChunkSmokeExpectation {
    pub(super) fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        let report: OfficialOutputChunkSmokeExpectationReport = serde_json::from_slice(bytes)
            .context("official-output chunk smoke expectation report is malformed")?;
        anyhow::ensure!(
            report.kind == OFFICIAL_OUTPUT_CHUNK_SMOKE_TEST_EXPECTATION_KIND,
            "official-output chunk smoke expectation report kind is unsupported"
        );
        anyhow::ensure!(
            matches!(report.abi.capability_request_abi_version, 1 | 2 | 3 | 4)
                && report.abi.entry_selector_abi_version == 2
                && report.abi.opaque_value_abi_version == OPAQUE_VALUE_ABI_VERSION,
            "official-output chunk smoke expectation ABI is unsupported"
        );
        anyhow::ensure!(
            report.execution.value_mode() == ValueMode::GuestNativeJson
                && report.execution.effect_execution_mode()
                    == EffectExecutionMode::GuestPromiseEventLoop
                && report.execution.imported_operations().is_empty()
                && report.execution.value_codec().is_some()
                && report.execution.request_envelope().is_some(),
            "official-output chunk smoke expectation execution policy is not a capability-entry \
             policy"
        );
        anyhow::ensure!(
            report.module.size > 0,
            "official-output chunk smoke expectation module size must be greater than zero"
        );
        validate_official_output_chunk_smoke_lowercase_hex(
            "module.sha256",
            &report.module.sha256,
            64,
        )?;
        validate_official_output_chunk_smoke_lowercase_hex("packageKey", &report.package_key, 64)?;
        validate_official_output_chunk_smoke_descriptor(&report.descriptor)?;
        anyhow::ensure!(
            report.package_binding.kind == OFFICIAL_OUTPUT_CHUNK_SMOKE_PACKAGE_BINDING_KIND
                && report.package_binding.package_key == report.package_key
                && report.package_binding.descriptor_identity_sha256
                    == report.descriptor.identity_sha256
                && report.package_binding.entries.len() == report.descriptor.entries.len(),
            "official-output chunk smoke package binding does not identify its descriptor and \
             loaded package"
        );
        validate_official_output_chunk_smoke_lowercase_hex(
            "packageBinding.sha256",
            &report.package_binding.sha256,
            64,
        )?;
        validate_official_output_chunk_smoke_package_binding_digest(&report.package_binding)?;
        validate_official_output_chunk_smoke_package_binding_semantics(
            &report.package_binding,
            &report.descriptor.entries,
            &report.selectors,
        )?;
        validate_official_output_chunk_native_validation(&report.behavior)?;
        anyhow::ensure!(
            report.selectors.len() == 2,
            "official-output chunk smoke expectation must contain exactly two selectors"
        );

        let mut selector_ids = BTreeSet::new();
        let mut primary = None;
        let mut secondary = None;
        for selector in report.selectors {
            validate_official_output_chunk_smoke_lowercase_hex(
                "selectors.entryId",
                &selector.entry_id,
                64,
            )?;
            validate_official_output_chunk_smoke_lowercase_hex(
                "selectors.entrySelectorId",
                &selector.entry_selector_id,
                16,
            )?;
            validate_official_output_chunk_smoke_lowercase_hex(
                "selectors.routeId",
                &selector.route_id,
                64,
            )?;
            anyhow::ensure!(
                selector_ids.insert(selector.entry_selector_id.clone()),
                "official-output chunk smoke expectation contains duplicate entry selector IDs"
            );
            validate_official_output_chunk_smoke_identifier(
                "selectors.entrySymbol",
                &selector.entry_symbol,
            )?;
            validate_official_output_chunk_smoke_identifier(
                "selectors.exportName",
                &selector.export_name,
            )?;
            validate_official_output_chunk_smoke_nonempty(
                "selectors.entryPath",
                &selector.entry_path,
            )?;
            validate_official_output_chunk_smoke_nonempty(
                "selectors.modulePath",
                &selector.module_path,
            )?;
            validate_official_output_chunk_smoke_nonempty(
                "selectors.expectedResult",
                &selector.expected_result,
            )?;
            anyhow::ensure!(
                selector.udf_kind == ManifestUdfKind::Query,
                "official-output chunk smoke supports only query handlers"
            );
            let entry = report
                .descriptor
                .entries
                .iter()
                .find(|entry| entry.entry_path == selector.entry_path)
                .context(
                    "official-output chunk smoke selector does not identify a descriptor entry",
                )?;
            anyhow::ensure!(
                entry.module_path == selector.module_path,
                "official-output chunk smoke selector module path disagrees with its descriptor \
                 entry"
            );
            anyhow::ensure!(
                entry.routes.iter().any(|route| {
                    route.export_name == selector.export_name
                        && route.udf_kind == selector.udf_kind
                        && route.visibility == "public"
                }),
                "official-output chunk smoke selector does not identify a public descriptor route"
            );
            let normalized = OfficialOutputChunkSmokeSelector {
                entry_id: selector.entry_id,
                entry_path: selector.entry_path,
                entry_selector: u64::from_str_radix(&selector.entry_selector_id, 16)
                    .context("official-output chunk smoke entry selector ID is invalid")?,
                expected_result: selector.expected_result,
                export_name: selector.export_name,
                module_path: selector.module_path,
                route_id: selector.route_id,
            };
            let slot = match selector.role {
                OfficialOutputChunkSmokeSelectorRole::Primary => &mut primary,
                OfficialOutputChunkSmokeSelectorRole::Secondary => &mut secondary,
            };
            anyhow::ensure!(
                slot.replace(normalized).is_none(),
                "official-output chunk smoke expectation contains a duplicate selector role"
            );
        }
        let primary = primary
            .context("official-output chunk smoke expectation must contain one primary selector")?;
        let secondary = secondary.context(
            "official-output chunk smoke expectation must contain one secondary selector",
        )?;
        anyhow::ensure!(
            primary.entry_id != secondary.entry_id
                && primary.entry_selector != secondary.entry_selector
                && primary.expected_result != secondary.expected_result
                && primary.route_id != secondary.route_id,
            "official-output chunk smoke selectors must identify distinct entries, routes, and \
             results"
        );
        anyhow::ensure!(
            report.behavior.deferred_dynamic_import.expected_result != secondary.expected_result
                && report.behavior.late_initialization_failure.expected_result
                    != secondary.expected_result
                && report.behavior.deferred_dynamic_import.expected_result
                    != report.behavior.late_initialization_failure.expected_result
                && report.behavior.reselected_primary_expected_result != primary.expected_result
                && primary
                    .expected_result
                    .contains(&report.behavior.common_js_cycle_marker)
                && report
                    .behavior
                    .reselected_primary_expected_result
                    .contains(&report.behavior.common_js_cycle_marker)
                && primary
                    .expected_result
                    .contains(&report.behavior.unselected_top_level_trap_marker)
                && report
                    .behavior
                    .reselected_primary_expected_result
                    .contains(&report.behavior.unselected_top_level_trap_marker)
                && secondary
                    .expected_result
                    .contains(&report.behavior.unselected_top_level_trap_marker)
                && report
                    .behavior
                    .deferred_dynamic_import
                    .expected_result
                    .contains(&report.behavior.unselected_top_level_trap_marker)
                && report
                    .behavior
                    .late_initialization_failure
                    .expected_result
                    .contains(&report.behavior.unselected_top_level_trap_marker),
            "official-output chunk native validation expectations are inconsistent"
        );
        Ok(Self {
            behavior: report.behavior,
            module_sha256: report.module.sha256,
            module_size: report.module.size,
            package_key: report.package_key,
            primary,
            secondary,
        })
    }

    pub(super) fn validate_module_bytes(&self, module_bytes: &[u8]) -> anyhow::Result<()> {
        let actual_size = u64::try_from(module_bytes.len())
            .context("official-output chunk smoke module size does not fit in the report schema")?;
        anyhow::ensure!(
            actual_size == self.module_size,
            "official-output chunk smoke module size does not match the expectation report"
        );
        anyhow::ensure!(
            format!("{:x}", Sha256::digest(module_bytes)) == self.module_sha256,
            "official-output chunk smoke module SHA-256 does not match the expectation report"
        );
        Ok(())
    }
}

#[cfg(test)]
#[test]
fn official_output_chunk_smoke_package_binding_authenticates_semantic_entry_identity(
) -> anyhow::Result<()> {
    let mut binding = OfficialOutputChunkSmokePackageBindingReport {
        descriptor_identity_sha256: "a".repeat(64),
        entries: vec![
            OfficialOutputChunkSmokePackageBindingEntryReport {
                entry_id: String::new(),
                entry_path: "convex/first.ts".to_owned(),
                local_profile_sha256: "c".repeat(64),
                module_path: "first".to_owned(),
            },
            OfficialOutputChunkSmokePackageBindingEntryReport {
                entry_id: String::new(),
                entry_path: "convex/second.ts".to_owned(),
                local_profile_sha256: "e".repeat(64),
                module_path: "second".to_owned(),
            },
        ],
        kind: OFFICIAL_OUTPUT_CHUNK_SMOKE_PACKAGE_BINDING_KIND.to_owned(),
        package_key: "f".repeat(64),
        sha256: String::new(),
    };
    for entry in &mut binding.entries {
        entry.entry_id = official_output_chunk_smoke_package_binding_entry_id(entry)?;
    }
    binding.sha256 = canonical_json_sha256(
        &official_output_chunk_smoke_package_binding_projection(&binding),
    )
    .map_err(anyhow::Error::msg)?;
    let descriptor_entries = vec![
        OfficialOutputChunkSmokeDescriptorEntryReport {
            dependency_graph_sha256: "1".repeat(64),
            entry_module_path: "first.js".to_owned(),
            entry_path: "convex/first.ts".to_owned(),
            entry_publication_unit_slot: 2,
            entry_slot: 0,
            handoff_slot: 0,
            module_path: "first".to_owned(),
            routes: vec![OfficialOutputChunkSmokeDescriptorRouteReport {
                export_name: "first".to_owned(),
                udf_kind: ManifestUdfKind::Query,
                visibility: "public".to_owned(),
            }],
        },
        OfficialOutputChunkSmokeDescriptorEntryReport {
            dependency_graph_sha256: "2".repeat(64),
            entry_module_path: "second.js".to_owned(),
            entry_path: "convex/second.ts".to_owned(),
            entry_publication_unit_slot: 3,
            entry_slot: 1,
            handoff_slot: 1,
            module_path: "second".to_owned(),
            routes: vec![OfficialOutputChunkSmokeDescriptorRouteReport {
                export_name: "second".to_owned(),
                udf_kind: ManifestUdfKind::Query,
                visibility: "public".to_owned(),
            }],
        },
    ];
    let selectors = vec![
        OfficialOutputChunkSmokeSelectorReport {
            entry_id: binding.entries[0].entry_id.clone(),
            entry_path: binding.entries[0].entry_path.clone(),
            entry_selector_id: "1".repeat(16),
            entry_symbol: official_output_chunk_smoke_entry_symbol(&binding.entries[0].entry_id),
            expected_result: "first".to_owned(),
            export_name: "first".to_owned(),
            module_path: binding.entries[0].module_path.clone(),
            role: OfficialOutputChunkSmokeSelectorRole::Primary,
            route_id: "3".repeat(64),
            udf_kind: ManifestUdfKind::Query,
        },
        OfficialOutputChunkSmokeSelectorReport {
            entry_id: binding.entries[1].entry_id.clone(),
            entry_path: binding.entries[1].entry_path.clone(),
            entry_selector_id: "2".repeat(16),
            entry_symbol: official_output_chunk_smoke_entry_symbol(&binding.entries[1].entry_id),
            expected_result: "second".to_owned(),
            export_name: "second".to_owned(),
            module_path: binding.entries[1].module_path.clone(),
            role: OfficialOutputChunkSmokeSelectorRole::Secondary,
            route_id: "4".repeat(64),
            udf_kind: ManifestUdfKind::Query,
        },
    ];
    validate_official_output_chunk_smoke_package_binding_digest(&binding)?;
    validate_official_output_chunk_smoke_package_binding_semantics(
        &binding,
        &descriptor_entries,
        &selectors,
    )?;

    let mut stale_digest = binding.clone();
    stale_digest.entries[0].local_profile_sha256 = "0".repeat(64);
    let error = validate_official_output_chunk_smoke_package_binding_digest(&stale_digest)
        .expect_err("changed package-binding payload retained its prior digest");
    assert!(error.to_string().contains(
        "official-output chunk smoke package binding digest does not authenticate its payload"
    ));

    let mut forged = binding;
    forged.entries[0].local_profile_sha256 = "0".repeat(64);
    forged.entries[0].entry_id =
        official_output_chunk_smoke_package_binding_entry_id(&forged.entries[0])?;
    forged.sha256 = canonical_json_sha256(&official_output_chunk_smoke_package_binding_projection(
        &forged,
    ))
    .map_err(anyhow::Error::msg)?;
    validate_official_output_chunk_smoke_package_binding_digest(&forged)?;
    let error = validate_official_output_chunk_smoke_package_binding_semantics(
        &forged,
        &descriptor_entries,
        &selectors,
    )
    .expect_err("forged and re-signed package binding retained selector authority");
    assert!(error
        .to_string()
        .contains("disagrees with its descriptor or selector"));
    Ok(())
}

#[cfg(test)]
fn validate_official_output_chunk_native_validation(
    behavior: &OfficialOutputChunkNativeValidationReport,
) -> anyhow::Result<()> {
    for (field, value) in [
        (
            "behavior.commonJsCycleMarker",
            &behavior.common_js_cycle_marker,
        ),
        (
            "behavior.deferredDynamicImport.expectedResult",
            &behavior.deferred_dynamic_import.expected_result,
        ),
        (
            "behavior.lateInitializationFailure.expectedResult",
            &behavior.late_initialization_failure.expected_result,
        ),
        (
            "behavior.reselectedPrimaryExpectedResult",
            &behavior.reselected_primary_expected_result,
        ),
        (
            "behavior.unselectedTopLevelTrapMarker",
            &behavior.unselected_top_level_trap_marker,
        ),
    ] {
        validate_official_output_chunk_smoke_nonempty(field, value)?;
    }
    anyhow::ensure!(
        behavior
            .deferred_dynamic_import
            .arguments
            .as_object()
            .is_some_and(|arguments| !arguments.is_empty())
            && behavior
                .late_initialization_failure
                .arguments
                .as_object()
                .is_some_and(|arguments| !arguments.is_empty())
            && behavior.deferred_dynamic_import.arguments
                != behavior.late_initialization_failure.arguments,
        "official-output chunk native validation arguments are invalid"
    );
    Ok(())
}

#[cfg(test)]
fn validate_official_output_chunk_smoke_descriptor(
    descriptor: &OfficialOutputChunkSmokeDescriptorReport,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        descriptor.kind == OFFICIAL_OUTPUT_CHUNK_DESCRIPTOR_KIND,
        "official-output chunk smoke descriptor kind is unsupported"
    );
    validate_official_output_chunk_smoke_lowercase_hex(
        "descriptor.identitySha256",
        &descriptor.identity_sha256,
        64,
    )?;
    anyhow::ensure!(
        descriptor.initialization.kind == OFFICIAL_OUTPUT_CHUNK_INITIALIZATION_KIND
            && descriptor.initialization.namespace_slot_count >= 1
            && descriptor.initialization.chunk_slot_count
                == descriptor.initialization.namespace_slot_count
            && descriptor.initialization.entry_publication_unit_slots.len()
                == descriptor.entries.len()
            && descriptor
                .initialization
                .entry_publication_unit_slots
                .iter()
                .enumerate()
                .all(|(handoff_slot, unit_slot)| {
                    *unit_slot == descriptor.initialization.chunk_slot_count + handoff_slot
                }),
        "official-output chunk smoke initialization descriptor is invalid"
    );
    anyhow::ensure!(
        descriptor.native_descriptor.kind == OFFICIAL_OUTPUT_CHUNK_DESCRIPTOR_ABI_KIND
            && descriptor.native_descriptor.destruction
                == "destroy-store-on-any-initialization-failure"
            && descriptor.native_descriptor.initialization
                == "recursive-closed-literal-require-with-provisional-cjs-namespaces"
            && descriptor.native_descriptor.publication
                == "authenticated-selected-entry-wrapper-validation-after-selected-closure-initialization"
            && descriptor.native_descriptor.slots
                == "closed-numbered-namespace-slots-with-per-entry-publication-units",
        "official-output chunk smoke native descriptor ABI is invalid"
    );
    anyhow::ensure!(
        descriptor.entries.len() == 2,
        "official-output chunk smoke descriptor must contain exactly two entries"
    );
    anyhow::ensure!(
        descriptor.units.len() >= 2
            && descriptor.units.len() <= OFFICIAL_OUTPUT_CHUNK_MAXIMUM_UNITS
            && descriptor.units.len()
                == descriptor.initialization.namespace_slot_count + descriptor.entries.len(),
        "official-output chunk smoke descriptor unit count is invalid"
    );

    let chunk_count = descriptor.initialization.chunk_slot_count;
    let mut entry_paths = BTreeSet::new();
    let mut entry_slots = BTreeSet::new();
    let mut previous_entry_path = None;
    for (entry_index, entry) in descriptor.entries.iter().enumerate() {
        validate_official_output_chunk_smoke_lowercase_hex(
            "descriptor.entries.dependencyGraphSha256",
            &entry.dependency_graph_sha256,
            64,
        )?;
        for (field, value) in [
            (
                "descriptor.entries.entryModulePath",
                &entry.entry_module_path,
            ),
            ("descriptor.entries.entryPath", &entry.entry_path),
            ("descriptor.entries.modulePath", &entry.module_path),
        ] {
            validate_official_output_chunk_smoke_nonempty(field, value)?;
        }
        anyhow::ensure!(
            entry.handoff_slot == entry_index
                && entry.entry_slot < chunk_count
                && entry.entry_publication_unit_slot
                    == descriptor.initialization.entry_publication_unit_slots[entry_index]
                && entry_slots.insert(entry.entry_slot)
                && entry_paths.insert(entry.entry_path.clone())
                && previous_entry_path
                    .as_ref()
                    .is_none_or(|previous: &String| previous < &entry.entry_path),
            "official-output chunk smoke descriptor entry slots or order are invalid"
        );
        previous_entry_path = Some(entry.entry_path.clone());
        anyhow::ensure!(
            entry.routes.len() == 1,
            "official-output chunk smoke descriptor entries must each contain one route"
        );
        let route = &entry.routes[0];
        validate_official_output_chunk_smoke_identifier(
            "descriptor.entries.routes.exportName",
            &route.export_name,
        )?;
        anyhow::ensure!(
            route.udf_kind == ManifestUdfKind::Query && route.visibility == "public",
            "official-output chunk smoke descriptor route is not a public query"
        );
    }

    let mut unit_identities = BTreeSet::new();
    for (unit_index, unit) in descriptor.units.iter().enumerate() {
        anyhow::ensure!(
            unit.application_unit_slot == unit_index,
            "official-output chunk smoke unit application slot is invalid"
        );
        validate_official_output_chunk_smoke_lowercase_hex(
            "descriptor.units.identitySha256",
            &unit.identity_sha256,
            64,
        )?;
        anyhow::ensure!(
            unit_identities.insert(unit.identity_sha256.clone()),
            "official-output chunk smoke descriptor repeats a unit identity"
        );
        validate_official_output_chunk_smoke_lowercase_hex(
            "descriptor.units.javascript.sha256",
            &unit.javascript.sha256,
            64,
        )?;
        anyhow::ensure!(
            unit.javascript.size > 0,
            "official-output chunk smoke descriptor JavaScript size is invalid"
        );
        validate_official_output_chunk_smoke_identifier(
            "descriptor.units.entrySymbol",
            &unit.entry_symbol,
        )?;
        validate_official_output_chunk_smoke_identifier(
            "descriptor.units.exportedUnitName",
            &unit.exported_unit_name,
        )?;
        if unit_index >= chunk_count {
            let handoff_slot = unit_index - chunk_count;
            anyhow::ensure!(
                unit.entry_publication
                    && unit.chunk_slot == -1
                    && unit.dependencies.is_empty()
                    && unit.publication_handoff_slot == i32::try_from(handoff_slot)?
                    && unit.kind == OFFICIAL_OUTPUT_CHUNK_ENTRY_PUBLICATION_UNIT_KIND,
                "official-output chunk smoke entry-publication unit is invalid"
            );
            continue;
        }
        anyhow::ensure!(
            !unit.entry_publication
                && unit.chunk_slot == i32::try_from(unit_index)?
                && unit.publication_handoff_slot == -1
                && unit.kind == OFFICIAL_OUTPUT_CHUNK_UNIT_KIND,
            "official-output chunk smoke physical chunk unit is invalid"
        );
        let mut dependency_specifiers = BTreeSet::new();
        for dependency in &unit.dependencies {
            anyhow::ensure!(
                matches!(
                    dependency.kind.as_str(),
                    "dynamic-import" | "import-statement"
                ) && dependency.slot < chunk_count
                    && dependency_specifiers.insert(dependency.specifier.clone()),
                "official-output chunk smoke dependency descriptor is invalid"
            );
            validate_official_output_chunk_smoke_nonempty(
                "descriptor.units.dependencies.path",
                &dependency.path,
            )?;
            validate_official_output_chunk_smoke_nonempty(
                "descriptor.units.dependencies.specifier",
                &dependency.specifier,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
fn validate_official_output_chunk_smoke_identifier(field: &str, value: &str) -> anyhow::Result<()> {
    let mut bytes = value.bytes();
    let first = bytes.next();
    anyhow::ensure!(
        first.is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "official-output chunk smoke expectation {field} must be an identifier"
    );
    Ok(())
}

#[cfg(test)]
fn validate_official_output_chunk_smoke_lowercase_hex(
    field: &str,
    value: &str,
    length: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == length
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "official-output chunk smoke expectation {field} must contain exactly {length} lowercase \
         hexadecimal characters"
    );
    Ok(())
}

#[cfg(test)]
fn validate_official_output_chunk_smoke_nonempty(field: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty() && !value.contains('\0'),
        "official-output chunk smoke expectation {field} must be non-empty and exclude NUL bytes"
    );
    Ok(())
}

#[cfg(test)]
fn validate_native_capability_test_handler_export_name(value: &str) -> anyhow::Result<()> {
    let mut bytes = value.bytes();
    let first = bytes.next();
    anyhow::ensure!(
        first.is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$'))
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')),
        "native capability test expectation handler export names must be JavaScript identifiers"
    );
    Ok(())
}

#[cfg(test)]
fn validate_native_capability_test_lowercase_hex(
    field: &str,
    value: &str,
    length: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == length
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "native capability test expectation {field} must contain exactly {length} lowercase \
         hexadecimal characters"
    );
    Ok(())
}

#[cfg(test)]
fn validate_native_capability_test_nonempty(field: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty() && !value.contains('\0'),
        "native capability test expectation {field} must be non-empty and exclude NUL bytes"
    );
    Ok(())
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealPackageTestExpectation {
    wasm: RealPackageTestRoute,
    existing_runtime: RealMultiMemberExistingRuntimeRoute,
    v8_fallback: RealMultiMemberExistingRuntimeRoute,
    package_key: String,
    serialized_module_sha256: String,
    #[serde(default)]
    execution: Option<RealPackageTestExecution>,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealMultiMemberPackageTestExpectation {
    wasm: Vec<RealMultiMemberPackageTestRoute>,
    existing_runtime: RealMultiMemberExistingRuntimeRoute,
    v8_fallback: RealPackageTestRoute,
    required_member_imports: Vec<String>,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealMultiMemberExistingRuntimeRoute {
    runtime_module_path: String,
    export_name: String,
    udf_kind: RealMultiMemberExistingRuntimeUdfKind,
}

#[cfg(test)]
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
enum RealMultiMemberExistingRuntimeUdfKind {
    Query,
    Mutation,
    Action,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealMultiMemberPackageTestRoute {
    route: RealPackageTestRoute,
    package_key: String,
    serialized_module_sha256: String,
    execution: RealPackageTestExecution,
}

#[cfg(test)]
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RealModuleGraphTestUdfKind {
    Query,
    Mutation,
}

#[cfg(test)]
impl RealModuleGraphTestUdfKind {
    fn manifest_kind(self) -> ManifestUdfKind {
        match self {
            Self::Query => ManifestUdfKind::Query,
            Self::Mutation => ManifestUdfKind::Mutation,
        }
    }

    fn udf_type(self) -> UdfType {
        match self {
            Self::Query => UdfType::Query,
            Self::Mutation => UdfType::Mutation,
        }
    }
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealModuleGraphTestExpectation {
    runtime_module_path: String,
    export_name: String,
    udf_kind: RealModuleGraphTestUdfKind,
    setup: RealModuleGraphTestSetup,
    // An explicit sequence exercises lazy module initialization in one Store
    // without invoking handlers or requiring their application data.
    #[serde(default)]
    preparation_entry_selectors: Vec<String>,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(untagged)]
enum RealModuleGraphTestSetup {
    EmptyIndexed(RealModuleGraphEmptyIndexedTestSetup),
    SeededDocuments(RealModuleGraphSeededDocumentsTestSetup),
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealModuleGraphEmptyIndexedTestSetup {
    application_index: String,
    application_index_fields: Vec<String>,
    application_table: String,
    expected_result: JsonValue,
    invocation_arguments: JsonValue,
    kind: RealModuleGraphEmptyIndexedTestSetupKind,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealModuleGraphSeededDocumentsTestSetup {
    application_index: String,
    application_index_fields: Vec<String>,
    application_table: String,
    documents: Vec<JsonValue>,
    expected_result: JsonValue,
    invocation_arguments: JsonValue,
    kind: RealModuleGraphSeededDocumentsTestSetupKind,
}

#[cfg(test)]
#[derive(Clone, Copy, Deserialize)]
enum RealModuleGraphEmptyIndexedTestSetupKind {
    #[serde(rename = "empty-indexed-query-v1")]
    Query,
    #[serde(rename = "empty-indexed-mutation-v1")]
    Mutation,
}

#[cfg(test)]
#[derive(Clone, Copy, Deserialize)]
enum RealModuleGraphSeededDocumentsTestSetupKind {
    #[serde(rename = "seeded-documents-query-v1")]
    Query,
    #[serde(rename = "seeded-documents-mutation-v1")]
    Mutation,
}

#[cfg(test)]
struct RealModuleGraphTestSetupFields<'a> {
    application_index: &'a str,
    application_index_fields: &'a [String],
    application_table: &'a str,
    documents: &'a [JsonValue],
    expected_result: &'a JsonValue,
    invocation_arguments: &'a JsonValue,
}

#[cfg(test)]
impl RealModuleGraphTestSetup {
    fn fields(&self) -> RealModuleGraphTestSetupFields<'_> {
        match self {
            Self::EmptyIndexed(setup) => RealModuleGraphTestSetupFields {
                application_index: &setup.application_index,
                application_index_fields: &setup.application_index_fields,
                application_table: &setup.application_table,
                documents: &[],
                expected_result: &setup.expected_result,
                invocation_arguments: &setup.invocation_arguments,
            },
            Self::SeededDocuments(setup) => RealModuleGraphTestSetupFields {
                application_index: &setup.application_index,
                application_index_fields: &setup.application_index_fields,
                application_table: &setup.application_table,
                documents: &setup.documents,
                expected_result: &setup.expected_result,
                invocation_arguments: &setup.invocation_arguments,
            },
        }
    }

    fn validate_for_udf_kind(&self, udf_kind: RealModuleGraphTestUdfKind) -> anyhow::Result<()> {
        let fields = self.fields();
        anyhow::ensure!(
            !fields.application_index.is_empty()
                && !fields.application_table.is_empty()
                && !fields.application_index_fields.is_empty()
                && fields
                    .application_index_fields
                    .iter()
                    .all(|field| !field.is_empty())
                && fields
                    .application_index_fields
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    == fields.application_index_fields.len(),
            "real module graph setup application index is invalid"
        );
        anyhow::ensure!(
            fields.invocation_arguments.is_object(),
            "real module graph setup invocation arguments must be an object"
        );
        match self {
            Self::EmptyIndexed(setup) => {
                anyhow::ensure!(
                    matches!(
                        (setup.kind, udf_kind),
                        (
                            RealModuleGraphEmptyIndexedTestSetupKind::Query,
                            RealModuleGraphTestUdfKind::Query
                        ) | (
                            RealModuleGraphEmptyIndexedTestSetupKind::Mutation,
                            RealModuleGraphTestUdfKind::Mutation
                        )
                    ),
                    "real module graph empty-indexed setup kind does not match the route UDF kind"
                );
                if matches!(udf_kind, RealModuleGraphTestUdfKind::Query) {
                    anyhow::ensure!(
                        setup.expected_result == JsonValue::Array(Vec::new()),
                        "real module graph empty-indexed query expected result must be an empty \
                         array"
                    );
                } else {
                    anyhow::ensure!(
                        setup.expected_result.is_object(),
                        "real module graph empty-indexed mutation expected result must be an \
                         object"
                    );
                }
            },
            Self::SeededDocuments(setup) => {
                anyhow::ensure!(
                    matches!(
                        (setup.kind, udf_kind),
                        (
                            RealModuleGraphSeededDocumentsTestSetupKind::Query,
                            RealModuleGraphTestUdfKind::Query
                        ) | (
                            RealModuleGraphSeededDocumentsTestSetupKind::Mutation,
                            RealModuleGraphTestUdfKind::Mutation
                        )
                    ),
                    "real module graph seeded-documents setup kind does not match the route UDF \
                     kind"
                );
                anyhow::ensure!(
                    !setup.documents.is_empty() && setup.documents.iter().all(JsonValue::is_object),
                    "real module graph seeded-documents setup documents must be non-empty objects"
                );
            },
        }
        Ok(())
    }
}

#[cfg(test)]
#[test]
fn real_module_graph_seeded_documents_setup_is_strict_and_matches_the_route_kind(
) -> anyhow::Result<()> {
    let expectation: RealModuleGraphTestExpectation = serde_json::from_value(json!({
        "runtimeModulePath": "fixture.js",
        "exportName": "read",
        "udfKind": "query",
        "setup": {
            "kind": "seeded-documents-query-v1",
            "applicationTable": "fixtureRows",
            "applicationIndex": "by_marker",
            "applicationIndexFields": ["marker"],
            "documents": [
                { "marker": "leaf" },
                { "marker": "parent", "nextId": { "$documentId": 0 } }
            ],
            "invocationArguments": { "parentId": { "$documentId": 1 } },
            "expectedResult": [{ "$documentId": 0 }]
        }
    }))?;
    expectation
        .setup
        .validate_for_udf_kind(expectation.udf_kind)?;
    assert_eq!(expectation.setup.fields().documents.len(), 2);

    let mismatched_kind: RealModuleGraphTestExpectation = serde_json::from_value(json!({
        "runtimeModulePath": "fixture.js",
        "exportName": "write",
        "udfKind": "mutation",
        "setup": {
            "kind": "seeded-documents-query-v1",
            "applicationTable": "fixtureRows",
            "applicationIndex": "by_marker",
            "applicationIndexFields": ["marker"],
            "documents": [{ "marker": "seed" }],
            "invocationArguments": {},
            "expectedResult": null
        }
    }))?;
    assert!(mismatched_kind
        .setup
        .validate_for_udf_kind(mismatched_kind.udf_kind)
        .is_err());

    assert!(
        serde_json::from_value::<RealModuleGraphTestExpectation>(json!({
            "runtimeModulePath": "fixture.js",
            "exportName": "read",
            "udfKind": "query",
            "setup": {
                "kind": "empty-indexed-query-v1",
                "applicationTable": "fixtureRows",
                "applicationIndex": "by_marker",
                "applicationIndexFields": ["marker"],
                "documents": [],
                "invocationArguments": {},
                "expectedResult": []
            }
        }))
        .is_err()
    );
    Ok(())
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealPackageTestExecution {
    table_name: String,
    index_name: String,
    index_fields: Vec<String>,
    #[serde(default)]
    preserve_result_order: bool,
    cases: Vec<RealPackageTestCase>,
}

#[cfg(test)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealPackageTestCase {
    name: String,
    request: JsonValue,
    documents: Vec<JsonValue>,
    expected_documents_read: usize,
    #[serde(default)]
    expected_host_read_batches: Option<usize>,
    #[serde(default)]
    expected_abandoned_operations: Option<usize>,
    #[serde(default)]
    expected_cancel_all_calls: Option<usize>,
    #[serde(default)]
    expect_runtime_reuse_from_previous: bool,
    expected_result: Option<JsonValue>,
    developer_error_contains: Option<String>,
}
#[cfg(test)]
#[test]
fn generated_module_file_backed_initial_bytes_reserve_one_aot_mapping() -> anyhow::Result<()> {
    assert_eq!(generated_module_initial_fixed_bytes(19, 23)?, 42);
    assert!(generated_module_initial_fixed_bytes(u64::MAX, 1).is_err());
    Ok(())
}

#[cfg(test)]
#[test]
fn generated_module_in_memory_initial_bytes_reserve_input_and_aot_mapping() -> anyhow::Result<()> {
    assert_eq!(generated_in_memory_module_initial_fixed_bytes(19, 23)?, 65);
    assert!(generated_in_memory_module_initial_fixed_bytes(1, u64::MAX).is_err());
    Ok(())
}

#[cfg(test)]
#[test]
fn generated_shared_aot_cold_loads_single_flight_by_identity() {
    let identity = GeneratedAotModuleIdentity {
        engine_compatibility_sha256: "engine".to_owned(),
        serialized_module_sha256: "module".to_owned(),
    };
    let first = generated_shared_aot_load_lock(&identity);
    let second = generated_shared_aot_load_lock(&identity);
    assert!(Arc::ptr_eq(&first, &second));

    let distinct = generated_shared_aot_load_lock(&GeneratedAotModuleIdentity {
        engine_compatibility_sha256: "engine".to_owned(),
        serialized_module_sha256: "distinct".to_owned(),
    });
    assert!(!Arc::ptr_eq(&first, &distinct));
}

#[cfg(test)]
#[test]
fn generated_engine_compatibility_hash_is_deterministic_and_covers_fuel() -> anyhow::Result<()> {
    let first = new_generated_engine()?;
    let second = new_generated_engine()?;
    let first_sha256 = calculated_precompile_compatibility_sha256(&first);
    assert_eq!(
        first_sha256,
        calculated_precompile_compatibility_sha256(&second)
    );
    assert_eq!(first_sha256, GENERATED_ENGINE_COMPATIBILITY_SHA256);
    assert_eq!(first_sha256.len(), 64);
    assert!(first_sha256
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));

    let mut perf_map_config = Config::new();
    perf_map_config
        .target(GENERATED_TARGET_TRIPLE)
        .map_err(wasmtime_anyhow)?;
    perf_map_config
        .consume_fuel(true)
        .epoch_interruption(true)
        .profiler(ProfilingStrategy::PerfMap)
        .wasm_exceptions(true);
    let perf_map = Engine::new(&perf_map_config).map_err(wasmtime_anyhow)?;
    assert_eq!(
        first_sha256,
        calculated_precompile_compatibility_sha256(&perf_map),
        "runtime profiling must not rotate or invalidate precompiled artifacts"
    );

    let mut no_fuel_config = Config::new();
    no_fuel_config
        .target(GENERATED_TARGET_TRIPLE)
        .map_err(wasmtime_anyhow)?;
    no_fuel_config
        .epoch_interruption(true)
        .profiler(ProfilingStrategy::None)
        .wasm_exceptions(true);
    let no_fuel = Engine::new(&no_fuel_config).map_err(wasmtime_anyhow)?;
    assert_ne!(
        first_sha256,
        calculated_precompile_compatibility_sha256(&no_fuel)
    );
    Ok(())
}

#[cfg(test)]
#[test]
fn generated_wasmtime_revision_matches_the_direct_dependency() -> anyhow::Result<()> {
    let manifest = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    )?;
    let pinned_revision = format!("rev = \"{GENERATED_WASMTIME_REVISION}\"");
    assert_eq!(
        manifest.matches(&pinned_revision).count(),
        2,
        "runtime and test Wasmtime dependencies must use the advertised revision"
    );

    let lock = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"),
    )?;
    let exact_source = format!("?rev={GENERATED_WASMTIME_REVISION}#{GENERATED_WASMTIME_REVISION}");
    assert!(
        lock.contains(&exact_source),
        "Cargo.lock must resolve the advertised Wasmtime revision exactly"
    );
    Ok(())
}

#[cfg(test)]
fn generated_initialization_trace_module(
    engine: &Engine,
    exports: &[(&str, i32)],
) -> anyhow::Result<Module> {
    let mut types = EncodedTypeSection::new();
    types.ty().function([EncodedValType::I32], []);
    types.ty().function([], []);
    let mut imports = EncodedImportSection::new();
    imports.import("trace", "record", EncodedEntityType::Function(0));
    let mut functions = EncodedFunctionSection::new();
    let mut encoded_exports = EncodedExportSection::new();
    let mut code = EncodedCodeSection::new();
    for (index, (name, marker)) in exports.iter().enumerate() {
        functions.function(1);
        encoded_exports.export(name, EncodedExportKind::Func, u32::try_from(index)? + 1);
        let mut function = EncodedFunction::new([]);
        function.instruction(&EncodedInstruction::I32Const(*marker));
        function.instruction(&EncodedInstruction::Call(0));
        function.instruction(&EncodedInstruction::End);
        code.function(&function);
    }
    let mut module = EncodedModule::new();
    module
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&encoded_exports)
        .section(&code);
    Module::new(engine, module.finish()).map_err(wasmtime_anyhow)
}

#[cfg(test)]
async fn run_generated_emscripten_initialization_order_test() -> anyhow::Result<()> {
    let engine = new_generated_engine()?;
    let base_module = generated_initialization_trace_module(
        &engine,
        &[("__wasm_apply_data_relocs", 1), ("_initialize", 2)],
    )?;
    let first_shared_module = generated_initialization_trace_module(
        &engine,
        &[("__wasm_apply_data_relocs", 3), ("__wasm_call_ctors", 4)],
    )?;
    let second_shared_module = generated_initialization_trace_module(
        &engine,
        &[("__wasm_apply_data_relocs", 5), ("__wasm_call_ctors", 6)],
    )?;
    let leaf_module = generated_initialization_trace_module(
        &engine,
        &[("__wasm_apply_data_relocs", 7), ("__wasm_call_ctors", 8)],
    )?;
    let mut linker = Linker::new(&engine);
    linker.func_wrap(
        "trace",
        "record",
        |mut caller: Caller<'_, Vec<i32>>, marker: i32| {
            caller.data_mut().push(marker);
        },
    )?;
    let mut store = Store::new(&engine, Vec::new());
    store.set_fuel(1_000_000)?;
    store.set_epoch_deadline(u64::MAX / 2);
    let base = linker.instantiate_async(&mut store, &base_module).await?;
    let first_shared = linker
        .instantiate_async(&mut store, &first_shared_module)
        .await?;
    let second_shared = linker
        .instantiate_async(&mut store, &second_shared_module)
        .await?;
    let leaf = linker.instantiate_async(&mut store, &leaf_module).await?;
    let initialization = GeneratedFreshInitialization::EmscriptenGraph {
        instances: GeneratedEmscriptenGraphInstances {
            modules: vec![base, first_shared, second_shared, leaf],
        },
        base_initialize: base.get_typed_func(&mut store, "_initialize")?,
        initialization: GraphInitialization {
            base_heap_base: 1,
            base_table_size: 1,
            constructor_order: vec!["shared-a".into(), "shared-b".into(), "leaf".into()],
            final_memory_cursor: 1,
            final_table_cursor: 1,
            module_order: vec![
                "base".into(),
                "shared-a".into(),
                "shared-b".into(),
                "leaf".into(),
            ],
            relocation_order: vec!["shared-a".into(), "shared-b".into(), "leaf".into()],
        },
    };

    assert!(store.data().is_empty());
    run_generated_fresh_initialization(&mut store, initialization).await?;
    assert_eq!(store.data(), &[1, 2, 3, 5, 7, 4, 6, 8]);
    Ok(())
}

#[test]
fn generated_emscripten_graph_initialization_is_deferred_and_ordered() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    rt.block_on(
        "generated_emscripten_graph_initialization_order",
        run_generated_emscripten_initialization_order_test(),
    )
}

#[cfg(test)]
async fn run_generated_emscripten_initialization_error_context_test() -> anyhow::Result<()> {
    let engine = new_generated_engine()?;
    let base_module = generated_initialization_trace_module(&engine, &[("_initialize", 1)])?;
    let shared_module =
        generated_initialization_trace_module(&engine, &[("__wasm_call_ctors", 2)])?;
    let leaf_module = generated_initialization_trace_module(&engine, &[])?;
    let mut linker = Linker::new(&engine);
    linker.func_wrap(
        "trace",
        "record",
        |mut caller: Caller<'_, Vec<i32>>, marker: i32| -> Result<(), WasmtimeError> {
            caller.data_mut().push(marker);
            if marker == 2 {
                return Err(WasmtimeError::msg("initialization context test failure"));
            }
            Ok(())
        },
    )?;
    let mut store = Store::new(&engine, Vec::new());
    store.set_fuel(1_000_000)?;
    store.set_epoch_deadline(u64::MAX / 2);
    let base = linker.instantiate_async(&mut store, &base_module).await?;
    let shared = linker.instantiate_async(&mut store, &shared_module).await?;
    let leaf = linker.instantiate_async(&mut store, &leaf_module).await?;
    let initialization = GeneratedFreshInitialization::EmscriptenGraph {
        instances: GeneratedEmscriptenGraphInstances {
            modules: vec![base, shared, leaf],
        },
        base_initialize: base.get_typed_func(&mut store, "_initialize")?,
        initialization: GraphInitialization {
            base_heap_base: 1,
            base_table_size: 1,
            constructor_order: vec!["shared-a".into()],
            final_memory_cursor: 1,
            final_table_cursor: 1,
            module_order: vec!["base".into(), "shared-a".into(), "leaf".into()],
            relocation_order: vec![],
        },
    };

    let error = run_generated_fresh_initialization(&mut store, initialization)
        .await
        .unwrap_err();
    let error = format!("{error:#}");
    assert!(error.contains(
        "run constructors for authenticated module graph role shared-a (module index 1)"
    ));
    assert!(error.contains("initialization context test failure"));
    Ok(())
}

#[test]
fn generated_emscripten_graph_initialization_failure_identifies_module() -> anyhow::Result<()> {
    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    rt.block_on(
        "generated_emscripten_graph_initialization_error_context",
        run_generated_emscripten_initialization_error_context_test(),
    )
}

#[cfg(test)]
pub(super) fn generated_module_cache_test_controller(
    soft_budget_bytes: usize,
    hard_budget_bytes: usize,
) -> anyhow::Result<Arc<GeneratedMemoryController>> {
    GeneratedMemoryController::new(GeneratedMemoryPolicy {
        hard_instance_ceiling: 8,
        soft_budget_bytes,
        hard_budget_bytes,
        safety_reserve_bytes: 8,
        unattributed_bytes_per_slot: 0,
        cold_peak_growth_bytes: 8,
        warm_idle_target: 0,
        maximum_idle_age: Duration::from_secs(1),
        pressure_enter_headroom_bytes: 8,
        pressure_exit_headroom_bytes: 16,
    })
}

#[cfg(test)]
fn generated_module_cache_test_route(
    deployment: char,
    export_name: &str,
) -> GeneratedRouteIdentity {
    GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: deployment.to_string().repeat(64),
        generation_sha256: deployment.to_string().repeat(64),
        package_key: format!("package-{deployment}"),
        entry: DeploymentEntryIdentity::LegacyExport {
            runtime_module_path: "functions/cache_test.js".to_owned(),
            export_name: export_name.to_owned(),
        },
    }
}

#[cfg(test)]
fn generated_module_cache_test_routed(
    route_identity: GeneratedRouteIdentity,
    fixed_module_bytes: usize,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    generated_module_cache_test_routed_with_generation(route_identity, fixed_module_bytes, None)
}

#[cfg(test)]
fn generated_module_cache_test_routed_with_generation(
    route_identity: GeneratedRouteIdentity,
    fixed_module_bytes: usize,
    generation: Option<Arc<DeploymentGeneration>>,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    let mut types = EncodedTypeSection::new();
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);
    let mut functions = EncodedFunctionSection::new();
    functions.function(0);
    let mut exports = EncodedExportSection::new();
    exports.export("run", EncodedExportKind::Func, 0);
    let mut run = EncodedFunction::new([]);
    run.instruction(&EncodedInstruction::I32Const(7));
    run.instruction(&EncodedInstruction::End);
    let mut code = EncodedCodeSection::new();
    code.function(&run);
    let mut encoded = EncodedModule::new();
    encoded
        .section(&types)
        .section(&functions)
        .section(&exports)
        .section(&code);

    let engine = Arc::new(Engine::default());
    let module = Module::new(&engine, encoded.finish()).map_err(wasmtime_anyhow)?;
    let manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
    let permitted_conditional_convex_imports = permitted_conditional_convex_imports(
        manifest.value_mode(),
        manifest.effect_execution_mode(),
        manifest.imported_operations(),
    );
    let execution = Arc::new(WasmUdfExecutionPolicy::from_legacy(&manifest));
    let package_identity = Arc::new(ValidatedWasmUdfPackageIdentity::Legacy((*manifest).clone()));
    Ok(Arc::new(GeneratedRoutedModule {
        emscripten_graph: None,
        engine,
        entry_selector: None,
        fixed_module_bytes,
        generation,
        module_charge: OnceLock::new(),
        module: Arc::new(module),
        manifest: execution,
        package_identity,
        permitted_conditional_convex_imports,
        graph: None,
        pool_identity: GeneratedPoolIdentity::Route(route_identity.clone()),
        route_identity,
    }))
}

#[test]
fn capability_package_cache_and_pool_identity_is_package_scoped() -> anyhow::Result<()> {
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: "a".repeat(64),
        generation_sha256: "d".repeat(64),
        package_key: "b".repeat(64),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    let sibling_route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: "a".repeat(64),
        generation_sha256: "d".repeat(64),
        package_key: "b".repeat(64),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    assert_eq!(route_identity, sibling_route_identity);

    let different_package_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: "a".repeat(64),
        generation_sha256: "d".repeat(64),
        package_key: "c".repeat(64),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    assert_ne!(route_identity, different_package_identity);

    let different_generation_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: "a".repeat(64),
        generation_sha256: "e".repeat(64),
        package_key: "b".repeat(64),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    assert_ne!(route_identity, different_generation_identity);

    let routed = generated_module_cache_test_routed(route_identity.clone(), 1)?;
    let mut cache = GeneratedRoutedModuleCache::default();
    cache.entries.insert(
        route_identity,
        GeneratedRoutedModuleCacheEntry {
            last_used: 0,
            routed,
        },
    );
    assert!(cache.singleton().is_none());
    Ok(())
}

#[test]
fn generation_retirement_preserves_same_deployment_replacement_cache() -> anyhow::Result<()> {
    let route = |generation: char| GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: "a".repeat(64),
        generation_sha256: generation.to_string().repeat(64),
        package_key: "b".repeat(64),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    let old_route = route('c');
    let replacement_route = route('d');
    let generations = ['c', 'd'].map(|generation_sha256| {
        let mut generation = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/exact-generation-retirement"),
            ValidatedDeploymentManifest::empty_for_test(&"a".repeat(64)),
        );
        Arc::get_mut(&mut generation)
            .expect("generation retirement fixture unexpectedly shared its generation")
            .generation_sha256 = generation_sha256.to_string().repeat(64);
        generation
    });
    let mut cache = GeneratedRoutedModuleCache::default();
    for (route_identity, generation) in [old_route.clone(), replacement_route.clone()]
        .into_iter()
        .zip(&generations)
    {
        cache.entries.insert(
            route_identity.clone(),
            GeneratedRoutedModuleCacheEntry {
                last_used: 0,
                routed: generated_module_cache_test_routed_with_generation(
                    route_identity,
                    1,
                    Some(Arc::clone(generation)),
                )?,
            },
        );
    }

    cache.retire_generation(&generations[0]);

    assert!(!cache.contains_key(&old_route));
    assert!(cache.contains_key(&replacement_route));
    Ok(())
}

#[test]
fn delayed_retirement_does_not_evict_a_later_same_digest_incarnation() -> anyhow::Result<()> {
    let deployment_sha256 = "a".repeat(64);
    let old_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/old-generation-incarnation"),
        ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
    );
    let later_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/later-generation-incarnation"),
        ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
    );
    assert_eq!(
        old_generation.generation_sha256,
        later_generation.generation_sha256
    );
    assert!(!Arc::ptr_eq(&old_generation, &later_generation));

    let route = generated_module_cache_test_route('a', "same-digest");
    let routed = generated_module_cache_test_routed_with_generation(
        route.clone(),
        1,
        Some(Arc::clone(&later_generation)),
    )?;
    let mut cache = GeneratedRoutedModuleCache::default();
    cache.entries.insert(
        route.clone(),
        GeneratedRoutedModuleCacheEntry {
            last_used: 0,
            routed,
        },
    );

    cache.retire_generation(&old_generation);
    assert!(cache.contains_key(&route));
    cache.retire_generation(&later_generation);
    assert!(!cache.contains_key(&route));
    Ok(())
}

#[test]
fn graph_pool_identity_covers_generation_engine_and_every_aot_digest() {
    let identity = |generation: char, engine: char, shared: Vec<char>| {
        let engine_compatibility_sha256 = engine.to_string().repeat(64);
        let aot = |digest: char| GeneratedAotModuleIdentity {
            engine_compatibility_sha256: engine_compatibility_sha256.clone(),
            serialized_module_sha256: digest.to_string().repeat(64),
        };
        let base = aot('a');
        let leaf = aot('e');
        let shared = shared.into_iter().map(aot).collect();
        GeneratedPoolIdentity::Graph(GeneratedGraphPoolIdentity {
            generation_sha256: generation.to_string().repeat(64),
            graph_sha256: "f".repeat(64),
            engine_compatibility_sha256,
            base,
            shared,
            leaf,
        })
    };
    let original = identity('1', '2', vec!['b', 'c']);
    assert_eq!(original, identity('1', '2', vec!['b', 'c']));
    assert_ne!(original, identity('9', '2', vec!['b', 'c']));
    assert_ne!(original, identity('1', '8', vec!['b', 'c']));
    assert_ne!(original, identity('1', '2', vec!['c', 'b']));
    assert_ne!(original, identity('1', '2', vec!['b', 'd']));
}

#[test]
fn graph_instantiates_base_then_ordered_shared_modules_then_leaf() -> anyhow::Result<()> {
    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Arc::new(Engine::new(&config).map_err(wasmtime_anyhow)?);
    let memory_type = EncodedMemoryType {
        minimum: 1,
        maximum: Some(1),
        memory64: false,
        shared: false,
        page_size_log2: None,
    };
    let table_type = EncodedTableType {
        element_type: EncodedRefType::FUNCREF,
        table64: false,
        minimum: 1,
        maximum: Some(1),
        shared: false,
    };
    let stack_type = EncodedGlobalType {
        val_type: EncodedValType::I32,
        mutable: true,
        shared: false,
    };

    let mut base_tables = EncodedTableSection::new();
    base_tables.table(table_type);
    let mut base_memory = EncodedMemorySection::new();
    base_memory.memory(memory_type);
    let mut base_globals = EncodedGlobalSection::new();
    base_globals.global(stack_type, &EncodedConstExpr::i32_const(32));
    let mut base_exports = EncodedExportSection::new();
    base_exports.export("memory", EncodedExportKind::Memory, 0);
    base_exports.export("table", EncodedExportKind::Table, 0);
    base_exports.export("stack", EncodedExportKind::Global, 0);
    let mut base = EncodedModule::new();
    base.section(&base_tables)
        .section(&base_memory)
        .section(&base_globals)
        .section(&base_exports);
    let base = Module::new(&engine, base.finish()).map_err(wasmtime_anyhow)?;

    let mut shared_types = EncodedTypeSection::new();
    shared_types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);
    let mut shared_imports = EncodedImportSection::new();
    shared_imports.import(
        "graph.base",
        "memory",
        EncodedEntityType::Memory(memory_type),
    );
    shared_imports.import("graph.base", "table", EncodedEntityType::Table(table_type));
    shared_imports.import("graph.base", "stack", EncodedEntityType::Global(stack_type));
    let mut shared_functions = EncodedFunctionSection::new();
    shared_functions.function(0);
    let mut shared_exports = EncodedExportSection::new();
    shared_exports.export("shared_ready", EncodedExportKind::Func, 0);
    let mut shared_ready = EncodedFunction::new([]);
    shared_ready.instruction(&EncodedInstruction::I32Const(7));
    shared_ready.instruction(&EncodedInstruction::End);
    let mut shared_code = EncodedCodeSection::new();
    shared_code.function(&shared_ready);
    let mut shared = EncodedModule::new();
    shared
        .section(&shared_types)
        .section(&shared_imports)
        .section(&shared_functions)
        .section(&shared_exports)
        .section(&shared_code);
    let shared = Module::new(&engine, shared.finish()).map_err(wasmtime_anyhow)?;

    let mut leaf_types = EncodedTypeSection::new();
    leaf_types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);
    let mut leaf_imports = EncodedImportSection::new();
    leaf_imports.import(
        "graph.shared",
        "shared_ready",
        EncodedEntityType::Function(0),
    );
    let mut leaf_functions = EncodedFunctionSection::new();
    leaf_functions.function(0);
    let mut leaf_exports = EncodedExportSection::new();
    leaf_exports.export("run", EncodedExportKind::Func, 1);
    let mut run = EncodedFunction::new([]);
    run.instruction(&EncodedInstruction::Call(0));
    run.instruction(&EncodedInstruction::End);
    let mut leaf_code = EncodedCodeSection::new();
    leaf_code.function(&run);
    let mut leaf = EncodedModule::new();
    leaf.section(&leaf_types)
        .section(&leaf_imports)
        .section(&leaf_functions)
        .section(&leaf_exports)
        .section(&leaf_code);
    let leaf = Module::new(&engine, leaf.finish()).map_err(wasmtime_anyhow)?;

    let dependency = |digest: char, module: Module| {
        Arc::new(GeneratedSharedAotModule {
            identity: GeneratedAotModuleIdentity {
                engine_compatibility_sha256: "e".repeat(64),
                serialized_module_sha256: digest.to_string().repeat(64),
            },
            module: Arc::new(module),
            module_charge: parking_lot::Mutex::new(None),
        })
    };
    let graph = GeneratedGraphModules {
        dependencies: vec![
            GeneratedGraphDependency {
                module_id: "base".to_owned(),
                provider_namespace: "graph.base".to_owned(),
                module: dependency('a', base),
            },
            GeneratedGraphDependency {
                module_id: "shared".to_owned(),
                provider_namespace: "graph.shared".to_owned(),
                module: dependency('b', shared),
            },
        ],
        layout: CapabilityGraphLayout {
            memory: CapabilityGraphExportReference {
                export_name: "memory".to_owned(),
                module_id: "base".to_owned(),
            },
            stack: super::super::wasm_udf_package::CapabilityGraphStackLayout {
                alignment_bytes: 16,
                initial_pointer: 32,
                lower_bound_bytes: 16,
                pointer: CapabilityGraphExportReference {
                    export_name: "stack".to_owned(),
                    module_id: "base".to_owned(),
                },
                upper_bound_bytes: 64,
            },
            table: CapabilityGraphExportReference {
                export_name: "table".to_owned(),
                module_id: "base".to_owned(),
            },
            tags: vec![],
        },
    };

    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("generated_wasm_graph_instantiation", async move {
        let mut linker = Linker::new(&engine);
        let mut store = Store::new(&engine, ());
        store.set_fuel(1_000).map_err(wasmtime_anyhow)?;
        let instances =
            instantiate_generated_graph_dependencies(&mut linker, &mut store, &graph).await?;
        let leaf = linker
            .instantiate_async(&mut store, &leaf)
            .await
            .map_err(wasmtime_anyhow)?;
        validate_generated_graph_instance_layout(&mut store, &instances, &graph.layout)?;
        let result = leaf
            .get_typed_func::<(), i32>(&mut store, "run")
            .map_err(wasmtime_anyhow)?
            .call_async(&mut store, ())
            .await
            .map_err(wasmtime_anyhow)?;
        anyhow::ensure!(
            result == 7,
            "generated graph leaf called the wrong provider"
        );
        Ok(())
    })
}

#[test]
fn graph_cross_generation_replacement_never_reuses_the_old_pool_identity() -> anyhow::Result<()> {
    let old_deployment = "5".repeat(64);
    let new_deployment = "6".repeat(64);
    let registry = DeploymentRegistry::from_legacy(
        PathBuf::from("/unused/graph-old"),
        ValidatedDeploymentManifest::empty_for_test(&old_deployment),
    );
    let old_generation = registry.current();
    let replacement = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/graph-new"),
        ValidatedDeploymentManifest::empty_for_test(&new_deployment),
    );
    let module = |digest: char| GeneratedAotModuleIdentity {
        engine_compatibility_sha256: "7".repeat(64),
        serialized_module_sha256: digest.to_string().repeat(64),
    };
    let pool_identity = |generation_sha256: String| {
        GeneratedPoolIdentity::Graph(GeneratedGraphPoolIdentity {
            generation_sha256,
            graph_sha256: "8".repeat(64),
            engine_compatibility_sha256: "7".repeat(64),
            base: module('a'),
            shared: vec![module('b')],
            leaf: module('c'),
        })
    };
    let old_pool = pool_identity(old_generation.generation_sha256.clone());
    let retired = registry
        .activate(Arc::clone(&replacement))?
        .expect("previous generation");
    let new_pool = pool_identity(replacement.generation_sha256.clone());

    assert!(Arc::ptr_eq(&retired, &old_generation));
    assert!(old_generation.is_retired());
    assert_ne!(old_pool, new_pool);
    assert!(old_generation.module_graph_catalog.is_none());
    assert!(replacement.module_graph_catalog.is_none());
    Ok(())
}

#[test]
fn graph_module_contract_rejects_serialized_type_drift() -> anyhow::Result<()> {
    let routed = generated_module_cache_test_routed(
        generated_module_cache_test_route('t', "type-drift"),
        1,
    )?;
    let contract = |result| CapabilityGraphModuleContract {
        imports: vec![],
        exports: vec![
            super::super::wasm_udf_package::CapabilityGraphExportContract {
                name: "run".to_owned(),
                ty: CapabilityGraphExternType::Function {
                    parameters: vec![],
                    results: vec![result],
                },
            },
        ],
    };
    assert!(validate_generated_graph_module_contract(
        &routed.module,
        &contract(CapabilityGraphValueType::I32),
    )
    .is_ok());
    assert!(validate_generated_graph_module_contract(
        &routed.module,
        &contract(CapabilityGraphValueType::I64),
    )
    .is_err());
    Ok(())
}

#[test]
fn graph_shared_aot_accounting_is_deduplicated_by_engine_and_digest() -> anyhow::Result<()> {
    let mut cache = GeneratedRoutedModuleCache::default();
    let routed = generated_module_cache_test_routed(
        generated_module_cache_test_route('g', "shared-aot"),
        1,
    )?;
    let resized_fixed_bytes = generated_module_fixed_bytes(128, 1, &routed.module)?;
    let controller = generated_module_cache_test_controller(
        resized_fixed_bytes
            .checked_add(64)
            .context("shared AOT test soft budget overflow")?,
        resized_fixed_bytes
            .checked_add(128)
            .context("shared AOT test hard budget overflow")?,
    )?;
    let identity = GeneratedAotModuleIdentity {
        engine_compatibility_sha256: "a".repeat(64),
        serialized_module_sha256: "b".repeat(64),
    };
    let module = Arc::new(GeneratedSharedAotModule {
        identity: identity.clone(),
        module: Arc::clone(&routed.module),
        module_charge: parking_lot::Mutex::new(None),
    });
    let charge = cache
        .reserve(&controller, 64)
        .map_err(module_memory_admission_error)?;
    let first = cache.insert_shared_aot_module(module, charge);
    let second = cache
        .get_shared_aot_module(&identity)
        .context("shared AOT cache entry disappeared")?;
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(cache.shared_aot_modules.len(), 1);
    assert_eq!(
        first
            .module_charge
            .lock()
            .as_ref()
            .context("shared AOT cache entry lost its charge")?
            .fixed_bytes(),
        64
    );
    // The shared key deduplicates the AOT image, but the charge must still
    // cover each authenticated Core Wasm size requested by a graph.
    resize_shared_aot_charge(&mut cache, &first, 128, 1)?;
    assert_eq!(
        first
            .module_charge
            .lock()
            .as_ref()
            .context("shared AOT cache entry lost its resized charge")?
            .fixed_bytes(),
        resized_fixed_bytes
    );
    assert!(cache
        .get_shared_aot_module(&GeneratedAotModuleIdentity {
            engine_compatibility_sha256: "c".repeat(64),
            serialized_module_sha256: "b".repeat(64),
        })
        .is_none());
    assert!(cache
        .get_shared_aot_module(&GeneratedAotModuleIdentity {
            engine_compatibility_sha256: "a".repeat(64),
            serialized_module_sha256: "d".repeat(64),
        })
        .is_none());
    Ok(())
}

#[test]
fn graph_leaf_accounting_survives_independent_generation_retirement() -> anyhow::Result<()> {
    let mut cache = GeneratedRoutedModuleCache::default();
    let route_bytes = std::mem::size_of::<GeneratedRoutedModule>();
    let generations = ['a', 'b'].map(|marker| {
        DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/shared-leaf-retirement"),
            ValidatedDeploymentManifest::empty_for_test(&marker.to_string().repeat(64)),
        )
    });
    let mut first = generated_module_cache_test_routed_with_generation(
        generated_module_cache_test_route('a', "shared-leaf"),
        route_bytes,
        Some(Arc::clone(&generations[0])),
    )?;
    let mut second = generated_module_cache_test_routed_with_generation(
        generated_module_cache_test_route('b', "shared-leaf"),
        route_bytes,
        Some(Arc::clone(&generations[1])),
    )?;
    let aot = first.module.serialize().map_err(wasmtime_anyhow)?;
    let compiled_bytes =
        generated_module_fixed_bytes(128, u64::try_from(aot.len())?, &first.module)?;
    let controller = generated_module_cache_test_controller(
        compiled_bytes + route_bytes * 2 + 64,
        compiled_bytes + route_bytes * 2 + 128,
    )?;
    let identity = GeneratedAotModuleIdentity {
        engine_compatibility_sha256: "e".repeat(64),
        serialized_module_sha256: format!("{:x}", Sha256::digest(&aot)),
    };
    let charge = cache
        .reserve(&controller, compiled_bytes)
        .map_err(module_memory_admission_error)?;
    let leaf = cache.insert_shared_aot_module(
        Arc::new(GeneratedSharedAotModule {
            identity: identity.clone(),
            module: Arc::clone(&first.module),
            module_charge: parking_lot::Mutex::new(None),
        }),
        charge,
    );
    let engine = Arc::clone(&first.engine);
    // The base must not retain the leaf's charged owner: otherwise it can
    // conceal a missing leaf owner in the routed graph.
    let base_module =
        Module::new(&engine, EncodedModule::new().finish()).map_err(wasmtime_anyhow)?;
    let base = Arc::new(GeneratedSharedAotModule {
        identity: GeneratedAotModuleIdentity {
            engine_compatibility_sha256: identity.engine_compatibility_sha256.clone(),
            serialized_module_sha256: format!(
                "{:x}",
                Sha256::digest(base_module.serialize().map_err(wasmtime_anyhow)?)
            ),
        },
        module: Arc::new(base_module),
        module_charge: parking_lot::Mutex::new(None),
    });
    for routed in [&mut first, &mut second] {
        let routed = Arc::get_mut(routed).context("leaf lifecycle fixture route was shared")?;
        routed.engine = Arc::clone(&engine);
        routed.module = Arc::clone(&leaf.module);
        routed.emscripten_graph = Some(GeneratedEmscriptenGraphModules {
            base: Arc::clone(&base),
            shared: vec![],
            leaf: Arc::clone(&leaf),
            initialization: GraphInitialization {
                base_heap_base: 0,
                base_table_size: 0,
                constructor_order: vec![],
                final_memory_cursor: 0,
                final_table_cursor: 0,
                module_order: vec![],
                relocation_order: vec![],
            },
            modules: vec![],
        });
        routed.pool_identity = GeneratedPoolIdentity::Graph(GeneratedGraphPoolIdentity {
            generation_sha256: routed
                .generation
                .as_ref()
                .context("fixture lost generation")?
                .generation_sha256
                .clone(),
            graph_sha256: "f".repeat(64),
            engine_compatibility_sha256: identity.engine_compatibility_sha256.clone(),
            base: base.identity.clone(),
            shared: vec![],
            leaf: identity.clone(),
        });
    }
    let memory_identity = generated_memory_identity_for_package(
        &first.manifest,
        &first.package_identity,
        &first.route_identity,
    );
    for routed in [&first, &second] {
        let charge = cache
            .reserve(&controller, route_bytes)
            .map_err(module_memory_admission_error)?;
        cache.insert(Arc::clone(routed), charge);
    }
    assert!(Arc::ptr_eq(&first.module, &second.module));
    assert_ne!(first.pool_identity, second.pool_identity);
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        compiled_bytes + route_bytes * 2
    );
    drop(leaf);

    generations[0].retired.store(true, Ordering::Release);
    cache.retire_generation(&generations[0]);
    drop(first);
    cache.evict_all_idle_shared_aot_modules();
    assert!(cache.contains_key(&second.route_identity));
    assert!(cache.shared_aot_modules.contains_key(&identity));
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        compiled_bytes + route_bytes
    );
    let mut store = Store::new(&engine, ());
    let instance =
        wasmtime::Instance::new(&mut store, &second.module, &[]).map_err(wasmtime_anyhow)?;
    assert_eq!(
        instance
            .get_typed_func::<(), i32>(&mut store, "run")
            .map_err(wasmtime_anyhow)?
            .call(&mut store, ())
            .map_err(wasmtime_anyhow)?,
        7
    );
    drop(store);

    generations[1].retired.store(true, Ordering::Release);
    cache.retire_generation(&generations[1]);
    cache.evict_all_idle_shared_aot_modules();
    assert!(cache.shared_aot_modules.contains_key(&identity));
    drop(second);
    cache.evict_all_idle_shared_aot_modules();
    assert!(!cache.shared_aot_modules.contains_key(&identity));
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        0
    );
    Ok(())
}

#[test]
fn graph_retirement_keeps_active_modules_and_releases_idle_shared_accounting() -> anyhow::Result<()>
{
    let controller = generated_module_cache_test_controller(512, 640)?;
    let mut cache = GeneratedRoutedModuleCache::default();
    let deployment_sha256 = "e".repeat(64);
    let generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/graph-retirement"),
        ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
    );
    let route_identity = generated_module_cache_test_route('e', "graph-retirement");
    let mut routed = generated_module_cache_test_routed_with_generation(
        route_identity.clone(),
        16,
        Some(Arc::clone(&generation)),
    )?;
    let shared_identity = GeneratedAotModuleIdentity {
        engine_compatibility_sha256: "1".repeat(64),
        serialized_module_sha256: "2".repeat(64),
    };
    let shared = Arc::new(GeneratedSharedAotModule {
        identity: shared_identity.clone(),
        module: Arc::clone(&routed.module),
        module_charge: parking_lot::Mutex::new(None),
    });
    let shared_charge = cache
        .reserve(&controller, 64)
        .map_err(module_memory_admission_error)?;
    let shared = cache.insert_shared_aot_module(shared, shared_charge);
    let routed_mut =
        Arc::get_mut(&mut routed).context("graph test route was unexpectedly shared")?;
    routed_mut.graph = Some(GeneratedGraphModules {
        dependencies: vec![GeneratedGraphDependency {
            module_id: "base".to_owned(),
            provider_namespace: "graph-base".to_owned(),
            module: Arc::clone(&shared),
        }],
        layout: CapabilityGraphLayout {
            memory: CapabilityGraphExportReference {
                export_name: "memory".to_owned(),
                module_id: "base".to_owned(),
            },
            stack: super::super::wasm_udf_package::CapabilityGraphStackLayout {
                alignment_bytes: 16,
                initial_pointer: 32,
                lower_bound_bytes: 16,
                pointer: CapabilityGraphExportReference {
                    export_name: "stack".to_owned(),
                    module_id: "base".to_owned(),
                },
                upper_bound_bytes: 64,
            },
            table: CapabilityGraphExportReference {
                export_name: "table".to_owned(),
                module_id: "base".to_owned(),
            },
            tags: vec![],
        },
    });
    routed_mut.pool_identity = GeneratedPoolIdentity::Graph(GeneratedGraphPoolIdentity {
        generation_sha256: generation.generation_sha256.clone(),
        graph_sha256: "3".repeat(64),
        engine_compatibility_sha256: "1".repeat(64),
        base: shared_identity.clone(),
        shared: vec![],
        leaf: GeneratedAotModuleIdentity {
            engine_compatibility_sha256: "1".repeat(64),
            serialized_module_sha256: "4".repeat(64),
        },
    });
    let route_charge = cache
        .reserve(&controller, 16)
        .map_err(module_memory_admission_error)?;
    let active = cache.insert(Arc::clone(&routed), route_charge);
    let memory_identity = generated_memory_identity_for_package(
        &active.manifest,
        &active.package_identity,
        &active.route_identity,
    );
    drop(shared);
    drop(routed);

    generation.retired.store(true, Ordering::Release);
    cache.retire_generation(&generation);
    assert!(!cache.contains_key(&route_identity));
    assert!(cache.shared_aot_modules.contains_key(&shared_identity));
    assert_eq!(Arc::strong_count(&active), 1);
    assert!(active.graph.is_some());
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        80
    );

    drop(active);
    cache.evict_all_idle_shared_aot_modules();
    assert!(!cache.shared_aot_modules.contains_key(&shared_identity));
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        0
    );
    Ok(())
}

#[cfg(test)]
fn materialize_real_package_expected_result(
    value: &JsonValue,
    document_ids: &[DeveloperDocumentId],
) -> anyhow::Result<JsonValue> {
    match value {
        JsonValue::Array(values) => values
            .iter()
            .map(|value| materialize_real_package_expected_result(value, document_ids))
            .collect(),
        JsonValue::Object(object) if object.len() == 1 && object.contains_key("$documentId") => {
            let index = object["$documentId"]
                .as_u64()
                .context("$documentId placeholder must be a nonnegative integer")?;
            let index = usize::try_from(index)?;
            Ok(json!(document_ids
                .get(index)
                .with_context(|| format!("$documentId placeholder {index} is out of range"))?
                .encode()))
        },
        JsonValue::Object(object) => object
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.clone(),
                    materialize_real_package_expected_result(value, document_ids)?,
                ))
            })
            .collect(),
        value => Ok(value.clone()),
    }
}

#[cfg(test)]
fn order_real_package_expected_rows(expected: JsonValue) -> anyhow::Result<JsonValue> {
    let rows = expected
        .as_array()
        .context("real-package expected result must be an array")?;
    let mut ordered = rows
        .iter()
        .map(|row| {
            let subscriber_id = row
                .get("subscriberId")
                .and_then(JsonValue::as_str)
                .context("real-package expected row must contain subscriberId")?;
            let document_id = DeveloperDocumentId::decode(subscriber_id)?;
            Ok((document_id.internal_id(), row.clone()))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    // Equal `active` index keys use `_id` as their final ordering component.
    ordered.sort_by_key(|(internal_id, _)| *internal_id);
    Ok(JsonValue::Array(
        ordered.into_iter().map(|(_, row)| row).collect(),
    ))
}

#[cfg(test)]
async fn prepare_real_package_case_database(
    rt: ProdRuntime,
    execution: &RealPackageTestExecution,
    case: &RealPackageTestCase,
) -> anyhow::Result<(Database<ProdRuntime>, Vec<DeveloperDocumentId>, JsonValue)> {
    prepare_seeded_document_case_database(
        rt,
        &execution.table_name,
        &execution.index_name,
        &execution.index_fields,
        &case.documents,
        &case.request,
        &format!("real-package case {}", case.name),
        "generated_real_package_test_setup",
        false,
    )
    .await
}

#[cfg(test)]
async fn prepare_seeded_document_case_database(
    rt: ProdRuntime,
    table_name: &str,
    index_name: &str,
    index_fields: &[String],
    documents: &[JsonValue],
    request: &JsonValue,
    description: &str,
    write_source: &'static str,
    initialize_application_tables: bool,
) -> anyhow::Result<(Database<ProdRuntime>, Vec<DeveloperDocumentId>, JsonValue)> {
    let table: TableName = table_name.parse()?;
    let index_name = IndexName::new(table.clone(), IndexDescriptor::new(index_name.to_owned())?)?;
    let indexed_fields = IndexedFields::try_from(
        index_fields
            .iter()
            .map(|field| field.parse())
            .collect::<anyhow::Result<Vec<_>>>()?,
    )?;
    let database = new_test_database(rt).await?;
    if initialize_application_tables {
        initialize_application_system_tables(&database).await?;
    }
    let mut setup = database.begin_system().await?;
    IndexModel::new(&mut setup)
        .add_application_index(
            TableNamespace::root_component(),
            IndexMetadata::new_enabled(index_name, indexed_fields),
        )
        .await?;
    let mut document_ids = Vec::with_capacity(documents.len());
    for document in documents {
        // A fixture may refer only to documents inserted earlier in the same case. This
        // permits deterministic linked-document graphs without requiring
        // production-only ID allocation.
        let document = materialize_real_package_expected_result(document, &document_ids)?;
        let document = ConvexObject::try_from(document)
            .with_context(|| format!("{description} has an invalid document"))?;
        document_ids.push(
            UserFacingModel::new(&mut setup, TableNamespace::root_component())
                .insert(table.clone(), document)
                .await?,
        );
    }
    database
        .commit_with_write_source(setup, write_source)
        .await?;
    let request = materialize_real_package_expected_result(request, &document_ids)?;
    Ok((database, document_ids, request))
}

#[cfg(test)]
async fn prepare_real_module_graph_test_database(
    rt: ProdRuntime,
    setup: &RealModuleGraphTestSetup,
) -> anyhow::Result<(Database<ProdRuntime>, JsonValue, JsonValue)> {
    let fields = setup.fields();
    let (database, document_ids, request) = prepare_seeded_document_case_database(
        rt,
        fields.application_table,
        fields.application_index,
        fields.application_index_fields,
        fields.documents,
        fields.invocation_arguments,
        "real module graph setup",
        "generated_real_module_graph_test_setup",
        true,
    )
    .await?;
    let expected_result =
        materialize_real_package_expected_result(fields.expected_result, &document_ids)?;
    Ok((database, request, expected_result))
}

#[cfg(test)]
async fn run_real_package_execution_cases(
    rt: ProdRuntime,
    routed: Arc<GeneratedRoutedModule>,
    controller: Arc<GeneratedMemoryController>,
    execution: &RealPackageTestExecution,
) -> anyhow::Result<Vec<u64>> {
    anyhow::ensure!(
        !execution.cases.is_empty(),
        "real-package execution requires at least one case"
    );
    let metrics = Arc::new(GateMetrics::default());
    let ValidatedWasmUdfPackageIdentity::Legacy(package_manifest) = &*routed.package_identity
    else {
        anyhow::bail!("legacy real-package execution loaded a capability-entry package")
    };
    let package_manifest = Arc::new(package_manifest.clone());
    let mut reusable = None;
    let mut previous_runtime_id = None;
    for (case_index, case) in execution.cases.iter().enumerate() {
        anyhow::ensure!(
            case.expected_result.is_some() ^ case.developer_error_contains.is_some(),
            "real-package case {} must declare exactly one expected outcome",
            case.name
        );
        let (database, document_ids, request) =
            prepare_real_package_case_database(rt.clone(), execution, case).await?;

        let retain_on_success =
            case.expected_result.is_some() && case_index + 1 < execution.cases.len();
        let read_batches_before = metrics.read_completed.load(Ordering::SeqCst);
        let cancel_all_calls_before = metrics
            .async_operation_cancel_all_calls
            .load(Ordering::SeqCst);
        let abandoned_operations_before = metrics.async_operations_abandoned.load(Ordering::SeqCst);
        let mut output = execute_reusable_generated_async_batch_test_module(
            rt.clone(),
            database.begin_system().await?,
            Arc::clone(&package_manifest),
            Arc::clone(&routed),
            request,
            Arc::clone(&controller),
            Arc::clone(&metrics),
            reusable.take(),
            retain_on_success,
        )
        .await
        .with_context(|| format!("real-package case {} failed internally", case.name))?;
        let current_runtime_id = metrics
            .generated_runtimes
            .lock()
            .last()
            .copied()
            .context("real-package execution did not record its runtime")?;
        if case.expect_runtime_reuse_from_previous {
            anyhow::ensure!(
                previous_runtime_id == Some(current_runtime_id),
                "real-package case {} did not reuse the previous runtime",
                case.name
            );
        }
        if let Some(expected) = &case.expected_result {
            anyhow::ensure!(
                output.invocation.outcome == InvocationOutcome::Success,
                "real-package case {} did not succeed: {:?}",
                case.name,
                output.invocation.outcome
            );
            let expected = materialize_real_package_expected_result(expected, &document_ids)?;
            let expected = if execution.preserve_result_order {
                expected
            } else {
                order_real_package_expected_rows(expected)?
            };
            anyhow::ensure!(
                output.invocation.function_result.as_ref() == Some(&expected),
                "real-package case {} returned an unexpected result: actual={}, expected={}",
                case.name,
                output
                    .invocation
                    .function_result
                    .as_ref()
                    .map_or_else(|| "<missing>".to_owned(), JsonValue::to_string),
                expected
            );
            anyhow::ensure!(
                output.invocation.opaque_live_handles == 0
                    && output.invocation.opaque_current_bytes == 0,
                "real-package case {} leaked host value handles",
                case.name
            );
            if retain_on_success {
                reusable = Some(retain_generated_test_runtime(&mut output)?);
            }
        } else {
            let expected = case
                .developer_error_contains
                .as_deref()
                .context("validated real-package developer error disappeared")?;
            let InvocationOutcome::DeveloperError(message) = &output.invocation.outcome else {
                anyhow::bail!(
                    "real-package case {} did not return a developer error",
                    case.name
                )
            };
            anyhow::ensure!(
                message.contains(expected),
                "real-package case {} returned an unexpected developer error",
                case.name
            );
        }
        anyhow::ensure!(
            output.invocation.read_accounting.documents == case.expected_documents_read,
            "real-package case {} recorded an unexpected document-read count",
            case.name
        );
        if let Some(expected_host_read_batches) = case.expected_host_read_batches {
            let read_batches = metrics
                .read_completed
                .load(Ordering::SeqCst)
                .checked_sub(read_batches_before)
                .context("real-package host read-batch counter moved backwards")?;
            anyhow::ensure!(
                read_batches == expected_host_read_batches,
                "real-package case {} used {read_batches} host read batches instead of {}",
                case.name,
                expected_host_read_batches
            );
        }
        if let Some(expected_cancel_all_calls) = case.expected_cancel_all_calls {
            let cancel_all_calls = metrics
                .async_operation_cancel_all_calls
                .load(Ordering::SeqCst)
                .checked_sub(cancel_all_calls_before)
                .context("real-package cancel-all counter moved backwards")?;
            anyhow::ensure!(
                cancel_all_calls == expected_cancel_all_calls,
                "real-package case {} called async cleanup {cancel_all_calls} times instead of {}",
                case.name,
                expected_cancel_all_calls
            );
        }
        if let Some(expected_abandoned_operations) = case.expected_abandoned_operations {
            let abandoned_operations = metrics
                .async_operations_abandoned
                .load(Ordering::SeqCst)
                .checked_sub(abandoned_operations_before)
                .context("real-package abandoned-operation counter moved backwards")?;
            anyhow::ensure!(
                abandoned_operations == expected_abandoned_operations,
                "real-package case {} abandoned {abandoned_operations} async operations instead \
                 of {}",
                case.name,
                expected_abandoned_operations
            );
        }
        if let Some(instance) = output.reusable_instance.take() {
            discard_generated_instance(instance).await?;
            output
                .memory_permit
                .take()
                .context("real-package case reusable instance lost its memory permit")?
                .finish(output.terminal_memory_outcome, false);
        }
        drop(output.invocation.transaction);
        database.shutdown().await?;
        previous_runtime_id = Some(current_runtime_id);
    }
    anyhow::ensure!(
        reusable.is_none(),
        "real-package execution retained its final runtime"
    );
    let runtime_ids = metrics.generated_runtimes.lock().clone();
    Ok(runtime_ids)
}

#[cfg(test)]
async fn reject_real_package_cross_route_reuse(
    rt: ProdRuntime,
    controller: Arc<GeneratedMemoryController>,
    retained_route: Arc<GeneratedRoutedModule>,
    retained_execution: &RealPackageTestExecution,
    requested_route: Arc<GeneratedRoutedModule>,
    requested_execution: &RealPackageTestExecution,
) -> anyhow::Result<()> {
    let retained_case = retained_execution
        .cases
        .first()
        .context("retained real-package route requires an execution case")?;
    anyhow::ensure!(
        retained_case.expected_result.is_some(),
        "retained real-package route first case must succeed"
    );
    let retained_metrics = Arc::new(GateMetrics::default());
    let ValidatedWasmUdfPackageIdentity::Legacy(retained_manifest) =
        &*retained_route.package_identity
    else {
        anyhow::bail!("legacy retained route loaded a capability-entry package")
    };
    let retained_manifest = Arc::new(retained_manifest.clone());
    let (retained_database, _, retained_request) =
        prepare_real_package_case_database(rt.clone(), retained_execution, retained_case).await?;
    let mut retained_output = execute_reusable_generated_async_batch_test_module(
        rt.clone(),
        retained_database.begin_system().await?,
        retained_manifest,
        Arc::clone(&retained_route),
        retained_request,
        Arc::clone(&controller),
        retained_metrics,
        None,
        true,
    )
    .await?;
    anyhow::ensure!(
        retained_output.invocation.outcome == InvocationOutcome::Success,
        "real-package route did not produce a reusable runtime"
    );
    let retained = retain_generated_test_runtime(&mut retained_output)?;
    drop(retained_output.invocation.transaction);
    retained_database.shutdown().await?;

    let requested_case = requested_execution
        .cases
        .first()
        .context("requested real-package route requires an execution case")?;
    let requested_metrics = Arc::new(GateMetrics::default());
    let ValidatedWasmUdfPackageIdentity::Legacy(requested_manifest) =
        &*requested_route.package_identity
    else {
        anyhow::bail!("legacy requested route loaded a capability-entry package")
    };
    let requested_manifest = Arc::new(requested_manifest.clone());
    let (requested_database, _, requested_request) =
        prepare_real_package_case_database(rt.clone(), requested_execution, requested_case).await?;
    let error = match execute_reusable_generated_async_batch_test_module(
        rt,
        requested_database.begin_system().await?,
        requested_manifest,
        requested_route,
        requested_request,
        controller,
        requested_metrics,
        Some(retained),
        false,
    )
    .await
    {
        Ok(_) => anyhow::bail!("a route-bound Store selected a sibling cohort entry"),
        Err(error) => error,
    };
    anyhow::ensure!(
        format!("{error:#}").contains("generated Wasm instance route changed while pooled"),
        "cross-route Store reuse failed for an unexpected reason: {error:#}"
    );
    requested_database.shutdown().await?;
    Ok(())
}

#[test]
fn runtime_registry_switch_retires_old_generation_without_changing_resolved_handle(
) -> anyhow::Result<()> {
    let old_sha256 = "a".repeat(64);
    let new_sha256 = "b".repeat(64);
    let registry = DeploymentRegistry::from_legacy(
        PathBuf::from("/unused/old"),
        ValidatedDeploymentManifest::empty_for_test(&old_sha256),
    );
    let old_selection = registry.query_shadow_registry();
    let resolved_generation = registry.current();
    let candidate = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/new"),
        ValidatedDeploymentManifest::empty_for_test(&new_sha256),
    );

    let retired = registry
        .activate(Arc::clone(&candidate))?
        .expect("previous generation");
    assert!(Arc::ptr_eq(&retired, &resolved_generation));
    assert!(resolved_generation.is_retired());
    assert_eq!(resolved_generation.deployment_sha256(), old_sha256);
    assert_eq!(resolved_generation.generation_sha256, old_sha256);
    assert!(Arc::ptr_eq(&registry.current(), &candidate));
    assert_eq!(registry.current().deployment_sha256(), new_sha256);
    let active_selection = registry.query_shadow_registry();
    assert_eq!(active_selection.generation_sha256(), new_sha256);
    assert!(!old_selection.shares_selection_with(&active_selection));
    Ok(())
}

#[test]
fn runtime_registry_promotes_a_distinct_generation_for_the_same_deployment() -> anyhow::Result<()> {
    let deployment_sha256 = "a".repeat(64);
    let mut shadow_only_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/shadow-only"),
        ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
    );
    Arc::get_mut(&mut shadow_only_generation)
        .expect("unpublished deployment generation unexpectedly had another owner")
        .admission = RuntimeRegistryAdmission::ShadowOnly;
    let registry = DeploymentRegistry::from_generation_for_test(shadow_only_generation, false)?;
    let original = registry.current();
    let mut primary_candidate = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/primary-admitted"),
        ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
    );
    let candidate = Arc::get_mut(&mut primary_candidate)
        .expect("unpublished deployment generation unexpectedly had another owner");
    candidate.current_sha256 = "c".repeat(64);
    candidate.generation_sha256 = "b".repeat(64);

    let retired = registry
        .activate(Arc::clone(&primary_candidate))?
        .expect("previous generation");
    assert!(Arc::ptr_eq(&retired, &original));
    assert!(original.is_retired());
    assert_eq!(registry.current().deployment_sha256(), deployment_sha256);
    assert_eq!(registry.current().generation_sha256, "b".repeat(64));
    Ok(())
}

#[test]
fn primary_routing_rejects_a_shadow_only_registry_at_startup() {
    let mut generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/shadow-only"),
        ValidatedDeploymentManifest::empty_for_test(&"a".repeat(64)),
    );
    Arc::get_mut(&mut generation)
        .expect("unpublished deployment generation unexpectedly had another owner")
        .admission = RuntimeRegistryAdmission::ShadowOnly;

    let error = DeploymentRegistry::from_generation_for_test(generation, true)
        .err()
        .expect("primary routing accepted a shadow-only registry at startup");
    assert!(error
        .to_string()
        .contains("requires a primary-admitted runtime registry generation"));
}

#[test]
fn primary_routing_rejects_a_shadow_only_registry_during_reload_activation() -> anyhow::Result<()> {
    let deployment_sha256 = "a".repeat(64);
    let registry = DeploymentRegistry::from_generation_for_test(
        DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/primary"),
            ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
        ),
        true,
    )?;
    let original = registry.current();
    let mut shadow_only_candidate = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/shadow-only"),
        ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
    );
    let candidate = Arc::get_mut(&mut shadow_only_candidate)
        .expect("unpublished deployment generation unexpectedly had another owner");
    candidate.admission = RuntimeRegistryAdmission::ShadowOnly;
    candidate.current_sha256 = "b".repeat(64);
    candidate.generation_sha256 = "c".repeat(64);

    assert!(registry.activate(shadow_only_candidate).is_err());
    assert!(Arc::ptr_eq(&registry.current(), &original));
    assert!(!original.is_retired());
    Ok(())
}

#[test]
fn shadow_routing_accepts_a_primary_registry_at_startup() {
    let generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/primary"),
        ValidatedDeploymentManifest::empty_for_test(&"a".repeat(64)),
    );

    let registry = DeploymentRegistry::from_generation_for_shadow_test(Arc::clone(&generation))
        .expect("shadow routing rejected a primary-admitted registry at startup");
    assert!(Arc::ptr_eq(&registry.current(), &generation));
}

#[test]
fn shadow_routing_accepts_a_primary_registry_during_reload_activation() -> anyhow::Result<()> {
    let deployment_sha256 = "a".repeat(64);
    let mut shadow_only_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/shadow-only"),
        ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
    );
    Arc::get_mut(&mut shadow_only_generation)
        .expect("unpublished deployment generation unexpectedly had another owner")
        .admission = RuntimeRegistryAdmission::ShadowOnly;
    let registry = DeploymentRegistry::from_generation_for_shadow_test(shadow_only_generation)?;
    let original = registry.current();
    let mut primary_candidate = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/primary"),
        ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
    );
    let candidate = Arc::get_mut(&mut primary_candidate)
        .expect("unpublished deployment generation unexpectedly had another owner");
    candidate.current_sha256 = "b".repeat(64);
    candidate.generation_sha256 = "c".repeat(64);

    let retired = registry
        .activate(Arc::clone(&primary_candidate))?
        .context("shadow registry did not retire its previous generation")?;
    assert!(Arc::ptr_eq(&retired, &original));
    assert!(original.is_retired());
    assert!(Arc::ptr_eq(&registry.current(), &primary_candidate));
    Ok(())
}

#[test]
fn runtime_registry_reload_rejects_incomplete_generation_and_later_accepts_valid_current(
) -> anyhow::Result<()> {
    let fixture = super::super::wasm_udf_package::tests::RuntimeRegistryReloadFixture::create()?;
    let registry = DeploymentRegistry::load(fixture.root().to_owned())?;
    let original = registry.current();

    let rejected = fixture.publish_generation("rejected", false)?;
    assert!(registry.reload().is_err());
    assert!(Arc::ptr_eq(&registry.current(), &original));
    assert!(!original.is_retired());

    fixture.complete_generation(&rejected)?;
    assert!(registry.reload()?.is_none());
    assert!(Arc::ptr_eq(&registry.current(), &original));
    assert!(!original.is_retired());

    let accepted = fixture.publish_generation("accepted", true)?;
    let retired = registry
        .reload()?
        .context("valid runtime registry current was not activated")?;
    assert!(Arc::ptr_eq(&retired, &original));
    assert!(original.is_retired());
    assert_eq!(
        registry.current().deployment_sha256(),
        accepted.deployment_sha256
    );
    assert_eq!(
        registry.current().generation_sha256,
        accepted.generation_sha256
    );
    Ok(())
}

#[test]
fn authenticated_runtime_registry_exposes_shadow_route_digests_by_udf_type() -> anyhow::Result<()> {
    let fixture =
        super::super::wasm_udf_package::tests::RuntimeRegistryQueryShadowFixture::create()?;
    let registry = DeploymentRegistry::load(fixture.root().to_owned())?;

    let selection = registry.query_shadow_registry();
    assert_eq!(selection.generation_sha256(), fixture.generation_sha256());
    assert_eq!(
        selection.route_sha256s(UdfType::Query),
        &[fixture.query_route_sha256().to_owned()]
    );
    assert_ne!(
        selection.route_sha256s(UdfType::Query)[0],
        fixture.mutation_route_sha256()
    );
    // Attempts, admitted reports, and terminal/comparison reports all reuse
    // this generation-scoped index rather than rebuilding selection state.
    for _ in 0..3 {
        let report_selection = registry.query_shadow_registry();
        assert!(selection.shares_selection_with(&report_selection));
        assert!(report_selection.authenticates_route(
            UdfType::Query,
            fixture.generation_sha256(),
            fixture.query_route_sha256(),
        ));
    }
    assert!(!selection.authenticates_route(
        UdfType::Query,
        fixture.generation_sha256(),
        fixture.mutation_route_sha256(),
    ));
    assert!(selection.authenticates_route(
        UdfType::Mutation,
        fixture.generation_sha256(),
        fixture.mutation_route_sha256(),
    ));
    assert!(!selection.authenticates_route(
        UdfType::Query,
        &"8".repeat(64),
        fixture.query_route_sha256(),
    ));
    assert!(!selection.authenticates_route(
        UdfType::Query,
        fixture.generation_sha256(),
        &"9".repeat(64),
    ));
    Ok(())
}

#[test]
fn retired_generation_releases_module_cache_ownership() -> anyhow::Result<()> {
    let controller = generated_module_cache_test_controller(512, 640)?;
    let cache = parking_lot::Mutex::new(GeneratedRoutedModuleCache::default());
    let deployment_sha256 = "c".repeat(64);
    let generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/retired"),
        ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
    );
    let route_identity = generated_module_cache_test_route('c', "retired");
    let build_generation = Arc::clone(&generation);
    let routed = cache_generated_routed_module_with_generation(
        &cache,
        &controller,
        route_identity.clone(),
        64,
        Some(&generation),
        || {
            generated_module_cache_test_routed_with_generation(
                route_identity,
                64,
                Some(build_generation),
            )
        },
    )?;
    assert_eq!(cache.lock().len(), 1);
    generation.retired.store(true, Ordering::Release);
    cache.lock().retire_generation(&generation);
    assert_eq!(cache.lock().len(), 0);
    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &routed.route_identity,
    );
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        64
    );
    drop(routed);
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        0
    );
    Ok(())
}

#[test]
fn generated_wasm_module_cache_charges_repeated_lookup_once_and_cleans_failed_load(
) -> anyhow::Result<()> {
    let controller = generated_module_cache_test_controller(512, 640)?;
    let cache = parking_lot::Mutex::new(GeneratedRoutedModuleCache::default());
    let route_identity = generated_module_cache_test_route('a', "first");
    let builds = AtomicUsize::new(0);
    let first = cache_generated_routed_module_with(
        &cache,
        &controller,
        route_identity.clone(),
        64,
        || {
            builds.fetch_add(1, Ordering::SeqCst);
            generated_module_cache_test_routed(route_identity.clone(), 64)
        },
    )?;
    let second = cache_generated_routed_module_with(
        &cache,
        &controller,
        route_identity.clone(),
        64,
        || {
            builds.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("cached module was rebuilt")
        },
    )?;
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    let memory_identity = generated_memory_identity_for_package(
        &first.manifest,
        &first.package_identity,
        &route_identity,
    );
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        64
    );

    drop(first);
    drop(second);
    cache
        .lock()
        .remove(&route_identity, "module_cache_evicted_test");
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        0
    );

    let failed_route = generated_module_cache_test_route('b', "failed");
    let result =
        cache_generated_routed_module_with(&cache, &controller, failed_route.clone(), 80, || {
            let engine = Engine::default();
            match Module::new(&engine, b"corrupt generated module") {
                Ok(_) => anyhow::bail!("corrupt generated module compiled"),
                Err(error) => Err(wasmtime_anyhow(error)),
            }
        });
    assert!(result.is_err());
    assert_eq!(cache.lock().len(), 0);
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        0
    );
    Ok(())
}

#[test]
fn generated_wasm_module_cache_retirement_keeps_active_charge_and_arc_executable(
) -> anyhow::Result<()> {
    let controller = generated_module_cache_test_controller(512, 640)?;
    let cache = parking_lot::Mutex::new(GeneratedRoutedModuleCache::default());
    let first_route = generated_module_cache_test_route('c', "first");
    let sibling_route = generated_module_cache_test_route('c', "sibling");
    let replacement_route = generated_module_cache_test_route('d', "replacement");
    let old_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/active-retired-generation"),
        ValidatedDeploymentManifest::empty_for_test(&"c".repeat(64)),
    );
    let replacement_generation = DeploymentGeneration::from_legacy(
        PathBuf::from("/unused/replacement-generation"),
        ValidatedDeploymentManifest::empty_for_test(&"d".repeat(64)),
    );
    let first_generation = Arc::clone(&old_generation);
    let first = cache_generated_routed_module_with_generation(
        &cache,
        &controller,
        first_route.clone(),
        48,
        Some(&old_generation),
        || {
            generated_module_cache_test_routed_with_generation(
                first_route.clone(),
                48,
                Some(first_generation),
            )
        },
    )?;
    let sibling_generation = Arc::clone(&old_generation);
    let sibling = cache_generated_routed_module_with_generation(
        &cache,
        &controller,
        sibling_route.clone(),
        56,
        Some(&old_generation),
        || {
            generated_module_cache_test_routed_with_generation(
                sibling_route.clone(),
                56,
                Some(sibling_generation),
            )
        },
    )?;
    assert_eq!(cache.lock().len(), 2);
    assert_eq!(
        controller
            .snapshot_for_test(&generated_memory_identity_for_package(
                &first.manifest,
                &first.package_identity,
                &first_route,
            ))
            .fixed_module_bytes,
        104
    );
    cache.lock().retire_generation(&old_generation);

    let build_replacement_generation = Arc::clone(&replacement_generation);
    let replacement = cache_generated_routed_module_with_generation(
        &cache,
        &controller,
        replacement_route.clone(),
        72,
        Some(&replacement_generation),
        || {
            generated_module_cache_test_routed_with_generation(
                replacement_route.clone(),
                72,
                Some(build_replacement_generation),
            )
        },
    )?;
    let memory_identity = generated_memory_identity_for_package(
        &replacement.manifest,
        &replacement.package_identity,
        &replacement_route,
    );
    {
        let cache = cache.lock();
        assert_eq!(cache.len(), 1);
        assert!(!cache.contains_key(&first_route));
        assert!(!cache.contains_key(&sibling_route));
        assert!(cache.contains_key(&replacement_route));
    }
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        176
    );

    let mut store = Store::new(&first.engine, ());
    let instance =
        wasmtime::Instance::new(&mut store, &first.module, &[]).map_err(wasmtime_anyhow)?;
    let run = instance
        .get_typed_func::<(), i32>(&mut store, "run")
        .map_err(wasmtime_anyhow)?;
    assert_eq!(run.call(&mut store, ()).map_err(wasmtime_anyhow)?, 7);
    drop(run);
    drop(store);

    drop(first);
    drop(sibling);
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        72
    );
    drop(replacement);
    let removed = cache
        .lock()
        .remove(&replacement_route, "module_cache_evicted_test");
    assert!(removed.is_some());
    drop(removed);
    assert!(cache
        .lock()
        .remove(&replacement_route, "module_cache_evicted_test")
        .is_none());
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        0
    );
    Ok(())
}

#[test]
fn generated_wasm_module_cache_evicts_lru_before_budget_rejection() -> anyhow::Result<()> {
    let controller = generated_module_cache_test_controller(180, 240)?;
    let cache = parking_lot::Mutex::new(GeneratedRoutedModuleCache::default());
    let first_route = generated_module_cache_test_route('e', "first");
    let second_route = generated_module_cache_test_route('e', "second");
    let large_route = generated_module_cache_test_route('e', "large");

    let first =
        cache_generated_routed_module_with(&cache, &controller, first_route.clone(), 40, || {
            generated_module_cache_test_routed(first_route.clone(), 40)
        })?;
    let second =
        cache_generated_routed_module_with(&cache, &controller, second_route.clone(), 40, || {
            generated_module_cache_test_routed(second_route.clone(), 40)
        })?;
    drop(first);
    drop(second);
    drop(
        cache
            .lock()
            .get(&first_route)
            .context("first cache fixture disappeared")?,
    );

    let large =
        cache_generated_routed_module_with(&cache, &controller, large_route.clone(), 120, || {
            generated_module_cache_test_routed(large_route.clone(), 120)
        })?;
    let memory_identity = generated_memory_identity_for_package(
        &large.manifest,
        &large.package_identity,
        &large_route,
    );
    {
        let cache = cache.lock();
        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&first_route));
        assert!(!cache.contains_key(&second_route));
        assert!(cache.contains_key(&large_route));
    }
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        160
    );

    drop(large);
    controller.set_pressure_for_test(BackendPressure::Active { headroom_bytes: 8 });
    cache.lock().evict_idle_for_pressure();
    assert_eq!(cache.lock().len(), 0);
    assert_eq!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes,
        0
    );
    Ok(())
}

#[test]
#[ignore = "requires an operator-provided capability-entry package"]
fn generated_wasm_real_capability_entry_package_is_runtime_compatible() -> anyhow::Result<()> {
    let package_directory = std::env::var_os(REAL_CAPABILITY_ENTRY_PACKAGE_TEST_DIRECTORY_ENV)
        .map(PathBuf::from)
        .context(format!(
            "{REAL_CAPABILITY_ENTRY_PACKAGE_TEST_DIRECTORY_ENV} must identify the package"
        ))?;
    let expectation: RealCapabilityEntryPackageTestExpectation = serde_json::from_str(
        &std::env::var(REAL_CAPABILITY_ENTRY_PACKAGE_TEST_EXPECTATION_ENV).with_context(|| {
            format!("failed to read {REAL_CAPABILITY_ENTRY_PACKAGE_TEST_EXPECTATION_ENV}")
        })?,
    )
    .with_context(|| {
        format!("failed to parse {REAL_CAPABILITY_ENTRY_PACKAGE_TEST_EXPECTATION_ENV}")
    })?;
    let [first_expected, second_expected] = expectation.routes.as_slice() else {
        anyhow::bail!("capability-entry compatibility test requires exactly two sibling routes")
    };
    anyhow::ensure!(
        first_expected.route_id != second_expected.route_id
            && first_expected.entry_selector_id != second_expected.entry_selector_id
            && first_expected.export_name != second_expected.export_name
            && first_expected.udf_kind != second_expected.udf_kind
            && first_expected.visibility != second_expected.visibility,
        "capability-entry compatibility routes must differ in every route-owned field"
    );

    let engine = shared_generated_engine()?;
    let runtime_compatibility =
        generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?;
    let mut loaded = Vec::with_capacity(expectation.routes.len());
    for expected in &expectation.routes {
        let package = load_capability_entry_package_for_compatibility_test(
            &package_directory,
            &expectation.package_id,
            &expectation.entry_id,
            &expected.route_id,
            &expected.entry_selector_id,
            &expectation.entry_path,
            &expectation.runtime_module_path,
            &expected.export_name,
            expected.udf_kind,
            &expected.visibility,
            &runtime_compatibility,
        )?;
        let route_lease = DeploymentRouteLease::capability_entry_for_test(
            expectation.entry_id.clone(),
            expected.entry_selector_id.clone(),
            expected.export_name.clone(),
            expected.route_id.clone(),
            expected.udf_kind,
            expected.visibility.clone(),
        );
        anyhow::ensure!(
            package.package_key == expectation.package_id
                && package.entry_selector == route_lease.entry_selector()
                && route_lease.route_id() == Some(expected.route_id.as_str())
                && route_lease.export_name() == expected.export_name
                && route_lease.udf_kind() == expected.udf_kind
                && route_lease.visibility() == expected.visibility
                && package.identity.authenticates_route_lease(&route_lease),
            "capability-entry loader changed or rejected an authenticated sibling route"
        );
        let ValidatedWasmUdfPackageIdentity::CapabilityEntry(identity) = &package.identity else {
            anyhow::bail!("capability-entry compatibility package loaded as a legacy package")
        };
        anyhow::ensure!(
            identity.entry_id() == expectation.entry_id
                && identity.entry_path() == expectation.entry_path
                && expectation.runtime_module_path.strip_suffix(".js")
                    == Some(identity.module_path())
                && identity.routes().len() == expectation.routes.len(),
            "capability-entry loader changed the package or entry identity"
        );
        for expected_route in &expectation.routes {
            anyhow::ensure!(
                identity
                    .routes()
                    .iter()
                    .filter(|route| {
                        route.route_id() == expected_route.route_id
                            && route.entry_selector_id() == expected_route.entry_selector_id
                            && route.export_name() == expected_route.export_name
                            && route.udf_kind() == expected_route.udf_kind
                            && route.visibility() == expected_route.visibility
                    })
                    .count()
                    == 1,
                "capability-entry loader changed the authenticated sibling route table"
            );
        }
        loaded.push((package, route_lease));
    }
    anyhow::ensure!(
        loaded[0].0.identity == loaded[1].0.identity,
        "sibling routes produced different capability-entry package identities"
    );

    let controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: expectation.package_id.clone(),
        generation_sha256: expectation.package_id.clone(),
        package_key: expectation.package_id.clone(),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    anyhow::ensure!(
        !GENERATED_ROUTED_MODULES
            .lock()
            .contains_key(&route_identity),
        "capability-entry compatibility route was already cached"
    );
    let (first_package, first_lease) = loaded.remove(0);
    let (second_package, second_lease) = loaded.remove(0);
    let routed = cache_generated_routed_module(
        &controller,
        first_package,
        route_identity.clone(),
        Arc::clone(&engine),
        None,
    )?;
    let sibling_routed = cache_generated_routed_module(
        &controller,
        second_package,
        route_identity.clone(),
        Arc::clone(&engine),
        None,
    )?;
    anyhow::ensure!(
        Arc::ptr_eq(&routed, &sibling_routed)
            && routed
                .package_identity
                .authenticates_route_lease(&first_lease)
            && routed
                .package_identity
                .authenticates_route_lease(&second_lease),
        "sibling routes did not share one authenticated cached module"
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
    let ValidatedWasmUdfPackageIdentity::CapabilityEntry(capability_identity) =
        &*routed.package_identity
    else {
        anyhow::bail!("cached capability-entry package changed identity kind")
    };
    let sibling_route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: expectation.package_id.clone(),
        generation_sha256: expectation.package_id.clone(),
        package_key: expectation.package_id.clone(),
        entry: DeploymentEntryIdentity::CapabilityPackage,
    };
    anyhow::ensure!(
        route_identity == sibling_route_identity
            && memory_identity
                == generated_memory_identity_for_package(
                    &routed.manifest,
                    &ValidatedWasmUdfPackageIdentity::CapabilityEntry(capability_identity.clone()),
                    &sibling_route_identity,
                ),
        "sibling capability entries did not share module and memory-slot identity"
    );

    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    block_rt.block_on("generated_wasm_real_capability_entry_selectors", async {
        let database = new_test_database(rt.clone()).await?;
        let state = new_state(
            rt,
            database.begin_system().await?,
            None,
            Arc::new(GateMetrics::default()),
            QueryJournal::new(),
        )
        .await?;
        let mut linker = Linker::new(&engine);
        add_generated_convex_imports(&mut linker).map_err(wasmtime_anyhow)?;
        add_wasi_imports(&mut linker).map_err(wasmtime_anyhow)?;
        let mut store = Store::new(&engine, state);
        store.set_fuel(SUCCESS_FUEL).map_err(wasmtime_anyhow)?;
        store.epoch_deadline_trap();
        store.set_epoch_deadline(u64::MAX / 2);
        let instance = linker
            .instantiate_async(&mut store, &routed.module)
            .await
            .map_err(wasmtime_anyhow)?;
        instance
            .get_typed_func::<(), ()>(&mut store, "_initialize")
            .map_err(wasmtime_anyhow)?
            .call_async(&mut store, ())
            .await
            .map_err(wasmtime_anyhow)?;
        let select_entry = instance
            .get_typed_func::<i64, i32>(&mut store, "convex_wasm_select_entry")
            .map_err(wasmtime_anyhow)?;
        for lease in [&first_lease, &second_lease] {
            let selector = lease
                .entry_selector()
                .context("capability-entry route lease omitted its selector")?;
            anyhow::ensure!(
                select_entry
                    .call_async(&mut store, selector as i64)
                    .await
                    .map_err(wasmtime_anyhow)?
                    == 0,
                "capability-entry guest rejected an authenticated sibling selector"
            );
        }
        drop(store);
        database.shutdown().await
    })?;

    let removed = GENERATED_ROUTED_MODULES
        .lock()
        .remove(
            &route_identity,
            "module_cache_evicted_real_capability_entry_package_test",
        )
        .context("capability-entry compatibility module was not cached")?;
    anyhow::ensure!(
        Arc::ptr_eq(&removed, &routed),
        "capability-entry compatibility test removed an unexpected module"
    );
    drop(removed);
    drop(sibling_routed);
    drop(routed);
    anyhow::ensure!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes
            == 0,
        "dropped capability-entry package retained its fixed module-byte charge"
    );
    Ok(())
}

#[test]
#[ignore = "requires an operator-provided generated deployment and artifact cache"]
fn generated_wasm_real_deployment_package_is_runtime_compatible() -> anyhow::Result<()> {
    let deployment_manifest = std::env::var_os(REAL_PACKAGE_TEST_DEPLOYMENT_MANIFEST_ENV)
        .map(PathBuf::from)
        .context(format!(
            "{REAL_PACKAGE_TEST_DEPLOYMENT_MANIFEST_ENV} must identify the deployment manifest"
        ))?;
    let artifact_cache_root = std::env::var_os(REAL_PACKAGE_TEST_ARTIFACT_CACHE_ROOT_ENV)
        .map(PathBuf::from)
        .context(format!(
            "{REAL_PACKAGE_TEST_ARTIFACT_CACHE_ROOT_ENV} must identify the artifact cache root"
        ))?;
    let expectation: RealPackageTestExpectation = serde_json::from_str(
        &std::env::var(REAL_PACKAGE_TEST_EXPECTATION_ENV)
            .with_context(|| format!("failed to read {REAL_PACKAGE_TEST_EXPECTATION_ENV}"))?,
    )
    .with_context(|| format!("failed to parse {REAL_PACKAGE_TEST_EXPECTATION_ENV}"))?;
    let controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let registry = ValidatedDeploymentManifest::load_for_development_test(&deployment_manifest)?;
    let expected_route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: registry.deployment_sha256().to_owned(),
        generation_sha256: registry.deployment_sha256().to_owned(),
        package_key: expectation.package_key.clone(),
        entry: DeploymentEntryIdentity::LegacyExport {
            runtime_module_path: expectation.wasm.runtime_module_path.clone(),
            export_name: expectation.wasm.export_name.clone(),
        },
    };
    anyhow::ensure!(
        !GENERATED_ROUTED_MODULES
            .lock()
            .contains_key(&expected_route_identity),
        "real-package compatibility test route was already cached"
    );
    let generation = DeploymentGeneration::from_legacy(artifact_cache_root, registry);
    let package_key = generation
        .registry
        .package_key_for_compatibility_test(
            &expectation.wasm.runtime_module_path,
            &expectation.wasm.export_name,
            expectation.wasm.udf_kind,
        )?
        .to_owned();
    anyhow::ensure!(
        package_key == expectation.package_key,
        "real-package compatibility test deployment selected an unexpected package"
    );
    let engine = shared_generated_engine()?;
    let serialized_module_snapshot_directory = tempfile::tempdir()?;
    let package = match generation
        .registry
        .load_export_package_for_compatibility_test(
            &generation.packages_root,
            &expectation.wasm.runtime_module_path,
            &expectation.wasm.export_name,
            expectation.wasm.udf_kind,
            &generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?,
            serialized_module_snapshot_directory.path(),
        )? {
        DeploymentExportPackage::Wasm(package) => package,
        DeploymentExportPackage::ExistingRuntime | DeploymentExportPackage::V8Fallback => {
            anyhow::bail!("real-package compatibility test Wasm route was not selected")
        },
    };
    let routed = cache_generated_routed_module(
        &controller,
        package,
        expected_route_identity.clone(),
        engine,
        Some(Arc::clone(&generation)),
    )?;
    anyhow::ensure!(
        routed.route_identity == expected_route_identity,
        "real-package compatibility test loaded an unexpected route identity"
    );
    anyhow::ensure!(
        Arc::ptr_eq(&routed.engine, &shared_generated_engine()?),
        "real generated package did not use the shared engine"
    );
    let ValidatedWasmUdfPackageIdentity::Legacy(package_manifest) = &*routed.package_identity
    else {
        anyhow::bail!("legacy real-package test loaded a capability-entry package")
    };
    let artifact = package_manifest
        .artifact()
        .context("real generated package omitted its AOT artifact identity")?;
    anyhow::ensure!(
        artifact.serialized_module_sha256().as_str() == expectation.serialized_module_sha256,
        "real generated package loaded an unexpected AOT artifact"
    );
    validate_generated_module_contract_with_imports(
        &routed.module,
        &routed.manifest,
        &routed.permitted_conditional_convex_imports,
        routed.package_identity.requires_entry_selector(),
    )?;
    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &routed.route_identity,
    );

    anyhow::ensure!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes
            == routed.fixed_module_bytes,
        "real generated package fixed module-byte charge is incorrect"
    );
    for non_wasm_route in [&expectation.existing_runtime, &expectation.v8_fallback] {
        let is_non_wasm = match non_wasm_route.udf_kind {
            RealMultiMemberExistingRuntimeUdfKind::Query
            | RealMultiMemberExistingRuntimeUdfKind::Mutation => matches!(
                generation.registry.export_routing(
                    &non_wasm_route.runtime_module_path,
                    &non_wasm_route.export_name,
                    match non_wasm_route.udf_kind {
                        RealMultiMemberExistingRuntimeUdfKind::Query => ManifestUdfKind::Query,
                        RealMultiMemberExistingRuntimeUdfKind::Mutation => {
                            ManifestUdfKind::Mutation
                        },
                        RealMultiMemberExistingRuntimeUdfKind::Action => unreachable!(),
                    },
                )?,
                DeploymentExportRouting::ExistingRuntime | DeploymentExportRouting::V8Fallback
            ),
            RealMultiMemberExistingRuntimeUdfKind::Action => {
                generation.registry.contains_existing_runtime_action(
                    &non_wasm_route.runtime_module_path,
                    &non_wasm_route.export_name,
                )
            },
        };
        anyhow::ensure!(
            is_non_wasm,
            "real-package compatibility test non-Wasm route was unexpectedly selected"
        );
    }
    if let Some(execution) = &expectation.execution {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on(
            "generated_wasm_real_package_execution",
            run_real_package_execution_cases(
                rt,
                Arc::clone(&routed),
                Arc::clone(&controller),
                execution,
            ),
        )?;
    }

    let removed = GENERATED_ROUTED_MODULES
        .lock()
        .remove(
            &expected_route_identity,
            "module_cache_evicted_real_package_test",
        )
        .context("real-package compatibility test module was not cached")?;
    anyhow::ensure!(
        Arc::ptr_eq(&removed, &routed),
        "real-package compatibility test removed an unexpected cached module"
    );
    drop(removed);
    anyhow::ensure!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes
            == routed.fixed_module_bytes,
        "active real generated package did not retain its fixed module-byte charge"
    );
    drop(routed);
    anyhow::ensure!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes
            == 0,
        "dropped real generated package retained its fixed module-byte charge"
    );
    Ok(())
}

#[test]
#[ignore = "requires an operator-provided multi-member runtime registry"]
fn generated_wasm_real_multi_member_runtime_registry_is_compatible() -> anyhow::Result<()> {
    let runtime_registry_root =
        std::env::var_os(REAL_MULTI_MEMBER_PACKAGE_TEST_RUNTIME_REGISTRY_ROOT_ENV)
            .map(PathBuf::from)
            .context(format!(
                "{REAL_MULTI_MEMBER_PACKAGE_TEST_RUNTIME_REGISTRY_ROOT_ENV} must identify the \
                 runtime registry"
            ))?;
    let expectation: RealMultiMemberPackageTestExpectation = serde_json::from_str(
        &std::env::var(REAL_MULTI_MEMBER_PACKAGE_TEST_EXPECTATION_ENV).with_context(|| {
            format!("failed to read {REAL_MULTI_MEMBER_PACKAGE_TEST_EXPECTATION_ENV}")
        })?,
    )
    .with_context(|| format!("failed to parse {REAL_MULTI_MEMBER_PACKAGE_TEST_EXPECTATION_ENV}"))?;
    anyhow::ensure!(
        expectation.wasm.len() >= 2,
        "multi-member compatibility test requires at least two Wasm routes"
    );
    anyhow::ensure!(
        expectation
            .wasm
            .iter()
            .all(|expected| expected.route.udf_kind == ManifestUdfKind::Query),
        "multi-member compatibility execution currently requires query routes"
    );
    anyhow::ensure!(
        !expectation.required_member_imports.is_empty()
            && expectation
                .required_member_imports
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                == expectation.required_member_imports.len(),
        "multi-member compatibility test imports must be nonempty and unique"
    );

    // Loading the immutable generation first proves that this package is
    // accepted through the normal all-eligible registry contract.
    let registry = DeploymentRegistry::load(runtime_registry_root)?;
    let generation = registry.current();
    let controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let engine = shared_generated_engine()?;
    let runtime_compatibility =
        generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?;
    let serialized_module_snapshot_directory = tempfile::tempdir()?;
    let mut routed_routes = Vec::with_capacity(expectation.wasm.len());
    let mut package_keys = BTreeSet::new();
    let mut serialized_module_sha256s = BTreeSet::new();
    let mut entry_selectors = BTreeSet::new();
    let mut member_import_union: Option<BTreeSet<&'static str>> = None;

    for expected in &expectation.wasm {
        let package_key = match generation.registry.export_routing(
            &expected.route.runtime_module_path,
            &expected.route.export_name,
            expected.route.udf_kind,
        )? {
            DeploymentExportRouting::Wasm { package_key, .. } => package_key.to_owned(),
            DeploymentExportRouting::ExistingRuntime | DeploymentExportRouting::V8Fallback => {
                anyhow::bail!("multi-member compatibility test Wasm route was not selected")
            },
        };
        anyhow::ensure!(
            package_key == expected.package_key,
            "multi-member deployment selected an unexpected cohort package"
        );
        package_keys.insert(package_key.clone());
        let route_identity = GeneratedRouteIdentity::DeploymentExport {
            deployment_sha256: generation.deployment_sha256().to_owned(),
            generation_sha256: generation.generation_sha256.clone(),
            package_key,
            entry: DeploymentEntryIdentity::LegacyExport {
                runtime_module_path: expected.route.runtime_module_path.clone(),
                export_name: expected.route.export_name.clone(),
            },
        };
        anyhow::ensure!(
            !GENERATED_ROUTED_MODULES
                .lock()
                .contains_key(&route_identity),
            "multi-member compatibility test route was already cached"
        );
        let package = match generation.registry.load_export_package_from_packages_root(
            &generation.packages_root,
            &expected.route.runtime_module_path,
            &expected.route.export_name,
            expected.route.udf_kind,
            &runtime_compatibility,
            serialized_module_snapshot_directory.path(),
        )? {
            DeploymentExportPackage::Wasm(package) => package,
            DeploymentExportPackage::ExistingRuntime | DeploymentExportPackage::V8Fallback => {
                anyhow::bail!("multi-member compatibility test Wasm route was not selected")
            },
        };
        let routed = cache_generated_routed_module(
            &controller,
            package,
            route_identity,
            Arc::clone(&engine),
            Some(Arc::clone(&generation)),
        )?;
        let ValidatedWasmUdfPackageIdentity::Legacy(package_manifest) = &*routed.package_identity
        else {
            anyhow::bail!("legacy multi-member test loaded a capability-entry package")
        };
        let serialized_module_sha256 = package_manifest
            .artifact()
            .context("multi-member package omitted its AOT artifact identity")?
            .serialized_module_sha256()
            .as_str();
        anyhow::ensure!(
            serialized_module_sha256 == expected.serialized_module_sha256,
            "multi-member package loaded an unexpected AOT artifact"
        );
        serialized_module_sha256s.insert(serialized_module_sha256.to_owned());
        let entry_selector = routed
            .entry_selector
            .context("multi-member package route omitted its cohort selector")?;
        anyhow::ensure!(
            entry_selectors.insert(entry_selector),
            "multi-member package routes used the same cohort selector"
        );
        for required_import in &expectation.required_member_imports {
            anyhow::ensure!(
                routed
                    .permitted_conditional_convex_imports
                    .contains(required_import.as_str()),
                "multi-member package omitted a required authenticated member import"
            );
        }
        match &member_import_union {
            Some(permitted) => anyhow::ensure!(
                permitted == &routed.permitted_conditional_convex_imports,
                "multi-member routes disagreed about the authenticated member import union"
            ),
            None => member_import_union = Some(routed.permitted_conditional_convex_imports.clone()),
        }
        validate_generated_module_contract_with_imports(
            &routed.module,
            &routed.manifest,
            &routed.permitted_conditional_convex_imports,
            routed.package_identity.requires_entry_selector(),
        )?;
        routed_routes.push(routed);
    }

    anyhow::ensure!(
        package_keys.len() == 1 && serialized_module_sha256s.len() == 1,
        "multi-member routes did not share one cohort package and AOT artifact"
    );
    anyhow::ensure!(
        routed_routes
            .iter()
            .any(|routed| routed.manifest.value_mode() == ValueMode::Opaque)
            && routed_routes
                .iter()
                .any(|routed| routed.manifest.value_mode() == ValueMode::GuestNativeJson),
        "multi-member compatibility test requires mixed value modes"
    );
    let existing_runtime_preserved = match expectation.existing_runtime.udf_kind {
        RealMultiMemberExistingRuntimeUdfKind::Query => matches!(
            generation.registry.export_routing(
                &expectation.existing_runtime.runtime_module_path,
                &expectation.existing_runtime.export_name,
                ManifestUdfKind::Query,
            )?,
            DeploymentExportRouting::ExistingRuntime
        ),
        RealMultiMemberExistingRuntimeUdfKind::Mutation => matches!(
            generation.registry.export_routing(
                &expectation.existing_runtime.runtime_module_path,
                &expectation.existing_runtime.export_name,
                ManifestUdfKind::Mutation,
            )?,
            DeploymentExportRouting::ExistingRuntime
        ),
        RealMultiMemberExistingRuntimeUdfKind::Action => {
            generation.registry.contains_existing_runtime_action(
                &expectation.existing_runtime.runtime_module_path,
                &expectation.existing_runtime.export_name,
            )
        },
    };
    anyhow::ensure!(
        existing_runtime_preserved,
        "multi-member deployment existing-runtime route was not preserved"
    );
    anyhow::ensure!(
        matches!(
            generation.registry.export_routing(
                &expectation.v8_fallback.runtime_module_path,
                &expectation.v8_fallback.export_name,
                expectation.v8_fallback.udf_kind,
            )?,
            DeploymentExportRouting::V8Fallback
        ),
        "multi-member deployment V8 fallback route was not preserved"
    );

    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    let test_hooks = StaticHermesGateTestHooks::new(0, 0);
    let _test_hooks_guard = install_test_hooks(&test_hooks)?;
    block_rt.block_on("generated_wasm_real_multi_member_execution", async {
        let mut first_runtime_ids = BTreeSet::new();
        for (expected, routed) in expectation.wasm.iter().zip(&routed_routes) {
            anyhow::ensure!(
                expected.execution.cases.len() >= 2
                    && expected.execution.cases[0].expected_result.is_some()
                    && expected.execution.cases[1].expected_result.is_some(),
                "each multi-member route requires two leading successful execution cases"
            );
            let runtime_ids = run_real_package_execution_cases(
                rt.clone(),
                Arc::clone(routed),
                Arc::clone(&controller),
                &expected.execution,
            )
            .await?;
            anyhow::ensure!(
                runtime_ids.len() == expected.execution.cases.len()
                    && runtime_ids[0] == runtime_ids[1],
                "multi-member route did not reuse its route-bound Store sequentially"
            );
            anyhow::ensure!(
                first_runtime_ids.insert(runtime_ids[0]),
                "different multi-member routes executed through the same route-bound Store"
            );
        }
        reject_real_package_cross_route_reuse(
            rt,
            Arc::clone(&controller),
            Arc::clone(&routed_routes[0]),
            &expectation.wasm[0].execution,
            Arc::clone(&routed_routes[1]),
            &expectation.wasm[1].execution,
        )
        .await
    })?;

    let cleanup = routed_routes
        .iter()
        .map(|routed| {
            (
                routed.route_identity.clone(),
                generated_memory_identity_for_package(
                    &routed.manifest,
                    &routed.package_identity,
                    &routed.route_identity,
                ),
            )
        })
        .collect::<Vec<_>>();
    for routed in routed_routes {
        let removed = GENERATED_ROUTED_MODULES
            .lock()
            .remove(
                &routed.route_identity,
                "module_cache_evicted_real_multi_member_package_test",
            )
            .context("multi-member compatibility test module was not cached")?;
        anyhow::ensure!(
            Arc::ptr_eq(&removed, &routed),
            "multi-member compatibility test removed an unexpected cached module"
        );
        drop(removed);
        drop(routed);
    }
    for (_, memory_identity) in cleanup {
        let completed = controller.snapshot_for_test(&memory_identity);
        anyhow::ensure!(
            completed.active_instances == 0
                && completed.idle_instances == 0
                && completed.fixed_module_bytes == 0,
            "multi-member compatibility test retained guest or host ownership"
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires an operator-provided registry-v5/deployment-v6 module graph"]
fn generated_wasm_real_module_graph_registry_v5_executes_authenticated_route() -> anyhow::Result<()>
{
    let runtime_registry_root = std::env::var_os(REAL_MODULE_GRAPH_TEST_RUNTIME_REGISTRY_ROOT_ENV)
        .map(PathBuf::from)
        .context(format!(
            "{REAL_MODULE_GRAPH_TEST_RUNTIME_REGISTRY_ROOT_ENV} must identify the runtime registry"
        ))?;
    let expectation: RealModuleGraphTestExpectation = serde_json::from_str(
        &std::env::var(REAL_MODULE_GRAPH_TEST_EXPECTATION_ENV)
            .with_context(|| format!("failed to read {REAL_MODULE_GRAPH_TEST_EXPECTATION_ENV}"))?,
    )
    .with_context(|| format!("failed to parse {REAL_MODULE_GRAPH_TEST_EXPECTATION_ENV}"))?;
    expectation
        .setup
        .validate_for_udf_kind(expectation.udf_kind)?;
    let udf_kind = expectation.udf_kind;
    let setup = expectation.setup;
    let preparation_entry_selectors = expectation
        .preparation_entry_selectors
        .iter()
        .map(|selector| {
            anyhow::ensure!(
                selector.len() == 16,
                "entry selector must have 16 hex digits"
            );
            u64::from_str_radix(selector, 16).context("invalid preparation entry selector")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if !preparation_entry_selectors.is_empty() {
        anyhow::ensure!(
            preparation_entry_selectors
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                >= 2,
            "preparation sequence must contain at least two distinct entry selectors"
        );
    }
    let registry = DeploymentRegistry::load(runtime_registry_root)?;
    let generation = registry.current();
    let (package_key, runtime_entry, route_lease, deployed_runtime_identity) =
        match generation.registry.export_routing(
            &expectation.runtime_module_path,
            &expectation.export_name,
            udf_kind.manifest_kind(),
        )? {
            DeploymentExportRouting::Wasm {
                package_key,
                runtime_entry,
                route_lease,
                deployed_runtime_identity,
            } => (
                package_key.to_owned(),
                DeploymentEntryIdentity::from_registry(runtime_entry),
                route_lease,
                deployed_runtime_identity.clone(),
            ),
            DeploymentExportRouting::ExistingRuntime | DeploymentExportRouting::V8Fallback => {
                anyhow::bail!("real module graph test route was not selected for Wasm")
            },
        };
    let route_id = route_lease
        .route_id()
        .context("real module graph test route omitted its authenticated route ID")?;
    let graph_sha256 = generation
        .module_graph_catalog
        .as_ref()
        .context("registry-v5 generation omitted its executable graph catalog")?
        .execution_graph(route_id)
        .context("deployment-v6 route was not covered by an authenticated module graph")?
        .graph_sha256()
        .to_owned();
    let route = DeploymentWasmRoute {
        generation: Arc::clone(&generation),
        package_key,
        entry_identity: runtime_entry,
        route_lease,
        runtime_module_path: expectation.runtime_module_path.clone(),
        export_name: expectation.export_name.clone(),
        udf_kind: udf_kind.manifest_kind(),
        deployed_runtime_identity,
    };
    let controller =
        generated_module_cache_test_controller(4 * 1024 * 1024 * 1024, 6 * 1024 * 1024 * 1024)?;
    let serialized_module_snapshot_directory = tempfile::tempdir()?;
    let routed = load_generated_deployment_routed_module(
        &controller,
        serialized_module_snapshot_directory.path(),
        &route,
    )?;
    let cached = load_generated_deployment_routed_module(
        &controller,
        serialized_module_snapshot_directory.path(),
        &route,
    )?;
    anyhow::ensure!(
        Arc::ptr_eq(&routed, &cached),
        "authenticated graph route was deserialized more than once"
    );
    let GeneratedPoolIdentity::Graph(pool_identity) = &routed.pool_identity else {
        anyhow::bail!("registry-v5 route did not receive a module graph pool identity")
    };
    anyhow::ensure!(
        routed.emscripten_graph.is_some()
            && routed.graph.is_none()
            && pool_identity.generation_sha256 == generation.generation_sha256
            && pool_identity.graph_sha256 == graph_sha256
            && Arc::ptr_eq(
                routed
                    .generation
                    .as_ref()
                    .context("authenticated graph route lost its generation")?,
                &generation,
            ),
        "authenticated graph route lost its generation or graph authority"
    );
    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &routed.route_identity,
    );

    let tokio = ProdRuntime::init_tokio()?;
    let rt = ProdRuntime::new(&tokio);
    let block_rt = rt.clone();
    let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
    let test_hooks = StaticHermesGateTestHooks::new(0, 0);
    let _test_hooks_guard = install_test_hooks(&test_hooks)?;
    test_hooks.set_generated_execution_fuel_override(20_000_000_000)?;
    let execution_controller = Arc::clone(&controller);
    let execution_memory_identity = memory_identity.clone();
    let execution_routed = Arc::clone(&routed);
    block_rt.block_on("generated_wasm_real_module_graph_registry_v5", async move {
        let (database, request, expected_result) =
            prepare_real_module_graph_test_database(rt.clone(), &setup).await?;

        let metrics = Arc::new(GateMetrics::default());
        let mut first_state = generated_routed_test_state(
            rt.clone(),
            database.begin_system().await?,
            &execution_routed,
            request.clone(),
            GeneratedRoutedTestMemorySetup {
                controller: Arc::clone(&execution_controller),
                existing_slot: None,
                memory_identity: execution_memory_identity.clone(),
                values: None,
            },
            Arc::clone(&metrics),
        )
        .await?;
        first_state.provider.set_udf_type(udf_kind.udf_type());
        first_state.provider.snoop_initialization_reads()?;
        let timeout = first_state
            .timeout
            .as_mut()
            .context("module graph test timeout missing")?;
        first_state
            .provider
            .initialize_static_hermes(timeout)
            .await?;
        if !preparation_entry_selectors.is_empty() {
            let timeout = first_state
                .timeout
                .as_mut()
                .context("module graph test timeout missing")?;
            first_state.provider.prepare_execution(timeout).await?;
            let graph = execution_routed
                .emscripten_graph
                .as_ref()
                .context("preparation sequence requires an executable module graph")?;
            let mut linker = Linker::new(&execution_routed.engine);
            add_generated_convex_imports(&mut linker).map_err(wasmtime_anyhow)?;
            add_wasi_imports(&mut linker).map_err(wasmtime_anyhow)?;
            let mut store = Store::new(&execution_routed.engine, first_state);
            store.set_fuel(GENERATED_INITIALIZATION_FUEL)?;
            store.set_epoch_deadline(u64::MAX / 2);
            store.limiter(|state| {
                &mut state
                    .generated
                    .as_mut()
                    .expect("test Store lost generated state")
                    .memory_limiter
            });
            let instances =
                instantiate_generated_emscripten_graph(&linker, &mut store, graph).await?;
            let base = instances.modules[0];
            let initialize = base.get_typed_func::<(), ()>(&mut store, "_initialize")?;
            generated_state_mut(store.data_mut())?
                .capability_bridge
                .begin_initialization()?;
            run_generated_fresh_initialization(
                &mut store,
                GeneratedFreshInitialization::EmscriptenGraph {
                    instances,
                    base_initialize: initialize,
                    initialization: graph.initialization.clone(),
                },
            )
            .await?;
            let select =
                base.get_typed_func::<i64, i32>(&mut store, "convex_wasm_graph_select_entry")?;
            let prepare = base
                .get_typed_func::<(), i32>(&mut store, "convex_wasm_udf_prepare_selected_entry")?;
            let destroy =
                base.get_typed_func::<(), ()>(&mut store, "convex_wasm_udf_destroy_runtime")?;
            let result = async {
                for (index, selector) in preparation_entry_selectors.iter().enumerate() {
                    if index != 0 {
                        generated_state_mut(store.data_mut())?
                            .capability_bridge
                            .begin_initialization()?;
                    }
                    store.set_fuel(GENERATED_INITIALIZATION_FUEL)?;
                    let selection_status = select.call_async(&mut store, *selector as i64).await?;
                    anyhow::ensure!(
                        selection_status == 0,
                        "preparation sequence selector {index} rejected: {selection_status}"
                    );
                    let status = prepare.call_async(&mut store, ()).await?;
                    anyhow::ensure!(
                        status == 0,
                        "preparation sequence entry {index} ({selector:016x}) failed: \
                         status={status}, diagnostic={:?}",
                        store
                            .data()
                            .developer_error
                            .as_ref()
                            .map(|error| &error.message)
                    );
                    generated_state_mut(store.data_mut())?
                        .capability_bridge
                        .finish_initialization()?;
                    println!("prepared entry {index}: {selector:016x}");
                }
                Ok(())
            }
            .await;
            // Destroy guest state even when an intermediate module fails.
            destroy.call_async(&mut store, ()).await?;
            return result;
        }
        arm_generated_timeout(
            rt.clone(),
            &mut first_state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let mut first = execute_generated_with_entry_selector(
            Arc::clone(&execution_routed),
            execution_routed.entry_selector,
            first_state,
            None,
            true,
        )
        .await?;
        anyhow::ensure!(
            first.invocation.outcome == InvocationOutcome::Success
                && first
                    .invocation
                    .function_result
                    .as_ref()
                    .is_some_and(|actual| {
                        application_capability_entry_tests::application_result_values_equal(
                            &expected_result,
                            actual,
                        )
                    })
                && metrics.fresh_initialization_attempts.load(Ordering::SeqCst) == 1,
            "authenticated graph route did not initialize once and return the expected result: \
             outcome={:?}, result={:?}, system_error={:?}, cancelled={}, \
             fresh_initialization_attempts={}",
            first.invocation.outcome,
            first.invocation.function_result,
            first.system_error,
            first.cancelled,
            metrics.fresh_initialization_attempts.load(Ordering::SeqCst),
        );
        let first_runtime_id = first.runtime_id;
        let retained = retain_generated_test_runtime(&mut first)?;
        drop(first.invocation.transaction);

        let mut second_state = generated_routed_test_state(
            rt.clone(),
            database.begin_system().await?,
            &execution_routed,
            request,
            GeneratedRoutedTestMemorySetup {
                controller: Arc::clone(&execution_controller),
                existing_slot: Some(retained.memory_slot_id),
                memory_identity: execution_memory_identity.clone(),
                values: Some(retained.values),
            },
            Arc::clone(&metrics),
        )
        .await?;
        second_state.provider.set_udf_type(udf_kind.udf_type());
        let timeout = second_state
            .timeout
            .as_mut()
            .context("module graph test timeout missing")?;
        second_state
            .provider
            .initialize_static_hermes(timeout)
            .await?;
        arm_generated_timeout(
            rt,
            &mut second_state,
            Duration::from_secs(5),
            *DATABASE_UDF_SYSTEM_TIMEOUT,
        )?;
        let second = execute_generated_with_entry_selector(
            Arc::clone(&execution_routed),
            execution_routed.entry_selector,
            second_state,
            Some(retained.instance),
            false,
        )
        .await?;
        anyhow::ensure!(
            second.invocation.outcome == InvocationOutcome::Success
                && second
                    .invocation
                    .function_result
                    .as_ref()
                    .is_some_and(|actual| {
                        application_capability_entry_tests::application_result_values_equal(
                            &expected_result,
                            actual,
                        )
                    })
                && second.runtime_id == first_runtime_id
                && metrics.fresh_initialization_attempts.load(Ordering::SeqCst) == 1
                && second.reusable_instance.is_none()
                && second.memory_permit.is_none()
                && metrics.teardowns.load(Ordering::SeqCst) == 1,
            "authenticated graph route did not reuse its initialized runtime, preserve its \
             result, and discard it after the second execution"
        );
        let completed_memory = execution_controller.snapshot_for_test(&execution_memory_identity);
        anyhow::ensure!(
            completed_memory.active_instances == 0
                && completed_memory.idle_instances == 0
                && completed_memory.evicting_instances == 0
                && completed_memory.retained_baseline_bytes == 0
                && completed_memory.active_checkout_baseline_bytes == 0
                && completed_memory.active_guest_bytes == 0
                && completed_memory.active_host_bytes == 0,
            "discarded authenticated graph runtime retained invocation memory accounting"
        );
        let phases = metrics.generated_execution_phases.lock().clone();
        anyhow::ensure!(
            phases.len() == 2,
            "authenticated graph route expected two execution phase observations, got {}",
            phases.len()
        );
        let cold = &phases[0];
        let warm = &phases[1];
        anyhow::ensure!(
            cold.fresh_runtime
                && !warm.fresh_runtime
                && cold.instantiation_completed.is_some()
                && cold.initialization_started.is_some()
                && cold.initialization_completed.is_some()
                && cold.entry_selection_started.is_some()
                && cold.entry_selection_completed.is_some()
                && warm.instantiation_completed.is_none()
                && warm.initialization_started.is_none()
                && warm.initialization_completed.is_none()
                && warm.entry_selection_started.is_some()
                && warm.entry_selection_completed.is_some(),
            "authenticated graph route phase trace did not distinguish fresh initialization from \
             warm reuse"
        );
        let cold_initial_fuel = cold
            .initial_fuel
            .context("cold phase did not record initial fuel")?;
        let cold_handler_start_fuel = cold
            .handler_start_fuel
            .context("cold phase did not record handler-start fuel")?;
        let cold_handler_end_fuel = cold
            .handler_end_fuel
            .context("cold phase did not record handler-end fuel")?;
        let cold_final_fuel = cold
            .final_fuel
            .context("cold phase did not record final fuel")?;
        let warm_initial_fuel = warm
            .initial_fuel
            .context("warm phase did not record initial fuel")?;
        let warm_handler_start_fuel = warm
            .handler_start_fuel
            .context("warm phase did not record handler-start fuel")?;
        let warm_handler_end_fuel = warm
            .handler_end_fuel
            .context("warm phase did not record handler-end fuel")?;
        let warm_final_fuel = warm
            .final_fuel
            .context("warm phase did not record final fuel")?;
        let cold_handler_started = cold
            .handler_started
            .context("cold phase did not start the handler")?;
        let cold_handler_completed = cold
            .handler_completed
            .context("cold phase did not complete the handler")?;
        let warm_handler_started = warm
            .handler_started
            .context("warm phase did not start the handler")?;
        let warm_handler_completed = warm
            .handler_completed
            .context("warm phase did not complete the handler")?;
        anyhow::ensure!(
            cold.handler_completion == Some("returned")
                && warm.handler_completion == Some("returned")
                && !cold.cancellation_observed
                && !warm.cancellation_observed
                && cold_initial_fuel >= cold_handler_start_fuel
                && cold_handler_start_fuel >= cold_handler_end_fuel
                && cold_handler_end_fuel >= cold_final_fuel
                && warm_initial_fuel >= warm_handler_start_fuel
                && warm_handler_start_fuel >= warm_handler_end_fuel
                && warm_handler_end_fuel >= warm_final_fuel,
            "authenticated graph route phase trace did not record successful monotonic fuel use"
        );
        println!(
            "{}",
            json!({
                "cold": {
                    "handlerDurationMicros": cold_handler_completed
                        .saturating_sub(cold_handler_started)
                        .as_micros(),
                    "handlerFuelUsed": cold_handler_start_fuel - cold_handler_end_fuel,
                    "initializationDurationMicros": cold
                        .initialization_completed
                        .expect("checked cold initialization completion disappeared")
                        .saturating_sub(
                            cold.initialization_started
                                .expect("checked cold initialization start disappeared"),
                        )
                        .as_micros(),
                    "preHandlerFuelUsed": cold_initial_fuel - cold_handler_start_fuel,
                    "totalFuelUsed": cold_initial_fuel - cold_final_fuel,
                    "totalMicros": cold.total.as_micros(),
                },
                "expectedResult": expected_result,
                "freshInitializationAttempts": metrics
                    .fresh_initialization_attempts
                    .load(Ordering::SeqCst),
                "generationSha256": generation.generation_sha256.as_str(),
                "graphManifestSha256": graph_sha256,
                "packageKey": route.package_key.as_str(),
                "routeId": route.route_lease.route_id(),
                "runtimeId": second.runtime_id,
                "runtimeReused": second.runtime_id == first_runtime_id,
                "warm": {
                    "handlerDurationMicros": warm_handler_completed
                        .saturating_sub(warm_handler_started)
                        .as_micros(),
                    "handlerFuelUsed": warm_handler_start_fuel - warm_handler_end_fuel,
                    "preHandlerFuelUsed": warm_initial_fuel - warm_handler_start_fuel,
                    "totalFuelUsed": warm_initial_fuel - warm_final_fuel,
                    "totalMicros": warm.total.as_micros(),
                },
            })
        );
        drop(second.invocation.transaction);
        database.shutdown().await
    })?;

    drop(cached);
    let removed = GENERATED_ROUTED_MODULES
        .lock()
        .remove(
            &routed.route_identity,
            "module_cache_evicted_real_module_graph_registry_v5_test",
        )
        .context("authenticated graph route was not cached")?;
    anyhow::ensure!(
        Arc::ptr_eq(&removed, &routed),
        "real module graph test removed an unexpected cached route"
    );
    drop(removed);
    drop(routed);
    GENERATED_ROUTED_MODULES
        .lock()
        .evict_all_idle_shared_aot_modules();
    anyhow::ensure!(
        controller
            .snapshot_for_test(&memory_identity)
            .fixed_module_bytes
            == 0,
        "authenticated graph test cleanup retained a shared AOT module charge"
    );
    Ok(())
}

#[test]
fn generated_host_import_signatures_include_emscripten_filesystem_probes() {
    for (name, parameter_count) in [
        ("__syscall_faccessat", 4),
        ("__syscall_getcwd", 2),
        ("__syscall_readlinkat", 4),
    ] {
        let (parameters, results) =
            generated_host_import_signature("env", name).expect("filesystem probe is supported");
        assert!(
            parameters.len() == parameter_count
                && parameters
                    .iter()
                    .all(|parameter| ValType::eq(parameter, &ValType::I32))
                && results.len() == 1
                && ValType::eq(&results[0], &ValType::I32)
        );
    }
}

#[test]
fn generated_wasm_module_contract_rejects_unsupported_host_import() -> anyhow::Result<()> {
    let mut types = EncodedTypeSection::new();
    types.ty().function(std::iter::empty(), std::iter::empty());
    types
        .ty()
        .function([EncodedValType::I64], std::iter::empty());
    types
        .ty()
        .function(std::iter::empty(), [EncodedValType::I32]);
    let mut imports = EncodedImportSection::new();
    imports.import("unsupported", "call", EncodedEntityType::Function(0));
    imports.import(
        "convex",
        "convex_function_result",
        EncodedEntityType::Function(1),
    );
    let mut functions = EncodedFunctionSection::new();
    functions.function(0);
    functions.function(2);
    functions.function(0);
    functions.function(2);
    let mut exports = EncodedExportSection::new();
    exports.export("_initialize", EncodedExportKind::Func, 2);
    exports.export("convex_wasm_udf_run", EncodedExportKind::Func, 3);
    exports.export(
        "convex_wasm_udf_destroy_runtime",
        EncodedExportKind::Func,
        4,
    );
    exports.export(
        "convex_wasm_udf_prepare_selected_entry",
        EncodedExportKind::Func,
        5,
    );
    let mut initialize = EncodedFunction::new([]);
    initialize.instruction(&EncodedInstruction::End);
    let mut run = EncodedFunction::new([]);
    run.instruction(&EncodedInstruction::I32Const(0));
    run.instruction(&EncodedInstruction::End);
    let mut destroy = EncodedFunction::new([]);
    destroy.instruction(&EncodedInstruction::End);
    let mut prepare = EncodedFunction::new([]);
    prepare.instruction(&EncodedInstruction::I32Const(0));
    prepare.instruction(&EncodedInstruction::End);
    let mut code = EncodedCodeSection::new();
    code.function(&initialize);
    code.function(&run);
    code.function(&destroy);
    code.function(&prepare);
    let mut encoded = EncodedModule::new();
    encoded
        .section(&types)
        .section(&imports)
        .section(&functions)
        .section(&exports)
        .section(&code);

    let engine = shared_generated_engine()?;
    let module = Module::new(&engine, encoded.finish()).map_err(wasmtime_anyhow)?;
    let manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
    let error = validate_generated_module_contract(&module, &manifest)
        .expect_err("unsupported host import passed generated module validation");
    assert!(error.to_string().contains("unsupported host import"));
    Ok(())
}

#[test]
fn generated_wasm_module_contract_fails_closed_across_value_modes() -> anyhow::Result<()> {
    let opaque_manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
    let mut value = serde_json::to_value(opaque_manifest.as_ref())?;
    value["valueMode"] = json!("guest-native-json");
    let engine = shared_generated_engine()?;
    let guest_manifest = WasmUdfExecutionManifest::parse_for_runtime(
        &serde_json::to_vec(&value)?,
        &generated_runtime_compatibility(GENERATED_ENGINE_COMPATIBILITY_SHA256)?,
    )?;
    let module = Module::new(
        &engine,
        generated_memory_test_module(GeneratedMemoryTestOperation {
            maximum_pages: 1,
            additional_pages: 0,
            loop_iterations: Some(0),
            destroy_infinite_loop: false,
        }),
    )?;
    let error = validate_generated_module_contract(&module, &guest_manifest)
        .expect_err("opaque imports passed a guest-native manifest");
    assert!(
        error.to_string().contains("different manifest value mode")
            || error
                .to_string()
                .contains("guest-value decode imports required"),
        "unexpected module-contract error: {error:#}"
    );
    Ok(())
}

#[test]
fn generated_wasm_guest_native_owned_payload_imports_are_complete() -> anyhow::Result<()> {
    let payload_imports = BTreeSet::from([
        "convex_guest_value_payload_len".to_owned(),
        "convex_guest_value_payload_copy".to_owned(),
        "convex_guest_value_payload_release".to_owned(),
    ]);
    validate_guest_value_decode_imports(&payload_imports)?;
    for removed in &payload_imports {
        let mut incomplete = payload_imports.clone();
        incomplete.remove(removed);
        validate_guest_value_decode_imports(&incomplete)
            .expect_err("incomplete owned-payload imports passed module validation");
    }
    validate_guest_value_decode_imports(&BTreeSet::from(["convex_guest_value_decode".to_owned()]))?;
    Ok(())
}

#[test]
fn generated_wasm_function_handle_import_requires_manifest_descriptor() -> anyhow::Result<()> {
    let manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
    let engine = shared_generated_engine()?;
    let module = Module::new(
        &engine,
        generated_test_module(GeneratedTestOperation::FunctionHandleCreate(
            GeneratedFunctionHandleTestOperation {
                operation_id: 1,
                stale_reference: false,
            },
        )),
    )
    .map_err(wasmtime_anyhow)?;
    let error = validate_generated_module_contract(&module, &manifest)
        .expect_err("undeclared function-handle import passed module validation");
    assert!(error
        .to_string()
        .contains("not declared by its authenticated package members"));
    Ok(())
}

#[test]
fn generated_wasm_host_secret_import_and_selector_authorization_are_exact() -> anyhow::Result<()> {
    let engine = shared_generated_engine()?;
    let host_secret_module = Module::new(
        &engine,
        generated_host_secret_verify_test_module(1, 64, 6, b"secret"),
    )
    .map_err(wasmtime_anyhow)?;
    let empty_manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
    let error = validate_generated_module_contract(&host_secret_module, &empty_manifest)
        .expect_err("undeclared host-secret import passed module validation");
    assert!(error
        .to_string()
        .contains("not declared by its authenticated package members"));

    let operations = vec![
        json!({
            "id": 1,
            "debugName": "verifyFirstHostSecret",
            "operation": {
                "kind": "hostSecretVerify",
                "contractVersion": 1,
                "selector": "FIRST_SECRET",
            },
        }),
        json!({
            "id": 2,
            "debugName": "verifySecondHostSecret",
            "operation": {
                "kind": "hostSecretVerify",
                "contractVersion": 1,
                "selector": "SECOND_SECRET",
            },
        }),
    ];
    let manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, operations, 1 << 20)?;
    validate_generated_module_contract(&host_secret_module, &manifest)?;

    let configured = BTreeSet::from(["FIRST_SECRET".to_owned(), "UNDECLARED_SECRET".to_owned()]);
    let execution = WasmUdfExecutionPolicy::from_legacy(&manifest);
    assert_eq!(
        authorized_host_secret_selectors_in_manifest(&execution, &configured),
        BTreeSet::from(["FIRST_SECRET".to_owned()])
    );

    let module_without_host_secret = Module::new(
        &engine,
        generated_memory_test_module(GeneratedMemoryTestOperation {
            maximum_pages: 1,
            additional_pages: 0,
            loop_iterations: Some(0),
            destroy_infinite_loop: false,
        }),
    )?;
    let error = validate_generated_module_contract(&module_without_host_secret, &manifest)
        .expect_err("manifest host-secret descriptor passed without its required import");
    assert!(error.to_string().contains("omits an import required"));
    Ok(())
}

#[test]
fn generated_wasm_member_import_union_authorizes_only_declared_imports() -> anyhow::Result<()> {
    let manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
    let engine = shared_generated_engine()?;
    let module = Module::new(
        &engine,
        generated_test_module(GeneratedTestOperation::FunctionHandleCreate(
            GeneratedFunctionHandleTestOperation {
                operation_id: 1,
                stale_reference: false,
            },
        )),
    )
    .map_err(wasmtime_anyhow)?;
    let mut permitted = permitted_conditional_convex_imports(
        manifest.value_mode(),
        manifest.effect_execution_mode(),
        manifest.imported_operations(),
    );
    let execution = WasmUdfExecutionPolicy::from_legacy(&manifest);
    let requires_entry_selector = matches!(
        manifest.manifest_schema_version(),
        COHORT_MANIFEST_SCHEMA_VERSION
            | COMPOUND_QUERY_MANIFEST_SCHEMA_VERSION
            | EFFECT_EXECUTION_MANIFEST_SCHEMA_VERSION
    );
    permitted.insert("convex_function_handle_create");
    validate_generated_module_contract_with_imports(
        &module,
        &execution,
        &permitted,
        requires_entry_selector,
    )?;

    permitted.remove("convex_function_handle_create");
    permitted.insert("convex_db_get");
    let error = validate_generated_module_contract_with_imports(
        &module,
        &execution,
        &permitted,
        requires_entry_selector,
    )
    .expect_err("an import outside the supplied member union passed module validation");
    assert!(error
        .to_string()
        .contains("not declared by its authenticated package members"));
    Ok(())
}

#[test]
fn generated_wasm_database_normalize_id_import_requires_manifest_descriptor() -> anyhow::Result<()>
{
    let manifest =
        generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 1 << 20)?;
    let engine = shared_generated_engine()?;
    let module = Module::new(
        &engine,
        generated_database_normalize_id_test_module(1, false, false),
    )
    .map_err(wasmtime_anyhow)?;
    let error = validate_generated_module_contract(&module, &manifest)
        .expect_err("undeclared database normalizeId import passed module validation");
    assert!(error
        .to_string()
        .contains("not declared by its authenticated package members"));
    Ok(())
}

#[test]
fn native_capability_test_expectation_accepts_bound_report() -> anyhow::Result<()> {
    let module_bytes = b"native capability Core Wasm";
    let report = native_capability_test_expectation_report(module_bytes);
    let expectation = NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?)?;

    assert_eq!(expectation.primary.entry_selector, 0x1111_1111_1111_1111);
    assert_eq!(expectation.primary.handler_export_name, "A");
    assert_eq!(expectation.alternate.entry_selector, 0x2222_2222_2222_2222);
    assert_eq!(expectation.alternate.handler_export_name, "B");
    assert_eq!(
        expectation.partial_initialization_failure.failure_trace,
        0x0105
    );
    assert_eq!(
        expectation.partial_initialization_failure.recovery_trace,
        0x0102_0304
    );
    assert_eq!(
        expectation.execution.value_mode(),
        ValueMode::GuestNativeJson
    );
    assert_eq!(
        expectation.execution.effect_execution_mode(),
        EffectExecutionMode::GuestPromiseEventLoop
    );
    assert!(expectation.execution.imported_operations().is_empty());
    expectation.validate_module_bytes(module_bytes)?;
    Ok(())
}

#[test]
fn native_capability_test_expectation_preserves_authenticated_route_identity() -> anyhow::Result<()>
{
    let module_bytes = b"native capability Core Wasm";
    let expectation = NativeCapabilityTestExpectation::parse(&serde_json::to_vec(
        &native_capability_test_expectation_report(module_bytes),
    )?)?;

    assert_eq!(expectation.package_key, "a".repeat(64));
    assert_eq!(expectation.primary.entry_id, "b".repeat(64));
    assert_eq!(expectation.primary.route_id, "c".repeat(64));
    assert_eq!(
        expectation.primary.entry_path,
        "convex/nativeCapabilityApplicationA.ts"
    );
    assert_eq!(
        expectation.primary.runtime_module_path,
        "nativeCapabilityApplicationA.js"
    );
    assert_eq!(expectation.alternate.entry_id, "d".repeat(64));
    assert_eq!(expectation.alternate.route_id, "e".repeat(64));
    assert_eq!(
        expectation.alternate.entry_path,
        "convex/nativeCapabilityApplicationB.ts"
    );
    assert_eq!(
        expectation.alternate.runtime_module_path,
        "nativeCapabilityApplicationB.js"
    );
    assert_eq!(
        expectation.execution.value_mode(),
        ValueMode::GuestNativeJson
    );
    assert_eq!(
        expectation.execution.effect_execution_mode(),
        EffectExecutionMode::GuestPromiseEventLoop
    );
    assert!(expectation.execution.imported_operations().is_empty());
    Ok(())
}

#[test]
fn native_capability_test_document_is_guest_native_encodable() -> anyhow::Result<()> {
    let document = backend_gate_document(ConvexValue::Float64(1.0))?;
    GuestNativeValueCodec::encode(document.to_internal_json(), MAX_RESULT_BYTES)?;
    Ok(())
}

#[test]
fn native_capability_test_expectation_rejects_unknown_and_missing_fields() {
    let module_bytes = b"native capability Core Wasm";

    let mut report = native_capability_test_expectation_report(module_bytes);
    report
        .as_object_mut()
        .expect("test report is an object")
        .insert("unexpected".to_owned(), json!(true));
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report
        .as_object_mut()
        .expect("test report is an object")
        .remove("packageKey");
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["kind"] = json!("convex-wasm-native-capability-test-expectation-v2");
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    for entry_selector_abi_version in [1, 3] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        report["abi"]["entrySelectorAbiVersion"] = json!(entry_selector_abi_version);
        assert!(NativeCapabilityTestExpectation::parse(
            &serde_json::to_vec(&report).expect("test report serializes")
        )
        .is_err());
    }

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["module"]
        .as_object_mut()
        .expect("test module report is an object")
        .insert("unexpected".to_owned(), json!(true));
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["partialInitializationFailure"]
        .as_object_mut()
        .expect("test failure fixture report is an object")
        .insert("unexpected".to_owned(), json!(true));
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report
        .as_object_mut()
        .expect("test report is an object")
        .remove("partialInitializationFailure");
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report
        .as_object_mut()
        .expect("test report is an object")
        .remove("kind");
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["module"]
        .as_object_mut()
        .expect("test module report is an object")
        .remove("size");
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["selectors"][0]
        .as_object_mut()
        .expect("test selector report is an object")
        .insert("unexpected".to_owned(), json!(true));
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["selectors"][0]
        .as_object_mut()
        .expect("test selector report is an object")
        .remove("handlerExportName");
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["selectors"][0]
        .as_object_mut()
        .expect("test selector report is an object")
        .remove("invocationAbi");
    assert!(NativeCapabilityTestExpectation::parse(
        &serde_json::to_vec(&report).expect("test report serializes")
    )
    .is_err());
}

#[test]
fn native_capability_test_expectation_rejects_partial_initialization_fixture_tampering(
) -> anyhow::Result<()> {
    let module_bytes = b"native capability Core Wasm";
    for (field, tampered) in [
        ("kind", json!("unknown-fixture")),
        ("armExportName", json!("forged_arm")),
        ("traceExportName", json!("forged_trace")),
        ("failureStatus", json!(61)),
        ("failureTrace", json!(0x0102)),
        ("recoveryTrace", json!(0x0102_0305)),
        (
            "applicationEntryOrder",
            json!([
                "convex/nativeCapabilityApplicationB.ts",
                "convex/nativeCapabilityApplicationA.ts",
            ]),
        ),
        (
            "initializationOrder",
            json!([
                "shared-untyped-runtime-support",
                "shared-typed-capability-bridge",
                "untyped-applications-by-entry-slot",
            ]),
        ),
    ] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        report["partialInitializationFailure"][field] = tampered;
        assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    }
    Ok(())
}

#[test]
fn native_capability_test_expectation_rejects_invalid_module_identity() -> anyhow::Result<()> {
    let module_bytes = b"native capability Core Wasm";

    for invalid_package_key in ["a".repeat(63), "A".repeat(64), "g".repeat(64)] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        report["packageKey"] = json!(invalid_package_key);
        assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    }

    for invalid_sha256 in ["a".repeat(63), "A".repeat(64), "g".repeat(64)] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        report["module"]["sha256"] = json!(invalid_sha256);
        assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    }

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["module"]["size"] = json!(0);
    assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());

    let report = native_capability_test_expectation_report(module_bytes);
    let expectation = NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?)?;
    let mut wrong_digest = module_bytes.to_vec();
    wrong_digest[0] ^= 1;
    assert!(expectation.validate_module_bytes(&wrong_digest).is_err());
    let mut wrong_size = module_bytes.to_vec();
    wrong_size.push(0);
    assert!(expectation.validate_module_bytes(&wrong_size).is_err());
    Ok(())
}

#[test]
fn native_capability_test_expectation_rejects_invalid_selector_identity() -> anyhow::Result<()> {
    let module_bytes = b"native capability Core Wasm";

    for (field, invalid_values) in [
        (
            "entryId",
            vec![
                json!("b".repeat(63)),
                json!("B".repeat(64)),
                json!("g".repeat(64)),
            ],
        ),
        (
            "routeId",
            vec![
                json!("c".repeat(63)),
                json!("C".repeat(64)),
                json!("g".repeat(64)),
            ],
        ),
        (
            "entryPath",
            vec![
                json!(""),
                json!(format!("convex{}entry.ts", char::from(0_u8))),
            ],
        ),
        (
            "runtimeModulePath",
            vec![json!(""), json!(format!("entry{}.js", char::from(0_u8)))],
        ),
        ("visibility", vec![json!("private"), json!("")]),
    ] {
        for invalid_value in invalid_values {
            let mut report = native_capability_test_expectation_report(module_bytes);
            report["selectors"][0][field] = invalid_value;
            assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
        }
    }

    for field in [
        "entryId",
        "entryPath",
        "routeId",
        "runtimeModulePath",
        "visibility",
    ] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        report["selectors"][0]
            .as_object_mut()
            .context("native capability selector fixture must be an object")?
            .remove(field);
        assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    }

    for invalid_selector_id in ["1".repeat(15), "A".repeat(16), "g".repeat(16)] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        report["selectors"][0]["entrySelectorId"] = json!(invalid_selector_id);
        assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    }

    let mut report = native_capability_test_expectation_report(module_bytes);
    let duplicate_selector_id = report["selectors"][0]["entrySelectorId"].clone();
    report["selectors"][1]["entrySelectorId"] = duplicate_selector_id;
    assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["selectors"][1]["role"] = json!("primary");
    assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());

    for selector_count in [1, 3] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        let selectors = report["selectors"]
            .as_array_mut()
            .expect("test selectors are an array");
        if selector_count == 1 {
            selectors.pop();
        } else {
            selectors.push(json!({
                "role": "primary",
                "entryId": "f".repeat(64),
                "entryPath": "convex/nativeCapabilityApplicationC.ts",
                "entrySelectorId": "3333333333333333",
                "handlerExportName": "C",
                "handlerUdfKind": "query",
                "invocationAbi": "convex-wasm-legacy-custom-context-handler-v1",
                "routeId": "0".repeat(64),
                "runtimeModulePath": "nativeCapabilityApplicationC.js",
                "visibility": "public",
            }));
        }
        assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    }
    Ok(())
}

#[test]
fn native_capability_test_expectation_rejects_invalid_or_duplicate_export_names(
) -> anyhow::Result<()> {
    let module_bytes = b"native capability Core Wasm";

    for invalid_export_name in ["", "1A", "A-B", "A B", "A.b", "exporté"] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        report["selectors"][0]["handlerExportName"] = json!(invalid_export_name);
        assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    }

    let mut report = native_capability_test_expectation_report(module_bytes);
    let duplicate_export_name = report["selectors"][0]["handlerExportName"].clone();
    report["selectors"][1]["handlerExportName"] = duplicate_export_name;
    assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["selectors"][0]["invocationAbi"] =
        json!("convex-sdk-registration-wrapper-tagged-json-v1");
    assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    Ok(())
}

#[test]
fn native_capability_test_expectation_rejects_unsupported_udf_kind() -> anyhow::Result<()> {
    let module_bytes = b"native capability Core Wasm";
    for handler_udf_kind in ["mutation", "action"] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        report["selectors"][1]["handlerUdfKind"] = json!(handler_udf_kind);
        assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    }
    Ok(())
}

#[test]
fn native_capability_test_expectation_rejects_invalid_execution_policy() -> anyhow::Result<()> {
    let module_bytes = b"native capability Core Wasm";

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["execution"]["valueMode"] = json!("opaque");
    assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());

    let mut report = native_capability_test_expectation_report(module_bytes);
    report["execution"]["importedOperations"] = json!([{
        "debugName": "unexpectedSha256",
        "id": 1,
        "operation": { "kind": "sha256" },
    }]);
    assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());

    for identity in ["requestEnvelope", "valueCodec"] {
        let mut report = native_capability_test_expectation_report(module_bytes);
        report["execution"]
            .as_object_mut()
            .context("native capability execution fixture must be an object")?
            .remove(identity);
        assert!(NativeCapabilityTestExpectation::parse(&serde_json::to_vec(&report)?).is_err());
    }
    Ok(())
}

#[cfg(test)]
fn native_capability_test_expectation_report(module_bytes: &[u8]) -> JsonValue {
    json!({
        "abi": {
            "capabilityRequestAbiVersion": 4,
            "entrySelectorAbiVersion": 2,
            "opaqueValueAbiVersion": OPAQUE_VALUE_ABI_VERSION,
        },
        "execution": {
            "effectExecutionMode": "guest-promise-event-loop",
            "importedOperations": [],
            "limits": {
                "executionFuel": 2_000_000_000_u64,
                "maxGuestMemoryBytes": 32 * 1024 * 1024,
                "maxHostOwnedBytes": 16 * 1024 * 1024,
                "maxOperationCount": 10_000,
                "maxResultBytes": 16 * 1024 * 1024,
                "maxValueHandles": 4_096,
                "timeoutMilliseconds": 1_000,
            },
            "platformLimits": PlatformLimits::authoritative()
                .expect("test platform limits are valid"),
            "requestEnvelope": {
                "kind": "test-capability-request-envelope-identity",
            },
            "valueCodec": {
                "kind": "test-capability-value-codec-identity",
            },
            "valueMode": "guest-native-json",
        },
        "kind": NATIVE_CAPABILITY_TEST_EXPECTATION_KIND,
        "module": {
            "sha256": format!("{:x}", Sha256::digest(module_bytes)),
            "size": module_bytes.len(),
        },
        "packageKey": "a".repeat(64),
        "partialInitializationFailure": {
            "applicationEntryOrder": [
                "convex/nativeCapabilityApplicationA.ts",
                "convex/nativeCapabilityApplicationB.ts",
            ],
            "armExportName": NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_ARM_EXPORT,
            "failureStatus": 62,
            "failureTrace": 0x0105,
            "initializationOrder": [
                "shared-typed-capability-bridge",
                "shared-untyped-runtime-support",
                "untyped-applications-by-entry-slot",
            ],
            "kind": NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_FIXTURE_KIND,
            "recoveryTrace": 0x0102_0304,
            "traceExportName": NATIVE_CAPABILITY_PARTIAL_INITIALIZATION_TRACE_EXPORT,
        },
        "selectors": [
            {
                "role": "primary",
                "entryId": "b".repeat(64),
                "entryPath": "convex/nativeCapabilityApplicationA.ts",
                "entrySelectorId": "1111111111111111",
                "handlerExportName": "A",
                "handlerUdfKind": "query",
                "invocationAbi": "convex-wasm-legacy-custom-context-handler-v1",
                "routeId": "c".repeat(64),
                "runtimeModulePath": "nativeCapabilityApplicationA.js",
                "visibility": "public",
            },
            {
                "role": "alternate",
                "entryId": "d".repeat(64),
                "entryPath": "convex/nativeCapabilityApplicationB.ts",
                "entrySelectorId": "2222222222222222",
                "handlerExportName": "B",
                "handlerUdfKind": "query",
                "invocationAbi": "convex-wasm-legacy-custom-context-handler-v1",
                "routeId": "e".repeat(64),
                "runtimeModulePath": "nativeCapabilityApplicationB.js",
                "visibility": "public",
            },
        ],
    })
}
