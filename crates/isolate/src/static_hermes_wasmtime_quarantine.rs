//! Durable, subtractive route-admission policy for the Wasm runtime.
//!
//! A matching selector stays quarantined across runtime-generation changes
//! until an authenticated control-plane action clears that exact selector.
//! Route admission observes one immutable snapshot and never reroutes an
//! invocation after its engine has been selected.

use std::{
    collections::BTreeMap,
    fmt,
    fs::{
        self,
        File,
        OpenOptions,
    },
    io::{
        ErrorKind,
        Read,
        Write,
    },
    path::{
        Path,
        PathBuf,
    },
    sync::{
        Arc,
        OnceLock,
        RwLock,
    },
};

use anyhow::Context as _;
use metrics::register_convex_counter;
use serde::{
    Deserialize,
    Serialize,
};
use sha2::{
    Digest,
    Sha256,
};
use sync_types::{
    CanonicalizedModulePath,
    FunctionName,
};

const MAX_ENTRIES: usize = 1_024;
const MAX_MODULE_PATH_BYTES: usize = 512;
const MAX_FUNCTION_NAME_BYTES: usize = 128;
const MAX_REASON_BYTES: usize = 256;
const MAX_OPERATOR_REFERENCE_BYTES: usize = 256;
const MAX_POLICY_BYTES: usize = 1 << 20;
const POLICY_SCHEMA_VERSION: u32 = 1;

register_convex_counter!(
    STATIC_HERMES_WASMTIME_QUARANTINE_FORCED_V8_TOTAL,
    "Wasm route admissions forced to V8 by quarantine"
);

pub(crate) fn record_forced_to_v8() {
    metrics::log_counter(&STATIC_HERMES_WASMTIME_QUARANTINE_FORCED_V8_TOTAL, 1);
}

/// A bounded exact route selector. `function_name = None` applies to every
/// export in `module_path`.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct StaticHermesWasmtimeQuarantineSelector {
    module_path: String,
    function_name: Option<String>,
}

impl StaticHermesWasmtimeQuarantineSelector {
    pub fn new(
        module_path: impl Into<String>,
        function_name: Option<String>,
    ) -> anyhow::Result<Self> {
        let canonical_module_path = module_path.into().parse::<CanonicalizedModulePath>()?;
        let selector = Self {
            module_path: canonical_module_path.as_str().to_owned(),
            function_name,
        };
        selector.validate()?;
        Ok(selector)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.module_path.is_empty() && self.module_path.len() <= MAX_MODULE_PATH_BYTES,
            "Wasm quarantine module path is empty or too long"
        );
        let module_path = self.module_path.parse::<CanonicalizedModulePath>()?;
        anyhow::ensure!(
            !module_path.is_system(),
            "Wasm quarantine cannot target a system module"
        );
        if let Some(function_name) = &self.function_name {
            anyhow::ensure!(
                !function_name.is_empty() && function_name.len() <= MAX_FUNCTION_NAME_BYTES,
                "Wasm quarantine function name is empty or too long"
            );
            function_name.parse::<FunctionName>()?;
        }
        Ok(())
    }

    pub fn module_path(&self) -> &str {
        &self.module_path
    }

    pub fn function_name(&self) -> Option<&str> {
        self.function_name.as_deref()
    }

    fn matches(&self, module_path: &str, function_name: &str) -> bool {
        self.module_path == module_path
            && self
                .function_name
                .as_deref()
                .is_none_or(|selector| selector == function_name)
    }

    /// A stable, data-free identifier suitable for bounded audit evidence.
    pub fn digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.module_path.as_bytes());
        hasher.update([0]);
        if let Some(function_name) = &self.function_name {
            hasher.update(function_name.as_bytes());
        }
        hex_digest(hasher)
    }
}

/// An authenticated runtime-generation identity supplied to an admission
/// decision. It is kept separate from policy state and never persisted from a
/// request path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticHermesWasmtimeSourceIdentity {
    generation_sha256: String,
}

impl StaticHermesWasmtimeSourceIdentity {
    pub fn new(generation_sha256: impl Into<String>) -> anyhow::Result<Self> {
        let identity = Self {
            generation_sha256: generation_sha256.into(),
        };
        anyhow::ensure!(
            is_sha256(&identity.generation_sha256),
            "Wasm quarantine source generation must be a lowercase SHA-256 digest"
        );
        Ok(identity)
    }

    pub fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StaticHermesWasmtimeQuarantineEntry {
    pub selector: StaticHermesWasmtimeQuarantineSelector,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticHermesWasmtimeRouteDecision {
    AdmitWasm,
    QuarantineToV8,
    AmbiguousToV8,
}

impl StaticHermesWasmtimeRouteDecision {
    pub fn uses_wasm(self) -> bool {
        matches!(self, Self::AdmitWasm)
    }

    pub fn uses_v8(self) -> bool {
        !self.uses_wasm()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticHermesWasmtimeQuarantineSnapshot {
    version: u64,
    ambiguous: bool,
    entries: BTreeMap<StaticHermesWasmtimeQuarantineSelector, ()>,
}

impl StaticHermesWasmtimeQuarantineSnapshot {
    fn empty(version: u64) -> Self {
        Self {
            version,
            ambiguous: false,
            entries: BTreeMap::new(),
        }
    }

    fn ambiguous(version: u64) -> Self {
        Self {
            version,
            ambiguous: true,
            entries: BTreeMap::new(),
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn is_ambiguous(&self) -> bool {
        self.ambiguous
    }

    pub fn entries(&self) -> Vec<StaticHermesWasmtimeQuarantineEntry> {
        self.entries
            .keys()
            .cloned()
            .map(|selector| StaticHermesWasmtimeQuarantineEntry { selector })
            .collect()
    }

    /// Evaluate only this immutable snapshot. A matching selector with no
    /// trusted source identity remains fail-closed.
    pub fn evaluate_generation(
        &self,
        module_path: &str,
        function_name: &str,
        generation_sha256: Option<&str>,
    ) -> StaticHermesWasmtimeRouteDecision {
        let source_identity = generation_sha256
            .and_then(|generation| StaticHermesWasmtimeSourceIdentity::new(generation).ok());
        self.decide(module_path, function_name, source_identity)
    }

    fn decide(
        &self,
        module_path: &str,
        function_name: &str,
        source_identity: Option<StaticHermesWasmtimeSourceIdentity>,
    ) -> StaticHermesWasmtimeRouteDecision {
        if self.ambiguous {
            return StaticHermesWasmtimeRouteDecision::AmbiguousToV8;
        }
        let matching = self
            .entries
            .keys()
            .any(|selector| selector.matches(module_path, function_name));
        if !matching {
            return StaticHermesWasmtimeRouteDecision::AdmitWasm;
        }
        if source_identity.is_none() {
            return StaticHermesWasmtimeRouteDecision::AmbiguousToV8;
        }
        StaticHermesWasmtimeRouteDecision::QuarantineToV8
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticHermesWasmtimeQuarantineAction {
    Quarantine,
    Clear,
    Initialize,
}

impl fmt::Display for StaticHermesWasmtimeQuarantineAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Quarantine => "quarantine",
            Self::Clear => "clear",
            Self::Initialize => "initialize",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticHermesWasmtimeQuarantineUpdate {
    pub action: StaticHermesWasmtimeQuarantineAction,
    pub changed: bool,
    pub snapshot: Arc<StaticHermesWasmtimeQuarantineSnapshot>,
    pub existing_invocations_retain_captured_engine: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum StaticHermesWasmtimeQuarantineMutationError {
    #[error("invalid Wasm quarantine selector")]
    InvalidSelector(#[source] anyhow::Error),
    #[error("invalid Wasm quarantine update evidence")]
    InvalidEvidence(#[source] anyhow::Error),
    #[error("Wasm quarantine policy must be initialized first")]
    InitializationRequired,
    #[error("Wasm quarantine policy requires durable storage")]
    DurablePolicyRequired,
    #[error("Wasm quarantine policy is at capacity")]
    PolicyCapacityReached,
    #[error("Wasm quarantine persistence failed")]
    Persistence(#[source] anyhow::Error),
    #[error("Wasm quarantine policy is unavailable")]
    Internal(#[source] anyhow::Error),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedQuarantinePolicy {
    schema_version: u32,
    snapshot_version: u64,
    ambiguous: bool,
    entries: Vec<PersistedQuarantineEntry>,
    last_update: Option<PersistedQuarantineUpdateEvidence>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedQuarantineEntry {
    module_path: String,
    function_name: Option<String>,
    #[serde(rename = "last_seen_source_identity", default, skip_serializing)]
    _legacy_last_seen_source_identity: Option<StaticHermesWasmtimeSourceIdentity>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedQuarantineUpdateEvidence {
    action: String,
    selector_sha256: String,
    reason: String,
    operator_reference: String,
}

type ParentDirectorySync = fn(&Path) -> anyhow::Result<()>;

pub struct StaticHermesWasmtimeQuarantine {
    snapshot: RwLock<Arc<StaticHermesWasmtimeQuarantineSnapshot>>,
    policy_path: Option<PathBuf>,
    parent_directory_sync: ParentDirectorySync,
}

impl StaticHermesWasmtimeQuarantine {
    #[cfg(any(test, feature = "testing"))]
    fn in_memory_for_testing() -> Self {
        Self {
            snapshot: RwLock::new(Arc::new(StaticHermesWasmtimeQuarantineSnapshot::empty(0))),
            policy_path: None,
            parent_directory_sync: sync_parent_directory,
        }
    }

    fn fail_closed() -> Self {
        Self {
            snapshot: RwLock::new(Arc::new(StaticHermesWasmtimeQuarantineSnapshot::ambiguous(
                0,
            ))),
            policy_path: None,
            parent_directory_sync: sync_parent_directory,
        }
    }

    /// Load durable policy before runtime admission starts. A missing policy is
    /// deliberately ambiguous and therefore fails closed until `initialize`.
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::load_with_parent_directory_sync(path, sync_parent_directory)
    }

    fn load_with_parent_directory_sync(
        path: impl AsRef<Path>,
        parent_directory_sync: ParentDirectorySync,
    ) -> anyhow::Result<Self> {
        let path = canonical_policy_path(path.as_ref())?;
        let bytes = match read_policy_file(&path) {
            Ok(bytes) => bytes,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == ErrorKind::NotFound) =>
            {
                return Ok(Self {
                    snapshot: RwLock::new(Arc::new(
                        StaticHermesWasmtimeQuarantineSnapshot::ambiguous(0),
                    )),
                    policy_path: Some(path),
                    parent_directory_sync,
                });
            },
            Err(error) => return Err(error).context("failed to read Wasm quarantine policy"),
        };
        let persisted: PersistedQuarantinePolicy =
            serde_json::from_slice(&bytes).context("failed to decode Wasm quarantine policy")?;
        let snapshot = snapshot_from_persisted(persisted)?;
        Ok(Self {
            snapshot: RwLock::new(Arc::new(snapshot)),
            policy_path: Some(path),
            parent_directory_sync,
        })
    }

    /// SQLite policies live beside the database; URL-backed databases use the
    /// explicit persistent data root.
    pub fn policy_path_for_database_spec(
        db_spec: &str,
        data_dir: Option<&Path>,
    ) -> anyhow::Result<PathBuf> {
        if !db_spec.contains("://") {
            let base = fs::canonicalize(std::env::current_dir()?)?;
            let db_path = PathBuf::from(db_spec);
            let absolute_db = if db_path.is_absolute() {
                db_path
            } else {
                base.join(db_path)
            };
            let file_name = absolute_db
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.is_empty())
                .ok_or_else(|| anyhow::anyhow!("database specification has no file name"))?;
            let parent = absolute_db
                .parent()
                .ok_or_else(|| anyhow::anyhow!("database specification has no parent"))?;
            return Ok(fs::canonicalize(parent)?.join(format!(
                ".{file_name}.static-hermes-wasmtime-quarantine.json"
            )));
        }
        let data_dir = data_dir.ok_or_else(|| {
            anyhow::anyhow!("Wasm quarantine requires DATA_DIR for a URL-backed database")
        })?;
        Ok(fs::canonicalize(data_dir)?.join("static-hermes-wasmtime-quarantine.json"))
    }

    pub fn snapshot(&self) -> Arc<StaticHermesWasmtimeQuarantineSnapshot> {
        self.snapshot
            .read()
            .map(|snapshot| Arc::clone(&*snapshot))
            .unwrap_or_else(|_| Arc::new(StaticHermesWasmtimeQuarantineSnapshot::ambiguous(0)))
    }

    pub fn update(
        &self,
        action: StaticHermesWasmtimeQuarantineAction,
        selector: StaticHermesWasmtimeQuarantineSelector,
        reason: impl Into<String>,
        operator_reference: impl Into<String>,
    ) -> Result<StaticHermesWasmtimeQuarantineUpdate, StaticHermesWasmtimeQuarantineMutationError>
    {
        if matches!(action, StaticHermesWasmtimeQuarantineAction::Initialize) {
            return Err(StaticHermesWasmtimeQuarantineMutationError::Internal(
                anyhow::anyhow!("use initialize() to establish an empty quarantine policy"),
            ));
        }
        selector
            .validate()
            .map_err(StaticHermesWasmtimeQuarantineMutationError::InvalidSelector)?;
        let evidence = PersistedQuarantineUpdateEvidence {
            action: action.to_string(),
            selector_sha256: selector.digest(),
            reason: bounded_evidence(reason.into(), MAX_REASON_BYTES, "reason")
                .map_err(StaticHermesWasmtimeQuarantineMutationError::InvalidEvidence)?,
            operator_reference: bounded_evidence(
                operator_reference.into(),
                MAX_OPERATOR_REFERENCE_BYTES,
                "operator reference",
            )
            .map_err(StaticHermesWasmtimeQuarantineMutationError::InvalidEvidence)?,
        };
        let policy_path = self
            .policy_path
            .as_deref()
            .ok_or(StaticHermesWasmtimeQuarantineMutationError::DurablePolicyRequired)?;
        let mut current = self.snapshot.write().map_err(|_| {
            StaticHermesWasmtimeQuarantineMutationError::Internal(anyhow::anyhow!(
                "Wasm quarantine snapshot is unavailable"
            ))
        })?;
        if current.ambiguous {
            return Err(StaticHermesWasmtimeQuarantineMutationError::InitializationRequired);
        }
        let mut entries = current.entries.clone();
        let changed = match action {
            StaticHermesWasmtimeQuarantineAction::Quarantine => {
                entries.insert(selector, ()).is_none()
            },
            StaticHermesWasmtimeQuarantineAction::Clear => entries.remove(&selector).is_some(),
            StaticHermesWasmtimeQuarantineAction::Initialize => unreachable!(),
        };
        if entries.len() > MAX_ENTRIES {
            return Err(StaticHermesWasmtimeQuarantineMutationError::PolicyCapacityReached);
        }
        let next = if changed {
            Arc::new(StaticHermesWasmtimeQuarantineSnapshot {
                version: current.version.checked_add(1).ok_or_else(|| {
                    StaticHermesWasmtimeQuarantineMutationError::Internal(anyhow::anyhow!(
                        "Wasm quarantine snapshot version exhausted"
                    ))
                })?,
                ambiguous: false,
                entries,
            })
        } else {
            Arc::clone(&current)
        };
        if let Err(error) = self.persist_snapshot(policy_path, &next, Some(evidence)) {
            if error.replacement_happened {
                *current = if changed
                    && matches!(action, StaticHermesWasmtimeQuarantineAction::Quarantine)
                {
                    Arc::clone(&next)
                } else {
                    Arc::new(StaticHermesWasmtimeQuarantineSnapshot::ambiguous(
                        next.version,
                    ))
                };
            }
            return Err(StaticHermesWasmtimeQuarantineMutationError::Persistence(
                anyhow::Error::new(error),
            ));
        }
        if changed {
            *current = Arc::clone(&next);
        }
        Ok(StaticHermesWasmtimeQuarantineUpdate {
            action,
            changed,
            snapshot: next,
            existing_invocations_retain_captured_engine: true,
        })
    }

    pub fn initialize(
        &self,
        reason: impl Into<String>,
        operator_reference: impl Into<String>,
    ) -> Result<StaticHermesWasmtimeQuarantineUpdate, StaticHermesWasmtimeQuarantineMutationError>
    {
        let evidence = PersistedQuarantineUpdateEvidence {
            action: StaticHermesWasmtimeQuarantineAction::Initialize.to_string(),
            selector_sha256: digest_text("initialize"),
            reason: bounded_evidence(reason.into(), MAX_REASON_BYTES, "reason")
                .map_err(StaticHermesWasmtimeQuarantineMutationError::InvalidEvidence)?,
            operator_reference: bounded_evidence(
                operator_reference.into(),
                MAX_OPERATOR_REFERENCE_BYTES,
                "operator reference",
            )
            .map_err(StaticHermesWasmtimeQuarantineMutationError::InvalidEvidence)?,
        };
        let policy_path = self
            .policy_path
            .as_deref()
            .ok_or(StaticHermesWasmtimeQuarantineMutationError::DurablePolicyRequired)?;
        let mut current = self.snapshot.write().map_err(|_| {
            StaticHermesWasmtimeQuarantineMutationError::Internal(anyhow::anyhow!(
                "Wasm quarantine snapshot is unavailable"
            ))
        })?;
        if !current.ambiguous {
            if let Err(error) = self.persist_snapshot(policy_path, &current, Some(evidence)) {
                if error.replacement_happened {
                    let current_version = current.version;
                    *current = Arc::new(StaticHermesWasmtimeQuarantineSnapshot::ambiguous(
                        current_version,
                    ));
                }
                return Err(StaticHermesWasmtimeQuarantineMutationError::Persistence(
                    anyhow::Error::new(error),
                ));
            }
            return Ok(StaticHermesWasmtimeQuarantineUpdate {
                action: StaticHermesWasmtimeQuarantineAction::Initialize,
                changed: false,
                snapshot: Arc::clone(&current),
                existing_invocations_retain_captured_engine: true,
            });
        }
        let next = Arc::new(StaticHermesWasmtimeQuarantineSnapshot::empty(
            current.version.checked_add(1).ok_or_else(|| {
                StaticHermesWasmtimeQuarantineMutationError::Internal(anyhow::anyhow!(
                    "Wasm quarantine snapshot version exhausted"
                ))
            })?,
        ));
        if let Err(error) = self.persist_snapshot(policy_path, &next, Some(evidence)) {
            if error.replacement_happened {
                *current = Arc::new(StaticHermesWasmtimeQuarantineSnapshot::ambiguous(
                    next.version,
                ));
            }
            return Err(StaticHermesWasmtimeQuarantineMutationError::Persistence(
                anyhow::Error::new(error),
            ));
        }
        *current = Arc::clone(&next);
        Ok(StaticHermesWasmtimeQuarantineUpdate {
            action: StaticHermesWasmtimeQuarantineAction::Initialize,
            changed: true,
            snapshot: next,
            existing_invocations_retain_captured_engine: true,
        })
    }

    pub fn evaluate_generation(
        &self,
        module_path: &str,
        function_name: &str,
        generation_sha256: Option<&str>,
    ) -> StaticHermesWasmtimeRouteDecision {
        self.snapshot()
            .evaluate_generation(module_path, function_name, generation_sha256)
    }

    /// Evaluate a candidate route using one immutable snapshot and without
    /// publishing or persisting any invocation-path state.
    pub fn evaluate(
        &self,
        module_path: &str,
        function_name: &str,
        source_identity: Option<StaticHermesWasmtimeSourceIdentity>,
    ) -> StaticHermesWasmtimeRouteDecision {
        self.snapshot()
            .decide(module_path, function_name, source_identity)
    }

    pub fn decide(
        &self,
        module_path: &str,
        function_name: &str,
        source_identity: Option<StaticHermesWasmtimeSourceIdentity>,
    ) -> StaticHermesWasmtimeRouteDecision {
        self.evaluate(module_path, function_name, source_identity)
    }

    fn persist_snapshot(
        &self,
        path: &Path,
        snapshot: &StaticHermesWasmtimeQuarantineSnapshot,
        last_update: Option<PersistedQuarantineUpdateEvidence>,
    ) -> Result<(), PolicyPersistenceError> {
        let document = PersistedQuarantinePolicy {
            schema_version: POLICY_SCHEMA_VERSION,
            snapshot_version: snapshot.version,
            ambiguous: snapshot.ambiguous,
            entries: snapshot
                .entries
                .keys()
                .map(|selector| PersistedQuarantineEntry {
                    module_path: selector.module_path.clone(),
                    function_name: selector.function_name.clone(),
                    _legacy_last_seen_source_identity: None,
                })
                .collect(),
            last_update,
        };
        let bytes = serde_json::to_vec(&document)
            .map_err(|error| PolicyPersistenceError::before(anyhow::Error::new(error)))?;
        if bytes.len() > MAX_POLICY_BYTES {
            return Err(PolicyPersistenceError::before(anyhow::anyhow!(
                "Wasm quarantine policy is too large"
            )));
        }
        atomic_replace_policy(path, &bytes, self.parent_directory_sync)
    }
}

pub fn static_hermes_wasmtime_quarantine_path_for_database_spec(
    db_spec: &str,
    data_dir: Option<&Path>,
) -> anyhow::Result<PathBuf> {
    StaticHermesWasmtimeQuarantine::policy_path_for_database_spec(db_spec, data_dir)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn bounded_evidence(value: String, maximum: usize, label: &str) -> anyhow::Result<String> {
    let trimmed = value.trim();
    anyhow::ensure!(!trimmed.is_empty(), "Wasm quarantine {label} is required");
    anyhow::ensure!(
        trimmed.len() <= maximum,
        "Wasm quarantine {label} is too long"
    );
    Ok(trimmed.to_owned())
}

fn hex_digest(hasher: Sha256) -> String {
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn digest_text(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex_digest(hasher)
}

fn canonical_policy_path(path: &Path) -> anyhow::Result<PathBuf> {
    anyhow::ensure!(
        path.is_absolute(),
        "Wasm quarantine policy path must be absolute"
    );
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Wasm quarantine policy has no parent"))?;
    let canonical_parent = parent
        .canonicalize()
        .context("Wasm quarantine policy parent is unavailable")?;
    let canonical = canonical_parent.join(
        path.file_name()
            .ok_or_else(|| anyhow::anyhow!("Wasm quarantine policy has no file name"))?,
    );
    anyhow::ensure!(
        canonical == path,
        "Wasm quarantine policy path is not canonical"
    );
    if let Ok(metadata) = fs::symlink_metadata(path) {
        anyhow::ensure!(
            !metadata.file_type().is_symlink(),
            "Wasm quarantine policy must not be a symlink"
        );
    }
    Ok(canonical)
}

fn read_policy_file(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file(),
        "Wasm quarantine policy is not a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        anyhow::ensure!(
            metadata.mode() & 0o077 == 0,
            "Wasm quarantine policy permissions are too broad"
        );
        anyhow::ensure!(
            metadata.uid() == unsafe { libc::geteuid() },
            "Wasm quarantine policy owner is unexpected"
        );
    }
    let mut bytes = Vec::new();
    file.take((MAX_POLICY_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= MAX_POLICY_BYTES,
        "Wasm quarantine policy is too large"
    );
    Ok(bytes)
}

fn snapshot_from_persisted(
    persisted: PersistedQuarantinePolicy,
) -> anyhow::Result<StaticHermesWasmtimeQuarantineSnapshot> {
    anyhow::ensure!(
        persisted.schema_version == POLICY_SCHEMA_VERSION,
        "unsupported Wasm quarantine policy schema"
    );
    anyhow::ensure!(
        !persisted.ambiguous,
        "persisted Wasm quarantine policy cannot be ambiguous"
    );
    anyhow::ensure!(
        persisted.entries.len() <= MAX_ENTRIES,
        "too many Wasm quarantine entries"
    );
    if let Some(evidence) = &persisted.last_update {
        anyhow::ensure!(matches!(
            evidence.action.as_str(),
            "quarantine" | "clear" | "initialize"
        ));
        anyhow::ensure!(is_sha256(&evidence.selector_sha256));
        bounded_evidence(evidence.reason.clone(), MAX_REASON_BYTES, "reason")?;
        bounded_evidence(
            evidence.operator_reference.clone(),
            MAX_OPERATOR_REFERENCE_BYTES,
            "operator reference",
        )?;
    }
    let mut entries = BTreeMap::new();
    for entry in persisted.entries {
        let selector =
            StaticHermesWasmtimeQuarantineSelector::new(entry.module_path, entry.function_name)?;
        anyhow::ensure!(
            entries.insert(selector, ()).is_none(),
            "duplicate Wasm quarantine selector"
        );
    }
    Ok(StaticHermesWasmtimeQuarantineSnapshot {
        version: persisted.snapshot_version,
        ambiguous: false,
        entries,
    })
}

#[derive(Debug, thiserror::Error)]
#[error("failed to persist Wasm quarantine policy")]
struct PolicyPersistenceError {
    replacement_happened: bool,
    #[source]
    source: anyhow::Error,
}

impl PolicyPersistenceError {
    fn before(source: anyhow::Error) -> Self {
        Self {
            replacement_happened: false,
            source,
        }
    }

    fn after(source: anyhow::Error) -> Self {
        Self {
            replacement_happened: true,
            source,
        }
    }
}

fn sync_parent_directory(parent: &Path) -> anyhow::Result<()> {
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn atomic_replace_policy(
    path: &Path,
    bytes: &[u8],
    parent_directory_sync: ParentDirectorySync,
) -> Result<(), PolicyPersistenceError> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Wasm quarantine policy has no parent"))
        .map_err(PolicyPersistenceError::before)?;
    let prefix = format!(
        ".{}.tmp-",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("policy")
    );
    let mut temporary = tempfile::Builder::new()
        .prefix(&prefix)
        .tempfile_in(parent)
        .map_err(|error| PolicyPersistenceError::before(anyhow::Error::new(error)))?;
    temporary
        .write_all(bytes)
        .map_err(|error| PolicyPersistenceError::before(anyhow::Error::new(error)))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| PolicyPersistenceError::before(anyhow::Error::new(error)))?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .context("failed to publish Wasm quarantine policy")
        .map_err(PolicyPersistenceError::before)?;
    parent_directory_sync(parent)
        .context("failed to sync Wasm quarantine policy directory")
        .map_err(PolicyPersistenceError::after)?;
    Ok(())
}

static GLOBAL_QUARANTINE: OnceLock<StaticHermesWasmtimeQuarantine> = OnceLock::new();

pub fn initialize_static_hermes_wasmtime_quarantine(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let policy = StaticHermesWasmtimeQuarantine::load(path)?;
    GLOBAL_QUARANTINE
        .set(policy)
        .map_err(|_| anyhow::anyhow!("Wasm quarantine may only be initialized once at startup"))
}

pub fn static_hermes_wasmtime_quarantine() -> &'static StaticHermesWasmtimeQuarantine {
    GLOBAL_QUARANTINE.get_or_init(|| {
        #[cfg(any(test, feature = "testing"))]
        {
            StaticHermesWasmtimeQuarantine::in_memory_for_testing()
        }
        #[cfg(not(any(test, feature = "testing")))]
        {
            StaticHermesWasmtimeQuarantine::fail_closed()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initialized_policy() -> (tempfile::TempDir, StaticHermesWasmtimeQuarantine) {
        let directory = tempfile::tempdir().unwrap();
        let policy =
            StaticHermesWasmtimeQuarantine::load(directory.path().join("quarantine.json")).unwrap();
        policy.initialize("initialize", "test operator").unwrap();
        (directory, policy)
    }

    fn selector(function_name: Option<&str>) -> StaticHermesWasmtimeQuarantineSelector {
        StaticHermesWasmtimeQuarantineSelector::new(
            "functions/example.js",
            function_name.map(str::to_owned),
        )
        .unwrap()
    }

    #[test]
    fn selectors_are_exact_and_sticky_across_generations() {
        let (_directory, policy) = initialized_policy();
        policy
            .update(
                StaticHermesWasmtimeQuarantineAction::Quarantine,
                selector(Some("run")),
                "test quarantine",
                "test operator",
            )
            .unwrap();
        for generation in ["a", "b"] {
            assert_eq!(
                policy.evaluate_generation(
                    "functions/example.js",
                    "run",
                    Some(&generation.repeat(64)),
                ),
                StaticHermesWasmtimeRouteDecision::QuarantineToV8
            );
        }
        assert_eq!(
            policy.evaluate_generation("functions/example.js", "other", Some(&"a".repeat(64))),
            StaticHermesWasmtimeRouteDecision::AdmitWasm
        );
    }

    #[test]
    fn a_matching_selector_without_identity_fails_closed() {
        let (_directory, policy) = initialized_policy();
        policy
            .update(
                StaticHermesWasmtimeQuarantineAction::Quarantine,
                selector(None),
                "test quarantine",
                "test operator",
            )
            .unwrap();
        assert_eq!(
            policy.evaluate_generation("functions/example.js", "run", None),
            StaticHermesWasmtimeRouteDecision::AmbiguousToV8
        );
    }

    #[test]
    fn policy_updates_publish_new_snapshots_without_mutating_old_ones() {
        let (_directory, policy) = initialized_policy();
        let before = policy.snapshot();
        policy
            .update(
                StaticHermesWasmtimeQuarantineAction::Quarantine,
                selector(None),
                "test quarantine",
                "test operator",
            )
            .unwrap();
        let after = policy.snapshot();
        assert!(!Arc::ptr_eq(&before, &after));
        assert!(before.entries().is_empty());
        assert_eq!(after.entries().len(), 1);
    }

    #[test]
    fn missing_policy_requires_explicit_initialization_and_survives_reload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("quarantine.json");
        let policy = StaticHermesWasmtimeQuarantine::load(&path).unwrap();
        assert!(policy.snapshot().is_ambiguous());
        assert!(matches!(
            policy.update(
                StaticHermesWasmtimeQuarantineAction::Clear,
                selector(None),
                "test clear",
                "test operator",
            ),
            Err(StaticHermesWasmtimeQuarantineMutationError::InitializationRequired)
        ));
        policy.initialize("initialize", "test operator").unwrap();
        policy
            .update(
                StaticHermesWasmtimeQuarantineAction::Quarantine,
                selector(None),
                "test quarantine",
                "test operator",
            )
            .unwrap();
        let reloaded = StaticHermesWasmtimeQuarantine::load(&path).unwrap();
        assert_eq!(reloaded.snapshot().entries().len(), 1);
    }

    #[test]
    fn url_backed_database_paths_are_credential_free_and_require_data_dir() {
        let directory = tempfile::tempdir().unwrap();
        let path = StaticHermesWasmtimeQuarantine::policy_path_for_database_spec(
            "postgres://user:secret@example.invalid/backend",
            Some(directory.path()),
        )
        .unwrap();
        assert!(!path.to_string_lossy().contains("secret"));
        assert!(
            StaticHermesWasmtimeQuarantine::policy_path_for_database_spec(
                "postgres://user:secret@example.invalid/backend",
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn selector_validation_rejects_system_modules_and_glob_like_names() {
        assert!(StaticHermesWasmtimeQuarantineSelector::new("_system/test.js", None).is_err());
        assert!(StaticHermesWasmtimeQuarantineSelector::new(
            "functions/example.js",
            Some("run*".to_owned()),
        )
        .is_err());
    }

    #[test]
    fn maximum_extensionless_selector_is_stored_canonically_and_reloads() {
        let mut components = vec!["a".repeat(64); 7];
        components.push("b".repeat(54));
        let extensionless = components.join("/");
        assert_eq!(extensionless.len(), MAX_MODULE_PATH_BYTES - ".js".len());

        let selector = StaticHermesWasmtimeQuarantineSelector::new(extensionless, None).unwrap();
        assert_eq!(selector.module_path().len(), MAX_MODULE_PATH_BYTES);
        assert!(selector.module_path().ends_with(".js"));
        components[7].push('b');
        assert!(StaticHermesWasmtimeQuarantineSelector::new(components.join("/"), None).is_err());

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("quarantine.json");
        let policy = StaticHermesWasmtimeQuarantine::load(&path).unwrap();
        policy.initialize("initialize", "test operator").unwrap();
        policy
            .update(
                StaticHermesWasmtimeQuarantineAction::Quarantine,
                selector,
                "test quarantine",
                "test operator",
            )
            .unwrap();

        let reloaded = StaticHermesWasmtimeQuarantine::load(path).unwrap();
        assert_eq!(reloaded.snapshot().entries().len(), 1);
        assert_eq!(
            reloaded.snapshot().entries()[0]
                .selector
                .module_path()
                .len(),
            MAX_MODULE_PATH_BYTES
        );
    }

    #[test]
    fn update_without_a_durable_path_is_rejected() {
        let policy = StaticHermesWasmtimeQuarantine::in_memory_for_testing();
        assert!(matches!(
            policy.update(
                StaticHermesWasmtimeQuarantineAction::Quarantine,
                selector(None),
                "test quarantine",
                "test operator",
            ),
            Err(StaticHermesWasmtimeQuarantineMutationError::DurablePolicyRequired)
        ));
    }

    #[test]
    fn post_rename_directory_sync_failure_never_retains_an_admitting_snapshot() {
        fn fail_parent_directory_sync(_parent: &Path) -> anyhow::Result<()> {
            anyhow::bail!("injected parent directory sync failure")
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("quarantine.json");
        let initial = StaticHermesWasmtimeQuarantine::load(&path).unwrap();
        initial.initialize("initialize", "test operator").unwrap();
        drop(initial);

        let policy = StaticHermesWasmtimeQuarantine::load_with_parent_directory_sync(
            &path,
            fail_parent_directory_sync,
        )
        .unwrap();
        let selected = selector(None);
        assert!(matches!(
            policy.update(
                StaticHermesWasmtimeQuarantineAction::Quarantine,
                selected.clone(),
                "test quarantine",
                "test operator",
            ),
            Err(StaticHermesWasmtimeQuarantineMutationError::Persistence(_))
        ));
        let restrictive = policy.snapshot();
        assert!(!restrictive.is_ambiguous());
        assert_eq!(restrictive.entries().len(), 1);
        assert_eq!(
            StaticHermesWasmtimeQuarantine::load(&path)
                .unwrap()
                .snapshot()
                .entries()
                .len(),
            1
        );

        assert!(matches!(
            policy.update(
                StaticHermesWasmtimeQuarantineAction::Clear,
                selected,
                "test clear",
                "test operator",
            ),
            Err(StaticHermesWasmtimeQuarantineMutationError::Persistence(_))
        ));
        assert!(policy.snapshot().is_ambiguous());
        assert!(StaticHermesWasmtimeQuarantine::load(path)
            .unwrap()
            .snapshot()
            .entries()
            .is_empty());
    }
}
