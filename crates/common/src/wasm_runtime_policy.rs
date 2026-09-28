//! Invocation-scoped runtime selection. Durable updates never hold the read
//! lock during filesystem work; an invocation keeps the policy it already
//! captured.

use std::{
    fs::{
        self,
        File,
        OpenOptions,
    },
    io::Read,
    path::PathBuf,
    sync::{
        Arc,
        OnceLock,
    },
    time::Duration,
};

use anyhow::Context;
use parking_lot::{
    Mutex,
    RwLock,
};
use serde::{
    Deserialize,
    Serialize,
};
use utoipa::ToSchema;

use crate::{
    knobs,
    types::UdfType,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum PrimaryRuntime {
    V8,
    Wasm,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeMode {
    pub primary: PrimaryRuntime,
    #[schema(maximum = 10000)]
    pub shadow_bps: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimePolicy {
    pub query: RuntimeMode,
    pub mutation: RuntimeMode,
}

impl RuntimePolicy {
    pub fn for_udf(self, kind: UdfType) -> Option<RuntimeMode> {
        match kind {
            UdfType::Query => Some(self.query),
            UdfType::Mutation => Some(self.mutation),
            UdfType::Action | UdfType::HttpAction => None,
        }
    }

    pub fn validate(self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.query.shadow_bps <= 10_000 && self.mutation.shadow_bps <= 10_000,
            "runtime shadow sampling must not exceed 10000 basis points"
        );
        Ok(())
    }

    pub fn from_environment() -> anyhow::Result<Self> {
        let gate_enabled = match std::env::var("CONVEX_STATIC_HERMES_WASM_GATE_ENABLED") {
            Ok(value) if value == "1" => true,
            Ok(value) if value == "0" => false,
            Err(std::env::VarError::NotPresent) => false,
            _ => anyhow::bail!("CONVEX_STATIC_HERMES_WASM_GATE_ENABLED must be 0 or 1"),
        };
        let (query, mutation) = knobs::static_hermes_wasm_primary_directions(gate_enabled)?;
        knobs::validate_static_hermes_verifier_directions(
            *knobs::APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS,
            *knobs::APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS,
            *knobs::APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_V8_SHADOW_BPS,
            *knobs::APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_V8_SHADOW_BPS,
            query,
            mutation,
        )?;
        let mode = |primary, forward, inverse| RuntimeMode {
            primary: if primary {
                PrimaryRuntime::Wasm
            } else {
                PrimaryRuntime::V8
            },
            shadow_bps: if primary { inverse } else { forward },
        };
        let policy = Self {
            query: mode(
                query,
                *knobs::APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS,
                *knobs::APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_V8_SHADOW_BPS,
            ),
            mutation: mode(
                mutation,
                *knobs::APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS,
                *knobs::APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_V8_SHADOW_BPS,
            ),
        };
        policy.validate()?;
        Ok(policy)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimePolicySnapshot {
    pub revision: u64,
    pub policy: RuntimePolicy,
}

#[derive(Debug, Eq, PartialEq)]
pub enum RuntimePolicyUpdate {
    Applied(RuntimePolicySnapshot),
    Conflict,
}

// Only prepublication reads are cancellable. A submitted database or file
// write retains the publication gate until its actual outcome is known.
pub const RUNTIME_PUBLICATION_VALIDATION_TIMEOUT: Duration = Duration::from_secs(10);

pub struct RuntimePolicyStore {
    snapshot: RwLock<RuntimePolicySnapshot>,
    writer: Mutex<()>,
    // Source publication and policy publication serialize only their final
    // checks and commit. Preparation must never hold this gate.
    publication: Arc<tokio::sync::Mutex<()>>,
    path: Option<PathBuf>,
}

impl RuntimePolicyStore {
    pub fn in_memory(policy: RuntimePolicy) -> Self {
        policy.validate().expect("invalid runtime policy");
        Self {
            snapshot: RwLock::new(RuntimePolicySnapshot {
                revision: 0,
                policy,
            }),
            writer: Mutex::new(()),
            publication: Arc::new(tokio::sync::Mutex::new(())),
            path: None,
        }
    }

    pub fn load(
        path: PathBuf,
        default: impl FnOnce() -> anyhow::Result<RuntimePolicy>,
    ) -> anyhow::Result<Self> {
        // Stored policy replaces the environment defaults, including obsolete
        // direction/rate combinations. Read defaults only on first startup.
        let snapshot = match File::open(&path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(4097).read_to_end(&mut bytes)?;
                anyhow::ensure!(bytes.len() <= 4096, "runtime policy file is too large");
                let snapshot: RuntimePolicySnapshot = serde_json::from_slice(&bytes)?;
                snapshot.policy.validate()?;
                snapshot
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let policy = default()?;
                policy.validate()?;
                RuntimePolicySnapshot {
                    revision: 0,
                    policy,
                }
            },
            Err(error) => return Err(error).context("failed to read runtime policy"),
        };
        Ok(Self {
            snapshot: RwLock::new(snapshot),
            writer: Mutex::new(()),
            publication: Arc::new(tokio::sync::Mutex::new(())),
            path: Some(path),
        })
    }

    pub fn snapshot(&self) -> RuntimePolicySnapshot {
        *self.snapshot.read()
    }

    pub fn has_durable_storage(&self) -> bool {
        self.path.is_some()
    }

    pub async fn lock_publication(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.publication).lock_owned().await
    }

    pub fn update(
        &self,
        expected_revision: u64,
        policy: RuntimePolicy,
    ) -> anyhow::Result<RuntimePolicyUpdate> {
        policy.validate()?;
        let path = self
            .path
            .as_ref()
            .context("runtime policy has no durable storage path")?;
        let _writer = self.writer.lock();
        let previous = self.snapshot();
        if previous.revision != expected_revision {
            return Ok(RuntimePolicyUpdate::Conflict);
        }
        // Persist even an unchanged startup selection: it must survive a
        // restart with different environment defaults.
        let next = RuntimePolicySnapshot {
            revision: previous
                .revision
                .checked_add(1)
                .context("runtime policy revision exhausted")?,
            policy,
        };
        let directory = path.parent().context("runtime policy path has no parent")?;
        fs::create_dir_all(directory)?;
        let temporary = directory.join(format!(".runtime-policy-{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| -> anyhow::Result<()> {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let file = options.open(&temporary)?;
            serde_json::to_writer(&file, &next)?;
            file.sync_all()?;
            fs::rename(&temporary, path)?;
            // Rename is the visibility boundary. Publish even if the following
            // directory fsync fails so the live policy still matches the file.
            *self.snapshot.write() = next;
            File::open(directory)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            match fs::remove_file(&temporary) {
                Ok(()) => {},
                Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => {},
                Err(cleanup) => {
                    return Err(error).context(format!(
                        "failed to remove runtime policy temporary file: {cleanup}"
                    ));
                },
            }
            return Err(error);
        }
        Ok(RuntimePolicyUpdate::Applied(next))
    }
}

static POLICY: OnceLock<Arc<RuntimePolicyStore>> = OnceLock::new();

pub fn runtime_policy_store() -> &'static Arc<RuntimePolicyStore> {
    POLICY.get_or_init(|| {
        Arc::new(RuntimePolicyStore::in_memory(
            RuntimePolicy::from_environment().expect("invalid initial runtime policy"),
        ))
    })
}

pub fn initialize_runtime_policy(path: PathBuf) -> anyhow::Result<()> {
    let store = RuntimePolicyStore::load(path, RuntimePolicy::from_environment)?;
    POLICY
        .set(Arc::new(store))
        .map_err(|_| anyhow::anyhow!("runtime policy initialized after use"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_policy_overrides_startup_and_rejects_stale_writers() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("policy.json");
        let v8 = RuntimeMode {
            primary: PrimaryRuntime::V8,
            shadow_bps: 0,
        };
        let wasm = RuntimeMode {
            primary: PrimaryRuntime::Wasm,
            shadow_bps: 500,
        };
        let initial = RuntimePolicy {
            query: v8,
            mutation: v8,
        };
        let changed = RuntimePolicy {
            query: wasm,
            mutation: v8,
        };
        let store = RuntimePolicyStore::load(path.clone(), || Ok(initial))?;
        let captured = store.snapshot();
        let RuntimePolicyUpdate::Applied(published) = store.update(captured.revision, changed)?
        else {
            panic!("initial update conflicted");
        };
        assert_eq!(captured.policy, initial);
        assert_eq!(published.policy, changed);
        assert_eq!(
            store.update(captured.revision, initial)?,
            RuntimePolicyUpdate::Conflict
        );
        assert_eq!(store.snapshot(), published);
        assert_eq!(
            RuntimePolicyStore::load(path.clone(), || anyhow::bail!("defaults must not be read"))?
                .snapshot(),
            published
        );

        let RuntimePolicyUpdate::Applied(restored) = store.update(published.revision, initial)?
        else {
            panic!("current revision conflicted");
        };
        assert_eq!(
            RuntimePolicyStore::load(path, || Ok(changed))?.snapshot(),
            restored
        );
        Ok(())
    }

    #[test]
    fn invalid_or_failed_writes_do_not_publish() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("policy.json");
        let v8 = RuntimeMode {
            primary: PrimaryRuntime::V8,
            shadow_bps: 0,
        };
        let initial = RuntimePolicy {
            query: v8,
            mutation: v8,
        };
        let store = RuntimePolicyStore::load(path.clone(), || Ok(initial))?;
        let invalid = RuntimePolicy {
            query: RuntimeMode {
                shadow_bps: 10_001,
                ..v8
            },
            ..initial
        };
        assert!(store.update(0, invalid).is_err());
        // A directory at the destination forces the atomic replacement to fail.
        fs::create_dir(&path)?;
        let changed = RuntimePolicy {
            query: RuntimeMode {
                primary: PrimaryRuntime::Wasm,
                ..v8
            },
            ..initial
        };
        assert!(store.update(0, changed).is_err());
        assert_eq!(store.snapshot().policy, initial);
        assert_eq!(store.snapshot().revision, 0);
        assert_eq!(fs::read_dir(directory.path())?.count(), 1);
        Ok(())
    }
}
