//! Backend-owned native residents. The executable implements lifecycle protocol
//! v1; it is not an action runtime and receives no request or module-loading
//! API.
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::{
    fs::File,
    io::{
        Read,
        Seek,
    },
    path::{
        Path,
        PathBuf,
    },
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use common::{
    memory_pressure::MemoryPressureSignal,
    types::Timestamp,
};
use futures::{
    future::{
        BoxFuture,
        Shared,
    },
    FutureExt,
};
use model::source_packages::native::NativeResidentDescriptor;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    json,
    Value,
};
use tokio::{
    io::{
        AsyncBufReadExt,
        AsyncWriteExt,
        BufReader,
    },
    process::{
        Child,
        ChildStdin,
        ChildStdout,
        Command,
    },
    sync::{
        watch,
        Mutex,
    },
    time::{
        Instant,
        MissedTickBehavior,
    },
};
use tokio_util::task::TaskTracker;
use value::sha256::Sha256;

use crate::{
    local::{
        SurgeCoordinator,
        SurgePermit,
        SurgePriority,
    },
    process::kill_and_reap,
};

const MAX_CONTROL_BYTES: usize = 64 * 1024;
const MAX_ARTIFACT_BYTES: u64 = 512 * 1024 * 1024;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const RETIRE_TIMEOUT: Duration = Duration::from_secs(180);
const REAP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct Configuration {
    artifacts: PathBuf,
    configurations: PathBuf,
    max_rss_bytes: usize,
}

/// Native RSS contributes to the same steady-state and replacement budget as
/// Node. An unset limit disables native residents; enabling them requires
/// explicit capacity.
pub(crate) fn configured_rss_bytes() -> anyhow::Result<usize> {
    match std::env::var("LOCAL_NATIVE_RESIDENT_MAX_RSS_BYTES") {
        Ok(value) => {
            let bytes: usize = value.parse().context("Invalid native resident RSS limit")?;
            anyhow::ensure!(
                bytes >= 16 * 1024 * 1024,
                "Native resident RSS limit must be at least 16 MiB"
            );
            Ok(bytes)
        },
        Err(std::env::VarError::NotPresent) => Ok(0),
        Err(error) => Err(error.into()),
    }
}

impl Configuration {
    fn from_env() -> anyhow::Result<Option<Self>> {
        let max_rss_bytes = configured_rss_bytes()?;
        if max_rss_bytes == 0 {
            anyhow::ensure!(
                std::env::var_os("LOCAL_NATIVE_RESIDENT_ARTIFACTS_DIR").is_none()
                    && std::env::var_os("LOCAL_NATIVE_RESIDENT_CONFIGURATIONS_DIR").is_none(),
                "Native resident directories require an explicit RSS limit"
            );
            return Ok(None);
        }
        anyhow::ensure!(
            cfg!(target_os = "linux"),
            "Native residents require Linux parent-death supervision"
        );
        Ok(Some(Self {
            artifacts: std::env::var_os("LOCAL_NATIVE_RESIDENT_ARTIFACTS_DIR")
                .context("Native resident artifact directory is required")?
                .into(),
            configurations: std::env::var_os("LOCAL_NATIVE_RESIDENT_CONFIGURATIONS_DIR")
                .context("Native resident configuration directory is required")?
                .into(),
            max_rss_bytes,
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Selection {
    version: Timestamp,
    descriptor: Option<NativeResidentDescriptor>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeResidentStatus {
    pub selected: Option<NativeResidentDescriptor>,
    pub active_generation: Option<String>,
    pub active_descriptor: Option<NativeResidentDescriptor>,
    pub phase: &'static str,
}

/// A single task owns transitions, children, and cleanup. Publishing a newer
/// selection never detaches an earlier candidate or releases its surge permit.
pub struct NativeResidentSupervisor {
    config: Option<Configuration>,
    surge: Arc<SurgeCoordinator>,
    pressure: MemoryPressureSignal,
    selection: watch::Sender<Selection>,
    shutdown: watch::Sender<bool>,
    force: watch::Sender<Option<String>>,
    status: watch::Receiver<NativeResidentStatus>,
    task: Shared<BoxFuture<'static, Result<(), Arc<anyhow::Error>>>>,
    staging: Mutex<()>,
    publication: Mutex<()>,
    cleanup: TaskTracker,
}

impl NativeResidentSupervisor {
    pub(crate) fn new(
        surge: Arc<SurgeCoordinator>,
        pressure: MemoryPressureSignal,
    ) -> anyhow::Result<Arc<Self>> {
        Self::with_configuration(Configuration::from_env()?, surge, pressure)
    }

    fn with_configuration(
        config: Option<Configuration>,
        surge: Arc<SurgeCoordinator>,
        pressure: MemoryPressureSignal,
    ) -> anyhow::Result<Arc<Self>> {
        let (selection, selection_rx) = watch::channel(Selection {
            version: Timestamp::MIN,
            descriptor: None,
        });
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (force, force_rx) = watch::channel(None);
        let (status_tx, status) = watch::channel(NativeResidentStatus {
            selected: None,
            active_generation: None,
            active_descriptor: None,
            phase: "inactive",
        });
        let cleanup = TaskTracker::new();
        let task = tokio::spawn(run_supervisor(
            config.clone(),
            surge.clone(),
            pressure.clone(),
            selection_rx,
            shutdown_rx,
            force_rx,
            status_tx,
            cleanup.clone(),
        ));
        Ok(Arc::new(Self {
            config,
            surge,
            pressure,
            selection,
            shutdown,
            force,
            status,
            task: async move {
                task.await
                    .context("Native resident supervisor task failed")
                    .and_then(|result| result)
                    .map_err(Arc::new)
            }
            .boxed()
            .shared(),
            staging: Mutex::new(()),
            publication: Mutex::new(()),
            cleanup,
        }))
    }

    pub fn status(&self) -> NativeResidentStatus {
        self.status.borrow().clone()
    }

    /// Serialize the live-process compatibility check with publication. The
    /// caller refreshes committed selection under this guard before checking.
    pub async fn publication_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.publication.lock().await
    }

    pub fn validate_publication_contract(&self, contract: Option<&str>) -> anyhow::Result<()> {
        let status = self.status.borrow();
        let selection = self.selection.borrow();
        // A cold candidate can still become active before reconciliation observes
        // an intervening retirement commit. Its contract remains live until the
        // supervisor has applied that selection and reaped the candidate.
        for resident in [
            status.active_descriptor.as_ref(),
            status.selected.as_ref(),
            selection.descriptor.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            anyhow::ensure!(
                contract == Some(resident.application_contract.as_str()),
                "Wait for native resident retirement before publishing an incompatible \
                 application contract"
            );
        }
        Ok(())
    }

    pub fn reconcile(
        &self,
        descriptor: Option<NativeResidentDescriptor>,
        version: Timestamp,
    ) -> anyhow::Result<()> {
        if let Some(descriptor) = &descriptor {
            descriptor.validate()?;
            anyhow::ensure!(
                self.config.is_some(),
                "Native residents are not configured on this backend"
            );
        }
        anyhow::ensure!(
            !*self.shutdown.borrow(),
            "Native resident supervisor is shutting down"
        );
        let mut failure = false;
        self.selection.send_if_modified(|current| {
            if version < current.version {
                return false;
            }
            if version == current.version {
                failure = current.descriptor != descriptor;
                return false;
            }
            let changed = current.descriptor != descriptor;
            *current = Selection {
                version,
                descriptor,
            };
            changed
        });
        anyhow::ensure!(
            !failure,
            "Native resident selection differs at the same commit version"
        );
        anyhow::ensure!(
            !self.selection.is_closed(),
            "Native resident supervision stopped"
        );
        Ok(())
    }

    /// Readiness is inert and bounded. Reap the trial before Node cutover
    /// reserves surge capacity; holding both reservations would deadlock a
    /// mixed deployment.
    pub async fn prepare(
        &self,
        descriptor: &NativeResidentDescriptor,
        force: bool,
    ) -> anyhow::Result<()> {
        let _operation = self.cleanup.token();
        descriptor.validate()?;
        let config = self
            .config
            .as_ref()
            .context("Native residents are not configured on this backend")?;
        let _stage = self.staging.lock().await;
        // Subscribe before checking admission. A signal published between the
        // check and the capacity wait must remain an unobserved interruption.
        let mut shutdown = self.shutdown.subscribe();
        let mut memory = self.pressure.subscribe();
        anyhow::ensure!(
            !*self.shutdown.borrow() && !self.pressure.is_active(),
            "Native resident staging is unavailable"
        );
        if self.status.borrow().selected.as_ref() == Some(descriptor)
            && self.status.borrow().active_generation.is_some()
            && self.status.borrow().phase == "active"
        {
            return Ok(());
        }
        let permit = tokio::select! {
            permit = tokio::time::timeout(
                SurgeCoordinator::DEPLOYMENT_ADMISSION_TIMEOUT,
                self.surge.acquire_deployment(force),
            ) => permit.context("Native resident staging capacity is unavailable")?,
            _ = shutdown.changed() => anyhow::bail!("Native resident staging interrupted by shutdown"),
            _ = memory.changed() => anyhow::bail!("Native resident staging interrupted by memory pressure"),
        };
        anyhow::ensure!(
            !*self.shutdown.borrow() && !self.pressure.is_active(),
            "Native resident staging is unavailable"
        );
        let generation = new_generation();
        tokio::select! {
            result = async {
                let mut child = start(
                    config, descriptor, generation, Some(permit), self.cleanup.clone(),
                ).await?;
                child.terminate().await
            } => result,
            _ = shutdown.changed() => anyhow::bail!("Native resident staging interrupted by shutdown"),
            _ = memory.changed() => anyhow::bail!("Native resident staging interrupted by memory pressure"),
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.is_some()
    }

    pub fn force_retire(&self, generation: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.status.borrow().active_generation.as_deref() == Some(generation),
            "Native resident force request does not match the active generation"
        );
        self.force.send_replace(Some(generation.to_owned()));
        Ok(())
    }

    pub fn is_shutting_down(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub fn request_shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.request_shutdown();
        // Administrative readiness trials own real children too. Join their
        // bounded cleanup before the backend releases its application runtime.
        // Admission is closed. Let queued preparations acquire the staging
        // lock and observe shutdown so their tracked operation tokens can drain.
        drop(self.staging.lock().await);
        // Every caller observes the same join and terminal result, including
        // cancellation after the join but before cleanup finishes.
        let supervision = self.task.clone().await;
        // No child producer remains after staging and supervisor completion.
        // Canceled futures register their cleanup synchronously from Drop.
        self.cleanup.close();
        tokio::time::timeout(REAP_TIMEOUT, self.cleanup.wait())
            .await
            .context("Native resident cleanup is still awaiting confirmed reaping")?;
        supervision.map_err(|error| anyhow::anyhow!("{error:#}"))
    }
}

#[cfg(feature = "testing")]
pub fn test_supervisor(
    artifacts: PathBuf,
    configurations: PathBuf,
) -> anyhow::Result<Arc<NativeResidentSupervisor>> {
    test_supervisor_with_limits(
        artifacts,
        configurations,
        64 * 1024 * 1024,
        MemoryPressureSignal::default(),
    )
}

#[cfg(feature = "testing")]
pub fn test_supervisor_with_limits(
    artifacts: PathBuf,
    configurations: PathBuf,
    max_rss_bytes: usize,
    pressure: MemoryPressureSignal,
) -> anyhow::Result<Arc<NativeResidentSupervisor>> {
    NativeResidentSupervisor::with_configuration(
        Some(Configuration {
            artifacts,
            configurations,
            max_rss_bytes,
        }),
        SurgeCoordinator::new(),
        pressure,
    )
}

fn new_generation() -> String {
    // Administrative requests can survive a backend restart and PID reuse.
    // A process-local sequence would let such a request name a new child.
    format!("{:032x}", rand::random::<u128>())
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
enum Reply {
    #[serde(rename_all = "camelCase")]
    Ready {
        generation: String,
        artifact_sha256: String,
        configuration_sha256: String,
        lifecycle_protocol: u32,
        application_contract: String,
    },
    Progress {
        generation: String,
        nonce: u64,
    },
    Drained {
        generation: String,
    },
    Failed {
        generation: String,
        code: String,
    },
}

struct ResidentChild {
    child: Option<Child>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    generation: String,
    descriptor: NativeResidentDescriptor,
    permit: Option<SurgePermit>,
    probe: u64,
    pending_probe: Option<Instant>,
    pending_write: Option<ControlWrite>,
    pending: Vec<u8>,
    cleanup: TaskTracker,
    logs: tokio::task::JoinHandle<()>,
}

struct ControlWrite {
    bytes: Vec<u8>,
    written: usize,
    deadline: Instant,
}

impl ResidentChild {
    async fn send(&mut self, message: Value, max_rss_bytes: usize) -> anyhow::Result<()> {
        let mut bytes = serde_json::to_vec(&message)?;
        anyhow::ensure!(
            bytes.len() < MAX_CONTROL_BYTES,
            "Native control request exceeds its bound"
        );
        bytes.push(b'\n');
        let pending = self.pending_write.get_or_insert_with(|| ControlWrite {
            bytes: bytes.clone(),
            written: 0,
            deadline: Instant::now() + PROBE_TIMEOUT,
        });
        anyhow::ensure!(
            pending.bytes == bytes,
            "Native control write changed during cancellation"
        );
        let deadline = pending.deadline;
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            let pending = self
                .pending_write
                .as_mut()
                .expect("Native control write missing");
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => anyhow::bail!("Native control write timed out"),
                _ = interval.tick() => self.check_rss(max_rss_bytes).await?,
                count = self.stdin.write(&pending.bytes[pending.written..]) => {
                    let count = count?;
                    anyhow::ensure!(count > 0, "Native control write closed");
                    pending.written += count;
                    if pending.written == pending.bytes.len() {
                        self.pending_write = None;
                        return Ok(());
                    }
                },
            }
        }
    }

    async fn read(&mut self) -> anyhow::Result<Reply> {
        // read_line may allocate until EOF. Read bounded chunks before parsing.
        loop {
            let available = self.stdout.fill_buf().await?;
            anyhow::ensure!(!available.is_empty(), "Native control channel closed");
            let length = available
                .iter()
                .position(|b| *b == b'\n')
                .map_or(available.len(), |end| end + 1);
            anyhow::ensure!(
                self.pending.len() + length <= MAX_CONTROL_BYTES,
                "Native control reply exceeds its bound"
            );
            let complete = available[length - 1] == b'\n';
            self.pending.extend_from_slice(&available[..length]);
            self.stdout.consume(length);
            if complete {
                break;
            }
        }
        let reply: Reply = serde_json::from_slice(&self.pending)
            .map_err(|_| anyhow::anyhow!("Invalid native control reply"))?;
        self.pending.clear();
        let generation = match &reply {
            Reply::Ready { generation, .. }
            | Reply::Progress { generation, .. }
            | Reply::Drained { generation }
            | Reply::Failed { generation, .. } => generation,
        };
        anyhow::ensure!(
            generation == &self.generation,
            "Native control reply generation mismatch"
        );
        if let Reply::Failed { code, .. } = &reply {
            // Child-controlled details do not become backend log fields.
            anyhow::ensure!(code.len() <= 128, "Invalid native failure code");
            anyhow::bail!("Native resident reported failure");
        }
        Ok(reply)
    }

    async fn read_healthy_until(
        &mut self,
        deadline: Instant,
        max_rss_bytes: usize,
    ) -> anyhow::Result<Reply> {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => anyhow::bail!("Native resident control reply timed out"),
                _ = interval.tick() => self.check_rss(max_rss_bytes).await?,
                reply = self.read() => return reply,
            }
        }
    }

    async fn ready(
        &mut self,
        expected: &NativeResidentDescriptor,
        max_rss_bytes: usize,
        deadline: Instant,
    ) -> anyhow::Result<()> {
        let reply = self.read_healthy_until(deadline, max_rss_bytes).await?;
        match reply {
            Reply::Ready {
                artifact_sha256,
                configuration_sha256,
                lifecycle_protocol,
                application_contract,
                ..
            } => {
                anyhow::ensure!(
                    NativeResidentDescriptor {
                        artifact_sha256,
                        configuration_sha256,
                        lifecycle_protocol,
                        application_contract
                    } == *expected,
                    "Native resident readiness identity mismatch"
                );
                Ok(())
            },
            Reply::Progress { .. } | Reply::Drained { .. } | Reply::Failed { .. } => {
                anyhow::bail!("Unexpected native readiness reply")
            },
        }
    }

    async fn activate(&mut self, max_rss_bytes: usize) -> anyhow::Result<()> {
        self.send(
            json!({"type": "activate", "generation": self.generation}),
            max_rss_bytes,
        )
        .await
    }

    async fn probe(&mut self, max_rss_bytes: usize) -> anyhow::Result<()> {
        // Predecessor drain can finish while the inert candidate awaits progress.
        // Promotion resumes that exact probe instead of consuming its reply as
        // stale evidence for a newer nonce, or extending the health deadline.
        let must_send = self.pending_probe.is_none() || self.pending_write.is_some();
        let deadline = if let Some(deadline) = self.pending_probe {
            deadline
        } else {
            self.probe = self
                .probe
                .checked_add(1)
                .context("Native probe sequence overflow")?;
            let deadline = Instant::now() + PROBE_TIMEOUT;
            self.pending_probe = Some(deadline);
            deadline
        };
        if must_send {
            // Record the nonce and deadline before writing: a canceled partial
            // write resumes its bytes and cannot renew the progress budget.
            self.send(
                json!({"type": "probe", "generation": self.generation, "nonce": self.probe}),
                max_rss_bytes,
            )
            .await?;
        }
        match self.read_healthy_until(deadline, max_rss_bytes).await? {
            Reply::Progress { nonce, .. } => {
                anyhow::ensure!(nonce == self.probe, "Native progress nonce mismatch")
            },
            Reply::Ready { .. } | Reply::Drained { .. } | Reply::Failed { .. } => {
                anyhow::bail!("Unexpected native progress reply")
            },
        }
        self.pending_probe = None;
        self.check_rss(max_rss_bytes).await
    }

    #[cfg(target_os = "linux")]
    async fn check_rss(&mut self, max_rss_bytes: usize) -> anyhow::Result<()> {
        let child = self.child.as_mut().context("Native child already reaped")?;
        anyhow::ensure!(child.try_wait()?.is_none(), "Native child exited");
        let pid = child.id().context("Native child PID is unavailable")?;
        let statm = tokio::fs::read_to_string(format!("/proc/{pid}/statm")).await?;
        let pages: usize = statm
            .split_whitespace()
            .nth(1)
            .context("Invalid native process RSS")?
            .parse()?;
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        anyhow::ensure!(page_size > 0, "Invalid host page size");
        let rss = pages
            .checked_mul(usize::try_from(page_size)?)
            .context("Native RSS overflow")?;
        anyhow::ensure!(
            rss <= max_rss_bytes,
            "Native resident exceeded its RSS limit"
        );
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    async fn check_rss(&mut self, _max_rss_bytes: usize) -> anyhow::Result<()> {
        anyhow::bail!("Native resident process sampling requires Linux")
    }

    async fn retire(
        &mut self,
        max_rss_bytes: usize,
        pressure: &MemoryPressureSignal,
        shutdown: &mut watch::Receiver<bool>,
        force: &mut watch::Receiver<Option<String>>,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + RETIRE_TIMEOUT;
        let generation = self.generation.clone();
        let permit = self.permit.clone();
        let mut memory = pressure.subscribe();
        // These interrupts cover writes as well as reads. A resident that stops
        // consuming stdin must not postpone shutdown or pressure termination.
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => anyhow::bail!("Native resident retirement timed out"),
            _ = shutdown.wait_for(|shutdown| *shutdown) => anyhow::bail!("Native resident shutdown"),
            _ = memory.wait_for(|pressure| *pressure) => anyhow::bail!("Native resident memory pressure"),
            _ = force.wait_for(|forced| forced.as_deref() == Some(generation.as_str())) => anyhow::bail!("Native drain was explicitly forced"),
            _ = async {
                match permit {
                    Some(permit) => permit.wait_until_preempted().await,
                    None => std::future::pending().await,
                }
            } => anyhow::bail!("Native drain was forcibly preempted"),
            result = self.drain(max_rss_bytes) => result,
        }
    }

    async fn drain(&mut self, max_rss_bytes: usize) -> anyhow::Result<()> {
        // Candidate readiness may cancel the incumbent's monitor while a probe
        // is pending. Finish that exact exchange before starting retirement.
        if self.pending_probe.is_some() {
            self.probe(max_rss_bytes).await?;
        }
        self.send(json!({"type": "retire", "generation": self.generation, "deadlineMs": RETIRE_TIMEOUT.as_millis() as u64}), max_rss_bytes).await?;
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut next_probe = Instant::now();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.check_rss(max_rss_bytes).await?;
                    if let Some(deadline) = self.pending_probe {
                        anyhow::ensure!(Instant::now() < deadline, "Native retiring coordinator stopped progressing");
                    } else if Instant::now() >= next_probe {
                        self.probe = self.probe.checked_add(1).context("Native probe sequence overflow")?;
                        self.pending_probe = Some(Instant::now() + PROBE_TIMEOUT);
                        self.send(json!({"type": "probe", "generation": self.generation, "nonce": self.probe}), max_rss_bytes).await?;
                        next_probe = Instant::now() + Duration::from_secs(1);
                    }
                },
                reply = self.read() => match reply? {
                    Reply::Drained { .. } => return Ok(()),
                    Reply::Progress { nonce, .. } => {
                        anyhow::ensure!(self.pending_probe.is_some() && self.probe == nonce, "Native retirement progress nonce mismatch");
                        self.pending_probe = None;
                    },
                    Reply::Ready { .. } | Reply::Failed { .. } => anyhow::bail!("Unexpected native retirement reply"),
                },
            }
        }
    }

    async fn terminate(&mut self) -> anyhow::Result<()> {
        self.logs.abort();
        if let Some(child) = &mut self.child {
            tokio::time::timeout(REAP_TIMEOUT, kill_and_reap(child))
                .await
                .context("Native child reaping timed out")??;
            self.child.take();
            if let Some(permit) = self.permit.take() {
                permit.confirm_direct_child_reaped();
            }
        }
        Ok(())
    }
}

impl Drop for ResidentChild {
    fn drop(&mut self) {
        self.logs.abort();
        let Some(mut child) = self.child.take() else {
            return;
        };
        let permit = self.permit.take();
        // Caller cancellation never reports cleanup or returns shared surge before
        // confirmed reap. kill_on_drop remains a last resort during runtime teardown.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            self.cleanup.spawn_on(
                async move {
                    match kill_and_reap(&mut child).await {
                        Ok(_) => {
                            if let Some(permit) = permit {
                                permit.confirm_direct_child_reaped();
                            }
                        },
                        Err(_) => {
                            tracing::error!("Failed to reap a native resident during cleanup");
                            // Keep the child and its accounting in the tracked
                            // owner. Shutdown reports this unfinished cleanup.
                            std::future::pending::<()>().await;
                            drop((child, permit));
                        },
                    }
                },
                &runtime,
            );
        }
    }
}

fn forward_logs(
    stderr: tokio::process::ChildStderr,
    generation: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        crate::native_logs::forward(stderr, |record| {
            crate::native_logs::emit(&generation, record)
        })
        .await;
    })
}

fn verified_file(path: &Path, expected: &str, limit: u64) -> anyhow::Result<File> {
    let mut file = File::open(path).context("Native resident input is unavailable")?;
    let metadata = file.metadata()?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() <= limit,
        "Invalid native resident input size"
    );
    let mut hash = Sha256::new();
    std::io::copy(&mut (&mut file).take(limit + 1), &mut hash)?;
    anyhow::ensure!(
        hash.finalize().as_hex() == expected,
        "Native resident input checksum mismatch"
    );
    file.rewind()?;
    Ok(file)
}

#[cfg(target_os = "linux")]
async fn start(
    config: &Configuration,
    descriptor: &NativeResidentDescriptor,
    generation: String,
    permit: Option<SurgePermit>,
    cleanup: TaskTracker,
) -> anyhow::Result<ResidentChild> {
    let artifact_path = config
        .artifacts
        .join(&descriptor.artifact_sha256)
        .join("resident");
    let configuration_path = config
        .configurations
        .join(format!("{}.json", descriptor.configuration_sha256));
    let identity = descriptor.clone();
    // Blocking file verification cannot be canceled once running. Shutdown
    // still owns that work even when its awaiting preparation was canceled.
    let verification = cleanup.token();
    let (artifact, configuration) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let _verification = verification;
        let artifact = verified_file(
            &artifact_path,
            &identity.artifact_sha256,
            MAX_ARTIFACT_BYTES,
        )?;
        let mut configuration = verified_file(
            &configuration_path,
            &identity.configuration_sha256,
            (MAX_CONTROL_BYTES / 2) as u64,
        )?;
        let mut bytes = Vec::new();
        configuration.read_to_end(&mut bytes)?;
        let configuration: Value =
            serde_json::from_slice(&bytes).context("Invalid native resident configuration")?;
        anyhow::ensure!(
            configuration.is_object(),
            "Native resident configuration must be an object"
        );
        Ok((artifact, configuration))
    })
    .await??;
    // Execute the verified inode, avoiding a path replacement between hashing and
    // exec.
    let mut command = Command::new(format!("/proc/self/fd/{}", artifact.as_raw_fd()));
    command
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(target_os = "linux")]
    unsafe {
        let parent = libc::getpid();
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() != parent {
                return Err(std::io::Error::other(
                    "Native resident parent exited during spawn",
                ));
            }
            Ok(())
        });
    }
    let readiness_deadline = Instant::now() + STARTUP_TIMEOUT;
    let mut child = command.spawn().context("Failed to spawn native resident")?;
    if let Some(permit) = &permit {
        permit.require_confirmed_cleanup();
    }
    let stdin = child
        .stdin
        .take()
        .context("Native resident stdin missing")?;
    let stdout = BufReader::new(
        child
            .stdout
            .take()
            .context("Native resident stdout missing")?,
    );
    let logs = forward_logs(
        child
            .stderr
            .take()
            .context("Native resident stderr missing")?,
        generation.clone(),
    );
    let mut result = ResidentChild {
        logs,
        child: Some(child),
        stdin,
        stdout,
        generation,
        descriptor: descriptor.clone(),
        permit,
        probe: 0,
        pending_probe: None,
        pending_write: None,
        pending: Vec::new(),
        cleanup,
    };
    let readiness = async {
        result.send(json!({"type": "initialize", "generation": result.generation, "descriptor": descriptor, "configuration": configuration}), config.max_rss_bytes).await?;
        result.ready(descriptor, config.max_rss_bytes, readiness_deadline).await
    }.await;
    if let Err(error) = readiness {
        result.terminate().await?;
        return Err(error);
    }
    Ok(result)
}

#[cfg(not(target_os = "linux"))]
async fn start(
    _config: &Configuration,
    _descriptor: &NativeResidentDescriptor,
    _generation: String,
    _permit: Option<SurgePermit>,
    _cleanup: TaskTracker,
) -> anyhow::Result<ResidentChild> {
    anyhow::bail!("Native resident execution requires Linux")
}

async fn monitor_child(candidate: &mut ResidentChild, max_rss_bytes: usize) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        candidate.probe(max_rss_bytes).await?;
    }
}

async fn run_supervisor(
    config: Option<Configuration>,
    surge: Arc<SurgeCoordinator>,
    pressure: MemoryPressureSignal,
    mut selection: watch::Receiver<Selection>,
    mut shutdown: watch::Receiver<bool>,
    mut force: watch::Receiver<Option<String>>,
    status: watch::Sender<NativeResidentStatus>,
    cleanup: TaskTracker,
) -> anyhow::Result<()> {
    let mut active: Option<ResidentChild> = None;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut memory = pressure.subscribe();
    'supervisor: loop {
        tokio::select! {
            _ = memory.changed() => (),
            _ = shutdown.changed() => break,
            result = selection.changed() => if result.is_err() { break; },
            _ = force.changed() => (),
            _ = interval.tick() => (),
        }
        if *shutdown.borrow() {
            break;
        }
        let desired = selection.borrow().clone();
        let Some(config) = &config else {
            anyhow::ensure!(
                desired.descriptor.is_none(),
                "Native resident selected without configuration"
            );
            continue;
        };
        if pressure.is_active() {
            if let Some(mut old) = active.take() {
                old.terminate().await?;
            }
            status.send_replace(NativeResidentStatus {
                selected: desired.descriptor,
                active_generation: None,
                active_descriptor: None,
                phase: "pressure",
            });
            continue;
        }
        if let Some(child) = &mut active {
            let generation = child.generation.clone();
            let healthy = tokio::select! {
                result = child.probe(config.max_rss_bytes) => result.is_ok(),
                _ = shutdown.changed() => break,
                _ = memory.changed() => continue,
                _ = force.wait_for(|forced| {
                    forced.as_deref() == Some(generation.as_str())
                }) => false,
            };
            if force.borrow().as_deref() == Some(child.generation.as_str()) || !healthy {
                tracing::warn!("Native resident failed its coordinator progress or RSS check");
                child.terminate().await?;
                active = None;
            }
        }
        if active.as_ref().map(|child| &child.descriptor) == desired.descriptor.as_ref() {
            status.send_replace(NativeResidentStatus {
                selected: desired.descriptor,
                active_generation: active.as_ref().map(|child| child.generation.clone()),
                active_descriptor: active.as_ref().map(|child| child.descriptor.clone()),
                phase: if active.is_some() {
                    "active"
                } else {
                    "inactive"
                },
            });
            continue;
        }
        status.send_replace(NativeResidentStatus {
            selected: desired.descriptor.clone(),
            active_generation: active.as_ref().map(|child| child.generation.clone()),
            active_descriptor: active.as_ref().map(|child| child.descriptor.clone()),
            phase: "candidate",
        });
        let mut candidate = if let Some(descriptor) = &desired.descriptor {
            let acquisition = surge.acquire(SurgePriority::Deployment, Arc::from("native"));
            tokio::pin!(acquisition);
            // Retain the receiver checked above: resubscribing here would mark
            // pressure that arrived during the incumbent probe as already seen.
            let permit = loop {
                // Waiting behind another adapter's drain must not suspend
                // supervision of the incumbent native steady slot.
                tokio::select! {
                    permit = &mut acquisition => break permit,
                    _ = shutdown.changed() => break 'supervisor,
                    result = selection.changed() => {
                        if result.is_err() {
                            break 'supervisor;
                        }
                        if selection.borrow().descriptor != desired.descriptor {
                            continue 'supervisor;
                        }
                    },
                    _ = memory.changed() => continue 'supervisor,
                    _ = force.changed() => {
                        if let Some(child) = &mut active
                            && force.borrow().as_deref() == Some(child.generation.as_str())
                        {
                            child.terminate().await?;
                            active = None;
                        }
                    },
                    _ = interval.tick() => {
                        if let Some(child) = &mut active {
                            let generation = child.generation.clone();
                            let healthy = tokio::select! {
                                result = child.probe(config.max_rss_bytes) => result.is_ok(),
                                _ = shutdown.changed() => break 'supervisor,
                                _ = memory.changed() => continue 'supervisor,
                                _ = force.wait_for(|forced| {
                                    forced.as_deref() == Some(generation.as_str())
                                }) => false,
                            };
                            if !healthy {
                                tracing::warn!("Native resident failed while waiting for replacement capacity");
                                child.terminate().await?;
                                active = None;
                            }
                        }
                    },
                }
            };
            // Both first startup and replacement serialize with other adapters.
            // Release the permit after promotion when there is no old native child.
            let incumbent_generation = active.as_ref().map(|child| child.generation.clone());
            let candidate = tokio::select! {
                candidate = start(
                    config, descriptor, new_generation(), Some(permit), cleanup.clone(),
                ) => candidate,
                _ = shutdown.changed() => break 'supervisor,
                _ = memory.changed() => continue 'supervisor,
                // Drop watch read guards inside these futures before any
                // selected branch awaits child cleanup.
                _ = async {
                    selection.wait_for(|selection| selection.descriptor != desired.descriptor)
                        .await.map(|_| ())
                } => {
                    continue 'supervisor;
                },
                // Verification and readiness can stall too. Keep the steady
                // child supervised for the entire lifetime of its replacement.
                _ = async {
                    match &mut active {
                        Some(child) => monitor_child(child, config.max_rss_bytes).await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(mut child) = active.take() {
                        child.terminate().await?;
                    }
                    continue 'supervisor;
                },
                _ = async {
                    force.wait_for(|forced| {
                        incumbent_generation.is_some() && *forced == incumbent_generation
                    }).await.map(|_| ())
                } => {
                    if let Some(mut child) = active.take() {
                        child.terminate().await?;
                    }
                    continue 'supervisor;
                },
            };
            match candidate {
                Ok(candidate) => Some(candidate),
                Err(_) => {
                    tracing::warn!("Native resident candidate startup failed");
                    continue;
                },
            }
        } else {
            None
        };
        // A candidate never obtains activation for a replaced committed selection.
        if selection.borrow().descriptor != desired.descriptor
            || *shutdown.borrow()
            || pressure.is_active()
        {
            if let Some(candidate) = &mut candidate {
                candidate.terminate().await?;
            }
            continue;
        }
        if let Some(mut old) = active.take() {
            if let Some(candidate) = &mut candidate {
                old.permit = candidate.permit.take();
                if let Some(permit) = &old.permit {
                    permit.set_phase("draining");
                }
            }
            status.send_replace(NativeResidentStatus {
                selected: desired.descriptor.clone(),
                active_generation: Some(old.generation.clone()),
                active_descriptor: Some(old.descriptor.clone()),
                phase: "draining",
            });
            let mut candidate_failed = false;
            let retirement = match candidate.as_mut() {
                Some(candidate) => tokio::select! {
                    result = old.retire(
                        config.max_rss_bytes, &pressure, &mut shutdown, &mut force,
                    ) => result,
                    result = monitor_child(candidate, config.max_rss_bytes) => {
                        candidate_failed = true;
                        result
                    },
                },
                None => {
                    old.retire(config.max_rss_bytes, &pressure, &mut shutdown, &mut force)
                        .await
                },
            };
            if retirement.is_err() {
                tracing::warn!("Native resident replacement interrupted application drain");
            }
            old.terminate().await?;
            if candidate_failed {
                // A later valid reply cannot restore a candidate that already
                // failed protocol, progress, or RSS supervision during drain.
                candidate
                    .as_mut()
                    .expect("Failed native candidate missing")
                    .terminate()
                    .await?;
                continue;
            }
        }
        if selection.borrow().descriptor != desired.descriptor
            || *shutdown.borrow()
            || pressure.is_active()
        {
            if let Some(candidate) = &mut candidate {
                candidate.terminate().await?;
            }
            continue;
        }
        if let Some(candidate) = &mut candidate {
            // A candidate can wait through the predecessor's whole drain. Recheck
            // its coordinator progress before granting activation authority.
            let promotion = tokio::select! {
                result = async {
                    candidate.probe(config.max_rss_bytes).await?;
                    // A publication may replace selection while the readiness
                    // probe yields. Check at the final activation boundary.
                    anyhow::ensure!(selection.borrow().descriptor == desired.descriptor, "Native candidate selection changed");
                    candidate.activate(config.max_rss_bytes).await
                } => result,
                _ = shutdown.changed() => Err(anyhow::anyhow!("Native candidate shutdown")),
                _ = memory.changed() => Err(anyhow::anyhow!("Native candidate memory pressure")),
            };
            if promotion.is_err() {
                candidate.terminate().await?;
                continue;
            }
            if let Some(permit) = candidate.permit.take() {
                permit.release_for_steady_promotion();
            }
        }
        active = candidate;
        status.send_replace(NativeResidentStatus {
            selected: desired.descriptor,
            active_generation: active.as_ref().map(|child| child.generation.clone()),
            active_descriptor: active.as_ref().map(|child| child.descriptor.clone()),
            phase: if active.is_some() {
                "active"
            } else {
                "inactive"
            },
        });
    }
    if let Some(mut child) = active {
        child.terminate().await?;
    }
    cleanup.close();
    tokio::time::timeout(REAP_TIMEOUT, cleanup.wait())
        .await
        .context("Native resident shutdown cleanup is not yet confirmed")?;
    status.send_replace(NativeResidentStatus {
        selected: None,
        active_generation: None,
        active_descriptor: None,
        phase: "stopped",
    });
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn descriptor() -> NativeResidentDescriptor {
        NativeResidentDescriptor {
            artifact_sha256: "a".repeat(64),
            configuration_sha256: "b".repeat(64),
            lifecycle_protocol: 1,
            application_contract: "example-v1".to_owned(),
        }
    }

    async fn fixture(mode: &str, permit: Option<SurgePermit>) -> ResidentChild {
        // A real OS child exercises pipe readiness, progress, disconnect, and
        // reaping without coupling lifecycle tests to a particular application.
        let mut child = Command::new("python3")
            .args(["-u", "-c", r#"
import json, sys, time
mode = sys.argv[1]
for line in sys.stdin:
    msg = json.loads(line)
    kind = msg['type']
    generation = msg['generation']
    if kind == 'initialize':
        if mode == 'log_flood':
            sys.stderr.write('x' * 1000000 + '\n' + '{\"event\":\"fixture\"}\n' * 1000)
            sys.stderr.flush()
        print(json.dumps(dict(type='ready', generation=generation, **msg['descriptor'])), flush=True)
        if mode == 'blocked_input':
            time.sleep(30)
    elif kind == 'probe':
        if mode == 'stalled':
            time.sleep(30)
        elif mode == 'disconnect':
            break
        else:
            if mode == 'delayed':
                time.sleep(0.1)
            print(json.dumps(dict(type='progress', generation=generation, nonce=msg['nonce'] + (1 if mode == 'wrong_nonce' else 0))), flush=True)
    elif kind == 'retire':
        print(json.dumps(dict(type='drained', generation=generation)), flush=True)
"#, mode])
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
            .kill_on_drop(true).spawn().unwrap();
        if let Some(permit) = &permit {
            permit.require_confirmed_cleanup();
        }
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let logs = forward_logs(child.stderr.take().unwrap(), "test-1".into());
        let mut child = ResidentChild {
            logs,
            child: Some(child),
            stdin,
            stdout,
            generation: "test-1".into(),
            descriptor: descriptor(),
            permit,
            probe: 0,
            pending_probe: None,
            pending_write: None,
            pending: Vec::new(),
            cleanup: TaskTracker::new(),
        };
        child.send(json!({"type":"initialize", "generation":child.generation, "descriptor":descriptor(), "configuration":{}}), usize::MAX).await.unwrap();
        child
            .ready(&descriptor(), usize::MAX, Instant::now() + STARTUP_TIMEOUT)
            .await
            .unwrap();
        child
    }

    #[tokio::test]
    async fn resident_progress_and_explicit_drain_precede_confirmed_reap() {
        let surge = SurgeCoordinator::new();
        let permit = surge
            .acquire(SurgePriority::Deployment, Arc::from("native"))
            .await;
        let mut child = fixture("log_flood", Some(permit)).await;
        child.activate(usize::MAX).await.unwrap();
        child.probe(usize::MAX).await.unwrap();
        let (_shutdown, mut shutdown) = watch::channel(false);
        let (_force, mut force) = watch::channel(None);
        child
            .retire(
                usize::MAX,
                &MemoryPressureSignal::default(),
                &mut shutdown,
                &mut force,
            )
            .await
            .unwrap();
        // Drained is an application outcome, not proof that the process exited.
        assert!(child.child.as_mut().unwrap().try_wait().unwrap().is_none());
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            surge.acquire(SurgePriority::Deployment, Arc::from("node"))
        )
        .await
        .is_err());
        child.terminate().await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            surge.acquire(SurgePriority::Deployment, Arc::from("node")),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stale_progress_and_disconnect_fail_health() {
        for mode in ["wrong_nonce", "disconnect"] {
            let mut child = fixture(mode, None).await;
            assert!(child.probe(usize::MAX).await.is_err());
            child.terminate().await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_candidate_probe_resumes_exact_nonce_and_deadline() {
        let mut child = fixture("delayed", None).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), child.probe(usize::MAX))
                .await
                .is_err()
        );
        let deadline = child.pending_probe.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), child.probe(usize::MAX))
                .await
                .is_err()
        );
        assert_eq!(child.pending_probe, Some(deadline));
        child.probe(usize::MAX).await.unwrap();
        assert_eq!(child.probe, 1);
        assert!(child.pending_probe.is_none());
        assert!(Instant::now() < deadline);
        child.terminate().await.unwrap();
    }

    #[tokio::test]
    async fn blocked_probe_write_preserves_budget_and_pressure_interrupts_retirement() {
        let mut child = fixture("blocked_input", None).await;
        let pid = child.child.as_ref().unwrap().id().unwrap();
        let capacity = unsafe { libc::fcntl(child.stdin.as_raw_fd(), libc::F_GETPIPE_SZ) };
        assert!(capacity > 0);
        child
            .stdin
            .write_all(&vec![b' '; capacity as usize])
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), child.probe(usize::MAX))
                .await
                .is_err()
        );
        let deadline = child.pending_probe.unwrap();
        assert!(child.pending_write.is_some());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), child.probe(usize::MAX))
                .await
                .is_err()
        );
        assert_eq!(child.pending_probe, Some(deadline));
        assert_eq!(child.probe, 1);
        let pressure = MemoryPressureSignal::default();
        let trigger = pressure.clone();
        let publish_pressure = async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.set_active(true);
        };
        let (_shutdown, mut shutdown) = watch::channel(false);
        let (_force, mut force) = watch::channel(None);
        let (_, retired) = tokio::join!(
            publish_pressure,
            tokio::time::timeout(
                Duration::from_secs(1),
                child.retire(usize::MAX, &pressure, &mut shutdown, &mut force),
            )
        );
        assert!(retired
            .expect("Pressure waited for the blocked write timeout")
            .is_err());
        child.terminate().await.unwrap();
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }

    #[tokio::test]
    async fn cancelled_shutdown_preserves_supervisor_failure_for_later_callers() {
        let supervisor = NativeResidentSupervisor::with_configuration(
            None,
            SurgeCoordinator::new(),
            MemoryPressureSignal::default(),
        )
        .unwrap();
        // Inject an invariant failure in the real supervisor task before join.
        supervisor.selection.send_replace(Selection {
            version: Timestamp::try_from(1u64).unwrap(),
            descriptor: Some(descriptor()),
        });
        assert!(supervisor.task.clone().await.is_err());
        let cleanup = supervisor.cleanup.token();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), supervisor.shutdown())
                .await
                .is_err()
        );
        drop(cleanup);
        for _ in 0..2 {
            assert!(supervisor
                .shutdown()
                .await
                .unwrap_err()
                .to_string()
                .contains("without configuration"));
        }
    }

    #[tokio::test]
    async fn candidate_cancellation_keeps_surge_until_reaped() {
        let surge = SurgeCoordinator::new();
        let permit = surge
            .acquire(SurgePriority::Deployment, Arc::from("native"))
            .await;
        let child = fixture("healthy", Some(permit)).await;
        let pid = child.child.as_ref().unwrap().id().unwrap();
        drop(child);
        let _permit = tokio::time::timeout(
            Duration::from_secs(2),
            surge.acquire(SurgePriority::Deployment, Arc::from("node")),
        )
        .await
        .unwrap();
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }

    #[tokio::test]
    async fn forced_preparation_reclaims_shared_drain_only_after_reaping() {
        let surge = SurgeCoordinator::new();
        let permit = surge
            .acquire(SurgePriority::Deployment, Arc::from("node"))
            .await;
        let mut old = fixture("healthy", Some(permit.clone())).await;
        let pid = old.child.as_ref().unwrap().id().unwrap();
        permit.set_phase("draining");
        let root = tempfile::tempdir().unwrap();
        let supervisor = NativeResidentSupervisor::with_configuration(
            Some(Configuration {
                artifacts: root.path().join("unpublished"),
                configurations: root.path().join("configurations"),
                max_rss_bytes: usize::MAX,
            }),
            surge.clone(),
            MemoryPressureSignal::default(),
        )
        .unwrap();
        let preparation = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.prepare(&descriptor(), true).await })
        };
        tokio::time::timeout(Duration::from_secs(1), permit.wait_until_preempted())
            .await
            .unwrap();
        assert!(!preparation.is_finished());
        old.terminate().await.unwrap();
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        drop(permit);
        // Verification can only begin after that shared surge owner was reaped.
        let error = tokio::time::timeout(Duration::from_secs(1), preparation)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("input is unavailable"));
        supervisor.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), surge.acquire_deployment(false))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn worker_responsiveness_cannot_replace_coordinator_progress() {
        let mut child = fixture("stalled", None).await;
        let started = Instant::now();
        assert!(child.probe(usize::MAX).await.is_err());
        assert!(started.elapsed() < Duration::from_secs(10));
        child.terminate().await.unwrap();
    }

    #[test]
    fn artifact_verification_rejects_partial_and_changed_inputs() {
        use std::io::Write;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"complete executable").unwrap();
        let digest = Sha256::hash(b"complete executable").as_hex();
        assert!(verified_file(file.path(), &digest, 1024).is_ok());
        assert!(verified_file(file.path(), &digest, 2).is_err());
        file.as_file_mut().set_len(8).unwrap();
        assert!(verified_file(file.path(), &digest, 1024).is_err());
    }
}
