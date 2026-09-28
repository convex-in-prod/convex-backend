use common::{
    types::ActiveJavascriptClass,
    wasm_runtime_policy::{
        runtime_policy_store,
        RuntimePolicySnapshot,
        RUNTIME_PUBLICATION_VALIDATION_TIMEOUT,
    },
};
use futures::{
    future::{
        BoxFuture,
        Shared,
    },
    FutureExt,
};
use sync_types::Timestamp;
use tokio::sync::{
    Notify,
    OwnedSemaphorePermit,
};

use super::{
    source_keyed_deployment_catalog::{
        SourceKeyedDeploymentCatalog,
        SourceKeyedGenerationSelector,
    },
    *,
};
use crate::{
    concurrency_limiter::ConcurrencyPermitPhase,
    environment::udf::wasm_udf_package::{
        load_runtime_registry_generation_from_manifest,
        AuthenticatedRuntimeRegistryGenerationManifest,
        WasmUdfPackageError,
    },
};

pub(super) mod admission;
mod preparation;
pub use self::preparation::initialize_runtime_preparation;
pub(crate) use self::preparation::{
    DemandObservation,
    PrimaryDemandObservation,
};

const GENERATED_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);
static GENERATED_GENERATION_RETIREMENT_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[derive(Clone, Copy)]
enum GeneratedFailureLog {
    IdleRuntimeCleanup,
    RetiredGenerationCleanup,
    RegistryReloadRejected,
    RegistryReloadTask,
    RetiredUnstartedCleanup,
    RejectedUnstartedCleanup,
    RuntimePreparation,
}

#[derive(Clone, Copy)]
pub(super) struct IdleEvictionLimit<'a> {
    pub(super) deadline: tokio::time::Instant,
    pub(super) cancellation: Option<&'a CancellationSignal>,
}

impl IdleEvictionLimit<'_> {
    pub(super) fn maintenance() -> Self {
        Self {
            deadline: tokio::time::Instant::now() + GENERATED_RUNTIME_DESTROY_TIMEOUT,
            cancellation: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum IdleEvictionError {
    #[error("generated Wasm idle runtime cleanup cancelled")]
    Cancelled,
    #[error(transparent)]
    Cleanup(#[from] anyhow::Error),
    #[error("generated Wasm idle runtime cleanup exceeded its deadline")]
    Deadline,
}

impl GeneratedFailureLog {
    const COUNT: usize = 7;

    const fn index(self) -> usize {
        match self {
            Self::IdleRuntimeCleanup => 0,
            Self::RetiredGenerationCleanup => 1,
            Self::RegistryReloadRejected => 2,
            Self::RegistryReloadTask => 3,
            Self::RetiredUnstartedCleanup => 4,
            Self::RejectedUnstartedCleanup => 5,
            Self::RuntimePreparation => 6,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::IdleRuntimeCleanup => "idle_runtime_cleanup",
            Self::RetiredGenerationCleanup => "retired_generation_cleanup",
            Self::RegistryReloadRejected => "registry_reload_rejected",
            Self::RegistryReloadTask => "registry_reload_task",
            Self::RetiredUnstartedCleanup => "retired_unstarted_cleanup",
            Self::RejectedUnstartedCleanup => "rejected_unstarted_cleanup",
            Self::RuntimePreparation => "runtime_preparation",
        }
    }
}

struct GeneratedFailureLogLimiter {
    last_logged: [Option<Instant>; GeneratedFailureLog::COUNT],
}

impl GeneratedFailureLogLimiter {
    const fn new() -> Self {
        Self {
            last_logged: [None; GeneratedFailureLog::COUNT],
        }
    }

    fn should_log(&mut self, failure: GeneratedFailureLog, now: Instant) -> bool {
        let last_logged = &mut self.last_logged[failure.index()];
        if last_logged.is_some_and(|last_logged| {
            now.saturating_duration_since(last_logged) < GENERATED_FAILURE_LOG_INTERVAL
        }) {
            return false;
        }
        *last_logged = Some(now);
        true
    }
}

static GENERATED_FAILURE_LOG_LIMITER: LazyLock<parking_lot::Mutex<GeneratedFailureLogLimiter>> =
    LazyLock::new(|| parking_lot::Mutex::new(GeneratedFailureLogLimiter::new()));

fn should_log_generated_failure(failure: GeneratedFailureLog) -> bool {
    GENERATED_FAILURE_LOG_LIMITER
        .lock()
        .should_log(failure, Instant::now())
}

fn log_generated_failure(failure: GeneratedFailureLog) {
    if should_log_generated_failure(failure) {
        // Cleanup and registry errors may contain paths or provider data; only
        // the fixed failure class is safe for this maintenance log.
        tracing::error!(
            failure = failure.label(),
            "Generated Wasm maintenance operation failed"
        );
    }
}

pub(super) struct RouteConfiguration {
    active_wasm_cpu_limiter: Option<ConcurrencyLimiter>,
    custom_path: Option<String>,
    deployment_registry: Option<Arc<DeploymentRegistry>>,
    generated_memory_admission_wait: Duration,
    pub(super) generated_memory_controller: Arc<GeneratedMemoryController>,
    host_secret_selectors: BTreeSet<String>,
    lifecycle_barrier: Option<Arc<GeneratedLifecycleBarrier>>,
    package_directory: Option<PathBuf>,
    reuse_instances: bool,
    serialized_module_snapshot_directory: Option<PathBuf>,
}

struct DeploymentRegistryState {
    current: Option<Arc<DeploymentGeneration>>,
    rejected_current_sha256: Option<String>,
    source_keyed_catalog: Option<SourceKeyedDeploymentCatalog>,
    source_keyed_query_shadow_observation: Option<SourceKeyedQueryShadowObservation>,
    source_keyed_shadow_evidence:
        BTreeMap<SourceKeyedGenerationSelector, Arc<QueryShadowRouteSelection>>,
}

struct SourceKeyedQueryShadowObservation {
    begin_timestamp: Timestamp,
    selection: Arc<QueryShadowRouteSelection>,
}

pub(super) struct DeploymentRegistry {
    primary_routing_enabled: bool,
    shadow_routing_enabled: bool,
    root: Option<PathBuf>,
    state: parking_lot::RwLock<DeploymentRegistryState>,
    source_keyed_residency: Option<Arc<SourceKeyedResidency>>,
}

struct SourceKeyedLoadContext {
    controller: Arc<GeneratedMemoryController>,
    serialized_module_snapshot_directory: PathBuf,
}

struct SourceKeyedResidentGeneration {
    generation: Arc<DeploymentGeneration>,
    last_used: u64,
}

#[derive(Default)]
struct SourceKeyedResidencyState {
    clock: u64,
    resident: BTreeMap<SourceKeyedGenerationSelector, SourceKeyedResidentGeneration>,
    // This selector comes from the database source-package record. Candidate
    // readiness may retain another generation, but it must never evict this
    // serving owner merely because it was least recently touched.
    selected: Option<SourceKeyedGenerationSelector>,
    selected_at: Option<Timestamp>,
    activation_pins: Vec<Weak<SourceKeyedActivationPin>>,
    retired: Vec<Weak<DeploymentGeneration>>,
}

struct SourceKeyedActivationPin {
    generation: Arc<DeploymentGeneration>,
}

impl SourceKeyedResidencyState {
    fn activation_pinned(&self, generation: &Arc<DeploymentGeneration>) -> bool {
        self.activation_pins
            .iter()
            .filter_map(Weak::upgrade)
            .any(|pin| Arc::ptr_eq(&pin.generation, generation))
    }

    fn evict_lru_before_preload(
        &mut self,
    ) -> anyhow::Result<Option<SourceKeyedResidentGeneration>> {
        if self.resident.len() < 2 {
            return Ok(None);
        }
        assert_eq!(
            self.resident.len(),
            2,
            "source-keyed residency exceeds its bound"
        );
        let victim = self
            .resident
            .iter()
            .filter(|(selector, resident)| {
                self.selected.as_ref() != Some(*selector)
                    && !self.activation_pinned(&resident.generation)
            })
            .min_by_key(|(_, resident)| resident.last_used)
            .map(|(selector, _)| selector.clone())
            .context("source-keyed residency has no reclaimable generation")?;
        Ok(self.resident.remove(&victim))
    }

    fn insert_and_evict_lru(
        &mut self,
        selector: SourceKeyedGenerationSelector,
        generation: Arc<DeploymentGeneration>,
    ) -> Option<SourceKeyedResidentGeneration> {
        self.clock = self
            .clock
            .checked_add(1)
            .expect("source-keyed residency clock overflow");
        let clock = self.clock;
        assert!(
            self.resident
                .insert(
                    selector.clone(),
                    SourceKeyedResidentGeneration {
                        generation,
                        last_used: clock,
                    },
                )
                .is_none(),
            "source-keyed residency published one selector twice"
        );
        if self.resident.len() <= 2 {
            return None;
        }
        // Residency owns at most two full generations. External Arc owners do
        // not pin a registry slot; retirement preserves those owners while
        // fencing caches and pooled instances from a later incarnation.
        let victim = self
            .resident
            .iter()
            .filter(|(candidate, resident)| {
                **candidate != selector
                    && self.selected.as_ref() != Some(*candidate)
                    && !self.activation_pinned(&resident.generation)
            })
            .min_by_key(|(_, resident)| resident.last_used)
            .map(|(candidate, _)| candidate.clone())
            .expect("over-capacity source-keyed residency has no reclaimable LRU victim");
        self.resident.remove(&victim)
    }
}

#[derive(Clone)]
enum SourceKeyedLoadFailure {
    CapacityUnavailable,
    Failed(Arc<anyhow::Error>),
}

type SourceKeyedLoadResult = Result<Arc<DeploymentGeneration>, SourceKeyedLoadFailure>;
#[derive(Clone)]
struct SourceKeyedLoadFlight {
    result: Shared<BoxFuture<'static, SourceKeyedLoadResult>>,
    waiters: Arc<AtomicUsize>,
}

struct SourceKeyedLoadWaiter(Arc<AtomicUsize>);

impl Drop for SourceKeyedLoadWaiter {
    fn drop(&mut self) {
        assert!(
            self.0.fetch_sub(1, Ordering::AcqRel) > 0,
            "source-keyed load waiter underflow"
        );
    }
}

struct SourceKeyedLoadOwner {
    residency: Arc<SourceKeyedResidency>,
    selector: SourceKeyedGenerationSelector,
}

impl Drop for SourceKeyedLoadOwner {
    fn drop(&mut self) {
        // A panic must not leave a failed shared future permanently owning this
        // selector. The creator holds this same short lock through insertion.
        self.residency.inflight.lock().remove(&self.selector);
    }
}

struct SourceKeyedAuthenticatedManifest {
    descriptor: Arc<ValidatedRuntimeRegistryGenerationDescriptor>,
    manifest: Arc<AuthenticatedRuntimeRegistryGenerationManifest>,
}

#[derive(Default)]
struct SourceKeyedManifestCache {
    entries: VecDeque<SourceKeyedAuthenticatedManifest>,
    #[cfg(test)]
    started_load_pause: Option<(
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
    #[cfg(test)]
    started_loads: usize,
}

struct SourceKeyedResidency {
    root: PathBuf,
    registry_use: RegistryUse,
    context: OnceLock<SourceKeyedLoadContext>,
    state: parking_lot::Mutex<SourceKeyedResidencyState>,
    // Authentication has its own bounded owner so excluded exports need not
    // wait for module memory or AOT readiness. The blocking owner retains this
    // lock through publication when the last request's deadline expires.
    manifests: Arc<tokio::sync::Mutex<SourceKeyedManifestCache>>,
    cold_load: tokio::sync::Semaphore,
    inflight: parking_lot::Mutex<BTreeMap<SourceKeyedGenerationSelector, SourceKeyedLoadFlight>>,
    #[cfg(test)]
    started_load_pause: parking_lot::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

impl SourceKeyedResidency {
    fn new(root: PathBuf, registry_use: RegistryUse) -> Arc<Self> {
        Arc::new(Self {
            root,
            registry_use,
            context: OnceLock::new(),
            state: parking_lot::Mutex::new(SourceKeyedResidencyState::default()),
            manifests: Arc::new(tokio::sync::Mutex::new(SourceKeyedManifestCache::default())),
            cold_load: tokio::sync::Semaphore::new(1),
            inflight: parking_lot::Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            started_load_pause: parking_lot::Mutex::new(None),
        })
    }

    async fn authenticated_manifest(
        &self,
        registry: &DeploymentRegistry,
        descriptor: Arc<ValidatedRuntimeRegistryGenerationDescriptor>,
    ) -> anyhow::Result<Arc<AuthenticatedRuntimeRegistryGenerationManifest>> {
        let selector = SourceKeyedGenerationSelector::from_descriptor(&descriptor)?;
        // Request admission bounds this wait. An already admitted generation
        // loader keeps ownership until its finite reconstruction completes.
        let mut cache = Arc::clone(&self.manifests).lock_owned().await;
        anyhow::ensure!(
            registry.descriptor_incarnation_is_current(&selector, &descriptor),
            "source-keyed runtime descriptor changed before manifest authentication"
        );
        // Catalog successors preserve each unchanged descriptor's Arc. A
        // withdrawn and subsequently restaged descriptor must authenticate anew.
        let manifest = if let Some(index) = cache
            .entries
            .iter()
            .position(|entry| Arc::ptr_eq(&entry.descriptor, &descriptor))
        {
            let entry = cache
                .entries
                .remove(index)
                .expect("manifest cache entry disappeared");
            let manifest = Arc::clone(&entry.manifest);
            cache.entries.push_back(entry);
            manifest
        } else {
            let root = self.root.clone();
            let descriptor = Arc::clone(&descriptor);
            tokio_spawn_blocking("source_keyed_manifest_authentication", move || {
                #[cfg(test)]
                {
                    cache.started_loads += 1;
                    if let Some((reached, resume)) = cache.started_load_pause.take() {
                        reached
                            .send(())
                            .expect("manifest test dropped its observer");
                        resume
                            .recv()
                            .expect("manifest test dropped its resume authority");
                    }
                }
                let manifest = Arc::new(load_runtime_registry_generation_manifest(
                    &root,
                    &descriptor,
                )?);
                // Match the full-generation residency bound without retaining
                // arbitrary historical manifests or duplicating parsed registries.
                if cache.entries.len() == 2 {
                    cache.entries.pop_front();
                }
                cache.entries.push_back(SourceKeyedAuthenticatedManifest {
                    descriptor,
                    manifest: Arc::clone(&manifest),
                });
                Ok::<_, WasmUdfPackageError>(manifest)
            })
            .await
            .context("source-keyed manifest authentication task failed")??
        };
        anyhow::ensure!(
            registry.descriptor_incarnation_is_current(&selector, &descriptor),
            "source-keyed runtime descriptor changed during manifest authentication"
        );
        Ok(manifest)
    }

    fn configure(
        &self,
        controller: Arc<GeneratedMemoryController>,
        serialized_module_snapshot_directory: PathBuf,
    ) -> anyhow::Result<()> {
        if let Some(context) = self.context.get() {
            anyhow::ensure!(
                Arc::ptr_eq(&context.controller, &controller)
                    && context.serialized_module_snapshot_directory
                        == serialized_module_snapshot_directory,
                "source-keyed residency configuration changed after initialization"
            );
            return Ok(());
        }
        self.context
            .set(SourceKeyedLoadContext {
                controller,
                serialized_module_snapshot_directory,
            })
            .map_err(|_| anyhow::anyhow!("source-keyed residency was configured twice"))
    }

    fn resident(
        &self,
        selector: &SourceKeyedGenerationSelector,
    ) -> Option<Arc<DeploymentGeneration>> {
        let mut state = self.state.lock();
        if !state.resident.contains_key(selector) {
            return None;
        }
        state.clock = state
            .clock
            .checked_add(1)
            .expect("source-keyed residency clock overflow");
        let clock = state.clock;
        let resident = state
            .resident
            .get_mut(selector)
            .expect("source-keyed resident disappeared while locked");
        resident.last_used = clock;
        Some(Arc::clone(&resident.generation))
    }

    fn observe_selected(
        &self,
        selector: Option<SourceKeyedGenerationSelector>,
        at: Timestamp,
    ) -> anyhow::Result<bool> {
        let mut state = self.state.lock();
        if state.selected_at.is_some_and(|previous| at < previous) {
            return Ok(false);
        }
        if state.selected_at == Some(at) {
            anyhow::ensure!(
                state.selected == selector,
                "source selection disagrees at one snapshot"
            );
        }
        state.selected = selector;
        state.selected_at = Some(at);
        if let Some(generation) = state
            .selected
            .as_ref()
            .and_then(|selector| state.resident.get(selector))
            .map(|resident| &resident.generation)
        {
            preparation::set_selected_generation(generation);
        } else {
            preparation::clear_selected_generation();
        }
        drop(state);
        GENERATED_GENERATION_RETIREMENT_NOTIFY.notify_waiters();
        Ok(true)
    }

    fn owns_replenishment(&self, generation: &Arc<DeploymentGeneration>) -> bool {
        let state = self.state.lock();
        state
            .selected
            .as_ref()
            .and_then(|selector| state.resident.get(selector))
            .is_some_and(|resident| Arc::ptr_eq(&resident.generation, generation))
    }

    fn retained_graph_generations(&self) -> Vec<Arc<DeploymentGeneration>> {
        self.state
            .lock()
            .resident
            .values()
            .map(|resident| Arc::clone(&resident.generation))
            .collect()
    }

    fn retire_other_generation_for_admission(&self, protected: &Arc<DeploymentGeneration>) -> bool {
        let victim = {
            let mut state = self.state.lock();
            let selected = state.selected.clone();
            let selector = state
                .resident
                .iter()
                .filter(|(selector, resident)| {
                    !Arc::ptr_eq(&resident.generation, protected)
                        && selected.as_ref() != Some(*selector)
                        && !state.activation_pinned(&resident.generation)
                })
                .min_by_key(|(_, resident)| resident.last_used)
                .map(|(selector, _)| selector.clone());
            let Some(selector) = selector else {
                return false;
            };
            let victim = state
                .resident
                .remove(&selector)
                .expect("admission eviction victim disappeared while locked");
            state.retired.push(Arc::downgrade(&victim.generation));
            victim
        };
        // Readiness may retain two generations that fit the module budget but
        // leave no room for an invocation. Reclaim the other incarnation only
        // when admission needs memory. Existing invocations keep their Arcs;
        // their idle instances drain through the ordinary retirement path.
        retire_source_keyed_generation(&victim.generation);
        drop(victim);
        GENERATED_ROUTED_MODULES
            .lock()
            .evict_all_idle_shared_aot_modules();
        true
    }

    async fn ensure(
        self: &Arc<Self>,
        registry: &Arc<DeploymentRegistry>,
        selector: SourceKeyedGenerationSelector,
        descriptor: Arc<ValidatedRuntimeRegistryGenerationDescriptor>,
        deadline: tokio::time::Instant,
    ) -> anyhow::Result<Arc<DeploymentGeneration>> {
        if let Some(generation) = self.resident(&selector) {
            return Ok(generation);
        }
        let (flight, _waiter) = {
            let mut inflight = self.inflight.lock();
            if tokio::time::Instant::now() >= deadline {
                return Err(SourceKeyedRuntimeCapacityUnavailable.into());
            }
            if let Some(flight) = inflight.get(&selector) {
                flight.waiters.fetch_add(1, Ordering::AcqRel);
                (
                    flight.clone(),
                    SourceKeyedLoadWaiter(Arc::clone(&flight.waiters)),
                )
            } else {
                let waiters = Arc::new(AtomicUsize::new(1));
                let task_waiters = Arc::clone(&waiters);
                let owner = SourceKeyedLoadOwner {
                    residency: Arc::clone(self),
                    selector: selector.clone(),
                };
                let task_registry = Arc::clone(registry);
                let task = tokio::spawn(async move {
                    owner
                        .residency
                        .load_and_publish(
                            &task_registry,
                            owner.selector.clone(),
                            descriptor,
                            deadline,
                            &task_waiters,
                        )
                        .await
                });
                let future = async move {
                    let result = match task.await {
                        Ok(Ok(generation)) => Ok(generation),
                        Ok(Err(error)) => {
                            let temporary_capacity = error
                                .downcast_ref::<SourceKeyedRuntimeCapacityUnavailable>()
                                .is_some()
                                || matches!(
                                    error.downcast_ref::<ModuleMemoryAdmissionError>(),
                                    Some(
                                        ModuleMemoryAdmissionError::Pressure
                                            | ModuleMemoryAdmissionError::SoftBudget
                                            | ModuleMemoryAdmissionError::HardBudget
                                    )
                                );
                            Err(if temporary_capacity {
                                SourceKeyedLoadFailure::CapacityUnavailable
                            } else {
                                SourceKeyedLoadFailure::Failed(Arc::new(error))
                            })
                        },
                        Err(error) => {
                            Err(SourceKeyedLoadFailure::Failed(Arc::new(anyhow::anyhow!(
                                "exact source-keyed generation load task failed: {error}"
                            ))))
                        },
                    };
                    result
                }
                .boxed()
                .shared();
                let flight = SourceKeyedLoadFlight {
                    result: future,
                    waiters: Arc::clone(&waiters),
                };
                inflight.insert(selector, flight.clone());
                (flight, SourceKeyedLoadWaiter(waiters))
            }
        };
        tokio::time::timeout_at(deadline, flight.result)
            .await
            .map_err(|_| SourceKeyedRuntimeCapacityUnavailable)?
            .map_err(|error| match error {
                SourceKeyedLoadFailure::CapacityUnavailable => {
                    SourceKeyedRuntimeCapacityUnavailable.into()
                },
                SourceKeyedLoadFailure::Failed(error) => anyhow::anyhow!("{error:#}"),
            })
    }

    async fn load_and_publish(
        &self,
        registry: &Arc<DeploymentRegistry>,
        selector: SourceKeyedGenerationSelector,
        descriptor: Arc<ValidatedRuntimeRegistryGenerationDescriptor>,
        deadline: tokio::time::Instant,
        waiters: &AtomicUsize,
    ) -> anyhow::Result<Arc<DeploymentGeneration>> {
        let _cold_load = tokio::time::timeout_at(deadline, self.cold_load.acquire())
            .await
            .map_err(|_| SourceKeyedRuntimeCapacityUnavailable)??;
        if tokio::time::Instant::now() >= deadline
            || (waiters.load(Ordering::Acquire) == 0
                && self.state.lock().selected.as_ref() != Some(&selector))
        {
            return Err(SourceKeyedRuntimeCapacityUnavailable.into());
        }
        // Acquiring the cold-load slot transfers this finite load to its owner.
        // Caller deadlines only bound waiting: discarding completed AOT work
        // would make a valid reconstruction longer than that bound never ready.
        // Keep the slot and memory charges through publication, even if all
        // waiters leave. Selected and activation-pinned residents stay protected.
        if let Some(generation) = self.resident(&selector) {
            return Ok(generation);
        }
        #[cfg(test)]
        {
            let pause = self.started_load_pause.lock().take();
            if let Some((reached, resume)) = pause {
                reached
                    .send(())
                    .expect("cold-load test dropped its observer");
                resume
                    .await
                    .context("cold-load test dropped its resume authority")?;
            }
        }
        let context = self
            .context
            .get()
            .context("source-keyed residency is not configured")?;
        let manifest = self
            .authenticated_manifest(registry, Arc::clone(&descriptor))
            .await?;
        let retained = self.retained_graph_generations();
        let root = self.root.clone();
        let descriptor_for_load = Arc::clone(&descriptor);
        let controller = Arc::clone(&context.controller);
        let snapshot_directory = context.serialized_module_snapshot_directory.clone();
        let registry_use = self.registry_use;
        let generation = tokio_spawn_blocking("source_keyed_exact_generation_load", move || {
            let retained_catalogs = retained
                .iter()
                .filter_map(|generation| generation.module_graph_catalog.as_ref())
                .collect::<Vec<_>>();
            let generation = load_runtime_registry_generation_from_manifest(
                &root,
                &descriptor_for_load,
                &manifest,
                &retained_catalogs,
            )
            .map(DeploymentGeneration::from_validated)
            .context("failed to load exact source-keyed runtime generation")?;
            ensure_registry_admission(
                registry_use == RegistryUse::Primary,
                registry_use == RegistryUse::Shadow,
                &generation,
            )?;
            Ok::<_, anyhow::Error>(generation)
        })
        .await
        .context("exact source-keyed generation load task failed")??;
        if !registry.descriptor_is_current(&selector, &descriptor) {
            discard_unpublished_generation(generation);
            anyhow::bail!("source-keyed runtime descriptor changed during exact generation load");
        }

        // Authenticate the candidate before changing residency, then release
        // the old LRU before preloading. Keeping two old generations through
        // preload otherwise requires capacity for a third full generation.
        // The cold-load semaphore reserves this slot until publication; Arc
        // owners of the retired incarnation can still finish their invocations.
        let victim = match self.state.lock().evict_lru_before_preload() {
            Ok(victim) => victim,
            Err(_) => {
                discard_unpublished_generation(generation);
                return Err(SourceKeyedRuntimeCapacityUnavailable.into());
            },
        };
        if let Some(victim) = victim {
            retire_source_keyed_generation(&victim.generation);
            self.state
                .lock()
                .retired
                .push(Arc::downgrade(&victim.generation));
            // Drop residency's owner before the idle sweep; retained module
            // owners must not make an otherwise unused AOT entry appear busy.
            drop(victim);
            GENERATED_ROUTED_MODULES
                .lock()
                .evict_all_idle_shared_aot_modules();
        }
        let (generation, preloaded_graph_modules) =
            tokio_spawn_blocking("source_keyed_exact_generation_preload", move || {
                let preloaded_graph_modules = match preload_generation_routes(
                    &controller,
                    &snapshot_directory,
                    &generation,
                ) {
                    Ok(modules) => modules,
                    Err(error) => {
                        discard_unpublished_generation(generation);
                        return Err(error);
                    },
                };
                Ok::<_, anyhow::Error>((generation, preloaded_graph_modules))
            })
            .await
            .context("exact source-keyed generation preload task failed")??;
        if !registry.descriptor_is_current(&selector, &descriptor) {
            drop(preloaded_graph_modules);
            discard_unpublished_generation(generation);
            anyhow::bail!("source-keyed runtime descriptor changed during exact generation load");
        }
        if let Err(error) = generation.publish_preloaded_load_verification(preloaded_graph_modules)
        {
            discard_unpublished_generation(generation);
            return Err(error);
        }

        let mut state = self.state.lock();
        if !registry.descriptor_is_current(&selector, &descriptor) {
            drop(state);
            discard_unpublished_generation(generation);
            anyhow::bail!("source-keyed runtime descriptor changed before residency publication");
        }
        let victim = state.insert_and_evict_lru(selector.clone(), Arc::clone(&generation));
        assert!(
            victim.is_none(),
            "source-keyed preload lost its residency slot"
        );
        if state.selected.as_ref() == Some(&selector) {
            preparation::set_selected_generation(&generation);
        }
        drop(state);
        registry.record_source_keyed_shadow_evidence(&selector, &generation);
        Ok(generation)
    }

    fn take_retired(&self) -> Vec<Arc<DeploymentGeneration>> {
        std::mem::take(&mut self.state.lock().retired)
            .into_iter()
            .filter_map(|generation| generation.upgrade())
            .collect()
    }
}

fn registry_use_for_selection(primary_routing_enabled: bool) -> RegistryUse {
    if primary_routing_enabled {
        // A primary-admitted generation is also safe for a non-authoritative
        // verifier. Prefer it whenever primary routing is enabled.
        RegistryUse::Primary
    } else {
        // Source-keyed readiness is established before shadow sampling starts,
        // so dormant routing must accept shadow-only generations.
        RegistryUse::Shadow
    }
}

impl DeploymentRegistry {
    pub(super) fn load(root: PathBuf) -> anyhow::Result<Arc<Self>> {
        Self::load_with_primary_routing(root, false)
    }

    pub(super) fn load_with_primary_routing(
        root: PathBuf,
        primary_routing_enabled: bool,
    ) -> anyhow::Result<Arc<Self>> {
        Self::load_with_routing(root, primary_routing_enabled, false)
    }

    fn load_with_routing(
        root: PathBuf,
        primary_routing_enabled: bool,
        shadow_routing_enabled: bool,
    ) -> anyhow::Result<Arc<Self>> {
        Self::load_with_selection(root, primary_routing_enabled, shadow_routing_enabled, false)
    }

    fn load_with_selection(
        root: PathBuf,
        primary_routing_enabled: bool,
        shadow_routing_enabled: bool,
        source_keyed: bool,
    ) -> anyhow::Result<Arc<Self>> {
        if runtime_registry_is_empty(&root)? {
            anyhow::ensure!(
                !primary_routing_enabled,
                "primary Wasm routing requires a published runtime registry"
            );
            // Retain the registry and reload machinery before the first publication.
            // No generation or authenticated route exists until normal loading succeeds.
            return Ok(Arc::new(Self {
                primary_routing_enabled,
                shadow_routing_enabled,
                source_keyed_residency: source_keyed.then(|| {
                    SourceKeyedResidency::new(
                        root.clone(),
                        registry_use_for_selection(primary_routing_enabled),
                    )
                }),
                root: Some(root),
                state: parking_lot::RwLock::new(DeploymentRegistryState {
                    current: None,
                    rejected_current_sha256: None,
                    source_keyed_catalog: None,
                    source_keyed_query_shadow_observation: None,
                    source_keyed_shadow_evidence: BTreeMap::new(),
                }),
            }));
        }
        if source_keyed {
            let catalog = load_runtime_registry_source_catalog(&root)
                .context("failed to load Wasm UDF runtime source catalog")?;
            let catalog_sha256 = catalog.catalog_sha256().to_owned();
            let registry_use = registry_use_for_selection(primary_routing_enabled);
            let source_keyed_catalog = SourceKeyedDeploymentCatalog::from_descriptors(
                catalog_sha256,
                catalog.into_generations(),
                registry_use,
            )?;
            let source_keyed_residency = SourceKeyedResidency::new(root.clone(), registry_use);
            return Ok(Arc::new(Self {
                primary_routing_enabled,
                shadow_routing_enabled,
                root: Some(root),
                state: parking_lot::RwLock::new(DeploymentRegistryState {
                    current: None,
                    rejected_current_sha256: None,
                    source_keyed_catalog: Some(source_keyed_catalog),
                    source_keyed_query_shadow_observation: None,
                    source_keyed_shadow_evidence: BTreeMap::new(),
                }),
                source_keyed_residency: Some(source_keyed_residency),
            }));
        }
        let current = load_runtime_registry_current(&root)
            .context("failed to load Wasm UDF runtime registry current pointer")?;
        let generation = load_runtime_registry_generation(&root, &current)
            .context("failed to load Wasm UDF runtime registry generation")?;
        Self::from_generation(
            Some(root),
            DeploymentGeneration::from_validated(generation),
            primary_routing_enabled,
            shadow_routing_enabled,
        )
    }

    pub(super) fn from_legacy(
        artifact_cache_root: PathBuf,
        registry: ValidatedDeploymentManifest,
    ) -> Arc<Self> {
        Self::from_legacy_with_primary_routing(artifact_cache_root, registry, false)
            .expect("legacy deployment registry must remain primary-admitted")
    }

    pub(super) fn from_legacy_with_primary_routing(
        artifact_cache_root: PathBuf,
        registry: ValidatedDeploymentManifest,
        primary_routing_enabled: bool,
    ) -> anyhow::Result<Arc<Self>> {
        Self::from_legacy_with_routing(
            artifact_cache_root,
            registry,
            primary_routing_enabled,
            false,
        )
    }

    fn from_legacy_with_routing(
        artifact_cache_root: PathBuf,
        registry: ValidatedDeploymentManifest,
        primary_routing_enabled: bool,
        shadow_routing_enabled: bool,
    ) -> anyhow::Result<Arc<Self>> {
        Self::from_generation(
            None,
            DeploymentGeneration::from_legacy(artifact_cache_root, registry),
            primary_routing_enabled,
            shadow_routing_enabled,
        )
    }

    fn from_generation(
        root: Option<PathBuf>,
        current: Arc<DeploymentGeneration>,
        primary_routing_enabled: bool,
        shadow_routing_enabled: bool,
    ) -> anyhow::Result<Arc<Self>> {
        ensure_registry_admission(primary_routing_enabled, shadow_routing_enabled, &current)?;
        Ok(Arc::new(Self {
            primary_routing_enabled,
            shadow_routing_enabled,
            root,
            state: parking_lot::RwLock::new(DeploymentRegistryState {
                current: Some(current),
                rejected_current_sha256: None,
                source_keyed_catalog: None,
                source_keyed_query_shadow_observation: None,
                source_keyed_shadow_evidence: BTreeMap::new(),
            }),
            source_keyed_residency: None,
        }))
    }

    #[cfg(test)]
    pub(super) fn from_generation_for_test(
        current: Arc<DeploymentGeneration>,
        primary_routing_enabled: bool,
    ) -> anyhow::Result<Arc<Self>> {
        Self::from_generation(None, current, primary_routing_enabled, false)
    }

    #[cfg(test)]
    pub(super) fn from_generation_for_shadow_test(
        current: Arc<DeploymentGeneration>,
    ) -> anyhow::Result<Arc<Self>> {
        Self::from_generation(None, current, false, true)
    }

    #[cfg(test)]
    pub(super) fn current(&self) -> Arc<DeploymentGeneration> {
        self.state
            .read()
            .current
            .as_ref()
            .cloned()
            .expect("source-keyed runtime registry has no current-pointer generation")
    }

    pub(super) fn query_shadow_registry(&self) -> StaticHermesQueryShadowRegistry {
        let state = self.state.read();
        let selection = state
            .source_keyed_query_shadow_observation
            .as_ref()
            .map(|observation| Arc::clone(&observation.selection))
            .or_else(|| {
                state
                    .source_keyed_catalog
                    .is_none()
                    .then(|| {
                        state
                            .current
                            .as_ref()
                            .map(|current| Arc::clone(&current.query_shadow_route_selection))
                    })
                    .flatten()
            });
        let authenticated_selections = state
            .source_keyed_shadow_evidence
            .values()
            .cloned()
            .chain(
                state
                    .source_keyed_catalog
                    .is_none()
                    .then(|| state.current.as_ref().map(Arc::clone))
                    .flatten()
                    .map(|generation| Arc::clone(&generation.query_shadow_route_selection)),
            )
            .collect();
        StaticHermesQueryShadowRegistry {
            selection,
            authenticated_selections,
        }
    }

    fn uses_source_keyed_selection(&self) -> bool {
        self.source_keyed_residency.is_some()
    }

    #[cfg(test)]
    fn source_keyed_runtime_readiness(
        &self,
        runtime_content_sha256: &str,
        generation: &SourceKeyedRuntimeGenerationIdentity,
    ) -> anyhow::Result<SourceKeyedRuntimeReadiness> {
        if !self.uses_source_keyed_selection() {
            return Ok(SourceKeyedRuntimeReadiness::NotStaged);
        }
        let selector = SourceKeyedGenerationSelector::requested(runtime_content_sha256, generation);
        let resident = self
            .source_keyed_residency
            .as_ref()
            .and_then(|residency| residency.resident(&selector));
        Ok(
            if resident.is_some_and(|generation| generation.is_load_verified()) {
                SourceKeyedRuntimeReadiness::Ready
            } else {
                SourceKeyedRuntimeReadiness::NotStaged
            },
        )
    }

    #[cfg(test)]
    fn source_keyed_runtime_generation_identity(
        &self,
        runtime_content_sha256: &str,
        generation: &SourceKeyedRuntimeGenerationIdentity,
    ) -> anyhow::Result<Option<SourceKeyedRuntimeGenerationIdentity>> {
        let selector = SourceKeyedGenerationSelector::requested(runtime_content_sha256, generation);
        let Some(resident) = self
            .source_keyed_residency
            .as_ref()
            .and_then(|residency| residency.resident(&selector))
        else {
            return Ok(None);
        };
        Ok(resident
            .is_load_verified()
            .then(|| source_keyed_generation_identity(&resident)))
    }

    fn configure_source_keyed_residency(
        &self,
        controller: &Arc<GeneratedMemoryController>,
        serialized_module_snapshot_directory: &Path,
    ) -> anyhow::Result<()> {
        self.source_keyed_residency
            .as_ref()
            .context("source-keyed runtime registry has no residency manager")?
            .configure(
                Arc::clone(controller),
                serialized_module_snapshot_directory.to_owned(),
            )
    }

    fn source_keyed_paired_deployment_guard_enabled(&self) -> bool {
        (self.primary_routing_enabled || self.shadow_routing_enabled)
            && self.uses_source_keyed_selection()
    }

    async fn ensure_source_keyed_generation(
        self: &Arc<Self>,
        runtime_content_sha256: &str,
        generation_identity: &SourceKeyedRuntimeGenerationIdentity,
        begin_timestamp: Option<Timestamp>,
        deadline: tokio::time::Instant,
    ) -> anyhow::Result<Option<Arc<DeploymentGeneration>>> {
        let selector =
            SourceKeyedGenerationSelector::requested(runtime_content_sha256, generation_identity);
        let descriptor = {
            let state = self.state.read();
            anyhow::ensure!(
                self.uses_source_keyed_selection(),
                "runtime registry is not configured for source-keyed selection"
            );
            state
                .source_keyed_catalog
                .as_ref()
                .and_then(|catalog| catalog.descriptor(runtime_content_sha256, generation_identity))
        };
        let Some(descriptor) = descriptor else {
            return Ok(None);
        };
        let residency = self
            .source_keyed_residency
            .as_ref()
            .context("source-keyed runtime registry has no residency manager")?;
        if let Some(at) = begin_timestamp {
            // Protect the database's selection before loading needs to reclaim
            // an older resident. An older request cannot regress this owner.
            residency.observe_selected(Some(selector.clone()), at)?;
        }
        let generation = residency
            .ensure(self, selector, descriptor, deadline)
            .await?;
        if let Some(begin_timestamp) = begin_timestamp {
            self.record_source_keyed_selection(&generation, begin_timestamp)?;
        }
        Ok(Some(generation))
    }

    fn record_source_keyed_selection(
        &self,
        generation: &Arc<DeploymentGeneration>,
        begin_timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        let mut state = self.state.write();
        let selection = Arc::clone(&generation.query_shadow_route_selection);
        match state.source_keyed_query_shadow_observation.as_ref() {
            Some(observation) if observation.begin_timestamp > begin_timestamp => {},
            Some(observation) if observation.begin_timestamp == begin_timestamp => {
                anyhow::ensure!(
                    observation.selection.generation_sha256 == selection.generation_sha256,
                    "one committed source snapshot selected multiple Wasm generations"
                );
            },
            Some(_) | None => {
                state.source_keyed_query_shadow_observation =
                    Some(SourceKeyedQueryShadowObservation {
                        begin_timestamp,
                        selection,
                    });
            },
        }
        Ok(())
    }

    fn descriptor_incarnation_is_current(
        &self,
        selector: &SourceKeyedGenerationSelector,
        descriptor: &Arc<ValidatedRuntimeRegistryGenerationDescriptor>,
    ) -> bool {
        self.state
            .read()
            .source_keyed_catalog
            .as_ref()
            .and_then(|catalog| catalog.descriptor_by_selector(selector))
            .is_some_and(|candidate| Arc::ptr_eq(&candidate, descriptor))
    }

    fn descriptor_is_current(
        &self,
        selector: &SourceKeyedGenerationSelector,
        descriptor: &ValidatedRuntimeRegistryGenerationDescriptor,
    ) -> bool {
        self.state
            .read()
            .source_keyed_catalog
            .as_ref()
            .and_then(|catalog| catalog.descriptor_by_selector(selector))
            .is_some_and(|candidate| candidate.as_ref().same_authenticated_identity(descriptor))
    }

    fn record_source_keyed_shadow_evidence(
        &self,
        selector: &SourceKeyedGenerationSelector,
        generation: &Arc<DeploymentGeneration>,
    ) {
        self.state.write().source_keyed_shadow_evidence.insert(
            selector.clone(),
            Arc::clone(&generation.query_shadow_route_selection),
        );
    }

    async fn retire_source_keyed_idle_generations<RT: Runtime>(
        &self,
        controller: &GeneratedMemoryController,
    ) {
        let Some(residency) = &self.source_keyed_residency else {
            return;
        };
        for generation in residency.take_retired() {
            if retire_deployment_generation::<RT>(controller, generation)
                .await
                .is_err()
            {
                log_generated_failure(GeneratedFailureLog::RetiredGenerationCleanup);
            }
        }
    }

    fn reload_source_keyed_catalog(
        &self,
        controller: &Arc<GeneratedMemoryController>,
        serialized_module_snapshot_directory: &Path,
    ) -> anyhow::Result<Option<Arc<DeploymentGeneration>>> {
        let root = self
            .root
            .as_ref()
            .context("source-keyed runtime registry has no root")?;
        if self.state.read().source_keyed_catalog.is_none() && runtime_registry_is_empty(root)? {
            return Ok(None);
        }
        let catalog = load_runtime_registry_source_catalog(root)
            .context("failed to reload Wasm UDF runtime source catalog")?;
        let catalog_sha256 = catalog.catalog_sha256().to_owned();
        if self
            .state
            .read()
            .source_keyed_catalog
            .as_ref()
            .map(SourceKeyedDeploymentCatalog::catalog_sha256)
            == Some(catalog_sha256.as_str())
        {
            return Ok(None);
        }
        let descriptors = catalog.into_generations();
        let (candidate_catalog, previous_catalog_sha256) = {
            let state = self.state.read();
            let previous_catalog_sha256 = state
                .source_keyed_catalog
                .as_ref()
                .map(|previous| previous.catalog_sha256().to_owned());
            let candidate = match &state.source_keyed_catalog {
                Some(previous) => previous.successor(catalog_sha256.clone(), descriptors)?,
                None => SourceKeyedDeploymentCatalog::from_descriptors(
                    catalog_sha256.clone(),
                    descriptors,
                    registry_use_for_selection(self.primary_routing_enabled),
                )?,
            };
            (candidate, previous_catalog_sha256)
        };
        let latest = load_runtime_registry_source_catalog(root)
            .context("failed to confirm Wasm UDF runtime source catalog before publication")?;
        if latest.catalog_sha256() != catalog_sha256 {
            return Ok(None);
        }

        if let Some(residency) = &self.source_keyed_residency {
            residency.configure(
                Arc::clone(controller),
                serialized_module_snapshot_directory.to_owned(),
            )?;
        }

        // Serialize catalog withdrawal with selection and activation reservation.
        // Combined acquisition follows pool -> residency -> catalog; this path
        // needs only the residency -> catalog suffix.
        let residency = self
            .source_keyed_residency
            .as_ref()
            .context("source-keyed catalog lost its residency manager")?;
        let residency = residency.state.lock();
        let mut state = self.state.write();
        let current_catalog_sha256 = state
            .source_keyed_catalog
            .as_ref()
            .map(SourceKeyedDeploymentCatalog::catalog_sha256);
        if current_catalog_sha256 == Some(catalog_sha256.as_str()) {
            return Ok(None);
        }
        // A concurrent reload may have published another catalog while this
        // candidate was authenticated. Retry from that state rather than
        // overwriting it with a successor built from an older snapshot.
        if current_catalog_sha256 != previous_catalog_sha256.as_deref() {
            return Ok(None);
        }
        if let Some(selected) = &residency.selected {
            anyhow::ensure!(
                candidate_catalog.descriptor_by_selector(selected).is_some(),
                "runtime catalog cannot withdraw the selected Wasm generation"
            );
        }
        for (selector, resident) in &residency.resident {
            if residency.activation_pinned(&resident.generation) {
                anyhow::ensure!(
                    candidate_catalog.descriptor_by_selector(selector).is_some(),
                    "runtime catalog cannot withdraw a reserved Wasm generation"
                );
            }
        }
        state.source_keyed_catalog = Some(candidate_catalog);
        drop(state);
        drop(residency);
        log_registry_reload_event("accepted");
        Ok(None)
    }

    fn reload_with_source_preload(
        &self,
        controller: &Arc<GeneratedMemoryController>,
        serialized_module_snapshot_directory: &Path,
    ) -> anyhow::Result<Option<Arc<DeploymentGeneration>>> {
        if self.uses_source_keyed_selection() {
            self.reload_source_keyed_catalog(controller, serialized_module_snapshot_directory)
        } else {
            self.reload_non_source(NonSourceReloadReadiness::Preload {
                controller,
                serialized_module_snapshot_directory,
            })
        }
    }

    #[cfg(test)]
    pub(super) fn reload(&self) -> anyhow::Result<Option<Arc<DeploymentGeneration>>> {
        self.reload_non_source(NonSourceReloadReadiness::UnverifiedTestOnly)
    }

    fn reload_non_source(
        &self,
        readiness: NonSourceReloadReadiness<'_>,
    ) -> anyhow::Result<Option<Arc<DeploymentGeneration>>> {
        anyhow::ensure!(
            !self.uses_source_keyed_selection(),
            "source-keyed runtime registry reload requires route preload context"
        );
        let Some(root) = &self.root else {
            return Ok(None);
        };
        if self.state.read().current.is_none() && runtime_registry_is_empty(root)? {
            return Ok(None);
        }
        let current = load_runtime_registry_current(root)
            .context("failed to reload Wasm UDF runtime registry current pointer")?;
        {
            let state = self.state.read();
            if state
                .current
                .as_ref()
                .is_some_and(|active| current.current_sha256() == active.current_sha256)
                || state.rejected_current_sha256.as_deref() == Some(current.current_sha256())
            {
                return Ok(None);
            }
        }
        let generation = match load_runtime_registry_generation(root, &current) {
            Ok(generation) => generation,
            Err(error) => {
                self.state.write().rejected_current_sha256 =
                    Some(current.current_sha256().to_owned());
                return Err(error).context("Wasm UDF runtime registry reload was rejected");
            },
        };
        let candidate = DeploymentGeneration::from_validated(generation);
        if let Err(error) = ensure_registry_admission(
            self.primary_routing_enabled,
            self.shadow_routing_enabled,
            &candidate,
        ) {
            self.state.write().rejected_current_sha256 = Some(current.current_sha256().to_owned());
            return Err(error).context("Wasm UDF runtime registry reload was rejected");
        }
        let preloaded_graph_modules = match readiness {
            NonSourceReloadReadiness::Preload {
                controller,
                serialized_module_snapshot_directory,
            } => match preload_generation_routes(
                controller,
                serialized_module_snapshot_directory,
                &candidate,
            ) {
                Ok(modules) => Some(modules),
                Err(error) => {
                    self.state.write().rejected_current_sha256 =
                        Some(current.current_sha256().to_owned());
                    discard_unpublished_generation(candidate);
                    return Err(error).context("Wasm UDF runtime registry reload was rejected");
                },
            },
            #[cfg(test)]
            NonSourceReloadReadiness::UnverifiedTestOnly => None,
        };
        let latest = match load_runtime_registry_current(root) {
            Ok(latest) => latest,
            Err(error) => {
                drop(preloaded_graph_modules);
                discard_unpublished_generation(candidate);
                return Err(error)
                    .context("failed to confirm Wasm UDF runtime registry current pointer");
            },
        };
        if latest.current_sha256() != candidate.current_sha256 {
            drop(preloaded_graph_modules);
            discard_unpublished_generation(candidate);
            return Ok(None);
        }
        if let Some(preloaded_graph_modules) = preloaded_graph_modules
            && let Err(error) =
                candidate.publish_preloaded_load_verification(preloaded_graph_modules)
        {
            self.state.write().rejected_current_sha256 = Some(current.current_sha256().to_owned());
            discard_unpublished_generation(candidate);
            return Err(error).context("Wasm UDF runtime registry reload was rejected");
        }
        match self.activate(Arc::clone(&candidate)) {
            Ok(retired) => {
                if retired.is_none() {
                    log_registry_reload_event("accepted");
                }
                Ok(retired)
            },
            Err(error) => {
                self.state.write().rejected_current_sha256 =
                    Some(current.current_sha256().to_owned());
                discard_unpublished_generation(candidate);
                Err(error).context("Wasm UDF runtime registry reload was rejected")
            },
        }
    }

    fn preload_current_generation(
        &self,
        controller: &Arc<GeneratedMemoryController>,
        serialized_module_snapshot_directory: &Path,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.uses_source_keyed_selection(),
            "source-keyed runtime registry has no current-pointer generation to preload"
        );
        let Some(generation) = self.state.read().current.clone() else {
            return Ok(());
        };
        let preloaded_graph_modules = match preload_generation_routes(
            controller,
            serialized_module_snapshot_directory,
            &generation,
        ) {
            Ok(modules) => modules,
            Err(error) => {
                retire_source_keyed_generation(&generation);
                return Err(error);
            },
        };
        if let Some(root) = &self.root {
            let latest = match load_runtime_registry_current(root) {
                Ok(latest) => latest,
                Err(error) => {
                    drop(preloaded_graph_modules);
                    retire_source_keyed_generation(&generation);
                    return Err(error).context(
                        "failed to confirm initial Wasm UDF runtime registry current pointer",
                    );
                },
            };
            if latest.current_sha256() != generation.current_sha256 {
                drop(preloaded_graph_modules);
                retire_source_keyed_generation(&generation);
                anyhow::bail!(
                    "Wasm UDF runtime registry current pointer changed during initial preload"
                );
            }
        }
        generation.publish_preloaded_load_verification(preloaded_graph_modules)
    }

    pub(super) fn activate(
        &self,
        candidate: Arc<DeploymentGeneration>,
    ) -> anyhow::Result<Option<Arc<DeploymentGeneration>>> {
        ensure_registry_admission(
            self.primary_routing_enabled,
            self.shadow_routing_enabled,
            &candidate,
        )?;
        let mut state = self.state.write();
        anyhow::ensure!(
            !self.uses_source_keyed_selection(),
            "source-keyed runtime generations are staged, not independently activated"
        );
        anyhow::ensure!(
            state
                .current
                .as_ref()
                .is_none_or(|current| current.current_sha256 != candidate.current_sha256),
            "Wasm UDF runtime registry activated its current generation twice"
        );
        anyhow::ensure!(
            state
                .current
                .as_ref()
                .is_none_or(|current| current.generation_sha256 != candidate.generation_sha256),
            "Wasm UDF runtime registry activated the same generation twice"
        );
        let retired = state.current.replace(candidate);
        if let Some(retired) = &retired {
            retired.retired.store(true, Ordering::Release);
        }
        state.rejected_current_sha256 = None;
        drop(state);
        if let Some(retired) = &retired {
            retired.release_preloaded_graph_modules();
        }
        Ok(retired)
    }
}

enum NonSourceReloadReadiness<'a> {
    Preload {
        controller: &'a Arc<GeneratedMemoryController>,
        serialized_module_snapshot_directory: &'a Path,
    },
    #[cfg(test)]
    UnverifiedTestOnly,
}

fn ensure_registry_admission(
    primary_routing_enabled: bool,
    shadow_routing_enabled: bool,
    generation: &DeploymentGeneration,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !primary_routing_enabled || generation.admission.allows(RegistryUse::Primary),
        "normal Static Hermes Wasm routing requires a primary-admitted runtime registry generation"
    );
    anyhow::ensure!(
        !shadow_routing_enabled || generation.admission.allows(RegistryUse::Shadow),
        "Static Hermes shadow routing requires a runtime registry generation admitted for shadow \
         verification"
    );
    Ok(())
}

#[derive(Clone)]
pub(super) struct DeploymentWasmRoute {
    pub(super) generation: Arc<DeploymentGeneration>,
    pub(super) package_key: String,
    pub(super) entry_identity: DeploymentEntryIdentity,
    pub(super) route_lease: DeploymentRouteLease,
    pub(super) runtime_module_path: String,
    pub(super) export_name: String,
    pub(super) udf_kind: ManifestUdfKind,
    pub(super) deployed_runtime_identity: DeployedRuntimeIdentity,
}

#[derive(Clone)]
struct SourceKeyedDeploymentRoute {
    deployment_registry: Arc<DeploymentRegistry>,
    registry_use: RegistryUse,
    runtime_module_path: String,
    export_name: String,
    udf_kind: ManifestUdfKind,
}

#[derive(Clone)]
enum StaticHermesWasmtimeRoute {
    GeneratedSingleton,
    GeneratedDeployment(DeploymentWasmRoute),
    SourceKeyedDeployment(SourceKeyedDeploymentRoute),
}

#[derive(Clone)]
pub struct StaticHermesWasmtimeRouteHandle {
    route: StaticHermesWasmtimeRoute,
    udf_type: UdfType,
    // Route resolution captures policy exactly once. Retain that immutable
    // snapshot through source-keyed selection and execution so an admitted
    // invocation cannot change engines after a concurrent policy update.
    quarantine_snapshot: Arc<crate::StaticHermesWasmtimeQuarantineSnapshot>,
    // Deployment routes already retain their authenticated module and export
    // identities. Singleton routes have no equivalent registry identity, so
    // retain the full path only for them.
    singleton_udf_path: Option<String>,
    // This handle belongs to one invocation. Source selection may replace its
    // generation binding, but must not restart admission after a slow phase.
    admission_started: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticHermesWasmtimePreparationMode {
    Primary,
    Shadow,
}

impl StaticHermesWasmtimePreparationMode {
    const fn is_shadow(self) -> bool {
        matches!(self, Self::Shadow)
    }
}

// Pin whether preparation selected the configured module or the test-hook
// module. Selector authorization and execution must use this same value.
enum PreparedRoutedModule {
    Deferred,
    Configured(Arc<GeneratedRoutedModule>),
    TestHook(Arc<GeneratedRoutedModule>),
}

/// An authenticated Wasm invocation together with the transaction that
/// supplied its source identity.
///
/// Keeping the transaction inside this opaque value prevents route
/// authentication from being reused with a different invocation transaction.
pub struct PreparedStaticHermesWasmtimeInvocation<RT: Runtime> {
    route: StaticHermesWasmtimeRouteHandle,
    path_and_args: ValidatedPathAndArgs,
    transaction: Transaction<RT>,
    routed: PreparedRoutedModule,
    host_secret_selectors: BTreeSet<String>,
    memory_observer: Option<udf::wasm_memory::WasmMemoryObserver>,
}

impl<RT: Runtime> PreparedStaticHermesWasmtimeInvocation<RT> {
    pub fn observe_memory(&mut self, observer: udf::wasm_memory::WasmMemoryObserver) {
        assert!(
            self.memory_observer.replace(observer).is_none(),
            "memory observer attached twice"
        );
    }

    /// Load only the configured host-secret values authorized by the prepared
    /// module. The authenticated transaction remains opaque so callers cannot
    /// replace it after source verification.
    pub async fn load_authorized_host_secret_values(
        &mut self,
    ) -> anyhow::Result<BTreeMap<String, HostSecretValue>> {
        let deadline = self.route.admission_deadline()?;
        within_static_hermes_admission(deadline, async {
            // Build the Store-owned representation here so the runner can move it
            // into the invocation without an intermediate environment-value map.
            let mut values = BTreeMap::new();
            let mut model = EnvironmentVariablesModel::new(&mut self.transaction);
            for selector in &self.host_secret_selectors {
                let name = selector.parse::<EnvVarName>()?;
                if let Some(environment_variable) = model.get(&name).await? {
                    let value = String::from(environment_variable.into_value().into_value());
                    // Host-secret contract v1 treats an empty deployment value as
                    // missing configuration rather than as a valid zero-length secret.
                    if value.is_empty() {
                        continue;
                    }
                    assert!(
                        values
                            .insert(selector.clone(), HostSecretValue::new(value.into_bytes()))
                            .is_none(),
                        "prepared host-secret selector was loaded twice"
                    );
                }
            }
            Ok(values)
        })
        .await
    }
}

/// Only expected admission saturation carries this marker. Callers may defer
/// degradable queries without concealing a runtime or artifact failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StaticHermesWasmAdmissionFailure {
    #[error("Static Hermes Wasm pending admission capacity is full")]
    Backlog,
    #[error("Static Hermes Wasm admission was shed after sustained queue delay")]
    QueueDelay,
    #[error("Static Hermes Wasm setup exceeded its admission deadline")]
    Deadline,
    #[error("Static Hermes Wasm module loading capacity is unavailable")]
    Module,
    #[error("generated Wasm memory admission timed out while capacity was unavailable")]
    Memory,
    #[error("generated Wasm active CPU admission timed out while capacity was unavailable")]
    Cpu,
}

impl StaticHermesWasmAdmissionFailure {
    fn overloaded(self) -> anyhow::Error {
        let metadata = ErrorMetadata::overloaded("WasmAdmissionUnavailable", self.to_string());
        anyhow::Error::new(self).context(metadata)
    }
}

/// Bound setup phases with the original invocation deadline. The timer is
/// checked first so an immediately-ready phase cannot admit expired work.
pub async fn within_static_hermes_admission<T>(
    deadline: tokio::time::Instant,
    work: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    if tokio::time::Instant::now() >= deadline {
        return Err(StaticHermesWasmAdmissionFailure::Deadline.overloaded());
    }
    tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => {
            Err(StaticHermesWasmAdmissionFailure::Deadline.overloaded())
        },
        result = work => match result {
            Ok(_) if tokio::time::Instant::now() >= deadline => {
                Err(StaticHermesWasmAdmissionFailure::Deadline.overloaded())
            },
            result => result,
        },
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Static Hermes query shadow capacity is unavailable")]
pub struct StaticHermesQueryShadowCapacityUnavailable;

#[derive(Debug, thiserror::Error)]
#[error("Static Hermes Wasm module loading failed")]
pub struct StaticHermesWasmModuleLoadingFailure;

pub(super) fn classify_generated_module_load_error(
    error: anyhow::Error,
    shadow: bool,
) -> anyhow::Error {
    let admission = error.downcast_ref::<ModuleMemoryAdmissionError>().copied();
    #[cfg(any(test, feature = "testing"))]
    if admission.is_some() {
        record_generated_route_preflight_stage(
            StaticHermesGeneratedRoutePreflightStage::RoutedModuleCapacityRejected,
        );
    }
    if admission.is_some_and(|admission| {
        matches!(
            admission,
            ModuleMemoryAdmissionError::Pressure
                | ModuleMemoryAdmissionError::SoftBudget
                | ModuleMemoryAdmissionError::HardBudget
        )
    }) {
        if shadow {
            StaticHermesQueryShadowCapacityUnavailable.into()
        } else {
            error
                .context(StaticHermesWasmAdmissionFailure::Module)
                .context(ErrorMetadata::overloaded(
                    "WasmAdmissionUnavailable",
                    "Wasm module loading capacity is unavailable",
                ))
        }
    } else {
        error
    }
}

/// Opaque route selection from the authenticated active deployment registry.
///
/// This type intentionally exposes only SHA-256 digests. It does not retain
/// deployment paths, exports, packages, identities, or other registry data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticHermesQueryShadowRegistry {
    selection: Option<Arc<QueryShadowRouteSelection>>,
    authenticated_selections: Vec<Arc<QueryShadowRouteSelection>>,
}

impl StaticHermesQueryShadowRegistry {
    pub fn generation_sha256(&self) -> &str {
        self.selection
            .as_ref()
            .map_or("", |selection| selection.generation_sha256.as_str())
    }

    /// Return sorted authenticated route SHA-256 digests for one shadow UDF
    /// kind.
    pub fn route_sha256s(&self, udf_type: UdfType) -> &[String] {
        self.selection
            .as_ref()
            .map_or(&[], |selection| selection.route_sha256s(udf_type))
    }

    /// Return whether one immutable retained source-generation selection
    /// authenticates a shadow route. Both digests and its UDF kind must match
    /// the same selection; retaining older selections admits factual late
    /// reports without making them active for route-detail capacity.
    pub fn authenticates_route(
        &self,
        udf_type: UdfType,
        generation_sha256: &str,
        route_sha256: &str,
    ) -> bool {
        self.authenticated_selections.iter().any(|selection| {
            selection.authenticates_route(udf_type, generation_sha256, route_sha256)
        })
    }

    #[cfg(test)]
    pub(super) fn shares_selection_with(&self, other: &Self) -> bool {
        match (&self.selection, &other.selection) {
            (Some(left), Some(right)) if !Arc::ptr_eq(left, right) => return false,
            (None, None) | (Some(_), Some(_)) => {},
            _ => return false,
        }
        self.authenticated_selections.len() == other.authenticated_selections.len()
            && self
                .authenticated_selections
                .iter()
                .zip(&other.authenticated_selections)
                .all(|(left, right)| Arc::ptr_eq(left, right))
    }
}

impl StaticHermesWasmtimeRouteHandle {
    fn admit(
        route: StaticHermesWasmtimeRoute,
        udf_type: UdfType,
        path_and_args: &ValidatedPathAndArgs,
        quarantine_snapshot: Arc<crate::StaticHermesWasmtimeQuarantineSnapshot>,
    ) -> Option<Self> {
        let generation_sha256 = match &route {
            StaticHermesWasmtimeRoute::GeneratedSingleton
            | StaticHermesWasmtimeRoute::SourceKeyedDeployment(_) => None,
            StaticHermesWasmtimeRoute::GeneratedDeployment(route) => {
                Some(route.generation.generation_sha256.as_str())
            },
        };
        if !quarantine_admits_wasm(
            &quarantine_snapshot,
            path_and_args.path().udf_path.module().as_str(),
            path_and_args.path().udf_path.function_name(),
            generation_sha256,
        ) {
            return None;
        }
        Some(Self {
            singleton_udf_path: matches!(&route, StaticHermesWasmtimeRoute::GeneratedSingleton)
                .then(|| path_and_args.path().udf_path.to_string()),
            route,
            udf_type,
            quarantine_snapshot,
            admission_started: tokio::time::Instant::now(),
        })
    }

    pub fn admission_deadline(&self) -> anyhow::Result<tokio::time::Instant> {
        self.admission_started
            .checked_add(
                route_configuration()?
                    .generated_memory_admission_wait
                    .min(*common::knobs::ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS),
            )
            .context("Wasm admission deadline exceeds the monotonic clock range")
    }

    /// Return the opaque route ID authenticated by the deployment registry.
    /// Custom/test singleton routes have no authenticated route ID and must not
    /// be used as per-route telemetry keys.
    pub fn authenticated_route_id(&self) -> Option<&str> {
        match &self.route {
            StaticHermesWasmtimeRoute::GeneratedSingleton => None,
            StaticHermesWasmtimeRoute::GeneratedDeployment(route) => route.route_lease.route_id(),
            StaticHermesWasmtimeRoute::SourceKeyedDeployment(_) => None,
        }
    }

    /// Return the registry generation digest that authenticated this route.
    pub fn authenticated_generation_sha256(&self) -> Option<&str> {
        match &self.route {
            StaticHermesWasmtimeRoute::GeneratedSingleton => None,
            StaticHermesWasmtimeRoute::GeneratedDeployment(route) => {
                Some(&route.generation.generation_sha256)
            },
            StaticHermesWasmtimeRoute::SourceKeyedDeployment(_) => None,
        }
    }

    fn validate_invocation(
        &self,
        udf_type: UdfType,
        path_and_args: &ValidatedPathAndArgs,
    ) -> anyhow::Result<()> {
        let path = path_and_args.path();
        anyhow::ensure!(
            path.component == ComponentId::Root
                && path.component_path.is_root()
                && self.udf_type == udf_type,
            "Static Hermes gate route handle does not match its invocation"
        );
        let route_matches = match &self.route {
            StaticHermesWasmtimeRoute::GeneratedSingleton => self
                .singleton_udf_path
                .as_deref()
                .is_some_and(|udf_path| udf_path == path.udf_path.to_string()),
            StaticHermesWasmtimeRoute::GeneratedDeployment(route) => {
                route.runtime_module_path == path.udf_path.module().as_str()
                    && route.export_name.as_str() == &**path.udf_path.function_name()
            },
            StaticHermesWasmtimeRoute::SourceKeyedDeployment(route) => {
                route.runtime_module_path == path.udf_path.module().as_str()
                    && route.export_name.as_str() == &**path.udf_path.function_name()
            },
        };
        anyhow::ensure!(
            route_matches,
            "Static Hermes gate route handle does not match its invocation"
        );
        Ok(())
    }
}

static ROUTE_CONFIGURATION: OnceLock<RouteConfiguration> = OnceLock::new();
static ROUTE_CONFIGURATION_INITIALIZATION: LazyLock<parking_lot::Mutex<()>> =
    LazyLock::new(|| parking_lot::Mutex::new(()));

fn optional_boolean_environment(name: &str) -> anyhow::Result<Option<bool>> {
    optional_boolean_environment_value(name, std::env::var(name))
}

fn optional_boolean_environment_value(
    name: &str,
    value: Result<String, std::env::VarError>,
) -> anyhow::Result<Option<bool>> {
    match value {
        Ok(value) if value == "0" => Ok(Some(false)),
        Ok(value) if value == "1" => Ok(Some(true)),
        Ok(_) => anyhow::bail!("{name} must be 0 or 1"),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn configured_reuse_instances(
    generated_runtime_configured: bool,
    requested_reuse_instances: Option<bool>,
) -> anyhow::Result<bool> {
    let reuse_instances = requested_reuse_instances.unwrap_or(generated_runtime_configured);
    anyhow::ensure!(
        !reuse_instances || generated_runtime_configured,
        "{REUSE_INSTANCES_ENV}=1 requires a configured generated package or deployment registry"
    );
    Ok(reuse_instances)
}

fn usize_environment(name: &str, default: usize) -> anyhow::Result<usize> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("failed to read {name}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn required_usize_environment(name: &str) -> anyhow::Result<usize> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("failed to read {name}")),
        Err(std::env::VarError::NotPresent) => {
            anyhow::bail!("{name} must be set when Static Hermes Wasm routing is configured")
        },
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn load_route_configuration() -> anyhow::Result<RouteConfiguration> {
    let custom_path = match std::env::var(ROUTED_UDF_PATH_ENV) {
        Ok(path) => Some(path),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {ROUTED_UDF_PATH_ENV}"));
        },
    };
    let package_directory = std::env::var_os(PACKAGE_DIRECTORY_ENV).map(PathBuf::from);
    let serialized_module_snapshot_directory =
        std::env::var_os(SERIALIZED_MODULE_SNAPSHOT_DIRECTORY_ENV).map(PathBuf::from);
    let deployment_manifest = std::env::var_os(DEPLOYMENT_MANIFEST_ENV).map(PathBuf::from);
    let artifact_cache_root = std::env::var_os(ARTIFACT_CACHE_ROOT_ENV).map(PathBuf::from);
    let runtime_registry_root = std::env::var_os(RUNTIME_REGISTRY_ROOT_ENV).map(PathBuf::from);
    let lifecycle_barrier_directory =
        std::env::var_os(LIFECYCLE_BARRIER_DIRECTORY_ENV).map(PathBuf::from);
    let lifecycle_barrier_udf_path = match std::env::var(LIFECYCLE_BARRIER_UDF_PATH_ENV) {
        Ok(path) => Some(path),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read {LIFECYCLE_BARRIER_UDF_PATH_ENV}"));
        },
    };
    // Catalog residency admits both publication kinds. Primary authority is
    // checked on the selected generation for each invocation, not frozen at
    // process startup; a live mode change cannot promote shadow-only artifacts.
    let primary_routing_enabled = false;
    let shadow_enabled = runtime_registry_root.is_some() || deployment_manifest.is_some();
    let source_keyed_deployment =
        optional_boolean_environment(SOURCE_KEYED_DEPLOYMENT_ENV)?.unwrap_or(false);
    let has_runtime_registry_root = runtime_registry_root.is_some();
    let deployment_registry = match (
        runtime_registry_root,
        deployment_manifest,
        artifact_cache_root,
    ) {
        (Some(root), None, None) => Some(DeploymentRegistry::load_with_selection(
            root,
            primary_routing_enabled,
            shadow_enabled,
            source_keyed_deployment,
        )?),
        (None, Some(manifest_path), Some(artifact_cache_root)) => {
            let registry = ValidatedDeploymentManifest::load(&manifest_path)
                .context("failed to load Wasm UDF deployment registry")?;
            Some(DeploymentRegistry::from_legacy_with_routing(
                artifact_cache_root,
                registry,
                primary_routing_enabled,
                shadow_enabled,
            )?)
        },
        (None, None, None) => None,
        (None, Some(_), None) | (None, None, Some(_)) => {
            anyhow::bail!(
                "{DEPLOYMENT_MANIFEST_ENV} and {ARTIFACT_CACHE_ROOT_ENV} must be set together"
            )
        },
        (Some(_), ..) => {
            anyhow::bail!(
                "{RUNTIME_REGISTRY_ROOT_ENV} is incompatible with {DEPLOYMENT_MANIFEST_ENV} and \
                 {ARTIFACT_CACHE_ROOT_ENV}"
            )
        },
    };
    anyhow::ensure!(
        !source_keyed_deployment || has_runtime_registry_root,
        "{SOURCE_KEYED_DEPLOYMENT_ENV}=1 requires {RUNTIME_REGISTRY_ROOT_ENV}"
    );
    if let Some(path) = &custom_path {
        anyhow::ensure!(
            !path.is_empty(),
            "{ROUTED_UDF_PATH_ENV} must not be empty when set"
        );
    }
    let host_secret_selectors = match std::env::var(HOST_SECRET_SELECTORS_ENV) {
        Ok(value) => {
            let mut selectors = BTreeSet::new();
            for selector in value.split(',') {
                let selector = selector.parse::<EnvVarName>().with_context(|| {
                    format!("{HOST_SECRET_SELECTORS_ENV} contains an invalid selector")
                })?;
                anyhow::ensure!(
                    selectors.insert(selector.to_string()),
                    "{HOST_SECRET_SELECTORS_ENV} must contain unique comma-separated opaque \
                     selectors"
                );
            }
            selectors
        },
        Err(std::env::VarError::NotPresent) => BTreeSet::new(),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read {HOST_SECRET_SELECTORS_ENV}"));
        },
    };
    let reuse_instances = configured_reuse_instances(
        custom_path.is_some() || deployment_registry.is_some(),
        optional_boolean_environment(REUSE_INSTANCES_ENV)?,
    )?;
    anyhow::ensure!(
        package_directory.is_some() == custom_path.is_some(),
        "{PACKAGE_DIRECTORY_ENV} and {ROUTED_UDF_PATH_ENV} must be set together"
    );
    anyhow::ensure!(
        deployment_registry.is_none() || custom_path.is_none(),
        "a Wasm deployment registry is incompatible with {ROUTED_UDF_PATH_ENV}"
    );
    anyhow::ensure!(
        deployment_registry.is_none() || package_directory.is_none(),
        "a Wasm deployment registry is incompatible with {PACKAGE_DIRECTORY_ENV}"
    );
    let serialized_module_snapshot_directory = match (
        deployment_registry.is_some() || package_directory.is_some(),
        serialized_module_snapshot_directory,
    ) {
        (false, None) => None,
        (false, Some(_)) => {
            anyhow::bail!(
                "{SERIALIZED_MODULE_SNAPSHOT_DIRECTORY_ENV} requires a configured generated \
                 package or deployment registry"
            )
        },
        (true, None) => {
            anyhow::bail!(
                "{SERIALIZED_MODULE_SNAPSHOT_DIRECTORY_ENV} must identify a private executable \
                 snapshot directory when a generated package or deployment registry is configured"
            )
        },
        (true, Some(directory)) => {
            anyhow::ensure!(
                directory.is_absolute(),
                "{SERIALIZED_MODULE_SNAPSHOT_DIRECTORY_ENV} must be an absolute path"
            );
            let canonical = directory.canonicalize().with_context(|| {
                format!("failed to resolve {SERIALIZED_MODULE_SNAPSHOT_DIRECTORY_ENV}")
            })?;
            anyhow::ensure!(
                canonical == directory,
                "{SERIALIZED_MODULE_SNAPSHOT_DIRECTORY_ENV} must be a canonical path"
            );
            validate_serialized_module_snapshot_directory(&directory).with_context(|| {
                format!(
                    "{SERIALIZED_MODULE_SNAPSHOT_DIRECTORY_ENV} must be an owner-private \
                     mode-0700 writable directory"
                )
            })?;
            Some(directory)
        },
    };
    let lifecycle_barrier = match (lifecycle_barrier_directory, lifecycle_barrier_udf_path) {
        (None, None) => None,
        (Some(directory), Some(udf_path)) => {
            anyhow::ensure!(
                (primary_routing_enabled || shadow_enabled)
                    && deployment_registry
                        .as_ref()
                        .is_some_and(|registry| registry.root.is_some()),
                "{LIFECYCLE_BARRIER_DIRECTORY_ENV} requires a runtime registry with primary or \
                 shadow execution enabled"
            );
            Some(GeneratedLifecycleBarrier::load(directory, udf_path)?)
        },
        _ => {
            anyhow::bail!(
                "{LIFECYCLE_BARRIER_DIRECTORY_ENV} and {LIFECYCLE_BARRIER_UDF_PATH_ENV} must be \
                 set together"
            )
        },
    };
    let generated_memory_policy = GeneratedMemoryPolicy {
        hard_instance_ceiling: usize_environment(GENERATED_INSTANCE_HARD_CEILING_ENV, 200)?,
        soft_budget_bytes: usize_environment(
            GENERATED_MEMORY_SOFT_BUDGET_ENV,
            4 * 1024 * 1024 * 1024,
        )?,
        hard_budget_bytes: usize_environment(
            GENERATED_MEMORY_HARD_BUDGET_ENV,
            6 * 1024 * 1024 * 1024,
        )?,
        safety_reserve_bytes: usize_environment(
            GENERATED_MEMORY_SAFETY_RESERVE_ENV,
            512 * 1024 * 1024,
        )?,
        unattributed_bytes_per_slot: usize_environment(
            GENERATED_MEMORY_UNATTRIBUTED_PER_SLOT_ENV,
            2 * 1024 * 1024,
        )?,
        cold_peak_growth_bytes: usize_environment(
            GENERATED_MEMORY_COLD_FORECAST_ENV,
            64 * 1024 * 1024,
        )?,
        warm_idle_target: usize_environment(GENERATED_MEMORY_WARM_IDLE_TARGET_ENV, 8)?,
        maximum_idle_age: Duration::from_secs(u64::try_from(usize_environment(
            GENERATED_MEMORY_MAX_IDLE_SECONDS_ENV,
            5 * 60,
        )?)?),
        pressure_enter_headroom_bytes: *LOCAL_BACKEND_MEMORY_PRESSURE_ENTER_HEADROOM_BYTES,
        pressure_exit_headroom_bytes: *LOCAL_BACKEND_MEMORY_PRESSURE_EXIT_HEADROOM_BYTES,
    };
    anyhow::ensure!(
        (1..=200).contains(&generated_memory_policy.hard_instance_ceiling),
        "{GENERATED_INSTANCE_HARD_CEILING_ENV} must be between 1 and 200"
    );
    let mut resource_class_minimums = (0, 0);
    let active_wasm_cpu_limiter = if deployment_registry.is_some() || package_directory.is_some() {
        let active_wasm_cpu_concurrency =
            required_usize_environment(ACTIVE_WASM_CPU_CONCURRENCY_ENV)?;
        anyhow::ensure!(
            (1..=generated_memory_policy.hard_instance_ceiling)
                .contains(&active_wasm_cpu_concurrency),
            "{ACTIVE_WASM_CPU_CONCURRENCY_ENV} must be between 1 and \
             {GENERATED_INSTANCE_HARD_CEILING_ENV}"
        );
        let protected_minimum = usize_environment(ACTIVE_WASM_PROTECTED_MINIMUM_ENV, 0)?;
        let degradable_minimum = usize_environment(ACTIVE_WASM_DEGRADABLE_MINIMUM_ENV, 0)?;
        anyhow::ensure!(
            (protected_minimum == 0) == (degradable_minimum == 0),
            "{ACTIVE_WASM_PROTECTED_MINIMUM_ENV} and {ACTIVE_WASM_DEGRADABLE_MINIMUM_ENV} must be \
             enabled together"
        );
        anyhow::ensure!(
            protected_minimum
                .checked_add(degradable_minimum)
                .is_some_and(|sum| sum <= active_wasm_cpu_concurrency),
            "Wasm service floors exceed {ACTIVE_WASM_CPU_CONCURRENCY_ENV}"
        );
        resource_class_minimums = (protected_minimum, degradable_minimum);
        log_active_wasm_cpu_capacity(active_wasm_cpu_concurrency);
        Some(ConcurrencyLimiter::new_for_wasm(
            active_wasm_cpu_concurrency,
            protected_minimum,
            degradable_minimum,
        ))
    } else {
        None
    };
    let generated_memory_admission_wait = Duration::from_millis(u64::try_from(usize_environment(
        GENERATED_MEMORY_ADMISSION_WAIT_MILLISECONDS_ENV,
        30_000,
    )?)?);
    anyhow::ensure!(
        !generated_memory_admission_wait.is_zero(),
        "{GENERATED_MEMORY_ADMISSION_WAIT_MILLISECONDS_ENV} must be greater than zero"
    );
    let generated_memory_controller = GeneratedMemoryController::new(generated_memory_policy)?;
    generated_memory_controller
        .routed_admission
        .configure_classes(resource_class_minimums.0, resource_class_minimums.1);
    if source_keyed_deployment {
        deployment_registry
            .as_ref()
            .context("source-keyed runtime registry disappeared during configuration")?
            .configure_source_keyed_residency(
                &generated_memory_controller,
                serialized_module_snapshot_directory
                    .as_deref()
                    .context("source-keyed runtime registry has no snapshot directory")?,
            )
            .context("failed to configure Wasm UDF runtime source catalog residency")?;
    } else if let Some(deployment_registry) = deployment_registry.as_ref() {
        let serialized_module_snapshot_directory = serialized_module_snapshot_directory
            .as_deref()
            .context("deployment runtime registry has no serialized-module snapshot directory")?;
        generated_memory_controller.refresh_pressure_from_cgroup();
        deployment_registry
            .preload_current_generation(
                &generated_memory_controller,
                serialized_module_snapshot_directory,
            )
            .context("failed to preload the current Wasm UDF runtime generation")?;
    }
    Ok(RouteConfiguration {
        active_wasm_cpu_limiter,
        custom_path,
        deployment_registry,
        generated_memory_admission_wait,
        generated_memory_controller,
        host_secret_selectors,
        lifecycle_barrier,
        package_directory,
        reuse_instances,
        serialized_module_snapshot_directory,
    })
}

pub(super) fn route_configuration() -> anyhow::Result<&'static RouteConfiguration> {
    if let Some(configuration) = ROUTE_CONFIGURATION.get() {
        return Ok(configuration);
    }
    // Configuration loading may authenticate, compile, and cache a complete
    // generation. Serialize first publication so concurrent client startup
    // cannot build competing generation incarnations in the global caches.
    let _initialization = ROUTE_CONFIGURATION_INITIALIZATION.lock();
    if let Some(configuration) = ROUTE_CONFIGURATION.get() {
        return Ok(configuration);
    }
    let configuration = load_route_configuration()?;
    ROUTE_CONFIGURATION
        .set(configuration)
        .map_err(|_| anyhow::anyhow!("Wasm UDF route configuration was published twice"))?;
    ROUTE_CONFIGURATION
        .get()
        .context("Wasm UDF route configuration initialization failed")
}

pub(crate) fn initialize_route_configuration() -> anyhow::Result<()> {
    route_configuration().map(|_| ())
}

pub use super::super::generated_wasm_memory::PreparationCapacity as SourceKeyedRuntimeCapacity;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceKeyedRuntimeReadiness {
    Ready,
    CapacityUnavailable(SourceKeyedRuntimeCapacity),
    NotStaged,
}

#[derive(Debug, thiserror::Error)]
#[error("Static Hermes Wasm preparation capacity is unavailable")]
struct SourceKeyedRuntimeCapacityUnavailable;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceKeyedRuntimeGenerationIdentity {
    pub deployment_sha256: String,
    pub generation_manifest_sha256: String,
    pub generation_sha256: String,
}

fn source_keyed_generation_identity(
    generation: &DeploymentGeneration,
) -> SourceKeyedRuntimeGenerationIdentity {
    SourceKeyedRuntimeGenerationIdentity {
        deployment_sha256: generation.deployment_sha256().to_owned(),
        generation_manifest_sha256: generation.generation_manifest_sha256.clone(),
        generation_sha256: generation.generation_sha256.clone(),
    }
}

/// Ensure the exact authenticated source/generation tuple is resident and
/// route-preloaded. An absent descriptor is the only `NotStaged` result;
/// physical generation or route-load failures are operational errors.
pub async fn prepare_source_keyed_runtime<RT: Runtime>(
    rt: RT,
    database: database::Database<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    runtime_content_sha256: &str,
    generation: &SourceKeyedRuntimeGenerationIdentity,
) -> anyhow::Result<SourceKeyedRuntimePreparation> {
    let deadline = tokio::time::Instant::now()
        .checked_add(route_configuration()?.generated_memory_admission_wait)
        .context("Wasm readiness deadline exceeds the monotonic clock range")?;
    prepare_source_keyed_runtime_inner(
        rt,
        database,
        default_system_env_vars,
        runtime_content_sha256,
        generation,
        runtime_policy_store().snapshot(),
        deadline,
    )
    .await
}

pub async fn prepare_source_keyed_runtime_for_policy<RT: Runtime>(
    rt: RT,
    database: database::Database<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    runtime_content_sha256: &str,
    generation: &SourceKeyedRuntimeGenerationIdentity,
    policy: RuntimePolicySnapshot,
) -> anyhow::Result<Option<SourceKeyedRuntimePreparation>> {
    let configuration = route_configuration()?;
    let Some(registry) = configuration.deployment_registry.as_ref() else {
        return Ok(Some(SourceKeyedRuntimePreparation::NotStaged));
    };
    let Some(residency) = registry.source_keyed_residency.as_ref() else {
        return Ok(Some(SourceKeyedRuntimePreparation::NotStaged));
    };
    let selector = SourceKeyedGenerationSelector::requested(runtime_content_sha256, generation);
    let descriptor = registry
        .state
        .read()
        .source_keyed_catalog
        .as_ref()
        .and_then(|catalog| catalog.descriptor_by_selector(&selector));
    let Some(descriptor) = descriptor else {
        return Ok(Some(SourceKeyedRuntimePreparation::NotStaged));
    };
    let destination = policy.policy;
    let deadline = tokio::time::Instant::now()
        .checked_add(configuration.generated_memory_admission_wait)
        .context("Wasm readiness deadline exceeds the monotonic clock range")?;
    let authentication = residency.authenticated_manifest(registry, descriptor);
    let manifest = tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => {
            return Ok(Some(SourceKeyedRuntimePreparation::CapacityUnavailable {
                capacity: configuration
                    .generated_memory_controller
                    .preparation_capacity("loading"),
                population: Vec::new(),
            }));
        },
        result = authentication => result?,
    };
    // Quarantine controls execution, not preparation. Clearing it must not
    // expose a newly primary kind whose population was skipped at promotion.
    let eligible = manifest.admission().allows(RegistryUse::Primary)
        && manifest
            .registry
            .selected_wasm_exports()
            .iter()
            .any(|entry| {
                let primary = match entry.udf_kind {
                    ManifestUdfKind::Query => destination.query.primary,
                    ManifestUdfKind::Mutation => destination.mutation.primary,
                };
                primary == common::wasm_runtime_policy::PrimaryRuntime::Wasm
            });
    if !eligible {
        return Ok(None);
    }
    prepare_source_keyed_runtime_inner(
        rt,
        database,
        default_system_env_vars,
        runtime_content_sha256,
        generation,
        policy,
        deadline,
    )
    .await
    .map(Some)
}

async fn prepare_source_keyed_runtime_inner<RT: Runtime>(
    rt: RT,
    database: database::Database<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    runtime_content_sha256: &str,
    generation: &SourceKeyedRuntimeGenerationIdentity,
    policy: RuntimePolicySnapshot,
    deadline: tokio::time::Instant,
) -> anyhow::Result<SourceKeyedRuntimePreparation> {
    let configuration = route_configuration()?;
    let Some(registry) = configuration.deployment_registry.as_ref() else {
        return Ok(SourceKeyedRuntimePreparation::NotStaged);
    };
    if !registry.uses_source_keyed_selection() {
        return Ok(SourceKeyedRuntimePreparation::NotStaged);
    }
    let selector = SourceKeyedGenerationSelector::requested(runtime_content_sha256, generation);
    let residency = registry
        .source_keyed_residency
        .as_ref()
        .context("source-keyed runtime registry has no residency manager")?;
    let loading = async {
        let mut transaction = database.begin_system().await?;
        let selected_selector =
            match SourcePackageModel::new(&mut transaction, TableNamespace::Global)
                .get_latest_record()
                .await?
            {
                Some(source) => match (
                    source.runtime_content_sha256.as_ref(),
                    source.runtime_generation.as_ref(),
                ) {
                    (Some(content), Some(generation)) => {
                        Some(SourceKeyedGenerationSelector::requested(
                            &content.as_hex(),
                            &SourceKeyedRuntimeGenerationIdentity {
                                deployment_sha256: generation.deployment_sha256.as_hex(),
                                generation_manifest_sha256: generation
                                    .generation_manifest_sha256
                                    .as_hex(),
                                generation_sha256: generation.generation_sha256.as_hex(),
                            },
                        ))
                    },
                    _ => None,
                },
                None => None,
            };
        residency.observe_selected(selected_selector, *transaction.begin_timestamp())?;
        drop(transaction);
        registry
            .ensure_source_keyed_generation(runtime_content_sha256, generation, None, deadline)
            .await
    };
    let generation = match tokio::time::timeout_at(deadline, loading).await {
        Ok(Ok(Some(generation))) => generation,
        Ok(Ok(None)) => return Ok(SourceKeyedRuntimePreparation::NotStaged),
        Ok(Err(error)) if !error.is::<SourceKeyedRuntimeCapacityUnavailable>() => {
            return Err(error)
        },
        Ok(Err(_)) | Err(_) => {
            return Ok(SourceKeyedRuntimePreparation::CapacityUnavailable {
                capacity: configuration
                    .generated_memory_controller
                    .preparation_capacity("loading"),
                population: Vec::new(),
            })
        },
    };
    // Keep ownership of guest teardown if the HTTP caller disappears. The
    // lifecycle itself bounds initialization and admission; dropping the waiter
    // must not drop a live guest halfway through those operations.
    let mode = if generation.admission.allows(RegistryUse::Primary) {
        preparation::PreparationMode::for_policy(policy.policy, &generation)
    } else {
        preparation::PreparationMode::Activation
    };
    let deadline = match mode {
        preparation::PreparationMode::PrimaryActivation { .. } => {
            deadline + preparation::EXTRA_POPULATION_WINDOW
        },
        preparation::PreparationMode::Activation | preparation::PreparationMode::Maintenance => {
            deadline
        },
    };
    let prepared = preparation::prepare_generation(
        rt,
        database,
        default_system_env_vars,
        generation,
        mode,
        deadline,
    )
    .await?;
    match prepared {
        preparation::GenerationPreparation::Prepared(inventory) => Ok(
            SourceKeyedRuntimePreparation::Prepared(PreparedSourceKeyedRuntime {
                registry: Arc::clone(registry),
                residency: Arc::clone(residency),
                selector,
                inventory,
                policy_revision: policy.revision,
            }),
        ),
        preparation::GenerationPreparation::CapacityUnavailable { reason, population } => {
            Ok(SourceKeyedRuntimePreparation::CapacityUnavailable {
                capacity: configuration
                    .generated_memory_controller
                    .preparation_capacity(reason),
                population,
            })
        },
    }
}

pub async fn source_keyed_runtime_readiness<RT: Runtime>(
    rt: RT,
    database: database::Database<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    runtime_content_sha256: &str,
    generation: &SourceKeyedRuntimeGenerationIdentity,
) -> anyhow::Result<SourceKeyedRuntimeReadiness> {
    Ok(
        match prepare_source_keyed_runtime(
            rt,
            database,
            default_system_env_vars,
            runtime_content_sha256,
            generation,
        )
        .await?
        {
            SourceKeyedRuntimePreparation::Prepared(_) => SourceKeyedRuntimeReadiness::Ready,
            SourceKeyedRuntimePreparation::CapacityUnavailable { capacity, .. } => {
                SourceKeyedRuntimeReadiness::CapacityUnavailable(capacity)
            },
            SourceKeyedRuntimePreparation::NotStaged => SourceKeyedRuntimeReadiness::NotStaged,
        },
    )
}

pub async fn source_keyed_runtime_generation_identity(
    runtime_content_sha256: &str,
    generation: &SourceKeyedRuntimeGenerationIdentity,
) -> anyhow::Result<Option<SourceKeyedRuntimeGenerationIdentity>> {
    let configuration = route_configuration()?;
    let Some(registry) = configuration.deployment_registry.as_ref() else {
        return Ok(None);
    };
    if !registry.uses_source_keyed_selection() {
        return Ok(None);
    }
    let deadline = tokio::time::Instant::now()
        .checked_add(configuration.generated_memory_admission_wait)
        .context("Wasm generation load deadline exceeds the monotonic clock range")?;
    Ok(registry
        .ensure_source_keyed_generation(runtime_content_sha256, generation, None, deadline)
        .await?
        .map(|generation| source_keyed_generation_identity(&generation)))
}

pub enum SourceKeyedRuntimePreparation {
    Prepared(PreparedSourceKeyedRuntime),
    CapacityUnavailable {
        capacity: SourceKeyedRuntimeCapacity,
        population: Vec<SourceKeyedRuntimeGroupPopulation>,
    },
    NotStaged,
}

pub struct PreparedSourceKeyedRuntime {
    registry: Arc<DeploymentRegistry>,
    residency: Arc<SourceKeyedResidency>,
    selector: SourceKeyedGenerationSelector,
    inventory: Arc<preparation::PreparedInventory>,
    policy_revision: u64,
}

/// Owns the exact initialized Stores while a publication transaction is in
/// flight. Their backing and shared-readiness forecasts remain accounted. The
/// commit task, not its HTTP waiter, must retain this value until it knows the
/// database outcome.
pub struct SourceKeyedRuntimeActivationReservation<RT: Runtime> {
    residency: Arc<SourceKeyedResidency>,
    selector: SourceKeyedGenerationSelector,
    pin: Arc<SourceKeyedActivationPin>,
    instances: parking_lot::Mutex<Vec<GeneratedReusableInstance<RT>>>,
    population_owners: Vec<preparation::ReservedPopulation>,
    read_sets: Vec<ContextReadSet>,
    population: Vec<SourceKeyedRuntimeGroupPopulation>,
}

#[derive(Clone, Debug)]
pub struct SourceKeyedRuntimeGroupPopulation {
    pub group: String,
    pub target: usize,
    pub prepared: usize,
    pub active: usize,
    pub in_progress: usize,
    pub shortfall: Option<&'static str>,
}

impl preparation::PreparedInventory {
    pub(super) fn population(&self) -> Vec<SourceKeyedRuntimeGroupPopulation> {
        use sha2::{
            Digest,
            Sha256,
        };
        self.groups
            .iter()
            .map(|group| SourceKeyedRuntimeGroupPopulation {
                // Like memory inspection keys, this is an opaque correlation digest;
                // legacy pool identities can contain paths and must not be exposed.
                group: format!("{:x}", Sha256::digest(format!("{:?}", group.pool_identity))),
                target: group.target,
                prepared: group.instances.len(),
                active: group.active,
                in_progress: group.in_progress,
                shortfall: group.shortfall,
            })
            .collect()
    }
}

impl PreparedSourceKeyedRuntime {
    pub fn population(&self) -> Vec<SourceKeyedRuntimeGroupPopulation> {
        self.inventory.population()
    }

    pub fn reserve<RT: Runtime>(
        &self,
    ) -> anyhow::Result<SourceKeyedRuntimeActivationReservation<RT>> {
        let not_ready = || {
            ErrorMetadata::conflict(
                "StaticHermesWasmGenerationNotReady",
                "The chosen initialized Wasm capacity is no longer available",
            )
        };
        anyhow::ensure!(
            runtime_policy_store().snapshot().revision == self.policy_revision,
            ErrorMetadata::conflict(
                "WasmRuntimePolicyChanged",
                "The runtime policy changed during Wasm preparation"
            )
        );
        let mut population = self.population();
        // Admission checks replenishment ownership while holding the pool.
        // Reservation must use the same pool -> residency order while pinning
        // the exact inventory, or publication and queued admission can deadlock.
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        let mut residency = self.residency.state.lock();
        anyhow::ensure!(
            self.registry
                .state
                .read()
                .source_keyed_catalog
                .as_ref()
                .and_then(|catalog| catalog.descriptor_by_selector(&self.selector))
                .is_some(),
            not_ready()
        );
        let generation = residency
            .resident
            .get(&self.selector)
            .map(|resident| Arc::clone(&resident.generation))
            .context(not_ready())?;
        anyhow::ensure!(
            !generation.is_retired()
                && self
                    .inventory
                    .generation
                    .ptr_eq(&Arc::downgrade(&generation)),
            not_ready()
        );
        let mut selected = Vec::new();
        let mut read_sets = Vec::new();
        let mut population_owners = Vec::new();
        for (group, report) in self.inventory.groups.iter().zip(&mut population) {
            let occupancy = preparation::population_occupancy(&generation, &group.pool_identity);
            let active = occupancy.active;
            // Recheck achieved capacity under the reservation lock. Serving
            // Stores may have checked out or returned since the receipt, while
            // optional copies may have yielded to pressure. Only baseline loss
            // rejects publication; every chosen idle Store is pinned exactly.
            let instances = pool
                .iter()
                .filter_map(|value| value.downcast_ref::<GeneratedReusableInstance<RT>>())
                .filter(|instance| {
                    instance.all_entries_prepared
                        && instance.pool_identity.as_ref() == &group.pool_identity
                        && instance
                            .routed
                            .generation
                            .as_ref()
                            .is_some_and(|owner| Arc::ptr_eq(owner, &generation))
                })
                .take(
                    group
                        .target
                        .saturating_sub(active + occupancy.reserved)
                        .max(1),
                )
                .collect::<Vec<_>>();
            // Active occupancy starts before initialization validation and can
            // outlive execution during teardown. It reduces optional stock but
            // cannot replace an idle Store's validated initialization read set.
            anyhow::ensure!(!instances.is_empty(), not_ready());
            report.prepared = instances.len();
            report.active = active;
            report.shortfall = (report.prepared + active < report.target)
                .then_some(report.shortfall.unwrap_or("capacity_changed"));
            // Transfer population under the pool lock as well. If a later
            // group fails validation, these owners unwind before unlocking;
            // no physical Store has been removed yet.
            population_owners.push(preparation::reserve_population(
                &generation,
                &group.pool_identity,
                instances.len(),
            )?);
            for instance in instances {
                read_sets.push(
                    instance
                        .context_read_set
                        .as_ref()
                        .context("prepared runtime has no initialization read set")?
                        .clone(),
                );
                selected.push((instance.memory_slot_id, instance.id));
            }
        }
        anyhow::ensure!(!self.inventory.groups.is_empty(), not_ready());
        let pin = Arc::new(SourceKeyedActivationPin { generation });
        residency
            .activation_pins
            .retain(|pin| pin.strong_count() > 0);
        residency.activation_pins.push(Arc::downgrade(&pin));
        let instances = selected
            .into_iter()
            .map(|(slot, id)| {
                pool.take_instance_identity::<RT>(slot, id)
                    .expect("reserved Wasm inventory changed under its pool lock")
            })
            .collect();
        Ok(SourceKeyedRuntimeActivationReservation {
            residency: Arc::clone(&self.residency),
            selector: self.selector.clone(),
            pin,
            instances: parking_lot::Mutex::new(instances),
            population_owners,
            read_sets,
            population,
        })
    }
}

impl<RT: Runtime> SourceKeyedRuntimeActivationReservation<RT> {
    pub fn population(&self) -> &[SourceKeyedRuntimeGroupPopulation] {
        &self.population
    }

    pub async fn validate_dependencies(
        &self,
        transaction: &mut Transaction<RT>,
    ) -> anyhow::Result<()> {
        // This check holds the source/policy publication gate. A slow range
        // hash must release it without making rollback wait for a full
        // preparation deadline. No write has been submitted at this point.
        tokio::time::timeout(RUNTIME_PUBLICATION_VALIDATION_TIMEOUT, async {
            for read_set in &self.read_sets {
                anyhow::ensure!(
                    ContextCache::validate_and_apply_context_read_set(transaction, read_set)
                        .await?,
                    ErrorMetadata::conflict(
                        "StaticHermesWasmInitializationChanged",
                        "The prepared Wasm generation initialization dependencies changed"
                    )
                );
            }
            Ok(())
        })
        .await
        .context(ErrorMetadata::conflict(
            "StaticHermesWasmPublicationValidationTimeout",
            "Wasm initialization dependency validation timed out before publication",
        ))?
    }

    pub fn committed(&self, at: Timestamp) {
        // Selection is installed before Drop returns the inventory and releases
        // its pin. Older readiness/worker snapshots cannot regress this handoff.
        self.residency
            .observe_selected(Some(self.selector.clone()), at)
            .expect("committed source selection disagrees at its commit timestamp");
    }
}

impl<RT: Runtime> Drop for SourceKeyedRuntimeActivationReservation<RT> {
    fn drop(&mut self) {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        assert!(
            !self.pin.generation.is_retired(),
            "pinned Wasm generation was retired"
        );
        for mut instance in self.instances.get_mut().drain(..) {
            instance.idle_since = Instant::now();
            pool.insert(instance);
        }
        // Publish idle inventory and release its reserved population together.
        // The generation pin and physical memory charges outlive this transfer.
        self.population_owners.clear();
        drop(pool);
        GENERATED_GENERATION_RETIREMENT_NOTIFY.notify_waiters();
    }
}

pub fn source_keyed_runtime_unpaired_committed(at: Timestamp) {
    if let Some(residency) = ROUTE_CONFIGURATION
        .get()
        .and_then(|configuration| configuration.deployment_registry.as_ref())
        .and_then(|registry| registry.source_keyed_residency.as_ref())
    {
        residency
            .observe_selected(None, at)
            .expect("committed unpaired source disagrees at its commit timestamp");
    }
}

pub fn source_keyed_paired_deployment_guard_enabled() -> anyhow::Result<bool> {
    let configuration = route_configuration()?;
    Ok(configuration
        .deployment_registry
        .as_ref()
        .is_some_and(|registry| registry.source_keyed_paired_deployment_guard_enabled()))
}

fn configured_host_secret_selectors() -> anyhow::Result<&'static BTreeSet<String>> {
    Ok(&route_configuration()?.host_secret_selectors)
}

fn authorized_host_secret_selectors_for_routed_module(
    routed: &GeneratedRoutedModule,
) -> anyhow::Result<BTreeSet<String>> {
    Ok(authorized_host_secret_selectors_in_manifest(
        &routed.manifest,
        configured_host_secret_selectors()?,
    ))
}

fn load_generated_routed_module(
    route: &StaticHermesWasmtimeRouteHandle,
    mode: StaticHermesWasmtimePreparationMode,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    // This boundary is used from either preparation or execution so each
    // physical module load has one truthful pair of load markers.
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedModuleLoadStarted,
    );
    let routed = generated_routed_module(route, mode)
        .map_err(|error| classify_generated_module_load_error(error, mode.is_shadow()))
        .context(StaticHermesWasmModuleLoadingFailure)
        .map_err(allow_disposed_wasm_primary_fallback)?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedModuleLoaded,
    );
    Ok(routed)
}

fn prepared_routed_module(
    route: &StaticHermesWasmtimeRouteHandle,
    mode: StaticHermesWasmtimePreparationMode,
) -> anyhow::Result<PreparedRoutedModule> {
    #[cfg(any(test, feature = "testing"))]
    if let Some(routed) = TEST_HOOKS
        .lock()
        .as_ref()
        .and_then(Weak::upgrade)
        .and_then(|hooks| hooks.generated_route.clone())
    {
        return Ok(PreparedRoutedModule::TestHook(routed));
    }
    if configured_host_secret_selectors()?.is_empty() {
        return Ok(PreparedRoutedModule::Deferred);
    }
    Ok(PreparedRoutedModule::Configured(
        load_generated_routed_module(route, mode)?,
    ))
}

pub(super) fn authorized_host_secret_selectors_in_manifest(
    manifest: &WasmUdfExecutionPolicy,
    configured: &BTreeSet<String>,
) -> BTreeSet<String> {
    manifest
        .imported_operations()
        .iter()
        .filter_map(|operation| match operation.operation() {
            ImportedOperationDescriptor::HostSecretVerify { selector, .. }
                if configured.contains(selector) =>
            {
                Some(selector.clone())
            },
            _ => None,
        })
        .collect()
}

pub(crate) fn initialize_memory_pressure_sampler<RT: Runtime>(rt: RT) -> anyhow::Result<()> {
    let route_configuration = route_configuration()?;
    let controller = Arc::clone(&route_configuration.generated_memory_controller);
    let deployment_registry = route_configuration.deployment_registry.clone();
    let serialized_module_snapshot_directory = route_configuration
        .serialized_module_snapshot_directory
        .clone();
    if GENERATED_POOL_MAINTENANCE_STARTED.swap(true, Ordering::AcqRel) {
        return Ok(());
    }
    // Refresh admission from the finite cgroup limit before the backend can
    // accept generated Wasm work. Non-source startup already seeded the same
    // controller before its initial preload; the maintenance task keeps this
    // sample current afterward.
    controller.refresh_pressure_from_cgroup();
    struct MaintenanceLease;
    impl Drop for MaintenanceLease {
        fn drop(&mut self) {
            GENERATED_POOL_MAINTENANCE_STARTED.store(false, Ordering::Release);
        }
    }
    let maintenance_lease = MaintenanceLease;
    let maintenance_rt = rt.clone();
    rt.spawn_background("generated_wasm_memory_maintenance", async move {
        let _maintenance_lease = maintenance_lease;
        loop {
            controller.refresh_pressure_from_cgroup();
            if evict_generated_idle_instances::<RT>(
                &controller,
                IdleEvictionTrigger::Maintenance,
                IdleEvictionLimit::maintenance(),
            )
            .await
            .is_err()
            {
                log_generated_failure(GeneratedFailureLog::IdleRuntimeCleanup);
            }
            if matches!(controller.pressure(), BackendPressure::Active { .. }) {
                GENERATED_ROUTED_MODULES.lock().evict_idle_for_pressure();
            }
            if let Some(registry) = &deployment_registry {
                registry
                    .retire_source_keyed_idle_generations::<RT>(&controller)
                    .await;
                let reload_registry = Arc::clone(registry);
                let reload_controller = Arc::clone(&controller);
                let serialized_module_snapshot_directory =
                    serialized_module_snapshot_directory.clone().expect(
                        "deployment runtime registry lost its serialized-module snapshot directory",
                    );
                match tokio_spawn_blocking("generated_wasm_registry_reload", move || {
                    reload_registry.reload_with_source_preload(
                        &reload_controller,
                        &serialized_module_snapshot_directory,
                    )
                })
                .await
                {
                    Ok(Ok(Some(retired))) => {
                        log_registry_reload_event("accepted");
                        if retire_deployment_generation::<RT>(&controller, retired)
                            .await
                            .is_err()
                        {
                            log_generated_failure(GeneratedFailureLog::RetiredGenerationCleanup);
                        }
                    },
                    Ok(Ok(None)) => {},
                    Ok(Err(_error)) => {
                        log_registry_reload_event("rejected");
                        log_generated_failure(GeneratedFailureLog::RegistryReloadRejected);
                    },
                    Err(_error) => {
                        log_registry_reload_event("poll_failed");
                        log_generated_failure(GeneratedFailureLog::RegistryReloadTask);
                    },
                }
            }
            maintenance_rt
                .wait(GENERATED_POOL_MAINTENANCE_INTERVAL)
                .await;
        }
    });
    Ok(())
}

/// Identity index over the memory-controller-bounded set of idle Stores.
///
/// The outer mutex remains the synchronization boundary for the physical
/// instance and memory-ledger transitions. The index only avoids scanning
/// unrelated routes during request admission.
#[derive(Default)]
pub(super) struct GeneratedRoutedInstancePool {
    instances_by_identity: BTreeMap<Arc<GeneratedPoolIdentity>, Vec<Box<dyn Any + Send>>>,
}

impl GeneratedRoutedInstancePool {
    fn insert<RT: Runtime>(&mut self, instance: GeneratedReusableInstance<RT>) {
        if let Some(instances) = self
            .instances_by_identity
            .get_mut(instance.pool_identity.as_ref())
        {
            instances.push(Box::new(instance));
        } else {
            let pool_identity = Arc::clone(&instance.pool_identity);
            self.instances_by_identity
                .insert(pool_identity, vec![Box::new(instance)]);
        }
    }

    fn take<RT: Runtime>(
        &mut self,
        routed: &GeneratedRoutedModule,
    ) -> Option<GeneratedReusableInstance<RT>> {
        let pool_identity = &routed.pool_identity;
        let (value, identity_became_empty) = {
            let instances = self.instances_by_identity.get_mut(pool_identity)?;
            let position = instances.iter().position(|instance| {
                instance
                    .downcast_ref::<GeneratedReusableInstance<RT>>()
                    .is_some_and(|instance| instance.routed.has_same_generation_incarnation(routed))
            })?;
            let value = instances.swap_remove(position);
            (value, instances.is_empty())
        };
        if identity_became_empty {
            self.instances_by_identity.remove(pool_identity);
        }
        Some(
            *value
                .downcast::<GeneratedReusableInstance<RT>>()
                .expect("generated Wasm instance pool type changed after inspection"),
        )
    }

    fn contains<RT: Runtime>(&self, routed: &GeneratedRoutedModule) -> bool {
        self.instances_by_identity
            .get(&routed.pool_identity)
            .is_some_and(|instances| {
                instances.iter().any(|instance| {
                    instance
                        .downcast_ref::<GeneratedReusableInstance<RT>>()
                        .is_some_and(|instance| {
                            instance.routed.has_same_generation_incarnation(routed)
                        })
                })
            })
    }

    fn matching_memory_slot<RT: Runtime>(
        &self,
        routed: &GeneratedRoutedModule,
    ) -> Option<GeneratedSlotId> {
        self.instances_by_identity
            .get(&routed.pool_identity)?
            .iter()
            .find_map(|value| {
                let instance = value.downcast_ref::<GeneratedReusableInstance<RT>>()?;
                instance
                    .routed
                    .has_same_generation_incarnation(routed)
                    .then_some(instance.memory_slot_id)
            })
    }

    fn contains_value<T: Any + Send>(&self, pool_identity: &GeneratedPoolIdentity) -> bool {
        self.instances_by_identity
            .get(pool_identity)
            .is_some_and(|instances| instances.iter().any(|instance| instance.is::<T>()))
    }

    fn take_value<T: Any + Send>(&mut self, pool_identity: &GeneratedPoolIdentity) -> Option<T> {
        let (value, identity_became_empty) = {
            let instances = self.instances_by_identity.get_mut(pool_identity)?;
            let position = instances.iter().position(|instance| instance.is::<T>())?;
            let value = instances.swap_remove(position);
            (value, instances.is_empty())
        };
        if identity_became_empty {
            self.instances_by_identity.remove(pool_identity);
        }
        Some(
            *value
                .downcast::<T>()
                .expect("generated Wasm instance pool type changed after inspection"),
        )
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &Box<dyn Any + Send>> {
        self.instances_by_identity
            .values()
            .flat_map(|instances| instances.iter())
    }

    fn take_slot<RT: Runtime>(
        &mut self,
        slot_id: GeneratedSlotId,
    ) -> Option<GeneratedReusableInstance<RT>> {
        let position = self.iter().position(|instance| {
            instance
                .downcast_ref::<GeneratedReusableInstance<RT>>()
                .is_some_and(|instance| instance.memory_slot_id == slot_id)
        })?;
        Some(
            *self
                .swap_remove(position)
                .downcast::<GeneratedReusableInstance<RT>>()
                .expect("generated Wasm pool type changed after slot selection"),
        )
    }

    fn take_instance_identity<RT: Runtime>(
        &mut self,
        slot_id: GeneratedSlotId,
        instance_id: u64,
    ) -> Option<GeneratedReusableInstance<RT>> {
        let position = self.iter().position(|instance| {
            instance
                .downcast_ref::<GeneratedReusableInstance<RT>>()
                .is_some_and(|instance| {
                    instance.memory_slot_id == slot_id && instance.id == instance_id
                })
        })?;
        Some(
            *self
                .swap_remove(position)
                .downcast::<GeneratedReusableInstance<RT>>()
                .expect("generated Wasm instance pool type changed after identity selection"),
        )
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Box<dyn Any + Send>> {
        self.instances_by_identity
            .values_mut()
            .flat_map(|instances| instances.iter_mut())
    }

    pub(super) fn swap_remove(&mut self, mut index: usize) -> Box<dyn Any + Send> {
        let mut removed = None;
        let mut empty_identity = None;
        for (pool_identity, instances) in &mut self.instances_by_identity {
            if index < instances.len() {
                removed = Some(instances.swap_remove(index));
                if instances.is_empty() {
                    empty_identity = Some(pool_identity.clone());
                }
                break;
            }
            index -= instances.len();
        }
        let removed = removed.expect("generated Wasm instance pool index out of bounds");
        if let Some(pool_identity) = empty_identity {
            self.instances_by_identity.remove(&pool_identity);
        }
        removed
    }

    pub(super) fn take_all<RT: Runtime>(&mut self) -> Vec<GeneratedReusableInstance<RT>> {
        let mut selected = Vec::new();
        let mut retained_by_identity = BTreeMap::new();
        for (pool_identity, instances) in std::mem::take(&mut self.instances_by_identity) {
            let mut retained = Vec::new();
            for instance in instances {
                match instance.downcast::<GeneratedReusableInstance<RT>>() {
                    Ok(instance) => selected.push(*instance),
                    Err(instance) => retained.push(instance),
                }
            }
            if !retained.is_empty() {
                retained_by_identity.insert(pool_identity, retained);
            }
        }
        self.instances_by_identity = retained_by_identity;
        selected
    }

    #[cfg(test)]
    fn insert_test_value<T: Any + Send>(&mut self, pool_identity: GeneratedPoolIdentity, value: T) {
        self.instances_by_identity
            .entry(Arc::new(pool_identity))
            .or_default()
            .push(Box::new(value));
    }
}

pub(super) static GENERATED_ROUTED_INSTANCE_POOL: LazyLock<
    parking_lot::Mutex<GeneratedRoutedInstancePool>,
> = LazyLock::new(|| parking_lot::Mutex::new(GeneratedRoutedInstancePool::default()));

#[cfg(test)]
mod generated_routed_instance_pool_tests {
    use super::*;

    fn identity(package_key: &str) -> GeneratedPoolIdentity {
        GeneratedPoolIdentity::Route(GeneratedRouteIdentity::Singleton {
            package_key: package_key.to_owned(),
        })
    }

    #[test]
    fn identity_index_isolates_buckets_and_removes_empty_identities() {
        let first = identity("first");
        let second = identity("second");
        let mut pool = GeneratedRoutedInstancePool::default();
        pool.insert_test_value(first.clone(), 1_u64);
        pool.insert_test_value(first.clone(), "first-type");
        pool.insert_test_value(second.clone(), 2_u64);

        assert_eq!(pool.iter().count(), 3);
        assert_eq!(pool.instances_by_identity.len(), 2);
        assert!(pool.contains_value::<u64>(&first));
        assert!(pool.contains_value::<&'static str>(&first));
        assert_eq!(pool.take_value::<u64>(&first), Some(1));
        assert_eq!(pool.take_value::<u64>(&first), None);
        assert_eq!(pool.iter().count(), 2);
        assert_eq!(pool.instances_by_identity.len(), 2);

        assert_eq!(pool.take_value::<&'static str>(&first), Some("first-type"));
        assert!(!pool.instances_by_identity.contains_key(&first));
        assert_eq!(pool.take_value::<u64>(&second), Some(2));
        assert_eq!(pool.iter().count(), 0);
        assert!(pool.instances_by_identity.is_empty());
    }
}

fn generated_singleton_routed_module() -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    if let Some(routed) = GENERATED_ROUTED_MODULES.lock().singleton() {
        return Ok(routed);
    }
    let route_configuration = route_configuration()?;
    let package_directory = route_configuration
        .package_directory
        .as_deref()
        .context("generated Wasm route package directory is not configured")?;
    let serialized_module_snapshot_directory = route_configuration
        .serialized_module_snapshot_directory
        .as_deref()
        .context("generated Wasm serialized-module snapshot directory is not configured")?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedSharedEngineAcquisitionStarted,
    );
    let engine = shared_generated_engine(WasmMeteringMode::EpochChecks)?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedSharedEngineAcquired,
    );
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedRuntimeCompatibilityResolutionStarted,
    );
    let runtime_compatibility = generated_runtime_compatibility(
        GENERATED_ENGINE_COMPATIBILITY_SHA256,
        WasmMeteringMode::EpochChecks,
    )?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedRuntimeCompatibilityResolved,
    );
    let package = load_validated_package(
        package_directory,
        &runtime_compatibility,
        serialized_module_snapshot_directory,
    )?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedArtifactResolved,
    );
    let route_identity = GeneratedRouteIdentity::Singleton {
        package_key: package.package_key.clone(),
    };
    let routed = cache_generated_routed_module(
        &route_configuration.generated_memory_controller,
        package,
        route_identity,
        engine,
        None,
    )?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedModuleCacheBuilt,
    );
    Ok(routed)
}

pub(super) fn manifest_udf_kind(udf_type: UdfType) -> Option<ManifestUdfKind> {
    match udf_type {
        UdfType::Query => Some(ManifestUdfKind::Query),
        UdfType::Mutation => Some(ManifestUdfKind::Mutation),
        UdfType::Action | UdfType::HttpAction => None,
    }
}

fn generated_deployment_routed_module(
    route: &DeploymentWasmRoute,
    mode: StaticHermesWasmtimePreparationMode,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    let route_configuration = route_configuration()?;
    let serialized_module_snapshot_directory = route_configuration
        .serialized_module_snapshot_directory
        .as_deref()
        .context("generated Wasm serialized-module snapshot directory is not configured")?;
    load_generated_deployment_routed_module_with_aot_resolution(
        &route_configuration.generated_memory_controller,
        serialized_module_snapshot_directory,
        route,
        AuthenticatedGraphAotResolution::PreloadedOnly,
        mode,
    )
}

#[cfg(test)]
pub(super) fn load_generated_deployment_routed_module(
    controller: &Arc<GeneratedMemoryController>,
    serialized_module_snapshot_directory: &Path,
    route: &DeploymentWasmRoute,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    load_generated_deployment_routed_module_with_aot_resolution(
        controller,
        serialized_module_snapshot_directory,
        route,
        AuthenticatedGraphAotResolution::PreloadedOnly,
        StaticHermesWasmtimePreparationMode::Primary,
    )
}

fn load_generated_deployment_routed_module_with_aot_resolution(
    controller: &Arc<GeneratedMemoryController>,
    serialized_module_snapshot_directory: &Path,
    route: &DeploymentWasmRoute,
    aot_resolution: AuthenticatedGraphAotResolution,
    mode: StaticHermesWasmtimePreparationMode,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    match route.generation.registry.export_routing(
        &route.runtime_module_path,
        &route.export_name,
        route.udf_kind,
    )? {
        DeploymentExportRouting::Wasm {
            package_key,
            runtime_entry,
            route_lease,
            deployed_runtime_identity,
        } => {
            anyhow::ensure!(
                package_key == route.package_key
                    && DeploymentEntryIdentity::from_registry(runtime_entry)
                        == route.entry_identity
                    && route_lease == route.route_lease
                    && deployed_runtime_identity == &route.deployed_runtime_identity,
                "pinned generated Wasm route differs from its deployment generation"
            );
        },
        DeploymentExportRouting::ExistingRuntime => {
            anyhow::bail!("existing-runtime export reached the generated Wasm runtime")
        },
        DeploymentExportRouting::V8Fallback => {
            anyhow::bail!("V8 fallback export reached the generated Wasm runtime")
        },
    }
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedRegistryRouteValidated,
    );
    let route_identity = GeneratedRouteIdentity::DeploymentExport {
        deployment_sha256: route.generation.deployment_sha256().to_owned(),
        generation_sha256: route.generation.generation_sha256.clone(),
        package_key: route.package_key.clone(),
        entry: route.entry_identity.clone(),
    };
    let execution_graph = route.route_lease.route_id().and_then(|route_id| {
        route
            .generation
            .module_graph_catalog
            .as_ref()
            .and_then(|catalog| catalog.execution_graph(route_id))
    });
    if !route.generation.is_retired() {
        if let Some(routed) = GENERATED_ROUTED_MODULES.lock().get(&route_identity) {
            if cached_routed_module_matches_generation(&routed, Some(route.generation.as_ref()))? {
                #[cfg(any(test, feature = "testing"))]
                record_generated_route_preflight_stage(
                    StaticHermesGeneratedRoutePreflightStage::RoutedRouteLeaseAuthenticationStarted,
                );
                anyhow::ensure!(
                    routed
                        .package_identity
                        .authenticates_route_lease(&route.route_lease),
                    "cached generated Wasm package does not authenticate the route lease"
                );
                #[cfg(any(test, feature = "testing"))]
                record_generated_route_preflight_stage(
                    StaticHermesGeneratedRoutePreflightStage::RoutedRouteLeaseAuthenticated,
                );
                #[cfg(any(test, feature = "testing"))]
                record_generated_route_preflight_stage(
                    StaticHermesGeneratedRoutePreflightStage::RoutedArtifactResolved,
                );
                return Ok(routed);
            }
        }
    }
    // A cache miss can reclaim other routed modules before instance admission
    // runs. Optional verification may use authenticated cached modules only;
    // primary loading and background preparation own reconstruction.
    if mode.is_shadow() {
        return Err(StaticHermesQueryShadowCapacityUnavailable.into());
    }
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedSharedEngineAcquisitionStarted,
    );
    let metering_mode = match execution_graph.as_ref() {
        Some(graph) => graph.metering_mode()?,
        None => WasmMeteringMode::EpochChecks,
    };
    let engine = shared_generated_engine(metering_mode)?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedSharedEngineAcquired,
    );
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedRuntimeCompatibilityResolutionStarted,
    );
    let runtime_compatibility =
        generated_runtime_compatibility(metering_mode.compatibility_sha256(), metering_mode)?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedRuntimeCompatibilityResolved,
    );
    if let Some(graph) = execution_graph {
        #[cfg(any(test, feature = "testing"))]
        record_generated_route_preflight_stage(
            StaticHermesGeneratedRoutePreflightStage::RoutedGraphRouteMaterialResolutionStarted,
        );
        let route_material = route
            .generation
            .registry
            .authenticated_module_graph_route_material(
                &route.runtime_module_path,
                &route.export_name,
                route.udf_kind,
                &runtime_compatibility,
            )?;
        #[cfg(any(test, feature = "testing"))]
        record_generated_route_preflight_stage(
            StaticHermesGeneratedRoutePreflightStage::RoutedGraphRouteMaterialResolved,
        );
        #[cfg(any(test, feature = "testing"))]
        record_generated_route_preflight_stage(
            StaticHermesGeneratedRoutePreflightStage::RoutedRouteLeaseAuthenticationStarted,
        );
        anyhow::ensure!(
            route_material
                .identity
                .authenticates_route_lease(&route.route_lease),
            "authenticated module graph cohort does not authenticate the route lease"
        );
        #[cfg(any(test, feature = "testing"))]
        record_generated_route_preflight_stage(
            StaticHermesGeneratedRoutePreflightStage::RoutedRouteLeaseAuthenticated,
        );
        #[cfg(any(test, feature = "testing"))]
        record_generated_route_preflight_stage(
            StaticHermesGeneratedRoutePreflightStage::RoutedArtifactResolved,
        );
        let routed = cache_authenticated_graph_routed_module(
            controller,
            engine,
            serialized_module_snapshot_directory,
            Arc::clone(&route.generation),
            graph,
            route_material,
            route_identity,
            aot_resolution,
        )?;
        #[cfg(any(test, feature = "testing"))]
        record_generated_route_preflight_stage(
            StaticHermesGeneratedRoutePreflightStage::RoutedModuleCacheBuilt,
        );
        return Ok(routed);
    }
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedNongraphPackageResolutionStarted,
    );
    let package = match route
        .generation
        .registry
        .load_export_package_from_packages_root(
            &route.generation.packages_root,
            &route.runtime_module_path,
            &route.export_name,
            route.udf_kind,
            &runtime_compatibility,
            serialized_module_snapshot_directory,
        )? {
        DeploymentExportPackage::Wasm(package) => package,
        DeploymentExportPackage::ExistingRuntime => {
            anyhow::bail!("Wasm deployment route changed to existing runtime during package lookup")
        },
        DeploymentExportPackage::V8Fallback => {
            anyhow::bail!("Wasm deployment route changed to V8 fallback during package lookup")
        },
    };
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedNongraphPackageResolved,
    );
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedRouteLeaseAuthenticationStarted,
    );
    anyhow::ensure!(
        package
            .identity
            .authenticates_route_lease(&route.route_lease),
        "loaded generated Wasm package does not authenticate the route lease"
    );
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedRouteLeaseAuthenticated,
    );
    anyhow::ensure!(
        package.graph.is_none(),
        "generated graph execution requires explicit deployment authority"
    );
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedArtifactResolved,
    );
    let routed = cache_generated_routed_module(
        controller,
        package,
        route_identity,
        engine,
        Some(Arc::clone(&route.generation)),
    )?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedModuleCacheBuilt,
    );
    Ok(routed)
}

fn selected_generation_routes(
    generation: &Arc<DeploymentGeneration>,
) -> anyhow::Result<Vec<DeploymentWasmRoute>> {
    let selected_exports = generation.registry.selected_wasm_exports();
    let mut routes = Vec::with_capacity(selected_exports.len());
    for selected in selected_exports {
        let route = match generation.registry.export_routing(
            &selected.runtime_module_path,
            &selected.export_name,
            selected.udf_kind,
        )? {
            DeploymentExportRouting::Wasm {
                package_key,
                runtime_entry,
                route_lease,
                deployed_runtime_identity,
            } => DeploymentWasmRoute {
                generation: Arc::clone(generation),
                package_key: package_key.to_owned(),
                entry_identity: DeploymentEntryIdentity::from_registry(runtime_entry),
                route_lease,
                runtime_module_path: selected.runtime_module_path.clone(),
                export_name: selected.export_name.clone(),
                udf_kind: selected.udf_kind,
                deployed_runtime_identity: deployed_runtime_identity.clone(),
            },
            DeploymentExportRouting::ExistingRuntime | DeploymentExportRouting::V8Fallback => {
                anyhow::bail!(
                    "selected Wasm export changed routing while its generation was preloaded"
                )
            },
        };
        routes.push(route);
    }
    Ok(routes)
}

fn preload_generation_routes(
    controller: &Arc<GeneratedMemoryController>,
    serialized_module_snapshot_directory: &Path,
    generation: &Arc<DeploymentGeneration>,
) -> anyhow::Result<Vec<Arc<GeneratedSharedAotModule>>> {
    let routes = selected_generation_routes(generation)?;
    let mut loaded_routes = Vec::with_capacity(routes.len());
    let mut graph_modules = BTreeMap::new();
    for route in routes {
        let routed = load_generated_deployment_routed_module_with_aot_resolution(
            controller,
            serialized_module_snapshot_directory,
            &route,
            AuthenticatedGraphAotResolution::ReconstructBeforeReadiness,
            StaticHermesWasmtimePreparationMode::Primary,
        )?;
        if let Some(graph) = &routed.emscripten_graph {
            for module in std::iter::once(&graph.base)
                .chain(&graph.shared)
                .chain(std::iter::once(&graph.leaf))
            {
                graph_modules
                    .entry(module.identity.clone())
                    .or_insert_with(|| Arc::clone(module));
            }
        }
        // Keep every routed owner until the complete selected set is loaded.
        // Admission must fail before readiness instead of evicting an earlier
        // selected route and hiding an over-budget generation.
        loaded_routes.push(routed);
    }
    Ok(graph_modules.into_values().collect())
}

fn generated_routed_module(
    route: &StaticHermesWasmtimeRouteHandle,
    mode: StaticHermesWasmtimePreparationMode,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    match &route.route {
        StaticHermesWasmtimeRoute::GeneratedSingleton => generated_singleton_routed_module(),
        StaticHermesWasmtimeRoute::GeneratedDeployment(route) => {
            generated_deployment_routed_module(route, mode)
        },
        StaticHermesWasmtimeRoute::SourceKeyedDeployment(_) => {
            anyhow::bail!("source-keyed Wasm route was not selected before module loading")
        },
    }
}

fn take_generated_routed_instance_locked<RT: Runtime>(
    pool: &mut GeneratedRoutedInstancePool,
    routed: &GeneratedRoutedModule,
    reuse_instances: bool,
) -> Option<GeneratedReusableInstance<RT>> {
    if !reuse_instances {
        return None;
    }
    let mut instance = pool.take::<RT>(routed)?;
    instance.pool_checked_out = true;
    Some(instance)
}

fn log_generated_instance_pool_checkout(_metrics: &GateMetrics, reused: bool) {
    log_instance_pool_event(if reused {
        "generated_hit"
    } else {
        "generated_miss"
    });
    #[cfg(any(test, feature = "testing"))]
    if reused {
        _metrics.generated_pool_hits.fetch_add(1, Ordering::SeqCst);
    } else {
        _metrics
            .generated_pool_misses
            .fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GeneratedInstanceReturn {
    Pooled,
    CapacityUnavailable,
    GenerationRetired,
}

pub(super) async fn return_generated_routed_instance<RT: Runtime>(
    mut instance: GeneratedReusableInstance<RT>,
    memory_permit: InvocationMemoryPermit,
    terminal_memory_outcome: TerminalMemoryOutcome,
) -> anyhow::Result<GeneratedInstanceReturn> {
    anyhow::ensure!(
        instance.runtime_state == GeneratedRuntimeState::Live,
        "generated Wasm instance cannot be pooled after a failed guest call"
    );
    anyhow::ensure!(
        matches!(
            &*instance.routed.package_identity,
            ValidatedWasmUdfPackageIdentity::Legacy(_)
        ) || instance.all_entries_prepared,
        "generated Wasm instance cannot be pooled before every entry is initialized"
    );
    #[cfg(any(test, feature = "testing"))]
    let metrics = Arc::clone(&instance.store.data().metrics);
    #[cfg(any(test, feature = "testing"))]
    let runtime_id = instance.id;
    #[cfg(any(test, feature = "testing"))]
    let memory_slot_id = instance.memory_slot_id.test_identity();
    let usable_capacity = !instance.all_entries_prepared || memory_permit.reserve_idle_growth();
    let mut retired_instance = None;
    let mut memory_permit = Some(memory_permit);
    let retired = {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        instance.pool_checked_out = false;
        instance.idle_since = Instant::now();
        let retired = !usable_capacity
            || instance
                .routed
                .generation
                .as_ref()
                .is_some_and(|generation| generation.is_retired());
        if !retired {
            drop(instance.store.data_mut().teardown_cpu_permit.take());
            pool.insert(instance);
            // Checkout and idle eviction take this same pool lock before they
            // reserve or mark a slot, so neither can observe the physical
            // instance until its ledger transition is also complete.
            let memory_permit = memory_permit
                .take()
                .expect("generated Wasm memory permit disappeared before pooling");
            #[cfg(any(test, feature = "testing"))]
            metrics.record_generated_memory_snapshot(
                memory_permit.finish_with_snapshot_for_test(terminal_memory_outcome, true),
            );
            #[cfg(not(any(test, feature = "testing")))]
            memory_permit.finish(terminal_memory_outcome, true);
        } else {
            retired_instance = Some(instance);
        }
        retired
    };
    if retired {
        #[cfg(any(test, feature = "testing"))]
        metrics.record_generated_pool_event(
            Some(runtime_id),
            Some(memory_slot_id),
            if usable_capacity {
                StaticHermesGeneratedPoolEvent::ReturnedForGenerationRetirement
            } else {
                StaticHermesGeneratedPoolEvent::ReturnedWithoutGrowthCapacity
            },
        );
        let discard_result = discard_generated_instance(
            retired_instance.expect("retired generated Wasm instance disappeared"),
        )
        .await;
        memory_permit
            .take()
            .expect("retired generated Wasm memory permit disappeared")
            .finish(
                // A completed handler can still fail while disposing a Store
                // that cannot return to the pool. Publish that failure once.
                if discard_result.is_err() {
                    TerminalMemoryOutcome::SystemError
                } else {
                    terminal_memory_outcome
                },
                false,
            );
        discard_result?;
        log_instance_pool_event(if usable_capacity {
            "generated_retired_drop"
        } else {
            "generated_capacity_drop"
        });
    } else {
        #[cfg(any(test, feature = "testing"))]
        metrics.record_generated_pool_event(
            Some(runtime_id),
            Some(memory_slot_id),
            StaticHermesGeneratedPoolEvent::Returned,
        );
        log_instance_pool_event("generated_returned");
    }
    Ok(if !usable_capacity {
        GeneratedInstanceReturn::CapacityUnavailable
    } else if retired {
        GeneratedInstanceReturn::GenerationRetired
    } else {
        GeneratedInstanceReturn::Pooled
    })
}

pub(super) fn return_optional_generated_routed_instance_after_cpu_admission_rejection<
    RT: Runtime,
>(
    rt: RT,
    instance: Option<GeneratedReusableInstance<RT>>,
    memory_permit: InvocationMemoryPermit,
    shadow_work_guard: Option<Arc<OwnedSemaphorePermit>>,
) {
    let Some(instance) = instance else {
        memory_permit.finish_unstarted(false);
        return;
    };
    let mut instance = Some(instance);
    let mut memory_permit = Some(memory_permit);
    let retired = {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        let checked_out_instance = instance
            .as_mut()
            .expect("unstarted generated Wasm instance disappeared before CPU rejection");
        checked_out_instance.pool_checked_out = false;
        checked_out_instance.idle_since = Instant::now();
        let retired = checked_out_instance
            .routed
            .generation
            .as_ref()
            .is_some_and(|generation| generation.is_retired());
        if !retired {
            pool.insert(instance.take().expect(
                "unstarted generated Wasm instance disappeared before CPU rejection pooling",
            ));
            // Keep the pool and memory ledger transition together so a new
            // admission cannot observe this idle runtime before its slot.
            memory_permit
                .take()
                .expect("unstarted generated Wasm memory permit disappeared before pooling")
                .finish_unstarted(true);
        }
        retired
    };
    if !retired {
        return;
    }

    // CPU-admission failure must return immediately. Retired runtimes still
    // need guest teardown, but detached cleanup bounds its CPU admission and
    // drops the Store fail-closed if it cannot run.
    let instance = instance.expect("retired unstarted generated Wasm instance disappeared");
    let memory_permit = memory_permit
        .take()
        .expect("retired unstarted generated Wasm memory permit disappeared");
    rt.spawn_background("generated_wasm_retired_unstarted_cleanup", async move {
        let result = discard_generated_instance_after_cpu_admission_rejection(instance).await;
        memory_permit.finish_unstarted(false);
        if result.is_err() {
            log_generated_failure(GeneratedFailureLog::RetiredUnstartedCleanup);
        }
        drop(shadow_work_guard);
    });
}

pub(super) fn discard_generated_routed_instance_after_cpu_admission_rejection<RT: Runtime>(
    rt: RT,
    instance: GeneratedReusableInstance<RT>,
    memory_permit: InvocationMemoryPermit,
    shadow_work_guard: Option<Arc<OwnedSemaphorePermit>>,
) {
    // This dirty Store cannot return to the pool after CPU admission fails. Its
    // detached guest teardown has bounded CPU admission, after which it drops
    // the Store fail-closed.
    rt.spawn_background("generated_wasm_rejected_unstarted_cleanup", async move {
        let result = discard_generated_instance_after_cpu_admission_rejection(instance).await;
        memory_permit.finish_unstarted(false);
        if result.is_err() {
            log_generated_failure(GeneratedFailureLog::RejectedUnstartedCleanup);
        }
        drop(shadow_work_guard);
    });
}

pub(super) async fn retire_deployment_generation<RT: Runtime>(
    controller: &GeneratedMemoryController,
    generation: Arc<DeploymentGeneration>,
) -> anyhow::Result<()> {
    generation.retired.store(true, Ordering::Release);
    preparation::release_generation(&generation);
    GENERATED_GENERATION_RETIREMENT_NOTIFY.notify_waiters();
    // Active routed modules retain their own compiled graph members. Once the
    // generation is retired, its readiness owner must stop pinning unrelated
    // idle shared modules while those active invocations finish.
    generation.release_preloaded_graph_modules();
    GENERATED_ROUTED_MODULES
        .lock()
        .retire_generation(&generation);
    let retired = {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        let idle_slots = pool
            .iter()
            .filter_map(|instance| {
                instance
                    .downcast_ref::<GeneratedReusableInstance<RT>>()
                    .filter(|instance| {
                        instance
                            .routed
                            .generation
                            .as_ref()
                            .is_some_and(|candidate| Arc::ptr_eq(candidate, &generation))
                    })
                    .map(|instance| (instance.memory_slot_id, instance.idle_since.elapsed()))
            })
            .collect::<Vec<_>>();
        let candidates = controller.idle_eviction_candidates(
            &BTreeSet::new(),
            &idle_slots,
            IdleEvictionTrigger::GenerationRetirement,
        );
        anyhow::ensure!(
            candidates.len() == idle_slots.len(),
            "retired generated Wasm generation did not select every idle runtime"
        );
        candidates
            .into_iter()
            .map(|(slot_id, reason)| -> anyhow::Result<_> {
                let position = pool
                    .iter()
                    .position(|instance| {
                        instance
                            .downcast_ref::<GeneratedReusableInstance<RT>>()
                            .is_some_and(|instance| instance.memory_slot_id == slot_id)
                    })
                    .context("retired generated Wasm idle runtime disappeared from its pool")?;
                let instance = *pool
                    .swap_remove(position)
                    .downcast::<GeneratedReusableInstance<RT>>()
                    .expect("generated Wasm pool type changed during generation retirement");
                Ok((instance, reason))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    };

    let result = discard_generated_idle_instances(
        controller,
        retired,
        IdleEvictionTrigger::GenerationRetirement,
        IdleEvictionLimit::maintenance(),
    )
    .await;
    GENERATED_ROUTED_MODULES
        .lock()
        .evict_all_idle_shared_aot_modules();
    log_registry_reload_event("retired");
    result.map(|_| ()).map_err(Into::into)
}

/// Residency eviction is synchronous with admission: mark the exact
/// incarnation retired and remove its routed-module cache entries before a
/// future incarnation can reuse the same semantic route identity. Existing
/// Arc owners continue to execute; maintenance and return paths discard idle
/// instances belonging to the retired pointer.
fn retire_source_keyed_generation(generation: &Arc<DeploymentGeneration>) {
    generation.retired.store(true, Ordering::Release);
    preparation::release_generation(generation);
    GENERATED_GENERATION_RETIREMENT_NOTIFY.notify_waiters();
    generation.release_preloaded_graph_modules();
    GENERATED_ROUTED_MODULES
        .lock()
        .retire_generation(generation);
    GENERATED_ROUTED_MODULES
        .lock()
        .evict_all_idle_shared_aot_modules();
}

fn discard_unpublished_generation(generation: Arc<DeploymentGeneration>) {
    generation.retired.store(true, Ordering::Release);
    generation.release_preloaded_graph_modules();
    GENERATED_ROUTED_MODULES
        .lock()
        .retire_generation(&generation);
    drop(generation);
    GENERATED_ROUTED_MODULES
        .lock()
        .evict_all_idle_shared_aot_modules();
}

fn take_generated_idle_instances_for_eviction_locked<RT: Runtime>(
    pool: &mut GeneratedRoutedInstancePool,
    controller: &GeneratedMemoryController,
    trigger: IdleEvictionTrigger,
) -> Vec<(GeneratedReusableInstance<RT>, IdleEvictionReason)> {
    let mut instances = pool
        .iter()
        .filter_map(|instance| instance.downcast_ref::<GeneratedReusableInstance<RT>>())
        .collect::<Vec<_>>();
    instances.sort_by_key(|instance| instance.idle_since);
    let mut protected_from_age = BTreeSet::new();
    if trigger == IdleEvictionTrigger::Maintenance {
        let mut targets = preparation::idle_retention_targets();
        // Retain the newest prepared instances up to each group's demand
        // target. Age eviction must not undo preparation every idle interval;
        // pressure, admission and retirement can still reclaim these slots.
        for instance in instances.iter().rev() {
            if instance.all_entries_prepared
                && let Some(remaining) = targets.get_mut(instance.pool_identity.as_ref())
                && *remaining > 0
            {
                *remaining -= 1;
                protected_from_age.insert(instance.memory_slot_id);
            }
        }
    }
    let idle_slots = instances
        .iter()
        .map(|instance| (instance.memory_slot_id, instance.idle_since.elapsed()))
        .collect::<Vec<_>>();
    controller
        .idle_eviction_candidates(&protected_from_age, &idle_slots, trigger)
        .into_iter()
        .map(|(slot_id, reason)| {
            let instance = pool
                .take_slot::<RT>(slot_id)
                .expect("selected generated Wasm idle slot disappeared from its pool");
            (instance, reason)
        })
        .collect()
}

struct EvictingGeneratedInstance<'a, RT: Runtime> {
    instance: Option<GeneratedReusableInstance<RT>>,
    controller: &'a GeneratedMemoryController,
    slot_id: GeneratedSlotId,
    reason: IdleEvictionReason,
}

impl<RT: Runtime> Drop for EvictingGeneratedInstance<'_, RT> {
    fn drop(&mut self) {
        // Unvisited entries in a cancelled batch still own their Store. Free
        // it before releasing the slot and its protected forecast identity.
        drop(self.instance.take());
        self.controller
            .finish_idle_eviction(self.slot_id, self.reason);
    }
}

fn discard_generated_idle_instances<'a, RT: Runtime>(
    controller: &'a GeneratedMemoryController,
    evicted: Vec<(GeneratedReusableInstance<RT>, IdleEvictionReason)>,
    _trigger: IdleEvictionTrigger,
    limit: IdleEvictionLimit<'a>,
) -> impl Future<Output = Result<bool, IdleEvictionError>> + Send + 'a {
    // Construct the owners before returning the future: dropping it without
    // polling must release both physical instances and their evicting slots.
    let evicted = evicted
        .into_iter()
        .map(|(instance, reason)| EvictingGeneratedInstance {
            slot_id: instance.memory_slot_id,
            instance: Some(instance),
            controller,
            reason,
        })
        .collect::<Vec<_>>();
    async move {
        let evicted_any = !evicted.is_empty();
        let mut first_error = None;
        let mut interrupted = None;
        for mut eviction in evicted {
            #[cfg(any(test, feature = "testing"))]
            let reason = eviction.reason;
            // This local and its teardown future are dropped before `eviction`
            // on cancellation, preserving physical-destruction-before-release.
            let instance = eviction.instance.take().expect("eviction lost its Store");
            #[cfg(any(test, feature = "testing"))]
            let metrics = Arc::clone(&instance.store.data().metrics);
            #[cfg(any(test, feature = "testing"))]
            let runtime_id = instance.id;
            #[cfg(any(test, feature = "testing"))]
            let slot_id = instance.memory_slot_id;
            #[cfg(any(test, feature = "testing"))]
            metrics.record_generated_pool_event(
                Some(runtime_id),
                Some(slot_id.test_identity()),
                StaticHermesGeneratedPoolEvent::Evicted {
                    trigger: static_hermes_generated_pool_eviction_trigger(_trigger),
                    reason: static_hermes_generated_pool_eviction_reason(reason),
                },
            );
            let destroy_result = if interrupted.is_some() {
                drop(instance);
                None
            } else {
                let discard = discard_generated_instance(instance);
                tokio::pin!(discard);
                let outcome = if let Some(cancellation) = limit.cancellation {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => Err(IdleEvictionError::Cancelled),
                        _ = tokio::time::sleep_until(limit.deadline) => {
                            Err(IdleEvictionError::Deadline)
                        },
                        result = &mut discard => result.map_err(IdleEvictionError::Cleanup),
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = tokio::time::sleep_until(limit.deadline) => {
                            Err(IdleEvictionError::Deadline)
                        },
                        result = &mut discard => result.map_err(IdleEvictionError::Cleanup),
                    }
                };
                match outcome {
                    Ok(()) => None,
                    Err(IdleEvictionError::Cleanup(error)) => Some(error),
                    Err(error @ (IdleEvictionError::Cancelled | IdleEvictionError::Deadline)) => {
                        interrupted = Some(error);
                        None
                    },
                }
            };
            drop(eviction);
            // Do not let one maintenance batch repeatedly barge ahead of the
            // primary waiter notified when the teardown permit was released.
            tokio::task::yield_now().await;
            if first_error.is_none() {
                first_error = destroy_result;
            }
        }
        // A later deadline can bound the rest of the batch, but cannot turn an
        // already observed teardown failure into ordinary admission overload.
        match (first_error, interrupted) {
            (Some(error), _) => Err(IdleEvictionError::Cleanup(error)),
            (None, Some(interrupted)) => Err(interrupted),
            (None, None) => Ok(evicted_any),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
fn static_hermes_generated_pool_admission_wait_reason(
    reason: AdmissionWaitReason,
) -> StaticHermesGeneratedPoolAdmissionWaitReason {
    match reason {
        AdmissionWaitReason::Pressure => StaticHermesGeneratedPoolAdmissionWaitReason::Pressure,
        AdmissionWaitReason::ActiveCountCeiling => {
            StaticHermesGeneratedPoolAdmissionWaitReason::ActiveCountCeiling
        },
        AdmissionWaitReason::InstanceCountCeiling => {
            StaticHermesGeneratedPoolAdmissionWaitReason::InstanceCountCeiling
        },
        AdmissionWaitReason::SlotUnavailable => {
            StaticHermesGeneratedPoolAdmissionWaitReason::SlotUnavailable
        },
        AdmissionWaitReason::SoftBudget => StaticHermesGeneratedPoolAdmissionWaitReason::SoftBudget,
        AdmissionWaitReason::HardBudget => StaticHermesGeneratedPoolAdmissionWaitReason::HardBudget,
        AdmissionWaitReason::FunctionRecordCapacity => {
            StaticHermesGeneratedPoolAdmissionWaitReason::FunctionRecordCapacity
        },
    }
}

#[cfg(any(test, feature = "testing"))]
fn static_hermes_generated_pool_eviction_trigger(
    trigger: IdleEvictionTrigger,
) -> StaticHermesGeneratedPoolEvictionTrigger {
    match trigger {
        IdleEvictionTrigger::Maintenance => StaticHermesGeneratedPoolEvictionTrigger::Maintenance,
        IdleEvictionTrigger::NewSlot => StaticHermesGeneratedPoolEvictionTrigger::NewSlot,
        IdleEvictionTrigger::AdmissionBudget => {
            StaticHermesGeneratedPoolEvictionTrigger::AdmissionBudget
        },
        IdleEvictionTrigger::GenerationRetirement => {
            StaticHermesGeneratedPoolEvictionTrigger::GenerationRetirement
        },
        IdleEvictionTrigger::InitializationInvalidation => {
            StaticHermesGeneratedPoolEvictionTrigger::InitializationInvalidation
        },
    }
}

#[cfg(any(test, feature = "testing"))]
fn static_hermes_generated_pool_eviction_reason(
    reason: IdleEvictionReason,
) -> StaticHermesGeneratedPoolEvictionReason {
    match reason {
        IdleEvictionReason::IdleAge => StaticHermesGeneratedPoolEvictionReason::IdleAge,
        IdleEvictionReason::MemoryPressure => {
            StaticHermesGeneratedPoolEvictionReason::MemoryPressure
        },
        IdleEvictionReason::InstanceCeiling => {
            StaticHermesGeneratedPoolEvictionReason::InstanceCeiling
        },
        IdleEvictionReason::AdmissionBudget => {
            StaticHermesGeneratedPoolEvictionReason::AdmissionBudget
        },
        IdleEvictionReason::GenerationRetirement => {
            StaticHermesGeneratedPoolEvictionReason::GenerationRetirement
        },
        IdleEvictionReason::InitializationInvalidation => {
            StaticHermesGeneratedPoolEvictionReason::InitializationInvalidation
        },
        IdleEvictionReason::TestCleanup => StaticHermesGeneratedPoolEvictionReason::TestCleanup,
    }
}

pub(super) fn evict_generated_idle_instances<'a, RT: Runtime>(
    controller: &'a GeneratedMemoryController,
    trigger: IdleEvictionTrigger,
    limit: IdleEvictionLimit<'a>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<bool, IdleEvictionError>> + Send + 'a>> {
    let evicted = take_generated_idle_instances_for_eviction_locked::<RT>(
        &mut GENERATED_ROUTED_INSTANCE_POOL.lock(),
        controller,
        trigger,
    );
    Box::pin(discard_generated_idle_instances(
        controller, evicted, trigger, limit,
    ))
}

fn evict_generated_idle_instance_for_new_slot<'a, RT: Runtime>(
    controller: &'a GeneratedMemoryController,
    routed: &GeneratedRoutedModule,
    reuse_instances: bool,
    limit: IdleEvictionLimit<'a>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<bool, IdleEvictionError>> + Send + 'a>> {
    let evicted = {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        // A runtime can return after the failed admission attempt. Recheck the
        // route under the pool lock before selecting an idle slot so that the
        // returned runtime is adopted on the next attempt instead of destroyed.
        let route_match_available = reuse_instances && pool.contains::<RT>(routed);
        if route_match_available {
            Vec::new()
        } else {
            take_generated_idle_instances_for_eviction_locked::<RT>(
                &mut pool,
                controller,
                IdleEvictionTrigger::NewSlot,
            )
        }
    };
    Box::pin(discard_generated_idle_instances(
        controller,
        evicted,
        IdleEvictionTrigger::NewSlot,
        limit,
    ))
}

pub(crate) fn resolve_route(
    udf_type: UdfType,
    path_and_args: &ValidatedPathAndArgs,
) -> anyhow::Result<Option<StaticHermesWasmtimeRouteHandle>> {
    resolve_route_inner(udf_type, path_and_args, RegistryUse::Primary)
}

pub(crate) fn resolve_shadow_route(
    udf_type: UdfType,
    path_and_args: &ValidatedPathAndArgs,
) -> anyhow::Result<Option<StaticHermesWasmtimeRouteHandle>> {
    resolve_route_inner(udf_type, path_and_args, RegistryUse::Shadow)
}

/// Resolve a legacy test fixture through the shadow lane without weakening
/// production admission. These fixtures predate the maintained authenticated
/// shadow-only registry contract and are not an operational routing path.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn resolve_compatibility_shadow_route(
    udf_type: UdfType,
    path_and_args: &ValidatedPathAndArgs,
) -> anyhow::Result<Option<StaticHermesWasmtimeRouteHandle>> {
    resolve_route_inner(udf_type, path_and_args, RegistryUse::CompatibilityShadow)
}

/// Return the active registry generation's authenticated query-shadow route
/// selection without exposing executable or user-provided route metadata.
pub(crate) fn active_query_shadow_registry(
) -> anyhow::Result<Option<StaticHermesQueryShadowRegistry>> {
    let route_configuration = route_configuration()?;
    let Some(deployment_registry) = &route_configuration.deployment_registry else {
        return Ok(None);
    };
    let registry = deployment_registry.query_shadow_registry();
    // A source-keyed descriptor catalog is authenticated before any
    // invocation selects a source snapshot. Until that first selection there is no
    // active generation to report; returning the opaque wrapper here would
    // expose an empty generation digest and make an unselected baseline look
    // like a malformed active registry.
    Ok(selected_query_shadow_registry(registry))
}

pub(crate) fn generated_memory_inspection(
    udf_type: UdfType,
    after_route: Option<&str>,
    limit: usize,
) -> anyhow::Result<StaticHermesMemoryInspection> {
    // Diagnostics must not initialize configuration: that path can load and
    // compile a generation. Client startup owns initialization.
    let configuration = ROUTE_CONFIGURATION
        .get()
        .context("Wasm memory inspection is unavailable before runtime initialization")?;
    let registry = configuration
        .deployment_registry
        .as_ref()
        .and_then(|registry| selected_query_shadow_registry(registry.query_shadow_registry()));
    let kind = manifest_udf_kind(udf_type).context("unsupported memory inspection UDF type")?;
    Ok(configuration.generated_memory_controller.inspect(
        registry.as_ref().map(|r| r.generation_sha256()),
        kind,
        registry.as_ref().map_or(&[], |r| r.route_sha256s(udf_type)),
        after_route,
        limit,
    ))
}

pub(crate) fn generated_memory_statistics(
    udf_type: UdfType,
) -> anyhow::Result<Option<StaticHermesGeneratedMemoryStatistics>> {
    let route_configuration = route_configuration()?;
    let Some(deployment_registry) = &route_configuration.deployment_registry else {
        return Ok(None);
    };
    let Some(registry) =
        selected_query_shadow_registry(deployment_registry.query_shadow_registry())
    else {
        return Ok(None);
    };
    let Some(udf_kind) = manifest_udf_kind(udf_type) else {
        return Ok(None);
    };
    Ok(Some(
        route_configuration
            .generated_memory_controller
            .statistics_for_routes(
                registry.generation_sha256(),
                udf_kind,
                registry.route_sha256s(udf_type),
            ),
    ))
}

fn selected_query_shadow_registry(
    registry: StaticHermesQueryShadowRegistry,
) -> Option<StaticHermesQueryShadowRegistry> {
    registry.selection.is_some().then_some(registry)
}

fn resolve_route_inner(
    udf_type: UdfType,
    path_and_args: &ValidatedPathAndArgs,
    registry_use: RegistryUse,
) -> anyhow::Result<Option<StaticHermesWasmtimeRouteHandle>> {
    let route_configuration = route_configuration()?;
    if path_and_args.path().component != ComponentId::Root
        || !path_and_args.path().component_path.is_root()
    {
        return Ok(None);
    }
    let quarantine_snapshot = crate::static_hermes_wasmtime_quarantine().snapshot();
    #[cfg(any(test, feature = "testing"))]
    if !path_and_args.path().udf_path.is_system()
        && matches!(udf_type, UdfType::Query | UdfType::Mutation)
        && TEST_HOOKS
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|hooks| hooks.generated_route.is_some())
    {
        return Ok(StaticHermesWasmtimeRouteHandle::admit(
            StaticHermesWasmtimeRoute::GeneratedSingleton,
            udf_type,
            path_and_args,
            quarantine_snapshot,
        ));
    }
    if let Some(deployment) = &route_configuration.deployment_registry {
        if path_and_args.path().udf_path.is_system() {
            return Ok(None);
        }
        let Some(udf_kind) = manifest_udf_kind(udf_type) else {
            return Ok(None);
        };
        let udf_path = &path_and_args.path().udf_path;
        if deployment.uses_source_keyed_selection() {
            return Ok(StaticHermesWasmtimeRouteHandle::admit(
                StaticHermesWasmtimeRoute::SourceKeyedDeployment(SourceKeyedDeploymentRoute {
                    deployment_registry: Arc::clone(deployment),
                    registry_use,
                    runtime_module_path: udf_path.module().as_str().to_owned(),
                    export_name: udf_path.function_name().to_string(),
                    udf_kind,
                }),
                udf_type,
                path_and_args,
                quarantine_snapshot,
            ));
        }
        let Some(generation) = deployment.state.read().current.clone() else {
            return Ok(None);
        };
        if !generation.admission.allows(registry_use) {
            return Ok(None);
        }
        return resolve_generation_route(
            generation,
            registry_use,
            udf_type,
            path_and_args,
            udf_kind,
            quarantine_snapshot,
        );
    }
    if registry_use.is_shadow() {
        return Ok(None);
    }
    let Some(path) = &route_configuration.custom_path else {
        return Ok(None);
    };
    let path_matches = !path_and_args.path().udf_path.is_system()
        && matches!(udf_type, UdfType::Query | UdfType::Mutation)
        && path_and_args.path().udf_path.to_string() == *path;
    if !path_matches {
        return Ok(None);
    }
    Ok(StaticHermesWasmtimeRouteHandle::admit(
        StaticHermesWasmtimeRoute::GeneratedSingleton,
        udf_type,
        path_and_args,
        quarantine_snapshot,
    ))
}

fn quarantine_admits_wasm(
    snapshot: &crate::StaticHermesWasmtimeQuarantineSnapshot,
    module_path: &str,
    function_name: &str,
    generation_sha256: Option<&str>,
) -> bool {
    let uses_wasm = snapshot
        .evaluate_generation(module_path, function_name, generation_sha256)
        .uses_wasm();
    if !uses_wasm {
        crate::static_hermes_wasmtime_quarantine::record_forced_to_v8();
    }
    uses_wasm
}

fn resolve_generation_route(
    generation: Arc<DeploymentGeneration>,
    registry_use: RegistryUse,
    udf_type: UdfType,
    path_and_args: &ValidatedPathAndArgs,
    udf_kind: ManifestUdfKind,
    quarantine_snapshot: Arc<crate::StaticHermesWasmtimeQuarantineSnapshot>,
) -> anyhow::Result<Option<StaticHermesWasmtimeRouteHandle>> {
    if !generation.admission.allows(registry_use) {
        return Ok(None);
    }
    let udf_path = &path_and_args.path().udf_path;
    match generation.registry.export_routing(
        udf_path.module().as_str(),
        udf_path.function_name(),
        udf_kind,
    )? {
        DeploymentExportRouting::Wasm {
            package_key,
            runtime_entry,
            route_lease,
            deployed_runtime_identity,
        } => Ok(StaticHermesWasmtimeRouteHandle::admit(
            StaticHermesWasmtimeRoute::GeneratedDeployment(DeploymentWasmRoute {
                generation: Arc::clone(&generation),
                package_key: package_key.to_owned(),
                entry_identity: DeploymentEntryIdentity::from_registry(runtime_entry),
                route_lease,
                runtime_module_path: udf_path.module().as_str().to_owned(),
                export_name: udf_path.function_name().to_string(),
                udf_kind,
                deployed_runtime_identity: deployed_runtime_identity.clone(),
            }),
            udf_type,
            path_and_args,
            quarantine_snapshot,
        )),
        DeploymentExportRouting::ExistingRuntime | DeploymentExportRouting::V8Fallback => Ok(None),
    }
}

pub(crate) fn observe_v8_primary_demand(
    udf_type: UdfType,
    path_and_args: &ValidatedPathAndArgs,
) -> Option<[PrimaryDemandObservation; 2]> {
    let path = path_and_args.path();
    if path.component != ComponentId::Root
        || !path.component_path.is_root()
        || path.udf_path.is_system()
    {
        return None;
    }
    let kind = manifest_udf_kind(udf_type)?;
    let residency = ROUTE_CONFIGURATION
        .get()?
        .deployment_registry
        .as_ref()?
        .source_keyed_residency
        .as_ref()?;
    // Bootstrap estimates follow the last database-confirmed selection. They
    // grant no routing authority and must not add source reads or a new failure
    // dependency to an admitted V8 request. Candidate residency is not selection.
    let generation = {
        let state = residency.state.lock();
        let generation = &state.resident.get(state.selected.as_ref()?)?.generation;
        if generation.is_retired()
            || !generation.admission.allows(RegistryUse::Primary)
            || !generation.registry.is_selected_wasm_export(
                path.udf_path.module().as_str(),
                path.udf_path.function_name(),
                kind,
            )
        {
            return None;
        }
        Arc::clone(generation)
    };
    preparation::observe_primary_demand(
        &generation,
        path.udf_path.module().as_str(),
        path.udf_path.function_name(),
        kind,
    )
}

pub(crate) async fn select_route_for_invocation<RT: Runtime>(
    route: StaticHermesWasmtimeRouteHandle,
    udf_type: UdfType,
    path_and_args: &ValidatedPathAndArgs,
    transaction: &mut Transaction<RT>,
) -> anyhow::Result<Option<StaticHermesWasmtimeRouteHandle>> {
    let deadline = route.admission_deadline()?;
    within_static_hermes_admission(deadline, async {
        route.validate_invocation(udf_type, path_and_args)?;
        let StaticHermesWasmtimeRoute::SourceKeyedDeployment(candidate) = &route.route else {
            return Ok(Some(route));
        };
        // Selection is snapshot authority. Generated preparation records the
        // complete code dependency below; V8 records its imports on execution.
        let Some(source_package) = SourcePackageModel::new(transaction, TableNamespace::Global)
            .get_latest_for_runtime_selection()
            .await?
        else {
            return Ok(None);
        };
        let Some(runtime_generation) = source_package.runtime_generation.as_ref() else {
            return Ok(None);
        };
        let runtime_content_sha256 = source_package
            .runtime_content_sha256
            .as_ref()
            .context("paired source package has no runtime content identity")?
            .as_hex();
        let runtime_generation = SourceKeyedRuntimeGenerationIdentity {
            deployment_sha256: runtime_generation.deployment_sha256.as_hex(),
            generation_manifest_sha256: runtime_generation.generation_manifest_sha256.as_hex(),
            generation_sha256: runtime_generation.generation_sha256.as_hex(),
        };
        let begin_timestamp = *transaction.begin_timestamp();
        let registry = &candidate.deployment_registry;
        let selector =
            SourceKeyedGenerationSelector::requested(&runtime_content_sha256, &runtime_generation);
        let descriptor = registry
            .state
            .read()
            .source_keyed_catalog
            .as_ref()
            .and_then(|catalog| catalog.descriptor_by_selector(&selector))
            .context("committed paired Wasm generation is absent from the runtime catalog")?;
        let residency = registry
            .source_keyed_residency
            .as_ref()
            .context("source-keyed selection lost its residency manager")?;
        if candidate.registry_use == RegistryUse::Primary {
            let eligible = if let Some(generation) = residency.resident(&selector) {
                generation.admission.allows(RegistryUse::Primary)
                    && matches!(
                        generation.registry.export_routing(
                            &candidate.runtime_module_path,
                            &candidate.export_name,
                            candidate.udf_kind,
                        )?,
                        DeploymentExportRouting::Wasm { .. }
                    )
            } else {
                // Publication can change independently of live mode. Authenticate
                // eligibility before cold Wasm preloading: shadow-only sources
                // and excluded exports use V8 even when Wasm capacity is unavailable.
                let manifest = residency
                    .authenticated_manifest(registry, Arc::clone(&descriptor))
                    .await
                    .map_err(|error| {
                        allow_disposed_wasm_primary_fallback(
                            error.context(StaticHermesWasmModuleLoadingFailure),
                        )
                    })?;
                manifest.admission().allows(RegistryUse::Primary)
                    && matches!(
                        manifest
                            .registry
                            .export_routing(
                                &candidate.runtime_module_path,
                                &candidate.export_name,
                                candidate.udf_kind,
                            )
                            .map_err(|error| {
                                allow_disposed_wasm_primary_fallback(
                                    anyhow::Error::new(error)
                                        .context(StaticHermesWasmModuleLoadingFailure),
                                )
                            })?,
                        DeploymentExportRouting::Wasm { .. }
                    )
            };
            if !eligible {
                return Ok(None);
            }
        }
        let generation = if candidate.registry_use.is_shadow() {
            // Optional verification cannot make V8 wait for authentication or
            // compilation. The startup/preparation owner loads cold capacity.
            residency.observe_selected(Some(selector.clone()), begin_timestamp)?;
            let generation = residency
                .resident(&selector)
                .context(StaticHermesQueryShadowCapacityUnavailable)?;
            registry.record_source_keyed_selection(&generation, begin_timestamp)?;
            generation
        } else {
            registry
                .ensure_source_keyed_generation(
                    &runtime_content_sha256,
                    &runtime_generation,
                    Some(begin_timestamp),
                    deadline,
                )
                .await
                .map_err(|error| {
                    if error.is::<SourceKeyedRuntimeCapacityUnavailable>() {
                        error
                            .context(StaticHermesWasmAdmissionFailure::Module)
                            .context(ErrorMetadata::overloaded(
                                "WasmAdmissionUnavailable",
                                "Wasm module loading capacity is unavailable",
                            ))
                    } else {
                        // This load reads authenticated runtime artifacts; the
                        // database source selection above keeps its own errors.
                        allow_disposed_wasm_primary_fallback(
                            error.context(StaticHermesWasmModuleLoadingFailure),
                        )
                    }
                })?
                .context("committed paired Wasm generation is absent from the runtime catalog")?
        };
        if !quarantine_admits_wasm(
            &route.quarantine_snapshot,
            path_and_args.path().udf_path.module().as_str(),
            path_and_args.path().udf_path.function_name(),
            Some(generation.generation_sha256.as_str()),
        ) {
            return Ok(None);
        }
        let selected = resolve_generation_route(
            generation,
            candidate.registry_use,
            udf_type,
            path_and_args,
            candidate.udf_kind,
            Arc::clone(&route.quarantine_snapshot),
        )?;
        Ok(selected.map(|mut selected| {
            selected.admission_started = route.admission_started;
            selected
        }))
    })
    .await
}

enum DecodedCustomArgument {
    One(JsonValue),
    WrongCount,
}

struct CustomArgumentVisitor;

impl<'de> serde::de::Visitor<'de> for CustomArgumentVisitor {
    type Value = DecodedCustomArgument;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a function argument array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let Some(argument) = sequence.next_element()? else {
            return Ok(DecodedCustomArgument::WrongCount);
        };
        let has_extra = sequence.next_element::<serde::de::IgnoredAny>()?.is_some();
        while sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {}
        Ok(if has_extra {
            DecodedCustomArgument::WrongCount
        } else {
            DecodedCustomArgument::One(argument)
        })
    }
}

fn custom_argument(arguments: &SerializedArgs) -> anyhow::Result<JsonValue> {
    let mut deserializer = serde_json::Deserializer::from_str(arguments.get());
    let argument =
        serde::de::Deserializer::deserialize_seq(&mut deserializer, CustomArgumentVisitor)
            .and_then(|argument| {
                deserializer.end()?;
                Ok(argument)
            })
            .context(ErrorMetadata::bad_request(
                "InvalidArguments",
                "Invalid arguments provided",
            ))?;
    match argument {
        DecodedCustomArgument::One(argument) => Ok(argument),
        DecodedCustomArgument::WrongCount => {
            anyhow::bail!("Static Hermes route expects one argument object")
        },
    }
}

#[cfg(test)]
#[test]
fn runtime_hot_path_custom_argument_decodes_one_value_and_rejects_other_arities(
) -> anyhow::Result<()> {
    let argument = SerializedArgs::from_slice(br#" [ {"value":1} ] "#)?;
    assert_eq!(custom_argument(&argument)?, json!({ "value": 1 }));

    for arguments in [
        SerializedArgs::from_args(vec![])?,
        SerializedArgs::from_args(vec![json!(1), json!(2)])?,
    ] {
        let error = custom_argument(&arguments).expect_err("invalid arity was accepted");
        assert!(error
            .to_string()
            .contains("Static Hermes route expects one argument object"));
    }
    Ok(())
}

pub(super) fn arm_generated_timeout<RT: Runtime>(
    rt: RT,
    state: &mut HostState<RT>,
    timeout: Duration,
    system_timeout: Duration,
) -> anyhow::Result<()> {
    let interrupt = Arc::clone(&generated_state(state).map_err(wasmtime_anyhow)?.interrupt);
    #[cfg(any(test, feature = "testing"))]
    interrupt.record_timeout_arm();
    let current_timeout = state.timeout.take().context("gate timeout missing")?;
    let permit = current_timeout.finish_with_permit()?;
    state.timeout = Some(Timeout::new_with_termination(
        rt.clone(),
        Some(timeout),
        Some(system_timeout),
        permit,
        move |reason| interrupt.terminate(reason),
    ));
    state.backend_timeout_armed = true;
    Ok(())
}

pub(super) fn rearm_generated_active_timeout<RT: Runtime>(
    state: &mut HostState<RT>,
    timeout: Duration,
) -> anyhow::Result<()> {
    let interrupt = Arc::clone(&generated_state(state).map_err(wasmtime_anyhow)?.interrupt);
    state
        .timeout
        .as_mut()
        .context("generated Wasm timeout disappeared before activation")?
        .rearm_static_hermes_user_timeout(timeout, || {
            interrupt.set_execution_phase(GeneratedExecutionPhase::Active);
        })
}

fn generated_route_requires_prepared_instance(
    routed: &GeneratedRoutedModule,
    reuse_instances: bool,
) -> bool {
    reuse_instances
        && routed.generation.as_ref().is_some_and(|generation| {
            !generation.is_retired()
                && route_configuration()
                    .expect("initialized route configuration disappeared")
                    .deployment_registry
                    .as_ref()
                    .and_then(|registry| registry.source_keyed_residency.as_ref())
                    .is_some_and(|residency| residency.owns_replenishment(generation))
        })
}

pub(super) async fn acquire_generated_memory_permit<RT: Runtime>(
    memory_controller: &Arc<GeneratedMemoryController>,
    identity: FunctionMemoryIdentity,
    route_context: Option<&GeneratedMemoryRouteContext>,
    routed: &Arc<GeneratedRoutedModule>,
    reuse_instances: bool,
    reusable_instance: &mut Option<GeneratedReusableInstance<RT>>,
    deadline: tokio::time::Instant,
    cancellation: &CancellationSignal,
    metrics: &GateMetrics,
    immediate: bool,
    class: ActiveJavascriptClass,
    primary_kind: Option<ManifestUdfKind>,
) -> anyhow::Result<InvocationMemoryPermit> {
    // A full backlog must not change a cancellation or an expired setup into
    // a new queue admission outcome.
    if cancellation.is_cancelled() {
        anyhow::bail!("generated Wasm admission cancelled");
    }
    if tokio::time::Instant::now() >= deadline {
        memory_controller.record_admission_timeout();
        return Err(StaticHermesWasmAdmissionFailure::Memory.overloaded());
    }
    let mut ticket = if immediate {
        None
    } else {
        let controller = memory_controller.clone();
        let pending_routed = routed.clone();
        let pending_identity = identity.clone();
        let pending_context = route_context.cloned();
        let pending_cancellation = cancellation.clone();
        Some(memory_controller.routed_admission.enter(
            class,
            deadline,
            Box::new(move |pool| {
                if pending_cancellation.is_cancelled() {
                    return false;
                }
                let slot = if reuse_instances {
                    pool.matching_memory_slot::<RT>(&pending_routed)
                } else {
                    None
                };
                if generated_route_requires_prepared_instance(&pending_routed, reuse_instances)
                    && slot.is_none()
                {
                    return false;
                }
                // Permanent failures also need a turn so their owner can report
                // the error promptly instead of waiting for an unrelated release.
                !matches!(
                    controller.check_routed_admission(
                        &pending_identity,
                        slot,
                        pending_context.as_ref()
                    ),
                    Err(TryAdmissionError::Wait(_))
                )
            }),
        )?)
    };
    let queue_wake = ticket.as_ref().map(|ticket| ticket.wake.clone());
    let mut waited = false;
    loop {
        let retirement = GENERATED_GENERATION_RETIREMENT_NOTIFY.notified();
        tokio::pin!(retirement);
        let _ = retirement.as_mut().enable();
        // A request may have captured this exact generation before residency
        // retirement. Once that incarnation is fenced, it may reconstruct a
        // Store for the captured route; it must never wait for a replacement
        // incarnation to satisfy the old pointer.
        let prepared_only = generated_route_requires_prepared_instance(routed, reuse_instances);
        if cancellation.is_cancelled() {
            anyhow::bail!("generated Wasm admission cancelled");
        }
        if tokio::time::Instant::now() >= deadline {
            memory_controller.record_admission_timeout();
            return Err(StaticHermesWasmAdmissionFailure::Memory.overloaded());
        }
        let notification = memory_controller.change_notification();
        tokio::pin!(notification);
        let _ = notification.as_mut().enable();
        let queue_changed = memory_controller.routed_admission.changed();
        tokio::pin!(queue_changed);
        queue_changed.as_mut().enable();
        let attempt = {
            // Pool return holds this lock until it publishes both the physical
            // instance and its idle ledger state. Pairing checkout and this
            // single admission attempt under the same lock prevents a caller
            // from creating a new slot while a route-matched instance returns.
            let mut scheduling = memory_controller.routed_admission.lock();
            let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
            if cancellation.is_cancelled() {
                anyhow::bail!("generated Wasm admission cancelled");
            }
            if tokio::time::Instant::now() >= deadline {
                memory_controller.record_admission_timeout();
                return Err(StaticHermesWasmAdmissionFailure::Memory.overloaded());
            }
            let existing_slot = if reuse_instances {
                pool.matching_memory_slot::<RT>(routed)
            } else {
                None
            };
            #[cfg(any(test, feature = "testing"))]
            let matching_pool_identity_available = existing_slot.is_some();
            let eligibility = if prepared_only && existing_slot.is_none() {
                if immediate {
                    return Err(StaticHermesQueryShadowCapacityUnavailable.into());
                }
                if !preparation::PREPARATION_STARTED.load(Ordering::Acquire) {
                    anyhow::bail!("generated Wasm preparation worker is unavailable");
                }
                if let Some(error) = preparation::failed_preparation(routed) {
                    return Err(error);
                }
                preparation::observe_saturation(routed);
                Err(TryAdmissionError::Wait(
                    AdmissionWaitReason::SlotUnavailable,
                ))
            } else if scheduling.has_competing_waiters(ticket.as_ref()) {
                memory_controller.check_routed_admission(&identity, existing_slot, route_context)
            } else {
                // With no competing waiter, the actual reservation can decide
                // eligibility. Avoid planning the same warm grant twice. Failed
                // reservations do not spend a class turn or retain a Store.
                Ok(())
            };
            let turn = match &ticket {
                Some(ticket) if eligibility.is_ok() => {
                    scheduling.primary_turn(ticket, &pool, true)?
                },
                Some(_) => None,
                None => None,
            };
            let may_admit = if immediate {
                eligibility.is_ok() && scheduling.shadow_turn(&pool)
            } else {
                turn.is_some()
            };
            if eligibility.is_ok() && !may_admit {
                None
            } else {
                let mut candidate = if may_admit {
                    take_generated_routed_instance_locked(&mut pool, routed, reuse_instances)
                } else {
                    None
                };
                let admission = eligibility.and_then(|()| {
                    memory_controller.try_admit_routed(
                        identity.clone(),
                        existing_slot,
                        route_context,
                    )
                });
                match admission {
                    Ok(mut permit) => {
                        // Transfer idle capacity to observed occupancy while
                        // the pool is locked; preparation must never see a gap.
                        // The memory permit keeps this owner through teardown.
                        permit.demand_observation =
                            preparation::observe_demand(routed, primary_kind);
                        permit.routed_admission = Some(match (&mut ticket, turn) {
                            (Some(ticket), Some(policy)) => scheduling.granted(ticket, policy),
                            (None, None) => scheduling.granted_shadow(),
                            _ => unreachable!("Wasm memory admitted without a scheduling turn"),
                        });
                        scheduling.wake_next(&pool);
                        if reuse_instances {
                            log_generated_instance_pool_checkout(metrics, candidate.is_some());
                            #[cfg(any(test, feature = "testing"))]
                            metrics.record_generated_pool_event(
                                candidate.as_ref().map(|instance| instance.id),
                                candidate
                                    .as_ref()
                                    .map(|instance| instance.memory_slot_id.test_identity()),
                                StaticHermesGeneratedPoolEvent::Checkout {
                                    matching_pool_identity_available,
                                    reused_runtime: candidate.is_some(),
                                },
                            );
                        }
                        *reusable_instance = candidate;
                        Some(Ok(permit))
                    },
                    Err(error) => {
                        #[cfg(any(test, feature = "testing"))]
                        let event = match error {
                            TryAdmissionError::Permanent(_) => {
                                StaticHermesGeneratedPoolEvent::AdmissionRejected {
                                    matching_pool_identity_available,
                                }
                            },
                            TryAdmissionError::Wait(reason) => {
                                StaticHermesGeneratedPoolEvent::AdmissionWait {
                                    matching_pool_identity_available,
                                    reason: static_hermes_generated_pool_admission_wait_reason(
                                        reason,
                                    ),
                                }
                            },
                        };
                        #[cfg(any(test, feature = "testing"))]
                        metrics.record_generated_pool_event(
                            candidate.as_ref().map(|instance| instance.id),
                            candidate
                                .as_ref()
                                .map(|instance| instance.memory_slot_id.test_identity()),
                            event,
                        );
                        if let Some(mut instance) = candidate.take() {
                            instance.pool_checked_out = false;
                            pool.insert(instance);
                        }
                        // Live shared backing can invalidate advisory eligibility
                        // without a release notification. Hand off to a smaller
                        // eligible waiter after restoring the candidate Store.
                        let eligible_waiter = scheduling.wake_next(&pool);
                        if eligible_waiter && matches!(error, TryAdmissionError::Wait(_)) {
                            // Reclamation must yield to eligible demand too:
                            // evicting its idle Store here would undo the handoff
                            // before the selected caller can claim that Store.
                            None
                        } else {
                            Some(Err(error))
                        }
                    },
                }
            }
        };
        match attempt {
            None if immediate => return Err(StaticHermesQueryShadowCapacityUnavailable.into()),
            None => {},
            Some(Ok(permit)) => {
                if waited {
                    memory_controller.record_admission_wait_completed();
                }
                return Ok(permit);
            },
            Some(Err(TryAdmissionError::Permanent(
                AdmissionRejection::ForecastExceedsHardBudget,
            ))) => {
                memory_controller.record_admission_rejection();
                anyhow::bail!("generated Wasm invocation forecast exceeds its hard memory budget")
            },
            Some(Err(TryAdmissionError::Wait(reason))) => {
                // Optional work never evicts a retained instance or waits for
                // preparation merely to obtain comparison capacity.
                if immediate {
                    return Err(StaticHermesQueryShadowCapacityUnavailable.into());
                }
                if matches!(
                    reason,
                    AdmissionWaitReason::SoftBudget | AdmissionWaitReason::HardBudget
                ) && let Some(generation) = routed.generation.as_ref()
                    && let Some(residency) = route_configuration()?
                        .deployment_registry
                        .as_ref()
                        .and_then(|registry| registry.source_keyed_residency.as_ref())
                    && residency.retire_other_generation_for_admission(generation)
                {
                    continue;
                }
                if !waited {
                    memory_controller.record_admission_wait(reason);
                    waited = true;
                }
                match reason {
                    AdmissionWaitReason::InstanceCountCeiling => {
                        if evict_generated_idle_instance_for_new_slot::<RT>(
                            memory_controller,
                            routed,
                            reuse_instances,
                            IdleEvictionLimit {
                                deadline,
                                cancellation: Some(cancellation),
                            },
                        )
                        .await
                        .map_err(|error| {
                            generated_admission_idle_eviction_error(memory_controller, error)
                        })? {
                            continue;
                        }
                    },
                    AdmissionWaitReason::SoftBudget | AdmissionWaitReason::HardBudget => {
                        let evicted = {
                            let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
                            let candidate = if reuse_instances {
                                pool.take::<RT>(routed)
                            } else {
                                None
                            };
                            let existing_slot =
                                candidate.as_ref().map(|instance| instance.memory_slot_id);
                            let mut instances = pool
                                .iter()
                                .filter_map(|instance| {
                                    instance.downcast_ref::<GeneratedReusableInstance<RT>>()
                                })
                                .collect::<Vec<_>>();
                            instances.sort_by_key(|instance| instance.idle_since);
                            let candidates = instances
                                .iter()
                                .map(|instance| {
                                    (instance.memory_slot_id, instance.idle_since.elapsed())
                                })
                                .collect::<Vec<_>>();
                            let selected = memory_controller.invocation_eviction_candidates(
                                &identity,
                                existing_slot,
                                route_context,
                                &candidates,
                            );
                            if let Some(instance) = candidate {
                                pool.insert(instance);
                            }
                            selected
                                .into_iter()
                                .map(|(slot, reason)| {
                                    (
                                        pool.take_slot::<RT>(slot)
                                            .expect("admission eviction lost its selected Store"),
                                        reason,
                                    )
                                })
                                .collect()
                        };
                        if discard_generated_idle_instances(
                            memory_controller,
                            evicted,
                            IdleEvictionTrigger::AdmissionBudget,
                            IdleEvictionLimit {
                                deadline,
                                cancellation: Some(cancellation),
                            },
                        )
                        .await
                        .map_err(|error| {
                            generated_admission_idle_eviction_error(memory_controller, error)
                        })? {
                            continue;
                        }
                    },
                    AdmissionWaitReason::Pressure
                    | AdmissionWaitReason::ActiveCountCeiling
                    | AdmissionWaitReason::SlotUnavailable
                    | AdmissionWaitReason::FunctionRecordCapacity => {},
                }
            },
        }
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                anyhow::bail!("generated Wasm admission cancelled")
            },
            _ = &mut retirement => {},
            _ = tokio::time::sleep_until(deadline) => {
                memory_controller.record_admission_timeout();
                return Err(StaticHermesWasmAdmissionFailure::Memory.overloaded());
            },
            _ = &mut notification => {},
            _ = &mut queue_changed => {},
            _ = async {
                match &queue_wake {
                    Some(wake) => wake.notified().await,
                    None => std::future::pending::<()>().await,
                }
            } => {},
        }
    }
}

fn generated_admission_idle_eviction_error(
    memory_controller: &GeneratedMemoryController,
    error: IdleEvictionError,
) -> anyhow::Error {
    match error {
        IdleEvictionError::Cancelled => anyhow::anyhow!("generated Wasm admission cancelled"),
        IdleEvictionError::Cleanup(error) => error,
        IdleEvictionError::Deadline => {
            memory_controller.record_admission_timeout();
            StaticHermesWasmAdmissionFailure::Memory.overloaded()
        },
    }
}

pub(super) async fn acquire_generated_routed_memory_permit<RT: Runtime>(
    memory_controller: &Arc<GeneratedMemoryController>,
    identity: FunctionMemoryIdentity,
    route_context: Option<&GeneratedMemoryRouteContext>,
    routed: &Arc<GeneratedRoutedModule>,
    reuse_instances: bool,
    reusable_instance: &mut Option<GeneratedReusableInstance<RT>>,
    deadline: tokio::time::Instant,
    cancellation: &CancellationSignal,
    metrics: &GateMetrics,
    immediate: bool,
    class: ActiveJavascriptClass,
    primary_kind: Option<ManifestUdfKind>,
) -> anyhow::Result<InvocationMemoryPermit> {
    let _timer = GatePhaseTimer::new(GATE_PHASE_RETAINED_POOL_ADMISSION_CHECKOUT);
    let permit = acquire_generated_memory_permit(
        memory_controller,
        identity,
        route_context,
        routed,
        reuse_instances,
        reusable_instance,
        deadline,
        cancellation,
        metrics,
        immediate,
        class,
        primary_kind,
    )
    .await?;
    Ok(permit)
}

pub(super) async fn acquire_active_wasm_cpu_permit(
    limiter: &ConcurrencyLimiter,
    cancellation: &CancellationSignal,
    immediate: bool,
    deadline: tokio::time::Instant,
    class: ActiveJavascriptClass,
) -> anyhow::Result<ConcurrencyPermit> {
    let wait_started = Instant::now();
    if cancellation.is_cancelled() {
        log_active_wasm_cpu_admission("cancelled", wait_started.elapsed());
        anyhow::bail!("generated Wasm active CPU admission cancelled")
    }
    if tokio::time::Instant::now() >= deadline {
        log_active_wasm_cpu_admission("timeout", wait_started.elapsed());
        return Err(StaticHermesWasmAdmissionFailure::Cpu.overloaded());
    }
    let permit = if immediate {
        let Some(permit) = limiter.try_acquire(Arc::clone(&ACTIVE_WASM_CPU_ADMISSION_CLIENT_ID))
        else {
            log_active_wasm_cpu_admission("capacity_unavailable", wait_started.elapsed());
            return Err(StaticHermesQueryShadowCapacityUnavailable.into());
        };
        permit
    } else {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                log_active_wasm_cpu_admission("cancelled", wait_started.elapsed());
                anyhow::bail!("generated Wasm active CPU admission cancelled")
            },
            _ = tokio::time::sleep_until(deadline) => {
                log_active_wasm_cpu_admission("timeout", wait_started.elapsed());
                return Err(StaticHermesWasmAdmissionFailure::Cpu.overloaded());
            },
            permit = limiter.acquire_with_class(
                Arc::clone(&ACTIVE_WASM_CPU_ADMISSION_CLIENT_ID),
                class,
                ConcurrencyPermitPhase::Initial,
            ) => permit,
        }
    };
    // An immediately-ready grant can finish after a synchronous critical
    // section crosses the deadline. Drop that grant before returning rejection.
    if cancellation.is_cancelled() {
        log_active_wasm_cpu_admission("cancelled", wait_started.elapsed());
        anyhow::bail!("generated Wasm active CPU admission cancelled")
    }
    if tokio::time::Instant::now() >= deadline {
        log_active_wasm_cpu_admission("timeout", wait_started.elapsed());
        return Err(StaticHermesWasmAdmissionFailure::Cpu.overloaded());
    }
    log_active_wasm_cpu_admission("admitted", wait_started.elapsed());
    Ok(permit)
}

pub(super) async fn verify_deployed_source_identity<RT: Runtime>(
    transaction: &mut Transaction<RT>,
    module_path: &CanonicalizedModulePath,
    expected: &DeployedRuntimeIdentity,
) -> anyhow::Result<()> {
    let module = ModuleModel::new(transaction)
        .get_metadata(CanonicalizedComponentModulePath {
            component: ComponentId::Root,
            module_path: module_path.clone(),
        })
        .await?
        .context("generated Wasm route source module is not active")?;
    anyhow::ensure!(
        module.sha256.as_hex() == expected.module_sha256(),
        "generated Wasm route source module identity does not match the active transaction"
    );
    // Generated artifacts authenticate a complete package, not a per-route JS
    // import closure. Until that closure is part of the authenticated contract,
    // track every module (including the empty range for additions/deletions).
    // Unchanged rows survive generation-only publication. Artifact/engine changes
    // must implement the same JS semantics; activation identity is not result data.
    ModuleModel::new(transaction).record_package_metadata_dependency(ComponentId::Root)?;
    let source_package = SourcePackageModel::new(transaction, TableNamespace::Global)
        // Convex keeps unchanged module metadata pointing at an older compatible
        // package. Deployment identity must still bind the newest package visible
        // to this transaction.
        .get_latest_for_runtime_selection()
        .await?
        .context("generated Wasm route source package is not active")?;
    // The artifact authenticates the published package, not an independently
    // edited transaction-local graph. Even an unchanged entry can import a
    // changed or deleted pending module. Publication itself does not execute
    // routes; ad-hoc packages select V8 because they have no paired generation.
    anyhow::ensure!(
        !ModuleModel::new(transaction).has_pending_module(ComponentId::Root),
        "generated Wasm source identity cannot authenticate pending module changes"
    );
    if let Some(expected_sha256) = expected.source_package_archive_sha256() {
        anyhow::ensure!(
            source_package.sha256.as_hex() == expected_sha256,
            "generated Wasm route source package archive identity does not match the active \
             transaction"
        );
    } else {
        let expected_sha256 = expected
            .source_package_runtime_content_sha256()
            .context("generated Wasm route has no source-package identity")?;
        let active_sha256 = source_package.runtime_content_sha256.as_ref().context(
            "generated Wasm route source package has no persisted runtime-content identity",
        )?;
        anyhow::ensure!(
            active_sha256.as_hex() == expected_sha256,
            "generated Wasm route source package runtime-content identity does not match the \
             active transaction"
        );
    }
    Ok(())
}

async fn authenticate_route_source_identity<RT: Runtime>(
    route: &StaticHermesWasmtimeRouteHandle,
    udf_type: UdfType,
    path_and_args: &ValidatedPathAndArgs,
    transaction: &mut Transaction<RT>,
) -> anyhow::Result<()> {
    route.validate_invocation(udf_type, path_and_args)?;
    if let StaticHermesWasmtimeRoute::GeneratedDeployment(deployment_route) = &route.route {
        verify_deployed_source_identity(
            transaction,
            path_and_args.path().udf_path.module(),
            &deployment_route.deployed_runtime_identity,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn prepare_routed_invocation<RT: Runtime>(
    route: StaticHermesWasmtimeRouteHandle,
    udf_type: UdfType,
    path_and_args: ValidatedPathAndArgs,
    mut transaction: Transaction<RT>,
    mode: StaticHermesWasmtimePreparationMode,
) -> anyhow::Result<PreparedStaticHermesWasmtimeInvocation<RT>> {
    let deadline = route.admission_deadline()?;
    within_static_hermes_admission(deadline, async {
        anyhow::ensure!(
            !matches!(
                &route.route,
                StaticHermesWasmtimeRoute::SourceKeyedDeployment(_)
            ),
            "source-keyed Wasm route was not selected before invocation preparation"
        );
        #[cfg(any(test, feature = "testing"))]
        record_generated_route_preflight_stage(
            StaticHermesGeneratedRoutePreflightStage::InvocationPreparationEntered,
        );
        {
            let _timer =
                GatePhaseTimer::new(GATE_PHASE_INVOCATION_PREPARATION_SOURCE_AUTHENTICATION);
            authenticate_route_source_identity(&route, udf_type, &path_and_args, &mut transaction)
                .await?;
        }
        #[cfg(any(test, feature = "testing"))]
        if matches!(
            &route.route,
            StaticHermesWasmtimeRoute::GeneratedDeployment(_)
        ) {
            record_generated_route_preflight_stage(
                StaticHermesGeneratedRoutePreflightStage::SourceIdentityVerified,
            );
        }
        let routed = prepared_routed_module(&route, mode)?;
        let selectors = match &routed {
            PreparedRoutedModule::Deferred => BTreeSet::new(),
            PreparedRoutedModule::Configured(routed) | PreparedRoutedModule::TestHook(routed) => {
                authorized_host_secret_selectors_for_routed_module(routed)?
            },
        };
        Ok(PreparedStaticHermesWasmtimeInvocation {
            route,
            path_and_args,
            transaction,
            routed,
            host_secret_selectors: selectors,
            memory_observer: None,
        })
    })
    .await
}

#[track_caller]
fn classify_pre_execution_opaque_value_error(error: OpaqueValueError) -> anyhow::Error {
    // These failures occur before GeneratedInvocationState installs its
    // first-failure latch. Retain a closed category and compiled operation site;
    // the original error can contain a guest-provided field name.
    let failure = StaticHermesWasmExecutionFailure::from(&error);
    let diagnostic = StaticHermesWasmHostInvariantDiagnostic::from_opaque_at(
        &error,
        std::panic::Location::caller(),
    );
    anyhow::Error::new(HostInvariant)
        .context(diagnostic)
        .context(failure)
}

async fn run_generated_routed<RT: Runtime>(
    rt: RT,
    route: &StaticHermesWasmtimeRouteHandle,
    prepared_routed: PreparedRoutedModule,
    nested_udf_callback: IsolateClient<RT>,
    client_id: String,
    reactor_depth: usize,
    udf_type: UdfType,
    path_and_args: ValidatedPathAndArgs,
    transaction: Transaction<RT>,
    journal: QueryJournal,
    context: ExecutionContext,
    environment_data: EnvironmentData<RT>,
    rng_seed: [u8; 32],
    unix_timestamp: UnixTimestamp,
    cancellation: CancellationSignal,
    function_started_sender: Option<oneshot::Sender<()>>,
    capture_handler_reads: bool,
    shadow_work_guard: Option<Arc<OwnedSemaphorePermit>>,
    trace_host_operations: bool,
    memory_observer: Option<udf::wasm_memory::WasmMemoryObserver>,
    cpu_class: ActiveJavascriptClass,
) -> anyhow::Result<(Transaction<RT>, FunctionOutcome)> {
    let shadow = shadow_work_guard.is_some();
    macro_rules! try_checked_out_instance_with_active_cpu_permit {
        ($state:expr, $memory_permit:expr, $instance:expr, $result:expr $(,)?) => {{
            match $result {
                Ok(value) => value,
                Err(error) => {
                    let error: anyhow::Error = error.into();
                    let cpu_permit = $state
                        .timeout
                        .as_ref()
                        .and_then(|timeout| timeout.permit.as_ref())
                        .context("generated Wasm invocation lost its active CPU permit")?;
                    let error = discard_optional_generated_instance_while_cpu_permit_held(
                        $instance, cpu_permit, error,
                    )
                    .await;
                    $memory_permit.finish_unstarted(false);
                    return Err(error);
                },
            }
        }};
    }

    let uses_test_route = matches!(&prepared_routed, PreparedRoutedModule::TestHook(_));
    let reuse_instances = uses_test_route || route_configuration()?.reuse_instances;
    let routed = match prepared_routed {
        PreparedRoutedModule::Configured(routed) | PreparedRoutedModule::TestHook(routed) => routed,
        PreparedRoutedModule::Deferred => load_generated_routed_module(
            route,
            if shadow {
                StaticHermesWasmtimePreparationMode::Shadow
            } else {
                StaticHermesWasmtimePreparationMode::Primary
            },
        )?,
    };
    let route_configuration = route_configuration()?;
    let memory_controller = Arc::clone(&route_configuration.generated_memory_controller);
    let (routed_udf_kind, entry_selector) = if uses_test_route {
        (
            routed
                .package_identity
                .legacy_udf_kind()
                .context("generated test route must use a legacy execution manifest")?,
            routed.entry_selector,
        )
    } else {
        match &route.route {
            StaticHermesWasmtimeRoute::GeneratedSingleton => (
                routed
                    .package_identity
                    .legacy_udf_kind()
                    .context("generated singleton route must use a legacy execution manifest")?,
                routed.entry_selector,
            ),
            StaticHermesWasmtimeRoute::GeneratedDeployment(deployment_route) => {
                anyhow::ensure!(
                    deployment_route.route_lease.export_name() == deployment_route.export_name
                        && deployment_route.route_lease.udf_kind() == deployment_route.udf_kind,
                    "generated Wasm route lease differs from the pinned invocation route"
                );
                (
                    deployment_route.route_lease.udf_kind(),
                    deployment_route.route_lease.entry_selector(),
                )
            },
            StaticHermesWasmtimeRoute::SourceKeyedDeployment(_) => {
                anyhow::bail!("source-keyed Wasm route was not selected before execution")
            },
        }
    };
    let expected_udf_type = match routed_udf_kind {
        ManifestUdfKind::Query => UdfType::Query,
        ManifestUdfKind::Mutation => UdfType::Mutation,
    };
    anyhow::ensure!(
        udf_type == expected_udf_type,
        "generated package UDF kind does not match the routed invocation"
    );
    let allows_caught_official_output_chunk_initialization_failure =
        entry_selector.is_some_and(|entry_selector| {
            matches!(
                &*routed.package_identity,
                ValidatedWasmUdfPackageIdentity::CapabilityEntry(identity)
                    if identity.official_output_chunk_entry_slot(entry_selector).is_some()
            )
        });
    let mut environment_data = environment_data;
    // Move this map into the invocation Store so clearing the Store state also
    // releases the request's only long-lived host-secret buffer ownership.
    let host_secret_values = environment_data
        .host_secret_values
        .take()
        .context("generated Wasm route did not receive top-level host-secret state")?;
    let request = custom_argument(path_and_args.args())?;
    #[cfg(any(test, feature = "testing"))]
    let metrics = TEST_HOOKS
        .lock()
        .as_ref()
        .and_then(Weak::upgrade)
        .map_or_else(
            || Arc::new(GateMetrics::default()),
            |hooks| Arc::clone(&hooks.metrics),
        );
    #[cfg(not(any(test, feature = "testing")))]
    let metrics = Arc::clone(&PRODUCTION_GATE_METRICS);
    let (environment, args) = DatabaseUdfEnvironment::new(
        rt.clone(),
        UdfRequest {
            file_downloads: file_storage::FileDownloads::default(),
            udf_type,
            path_and_args,
            transaction,
            unix_timestamp,
            journal,
            context,
            environment_data,
            trace_host_operations: shadow || trace_host_operations,
            capture_handler_reads,
            shadow_work_guard: shadow_work_guard.clone(),
        },
        reactor_depth,
        client_id,
        rng_seed,
    );
    let mut provider = DatabaseUdfWasmInvocation::new(environment, args);
    let maximum_handles = usize::try_from(routed.manifest.limits().max_value_handles())?;
    let maximum_host_owned_bytes =
        usize::try_from(routed.manifest.limits().max_host_owned_bytes())?;
    let maximum_guest_memory_bytes =
        usize::try_from(routed.manifest.limits().max_guest_memory_bytes())?;
    let mut reusable_instance = None;
    let mut invalidated_reusable_instance = None;
    let admission_deadline = route.admission_deadline()?;
    let route_context = match (
        route.authenticated_generation_sha256(),
        route.authenticated_route_id(),
    ) {
        (Some(generation_sha256), Some(route_sha256)) => Some(GeneratedMemoryRouteContext {
            identity: GeneratedMemoryRouteIdentity {
                generation_sha256: generation_sha256.to_owned(),
                route_sha256: route_sha256.to_owned(),
                udf_kind: routed_udf_kind,
            },
            role: if shadow {
                GeneratedMemoryExecutionRole::Shadow
            } else {
                GeneratedMemoryExecutionRole::Primary
            },
        }),
        // Legacy deployment leases can authenticate a generation without a
        // route digest. Diagnostics must not reject those executable routes
        // or manufacture a telemetry identity for them.
        (_, None) => None,
        (None, Some(_)) => {
            anyhow::bail!("generated Wasm memory route identity is incomplete")
        },
    };
    let mut memory_permit = acquire_generated_routed_memory_permit(
        &memory_controller,
        generated_memory_identity_for_package(
            &routed.manifest,
            &routed.package_identity,
            &routed.route_identity,
        ),
        route_context.as_ref(),
        &routed,
        reuse_instances,
        &mut reusable_instance,
        admission_deadline,
        &cancellation,
        &metrics,
        shadow,
        cpu_class,
        (!shadow && reactor_depth == 0).then_some(routed_udf_kind),
    )
    .await?;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::MemoryAdmitted,
    );
    #[cfg(any(test, feature = "testing"))]
    let active_wasm_cpu_limiter = if uses_test_route {
        TEST_HOOKS
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .and_then(|hooks| hooks.generated_active_wasm_cpu_limiter())
            .unwrap_or_else(|| ConcurrencyLimiter::new_for_wasm(usize::MAX, 0, 0))
    } else {
        route_configuration
            .active_wasm_cpu_limiter
            .clone()
            .context("Static Hermes Wasm routing has no active CPU limiter")?
    };
    #[cfg(not(any(test, feature = "testing")))]
    let active_wasm_cpu_limiter = route_configuration
        .active_wasm_cpu_limiter
        .clone()
        .context("Static Hermes Wasm routing has no active CPU limiter")?;
    if let Some(instance) = reusable_instance.take() {
        let Some(generated) = instance.store.data().generated.as_ref() else {
            let error =
                anyhow::anyhow!("pooled generated Store lost its completed invocation state");
            discard_generated_routed_instance_after_cpu_admission_rejection(
                rt.clone(),
                instance,
                memory_permit,
                shadow_work_guard,
            );
            return Err(error);
        };
        if !generated.capability_bridge.is_revoked() || !generated.async_operations.is_empty() {
            let error = anyhow::anyhow!(
                "pooled generated Store retained authority or an operation before the next \
                 invocation"
            );
            discard_generated_routed_instance_after_cpu_admission_rejection(
                rt.clone(),
                instance,
                memory_permit,
                shadow_work_guard,
            );
            return Err(error);
        }
        let Some(read_set) = instance.context_read_set.as_ref() else {
            let error = anyhow::anyhow!("pooled generated runtime has no initialization read set");
            discard_generated_routed_instance_after_cpu_admission_rejection(
                rt.clone(),
                instance,
                memory_permit,
                shadow_work_guard,
            );
            return Err(error);
        };
        enum ReadSetValidation {
            Cancelled,
            Complete(anyhow::Result<bool>),
            Deadline,
        }
        // Isolate-thread callers release their permit around this range hashing.
        // This route has not acquired normal execution CPU yet, so validation
        // neither holds nor needs to regain an execution permit.
        let _validation_timer =
            GatePhaseTimer::new(GATE_PHASE_RETAINED_CONTEXT_READ_SET_VALIDATION);
        let validation = match provider.tx_for_initialization() {
            Ok(initialization_tx) => {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => ReadSetValidation::Cancelled,
                    _ = tokio::time::sleep_until(admission_deadline) => {
                        ReadSetValidation::Deadline
                    },
                    result = ContextCache::validate_and_apply_context_read_set(
                        initialization_tx,
                        read_set,
                    ) => {
                        ReadSetValidation::Complete(result)
                    },
                }
            },
            Err(error) => ReadSetValidation::Complete(Err(error)),
        };
        match validation {
            ReadSetValidation::Cancelled => {
                // Re-pool a live generation synchronously, but never make a
                // cancelled caller wait for retired guest teardown to regain
                // shared execution CPU.
                return_optional_generated_routed_instance_after_cpu_admission_rejection(
                    rt.clone(),
                    Some(instance),
                    memory_permit,
                    shadow_work_guard,
                );
                anyhow::bail!("generated Wasm initialization read-set validation cancelled");
            },
            ReadSetValidation::Deadline => {
                memory_controller.record_admission_timeout();
                return_optional_generated_routed_instance_after_cpu_admission_rejection(
                    rt.clone(),
                    Some(instance),
                    memory_permit,
                    shadow_work_guard,
                );
                return Err(StaticHermesWasmAdmissionFailure::Deadline.overloaded());
            },
            ReadSetValidation::Complete(Ok(true)) => {
                #[cfg(any(test, feature = "testing"))]
                record_generated_route_preflight_stage(
                    StaticHermesGeneratedRoutePreflightStage::WarmContextReadSetValidated,
                );
                reusable_instance = Some(instance);
            },
            ReadSetValidation::Complete(Ok(false)) => {
                #[cfg(any(test, feature = "testing"))]
                instance.store.data().metrics.record_generated_pool_event(
                    Some(instance.id),
                    Some(instance.memory_slot_id.test_identity()),
                    StaticHermesGeneratedPoolEvent::ContextReadSetRejected,
                );
                // Keep the stale Store checked out until normal execution CPU
                // is admitted. Its destroy export must run under that same
                // permit, and a rejected shadow must not queue its teardown.
                invalidated_reusable_instance = Some(instance);
            },
            ReadSetValidation::Complete(Err(error)) => {
                let error = error.context("generated initialization read-set validation failed");
                discard_generated_routed_instance_after_cpu_admission_rejection(
                    rt.clone(),
                    instance,
                    memory_permit,
                    shadow_work_guard,
                );
                return Err(error);
            },
        }
    }
    let mut active_wasm_cpu_permit = match acquire_active_wasm_cpu_permit(
        &active_wasm_cpu_limiter,
        &cancellation,
        shadow,
        admission_deadline,
        cpu_class,
    )
    .await
    {
        Ok(permit) => permit,
        Err(error) => {
            if let Some(instance) = invalidated_reusable_instance.take() {
                discard_generated_routed_instance_after_cpu_admission_rejection(
                    rt.clone(),
                    instance,
                    memory_permit,
                    shadow_work_guard,
                );
            } else {
                return_optional_generated_routed_instance_after_cpu_admission_rejection(
                    rt.clone(),
                    reusable_instance.take(),
                    memory_permit,
                    shadow_work_guard,
                );
            }
            return Err(error);
        },
    };
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::ActiveCpuAdmitted,
    );
    if let Some(instance) = invalidated_reusable_instance.take() {
        let discard_result = {
            let discard =
                discard_generated_instance_while_cpu_permit_held(instance, &active_wasm_cpu_permit);
            tokio::pin!(discard);
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    Err(anyhow::anyhow!(
                        "generated Wasm admission cancelled during stale runtime cleanup"
                    ))
                },
                _ = tokio::time::sleep_until(admission_deadline) => {
                    memory_controller.record_admission_timeout();
                    Err(StaticHermesWasmAdmissionFailure::Deadline.overloaded())
                },
                result = &mut discard => result,
            }
        };
        if let Err(error) = discard_result {
            memory_permit.finish_unstarted(false);
            return Err(error);
        }
        // Destruction ended the retained Store's lifetime. Its old capacity,
        // static credit and warm reservation cannot seed a fresh limiter.
        // Release CPU while admitting the replacement's cold requirement.
        memory_permit.finish_unstarted(false);
        drop(active_wasm_cpu_permit);
        memory_permit = acquire_generated_memory_permit(
            &memory_controller,
            generated_memory_identity_for_package(
                &routed.manifest,
                &routed.package_identity,
                &routed.route_identity,
            ),
            route_context.as_ref(),
            &routed,
            false,
            &mut reusable_instance,
            admission_deadline,
            &cancellation,
            &metrics,
            shadow,
            cpu_class,
            (!shadow && reactor_depth == 0).then_some(routed_udf_kind),
        )
        .await?;
        active_wasm_cpu_permit = match acquire_active_wasm_cpu_permit(
            &active_wasm_cpu_limiter,
            &cancellation,
            shadow,
            admission_deadline,
            cpu_class,
        )
        .await
        {
            Ok(permit) => permit,
            Err(error) => {
                memory_permit.finish_unstarted(false);
                return Err(error);
            },
        };
    }
    memory_permit.configure_route_statistics(route_context, reuse_instances);
    if let Some(memory_observer) = memory_observer {
        memory_permit.observe_completion(memory_observer);
    }
    if reusable_instance.is_none() {
        // A retained runtime can fail validation against this invocation's
        // transaction snapshot. Rebuild under that snapshot: background
        // preparation reads current configuration and cannot safely substitute
        // for an older snapshot. Ordinary pool misses wait for prepared capacity.
        if let Err(error) = provider.snoop_initialization_reads() {
            memory_permit.finish_unstarted(false);
            return Err(error);
        }
    }
    if let Some(sender) = function_started_sender {
        // Match V8 by publishing only after every execution-capacity admission
        // has succeeded. The CPU permit is retained in Timeout and released
        // around the asynchronous host waits below.
        _ = sender.send(());
    }
    let mut state = new_invocation_state(
        rt.clone(),
        provider,
        StaticHermesUdfCallback::Isolate(nested_udf_callback),
        None,
        Arc::clone(&metrics),
        active_wasm_cpu_permit,
    );
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::InvocationStateCreated,
    );
    let memory_observer = memory_permit.observer();
    let reusable_resources = reusable_instance.as_mut().map(|instance| {
        {
            let generated = instance
                .store
                .data_mut()
                .generated
                .as_mut()
                .context("pooled generated Store lost its completed invocation state")?;
            generated
                .values
                .begin_invocation(Arc::clone(&memory_observer))
                .map_err(classify_pre_execution_opaque_value_error)
                .context(
                    "pooled generated Store could not begin the next opaque-value invocation",
                )?;
            generated
                .async_operations
                .begin_invocation()
                .map_err(wasmtime_anyhow)?;
        }
        let generated = instance
            .store
            .data_mut()
            .generated
            .take()
            .expect("validated pooled generated invocation state disappeared");
        Ok::<_, anyhow::Error>((generated.values, generated.async_operations))
    });
    let reusable_resources = try_checked_out_instance_with_active_cpu_permit!(
        state,
        memory_permit,
        reusable_instance,
        reusable_resources.transpose(),
    );
    // On setup failure, drop the local table before the cleanup path finishes
    // admission. Insertion can fail after committing values and table capacity.
    let request_resources = (|| {
        let (mut values, async_operations) = reusable_resources.unwrap_or_else(|| {
            (
                OpaqueValueTable::new_with_observer(
                    maximum_handles,
                    maximum_host_owned_bytes,
                    Some(Arc::clone(&memory_observer)),
                ),
                AsyncOperationState::default(),
            )
        });
        let request_value = match routed.manifest.value_mode() {
            ValueMode::Opaque => OpaqueValue::ConvexJson(request),
            ValueMode::GuestNativeJson => OpaqueValue::Bytes(
                routed
                    .encode_invocation_arguments(request, state.provider.allows_pending_values())
                    .context("generated invocation arguments could not be encoded")?,
            ),
        };
        let request_handle = values
            .insert(request_value)
            .map_err(classify_pre_execution_opaque_value_error)
            .context("generated request exceeds its value-mode limits")?;
        Ok::<_, anyhow::Error>((values, async_operations, request_handle))
    })();
    let request_resources = try_checked_out_instance_with_active_cpu_permit!(
        state,
        memory_permit,
        reusable_instance,
        request_resources,
    );
    let (values, async_operations, request_handle) = request_resources;
    let capability_bridge = InvocationCapabilityBridge::unissued();
    let performance_monotonic_start = state.provider.rt().monotonic_now();
    let interrupt = Arc::new(GeneratedInterruptState::default());
    interrupt.set_execution_phase(GeneratedExecutionPhase::Preparing);
    state.generated = Some(GeneratedInvocationState {
        manifest: Arc::clone(&routed.manifest),
        values,
        async_operations,
        capability_bridge,
        performance_monotonic_start,
        performance_runtime_available: false,
        runtime_reuse_contaminated: false,
        allows_caught_official_output_chunk_initialization_failure,
        discard_after_caught_initialization_failure: false,
        context_read_set_required: true,
        request_handle: Some(request_handle),
        operation_count: 0,
        terminal_failure: None,
        cancellation,
        interrupt,
        memory_limiter: memory_permit.limiter(
            StoreLimitsBuilder::new()
                .memory_size(maximum_guest_memory_bytes)
                .instances(routed.store_instance_limit())
                .memories(1)
                .trap_on_grow_failure(true)
                .build(),
        ),
        memory_permit: Some(memory_permit),
        host_secret_values,
    });
    if let Err(error) = arm_generated_timeout(
        rt,
        &mut state,
        GENERATED_INITIALIZATION_TIMEOUT,
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    ) {
        let error = if let Some(cpu_permit) = state
            .timeout
            .as_ref()
            .and_then(|timeout| timeout.permit.as_ref())
        {
            discard_optional_generated_instance_while_cpu_permit_held(
                reusable_instance.take(),
                cpu_permit,
                error,
            )
            .await
        } else {
            discard_optional_generated_instance(reusable_instance.take(), error).await
        };
        // The Store is gone, but request values still belong to the new state.
        // Its field order drops them before the unstarted permit releases admission.
        drop(state);
        return Err(error);
    }

    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::ExecutionEntered,
    );
    let generated_output = execute_generated_with_lifecycle_barrier_and_entry_selector(
        Arc::clone(&routed),
        entry_selector,
        super::runtime_lifecycle::GeneratedExecutionPurpose::Invocation,
        state,
        reusable_instance,
        reuse_instances,
        route_configuration.lifecycle_barrier.clone(),
        None,
    )
    .await
    .map_err(|error| {
        if error.is::<StaticHermesWasmExecutionFailure>() {
            error
        } else {
            error.context(StaticHermesWasmExecutionFailure::RuntimeInitialization)
        }
    })?;
    let GeneratedExecutionOutput {
        invocation: output,
        reusable_instance,
        memory_permit,
        terminal_memory_outcome,
        system_error,
        ..
    } = generated_output;
    let processed = output.into_routed_result(system_error, shadow);
    match (processed, reusable_instance, memory_permit) {
        (Ok(output), Some(instance), Some(memory_permit)) => {
            // Retained capacity is separate from handler success; only cleanup
            // failure prevents delivering the completed result.
            return_generated_routed_instance(instance, memory_permit, terminal_memory_outcome)
                .await
                .context(StaticHermesWasmExecutionFailure::RuntimeCleanup)?;
            Ok(output)
        },
        (Ok(output), None, None) => Ok(output),
        (Err(error), Some(instance), Some(memory_permit)) => {
            let discard_result = discard_generated_instance(instance).await;
            memory_permit.finish(TerminalMemoryOutcome::SystemError, false);
            Err(match discard_result {
                Ok(()) => allow_disposed_wasm_primary_fallback(error),
                Err(cleanup_error) => error
                    .context(format!(
                        "generated Wasm runtime cleanup also failed: {cleanup_error:#}"
                    ))
                    .context(GeneratedNonRetryableFailure::Cleanup),
            })
        },
        // The lifecycle owner disposed the runtime and completed its memory
        // accounting before returning this failed invocation. Cleanup failures
        // exit above and must never acquire retry authority here.
        (Err(error), None, None) => Err(allow_disposed_wasm_primary_fallback(error)),
        (processed, reusable_instance, memory_permit) => {
            drop(processed);
            let cleanup_result = discard_optional_generated_instance(
                reusable_instance,
                anyhow::anyhow!("generated Wasm memory permit and reusable instance drifted"),
            )
            .await;
            if let Some(memory_permit) = memory_permit {
                memory_permit.finish(TerminalMemoryOutcome::SystemError, false);
            }
            Err(cleanup_result.context(StaticHermesWasmExecutionFailure::RuntimeCleanup))
        },
    }
}

pub(crate) async fn run_prepared_routed<RT: Runtime>(
    rt: RT,
    prepared: PreparedStaticHermesWasmtimeInvocation<RT>,
    nested_udf_callback: IsolateClient<RT>,
    client_id: String,
    reactor_depth: usize,
    udf_type: UdfType,
    journal: QueryJournal,
    context: ExecutionContext,
    environment_data: EnvironmentData<RT>,
    rng_seed: [u8; 32],
    unix_timestamp: UnixTimestamp,
    cancellation: CancellationSignal,
    function_started_sender: Option<oneshot::Sender<()>>,
    capture_handler_reads: bool,
    shadow_work_guard: Option<Arc<OwnedSemaphorePermit>>,
    trace_host_operations: bool,
    active_javascript_class: ActiveJavascriptClass,
) -> anyhow::Result<(Transaction<RT>, FunctionOutcome)> {
    let PreparedStaticHermesWasmtimeInvocation {
        route,
        path_and_args,
        transaction,
        routed,
        host_secret_selectors: _,
        memory_observer,
    } = prepared;
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedWorkerEntered,
    );
    // Preparation authenticated and retained the invocation path before the
    // worker was scheduled; preserve the existing lifecycle observation at
    // the point where execution consumes that prepared binding.
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::InvocationValidated,
    );
    // Completion may happen under the instance-pool lock. Capture there, but
    // publish only after execution and cleanup release all runtime locks.
    let (completion_observer, memory_report) = if let Some(observer) = memory_observer {
        let memory = Arc::new(std::sync::OnceLock::new());
        let capture_memory = Arc::clone(&memory);
        let completion_observer: udf::wasm_memory::WasmMemoryObserver =
            Arc::new(move |observation| {
                assert!(
                    capture_memory.set(observation).is_ok(),
                    "memory completed twice"
                );
            });
        (Some(completion_observer), Some((observer, memory)))
    } else {
        (None, None)
    };
    let result = run_generated_routed(
        rt,
        &route,
        routed,
        nested_udf_callback,
        client_id,
        reactor_depth,
        udf_type,
        path_and_args,
        transaction,
        journal,
        context,
        environment_data,
        rng_seed,
        unix_timestamp,
        cancellation,
        function_started_sender,
        capture_handler_reads,
        shadow_work_guard,
        trace_host_operations,
        completion_observer,
        active_javascript_class,
    )
    .await;
    if let Some((observer, memory)) = memory_report {
        if let Some(observation) = memory.get() {
            observer(observation.clone());
        }
    }
    result
}

#[cfg(test)]
pub(crate) async fn run_routed<RT: Runtime>(
    rt: RT,
    route: StaticHermesWasmtimeRouteHandle,
    nested_udf_callback: IsolateClient<RT>,
    client_id: String,
    reactor_depth: usize,
    udf_type: UdfType,
    path_and_args: ValidatedPathAndArgs,
    transaction: Transaction<RT>,
    journal: QueryJournal,
    context: ExecutionContext,
    environment_data: EnvironmentData<RT>,
    rng_seed: [u8; 32],
    unix_timestamp: UnixTimestamp,
    cancellation: CancellationSignal,
    function_started_sender: Option<oneshot::Sender<()>>,
    capture_handler_reads: bool,
    shadow_work_guard: Option<Arc<OwnedSemaphorePermit>>,
    trace_host_operations: bool,
    active_javascript_class: ActiveJavascriptClass,
) -> anyhow::Result<(Transaction<RT>, FunctionOutcome)> {
    let prepared = prepare_routed_invocation(
        route,
        udf_type,
        path_and_args,
        transaction,
        if shadow_work_guard.is_some() {
            StaticHermesWasmtimePreparationMode::Shadow
        } else {
            StaticHermesWasmtimePreparationMode::Primary
        },
    )
    .await?;
    run_prepared_routed(
        rt,
        prepared,
        nested_udf_callback,
        client_id,
        reactor_depth,
        udf_type,
        journal,
        context,
        environment_data,
        rng_seed,
        unix_timestamp,
        cancellation,
        function_started_sender,
        capture_handler_reads,
        shadow_work_guard,
        trace_host_operations,
        active_javascript_class,
    )
    .await
}

#[cfg(test)]
mod quarantine_route_admission_tests {
    use super::{
        super::super::wasm_udf_package::tests::{
            RuntimeRegistryReloadFixture,
            RuntimeRegistrySourceKeyedCapabilityFixture,
        },
        *,
    };

    const MODULE_PATH: &str = "functions/example.js";
    const FUNCTION_NAME: &str = "run";
    const GENERATION_SHA256: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[derive(Clone, Copy, Debug)]
    enum GeneratedRouteForm {
        SourceKeyed,
        Ordinary,
        Singleton,
    }

    impl GeneratedRouteForm {
        fn generation_sha256(self) -> Option<&'static str> {
            match self {
                Self::SourceKeyed | Self::Ordinary => Some(GENERATION_SHA256),
                Self::Singleton => None,
            }
        }
    }

    fn initialized_policy() -> (tempfile::TempDir, crate::StaticHermesWasmtimeQuarantine) {
        let directory = tempfile::tempdir().unwrap();
        let policy =
            crate::StaticHermesWasmtimeQuarantine::load(directory.path().join("quarantine.json"))
                .unwrap();
        policy.initialize("initialize", "test operator").unwrap();
        (directory, policy)
    }

    fn path_and_args(path: &str) -> anyhow::Result<ValidatedPathAndArgs> {
        let args = SerializedArgs::from_args(vec![JsonValue::Null])?;
        ValidatedPathAndArgs::from_proto(pb::common::ValidatedPathAndArgs {
            path: Some(path.to_owned()),
            args: Some(args.into_bytes()),
            npm_version: Some(Version::new(1, 43, 0).to_string()),
            component_path: Some(ComponentPath::root().into()),
            component_id: ComponentId::Root.serialize_to_string(),
            reuse_context: Some(false),
            context_reuse: Some(Default::default()),
            context_initialization_module: None,
            context_initialization_modules: None,
        })
    }

    #[test]
    fn source_keyed_manifest_cold_routing_survives_all_waiter_deadlines() -> anyhow::Result<()> {
        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on("source_keyed_manifest_cold_routing", async move {
            let database = new_test_database(rt).await?;
            let engine = new_generated_engine(WasmMeteringMode::EpochChecks)?;
            let core_wasm = source_keyed_capability_scale_tests::selector_module();
            let serialized_module = engine
                .precompile_module(&core_wasm)
                .map_err(wasmtime_anyhow)?;
            let mut fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(
                &core_wasm,
                &serialized_module,
                1,
            )?;
            fixture.publish_excluded_export()?;
            let initial = fixture.initial();
            let _cleanup = source_keyed_capability_scale_tests::GeneratedRouteCacheCleanup {
                routes: vec![source_keyed_capability_scale_tests::route_identity(initial)],
            };
            let digest = |hex: &str| -> anyhow::Result<Sha256Digest> {
                let mut bytes = [0; 32];
                anyhow::ensure!(
                    hex.len() == 64,
                    "fixture digest must have 64 hex characters"
                );
                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)?;
                }
                Ok(Sha256Digest::from(bytes))
            };
            let mut transaction = database.begin_system().await?;
            SourcePackageModel::new(&mut transaction, TableNamespace::Global)
                .put(SourcePackage {
                    storage_key: ObjectKey::try_from("source-keyed-cold-routing-test")?,
                    sha256: Sha256Digest::from([0; 32]),
                    runtime_content_sha256: Some(digest(
                        &initial.source_package_runtime_content_sha256,
                    )?),
                    runtime_generation: Some(
                        model::source_packages::types::SourcePackageRuntimeGeneration {
                            deployment_sha256: digest(&initial.deployment_sha256)?,
                            generation_manifest_sha256: digest(&initial.generation_file_sha256)?,
                            generation_sha256: digest(&initial.generation_sha256)?,
                        },
                    ),
                    native_resident: None,
                    external_deps_package_id: None,
                    package_size: PackageSize::default(),
                    node_version: None,
                    node_executor_pool_topology: Default::default(),
                })
                .await?;
            database
                .commit_with_write_source(transaction, "cold_routing_test")
                .await?;
            let registry = DeploymentRegistry::load_with_selection(
                fixture.root().to_owned(),
                true,
                false,
                true,
            )?;
            let controller = generated_module_cache_test_controller(128 << 20, 192 << 20)?;
            registry.configure_source_keyed_residency(
                &controller,
                fixture.serialized_module_snapshot_directory(),
            )?;
            let residency = registry.source_keyed_residency.as_ref().unwrap();
            let occupied_module_capacity = residency.cold_load.acquire().await?;
            let (_policy_directory, policy) = initialized_policy();
            let route = |export: &str, path: &ValidatedPathAndArgs| {
                StaticHermesWasmtimeRouteHandle::admit(
                    StaticHermesWasmtimeRoute::SourceKeyedDeployment(SourceKeyedDeploymentRoute {
                        deployment_registry: Arc::clone(&registry),
                        registry_use: RegistryUse::Primary,
                        runtime_module_path: "sourceKeyed.js".to_owned(),
                        export_name: export.to_owned(),
                        udf_kind: ManifestUdfKind::Query,
                    }),
                    UdfType::Query,
                    path,
                    policy.snapshot(),
                )
                .context("cold route was unexpectedly quarantined")
            };
            let excluded_path = path_and_args("sourceKeyed:excluded")?;
            let mut transaction = database.begin_system().await?;
            assert!(tokio::time::timeout(
                Duration::from_secs(5),
                select_route_for_invocation(
                    route("excluded", &excluded_path)?,
                    UdfType::Query,
                    &excluded_path,
                    &mut transaction,
                )
            )
            .await??
            .is_none());
            drop(transaction);
            assert_eq!(residency.manifests.lock().await.started_loads, 1);
            assert!(residency.inflight.lock().is_empty());
            drop(occupied_module_capacity);

            let (reached, started) = tokio::sync::oneshot::channel();
            let (resume, paused) = tokio::sync::oneshot::channel();
            *residency.started_load_pause.lock() = Some((reached, paused));
            let selected_path = path_and_args("sourceKeyed:route_00000000")?;
            let mut waiters = Vec::new();
            for _ in 0..2 {
                let mut candidate = route("route_00000000", &selected_path)?;
                let admission_window = candidate
                    .admission_deadline()?
                    .duration_since(candidate.admission_started);
                // Shorten only these invocation deadlines; shared process knobs
                // and the finite load owner's lifetime remain unchanged.
                candidate.admission_started =
                    tokio::time::Instant::now() + Duration::from_secs(1) - admission_window;
                let task_database = database.clone();
                let task_path = selected_path.clone();
                waiters.push(tokio::spawn(async move {
                    let mut transaction = task_database.begin_system().await?;
                    select_route_for_invocation(
                        candidate,
                        UdfType::Query,
                        &task_path,
                        &mut transaction,
                    )
                    .await
                }));
            }
            tokio::time::timeout(Duration::from_secs(5), started).await??;
            let selector = SourceKeyedGenerationSelector {
                deployment_sha256: initial.deployment_sha256.clone(),
                generation_manifest_sha256: initial.generation_file_sha256.clone(),
                generation_sha256: initial.generation_sha256.clone(),
                source_package_runtime_content_sha256: initial
                    .source_package_runtime_content_sha256
                    .clone(),
            };
            let flight = {
                let inflight = residency.inflight.lock();
                assert_eq!(inflight.len(), 1);
                inflight
                    .get(&selector)
                    .context("selected source has no shared load")?
                    .clone()
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                while flight.waiters.load(Ordering::Acquire) != 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            for waiter in waiters {
                let error = waiter
                    .await?
                    .err()
                    .context("paused selected request did not expire")?;
                assert_eq!(
                    error.downcast_ref::<StaticHermesWasmAdmissionFailure>(),
                    Some(&StaticHermesWasmAdmissionFailure::Deadline)
                );
            }
            assert_eq!(flight.waiters.load(Ordering::Acquire), 0);
            assert_eq!(residency.inflight.lock().len(), 1);
            assert_eq!(residency.cold_load.available_permits(), 0);
            assert!(residency.resident(&selector).is_none());
            assert_eq!(residency.manifests.lock().await.started_loads, 1);
            resume
                .send(())
                .expect("shared load lost ownership after request expiry");
            let generation = tokio::time::timeout(Duration::from_secs(5), flight.result)
                .await?
                .map_err(|error| match error {
                    SourceKeyedLoadFailure::CapacityUnavailable => {
                        SourceKeyedRuntimeCapacityUnavailable.into()
                    },
                    SourceKeyedLoadFailure::Failed(error) => anyhow::anyhow!("{error:#}"),
                })?;
            assert!(generation.is_load_verified());
            assert!(Arc::ptr_eq(
                &generation,
                &residency.resident(&selector).unwrap()
            ));
            assert!(residency.inflight.lock().is_empty());

            let mut transaction = database.begin_system().await?;
            let selected = select_route_for_invocation(
                route("route_00000000", &selected_path)?,
                UdfType::Query,
                &selected_path,
                &mut transaction,
            )
            .await?
            .context("loaded selected route unexpectedly used the existing runtime")?;
            let StaticHermesWasmtimeRoute::GeneratedDeployment(selected) = selected.route else {
                anyhow::bail!("loaded selected route did not bind its generation");
            };
            assert!(Arc::ptr_eq(&selected.generation, &generation));
            assert_eq!(
                selected.generation.generation_sha256,
                initial.generation_sha256
            );
            assert_eq!(
                selected.generation.deployment_sha256(),
                initial.deployment_sha256
            );
            assert_eq!(
                selected.generation.generation_manifest_sha256,
                initial.generation_file_sha256
            );
            assert!(select_route_for_invocation(
                route("excluded", &excluded_path)?,
                UdfType::Query,
                &excluded_path,
                &mut transaction,
            )
            .await?
            .is_none());
            assert_eq!(residency.manifests.lock().await.started_loads, 1);
            drop(transaction);
            database.shutdown().await?;
            Ok(())
        })
    }

    fn assert_all_route_forms(
        snapshot: &crate::StaticHermesWasmtimeQuarantineSnapshot,
        expected_admission: bool,
    ) {
        for route_form in [
            GeneratedRouteForm::SourceKeyed,
            GeneratedRouteForm::Ordinary,
            GeneratedRouteForm::Singleton,
        ] {
            assert_eq!(
                quarantine_admits_wasm(
                    snapshot,
                    MODULE_PATH,
                    FUNCTION_NAME,
                    route_form.generation_sha256(),
                ),
                expected_admission,
                "unexpected quarantine decision for {route_form:?}",
            );
        }
    }

    #[test]
    fn missing_policy_fails_closed_for_every_generated_route_form() -> anyhow::Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let policy = crate::StaticHermesWasmtimeQuarantine::load(
            directory.path().join("missing-quarantine.json"),
        )
        .unwrap();
        let snapshot = policy.snapshot();
        let (_initialized_directory, initialized_policy) = initialized_policy();
        let admitting_snapshot = initialized_policy.snapshot();

        assert!(snapshot.is_ambiguous());
        assert_all_route_forms(&snapshot, false);

        let singleton_path = path_and_args("singleton:run")?;
        assert!(StaticHermesWasmtimeRouteHandle::admit(
            StaticHermesWasmtimeRoute::GeneratedSingleton,
            UdfType::Query,
            &singleton_path,
            Arc::clone(&admitting_snapshot),
        )
        .is_some());
        assert!(StaticHermesWasmtimeRouteHandle::admit(
            StaticHermesWasmtimeRoute::GeneratedSingleton,
            UdfType::Query,
            &singleton_path,
            Arc::clone(&snapshot),
        )
        .is_none());

        let ordinary_fixture = RuntimeRegistryReloadFixture::create()?;
        let ordinary_registry = DeploymentRegistry::load_with_primary_routing(
            ordinary_fixture.root().to_owned(),
            true,
        )?;
        let ordinary_path = path_and_args("example:run")?;
        assert!(resolve_generation_route(
            ordinary_registry.current(),
            RegistryUse::Primary,
            UdfType::Query,
            &ordinary_path,
            ManifestUdfKind::Query,
            Arc::clone(&admitting_snapshot),
        )?
        .is_some());
        assert!(resolve_generation_route(
            ordinary_registry.current(),
            RegistryUse::Primary,
            UdfType::Query,
            &ordinary_path,
            ManifestUdfKind::Query,
            Arc::clone(&snapshot),
        )?
        .is_none());

        let source_keyed_fixture =
            RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
        let source_keyed_registry = DeploymentRegistry::load_with_selection(
            source_keyed_fixture.root().to_owned(),
            true,
            false,
            true,
        )?;
        let source_keyed_path = path_and_args("sourceKeyed:route_00000000")?;
        assert!(StaticHermesWasmtimeRouteHandle::admit(
            StaticHermesWasmtimeRoute::SourceKeyedDeployment(SourceKeyedDeploymentRoute {
                deployment_registry: Arc::clone(&source_keyed_registry),
                registry_use: RegistryUse::Primary,
                runtime_module_path: "sourceKeyed.js".to_owned(),
                export_name: "route_00000000".to_owned(),
                udf_kind: ManifestUdfKind::Query,
            }),
            UdfType::Query,
            &source_keyed_path,
            admitting_snapshot,
        )
        .is_some());
        assert!(StaticHermesWasmtimeRouteHandle::admit(
            StaticHermesWasmtimeRoute::SourceKeyedDeployment(SourceKeyedDeploymentRoute {
                deployment_registry: source_keyed_registry,
                registry_use: RegistryUse::Primary,
                runtime_module_path: "sourceKeyed.js".to_owned(),
                export_name: "route_00000000".to_owned(),
                udf_kind: ManifestUdfKind::Query,
            }),
            UdfType::Query,
            &source_keyed_path,
            snapshot,
        )
        .is_none());
        Ok(())
    }

    #[test]
    fn generated_routes_retain_their_captured_snapshot_after_policy_update() {
        let (_directory, policy) = initialized_policy();
        let captured = policy.snapshot();
        assert_all_route_forms(&captured, true);

        policy
            .update(
                crate::StaticHermesWasmtimeQuarantineAction::Quarantine,
                crate::StaticHermesWasmtimeQuarantineSelector::new(
                    MODULE_PATH,
                    Some(FUNCTION_NAME.to_owned()),
                )
                .unwrap(),
                "test quarantine",
                "test operator",
            )
            .unwrap();
        let updated = policy.snapshot();

        assert!(!Arc::ptr_eq(&captured, &updated));
        assert_all_route_forms(&captured, true);
        assert_all_route_forms(&updated, false);
    }

    #[test]
    fn source_keyed_selection_retains_candidate_snapshot() -> anyhow::Result<()> {
        let (_directory, policy) = initialized_policy();
        let captured = policy.snapshot();
        let fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
        let registry =
            DeploymentRegistry::load_with_selection(fixture.root().to_owned(), true, false, true)?;
        let descriptor = load_runtime_registry_source_catalog(fixture.root())?
            .into_generations()
            .into_iter()
            .next()
            .context("source-keyed quarantine fixture descriptor is missing")?;
        let generation = DeploymentGeneration::from_validated(load_runtime_registry_generation(
            fixture.root(),
            &descriptor,
        )?);
        let path_and_args = path_and_args("sourceKeyed:route_00000000")?;
        let candidate = StaticHermesWasmtimeRouteHandle::admit(
            StaticHermesWasmtimeRoute::SourceKeyedDeployment(SourceKeyedDeploymentRoute {
                deployment_registry: Arc::clone(&registry),
                registry_use: RegistryUse::Primary,
                runtime_module_path: "sourceKeyed.js".to_owned(),
                export_name: "route_00000000".to_owned(),
                udf_kind: ManifestUdfKind::Query,
            }),
            UdfType::Query,
            &path_and_args,
            Arc::clone(&captured),
        )
        .context("source-keyed candidate was not admitted by the empty captured policy")?;

        policy.update(
            crate::StaticHermesWasmtimeQuarantineAction::Quarantine,
            crate::StaticHermesWasmtimeQuarantineSelector::new(
                "sourceKeyed.js",
                Some("route_00000000".to_owned()),
            )?,
            "test quarantine",
            "test operator",
        )?;
        let updated = policy.snapshot();
        assert!(!Arc::ptr_eq(&candidate.quarantine_snapshot, &updated));

        let selected = resolve_generation_route(
            Arc::clone(&generation),
            RegistryUse::Primary,
            UdfType::Query,
            &path_and_args,
            ManifestUdfKind::Query,
            Arc::clone(&candidate.quarantine_snapshot),
        )?
        .context("captured source-keyed candidate did not retain its Wasm admission")?;
        assert!(Arc::ptr_eq(
            &candidate.quarantine_snapshot,
            &selected.quarantine_snapshot,
        ));
        assert!(selected.authenticated_generation_sha256().is_some());
        assert!(resolve_generation_route(
            generation,
            RegistryUse::Primary,
            UdfType::Query,
            &path_and_args,
            ManifestUdfKind::Query,
            Arc::clone(&updated),
        )?
        .is_none());

        assert!(StaticHermesWasmtimeRouteHandle::admit(
            StaticHermesWasmtimeRoute::SourceKeyedDeployment(SourceKeyedDeploymentRoute {
                deployment_registry: registry,
                registry_use: RegistryUse::Primary,
                runtime_module_path: "sourceKeyed.js".to_owned(),
                export_name: "route_00000000".to_owned(),
                udf_kind: ManifestUdfKind::Query,
            }),
            UdfType::Query,
            &path_and_args,
            updated,
        )
        .is_none());
        Ok(())
    }

    #[test]
    fn source_keyed_shadow_registry_omits_unselected_generation() -> anyhow::Result<()> {
        let fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
        let registry =
            DeploymentRegistry::load_with_selection(fixture.root().to_owned(), true, false, true)?;

        assert!(selected_query_shadow_registry(registry.query_shadow_registry()).is_none());
        Ok(())
    }
}

#[cfg(test)]
mod prepared_invocation_tests {
    use super::*;

    #[test]
    fn test_hook_preparation_pins_selector_authorization_to_test_module() -> anyhow::Result<()> {
        let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
        let manifest = generated_test_manifest(
            ManifestUdfKind::Query,
            json!({
                "kind": "hostSecretVerify",
                "contractVersion": 1,
                "selector": "TEST_HOOK_SECRET",
            }),
        )?;
        let test_routed = generated_host_secret_verify_test_routed_module(manifest, 1, 0, 0, b"")?;
        let test_hooks = StaticHermesGateTestHooks::new_generated_route(Arc::clone(&test_routed));
        let _test_hooks_guard = install_test_hooks(&test_hooks)?;
        let route = StaticHermesWasmtimeRouteHandle {
            route: StaticHermesWasmtimeRoute::GeneratedSingleton,
            udf_type: UdfType::Query,
            quarantine_snapshot: crate::static_hermes_wasmtime_quarantine().snapshot(),
            singleton_udf_path: Some("configured:singleton".to_owned()),
            admission_started: tokio::time::Instant::now(),
        };

        let prepared =
            prepared_routed_module(&route, StaticHermesWasmtimePreparationMode::Primary)?;
        let PreparedRoutedModule::TestHook(prepared_routed) = prepared else {
            anyhow::bail!("test-hook preparation did not pin the test module")
        };
        assert!(Arc::ptr_eq(&prepared_routed, &test_routed));
        assert_eq!(
            authorized_host_secret_selectors_in_manifest(
                &prepared_routed.manifest,
                &BTreeSet::from([
                    "CONFIGURED_SINGLETON_SECRET".to_owned(),
                    "TEST_HOOK_SECRET".to_owned(),
                ]),
            ),
            BTreeSet::from(["TEST_HOOK_SECRET".to_owned()])
        );
        Ok(())
    }
}

#[cfg(test)]
mod active_wasm_cpu_admission_tests {
    use std::{
        sync::Arc,
        task::Poll,
    };

    use futures::poll;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn setup_phases_share_one_deadline_and_release_expired_work() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        within_static_hermes_admission(deadline, async {
            tokio::time::sleep(Duration::from_secs(4)).await;
            Ok(())
        })
        .await
        .unwrap();
        let (sender, receiver) = oneshot::channel::<()>();
        let error = within_static_hermes_admission(deadline, async move {
            let _held_request_resource = sender;
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(())
        })
        .await
        .unwrap_err();
        assert_eq!(tokio::time::Instant::now(), deadline);
        assert!(error.is_overloaded());
        assert_eq!(
            error.downcast_ref::<StaticHermesWasmAdmissionFailure>(),
            Some(&StaticHermesWasmAdmissionFailure::Deadline)
        );
        assert!(
            receiver.await.is_err(),
            "timed-out setup retained its resources"
        );

        let polled = AtomicBool::new(false);
        let error = within_static_hermes_admission(deadline, async {
            polled.store(true, Ordering::Relaxed);
            Ok(())
        })
        .await
        .unwrap_err();
        assert!(error.is_overloaded());
        assert!(
            !polled.load(Ordering::Relaxed),
            "expired setup started another phase"
        );

        let error = within_static_hermes_admission(deadline + Duration::from_secs(1), async {
            Err::<(), _>(anyhow::anyhow!("invalid authenticated artifact"))
        })
        .await
        .unwrap_err();
        assert!(!error.is_overloaded());
        assert!(!error.is::<StaticHermesWasmAdmissionFailure>());
    }

    #[tokio::test]
    async fn wasm_service_floors_survive_contention_suspension_and_dependency_work() {
        use ActiveJavascriptClass::{
            Degradable,
            Dependency,
            Protected,
        };
        let limiter = ConcurrencyLimiter::new_for_wasm(2, 1, 1);
        let cancellation = CancellationSignal::new_for_test();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        // Either class may borrow the whole idle pool. When the other arrives,
        // the next released permit restores its floor without preemption.
        for (borrower, waiting_class) in [(Protected, Degradable), (Degradable, Protected)] {
            let first =
                acquire_active_wasm_cpu_permit(&limiter, &cancellation, false, deadline, borrower)
                    .await
                    .unwrap();
            let second =
                acquire_active_wasm_cpu_permit(&limiter, &cancellation, false, deadline, borrower)
                    .await
                    .unwrap();
            let mut another = Box::pin(acquire_active_wasm_cpu_permit(
                &limiter,
                &cancellation,
                false,
                deadline,
                borrower,
            ));
            let mut waiting = Box::pin(acquire_active_wasm_cpu_permit(
                &limiter,
                &cancellation,
                false,
                deadline,
                waiting_class,
            ));
            assert!(poll!(another.as_mut()).is_pending());
            assert!(poll!(waiting.as_mut()).is_pending());
            drop(first);
            let waiting = waiting
                .now_or_never()
                .expect("restore the other class's floor")
                .unwrap();
            assert!(poll!(another.as_mut()).is_pending());
            drop(another);

            // Suspension must retain the class; a new request cannot replace
            // that class's queued resumption with an ordinary initial start.
            let suspended = waiting.suspend();
            let held =
                acquire_active_wasm_cpu_permit(&limiter, &cancellation, false, deadline, borrower)
                    .await
                    .unwrap();
            let mut initial = Box::pin(acquire_active_wasm_cpu_permit(
                &limiter,
                &cancellation,
                false,
                deadline,
                waiting_class,
            ));
            let mut resume = Box::pin(suspended.acquire());
            let mut dependency = Box::pin(acquire_active_wasm_cpu_permit(
                &limiter,
                &cancellation,
                false,
                deadline,
                Dependency,
            ));
            assert!(poll!(initial.as_mut()).is_pending());
            assert!(poll!(resume.as_mut()).is_pending());
            assert!(poll!(dependency.as_mut()).is_pending());
            drop(held);
            let dependency = dependency
                .now_or_never()
                .expect("unblock the ancestor first")
                .unwrap();
            assert!(poll!(resume.as_mut()).is_pending());
            drop(dependency);
            assert!(poll!(initial.as_mut()).is_pending());
            // Optional verification must not consume a grant awaiting delivery.
            assert!(acquire_active_wasm_cpu_permit(
                &limiter,
                &cancellation,
                true,
                deadline,
                Protected,
            )
            .await
            .unwrap_err()
            .is::<StaticHermesQueryShadowCapacityUnavailable>());
            drop(
                resume
                    .now_or_never()
                    .expect("resume retains its class and phase"),
            );
            drop(initial);
            drop(second);
            assert_eq!(limiter.active_permits(), 0);
        }
    }

    #[tokio::test]
    async fn mutation_priority_reaches_primary_wasm_cpu_admission() {
        use futures::FutureExt;
        let limiter = ConcurrencyLimiter::new_for_wasm(1, 0, 0);
        let held = limiter.acquire(Arc::new("held".to_owned()), false).await;
        let cancellation = CancellationSignal::new_for_test();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut normal = Box::pin(acquire_active_wasm_cpu_permit(
            &limiter,
            &cancellation,
            false,
            deadline,
            ActiveJavascriptClass::Protected,
        ));
        let mut highs = (0..3)
            .map(|_| {
                Box::pin(acquire_active_wasm_cpu_permit(
                    &limiter,
                    &cancellation,
                    false,
                    deadline,
                    ActiveJavascriptClass::HighPriorityMutation,
                ))
            })
            .collect::<Vec<_>>();
        assert!(poll!(normal.as_mut()).is_pending());
        for high in &mut highs {
            assert!(poll!(high.as_mut()).is_pending());
        }
        let shadow_error = acquire_active_wasm_cpu_permit(
            &limiter,
            &cancellation,
            true,
            deadline,
            ActiveJavascriptClass::HighPriorityMutation,
        )
        .await
        .unwrap_err();
        assert!(shadow_error.is::<StaticHermesQueryShadowCapacityUnavailable>());
        drop(held);
        drop(
            highs
                .remove(0)
                .now_or_never()
                .expect("first high turn")
                .unwrap(),
        );
        drop(
            highs
                .remove(0)
                .now_or_never()
                .expect("second high turn")
                .unwrap(),
        );
        let normal = normal
            .now_or_never()
            .expect("normal service after two high turns")
            .unwrap();
        assert!(poll!(highs[0].as_mut()).is_pending());
        drop(normal);
        let high = highs
            .remove(0)
            .now_or_never()
            .expect("next high turn")
            .unwrap();
        let suspended = high.suspend();
        let held = limiter.acquire(Arc::new("held".to_owned()), false).await;
        let mut normal = Box::pin(acquire_active_wasm_cpu_permit(
            &limiter,
            &cancellation,
            false,
            deadline,
            ActiveJavascriptClass::Protected,
        ));
        let mut resumed = Box::pin(suspended.acquire());
        assert!(poll!(normal.as_mut()).is_pending());
        assert!(poll!(resumed.as_mut()).is_pending());
        drop(held);
        drop(
            resumed
                .now_or_never()
                .expect("Wasm resume retains high priority"),
        );
        drop(
            normal
                .now_or_never()
                .expect("normal follows resume")
                .unwrap(),
        );
        assert_eq!(limiter.active_permits(), 0);
    }

    #[tokio::test]
    async fn pending_admission_observes_cancellation_without_losing_capacity() {
        let limiter = ConcurrencyLimiter::new(1);
        let held_permit = limiter
            .acquire(
                Arc::new("Static Hermes Wasm active CPU test".to_owned()),
                false,
            )
            .await;
        let cancellation = CancellationSignal::new_for_test();
        let mut admission = Box::pin(acquire_active_wasm_cpu_permit(
            &limiter,
            &cancellation,
            false,
            tokio::time::Instant::now() + Duration::from_secs(5),
            common::types::ActiveJavascriptClass::Protected,
        ));
        assert!(matches!(poll!(admission.as_mut()), Poll::Pending));

        cancellation.cancel_for_test();
        let error = admission
            .await
            .expect_err("cancelled active CPU admission unexpectedly succeeded");
        assert!(error
            .to_string()
            .contains("generated Wasm active CPU admission cancelled"));
        assert!(!error.is_overloaded());
        assert_eq!(limiter.active_permits(), 1);

        drop(held_permit);
        assert_eq!(limiter.active_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_cpu_admission_cannot_take_an_immediately_available_permit() {
        let limiter = ConcurrencyLimiter::new_for_wasm(1, 0, 0);
        let cancellation = CancellationSignal::new_for_test();
        let deadline = tokio::time::Instant::now();
        for immediate in [false, true] {
            let error = acquire_active_wasm_cpu_permit(
                &limiter,
                &cancellation,
                immediate,
                deadline,
                ActiveJavascriptClass::Protected,
            )
            .await
            .unwrap_err();
            assert_eq!(
                error.downcast_ref::<StaticHermesWasmAdmissionFailure>(),
                Some(&StaticHermesWasmAdmissionFailure::Cpu),
            );
            assert_eq!(limiter.active_permits(), 0);
        }
    }

    #[tokio::test]
    async fn immediate_admission_returns_shadow_capacity_error_without_queueing() {
        let limiter = ConcurrencyLimiter::new(1);
        let held_permit = limiter
            .acquire(
                Arc::new("Static Hermes Wasm active CPU test".to_owned()),
                false,
            )
            .await;
        let cancellation = CancellationSignal::new_for_test();

        let error = acquire_active_wasm_cpu_permit(
            &limiter,
            &cancellation,
            true,
            tokio::time::Instant::now() + Duration::from_secs(5),
            common::types::ActiveJavascriptClass::Protected,
        )
        .await
        .expect_err("saturated immediate active CPU admission unexpectedly succeeded");
        assert!(error.is::<StaticHermesQueryShadowCapacityUnavailable>());
        assert_eq!(limiter.active_permits(), 1);

        drop(held_permit);
        let permit = acquire_active_wasm_cpu_permit(
            &limiter,
            &cancellation,
            true,
            tokio::time::Instant::now() + Duration::from_secs(5),
            common::types::ActiveJavascriptClass::Protected,
        )
        .await
        .expect("released immediate active CPU capacity was not reusable");
        drop(permit);
    }

    #[tokio::test(start_paused = true)]
    async fn pending_admission_has_a_deadline_without_losing_capacity() {
        let limiter = ConcurrencyLimiter::new(1);
        let held_permit = limiter
            .acquire(
                Arc::new("Static Hermes Wasm active CPU test".to_owned()),
                false,
            )
            .await;
        let cancellation = CancellationSignal::new_for_test();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let admission = acquire_active_wasm_cpu_permit(
            &limiter,
            &cancellation,
            false,
            deadline,
            common::types::ActiveJavascriptClass::Protected,
        );

        let error = admission
            .await
            .expect_err("saturated active CPU admission exceeded its deadline");
        assert!(error.is_overloaded());
        assert!(error
            .to_string()
            .contains("generated Wasm active CPU admission timed out"));
        assert_eq!(limiter.active_permits(), 1);

        drop(held_permit);
        assert_eq!(limiter.active_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cpu_admission_uses_the_existing_request_deadline() {
        let limiter = ConcurrencyLimiter::new(1);
        let held_permit = limiter
            .acquire(
                Arc::new("Static Hermes Wasm active CPU test".to_owned()),
                false,
            )
            .await;
        let cancellation = CancellationSignal::new_for_test();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        tokio::time::advance(Duration::from_secs(4)).await;
        let cpu_wait_started = tokio::time::Instant::now();

        let error = acquire_active_wasm_cpu_permit(
            &limiter,
            &cancellation,
            false,
            deadline,
            common::types::ActiveJavascriptClass::Protected,
        )
        .await
        .expect_err("saturated CPU admission ignored the existing request deadline");
        assert!(error.is_overloaded());
        assert!(error
            .to_string()
            .contains("generated Wasm active CPU admission timed out"));
        assert_eq!(
            tokio::time::Instant::now().duration_since(cpu_wait_started),
            Duration::from_secs(1)
        );

        drop(held_permit);
        assert_eq!(limiter.active_permits(), 0);
    }
}

#[cfg(test)]
mod generated_failure_log_tests {
    use super::*;

    #[test]
    fn rate_limits_each_failure_kind_independently() {
        let now = Instant::now();
        let mut limiter = GeneratedFailureLogLimiter::new();
        let failures = [
            GeneratedFailureLog::IdleRuntimeCleanup,
            GeneratedFailureLog::RetiredGenerationCleanup,
            GeneratedFailureLog::RegistryReloadRejected,
            GeneratedFailureLog::RegistryReloadTask,
            GeneratedFailureLog::RetiredUnstartedCleanup,
            GeneratedFailureLog::RejectedUnstartedCleanup,
        ];
        let labels = [
            "idle_runtime_cleanup",
            "retired_generation_cleanup",
            "registry_reload_rejected",
            "registry_reload_task",
            "retired_unstarted_cleanup",
            "rejected_unstarted_cleanup",
        ];

        for (failure, label) in failures.into_iter().zip(labels) {
            assert_eq!(failure.label(), label);
            assert!(limiter.should_log(failure, now));
        }
        for failure in failures {
            assert!(!limiter.should_log(
                failure,
                now + GENERATED_FAILURE_LOG_INTERVAL - Duration::from_nanos(1)
            ));
            assert!(limiter.should_log(failure, now + GENERATED_FAILURE_LOG_INTERVAL));
        }
    }
}

#[cfg(test)]
mod registry_admission_tests {
    use super::{
        super::super::wasm_udf_package::tests::{
            RuntimeRegistryQueryShadowFixture,
            RuntimeRegistrySourceKeyedCapabilityFixture,
            RuntimeRegistrySourceKeyedCapabilityGeneration,
        },
        *,
    };

    fn publish_fixture(source: &Path, destination: &Path) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            std::fs::rename(entry.path(), destination.join(entry.file_name()))?;
        }
        Ok(())
    }

    fn private_tempdir() -> anyhow::Result<tempfile::TempDir> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let directory = tempfile::Builder::new()
                .permissions(std::fs::Permissions::from_mode(0o700))
                .tempdir()?;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
            Ok(directory)
        }
        #[cfg(not(unix))]
        {
            Ok(tempfile::tempdir()?)
        }
    }

    fn manifest_fixture() -> anyhow::Result<(
        RuntimeRegistrySourceKeyedCapabilityFixture,
        Arc<DeploymentRegistry>,
        Arc<ValidatedRuntimeRegistryGenerationDescriptor>,
    )> {
        let fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
        let registry =
            DeploymentRegistry::load_with_selection(fixture.root().to_owned(), false, true, true)?;
        let initial = fixture.initial();
        let descriptor = registry
            .state
            .read()
            .source_keyed_catalog
            .as_ref()
            .unwrap()
            .descriptor(
                &initial.source_package_runtime_content_sha256,
                &SourceKeyedRuntimeGenerationIdentity {
                    deployment_sha256: initial.deployment_sha256.clone(),
                    generation_manifest_sha256: initial.generation_file_sha256.clone(),
                    generation_sha256: initial.generation_sha256.clone(),
                },
            )
            .context("manifest fixture is absent from its catalog")?;
        Ok((fixture, registry, descriptor))
    }

    #[tokio::test]
    async fn source_keyed_manifest_owner_survives_all_request_deadlines() -> anyhow::Result<()> {
        let (fixture, registry, descriptor) = manifest_fixture()?;
        let residency = registry.source_keyed_residency.as_ref().unwrap();
        let expired = within_static_hermes_admission(
            tokio::time::Instant::now(),
            residency.authenticated_manifest(&registry, Arc::clone(&descriptor)),
        )
        .await;
        assert!(expired.is_err());
        assert_eq!(residency.manifests.lock().await.started_loads, 0);

        let (reached, started) = tokio::sync::oneshot::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        residency.manifests.lock().await.started_load_pause = Some((reached, paused));
        let task_registry = Arc::clone(&registry);
        let task_descriptor = Arc::clone(&descriptor);
        let owner_waiter = tokio::spawn(async move {
            within_static_hermes_admission(
                tokio::time::Instant::now() + Duration::from_millis(20),
                task_registry
                    .source_keyed_residency
                    .as_ref()
                    .unwrap()
                    .authenticated_manifest(&task_registry, task_descriptor),
            )
            .await
        });
        started.await?;
        let mut waiters = Vec::new();
        for index in 0..64 {
            let task_registry = Arc::clone(&registry);
            let task_descriptor = Arc::clone(&descriptor);
            waiters.push(tokio::spawn(async move {
                within_static_hermes_admission(
                    tokio::time::Instant::now()
                        + if index < 32 {
                            Duration::from_secs(60)
                        } else {
                            Duration::from_millis(20)
                        },
                    task_registry
                        .source_keyed_residency
                        .as_ref()
                        .unwrap()
                        .authenticated_manifest(&task_registry, task_descriptor),
                )
                .await
            }));
        }
        tokio::task::yield_now().await;
        for waiter in &waiters[..32] {
            waiter.abort();
        }
        assert!(owner_waiter.await?.is_err());
        for (index, waiter) in waiters.into_iter().enumerate() {
            if index < 32 {
                assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
            } else {
                assert!(waiter.await?.is_err());
            }
        }
        // Module capacity cannot prevent a completed eligibility owner from
        // publishing or a later caller from consuming its authenticated snapshot.
        let _occupied_module_capacity = residency.cold_load.acquire().await?;
        resume.send(())?;
        let manifest = tokio::time::timeout(
            Duration::from_secs(5),
            residency.authenticated_manifest(&registry, Arc::clone(&descriptor)),
        )
        .await??;
        let cached = residency
            .authenticated_manifest(&registry, Arc::clone(&descriptor))
            .await?;
        assert!(Arc::ptr_eq(&manifest, &cached));
        assert_eq!(residency.manifests.lock().await.started_loads, 1);
        let generation =
            DeploymentGeneration::from_validated(load_runtime_registry_generation_from_manifest(
                fixture.root(),
                &descriptor,
                &manifest,
                &[],
            )?);
        assert!(Arc::ptr_eq(&manifest.registry, &generation.registry));
        Ok(())
    }

    #[tokio::test]
    async fn source_keyed_manifest_failure_and_panic_allow_reauthentication() -> anyhow::Result<()>
    {
        let (fixture, registry, descriptor) = manifest_fixture()?;
        let residency = registry.source_keyed_residency.as_ref().unwrap();
        let initial = fixture.initial();
        let path = fixture
            .root()
            .join("generations")
            .join(&initial.deployment_sha256)
            .join(&initial.generation_sha256)
            .join("generation.json");
        let authentic = std::fs::read(&path)?;
        std::fs::write(&path, b"corrupt")?;
        assert!(residency
            .authenticated_manifest(&registry, Arc::clone(&descriptor))
            .await
            .is_err());
        assert!(residency.manifests.lock().await.entries.is_empty());
        std::fs::write(&path, authentic)?;

        let (reached, started) = tokio::sync::oneshot::channel();
        let (_resume, paused) = std::sync::mpsc::channel();
        drop(started);
        residency.manifests.lock().await.started_load_pause = Some((reached, paused));
        assert!(residency
            .authenticated_manifest(&registry, Arc::clone(&descriptor))
            .await
            .is_err());
        assert!(residency.manifests.lock().await.entries.is_empty());
        residency
            .authenticated_manifest(&registry, descriptor)
            .await?;
        assert_eq!(residency.manifests.lock().await.started_loads, 3);
        Ok(())
    }

    #[tokio::test]
    async fn source_keyed_manifest_cache_tracks_descriptor_incarnations() -> anyhow::Result<()> {
        let (mut fixture, registry, descriptor) = manifest_fixture()?;
        let residency = registry.source_keyed_residency.as_ref().unwrap();
        let selector = SourceKeyedGenerationSelector::from_descriptor(&descriptor)?;
        let original = residency
            .authenticated_manifest(&registry, Arc::clone(&descriptor))
            .await?;
        let latest = fixture.publish_additive_generation()?;
        let catalog_path = fixture.root().join("source-catalog.json");
        let additive_catalog = std::fs::read(&catalog_path)?;
        let controller = generated_module_cache_test_controller(128 << 20, 192 << 20)?;
        registry.reload_with_source_preload(
            &controller,
            fixture.serialized_module_snapshot_directory(),
        )?;
        let retained = registry
            .state
            .read()
            .source_keyed_catalog
            .as_ref()
            .unwrap()
            .descriptor_by_selector(&selector)
            .unwrap();
        assert!(Arc::ptr_eq(&descriptor, &retained));
        let unchanged = residency
            .authenticated_manifest(&registry, retained)
            .await?;
        assert!(Arc::ptr_eq(&original, &unchanged));

        // Queue an old caller across withdrawal and restaging of identical bytes.
        // Its descriptor cannot consume either the old or new incarnation's cache.
        let cache = Arc::clone(&residency.manifests).lock_owned().await;
        let task_registry = Arc::clone(&registry);
        let old_descriptor = Arc::clone(&descriptor);
        let waiter = tokio::spawn(async move {
            task_registry
                .source_keyed_residency
                .as_ref()
                .unwrap()
                .authenticated_manifest(&task_registry, old_descriptor)
                .await
        });
        tokio::task::yield_now().await;
        fixture.publish_latest_only_catalog()?;
        registry.reload_with_source_preload(
            &controller,
            fixture.serialized_module_snapshot_directory(),
        )?;
        assert!(!registry.descriptor_incarnation_is_current(&selector, &descriptor));
        std::fs::write(&catalog_path, additive_catalog)?;
        registry.reload_with_source_preload(
            &controller,
            fixture.serialized_module_snapshot_directory(),
        )?;
        drop(cache);
        assert!(waiter.await?.is_err());
        let restaged = registry
            .state
            .read()
            .source_keyed_catalog
            .as_ref()
            .unwrap()
            .descriptor_by_selector(&selector)
            .unwrap();
        assert!(!Arc::ptr_eq(&descriptor, &restaged));
        let renewed = residency
            .authenticated_manifest(&registry, restaged)
            .await?;
        assert!(!Arc::ptr_eq(&original, &renewed));
        assert_eq!(residency.manifests.lock().await.started_loads, 2);
        let latest_descriptor = registry
            .state
            .read()
            .source_keyed_catalog
            .as_ref()
            .unwrap()
            .descriptor(
                &latest.source_package_runtime_content_sha256,
                &SourceKeyedRuntimeGenerationIdentity {
                    deployment_sha256: latest.deployment_sha256,
                    generation_manifest_sha256: latest.generation_file_sha256,
                    generation_sha256: latest.generation_sha256,
                },
            )
            .unwrap();
        residency
            .authenticated_manifest(&registry, latest_descriptor)
            .await?;
        let cache = residency.manifests.lock().await;
        assert_eq!(cache.started_loads, 3);
        assert_eq!(cache.entries.len(), 2);
        assert!(!cache
            .entries
            .iter()
            .any(|entry| Arc::ptr_eq(&entry.manifest, &original)));
        drop(cache);
        assert_eq!(
            original.registry.deployment_sha256(),
            renewed.registry.deployment_sha256()
        );
        Ok(())
    }

    #[test]
    fn shadow_module_cache_miss_does_not_reconstruct() -> anyhow::Result<()> {
        let fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(
            b"uncached-shadow-module",
            b"uncached-shadow-aot",
            1,
        )?;
        let descriptor = load_runtime_registry_source_catalog(fixture.root())?
            .into_generations()
            .into_iter()
            .next()
            .context("shadow cache-miss fixture has no generation")?;
        let generation = DeploymentGeneration::from_validated(load_runtime_registry_generation(
            fixture.root(),
            &descriptor,
        )?);
        let mut route = selected_generation_routes(&generation)?
            .into_iter()
            .next()
            .context("shadow cache-miss fixture has no route")?;
        let controller = generated_module_cache_test_controller(128 << 20, 192 << 20)?;
        let load = |route: &DeploymentWasmRoute| {
            load_generated_deployment_routed_module_with_aot_resolution(
                &controller,
                fixture.serialized_module_snapshot_directory(),
                route,
                AuthenticatedGraphAotResolution::PreloadedOnly,
                StaticHermesWasmtimePreparationMode::Shadow,
            )
        };
        // The metadata is authenticated, but the deliberately non-executable
        // payload must never reach reconstruction for an optional caller.
        let error = load(&route).err().expect("cold shadow module was loaded");
        assert!(error.is::<StaticHermesQueryShadowCapacityUnavailable>());

        route.package_key.push_str("-mismatched");
        let error = load(&route)
            .err()
            .expect("mismatched shadow route was accepted");
        assert!(!error.is::<StaticHermesQueryShadowCapacityUnavailable>());
        Ok(())
    }

    #[test]
    fn unpublished_registry_admits_first_publication_without_restart() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let registry =
            DeploymentRegistry::load_with_selection(root.path().to_owned(), false, false, false)?;
        let shadow_registry =
            DeploymentRegistry::load_with_selection(root.path().to_owned(), false, true, false)?;
        assert!(shadow_registry.reload()?.is_none());
        assert!(registry.state.read().current.is_none());
        assert!(registry.query_shadow_registry().selection.is_none());
        assert!(registry.reload()?.is_none());

        // Incomplete publication is rejected without converting it to an active
        // generation.
        std::fs::write(root.path().join("current"), b"invalid")?;
        assert!(registry.reload().is_err());
        assert!(registry.state.read().current.is_none());
        std::fs::remove_file(root.path().join("current"))?;
        let fixture = RuntimeRegistryQueryShadowFixture::create()?;
        publish_fixture(fixture.root(), root.path())?;
        // Primary admission is stronger than shadow admission, so the same
        // generation may serve the non-authoritative lane.
        assert!(shadow_registry.reload()?.is_none());
        assert!(shadow_registry.state.read().current.is_some());
        assert!(
            registry.reload()?.is_none(),
            "first publication has no retired generation"
        );
        let generation = registry.current();
        assert_eq!(generation.generation_sha256, fixture.generation_sha256());
        assert!(!generation.is_retired());
        assert!(registry.query_shadow_registry().selection.is_some());
        assert!(registry.reload()?.is_none());

        // Removing a publication after admission is corruption, not a return to
        // unpublished.
        let removed = tempfile::tempdir()?;
        publish_fixture(root.path(), removed.path())?;
        assert!(registry.reload().is_err());
        assert!(Arc::ptr_eq(&registry.current(), &generation));
        Ok(())
    }

    #[test]
    fn unpublished_source_keyed_registry_admits_first_catalog() -> anyhow::Result<()> {
        let root = private_tempdir()?;
        let registry =
            DeploymentRegistry::load_with_selection(root.path().to_owned(), false, true, true)?;
        let snapshots = private_tempdir()?;
        let controller = generated_module_cache_test_controller(128 << 20, 192 << 20)?;
        assert!(registry.uses_source_keyed_selection());
        assert!(registry.query_shadow_registry().selection.is_none());
        assert!(registry
            .reload_with_source_preload(&controller, snapshots.path())?
            .is_none());
        let fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
        publish_fixture(fixture.root(), root.path())?;
        registry.reload_with_source_preload(&controller, snapshots.path())?;
        assert!(registry.state.read().source_keyed_catalog.is_some());
        // A catalog is discoverable, but no source snapshot has selected or loaded a
        // route yet.
        assert!(registry.query_shadow_registry().selection.is_none());
        assert!(registry
            .reload_with_source_preload(&controller, snapshots.path())?
            .is_none());
        let removed = tempfile::tempdir()?;
        publish_fixture(root.path(), removed.path())?;
        assert!(registry
            .reload_with_source_preload(&controller, snapshots.path())
            .is_err());
        Ok(())
    }

    #[test]
    fn source_keyed_registry_accepts_catalog_shrink() -> anyhow::Result<()> {
        let mut fixture =
            RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
        let original = fixture.initial().clone();
        let registry =
            DeploymentRegistry::load_with_selection(fixture.root().to_owned(), false, true, true)?;
        let latest = fixture.publish_additive_generation()?;
        let controller = generated_module_cache_test_controller(128 << 20, 192 << 20)?;
        registry.reload_with_source_preload(
            &controller,
            fixture.serialized_module_snapshot_directory(),
        )?;
        let selector = |generation: &RuntimeRegistrySourceKeyedCapabilityGeneration| {
            SourceKeyedGenerationSelector {
                deployment_sha256: generation.deployment_sha256.clone(),
                generation_manifest_sha256: generation.generation_file_sha256.clone(),
                generation_sha256: generation.generation_sha256.clone(),
                source_package_runtime_content_sha256: generation
                    .source_package_runtime_content_sha256
                    .clone(),
            }
        };
        {
            let state = registry.state.read();
            let catalog = state.source_keyed_catalog.as_ref().unwrap();
            assert!(catalog
                .descriptor_by_selector(&selector(&original))
                .is_some());
            assert!(catalog.descriptor_by_selector(&selector(&latest)).is_some());
        }
        fixture.publish_latest_only_catalog()?;
        let residency = registry.source_keyed_residency.as_ref().unwrap();
        residency.observe_selected(Some(selector(&original)), Timestamp::MIN)?;
        assert!(registry
            .reload_with_source_preload(&controller, fixture.serialized_module_snapshot_directory())
            .is_err());
        residency.observe_selected(Some(selector(&latest)), Timestamp::try_from(1_u64)?)?;
        let reserved = DeploymentGeneration::from_legacy(
            fixture.root().to_owned(),
            ValidatedDeploymentManifest::empty_for_test(&original.deployment_sha256),
        );
        let pin = Arc::new(SourceKeyedActivationPin {
            generation: Arc::clone(&reserved),
        });
        {
            let mut state = residency.state.lock();
            state.insert_and_evict_lru(selector(&original), reserved);
            state.activation_pins.push(Arc::downgrade(&pin));
        }
        assert!(registry
            .reload_with_source_preload(&controller, fixture.serialized_module_snapshot_directory())
            .is_err());
        drop(pin);
        registry.reload_with_source_preload(
            &controller,
            fixture.serialized_module_snapshot_directory(),
        )?;
        let state = registry.state.read();
        let catalog = state.source_keyed_catalog.as_ref().unwrap();
        assert!(catalog
            .descriptor_by_selector(&selector(&original))
            .is_none());
        assert!(catalog.descriptor_by_selector(&selector(&latest)).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn source_keyed_queued_load_expires_without_starting_artifact_work() -> anyhow::Result<()>
    {
        let fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
        let registry =
            DeploymentRegistry::load_with_selection(fixture.root().to_owned(), false, true, true)?;
        let residency = registry.source_keyed_residency.as_ref().unwrap();
        let initial = fixture.initial();
        let descriptor = registry
            .state
            .read()
            .source_keyed_catalog
            .as_ref()
            .unwrap()
            .descriptor(
                &initial.source_package_runtime_content_sha256,
                &SourceKeyedRuntimeGenerationIdentity {
                    deployment_sha256: initial.deployment_sha256.clone(),
                    generation_manifest_sha256: initial.generation_file_sha256.clone(),
                    generation_sha256: initial.generation_sha256.clone(),
                },
            )
            .unwrap();
        let selector = SourceKeyedGenerationSelector::from_descriptor(&descriptor)?;
        let _blocked = residency.cold_load.acquire().await?;
        let result = residency
            .ensure(
                &registry,
                selector,
                descriptor,
                tokio::time::Instant::now() + Duration::from_millis(20),
            )
            .await;
        let error = result.err().expect("expired load succeeded");
        assert!(error
            .downcast_ref::<SourceKeyedRuntimeCapacityUnavailable>()
            .is_some());
        assert!(residency.state.lock().resident.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn source_keyed_abandoned_queue_does_not_begin_an_unselected_load() -> anyhow::Result<()>
    {
        let fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
        let registry =
            DeploymentRegistry::load_with_selection(fixture.root().to_owned(), false, true, true)?;
        let residency = registry.source_keyed_residency.as_ref().unwrap().clone();
        let initial = fixture.initial();
        let descriptor = registry
            .state
            .read()
            .source_keyed_catalog
            .as_ref()
            .unwrap()
            .descriptor(
                &initial.source_package_runtime_content_sha256,
                &SourceKeyedRuntimeGenerationIdentity {
                    deployment_sha256: initial.deployment_sha256.clone(),
                    generation_manifest_sha256: initial.generation_file_sha256.clone(),
                    generation_sha256: initial.generation_sha256.clone(),
                },
            )
            .unwrap();
        let selector = SourceKeyedGenerationSelector::from_descriptor(&descriptor)?;
        let blocked = residency.cold_load.acquire().await?;
        let task_registry = Arc::clone(&registry);
        let task_residency = Arc::clone(&residency);
        let task_selector = selector.clone();
        let task = tokio::spawn(async move {
            task_residency
                .ensure(
                    &task_registry,
                    task_selector,
                    descriptor,
                    tokio::time::Instant::now() + Duration::from_secs(30),
                )
                .await
        });
        let flight = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(flight) = residency.inflight.lock().get(&selector).cloned() {
                    break flight;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        task.abort();
        assert!(task.await.is_err());
        assert_eq!(flight.waiters.load(Ordering::Acquire), 0);
        drop(blocked);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), flight.result).await?,
            Err(SourceKeyedLoadFailure::CapacityUnavailable)
        ));
        assert!(residency.state.lock().resident.is_empty());
        Ok(())
    }

    #[test]
    fn unpublished_registry_is_not_missing_malformed_or_primary() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        for source_keyed in [false, true] {
            assert!(DeploymentRegistry::load_with_selection(
                root.path().join("missing"),
                false,
                true,
                source_keyed
            )
            .is_err());
            assert!(DeploymentRegistry::load_with_selection(
                root.path().to_owned(),
                true,
                false,
                source_keyed
            )
            .is_err());
            std::fs::write(root.path().join("unexpected"), b"incomplete")?;
            assert!(DeploymentRegistry::load_with_selection(
                root.path().to_owned(),
                false,
                true,
                source_keyed
            )
            .is_err());
            std::fs::remove_file(root.path().join("unexpected"))?;
        }
        Ok(())
    }

    #[test]
    fn descriptor_only_startup_does_not_open_an_unused_generation() -> anyhow::Result<()> {
        let mut fixture =
            RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
        let added = fixture.publish_additive_generation()?;
        let withheld = tempfile::tempdir()?;
        // Startup authenticates only the catalog. An unavailable historical
        // generation cannot block a process whose database has not selected it.
        std::fs::rename(
            fixture
                .root()
                .join("generations")
                .join(&added.deployment_sha256)
                .join(&added.generation_sha256)
                .join("generation.json"),
            withheld.path().join("generation.json"),
        )?;
        let registry =
            DeploymentRegistry::load_with_selection(fixture.root().to_owned(), false, true, true)?;
        assert!(registry.uses_source_keyed_selection());
        Ok(())
    }

    #[test]
    fn shadow_only_admission_routes_only_the_shadow_lane() {
        assert!(RuntimeRegistryAdmission::ShadowOnly.allows(RegistryUse::Shadow));
        assert!(!RuntimeRegistryAdmission::ShadowOnly.allows(RegistryUse::Primary));
        assert!(RuntimeRegistryAdmission::PrimaryAdmitted.allows(RegistryUse::Shadow));
        assert!(RuntimeRegistryAdmission::PrimaryAdmitted.allows(RegistryUse::Primary));
        #[cfg(any(test, feature = "testing"))]
        assert!(RuntimeRegistryAdmission::PrimaryAdmitted.allows(RegistryUse::CompatibilityShadow));
    }

    #[test]
    fn source_catalog_selection_uses_shadow_admission_until_primary_routing() {
        assert_eq!(registry_use_for_selection(true), RegistryUse::Primary);
        assert_eq!(registry_use_for_selection(false), RegistryUse::Shadow);
    }
}

#[cfg(test)]
mod reuse_instances_configuration_tests {
    use super::*;

    fn reuse_instances(
        generated_runtime_configured: bool,
        value: Result<String, std::env::VarError>,
    ) -> anyhow::Result<bool> {
        configured_reuse_instances(
            generated_runtime_configured,
            optional_boolean_environment_value(REUSE_INSTANCES_ENV, value)?,
        )
    }

    #[test]
    fn default_v8_configuration_does_not_require_a_generated_runtime() -> anyhow::Result<()> {
        assert!(!reuse_instances(
            false,
            Err(std::env::VarError::NotPresent)
        )?);
        assert!(reuse_instances(true, Err(std::env::VarError::NotPresent))?);
        assert!(!reuse_instances(false, Ok("0".to_owned()))?);
        assert!(reuse_instances(true, Ok("1".to_owned()))?);

        let error = reuse_instances(false, Ok("1".to_owned()))
            .expect_err("unconfigured instance reuse unexpectedly succeeded");
        assert_eq!(
            error.to_string(),
            format!(
                "{REUSE_INSTANCES_ENV}=1 requires a configured generated package or deployment \
                 registry"
            )
        );

        let error = reuse_instances(false, Ok("enabled".to_owned()))
            .expect_err("invalid reuse configuration unexpectedly succeeded");
        assert_eq!(
            error.to_string(),
            format!("{REUSE_INSTANCES_ENV} must be 0 or 1")
        );
        Ok(())
    }
}

#[cfg(test)]
mod source_keyed_capability_scale_tests {
    use super::{
        super::super::wasm_udf_package::tests::{
            RuntimeRegistrySourceKeyedCapabilityFixture,
            RuntimeRegistrySourceKeyedCapabilityGeneration,
        },
        *,
    };

    const SELECTOR_COUNT: usize = 1_421;

    fn residency_selector(marker: char) -> SourceKeyedGenerationSelector {
        SourceKeyedGenerationSelector {
            deployment_sha256: marker.to_string().repeat(64),
            generation_manifest_sha256: marker.to_string().repeat(64),
            generation_sha256: marker.to_string().repeat(64),
            source_package_runtime_content_sha256: marker.to_string().repeat(64),
        }
    }

    fn residency_generation(marker: char) -> Arc<DeploymentGeneration> {
        DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/source-keyed-residency"),
            ValidatedDeploymentManifest::empty_for_test(&marker.to_string().repeat(64)),
        )
    }

    #[test]
    fn source_keyed_residency_evicts_the_lru_even_while_an_invocation_pins_it() {
        let mut state = SourceKeyedResidencyState::default();
        let first = residency_generation('1');
        let pinned_first = Arc::clone(&first);
        let second = residency_generation('2');
        let third = residency_generation('3');

        assert!(state
            .insert_and_evict_lru(residency_selector('1'), first)
            .is_none());
        assert!(state
            .insert_and_evict_lru(residency_selector('2'), second)
            .is_none());
        let victim = state
            .insert_and_evict_lru(residency_selector('3'), third)
            .expect("over-capacity residency did not select an LRU victim");

        assert!(Arc::ptr_eq(&victim.generation, &pinned_first));
        assert_eq!(state.resident.len(), 2);
        assert!(!pinned_first.is_retired());
    }

    #[test]
    fn source_keyed_admission_releases_the_lru_slot_before_candidate_publication() {
        let mut state = SourceKeyedResidencyState::default();
        let first = residency_generation('1');
        let pinned_first = Arc::clone(&first);
        let second = residency_generation('2');
        let pinned_second = Arc::clone(&second);
        assert!(state.evict_lru_before_preload().unwrap().is_none());
        assert!(state
            .insert_and_evict_lru(residency_selector('1'), first)
            .is_none());
        assert!(state.evict_lru_before_preload().unwrap().is_none());
        assert!(state
            .insert_and_evict_lru(residency_selector('2'), second)
            .is_none());

        let victim = state
            .evict_lru_before_preload()
            .unwrap()
            .expect("full residency did not select a victim");
        assert!(Arc::ptr_eq(&victim.generation, &pinned_first));
        drop(victim);
        assert_eq!(state.resident.len(), 1);
        assert!(Arc::ptr_eq(
            &state.resident[&residency_selector('2')].generation,
            &pinned_second
        ));
        assert_eq!(Arc::strong_count(&pinned_first), 1);

        // A failed candidate leaves the remaining resident usable. A retry
        // uses the already available slot instead of evicting that resident.
        assert!(state.evict_lru_before_preload().unwrap().is_none());
        assert!(state
            .insert_and_evict_lru(residency_selector('3'), residency_generation('3'))
            .is_none());
        assert_eq!(state.resident.len(), 2);
    }

    #[test]
    fn source_keyed_memory_admission_retires_only_the_other_incarnation() {
        let residency = SourceKeyedResidency::new(
            PathBuf::from("/unused/source-keyed-residency"),
            RegistryUse::Shadow,
        );
        let selected = residency_generation('1');
        let other = residency_generation('2');
        {
            let mut state = residency.state.lock();
            assert!(state
                .insert_and_evict_lru(residency_selector('1'), Arc::clone(&selected))
                .is_none());
            assert!(state
                .insert_and_evict_lru(residency_selector('2'), Arc::clone(&other))
                .is_none());
        }

        // The selected incarnation remains usable even when it is the LRU.
        assert!(residency.retire_other_generation_for_admission(&selected));
        assert!(!selected.is_retired());
        assert!(other.is_retired());
        assert!(Arc::ptr_eq(
            &residency.resident(&residency_selector('1')).unwrap(),
            &selected,
        ));
        assert!(residency.resident(&residency_selector('2')).is_none());
        assert!(!residency.retire_other_generation_for_admission(&selected));
        let retired = residency.take_retired();
        assert_eq!(retired.len(), 1);
        assert!(Arc::ptr_eq(&retired[0], &other));
    }

    #[test]
    fn source_keyed_activation_pin_survives_competing_admission_and_stale_selection(
    ) -> anyhow::Result<()> {
        let residency = SourceKeyedResidency::new(
            PathBuf::from("/unused/source-keyed-residency"),
            RegistryUse::Shadow,
        );
        let serving = residency_generation('1');
        let candidate = residency_generation('2');
        let competing = residency_generation('3');
        {
            let mut state = residency.state.lock();
            assert!(state
                .insert_and_evict_lru(residency_selector('1'), Arc::clone(&serving))
                .is_none());
            assert!(state
                .insert_and_evict_lru(residency_selector('2'), Arc::clone(&candidate))
                .is_none());
        }
        let before = Timestamp::try_from(10_u64)?;
        let committed = Timestamp::try_from(11_u64)?;
        assert!(residency.observe_selected(Some(residency_selector('1')), before)?);
        let pin = Arc::new(SourceKeyedActivationPin {
            generation: Arc::clone(&candidate),
        });
        residency
            .state
            .lock()
            .activation_pins
            .push(Arc::downgrade(&pin));

        // Candidate admission and pressure cannot revoke the owner whose
        // database commit is in flight, or evict the still-serving generation.
        assert!(residency.state.lock().evict_lru_before_preload().is_err());
        assert!(!residency.retire_other_generation_for_admission(&competing));
        assert!(!serving.is_retired());
        assert!(!candidate.is_retired());

        assert!(residency.observe_selected(Some(residency_selector('2')), committed)?);
        drop(pin);
        assert!(!residency.observe_selected(Some(residency_selector('1')), before)?);
        assert!(residency.owns_replenishment(&candidate));
        assert!(!residency.owns_replenishment(&serving));
        assert!(residency.retire_other_generation_for_admission(&competing));
        assert!(serving.is_retired());
        assert!(!candidate.is_retired());
        assert!(residency.observe_selected(None, Timestamp::try_from(12_u64)?)?);
        assert!(!residency.owns_replenishment(&candidate));
        Ok(())
    }

    pub(super) struct GeneratedRouteCacheCleanup {
        pub(super) routes: Vec<GeneratedRouteIdentity>,
    }

    impl Drop for GeneratedRouteCacheCleanup {
        fn drop(&mut self) {
            let mut cache = GENERATED_ROUTED_MODULES.lock();
            for route in &self.routes {
                drop(cache.remove(route, "module_cache_evicted_source_keyed_scale_test"));
            }
        }
    }

    pub(super) fn route_identity(
        generation: &RuntimeRegistrySourceKeyedCapabilityGeneration,
    ) -> GeneratedRouteIdentity {
        GeneratedRouteIdentity::DeploymentExport {
            deployment_sha256: generation.deployment_sha256.clone(),
            generation_sha256: generation.generation_sha256.clone(),
            package_key: generation.package_key.clone(),
            entry: DeploymentEntryIdentity::CapabilityPackage,
        }
    }

    pub(super) fn selector_module() -> Vec<u8> {
        let mut types = EncodedTypeSection::new();
        types
            .ty()
            .function(std::iter::empty(), [EncodedValType::I32]);
        types.ty().function(
            [EncodedValType::I32, EncodedValType::I32],
            [EncodedValType::I32],
        );
        types
            .ty()
            .function([EncodedValType::I64], [EncodedValType::I64]);
        types.ty().function(
            [EncodedValType::I32, EncodedValType::I32],
            std::iter::empty(),
        );
        types.ty().function(std::iter::empty(), std::iter::empty());
        types
            .ty()
            .function([EncodedValType::I64], [EncodedValType::I32]);
        types.ty().function(
            [EncodedValType::I32, EncodedValType::I32],
            [EncodedValType::I64],
        );

        let mut imports = EncodedImportSection::new();
        for (name, ty) in [
            ("convex_guest_value_request_len", 0),
            ("convex_guest_value_request_copy", 1),
            ("convex_guest_value_encode", 2),
            ("convex_guest_value_result", 3),
            ("convex_guest_value_decode", 6),
        ] {
            imports.import("convex", name, EncodedEntityType::Function(ty));
        }

        let imported_function_count = 5;
        let mut functions = EncodedFunctionSection::new();
        functions.function(4);
        functions.function(0);
        functions.function(4);
        functions.function(5);
        functions.function(0);

        let mut memory = EncodedMemorySection::new();
        memory.memory(EncodedMemoryType {
            minimum: 1,
            maximum: Some(1),
            memory64: false,
            shared: false,
            page_size_log2: None,
        });

        let mut exports = EncodedExportSection::new();
        exports.export("memory", EncodedExportKind::Memory, 0);
        exports.export(
            "_initialize",
            EncodedExportKind::Func,
            imported_function_count,
        );
        exports.export(
            "convex_wasm_udf_run",
            EncodedExportKind::Func,
            imported_function_count + 1,
        );
        exports.export(
            "convex_wasm_udf_destroy_runtime",
            EncodedExportKind::Func,
            imported_function_count + 2,
        );
        exports.export(
            "convex_wasm_select_entry",
            EncodedExportKind::Func,
            imported_function_count + 3,
        );
        exports.export(
            "convex_wasm_udf_prepare_selected_entry",
            EncodedExportKind::Func,
            imported_function_count + 4,
        );

        let mut initialize = EncodedFunction::new([]);
        initialize.instruction(&EncodedInstruction::End);
        let mut run = EncodedFunction::new([]);
        run.instruction(&EncodedInstruction::I32Const(0));
        run.instruction(&EncodedInstruction::End);
        let mut destroy = EncodedFunction::new([]);
        destroy.instruction(&EncodedInstruction::End);
        let mut select = EncodedFunction::new([]);
        select.instruction(&EncodedInstruction::I32Const(0));
        select.instruction(&EncodedInstruction::End);
        let mut prepare = EncodedFunction::new([]);
        prepare.instruction(&EncodedInstruction::I32Const(0));
        prepare.instruction(&EncodedInstruction::End);

        let mut code = EncodedCodeSection::new();
        code.function(&initialize);
        code.function(&run);
        code.function(&destroy);
        code.function(&select);
        code.function(&prepare);

        let mut module = EncodedModule::new();
        module
            .section(&types)
            .section(&imports)
            .section(&functions)
            .section(&memory)
            .section(&exports)
            .section(&code);
        module.finish()
    }

    fn assert_one_cache_entry(
        generation: &RuntimeRegistrySourceKeyedCapabilityGeneration,
    ) -> anyhow::Result<()> {
        let route = route_identity(generation);
        let cache = GENERATED_ROUTED_MODULES.lock();
        let generation_entries = cache
            .entries
            .iter()
            .filter(|(identity, _)| {
                matches!(
                    identity,
                    GeneratedRouteIdentity::DeploymentExport {
                        deployment_sha256,
                        generation_sha256,
                        package_key,
                        ..
                    } if deployment_sha256 == &generation.deployment_sha256
                        && generation_sha256 == &generation.generation_sha256
                        && package_key == &generation.package_key
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(generation_entries.len(), 1);
        assert_eq!(generation_entries[0].0, &route);
        let routed = &cache
            .entries
            .get(&route)
            .context("source-keyed capability package was not cached")?
            .routed;
        assert_eq!(Arc::strong_count(routed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn source_keyed_started_load_survives_its_last_waiter_deadline() -> anyhow::Result<()> {
        let engine = new_generated_engine(WasmMeteringMode::EpochChecks)?;
        let core_wasm = selector_module();
        let serialized_module = engine
            .precompile_module(&core_wasm)
            .map_err(wasmtime_anyhow)?;
        let fixture =
            RuntimeRegistrySourceKeyedCapabilityFixture::create(&core_wasm, &serialized_module, 1)?;
        let _cleanup = GeneratedRouteCacheCleanup {
            routes: vec![route_identity(fixture.initial())],
        };
        let registry =
            DeploymentRegistry::load_with_selection(fixture.root().to_owned(), false, true, true)?;
        let residency = registry.source_keyed_residency.as_ref().unwrap().clone();
        let controller = generated_module_cache_test_controller(128 << 20, 192 << 20)?;
        // An unchanged catalog reload returns before configuring residency.
        registry.configure_source_keyed_residency(
            &controller,
            fixture.serialized_module_snapshot_directory(),
        )?;
        let initial = fixture.initial();
        let descriptor = registry
            .state
            .read()
            .source_keyed_catalog
            .as_ref()
            .unwrap()
            .descriptor(
                &initial.source_package_runtime_content_sha256,
                &SourceKeyedRuntimeGenerationIdentity {
                    deployment_sha256: initial.deployment_sha256.clone(),
                    generation_manifest_sha256: initial.generation_file_sha256.clone(),
                    generation_sha256: initial.generation_sha256.clone(),
                },
            )
            .unwrap();
        let selector = SourceKeyedGenerationSelector::from_descriptor(&descriptor)?;
        // Closing the existing observation hook makes the detached owner panic
        // before artifact work. Its failed flight must not poison this selector.
        let (reached, started) = tokio::sync::oneshot::channel();
        let (_resume, paused) = tokio::sync::oneshot::channel();
        drop(started);
        *residency.started_load_pause.lock() = Some((reached, paused));
        assert!(residency
            .ensure(
                &registry,
                selector.clone(),
                Arc::clone(&descriptor),
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .is_err());
        assert!(residency.inflight.lock().is_empty());
        assert_eq!(residency.cold_load.available_permits(), 1);

        let (reached, started) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        *residency.started_load_pause.lock() = Some((reached, paused));
        let task_registry = Arc::clone(&registry);
        let task_residency = Arc::clone(&residency);
        let task_selector = selector.clone();
        let task = tokio::spawn(async move {
            task_residency
                .ensure(
                    &task_registry,
                    task_selector,
                    descriptor,
                    tokio::time::Instant::now() + Duration::from_millis(20),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), started).await??;
        let error = task.await?.err().expect("paused caller did not time out");
        assert!(error
            .downcast_ref::<SourceKeyedRuntimeCapacityUnavailable>()
            .is_some());
        let flight = residency.inflight.lock().get(&selector).unwrap().clone();
        assert_eq!(flight.waiters.load(Ordering::Acquire), 0);
        resume.send(()).expect("started loader lost ownership");
        let generation = tokio::time::timeout(Duration::from_secs(1), flight.result)
            .await?
            .map_err(|error| match error {
                SourceKeyedLoadFailure::CapacityUnavailable => {
                    SourceKeyedRuntimeCapacityUnavailable.into()
                },
                SourceKeyedLoadFailure::Failed(error) => anyhow::anyhow!("{error:#}"),
            })
            .context("started load failed after caller expiry")?;
        assert!(Arc::ptr_eq(
            &generation,
            &residency.resident(&selector).unwrap()
        ));
        assert!(generation.is_load_verified());
        assert_one_cache_entry(initial)?;
        assert!(!generation.is_retired());
        Ok(())
    }

    #[tokio::test]
    async fn source_keyed_lazy_load_and_additive_reload_dedupe_1421_capability_selectors(
    ) -> anyhow::Result<()> {
        let engine = new_generated_engine(WasmMeteringMode::EpochChecks)?;
        let core_wasm = selector_module();
        let serialized_module = engine
            .precompile_module(&core_wasm)
            .map_err(wasmtime_anyhow)?;
        let mut fixture = RuntimeRegistrySourceKeyedCapabilityFixture::create(
            &core_wasm,
            &serialized_module,
            SELECTOR_COUNT,
        )?;
        let initial = fixture.initial().clone();
        let initial_identity = SourceKeyedRuntimeGenerationIdentity {
            deployment_sha256: initial.deployment_sha256.clone(),
            generation_manifest_sha256: initial.generation_file_sha256.clone(),
            generation_sha256: initial.generation_sha256.clone(),
        };
        assert_eq!(initial.selector_count, SELECTOR_COUNT);
        let mut cleanup = GeneratedRouteCacheCleanup {
            routes: vec![route_identity(&initial)],
        };

        let registry =
            DeploymentRegistry::load_with_selection(fixture.root().to_owned(), true, false, true)?;
        let controller = generated_module_cache_test_controller(128 << 20, 192 << 20)?;
        registry.configure_source_keyed_residency(
            &controller,
            fixture.serialized_module_snapshot_directory(),
        )?;
        assert_eq!(
            registry.source_keyed_runtime_readiness(
                &initial.source_package_runtime_content_sha256,
                &initial_identity,
            )?,
            SourceKeyedRuntimeReadiness::NotStaged,
        );
        assert_eq!(
            registry.source_keyed_runtime_generation_identity(
                &initial.source_package_runtime_content_sha256,
                &initial_identity,
            )?,
            None,
        );
        assert!(registry.query_shadow_registry().selection.is_none());
        let initial_selection = registry
            .ensure_source_keyed_generation(
                &initial.source_package_runtime_content_sha256,
                &initial_identity,
                Some(Timestamp::MIN),
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await?
            .context("initial source-keyed generation was not selected")?;
        assert_eq!(
            registry.source_keyed_runtime_readiness(
                &initial.source_package_runtime_content_sha256,
                &initial_identity,
            )?,
            SourceKeyedRuntimeReadiness::Ready,
        );
        assert_eq!(
            registry.source_keyed_runtime_generation_identity(
                &initial.source_package_runtime_content_sha256,
                &initial_identity,
            )?,
            Some(initial_identity.clone()),
        );
        assert_eq!(
            initial_selection.generation_sha256,
            initial.generation_sha256,
        );
        assert_eq!(
            initial_selection.registry.selected_wasm_exports().len(),
            SELECTOR_COUNT,
        );
        assert_one_cache_entry(&initial)?;

        let added = fixture.publish_additive_generation()?;
        let added_identity = SourceKeyedRuntimeGenerationIdentity {
            deployment_sha256: added.deployment_sha256.clone(),
            generation_manifest_sha256: added.generation_file_sha256.clone(),
            generation_sha256: added.generation_sha256.clone(),
        };
        cleanup.routes.push(route_identity(&added));
        assert_eq!(
            registry.source_keyed_runtime_readiness(
                &added.source_package_runtime_content_sha256,
                &added_identity,
            )?,
            SourceKeyedRuntimeReadiness::NotStaged,
        );
        // Retained generations use their authenticated state, but an added
        // generation must still load its own manifest before becoming ready.
        let withheld_manifests = tempfile::tempdir()?;
        let initial_manifest = fixture
            .root()
            .join("generations")
            .join(&initial.deployment_sha256)
            .join(&initial.generation_sha256)
            .join("generation.json");
        let added_manifest = fixture
            .root()
            .join("generations")
            .join(&added.deployment_sha256)
            .join(&added.generation_sha256)
            .join("generation.json");
        std::fs::rename(
            &initial_manifest,
            withheld_manifests.path().join("initial.json"),
        )?;
        std::fs::rename(
            &added_manifest,
            withheld_manifests.path().join("added.json"),
        )?;
        anyhow::ensure!(
            registry
                .reload_source_keyed_catalog(
                    &controller,
                    fixture.serialized_module_snapshot_directory(),
                )?
                .is_none(),
            "descriptor-only catalog reload unexpectedly retired a generation",
        );
        assert_eq!(
            registry.source_keyed_runtime_readiness(
                &initial.source_package_runtime_content_sha256,
                &initial_identity,
            )?,
            SourceKeyedRuntimeReadiness::Ready,
        );
        assert_eq!(
            registry.source_keyed_runtime_readiness(
                &added.source_package_runtime_content_sha256,
                &added_identity,
            )?,
            SourceKeyedRuntimeReadiness::NotStaged,
        );
        std::fs::rename(
            withheld_manifests.path().join("added.json"),
            &added_manifest,
        )?;
        let retained_selection = registry
            .ensure_source_keyed_generation(
                &initial.source_package_runtime_content_sha256,
                &initial_identity,
                Some(Timestamp::try_from(1_u64)?),
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await?
            .context("retained source-keyed generation was not selected")?;
        assert!(Arc::ptr_eq(&initial_selection, &retained_selection));
        let added_selection = registry
            .ensure_source_keyed_generation(
                &added.source_package_runtime_content_sha256,
                &added_identity,
                Some(Timestamp::try_from(2_u64)?),
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await?
            .context("additive source-keyed generation was not selected")?;
        assert_eq!(added_selection.generation_sha256, added.generation_sha256);
        assert_eq!(
            added_selection.registry.selected_wasm_exports().len(),
            added.selector_count,
        );
        for generation in [&initial, &added] {
            assert_eq!(
                registry.source_keyed_runtime_readiness(
                    &generation.source_package_runtime_content_sha256,
                    &SourceKeyedRuntimeGenerationIdentity {
                        deployment_sha256: generation.deployment_sha256.clone(),
                        generation_manifest_sha256: generation.generation_file_sha256.clone(),
                        generation_sha256: generation.generation_sha256.clone(),
                    },
                )?,
                SourceKeyedRuntimeReadiness::Ready,
            );
            assert_one_cache_entry(generation)?;
        }

        // A late transaction from the preceding source snapshot may still
        // select its retained generation, but it must not roll the displayed
        // shadow selection back. Reusing a begin timestamp for a different
        // generation is an integrity failure rather than a last-writer win.
        let late_initial_selection = registry
            .ensure_source_keyed_generation(
                &initial.source_package_runtime_content_sha256,
                &initial_identity,
                Some(Timestamp::try_from(1_u64)?),
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await?
            .context("late preceding source-keyed generation was not selectable")?;
        assert!(Arc::ptr_eq(&late_initial_selection, &retained_selection));
        assert_eq!(
            registry.query_shadow_registry().generation_sha256(),
            added.generation_sha256,
        );
        assert!(registry
            .ensure_source_keyed_generation(
                &initial.source_package_runtime_content_sha256,
                &initial_identity,
                Some(Timestamp::try_from(2_u64)?),
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await
            .is_err());

        let weak_initial = Arc::downgrade(&initial_selection);
        let weak_added = Arc::downgrade(&added_selection);
        drop(initial_selection);
        drop(retained_selection);
        drop(late_initial_selection);
        drop(added_selection);
        drop(registry);
        drop(cleanup);
        assert!(weak_initial.upgrade().is_none());
        assert!(weak_added.upgrade().is_none());
        Ok(())
    }
}
