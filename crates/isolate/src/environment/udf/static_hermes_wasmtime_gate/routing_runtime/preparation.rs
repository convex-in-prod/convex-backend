use common::wasm_runtime_policy::{
    PrimaryRuntime,
    RuntimePolicy,
};
use database::Database;
use futures::{
    stream::FuturesUnordered,
    StreamExt,
};

#[cfg(not(any(test, feature = "testing")))]
use super::super::PRODUCTION_GATE_METRICS;
use super::{
    source_keyed_deployment_catalog::SourceKeyedGenerationSelector,
    *,
};
use crate::environment::udf::generated_wasm_memory::PreparationMemoryCost;

// This lock protects only the immutable route/group ownership table. Slow
// database, memory, CPU, and guest waits run outside it.
static PREPARATION_LOCK: LazyLock<parking_lot::Mutex<Vec<PreparedGeneration>>> =
    LazyLock::new(|| parking_lot::Mutex::new(Vec::new()));
static PREPARATION_FLIGHTS: LazyLock<
    parking_lot::Mutex<BTreeMap<PreparationFlightKey, Arc<PreparationFlight>>>,
> = LazyLock::new(|| parking_lot::Mutex::new(BTreeMap::new()));
pub(super) static PREPARATION_STARTED: AtomicBool = AtomicBool::new(false);
static DEMAND: LazyLock<parking_lot::Mutex<PreparationDemand>> =
    LazyLock::new(|| parking_lot::Mutex::new(PreparationDemand::default()));
const DEMAND_WINDOW: Duration = Duration::from_secs(60);
const DEMAND_BUCKETS: usize = 5;
pub(super) const EXTRA_POPULATION_WINDOW: Duration = Duration::from_secs(30);
const PREPARATION_CONCURRENCY: usize = 2;
static PREPARATION_LIMIT: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(PREPARATION_CONCURRENCY);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PreparationFlightKey {
    generation: usize,
}

#[derive(Clone)]
struct PreparationFlight {
    mode: PreparationMode,
    progress: Arc<parking_lot::Mutex<&'static str>>,
    targets: Arc<parking_lot::Mutex<BTreeMap<GeneratedPoolIdentity, usize>>>,
    baseline_ready: Arc<AtomicBool>,
    result: Shared<BoxFuture<'static, Result<GenerationPreparation, Arc<anyhow::Error>>>>,
}

struct PreparationFlightOwner(Arc<DeploymentGeneration>);

impl Drop for PreparationFlightOwner {
    fn drop(&mut self) {
        // Keep the incarnation alive through removal, including panic unwinding.
        // A detached producer must not leave a completed flight or a reusable
        // pointer key behind when every caller has already gone away.
        PREPARATION_FLIGHTS.lock().remove(&PreparationFlightKey {
            generation: Arc::as_ptr(&self.0) as usize,
        });
    }
}

struct PreparedGeneration {
    generation: Arc<DeploymentGeneration>,
    groups: Vec<PreparationGroup>,
}

#[derive(Clone)]
struct PreparationGroup {
    routed: Arc<GeneratedRoutedModule>,
    members: BTreeSet<DemandEntry>,
    exports: BTreeMap<String, BTreeMap<String, ManifestUdfKind>>,
    queries: bool,
    mutations: bool,
    memory_cost: Arc<parking_lot::Mutex<Option<PreparationMemoryCost>>>,
    retry_at: Arc<parking_lot::Mutex<Option<Instant>>>,
    // Capacity recovery can clear the retry delay to allow baseline reordering;
    // only successful preparation clears an observed initializer failure.
    failed: Arc<parking_lot::Mutex<Option<PreparationFailure>>>,
    last_started: Arc<parking_lot::Mutex<Option<Instant>>>,
    images: Arc<super::super::runtime_lifecycle::GeneratedPreparedImageCache>,
    in_progress: Arc<AtomicUsize>,
}

#[derive(Clone, Copy)]
enum PreparationFailure {
    Engine(StaticHermesWasmFallbackReason),
    Other,
}

impl PreparationGroup {
    fn record_success(&self) {
        *self.retry_at.lock() = None;
        *self.failed.lock() = None;
    }

    fn record_failure(&self, error: &anyhow::Error) {
        *self.retry_at.lock() = Some(Instant::now() + GENERATED_FAILURE_LOG_INTERVAL);
        // Retain only a disposed engine failure's bounded category. A database,
        // application or budget failure must not become a retryable initializer.
        *self.failed.lock() = Some(
            match error.downcast_ref::<StaticHermesWasmFallbackReason>() {
                Some(reason) => PreparationFailure::Engine(*reason),
                None => PreparationFailure::Other,
            },
        );
        // A primary waiting for this group's first Store must see an
        // initializer failure, rather than expire as ordinary saturation.
        GENERATED_GENERATION_RETIREMENT_NOTIFY.notify_waiters();
    }
}

pub(super) fn failed_preparation(routed: &GeneratedRoutedModule) -> Option<anyhow::Error> {
    PREPARATION_LOCK.lock().iter().find_map(|prepared| {
        if !routed
            .generation
            .as_ref()
            .is_some_and(|generation| Arc::ptr_eq(generation, &prepared.generation))
        {
            return None;
        }
        prepared.groups.iter().find_map(|group| {
            if group.routed.pool_identity != routed.pool_identity {
                return None;
            }
            group.failed.lock().map(|failure| {
                let error = anyhow::anyhow!("generated Wasm instance preparation failed")
                    .context(StaticHermesWasmExecutionFailure::RuntimeInitialization);
                match failure {
                    PreparationFailure::Engine(reason) => error.context(reason),
                    PreparationFailure::Other => error,
                }
            })
        })
    })
}

#[derive(Default)]
struct PreparationDemand {
    // Original observations survive retirement without retaining compiled graphs.
    history: [BTreeMap<(String, Vec<DemandEntry>), Arc<parking_lot::Mutex<GroupDemand>>>; 3],
    primary_history:
        [BTreeMap<(String, Vec<DemandEntry>), Arc<parking_lot::Mutex<GroupDemand>>>; 3],
    pools: BTreeMap<GeneratedPoolIdentity, RegisteredDemand>,
    selected_generation: Weak<DeploymentGeneration>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DemandScope {
    Query = 0,
    Mutation = 1,
    Both = 2,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DemandEntry {
    module: String,
    export: String,
    kind: DemandScope,
}

impl From<ManifestUdfKind> for DemandScope {
    fn from(kind: ManifestUdfKind) -> Self {
        match kind {
            ManifestUdfKind::Query => Self::Query,
            ManifestUdfKind::Mutation => Self::Mutation,
        }
    }
}

struct RegisteredDemand {
    generation: Weak<DeploymentGeneration>,
    members: Vec<DemandEntry>,
    exports: BTreeMap<String, BTreeMap<String, ManifestUdfKind>>,
    demand: [Arc<parking_lot::Mutex<GroupDemand>>; 3],
    primary_demand: [Arc<parking_lot::Mutex<GroupDemand>>; 3],
    checked_out: Arc<AtomicUsize>,
    reserved: Arc<AtomicUsize>,
    saturation: Option<SaturationDemand>,
}

#[derive(Default)]
pub(super) struct PopulationOccupancy {
    pub(super) active: usize,
    pub(super) reserved: usize,
}

// Reservation acquisition and release run under the pool lock, including a
// failed acquisition's unwind. These owners never contribute demand history.
pub(super) struct ReservedPopulation {
    reserved: Arc<AtomicUsize>,
    count: usize,
}

impl Drop for ReservedPopulation {
    fn drop(&mut self) {
        assert!(
            self.reserved.fetch_sub(self.count, Ordering::AcqRel) >= self.count,
            "Wasm reserved population accounting underflow"
        );
    }
}

struct SaturationDemand {
    capacity: usize,
    expires: Instant,
}

impl PreparationDemand {
    fn occupancy(
        &self,
        generation: &Arc<DeploymentGeneration>,
        identity: &GeneratedPoolIdentity,
    ) -> PopulationOccupancy {
        // A retained previous selection can still own calls during rollback
        // preparation. Exact incarnation, rather than current selection, decides
        // whether those Stores consume this population's target.
        self.pools
            .get(identity)
            .filter(|registered| {
                std::ptr::eq(registered.generation.as_ptr(), Arc::as_ptr(generation))
            })
            .map(|registered| PopulationOccupancy {
                active: registered.checked_out.load(Ordering::Acquire),
                reserved: registered.reserved.load(Ordering::Acquire),
            })
            .unwrap_or_default()
    }

    fn idle_retention_targets(
        &self,
        preparation_running: bool,
        activation_targets: &BTreeMap<usize, BTreeMap<GeneratedPoolIdentity, usize>>,
        policy: RuntimePolicy,
    ) -> BTreeMap<GeneratedPoolIdentity, usize> {
        let selected_generation = self.selected_generation.upgrade();
        self.pools
            .iter()
            .filter_map(|(identity, registered)| {
                let generation = registered.generation.upgrade()?;
                if generation.is_retired() {
                    return None;
                }
                let selected = (preparation_running || !activation_targets.is_empty())
                    && selected_generation
                        .as_ref()
                        .is_some_and(|selected| Arc::ptr_eq(selected, &generation));
                let activating = activation_targets.get(&(Arc::as_ptr(&generation) as usize));
                if !selected && activating.is_none() {
                    return None;
                }
                // Candidate growth must not consume the serving population that
                // the same recent demand still needs, including suspended calls.
                let serving_target = if selected {
                    registered
                        .serving_scope(policy)
                        .map_or(1, |scope| registered.target(scope))
                        .saturating_sub(registered.checked_out.load(Ordering::Acquire))
                        .saturating_sub(registered.reserved.load(Ordering::Acquire))
                } else {
                    0
                };
                let activation_target = activating
                    .map(|targets| {
                        // Discovery has not captured a plan yet. Once it has,
                        // omitted groups are outside this activation's scope.
                        targets
                            .get(identity)
                            .copied()
                            .unwrap_or(usize::from(targets.is_empty()))
                    })
                    .unwrap_or(0);
                let activation_target = activation_target
                    .saturating_sub(registered.checked_out.load(Ordering::Acquire))
                    .saturating_sub(registered.reserved.load(Ordering::Acquire))
                    .max(activation_target.min(1));
                Some((identity.clone(), serving_target.max(activation_target)))
            })
            .collect()
    }
}

impl RegisteredDemand {
    fn serving_scope(&self, policy: RuntimePolicy) -> Option<DemandScope> {
        let queries = policy.query.primary == PrimaryRuntime::Wasm
            && self
                .members
                .iter()
                .any(|entry| entry.kind == DemandScope::Query);
        let mutations = policy.mutation.primary == PrimaryRuntime::Wasm
            && self
                .members
                .iter()
                .any(|entry| entry.kind == DemandScope::Mutation);
        match (queries, mutations) {
            (true, true) => Some(DemandScope::Both),
            (true, false) => Some(DemandScope::Query),
            (false, true) => Some(DemandScope::Mutation),
            (false, false) => None,
        }
    }

    fn observe_saturation(&mut self) {
        self.saturation = Some(SaturationDemand {
            capacity: self.checked_out.load(Ordering::Acquire).saturating_add(1),
            expires: Instant::now() + DEMAND_WINDOW,
        });
    }

    fn population_target(&self, scope: DemandScope) -> usize {
        let observed = |demand: &Arc<parking_lot::Mutex<GroupDemand>>| {
            let mut demand = demand.lock();
            demand.advance(Instant::now());
            demand.observed_peak() > 0
                || demand.inherited.is_some_and(|(_, at)| at > Instant::now())
        };
        // Both engines contribute to bootstrap concurrency. Actual Wasm history
        // wins only for the requested kinds: query-only history cannot predict a
        // newly promoted mutation sharing the same physical pool.
        let relevant = observed(&self.demand[scope as usize])
            && (scope != DemandScope::Both
                || [DemandScope::Query, DemandScope::Mutation]
                    .iter()
                    .all(|kind| {
                        observed(&self.demand[*kind as usize])
                            || !observed(&self.primary_demand[*kind as usize])
                    }));
        if relevant {
            self.demand[scope as usize].lock().target()
        } else {
            self.primary_demand[scope as usize].lock().target()
        }
    }

    fn target(&self, scope: DemandScope) -> usize {
        let observed = self.population_target(scope);
        if let Some(saturation) = &self.saturation
            && saturation.expires > Instant::now()
        {
            // Keep occupancy at the miss: short bursts can finish before the
            // maintenance tick. Repeated misses do not increment this target,
            // and it never becomes a readiness requirement or history.
            observed.max(saturation.capacity)
        } else {
            observed
        }
    }
}

struct GroupDemand {
    active: usize,
    peak: usize,
    previous_peaks: [usize; DEMAND_BUCKETS - 1],
    window_started: Instant,
    // Estimates retain their source observations' expiry and never become
    // observations themselves when another partition inherits demand.
    inherited: Option<(usize, Instant)>,
}

impl GroupDemand {
    fn advance(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.window_started);
        let steps = elapsed.as_secs() / DEMAND_WINDOW.as_secs();
        if steps == 0 {
            return;
        }
        for _ in 0..steps.min(DEMAND_BUCKETS as u64) {
            self.previous_peaks.rotate_right(1);
            self.previous_peaks[0] = self.peak;
            self.peak = self.active;
        }
        self.window_started =
            now - Duration::from_secs(elapsed.as_secs() % DEMAND_WINDOW.as_secs());
    }

    fn observed_peak(&self) -> usize {
        self.previous_peaks
            .iter()
            .copied()
            .fold(self.peak, usize::max)
    }

    fn observation(&self) -> Option<(usize, Instant)> {
        let peak = self.observed_peak();
        if peak == 0 {
            return None;
        }
        let age = if self.peak == peak {
            0
        } else {
            1 + self
                .previous_peaks
                .iter()
                .position(|value| *value == peak)
                .unwrap()
        };
        Some((
            peak,
            self.window_started + DEMAND_WINDOW * (DEMAND_BUCKETS - age) as u32,
        ))
    }

    fn target(&mut self) -> usize {
        self.advance(Instant::now());
        let inherited = self
            .inherited
            .filter(|(_, expires)| *expires > Instant::now())
            .map_or(0, |(peak, _)| peak);
        self.observed_peak().max(inherited).max(1)
    }
}

pub(crate) struct DemandObservation {
    checked_out: Arc<AtomicUsize>,
    _observations: Vec<PrimaryDemandObservation>,
}

impl Drop for DemandObservation {
    fn drop(&mut self) {
        assert!(
            self.checked_out.fetch_sub(1, Ordering::AcqRel) > 0,
            "Wasm checked-out accounting underflow"
        );
    }
}

pub(super) fn observe_demand(
    routed: &GeneratedRoutedModule,
    primary_kind: Option<ManifestUdfKind>,
) -> Option<DemandObservation> {
    let (observations, checked_out) = {
        let demands = DEMAND.lock();
        let registered = demands
            .pools
            .get(&routed.pool_identity)
            .filter(|registered| {
                routed.generation.as_ref().is_some_and(|generation| {
                    std::ptr::eq(registered.generation.as_ptr(), Arc::as_ptr(generation))
                })
            })?;
        let observations = if let Some(kind) = primary_kind {
            [DemandScope::from(kind), DemandScope::Both]
                .into_iter()
                .flat_map(|scope| {
                    [
                        Arc::clone(&registered.demand[scope as usize]),
                        Arc::clone(&registered.primary_demand[scope as usize]),
                    ]
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        (observations, Arc::clone(&registered.checked_out))
    };
    checked_out.fetch_add(1, Ordering::AcqRel);
    Some(DemandObservation {
        checked_out,
        _observations: observations
            .into_iter()
            .map(PrimaryDemandObservation::new)
            .collect(),
    })
}

// One owner per actual execution, including awaits; it is never created for
// cache hits, followers, verifier copies or nested recursion.
pub struct PrimaryDemandObservation(Arc<parking_lot::Mutex<GroupDemand>>);

impl PrimaryDemandObservation {
    fn new(demand: Arc<parking_lot::Mutex<GroupDemand>>) -> Self {
        {
            let mut state = demand.lock();
            state.advance(Instant::now());
            state.active = state
                .active
                .checked_add(1)
                .expect("primary demand accounting overflow");
            state.peak = state.peak.max(state.active);
        }
        Self(demand)
    }
}

impl Drop for PrimaryDemandObservation {
    fn drop(&mut self) {
        let mut demand = self.0.lock();
        // A still-held resource contributes before a quiet window advances.
        demand.advance(Instant::now());
        demand.active = demand
            .active
            .checked_sub(1)
            .expect("primary demand accounting underflow");
    }
}

pub(super) fn observe_primary_demand(
    generation: &DeploymentGeneration,
    runtime_module_path: &str,
    export_name: &str,
    kind: ManifestUdfKind,
) -> Option<[PrimaryDemandObservation; 2]> {
    let demands = {
        let demands = DEMAND.lock();
        let registered = demands.pools.values().find(|registered| {
            std::ptr::eq(registered.generation.as_ptr(), generation)
                && registered
                    .exports
                    .get(runtime_module_path)
                    .and_then(|exports| exports.get(export_name))
                    == Some(&kind)
        })?;
        [DemandScope::from(kind), DemandScope::Both]
            .map(|scope| Arc::clone(&registered.primary_demand[scope as usize]))
    };
    Some(demands.map(PrimaryDemandObservation::new))
}

pub(super) fn observe_saturation(routed: &GeneratedRoutedModule) {
    if let Some(registered) = DEMAND.lock().pools.get_mut(&routed.pool_identity)
        && routed.generation.as_ref().is_some_and(|generation| {
            std::ptr::eq(registered.generation.as_ptr(), Arc::as_ptr(generation))
        })
    {
        registered.observe_saturation();
    }
}

pub(super) fn population_occupancy(
    generation: &Arc<DeploymentGeneration>,
    identity: &GeneratedPoolIdentity,
) -> PopulationOccupancy {
    DEMAND.lock().occupancy(generation, identity)
}

pub(super) fn reserve_population(
    generation: &Arc<DeploymentGeneration>,
    identity: &GeneratedPoolIdentity,
    count: usize,
) -> anyhow::Result<ReservedPopulation> {
    let demands = DEMAND.lock();
    let registered = demands
        .pools
        .get(identity)
        .filter(|registered| {
            std::ptr::eq(registered.generation.as_ptr(), Arc::as_ptr(generation))
        })
        .context("reserved Wasm population has no matching generation registration")?;
    registered.reserved.fetch_add(count, Ordering::AcqRel);
    Ok(ReservedPopulation {
        reserved: Arc::clone(&registered.reserved),
        count,
    })
}

pub(super) fn idle_retention_targets() -> BTreeMap<GeneratedPoolIdentity, usize> {
    // Keep the flight map locked through lookup: its owner retains the exact
    // incarnation until removal, so a pointer key cannot be reused meanwhile.
    let flights = PREPARATION_FLIGHTS.lock();
    let activation_generations = flights
        .iter()
        .filter(|(_, flight)| flight.mode.is_activation())
        .map(|(key, flight)| (key.generation, flight.targets.lock().clone()))
        .collect();
    DEMAND.lock().idle_retention_targets(
        PREPARATION_STARTED.load(Ordering::Acquire),
        &activation_generations,
        runtime_policy_store().snapshot().policy,
    )
}

pub(super) fn set_selected_generation(generation: &Arc<DeploymentGeneration>) {
    DEMAND.lock().selected_generation = Arc::downgrade(generation);
}

pub(super) fn clear_selected_generation() {
    DEMAND.lock().selected_generation = Weak::new();
}

fn preparation_is_current(generation: &DeploymentGeneration, mode: PreparationMode) -> bool {
    !generation.is_retired()
        && (mode.is_activation()
            || std::ptr::eq(DEMAND.lock().selected_generation.as_ptr(), generation))
}

fn other_generation_is_activating(generation: &Arc<DeploymentGeneration>) -> bool {
    PREPARATION_FLIGHTS.lock().iter().any(|(key, flight)| {
        key.generation != Arc::as_ptr(generation) as usize && flight.mode.is_activation()
    })
}

fn register_demand(
    demands: &mut PreparationDemand,
    generation: &Arc<DeploymentGeneration>,
    pool_identity: &GeneratedPoolIdentity,
    members: BTreeSet<DemandEntry>,
    exports: BTreeMap<String, BTreeMap<String, ManifestUdfKind>>,
) {
    let now = Instant::now();
    demands.pools.retain(|_, registered| {
        registered
            .generation
            .upgrade()
            .is_some_and(|generation| !generation.is_retired())
    });
    let live_members = demands
        .pools
        .values()
        .filter_map(|registered| {
            registered.generation.upgrade().map(|generation| {
                (
                    generation.generation_sha256.clone(),
                    registered.members.clone(),
                )
            })
        })
        .collect::<BTreeSet<_>>();
    let members = members.into_iter().collect::<Vec<_>>();
    let history_key = (generation.generation_sha256.clone(), members.clone());
    let demand = [DemandScope::Query, DemandScope::Mutation, DemandScope::Both].map(|scope| {
        register_history(
            &mut demands.history[scope as usize],
            &live_members,
            &history_key,
            scope,
            now,
        )
    });
    let primary_demand = [DemandScope::Query, DemandScope::Mutation, DemandScope::Both].map(|scope| {
        register_history(
            &mut demands.primary_history[scope as usize],
            &live_members,
            &history_key,
            scope,
            now,
        )
    });
    if let Some(registered) = demands.pools.get(pool_identity)
        && registered
            .generation
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, generation))
    {
        assert!(registered
            .demand
            .iter()
            .zip(&demand)
            .all(|(left, right)| Arc::ptr_eq(left, right)));
        return;
    }
    demands.pools.insert(
        pool_identity.clone(),
        RegisteredDemand {
            generation: Arc::downgrade(generation),
            members,
            exports,
            demand,
            primary_demand,
            checked_out: Arc::new(AtomicUsize::new(0)),
            reserved: Arc::new(AtomicUsize::new(0)),
            saturation: None,
        },
    );
}

fn register_history(
    history: &mut BTreeMap<(String, Vec<DemandEntry>), Arc<parking_lot::Mutex<GroupDemand>>>,
    live_members: &BTreeSet<(String, Vec<DemandEntry>)>,
    history_key: &(String, Vec<DemandEntry>),
    scope: DemandScope,
    now: Instant,
) -> Arc<parking_lot::Mutex<GroupDemand>> {
    history.retain(|members, demand| {
        let mut demand = demand.lock();
        demand.advance(now);
        live_members.contains(members)
            || demand.active > 0
            || demand.observed_peak() > 0
            || demand.inherited.is_some_and(|(_, expires)| expires > now)
    });
    let mut by_generation = BTreeMap::<String, (usize, Instant)>::new();
    for ((observed_generation, previous_members), demand) in history.iter() {
        // Sharing a mutation cannot transfer an unrelated query peak (or the
        // reverse) into a newly partitioned group with different selected kinds.
        if !previous_members.iter().any(|member| {
            (scope == DemandScope::Both || member.kind == scope)
                && history_key.1.binary_search(member).is_ok()
        }) {
            continue;
        }
        let mut demand = demand.lock();
        demand.advance(now);
        let Some((peak, expires)) = demand.observation() else {
            continue;
        };
        let estimate = by_generation
            .entry(observed_generation.clone())
            .or_insert((0, expires));
        estimate.0 = estimate.0.saturating_add(peak);
        estimate.1 = estimate.1.min(expires);
    }
    // Only original observations contribute; inherited estimates cannot multiply
    // when groups split and merge over several generations.
    let inherited = by_generation.into_values().max_by_key(|(peak, _)| *peak);
    let demand = Arc::clone(history.entry(history_key.clone()).or_insert_with(|| {
        Arc::new(parking_lot::Mutex::new(GroupDemand {
            active: 0,
            peak: 0,
            previous_peaks: [0; DEMAND_BUCKETS - 1],
            window_started: now,
            inherited,
        }))
    }));
    demand.lock().inherited = inherited;
    demand
}

pub fn initialize_runtime_preparation<RT: Runtime>(
    rt: RT,
    database: Database<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
) -> anyhow::Result<()> {
    let configuration = route_configuration()?;
    let Some(registry) = configuration
        .deployment_registry
        .as_ref()
        .filter(|registry| registry.uses_source_keyed_selection())
        .cloned()
    else {
        return Ok(());
    };
    anyhow::ensure!(
        configuration.reuse_instances,
        "source-keyed preparation requires instance reuse"
    );
    if PREPARATION_STARTED.swap(true, Ordering::AcqRel) {
        return Ok(());
    }
    struct PreparationLease;
    impl Drop for PreparationLease {
        fn drop(&mut self) {
            PREPARATION_STARTED.store(false, Ordering::Release);
            GENERATED_GENERATION_RETIREMENT_NOTIFY.notify_waiters();
        }
    }
    let lease = PreparationLease;
    let worker_rt = rt.clone();
    rt.spawn_background("generated_wasm_preparation", async move {
        let _lease = lease;
        let mut retries = Vec::<(Weak<DeploymentGeneration>, Instant)>::new();
        loop {
            let deadline =
                tokio::time::Instant::now() + configuration.generated_memory_admission_wait;
            let work = async {
                // Release cached route owners before admitting another graph.
                // Otherwise retired preparation state can keep its compiled
                // bytes charged and prevent loading the replacement at all.
                remove_prepared_generations(|prepared| prepared.generation.is_retired());
                // After a backend restart, populate the selected generation
                // without making the first user request own initialization.
                let mut tx = database.begin_system().await?;
                let selector = SourcePackageModel::new(&mut tx, TableNamespace::Global)
                    .get_latest_record()
                    .await?
                    .and_then(|source| {
                        match (&source.runtime_content_sha256, &source.runtime_generation) {
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
                        }
                    });
                let residency = registry
                    .source_keyed_residency
                    .as_ref()
                    .context("source-keyed preparation lost its residency manager")?;
                if !residency.observe_selected(selector.clone(), *tx.begin_timestamp())? {
                    return Ok(());
                }
                drop(tx);
                let selected_generation = if let Some(selector) = selector {
                    let selected = registry
                        .ensure_source_keyed_generation(
                            &selector.source_package_runtime_content_sha256,
                            &SourceKeyedRuntimeGenerationIdentity {
                                deployment_sha256: selector.deployment_sha256,
                                generation_manifest_sha256: selector.generation_manifest_sha256,
                                generation_sha256: selector.generation_sha256,
                            },
                            None,
                            deadline,
                        )
                        .await?;
                    let selected = selected.context("selected Wasm generation is unavailable")?;
                    if !residency.owns_replenishment(&selected) {
                        return Ok(());
                    }
                    Some(selected)
                } else {
                    None
                };

                retries.retain(|(generation, retry_at)| {
                    *retry_at > Instant::now()
                        && generation
                            .upgrade()
                            .is_some_and(|generation| !generation.is_retired())
                });
                // An unpaired source owns no background preparation. Explicit
                // readiness remains responsible for uncommitted candidates.
                if let Some(generation) = selected_generation {
                    if generation.is_retired()
                        || retries
                            .iter()
                            .any(|(failed, _)| failed.ptr_eq(&Arc::downgrade(&generation)))
                    {
                        return Ok(());
                    }
                    match prepare_generation(
                        worker_rt.clone(),
                        database.clone(),
                        default_system_env_vars.clone(),
                        Arc::clone(&generation),
                        PreparationMode::Maintenance,
                        deadline,
                    )
                    .await
                    {
                        Ok(GenerationPreparation::Prepared(_)) => {},
                        // Replenishment is optional while memory is occupied.
                        // It neither evicts serving generations nor retries in
                        // a tight loop; readiness callers still fail explicitly.
                        Ok(GenerationPreparation::CapacityUnavailable { .. }) => {},
                        Err(_) => {
                            // A staged initializer failure must not suspend
                            // replenishment of the still-serving generation.
                            log_generated_failure(GeneratedFailureLog::RuntimePreparation);
                            retries.push((
                                Arc::downgrade(&generation),
                                Instant::now() + GENERATED_FAILURE_LOG_INTERVAL,
                            ));
                        },
                    }
                }
                Ok::<_, anyhow::Error>(())
            };
            let result = match tokio::time::timeout_at(deadline, work).await {
                Ok(result) => result,
                // Capacity expiry is retryable; it is not an initializer failure.
                Err(_) => Ok(()),
            };
            let delay = match result {
                Ok(()) => GENERATED_POOL_MAINTENANCE_INTERVAL,
                Err(error)
                    if error
                        .downcast_ref::<SourceKeyedRuntimeCapacityUnavailable>()
                        .is_some() =>
                {
                    GENERATED_POOL_MAINTENANCE_INTERVAL
                },
                Err(_) => {
                    // A failed initializer is never admitted. Background work
                    // has no request to return an error to; surface the failure
                    // and limit retries while source/configuration is unchanged.
                    log_generated_failure(GeneratedFailureLog::RuntimePreparation);
                    GENERATED_FAILURE_LOG_INTERVAL
                },
            };
            worker_rt.wait(delay).await;
        }
    });
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum PreparationAdmission {
    Prepared,
    CapacityUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum PreparationMode {
    Activation,
    PrimaryActivation { queries: bool, mutations: bool },
    Maintenance,
}

impl PreparationMode {
    pub(super) fn for_policy(policy: RuntimePolicy, generation: &DeploymentGeneration) -> Self {
        let selected = generation.registry.selected_wasm_exports();
        let queries = policy.query.primary == PrimaryRuntime::Wasm
            && selected
                .iter()
                .any(|entry| entry.udf_kind == ManifestUdfKind::Query);
        let mutations = policy.mutation.primary == PrimaryRuntime::Wasm
            && selected
                .iter()
                .any(|entry| entry.udf_kind == ManifestUdfKind::Mutation);
        if queries || mutations {
            Self::PrimaryActivation { queries, mutations }
        } else {
            Self::Activation
        }
    }

    fn is_activation(self) -> bool {
        match self {
            Self::Activation | Self::PrimaryActivation { .. } => true,
            Self::Maintenance => false,
        }
    }

    fn population_scope(self, group: &PreparationGroup) -> Option<DemandScope> {
        let (queries, mutations) = match self {
            Self::Activation => (false, false),
            Self::PrimaryActivation { queries, mutations } => (queries, mutations),
            Self::Maintenance => {
                let policy = common::wasm_runtime_policy::runtime_policy_store()
                    .snapshot()
                    .policy;
                (
                    policy.query.primary == PrimaryRuntime::Wasm,
                    policy.mutation.primary == PrimaryRuntime::Wasm,
                )
            },
        };
        match (queries && group.queries, mutations && group.mutations) {
            (true, true) => Some(DemandScope::Both),
            (true, false) => Some(DemandScope::Query),
            (false, true) => Some(DemandScope::Mutation),
            (false, false) => None,
        }
    }
}

pub(super) fn release_generation(generation: &Arc<DeploymentGeneration>) {
    remove_prepared_generations(|prepared| Arc::ptr_eq(&prepared.generation, generation));
}

fn remove_prepared_generations(mut remove: impl FnMut(&PreparedGeneration) -> bool) {
    let removed = PREPARATION_LOCK
        .lock()
        .extract_if(.., |prepared| remove(prepared))
        .collect::<Vec<_>>();
    // A final group owner can unmap its image and release controller charges.
    // Keep that work outside the preparation ownership-table lock as well.
    drop(removed);
}

#[derive(Clone)]
pub(super) enum GenerationPreparation {
    Prepared(Arc<PreparedInventory>),
    CapacityUnavailable {
        reason: &'static str,
        population: Vec<SourceKeyedRuntimeGroupPopulation>,
    },
}

impl GenerationPreparation {
    fn unavailable<RT: Runtime>(
        generation: &Arc<DeploymentGeneration>,
        plan: &parking_lot::Mutex<BTreeMap<GeneratedPoolIdentity, usize>>,
        reason: &'static str,
    ) -> Self {
        // Pool admission reads retention targets under the pool lock. Never
        // hold the plan lock while taking an inventory snapshot in reverse.
        let targets = plan.lock().clone();
        Self::CapacityUnavailable {
            reason,
            population: capture_inventory::<RT>(generation, &targets, Some(reason)).population(),
        }
    }
}

pub(super) struct PreparedInventory {
    pub(super) generation: Weak<DeploymentGeneration>,
    pub(super) groups: Vec<PreparedGroupInventory>,
}

pub(super) struct PreparedGroupInventory {
    pub(super) pool_identity: GeneratedPoolIdentity,
    pub(super) instances: Vec<(GeneratedSlotId, u64)>,
    pub(super) target: usize,
    pub(super) active: usize,
    pub(super) in_progress: usize,
    pub(super) shortfall: Option<&'static str>,
}

pub(super) async fn prepare_generation<RT: Runtime>(
    rt: RT,
    database: Database<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    generation: Arc<DeploymentGeneration>,
    mode: PreparationMode,
    batch_deadline: tokio::time::Instant,
) -> anyhow::Result<GenerationPreparation> {
    let key = PreparationFlightKey {
        generation: Arc::as_ptr(&generation) as usize,
    };
    loop {
        if !preparation_is_current(&generation, mode)
            || tokio::time::Instant::now() >= batch_deadline
        {
            return Ok(GenerationPreparation::CapacityUnavailable {
                reason: "preparing",
                population: Vec::new(),
            });
        }
        let flight = {
            let mut flights = PREPARATION_FLIGHTS.lock();
            if let Some(flight) = flights.get(&key) {
                Arc::clone(flight)
            } else {
                let (task_rt, task_database, task_env, task_generation) = (
                    rt.clone(),
                    database.clone(),
                    default_system_env_vars.clone(),
                    Arc::clone(&generation),
                );
                let progress = Arc::new(parking_lot::Mutex::new("preparing"));
                let task_progress = Arc::clone(&progress);
                let targets = Arc::new(parking_lot::Mutex::new(BTreeMap::new()));
                let task_targets = Arc::clone(&targets);
                let baseline_ready = Arc::new(AtomicBool::new(false));
                let task_baseline_ready = Arc::clone(&baseline_ready);
                let task = tokio::spawn(async move {
                    let owner = PreparationFlightOwner(task_generation);
                    let result = prepare_generation_inner(
                        task_rt,
                        task_database,
                        task_env,
                        Arc::clone(&owner.0),
                        mode,
                        batch_deadline,
                        &task_progress,
                        &task_targets,
                        &task_baseline_ready,
                    )
                    .await;
                    if mode.is_activation() {
                        // A batch may leave a valid partial baseline for its
                        // next bounded request. Start its ordinary idle grace
                        // when this owner finishes, not when the first group
                        // was built; otherwise age eviction in the retry gap
                        // can undo every batch's progress.
                        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
                        for instance in pool.iter_mut().filter_map(|value| {
                            value.downcast_mut::<GeneratedReusableInstance<RT>>()
                        }) {
                            if instance.all_entries_prepared
                                && instance
                                    .routed
                                    .generation
                                    .as_ref()
                                    .is_some_and(|generation| Arc::ptr_eq(generation, &owner.0))
                            {
                                instance.idle_since = Instant::now();
                            }
                        }
                    }
                    result.map_err(Arc::new)
                });
                let result = async move {
                    match task.await {
                        Ok(result) => result,
                        Err(error) => Err(Arc::new(anyhow::anyhow!(
                            "Wasm generation preparation task failed: {error}"
                        ))),
                    }
                }
                .boxed()
                .shared();
                let flight = Arc::new(PreparationFlight {
                    mode,
                    progress,
                    targets,
                    baseline_ready,
                    result,
                });
                flights.insert(key, Arc::clone(&flight));
                flight
            }
        };
        let result = tokio::time::timeout_at(batch_deadline, flight.result.clone()).await;
        let Ok(result) = result else {
            // Optional work cannot turn complete baseline coverage into a
            // failed readiness response. The flight still owns any guest work
            // and teardown; the receipt includes only completed idle Stores.
            if mode.is_activation()
                && mode == flight.mode
                && flight.baseline_ready.load(Ordering::Acquire)
            {
                return prepared_inventory::<RT>(
                    &generation,
                    mode,
                    &flight.targets,
                    Some("deadline"),
                );
            }
            // The detached producer retains a running initializer and teardown.
            // Keep its discoverable flight until that owner actually finishes.
            return Ok(GenerationPreparation::unavailable::<RT>(
                &generation,
                &flight.targets,
                *flight.progress.lock(),
            ));
        };
        {
            let mut flights = PREPARATION_FLIGHTS.lock();
            if flights
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &flight))
            {
                flights.remove(&key);
            }
        }
        // The two purposes share creation ownership. Activation needs its own
        // idle assessment after maintenance. Conversely, one failed activation
        // group must not turn a joining maintenance caller into a generation-wide
        // backoff: its own pass skips that group's recorded failure.
        if (mode.is_activation() && mode != flight.mode)
            || (mode == PreparationMode::Maintenance
                && flight.mode.is_activation()
                && result.is_err())
        {
            continue;
        }
        return result.map_err(|error| anyhow::anyhow!("{error:#}"));
    }
}

async fn prepare_generation_inner<RT: Runtime>(
    rt: RT,
    database: Database<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    generation: Arc<DeploymentGeneration>,
    mode: PreparationMode,
    batch_deadline: tokio::time::Instant,
    progress: &parking_lot::Mutex<&'static str>,
    plan: &parking_lot::Mutex<BTreeMap<GeneratedPoolIdentity, usize>>,
    baseline_ready: &AtomicBool,
) -> anyhow::Result<GenerationPreparation> {
    let baseline_deadline = match mode {
        PreparationMode::PrimaryActivation { .. } => batch_deadline - EXTRA_POPULATION_WINDOW,
        PreparationMode::Activation | PreparationMode::Maintenance => batch_deadline,
    };
    if !preparation_is_current(&generation, mode) {
        return Ok(GenerationPreparation::unavailable::<RT>(
            &generation,
            plan,
            *progress.lock(),
        ));
    }
    remove_prepared_generations(|prepared| prepared.generation.is_retired());
    let cached_groups = {
        let prepared_generations = PREPARATION_LOCK.lock();
        prepared_generations
            .iter()
            .find(|prepared| Arc::ptr_eq(&prepared.generation, &generation))
            .map(|prepared| prepared.groups.clone())
    };
    let groups = match cached_groups {
        Some(groups) => groups,
        None => {
            let load_generation = Arc::clone(&generation);
            let loading = tokio_spawn_blocking("generated_wasm_preparation_routes", move || {
                if !preparation_is_current(&load_generation, mode)
                    || tokio::time::Instant::now() >= baseline_deadline
                {
                    return Ok(None);
                }
                let mut groups = BTreeMap::new();
                for route in selected_generation_routes(&load_generation)? {
                    if !preparation_is_current(&load_generation, mode)
                        || tokio::time::Instant::now() >= baseline_deadline
                    {
                        return Ok(None);
                    }
                    let routed = generated_deployment_routed_module(
                        &route,
                        StaticHermesWasmtimePreparationMode::Primary,
                    )?;
                    let group = groups
                        .entry(routed.pool_identity.clone())
                        .or_insert_with(|| PreparationGroup {
                            routed,
                            members: BTreeSet::new(),
                            exports: BTreeMap::new(),
                            queries: false,
                            mutations: false,
                            memory_cost: Arc::new(parking_lot::Mutex::new(None)),
                            retry_at: Arc::new(parking_lot::Mutex::new(None)),
                            failed: Arc::new(parking_lot::Mutex::new(None)),
                            last_started: Arc::new(parking_lot::Mutex::new(None)),
                            images: Arc::default(),
                            in_progress: Arc::default(),
                        });
                    match route.udf_kind {
                        ManifestUdfKind::Query => group.queries = true,
                        ManifestUdfKind::Mutation => group.mutations = true,
                    }
                    group
                        .exports
                        .entry(route.runtime_module_path.clone())
                        .or_default()
                        .insert(route.export_name.clone(), route.udf_kind);
                    group.members.insert(DemandEntry {
                        module: route.runtime_module_path,
                        export: route.export_name,
                        kind: route.udf_kind.into(),
                    });
                }
                Ok::<_, anyhow::Error>(Some(groups.into_values().collect::<Vec<_>>()))
            });
            // Dropping a blocking JoinHandle does not cancel its work. Keep
            // the shared flight until route discovery exits so timed-out callers
            // cannot queue duplicate discovery jobs for this incarnation.
            // Their own wait remains bounded by prepare_generation's deadline.
            let Some(groups) = loading.await?? else {
                return Ok(GenerationPreparation::unavailable::<RT>(
                    &generation,
                    plan,
                    *progress.lock(),
                ));
            };
            let mut prepared_generations = PREPARATION_LOCK.lock();
            if let Some(prepared) = prepared_generations
                .iter()
                .find(|prepared| Arc::ptr_eq(&prepared.generation, &generation))
            {
                prepared.groups.clone()
            } else {
                if !preparation_is_current(&generation, mode) {
                    drop(prepared_generations);
                    return Ok(GenerationPreparation::unavailable::<RT>(
                        &generation,
                        plan,
                        *progress.lock(),
                    ));
                }
                prepared_generations.push(PreparedGeneration {
                    generation: Arc::clone(&generation),
                    groups: groups.clone(),
                });
                groups
            }
        },
    };
    for group in &groups {
        register_demand(
            &mut DEMAND.lock(),
            &generation,
            &group.routed.pool_identity,
            group.members.clone(),
            group.exports.clone(),
        );
    }
    // Preserve the full registry for V8 observations and future policy changes,
    // but require capacity only for kinds becoming primary in this destination.
    let groups = groups
        .into_iter()
        .filter(|group| {
            !matches!(mode, PreparationMode::PrimaryActivation { .. })
                || mode.population_scope(group).is_some()
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !groups.is_empty(),
        "Wasm preparation has no applicable groups"
    );
    // Snapshot demand once. A deployment must not chase a peak that keeps
    // rising while it prepares; later demand belongs to ordinary maintenance.
    let other_activation = other_generation_is_activating(&generation);
    let desired = {
        let demands = DEMAND.lock();
        groups
            .iter()
            .map(|group| {
                if let Some(scope) = mode.population_scope(group)
                    && !(mode == PreparationMode::Maintenance && other_activation)
                {
                    if mode == PreparationMode::Maintenance {
                        demands.pools[&group.routed.pool_identity].target(scope)
                    } else {
                        demands.pools[&group.routed.pool_identity].population_target(scope)
                    }
                } else {
                    1
                }
            })
            .collect::<Vec<_>>()
    };
    *plan.lock() = groups
        .iter()
        .zip(&desired)
        .map(|(group, target)| (group.routed.pool_identity.clone(), *target))
        .collect();
    for group in &groups {
        if validate_prepared_group::<RT>(&database, &generation, group, mode, baseline_deadline)
            .await?
            == PreparationAdmission::CapacityUnavailable
        {
            return Ok(GenerationPreparation::unavailable::<RT>(
                &generation,
                plan,
                *progress.lock(),
            ));
        }
    }
    let controller = &route_configuration()?.generated_memory_controller;
    let baseline_targets = groups
        .iter()
        .map(|group| (group.routed.pool_identity.clone(), 1))
        .collect::<BTreeMap<_, _>>();
    let (held_capacity, capacity) = {
        let pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        // Checkout, activation reservation and return transfer ownership under
        // this lock. Count held Stores once without making them idle evidence.
        let held_capacity = {
            let demands = DEMAND.lock();
            groups
                .iter()
                .map(|group| {
                    let occupancy = demands.occupancy(&generation, &group.routed.pool_identity);
                    occupancy.active + occupancy.reserved
                })
                .collect::<Vec<_>>()
        };
        let mut available_slots = pool
            .iter()
            .filter_map(|instance| instance.downcast_ref::<GeneratedReusableInstance<RT>>())
            .filter(|instance| {
                instance
                    .routed
                    .generation
                    .as_ref()
                    .is_some_and(|owner| Arc::ptr_eq(owner, &generation))
            })
            .map(|instance| instance.memory_slot_id)
            .collect::<BTreeSet<_>>();
        // A physically full pool can still contain surplus that ordinary
        // preparation may reclaim. Size the attempt with the same protection
        // rules as reclamation; actual construction still needs byte admission.
        available_slots.extend(
            preparation_surplus::<RT>(&pool, &generation, mode, &baseline_targets)
                .into_iter()
                .map(|instance| instance.memory_slot_id),
        );
        let capacity = controller.preparation_instance_capacity(
            available_slots
                .len()
                .saturating_add(held_capacity.iter().sum::<usize>()),
        );
        (held_capacity, capacity)
    };
    if mode.is_activation() && groups.len() > capacity.limit {
        return Ok(GenerationPreparation::unavailable::<RT>(
            &generation,
            plan,
            "instance_limit",
        ));
    }
    // Partial coexistence can leave room for only some baselines. Build those
    // now under ordinary admission; requiring room for every group up front
    // would leave spare slots idle and stop healthy maintenance progress.
    let total_targets = allocate_population(&desired, &held_capacity, capacity.available);
    let population_targets = groups
        .iter()
        .zip(&total_targets)
        .map(|(group, target)| (group.routed.pool_identity.clone(), *target))
        .collect::<BTreeMap<_, _>>();
    *progress.lock() = "preparing";
    let mut unavailable_groups = BTreeSet::new();
    // A failed baseline may be an ordering problem: already-retained state can
    // leave too little transient memory to initialize another group. Retry in
    // measured cost order. A selected maintenance owner may release its own
    // later idle baseline to restore a missing group. Candidate activation
    // preserves serving baselines; reserved Stores are absent from the pool.
    let mut baseline_order = match mode {
        PreparationMode::Activation | PreparationMode::PrimaryActivation { .. } => {
            PreparationOrder::Memory
        },
        PreparationMode::Maintenance => PreparationOrder::LeastRecentlyStarted,
    };
    loop {
        let changed = controller.change_notification();
        tokio::pin!(changed);
        let _ = changed.as_mut().enable();
        let returned = GENERATED_GENERATION_RETIREMENT_NOTIFY.notified();
        tokio::pin!(returned);
        let _ = returned.as_mut().enable();
        unavailable_groups.clear();
        for index in preparation_order(controller, &groups, baseline_order) {
            let group = &groups[index];
            if !preparation_is_current(&generation, mode)
                || tokio::time::Instant::now() >= baseline_deadline
            {
                return Ok(GenerationPreparation::unavailable::<RT>(
                    &generation,
                    plan,
                    *progress.lock(),
                ));
            }
            if mode == PreparationMode::Maintenance && group.retry_at.lock().is_some() {
                continue;
            }
            match prepare_group(
                rt.clone(),
                database.clone(),
                default_system_env_vars.clone(),
                &generation,
                group,
                PreparationTarget::Baseline {
                    population: if mode.is_activation() {
                        total_targets[index]
                    } else {
                        1
                    },
                },
                mode,
                baseline_deadline,
                &baseline_targets,
                progress,
            )
            .await
            {
                Ok(PreparationAdmission::Prepared) => group.record_success(),
                Ok(PreparationAdmission::CapacityUnavailable) => {
                    unavailable_groups.insert(group.routed.pool_identity.clone());
                    // A large missing baseline must not prevent later groups
                    // from retaining progress within this bounded attempt.
                },
                Err(error) => {
                    group.record_failure(&error);
                    if mode.is_activation() {
                        return Err(error);
                    }
                    log_generated_failure(GeneratedFailureLog::RuntimePreparation);
                },
            }
        }
        if unavailable_groups.is_empty() {
            break;
        }
        if !preparation_is_current(&generation, mode)
            || tokio::time::Instant::now() >= baseline_deadline
        {
            return Ok(GenerationPreparation::unavailable::<RT>(
                &generation,
                plan,
                *progress.lock(),
            ));
        }
        let order = preparation_order(controller, &groups, PreparationOrder::Memory);
        let mut reordered_targets = baseline_targets.clone();
        if let Some(missing) = order.iter().position(|index| {
            (mode.is_activation() || groups[*index].retry_at.lock().is_none())
                && current_group_capacity::<RT>(&groups[*index].routed).total == 0
        }) {
            for index in &order[missing + 1..] {
                reordered_targets.insert(groups[*index].routed.pool_identity.clone(), 0);
            }
            if reclaim_preparation_surplus::<RT>(
                controller,
                &generation,
                mode,
                &reordered_targets,
                &groups[order[missing]].routed,
                groups[order[missing]]
                    .images
                    .clone_construction_peak_bytes(),
            )
            .await?
            {
                // Once a baseline has been released for the memory order, finish
                // that order before rebuilding the later groups it displaced. Keep
                // the missing group's turn across a batch deadline as well.
                *groups[order[missing]].last_started.lock() = None;
                baseline_order = PreparationOrder::Memory;
                continue;
            }
        }
        if mode == PreparationMode::Maintenance {
            // A missing large group does not prevent a smaller healthy group
            // from using the remaining capacity for its observed demand.
            break;
        }
        if wait_for_preparation(&generation, mode, baseline_deadline, async {
            tokio::select! { _ = &mut changed => {}, _ = &mut returned => {} }
            Ok(())
        })
        .await?
        .is_none()
        {
            return Ok(GenerationPreparation::unavailable::<RT>(
                &generation,
                plan,
                *progress.lock(),
            ));
        }
    }
    baseline_ready.store(true, Ordering::Release);
    let extra_deadline = (tokio::time::Instant::now() + EXTRA_POPULATION_WINDOW).min(
        batch_deadline
            .checked_sub(Duration::from_millis(250))
            .unwrap_or(batch_deadline),
    );
    let mut shortfall = None;
    let mut pending = FuturesUnordered::new();
    let mut first_error = None;
    let mut in_progress = BTreeSet::new();
    loop {
        // Read each group's idle, active and reserved ownership under one pool lock.
        // Separate snapshots can count one Store twice after a checkout.
        let mut counts = groups
            .iter()
            .map(|group| current_group_capacity::<RT>(&group.routed).total)
            .collect::<Vec<_>>();
        while pending.len() < PREPARATION_CONCURRENCY
            && first_error.is_none()
            && preparation_is_current(&generation, mode)
            && tokio::time::Instant::now() < extra_deadline
            && !(mode == PreparationMode::Maintenance
                && other_generation_is_activating(&generation))
        {
            let next = least_fulfilled_group(&counts, &total_targets, |index| {
                !in_progress.contains(&index)
                    && !unavailable_groups.contains(&groups[index].routed.pool_identity)
                    && !(mode == PreparationMode::Maintenance
                        && groups[index].retry_at.lock().is_some())
            });
            let Some(index) = next else {
                break;
            };
            in_progress.insert(index);
            counts[index] += 1;
            let group = &groups[index];
            let rt = rt.clone();
            let database = database.clone();
            let env = default_system_env_vars.clone();
            let generation = &generation;
            // Keep the whole achieved allocation during optional growth. Memory
            // pressure may leave a shortfall; it must not cause destroy/refill
            // cycles among useful candidate or serving copies.
            let retention = &population_targets;
            let target = counts[index];
            pending.push(async move {
                let result = prepare_group(
                    rt,
                    database,
                    env,
                    generation,
                    group,
                    PreparationTarget::Population(target),
                    mode,
                    extra_deadline,
                    retention,
                    progress,
                )
                .await;
                (index, result)
            });
        }
        let Some((index, result)) = pending.next().await else {
            break;
        };
        in_progress.remove(&index);
        let group = &groups[index];
        match result {
            Ok(PreparationAdmission::Prepared) => group.record_success(),
            Ok(PreparationAdmission::CapacityUnavailable) => {
                unavailable_groups.insert(group.routed.pool_identity.clone());
                shortfall = Some(*progress.lock());
            },
            Err(error) => {
                group.record_failure(&error);
                if mode.is_activation() {
                    first_error.get_or_insert(error);
                } else {
                    log_generated_failure(GeneratedFailureLog::RuntimePreparation);
                }
                // Drain the other owned Store before returning an error.
                continue;
            },
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    if tokio::time::Instant::now() >= extra_deadline {
        shortfall = Some("deadline");
    }
    if mode == PreparationMode::Maintenance {
        // Retry failed initializers after healthy baselines and replicas. Keeping
        // expired failures in this separate pass prevents a slow failing prefix
        // from consuming every batch deadline before healthy groups can grow.
        let now = Instant::now();
        let mut retries = groups
            .iter()
            .filter_map(|group| {
                group
                    .retry_at
                    .lock()
                    .filter(|retry_at| *retry_at <= now)
                    .map(|retry_at| (retry_at, group))
            })
            .collect::<Vec<_>>();
        retries.sort_by_key(|(retry_at, _)| *retry_at);
        for (_, group) in retries {
            if !preparation_is_current(&generation, mode)
                || tokio::time::Instant::now() >= batch_deadline
            {
                return Ok(GenerationPreparation::unavailable::<RT>(
                    &generation,
                    plan,
                    *progress.lock(),
                ));
            }
            match prepare_group(
                rt.clone(),
                database.clone(),
                default_system_env_vars.clone(),
                &generation,
                group,
                PreparationTarget::Baseline { population: 1 },
                mode,
                batch_deadline,
                &baseline_targets,
                progress,
            )
            .await
            {
                Ok(PreparationAdmission::Prepared) => group.record_success(),
                Ok(PreparationAdmission::CapacityUnavailable) => {
                    // Capacity recovery may need the ordinary baseline reorder
                    // pass; keeping this group in the initializer-only tail
                    // would protect the very idle Stores it needs to replace.
                    *group.retry_at.lock() = None;
                },
                Err(error) => {
                    log_generated_failure(GeneratedFailureLog::RuntimePreparation);
                    group.record_failure(&error);
                },
            }
        }
    }
    prepared_inventory::<RT>(&generation, mode, plan, shortfall)
}

fn least_fulfilled_group(
    counts: &[usize],
    targets: &[usize],
    eligible: impl Fn(usize) -> bool,
) -> Option<usize> {
    (0..targets.len())
        .filter(|index| counts[*index] < targets[*index] && eligible(*index))
        .min_by(|left, right| {
            // Cross multiplication compares coverage without float rounding or
            // a count-level bias toward small groups.
            ((counts[*left] as u128) * (targets[*right] as u128))
                .cmp(&((counts[*right] as u128) * (targets[*left] as u128)))
                .then_with(|| left.cmp(right))
        })
}

fn allocate_population(desired: &[usize], held: &[usize], capacity: usize) -> Vec<usize> {
    // Other kinds, verifier Stores and publication reservations consume slots
    // without raising this attempt's primary target. Do not refill their
    // occupancy after it returns.
    let occupied = held.iter().map(|held| (*held).max(1)).sum::<usize>();
    let mut targets = held
        .iter()
        .zip(desired)
        .map(|(held, desired)| (*held).max(1).min(*desired))
        .collect::<Vec<_>>();
    let mut remaining = capacity.saturating_sub(occupied);
    while remaining > 0 {
        let Some(index) = least_fulfilled_group(&targets, desired, |_| true) else {
            break;
        };
        targets[index] += 1;
        remaining -= 1;
    }
    targets
}

fn prepared_inventory<RT: Runtime>(
    generation: &Arc<DeploymentGeneration>,
    mode: PreparationMode,
    plan: &parking_lot::Mutex<BTreeMap<GeneratedPoolIdentity, usize>>,
    shortfall: Option<&'static str>,
) -> anyhow::Result<GenerationPreparation> {
    let targets = plan.lock().clone();
    if !preparation_is_current(generation, mode) {
        return Ok(GenerationPreparation::unavailable::<RT>(
            generation,
            plan,
            "preparing",
        ));
    }
    if mode.is_activation() {
        let groups = PREPARATION_LOCK
            .lock()
            .iter()
            .find(|prepared| Arc::ptr_eq(&prepared.generation, generation))
            .map(|prepared| prepared.groups.clone())
            .unwrap_or_default();
        for group in groups {
            if !targets.contains_key(&group.routed.pool_identity) {
                continue;
            }
            if let Some(error) = failed_preparation(&group.routed) {
                return Err(error);
            }
        }
    }
    let inventory = capture_inventory::<RT>(generation, &targets, shortfall);
    if mode.is_activation()
        && inventory
            .groups
            .iter()
            .any(|group| group.instances.is_empty())
    {
        return Ok(GenerationPreparation::CapacityUnavailable {
            reason: shortfall.unwrap_or("preparing"),
            population: inventory.population(),
        });
    }
    Ok(GenerationPreparation::Prepared(inventory))
}

fn capture_inventory<RT: Runtime>(
    generation: &Arc<DeploymentGeneration>,
    targets: &BTreeMap<GeneratedPoolIdentity, usize>,
    shortfall: Option<&'static str>,
) -> Arc<PreparedInventory> {
    let preparing = PREPARATION_LOCK
        .lock()
        .iter()
        .find(|prepared| Arc::ptr_eq(&prepared.generation, generation))
        .map(|prepared| {
            prepared
                .groups
                .iter()
                .map(|group| {
                    (
                        group.routed.pool_identity.clone(),
                        group.in_progress.load(Ordering::Acquire),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
    let demands = DEMAND.lock();
    let mut inventory = Vec::with_capacity(targets.len());
    for (identity, target) in targets {
        let occupancy = demands.occupancy(generation, identity);
        let active = occupancy.active;
        let instances = pool
            .iter()
            .filter_map(|instance| instance.downcast_ref::<GeneratedReusableInstance<RT>>())
            .filter(|instance| {
                instance.all_entries_prepared
                    && instance
                        .routed
                        .generation
                        .as_ref()
                        .is_some_and(|owner| Arc::ptr_eq(owner, generation))
                    && instance.pool_identity.as_ref() == identity
            })
            // Occupancy reduces optional copies, but cannot replace the idle
            // baseline whose initialization read set publication validates.
            .take(
                target
                    .saturating_sub(active + occupancy.reserved)
                    .max((*target).min(1)),
            )
            .map(|instance| (instance.memory_slot_id, instance.id))
            .collect::<Vec<_>>();
        let incomplete = (*target > 0 && instances.is_empty())
            || instances.len().saturating_add(active) < *target;
        inventory.push(PreparedGroupInventory {
            pool_identity: identity.clone(),
            instances,
            target: *target,
            active,
            in_progress: preparing.get(identity).copied().unwrap_or(0),
            shortfall: incomplete.then_some(shortfall.unwrap_or(if occupancy.reserved > 0 {
                "capacity_changed"
            } else {
                "instance_limit"
            })),
        });
    }
    Arc::new(PreparedInventory {
        generation: Arc::downgrade(generation),
        groups: inventory,
    })
}

#[derive(Clone, Copy)]
enum PreparationOrder {
    Memory,
    LeastRecentlyStarted,
}

fn preparation_order(
    controller: &GeneratedMemoryController,
    groups: &[PreparationGroup],
    scheduling: PreparationOrder,
) -> Vec<usize> {
    let mut order = (0..groups.len()).collect::<Vec<_>>();
    order.sort_by_cached_key(|index| {
        let group = &groups[*index];
        let clone_peak_bytes = group.images.clone_construction_peak_bytes();
        let cost = group.memory_cost.lock().and_then(|observed| {
            let routed = &group.routed;
            controller.estimated_preparation_memory_cost(
                &generated_memory_identity_for_package(
                    &routed.manifest,
                    &routed.package_identity,
                    &routed.route_identity,
                ),
                observed,
                clone_peak_bytes,
            )
        });
        // Unknown groups go first when a partial baseline must be rearranged.
        // For measured groups, performing larger transient-minus-retained work
        // first minimizes the serial peak. Refresh reservation estimates from
        // the controller: later handler observations can reverse that order.
        // Admission still checks real accounting; ordering grants no budget.
        (
            match scheduling {
                PreparationOrder::Memory => None,
                PreparationOrder::LeastRecentlyStarted => *group.last_started.lock(),
            },
            cost.is_some(),
            std::cmp::Reverse(cost.map_or(0, |cost| {
                cost.peak_bytes.saturating_sub(cost.retained_bytes)
            })),
        )
    });
    order
}

// Non-guest waits are cancellable. Once guest execution starts, its lifecycle
// retains teardown ownership; retirement only prevents starting the next Store.
async fn wait_for_preparation<T>(
    generation: &DeploymentGeneration,
    mode: PreparationMode,
    deadline: tokio::time::Instant,
    work: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<Option<T>> {
    tokio::pin!(work);
    loop {
        let retirement = GENERATED_GENERATION_RETIREMENT_NOTIFY.notified();
        tokio::pin!(retirement);
        let _ = retirement.as_mut().enable();
        if !preparation_is_current(generation, mode) || tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::select! {
            biased;
            _ = &mut retirement => {},
            _ = tokio::time::sleep_until(deadline) => return Ok(None),
            result = &mut work => return result.map(Some),
        }
    }
}

async fn validate_prepared_group<RT: Runtime>(
    database: &Database<RT>,
    generation: &Arc<DeploymentGeneration>,
    group: &PreparationGroup,
    mode: PreparationMode,
    batch_deadline: tokio::time::Instant,
) -> anyhow::Result<PreparationAdmission> {
    let routed = &group.routed;
    let Some(mut transaction) =
        wait_for_preparation(generation, mode, batch_deadline, database.begin_system()).await?
    else {
        return Ok(PreparationAdmission::CapacityUnavailable);
    };
    let candidates = {
        let pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        pool.iter()
            .filter_map(|instance| instance.downcast_ref::<GeneratedReusableInstance<RT>>())
            .filter(|instance| {
                instance.all_entries_prepared
                    && instance.pool_identity.as_ref() == &routed.pool_identity
                    && instance.routed.has_same_generation_incarnation(routed)
            })
            .map(|instance| {
                Ok((
                    instance.memory_slot_id,
                    instance.id,
                    instance
                        .context_read_set
                        .as_ref()
                        .context("prepared runtime has no initialization read set")?
                        .clone(),
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    };
    let mut invalidated = BTreeSet::new();
    for (slot_id, instance_id, read_set) in candidates {
        if tokio::time::Instant::now() >= batch_deadline {
            return Ok(PreparationAdmission::CapacityUnavailable);
        }
        let Some(valid) = wait_for_preparation(
            generation,
            mode,
            batch_deadline,
            ContextCache::validate_and_apply_context_read_set(&mut transaction, &read_set),
        )
        .await?
        else {
            return Ok(PreparationAdmission::CapacityUnavailable);
        };
        if !valid {
            invalidated.insert((slot_id, instance_id));
        }
    }
    if !invalidated.is_empty() {
        let controller = &route_configuration()?.generated_memory_controller;
        let evicted = {
            let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
            let slots = pool
                .iter()
                .filter_map(|instance| instance.downcast_ref::<GeneratedReusableInstance<RT>>())
                .filter(|instance| invalidated.contains(&(instance.memory_slot_id, instance.id)))
                .map(|instance| (instance.memory_slot_id, instance.idle_since.elapsed()))
                .collect::<Vec<_>>();
            controller
                .idle_eviction_candidates(
                    &BTreeSet::new(),
                    &slots,
                    IdleEvictionTrigger::InitializationInvalidation,
                )
                .into_iter()
                .filter_map(|(slot, reason)| {
                    let id = invalidated
                        .iter()
                        .find(|(candidate_slot, _)| *candidate_slot == slot)
                        .map(|(_, id)| *id)?;
                    Some((
                        pool.take_instance_identity::<RT>(slot, id)
                            .expect("invalidated idle runtime disappeared"),
                        reason,
                    ))
                })
                .collect()
        };
        discard_preparation_instances(
            controller,
            evicted,
            IdleEvictionTrigger::InitializationInvalidation,
        )
        .await?;
    }
    Ok(PreparationAdmission::Prepared)
}

struct GroupCapacity {
    prepared: usize,
    total: usize,
}

#[derive(Clone, Copy)]
enum PreparationTarget {
    Baseline { population: usize },
    Population(usize),
}

impl GroupCapacity {
    fn admission(
        &self,
        target: PreparationTarget,
        mode: PreparationMode,
    ) -> Option<PreparationAdmission> {
        let population = match target {
            PreparationTarget::Baseline { .. } if self.prepared > 0 => {
                return Some(PreparationAdmission::Prepared);
            },
            PreparationTarget::Baseline { population }
            | PreparationTarget::Population(population) => population,
        };
        // A held Store does not establish an idle baseline. Create that baseline
        // if the captured population still has room; only a fully occupied target
        // must wait for a return instead of adding a replica reserve.
        (self.total >= population).then_some(if mode.is_activation() && self.prepared == 0 {
            PreparationAdmission::CapacityUnavailable
        } else {
            PreparationAdmission::Prepared
        })
    }
}

fn current_group_capacity<RT: Runtime>(routed: &GeneratedRoutedModule) -> GroupCapacity {
    let pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
    let prepared = pool
        .iter()
        .filter_map(|instance| instance.downcast_ref::<GeneratedReusableInstance<RT>>())
        .filter(|instance| {
            instance.all_entries_prepared
                && instance.pool_identity.as_ref() == &routed.pool_identity
                && instance.routed.has_same_generation_incarnation(routed)
        })
        .count();
    // Keep the pool locked through held ownership lookup: neither checkout nor
    // reservation return may count a Store twice or leave a population gap.
    let total = prepared
        + routed.generation.as_ref().map_or(0, |generation| {
            let occupancy = population_occupancy(generation, &routed.pool_identity);
            occupancy.active + occupancy.reserved
        });
    GroupCapacity { prepared, total }
}

struct GroupPreparationOwner(Arc<AtomicUsize>);

impl Drop for GroupPreparationOwner {
    fn drop(&mut self) {
        assert!(
            self.0.fetch_sub(1, Ordering::AcqRel) > 0,
            "preparation accounting underflow"
        );
    }
}

async fn prepare_group<RT: Runtime>(
    rt: RT,
    database: Database<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    generation: &Arc<DeploymentGeneration>,
    group: &PreparationGroup,
    target: PreparationTarget,
    mode: PreparationMode,
    batch_deadline: tokio::time::Instant,
    retention_targets: &BTreeMap<GeneratedPoolIdentity, usize>,
    progress: &parking_lot::Mutex<&'static str>,
) -> anyhow::Result<PreparationAdmission> {
    let capacity = current_group_capacity::<RT>(&group.routed);
    if let Some(admission) = capacity.admission(target, mode) {
        if admission == PreparationAdmission::CapacityUnavailable {
            *progress.lock() = "awaiting_idle_baseline";
        }
        return Ok(admission);
    }
    let Some(transaction) =
        wait_for_preparation(generation, mode, batch_deadline, database.begin_system()).await?
    else {
        return Ok(PreparationAdmission::CapacityUnavailable);
    };
    // Each scheduling turn creates at most one Store. Reassess proportional
    // coverage in the caller rather than chasing losses within one hot group.
    let result = prepare_fresh_instance(
        rt,
        group,
        transaction,
        default_system_env_vars,
        mode,
        batch_deadline,
        retention_targets,
        target,
        progress,
    )
    .await;
    match result {
        Err(error) if is_preparation_memory_shortfall(&error) => {
            // The construction owner has disposed the rejected Store and
            // completed its accounting. Aggregate growth denial is capacity,
            // not an initializer failure that should poison primary admission.
            *progress.lock() = "hard_budget";
            Ok(PreparationAdmission::CapacityUnavailable)
        },
        result => result,
    }
}

fn is_preparation_memory_shortfall(error: &anyhow::Error) -> bool {
    error.downcast_ref::<GeneratedGuestMemoryLimit>() == Some(&GeneratedGuestMemoryLimit::Aggregate)
        && !error.is::<GeneratedNonRetryableFailure>()
        && !error.is::<ErrorMetadata>()
        && !error.is::<Trap>()
        && matches!(
            error.downcast_ref::<StaticHermesWasmExecutionFailure>(),
            None
                | Some(
                    StaticHermesWasmExecutionFailure::RuntimeInitialization
                        | StaticHermesWasmExecutionFailure::AggregateMemoryLimit
                )
        )
}

// The caller holds the pool lock. Return only idle surplus, using the same
// serving and activation floors for allocation planning and actual eviction.
fn preparation_surplus<'a, RT: Runtime>(
    pool: &'a GeneratedRoutedInstancePool,
    generation: &Arc<DeploymentGeneration>,
    mode: PreparationMode,
    targets: &BTreeMap<GeneratedPoolIdentity, usize>,
) -> Vec<&'a GeneratedReusableInstance<RT>> {
    let policy = runtime_policy_store().snapshot().policy;
    let demands = DEMAND.lock();
    let selected = demands.selected_generation.upgrade();
    // A reloaded incarnation can have the same graph/pool digest as an
    // obsolete owner. Only exact-owner instances provide its coverage.
    let mut idle = BTreeMap::<(usize, Arc<GeneratedPoolIdentity>), usize>::new();
    for instance in pool
        .iter()
        .filter_map(|value| value.downcast_ref::<GeneratedReusableInstance<RT>>())
    {
        if instance.all_entries_prepared
            && let Some(owner) = &instance.routed.generation
        {
            *idle
                .entry((
                    Arc::as_ptr(owner) as usize,
                    Arc::clone(&instance.pool_identity),
                ))
                .or_default() += 1;
        }
    }
    let mut ordered = pool
        .iter()
        .filter_map(|value| value.downcast_ref::<GeneratedReusableInstance<RT>>())
        .collect::<Vec<_>>();
    ordered.sort_by_key(|instance| instance.idle_since);
    ordered
        .into_iter()
        .filter(|instance| {
            let owner = instance.routed.generation.as_ref();
            let same_generation = owner.is_some_and(|owner| Arc::ptr_eq(owner, generation));
            let serving = owner.is_some_and(|owner| {
                selected
                    .as_ref()
                    .is_some_and(|selected| Arc::ptr_eq(owner, selected))
            });
            let protect_serving = serving && mode.is_activation();
            if protect_serving || same_generation {
                // Checked-out and reserved Stores already occupy population
                // capacity and are never eviction candidates.
                if !instance.all_entries_prepared {
                    return true;
                }
                let serving_target = if protect_serving {
                    let registered = &demands.pools[instance.pool_identity.as_ref()];
                    registered
                        .serving_scope(policy)
                        .map_or(1, |scope| registered.population_target(scope))
                } else {
                    0
                };
                let activation_target = if same_generation {
                    // Other kinds in this generation may be outside the
                    // destination primary policy and have no target here.
                    targets
                        .get(instance.pool_identity.as_ref())
                        .copied()
                        .unwrap_or(0)
                } else {
                    0
                };
                // Same-generation promotion must preserve both populations:
                // the old serving kinds and the captured destination plan.
                // Otherwise each optional copy can evict its predecessor.
                let mut minimum = serving_target.max(activation_target).saturating_sub(
                    if serving || same_generation {
                        let occupancy = demands.occupancy(
                            owner.expect("protected runtime has no generation owner"),
                            &instance.pool_identity,
                        );
                        occupancy.active + occupancy.reserved
                    } else {
                        0
                    },
                );
                if same_generation
                    && mode.is_activation()
                    && targets
                        .get(instance.pool_identity.as_ref())
                        .is_some_and(|target| *target > 0)
                {
                    // Held occupancy is not initialization evidence. Keep
                    // an idle baseline for final dependency validation unless
                    // this group deliberately yields its turn to reordering.
                    minimum = minimum.max(1);
                }
                let owner = owner.expect("protected runtime has no generation owner");
                let remaining = idle
                    .get_mut(&(
                        Arc::as_ptr(owner) as usize,
                        Arc::clone(&instance.pool_identity),
                    ))
                    .expect("protected idle group disappeared");
                if *remaining <= minimum {
                    return false;
                }
                *remaining -= 1;
                true
            } else {
                !serving
            }
        })
        .collect()
}

async fn reclaim_preparation_surplus<RT: Runtime>(
    controller: &GeneratedMemoryController,
    generation: &Arc<DeploymentGeneration>,
    mode: PreparationMode,
    targets: &BTreeMap<GeneratedPoolIdentity, usize>,
    pending: &GeneratedRoutedModule,
    clone_peak_bytes: Option<usize>,
) -> anyhow::Result<bool> {
    let evicted = {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        let candidates = preparation_surplus::<RT>(&pool, generation, mode, targets)
            .into_iter()
            .map(|instance| (instance.memory_slot_id, instance.idle_since.elapsed()))
            .collect::<Vec<_>>();
        // Reallocation is independent of the global age-retention floor.
        // The supplied candidates already exclude useful serving capacity.
        let identity = generated_memory_identity_for_package(
            &pending.manifest,
            &pending.package_identity,
            &pending.route_identity,
        );
        let evictions =
            controller.preparation_eviction_candidates(&identity, clone_peak_bytes, &candidates);
        evictions
            .into_iter()
            .map(|(slot, reason)| {
                (
                    pool.take_slot::<RT>(slot)
                        .expect("preparation surplus disappeared under the pool lock"),
                    reason,
                )
            })
            .collect()
    };
    discard_preparation_instances(controller, evicted, IdleEvictionTrigger::AdmissionBudget).await
}

async fn discard_preparation_instances<RT: Runtime>(
    controller: &GeneratedMemoryController,
    evicted: Vec<(GeneratedReusableInstance<RT>, IdleEvictionReason)>,
    trigger: IdleEvictionTrigger,
) -> anyhow::Result<bool> {
    match discard_generated_idle_instances(
        controller,
        evicted,
        trigger,
        IdleEvictionLimit::maintenance(),
    )
    .await
    {
        Ok(reclaimed) => Ok(reclaimed),
        // This helper drops every Store and finishes every ledger eviction even
        // when guest teardown cannot acquire CPU within its short bound. The
        // capacity was reclaimed; contention must not suspend replenishment.
        Err(IdleEvictionError::Deadline) => Ok(true),
        Err(error) => Err(error.into()),
    }
}

async fn prepare_fresh_instance<RT: Runtime>(
    rt: RT,
    group: &PreparationGroup,
    transaction: Transaction<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    mode: PreparationMode,
    deadline: tokio::time::Instant,
    retention_targets: &BTreeMap<GeneratedPoolIdentity, usize>,
    target: PreparationTarget,
    progress: &parking_lot::Mutex<&'static str>,
) -> anyhow::Result<PreparationAdmission> {
    let Some(_preparation_permit) = wait_for_preparation(
        group
            .routed
            .generation
            .as_ref()
            .context("preparation lost its generation")?,
        mode,
        deadline,
        async {
            PREPARATION_LIMIT
                .acquire()
                .await
                .context("preparation limiter closed")
        },
    )
    .await?
    else {
        return Ok(PreparationAdmission::CapacityUnavailable);
    };
    // Report admitted preparation owners, not groups queued behind the global
    // limit or a no-op inspection of an already covered target.
    group.in_progress.fetch_add(1, Ordering::AcqRel);
    let _in_progress = GroupPreparationOwner(Arc::clone(&group.in_progress));
    let routed = Arc::clone(&group.routed);
    let configuration = route_configuration()?;
    anyhow::ensure!(
        configuration.reuse_instances,
        "Wasm preparation requires instance reuse"
    );
    routed.package_identity.entry_preparation_selectors()?;
    let limiter = configuration
        .active_wasm_cpu_limiter
        .as_ref()
        .context("Wasm preparation has no active CPU limiter")?;
    let controller = &configuration.generated_memory_controller;
    let maximum_handles = usize::try_from(routed.manifest.limits().max_value_handles())?;
    let maximum_host_owned_bytes =
        usize::try_from(routed.manifest.limits().max_host_owned_bytes())?;
    let maximum_guest_memory_bytes =
        usize::try_from(routed.manifest.limits().max_guest_memory_bytes())?;
    let mut provider = DatabaseUdfWasmInvocation::for_preparation(
        rt.clone(),
        transaction,
        default_system_env_vars,
    );
    provider.snoop_initialization_reads()?;
    let generation = routed
        .generation
        .as_ref()
        .context("preparation requires a generation owner")?;
    let memory_identity = generated_memory_identity_for_package(
        &routed.manifest,
        &routed.package_identity,
        &routed.route_identity,
    );
    let cancellation = CancellationSignal::new();
    // Read the cache outside pool/controller locks. A forecast grants no image
    // authority: the lifecycle still validates dependencies and actual growth.
    let clone_peak_bytes = group.images.clone_construction_peak_bytes();
    let (permit, memory_permit) = loop {
        // Join the ordinary CPU queue so sustained traffic cannot starve
        // replacements. Never hold CPU while waiting for memory admission.
        *progress.lock() = "cpu";
        let admitted = wait_for_preparation(
            generation,
            mode,
            deadline,
            acquire_active_wasm_cpu_permit(
                limiter,
                &cancellation,
                false,
                deadline,
                ActiveJavascriptClass::Protected,
            ),
        )
        .await;
        let permit = match admitted {
            Ok(Some(permit)) => permit,
            Ok(None) => return Ok(PreparationAdmission::CapacityUnavailable),
            Err(error)
                if error.downcast_ref::<StaticHermesWasmAdmissionFailure>()
                    == Some(&StaticHermesWasmAdmissionFailure::Cpu) =>
            {
                return Ok(PreparationAdmission::CapacityUnavailable);
            },
            Err(error) => return Err(error),
        };
        // Serving and publication owners can transfer Stores while CPU is
        // queued. Recheck total capacity, not an obsolete idle deficit.
        let capacity = current_group_capacity::<RT>(&routed);
        if let Some(admission) = capacity.admission(target, mode) {
            if admission == PreparationAdmission::CapacityUnavailable {
                *progress.lock() = "awaiting_idle_baseline";
            }
            return Ok(admission);
        }
        // Activation can begin while this maintenance task waits for CPU.
        // Recheck before memory admission so stale optional targets cannot
        // repeatedly take back the candidate's construction room.
        if mode == PreparationMode::Maintenance
            && retention_targets[&routed.pool_identity] > 1
            && other_generation_is_activating(generation)
        {
            return Ok(PreparationAdmission::CapacityUnavailable);
        }
        let rejection = match controller
            .try_admit_preparation(memory_identity.clone(), clone_peak_bytes)
        {
            Ok(memory_permit) => break (permit, memory_permit),
            Err(TryAdmissionError::Permanent(AdmissionRejection::ForecastExceedsHardBudget)) => {
                *progress.lock() = "forecast_exceeds_hard_budget";
                return Ok(PreparationAdmission::CapacityUnavailable);
            },
            Err(TryAdmissionError::Wait(reason)) => {
                drop(permit);
                reason
            },
        };
        *progress.lock() = match rejection {
            AdmissionWaitReason::Pressure => "pressure",
            AdmissionWaitReason::ActiveCountCeiling | AdmissionWaitReason::InstanceCountCeiling => {
                "instance_limit"
            },
            AdmissionWaitReason::SlotUnavailable => "slot_unavailable",
            AdmissionWaitReason::SoftBudget => "soft_budget",
            AdmissionWaitReason::HardBudget => "hard_budget",
            AdmissionWaitReason::FunctionRecordCapacity => "function_records",
        };
        if mode == PreparationMode::Maintenance
            && matches!(
                rejection,
                AdmissionWaitReason::SoftBudget | AdmissionWaitReason::HardBudget
            )
            && preparation_is_current(generation, mode)
            && let Some(residency) = configuration
                .deployment_registry
                .as_ref()
                .and_then(|registry| registry.source_keyed_residency.as_ref())
            && residency.retire_other_generation_for_admission(generation)
        {
            continue;
        }
        if reclaim_preparation_surplus::<RT>(
            controller,
            generation,
            mode,
            retention_targets,
            &routed,
            clone_peak_bytes,
        )
        .await?
        {
            continue;
        }
        return Ok(PreparationAdmission::CapacityUnavailable);
    };
    if !preparation_is_current(generation, mode) || tokio::time::Instant::now() >= deadline {
        memory_permit.finish_unstarted(false);
        return Ok(PreparationAdmission::CapacityUnavailable);
    }
    #[cfg(any(test, feature = "testing"))]
    let metrics = Arc::new(GateMetrics::default());
    #[cfg(not(any(test, feature = "testing")))]
    let metrics = Arc::clone(&PRODUCTION_GATE_METRICS);
    let mut state = new_invocation_state(
        rt.clone(),
        provider,
        StaticHermesUdfCallback::Unavailable,
        None,
        metrics,
        permit,
    );
    let observer = memory_permit.observer();
    let interrupt = Arc::new(GeneratedInterruptState::default());
    interrupt.set_execution_phase(GeneratedExecutionPhase::Preparing);
    state.generated = Some(GeneratedInvocationState {
        manifest: Arc::clone(&routed.manifest),
        values: OpaqueValueTable::new_with_observer(
            maximum_handles,
            maximum_host_owned_bytes,
            Some(observer),
        ),
        async_operations: AsyncOperationState::default(),
        capability_bridge: InvocationCapabilityBridge::unissued(),
        performance_monotonic_start: rt.monotonic_now(),
        performance_runtime_available: false,
        runtime_reuse_contaminated: false,
        allows_caught_official_output_chunk_initialization_failure: false,
        discard_after_caught_initialization_failure: false,
        context_read_set_required: true,
        request_handle: None,
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
        host_secret_values: BTreeMap::new(),
    });
    if let Err(error) = arm_generated_timeout(
        rt,
        &mut state,
        GENERATED_INITIALIZATION_TIMEOUT,
        *DATABASE_UDF_SYSTEM_TIMEOUT,
    ) {
        // Drop the value table before the unstarted permit releases admission.
        drop(state);
        return Err(error);
    }
    // Successful but expensive replacements can consume every batch while
    // traffic discards them again. Persist this turn so the next maintenance
    // batch starts with groups it has not reached. Capacity-only waits do not
    // consume a turn, including a baseline for which we just reclaimed memory.
    *group.last_started.lock() = Some(Instant::now());
    *progress.lock() = "initializing";
    let mut output = execute_generated_with_lifecycle_barrier_and_entry_selector(
        Arc::clone(&routed),
        routed.entry_selector,
        GeneratedExecutionPurpose::Preparation,
        state,
        None,
        true,
        None,
        Some(Arc::clone(&group.images)),
    )
    .await?;
    let error = if let Some(error) = output.system_error.take() {
        Some(error)
    } else {
        match &output.invocation.outcome {
            InvocationOutcome::Success => None,
            InvocationOutcome::DeveloperError(_) => {
                // Initializers can include configuration values in their
                // exception text. Infrastructure errors must not retain it.
                Some(anyhow::anyhow!("Wasm initializer raised a guest exception"))
            },
            InvocationOutcome::InitializationTimeout => {
                Some(StaticHermesWasmExecutionFailure::InitializationTimeout.into())
            },
            InvocationOutcome::ActiveTimeout => {
                Some(StaticHermesWasmExecutionFailure::ExecutionTimeout.into())
            },
            InvocationOutcome::FuelExhausted => {
                Some(StaticHermesWasmExecutionFailure::InstructionBudget.into())
            },
            InvocationOutcome::SystemTimeout | InvocationOutcome::SystemError => {
                Some(anyhow::anyhow!("Wasm preparation lost its system error"))
            },
        }
    };
    let error = error.or_else(|| {
        (output.reusable_instance.is_none()
            || output.memory_permit.is_none()
            || output.invocation.preparation_log_lines.is_none())
        .then(|| anyhow::anyhow!("successful preparation lost its retained state"))
    });
    if let Some(error) = error {
        let error = match output.reusable_instance.take() {
            Some(instance) => match discard_generated_instance(instance).await {
                Ok(()) => error,
                Err(cleanup_error) => {
                    // A retained guest error outcome can still fail destruction.
                    // Complete its memory charge once, with the cleanup failure.
                    output.terminal_memory_outcome = TerminalMemoryOutcome::SystemError;
                    error
                        .context(format!(
                            "generated Wasm runtime cleanup also failed: {cleanup_error:#}"
                        ))
                        .context(GeneratedNonRetryableFailure::Cleanup)
                },
            },
            None => error,
        };
        if let Some(permit) = output.memory_permit.take() {
            permit.finish(output.terminal_memory_outcome, false);
        }
        return Err(allow_disposed_wasm_primary_fallback(error));
    }
    let mut instance = output
        .reusable_instance
        .take()
        .expect("validated prepared runtime disappeared");
    let permit = output
        .memory_permit
        .take()
        .expect("validated preparation memory permit disappeared");
    *group.memory_cost.lock() = Some(permit.preparation_memory_cost());
    instance.pending_initialization_logs = output
        .invocation
        .preparation_log_lines
        .take()
        .expect("validated preparation initialization logs disappeared");
    match return_generated_routed_instance(instance, permit, output.terminal_memory_outcome).await?
    {
        GeneratedInstanceReturn::Pooled => Ok(PreparationAdmission::Prepared),
        GeneratedInstanceReturn::CapacityUnavailable => {
            *progress.lock() = "retention_budget";
            Ok(PreparationAdmission::CapacityUnavailable)
        },
        GeneratedInstanceReturn::GenerationRetired => Ok(PreparationAdmission::CapacityUnavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demand_entry(module: &str) -> DemandEntry {
        DemandEntry {
            module: module.to_owned(),
            export: "run".to_owned(),
            kind: DemandScope::Query,
        }
    }

    #[test]
    fn disposed_preparation_memory_pressure_is_not_an_initializer_failure() {
        let aggregate = || anyhow::Error::new(GeneratedGuestMemoryLimit::Aggregate);
        assert!(is_preparation_memory_shortfall(&aggregate()));
        for failure in [
            StaticHermesWasmExecutionFailure::RuntimeInitialization,
            StaticHermesWasmExecutionFailure::AggregateMemoryLimit,
        ] {
            assert!(is_preparation_memory_shortfall(&aggregate().context(failure)));
        }
        assert!(!is_preparation_memory_shortfall(&anyhow::Error::new(
            GeneratedGuestMemoryLimit::Instance,
        )));
        assert!(!is_preparation_memory_shortfall(&anyhow::anyhow!(
            "initializer failed"
        )));
        for failure in [
            GeneratedNonRetryableFailure::HostOperation,
            GeneratedNonRetryableFailure::Cleanup,
            GeneratedNonRetryableFailure::Cancelled,
        ] {
            assert!(!is_preparation_memory_shortfall(&aggregate().context(failure)));
        }
        for failure in [
            StaticHermesWasmExecutionFailure::InstructionBudget,
            StaticHermesWasmExecutionFailure::InitializationTimeout,
            StaticHermesWasmExecutionFailure::RuntimeCleanup,
            StaticHermesWasmExecutionFailure::HostAbiInvariant,
        ] {
            assert!(!is_preparation_memory_shortfall(&aggregate().context(failure)));
        }
        assert!(!is_preparation_memory_shortfall(
            &aggregate().context(Trap::OutOfFuel),
        ));
        assert!(!is_preparation_memory_shortfall(&aggregate().context(
            ErrorMetadata::bad_request("PreparationTestError", "host operation failed"),
        )));
    }

    #[test]
    fn captured_population_uses_peak_without_reserve_and_shares_capacity_proportionally() {
        assert_eq!(allocate_population(&[50, 20], &[0, 0], 100), vec![50, 20]);
        assert_eq!(allocate_population(&[50, 20], &[0, 0], 35), vec![25, 10]);
        assert_eq!(allocate_population(&[50, 20], &[0, 0], 2), vec![1, 1]);
        assert_eq!(allocate_population(&[50, 20], &[30, 0], 35), vec![30, 5]);
        assert_eq!(allocate_population(&[1, 1], &[0, 0], 100), vec![1, 1]);
        // Checked-out Stores from other kinds or verifiers consume slots but
        // cannot become a larger refill target once those executions finish.
        assert_eq!(allocate_population(&[2, 10], &[5, 0], 10), vec![2, 5]);
        assert_eq!(allocate_population(&[2, 10], &[5, 0], 100), vec![2, 10]);
    }

    #[test]
    fn baseline_uses_free_population_capacity_without_adding_a_reserve() {
        let mode = PreparationMode::PrimaryActivation {
            queries: true,
            mutations: false,
        };
        let target = PreparationTarget::Baseline { population: 4 };
        assert!(GroupCapacity { prepared: 0, total: 1 }
            .admission(target, mode)
            .is_none());
        assert!(matches!(
            GroupCapacity { prepared: 1, total: 2 }.admission(target, mode),
            Some(PreparationAdmission::Prepared)
        ));
        assert!(matches!(
            GroupCapacity { prepared: 0, total: 4 }.admission(target, mode),
            Some(PreparationAdmission::CapacityUnavailable)
        ));
        assert!(matches!(
            GroupCapacity { prepared: 0, total: 1 }.admission(
                PreparationTarget::Baseline { population: 1 },
                PreparationMode::Maintenance,
            ),
            Some(PreparationAdmission::Prepared)
        ));
    }

    #[test]
    fn activation_reservation_preserves_population_until_pool_return() -> anyhow::Result<()> {
        use crate::environment::udf::wasm_udf_package::tests::RuntimeRegistrySourceKeyedCapabilityFixture;

        let tokio = ProdRuntime::init_tokio()?;
        let rt = ProdRuntime::new(&tokio);
        let _test_hooks_lock = TEST_HOOKS_TEST_LOCK.lock();
        rt.clone().block_on("activation_reserved_population", async move {
            let fixture =
                RuntimeRegistrySourceKeyedCapabilityFixture::create(b"core-wasm", b"aot", 1)?;
            let registry = DeploymentRegistry::load_with_selection(
                fixture.root().to_owned(),
                false,
                true,
                true,
            )?;
            let residency = registry.source_keyed_residency.as_ref().unwrap().clone();
            let initial = fixture.initial();
            let selector = SourceKeyedGenerationSelector {
                deployment_sha256: initial.deployment_sha256.clone(),
                generation_manifest_sha256: initial.generation_file_sha256.clone(),
                generation_sha256: initial.generation_sha256.clone(),
                source_package_runtime_content_sha256: initial
                    .source_package_runtime_content_sha256
                    .clone(),
            };
            let new_generation = || {
                DeploymentGeneration::from_legacy(
                    fixture.root().to_owned(),
                    ValidatedDeploymentManifest::empty_for_test(&initial.deployment_sha256),
                )
            };
            let generation = new_generation();
            residency
                .state
                .lock()
                .insert_and_evict_lru(selector.clone(), Arc::clone(&generation));
            let manifest = generated_test_manifest_with_limits(
                ManifestUdfKind::Query,
                Vec::new(),
                4 * 64 * 1024,
            )?;
            let routed = generated_memory_test_routed_module_with_generation(
                Arc::clone(&manifest),
                GeneratedMemoryTestOperation {
                    additional_pages: 0,
                    destroy_infinite_loop: false,
                    loop_iterations: Some(0),
                    maximum_pages: 4,
                },
                GeneratedRouteIdentity::Singleton {
                    package_key: "activation-reserved-population".to_owned(),
                },
                Some(Arc::clone(&generation)),
            )?;
            static BUFFERS: packed_value::memory::BufferMemory =
                packed_value::memory::BufferMemory::new();
            let controller = GeneratedMemoryController::with_shared_buffers(
                GeneratedMemoryPolicy {
                    hard_instance_ceiling: 8,
                    soft_budget_bytes: 8 << 20,
                    hard_budget_bytes: 12 << 20,
                    safety_reserve_bytes: 1 << 20,
                    unattributed_bytes_per_slot: 4096,
                    cold_peak_growth_bytes: 64 * 1024,
                    warm_idle_target: 4,
                    maximum_idle_age: Duration::from_secs(60),
                    pressure_enter_headroom_bytes: 1 << 20,
                    pressure_exit_headroom_bytes: 2 << 20,
                },
                &BUFFERS,
            )?;
            controller.set_pressure_for_test(BackendPressure::Healthy {
                headroom_bytes: 16 << 20,
            });
            let database = new_test_database(rt.clone()).await?;
            register_demand(
                &mut DEMAND.lock(),
                &generation,
                &routed.pool_identity,
                BTreeSet::from([demand_entry("entry.js")]),
                BTreeMap::new(),
            );
            for _ in 0..4 {
                let mut state = generated_test_state(
                    rt.clone(),
                    database.begin_system().await?,
                    Arc::clone(&manifest),
                    JsonValue::Null,
                    UdfType::Query,
                    UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
                    CancellationSignal::new_for_test(),
                    None,
                    Arc::new(GateMetrics::default()),
                    Some(GeneratedTestMemorySetup {
                        controller: Arc::clone(&controller),
                        route_identity: routed.route_identity.clone(),
                        existing_slot: None,
                        values: None,
                        maximum_guest_memory_bytes: 4 * 64 * 1024,
                    }),
                )
                .await?;
                let generated = generated_state_mut(&mut state)?;
                let request = generated
                    .request_handle
                    .take()
                    .context("fixture request missing")?;
                generated
                    .values
                    .release(request, OpaqueValueKind::ConvexJson)?;
                generated.context_read_set_required = true;
                state.provider = DatabaseUdfWasmInvocation::for_preparation(
                    rt.clone(),
                    state.provider.take_transaction()?,
                    BTreeMap::new(),
                );
                state.provider.snoop_initialization_reads()?;
                arm_generated_timeout(
                    rt.clone(),
                    &mut state,
                    Duration::from_secs(5),
                    *DATABASE_UDF_SYSTEM_TIMEOUT,
                )?;
                let mut output = execute_generated_with_lifecycle_barrier_and_entry_selector(
                    Arc::clone(&routed),
                    None,
                    GeneratedExecutionPurpose::Preparation,
                    state,
                    None,
                    true,
                    None,
                    None,
                )
                .await?;
                assert_eq!(
                    output.invocation.outcome,
                    InvocationOutcome::Success,
                    "{:?}",
                    output.system_error,
                );
                let instance = output
                    .reusable_instance
                    .take()
                    .context("prepared fixture lost its Store")?;
                assert!(instance.all_entries_prepared && instance.context_read_set.is_some());
                return_generated_routed_instance(
                    instance,
                    output
                        .memory_permit
                        .take()
                        .context("prepared fixture lost its permit")?,
                    output.terminal_memory_outcome,
                )
                .await?;
            }
            let targets = BTreeMap::from([(routed.pool_identity.clone(), 4)]);
            let inventory = capture_inventory::<ProdRuntime>(&generation, &targets, None);
            let original = inventory.groups[0]
                .instances
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            assert_eq!(original.len(), 4);
            let prepared = PreparedSourceKeyedRuntime {
                registry,
                residency,
                selector,
                inventory,
                policy_revision: runtime_policy_store().snapshot().revision,
            };
            let reservation = prepared.reserve::<ProdRuntime>()?;
            reservation
                .validate_dependencies(&mut database.begin_system().await?)
                .await?;
            let occupancy = population_occupancy(&generation, &routed.pool_identity);
            assert_eq!((occupancy.active, occupancy.reserved), (0, 4));
            assert_eq!(controller.preparation_instance_capacity(0).available, 4);
            let held = occupancy.active + occupancy.reserved;
            let allocation = controller.preparation_instance_capacity(held);
            assert_eq!(allocation.available, 8);
            assert_eq!(
                allocate_population(&[4], &[held], allocation.available),
                vec![4],
            );
            let capacity = current_group_capacity::<ProdRuntime>(&routed);
            assert_eq!((capacity.prepared, capacity.total), (0, 4));
            let mode = PreparationMode::PrimaryActivation {
                queries: true,
                mutations: false,
            };
            assert!(matches!(
                capacity.admission(PreparationTarget::Baseline { population: 4 }, mode),
                Some(PreparationAdmission::CapacityUnavailable),
            ));
            assert!(matches!(
                capacity.admission(PreparationTarget::Population(4), PreparationMode::Maintenance),
                Some(PreparationAdmission::Prepared),
            ));
            // A held population cannot supply another caller's idle baseline.
            assert!(prepared.reserve::<ProdRuntime>().is_err());
            let held_inventory = capture_inventory::<ProdRuntime>(&generation, &targets, None);
            assert!(held_inventory.groups[0].instances.is_empty());
            assert_eq!(held_inventory.groups[0].active, 0);
            assert_eq!(
                population_occupancy(&new_generation(), &routed.pool_identity).reserved,
                0,
            );
            let returned = GENERATED_GENERATION_RETIREMENT_NOTIFY.notified();
            tokio::pin!(returned);
            let _ = returned.as_mut().enable();
            drop(reservation);
            assert!(futures::poll!(&mut returned).is_ready());
            let occupancy = population_occupancy(&generation, &routed.pool_identity);
            assert_eq!((occupancy.active, occupancy.reserved), (0, 0));
            let capacity = current_group_capacity::<ProdRuntime>(&routed);
            assert_eq!((capacity.prepared, capacity.total), (4, 4));
            assert_eq!(controller.preparation_instance_capacity(0).available, 4);
            let restored = capture_inventory::<ProdRuntime>(&generation, &targets, None);
            assert_eq!(
                restored.groups[0]
                    .instances
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>(),
                original,
            );
            assert!(matches!(
                capacity.admission(PreparationTarget::Baseline { population: 4 }, mode),
                Some(PreparationAdmission::Prepared),
            ));
            {
                let demands = DEMAND.lock();
                let registered = &demands.pools[&routed.pool_identity];
                for demand in registered.demand.iter().chain(&registered.primary_demand) {
                    let demand = demand.lock();
                    assert_eq!((demand.active, demand.peak), (0, 0));
                }
            }
            retire_deployment_generation::<ProdRuntime>(&controller, generation).await?;
            DEMAND.lock().pools.remove(&routed.pool_identity);
            database.shutdown().await?;
            Ok(())
        })
    }

    #[test]
    fn primary_store_history_precedes_v8_bootstrap_and_expires() {
        let generation = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/primary-demand"),
            ValidatedDeploymentManifest::empty_for_test(&"a".repeat(64)),
        );
        let pool = GeneratedPoolIdentity::Route(GeneratedRouteIdentity::Singleton {
            package_key: "primary-demand".to_owned(),
        });
        let mut demands = PreparationDemand::default();
        register_demand(
            &mut demands,
            &generation,
            &pool,
            BTreeSet::from([demand_entry("entry.js")]),
            BTreeMap::new(),
        );
        let registered = &demands.pools[&pool];
        registered.primary_demand[DemandScope::Both as usize]
            .lock()
            .peak = 50;
        assert_eq!(registered.population_target(DemandScope::Both), 50);
        // Shadow occupancy still owns capacity but is not an actual primary
        // observation and does not extrapolate the sampled fraction.
        registered.checked_out.store(5, Ordering::Release);
        assert_eq!(registered.population_target(DemandScope::Both), 50);
        registered.demand[DemandScope::Both as usize].lock().peak = 20;
        assert_eq!(registered.population_target(DemandScope::Both), 20);
        for demand in registered.demand.iter().chain(&registered.primary_demand) {
            demand.lock().window_started = Instant::now() - DEMAND_WINDOW * DEMAND_BUCKETS as u32;
        }
        assert_eq!(registered.population_target(DemandScope::Both), 1);
    }

    #[test]
    fn mixed_primary_kinds_use_their_own_history_until_both_have_wasm_observations() {
        let generation = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/mixed-demand"),
            ValidatedDeploymentManifest::empty_for_test(&"b".repeat(64)),
        );
        let pool = GeneratedPoolIdentity::Route(GeneratedRouteIdentity::Singleton {
            package_key: "mixed-demand".to_owned(),
        });
        let mut demands = PreparationDemand::default();
        register_demand(
            &mut demands,
            &generation,
            &pool,
            BTreeSet::from([demand_entry("entry.js")]),
            BTreeMap::new(),
        );
        let registered = &demands.pools[&pool];
        registered.demand[DemandScope::Query as usize].lock().peak = 4;
        registered.demand[DemandScope::Both as usize].lock().peak = 4;
        registered.primary_demand[DemandScope::Query as usize]
            .lock()
            .peak = 12;
        registered.primary_demand[DemandScope::Mutation as usize]
            .lock()
            .peak = 9;
        registered.primary_demand[DemandScope::Both as usize]
            .lock()
            .peak = 18;
        assert_eq!(registered.population_target(DemandScope::Query), 4);
        assert_eq!(registered.population_target(DemandScope::Mutation), 9);
        assert_eq!(registered.population_target(DemandScope::Both), 18);
        registered.demand[DemandScope::Mutation as usize]
            .lock()
            .peak = 3;
        registered.demand[DemandScope::Both as usize].lock().peak = 6;
        assert_eq!(registered.population_target(DemandScope::Both), 6);
    }

    #[test]
    fn regrouping_inherits_only_the_requested_kind() {
        let generations = ['a', 'b'].map(|name| {
            DeploymentGeneration::from_legacy(
                PathBuf::from("/unused/regrouped-demand"),
                ValidatedDeploymentManifest::empty_for_test(&name.to_string().repeat(64)),
            )
        });
        let [original, regrouped] = ["original", "regrouped"].map(|name| {
            GeneratedPoolIdentity::Route(GeneratedRouteIdentity::Singleton {
                package_key: name.to_owned(),
            })
        });
        let shared_mutation = DemandEntry {
            kind: DemandScope::Mutation,
            ..demand_entry("mutation.js")
        };
        let mut demands = PreparationDemand::default();
        register_demand(
            &mut demands,
            &generations[0],
            &original,
            BTreeSet::from([demand_entry("old-query.js"), shared_mutation.clone()]),
            BTreeMap::new(),
        );
        let registered = &demands.pools[&original];
        for history in [&registered.demand, &registered.primary_demand] {
            history[DemandScope::Query as usize].lock().peak = 100;
            history[DemandScope::Mutation as usize].lock().peak = 7;
            history[DemandScope::Both as usize].lock().peak = 105;
        }
        register_demand(
            &mut demands,
            &generations[1],
            &regrouped,
            BTreeSet::from([demand_entry("new-query.js"), shared_mutation]),
            BTreeMap::new(),
        );
        let registered = &demands.pools[&regrouped];
        assert_eq!(registered.population_target(DemandScope::Query), 1);
        assert_eq!(registered.population_target(DemandScope::Mutation), 7);
        assert_eq!(registered.population_target(DemandScope::Both), 105);
    }

    #[test]
    fn exports_in_one_module_do_not_alias_pool_demand() {
        let generation = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/export-demand"),
            ValidatedDeploymentManifest::empty_for_test(&"c".repeat(64)),
        );
        let mut demands = PreparationDemand::default();
        for (export, peak) in [("first", 7), ("second", 2)] {
            let pool = GeneratedPoolIdentity::Route(GeneratedRouteIdentity::Singleton {
                package_key: export.to_owned(),
            });
            register_demand(
                &mut demands,
                &generation,
                &pool,
                BTreeSet::from([DemandEntry {
                    module: "shared.js".to_owned(),
                    export: export.to_owned(),
                    kind: DemandScope::Query,
                }]),
                BTreeMap::from([(
                    "shared.js".to_owned(),
                    BTreeMap::from([(export.to_owned(), ManifestUdfKind::Query)]),
                )]),
            );
            let registered = &demands.pools[&pool];
            assert_eq!(registered.population_target(DemandScope::Query), 1);
            registered.primary_demand[DemandScope::Query as usize]
                .lock()
                .peak = peak;
        }
        for (export, peak) in [("first", 7), ("second", 2)] {
            let pool = GeneratedPoolIdentity::Route(GeneratedRouteIdentity::Singleton {
                package_key: export.to_owned(),
            });
            assert_eq!(
                demands.pools[&pool].population_target(DemandScope::Query),
                peak
            );
        }
    }

    #[test]
    fn peak_buckets_keep_the_original_expiry_through_quiet_intervals() {
        let start = Instant::now();
        let mut demand = GroupDemand {
            active: 0,
            peak: 50,
            previous_peaks: [0; DEMAND_BUCKETS - 1],
            window_started: start,
            inherited: None,
        };
        demand.advance(start + DEMAND_WINDOW * 4);
        assert_eq!(demand.observation(), Some((50, start + DEMAND_WINDOW * 5)));
        demand.advance(start + DEMAND_WINDOW * 5);
        assert_eq!(demand.observation(), None);
    }

    #[tokio::test]
    async fn preparation_failure_survives_retry_and_is_scoped_to_incarnation() -> anyhow::Result<()>
    {
        let manifest =
            generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 4 * 64 * 1024)?;
        let new_generation = || {
            DeploymentGeneration::from_legacy(
                PathBuf::from("/unused/preparation-failure"),
                ValidatedDeploymentManifest::empty_for_test(&"f".repeat(64)),
            )
        };
        let new_route = |generation| {
            generated_memory_test_routed_module_with_generation(
                Arc::clone(&manifest),
                GeneratedMemoryTestOperation {
                    additional_pages: 0,
                    destroy_infinite_loop: false,
                    loop_iterations: Some(0),
                    maximum_pages: 4,
                },
                GeneratedRouteIdentity::Singleton {
                    package_key: "preparation-failure".to_owned(),
                },
                Some(generation),
            )
        };
        let generation = new_generation();
        let routed = new_route(Arc::clone(&generation))?;
        let replacement = new_route(new_generation())?;
        let group = PreparationGroup {
            routed: Arc::clone(&routed),
            members: BTreeSet::from([demand_entry("entry.js")]),
            exports: BTreeMap::new(),
            queries: true,
            mutations: false,
            memory_cost: Arc::new(parking_lot::Mutex::new(None)),
            retry_at: Arc::new(parking_lot::Mutex::new(None)),
            failed: Arc::new(parking_lot::Mutex::new(None)),
            last_started: Arc::new(parking_lot::Mutex::new(None)),
            images: Arc::default(),
            in_progress: Arc::default(),
        };
        PREPARATION_LOCK.lock().push(PreparedGeneration {
            generation: Arc::clone(&generation),
            groups: vec![group.clone()],
        });
        assert!(failed_preparation(&routed).is_none());
        let changed = GENERATED_GENERATION_RETIREMENT_NOTIFY.notified();
        tokio::pin!(changed);
        let _ = changed.as_mut().enable();
        let error = anyhow::anyhow!("preparation fixture");
        group.record_failure(&error);
        assert!(futures::poll!(&mut changed).is_ready());
        assert!(!failed_preparation(&routed)
            .unwrap()
            .is::<StaticHermesWasmFallbackReason>());
        assert!(failed_preparation(&replacement).is_none());
        group.record_failure(&anyhow::Error::new(
            StaticHermesWasmFallbackReason::GuestMemoryLimit,
        ));
        assert!(failed_preparation(&routed)
            .unwrap()
            .is::<StaticHermesWasmFallbackReason>());

        // A capacity-only retry rejoins the baseline reorder pass, but has not
        // supplied a working Store or disproved the earlier initializer failure.
        *group.retry_at.lock() = None;
        assert!(failed_preparation(&routed).is_some());
        group.record_success();
        assert!(failed_preparation(&routed).is_none());
        release_generation(&generation);
        Ok(())
    }

    #[test]
    fn maintenance_reaches_every_group_across_repeated_replacement_batches() -> anyhow::Result<()> {
        const PAGE: usize = 64 * 1024;
        let manifest =
            generated_test_manifest_with_limits(ManifestUdfKind::Query, Vec::new(), 4 * PAGE)?;
        let routed = generated_memory_test_routed_module(
            manifest,
            GeneratedMemoryTestOperation {
                additional_pages: 0,
                destroy_infinite_loop: false,
                loop_iterations: Some(0),
                maximum_pages: 4,
            },
            GeneratedRouteIdentity::Singleton {
                package_key: "preparation-scheduling".to_owned(),
            },
        )?;
        let controller = GeneratedMemoryController::new(GeneratedMemoryPolicy {
            hard_instance_ceiling: 8,
            soft_budget_bytes: 64 * PAGE,
            hard_budget_bytes: 128 * PAGE,
            safety_reserve_bytes: 4 * PAGE,
            unattributed_bytes_per_slot: PAGE,
            cold_peak_growth_bytes: 4 * PAGE,
            warm_idle_target: 0,
            maximum_idle_age: Duration::from_secs(60),
            pressure_enter_headroom_bytes: PAGE,
            pressure_exit_headroom_bytes: 2 * PAGE,
        })?;
        controller
            .try_admit_preparation(
                generated_memory_identity_for_package(
                    &routed.manifest,
                    &routed.package_identity,
                    &routed.route_identity,
                ),
                None,
            )
            .unwrap()
            .finish_unstarted(false);
        let mut now = Instant::now();
        let groups = (0..8)
            .map(|index| PreparationGroup {
                routed: Arc::clone(&routed),
                members: BTreeSet::from([demand_entry(&format!("entry-{index}.js"))]),
                exports: BTreeMap::new(),
                queries: true,
                mutations: false,
                memory_cost: Arc::new(parking_lot::Mutex::new(Some(PreparationMemoryCost {
                    peak_bytes: 4 * PAGE,
                    retained_bytes: PAGE,
                }))),
                retry_at: Arc::new(parking_lot::Mutex::new(None)),
                failed: Arc::new(parking_lot::Mutex::new(None)),
                last_started: Arc::new(parking_lot::Mutex::new(Some(now))),
                images: Arc::default(),
                in_progress: Arc::default(),
            })
            .collect::<Vec<_>>();
        let mut reached = BTreeSet::new();
        // All groups have previously prepared successfully, but traffic keeps
        // consuming and discarding their Stores. Seven 4.9-second replacements
        // exhaust each 30-second batch before its cheap eighth group can start.
        for _ in 0..2 {
            let deadline = now + Duration::from_secs(30);
            for index in
                preparation_order(&controller, &groups, PreparationOrder::LeastRecentlyStarted)
            {
                if now >= deadline {
                    break;
                }
                *groups[index].last_started.lock() = Some(now);
                reached.insert(index);
                now += Duration::from_millis(if index == 7 { 10 } else { 4900 });
            }
            now += GENERATED_POOL_MAINTENANCE_INTERVAL;
        }
        assert_eq!(reached.len(), groups.len());

        // A baseline whose memory was reclaimed keeps its turn if the batch
        // expires before CPU admission. Activation's memory order ignores the
        // maintenance service history.
        *groups[7].last_started.lock() = None;
        assert_eq!(
            preparation_order(&controller, &groups, PreparationOrder::LeastRecentlyStarted)[0],
            7
        );
        assert_eq!(
            preparation_order(&controller, &groups, PreparationOrder::Memory),
            (0..groups.len()).collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn inherited_demand_refreshes_from_late_observations_without_multiplying_estimates() {
        let generations = ['a', 'b', 'c'].map(|name| {
            DeploymentGeneration::from_legacy(
                PathBuf::from("/unused/preparation-demand"),
                ValidatedDeploymentManifest::empty_for_test(&name.to_string().repeat(64)),
            )
        });
        let [first, second, merged, split_first, split_second] =
            ["first", "second", "merged", "split-first", "split-second"].map(|name| {
                GeneratedPoolIdentity::Route(GeneratedRouteIdentity::Singleton {
                    package_key: name.to_owned(),
                })
            });
        let members = |names: &[&str]| names.iter().map(|name| demand_entry(name)).collect();
        let mut demands = PreparationDemand::default();
        for (identity, entry, peak) in [(&first, "a.js", 2), (&second, "b.js", 5)] {
            register_demand(
                &mut demands,
                &generations[0],
                identity,
                members(&[entry]),
                BTreeMap::new(),
            );
            demands.pools[identity].demand[DemandScope::Both as usize]
                .lock()
                .peak = peak;
        }
        register_demand(
            &mut demands,
            &generations[1],
            &merged,
            members(&["a.js", "b.js"]),
            BTreeMap::new(),
        );
        assert_eq!(demands.pools[&merged].target(DemandScope::Both), 7);
        let checked_out = Arc::clone(&demands.pools[&merged].checked_out);
        demands.pools[&second].demand[DemandScope::Both as usize]
            .lock()
            .peak = 9;
        register_demand(
            &mut demands,
            &generations[1],
            &merged,
            members(&["a.js", "b.js"]),
            BTreeMap::new(),
        );
        assert_eq!(demands.pools[&merged].target(DemandScope::Both), 11);
        assert!(Arc::ptr_eq(
            &checked_out,
            &demands.pools[&merged].checked_out
        ));
        assert_eq!(
            demands.pools[&merged].demand[DemandScope::Both as usize]
                .lock()
                .peak,
            0
        );
        // The merged estimate contributes no new observation to either sibling,
        // including the sibling registered after another inherited estimate.
        for (identity, entry, expected) in [(&split_second, "b.js", 9), (&split_first, "a.js", 2)] {
            register_demand(
                &mut demands,
                &generations[2],
                identity,
                members(&[entry]),
                BTreeMap::new(),
            );
            assert_eq!(demands.pools[identity].target(DemandScope::Both), expected);
        }
        for identity in [&first, &second] {
            demands.pools[identity].demand[DemandScope::Both as usize]
                .lock()
                .window_started = Instant::now() - DEMAND_WINDOW * (DEMAND_BUCKETS as u32 + 1);
        }
        register_demand(
            &mut demands,
            &generations[1],
            &merged,
            members(&["a.js", "b.js"]),
            BTreeMap::new(),
        );
        assert_eq!(demands.pools[&merged].target(DemandScope::Both), 1);
    }

    #[test]
    fn saturation_survives_a_short_burst_without_counting_rejected_requests() {
        let demand = Arc::new(parking_lot::Mutex::new(GroupDemand {
            active: 1,
            peak: 1,
            previous_peaks: [0; DEMAND_BUCKETS - 1],
            window_started: Instant::now(),
            inherited: None,
        }));
        let checked_out = Arc::new(AtomicUsize::new(1));
        let observation = DemandObservation {
            _observations: vec![PrimaryDemandObservation(Arc::clone(&demand))],
            checked_out: Arc::clone(&checked_out),
        };
        let mut registered = RegisteredDemand {
            generation: Weak::new(),
            members: vec![demand_entry("entry.js")],
            exports: BTreeMap::new(),
            demand: std::array::from_fn(|_| Arc::clone(&demand)),
            primary_demand: std::array::from_fn(|_| Arc::clone(&demand)),
            checked_out,
            reserved: Arc::new(AtomicUsize::new(0)),
            saturation: None,
        };
        for _ in 0..100 {
            registered.observe_saturation();
        }
        drop(observation);
        // The only admitted call finishes before maintenance samples demand.
        assert_eq!(registered.target(DemandScope::Both), 2);
        assert_eq!(demand.lock().target(), 1);
        registered.saturation.as_mut().unwrap().expires = Instant::now();
        assert_eq!(registered.target(DemandScope::Both), 1);
    }

    #[test]
    fn completion_preserves_demand_that_spans_an_unsampled_window() {
        let demand = Arc::new(parking_lot::Mutex::new(GroupDemand {
            active: 3,
            peak: 3,
            previous_peaks: [0; DEMAND_BUCKETS - 1],
            window_started: Instant::now() - DEMAND_WINDOW * (DEMAND_BUCKETS as u32 + 1),
            inherited: None,
        }));
        drop(DemandObservation {
            _observations: vec![PrimaryDemandObservation(Arc::clone(&demand))],
            checked_out: Arc::new(AtomicUsize::new(3)),
        });
        let mut demand = demand.lock();
        assert_eq!(demand.active, 2);
        assert_eq!(demand.target(), 3);
    }

    #[test]
    fn activation_protects_candidate_population_and_serving_demand_from_idle_age() {
        let mode = common::wasm_runtime_policy::RuntimeMode {
            primary: PrimaryRuntime::Wasm,
            shadow_bps: 0,
        };
        let policy = RuntimePolicy {
            query: mode,
            mutation: mode,
        };
        let selected = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/preparation-retention"),
            ValidatedDeploymentManifest::empty_for_test(&"a".repeat(64)),
        );
        let candidate = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/preparation-retention"),
            ValidatedDeploymentManifest::empty_for_test(&"b".repeat(64)),
        );
        let selected_pool = GeneratedPoolIdentity::Route(GeneratedRouteIdentity::Singleton {
            package_key: "selected".to_owned(),
        });
        let candidate_pool = GeneratedPoolIdentity::Route(GeneratedRouteIdentity::Singleton {
            package_key: "candidate".to_owned(),
        });
        let registered = |generation: &Arc<DeploymentGeneration>| RegisteredDemand {
            generation: Arc::downgrade(generation),
            members: vec![demand_entry("entry.js")],
            exports: BTreeMap::new(),
            demand: std::array::from_fn(|_| {
                Arc::new(parking_lot::Mutex::new(GroupDemand {
                    active: 1,
                    peak: 4,
                    previous_peaks: [0; DEMAND_BUCKETS - 1],
                    window_started: Instant::now(),
                    inherited: None,
                }))
            }),
            primary_demand: std::array::from_fn(|_| {
                Arc::new(parking_lot::Mutex::new(GroupDemand {
                    active: 0,
                    peak: 0,
                    previous_peaks: [0; DEMAND_BUCKETS - 1],
                    window_started: Instant::now(),
                    inherited: None,
                }))
            }),
            checked_out: Arc::new(AtomicUsize::new(1)),
            reserved: Arc::new(AtomicUsize::new(0)),
            saturation: None,
        };
        let demands = PreparationDemand {
            history: std::array::from_fn(|_| BTreeMap::new()),
            primary_history: std::array::from_fn(|_| BTreeMap::new()),
            pools: BTreeMap::from([
                (selected_pool.clone(), registered(&selected)),
                (candidate_pool.clone(), registered(&candidate)),
            ]),
            selected_generation: Arc::downgrade(&selected),
        };
        let activating = BTreeMap::from([(
            Arc::as_ptr(&candidate) as usize,
            BTreeMap::from([(candidate_pool.clone(), 4)]),
        )]);
        // A previous selection can retain active calls while it is prepared
        // again. Those Stores count only for that exact live incarnation.
        assert_eq!(demands.occupancy(&candidate, &candidate_pool).active, 1);
        let reloaded = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/preparation-retention"),
            ValidatedDeploymentManifest::empty_for_test(&"b".repeat(64)),
        );
        assert_eq!(demands.occupancy(&reloaded, &candidate_pool).active, 0);
        assert_eq!(
            demands.idle_retention_targets(true, &activating, policy),
            BTreeMap::from([(selected_pool.clone(), 3), (candidate_pool.clone(), 3)])
        );
        // A disconnected waiter may leave its bounded producer running. Once
        // that producer finishes, a staged generation has no age-retention owner.
        assert_eq!(
            demands.idle_retention_targets(true, &BTreeMap::new(), policy),
            BTreeMap::from([(selected_pool.clone(), 3)])
        );
        // Explicit activation still owns progress if the replenishment service
        // has stopped; its baseline must not depend on that service's lease.
        assert_eq!(
            demands.idle_retention_targets(false, &activating, policy),
            BTreeMap::from([(selected_pool.clone(), 3), (candidate_pool, 3)])
        );
        candidate.retired.store(true, Ordering::Release);
        assert_eq!(
            demands.idle_retention_targets(false, &activating, policy),
            BTreeMap::from([(selected_pool, 3)])
        );
        assert!(demands
            .idle_retention_targets(false, &BTreeMap::new(), policy)
            .is_empty());
    }
}
