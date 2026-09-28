use database::Database;

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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PreparationFlightKey {
    generation: usize,
}

#[derive(Clone)]
struct PreparationFlight {
    mode: PreparationMode,
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
    members: BTreeSet<String>,
    memory_cost: Arc<parking_lot::Mutex<Option<PreparationMemoryCost>>>,
    retry_at: Arc<parking_lot::Mutex<Option<Instant>>>,
    last_started: Arc<parking_lot::Mutex<Option<Instant>>>,
}

#[derive(Default)]
struct PreparationDemand {
    // Original observations survive retirement without retaining compiled graphs.
    history: BTreeMap<(String, Vec<String>), Arc<parking_lot::Mutex<GroupDemand>>>,
    pools: BTreeMap<GeneratedPoolIdentity, RegisteredDemand>,
    selected_generation: Weak<DeploymentGeneration>,
}

struct RegisteredDemand {
    generation: Weak<DeploymentGeneration>,
    members: Vec<String>,
    demand: Arc<parking_lot::Mutex<GroupDemand>>,
    checked_out: Arc<AtomicUsize>,
    saturation: Option<SaturationDemand>,
}

struct SaturationDemand {
    capacity: usize,
    expires: Instant,
}

impl PreparationDemand {
    fn idle_retention_targets(
        &self,
        preparation_running: bool,
        activation_generations: &BTreeSet<usize>,
    ) -> BTreeMap<GeneratedPoolIdentity, usize> {
        let selected_generation = self.selected_generation.upgrade();
        self.pools
            .iter()
            .filter_map(|(identity, registered)| {
                let generation = registered.generation.upgrade()?;
                if generation.is_retired() {
                    return None;
                }
                let selected = preparation_running
                    && selected_generation
                        .as_ref()
                        .is_some_and(|selected| Arc::ptr_eq(selected, &generation));
                let activating =
                    activation_generations.contains(&(Arc::as_ptr(&generation) as usize));
                if !selected && !activating {
                    return None;
                }
                let target = if selected {
                    registered
                        .target()
                        .saturating_sub(registered.checked_out.load(Ordering::Acquire))
                } else {
                    0
                };
                // Serial readiness must retain earlier baselines until later
                // groups finish, even when that takes longer than the idle age.
                // Only a live activation flight owns this age protection.
                Some((identity.clone(), target.max(usize::from(activating))))
            })
            .collect()
    }
}

impl RegisteredDemand {
    fn observe_saturation(&mut self) {
        self.saturation = Some(SaturationDemand {
            capacity: self.checked_out.load(Ordering::Acquire).saturating_add(1),
            expires: Instant::now() + DEMAND_WINDOW,
        });
    }

    fn target(&self) -> usize {
        let observed = self.demand.lock().target();
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
    previous_peak: usize,
    window_started: Instant,
    // Estimates retain their source observations' expiry and never become
    // observations themselves when another partition inherits demand.
    inherited: Option<(usize, Instant)>,
}

impl GroupDemand {
    fn advance(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.window_started);
        if elapsed >= DEMAND_WINDOW {
            self.previous_peak = if elapsed < DEMAND_WINDOW * 2 {
                self.peak
            } else {
                0
            };
            self.peak = self.active;
            self.window_started = now;
        }
    }

    fn target(&mut self) -> usize {
        self.advance(Instant::now());
        let inherited = self
            .inherited
            .filter(|(_, expires)| *expires > Instant::now())
            .map_or(0, |(peak, _)| peak);
        self.peak.max(self.previous_peak).max(inherited).max(1)
    }
}

pub(super) struct DemandObservation {
    demand: Arc<parking_lot::Mutex<GroupDemand>>,
    checked_out: Arc<AtomicUsize>,
}

impl Drop for DemandObservation {
    fn drop(&mut self) {
        let previous = self.checked_out.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "Wasm checked-out accounting underflow");
        let mut demand = self.demand.lock();
        // This may be the first observation after an inactive generation's
        // window expires. Count the still-held Store before releasing it.
        demand.advance(Instant::now());
        demand.active = demand
            .active
            .checked_sub(1)
            .expect("Wasm demand accounting underflow");
    }
}

pub(super) fn observe_demand(routed: &GeneratedRoutedModule) -> Option<DemandObservation> {
    let (demand, checked_out) = DEMAND
        .lock()
        .pools
        .get(&routed.pool_identity)
        .filter(|registered| {
            routed.generation.as_ref().is_some_and(|generation| {
                std::ptr::eq(registered.generation.as_ptr(), Arc::as_ptr(generation))
            })
        })
        .map(|registered| {
            (
                Arc::clone(&registered.demand),
                Arc::clone(&registered.checked_out),
            )
        })?;
    checked_out.fetch_add(1, Ordering::AcqRel);
    {
        let mut state = demand.lock();
        state.advance(Instant::now());
        state.active = state
            .active
            .checked_add(1)
            .expect("Wasm demand accounting overflow");
        state.peak = state.peak.max(state.active);
    }
    Some(DemandObservation {
        demand,
        checked_out,
    })
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

pub(super) fn idle_retention_targets() -> BTreeMap<GeneratedPoolIdentity, usize> {
    // Keep the flight map locked through lookup: its owner retains the exact
    // incarnation until removal, so a pointer key cannot be reused meanwhile.
    let flights = PREPARATION_FLIGHTS.lock();
    let activation_generations = flights
        .iter()
        .filter(|(_, flight)| flight.mode == PreparationMode::Activation)
        .map(|(key, _)| key.generation)
        .collect();
    DEMAND.lock().idle_retention_targets(
        PREPARATION_STARTED.load(Ordering::Acquire),
        &activation_generations,
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
        && (mode == PreparationMode::Activation
            || std::ptr::eq(DEMAND.lock().selected_generation.as_ptr(), generation))
}

fn register_demand(
    demands: &mut PreparationDemand,
    generation: &Arc<DeploymentGeneration>,
    pool_identity: &GeneratedPoolIdentity,
    members: BTreeSet<String>,
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
    demands.history.retain(|members, demand| {
        let mut demand = demand.lock();
        demand.advance(now);
        live_members.contains(members)
            || demand.active > 0
            || demand.peak > 0
            || demand.previous_peak > 0
            || demand.inherited.is_some_and(|(_, expires)| expires > now)
    });
    let members = members.into_iter().collect::<Vec<_>>();
    let history_key = (generation.generation_sha256.clone(), members.clone());
    let mut by_generation = BTreeMap::<String, (usize, Instant)>::new();
    for ((observed_generation, previous_members), demand) in &demands.history {
        if !previous_members
            .iter()
            .any(|member| members.binary_search(member).is_ok())
        {
            continue;
        }
        let mut demand = demand.lock();
        demand.advance(now);
        let observed_peak = demand.peak.max(demand.previous_peak);
        if observed_peak == 0 {
            continue;
        }
        let expires = demand.window_started
            + if demand.peak >= demand.previous_peak {
                DEMAND_WINDOW * 2
            } else {
                DEMAND_WINDOW
            };
        let estimate = by_generation
            .entry(observed_generation.clone())
            .or_insert((0, expires));
        estimate.0 = estimate.0.saturating_add(observed_peak);
        estimate.1 = estimate.1.min(expires);
    }
    let inherited = by_generation.into_values().max_by_key(|(peak, _)| *peak);
    // Sum original observations only within one prior partition, and take the
    // maximum across generations. An inherited estimate never becomes a new
    // observation, including when its source generation has been retired.
    let demand = Arc::clone(demands.history.entry(history_key).or_insert_with(|| {
        Arc::new(parking_lot::Mutex::new(GroupDemand {
            active: 0,
            peak: 0,
            previous_peak: 0,
            window_started: now,
            inherited,
        }))
    }));
    // Staging can precede the predecessor's latest peak. Refresh only the
    // estimate on maintenance; preserve live observations and checkout owners.
    demand.lock().inherited = inherited;
    if let Some(registered) = demands.pools.get(pool_identity)
        && registered
            .generation
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, generation))
    {
        assert!(Arc::ptr_eq(&registered.demand, &demand));
        return;
    }
    demands.pools.insert(
        pool_identity.clone(),
        RegisteredDemand {
            generation: Arc::downgrade(generation),
            members,
            demand: Arc::clone(&demand),
            checked_out: Arc::new(AtomicUsize::new(0)),
            saturation: None,
        },
    );
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
                PREPARATION_LOCK
                    .lock()
                    .retain(|prepared| !prepared.generation.is_retired());
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
                        Ok(GenerationPreparation::CapacityUnavailable) => {},
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
    Maintenance,
}

pub(super) fn release_generation(generation: &Arc<DeploymentGeneration>) {
    PREPARATION_LOCK
        .lock()
        .retain(|prepared| !Arc::ptr_eq(&prepared.generation, generation));
}

#[derive(Clone)]
pub(super) enum GenerationPreparation {
    Prepared(Arc<PreparedInventory>),
    CapacityUnavailable,
}

pub(super) struct PreparedInventory {
    pub(super) generation: Weak<DeploymentGeneration>,
    pub(super) groups: Vec<PreparedGroupInventory>,
}

pub(super) struct PreparedGroupInventory {
    pub(super) pool_identity: GeneratedPoolIdentity,
    pub(super) instances: Vec<(GeneratedSlotId, u64)>,
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
            return Ok(GenerationPreparation::CapacityUnavailable);
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
                let task = tokio::spawn(async move {
                    let owner = PreparationFlightOwner(task_generation);
                    let result = prepare_generation_inner(
                        task_rt,
                        task_database,
                        task_env,
                        Arc::clone(&owner.0),
                        mode,
                        batch_deadline,
                    )
                    .await;
                    if mode == PreparationMode::Activation {
                        // A batch may leave a valid partial baseline for its
                        // next bounded request. Start its ordinary idle grace
                        // when this owner finishes, not when the first group
                        // was built; otherwise age eviction in the retry gap
                        // can undo every batch's progress.
                        let mut refreshed = BTreeSet::new();
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
                                && refreshed.insert(Arc::clone(&instance.pool_identity))
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
                let flight = Arc::new(PreparationFlight { mode, result });
                flights.insert(key, Arc::clone(&flight));
                flight
            }
        };
        let result = tokio::time::timeout_at(batch_deadline, flight.result.clone()).await;
        let Ok(result) = result else {
            // The detached producer retains a running initializer and teardown.
            // Keep its discoverable flight until that owner actually finishes.
            return Ok(GenerationPreparation::CapacityUnavailable);
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
        if (mode == PreparationMode::Activation && flight.mode == PreparationMode::Maintenance)
            || (mode == PreparationMode::Maintenance
                && flight.mode == PreparationMode::Activation
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
) -> anyhow::Result<GenerationPreparation> {
    if !preparation_is_current(&generation, mode) {
        return Ok(GenerationPreparation::CapacityUnavailable);
    }
    let cached_groups = {
        let mut prepared_generations = PREPARATION_LOCK.lock();
        prepared_generations.retain(|prepared| !prepared.generation.is_retired());
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
                    || tokio::time::Instant::now() >= batch_deadline
                {
                    return Ok(None);
                }
                let mut groups = BTreeMap::new();
                for route in selected_generation_routes(&load_generation)? {
                    if !preparation_is_current(&load_generation, mode)
                        || tokio::time::Instant::now() >= batch_deadline
                    {
                        return Ok(None);
                    }
                    let routed = generated_deployment_routed_module(&route)?;
                    let group = groups
                        .entry(routed.pool_identity.clone())
                        .or_insert_with(|| PreparationGroup {
                            routed,
                            members: BTreeSet::new(),
                            memory_cost: Arc::new(parking_lot::Mutex::new(None)),
                            retry_at: Arc::new(parking_lot::Mutex::new(None)),
                            last_started: Arc::new(parking_lot::Mutex::new(None)),
                        });
                    group.members.insert(route.runtime_module_path);
                }
                Ok::<_, anyhow::Error>(Some(groups.into_values().collect::<Vec<_>>()))
            });
            // Dropping a blocking JoinHandle does not cancel its work. Keep
            // the shared flight until route discovery exits so timed-out callers
            // cannot queue duplicate discovery jobs for this incarnation.
            // Their own wait remains bounded by prepare_generation's deadline.
            let Some(groups) = loading.await?? else {
                return Ok(GenerationPreparation::CapacityUnavailable);
            };
            let mut prepared_generations = PREPARATION_LOCK.lock();
            if let Some(prepared) = prepared_generations
                .iter()
                .find(|prepared| Arc::ptr_eq(&prepared.generation, &generation))
            {
                prepared.groups.clone()
            } else {
                if !preparation_is_current(&generation, mode) {
                    return Ok(GenerationPreparation::CapacityUnavailable);
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
        if validate_prepared_group::<RT>(&database, &generation, group, mode, batch_deadline)
            .await?
            == PreparationAdmission::CapacityUnavailable
        {
            return Ok(GenerationPreparation::CapacityUnavailable);
        }
    }
    for group in &groups {
        register_demand(
            &mut DEMAND.lock(),
            &generation,
            &group.routed.pool_identity,
            group.members.clone(),
        );
    }
    let desired = if mode == PreparationMode::Activation {
        // Publication needs the baseline. Optional growth belongs to the
        // selected maintenance owner and cannot delay or fail readiness.
        vec![1; groups.len()]
    } else {
        let demands = DEMAND.lock();
        groups
            .iter()
            .map(|group| demands.pools[&group.routed.pool_identity].target())
            .collect::<Vec<_>>()
    };
    let checked_out = {
        let demands = DEMAND.lock();
        groups
            .iter()
            .map(|group| {
                demands.pools[&group.routed.pool_identity]
                    .checked_out
                    .load(Ordering::Acquire)
            })
            .collect::<Vec<_>>()
    };
    let controller = &route_configuration()?.generated_memory_controller;
    let active_capacity = match mode {
        PreparationMode::Maintenance => checked_out.clone(),
        PreparationMode::Activation => vec![0; groups.len()],
    };
    // One shared allocation includes checked-out capacity for maintenance.
    // Activation instead needs idle capacity available at publication.
    let mut total_targets = active_capacity
        .iter()
        .map(|active| (*active).max(1))
        .collect::<Vec<_>>();
    let baseline_targets = groups
        .iter()
        .zip(&active_capacity)
        .map(|(group, active)| {
            (
                group.routed.pool_identity.clone(),
                1usize.saturating_sub(*active),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let capacity = loop {
        let changed = controller.change_notification();
        tokio::pin!(changed);
        let _ = changed.as_mut().enable();
        let returned = GENERATED_GENERATION_RETIREMENT_NOTIFY.notified();
        tokio::pin!(returned);
        let _ = returned.as_mut().enable();
        let own_idle = GENERATED_ROUTED_INSTANCE_POOL
            .lock()
            .iter()
            .filter_map(|instance| instance.downcast_ref::<GeneratedReusableInstance<RT>>())
            .filter(|instance| {
                instance
                    .routed
                    .generation
                    .as_ref()
                    .is_some_and(|owner| Arc::ptr_eq(owner, &generation))
            })
            .count();
        let capacity = controller.preparation_instance_capacity(
            own_idle.saturating_add(active_capacity.iter().sum::<usize>()),
        );
        if total_targets.iter().sum::<usize>() > capacity.limit {
            return Ok(GenerationPreparation::CapacityUnavailable);
        }
        if total_targets.iter().sum::<usize>() <= capacity.available {
            break capacity.available;
        }
        if !preparation_is_current(&generation, mode)
            || tokio::time::Instant::now() >= batch_deadline
        {
            return Ok(GenerationPreparation::CapacityUnavailable);
        }
        if reclaim_preparation_surplus::<RT>(controller, &generation, mode, &baseline_targets, None)
            .await?
        {
            continue;
        }
        if mode == PreparationMode::Maintenance
            || wait_for_preparation(&generation, mode, batch_deadline, async {
                tokio::select! { _ = &mut changed => {}, _ = &mut returned => {} }
                Ok(())
            })
            .await?
            .is_none()
        {
            return Ok(GenerationPreparation::CapacityUnavailable);
        }
    };
    let mut remaining = capacity - total_targets.iter().sum::<usize>();
    while remaining > 0 {
        let mut allocated = false;
        for (target, desired) in total_targets.iter_mut().zip(&desired) {
            if *target < *desired && remaining > 0 {
                *target += 1;
                remaining -= 1;
                allocated = true;
            }
        }
        if !allocated {
            break;
        }
    }
    let targets = total_targets
        .iter()
        .zip(&checked_out)
        .map(|(target, active)| match mode {
            PreparationMode::Activation => *target,
            PreparationMode::Maintenance => target.saturating_sub(*active),
        })
        .collect::<Vec<_>>();
    let mut unavailable_groups = BTreeSet::new();
    // A failed baseline may be an ordering problem: already-retained state can
    // leave too little transient memory to initialize another group. Retry in
    // measured cost order. A selected maintenance owner may release its own
    // later idle baseline to restore a missing group. Candidate activation
    // never reclaims serving capacity; reserved Stores are absent from the pool.
    let mut baseline_order = match mode {
        PreparationMode::Activation => PreparationOrder::Memory,
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
                || tokio::time::Instant::now() >= batch_deadline
            {
                return Ok(GenerationPreparation::CapacityUnavailable);
            }
            if targets[index] == 0 {
                continue;
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
                1,
                mode,
                batch_deadline,
                &baseline_targets,
            )
            .await
            {
                Ok(PreparationAdmission::Prepared) => *group.retry_at.lock() = None,
                Ok(PreparationAdmission::CapacityUnavailable) => {
                    unavailable_groups.insert(group.routed.pool_identity.clone());
                    if mode == PreparationMode::Activation {
                        break;
                    }
                },
                Err(error) => {
                    *group.retry_at.lock() = Some(Instant::now() + GENERATED_FAILURE_LOG_INTERVAL);
                    if mode == PreparationMode::Activation {
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
            || tokio::time::Instant::now() >= batch_deadline
        {
            return Ok(GenerationPreparation::CapacityUnavailable);
        }
        let order = preparation_order(controller, &groups, PreparationOrder::Memory);
        let prepared_groups = GENERATED_ROUTED_INSTANCE_POOL
            .lock()
            .iter()
            .filter_map(|value| value.downcast_ref::<GeneratedReusableInstance<RT>>())
            .filter(|instance| {
                instance.all_entries_prepared
                    && instance
                        .routed
                        .generation
                        .as_ref()
                        .is_some_and(|owner| Arc::ptr_eq(owner, &generation))
            })
            .map(|instance| (*instance.pool_identity).clone())
            .collect::<BTreeSet<_>>();
        let mut reordered_targets = baseline_targets.clone();
        let Some(missing) = order.iter().position(|index| {
            targets[*index] > 0
                && (mode == PreparationMode::Activation || groups[*index].retry_at.lock().is_none())
                && !prepared_groups.contains(&groups[*index].routed.pool_identity)
        }) else {
            continue;
        };
        for index in &order[missing + 1..] {
            reordered_targets.insert(groups[*index].routed.pool_identity.clone(), 0);
        }
        if reclaim_preparation_surplus::<RT>(
            controller,
            &generation,
            mode,
            &reordered_targets,
            Some(&groups[order[missing]].routed),
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
        if mode == PreparationMode::Maintenance {
            // A missing large group does not prevent a smaller healthy group
            // from using the remaining capacity for its observed demand.
            break;
        }
        if wait_for_preparation(&generation, mode, batch_deadline, async {
            tokio::select! { _ = &mut changed => {}, _ = &mut returned => {} }
            Ok(())
        })
        .await?
        .is_none()
        {
            return Ok(GenerationPreparation::CapacityUnavailable);
        }
    }
    // Grow physical capacity one level at a time as well as allocating count
    // targets that way. Protect only this round so a previously overallocated
    // group can supply the bytes another group needs to reach the same level.
    for level in 2..=targets.iter().copied().max().unwrap_or(0) {
        let retention_targets = groups
            .iter()
            .zip(&targets)
            .map(|(group, target)| (group.routed.pool_identity.clone(), (*target).min(level)))
            .collect::<BTreeMap<_, _>>();
        for index in preparation_order(controller, &groups, PreparationOrder::LeastRecentlyStarted)
        {
            let group = &groups[index];
            let target = targets[index];
            if !preparation_is_current(&generation, mode)
                || tokio::time::Instant::now() >= batch_deadline
            {
                return Ok(GenerationPreparation::CapacityUnavailable);
            }
            if target < level || unavailable_groups.contains(&group.routed.pool_identity) {
                continue;
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
                level,
                mode,
                batch_deadline,
                &retention_targets,
            )
            .await
            {
                Ok(PreparationAdmission::Prepared) => *group.retry_at.lock() = None,
                Ok(PreparationAdmission::CapacityUnavailable) => {
                    unavailable_groups.insert(group.routed.pool_identity.clone());
                },
                Err(error) => {
                    *group.retry_at.lock() = Some(Instant::now() + GENERATED_FAILURE_LOG_INTERVAL);
                    if mode == PreparationMode::Activation {
                        return Err(error);
                    }
                    log_generated_failure(GeneratedFailureLog::RuntimePreparation);
                },
            }
        }
    }
    if mode == PreparationMode::Maintenance {
        // Retry failed initializers after healthy baselines and replicas. Keeping
        // expired failures in this separate pass prevents a slow failing prefix
        // from consuming every batch deadline before healthy groups can grow.
        let now = Instant::now();
        let mut retries = groups
            .iter()
            .enumerate()
            .filter_map(|(index, group)| {
                group
                    .retry_at
                    .lock()
                    .filter(|retry_at| *retry_at <= now && targets[index] > 0)
                    .map(|retry_at| (retry_at, group))
            })
            .collect::<Vec<_>>();
        retries.sort_by_key(|(retry_at, _)| *retry_at);
        for (_, group) in retries {
            if !preparation_is_current(&generation, mode)
                || tokio::time::Instant::now() >= batch_deadline
            {
                return Ok(GenerationPreparation::CapacityUnavailable);
            }
            match prepare_group(
                rt.clone(),
                database.clone(),
                default_system_env_vars.clone(),
                &generation,
                group,
                1,
                mode,
                batch_deadline,
                &baseline_targets,
            )
            .await
            {
                Ok(PreparationAdmission::Prepared) => *group.retry_at.lock() = None,
                Ok(PreparationAdmission::CapacityUnavailable) => {
                    // Capacity recovery may need the ordinary baseline reorder
                    // pass; keeping this group in the initializer-only tail
                    // would protect the very idle Stores it needs to replace.
                    *group.retry_at.lock() = None;
                },
                Err(_) => {
                    log_generated_failure(GeneratedFailureLog::RuntimePreparation);
                    *group.retry_at.lock() = Some(Instant::now() + GENERATED_FAILURE_LOG_INTERVAL);
                },
            }
        }
    }
    if !preparation_is_current(&generation, mode) || tokio::time::Instant::now() >= batch_deadline {
        return Ok(GenerationPreparation::CapacityUnavailable);
    }
    let pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
    let mut inventory = Vec::with_capacity(groups.len());
    for (group, target) in groups.iter().zip(&targets) {
        let instances = pool
            .iter()
            .filter_map(|instance| instance.downcast_ref::<GeneratedReusableInstance<RT>>())
            .filter(|instance| {
                instance.all_entries_prepared
                    && instance
                        .routed
                        .has_same_generation_incarnation(&group.routed)
                    && instance.pool_identity.as_ref() == &group.routed.pool_identity
            })
            .take(*target)
            .map(|instance| (instance.memory_slot_id, instance.id))
            .collect::<Vec<_>>();
        if mode == PreparationMode::Activation && instances.is_empty() {
            return Ok(GenerationPreparation::CapacityUnavailable);
        }
        inventory.push(PreparedGroupInventory {
            pool_identity: group.routed.pool_identity.clone(),
            instances,
        });
    }
    // Receipts identify an exact inventory without keeping a retired graph's
    // compiled modules alive. Reservation later upgrades and pins that owner.
    Ok(GenerationPreparation::Prepared(Arc::new(
        PreparedInventory {
            generation: Arc::downgrade(&generation),
            groups: inventory,
        },
    )))
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
        let cost = group.memory_cost.lock().and_then(|observed| {
            let routed = &group.routed;
            controller.estimated_preparation_memory_cost(
                &generated_memory_identity_for_package(
                    &routed.manifest,
                    &routed.package_identity,
                    &routed.route_identity,
                ),
                observed,
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

async fn prepare_group<RT: Runtime>(
    rt: RT,
    database: Database<RT>,
    default_system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    generation: &Arc<DeploymentGeneration>,
    group: &PreparationGroup,
    target: usize,
    mode: PreparationMode,
    batch_deadline: tokio::time::Instant,
    retention_targets: &BTreeMap<GeneratedPoolIdentity, usize>,
) -> anyhow::Result<PreparationAdmission> {
    let routed = &group.routed;
    let prepared = GENERATED_ROUTED_INSTANCE_POOL
        .lock()
        .iter()
        .filter_map(|instance| instance.downcast_ref::<GeneratedReusableInstance<RT>>())
        .filter(|instance| {
            instance.all_entries_prepared
                && instance.pool_identity.as_ref() == &routed.pool_identity
                && instance.routed.has_same_generation_incarnation(routed)
        })
        .count();
    for _ in prepared..target {
        if !preparation_is_current(&generation, mode)
            || tokio::time::Instant::now() >= batch_deadline
        {
            return Ok(PreparationAdmission::CapacityUnavailable);
        }
        let Some(transaction) =
            wait_for_preparation(generation, mode, batch_deadline, database.begin_system()).await?
        else {
            return Ok(PreparationAdmission::CapacityUnavailable);
        };
        match prepare_fresh_instance(
            rt.clone(),
            group,
            transaction,
            default_system_env_vars.clone(),
            mode,
            batch_deadline,
            retention_targets,
        )
        .await?
        {
            PreparationAdmission::Prepared => {},
            PreparationAdmission::CapacityUnavailable => {
                return Ok(PreparationAdmission::CapacityUnavailable);
            },
        }
    }
    Ok(PreparationAdmission::Prepared)
}

async fn reclaim_preparation_surplus<RT: Runtime>(
    controller: &GeneratedMemoryController,
    generation: &Arc<DeploymentGeneration>,
    mode: PreparationMode,
    targets: &BTreeMap<GeneratedPoolIdentity, usize>,
    pending: Option<&GeneratedRoutedModule>,
) -> anyhow::Result<bool> {
    let evicted = {
        let mut pool = GENERATED_ROUTED_INSTANCE_POOL.lock();
        let demands = DEMAND.lock();
        let selected = demands.selected_generation.upgrade();
        let mut idle = BTreeMap::<GeneratedPoolIdentity, usize>::new();
        for instance in pool
            .iter()
            .filter_map(|value| value.downcast_ref::<GeneratedReusableInstance<RT>>())
        {
            *idle.entry((*instance.pool_identity).clone()).or_default() += 1;
        }
        let mut ordered = pool
            .iter()
            .filter_map(|value| value.downcast_ref::<GeneratedReusableInstance<RT>>())
            .collect::<Vec<_>>();
        ordered.sort_by_key(|instance| instance.idle_since);
        let candidates = ordered
            .into_iter()
            .filter(|instance| {
                let owner = instance.routed.generation.as_ref();
                let same_generation = owner.is_some_and(|owner| Arc::ptr_eq(owner, generation));
                let serving = owner.is_some_and(|owner| {
                    selected
                        .as_ref()
                        .is_some_and(|selected| Arc::ptr_eq(owner, selected))
                });
                if serving && mode == PreparationMode::Activation {
                    return false;
                }
                if same_generation {
                    let remaining = idle
                        .get_mut(instance.pool_identity.as_ref())
                        .expect("idle group disappeared");
                    if *remaining > targets[instance.pool_identity.as_ref()] {
                        *remaining -= 1;
                        true
                    } else {
                        false
                    }
                } else {
                    !serving
                }
            })
            .map(|instance| (instance.memory_slot_id, instance.idle_since.elapsed()))
            .collect::<Vec<_>>();
        // Reallocation is independent of the global age-retention floor.
        // The supplied candidates already exclude useful serving capacity.
        let evictions = if let Some(pending) = pending {
            let identity = generated_memory_identity_for_package(
                &pending.manifest,
                &pending.package_identity,
                &pending.route_identity,
            );
            controller.preparation_eviction_candidates(&identity, &candidates)
        } else {
            controller.idle_eviction_candidates(
                &BTreeSet::new(),
                &candidates,
                IdleEvictionTrigger::AdmissionBudget,
            )
        };
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
) -> anyhow::Result<PreparationAdmission> {
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
    let (permit, memory_permit) = loop {
        // Join the ordinary CPU queue so sustained traffic cannot starve
        // replacements. Never hold CPU while waiting for memory admission.
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
                if error
                    .downcast_ref::<StaticHermesWasmActiveCpuCapacityUnavailable>()
                    .is_some() =>
            {
                return Ok(PreparationAdmission::CapacityUnavailable);
            },
            Err(error) => return Err(error),
        };
        let rejection = match controller.try_admit_preparation(memory_identity.clone()) {
            Ok(memory_permit) => break (permit, memory_permit),
            Err(TryAdmissionError::Permanent(AdmissionRejection::ForecastExceedsHardBudget)) => {
                return Ok(PreparationAdmission::CapacityUnavailable);
            },
            Err(TryAdmissionError::Wait(reason)) => {
                drop(permit);
                reason
            },
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
            Some(&routed),
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
        state
            .generated
            .as_mut()
            .context("preparation state disappeared")?
            .memory_permit
            .take()
            .context("preparation memory permit disappeared")?
            .finish_unstarted(false);
        return Err(error);
    }
    // Successful but expensive replacements can consume every batch while
    // traffic discards them again. Persist this turn so the next maintenance
    // batch starts with groups it has not reached. Capacity-only waits do not
    // consume a turn, including a baseline for which we just reclaimed memory.
    *group.last_started.lock() = Some(Instant::now());
    let mut output = execute_generated_with_lifecycle_barrier_and_entry_selector(
        Arc::clone(&routed),
        routed.entry_selector,
        GeneratedExecutionPurpose::Preparation,
        state,
        None,
        true,
        None,
    )
    .await?;
    let error = if let Some(error) = output.system_error.take() {
        Some(error)
    } else if output.cancelled {
        Some(anyhow::anyhow!("Wasm preparation was cancelled"))
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
        let error =
            discard_optional_generated_instance(output.reusable_instance.take(), error).await;
        if let Some(permit) = output.memory_permit.take() {
            permit.finish(output.terminal_memory_outcome, false);
        }
        return Err(error);
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
    if !permit.reserve_idle_growth() {
        let discarded = discard_generated_instance(instance).await;
        permit.finish(output.terminal_memory_outcome, false);
        discarded?;
        return Ok(PreparationAdmission::CapacityUnavailable);
    }
    instance.pending_initialization_logs = output
        .invocation
        .preparation_log_lines
        .take()
        .expect("validated preparation initialization logs disappeared");
    return_generated_routed_instance(instance, permit, output.terminal_memory_outcome).await?;
    Ok(PreparationAdmission::Prepared)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            .try_admit_preparation(generated_memory_identity_for_package(
                &routed.manifest,
                &routed.package_identity,
                &routed.route_identity,
            ))
            .unwrap()
            .finish_unstarted(false);
        let mut now = Instant::now();
        let groups = (0..8)
            .map(|index| PreparationGroup {
                routed: Arc::clone(&routed),
                members: BTreeSet::from([format!("entry-{index}.js")]),
                memory_cost: Arc::new(parking_lot::Mutex::new(Some(PreparationMemoryCost {
                    peak_bytes: 4 * PAGE,
                    retained_bytes: PAGE,
                }))),
                retry_at: Arc::new(parking_lot::Mutex::new(None)),
                last_started: Arc::new(parking_lot::Mutex::new(Some(now))),
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
        let members = |names: &[&str]| names.iter().map(|name| (*name).to_owned()).collect();
        let mut demands = PreparationDemand::default();
        for (identity, entry, peak) in [(&first, "a.js", 2), (&second, "b.js", 5)] {
            register_demand(&mut demands, &generations[0], identity, members(&[entry]));
            demands.pools[identity].demand.lock().peak = peak;
        }
        register_demand(
            &mut demands,
            &generations[1],
            &merged,
            members(&["a.js", "b.js"]),
        );
        assert_eq!(demands.pools[&merged].target(), 7);
        let checked_out = Arc::clone(&demands.pools[&merged].checked_out);
        demands.pools[&second].demand.lock().peak = 9;
        register_demand(
            &mut demands,
            &generations[1],
            &merged,
            members(&["a.js", "b.js"]),
        );
        assert_eq!(demands.pools[&merged].target(), 11);
        assert!(Arc::ptr_eq(
            &checked_out,
            &demands.pools[&merged].checked_out
        ));
        assert_eq!(demands.pools[&merged].demand.lock().peak, 0);
        // The merged estimate contributes no new observation to either sibling,
        // including the sibling registered after another inherited estimate.
        for (identity, entry, expected) in [(&split_second, "b.js", 9), (&split_first, "a.js", 2)] {
            register_demand(&mut demands, &generations[2], identity, members(&[entry]));
            assert_eq!(demands.pools[identity].target(), expected);
        }
        for identity in [&first, &second] {
            demands.pools[identity].demand.lock().window_started =
                Instant::now() - DEMAND_WINDOW * 3;
        }
        register_demand(
            &mut demands,
            &generations[1],
            &merged,
            members(&["a.js", "b.js"]),
        );
        assert_eq!(demands.pools[&merged].target(), 1);
    }

    #[test]
    fn saturation_survives_a_short_burst_without_counting_rejected_requests() {
        let demand = Arc::new(parking_lot::Mutex::new(GroupDemand {
            active: 1,
            peak: 1,
            previous_peak: 0,
            window_started: Instant::now(),
            inherited: None,
        }));
        let checked_out = Arc::new(AtomicUsize::new(1));
        let observation = DemandObservation {
            demand: Arc::clone(&demand),
            checked_out: Arc::clone(&checked_out),
        };
        let mut registered = RegisteredDemand {
            generation: Weak::new(),
            members: vec!["entry.js".to_owned()],
            demand: Arc::clone(&demand),
            checked_out,
            saturation: None,
        };
        for _ in 0..100 {
            registered.observe_saturation();
        }
        drop(observation);
        // The only admitted call finishes before maintenance samples demand.
        assert_eq!(registered.target(), 2);
        assert_eq!(demand.lock().target(), 1);
        registered.saturation.as_mut().unwrap().expires = Instant::now();
        assert_eq!(registered.target(), 1);
    }

    #[test]
    fn completion_preserves_demand_that_spans_an_unsampled_window() {
        let demand = Arc::new(parking_lot::Mutex::new(GroupDemand {
            active: 3,
            peak: 3,
            previous_peak: 0,
            window_started: Instant::now() - DEMAND_WINDOW * 3,
            inherited: None,
        }));
        drop(DemandObservation {
            demand: Arc::clone(&demand),
            checked_out: Arc::new(AtomicUsize::new(3)),
        });
        let mut demand = demand.lock();
        assert_eq!(demand.active, 2);
        assert_eq!(demand.target(), 3);
    }

    #[test]
    fn activation_protects_only_its_live_baseline_from_idle_age() {
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
            members: vec!["entry.js".to_owned()],
            demand: Arc::new(parking_lot::Mutex::new(GroupDemand {
                active: 1,
                peak: 4,
                previous_peak: 0,
                window_started: Instant::now(),
                inherited: None,
            })),
            checked_out: Arc::new(AtomicUsize::new(1)),
            saturation: None,
        };
        let demands = PreparationDemand {
            history: BTreeMap::new(),
            pools: BTreeMap::from([
                (selected_pool.clone(), registered(&selected)),
                (candidate_pool.clone(), registered(&candidate)),
            ]),
            selected_generation: Arc::downgrade(&selected),
        };
        let activating = BTreeSet::from([Arc::as_ptr(&candidate) as usize]);
        assert_eq!(
            demands.idle_retention_targets(true, &activating),
            BTreeMap::from([(selected_pool.clone(), 3), (candidate_pool.clone(), 1)])
        );
        // A disconnected waiter may leave its bounded producer running. Once
        // that producer finishes, a staged generation has no age-retention owner.
        assert_eq!(
            demands.idle_retention_targets(true, &BTreeSet::new()),
            BTreeMap::from([(selected_pool, 3)])
        );
        // Explicit activation still owns progress if the replenishment service
        // has stopped; its baseline must not depend on that service's lease.
        assert_eq!(
            demands.idle_retention_targets(false, &activating),
            BTreeMap::from([(candidate_pool, 1)])
        );
        candidate.retired.store(true, Ordering::Release);
        assert!(demands
            .idle_retention_targets(false, &activating)
            .is_empty());
    }
}
