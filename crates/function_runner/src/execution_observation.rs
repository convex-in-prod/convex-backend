use std::{
    sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Arc,
    },
    time::{
        Duration,
        Instant,
        SystemTime,
        UNIX_EPOCH,
    },
};

use common::types::UdfType;
use isolate::execution_observation::ExecutionObserver;
use parking_lot::Mutex;
use rand::Rng;
use serde::Serialize;
use udf::execution_observation::PairedExecutionObservation;

use crate::QueryShadowRouteKey;

const MAX_ROUTES: usize = 8;
const REPRESENTATIVE_RECORDS: usize = 16;
const SLOW_RECORDS: usize = 8;

pub struct ExecutionPairObservers {
    pub(crate) selection: ExecutionSelection,
    pub(crate) primary: ExecutionObserver,
    pub(crate) wasm: ExecutionObserver,
    pub(crate) primary_transaction_duration: std::time::Duration,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) struct ExecutionPairTasks {
    pub(crate) primary: isolate::execution_observation::ObservedTask,
    pub(crate) wasm: isolate::execution_observation::ObservedTask,
}

pub(crate) struct ExecutionComparison {
    pub(crate) matches: bool,
    pub(crate) primary_duration: std::time::Duration,
    pub(crate) wasm_duration: std::time::Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ExecutionSessionState {
    Collecting,
    BudgetExhausted,
    Expired,
    Stopped,
    GenerationChanged,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionRecord {
    pub sequence: u32,
    pub started_at_unix_milliseconds: u64,
    pub finished_at_unix_milliseconds: u64,
    pub observation: PairedExecutionObservation,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionRouteRecords {
    pub udf_type: UdfType,
    pub route_key: String,
    pub eligible: u64,
    pub selected: u32,
    pub completed: u32,
    pub abandoned: u32,
    pub in_flight: u32,
    pub without_comparison: u32,
    pub incomplete_accounting: u32,
    /// Records absent from both retained sets. The sets may overlap by
    /// sequence.
    pub not_retained: u32,
    pub representative: Vec<ExecutionRecord>,
    pub slowest: Vec<ExecutionRecord>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionSessionSnapshot {
    pub session_id: String,
    pub generation_sha256: String,
    pub state: ExecutionSessionState,
    pub started_at_unix_milliseconds: u64,
    pub expires_at_unix_milliseconds: u64,
    pub pairs_per_route: u32,
    pub sample_every: u32,
    pub representative_capacity_per_route: usize,
    pub slow_capacity_per_route: usize,
    pub routes: Vec<ExecutionRouteRecords>,
}

struct Session {
    snapshot: ExecutionSessionSnapshot,
    deadline: Instant,
}

impl Session {
    fn refresh(&mut self, generation: Option<&str>, now: Instant) {
        if generation != Some(self.snapshot.generation_sha256.as_str()) {
            self.snapshot.state = ExecutionSessionState::GenerationChanged;
        } else if self.snapshot.state == ExecutionSessionState::Collecting && now >= self.deadline {
            self.snapshot.state = ExecutionSessionState::Expired;
        }
    }
}

/// One bounded session per application log. Tickets keep the selected session
/// alive through finalization; replacing it cannot redirect an old completion.
#[derive(Default)]
pub struct ExecutionObservationController {
    enabled: AtomicBool,
    session: Mutex<Option<Arc<Mutex<Session>>>>,
}

impl ExecutionObservationController {
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Callers authenticate every route against the current runtime registry.
    pub fn arm(
        &self,
        routes: Vec<(UdfType, QueryShadowRouteKey)>,
        duration: Duration,
        pairs_per_route: u32,
        sample_every: u32,
        replace_session_id: Option<&str>,
    ) -> anyhow::Result<ExecutionSessionSnapshot> {
        anyhow::ensure!(
            (1..=MAX_ROUTES).contains(&routes.len()),
            "Select between 1 and 8 routes"
        );
        anyhow::ensure!(
            (Duration::from_secs(1)..=Duration::from_secs(1800)).contains(&duration),
            "Observation duration must be between 1 and 1800 seconds"
        );
        anyhow::ensure!(
            (1..=4096).contains(&pairs_per_route),
            "Pair budget must be between 1 and 4096 per route"
        );
        anyhow::ensure!(
            (1..=1024).contains(&sample_every),
            "Sampling interval must be between 1 and 1024"
        );
        let generation = routes[0].1.generation_sha256().to_owned();
        let mut selected_routes = Vec::with_capacity(routes.len());
        for (udf_type, route_key) in routes {
            anyhow::ensure!(
                matches!(udf_type, UdfType::Query | UdfType::Mutation),
                "Only query and mutation pairs can be observed"
            );
            anyhow::ensure!(
                route_key.generation_sha256() == generation,
                "All routes must belong to the active generation"
            );
            anyhow::ensure!(
                !selected_routes
                    .iter()
                    .any(|route: &ExecutionRouteRecords| route.udf_type == udf_type
                        && route.route_key == route_key.as_str()),
                "Duplicate observation route"
            );
            selected_routes.push(ExecutionRouteRecords {
                udf_type,
                route_key: route_key.as_str().to_owned(),
                eligible: 0,
                selected: 0,
                completed: 0,
                abandoned: 0,
                in_flight: 0,
                without_comparison: 0,
                incomplete_accounting: 0,
                not_retained: 0,
                representative: Vec::new(),
                slowest: Vec::new(),
            });
        }
        let mut current = self.session.lock();
        if let Some(previous) = current.as_ref() {
            let mut previous = previous.lock();
            previous.refresh(Some(&generation), Instant::now());
            anyhow::ensure!(
                replace_session_id == Some(previous.snapshot.session_id.as_str()),
                "Replacing retained observations requires the current session ID"
            );
            anyhow::ensure!(
                previous.snapshot.state != ExecutionSessionState::Collecting,
                "Stop the current observation session before replacing it"
            );
            anyhow::ensure!(
                previous
                    .snapshot
                    .routes
                    .iter()
                    .all(|route| route.in_flight == 0),
                "Wait for selected pairs to finish before replacing observations"
            );
        } else {
            anyhow::ensure!(
                replace_session_id.is_none(),
                "No observation session to replace"
            );
        }
        let started = unix_millis();
        let snapshot = ExecutionSessionSnapshot {
            session_id: format!("{:032x}", rand::random::<u128>()),
            generation_sha256: generation,
            state: ExecutionSessionState::Collecting,
            started_at_unix_milliseconds: started,
            expires_at_unix_milliseconds: started + duration.as_millis() as u64,
            pairs_per_route,
            sample_every,
            representative_capacity_per_route: REPRESENTATIVE_RECORDS,
            slow_capacity_per_route: SLOW_RECORDS,
            routes: selected_routes,
        };
        *current = Some(Arc::new(Mutex::new(Session {
            snapshot: snapshot.clone(),
            deadline: Instant::now() + duration,
        })));
        self.enabled.store(true, Ordering::Relaxed);
        Ok(snapshot)
    }

    pub fn snapshot(&self, generation: Option<&str>) -> Option<ExecutionSessionSnapshot> {
        let current = self.session.lock();
        let mut session = current.as_ref()?.lock();
        session.refresh(generation, Instant::now());
        self.enabled.store(
            session.snapshot.state == ExecutionSessionState::Collecting,
            Ordering::Relaxed,
        );
        Some(session.snapshot.clone())
    }

    pub fn stop(&self, session_id: &str) -> anyhow::Result<ExecutionSessionSnapshot> {
        let current = self.session.lock();
        let mut session = current
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No observation session"))?
            .lock();
        anyhow::ensure!(
            session.snapshot.session_id == session_id,
            "Observation session ID changed"
        );
        if session.snapshot.state == ExecutionSessionState::Collecting {
            session.snapshot.state = ExecutionSessionState::Stopped;
        }
        self.enabled.store(false, Ordering::Relaxed);
        Ok(session.snapshot.clone())
    }

    pub fn try_observe(
        &self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        active_generation: Option<&str>,
    ) -> Option<ExecutionPairObservers> {
        if !self.is_enabled() {
            return None;
        }
        let current = self.session.lock();
        let selected_session = current.as_ref()?;
        let mut session = selected_session.lock();
        session.refresh(active_generation, Instant::now());
        let snapshot = &mut session.snapshot;
        if snapshot.state != ExecutionSessionState::Collecting {
            self.enabled.store(false, Ordering::Relaxed);
            return None;
        }
        let index = snapshot.routes.iter().position(|route| {
            route.udf_type == udf_type && route.route_key == route_key.as_str()
        })?;
        let route = &mut snapshot.routes[index];
        route.eligible += 1;
        if route.selected >= snapshot.pairs_per_route
            || (route.eligible - 1) % u64::from(snapshot.sample_every) != 0
        {
            return None;
        }
        route.selected += 1;
        route.in_flight += 1;
        let sequence = route.selected;
        if snapshot
            .routes
            .iter()
            .all(|route| route.selected == snapshot.pairs_per_route)
        {
            snapshot.state = ExecutionSessionState::BudgetExhausted;
            self.enabled.store(false, Ordering::Relaxed);
        }
        Some(ExecutionPairObservers {
            selection: ExecutionSelection {
                session: Arc::clone(selected_session),
                index,
                sequence,
                started_at_unix_milliseconds: unix_millis(),
                finished: false,
            },
            primary: ExecutionObserver::default(),
            wasm: ExecutionObserver::default(),
            primary_transaction_duration: Duration::ZERO,
        })
    }
}

pub(crate) struct ExecutionSelection {
    session: Arc<Mutex<Session>>,
    index: usize,
    sequence: u32,
    started_at_unix_milliseconds: u64,
    finished: bool,
}

impl ExecutionSelection {
    pub(crate) fn finish(mut self, observation: PairedExecutionObservation) {
        let mut session = self.session.lock();
        let route = &mut session.snapshot.routes[self.index];
        route.in_flight -= 1;
        route.completed += 1;
        route.without_comparison += u32::from(observation.comparison_matches.is_none());
        route.incomplete_accounting += u32::from(
            [&observation.primary, &observation.wasm]
                .iter()
                .any(|engine| {
                    engine.invalid.is_some()
                        || engine.active_tasks != 0
                        || engine.cancelled_tasks != 0
                }),
        );
        let record = ExecutionRecord {
            sequence: self.sequence,
            started_at_unix_milliseconds: self.started_at_unix_milliseconds,
            finished_at_unix_milliseconds: unix_millis(),
            observation,
        };
        // Algorithm R samples all completed observed pairs for this route,
        // including terminal outcomes. It does not estimate unsampled traffic.
        if route.representative.len() < REPRESENTATIVE_RECORDS {
            route.representative.push(record.clone());
        } else {
            let index = rand::rng().random_range(0..route.completed) as usize;
            if index < REPRESENTATIVE_RECORDS {
                route.representative[index] = record.clone();
            }
        }
        if record.observation.wasm_wall_nanos.is_some() {
            route.slowest.push(record);
            route.slowest.sort_unstable_by_key(|record| {
                std::cmp::Reverse(record.observation.wasm_wall_nanos)
            });
            route.slowest.truncate(SLOW_RECORDS);
        }
        let retained = route
            .representative
            .iter()
            .chain(&route.slowest)
            .map(|record| record.sequence)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        route.not_retained = route.completed - retained as u32;
        self.finished = true;
    }
}

impl Drop for ExecutionSelection {
    fn drop(&mut self) {
        if !self.finished {
            let mut session = self.session.lock();
            let route = &mut session.snapshot.routes[self.index];
            route.in_flight -= 1;
            route.abandoned += 1;
        }
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock precedes epoch")
        .as_millis()
        .try_into()
        .expect("system timestamp exceeds u64")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(digest: &str) -> QueryShadowRouteKey {
        QueryShadowRouteKey::new(&"a".repeat(64), &digest.repeat(64)).unwrap()
    }

    fn pair(wasm: u64) -> PairedExecutionObservation {
        PairedExecutionObservation {
            primary: Default::default(),
            wasm: Default::default(),
            comparison_matches: Some(true),
            primary_wall_nanos: Some(1),
            wasm_wall_nanos: Some(wasm),
            primary_prestarted_transaction_nanos: 0,
        }
    }

    #[test]
    fn sessions_bound_each_route_and_keep_slow_pairs_independently() {
        let controller = ExecutionObservationController::default();
        let query = route("b");
        let mutation = route("c");
        let generation = "a".repeat(64);
        let routes = vec![
            (UdfType::Query, query.clone()),
            (UdfType::Mutation, mutation.clone()),
        ];
        let session = controller
            .arm(routes.clone(), Duration::from_secs(60), 40, 2, None)
            .unwrap();
        for expected in 1..=40 {
            let observers = controller
                .try_observe(UdfType::Query, &query, Some(&generation))
                .unwrap();
            observers.selection.finish(pair(expected));
            assert!(controller
                .try_observe(UdfType::Query, &query, Some(&generation))
                .is_none());
        }
        // A busy route cannot consume the other route's budget.
        let pending = controller
            .try_observe(UdfType::Mutation, &mutation, Some(&generation))
            .unwrap();
        let snapshot = controller.stop(&session.session_id).unwrap();
        assert_eq!(
            snapshot.routes[0].representative.len(),
            REPRESENTATIVE_RECORDS
        );
        assert_eq!(snapshot.routes[0].slowest.len(), SLOW_RECORDS);
        assert_eq!(
            snapshot.routes[0]
                .slowest
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            (33..=40).rev().collect::<Vec<_>>()
        );
        assert_eq!(snapshot.routes[0].completed, 40);
        assert_eq!(snapshot.routes[1].in_flight, 1);
        assert!(!controller.is_enabled());
        assert!(controller
            .arm(
                routes.clone(),
                Duration::from_secs(60),
                1,
                1,
                Some(&session.session_id)
            )
            .is_err());
        drop(pending);
        let snapshot = controller.snapshot(Some(&generation)).unwrap();
        assert_eq!(snapshot.routes[1].abandoned, 1);
        assert_eq!(snapshot.routes[1].in_flight, 0);
        assert!(controller
            .arm(routes.clone(), Duration::from_secs(60), 1, 1, None)
            .is_err());
        let next = controller
            .arm(
                routes,
                Duration::from_secs(60),
                1,
                1,
                Some(&session.session_id),
            )
            .unwrap();
        assert_ne!(next.session_id, session.session_id);
        assert!(controller.stop(&session.session_id).is_err());
    }

    #[test]
    fn generation_and_expiry_stop_selection_but_preserve_in_flight_evidence() {
        let controller = ExecutionObservationController::default();
        let route = route("b");
        let generation = "a".repeat(64);
        let config = vec![(UdfType::Query, route.clone())];
        let session = controller
            .arm(config.clone(), Duration::from_secs(60), 1, 1, None)
            .unwrap();
        let observers = controller
            .try_observe(UdfType::Query, &route, Some(&generation))
            .unwrap();
        assert!(!controller.is_enabled());
        assert_eq!(
            controller.snapshot(Some(&generation)).unwrap().state,
            ExecutionSessionState::BudgetExhausted
        );
        assert_eq!(
            controller.snapshot(Some(&"d".repeat(64))).unwrap().state,
            ExecutionSessionState::GenerationChanged
        );
        observers.selection.finish(pair(10));
        let old = controller.snapshot(None).unwrap();
        assert_eq!(old.generation_sha256, generation);
        assert_eq!(old.routes[0].completed, 1);
        let next = controller
            .arm(
                config,
                Duration::from_secs(60),
                2,
                1,
                Some(&session.session_id),
            )
            .unwrap();
        controller.session.lock().as_ref().unwrap().lock().deadline = Instant::now();
        assert!(controller
            .try_observe(UdfType::Query, &route, Some(&generation))
            .is_none());
        assert_eq!(
            controller.snapshot(Some(&generation)).unwrap().state,
            ExecutionSessionState::Expired
        );
        assert_eq!(
            controller.stop(&next.session_id).unwrap().routes[0].selected,
            0
        );
    }
}
