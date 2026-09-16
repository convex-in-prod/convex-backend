use std::{
    collections::{
        HashMap,
        VecDeque,
    },
    sync::{
        Arc,
        LazyLock,
    },
    time::{
        Duration,
        Instant,
    },
};

#[cfg(all(test, feature = "static-hermes-wasmtime-gate"))]
use anyhow::Context;
use common::{
    knobs::{
        static_hermes_wasm_primary_directions,
        validate_static_hermes_verifier_directions,
        APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS,
        APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_V8_SHADOW_BPS,
        APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS,
        APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_V8_SHADOW_BPS,
        APPLICATION_STATIC_HERMES_SHADOW_CONCURRENCY,
        APPLICATION_STATIC_HERMES_SHADOW_MAX_ACTIVE_ROUTES,
        APPLICATION_STATIC_HERMES_SHADOW_TIMEOUT,
    },
    runtime::Runtime,
    types::UdfType,
};
use function_runner::{
    FunctionExecutionMode,
    QueryShadowAdmission,
    QueryShadowAdmissionEvent,
    QueryShadowDirection,
    QueryShadowObservationTracker,
    QueryShadowPermit,
    QueryShadowReport,
    QueryShadowReportSink,
    QueryShadowRouteKey,
};
use metrics::{
    log_counter_with_labels,
    register_convex_counter,
    StaticMetricLabel,
};
use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::Semaphore;

const QUERY_SHADOW_OBSERVATION_TTL: Duration = Duration::from_secs(60 * 60);
const STATIC_HERMES_WASM_GATE_ENABLED_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_ENABLED";

static STATIC_HERMES_WASM_GATE_ENABLED: LazyLock<bool> =
    LazyLock::new(
        || match std::env::var(STATIC_HERMES_WASM_GATE_ENABLED_ENV) {
            Ok(value) if value == "1" => true,
            Ok(value) if value == "0" => false,
            Ok(_) => panic!("{STATIC_HERMES_WASM_GATE_ENABLED_ENV} must be 0 or 1 when set"),
            Err(std::env::VarError::NotPresent) => false,
            Err(error) => panic!("failed to read {STATIC_HERMES_WASM_GATE_ENABLED_ENV}: {error}"),
        },
    );

register_convex_counter!(
    STATIC_HERMES_QUERY_SHADOW_ADMISSION_TOTAL,
    "Static Hermes shadow sampling and admission decisions",
    &["udf_type", "outcome"]
);

static SHADOW_PERMITS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    let concurrency = *APPLICATION_STATIC_HERMES_SHADOW_CONCURRENCY;
    assert!(
        concurrency > 0,
        "enabled Static Hermes shadow concurrency must be positive"
    );
    Arc::new(Semaphore::new(concurrency))
});

static SHADOW_OBSERVATIONS: LazyLock<Arc<Mutex<QueryShadowObservationCache>>> =
    LazyLock::new(|| {
        Arc::new(Mutex::new(QueryShadowObservationCache::new(
            *APPLICATION_STATIC_HERMES_SHADOW_MAX_ACTIVE_ROUTES,
            QUERY_SHADOW_OBSERVATION_TTL,
        )))
    });

fn validate_shadow_sampling_configuration(
    query_sampling_bps: u16,
    mutation_sampling_bps: u16,
    query_wasm_primary_v8_sampling_bps: u16,
    mutation_wasm_primary_v8_sampling_bps: u16,
    query_wasm_primary_enabled: bool,
    mutation_wasm_primary_enabled: bool,
) -> anyhow::Result<()> {
    #[cfg(not(feature = "static-hermes-wasmtime-gate"))]
    anyhow::ensure!(
        query_sampling_bps == 0
            && mutation_sampling_bps == 0
            && query_wasm_primary_v8_sampling_bps == 0
            && mutation_wasm_primary_v8_sampling_bps == 0,
        "Static Hermes shadow sampling requires static-hermes-wasmtime-gate; set all shadow BPS \
         knobs to 0 in a build without that feature"
    );

    validate_static_hermes_verifier_directions(
        query_sampling_bps,
        mutation_sampling_bps,
        query_wasm_primary_v8_sampling_bps,
        mutation_wasm_primary_v8_sampling_bps,
        query_wasm_primary_enabled,
        mutation_wasm_primary_enabled,
    )
}

#[derive(Clone)]
pub(crate) struct QueryShadowCoordinator {
    gate_enabled: bool,
    query_sampling_bps: u16,
    mutation_sampling_bps: u16,
    query_wasm_primary_enabled: bool,
    mutation_wasm_primary_enabled: bool,
    query_wasm_primary_v8_sampling_bps: u16,
    mutation_wasm_primary_v8_sampling_bps: u16,
    timeout: Duration,
    permits: Option<Arc<Semaphore>>,
    observations: Arc<Mutex<QueryShadowObservationCache>>,
    report_sink: Arc<dyn QueryShadowReportSink>,
}

impl QueryShadowCoordinator {
    #[cfg(all(test, feature = "static-hermes-wasmtime-gate"))]
    pub(super) fn detached_cleanup_test_permits(&self) -> anyhow::Result<Arc<Semaphore>> {
        anyhow::ensure!(
            self.mutation_sampling_bps == 10_000,
            "cleanup test requires every mutation to be sampled"
        );
        anyhow::ensure!(
            self.timeout >= Duration::from_secs(1) && self.timeout <= Duration::from_secs(30),
            "cleanup test requires a shadow timeout between one and thirty seconds"
        );
        let permits = self
            .permits
            .as_ref()
            .context("cleanup test requires shadow admission")?;
        anyhow::ensure!(
            permits.available_permits() == 1,
            "cleanup test requires exactly one free shadow permit"
        );
        Ok(Arc::clone(permits))
    }

    pub(crate) fn new(report_sink: Arc<dyn QueryShadowReportSink>) -> Self {
        let query_sampling_bps = *APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS;
        let mutation_sampling_bps = *APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS;
        let query_wasm_primary_v8_sampling_bps =
            *APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_V8_SHADOW_BPS;
        let mutation_wasm_primary_v8_sampling_bps =
            *APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_V8_SHADOW_BPS;
        let gate_enabled = *STATIC_HERMES_WASM_GATE_ENABLED;
        let (query_wasm_primary_enabled, mutation_wasm_primary_enabled) =
            static_hermes_wasm_primary_directions(gate_enabled)
                .expect("invalid per-UDF Wasm-primary routing configuration");
        validate_shadow_sampling_configuration(
            query_sampling_bps,
            mutation_sampling_bps,
            query_wasm_primary_v8_sampling_bps,
            mutation_wasm_primary_v8_sampling_bps,
            query_wasm_primary_enabled,
            mutation_wasm_primary_enabled,
        )
        .expect("invalid Static Hermes shadow sampling configuration");
        let timeout = *APPLICATION_STATIC_HERMES_SHADOW_TIMEOUT;
        let permits = if query_sampling_bps == 0
            && mutation_sampling_bps == 0
            && query_wasm_primary_v8_sampling_bps == 0
            && mutation_wasm_primary_v8_sampling_bps == 0
        {
            None
        } else {
            Some(Arc::clone(&SHADOW_PERMITS))
        };
        Self {
            gate_enabled,
            query_sampling_bps,
            mutation_sampling_bps,
            query_wasm_primary_enabled,
            mutation_wasm_primary_enabled,
            query_wasm_primary_v8_sampling_bps,
            mutation_wasm_primary_v8_sampling_bps,
            timeout,
            permits,
            observations: Arc::clone(&SHADOW_OBSERVATIONS),
            report_sink,
        }
    }

    /// Select an engine mode for a real query or mutation execution.
    ///
    /// Enabled coordinators defer admission until the function runner has
    /// resolved an authenticated registry-generation route. The configured
    /// direction determines primary authority, and every verifier attempt is
    /// sampled before capacity is reserved.
    pub(crate) fn execution_mode<RT: Runtime>(
        &self,
        rt: &RT,
        udf_type: UdfType,
    ) -> FunctionExecutionMode {
        self.configured_execution_mode_for_sample(udf_type, rt.rng().random_range(0..10_000u16))
    }

    fn configured_execution_mode_for_sample(
        &self,
        udf_type: UdfType,
        sample: u16,
    ) -> FunctionExecutionMode {
        let wasm_primary_enabled = match udf_type {
            UdfType::Query => self.query_wasm_primary_enabled,
            UdfType::Mutation => self.mutation_wasm_primary_enabled,
            UdfType::Action | UdfType::HttpAction => return FunctionExecutionMode::Configured,
        };
        if wasm_primary_enabled {
            return self.wasm_primary_execution_mode_for_sample(udf_type, sample);
        }
        let mode = self.execution_mode_for_sample(udf_type, sample);
        if self.gate_enabled && matches!(mode, FunctionExecutionMode::Configured) {
            // The process-wide gate may remain enabled for the other UDF type.
            // Pin this type to V8 even when its shadow sampler is disabled.
            FunctionExecutionMode::V8
        } else {
            mode
        }
    }

    fn execution_mode_for_sample(
        &self,
        udf_type: UdfType,
        sampled_basis_point: u16,
    ) -> FunctionExecutionMode {
        let sampling_bps = match udf_type {
            UdfType::Query => self.query_sampling_bps,
            UdfType::Mutation => self.mutation_sampling_bps,
            UdfType::Action | UdfType::HttpAction => return FunctionExecutionMode::Configured,
        };
        let Some(permits) = self.permits.as_ref().filter(|_| sampling_bps > 0) else {
            return FunctionExecutionMode::Configured;
        };
        assert!(sampled_basis_point < 10_000);
        if sampled_basis_point >= sampling_bps {
            // A sample miss must remain on the ordinary V8 path. In
            // particular, it must not resolve a configured Wasm route or
            // acquire the route-evidence lock merely to be rejected later.
            log_admission(udf_type, "not_sampled");
            return FunctionExecutionMode::V8;
        }
        FunctionExecutionMode::V8WithStaticHermesWasmShadow(Arc::new(QueryShadowAdmissionAttempt {
            sampling_bps,
            sampled_basis_point,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            timeout: self.timeout,
            permits: Arc::clone(permits),
            observations: Arc::clone(&self.observations),
            report_sink: Arc::clone(&self.report_sink),
        }))
    }

    /// Select a Wasm-primary execution with a non-blocking sampled V8
    /// verifier. A sample miss preserves configured routing: selected routes
    /// remain Wasm-primary, while unselected and quarantined routes remain V8.
    pub(crate) fn wasm_primary_execution_mode<RT: Runtime>(
        &self,
        rt: &RT,
        udf_type: UdfType,
    ) -> FunctionExecutionMode {
        self.wasm_primary_execution_mode_for_sample(udf_type, rt.rng().random_range(0..10_000u16))
    }

    fn wasm_primary_execution_mode_for_sample(
        &self,
        udf_type: UdfType,
        sampled_basis_point: u16,
    ) -> FunctionExecutionMode {
        let sampling_bps = match udf_type {
            UdfType::Query => self.query_wasm_primary_v8_sampling_bps,
            UdfType::Mutation => self.mutation_wasm_primary_v8_sampling_bps,
            UdfType::Action | UdfType::HttpAction => return FunctionExecutionMode::Configured,
        };
        let Some(permits) = self.permits.as_ref().filter(|_| sampling_bps > 0) else {
            return FunctionExecutionMode::Configured;
        };
        assert!(sampled_basis_point < 10_000);
        if sampled_basis_point >= sampling_bps {
            log_admission(udf_type, "wasm_primary_not_sampled");
            return FunctionExecutionMode::Configured;
        }
        FunctionExecutionMode::StaticHermesWasmWithV8Shadow(Arc::new(QueryShadowAdmissionAttempt {
            sampling_bps,
            sampled_basis_point,
            direction: QueryShadowDirection::WasmPrimaryV8Shadow,
            timeout: self.timeout,
            permits: Arc::clone(permits),
            observations: Arc::clone(&self.observations),
            report_sink: Arc::clone(&self.report_sink),
        }))
    }
}

struct QueryShadowAdmissionAttempt {
    sampling_bps: u16,
    sampled_basis_point: u16,
    direction: QueryShadowDirection,
    timeout: Duration,
    permits: Arc<Semaphore>,
    observations: Arc<Mutex<QueryShadowObservationCache>>,
    report_sink: Arc<dyn QueryShadowReportSink>,
}

impl QueryShadowAdmission for QueryShadowAdmissionAttempt {
    fn shadow_timeout(&self) -> Duration {
        self.timeout
    }

    fn try_admit(
        &self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
    ) -> Option<QueryShadowPermit> {
        // Keep this check as a strict defensive boundary for direct admission
        // callers. Ordinary requests are rejected earlier in
        // `execution_mode_for_sample`, before registry work is attempted.
        if self.sampled_basis_point >= self.sampling_bps {
            log_admission(udf_type, "not_sampled");
            return None;
        }
        if !self
            .report_sink
            .record_query_shadow_report(QueryShadowReport::Admission {
                udf_type,
                route_key: route_key.clone(),
                direction: self.direction,
                event: QueryShadowAdmissionEvent::Attempt,
            })
        {
            // Without accepted route-level evidence, this request cannot be
            // accounted for under the bounded route/detail policy. Refuse work
            // before reserving observation state or shared capacity.
            log_admission(udf_type, "evidence_rejected");
            return None;
        }
        let route_admission = {
            let mut observations = self.observations.lock();
            observations.reserve_in_direction(udf_type, route_key, self.direction, Instant::now())
        };
        let reservation = match route_admission {
            RouteObservationAdmission::Reserved(reservation) => Some(reservation),
            RouteObservationAdmission::Pending => {
                let _ = self
                    .report_sink
                    .record_query_shadow_report(QueryShadowReport::Admission {
                        udf_type,
                        route_key: route_key.clone(),
                        direction: self.direction,
                        event: QueryShadowAdmissionEvent::PendingDrop,
                    });
                log_admission(udf_type, "pending");
                return None;
            },
        };

        match self.permits.clone().try_acquire_owned() {
            Ok(permit) => {
                if !self
                    .report_sink
                    .record_query_shadow_report(QueryShadowReport::Admission {
                        udf_type,
                        route_key: route_key.clone(),
                        direction: self.direction,
                        event: QueryShadowAdmissionEvent::Admitted,
                    })
                {
                    if let Some(reservation) = reservation {
                        self.observations.lock().release_pending_in_direction(
                            udf_type,
                            route_key,
                            self.direction,
                            reservation,
                        );
                    }
                    // Dropping the immediate-admission permit here preserves the
                    // zero-queue contract and leaves the configured primary
                    // authoritative.
                    drop(permit);
                    log_admission(udf_type, "evidence_rejected");
                    return None;
                }
                log_admission(udf_type, "admitted");
                let tracker = QueryShadowRouteObservationTracker {
                    observations: Arc::clone(&self.observations),
                    udf_type,
                    route_key: route_key.clone(),
                    direction: self.direction,
                    reservation,
                };
                Some(QueryShadowPermit::new_with_direction(
                    permit,
                    udf_type,
                    route_key.clone(),
                    self.direction,
                    Arc::clone(&self.report_sink),
                    Arc::new(tracker),
                ))
            },
            Err(_) => {
                let _ = self
                    .report_sink
                    .record_query_shadow_report(QueryShadowReport::Admission {
                        udf_type,
                        route_key: route_key.clone(),
                        direction: self.direction,
                        event: QueryShadowAdmissionEvent::CapacityDrop,
                    });
                if let Some(reservation) = reservation {
                    self.observations.lock().release_pending_in_direction(
                        udf_type,
                        route_key,
                        self.direction,
                        reservation,
                    );
                }
                // The configured primary remains authoritative if the
                // non-queued verifier budget is full.
                log_admission(udf_type, "overloaded");
                None
            },
        }
    }
}

struct QueryShadowRouteObservationTracker {
    observations: Arc<Mutex<QueryShadowObservationCache>>,
    udf_type: UdfType,
    route_key: QueryShadowRouteKey,
    direction: QueryShadowDirection,
    reservation: Option<u64>,
}

impl QueryShadowObservationTracker for QueryShadowRouteObservationTracker {
    fn comparison_completed(&self) {
        self.observations.lock().complete_comparison_in_direction(
            self.udf_type,
            &self.route_key,
            self.direction,
            self.reservation,
            Instant::now(),
        );
    }

    fn terminal(&self) {
        if let Some(reservation) = self.reservation {
            self.observations.lock().release_pending_in_direction(
                self.udf_type,
                &self.route_key,
                self.direction,
                reservation,
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RouteObservationState {
    Pending {
        timestamp: Instant,
        reservation: u64,
    },
    Observed {
        timestamp: Instant,
    },
}

impl RouteObservationState {
    fn timestamp(self) -> Instant {
        match self {
            Self::Pending { timestamp, .. } | Self::Observed { timestamp } => timestamp,
        }
    }
}

enum RouteObservationAdmission {
    Reserved(u64),
    Pending,
}

struct QueryShadowObservationCache {
    capacity: usize,
    ttl: Duration,
    entries: HashMap<ShadowObservationKey, RouteObservationState>,
    recency: VecDeque<ShadowObservationKey>,
    next_reservation: u64,
}

impl QueryShadowObservationCache {
    fn new(capacity: usize, ttl: Duration) -> Self {
        assert!(
            capacity > 0,
            "query shadow observation capacity must be positive"
        );
        assert!(
            !ttl.is_zero(),
            "query shadow observation TTL must be positive"
        );
        Self {
            capacity,
            ttl,
            entries: HashMap::new(),
            recency: VecDeque::new(),
            next_reservation: 0,
        }
    }

    fn reserve(
        &mut self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        now: Instant,
    ) -> RouteObservationAdmission {
        self.reserve_in_direction(
            udf_type,
            route_key,
            QueryShadowDirection::V8PrimaryWasmShadow,
            now,
        )
    }

    fn reserve_in_direction(
        &mut self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        direction: QueryShadowDirection,
        now: Instant,
    ) -> RouteObservationAdmission {
        self.remove_expired(now);
        let observation_key = ShadowObservationKey::in_direction(udf_type, route_key, direction);
        if let Some(state) = self.entries.get(&observation_key).copied() {
            if matches!(state, RouteObservationState::Pending { .. }) {
                self.touch(&observation_key);
                return RouteObservationAdmission::Pending;
            }
        }

        self.next_reservation = self
            .next_reservation
            .checked_add(1)
            .expect("query shadow observation reservation ID exhausted");
        let reservation = self.next_reservation;
        self.entries.insert(
            observation_key.clone(),
            RouteObservationState::Pending {
                timestamp: now,
                reservation,
            },
        );
        self.touch(&observation_key);
        self.enforce_capacity(direction);
        RouteObservationAdmission::Reserved(reservation)
    }

    fn release_pending(
        &mut self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        reservation: u64,
    ) {
        self.release_pending_in_direction(
            udf_type,
            route_key,
            QueryShadowDirection::V8PrimaryWasmShadow,
            reservation,
        )
    }

    fn release_pending_in_direction(
        &mut self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        direction: QueryShadowDirection,
        reservation: u64,
    ) {
        let observation_key = ShadowObservationKey::in_direction(udf_type, route_key, direction);
        if matches!(
            self.entries.get(&observation_key),
            Some(RouteObservationState::Pending {
                reservation: current_reservation,
                ..
            }) if *current_reservation == reservation
        ) {
            self.entries.remove(&observation_key);
            self.recency.retain(|key| key != &observation_key);
        }
    }

    fn complete_comparison(
        &mut self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        reservation: Option<u64>,
        now: Instant,
    ) {
        self.complete_comparison_in_direction(
            udf_type,
            route_key,
            QueryShadowDirection::V8PrimaryWasmShadow,
            reservation,
            now,
        )
    }

    fn complete_comparison_in_direction(
        &mut self,
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        direction: QueryShadowDirection,
        reservation: Option<u64>,
        now: Instant,
    ) {
        self.remove_expired(now);
        let observation_key = ShadowObservationKey::in_direction(udf_type, route_key, direction);
        let reservation_is_current = match (reservation, self.entries.get(&observation_key)) {
            (
                Some(reservation),
                Some(RouteObservationState::Pending {
                    reservation: current_reservation,
                    ..
                }),
            ) => reservation == *current_reservation,
            (Some(_), Some(RouteObservationState::Observed { .. }) | None) => false,
            (None, Some(RouteObservationState::Pending { .. })) => false,
            (None, Some(RouteObservationState::Observed { .. }) | None) => true,
        };
        if !reservation_is_current {
            return;
        }
        self.entries.insert(
            observation_key.clone(),
            RouteObservationState::Observed { timestamp: now },
        );
        self.touch(&observation_key);
        self.enforce_capacity(direction);
    }

    fn remove_expired(&mut self, now: Instant) {
        self.entries.retain(|_, state| {
            !now.checked_duration_since(state.timestamp())
                .is_some_and(|elapsed| elapsed >= self.ttl)
        });
        self.recency
            .retain(|route_key| self.entries.contains_key(route_key));
    }

    fn touch(&mut self, route_key: &ShadowObservationKey) {
        self.recency.retain(|key| key != route_key);
        self.recency.push_back(route_key.clone());
    }

    fn enforce_capacity(&mut self, direction: QueryShadowDirection) {
        while self
            .entries
            .keys()
            .filter(|key| key.direction == direction)
            .count()
            > self.capacity
        {
            let position = self
                .recency
                .iter()
                .position(|key| key.direction == direction)
                .expect("query shadow observation recency lost an entry");
            let route_key = self
                .recency
                .remove(position)
                .expect("query shadow observation recency lost an entry");
            self.entries.remove(&route_key);
        }
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct ShadowObservationKey {
    udf_type: UdfType,
    route_key: QueryShadowRouteKey,
    direction: QueryShadowDirection,
}

impl ShadowObservationKey {
    #[cfg(test)]
    fn new(udf_type: UdfType, route_key: &QueryShadowRouteKey) -> Self {
        Self::in_direction(
            udf_type,
            route_key,
            QueryShadowDirection::V8PrimaryWasmShadow,
        )
    }

    fn in_direction(
        udf_type: UdfType,
        route_key: &QueryShadowRouteKey,
        direction: QueryShadowDirection,
    ) -> Self {
        Self {
            udf_type,
            route_key: route_key.clone(),
            direction,
        }
    }
}

fn log_admission(udf_type: UdfType, outcome: &'static str) {
    log_counter_with_labels(
        &STATIC_HERMES_QUERY_SHADOW_ADMISSION_TOTAL,
        1,
        vec![
            udf_type.metric_label(),
            StaticMetricLabel::new("outcome", outcome),
        ],
    );
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::Write,
        path::Path,
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            Arc,
        },
        time::{
            Duration,
            Instant,
        },
    };

    use anyhow::Context;
    use function_runner::{
        QueryShadowAdmission,
        QueryShadowComparison,
        QueryShadowFailureReason,
        QueryShadowFailureStage,
        QueryShadowInsertedDocumentIdentity,
        QueryShadowTerminal,
    };
    use serde_json::{
        json,
        Value as JsonValue,
    };
    use tokio::sync::Semaphore;
    use value::sha256::Sha256;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(30);

    fn route_key(generation: char, route: char) -> QueryShadowRouteKey {
        QueryShadowRouteKey::new(
            &generation.to_string().repeat(64),
            &route.to_string().repeat(64),
        )
        .unwrap()
    }

    fn coordinator(
        sampling_bps: u16,
        permits: Arc<Semaphore>,
        observations: Arc<Mutex<QueryShadowObservationCache>>,
    ) -> QueryShadowCoordinator {
        QueryShadowCoordinator {
            gate_enabled: false,
            query_sampling_bps: sampling_bps,
            mutation_sampling_bps: sampling_bps,
            query_wasm_primary_enabled: false,
            mutation_wasm_primary_enabled: false,
            query_wasm_primary_v8_sampling_bps: 0,
            mutation_wasm_primary_v8_sampling_bps: 0,
            timeout: TEST_TIMEOUT,
            permits: Some(permits),
            observations,
            report_sink: Arc::new(|_| true),
        }
    }

    const FOCUSED_EXECUTION_REQUEST_FILE_ENV: &str =
        "CONVEX_STATIC_HERMES_WASM_GATE_FOCUSED_SHADOW_REQUEST_FILE";
    const FOCUSED_EXECUTION_REQUEST_SHA256_ENV: &str =
        "CONVEX_STATIC_HERMES_WASM_GATE_FOCUSED_SHADOW_REQUEST_SHA256";
    const FOCUSED_EXECUTION_OUTPUT_ENV: &str =
        "CONVEX_STATIC_HERMES_WASM_GATE_FOCUSED_SHADOW_OUTPUT";
    const FOCUSED_EXECUTION_KIND: &str = "convex-static-hermes-focused-shadow-execution-v1";
    const FOCUSED_EXECUTION_REQUEST_KIND: &str =
        "convex-static-hermes-focused-shadow-execution-request-v1";

    fn focused_execution_request() -> anyhow::Result<(JsonValue, String, String)> {
        let request_path = std::env::var(FOCUSED_EXECUTION_REQUEST_FILE_ENV)?;
        let request_sha256 = std::env::var(FOCUSED_EXECUTION_REQUEST_SHA256_ENV)?;
        let output_path = std::env::var(FOCUSED_EXECUTION_OUTPUT_ENV)?;
        anyhow::ensure!(
            request_sha256.len() == 64
                && request_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "focused shadow execution request digest is invalid"
        );
        anyhow::ensure!(
            Path::new(&request_path).is_absolute() && Path::new(&output_path).is_absolute(),
            "focused shadow execution paths must be absolute"
        );
        let request_bytes = std::fs::read(&request_path)
            .context("failed to read focused shadow execution request")?;
        anyhow::ensure!(
            Sha256::hash(&request_bytes).as_hex() == request_sha256,
            "focused shadow execution request digest does not match its bytes"
        );
        let request: JsonValue = serde_json::from_slice(&request_bytes)
            .context("failed to parse focused shadow execution request")?;
        let object = request
            .as_object()
            .context("focused shadow execution request must be an object")?;
        anyhow::ensure!(
            object.len() == 7
                && object.get("kind") == Some(&json!(FOCUSED_EXECUTION_REQUEST_KIND))
                && object.get("schemaVersion") == Some(&json!(1)),
            "focused shadow execution request has an unexpected shape"
        );
        for field in [
            "backendImageId",
            "caseKind",
            "deploymentSha256",
            "generationSha256",
        ] {
            anyhow::ensure!(
                object.get(field).is_some_and(JsonValue::is_string),
                "focused shadow execution request field {field} is invalid"
            );
        }
        let registry = object
            .get("registry")
            .and_then(JsonValue::as_object)
            .context("focused shadow execution request registry is invalid")?;
        anyhow::ensure!(
            registry.len() == 3
                && [
                    "currentSha256",
                    "generationManifestSha256",
                    "generationSha256"
                ]
                .into_iter()
                .all(|field| registry.get(field).is_some_and(JsonValue::is_string)),
            "focused shadow execution request registry has an unexpected shape"
        );
        anyhow::ensure!(
            matches!(
                object.get("caseKind").and_then(JsonValue::as_str),
                Some("shadowFailure" | "shadowTimeout" | "sharedPermit")
            ),
            "focused shadow execution request case is invalid"
        );
        Ok((request, request_sha256, output_path))
    }

    fn focused_execution_event(report: QueryShadowReport) -> anyhow::Result<&'static str> {
        match report {
            QueryShadowReport::Memory { .. } => {
                unreachable!("memory reports are not lifecycle events")
            },
            QueryShadowReport::Admission { event, .. } => Ok(match event {
                QueryShadowAdmissionEvent::Attempt => "attempt",
                QueryShadowAdmissionEvent::Admitted => "admitted",
                QueryShadowAdmissionEvent::PendingDrop => "pendingDrop",
                QueryShadowAdmissionEvent::CapacityDrop => "capacityDrop",
            }),
            QueryShadowReport::Terminal { terminal, .. } => Ok(match terminal {
                QueryShadowTerminal::CapacityDrop => "terminal:capacityDrop",
                QueryShadowTerminal::ShadowFailure { .. } => "terminal:shadowFailure",
                QueryShadowTerminal::Timeout => "terminal:timeout",
                QueryShadowTerminal::InvalidShadow { .. } => "terminal:invalidShadow",
                QueryShadowTerminal::InvalidPrimary => "terminal:invalidPrimary",
                QueryShadowTerminal::PrimaryFailure => "terminal:primaryFailure",
                QueryShadowTerminal::PrimarySnapshotRejected => "terminal:primarySnapshotRejected",
                QueryShadowTerminal::UnexpectedTaskExit => "terminal:unexpectedTaskExit",
            }),
            QueryShadowReport::Compared { comparison, .. } => {
                anyhow::ensure!(
                    comparison.matches(),
                    "focused shadow comparison did not match"
                );
                Ok("compared")
            },
        }
    }

    fn focused_execution_events(
        reports: &Arc<Mutex<Vec<QueryShadowReport>>>,
        cursor: &mut usize,
    ) -> anyhow::Result<Vec<&'static str>> {
        let reports = reports.lock();
        let events = reports[*cursor..]
            .iter()
            .filter(|report| !matches!(report, QueryShadowReport::Memory { .. }))
            .cloned()
            .map(focused_execution_event)
            .collect::<anyhow::Result<Vec<_>>>()?;
        *cursor = reports.len();
        Ok(events)
    }

    fn matching_comparison() -> QueryShadowComparison {
        QueryShadowComparison {
            snapshot_matches: true,
            result_matches: true,
            journal_matches: true,
            read_dependencies_match: true,
            invocation_inputs_match: true,
            host_operation_trace_matches: true,
            host_operation_error_matches: true,
            observed_identity_matches: true,
            observed_time_matches: true,
            observed_rng_matches: true,
            log_lines_match: true,
            audit_log_lines_match: true,
            write_set_matches: true,
            inserted_document_identity: QueryShadowInsertedDocumentIdentity::NotApplicable,
            host_operation_trace_diagnostic: None,
            result_diagnostic: None,
        }
    }

    fn focused_execution_admit(
        coordinator: &QueryShadowCoordinator,
        route_key: &QueryShadowRouteKey,
    ) -> anyhow::Result<QueryShadowPermit> {
        let FunctionExecutionMode::V8WithStaticHermesWasmShadow(admission) =
            coordinator.execution_mode_for_sample(UdfType::Query, 0)
        else {
            anyhow::bail!("focused shadow execution did not sample the route")
        };
        admission
            .try_admit(UdfType::Query, route_key)
            .context("focused shadow execution did not admit the route")
    }

    fn write_focused_execution_output(path: &str, output: &JsonValue) -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .context("failed to create focused shadow execution output")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .context("failed to set focused shadow execution output permissions")?;
        }
        serde_json::to_writer(&mut file, output)
            .context("failed to write focused shadow execution output")?;
        file.write_all(b"\n")
            .context("failed to terminate focused shadow execution output")?;
        file.sync_all()
            .context("failed to sync focused shadow execution output")?;
        Ok(())
    }

    #[test]
    fn disabled_coordinator_keeps_queries_and_mutations_configured() {
        let coordinator = QueryShadowCoordinator {
            gate_enabled: false,
            query_sampling_bps: 0,
            mutation_sampling_bps: 0,
            query_wasm_primary_enabled: false,
            mutation_wasm_primary_enabled: false,
            query_wasm_primary_v8_sampling_bps: 0,
            mutation_wasm_primary_v8_sampling_bps: 0,
            timeout: TEST_TIMEOUT,
            permits: None,
            observations: Arc::new(Mutex::new(QueryShadowObservationCache::new(
                2,
                Duration::from_secs(1),
            ))),
            report_sink: Arc::new(|_| true),
        };
        for udf_type in [UdfType::Query, UdfType::Mutation] {
            assert!(matches!(
                coordinator.execution_mode_for_sample(udf_type, 0),
                FunctionExecutionMode::Configured
            ));
        }
    }

    #[test]
    fn direct_no_cache_query_uses_shadow_sampling_or_configured_v8() {
        let enabled = coordinator(
            1,
            Arc::new(Semaphore::new(1)),
            Arc::new(Mutex::new(QueryShadowObservationCache::new(
                2,
                Duration::from_secs(1),
            ))),
        );
        assert!(matches!(
            enabled.execution_mode_for_sample(UdfType::Query, 0),
            FunctionExecutionMode::V8WithStaticHermesWasmShadow(_)
        ));

        let disabled = QueryShadowCoordinator {
            gate_enabled: false,
            query_sampling_bps: 0,
            mutation_sampling_bps: 1,
            query_wasm_primary_enabled: false,
            mutation_wasm_primary_enabled: false,
            query_wasm_primary_v8_sampling_bps: 0,
            mutation_wasm_primary_v8_sampling_bps: 0,
            timeout: TEST_TIMEOUT,
            permits: Some(Arc::new(Semaphore::new(1))),
            observations: Arc::new(Mutex::new(QueryShadowObservationCache::new(
                2,
                Duration::from_secs(1),
            ))),
            report_sink: Arc::new(|_| true),
        };
        assert!(matches!(
            disabled.execution_mode_for_sample(UdfType::Query, 0),
            FunctionExecutionMode::Configured
        ));

        assert!(matches!(
            enabled.execution_mode_for_sample(UdfType::Query, 999),
            FunctionExecutionMode::V8
        ));
    }

    #[test]
    fn disabled_query_sampling_stays_configured_when_mutation_shadow_is_enabled() {
        let coordinator = QueryShadowCoordinator {
            gate_enabled: false,
            query_sampling_bps: 0,
            mutation_sampling_bps: 10_000,
            query_wasm_primary_enabled: false,
            mutation_wasm_primary_enabled: false,
            query_wasm_primary_v8_sampling_bps: 0,
            mutation_wasm_primary_v8_sampling_bps: 0,
            timeout: TEST_TIMEOUT,
            permits: Some(Arc::new(Semaphore::new(1))),
            observations: Arc::new(Mutex::new(QueryShadowObservationCache::new(
                2,
                Duration::from_secs(1),
            ))),
            report_sink: Arc::new(|_| true),
        };

        assert!(matches!(
            coordinator.execution_mode_for_sample(UdfType::Query, 0),
            FunctionExecutionMode::Configured
        ));
        assert!(matches!(
            coordinator.execution_mode_for_sample(UdfType::Mutation, 0),
            FunctionExecutionMode::V8WithStaticHermesWasmShadow(_)
        ));
    }

    #[test]
    fn five_percent_sampling_has_one_fixed_boundary_for_queries_and_mutations() {
        let coordinator = coordinator(
            500,
            Arc::new(Semaphore::new(1)),
            Arc::new(Mutex::new(QueryShadowObservationCache::new(
                2,
                Duration::from_secs(1),
            ))),
        );

        for udf_type in [UdfType::Query, UdfType::Mutation] {
            for sampled_basis_point in [0, 499] {
                assert!(matches!(
                    coordinator.execution_mode_for_sample(udf_type, sampled_basis_point),
                    FunctionExecutionMode::V8WithStaticHermesWasmShadow(_)
                ));
            }
            for sampled_basis_point in [500, 9_999] {
                assert!(matches!(
                    coordinator.execution_mode_for_sample(udf_type, sampled_basis_point),
                    FunctionExecutionMode::V8
                ));
            }
        }
    }

    #[test]
    fn sampling_rejects_an_unseen_route_without_reserving_or_acquiring_capacity() {
        let permits = Arc::new(Semaphore::new(2));
        let observations = Arc::new(Mutex::new(QueryShadowObservationCache::new(
            8,
            Duration::from_secs(60),
        )));
        let coordinator = coordinator(500, Arc::clone(&permits), Arc::clone(&observations));
        let route_key = route_key('1', 'a');

        assert!(matches!(
            coordinator.execution_mode_for_sample(UdfType::Query, 999),
            FunctionExecutionMode::V8
        ));
        assert!(observations.lock().entries.is_empty());
        assert_eq!(permits.available_permits(), 2);

        let first = QueryShadowAdmissionAttempt {
            sampling_bps: coordinator.query_sampling_bps,
            sampled_basis_point: 999,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            timeout: coordinator.timeout,
            permits: Arc::clone(coordinator.permits.as_ref().unwrap()),
            observations: Arc::clone(&coordinator.observations),
            report_sink: Arc::clone(&coordinator.report_sink),
        }
        .try_admit(UdfType::Query, &route_key);
        assert!(first.is_none());
        assert!(observations.lock().entries.is_empty());
        assert_eq!(permits.available_permits(), 2);
        assert!(QueryShadowAdmissionAttempt {
            sampling_bps: coordinator.query_sampling_bps,
            sampled_basis_point: 499,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            timeout: coordinator.timeout,
            permits: Arc::clone(coordinator.permits.as_ref().unwrap()),
            observations: Arc::clone(&coordinator.observations),
            report_sink: Arc::clone(&coordinator.report_sink),
        }
        .try_admit(UdfType::Query, &route_key)
        .is_some());
    }

    #[test]
    fn overload_reports_capacity_drop_before_releasing_reservation_for_retry() {
        let permits = Arc::new(Semaphore::new(0));
        let observations = Arc::new(Mutex::new(QueryShadowObservationCache::new(
            8,
            Duration::from_secs(60),
        )));
        let route_key = route_key('2', 'b');
        let observations_for_sink = Arc::clone(&observations);
        let route_key_for_sink = route_key.clone();
        let attempt = QueryShadowAdmissionAttempt {
            sampling_bps: 500,
            sampled_basis_point: 499,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            timeout: TEST_TIMEOUT,
            permits: Arc::clone(&permits),
            observations: Arc::clone(&observations),
            report_sink: Arc::new(move |report| {
                if matches!(
                    report,
                    QueryShadowReport::Admission {
                        event: QueryShadowAdmissionEvent::CapacityDrop,
                        ..
                    }
                ) {
                    let observation_key =
                        ShadowObservationKey::new(UdfType::Query, &route_key_for_sink);
                    assert!(matches!(
                        observations_for_sink.lock().entries.get(&observation_key),
                        Some(RouteObservationState::Pending { .. })
                    ));
                }
                true
            }),
        };

        assert!(attempt.try_admit(UdfType::Query, &route_key).is_none());
        assert!(observations.lock().entries.is_empty());
        permits.add_permits(1);
        assert!(attempt.try_admit(UdfType::Query, &route_key).is_some());
    }

    #[test]
    fn concurrent_route_attempt_retains_a_pending_drop() {
        let permits = Arc::new(Semaphore::new(2));
        let observations = Arc::new(Mutex::new(QueryShadowObservationCache::new(
            8,
            Duration::from_secs(60),
        )));
        let reports = Arc::new(Mutex::new(Vec::new()));
        let reports_for_sink = Arc::clone(&reports);
        let route_key = route_key('2', 'e');
        let attempt = QueryShadowAdmissionAttempt {
            sampling_bps: 10_000,
            sampled_basis_point: 0,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            timeout: TEST_TIMEOUT,
            permits,
            observations,
            report_sink: Arc::new(move |report| {
                reports_for_sink.lock().push(report);
                true
            }),
        };

        let first = attempt
            .try_admit(UdfType::Query, &route_key)
            .expect("first route attempt was not admitted");
        assert!(attempt.try_admit(UdfType::Query, &route_key).is_none());
        assert!(matches!(
            reports.lock().last(),
            Some(QueryShadowReport::Admission {
                event: QueryShadowAdmissionEvent::PendingDrop,
                ..
            })
        ));
        drop(first);
    }

    #[test]
    fn two_completed_routes_at_capacity_release_for_immediate_reuse() {
        let permits = Arc::new(Semaphore::new(2));
        let observations = Arc::new(Mutex::new(QueryShadowObservationCache::new(
            8,
            Duration::from_secs(60),
        )));
        let first_route_key = route_key('2', 'c');
        let second_route_key = route_key('2', 'd');
        let reports = Arc::new(AtomicUsize::new(0));
        let reports_for_sink = Arc::clone(&reports);
        let attempt = QueryShadowAdmissionAttempt {
            sampling_bps: 10_000,
            sampled_basis_point: 0,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            timeout: TEST_TIMEOUT,
            permits: Arc::clone(&permits),
            observations: Arc::clone(&observations),
            report_sink: Arc::new(move |_| {
                reports_for_sink.fetch_add(1, Ordering::Relaxed);
                true
            }),
        };

        let mut first = attempt.try_admit(UdfType::Query, &first_route_key).unwrap();
        let mut second = attempt
            .try_admit(UdfType::Query, &second_route_key)
            .unwrap();
        assert_eq!(permits.available_permits(), 0);

        first.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());
        second.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());
        assert_eq!(permits.available_permits(), 2);

        let mut first_reused = attempt
            .try_admit(UdfType::Query, &first_route_key)
            .expect("completed route retained shadow capacity at the hard ceiling");
        let mut second_reused = attempt
            .try_admit(UdfType::Query, &second_route_key)
            .unwrap();
        assert_eq!(permits.available_permits(), 0);
        first_reused.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());
        second_reused.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());

        assert_eq!(reports.load(Ordering::Relaxed), 12);
        assert_eq!(permits.available_permits(), 2);
    }

    #[test]
    fn rejected_attempt_evidence_neither_reserves_nor_acquires_capacity() {
        let permits = Arc::new(Semaphore::new(1));
        let observations = Arc::new(Mutex::new(QueryShadowObservationCache::new(
            8,
            Duration::from_secs(60),
        )));
        let route_key = route_key('7', 'a');
        let attempt = QueryShadowAdmissionAttempt {
            sampling_bps: 500,
            sampled_basis_point: 499,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            timeout: TEST_TIMEOUT,
            permits: Arc::clone(&permits),
            observations: Arc::clone(&observations),
            report_sink: Arc::new(|_| false),
        };

        assert!(attempt.try_admit(UdfType::Query, &route_key).is_none());
        assert!(observations.lock().entries.is_empty());
        assert_eq!(permits.available_permits(), 1);
    }

    #[test]
    fn coordinator_passes_its_validated_timeout_to_admission() {
        let permits = Arc::new(Semaphore::new(1));
        let observations = Arc::new(Mutex::new(QueryShadowObservationCache::new(
            8,
            Duration::from_secs(60),
        )));
        let coordinator = coordinator(500, permits, observations);
        let FunctionExecutionMode::V8WithStaticHermesWasmShadow(admission) =
            coordinator.execution_mode_for_sample(UdfType::Query, 0)
        else {
            panic!("query shadow coordinator unexpectedly disabled")
        };
        assert_eq!(admission.shadow_timeout(), TEST_TIMEOUT);
    }

    #[test]
    fn coordinator_construction_reads_the_default_timeout() {
        let coordinator = QueryShadowCoordinator::new(Arc::new(|_| true));
        assert_eq!(coordinator.timeout, Duration::from_secs(30));
    }

    #[test]
    fn wasm_primary_sampling_is_independent_from_v8_primary_sampling() {
        let coordinator = QueryShadowCoordinator {
            gate_enabled: true,
            query_sampling_bps: 0,
            mutation_sampling_bps: 0,
            query_wasm_primary_enabled: true,
            mutation_wasm_primary_enabled: true,
            query_wasm_primary_v8_sampling_bps: 500,
            mutation_wasm_primary_v8_sampling_bps: 10_000,
            timeout: TEST_TIMEOUT,
            permits: Some(Arc::new(Semaphore::new(2))),
            observations: Arc::new(Mutex::new(QueryShadowObservationCache::new(
                2,
                Duration::from_secs(1),
            ))),
            report_sink: Arc::new(|_| true),
        };
        assert!(matches!(
            coordinator.wasm_primary_execution_mode_for_sample(UdfType::Query, 0),
            FunctionExecutionMode::StaticHermesWasmWithV8Shadow(_)
        ));
        assert!(matches!(
            coordinator.wasm_primary_execution_mode_for_sample(UdfType::Query, 500),
            FunctionExecutionMode::Configured
        ));
        assert!(matches!(
            coordinator.wasm_primary_execution_mode_for_sample(UdfType::Mutation, 9_999),
            FunctionExecutionMode::StaticHermesWasmWithV8Shadow(_)
        ));
        assert!(matches!(
            coordinator.execution_mode_for_sample(UdfType::Query, 0),
            FunctionExecutionMode::Configured
        ));
        let disabled = QueryShadowCoordinator {
            query_wasm_primary_v8_sampling_bps: 0,
            mutation_wasm_primary_v8_sampling_bps: 0,
            permits: None,
            ..coordinator
        };
        assert!(matches!(
            disabled.wasm_primary_execution_mode_for_sample(UdfType::Mutation, 0),
            FunctionExecutionMode::Configured
        ));
    }

    #[test]
    fn query_and_mutation_can_use_opposite_primary_directions() {
        let coordinator = QueryShadowCoordinator {
            gate_enabled: true,
            query_sampling_bps: 10_000,
            mutation_sampling_bps: 0,
            query_wasm_primary_enabled: false,
            mutation_wasm_primary_enabled: true,
            query_wasm_primary_v8_sampling_bps: 0,
            mutation_wasm_primary_v8_sampling_bps: 10_000,
            timeout: TEST_TIMEOUT,
            permits: Some(Arc::new(Semaphore::new(2))),
            observations: Arc::new(Mutex::new(QueryShadowObservationCache::new(
                2,
                Duration::from_secs(1),
            ))),
            report_sink: Arc::new(|_| true),
        };

        assert!(matches!(
            coordinator.configured_execution_mode_for_sample(UdfType::Query, 0),
            FunctionExecutionMode::V8WithStaticHermesWasmShadow(_)
        ));
        assert!(matches!(
            coordinator.configured_execution_mode_for_sample(UdfType::Mutation, 0),
            FunctionExecutionMode::StaticHermesWasmWithV8Shadow(_)
        ));

        let query_without_shadow = QueryShadowCoordinator {
            query_sampling_bps: 0,
            ..coordinator
        };
        assert!(matches!(
            query_without_shadow.configured_execution_mode_for_sample(UdfType::Query, 0),
            FunctionExecutionMode::V8
        ));
    }

    #[cfg(not(feature = "static-hermes-wasmtime-gate"))]
    #[test]
    fn shadow_sampling_requires_static_hermes_wasmtime_gate() {
        let error = validate_shadow_sampling_configuration(500, 0, 0, 0, false, false).unwrap_err();

        assert_eq!(
            error.to_string(),
            "Static Hermes shadow sampling requires static-hermes-wasmtime-gate; set all shadow \
             BPS knobs to 0 in a build without that feature"
        );
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn shadow_sampling_accepts_each_udf_type_in_its_configured_direction() -> anyhow::Result<()> {
        validate_shadow_sampling_configuration(500, 500, 0, 0, false, false)?;
        validate_shadow_sampling_configuration(0, 0, 500, 500, true, true)?;
        validate_shadow_sampling_configuration(500, 0, 0, 500, false, true)
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn shadow_sampling_rejects_the_opposite_primary_direction() {
        let legacy_error =
            validate_shadow_sampling_configuration(500, 0, 0, 0, true, false).unwrap_err();
        assert!(legacy_error
            .to_string()
            .contains("query V8-primary Static Hermes shadow sampling must be zero"));

        let inverse_error =
            validate_shadow_sampling_configuration(0, 0, 500, 0, false, false).unwrap_err();
        assert!(inverse_error
            .to_string()
            .contains("query Wasm-primary V8 verifier sampling must be zero"));
    }

    #[test]
    fn rejected_admitted_evidence_releases_reservation_and_capacity() {
        let permits = Arc::new(Semaphore::new(1));
        let observations = Arc::new(Mutex::new(QueryShadowObservationCache::new(
            8,
            Duration::from_secs(60),
        )));
        let reports = Arc::new(AtomicUsize::new(0));
        let reports_for_sink = Arc::clone(&reports);
        let route_key = route_key('7', 'b');
        let attempt = QueryShadowAdmissionAttempt {
            sampling_bps: 500,
            sampled_basis_point: 499,
            direction: QueryShadowDirection::V8PrimaryWasmShadow,
            timeout: TEST_TIMEOUT,
            permits: Arc::clone(&permits),
            observations: Arc::clone(&observations),
            report_sink: Arc::new(move |_| reports_for_sink.fetch_add(1, Ordering::Relaxed) == 0),
        };

        assert!(attempt.try_admit(UdfType::Query, &route_key).is_none());
        assert_eq!(reports.load(Ordering::Relaxed), 2);
        assert!(observations.lock().entries.is_empty());
        assert_eq!(permits.available_permits(), 1);
    }

    #[test]
    fn terminal_reservation_is_not_observed_and_completed_comparison_is() {
        let route_key = route_key('3', 'c');
        let now = Instant::now();
        let mut observations = QueryShadowObservationCache::new(8, Duration::from_secs(60));

        let RouteObservationAdmission::Reserved(first) =
            observations.reserve(UdfType::Query, &route_key, now)
        else {
            panic!("first route was not reserved")
        };
        observations.release_pending(UdfType::Query, &route_key, first);
        assert!(matches!(
            observations.reserve(UdfType::Query, &route_key, now),
            RouteObservationAdmission::Reserved(_)
        ));

        observations.complete_comparison(UdfType::Query, &route_key, Some(2), now);
        assert!(matches!(
            observations.reserve(UdfType::Query, &route_key, now),
            RouteObservationAdmission::Reserved(_)
        ));
        assert!(matches!(
            observations.reserve(UdfType::Query, &route_key, now),
            RouteObservationAdmission::Pending
        ));
    }

    #[test]
    fn observation_cache_keeps_query_and_mutation_routes_separate() {
        let route_key = route_key('3', 'd');
        let now = Instant::now();
        let mut observations = QueryShadowObservationCache::new(8, Duration::from_secs(60));

        assert!(matches!(
            observations.reserve(UdfType::Query, &route_key, now),
            RouteObservationAdmission::Reserved(_)
        ));
        assert!(matches!(
            observations.reserve(UdfType::Query, &route_key, now),
            RouteObservationAdmission::Pending
        ));
        assert!(matches!(
            observations.reserve(UdfType::Mutation, &route_key, now),
            RouteObservationAdmission::Reserved(_)
        ));
    }

    #[test]
    fn observation_cache_keeps_runtime_directions_separate() {
        let route_key = route_key('d', '1');
        let now = Instant::now();
        let mut observations = QueryShadowObservationCache::new(8, Duration::from_secs(60));
        let v8_reservation = match observations.reserve_in_direction(
            UdfType::Query,
            &route_key,
            QueryShadowDirection::V8PrimaryWasmShadow,
            now,
        ) {
            RouteObservationAdmission::Reserved(reservation) => reservation,
            RouteObservationAdmission::Pending => panic!("V8 direction was not reserved"),
        };
        observations.complete_comparison_in_direction(
            UdfType::Query,
            &route_key,
            QueryShadowDirection::V8PrimaryWasmShadow,
            Some(v8_reservation),
            now,
        );
        assert!(matches!(
            observations.reserve_in_direction(
                UdfType::Query,
                &route_key,
                QueryShadowDirection::WasmPrimaryV8Shadow,
                now,
            ),
            RouteObservationAdmission::Reserved(_)
        ));
    }

    #[test]
    fn observation_cache_capacity_is_independent_per_direction() {
        let now = Instant::now();
        let v8_route = route_key('d', '2');
        let wasm_route = route_key('d', '3');
        let mut observations = QueryShadowObservationCache::new(1, Duration::from_secs(60));

        assert!(matches!(
            observations.reserve_in_direction(
                UdfType::Query,
                &v8_route,
                QueryShadowDirection::V8PrimaryWasmShadow,
                now,
            ),
            RouteObservationAdmission::Reserved(_)
        ));
        assert!(matches!(
            observations.reserve_in_direction(
                UdfType::Query,
                &wasm_route,
                QueryShadowDirection::WasmPrimaryV8Shadow,
                now,
            ),
            RouteObservationAdmission::Reserved(_)
        ));
        assert!(matches!(
            observations.reserve_in_direction(
                UdfType::Query,
                &v8_route,
                QueryShadowDirection::V8PrimaryWasmShadow,
                now,
            ),
            RouteObservationAdmission::Pending
        ));
    }

    #[test]
    fn evicted_reservation_cannot_displace_a_newer_pending_route() {
        let now = Instant::now();
        let old_route = route_key('5', 'a');
        let active_route = route_key('6', 'a');
        let mut observations = QueryShadowObservationCache::new(1, Duration::from_secs(60));

        let RouteObservationAdmission::Reserved(old_reservation) =
            observations.reserve(UdfType::Query, &old_route, now)
        else {
            panic!("old route was not reserved")
        };
        let RouteObservationAdmission::Reserved(active_reservation) =
            observations.reserve(UdfType::Query, &active_route, now)
        else {
            panic!("active route was not reserved")
        };
        observations.complete_comparison(UdfType::Query, &old_route, Some(old_reservation), now);
        assert!(matches!(
            observations.reserve(UdfType::Query, &active_route, now),
            RouteObservationAdmission::Pending
        ));

        observations.complete_comparison(
            UdfType::Query,
            &active_route,
            Some(active_reservation),
            now,
        );
        assert!(matches!(
            observations.reserve(UdfType::Query, &active_route, now),
            RouteObservationAdmission::Reserved(_)
        ));
        assert!(matches!(
            observations.reserve(UdfType::Query, &active_route, now),
            RouteObservationAdmission::Pending
        ));
    }

    #[test]
    fn observation_cache_expires_and_evicts_least_recent_route() {
        let now = Instant::now();
        let route_a = route_key('4', 'a');
        let route_b = route_key('4', 'b');
        let route_c = route_key('4', 'c');
        let mut observations = QueryShadowObservationCache::new(2, Duration::from_secs(1));

        observations.complete_comparison(UdfType::Query, &route_a, None, now);
        observations.complete_comparison(UdfType::Query, &route_b, None, now);
        assert!(matches!(
            observations.reserve(UdfType::Query, &route_a, now),
            RouteObservationAdmission::Reserved(_)
        ));
        observations.complete_comparison(UdfType::Query, &route_c, None, now);
        assert!(matches!(
            observations.reserve(UdfType::Query, &route_b, now),
            RouteObservationAdmission::Reserved(_)
        ));
        assert!(matches!(
            observations.reserve(UdfType::Query, &route_a, now + Duration::from_secs(1)),
            RouteObservationAdmission::Reserved(_)
        ));
    }

    #[test]
    #[ignore = "writes focused executable shadow evidence for an operator-provided request"]
    fn static_hermes_focused_shadow_execution_writes_actual_records() -> anyhow::Result<()> {
        let (request, request_sha256, output_path) = focused_execution_request()?;
        let case_kind = request
            .get("caseKind")
            .and_then(JsonValue::as_str)
            .context("focused shadow execution request case is missing")?;
        let permits = Arc::new(Semaphore::new(1));
        let observations = Arc::new(Mutex::new(QueryShadowObservationCache::new(
            16,
            Duration::from_secs(60),
        )));
        let reports = Arc::new(Mutex::new(Vec::<QueryShadowReport>::new()));
        let reports_for_sink = Arc::clone(&reports);
        let coordinator = QueryShadowCoordinator {
            gate_enabled: false,
            query_sampling_bps: 10_000,
            mutation_sampling_bps: 10_000,
            query_wasm_primary_enabled: false,
            mutation_wasm_primary_enabled: false,
            query_wasm_primary_v8_sampling_bps: 0,
            mutation_wasm_primary_v8_sampling_bps: 0,
            timeout: TEST_TIMEOUT,
            permits: Some(Arc::clone(&permits)),
            observations,
            report_sink: Arc::new(move |report| {
                reports_for_sink.lock().push(report);
                true
            }),
        };
        let mut cursor = 0;
        let records = match case_kind {
            "shadowFailure" => {
                let route_key = route_key('a', '1');
                let mut permit = focused_execution_admit(&coordinator, &route_key)?;
                permit.record_terminal(QueryShadowTerminal::ShadowFailure {
                    stage: QueryShadowFailureStage::Unclassified,
                    reason: QueryShadowFailureReason::Unclassified,
                    generated_export_diagnostic: None,
                    wasmtime_trap_diagnostic: None,
                });
                drop(permit);
                anyhow::ensure!(
                    permits.available_permits() == 1,
                    "shadow failure retained a permit"
                );
                vec![json!({
                    "events": focused_execution_events(&reports, &mut cursor)?,
                    "role": "target",
                    "sequence": 1,
                })]
            },
            "shadowTimeout" => {
                let route_key = route_key('b', '2');
                let mut permit = focused_execution_admit(&coordinator, &route_key)?;
                let work_guard = permit.work_guard();
                permit.record_terminal(QueryShadowTerminal::Timeout);
                drop(permit);
                anyhow::ensure!(
                    permits.available_permits() == 0,
                    "shadow timeout released its permit before detached cleanup"
                );
                drop(work_guard);
                anyhow::ensure!(
                    permits.available_permits() == 1,
                    "shadow timeout did not release its permit after detached cleanup"
                );
                vec![json!({
                    "cleanupReleased": true,
                    "events": focused_execution_events(&reports, &mut cursor)?,
                    "role": "target",
                    "sequence": 1,
                })]
            },
            "sharedPermit" => {
                let holder_key = route_key('c', '3');
                let contender_key = route_key('d', '4');
                let released_key = route_key('e', '5');
                let mut holder = focused_execution_admit(&coordinator, &holder_key)?;
                let holder_events = focused_execution_events(&reports, &mut cursor)?;
                let work_guard = holder.work_guard();

                let FunctionExecutionMode::V8WithStaticHermesWasmShadow(contender) =
                    coordinator.execution_mode_for_sample(UdfType::Query, 0)
                else {
                    anyhow::bail!("focused shared-permit contender was not sampled")
                };
                anyhow::ensure!(
                    contender
                        .try_admit(UdfType::Query, &contender_key)
                        .is_none(),
                    "focused shared-permit contender unexpectedly queued or acquired a permit"
                );
                let contender_events = focused_execution_events(&reports, &mut cursor)?;

                holder.record_terminal(QueryShadowTerminal::Timeout);
                drop(holder);
                anyhow::ensure!(
                    permits.available_permits() == 0,
                    "focused shared-permit holder released before its detached cleanup"
                );
                let holder_cleanup_events = focused_execution_events(&reports, &mut cursor)?;
                drop(work_guard);
                anyhow::ensure!(
                    permits.available_permits() == 1,
                    "focused shared-permit holder did not release after detached cleanup"
                );

                let mut released = focused_execution_admit(&coordinator, &released_key)?;
                released.record_comparison(Duration::ZERO, Duration::ZERO, matching_comparison());
                drop(released);
                anyhow::ensure!(
                    permits.available_permits() == 1,
                    "focused shared-permit release left the permit unavailable"
                );
                let released_events = focused_execution_events(&reports, &mut cursor)?;
                vec![
                    json!({
                        "events": holder_events,
                        "role": "holderAdmitted",
                        "sequence": 1,
                    }),
                    json!({
                        "events": contender_events,
                        "role": "contender",
                        "sequence": 2,
                    }),
                    json!({
                        "cleanupReleased": true,
                        "events": holder_cleanup_events,
                        "role": "holderCleanup",
                        "sequence": 3,
                    }),
                    json!({
                        "events": released_events,
                        "role": "released",
                        "sequence": 4,
                    }),
                ]
            },
            _ => anyhow::bail!("focused shadow execution request case is unsupported"),
        };
        anyhow::ensure!(
            cursor == reports.lock().len(),
            "focused shadow reports were not retained"
        );
        write_focused_execution_output(
            &output_path,
            &json!({
                "backendImageId": request["backendImageId"],
                "caseKind": case_kind,
                "deploymentSha256": request["deploymentSha256"],
                "generationSha256": request["generationSha256"],
                "kind": FOCUSED_EXECUTION_KIND,
                "records": records,
                "registry": request["registry"],
                "requestSha256": request_sha256,
                "schemaVersion": 1,
            }),
        )
    }
}
