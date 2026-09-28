//! On-demand accounting evidence. Reading this view never checks out a Store,
//! runs guest code, creates a Module, or changes an admission decision.
use serde::Serialize;
use sha2::{
    Digest,
    Sha256,
};

use super::*;

fn identity_digest(identity: &FunctionMemoryIdentity) -> String {
    // This is an opaque correlation key within one controller lifetime, not a
    // serialized artifact identity. Never expose paths from legacy identities.
    format!("{:x}", Sha256::digest(format!("{identity:?}")))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModuleChargeInspection {
    id: u64,
    pub(super) bytes: usize,
    kind: &'static str,
    identity_sha256: Option<String>,
    image_bytes: Option<usize>,
    cached: bool,
}

impl ModuleChargeInspection {
    pub(super) fn loading(id: u64, bytes: usize) -> Self {
        Self {
            id,
            bytes,
            kind: "loading",
            identity_sha256: None,
            image_bytes: None,
            cached: false,
        }
    }
}

impl GeneratedModuleMemoryCharge {
    pub(crate) fn inspection_id(&self) -> u64 {
        self.charge_id
    }

    pub(crate) fn describe(
        &self,
        kind: &'static str,
        identity_sha256: String,
        image_bytes: Option<usize>,
        cached: bool,
    ) {
        let mut state = self.controller.state.lock();
        let record = state
            .module_charges
            .get_mut(&self.charge_id)
            .expect("module charge description lost its owner");
        record.kind = kind;
        record.identity_sha256 = Some(identity_sha256);
        record.image_bytes = image_bytes;
        record.cached = cached;
    }

    pub(crate) fn mark_uncached(&self) {
        self.controller
            .state
            .lock()
            .module_charges
            .get_mut(&self.charge_id)
            .expect("uncached module charge lost its owner")
            .cached = false;
    }
}

impl InvocationMemoryPermit {
    pub(crate) fn describe_modules(&self, mut charge_ids: Vec<u64>) {
        charge_ids.sort_unstable();
        charge_ids.dedup();
        let mut state = self.account.controller.state.lock();
        state
            .slots
            .get_mut(&self.account.slot_id)
            .expect("module description lost its Store")
            .module_charge_ids = charge_ids;
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HistogramInspection {
    // Counts are per bucket, not cumulative buckets. A consumer can subtract
    // two captures, unlike quantiles or exact lifetime maxima.
    counts: Vec<u64>,
    maximum: usize,
}

impl LogarithmicHistogram {
    fn inspect(&self) -> HistogramInspection {
        HistogramInspection {
            counts: self.buckets.to_vec(),
            maximum: self.maximum,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SlotInspection {
    id: u64,
    memory_identity_sha256: String,
    module_charge_ids: Vec<u64>,
    state: &'static str,
    guest_capacity_bytes: usize,
    shared_static_bytes: usize,
    private_host_bytes: usize,
    checkout_baseline_bytes: usize,
    idle_growth_reservation_bytes: usize,
    active_liability_bytes: usize,
    preparation_headroom_bytes: usize,
    forecast_growth_bytes: usize,
    cold_start: Option<bool>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ForecastInspection {
    memory_identity_sha256: String,
    warm_samples: u64,
    cold_samples: u64,
    warm_forecast_bytes: usize,
    cold_forecast_bytes: usize,
    warm_growth: HistogramInspection,
    cold_growth: HistogramInspection,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PopulationInspection {
    summary: WasmMemoryExecutionStatistics,
    distributions: BTreeMap<&'static str, HistogramInspection>,
}

impl MemoryStatisticField {
    fn inspection_name(self) -> &'static str {
        match self {
            Self::CheckoutBaseline => "checkoutBaselineBytes",
            Self::PeakAdmitted => "peakAdmittedBytes",
            Self::PeakGuest => "peakGuestBytes",
            Self::PeakHost => "peakHostBytes",
            Self::RequestedPeak => "requestedPeakBytes",
            Self::ForecastGrowth => "forecastGrowthBytes",
            Self::Completion => "completionBytes",
            Self::ReturnedToPool => "returnedToPoolBytes",
        }
    }
}

impl ExecutionMemoryStatistics {
    fn inspect(&self) -> Option<PopulationInspection> {
        Some(PopulationInspection {
            summary: self.snapshot()?,
            distributions: MemoryStatisticField::ALL
                .into_iter()
                .map(|field| {
                    (
                        field.inspection_name(),
                        self.distributions[field.index()].inspect(),
                    )
                })
                .collect(),
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RouteInspection {
    route_sha256: String,
    memory_identity_sha256: String,
    primary: Option<PopulationInspection>,
    shadow: Option<PopulationInspection>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticHermesMemoryInspection {
    schema_version: u32,
    controller_id: String,
    controller_elapsed_milliseconds: u64,
    generation_sha256: Option<String>,
    udf_type: &'static str,
    // The controller portion is captured under one lock. Packed bytes are one
    // atomic observation; unrelated database allocations remain concurrent.
    ledger_bytes: BTreeMap<&'static str, usize>,
    pressure: &'static str,
    cgroup_headroom_bytes: Option<usize>,
    cgroup_sample_available: bool,
    hard_instance_ceiling: usize,
    slots: Vec<SlotInspection>,
    forecasts: Vec<ForecastInspection>,
    module_charge_count: usize,
    module_charges: Vec<ModuleChargeInspection>,
    omitted_module_charges: usize,
    // None denotes the overflow bucket, not a finite byte upper bound.
    bucket_upper_bounds: Vec<Option<usize>>,
    route_record_eviction_count: u64,
    selected_route_count: usize,
    observed_route_count: usize,
    routes: Vec<RouteInspection>,
    next_cursor: Option<String>,
}

impl GeneratedMemoryController {
    pub(crate) fn inspect(
        &self,
        generation: Option<&str>,
        udf_kind: UdfKind,
        selected_routes: &[String],
        after_route: Option<&str>,
        limit: usize,
    ) -> StaticHermesMemoryInspection {
        let selected: BTreeSet<&str> = selected_routes.iter().map(String::as_str).collect();
        let state = self.state.lock();
        let observed_at = self.inspection_started.elapsed();
        let shared_buffers = self.shared_buffers.bytes();
        let aggregate = self.aggregate_metrics_locked(&state, shared_buffers);
        let idle_growth = state
            .slots
            .values()
            .filter(|s| s.status == SlotStatus::Idle)
            .fold(0usize, |sum, s| sum.saturating_add(s.idle_growth_bytes));
        let selected_records = state
            .routes
            .iter()
            .filter(|(identity, _)| {
                Some(identity.generation_sha256.as_str()) == generation
                    && identity.udf_kind == udf_kind
                    && selected.contains(identity.route_sha256.as_str())
            })
            .collect::<Vec<_>>();
        let observed_route_count = selected_records.len();
        let mut matches = selected_records.into_iter().filter(|(identity, _)| {
            after_route.is_none_or(|after| identity.route_sha256.as_str() > after)
        });
        let page = matches.by_ref().take(limit).collect::<Vec<_>>();
        let next_cursor = matches.next().and_then(|_| {
            page.last()
                .map(|(identity, _)| identity.route_sha256.clone())
        });
        // Copy only this page and bounded live ownership while locked. Digest
        // formatting and histogram/response construction must not block the
        // controller used by allocation, checkout, release and cancellation.
        let slot_records = state.slots.clone();
        let active_records = state.active.clone();
        let identities = slot_records
            .values()
            .map(|slot| &slot.identity)
            .chain(
                page.iter()
                    .filter_map(|(_, stats)| stats.memory_identity.as_ref()),
            )
            .collect::<BTreeSet<_>>();
        let function_records = identities
            .into_iter()
            .filter_map(|identity| {
                state
                    .functions
                    .get(identity)
                    .map(|stats| (identity.clone(), stats.clone()))
            })
            .collect::<Vec<_>>();
        let page = page
            .into_iter()
            .map(|(identity, stats)| (identity.clone(), stats.clone()))
            .collect::<Vec<_>>();
        const MAX_MODULE_ROWS: usize = 512;
        let module_charge_count = state.module_charges.len();
        let module_charges = state
            .module_charges
            .values()
            .take(MAX_MODULE_ROWS)
            .cloned()
            .collect();
        let fixed_module_bytes = state.fixed_module_bytes;
        let pressure = state.pressure;
        let pressure_sample_available = state.pressure_sample_available;
        let route_record_eviction_count = state.route_record_eviction_count;
        drop(state);
        let routes = page
            .iter()
            .map(|(identity, stats)| RouteInspection {
                route_sha256: identity.route_sha256.clone(),
                memory_identity_sha256: identity_digest(
                    stats
                        .memory_identity
                        .as_ref()
                        .expect("route observation lost its memory identity"),
                ),
                primary: stats.primary.inspect(),
                shadow: stats.shadow.inspect(),
            })
            .collect();
        let slots = slot_records
            .iter()
            .map(|(id, slot)| {
                let active = match slot.status {
                    SlotStatus::Active { invocation_id } => Some(&active_records[&invocation_id]),
                    _ => None,
                };
                SlotInspection {
                    id: id.0,
                    memory_identity_sha256: identity_digest(&slot.identity),
                    module_charge_ids: slot.module_charge_ids.clone(),
                    state: match slot.status {
                        SlotStatus::Idle => "idle",
                        SlotStatus::Evicting => "evicting",
                        SlotStatus::Active { .. } => "active",
                    },
                    guest_capacity_bytes: active
                        .map_or(slot.guest_bytes, |a| a.current_guest_bytes),
                    shared_static_bytes: active
                        .map_or(slot.shared_static_bytes, |a| a.shared_static_bytes),
                    private_host_bytes: active
                        .map_or(slot.retained_host_bytes, |a| a.current_host_bytes),
                    checkout_baseline_bytes: slot.retained_baseline(),
                    idle_growth_reservation_bytes: if active.is_none()
                        && slot.status == SlotStatus::Idle
                    {
                        slot.idle_growth_bytes
                    } else {
                        0
                    },
                    active_liability_bytes: active.map_or(0, |a| a.liability_bytes),
                    preparation_headroom_bytes: active.map_or(0, |a| a.preparation_headroom_bytes),
                    forecast_growth_bytes: active.map_or(0, |a| a.forecast_bytes),
                    cold_start: active.map(|a| a.cold_start),
                }
            })
            .collect();
        let forecasts = function_records
            .iter()
            .map(|(identity, stats)| ForecastInspection {
                memory_identity_sha256: identity_digest(identity),
                warm_samples: stats.warm_sample_count,
                cold_samples: stats.preparation_peak_growth.samples,
                warm_forecast_bytes: stats.forecast(self.policy.cold_peak_growth_bytes),
                cold_forecast_bytes: stats.preparation_forecast(self.policy.cold_peak_growth_bytes),
                warm_growth: stats.warm_peak_growth.inspect(),
                cold_growth: stats.preparation_peak_growth.inspect(),
            })
            .collect();
        let (pressure, headroom) = match pressure {
            BackendPressure::Unavailable => ("unavailable", None),
            BackendPressure::Active { headroom_bytes } => ("active", Some(headroom_bytes)),
            BackendPressure::Healthy { headroom_bytes } => ("healthy", Some(headroom_bytes)),
        };
        StaticHermesMemoryInspection {
            schema_version: 1,
            controller_id: self.inspection_id.to_string(),
            generation_sha256: generation.map(str::to_owned),
            controller_elapsed_milliseconds: observed_at
                .as_millis()
                .try_into()
                .expect("controller age overflow"),
            udf_type: match udf_kind {
                UdfKind::Query => "query",
                UdfKind::Mutation => "mutation",
            },
            ledger_bytes: BTreeMap::from([
                ("projected", aggregate.projected_bytes),
                ("retainedBaseline", aggregate.retained_baseline_bytes),
                ("activeLiability", aggregate.active_liability_bytes),
                ("idleGrowthReservation", idle_growth),
                ("modules", fixed_module_bytes),
                ("sharedPackedBuffers", shared_buffers),
                (
                    "unattributedAllowance",
                    aggregate.unattributed_allowance_bytes,
                ),
                ("safetyReserve", self.policy.safety_reserve_bytes),
                ("softBudget", self.policy.soft_budget_bytes),
                ("hardBudget", self.policy.hard_budget_bytes),
            ]),
            pressure,
            cgroup_headroom_bytes: headroom,
            cgroup_sample_available: pressure_sample_available,
            hard_instance_ceiling: self.policy.hard_instance_ceiling,
            slots,
            forecasts,
            module_charge_count,
            module_charges,
            omitted_module_charges: module_charge_count.saturating_sub(MAX_MODULE_ROWS),
            bucket_upper_bounds: (0..LOGARITHMIC_BUCKETS)
                .map(|i| (i + 1 < LOGARITHMIC_BUCKETS).then(|| logarithmic_bucket_upper_bound(i)))
                .collect(),
            route_record_eviction_count,
            selected_route_count: selected.len(),
            observed_route_count,
            routes,
            next_cursor,
        }
    }
}
