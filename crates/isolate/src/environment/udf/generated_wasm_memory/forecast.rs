//! Bounded warm-demand history for an authenticated route. Capacity and image
//! exposure are different populations; neither implies reusable free heap.

use std::collections::VecDeque;

use super::*;

pub(super) const ROUTE_HISTORY_SAMPLES: usize = 128;
const BASELINE_QUANTUM_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ForecastBaseline {
    initialized: bool,
    image_charge_id: Option<u64>,
    guest_bucket: usize,
    other_guest_bucket: usize,
    host_bucket: usize,
}

impl ForecastBaseline {
    fn from_slot(slot: &SlotMemory) -> Self {
        let (guest, other) = match slot.initialized_memory {
            Some(image) => (
                image.exposed_bytes,
                slot.guest_bytes
                    .checked_sub(image.linear_bytes)
                    .expect("initialized forecast has inconsistent capacities"),
            ),
            None => (
                slot.guest_bytes
                    .checked_sub(slot.shared_static_bytes)
                    .expect("forecast static credit exceeds capacity"),
                0,
            ),
        };
        Self {
            initialized: slot.initialized_memory.is_some(),
            // Image ownership separates preparation under changed configuration
            // even when source generation and raw capacities are unchanged.
            image_charge_id: slot.initialized_image_id,
            guest_bucket: guest / BASELINE_QUANTUM_BYTES,
            other_guest_bucket: other / BASELINE_QUANTUM_BYTES,
            host_bucket: slot.retained_host_bytes / BASELINE_QUANTUM_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ForecastSource {
    Preparation,
    ImageClone,
    GroupPrior,
    RoutePrior,
    Route,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ForecastSelection {
    pub(super) bytes: usize,
    pub(super) source: ForecastSource,
    matching_samples: usize,
    pub(super) baseline: Option<ForecastBaseline>,
}

#[derive(Clone, Debug)]
struct RouteDemandSample {
    baseline: ForecastBaseline,
    growth: usize,
    complete: bool,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RouteDemandPopulation {
    baseline: ForecastBaseline,
    complete_samples: usize,
    truncated_samples: usize,
    // Match histogram inspection: None is the unbounded overflow bucket.
    growth_p99_upper_bound: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct RouteDemandHistory {
    samples: VecDeque<RouteDemandSample>,
}

impl RouteDemandHistory {
    pub(super) fn inspect(&self) -> Vec<RouteDemandPopulation> {
        let mut populations = BTreeMap::<ForecastBaseline, (LogarithmicHistogram, usize)>::new();
        for sample in &self.samples {
            let (histogram, completed) = populations.entry(sample.baseline).or_default();
            histogram.observe(sample.growth);
            *completed += usize::from(sample.complete);
        }
        populations
            .into_iter()
            .map(|(baseline, (histogram, complete_samples))| {
                let bound = histogram
                    .quantile(99, 100)
                    .expect("retained forecast population is empty");
                RouteDemandPopulation {
                    baseline,
                    complete_samples,
                    truncated_samples: histogram.samples as usize - complete_samples,
                    growth_p99_upper_bound: (bound != usize::MAX).then_some(bound),
                }
            })
            .collect()
    }

    fn select(&self, baseline: ForecastBaseline, prior: usize) -> ForecastSelection {
        let mut histogram = LogarithmicHistogram::default();
        let mut matching_samples = 0;
        for sample in &self.samples {
            if sample.baseline == baseline {
                histogram.observe(sample.growth);
                matching_samples += usize::from(sample.complete);
            }
        }
        let learned = histogram.quantile(99, 100).unwrap_or(0);
        let enough = matching_samples >= MINIMUM_USEFUL_SAMPLES as usize;
        ForecastSelection {
            bytes: learned.saturating_add(learned.div_ceil(4)).max(if enough {
                MINIMUM_WARM_GROWTH_BYTES.min(prior)
            } else {
                prior
            }),
            source: if enough {
                ForecastSource::Route
            } else if histogram.samples > 0 {
                ForecastSource::RoutePrior
            } else {
                ForecastSource::GroupPrior
            },
            matching_samples,
            baseline: Some(baseline),
        }
    }

    pub(super) fn observe(
        &mut self,
        selection: ForecastSelection,
        growth: usize,
        complete: bool,
        prior: usize,
    ) {
        let Some(baseline) = selection.baseline else {
            // Fresh preparation and an image attached during this invocation
            // must not train a retained/image checkout population.
            return;
        };
        // A truncated execution supplies only a lower bound. Compare against
        // the current prediction, since another completion may have raised it
        // since checkout. Low failures neither age nor dilute useful history.
        if !complete && growth < self.select(baseline, prior).bytes {
            return;
        }
        if self.samples.len() == ROUTE_HISTORY_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(RouteDemandSample {
            baseline,
            growth,
            complete,
        });
    }
}

impl GeneratedMemoryController {
    pub(super) fn select_forecast_locked(
        &self,
        state: &ControllerState,
        identity: &FunctionMemoryIdentity,
        route: Option<&GeneratedMemoryRouteContext>,
        existing_slot: Option<GeneratedSlotId>,
        prior: usize,
        clone_peak_bytes: Option<usize>,
    ) -> ForecastSelection {
        let Some(slot_id) = existing_slot else {
            return ForecastSelection {
                // This predicts construction, not shared-memory credit. The
                // limiter still charges ordinary linear capacity until remap.
                bytes: clone_peak_bytes.unwrap_or_else(|| {
                    state.functions[identity]
                        .preparation_forecast(self.policy.cold_peak_growth_bytes)
                }),
                source: if clone_peak_bytes.is_some() {
                    ForecastSource::ImageClone
                } else {
                    ForecastSource::Preparation
                },
                matching_samples: 0,
                baseline: None,
            };
        };
        let baseline = ForecastBaseline::from_slot(&state.slots[&slot_id]);
        let history = route
            .and_then(|context| state.routes.get(&context.identity))
            .filter(|statistics| statistics.memory_identity.as_ref() == Some(identity));
        match history {
            Some(statistics) => statistics.demand.select(baseline, prior),
            None => ForecastSelection {
                bytes: prior,
                source: ForecastSource::GroupPrior,
                matching_samples: 0,
                baseline: Some(baseline),
            },
        }
    }
}
