//! Bounded, process-lifetime function counters for periodic usage scrapes.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Arc,
        LazyLock,
    },
    time::{
        Duration,
        Instant,
    },
};

use common::types::UdfType;
use metrics::{
    prometheus::{
        core::{
            Collector,
            Desc,
        },
        proto::MetricFamily,
        Counter,
        CounterVec,
        IntCounterVec,
        IntGauge,
        Opts,
        Registry,
    },
    FUNCTION_USAGE_METRICS_REGISTRY,
};
use parking_lot::Mutex;
use usage_tracking::AggregatedFunctionUsageStats;

use crate::function_log::{
    FunctionExecution,
    UdfParams,
};

const MAX_FUNCTIONS: usize = 4096;
const MAX_LABEL_BYTES: usize = 1024;
const FULL_SCRAPE_INTERVAL: Duration = Duration::from_secs(60 * 60);

static METRICS: LazyLock<Option<Arc<FunctionUsageMetrics>>> = LazyLock::new(|| {
    let enabled = match std::env::var("FUNCTION_USAGE_METRICS_ENABLED") {
        Ok(value) if value == "true" => true,
        Ok(value) if value == "false" => false,
        Err(std::env::VarError::NotPresent) => false,
        _ => panic!("FUNCTION_USAGE_METRICS_ENABLED requires true or false"),
    };
    enabled.then(|| {
        FunctionUsageMetrics::new(&FUNCTION_USAGE_METRICS_REGISTRY, MAX_FUNCTIONS)
            .expect("Function usage metric initialization failed")
    })
});

pub(crate) fn initialize() {
    LazyLock::force(&METRICS);
}

pub(crate) fn record(execution: &FunctionExecution) {
    let Some(metrics) = &*METRICS else { return };
    let component = match &execution.params {
        UdfParams::Function { identifier, .. } => {
            if identifier.udf_path.is_system() {
                return;
            }
            identifier.component.to_string()
        },
        UdfParams::Http { identifier, .. } => {
            // Unmatched HTTP paths contain arbitrary client input, not route labels.
            if !identifier.matched {
                return;
            }
            String::new()
        },
    };
    let key = FunctionKey {
        component,
        function: execution.params.identifier_str(),
        udf_type: execution.udf_type.to_lowercase_string(),
    };
    metrics.record(
        key,
        &Observation {
            udf_type: execution.udf_type,
            cached: execution.cached_result,
            failed: execution.params.is_err(),
            will_retry: execution.will_retry,
            execution_seconds: execution.execution_time,
            user_execution_time: execution.user_execution_time,
            usage: &execution.usage_stats,
        },
    );
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FunctionKey {
    component: String,
    function: String,
    udf_type: &'static str,
}

struct Observation<'a> {
    udf_type: UdfType,
    cached: bool,
    failed: bool,
    will_retry: bool,
    execution_seconds: f64,
    user_execution_time: Option<Duration>,
    usage: &'a AggregatedFunctionUsageStats,
}

// Keep family registration, retained handles, and updates in one definition.
// Unlike the ordinary evictable metric vectors, these counters preserve idle
// functions until restart.
macro_rules! usage_counters {
    ($observation:ident; $($field:ident: $help:literal => $value:expr),+ $(,)?) => {
        struct Families { $($field: CounterVec),+ }
        #[derive(Clone)]
        struct Counters { dirty: Arc<AtomicBool>, $($field: Counter),+ }
        impl Families {
            fn new() -> anyhow::Result<Self> {
                $(let $field = CounterVec::new(
                    Opts::new(concat!("function_usage_", stringify!($field)), $help),
                    &["component", "function", "udf_type", "label_lengths"],
                )?;)+
                Ok(Self { $($field),+ })
            }
            fn desc(&self) -> Vec<&Desc> {
                let mut desc = Vec::new();
                $(desc.extend(self.$field.desc());)+
                desc
            }
            fn for_function(&self, key: &FunctionKey) -> Counters {
                // The pinned metric vector hashes concatenated label values without
                // separators. Lengths distinguish root and component functions whose
                // concatenated paths are equal while preserving their canonical labels.
                let label_lengths = format!(
                    "{}:{}:{}", key.component.len(), key.function.len(), key.udf_type.len(),
                );
                let labels = &[
                    key.component.as_str(), key.function.as_str(), key.udf_type,
                    label_lengths.as_str(),
                ];
                Counters {
                    dirty: Arc::new(AtomicBool::new(true)),
                    $($field: self.$field.with_label_values(labels)),+
                }
            }
        }
        impl Counters {
            fn record(&self, $observation: &Observation<'_>) {
                $(self.$field.inc_by($value);)+
                // Publish after updating counters. A scrape clears this flag before
                // reading values, so a concurrent update is included now or next time.
                self.dirty.store(true, Ordering::Release);
            }
            fn collect(&self) -> Vec<MetricFamily> {
                let mut families = Vec::new();
                $(families.extend(self.$field.collect());)+
                families
            }
        }
    };
}

usage_counters!(e;
    calls_total: "Terminal function completions including cached queries" =>
        f64::from(u8::from(!e.will_retry)),
    failures_total: "Terminal function completions with errors" =>
        f64::from(u8::from(!e.will_retry && e.failed)),
    attempts_total: "Non-cached function execution attempts" =>
        f64::from(u8::from(!e.cached)),
    retries_total: "Function execution attempts requesting a retry" =>
        f64::from(u8::from(e.will_retry)),
    cache_hits_total: "Query completions served from cache" =>
        f64::from(u8::from(e.udf_type == UdfType::Query && e.cached)),
    cache_misses_total: "Query completions executed without a cache hit" =>
        f64::from(u8::from(e.udf_type == UdfType::Query && !e.cached)),
    execution_seconds_total: "Execution seconds across non-cached attempts" =>
        if e.cached { 0.0 } else { e.execution_seconds },
    user_execution_seconds_total: "Measured user execution seconds excluding selected waits" =>
        if e.cached { 0.0 } else { e.user_execution_time.map_or(0.0, |d| d.as_secs_f64()) },
    user_execution_measurements_total: "Non-cached attempts with measured user execution time" =>
        f64::from(u8::from(!e.cached && e.user_execution_time.is_some())),
    database_read_bytes_total: "Attributed database bandwidth bytes read" =>
        if e.cached { 0.0 } else { e.usage.database_read_bytes as f64 },
    database_write_bytes_total: "Attributed database bandwidth bytes written" =>
        if e.cached { 0.0 } else { e.usage.database_write_bytes as f64 },
    database_io_read_bytes_total: "Attributed database IO bytes read" =>
        if e.cached { 0.0 } else { e.usage.database_io_read_bytes as f64 },
    database_io_write_bytes_total: "Attributed database IO bytes written" =>
        if e.cached { 0.0 } else { e.usage.database_io_write_bytes as f64 },
    database_read_documents_total: "Attributed database documents read" =>
        if e.cached { 0.0 } else { e.usage.database_read_documents as f64 },
    database_write_documents_total: "Attributed database documents written" =>
        if e.cached { 0.0 } else { e.usage.database_write_documents as f64 },
    database_write_index_rows_total: "Attributed database index rows written" =>
        if e.cached { 0.0 } else { e.usage.database_write_index_rows as f64 },
    storage_read_bytes_total: "Attributed file storage bytes read" =>
        if e.cached { 0.0 } else { e.usage.storage_read_bytes as f64 },
    storage_write_bytes_total: "Attributed file storage bytes written" =>
        if e.cached { 0.0 } else { e.usage.storage_write_bytes as f64 },
    network_egress_bytes_total: "Attributed network egress bytes" =>
        if e.cached { 0.0 } else { e.usage.network_egress_bytes as f64 },
);

struct FunctionUsageMetrics {
    families: Families,
    functions: Mutex<BTreeMap<FunctionKey, Counters>>,
    last_full_scrape: Mutex<Option<Instant>>,
    max_functions: usize,
    retained_functions: IntGauge,
    dropped: IntCounterVec,
}

impl FunctionUsageMetrics {
    fn new(registry: &Registry, max_functions: usize) -> anyhow::Result<Arc<Self>> {
        let families = Families::new()?;
        let retained_functions = IntGauge::new(
            "function_usage_retained_functions",
            "Retained function label combinations",
        )?;
        registry.register(Box::new(retained_functions.clone()))?;
        let dropped = IntCounterVec::new(
            Opts::new(
                "function_usage_dropped_observations_total",
                "Omitted function usage observations",
            ),
            &["reason"],
        )?;
        for reason in ["capacity", "label_length", "contention", "invalid_duration"] {
            dropped.with_label_values(&[reason]);
        }
        registry.register(Box::new(dropped.clone()))?;
        let metrics = Arc::new(Self {
            families,
            functions: Mutex::new(BTreeMap::new()),
            last_full_scrape: Mutex::new(None),
            max_functions,
            retained_functions,
            dropped,
        });
        registry.register(Box::new(UsageCollector(metrics.clone())))?;
        Ok(metrics)
    }

    fn collect_at(&self, now: Instant) -> Vec<MetricFamily> {
        let counters: Vec<_> = {
            let mut last_full = self.last_full_scrape.lock();
            let full =
                last_full.is_none_or(|last| now.duration_since(last) >= FULL_SCRAPE_INTERVAL);
            let functions = self.functions.lock();
            let counters = functions
                .values()
                .filter_map(|counters| {
                    let dirty = counters.dirty.swap(false, Ordering::AcqRel);
                    (dirty || full).then(|| counters.clone())
                })
                .collect();
            if full {
                *last_full = Some(now);
            }
            counters
        };
        // Build metric objects only for selected functions, outside the admission lock.
        // The hourly snapshot repairs a response lost after clearing the dirty flags.
        counters.iter().flat_map(Counters::collect).collect()
    }

    fn record(&self, key: FunctionKey, observation: &Observation<'_>) {
        if key.component.len() > MAX_LABEL_BYTES || key.function.len() > MAX_LABEL_BYTES {
            self.dropped.with_label_values(&["label_length"]).inc();
            return;
        }
        // Invalid telemetry never poisons a cumulative counter or fails the function.
        if !observation.execution_seconds.is_finite() || observation.execution_seconds < 0.0 {
            self.dropped.with_label_values(&["invalid_duration"]).inc();
            return;
        }
        let counters = {
            // Usage is best effort: admission contention cannot stall function completion.
            let Some(mut functions) = self.functions.try_lock() else {
                self.dropped.with_label_values(&["contention"]).inc();
                return;
            };
            if let Some(counters) = functions.get(&key) {
                counters.clone()
            } else {
                if functions.len() == self.max_functions {
                    self.dropped.with_label_values(&["capacity"]).inc();
                    return;
                }
                let counters = self.families.for_function(&key);
                functions.insert(key, counters.clone());
                self.retained_functions.inc();
                counters
            }
        };
        counters.record(observation);
    }
}

struct UsageCollector(Arc<FunctionUsageMetrics>);

impl Collector for UsageCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.0.families.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.0.collect_at(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metrics::prometheus::{
        Encoder,
        TextEncoder,
    };

    use super::*;

    fn key(function: &str) -> FunctionKey {
        FunctionKey {
            component: String::new(),
            function: function.into(),
            udf_type: "query",
        }
    }

    fn observation(usage: &AggregatedFunctionUsageStats) -> Observation<'_> {
        Observation {
            udf_type: UdfType::Query,
            cached: false,
            failed: false,
            will_retry: false,
            execution_seconds: 2.0,
            user_execution_time: Some(Duration::from_millis(500)),
            usage,
        }
    }

    fn calls(families: &[MetricFamily]) -> BTreeMap<String, f64> {
        families
            .iter()
            .filter(|family| family.name() == "function_usage_calls_total")
            .flat_map(|family| family.get_metric())
            .map(|metric| {
                let function = metric
                    .get_label()
                    .iter()
                    .find(|label| label.name() == "function")
                    .unwrap()
                    .value()
                    .to_owned();
                (function, metric.get_counter().value())
            })
            .collect()
    }

    #[test]
    fn sparse_scrapes_skip_idle_functions_and_hourly_snapshots_repair_lost_responses() {
        let metrics = FunctionUsageMetrics::new(&Registry::new(), 2).unwrap();
        let usage = AggregatedFunctionUsageStats::default();
        let e = observation(&usage);
        let now = Instant::now();
        metrics.record(key("active:run"), &e);
        metrics.record(key("idle:run"), &e);
        assert_eq!(
            calls(&metrics.collect_at(now)),
            BTreeMap::from([("active:run".into(), 1.0), ("idle:run".into(), 1.0),])
        );
        assert!(metrics
            .collect_at(now + Duration::from_secs(300))
            .is_empty());
        metrics.record(key("active:run"), &e);
        assert_eq!(
            calls(&metrics.collect_at(now + Duration::from_secs(600))),
            BTreeMap::from([("active:run".into(), 2.0)])
        );
        metrics.record(key("idle:run"), &e);
        // The response is collected but never received by the scraper.
        metrics.collect_at(now + Duration::from_secs(900));
        assert_eq!(
            calls(&metrics.collect_at(now + FULL_SCRAPE_INTERVAL)),
            BTreeMap::from([("active:run".into(), 2.0), ("idle:run".into(), 2.0),])
        );
        assert!(metrics
            .collect_at(now + FULL_SCRAPE_INTERVAL + Duration::from_secs(300))
            .is_empty());
    }

    #[test]
    fn cached_results_do_not_repeat_original_work() {
        let registry = Registry::new();
        let metrics = FunctionUsageMetrics::new(&registry, 2).unwrap();
        let usage = AggregatedFunctionUsageStats {
            database_read_bytes: 900,
            storage_read_bytes: 80,
            network_egress_bytes: 70,
            ..Default::default()
        };
        let mut e = observation(&usage);
        metrics.record(key("orders:list"), &e);
        e.cached = true;
        metrics.record(key("orders:list"), &e);
        let functions = metrics.functions.lock();
        let c = &functions[&key("orders:list")];
        assert_eq!(c.calls_total.get(), 2.0);
        assert_eq!(c.attempts_total.get(), 1.0);
        assert_eq!(c.cache_hits_total.get(), 1.0);
        assert_eq!(c.cache_misses_total.get(), 1.0);
        assert_eq!(c.execution_seconds_total.get(), 2.0);
        assert_eq!(c.user_execution_seconds_total.get(), 0.5);
        assert_eq!(c.user_execution_measurements_total.get(), 1.0);
        assert_eq!(c.database_read_bytes_total.get(), 900.0);
        assert_eq!(c.storage_read_bytes_total.get(), 80.0);
        assert_eq!(c.network_egress_bytes_total.get(), 70.0);
    }

    #[test]
    fn retries_and_missing_timing_have_separate_denominators() {
        let metrics = FunctionUsageMetrics::new(&Registry::new(), 2).unwrap();
        let usage = AggregatedFunctionUsageStats::default();
        let mut e = observation(&usage);
        e.udf_type = UdfType::Mutation;
        e.failed = true;
        e.will_retry = true;
        metrics.record(key("orders:update"), &e);
        e.will_retry = false;
        e.user_execution_time = None;
        metrics.record(key("orders:update"), &e);
        let functions = metrics.functions.lock();
        let c = &functions[&key("orders:update")];
        assert_eq!(c.calls_total.get(), 1.0);
        assert_eq!(c.failures_total.get(), 1.0);
        assert_eq!(c.attempts_total.get(), 2.0);
        assert_eq!(c.retries_total.get(), 1.0);
        assert_eq!(c.user_execution_measurements_total.get(), 1.0);
        assert_eq!(c.cache_misses_total.get(), 0.0);
    }

    #[test]
    fn capacity_preserves_existing_series_and_labels_are_escaped() {
        let registry = Registry::new();
        let metrics = FunctionUsageMetrics::new(&registry, 1).unwrap();
        let usage = AggregatedFunctionUsageStats::default();
        let e = observation(&usage);
        let function = "module:quoted\"function";
        metrics.record(key(function), &e);
        metrics.record(key("other:call"), &e);
        metrics.record(key(function), &e);
        metrics.record(key(&"x".repeat(MAX_LABEL_BYTES + 1)), &e);
        assert_eq!(metrics.retained_functions.get(), 1);
        assert_eq!(metrics.dropped.with_label_values(&["capacity"]).get(), 1);
        assert_eq!(
            metrics.dropped.with_label_values(&["label_length"]).get(),
            1
        );
        assert_eq!(
            metrics.functions.lock()[&key(function)].calls_total.get(),
            2.0
        );
        let mut text = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut text)
            .unwrap();
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("function=\"module:quoted\\\"function\""));
        assert!(!text.contains("other:call"));
        // Repeated scrapes do not clear retained cumulative values.
        registry.gather();
        assert_eq!(
            metrics.functions.lock()[&key(function)].calls_total.get(),
            2.0
        );
    }

    #[test]
    fn component_and_root_paths_with_equal_concatenations_remain_separate() {
        let registry = Registry::new();
        let metrics = FunctionUsageMetrics::new(&registry, 2).unwrap();
        let root = key("orderslist:run");
        let component = FunctionKey {
            component: "orders".into(),
            function: "list:run".into(),
            udf_type: "query",
        };
        let usage = AggregatedFunctionUsageStats::default();
        metrics.record(root.clone(), &observation(&usage));
        metrics.record(component.clone(), &observation(&usage));
        metrics.record(component.clone(), &observation(&usage));
        let functions = metrics.functions.lock();
        assert_eq!(functions[&root].calls_total.get(), 1.0);
        assert_eq!(functions[&component].calls_total.get(), 2.0);
        drop(functions);
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "function_usage_calls_total")
            .unwrap();
        assert_eq!(family.get_metric().len(), 2);
    }

    #[test]
    fn concurrent_updates_are_counted_or_explicitly_dropped() {
        let metrics = FunctionUsageMetrics::new(&Registry::new(), 1).unwrap();
        let usage = AggregatedFunctionUsageStats::default();
        metrics.record(key("orders:list"), &observation(&usage));
        let done = Arc::new(AtomicBool::new(false));
        let scraper = {
            let metrics = metrics.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let mut largest: f64 = 0.0;
                while !done.load(Ordering::Acquire) {
                    for value in calls(&metrics.collect_at(Instant::now())).values() {
                        largest = largest.max(*value);
                    }
                    std::thread::yield_now();
                }
                largest
            })
        };
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let metrics = metrics.clone();
                std::thread::spawn(move || {
                    let usage = AggregatedFunctionUsageStats::default();
                    for _ in 0..1000 {
                        metrics.record(key("orders:list"), &observation(&usage));
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        done.store(true, Ordering::Release);
        let mut exported = scraper.join().unwrap();
        for value in calls(&metrics.collect_at(Instant::now())).values() {
            exported = exported.max(*value);
        }
        let accepted = metrics.functions.lock()[&key("orders:list")]
            .calls_total
            .get();
        let dropped = metrics.dropped.with_label_values(&["contention"]).get();
        assert_eq!(accepted + dropped as f64, 8001.0);
        assert_eq!(exported, accepted);
    }
}
