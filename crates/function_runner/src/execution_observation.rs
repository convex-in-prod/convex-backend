#[cfg(feature = "static-hermes-wasmtime-gate")]
use std::sync::{
    atomic::{
        AtomicUsize,
        Ordering,
    },
    LazyLock,
};

use isolate::execution_observation::ExecutionObserver;

#[cfg(feature = "static-hermes-wasmtime-gate")]
use crate::QueryShadowRouteKey;

// The retained diagnostic ring is smaller. A bounded warmup population lets
// its final records include reused runtimes after process startup.
#[cfg(feature = "static-hermes-wasmtime-gate")]
const MAX_OBSERVED_PAIRS: usize = 64;
#[cfg(feature = "static-hermes-wasmtime-gate")]
static OBSERVED_PAIRS: AtomicUsize = AtomicUsize::new(0);

// Invalid diagnostic configuration must not fail an authoritative request.
// Report the fixed category once and leave collection disabled.
#[cfg(feature = "static-hermes-wasmtime-gate")]
static OBSERVED_ROUTE: LazyLock<Option<QueryShadowRouteKey>> = LazyLock::new(
    || match std::env::var("STATIC_HERMES_EXECUTION_OBSERVER_ROUTE") {
        Ok(value) => match QueryShadowRouteKey::parse(&value) {
            Ok(route) => Some(route),
            Err(_) => {
                tracing::error!("execution_observer_invalid_route_configuration");
                None
            },
        },
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            tracing::error!("execution_observer_invalid_route_configuration");
            None
        },
    },
);

pub(crate) struct ExecutionPairObservers {
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

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) fn try_observe(route: &QueryShadowRouteKey) -> Option<ExecutionPairObservers> {
    if OBSERVED_ROUTE.as_ref() != Some(route) {
        return None;
    }
    OBSERVED_PAIRS
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            (count < MAX_OBSERVED_PAIRS).then_some(count + 1)
        })
        .ok()?;
    Some(ExecutionPairObservers {
        primary: ExecutionObserver::default(),
        wasm: ExecutionObserver::default(),
        primary_transaction_duration: std::time::Duration::ZERO,
    })
}
