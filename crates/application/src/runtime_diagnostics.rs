//! Bounded per-attempt observations. Identities and paths are log fields only;
//! arguments, request metadata, environment and error text are never retained.

use std::{
    sync::{
        LazyLock,
        Weak,
    },
    time::{
        Duration,
        Instant,
    },
};

use common::{
    execution_context::{
        ExecutionContext,
        ExecutionId,
    },
    types::UdfType,
};
use isolate::execution_observation::{
    self,
    ExecutionObserver,
    ObservedTask,
    TaskKind,
};
use parking_lot::Mutex;
use udf::execution_observation::ExecutionObservation;

static SAMPLER: LazyLock<Mutex<Sampler>> = LazyLock::new(|| {
    Mutex::new(Sampler {
        window: Instant::now(),
        seen: 0,
        admitted: 0,
        active: Vec::new(),
    })
});

struct Sampler {
    window: Instant,
    seen: u64,
    admitted: usize,
    active: Vec<Weak<Mutex<ExecutionObservation>>>,
}

impl Sampler {
    fn admit(&mut self, now: Instant) -> Option<ExecutionObserver> {
        if now.duration_since(self.window) >= Duration::from_secs(60) {
            self.window = now;
            self.admitted = 0;
        }
        self.seen = self.seen.wrapping_add(1);
        if self.seen % 128 != 0 || self.admitted >= 60 {
            return None;
        }
        // Caller cancellation can leave an observed task queued or executing.
        // Keep its slot until the last task drops, without retaining its state.
        self.active.retain(|observer| observer.strong_count() > 0);
        if self.active.len() >= 8 {
            return None;
        }
        let observer = ExecutionObserver::default();
        self.admitted += 1;
        self.active.push(observer.downgrade());
        Some(observer)
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AttemptOutcome {
    Interrupted,
    Completed,
    DeveloperError,
    SystemError,
    OccRetry,
    WriteLimitRetry,
    AlreadyCommitted,
}

pub(crate) struct RuntimeDiagnostic {
    observer: ExecutionObserver,
    request_id: Option<String>,
    execution_id: ExecutionId,
    path: String,
    kind: UdfType,
    attempt: usize,
    started: Instant,
    pub(crate) outcome: AttemptOutcome,
    pub(crate) commit: Option<Duration>,
    pub(crate) selected_backoff: Option<Duration>,
    pub(crate) elapsed_backoff: Option<Duration>,
    pub(crate) conflict_wait: Option<Duration>,
}

impl RuntimeDiagnostic {
    pub(crate) fn start(
        context: &ExecutionContext,
        kind: UdfType,
        attempt: usize,
        path: impl FnOnce() -> String,
    ) -> Option<Self> {
        // Sampling must not put contending requests on a telemetry wait queue.
        if execution_observation::current().is_some() {
            return None;
        }
        let observer = SAMPLER.try_lock()?.admit(Instant::now())?;
        let mut path = path();
        path.truncate(path.floor_char_boundary(512));
        let request_id = context.request_id.as_str();
        // RequestId's parser accepts arbitrary strings. Only retain the fixed
        // ID representations used by the server; execution_id is always typed.
        let request_id = (matches!(request_id.len(), 16 | 32 | 36)
            && request_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'-'))
        .then(|| request_id.to_owned());
        Some(Self {
            observer,
            request_id,
            execution_id: context.execution_id,
            kind,
            path,
            attempt,
            started: Instant::now(),
            outcome: AttemptOutcome::Interrupted,
            commit: None,
            selected_backoff: None,
            elapsed_backoff: None,
            conflict_wait: None,
        })
    }

    pub(crate) fn task(&self) -> ObservedTask {
        self.observer.task(TaskKind::Lane)
    }
}

impl Drop for RuntimeDiagnostic {
    fn drop(&mut self) {
        // One bounded record per admitted attempt, including cancellation. No
        // queue of retained records can accumulate when telemetry is slow.
        if tracing::enabled!(tracing::Level::INFO) {
            // Keep one versioned JSON payload inside the message: text log
            // collectors preserve it without parsing Rust Debug or durations.
            // Null timings mean no completed measurement, including cancellation.
            let millis = |duration: Duration| duration.as_secs_f64() * 1000.0;
            let record = serde_json::json!({
                "version": 1,
                "requestId": self.request_id,
                "executionId": self.execution_id.to_string(),
                "functionPath": self.path,
                "udfType": self.kind.to_string(),
                "attempt": self.attempt,
                "outcome": self.outcome,
                "wallMs": millis(self.started.elapsed()),
                "commitMs": self.commit.map(millis),
                "selectedBackoffMs": self.selected_backoff.map(millis),
                "elapsedBackoffMs": self.elapsed_backoff.map(millis),
                "conflictWaitMs": self.conflict_wait.map(millis),
                "observation": self.observer.snapshot(),
            });
            tracing::info!("runtime_attempt_diagnostic {record}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_limits_survive_window_rollover_and_saturation() {
        let now = Instant::now();
        let mut sampler = Sampler {
            window: now,
            seen: 0,
            admitted: 0,
            active: Vec::new(),
        };
        for _ in 0..128 * 100 {
            drop(sampler.admit(now));
        }
        assert_eq!(sampler.admitted, 60);
        let later = now + Duration::from_secs(61);
        let mut tasks = Vec::new();
        for _ in 0..128 * 8 {
            if let Some(observer) = sampler.admit(later) {
                tasks.push(observer.task(TaskKind::Runtime));
                // The caller is gone, but even an unpolled task owns its slot.
                drop(observer);
            }
        }
        assert_eq!(tasks.len(), 8);
        for _ in 0..128 {
            assert!(sampler.admit(later).is_none());
        }
        drop(tasks.pop());
        let observer = (0..128).find_map(|_| sampler.admit(later)).unwrap();
        assert_eq!(sampler.active.len(), 8);
        drop(observer);
    }
}
