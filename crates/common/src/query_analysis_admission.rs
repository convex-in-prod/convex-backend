use std::sync::Arc;

use tokio::sync::{
    OwnedSemaphorePermit,
    Semaphore,
    TryAcquireError,
};

/// One application-scoped capacity gate shared by degradable query leaders
/// and isolate module analysis.
///
/// Degradable queries require an immediate decision so the sync protocol can
/// retain stale results instead of adding a waiter. Analysis waits fairly for
/// the same permits. Consequently, analysis borrows elastic capacity instead
/// of adding work above the configured degradable-query ceiling.
#[derive(Clone)]
pub struct QueryAnalysisAdmission {
    capacity: usize,
    semaphore: Arc<Semaphore>,
}

/// A permit from [`QueryAnalysisAdmission`].
pub struct QueryAnalysisPermit {
    _permit: OwnedSemaphorePermit,
}

impl QueryAnalysisAdmission {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "query-analysis capacity must be positive");
        Self {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity)),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Attempts immediate admission for a degradable query leader.
    ///
    /// A queued analysis waiter receives a released permit before a later
    /// immediate acquisition can take it because the underlying semaphore is
    /// fair.
    pub fn try_acquire_degradable(&self) -> Option<QueryAnalysisPermit> {
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return None,
            Err(TryAcquireError::Closed) => {
                panic!("query-analysis admission semaphore unexpectedly closed")
            },
        };
        Some(QueryAnalysisPermit { _permit: permit })
    }

    /// Waits fairly for capacity for one isolate module analysis attempt.
    pub async fn acquire_analysis(&self) -> QueryAnalysisPermit {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("query-analysis admission semaphore unexpectedly closed");
        QueryAnalysisPermit { _permit: permit }
    }
}

tokio::task_local! {
    pub static DEPLOYMENT_ANALYSIS_JOB: DeploymentAnalysisPermit;
    // Only opt-in operations install this signal. Legacy subscriber loss keeps
    // the existing Node invocation owner running to its terminal boundary.
    pub static DEPLOYMENT_OPERATION_CANCELLATION: tokio::sync::watch::Receiver<bool>;
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum DeploymentAnalysisKind {
    Deploy,
    Preflight,
}

/// One whole analysis job per application, with a finite queue and retained
/// source budget. Deployment wins queued preflight, with a four-job burst
/// limit so repeated deployment cannot starve preflight indefinitely.
#[derive(Clone, Default)]
pub struct DeploymentAnalysisAdmission(Arc<DeploymentAnalysisState>);

#[derive(Default)]
struct DeploymentAnalysisState {
    inner: parking_lot::Mutex<DeploymentAnalysisQueue>,
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct DeploymentAnalysisQueue {
    next_id: u64,
    waiting: std::collections::VecDeque<(u64, DeploymentAnalysisKind)>,
    active: Option<u64>,
    deploy_burst: usize,
    retained_bytes: usize,
}

metrics::register_convex_gauge!(
    DEPLOYMENT_ANALYSIS_JOBS_INFO,
    "Whole analysis jobs, separate from root permits; one application per process",
    &["state"]
);
metrics::register_convex_histogram!(
    DEPLOYMENT_ANALYSIS_JOB_WAIT_SECONDS,
    "Whole-job admission wait including immediate, rejected, timed-out and canceled attempts",
    &["kind", "status"]
);

impl DeploymentAnalysisQueue {
    fn publish_metrics(&self) {
        for (state, count) in [
            ("active", usize::from(self.active.is_some())),
            ("waiting", self.waiting.len()),
        ] {
            metrics::log_gauge_with_labels(
                &DEPLOYMENT_ANALYSIS_JOBS_INFO,
                count as f64,
                vec![metrics::StaticMetricLabel::new("state", state)],
            );
        }
    }
}

#[derive(Clone)]
pub struct DeploymentAnalysisPermit(Arc<DeploymentAnalysisOwner>);

struct DeploymentAnalysisOwner {
    state: Arc<DeploymentAnalysisState>,
    id: u64,
    bytes: usize,
}

impl Drop for DeploymentAnalysisOwner {
    fn drop(&mut self) {
        let mut inner = self.state.inner.lock();
        inner.waiting.retain(|(id, _)| *id != self.id);
        if inner.active == Some(self.id) {
            inner.active = None;
        }
        inner.retained_bytes = inner
            .retained_bytes
            .checked_sub(self.bytes)
            .expect("deployment analysis byte accounting underflow");
        inner.publish_metrics();
        drop(inner);
        self.state.changed.notify_waiters();
    }
}

impl DeploymentAnalysisAdmission {
    pub async fn acquire(
        &self,
        kind: DeploymentAnalysisKind,
        bytes: usize,
    ) -> anyhow::Result<DeploymentAnalysisPermit> {
        use errors::ErrorMetadata;
        let mut timer = metrics::CancelableTimer::new(&DEPLOYMENT_ANALYSIS_JOB_WAIT_SECONDS);
        timer.add_label(metrics::StaticMetricLabel::new(
            "kind",
            match kind {
                DeploymentAnalysisKind::Deploy => "deploy",
                DeploymentAnalysisKind::Preflight => "preflight",
            },
        ));
        let owner = {
            let mut inner = self.0.inner.lock();
            inner.publish_metrics();
            if inner.waiting.len() >= 4
                || bytes > (768 * 1024 * 1024usize).saturating_sub(inner.retained_bytes)
            {
                timer.finish_with(if inner.waiting.len() >= 4 {
                    "queue_full"
                } else {
                    "byte_budget"
                });
                anyhow::bail!(ErrorMetadata::overloaded(
                    "DeploymentAnalysisBusy",
                    "Deployment analysis capacity is full. Retry the request."
                ));
            }
            let id = inner.next_id;
            inner.next_id = id
                .checked_add(1)
                .expect("deployment analysis identity overflow");
            inner.retained_bytes += bytes;
            inner.waiting.push_back((id, kind));
            inner.publish_metrics();
            DeploymentAnalysisPermit(Arc::new(DeploymentAnalysisOwner {
                state: self.0.clone(),
                id,
                bytes,
            }))
        };
        let wait = async {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let mut inner = self.0.inner.lock();
                    let preferred = if inner.deploy_burst >= 4 {
                        DeploymentAnalysisKind::Preflight
                    } else {
                        DeploymentAnalysisKind::Deploy
                    };
                    let next = inner
                        .waiting
                        .iter()
                        .find(|(_, kind)| *kind == preferred)
                        .or_else(|| inner.waiting.front())
                        .copied();
                    if inner.active.is_none() && next.is_some_and(|(id, _)| id == owner.0.id) {
                        inner.waiting.retain(|(id, _)| *id != owner.0.id);
                        inner.active = Some(owner.0.id);
                        inner.publish_metrics();
                        inner.deploy_burst = if kind == DeploymentAnalysisKind::Deploy {
                            inner.deploy_burst.saturating_add(1)
                        } else {
                            0
                        };
                        return;
                    }
                }
                changed.await;
            }
        };
        if tokio::time::timeout(std::time::Duration::from_secs(30), wait)
            .await
            .is_err()
        {
            timer.finish_with("timeout");
            anyhow::bail!(ErrorMetadata::overloaded(
                "DeploymentAnalysisBusy",
                "Timed out waiting for deployment analysis capacity. Retry the request.",
            ));
        }
        timer.finish(true);
        Ok(owner)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::QueryAnalysisAdmission;

    #[tokio::test]
    async fn deployment_precedes_queued_preflight_and_cancellation_releases_ownership() {
        use super::{
            DeploymentAnalysisAdmission,
            DeploymentAnalysisKind,
        };
        let gate = DeploymentAnalysisAdmission::default();
        let active = gate
            .acquire(DeploymentAnalysisKind::Preflight, 1024)
            .await
            .unwrap();
        let dispatched_root = active.clone();
        let mut preflight = Box::pin(gate.acquire(DeploymentAnalysisKind::Preflight, 2048));
        let mut deployment = Box::pin(gate.acquire(DeploymentAnalysisKind::Deploy, 4096));
        assert!(futures::poll!(&mut preflight).is_pending());
        assert!(futures::poll!(&mut deployment).is_pending());
        drop(active);
        assert!(futures::poll!(&mut deployment).is_pending());
        drop(dispatched_root);
        assert!(futures::poll!(&mut preflight).is_pending());
        let active = deployment.await.unwrap();
        drop(preflight);
        assert_eq!(gate.0.inner.lock().retained_bytes, 4096);
        drop(active);
        assert_eq!(gate.0.inner.lock().retained_bytes, 0);
        assert!(gate.0.inner.lock().waiting.is_empty());
    }

    #[tokio::test]
    async fn deployment_analysis_rejects_excess_bytes_without_leaking_waiters() {
        use super::{
            DeploymentAnalysisAdmission,
            DeploymentAnalysisKind,
        };
        let gate = DeploymentAnalysisAdmission::default();
        assert!(gate
            .acquire(DeploymentAnalysisKind::Deploy, 769 * 1024 * 1024)
            .await
            .is_err());
        let active = gate
            .acquire(DeploymentAnalysisKind::Deploy, 768 * 1024 * 1024)
            .await
            .unwrap();
        assert!(gate
            .acquire(DeploymentAnalysisKind::Preflight, 1)
            .await
            .is_err());
        assert!(gate.0.inner.lock().waiting.is_empty());
        drop(active);
        assert_eq!(gate.0.inner.lock().retained_bytes, 0);
    }

    #[tokio::test]
    async fn queued_preflight_progresses_after_four_deployments() {
        use super::{
            DeploymentAnalysisAdmission,
            DeploymentAnalysisKind,
        };
        let gate = DeploymentAnalysisAdmission::default();
        let mut active = gate
            .acquire(DeploymentAnalysisKind::Deploy, 1)
            .await
            .unwrap();
        let mut preflight = Box::pin(gate.acquire(DeploymentAnalysisKind::Preflight, 1));
        assert!(futures::poll!(&mut preflight).is_pending());
        for _ in 0..3 {
            let mut deployment = Box::pin(gate.acquire(DeploymentAnalysisKind::Deploy, 1));
            assert!(futures::poll!(&mut deployment).is_pending());
            drop(active);
            assert!(futures::poll!(&mut preflight).is_pending());
            active = deployment.await.unwrap();
        }
        let mut fifth = Box::pin(gate.acquire(DeploymentAnalysisKind::Deploy, 1));
        assert!(futures::poll!(&mut fifth).is_pending());
        drop(active);
        assert!(futures::poll!(&mut fifth).is_pending());
        let preflight = preflight.await.unwrap();
        drop(preflight);
        drop(fifth.await.unwrap());
        assert_eq!(gate.0.inner.lock().retained_bytes, 0);
    }

    #[tokio::test]
    async fn spawned_job_scope_keeps_admission_until_the_child_future_drops() {
        use super::{
            DeploymentAnalysisAdmission,
            DeploymentAnalysisKind,
            DEPLOYMENT_ANALYSIS_JOB,
        };
        let gate = DeploymentAnalysisAdmission::default();
        let permit = gate
            .acquire(DeploymentAnalysisKind::Deploy, 1024)
            .await
            .unwrap();
        let (release, finish) = tokio::sync::oneshot::channel();
        let child = DEPLOYMENT_ANALYSIS_JOB
            .scope(permit, async {
                let lease = DEPLOYMENT_ANALYSIS_JOB.with(Clone::clone);
                tokio::spawn(DEPLOYMENT_ANALYSIS_JOB.scope(lease, async move {
                    finish.await.unwrap();
                }))
            })
            .await;
        let mut next = Box::pin(gate.acquire(DeploymentAnalysisKind::Deploy, 2048));
        // The parent scope is gone and the child has not been polled yet.
        assert!(futures::poll!(&mut next).is_pending());
        assert_eq!(gate.0.inner.lock().retained_bytes, 3072);
        release.send(()).unwrap();
        child.await.unwrap();
        drop(next.await.unwrap());
        assert_eq!(gate.0.inner.lock().retained_bytes, 0);
    }

    #[tokio::test]
    async fn analysis_borrows_from_degradable_capacity() {
        let admission = QueryAnalysisAdmission::new(2);
        let degradable = admission
            .try_acquire_degradable()
            .expect("first degradable query was not admitted");
        let analysis = admission.acquire_analysis().await;

        assert!(admission.try_acquire_degradable().is_none());

        drop(analysis);
        let replacement = admission
            .try_acquire_degradable()
            .expect("released analysis reservation was not returned");
        drop(replacement);
        drop(degradable);
    }

    #[tokio::test]
    async fn queued_analysis_precedes_new_degradable_admission() {
        let admission = QueryAnalysisAdmission::new(1);
        let degradable = admission
            .try_acquire_degradable()
            .expect("degradable query was not admitted");
        let mut analysis = std::pin::pin!(admission.acquire_analysis());
        assert!(futures::poll!(&mut analysis).is_pending());

        drop(degradable);
        // The fair semaphore assigns the released permit to the queued
        // analysis future before that future is polled again.
        assert!(admission.try_acquire_degradable().is_none());
        let analysis = tokio::time::timeout(Duration::from_secs(1), &mut analysis)
            .await
            .expect("queued analysis did not receive released capacity");
        drop(analysis);

        assert!(admission.try_acquire_degradable().is_some());
    }
}
