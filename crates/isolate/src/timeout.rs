use std::{
    collections::HashMap,
    ops::{
        Deref,
        DerefMut,
    },
    sync::Arc,
    time::Duration,
};

use anyhow::Context as _;
use async_broadcast::broadcast;
use common::{
    runtime::{
        Runtime,
        SpawnHandle,
    },
    types::UdfType,
};
use errors::ErrorMetadata;
use futures::{
    future::{
        self,
        Either,
    },
    pin_mut,
    Future,
    FutureExt,
};
use parking_lot::Mutex;
use tokio::select;

use crate::{
    concurrency_limiter::SuspendedPermit,
    environment::JsEnvironment,
    metrics,
    termination::{
        ContextId,
        ExecutionHandle,
        IsolateTerminationReason,
        TerminationReason,
    },
    ConcurrencyPermit,
};

#[derive(Debug, thiserror::Error)]
#[error("Timed out")]
pub(crate) struct TimeoutExpired;

pub const SYSTEM_TIMEOUT_ERROR_MESSAGE: &str =
    "Your request timed out performing too many system operations.";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PauseReason {
    DatabaseSyscall { name: String },
    ConcurrencyPermitReacquire,
    GeneratedLifecycleBarrier,
    UdfInitialize,
    LoadModule,
}

impl PauseReason {
    pub fn as_str(&self) -> String {
        match self {
            Self::DatabaseSyscall { name } => format!("database_syscall({name})"),
            Self::ConcurrencyPermitReacquire => "concurrency_permit_reacquire".to_string(),
            Self::GeneratedLifecycleBarrier => "generated_lifecycle_barrier".to_string(),
            Self::UdfInitialize => "udf_initialize".to_string(),
            Self::LoadModule => "load_module".to_string(),
        }
    }
}

/// A `Timeout` owns a deadline and a background job that terminates an
/// `ExecutionHandle` after some time has passed. An executing guest can also
/// check the same deadline synchronously. The holder of a `Timeout` can
/// temporarily pause the termination countdown with [`Timeout::pause`] and then
/// resume time tracking by dropping the returned guard.
///
/// If the higher level operation succeeds, call `Timeout::finish` to cancel the
/// background job and prevent it from terminating the isolate.
pub struct Timeout<RT: Runtime> {
    handle: Box<dyn SpawnHandle>,
    inner: Arc<Mutex<TimeoutInner<RT>>>,
    done_rx: async_broadcast::Receiver<()>,
    deadline_changed: async_broadcast::Sender<()>,
    pub permit: Option<ConcurrencyPermit>,
}

struct TimeoutInner<RT: Runtime> {
    rt: RT,
    // Both the watchdog and an executing guest can observe expiration. Publish
    // it once, while holding the same lock that protects deadline rearming.
    // Callbacks must not reenter Timeout or acquire a lock whose holder does.
    terminate: Option<Box<dyn FnOnce(TerminationReason) + Send>>,

    start: tokio::time::Instant,
    timeout: Option<Duration>,

    // How long has the timeout been in the paused state?
    pause_elapsed: Duration,
    // A Static Hermes prepared entry starts a fresh active deadline without
    // erasing the outer system-time accounting accumulated before activation.
    active_pause_elapsed_base: Duration,
    max_time_paused: Option<Duration>,

    state: TimeoutState,
    pause_breakdown: HashMap<PauseReason, (usize, Duration)>,
}

impl<RT: Runtime> TimeoutInner<RT> {
    fn deadline(&self) -> Option<tokio::time::Instant> {
        match &self.state {
            TimeoutState::Running => {
                // Extend the deadline by the time spent paused.
                Some(
                    self.start
                        + self.timeout?
                        + self
                            .pause_elapsed
                            .saturating_sub(self.active_pause_elapsed_base),
                )
            },
            TimeoutState::Paused { pause_start, .. } => self
                .max_time_paused
                .map(|limit| *pause_start + limit.saturating_sub(self.pause_elapsed)),
            TimeoutState::Finished => None,
        }
    }

    fn check_expired(&mut self, now: tokio::time::Instant) {
        if self.terminate.is_none() || self.timeout.is_none() {
            return;
        }
        if self.deadline().is_none_or(|deadline| now < deadline) {
            return;
        }
        let reason = match &self.state {
            TimeoutState::Running => {
                metrics::log_user_timeout();
                IsolateTerminationReason::UserTimeout(self.timeout.expect("armed timeout missing"))
            },
            TimeoutState::Paused {
                pause_start,
                reason,
                ..
            } => {
                let max_time_paused = self.max_time_paused.expect("paused deadline missing");
                let current_pause_duration = now - *pause_start;
                let total_pause_elapsed = self.pause_elapsed + current_pause_duration;
                metrics::log_system_timeout();

                let mut reasons: Vec<_> = self.pause_breakdown.iter().collect();
                reasons.sort_by_key(|(_, (_, duration))| std::cmp::Reverse(*duration));
                let reasons_str = reasons
                    .iter()
                    .map(|(reason, (count, duration))| {
                        format!("{}={}({:?})", reason.as_str(), count, duration)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                tracing::warn!(
                    "SystemTimeout: pause breakdown: {} ({:?}). Final pause {} ({:?})",
                    reasons_str,
                    total_pause_elapsed,
                    reason.as_str(),
                    current_pause_duration,
                );

                IsolateTerminationReason::SystemTimeout(max_time_paused)
            },
            TimeoutState::Finished => unreachable!("finished timeout has no deadline"),
        };
        self.terminate
            .take()
            .expect("timeout termination callback missing")(reason.into());
    }

    fn check_expired_or_wait(&mut self) -> Option<impl Future<Output = ()> + 'static + use<RT>> {
        self.check_expired(self.rt.monotonic_now());
        if self.terminate.is_none() || self.timeout.is_none() {
            return None;
        }
        let expired = match self.deadline() {
            Some(deadline) => Either::Left(
                self.rt
                    .wait(deadline.saturating_duration_since(self.rt.monotonic_now())),
            ),
            None => Either::Right(future::pending()),
        };
        match &self.state {
            TimeoutState::Running => Some(Either::Left(expired)),
            TimeoutState::Paused { pause_done, .. } => {
                let mut pause_done = pause_done.clone();
                Some(Either::Right(async move {
                    let _ = future::select(expired, pause_done.recv()).await;
                }))
            },
            TimeoutState::Finished => None,
        }
    }
}

#[derive(Debug)]
enum TimeoutState {
    Running,
    Paused {
        pause_start: tokio::time::Instant,
        pause_done: async_broadcast::Receiver<()>,
        reason: PauseReason,
    },
    Finished,
}

impl<RT: Runtime> Drop for Timeout<RT> {
    fn drop(&mut self) {
        self.finish();
    }
}

// We default to counting everything as user time but we exempt async syscalls
// from the user timeout and count them as system time instead.
impl<RT: Runtime> Timeout<RT> {
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub(crate) fn for_static_hermes_gate(rt: RT, permit: ConcurrencyPermit) -> Self {
        let (done_tx, done_rx) = broadcast(1);
        let (deadline_changed, _deadline_changed_rx) = broadcast(1);
        let handle = rt.spawn("static_hermes_gate_timeout", async move {
            let _done_tx = done_tx;
            future::pending::<()>().await;
        });
        Self {
            handle,
            inner: Arc::new(Mutex::new(TimeoutInner {
                rt: rt.clone(),
                terminate: None,
                start: rt.monotonic_now(),
                timeout: None,
                pause_elapsed: Duration::ZERO,
                active_pause_elapsed_base: Duration::ZERO,
                max_time_paused: None,
                state: TimeoutState::Running,
                pause_breakdown: HashMap::new(),
            })),
            done_rx,
            deadline_changed,
            permit: Some(permit),
        }
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub(crate) fn rearm_static_hermes_user_timeout(
        &mut self,
        timeout: Duration,
        activate: impl FnOnce(),
    ) -> anyhow::Result<()> {
        {
            let mut inner = self.inner.lock();
            anyhow::ensure!(
                matches!(inner.state, TimeoutState::Running),
                "cannot rearm a paused Static Hermes timeout"
            );
            anyhow::ensure!(
                inner.timeout.is_some(),
                "cannot rearm an unarmed Static Hermes timeout"
            );
            // An expired preparation deadline retains its original phase even
            // if the watchdog has not run before activation.
            // Use one transition instant for both budgets. Descheduling while
            // this lock is held must not grant time between the two phases.
            let now = inner.rt.monotonic_now();
            inner.check_expired(now);
            if inner.terminate.is_none() {
                // Do not issue handler authority after preparation expired. A
                // short handler could finish before the next epoch check.
                return Err(TimeoutExpired.into());
            }
            activate();
            inner.start = now;
            inner.timeout = Some(timeout);
            inner.active_pause_elapsed_base = inner.pause_elapsed;
        }
        // Keep the system-time accounting in the existing Timeout while waking
        // its background task to observe the new active deadline.
        let _ = self.deadline_changed.try_broadcast(());
        Ok(())
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub(crate) fn check_timeout(&self) {
        let mut inner = self.inner.lock();
        let now = inner.rt.monotonic_now();
        inner.check_expired(now);
    }

    pub fn new(
        rt: RT,
        handle: ExecutionHandle,
        timeout: Option<Duration>,
        max_time_paused: Option<Duration>,
        permit: ConcurrencyPermit,
    ) -> Self {
        Self::new_with_termination(rt, timeout, max_time_paused, permit, move |reason| {
            handle.terminate(reason)
        })
    }

    pub(crate) fn new_with_termination(
        rt: RT,
        timeout: Option<Duration>,
        max_time_paused: Option<Duration>,
        permit: ConcurrencyPermit,
        terminate: impl FnOnce(TerminationReason) + Send + 'static,
    ) -> Self {
        let start = rt.monotonic_now();
        let inner = TimeoutInner {
            rt: rt.clone(),
            terminate: Some(Box::new(terminate)),
            start,
            timeout,
            pause_elapsed: Duration::ZERO,
            active_pause_elapsed_base: Duration::ZERO,
            max_time_paused,
            state: TimeoutState::Running,
            pause_breakdown: HashMap::new(),
        };
        let inner = Arc::new(Mutex::new(inner));
        let (done_tx, done_rx) = broadcast(1);
        let (deadline_changed, deadline_changed_rx) = broadcast(1);
        let handle = rt.spawn(
            "isolate_timeout",
            Self::go(inner.clone(), done_tx, deadline_changed_rx),
        );
        Self {
            handle,
            inner,
            done_rx,
            deadline_changed,
            permit: Some(permit),
        }
    }

    // Returns a future that resolves when the background timeout thread has
    // completed. This can either happen if the isolate has been terminated
    // due to timeout or the Timeout has been dropped due to an error.
    pub fn wait_until_completed(&self) -> impl Future<Output = ()> + 'static + use<RT> {
        let mut done_rx = self.done_rx.clone();
        async move {
            let _ = done_rx.recv().await;
        }
    }

    /// Runs the future until completely or until the timeout has expired.
    /// Returns an error in the latter case.
    pub fn with_timeout<T, F: Future<Output = T>>(
        &self,
    ) -> impl FnOnce(F) -> (impl Future<Output = anyhow::Result<T>> + use<T, F, RT>) + use<T, F, RT>
    {
        let completed = self.wait_until_completed();
        move |f| async move {
            select! {
                biased;
                result = f => Ok(result),
                // NOTE: When the background thread returns, it either means we have
                // terminated due to timeout or have been dropped. Either way, it
                // is ok to stop executing. The exact error we throw here doesn't
                // matter since we know the isolate layer overrides the error if there
                // is a termination reason.
                _ = completed => Err(TimeoutExpired.into()),
            }
        }
    }

    fn pause_start(
        &mut self,
        reason: PauseReason,
    ) -> (tokio::time::Instant, async_broadcast::Sender<()>) {
        let (tx, rx) = broadcast(1);
        let mut inner = self.inner.lock();
        let TimeoutState::Running = inner.state else {
            panic!("Overlapping calls to timeout.pause()");
        };
        // Switching clocks must not hide an active deadline that expired
        // before the watchdog could observe it.
        let pause_start = inner.rt.monotonic_now();
        inner.check_expired(pause_start);
        inner.state = TimeoutState::Paused {
            pause_done: rx,
            pause_start,
            reason,
        };
        let _ = self.deadline_changed.try_broadcast(());
        (pause_start, tx)
    }

    pub fn finish(&mut self) {
        {
            let mut inner = self.inner.lock();
            if matches!(inner.state, TimeoutState::Finished) {
                return;
            }
            inner.state = TimeoutState::Finished;
        }
        self.handle.shutdown();
    }

    pub fn finish_with_permit(mut self) -> anyhow::Result<ConcurrencyPermit> {
        self.permit.take().context("lost the permit")
    }

    // Similar to releasing the GIL in Python, it's advisable to drop the
    // ConcurrencyPermit when entering async code on the V8 thread. This helper also
    // integrates with our user time tracking to not count async code against the
    // user timeout.
    pub async fn with_release_permit_regainable<T>(
        &mut self,
        reason: PauseReason,
        f: impl AsyncFnOnce(&mut PauseGuard<'_, RT>) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let permit = self.permit.take().context("lost the permit")?;
        let timeout = self.with_timeout();
        let (pause_start, tx) = self.pause_start(reason.clone());
        let suspended_permit = permit.suspend();
        let mut pause_guard = PauseGuard {
            timeout: self,
            pause_start,
            pause_done: Some(tx),
            reason,
            suspended_permit: Some(suspended_permit),
        };
        let result = timeout(f(&mut pause_guard))
            .await
            .context(ErrorMetadata::bad_request(
                "SystemTimeoutError",
                SYSTEM_TIMEOUT_ERROR_MESSAGE,
            ));
        let suspended_permit = pause_guard
            .suspended_permit
            .take()
            .context("lost the suspended permit")?;
        drop(pause_guard);

        // Reacquire before reporting the suspended operation's result. If the
        // system timeout fired while the operation was suspended, returning
        // first would let a Wasmtime host import trap and finalize without its
        // active CPU admission. The reacquire itself cannot be bounded by this
        // request timeout for the same reason.
        let (pause_start, tx) = self.pause_start(PauseReason::ConcurrencyPermitReacquire);
        let pause_guard = PauseGuard {
            timeout: self,
            pause_start,
            pause_done: Some(tx),
            reason: PauseReason::ConcurrencyPermitReacquire,
            suspended_permit: None,
        };
        let permit = suspended_permit.acquire().await;
        drop(pause_guard);
        self.permit = Some(permit);
        result?
    }

    pub async fn with_release_permit<T>(
        &mut self,
        reason: PauseReason,
        f: impl Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        pin_mut!(f);
        // If the future completes without blocking, don't pause.
        // This is useful if `f` is just reading from caches.
        if let Some(r) = f.as_mut().now_or_never() {
            return r;
        }
        self.with_release_permit_regainable(reason, async move |_| f.await)
            .await
    }

    async fn go(
        inner: Arc<Mutex<TimeoutInner<RT>>>,
        done_tx: async_broadcast::Sender<()>,
        mut deadline_changed: async_broadcast::Receiver<()>,
    ) {
        loop {
            let wait = match inner.lock().check_expired_or_wait() {
                None => break,
                Some(wait) => wait,
            };
            let deadline_change = deadline_changed.recv();
            pin_mut!(wait);
            pin_mut!(deadline_change);
            let _ = future::select(wait, deadline_change).await;
        }
        let _ = done_tx.try_broadcast(());
    }

    pub fn into_function_execution_time(self, udf_type: UdfType) -> FunctionExecutionTime {
        self.function_execution_time(udf_type)
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    pub(crate) fn into_function_execution_time_with_permit(
        mut self,
        udf_type: UdfType,
    ) -> anyhow::Result<(FunctionExecutionTime, ConcurrencyPermit)> {
        let execution_time = self.function_execution_time(udf_type);
        let permit = self.permit.take().context("lost the permit")?;
        Ok((execution_time, permit))
    }

    fn function_execution_time(&self, udf_type: UdfType) -> FunctionExecutionTime {
        let inner = self.inner.lock();
        let elapsed = inner.rt.monotonic_now()
            - inner.start
            - inner
                .pause_elapsed
                .saturating_sub(inner.active_pause_elapsed_base);
        let limit = inner.timeout.unwrap_or(Duration::ZERO);
        metrics::log_user_function_execution_time(udf_type, elapsed);
        FunctionExecutionTime { elapsed, limit }
    }
}

/// Starts a request on `handle`, arming a `Timeout` against the environment's
/// user and system timeouts. `max_user_timeout` is an upper bound set by tests.
///
/// The caller is responsible for popping the returned `ContextId`.
pub fn start_request_on_handle<RT: Runtime, E: JsEnvironment<RT>>(
    rt: RT,
    handle: &ExecutionHandle,
    permit: ConcurrencyPermit,
    environment: &E,
    max_user_timeout: Option<Duration>,
) -> (ContextId, Timeout<RT>) {
    let context_id = handle.push_context(false /* nested */);
    let mut user_timeout = environment.user_timeout();
    if let Some(max_user_timeout) = max_user_timeout {
        user_timeout = user_timeout.min(max_user_timeout);
    }
    let timeout = Timeout::new(
        rt,
        handle.clone(),
        Some(user_timeout),
        Some(environment.system_timeout()),
        permit,
    );
    (context_id, timeout)
}

/// Starts a request on a fresh handle for an engine with no V8 isolate behind
/// it. The timeout can only record its reason, so the engine must poll
/// [`ExecutionHandle::check_terminated`] between steps to notice it.
/// TODO(runtime): Replace this when we have an impl that uses epoch
/// interruption in wasm
pub fn start_cooperative_request<RT: Runtime, E: JsEnvironment<RT>>(
    rt: RT,
    permit: ConcurrencyPermit,
    environment: &E,
    max_user_timeout: Option<Duration>,
) -> (ExecutionHandle, ContextId, Timeout<RT>) {
    let handle = ExecutionHandle::cooperative();
    let (context_id, timeout) =
        start_request_on_handle(rt, &handle, permit, environment, max_user_timeout);
    (handle, context_id, timeout)
}

#[derive(Debug)]
pub struct FunctionExecutionTime {
    pub elapsed: Duration,
    pub limit: Duration,
}

pub struct PauseGuard<'a, RT: Runtime> {
    timeout: &'a mut Timeout<RT>,
    pause_start: tokio::time::Instant,
    pause_done: Option<async_broadcast::Sender<()>>,
    reason: PauseReason,
    suspended_permit: Option<SuspendedPermit>,
}

pub struct RegainedTimeout<'a, 'b, RT: Runtime> {
    paused: &'b mut PauseGuard<'a, RT>,
}

impl<'a, 'b, RT: Runtime> Deref for RegainedTimeout<'a, 'b, RT> {
    type Target = Timeout<RT>;

    fn deref(&self) -> &Self::Target {
        self.paused.timeout
    }
}
impl<'a, 'b, RT: Runtime> DerefMut for RegainedTimeout<'a, 'b, RT> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.paused.timeout
    }
}

impl<'a, 'b, RT: Runtime> Drop for RegainedTimeout<'a, 'b, RT> {
    fn drop(&mut self) {
        if let Some(permit) = self.paused.timeout.permit.take() {
            self.paused.suspended_permit = Some(permit.suspend());
        }
        let (pause_start, tx) = self.paused.timeout.pause_start(self.paused.reason.clone());
        self.paused.pause_start = pause_start;
        self.paused.pause_done = Some(tx);
    }
}

impl<'a, RT: Runtime> PauseGuard<'a, RT> {
    pub async fn regain<'b>(&'b mut self) -> anyhow::Result<RegainedTimeout<'a, 'b, RT>> {
        let permit = self
            .suspended_permit
            .take()
            .context("lost the suspended permit")?
            .acquire()
            .await;
        self.timeout.permit = Some(permit);
        self.unpause();
        Ok(RegainedTimeout { paused: self })
    }
}

impl<RT: Runtime> PauseGuard<'_, RT> {
    fn unpause(&mut self) {
        let Some(tx) = self.pause_done.take() else {
            return;
        };
        {
            let mut inner = self.timeout.inner.lock();
            assert!(matches!(inner.state, TimeoutState::Paused { .. }));

            // A resumed host operation can beat the watchdog to this lock.
            // Publish its expired system deadline before switching clocks.
            let now = inner.rt.monotonic_now();
            inner.check_expired(now);
            let pause_duration = now - self.pause_start;
            inner.pause_elapsed += pause_duration;

            let entry = inner
                .pause_breakdown
                .entry(self.reason.clone())
                .or_insert((0, Duration::ZERO));
            entry.0 += 1;
            entry.1 += pause_duration;

            inner.state = TimeoutState::Running;
        }
        let _ = tx.try_broadcast(());
    }
}

impl<RT: Runtime> Drop for PauseGuard<'_, RT> {
    fn drop(&mut self) {
        self.unpause();
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    use std::thread;
    use std::{
        sync::{
            atomic::{
                AtomicBool,
                Ordering,
            },
            Arc,
        },
        task::Poll,
    };

    use futures::{
        future,
        poll,
    };
    use runtime::prod::ProdRuntime;

    use super::*;
    use crate::ConcurrencyLimiter;

    fn timeout_for_test(rt: &ProdRuntime, permit: ConcurrencyPermit) -> Timeout<ProdRuntime> {
        let (done_tx, done_rx) = broadcast(1);
        let (deadline_changed, _deadline_changed_rx) = broadcast(1);
        let handle = rt.spawn("timeout_test", async move {
            let _done_tx = done_tx;
            future::pending::<()>().await;
        });
        Timeout {
            handle,
            inner: Arc::new(Mutex::new(TimeoutInner {
                rt: rt.clone(),
                terminate: None,
                start: rt.monotonic_now(),
                timeout: None,
                pause_elapsed: Duration::ZERO,
                active_pause_elapsed_base: Duration::ZERO,
                max_time_paused: None,
                state: TimeoutState::Running,
                pause_breakdown: HashMap::new(),
            })),
            done_rx,
            deadline_changed,
            permit: Some(permit),
        }
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn synchronous_timeout_publishes_once_without_watchdog_progress() -> anyhow::Result<()> {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let permit = ConcurrencyLimiter::new(1)
                .acquire(Arc::new("timeout test".to_owned()), false)
                .await;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let timeout = Timeout::new_with_termination(
                rt,
                Some(Duration::ZERO),
                Some(Duration::from_secs(1)),
                permit,
                move |reason| tx.send(reason).unwrap(),
            );
            // No suspension after spawning the watchdog: only this synchronous
            // check can publish expiration on the current-thread runtime.
            assert!(rx.try_recv().is_err());
            timeout.check_timeout();
            assert!(matches!(
                rx.try_recv()?,
                TerminationReason::Isolate(IsolateTerminationReason::UserTimeout(Duration::ZERO))
            ));
            timeout.check_timeout();
            assert!(rx.try_recv().is_err());
            timeout.wait_until_completed().await;
            assert!(rx.try_recv().is_err());
            Ok(())
        })
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn synchronous_timeout_preserves_pause_and_rearm_accounting() -> anyhow::Result<()> {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let limiter = ConcurrencyLimiter::new(1);
            let permit = limiter
                .acquire(Arc::new("timeout test".to_owned()), false)
                .await;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut timeout = Timeout::new_with_termination(
                rt.clone(),
                Some(Duration::from_secs(1)),
                Some(Duration::from_secs(10)),
                permit,
                move |reason| tx.send(reason).unwrap(),
            );
            let inner = Arc::clone(&timeout.inner);
            let (resume, resumed) = tokio::sync::oneshot::channel();
            let mut suspended = Box::pin(timeout.with_release_permit(
                PauseReason::DatabaseSyscall {
                    name: "timeout test".to_owned(),
                },
                async {
                    resumed.await?;
                    Ok(())
                },
            ));
            assert!(poll!(suspended.as_mut()).is_pending());
            assert_eq!(limiter.active_permits(), 0);
            {
                let mut inner = inner.lock();
                // Age the active deadline while the real pause guard is live.
                // The active budget must not expire during this host wait.
                inner.start -= Duration::from_secs(2);
                let now = inner.rt.monotonic_now();
                inner.check_expired(now);
            }
            assert!(rx.try_recv().is_err());
            inner.lock().start += Duration::from_secs(2);
            thread::sleep(Duration::from_millis(2));
            resume.send(()).unwrap();
            assert!(matches!(poll!(suspended.as_mut()), Poll::Ready(Ok(()))));
            drop(suspended);
            assert_eq!(limiter.active_permits(), 1);
            timeout.check_timeout();
            assert!(rx.try_recv().is_err());
            let paused = inner.lock().pause_elapsed;
            assert!(paused >= Duration::from_millis(2));
            timeout
                .rearm_static_hermes_user_timeout(Duration::from_secs(2), || {})
                .unwrap();
            {
                let inner = inner.lock();
                assert_eq!(inner.pause_elapsed, paused);
                assert_eq!(inner.active_pause_elapsed_base, paused);
                assert_eq!(inner.deadline(), Some(inner.start + Duration::from_secs(2)));
            }
            // Rearming active time does not replenish the cumulative system
            // budget. A subsequent real pause sees the retained accounting.
            inner.lock().max_time_paused = Some(paused);
            let (resume, resumed) = tokio::sync::oneshot::channel();
            let mut suspended =
                Box::pin(timeout.with_release_permit(PauseReason::LoadModule, async {
                    resumed.await?;
                    Ok(())
                }));
            assert!(poll!(suspended.as_mut()).is_pending());
            resume.send(()).unwrap();
            // Resume without polling the watchdog or manually checking the
            // deadline. Leaving Paused must preserve system expiration.
            assert!(matches!(poll!(suspended.as_mut()), Poll::Ready(Ok(()))));
            drop(suspended);
            assert_eq!(limiter.active_permits(), 1);
            assert!(matches!(rx.try_recv()?, TerminationReason::Isolate(
                IsolateTerminationReason::SystemTimeout(limit)
            ) if limit == paused));
            Ok(())
        })
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn synchronous_timeout_rearm_keeps_expired_preparation_phase() -> anyhow::Result<()> {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let permit = ConcurrencyLimiter::new(1)
                .acquire(Arc::new("timeout test".to_owned()), false)
                .await;
            let active = Arc::new(AtomicBool::new(false));
            let phase = Arc::clone(&active);
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut timeout = Timeout::new_with_termination(
                rt,
                Some(Duration::ZERO),
                Some(Duration::from_secs(1)),
                permit,
                move |reason| tx.send((reason, phase.load(Ordering::Acquire))).unwrap(),
            );
            let error = timeout
                .rearm_static_hermes_user_timeout(Duration::from_secs(1), || {
                    active.store(true, Ordering::Release);
                })
                .unwrap_err();
            assert!(error.is::<TimeoutExpired>());
            let (reason, was_active) = rx.try_recv()?;
            assert!(matches!(
                reason,
                TerminationReason::Isolate(IsolateTerminationReason::UserTimeout(_))
            ));
            assert!(!was_active);
            assert!(!active.load(Ordering::Acquire));
            timeout.check_timeout();
            assert!(rx.try_recv().is_err());
            Ok(())
        })
    }

    #[test]
    fn pause_preserves_expired_user_timeout_without_watchdog() -> anyhow::Result<()> {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let rt = ProdRuntime::new(&tokio);
        tokio.block_on(async {
            let limiter = ConcurrencyLimiter::new(1);
            let permit = limiter
                .acquire(Arc::new("timeout test".to_owned()), false)
                .await;
            let handle = ExecutionHandle::cooperative();
            let mut timeout = Timeout::new(
                rt,
                handle.clone(),
                Some(Duration::ZERO),
                Some(Duration::from_secs(1)),
                permit,
            );
            let mut suspended = Box::pin(timeout.with_release_permit(
                PauseReason::LoadModule,
                future::pending::<anyhow::Result<()>>(),
            ));
            assert!(poll!(suspended.as_mut()).is_pending());
            // Exercise the ordinary ExecutionHandle callback before the
            // watchdog runs. Pausing must not turn this into system time.
            assert!(matches!(
                handle.is_not_clean(),
                Some(crate::isolate::IsolateNotClean::UserTimeout)
            ));
            drop(suspended);
            assert_eq!(limiter.active_permits(), 0);
            Ok(())
        })
    }

    #[test]
    fn with_release_permit_only_pauses_after_pending() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on("conditional_permit_release_test", async move {
            let limiter = ConcurrencyLimiter::new(1);
            let permit = limiter
                .acquire(Arc::new("timeout test".to_owned()), false)
                .await;
            let mut timeout = timeout_for_test(&rt, permit);

            let ready = timeout
                .with_release_permit(PauseReason::UdfInitialize, future::ready(Ok("ready")))
                .await
                .expect("ready future failed");
            assert_eq!(ready, "ready");
            assert_eq!(limiter.active_permits(), 1);
            {
                let inner = timeout.inner.lock();
                assert!(matches!(inner.state, TimeoutState::Running));
                assert_eq!(inner.pause_elapsed, Duration::ZERO);
                assert!(inner.pause_breakdown.is_empty());
            }

            let mut polls = 0;
            let pending_once = std::future::poll_fn(|cx| {
                polls += 1;
                match polls {
                    1 => {
                        assert_eq!(limiter.active_permits(), 1);
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    },
                    2 => {
                        assert_eq!(limiter.active_permits(), 0);
                        Poll::Ready(Ok("pending"))
                    },
                    _ => panic!("future was polled after completion"),
                }
            });
            let pending = timeout
                .with_release_permit(PauseReason::LoadModule, pending_once)
                .await
                .expect("pending future failed");
            assert_eq!(pending, "pending");
            assert_eq!(limiter.active_permits(), 1);
            {
                let inner = timeout.inner.lock();
                assert!(matches!(inner.state, TimeoutState::Running));
                assert_eq!(
                    inner
                        .pause_breakdown
                        .get(&PauseReason::LoadModule)
                        .map(|(count, _)| *count),
                    Some(1)
                );
                assert_eq!(
                    inner
                        .pause_breakdown
                        .get(&PauseReason::ConcurrencyPermitReacquire)
                        .map(|(count, _)| *count),
                    Some(1)
                );
            }

            drop(
                timeout
                    .finish_with_permit()
                    .expect("timeout lost reacquired permit"),
            );
            assert_eq!(limiter.active_permits(), 0);
        });
    }

    #[test]
    fn cancelling_pending_release_drops_the_permit() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on("conditional_permit_cancellation_test", async move {
            let limiter = ConcurrencyLimiter::new(1);
            let permit = limiter
                .acquire(Arc::new("timeout test".to_owned()), false)
                .await;
            let mut timeout = timeout_for_test(&rt, permit);

            let mut release = Box::pin(timeout.with_release_permit(
                PauseReason::UdfInitialize,
                future::pending::<anyhow::Result<()>>(),
            ));
            assert!(matches!(poll!(release.as_mut()), Poll::Pending));
            assert_eq!(limiter.active_permits(), 0);
            drop(release);

            assert!(timeout.permit.is_none());
            {
                let inner = timeout.inner.lock();
                assert!(matches!(inner.state, TimeoutState::Running));
                assert_eq!(
                    inner
                        .pause_breakdown
                        .get(&PauseReason::UdfInitialize)
                        .map(|(count, _)| *count),
                    Some(1)
                );
            }
            drop(timeout);
            assert_eq!(limiter.active_permits(), 0);
        });
    }

    #[test]
    fn system_timeout_regains_the_permit_before_returning() {
        let tokio = ProdRuntime::init_tokio().expect("failed to create Tokio runtime");
        let rt = ProdRuntime::new(&tokio);
        let block_rt = rt.clone();
        block_rt.block_on("system_timeout_permit_regain_test", async move {
            let limiter = ConcurrencyLimiter::new(1);
            let permit = limiter
                .acquire(Arc::new("timeout test".to_owned()), false)
                .await;
            let terminated_by_system_timeout = Arc::new(AtomicBool::new(false));
            let termination_observer = Arc::clone(&terminated_by_system_timeout);
            let mut timeout = Timeout::new_with_termination(
                rt,
                // Pausing wakes the watchdog to observe the earlier system
                // deadline without waiting for the active deadline.
                Some(Duration::from_secs(60)),
                Some(Duration::from_millis(5)),
                permit,
                move |reason| {
                    termination_observer.store(
                        matches!(
                            reason,
                            TerminationReason::Isolate(IsolateTerminationReason::SystemTimeout(_))
                        ),
                        Ordering::Release,
                    );
                },
            );

            let result = tokio::time::timeout(
                Duration::from_secs(5),
                timeout.with_release_permit(
                    PauseReason::DatabaseSyscall {
                        name: "timeout test".to_owned(),
                    },
                    future::pending::<anyhow::Result<()>>(),
                ),
            )
            .await
            .expect("system timeout did not complete");
            assert!(result.is_err());
            assert!(terminated_by_system_timeout.load(Ordering::Acquire));
            assert!(timeout.permit.is_some());
            assert_eq!(limiter.active_permits(), 1);

            drop(
                timeout
                    .finish_with_permit()
                    .expect("system timeout lost reacquired permit"),
            );
            assert_eq!(limiter.active_permits(), 0);
        });
    }
}
