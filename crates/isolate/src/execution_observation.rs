//! Bounded, opt-in thread CPU accounting. Context is installed only for one
//! synchronous poll; it never follows unrelated work on an executor thread.

use std::{
    cell::RefCell,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        Weak,
    },
    task::{
        Context,
        Poll,
    },
    time::Instant,
};

use parking_lot::Mutex;
use udf::execution_observation::{
    ExecutionObservation,
    ExecutionObservationInvalid,
    ExecutionOwnerTime,
    ExecutionPhaseObservation,
};

#[derive(Clone, Default)]
pub struct ExecutionObserver(Arc<Mutex<ExecutionObservation>>);

#[derive(Clone, Copy)]
pub enum TaskKind {
    Lane,
    Runtime,
}

pub struct ObservedTask {
    observer: ExecutionObserver,
    kind: TaskKind,
    queued: Instant,
}

#[derive(Clone, Copy)]
pub(crate) enum Owner {
    Guest,
    Host,
    Provider,
}

#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Lane,
    Preparation,
    Handler,
    Finalization,
}

#[derive(Clone, Copy)]
pub(crate) enum Suspension {
    Provider,
    Permit,
    Other,
}

struct Frame {
    observer: ExecutionObserver,
    // Merge once per poll, so a hot import path never takes the shared mutex.
    local: ExecutionObservation,
    phase: Phase,
    owner: Owner,
    owners: Vec<Owner>,
    pending: Suspension,
    suspended: Option<Instant>,
    wall: Instant,
    cpu: Option<u64>,
}

thread_local! {
    static ACTIVE: RefCell<Option<Frame>> = const { RefCell::new(None) };
}

fn nanos(duration: std::time::Duration) -> u64 {
    duration
        .as_nanos()
        .try_into()
        .expect("execution observation duration exceeds u64")
}

#[cfg(target_os = "linux")]
pub(crate) fn thread_cpu() -> Option<u64> {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // The kernel initializes the complete timespec on success. This clock
    // measures only the current thread and is never compared across polls.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, time.as_mut_ptr()) } != 0 {
        return None;
    }
    let time = unsafe { time.assume_init() };
    u64::try_from(time.tv_sec)
        .ok()?
        .checked_mul(1_000_000_000)?
        .checked_add(u64::try_from(time.tv_nsec).ok()?)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn thread_cpu() -> Option<u64> {
    None
}

impl ExecutionObserver {
    pub fn task(&self, kind: TaskKind) -> ObservedTask {
        ObservedTask {
            observer: self.clone(),
            kind,
            queued: Instant::now(),
        }
    }

    pub fn snapshot(&self) -> ExecutionObservation {
        self.0.lock().clone()
    }

    /// Includes queued and detached tasks, which can outlive the caller's
    /// record.
    pub fn downgrade(&self) -> Weak<Mutex<ExecutionObservation>> {
        Arc::downgrade(&self.0)
    }
}

pub fn current() -> Option<ExecutionObserver> {
    ACTIVE.with_borrow(|active| active.as_ref().map(|frame| frame.observer.clone()))
}

pub(crate) fn is_observed() -> bool {
    ACTIVE.with_borrow(|active| active.is_some())
}

pub(crate) fn record_module_phase(
    phase: crate::module_diagnostics::ModulePhase,
    wall: std::time::Duration,
    cpu: Option<u64>,
) {
    use crate::module_diagnostics::ModulePhase;
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            let module = &mut frame.local.module_initialization;
            let time = match phase {
                ModulePhase::Registration | ModulePhase::Cleanup => return,
                ModulePhase::Compilation => &mut module.compilation,
                ModulePhase::Serialization => &mut module.serialization,
                ModulePhase::Instantiation => &mut module.instantiation,
                ModulePhase::Evaluation => &mut module.evaluation,
                ModulePhase::ExportInspection => &mut module.export_inspection,
            };
            time.poll_wall_nanos += nanos(wall);
            if let Some(cpu) = cpu {
                time.cpu_nanos += cpu;
            } else {
                frame.local.invalid = Some(ExecutionObservationInvalid::CpuClockUnavailable);
            }
        }
    });
}

impl Frame {
    fn phase(
        phase: Phase,
        observation: &mut ExecutionObservation,
    ) -> &mut ExecutionPhaseObservation {
        match phase {
            Phase::Lane => &mut observation.lane,
            Phase::Preparation => &mut observation.preparation,
            Phase::Handler => &mut observation.handler,
            Phase::Finalization => &mut observation.finalization,
        }
    }

    fn flush(&mut self) {
        let wall = Instant::now();
        let cpu = thread_cpu();
        self.local.accounting_flushes += 1;
        let delta = self
            .cpu
            .zip(cpu)
            .and_then(|(start, end)| end.checked_sub(start));
        if delta.is_none() {
            self.local.invalid = Some(ExecutionObservationInvalid::CpuClockUnavailable);
        }
        let phase = Self::phase(self.phase, &mut self.local);
        let time: &mut ExecutionOwnerTime = match self.owner {
            Owner::Guest => &mut phase.guest,
            Owner::Host => &mut phase.host,
            Owner::Provider => &mut phase.provider,
        };
        if let Some(delta) = delta {
            time.cpu_nanos += delta;
        }
        time.poll_wall_nanos += nanos(wall.duration_since(self.wall));
        self.wall = wall;
        self.cpu = cpu;
    }

    fn resume(&mut self) {
        let now = Instant::now();
        if let Some(suspended) = self.suspended.take() {
            let waits = &mut Self::phase(self.phase, &mut self.local).suspension;
            let target = match self.pending {
                Suspension::Provider => &mut waits.provider_nanos,
                Suspension::Permit => &mut waits.permit_nanos,
                Suspension::Other => &mut waits.other_nanos,
            };
            *target += nanos(now.duration_since(suspended));
        }
        self.pending = Suspension::Other;
        self.wall = Instant::now();
        self.cpu = thread_cpu();
    }

    fn publish(&mut self) {
        let local = std::mem::take(&mut self.local);
        let mut shared = self.observer.0.lock();
        let observation = &mut *shared;
        observation.context_lookup.absent += local.context_lookup.absent;
        observation.context_lookup.validated += local.context_lookup.validated;
        observation.context_lookup.validation_failed += local.context_lookup.validation_failed;
        observation.context_lookup.validation_error += local.context_lookup.validation_error;
        let source = local.module_initialization;
        let target = &mut observation.module_initialization;
        for (source, target) in [
            (source.compilation, &mut target.compilation),
            (source.serialization, &mut target.serialization),
            (source.instantiation, &mut target.instantiation),
            (source.evaluation, &mut target.evaluation),
            (source.export_inspection, &mut target.export_inspection),
        ] {
            target.cpu_nanos += source.cpu_nanos;
            target.poll_wall_nanos += source.poll_wall_nanos;
        }
        for (source, target) in [
            (local.lane, &mut observation.lane),
            (local.preparation, &mut observation.preparation),
            (local.handler, &mut observation.handler),
            (local.finalization, &mut observation.finalization),
        ] {
            for (source, target) in [
                (source.guest, &mut target.guest),
                (source.host, &mut target.host),
                (source.provider, &mut target.provider),
            ] {
                target.cpu_nanos += source.cpu_nanos;
                target.poll_wall_nanos += source.poll_wall_nanos;
            }
            target.suspension.provider_nanos += source.suspension.provider_nanos;
            target.suspension.permit_nanos += source.suspension.permit_nanos;
            target.suspension.other_nanos += source.suspension.other_nanos;
        }
        observation.poll_count += local.poll_count;
        observation.accounting_flushes += local.accounting_flushes;
        observation.wasm_transitions += local.wasm_transitions;
        observation.provider_batches += local.provider_batches;
        observation.provider_operations += local.provider_operations;
        observation.provider_response_bytes += local.provider_response_bytes;
        if local.reused_runtime.is_some() {
            observation.reused_runtime = local.reused_runtime;
        }
        if local.invalid.is_some() {
            observation.invalid = local.invalid;
        }
    }
}

/// This guard must be dropped in the same synchronous poll that created it.
pub(crate) struct OwnerGuard {
    previous: Option<Owner>,
    _not_send: PhantomData<Rc<()>>,
}

pub(crate) fn enter(owner: Owner) -> OwnerGuard {
    let previous = ACTIVE.with_borrow_mut(|active| {
        active.as_mut().map(|frame| {
            frame.flush();
            std::mem::replace(&mut frame.owner, owner)
        })
    });
    OwnerGuard {
        previous,
        _not_send: PhantomData,
    }
}

impl Drop for OwnerGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous {
            ACTIVE.with_borrow_mut(|active| {
                let frame = active
                    .as_mut()
                    .expect("execution owner guard escaped its poll");
                frame.flush();
                frame.owner = previous;
            });
        }
    }
}

pub(crate) fn set_phase(phase: Phase) {
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            frame.flush();
            frame.phase = phase;
        }
    });
}

pub(crate) async fn observe_poll<F: Future>(
    future: F,
    owner: Option<Owner>,
    suspension: Suspension,
) -> F::Output {
    if ACTIVE.with_borrow(Option::is_none) {
        return future.await;
    }
    futures::pin_mut!(future);
    std::future::poll_fn(|cx| {
        let _owner = owner.map(enter);
        let result = future.as_mut().poll(cx);
        if result.is_pending() {
            ACTIVE.with_borrow_mut(|active| {
                if let Some(frame) = active {
                    frame.pending = suspension;
                }
            });
        }
        result
    })
    .await
}

pub(crate) fn record_provider(operations: usize, response_bytes: usize) {
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            let observation = &mut frame.local;
            observation.provider_batches += 1;
            observation.provider_operations += operations as u64;
            observation.provider_response_bytes += response_bytes as u64;
        }
    });
}

pub(crate) fn record_runtime_reuse(reused: bool) {
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            frame.local.reused_runtime = Some(reused);
        }
    });
}

pub(crate) fn record_nested_runtime() {
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            // Same-isolate recursion shares this frame and changes its phase.
            // Exclude that aggregate just like separately scheduled descendants.
            frame.local.invalid = Some(ExecutionObservationInvalid::NestedRuntime);
        }
    });
}

pub(crate) fn record_context_lookup(outcome: crate::metrics::DatabaseUdfContextReuseLookupOutcome) {
    use crate::metrics::DatabaseUdfContextReuseLookupOutcome;
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            let lookup = &mut frame.local.context_lookup;
            match outcome {
                DatabaseUdfContextReuseLookupOutcome::NotFound => lookup.absent += 1,
                DatabaseUdfContextReuseLookupOutcome::Hit => lookup.validated += 1,
                DatabaseUdfContextReuseLookupOutcome::ValidationFailed => {
                    lookup.validation_failed += 1
                },
                DatabaseUdfContextReuseLookupOutcome::ValidationError => {
                    lookup.validation_error += 1
                },
            }
        }
    });
}

/// Disabled observation takes the original future path without allocating.
pub async fn observe<F: Future>(future: F, task: Option<ObservedTask>) -> F::Output {
    let Some(task) = task else {
        return future.await;
    };
    let now = Instant::now();
    {
        let mut observation = task.observer.0.lock();
        observation.active_tasks += 1;
        if matches!(task.kind, TaskKind::Runtime) {
            observation.runtime_tasks += 1;
            observation.runtime_queue_nanos += nanos(now.duration_since(task.queued));
            if observation.runtime_tasks > 1 {
                observation.invalid = Some(ExecutionObservationInvalid::NestedRuntime);
            }
        }
    }
    ObservedFuture {
        future: Some(Box::pin(future)),
        frame: Some(Frame {
            observer: task.observer,
            local: ExecutionObservation::default(),
            phase: match task.kind {
                TaskKind::Lane => Phase::Lane,
                TaskKind::Runtime => Phase::Preparation,
            },
            owner: Owner::Host,
            owners: Vec::new(),
            pending: Suspension::Other,
            suspended: None,
            wall: now,
            cpu: None,
        }),
        completed: false,
    }
    .await
}

struct ObservedFuture<F> {
    future: Option<Pin<Box<F>>>,
    frame: Option<Frame>,
    completed: bool,
}

// Restores thread-local context during both normal return and unwinding.
struct PollGuard<'a> {
    destination: &'a mut Option<Frame>,
    previous: Option<Frame>,
}

impl<'a> PollGuard<'a> {
    fn new(destination: &'a mut Option<Frame>) -> Self {
        let mut frame = destination.take().expect("execution frame missing");
        let previous = ACTIVE.with_borrow_mut(|active| {
            if let Some(parent) = active {
                parent.flush();
            }
            active.take()
        });
        frame.resume();
        ACTIVE.with_borrow_mut(|active| *active = Some(frame));
        Self {
            destination,
            previous,
        }
    }
}

impl Drop for PollGuard<'_> {
    fn drop(&mut self) {
        ACTIVE.with_borrow_mut(|active| {
            let mut frame = active
                .take()
                .expect("execution frame missing at poll completion");
            frame.flush();
            frame.publish();
            frame.suspended = Some(Instant::now());
            *self.destination = Some(frame);
            if let Some(parent) = &mut self.previous {
                parent.wall = Instant::now();
                parent.cpu = thread_cpu();
            }
            *active = self.previous.take();
        });
    }
}

impl<F: Future> Future for ObservedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _guard = PollGuard::new(&mut this.frame);
        ACTIVE.with_borrow_mut(|active| {
            active
                .as_mut()
                .expect("execution frame missing")
                .local
                .poll_count += 1;
        });
        let result = this
            .future
            .as_mut()
            .expect("execution future missing")
            .as_mut()
            .poll(cx);
        this.completed = result.is_ready();
        result
    }
}

impl<F> Drop for ObservedFuture<F> {
    fn drop(&mut self) {
        {
            let _guard = PollGuard::new(&mut self.frame);
            // Dropping a suspended Wasmtime future can resume its fiber for
            // teardown. Keep both hooks and CPU accounting active through it.
            drop(self.future.take());
        }
        let frame = self
            .frame
            .as_ref()
            .expect("execution frame missing at finalization");
        let mut observation = frame.observer.0.lock();
        observation.active_tasks -= 1;
        if self.completed {
            observation.completed_tasks += 1;
        } else {
            observation.cancelled_tasks += 1;
        }
        if !frame.owners.is_empty() {
            observation.invalid = Some(ExecutionObservationInvalid::OwnerStackMismatch);
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::sync::atomic::{
        AtomicBool,
        Ordering,
    };

    use futures::{
        channel::oneshot,
        executor::block_on,
        task::noop_waker,
    };

    use super::*;

    fn work() {
        let mut value = 1u64;
        for index in 0..10_000 {
            value = std::hint::black_box(value.wrapping_mul(31).wrapping_add(index));
        }
        std::hint::black_box(value);
    }

    #[test]
    fn immediate_provider_has_cpu_without_suspension() {
        let observer = ExecutionObserver::default();
        let result = block_on(observe(
            async {
                set_phase(Phase::Handler);
                let result = observe_poll(
                    async {
                        work();
                        record_provider(3, 120);
                        42
                    },
                    Some(Owner::Provider),
                    Suspension::Provider,
                )
                .await;
                {
                    let _guest = enter(Owner::Guest);
                    work();
                    {
                        let _host = enter(Owner::Host);
                        work();
                    }
                    work();
                }
                result
            },
            Some(observer.task(TaskKind::Runtime)),
        ));
        assert_eq!(result, 42);
        let result = observer.snapshot();
        assert_eq!(result.invalid, None);
        assert_eq!(result.active_tasks, 0);
        assert_eq!(result.completed_tasks, 1);
        assert_eq!(result.provider_batches, 1);
        assert_eq!(result.provider_operations, 3);
        assert_eq!(result.provider_response_bytes, 120);
        assert_eq!(result.handler.suspension.provider_nanos, 0);
        assert!(result.handler.guest.cpu_nanos > 0);
        assert!(result.handler.host.cpu_nanos > 0);
        assert!(result.handler.provider.cpu_nanos > 0);
        assert!(current().is_none());
    }

    #[test]
    fn pending_provider_can_resume_on_another_thread() {
        let observer = ExecutionObserver::default();
        let task = observer.task(TaskKind::Runtime);
        let (sender, receiver) = oneshot::channel();
        let future = Box::pin(observe(
            async {
                set_phase(Phase::Handler);
                let result =
                    observe_poll(receiver, Some(Owner::Provider), Suspension::Provider).await;
                result.unwrap()
            },
            Some(task),
        ));
        let future = std::thread::spawn(move || {
            let mut future = future;
            let waker = noop_waker();
            assert!(future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            assert!(current().is_none());
            work();
            future
        })
        .join()
        .unwrap();
        sender.send(7).unwrap();
        let result = std::thread::spawn(move || block_on(future)).join().unwrap();
        assert_eq!(result, 7);
        let result = observer.snapshot();
        assert_eq!(result.invalid, None);
        assert_eq!(result.completed_tasks, 1);
        assert_eq!(result.active_tasks, 0);
        assert!(result.handler.suspension.provider_nanos > 0);
        assert_eq!(result.handler.suspension.permit_nanos, 0);
    }

    struct PendingWithCleanup(Arc<AtomicBool>);

    impl Future for PendingWithCleanup {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    impl Drop for PendingWithCleanup {
        fn drop(&mut self) {
            self.0.store(current().is_some(), Ordering::SeqCst);
            let _host = enter(Owner::Host);
            work();
        }
    }

    #[test]
    fn cancellation_observes_cleanup_and_clears_thread_context() {
        let observer = ExecutionObserver::default();
        let cleaned = Arc::new(AtomicBool::new(false));
        let mut future = Box::pin(observe(
            PendingWithCleanup(cleaned.clone()),
            Some(observer.task(TaskKind::Runtime)),
        ));
        let waker = noop_waker();
        assert!(future
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        assert!(current().is_none());
        drop(future);
        assert!(cleaned.load(Ordering::SeqCst));
        assert!(current().is_none());
        let result = observer.snapshot();
        assert_eq!(result.active_tasks, 0);
        assert_eq!(result.cancelled_tasks, 1);
        assert_eq!(result.completed_tasks, 0);
        assert_eq!(result.invalid, None);
        assert!(result.preparation.host.cpu_nanos > 0);
    }

    #[test]
    fn disabled_observation_preserves_the_future_result() {
        assert_eq!(
            block_on(observe(
                async {
                    assert!(current().is_none());
                    17
                },
                None
            )),
            17
        );
        assert!(current().is_none());
    }

    #[test]
    fn same_isolate_nested_work_invalidates_phase_attribution() {
        let observer = ExecutionObserver::default();
        block_on(observe(
            async {
                set_phase(Phase::Handler);
                work();
                record_nested_runtime();
                set_phase(Phase::Preparation);
                work();
            },
            Some(observer.task(TaskKind::Runtime)),
        ));
        assert_eq!(
            observer.snapshot().invalid,
            Some(ExecutionObservationInvalid::NestedRuntime)
        );
        assert_eq!(observer.snapshot().active_tasks, 0);
        assert!(current().is_none());
    }
}
