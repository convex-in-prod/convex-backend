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
        Wake,
        Waker,
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
            target.wake_to_poll_nanos += source.wake_to_poll_nanos;
            target.resumed_polls += source.resumed_polls;
            target.resumed_polls_without_wake += source.resumed_polls_without_wake;
        }
        observation.poll_count += local.poll_count;
        observation.accounting_flushes += local.accounting_flushes;
        observation.wasm_transitions += local.wasm_transitions;
        observation.provider_batches += local.provider_batches;
        observation.provider_operations += local.provider_operations;
        observation.provider_response_bytes += local.provider_response_bytes;
        if let Some(source) = local.object_layouts {
            let target = observation.object_layouts.get_or_insert_default();
            target.hits += source.hits;
            target.misses += source.misses;
            target.evictions += source.evictions;
            target.fallbacks += source.fallbacks;
        }
        if let Some(source) = local.v8_gc_callbacks {
            let target = observation.v8_gc_callbacks.get_or_insert_default();
            target.callbacks += source.callbacks;
            target.time.cpu_nanos += source.time.cpu_nanos;
            target.time.poll_wall_nanos += source.time.poll_wall_nanos;
        }
        if let Some(source) = local.hermes_handler_heap {
            let target = observation.hermes_handler_heap.get_or_insert_with(|| {
                udf::execution_observation::HermesHeapObservation {
                    heap_before_bytes: source.heap_before_bytes,
                    gc_cpu_nanos: source.gc_cpu_nanos.map(|_| 0),
                    ..Default::default()
                }
            });
            target.collections += source.collections;
            target.gc_wall_nanos += source.gc_wall_nanos;
            target.gc_cpu_nanos = target
                .gc_cpu_nanos
                .zip(source.gc_cpu_nanos)
                .map(|(before, added)| before + added);
            target.allocated_bytes += source.allocated_bytes;
            target.heap_after_bytes = source.heap_after_bytes;
        }
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

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) fn install_wasm_hook_if_observed<T: 'static>(store: &mut wasmtime::Store<T>) -> bool {
    if ACTIVE.with_borrow(Option::is_none) {
        return false;
    }
    // This Wasmtime version cannot remove a hook. Install it only on Stores
    // selected for observation, retaining it across reuse without changing
    // pool identity or lifetime. Inactive callbacks do no timing or locking.
    store.call_hook(|_, transition| {
        wasm_transition(transition);
        Ok(())
    });
    true
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) fn wasm_transition(transition: wasmtime::CallHook) {
    ACTIVE.with_borrow_mut(|active| {
        let Some(frame) = active else { return };
        frame.local.wasm_transitions += 1;
        frame.flush();
        match transition {
            wasmtime::CallHook::CallingWasm | wasmtime::CallHook::CallingHost => {
                if frame.owners.len() == 128 {
                    frame.local.invalid = Some(ExecutionObservationInvalid::OwnerStackLimit);
                    return;
                }
                frame.owners.push(frame.owner);
                frame.owner = match transition {
                    wasmtime::CallHook::CallingWasm => Owner::Guest,
                    wasmtime::CallHook::CallingHost => Owner::Host,
                    wasmtime::CallHook::ReturningFromWasm
                    | wasmtime::CallHook::ReturningFromHost => unreachable!(),
                };
            },
            wasmtime::CallHook::ReturningFromWasm | wasmtime::CallHook::ReturningFromHost => {
                if let Some(owner) = frame.owners.pop() {
                    frame.owner = owner;
                } else {
                    frame.local.invalid = Some(ExecutionObservationInvalid::OwnerStackMismatch);
                }
            },
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

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) fn record_object_layouts([hits, misses, evictions, fallbacks]: [u64; 4]) {
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            let counts = frame.local.object_layouts.get_or_insert_default();
            counts.hits += hits;
            counts.misses += misses;
            counts.evictions += evictions;
            counts.fallbacks += fallbacks;
        }
    });
}

pub(crate) fn record_v8_gc_support() {
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            frame.local.v8_gc_callbacks.get_or_insert_default();
        }
    });
}

struct GcStart {
    wall: Instant,
    cpu: Option<u64>,
    depth: u32,
}

thread_local! {
    static V8_GC_START: RefCell<Option<GcStart>> = const { RefCell::new(None) };
}

pub(crate) fn begin_v8_gc() {
    if !is_observed() {
        return;
    }
    V8_GC_START.with_borrow_mut(|start| {
        if let Some(start) = start {
            start.depth += 1;
        } else {
            *start = Some(GcStart {
                wall: Instant::now(),
                cpu: thread_cpu(),
                depth: 1,
            });
        }
    });
}

pub(crate) fn end_v8_gc() {
    let start = V8_GC_START.with_borrow_mut(|start| {
        let active = start.as_mut()?;
        active.depth -= 1;
        if active.depth == 0 {
            start.take()
        } else {
            None
        }
    });
    let Some(start) = start else {
        return;
    };
    let wall = nanos(start.wall.elapsed());
    let cpu = start
        .cpu
        .zip(thread_cpu())
        .and_then(|(before, after)| after.checked_sub(before));
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            let stats = frame.local.v8_gc_callbacks.get_or_insert_default();
            stats.callbacks += 1;
            stats.time.poll_wall_nanos += wall;
            if let Some(cpu) = cpu {
                stats.time.cpu_nanos += cpu;
            } else {
                frame.local.invalid = Some(ExecutionObservationInvalid::CpuClockUnavailable);
            }
        }
    });
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
pub(crate) fn record_hermes_gc([collections, wall, cpu, allocated, before, after]: [u64; 6]) {
    let cpu = thread_cpu().map(|_| cpu);
    ACTIVE.with_borrow_mut(|active| {
        if let Some(frame) = active {
            let stats = frame.local.hermes_handler_heap.get_or_insert_with(|| {
                udf::execution_observation::HermesHeapObservation {
                    heap_before_bytes: before,
                    gc_cpu_nanos: cpu.map(|_| 0),
                    ..Default::default()
                }
            });
            stats.collections += collections;
            stats.gc_wall_nanos += wall;
            stats.gc_cpu_nanos = stats
                .gc_cpu_nanos
                .zip(cpu)
                .map(|(before, added)| before + added);
            stats.allocated_bytes += allocated;
            stats.heap_after_bytes = after;
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
        wake: None,
    }
    .await
}

struct ObservedFuture<F> {
    future: Option<Pin<Box<F>>>,
    frame: Option<Frame>,
    completed: bool,
    wake: Option<Arc<ObservedWake>>,
}

struct ObservedWake(Mutex<WakeState>);

struct WakeState {
    parent: Waker,
    first_wake: Option<Instant>,
}

impl Wake for ObservedWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let parent = {
            let mut state = self.0.lock();
            state.first_wake.get_or_insert_with(Instant::now);
            state.parent.clone()
        };
        // Wake outside the lock: a parent wrapper can synchronously record or
        // forward this wake, including on another executor thread.
        parent.wake();
    }
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
        let wake = this.wake.get_or_insert_with(|| {
            Arc::new(ObservedWake(Mutex::new(WakeState {
                parent: cx.waker().clone(),
                first_wake: None,
            })))
        });
        let woke_at = {
            let mut state = wake.0.lock();
            state.parent.clone_from(cx.waker());
            state.first_wake.take()
        };
        let frame = this.frame.as_mut().expect("execution frame missing");
        if let Some(suspended) = frame.suspended {
            let phase = Frame::phase(frame.phase, &mut frame.local);
            phase.resumed_polls += 1;
            if let Some(woke_at) = woke_at {
                phase.wake_to_poll_nanos +=
                    nanos(Instant::now().duration_since(woke_at.max(suspended)));
            } else {
                phase.resumed_polls_without_wake += 1;
            }
        }
        let waker = Waker::from(Arc::clone(wake));
        let mut observed_context = Context::from_waker(&waker);
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
            .poll(&mut observed_context);
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
                    record_v8_gc_support();
                    begin_v8_gc();
                    begin_v8_gc();
                    work();
                    end_v8_gc();
                    end_v8_gc();
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
        let gc = result.v8_gc_callbacks.unwrap();
        assert_eq!(gc.callbacks, 1);
        assert!(gc.time.cpu_nanos > 0);
        assert!(gc.time.cpu_nanos <= result.handler.guest.cpu_nanos);
        assert!(gc.time.poll_wall_nanos <= result.handler.guest.poll_wall_nanos);
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
                #[cfg(feature = "static-hermes-wasmtime-gate")]
                {
                    wasm_transition(wasmtime::CallHook::CallingWasm);
                    wasm_transition(wasmtime::CallHook::CallingHost);
                }
                let result =
                    observe_poll(receiver, Some(Owner::Provider), Suspension::Provider).await;
                #[cfg(feature = "static-hermes-wasmtime-gate")]
                {
                    wasm_transition(wasmtime::CallHook::ReturningFromHost);
                    work();
                    wasm_transition(wasmtime::CallHook::ReturningFromWasm);
                }
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
        assert_eq!(result.handler.resumed_polls, 1);
        assert_eq!(result.handler.resumed_polls_without_wake, 0);
        assert!(result.handler.wake_to_poll_nanos > 0);
        assert!(result.handler.wake_to_poll_nanos <= result.handler.suspension.provider_nanos);
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

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    fn wasm_fixture(
        trap: bool,
        host_calls: usize,
    ) -> (wasmtime::Store<()>, wasmtime::TypedFunc<(), ()>) {
        let mut types = wasm_encoder::TypeSection::new();
        types.ty().function([], []);
        let mut imports = wasm_encoder::ImportSection::new();
        if host_calls > 0 {
            imports.import("env", "noop", wasm_encoder::EntityType::Function(0));
        }
        let mut functions = wasm_encoder::FunctionSection::new();
        functions.function(0);
        let mut exports = wasm_encoder::ExportSection::new();
        exports.export(
            "run",
            wasm_encoder::ExportKind::Func,
            u32::from(host_calls > 0),
        );
        let mut function = wasm_encoder::Function::new([]);
        for _ in 0..host_calls {
            function.instruction(&wasm_encoder::Instruction::Call(0));
        }
        if trap {
            function.instruction(&wasm_encoder::Instruction::Unreachable);
        }
        function.instruction(&wasm_encoder::Instruction::End);
        let mut code = wasm_encoder::CodeSection::new();
        code.function(&function);
        let mut module = wasm_encoder::Module::new();
        module
            .section(&types)
            .section(&imports)
            .section(&functions)
            .section(&exports)
            .section(&code);
        let engine = wasmtime::Engine::default();
        let module = wasmtime::Module::new(&engine, module.finish()).unwrap();
        let mut store = wasmtime::Store::new(&engine, ());
        let imports = if host_calls > 0 {
            vec![wasmtime::Func::wrap(&mut store, || {}).into()]
        } else {
            vec![]
        };
        let instance =
            block_on(wasmtime::Instance::new_async(&mut store, &module, &imports)).unwrap();
        let run = instance
            .get_typed_func::<(), ()>(&mut store, "run")
            .unwrap();
        (store, run)
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn reused_store_uses_the_current_invocation_collector() {
        let (mut store, run) = wasm_fixture(false, 3);
        assert!(!install_wasm_hook_if_observed(&mut store));
        block_on(run.call_async(&mut store, ())).unwrap();
        let first = ExecutionObserver::default();
        block_on(observe(
            async {
                assert!(install_wasm_hook_if_observed(&mut store));
                run.call_async(&mut store, ()).await
            },
            Some(first.task(TaskKind::Runtime)),
        ))
        .unwrap();
        let retained = first.snapshot();
        assert!(retained.preparation.guest.cpu_nanos > 0);
        assert_eq!(retained.wasm_transitions, 8);
        assert_eq!(retained.invalid, None);
        let second = ExecutionObserver::default();
        block_on(observe(
            run.call_async(&mut store, ()),
            Some(second.task(TaskKind::Runtime)),
        ))
        .unwrap();
        block_on(run.call_async(&mut store, ())).unwrap();
        assert_eq!(first.snapshot(), retained);
        let result = second.snapshot();
        assert!(result.preparation.guest.cpu_nanos > 0);
        assert_eq!(result.runtime_tasks, 1);
        assert_eq!(result.wasm_transitions, 8);
        assert_eq!(result.invalid, None);
        assert!(current().is_none());
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    fn wasm_trap_balances_owners_without_changing_the_error() {
        let (mut store, run) = wasm_fixture(true, 0);
        let observer = ExecutionObserver::default();
        let result = block_on(observe(
            async {
                assert!(install_wasm_hook_if_observed(&mut store));
                run.call_async(&mut store, ()).await
            },
            Some(observer.task(TaskKind::Runtime)),
        ));
        assert!(matches!(
            result.unwrap_err().downcast_ref::<wasmtime::Trap>(),
            Some(wasmtime::Trap::UnreachableCodeReached)
        ));
        let result = observer.snapshot();
        assert_eq!(result.invalid, None);
        assert_eq!(result.completed_tasks, 1);
        assert_eq!(result.active_tasks, 0);
        assert!(current().is_none());
    }

    #[cfg(feature = "static-hermes-wasmtime-gate")]
    #[test]
    #[ignore = "manual observer calibration; use --release --ignored --nocapture"]
    fn wasm_import_observer_calibration() {
        const IMPORTS_PER_CALL: usize = 1000;
        const CALLS: usize = 500;
        let (mut plain_store, plain_run) = wasm_fixture(false, IMPORTS_PER_CALL);
        let (mut hooked_store, hooked_run) = wasm_fixture(false, IMPORTS_PER_CALL);
        let warmup = ExecutionObserver::default();
        block_on(observe(
            async {
                assert!(install_wasm_hook_if_observed(&mut hooked_store));
                for _ in 0..100 {
                    plain_run.call_async(&mut plain_store, ()).await.unwrap();
                    hooked_run.call_async(&mut hooked_store, ()).await.unwrap();
                }
            },
            Some(warmup.task(TaskKind::Runtime)),
        ));
        // Bracket both controls to expose drift. This compares no installed
        // hook, installed but inactive, and collecting within the same
        // call-hook-enabled binary; it does not measure the Cargo feature tax.
        for mode in ["no_hook", "inactive", "collecting", "inactive", "no_hook"] {
            let observer = ExecutionObserver::default();
            let (store, run) = if mode == "no_hook" {
                (&mut plain_store, &plain_run)
            } else {
                (&mut hooked_store, &hooked_run)
            };
            let task = (mode == "collecting").then(|| observer.task(TaskKind::Runtime));
            let wall = Instant::now();
            let cpu = thread_cpu().unwrap();
            block_on(observe(
                async {
                    for _ in 0..CALLS {
                        run.call_async(&mut *store, ()).await.unwrap();
                    }
                },
                task,
            ));
            let cpu = thread_cpu().unwrap() - cpu;
            let wall = nanos(wall.elapsed());
            let result = observer.snapshot();
            assert_eq!(result.invalid, None);
            if mode == "collecting" {
                assert_eq!(
                    result.wasm_transitions,
                    (CALLS * (IMPORTS_PER_CALL + 1) * 2) as u64,
                );
            }
            println!(
                "observer_calibration mode={mode} imports={} cpu_nanos={cpu} wall_nanos={wall} \
                 accounting_flushes={} wasm_transitions={}",
                CALLS * IMPORTS_PER_CALL,
                result.accounting_flushes,
                result.wasm_transitions,
            );
        }
    }
}
