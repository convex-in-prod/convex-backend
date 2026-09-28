//! Arbitration before instance checkout. Waiters own no Store or CPU permit.
//! The memory controller still owns every byte grant; this queue decides which
//! eligible request may try it, using the same class policy as CPU admission.

use std::{
    array,
    collections::VecDeque,
    sync::{
        atomic::{
            AtomicUsize,
            Ordering,
        },
        Arc,
    },
};

use common::{
    knobs::*,
    types::ActiveJavascriptClass,
};
use parking_lot::{
    Mutex,
    MutexGuard,
};
use tokio::{
    sync::Notify,
    time::Instant,
};

use super::{
    GeneratedRoutedInstancePool,
    StaticHermesWasmAdmissionFailure,
};
use crate::{
    isolate_queue::{
        IsolateQueueConfig,
        LaneDelayController,
    },
    service_class_policy::{
        class_index,
        ServiceClassPolicy,
    },
};

metrics::register_convex_gauge!(
    WASM_RESOURCE_ADMISSION_REQUESTS,
    "Wasm resource admission owners, including retained requests awaiting children or cleanup",
    &["active_javascript_class", "state"]
);
metrics::register_convex_counter!(
    WASM_RESOURCE_ADMISSION_EVENTS_TOTAL,
    "Wasm resource queue transitions; withdrawal is not an execution outcome",
    &["active_javascript_class", "event"],
    std::time::Duration::MAX,
);
metrics::register_convex_histogram!(
    WASM_RESOURCE_ADMISSION_WAIT_SECONDS,
    "Primary Wasm resource wait through grant or withdrawal",
    &["active_javascript_class", "event"]
);

type Eligibility = Box<dyn Fn(&GeneratedRoutedInstancePool) -> bool + Send>;

pub(crate) struct RoutedAdmission {
    inner: Mutex<AdmissionQueue>,
    occupancy: Arc<Occupancy>,
}

struct Occupancy {
    counts: [AtomicUsize; 5],
    changed: Notify,
}

struct Waiter {
    id: u64,
    class: ActiveJavascriptClass,
    entered: Instant,
    deadline: Instant,
    eligible: Eligibility,
    wake: Arc<Notify>,
}

pub(super) struct AdmissionQueue {
    waiters: VecDeque<Waiter>,
    next_id: u64,
    capacity: usize,
    dependency_reserve: usize,
    class_floors_enabled: bool,
    policy: ServiceClassPolicy,
    occupancy: Arc<Occupancy>,
    delay_config: IsolateQueueConfig,
    delay_enabled: bool,
    delays: [LaneDelayController; 5],
}

pub(super) struct AdmissionTicket {
    owner: Arc<RoutedAdmission>,
    id: Option<u64>,
    pub(super) wake: Arc<Notify>,
}

/// Remains with the memory permit through suspension and detached cleanup.
/// Releasing occupancy never takes the queue lock: pool return already holds
/// the pool mutex, whereas admission locks queue -> pool -> memory controller.
pub(crate) struct RoutedAdmissionPermit {
    occupancy: Arc<Occupancy>,
    class: ActiveJavascriptClass,
}

impl Drop for RoutedAdmissionPermit {
    fn drop(&mut self) {
        let previous =
            self.occupancy.counts[class_index(self.class)].fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "Wasm resource class occupancy underflow");
        let class = crate::metrics::active_javascript_class_label(self.class);
        WASM_RESOURCE_ADMISSION_REQUESTS
            .with_label_values(&[class.value.as_ref(), "retained"])
            .dec();
        self.occupancy.changed.notify_waiters();
    }
}

impl RoutedAdmission {
    pub(crate) fn new() -> anyhow::Result<Arc<Self>> {
        let config = IsolateQueueConfig::new(
            *ISOLATE_QUEUE_DELAY_TARGET_MILLIS,
            *ISOLATE_QUEUE_DELAY_INTERVAL_MILLIS,
            *ISOLATE_QUEUE_DELAY_SHED_THRESHOLD_MILLIS,
            *ISOLATE_QUEUE_HARD_MAX_AGE_MILLIS,
            *ISOLATE_CONTROL_PLANE_HARD_MAX_AGE_MILLIS,
            *ISOLATE_CONTROL_PLANE_QUEUE_CAPACITY,
        )?;
        let capacity = *ISOLATE_QUEUE_SIZE;
        anyhow::ensure!(capacity > 0, "Wasm pending capacity must be positive");
        let occupancy = Arc::new(Occupancy {
            counts: array::from_fn(|_| AtomicUsize::new(0)),
            changed: Notify::new(),
        });
        Ok(Arc::new(Self {
            inner: Mutex::new(AdmissionQueue {
                waiters: VecDeque::new(),
                next_id: 0,
                capacity,
                dependency_reserve: *ISOLATE_DEPENDENCY_WORKER_RESERVE,
                class_floors_enabled: false,
                policy: ServiceClassPolicy::new(0, 0),
                occupancy: occupancy.clone(),
                delay_config: config,
                delay_enabled: *ISOLATE_QUEUE_DELAY_CONTROL_ENABLED,
                delays: array::from_fn(|_| {
                    LaneDelayController::new(Instant::now(), config.interval)
                }),
            }),
            occupancy,
        }))
    }

    pub(super) fn configure_classes(&self, protected: usize, degradable: usize) {
        let mut queue = self.inner.lock();
        assert!(queue.waiters.is_empty());
        assert!(self
            .occupancy
            .counts
            .iter()
            .all(|count| count.load(Ordering::Acquire) == 0));
        queue.class_floors_enabled = protected > 0;
        queue.policy = ServiceClassPolicy::new(protected, degradable);
    }

    pub(super) fn enter(
        self: &Arc<Self>,
        class: ActiveJavascriptClass,
        deadline: Instant,
        eligible: Eligibility,
    ) -> anyhow::Result<AdmissionTicket> {
        assert_ne!(class, ActiveJavascriptClass::ControlPlane);
        let mut queue = self.inner.lock();
        let capacity = queue.capacity
            + if class == ActiveJavascriptClass::Dependency {
                queue.dependency_reserve
            } else {
                0
            };
        if queue.waiters.len() >= capacity {
            let label = crate::metrics::active_javascript_class_label(class);
            WASM_RESOURCE_ADMISSION_EVENTS_TOTAL
                .with_label_values(&[label.value.as_ref(), "backlog_full"])
                .inc();
            return Err(StaticHermesWasmAdmissionFailure::Backlog.overloaded());
        }
        let class = if !queue.class_floors_enabled && class == ActiveJavascriptClass::Degradable {
            ActiveJavascriptClass::Protected
        } else {
            class
        };
        let id = queue.next_id;
        queue.next_id = id
            .checked_add(1)
            .expect("Wasm admission waiter ID overflow");
        let wake = Arc::new(Notify::new());
        if !queue.waiters.iter().any(|waiter| waiter.class == class) {
            queue.delays[class_index(class)] =
                LaneDelayController::new(Instant::now(), queue.delay_config.interval);
        }
        let label = crate::metrics::active_javascript_class_label(class);
        WASM_RESOURCE_ADMISSION_REQUESTS
            .with_label_values(&[label.value.as_ref(), "pending"])
            .inc();
        WASM_RESOURCE_ADMISSION_EVENTS_TOTAL
            .with_label_values(&[label.value.as_ref(), "queued"])
            .inc();
        queue.waiters.push_back(Waiter {
            id,
            class,
            entered: Instant::now(),
            deadline,
            eligible,
            wake: wake.clone(),
        });
        Ok(AdmissionTicket {
            owner: self.clone(),
            id: Some(id),
            wake,
        })
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, AdmissionQueue> {
        self.inner.lock()
    }

    pub(super) fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.occupancy.changed.notified()
    }
}

impl AdmissionQueue {
    pub(super) fn has_competing_waiters(&self, ticket: Option<&AdmissionTicket>) -> bool {
        self.waiters.len() > usize::from(ticket.is_some())
    }

    // Compute a candidate policy without spending its turn. Only an actual
    // resource grant commits tie-breaking and mutation-burst state.
    fn next(
        &self,
        pool: &GeneratedRoutedInstancePool,
        known_ready: Option<u64>,
    ) -> Option<(usize, ServiceClassPolicy)> {
        let now = Instant::now();
        let mut waiting = [false; 5];
        for waiter in &self.waiters {
            if now < waiter.deadline {
                waiting[class_index(waiter.class)] = true;
            }
        }
        let occupancy =
            array::from_fn(|index| self.occupancy.counts[index].load(Ordering::Acquire));
        loop {
            let mut policy = self.policy.clone();
            let class = policy.select_class(waiting, occupancy)?;
            if let Some(index) = self.waiters.iter().position(|waiter| {
                waiter.class == class
                    && now < waiter.deadline
                    && (Some(waiter.id) == known_ready || (waiter.eligible)(pool))
            }) {
                return Some((index, policy));
            }
            waiting[class_index(class)] = false;
        }
    }

    pub(super) fn primary_turn(
        &mut self,
        ticket: &AdmissionTicket,
        pool: &GeneratedRoutedInstancePool,
        known_ready: bool,
    ) -> anyhow::Result<Option<ServiceClassPolicy>> {
        let Some((index, policy)) = self.next(pool, if known_ready { ticket.id } else { None })
        else {
            return Ok(None);
        };
        let waiter = &self.waiters[index];
        if Some(waiter.id) != ticket.id {
            waiter.wake.notify_one();
            return Ok(None);
        }
        let age = Instant::now().saturating_duration_since(waiter.entered);
        let delay = &mut self.delays[class_index(waiter.class)];
        if self.delay_enabled {
            delay.observe(Instant::now(), age, self.delay_config);
            if waiter.class != ActiveJavascriptClass::Dependency
                && delay.overloaded
                && age > self.delay_config.shed_threshold
            {
                let label = crate::metrics::active_javascript_class_label(waiter.class);
                WASM_RESOURCE_ADMISSION_EVENTS_TOTAL
                    .with_label_values(&[label.value.as_ref(), "delay_shed"])
                    .inc();
                return Err(StaticHermesWasmAdmissionFailure::QueueDelay.overloaded());
            }
        }
        Ok(Some(policy))
    }

    pub(super) fn shadow_turn(&self, pool: &GeneratedRoutedInstancePool) -> bool {
        if let Some((index, _)) = self.next(pool, None) {
            self.waiters[index].wake.notify_one();
            false
        } else {
            true
        }
    }

    pub(super) fn granted(
        &mut self,
        ticket: &mut AdmissionTicket,
        policy: ServiceClassPolicy,
    ) -> RoutedAdmissionPermit {
        let index = self
            .waiters
            .iter()
            .position(|waiter| Some(waiter.id) == ticket.id)
            .expect("granted Wasm admission lost its request");
        self.policy = policy;
        let waiter = self.waiters.remove(index).unwrap();
        ticket.id = None;
        let label = crate::metrics::active_javascript_class_label(waiter.class);
        WASM_RESOURCE_ADMISSION_REQUESTS
            .with_label_values(&[label.value.as_ref(), "pending"])
            .dec();
        WASM_RESOURCE_ADMISSION_REQUESTS
            .with_label_values(&[label.value.as_ref(), "retained"])
            .inc();
        WASM_RESOURCE_ADMISSION_EVENTS_TOTAL
            .with_label_values(&[label.value.as_ref(), "granted"])
            .inc();
        WASM_RESOURCE_ADMISSION_WAIT_SECONDS
            .with_label_values(&[label.value.as_ref(), "granted"])
            .observe(waiter.entered.elapsed().as_secs_f64());
        self.occupancy.counts[class_index(waiter.class)].fetch_add(1, Ordering::AcqRel);
        RoutedAdmissionPermit {
            occupancy: self.occupancy.clone(),
            class: waiter.class,
        }
    }

    pub(super) fn granted_shadow(&self) -> RoutedAdmissionPermit {
        let class = ActiveJavascriptClass::Protected;
        let label = crate::metrics::active_javascript_class_label(class);
        WASM_RESOURCE_ADMISSION_REQUESTS
            .with_label_values(&[label.value.as_ref(), "retained"])
            .inc();
        self.occupancy.counts[class_index(class)].fetch_add(1, Ordering::AcqRel);
        RoutedAdmissionPermit {
            occupancy: self.occupancy.clone(),
            class,
        }
    }

    pub(super) fn wake_next(&self, pool: &GeneratedRoutedInstancePool) -> bool {
        if let Some((index, _)) = self.next(pool, None) {
            self.waiters[index].wake.notify_one();
            true
        } else {
            false
        }
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let waiter = {
                let mut queue = self.owner.inner.lock();
                let index = queue
                    .waiters
                    .iter()
                    .position(|waiter| waiter.id == id)
                    .expect("Wasm admission ticket lost its waiter");
                queue.waiters.remove(index).unwrap()
            };
            // Captured routes can own compiled modules. Release them outside
            // the admission lock, including on cancellation before any grant.
            let label = crate::metrics::active_javascript_class_label(waiter.class);
            WASM_RESOURCE_ADMISSION_REQUESTS
                .with_label_values(&[label.value.as_ref(), "pending"])
                .dec();
            WASM_RESOURCE_ADMISSION_EVENTS_TOTAL
                .with_label_values(&[label.value.as_ref(), "withdrawn"])
                .inc();
            WASM_RESOURCE_ADMISSION_WAIT_SECONDS
                .with_label_values(&[label.value.as_ref(), "withdrawn"])
                .observe(waiter.entered.elapsed().as_secs_f64());
            drop(waiter);
            self.owner.occupancy.changed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::AtomicBool,
        time::Duration,
    };

    use super::*;

    #[tokio::test]
    async fn ready_groups_borrow_capacity_without_losing_class_or_dependency_progress() {
        let admission = RoutedAdmission::new().unwrap();
        admission.configure_classes(1, 1);
        let pool = GeneratedRoutedInstancePool::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        let cold_ready = Arc::new(AtomicBool::new(false));
        let ready = cold_ready.clone();
        let mut cold = admission
            .enter(
                ActiveJavascriptClass::Protected,
                deadline,
                Box::new(move |_| ready.load(Ordering::Acquire)),
            )
            .unwrap();
        let mut first = admission
            .enter(
                ActiveJavascriptClass::Degradable,
                deadline,
                Box::new(|_| true),
            )
            .unwrap();
        let first_grant = {
            let mut queue = admission.lock();
            let policy = queue.primary_turn(&first, &pool, false).unwrap().unwrap();
            queue.granted(&mut first, policy)
        };
        let mut second = admission
            .enter(
                ActiveJavascriptClass::Degradable,
                deadline,
                Box::new(|_| true),
            )
            .unwrap();
        let second_grant = {
            let mut queue = admission.lock();
            let policy = queue.primary_turn(&second, &pool, false).unwrap().unwrap();
            queue.granted(&mut second, policy)
        };
        // Occupancy belongs to retained work, independent of CPU suspension.
        let third = admission
            .enter(
                ActiveJavascriptClass::Degradable,
                deadline,
                Box::new(|_| true),
            )
            .unwrap();
        cold_ready.store(true, Ordering::Release);
        assert!(admission
            .lock()
            .primary_turn(&third, &pool, false)
            .unwrap()
            .is_none());
        assert!(!admission.lock().shadow_turn(&pool));
        let mut dependency = admission
            .enter(
                ActiveJavascriptClass::Dependency,
                deadline,
                Box::new(|_| true),
            )
            .unwrap();
        assert!(admission
            .lock()
            .primary_turn(&cold, &pool, false)
            .unwrap()
            .is_none());
        let dependency_grant = {
            let mut queue = admission.lock();
            let policy = queue
                .primary_turn(&dependency, &pool, false)
                .unwrap()
                .unwrap();
            queue.granted(&mut dependency, policy)
        };
        let cold_grant = {
            let mut queue = admission.lock();
            let policy = queue.primary_turn(&cold, &pool, false).unwrap().unwrap();
            queue.granted(&mut cold, policy)
        };
        drop((
            first_grant,
            second_grant,
            dependency_grant,
            cold_grant,
            third,
        ));
        assert!(admission.lock().waiters.is_empty());
        assert!(admission
            .occupancy
            .counts
            .iter()
            .all(|count| count.load(Ordering::Acquire) == 0));
    }

    #[tokio::test]
    async fn bounded_pending_admission_releases_cancelled_waiters_and_limits_mutation_priority() {
        let admission = RoutedAdmission::new().unwrap();
        admission.lock().capacity = 4;
        admission.lock().dependency_reserve = 1;
        let pool = GeneratedRoutedInstancePool::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut ordinary = admission
            .enter(
                ActiveJavascriptClass::Protected,
                deadline,
                Box::new(|_| true),
            )
            .unwrap();
        let mut high = (0..3)
            .map(|_| {
                admission
                    .enter(
                        ActiveJavascriptClass::HighPriorityMutation,
                        deadline,
                        Box::new(|_| true),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(admission
            .enter(
                ActiveJavascriptClass::Protected,
                deadline,
                Box::new(|_| true)
            )
            .is_err());
        let dependency = admission
            .enter(
                ActiveJavascriptClass::Dependency,
                deadline,
                Box::new(|_| true),
            )
            .unwrap();
        assert!(admission
            .lock()
            .primary_turn(&high[0], &pool, false)
            .unwrap()
            .is_none());
        drop(dependency);
        for ticket in &mut high[..2] {
            let grant = {
                let mut queue = admission.lock();
                let policy = queue.primary_turn(ticket, &pool, false).unwrap().unwrap();
                queue.granted(ticket, policy)
            };
            drop(grant);
        }
        assert!(admission
            .lock()
            .primary_turn(&high[2], &pool, false)
            .unwrap()
            .is_none());
        let grant = {
            let mut queue = admission.lock();
            let policy = queue
                .primary_turn(&ordinary, &pool, false)
                .unwrap()
                .unwrap();
            queue.granted(&mut ordinary, policy)
        };
        drop((grant, high));
        assert!(admission.lock().waiters.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn sustained_resource_delay_sheds_without_spending_a_grant() {
        let admission = RoutedAdmission::new().unwrap();
        {
            let mut queue = admission.lock();
            queue.delay_config = IsolateQueueConfig::new(
                Duration::from_millis(1),
                Duration::from_millis(10),
                Duration::from_millis(5),
                Duration::from_secs(1),
                Duration::from_secs(1),
                1,
            )
            .unwrap();
            queue.delay_enabled = true;
        }
        let pool = GeneratedRoutedInstancePool::default();
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut tickets = (0..3)
            .map(|_| {
                admission
                    .enter(
                        ActiveJavascriptClass::Protected,
                        deadline,
                        Box::new(|_| true),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        tokio::time::advance(Duration::from_millis(2)).await;
        for ticket in &mut tickets[..2] {
            let grant = {
                let mut queue = admission.lock();
                let policy = queue.primary_turn(ticket, &pool, false).unwrap().unwrap();
                queue.granted(ticket, policy)
            };
            drop(grant);
            tokio::time::advance(Duration::from_millis(1)).await;
        }
        tokio::time::advance(Duration::from_millis(8)).await;
        let error = admission
            .lock()
            .primary_turn(&tickets[2], &pool, false)
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<StaticHermesWasmAdmissionFailure>(),
            Some(&StaticHermesWasmAdmissionFailure::QueueDelay)
        );
        drop(tickets);
        assert!(admission.lock().waiters.is_empty());
        assert!(admission
            .occupancy
            .counts
            .iter()
            .all(|count| count.load(Ordering::Acquire) == 0));
    }
}
