use common::types::ActiveJavascriptClass;

/// Shared non-preemptive grant policy for CPU execution and retained-instance
/// admission. Each owner supplies its own occupancy and eligible demand; an
/// unavailable group never consumes a service turn merely by being queued.
#[derive(Clone, Debug)]
pub(crate) struct ServiceClassPolicy {
    protected_minimum: usize,
    degradable_minimum: usize,
    next_tied_class: ActiveJavascriptClass,
    next_protected_control_plane: bool,
    high_priority_streak: u8,
}

pub(crate) const SERVICE_CLASSES: [ActiveJavascriptClass; 5] = [
    ActiveJavascriptClass::Dependency,
    ActiveJavascriptClass::Protected,
    ActiveJavascriptClass::Degradable,
    ActiveJavascriptClass::ControlPlane,
    ActiveJavascriptClass::HighPriorityMutation,
];

pub(crate) fn class_index(class: ActiveJavascriptClass) -> usize {
    match class {
        ActiveJavascriptClass::Dependency => 0,
        ActiveJavascriptClass::Protected => 1,
        ActiveJavascriptClass::Degradable => 2,
        ActiveJavascriptClass::ControlPlane => 3,
        ActiveJavascriptClass::HighPriorityMutation => 4,
    }
}

impl ServiceClassPolicy {
    pub(crate) fn new(protected_minimum: usize, degradable_minimum: usize) -> Self {
        assert_eq!(protected_minimum == 0, degradable_minimum == 0);
        Self {
            protected_minimum,
            degradable_minimum,
            next_tied_class: ActiveJavascriptClass::Degradable,
            next_protected_control_plane: true,
            high_priority_streak: 0,
        }
    }

    pub(crate) fn select_class(
        &mut self,
        waiting: [bool; 5],
        occupancy: [usize; 5],
    ) -> Option<ActiveJavascriptClass> {
        let group = self.select_group(waiting, occupancy)?;
        if group != ActiveJavascriptClass::Protected {
            return Some(group);
        }
        let ordinary = waiting[class_index(ActiveJavascriptClass::Protected)];
        let high = waiting[class_index(ActiveJavascriptClass::HighPriorityMutation)];
        let application = ordinary || high;
        let control = waiting[class_index(ActiveJavascriptClass::ControlPlane)];
        let selected = match (application, control) {
            (true, true) if self.next_protected_control_plane => {
                ActiveJavascriptClass::ControlPlane
            },
            (true, _) => ActiveJavascriptClass::Protected,
            (false, true) => ActiveJavascriptClass::ControlPlane,
            (false, false) => unreachable!("selected protected group has no waiter"),
        };
        // Alternate grants whenever both queues are ready. Ordinary resumptions
        // cannot hide deployment demand, and repeated deployments cannot starve
        // live protected work. Both retain the existing combined service floor.
        self.next_protected_control_plane = selected == ActiveJavascriptClass::Protected;
        if selected == ActiveJavascriptClass::ControlPlane {
            return Some(selected);
        }
        // Client hints divide existing protected service; sustained high demand
        // never removes the ordinary service turn, including resumptions.
        if high && (!ordinary || self.high_priority_streak < 2) {
            self.high_priority_streak = self.high_priority_streak.saturating_add(1).min(2);
            Some(ActiveJavascriptClass::HighPriorityMutation)
        } else {
            self.high_priority_streak = 0;
            Some(ActiveJavascriptClass::Protected)
        }
    }

    fn select_group(
        &mut self,
        waiting: [bool; 5],
        occupancy: [usize; 5],
    ) -> Option<ActiveJavascriptClass> {
        if waiting[class_index(ActiveJavascriptClass::Dependency)] {
            return Some(ActiveJavascriptClass::Dependency);
        }
        let protected_waiting = waiting[class_index(ActiveJavascriptClass::Protected)]
            || waiting[class_index(ActiveJavascriptClass::HighPriorityMutation)]
            || waiting[class_index(ActiveJavascriptClass::ControlPlane)];
        if self.protected_minimum == 0 {
            assert!(
                !waiting[class_index(ActiveJavascriptClass::Degradable)],
                "degradable waiter escaped compatibility collapse"
            );
            return protected_waiting.then_some(ActiveJavascriptClass::Protected);
        }

        let degradable_waiting = waiting[class_index(ActiveJavascriptClass::Degradable)];
        match (protected_waiting, degradable_waiting) {
            (false, false) => return None,
            (true, false) => return Some(ActiveJavascriptClass::Protected),
            (false, true) => return Some(ActiveJavascriptClass::Degradable),
            (true, true) => {},
        }

        let protected_occupancy = occupancy[class_index(ActiveJavascriptClass::Protected)]
            + occupancy[class_index(ActiveJavascriptClass::HighPriorityMutation)]
            + occupancy[class_index(ActiveJavascriptClass::ControlPlane)];
        let degradable_occupancy = occupancy[class_index(ActiveJavascriptClass::Degradable)];
        let protected_below = protected_occupancy < self.protected_minimum;
        let degradable_below = degradable_occupancy < self.degradable_minimum;
        match (protected_below, degradable_below) {
            (true, false) => Some(ActiveJavascriptClass::Protected),
            (false, true) => Some(ActiveJavascriptClass::Degradable),
            (true, true) => {
                // Compare satisfaction ratios without floating point. Filling
                // floors proportionally prevents the larger floor from delaying
                // all progress in the smaller class during a cold burst.
                let protected_scaled =
                    protected_occupancy as u128 * self.degradable_minimum as u128;
                let degradable_scaled =
                    degradable_occupancy as u128 * self.protected_minimum as u128;
                if protected_scaled < degradable_scaled {
                    Some(ActiveJavascriptClass::Protected)
                } else if degradable_scaled < protected_scaled {
                    Some(ActiveJavascriptClass::Degradable)
                } else {
                    Some(self.take_next_tied_class())
                }
            },
            (false, false) => {
                let protected_elastic = protected_occupancy - self.protected_minimum;
                let degradable_elastic = degradable_occupancy - self.degradable_minimum;
                // Balance elastic occupancy, not only the handoff sequence.
                // Otherwise a class that initially borrowed the whole gate can
                // lose every elastic slot as its incumbents finish, even while
                // both classes remain continuously runnable.
                if protected_elastic < degradable_elastic {
                    Some(ActiveJavascriptClass::Protected)
                } else if degradable_elastic < protected_elastic {
                    Some(ActiveJavascriptClass::Degradable)
                } else {
                    Some(self.take_next_tied_class())
                }
            },
        }
    }

    fn take_next_tied_class(&mut self) -> ActiveJavascriptClass {
        let selected = self.next_tied_class;
        self.next_tied_class = match selected {
            ActiveJavascriptClass::Protected => ActiveJavascriptClass::Degradable,
            ActiveJavascriptClass::Degradable => ActiveJavascriptClass::Protected,
            ActiveJavascriptClass::Dependency
            | ActiveJavascriptClass::ControlPlane
            | ActiveJavascriptClass::HighPriorityMutation => {
                panic!("only protected and degradable groups can break an admission tie")
            },
        };
        selected
    }
}
