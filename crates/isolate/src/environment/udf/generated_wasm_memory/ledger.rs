//! Authoritative reservation totals. Updating one owner never scans other
//! invocations; equal readiness forecasts keep independent counted ownership.

use super::*;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct SlotLedger {
    retained: u128,
    growth: GrowthReservations,
}

impl SlotLedger {
    pub(super) fn idle(slot: &SlotMemory) -> Self {
        assert_eq!(slot.status, SlotStatus::Idle);
        Self::from_slot(slot, None)
    }

    fn from_slot(slot: &SlotMemory, active: Option<&ActiveInvocation>) -> Self {
        let retained = guest_memory_charge(
            slot.guest_bytes,
            slot.shared_static_bytes,
            slot.initialized_memory,
        )
        .expect("retained guest memory accounting is invalid")
        .0 as u128
            + slot.retained_host_bytes as u128;
        let growth = match slot.status {
            SlotStatus::Idle => GrowthReservations {
                ready: slot.idle_growth_bytes,
                ..Default::default()
            },
            SlotStatus::Evicting => GrowthReservations::default(),
            SlotStatus::Active { .. } => active
                .expect("active reservation lost its invocation")
                .growth_reservation(),
        };
        Self { retained, growth }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct MemoryLedger {
    // A committed allocation can cross usize::MAX in the aggregate. Wider
    // exact sums preserve its charge and permit later releases to recover;
    // checked conversion still denies new admission while it is out of range.
    retained: u128,
    executing: u128,
    preparing: u128,
    ready: BTreeMap<usize, usize>,
    slots: BTreeMap<GeneratedSlotId, SlotLedger>,
}

impl MemoryLedger {
    pub(super) fn update(&mut self, id: GeneratedSlotId, next: Option<SlotLedger>) {
        let previous = match next {
            Some(next) => self.slots.insert(id, next),
            None => Some(
                self.slots
                    .remove(&id)
                    .expect("released slot lost its reservation"),
            ),
        };
        let previous = previous.unwrap_or_default();
        let next = next.unwrap_or_default();
        if previous == next {
            return;
        }
        for (total, old, new) in [
            (&mut self.retained, previous.retained, next.retained),
            (
                &mut self.executing,
                previous.growth.executing as u128,
                next.growth.executing as u128,
            ),
            (
                &mut self.preparing,
                previous.growth.preparing as u128,
                next.growth.preparing as u128,
            ),
        ] {
            *total = total
                .checked_sub(old)
                .and_then(|value| value.checked_add(new))
                .expect("memory reservation total is inconsistent");
        }
        if previous.growth.ready != next.growth.ready {
            if previous.growth.ready != 0 {
                let count = self
                    .ready
                    .get_mut(&previous.growth.ready)
                    .expect("ready reservation lost its counted owner");
                *count = count
                    .checked_sub(1)
                    .expect("ready reservation count underflow");
                if *count == 0 {
                    self.ready.remove(&previous.growth.ready);
                }
            }
            if next.growth.ready != 0 {
                let count = self.ready.entry(next.growth.ready).or_default();
                *count = count
                    .checked_add(1)
                    .expect("ready reservation count overflow");
            }
        }
    }

    pub(super) fn retained_bytes(&self) -> Option<usize> {
        self.retained.try_into().ok()
    }

    pub(super) fn growth_excluding(
        &self,
        excluded: Option<GeneratedSlotId>,
    ) -> Option<GrowthReservations> {
        let excluded = excluded.map_or(GrowthReservations::default(), |id| {
            self.slots
                .get(&id)
                .expect("reservation projection excluded an unknown slot")
                .growth
        });
        // At most the largest unique key is skipped. Replacing one of several
        // equal maxima cannot release another owner's readiness reservation.
        let ready = self
            .ready
            .iter()
            .rev()
            .find(|(bytes, count)| **bytes != excluded.ready || **count > 1)
            .map_or(0, |(bytes, _)| *bytes);
        Some(GrowthReservations {
            executing: self
                .executing
                .checked_sub(excluded.executing as u128)?
                .try_into()
                .ok()?,
            preparing: self
                .preparing
                .checked_sub(excluded.preparing as u128)?
                .try_into()
                .ok()?,
            ready,
        })
    }
}

impl ControllerState {
    pub(super) fn update_slot_ledger(&mut self, id: GeneratedSlotId) {
        let next = self.slots.get(&id).map(|slot| {
            let active = match slot.status {
                SlotStatus::Active { invocation_id } => Some(
                    self.active
                        .get(&invocation_id)
                        .expect("active slot lost its memory owner")
                        .lock(),
                ),
                SlotStatus::Idle | SlotStatus::Evicting => None,
            };
            SlotLedger::from_slot(slot, active.as_deref())
        });
        self.ledger.update(id, next);
    }

    #[cfg(test)]
    pub(super) fn assert_ledger_matches_recomputation(&self) {
        let mut expected = MemoryLedger::default();
        for (id, slot) in &self.slots {
            let active = match slot.status {
                SlotStatus::Active { invocation_id } => Some(self.active[&invocation_id].lock()),
                SlotStatus::Idle | SlotStatus::Evicting => None,
            };
            let contribution = SlotLedger::from_slot(slot, active.as_deref());
            expected.retained += contribution.retained;
            expected.executing += contribution.growth.executing as u128;
            expected.preparing += contribution.growth.preparing as u128;
            if contribution.growth.ready > 0 {
                *expected.ready.entry(contribution.growth.ready).or_default() += 1;
            }
            expected.slots.insert(*id, contribution);
        }
        assert_eq!(self.ledger, expected);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counted_ready_maximum_preserves_equal_owners_and_replacement() {
        let mut ledger = MemoryLedger::default();
        for (id, ready) in [(1, 64), (2, 64), (3, 32)] {
            ledger.update(
                GeneratedSlotId(id),
                Some(SlotLedger {
                    retained: 10,
                    growth: GrowthReservations {
                        ready,
                        ..Default::default()
                    },
                }),
            );
        }
        assert_eq!(
            ledger
                .growth_excluding(Some(GeneratedSlotId(1)))
                .unwrap()
                .ready,
            64
        );
        ledger.update(GeneratedSlotId(2), None);
        assert_eq!(
            ledger
                .growth_excluding(Some(GeneratedSlotId(1)))
                .unwrap()
                .ready,
            32
        );
        ledger.update(
            GeneratedSlotId(1),
            Some(SlotLedger {
                retained: 10,
                growth: GrowthReservations {
                    executing: 48,
                    ..Default::default()
                },
            }),
        );
        assert_eq!(ledger.growth_excluding(None).unwrap().bytes(), Some(48));
        ledger.update(GeneratedSlotId(1), None);
        assert_eq!(ledger.growth_excluding(None).unwrap().bytes(), Some(32));
        assert_eq!(ledger.retained_bytes(), Some(10));
    }

    #[test]
    fn committed_overflow_keeps_exact_ownership_until_release() {
        let mut ledger = MemoryLedger::default();
        for id in [1, 2] {
            ledger.update(
                GeneratedSlotId(id),
                Some(SlotLedger {
                    retained: usize::MAX as u128,
                    growth: GrowthReservations {
                        executing: usize::MAX,
                        ..Default::default()
                    },
                }),
            );
        }
        assert_eq!(ledger.retained_bytes(), None);
        assert_eq!(ledger.growth_excluding(None), None);
        assert_eq!(
            ledger
                .growth_excluding(Some(GeneratedSlotId(1)))
                .unwrap()
                .executing,
            usize::MAX,
        );
        ledger.update(GeneratedSlotId(1), None);
        assert_eq!(ledger.retained_bytes(), Some(usize::MAX));
        assert_eq!(ledger.growth_excluding(None).unwrap().executing, usize::MAX);
        ledger.update(GeneratedSlotId(2), None);
        assert_eq!(ledger, MemoryLedger::default());
    }
}
