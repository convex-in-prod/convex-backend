//! Reclamation selection at a successful invocation boundary. Planned savings
//! only prevent duplicate maintenance; they never grant admission credit.

use super::*;

pub(crate) enum InitializedRetention {
    Retain,
    Reclaim { target_bytes: usize },
    Dispose,
}

impl GeneratedMemoryController {
    pub(super) fn ready_forecast_locked(
        &self,
        state: &ControllerState,
        invocation: &ActiveInvocation,
    ) -> usize {
        // Readiness has no selected route. The completing call's peak is
        // already accounted for, and may disappear or become retained backing;
        // it must not impose another peak-sized reservation on this idle Store.
        // Completion can lower this prior, while larger future route demand
        // still passes ordinary checkout and allocation admission.
        state.functions[&invocation.identity].forecast(self.policy.cold_peak_growth_bytes)
    }
}

impl InvocationMemoryPermit {
    /// Call after exclusive-Store reconciliation, before releasing finalizer
    /// resources or the active deadline. Claims cover only savings that have
    /// not yet reached the ledger through readiness transfer or completion.
    pub(crate) fn initialized_retention(&self) -> InitializedRetention {
        let controller = &self.account.controller;
        let mut state = controller.state.lock();
        let invocation = state
            .active
            .get(&self.account.invocation_id)
            .expect("reclamation selection lost its active invocation");
        let initialized = invocation
            .initialized_memory
            .expect("reclamation selection requires an initialized image");
        assert_eq!(invocation.reclamation_claim_bytes, 0);
        if matches!(state.pressure, BackendPressure::Active { .. }) {
            // Existing idle admission rejects Stores during cgroup pressure.
            // Collecting one first cannot make it reusable and only delays the
            // host-only disposal that releases its private backing.
            return InitializedRetention::Dispose;
        }
        if initialized.exposed_bytes == 0 {
            return InitializedRetention::Retain;
        }
        let retained = guest_memory_charge(
            invocation.current_guest_bytes,
            0,
            invocation.initialized_memory,
        )
        .expect("reclamation selection has invalid guest accounting")
        .0
        .checked_add(invocation.current_host_bytes)
        .expect("reclamation selection retained bytes overflow");
        // Predict the completed return with the shared-headroom equation.
        // Admission still charges the checkout baseline until completion; do
        // not collect merely because that conservative charge has not yet
        // published a baseline decrease already observed by reconciliation.
        let returning = controller
            .ordinary_with_growth(
                &state,
                Some(invocation.slot_id),
                GrowthReservations {
                    ready: controller.ready_forecast_locked(&state, invocation),
                    ..Default::default()
                },
            )
            .and_then(|bytes| bytes.checked_sub(invocation.checkout_baseline_bytes))
            .and_then(|bytes| bytes.checked_add(retained))
            .and_then(|bytes| bytes.checked_add(controller.shared_buffers.bytes()))
            .and_then(|bytes| bytes.checked_add(controller.overflow.borrowed_bytes()))
            .unwrap_or(usize::MAX);
        let exposure = initialized.exposed_bytes;
        // A small recovery gap avoids collecting on every crossing of the soft
        // budget. It is aggregate policy, not another per-slot memory limit.
        let recovery =
            controller.policy.soft_budget_bytes - controller.policy.soft_budget_bytes / 10;
        if returning > controller.policy.soft_budget_bytes {
            state.retention_pressure_active = true;
        } else if returning <= recovery {
            state.retention_pressure_active = false;
        }
        let budget_need = if state.retention_pressure_active {
            returning.saturating_sub(recovery)
        } else {
            0
        };
        let needed = budget_need.saturating_sub(state.reclamation_in_progress_bytes);
        let pages = needed
            .div_ceil(overflow::PRIVATE_PAGE_BYTES)
            .min(exposure / overflow::PRIVATE_PAGE_BYTES);
        if pages == 0 {
            return InitializedRetention::Retain;
        }
        let reclaim = pages * overflow::PRIVATE_PAGE_BYTES;
        state
            .active
            .get_mut(&self.account.invocation_id)
            .expect("reclamation invocation disappeared")
            .reclamation_claim_bytes = reclaim;
        state.reclamation_in_progress_bytes = state
            .reclamation_in_progress_bytes
            .checked_add(reclaim)
            .expect("reclamation claim accounting overflow");
        InitializedRetention::Reclaim {
            target_bytes: exposure - reclaim,
        }
    }
}
