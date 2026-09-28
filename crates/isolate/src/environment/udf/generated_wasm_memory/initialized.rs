//! Initialized-image accounting at exclusive Store boundaries. Page faults
//! consume an already reserved allowance; they never enter this controller.

use super::*;

const HOST_PAGE_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InitializedMemoryAccounting {
    pub(super) linear_bytes: usize,
    // Exposure is sampled at Store boundaries, not a live RSS measurement.
    pub(super) exposed_bytes: usize,
    pub(super) peak_exposed_bytes: usize,
    pub(super) allowance_bytes: usize,
}

/// Return observed and reserved private guest bytes. A prepared image replaces
/// static-page credit: its whole backing has a separate shared owner. Internal
/// GC memory retains its raw charge. The typed limiter updates raw and linear
/// capacity together; the mapping hook enforces new private exposure
/// separately.
pub(super) fn guest_memory_charge(
    raw_bytes: usize,
    static_credit: usize,
    initialized: Option<InitializedMemoryAccounting>,
) -> Option<(usize, usize)> {
    match initialized {
        Some(initialized) => {
            let other = raw_bytes.checked_sub(initialized.linear_bytes)?;
            Some((
                other.checked_add(initialized.exposed_bytes)?,
                other.checked_add(initialized.allowance_bytes)?,
            ))
        },
        None => {
            let private = raw_bytes.checked_sub(static_credit)?;
            Some((private, private))
        },
    }
}

impl InvocationMemoryPermit {
    /// Disposal may bypass capacity reconciliation. Preserve the recorder's
    /// peak before Store teardown drops this permit, without settling or
    /// releasing the loan that still owns the mapping's private pages.
    pub(crate) fn observe_initialized_memory_disposal(&self, peak_bytes: usize, denied: bool) {
        if !self.observation_started {
            return;
        }
        let mut state = self.account.controller.state.lock();
        state
            .active
            .get_mut(&self.account.invocation_id)
            .expect("initialized memory disposal lost its invocation")
            .lock()
            .observe_initialized_memory_peak(peak_bytes, denied);
    }

    pub(crate) fn initialized_memory_loan(&self) -> InitializedMemoryLoan {
        self.account.controller.overflow.loan()
    }

    /// Before attachment the old linear allocation is still fully charged.
    /// Borrow only that existing private charge for the initial write
    /// allowance; the remap must complete before
    /// `attach_initialized_memory` releases it.
    pub(crate) fn initial_private_allowance(&self, linear_bytes: usize, maximum: usize) -> usize {
        let state = self.account.controller.state.lock();
        let invocation = state.active[&self.account.invocation_id].lock();
        assert!(invocation.initialized_memory.is_none());
        assert!(linear_bytes <= invocation.current_guest_bytes);
        linear_bytes
            .checked_sub(invocation.shared_static_bytes)
            .expect("static credit exceeds initialized linear memory")
            .min(maximum)
            / HOST_PAGE_BYTES
            * HOST_PAGE_BYTES
    }

    /// Install only after the image owns the actual mapping, with no guest
    /// reentry between remap and this transfer. Attachment consumes no private
    /// pages; its recorder and sealed-image charges are owned separately.
    pub(crate) fn attach_initialized_memory(&self, linear_bytes: usize, allowance: usize) {
        let controller = &self.account.controller;
        let mut state = controller.state.lock();
        let mut invocation_guard = state
            .active
            .get_mut(&self.account.invocation_id)
            .expect("image attachment lost its active invocation")
            .lock();
        let invocation = &mut *invocation_guard;
        assert!(invocation.initialized_memory.is_none());
        assert!(linear_bytes <= invocation.current_guest_bytes);
        assert!(allowance % HOST_PAGE_BYTES == 0);
        assert!(
            allowance
                <= linear_bytes
                    .checked_sub(invocation.shared_static_bytes)
                    .expect("static credit exceeds initialized linear memory")
        );
        invocation.initialized_memory = Some(InitializedMemoryAccounting {
            linear_bytes,
            exposed_bytes: 0,
            peak_exposed_bytes: 0,
            allowance_bytes: allowance,
        });
        invocation.peak_initialized_other_bytes = invocation
            .current_guest_bytes
            .checked_sub(linear_bytes)
            .and_then(|bytes| bytes.checked_add(invocation.current_host_bytes))
            .expect("initialized attachment accounting overflow");
        invocation.shared_static_bytes = 0;
        let (_, reserved) = guest_memory_charge(
            invocation.current_guest_bytes,
            0,
            invocation.initialized_memory,
        )
        .expect("initialized memory charge overflow");
        let liability = reserved
            .checked_add(invocation.current_host_bytes)
            .expect("initialized memory charge overflow")
            .saturating_sub(invocation.checkout_baseline_bytes)
            .max(invocation.forecast_bytes);
        assert!(
            liability <= invocation.liability_bytes,
            "image attachment cannot borrow unreserved private memory"
        );
        invocation.liability_bytes = liability;
        drop(invocation_guard);
        state.update_slot_ledger(self.account.slot_id);
        controller.record_state_locked(&state);
        drop(state);
        controller.changed.notify_waiters();
    }

    /// The selected slot's observed backing plus its admitted growth forecast
    /// supplies a bounded grant. Host allocations remain charged separately and
    /// can never spend memory promised to this grant. The caller installs this
    /// limit in the recorder before any guest call, including entry selection.
    pub(crate) fn begin_initialized_memory(&self, maximum: usize) -> usize {
        let controller = &self.account.controller;
        let mut state = controller.state.lock();
        let mut invocation_guard = state
            .active
            .get_mut(&self.account.invocation_id)
            .expect("initialized memory checkout lost its active invocation")
            .lock();
        let invocation = &mut *invocation_guard;
        let initialized = invocation
            .initialized_memory
            .as_mut()
            .expect("initialized memory checkout has no image");
        let other = invocation
            .current_guest_bytes
            .checked_sub(initialized.linear_bytes)
            .expect("initialized linear memory exceeds guest capacity");
        // Request setup may have consumed part of the forecast already. Assign
        // only the still-reserved guest share, rather than promising it twice.
        let allowance = invocation
            .checkout_baseline_bytes
            .checked_add(invocation.liability_bytes)
            .and_then(|bytes| bytes.checked_sub(invocation.current_host_bytes))
            .and_then(|bytes| bytes.checked_sub(other))
            .expect("initialized memory checkout has insufficient reservation")
            .min(maximum)
            / HOST_PAGE_BYTES
            * HOST_PAGE_BYTES;
        assert!(
            allowance >= initialized.exposed_bytes,
            "initialized memory grant excludes retained backing"
        );
        initialized.allowance_bytes = allowance;
        initialized.peak_exposed_bytes = initialized.exposed_bytes;
        let (_, reserved) = guest_memory_charge(
            invocation.current_guest_bytes,
            0,
            invocation.initialized_memory,
        )
        .expect("initialized memory charge overflow");
        let liability = reserved
            .checked_add(invocation.current_host_bytes)
            .expect("initialized memory charge overflow")
            .saturating_sub(invocation.checkout_baseline_bytes)
            .max(invocation.forecast_bytes);
        // Checkout already admitted baseline + forecast. This operation only
        // assigns that reservation; it cannot request more global capacity.
        assert!(
            liability <= invocation.liability_bytes,
            "initialized memory grant exceeds checkout admission"
        );
        invocation.liability_bytes = liability;
        drop(invocation_guard);
        state.update_slot_ledger(self.account.slot_id);
        controller.record_state_locked(&state);
        allowance
    }
}

impl GeneratedStoreLimiter {
    /// At an exclusive Store boundary, move fault-side credit into the ordinary
    /// invocation ledger. No memory is granted or released by this transfer.
    pub(crate) fn absorb_initialized_loan(&self, loan: &InitializedMemoryLoan) -> usize {
        let bytes = loan.borrowed_bytes();
        if bytes == 0 {
            return 0;
        }
        let controller = &self.account.controller;
        let mut state = controller.state.lock();
        // Settle the full loan atomically before releasing any overlapping
        // readiness headroom. The fault-side ledger must never see the loan
        // disappear before ordinary liability owns every borrowed page.
        let ordinary = controller
            .checked_ordinary_projection(&state, 0)
            .and_then(|ordinary| ordinary.checked_add(bytes))
            // Committed host backing can overflow the aggregate usize
            // projection. Saturate only the fault-side representation; the
            // wide ledger still owns the exact charge and must permit cleanup.
            .unwrap_or(usize::MAX);
        let mut invocation_guard = state
            .active
            .get_mut(&self.account.invocation_id)
            .expect("initialized overflow transfer lost its invocation")
            .lock();
        let invocation = &mut *invocation_guard;
        let initialized = invocation
            .initialized_memory
            .as_mut()
            .expect("initialized overflow transfer has no image");
        initialized.allowance_bytes = initialized
            .allowance_bytes
            .checked_add(bytes)
            .expect("initialized overflow allowance overflow");
        invocation.liability_bytes = invocation
            .liability_bytes
            .checked_add(bytes)
            .expect("initialized overflow liability overflow");
        loan.transfer_to_ordinary(&controller.overflow, ordinary);
        // The shared projection can now release readiness already covered by
        // the increased invocation liability, without a gap in loan ownership.
        drop(invocation_guard);
        state.update_slot_ledger(self.account.slot_id);
        controller.record_state_locked(&state);
        bytes
    }

    /// Called after the actual raw capacities have been reconciled. A poisoned
    /// recorder may include failed growth, so retain the larger exposure until
    /// disposal. This updates evidence, never retroactively grants permission.
    pub(crate) fn reconcile_initialized_memory(
        &self,
        linear_bytes: usize,
        exposed_bytes: usize,
        peak_exposed_bytes: usize,
        allowance_bytes: usize,
        denied: bool,
    ) {
        let controller = &self.account.controller;
        let mut state = controller.state.lock();
        let mut invocation_guard = state
            .active
            .get_mut(&self.account.invocation_id)
            .expect("initialized memory reconciliation lost its invocation")
            .lock();
        let invocation = &mut *invocation_guard;
        let initialized = invocation
            .initialized_memory
            .as_mut()
            .expect("initialized memory reconciliation has no image");
        assert_eq!(allowance_bytes, initialized.allowance_bytes);
        assert!(exposed_bytes <= peak_exposed_bytes && peak_exposed_bytes <= allowance_bytes);
        assert_eq!(
            linear_bytes, initialized.linear_bytes,
            "initialized exposure requires reconciled linear capacity"
        );
        initialized.exposed_bytes = exposed_bytes;
        initialized.peak_exposed_bytes = peak_exposed_bytes;
        let other = invocation
            .current_guest_bytes
            .checked_sub(linear_bytes)
            .expect("initialized linear memory exceeds guest capacity");
        invocation.observe_initialized_memory_peak(peak_exposed_bytes, denied);
        let reserved = other
            .checked_add(allowance_bytes)
            .and_then(|bytes| bytes.checked_add(invocation.current_host_bytes))
            .expect("initialized memory charge overflow");
        let liability = reserved
            .saturating_sub(invocation.checkout_baseline_bytes)
            .max(invocation.forecast_bytes);
        // Sampling actual exposure cannot add authority beyond the write grant.
        // Raw linear growth is tracked separately from internal GC capacity by
        // the limiter and does not add a second charge for this same grant.
        assert!(
            liability <= invocation.liability_bytes,
            "initialized memory reconciliation exceeded its reservation"
        );
        invocation.liability_bytes = liability;
        drop(invocation_guard);
        state.update_slot_ledger(self.account.slot_id);
        controller.record_state_locked(&state);
        drop(state);
        controller.changed.notify_waiters();
    }
}

impl ActiveInvocation {
    fn observe_initialized_memory_peak(&mut self, peak_bytes: usize, denied: bool) {
        let initialized = self
            .initialized_memory
            .expect("initialized memory observation has no image");
        let other = self
            .current_guest_bytes
            .checked_sub(initialized.linear_bytes)
            .expect("initialized linear memory exceeds guest capacity");
        // Host and guest peaks may occur at different moments. Their sum is a
        // conservative observed upper bound, unlike the unused write allowance.
        let observed_peak = other
            .saturating_add(peak_bytes)
            .saturating_add(self.peak_host_bytes);
        let requested_peak = self
            .peak_initialized_other_bytes
            .saturating_add(peak_bytes)
            .max(observed_peak);
        self.peak_accounted_bytes = self.peak_accounted_bytes.max(requested_peak);
        // Every recorded page already consumed a local grant or a loan. A
        // later denial cannot erase that admitted exposure. The host peak and
        // current GC capacity exclude rejected allocations, so this does not
        // promote the separately retained requested demand into admission.
        self.peak_admitted_bytes = self.peak_admitted_bytes.max(observed_peak);
        self.growth_denied |= denied;
        if requested_peak.saturating_sub(self.checkout_baseline_bytes) > self.forecast_bytes
            && !self.prediction_overrun_recorded
        {
            self.prediction_overrun_recorded = true;
            log_memory_event("prediction_overrun");
        }
    }
}
