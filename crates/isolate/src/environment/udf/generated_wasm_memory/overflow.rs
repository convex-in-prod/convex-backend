//! Precharged initialized-memory overflow. The controller remains the owner of
//! ordinary admission; a write fault can only consume its unassigned headroom.
//! Ordinary pages and loans share one atomic word so admission cannot spend
//! credit concurrently borrowed by another Store. Unused credit is not demand.

use std::sync::{
    atomic::{
        AtomicBool,
        AtomicU64,
        AtomicUsize,
        Ordering,
    },
    Arc,
};

use packed_value::memory::BufferMemory;

pub(crate) const PRIVATE_PAGE_BYTES: usize = 4096;
const PAGE_MASK: u64 = u32::MAX as u64;

pub(crate) struct InitializedMemoryReserve {
    // High word: rounded ordinary charges, excluding shared packed backing.
    // Low word: outstanding private-page loans. The representable reserve is
    // bounded to 16 TiB; larger ordinary charges disable additional borrowing.
    pages: AtomicU64,
    hard_bytes: usize,
    shared_buffers: &'static BufferMemory,
    pressure: AtomicBool,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OverflowInspection {
    pub(super) available_bytes: usize,
    pub(super) borrowed_bytes: usize,
}

impl InitializedMemoryReserve {
    pub(super) fn new(hard_bytes: usize, shared_buffers: &'static BufferMemory) -> Self {
        Self {
            pages: AtomicU64::new(0),
            hard_bytes,
            shared_buffers,
            pressure: AtomicBool::new(false),
        }
    }

    /// Controller lock required. This is the allocation's linearization point:
    /// validate live loans and packed backing before reserving ordinary bytes.
    pub(super) fn try_reserve_ordinary(&self, bytes: usize, limit: usize) -> bool {
        self.pages
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                let borrowed = (state & PAGE_MASK) as usize * PRIVATE_PAGE_BYTES;
                let projected = bytes
                    .checked_add(borrowed)?
                    .checked_add(self.shared_buffers.bytes())?;
                (projected <= limit).then_some(
                    ((bytes.div_ceil(PRIVATE_PAGE_BYTES).min(u32::MAX as usize) as u64) << 32)
                        | (state & PAGE_MASK),
                )
            })
            .is_ok()
    }

    /// Commit the controller's ledger after a release or a previously reserved
    /// change. Post-allocation host failures must also retain their real
    /// charge, even when external owners have since crossed the budget.
    pub(super) fn record_ordinary(&self, bytes: usize) {
        let ordinary = bytes.div_ceil(PRIVATE_PAGE_BYTES).min(u32::MAX as usize) as u64;
        let mut state = self.pages.load(Ordering::Acquire);
        loop {
            if state >> 32 == ordinary {
                return;
            }
            match self.pages.compare_exchange_weak(
                state,
                (ordinary << 32) | (state & PAGE_MASK),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(current) => state = current,
            }
        }
    }

    /// Existing ordinary credit can be spent without changing the shared
    /// reservation word. Re-read external backing on each increase, as those
    /// owners may exceed the budget independently. Rounded ordinary bytes are
    /// conservative; an ambiguous boundary uses the controller's exact check.
    pub(super) fn ordinary_grant_is_available(&self) -> bool {
        let state = self.pages.load(Ordering::Acquire);
        let ordinary_pages = state >> 32;
        if ordinary_pages == PAGE_MASK {
            // The saturated representation cannot prove full-width capacity.
            return false;
        }
        let borrowed_pages = state & PAGE_MASK;
        let Some(reserved) = (ordinary_pages as usize)
            .checked_add(borrowed_pages as usize)
            .and_then(|pages| pages.checked_mul(PRIVATE_PAGE_BYTES))
        else {
            return false;
        };
        reserved
            .checked_add(self.shared_buffers.bytes())
            .is_some_and(|bytes| bytes <= self.hard_bytes)
    }

    pub(super) fn set_pressure(&self, active: bool) {
        self.pressure.store(active, Ordering::Release);
    }

    pub(super) fn inspect(&self, shared_buffers: usize) -> OverflowInspection {
        let state = self.pages.load(Ordering::Acquire);
        let ordinary = (state >> 32) as usize;
        let borrowed = (state & PAGE_MASK) as usize;
        let available = if self.pressure.load(Ordering::Acquire) {
            0
        } else {
            (self.hard_bytes / PRIVATE_PAGE_BYTES)
                .min(u32::MAX as usize)
                .saturating_sub(ordinary)
                .saturating_sub(borrowed)
                .saturating_sub(shared_buffers.div_ceil(PRIVATE_PAGE_BYTES))
        };
        OverflowInspection {
            available_bytes: available * PRIVATE_PAGE_BYTES,
            borrowed_bytes: borrowed * PRIVATE_PAGE_BYTES,
        }
    }

    pub(super) fn borrowed_bytes(&self) -> usize {
        (self.pages.load(Ordering::Acquire) & PAGE_MASK) as usize * PRIVATE_PAGE_BYTES
    }

    pub(crate) fn loan(self: &Arc<Self>) -> InitializedMemoryLoan {
        InitializedMemoryLoan {
            reserve: Arc::clone(self),
            pages: AtomicUsize::new(0),
        }
    }
}

/// One exclusively used Store owns a loan. Keep it alive until the mapping is
/// destroyed or its entire charge has transferred to ordinary accounting.
/// Dropping it may notify admission and must never happen in signal context.
pub(crate) struct InitializedMemoryLoan {
    reserve: Arc<InitializedMemoryReserve>,
    pages: AtomicUsize,
}

impl InitializedMemoryLoan {
    pub(crate) fn available_pages(&self) -> usize {
        self.reserve
            .inspect(self.reserve.shared_buffers.bytes())
            .available_bytes
            / PRIVATE_PAGE_BYTES
    }

    /// Signal-safe: only lock-free atomics and integer arithmetic. Independent
    /// packed owners can grow outside this controller; re-read their live
    /// charge on every attempt instead of caching a headroom sample at
    /// checkout.
    pub(crate) fn try_borrow(&self, pages: usize) -> bool {
        let borrowed =
            self.reserve
                .pages
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                    if self.reserve.pressure.load(Ordering::Acquire) {
                        return None;
                    }
                    let ordinary = (state >> 32) as usize;
                    let current = (state & PAGE_MASK) as usize;
                    let next = current.checked_add(pages)?;
                    let limit =
                        (self.reserve.hard_bytes / PRIVATE_PAGE_BYTES).min(u32::MAX as usize);
                    let packed = self
                        .reserve
                        .shared_buffers
                        .bytes()
                        .div_ceil(PRIVATE_PAGE_BYTES);
                    (ordinary.checked_add(next)?.checked_add(packed)? <= limit)
                        .then_some((state & !PAGE_MASK) | next as u64)
                });
        if borrowed.is_err() {
            return false;
        }
        // Exclusive Store ownership prevents another operation on this loan;
        // global credit is charged before any private page becomes writable.
        self.pages.fetch_add(pages, Ordering::Release);
        true
    }

    pub(crate) fn borrowed_bytes(&self) -> usize {
        self.pages.load(Ordering::Acquire) * PRIVATE_PAGE_BYTES
    }

    /// Exclusive Store ownership and controller lock required. Transfer the
    /// loan to ordinary retained liability without returning available credit.
    /// This changes ownership, not total permission to allocate memory.
    pub(super) fn transfer_to_ordinary(
        &self,
        reserve: &Arc<InitializedMemoryReserve>,
        ordinary_bytes: usize,
    ) {
        assert!(
            Arc::ptr_eq(&self.reserve, reserve),
            "initialized loan belongs to another controller"
        );
        let loan = self.pages.load(Ordering::Acquire);
        if loan == 0 {
            return;
        }
        // A committed host allocation can cross the representable bound while
        // a loan is outstanding. Keep borrowing disabled, but still transfer
        // that loan into the full-width ordinary ledger without panicking.
        let ordinary = ordinary_bytes
            .div_ceil(PRIVATE_PAGE_BYTES)
            .min(u32::MAX as usize);
        self.reserve
            .pages
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                let old_ordinary = (state >> 32) as usize;
                let borrowed = (state & PAGE_MASK) as usize;
                assert!(
                    borrowed >= loan && ordinary == (old_ordinary + loan).min(u32::MAX as usize)
                );
                Some(((ordinary as u64) << 32) | (borrowed - loan) as u64)
            })
            .unwrap();
        self.pages.store(0, Ordering::Release);
    }
}

impl Drop for InitializedMemoryLoan {
    fn drop(&mut self) {
        let loan = *self.pages.get_mut();
        if loan == 0 {
            return;
        }
        // Mapping destruction precedes loan destruction. Clearing ordinary
        // invocation admission earlier must not release these private pages.
        self.reserve
            .pages
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                let borrowed = (state & PAGE_MASK) as usize;
                assert!(borrowed >= loan);
                Some((state & !PAGE_MASK) | (borrowed - loan) as u64)
            })
            .unwrap();
        self.reserve.shared_buffers.changed().notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use super::*;

    #[test]
    fn loans_transfer_without_creating_available_credit() {
        let buffers = Box::leak(Box::new(BufferMemory::new()));
        let reserve = Arc::new(InitializedMemoryReserve::new(
            16 * PRIVATE_PAGE_BYTES,
            buffers,
        ));
        assert!(reserve.try_reserve_ordinary(4 * PRIVATE_PAGE_BYTES, 16 * PRIVATE_PAGE_BYTES));
        let first = reserve.loan();
        assert!(first.try_borrow(8));
        assert!(!reserve.try_reserve_ordinary(9 * PRIVATE_PAGE_BYTES, 16 * PRIVATE_PAGE_BYTES));
        assert!(reserve.try_reserve_ordinary(8 * PRIVATE_PAGE_BYTES, 16 * PRIVATE_PAGE_BYTES));
        let second = reserve.loan();
        assert!(!second.try_borrow(1));
        first.transfer_to_ordinary(&reserve, 16 * PRIVATE_PAGE_BYTES);
        drop(first);
        assert_eq!(reserve.inspect(0).available_bytes, 0);
        assert_eq!(reserve.borrowed_bytes(), 0);
        reserve.record_ordinary(8 * PRIVATE_PAGE_BYTES);
        assert!(second.try_borrow(8));
        drop(second);
        assert_eq!(reserve.inspect(0).available_bytes, 8 * PRIVATE_PAGE_BYTES);
    }

    #[test]
    fn packed_backing_and_pressure_limit_new_loans() {
        let buffers = Box::leak(Box::new(BufferMemory::new()));
        let reserve = Arc::new(InitializedMemoryReserve::new(
            16 * PRIVATE_PAGE_BYTES,
            buffers,
        ));
        reserve.record_ordinary(4 * PRIVATE_PAGE_BYTES);
        let packed = buffers.track(vec![0; 8 * PRIVATE_PAGE_BYTES]);
        let loan = reserve.loan();
        assert!(!loan.try_borrow(5));
        assert!(loan.try_borrow(4));
        drop(packed);
        reserve.set_pressure(true);
        assert!(!loan.try_borrow(1));
        assert_eq!(reserve.borrowed_bytes(), 4 * PRIVATE_PAGE_BYTES);
        reserve.set_pressure(false);
        assert!(loan.try_borrow(8));
        assert!(!loan.try_borrow(1));
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn committed_ordinary_overflow_does_not_prevent_loan_settlement() {
        let buffers = Box::leak(Box::new(BufferMemory::new()));
        let capacity = u32::MAX as usize * PRIVATE_PAGE_BYTES;
        let reserve = Arc::new(InitializedMemoryReserve::new(
            capacity + 2 * PRIVATE_PAGE_BYTES,
            buffers,
        ));
        reserve.record_ordinary(capacity - 2 * PRIVATE_PAGE_BYTES + 1);
        let loan = reserve.loan();
        assert!(loan.try_borrow(1));
        assert!(!loan.try_borrow(1));
        // Post-allocation accounting must keep real host backing even when it
        // exceeds the atomic representation; no new loan may spend that gap.
        reserve.record_ordinary(capacity + 1);
        loan.transfer_to_ordinary(&reserve, capacity + 1 + PRIVATE_PAGE_BYTES);
        assert_eq!(loan.borrowed_bytes(), 0);
        assert_eq!(reserve.borrowed_bytes(), 0);
        assert_eq!(reserve.inspect(0).available_bytes, 0);
        drop(loan);
        assert_eq!(reserve.inspect(0).available_bytes, 0);
    }

    #[test]
    fn concurrent_ordinary_admission_and_fault_cannot_spend_the_same_headroom() {
        let buffers = Box::leak(Box::new(BufferMemory::new()));
        for _ in 0..64 {
            let reserve = Arc::new(InitializedMemoryReserve::new(
                8 * PRIVATE_PAGE_BYTES,
                buffers,
            ));
            let barrier = Barrier::new(2);
            std::thread::scope(|scope| {
                let fault = scope.spawn(|| {
                    let loan = reserve.loan();
                    barrier.wait();
                    let accepted = loan.try_borrow(8);
                    // Keep the loan alive until ordinary admission has tried.
                    barrier.wait();
                    accepted
                });
                barrier.wait();
                let admitted =
                    reserve.try_reserve_ordinary(8 * PRIVATE_PAGE_BYTES, 8 * PRIVATE_PAGE_BYTES);
                barrier.wait();
                assert_ne!(admitted, fault.join().unwrap());
            });
        }
    }
}
