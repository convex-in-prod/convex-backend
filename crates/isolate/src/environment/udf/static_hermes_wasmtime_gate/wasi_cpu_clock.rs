use std::{
    future::{
        poll_fn,
        Future,
    },
    pin::pin,
    sync::Arc,
};

use parking_lot::Mutex;

use crate::execution_observation::thread_cpu;

/// A guest thread can move between executor threads at an epoch yield,
/// including in the middle of a synchronous GC section. Accumulate CPU within
/// each poll; executor-thread clocks cannot be compared across those suspension
/// boundaries.
#[derive(Clone)]
pub(super) struct WasiCpuClock(Arc<Mutex<ClockState>>);

struct ClockState {
    elapsed: Option<u64>,
    poll_started: Option<u64>,
    polling: bool,
}

impl Default for WasiCpuClock {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(ClockState {
            elapsed: Some(0),
            poll_started: None,
            polling: false,
        })))
    }
}

impl ClockState {
    fn current(&self) -> Option<u64> {
        let elapsed = self.elapsed?;
        let started = self.poll_started?;
        let delta = thread_cpu()?
            .checked_sub(started)
            .expect("thread CPU clock regressed within a poll");
        Some(elapsed.checked_add(delta).expect("WASI CPU clock overflow"))
    }
}

impl WasiCpuClock {
    /// An unavailable platform clock or a call outside a metered guest poll has
    /// no CPU reading. The WASI boundary reports NOTSUP for those cases.
    pub(super) fn current(&self) -> Option<u64> {
        self.0.lock().current()
    }

    pub(super) async fn measure<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        poll_fn(|context| {
            {
                let mut state = self.0.lock();
                assert!(!state.polling, "nested polls of the same WASI CPU clock");
                state.poll_started = thread_cpu();
                state.polling = true;
            }
            let _poll = ClockPoll(self);
            future.as_mut().poll(context)
        })
        .await
    }
}

struct ClockPoll<'a>(&'a WasiCpuClock);

impl Drop for ClockPoll<'_> {
    fn drop(&mut self) {
        let mut state = self.0 .0.lock();
        assert!(state.polling, "WASI CPU clock poll was not active");
        state.elapsed = state.current();
        state.poll_started = None;
        state.polling = false;
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::{
        task::{
            Context,
            Poll,
            Waker,
        },
        thread,
    };

    use super::*;

    fn consume_cpu(nanos: u64) {
        let started = thread_cpu().expect("Linux thread CPU clock");
        while thread_cpu().unwrap() - started < nanos {
            std::hint::spin_loop();
        }
    }

    #[test]
    fn wasi_cpu_clock_survives_executor_migration() {
        let clock = WasiCpuClock::default();
        let guest_clock = clock.clone();
        let mut future = Box::pin(async move {
            guest_clock
                .measure(async {
                    let before = clock.current().unwrap();
                    let raw_before = thread_cpu().unwrap();
                    let mut yielded = false;
                    poll_fn(|_| {
                        if yielded {
                            Poll::Ready(())
                        } else {
                            yielded = true;
                            Poll::Pending
                        }
                    })
                    .await;
                    consume_cpu(2_000_000);
                    (
                        before,
                        clock.current().unwrap(),
                        raw_before,
                        thread_cpu().unwrap(),
                    )
                })
                .await
        });
        let future = thread::spawn(move || {
            consume_cpu(30_000_000);
            assert!(future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
            // CPU spent on unrelated work while this guest is suspended must
            // not enter its clock, even on the same executor thread.
            consume_cpu(30_000_000);
            future
        })
        .join()
        .unwrap();
        let (before, after, raw_before, raw_after) = thread::spawn(move || {
            let mut future = future;
            match future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                Poll::Ready(result) => result,
                Poll::Pending => panic!("guest did not resume"),
            }
        })
        .join()
        .unwrap();
        assert!(
            raw_after < raw_before,
            "raw executor clocks must expose the regression"
        );
        assert!(after >= before + 2_000_000);
        assert!(
            after - before < 30_000_000,
            "suspended CPU entered the guest clock"
        );
    }

    #[test]
    fn wasi_cpu_clock_retains_only_polled_time_after_cancellation() {
        let clock = WasiCpuClock::default();
        let mut pending = Box::pin(clock.measure(async {
            consume_cpu(2_000_000);
            std::future::pending::<()>().await;
        }));
        assert!(pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(pending);
        assert!(clock.current().is_none());
        consume_cpu(30_000_000);
        let mut next = pin!(clock.measure(async { clock.current().unwrap() }));
        let elapsed = match next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(elapsed) => elapsed,
            Poll::Pending => panic!("clock read suspended"),
        };
        assert!(elapsed >= 2_000_000);
        assert!(
            elapsed < 30_000_000,
            "idle executor CPU entered the retained clock"
        );
    }
}
