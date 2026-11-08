use std::{
    collections::VecDeque,
    time::{
        Duration,
        Instant,
    },
};

// Retain more than a client timeout's worth of five-second heartbeats, while
// bounding memory for clients that send other messages but never echo pings.
const MAX_PENDING_PINGS: usize = 32;

struct PendingPing {
    id: u64,
    sent_at: Instant,
}

#[derive(Default)]
pub(super) struct PingTracker {
    next_id: u64,
    pending: VecDeque<PendingPing>,
}

impl PingTracker {
    pub(super) fn record_ping(&mut self, now: Instant) -> [u8; 8] {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        if self.pending.len() == MAX_PENDING_PINGS {
            self.pending.pop_front();
        }
        self.pending.push_back(PendingPing { id, sent_at: now });
        id.to_be_bytes()
    }

    pub(super) fn record_pong(&mut self, payload: &[u8], now: Instant) -> Option<Duration> {
        let id = u64::from_be_bytes(payload.try_into().ok()?);
        let index = self.pending.iter().position(|ping| ping.id == id)?;
        let ping = self
            .pending
            .remove(index)
            .expect("pending ping index exists");
        // A newer ping must not shorten an older ping's RTT. Removing the match
        // also prevents duplicate pongs from recording additional samples.
        Some(now - ping.sent_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delayed_pong_uses_its_ping_even_after_newer_pongs() {
        let mut tracker = PingTracker::default();
        let start = Instant::now();
        let first = tracker.record_ping(start);
        let second = tracker.record_ping(start + Duration::from_secs(5));
        let third = tracker.record_ping(start + Duration::from_secs(10));
        assert_eq!(
            tracker.record_pong(&third, start + Duration::from_secs(11)),
            Some(Duration::from_secs(1)),
        );
        assert_eq!(
            tracker.record_pong(&first, start + Duration::from_secs(41)),
            Some(Duration::from_secs(41)),
        );
        assert_eq!(
            tracker.record_pong(&second, start + Duration::from_secs(42)),
            Some(Duration::from_secs(37)),
        );
    }

    #[test]
    fn invalid_unknown_and_duplicate_pongs_do_not_record_rtt() {
        let mut tracker = PingTracker::default();
        let now = Instant::now();
        let payload = tracker.record_ping(now);
        assert_eq!(tracker.record_pong(&[], now), None);
        assert_eq!(tracker.record_pong(&[0; 9], now), None);
        assert_eq!(tracker.record_pong(&u64::MAX.to_be_bytes(), now), None);
        assert_eq!(tracker.record_pong(&payload, now), Some(Duration::ZERO));
        assert_eq!(tracker.record_pong(&payload, now), None);
    }

    #[test]
    fn pending_pings_are_bounded_without_losing_recent_matches() {
        let mut tracker = PingTracker::default();
        let now = Instant::now();
        let first = tracker.record_ping(now);
        let second = tracker.record_ping(now);
        for _ in 2..=MAX_PENDING_PINGS {
            tracker.record_ping(now);
        }
        assert_eq!(tracker.pending.len(), MAX_PENDING_PINGS);
        assert_eq!(tracker.record_pong(&first, now), None);
        assert_eq!(tracker.record_pong(&second, now), Some(Duration::ZERO));
        assert_eq!(tracker.pending.len(), MAX_PENDING_PINGS - 1);
    }
}
