use std::fmt;
use std::time::{Duration, Instant};

use rand::RngExt;

const SNAPSHOT_MIN_INTERVAL: Duration = Duration::from_secs(1);
/// One curve for reconnects and snapshot retries.
pub(crate) const BACKOFF_BASE: Duration = Duration::from_millis(500);
pub(crate) const BACKOFF_CAP: Duration = Duration::from_secs(30);

/// Where the backoff curve reaches its cap. An IP ban sets the counter to this value.
const SENDS_AT_CAP: u32 = 16;

/// Jittered into the upper half.
pub(crate) fn delay_after(failures: u32) -> Duration {
    let doubled = BACKOFF_BASE.saturating_mul(2u32.saturating_pow(failures));
    let nominal = doubled.min(BACKOFF_CAP);
    rand::rng().random_range(nominal / 2..=nominal)
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RateLimit {
    TooManyRequests,
    IpBan,
}

impl RateLimit {
    pub(crate) fn from_status(status: u16) -> Option<RateLimit> {
        match status {
            429 => Some(RateLimit::TooManyRequests),
            418 => Some(RateLimit::IpBan),
            _ => None,
        }
    }
}

impl fmt::Display for RateLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RateLimit::TooManyRequests => f.write_str("http-429"),
            RateLimit::IpBan => f.write_str("http-418-ip-ban"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum WaitCause {
    MinInterval,
    Backoff { attempt: u32 },
    RateLimited(RateLimit),
}

impl fmt::Display for WaitCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaitCause::MinInterval => f.write_str("min-interval"),
            WaitCause::Backoff { attempt } => {
                write!(f, "backoff#{attempt}")
            }
            WaitCause::RateLimited(limit) => write!(f, "{limit}"),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ScheduledFetch {
    pub(crate) send_at: Instant,
    pub(crate) cause: Option<WaitCause>,
}

/// A cycle of requests ends when one of them anchors a book, only wasted requests escalate.
#[derive(Default)]
pub(crate) struct SnapshotLimiter {
    requests_in_cycle: u32,
    last_request_at: Option<Instant>,
    not_before: Option<(Instant, RateLimit)>,
}

impl SnapshotLimiter {
    pub(crate) fn schedule(&self, now: Instant) -> ScheduledFetch {
        let (mut send_at, mut cause) = match self.requests_in_cycle {
            0 => (now, None),
            sent => (
                now + delay_after(sent - 1),
                Some(WaitCause::Backoff { attempt: sent + 1 }),
            ),
        };
        if let Some(last) = self.last_request_at
            && last + SNAPSHOT_MIN_INTERVAL > send_at
        {
            send_at = last + SNAPSHOT_MIN_INTERVAL;
            cause = Some(WaitCause::MinInterval);
        }
        if let Some((until, limit)) = self.not_before
            && until > send_at
        {
            send_at = until;
            cause = Some(WaitCause::RateLimited(limit));
        }
        ScheduledFetch { send_at, cause }
    }

    pub(crate) fn sent(&mut self, now: Instant) {
        self.last_request_at = Some(now);
        self.requests_in_cycle = self.requests_in_cycle.saturating_add(1);
    }

    pub(crate) fn rate_limited(
        &mut self,
        limit: RateLimit,
        retry_after: Option<Duration>,
        now: Instant,
    ) {
        self.requests_in_cycle = match limit {
            RateLimit::TooManyRequests => self.requests_in_cycle.saturating_add(1),
            RateLimit::IpBan => SENDS_AT_CAP,
        };
        if let Some(retry_after) = retry_after {
            self.not_before = Some((now + retry_after, limit));
        }
    }

    pub(crate) fn attempts(&self) -> u32 {
        self.requests_in_cycle
    }

    pub(crate) fn reset(&mut self) {
        self.requests_in_cycle = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: Duration = BACKOFF_BASE;
    const CAP: Duration = BACKOFF_CAP;

    fn assert_within(delay: Duration, low: Duration, high: Duration) {
        assert!(
            delay >= low && delay <= high,
            "{delay:?} not within [{low:?}, {high:?}]"
        );
    }

    #[test]
    fn delays_double_from_base_within_jitter_bounds() {
        assert_within(delay_after(0), BASE / 2, BASE);
        assert_within(delay_after(1), BASE, BASE * 2);
        assert_within(delay_after(2), BASE * 2, BASE * 4);
    }

    #[test]
    fn delays_never_exceed_the_cap() {
        for failures in 0..40 {
            assert!(delay_after(failures) <= CAP);
        }
        assert_within(delay_after(40), CAP / 2, CAP);
    }

    #[test]
    fn scheduling_without_sending_does_not_escalate() {
        let now = Instant::now();
        let limiter = SnapshotLimiter::default();
        let first = limiter.schedule(now);
        assert_eq!(first.send_at, now);
        assert_eq!(first.cause, None);
        let again = limiter.schedule(now);
        assert_eq!(again.send_at, now);
        assert_eq!(again.cause, None);
        assert_eq!(limiter.attempts(), 0);
    }

    #[test]
    fn requests_stay_at_least_one_second_apart() {
        let now = Instant::now();
        let mut limiter = SnapshotLimiter::default();
        limiter.sent(now);
        let fetch = limiter.schedule(now + Duration::from_millis(100));
        assert_eq!(fetch.send_at, now + SNAPSHOT_MIN_INTERVAL);
        assert_eq!(fetch.cause, Some(WaitCause::MinInterval));
    }

    #[test]
    fn later_requests_in_a_cycle_wait_for_the_backoff() {
        let now = Instant::now();
        let mut limiter = SnapshotLimiter::default();
        limiter.sent(now);
        limiter.sent(now + Duration::from_secs(5));
        let third = limiter.schedule(now + Duration::from_secs(10));
        assert_eq!(third.cause, Some(WaitCause::Backoff { attempt: 3 }));
        assert_within(
            third.send_at - now,
            Duration::from_millis(10_500),
            Duration::from_secs(11),
        );
    }

    #[test]
    fn a_429_without_retry_after_costs_one_backoff_step() {
        let now = Instant::now();
        let mut limiter = SnapshotLimiter::default();
        limiter.rate_limited(RateLimit::TooManyRequests, None, now);
        assert_eq!(
            limiter.schedule(now + Duration::from_secs(1)).cause,
            Some(WaitCause::Backoff { attempt: 2 })
        );
    }

    #[test]
    fn retry_after_is_never_undercut() {
        let now = Instant::now();
        let mut limiter = SnapshotLimiter::default();
        limiter.rate_limited(
            RateLimit::TooManyRequests,
            Some(Duration::from_secs(90)),
            now,
        );
        let fetch = limiter.schedule(now);
        assert_eq!(fetch.send_at, now + Duration::from_secs(90));
        assert_eq!(
            fetch.cause,
            Some(WaitCause::RateLimited(RateLimit::TooManyRequests))
        );
    }

    #[test]
    fn ip_ban_without_retry_after_waits_the_full_cap() {
        let now = Instant::now();
        let mut limiter = SnapshotLimiter::default();
        limiter.sent(now);
        limiter.rate_limited(RateLimit::IpBan, None, now);
        let fetch = limiter.schedule(now + Duration::from_secs(1));
        assert_within(
            fetch.send_at - now,
            CAP / 2 + Duration::from_secs(1),
            CAP + Duration::from_secs(1),
        );
    }

    #[test]
    fn reset_makes_the_next_request_immediate_again() {
        let now = Instant::now();
        let mut limiter = SnapshotLimiter::default();
        limiter.sent(now);
        limiter.sent(now);
        limiter.reset();
        let fetch = limiter.schedule(now + Duration::from_secs(2));
        assert_eq!(fetch.send_at, now + Duration::from_secs(2));
        assert_eq!(limiter.attempts(), 0);
    }
}
