use std::fmt;
use std::time::{Duration, Instant};

use rand::RngExt;

pub(crate) const SNAPSHOT_MIN_INTERVAL: Duration =
    Duration::from_secs(1);
pub(crate) const SNAPSHOT_BACKOFF_BASE: Duration =
    Duration::from_millis(500);
pub(crate) const SNAPSHOT_BACKOFF_CAP: Duration =
    Duration::from_secs(30);
/// Once the book has stayed on one anchor this long, both backoffs start from scratch.
pub(crate) const LIVE_RESET_AFTER: Duration = Duration::from_secs(60);
/// Binance bans last up to three days. A longer `Retry-After` is treated as garbage.
pub(crate) const RETRY_AFTER_CAP: Duration =
    Duration::from_secs(3 * 24 * 60 * 60);

/// After this many sends the snapshot backoff is already at its cap. An IP ban sets the counter
/// straight to it.
const SENDS_AT_CAP: u32 = 16;

/// The nominal delay doubles with every failure, up to `cap`. The actual delay is a random point
/// in the upper half of the nominal, so that after a venue restart its clients come back spread
/// out instead of all at once.
pub(crate) fn delay_after(
    base: Duration,
    cap: Duration,
    failures: u32,
) -> Duration {
    let doubled = base.saturating_mul(2u32.saturating_pow(failures));
    let nominal = doubled.min(cap);
    rand::rng().random_range(nominal / 2..=nominal)
}

pub(crate) struct Backoff {
    base: Duration,
    cap: Duration,
    failures: u32,
}

impl Backoff {
    pub(crate) fn new(base: Duration, cap: Duration) -> Self {
        Backoff {
            base,
            cap,
            failures: 0,
        }
    }

    pub(crate) fn failures(&self) -> u32 {
        self.failures
    }

    /// Also counts the attempt, so the next delay is longer.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let delay = delay_after(self.base, self.cap, self.failures);
        self.failures = self.failures.saturating_add(1);
        delay
    }

    pub(crate) fn reset(&mut self) {
        self.failures = 0;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RateLimit {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// Keeps snapshot requests at least a second apart and backs off with every request actually
/// sent in the current cycle. A `Retry-After` from the exchange is always waited out in full.
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
                now + delay_after(
                    SNAPSHOT_BACKOFF_BASE,
                    SNAPSHOT_BACKOFF_CAP,
                    sent - 1,
                ),
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
        self.requests_in_cycle =
            self.requests_in_cycle.saturating_add(1);
    }

    pub(crate) fn rate_limited(
        &mut self,
        limit: RateLimit,
        retry_after: Option<Duration>,
        now: Instant,
    ) {
        self.requests_in_cycle = match limit {
            RateLimit::TooManyRequests => {
                self.requests_in_cycle.saturating_add(1)
            }
            RateLimit::IpBan => SENDS_AT_CAP,
        };
        if let Some(retry_after) = retry_after {
            self.not_before =
                Some((now + retry_after.min(RETRY_AFTER_CAP), limit));
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

    const BASE: Duration = Duration::from_millis(500);
    const CAP: Duration = Duration::from_secs(30);

    fn assert_within(delay: Duration, low: Duration, high: Duration) {
        assert!(
            delay >= low && delay <= high,
            "{delay:?} not within [{low:?}, {high:?}]"
        );
    }

    #[test]
    fn delays_double_from_base_within_jitter_bounds() {
        let mut backoff = Backoff::new(BASE, CAP);
        assert_within(backoff.next_delay(), BASE / 2, BASE);
        assert_within(backoff.next_delay(), BASE, BASE * 2);
        assert_within(backoff.next_delay(), BASE * 2, BASE * 4);
        assert_eq!(backoff.failures(), 3);
    }

    #[test]
    fn delays_never_exceed_the_cap() {
        let mut backoff = Backoff::new(BASE, CAP);
        for _ in 0..40 {
            assert!(backoff.next_delay() <= CAP);
        }
        assert_within(backoff.next_delay(), CAP / 2, CAP);
    }

    #[test]
    fn reset_starts_the_sequence_over() {
        let mut backoff = Backoff::new(BASE, CAP);
        backoff.next_delay();
        backoff.next_delay();
        backoff.reset();
        assert_eq!(backoff.failures(), 0);
        assert_within(backoff.next_delay(), BASE / 2, BASE);
    }

    fn limiter() -> SnapshotLimiter {
        SnapshotLimiter::default()
    }

    #[test]
    fn scheduling_without_sending_does_not_escalate() {
        let now = Instant::now();
        let limiter = limiter();
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
        let mut limiter = limiter();
        limiter.sent(now);
        let fetch =
            limiter.schedule(now + Duration::from_millis(100));
        assert_eq!(fetch.send_at, now + SNAPSHOT_MIN_INTERVAL);
        assert_eq!(fetch.cause, Some(WaitCause::MinInterval));
    }

    #[test]
    fn later_requests_in_a_cycle_wait_for_the_backoff() {
        let now = Instant::now();
        let mut limiter = limiter();
        limiter.sent(now);
        limiter.sent(now + Duration::from_secs(5));
        let third = limiter.schedule(now + Duration::from_secs(10));
        assert_eq!(
            third.cause,
            Some(WaitCause::Backoff { attempt: 3 })
        );
        assert_within(
            third.send_at - now,
            Duration::from_millis(10_500),
            Duration::from_secs(11),
        );
    }

    #[test]
    fn retry_after_is_never_undercut() {
        let now = Instant::now();
        let mut limiter = limiter();
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
    fn absurd_retry_after_is_capped_instead_of_overflowing() {
        let now = Instant::now();
        let mut limiter = limiter();
        limiter.rate_limited(
            RateLimit::IpBan,
            Some(Duration::MAX),
            now,
        );
        let fetch = limiter.schedule(now);
        assert_eq!(fetch.send_at, now + RETRY_AFTER_CAP);
    }

    #[test]
    fn ip_ban_without_retry_after_waits_the_full_cap() {
        let now = Instant::now();
        let mut limiter = limiter();
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
        let mut limiter = limiter();
        limiter.sent(now);
        limiter.sent(now);
        limiter.reset();
        let fetch = limiter.schedule(now + Duration::from_secs(2));
        assert_eq!(fetch.send_at, now + Duration::from_secs(2));
        assert_eq!(limiter.attempts(), 0);
    }
}
