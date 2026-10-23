use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backoff::{RateLimit, WaitCause};
use crate::book::Band;
use crate::fixed::{Px, Qty};
use crate::sync::{ResyncReason, SyncCounters};

#[derive(Clone, Copy, Default)]
pub(crate) struct FeedCounters {
    pub(crate) events_received: u64,
    pub(crate) bad_frames: u64,
    pub(crate) snapshots_fetched: u64,
    pub(crate) snapshot_errors: u64,
    pub(crate) reconnects: u64,
    pub(crate) pings: u64,
    pub(crate) views_published: u64,
    pub(crate) crossed_views: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DisconnectReason {
    Stale { silent_for: Duration },
    Closed { code: Option<u16> },
    RateLimited { limit: RateLimit, wait: Duration },
    Transport,
    Eof,
    ConnectFailed,
    ConnectTimeout,
    HandshakeRejected { status: u16 },
}

impl fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stale { silent_for } => {
                write!(f, "stale(silent={}ms)", silent_for.as_millis())
            }
            Self::Closed { code: Some(code) } => {
                write!(f, "closed(code={code})")
            }
            Self::Closed { code: None } => f.write_str("closed"),
            Self::RateLimited { limit, wait } => {
                write!(f, "rate-limited({limit},wait={}s)", wait.as_secs())
            }
            Self::Transport => f.write_str("transport-error"),
            Self::Eof => f.write_str("eof"),
            Self::ConnectFailed => f.write_str("connect-failed"),
            Self::ConnectTimeout => f.write_str("connect-timeout"),
            Self::HandshakeRejected { status } => {
                write!(f, "handshake-rejected(http={status})")
            }
        }
    }
}

#[derive(Clone)]
pub struct Failure {
    pub(crate) status: u16,
    pub(crate) body: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "http {}: {}", self.status, self.body)
    }
}

pub struct ReanchorStatus {
    pub(crate) pending_for: Duration,
    pub(crate) kept: usize,
}

pub enum FeedState {
    Connecting {
        attempt: u32,
    },
    Reconnecting {
        reason: DisconnectReason,
        retry_in: Duration,
    },
    Buffering {
        buffered: usize,
    },
    Stitching {
        last_update_id: u64,
        buffered: usize,
    },
    Live {
        reanchor: Option<ReanchorStatus>,
    },
    Resyncing {
        reason: ResyncReason,
        buffered: usize,
    },
    Failed(Failure),
}

impl fmt::Display for FeedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connecting { attempt } => {
                write!(f, "connecting#{attempt}")
            }
            Self::Reconnecting { reason, retry_in } => {
                write!(f, "reconnecting({reason},in={}ms)", retry_in.as_millis())
            }
            Self::Buffering { buffered } => {
                write!(f, "buffering(buf={buffered})")
            }
            Self::Stitching {
                last_update_id,
                buffered,
            } => write!(f, "stitching(N={last_update_id},buf={buffered})"),
            Self::Live { reanchor: None } => f.write_str("live"),
            Self::Live {
                reanchor: Some(pending),
            } => write!(
                f,
                "live(reanchor:pending {}ms,kept={})",
                pending.pending_for.as_millis(),
                pending.kept
            ),
            Self::Resyncing { reason, buffered } => {
                write!(f, "resyncing({reason},buf={buffered})")
            }
            Self::Failed(failure) => write!(f, "failed({failure})"),
        }
    }
}

pub(crate) enum SnapshotActivity {
    Idle,
    Scheduled {
        due_in: Duration,
        cause: Option<WaitCause>,
    },
    InFlight {
        elapsed: Duration,
    },
}

impl fmt::Display for SnapshotActivity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idle => f.write_str("idle"),
            Self::Scheduled {
                due_in,
                cause: Some(cause),
            } => write!(f, "throttled(in={}ms,{cause})", due_in.as_millis()),
            Self::Scheduled {
                due_in,
                cause: None,
            } => write!(f, "scheduled(in={}ms)", due_in.as_millis()),
            Self::InFlight { elapsed } => {
                write!(f, "in-flight({}ms)", elapsed.as_millis())
            }
        }
    }
}

pub struct FeedStatus {
    pub(crate) at: SystemTime,
    pub name: &'static str,
    pub state: FeedState,
    pub(crate) best_bid: Option<(Px, Qty)>,
    pub(crate) best_ask: Option<(Px, Qty)>,
    pub(crate) bid_levels: usize,
    pub(crate) ask_levels: usize,
    pub(crate) band: Option<Band>,
    pub(crate) since_last_event: Duration,
    pub(crate) latency_ms: Option<i64>,
    pub(crate) events_per_sec: u64,
    pub(crate) snapshot: SnapshotActivity,
    pub(crate) sync: SyncCounters,
    pub(crate) feed: FeedCounters,
}

impl fmt::Display for FeedStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {:<7} {}", TimeOfDay(self.at), self.name, self.state)?;
        match (self.best_bid, self.best_ask) {
            (Some((bid, bid_qty)), Some((ask, ask_qty))) => {
                write!(f, " bid={bid}x{bid_qty} ask={ask}x{ask_qty}")?;
            }
            (None, _) | (_, None) => f.write_str(" bid=- ask=-")?,
        }
        write!(f, " levels={}/{}", self.bid_levels, self.ask_levels)?;
        match self.band {
            Some(band) => write!(f, " band=[{}..{}]", band.bid_floor, band.ask_ceiling)?,
            None => f.write_str(" band=-")?,
        }
        write!(f, " last_event={}ms", self.since_last_event.as_millis())?;
        match self.latency_ms {
            Some(ms) => write!(f, " latency={ms}ms")?,
            None => f.write_str(" latency=-")?,
        }
        write!(
            f,
            " rate={}/s snapshot={}",
            self.events_per_sec, self.snapshot
        )?;
        // Zero counters are left out so the periodic line stays short.
        // The alternate form prints every one.
        let (sync, feed) = (self.sync, self.feed);
        let counters = [
            ("applied", sync.events_applied),
            ("reanchors", sync.reanchors),
            ("gaps", sync.gaps),
            ("band_exhausted", sync.band_exhausted),
            ("snapshot_rejected", sync.snapshot_rejected),
            ("stale", sync.stale_timeouts),
            ("buffer_dropped", sync.buffer_dropped),
            ("received", feed.events_received),
            ("bad_frames", feed.bad_frames),
            ("snapshots", feed.snapshots_fetched),
            ("snapshot_errors", feed.snapshot_errors),
            ("reconnects", feed.reconnects),
            ("pings", feed.pings),
            ("views", feed.views_published),
            ("crossed", feed.crossed_views),
        ];
        for (name, value) in counters {
            if value > 0 || f.alternate() {
                write!(f, " {name}={value}")?;
            }
        }
        Ok(())
    }
}

pub(crate) struct TimeOfDay(pub(crate) SystemTime);

impl fmt::Display for TimeOfDay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A pre-1970 clock reads as the epoch.
        let since_epoch = self.0.duration_since(UNIX_EPOCH).unwrap_or_default();
        let seconds = since_epoch.as_secs();
        write!(
            f,
            "{:02}:{:02}:{:02}.{:03}",
            seconds / 3600 % 24,
            seconds / 60 % 60,
            seconds % 60,
            since_epoch.subsec_millis()
        )
    }
}

/// Wall clock minus the time the venue stamped the event.
pub(crate) fn latency_ms(event_time_ms: u64) -> i64 {
    let event = UNIX_EPOCH + Duration::from_millis(event_time_ms);
    match SystemTime::now().duration_since(event) {
        Ok(behind) => i64::try_from(behind.as_millis()).unwrap_or(i64::MAX),
        Err(ahead) => -i64::try_from(ahead.duration().as_millis()).unwrap_or(i64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_counters_are_left_out_except_in_the_alternate_form() {
        let mut status = FeedStatus {
            at: UNIX_EPOCH,
            name: "binance",
            state: FeedState::Live { reanchor: None },
            best_bid: None,
            best_ask: None,
            bid_levels: 0,
            ask_levels: 0,
            band: None,
            since_last_event: Duration::ZERO,
            latency_ms: None,
            events_per_sec: 0,
            snapshot: SnapshotActivity::Idle,
            sync: SyncCounters::default(),
            feed: FeedCounters::default(),
        };
        assert!(status.to_string().ends_with(" snapshot=idle"));
        status.sync.gaps = 1;
        assert!(status.to_string().ends_with(" snapshot=idle gaps=1"));
        let full = format!("{status:#}");
        assert!(full.contains(" reconnects=0 "));
        assert!(full.ends_with(" crossed=0"));
    }

    #[test]
    fn stamp_prints_utc_time_of_day_with_milliseconds() {
        let at = UNIX_EPOCH + Duration::from_millis(90_061_500);
        assert_eq!(TimeOfDay(at).to_string(), "01:01:01.500");
    }
}
