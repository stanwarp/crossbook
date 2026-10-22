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
}

#[derive(Clone, Copy)]
pub(crate) enum DisconnectReason {
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
                write!(f, "stale(silent={}ms)", millis(*silent_for))
            }
            Self::Closed { code: Some(code) } => {
                write!(f, "closed(code={code})")
            }
            Self::Closed { code: None } => f.write_str("closed"),
            Self::RateLimited { limit, wait } => write!(
                f,
                "rate-limited({limit},wait={}s)",
                wait.as_secs()
            ),
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

pub(crate) struct Failure {
    pub(crate) status: u16,
    pub(crate) body: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "http {}: {}", self.status, self.body)
    }
}

pub(crate) struct ReanchorStatus {
    pub(crate) pending_for: Duration,
    pub(crate) kept: usize,
}

pub(crate) enum FeedState {
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
            Self::Reconnecting { reason, retry_in } => write!(
                f,
                "reconnecting({reason},in={}ms)",
                millis(*retry_in)
            ),
            Self::Buffering { buffered } => {
                write!(f, "buffering(buf={buffered})")
            }
            Self::Stitching {
                last_update_id,
                buffered,
            } => write!(
                f,
                "stitching(N={last_update_id},buf={buffered})"
            ),
            Self::Live { reanchor: None } => f.write_str("live"),
            Self::Live {
                reanchor: Some(pending),
            } => write!(
                f,
                "live(reanchor:in-flight {}ms,kept={})",
                millis(pending.pending_for),
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
            } => write!(
                f,
                "throttled(in={}ms,{cause})",
                millis(*due_in)
            ),
            Self::Scheduled {
                due_in,
                cause: None,
            } => write!(f, "scheduled(in={}ms)", millis(*due_in)),
            Self::InFlight { elapsed } => {
                write!(f, "in-flight({}ms)", millis(*elapsed))
            }
        }
    }
}

pub(crate) struct FeedStatus {
    pub(crate) at: SystemTime,
    pub(crate) name: &'static str,
    pub(crate) state: FeedState,
    pub(crate) best_bid: Option<(Px, Qty)>,
    pub(crate) best_ask: Option<(Px, Qty)>,
    pub(crate) bid_levels: usize,
    pub(crate) ask_levels: usize,
    pub(crate) band: Option<Band>,
    pub(crate) since_last_event: Duration,
    pub(crate) events_per_sec: u64,
    pub(crate) snapshot: SnapshotActivity,
    pub(crate) sync: SyncCounters,
    pub(crate) feed: FeedCounters,
}

impl fmt::Display for FeedStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (hours, minutes, seconds, ms) = clock(self.at);
        write!(
            f,
            "{hours:02}:{minutes:02}:{seconds:02}.{ms:03} {:<7} {}",
            self.name, self.state
        )?;
        match (self.best_bid, self.best_ask) {
            (Some((bid, bid_qty)), Some((ask, ask_qty))) => {
                write!(
                    f,
                    " bid={bid}x{bid_qty} ask={ask}x{ask_qty}"
                )?;
            }
            (None, _) | (_, None) => f.write_str(" bid=- ask=-")?,
        }
        write!(f, " levels={}/{}", self.bid_levels, self.ask_levels)?;
        match self.band {
            Some(band) => write!(
                f,
                " band=[{}..{}]",
                band.bid_floor, band.ask_ceiling
            )?,
            None => f.write_str(" band=-")?,
        }
        write!(
            f,
            " last_event={}ms rate={}/s snapshot={}",
            millis(self.since_last_event),
            self.events_per_sec,
            self.snapshot
        )?;
        let (sync, feed) = (self.sync, self.feed);
        write!(
            f,
            " applied={} reanchors={} gaps={} band_exhausted={} snapshot_rejected={} stale={} buffer_dropped={}",
            sync.events_applied,
            sync.reanchors,
            sync.gaps,
            sync.band_exhausted,
            sync.snapshot_rejected,
            sync.stale_timeouts,
            sync.buffer_dropped
        )?;
        write!(
            f,
            " received={} bad_frames={} snapshots={} snapshot_errors={} reconnects={} pings={}",
            feed.events_received,
            feed.bad_frames,
            feed.snapshots_fetched,
            feed.snapshot_errors,
            feed.reconnects,
            feed.pings
        )
    }
}

pub(crate) fn millis(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_mul(1000)
        .saturating_add(u64::from(duration.subsec_millis()))
}

/// UTC time of day. The date is in the logs.
fn clock(at: SystemTime) -> (u64, u64, u64, u32) {
    let since_epoch =
        at.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = since_epoch.as_secs();
    (
        seconds / 3600 % 24,
        seconds / 60 % 60,
        seconds % 60,
        since_epoch.subsec_millis(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_splits_a_timestamp_into_utc_time_of_day() {
        let at = UNIX_EPOCH + Duration::from_millis(90_061_500);
        assert_eq!(clock(at), (1, 1, 1, 500));
    }
}
