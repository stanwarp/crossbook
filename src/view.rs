use std::fmt;
use std::time::Instant;

use crate::book::{Band, Book};
use crate::fixed::{Px, Qty};
use crate::status::DisconnectReason;
use crate::sync::ResyncReason;

/// The feed publishes a new empty view only when this changes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NotLiveReason {
    Connecting,
    Reconnecting(DisconnectReason),
    Buffering,
    Stitching,
    Resyncing(ResyncReason),
    Failed,
}

impl fmt::Display for NotLiveReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connecting => f.write_str("connecting"),
            Self::Reconnecting(reason) => {
                write!(f, "reconnecting({reason})")
            }
            Self::Buffering => f.write_str("buffering"),
            Self::Stitching => f.write_str("stitching"),
            Self::Resyncing(reason) => {
                write!(f, "resyncing({reason})")
            }
            Self::Failed => f.write_str("failed"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ViewState {
    Live,
    NotLive(NotLiveReason),
}

impl fmt::Display for ViewState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Live => f.write_str("live"),
            Self::NotLive(reason) => write!(f, "{reason}"),
        }
    }
}

/// Live means exactly K levels per side, best first; not live means both sides empty.
pub struct BookView {
    pub feed: &'static str,
    pub state: ViewState,
    pub bids: Vec<(Px, Qty)>,
    pub asks: Vec<(Px, Qty)>,
    pub last_update_id: u64,
    pub last_event_at: Instant,
    pub exchange_event_time_ms: u64,
    /// Wall clock minus `E` when the last event arrived; negative means the venue clock is ahead.
    pub latency_ms: Option<i64>,
    pub band: Option<Band>,
}

impl BookView {
    #[must_use]
    pub fn live(
        feed: &'static str,
        book: &Book,
        k: usize,
        last_event_at: Instant,
        latency_ms: Option<i64>,
    ) -> BookView {
        BookView {
            feed,
            state: ViewState::Live,
            bids: book
                .bids
                .iter()
                .rev()
                .take(k)
                .map(|(&price, &size)| (price, size))
                .collect(),
            asks: book
                .asks
                .iter()
                .take(k)
                .map(|(&price, &size)| (price, size))
                .collect(),
            last_update_id: book.last_update_id,
            last_event_at,
            exchange_event_time_ms: book.event_time_ms,
            latency_ms,
            band: Some(book.band),
        }
    }

    #[must_use]
    pub fn empty(feed: &'static str, reason: NotLiveReason, last_event_at: Instant) -> BookView {
        BookView {
            feed,
            state: ViewState::NotLive(reason),
            bids: Vec::new(),
            asks: Vec::new(),
            last_update_id: 0,
            last_event_at,
            exchange_event_time_ms: 0,
            latency_ms: None,
            band: None,
        }
    }

    pub(crate) fn crossing(&self) -> Option<(Px, Px)> {
        match (self.bids.first(), self.asks.first()) {
            (Some(&(bid, _)), Some(&(ask, _))) if bid >= ask => Some((bid, ask)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::Snapshot;
    use crate::fixed::level;

    fn book() -> Book {
        let snapshot = Snapshot {
            event_time_ms: 1_700_000_000_000,
            last_update_id: 42,
            bids: vec![level("99", "3"), level("100", "1"), level("98", "2")],
            asks: vec![level("103", "3"), level("101", "1"), level("102", "2")],
        };
        Book::from_snapshot(&snapshot).expect("both sides present")
    }

    #[test]
    fn live_view_takes_the_best_k_levels_of_each_side_best_first() {
        let book = book();
        let at = Instant::now();
        let view = BookView::live("binance", &book, 2, at, Some(7));
        assert_eq!(view.state, ViewState::Live);
        assert_eq!(view.bids, vec![level("100", "1"), level("99", "3")]);
        assert_eq!(view.asks, vec![level("101", "1"), level("102", "2")]);
        assert_eq!(view.band, Some(book.band));
        assert_eq!(view.last_update_id, 42);
        assert_eq!(view.last_event_at, at);
        assert_eq!(view.exchange_event_time_ms, 1_700_000_000_000);
        assert_eq!(view.latency_ms, Some(7));
        assert_eq!(view.crossing(), None);
    }

    #[test]
    fn crossed_top_of_book_is_detected() {
        let mut view = BookView::live("binance", &book(), 1, Instant::now(), None);
        assert_eq!(view.crossing(), None);
        let hundred: Px = "100".parse().expect("test literal");
        view.asks[0].0 = hundred;
        assert_eq!(view.crossing(), Some((hundred, hundred)));
    }
}
