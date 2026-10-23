use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::view::BookView;

#[must_use]
pub fn book_store(initial: BookView) -> (BookWriter, BookReader) {
    let shared = Arc::new(ArcSwap::from_pointee(initial));
    (
        BookWriter {
            shared: Arc::clone(&shared),
        },
        BookReader { shared },
    )
}

pub struct BookWriter {
    shared: Arc<ArcSwap<BookView>>,
}

impl BookWriter {
    pub fn publish(&self, view: BookView) {
        self.shared.store(Arc::new(view));
    }
}

pub struct BookReader {
    shared: Arc<ArcSwap<BookView>>,
}

impl BookReader {
    #[must_use]
    pub fn load(&self) -> Arc<BookView> {
        self.shared.load_full()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::fixed::level;
    use crate::view::{NotLiveReason, ViewState};

    fn live_view(last_update_id: u64, best_bid: &str) -> BookView {
        BookView {
            feed: "binance",
            state: ViewState::Live,
            bids: vec![level(best_bid, "1"), level("99", "2")],
            asks: vec![level("101", "1"), level("102", "2")],
            last_update_id,
            last_event_at: Instant::now(),
            latency_ms: None,
            band: None,
        }
    }

    #[test]
    fn a_held_view_survives_later_publishes_and_a_fresh_load_sees_the_newest() {
        let (writer, reader) = book_store(live_view(1, "100"));
        let held = reader.load();
        writer.publish(live_view(2, "100.5"));
        writer.publish(BookView::empty(
            "binance",
            NotLiveReason::Buffering,
            Instant::now(),
        ));
        assert_eq!(held.last_update_id, 1);
        assert_eq!(held.state, ViewState::Live);
        assert_eq!(held.bids, vec![level("100", "1"), level("99", "2")]);
        assert_eq!(held.asks, vec![level("101", "1"), level("102", "2")]);
        let newest = reader.load();
        assert_eq!(newest.state, ViewState::NotLive(NotLiveReason::Buffering));
        assert!(newest.bids.is_empty());
    }
}
