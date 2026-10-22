//! Local order book built from a snapshot and subsequent diff events.
//!
//! The snapshot defines the tracked price range. Updates outside these bounds are ignored.
//! Band changes when the book is rebuilt from a new snapshot, either
//! because a side fell below `k_levels` (the band is exhausted) or because the best price came
//! close to an edge and `sync` re-anchored the book ahead of time.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::fixed::{Px, Qty};

#[derive(Debug)]
pub(crate) struct DepthEvent {
    pub(crate) first_update_id: u64,
    pub(crate) final_update_id: u64,
    pub(crate) prev_final_update_id: u64,
    pub(crate) bids: Vec<(Px, Qty)>,
    pub(crate) asks: Vec<(Px, Qty)>,
}

pub(crate) struct Snapshot {
    pub(crate) last_update_id: u64,
    pub(crate) bids: Vec<(Px, Qty)>,
    pub(crate) asks: Vec<(Px, Qty)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Band {
    pub(crate) bid_floor: Px,
    pub(crate) ask_ceiling: Px,
}

pub(crate) struct Book {
    pub(crate) bids: BTreeMap<Px, Qty>,
    pub(crate) asks: BTreeMap<Px, Qty>,
    pub(crate) last_update_id: u64,
    pub(crate) last_event_time: Instant,
    pub(crate) band: Band,
}

impl Book {
    /// `None` when a side is empty, because then there is no price range the book can vouch for.
    pub(crate) fn from_snapshot(
        snapshot: &Snapshot,
        now: Instant,
    ) -> Option<Book> {
        let bids = non_empty_levels(&snapshot.bids);
        let asks = non_empty_levels(&snapshot.asks);
        let band = Band {
            bid_floor: *bids.keys().next()?,
            ask_ceiling: *asks.keys().next_back()?,
        };
        Some(Book {
            bids,
            asks,
            last_update_id: snapshot.last_update_id,
            last_event_time: now,
            band,
        })
    }

    pub(crate) fn apply_event(
        &mut self,
        event: &DepthEvent,
        now: Instant,
    ) {
        for &(price, qty) in &event.bids {
            if price < self.band.bid_floor {
                continue;
            }
            apply_level(&mut self.bids, price, qty);
        }
        for &(price, qty) in &event.asks {
            if price > self.band.ask_ceiling {
                continue;
            }
            apply_level(&mut self.asks, price, qty);
        }
        self.last_update_id = event.final_update_id;
        self.last_event_time = now;
    }

    pub(crate) fn has_depth(&self, k_levels: usize) -> bool {
        self.bids.len() >= k_levels && self.asks.len() >= k_levels
    }

    pub(crate) fn best_bid(&self) -> Option<(Px, Qty)> {
        self.bids.last_key_value().map(|(&price, &qty)| (price, qty))
    }

    pub(crate) fn best_ask(&self) -> Option<(Px, Qty)> {
        self.asks.first_key_value().map(|(&price, &qty)| (price, qty))
    }
}

fn non_empty_levels(levels: &[(Px, Qty)]) -> BTreeMap<Px, Qty> {
    levels.iter().copied().filter(|(_, qty)| !qty.is_zero()).collect()
}

fn apply_level(side: &mut BTreeMap<Px, Qty>, price: Px, qty: Qty) {
    if qty.is_zero() {
        side.remove(&price);
    } else {
        side.insert(price, qty);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn px(text: &str) -> Px {
        text.parse().expect("test literal")
    }

    fn qty(text: &str) -> Qty {
        text.parse().expect("test literal")
    }

    fn level(price: &str, size: &str) -> (Px, Qty) {
        (px(price), qty(size))
    }

    fn snapshot() -> Snapshot {
        Snapshot {
            last_update_id: 100,
            bids: vec![
                level("100", "1"),
                level("99", "2"),
                level("98", "3"),
            ],
            asks: vec![
                level("101", "1"),
                level("102", "2"),
                level("103", "3"),
            ],
        }
    }

    fn event(
        bids: Vec<(Px, Qty)>,
        asks: Vec<(Px, Qty)>,
    ) -> DepthEvent {
        DepthEvent {
            first_update_id: 101,
            final_update_id: 105,
            prev_final_update_id: 100,
            bids,
            asks,
        }
    }

    #[test]
    fn snapshot_sets_band_to_lowest_bid_and_highest_ask() {
        let book = Book::from_snapshot(&snapshot(), Instant::now())
            .expect("both sides present");
        assert_eq!(
            book.band,
            Band {
                bid_floor: px("98"),
                ask_ceiling: px("103")
            }
        );
        assert_eq!(book.last_update_id, 100);
        assert_eq!(book.best_bid(), Some(level("100", "1")));
        assert_eq!(book.best_ask(), Some(level("101", "1")));
    }

    #[test]
    fn snapshot_with_an_empty_side_has_no_book() {
        let mut empty_asks = snapshot();
        empty_asks.asks.clear();
        assert!(
            Book::from_snapshot(&empty_asks, Instant::now())
                .is_none()
        );
    }

    #[test]
    fn snapshot_zero_quantity_levels_are_not_stored() {
        let mut with_zero = snapshot();
        with_zero.bids.push(level("97", "0"));
        let book = Book::from_snapshot(&with_zero, Instant::now())
            .expect("both sides present");
        assert_eq!(book.bids.len(), 3);
        assert_eq!(book.band.bid_floor, px("98"));
    }

    #[test]
    fn event_levels_outside_the_band_are_ignored() {
        let mut book =
            Book::from_snapshot(&snapshot(), Instant::now())
                .expect("both sides present");
        book.apply_event(
            &event(
                vec![level("97.5", "9")],
                vec![level("103.5", "9")],
            ),
            Instant::now(),
        );
        assert_eq!(book.bids.len(), 3);
        assert_eq!(book.asks.len(), 3);
        assert_eq!(book.bids.get(&px("97.5")), None);
        assert_eq!(book.asks.get(&px("103.5")), None);
    }

    #[test]
    fn event_levels_inside_the_band_replace_sizes_and_delete_on_zero()
    {
        let mut book =
            Book::from_snapshot(&snapshot(), Instant::now())
                .expect("both sides present");
        book.apply_event(
            &event(
                vec![
                    level("99", "7"),
                    level("99.5", "4"),
                    level("98", "0"),
                ],
                vec![level("101", "0")],
            ),
            Instant::now(),
        );
        assert_eq!(book.bids.get(&px("99")), Some(&qty("7")));
        assert_eq!(book.bids.get(&px("99.5")), Some(&qty("4")));
        assert_eq!(book.bids.get(&px("98")), None);
        assert_eq!(book.best_ask(), Some(level("102", "2")));
    }

    #[test]
    fn deleting_an_unknown_level_changes_nothing() {
        let mut book =
            Book::from_snapshot(&snapshot(), Instant::now())
                .expect("both sides present");
        book.apply_event(
            &event(
                vec![level("99.5", "0")],
                vec![level("102.5", "0")],
            ),
            Instant::now(),
        );
        assert_eq!(book.bids.len(), 3);
        assert_eq!(book.asks.len(), 3);
    }

    #[test]
    fn event_advances_last_update_id_and_event_time() {
        let built_at = Instant::now();
        let mut book = Book::from_snapshot(&snapshot(), built_at)
            .expect("both sides present");
        let applied_at =
            built_at + std::time::Duration::from_millis(100);
        book.apply_event(&event(Vec::new(), Vec::new()), applied_at);
        assert_eq!(book.last_update_id, 105);
        assert_eq!(book.last_event_time, applied_at);
    }

    #[test]
    fn depth_below_k_on_either_side_is_detected() {
        let mut book =
            Book::from_snapshot(&snapshot(), Instant::now())
                .expect("both sides present");
        assert!(book.has_depth(3));
        book.apply_event(
            &event(vec![level("100", "0")], Vec::new()),
            Instant::now(),
        );
        assert!(!book.has_depth(3));
        assert!(book.has_depth(2));
    }
}
