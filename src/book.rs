//! Local order book built from a snapshot and subsequent diff events.
//!
//! The band is the price range between the snapshot's lowest bid and highest ask. Updates
//! outside it are ignored. If the market drifts towards an edge, `sync`
//! decides when to replace the book with one built from a fresh snapshot.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::fixed::{Px, Qty};

pub struct DepthEvent {
    pub(crate) event_time_ms: u64,
    pub(crate) first_update_id: u64,
    pub(crate) final_update_id: u64,
    pub(crate) prev_final_update_id: u64,
    pub(crate) bids: Vec<(Px, Qty)>,
    pub(crate) asks: Vec<(Px, Qty)>,
}

#[derive(Deserialize)]
pub struct Snapshot {
    #[serde(rename = "lastUpdateId")]
    pub(crate) last_update_id: u64,
    pub(crate) bids: Vec<(Px, Qty)>,
    pub(crate) asks: Vec<(Px, Qty)>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Band {
    pub bid_floor: Px,
    pub ask_ceiling: Px,
}

pub struct Book {
    pub(crate) bids: BTreeMap<Px, Qty>,
    pub(crate) asks: BTreeMap<Px, Qty>,
    pub(crate) last_update_id: u64,
    pub(crate) band: Band,
}

impl Book {
    /// `None` when a side is empty.
    #[must_use]
    pub fn from_snapshot(snapshot: &Snapshot) -> Option<Book> {
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
            band,
        })
    }

    pub fn apply_event(&mut self, event: &DepthEvent) {
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
    }

    pub(crate) fn has_depth(&self, k_levels: usize) -> bool {
        self.bids.len() >= k_levels && self.asks.len() >= k_levels
    }

    pub(crate) fn best_bid(&self) -> Option<(Px, Qty)> {
        self.bids
            .last_key_value()
            .map(|(&price, &qty)| (price, qty))
    }

    pub(crate) fn best_ask(&self) -> Option<(Px, Qty)> {
        self.asks
            .first_key_value()
            .map(|(&price, &qty)| (price, qty))
    }
}

fn non_empty_levels(levels: &[(Px, Qty)]) -> BTreeMap<Px, Qty> {
    levels
        .iter()
        .copied()
        .filter(|(_, qty)| !qty.is_zero())
        .collect()
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
    use crate::fixed::{level, px, qty};

    fn snapshot() -> Snapshot {
        Snapshot {
            last_update_id: 100,
            bids: vec![level("100", "1"), level("99", "2"), level("98", "3")],
            asks: vec![level("101", "1"), level("102", "2"), level("103", "3")],
        }
    }

    fn event(bids: Vec<(Px, Qty)>, asks: Vec<(Px, Qty)>) -> DepthEvent {
        DepthEvent {
            event_time_ms: 7,
            first_update_id: 101,
            final_update_id: 105,
            prev_final_update_id: 100,
            bids,
            asks,
        }
    }

    #[test]
    fn snapshot_sets_band_to_lowest_bid_and_highest_ask() {
        let book = Book::from_snapshot(&snapshot()).expect("both sides present");
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
    fn snapshot_zero_quantity_levels_are_not_stored() {
        let mut with_zero = snapshot();
        with_zero.bids.push(level("97", "0"));
        let book = Book::from_snapshot(&with_zero).expect("both sides present");
        assert_eq!(book.bids.len(), 3);
        assert_eq!(book.band.bid_floor, px("98"));
    }

    #[test]
    fn event_levels_outside_the_band_are_ignored() {
        let mut book = Book::from_snapshot(&snapshot()).expect("both sides present");
        book.apply_event(&event(vec![level("97.5", "9")], vec![level("103.5", "9")]));
        assert_eq!(book.bids.len(), 3);
        assert_eq!(book.asks.len(), 3);
        assert_eq!(book.bids.get(&px("97.5")), None);
        assert_eq!(book.asks.get(&px("103.5")), None);
    }

    #[test]
    fn event_levels_inside_the_band_replace_sizes_and_delete_on_zero() {
        let mut book = Book::from_snapshot(&snapshot()).expect("both sides present");
        book.apply_event(&event(
            vec![level("99", "7"), level("99.5", "4"), level("98", "0")],
            vec![level("101", "0")],
        ));
        assert_eq!(book.bids.get(&px("99")), Some(&qty("7")));
        assert_eq!(book.bids.get(&px("99.5")), Some(&qty("4")));
        assert_eq!(book.bids.get(&px("98")), None);
        assert_eq!(book.best_ask(), Some(level("102", "2")));
        assert_eq!(book.last_update_id, 105);
    }

    #[test]
    fn depth_below_k_on_either_side_is_detected() {
        let mut book = Book::from_snapshot(&snapshot()).expect("both sides present");
        assert!(book.has_depth(3));
        book.apply_event(&event(vec![level("100", "0")], Vec::new()));
        assert!(!book.has_depth(3));
        assert!(book.has_depth(2));
    }
}
