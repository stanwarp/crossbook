//! Keeps the local book synchronized with the Binance USD-M futures diff stream.
//!
//! A snapshot with `lastUpdateId = N` is synchronized using the first event
//! satisfying `U <= N <= u`. Events with `u < N` are discarded. If the next
//! event starts at `U > N`, the snapshot cannot be synchronized and another
//! snapshot is needed.
//!
//! Once synchronized, each event must have `pu == last_u`. A mismatch means
//! an update was missed, so the book is invalidated and rebuilt from a fresh
//! snapshot. The WebSocket connection remains open.
//!
//! If the best price approaches the edge of the book, the snapshot is fetched behind
//! the live book and swapped in once the buffered events stitch to it,
//! and readers keep a book throughout.
//! Once a side falls below `k_levels`, the book is invalidated
//! and readers wait until the fresh snapshot stitches.

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

use tracing::{debug, info, trace, warn};

use crate::book::{Band, Book, DepthEvent, Snapshot};
use crate::fixed::Px;

/// About 100 seconds at ten events per second. An overflow
/// cannot corrupt the book, costs another snapshot.
const MAX_BUFFERED_EVENTS: usize = 1024;
const OVERFLOW_WARN_EVERY: u64 = 1000;
const STITCH_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) enum Input {
    Event(DepthEvent),
    Snapshot(Snapshot),
    Tick,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Action {
    None,
    FetchSnapshot,
    ResetConnection { silent_for: Duration },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResyncReason {
    Gap {
        expected: u64,
        got_pu: u64,
    },
    BandExhausted {
        bids: usize,
        asks: usize,
        k_levels: usize,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct ReanchorPrices {
    bid_at: Px,
    ask_at: Px,
}

/// A re-anchor in progress: events since the request are kept to stitch the new book to.
pub(crate) struct Reanchor {
    buffer: VecDeque<DepthEvent>,
    held: Option<HeldSnapshot>,
    pub(crate) requested_at: Instant,
}

/// Waits for an event that covers it.
struct HeldSnapshot {
    snapshot: Snapshot,
    since: Instant,
    dropped: usize,
}

impl Reanchor {
    pub(crate) fn kept(&self) -> usize {
        self.buffer.len()
    }
}

pub(crate) enum State {
    Buffering,
    Stitching {
        snapshot: Snapshot,
        dropped: usize,
        since: Instant,
        recovering_from: Option<ResyncReason>,
    },
    Live {
        book: Book,
        anchored_at: Instant,
        reanchor_at: ReanchorPrices,
        reanchor: Option<Reanchor>,
    },
    Resyncing {
        reason: ResyncReason,
    },
}

impl State {
    fn name(&self) -> &'static str {
        match self {
            State::Buffering => "buffering",
            State::Stitching { .. } => "stitching",
            State::Live { .. } => "live",
            State::Resyncing { .. } => "resyncing",
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct SyncCounters {
    pub(crate) events_applied: u64,
    pub(crate) gaps: u64,
    pub(crate) band_exhausted: u64,
    pub(crate) snapshot_rejected: u64,
    pub(crate) stale_timeouts: u64,
    pub(crate) buffer_dropped: u64,
    pub(crate) reanchors: u64,
}

pub(crate) struct SyncMachine {
    k_levels: usize,
    stale_after: Duration,
    reanchor_margin_percent: u64,
    state: State,
    buffer: VecDeque<DepthEvent>,
    last_event_at: Instant,
    counters: SyncCounters,
}

enum Built {
    Wait,
    Book {
        book: Book,
        bridge_first: u64,
        bridge_final: u64,
        replayed: u64,
    },
    Rejected(Rejection),
    Broken {
        reason: ResyncReason,
    },
}

enum Rejection {
    Hole {
        first_update_id: u64,
        final_update_id: u64,
    },
    EmptySide {
        bids: usize,
        asks: usize,
    },
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rejection::Hole {
                first_update_id,
                final_update_id,
            } => write!(
                f,
                "hole(first_u={first_update_id},final_u={final_update_id})"
            ),
            Rejection::EmptySide { bids, asks } => {
                write!(f, "empty-side(bids={bids},asks={asks})")
            }
        }
    }
}

/// On rejection the buffer keeps what the next snapshot can still use.
fn build_book(
    snapshot: &Snapshot,
    buffer: &mut VecDeque<DepthEvent>,
    k_levels: usize,
    dropped: &mut usize,
) -> Built {
    let last_update_id = snapshot.last_update_id;
    while buffer
        .front()
        .is_some_and(|event| event.final_update_id < last_update_id)
    {
        buffer.pop_front();
        *dropped += 1;
    }
    let Some(first) = buffer.front() else {
        return Built::Wait;
    };
    if first.first_update_id > last_update_id {
        return Built::Rejected(Rejection::Hole {
            first_update_id: first.first_update_id,
            final_update_id: first.final_update_id,
        });
    }
    let (bridge_first, bridge_final) = (first.first_update_id, first.final_update_id);
    let Some(mut book) = Book::from_snapshot(snapshot) else {
        return Built::Rejected(Rejection::EmptySide {
            bids: snapshot.bids.len(),
            asks: snapshot.asks.len(),
        });
    };
    // Events leave the buffer only as they apply, so a broken chain keeps its contiguous
    // tail for the next snapshot.
    let mut replayed: u64 = 0;
    while let Some(event) = buffer.front() {
        if replayed > 0 && event.prev_final_update_id != book.last_update_id {
            let reason = ResyncReason::Gap {
                expected: book.last_update_id,
                got_pu: event.prev_final_update_id,
            };
            return Built::Broken { reason };
        }
        book.apply_event(event);
        replayed += 1;
        buffer.pop_front();
    }
    if !book.has_depth(k_levels) {
        let reason = ResyncReason::BandExhausted {
            bids: book.bids.len(),
            asks: book.asks.len(),
            k_levels,
        };
        return Built::Broken { reason };
    }
    Built::Book {
        book,
        bridge_first,
        bridge_final,
        replayed,
    }
}

fn push_bounded(buffer: &mut VecDeque<DepthEvent>, event: DepthEvent, dropped: &mut u64) {
    if buffer.len() == MAX_BUFFERED_EVENTS {
        buffer.pop_front();
        *dropped += 1;
        if (*dropped - 1).is_multiple_of(OVERFLOW_WARN_EVERY) {
            warn!(
                dropped_total = *dropped,
                "buffer full, dropping oldest events"
            );
        }
    }
    buffer.push_back(event);
}

fn live_state(book: Book, margin_percent: u64, now: Instant) -> State {
    State::Live {
        reanchor_at: reanchor_prices(&book, margin_percent),
        book,
        anchored_at: now,
        reanchor: None,
    }
}

fn reanchor_prices(book: &Book, margin_percent: u64) -> ReanchorPrices {
    let Band {
        bid_floor,
        ask_ceiling,
    } = book.band;
    ReanchorPrices {
        bid_at: book.best_bid().map_or(bid_floor, |(best, _)| {
            bid_floor.towards(best, margin_percent)
        }),
        ask_at: book.best_ask().map_or(ask_ceiling, |(best, _)| {
            ask_ceiling.towards(best, margin_percent)
        }),
    }
}

fn near_an_edge(reanchor_at: ReanchorPrices, book: &Book) -> bool {
    book.best_ask()
        .is_some_and(|(best, _)| best >= reanchor_at.ask_at)
        || book
            .best_bid()
            .is_some_and(|(best, _)| best <= reanchor_at.bid_at)
}

fn waiting_state(recovering_from: Option<ResyncReason>) -> State {
    match recovering_from {
        Some(reason) => State::Resyncing { reason },
        None => State::Buffering,
    }
}

impl SyncMachine {
    pub(crate) fn new(
        k_levels: usize,
        stale_after: Duration,
        reanchor_margin_percent: u64,
        now: Instant,
    ) -> Self {
        SyncMachine {
            k_levels,
            stale_after,
            reanchor_margin_percent,
            state: State::Buffering,
            buffer: VecDeque::new(),
            last_event_at: now,
            counters: SyncCounters::default(),
        }
    }

    pub(crate) fn connected(&mut self, now: Instant) -> Action {
        self.reset();
        self.last_event_at = now;
        Action::FetchSnapshot
    }

    pub(crate) fn reset(&mut self) {
        self.state = State::Buffering;
        self.buffer.clear();
    }

    pub(crate) fn state(&self) -> &State {
        &self.state
    }

    pub(crate) fn book(&self) -> Option<&Book> {
        match &self.state {
            State::Live { book, .. } => Some(book),
            State::Buffering | State::Stitching { .. } | State::Resyncing { .. } => None,
        }
    }

    /// A new value means a new book.
    pub(crate) fn anchored_at(&self) -> Option<Instant> {
        match &self.state {
            State::Live { anchored_at, .. } => Some(*anchored_at),
            State::Buffering | State::Stitching { .. } | State::Resyncing { .. } => None,
        }
    }

    pub(crate) fn counters(&self) -> SyncCounters {
        self.counters
    }

    pub(crate) fn last_event_at(&self) -> Instant {
        self.last_event_at
    }

    pub(crate) fn buffered(&self) -> usize {
        self.buffer.len()
    }

    pub(crate) fn handle(&mut self, input: Input, now: Instant) -> Action {
        match input {
            Input::Event(event) => self.on_event(event, now),
            Input::Snapshot(snapshot) => self.on_snapshot(snapshot, now),
            Input::Tick => self.on_tick(now),
        }
    }

    fn on_event(&mut self, event: DepthEvent, now: Instant) -> Action {
        self.last_event_at = now;
        match &mut self.state {
            State::Live {
                book,
                reanchor_at,
                reanchor,
                ..
            } => {
                let broken = if event.prev_final_update_id == book.last_update_id {
                    book.apply_event(&event);
                    self.counters.events_applied += 1;
                    if book.has_depth(self.k_levels) {
                        None
                    } else {
                        Some(ResyncReason::BandExhausted {
                            bids: book.bids.len(),
                            asks: book.asks.len(),
                            k_levels: self.k_levels,
                        })
                    }
                } else {
                    Some(ResyncReason::Gap {
                        expected: book.last_update_id,
                        got_pu: event.prev_final_update_id,
                    })
                };
                if let Some(reason) = broken {
                    // A pending re-anchor goes with the book. Its snapshot, if still in flight,
                    // arrives into the resync and serves it.
                    let action = self.resync(reason);
                    // This event starts the chain the next snapshot stitches to.
                    self.buffer_event(event);
                    return action;
                }
                match reanchor {
                    Some(pending) => {
                        push_bounded(
                            &mut pending.buffer,
                            event,
                            &mut self.counters.buffer_dropped,
                        );
                        if pending.held.is_some() {
                            self.try_swap(now)
                        } else {
                            Action::None
                        }
                    }
                    None if near_an_edge(*reanchor_at, book) => {
                        info!(
                            bid_at = %reanchor_at.bid_at,
                            ask_at = %reanchor_at.ask_at,
                            bid_floor = %book.band.bid_floor,
                            ask_ceiling = %book.band.ask_ceiling,
                            "best price near the band edge, re-anchoring"
                        );
                        *reanchor = Some(Reanchor {
                            buffer: VecDeque::from([event]),
                            held: None,
                            requested_at: now,
                        });
                        Action::FetchSnapshot
                    }
                    None => Action::None,
                }
            }
            State::Stitching { .. } => {
                self.buffer_event(event);
                self.try_stitch(now)
            }
            State::Buffering | State::Resyncing { .. } => {
                self.buffer_event(event);
                Action::None
            }
        }
    }

    fn buffer_event(&mut self, event: DepthEvent) {
        push_bounded(&mut self.buffer, event, &mut self.counters.buffer_dropped);
    }

    fn on_snapshot(&mut self, snapshot: Snapshot, now: Instant) -> Action {
        let recovering_from = match &mut self.state {
            State::Live {
                reanchor: Some(pending),
                ..
            } => {
                pending.held = Some(HeldSnapshot {
                    snapshot,
                    since: now,
                    dropped: 0,
                });
                return self.try_swap(now);
            }
            State::Live { reanchor: None, .. } => {
                debug!(
                    last_update_id = snapshot.last_update_id,
                    "snapshot ignored while live"
                );
                return Action::None;
            }
            State::Buffering => None,
            State::Resyncing { reason } => Some(*reason),
            State::Stitching {
                recovering_from, ..
            } => *recovering_from,
        };
        self.state = State::Stitching {
            snapshot,
            dropped: 0,
            since: now,
            recovering_from,
        };
        self.try_stitch(now)
    }

    fn try_stitch(&mut self, now: Instant) -> Action {
        let State::Stitching {
            snapshot,
            dropped,
            recovering_from,
            ..
        } = &mut self.state
        else {
            unreachable!("try_stitch runs only in Stitching");
        };
        let (last_update_id, recovering_from) = (snapshot.last_update_id, *recovering_from);
        // Summed in the state over the whole wait, so the live line shows the full cost.
        match build_book(snapshot, &mut self.buffer, self.k_levels, dropped) {
            Built::Wait => {
                trace!(
                    last_update_id,
                    dropped = *dropped,
                    "waiting for an event that covers the snapshot"
                );
                Action::None
            }
            Built::Book {
                book,
                bridge_first,
                bridge_final,
                replayed,
            } => {
                self.counters.events_applied += replayed;
                info!(
                    last_update_id,
                    bridge_first,
                    bridge_final,
                    dropped = *dropped,
                    replayed,
                    bids = book.bids.len(),
                    asks = book.asks.len(),
                    bid_floor = %book.band.bid_floor,
                    ask_ceiling = %book.band.ask_ceiling,
                    "live"
                );
                self.state = live_state(book, self.reanchor_margin_percent, now);
                Action::None
            }
            Built::Rejected(why) => {
                self.counters.snapshot_rejected += 1;
                warn!(
                    last_update_id,
                    %why,
                    dropped = *dropped,
                    buffered = self.buffer.len(),
                    "snapshot rejected, fetching another"
                );
                self.state = waiting_state(recovering_from);
                Action::FetchSnapshot
            }
            Built::Broken { reason } => self.resync(reason),
        }
    }

    fn try_swap(&mut self, now: Instant) -> Action {
        let State::Live {
            book,
            reanchor: Some(pending),
            ..
        } = &mut self.state
        else {
            unreachable!("try_swap runs only with a re-anchor pending");
        };
        let Some(held) = &mut pending.held else {
            unreachable!("try_swap runs only with a snapshot held");
        };
        let last_update_id = held.snapshot.last_update_id;
        match build_book(
            &held.snapshot,
            &mut pending.buffer,
            self.k_levels,
            &mut held.dropped,
        ) {
            Built::Wait => {
                trace!(
                    last_update_id,
                    dropped = held.dropped,
                    "waiting for an event that covers the re-anchor snapshot"
                );
                Action::None
            }
            Built::Book {
                book: fresh,
                replayed,
                ..
            } => {
                info!(
                    last_update_id,
                    old_bid_floor = %book.band.bid_floor,
                    old_ask_ceiling = %book.band.ask_ceiling,
                    bid_floor = %fresh.band.bid_floor,
                    ask_ceiling = %fresh.band.ask_ceiling,
                    dropped = held.dropped,
                    replayed,
                    bids = fresh.bids.len(),
                    asks = fresh.asks.len(),
                    "re-anchored"
                );
                self.counters.reanchors += 1;
                self.state = live_state(fresh, self.reanchor_margin_percent, now);
                Action::None
            }
            Built::Rejected(why) => {
                self.counters.snapshot_rejected += 1;
                warn!(
                    last_update_id,
                    %why,
                    dropped = held.dropped,
                    "re-anchor snapshot rejected, fetching another"
                );
                pending.held = None;
                Action::FetchSnapshot
            }
            Built::Broken { reason } => {
                self.counters.snapshot_rejected += 1;
                warn!(
                    last_update_id,
                    %reason,
                    dropped = held.dropped,
                    "re-anchor snapshot rejected, fetching another"
                );
                pending.held = None;
                Action::FetchSnapshot
            }
        }
    }

    fn resync(&mut self, reason: ResyncReason) -> Action {
        match reason {
            ResyncReason::Gap { expected, got_pu } => {
                self.counters.gaps += 1;
                warn!(expected, got_pu, "gap in update ids, rebuilding book");
            }
            ResyncReason::BandExhausted {
                bids,
                asks,
                k_levels,
            } => {
                self.counters.band_exhausted += 1;
                warn!(bids, asks, k_levels, "band exhausted, rebuilding book");
            }
        }
        // The buffer holds what the next snapshot can be stitched to.
        self.state = State::Resyncing { reason };
        Action::FetchSnapshot
    }

    fn on_tick(&mut self, now: Instant) -> Action {
        let silent_for = now.saturating_duration_since(self.last_event_at);
        if silent_for >= self.stale_after {
            self.counters.stale_timeouts += 1;
            warn!(
                silent_ms = silent_for.as_millis(),
                state = self.state.name(),
                "no events, connection is stale"
            );
            self.reset();
            return Action::ResetConnection { silent_for };
        }
        match &mut self.state {
            State::Stitching {
                snapshot,
                since,
                recovering_from,
                ..
            } if now.saturating_duration_since(*since) >= STITCH_TIMEOUT => {
                self.counters.snapshot_rejected += 1;
                warn!(
                    last_update_id = snapshot.last_update_id,
                    waited_ms = STITCH_TIMEOUT.as_millis(),
                    buffered = self.buffer.len(),
                    "no event covered the snapshot in time, fetching another"
                );
                self.state = waiting_state(*recovering_from);
                Action::FetchSnapshot
            }
            State::Live {
                reanchor: Some(pending),
                ..
            } if pending.held.as_ref().is_some_and(|held| {
                now.saturating_duration_since(held.since) >= STITCH_TIMEOUT
            }) =>
            {
                self.counters.snapshot_rejected += 1;
                warn!(
                    waited_ms = STITCH_TIMEOUT.as_millis(),
                    kept = pending.buffer.len(),
                    "no event covered the re-anchor snapshot in time, fetching another"
                );
                pending.held = None;
                Action::FetchSnapshot
            }
            State::Buffering
            | State::Stitching { .. }
            | State::Live { .. }
            | State::Resyncing { .. } => Action::None,
        }
    }
}

impl fmt::Display for ResyncReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResyncReason::Gap { expected, got_pu } => {
                write!(f, "gap(expected={expected},got_pu={got_pu})")
            }
            ResyncReason::BandExhausted {
                bids,
                asks,
                k_levels,
            } => write!(f, "band-exhausted(bids={bids},asks={asks},k={k_levels})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed::{Px, Qty, level, px, qty};

    const K: usize = 2;
    const STALE_AFTER: Duration = Duration::from_secs(5);
    const REANCHOR_MARGIN_PERCENT: u64 = 10;

    fn ids(prev: u64, first: u64, last: u64) -> DepthEvent {
        DepthEvent {
            event_time_ms: 0,
            first_update_id: first,
            final_update_id: last,
            prev_final_update_id: prev,
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    fn snapshot(last_update_id: u64) -> Snapshot {
        Snapshot {
            last_update_id,
            bids: vec![level("100", "1"), level("99", "1")],
            asks: vec![level("101", "1"), level("102", "1")],
        }
    }

    fn machine(now: Instant) -> SyncMachine {
        let mut machine = SyncMachine::new(K, STALE_AFTER, REANCHOR_MARGIN_PERCENT, now);
        assert_eq!(machine.connected(now), Action::FetchSnapshot);
        machine
    }

    /// Connected, one event buffered, snapshot bridged by it: live with `last_update_id = 6`.
    fn live_machine(now: Instant) -> SyncMachine {
        let mut machine = machine(now);
        assert_eq!(
            machine.handle(Input::Event(ids(4, 5, 6)), now),
            Action::None
        );
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(6)), now),
            Action::None
        );
        assert!(matches!(machine.state(), State::Live { .. }));
        machine
    }

    fn resync_reason(machine: &SyncMachine) -> Option<ResyncReason> {
        match machine.state() {
            State::Resyncing { reason } => Some(*reason),
            State::Buffering | State::Stitching { .. } | State::Live { .. } => None,
        }
    }

    struct StitchCase {
        name: &'static str,
        last_update_id: u64,
        buffered: &'static [(u64, u64)],
        dropped: usize,
        expected: Expected,
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Expected {
        Wait,
        Bridge { first_update_id: u64 },
        Hole { first_update_id: u64 },
    }

    fn summary(built: &Built) -> Expected {
        match built {
            Built::Wait => Expected::Wait,
            Built::Book { bridge_first, .. } => Expected::Bridge {
                first_update_id: *bridge_first,
            },
            Built::Rejected(Rejection::Hole {
                first_update_id, ..
            }) => Expected::Hole {
                first_update_id: *first_update_id,
            },
            Built::Rejected(Rejection::EmptySide { .. }) | Built::Broken { .. } => {
                panic!("a plain chain on a two-sided snapshot ends elsewhere")
            }
        }
    }

    impl SyncMachine {
        pub(crate) fn reanchor_buffered(&self) -> Option<usize> {
            match &self.state {
                State::Live {
                    reanchor: Some(pending),
                    ..
                } => Some(pending.kept()),
                State::Live { reanchor: None, .. }
                | State::Buffering
                | State::Stitching { .. }
                | State::Resyncing { .. } => None,
            }
        }
    }

    const STITCH_CASES: &[StitchCase] = &[
        StitchCase {
            name: "empty buffer waits",
            last_update_id: 100,
            buffered: &[],
            dropped: 0,
            expected: Expected::Wait,
        },
        StitchCase {
            name: "events older than the snapshot are dropped, then wait",
            last_update_id: 100,
            buffered: &[(90, 95), (96, 99)],
            dropped: 2,
            expected: Expected::Wait,
        },
        StitchCase {
            name: "event ending exactly at the snapshot bridges",
            last_update_id: 100,
            buffered: &[(98, 100)],
            dropped: 0,
            expected: Expected::Bridge {
                first_update_id: 98,
            },
        },
        StitchCase {
            name: "event starting exactly at the snapshot bridges",
            last_update_id: 100,
            buffered: &[(100, 103)],
            dropped: 0,
            expected: Expected::Bridge {
                first_update_id: 100,
            },
        },
        StitchCase {
            name: "event spanning the snapshot bridges after older ones are dropped",
            last_update_id: 100,
            buffered: &[(90, 95), (96, 101), (102, 105)],
            dropped: 1,
            expected: Expected::Bridge {
                first_update_id: 96,
            },
        },
        StitchCase {
            name: "event starting after the snapshot is a hole",
            last_update_id: 100,
            buffered: &[(101, 105)],
            dropped: 0,
            expected: Expected::Hole {
                first_update_id: 101,
            },
        },
        StitchCase {
            name: "hole is detected behind dropped events",
            last_update_id: 100,
            buffered: &[(90, 99), (101, 105)],
            dropped: 1,
            expected: Expected::Hole {
                first_update_id: 101,
            },
        },
    ];

    #[test]
    fn stitch_rule_table() {
        for case in STITCH_CASES {
            let mut buffer: VecDeque<DepthEvent> = case
                .buffered
                .iter()
                .map(|&(first, last)| ids(first.saturating_sub(1), first, last))
                .collect();
            let mut dropped = 0;
            let built = build_book(&snapshot(case.last_update_id), &mut buffer, 1, &mut dropped);
            assert_eq!(dropped, case.dropped, "{}", case.name);
            assert_eq!(summary(&built), case.expected, "{}", case.name);
        }
    }

    #[test]
    fn snapshot_without_bridging_event_waits_for_the_stream() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.handle(Input::Event(ids(4, 5, 6)), now);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(10)), now),
            Action::None
        );
        assert!(matches!(machine.state(), State::Stitching { .. }));
        assert_eq!(
            machine.handle(Input::Event(ids(6, 7, 9)), now),
            Action::None
        );
        assert!(matches!(machine.state(), State::Stitching { .. }));
        assert_eq!(
            machine.handle(Input::Event(ids(9, 10, 12)), now),
            Action::None
        );
        assert_eq!(machine.book().expect("live").last_update_id, 12);
    }

    #[test]
    fn snapshot_older_than_the_stream_is_rejected_and_refetched() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.handle(Input::Event(ids(20, 21, 25)), now);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(10)), now),
            Action::FetchSnapshot
        );
        assert!(matches!(machine.state(), State::Buffering));
        assert_eq!(machine.counters().snapshot_rejected, 1);
        assert_eq!(machine.buffered(), 1);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(23)), now),
            Action::None
        );
        assert_eq!(machine.book().expect("live").last_update_id, 25);
    }

    #[test]
    fn gap_in_live_rebuilds_the_book_from_a_new_snapshot() {
        let now = Instant::now();
        let mut machine = live_machine(now);
        assert_eq!(
            machine.handle(Input::Event(ids(6, 7, 8)), now),
            Action::None
        );
        assert_eq!(
            machine.handle(Input::Event(ids(9, 10, 11)), now),
            Action::FetchSnapshot
        );
        assert_eq!(
            resync_reason(&machine),
            Some(ResyncReason::Gap {
                expected: 8,
                got_pu: 9
            })
        );
        assert!(machine.book().is_none());
        assert_eq!(machine.counters().gaps, 1);
        assert_eq!(machine.buffered(), 1);
        assert_eq!(
            machine.handle(Input::Event(ids(11, 12, 13)), now),
            Action::None
        );
        assert_eq!(
            machine.handle(Input::Event(ids(13, 14, 15)), now),
            Action::None
        );
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(14)), now),
            Action::None
        );
        assert_eq!(machine.book().expect("live again").last_update_id, 15);
        assert_eq!(machine.counters().events_applied, 3);
    }

    #[test]
    fn gap_inside_the_buffer_is_caught_during_replay() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.handle(Input::Event(ids(4, 5, 6)), now);
        machine.handle(Input::Event(ids(6, 7, 8)), now);
        machine.handle(Input::Event(ids(99, 9, 10)), now);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(5)), now),
            Action::FetchSnapshot
        );
        assert_eq!(
            resync_reason(&machine),
            Some(ResyncReason::Gap {
                expected: 8,
                got_pu: 99
            })
        );
        assert_eq!(machine.buffered(), 1);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(9)), now),
            Action::None
        );
        assert_eq!(
            machine
                .book()
                .expect("live from the kept tail")
                .last_update_id,
            10
        );
    }

    #[test]
    fn band_exhausted_by_an_event_triggers_resync() {
        let now = Instant::now();
        let mut machine = live_machine(now);
        let mut delete_floor = ids(6, 7, 8);
        delete_floor.bids.push(level("99", "0"));
        assert_eq!(
            machine.handle(Input::Event(delete_floor), now),
            Action::FetchSnapshot
        );
        assert_eq!(
            resync_reason(&machine),
            Some(ResyncReason::BandExhausted {
                bids: 1,
                asks: 2,
                k_levels: K
            })
        );
        assert_eq!(machine.counters().band_exhausted, 1);
        assert_eq!(machine.buffered(), 1);
    }

    #[test]
    fn snapshot_thinner_than_k_resyncs_right_after_stitching() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.handle(Input::Event(ids(4, 5, 6)), now);
        let mut thin = snapshot(6);
        thin.bids.truncate(1);
        assert_eq!(
            machine.handle(Input::Snapshot(thin), now),
            Action::FetchSnapshot
        );
        assert_eq!(
            resync_reason(&machine),
            Some(ResyncReason::BandExhausted {
                bids: 1,
                asks: 2,
                k_levels: K
            })
        );
    }

    #[test]
    fn a_snapshot_with_an_empty_side_is_rejected() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.handle(Input::Event(ids(4, 5, 6)), now);
        let mut one_sided = snapshot(6);
        one_sided.asks.clear();
        assert_eq!(
            machine.handle(Input::Snapshot(one_sided), now),
            Action::FetchSnapshot
        );
        assert!(matches!(machine.state(), State::Buffering));
        assert_eq!(machine.counters().snapshot_rejected, 1);
        assert_eq!(machine.buffered(), 1);
    }

    #[test]
    fn snapshot_while_live_is_ignored() {
        let now = Instant::now();
        let mut machine = live_machine(now);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(9)), now),
            Action::None
        );
        assert_eq!(machine.book().expect("still live").last_update_id, 6);
    }

    #[test]
    fn silence_longer_than_stale_after_resets_the_connection_in_any_state() {
        let connected_at = Instant::now();
        let mut buffering = machine(connected_at);
        let mut stitching = machine(connected_at);
        stitching.handle(Input::Event(ids(4, 5, 6)), connected_at);
        stitching.handle(Input::Snapshot(snapshot(10)), connected_at);
        assert!(matches!(stitching.state(), State::Stitching { .. }));
        let mut live = live_machine(connected_at);
        for machine in [&mut buffering, &mut stitching, &mut live] {
            assert_eq!(
                machine.handle(Input::Tick, connected_at + Duration::from_secs(4)),
                Action::None
            );
            assert_eq!(
                machine.handle(Input::Tick, connected_at + STALE_AFTER),
                Action::ResetConnection {
                    silent_for: STALE_AFTER
                }
            );
            assert_eq!(machine.counters().stale_timeouts, 1);
            assert!(matches!(machine.state(), State::Buffering));
        }
    }

    #[test]
    fn snapshot_nothing_can_bridge_is_rejected_after_the_stitch_timeout() {
        let connected_at = Instant::now();
        let mut machine = machine(connected_at);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(100)), connected_at),
            Action::None
        );
        for second in 1..30u64 {
            let at = connected_at + Duration::from_secs(second);
            let event = ids(second + 3, second + 4, second + 4);
            assert_eq!(machine.handle(Input::Event(event), at), Action::None);
            assert_eq!(machine.handle(Input::Tick, at), Action::None);
        }
        assert_eq!(
            machine.handle(Input::Tick, connected_at + STITCH_TIMEOUT),
            Action::FetchSnapshot
        );
        assert_eq!(machine.counters().snapshot_rejected, 1);
        assert!(matches!(machine.state(), State::Buffering));
    }

    #[test]
    fn buffered_events_count_as_activity_for_the_watchdog() {
        let connected_at = Instant::now();
        let mut machine = machine(connected_at);
        machine.handle(
            Input::Event(ids(4, 5, 6)),
            connected_at + Duration::from_secs(4),
        );
        assert_eq!(
            machine.handle(Input::Tick, connected_at + Duration::from_secs(8)),
            Action::None
        );
        assert!(matches!(
            machine.handle(Input::Tick, connected_at + Duration::from_secs(9)),
            Action::ResetConnection { .. }
        ));
    }

    #[test]
    fn overflowed_buffer_rejects_a_snapshot_it_can_no_longer_bridge() {
        let now = Instant::now();
        let mut machine = machine(now);
        for i in 0..MAX_BUFFERED_EVENTS + 3 {
            let first = u64::try_from(i).expect("small index");
            machine.handle(Input::Event(ids(first, first + 1, first + 1)), now);
        }
        assert_eq!(machine.buffered(), MAX_BUFFERED_EVENTS);
        assert_eq!(machine.counters().buffer_dropped, 3);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(2)), now),
            Action::FetchSnapshot
        );
        assert_eq!(machine.counters().snapshot_rejected, 1);
    }

    fn with_levels(
        prev: u64,
        first: u64,
        last: u64,
        bids: Vec<(Px, Qty)>,
        asks: Vec<(Px, Qty)>,
    ) -> DepthEvent {
        DepthEvent {
            bids,
            asks,
            ..ids(prev, first, last)
        }
    }

    /// Bids 100/50/15/10 and asks 101/150/191/200: the re-anchor alarms sit at 19 and 190.1.
    fn wide_snapshot(last_update_id: u64) -> Snapshot {
        Snapshot {
            last_update_id,
            bids: vec![
                level("100", "1"),
                level("50", "1"),
                level("15", "1"),
                level("10", "1"),
            ],
            asks: vec![
                level("101", "1"),
                level("150", "1"),
                level("191", "1"),
                level("200", "1"),
            ],
        }
    }

    /// Live on the wide book with `last_update_id = 6`.
    fn wide_live_machine(now: Instant) -> SyncMachine {
        let mut machine = machine(now);
        machine.handle(Input::Event(ids(4, 5, 6)), now);
        assert_eq!(
            machine.handle(Input::Snapshot(wide_snapshot(6)), now),
            Action::None
        );
        machine
    }

    /// Live on the wide book with a re-anchor pending: asks 101 and 150 are gone, `last_u = 10`.
    fn pending_reanchor_machine(now: Instant) -> SyncMachine {
        let mut machine = wide_live_machine(now);
        let delete_101 = with_levels(6, 7, 8, Vec::new(), vec![level("101", "0")]);
        assert_eq!(machine.handle(Input::Event(delete_101), now), Action::None);
        let delete_150 = with_levels(8, 9, 10, Vec::new(), vec![level("150", "0")]);
        assert_eq!(
            machine.handle(Input::Event(delete_150), now),
            Action::FetchSnapshot
        );
        machine
    }

    fn narrower_snapshot(last_update_id: u64) -> Snapshot {
        Snapshot {
            last_update_id,
            bids: vec![level("190", "1"), level("180", "1")],
            asks: vec![level("191", "1"), level("250", "1"), level("300", "1")],
        }
    }

    #[test]
    fn band_exhaustion_during_a_pending_reanchor_resyncs_and_drops_it() {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        let delete_191 = with_levels(10, 11, 12, Vec::new(), vec![level("191", "0")]);
        assert_eq!(
            machine.handle(Input::Event(delete_191), now),
            Action::FetchSnapshot
        );
        assert!(matches!(machine.state(), State::Resyncing { .. }));
        assert_eq!(machine.reanchor_buffered(), None);
        assert_eq!(machine.buffered(), 1);
    }

    #[test]
    fn reanchor_fires_at_the_margin_and_not_before() {
        let now = Instant::now();
        // The helper asserts the first delete returns no action and the second asks for the
        // snapshot.
        let machine = pending_reanchor_machine(now);
        assert!(matches!(machine.state(), State::Live { .. }));
        assert_eq!(machine.reanchor_buffered(), Some(1));
    }

    #[test]
    fn events_during_a_pending_reanchor_reach_both_book_and_buffer() {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        let insert_195 = with_levels(10, 11, 12, Vec::new(), vec![level("195", "5")]);
        assert_eq!(machine.handle(Input::Event(insert_195), now), Action::None);
        let book = machine.book().expect("live");
        assert_eq!(book.asks.get(&px("195")), Some(&qty("5")));
        assert_eq!(book.last_update_id, 12);
        assert_eq!(machine.reanchor_buffered(), Some(2));
    }

    #[test]
    fn successful_reanchor_keeps_live_and_changes_the_band() {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        let insert_195 = with_levels(10, 11, 12, Vec::new(), vec![level("195", "5")]);
        machine.handle(Input::Event(insert_195), now);
        assert_eq!(
            machine.handle(Input::Snapshot(narrower_snapshot(11)), now),
            Action::None
        );
        let book = machine.book().expect("still live");
        assert_eq!(
            book.band,
            Band {
                bid_floor: px("180"),
                ask_ceiling: px("300")
            }
        );
        assert_eq!(book.last_update_id, 12);
        assert_eq!(book.asks.get(&px("195")), Some(&qty("5")));
        assert_eq!(book.asks.get(&px("200")), None);
        assert_eq!(machine.counters().reanchors, 1);
        assert_eq!(machine.reanchor_buffered(), None);
    }

    #[test]
    fn reanchor_snapshot_without_bridge_is_rejected_and_the_live_book_kept() {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        assert_eq!(
            machine.handle(Input::Snapshot(wide_snapshot(5)), now),
            Action::FetchSnapshot
        );
        assert_eq!(machine.counters().snapshot_rejected, 1);
        assert_eq!(machine.counters().reanchors, 0);
        let book = machine.book().expect("still live");
        assert_eq!(
            book.band,
            Band {
                bid_floor: px("10"),
                ask_ceiling: px("200")
            }
        );
        assert_eq!(machine.reanchor_buffered(), Some(1));
    }

    #[test]
    fn gap_during_a_pending_reanchor_takes_the_ordinary_resync_path() {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        assert_eq!(
            machine.handle(Input::Event(ids(99, 11, 12)), now),
            Action::FetchSnapshot
        );
        assert_eq!(
            resync_reason(&machine),
            Some(ResyncReason::Gap {
                expected: 10,
                got_pu: 99
            })
        );
        assert_eq!(machine.reanchor_buffered(), None);
        assert_eq!(machine.buffered(), 1);
    }

    #[test]
    fn reanchor_does_not_refire_while_one_is_pending() {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        let bids_near_floor = with_levels(
            10,
            11,
            12,
            vec![level("100", "0"), level("50", "0")],
            Vec::new(),
        );
        assert_eq!(
            machine.handle(Input::Event(bids_near_floor), now),
            Action::None
        );
        assert_eq!(machine.reanchor_buffered(), Some(2));
    }

    #[test]
    fn reanchor_snapshot_newer_than_the_kept_events_waits_for_a_bridge() {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        assert_eq!(
            machine.handle(Input::Snapshot(narrower_snapshot(50)), now),
            Action::None
        );
        assert_eq!(machine.counters().reanchors, 0);
        assert_eq!(
            machine.handle(Input::Event(ids(10, 11, 51)), now),
            Action::None
        );
        assert_eq!(machine.counters().reanchors, 1);
        assert_eq!(machine.book().expect("live").band.ask_ceiling, px("300"));
    }

    #[test]
    fn reanchor_snapshot_nothing_bridges_is_rejected_after_the_stitch_timeout() {
        let started = Instant::now();
        let mut machine = pending_reanchor_machine(started);
        assert_eq!(
            machine.handle(Input::Snapshot(narrower_snapshot(50)), started),
            Action::None
        );
        for second in 1..30u64 {
            let at = started + Duration::from_secs(second);
            let last = 10 + second;
            assert_eq!(
                machine.handle(Input::Event(ids(last - 1, last, last)), at),
                Action::None
            );
            assert_eq!(machine.handle(Input::Tick, at), Action::None);
        }
        assert_eq!(
            machine.handle(Input::Tick, started + STITCH_TIMEOUT),
            Action::FetchSnapshot
        );
        assert_eq!(machine.counters().snapshot_rejected, 1);
        assert!(matches!(machine.state(), State::Live { .. }));
        assert_eq!(machine.reanchor_buffered(), Some(0));
    }

    #[test]
    fn rejected_snapshot_keeps_the_resync_reason() {
        let now = Instant::now();
        let mut machine = live_machine(now);
        assert_eq!(
            machine.handle(Input::Event(ids(9, 10, 11)), now),
            Action::FetchSnapshot
        );
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(3)), now),
            Action::FetchSnapshot
        );
        assert_eq!(
            resync_reason(&machine),
            Some(ResyncReason::Gap {
                expected: 6,
                got_pu: 9
            })
        );
        assert_eq!(machine.counters().snapshot_rejected, 1);
    }
}
