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
//! When the best price approaches a band edge, a fresh snapshot is fetched
//! without interrupting the live book. Events received during the fetch are
//! buffered and applied to the new snapshot before it replaces the current
//! book.

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::book::{Band, Book, DepthEvent, Snapshot};
use crate::fixed::Px;

/// About 100 s of events at the protocol's 10 per second, far more than a snapshot takes.
/// Dropping the oldest cannot hide a loss: if the bridging event is gone, the stitch sees a hole
/// and rejects the snapshot.
const MAX_BUFFERED_EVENTS: usize = 1024;
const OVERFLOW_WARN_EVERY: u64 = 1000;
/// Half a minute covers any stream lag still worth waiting out. If nothing has bridged a
/// snapshot by then, the numbering is off and another one is fetched.
const STITCH_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) enum Input {
    Event(DepthEvent),
    Snapshot(Snapshot),
    Tick,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    None,
    FetchSnapshot,
    ResetConnection { reason: ResetReason },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResetReason {
    Stale { silent_for: Duration },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResyncReason {
    Gap {
        expected: u64,
        got_pu: u64,
    },
    BandExhausted {
        bids: usize,
        asks: usize,
        k_levels: usize,
    },
    EmptySnapshotSide {
        bids: usize,
        asks: usize,
    },
}

/// Best prices at which a side is close enough to its edge to re-anchor.
#[derive(Clone, Copy)]
pub(crate) struct Alarm {
    bid_at: Px,
    ask_at: Px,
}

/// A fresh snapshot in flight while the book stays live. Events since the request are kept, so
/// the new book can be stitched to them.
pub(crate) struct Reanchor {
    buffer: VecDeque<DepthEvent>,
    held: Option<(Snapshot, Instant)>,
    pub(crate) requested_at: Instant,
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
        /// The resync being recovered from, or `None` for the first sync of a connection.
        after: Option<ResyncReason>,
    },
    Live {
        book: Book,
        anchored_at: Instant,
        alarm: Alarm,
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
    /// Re-anchor once the best price is within this percent of its way to the band edge.
    reanchor_margin_percent: u64,
    state: State,
    buffer: VecDeque<DepthEvent>,
    last_event_at: Instant,
    counters: SyncCounters,
}

struct Stitch {
    dropped: usize,
    verdict: StitchVerdict,
}

#[derive(Debug)]
enum StitchVerdict {
    Wait,
    Bridge(DepthEvent),
    Hole {
        first_update_id: u64,
        final_update_id: u64,
    },
}

/// Drops events older than the snapshot and pops the one bridging it.
fn stitch(
    last_update_id: u64,
    buffer: &mut VecDeque<DepthEvent>,
) -> Stitch {
    let mut dropped = 0;
    while buffer
        .front()
        .is_some_and(|event| event.final_update_id < last_update_id)
    {
        buffer.pop_front();
        dropped += 1;
    }
    let Some(first) = buffer.pop_front() else {
        return Stitch {
            dropped,
            verdict: StitchVerdict::Wait,
        };
    };
    if first.first_update_id > last_update_id {
        let verdict = StitchVerdict::Hole {
            first_update_id: first.first_update_id,
            final_update_id: first.final_update_id,
        };
        buffer.push_front(first);
        return Stitch { dropped, verdict };
    }
    Stitch {
        dropped,
        verdict: StitchVerdict::Bridge(first),
    }
}

#[derive(Clone, Copy)]
enum Rejection {
    Hole {
        first_update_id: u64,
        final_update_id: u64,
    },
    EmptySide {
        bids: usize,
        asks: usize,
    },
    Chain {
        expected: u64,
        got_pu: u64,
    },
    Thin {
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
            Rejection::Chain { expected, got_pu } => {
                write!(
                    f,
                    "chain(expected={expected},got_pu={got_pu})"
                )
            }
            Rejection::Thin { bids, asks } => {
                write!(f, "thin(bids={bids},asks={asks})")
            }
        }
    }
}

enum Built {
    Wait {
        dropped: usize,
    },
    Book {
        book: Book,
        bridge_first: u64,
        bridge_final: u64,
        replayed: u64,
        dropped: usize,
    },
    Rejected {
        why: Rejection,
        dropped: usize,
    },
}

/// Stitches a snapshot to the buffered chain: drops what predates it, applies the bridging event,
/// replays the rest and checks the depth. On rejection the buffer keeps whatever the next snapshot
/// can still use: the bridge after a hole or an empty side, the tail after a broken chain, nothing
/// after a thin book.
fn build_book(
    snapshot: &Snapshot,
    buffer: &mut VecDeque<DepthEvent>,
    k_levels: usize,
    now: Instant,
) -> Built {
    let Stitch { dropped, verdict } =
        stitch(snapshot.last_update_id, buffer);
    let bridge = match verdict {
        StitchVerdict::Wait => return Built::Wait { dropped },
        StitchVerdict::Hole {
            first_update_id,
            final_update_id,
        } => {
            return Built::Rejected {
                why: Rejection::Hole {
                    first_update_id,
                    final_update_id,
                },
                dropped,
            };
        }
        StitchVerdict::Bridge(bridge) => bridge,
    };
    let Some(mut book) = Book::from_snapshot(snapshot, now) else {
        let why = Rejection::EmptySide {
            bids: snapshot.bids.len(),
            asks: snapshot.asks.len(),
        };
        buffer.push_front(bridge);
        return Built::Rejected { why, dropped };
    };
    book.apply_event(&bridge, now);
    let mut applied: u64 = 1;
    while let Some(event) = buffer.front() {
        if event.prev_final_update_id != book.last_update_id {
            // From here on the chain is contiguous again, so it stays.
            let why = Rejection::Chain {
                expected: book.last_update_id,
                got_pu: event.prev_final_update_id,
            };
            return Built::Rejected { why, dropped };
        }
        book.apply_event(event, now);
        applied += 1;
        buffer.pop_front();
    }
    if !book.has_depth(k_levels) {
        let why = Rejection::Thin {
            bids: book.bids.len(),
            asks: book.asks.len(),
        };
        return Built::Rejected { why, dropped };
    }
    Built::Book {
        book,
        bridge_first: bridge.first_update_id,
        bridge_final: bridge.final_update_id,
        replayed: applied,
        dropped,
    }
}

fn push_bounded(
    buffer: &mut VecDeque<DepthEvent>,
    event: DepthEvent,
    dropped: &mut u64,
) {
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

fn alarm_for(book: &Book, margin_percent: u64) -> Alarm {
    let Band {
        bid_floor,
        ask_ceiling,
    } = book.band;
    // A live book has levels on both sides. The edge is a harmless value for the empty case
    // that cannot happen.
    Alarm {
        bid_at: book.best_bid().map_or(bid_floor, |(best, _)| {
            bid_floor.towards(best, margin_percent)
        }),
        ask_at: book.best_ask().map_or(ask_ceiling, |(best, _)| {
            ask_ceiling.towards(best, margin_percent)
        }),
    }
}

fn near_an_edge(alarm: Alarm, book: &Book) -> bool {
    book.best_ask().is_some_and(|(best, _)| best >= alarm.ask_at)
        || book
            .best_bid()
            .is_some_and(|(best, _)| best <= alarm.bid_at)
}

/// Where a failed stitch goes back to: the resync it came from, or the initial wait.
fn waiting_state(after: Option<ResyncReason>) -> State {
    match after {
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

    /// New socket. The stream was subscribed in the handshake, so the snapshot can go out now.
    pub(crate) fn connected(&mut self, now: Instant) -> Action {
        self.disconnected();
        self.last_event_at = now;
        Action::FetchSnapshot
    }

    pub(crate) fn disconnected(&mut self) {
        self.state = State::Buffering;
        self.buffer.clear();
    }

    pub(crate) fn state(&self) -> &State {
        &self.state
    }

    pub(crate) fn book(&self) -> Option<&Book> {
        match &self.state {
            State::Live { book, .. } => Some(book),
            State::Buffering
            | State::Stitching { .. }
            | State::Resyncing { .. } => None,
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

    #[cfg(test)]
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

    pub(crate) fn handle(
        &mut self,
        input: Input,
        now: Instant,
    ) -> Action {
        match input {
            Input::Event(event) => self.on_event(event, now),
            Input::Snapshot(snapshot) => {
                self.on_snapshot(snapshot, now)
            }
            Input::Tick => self.on_tick(now),
        }
    }

    fn on_event(
        &mut self,
        event: DepthEvent,
        now: Instant,
    ) -> Action {
        self.last_event_at = now;
        match &mut self.state {
            State::Live {
                book,
                alarm,
                reanchor,
                ..
            } => {
                let broken = if event.prev_final_update_id
                    == book.last_update_id
                {
                    book.apply_event(&event, now);
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
                    // The events kept for the re-anchor still chain into this one, unless it was
                    // the chain itself that broke. A snapshot already in hand may well be
                    // bridged by it.
                    let held = match reanchor.take() {
                        Some(pending)
                            if matches!(
                                reason,
                                ResyncReason::BandExhausted { .. }
                            ) =>
                        {
                            self.buffer = pending.buffer;
                            pending.held.map(|(snapshot, _)| snapshot)
                        }
                        _ => None,
                    };
                    let action = self.resync(reason);
                    // This event starts the chain the next snapshot stitches to.
                    self.buffer_event(event);
                    return match held {
                        Some(snapshot) => {
                            self.on_snapshot(snapshot, now)
                        }
                        None => action,
                    };
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
                    None if near_an_edge(*alarm, book) => {
                        info!(
                            bid_at = %alarm.bid_at,
                            ask_at = %alarm.ask_at,
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
        push_bounded(
            &mut self.buffer,
            event,
            &mut self.counters.buffer_dropped,
        );
    }

    fn on_snapshot(
        &mut self,
        snapshot: Snapshot,
        now: Instant,
    ) -> Action {
        let after = match &mut self.state {
            State::Live {
                reanchor: Some(pending),
                ..
            } => {
                pending.held = Some((snapshot, now));
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
            State::Stitching { after, .. } => *after,
        };
        self.state = State::Stitching {
            snapshot,
            dropped: 0,
            since: now,
            after,
        };
        self.try_stitch(now)
    }

    fn try_stitch(&mut self, now: Instant) -> Action {
        let State::Stitching {
            snapshot,
            dropped: dropped_earlier,
            after,
            ..
        } = &self.state
        else {
            unreachable!("try_stitch runs only in Stitching");
        };
        let (last_update_id, dropped_earlier, after) =
            (snapshot.last_update_id, *dropped_earlier, *after);
        match build_book(
            snapshot, &mut self.buffer, self.k_levels, now,
        ) {
            Built::Wait { dropped } => {
                // Dropped counts are summed over the whole wait, so the live line shows what
                // the snapshot cost in full.
                let dropped = dropped_earlier + dropped;
                debug!(
                    last_update_id,
                    dropped,
                    "waiting for an event that covers the snapshot"
                );
                if let State::Stitching { dropped: total, .. } =
                    &mut self.state
                {
                    *total = dropped;
                }
                Action::None
            }
            Built::Book {
                book,
                bridge_first,
                bridge_final,
                replayed,
                dropped,
            } => {
                self.counters.events_applied += replayed;
                info!(
                    last_update_id,
                    bridge_first,
                    bridge_final,
                    dropped = dropped_earlier + dropped,
                    replayed,
                    bids = book.bids.len(),
                    asks = book.asks.len(),
                    bid_floor = %book.band.bid_floor,
                    ask_ceiling = %book.band.ask_ceiling,
                    "live"
                );
                self.state = State::Live {
                    alarm: alarm_for(
                        &book,
                        self.reanchor_margin_percent,
                    ),
                    book,
                    anchored_at: now,
                    reanchor: None,
                };
                Action::None
            }
            Built::Rejected { why, dropped } => self.reject_stitch(
                why,
                last_update_id,
                dropped_earlier + dropped,
                after,
            ),
        }
    }

    fn reject_stitch(
        &mut self,
        why: Rejection,
        last_update_id: u64,
        dropped: usize,
        after: Option<ResyncReason>,
    ) -> Action {
        match why {
            Rejection::Hole {
                first_update_id,
                final_update_id,
            } => {
                self.counters.snapshot_rejected += 1;
                warn!(
                    last_update_id,
                    first_update_id,
                    final_update_id,
                    dropped,
                    buffered = self.buffer.len(),
                    "snapshot rejected: first buffered event starts after it"
                );
                self.state = waiting_state(after);
                Action::FetchSnapshot
            }
            Rejection::EmptySide { bids, asks } => {
                self.resync(ResyncReason::EmptySnapshotSide {
                    bids,
                    asks,
                })
            }
            Rejection::Chain { expected, got_pu } => {
                self.resync(ResyncReason::Gap { expected, got_pu })
            }
            Rejection::Thin { bids, asks } => {
                self.resync(ResyncReason::BandExhausted {
                    bids,
                    asks,
                    k_levels: self.k_levels,
                })
            }
        }
    }

    fn try_swap(&mut self, now: Instant) -> Action {
        let State::Live {
            book,
            reanchor: Some(pending),
            ..
        } = &mut self.state
        else {
            unreachable!(
                "try_swap runs only with a re-anchor pending"
            );
        };
        let Some((snapshot, held_since)) = pending.held.take() else {
            unreachable!("try_swap runs only with a snapshot held");
        };
        let last_update_id = snapshot.last_update_id;
        match build_book(
            &snapshot, &mut pending.buffer, self.k_levels, now,
        ) {
            Built::Wait { dropped } => {
                debug!(
                    last_update_id,
                    dropped,
                    "waiting for an event that covers the re-anchor snapshot"
                );
                pending.held = Some((snapshot, held_since));
                Action::None
            }
            Built::Book {
                book: fresh,
                replayed,
                dropped,
                ..
            } => {
                info!(
                    last_update_id,
                    old_bid_floor = %book.band.bid_floor,
                    old_ask_ceiling = %book.band.ask_ceiling,
                    bid_floor = %fresh.band.bid_floor,
                    ask_ceiling = %fresh.band.ask_ceiling,
                    dropped,
                    replayed,
                    bids = fresh.bids.len(),
                    asks = fresh.asks.len(),
                    "re-anchored"
                );
                self.counters.reanchors += 1;
                self.state = State::Live {
                    alarm: alarm_for(
                        &fresh,
                        self.reanchor_margin_percent,
                    ),
                    book: fresh,
                    anchored_at: now,
                    reanchor: None,
                };
                Action::None
            }
            Built::Rejected { why, dropped } => {
                // The live book is untouched, so this only costs another fetch.
                self.counters.snapshot_rejected += 1;
                warn!(last_update_id, %why, dropped, "re-anchor snapshot rejected, fetching another");
                Action::FetchSnapshot
            }
        }
    }

    fn resync(&mut self, reason: ResyncReason) -> Action {
        match reason {
            ResyncReason::Gap { expected, got_pu } => {
                self.counters.gaps += 1;
                warn!(
                    expected,
                    got_pu, "gap in update ids, rebuilding book"
                );
            }
            ResyncReason::BandExhausted {
                bids,
                asks,
                k_levels,
            } => {
                self.counters.band_exhausted += 1;
                warn!(
                    bids,
                    asks, k_levels, "band exhausted, rebuilding book"
                );
            }
            ResyncReason::EmptySnapshotSide { bids, asks } => {
                self.counters.snapshot_rejected += 1;
                warn!(
                    bids,
                    asks,
                    "snapshot has an empty side, fetching another"
                );
            }
        }
        // There is nothing to clear here. In Live the buffer is empty, and after a failed replay
        // it holds exactly the chain the next snapshot has to be stitched to.
        self.state = State::Resyncing { reason };
        Action::FetchSnapshot
    }

    fn on_tick(&mut self, now: Instant) -> Action {
        let silent_for =
            now.saturating_duration_since(self.last_event_at);
        if silent_for >= self.stale_after {
            self.counters.stale_timeouts += 1;
            warn!(
                silent_ms = silent_for.as_millis(),
                state = self.state.name(),
                "no events, connection is stale"
            );
            self.disconnected();
            return Action::ResetConnection {
                reason: ResetReason::Stale { silent_for },
            };
        }
        match &mut self.state {
            State::Stitching {
                snapshot,
                since,
                after,
                ..
            } if now.saturating_duration_since(*since)
                >= STITCH_TIMEOUT =>
            {
                self.counters.snapshot_rejected += 1;
                warn!(
                    last_update_id = snapshot.last_update_id,
                    waited_ms = STITCH_TIMEOUT.as_millis(),
                    buffered = self.buffer.len(),
                    "no event covered the snapshot in time, fetching another"
                );
                self.state = waiting_state(*after);
                Action::FetchSnapshot
            }
            State::Live {
                reanchor: Some(pending),
                ..
            } if pending.held.as_ref().is_some_and(
                |(_, held_since)| {
                    now.saturating_duration_since(*held_since)
                        >= STITCH_TIMEOUT
                },
            ) =>
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
            } => write!(
                f,
                "band-exhausted(bids={bids},asks={asks},k={k_levels})"
            ),
            ResyncReason::EmptySnapshotSide { bids, asks } => {
                write!(
                    f,
                    "empty-snapshot-side(bids={bids},asks={asks})"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed::{Px, Qty};

    const K: usize = 2;
    const STALE_AFTER: Duration = Duration::from_secs(5);
    const REANCHOR_MARGIN_PERCENT: u64 = 10;

    fn level(price: &str, size: &str) -> (Px, Qty) {
        (
            price.parse().expect("test literal"),
            size.parse().expect("test literal"),
        )
    }

    fn ids(prev: u64, first: u64, last: u64) -> DepthEvent {
        DepthEvent {
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
        let mut machine = SyncMachine::new(
            K,
            STALE_AFTER,
            REANCHOR_MARGIN_PERCENT,
            now,
        );
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
            State::Buffering
            | State::Stitching { .. }
            | State::Live { .. } => None,
        }
    }

    struct StitchCase {
        name: &'static str,
        last_update_id: u64,
        buffered: &'static [(u64, u64)],
        dropped: usize,
        expected: Expected,
    }

    #[derive(Clone, Copy)]
    enum Expected {
        Wait,
        Bridge { first_update_id: u64 },
        Hole { first_update_id: u64 },
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
                .map(|&(first, last)| {
                    ids(first.saturating_sub(1), first, last)
                })
                .collect();
            let Stitch { dropped, verdict } =
                stitch(case.last_update_id, &mut buffer);
            assert_eq!(dropped, case.dropped, "{}", case.name);
            match (verdict, case.expected) {
                (StitchVerdict::Wait, Expected::Wait) => {}
                (
                    StitchVerdict::Bridge(bridge),
                    Expected::Bridge { first_update_id },
                ) => {
                    assert_eq!(
                        bridge.first_update_id, first_update_id,
                        "{}",
                        case.name
                    );
                }
                (
                    StitchVerdict::Hole {
                        first_update_id, ..
                    },
                    Expected::Hole {
                        first_update_id: expected,
                    },
                ) => {
                    assert_eq!(
                        first_update_id, expected,
                        "{}",
                        case.name
                    );
                }
                (verdict, _) => panic!(
                    "{}: unexpected verdict {verdict:?}",
                    case.name
                ),
            }
        }
    }

    #[test]
    fn stitch_keeps_events_after_the_bridge_in_the_buffer() {
        let mut buffer: VecDeque<DepthEvent> =
            [ids(89, 90, 95), ids(95, 96, 101), ids(101, 102, 105)]
                .into();
        let Stitch { verdict, .. } = stitch(100, &mut buffer);
        assert!(matches!(verdict, StitchVerdict::Bridge(_)));
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].first_update_id, 102);
    }

    #[test]
    fn connect_requests_the_snapshot_once() {
        let now = Instant::now();
        let mut machine = machine(now);
        assert_eq!(
            machine.handle(Input::Event(ids(4, 5, 6)), now),
            Action::None
        );
        assert_eq!(
            machine.handle(Input::Event(ids(6, 7, 8)), now),
            Action::None
        );
        assert_eq!(machine.buffered(), 2);
        assert!(matches!(machine.state(), State::Buffering));
    }

    #[test]
    fn snapshot_bridged_by_buffered_event_goes_live() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.handle(Input::Event(ids(4, 5, 6)), now);
        machine.handle(Input::Event(ids(6, 7, 8)), now);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(7)), now),
            Action::None
        );
        let book = machine.book().expect("live");
        assert_eq!(book.last_update_id, 8);
        assert_eq!(machine.counters().events_applied, 1);
        assert_eq!(machine.buffered(), 0);
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
        assert_eq!(
            machine.book().expect("live again").last_update_id,
            15
        );
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
    fn snapshot_with_an_empty_side_resyncs() {
        let now = Instant::now();
        let mut machine = machine(now);
        machine.handle(Input::Event(ids(4, 5, 6)), now);
        let mut one_sided = snapshot(6);
        one_sided.asks.clear();
        assert_eq!(
            machine.handle(Input::Snapshot(one_sided), now),
            Action::FetchSnapshot
        );
        assert_eq!(
            resync_reason(&machine),
            Some(ResyncReason::EmptySnapshotSide {
                bids: 2,
                asks: 0
            })
        );
    }

    #[test]
    fn snapshot_while_live_is_ignored() {
        let now = Instant::now();
        let mut machine = live_machine(now);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(9)), now),
            Action::None
        );
        assert_eq!(
            machine.book().expect("still live").last_update_id,
            6
        );
    }

    #[test]
    fn silence_longer_than_stale_after_resets_the_connection_in_any_state()
     {
        let connected_at = Instant::now();
        let mut buffering = machine(connected_at);
        let mut stitching = machine(connected_at);
        stitching.handle(Input::Event(ids(4, 5, 6)), connected_at);
        stitching.handle(Input::Snapshot(snapshot(10)), connected_at);
        assert!(matches!(stitching.state(), State::Stitching { .. }));
        let mut live = live_machine(connected_at);
        for machine in [&mut buffering, &mut stitching, &mut live] {
            assert_eq!(
                machine.handle(
                    Input::Tick,
                    connected_at + Duration::from_secs(4)
                ),
                Action::None
            );
            assert_eq!(
                machine
                    .handle(Input::Tick, connected_at + STALE_AFTER),
                Action::ResetConnection {
                    reason: ResetReason::Stale {
                        silent_for: STALE_AFTER
                    }
                }
            );
            assert_eq!(machine.counters().stale_timeouts, 1);
            assert!(matches!(machine.state(), State::Buffering));
        }
    }

    #[test]
    fn snapshot_nothing_can_bridge_is_rejected_after_the_stitch_timeout()
     {
        let connected_at = Instant::now();
        let mut machine = machine(connected_at);
        assert_eq!(
            machine
                .handle(Input::Snapshot(snapshot(100)), connected_at),
            Action::None
        );
        for second in 1..30u64 {
            let at = connected_at + Duration::from_secs(second);
            let event = ids(second + 3, second + 4, second + 4);
            assert_eq!(
                machine.handle(Input::Event(event), at),
                Action::None
            );
            assert_eq!(machine.handle(Input::Tick, at), Action::None);
        }
        assert_eq!(
            machine
                .handle(Input::Tick, connected_at + STITCH_TIMEOUT),
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
            machine.handle(
                Input::Tick,
                connected_at + Duration::from_secs(8)
            ),
            Action::None
        );
        assert!(matches!(
            machine.handle(
                Input::Tick,
                connected_at + Duration::from_secs(9)
            ),
            Action::ResetConnection { .. }
        ));
    }

    #[test]
    fn overflowed_buffer_rejects_a_snapshot_it_can_no_longer_bridge()
    {
        let now = Instant::now();
        let mut machine = machine(now);
        for i in 0..MAX_BUFFERED_EVENTS + 3 {
            let first = u64::try_from(i).expect("small index");
            machine.handle(
                Input::Event(ids(first, first + 1, first + 1)),
                now,
            );
        }
        assert_eq!(machine.buffered(), MAX_BUFFERED_EVENTS);
        assert_eq!(machine.counters().buffer_dropped, 3);
        assert_eq!(
            machine.handle(Input::Snapshot(snapshot(2)), now),
            Action::FetchSnapshot
        );
        assert_eq!(machine.counters().snapshot_rejected, 1);
    }

    fn px(text: &str) -> Px {
        text.parse().expect("test literal")
    }

    fn qty(text: &str) -> Qty {
        text.parse().expect("test literal")
    }

    fn with_levels(
        prev: u64,
        first: u64,
        last: u64,
        bids: Vec<(Px, Qty)>,
        asks: Vec<(Px, Qty)>,
    ) -> DepthEvent {
        DepthEvent {
            first_update_id: first,
            final_update_id: last,
            prev_final_update_id: prev,
            bids,
            asks,
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
        let delete_101 =
            with_levels(6, 7, 8, Vec::new(), vec![level("101", "0")]);
        assert_eq!(
            machine.handle(Input::Event(delete_101), now),
            Action::None
        );
        let delete_150 = with_levels(
            8,
            9,
            10,
            Vec::new(),
            vec![level("150", "0")],
        );
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
            asks: vec![
                level("191", "1"),
                level("250", "1"),
                level("300", "1"),
            ],
        }
    }

    #[test]
    fn reanchor_fires_at_the_margin_and_not_before() {
        let now = Instant::now();
        let mut machine = wide_live_machine(now);
        let delete_101 =
            with_levels(6, 7, 8, Vec::new(), vec![level("101", "0")]);
        assert_eq!(
            machine.handle(Input::Event(delete_101), now),
            Action::None
        );
        assert_eq!(machine.reanchor_buffered(), None);
        let delete_150 = with_levels(
            8,
            9,
            10,
            Vec::new(),
            vec![level("150", "0")],
        );
        assert_eq!(
            machine.handle(Input::Event(delete_150), now),
            Action::FetchSnapshot
        );
        assert!(matches!(machine.state(), State::Live { .. }));
        assert_eq!(machine.reanchor_buffered(), Some(1));
    }

    #[test]
    fn events_during_a_pending_reanchor_reach_both_book_and_buffer() {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        let insert_195 = with_levels(
            10,
            11,
            12,
            Vec::new(),
            vec![level("195", "5")],
        );
        assert_eq!(
            machine.handle(Input::Event(insert_195), now),
            Action::None
        );
        let book = machine.book().expect("live");
        assert_eq!(book.asks.get(&px("195")), Some(&qty("5")));
        assert_eq!(book.last_update_id, 12);
        assert_eq!(machine.reanchor_buffered(), Some(2));
    }

    #[test]
    fn successful_reanchor_keeps_live_and_changes_the_band() {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        let insert_195 = with_levels(
            10,
            11,
            12,
            Vec::new(),
            vec![level("195", "5")],
        );
        machine.handle(Input::Event(insert_195), now);
        assert_eq!(
            machine
                .handle(Input::Snapshot(narrower_snapshot(11)), now),
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
    fn reanchor_snapshot_without_bridge_is_rejected_and_the_live_book_kept()
     {
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
    fn gap_during_a_pending_reanchor_takes_the_ordinary_resync_path()
    {
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
    fn band_exhaustion_during_a_pending_reanchor_keeps_the_kept_events()
     {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        let delete_191 = with_levels(
            10,
            11,
            12,
            Vec::new(),
            vec![level("191", "0")],
        );
        assert_eq!(
            machine.handle(Input::Event(delete_191), now),
            Action::FetchSnapshot
        );
        assert_eq!(
            resync_reason(&machine),
            Some(ResyncReason::BandExhausted {
                bids: 4,
                asks: 1,
                k_levels: K
            })
        );
        assert_eq!(machine.buffered(), 2);
        assert_eq!(
            machine.handle(Input::Snapshot(wide_snapshot(10)), now),
            Action::None
        );
        assert_eq!(
            machine.book().expect("live again").last_update_id,
            12
        );
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
    fn reanchor_snapshot_newer_than_the_kept_events_waits_for_a_bridge()
     {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        assert_eq!(
            machine
                .handle(Input::Snapshot(narrower_snapshot(50)), now),
            Action::None
        );
        assert_eq!(machine.counters().reanchors, 0);
        assert_eq!(
            machine.handle(Input::Event(ids(10, 11, 51)), now),
            Action::None
        );
        assert_eq!(machine.counters().reanchors, 1);
        assert_eq!(
            machine.book().expect("live").band.ask_ceiling,
            px("300")
        );
    }

    #[test]
    fn reanchor_snapshot_nothing_bridges_is_rejected_after_the_stitch_timeout()
     {
        let started = Instant::now();
        let mut machine = pending_reanchor_machine(started);
        assert_eq!(
            machine.handle(
                Input::Snapshot(narrower_snapshot(50)),
                started
            ),
            Action::None
        );
        for second in 1..30u64 {
            let at = started + Duration::from_secs(second);
            let last = 10 + second;
            assert_eq!(
                machine.handle(
                    Input::Event(ids(last - 1, last, last)),
                    at
                ),
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
    fn band_exhaustion_with_a_held_reanchor_snapshot_stitches_it_at_once()
     {
        let now = Instant::now();
        let mut machine = pending_reanchor_machine(now);
        assert_eq!(
            machine
                .handle(Input::Snapshot(narrower_snapshot(50)), now),
            Action::None
        );
        let delete_191 = with_levels(
            10,
            11,
            51,
            Vec::new(),
            vec![level("191", "0")],
        );
        assert_eq!(
            machine.handle(Input::Event(delete_191), now),
            Action::None
        );
        let book =
            machine.book().expect("live from the held snapshot");
        assert_eq!(book.last_update_id, 51);
        assert_eq!(book.band.ask_ceiling, px("300"));
        assert_eq!(book.asks.get(&px("191")), None);
        assert_eq!(machine.counters().band_exhausted, 1);
        assert_eq!(machine.counters().reanchors, 0);
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
