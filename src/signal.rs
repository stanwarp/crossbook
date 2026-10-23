use std::fmt;
use std::time::{Duration, Instant, SystemTime};

use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tokio::time::{self, MissedTickBehavior};
use tracing::warn;

use crate::fixed::Px;
use crate::status::TimeOfDay;
use crate::store::BookReader;
use crate::view::{BookView, ViewState};

pub struct SignalConfig {
    pub max_age: Duration,
    pub max_latency_ms: i64,
    pub spread_scale_bps: f64,
    pub threshold: f64,
}

pub struct Pace {
    pub interval: Duration,
    pub heartbeat: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum HoldCause {
    NotLive { feed: &'static str },
    Stale { feed: &'static str },
    Lagging { feed: &'static str },
    Flat,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Verdict {
    Hold(HoldCause),
    LongBias,
    ShortBias,
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hold(HoldCause::NotLive { feed }) => {
                write!(f, "hold(not-live {feed})")
            }
            Self::Hold(HoldCause::Stale { feed }) => {
                write!(f, "hold(stale {feed})")
            }
            Self::Hold(HoldCause::Lagging { feed }) => {
                write!(f, "hold(lagging {feed})")
            }
            Self::Hold(HoldCause::Flat) => f.write_str("hold(flat)"),
            Self::LongBias => f.write_str("long"),
            Self::ShortBias => f.write_str("short"),
        }
    }
}

#[derive(Clone, Copy)]
struct Quote {
    mid: Px,
    imbalance: f64,
}

struct SideReport {
    feed: &'static str,
    state: ViewState,
    age: Duration,
    latency_ms: Option<i64>,
    quote: Option<Quote>,
}

impl fmt::Display for SideReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} age={}ms",
            self.feed,
            self.state,
            self.age.as_millis()
        )?;
        match self.latency_ms {
            Some(ms) => write!(f, " latency={ms}ms")?,
            None => f.write_str(" latency=-")?,
        }
        match self.quote {
            Some(Quote { mid, imbalance }) => {
                write!(f, " mid={mid} imb={imbalance:+.3}")
            }
            None => f.write_str(" mid=- imb=-"),
        }
    }
}

#[derive(Clone, Copy)]
struct Cross {
    spread_bps: f64,
    score: f64,
}

struct Report {
    a: SideReport,
    b: SideReport,
    cross: Option<Cross>,
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a: {} | b: {}", self.a, self.b)?;
        match self.cross {
            Some(Cross { spread_bps, score }) => {
                write!(f, " | spread={spread_bps:+.3}bps score={score:+.3}")
            }
            None => f.write_str(" | spread=- score=-"),
        }
    }
}

struct Decision {
    verdict: Verdict,
    report: Report,
}

impl fmt::Display for Decision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.verdict, self.report)
    }
}

impl SideReport {
    fn new(view: &BookView, now: Instant) -> Self {
        let quote = match (view.state, view.bids.first(), view.asks.first()) {
            (ViewState::Live, Some(&(best_bid, _)), Some(&(best_ask, _))) => {
                let bids: f64 = view.bids.iter().map(|(_, size)| size.to_f64()).sum();
                let asks: f64 = view.asks.iter().map(|(_, size)| size.to_f64()).sum();
                Some(Quote {
                    mid: best_bid.midpoint(best_ask),
                    // A live book has no zero-size levels, so the sum is positive.
                    imbalance: (bids - asks) / (bids + asks),
                })
            }
            _ => None,
        };
        SideReport {
            feed: view.feed,
            state: view.state,
            age: now.saturating_duration_since(view.last_event_at),
            latency_ms: view.latency_ms,
            quote,
        }
    }
}

/// Venue `a` is assumed to lead: `a` above `b` with both books leaning to the bid reads as `b`
/// about to move up.
fn evaluate(a: &BookView, b: &BookView, now: Instant, cfg: &SignalConfig) -> Decision {
    let (a, b) = (SideReport::new(a, now), SideReport::new(b, now));
    let cross = match (a.quote, b.quote) {
        (Some(quote_a), Some(quote_b)) => {
            let (mid_a, mid_b) = (quote_a.mid.to_f64(), quote_b.mid.to_f64());
            let spread_bps = (mid_a - mid_b) / mid_b * 10_000.0;
            Some(Cross {
                spread_bps,
                score: spread_bps / cfg.spread_scale_bps
                    + f64::midpoint(quote_a.imbalance, quote_b.imbalance),
            })
        }
        _ => None,
    };
    let sides = [&a, &b];
    let not_live = sides.iter().find(|side| side.state != ViewState::Live);
    let stale = sides.iter().find(|side| side.age > cfg.max_age);
    let lagging = sides
        .iter()
        .find(|side| side.latency_ms.is_some_and(|ms| ms > cfg.max_latency_ms));
    let verdict = if let Some(side) = not_live {
        Verdict::Hold(HoldCause::NotLive { feed: side.feed })
    } else if let Some(side) = stale {
        Verdict::Hold(HoldCause::Stale { feed: side.feed })
    } else if let Some(side) = lagging {
        Verdict::Hold(HoldCause::Lagging { feed: side.feed })
    } else {
        match cross.map(|cross| cross.score) {
            Some(score) if score > cfg.threshold => Verdict::LongBias,
            Some(score) if score < -cfg.threshold => Verdict::ShortBias,
            _ => Verdict::Hold(HoldCause::Flat),
        }
    };
    Decision {
        verdict,
        report: Report { a, b, cross },
    }
}

/// Due on a verdict change and at least once per heartbeat.
struct Cadence {
    heartbeat: Duration,
    last: Option<(Verdict, Instant)>,
}

impl Cadence {
    fn new(heartbeat: Duration) -> Self {
        Cadence {
            heartbeat,
            last: None,
        }
    }

    fn due(&mut self, verdict: Verdict, now: Instant) -> bool {
        let due = match self.last {
            Some((printed, at)) => {
                printed != verdict || now.saturating_duration_since(at) >= self.heartbeat
            }
            None => true,
        };
        if due {
            self.last = Some((verdict, now));
        }
        due
    }
}

#[derive(Default)]
pub struct SignalSummary {
    evaluations: u64,
    long: u64,
    short: u64,
    hold: u64,
}

impl SignalSummary {
    fn count(&mut self, verdict: Verdict) {
        self.evaluations += 1;
        match verdict {
            Verdict::LongBias => self.long += 1,
            Verdict::ShortBias => self.short += 1,
            Verdict::Hold(_) => self.hold += 1,
        }
    }
}

impl fmt::Display for SignalSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "evaluations={} long={} short={} hold={}",
            self.evaluations, self.long, self.short, self.hold
        )
    }
}

pub async fn run(
    a: BookReader,
    b: BookReader,
    cfg: SignalConfig,
    pace: Pace,
    mut shutdown: watch::Receiver<bool>,
) -> SignalSummary {
    let mut tick = time::interval(pace.interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut cadence = Cadence::new(pace.heartbeat);
    let mut summary = SignalSummary::default();
    let mut out = tokio::io::stdout();
    let mut stdout_open = true;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let (view_a, view_b) = (a.load(), b.load());
                let now = Instant::now();
                let decision = evaluate(&view_a, &view_b, now, &cfg);
                summary.count(decision.verdict);
                if cadence.due(decision.verdict, now) && stdout_open {
                    let line = format!("{} signal {decision}\n", TimeOfDay(SystemTime::now()));
                    if out.write_all(line.as_bytes()).await.is_err() {
                        stdout_open = false;
                        warn!("stdout closed, signal lines stop");
                    }
                }
            }
            _ = shutdown.changed() => return summary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed::{level, px};
    use crate::view::NotLiveReason;

    const MAX_AGE: Duration = Duration::from_secs(1);

    fn cfg() -> SignalConfig {
        SignalConfig {
            max_age: MAX_AGE,
            max_latency_ms: 1000,
            spread_scale_bps: 2.0,
            threshold: 0.5,
        }
    }

    struct Side {
        feed: &'static str,
        mid: &'static str,
        bid_size: &'static str,
        ask_size: &'static str,
        age: Duration,
        latency_ms: Option<i64>,
    }

    /// A live view with one level per side half a unit around `mid`.
    fn live(side: &Side, now: Instant) -> BookView {
        let mid: f64 = side.mid.parse().expect("test literal");
        let bid = format!("{:.1}", mid - 0.5);
        let ask = format!("{:.1}", mid + 0.5);
        BookView {
            feed: side.feed,
            state: ViewState::Live,
            bids: vec![level(&bid, side.bid_size)],
            asks: vec![level(&ask, side.ask_size)],
            last_update_id: 1,
            last_event_at: now.checked_sub(side.age).expect("test ages are short"),
            latency_ms: side.latency_ms,
            band: None,
        }
    }

    fn fresh(
        feed: &'static str,
        mid: &'static str,
        bid_size: &'static str,
        ask_size: &'static str,
    ) -> Side {
        Side {
            feed,
            mid,
            bid_size,
            ask_size,
            age: Duration::from_millis(50),
            latency_ms: Some(20),
        }
    }

    struct Case {
        name: &'static str,
        a: BookView,
        b: BookView,
        expected: Verdict,
    }

    fn assert_verdicts(cases: &[Case], now: Instant) {
        for case in cases {
            let decision = evaluate(&case.a, &case.b, now, &cfg());
            assert_eq!(decision.verdict, case.expected, "{}", case.name);
        }
    }

    #[test]
    fn live_fresh_books_lean_by_spread_and_imbalance() {
        let now = Instant::now();
        let cases = [
            Case {
                name: "a above b and both bid-heavy leans long",
                a: live(&fresh("binance", "100", "9", "1"), now),
                b: live(&fresh("aster", "99.9", "9", "1"), now),
                expected: Verdict::LongBias,
            },
            Case {
                name: "a below b and both ask-heavy leans short",
                a: live(&fresh("binance", "99.9", "1", "9"), now),
                b: live(&fresh("aster", "100", "1", "9"), now),
                expected: Verdict::ShortBias,
            },
            Case {
                name: "equal mids and a lean inside the threshold hold flat",
                a: live(&fresh("binance", "100", "13", "7"), now),
                b: live(&fresh("aster", "100", "13", "7"), now),
                expected: Verdict::Hold(HoldCause::Flat),
            },
            Case {
                name: "a score exactly at the threshold still holds",
                a: live(&fresh("binance", "100", "15", "5"), now),
                b: live(&fresh("aster", "100", "15", "5"), now),
                expected: Verdict::Hold(HoldCause::Flat),
            },
            Case {
                name: "age exactly max_age is still fresh",
                a: live(
                    &Side {
                        age: MAX_AGE,
                        ..fresh("binance", "100", "9", "1")
                    },
                    now,
                ),
                b: live(&fresh("aster", "99.9", "9", "1"), now),
                expected: Verdict::LongBias,
            },
            Case {
                name: "a venue clock ahead of ours is not lagging",
                a: live(
                    &Side {
                        latency_ms: Some(-40),
                        ..fresh("binance", "100", "9", "1")
                    },
                    now,
                ),
                b: live(&fresh("aster", "99.9", "9", "1"), now),
                expected: Verdict::LongBias,
            },
        ];
        assert_verdicts(&cases, now);
    }

    #[test]
    fn a_book_that_is_not_live_stale_or_lagging_holds_naming_its_feed() {
        let now = Instant::now();
        let stale = Duration::from_millis(1001);
        let cases = [
            Case {
                name: "a not live",
                a: BookView::empty("binance", NotLiveReason::Buffering, now),
                b: live(&fresh("aster", "100", "9", "1"), now),
                expected: Verdict::Hold(HoldCause::NotLive { feed: "binance" }),
            },
            Case {
                name: "b not live",
                a: live(&fresh("binance", "100", "9", "1"), now),
                b: BookView::empty("aster", NotLiveReason::Connecting, now),
                expected: Verdict::Hold(HoldCause::NotLive { feed: "aster" }),
            },
            Case {
                name: "a stale",
                a: live(
                    &Side {
                        age: stale,
                        ..fresh("binance", "100", "9", "1")
                    },
                    now,
                ),
                b: live(&fresh("aster", "99.9", "9", "1"), now),
                expected: Verdict::Hold(HoldCause::Stale { feed: "binance" }),
            },
            Case {
                name: "b stale",
                a: live(&fresh("binance", "100", "9", "1"), now),
                b: live(
                    &Side {
                        age: stale,
                        ..fresh("aster", "99.9", "9", "1")
                    },
                    now,
                ),
                expected: Verdict::Hold(HoldCause::Stale { feed: "aster" }),
            },
            Case {
                name: "a lagging by the venue clock",
                a: live(
                    &Side {
                        latency_ms: Some(1500),
                        ..fresh("binance", "100", "9", "1")
                    },
                    now,
                ),
                b: live(&fresh("aster", "99.9", "9", "1"), now),
                expected: Verdict::Hold(HoldCause::Lagging { feed: "binance" }),
            },
        ];
        assert_verdicts(&cases, now);
    }

    #[test]
    fn hold_causes_are_checked_in_order_not_live_then_stale_then_lagging() {
        let now = Instant::now();
        let stale = Duration::from_millis(1001);
        let cases = [
            Case {
                name: "not live outranks stale on the same side",
                a: BookView::empty(
                    "binance",
                    NotLiveReason::Buffering,
                    now.checked_sub(stale).expect("test ages are short"),
                ),
                b: live(&fresh("aster", "100", "9", "1"), now),
                expected: Verdict::Hold(HoldCause::NotLive { feed: "binance" }),
            },
            Case {
                name: "not live on b outranks stale on a",
                a: live(
                    &Side {
                        age: stale,
                        ..fresh("binance", "100", "9", "1")
                    },
                    now,
                ),
                b: BookView::empty("aster", NotLiveReason::Connecting, now),
                expected: Verdict::Hold(HoldCause::NotLive { feed: "aster" }),
            },
            Case {
                name: "stale on b outranks lagging on a",
                a: live(
                    &Side {
                        latency_ms: Some(1500),
                        ..fresh("binance", "100", "9", "1")
                    },
                    now,
                ),
                b: live(
                    &Side {
                        age: stale,
                        ..fresh("aster", "99.9", "9", "1")
                    },
                    now,
                ),
                expected: Verdict::Hold(HoldCause::Stale { feed: "aster" }),
            },
        ];
        assert_verdicts(&cases, now);
    }

    #[test]
    fn stale_hold_still_reports_both_mids_and_the_spread() {
        let now = Instant::now();
        let a = live(
            &Side {
                age: Duration::from_secs(2),
                ..fresh("binance", "100", "9", "1")
            },
            now,
        );
        let b = live(&fresh("aster", "99.9", "9", "1"), now);
        let decision = evaluate(&a, &b, now, &cfg());
        assert_eq!(
            decision.verdict,
            Verdict::Hold(HoldCause::Stale { feed: "binance" })
        );
        assert!(decision.report.cross.is_some());
        assert_eq!(decision.report.a.quote.map(|q| q.mid), Some(px("100")));
        assert_eq!(decision.report.b.quote.map(|q| q.mid), Some(px("99.9")));
    }

    #[test]
    fn display_carries_every_number() {
        let now = Instant::now();
        let a = live(
            &Side {
                latency_ms: Some(17),
                ..fresh("binance", "100", "9", "1")
            },
            now,
        );
        let b = live(&fresh("aster", "99.9", "3", "7"), now);
        let line = evaluate(&a, &b, now, &cfg()).to_string();
        for expected in [
            "long",
            "a: binance live age=50ms latency=17ms mid=100 imb=+0.800",
            "b: aster live age=50ms latency=20ms mid=99.9 imb=-0.400",
            "spread=+10.010bps",
            "score=+5.205",
        ] {
            assert!(
                line.contains(expected),
                "{expected:?} missing from {line:?}"
            );
        }
    }

    #[test]
    fn a_line_is_due_on_change_and_on_heartbeat_only() {
        let start = Instant::now();
        let mut cadence = Cadence::new(Duration::from_secs(5));
        let flat = Verdict::Hold(HoldCause::Flat);
        assert!(cadence.due(flat, start));
        assert!(!cadence.due(flat, start + Duration::from_secs(1)));
        assert!(cadence.due(Verdict::LongBias, start + Duration::from_secs(2)));
        assert!(!cadence.due(Verdict::LongBias, start + Duration::from_secs(6)));
        assert!(cadence.due(Verdict::LongBias, start + Duration::from_secs(7)));
        let stale_a = Verdict::Hold(HoldCause::Stale { feed: "binance" });
        let stale_b = Verdict::Hold(HoldCause::Stale { feed: "aster" });
        assert!(cadence.due(stale_a, start + Duration::from_secs(8)));
        assert!(cadence.due(stale_b, start + Duration::from_secs(8)));
    }
}
