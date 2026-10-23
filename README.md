# crossbook

Keeps two live order books for one symbol, Binance USD-M futures and Aster, and derives a
signal from both.

## Market data

USD-M perpetual futures on both venues, default symbol `BTCUSDT`.

Binance
- stream `wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms`
- snapshot `https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000`

Aster
- stream `wss://fstream.asterdex.com/stream?streams=btcusdt@depth@100ms`
- snapshot `https://fapi.asterdex.com/fapi/v3/depth?symbol=BTCUSDT&limit=1000`

## Synchronization

**Bootstrap.** The stream is subscribed in the handshake and the snapshot requested after,
so diff events are already being buffered when the venue takes the snapshot. A snapshot with
`lastUpdateId = N` is stitched to the first buffered event with `U <= N <= u`: older events are
dropped, the rest replayed, and the book is live once both sides hold at least `--k-levels`. A
snapshot older than the whole chain is rejected and another one fetched; if nothing covers `N`,
the feed waits up to 30 s.

**Live updates.** Every event must have `pu` equal to the previous `u`. Anything else is a gap
and a diff stream cannot say what it did. The book is dropped, the state
becomes `resyncing(gap)`, and the event that revealed the gap stays in the buffer for the next
snapshot, which is stitched to it by the same update-id rule as at bootstrap. The socket stays
open.

**Band and re-anchor.** A snapshot vouches only for prices between its lowest bid and highest
ask, so updates outside that band are ignored. The book follows the market inward freely and
thins as the market moves outward; when a side falls below `--k-levels` it is rebuilt from a
fresh snapshot. Before it comes to that, once the best price has covered all but
`--reanchor-margin` percent of its distance to the edge, a new snapshot is fetched while the
book stays live and swapped in without a gap. A jump that crosses margin and edge within one
frame goes the rebuild way instead.

**Stale connection.** 5 seconds without a depth event (`--stale-after`) closes the socket and
reconnects.

## Architecture

Each venue runs in its own tokio task with its own socket, HTTP client and sync machine; the
feeds share only the book store. Transient errors (network, timeouts, the venue closing
the socket, bad frames, 5xx, 429/418 with `Retry-After`) are retried with exponential backoff
and jitter, snapshot requests on a separate limiter, and never touch the other feed. A 400, 404
or 451 means a wrong symbol, path or region: that feed fails permanently and the process exits
with code 1, as does a panic in any task, assumed the service needs both books.

Each feed publishes an immutable `BookView` through its own `ArcSwap`. The feed task is the
only writer. The signal task holds read-only handles and always sees a complete view without
blocking the writer. A live view carries the top `--k-levels` of each side, the last update
id, the time and latency of its last event, and the band. On a gap, a resync or a lost
connection the feed publishes an empty view with the reason, so a stale
book cannot be read by mistake, the other feed is untouched.

The implementation expects a live book never to be crossed; if it happens, the view is still
published, `crossed` is counted and a warning logged.

## Signal

Every `--signal-interval-ms` (250) the signal loads both views. It holds if either view is not
live, older than `--max-age-ms` (1000), or reached us more than `--max-latency-ms` (1000) after
the venue stamped it, naming the feed and the reason. Otherwise, over the K levels of each view:

```
mid        = (best bid + best ask) / 2
spread     = (mid_a - mid_b) / mid_b * 10000 (bps)
bid_volume = sum of bid sizes over the K levels
ask_volume = sum of ask sizes over the K levels
imbalance  = (bid_volume - ask_volume) / (bid_volume + ask_volume)
score      = spread / --spread-scale-bps + (imbalance_a + imbalance_b) / 2
```

`long` above `--threshold` (0.5), `short` below minus that, `hold(flat)` in between. Venue `a`
is the first in `--feeds`, and the signal assumes `b` follows it: when `a` quotes above `b` and
both books lean to the bid, `b` is expected to move up. The latency check complements the
local age: a view received a moment ago can still describe an old market state if the stream
was delayed. It assumes the local clock is within a second of the venue clocks.

A line goes to stdout when the verdict changes and at least every `--signal-heartbeat` (5):

```
09:07:11.248 signal hold(flat) a: binance live age=107ms latency=135ms mid=81768.15 imb=+0.512 | b: aster live age=8ms latency=18ms mid=81768.35 imb=+0.045 | spread=-0.024bps score=+0.266
```

## Run and test

```
cargo run --release
```

That runs both feeds on `BTCUSDT` until Ctrl-C; `--duration 600` stops after ten minutes and
prints the final counters. The other flags, with defaults: `--feeds binance,aster` (the order
defines `a` and `b`), `--k-levels 20`, `--depth-limit 1000`, `--stale-after 5`,
`--status-interval 2`, `--reanchor-margin 10`, `--signal-interval-ms 250`,
`--signal-heartbeat 5`, `--max-age-ms 1000`, `--max-latency-ms 1000`,
`--spread-scale-bps 2`, `--threshold 0.5`, `--log-format text|json`.

stdout is data: a status line per feed every two seconds, the signal lines, `final` lines at
exit. stderr is logs, `info` by default.

```
cargo run --release > status.log                     # data only, logs stay on the terminal
RUST_LOG=info,crossbook=trace cargo run --release    # every depth event in the logs
```

A status line shows the feed's state (`connecting`, `reconnecting`, `buffering`,
`stitching`, `live`, `resyncing`, `failed`, each with its reason), the top of the book, the
band, the age and latency of the last event, the event rate, the snapshot activity and the
counters that are not zero; the `final` lines carry every counter.

`cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo bench`.

Per event the hot path parses the frame into fixed-point `u64`, applies it to the
bounded `BTreeMap` book, checks the chain and the depth, and publishes a new view. Snapshot
fetches, logging above `trace`, the status line and the signal run outside it. Measured with
`cargo bench` on an Apple M5, rustc 1.94, on a recorded Binance frame of 58 levels and a
1000-level snapshot from `tests/fixtures/`:

| Operation                            | Time    |
| ------------------------------------ | ------- |
| Parse the frame into a `DepthEvent`  | 2.0 µs  |
| Apply it to a fresh 1000-level book  | 0.60 µs |
| Apply the same frame again           | 0.27 µs |
| Build and publish the K=20 view      | 0.14 µs |
| Whole frame-to-view path             | 2.8 µs  |

`BTreeMap` keeps each side ordered by price without shifting entries on every update. Prices
and sizes are `u64` at scale 1e8 so that they compare exactly. `f64` appears only in the signal.
Converting a scaled value to `f64` is exact up to 2^53, that is for prices and sizes below
about 90 million; the signal arithmetic itself is ordinary floating point.

## Limitations

One symbol per run, the same on both venues. Fixed-point scale 1e8 with 8 fraction digits,
values below about 1.84e11. The thresholds (stale 5 s, max age 1 s, spread scale 2 bps, score
threshold 0.5) are tuned to BTCUSDT.
