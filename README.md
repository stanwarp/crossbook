# crossbook

Maintains live order books for a symbol on Binance USD-M Futures and Aster
and derives a cross-venue trading signal from them.

## Market

USD-M perpetual futures on both venues, one symbol (default `BTCUSDT`).

Binance
- stream `wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms`
- snapshot `https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000`

Aster
- stream `wss://fstream.asterdex.com/stream?streams=btcusdt@depth@100ms`
- snapshot `https://fapi.asterdex.com/fapi/v3/depth?symbol=BTCUSDT&limit=1000`

## Failure model

- Transient (network errors, timeouts, closes by the venue, stale socket, bad frames, 5xx,
  429/418 waiting out `Retry-After`): retried with backoff; the other feed is unaffected and the
  status line shows the wait.
- Permanent (400, 404, 451 from the snapshot or the handshake): the feed is `failed(...)` and the
  process exits with code 1, since the service needs both books and a live process would hide it.
- Panic in a feed task: exit code 1.

## Backoff

Retries wait 500 ms, 1 s, 2 s, ... up to 30 s, jittered down by up to half. Snapshot requests are
also at least 1 s apart and honour `Retry-After` (up to three days). Both reset after 60 s on one
anchor.
