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
- snapshot `https://fapi.asterdex.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000`
