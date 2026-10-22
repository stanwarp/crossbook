use std::time::Duration;

use clap::ValueEnum;

#[derive(Clone, Copy, PartialEq, ValueEnum)]
pub(crate) enum Venue {
    Binance,
    Aster,
}

impl Venue {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Venue::Binance => "binance",
            Venue::Aster => "aster",
        }
    }

    fn ws_base(self) -> &'static str {
        match self {
            Venue::Binance => "wss://fstream.binance.com",
            Venue::Aster => "wss://fstream.asterdex.com",
        }
    }

    fn depth_endpoint(self) -> &'static str {
        match self {
            Venue::Binance => {
                "https://fapi.binance.com/fapi/v1/depth"
            }
            Venue::Aster => "https://fapi.asterdex.com/fapi/v3/depth",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Settings {
    pub(crate) depth_limit: u16,
    pub(crate) k_levels: usize,
    pub(crate) stale_after: Duration,
    pub(crate) status_interval: Duration,
    pub(crate) reanchor_margin_percent: u64,
}

pub(crate) struct FeedConfig {
    pub(crate) name: &'static str,
    pub(crate) ws_url: String,
    pub(crate) stream: String,
    pub(crate) rest_url: String,
    pub(crate) symbol: String,
    pub(crate) settings: Settings,
}

impl FeedConfig {
    pub(crate) fn new(
        venue: Venue,
        symbol: &str,
        settings: Settings,
    ) -> Self {
        let stream =
            format!("{}@depth@100ms", symbol.to_ascii_lowercase());
        FeedConfig {
            name: venue.name(),
            ws_url: format!(
                "{}/stream?streams={stream}",
                venue.ws_base()
            ),
            rest_url: format!(
                "{}?symbol={symbol}&limit={}",
                venue.depth_endpoint(),
                settings.depth_limit
            ),
            stream,
            symbol: symbol.to_owned(),
            settings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_follow_venue_symbol_and_depth_limit() {
        let settings = Settings {
            depth_limit: 1000,
            k_levels: 20,
            stale_after: Duration::from_secs(5),
            status_interval: Duration::from_secs(1),
            reanchor_margin_percent: 10,
        };
        let config =
            FeedConfig::new(Venue::Aster, "ETHUSDT", settings);
        assert_eq!(config.name, "aster");
        assert_eq!(config.symbol, "ETHUSDT");
        assert_eq!(config.stream, "ethusdt@depth@100ms");
        assert_eq!(
            config.ws_url,
            "wss://fstream.asterdex.com/stream?streams=ethusdt@depth@100ms"
        );
        assert_eq!(
            config.rest_url,
            "https://fapi.asterdex.com/fapi/v3/depth?symbol=ETHUSDT&limit=1000"
        );
    }
}
