use std::future;
use std::io::{self, IsTerminal, Write};
use std::time::Duration;

use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};
use tokio::{signal as os_signal, time};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

use crossbook::config::{FeedConfig, Settings, Venue};
use crossbook::feed::Feed;
use crossbook::signal::{self, Pace, SignalConfig, SignalSummary};
use crossbook::status::FeedStatus;
use crossbook::store::BookReader;

const DEPTH_LIMITS: [u16; 7] = [5, 10, 20, 50, 100, 500, 1000];

/// Live order books from Binance USD-M futures and Aster
#[derive(Parser)]
struct Cli {
    #[arg(long, default_value = "BTCUSDT", value_parser = symbol_arg)]
    symbol: String,
    #[arg(long, value_delimiter = ',', default_value = "binance,aster")]
    feeds: Vec<Venue>,
    /// Exit after this many seconds and print the final counters. Without it, run until Ctrl-C.
    #[arg(long)]
    duration: Option<u64>,
    /// Log lines on stderr as readable text or as one JSON object per line.
    #[arg(long, value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,
    #[command(flatten)]
    feed: FeedArgs,
    #[command(flatten)]
    signal: SignalArgs,
}

#[derive(clap::Args)]
#[command(next_help_heading = "Feeds")]
struct FeedArgs {
    /// Levels per side the consumer needs. Fewer than this triggers a resync.
    #[arg(long, default_value_t = 20, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..))]
    k_levels: usize,
    /// Levels per side requested in the REST snapshot: 5, 10, 20, 50, 100, 500 or 1000.
    #[arg(long, default_value_t = 1000, value_parser = depth_limit_arg)]
    depth_limit: u16,
    /// Seconds without any event after which the socket is reconnected.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    stale_after: u64,
    /// Fetch a fresh snapshot while live once the best price is within this percent of its
    /// band edge. The --k-levels floor still applies as hard resync.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=50))]
    reanchor_margin: u64,
    /// Seconds between status lines per feed.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..))]
    status_interval: u64,
}

#[derive(clap::Args)]
#[command(next_help_heading = "Signal")]
struct SignalArgs {
    /// How often the signal evaluates both books.
    #[arg(long, default_value_t = 250, value_parser = clap::value_parser!(u64).range(1..))]
    signal_interval_ms: u64,
    /// The signal line is printed at least this often even when nothing changed.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    signal_heartbeat: u64,
    /// A book whose last event is older than this holds the signal.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u64).range(1..))]
    max_age_ms: u64,
    /// A book whose last event reached us this long after the venue stamped it holds the signal.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(i64).range(1..))]
    max_latency_ms: i64,
    /// Cross-venue spread, in basis points, that counts as much as a fully one-sided book.
    #[arg(long, default_value_t = 2.0, value_parser = positive_arg)]
    spread_scale_bps: f64,
    /// Score beyond which the signal leans long or short.
    #[arg(long, default_value_t = 0.5, value_parser = non_negative_arg)]
    threshold: f64,
}

#[derive(Clone, Copy, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

fn symbol_arg(text: &str) -> Result<String, String> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err("symbol must be letters and digits only".to_owned());
    }
    Ok(text.to_ascii_uppercase())
}

fn depth_limit_arg(text: &str) -> Result<u16, String> {
    let limit: u16 = text.parse().map_err(|error| format!("{error}"))?;
    if DEPTH_LIMITS.contains(&limit) {
        Ok(limit)
    } else {
        Err(format!("must be one of {DEPTH_LIMITS:?}"))
    }
}

fn positive_arg(text: &str) -> Result<f64, String> {
    let value: f64 = text.parse().map_err(|error| format!("{error}"))?;
    if value > 0.0 && value.is_finite() {
        Ok(value)
    } else {
        Err("must be a positive number".to_owned())
    }
}

fn non_negative_arg(text: &str) -> Result<f64, String> {
    let value: f64 = text.parse().map_err(|error| format!("{error}"))?;
    if value >= 0.0 && value.is_finite() {
        Ok(value)
    } else {
        Err("must be zero or a positive number".to_owned())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let logs = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .with_ansi(io::stderr().is_terminal());
    match cli.log_format {
        LogFormat::Text => logs.init(),
        LogFormat::Json => logs.json().init(),
    }
    for (index, venue) in cli.feeds.iter().enumerate() {
        if cli.feeds[..index].contains(venue) {
            bail!("feed {} given twice", venue.name());
        }
    }
    if cli.feed.k_levels > usize::from(cli.feed.depth_limit) / 2 {
        bail!(
            "--k-levels {} must not exceed half of --depth-limit {}: the first deletions would exhaust every snapshot",
            cli.feed.k_levels,
            cli.feed.depth_limit
        );
    }
    let settings = Settings {
        depth_limit: cli.feed.depth_limit,
        k_levels: cli.feed.k_levels,
        stale_after: Duration::from_secs(cli.feed.stale_after),
        status_interval: Duration::from_secs(cli.feed.status_interval),
        reanchor_margin_percent: cli.feed.reanchor_margin,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut printers = JoinSet::new();
    let mut readers = Vec::new();
    for &venue in &cli.feeds {
        let config = FeedConfig::new(venue, &cli.symbol, settings);
        let (status_tx, status_rx) = watch::channel(None);
        let (feed, reader) = Feed::new(config, status_tx, shutdown_rx.clone())?;
        readers.push(reader);
        tasks.spawn(async move { Done::Feed(Box::new(feed.run().await)) });
        printers.spawn(print_status(status_rx));
    }
    if let Ok([a, b]) = <[BookReader; 2]>::try_from(readers) {
        let cfg = SignalConfig {
            max_age: Duration::from_millis(cli.signal.max_age_ms),
            max_latency_ms: cli.signal.max_latency_ms,
            spread_scale_bps: cli.signal.spread_scale_bps,
            threshold: cli.signal.threshold,
        };
        let pace = Pace {
            interval: Duration::from_millis(cli.signal.signal_interval_ms),
            heartbeat: Duration::from_secs(cli.signal.signal_heartbeat),
        };
        tasks.spawn(async move { Done::Signal(signal::run(a, b, cfg, pace, shutdown_rx).await) });
    } else {
        info!("signal disabled with one feed");
    }
    let early = tokio::select! {
        ended = wait_for_end(cli.duration) => {
            ended?;
            None
        }
        finished = tasks.join_next() => finished,
    };
    shutdown_tx.send_replace(true);
    let failed = early.is_some();
    if let Some(result) = early {
        print_final(&result);
        ended_early(&result);
    }
    while let Some(result) = tasks.join_next().await {
        print_final(&result);
    }
    printers.join_all().await;
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

enum Done {
    Feed(Box<FeedStatus>),
    Signal(SignalSummary),
}

fn ended_early(result: &Result<Done, JoinError>) {
    match result {
        Ok(Done::Feed(status)) => error!(
            feed = status.name,
            state = %status.state,
            "feed stopped before shutdown"
        ),
        Ok(Done::Signal(_)) => error!("signal task stopped before shutdown"),
        // Logged by print_final.
        Err(_) => {}
    }
}

fn print_final(result: &Result<Done, JoinError>) {
    let line = match result {
        Ok(Done::Feed(status)) => format!("final {status:#}\n"),
        Ok(Done::Signal(summary)) => format!("final signal {summary}\n"),
        Err(error) => {
            error!(%error, "task ended abnormally");
            return;
        }
    };
    if io::stdout().write_all(line.as_bytes()).is_err() {
        error!("stdout closed, final line not printed");
    }
}

async fn print_status(mut status: watch::Receiver<Option<FeedStatus>>) {
    let mut out = tokio::io::stdout();
    while status.changed().await.is_ok() {
        let Some(line) = status
            .borrow_and_update()
            .as_ref()
            .map(|status| format!("{status}\n"))
        else {
            continue;
        };
        // A closed pipe means nobody reads the status any more.
        if out.write_all(line.as_bytes()).await.is_err() {
            return;
        }
    }
}

async fn wait_for_end(duration: Option<u64>) -> anyhow::Result<()> {
    tokio::select! {
        () = deadline(duration) => info!("duration elapsed, shutting down"),
        interrupted = os_signal::ctrl_c() => {
            interrupted.context("ctrl-c handler")?;
            info!("interrupted, shutting down");
        }
    }
    Ok(())
}

async fn deadline(duration: Option<u64>) {
    match duration {
        Some(secs) => time::sleep(Duration::from_secs(secs)).await,
        None => future::pending().await,
    }
}
