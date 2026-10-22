mod backoff;
mod book;
mod config;
mod feed;
mod fixed;
mod protocol;
mod status;
mod sync;

use std::io::{self, IsTerminal, Write};
use std::time::Duration;

use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use tokio::signal;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

use crate::config::{FeedConfig, Settings, Venue};
use crate::feed::Feed;
use crate::status::FeedStatus;

const DEPTH_LIMITS: [u16; 7] = [5, 10, 20, 50, 100, 500, 1000];

#[derive(Parser)]
#[command(
    name = "crossbook",
    about = "Live order books from Binance USD-M futures and Aster"
)]
struct Cli {
    #[arg(long, default_value = "BTCUSDT", value_parser = symbol_arg)]
    symbol: String,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "binance,aster"
    )]
    feeds: Vec<Venue>,
    /// Exit after this many seconds and print the final counters. Without it, run until Ctrl-C.
    #[arg(long)]
    duration: Option<u64>,
    /// Levels per side the consumer needs. Fewer than this triggers a resync.
    #[arg(long, default_value_t = 20, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..))]
    k_levels: usize,
    /// Seconds without any event after which the socket is reconnected.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    stale_after: u64,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
    status_interval: u64,
    /// Levels per side requested in the REST snapshot: 5, 10, 20, 50, 100, 500 or 1000.
    #[arg(long, default_value_t = 1000, value_parser = depth_limit_arg)]
    depth_limit: u16,
    /// Fetch a fresh snapshot while live once the best price is within this percent of its
    /// band edge. The --k-levels floor still applies.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=50))]
    reanchor_margin: u64,
    /// Log lines on stderr as readable text or as one JSON object per line.
    #[arg(long, value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,
}

#[derive(Clone, Copy, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

fn symbol_arg(text: &str) -> Result<String, String> {
    if text.is_empty()
        || !text.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(
            "symbol must be letters and digits only".to_owned()
        );
    }
    Ok(text.to_ascii_uppercase())
}

fn depth_limit_arg(text: &str) -> Result<u16, String> {
    let limit: u16 =
        text.parse().map_err(|error| format!("{error}"))?;
    if DEPTH_LIMITS.contains(&limit) {
        Ok(limit)
    } else {
        Err(format!("must be one of {DEPTH_LIMITS:?}"))
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
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal());
    match cli.log_format {
        LogFormat::Text => logs.init(),
        LogFormat::Json => logs.json().init(),
    }
    for (index, venue) in cli.feeds.iter().enumerate() {
        if cli.feeds[..index].contains(venue) {
            bail!("feed {} given twice", venue.name());
        }
    }
    if cli.k_levels > usize::from(cli.depth_limit) / 2 {
        bail!(
            "--k-levels {} must not exceed half of --depth-limit {}: the first deletions would exhaust every snapshot",
            cli.k_levels,
            cli.depth_limit
        );
    }
    let settings = Settings {
        depth_limit: cli.depth_limit,
        k_levels: cli.k_levels,
        stale_after: Duration::from_secs(cli.stale_after),
        status_interval: Duration::from_secs(cli.status_interval),
        reanchor_margin_percent: cli.reanchor_margin,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut feeds = JoinSet::new();
    let mut printers = JoinSet::new();
    for venue in cli.feeds {
        let config = FeedConfig::new(venue, &cli.symbol, settings);
        let (status_tx, status_rx) = watch::channel(None);
        feeds.spawn(
            Feed::new(config, status_tx, shutdown_rx.clone())?.run(),
        );
        printers.spawn(print_status(status_rx));
    }
    // A feed task returns only at shutdown. One that returns earlier has failed or panicked.
    let early_exit = tokio::select! {
        ended = wait_for_end(cli.duration) => {
            ended?;
            None
        }
        finished = feeds.join_next() => finished,
    };
    shutdown_tx.send_replace(true);
    if let Some(result) = &early_exit {
        print_final(result);
    }
    while let Some(result) = feeds.join_next().await {
        print_final(&result);
    }
    printers.join_all().await;
    match early_exit {
        None => Ok(()),
        Some(Ok(status)) => bail!(
            "feed {} stopped before shutdown: {}",
            status.name,
            status.state
        ),
        Some(Err(error)) => {
            bail!("feed task ended abnormally: {error}")
        }
    }
}

fn print_final(result: &Result<FeedStatus, JoinError>) {
    match result {
        Ok(status) => {
            if writeln!(io::stdout(), "final {status}").is_err() {
                error!(
                    feed = status.name,
                    "stdout closed, final status not printed"
                );
            }
        }
        Err(error) => error!(%error, "feed task ended abnormally"),
    }
}

async fn print_status(
    mut status: watch::Receiver<Option<FeedStatus>>,
) {
    let mut out = io::stdout();
    while status.changed().await.is_ok() {
        let line = status
            .borrow_and_update()
            .as_ref()
            .map(ToString::to_string);
        // A closed pipe, as with `| head`, means nobody reads the status any more.
        if let Some(line) = line
            && writeln!(out, "{line}").is_err()
        {
            return;
        }
    }
}

async fn wait_for_end(duration: Option<u64>) -> anyhow::Result<()> {
    let Some(secs) = duration else {
        signal::ctrl_c().await.context("ctrl-c handler")?;
        info!("interrupted, shutting down");
        return Ok(());
    };
    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(secs)) => info!(secs, "duration elapsed, shutting down"),
        interrupted = signal::ctrl_c() => {
            interrupted.context("ctrl-c handler")?;
            info!("interrupted, shutting down");
        }
    }
    Ok(())
}
