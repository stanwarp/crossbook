use std::cell::Cell;
use std::future::{self, Future};
use std::ops::ControlFlow;
use std::pin::{Pin, pin};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Context;
use futures_util::StreamExt;
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{self, MissedTickBehavior, Sleep};
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
};
use tracing::{
    Instrument, debug, error, info, info_span, trace, warn,
};

use crate::backoff::{
    Backoff, LIVE_RESET_AFTER, RETRY_AFTER_CAP, RateLimit,
    ScheduledFetch, SnapshotLimiter,
};
use crate::book::{Book, DepthEvent, Snapshot};
use crate::config::FeedConfig;
use crate::protocol::{DepthSnapshot, Envelope};
use crate::status::{
    DisconnectReason, Failure, FeedCounters, FeedState, FeedStatus,
    ReanchorStatus, SnapshotActivity, millis,
};
use crate::sync::{Action, Input, ResetReason, State, SyncMachine};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const SNAPSHOT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const SNAPSHOT_HTTP_TIMEOUT: Duration = Duration::from_secs(5);
const WATCHDOG_TICK: Duration = Duration::from_millis(250);
const RECONNECT_BACKOFF_BASE: Duration = Duration::from_millis(500);
const RECONNECT_BACKOFF_CAP: Duration = Duration::from_secs(30);
const PREVIEW_BYTES: usize = 300;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type SnapshotFuture = Pin<
    Box<dyn Future<Output = Result<Snapshot, SnapshotError>> + Send>,
>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotError {
    #[error("{limit}")]
    RateLimited {
        limit: RateLimit,
        retry_after: Option<Duration>,
    },
    #[error("http {status}: {body}")]
    Http { status: StatusCode, body: String },
    #[error("transport")]
    Transport(#[from] reqwest::Error),
    #[error("decode")]
    Decode(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
enum FrameError<'a> {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unexpected stream {0}")]
    Stream(&'a str),
    #[error("unexpected event type {0}")]
    EventType(&'a str),
    #[error("unexpected symbol {0}")]
    Symbol(&'a str),
    #[error("not a text message")]
    NotText,
}

enum Link {
    Connecting {
        attempt: u32,
    },
    Reconnecting {
        reason: DisconnectReason,
        until: Instant,
    },
    Connected,
}

enum Exit {
    Shutdown,
    Reconnect(DisconnectReason),
    Failed(Failure),
}

enum ConnectError {
    Rejected { status: StatusCode, body: String },
    Other(DisconnectReason),
}

struct InFlight {
    future: SnapshotFuture,
    started_at: Instant,
}

#[derive(Clone, Copy)]
struct RateWindow {
    received: u64,
    at: Instant,
}

/// Fires at the scheduled send time. It is re-armed only when the schedule changes.
struct FetchTimer {
    sleep: Pin<Box<Sleep>>,
    armed_for: Option<Instant>,
}

impl FetchTimer {
    fn new() -> Self {
        FetchTimer {
            sleep: Box::pin(time::sleep(Duration::ZERO)),
            armed_for: None,
        }
    }

    fn arm(&mut self, due: Option<Instant>) {
        if due != self.armed_for {
            self.armed_for = due;
            if let Some(at) = due {
                self.sleep
                    .as_mut()
                    .reset(time::Instant::from_std(at));
            }
        }
    }
}

pub(crate) struct Feed {
    config: FeedConfig,
    http: reqwest::Client,
    machine: SyncMachine,
    limiter: SnapshotLimiter,
    reconnect: Backoff,
    counters: FeedCounters,
    link: Link,
    pending_fetch: Option<ScheduledFetch>,
    inflight: Option<InFlight>,
    first_frame_logged: bool,
    /// The anchor whose stable stretch already reset the backoffs.
    settled_anchor: Option<Instant>,
    rate: Cell<RateWindow>,
    status_tx: watch::Sender<Option<FeedStatus>>,
    shutdown: watch::Receiver<bool>,
}

impl Feed {
    pub(crate) fn new(
        config: FeedConfig,
        status_tx: watch::Sender<Option<FeedStatus>>,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .connect_timeout(SNAPSHOT_CONNECT_TIMEOUT)
            .timeout(SNAPSHOT_HTTP_TIMEOUT)
            .build()
            .with_context(|| {
                format!("http client for {}", config.name)
            })?;
        let now = Instant::now();
        let settings = config.settings;
        Ok(Feed {
            machine: SyncMachine::new(
                settings.k_levels,
                settings.stale_after,
                settings.reanchor_margin_percent,
                now,
            ),
            config,
            http,
            limiter: SnapshotLimiter::default(),
            reconnect: Backoff::new(
                RECONNECT_BACKOFF_BASE,
                RECONNECT_BACKOFF_CAP,
            ),
            counters: FeedCounters::default(),
            link: Link::Connecting { attempt: 1 },
            pending_fetch: None,
            inflight: None,
            first_frame_logged: false,
            settled_anchor: None,
            rate: Cell::new(RateWindow {
                received: 0,
                at: now,
            }),
            status_tx,
            shutdown,
        })
    }

    /// Returns at shutdown. An earlier return means the feed is beyond saving, and main treats
    /// that as fatal.
    pub(crate) async fn run(self) -> FeedStatus {
        let span = info_span!("feed", feed = self.config.name);
        self.run_connections().instrument(span).await
    }

    async fn run_connections(mut self) -> FeedStatus {
        let mut connected_before = false;
        loop {
            self.link = Link::Connecting {
                attempt: self.reconnect.failures() + 1,
            };
            let connected = {
                let mut status_tick = time::interval(
                    self.config.settings.status_interval,
                );
                let mut connecting = pin!(connect(&self.config));
                loop {
                    tokio::select! {
                        result = &mut connecting => break result,
                        _ = status_tick.tick() => self.publish_status(Instant::now()),
                        // If main is gone the channel errors, and that is a shutdown too.
                        _ = self.shutdown.changed() => return self.final_status(),
                    }
                }
            };
            let exit = match connected {
                Err(ConnectError::Rejected { status, body })
                    if is_configuration_error(status) =>
                {
                    Exit::Failed(Failure {
                        status: status.as_u16(),
                        body,
                    })
                }
                Err(ConnectError::Rejected { status, .. }) => {
                    Exit::Reconnect(
                        DisconnectReason::HandshakeRejected {
                            status: status.as_u16(),
                        },
                    )
                }
                Err(ConnectError::Other(reason)) => {
                    Exit::Reconnect(reason)
                }
                Ok(socket) => {
                    if connected_before {
                        self.counters.reconnects += 1;
                    }
                    connected_before = true;
                    self.run_connection(socket).await
                }
            };
            let reason = match exit {
                Exit::Shutdown => return self.final_status(),
                Exit::Failed(failure) => {
                    error!(%failure, "feed failed, giving up");
                    return self.status_with(
                        Instant::now(),
                        FeedState::Failed(failure),
                    );
                }
                Exit::Reconnect(reason) => reason,
            };
            self.machine.disconnected();
            self.pending_fetch = None;
            self.inflight = None;
            let backoff = self.reconnect.next_delay();
            let delay = match reason {
                DisconnectReason::RateLimited { wait, .. } => {
                    backoff.max(wait)
                }
                _ => backoff,
            };
            warn!(
                %reason,
                delay_ms = delay.as_millis(),
                failures = self.reconnect.failures(),
                "reconnecting"
            );
            if self
                .wait_before_reconnect(reason, delay)
                .await
                .is_break()
            {
                return self.final_status();
            }
        }
    }

    async fn wait_before_reconnect(
        &mut self,
        reason: DisconnectReason,
        delay: Duration,
    ) -> ControlFlow<()> {
        let until = Instant::now() + delay;
        self.link = Link::Reconnecting { reason, until };
        let mut status_tick =
            time::interval(self.config.settings.status_interval);
        loop {
            tokio::select! {
                () = time::sleep_until(time::Instant::from_std(until)) => return ControlFlow::Continue(()),
                _ = status_tick.tick() => self.publish_status(Instant::now()),
                // If main is gone the channel errors, and that is a shutdown too.
                _ = self.shutdown.changed() => return ControlFlow::Break(()),
            }
        }
    }

    async fn run_connection(&mut self, mut socket: Socket) -> Exit {
        self.link = Link::Connected;
        self.first_frame_logged = false;
        let now = Instant::now();
        let action = self.machine.connected(now);
        if let ControlFlow::Break(exit) = self.perform(action, now) {
            return exit;
        }
        let mut watchdog = time::interval(WATCHDOG_TICK);
        watchdog.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut status_tick =
            time::interval(self.config.settings.status_interval);
        status_tick
            .set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut fetch_timer = FetchTimer::new();
        loop {
            fetch_timer
                .arm(self.pending_fetch.map(|fetch| fetch.send_at));
            let flow = tokio::select! {
                message = socket.next() => self.on_message(message, Instant::now()),
                (result, started_at) = await_inflight(&mut self.inflight) => {
                    self.inflight = None;
                    let now = Instant::now();
                    self.on_snapshot_result(result, now.saturating_duration_since(started_at), now)
                }
                () = &mut fetch_timer.sleep, if fetch_timer.armed_for.is_some() => {
                    self.start_fetch(Instant::now());
                    ControlFlow::Continue(())
                }
                _ = watchdog.tick() => self.on_watchdog(Instant::now()),
                _ = status_tick.tick() => {
                    self.publish_status(Instant::now());
                    ControlFlow::Continue(())
                }
                // If main is gone the channel errors, and that is a shutdown too.
                _ = self.shutdown.changed() => ControlFlow::Break(Exit::Shutdown),
            };
            if let ControlFlow::Break(exit) = flow {
                close_socket(&mut socket).await;
                return exit;
            }
        }
    }

    fn on_message(
        &mut self,
        message: Option<Result<Message, tungstenite::Error>>,
        now: Instant,
    ) -> ControlFlow<Exit> {
        match message {
            Some(Ok(Message::Text(text))) => {
                if !self.first_frame_logged {
                    self.first_frame_logged = true;
                    debug!(
                        frame = preview(text.as_str()),
                        "first frame of this connection"
                    );
                }
                let action = self.on_frame(text.as_str(), now);
                self.perform(action, now)
            }
            Some(Ok(Message::Ping(payload))) => {
                self.counters.pings += 1;
                debug!(
                    bytes = payload.len(),
                    "ping received, pong queued by tungstenite"
                );
                ControlFlow::Continue(())
            }
            Some(Ok(Message::Pong(_))) => {
                debug!("pong received");
                ControlFlow::Continue(())
            }
            Some(Ok(Message::Close(frame))) => {
                let code =
                    frame.as_ref().map(|frame| u16::from(frame.code));
                warn!(?frame, "close frame received");
                ControlFlow::Break(Exit::Reconnect(
                    DisconnectReason::Closed { code },
                ))
            }
            Some(Ok(Message::Binary(_) | Message::Frame(_))) => {
                self.bad_frame("", &FrameError::NotText);
                ControlFlow::Continue(())
            }
            Some(Err(error)) => {
                warn!(%error, "socket error");
                ControlFlow::Break(Exit::Reconnect(
                    DisconnectReason::Transport,
                ))
            }
            None => {
                warn!("socket closed by peer");
                ControlFlow::Break(Exit::Reconnect(
                    DisconnectReason::Eof,
                ))
            }
        }
    }

    fn on_frame(&mut self, text: &str, now: Instant) -> Action {
        match parse_frame(text, &self.config) {
            Ok(event) => {
                self.counters.events_received += 1;
                self.machine.handle(Input::Event(event), now)
            }
            Err(error) => {
                self.bad_frame(text, &error);
                Action::None
            }
        }
    }

    fn bad_frame(&mut self, frame: &str, reason: &FrameError<'_>) {
        self.counters.bad_frames += 1;
        warn!(
            bad_frames = self.counters.bad_frames,
            %reason,
            frame = preview(frame),
            "bad frame skipped"
        );
    }

    fn perform(
        &mut self,
        action: Action,
        now: Instant,
    ) -> ControlFlow<Exit> {
        match action {
            Action::None => ControlFlow::Continue(()),
            Action::FetchSnapshot => {
                self.request_snapshot(now);
                ControlFlow::Continue(())
            }
            Action::ResetConnection {
                reason: ResetReason::Stale { silent_for },
            } => ControlFlow::Break(Exit::Reconnect(
                DisconnectReason::Stale { silent_for },
            )),
        }
    }

    fn request_snapshot(&mut self, now: Instant) {
        if self.inflight.is_some() || self.pending_fetch.is_some() {
            debug!("snapshot already requested");
            return;
        }
        let scheduled = self.limiter.schedule(now);
        match scheduled.cause {
            None => debug!(
                attempt = self.limiter.attempts() + 1,
                "snapshot request scheduled"
            ),
            Some(cause) => info!(
                %cause,
                delay_ms = scheduled.send_at.saturating_duration_since(now).as_millis(),
                attempt = self.limiter.attempts() + 1,
                "snapshot request held back"
            ),
        }
        self.pending_fetch = Some(scheduled);
    }

    fn start_fetch(&mut self, now: Instant) {
        self.pending_fetch = None;
        self.limiter.sent(now);
        debug!(url = %self.config.rest_url, "snapshot requested");
        self.inflight = Some(InFlight {
            future: fetch_snapshot(&self.http, &self.config.rest_url),
            started_at: now,
        });
    }

    fn on_snapshot_result(
        &mut self,
        result: Result<Snapshot, SnapshotError>,
        took: Duration,
        now: Instant,
    ) -> ControlFlow<Exit> {
        match result {
            Ok(snapshot) => {
                self.counters.snapshots_fetched += 1;
                info!(
                    last_update_id = snapshot.last_update_id,
                    bids = snapshot.bids.len(),
                    asks = snapshot.asks.len(),
                    took_ms = took.as_millis(),
                    "snapshot received"
                );
                let action = self
                    .machine
                    .handle(Input::Snapshot(snapshot), now);
                self.perform(action, now)
            }
            Err(SnapshotError::RateLimited {
                limit,
                retry_after,
            }) => {
                self.counters.snapshot_errors += 1;
                self.limiter.rate_limited(limit, retry_after, now);
                error!(%limit, ?retry_after, "snapshot request rate limited, backing off");
                self.perform(Action::FetchSnapshot, now)
            }
            Err(SnapshotError::Http { status, body })
                if is_configuration_error(status) =>
            {
                self.counters.snapshot_errors += 1;
                ControlFlow::Break(Exit::Failed(Failure {
                    status: status.as_u16(),
                    body,
                }))
            }
            Err(error) => {
                self.counters.snapshot_errors += 1;
                let error = anyhow::Error::from(error);
                warn!(
                    error = format_args!("{error:#}"),
                    took_ms = took.as_millis(),
                    "snapshot request failed"
                );
                self.perform(Action::FetchSnapshot, now)
            }
        }
    }

    fn on_watchdog(&mut self, now: Instant) -> ControlFlow<Exit> {
        if let State::Live { anchored_at, .. } = self.machine.state()
            && now.saturating_duration_since(*anchored_at)
                >= LIVE_RESET_AFTER
            && self.settled_anchor != Some(*anchored_at)
        {
            self.settle(*anchored_at);
        }
        let action = self.machine.handle(Input::Tick, now);
        self.perform(action, now)
    }

    // The backoffs are forgiven once per anchor, after a stable stretch. A connection that dies
    // right after coming up keeps escalating.
    fn settle(&mut self, anchored_at: Instant) {
        self.settled_anchor = Some(anchored_at);
        if self.reconnect.failures() > 0
            || self.limiter.attempts() > 0
        {
            self.reconnect.reset();
            self.limiter.reset();
            info!(
                live_s = LIVE_RESET_AFTER.as_secs(),
                "backoff counters reset"
            );
        }
    }

    fn publish_status(&self, now: Instant) {
        let state = self.feed_state(now);
        self.status_tx
            .send_replace(Some(self.status_with(now, state)));
    }

    fn final_status(&self) -> FeedStatus {
        let now = Instant::now();
        let state = self.feed_state(now);
        self.status_with(now, state)
    }

    fn status_with(
        &self,
        now: Instant,
        state: FeedState,
    ) -> FeedStatus {
        let window = self.rate.get();
        let elapsed_ms =
            millis(now.saturating_duration_since(window.at));
        let received = self
            .counters
            .events_received
            .saturating_sub(window.received);
        let events_per_sec = if elapsed_ms == 0 {
            0
        } else {
            received.saturating_mul(1000) / elapsed_ms
        };
        self.rate.set(RateWindow {
            received: self.counters.events_received,
            at: now,
        });
        let book = self.machine.book();
        FeedStatus {
            at: SystemTime::now(),
            name: self.config.name,
            state,
            best_bid: book.and_then(Book::best_bid),
            best_ask: book.and_then(Book::best_ask),
            bid_levels: book.map_or(0, |book| book.bids.len()),
            ask_levels: book.map_or(0, |book| book.asks.len()),
            band: book.map(|book| book.band),
            since_last_event: now.saturating_duration_since(
                self.machine.last_event_at(),
            ),
            events_per_sec,
            snapshot: self.snapshot_activity(now),
            sync: self.machine.counters(),
            feed: self.counters,
        }
    }

    fn feed_state(&self, now: Instant) -> FeedState {
        let buffered = self.machine.buffered();
        match self.link {
            Link::Connecting { attempt } => {
                FeedState::Connecting { attempt }
            }
            Link::Reconnecting { reason, until } => {
                FeedState::Reconnecting {
                    reason,
                    retry_in: until.saturating_duration_since(now),
                }
            }
            Link::Connected => match self.machine.state() {
                State::Buffering => FeedState::Buffering { buffered },
                State::Stitching { snapshot, .. } => {
                    FeedState::Stitching {
                        last_update_id: snapshot.last_update_id,
                        buffered,
                    }
                }
                State::Live { reanchor, .. } => FeedState::Live {
                    reanchor: reanchor.as_ref().map(|pending| {
                        ReanchorStatus {
                            pending_for: now
                                .saturating_duration_since(
                                    pending.requested_at,
                                ),
                            kept: pending.kept(),
                        }
                    }),
                },
                State::Resyncing { reason } => FeedState::Resyncing {
                    reason: *reason,
                    buffered,
                },
            },
        }
    }

    fn snapshot_activity(&self, now: Instant) -> SnapshotActivity {
        match (&self.inflight, self.pending_fetch) {
            (Some(inflight), _) => SnapshotActivity::InFlight {
                elapsed: now
                    .saturating_duration_since(inflight.started_at),
            },
            (None, Some(fetch)) => SnapshotActivity::Scheduled {
                due_in: fetch.send_at.saturating_duration_since(now),
                cause: fetch.cause,
            },
            (None, None) => SnapshotActivity::Idle,
        }
    }
}

async fn connect(
    config: &FeedConfig,
) -> Result<Socket, ConnectError> {
    info!(url = %config.ws_url, "connecting");
    match time::timeout(
        CONNECT_TIMEOUT,
        connect_async(config.ws_url.as_str()),
    )
    .await
    {
        Ok(Ok((socket, response))) => {
            info!(status = %response.status(), "connected");
            Ok(socket)
        }
        Ok(Err(tungstenite::Error::Http(response))) => {
            let status = response.status();
            // No body is normal for a rejected handshake.
            let body = response
                .body()
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_default();
            error!(%status, body = %body, "handshake rejected");
            match RateLimit::from_status(status.as_u16()) {
                // Without a header a ban is waited out for the full reconnect cap.
                Some(limit) => Err(ConnectError::Other(
                    DisconnectReason::RateLimited {
                        limit,
                        wait: retry_after(response.headers())
                            .unwrap_or(RECONNECT_BACKOFF_CAP)
                            .min(RETRY_AFTER_CAP),
                    },
                )),
                None => Err(ConnectError::Rejected {
                    status,
                    body: preview(&body).to_owned(),
                }),
            }
        }
        Ok(Err(error)) => {
            warn!(%error, "connect failed");
            Err(ConnectError::Other(DisconnectReason::ConnectFailed))
        }
        Err(_elapsed) => {
            warn!(
                timeout_ms = CONNECT_TIMEOUT.as_millis(),
                "connect timed out"
            );
            Err(ConnectError::Other(DisconnectReason::ConnectTimeout))
        }
    }
}

/// A 400, 404 or 451 means a wrong symbol, a wrong path or a blocked region, retry no good
fn is_configuration_error(status: StatusCode) -> bool {
    matches!(status.as_u16(), 400 | 404 | 451)
}

fn parse_frame<'a>(
    text: &'a str,
    config: &FeedConfig,
) -> Result<DepthEvent, FrameError<'a>> {
    let envelope: Envelope<'a> = serde_json::from_str(text)?;
    if envelope.stream != config.stream {
        return Err(FrameError::Stream(envelope.stream));
    }
    let update = envelope.data;
    if update.event_type != "depthUpdate" {
        return Err(FrameError::EventType(update.event_type));
    }
    if update.symbol != config.symbol {
        return Err(FrameError::Symbol(update.symbol));
    }
    trace!(
        first_update_id = update.first_update_id,
        final_update_id = update.final_update_id,
        prev_final_update_id = update.prev_final_update_id,
        event_time_ms = update.event_time_ms,
        bids = update.bids.len(),
        asks = update.asks.len(),
        "depth update"
    );
    Ok(DepthEvent::from(update))
}

async fn await_inflight(
    slot: &mut Option<InFlight>,
) -> (Result<Snapshot, SnapshotError>, Instant) {
    match slot {
        Some(inflight) => {
            let result = inflight.future.as_mut().await;
            (result, inflight.started_at)
        }
        None => future::pending().await,
    }
}

async fn close_socket(socket: &mut Socket) {
    match time::timeout(CLOSE_TIMEOUT, socket.close(None)).await {
        Ok(Ok(())) => debug!("socket closed"),
        Ok(Err(error)) => debug!(%error, "close failed"),
        Err(_elapsed) => debug!("close timed out"),
    }
}

fn fetch_snapshot(
    http: &reqwest::Client,
    url: &str,
) -> SnapshotFuture {
    let request = http.get(url);
    Box::pin(async move {
        let response = request.send().await?;
        let status = response.status();
        if let Some(limit) = RateLimit::from_status(status.as_u16()) {
            return Err(SnapshotError::RateLimited {
                limit,
                retry_after: retry_after(response.headers()),
            });
        }
        let body = response.bytes().await?;
        if !status.is_success() {
            let preview = &body[..body.len().min(PREVIEW_BYTES)];
            return Err(SnapshotError::Http {
                status,
                body: String::from_utf8_lossy(preview).into_owned(),
            });
        }
        let snapshot: DepthSnapshot = serde_json::from_slice(&body)?;
        Ok(Snapshot::from(snapshot))
    })
}

/// Binance sends `Retry-After` in whole seconds. Anything else is ignored.
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds: u64 =
        headers.get(RETRY_AFTER)?.to_str().ok()?.parse().ok()?;
    Some(Duration::from_secs(seconds))
}

fn preview(text: &str) -> &str {
    &text[..text.floor_char_boundary(PREVIEW_BYTES)]
}
