use std::future;
use std::ops::ControlFlow;
use std::pin::{Pin, pin};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Context;
use futures_util::StreamExt;
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{self, MissedTickBehavior};
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{Instrument, debug, error, info, info_span, trace, warn};

use crate::backoff::{BACKOFF_CAP, RateLimit, ScheduledFetch, SnapshotLimiter, delay_after};
use crate::book::{Book, DepthEvent, Snapshot};
use crate::config::FeedConfig;
use crate::status::{
    DisconnectReason, Failure, FeedCounters, FeedState, FeedStatus, ReanchorStatus,
    SnapshotActivity, latency_ms,
};
use crate::store::{BookReader, BookWriter, book_store};
use crate::sync::{Action, Input, State, SyncMachine};
use crate::view::{BookView, NotLiveReason, ViewState};
use crate::wire::Envelope;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const SNAPSHOT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const SNAPSHOT_HTTP_TIMEOUT: Duration = Duration::from_secs(5);
const WATCHDOG_TICK: Duration = Duration::from_millis(250);
/// Once a connection has been up and live this long, the reconnect backoff starts from scratch;
/// a connection that dies right after coming up keeps escalating.
const LIVE_RESET_AFTER: Duration = Duration::from_secs(60);
const PREVIEW_BYTES: usize = 300;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type SnapshotFuture = Pin<Box<dyn Future<Output = Result<Snapshot, SnapshotError>> + Send>>;

#[derive(Debug, thiserror::Error)]
enum SnapshotError {
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
    Connected {
        since: Instant,
    },
    Failed(Failure),
}

/// The link already shows the outcome; a reconnect waits until then.
enum Exit {
    Shutdown,
    Reconnect { until: Instant },
    Failed,
}

enum ConnectError {
    Failed(Failure),
    Lost(DisconnectReason),
}

/// A new view goes out only when this changes, so a watchdog tick or an ignored snapshot
/// publishes nothing.
#[derive(PartialEq)]
enum Published {
    Live {
        last_update_id: u64,
        anchored_at: Instant,
    },
    NotLive(NotLiveReason),
}

enum Fetch {
    Idle,
    Scheduled(ScheduledFetch),
    InFlight {
        reply: SnapshotFuture,
        started_at: Instant,
    },
}

pub struct Feed {
    config: FeedConfig,
    http: reqwest::Client,
    machine: SyncMachine,
    limiter: SnapshotLimiter,
    reconnect_failures: u32,
    counters: FeedCounters,
    link: Link,
    fetch: Fetch,
    latency_ms: Option<i64>,
    views: BookWriter,
    published: Published,
    crossed: bool,
    status_tx: watch::Sender<Option<FeedStatus>>,
    shutdown: watch::Receiver<bool>,
}

impl Feed {
    /// The returned reader sees this feed's views; the feed keeps the only writer.
    ///
    /// # Errors
    /// Only when the HTTP client cannot be built.
    pub fn new(
        config: FeedConfig,
        status_tx: watch::Sender<Option<FeedStatus>>,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<(Self, BookReader)> {
        // No connection pool: a kept-alive connection can die silently when the network changes,
        // and every retry would then hang on it. Snapshots are rare enough for a fresh handshake.
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .connect_timeout(SNAPSHOT_CONNECT_TIMEOUT)
            .timeout(SNAPSHOT_HTTP_TIMEOUT)
            .build()
            .with_context(|| format!("http client for {}", config.name))?;
        let now = Instant::now();
        let settings = config.settings;
        let (views, reader) =
            book_store(BookView::empty(config.name, NotLiveReason::Connecting, now));
        let feed = Feed {
            machine: SyncMachine::new(
                settings.k_levels,
                settings.stale_after,
                settings.reanchor_margin_percent,
                now,
            ),
            config,
            http,
            limiter: SnapshotLimiter::default(),
            reconnect_failures: 0,
            counters: FeedCounters::default(),
            link: Link::Connecting { attempt: 1 },
            fetch: Fetch::Idle,
            latency_ms: None,
            views,
            published: Published::NotLive(NotLiveReason::Connecting),
            crossed: false,
            status_tx,
            shutdown,
        };
        Ok((feed, reader))
    }

    /// Returns at shutdown. An earlier return means the feed is beyond saving, and main treats
    /// that as fatal.
    pub async fn run(self) -> FeedStatus {
        let span = info_span!("feed", feed = self.config.name);
        self.run_connections().instrument(span).await
    }

    async fn run_connections(mut self) -> FeedStatus {
        let mut connected_before = false;
        loop {
            self.set_link(Link::Connecting {
                attempt: self.reconnect_failures + 1,
            });
            let connected = {
                let mut status_tick = time::interval(self.config.settings.status_interval);
                let mut connecting = pin!(connect(&self.config));
                loop {
                    tokio::select! {
                        result = &mut connecting => break result,
                        _ = status_tick.tick() => self.publish_status(),
                        _ = self.shutdown.changed() => return self.status(),
                    }
                }
            };
            let exit = match connected {
                Err(ConnectError::Failed(failure)) => self.failed(failure),
                Err(ConnectError::Lost(reason)) => self.disconnected(reason, Instant::now()),
                Ok(socket) => {
                    if connected_before {
                        self.counters.reconnects += 1;
                    }
                    connected_before = true;
                    self.run_connection(socket).await
                }
            };
            let until = match exit {
                Exit::Shutdown | Exit::Failed => {
                    return self.status();
                }
                Exit::Reconnect { until } => until,
            };
            if self.wait_before_reconnect(until).await.is_break() {
                return self.status();
            }
        }
    }

    /// Decides the wait and takes the book away from the readers right here, before the socket
    /// is closed: the close handshake can take a while on a dead link.
    fn disconnected(&mut self, reason: DisconnectReason, now: Instant) -> Exit {
        let backoff = delay_after(self.reconnect_failures);
        self.reconnect_failures = self.reconnect_failures.saturating_add(1);
        let delay = match reason {
            DisconnectReason::RateLimited { wait, .. } => backoff.max(wait),
            _ => backoff,
        };
        warn!(
            %reason,
            delay_ms = delay.as_millis(),
            failures = self.reconnect_failures,
            "reconnecting"
        );
        self.machine.reset();
        self.fetch = Fetch::Idle;
        let until = now + delay;
        self.set_link(Link::Reconnecting { reason, until });
        Exit::Reconnect { until }
    }

    /// Like a disconnect, but final: the readers learn before the socket is closed.
    fn failed(&mut self, failure: Failure) -> Exit {
        error!(%failure, "feed failed, giving up");
        self.machine.reset();
        self.fetch = Fetch::Idle;
        self.set_link(Link::Failed(failure));
        Exit::Failed
    }

    async fn wait_before_reconnect(&mut self, until: Instant) -> ControlFlow<()> {
        let mut status_tick = time::interval(self.config.settings.status_interval);
        loop {
            tokio::select! {
                () = time::sleep_until(time::Instant::from_std(until)) => return ControlFlow::Continue(()),
                _ = status_tick.tick() => self.publish_status(),
                _ = self.shutdown.changed() => return ControlFlow::Break(()),
            }
        }
    }

    fn on_connected(&mut self, now: Instant) -> ControlFlow<Exit> {
        let action = self.machine.connected(now);
        self.set_link(Link::Connected { since: now });
        self.perform(action, now)
    }

    async fn run_connection(&mut self, mut socket: Socket) -> Exit {
        if let ControlFlow::Break(exit) = self.on_connected(Instant::now()) {
            return exit;
        }
        let mut watchdog = time::interval(WATCHDOG_TICK);
        watchdog.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut status_tick = time::interval(self.config.settings.status_interval);
        status_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            let send_at = match &self.fetch {
                Fetch::Scheduled(scheduled) => Some(scheduled.send_at),
                Fetch::Idle | Fetch::InFlight { .. } => None,
            };
            let flow = tokio::select! {
                message = socket.next() => self.on_message(message, Instant::now()),
                (result, started_at) = await_reply(&mut self.fetch) => {
                    self.fetch = Fetch::Idle;
                    let now = Instant::now();
                    self.on_snapshot_result(result, now.saturating_duration_since(started_at), now)
                }
                () = send_time(send_at) => {
                    self.start_fetch(Instant::now());
                    ControlFlow::Continue(())
                }
                _ = watchdog.tick() => self.on_watchdog(Instant::now()),
                _ = status_tick.tick() => {
                    self.publish_status();
                    ControlFlow::Continue(())
                }
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
            Some(Ok(Message::Text(text))) => self.on_frame(text.as_str(), now),
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
                let code = frame.as_ref().map(|frame| u16::from(frame.code));
                warn!(?frame, "close frame received");
                ControlFlow::Break(self.disconnected(DisconnectReason::Closed { code }, now))
            }
            Some(Ok(Message::Binary(_) | Message::Frame(_))) => {
                self.bad_frame("", &FrameError::NotText);
                ControlFlow::Continue(())
            }
            Some(Err(error)) => {
                warn!(%error, "socket error");
                ControlFlow::Break(self.disconnected(DisconnectReason::Transport, now))
            }
            None => {
                warn!("socket closed by peer");
                ControlFlow::Break(self.disconnected(DisconnectReason::Eof, now))
            }
        }
    }

    fn on_frame(&mut self, text: &str, now: Instant) -> ControlFlow<Exit> {
        match parse_frame(text, &self.config) {
            Ok(event) => {
                self.counters.events_received += 1;
                self.latency_ms = Some(latency_ms(event.event_time_ms));
                self.step(Input::Event(event), now)
            }
            Err(error) => {
                self.bad_frame(text, &error);
                ControlFlow::Continue(())
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

    /// Every input to the machine goes through here, so the view is published in one place.
    fn step(&mut self, input: Input, now: Instant) -> ControlFlow<Exit> {
        let anchor = self.machine.anchored_at();
        let action = self.machine.handle(input, now);
        let anchored = self.machine.anchored_at();
        if anchored.is_some() && anchored != anchor {
            self.limiter.reset();
        }
        let flow = self.perform(action, now);
        self.publish_view();
        flow
    }

    fn perform(&mut self, action: Action, now: Instant) -> ControlFlow<Exit> {
        match action {
            Action::None => ControlFlow::Continue(()),
            Action::FetchSnapshot => {
                self.request_snapshot(now);
                ControlFlow::Continue(())
            }
            Action::ResetConnection { silent_for } => {
                ControlFlow::Break(self.disconnected(DisconnectReason::Stale { silent_for }, now))
            }
        }
    }

    fn set_link(&mut self, link: Link) {
        self.link = link;
        self.publish_view();
    }

    fn live_book(&self) -> Result<(&Book, Instant), NotLiveReason> {
        match (&self.link, self.machine.state()) {
            (
                Link::Connected { .. },
                State::Live {
                    book, anchored_at, ..
                },
            ) => Ok((book, *anchored_at)),
            (Link::Connected { .. }, State::Buffering) => Err(NotLiveReason::Buffering),
            (Link::Connected { .. }, State::Stitching { .. }) => Err(NotLiveReason::Stitching),
            (Link::Connected { .. }, State::Resyncing { reason }) => {
                Err(NotLiveReason::Resyncing(*reason))
            }
            (Link::Connecting { .. }, _) => Err(NotLiveReason::Connecting),
            (Link::Reconnecting { reason, .. }, _) => Err(NotLiveReason::Reconnecting(*reason)),
            (Link::Failed(_), _) => Err(NotLiveReason::Failed),
        }
    }

    fn publish_view(&mut self) {
        match self.live_book() {
            Ok((book, anchored_at)) => {
                let key = Published::Live {
                    last_update_id: book.last_update_id,
                    anchored_at,
                };
                if key != self.published {
                    let view = BookView::live(
                        self.config.name,
                        book,
                        self.config.settings.k_levels,
                        self.machine.last_event_at(),
                        self.latency_ms,
                    );
                    self.store_view(key, view);
                }
            }
            Err(reason) => {
                let key = Published::NotLive(reason);
                if key != self.published {
                    let view =
                        BookView::empty(self.config.name, reason, self.machine.last_event_at());
                    self.store_view(key, view);
                }
            }
        }
    }

    fn store_view(&mut self, key: Published, view: BookView) {
        self.published = key;
        self.counters.views_published += 1;
        // A crossed top after a fully applied event cannot come from the protocol.
        if let Some((bid, ask)) = view.crossing() {
            self.counters.crossed_views += 1;
            if !self.crossed {
                warn!(%bid, %ask, "book crossed after a full event");
            }
            self.crossed = true;
        } else if self.crossed {
            self.crossed = false;
            if view.state == ViewState::Live {
                info!("book no longer crossed");
            }
        }
        self.views.publish(view);
    }

    fn request_snapshot(&mut self, now: Instant) {
        if !matches!(self.fetch, Fetch::Idle) {
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
        self.fetch = Fetch::Scheduled(scheduled);
    }

    fn start_fetch(&mut self, now: Instant) {
        self.limiter.sent(now);
        debug!(url = %self.config.rest_url, "snapshot requested");
        self.fetch = Fetch::InFlight {
            reply: fetch_snapshot(&self.http, &self.config.rest_url),
            started_at: now,
        };
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
                self.step(Input::Snapshot(snapshot), now)
            }
            Err(SnapshotError::RateLimited { limit, retry_after }) => {
                self.counters.snapshot_errors += 1;
                self.limiter.rate_limited(limit, retry_after, now);
                error!(%limit, ?retry_after, "snapshot request rate limited, backing off");
                self.perform(Action::FetchSnapshot, now)
            }
            Err(SnapshotError::Http { status, body }) if is_configuration_error(status) => {
                self.counters.snapshot_errors += 1;
                ControlFlow::Break(self.failed(Failure {
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
        if self.reconnect_failures > 0
            && let Link::Connected { since } = &self.link
            && matches!(self.machine.state(), State::Live { .. })
            && now.saturating_duration_since(*since) >= LIVE_RESET_AFTER
        {
            self.reconnect_failures = 0;
            info!(
                live_s = LIVE_RESET_AFTER.as_secs(),
                "reconnect backoff reset"
            );
        }
        self.step(Input::Tick, now)
    }

    fn publish_status(&self) {
        self.status_tx.send_replace(Some(self.status()));
    }

    fn status(&self) -> FeedStatus {
        let now = Instant::now();
        let at = SystemTime::now();
        let events_per_sec = match self.status_tx.borrow().as_ref() {
            Some(last) => {
                let elapsed = at.duration_since(last.at).unwrap_or_default();
                let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
                let received = self
                    .counters
                    .events_received
                    .saturating_sub(last.feed.events_received);
                if elapsed_ms == 0 {
                    0
                } else {
                    received.saturating_mul(1000) / elapsed_ms
                }
            }
            None => 0,
        };
        let book = self.machine.book();
        FeedStatus {
            at,
            name: self.config.name,
            state: self.feed_state(now),
            best_bid: book.and_then(Book::best_bid),
            best_ask: book.and_then(Book::best_ask),
            bid_levels: book.map_or(0, |book| book.bids.len()),
            ask_levels: book.map_or(0, |book| book.asks.len()),
            band: book.map(|book| book.band),
            since_last_event: now.saturating_duration_since(self.machine.last_event_at()),
            latency_ms: self.latency_ms,
            events_per_sec,
            snapshot: self.snapshot_activity(now),
            sync: self.machine.counters(),
            feed: self.counters,
        }
    }

    fn feed_state(&self, now: Instant) -> FeedState {
        let buffered = self.machine.buffered();
        match &self.link {
            Link::Connecting { attempt } => FeedState::Connecting { attempt: *attempt },
            Link::Reconnecting { reason, until } => FeedState::Reconnecting {
                reason: *reason,
                retry_in: until.saturating_duration_since(now),
            },
            Link::Failed(failure) => FeedState::Failed(failure.clone()),
            Link::Connected { .. } => match self.machine.state() {
                State::Buffering => FeedState::Buffering { buffered },
                State::Stitching { snapshot, .. } => FeedState::Stitching {
                    last_update_id: snapshot.last_update_id,
                    buffered,
                },
                State::Live { reanchor, .. } => FeedState::Live {
                    reanchor: reanchor.as_ref().map(|pending| ReanchorStatus {
                        pending_for: now.saturating_duration_since(pending.requested_at),
                        kept: pending.kept(),
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
        match &self.fetch {
            Fetch::InFlight { started_at, .. } => SnapshotActivity::InFlight {
                elapsed: now.saturating_duration_since(*started_at),
            },
            Fetch::Scheduled(scheduled) => SnapshotActivity::Scheduled {
                due_in: scheduled.send_at.saturating_duration_since(now),
                cause: scheduled.cause,
            },
            Fetch::Idle => SnapshotActivity::Idle,
        }
    }
}

async fn connect(config: &FeedConfig) -> Result<Socket, ConnectError> {
    info!(url = %config.ws_url, "connecting");
    match time::timeout(CONNECT_TIMEOUT, connect_async(config.ws_url.as_str())).await {
        Ok(Ok((socket, response))) => {
            info!(status = %response.status(), "connected");
            Ok(socket)
        }
        Ok(Err(tungstenite::Error::Http(response))) => {
            let status = response.status();
            let body = response
                .body()
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_default();
            error!(%status, body = %preview(&body), "handshake rejected");
            match RateLimit::from_status(status.as_u16()) {
                // Without a header a ban is waited out for the full reconnect cap.
                Some(limit) => Err(ConnectError::Lost(DisconnectReason::RateLimited {
                    limit,
                    wait: retry_after(response.headers()).unwrap_or(BACKOFF_CAP),
                })),
                None if is_configuration_error(status) => Err(ConnectError::Failed(Failure {
                    status: status.as_u16(),
                    body: preview(&body).to_owned(),
                })),
                None => Err(ConnectError::Lost(DisconnectReason::HandshakeRejected {
                    status: status.as_u16(),
                })),
            }
        }
        Ok(Err(error)) => {
            warn!(%error, "connect failed");
            Err(ConnectError::Lost(DisconnectReason::ConnectFailed))
        }
        Err(_elapsed) => {
            warn!(
                timeout_ms = CONNECT_TIMEOUT.as_millis(),
                "connect timed out"
            );
            Err(ConnectError::Lost(DisconnectReason::ConnectTimeout))
        }
    }
}

fn is_configuration_error(status: StatusCode) -> bool {
    matches!(status.as_u16(), 400 | 404 | 451)
}

fn parse_frame<'a>(text: &'a str, config: &FeedConfig) -> Result<DepthEvent, FrameError<'a>> {
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
        frame = preview(text),
        "depth update"
    );
    Ok(DepthEvent::from(update))
}

async fn send_time(send_at: Option<Instant>) {
    match send_at {
        Some(at) => time::sleep_until(time::Instant::from_std(at)).await,
        None => future::pending().await,
    }
}

async fn await_reply(fetch: &mut Fetch) -> (Result<Snapshot, SnapshotError>, Instant) {
    match fetch {
        Fetch::InFlight { reply, started_at } => (reply.as_mut().await, *started_at),
        Fetch::Idle | Fetch::Scheduled(_) => future::pending().await,
    }
}

async fn close_socket(socket: &mut Socket) {
    match time::timeout(CLOSE_TIMEOUT, socket.close(None)).await {
        Ok(Ok(())) => debug!("socket closed"),
        Ok(Err(error)) => debug!(%error, "close failed"),
        Err(_elapsed) => debug!("close timed out"),
    }
}

fn fetch_snapshot(http: &reqwest::Client, url: &str) -> SnapshotFuture {
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
            return Err(SnapshotError::Http {
                status,
                body: preview(&String::from_utf8_lossy(&body)).to_owned(),
            });
        }
        Ok(serde_json::from_slice(&body)?)
    })
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds: u64 = headers.get(RETRY_AFTER)?.to_str().ok()?.parse().ok()?;
    let wait = Duration::from_secs(seconds);
    Instant::now().checked_add(wait).map(|_| wait)
}

fn preview(text: &str) -> &str {
    &text[..text.floor_char_boundary(PREVIEW_BYTES)]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::backoff::WaitCause;
    use crate::config::{Settings, Venue};
    use crate::fixed::{level, px};
    use crate::sync::ResyncReason;

    const K: usize = 2;

    fn settings() -> Settings {
        Settings {
            depth_limit: 1000,
            k_levels: K,
            stale_after: Duration::from_secs(5),
            status_interval: Duration::from_secs(1),
            reanchor_margin_percent: 10,
        }
    }

    /// The shutdown sender is handed back because dropping it reads as a shutdown.
    fn feed_with(config: FeedConfig) -> (Feed, BookReader, watch::Sender<bool>) {
        let (status_tx, _status_rx) = watch::channel(None);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (feed, reader) =
            Feed::new(config, status_tx, shutdown_rx).expect("the http client builds offline");
        (feed, reader, shutdown_tx)
    }

    fn feed() -> (Feed, BookReader, watch::Sender<bool>) {
        feed_with(FeedConfig::new(Venue::Binance, "BTCUSDT", settings()))
    }

    /// Connected and live on snapshot 6, bridged by one buffered event.
    fn live_feed(now: Instant) -> (Feed, BookReader, watch::Sender<bool>) {
        let (mut feed, reader, shutdown) = feed();
        assert!(feed.on_connected(now).is_continue());
        let bridge = frame(4, 5, 6, "[]", "[]");
        assert!(feed.on_message(Some(Ok(bridge)), now).is_continue());
        assert!(
            feed.on_snapshot_result(Ok(snapshot(6)), Duration::ZERO, now)
                .is_continue()
        );
        assert_eq!(reader.load().state, ViewState::Live);
        (feed, reader, shutdown)
    }

    fn frame(prev: u64, first: u64, last: u64, bids: &str, asks: &str) -> Message {
        Message::text(format!(
            r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1700000000000,"T":1700000000000,"s":"BTCUSDT","U":{first},"u":{last},"pu":{prev},"b":{bids},"a":{asks}}}}}"#
        ))
    }

    fn snapshot(last_update_id: u64) -> Snapshot {
        Snapshot {
            last_update_id,
            bids: vec![level("100", "1"), level("99", "1")],
            asks: vec![level("101", "1"), level("102", "1")],
        }
    }

    #[test]
    fn views_appear_on_connect_and_on_going_live() {
        let (mut feed, reader, _shutdown) = feed();
        let now = Instant::now();
        assert_eq!(
            reader.load().state,
            ViewState::NotLive(NotLiveReason::Connecting)
        );

        assert!(feed.on_connected(now).is_continue());
        assert_eq!(
            reader.load().state,
            ViewState::NotLive(NotLiveReason::Buffering)
        );
        assert_eq!(feed.counters.views_published, 1);
        feed.limiter.sent(now);

        let buffering = reader.load();
        let bridge = frame(4, 5, 6, "[]", "[]");
        assert!(feed.on_message(Some(Ok(bridge)), now).is_continue());
        assert!(Arc::ptr_eq(&buffering, &reader.load()));

        assert!(
            feed.on_snapshot_result(Ok(snapshot(6)), Duration::ZERO, now)
                .is_continue()
        );
        let live = reader.load();
        assert_eq!(live.state, ViewState::Live);
        assert_eq!(live.bids, vec![level("100", "1"), level("99", "1")]);
        assert_eq!(live.asks, vec![level("101", "1"), level("102", "1")]);
        assert_eq!(live.last_update_id, 6);
        assert_eq!(live.band.map(|band| band.bid_floor), Some(px("99")));
        assert_eq!(feed.counters.views_published, 2);
        assert_eq!(feed.limiter.attempts(), 0);
    }

    #[test]
    fn views_change_only_with_the_live_book_or_the_not_live_reason() {
        let now = Instant::now();
        let (mut feed, reader, _shutdown) = live_feed(now);
        let live = reader.load();
        assert_eq!(feed.counters.views_published, 2);

        assert!(feed.on_watchdog(now).is_continue());
        assert!(Arc::ptr_eq(&live, &reader.load()));
        assert!(
            feed.on_snapshot_result(Ok(snapshot(6)), Duration::ZERO, now)
                .is_continue()
        );
        assert!(Arc::ptr_eq(&live, &reader.load()));

        let update = frame(6, 7, 8, r#"[["100","2"]]"#, "[]");
        assert!(feed.on_message(Some(Ok(update)), now).is_continue());
        let applied = reader.load();
        assert_eq!(applied.last_update_id, 8);
        assert_eq!(applied.bids[0], level("100", "2"));
        assert_eq!(feed.counters.views_published, 3);

        let skipped = frame(99, 9, 10, "[]", "[]");
        assert!(feed.on_message(Some(Ok(skipped)), now).is_continue());
        let gone = reader.load();
        let gap = ResyncReason::Gap {
            expected: 8,
            got_pu: 99,
        };
        assert_eq!(
            gone.state,
            ViewState::NotLive(NotLiveReason::Resyncing(gap))
        );
        assert!(gone.bids.is_empty() && gone.asks.is_empty());
        assert_eq!(gone.band, None);
        assert_eq!(feed.counters.views_published, 4);

        // Older than the buffered chain, so the resync goes on with the same reason and the
        // wasted request counts towards the backoff.
        feed.limiter.sent(now);
        assert!(
            feed.on_snapshot_result(Ok(snapshot(3)), Duration::ZERO, now)
                .is_continue()
        );
        assert!(Arc::ptr_eq(&gone, &reader.load()));
        assert_eq!(feed.limiter.attempts(), 1);

        let later = now + Duration::from_secs(5);
        assert!(feed.on_watchdog(later).is_break());
        let stale = DisconnectReason::Stale {
            silent_for: Duration::from_secs(5),
        };
        assert_eq!(
            reader.load().state,
            ViewState::NotLive(NotLiveReason::Reconnecting(stale))
        );
        assert_eq!(feed.counters.views_published, 5);
        assert_eq!(feed.counters.crossed_views, 0);
    }

    #[test]
    fn a_close_frame_takes_the_book_away_at_once() {
        let now = Instant::now();
        let (mut feed, reader, _shutdown) = live_feed(now);
        let close = Some(Ok(Message::Close(None)));
        assert!(feed.on_message(close, now).is_break());
        let closed = DisconnectReason::Closed { code: None };
        assert_eq!(
            reader.load().state,
            ViewState::NotLive(NotLiveReason::Reconnecting(closed))
        );
        assert_eq!(feed.counters.views_published, 3);
    }

    #[test]
    fn a_crossed_top_is_published_and_counted() {
        let now = Instant::now();
        let (mut feed, reader, _shutdown) = live_feed(now);
        let crossing = frame(6, 7, 8, r#"[["101.5","1"]]"#, "[]");
        assert!(feed.on_message(Some(Ok(crossing)), now).is_continue());
        let crossed = reader.load();
        assert_eq!(crossed.state, ViewState::Live);
        assert_eq!(crossed.bids[0], level("101.5", "1"));
        assert_eq!(crossed.asks[0], level("101", "1"));
        assert_eq!(feed.counters.crossed_views, 1);
        assert!(feed.crossed);
        let clearing = frame(8, 9, 10, r#"[["101.5","0"]]"#, "[]");
        assert!(feed.on_message(Some(Ok(clearing)), now).is_continue());
        assert_eq!(reader.load().bids[0], level("100", "1"));
        assert_eq!(feed.counters.crossed_views, 1);
        assert!(!feed.crossed);
    }

    #[test]
    fn a_re_anchor_swaps_the_band_without_leaving_live() {
        let (mut feed, reader, _shutdown) = feed();
        let now = Instant::now();
        assert!(feed.on_connected(now).is_continue());
        let bridge = frame(4, 5, 6, "[]", "[]");
        assert!(feed.on_message(Some(Ok(bridge)), now).is_continue());
        // Four asks, so the best ask can reach the alarm without the side going thin.
        let wide = Snapshot {
            last_update_id: 6,
            bids: vec![level("100", "1"), level("99", "1"), level("98", "1")],
            asks: vec![
                level("101", "1"),
                level("102", "1"),
                level("102.9", "1"),
                level("103", "1"),
            ],
        };
        assert!(
            feed.on_snapshot_result(Ok(wide), Duration::ZERO, now)
                .is_continue()
        );
        let anchored = reader.load();
        assert_eq!(anchored.band.map(|band| band.bid_floor), Some(px("98")));
        assert_eq!(feed.counters.views_published, 2);

        // The best ask moves to 102.9, inside the ten percent margin before the ceiling.
        let drift = frame(6, 7, 8, "[]", r#"[["101","0"],["102","0"]]"#);
        assert!(feed.on_message(Some(Ok(drift)), now).is_continue());
        assert_eq!(reader.load().state, ViewState::Live);
        assert_eq!(feed.machine.reanchor_buffered(), Some(1));
        assert_eq!(feed.counters.views_published, 3);

        let later = now + Duration::from_millis(500);
        feed.limiter.sent(now);
        let narrow = Snapshot {
            last_update_id: 8,
            bids: vec![level("100", "1"), level("99", "1")],
            asks: vec![level("102.9", "1"), level("103", "1")],
        };
        assert!(
            feed.on_snapshot_result(Ok(narrow), Duration::ZERO, later)
                .is_continue()
        );
        let swapped = reader.load();
        assert_eq!(swapped.state, ViewState::Live);
        assert_eq!(swapped.band.map(|band| band.bid_floor), Some(px("99")));
        assert_eq!(swapped.last_update_id, 8);
        assert_eq!(feed.machine.counters().reanchors, 1);
        assert_eq!(feed.counters.views_published, 4);
        assert_eq!(feed.limiter.attempts(), 0);
    }

    #[test]
    fn a_configuration_error_from_the_snapshot_ends_the_feed_with_the_reason() {
        let (mut feed, reader, _shutdown) = feed();
        let now = Instant::now();
        assert!(feed.on_connected(now).is_continue());
        let error = SnapshotError::Http {
            status: StatusCode::BAD_REQUEST,
            body: "invalid symbol".to_owned(),
        };
        assert!(matches!(
            feed.on_snapshot_result(Err(error), Duration::ZERO, now),
            ControlFlow::Break(Exit::Failed)
        ));
        let FeedState::Failed(failure) = feed.feed_state(now) else {
            panic!("a 400 must end the feed");
        };
        assert_eq!(failure.status, 400);
        assert_eq!(failure.body, "invalid symbol");
        assert_eq!(feed.counters.snapshot_errors, 1);
        assert_eq!(
            reader.load().state,
            ViewState::NotLive(NotLiveReason::Failed)
        );
    }

    #[test]
    fn a_bad_frame_is_skipped_and_counted() {
        let now = Instant::now();
        let (mut feed, reader, _shutdown) = live_feed(now);
        let live = reader.load();
        let bad = [
            Message::text("{"),
            frame(6, 7, 8, "[]", "[]").to_text().map_or_else(
                |_| Message::text(""),
                |text| Message::text(text.replace("BTCUSDT", "ETHUSDT")),
            ),
            Message::Binary(Vec::new().into()),
        ];
        for (count, message) in (1..).zip(bad) {
            assert!(feed.on_message(Some(Ok(message)), now).is_continue());
            assert_eq!(feed.counters.bad_frames, count);
        }
        assert!(Arc::ptr_eq(&live, &reader.load()));
        let good = frame(6, 7, 8, "[]", "[]");
        assert!(feed.on_message(Some(Ok(good)), now).is_continue());
        assert_eq!(reader.load().last_update_id, 8);
    }

    #[test]
    fn a_snapshot_ahead_of_the_stream_publishes_stitching() {
        let (mut feed, reader, _shutdown) = feed();
        let now = Instant::now();
        assert!(feed.on_connected(now).is_continue());
        assert!(
            feed.on_snapshot_result(Ok(snapshot(10)), Duration::ZERO, now)
                .is_continue()
        );
        assert_eq!(
            reader.load().state,
            ViewState::NotLive(NotLiveReason::Stitching)
        );
        assert_eq!(feed.counters.views_published, 2);
        let bridge = frame(9, 10, 12, "[]", "[]");
        assert!(feed.on_message(Some(Ok(bridge)), now).is_continue());
        let live = reader.load();
        assert_eq!(live.state, ViewState::Live);
        assert_eq!(live.last_update_id, 12);
        assert_eq!(feed.counters.views_published, 3);
    }

    #[test]
    fn the_reconnect_backoff_is_forgiven_after_a_minute_live_on_one_connection() {
        let settings = Settings {
            stale_after: Duration::from_secs(120),
            ..settings()
        };
        let (mut feed, reader, _shutdown) =
            feed_with(FeedConfig::new(Venue::Binance, "BTCUSDT", settings));
        let now = Instant::now();
        assert!(feed.on_connected(now).is_continue());
        let bridge = frame(4, 5, 6, "[]", "[]");
        assert!(feed.on_message(Some(Ok(bridge)), now).is_continue());
        assert!(
            feed.on_snapshot_result(Ok(snapshot(6)), Duration::ZERO, now)
                .is_continue()
        );
        feed.reconnect_failures = 3;
        // A resync half-way through changes the anchor, not the connection.
        let midway = now + Duration::from_secs(30);
        let gap = frame(99, 9, 10, "[]", "[]");
        assert!(feed.on_message(Some(Ok(gap)), midway).is_continue());
        assert!(
            feed.on_snapshot_result(Ok(snapshot(10)), Duration::ZERO, midway)
                .is_continue()
        );
        assert_eq!(reader.load().state, ViewState::Live);
        let almost = now + Duration::from_secs(59);
        assert!(feed.on_watchdog(almost).is_continue());
        assert_eq!(feed.reconnect_failures, 3);
        let stable = now + Duration::from_secs(60);
        assert!(feed.on_watchdog(stable).is_continue());
        assert_eq!(feed.reconnect_failures, 0);
    }

    #[test]
    fn a_server_error_from_the_snapshot_schedules_another_request() {
        let (mut feed, reader, _shutdown) = feed();
        let now = Instant::now();
        assert!(feed.on_connected(now).is_continue());
        feed.fetch = Fetch::Idle;
        let error = SnapshotError::Http {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: "down".to_owned(),
        };
        assert!(
            feed.on_snapshot_result(Err(error), Duration::ZERO, now)
                .is_continue()
        );
        assert!(matches!(feed.fetch, Fetch::Scheduled(_)));
        assert_eq!(feed.counters.snapshot_errors, 1);
        assert_eq!(
            reader.load().state,
            ViewState::NotLive(NotLiveReason::Buffering)
        );
    }

    #[test]
    fn a_rate_limit_holds_the_next_request_for_retry_after() {
        let (mut feed, _reader, _shutdown) = feed();
        let now = Instant::now();
        assert!(feed.on_connected(now).is_continue());
        feed.fetch = Fetch::Idle;
        let error = SnapshotError::RateLimited {
            limit: RateLimit::TooManyRequests,
            retry_after: Some(Duration::from_secs(90)),
        };
        assert!(
            feed.on_snapshot_result(Err(error), Duration::ZERO, now)
                .is_continue()
        );
        let Fetch::Scheduled(scheduled) = &feed.fetch else {
            panic!("a retry is scheduled");
        };
        assert_eq!(
            scheduled.cause,
            Some(WaitCause::RateLimited(RateLimit::TooManyRequests))
        );
        assert_eq!(scheduled.send_at, now + Duration::from_secs(90));
    }

    #[test]
    fn retry_after_is_whole_seconds_the_clock_can_hold() {
        let header = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(RETRY_AFTER, value.parse().expect("test literal"));
            headers
        };
        assert_eq!(retry_after(&header("90")), Some(Duration::from_secs(90)));
        assert_eq!(retry_after(&header("soon")), None);
        assert_eq!(retry_after(&header("18446744073709551615")), None);
        assert_eq!(retry_after(&HeaderMap::new()), None);
    }

    #[tokio::test]
    async fn a_rejected_handshake_ends_the_run_with_failed() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener");
        let address = listener.local_addr().expect("bound address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("one client");
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.ends_with(b"\r\n\r\n") {
                let read = socket.read(&mut chunk).await.expect("a request");
                assert!(read > 0, "the client hung up before the request ended");
                request.extend_from_slice(&chunk[..read]);
            }
            socket
                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                .await
                .expect("a reply");
        });
        let mut config = FeedConfig::new(Venue::Binance, "BTCUSDT", settings());
        config.ws_url = format!("ws://{address}/stream?streams=btcusdt@depth@100ms");
        let (feed, reader, _shutdown) = feed_with(config);
        let status = time::timeout(Duration::from_secs(5), feed.run())
            .await
            .expect("a rejected handshake ends the run at once");
        server.await.expect("server task");
        assert!(matches!(
            status.state,
            FeedState::Failed(Failure { status: 404, .. })
        ));
        assert_eq!(
            reader.load().state,
            ViewState::NotLive(NotLiveReason::Failed)
        );
    }
}
