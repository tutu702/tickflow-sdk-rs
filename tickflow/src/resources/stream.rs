//! Real-time WebSocket market data stream (`/v1/ws/stream`).
//!
//! Subscribes to one or both of the unified stream's channels —
//! [`Channel::Quotes`] for real-time quote snapshots and
//! [`Channel::Depth`] for top-of-book updates — and dispatches the
//! incoming frames to user-supplied handlers.
//!
//! # Quick start
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use tickflow::client::TickFlow;
//! use tickflow::resources::stream::Channel;
//!
//! let tf = TickFlow::new(std::env::var("TICKFLOW_API_KEY")?)?;
//! let stream = tf.stream().clone();
//!
//! stream.on_quotes(|quotes| {
//!     for q in quotes {
//!         println!("{}: {}", q.symbol, q.last_price);
//!     }
//! });
//!
//! stream.on_error(|err| eprintln!("stream error: {err}"));
//!
//! // Queue a subscription. Sent on the next (re)connect.
//! stream.subscribe(Channel::Quotes, ["600000.SH".to_string()]);
//!
//! // Drive the connection loop until close() or a fatal error.
//! let handle = tokio::spawn({
//!     let stream = stream.clone();
//!     async move { stream.connect().await }
//! });
//!
//! // ... later, shut down gracefully ...
//! stream.close();
//! let _ = handle.await;
//! # Ok(()) }
//! ```
//!

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::{select, time::sleep};
use tokio_tungstenite::{
    WebSocketStream, connect_async,
    tungstenite::{Error as WsError, Message, http::StatusCode},
};
use tracing::{debug, info, warn};
use url::Url;

use crate::model::Quote;
use crate::model::depth::MarketDepth;
use crate::{
    client::Config,
    error::{Error, Result},
};

// ============================================================================
// Public types
// ============================================================================

/// Real-time data channel on the unified stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    /// Real-time quote snapshot.
    Quotes,
    /// Five-level order-book depth.
    Depth,
}

impl Channel {
    /// Wire-format channel name (the value used in `op`/JSON messages).
    pub fn as_str(&self) -> &'static str {
        match self {
            Channel::Quotes => "quotes",
            Channel::Depth => "depth",
        }
    }
}

#[derive(Debug, Serialize)]
struct StreamRequest<'a> {
    op: &'a str,
    channel: &'a str,
    symbols: &'a [String],
}

#[derive(Debug, Deserialize)]
struct StreamResponse {
    op: String,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    message: Option<String>,
}

// ============================================================================
// Stream state
// ============================================================================

type QuotesHandler = Arc<dyn Fn(Vec<Quote>) + Send + Sync>;
type DepthHandler = Arc<dyn Fn(Vec<MarketDepth>) + Send + Sync>;
type ErrorHandler = Arc<dyn Fn(String) + Send + Sync>;

#[derive(Default)]
struct Handlers {
    quotes: Option<QuotesHandler>,
    depth: Option<DepthHandler>,
    error: Option<ErrorHandler>,
}

enum Command {
    Subscribe(Channel, Vec<String>),
    Unsubscribe(Channel, Vec<String>),
    Shutdown,
}

enum RunLoopExit {
    /// Caller asked for [`MarketStream::close`].
    Shutdown,
    /// Connection ended or errored — outer loop should reconnect.
    Disconnected,
}

/// Parsed inbound event that needs handler dispatch.
enum StreamEvent {
    /// Batch of [`Quote`] snapshots.
    Quotes(Vec<Quote>),
    /// Batch of [`MarketDepth`] updates.
    Depth(Vec<MarketDepth>),
    /// Server- or transport-reported error.
    Error(String),
}

/// Unified WebSocket market data stream.
///
/// Cloned instances share the same underlying state, so handlers and
/// subscriptions registered on one clone are visible to every other
/// clone and to the live connection loop.
#[derive(Clone)]
pub struct MarketStream {
    inner: Arc<MarketStreamInner>,
}

struct MarketStreamInner {
    ws_url: Url,
    api_key: Option<String>,
    handlers: StdMutex<Handlers>,
    pending: StdMutex<HashMap<Channel, HashSet<String>>>,
    command_tx: StdMutex<Option<mpsc::UnboundedSender<Command>>>,
}

/// Statuses returned by the handshake that the SDK treats as fatal —
/// the connection loop logs the rejection through the error handler
/// and stops trying to reconnect.
const NO_RETRY_STATUS: &[StatusCode] = &[
    StatusCode::UNAUTHORIZED,
    StatusCode::FORBIDDEN,
    StatusCode::NOT_FOUND,
];

/// Backoff between reconnect attempts after a transient failure.
const RECONNECT_DELAY: Duration = Duration::from_secs(3);

// ============================================================================
// Implementation
// ============================================================================

impl MarketStream {
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            inner: Arc::new(MarketStreamInner {
                ws_url: config.ws_url.clone(),
                api_key: config.api_key.clone(),
                handlers: StdMutex::new(Handlers::default()),
                pending: StdMutex::new(HashMap::new()),
                command_tx: StdMutex::new(None),
            }),
        }
    }

    // ----- Handlers --------------------------------------------------------

    /// Register (or replace) the handler for the [`Channel::Quotes`]
    /// channel.
    ///
    /// Re-registering replaces the previous handler — only the latest
    /// one is kept.
    pub fn on_quotes<F>(&self, handler: F) -> &Self
    where
        F: Fn(Vec<Quote>) + Send + Sync + 'static,
    {
        self.store_handler(|h| h.quotes = Some(Arc::new(handler)));
        self
    }

    /// Register (or replace) the handler for the [`Channel::Depth`]
    /// channel.
    pub fn on_depth<F>(&self, handler: F) -> &Self
    where
        F: Fn(Vec<MarketDepth>) + Send + Sync + 'static,
    {
        self.store_handler(|h| h.depth = Some(Arc::new(handler)));
        self
    }

    /// Register (or replace) the error handler. Invoked for:
    ///
    /// - `{"op":"error", "message":...}` server messages,
    /// - transport-level failures (logged before each reconnect
    ///   attempt),
    /// - HTTP 401/403/404 rejections during the handshake.
    pub fn on_error<F>(&self, handler: F) -> &Self
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        self.store_handler(|h| h.error = Some(Arc::new(handler)));
        self
    }

    fn store_handler(&self, f: impl FnOnce(&mut Handlers)) {
        let mut guard = self.inner.handlers.lock().expect("handlers mutex poisoned");
        f(&mut guard);
    }

    // ----- Subscription ----------------------------------------------------

    /// Subscribe to *channel* for the given *symbols*.
    ///
    /// Safe to call before or after [`connect`](Self::connect). When
    /// called while connected the subscription is forwarded to the
    /// live socket in addition to being recorded for re-subscription
    /// on reconnect. Calls before connect are buffered and flushed
    /// automatically once the socket opens.
    pub fn subscribe<I, S>(&self, channel: Channel, symbols: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let syms: Vec<String> = symbols.into_iter().map(Into::into).collect();
        if syms.is_empty() {
            return;
        }

        {
            let mut pending = self.inner.pending.lock().expect("pending mutex poisoned");
            pending
                .entry(channel)
                .or_default()
                .extend(syms.iter().cloned());
        }

        self.send_command(Command::Subscribe(channel, syms));
    }

    /// Unsubscribe *symbols* from *channel*. Like
    /// [`subscribe`](Self::subscribe), works both before and after
    /// connect. Empty input is a no-op.
    pub fn unsubscribe<I, S>(&self, channel: Channel, symbols: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let syms: Vec<String> = symbols.into_iter().map(Into::into).collect();
        if syms.is_empty() {
            return;
        }

        {
            let mut pending = self.inner.pending.lock().expect("pending mutex poisoned");
            if let Some(set) = pending.get_mut(&channel) {
                for s in &syms {
                    set.remove(s);
                }
            }
        }

        self.send_command(Command::Unsubscribe(channel, syms));
    }

    fn send_command(&self, cmd: Command) {
        // Best-effort: the channel only exists once `connect` has
        // been called. If we cannot lock the mutex (rare race with
        // `connect`), drop the command — the change is already
        // recorded in `pending` and will be replayed on reconnect.
        let Some(tx) = self
            .inner
            .command_tx
            .try_lock()
            .ok()
            .and_then(|g| g.clone())
        else {
            return;
        };
        let _ = tx.send(cmd);
    }

    // ----- Lifecycle -------------------------------------------------------

    /// Drive the WebSocket loop until [`close`](Self::close) is called
    /// or a non-recoverable error occurs.
    ///
    /// The future resolves with `Ok(())` after a clean shutdown, and
    /// with `Err(Error::Api(_))` after an HTTP 401/403/404 rejection.
    /// Spawn it in a background task to use the stream
    /// non-blockingly.
    #[must_use = "the Result indicates whether shutdown was clean or fatal"]
    pub async fn connect(&self) -> Result<()> {
        let (tx, rx) = mpsc::unbounded_channel::<Command>();
        {
            let mut guard = self
                .inner
                .command_tx
                .lock()
                .expect("command_tx mutex poisoned");
            if guard.is_some() {
                // Another connect() is already running; nothing to do.
                return Ok(());
            }
            *guard = Some(tx);
        }

        let result = self.websocket_connect(rx).await;

        {
            let mut guard = self
                .inner
                .command_tx
                .lock()
                .expect("command_tx mutex poisoned");
            *guard = None;
        }
        result
    }

    /// Best-effort synchronous shutdown signal. The connection loop
    /// exits at the next iteration of its select; in-flight
    /// subscribe/unsubscribe frames are not interrupted.
    pub fn close(&self) {
        self.send_command(Command::Shutdown);
    }

    // ----- Internals -------------------------------------------------------

    fn build_url(&self) -> Url {
        let mut url = self.inner.ws_url.clone();
        url.set_path("/v1/ws/stream");
        url.query_pairs_mut()
            .append_pair("api_key", self.inner.api_key.as_deref().unwrap_or(""));
        url
    }

    /// Dispatch a parsed inbound event to the matching user
    /// handler. Errors are always logged at warn level so
    /// transport and server-reported failures surface in tracing
    /// output even when no error handler is registered.
    async fn dispatch(&self, event: StreamEvent) {
        // Clone the matching handler under the lock and release
        // it before invoking the user callback — handlers may
        // call back into the public API (e.g. re-register
        // themselves) and must not deadlock on the handler mutex.
        match event {
            StreamEvent::Error(msg) => {
                warn!(target: "tickflow.stream", "{msg}");
                let handler = self
                    .inner
                    .handlers
                    .lock()
                    .expect("handlers mutex poisoned")
                    .error
                    .clone();
                if let Some(h) = handler {
                    h(msg);
                }
            }
            StreamEvent::Quotes(quotes) => {
                let handler = self
                    .inner
                    .handlers
                    .lock()
                    .expect("handlers mutex poisoned")
                    .quotes
                    .clone();
                if let Some(h) = handler {
                    h(quotes);
                }
            }
            StreamEvent::Depth(depths) => {
                let handler = self
                    .inner
                    .handlers
                    .lock()
                    .expect("handlers mutex poisoned")
                    .depth
                    .clone();
                if let Some(h) = handler {
                    h(depths);
                }
            }
        }
    }

    async fn resubscribe<W>(&self, ws: &mut WebSocketStream<W>) -> Result<()>
    where
        W: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Clone under the lock to avoid holding it across an await.
        let pending: Vec<(Channel, Vec<String>)> = {
            let guard = self.inner.pending.lock().expect("pending mutex poisoned");
            guard
                .iter()
                .filter_map(|(ch, syms)| {
                    let v: Vec<String> = syms.iter().cloned().collect();
                    if v.is_empty() { None } else { Some((*ch, v)) }
                })
                .collect()
        };

        for (ch, syms) in pending {
            Self::send_request(ws, "subscribe", ch, &syms)
                .await
                .map_err(|e| Error::Api(format!("websocket: {e}")))?;
        }
        Ok(())
    }

    async fn send_request<S>(
        sink: &mut S,
        op: &str,
        channel: Channel,
        symbols: &[String],
    ) -> std::result::Result<(), WsError>
    where
        S: futures::Sink<Message, Error = WsError> + Unpin,
    {
        let payload = StreamRequest {
            op,
            channel: channel.as_str(),
            symbols,
        };
        let json = serde_json::to_string(&payload)
            .map_err(|e| WsError::Io(std::io::Error::other(format!("serialize {op}: {e}"))))?;
        sink.send(Message::Text(json.into())).await
    }

    async fn websocket_connect(
        &self,
        mut command_rx: mpsc::UnboundedReceiver<Command>,
    ) -> Result<()> {
        let url = self.build_url();
        debug!(target: "tickflow.stream", "endpoint = {url}");

        loop {
            match connect_async(url.as_str()).await {
                Ok((mut ws, _response)) => {
                    info!(target: "tickflow.stream", "connected");
                    if let Err(e) = self.resubscribe(&mut ws).await {
                        warn!(target: "tickflow.stream", "resubscribe failed: {e}");
                        sleep(RECONNECT_DELAY).await;
                        continue;
                    }

                    match self.run_loop(&mut ws, &mut command_rx).await {
                        RunLoopExit::Shutdown => return Ok(()),
                        RunLoopExit::Disconnected => sleep(RECONNECT_DELAY).await,
                    }
                }
                Err(WsError::Http(response)) => {
                    let status = response.status();
                    let body = response
                        .body()
                        .as_ref()
                        .map(|v| String::from_utf8_lossy(v.as_slice()).into_owned())
                        .unwrap_or_default();
                    let reason = format!("HTTP {status}: {body}");
                    self.dispatch(StreamEvent::Error(reason.clone())).await;
                    if NO_RETRY_STATUS.contains(&status) {
                        return Err(Error::Api(reason));
                    }
                    sleep(RECONNECT_DELAY).await;
                }
                Err(e) => {
                    let reason = format!("connection error: {e}");
                    self.dispatch(StreamEvent::Error(reason)).await;
                    sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    async fn run_loop<W>(
        &self,
        ws: &mut WebSocketStream<W>,
        command_rx: &mut mpsc::UnboundedReceiver<Command>,
    ) -> RunLoopExit
    where
        W: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Splitting lets `command_rx` and the read half be polled
        // independently in `select!` without borrowing the same
        // `&mut ws` twice.
        let (mut write, mut read) = ws.split();

        loop {
            select! {
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Subscribe(ch, syms)) => {
                            if let Err(e) = Self::send_request(
                                &mut write, "subscribe", ch, &syms,
                            ).await {
                                warn!(target: "tickflow.stream", "subscribe send failed: {e}");
                                return RunLoopExit::Disconnected;
                            }
                        }
                        Some(Command::Unsubscribe(ch, syms)) => {
                            if let Err(e) = Self::send_request(
                                &mut write, "unsubscribe", ch, &syms,
                            ).await {
                                warn!(target: "tickflow.stream", "unsubscribe send failed: {e}");
                                return RunLoopExit::Disconnected;
                            }
                        }
                        Some(Command::Shutdown) | None => {
                            let _ = write.send(Message::Close(None)).await;
                            return RunLoopExit::Shutdown;
                        }
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            self.handle_text(text.as_str()).await;
                        }
                        Some(Ok(Message::Ping(_))) => {
                            // tungstenite auto-replies with Pong.
                        }
                        Some(Ok(Message::Close(frame))) => {
                            debug!(target: "tickflow.stream", "close frame: {frame:?}");
                            return RunLoopExit::Disconnected;
                        }
                        Some(Err(e)) => {
                            warn!(target: "tickflow.stream", "read error: {e}");
                            return RunLoopExit::Disconnected;
                        }
                        None => return RunLoopExit::Disconnected,
                        // Binary / Frame / Pong — not produced by the
                        // tickflow server today; ignore silently.
                        _ => {}
                    }
                }
            }
        }
    }

    async fn handle_text(&self, text: &str) {
        let resp: StreamResponse = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    target: "tickflow.stream",
                    "parse error: {e}; text={}",
                    &text[..text.len().min(200)]
                );
                return;
            }
        };

        match resp.op.as_str() {
            "quotes" => {
                if let Some(data) = resp.data
                    && let Ok(quotes) = serde_json::from_value::<Vec<Quote>>(data)
                {
                    self.dispatch(StreamEvent::Quotes(quotes)).await;
                }
            }
            "depth" => {
                if let Some(data) = resp.data
                    && let Ok(depths) = serde_json::from_value::<Vec<MarketDepth>>(data)
                {
                    self.dispatch(StreamEvent::Depth(depths)).await;
                }
            }
            "error" => {
                let msg = resp.message.unwrap_or_else(|| "unknown error".to_string());
                self.dispatch(StreamEvent::Error(msg)).await;
            }
            "subscribed" => {
                debug!(target: "tickflow.stream", "subscribed ack: {resp:?}");
            }
            other => {
                debug!(target: "tickflow.stream", "unknown op: {other}");
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Region;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn build_ws_url_https_default() {
        let base = Url::parse("https://api.tickflow.org").unwrap();
        let url = build_ws_url_for_test(&base, Some("secret"));
        assert_eq!(url.scheme(), "wss");
        assert_eq!(url.path(), "/v1/ws/stream");
        assert_eq!(url.query_pairs().count(), 1);
        let (k, v) = url.query_pairs().next().unwrap();
        assert_eq!(k, "api_key");
        assert_eq!(v, "secret");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn parse_quotes_dispatches_via_handle_text() {
        let stream = make_test_stream();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        stream.on_quotes(move |quotes| {
            assert_eq!(quotes.len(), 1);
            assert_eq!(quotes[0].symbol, "600000.SH");
            assert_eq!(quotes[0].region, Region::Cn);
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        let text = r#"{"op":"quotes","data":[{"symbol":"600000.SH","region":"CN","last_price":9.72,"prev_close":9.78,"open":9.78,"high":9.78,"low":9.68,"volume":426585,"amount":422430500.0,"timestamp":1776754802000}]}"#;
        stream.handle_text(text).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn parse_depth_dispatches_via_handle_text() {
        let stream = make_test_stream();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        stream.on_depth(move |depths| {
            assert_eq!(depths.len(), 1);
            assert_eq!(depths[0].symbol, "600000.SH");
            assert_eq!(depths[0].bid_prices, vec![9.72, 9.71, 9.7]);
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        let text = r#"{"op":"depth","data":[{"symbol":"600000.SH","region":"CN","timestamp":1776754802000,"bid_prices":[9.72,9.71,9.7],"bid_volumes":[3192,3870,26168],"ask_prices":[9.73,9.74,9.75],"ask_volumes":[74,1602,1148]}]}"#;
        stream.handle_text(text).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn parse_error_dispatches_via_handle_text() {
        let stream = make_test_stream();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        stream.on_error(move |msg| {
            assert_eq!(msg, "no permission for channel: depth");
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        let text = r#"{"op":"error","message":"no permission for channel: depth"}"#;
        stream.handle_text(text).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn parse_subscribed_does_not_dispatch() {
        let stream = make_test_stream();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_q = Arc::clone(&counter);
        stream.on_quotes(move |_| {
            counter_q.fetch_add(1, Ordering::SeqCst);
        });
        let counter_e = Arc::clone(&counter);
        stream.on_error(move |_| {
            counter_e.fetch_add(1, Ordering::SeqCst);
        });

        let text = r#"{"op":"subscribed","channel":"quotes","symbols":["600000.SH"],"total":1}"#;
        stream.handle_text(text).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn subscribe_records_pending_before_connect() {
        let stream = make_test_stream();
        stream.subscribe(
            Channel::Quotes,
            ["600000.SH".to_string(), "000001.SZ".to_string()],
        );
        stream.subscribe(Channel::Depth, ["600000.SH".to_string()]);

        let pending = stream.inner.pending.lock().unwrap();
        assert_eq!(pending.get(&Channel::Quotes).unwrap().len(), 2);
        assert!(pending.get(&Channel::Quotes).unwrap().contains("600000.SH"));
        assert!(pending.get(&Channel::Quotes).unwrap().contains("000001.SZ"));
        assert_eq!(pending.get(&Channel::Depth).unwrap().len(), 1);
    }

    #[test]
    fn unsubscribe_removes_from_pending() {
        let stream = make_test_stream();
        stream.subscribe(
            Channel::Quotes,
            ["600000.SH".to_string(), "000001.SZ".to_string()],
        );
        stream.unsubscribe(Channel::Quotes, ["600000.SH".to_string()]);

        let pending = stream.inner.pending.lock().unwrap();
        let set = pending.get(&Channel::Quotes).unwrap();
        assert_eq!(set.len(), 1);
        assert!(set.contains("000001.SZ"));
    }

    #[test]
    fn empty_subscribe_is_noop() {
        let stream = make_test_stream();
        stream.subscribe(Channel::Quotes, Vec::<String>::new());
        assert!(stream.inner.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn subscribe_command_json_shape() {
        let payload = StreamRequest {
            op: "subscribe",
            channel: "quotes",
            symbols: &["600000.SH".to_string(), "000001.SZ".to_string()],
        };
        let json = serde_json::to_string(&payload).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["op"], "subscribe");
        assert_eq!(parsed["channel"], "quotes");
        assert_eq!(parsed["symbols"][0], "600000.SH");
        assert_eq!(parsed["symbols"][1], "000001.SZ");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handler_registration_works_from_inside_async_context() {
        // Regression test: handlers used to be stored under a
        // `tokio::sync::RwLock` and registered via
        // `blocking_write`, which panics when called from inside a
        // `#[tokio::main]` runtime. The handler mutex is now
        // `std::sync::Mutex`, so registration from any context is
        // safe.
        let stream = make_test_stream();
        stream.on_quotes(|_| {});
        stream.on_depth(|_| {});
        stream.on_error(|_| {});
    }

    fn make_test_stream() -> MarketStream {
        let config = Config {
            api_key: Some("k".into()),
            base_url: Url::parse("https://example.test").unwrap(),
            ws_url: Url::parse("wss://example.test").unwrap(),
            timeout: std::time::Duration::from_secs(30),
            max_retries: 3,
        };
        MarketStream::new(&config)
    }

    fn build_ws_url_for_test(base: &Url, api_key: Option<&str>) -> Url {
        let mut url = base.clone();
        match url.scheme() {
            "https" => {
                url.set_scheme("wss").unwrap();
            }
            "http" => {
                url.set_scheme("ws").unwrap();
            }
            _ => {}
        }
        url.set_path("/v1/ws/stream");
        url.query_pairs_mut()
            .append_pair("api_key", api_key.unwrap_or(""));
        url
    }
}
