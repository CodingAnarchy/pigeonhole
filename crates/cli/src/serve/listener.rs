//! One accept loop for plain and TLS connections, each served as HTTP/1.1 or HTTP/2 (gRPC
//! needs HTTP/2). Shutdown stops accepting, then lets in-flight connections finish within
//! the grace period.
//!
//! **Connections are capped** (`--max-connections`, #1604): each accepted TCP connection
//! holds a slot from `accept` until it ends, TLS handshake included, HTTP/1.1 and HTTP/2
//! alike. Past the cap a connection is accepted and closed at once, before any protocol is
//! read, so the client sees the close (gRPC: `UNAVAILABLE`) rather than waiting in the
//! kernel's backlog. A connection that sends nothing within `--request-timeout-ms` is
//! dropped, so silent sockets can't hold the slots (cli692's D1 on the design).

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// A TLS handshake that takes longer than this is dropped.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// HTTP/2 streams per connection, set explicitly rather than left to hyper's default. The
/// in-flight budget is reserved before a gRPC message is read (`grpc::prepay`), so this only
/// bounds how many streams one connection keeps waiting.
pub(crate) const MAX_STREAMS: u32 = 128;

/// The HTTP/2 ping interval, and how long a ping may go unanswered.
const KEEPALIVE: Duration = Duration::from_secs(20);

/// The default cap's ceiling: 4096 connections pin at most 256 MiB in HTTP/1.1 pre-reads
/// (64 KiB each), before HTTP/2's windows and send buffers (cli692 on #1604).
pub(crate) const DEFAULT_MAX_CONNECTIONS: usize = 4096;

/// Refusals in a row before the accept loop yields, so a connection flood doesn't keep the
/// other tasks on its worker from running.
const REFUSALS_PER_YIELD: u64 = 64;

/// The default `--max-connections`: half the soft open-file limit, leaving the other half to
/// the database (its file, WAL segments, a backup being written, an embedding endpoint's and
/// a replica's connections), between 16 and [`DEFAULT_MAX_CONNECTIONS`]. With no limit to
/// read: the ceiling.
pub(crate) fn default_max_connections(open_files: Option<u64>) -> usize {
    open_files.map_or(DEFAULT_MAX_CONNECTIONS, |n| {
        usize::try_from(n / 2)
            .unwrap_or(usize::MAX)
            .clamp(16, DEFAULT_MAX_CONNECTIONS)
    })
}

/// The process's soft open-file limit (`RLIMIT_NOFILE`), where there is one.
pub(crate) fn open_file_limit() -> Option<u64> {
    #[cfg(unix)]
    {
        nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE)
            .ok()
            .map(|(soft, _)| soft)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// The open connections against their cap, and what the cap refused.
#[derive(Debug)]
pub(crate) struct Connections {
    max: usize,
    /// Connections holding a slot: never more than `max`.
    open: AtomicUsize,
    refused: AtomicU64,
    accept_errors: AtomicU64,
    /// A slot was freed: an accept loop out of descriptors waits for it.
    closed: tokio::sync::Notify,
    /// How each served connection ended, for a test's failure message (#1668).
    #[cfg(test)]
    ended: std::sync::Mutex<Vec<String>>,
}

impl Connections {
    pub(crate) fn new(max: usize) -> Arc<Self> {
        Arc::new(Self {
            max: max.max(1),
            open: AtomicUsize::new(0),
            refused: AtomicU64::new(0),
            accept_errors: AtomicU64::new(0),
            closed: tokio::sync::Notify::new(),
            #[cfg(test)]
            ended: std::sync::Mutex::default(),
        })
    }

    /// How the connections served so far ended, in order (#1668).
    #[cfg(test)]
    pub(crate) fn ended(&self) -> Vec<String> {
        self.ended.lock().unwrap().clone()
    }

    /// A slot for a new connection, or `None` at the cap.
    fn take(self: &Arc<Self>) -> Option<Slot> {
        let mut n = self.open.load(Ordering::Relaxed);
        loop {
            if n >= self.max {
                return None;
            }
            match self
                .open
                .compare_exchange_weak(n, n + 1, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return Some(Slot(Arc::clone(self))),
                Err(now) => n = now,
            }
        }
    }

    /// Connections holding a slot now.
    pub(crate) fn open(&self) -> usize {
        self.open.load(Ordering::Relaxed)
    }

    /// Connections closed at the cap, since the start.
    pub(crate) fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// The metrics: `/metrics` and `Admin.Metrics`.
    pub(crate) fn render(&self, out: &mut String) {
        use std::fmt::Write as _;
        for (name, kind, v) in [
            ("pigeonhole_serve_connections", "gauge", self.open() as u64),
            ("pigeonhole_serve_connections_max", "gauge", self.max as u64),
            (
                "pigeonhole_serve_connections_refused_total",
                "counter",
                self.refused(),
            ),
            (
                "pigeonhole_serve_accept_errors_total",
                "counter",
                self.accept_errors.load(Ordering::Relaxed),
            ),
        ] {
            let _ = writeln!(out, "# TYPE {name} {kind}");
            let _ = writeln!(out, "{name} {v}");
        }
    }
}

/// A connection's place under the cap, given back when its task ends.
struct Slot(Arc<Connections>);

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::AcqRel);
        self.0.closed.notify_one();
    }
}

/// How the listener bounds its connections.
pub(crate) struct Bounds {
    pub(crate) connections: Arc<Connections>,
    /// How long a connection may go without sending a byte before it's dropped, and
    /// HTTP/1.1's limit on reading a request's headers: `--request-timeout-ms`.
    pub(crate) first_byte: Duration,
}

/// Whether an accept failed for want of a resource that a closing connection may free.
fn exhausted(e: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(
            e.raw_os_error(),
            Some(nix::libc::EMFILE | nix::libc::ENFILE | nix::libc::ENOBUFS | nix::libc::ENOMEM)
        )
    }
    #[cfg(not(unix))]
    {
        let _ = e;
        false
    }
}

/// A connection's stream, noting when its first byte arrives.
struct FirstByte<S> {
    inner: S,
    got: Arc<AtomicBool>,
}

impl<S: AsyncRead + Unpin> AsyncRead for FirstByte<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        if buf.filled().len() > before {
            self.got.store(true, Ordering::Relaxed);
        }
        r
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FirstByte<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// How a connection's serving ended.
// Its fields are read through `Debug`, by tests only (#1668).
#[allow(dead_code)]
#[derive(Debug)]
enum Ended {
    /// Dropped at the first-byte deadline.
    Silent,
    /// The TLS handshake failed or timed out.
    Handshake,
    /// hyper's connection ended, with its result.
    Served(Result<(), String>),
}

/// Serves one connection: dropped if no byte arrives within `first_byte` (the deadline ends
/// at the first byte, not the first request: a slow header is HTTP/1.1's header timeout's,
/// and an HTTP/2 connection that sent its preface may idle on pings).
async fn serve_one<S>(
    builder: auto::Builder<TokioExecutor>,
    io: S,
    service: TowerToHyperService<axum::Router>,
    watcher: hyper_util::server::graceful::Watcher,
    first_byte: Duration,
) -> Ended
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let got = Arc::new(AtomicBool::new(false));
    let io = FirstByte {
        inner: io,
        got: Arc::clone(&got),
    };
    let conn = watcher.watch(
        builder
            .serve_connection_with_upgrades(TokioIo::new(io), service)
            .into_owned(),
    );
    tokio::pin!(conn);
    let served = |r: Result<(), Box<dyn std::error::Error + Send + Sync>>| {
        Ended::Served(r.map_err(|e| e.to_string()))
    };
    tokio::select! {
        r = &mut conn => return served(r),
        () = tokio::time::sleep(first_byte) => {}
    }
    if !got.load(Ordering::Relaxed) {
        // Silent since it connected: returning drops `conn`, and with it the socket, which
        // closes; the task's end frees the slot (cli692's N2 on #1653).
        return Ended::Silent;
    }
    served(conn.await)
}

pub(crate) async fn serve(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    app: axum::Router,
    shutdown: impl Future<Output = ()>,
    grace: Duration,
    send_buffer: Option<usize>,
    bounds: Bounds,
) {
    let graceful = GracefulShutdown::new();
    let conns = bounds.connections;
    tokio::pin!(shutdown);
    let mut refusals = 0u64;
    let mut logged: Option<Instant> = None;
    loop {
        let (stream, _peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(a) => a,
                Err(e) => {
                    conns.accept_errors.fetch_add(1, Ordering::Relaxed);
                    // One line a second at most: a long run out of descriptors would flood
                    // the log.
                    if logged.is_none_or(|t| t.elapsed() >= Duration::from_secs(1)) {
                        eprintln!("phdb serve: accept: {e}");
                        logged = Some(Instant::now());
                    }
                    // Out of descriptors (or buffers): wait for a connection to close, 1 s at
                    // most, since something else may free one. Anything else: a short pause.
                    let wait = if exhausted(&e) {
                        Duration::from_secs(1)
                    } else {
                        Duration::from_millis(50)
                    };
                    tokio::select! {
                        () = conns.closed.notified(), if exhausted(&e) => {}
                        () = tokio::time::sleep(wait) => {}
                        () = &mut shutdown => break,
                    }
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        let Some(slot) = conns.take() else {
            // At the cap: closed before any protocol is read.
            conns.refused.fetch_add(1, Ordering::Relaxed);
            drop(stream);
            refusals += 1;
            if refusals.is_multiple_of(REFUSALS_PER_YIELD) {
                tokio::task::yield_now().await;
            }
            continue;
        };
        let _ = stream.set_nodelay(true);
        let service = TowerToHyperService::new(app.clone());
        let mut builder = auto::Builder::new(TokioExecutor::new());
        // An idle stream (a feed with nothing to send) is kept open through proxies by
        // HTTP/2 pings; a peer that stops answering them is dropped (D313 §4).
        builder
            .http2()
            .max_concurrent_streams(MAX_STREAMS)
            .timer(hyper_util::rt::TokioTimer::new())
            .keep_alive_interval(KEEPALIVE)
            .keep_alive_timeout(KEEPALIVE);
        // A client that sends a request's headers slowly holds its slot this long at most.
        builder
            .http1()
            .timer(hyper_util::rt::TokioTimer::new())
            .header_read_timeout(bounds.first_byte);
        // What one stream may queue for its client beyond its window (`--http2-send-buffer`;
        // hyper's 400 KiB by default): with a fixed client window, a feed stream's queue is
        // its delivery lag (perf287 on #1397).
        if let Some(n) = send_buffer {
            builder.http2().max_send_buf_size(n);
        }
        let watcher = graceful.watcher();
        let first_byte = bounds.first_byte;
        let tls = tls.clone();
        tokio::spawn(async move {
            let ended = match tls {
                None => serve_one(builder, stream, service, watcher, first_byte).await,
                Some(acceptor) => {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                        Ok(Ok(tls)) => serve_one(builder, tls, service, watcher, first_byte).await,
                        _ => Ended::Handshake,
                    }
                }
            };
            #[cfg(test)]
            slot.0.ended.lock().unwrap().push(format!("{ended:?}"));
            let _ = ended;
            drop(slot);
        });
    }
    drop(listener);
    if tokio::time::timeout(grace, graceful.shutdown())
        .await
        .is_err()
    {
        eprintln!("phdb serve: connections still open after the grace period; closing them");
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_MAX_CONNECTIONS, default_max_connections};

    #[test]
    fn the_default_cap_is_half_the_open_file_limit_between_16_and_4096() {
        assert_eq!(default_max_connections(Some(256)), 128); // macOS's default
        assert_eq!(default_max_connections(Some(1024)), 512); // Linux's usual
        assert_eq!(default_max_connections(Some(20)), 16);
        assert_eq!(
            default_max_connections(Some(1 << 20)),
            DEFAULT_MAX_CONNECTIONS
        );
        assert_eq!(
            default_max_connections(Some(u64::MAX)),
            DEFAULT_MAX_CONNECTIONS
        );
        assert_eq!(default_max_connections(None), DEFAULT_MAX_CONNECTIONS);
    }
}
