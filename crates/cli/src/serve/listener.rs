//! One accept loop for plain and TLS connections, each served as HTTP/1.1 or HTTP/2 (gRPC
//! needs HTTP/2). Shutdown stops accepting, then lets in-flight connections finish within
//! the grace period.

use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
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

pub(crate) async fn serve(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    app: axum::Router,
    shutdown: impl Future<Output = ()>,
    grace: Duration,
    send_buffer: Option<usize>,
) {
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);
    loop {
        let (stream, _peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("phdb serve: accept: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
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
        // What one stream may queue for its client beyond its window (`--http2-send-buffer`;
        // hyper's 400 KiB by default): with a fixed client window, a feed stream's queue is
        // its delivery lag (perf287 on #1397).
        if let Some(n) = send_buffer {
            builder.http2().max_send_buf_size(n);
        }
        match &tls {
            None => {
                let conn = builder
                    .serve_connection_with_upgrades(TokioIo::new(stream), service)
                    .into_owned();
                let conn = graceful.watch(conn);
                tokio::spawn(async move {
                    let _ = conn.await;
                });
            }
            Some(acceptor) => {
                let acceptor = acceptor.clone();
                let watcher = graceful.watcher();
                tokio::spawn(async move {
                    let Ok(Ok(tls)) =
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await
                    else {
                        return;
                    };
                    let conn = builder
                        .serve_connection_with_upgrades(TokioIo::new(tls), service)
                        .into_owned();
                    let _ = watcher.watch(conn).await;
                });
            }
        }
    }
    drop(listener);
    if tokio::time::timeout(grace, graceful.shutdown())
        .await
        .is_err()
    {
        eprintln!("phdb serve: connections still open after the grace period; closing them");
    }
}
