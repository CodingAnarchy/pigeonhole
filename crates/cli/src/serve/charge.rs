//! Unary read replies charged to `--inflight-bytes` (#1567, cli692's review of the design).
//!
//! - **What:** `ReadRow`, `MultiGet`, `Graph.Traverse` and `Vectors.Nearest` (their
//!   `page_bytes`), `Memory.Recall` (its byte budget), and `Get` (its cell, after the read).
//! - **When:** the reply's cap is reserved before the read, so the reply is never built
//!   unbudgeted, then trimmed to its encoded size (twice the cap for HTTP/JSON, fitted to the
//!   rendered body). A reservation that can't be had within the request timeout answers
//!   `Overloaded`, as a request body's share does.
//! - **Until:** the reply's body has been written to the connection, or dropped. The handler
//!   puts a [`Charged`] in the response's extensions; [`attach`], a layer over the whole
//!   router, moves it into a [`ChargedBody`] around the body.
//! - **A client that stops reading** (cli692's D1): the body hands its data to the transport
//!   in [`PIECE`]-sized slices (zero-copy), so HTTP/2 holds about one slice beyond the
//!   client's window. A wrapped body (a reply over one slice) starts a watchdog when it's
//!   wrapped: after `--stream-idle-ms` without a slice taken, it drops the rest and the
//!   charge, and the body ends with `DeadlineExceeded` if it's polled again. A reply within
//!   one slice is never wrapped, so it starts none.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, HeaderValue, Response};
use http_body::{Frame, SizeHint};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::errors::ApiError;

/// The slice a charged body hands the transport at a time.
pub(crate) const PIECE: usize = 64 << 10;

/// gRPC's framing of a unary message: its compression flag and length.
pub(crate) const GRPC_FRAME: usize = 5;

/// The charged replies' gauges and counters.
#[derive(Debug, Default)]
pub(crate) struct ReplyStats {
    /// Reply bytes charged now.
    pub(crate) bytes: AtomicU64,
    /// Replies whose client took nothing for `--stream-idle-ms`: dropped, their charge freed.
    pub(crate) idle_drops: AtomicU64,
}

impl ReplyStats {
    pub(crate) fn render(&self, out: &mut String) {
        use std::fmt::Write as _;
        for (name, kind, v) in [
            (
                "pigeonhole_serve_inflight_reply_bytes",
                "gauge",
                self.bytes.load(Ordering::Relaxed),
            ),
            (
                "pigeonhole_serve_reply_idle_drops_total",
                "counter",
                self.idle_drops.load(Ordering::Relaxed),
            ),
        ] {
            let _ = writeln!(out, "# TYPE {name} {kind}");
            let _ = writeln!(out, "{name} {v}");
        }
    }
}

/// A reply's share of the in-flight budget.
#[derive(Debug)]
pub(crate) struct Charge {
    permit: OwnedSemaphorePermit,
    inflight: Arc<Semaphore>,
    stats: Arc<ReplyStats>,
}

impl Charge {
    /// `bytes` of the budget (at least 1, at most `total`, the whole budget, so a lone large
    /// reply still runs), waiting at most `wait`: `Overloaded` past it.
    pub(crate) async fn reserve(
        inflight: &Arc<Semaphore>,
        total: usize,
        bytes: usize,
        wait: Duration,
        stats: &Arc<ReplyStats>,
    ) -> Result<Self, ApiError> {
        let n = bytes.clamp(1, total.max(1));
        let permit = acquire(inflight, n, wait).await?;
        stats.bytes.fetch_add(n as u64, Ordering::Relaxed);
        Ok(Self {
            permit,
            inflight: Arc::clone(inflight),
            stats: Arc::clone(stats),
        })
    }

    /// The bytes it holds.
    pub(crate) fn bytes(&self) -> usize {
        self.permit.num_permits()
    }

    /// Gives back what's above `bytes` (at least 1 is kept).
    pub(crate) fn trim(&mut self, bytes: usize) {
        let keep = bytes.max(1);
        let held = self.bytes();
        if held > keep
            && let Some(extra) = self.permit.split(held - keep)
        {
            drop(extra);
            self.stats
                .bytes
                .fetch_sub((held - keep) as u64, Ordering::Relaxed);
        }
    }

    /// Holds exactly `bytes` (at most `total`): trims, or takes the difference, waiting at
    /// most `wait` (an HTTP/JSON rendering larger than its reservation, D2).
    pub(crate) async fn fit(
        &mut self,
        bytes: usize,
        total: usize,
        wait: Duration,
    ) -> Result<(), ApiError> {
        let want = bytes.clamp(1, total.max(1));
        let held = self.bytes();
        if want <= held {
            self.trim(want);
            return Ok(());
        }
        let more = acquire(&self.inflight, want - held, wait).await?;
        self.permit.merge(more);
        self.stats
            .bytes
            .fetch_add((want - held) as u64, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.stats
            .bytes
            .fetch_sub(self.bytes() as u64, Ordering::Relaxed);
    }
}

async fn acquire(
    inflight: &Arc<Semaphore>,
    n: usize,
    wait: Duration,
) -> Result<OwnedSemaphorePermit, ApiError> {
    let n = u32::try_from(n.max(1)).unwrap_or(u32::MAX);
    match tokio::time::timeout(wait, Arc::clone(inflight).acquire_many_owned(n)).await {
        Ok(Ok(p)) => Ok(p),
        Ok(Err(_)) => Err(ApiError::internal("the in-flight budget closed")),
        Err(_) => Err(ApiError::overloaded(
            "the server's in-flight bytes stayed full past the deadline (a read's reply \
             waits for its share)",
        )),
    }
}

/// A response's charge, in its extensions (which need `Clone`): taken once, by [`attach`].
#[derive(Clone, Debug)]
pub(crate) struct Charged(Arc<Mutex<Option<Charge>>>);

impl Charged {
    pub(crate) fn new(charge: Charge) -> Self {
        Self(Arc::new(Mutex::new(Some(charge))))
    }

    fn take(&self) -> Option<Charge> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

/// The layer over the router: a response with a [`Charged`] gets its body wrapped, so the
/// charge lasts until the body has been written. Others pass through untouched.
pub(crate) fn attach(
    idle: Duration,
) -> impl Fn(Response<Body>) -> std::future::Ready<Response<Body>> + Clone {
    move |resp| std::future::ready(wrap(resp, idle))
}

fn wrap(mut resp: Response<Body>, idle: Duration) -> Response<Body> {
    let Some(charge) = resp
        .extensions_mut()
        .remove::<Charged>()
        .and_then(|c| c.take())
    else {
        return resp;
    };
    // A reply within one slice goes to the transport whole, at its first poll, and sits in
    // the stream's own send buffer, bounded per stream (cli692's (a)): its charge is freed
    // here, with no body wrapper to allocate or watchdog to start (#1567).
    // The charge is the reply's size by now: trimmed to the encoded message (gRPC), or
    // fitted to the rendered body (HTTP/JSON).
    if charge.bytes() <= PIECE {
        drop(charge);
        return resp;
    }
    let grpc = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes().starts_with(b"application/grpc"));
    // The watchdog starts now, not at the first slice (cli692's R1 on #1661): a body whose
    // client's initial window is 0 may never be polled. Today's hyper polls it once even
    // then (the slice waits in h2's send buffer), but this doesn't rely on that. It holds
    // only a weak handle, so a body that ends or is dropped frees everything at once, and
    // the watchdog ends at its next look.
    let stats = Arc::clone(&charge.stats);
    let slot = Arc::new(Mutex::new(Slot {
        rest: Bytes::new(),
        charge: Some(charge),
        progress: Instant::now(),
        expired: false,
    }));
    tokio::spawn(watchdog(Arc::downgrade(&slot), idle, stats));
    resp.map(|inner| {
        Body::new(ChargedBody {
            inner,
            slot,
            grpc,
            done: false,
        })
    })
}

/// What a charged body and its watchdog share.
struct Slot {
    /// Data taken from the inner body, not handed out yet.
    rest: Bytes,
    charge: Option<Charge>,
    /// When a slice was last handed out.
    progress: Instant,
    /// The watchdog dropped it.
    expired: bool,
}

/// A body holding its reply's charge until it ends (cli692's D1 on the #1567 design).
pub(crate) struct ChargedBody {
    inner: Body,
    slot: Arc<Mutex<Slot>>,
    grpc: bool,
    done: bool,
}

impl ChargedBody {
    fn lock(&self) -> std::sync::MutexGuard<'_, Slot> {
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How the body ends once the watchdog dropped it: for gRPC, trailers saying
    /// `DEADLINE_EXCEEDED` (what's been sent is incomplete), else an error, which ends the
    /// connection's response mid-body.
    fn expired(&mut self) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        self.done = true;
        let message = "the client took nothing of its reply for --stream-idle-ms";
        if self.grpc {
            let mut t = HeaderMap::new();
            t.insert("grpc-status", HeaderValue::from_static("4"));
            t.insert(
                "grpc-message",
                HeaderValue::from_static(
                    "the client took nothing of its reply for --stream-idle-ms",
                ),
            );
            return Poll::Ready(Some(Ok(Frame::trailers(t))));
        }
        Poll::Ready(Some(Err(axum::Error::new(message))))
    }
}

async fn watchdog(slot: Weak<Mutex<Slot>>, idle: Duration, stats: Arc<ReplyStats>) {
    let mut wait = idle;
    loop {
        tokio::time::sleep(wait).await;
        let Some(slot) = slot.upgrade() else {
            return;
        };
        let mut s = slot.lock().unwrap_or_else(PoisonError::into_inner);
        if s.charge.is_none() && s.rest.is_empty() {
            return;
        }
        let since = s.progress.elapsed();
        if since >= idle {
            s.rest = Bytes::new();
            s.charge = None;
            s.expired = true;
            stats.idle_drops.fetch_add(1, Ordering::Relaxed);
            return;
        }
        wait = idle - since;
    }
}

impl http_body::Body for ChargedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = &mut *self;
        if this.done {
            return Poll::Ready(None);
        }
        loop {
            let piece = {
                let mut s = this.lock();
                if s.expired {
                    drop(s);
                    return this.expired();
                }
                if s.rest.is_empty() {
                    None
                } else {
                    let n = s.rest.len().min(PIECE);
                    s.progress = Instant::now();
                    Some(s.rest.split_to(n))
                }
            };
            if let Some(piece) = piece {
                return Poll::Ready(Some(Ok(Frame::data(piece))));
            }
            match Pin::new(&mut this.inner).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => {
                        this.lock().rest = data;
                    }
                    Err(other) => return Poll::Ready(Some(Ok(other))),
                },
                Poll::Ready(None) => {
                    // Written: the charge goes back.
                    this.done = true;
                    this.lock().charge = None;
                    return Poll::Ready(None);
                }
                other => return other,
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done || (self.lock().rest.is_empty() && self.inner.is_end_stream())
    }

    fn size_hint(&self) -> SizeHint {
        let rest = self.lock().rest.len() as u64;
        let inner = self.inner.size_hint();
        let mut hint = SizeHint::new();
        hint.set_lower(inner.lower() + rest);
        if let Some(upper) = inner.upper() {
            hint.set_upper(upper + rest);
        }
        hint
    }
}
